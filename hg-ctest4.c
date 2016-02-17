/* Test handling of point-to-point concurrent RPCs and bulk transfers in
 * mercury */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <ctype.h>

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_macros.h>
#include <na_cci.h> /* need for CCI-specific grabbing of URI */

#define VERBOSE_LOG 0
#include "hg-ctest-util.h"

static int benchmark_seconds = 10;

/* checked by callbacks, set by progress/trigger loop */
static int is_finished = 0;

/* incremented by calls, decremented by callbacks (so we can keep
 * track of pending ops - stopping the loop early causes asserts) */
static int op_cnt = 0;

/* this needs to be a global to pass around between callback functions and
 * whatnot */
static struct nahg_comm_info nhcli;

/* global id for client process */
static int bench_client_id = -1;

/* servers (need to be global for now) */
na_addr_t svr_addr = NA_ADDR_NULL;

enum cli_mode_t {
    RPC_MODE = 20,
    BULK_MODE,
    RPCBULK_MODE
};

static enum cli_mode_t cli_mode;

/* individual call/complete times */
struct cli_times {
    double call;
    double complete;
};

/* for "all" option - array of times */
static struct cli_times * all_times = NULL; 
static int time_idx = 0;

/* gets passed throughout benchmark */
struct cli_cb_data {
    hg_handle_t handle;
    hg_bulk_t svr_bulk; // for BULK_MODE
    bulk_read_in_t cli_bulk_in; // for RPCBULK_MODE
    int is_init;
    union {
        struct {
            int num_complete;
            struct timespec start_call;
            double total_time, total_time_call;
        } times;
        int is_finished;
    } u;
};

static hg_return_t cli_sync_cb(const struct hg_cb_info *info)
{
    dprintf("cli recv sync cb: arg:%p\n", info->arg);
    struct cli_cb_data *c = info->arg;
    assert(c);
    c->u.is_finished = 1;
    return HG_SUCCESS;
}

static hg_return_t get_bulk_handle_cli_cb(const struct hg_cb_info *info);
static hg_return_t rpc_cli_cb(const struct hg_cb_info *info);

/* call an iteration of the rpc bench */
static hg_return_t call_next_rpc(
        struct cli_cb_data *c,
        struct timespec *start)
{
    hg_return_t hret;
    struct timespec t;

    dprintf("calling next rpc\n");
    clock_gettime(CLOCK_MONOTONIC, &c->u.times.start_call);
    hret = HG_Forward(c->handle, rpc_cli_cb, c, &c->cli_bulk_in);
    if (hret == HG_SUCCESS) {
        op_cnt++;
        clock_gettime(CLOCK_MONOTONIC, &t);
        double tlf = time_to_s_lf(timediff(c->u.times.start_call, t));
        c->u.times.total_time_call += tlf;
        if (all_times!=NULL) all_times[time_idx].call = tlf;
        if (start) *start = c->u.times.start_call;
    }
    return hret;
}

static hg_return_t rpc_cli_cb(const struct hg_cb_info *info)
{
    hg_return_t hret;
    struct cli_cb_data *cb_dat = (struct cli_cb_data*) info->arg;
    double tlf;
    struct timespec t;

    op_cnt--;

    clock_gettime(CLOCK_MONOTONIC, &t);
    cb_dat->u.times.num_complete++;
    tlf = time_to_s_lf(timediff(cb_dat->u.times.start_call, t));
    cb_dat->u.times.total_time += tlf;
    if (all_times!=NULL) all_times[time_idx++].complete = tlf;
    if (!is_finished){
        hret = call_next_rpc(cb_dat, NULL);
        assert(hret == HG_SUCCESS);
    }

    return HG_SUCCESS;
}

static hg_return_t get_bulk_handle_cli_cb(const struct hg_cb_info *info)
{
    get_bulk_handle_out_t out;
    struct cli_cb_data *cb_dat;
    hg_return_t hret;
    struct timespec t;

    dprintf("rpc callback entered\n");

    assert(info->ret == HG_SUCCESS);
    cb_dat = (struct cli_cb_data*) info->arg;
    if (cb_dat->is_init) {
        hret = HG_Get_output(info->handle, &out);
        assert(hret == HG_SUCCESS);
        if (cb_dat) {
            /* sadly, have to copyout the bulk handle, which is awkward */
            cb_dat->svr_bulk = dup_hg_bulk(nhcli.hbcl, out.bh);
            cb_dat->u.is_finished = 1;
        }
        HG_Free_output(info->handle, &out);
    }
    else {
        op_cnt--;
        clock_gettime(CLOCK_MONOTONIC, &t);
        cb_dat->u.times.num_complete++;
        double tlf = time_to_s_lf(timediff(cb_dat->u.times.start_call, t));
        cb_dat->u.times.total_time += tlf;
        if (all_times!=NULL) all_times[time_idx++].complete = tlf;
        if (!is_finished){
            hret = call_next_rpc(cb_dat, NULL);
            assert(hret == HG_SUCCESS);
        }
    }
    return HG_SUCCESS;
}

static hg_return_t cli_bulk_xfer_cb(const struct hg_bulk_cb_info *info);

static hg_return_t call_next_bulk(
        struct cli_cb_data *c,
        struct timespec *start)
{
    struct timespec t;
    hg_return_t hret;

    dprintf("calling next bulk\n");
    clock_gettime(CLOCK_MONOTONIC, &c->u.times.start_call);
    hret = HG_Bulk_transfer(nhcli.hbctx, cli_bulk_xfer_cb, c, HG_BULK_PUSH,
            svr_addr, c->svr_bulk, 0, nhcli.bh, 0, nhcli.buf_sz,
            HG_OP_ID_IGNORE);
    if (hret == HG_SUCCESS) {
        op_cnt++;
        clock_gettime(CLOCK_MONOTONIC, &t);
        double tlf = time_to_s_lf(timediff(c->u.times.start_call, t));
        c->u.times.total_time_call += tlf;
        if (all_times!=NULL) all_times[time_idx].call = tlf;
        if (start) *start = c->u.times.start_call;
    }
    return hret;
}

static hg_return_t cli_bulk_xfer_cb(const struct hg_bulk_cb_info *info)
{
    struct cli_cb_data * cb_dat = info->arg;
    struct timespec t;
    hg_return_t hret;

    assert(info->ret == HG_SUCCESS);
    assert(!cb_dat->is_init);
    dprintf("bulk callback entered\n");

    clock_gettime(CLOCK_MONOTONIC, &t);
    cb_dat->u.times.num_complete++;
    double tlf = time_to_s_lf(timediff(cb_dat->u.times.start_call, t));
    cb_dat->u.times.total_time += tlf;
    if (all_times!=NULL) all_times[time_idx++].complete = tlf;
    op_cnt--;
    if (!is_finished) {
        hret = call_next_bulk(cb_dat, NULL);
        assert(hret == HG_SUCCESS);
    }

    return HG_SUCCESS;
}

static hg_return_t cli_wait_timed(struct timespec start)
{
    hg_return_t hret = HG_SUCCESS;
    unsigned int num_cb;
    struct timespec t;
    /* wait a bit of time for processes to wind down */
    int time_cond = 1;

    dprintf("progress/trigger loop entered\n");

    while (time_cond || op_cnt > 0) {
        if (!time_cond) {
            is_finished = 1;
            dprintf("time over, op_cnt=%d\n", op_cnt);
        }
        do {
            hret = HG_Trigger(nhcli.hgcl, nhcli.hgctx, 0, 1, &num_cb);
        } while(hret == HG_SUCCESS && num_cb);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            break;

        hret = HG_Progress(nhcli.hgcl, nhcli.hgctx, 100);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            break;

        clock_gettime(CLOCK_MONOTONIC, &t);

        time_cond = (time_to_s_lf(timediff(start,t)) <= benchmark_seconds);
    }

    if (hret == HG_TIMEOUT) hret = HG_SUCCESS;
    return hret;
}

static hg_return_t cli_wait_loop_all(
    int max_retries,
    int num_check_cbs,
    struct cli_cb_data *cb_data)
{
    hg_return_t hret;

    int retry;
    unsigned int num_cb;
    int c;

    dprintf("progress/trigger loop entered\n");

    for(retry = 0; retry < max_retries; retry++) {
        hret = HG_Trigger(nhcli.hgcl, nhcli.hgctx, 0, 1, &num_cb);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            return hret;

        /* check completions before progress so we don't get stuck in a wait */
        if (num_check_cbs > 0) {
            for (c = 0; c < num_check_cbs; c++) {
                if (!cb_data[c].u.is_finished)
                    break;
            }
            if (c == num_check_cbs)
                return HG_SUCCESS;
        }

        hret = HG_Progress(nhcli.hgcl, nhcli.hgctx, 100);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            return hret;
    }
    return HG_TIMEOUT;
}

static void run_client(
        size_t rdma_size,
        enum cli_mode_t mode,
        char const * svr)
{

    /* RPC params */
    hg_handle_t handle;
    hg_bulk_t rdbulk;
    struct cli_cb_data cb_init,
                       cb_sync,
                       bulk_isolated,
                       rpc_isolated;

    /* return params */
    na_return_t nret;
    hg_return_t hret;

    /* benchmark times */
    struct timespec start_time;

    /* initialize */
    nahg_init(svr, rdma_size, NA_FALSE, 0, &nhcli);

    nret = NA_Addr_lookup_wait(
            nhcli.nacl,
            (strcmp(nhcli.class,"mpi")==0) ? nhcli.hoststr : svr,
            &svr_addr);
    assert(nret == NA_SUCCESS);

    nhcli.is_separate_servers = 0;

    memset(&bulk_isolated, 0, sizeof(bulk_isolated));
    memset(&rpc_isolated, 0, sizeof(rpc_isolated));

    /* create, run RPC to grab bulk handle from rdma server
     * (used in bulk mode) */

    cb_init.is_init = 1;
    cb_init.u.is_finished = 0;
    hret = HG_Create(nhcli.hgcl, nhcli.hgctx, svr_addr,
            nhcli.get_bulk_handle_rpc_id, &cb_init.handle);
    assert(hret == HG_SUCCESS);

    HG_Forward(cb_init.handle, get_bulk_handle_cli_cb, &cb_init, NULL);

    hret = cli_wait_loop_all(20, 1, &cb_init);

    assert(hret == HG_SUCCESS);

    bulk_isolated.svr_bulk = cb_init.svr_bulk;

    HG_Destroy(cb_init.handle);

    /* create our own bulk handle if needed */
    if (mode == RPCBULK_MODE) {
        hret = HG_Bulk_create(nhcli.hbcl, 1, &nhcli.buf, &nhcli.buf_sz,
                HG_BULK_READ_ONLY, &rdbulk);
        assert(hret == HG_SUCCESS);

        rpc_isolated.cli_bulk_in.bh = rdbulk;
    }

    /* init rpc handle for benchmark */
    hret = HG_Create(nhcli.hgcl, nhcli.hgctx, svr_addr,
            mode == RPC_MODE ? nhcli.noop_rpc_id : nhcli.bulk_read_rpc_id,
            &rpc_isolated.handle);
    assert(hret == HG_SUCCESS);

    /* do a sync before beginning the benchmark */
    cb_sync.is_init = 1;
    cb_sync.u.is_finished = 0;
    hret = HG_Create(nhcli.hgcl, nhcli.hgctx, svr_addr,
            nhcli.check_in_id, &handle);
    assert(hret == HG_SUCCESS);
    hret = HG_Forward(handle, cli_sync_cb, &cb_sync, NULL);
    assert(hret == HG_SUCCESS);
    /* wait for up to two minutes for other clients to start up */
    hret = cli_wait_loop_all(1200, 1, &cb_sync);
    assert(hret == HG_SUCCESS);
    HG_Destroy(handle);

    dprintf("client running benchmark...\n");

    is_finished = 0;

    if (mode == RPC_MODE || mode == RPCBULK_MODE) {
        hret = call_next_rpc(&rpc_isolated, &start_time);
        assert(hret == HG_SUCCESS);
        hret = cli_wait_timed(start_time);
        assert(hret == HG_SUCCESS);
    }
    else {
        hret = call_next_bulk(&bulk_isolated, &start_time);
        assert(hret == HG_SUCCESS);
        hret = cli_wait_timed(start_time);
        assert(hret == HG_SUCCESS);
    }

    dprintf("client finished benchmark, waiting for others...\n");

    /* wait on a sync for others to complete */
    cb_sync.is_init = 1;
    cb_sync.u.is_finished = 0;
    hret = HG_Create(nhcli.hgcl, nhcli.hgctx, svr_addr,
            nhcli.check_in_id, &handle);
    assert(hret == HG_SUCCESS);
    hret = HG_Forward(handle, cli_sync_cb, &cb_sync, NULL);
    assert(hret == HG_SUCCESS);
    hret = cli_wait_loop_all(20, 1, &cb_sync);
    assert(hret == HG_SUCCESS);

    /* send a shutdown request (don't bother checking) */
    if (bench_client_id == 0) {
        hret = HG_Create(nhcli.hgcl, nhcli.hgctx, svr_addr,
                nhcli.shutdown_server_rpc_id, &handle);
        assert(hret == HG_SUCCESS);
        HG_Forward(handle, NULL, NULL, NULL);
        hret = cli_wait_loop_all(10, 0, NULL);
        HG_Destroy(handle);
    }

    /* print out resulting times */

    struct cli_cb_data *cbd;
    const char * type;
    switch(mode) {
        case RPC_MODE:     cbd = &rpc_isolated; type = "rpc"; break;
        case BULK_MODE:    cbd = &bulk_isolated; type = "bulk"; break;
        case RPCBULK_MODE: cbd = &rpc_isolated; type = "rpcbulk"; break;
        default: abort();
    }
    if (all_times == NULL) {
        printf("%-8s %-8s %12lu %3d %4s %3d %7d %.3e %.3e\n",
                nhcli.class ? nhcli.class : "default", nhcli.transport,
                nhcli.buf_sz, benchmark_seconds, type,
                bench_client_id, cbd->u.times.num_complete,
                cbd->u.times.total_time_call / cbd->u.times.num_complete,
                cbd->u.times.total_time / cbd->u.times.num_complete);
    }
    else {
        for (int i = 0; i < time_idx; i++) {
            printf("%-8s %-8s %12lu %3d %4s %3d %.3e %.3e\n",
                    nhcli.class ? nhcli.class : "default", nhcli.transport,
                    nhcli.buf_sz, benchmark_seconds, type,
                    bench_client_id, all_times[i].call, all_times[i].complete);
        }
    }

    HG_Destroy(rpc_isolated.handle);
    if (mode == RPCBULK_MODE) HG_Bulk_free(rdbulk);
    HG_Bulk_free(bulk_isolated.svr_bulk);
    NA_Addr_free(nhcli.nacl, svr_addr);

    nahg_fini(&nhcli);
}

static void usage();

int main(int argc, char *argv[])
{
    enum mode_t mode = UNKNOWN;
    size_t rdma_size;
    int num_clients;
    char const * svr, * listen_addr;
    char const * svr_id;
    int arg = 1;

    init_verbose();

    if (argc < 2) {
        usage();
        exit(1);
    }

    for (;;) {
        if (strcmp(argv[arg], "-a") == 0) {
            all_times = malloc((1<<21) * sizeof(*all_times));
            assert(all_times);
            arg++;
        }
        else if (strcmp(argv[arg], "-t") == 0) {
            if (arg+1 >= argc){
                usage();
                exit(1);
            }
            else {
                benchmark_seconds = atoi(argv[arg+1]);
                arg += 2;
            }
        }
        else
            break;
    }

    if (arg >= argc) {
        usage();
        exit(1);
    }
    
    if (strcmp(argv[arg], "client") == 0) {
        mode = CLIENT;
    }
    else if (strcmp(argv[arg], "server") == 0)
        mode = SERVER;
    else {
        usage();
        exit(1);
    }
    arg++;

    rdma_size = (size_t) strtol(argv[arg++], NULL, 10);

    switch(mode) {
        case CLIENT:
            if (arg+1 >= argc) {
                usage();
                exit(1);
            }
            /* dumb check but probably effective */
            if (isdigit(argv[arg][0]))
                bench_client_id = atoi(argv[arg]);
            else {
                fprintf(stderr, "bad or nonexisting client id, exiting...\n");
                exit(1);
            }
            arg++;

            if (arg+1 >= argc) {
                usage();
                exit(1);
            }
           
            if (strcmp(argv[arg], "rpc") == 0)
                cli_mode = RPC_MODE;
            else if (strcmp(argv[arg], "bulk") == 0)
                cli_mode = BULK_MODE;
            else if (strcmp(argv[arg], "rpcbulk") == 0)
                cli_mode = RPCBULK_MODE;
            else {
                fprintf(stderr, "expected mode \"rpc\" or \"bulk\", got %s\n",
                        argv[arg]);
                usage();
                exit(1);
            }
            arg++;

            if (arg >= argc) {
                usage();
                exit(1);
            }

            svr  = argv[arg];
            run_client(rdma_size, cli_mode, svr);
            break;

        case SERVER:
            if (arg >= argc) {
                usage();
                exit(1);
            }
            if (isdigit(argv[arg][0]))
                num_clients = atoi(argv[arg]);
            else {
                fprintf(stderr, "error: num_clients non a number\n");
                usage();
                exit(1);
            }
            arg++;
            if (arg >= argc) {
                usage();
                exit(1);
            }
            listen_addr = argv[arg];
            arg++;
            if (arg < argc)
                svr_id = argv[arg];
            else
                svr_id = NULL;
            run_server(rdma_size, listen_addr, svr_id, num_clients);
            break;
        default:
            assert(0);
    }

    return 0;
}


const char * usage_str =
"Usage: hg-ctest4 [-a] [-t TIME] (client | server) OPTIONS\n"
"  -a prints out every measurement, rather than an average in client mode\n"
"  -t is the time to run the benchmark in client mode\n"
"  in client mode, OPTIONS are:\n"
"    <rdma size> <client id> <mode> <server>\n"
"    where client id should be unique among all clients in this run\n"
"    and mode is one of \"rpc\", \"bulk\", or \"rpcbulk\" \n"
"  in server mode, OPTIONS are:\n"
"    <rdma size max> <num clients> <listen addr> [<id>]\n"
"  servers spit out files named ctest1-server-addr.tmp[-<id>] \n"
"    containing their mercury names for clients to gobble up\n"
"  Example:\n"
"    hg-ctest4 server 1048576 1 bmi+tcp://localhost:3344 foo\n"
"    hg-ctest4 client 1048576 0 rpc $(cat ctest1-server-addr.tmp-foo)\n";

static void usage() {
    fprintf(stderr, "%s", usage_str);
}

