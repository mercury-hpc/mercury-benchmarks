/*
 * Copyright 2015-2016 Argonne National Laboratory, Department of Energy,
 * UChicago Argonne, LLC and the HDF Group. See COPYING in the top-level
 * directory
 */

/* Test handling of point-to-point concurrent RPCs and bulk transfers in
 * mercury */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_macros.h>
#include <na_cci.h> /* need for CCI-specific grabbing of URI */

#define VERBOSE_LOG 0
#include "hg-ctest-util.h"

static int benchmark_seconds = 10;
static const int NUM_REPS = 100;
static const int WARMUP = 20;

/* checked by callbacks, set by progress/trigger loop */
static int is_finished = 0;

/* incremented by calls, decremented by callbacks (so we can keep
 * track of pending ops - stopping the loop early causes asserts) */
static int op_cnt = 0;

/* this needs to be a global to pass around between callback functions and
 * whatnot */
static struct nahg_comm_info nhcli;

/* servers (need to be global for now) */
na_addr_t rdma_svr_addr = NA_ADDR_NULL;
na_addr_t rpc_svr_addr = NA_ADDR_NULL;

/* gets passed throughout benchmark */
struct cli_cb_data {
    hg_handle_t handle;
    hg_bulk_t bulk;
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

static hg_return_t get_bulk_handle_cli_cb(const struct hg_cb_info *info);

/* call an iteration of the rpc bench */
static hg_return_t call_next_rpc(
        struct cli_cb_data *c,
        struct timespec *start)
{
    hg_return_t hret;
    struct timespec t;

    dprintf("calling next rpc\n");
    clock_gettime(CLOCK_MONOTONIC, &c->u.times.start_call);
    hret = HG_Forward(c->handle, get_bulk_handle_cli_cb, c, NULL);
    if (hret == HG_SUCCESS) {
        op_cnt++;
        clock_gettime(CLOCK_MONOTONIC, &t);
        c->u.times.total_time_call +=
            time_to_s_lf(timediff(c->u.times.start_call, t));
        if (start) *start = c->u.times.start_call;
    }
    return hret;
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
        hret = HG_Get_output(info->info.forward.handle, &out);
        assert(hret == HG_SUCCESS);
        if (cb_dat) {
            /* sadly, have to copyout the bulk handle, which is awkward */
            cb_dat->bulk = dup_hg_bulk(nhcli.hgcl, out.bh);
            cb_dat->u.is_finished = 1;
        }
        HG_Free_output(info->info.forward.handle, &out);
    }
    else {
        op_cnt--;
        clock_gettime(CLOCK_MONOTONIC, &t);
        cb_dat->u.times.num_complete++;
        cb_dat->u.times.total_time +=
            time_to_s_lf(timediff(cb_dat->u.times.start_call, t));
        if (!is_finished){
            hret = call_next_rpc(cb_dat, NULL);
            assert(hret == HG_SUCCESS);
        }
    }
    return HG_SUCCESS;
}

static hg_return_t cli_bulk_xfer_cb(const struct hg_cb_info *info);

static hg_return_t call_next_bulk(
        struct cli_cb_data *c,
        struct timespec *start)
{
    struct timespec t;
    hg_return_t hret;

    dprintf("calling next bulk\n");
    clock_gettime(CLOCK_MONOTONIC, &c->u.times.start_call);
    hret = HG_Bulk_transfer(nhcli.hgctx, cli_bulk_xfer_cb, c, HG_BULK_PUSH,
            rdma_svr_addr, c->bulk, 0, nhcli.bh, 0, nhcli.buf_sz,
            HG_OP_ID_IGNORE);
    if (hret == HG_SUCCESS) {
        op_cnt++;
        clock_gettime(CLOCK_MONOTONIC, &t);
        c->u.times.total_time_call +=
            time_to_s_lf(timediff(c->u.times.start_call, t));
        if (start) *start = c->u.times.start_call;
    }
    return hret;
}

static hg_return_t cli_bulk_xfer_cb(const struct hg_cb_info *info)
{
    struct cli_cb_data * cb_dat = info->arg;
    struct timespec t;
    hg_return_t hret;

    assert(info->ret == HG_SUCCESS);
    assert(!cb_dat->is_init);
    dprintf("bulk callback entered\n");

    clock_gettime(CLOCK_MONOTONIC, &t);
    cb_dat->u.times.num_complete++;
    cb_dat->u.times.total_time +=
        time_to_s_lf(timediff(cb_dat->u.times.start_call, t));
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
        if (!time_cond) { is_finished = 1; dprintf("time over\n"); }
        do {
            hret = HG_Trigger(nhcli.hgctx, 0, 1, &num_cb);
        } while(hret == HG_SUCCESS && num_cb);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            break;

        hret = HG_Progress(nhcli.hgctx, 100);
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
        hret = HG_Trigger(nhcli.hgctx, 0, 1, &num_cb);
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

        hret = HG_Progress(nhcli.hgctx, 100);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            return hret;
    }
    return HG_TIMEOUT;
}

static void run_client(
        size_t rdma_size,
        char const * rdma_svr,
        char const * rpc_svr)
{

    /* RPC params */
    hg_handle_t handle;
    struct cli_cb_data cb_init,
                       bulk_isolated,
                       rpc_isolated,
                       bulk_concurrent,
                       rpc_concurrent;

    /* return params */
    hg_return_t hret;

    /* benchmark times */
    struct timespec start_time;

    /* initialize */
    nahg_init(rdma_svr, rdma_size, NA_FALSE, 0, &nhcli);

    rdma_svr_addr = lookup_serv_addr(&nhcli, rdma_svr);
    assert(rdma_svr_addr != NA_ADDR_NULL);
    rpc_svr_addr = lookup_serv_addr(&nhcli, rpc_svr);
    assert(rpc_svr_addr != NA_ADDR_NULL);


    if (strcmp(rdma_svr, rpc_svr) != 0)
        nhcli.is_separate_servers = 1;

    memset(&bulk_isolated, 0, sizeof(bulk_isolated));
    memset(&rpc_isolated, 0, sizeof(rpc_isolated));
    memset(&bulk_concurrent, 0, sizeof(bulk_concurrent));
    memset(&rpc_concurrent, 0, sizeof(rpc_concurrent));

    /* create, run RPC to grab bulk handle from rdma server */

    cb_init.is_init = 1;
    cb_init.u.is_finished = 0;
    hret = HG_Create(nhcli.hgctx, rdma_svr_addr,
            nhcli.get_bulk_handle_rpc_id, &cb_init.handle);
    assert(hret == HG_SUCCESS);

    HG_Forward(cb_init.handle, get_bulk_handle_cli_cb, &cb_init, NULL);

    hret = cli_wait_loop_all(20, 1, &cb_init);

    assert(hret == HG_SUCCESS);

    bulk_isolated.bulk = cb_init.bulk;
    bulk_concurrent.bulk = cb_init.bulk;

    HG_Destroy(cb_init.handle);

    /* init rpc handle for benchmark */
    hret = HG_Create(nhcli.hgctx, rpc_svr_addr,
            nhcli.get_bulk_handle_rpc_id, &rpc_isolated.handle);
    assert(hret == HG_SUCCESS);
    rpc_concurrent.handle = rpc_isolated.handle;

    is_finished = 0;

    /* first up, time rpcs / bulks in isolation */
    hret = call_next_rpc(&rpc_isolated, &start_time);
    assert(hret == HG_SUCCESS);
    hret = cli_wait_timed(start_time);
    assert(hret == HG_SUCCESS);
    is_finished = 0;
    hret = call_next_bulk(&bulk_isolated, &start_time);
    assert(hret == HG_SUCCESS);
    hret = cli_wait_timed(start_time);
    assert(hret == HG_SUCCESS);

    /* now, time rpcs / bulks "concurrently" (concurrently issued, that is) */
    is_finished = 0;
    hret = call_next_rpc(&rpc_concurrent, &start_time);
    assert(hret == HG_SUCCESS);
    hret = call_next_bulk(&bulk_concurrent, NULL);
    assert(hret == HG_SUCCESS);
    hret = cli_wait_timed(start_time);
    assert(hret == HG_SUCCESS);

    /* shutdown the servers (don't bother checking) */
    hret = HG_Create(nhcli.hgctx, rdma_svr_addr,
            nhcli.shutdown_server_rpc_id, &handle);
    assert(hret == HG_SUCCESS);
    HG_Forward(handle, NULL, NULL, NULL);
    hret = cli_wait_loop_all(10, 0, NULL);
    HG_Destroy(handle);

    if (nhcli.is_separate_servers) {
        hret = HG_Create(nhcli.hgctx, rpc_svr_addr,
                nhcli.shutdown_server_rpc_id, &handle);
        assert(hret == HG_SUCCESS);
        HG_Forward(handle, NULL, NULL, NULL);
        hret = cli_wait_loop_all(10, 0, NULL);
        HG_Destroy(handle);
    }

    /* print out resulting times
     * format:
     *   <seconds> <size> <is separate servers>
     *     rpc  isolated   <count> <avg time call> <avg time complete>
     *     rpc  concurrent <count> <avg time call> <avg time complete>
     *     bulk isolated   <count> <avg time call> <avg time complete>
     *     bulk concurrent <count> <avg time call> <avg time complete>
     */
#define PR_STAT(_cb) \
    _cb.u.times.num_complete, \
    _cb.u.times.total_time_call/_cb.u.times.num_complete, \
    _cb.u.times.total_time/_cb.u.times.num_complete

    printf( "%-8s %-8s %d %12lu %d "
            "%7d %.3e %.3e %7d %.3e %.3e "
            "%7d %.3e %.3e %7d %.3e %.3e\n",
            nhcli.class ? nhcli.class : "default", nhcli.transport,
            nhcli.is_separate_servers, nhcli.buf_sz, benchmark_seconds,
            PR_STAT(rpc_isolated), PR_STAT(rpc_concurrent),
            PR_STAT(bulk_isolated), PR_STAT(bulk_concurrent));

#undef PR_STAT

    HG_Destroy(rpc_isolated.handle);
    HG_Bulk_free(bulk_isolated.bulk);
    NA_Addr_free(nhcli.nacl, rdma_svr_addr);
    NA_Addr_free(nhcli.nacl, rpc_svr_addr);

    nahg_fini(&nhcli);
}

static void usage();

int main(int argc, char *argv[])
{
    enum mode_t mode = UNKNOWN;
    size_t rdma_size;
    char const * rdma_svr, * rpc_svr, * listen_addr;
    char const * svr_id;
    int arg = 1;

    init_verbose();

    if (argc < 2) {
        usage();
        exit(1);
    }

    for (;;) {
        if (strcmp(argv[arg], "-a") == 0) {
            fprintf(stderr, "-a currently ignored\n");
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
    else if (strcmp(argv[arg], "client") == 0)
        mode = CLIENT;
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
            rdma_svr  = argv[arg++];
            rpc_svr   = argv[arg];
            run_client(rdma_size, rdma_svr, rpc_svr);
            break;
        case SERVER:
            if (arg >= argc) {
                usage();
                exit(1);
            }
            listen_addr = argv[arg++];
            if (arg < argc)
                svr_id = argv[arg];
            else
                svr_id = NULL;
            run_server(rdma_size, listen_addr, svr_id, 0);
            break;
        default:
            assert(0);
    }

    return 0;
}


const char * usage_str =
"Usage: hg-ctest3 [-a] [-t TIME] (client | server) OPTIONS\n"
"  --all prints out every measurement, rather than an average in client mode\n"
"  -t is the time to run the benchmark in client mode\n"
"  in client mode, OPTIONS are:\n"
"    <rdma size> <rdma server> <rpc server>\n"
"  in server mode, OPTIONS are:\n"
"    <rdma size max> <listen addr> [<id>]\n"
"  servers spit out files named ctest1-server-addr.tmp[-<id>] \n"
"    containing their mercury names for clients to gobble up\n"
"  Example:\n"
"    hg-ctest1 server 1048576 bmi+tcp://localhost:3344 foo\n"
"    hg-ctest1 client 1048576 \\\n"
"      $(cat ctest1-server-addr.tmp-foo) $(cat ctest1-server-addr.tmp-foo)\n";

static void usage() {
    fprintf(stderr, usage_str);
}

