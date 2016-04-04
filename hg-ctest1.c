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

const int NUM_REPS = 100;
const int WARMUP = 20;
int output_all_times = 0;

/* this needs to be a global to pass around between callback functions and
 * whatnot */
static struct nahg_comm_info nhcli;

struct cli_cb_data {
    hg_bulk_t bulk_handle;
    int is_finished;
    struct timespec ts;
};

static hg_return_t get_bulk_handle_cli_cb(const struct hg_cb_info *info)
{
    get_bulk_handle_out_t out;
    struct cli_cb_data *out_cb;
    hg_return_t hret;
    /* sadly, have to copyout the bulk handle, which is awkward */

    dprintf("rpc callback entered\n");

    assert(info->ret == HG_SUCCESS);
    out_cb = (struct cli_cb_data*) info->arg;
    hret = HG_Get_output(info->info.forward.handle, &out);
    assert(hret == HG_SUCCESS);
    if (out_cb) {
        clock_gettime(CLOCK_MONOTONIC, &out_cb->ts);
        out_cb->bulk_handle = dup_hg_bulk(nhcli.hgcl, out.bh);
        out_cb->is_finished = 1;
    }
    HG_Free_output(info->info.forward.handle, &out);
    return HG_SUCCESS;
}

static hg_return_t cli_bulk_xfer_cb(const struct hg_cb_info *info)
{
    struct cli_cb_data * out_cb = info->arg;
    assert(info->ret == HG_SUCCESS);
    clock_gettime(CLOCK_MONOTONIC, &out_cb->ts);
    dprintf("bulk callback entered\n");
    out_cb->is_finished = 1;
    return HG_SUCCESS;
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
                if (!cb_data[c].is_finished)
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
    /* servers */
    na_addr_t rdma_svr_addr = NA_ADDR_NULL;
    na_addr_t rpc_svr_addr = NA_ADDR_NULL;

    /* RPC params */
    hg_handle_t handle;
    hg_bulk_t bulk_handle;
    struct cli_cb_data cb_data[2];
    struct cli_cb_data *cb_data_bulk = &cb_data[0], *cb_data_rpc = &cb_data[1];

    /* timing params */
    struct timespec ts_bulk_start, ts_bulk_end,
                    ts_get_bulk_start, ts_get_bulk_end;

    struct cli_times {
        double isolated_cb, isolated_call;
        double first_cb, first_call;
        double last_cb, last_call;
    };

    struct cli_times *rpc_times, *bulk_times;
    struct cli_times
        rpc_avg  = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0},
        bulk_avg = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

    int r;

    /* return params */
    hg_return_t hret;

    /* initialize */
    nahg_init(rdma_svr, rdma_size, NA_FALSE, 0, &nhcli);

    rdma_svr_addr = lookup_serv_addr(&nhcli, rdma_svr);
    assert(rdma_svr_addr != NA_ADDR_NULL);
    rpc_svr_addr = lookup_serv_addr(&nhcli, rpc_svr);
    assert(rpc_svr_addr != NA_ADDR_NULL);

    if (strcmp(rdma_svr, rpc_svr) != 0)
        nhcli.is_separate_servers = 1;

    cb_data_bulk->is_finished = 0;
    cb_data_bulk->bulk_handle = HG_BULK_NULL;
    cb_data_rpc->is_finished = 0;
    cb_data_rpc->bulk_handle = HG_BULK_NULL;

    /* create, run RPC to grab bulk handle from rdma server */

    hret = HG_Create(nhcli.hgctx, rdma_svr_addr,
            nhcli.get_bulk_handle_rpc_id, &handle);
    assert(hret == HG_SUCCESS);

    HG_Forward(handle, get_bulk_handle_cli_cb, cb_data_rpc, NULL);

    hret = cli_wait_loop_all(20, 1, cb_data_rpc);

    assert(hret == HG_SUCCESS);

    bulk_handle = cb_data_rpc->bulk_handle;

    HG_Destroy(handle);

    /* allocate times */
    rpc_times  = malloc(NUM_REPS * sizeof(*rpc_times));
    bulk_times = malloc(NUM_REPS * sizeof(*bulk_times)); 

    /* get our base timings: no concurrency */

    hret = HG_Create(nhcli.hgctx, rpc_svr_addr,
            nhcli.get_bulk_handle_rpc_id, &handle);
    assert(hret == HG_SUCCESS);

    for (r = 0 ; r < NUM_REPS+WARMUP; r++) {
        cb_data_rpc->is_finished = 0;
        dprintf("rpc, iteration %d\n", r);
        clock_gettime(CLOCK_MONOTONIC, &ts_get_bulk_start);
        hret = HG_Forward(handle, get_bulk_handle_cli_cb, cb_data_rpc, NULL);
        assert(hret == HG_SUCCESS);
        clock_gettime(CLOCK_MONOTONIC, &ts_get_bulk_end);
        hret = cli_wait_loop_all(100, 1, cb_data_rpc);
        assert(hret == HG_SUCCESS);
        if (r >= WARMUP) {
            rpc_times[r-WARMUP].isolated_cb =
                time_to_s_lf(timediff(ts_get_bulk_start, cb_data_rpc->ts));
            rpc_times[r-WARMUP].isolated_call =
                time_to_s_lf(timediff(ts_get_bulk_start, ts_get_bulk_end));
        }
    }
    for (r = 0; r < NUM_REPS+WARMUP; r++) {
        cb_data_bulk->is_finished = 0;
        dprintf("bulk, iteration %d\n", r);
        clock_gettime(CLOCK_MONOTONIC, &ts_bulk_start);
        hret = HG_Bulk_transfer(nhcli.hgctx, cli_bulk_xfer_cb, cb_data_bulk,
                HG_BULK_PUSH, rdma_svr_addr, bulk_handle, 0, nhcli.bh,
                0, nhcli.buf_sz, HG_OP_ID_IGNORE);
        clock_gettime(CLOCK_MONOTONIC, &ts_bulk_end);
        assert(hret == HG_SUCCESS);
        hret = cli_wait_loop_all(100, 1, cb_data_bulk);
        assert(hret == HG_SUCCESS);
        if (r >= WARMUP) {
            bulk_times[r-WARMUP].isolated_cb =
                time_to_s_lf(timediff(ts_bulk_start, cb_data_bulk->ts));
            bulk_times[r-WARMUP].isolated_call =
                time_to_s_lf(timediff(ts_bulk_start, ts_bulk_end));
        }
    }

    /* now do bulk first, rpc second */
    for (r = 0; r < NUM_REPS+WARMUP; r++) {
        cb_data_rpc->is_finished = 0;
        cb_data_bulk->is_finished = 0;
        dprintf("bulk*+rpc, iteration %d\n", r);
        clock_gettime(CLOCK_MONOTONIC, &ts_bulk_start);
        hret = HG_Bulk_transfer(nhcli.hgctx, cli_bulk_xfer_cb, cb_data_bulk,
                HG_BULK_PUSH, rdma_svr_addr, bulk_handle, 0, nhcli.bh,
                0, nhcli.buf_sz, HG_OP_ID_IGNORE);
        assert(hret == HG_SUCCESS);
        clock_gettime(CLOCK_MONOTONIC, &ts_get_bulk_start);
        ts_bulk_end = ts_get_bulk_start;
        dprintf("bulk+rpc*, iteration %d\n", r);
        hret = HG_Forward(handle, get_bulk_handle_cli_cb, cb_data_rpc, NULL);
        assert(hret == HG_SUCCESS);
        clock_gettime(CLOCK_MONOTONIC, &ts_get_bulk_end);
        hret = cli_wait_loop_all(100, 2, cb_data);
        assert(hret == HG_SUCCESS);
        if (r >= WARMUP) {
            rpc_times[r-WARMUP].last_cb =
                time_to_s_lf(timediff(ts_get_bulk_start, cb_data_rpc->ts));
            rpc_times[r-WARMUP].last_call =
                time_to_s_lf(timediff(ts_get_bulk_start, ts_get_bulk_end));
            bulk_times[r-WARMUP].first_cb =
                time_to_s_lf(timediff(ts_bulk_start, cb_data_bulk->ts));
            bulk_times[r-WARMUP].first_call =
                time_to_s_lf(timediff(ts_bulk_start, ts_bulk_end));
        }
    }

    /* finally, do rpc first, bulk second */
    for (r = 0; r < NUM_REPS+WARMUP; r++) {
        cb_data_rpc->is_finished = 0;
        cb_data_bulk->is_finished = 0;
        dprintf("rpc*+bulk, iteration %d\n", r);
        clock_gettime(CLOCK_MONOTONIC, &ts_get_bulk_start);
        hret = HG_Forward(handle, get_bulk_handle_cli_cb, cb_data_rpc, NULL);
        assert(hret == HG_SUCCESS);
        dprintf("rpc+bulk*, iteration %d\n", r);
        clock_gettime(CLOCK_MONOTONIC, &ts_bulk_start);
        ts_get_bulk_end = ts_bulk_start;
        hret = HG_Bulk_transfer(nhcli.hgctx, cli_bulk_xfer_cb, cb_data_bulk,
                HG_BULK_PUSH, rdma_svr_addr, bulk_handle, 0, nhcli.bh,
                0, nhcli.buf_sz, HG_OP_ID_IGNORE);
        assert(hret == HG_SUCCESS);
        clock_gettime(CLOCK_MONOTONIC, &ts_bulk_end);
        hret = cli_wait_loop_all(100, 2, cb_data);
        assert(hret == HG_SUCCESS);
        if (r >= WARMUP) {
            rpc_times[r-WARMUP].first_cb =
                time_to_s_lf(timediff(ts_get_bulk_start, cb_data_rpc->ts));
            rpc_times[r-WARMUP].first_call =
                time_to_s_lf(timediff(ts_get_bulk_start, ts_get_bulk_end));
            bulk_times[r-WARMUP].last_cb =
                time_to_s_lf(timediff(ts_bulk_start, cb_data_bulk->ts));
            bulk_times[r-WARMUP].last_call =
                time_to_s_lf(timediff(ts_bulk_start, ts_bulk_end));
        }
    }

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

#define PRINT_RECORD(_rpc, _bulk) \
    printf("%-8s %-8s %1d %12lu %3d " \
           "%1.3e %1.3e %1.3e %1.3e %1.3e %1.3e " \
           "%1.3e %1.3e %1.3e %1.3e %1.3e %1.3e\n", \
            nhcli.class ? nhcli.class : "default", nhcli.transport, \
            nhcli.is_separate_servers, nhcli.buf_sz, NUM_REPS, \
            _rpc.isolated_call, _rpc.isolated_cb, \
            _rpc.first_call, _rpc.first_cb, \
            _rpc.last_call, _rpc.last_cb, \
            _bulk.isolated_call, _bulk.isolated_cb, \
            _bulk.first_call, _bulk.first_cb, \
            _bulk.last_call, _bulk.last_cb)
    /* finally, print out the results, format:
     * class, transport, separate servers used (bool)
     * size, repetitions,
     * isolated, concurrent (me-first), concurrent (me-last) rpc time,
     * "                                                   " bulk time
     * each measurement includes both the async call time and the full time
     * including callback */
    if (output_all_times) {
        for (r = 0; r < NUM_REPS; r++) {
            PRINT_RECORD(rpc_times[r], bulk_times[r]);
        }
    }
    else {
        for (r = 0; r < NUM_REPS; r++) {
            rpc_avg.isolated_cb += rpc_times[r].isolated_cb;
            rpc_avg.isolated_call += rpc_times[r].isolated_call;
            rpc_avg.first_cb += rpc_times[r].first_cb;
            rpc_avg.first_call += rpc_times[r].first_call;
            rpc_avg.last_cb += rpc_times[r].last_cb;
            rpc_avg.last_call += rpc_times[r].last_call;
            bulk_avg.isolated_cb += bulk_times[r].isolated_cb;
            bulk_avg.isolated_call += bulk_times[r].isolated_call;
            bulk_avg.first_cb += bulk_times[r].first_cb;
            bulk_avg.first_call += bulk_times[r].first_call;
            bulk_avg.last_cb += bulk_times[r].last_cb;
            bulk_avg.last_call += bulk_times[r].last_call;
        }
        rpc_avg.isolated_cb /= NUM_REPS;
        rpc_avg.isolated_call /= NUM_REPS;
        rpc_avg.first_cb /= NUM_REPS;
        rpc_avg.first_call /= NUM_REPS;
        rpc_avg.last_cb /= NUM_REPS;
        rpc_avg.last_call /= NUM_REPS;
        bulk_avg.isolated_cb /= NUM_REPS;
        bulk_avg.isolated_call /= NUM_REPS;
        bulk_avg.first_cb /= NUM_REPS;
        bulk_avg.first_call /= NUM_REPS;
        bulk_avg.last_cb /= NUM_REPS;
        bulk_avg.last_call /= NUM_REPS;

        PRINT_RECORD(rpc_avg, bulk_avg);
    }

#undef PRINT_RECORD

    free(rpc_times);
    free(bulk_times);

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

    init_verbose();

    if (argc < 2) {
        usage();
        exit(1);
    }

    if (strcmp(argv[1], "-a") == 0) {
        output_all_times = 1;
        argv++;
    }

    if (strcmp(argv[1], "client") == 0)
        mode = CLIENT;
    else if (strcmp(argv[1], "server") == 0)
        mode = SERVER;
    else {
        usage();
        exit(1);
    }

    switch(mode) {
        case CLIENT:
            if (argc < 5) {
                usage();
                exit(1);
            }
            rdma_size = (size_t) strtol(argv[2], NULL, 10);
            rdma_svr  = argv[3];
            rpc_svr   = argv[4];
            run_client(rdma_size, rdma_svr, rpc_svr);
            break;
        case SERVER:
            if (argc < 4) {
                usage();
                exit(1);
            }
            rdma_size = (size_t) strtol(argv[2], NULL, 10);
            listen_addr = argv[3];
            if (argc >= 5)
                svr_id = argv[4];
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
"Usage: hg-ctest1 [--all] (client | server) OPTIONS\n"
"  --all prints out every measurement, rather than an average\n"
"  in client mode, OPTIONS are:\n"
"    <rdma size> <rdma server> <rpc server>\n"
"  in server mode, OPTIONS are:\n"
"    <rdma size max> <listen addr> [<id>]\n"
"  servers spit out files named ctest1-server-addr.tmp[-<id>] \n"
"    containing their mercury names for clients to gobble up\n"
"  Example:\n"
"    hg-ctest1 server 1048576 tcp://localhost:3344 foo\n"
"    hg-ctest1 client 1048576 \\\n"
"      $(cat ctest1-server-addr.tmp-foo) $(cat ctest1-server-addr.tmp-foo)\n";

static void usage() {
    fprintf(stderr, usage_str);
}

