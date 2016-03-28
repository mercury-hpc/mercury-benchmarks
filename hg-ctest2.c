/*
 * Copyright 2015-2016 Argonne National Laboratory, Department of Energy,
 * UChicago Argonne, LLC and the HDF Group. See COPYING in the top-level
 * directory
 */

#include <stdio.h>
#include <assert.h>
#include <time.h>

#include <pthread.h>

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_macros.h>
#include <na_cci.h> /* need for CCI-specific grabbing of URI */

#define VERBOSE_LOG 0
#include "hg-ctest-util.h"

static int benchmark_seconds = 10;

/* wait loop condition - set in the callback routines (can't base end of
 * progress loop on a timer as there may be pending callbacks to do) */
static int stop_rpc_loop = 0;
static int stop_bulk_loop = 0;

/* threads - these need to be global so we can do error checking */
static pthread_t rpc_thread;
static pthread_t bulk_thread;

static pthread_barrier_t *barrier,
                         barrier_single, barrier_concurrent;

static struct nahg_comm_info nhcli;

/* servers */
static na_addr_t rdma_svr_addr = NA_ADDR_NULL;
static na_addr_t rpc_svr_addr = NA_ADDR_NULL;

struct bulk_thread_args {
    hg_context_t *bulk_ctx;
    hg_bulk_t bulk_local, bulk_remote;
};

struct cli_cb_loop {
    int num_complete;
    struct timespec start;
    struct timespec start_call;
    double total_time, total_time_call;
    union {
        hg_handle_t handle; /* RPC */
        struct bulk_thread_args bargs; /* bulk */
    } u;
};

static hg_return_t cli_rpc_cb(const struct hg_cb_info *info)
{
    hg_return_t hret;
    struct cli_cb_loop *loop;
    struct timespec end;
    pthread_t self;

    dprintf("rpc callback entered, ");

    assert(info->ret == HG_SUCCESS);

    clock_gettime(CLOCK_MONOTONIC, &end);

    self = pthread_self();
    assert(pthread_equal(self, rpc_thread));

    loop = (struct cli_cb_loop*) info->arg;

    loop->total_time += time_to_s_lf(timediff(loop->start_call, end));

    loop->num_complete++;
    /* call the next one */
    if (time_to_s_lf(timediff(loop->start, end)) < benchmark_seconds) {
        loop->start_call = end;
        dprintf("calling next\n");
        hret = HG_Forward(loop->u.handle, cli_rpc_cb, loop, NULL);
        clock_gettime(CLOCK_MONOTONIC, &end);
        loop->total_time_call +=
            time_to_s_lf(timediff(loop->start_call, end));
        return hret;
    }
    else {
        dprintf("finished\n");
        stop_rpc_loop = 1;
        return HG_SUCCESS;
    }
}

static void * rpc_thread_run(void * arg)
{
    struct timespec end_call;
    hg_return_t hret;
    struct cli_cb_loop *loop;
    unsigned int num_cb;
    int rc;

    (void)arg;

    loop = malloc(sizeof(*loop));
    assert(loop);

    hret = HG_Create(nhcli.hgctx, rdma_svr_addr,
            nhcli.get_bulk_handle_rpc_id, &loop->u.handle);
    if (hret != HG_SUCCESS)
        goto done;

    loop->total_time = 0;
    loop->total_time_call = 0;
    loop->num_complete = 0;

    /* sync the start time */
    rc = pthread_barrier_wait(barrier);
    assert(rc == 0 || rc == PTHREAD_BARRIER_SERIAL_THREAD);

    /* initial forward */
    clock_gettime(CLOCK_MONOTONIC, &loop->start_call);
    loop->start = loop->start_call;
    dprintf("initial rpc call\n");
    hret = HG_Forward(loop->u.handle, cli_rpc_cb, loop, NULL);
    if (hret != HG_SUCCESS)
        goto done;
    clock_gettime(CLOCK_MONOTONIC, &end_call);
    loop->total_time_call += time_to_s_lf(timediff(loop->start_call, end_call));

    stop_rpc_loop = 0;
    /* wait loop until the benchmark is over */
    do {
        do {
            hret = HG_Trigger(nhcli.hgctx, 0, 1, &num_cb);
        } while (hret == HG_SUCCESS && num_cb > 0);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            goto done;
        hret = HG_Progress(nhcli.hgctx, 100);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            goto done;
    } while (!stop_rpc_loop);

done:
    return stop_rpc_loop ? loop : NULL;
}

static hg_return_t cli_bulk_cb(const struct hg_bulk_cb_info *info)
{
    struct timespec end;
    hg_return_t hret;
    struct cli_cb_loop *loop;
    pthread_t self;

    dprintf("bulk callback entered, ");

    assert(info->ret == HG_SUCCESS);

    clock_gettime(CLOCK_MONOTONIC, &end);

    /* sanity check */
    self = pthread_self();
    assert(pthread_equal(self, bulk_thread));

    loop = (struct cli_cb_loop*) info->arg;

    loop->total_time += time_to_s_lf(timediff(loop->start_call, end));
    /* call the next one */
    loop->num_complete++;
    if (time_to_s_lf(timediff(loop->start, end)) < benchmark_seconds) {
        loop->start_call = end;
        dprintf("calling next\n");
        hret = HG_Bulk_transfer(loop->u.bargs.bulk_ctx, cli_bulk_cb, loop,
                HG_BULK_PUSH, rdma_svr_addr, loop->u.bargs.bulk_remote, 0,
                loop->u.bargs.bulk_local, 0, nhcli.buf_sz, NULL);
        clock_gettime(CLOCK_MONOTONIC, &end);
        loop->total_time_call +=
            time_to_s_lf(timediff(loop->start_call, end));
        return hret;
    }
    else {
        dprintf("finished\n");
        stop_bulk_loop = 1;
        return HG_SUCCESS;
    }
}

static void * bulk_thread_run(void * arg)
{
    struct timespec end_call;
    hg_return_t hret;
    struct cli_cb_loop *loop;
    unsigned int num_cb;
    int rc;

    loop = malloc(sizeof(*loop));
    assert(loop);

    loop->u.bargs = *(struct bulk_thread_args*)arg;

    loop->total_time = 0;
    loop->total_time_call = 0;
    loop->num_complete = 0;

    /* sync the start time */
    rc = pthread_barrier_wait(barrier);
    assert(rc == 0 || rc == PTHREAD_BARRIER_SERIAL_THREAD);

    /* initial bulk */
    clock_gettime(CLOCK_MONOTONIC, &loop->start);
    loop->start_call = loop->start;
    dprintf("initial bulk call\n");
    hret = HG_Bulk_transfer(loop->u.bargs.bulk_ctx, cli_bulk_cb, loop,
            HG_BULK_PUSH, rdma_svr_addr, loop->u.bargs.bulk_remote, 0,
            loop->u.bargs.bulk_local, 0, nhcli.buf_sz, NULL);
    if (hret != HG_SUCCESS)
        goto done;
    clock_gettime(CLOCK_MONOTONIC, &end_call);
    loop->total_time_call += time_to_s_lf(timediff(loop->start_call, end_call));

    stop_bulk_loop = 0;
    /* wait loop until all expected ops are over */
    do {
        do {
            hret = HG_Trigger(loop->u.bargs.bulk_ctx, 0, 1,
                    &num_cb);
        } while (hret == HG_SUCCESS && num_cb > 0);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            goto done;
        hret = HG_Progress(loop->u.bargs.bulk_ctx, 100);
        if (hret != HG_SUCCESS && hret != HG_TIMEOUT)
            goto done;
    } while (!stop_bulk_loop);

done:
    return stop_bulk_loop ? loop : NULL;
}

struct cli_init_cb {
    int is_finished;
    hg_bulk_t bulk_handle;
};

static hg_return_t cli_wait_loop_all(
    int max_retries,
    int num_check_cbs,
    struct cli_init_cb *cb_data)
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

static hg_return_t init_get_handle_cb(const struct hg_cb_info *info)
{
    get_bulk_handle_out_t out;
    struct cli_init_cb *out_cb;
    hg_return_t hret;

    assert(info->ret == HG_SUCCESS);

    out_cb = (struct cli_init_cb *) info->arg;

    hret = HG_Get_output(info->handle, &out);
    assert(hret == HG_SUCCESS);
    if (out_cb) {
        out_cb->bulk_handle = dup_hg_bulk(nhcli.hgcl, out.bh);
        out_cb->is_finished = 1;
    }
    HG_Free_output(info->handle, &out);

    return HG_SUCCESS;
}

static void run_client(
        size_t rdma_size,
        char const * rdma_svr,
        char const * rpc_svr)
{
    /* return codes, params */
    hg_return_t hret;
    int rc;

    /* handle etc for initial call to get bulk handle - gotta get for bulk calls */
    hg_handle_t handle;
    struct cli_init_cb cb_data;

    /* paramters for bulk thread */
    struct bulk_thread_args bargs;

    /* results */
    struct cli_cb_loop *rpc_isolated, *bulk_isolated, *rpc_concurrent,
                       *bulk_concurrent;

    /* initialize */
    nahg_init(rdma_svr, rdma_size, NA_FALSE, 0, &nhcli);

    rdma_svr_addr = lookup_serv_addr(&nhcli, rdma_svr);
    assert(rdma_svr_addr != NA_ADDR_NULL);
    rpc_svr_addr = lookup_serv_addr(&nhcli, rpc_svr);
    assert(rpc_svr_addr != NA_ADDR_NULL);

    if (strcmp(rdma_svr, rpc_svr) != 0)
        nhcli.is_separate_servers = 1;

    /* get the bulk handle */
    hret = HG_Create(nhcli.hgctx, rdma_svr_addr,
            nhcli.get_bulk_handle_rpc_id, &handle);
    assert(hret == HG_SUCCESS);
    cb_data.is_finished = 0;
    cb_data.bulk_handle = HG_BULK_NULL;
    HG_Forward(handle, init_get_handle_cb, &cb_data, NULL);
    hret = cli_wait_loop_all(20, 1, &cb_data);
    assert(hret == HG_SUCCESS);
    HG_Destroy(handle);

    /* create a separate bulk context / handle for the bulk thread to iterate on */
    bargs.bulk_remote = cb_data.bulk_handle;
    bargs.bulk_ctx = HG_Context_create(nhcli.hgcl);
    assert(bargs.bulk_ctx != NULL);
    hret = HG_Bulk_create(nhcli.hgcl, 1, &nhcli.buf, &nhcli.buf_sz,
            HG_BULK_READWRITE, &bargs.bulk_local);
    assert(hret == HG_SUCCESS);

    /* initialize benchmark barriers */
    rc = pthread_barrier_init(&barrier_single, NULL, 1);
    assert(!rc);
    rc = pthread_barrier_init(&barrier_concurrent, NULL, 2);
    assert(!rc);

    /* start up and run rpc thread by itself */
    barrier = &barrier_single;
    rc = pthread_create(&rpc_thread, NULL, rpc_thread_run, NULL);
    assert(!rc);
    rc = pthread_join(rpc_thread, (void**)&rpc_isolated);
    assert(!rc && rpc_isolated != NULL);

    /* start up and run bulk thread by itself */
    barrier = &barrier_single;
    rc = pthread_create(&bulk_thread, NULL, bulk_thread_run, &bargs);
    assert(!rc);
    rc = pthread_join(bulk_thread, (void**)&bulk_isolated);
    assert(!rc && bulk_isolated != NULL);

    /* start up and run both threads at once */
    barrier = &barrier_concurrent;
    rc = pthread_create(&rpc_thread, NULL, rpc_thread_run, NULL);
    assert(!rc);
    rc = pthread_create(&bulk_thread, NULL, bulk_thread_run, &bargs);
    assert(!rc);
    rc = pthread_join(rpc_thread, (void**)&rpc_concurrent);
    assert(!rc && rpc_concurrent != NULL);
    rc = pthread_join(bulk_thread, (void**)&bulk_concurrent);
    assert(!rc && bulk_concurrent != NULL);

    /* print out resulting times
     * format:
     *   <seconds> <size> <is separate servers>
     *     rpc  isolated   <count> <avg time call> <avg time complete>
     *     rpc  concurrent <count> <avg time call> <avg time complete>
     *     bulk isolated   <count> <avg time call> <avg time complete>
     *     bulk concurrent <count> <avg time call> <avg time complete>
     */
#define PR_STAT(_loop) \
    _loop->num_complete, \
    _loop->total_time_call/_loop->num_complete, \
    _loop->total_time/_loop->num_complete

    printf( "%-8s %-8s %d %12lu %d "
            "%7d %.3e %.3e %7d %.3e %.3e "
            "%7d %.3e %.3e %7d %.3e %.3e\n",
            nhcli.class ? nhcli.class : "default", nhcli.transport,
            nhcli.is_separate_servers, nhcli.buf_sz, benchmark_seconds,
            PR_STAT(rpc_isolated), PR_STAT(rpc_concurrent),
            PR_STAT(bulk_isolated), PR_STAT(bulk_concurrent));

#undef PR_STAT

    /* clean up */

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

    pthread_barrier_destroy(&barrier_single);
    pthread_barrier_destroy(&barrier_concurrent);
    free(rpc_isolated);
    free(bulk_isolated);
    free(rpc_concurrent);
    free(bulk_concurrent);
    HG_Bulk_free(cb_data.bulk_handle);
    HG_Context_destroy(bargs.bulk_ctx);
    NA_Addr_free(nhcli.nacl, rdma_svr_addr);
    NA_Addr_free(nhcli.nacl, rpc_svr_addr);

    nahg_fini(&nhcli);
}

static void usage(void);

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

    switch(mode) {
        case CLIENT:
            if (arg+2 >= argc ) {
                usage();
                exit(1);
            }
            rdma_size = (size_t) strtol(argv[arg++], NULL, 10);
            rdma_svr  = argv[arg++];
            rpc_svr   = argv[arg];
            run_client(rdma_size, rdma_svr, rpc_svr);
            break;
        case SERVER:
            if (arg+1 >= argc) {
                usage();
                exit(1);
            }
            rdma_size = (size_t) strtol(argv[arg++], NULL, 10);
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
"Usage: hg-ctest2 [--all] [-t TIME] (client | server) OPTIONS\n"
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

