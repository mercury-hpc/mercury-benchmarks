/*
 * Copyright 2015-2016 Argonne National Laboratory, Department of Energy,
 * UChicago Argonne, LLC and the HDF Group. See COPYING in the top-level
 * directory
 */

#ifndef HG_CTEST_UTIL_H
#define HG_CTEST_UTIL_H

#include <time.h>
#include <stdlib.h>
#include <stdio.h>

#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_macros.h>
#include <na_cci.h> /* need for CCI-specific grabbing of URI */

#if VERBOSE_LOG
#   define dprintf(_fmt, ...) fprintf(stderr, _fmt, ##__VA_ARGS__)
#   define init_verbose() \
    do {  \
        char * iobuf = malloc(1<<22); \
        int ret = setvbuf(stderr, iobuf, _IOFBF, 1<<22); \
        assert(ret == 0); \
    } while (0)
#else
#   define dprintf(_fmt, ...)
#   define init_verbose() do { } while(0)
#endif

/* filename that server addresses get written to */

extern char const * const ADDR_FNAME;

/* timing utilities */

static inline struct timespec timediff(
        struct timespec start,
        struct timespec end)
{
    struct timespec temp;
    if ((end.tv_nsec-start.tv_nsec)<0) {
        temp.tv_sec = end.tv_sec-start.tv_sec-1;
        temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    } else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
    return temp;
}

static inline double time_to_ms_lf(struct timespec t){
        return (double) t.tv_sec * 1e3 + (double) t.tv_nsec / 1e6;
}
static inline double time_to_us_lf(struct timespec t){
        return (double) t.tv_sec * 1e6 + (double) t.tv_nsec / 1e3;
}
static inline double time_to_s_lf(struct timespec t){
        return (double) t.tv_sec + (double) t.tv_nsec / 1e9;
}

/* program running modes */
enum mode_t {
    CLIENT,
    SERVER,
    UNKNOWN
};

/* mercury/NA control structure */

struct nahg_comm_info {
    na_class_t *nacl;
    na_context_t *nactx;
    na_addr_t self;

    hg_class_t *hgcl;
    hg_context_t *hgctx;

    hg_bulk_class_t *hbcl;
    hg_bulk_context_t *hbctx;
    hg_bulk_t bh;

    hg_id_t check_in_id;
    hg_id_t noop_rpc_id;
    hg_id_t get_bulk_handle_rpc_id;
    hg_id_t shutdown_server_rpc_id;
    hg_id_t bulk_read_rpc_id;

    /* checkin state */
    int num_to_check_in;
    int num_checked_in;
    hg_handle_t *checkin_handles;

    void *buf;
    size_t buf_sz;

    char * class;
    char * transport;
    char * hoststr;

    /* filled in by clients at runtime */
    int is_separate_servers;
};

/* RPC processing def (the proc fn is static so this is OK */
MERCURY_GEN_PROC(get_bulk_handle_out_t, ((hg_bulk_t)(bh)))
MERCURY_GEN_PROC(bulk_read_in_t, ((hg_bulk_t)(bh)))

/* init/fini code for ^ */
void nahg_init(
        char const *info_str,
        size_t buf_sz,
        na_bool_t listen,
        int checkin_count,
        struct nahg_comm_info *nh);
void nahg_fini(struct nahg_comm_info *nh);

/* server RPC handlers */
hg_return_t check_in(hg_handle_t handle);
hg_return_t noop(hg_handle_t handle);
hg_return_t get_bulk_handle(hg_handle_t handle);
hg_return_t shutdown_server(hg_handle_t handle);
hg_return_t bulk_read(hg_handle_t handle);

/* main loop for server */
void run_server(
        size_t rdma_size,
        char const * listen_addr,
        char const * id_str,
        int num_checkins);

/* misc util */
hg_bulk_t dup_hg_bulk(hg_bulk_class_t *cl, hg_bulk_t in);

#endif /* end of include guard: HG_CTEST_UTIL_H */
