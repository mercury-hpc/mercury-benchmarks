/*
 * Copyright 2015-2016 Argonne National Laboratory, Department of Energy,
 * UChicago Argonne, LLC and the HDF Group. See COPYING in the top-level
 * directory
 */

#include "hg-ctest-util.h"
#include <assert.h>

/* generic server mercury setup */
static struct nahg_comm_info nhserv;

char const * const ADDR_FNAME = "ctest1-server-addr.tmp";

void nahg_init(
        char const *info_str,
        size_t buf_sz,
        na_bool_t listen,
        int checkin_count,
        struct nahg_comm_info *nh)
{
    hg_size_t hsz;
    hg_return_t hret;
    na_return_t nret;
    char const * class_end;
    char const * transport_end;

    nh->buf = calloc(buf_sz, 1);
    assert(nh->buf);
    nh->buf_sz = buf_sz;

    nh->is_separate_servers = 0;

    /* extract the class and transport for printing purposes */
    class_end = strchr(info_str, '+');
    transport_end = strchr(info_str, ':');
    assert(transport_end != NULL);

    // strings passed in of form class+transport://foo - in some cases we
    // want just foo (specifically for the mpi transport)
    nh->hoststr = strdup(transport_end + 3);

    if (class_end == NULL) {
        nh->class = NULL;
        nh->transport = strndup(info_str, (size_t)(transport_end-info_str));
    }
    else {
        char const * transport_begin = class_end+1;
        nh->class = strndup(info_str, (size_t)(class_end-info_str));
        nh->transport = strndup(transport_begin, (size_t)(transport_end-transport_begin));
    }

    // complete hack - mpi addresses are not to be trifled with
    if (strcmp(nh->class, "mpi") == 0) {
        if (strcmp(nh->transport, "static") == 0)
            nh->nacl = NA_Initialize("mpi+static://localhost:1111", listen);
        else
            nh->nacl = NA_Initialize("mpi+tcp://localhost:1111", listen);
    }
    else
        nh->nacl = NA_Initialize(info_str, listen);

    assert(nh->nacl);
    nh->nactx = NA_Context_create(nh->nacl);
    assert(nh->nactx);

    nh->hgcl = HG_Init_na(nh->nacl, nh->nactx);
    assert(nh->hgcl);
    nh->hgctx = HG_Context_create(nh->hgcl);
    assert(nh->hgctx);

    /* initialize the checkin state */
    nh->num_to_check_in = checkin_count;
    nh->num_checked_in = 0;
    nh->checkin_handles = malloc(checkin_count * sizeof(nh->checkin_handles));
    for (int i = 0; i < checkin_count; i++)
        nh->checkin_handles[i] = HG_HANDLE_NULL;

    nh->check_in_id = MERCURY_REGISTER(nh->hgcl, "check_in",
            void, void, check_in);
    nh->get_bulk_handle_rpc_id = MERCURY_REGISTER(nh->hgcl, "get_bulk_handle",
            void, get_bulk_handle_out_t, get_bulk_handle);
    nh->shutdown_server_rpc_id = MERCURY_REGISTER(nh->hgcl, "shutdown_server",
            void, void, shutdown_server);
    nh->noop_rpc_id = MERCURY_REGISTER(nh->hgcl, "noop",
            void, void, noop);
    nh->bulk_read_rpc_id = MERCURY_REGISTER(nh->hgcl, "bulk_read",
            bulk_read_in_t, void, bulk_read);

    nret = NA_Addr_self(nh->nacl, &nh->self);
    assert(nret == NA_SUCCESS);

    hsz = buf_sz;
    hret = HG_Bulk_create(nh->hgcl, 1, &nh->buf, &hsz, HG_BULK_READWRITE,
            &nh->bh);
    assert(hret == HG_SUCCESS);
}

void nahg_fini(struct nahg_comm_info *nh)
{
    free(nh->buf);
    free(nh->class);
    free(nh->transport);
    free(nh->hoststr);
    free(nh->checkin_handles);
    hg_return_t hret;
    na_return_t nret;
    hret = HG_Bulk_free(nh->bh); assert(hret == HG_SUCCESS);
    hret = HG_Context_destroy(nh->hgctx); assert(hret == HG_SUCCESS);
    hret = HG_Finalize(nh->hgcl); assert(hret == HG_SUCCESS);
    nret = NA_Addr_free(nh->nacl, nh->self); assert(nret == NA_SUCCESS);
    nret = NA_Context_destroy(nh->nacl, nh->nactx); assert(nret == NA_SUCCESS);
    /* This can fail for some reason in cci... spits out
     * "Completion queue should be empty" - don't assert */
    NA_Finalize(nh->nacl);
}

hg_return_t check_in(hg_handle_t handle)
{
    assert(nhserv.num_checked_in < nhserv.num_to_check_in);
    nhserv.checkin_handles[nhserv.num_checked_in] = handle;
    nhserv.num_checked_in++;
    dprintf("server recv checkin %d of %d\n", nhserv.num_checked_in, nhserv.num_to_check_in);

    if (nhserv.num_checked_in == nhserv.num_to_check_in) {
        hg_return_t hret_end = HG_SUCCESS;
        for (int i = 0; i < nhserv.num_to_check_in; i++) {
            dprintf("server responding to %d\n", i);
            hg_return_t hret =
                HG_Respond(nhserv.checkin_handles[i], NULL, NULL, NULL);
            if (hret != HG_SUCCESS) hret_end = hret;
        }
        for (int i = 0; i < nhserv.num_to_check_in; i++)
            HG_Destroy(nhserv.checkin_handles[i]);
        nhserv.num_checked_in = 0;
        dprintf("server done issuing responds, returning\n");
        return hret_end;
    }
    else
        return HG_SUCCESS;
}

hg_return_t noop(hg_handle_t handle)
{
    hg_return_t hret = HG_Respond(handle, NULL, NULL, NULL);
    assert(hret == HG_SUCCESS);
    HG_Destroy(handle);
    return hret;
}

hg_return_t get_bulk_handle(hg_handle_t handle)
{
    hg_return_t hret;
    get_bulk_handle_out_t out;

    out.bh = nhserv.bh;

    hret = HG_Respond(handle, NULL, NULL, &out);
    assert(hret == HG_SUCCESS);

    HG_Destroy(handle);
    return hret;
}

static int do_shutdown = 0;

hg_return_t shutdown_server(hg_handle_t handle)
{
    hg_return_t hret;
    hret = HG_Respond(handle, NULL, NULL, NULL);
    HG_Destroy(handle);
    printf("server received shutdown request\n");
    do_shutdown = 1;
    return hret;
}

static hg_return_t bulk_read_continuation(
        const struct hg_cb_info *callback_info)
{
    hg_handle_t h = callback_info->arg;
    hg_return_t hret = HG_Respond(h, NULL, NULL, NULL);
    return hret;
}

hg_return_t bulk_read(hg_handle_t handle)
{
    // get bulk handle to read from
    hg_return_t hret;
    bulk_read_in_t in;
    hret = HG_Get_input(handle, &in);
    assert(hret == HG_SUCCESS);
    hg_size_t in_buf_sz = HG_Bulk_get_size(in.bh);

    struct hg_info *info = HG_Get_info(handle);

    // create bulk handle to write to local buffer
    hg_bulk_t wrbulk = HG_BULK_NULL;
    hg_size_t buf_sz = nhserv.buf_sz;
    hret = HG_Bulk_create(nhserv.hgcl, 1,
            &nhserv.buf, &buf_sz, HG_BULK_WRITE_ONLY, &wrbulk);
    assert(hret == HG_SUCCESS);

    // perform the bulk transfer
    na_return_t nret = NA_Addr_self(nhserv.nacl, &nhserv.self);
    assert(nret == NA_SUCCESS);
    hret = HG_Bulk_transfer(nhserv.hgctx, bulk_read_continuation,
        handle, HG_BULK_PULL, info->addr, in.bh, 0, wrbulk, 0,
        in_buf_sz > buf_sz ? buf_sz : in_buf_sz, HG_OP_ID_NULL);
    assert(hret == HG_SUCCESS);

    HG_Free_input(handle, &in);
    HG_Destroy(handle);

    return HG_SUCCESS;
}

void run_server(
        size_t rdma_size,
        char const * listen_addr,
        char const * id_str,
        int num_checkins)
{
    hg_return_t hret;
    na_return_t nret;
    unsigned int num_cb;
    FILE *f;
    char * nm;
    const char * cl;
    na_size_t nm_len = 256;
    char * fname;

    if (id_str) {
        fname = malloc(strlen(id_str)+strlen(ADDR_FNAME)+2);
        assert(fname);
        sprintf(fname, "%s-%s", ADDR_FNAME, id_str);
    }
    else
        fname = strdup(ADDR_FNAME);

    nahg_init(listen_addr, rdma_size, NA_TRUE, num_checkins, &nhserv);

    /* print out server addr to file */
    f = fopen(fname, "w");
    assert(f);

    /* TODO: would be nice to be able to get the network class out of mercury,
     * but for now this will do
     * default: choose bmi */
    if (strncmp("cci", listen_addr, 3) == 0)
        cl = "cci+";
    else if (strncmp("bmi", listen_addr, 3) == 0)
        cl = "bmi+";
    else if (strncmp("mpi", listen_addr, 3) == 0)
        // for clients to read the class/transport and address string,
        // need to put in a dummy transport, URI str to read correctly
        cl = "mpi+tcp://";
    else
        cl = "bmi+";

    nm = malloc(nm_len);
    assert(nm);
    nret = NA_Addr_to_string(nhserv.nacl, nm, &nm_len, nhserv.self);
    assert(nret == NA_SUCCESS);

    fprintf(f, "%s%s\n", cl, nm);
    fclose(f);

    free(nm);
    free(fname);

    /* unclear whether this is the correct processing loop or not for single
     * threaded */
    do {
        do {
            hret = HG_Trigger(nhserv.hgctx, 0, 1, &num_cb);
        } while(hret == HG_SUCCESS && num_cb == 1);
        hret = HG_Progress(nhserv.hgctx, 1000);
    } while((hret == HG_SUCCESS || hret == HG_TIMEOUT) && !do_shutdown);

    nahg_fini(&nhserv);
}

hg_bulk_t dup_hg_bulk(hg_class_t *cl, hg_bulk_t in)
{
    hg_bulk_t rtn;
    hg_size_t sz;
    hg_return_t hret;
    void * buf;

    sz = HG_Bulk_get_serialize_size(in, HG_FALSE);
    buf = malloc(sz);
    if (!buf)
        return HG_BULK_NULL;
    hret = HG_Bulk_serialize(buf, sz, HG_FALSE, in);
    if (hret != HG_SUCCESS)
        return HG_BULK_NULL;
    hret = HG_Bulk_deserialize(cl, &rtn, buf, sz);
    free(buf);
    if (hret != HG_SUCCESS)
        return HG_BULK_NULL;
    else
        return rtn;
}

typedef struct serv_addr_out
{
    na_addr_t addr;
    int set;
} serv_addr_out_t;

// helper for lookup_serv_addr
static na_return_t lookup_serv_addr_cb(const struct na_cb_info *info)
{
    serv_addr_out_t *out = info->arg;
    out->addr = info->info.lookup.addr;
    out->set = 1;
    return NA_SUCCESS;
}

na_addr_t lookup_serv_addr(struct nahg_comm_info *nahg, const char *info_str)
{
    serv_addr_out_t out;
    na_return_t nret;

    out.addr = NA_ADDR_NULL;
    out.set = 0;

    nret = NA_Addr_lookup(nahg->nacl, nahg->nactx, &lookup_serv_addr_cb, &out,
            info_str, NA_OP_ID_IGNORE);
    assert(nret == NA_SUCCESS);

    // run the progress loop until we've got the output
    do {
        unsigned int count = 0;
        do {
            nret = NA_Trigger(nahg->nactx, 0, 1, &count);
        } while (nret == NA_SUCCESS && count > 0);

        if (out.set != 0) break;

        nret = NA_Progress(nahg->nacl, nahg->nactx, 5000);
    } while(nret == NA_SUCCESS || nret == NA_TIMEOUT);

    return out.addr;
}
