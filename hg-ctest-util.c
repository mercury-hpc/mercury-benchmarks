/*
 * Copyright 2015-2016 Argonne National Laboratory, Department of Energy,
 * UChicago Argonne, LLC and the HDF Group. See COPYING in the top-level
 * directory
 */

#include "hg-ctest-util.h"
#include <assert.h>

/* generic server mercury setup */
static struct hg_comm_info hserv;

char const * const ADDR_FNAME = "ctest-server-addr.tmp";

void hg_init(
        char const *info_str,
        size_t buf_sz,
        hg_bool_t listen,
        int checkin_count,
        struct hg_comm_info *h)
{
    hg_size_t hsz;
    hg_return_t hret;

    h->buf = calloc(buf_sz, 1);
    assert(h->buf);
    h->buf_sz = buf_sz;

    h->is_separate_servers = 0;

    h->hgcl = HG_Init(info_str, listen);
    assert(h->hgcl != NULL);
    h->hgctx = HG_Context_create(h->hgcl);
    assert(h->hgctx != NULL);

    h->class = HG_Class_get_name(h->hgcl);
    assert(h->class != NULL);
    h->transport = HG_Class_get_protocol(h->hgcl);
    assert(h->transport != NULL);

    /* initialize the checkin state */
    h->num_to_check_in = checkin_count;
    h->num_checked_in = 0;
    h->checkin_handles = malloc(checkin_count * sizeof(h->checkin_handles));
    for (int i = 0; i < checkin_count; i++)
        h->checkin_handles[i] = HG_HANDLE_NULL;

    h->check_in_id = MERCURY_REGISTER(h->hgcl, "check_in",
            void, void, check_in);
    h->get_bulk_handle_rpc_id = MERCURY_REGISTER(h->hgcl, "get_bulk_handle",
            void, get_bulk_handle_out_t, get_bulk_handle);
    h->shutdown_server_rpc_id = MERCURY_REGISTER(h->hgcl, "shutdown_server",
            void, void, shutdown_server);
    h->noop_rpc_id = MERCURY_REGISTER(h->hgcl, "noop",
            void, void, noop);
    h->bulk_read_rpc_id = MERCURY_REGISTER(h->hgcl, "bulk_read",
            bulk_read_in_t, void, bulk_read);

    hret = HG_Addr_self(h->hgcl, &h->self);
    assert(hret == HG_SUCCESS);

    hsz = buf_sz;
    hret = HG_Bulk_create(h->hgcl, 1, &h->buf, &hsz, HG_BULK_READWRITE,
            &h->bh);
    assert(hret == HG_SUCCESS);
}

void hg_fini(struct hg_comm_info *h)
{
    free(h->buf);
    free(h->checkin_handles);
    hg_return_t hret;
    hret = HG_Bulk_free(h->bh); assert(hret == HG_SUCCESS);
    hret = HG_Context_destroy(h->hgctx); assert(hret == HG_SUCCESS);
    hret = HG_Addr_free(h->hgcl, h->self); assert(hret == HG_SUCCESS);
    hret = HG_Finalize(h->hgcl); assert(hret == HG_SUCCESS);
}

hg_return_t check_in(hg_handle_t handle)
{
    assert(hserv.num_checked_in < hserv.num_to_check_in);
    hserv.checkin_handles[hserv.num_checked_in] = handle;
    hserv.num_checked_in++;
    dprintf("server recv checkin %d of %d\n", hserv.num_checked_in,
            hserv.num_to_check_in);

    if (hserv.num_checked_in == hserv.num_to_check_in) {
        hg_return_t hret_end = HG_SUCCESS;
        for (int i = 0; i < hserv.num_to_check_in; i++) {
            dprintf("server responding to %d\n", i);
            hg_return_t hret =
                HG_Respond(hserv.checkin_handles[i], NULL, NULL, NULL);
            if (hret != HG_SUCCESS) hret_end = hret;
        }
        for (int i = 0; i < hserv.num_to_check_in; i++)
            HG_Destroy(hserv.checkin_handles[i]);
        hserv.num_checked_in = 0;
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

    out.bh = hserv.bh;

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
    hg_size_t buf_sz = hserv.buf_sz;
    hret = HG_Bulk_create(hserv.hgcl, 1,
            &hserv.buf, &buf_sz, HG_BULK_WRITE_ONLY, &wrbulk);
    assert(hret == HG_SUCCESS);

    // perform the bulk transfer
    hret = HG_Addr_self(hserv.hgcl, &hserv.self);
    assert(hret == HG_SUCCESS);
    hret = HG_Bulk_transfer(hserv.hgctx, bulk_read_continuation,
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
    unsigned int num_cb;
    FILE *f;
    char * nm;
    hg_size_t nm_len = 256;
    char * fname;

    if (id_str) {
        fname = malloc(strlen(id_str)+strlen(ADDR_FNAME)+2);
        assert(fname);
        sprintf(fname, "%s-%s", ADDR_FNAME, id_str);
    }
    else
        fname = strdup(ADDR_FNAME);

    hg_init(listen_addr, rdma_size, HG_TRUE, num_checkins, &hserv);

    /* print out server addr to file */
    f = fopen(fname, "w");
    assert(f);

    nm = malloc(nm_len);
    assert(nm);
    hret = HG_Addr_to_string(hserv.hgcl, nm, &nm_len, hserv.self);
    assert(hret == HG_SUCCESS);

    fprintf(f, "%s\n", nm);
    fclose(f);

    free(nm);
    free(fname);

    /* unclear whether this is the correct processing loop or not for single
     * threaded */
    do {
        do {
            hret = HG_Trigger(hserv.hgctx, 0, 1, &num_cb);
        } while(hret == HG_SUCCESS && num_cb == 1);
        hret = HG_Progress(hserv.hgctx, 1000);
    } while((hret == HG_SUCCESS || hret == HG_TIMEOUT) && !do_shutdown);

    hg_fini(&hserv);
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
    hg_addr_t addr;
    int set;
} serv_addr_out_t;

// helper for lookup_serv_addr
static hg_return_t lookup_serv_addr_cb(const struct hg_cb_info *info)
{
    serv_addr_out_t *out = info->arg;
    out->addr = info->info.lookup.addr;
    out->set = 1;
    return HG_SUCCESS;
}

hg_addr_t lookup_serv_addr(struct hg_comm_info *hg, const char *info_str)
{
    serv_addr_out_t out;
    hg_return_t hret;

    out.addr = HG_ADDR_NULL;
    out.set = 0;

    hret = HG_Addr_lookup(hg->hgctx, &lookup_serv_addr_cb, &out, info_str,
            HG_OP_ID_IGNORE);
    assert(hret == HG_SUCCESS);

    // run the progress loop until we've got the output
    do {
        unsigned int count = 0;
        do {
            hret = HG_Trigger(hg->hgctx, 0, 1, &count);
        } while (hret == HG_SUCCESS && count > 0);

        if (out.set != 0) break;

        hret = HG_Progress(hg->hgctx, 5000);
    } while(hret == HG_SUCCESS || hret == HG_TIMEOUT);

    return out.addr;
}
