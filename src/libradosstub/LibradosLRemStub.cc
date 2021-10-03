// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LibradosLRemStub.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/snap_types.h"
#include "common/armor.h"
#include "librados/AioCompletionImpl.h"
#include "librados/PoolAsyncCompletionImpl.h"
#include "log/Log.h"
#include "LRemClassHandler.h"
#include "LRemIoCtxImpl.h"
#include "LRemRadosClient.h"
#include "mod/db/LRemDBCluster.h"
#include "objclass/objclass.h"
#include "osd/osd_types.h"
#include <arpa/inet.h>
#include <boost/shared_ptr.hpp>
#include <deque>
#include <functional>
#include <list>
#include <vector>
#include "include/ceph_assert.h"
#include "include/compat.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "librados:"


namespace librados_stub {

LRemClusterRef &cluster() {
  static LRemClusterRef s_cluster;
  return s_cluster;
}

void set_cluster(LRemClusterRef cluster_ref) {
  cluster() = cluster_ref;
}

LRemClusterRef get_cluster() {
  auto &cluster_ref = cluster();
  if (cluster_ref.get() == nullptr) {
    cluster_ref.reset(new librados::LRemDBCluster(g_ceph_context));
  }
  return cluster_ref;
}

librados::LRemClassHandler *get_class_handler() {
  static boost::shared_ptr<librados::LRemClassHandler> s_class_handler;
  if (!s_class_handler) {
    s_class_handler.reset(new librados::LRemClassHandler());
    s_class_handler->open_all_classes();
  }
  return s_class_handler.get();
}

} // namespace librados_stub

namespace {

void do_out_buffer(bufferlist& outbl, char **outbuf, size_t *outbuflen) {
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen) {
    *outbuflen = outbl.length();
  }
}

void do_out_buffer(string& outbl, char **outbuf, size_t *outbuflen) {
  if (outbuf) {
    if (outbl.length() > 0) {
      *outbuf = (char *)malloc(outbl.length());
      memcpy(*outbuf, outbl.c_str(), outbl.length());
    } else {
      *outbuf = NULL;
    }
  }
  if (outbuflen) {
    *outbuflen = outbl.length();
  }
}

librados::LRemRadosClient *create_rados_client() {
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf.parse_env(cct->get_module_type());
  cct->_conf.apply_changes(nullptr);
  cct->_log->start();

  auto rados_client =
    librados_stub::get_cluster()->create_rados_client(cct);
  cct->put();
  return rados_client;
}

} // anonymous namespace

#if defined(HAVE_ASM_SYMVER) || defined(HAVE_ATTR_SYMVER)
// prefer __attribute__() over global asm(".symver"). because the latter
// is not parsed by the compiler and is partitioned away by GCC if
// lto-partitions is enabled, in other words, these asm() statements
// are dropped by the -flto option by default. the way to address it is
// to use __attribute__. so this information can be processed by the
// C compiler, and be preserved after LTO partitions the code
#ifdef HAVE_ATTR_SYMVER
#define LIBRADOS_C_API_BASE(fn)               \
  extern __typeof (_##fn##_base) _##fn##_base __attribute__((__symver__ (#fn "@")))
#define LIBRADOS_C_API_BASE_DEFAULT(fn)       \
  extern __typeof (_##fn) _##fn __attribute__((__symver__ (#fn "@@")))
#define LIBRADOS_C_API_DEFAULT(fn, ver)       \
  extern __typeof (_##fn) _##fn __attribute__((__symver__ (#fn "@@LIBRADOS_" #ver)))
#else
#define LIBRADOS_C_API_BASE(fn)               \
  asm(".symver _" #fn "_base, " #fn "@")
#define LIBRADOS_C_API_BASE_DEFAULT(fn)       \
  asm(".symver _" #fn ", " #fn "@@")
#define LIBRADOS_C_API_DEFAULT(fn, ver)       \
  asm(".symver _" #fn ", " #fn "@@LIBRADOS_" #ver)
#endif

#define LIBRADOS_C_API_BASE_F(fn) _ ## fn ## _base
#define LIBRADOS_C_API_DEFAULT_F(fn) _ ## fn

#else
#define LIBRADOS_C_API_BASE(fn)
#define LIBRADOS_C_API_BASE_DEFAULT(fn)
#define LIBRADOS_C_API_DEFAULT(fn, ver)

#define LIBRADOS_C_API_BASE_F(fn) _ ## fn ## _base
// There shouldn't be multiple default versions of the same
// function.
#define LIBRADOS_C_API_DEFAULT_F(fn) fn
#endif

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_create_completion2)(void *cb_arg,
					    rados_callback_t cb_complete,
					    rados_completion_t *pc)
{
  librados::AioCompletionImpl *c = new librados::AioCompletionImpl;
  if (cb_complete) {
    c->set_complete_callback(cb_arg, cb_complete);
  }
  *pc = c;
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_create_completion2);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_aio_get_return_value)(rados_completion_t c) {
  return reinterpret_cast<librados::AioCompletionImpl*>(c)->get_return_value();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_aio_get_return_value);

extern "C" rados_config_t LIBRADOS_C_API_DEFAULT_F(rados_cct)(rados_t cluster)
{
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  return reinterpret_cast<rados_config_t>(client->cct());
}
LIBRADOS_C_API_BASE_DEFAULT(rados_cct);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_set)(rados_t cluster, const char *option,
                              const char *value) {
  librados::LRemRadosClient *impl =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  CephContext *cct = impl->cct();
  return cct->_conf.set_val(option, value);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_set);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_parse_env)(rados_t cluster, const char *var) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  auto& conf = client->cct()->_conf;
  conf.parse_env(client->cct()->get_module_type(), var);
  conf.apply_changes(NULL);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_parse_env);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_conf_read_file)(rados_t cluster, const char *path) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  auto& conf = client->cct()->_conf;
  int ret = conf.parse_config_files(path, NULL, 0);
  if (ret == 0) {
    conf.parse_env(client->cct()->get_module_type());
    conf.apply_changes(NULL);
    conf.complain_about_parse_error(client->cct());
  } else if (ret == -ENOENT) {
    // ignore missing client config
    return 0;
  }
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_conf_read_file);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_connect)(rados_t cluster) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  return client->connect();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_connect);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_create)(rados_t *cluster, const char * const id) {
  *cluster = create_rados_client();
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_create_with_context)(rados_t *cluster,
                                         rados_config_t cct_) {
  auto cct = reinterpret_cast<CephContext*>(cct_);
  *cluster = librados_stub::get_cluster()->create_rados_client(cct);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_create_with_context);

extern "C" rados_config_t LIBRADOS_C_API_DEFAULT_F(rados_ioctx_cct)(rados_ioctx_t ioctx)
{
  librados::LRemIoCtxImpl *ctx =
    reinterpret_cast<librados::LRemIoCtxImpl*>(ioctx);
  return reinterpret_cast<rados_config_t>(ctx->get_rados_client()->cct());
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_cct);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_create)(rados_t cluster, const char *pool_name,
                                  rados_ioctx_t *ioctx) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);

  int64_t pool_id = client->pool_lookup(pool_name);
  if (pool_id < 0) {
    return static_cast<int>(pool_id);
  }

  *ioctx = reinterpret_cast<rados_ioctx_t>(
      client->create_ioctx(pool_id, pool_name));
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_ioctx_create2)(rados_t cluster, int64_t pool_id,
                                   rados_ioctx_t *ioctx)
{
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);

  std::list<std::pair<int64_t, std::string> > pools;
  int r = client->pool_list(pools);
  if (r < 0) {
    return r;
  }

  for (std::list<std::pair<int64_t, std::string> >::iterator it =
       pools.begin(); it != pools.end(); ++it) {
    if (it->first == pool_id) {
      *ioctx = reinterpret_cast<rados_ioctx_t>(
	client->create_ioctx(pool_id, it->second));
      return 0;
    }
  }
  return -ENOENT;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_create2);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_ioctx_destroy)(rados_ioctx_t io) {
  librados::LRemIoCtxImpl *ctx =
    reinterpret_cast<librados::LRemIoCtxImpl*>(io);
  ctx->put();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_destroy);

extern "C" rados_t LIBRADOS_C_API_DEFAULT_F(rados_ioctx_get_cluster)(rados_ioctx_t io) {
  librados::LRemIoCtxImpl *ctx =
    reinterpret_cast<librados::LRemIoCtxImpl*>(io);
  return reinterpret_cast<rados_t>(ctx->get_rados_client());
}
LIBRADOS_C_API_BASE_DEFAULT(rados_ioctx_get_cluster);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_mon_command)(rados_t cluster, const char **cmd,
                                 size_t cmdlen, const char *inbuf,
                                 size_t inbuflen, char **outbuf,
                                 size_t *outbuflen, char **outs,
                                 size_t *outslen) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);

  vector<string> cmdvec;
  for (size_t i = 0; i < cmdlen; i++) {
    cmdvec.push_back(cmd[i]);
  }

  bufferlist inbl;
  inbl.append(inbuf, inbuflen);

  bufferlist outbl;
  string outstring;
  int ret = client->mon_command(cmdvec, inbl, &outbl, &outstring);

  do_out_buffer(outbl, outbuf, outbuflen);
  do_out_buffer(outstring, outs, outslen);
  return ret;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_mon_command);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_open)(rados_ioctx_t io,
                                        rados_list_ctx_t *ctx) {
  librados::LRemIoCtxImpl *io_ctx =
    reinterpret_cast<librados::LRemIoCtxImpl*>(io);
  librados::LRemRadosClient *client = io_ctx->get_rados_client();

  librados::ObjListCtx *list_ctx = new librados::ObjListCtx();

  int r = client->object_list_open(io_ctx->get_id(), &list_ctx->op);
  if (r < 0) {
    return r;
  }
  auto ns = io_ctx->get_namespace();
  if (ns != librados::all_nspaces) {
    list_ctx->op->set_nspace(io_ctx->get_namespace());
  }
  list_ctx->objs.push_front(librados::LRemRadosClient::Object());
  *ctx = reinterpret_cast<rados_list_ctx_t>(list_ctx);
  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_open);

#define RADOS_LIST_MAX_ENTRIES 256

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_next)(rados_list_ctx_t ctx,
                                        const char **entry,
                                        const char **key,
                                        const char **nspace) {
  return rados_nobjects_list_next2(ctx, entry, key, nspace, nullptr, nullptr, nullptr);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_next);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_next2)(
  rados_list_ctx_t ctx,
  const char **entry,
  const char **key,
  const char **nspace,
  size_t *entry_size,
  size_t *key_size,
  size_t *nspace_size)
{
  librados::ObjListCtx *list_ctx = reinterpret_cast<librados::ObjListCtx *>(ctx);
  auto& objs = list_ctx->objs;

  // if the list is non-empty, this method has been called before
  if (!objs.empty())
    // so let's kill the previously-returned object
    objs.pop_front();

  if (objs.empty()) {
    if (!list_ctx->more) {
      list_ctx->at_end = true;
      return -ENOENT;
    }
    int ret = list_ctx->op->list_objs(RADOS_LIST_MAX_ENTRIES, &objs, &list_ctx->more);
    if (ret < 0) {
      return ret;
    }
    list_ctx->at_end = objs.empty();
    if (list_ctx->at_end) {
      return -ENOENT;
    }
  }

  *entry = objs.front().oid.c_str();

  if (key) {
    if (objs.front().locator.size())
      *key = objs.front().locator.c_str();
    else
      *key = NULL;
  }
  if (nspace)
    *nspace = objs.front().nspace.c_str();
  if (entry_size)
    *entry_size = objs.front().oid.size();
  if (key_size)
    *key_size = objs.front().locator.size();
  if (nspace_size)
    *nspace_size = objs.front().nspace.size();

  return 0;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_next2);


extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_close)(rados_list_ctx_t ctx) {
  librados::ObjListCtx *list_ctx = reinterpret_cast<librados::ObjListCtx *>(ctx);
  delete list_ctx;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_close);

extern "C" uint32_t LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_seek)(
  rados_list_ctx_t listctx,
  uint32_t pos)
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;
  uint32_t r = lh->seek(pos);
  return r;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_seek);

extern "C" uint32_t LIBRADOS_C_API_DEFAULT_F(rados_nobjects_list_seek_cursor)(
  rados_list_ctx_t listctx,
  rados_object_list_cursor cursor)
{
  librados::ObjListCtx *lh = (librados::ObjListCtx *)listctx;

  librados::ObjectCursor c(cursor);

  uint32_t r = lh->seek(c);
  return r;
}
LIBRADOS_C_API_BASE_DEFAULT(rados_nobjects_list_seek_cursor);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_create)(rados_t cluster, const char *pool_name) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  return client->pool_create(pool_name);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_create);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_pool_delete)(rados_t cluster, const char *pool_name) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  return client->pool_delete(pool_name);
}
LIBRADOS_C_API_BASE_DEFAULT(rados_pool_delete);

extern "C" void LIBRADOS_C_API_DEFAULT_F(rados_shutdown)(rados_t cluster) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  client->put();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_shutdown);

extern "C" int LIBRADOS_C_API_DEFAULT_F(rados_wait_for_latest_osdmap)(rados_t cluster) {
  librados::LRemRadosClient *client =
    reinterpret_cast<librados::LRemRadosClient*>(cluster);
  return client->wait_for_latest_osdmap();
}
LIBRADOS_C_API_BASE_DEFAULT(rados_wait_for_latest_osdmap);

using namespace std::placeholders;

namespace librados {

librados::PoolAsyncCompletion::PoolAsyncCompletion::~PoolAsyncCompletion()
{
  auto c = reinterpret_cast<PoolAsyncCompletionImpl *>(pc);
  c->release();
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::set_callback(void *cb_arg,
                                                                     rados_callback_t cb)
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->set_callback(cb_arg, cb);
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::wait()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->wait();
}

bool librados::PoolAsyncCompletion::PoolAsyncCompletion::is_complete()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->is_complete();
}

int librados::PoolAsyncCompletion::PoolAsyncCompletion::get_return_value()
{
  PoolAsyncCompletionImpl *c = (PoolAsyncCompletionImpl *)pc;
  return c->get_return_value();
}

void librados::PoolAsyncCompletion::PoolAsyncCompletion::release()
{
  delete this;
}

AioCompletion::~AioCompletion()
{
  auto c = reinterpret_cast<AioCompletionImpl *>(pc);
  c->release();
}

void AioCompletion::release() {
  delete this;
}

int AioCompletion::AioCompletion::get_return_value()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->get_return_value();
}

int AioCompletion::AioCompletion::wait_for_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete();
}

int AioCompletion::AioCompletion::wait_for_complete_and_cb()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->wait_for_complete_and_cb();
}

bool librados::AioCompletion::AioCompletion::is_complete()
{
  AioCompletionImpl *c = (AioCompletionImpl *)pc;
  return c->is_complete();
}


IoCtx::IoCtx() : io_ctx_impl(NULL) {
}

IoCtx::~IoCtx() {
  close();
}

IoCtx::IoCtx(const IoCtx& rhs) {
  io_ctx_impl = rhs.io_ctx_impl;
  if (io_ctx_impl) {
    LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
    ctx->get();
  }
}

IoCtx::IoCtx(IoCtx&& rhs) noexcept : io_ctx_impl(std::exchange(rhs.io_ctx_impl, nullptr))
{
}

IoCtx& IoCtx::operator=(const IoCtx& rhs) {
  if (io_ctx_impl) {
    LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
    ctx->put();
  }

  io_ctx_impl = rhs.io_ctx_impl;
  if (io_ctx_impl) {
    LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
    ctx->get();
  }
  return *this;
}

librados::IoCtx& librados::IoCtx::operator=(IoCtx&& rhs) noexcept
{
  if (io_ctx_impl) {
    LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
    ctx->put();
  }

  io_ctx_impl = std::exchange(rhs.io_ctx_impl, nullptr);
  return *this;
}

int IoCtx::aio_flush() {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_flush() ctx=" << ctx << dendl;
  ctx->aio_flush();
  return 0;
}

int IoCtx::aio_flush_async(AioCompletion *c) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_flush_async() ctx=" << ctx << dendl;
  ctx->aio_flush_async(c->pc);
  return 0;
}

int IoCtx::aio_notify(const std::string& oid, AioCompletion *c, bufferlist& bl,
                      uint64_t timeout_ms, bufferlist *pbl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_notify() ctx=" << ctx << " oid=" << oid << " c=" << c << " timeout_ms=" << timeout_ms << dendl;
  ctx->aio_notify(oid, c->pc, bl, timeout_ms, pbl);
  return 0;
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectReadOperation *op, bufferlist *pbl) {
  return aio_operate(oid, c, op, 0, pbl);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectReadOperation *op, int flags,
                       bufferlist *pbl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_operate() (read) ctx=" << ctx << " oid=" << oid << " c=" << c << " flags=" << flags << dendl;
  LRemObjectOperationImpl *ops = reinterpret_cast<LRemObjectOperationImpl*>(op->impl);
  return ctx->aio_operate_read(oid, *ops, c->pc, flags, pbl,
                               ctx->get_snap_read(), nullptr);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectReadOperation *op, int flags,
                       bufferlist *pbl, const blkin_trace_info *trace_info) {
  return aio_operate(oid, c, op, flags, pbl);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op, int flags) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_operate() (write) ctx=" << ctx << " oid=" << oid << " c=" << c << " flags=" << flags << dendl;
  LRemObjectOperationImpl *ops = reinterpret_cast<LRemObjectOperationImpl*>(op->impl);
  return ctx->aio_operate(oid, *ops, c->pc, NULL, flags);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_operate() (write) ctx=" << ctx << " oid=" << oid << " c=" << c << dendl;
  LRemObjectOperationImpl *ops = reinterpret_cast<LRemObjectOperationImpl*>(op->impl);
  return ctx->aio_operate(oid, *ops, c->pc, NULL, 0);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op, snap_t seq,
                       std::vector<snap_t>& snaps, int flags,
                       const blkin_trace_info *trace_info) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_operate() (write) ctx=" << ctx << " oid=" << oid << " c=" << c << dendl;
  LRemObjectOperationImpl *ops = reinterpret_cast<LRemObjectOperationImpl*>(op->impl);

  std::vector<snapid_t> snv;
  snv.resize(snaps.size());
  for (size_t i = 0; i < snaps.size(); ++i)
    snv[i] = snaps[i];
  SnapContext snapc(seq, snv);

  return ctx->aio_operate(oid, *ops, c->pc, &snapc, flags);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op, snap_t seq,
                       std::vector<snap_t>& snaps) {
  return aio_operate(oid, c, op, seq, snaps, 0, nullptr);
}

int IoCtx::aio_operate(const std::string& oid, AioCompletion *c,
                       ObjectWriteOperation *op, snap_t seq,
                       std::vector<snap_t>& snaps,
		       const blkin_trace_info *trace_info) {
  return aio_operate(oid, c, op, seq, snaps, 0, trace_info);
}

int IoCtx::aio_append(const std::string& oid, librados::AioCompletion *c,
                      const bufferlist& bl, size_t len)
{
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_append() ctx=" << ctx << " oid=" << oid << " c=" << c << dendl;
  return ctx->aio_append(oid, c->pc, bl, len);
}

int IoCtx::aio_remove(const std::string& oid, AioCompletion *c) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_remove() ctx=" << ctx << " oid=" << oid << " c=" << c << dendl;
  return ctx->aio_remove(oid, c->pc);
}

int IoCtx::aio_remove(const std::string& oid, AioCompletion *c, int flags) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_remove() ctx=" << ctx << " oid=" << oid << " c=" << c << " flags=" << flags << dendl;
  return ctx->aio_remove(oid, c->pc, flags);
}

int IoCtx::aio_watch(const std::string& o, AioCompletion *c, uint64_t *handle,
                     librados::WatchCtx2 *watch_ctx) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_watch() ctx=" << ctx << " oid=" << o << " c=" << c << dendl;
  return ctx->aio_watch(o, c->pc, handle, watch_ctx);
}

int IoCtx::aio_unwatch(uint64_t handle, AioCompletion *c) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_unwatch() ctx=" << ctx << " c=" << c << " handle=" << handle << dendl;
  return ctx->aio_unwatch(handle, c->pc);
}

int IoCtx::aio_read(const std::string& oid, librados::AioCompletion *c,
                              bufferlist *pbl, size_t len, uint64_t off) {
  ObjectReadOperation op;
  op.read(off, len, pbl, nullptr);
  return aio_operate(oid, c, &op, nullptr);
}

int IoCtx::aio_write(const std::string& oid, librados::AioCompletion *c,
                     const bufferlist& bl, size_t len, uint64_t off) {
  bufferlist newbl(bl);
  bufferlist wrbl;
  newbl.splice(0, len, &wrbl);
  ObjectWriteOperation op;
  op.write(off, wrbl);
  return aio_operate(oid, c, &op);
  
}


int IoCtx::aio_exec(const std::string& oid, AioCompletion *c,
                    const char *cls, const char *method,
                    bufferlist& inbl, bufferlist *outbl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_exec() ctx=" << ctx << " oid=" << oid << " c=" << c << " cls=" << cls << " method=" << method << dendl;
  return ctx->aio_exec(oid, c->pc, librados_stub::get_class_handler(), 
                       cls, method, inbl, outbl);
}

config_t IoCtx::cct() {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  return reinterpret_cast<config_t>(ctx->get_rados_client()->cct());
}

void IoCtx::close() {
  if (io_ctx_impl) {
    LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
    dout(20) << "IoCtx::aio_exec() ctx=" << ctx << dendl;
    ctx->put();
  }
  io_ctx_impl = NULL;
}

int IoCtx::create(const std::string& oid, bool exclusive) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::create() ctx=" << ctx << " oid=" << oid << " exclusive=" << exclusive << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::create, _1, _2, exclusive,
                     ctx->get_snap_context()));
}

void IoCtx::dup(const IoCtx& rhs) {
  close();
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(rhs.io_ctx_impl);
  dout(20) << "IoCtx::dup() ctx=" << ctx << dendl;
  io_ctx_impl = reinterpret_cast<IoCtxImpl*>(ctx->clone());
}

int IoCtx::exec(const std::string& oid, const char *cls, const char *method,
                bufferlist& inbl, bufferlist& outbl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::exec() ctx=" << ctx << " oid=" << oid << " cls=" << cls << " method=" << method << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::exec, _1, _2,
                     librados_stub::get_class_handler(), cls,
                     method, inbl, &outbl, ctx->get_snap_read(),
                     ctx->get_snap_context()));
}

void IoCtx::from_rados_ioctx_t(rados_ioctx_t p, IoCtx &io) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(p);
  ctx->get();

  io.close();
  io.io_ctx_impl = reinterpret_cast<IoCtxImpl*>(ctx);
}

uint64_t IoCtx::get_instance_id() const {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  return ctx->get_instance_id();
}

int64_t IoCtx::get_id() {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  return ctx->get_id();
}

uint32_t IoCtx::get_object_hash_position(const std::string& oid) {
  return (uint32_t)-1;
}

uint32_t IoCtx::get_object_pg_hash_position(const std::string& oid) {
  return (uint32_t)-1;
}

int IoCtx::get_object_hash_position2(
    const std::string& oid, uint32_t *hash_position)
{
  return -1;
}

int IoCtx::get_object_pg_hash_position2(
    const std::string& oid, uint32_t *pg_hash_position)
{
  return -1;
}

uint64_t IoCtx::get_last_version() {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  return ctx->get_last_version();
}

std::string IoCtx::get_pool_name() {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  return ctx->get_pool_name();
}
bool IoCtx::pool_requires_alignment() {
  return false;
}
int IoCtx::pool_requires_alignment2(bool * req) {
  *req = pool_requires_alignment();
  return 0;
}
uint64_t IoCtx::pool_required_alignment() {
  return 0;
}
int IoCtx::pool_required_alignment2(uint64_t * alignment) {
  *alignment = pool_required_alignment();
  return 0;
}

int IoCtx::list_snaps(const std::string& o, snap_set_t *out_snaps) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::list_snaps() ctx=" << ctx << " oid=" << o << dendl;
  return ctx->execute_operation(
    o, std::bind(&LRemIoCtxImpl::list_snaps, _1, _2, out_snaps));
}

int IoCtx::set_alloc_hint(const std::string& o,
                          uint64_t expected_object_size,
                          uint64_t expected_write_size) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::set_alloc_hint() ctx=" << ctx << " oid=" << o
    << " expected_object_size=" << expected_object_size
    << " expected_write_size=" << expected_write_size << dendl;

  return ctx->execute_operation(
    o, std::bind(&LRemIoCtxImpl::set_alloc_hint, _1, _2, expected_object_size, expected_write_size, 0,
                     ctx->get_snap_context()));
}

int librados::IoCtx::set_alloc_hint2(const std::string& o,
                                     uint64_t expected_object_size,
                                     uint64_t expected_write_size,
                                     uint32_t flags) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::set_alloc_hint2() ctx=" << ctx << " oid=" << o
    << " expected_object_size=" << expected_object_size
    << " expected_write_size=" << expected_write_size
    << " flags=" << flags << dendl;

  return ctx->execute_operation(
    o, std::bind(&LRemIoCtxImpl::set_alloc_hint, _1, _2, expected_object_size, expected_write_size, flags,
                     ctx->get_snap_context()));
}


int IoCtx::list_watchers(const std::string& o,
                         std::list<obj_watch_t> *out_watchers) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::list_watchers() ctx=" << ctx << " oid=" << o << dendl;
  return ctx->execute_operation(
    o, std::bind(&LRemIoCtxImpl::list_watchers, _1, _2, out_watchers));
}

int IoCtx::notify(const std::string& o, uint64_t ver, bufferlist& bl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::notify() ctx=" << ctx << " oid=" << o << " ver=" << ver << dendl;
  auto trans = ctx->init_transaction(o);
  return ctx->notify(trans, bl, 0, NULL);
}

int IoCtx::notify2(const std::string& o, bufferlist& bl,
                   uint64_t timeout_ms, bufferlist *pbl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::notify2() ctx=" << ctx << " oid=" << o << " timeout=" << timeout_ms << dendl;
  auto trans = ctx->init_transaction(o);
  return ctx->notify(trans, bl, timeout_ms, pbl);
}

void IoCtx::notify_ack(const std::string& o, uint64_t notify_id,
                       uint64_t handle, bufferlist& bl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::notify_ack() ctx=" << ctx << " oid=" << o << " notify_id=" << notify_id << " handle=" << handle << dendl;
  auto trans = ctx->init_transaction(o);
  ctx->notify_ack(trans, notify_id, handle, bl);
}

int IoCtx::omap_get_keys(const std::string& oid,
                         const std::string& start_after,
                         uint64_t max_return,
                         std::set<std::string> *out_keys) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_get_keys() ctx=" << ctx << " oid=" << oid
           << " start_after=" << start_after << " max_return=" << max_return << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_get_keys2, _1, _2, start_after,
                     max_return, out_keys, nullptr));
}

int IoCtx::omap_get_keys2(const std::string& oid,
                          const std::string& start_after,
                          uint64_t max_return,
                          std::set<std::string> *out_keys,
                          bool *pmore) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_get_keys() ctx=" << ctx << " oid=" << oid
           << " start_after=" << start_after << " max_return=" << max_return << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_get_keys2, _1, _2, start_after,
                     max_return, out_keys, pmore));
}


int IoCtx::omap_get_vals(const std::string& oid,
                         const std::string& start_after,
                         uint64_t max_return,
                         std::map<std::string, bufferlist> *out_vals) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_get_vals() ctx=" << ctx << " oid=" << oid
           << " start_after=" << start_after << " max_return=" << max_return << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_get_vals, _1, _2, start_after, "",
                     max_return, out_vals));
}

int IoCtx::omap_get_vals_by_keys(const std::string& oid,
                                 const std::set<std::string>& keys,
                                 std::map<std::string, bufferlist> *vals) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_get_vals_by_keys() ctx=" << ctx << " oid=" << oid << " keys=" << keys << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_get_vals_by_keys, _1, _2, keys, vals));
}

int IoCtx::omap_get_header(const std::string& oid,
                        bufferlist *bl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_get_header() ctx=" << ctx << " oid=" << oid << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_get_header, _1, _2, bl));
}

int librados::IoCtx::omap_set_header(const std::string& oid,
                                     const bufferlist& bl)
{
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_set_header() ctx=" << ctx << " oid=" << oid << " bl.length=" << bl.length() << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_set_header, _1, _2, bl));
}

int IoCtx::omap_set(const std::string& oid,
                    const std::map<std::string, bufferlist>& m) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_set() ctx=" << ctx << " oid=" << oid << " m.size()=" << m.size() << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_set, _1, _2, m));
}

int IoCtx::omap_rm_keys(const std::string& oid,
                        const std::set<std::string>& keys) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_rm_keys() ctx=" << ctx << " oid=" << oid << " keys=" << keys << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_rm_keys, _1, _2, keys));
}

int IoCtx::omap_clear(const std::string& oid) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::omap_clear() ctx=" << ctx << " oid=" << oid << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::omap_clear, _1, _2));
}

int IoCtx::operate(const std::string& oid, ObjectWriteOperation *op) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::operate() (write) ctx=" << ctx << " oid=" << oid << dendl;
  LRemObjectOperationImpl *ops = reinterpret_cast<LRemObjectOperationImpl*>(op->impl);
  return ctx->operate(oid, *ops, 0);
}

int IoCtx::operate(const std::string& oid, ObjectWriteOperation *op, int flags) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::operate() (write) ctx=" << ctx << " oid=" << oid << " flags=" << flags << dendl;
  LRemObjectOperationImpl *ops = reinterpret_cast<LRemObjectOperationImpl*>(op->impl);
  return ctx->operate(oid, *ops, flags);
}

int IoCtx::operate(const std::string& oid, ObjectReadOperation *op,
                   bufferlist *pbl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::operate() (read) ctx=" << ctx << " oid=" << oid << dendl;
  LRemObjectOperationImpl *ops = reinterpret_cast<LRemObjectOperationImpl*>(op->impl);
  return ctx->operate_read(oid, *ops, pbl, 0);
}
int IoCtx::operate(const std::string& oid, ObjectReadOperation *op,
                   bufferlist *pbl, int flags) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::operate() (read) ctx=" << ctx << " oid=" << oid << " flags=" << flags << dendl;
  LRemObjectOperationImpl *ops = reinterpret_cast<LRemObjectOperationImpl*>(op->impl);
  return ctx->operate_read(oid, *ops, pbl, flags);
}

int IoCtx::read(const std::string& oid, bufferlist& bl, size_t len,
                uint64_t off) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::read() ctx=" << ctx << " oid=" << oid << " len=" << len << " off=" << off << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::read, _1, _2, len, off, &bl,
                     ctx->get_snap_read(), nullptr));
}

int IoCtx::remove(const std::string& oid) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::remove() ctx=" << ctx << " oid=" << oid << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::remove, _1, _2, ctx->get_snap_context()));
}

int IoCtx::remove(const std::string& oid, int flags) {
#warning flags not used
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::remove() ctx=" << ctx << " oid=" << oid << " flags=" << flags << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::remove, _1, _2, ctx->get_snap_context()));
}

int IoCtx::selfmanaged_snap_create(uint64_t *snapid) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::selfmanaged_snap_create() ctx=" << ctx << dendl;
  return ctx->selfmanaged_snap_create(snapid);
}

void IoCtx::aio_selfmanaged_snap_create(uint64_t *snapid, AioCompletion* c) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_selfmanaged_snap_create() ctx=" << ctx << " c=" << c << dendl;
  return ctx->aio_selfmanaged_snap_create(snapid, c->pc);
}

int IoCtx::selfmanaged_snap_remove(uint64_t snapid) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::selfmanaged_snap_create() ctx=" << ctx << " snapid=" << snapid << dendl;
  return ctx->selfmanaged_snap_remove(snapid);
}

void IoCtx::aio_selfmanaged_snap_remove(uint64_t snapid, AioCompletion* c) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::aio_selfmanaged_snap_create() ctx=" << ctx << " snapid=" << snapid << " c=" << c << dendl;
  ctx->aio_selfmanaged_snap_remove(snapid, c->pc);
}

int IoCtx::selfmanaged_snap_rollback(const std::string& oid,
                                     uint64_t snapid) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::selfmanaged_snap_rollback() ctx=" << ctx << " oid=" << oid << " snapid=" << snapid << dendl;
  auto trans = ctx->init_transaction(oid);
  return ctx->selfmanaged_snap_rollback(trans, snapid);
}

int IoCtx::selfmanaged_snap_set_write_ctx(snap_t seq,
                                          std::vector<snap_t>& snaps) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::selfmanaged_snap_set_write_ctx() ctx=" << ctx << " seq=" << seq << dendl;
  return ctx->selfmanaged_snap_set_write_ctx(seq, snaps);
}

int IoCtx::snap_create(const char *snapname) {
#warning implement me
  return -ENOTSUP;
}

int IoCtx::snap_lookup(const char *name, snap_t *snapid) {
#warning not implemented
  return -ENOTSUP;
}


int IoCtx::snap_get_stamp(snap_t snapid, time_t *t) {
#warning implement me
  return -ENOTSUP;
}


int IoCtx::snap_get_name(snap_t snapid, std::string *s) {
#warning not implemented
  return -ENOTSUP;
}

int IoCtx::snap_remove(const char *snapname) {
#warning implement me
  return -ENOTSUP;
}

int IoCtx::snap_list(std::vector<snap_t> *snaps) {
#warning implement me
  return -ENOTSUP;
}

int IoCtx::snap_rollback(const std::string& oid, const char *snapname) {
#warning implement me
  return -ENOTSUP;
}

void IoCtx::snap_set_read(snap_t seq) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::selfmanaged_snap_set_read() ctx=" << ctx << " seq=" << seq << dendl;
  ctx->set_snap_read(seq);
}

int IoCtx::sparse_read(const std::string& oid, std::map<uint64_t,uint64_t>& m,
                       bufferlist& bl, size_t len, uint64_t off) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::sparse_read() ctx=" << ctx << " oid=" << oid << " len=" << len << " off=" << off << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::sparse_read, _1, _2, off, len, &m, &bl,
                     ctx->get_snap_read()));
}

int IoCtx::getxattr(const std::string& oid, const char *name, bufferlist& bl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::getxattr() ctx=" << ctx << " oid=" << oid << " name=" << name << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::getxattr, _1, _2, name, &bl));
}

int IoCtx::getxattrs(const std::string& oid, std::map<std::string, bufferlist>& attrset) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::getxattrs() ctx=" << ctx << " oid=" << oid << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::xattr_get, _1, _2, &attrset));
}

int IoCtx::setxattr(const std::string& oid, const char *name, bufferlist& bl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::setxattrs() ctx=" << ctx << " oid=" << oid << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::setxattr, _1, _2, name, bl));
}

int IoCtx::rmxattr(const std::string& oid, const char *name) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::rmxattrs() ctx=" << ctx << " oid=" << oid << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::rmxattr, _1, _2, name));
}

int IoCtx::stat(const std::string& oid, uint64_t *psize, time_t *pmtime) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::stat() ctx=" << ctx << " oid=" << oid << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::stat, _1, _2, psize, pmtime));
}

int IoCtx::stat2(const std::string& oid, uint64_t *psize, struct timespec *pts) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::stat2() ctx=" << ctx << " oid=" << oid << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::stat2, _1, _2, psize, pts));
}

int IoCtx::tmap_update(const std::string& oid, bufferlist& cmdbl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::tmap_update, _1, _2, cmdbl));
}

int IoCtx::trunc(const std::string& oid, uint64_t off) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::trunc() ctx=" << ctx << " oid=" << oid << " off=" << off << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::truncate, _1, _2, off,
                     ctx->get_snap_context()));
}

int IoCtx::mapext(const std::string& oid, uint64_t off, size_t len,
                            std::map<uint64_t,uint64_t>& m) {
  return -ENOTSUP;
}

int IoCtx::unwatch2(uint64_t handle) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::unwatch2() ctx=" << ctx << " handle=" << handle << dendl;
  return ctx->unwatch(handle);
}

int IoCtx::unwatch(const std::string& o, uint64_t handle) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::unwatch() ctx=" << ctx << " oid=" << o << " handle=" << handle << dendl;
  return ctx->unwatch(handle);
}

int IoCtx::watch(const std::string& o, uint64_t ver, uint64_t *handle,
                 librados::WatchCtx *wctx) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::watch() ctx=" << ctx << " oid=" << o << " ver=" << ver << dendl;
  auto trans = ctx->init_transaction(o);
  return ctx->watch(trans, handle, wctx, NULL);
}

int IoCtx::watch2(const std::string& o, uint64_t *handle,
                  librados::WatchCtx2 *wctx) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::watch2() ctx=" << ctx << " oid=" << o << dendl;
  auto trans = ctx->init_transaction(o);
  return ctx->watch(trans, handle, NULL, wctx);
}

int IoCtx::write(const std::string& oid, bufferlist& bl, size_t len,
                 uint64_t off) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::write() ctx=" << ctx << " oid=" << oid << " len=" << len << " off=" << off << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::write, _1, _2, bl, len, off,
                     ctx->get_snap_context()));
}

int librados::IoCtx::append(const std::string& oid, bufferlist& bl, size_t len) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::append() ctx=" << ctx << " oid=" << oid << " bl.length()=" << bl.length() << " len=" << len << dendl;
  bufferlist newbl(bl);
  bufferlist wrbl;
  newbl.splice(0, len, &wrbl);
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::append, _1, _2, wrbl,
                     ctx->get_snap_context()));
}


int IoCtx::write_full(const std::string& oid, bufferlist& bl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::write_full() ctx=" << ctx << " oid=" << oid << " bl.length()=" << bl.length() << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::write_full, _1, _2, bl,
                     ctx->get_snap_context()));
}

int IoCtx::writesame(const std::string& oid, bufferlist& bl, size_t len,
                     uint64_t off) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::write_full() ctx=" << ctx << " oid=" << oid << " bl.length()=" << bl.length() << " len=" << len << " off=" << off << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::writesame, _1, _2, bl, len, off,
                     ctx->get_snap_context()));
}

int IoCtx::cmpext(const std::string& oid, uint64_t off, bufferlist& cmp_bl) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::cmpext() ctx=" << ctx << " oid=" << oid << " off" << off << " cmp_bl.length()=" << cmp_bl.length() << dendl;
  return ctx->execute_operation(
    oid, std::bind(&LRemIoCtxImpl::cmpext, _1, _2, off, cmp_bl,
                     ctx->get_snap_read()));
}

int IoCtx::application_enable(const std::string& app_name, bool force) {
  return 0;
}

int IoCtx::application_enable_async(const std::string& app_name,
                                    bool force, PoolAsyncCompletion *c) {
  return -EOPNOTSUPP;
}

int IoCtx::application_list(std::set<std::string> *app_names) {
  return -EOPNOTSUPP;
}

int IoCtx::application_metadata_get(const std::string& app_name,
                                    const std::string &key,
                                    std::string *value) {
  return -EOPNOTSUPP;
}

int IoCtx::application_metadata_set(const std::string& app_name,
                                    const std::string &key,
                                    const std::string& value) {
  return -EOPNOTSUPP;
}

int IoCtx::application_metadata_remove(const std::string& app_name,
                                       const std::string &key) {
  return -EOPNOTSUPP;
}

int IoCtx::application_metadata_list(const std::string& app_name,
                                     std::map<std::string, std::string> *values) {
  return -EOPNOTSUPP;
}

void IoCtx::locator_set_key(const std::string& key) {
#warning locator missing implementation
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::locator_set_key() ctx=" << ctx << " key=" << key << dendl;
  ctx->locator_set_key(key);
}

void IoCtx::set_namespace(const std::string& nspace) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  dout(20) << "IoCtx::set_namespace() ctx=" << ctx << " nspace=" << nspace << dendl;
  ctx->set_namespace(nspace);
}

std::string IoCtx::get_namespace() const {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(io_ctx_impl);
  return ctx->get_namespace();
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
    const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.get_next();
  return iter;
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
  uint32_t pos, const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.seek(pos);
  return iter;
}

librados::NObjectIterator librados::IoCtx::nobjects_begin(
  const ObjectCursor& cursor, const bufferlist &filter)
{
  rados_list_ctx_t listh;
  rados_nobjects_list_open(io_ctx_impl, &listh);
  NObjectIterator iter((ObjListCtx*)listh);
  if (filter.length() > 0) {
    iter.set_filter(filter);
  }
  iter.seek(cursor);
  return iter;
}

const librados::NObjectIterator& librados::IoCtx::nobjects_end() const
{
  return NObjectIterator::__EndObjectIterator;
}

void IoCtx::set_osdmap_full_try() {
}

void IoCtx::unset_osdmap_full_try() {
}

void IoCtx::set_pool_full_try() {
}

void IoCtx::unset_pool_full_try() {
}

static int save_operation_result(int result, int *pval) {
  if (pval != NULL) {
    *pval = result;
  }
  return result;
}

ObjectOperation::ObjectOperation() {
  LRemObjectOperationImpl *o = new LRemObjectOperationImpl();
  o->get();
  impl = reinterpret_cast<ObjectOperationImpl*>(o);
}

librados::ObjectOperation::ObjectOperation(ObjectOperation&& rhs)
  : impl(rhs.impl) {
  rhs.impl = nullptr;
}

ObjectOperation::~ObjectOperation() {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  if (o) {
    o->put();
    o = NULL;
  }
}

librados::ObjectOperation&
librados::ObjectOperation::operator =(ObjectOperation&& rhs) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  if (o) {
    o->put();
  }
  impl = rhs.impl;
  rhs.impl = nullptr;
  return *this;
}

void ObjectOperation::assert_exists() {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::assert_exists, _1, _7, _4));
}

void ObjectOperation::assert_version(uint64_t ver) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::assert_version, _1, _7, ver));
}

void ObjectOperation::exec(const char *cls, const char *method,
                           bufferlist& inbl) {
  dout(20) << __func__ << "()" << dendl;
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::exec, _1, _7,
			       librados_stub::get_class_handler(), cls,
			       method, inbl, _3, _4, _5));
}

void ObjectOperation::exec(const char *cls, const char *method,
                           bufferlist& inbl,
                           bufferlist *outbl,
                           int *prval) {
  dout(20) << __func__ << "()" << dendl;
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::exec, _1, _7,
                                          librados_stub::get_class_handler(), cls,
                                          method, inbl, outbl, _4, _5);
  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

class ObjectOpCompletionCtx {
  librados::ObjectOperationCompletion *completion;
  bufferlist bl;
public:
  ObjectOpCompletionCtx(librados::ObjectOperationCompletion *c) : completion(c) {}
  ~ObjectOpCompletionCtx() {
    delete completion;
  }
  void finish(int r) {
    completion->handle_completion(r, bl);
    delete completion;
    completion = nullptr;
  }

  bufferlist *outbl() {
    return &bl;
  }
};

static int handle_operation_completion(int result, ObjectOpCompletionCtx *ctx) {
  ctx->finish(result);
  delete ctx;
  return result;
}

void ObjectOperation::exec(const char *cls, const char *method,
                           bufferlist& inbl, ObjectOperationCompletion *completion) {
  dout(20) << __func__ << "(): " << cls << ":" << method << dendl;
  ObjectOpCompletionCtx *ctx{nullptr};
  bufferlist *outbl{nullptr};

  if (completion) {
    ctx = new ObjectOpCompletionCtx(completion);
    outbl = ctx->outbl();
  }

  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::exec, _1, _7,
                                          librados_stub::get_class_handler(), cls,
                                          method, inbl, outbl, _4, _5);
  if (ctx) {
    op = std::bind(handle_operation_completion,
                   std::bind(op, _1, _2, _3, _4, _5, _6, _7), ctx);
  }
  o->ops.push_back(op);
}

void ObjectOperation::set_op_flags2(int flags) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  if (o->ops.empty()) {
    return;
  }

  /* insert op before the last operation so that it affects it */
  auto it = o->ops.end();
  --it;
  o->ops.insert(it, std::bind(&LRemIoCtxImpl::set_op_flags, _1, _7, flags));
}

size_t ObjectOperation::size() {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  return o->ops.size();
}

void ObjectOperation::cmpext(uint64_t off, const bufferlist& cmp_bl,
                             int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::cmpext, _1, _7, off,
                                           cmp_bl, _4);
  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectOperation::cmpxattr(const char *name, uint8_t op, const bufferlist& v)
{
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::cmpxattr_str, _1, _7, name, op, v));
}

void ObjectOperation::cmpxattr(const char *name, uint8_t op, uint64_t v)
{
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::cmpxattr, _1, _7, name, op, v));
}

void ObjectReadOperation::list_snaps(snap_set_t *out_snaps, int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::list_snaps, _1, _7,
                                           out_snaps);
  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::list_watchers(std::list<obj_watch_t> *out_watchers,
                                        int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::list_watchers, _1,
                                           _7, out_watchers);
  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::read(size_t off, uint64_t len, bufferlist *pbl,
                               int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op;
  if (pbl != NULL) {
    op = std::bind(&LRemIoCtxImpl::read, _1, _7, len, off, pbl, _4, nullptr);
  } else {
    op = std::bind(&LRemIoCtxImpl::read, _1, _7, len, off, _3, _4, nullptr);
  }

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::sparse_read(uint64_t off, uint64_t len,
                                      std::map<uint64_t,uint64_t> *m,
                                      bufferlist *pbl, int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op;
  if (pbl != NULL) {
    op = std::bind(&LRemIoCtxImpl::sparse_read, _1, _7, off, len, m, pbl, _4);
  } else {
    op = std::bind(&LRemIoCtxImpl::sparse_read, _1, _7, off, len, m, _3, _4);
  }

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::stat(uint64_t *psize, time_t *pmtime, int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::stat, _1, _7,
                                           psize, pmtime);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::stat2(uint64_t *psize, struct timespec *pts, int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::stat2, _1, _7,
                                           psize, pts);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::getxattrs(map<string, bufferlist> *pattrs, int *prval) {
  dout(20) << __func__ << "()" << dendl;
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::xattr_get, _1, _7,
                                         pattrs);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::getxattr(const char *name, bufferlist *pbl, int *prval) {
  dout(20) << __func__ << "()" << dendl;
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::getxattr, _1, _7,
                                         name, pbl);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::omap_get_keys2(const std::string &start_after,
                                         uint64_t max_return,
                                         std::set<std::string> *out_keys,
                                         bool *pmore,
                                         int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::omap_get_keys2, _1, _7,
                                         start_after, max_return,
                                         out_keys, pmore);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::omap_get_vals2(const std::string &start_after,
                                         const std::string &filter_prefix,
                                         uint64_t max_return,
                                         std::map<std::string, bufferlist> *out_vals,
                                         bool *pmore,
                                         int *prval) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);

  ObjectOperationLRemImpl op = std::bind(&LRemIoCtxImpl::omap_get_vals2, _1, _7,
                                         start_after, filter_prefix, max_return,
                                         out_vals, pmore);

  if (prval != NULL) {
    op = std::bind(save_operation_result,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), prval);
  }
  o->ops.push_back(op);
}

void ObjectReadOperation::omap_get_vals2(const std::string &start_after,
                                         uint64_t max_return,
                                         std::map<std::string, bufferlist> *out_vals,
                                         bool *pmore,
                                         int *prval) {
  omap_get_vals2(start_after, string(), max_return,
                 out_vals, pmore, prval);
}

void ObjectReadOperation::cache_flush() {
}

void ObjectReadOperation::cache_try_flush() {
}

void ObjectReadOperation::cache_evict() {
}

void ObjectReadOperation::tier_flush() {
}

void ObjectReadOperation::tier_evict() {
}


void ObjectWriteOperation::append(const bufferlist &bl) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::append, _1, _7, bl, _5));
}

void ObjectWriteOperation::create(bool exclusive) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::create, _1, _7, exclusive, _5));
}

void ObjectWriteOperation::omap_set(const std::map<std::string, bufferlist> &map) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::omap_set, _1, _7, boost::ref(map)));
}

void ObjectWriteOperation::omap_set_header(const bufferlist& bl) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::omap_set_header, _1, _7, bl));
}

void ObjectWriteOperation::omap_rm_keys(const std::set<std::string>& keys) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::omap_rm_keys, _1, _7, keys));
}

void ObjectWriteOperation::omap_clear() {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::omap_clear, _1, _7));
}

void ObjectWriteOperation::copy_from(const std::string& src,
                                     const IoCtx& src_ioctx,
                                     uint64_t src_version,
                                     uint32_t src_fadvise_flags)
{
#warning not implemented
}


void ObjectWriteOperation::remove() {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::remove, _1, _7, _5));
}

void ObjectWriteOperation::selfmanaged_snap_rollback(uint64_t snapid) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::selfmanaged_snap_rollback,
			       _1, _7, snapid));
}

void ObjectWriteOperation::snap_rollback(snap_t snapid) {
#warning not implemented
}

void ObjectWriteOperation::set_alloc_hint(uint64_t expected_object_size,
                                          uint64_t expected_write_size) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::set_alloc_hint, _1, _7,
			       expected_object_size, expected_write_size, 0,
                               _5));
}

void ObjectWriteOperation::set_alloc_hint2(uint64_t expected_object_size,
                                           uint64_t expected_write_size,
                                           uint32_t flags) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::set_alloc_hint, _1, _7,
			       expected_object_size, expected_write_size, flags,
                               _5));
}

void ObjectWriteOperation::tmap_update(const bufferlist& cmdbl) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::tmap_update, _1, _7,
                               cmdbl));
}

void ObjectWriteOperation::truncate(uint64_t off) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::truncate, _1, _7, off, _5));
}

void ObjectWriteOperation::write(uint64_t off, const bufferlist& bl) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::write, _1, _7, bl, bl.length(),
			       off, _5));
}

void ObjectWriteOperation::write_full(const bufferlist& bl) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::write_full, _1, _7, bl, _5));
}

void ObjectWriteOperation::writesame(uint64_t off, uint64_t len,
                                     const bufferlist& bl) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::writesame, _1, _7, bl, len,
			       off, _5));
}

void ObjectWriteOperation::zero(uint64_t off, uint64_t len) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::zero, _1, _7, off, len, _5));
}

void ObjectWriteOperation::mtime(time_t *pt) {
  if (!pt) {
    return;
  }
  struct timespec ts;
  ts.tv_sec = *pt;
  ts.tv_nsec = 0;
  mtime2(&ts);
}

void ObjectWriteOperation::mtime2(struct timespec *pts) {
  if (!pts) {
    return;
  }
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::mtime2, _1, _7, *pts, _5));
}

void ObjectWriteOperation::setxattr(const char *name, const bufferlist& v) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::setxattr, _1, _7, name, v));
}

void ObjectWriteOperation::rmxattr(const char *name) {
  LRemObjectOperationImpl *o = reinterpret_cast<LRemObjectOperationImpl*>(impl);
  o->ops.push_back(std::bind(&LRemIoCtxImpl::rmxattr, _1, _7, name));
}

void ObjectWriteOperation::set_redirect(const std::string& tgt_obj,
                                        const IoCtx& tgt_ioctx,
                                        uint64_t tgt_version,
                                        int flag) {
}

void ObjectReadOperation::set_chunk(uint64_t src_offset,
                                    uint64_t src_length,
                                    const IoCtx& tgt_ioctx,
                                    string tgt_oid,
                                    uint64_t tgt_offset,
                                    int flag) {
}

void ObjectWriteOperation::tier_promote() {
#warning not implemented
}

void ObjectWriteOperation::unset_manifest() {
#warning not implemented
}

Rados::Rados() : client(NULL) {
}

Rados::Rados(IoCtx& ioctx) {
  LRemIoCtxImpl *ctx = reinterpret_cast<LRemIoCtxImpl*>(ioctx.io_ctx_impl);
  LRemRadosClient *impl = ctx->get_rados_client();
  impl->get();

  client = reinterpret_cast<RadosClient*>(impl);
  ceph_assert(client != NULL);
}

Rados::~Rados() {
  shutdown();
}

void Rados::from_rados_t(rados_t p, Rados &rados) {
  if (rados.client != nullptr) {
    reinterpret_cast<LRemRadosClient*>(rados.client)->put();
    rados.client = nullptr;
  }

  auto impl = reinterpret_cast<LRemRadosClient*>(p);
  if (impl) {
    impl->get();
    rados.client = reinterpret_cast<RadosClient*>(impl);
  }
}

PoolAsyncCompletion *Rados::pool_async_create_completion()
{
  PoolAsyncCompletionImpl *c = new PoolAsyncCompletionImpl;
  return new PoolAsyncCompletion(c);
}

AioCompletion *Rados::aio_create_completion(void *cb_arg,
                                            callback_t cb_complete) {
  AioCompletionImpl *c;
  int r = rados_aio_create_completion2(cb_arg, cb_complete,
      reinterpret_cast<void**>(&c));
  ceph_assert(r == 0);
  return new AioCompletion(c);
}

librados::AioCompletion *librados::Rados::aio_create_completion()
{
  return aio_create_completion(nullptr, nullptr);
}

int Rados::aio_watch_flush(AioCompletion* c) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->aio_watch_flush(c->pc);
}

int Rados::blocklist_add(const std::string& client_address,
			 uint32_t expire_seconds) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->blocklist_add(client_address, expire_seconds);
}

config_t Rados::cct() {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return reinterpret_cast<config_t>(impl->cct());
}

int Rados::cluster_fsid(std::string* fsid) {
  *fsid = "00000000-1111-2222-3333-444444444444";
  return 0;
}

int Rados::conf_set(const char *option, const char *value) {
  return rados_conf_set(reinterpret_cast<rados_t>(client), option, value);
}

int Rados::conf_get(const char *option, std::string &val) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  CephContext *cct = impl->cct();

  char *str = NULL;
  int ret = cct->_conf.get_val(option, &str, -1);
  if (ret != 0) {
    free(str);
    return ret;
  }

  val = str;
  free(str);
  return 0;
}

int Rados::conf_parse_env(const char *env) const {
  return rados_conf_parse_env(reinterpret_cast<rados_t>(client), env);
}

int Rados::conf_read_file(const char * const path) const {
  return rados_conf_read_file(reinterpret_cast<rados_t>(client), path);
}

int Rados::connect() {
  return rados_connect(reinterpret_cast<rados_t>(client));
}

uint64_t Rados::get_instance_id() {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->get_instance_id();
}

int Rados::get_min_compatible_osd(int8_t* require_osd_release) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->get_min_compatible_osd(require_osd_release);
}

int Rados::get_min_compatible_client(int8_t* min_compat_client,
                                     int8_t* require_min_compat_client) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->get_min_compatible_client(min_compat_client,
                                         require_min_compat_client);
}

int Rados::init(const char * const id) {
  return rados_create(reinterpret_cast<rados_t *>(&client), id);
}

int Rados::init_with_context(config_t cct_) {
  return rados_create_with_context(reinterpret_cast<rados_t *>(&client), cct_);
}

int Rados::ioctx_create(const char *name, IoCtx &io) {
  rados_ioctx_t p;
  int ret = rados_ioctx_create(reinterpret_cast<rados_t>(client), name, &p);
  if (ret) {
    return ret;
  }

  io.close();
  io.io_ctx_impl = reinterpret_cast<IoCtxImpl*>(p);
  return 0;
}

int Rados::ioctx_create2(int64_t pool_id, IoCtx &io)
{
  rados_ioctx_t p;
  int ret = rados_ioctx_create2(reinterpret_cast<rados_t>(client), pool_id, &p);
  if (ret) {
    return ret;
  }

  io.close();
  io.io_ctx_impl = reinterpret_cast<IoCtxImpl*>(p);
  return 0;
}

int Rados::mon_command(std::string cmd, const bufferlist& inbl,
                       bufferlist *outbl, std::string *outs) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);

  std::vector<std::string> cmds;
  cmds.push_back(cmd);
  return impl->mon_command(cmds, inbl, outbl, outs);
}

int Rados::service_daemon_register(const std::string& service,
                                   const std::string& name,
                                   const std::map<std::string,std::string>& metadata) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->service_daemon_register(service, name, metadata);
}

int Rados::service_daemon_update_status(std::map<std::string,std::string>&& status) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->service_daemon_update_status(std::move(status));
}

int Rados::pool_create(const char *name) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->pool_create(name);
}

int Rados::pool_create_async(const char *name, PoolAsyncCompletion *c) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->pool_create_async(name, c->pc);
}


int Rados::pool_delete(const char *name) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->pool_delete(name);
}

int Rados::pool_get_base_tier(int64_t pool, int64_t* base_tier) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->pool_get_base_tier(pool, base_tier);
}

int Rados::pool_list(std::list<std::string>& v) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  std::list<std::pair<int64_t, std::string> > pools;
  int r = impl->pool_list(pools);
  if (r < 0) {
    return r;
  }

  v.clear();
  for (std::list<std::pair<int64_t, std::string> >::iterator it = pools.begin();
       it != pools.end(); ++it) {
    v.push_back(it->second);
  }
  return 0;
}

int Rados::pool_list2(std::list<std::pair<int64_t, std::string> >& v)
{
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->pool_list(v);
}

int64_t Rados::pool_lookup(const char *name) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->pool_lookup(name);
}

int Rados::pool_reverse_lookup(int64_t id, std::string *name) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->pool_reverse_lookup(id, name);
}

int Rados::cluster_stat(cluster_stat_t& result) {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->cluster_stat(result);
};

void Rados::shutdown() {
  if (client == NULL) {
    return;
  }
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  impl->put();
  client = NULL;
}

void Rados::test_blocklist_self(bool set) {
}

int Rados::wait_for_latest_osdmap() {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->wait_for_latest_osdmap();
}

int Rados::watch_flush() {
  LRemRadosClient *impl = reinterpret_cast<LRemRadosClient*>(client);
  return impl->watch_flush();
}

  struct PlacementGroupImpl {
    pg_t pgid;
  };

PlacementGroup::PlacementGroup()
    : impl{new PlacementGroupImpl} {}

PlacementGroup::PlacementGroup(const PlacementGroup& pg)
    : impl{new PlacementGroupImpl} {
  impl->pgid = pg.impl->pgid;
}

PlacementGroup::~PlacementGroup() {}

bool PlacementGroup::parse(const char* s) {
  return false;
}

std::ostream& librados::operator<<(std::ostream& out,
                                   const librados::PlacementGroup& pg)
{
  return out << pg.impl->pgid;
}


int Rados::get_inconsistent_pgs(int64_t pool_id,
                                std::vector<PlacementGroup>* pgs) {
  return -ENOTSUP;
}

int Rados::Rados::get_pool_stats(std::list<string>& v,
                                 stats_map& result) {
  return -ENOTSUP;
}


bool Rados::get_pool_is_selfmanaged_snaps_mode(const std::string& pool)
{
  return false;
}

int Rados::get_inconsistent_objects(const PlacementGroup& pg,
                                    const object_id_t &start_after,
                                    unsigned max_return,
                                    AioCompletion *c,
                                    std::vector<inconsistent_obj_t>* objects,
                                    uint32_t* interval) {
  return -ENOTSUP;
}

int Rados::get_inconsistent_snapsets(const PlacementGroup& pg,
                                     const object_id_t &start_after,
                                     unsigned max_return,
                                     AioCompletion *c,
                                     std::vector<inconsistent_snapset_t>* snapset,
                                     uint32_t* interval) {
  return -ENOTSUP;
}

WatchCtx::~WatchCtx() {
}

WatchCtx2::~WatchCtx2() {
}


} // namespace librados

int cls_cxx_create(cls_method_context_t hctx, bool exclusive) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << " exclusive=" << exclusive << dendl;
  return ctx->io_ctx_impl->create(ctx->trans, exclusive, ctx->snapc);
}

int cls_cxx_remove(cls_method_context_t hctx) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  return ctx->io_ctx_impl->remove(ctx->trans, ctx->io_ctx_impl->get_snap_context());
}

int cls_cxx_stat2(cls_method_context_t hctx, uint64_t *size, ceph::real_time *mtime) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  struct timespec ts;
  int r = ctx->io_ctx_impl->stat2(ctx->trans, size, &ts);
  if (r < 0) {
    return r;
  }

  if (mtime) {
    *mtime = ceph::real_clock::from_timespec(ts);
  }

  return 0;
}

int cls_get_request_origin(cls_method_context_t hctx, entity_inst_t *origin) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);

  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  librados::LRemRadosClient *rados_client =
    ctx->io_ctx_impl->get_rados_client();

  struct sockaddr_in sin;
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = 0;
  inet_pton(AF_INET, "127.0.0.1", &sin.sin_addr);

  entity_addr_t entity_addr(entity_addr_t::TYPE_DEFAULT,
                            rados_client->get_nonce());
  entity_addr.in4_addr() = sin;

  *origin = entity_inst_t(
    entity_name_t::CLIENT(rados_client->get_instance_id()),
    entity_addr);
  return 0;
}

int cls_cxx_getxattr(cls_method_context_t hctx, const char *name,
                     bufferlist *outbl) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << " name=" << name << dendl;
  return ctx->io_ctx_impl->getxattr(ctx->trans, name, outbl);
}

int cls_cxx_getxattrs(cls_method_context_t hctx, std::map<string, bufferlist> *attrset) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  return ctx->io_ctx_impl->xattr_get(ctx->trans, attrset);
}

int cls_cxx_map_get_keys(cls_method_context_t hctx, const string &start_obj,
                         uint64_t max_to_get, std::set<string> *keys, bool *more) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid
    << " start_obj=" << start_obj << " max=" << max_to_get << dendl;
  return ctx->io_ctx_impl->omap_get_keys2(ctx->trans, start_obj, max_to_get,
                                          keys, more);
}

int cls_cxx_map_get_val(cls_method_context_t hctx, const string &key,
                        bufferlist *outbl) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);

  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << " key=" << key << dendl;
  std::map<string, bufferlist> vals;
  int r = ctx->io_ctx_impl->omap_get_vals(ctx->trans, "", key, 1024, &vals);
  if (r < 0) {
    return r;
  }

  std::map<string, bufferlist>::iterator it = vals.find(key);
  if (it == vals.end()) {
    return -ENOENT;
  }

  *outbl = it->second;
  return 0;
}

int cls_cxx_map_get_vals(cls_method_context_t hctx, const string &start_obj,
                         const string &filter_prefix, uint64_t max_to_get,
                         std::map<string, bufferlist> *vals, bool *more) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid
    << " start_obj=" << start_obj << " filter_prefix=" << filter_prefix << " max=" << max_to_get << dendl;
  int r = ctx->io_ctx_impl->omap_get_vals2(ctx->trans, start_obj, filter_prefix,
					  max_to_get, vals, more);
  if (r < 0) {
    return r;
  }
  return vals->size();
}

int cls_cxx_map_remove_key(cls_method_context_t hctx, const string &key) {
  std::set<std::string> keys;
  keys.insert(key);

  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << " key=" << key << dendl;
  return ctx->io_ctx_impl->omap_rm_keys(ctx->trans, keys);
}

int cls_cxx_map_remove_range(cls_method_context_t hctx,
                             const std::string& key_begin,
                             const std::string& key_end) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid
    << " key_begin=" << key_begin << " key_end=" << key_end << dendl;
  return ctx->io_ctx_impl->omap_rm_range(ctx->trans, key_begin, key_end);
}


int cls_cxx_map_set_val(cls_method_context_t hctx, const string &key,
                        bufferlist *inbl) {
  std::map<std::string, bufferlist> m;
  m[key] = *inbl;
  return cls_cxx_map_set_vals(hctx, &m);
}

int cls_cxx_map_set_vals(cls_method_context_t hctx,
                         const std::map<string, bufferlist> *map) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  return ctx->io_ctx_impl->omap_set(ctx->trans, *map);
}

int cls_cxx_map_clear(cls_method_context_t hctx) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  return ctx->io_ctx_impl->omap_clear(ctx->trans);
}

int cls_cxx_read(cls_method_context_t hctx, int ofs, int len,
                 bufferlist *outbl) {
  return cls_cxx_read2(hctx, ofs, len, outbl, 0);
}

int cls_cxx_read2(cls_method_context_t hctx, int ofs, int len,
                  bufferlist *outbl, uint32_t op_flags) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid
    << " ofs=" << ofs << " len=" << len << " op_flags=" << op_flags << dendl;
  return ctx->io_ctx_impl->read(
          ctx->trans, len, ofs, outbl, ctx->snap_id, nullptr);
}

int cls_cxx_setxattr(cls_method_context_t hctx, const char *name,
                     bufferlist *inbl) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << " name=" << name << dendl;
  return ctx->io_ctx_impl->setxattr(ctx->trans, name, *inbl);
}

int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  return ctx->io_ctx_impl->stat(ctx->trans, size, mtime);
}

int cls_cxx_write(cls_method_context_t hctx, int ofs, int len,
                  bufferlist *inbl) {
  return cls_cxx_write2(hctx, ofs, len, inbl, 0);
}

int cls_cxx_write2(cls_method_context_t hctx, int ofs, int len,
                   bufferlist *inbl, uint32_t op_flags) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid
    << " ofs=" << ofs << " len=" << len << " op_flags=" << op_flags << dendl;
  return ctx->io_ctx_impl->write(ctx->trans, *inbl, len, ofs, ctx->snapc);
}

int cls_cxx_write_full(cls_method_context_t hctx, bufferlist *inbl) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  return ctx->io_ctx_impl->write_full(ctx->trans, *inbl, ctx->snapc);
}

int cls_cxx_replace(cls_method_context_t hctx, int ofs, int len,
                    bufferlist *inbl) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid
    << " ofs=" << ofs << " len=" << len << dendl;
  int r = ctx->io_ctx_impl->truncate(ctx->trans, 0, ctx->snapc);
  if (r < 0) {
    return r;
  }
  return ctx->io_ctx_impl->write(ctx->trans, *inbl, len, ofs, ctx->snapc);
}

int cls_cxx_truncate(cls_method_context_t hctx, int ofs) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid
    << " ofs=" << ofs << dendl;
  return ctx->io_ctx_impl->truncate(ctx->trans, ofs, ctx->snapc);
}

int cls_cxx_write_zero(cls_method_context_t hctx, int ofs, int len) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid
    << " ofs=" << ofs << " len=" << len << dendl;
  return ctx->io_ctx_impl->zero(ctx->trans, len, ofs, ctx->snapc);
}

int cls_cxx_list_watchers(cls_method_context_t hctx,
			  obj_list_watch_response_t *watchers) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);

  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  std::list<obj_watch_t> obj_watchers;
  int r = ctx->io_ctx_impl->list_watchers(ctx->trans, &obj_watchers);
  if (r < 0) {
    return r;
  }

  for (auto &w : obj_watchers) {
    watch_item_t watcher;
    watcher.name = entity_name_t::CLIENT(w.watcher_id);
    watcher.cookie = w.cookie;
    watcher.timeout_seconds = w.timeout_seconds;
    watcher.addr.parse(w.addr);
    watchers->entries.push_back(watcher);
  }

  return 0;
}

uint64_t cls_get_features(cls_method_context_t hctx) {
  return CEPH_FEATURES_SUPPORTED_DEFAULT;
}

uint64_t cls_get_client_features(cls_method_context_t hctx) {
  return CEPH_FEATURES_SUPPORTED_DEFAULT;
}

int cls_get_snapset_seq(cls_method_context_t hctx, uint64_t *snap_seq) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  librados::snap_set_t snapset;
  int r = ctx->io_ctx_impl->list_snaps(ctx->trans, &snapset);
  if (r < 0) {
    return r;
  }

  *snap_seq = snapset.seq;
  return 0;
}

void cls_cxx_subop_version(cls_method_context_t hctx, std::string *s)
{
  if (!s)
    return;

  char buf[32];
  uint64_t ver = cls_current_version(hctx);
  int subop_num = cls_current_subop_num(hctx);
  snprintf(buf, sizeof(buf), "%lld.%d", (long long)ver, subop_num);

  *s = buf;
}

int cls_log(int level, const char *format, ...) {
  int size = 256;
  va_list ap;
  while (1) {
    char buf[size];
    va_start(ap, format);
    int n = vsnprintf(buf, size, format, ap);
    va_end(ap);
    if ((n > -1 && n < size) || size > 8196) {
      dout(ceph::dout::need_dynamic(level)) << buf << dendl;
      return n;
    }
    size *= 2;
  }
  return 0;
}

int cls_register(const char *name, cls_handle_t *handle) {
  librados::LRemClassHandler *cls = librados_stub::get_class_handler();
  dout(20) << __func__ << "() name=" << name << dendl;
  return cls->create(name, handle);
}

int cls_register_cxx_method(cls_handle_t hclass, const char *method,
    int flags,
    cls_method_cxx_call_t class_call,
    cls_method_handle_t *handle) {
  librados::LRemClassHandler *cls = librados_stub::get_class_handler();
  dout(20) << __func__ << "() method=" << method << " flags=" << flags << " call=" << (void *)class_call << dendl;
  return cls->create_method(hclass, method, flags, class_call, handle);
}

int cls_register_cxx_filter(cls_handle_t hclass,
                            const std::string &filter_name,
                            cls_cxx_filter_factory_t fn,
                            cls_filter_handle_t *)
{
  librados::LRemClassHandler *cls = librados_stub::get_class_handler();
  return cls->create_filter(hclass, filter_name, fn);
}

ceph_release_t cls_get_required_osd_release(cls_handle_t hclass) {
  return ceph_release_t::nautilus;
}

ceph_release_t cls_get_min_compatible_client(cls_handle_t hclass) {
  return ceph_release_t::nautilus;
}

// stubs to silence LRemClassHandler::open_class()
PGLSFilter::~PGLSFilter()
{}

int cls_gen_rand_base64(char *dest, int size) /* size should be the required string size + 1 */
{
  char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */
  int ret;

  ret = cls_gen_random_bytes(buf, sizeof(buf));
  if (ret < 0) {
    derr << "cannot get random bytes: " << ret << dendl;
    return -1;
  }

  ret = ceph_armor(tmp_dest, &tmp_dest[sizeof(tmp_dest)],
                   (const char *)buf, ((const char *)buf) + ((size - 1) * 3 + 4 - 1) / 4);
  if (ret < 0) {
    derr << "ceph_armor failed" << dendl;
    return -1;
  }
  tmp_dest[ret] = '\0';
  memcpy(dest, tmp_dest, size);
  dest[size-1] = '\0';

  return 0;
}

int cls_cxx_chunk_write_and_set(cls_method_handle_t, int,
				int, bufferlist *,
				uint32_t, bufferlist *, int) {
  return -ENOTSUP;
}

int cls_cxx_map_read_header(cls_method_handle_t hctx, bufferlist *bl) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << dendl;
  return ctx->io_ctx_impl->omap_get_header(ctx->trans, bl);
}

int cls_cxx_map_write_header(cls_method_context_t hctx, ceph::buffer::list *inbl) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  bufferlist _bl;
  bufferlist& bl = (inbl ? *inbl : _bl);
  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << " bl.length()=" << bl.length() << dendl;
  return ctx->io_ctx_impl->omap_set_header(ctx->trans, bl);
}

uint64_t cls_current_version(cls_method_context_t hctx) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  uint64_t ver;
  int r = ctx->io_ctx_impl->get_current_ver(ctx->trans, &ver);
  if (r < 0) {
    return (uint64_t)r;
  }

  dout(20) << __func__ << "() ctx=" << ctx->io_ctx_impl << " oid=" << ctx->oid << " --> ver=" << ver << dendl;
  return ver;
}

int cls_current_subop_num(cls_method_context_t hctx) {
  librados::LRemClassHandler::MethodContext *ctx =
    reinterpret_cast<librados::LRemClassHandler::MethodContext*>(hctx);
  return ctx->trans->op_id;
}

uint64_t cls_get_osd_min_alloc_size(cls_method_context_t hctx) {
  return 0;
}

uint64_t cls_get_pool_stripe_width(cls_method_context_t hctx) {
  return 0;
}

int cls_gen_random_bytes(char *buf, int size)
{
  g_ceph_context->random()->get_bytes(buf, size);
  return 0;
}

