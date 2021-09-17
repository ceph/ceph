// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemIoCtxImpl.h"
#include "LRemClassHandler.h"
#include "LRemRadosClient.h"
#include "LRemWatchNotify.h"
#include "librados/AioCompletionImpl.h"
#include "include/ceph_assert.h"
#include "common/Finisher.h"
#include "common/valgrind.h"
#include "objclass/objclass.h"
#include <functional>
#include <errno.h>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rados

namespace librados {

LRemIoCtxImpl::LRemIoCtxImpl() : m_client(NULL) {
  get();
}

LRemIoCtxImpl::LRemIoCtxImpl(LRemRadosClient *client, int64_t pool_id,
                             const std::string& pool_name)
  : m_client(client), m_pool_id(pool_id), m_pool_name(pool_name),
    m_snap_seq(CEPH_NOSNAP)
{
  m_client->get();
  get();
}

LRemIoCtxImpl::LRemIoCtxImpl(const LRemIoCtxImpl& rhs)
  : m_client(rhs.m_client),
    m_pool_id(rhs.m_pool_id),
    m_pool_name(rhs.m_pool_name),
    m_oloc(rhs.m_oloc),
    m_snap_seq(rhs.m_snap_seq)
{
  m_client->get();
  get();
}

LRemIoCtxImpl::~LRemIoCtxImpl() {
  ceph_assert(m_pending_ops == 0);
}

void LRemObjectOperationImpl::get() {
  m_refcount++;
}

void LRemObjectOperationImpl::put() {
  if (--m_refcount == 0) {
    ANNOTATE_HAPPENS_AFTER(&m_refcount);
    ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_refcount);
    delete this;
  } else {
    ANNOTATE_HAPPENS_BEFORE(&m_refcount);
  }
}

void LRemIoCtxImpl::get() {
  m_refcount++;
}

void LRemIoCtxImpl::put() {
  if (--m_refcount == 0) {
    m_client->put();
    delete this;
  }
}

uint64_t LRemIoCtxImpl::get_instance_id() const {
  return m_client->get_instance_id();
}

int64_t LRemIoCtxImpl::get_id() {
  return m_pool_id;
}

uint64_t LRemIoCtxImpl::get_last_version() {
  return 0;
}

std::string LRemIoCtxImpl::get_pool_name() {
  return m_pool_name;
}

int LRemIoCtxImpl::aio_flush() {
  m_client->flush_aio_operations();
  return 0;
}

void LRemIoCtxImpl::aio_flush_async(AioCompletionImpl *c) {
  m_client->flush_aio_operations(c);
}

void LRemIoCtxImpl::aio_notify(const std::string& oid, AioCompletionImpl *c,
                               bufferlist& bl, uint64_t timeout_ms,
                               bufferlist *pbl) {
  get();
  m_pending_ops++;
  c->get();
  C_AioNotify *ctx = new C_AioNotify(this, c);
  m_client->get_watch_notify()->aio_notify(m_client, m_pool_id, get_namespace(),
                                           oid, bl, timeout_ms, pbl, ctx);
}

int LRemIoCtxImpl::aio_operate(const std::string& oid, LRemObjectOperationImpl &ops,
                               AioCompletionImpl *c, SnapContext *snap_context,
                               int flags) {
  // TODO flags for now
  get();
  ops.get();
  m_pending_ops++;
  m_client->add_aio_operation(oid, true, std::bind(
    &LRemIoCtxImpl::execute_aio_operations, this, oid, &ops,
    reinterpret_cast<bufferlist*>(0), m_snap_seq,
    snap_context != NULL ? *snap_context : m_snapc, flags, nullptr), c);
  return 0;
}

int LRemIoCtxImpl::aio_operate_read(const std::string& oid,
                                    LRemObjectOperationImpl &ops,
                                    AioCompletionImpl *c, int flags,
                                    bufferlist *pbl, uint64_t snap_id,
                                    uint64_t* objver) {
  // TODO ignoring flags for now
  get();
  ops.get();
  m_pending_ops++;
  m_client->add_aio_operation(oid, true, std::bind(
    &LRemIoCtxImpl::execute_aio_operations, this, oid, &ops, pbl, snap_id,
    m_snapc, flags, objver), c);
  return 0;
}

int LRemIoCtxImpl::aio_watch(const std::string& o, AioCompletionImpl *c,
                             uint64_t *handle, librados::WatchCtx2 *watch_ctx) {
  get();
  m_pending_ops++;
  c->get();
  C_AioNotify *ctx = new C_AioNotify(this, c);
  if (m_client->is_blocklisted()) {
    m_client->get_aio_finisher()->queue(ctx, -EBLOCKLISTED);
  } else {
    m_client->get_watch_notify()->aio_watch(m_client, m_pool_id,
                                            get_namespace(), o,
                                            get_instance_id(), handle, nullptr,
                                            watch_ctx, ctx);
  }
  return 0;
}

int LRemIoCtxImpl::aio_unwatch(uint64_t handle, AioCompletionImpl *c) {
  get();
  m_pending_ops++;
  c->get();
  C_AioNotify *ctx = new C_AioNotify(this, c);
  if (m_client->is_blocklisted()) {
    m_client->get_aio_finisher()->queue(ctx, -EBLOCKLISTED);
  } else {
    m_client->get_watch_notify()->aio_unwatch(m_client, handle, ctx);
  }
  return 0;
}

int LRemIoCtxImpl::aio_exec(const std::string& oid, AioCompletionImpl *c,
                            LRemClassHandler *handler,
                            const char *cls, const char *method,
                            bufferlist& inbl, bufferlist *outbl) {
  auto trans = init_transaction(oid);
  m_client->add_aio_operation(oid, true, std::bind(
    &LRemIoCtxImpl::exec, this, oid, handler, cls, method,
    inbl, outbl, m_snap_seq, m_snapc, trans), c);
  return 0;
}

int LRemIoCtxImpl::exec(const std::string& oid, LRemClassHandler *handler,
                        const char *cls, const char *method,
                        bufferlist& inbl, bufferlist* outbl,
                        uint64_t snap_id, const SnapContext &snapc,
                        LRemTransactionStateRef& trans) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  bool write;
  cls_method_cxx_call_t call = handler->get_method(cls, method, &write);
  if (call == NULL) {
    return -ENOSYS;
  }
  trans->set_write(write);

  int r = (*call)(reinterpret_cast<cls_method_context_t>(
    handler->get_method_context(this, oid, snap_id, snapc, trans).get()), &inbl,
    outbl);

  dout(20) << "objclass exec: " << oid << " -> " << cls << ":" << method << "=" << r <<  "(" << (outbl ? outbl->length() : 0) << ") outbl=" << (void *)outbl << dendl;
  return r;
}

int LRemIoCtxImpl::list_watchers(const std::string& o,
                                 std::list<obj_watch_t> *out_watchers) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  return m_client->get_watch_notify()->list_watchers(m_client,
                                                     m_pool_id, get_namespace(),
                                                     o, out_watchers);
}

int LRemIoCtxImpl::notify(const std::string& o, bufferlist& bl,
                          uint64_t timeout_ms, bufferlist *pbl) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  return m_client->get_watch_notify()->notify(m_client, m_pool_id,
                                              get_namespace(), o, bl,
                                              timeout_ms, pbl);
}

void LRemIoCtxImpl::notify_ack(const std::string& o, uint64_t notify_id,
                               uint64_t handle, bufferlist& bl) {
  m_client->get_watch_notify()->notify_ack(m_client, m_pool_id, get_namespace(),
                                           o, notify_id, handle,
                                           m_client->get_instance_id(), bl);
}

int LRemIoCtxImpl::omap_get_keys2(const std::string& oid,
                                  const std::string& start_after,
                                  uint64_t max_return,
                                  std::set<std::string> *out_keys,
                                  bool *pmore) {
  out_keys->clear();
  std::map<string, bufferlist> vals;
  int r = omap_get_vals2(oid, start_after, "", max_return,
                         &vals, pmore);
  if (r < 0) {
    return r;
  }

  for (std::map<string, bufferlist>::iterator it = vals.begin();
       it != vals.end(); ++it) {
    out_keys->insert(it->first);
  }
  return out_keys->size();
}

int LRemIoCtxImpl::operate(const std::string& oid,
                           LRemObjectOperationImpl &ops,
                           int flags) {
  AioCompletionImpl *comp = new AioCompletionImpl();

  get();
  ops.get();
  m_pending_ops++;
  m_client->add_aio_operation(oid, false, std::bind(
    &LRemIoCtxImpl::execute_aio_operations, this, oid, &ops,
    reinterpret_cast<bufferlist*>(0), m_snap_seq, m_snapc, flags, nullptr), comp);

  comp->wait_for_complete();
  int ret = comp->get_return_value();
  comp->put();
  return ret;
}

int LRemIoCtxImpl::operate_read(const std::string& oid,
                                LRemObjectOperationImpl &ops,
                                bufferlist *pbl,
                                int flags) {
  AioCompletionImpl *comp = new AioCompletionImpl();

  get();
  ops.get();
  m_pending_ops++;
  m_client->add_aio_operation(oid, false, std::bind(
    &LRemIoCtxImpl::execute_aio_operations, this, oid, &ops, pbl,
    m_snap_seq, m_snapc, flags, nullptr), comp);

  comp->wait_for_complete();
  int ret = comp->get_return_value();
  comp->put();
  return ret;
}

void LRemIoCtxImpl::aio_selfmanaged_snap_create(uint64_t *snapid,
                                                AioCompletionImpl *c) {
  m_client->add_aio_operation(
    "", true,
    std::bind(&LRemIoCtxImpl::selfmanaged_snap_create, this, snapid), c);
}

void LRemIoCtxImpl::aio_selfmanaged_snap_remove(uint64_t snapid,
                                                AioCompletionImpl *c) {
  m_client->add_aio_operation(
    "", true,
    std::bind(&LRemIoCtxImpl::selfmanaged_snap_remove, this, snapid), c);
}

int LRemIoCtxImpl::selfmanaged_snap_set_write_ctx(snap_t seq,
                                                  std::vector<snap_t>& snaps) {
  std::vector<snapid_t> snap_ids(snaps.begin(), snaps.end());
  m_snapc = SnapContext(seq, snap_ids);
  return 0;
}

int LRemIoCtxImpl::set_alloc_hint(const std::string& oid,
                                  uint64_t expected_object_size,
                                  uint64_t expected_write_size,
                                  uint32_t flags,
                                  const SnapContext &snapc) {
  return 0;
}

void LRemIoCtxImpl::set_snap_read(snap_t seq) {
  if (seq == 0) {
    seq = CEPH_NOSNAP;
  }
  m_snap_seq = seq;
}


int LRemIoCtxImpl::stat(LRemTransactionStateRef& trans, uint64_t *psize, time_t *pmtime) {
  struct timespec ts;
  int r = stat2(trans, psize, (pmtime ? &ts : nullptr));
  if (r < 0) {
    return r;
  }

  if (pmtime) {
    *pmtime = ts.tv_sec;
  }
  return 0;
}

int LRemIoCtxImpl::getxattr(LRemTransactionStateRef& trans, const char *name, bufferlist *pbl) {
  std::map<string, bufferlist> attrs;
  int r = xattr_get(trans, &attrs);
  if (r < 0) {
    return r;
  }

  std::map<string, bufferlist>::iterator it = attrs.find(name);
  if (it == attrs.end()) {
    return -ENODATA;
  }
  *pbl = it->second;
  return 0;
}

int LRemIoCtxImpl::tmap_update(LRemTransactionStateRef& trans, bufferlist& cmdbl) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  // TODO: protect against concurrent tmap updates
  bufferlist tmap_header;
  std::map<string,bufferlist> tmap;
  uint64_t size = 0;
  auto& oid = trans->oid();
  int r = stat(trans, &size, NULL);
  if (r == -ENOENT) {
    r = create(oid, false, m_snapc);
  }
  if (r < 0) {
    return r;
  }

  if (size > 0) {
    bufferlist inbl;
    r = read(oid, size, 0, &inbl, CEPH_NOSNAP, nullptr);
    if (r < 0) {
      return r;
    }
    auto iter = inbl.cbegin();
    decode(tmap_header, iter);
    decode(tmap, iter);
  }

  __u8 c;
  std::string key;
  bufferlist value;
  auto iter = cmdbl.cbegin();
  decode(c, iter);
  decode(key, iter);

  switch (c) {
    case CEPH_OSD_TMAP_SET:
      decode(value, iter);
      tmap[key] = value;
      break;
    case CEPH_OSD_TMAP_RM:
      r = tmap.erase(key);
      if (r == 0) {
        return -ENOENT;
      }
      break;
    default:
      return -EINVAL;
  }

  bufferlist out;
  encode(tmap_header, out);
  encode(tmap, out);
  r = write_full(oid, out, m_snapc);
  return r;
}

int LRemIoCtxImpl::unwatch(uint64_t handle) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  return m_client->get_watch_notify()->unwatch(m_client, handle);
}

int LRemIoCtxImpl::watch(const std::string& o, uint64_t *handle,
                         librados::WatchCtx *ctx, librados::WatchCtx2 *ctx2) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  return m_client->get_watch_notify()->watch(m_client, m_pool_id,
                                             get_namespace(), o,
                                             get_instance_id(), handle, ctx,
                                             ctx2);
}

int LRemIoCtxImpl::execute_operation(const std::string& oid,
                                     const Operation &operation) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto transaction_state = init_transaction(oid);
  LRemRadosClient::Transaction transaction(m_client, transaction_state);
  return operation(this, oid);
}

int LRemIoCtxImpl::execute_aio_operations(const std::string& oid,
                                          LRemObjectOperationImpl *ops,
                                          bufferlist *pbl, uint64_t snap_id,
                                          const SnapContext &snapc,
                                          int flags,
                                          uint64_t* objver) {
#warning flags not used
  int ret = 0;
  if (m_client->is_blocklisted()) {
    ret = -EBLOCKLISTED;
  } else {
    auto transaction_state = init_transaction(oid);
    LRemRadosClient::Transaction transaction(m_client, transaction_state);
    for (ObjectOperations::iterator it = ops->ops.begin();
         it != ops->ops.end(); ++it) {
      auto& state = transaction.get_state_ref();
      ret = (*it)(this, oid, pbl, snap_id, snapc, objver, state);
      dout(0) << "execute_aio_operations op=" << "(): -> " << ret << dendl;
      if (ret < 0 &&
          !(state->flags & LIBRADOS_OP_FLAG_FAILOK)) {
        break;
      }
      ++state->op_id;
    }
  }
  m_pending_ops--;
  ops->put();
  put();
  return ret;
}

void LRemIoCtxImpl::handle_aio_notify_complete(AioCompletionImpl *c, int r) {
  m_pending_ops--;

  m_client->finish_aio_completion(c, r);
  put();
}

int LRemIoCtxImpl::set_op_flags(LRemTransactionStateRef& trans, int flags) {
  trans->flags = flags;
  return 0;
}

} // namespace librados
