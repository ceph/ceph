// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestIoCtxImpl.h"
#include "test/librados_test_stub/TestClassHandler.h"
#include "test/librados_test_stub/TestRadosClient.h"
#include "test/librados_test_stub/TestWatchNotify.h"
#include "librados/AioCompletionImpl.h"
#include "include/ceph_assert.h"
#include "common/Finisher.h"
#include "common/valgrind.h"
#include "objclass/objclass.h"
#include <functional>
#include <errno.h>

using namespace std;

namespace librados {

TestIoCtxImpl::TestIoCtxImpl() : m_client(NULL) {
  get();
}

TestIoCtxImpl::TestIoCtxImpl(TestRadosClient *client, int64_t pool_id,
                             const std::string& pool_name)
  : m_client(client), m_pool_id(pool_id), m_pool_name(pool_name),
    m_snap_seq(CEPH_NOSNAP)
{
  m_client->get();
  get();
}

TestIoCtxImpl::TestIoCtxImpl(const TestIoCtxImpl& rhs)
  : m_client(rhs.m_client),
    m_pool_id(rhs.m_pool_id),
    m_pool_name(rhs.m_pool_name),
    m_namespace_name(rhs.m_namespace_name),
    m_snap_seq(rhs.m_snap_seq)
{
  m_client->get();
  get();
}

TestIoCtxImpl::~TestIoCtxImpl() {
  ceph_assert(m_pending_ops == 0);
}

void TestObjectOperationImpl::get() {
  m_refcount++;
}

void TestObjectOperationImpl::put() {
  if (--m_refcount == 0) {
    ANNOTATE_HAPPENS_AFTER(&m_refcount);
    ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_refcount);
    delete this;
  } else {
    ANNOTATE_HAPPENS_BEFORE(&m_refcount);
  }
}

void TestIoCtxImpl::get() {
  m_refcount++;
}

void TestIoCtxImpl::put() {
  if (--m_refcount == 0) {
    m_client->put();
    delete this;
  }
}

uint64_t TestIoCtxImpl::get_instance_id() const {
  return m_client->get_instance_id();
}

int64_t TestIoCtxImpl::get_id() {
  return m_pool_id;
}

uint64_t TestIoCtxImpl::get_last_version() {
  return 0;
}

std::string TestIoCtxImpl::get_pool_name() {
  return m_pool_name;
}

int TestIoCtxImpl::aio_flush() {
  m_client->flush_aio_operations();
  return 0;
}

void TestIoCtxImpl::aio_flush_async(AioCompletionImpl *c) {
  m_client->flush_aio_operations(c);
}

void TestIoCtxImpl::aio_notify(const std::string& oid, AioCompletionImpl *c,
                               bufferlist& bl, uint64_t timeout_ms,
                               bufferlist *pbl) {
  m_pending_ops++;
  c->get();
  C_AioNotify *ctx = new C_AioNotify(this, c);
  m_client->get_watch_notify()->aio_notify(m_client, m_pool_id, get_namespace(),
                                           oid, bl, timeout_ms, pbl, ctx);
}

int TestIoCtxImpl::aio_operate(const std::string& oid, TestObjectOperationImpl &ops,
                               AioCompletionImpl *c, SnapContext *snap_context,
                               const ceph::real_time *pmtime, int flags) {
  // TODO flags for now
  ops.get();
  m_pending_ops++;
  m_client->add_aio_operation(oid, true, std::bind(
    &TestIoCtxImpl::execute_aio_operations, this, oid, &ops,
    reinterpret_cast<bufferlist*>(0), m_snap_seq,
    snap_context != NULL ? *snap_context : m_snapc, nullptr), c);
  return 0;
}

int TestIoCtxImpl::aio_operate_read(const std::string& oid,
                                    TestObjectOperationImpl &ops,
                                    AioCompletionImpl *c, int flags,
                                    bufferlist *pbl, uint64_t snap_id,
                                    uint64_t* objver) {
  // TODO ignoring flags for now
  ops.get();
  m_pending_ops++;
  m_client->add_aio_operation(oid, true, std::bind(
    &TestIoCtxImpl::execute_aio_operations, this, oid, &ops, pbl, snap_id,
    m_snapc, objver), c);
  return 0;
}

int TestIoCtxImpl::aio_watch(const std::string& o, AioCompletionImpl *c,
                             uint64_t *handle, librados::WatchCtx2 *watch_ctx) {
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

int TestIoCtxImpl::aio_unwatch(uint64_t handle, AioCompletionImpl *c) {
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

int TestIoCtxImpl::exec(const std::string& oid, TestClassHandler *handler,
                        const char *cls, const char *method,
                        bufferlist& inbl, bufferlist* outbl,
                        uint64_t snap_id, const SnapContext &snapc) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  cls_method_cxx_call_t call = handler->get_method(cls, method);
  if (call == NULL) {
    return -ENOSYS;
  }

  return (*call)(reinterpret_cast<cls_method_context_t>(
    handler->get_method_context(this, oid, snap_id, snapc).get()), &inbl,
    outbl);
}

int TestIoCtxImpl::list_watchers(const std::string& o,
                                 std::list<obj_watch_t> *out_watchers) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  return m_client->get_watch_notify()->list_watchers(m_pool_id, get_namespace(),
                                                     o, out_watchers);
}

int TestIoCtxImpl::notify(const std::string& o, bufferlist& bl,
                          uint64_t timeout_ms, bufferlist *pbl) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  return m_client->get_watch_notify()->notify(m_client, m_pool_id,
                                              get_namespace(), o, bl,
                                              timeout_ms, pbl);
}

void TestIoCtxImpl::notify_ack(const std::string& o, uint64_t notify_id,
                               uint64_t handle, bufferlist& bl) {
  m_client->get_watch_notify()->notify_ack(m_client, m_pool_id, get_namespace(),
                                           o, notify_id, handle,
                                           m_client->get_instance_id(), bl);
}

int TestIoCtxImpl::operate(const std::string& oid,
                           TestObjectOperationImpl &ops) {
  AioCompletionImpl *comp = new AioCompletionImpl();

  ops.get();
  m_pending_ops++;
  m_client->add_aio_operation(oid, false, std::bind(
    &TestIoCtxImpl::execute_aio_operations, this, oid, &ops,
    reinterpret_cast<bufferlist*>(0), m_snap_seq, m_snapc, nullptr), comp);

  comp->wait_for_complete();
  int ret = comp->get_return_value();
  comp->put();
  return ret;
}

int TestIoCtxImpl::operate_read(const std::string& oid,
                                TestObjectOperationImpl &ops,
                                bufferlist *pbl) {
  AioCompletionImpl *comp = new AioCompletionImpl();

  ops.get();
  m_pending_ops++;
  m_client->add_aio_operation(oid, false, std::bind(
    &TestIoCtxImpl::execute_aio_operations, this, oid, &ops, pbl,
    m_snap_seq, m_snapc, nullptr), comp);

  comp->wait_for_complete();
  int ret = comp->get_return_value();
  comp->put();
  return ret;
}

void TestIoCtxImpl::aio_selfmanaged_snap_create(uint64_t *snapid,
                                                AioCompletionImpl *c) {
  m_client->add_aio_operation(
    "", true,
    std::bind(&TestIoCtxImpl::selfmanaged_snap_create, this, snapid), c);
}

void TestIoCtxImpl::aio_selfmanaged_snap_remove(uint64_t snapid,
                                                AioCompletionImpl *c) {
  m_client->add_aio_operation(
    "", true,
    std::bind(&TestIoCtxImpl::selfmanaged_snap_remove, this, snapid), c);
}

int TestIoCtxImpl::selfmanaged_snap_set_write_ctx(snap_t seq,
                                                  std::vector<snap_t>& snaps) {
  std::vector<snapid_t> snap_ids(snaps.begin(), snaps.end());
  m_snapc = SnapContext(seq, snap_ids);
  return 0;
}

int TestIoCtxImpl::set_alloc_hint(const std::string& oid,
                                  uint64_t expected_object_size,
                                  uint64_t expected_write_size,
                                  uint32_t flags,
                                  const SnapContext &snapc) {
  return 0;
}

void TestIoCtxImpl::set_snap_read(snap_t seq) {
  if (seq == 0) {
    seq = CEPH_NOSNAP;
  }
  m_snap_seq = seq;
}

int TestIoCtxImpl::tmap_update(const std::string& oid, bufferlist& cmdbl) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  // TODO: protect against concurrent tmap updates
  bufferlist tmap_header;
  std::map<string,bufferlist> tmap;
  uint64_t size = 0;
  int r = stat(oid, &size, NULL);
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

int TestIoCtxImpl::unwatch(uint64_t handle) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  return m_client->get_watch_notify()->unwatch(m_client, handle);
}

int TestIoCtxImpl::watch(const std::string& o, uint64_t *handle,
                         librados::WatchCtx *ctx, librados::WatchCtx2 *ctx2) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  return m_client->get_watch_notify()->watch(m_client, m_pool_id,
                                             get_namespace(), o,
                                             get_instance_id(), handle, ctx,
                                             ctx2);
}

int TestIoCtxImpl::execute_operation(const std::string& oid,
                                     const Operation &operation) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  TestRadosClient::Transaction transaction(m_client, get_namespace(), oid);
  return operation(this, oid);
}

int TestIoCtxImpl::execute_aio_operations(const std::string& oid,
                                          TestObjectOperationImpl *ops,
                                          bufferlist *pbl, uint64_t snap_id,
                                          const SnapContext &snapc,
                                          uint64_t* objver) {
  int ret = 0;
  if (m_client->is_blocklisted()) {
    ret = -EBLOCKLISTED;
  } else {
    TestRadosClient::Transaction transaction(m_client, get_namespace(), oid);
    for (ObjectOperations::iterator it = ops->ops.begin();
         it != ops->ops.end(); ++it) {
      ret = (*it)(this, oid, pbl, snap_id, snapc, objver);
      if (ret < 0) {
        break;
      }
    }
  }
  m_pending_ops--;
  ops->put();
  return ret;
}

void TestIoCtxImpl::handle_aio_notify_complete(AioCompletionImpl *c, int r) {
  m_pending_ops--;

  m_client->finish_aio_completion(c, r);
}

} // namespace librados
