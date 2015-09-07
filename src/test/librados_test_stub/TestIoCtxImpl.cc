// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestIoCtxImpl.h"
#include "test/librados_test_stub/TestClassHandler.h"
#include "test/librados_test_stub/TestRadosClient.h"
#include "test/librados_test_stub/TestWatchNotify.h"
#include "librados/AioCompletionImpl.h"
#include "include/assert.h"
#include "common/valgrind.h"
#include "objclass/objclass.h"
#include <boost/bind.hpp>
#include <errno.h>

namespace librados {

TestIoCtxImpl::TestIoCtxImpl() : m_client(NULL) {
  get();
}

TestIoCtxImpl::TestIoCtxImpl(TestRadosClient &client, int64_t pool_id,
                             const std::string& pool_name)
  : m_client(&client), m_pool_id(pool_id), m_pool_name(pool_name),
    m_snap_seq(CEPH_NOSNAP)
{
  m_client->get();
  get();
}

TestIoCtxImpl::TestIoCtxImpl(const TestIoCtxImpl& rhs)
  : m_client(rhs.m_client),
    m_pool_id(rhs.m_pool_id),
    m_pool_name(rhs.m_pool_name),
    m_snap_seq(rhs.m_snap_seq)
{
  m_client->get();
  get();
}

TestIoCtxImpl::~TestIoCtxImpl() {
  assert(m_pending_ops.read() == 0);
}

void TestObjectOperationImpl::get() {
  m_refcount.inc();
}

void TestObjectOperationImpl::put() {
  if (m_refcount.dec() == 0) {
    ANNOTATE_HAPPENS_AFTER(&m_refcount);
    ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_refcount);
    delete this;
  } else {
    ANNOTATE_HAPPENS_BEFORE(&m_refcount);
  }
}

void TestIoCtxImpl::get() {
  m_refcount.inc();
}

void TestIoCtxImpl::put() {
  if (m_refcount.dec() == 0) {
    m_client->put();
    delete this;
  }
}

uint64_t TestIoCtxImpl::get_instance_id() const {
  return 0;
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
  m_pending_ops.inc();
  c->get();
  C_AioNotify *ctx = new C_AioNotify(this, c);
  m_client->get_watch_notify().aio_notify(oid, bl, timeout_ms, pbl, ctx);
}

int TestIoCtxImpl::aio_operate(const std::string& oid, TestObjectOperationImpl &ops,
                               AioCompletionImpl *c, SnapContext *snap_context,
                               int flags) {
  // TODO flags for now
  ops.get();
  m_pending_ops.inc();
  m_client->add_aio_operation(oid, true, boost::bind(
    &TestIoCtxImpl::execute_aio_operations, this, oid, &ops,
    reinterpret_cast<bufferlist*>(NULL),
    snap_context != NULL ? *snap_context : m_snapc), c);
  return 0;
}

int TestIoCtxImpl::aio_operate_read(const std::string& oid,
                                    TestObjectOperationImpl &ops,
                                    AioCompletionImpl *c, int flags,
                                    bufferlist *pbl) {
  // TODO ignoring flags for now
  ops.get();
  m_pending_ops.inc();
  m_client->add_aio_operation(oid, true, boost::bind(
    &TestIoCtxImpl::execute_aio_operations, this, oid, &ops, pbl, m_snapc), c);
  return 0;
}

int TestIoCtxImpl::exec(const std::string& oid, TestClassHandler &handler,
                        const char *cls, const char *method,
                        bufferlist& inbl, bufferlist* outbl,
                        const SnapContext &snapc) {
  cls_method_cxx_call_t call = handler.get_method(cls, method);
  if (call == NULL) {
    return -ENOSYS;
  }

  return (*call)(reinterpret_cast<cls_method_context_t>(
    handler.get_method_context(this, oid, snapc).get()), &inbl, outbl);
}

int TestIoCtxImpl::list_watchers(const std::string& o,
                                 std::list<obj_watch_t> *out_watchers) {
  return m_client->get_watch_notify().list_watchers(o, out_watchers);
}

int TestIoCtxImpl::notify(const std::string& o, bufferlist& bl,
                          uint64_t timeout_ms, bufferlist *pbl) {
  return m_client->get_watch_notify().notify(o, bl, timeout_ms, pbl);
}

void TestIoCtxImpl::notify_ack(const std::string& o, uint64_t notify_id,
                               uint64_t handle, bufferlist& bl) {
  m_client->get_watch_notify().notify_ack(o, notify_id, handle,
                                          m_client->get_instance_id(), bl);
}

int TestIoCtxImpl::operate(const std::string& oid, TestObjectOperationImpl &ops) {
  AioCompletionImpl *comp = new AioCompletionImpl();

  ops.get();
  m_pending_ops.inc();
  m_client->add_aio_operation(oid, false, boost::bind(
    &TestIoCtxImpl::execute_aio_operations, this, oid, &ops,
    reinterpret_cast<bufferlist*>(NULL), m_snapc), comp);

  comp->wait_for_safe();
  int ret = comp->get_return_value();
  comp->put();
  return ret;
}

int TestIoCtxImpl::operate_read(const std::string& oid, TestObjectOperationImpl &ops,
                                bufferlist *pbl) {
  AioCompletionImpl *comp = new AioCompletionImpl();

  ops.get();
  m_pending_ops.inc();
  m_client->add_aio_operation(oid, false, boost::bind(
    &TestIoCtxImpl::execute_aio_operations, this, oid, &ops, pbl,
    m_snapc), comp);

  comp->wait_for_complete();
  int ret = comp->get_return_value();
  comp->put();
  return ret;
}

int TestIoCtxImpl::selfmanaged_snap_set_write_ctx(snap_t seq,
                                                  std::vector<snap_t>& snaps) {
  std::vector<snapid_t> snap_ids(snaps.begin(), snaps.end());
  m_snapc = SnapContext(seq, snap_ids);
  return 0;
}

int TestIoCtxImpl::set_alloc_hint(const std::string& oid,
                                  uint64_t expected_object_size,
                                  uint64_t expected_write_size) {
  return 0;
}

void TestIoCtxImpl::set_snap_read(snap_t seq) {
  if (seq == 0) {
    seq = CEPH_NOSNAP;
  }
  m_snap_seq = seq;
}

int TestIoCtxImpl::tmap_update(const std::string& oid, bufferlist& cmdbl) {
  // TODO: protect against concurrent tmap updates
  bufferlist tmap_header;
  std::map<string,bufferlist> tmap;
  uint64_t size = 0;
  int r = stat(oid, &size, NULL);
  if (r == -ENOENT) {
    r = create(oid, false);
  }
  if (r < 0) {
    return r;
  }

  if (size > 0) {
    bufferlist inbl;
    r = read(oid, size, 0, &inbl);
    if (r < 0) {
      return r;
    }
    bufferlist::iterator iter = inbl.begin();
    ::decode(tmap_header, iter);
    ::decode(tmap, iter);
  }

  __u8 c;
  std::string key;
  bufferlist value;
  bufferlist::iterator iter = cmdbl.begin();
  ::decode(c, iter);
  ::decode(key, iter);

  switch (c) {
    case CEPH_OSD_TMAP_SET:
      ::decode(value, iter);
      tmap[key] = value;
      break;
    case CEPH_OSD_TMAP_RM:
      tmap.erase(key);
      break;
    default:
      return -EINVAL;
  }

  bufferlist out;
  ::encode(tmap_header, out);
  ::encode(tmap, out);
  r = write_full(oid, out, m_snapc);
  return r;
}

int TestIoCtxImpl::unwatch(uint64_t handle) {
  return m_client->get_watch_notify().unwatch(handle);
}

int TestIoCtxImpl::watch(const std::string& o, uint64_t *handle,
                         librados::WatchCtx *ctx, librados::WatchCtx2 *ctx2) {
  return m_client->get_watch_notify().watch(o, get_instance_id(), handle, ctx,
                                            ctx2);
}

int TestIoCtxImpl::execute_aio_operations(const std::string& oid,
                                          TestObjectOperationImpl *ops,
                                          bufferlist *pbl,
                                          const SnapContext &snapc) {
  int ret = 0;
  for (ObjectOperations::iterator it = ops->ops.begin();
       it != ops->ops.end(); ++it) {
    ret = (*it)(this, oid, pbl, snapc);
    if (ret < 0) {
      break;
    }
  }
  m_pending_ops.dec();
  ops->put();
  return ret;
}

void TestIoCtxImpl::handle_aio_notify_complete(AioCompletionImpl *c, int r) {
  m_pending_ops.dec();

  m_client->finish_aio_completion(c, r);
}

} // namespace librados
