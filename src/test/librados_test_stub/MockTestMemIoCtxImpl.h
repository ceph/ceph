// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_TEST_STUB_MOCK_TEST_MEM_IO_CTX_IMPL_H
#define LIBRADOS_TEST_STUB_MOCK_TEST_MEM_IO_CTX_IMPL_H

#include "test/librados_test_stub/TestMemIoCtxImpl.h"
#include "gmock/gmock.h"

namespace librados {

class MockTestMemRadosClient;

class MockTestMemIoCtxImpl : public TestMemIoCtxImpl {
public:
  MockTestMemIoCtxImpl(MockTestMemRadosClient *mock_client,
                       TestMemRadosClient *client, int64_t pool_id,
                       const std::string& pool_name,
                       TestMemRadosClient::Pool *pool)
    : TestMemIoCtxImpl(client, pool_id, pool_name, pool),
      m_mock_client(mock_client), m_client(client) {
    default_to_parent();
  }

  MockTestMemRadosClient *get_mock_rados_client() {
    return m_mock_client;
  }

  virtual TestIoCtxImpl *clone() {
    TestIoCtxImpl *io_ctx_impl = new ::testing::NiceMock<MockTestMemIoCtxImpl>(
      m_mock_client, m_client, get_pool_id(), get_pool_name(), get_pool());
    io_ctx_impl->set_snap_read(get_snap_read());
    io_ctx_impl->set_snap_context(get_snap_context());
    return io_ctx_impl;
  }

  MOCK_METHOD4(aio_watch, int(const std::string& o, AioCompletionImpl *c,
                              uint64_t *handle, librados::WatchCtx2 *ctx));
  int do_aio_watch(const std::string& o, AioCompletionImpl *c,
                   uint64_t *handle, librados::WatchCtx2 *ctx) {
    return TestMemIoCtxImpl::aio_watch(o, c, handle, ctx);
  }

  MOCK_METHOD2(aio_unwatch, int(uint64_t handle, AioCompletionImpl *c));
  int do_aio_unwatch(uint64_t handle, AioCompletionImpl *c) {
    return TestMemIoCtxImpl::aio_unwatch(handle, c);
  }

  MOCK_METHOD7(exec, int(const std::string& oid,
                         TestClassHandler *handler,
                         const char *cls,
                         const char *method,
                         bufferlist& inbl,
                         bufferlist* outbl,
                         const SnapContext &snapc));
  int do_exec(const std::string& oid, TestClassHandler *handler,
              const char *cls, const char *method, bufferlist& inbl,
              bufferlist* outbl, const SnapContext &snapc) {
    return TestMemIoCtxImpl::exec(oid, handler, cls, method, inbl, outbl,
                                  snapc);
  }

  MOCK_CONST_METHOD0(get_instance_id, uint64_t());
  uint64_t do_get_instance_id() const {
    return TestMemIoCtxImpl::get_instance_id();
  }

  MOCK_METHOD2(list_snaps, int(const std::string& o, snap_set_t *out_snaps));
  int do_list_snaps(const std::string& o, snap_set_t *out_snaps) {
    return TestMemIoCtxImpl::list_snaps(o, out_snaps);
  }

  MOCK_METHOD2(list_watchers, int(const std::string& o,
                                  std::list<obj_watch_t> *out_watchers));
  int do_list_watchers(const std::string& o,
                       std::list<obj_watch_t> *out_watchers) {
    return TestMemIoCtxImpl::list_watchers(o, out_watchers);
  }

  MOCK_METHOD4(notify, int(const std::string& o, bufferlist& bl,
                           uint64_t timeout_ms, bufferlist *pbl));
  int do_notify(const std::string& o, bufferlist& bl,
                uint64_t timeout_ms, bufferlist *pbl) {
    return TestMemIoCtxImpl::notify(o, bl, timeout_ms, pbl);
  }

  MOCK_METHOD1(set_snap_read, void(snap_t));
  void do_set_snap_read(snap_t snap_id) {
    return TestMemIoCtxImpl::set_snap_read(snap_id);
  }

  MOCK_METHOD4(read, int(const std::string& oid,
                         size_t len,
                         uint64_t off,
                         bufferlist *bl));
  int do_read(const std::string& oid, size_t len, uint64_t off,
              bufferlist *bl) {
    return TestMemIoCtxImpl::read(oid, len, off, bl);
  }

  MOCK_METHOD2(remove, int(const std::string& oid, const SnapContext &snapc));
  int do_remove(const std::string& oid, const SnapContext &snapc) {
    return TestMemIoCtxImpl::remove(oid, snapc);
  }

  MOCK_METHOD1(selfmanaged_snap_create, int(uint64_t *snap_id));
  int do_selfmanaged_snap_create(uint64_t *snap_id) {
    return TestMemIoCtxImpl::selfmanaged_snap_create(snap_id);
  }

  MOCK_METHOD1(selfmanaged_snap_remove, int(uint64_t snap_id));
  int do_selfmanaged_snap_remove(uint64_t snap_id) {
    return TestMemIoCtxImpl::selfmanaged_snap_remove(snap_id);
  }

  MOCK_METHOD2(selfmanaged_snap_rollback, int(const std::string& oid,
                                              uint64_t snap_id));
  int do_selfmanaged_snap_rollback(const std::string& oid, uint64_t snap_id) {
    return TestMemIoCtxImpl::selfmanaged_snap_rollback(oid, snap_id);
  }

  MOCK_METHOD3(truncate, int(const std::string& oid,
                             uint64_t size,
                             const SnapContext &snapc));
  int do_truncate(const std::string& oid, uint64_t size,
                  const SnapContext &snapc) {
    return TestMemIoCtxImpl::truncate(oid, size, snapc);
  }

  MOCK_METHOD5(write, int(const std::string& oid, bufferlist& bl, size_t len,
                          uint64_t off, const SnapContext &snapc));
  int do_write(const std::string& oid, bufferlist& bl, size_t len, uint64_t off,
                    const SnapContext &snapc) {
    return TestMemIoCtxImpl::write(oid, bl, len, off, snapc);
  }

  MOCK_METHOD3(write_full, int(const std::string& oid,
                               bufferlist& bl,
                               const SnapContext &snapc));
  int do_write_full(const std::string& oid, bufferlist& bl,
                    const SnapContext &snapc) {
    return TestMemIoCtxImpl::write_full(oid, bl, snapc);
  }

  void default_to_parent() {
    using namespace ::testing;

    ON_CALL(*this, aio_watch(_, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_aio_watch));
    ON_CALL(*this, aio_unwatch(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_aio_unwatch));
    ON_CALL(*this, exec(_, _, _, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_exec));
    ON_CALL(*this, get_instance_id()).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_get_instance_id));
    ON_CALL(*this, list_snaps(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_list_snaps));
    ON_CALL(*this, list_watchers(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_list_watchers));
    ON_CALL(*this, notify(_, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_notify));
    ON_CALL(*this, read(_, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_read));
    ON_CALL(*this, set_snap_read(_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_set_snap_read));
    ON_CALL(*this, remove(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_remove));
    ON_CALL(*this, selfmanaged_snap_create(_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_selfmanaged_snap_create));
    ON_CALL(*this, selfmanaged_snap_remove(_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_selfmanaged_snap_remove));
    ON_CALL(*this, selfmanaged_snap_rollback(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_selfmanaged_snap_rollback));
    ON_CALL(*this, truncate(_,_,_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_truncate));
    ON_CALL(*this, write(_, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_write));
    ON_CALL(*this, write_full(_, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_write_full));
  }

private:
  MockTestMemRadosClient *m_mock_client;
  TestMemRadosClient *m_client;
};

} // namespace librados

#endif // LIBRADOS_TEST_STUB_MOCK_TEST_MEM_IO_CTX_IMPL_H
