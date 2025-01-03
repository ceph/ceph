// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_TEST_STUB_MOCK_TEST_MEM_IO_CTX_IMPL_H
#define LIBRADOS_TEST_STUB_MOCK_TEST_MEM_IO_CTX_IMPL_H

#include "test/librados_test_stub/TestMemIoCtxImpl.h"
#include "test/librados_test_stub/TestMemCluster.h"
#include "gmock/gmock.h"

namespace librados {

class MockTestMemRadosClient;

class MockTestMemIoCtxImpl : public TestMemIoCtxImpl {
public:
  MockTestMemIoCtxImpl(MockTestMemRadosClient *mock_client,
                       TestMemRadosClient *client, int64_t pool_id,
                       const std::string& pool_name,
                       TestMemCluster::Pool *pool)
    : TestMemIoCtxImpl(client, pool_id, pool_name, pool),
      m_mock_client(mock_client), m_client(client) {
    default_to_parent();
  }

  MockTestMemRadosClient *get_mock_rados_client() {
    return m_mock_client;
  }

  MOCK_METHOD0(clone, TestIoCtxImpl*());
  TestIoCtxImpl *do_clone() {
    TestIoCtxImpl *io_ctx_impl = new ::testing::NiceMock<MockTestMemIoCtxImpl>(
      m_mock_client, m_client, get_pool_id(), get_pool_name(), get_pool());
    io_ctx_impl->set_snap_read(get_snap_read());
    io_ctx_impl->set_snap_context(get_snap_context());
    return io_ctx_impl;
  }

  MOCK_METHOD5(aio_notify, void(const std::string& o, AioCompletionImpl *c,
                                bufferlist& bl, uint64_t timeout_ms,
                                bufferlist *pbl));
  void do_aio_notify(const std::string& o, AioCompletionImpl *c, bufferlist& bl,
                     uint64_t timeout_ms, bufferlist *pbl) {
    return TestMemIoCtxImpl::aio_notify(o, c, bl, timeout_ms, pbl);
  }

  MOCK_METHOD6(aio_operate, int(const std::string&, TestObjectOperationImpl&,
                                AioCompletionImpl*, SnapContext*,
                                const ceph::real_time*, int));
  int do_aio_operate(const std::string& o, TestObjectOperationImpl& ops,
                     AioCompletionImpl* c, SnapContext* snapc,
                     const ceph::real_time* pmtime, int flags) {
    return TestMemIoCtxImpl::aio_operate(o, ops, c, snapc, pmtime, flags);
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

  MOCK_METHOD2(assert_exists, int(const std::string &, uint64_t));
  int do_assert_exists(const std::string &oid, uint64_t snap_id) {
    return TestMemIoCtxImpl::assert_exists(oid, snap_id);
  }

  MOCK_METHOD2(assert_version, int(const std::string &, uint64_t));
  int do_assert_version(const std::string &oid, uint64_t ver) {
    return TestMemIoCtxImpl::assert_version(oid, ver);
  }

  MOCK_METHOD3(create, int(const std::string&, bool, const SnapContext &));
  int do_create(const std::string& oid, bool exclusive,
                const SnapContext &snapc) {
    return TestMemIoCtxImpl::create(oid, exclusive, snapc);
  }

  MOCK_METHOD4(cmpext, int(const std::string&, uint64_t, bufferlist&,
                           uint64_t snap_id));
  int do_cmpext(const std::string& oid, uint64_t off, bufferlist& cmp_bl,
                uint64_t snap_id) {
    return TestMemIoCtxImpl::cmpext(oid, off, cmp_bl, snap_id);
  }

  MOCK_METHOD8(exec, int(const std::string& oid,
                         TestClassHandler *handler,
                         const char *cls,
                         const char *method,
                         bufferlist& inbl,
                         bufferlist* outbl,
                         uint64_t snap_id,
                         const SnapContext &snapc));
  int do_exec(const std::string& oid, TestClassHandler *handler,
              const char *cls, const char *method, bufferlist& inbl,
              bufferlist* outbl, uint64_t snap_id, const SnapContext &snapc) {
    return TestMemIoCtxImpl::exec(oid, handler, cls, method, inbl, outbl,
                                  snap_id, snapc);
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
  MOCK_METHOD6(sparse_read, int(const std::string& oid,
                               uint64_t off,
                               uint64_t len,
                               std::map<uint64_t, uint64_t> *m,
                               bufferlist *bl, uint64_t));
  int do_sparse_read(const std::string& oid, uint64_t off, size_t len,
                     std::map<uint64_t, uint64_t> *m, bufferlist *bl,
                     uint64_t snap_id) {
     return TestMemIoCtxImpl::sparse_read(oid, off, len, m, bl, snap_id);
  }

  MOCK_METHOD6(read, int(const std::string& oid,
                         size_t len,
                         uint64_t off,
                         bufferlist *bl, uint64_t snap_id, uint64_t* objver));
  int do_read(const std::string& oid, size_t len, uint64_t off,
              bufferlist *bl, uint64_t snap_id, uint64_t* objver) {
    return TestMemIoCtxImpl::read(oid, len, off, bl, snap_id, objver);
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


  MOCK_METHOD5(writesame, int(const std::string& oid, bufferlist& bl,
                              size_t len, uint64_t off,
                              const SnapContext &snapc));
  int do_writesame(const std::string& oid, bufferlist& bl, size_t len,
                   uint64_t off, const SnapContext &snapc) {
    return TestMemIoCtxImpl::writesame(oid, bl, len, off, snapc);
  }

  MOCK_METHOD4(zero, int(const std::string& oid, uint64_t offset,
                         uint64_t length, const SnapContext &snapc));
  int do_zero(const std::string& oid, uint64_t offset,
              uint64_t length, const SnapContext &snapc) {
    return TestMemIoCtxImpl::zero(oid, offset, length, snapc);
  }

  void default_to_parent() {
    using namespace ::testing;

    ON_CALL(*this, clone()).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_clone));
    ON_CALL(*this, aio_notify(_, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_aio_notify));
    ON_CALL(*this, aio_operate(_, _, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_aio_operate));
    ON_CALL(*this, aio_watch(_, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_aio_watch));
    ON_CALL(*this, aio_unwatch(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_aio_unwatch));
    ON_CALL(*this, assert_exists(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_assert_exists));
    ON_CALL(*this, assert_version(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_assert_version));
    ON_CALL(*this, create(_, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_create));
    ON_CALL(*this, cmpext(_, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_cmpext));
    ON_CALL(*this, exec(_, _, _, _, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_exec));
    ON_CALL(*this, get_instance_id()).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_get_instance_id));
    ON_CALL(*this, list_snaps(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_list_snaps));
    ON_CALL(*this, list_watchers(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_list_watchers));
    ON_CALL(*this, notify(_, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_notify));
    ON_CALL(*this, read(_, _, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_read));
    ON_CALL(*this, set_snap_read(_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_set_snap_read));
    ON_CALL(*this, sparse_read(_, _, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_sparse_read));
    ON_CALL(*this, remove(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_remove));
    ON_CALL(*this, selfmanaged_snap_create(_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_selfmanaged_snap_create));
    ON_CALL(*this, selfmanaged_snap_remove(_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_selfmanaged_snap_remove));
    ON_CALL(*this, selfmanaged_snap_rollback(_, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_selfmanaged_snap_rollback));
    ON_CALL(*this, truncate(_,_,_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_truncate));
    ON_CALL(*this, write(_, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_write));
    ON_CALL(*this, write_full(_, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_write_full));
    ON_CALL(*this, writesame(_, _, _, _, _)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_writesame));
    ON_CALL(*this, zero(_,_,_,_)).WillByDefault(Invoke(this, &MockTestMemIoCtxImpl::do_zero));
  }

private:
  MockTestMemRadosClient *m_mock_client;
  TestMemRadosClient *m_client;
};

} // namespace librados

#endif // LIBRADOS_TEST_STUB_MOCK_TEST_MEM_IO_CTX_IMPL_H
