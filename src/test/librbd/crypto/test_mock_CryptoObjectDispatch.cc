// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librbd/mock/crypto/MockCryptoInterface.h"
#include "librbd/crypto/CryptoObjectDispatch.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/Utils.h"

#include "librbd/io/Utils.cc"
template bool librbd::io::util::trigger_copyup(
        MockImageCtx *image_ctx, uint64_t object_no, IOContext io_context,
        Context* on_finish);

template class librbd::io::ObjectWriteRequest<librbd::MockImageCtx>;
template class librbd::io::AbstractObjectWriteRequest<librbd::MockImageCtx>;
#include "librbd/io/ObjectRequest.cc"

namespace librbd {

namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util

namespace io {

template <>
struct CopyupRequest<librbd::MockImageCtx> {
    MOCK_METHOD0(send, void());
    MOCK_METHOD2(append_request, void(
            AbstractObjectWriteRequest<librbd::MockImageCtx>*,
            const Extents&));

    static CopyupRequest* s_instance;
    static CopyupRequest* create(librbd::MockImageCtx* ictx, uint64_t objectno,
                                 Extents&& image_extents, ImageArea area,
                                 const ZTracer::Trace& parent_trace) {
      return s_instance;
    }

    CopyupRequest() {
      s_instance = this;
    }
};

CopyupRequest<librbd::MockImageCtx>* CopyupRequest<
        librbd::MockImageCtx>::s_instance = nullptr;

namespace util {

namespace {

struct Mock {
    static Mock* s_instance;

    Mock() {
      s_instance = this;
    }

    MOCK_METHOD6(read_parent,
            void(MockImageCtx*, uint64_t, io::ReadExtents*,
                 librados::snap_t, const ZTracer::Trace &, Context*));
};

Mock *Mock::s_instance = nullptr;

} // anonymous namespace

template <> void read_parent(
        MockImageCtx *image_ctx, uint64_t object_no,
        io::ReadExtents* extents, librados::snap_t snap_id,
        const ZTracer::Trace &trace, Context* on_finish) {

  Mock::s_instance->read_parent(image_ctx, object_no, extents, snap_id, trace,
                                on_finish);
}

} // namespace util
} // namespace io

} // namespace librbd

#include "librbd/crypto/CryptoObjectDispatch.cc"

namespace librbd {
namespace crypto {

template <>
uint64_t get_file_offset(MockImageCtx *image_ctx, uint64_t object_no,
                         uint64_t offset) {
  return Striper::get_file_offset(image_ctx->cct, &image_ctx->layout,
                                  object_no, offset);
}

using ::testing::_;
using ::testing::ElementsAre;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Pair;
using ::testing::Return;
using ::testing::WithArg;

struct TestMockCryptoCryptoObjectDispatch : public TestMockFixture {
  typedef CryptoObjectDispatch<librbd::MockImageCtx> MockCryptoObjectDispatch;
  typedef io::AbstractObjectWriteRequest<librbd::MockImageCtx>
          MockAbstractObjectWriteRequest;
  typedef io::CopyupRequest<librbd::MockImageCtx> MockCopyupRequest;
  typedef io::util::Mock MockUtils;

  MockCryptoInterface crypto;
  MockImageCtx* mock_image_ctx;
  MockCryptoObjectDispatch* mock_crypto_object_dispatch;

  C_SaferCond finished_cond;
  Context *on_finish = &finished_cond;
  C_SaferCond dispatched_cond;
  Context *on_dispatched = &dispatched_cond;
  Context *dispatcher_ctx;
  ceph::bufferlist data;
  io::DispatchResult dispatch_result;
  io::Extents extent_map;
  int object_dispatch_flags = 0;
  MockUtils mock_utils;
  MockCopyupRequest copyup_request;

  void SetUp() override {
    TestMockFixture::SetUp();

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));
    mock_image_ctx = new MockImageCtx(*ictx);
    mock_crypto_object_dispatch = new MockCryptoObjectDispatch(
            mock_image_ctx, &crypto);
    data.append(std::string(4096, '1'));
  }

  void TearDown() override {
    C_SaferCond cond;
    Context *on_finish = &cond;
    mock_crypto_object_dispatch->shut_down(on_finish);
    ASSERT_EQ(0, cond.wait());

    delete mock_crypto_object_dispatch;
    delete mock_image_ctx;

    TestMockFixture::TearDown();
  }

  void expect_object_read(io::ReadExtents* extents, uint64_t version = 0) {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, send(_))
            .WillOnce(Invoke([this, extents,
                              version](io::ObjectDispatchSpec* spec) {
                auto* read = boost::get<io::ObjectDispatchSpec::ReadRequest>(
                        &spec->request);
                ASSERT_TRUE(read != nullptr);

                ASSERT_EQ(extents->size(), read->extents->size());
                for (uint64_t i = 0; i < extents->size(); ++i) {
                  ASSERT_EQ((*extents)[i].offset, (*read->extents)[i].offset);
                  ASSERT_EQ((*extents)[i].length, (*read->extents)[i].length);
                  (*read->extents)[i].bl = (*extents)[i].bl;
                  (*read->extents)[i].extent_map = (*extents)[i].extent_map;
                }

                if (read->version != nullptr) {
                  *(read->version) = version;
                }

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                dispatcher_ctx = &spec->dispatcher_ctx;
            }));
  }

  void expect_read_parent(MockUtils &mock_utils, uint64_t object_no,
                          io::ReadExtents* extents, librados::snap_t snap_id,
                          int r) {
    EXPECT_CALL(mock_utils,
                read_parent(_, object_no, extents, snap_id, _, _))
            .WillOnce(WithArg<5>(CompleteContext(
                    r, static_cast<asio::ContextWQ*>(nullptr))));
  }

  void expect_object_write(uint64_t object_off, const std::string& data,
                           int write_flags,
                           std::optional<uint64_t> assert_version) {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, send(_))
            .WillOnce(Invoke([this, object_off, data, write_flags,
                              assert_version](io::ObjectDispatchSpec* spec) {
                auto* write = boost::get<io::ObjectDispatchSpec::WriteRequest>(
                        &spec->request);
                ASSERT_TRUE(write != nullptr);

                ASSERT_EQ(object_off, write->object_off);
                ASSERT_TRUE(data == write->data.to_str());
                ASSERT_EQ(write_flags, write->write_flags);
                ASSERT_EQ(assert_version, write->assert_version);

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                dispatcher_ctx = &spec->dispatcher_ctx;
            }));
  }

  void expect_object_write_same() {
    EXPECT_CALL(*mock_image_ctx->io_object_dispatcher, send(_))
            .WillOnce(Invoke([this](io::ObjectDispatchSpec* spec) {
                auto* write_same = boost::get<
                        io::ObjectDispatchSpec::WriteSameRequest>(
                                &spec->request);
                ASSERT_TRUE(write_same != nullptr);

                spec->dispatch_result = io::DISPATCH_RESULT_COMPLETE;
                dispatcher_ctx = &spec->dispatcher_ctx;
            }));
  }

  void expect_get_object_size() {
    EXPECT_CALL(*mock_image_ctx, get_object_size()).WillOnce(Return(
            mock_image_ctx->layout.object_size));
  }

  void expect_get_parent_overlap(uint64_t overlap) {
    EXPECT_CALL(*mock_image_ctx, get_parent_overlap(_, _))
            .WillOnce(WithArg<1>(Invoke([overlap](uint64_t *o) {
                *o = overlap;
                return 0;
            })));
  }

  void expect_prune_parent_extents(uint64_t object_overlap) {
    EXPECT_CALL(*mock_image_ctx, prune_parent_extents(_, _, _, _))
            .WillOnce(Return(object_overlap));
  }

  void expect_copyup(MockAbstractObjectWriteRequest** write_request, int r) {
    EXPECT_CALL(copyup_request, append_request(_, _))
            .WillOnce(WithArg<0>(
                    Invoke([write_request](
                            MockAbstractObjectWriteRequest *req) {
                        *write_request = req;
                    })));
    EXPECT_CALL(copyup_request, send())
            .WillOnce(Invoke([write_request, r]() {
                (*write_request)->handle_copyup(r);
            }));
  }

  void expect_encrypt(int count = 1) {
    EXPECT_CALL(crypto, encrypt(_, _)).Times(count);
  }

  void expect_decrypt(int count = 1) {
    EXPECT_CALL(crypto, decrypt(_, _)).Times(count);
  }
};

TEST_F(TestMockCryptoCryptoObjectDispatch, Flush) {
  ASSERT_FALSE(mock_crypto_object_dispatch->flush(
          io::FLUSH_SOURCE_USER, {}, nullptr, nullptr, &on_finish, nullptr));
  ASSERT_EQ(on_finish, &finished_cond); // not modified
  on_finish->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, Discard) {
  expect_object_write_same();
  ASSERT_TRUE(mock_crypto_object_dispatch->discard(
          11, 0, 4096, mock_image_ctx->get_data_io_context(), 0, {},
          &object_dispatch_flags, nullptr, &dispatch_result, &on_finish,
          on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);

  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0);
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, AlignedReadFail) {
  io::ReadExtents extents = {{0, 4096}};
  expect_object_read(&extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->read(
      11, &extents, mock_image_ctx->get_data_io_context(), 0, 0, {},
      nullptr, &object_dispatch_flags, &dispatch_result,
      &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(-EIO);
  ASSERT_EQ(-EIO, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, AlignedRead) {
  io::ReadExtents extents = {{0, 16384}, {32768, 4096}};
  extents[0].bl.append(std::string(1024, '1') + std::string(1024, '2') +
                       std::string(1024, '3') + std::string(1024, '4'));
  extents[0].extent_map = {{1024, 1024}, {3072, 2048}, {16384 - 1024, 1024}};
  extents[1].bl.append(std::string(4096, '0'));
  expect_object_read(&extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->read(
          11, &extents, mock_image_ctx->get_data_io_context(), 0, 0, {},
          nullptr, &object_dispatch_flags, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  expect_decrypt(3);
  dispatcher_ctx->complete(0);
  ASSERT_EQ(16384 + 4096, dispatched_cond.wait());

  auto expected_bl_data = (
          std::string(1024, '\0') + std::string(1024, '1') +
          std::string(1024, '\0') + std::string(1024, '2') +
          std::string(1024, '3') + std::string(3072, '\0') +
          std::string(3072, '\0') + std::string(1024, '4'));
  ASSERT_TRUE(extents[0].bl.to_str() == expected_bl_data);
  ASSERT_THAT(extents[0].extent_map,
              ElementsAre(Pair(0, 8192), Pair(16384 - 4096, 4096)));
}

TEST_F(TestMockCryptoCryptoObjectDispatch, ReadFromParent) {
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  expect_object_read(&extents);
  expect_read_parent(mock_utils, 11, &extents, CEPH_NOSNAP, 8192);
  ASSERT_TRUE(mock_crypto_object_dispatch->read(
          11, &extents, mock_image_ctx->get_data_io_context(), 0, 0, {},
          nullptr, &object_dispatch_flags, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  // no decrypt
  dispatcher_ctx->complete(-ENOENT);
  ASSERT_EQ(8192, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, ReadFromParentDisabled) {
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  expect_object_read(&extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->read(
          11, &extents, mock_image_ctx->get_data_io_context(), 0,
          io::READ_FLAG_DISABLE_READ_FROM_PARENT, {},
          nullptr, &object_dispatch_flags, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  // no decrypt
  dispatcher_ctx->complete(-ENOENT);
  ASSERT_EQ(-ENOENT, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedRead) {
  io::ReadExtents extents = {{0, 1}, {8191, 1}, {8193, 1},
                             {16384 + 1, 4096 * 5 - 2}};
  io::ReadExtents aligned_extents = {{0, 4096}, {4096, 4096}, {8192, 4096},
                                     {16384, 4096 * 5}};
  aligned_extents[0].bl.append(std::string("1") + std::string(4096, '0'));
  aligned_extents[1].bl.append(std::string(4095, '0') + std::string("2"));
  aligned_extents[2].bl.append(std::string("03") + std::string(4094, '0'));
  aligned_extents[3].bl.append(std::string("0") + std::string(4095, '4') +
                               std::string(4096, '5') +
                               std::string(4095, '6') + std::string("0"));
  aligned_extents[3].extent_map = {{16384, 4096}, {16384 + 2 * 4096, 4096},
                                   {16384 + 4 * 4096, 4096}};

  expect_object_read(&aligned_extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->read(
          11, &extents, mock_image_ctx->get_data_io_context(), 0, 0, {},
          nullptr, &object_dispatch_flags, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  dispatcher_ctx->complete(4096*8);
  ASSERT_EQ(3 + 4096 * 5 - 2, dispatched_cond.wait());
  ASSERT_TRUE(extents[0].bl.to_str() == std::string("1"));
  ASSERT_TRUE(extents[1].bl.to_str() == std::string("2"));
  ASSERT_TRUE(extents[2].bl.to_str() == std::string("3"));

  auto expected_bl_data = (std::string(4095, '4') + std::string(4096, '5') +
                           std::string(4095, '6'));
  ASSERT_TRUE(extents[3].bl.to_str() == expected_bl_data);
  ASSERT_THAT(extents[3].extent_map,
              ElementsAre(Pair(16384 + 1, 4095), Pair(16384 + 2 * 4096, 4096),
                          Pair(16384 + 4 * 4096, 4095)));
}

TEST_F(TestMockCryptoCryptoObjectDispatch, AlignedWrite) {
  expect_encrypt();
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
        11, 0, std::move(data), mock_image_ctx->get_data_io_context(), 0, 0,
        std::nullopt, {}, nullptr, nullptr, &dispatch_result, &on_finish,
        on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_CONTINUE);
  ASSERT_EQ(on_finish, &finished_cond); // not modified
  on_finish->complete(0);
  ASSERT_EQ(0, finished_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedWrite) {
  ceph::bufferlist write_data;
  uint64_t version = 1234;
  write_data.append(std::string(8192, '1'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  expect_object_read(&extents, version);
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
          11, 1, std::move(write_data), mock_image_ctx->get_data_io_context(),
          0, 0, std::nullopt, {}, nullptr, nullptr, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  auto expected_data =
          std::string("2") + std::string(8192, '1') + std::string(4095, '3');
  expect_object_write(0, expected_data, 0, std::make_optional(version));
  dispatcher_ctx->complete(8192); // complete read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0); // complete write
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedWriteWithNoObject) {
  ceph::bufferlist write_data;
  write_data.append(std::string(8192, '1'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  expect_object_read(&extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
          11, 1, std::move(write_data), mock_image_ctx->get_data_io_context(),
          0, 0, std::nullopt, {}, nullptr, nullptr, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  expect_get_object_size();
  expect_get_parent_overlap(0);
  auto expected_data = (std::string(1, '\0') + std::string(8192, '1') +
                        std::string(4095, '\0'));
  expect_object_write(0, expected_data, io::OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE,
                      std::nullopt);
  dispatcher_ctx->complete(-ENOENT); // complete read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0); // complete write
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedWriteFailCreate) {
  ceph::bufferlist write_data;
  write_data.append(std::string(8192, '1'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  expect_object_read(&extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
          11, 1, std::move(write_data), mock_image_ctx->get_data_io_context(),
          0, 0, std::nullopt, {}, nullptr, nullptr, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  expect_get_object_size();
  expect_get_parent_overlap(0);
  auto expected_data = (std::string(1, '\0') + std::string(8192, '1') +
                        std::string(4095, '\0'));
  expect_object_write(0, expected_data, io::OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE,
          std::nullopt);
  dispatcher_ctx->complete(-ENOENT); // complete read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  uint64_t version = 1234;
  expect_object_read(&extents, version);
  dispatcher_ctx->complete(-EEXIST); // complete write, request will restart
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  auto expected_data2 =
        std::string("2") + std::string(8192, '1') + std::string(4095, '3');
  expect_object_write(0, expected_data2, 0, std::make_optional(version));
  dispatcher_ctx->complete(8192); // complete read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0); // complete write
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedWriteCopyup) {
  MockObjectMap mock_object_map;
  mock_image_ctx->object_map = &mock_object_map;
  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx->exclusive_lock = &mock_exclusive_lock;

  ceph::bufferlist write_data;
  write_data.append(std::string(8192, '1'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  expect_object_read(&extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
          11, 1, std::move(write_data), mock_image_ctx->get_data_io_context(),
          0, 0, std::nullopt, {}, nullptr, nullptr, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  expect_get_object_size();
  expect_get_parent_overlap(100 << 20);
  expect_prune_parent_extents(mock_image_ctx->layout.object_size);
  EXPECT_CALL(mock_exclusive_lock, is_lock_owner()).WillRepeatedly(
          Return(true));
  EXPECT_CALL(*mock_image_ctx->object_map, object_may_exist(11)).WillOnce(
          Return(false));
  MockAbstractObjectWriteRequest *write_request = nullptr;
  expect_copyup(&write_request, 0);

  // unaligned write restarted
  uint64_t version = 1234;
  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  expect_object_read(&extents, version);
  dispatcher_ctx->complete(-ENOENT); // complete first read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  auto expected_data =
        std::string("2") + std::string(8192, '1') + std::string(4095, '3');
  expect_object_write(0, expected_data, 0, std::make_optional(version));
  dispatcher_ctx->complete(8192); // complete second read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0); // complete write
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedWriteEmptyCopyup) {
  MockObjectMap mock_object_map;
  mock_image_ctx->object_map = &mock_object_map;
  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx->exclusive_lock = &mock_exclusive_lock;

  ceph::bufferlist write_data;
  write_data.append(std::string(8192, '1'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  expect_object_read(&extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
          11, 1, std::move(write_data), mock_image_ctx->get_data_io_context(),
          0, 0, std::nullopt, {}, nullptr, nullptr, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  expect_get_object_size();
  expect_get_parent_overlap(100 << 20);
  expect_prune_parent_extents(mock_image_ctx->layout.object_size);
  EXPECT_CALL(mock_exclusive_lock, is_lock_owner()).WillRepeatedly(
          Return(true));
  EXPECT_CALL(*mock_image_ctx->object_map, object_may_exist(11)).WillOnce(
          Return(false));
  MockAbstractObjectWriteRequest *write_request = nullptr;
  expect_copyup(&write_request, 0);

  // unaligned write restarted
  expect_object_read(&extents);
  dispatcher_ctx->complete(-ENOENT); // complete first read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  auto expected_data =
        std::string(1, '\0') + std::string(8192, '1') +
        std::string(4095, '\0');
  expect_object_write(0, expected_data, io::OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE,
                      std::nullopt);
  dispatcher_ctx->complete(-ENOENT); // complete second read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0); // complete write
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedWriteFailVersionCheck) {
  ceph::bufferlist write_data;
  uint64_t version = 1234;
  write_data.append(std::string(8192, '1'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  expect_object_read(&extents, version);
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
          11, 1, std::move(write_data), mock_image_ctx->get_data_io_context(),
          0, 0, std::nullopt, {}, nullptr, nullptr, &dispatch_result,
          &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  auto expected_data =
          std::string("2") + std::string(8192, '1') + std::string(4095, '3');
  expect_object_write(0, expected_data, 0, std::make_optional(version));
  dispatcher_ctx->complete(8192); // complete read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  version = 1235;
  expect_object_read(&extents, version);
  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  dispatcher_ctx->complete(-ERANGE); // complete write, request will restart
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));

  expect_object_write(0, expected_data, 0, std::make_optional(version));
  dispatcher_ctx->complete(8192); // complete read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0); // complete write
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedWriteWithAssertVersion) {
  ceph::bufferlist write_data;
  uint64_t version = 1234;
  uint64_t assert_version = 1233;
  write_data.append(std::string(8192, '1'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  expect_object_read(&extents, version);
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
          11, 1, std::move(write_data), mock_image_ctx->get_data_io_context(),
          0, 0, std::make_optional(assert_version), {}, nullptr, nullptr,
          &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  dispatcher_ctx->complete(8192); // complete read
  ASSERT_EQ(-ERANGE, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, UnalignedWriteWithExclusiveCreate) {
  ceph::bufferlist write_data;
  write_data.append(std::string(8192, '1'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}};
  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  expect_object_read(&extents);
  ASSERT_TRUE(mock_crypto_object_dispatch->write(
          11, 1, std::move(write_data), mock_image_ctx->get_data_io_context(),
          0, io::OBJECT_WRITE_FLAG_CREATE_EXCLUSIVE, std::nullopt, {}, nullptr,
          nullptr, &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  dispatcher_ctx->complete(8192); // complete read
  ASSERT_EQ(-EEXIST, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, CompareAndWrite) {
  ceph::bufferlist write_data;
  uint64_t version = 1234;
  write_data.append(std::string(8192, '1'));
  ceph::bufferlist cmp_data;
  cmp_data.append(std::string(4096, '2'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}, {0, 8192}};
  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  extents[2].bl.append(std::string(8192, '2'));
  expect_object_read(&extents, version);

  ASSERT_TRUE(mock_crypto_object_dispatch->compare_and_write(
          11, 1, std::move(cmp_data), std::move(write_data),
          mock_image_ctx->get_data_io_context(), 0, {}, nullptr, nullptr,
          nullptr, &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  auto expected_data =
          std::string("2") + std::string(8192, '1') + std::string(4095, '3');
  expect_object_write(0, expected_data, 0, std::make_optional(version));
  dispatcher_ctx->complete(4096*4); // complete read
  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0); // complete write
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, CompareAndWriteFail) {
  ceph::bufferlist write_data;
  uint64_t version = 1234;
  write_data.append(std::string(8192, '1'));
  ceph::bufferlist cmp_data;
  cmp_data.append(std::string(4094, '2') + std::string(2, '4'));
  io::ReadExtents extents = {{0, 4096}, {8192, 4096}, {0, 8192}};
  extents[0].bl.append(std::string(4096, '2'));
  extents[1].bl.append(std::string(4096, '3'));
  extents[2].bl.append(std::string(8192, '2'));
  expect_object_read(&extents, version);

  uint64_t mismatch_offset;
  ASSERT_TRUE(mock_crypto_object_dispatch->compare_and_write(
          11, 1, std::move(cmp_data), std::move(write_data),
          mock_image_ctx->get_data_io_context(), 0, {}, &mismatch_offset,
          nullptr, nullptr, &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);
  ASSERT_EQ(on_finish, &finished_cond);

  dispatcher_ctx->complete(4096*4); // complete read
  ASSERT_EQ(-EILSEQ, dispatched_cond.wait());
  ASSERT_EQ(mismatch_offset, 4094);
}

TEST_F(TestMockCryptoCryptoObjectDispatch, WriteSame) {
  io::LightweightBufferExtents buffer_extents;
  ceph::bufferlist write_data;
  write_data.append(std::string("12"));
  expect_object_write(0, std::string("12121") , 0, std::nullopt);
  ASSERT_TRUE(mock_crypto_object_dispatch->write_same(
          11, 0, 5, {{0, 5}}, std::move(write_data),
          mock_image_ctx->get_data_io_context(), 0, {}, nullptr, nullptr,
          &dispatch_result, &on_finish, on_dispatched));
  ASSERT_EQ(dispatch_result, io::DISPATCH_RESULT_COMPLETE);

  ASSERT_EQ(ETIMEDOUT, dispatched_cond.wait_for(0));
  dispatcher_ctx->complete(0);
  ASSERT_EQ(0, dispatched_cond.wait());
}

TEST_F(TestMockCryptoCryptoObjectDispatch, PrepareCopyup) {
  char* data = (char*)"0123456789";
  io::SnapshotSparseBufferlist snapshot_sparse_bufferlist;
  auto& snap1 = snapshot_sparse_bufferlist[0];
  auto& snap2 = snapshot_sparse_bufferlist[1];

  snap1.insert(0, 1, {io::SPARSE_EXTENT_STATE_DATA, 1,
                      ceph::bufferlist::static_from_mem(data + 1, 1)});
  snap1.insert(8191, 1, {io::SPARSE_EXTENT_STATE_DATA, 1,
                         ceph::bufferlist::static_from_mem(data + 2, 1)});
  snap1.insert(8193, 3, {io::SPARSE_EXTENT_STATE_DATA, 3,
                         ceph::bufferlist::static_from_mem(data + 3, 3)});

  snap2.insert(0, 2, {io::SPARSE_EXTENT_STATE_ZEROED, 2});
  snap2.insert(8191, 3, {io::SPARSE_EXTENT_STATE_DATA, 3,
                         ceph::bufferlist::static_from_mem(data + 6, 3)});
  snap2.insert(16384, 1, {io::SPARSE_EXTENT_STATE_DATA, 1,
                          ceph::bufferlist::static_from_mem(data + 9, 1)});

  expect_get_object_size();
  expect_encrypt(6);
  InSequence seq;
  ASSERT_EQ(0, mock_crypto_object_dispatch->prepare_copyup(
      11, &snapshot_sparse_bufferlist));

  ASSERT_EQ(2, snapshot_sparse_bufferlist.size());

  auto& snap1_result = snapshot_sparse_bufferlist[0];
  auto& snap2_result = snapshot_sparse_bufferlist[1];

  auto it = snap1_result.begin();
  ASSERT_NE(it, snap1_result.end());
  ASSERT_EQ(0, it.get_off());
  ASSERT_EQ(4096 * 3, it.get_len());

  ASSERT_TRUE(it.get_val().bl.to_str() ==
    std::string("1") + std::string(4095, '\0') +
    std::string(4095, '\0') + std::string("2") +
    std::string(1, '\0') + std::string("345") + std::string(4092, '\0'));
  ASSERT_EQ(++it, snap1_result.end());

  it = snap2_result.begin();
  ASSERT_NE(it, snap2_result.end());
  ASSERT_EQ(0, it.get_off());
  ASSERT_EQ(4096 * 3, it.get_len());
  ASSERT_TRUE(it.get_val().bl.to_str() ==
    std::string(4096, '\0') +
    std::string(4095, '\0') + std::string("6") +
    std::string("7845") + std::string(4092, '\0'));

  ASSERT_NE(++it, snap2_result.end());
  ASSERT_EQ(16384, it.get_off());
  ASSERT_EQ(4096, it.get_len());
  ASSERT_TRUE(it.get_val().bl.to_str() ==
    std::string("9") + std::string(4095, '\0'));
  ASSERT_EQ(++it, snap2_result.end());
}

} // namespace crypto
} // namespace librbd
