// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "librbd/migration/NBDStream.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "json_spirit/json_spirit.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

#include "librbd/migration/NBDStream.cc"

namespace librbd {
namespace migration {

template <>
struct NBDClient<MockTestImageCtx> {
  static NBDClient* s_instance;
  static NBDClient* create() {
    ceph_assert(s_instance != nullptr);
    return s_instance;
  }

  NBDClient() {
    s_instance = this;
  }

  MOCK_METHOD0(get_error, const char*());
  MOCK_METHOD0(get_errno, int());
  MOCK_METHOD0(init, int());
  MOCK_METHOD1(add_meta_context, int(const char*));
  MOCK_METHOD1(connect_uri, int(const char*));
  MOCK_METHOD0(get_size, int64_t());
  MOCK_METHOD4(pread, int(void*, size_t, uint64_t, uint32_t));
  MOCK_METHOD4(block_status, int(uint64_t, uint64_t, nbd_extent_callback,
                                 uint32_t));
  MOCK_METHOD1(shutdown, int(uint32_t));
};

NBDClient<MockTestImageCtx>* NBDClient<MockTestImageCtx>::s_instance = nullptr;

using ::testing::_;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::WithArg;

class TestMockMigrationNBDStream : public TestMockFixture {
public:
  typedef NBDStream<MockTestImageCtx> MockNBDStream;
  typedef NBDClient<MockTestImageCtx> MockNBDClient;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));
    m_json_object["uri"] = "nbd://foo.example";
  }

  void expect_get_errno(MockNBDClient& mock_nbd_client, int err) {
    EXPECT_CALL(mock_nbd_client, get_errno()).WillOnce(Return(err));
    EXPECT_CALL(mock_nbd_client, get_error()).WillOnce(Return("error message"));
  }

  void expect_init(MockNBDClient& mock_nbd_client, int rc) {
    EXPECT_CALL(mock_nbd_client, init()).WillOnce(Return(rc));
  }

  void expect_add_meta_context(MockNBDClient& mock_nbd_client, int rc) {
    EXPECT_CALL(mock_nbd_client, add_meta_context(_)).WillOnce(Return(rc));
  }

  void expect_connect_uri(MockNBDClient& mock_nbd_client, int rc) {
    EXPECT_CALL(mock_nbd_client, connect_uri(_)).WillOnce(Return(rc));
  }

  void expect_get_size(MockNBDClient& mock_nbd_client, int64_t rc) {
    EXPECT_CALL(mock_nbd_client, get_size()).WillOnce(Return(rc));
  }

  void expect_pread(MockNBDClient& mock_nbd_client, uint64_t byte_offset,
                    uint64_t byte_length, const void* buf, int rc) {
    EXPECT_CALL(mock_nbd_client, pread(_, byte_length, byte_offset, _))
      .WillOnce(WithArg<0>(Invoke(
        [byte_length, buf, rc](void* out_buf) {
          memcpy(out_buf, buf, byte_length);
          return rc;
        })));
  }

  struct block_status_cb_args {
    const char* metacontext;
    uint64_t entries_offset;
    std::vector<uint32_t> entries;
  };

  // cbs is taken by non-const reference only because of
  // nbd_extent_callback::callback() signature
  void expect_block_status(MockNBDClient& mock_nbd_client,
                           uint64_t byte_offset, uint64_t byte_length,
                           std::vector<block_status_cb_args>& cbs, int rc) {
    EXPECT_CALL(mock_nbd_client, block_status(byte_length, byte_offset, _, _))
      .WillOnce(WithArg<2>(Invoke(
        [&cbs, rc](nbd_extent_callback extent_callback) {
          int err = 0;
          for (auto& cb : cbs) {
            extent_callback.callback(extent_callback.user_data, cb.metacontext,
                                     cb.entries_offset, cb.entries.data(),
                                     cb.entries.size(), &err);
          }
          return rc;
        })));
  }

  void expect_shutdown(MockNBDClient& mock_nbd_client, int rc) {
    EXPECT_CALL(mock_nbd_client, shutdown(_)).WillOnce(Return(rc));
  }

  librbd::ImageCtx *m_image_ctx;
  json_spirit::mObject m_json_object;
};

TEST_F(TestMockMigrationNBDStream, OpenInvalidURI) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  m_json_object["uri"] = 123;
  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(-EINVAL, ctx1.wait());

  C_SaferCond ctx2;
  mock_nbd_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationNBDStream, OpenMissingURI) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  m_json_object.clear();
  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(-EINVAL, ctx1.wait());

  C_SaferCond ctx2;
  mock_nbd_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationNBDStream, OpenInitError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, -1);
  expect_get_errno(*mock_nbd_client, ENOMEM);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(-ENOMEM, ctx1.wait());

  C_SaferCond ctx2;
  mock_nbd_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationNBDStream, OpenAddMetaContextError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, -1);
  expect_get_errno(*mock_nbd_client, EINVAL);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(-EINVAL, ctx1.wait());

  C_SaferCond ctx2;
  mock_nbd_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationNBDStream, OpenConnectURIError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, -1);
  expect_get_errno(*mock_nbd_client, ECONNREFUSED);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(-ECONNREFUSED, ctx1.wait());

  C_SaferCond ctx2;
  mock_nbd_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationNBDStream, OpenConnectURIErrorNoErrno) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, -1);
  // libnbd actually does this for getaddrinfo() errors ("Name or
  // service not known", etc)
  expect_get_errno(*mock_nbd_client, 0);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(-EIO, ctx1.wait());

  C_SaferCond ctx2;
  mock_nbd_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

TEST_F(TestMockMigrationNBDStream, GetSize) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  expect_get_size(*mock_nbd_client, 128);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  uint64_t size;
  mock_nbd_stream.get_size(&size, &ctx2);
  ASSERT_EQ(0, ctx2.wait());
  ASSERT_EQ(128, size);

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, GetSizeError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  expect_get_size(*mock_nbd_client, -1);
  expect_get_errno(*mock_nbd_client, EOVERFLOW);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  uint64_t size;
  mock_nbd_stream.get_size(&size, &ctx2);
  ASSERT_EQ(-EOVERFLOW, ctx2.wait());

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, Read) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  std::string s1(128, '1');
  expect_pread(*mock_nbd_client, 0, 128, s1.c_str(), 0);
  std::string s2(64, '2');
  expect_pread(*mock_nbd_client, 256, 64, s2.c_str(), 0);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  bufferlist bl;
  mock_nbd_stream.read({{0, 128}, {256, 64}}, &bl, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  bufferlist expected_bl;
  expected_bl.append(s1);
  expected_bl.append(s2);
  ASSERT_EQ(expected_bl, bl);

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, ReadError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  std::string s1(128, '1');
  expect_pread(*mock_nbd_client, 0, 128, s1.c_str(), -1);
  expect_get_errno(*mock_nbd_client, ERANGE);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  bufferlist bl;
  mock_nbd_stream.read({{0, 128}, {256, 64}}, &bl, &ctx2);
  ASSERT_EQ(-ERANGE, ctx2.wait());

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, ListSparseExtents) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  // DATA
  std::vector<block_status_cb_args> cbs1 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 0, {128, 0}}
  };
  expect_block_status(*mock_nbd_client, 0, 128, cbs1, 0);
  // ZEROED (zero)
  std::vector<block_status_cb_args> cbs2 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 256, {64, LIBNBD_STATE_ZERO}}
  };
  expect_block_status(*mock_nbd_client, 256, 64, cbs2, 0);
  // ZEROED (hole)
  std::vector<block_status_cb_args> cbs3 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 352, {32, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 352, 32, cbs3, 0);
  // ZEROED, DATA
  std::vector<block_status_cb_args> cbs4 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 384,
     {56, LIBNBD_STATE_ZERO, 8, LIBNBD_STATE_HOLE, 16, 0}}
  };
  expect_block_status(*mock_nbd_client, 384, 80, cbs4, 0);
  // DATA, ZEROED
  std::vector<block_status_cb_args> cbs5 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 464,
     {40, 0, 16, LIBNBD_STATE_HOLE, 8, LIBNBD_STATE_ZERO}}
  };
  expect_block_status(*mock_nbd_client, 464, 64, cbs5, 0);
  // ZEROED, DATA, ZEROED
  std::vector<block_status_cb_args> cbs6 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 528,
     {80, LIBNBD_STATE_HOLE, 128, 0, 32, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 528, 240, cbs6, 0);
  // DATA, ZEROED, DATA
  std::vector<block_status_cb_args> cbs7 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 1536,
     {48, 0, 256, LIBNBD_STATE_ZERO, 16, 0}}
  };
  expect_block_status(*mock_nbd_client, 1536, 320, cbs7, 0);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SparseExtents sparse_extents;
  mock_nbd_stream.list_sparse_extents({{0, 128}, {256, 64}, {352, 32},
                                       {384, 80}, {464, 64}, {528, 240},
                                       {1536, 320}}, &sparse_extents, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SparseExtents expected_sparse_extents;
  expected_sparse_extents.insert(0, 128, {io::SPARSE_EXTENT_STATE_DATA, 128});
  expected_sparse_extents.insert(256, 64, {io::SPARSE_EXTENT_STATE_ZEROED, 64});
  expected_sparse_extents.insert(352, 96, {io::SPARSE_EXTENT_STATE_ZEROED, 96});
  expected_sparse_extents.insert(448, 56, {io::SPARSE_EXTENT_STATE_DATA, 56});
  expected_sparse_extents.insert(504, 104, {io::SPARSE_EXTENT_STATE_ZEROED, 104});
  expected_sparse_extents.insert(608, 128, {io::SPARSE_EXTENT_STATE_DATA, 128});
  expected_sparse_extents.insert(736, 32, {io::SPARSE_EXTENT_STATE_ZEROED, 32});
  expected_sparse_extents.insert(1536, 48, {io::SPARSE_EXTENT_STATE_DATA, 48});
  expected_sparse_extents.insert(1584, 256, {io::SPARSE_EXTENT_STATE_ZEROED, 256});
  expected_sparse_extents.insert(1840, 16, {io::SPARSE_EXTENT_STATE_DATA, 16});
  ASSERT_EQ(expected_sparse_extents, sparse_extents);

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, ListSparseExtentsMoreThanRequested) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  // extra byte at the end
  std::vector<block_status_cb_args> cbs1 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 0, {129, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 0, 128, cbs1, 0);
  // extra byte at the start
  std::vector<block_status_cb_args> cbs2 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 255, {65, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 256, 64, cbs2, 0);
  // extra byte on both sides
  std::vector<block_status_cb_args> cbs3 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 351, {34, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 352, 32, cbs3, 0);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SparseExtents sparse_extents;
  mock_nbd_stream.list_sparse_extents({{0, 128}, {256, 64}, {352, 32}},
                                      &sparse_extents, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SparseExtents expected_sparse_extents;
  expected_sparse_extents.insert(0, 128, {io::SPARSE_EXTENT_STATE_ZEROED, 128});
  expected_sparse_extents.insert(256, 64, {io::SPARSE_EXTENT_STATE_ZEROED, 64});
  expected_sparse_extents.insert(352, 32, {io::SPARSE_EXTENT_STATE_ZEROED, 32});
  ASSERT_EQ(expected_sparse_extents, sparse_extents);

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, ListSparseExtentsLessThanRequested) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  // missing byte at the end
  std::vector<block_status_cb_args> cbs1 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 0, {127, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 0, 128, cbs1, 0);
  // missing byte at the start
  std::vector<block_status_cb_args> cbs2 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 257, {63, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 256, 64, cbs2, 0);
  // missing byte on both sides
  std::vector<block_status_cb_args> cbs3 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 353, {30, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 352, 32, cbs3, 0);
  // zero-sized entry
  std::vector<block_status_cb_args> cbs4 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 400, {0, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 400, 48, cbs4, 0);
  // no entries
  std::vector<block_status_cb_args> cbs5 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 520, {}}
  };
  expect_block_status(*mock_nbd_client, 520, 16, cbs5, 0);
  // no callback
  std::vector<block_status_cb_args> cbs6;
  expect_block_status(*mock_nbd_client, 608, 8, cbs6, 0);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SparseExtents sparse_extents;
  mock_nbd_stream.list_sparse_extents({{0, 128}, {256, 64}, {352, 32},
                                       {400, 48}, {520, 16}, {608, 8}},
                                       &sparse_extents, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SparseExtents expected_sparse_extents;
  expected_sparse_extents.insert(0, 127, {io::SPARSE_EXTENT_STATE_ZEROED, 127});
  expected_sparse_extents.insert(127, 1, {io::SPARSE_EXTENT_STATE_DATA, 1});
  expected_sparse_extents.insert(256, 1, {io::SPARSE_EXTENT_STATE_DATA, 1});
  expected_sparse_extents.insert(257, 63, {io::SPARSE_EXTENT_STATE_ZEROED, 63});
  expected_sparse_extents.insert(352, 1, {io::SPARSE_EXTENT_STATE_DATA, 1});
  expected_sparse_extents.insert(353, 30, {io::SPARSE_EXTENT_STATE_ZEROED, 30});
  expected_sparse_extents.insert(383, 1, {io::SPARSE_EXTENT_STATE_DATA, 1});
  expected_sparse_extents.insert(400, 48, {io::SPARSE_EXTENT_STATE_DATA, 48});
  expected_sparse_extents.insert(520, 16, {io::SPARSE_EXTENT_STATE_DATA, 16});
  expected_sparse_extents.insert(608, 8, {io::SPARSE_EXTENT_STATE_DATA, 8});
  ASSERT_EQ(expected_sparse_extents, sparse_extents);

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, ListSparseExtentsMultipleCallbacks) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  std::vector<block_status_cb_args> cbs1 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 96, {32, LIBNBD_STATE_HOLE}},
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 32, {32, LIBNBD_STATE_ZERO}},
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 0, {32, LIBNBD_STATE_ZERO}},
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 64, {32, LIBNBD_STATE_HOLE}}
  };
  expect_block_status(*mock_nbd_client, 0, 128, cbs1, 0);
  std::vector<block_status_cb_args> cbs2 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 192, {32, 0}},
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 128, {32, LIBNBD_STATE_ZERO, 32, 0}},
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 224, {32, LIBNBD_STATE_ZERO}}
  };
  expect_block_status(*mock_nbd_client, 128, 128, cbs2, 0);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SparseExtents sparse_extents;
  mock_nbd_stream.list_sparse_extents({{0, 128}, {128, 128}}, &sparse_extents,
                                      &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SparseExtents expected_sparse_extents;
  expected_sparse_extents.insert(0, 160, {io::SPARSE_EXTENT_STATE_ZEROED, 160});
  expected_sparse_extents.insert(160, 64, {io::SPARSE_EXTENT_STATE_DATA, 64});
  expected_sparse_extents.insert(224, 32, {io::SPARSE_EXTENT_STATE_ZEROED, 32});
  ASSERT_EQ(expected_sparse_extents, sparse_extents);

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, ListSparseExtentsUnexpectedMetaContexts) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  std::vector<block_status_cb_args> cbs = {
    {"unexpected context 1", 0, {64, LIBNBD_STATE_ZERO, 64, 0}},
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 0, {32, LIBNBD_STATE_ZERO, 96, 0}},
    {"unexpected context 2", 0, {128, LIBNBD_STATE_ZERO}}
  };
  expect_block_status(*mock_nbd_client, 0, 128, cbs, 0);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SparseExtents sparse_extents;
  mock_nbd_stream.list_sparse_extents({{0, 128}}, &sparse_extents, &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SparseExtents expected_sparse_extents;
  expected_sparse_extents.insert(0, 32, {io::SPARSE_EXTENT_STATE_ZEROED, 32});
  expected_sparse_extents.insert(32, 96, {io::SPARSE_EXTENT_STATE_DATA, 96});
  ASSERT_EQ(expected_sparse_extents, sparse_extents);

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, ListSparseExtentsError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  // error isn't propagated -- DATA is assumed instead
  std::vector<block_status_cb_args> cbs1;
  expect_block_status(*mock_nbd_client, 0, 128, cbs1, -1);
  expect_get_errno(*mock_nbd_client, ENOTSUP);
  std::vector<block_status_cb_args> cbs2 = {
    {LIBNBD_CONTEXT_BASE_ALLOCATION, 256, {64, LIBNBD_STATE_ZERO}}
  };
  expect_block_status(*mock_nbd_client, 256, 64, cbs2, 0);
  expect_shutdown(*mock_nbd_client, 0);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  io::SparseExtents sparse_extents;
  mock_nbd_stream.list_sparse_extents({{0, 128}, {256, 64}}, &sparse_extents,
                                      &ctx2);
  ASSERT_EQ(0, ctx2.wait());

  io::SparseExtents expected_sparse_extents;
  expected_sparse_extents.insert(0, 128, {io::SPARSE_EXTENT_STATE_DATA, 128});
  expected_sparse_extents.insert(256, 64, {io::SPARSE_EXTENT_STATE_ZEROED, 64});
  ASSERT_EQ(expected_sparse_extents, sparse_extents);

  C_SaferCond ctx3;
  mock_nbd_stream.close(&ctx3);
  ASSERT_EQ(0, ctx3.wait());
}

TEST_F(TestMockMigrationNBDStream, ShutdownError) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  InSequence seq;

  auto mock_nbd_client = new MockNBDClient();
  expect_init(*mock_nbd_client, 0);
  expect_add_meta_context(*mock_nbd_client, 0);
  expect_connect_uri(*mock_nbd_client, 0);
  // error is ignored
  expect_shutdown(*mock_nbd_client, -1);

  MockNBDStream mock_nbd_stream(&mock_image_ctx, m_json_object);

  C_SaferCond ctx1;
  mock_nbd_stream.open(&ctx1);
  ASSERT_EQ(0, ctx1.wait());

  C_SaferCond ctx2;
  mock_nbd_stream.close(&ctx2);
  ASSERT_EQ(0, ctx2.wait());
}

} // namespace migration
} // namespace librbd
