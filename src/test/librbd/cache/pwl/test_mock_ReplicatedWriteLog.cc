// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "common/hostname.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "include/rbd/librbd.hpp"
#include "librbd/cache/pwl/ImageCacheState.h"
#include "librbd/cache/pwl/Types.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/plugin/Api.h"

namespace librbd {
namespace {

struct MockContextRWL : public C_SaferCond  {
  MOCK_METHOD1(complete, void(int));
  MOCK_METHOD1(finish, void(int));

  void do_complete(int r) {
    C_SaferCond::complete(r);
  }
};

} // anonymous namespace

namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/cache/pwl/AbstractWriteLog.cc"
#include "librbd/cache/pwl/rwl/WriteLog.cc"
template class librbd::cache::pwl::rwl::WriteLog<librbd::MockImageCtx>;

// template definitions
#include "librbd/cache/ImageWriteback.cc"
#include "librbd/cache/pwl/ImageCacheState.cc"
#include "librbd/cache/pwl/Request.cc"
#include "librbd/cache/pwl/rwl/Request.cc"
#include "librbd/plugin/Api.cc"

namespace librbd {
namespace cache {
namespace pwl {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;

typedef io::Extent Extent;
typedef io::Extents Extents;

struct TestMockCacheReplicatedWriteLog : public TestMockFixture {
  typedef librbd::cache::pwl::rwl::WriteLog<librbd::MockImageCtx> MockReplicatedWriteLog;
  typedef librbd::cache::pwl::ImageCacheState<librbd::MockImageCtx> MockImageCacheStateRWL;
  typedef librbd::cache::ImageWriteback<librbd::MockImageCtx> MockImageWriteback;
  typedef librbd::plugin::Api<librbd::MockImageCtx> MockApi;

  MockImageCacheStateRWL *get_cache_state(
      MockImageCtx& mock_image_ctx, MockApi& mock_api) {
    MockImageCacheStateRWL *rwl_state = new MockImageCacheStateRWL(&mock_image_ctx, mock_api);
    return rwl_state;
  }

  void validate_cache_state(librbd::ImageCtx *image_ctx,
                            MockImageCacheStateRWL &state,
                            bool present, bool empty, bool clean,
                            string host, string path,
                            uint64_t size) {
    ASSERT_EQ(present, state.present);
    ASSERT_EQ(empty, state.empty);
    ASSERT_EQ(clean, state.clean);
   
    ASSERT_EQ(host, state.host);
    ASSERT_EQ(path, state.path);
    ASSERT_EQ(size, state.size);
  }

  void expect_context_complete(MockContextRWL& mock_context, int r) {
    EXPECT_CALL(mock_context, complete(r))
      .WillRepeatedly(Invoke([&mock_context](int r) {
                        mock_context.do_complete(r);
                      }));
  }

  void expect_metadata_set(MockImageCtx& mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_metadata_set(_, _, _))
      .WillRepeatedly(Invoke([](std::string key, std::string val, Context* ctx) {
                        ctx->complete(0);
                      }));
  }

  void expect_metadata_remove(MockImageCtx& mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_metadata_remove(_, _))
      .WillRepeatedly(Invoke([](std::string key, Context* ctx) {
                        ctx->complete(0);
                      }));
  }
};

TEST_F(TestMockCacheReplicatedWriteLog, init_state_write) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockApi mock_api;
  MockImageCacheStateRWL image_cache_state(&mock_image_ctx, mock_api);

  validate_cache_state(ictx, image_cache_state, false, true, true, "", "", 0);
  
  image_cache_state.empty = false;
  image_cache_state.clean = false;
  MockContextRWL finish_ctx;
  expect_metadata_set(mock_image_ctx);
  expect_context_complete(finish_ctx, 0);
  image_cache_state.write_image_cache_state(&finish_ctx);
  ASSERT_EQ(0, finish_ctx.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, init_state_json_write) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockApi mock_api;
  MockImageCacheStateRWL image_cache_state(&mock_image_ctx, mock_api);

  string strf = "{ \"present\": true, \"empty\": false, \"clean\": false, \
                   \"host\": \"testhost\", \
                   \"path\": \"/tmp\", \
                   \"mode\": \"rwl\", \
                   \"size\": 1024 }";
  json_spirit::mValue json_root;
  ASSERT_TRUE(json_spirit::read(strf.c_str(), json_root));
  ASSERT_TRUE(image_cache_state.init_from_metadata(json_root));
  validate_cache_state(ictx, image_cache_state, true, false, false,
                       "testhost", "/tmp", 1024);

  MockContextRWL finish_ctx;
  expect_metadata_remove(mock_image_ctx);
  expect_context_complete(finish_ctx, 0);
  image_cache_state.clear_image_cache_state(&finish_ctx);
  ASSERT_EQ(0, finish_ctx.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, init_shutdown) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  MockContextRWL finish_ctx1;
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  rwl.shut_down(&finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, write) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);

  MockContextRWL finish_ctx1;
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, flush) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);

  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_flush;
  expect_context_complete(finish_ctx_flush, 0);
  rwl.flush(&finish_ctx_flush);
  ASSERT_EQ(0, finish_ctx_flush.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, flush_source_shutdown) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_flush;
  expect_context_complete(finish_ctx_flush, 0);
  rwl.flush(io::FLUSH_SOURCE_SHUTDOWN, &finish_ctx_flush);
  ASSERT_EQ(0, finish_ctx_flush.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, flush_source_internal) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);

  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_flush;
  expect_context_complete(finish_ctx_flush, 0);
  rwl.flush(io::FLUSH_SOURCE_INTERNAL, &finish_ctx_flush);
  ASSERT_EQ(0, finish_ctx_flush.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, flush_source_user) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  usleep(10000);
  MockContextRWL finish_ctx_flush;
  expect_context_complete(finish_ctx_flush, 0);
  rwl.flush(io::FLUSH_SOURCE_USER, &finish_ctx_flush);
  ASSERT_EQ(0, finish_ctx_flush.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, read_hit_rwl_cache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  expect_context_complete(finish_ctx_read, 0);
  Extents image_extents_read{{0, 4096}};
  bufferlist read_bl;
  rwl.read(std::move(image_extents_read), &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(bl_copy.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, read_hit_part_rwl_cache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  Extents image_extents_read{{512, 4096}};
  bufferlist hit_bl;
  bl_copy.begin(511).copy(4096-512, hit_bl);
  expect_context_complete(finish_ctx_read, 512);
  bufferlist read_bl;
  rwl.read(std::move(image_extents_read), &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(512, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  bufferlist read_bl_hit;
  read_bl.begin(0).copy(4096-512, read_bl_hit);
  ASSERT_TRUE(hit_bl.contents_equal(read_bl_hit));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, read_miss_rwl_cache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  Extents image_extents_read{{4096, 4096}};
  expect_context_complete(finish_ctx_read, 4096);
  bufferlist read_bl;
  ASSERT_EQ(0, read_bl.length());
  rwl.read(std::move(image_extents_read), &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(4096, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, discard) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_discard;
  expect_context_complete(finish_ctx_discard, 0);
  rwl.discard(0, 4096, 1, &finish_ctx_discard);
  ASSERT_EQ(0, finish_ctx_discard.wait());

  MockContextRWL finish_ctx_read;
  bufferlist read_bl;
  expect_context_complete(finish_ctx_read, 0);
  rwl.read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(read_bl.is_zero());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, writesame) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  bufferlist bl, test_bl;
  bl.append(std::string(512, '1'));
  test_bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.writesame(0, 4096, std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  bufferlist read_bl;
  expect_context_complete(finish_ctx_read, 0);
  rwl.read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(test_bl.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, invalidate) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_invalidate;
  expect_context_complete(finish_ctx_invalidate, 0);
  rwl.invalidate(&finish_ctx_invalidate);
  ASSERT_EQ(0, finish_ctx_invalidate.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, compare_and_write_compare_matched) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl1;
  bl1.append(std::string(4096, '1'));
  bufferlist com_bl = bl1;
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl1), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_cw;
  bufferlist bl2;
  bl2.append(std::string(4096, '2'));
  bufferlist bl2_copy = bl2;
  uint64_t mismatch_offset = -1;
  expect_context_complete(finish_ctx_cw, 0);
  rwl.compare_and_write({{0, 4096}}, std::move(com_bl), std::move(bl2),
                            &mismatch_offset, fadvise_flags, &finish_ctx_cw);
  ASSERT_EQ(0, finish_ctx_cw.wait());
  ASSERT_EQ(0, mismatch_offset);

  MockContextRWL finish_ctx_read;
  bufferlist read_bl;
  expect_context_complete(finish_ctx_read, 0);
  rwl.read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(bl2_copy.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, compare_and_write_compare_failed) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockImageWriteback mock_image_writeback(mock_image_ctx);
  MockApi mock_api;
  MockReplicatedWriteLog rwl(
      mock_image_ctx, get_cache_state(mock_image_ctx, mock_api),
      mock_image_writeback, mock_api);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl1;
  bl1.append(std::string(4096, '1'));
  bufferlist bl1_copy = bl1;
  int fadvise_flags = 0;
  rwl.write(std::move(image_extents), std::move(bl1), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_cw;
  bufferlist bl2;
  bl2.append(std::string(4096, '2'));
  bufferlist com_bl = bl2;
  uint64_t mismatch_offset = -1;
  expect_context_complete(finish_ctx_cw, -EILSEQ);
  rwl.compare_and_write({{0, 4096}}, std::move(com_bl), std::move(bl2),
                            &mismatch_offset, fadvise_flags, &finish_ctx_cw);
  ASSERT_EQ(-EILSEQ, finish_ctx_cw.wait());
  ASSERT_EQ(0, mismatch_offset);

  MockContextRWL finish_ctx_read;
  bufferlist read_bl;
  expect_context_complete(finish_ctx_read, 0);
  rwl.read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(bl1_copy.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

} // namespace pwl
} // namespace cache
} // namespace librbd
