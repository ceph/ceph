// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "common/hostname.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "include/rbd/librbd.hpp"
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/ReplicatedWriteLog.h"


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
} // namespace librbd

#include "librbd/cache/ReplicatedWriteLog.cc"

// template definitions
#include "librbd/cache/ImageWriteback.cc"
#include "librbd/cache/ImageCache.cc"

template class librbd::cache::ImageWriteback<librbd::MockImageCtx>;
template class librbd::cache::ImageCache<librbd::MockImageCtx>;
template class librbd::cache::ImageCacheState<librbd::MockImageCtx>;

namespace librbd {
namespace cache {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;

struct TestMockCacheReplicatedWriteLog : public TestMockFixture {
  typedef ReplicatedWriteLog<librbd::MockImageCtx> MockReplicatedWriteLog;
  typedef ImageCacheStateRWL<librbd::MockImageCtx> MockImageCacheStateRWL;

  MockImageCacheStateRWL *get_cache_state(MockImageCtx& mock_image_ctx) {
    MockImageCacheStateRWL *rwl_state = new MockImageCacheStateRWL(&mock_image_ctx);
    return rwl_state;
  }

  void expect_op_work_queue(MockImageCtx& mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.op_work_queue, queue(_, _))
      .WillRepeatedly(Invoke([](Context* ctx, int r) {
                        ctx->complete(r);
                      }));
  }

  void expect_op_work_queue1(MockImageCtx& mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.op_work_queue, queue(_))
      .WillRepeatedly(Invoke([](Context* ctx) {
                        ctx->complete(0);
                      }));
  }

  void expect_context_complete(MockContextRWL& mock_context, int r) {
    EXPECT_CALL(mock_context, complete(r))
      .WillOnce(Invoke([&mock_context](int r) {
                  mock_context.do_complete(r);
                }));
  }

  void expect_metadata_set(MockImageCtx& mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.operations, execute_metadata_set(_, _, _))
      .WillOnce(Invoke([](std::string key, std::string val, Context* ctx) {
                        ctx->complete(0);
                      }));
  }
};

TEST_F(TestMockCacheReplicatedWriteLog, init_shutdown) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  MockContextRWL finish_ctx1;
  expect_op_work_queue(mock_image_ctx);
  expect_op_work_queue1(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, present);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  rwl.shut_down(&finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_write) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));

  MockContextRWL finish_ctx1;
  expect_op_work_queue(mock_image_ctx);
  expect_op_work_queue1(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, present);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());
  
  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_flush_source_shutdown) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, present);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_flush;
  expect_context_complete(finish_ctx_flush, 0);
  rwl.aio_flush(io::FLUSH_SOURCE_SHUTDOWN, &finish_ctx_flush);
  ASSERT_EQ(0, finish_ctx_flush.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_flush_source_internal) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, present);
  ASSERT_EQ(0, finish_ctx1.wait());

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_flush;
  expect_context_complete(finish_ctx_flush, 0);
  rwl.aio_flush(io::FLUSH_SOURCE_INTERNAL, &finish_ctx_flush);
  ASSERT_EQ(0, finish_ctx_flush.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_flush_source_user) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, present);
  ASSERT_EQ(0, finish_ctx1.wait());
  
  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_flush;
  expect_context_complete(finish_ctx_flush, 0);
  rwl.aio_flush(io::FLUSH_SOURCE_USER, &finish_ctx_flush);
  ASSERT_EQ(0, finish_ctx_flush.wait());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_read_hit_rwl_cache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, present);
  ASSERT_EQ(0, finish_ctx1.wait());
  
  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  expect_context_complete(finish_ctx_read, 0);
  Extents image_extents_read{{0, 4096}};
  bufferlist read_bl;
  rwl.aio_read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(bl_copy.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_read_miss_rwl_cache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, present);
  ASSERT_EQ(0, finish_ctx1.wait());
  
  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  Extents image_extents_read{{4096, 4096}};
  expect_context_complete(finish_ctx_read, 4096);
  bufferlist read_bl;
  ASSERT_EQ(0, read_bl.length());
  rwl.aio_read(std::move(image_extents_read), &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(4096, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, invalidate) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);
  
  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_invalidate;
  expect_context_complete(finish_ctx_invalidate, 0);
  rwl.invalidate(false, &finish_ctx_invalidate);
  ASSERT_EQ(0, finish_ctx_invalidate.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(0, present);
}

TEST_F(TestMockCacheReplicatedWriteLog, invalidate_and_discard_unflushed_writes) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);
  
  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_invalidate;
  expect_context_complete(finish_ctx_invalidate, 0);
  rwl.invalidate(true, &finish_ctx_invalidate);
  ASSERT_EQ(0, finish_ctx_invalidate.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(0, present);
}

TEST_F(TestMockCacheReplicatedWriteLog, flush) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);
  
  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_flush;
  expect_context_complete(finish_ctx_flush, 0);
  rwl.flush(&finish_ctx_flush);
  ASSERT_EQ(0, finish_ctx_flush.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, present);

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(0, present);
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_discard) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl;
  bl.append(std::string(4096, '1'));
  bufferlist bl_copy = bl;
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_discard;
  expect_context_complete(finish_ctx_discard, 0);
  rwl.aio_discard(0, 4096, false, &finish_ctx_discard);
  ASSERT_EQ(0, finish_ctx_discard.wait());

  MockContextRWL finish_ctx_read;
  bufferlist read_bl;
  expect_context_complete(finish_ctx_read, 0);
  rwl.aio_read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(read_bl.is_zero());

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(0, present);
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_writesame) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  bufferlist bl, test_bl;
  bl.append(std::string(512, '1'));
  test_bl.append(std::string(4096, '1')); 
  int fadvise_flags = 0;
  rwl.aio_writesame(0, 4096, std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  bufferlist read_bl;
  expect_context_complete(finish_ctx_read, 0);
  rwl.aio_read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(test_bl.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(0, present);
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_compare_and_write_compare_matched) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl1;
  bl1.append(std::string(4096, '1'));
  bufferlist com_bl = bl1;
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl1), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_cw;
  bufferlist bl2;
  bl2.append(std::string(4096, '2'));
  bufferlist bl2_copy = bl2;
  uint64_t mismatch_offset = -1;
  expect_context_complete(finish_ctx_cw, 0);
  rwl.aio_compare_and_write({{0, 4096}}, std::move(com_bl), std::move(bl2), &mismatch_offset, fadvise_flags, &finish_ctx_cw);
  ASSERT_EQ(0, finish_ctx_cw.wait());
  ASSERT_EQ(0, mismatch_offset);

  MockContextRWL finish_ctx_read;
  bufferlist read_bl;
  expect_context_complete(finish_ctx_read, 0);
  rwl.aio_read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(bl2_copy.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);

  ASSERT_EQ(0, finish_ctx3.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(0, present);
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_compare_and_write_compare_failed) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
  expect_op_work_queue1(mock_image_ctx);
  expect_op_work_queue(mock_image_ctx);
  expect_metadata_set(mock_image_ctx);

  MockContextRWL finish_ctx1;
  expect_context_complete(finish_ctx1, 0);
  rwl.init(&finish_ctx1);
  ASSERT_EQ(0, finish_ctx1.wait());
  bool clean;
  bool empty;
  bool present;
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(1, present);

  MockContextRWL finish_ctx2;
  expect_context_complete(finish_ctx2, 0);
  Extents image_extents{{0, 4096}};
  bufferlist bl1;
  bl1.append(std::string(4096, '1'));
  bufferlist bl1_copy = bl1;
  int fadvise_flags = 0;
  rwl.aio_write(std::move(image_extents), std::move(bl1), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_cw;
  bufferlist bl2;
  bl2.append(std::string(4096, '2'));
  bufferlist com_bl = bl2;
  uint64_t mismatch_offset = -1;
  expect_context_complete(finish_ctx_cw, -EILSEQ);
  rwl.aio_compare_and_write({{0, 4096}}, std::move(com_bl), std::move(bl2), &mismatch_offset, fadvise_flags, &finish_ctx_cw);
  ASSERT_EQ(-EILSEQ, finish_ctx_cw.wait());
  ASSERT_EQ(0, mismatch_offset);

  MockContextRWL finish_ctx_read;
  bufferlist read_bl;
  expect_context_complete(finish_ctx_read, 0);
  rwl.aio_read({{0, 4096}}, &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(bl1_copy.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
  rwl.get_state(clean, empty, present);
  ASSERT_EQ(1, clean);
  ASSERT_EQ(1, empty);
  ASSERT_EQ(0, present);
}

} // namespace cache
} // namespace librbd
