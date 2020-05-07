// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include "common/hostname.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "include/rbd/librbd.hpp"
#include "librbd/cache/rwl/ImageCacheState.h"
#include "librbd/cache/rwl/Types.h"
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

namespace util {

inline ImageCtx *get_image_ctx(MockImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

#include "librbd/cache/ReplicatedWriteLog.cc"

// template definitions
#include "librbd/cache/ImageWriteback.cc"
#include "librbd/cache/rwl/ImageCacheState.cc"
#include "librbd/cache/rwl/SyncPoint.cc"
#include "librbd/cache/rwl/Request.cc"
#include "librbd/cache/rwl/Types.cc"
#include "librbd/cache/rwl/LogOperation.cc"
#include "librbd/cache/rwl/LogMap.cc"

template class librbd::cache::ImageWriteback<librbd::MockImageCtx>;
template class librbd::cache::rwl::ImageCacheState<librbd::MockImageCtx>;

namespace librbd {
namespace cache {

using ::testing::_;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;

struct TestMockCacheReplicatedWriteLog : public TestMockFixture {
  typedef ReplicatedWriteLog<librbd::MockImageCtx> MockReplicatedWriteLog;
  typedef librbd::cache::rwl::ImageCacheState<librbd::MockImageCtx> MockImageCacheStateRWL;

  MockImageCacheStateRWL *get_cache_state(MockImageCtx& mock_image_ctx) {
    MockImageCacheStateRWL *rwl_state = new MockImageCacheStateRWL(&mock_image_ctx);
    return rwl_state;
  }

  void validate_cache_state(librbd::ImageCtx *image_ctx,
                            MockImageCacheStateRWL &state,
                            bool present, bool empty, bool clean,
                            string host="", string path="",
                            uint64_t size=0) {
    ConfigProxy &config = image_ctx->config;
    ASSERT_EQ(present, state.present);
    ASSERT_EQ(empty, state.empty);
    ASSERT_EQ(clean, state.clean);
    
    if (host.empty())
      host = ceph_get_short_hostname();
    if (path.empty())
      path = config.get_val<string>("rbd_rwl_path");
    if (!size)
      size = config.get_val<uint64_t>("rbd_rwl_size");
    
    ASSERT_EQ(host, state.host);
    ASSERT_EQ(path, state.path);
    ASSERT_EQ(size, state.size);
    ASSERT_EQ(config.get_val<bool>("rbd_rwl_log_periodic_stats"),
	      state.log_periodic_stats);
  }

  void expect_op_work_queue(MockImageCtx& mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.op_work_queue, queue(_, _))
      .WillRepeatedly(Invoke([](Context* ctx, int r) {
                        ctx->complete(r);
                      }));
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
  MockImageCacheStateRWL image_cache_state(&mock_image_ctx);

  validate_cache_state(ictx, image_cache_state, true, true, true);
  
  image_cache_state.empty = false;
  image_cache_state.clean = false;
  MockContextRWL finish_ctx;
  expect_metadata_set(mock_image_ctx);
  expect_context_complete(finish_ctx, 0);
  image_cache_state.write_image_cache_state(&finish_ctx);
  ASSERT_EQ(0, finish_ctx.wait());
}

static void get_jf(const string& s, JSONFormattable *f)
{
  JSONParser p;
  bool result = p.parse(s.c_str(), s.size());
  if (!result) {
    cout << "Failed to parse: '" << s << "'" << std::endl;
  }
  ASSERT_EQ(true, result);
  try {
    decode_json_obj(*f, &p);
  } catch (JSONDecoder::err& e) {
    ASSERT_TRUE(0 == "Failed to decode JSON object");
  }
}

TEST_F(TestMockCacheReplicatedWriteLog, init_state_json_write) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  JSONFormattable f;
  string strf = "{ \"present\": \"1\", \"empty\": \"0\", \"clean\": \"0\", \
                   \"rwl_host\": \"testhost\", \
                   \"rwl_path\": \"/tmp\", \
                   \"rwl_size\": \"1024\" }";
  get_jf(strf, &f);
  MockImageCacheStateRWL image_cache_state(&mock_image_ctx, f);

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
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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

TEST_F(TestMockCacheReplicatedWriteLog, aio_write) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));

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
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
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
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
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

TEST_F(TestMockCacheReplicatedWriteLog, aio_flush_source_shutdown) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  usleep(10000);
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
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  expect_context_complete(finish_ctx_read, 0);
  Extents image_extents_read{{0, 4096}};
  bufferlist read_bl;
  rwl.aio_read(std::move(image_extents_read), &read_bl, fadvise_flags, &finish_ctx_read);
  ASSERT_EQ(0, finish_ctx_read.wait());
  ASSERT_EQ(4096, read_bl.length());
  ASSERT_TRUE(bl_copy.contents_equal(read_bl));

  MockContextRWL finish_ctx3;
  expect_context_complete(finish_ctx3, 0);
  rwl.shut_down(&finish_ctx3);
  ASSERT_EQ(0, finish_ctx3.wait());
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_read_hit_part_rwl_cache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_read;
  Extents image_extents_read{{512, 4096}};
  bufferlist hit_bl;
  bl_copy.begin(511).copy(4096-512, hit_bl);
  expect_context_complete(finish_ctx_read, 512);
  bufferlist read_bl;
  rwl.aio_read(std::move(image_extents_read), &read_bl, fadvise_flags, &finish_ctx_read);
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

TEST_F(TestMockCacheReplicatedWriteLog, aio_read_miss_rwl_cache) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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

TEST_F(TestMockCacheReplicatedWriteLog, aio_discard) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_discard;
  expect_context_complete(finish_ctx_discard, 0);
  rwl.aio_discard(0, 4096, 1, &finish_ctx_discard);
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
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_writesame) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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
}

TEST_F(TestMockCacheReplicatedWriteLog, invalidate) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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
  rwl.aio_write(std::move(image_extents), std::move(bl), fadvise_flags, &finish_ctx2);
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

TEST_F(TestMockCacheReplicatedWriteLog, aio_compare_and_write_compare_matched) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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
  rwl.aio_write(std::move(image_extents), std::move(bl1), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_cw;
  bufferlist bl2;
  bl2.append(std::string(4096, '2'));
  bufferlist bl2_copy = bl2;
  uint64_t mismatch_offset = -1;
  expect_context_complete(finish_ctx_cw, 0);
  rwl.aio_compare_and_write({{0, 4096}}, std::move(com_bl), std::move(bl2),
                            &mismatch_offset, fadvise_flags, &finish_ctx_cw);
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
}

TEST_F(TestMockCacheReplicatedWriteLog, aio_compare_and_write_compare_failed) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockReplicatedWriteLog rwl(mock_image_ctx, get_cache_state(mock_image_ctx));
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
  rwl.aio_write(std::move(image_extents), std::move(bl1), fadvise_flags, &finish_ctx2);
  ASSERT_EQ(0, finish_ctx2.wait());

  MockContextRWL finish_ctx_cw;
  bufferlist bl2;
  bl2.append(std::string(4096, '2'));
  bufferlist com_bl = bl2;
  uint64_t mismatch_offset = -1;
  expect_context_complete(finish_ctx_cw, -EILSEQ);
  rwl.aio_compare_and_write({{0, 4096}}, std::move(com_bl), std::move(bl2),
                            &mismatch_offset, fadvise_flags, &finish_ctx_cw);
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
}

} // namespace cache
} // namespace librbd
