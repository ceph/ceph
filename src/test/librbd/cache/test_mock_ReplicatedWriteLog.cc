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

} // namespace cache
} // namespace librbd
