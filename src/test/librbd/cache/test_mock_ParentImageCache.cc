// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "include/Context.h"
#include "tools/immutable_object_cache/CacheClient.h"
#include "test/immutable_object_cache/MockCacheDaemon.h"
#include "librbd/cache/SharedReadOnlyObjectDispatch.h"
#include "librbd/cache/SharedPersistentObjectCacher.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/mock/MockImageCtx.h"

using namespace ceph::immutable_obj_cache;

namespace librbd {

namespace {

struct MockParentImageCacheImageCtx : public MockImageCtx {
  MockParentImageCacheImageCtx(ImageCtx& image_ctx)
   : MockImageCtx(image_ctx), shared_cache_path("/tmp/socket/path"){
  }
  ~MockParentImageCacheImageCtx() {}

  std::string shared_cache_path;
};

}; // anonymous namespace

};

#include "librbd/cache/SharedReadOnlyObjectDispatch.cc"
#include "librbd/cache/SharedPersistentObjectCacher.cc"
template class librbd::cache::SharedReadOnlyObjectDispatch<librbd::MockParentImageCacheImageCtx, MockCacheClient>;
template class librbd::cache::SharedPersistentObjectCacher<librbd::MockParentImageCacheImageCtx>;

namespace librbd {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::WithArgs;

class TestMockParentImageCache : public TestMockFixture {
  public : 
  typedef cache::SharedReadOnlyObjectDispatch<librbd::MockParentImageCacheImageCtx, MockCacheClient> MockParentImageCache;

  // ====== mock cache client ==== 
  void expect_cache_run(MockParentImageCache& mparent_image_cache, bool ret_val) {
    auto& expect = EXPECT_CALL(*(mparent_image_cache.m_cache_client), run());

    expect.WillOnce((Invoke([ret_val]() {
    })));
  }

   void expect_cache_session_state(MockParentImageCache& mparent_image_cache, bool ret_val) {
     auto & expect = EXPECT_CALL(*(mparent_image_cache.m_cache_client), is_session_work());

     expect.WillOnce((Invoke([ret_val]() {
        return ret_val;
     })));
   }

   void expect_cache_connect(MockParentImageCache& mparent_image_cache, int ret_val) {
     auto& expect = EXPECT_CALL(*(mparent_image_cache.m_cache_client), connect());

     expect.WillOnce((Invoke([ret_val]() {
        return ret_val;
      })));
   }

  void expect_cache_lookup_object(MockParentImageCache& mparent_image_cache,
                                  Context* on_finish) {
    auto& expect = EXPECT_CALL(*(mparent_image_cache.m_cache_client),
                               internal_lookup(_, _, _, _));

     expect.WillOnce(WithArg<3>(Invoke([on_finish](std::string oid) {
       on_finish->complete(0);
     })));
  }

  void expect_cache_close(MockParentImageCache& mparent_image_cache, int ret_val) {
    auto& expect = EXPECT_CALL(*(mparent_image_cache.m_cache_client), close());

    expect.WillOnce((Invoke([ret_val]() {
    })));
  }

  void expect_cache_stop(MockParentImageCache& mparent_image_cache, int ret_val) {
    auto& expect = EXPECT_CALL(*(mparent_image_cache.m_cache_client), stop());

    expect.WillOnce((Invoke([ret_val]() {
    })));
  }

  void expect_cache_register(MockParentImageCache& mparent_image_cache, Context* mock_handle_register, int ret_val) {
    auto& expect = EXPECT_CALL(*(mparent_image_cache.m_cache_client), register_client(_));

    expect.WillOnce(WithArg<0>(Invoke([mock_handle_register, ret_val](Context* ctx) { 
      if(ret_val == 0) {
        mock_handle_register->complete(true);
      } else {
        mock_handle_register->complete(false);
      }
      ctx->complete(true);
      return ret_val;
    })));
  }

  void expect_io_object_dispatcher_register_state(MockParentImageCache& mparent_image_cache, 
                                                  int ret_val) {
    auto& expect = EXPECT_CALL((*(mparent_image_cache.m_image_ctx->io_object_dispatcher)), 
                               register_object_dispatch(_));

        expect.WillOnce(WithArg<0>(Invoke([ret_val, &mparent_image_cache]
                (io::ObjectDispatchInterface* object_dispatch) {
          ASSERT_EQ(object_dispatch, &mparent_image_cache);
        })));
  }
};

TEST_F(TestMockParentImageCache, test_initialization_success) {
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  MockParentImageCacheImageCtx mock_image_ctx(*ictx); 

  auto mock_parent_image_cache = MockParentImageCache::create(&mock_image_ctx);

  // will be released by MockParentImageCache's destruction.
  mock_parent_image_cache->m_cache_client = new MockCacheClient("/cache/path", ictx->cct);

  expect_cache_run(*mock_parent_image_cache, 0);
  expect_cache_connect(*mock_parent_image_cache, 0);
  Context* ctx = new FunctionContext([](bool reg) {
    ASSERT_EQ(reg, true);
  });
  expect_cache_register(*mock_parent_image_cache, ctx, 0);
  expect_cache_session_state(*mock_parent_image_cache, true);
  expect_io_object_dispatcher_register_state(*mock_parent_image_cache, 0);
  expect_cache_close(*mock_parent_image_cache, 0);
  expect_cache_stop(*mock_parent_image_cache, 0);

  mock_parent_image_cache->init();

  ASSERT_EQ(mock_parent_image_cache->get_object_dispatch_layer(),
            io::OBJECT_DISPATCH_LAYER_SHARED_PERSISTENT_CACHE);
  ASSERT_EQ(mock_parent_image_cache->get_state(), true);
  ASSERT_EQ(mock_parent_image_cache->m_cache_client->is_session_work(), true);

  mock_parent_image_cache->m_cache_client->close();
  mock_parent_image_cache->m_cache_client->stop();

  delete mock_parent_image_cache;
}

TEST_F(TestMockParentImageCache, test_initialization_fail_at_connect) {
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  MockParentImageCacheImageCtx mock_image_ctx(*ictx); 

  auto mock_parent_image_cache = MockParentImageCache::create(&mock_image_ctx);

  mock_parent_image_cache->m_cache_client = new MockCacheClient("/cache/path", ictx->cct);

  expect_cache_run(*mock_parent_image_cache, 0);
  expect_cache_connect(*mock_parent_image_cache, -1);
  expect_cache_session_state(*mock_parent_image_cache, false);
  expect_cache_close(*mock_parent_image_cache, 0);
  expect_cache_stop(*mock_parent_image_cache, 0);

  mock_parent_image_cache->init();

  // initialization fails.
  ASSERT_EQ(mock_parent_image_cache->get_object_dispatch_layer(),
            io::OBJECT_DISPATCH_LAYER_SHARED_PERSISTENT_CACHE);
  ASSERT_EQ(mock_parent_image_cache->get_state(), false);
  ASSERT_EQ(mock_parent_image_cache->m_cache_client->is_session_work(), false);

  mock_parent_image_cache->m_cache_client->close();
  mock_parent_image_cache->m_cache_client->stop();

  delete mock_parent_image_cache;

}

TEST_F(TestMockParentImageCache, test_initialization_fail_at_register) {
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  MockParentImageCacheImageCtx mock_image_ctx(*ictx); 

  auto mock_parent_image_cache = MockParentImageCache::create(&mock_image_ctx);

  mock_parent_image_cache->m_cache_client = new MockCacheClient("/cache/path", ictx->cct);
 
  expect_cache_run(*mock_parent_image_cache, 0);
  expect_cache_connect(*mock_parent_image_cache, 0);
  Context* ctx = new FunctionContext([](bool reg) {
    ASSERT_EQ(reg, false);
  });
  expect_cache_register(*mock_parent_image_cache, ctx, -1);
  expect_cache_session_state(*mock_parent_image_cache, false);
  expect_cache_close(*mock_parent_image_cache, 0);
  expect_cache_stop(*mock_parent_image_cache, 0);

  mock_parent_image_cache->init();

  ASSERT_EQ(mock_parent_image_cache->get_object_dispatch_layer(),
            io::OBJECT_DISPATCH_LAYER_SHARED_PERSISTENT_CACHE);
  ASSERT_EQ(mock_parent_image_cache->get_state(), false);
  ASSERT_EQ(mock_parent_image_cache->m_cache_client->is_session_work(), false);
 
  mock_parent_image_cache->m_cache_client->close();
  mock_parent_image_cache->m_cache_client->stop();

  delete mock_parent_image_cache;
}

TEST_F(TestMockParentImageCache, test_disble_interface) {
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  MockParentImageCacheImageCtx mock_image_ctx(*ictx); 

  auto mock_parent_image_cache = MockParentImageCache::create(&mock_image_ctx);

  std::string temp_oid("12345");
  ceph::bufferlist temp_bl;
  ::SnapContext *temp_snapc = nullptr;
  io::ExtentMap *temp_extent_map = nullptr;
  io::DispatchResult* temp_dispatch_result = nullptr;
  io::Extents temp_buffer_extents;
  int* temp_op_flags = nullptr;
  uint64_t* temp_journal_tid = nullptr;
  Context** temp_on_finish = nullptr;
  Context* temp_on_dispatched = nullptr;
  ZTracer::Trace* temp_trace = nullptr;
  io::FlushSource temp_flush_source;
  
  ASSERT_EQ(mock_parent_image_cache->discard(temp_oid, 0, 0, 0, *temp_snapc, 0, *temp_trace, temp_op_flags, 
            temp_journal_tid, temp_dispatch_result, temp_on_finish, temp_on_dispatched), false);
  ASSERT_EQ(mock_parent_image_cache->write(temp_oid, 0, 0, std::move(temp_bl), *temp_snapc, 0,
            *temp_trace, temp_op_flags, temp_journal_tid, temp_dispatch_result, 
            temp_on_finish, temp_on_dispatched), false);
  ASSERT_EQ(mock_parent_image_cache->write_same(temp_oid, 0, 0, 0, std::move(temp_buffer_extents),
            std::move(temp_bl), *temp_snapc, 0, *temp_trace, temp_op_flags,
            temp_journal_tid, temp_dispatch_result, temp_on_finish, temp_on_dispatched), false );
  ASSERT_EQ(mock_parent_image_cache->compare_and_write(temp_oid, 0, 0, std::move(temp_bl), std::move(temp_bl), 
            *temp_snapc, 0, *temp_trace, temp_journal_tid, temp_op_flags, temp_journal_tid, 
            temp_dispatch_result, temp_on_finish, temp_on_dispatched), false);
  ASSERT_EQ(mock_parent_image_cache->flush(temp_flush_source, *temp_trace, temp_journal_tid,
            temp_dispatch_result, temp_on_finish, temp_on_dispatched), false);
  ASSERT_EQ(mock_parent_image_cache->invalidate_cache(nullptr), false);
  ASSERT_EQ(mock_parent_image_cache->reset_existence_cache(nullptr), false);

  delete mock_parent_image_cache;

}

TEST_F(TestMockParentImageCache, test_read) {
  librbd::ImageCtx* ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  MockParentImageCacheImageCtx mock_image_ctx(*ictx); 

  auto mock_parent_image_cache = MockParentImageCache::create(&mock_image_ctx);

  mock_parent_image_cache->m_cache_client = new MockCacheClient("/cache/path", ictx->cct);

  // will be released by MockParentImageCache's destruction.
  mock_parent_image_cache->m_cache_client = new MockCacheClient("/cache/path", ictx->cct);

  expect_cache_run(*mock_parent_image_cache, 0);
  expect_cache_connect(*mock_parent_image_cache, 0);
  Context* ctx = new FunctionContext([](bool reg) {
    ASSERT_EQ(reg, true);
  });
  expect_cache_register(*mock_parent_image_cache, ctx, 0);
  expect_cache_session_state(*mock_parent_image_cache, true);
  expect_io_object_dispatcher_register_state(*mock_parent_image_cache, 0);
  expect_cache_close(*mock_parent_image_cache, 0);
  expect_cache_stop(*mock_parent_image_cache, 0);

  mock_parent_image_cache->init();

  ASSERT_EQ(mock_parent_image_cache->get_object_dispatch_layer(),
            io::OBJECT_DISPATCH_LAYER_SHARED_PERSISTENT_CACHE);
  ASSERT_EQ(mock_parent_image_cache->get_state(), true);
  ASSERT_EQ(mock_parent_image_cache->m_cache_client->is_session_work(), true);

  C_SaferCond cond;
  Context* on_finish = &cond;

  auto& expect = EXPECT_CALL(*(mock_parent_image_cache->m_cache_client), is_session_work())
                   .WillOnce(Return(true))
                   .WillOnce(Return(true));
  expect_cache_lookup_object(*mock_parent_image_cache, on_finish); 

  mock_parent_image_cache->read(ictx->get_object_name(0), 0, 0, 4096, CEPH_NOSNAP, 0, {}, 
                        nullptr, nullptr, nullptr, nullptr, &on_finish, nullptr);
  ASSERT_EQ(0, cond.wait());

  mock_parent_image_cache->m_cache_client->close();
  mock_parent_image_cache->m_cache_client->stop();
  delete mock_parent_image_cache;
}

}  // namespace librbd
