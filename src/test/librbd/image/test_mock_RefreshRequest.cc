// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockImageWatcher.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockJournalPolicy.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/api/Image.h"
#include "librbd/image/GetMetadataRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/image/RefreshParentRequest.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>
#include <boost/scope_exit.hpp>

namespace librbd {

namespace {

struct MockRefreshImageCtx : public MockImageCtx {
  MockRefreshImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace image {

template <>
struct GetMetadataRequest<MockRefreshImageCtx> {
  std::string oid;
  std::map<std::string, bufferlist>* pairs = nullptr;
  Context* on_finish = nullptr;

  static GetMetadataRequest* s_instance;
  static GetMetadataRequest* create(librados::IoCtx&,
                                    const std::string& oid,
                                    bool filter_internal,
                                    const std::string& filter_key_prefix,
                                    const std::string& last_key,
                                    uint32_t max_results,
                                    std::map<std::string, bufferlist>* pairs,
                                    Context* on_finish) {
    ceph_assert(s_instance != nullptr);
    EXPECT_EQ("conf_", filter_key_prefix);
    EXPECT_EQ("conf_", last_key);
    s_instance->oid = oid;
    s_instance->pairs = pairs;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetMetadataRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

template <>
struct RefreshParentRequest<MockRefreshImageCtx> {
  static RefreshParentRequest* s_instance;
  static RefreshParentRequest* create(MockRefreshImageCtx &mock_image_ctx,
                                      const ParentImageInfo &parent_md,
                                      const MigrationInfo &migration_info,
                                      Context *on_finish) {
    ceph_assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }
  static bool is_refresh_required(MockRefreshImageCtx &mock_image_ctx,
                                  const ParentImageInfo& parent_md,
                                  const MigrationInfo &migration_info) {
    ceph_assert(s_instance != nullptr);
    return s_instance->is_refresh_required();
  }

  Context *on_finish = nullptr;

  RefreshParentRequest() {
    s_instance = this;
  }

  MOCK_CONST_METHOD0(is_refresh_required, bool());
  MOCK_METHOD0(send, void());
  MOCK_METHOD0(apply, void());
  MOCK_METHOD1(finalize, void(Context *));
};

GetMetadataRequest<MockRefreshImageCtx>* GetMetadataRequest<MockRefreshImageCtx>::s_instance = nullptr;
RefreshParentRequest<MockRefreshImageCtx>* RefreshParentRequest<MockRefreshImageCtx>::s_instance = nullptr;

} // namespace image

namespace io {

template <>
struct ImageDispatchSpec<librbd::MockRefreshImageCtx> {
  static ImageDispatchSpec* s_instance;
  AioCompletion *aio_comp = nullptr;

  static ImageDispatchSpec* create_flush(
      librbd::MockRefreshImageCtx &image_ctx, ImageDispatchLayer dispatch_layer,
      AioCompletion *aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace) {
    ceph_assert(s_instance != nullptr);
    s_instance->aio_comp = aio_comp;
    return s_instance;
  }

  MOCK_CONST_METHOD0(send, void());

  ImageDispatchSpec() {
    s_instance = this;
  }
};

ImageDispatchSpec<librbd::MockRefreshImageCtx>* ImageDispatchSpec<librbd::MockRefreshImageCtx>::s_instance = nullptr;

} // namespace io
namespace util {

inline ImageCtx *get_image_ctx(librbd::MockRefreshImageCtx *image_ctx) {
  return image_ctx->image_ctx;
}

} // namespace util
} // namespace librbd

// template definitions
#include "librbd/image/RefreshRequest.cc"

ACTION_P(TestFeatures, image_ctx) {
  return ((image_ctx->features & arg0) != 0);
}

ACTION_P(ShutDownExclusiveLock, image_ctx) {
  // shutting down exclusive lock will close object map and journal
  image_ctx->exclusive_lock = nullptr;
  image_ctx->object_map = nullptr;
  image_ctx->journal = nullptr;
}

namespace librbd {
namespace image {

using ::testing::_;
using ::testing::DoAll;
using ::testing::DoDefault;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::WithArg;
using ::testing::StrEq;

class TestMockImageRefreshRequest : public TestMockFixture {
public:
  typedef GetMetadataRequest<MockRefreshImageCtx> MockGetMetadataRequest;
  typedef RefreshRequest<MockRefreshImageCtx> MockRefreshRequest;
  typedef RefreshParentRequest<MockRefreshImageCtx> MockRefreshParentRequest;
  typedef io::ImageDispatchSpec<librbd::MockRefreshImageCtx> MockIoImageDispatchSpec;
  typedef std::map<std::string, bufferlist> Metadata;

  void set_v1_migration_header(ImageCtx *ictx) {
    bufferlist hdr;
    ASSERT_EQ(0, read_header_bl(ictx->md_ctx, ictx->header_oid, hdr, nullptr));
    ASSERT_TRUE(hdr.length() >= sizeof(rbd_obj_header_ondisk));
    ASSERT_EQ(0, memcmp(RBD_HEADER_TEXT, hdr.c_str(), sizeof(RBD_HEADER_TEXT)));

    bufferlist::iterator it = hdr.begin();
    it.copy_in(sizeof(RBD_MIGRATE_HEADER_TEXT), RBD_MIGRATE_HEADER_TEXT);
    ASSERT_EQ(0, ictx->md_ctx.write(ictx->header_oid, hdr, hdr.length(), 0));
  }

  void expect_set_require_lock(MockExclusiveLock &mock_exclusive_lock,
                               librbd::io::Direction direction) {
    EXPECT_CALL(mock_exclusive_lock, set_require_lock(direction, _))
      .WillOnce(WithArg<1>(Invoke([](Context* ctx) { ctx->complete(0); })));
  }

  void expect_unset_require_lock(MockExclusiveLock &mock_exclusive_lock,
                                 librbd::io::Direction direction) {
    EXPECT_CALL(mock_exclusive_lock, unset_require_lock(direction));
  }

  void expect_v1_read_header(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               read(mock_image_ctx.header_oid, _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_v1_get_snapshots(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("snap_list"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_v1_get_locks(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                                    StrEq("get_info"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_mutable_metadata(MockRefreshImageCtx &mock_image_ctx,
                                   uint64_t features, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("get_size"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      uint64_t incompatible = (
        mock_image_ctx.read_only ? features & RBD_FEATURES_INCOMPATIBLE :
                                   features & RBD_FEATURES_RW_INCOMPATIBLE);

      expect.WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                       StrEq("get_features"), _, _, _, _))
                    .WillOnce(WithArg<5>(Invoke([features, incompatible](bufferlist* out_bl) {
                                           encode(features, *out_bl);
                                           encode(incompatible, *out_bl);
                                           return 0;
                                         })));
      expect_get_flags(mock_image_ctx, 0);
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                       StrEq("get_snapcontext"), _, _, _, _))
                    .WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                       StrEq("get_info"), _, _, _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_parent_overlap_get(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto& expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("parent_overlap_get"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_parent(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto& expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("parent_get"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
      expect_parent_overlap_get(mock_image_ctx, 0);
    }
  }

  void expect_get_parent_legacy(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto& expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("get_parent"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_migration_header(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("migration_get"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_metadata(MockRefreshImageCtx& mock_image_ctx,
                           MockGetMetadataRequest& mock_request,
                           const std::string& oid,
                           const Metadata& metadata, int r) {
    EXPECT_CALL(mock_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_request, oid, metadata, r]() {
        ASSERT_EQ(oid, mock_request.oid);
        *mock_request.pairs = metadata;
        mock_image_ctx.image_ctx->op_work_queue->queue(
          mock_request.on_finish, r);
      }));
  }

  void expect_get_flags(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("get_flags"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_op_features(MockRefreshImageCtx &mock_image_ctx,
                              uint64_t op_features, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                     StrEq("op_features_get"), _, _, _, _))
      .WillOnce(WithArg<5>(Invoke([op_features, r](bufferlist* out_bl) {
                             encode(op_features, *out_bl);
                             return r;
                           })));
  }

  void expect_get_group(MockRefreshImageCtx &mock_image_ctx, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("image_group_get"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
    }
  }

  void expect_get_snapshots(MockRefreshImageCtx &mock_image_ctx,
                            bool legacy_parent, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("snapshot_get"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
      if (legacy_parent) {
        EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                    exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                         StrEq("get_parent"), _, _, _, _))
                      .WillOnce(DoDefault());
      } else {
        expect_parent_overlap_get(mock_image_ctx, 0);
      }
      expect_get_flags(mock_image_ctx, 0);
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                       StrEq("get_protection_status"), _, _, _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_get_snapshots_legacy(MockRefreshImageCtx &mock_image_ctx,
                                   bool include_timestamp, int r) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                                    StrEq("get_snapshot_name"), _, _, _, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      expect.WillOnce(DoDefault());
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                       StrEq("get_size"), _, _, _, _))
                    .WillOnce(DoDefault());
      if (include_timestamp) {
        EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                    exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                         StrEq("get_snapshot_timestamp"), _, _, _, _))
                      .WillOnce(DoDefault());
      }
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                       StrEq("get_parent"), _, _, _, _))
                    .WillOnce(DoDefault());
      expect_get_flags(mock_image_ctx, 0);
      EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                  exec(mock_image_ctx.header_oid, _, StrEq("rbd"),
                       StrEq("get_protection_status"), _, _, _, _))
                    .WillOnce(DoDefault());
    }
  }

  void expect_apply_metadata(MockRefreshImageCtx &mock_image_ctx,
			     int r) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, is_unregistered())
      .WillOnce(Return(false));
    EXPECT_CALL(mock_image_ctx, apply_metadata(_, false))
		  .WillOnce(Return(r));
  }

  void expect_add_snap(MockRefreshImageCtx &mock_image_ctx,
                       const std::string &snap_name, uint64_t snap_id) {
    EXPECT_CALL(mock_image_ctx, add_snap(_, snap_name, snap_id, _, _, _, _, _));
  }

  void expect_init_exclusive_lock(MockRefreshImageCtx &mock_image_ctx,
                                  MockExclusiveLock &mock_exclusive_lock,
                                  int r) {
    EXPECT_CALL(mock_image_ctx, create_exclusive_lock())
                  .WillOnce(Return(&mock_exclusive_lock));
    EXPECT_CALL(mock_exclusive_lock, init(mock_image_ctx.features, _))
                  .WillOnce(WithArg<1>(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  void expect_shut_down_exclusive_lock(MockRefreshImageCtx &mock_image_ctx,
                                       MockExclusiveLock &mock_exclusive_lock,
                                       int r) {
    EXPECT_CALL(mock_exclusive_lock, shut_down(_))
                  .WillOnce(DoAll(ShutDownExclusiveLock(&mock_image_ctx),
                                  CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue)));
  }

  void expect_init_layout(MockRefreshImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, init_layout(_));
  }

  void expect_test_features(MockRefreshImageCtx &mock_image_ctx) {
    EXPECT_CALL(mock_image_ctx, test_features(_, _))
                  .WillRepeatedly(TestFeatures(&mock_image_ctx));
  }

  void expect_refresh_parent_is_required(MockRefreshParentRequest &mock_refresh_parent_request,
                                         bool required) {
    EXPECT_CALL(mock_refresh_parent_request, is_refresh_required())
                  .WillRepeatedly(Return(required));
  }

  void expect_refresh_parent_send(MockRefreshImageCtx &mock_image_ctx,
                                  MockRefreshParentRequest &mock_refresh_parent_request,
                                  int r) {
    EXPECT_CALL(mock_refresh_parent_request, send())
                  .WillOnce(FinishRequest(&mock_refresh_parent_request, r,
                                          &mock_image_ctx));
  }

  void expect_refresh_parent_apply(MockRefreshParentRequest &mock_refresh_parent_request) {
    EXPECT_CALL(mock_refresh_parent_request, apply());
  }

  void expect_refresh_parent_finalize(MockRefreshImageCtx &mock_image_ctx,
                                      MockRefreshParentRequest &mock_refresh_parent_request,
                                      int r) {
    EXPECT_CALL(mock_refresh_parent_request, finalize(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_is_exclusive_lock_owner(MockExclusiveLock &mock_exclusive_lock,
                                      bool is_owner) {
    EXPECT_CALL(mock_exclusive_lock, is_lock_owner()).WillOnce(Return(is_owner));
  }

  void expect_get_journal_policy(MockImageCtx &mock_image_ctx,
                                 MockJournalPolicy &mock_journal_policy) {
    EXPECT_CALL(mock_image_ctx, get_journal_policy())
                  .WillOnce(Return(&mock_journal_policy));
  }

  void expect_journal_disabled(MockJournalPolicy &mock_journal_policy,
                               bool disabled) {
    EXPECT_CALL(mock_journal_policy, journal_disabled())
      .WillOnce(Return(disabled));
  }

  void expect_open_journal(MockRefreshImageCtx &mock_image_ctx,
                           MockJournal &mock_journal, int r) {
    EXPECT_CALL(mock_image_ctx, create_journal())
                  .WillOnce(Return(&mock_journal));
    EXPECT_CALL(mock_journal, open(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_journal(MockRefreshImageCtx &mock_image_ctx,
                            MockJournal &mock_journal, int r) {
    EXPECT_CALL(mock_journal, close(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_open_object_map(MockRefreshImageCtx &mock_image_ctx,
                              MockObjectMap *mock_object_map, int r) {
    EXPECT_CALL(mock_image_ctx, create_object_map(_))
                  .WillOnce(Return(mock_object_map));
    EXPECT_CALL(*mock_object_map, open(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_object_map(MockRefreshImageCtx &mock_image_ctx,
                               MockObjectMap &mock_object_map, int r) {
    EXPECT_CALL(mock_object_map, close(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_get_snap_id(MockRefreshImageCtx &mock_image_ctx,
                          const std::string &snap_name,
			  uint64_t snap_id) {
    EXPECT_CALL(mock_image_ctx,
		get_snap_id(_, snap_name)).WillOnce(Return(snap_id));
  }

  void expect_block_writes(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, block_writes(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_unblock_writes(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.io_image_dispatcher, unblock_writes())
                  .Times(1);
  }

  void expect_image_flush(MockIoImageDispatchSpec &mock_image_request, int r) {
    EXPECT_CALL(mock_image_request, send())
      .WillOnce(Invoke([&mock_image_request, r]() {
                  mock_image_request.aio_comp->set_request_count(1);
                  mock_image_request.aio_comp->add_request();
                  mock_image_request.aio_comp->complete_request(r);
                }));
  }

};

TEST_F(TestMockImageRefreshRequest, SuccessV1) {
  REQUIRE_FORMAT_V1();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_v1_read_header(mock_image_ctx, 0);
  expect_v1_get_snapshots(mock_image_ctx, 0);
  expect_v1_get_locks(mock_image_ctx, 0);
  expect_init_layout(mock_image_ctx);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessSnapshotV1) {
  REQUIRE_FORMAT_V1();
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));
  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_v1_read_header(mock_image_ctx, 0);
  expect_v1_get_snapshots(mock_image_ctx, 0);
  expect_v1_get_locks(mock_image_ctx, 0);
  expect_init_layout(mock_image_ctx);
  expect_add_snap(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessV2) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, -EOPNOTSUPP);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessSnapshotV2) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_get_snapshots(mock_image_ctx, false, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  expect_add_snap(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessLegacySnapshotV2) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, -EOPNOTSUPP);
  expect_get_parent_legacy(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_get_snapshots(mock_image_ctx, true, -EOPNOTSUPP);
  expect_get_snapshots_legacy(mock_image_ctx, true, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  expect_add_snap(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessLegacySnapshotNoTimestampV2) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, -EOPNOTSUPP);
  expect_get_parent_legacy(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_get_snapshots(mock_image_ctx, true, -EOPNOTSUPP);
  expect_get_snapshots_legacy(mock_image_ctx, true, -EOPNOTSUPP);
  expect_get_snapshots_legacy(mock_image_ctx, false, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  expect_add_snap(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}


TEST_F(TestMockImageRefreshRequest, SuccessSetSnapshotV2) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));
  ASSERT_EQ(0, librbd::api::Image<>::snap_set(ictx,
                                              cls::rbd::UserSnapshotNamespace(),
                                              "snap"));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockObjectMap mock_object_map;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_get_snapshots(mock_image_ctx, false, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    expect_open_object_map(mock_image_ctx, &mock_object_map, 0);
  }
  expect_add_snap(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);
  expect_get_snap_id(mock_image_ctx, "snap", ictx->snap_ids.begin()->second);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessChild) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  librbd::ImageCtx *ictx2 = nullptr;
  std::string clone_name = get_temp_image_name();

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));
  ASSERT_EQ(0, snap_protect(*ictx, "snap"));
  BOOST_SCOPE_EXIT_ALL((&)) {
    if (ictx2 != nullptr) {
      close_image(ictx2);
    }

    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, librbd::api::Image<>::remove(m_ioctx, clone_name, no_op));
    ASSERT_EQ(0, ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(), "snap"));
  };

  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap", m_ioctx,
                             clone_name.c_str(), ictx->features, &order, 0, 0));

  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  MockRefreshImageCtx mock_image_ctx(*ictx2);
  MockRefreshParentRequest *mock_refresh_parent_request = new MockRefreshParentRequest();
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx2->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_op_features(mock_image_ctx, RBD_OPERATION_FEATURE_CLONE_CHILD, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(*mock_refresh_parent_request, true);
  expect_refresh_parent_send(mock_image_ctx, *mock_refresh_parent_request, 0);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  expect_refresh_parent_apply(*mock_refresh_parent_request);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());
  expect_refresh_parent_finalize(mock_image_ctx, *mock_refresh_parent_request, 0);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessChildDontOpenParent) {
  REQUIRE_FEATURE(RBD_FEATURE_LAYERING);

  librbd::ImageCtx *ictx;
  librbd::ImageCtx *ictx2 = nullptr;
  std::string clone_name = get_temp_image_name();

  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, snap_create(*ictx, "snap"));
  ASSERT_EQ(0, snap_protect(*ictx, "snap"));
  BOOST_SCOPE_EXIT_ALL((&)) {
    if (ictx2 != nullptr) {
      close_image(ictx2);
    }

    librbd::NoOpProgressContext no_op;
    ASSERT_EQ(0, librbd::api::Image<>::remove(m_ioctx, clone_name, no_op));
    ASSERT_EQ(0, ictx->operations->snap_unprotect(cls::rbd::UserSnapshotNamespace(), "snap"));
  };

  int order = ictx->order;
  ASSERT_EQ(0, librbd::clone(m_ioctx, m_image_name.c_str(), "snap", m_ioctx,
                             clone_name.c_str(), ictx->features, &order, 0, 0));

  ASSERT_EQ(0, open_image(clone_name, &ictx2));

  MockRefreshImageCtx mock_image_ctx(*ictx2);
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx2->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_op_features(mock_image_ctx, RBD_OPERATION_FEATURE_CLONE_CHILD, 0);
  expect_get_group(mock_image_ctx, 0);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, true, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, SuccessOpFeatures) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  mock_image_ctx.features |= RBD_FEATURE_OPERATIONS;

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, mock_image_ctx.features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_op_features(mock_image_ctx, 4096, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(4096U, mock_image_ctx.op_features);
  ASSERT_TRUE(mock_image_ctx.operations_disabled);
}

TEST_F(TestMockImageRefreshRequest, DisableExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  MockJournal mock_journal;
  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    mock_image_ctx.journal = &mock_journal;
  }

  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_JOURNALING,
                                                   false));
  }

  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_OBJECT_MAP,
                                                   false));
  }

  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_EXCLUSIVE_LOCK,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  // verify that exclusive lock is properly handled when object map
  // and journaling were never enabled (or active)
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());
  expect_shut_down_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, DisableExclusiveLockWhileAcquiringLock) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_JOURNALING,
                                                   false));
  }

  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_OBJECT_MAP,
                                                   false));
  }

  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_EXCLUSIVE_LOCK,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  // verify that exclusive lock is properly handled when object map
  // and journaling were never enabled (or active)
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, true, false, &ctx);
  req->send();

  ASSERT_EQ(-ERESTART, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, JournalDisabledByPolicy) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  if (ictx->test_features(RBD_FEATURE_FAST_DIFF)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_FAST_DIFF,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockJournal mock_journal;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, true);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);

  MockJournalPolicy mock_journal_policy;
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_journal_disabled(mock_journal_policy, true);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, EnableJournalWithExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  if (ictx->test_features(RBD_FEATURE_FAST_DIFF)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_FAST_DIFF,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockJournal mock_journal;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, true);

  // journal should be immediately opened if exclusive lock owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);

  MockJournalPolicy mock_journal_policy;
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_journal_disabled(mock_journal_policy, false);
  expect_open_journal(mock_image_ctx, mock_journal, 0);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, EnableJournalWithoutExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_OBJECT_MAP,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, false);

  // do not open the journal if exclusive lock is not owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_set_require_lock(mock_exclusive_lock, librbd::io::DIRECTION_BOTH);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, DisableJournal) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap mock_object_map;
  if (ictx->test_features(RBD_FEATURE_OBJECT_MAP)) {
    mock_image_ctx.object_map = &mock_object_map;
  }

  MockJournal mock_journal;
  mock_image_ctx.journal = &mock_journal;

  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_JOURNALING,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  // verify journal is closed if feature disabled
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_block_writes(mock_image_ctx, 0);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());
  if (!mock_image_ctx.clone_copy_on_read) {
    expect_unset_require_lock(mock_exclusive_lock, librbd::io::DIRECTION_READ);
  }
  expect_close_journal(mock_image_ctx, mock_journal, 0);
  expect_unblock_writes(mock_image_ctx);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false,
                                                   &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, EnableObjectMapWithExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_JOURNALING,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap mock_object_map;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, true);

  // object map should be immediately opened if exclusive lock owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_open_object_map(mock_image_ctx, &mock_object_map, 0);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, EnableObjectMapWithoutExclusiveLock) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_JOURNALING,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, false);

  // do not open the object map if exclusive lock is not owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, DisableObjectMap) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap mock_object_map;
  mock_image_ctx.object_map = &mock_object_map;

  MockJournal mock_journal;
  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    mock_image_ctx.journal = &mock_journal;
  }

  if (ictx->test_features(RBD_FEATURE_FAST_DIFF)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_FAST_DIFF,
                                                   false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  // verify object map is closed if feature disabled
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());
  expect_close_object_map(mock_image_ctx, mock_object_map, 0);

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, OpenObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_JOURNALING,
            false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap mock_object_map;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, true);

  // object map should be immediately opened if exclusive lock owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_open_object_map(mock_image_ctx, &mock_object_map, -EBLACKLISTED);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false,
                                                   &ctx);
  req->send();

  ASSERT_EQ(-EBLACKLISTED, ctx.wait());
  ASSERT_EQ(nullptr, mock_image_ctx.object_map);
}

TEST_F(TestMockImageRefreshRequest, OpenObjectMapTooLarge) {
  REQUIRE_FEATURE(RBD_FEATURE_OBJECT_MAP);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
    ASSERT_EQ(0, ictx->operations->update_features(RBD_FEATURE_JOURNALING,
            false));
  }

  ASSERT_EQ(0, ictx->state->refresh());

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;

  MockExclusiveLock mock_exclusive_lock;
  mock_image_ctx.exclusive_lock = &mock_exclusive_lock;

  MockObjectMap mock_object_map;

  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);
  expect_is_exclusive_lock_owner(mock_exclusive_lock, true);

  // object map should be immediately opened if exclusive lock owned
  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  expect_open_object_map(mock_image_ctx, &mock_object_map, -EFBIG);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false,
                                                   &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(nullptr, mock_image_ctx.object_map);
}

TEST_F(TestMockImageRefreshRequest, ApplyMetadataError) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;
  expect_get_mutable_metadata(mock_image_ctx, ictx->features, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, -EINVAL);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx;
  MockRefreshRequest *req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx);
  req->send();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockImageRefreshRequest, NonPrimaryFeature) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockRefreshImageCtx mock_image_ctx(*ictx);
  MockRefreshParentRequest mock_refresh_parent_request;
  MockExclusiveLock mock_exclusive_lock;
  expect_op_work_queue(mock_image_ctx);
  expect_test_features(mock_image_ctx);

  InSequence seq;

  // ensure the image is put into read-only mode
  expect_get_mutable_metadata(mock_image_ctx,
                              ictx->features | RBD_FEATURE_NON_PRIMARY, 0);
  expect_get_parent(mock_image_ctx, 0);
  MockGetMetadataRequest mock_get_metadata_request;
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx1;
  auto req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx1);
  req->send();

  ASSERT_EQ(0, ctx1.wait());
  ASSERT_TRUE(mock_image_ctx.read_only);
  ASSERT_EQ(IMAGE_READ_ONLY_FLAG_NON_PRIMARY, mock_image_ctx.read_only_flags);

  // try again but permit R/W against non-primary image
  mock_image_ctx.read_only_mask = ~IMAGE_READ_ONLY_FLAG_NON_PRIMARY;

  expect_get_mutable_metadata(mock_image_ctx,
                              ictx->features | RBD_FEATURE_NON_PRIMARY, 0);
  expect_get_parent(mock_image_ctx, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request,
                      mock_image_ctx.header_oid, {}, 0);
  expect_get_metadata(mock_image_ctx, mock_get_metadata_request, RBD_INFO, {},
                      0);
  expect_apply_metadata(mock_image_ctx, 0);
  expect_get_group(mock_image_ctx, 0);
  expect_refresh_parent_is_required(mock_refresh_parent_request, false);
  if (ictx->test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    expect_init_exclusive_lock(mock_image_ctx, mock_exclusive_lock, 0);
  }
  EXPECT_CALL(mock_image_ctx, rebuild_data_io_context());

  C_SaferCond ctx2;
  req = new MockRefreshRequest(mock_image_ctx, false, false, &ctx2);
  req->send();

  ASSERT_EQ(0, ctx2.wait());
  ASSERT_FALSE(mock_image_ctx.read_only);
  ASSERT_EQ(0U, mock_image_ctx.read_only_flags);
}

} // namespace image
} // namespace librbd
