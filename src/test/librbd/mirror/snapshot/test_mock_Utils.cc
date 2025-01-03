// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockOperations.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "librbd/mirror/snapshot/Utils.h"

namespace librbd {

namespace {

struct MockTestImageCtx : public MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx& image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "librbd/mirror/snapshot/Utils.cc"
template bool librbd::mirror::snapshot::util::can_create_primary_snapshot(
  librbd::MockTestImageCtx *image_ctx, bool demoted, bool force,
  bool* requires_orphan, uint64_t *rollback_snap_id);
template bool librbd::mirror::snapshot::util::can_create_non_primary_snapshot(
  librbd::MockTestImageCtx *image_ctx);

namespace librbd {
namespace mirror {
namespace snapshot {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockMirrorSnapshotUtils : public TestMockFixture {
public:
  uint64_t m_snap_seq = 0;

  uint64_t snap_create(MockTestImageCtx &mock_image_ctx,
                       const cls::rbd::SnapshotNamespace &ns,
                       const std::string& snap_name) {
    EXPECT_TRUE(mock_image_ctx.snap_info.insert(
                  {++m_snap_seq,
                   SnapInfo{snap_name, ns, 0, {}, 0, 0, {}}}).second);
    return m_snap_seq;
  }
};

TEST_F(TestMockMirrorSnapshotUtils, CanCreatePrimarySnapshot) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  // no previous mirror snapshots found
  bool requires_orphan;
  uint64_t rollback_snap_id;
  ASSERT_TRUE(util::can_create_primary_snapshot(&mock_image_ctx, false, false,
                                                &requires_orphan,
                                                &rollback_snap_id));
  ASSERT_FALSE(requires_orphan);
  ASSERT_EQ(rollback_snap_id, CEPH_NOSNAP);

  cls::rbd::MirrorSnapshotNamespace nns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "mirror_uuid", 123};
  nns.complete = true;
  auto copied_snap_id = snap_create(mock_image_ctx, nns, "NPS1");

  // without force, previous snapshot is non-primary
  ASSERT_FALSE(util::can_create_primary_snapshot(&mock_image_ctx, false, false,
                                                 nullptr, nullptr));

  // demoted, previous snapshot is non-primary
  ASSERT_FALSE(util::can_create_primary_snapshot(&mock_image_ctx, true, true,
                                                 nullptr, nullptr));

  // previous non-primary snapshot is copied
  ASSERT_TRUE(util::can_create_primary_snapshot(&mock_image_ctx, false, true,
                                                &requires_orphan,
                                                &rollback_snap_id));
  ASSERT_TRUE(requires_orphan);
  ASSERT_EQ(rollback_snap_id, CEPH_NOSNAP);

  nns.complete = false;
  snap_create(mock_image_ctx, nns, "NPS2");

  // previous non-primary snapshot is not copied yet
  ASSERT_FALSE(util::can_create_primary_snapshot(&mock_image_ctx, false, true,
                                                 nullptr, nullptr));

  // can rollback
  ASSERT_TRUE(util::can_create_primary_snapshot(&mock_image_ctx, false, true,
                                                nullptr, &rollback_snap_id));
  ASSERT_EQ(rollback_snap_id, copied_snap_id);

  nns.state = cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED;
  snap_create(mock_image_ctx, nns, "NPS3");

  // previous non-primary snapshot is orphan
  ASSERT_TRUE(util::can_create_primary_snapshot(&mock_image_ctx, false, true,
                                                nullptr, nullptr));

  cls::rbd::MirrorSnapshotNamespace pns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY_DEMOTED, {"uuid"}, "", CEPH_NOSNAP};
  snap_create(mock_image_ctx, pns, "PS1");

  // previous primary snapshot is demoted, no force
  ASSERT_FALSE(util::can_create_primary_snapshot(&mock_image_ctx, false, false,
                                                 nullptr, nullptr));

  // previous primary snapshot is demoted, force
  ASSERT_TRUE(util::can_create_primary_snapshot(&mock_image_ctx, false, true,
                                                nullptr, nullptr));

  pns.state = cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY;
  snap_create(mock_image_ctx, pns, "PS2");

  // previous snapshot is not demoted primary
  ASSERT_TRUE(util::can_create_primary_snapshot(&mock_image_ctx, false, false,
                                                nullptr, nullptr));
}

TEST_F(TestMockMirrorSnapshotUtils, CanCreateNonPrimarySnapshot) {
  REQUIRE_FORMAT_V2();

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);

  // no previous mirror snapshots found
  ASSERT_TRUE(util::can_create_non_primary_snapshot(&mock_image_ctx));

  cls::rbd::MirrorSnapshotNamespace nns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY, {}, "mirror_uuid", 123};
  snap_create(mock_image_ctx, nns, "NPS1");

  // previous non-primary snapshot is not copied yet
  ASSERT_FALSE(util::can_create_non_primary_snapshot(&mock_image_ctx));

  nns.complete = true;
  snap_create(mock_image_ctx, nns, "NPS2");

  // previous non-primary snapshot is copied
  ASSERT_TRUE(util::can_create_non_primary_snapshot(&mock_image_ctx));

  cls::rbd::MirrorSnapshotNamespace pns{
    cls::rbd::MIRROR_SNAPSHOT_STATE_PRIMARY, {"uuid"}, "", CEPH_NOSNAP};
  snap_create(mock_image_ctx, pns, "PS1");

  // previous primary snapshot is not in demoted state
  ASSERT_FALSE(util::can_create_non_primary_snapshot(&mock_image_ctx));

  pns.state = cls::rbd::MIRROR_SNAPSHOT_STATE_NON_PRIMARY_DEMOTED;
  snap_create(mock_image_ctx, pns, "PS2");

  // previous primary snapshot is in demoted state
  ASSERT_TRUE(util::can_create_non_primary_snapshot(&mock_image_ctx));
}

} // namespace snapshot
} // namespace mirror
} // namespace librbd

