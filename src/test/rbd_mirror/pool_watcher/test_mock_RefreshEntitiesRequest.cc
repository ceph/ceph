// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/rbd_mirror/test_mock_fixture.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "tools/rbd_mirror/pool_watcher/RefreshEntitiesRequest.h"
#include "include/stringify.h"

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  explicit MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

// template definitions
#include "tools/rbd_mirror/pool_watcher/RefreshEntitiesRequest.cc"
template class rbd::mirror::pool_watcher::RefreshEntitiesRequest<librbd::MockTestImageCtx>;

namespace rbd {
namespace mirror {
namespace pool_watcher {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockPoolWatcherRefreshEntitiesRequest : public TestMockFixture {
public:
  typedef RefreshEntitiesRequest<librbd::MockTestImageCtx> MockRefreshEntitiesRequest;

  void expect_mirror_image_list(librados::IoCtx &io_ctx,
                                const std::map<std::string, std::string> &ids,
                                int r) {
    bufferlist bl;
    encode(ids, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_image_list"),
                     _, _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }

  void expect_mirror_group_list(
      librados::IoCtx &io_ctx,
      const std::map<std::string, cls::rbd::MirrorGroup> &groups, int r) {
    bufferlist bl;
    encode(groups, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(RBD_MIRRORING, _, StrEq("rbd"), StrEq("mirror_group_list"),
                     _, _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }

  void expect_group_image_list(
      librados::IoCtx &io_ctx, const std::string &group_id,
      const std::vector<cls::rbd::GroupImageStatus> &images, int r) {
    bufferlist bl;
    encode(images, bl);

    EXPECT_CALL(get_mock_io_ctx(io_ctx),
                exec(librbd::util::group_header_name(group_id), _, StrEq("rbd"),
                     StrEq("group_image_list"), _, _, _, _))
      .WillOnce(DoAll(WithArg<5>(Invoke([bl](bufferlist *out_bl) {
                                          *out_bl = bl;
                                        })),
                      Return(r)));
  }
};

TEST_F(TestMockPoolWatcherRefreshEntitiesRequest, Success) {
  InSequence seq;
  expect_mirror_image_list(m_remote_io_ctx, {{"local id", "global id"}}, 0);
  cls::rbd::MirrorGroup mirror_group =
    {"global id", cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
     cls::rbd::MIRROR_GROUP_STATE_ENABLED};
  expect_mirror_group_list(m_remote_io_ctx, {{"local id", mirror_group}}, 0);
  expect_group_image_list(m_remote_io_ctx, "local id", {{}, {}, {}}, 0);

  C_SaferCond ctx;
  std::map<MirrorEntity, std::string> entities;
  MockRefreshEntitiesRequest *req = new MockRefreshEntitiesRequest(
    m_remote_io_ctx, &entities, &ctx);

  req->send();
  ASSERT_EQ(0, ctx.wait());

  std::map<MirrorEntity, std::string> expected_entities =
    {{{MIRROR_ENTITY_TYPE_IMAGE, "global id", 1}, "local id"},
     {{MIRROR_ENTITY_TYPE_GROUP, "global id", 3}, "local id"}};
  ASSERT_EQ(expected_entities, entities);
}

TEST_F(TestMockPoolWatcherRefreshEntitiesRequest, LargeDirectory) {
  InSequence seq;
  std::map<std::string, std::string> mirror_image_list;
  std::map<MirrorEntity, std::string> expected_entities;
  for (uint32_t idx = 1; idx <= 1024; ++idx) {
    mirror_image_list.insert({"local id " + stringify(idx),
                              "global id " + stringify(idx)});
    expected_entities.insert(
      {{MIRROR_ENTITY_TYPE_IMAGE, "global id " + stringify(idx), 1},
       "local id " + stringify(idx)});
  }

  expect_mirror_image_list(m_remote_io_ctx, mirror_image_list, 0);
  expect_mirror_image_list(m_remote_io_ctx, {{"local id", "global id"}}, 0);

  std::map<std::string, cls::rbd::MirrorGroup> mirror_group_list;
  for (uint32_t idx = 1; idx <= 1024; ++idx) {
    cls::rbd::MirrorGroup mirror_group =
      {"global id " + stringify(idx),
       cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
       cls::rbd::MIRROR_GROUP_STATE_ENABLED};

    mirror_group_list.insert({"local id " + stringify(idx), mirror_group});
    expected_entities.insert(
      {{MIRROR_ENTITY_TYPE_GROUP, "global id " + stringify(idx), 2},
       "local id " + stringify(idx)});
  }

  expect_mirror_group_list(m_remote_io_ctx, mirror_group_list, 0);
  cls::rbd::MirrorGroup mirror_group =
    {"global id", cls::rbd::MIRROR_IMAGE_MODE_SNAPSHOT,
     cls::rbd::MIRROR_GROUP_STATE_ENABLED};
  expect_mirror_group_list(m_remote_io_ctx, {{"local id", mirror_group}}, 0);

  expect_group_image_list(m_remote_io_ctx, "local id", {{}, {}}, 0);
  for (auto &[group_id, _] : mirror_group_list) {
    expect_group_image_list(m_remote_io_ctx, group_id, {{}, {}}, 0);
  }

  C_SaferCond ctx;
  std::map<MirrorEntity, std::string> entities;
  MockRefreshEntitiesRequest *req = new MockRefreshEntitiesRequest(
    m_remote_io_ctx, &entities, &ctx);

  req->send();
  ASSERT_EQ(0, ctx.wait());

  expected_entities.insert(
    {{MIRROR_ENTITY_TYPE_IMAGE, "global id", 1}, "local id"});
  expected_entities.insert(
    {{MIRROR_ENTITY_TYPE_GROUP, "global id", 2}, "local id"});
  ASSERT_EQ(expected_entities, entities);
}

TEST_F(TestMockPoolWatcherRefreshEntitiesRequest, MirrorImageListError) {
  InSequence seq;
  expect_mirror_image_list(m_remote_io_ctx, {}, -EINVAL);

  C_SaferCond ctx;
  std::map<MirrorEntity, std::string> entities;
  MockRefreshEntitiesRequest *req = new MockRefreshEntitiesRequest(
    m_remote_io_ctx, &entities, &ctx);

  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace pool_watcher
} // namespace mirror
} // namespace rbd
