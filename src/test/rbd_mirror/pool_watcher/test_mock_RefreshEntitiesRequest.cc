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

};

TEST_F(TestMockPoolWatcherRefreshEntitiesRequest, Success) {
  InSequence seq;
  expect_mirror_image_list(m_remote_io_ctx, {{"local id", "global id"}}, 0);

  C_SaferCond ctx;
  std::map<MirrorEntity, std::string> entities;
  MockRefreshEntitiesRequest *req = new MockRefreshEntitiesRequest(
    m_remote_io_ctx, &entities, &ctx);

  req->send();
  ASSERT_EQ(0, ctx.wait());

  std::map<MirrorEntity, std::string> expected_entities =
    {{{MIRROR_ENTITY_TYPE_IMAGE, "global id", 1}, "local id"}};
  ASSERT_EQ(expected_entities, entities);
}

TEST_F(TestMockPoolWatcherRefreshEntitiesRequest, LargeDirectory) {
  InSequence seq;
  std::map<std::string, std::string> mirror_list;
  std::map<MirrorEntity, std::string> expected_entities;
  for (uint32_t idx = 1; idx <= 1024; ++idx) {
    mirror_list.insert(std::make_pair("local id " + stringify(idx),
                                      "global id " + stringify(idx)));
    expected_entities.insert(
      {{MIRROR_ENTITY_TYPE_IMAGE, "global id " + stringify(idx), 1},
       "local id " + stringify(idx)});
  }

  expect_mirror_image_list(m_remote_io_ctx, mirror_list, 0);
  expect_mirror_image_list(m_remote_io_ctx, {{"local id", "global id"}}, 0);

  C_SaferCond ctx;
  std::map<MirrorEntity, std::string> entities;
  MockRefreshEntitiesRequest *req = new MockRefreshEntitiesRequest(
    m_remote_io_ctx, &entities, &ctx);

  req->send();
  ASSERT_EQ(0, ctx.wait());

  expected_entities.insert(
    {{MIRROR_ENTITY_TYPE_IMAGE, "global id", 1}, "local id"});
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
