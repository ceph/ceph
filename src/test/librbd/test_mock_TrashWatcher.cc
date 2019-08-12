// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "librbd/TrashWatcher.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <list>

namespace librbd {
namespace {

struct MockTrashWatcher : public TrashWatcher<> {
  MockTrashWatcher(ImageCtx &image_ctx)
    : TrashWatcher<>(image_ctx.md_ctx, image_ctx.op_work_queue) {
  }

  MOCK_METHOD2(handle_image_added, void(const std::string&,
                                        const cls::rbd::TrashImageSpec&));
  MOCK_METHOD1(handle_image_removed, void(const std::string&));
};

} // anonymous namespace

using ::testing::_;
using ::testing::AtLeast;
using ::testing::StrEq;

class TestTrashWatcher : public TestMockFixture {
public:
  void SetUp() override {
    TestFixture::SetUp();

    bufferlist bl;
    ASSERT_EQ(0, m_ioctx.write_full(RBD_TRASH, bl));

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));

    m_trash_watcher = new MockTrashWatcher(*ictx);

    C_SaferCond ctx;
    m_trash_watcher->register_watch(&ctx);
    if (ctx.wait() != 0) {
      delete m_trash_watcher;
      m_trash_watcher = nullptr;
      FAIL();
    }
  }

  void TearDown() override {
    if (m_trash_watcher != nullptr) {
      C_SaferCond ctx;
      m_trash_watcher->unregister_watch(&ctx);
      ASSERT_EQ(0, ctx.wait());
      delete m_trash_watcher;
    }

    TestFixture::TearDown();
  }

  MockTrashWatcher *m_trash_watcher = nullptr;
};

TEST_F(TestTrashWatcher, ImageAdded) {
  REQUIRE_FORMAT_V2();

  cls::rbd::TrashImageSpec trash_image_spec{
    cls::rbd::TRASH_IMAGE_SOURCE_USER, "image name",
    ceph_clock_now(), ceph_clock_now()};

  EXPECT_CALL(*m_trash_watcher, handle_image_added(StrEq("image id"),
                                                   trash_image_spec))
    .Times(AtLeast(1));

  C_SaferCond ctx;
  MockTrashWatcher::notify_image_added(m_ioctx, "image id", trash_image_spec,
                                       &ctx);
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestTrashWatcher, ImageRemoved) {
  REQUIRE_FORMAT_V2();

  EXPECT_CALL(*m_trash_watcher, handle_image_removed(StrEq("image id")))
    .Times(AtLeast(1));

  C_SaferCond ctx;
  MockTrashWatcher::notify_image_removed(m_ioctx, "image id", &ctx);
  ASSERT_EQ(0, ctx.wait());
}

} // namespace librbd
