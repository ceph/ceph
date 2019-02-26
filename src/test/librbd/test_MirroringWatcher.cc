// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "librbd/MirroringWatcher.h"
#include "common/Cond.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <list>

void register_test_mirroring_watcher() {
}

namespace librbd {

namespace {

struct MockMirroringWatcher : public MirroringWatcher<> {
  std::string oid;

  MockMirroringWatcher(ImageCtx &image_ctx)
    : MirroringWatcher<>(image_ctx.md_ctx, image_ctx.op_work_queue) {
  }

  MOCK_METHOD1(handle_mode_updated, void(cls::rbd::MirrorMode));
  MOCK_METHOD3(handle_image_updated, void(cls::rbd::MirrorImageState,
                                          const std::string &,
                                          const std::string &));
};

} // anonymous namespace

using ::testing::_;
using ::testing::AtLeast;
using ::testing::Invoke;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMirroringWatcher : public TestFixture {
public:
  void SetUp() override {
    TestFixture::SetUp();

    bufferlist bl;
    ASSERT_EQ(0, m_ioctx.write_full(RBD_MIRRORING, bl));

    librbd::ImageCtx *ictx;
    ASSERT_EQ(0, open_image(m_image_name, &ictx));

    m_image_watcher = new MockMirroringWatcher(*ictx);
    C_SaferCond ctx;
    m_image_watcher->register_watch(&ctx);
    if (ctx.wait() != 0) {
      delete m_image_watcher;
      m_image_watcher = nullptr;
      FAIL();
    }
  }

  void TearDown() override {
    if (m_image_watcher != nullptr) {
      C_SaferCond ctx;
      m_image_watcher->unregister_watch(&ctx);
      ASSERT_EQ(0, ctx.wait());
      delete m_image_watcher;
    }

    TestFixture::TearDown();
  }

  MockMirroringWatcher *m_image_watcher = nullptr;
};

TEST_F(TestMirroringWatcher, ModeUpdated) {
  EXPECT_CALL(*m_image_watcher,
              handle_mode_updated(cls::rbd::MIRROR_MODE_DISABLED))
    .Times(AtLeast(1));

  C_SaferCond ctx;
  MockMirroringWatcher::notify_mode_updated(
    m_ioctx, cls::rbd::MIRROR_MODE_DISABLED, &ctx);
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMirroringWatcher, ImageStatusUpdated) {
  EXPECT_CALL(*m_image_watcher,
              handle_image_updated(cls::rbd::MIRROR_IMAGE_STATE_ENABLED,
                                   StrEq("image id"),
                                   StrEq("global image id")))
    .Times(AtLeast(1));

  C_SaferCond ctx;
  MockMirroringWatcher::notify_image_updated(
    m_ioctx, cls::rbd::MIRROR_IMAGE_STATE_ENABLED, "image id",
    "global image id", &ctx);
  ASSERT_EQ(0, ctx.wait());
}

} // namespace librbd
