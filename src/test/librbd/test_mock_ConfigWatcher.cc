// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "include/rbd_types.h"
#include "common/ceph_mutex.h"
#include "librbd/ConfigWatcher.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <list>

namespace librbd {
namespace {

struct MockTestImageCtx : public MockImageCtx {
  MockTestImageCtx(ImageCtx &image_ctx) : MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace
} // namespace librbd

#include "librbd/ConfigWatcher.cc"

namespace librbd {

using ::testing::Invoke;

class TestMockConfigWatcher : public TestMockFixture {
public:
  typedef ConfigWatcher<MockTestImageCtx> MockConfigWatcher;

  librbd::ImageCtx *m_image_ctx;

  ceph::mutex m_lock = ceph::make_mutex("m_lock");
  ceph::condition_variable m_cv;
  bool m_refreshed = false;

  void SetUp() override {
    TestMockFixture::SetUp();

    ASSERT_EQ(0, open_image(m_image_name, &m_image_ctx));
  }

  void expect_update_notification(MockTestImageCtx& mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.state, handle_update_notification())
      .WillOnce(Invoke([this]() {
          std::unique_lock locker{m_lock};
          m_refreshed = true;
          m_cv.notify_all();
        }));
  }

  void wait_for_update_notification() {
    std::unique_lock locker{m_lock};
    m_cv.wait(locker, [this] {
        if (m_refreshed) {
          m_refreshed = false;
          return true;
        }
        return false;
      });
  }
};

TEST_F(TestMockConfigWatcher, GlobalConfig) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  MockConfigWatcher mock_config_watcher(mock_image_ctx);
  mock_config_watcher.init();

  expect_update_notification(mock_image_ctx);
  mock_image_ctx.cct->_conf.set_val("rbd_cache", "false");
  mock_image_ctx.cct->_conf.set_val("rbd_cache", "true");
  mock_image_ctx.cct->_conf.apply_changes(nullptr);
  wait_for_update_notification();

  mock_config_watcher.shut_down();
}

TEST_F(TestMockConfigWatcher, IgnoreOverriddenGlobalConfig) {
  MockTestImageCtx mock_image_ctx(*m_image_ctx);

  MockConfigWatcher mock_config_watcher(mock_image_ctx);
  mock_config_watcher.init();

  EXPECT_CALL(*mock_image_ctx.state, handle_update_notification())
    .Times(0);
  mock_image_ctx.config_overrides.insert("rbd_cache");
  mock_image_ctx.cct->_conf.set_val("rbd_cache", "false");
  mock_image_ctx.cct->_conf.set_val("rbd_cache", "true");
  mock_image_ctx.cct->_conf.apply_changes(nullptr);

  mock_config_watcher.shut_down();

  ASSERT_FALSE(m_refreshed);
}

} // namespace librbd
