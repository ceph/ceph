// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "tools/rbd_mirror/ImageDeleter.h"
#include "tools/rbd_mirror/ServiceDaemon.h"
#include "tools/rbd_mirror/Threads.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Journal.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/api/Mirror.h"
#include "librbd/journal/DisabledPolicy.h"
#include "test/rbd_mirror/test_fixture.h"

#include "test/librados/test.h"
#include "gtest/gtest.h"

#define GLOBAL_IMAGE_ID "global_image_id"
#define GLOBAL_CLONE_IMAGE_ID "global_image_id_clone"

#define dout_subsys ceph_subsys_rbd_mirror

using rbd::mirror::RadosRef;
using rbd::mirror::TestFixture;
using namespace librbd;
using cls::rbd::MirrorImageState;


void register_test_rbd_mirror_image_deleter() {
}

class TestImageDeleter : public TestFixture {
public:

  static int64_t m_local_pool_id;

  const std::string m_local_mirror_uuid = "local mirror uuid";
  const std::string m_remote_mirror_uuid = "remote mirror uuid";

  static void SetUpTestCase() {
    TestFixture::SetUpTestCase();

    m_local_pool_id = _rados->pool_lookup(_local_pool_name.c_str());
  }

  void SetUp() override {
    TestFixture::SetUp();
    m_service_daemon.reset(new rbd::mirror::ServiceDaemon<>(g_ceph_context,
                                                            _rados, m_threads));

    librbd::api::Mirror<>::mode_set(m_local_io_ctx, RBD_MIRROR_MODE_IMAGE);

    m_deleter = new rbd::mirror::ImageDeleter<>(m_threads->work_queue,
                                                m_threads->timer,
                                                &m_threads->timer_lock,
                                                m_service_daemon.get());

    m_local_image_id = librbd::util::generate_image_id(m_local_io_ctx);
    librbd::ImageOptions image_opts;
    image_opts.set(RBD_IMAGE_OPTION_FEATURES, RBD_FEATURES_ALL);
    EXPECT_EQ(0, librbd::create(m_local_io_ctx, m_image_name, m_local_image_id,
                                1 << 20, image_opts, GLOBAL_IMAGE_ID,
                                m_remote_mirror_uuid, true));

    cls::rbd::MirrorImage mirror_image(
      GLOBAL_IMAGE_ID, MirrorImageState::MIRROR_IMAGE_STATE_ENABLED);
    EXPECT_EQ(0, cls_client::mirror_image_set(&m_local_io_ctx, m_local_image_id,
                                              mirror_image));
  }

  void TearDown() override {
    remove_image();
    delete m_deleter;
    m_service_daemon.reset();

    TestFixture::TearDown();
  }

  void remove_image(bool force=false) {
    if (!force) {
      cls::rbd::MirrorImage mirror_image;
      int r = cls_client::mirror_image_get(&m_local_io_ctx, m_local_image_id,
                                           &mirror_image);
      EXPECT_EQ(1, r == 0 || r == -ENOENT);
      if (r != -ENOENT) {
        mirror_image.state = MirrorImageState::MIRROR_IMAGE_STATE_ENABLED;
        EXPECT_EQ(0, cls_client::mirror_image_set(&m_local_io_ctx,
                                                  m_local_image_id,
                                                  mirror_image));
      }
      promote_image();
    }
    NoOpProgressContext ctx;
    int r = remove(m_local_io_ctx, m_image_name, "", ctx, force);
    EXPECT_EQ(1, r == 0 || r == -ENOENT);
  }

  void promote_image(ImageCtx *ictx=nullptr) {
    bool close = false;
    int r = 0;
    if (!ictx) {
      ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                          false);
      r = ictx->state->open(false);
      close = (r == 0);
    }

    EXPECT_EQ(1, r == 0 || r == -ENOENT);

    if (r == 0) {
        int r2 = librbd::api::Mirror<>::image_promote(ictx, true);
        EXPECT_EQ(1, r2 == 0 || r2 == -EINVAL);
    }

    if (close) {
      EXPECT_EQ(0, ictx->state->close());
    }
  }

  void demote_image(ImageCtx *ictx=nullptr) {
    bool close = false;
    if (!ictx) {
      ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                          false);
      EXPECT_EQ(0, ictx->state->open(false));
      close = true;
    }

    EXPECT_EQ(0, librbd::api::Mirror<>::image_demote(ictx));

    if (close) {
      EXPECT_EQ(0, ictx->state->close());
    }
  }

  void create_snapshot(std::string snap_name="snap1", bool protect=false) {
    ImageCtx *ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                                  false);
    EXPECT_EQ(0, ictx->state->open(false));
    {
      RWLock::WLocker snap_locker(ictx->snap_lock);
      ictx->set_journal_policy(new librbd::journal::DisabledPolicy());
    }

    EXPECT_EQ(0, ictx->operations->snap_create(
                   cls::rbd::UserSnapshotNamespace(), snap_name.c_str()));

    if (protect) {
      EXPECT_EQ(0, ictx->operations->snap_protect(
                     cls::rbd::UserSnapshotNamespace(), snap_name.c_str()));
    }

    EXPECT_EQ(0, ictx->state->close());
  }

  std::string create_clone() {
    ImageCtx *ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                                  false);
    EXPECT_EQ(0, ictx->state->open(false));
    {
      RWLock::WLocker snap_locker(ictx->snap_lock);
      ictx->set_journal_policy(new librbd::journal::DisabledPolicy());
    }

    EXPECT_EQ(0, ictx->operations->snap_create(
                   cls::rbd::UserSnapshotNamespace(), "snap1"));
    EXPECT_EQ(0, ictx->operations->snap_protect(
                   cls::rbd::UserSnapshotNamespace(), "snap1"));
    EXPECT_EQ(0, librbd::snap_set(ictx, cls::rbd::UserSnapshotNamespace(),
                                  "snap1"));

    std::string clone_id = librbd::util::generate_image_id(m_local_io_ctx);
    librbd::ImageOptions clone_opts;
    clone_opts.set(RBD_IMAGE_OPTION_FEATURES, ictx->features);
    EXPECT_EQ(0, librbd::clone(ictx, m_local_io_ctx, "clone1", clone_id,
                               clone_opts, GLOBAL_CLONE_IMAGE_ID,
                               m_remote_mirror_uuid));

    cls::rbd::MirrorImage mirror_image(
      GLOBAL_CLONE_IMAGE_ID, MirrorImageState::MIRROR_IMAGE_STATE_ENABLED);
    EXPECT_EQ(0, cls_client::mirror_image_set(&m_local_io_ctx, clone_id,
                                              mirror_image));
    EXPECT_EQ(0, ictx->state->close());
    return clone_id;
  }

  void check_image_deleted() {
    ImageCtx *ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                                  false);
    EXPECT_EQ(-ENOENT, ictx->state->open(false));

    cls::rbd::MirrorImage mirror_image;
    EXPECT_EQ(-ENOENT, cls_client::mirror_image_get(&m_local_io_ctx,
                                                    m_local_image_id,
                                                    &mirror_image));
  }


  librbd::RBD rbd;
  std::string m_local_image_id;
  std::unique_ptr<rbd::mirror::ServiceDaemon<>> m_service_daemon;
  rbd::mirror::ImageDeleter<> *m_deleter;
};

int64_t TestImageDeleter::m_local_pool_id;


TEST_F(TestImageDeleter, Delete_NonPrimary_Image) {
  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());

  check_image_deleted();
}

TEST_F(TestImageDeleter, Delete_Split_Brain_Image) {
  promote_image();
  demote_image();

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   true);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());

  check_image_deleted();
}

TEST_F(TestImageDeleter, Fail_Delete_Primary_Image) {
  promote_image();

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(-rbd::mirror::ImageDeleter<>::EISPRM, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Fail_Delete_Orphan_Image) {
  promote_image();
  demote_image();

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(-rbd::mirror::ImageDeleter<>::EISPRM, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Delete_Image_With_Child) {
  create_snapshot();

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Delete_Image_With_Children) {
  create_snapshot("snap1");
  create_snapshot("snap2");

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Delete_Image_With_ProtectedChild) {
  create_snapshot("snap1", true);

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Delete_Image_With_ProtectedChildren) {
  create_snapshot("snap1", true);
  create_snapshot("snap2", true);

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Delete_Image_With_Clone) {
  std::string clone_id = create_clone();

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(-EBUSY, ctx.wait());

  ASSERT_EQ(1u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());

  m_deleter->schedule_image_delete(_rados, m_local_pool_id,
                                   GLOBAL_CLONE_IMAGE_ID, false);

  C_SaferCond ctx2;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_CLONE_IMAGE_ID,
                                         &ctx2);
  EXPECT_EQ(0, ctx2.wait());

  C_SaferCond ctx3;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx3);
  EXPECT_EQ(0, ctx3.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Delete_NonExistent_Image) {
  remove_image();

  cls::rbd::MirrorImage mirror_image(GLOBAL_IMAGE_ID,
                              MirrorImageState::MIRROR_IMAGE_STATE_ENABLED);
  EXPECT_EQ(0, cls_client::mirror_image_set(&m_local_io_ctx, m_local_image_id,
                                            mirror_image));

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());

  check_image_deleted();
}

TEST_F(TestImageDeleter, Delete_NonExistent_Image_With_MirroringState) {
  remove_image(true);

  cls::rbd::MirrorImage mirror_image(GLOBAL_IMAGE_ID,
                              MirrorImageState::MIRROR_IMAGE_STATE_ENABLED);
  EXPECT_EQ(0, cls_client::mirror_image_set(&m_local_io_ctx, m_local_image_id,
                                            mirror_image));
  mirror_image.state = MirrorImageState::MIRROR_IMAGE_STATE_DISABLING;
  EXPECT_EQ(0, cls_client::mirror_image_set(&m_local_io_ctx, m_local_image_id,
                                            mirror_image));

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());

  check_image_deleted();
}

TEST_F(TestImageDeleter, Delete_NonExistent_Image_Without_MirroringState) {
  remove_image();

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(-ENOENT, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());

  check_image_deleted();
}

TEST_F(TestImageDeleter, Fail_Delete_NonPrimary_Image) {
  ImageCtx *ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                                false);
  EXPECT_EQ(0, ictx->state->open(false));

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(-EBUSY, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(1u, m_deleter->get_failed_queue_items().size());

  EXPECT_EQ(0, ictx->state->close());
}

TEST_F(TestImageDeleter, Retry_Failed_Deletes) {
  ImageCtx *ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                                false);
  EXPECT_EQ(0, ictx->state->open(false));

  m_deleter->set_failed_timer_interval(2);

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(-EBUSY, ctx.wait());

  EXPECT_EQ(0, ictx->state->close());

  C_SaferCond ctx2;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx2);
  EXPECT_EQ(0, ctx2.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());

  check_image_deleted();
}

TEST_F(TestImageDeleter, Delete_Is_Idempotent) {
  ImageCtx *ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                                false);
  EXPECT_EQ(0, ictx->state->open(false));

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  C_SaferCond ctx;
  m_deleter->wait_for_scheduled_deletion(m_local_pool_id, GLOBAL_IMAGE_ID,
                                         &ctx);
  EXPECT_EQ(-EBUSY, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(1u, m_deleter->get_failed_queue_items().size());

  m_deleter->schedule_image_delete(_rados, m_local_pool_id, GLOBAL_IMAGE_ID,
                                   false);

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(1u, m_deleter->get_failed_queue_items().size());

  EXPECT_EQ(0, ictx->state->close());
}


