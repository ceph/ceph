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
#include "tools/rbd_mirror/Throttler.h"
#include "tools/rbd_mirror/Types.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Journal.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/api/Image.h"
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
using cls::rbd::MirrorImageMode;
using cls::rbd::MirrorImageState;


void register_test_rbd_mirror_image_deleter() {
}

class TestImageDeleter : public TestFixture {
public:
  const std::string m_local_mirror_uuid = "local mirror uuid";
  const std::string m_remote_mirror_uuid = "remote mirror uuid";

  void SetUp() override {
    TestFixture::SetUp();

    m_image_deletion_throttler.reset(
        new rbd::mirror::Throttler<>(g_ceph_context,
                                     "rbd_mirror_concurrent_image_deletions"));

    m_service_daemon.reset(new rbd::mirror::ServiceDaemon<>(g_ceph_context,
                                                            _rados, m_threads));

    librbd::api::Mirror<>::mode_set(m_local_io_ctx, RBD_MIRROR_MODE_IMAGE);

    m_deleter = new rbd::mirror::ImageDeleter<>(
        m_local_io_ctx, m_threads, m_image_deletion_throttler.get(),
        m_service_daemon.get());

    m_local_image_id = librbd::util::generate_image_id(m_local_io_ctx);
    librbd::ImageOptions image_opts;
    image_opts.set(RBD_IMAGE_OPTION_FEATURES,
                   (RBD_FEATURES_ALL & ~RBD_FEATURES_IMPLICIT_ENABLE));
    EXPECT_EQ(0, librbd::create(m_local_io_ctx, m_image_name, m_local_image_id,
                                1 << 20, image_opts, GLOBAL_IMAGE_ID,
                                m_remote_mirror_uuid, true));

    cls::rbd::MirrorImage mirror_image(
      MirrorImageMode::MIRROR_IMAGE_MODE_JOURNAL, GLOBAL_IMAGE_ID,
      MirrorImageState::MIRROR_IMAGE_STATE_ENABLED);
    EXPECT_EQ(0, cls_client::mirror_image_set(&m_local_io_ctx, m_local_image_id,
                                              mirror_image));
  }

  void TearDown() override {
    remove_image();

    C_SaferCond ctx;
    m_deleter->shut_down(&ctx);
    ctx.wait();

    delete m_deleter;
    m_service_daemon.reset();

    TestFixture::TearDown();
  }

  void init_image_deleter() {
    C_SaferCond ctx;
    m_deleter->init(&ctx);
    ASSERT_EQ(0, ctx.wait());
  }

  void remove_image() {
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

    NoOpProgressContext ctx;
    r = librbd::api::Image<>::remove(m_local_io_ctx, m_image_name, ctx);
    EXPECT_EQ(1, r == 0 || r == -ENOENT);
  }

  void promote_image(ImageCtx *ictx=nullptr) {
    bool close = false;
    int r = 0;
    if (!ictx) {
      ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                          false);
      r = ictx->state->open(0);
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
      EXPECT_EQ(0, ictx->state->open(0));
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
    EXPECT_EQ(0, ictx->state->open(0));
    {
      std::unique_lock image_locker{ictx->image_lock};
      ictx->set_journal_policy(new librbd::journal::DisabledPolicy());
    }

    librbd::NoOpProgressContext prog_ctx;
    EXPECT_EQ(0, ictx->operations->snap_create(
                   cls::rbd::UserSnapshotNamespace(), snap_name, 0, prog_ctx));

    if (protect) {
      EXPECT_EQ(0, ictx->operations->snap_protect(
                     cls::rbd::UserSnapshotNamespace(), snap_name));
    }

    EXPECT_EQ(0, ictx->state->close());
  }

  std::string create_clone() {
    ImageCtx *ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                                  false);
    EXPECT_EQ(0, ictx->state->open(0));
    {
      std::unique_lock image_locker{ictx->image_lock};
      ictx->set_journal_policy(new librbd::journal::DisabledPolicy());
    }

    librbd::NoOpProgressContext prog_ctx;
    EXPECT_EQ(0, ictx->operations->snap_create(
                   cls::rbd::UserSnapshotNamespace(), "snap1", 0, prog_ctx));
    EXPECT_EQ(0, ictx->operations->snap_protect(
                   cls::rbd::UserSnapshotNamespace(), "snap1"));
    EXPECT_EQ(0, librbd::api::Image<>::snap_set(
                   ictx, cls::rbd::UserSnapshotNamespace(), "snap1"));

    std::string clone_id = librbd::util::generate_image_id(m_local_io_ctx);
    librbd::ImageOptions clone_opts;
    clone_opts.set(RBD_IMAGE_OPTION_FEATURES, ictx->features);
    EXPECT_EQ(0, librbd::clone(m_local_io_ctx, m_local_image_id.c_str(),
                               nullptr, "snap1", m_local_io_ctx,
                               clone_id.c_str(), "clone1", clone_opts,
                               GLOBAL_CLONE_IMAGE_ID, m_remote_mirror_uuid));

    cls::rbd::MirrorImage mirror_image(
      MirrorImageMode::MIRROR_IMAGE_MODE_JOURNAL, GLOBAL_CLONE_IMAGE_ID,
      MirrorImageState::MIRROR_IMAGE_STATE_ENABLED);
    EXPECT_EQ(0, cls_client::mirror_image_set(&m_local_io_ctx, clone_id,
                                              mirror_image));
    EXPECT_EQ(0, ictx->state->close());
    return clone_id;
  }

  void check_image_deleted() {
    ImageCtx *ictx = new ImageCtx("", m_local_image_id, "", m_local_io_ctx,
                                  false);
    EXPECT_EQ(-ENOENT, ictx->state->open(0));

    cls::rbd::MirrorImage mirror_image;
    EXPECT_EQ(-ENOENT, cls_client::mirror_image_get(&m_local_io_ctx,
                                                    m_local_image_id,
                                                    &mirror_image));
  }

  int trash_move(const std::string& global_image_id) {
    C_SaferCond ctx;
    rbd::mirror::ImageDeleter<>::trash_move(m_local_io_ctx, global_image_id,
                                            true, m_threads->work_queue, &ctx);
    return ctx.wait();
  }

  librbd::RBD rbd;
  std::string m_local_image_id;
  std::unique_ptr<rbd::mirror::Throttler<>> m_image_deletion_throttler;
  std::unique_ptr<rbd::mirror::ServiceDaemon<>> m_service_daemon;
  rbd::mirror::ImageDeleter<> *m_deleter;
};

TEST_F(TestImageDeleter, ExistingTrashMove) {
  ASSERT_EQ(0, trash_move(GLOBAL_IMAGE_ID));

  C_SaferCond ctx;
  m_deleter->wait_for_deletion(m_local_image_id, false, &ctx);
  init_image_deleter();

  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestImageDeleter, LiveTrashMove) {
  init_image_deleter();

  C_SaferCond ctx;
  m_deleter->wait_for_deletion(m_local_image_id, false, &ctx);

  ASSERT_EQ(0, trash_move(GLOBAL_IMAGE_ID));
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestImageDeleter, Delete_Image_With_Snapshots) {
  init_image_deleter();
  create_snapshot("snap1");
  create_snapshot("snap2");

  C_SaferCond ctx;
  m_deleter->wait_for_deletion(m_local_image_id, false, &ctx);
  ASSERT_EQ(0, trash_move(GLOBAL_IMAGE_ID));
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Delete_Image_With_ProtectedSnapshots) {
  init_image_deleter();
  create_snapshot("snap1", true);
  create_snapshot("snap2", true);

  C_SaferCond ctx;
  m_deleter->wait_for_deletion(m_local_image_id, false, &ctx);
  ASSERT_EQ(0, trash_move(GLOBAL_IMAGE_ID));
  EXPECT_EQ(0, ctx.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

TEST_F(TestImageDeleter, Delete_Image_With_Clone) {
  init_image_deleter();
  std::string clone_id = create_clone();

  C_SaferCond ctx1;
  m_deleter->set_busy_timer_interval(0.1);
  m_deleter->wait_for_deletion(m_local_image_id, false, &ctx1);
  ASSERT_EQ(0, trash_move(GLOBAL_IMAGE_ID));
  EXPECT_EQ(-EBUSY, ctx1.wait());

  C_SaferCond ctx2;
  m_deleter->wait_for_deletion(clone_id, false, &ctx2);
  ASSERT_EQ(0, trash_move(GLOBAL_CLONE_IMAGE_ID));
  EXPECT_EQ(0, ctx2.wait());

  C_SaferCond ctx3;
  m_deleter->wait_for_deletion(m_local_image_id, true, &ctx3);
  EXPECT_EQ(0, ctx3.wait());

  ASSERT_EQ(0u, m_deleter->get_delete_queue_items().size());
  ASSERT_EQ(0u, m_deleter->get_failed_queue_items().size());
}

