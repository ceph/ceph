// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include <boost/scope_exit.hpp>
#include <boost/assign/list_of.hpp>
#include <utility>
#include <vector>

void register_test_mirroring() {
}

class TestMirroring : public TestFixture {
public:

  TestMirroring() {}


  virtual void TearDown() {
    unlock_image();

    TestFixture::TearDown();
  }

  std::string image_name = "mirrorimg1";

  void check_mirror_image_enable(uint64_t features,
                                 int expected_r,
                                 rbd_mirror_image_state_t mirror_state) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features, &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    ASSERT_EQ(expected_r, image.mirror_image_enable());

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_state, mirror_image.state);

    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  }

  void check_mirroring_on_create(uint64_t features,
                                 rbd_mirror_mode_t mirror_mode,
                                 rbd_mirror_image_state_t mirror_state) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features, &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_state, mirror_image.state);

    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  }

  void check_mirroring_on_update_features(uint64_t init_features,
                                 bool enable, bool enable_mirroring,
                                 uint64_t features, int expected_r,
                                 rbd_mirror_mode_t mirror_mode,
                                 rbd_mirror_image_state_t mirror_state) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, init_features, &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    if (enable_mirroring) {
      ASSERT_EQ(0, image.mirror_image_enable());
    }

    ASSERT_EQ(expected_r, image.update_features(features, enable));

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_state, mirror_image.state);
    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  }

};

TEST_F(TestMirroring, EnableImageMirror) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirror_image_enable(features, 0, RBD_MIRROR_IMAGE_ENABLED);
}

TEST_F(TestMirroring, EnableImageMirror_WithoutJournaling) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  check_mirror_image_enable(features, -EINVAL, RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, CreateImage_In_MirrorModeDisabled) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirroring_on_create(features, RBD_MIRROR_MODE_DISABLED,
                            RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, CreateImage_In_MirrorModeImage) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirroring_on_create(features, RBD_MIRROR_MODE_IMAGE,
                            RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, CreateImage_In_MirrorModePool) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirroring_on_create(features, RBD_MIRROR_MODE_POOL,
                            RBD_MIRROR_IMAGE_ENABLED);
}

TEST_F(TestMirroring, CreateImage_In_MirrorModePool_WithoutJournaling) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  check_mirroring_on_create(features, RBD_MIRROR_MODE_POOL,
                            RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, CreateImage_In_MirrorModeImage_WithoutJournaling) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  check_mirroring_on_create(features, RBD_MIRROR_MODE_IMAGE,
                            RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, EnableJournaling_In_MirrorModeDisabled) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  uint64_t features = init_features | RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, true, false, features, 0,
                      RBD_MIRROR_MODE_DISABLED, RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, EnableJournaling_In_MirrorModeImage) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  uint64_t features = init_features | RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, true, false, features, 0,
                      RBD_MIRROR_MODE_IMAGE, RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, EnableJournaling_In_MirrorModePool) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  uint64_t features = init_features | RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, true, false, features, 0,
                      RBD_MIRROR_MODE_POOL, RBD_MIRROR_IMAGE_ENABLED);
}

TEST_F(TestMirroring, DisableJournaling_In_MirrorModePool) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  init_features |= RBD_FEATURE_JOURNALING;
  uint64_t features = RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, false, false, features, 0,
                      RBD_MIRROR_MODE_POOL, RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, DisableJournaling_In_MirrorModeImage) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  init_features |= RBD_FEATURE_JOURNALING;
  uint64_t features = RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, false, true, features, -EINVAL,
                      RBD_MIRROR_MODE_IMAGE, RBD_MIRROR_IMAGE_ENABLED);
}

