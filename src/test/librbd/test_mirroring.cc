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
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Operations.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/journal/Types.h"
#include "librbd/api/Image.h"
#include "journal/Journaler.h"
#include "journal/Settings.h"
#include <boost/scope_exit.hpp>
#include <boost/assign/list_of.hpp>
#include <utility>
#include <vector>

void register_test_mirroring() {
}

class TestMirroring : public TestFixture {
public:

  TestMirroring() {}


  void TearDown() override {
    unlock_image();

    TestFixture::TearDown();
  }

  void SetUp() override {
    ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), m_ioctx));
  }

  std::string image_name = "mirrorimg1";

  void check_mirror_image_enable(rbd_mirror_mode_t mirror_mode,
                                 uint64_t features,
                                 int expected_r,
                                 rbd_mirror_image_state_t mirror_state) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features, &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    ASSERT_EQ(expected_r, image.mirror_image_enable());

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_state, mirror_image.state);

    librbd::mirror_image_status_t status;
    ASSERT_EQ(0, image.mirror_image_get_status(&status, sizeof(status)));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status.state);

    std::string instance_id;
    ASSERT_EQ(mirror_state == RBD_MIRROR_IMAGE_ENABLED ? -ENOENT : -EINVAL,
              image.mirror_image_get_instance_id(&instance_id));

    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
  }

  void check_mirror_image_disable(rbd_mirror_mode_t mirror_mode,
                                  uint64_t features,
                                  int expected_r,
                                  rbd_mirror_image_state_t mirror_state) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_POOL));

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features, &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    ASSERT_EQ(expected_r, image.mirror_image_disable(false));

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_state, mirror_image.state);

    librbd::mirror_image_status_t status;
    ASSERT_EQ(0, image.mirror_image_get_status(&status, sizeof(status)));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status.state);

    std::string instance_id;
    ASSERT_EQ(mirror_state == RBD_MIRROR_IMAGE_ENABLED ? -ENOENT : -EINVAL,
              image.mirror_image_get_instance_id(&instance_id));

    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
  }

  void check_mirroring_status(size_t *images_count) {
    std::map<std::string, librbd::mirror_image_status_t> images;
    ASSERT_EQ(0, m_rbd.mirror_image_status_list(m_ioctx, "", 4096, &images));

    std::map<librbd::mirror_image_status_state_t, int> states;
    ASSERT_EQ(0, m_rbd.mirror_image_status_summary(m_ioctx, &states));
    size_t states_count = 0;
    for (auto &s : states) {
      states_count += s.second;
    }
    ASSERT_EQ(images.size(), states_count);

    *images_count = images.size();

    std::map<std::string, std::string> instance_ids;
    ASSERT_EQ(0, m_rbd.mirror_image_instance_id_list(m_ioctx, "", 4096,
                                                     &instance_ids));
    ASSERT_TRUE(instance_ids.empty());
  }

  void check_mirroring_on_create(uint64_t features,
                                 rbd_mirror_mode_t mirror_mode,
                                 rbd_mirror_image_state_t mirror_state) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    size_t mirror_images_count = 0;
    check_mirroring_status(&mirror_images_count);

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features, &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_state, mirror_image.state);

    librbd::mirror_image_status_t status;
    ASSERT_EQ(0, image.mirror_image_get_status(&status, sizeof(status)));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status.state);

    size_t mirror_images_new_count = 0;
    check_mirroring_status(&mirror_images_new_count);
    if (mirror_mode == RBD_MIRROR_MODE_POOL &&
	mirror_state == RBD_MIRROR_IMAGE_ENABLED) {
      ASSERT_EQ(mirror_images_new_count, mirror_images_count + 1);
    } else {
      ASSERT_EQ(mirror_images_new_count, mirror_images_count);
    }

    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));

    check_mirroring_status(&mirror_images_new_count);
    ASSERT_EQ(mirror_images_new_count, mirror_images_count);
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

    librbd::mirror_image_status_t status;
    ASSERT_EQ(0, image.mirror_image_get_status(&status, sizeof(status)));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status.state);

    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
  }

  void setup_images_with_mirror_mode(rbd_mirror_mode_t mirror_mode,
                                        std::vector<uint64_t>& features_vec) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    int id = 1;
    int order = 20;
    for (const auto& features : features_vec) {
      std::stringstream img_name("img_");
      img_name << id++;
      std::string img_name_str = img_name.str();
      ASSERT_EQ(0, m_rbd.create2(m_ioctx, img_name_str.c_str(), 2048, features, &order));
    }
  }

  void check_mirroring_on_mirror_mode_set(rbd_mirror_mode_t mirror_mode,
                            std::vector<rbd_mirror_image_state_t>& states_vec) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    std::vector< std::tuple<std::string, rbd_mirror_image_state_t> > images;
    int id = 1;
    for (const auto& mirror_state : states_vec) {
      std::stringstream img_name("img_");
      img_name << id++;
      std::string img_name_str = img_name.str();
      librbd::Image image;
      ASSERT_EQ(0, m_rbd.open(m_ioctx, image, img_name_str.c_str()));
      images.push_back(std::make_tuple(img_name_str, mirror_state));

      librbd::mirror_image_info_t mirror_image;
      ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
      ASSERT_EQ(mirror_state, mirror_image.state);

      librbd::mirror_image_status_t status;
      ASSERT_EQ(0, image.mirror_image_get_status(&status, sizeof(status)));
      ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status.state);

      ASSERT_EQ(0, image.close());
      ASSERT_EQ(0, m_rbd.remove(m_ioctx, img_name_str.c_str()));
    }
  }

  void check_remove_image(rbd_mirror_mode_t mirror_mode, uint64_t features,
                          bool enable_mirroring, bool demote = false) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
              &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    if (enable_mirroring) {
      ASSERT_EQ(0, image.mirror_image_enable());
    }

    if (demote) {
      ASSERT_EQ(0, image.mirror_image_demote());
      ASSERT_EQ(0, image.mirror_image_disable(true));
    }

    image.close();
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
  }

  void check_trash_move_restore(rbd_mirror_mode_t mirror_mode,
                                bool enable_mirroring) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    int order = 20;
    uint64_t features = RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                               &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    if (enable_mirroring) {
      ASSERT_EQ(0, image.mirror_image_enable());
    }

    std::string image_id;
    ASSERT_EQ(0, image.get_id(&image_id));
    image.close();

    ASSERT_EQ(0, m_rbd.trash_move(m_ioctx, image_name.c_str(), 100));

    ASSERT_EQ(0, m_rbd.open_by_id(m_ioctx, image, image_id.c_str(), NULL));

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_image.state, RBD_MIRROR_IMAGE_DISABLED);

    ASSERT_EQ(0, m_rbd.trash_restore(m_ioctx, image_id.c_str(), ""));

    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    if (mirror_mode == RBD_MIRROR_MODE_POOL) {
      ASSERT_EQ(mirror_image.state, RBD_MIRROR_IMAGE_ENABLED);
    } else {
      ASSERT_EQ(mirror_image.state, RBD_MIRROR_IMAGE_DISABLED);
    }

    image.close();
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
  }

  void setup_mirror_peer(librados::IoCtx &io_ctx, librbd::Image &image) {
    ASSERT_EQ(0, image.snap_create("sync-point-snap"));

    std::string image_id;
    ASSERT_EQ(0, get_image_id(image, &image_id));

    librbd::journal::MirrorPeerClientMeta peer_client_meta(
      "remote-image-id", {{{}, "sync-point-snap", boost::none}}, {});
    librbd::journal::ClientData client_data(peer_client_meta);

    journal::Journaler journaler(io_ctx, image_id, "peer-client", {});
    C_SaferCond init_ctx;
    journaler.init(&init_ctx);
    ASSERT_EQ(-ENOENT, init_ctx.wait());

    bufferlist client_data_bl;
    encode(client_data, client_data_bl);
    ASSERT_EQ(0, journaler.register_client(client_data_bl));

    C_SaferCond shut_down_ctx;
    journaler.shut_down(&shut_down_ctx);
    ASSERT_EQ(0, shut_down_ctx.wait());
  }

};

TEST_F(TestMirroring, EnableImageMirror_In_MirrorModeImage) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirror_image_enable(RBD_MIRROR_MODE_IMAGE, features, 0,
      RBD_MIRROR_IMAGE_ENABLED);
}

TEST_F(TestMirroring, EnableImageMirror_In_MirrorModePool) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirror_image_enable(RBD_MIRROR_MODE_POOL, features, -EINVAL,
      RBD_MIRROR_IMAGE_ENABLED);
}

TEST_F(TestMirroring, EnableImageMirror_In_MirrorModeDisabled) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirror_image_enable(RBD_MIRROR_MODE_DISABLED, features, -EINVAL,
      RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, DisableImageMirror_In_MirrorModeImage) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirror_image_disable(RBD_MIRROR_MODE_IMAGE, features, 0,
      RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, DisableImageMirror_In_MirrorModePool) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirror_image_disable(RBD_MIRROR_MODE_POOL, features, -EINVAL,
      RBD_MIRROR_IMAGE_ENABLED);
}

TEST_F(TestMirroring, DisableImageMirror_In_MirrorModeDisabled) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirror_image_disable(RBD_MIRROR_MODE_DISABLED, features, -EINVAL,
      RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, DisableImageMirrorWithPeer) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  uint64_t features = RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
  int order = 20;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                             &order));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));
  ASSERT_EQ(0, image.mirror_image_enable());

  setup_mirror_peer(m_ioctx, image);

  ASSERT_EQ(0, image.mirror_image_disable(false));

  std::vector<librbd::snap_info_t> snaps;
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_TRUE(snaps.empty());

  librbd::mirror_image_info_t mirror_image;
  ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image,
                                           sizeof(mirror_image)));
  ASSERT_EQ(RBD_MIRROR_IMAGE_DISABLED, mirror_image.state);

  librbd::mirror_image_status_t status;
  ASSERT_EQ(0, image.mirror_image_get_status(&status, sizeof(status)));
  ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status.state);

  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestMirroring, DisableJournalingWithPeer) {
  REQUIRE_FEATURE(RBD_FEATURE_JOURNALING);

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_POOL));

  uint64_t features = RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
  int order = 20;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                             &order));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

  setup_mirror_peer(m_ioctx, image);

  ASSERT_EQ(0, image.update_features(RBD_FEATURE_JOURNALING, false));

  std::vector<librbd::snap_info_t> snaps;
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_TRUE(snaps.empty());

  librbd::mirror_image_info_t mirror_image;
  ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image,
                                           sizeof(mirror_image)));
  ASSERT_EQ(RBD_MIRROR_IMAGE_DISABLED, mirror_image.state);

  librbd::mirror_image_status_t status;
  ASSERT_EQ(0, image.mirror_image_get_status(&status, sizeof(status)));
  ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status.state);

  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestMirroring, EnableImageMirror_WithoutJournaling) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  check_mirror_image_enable(RBD_MIRROR_MODE_DISABLED, features, -EINVAL,
      RBD_MIRROR_IMAGE_DISABLED);
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
  uint64_t features = RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, true, false, features, 0,
                      RBD_MIRROR_MODE_DISABLED, RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, EnableJournaling_In_MirrorModeImage) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  uint64_t features = RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, true, false, features, 0,
                      RBD_MIRROR_MODE_IMAGE, RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, EnableJournaling_In_MirrorModePool) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  uint64_t features = RBD_FEATURE_JOURNALING;
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

TEST_F(TestMirroring, MirrorModeSet_DisabledMode_To_PoolMode) {
  std::vector<uint64_t> features_vec;
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK);
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING);

  setup_images_with_mirror_mode(RBD_MIRROR_MODE_DISABLED, features_vec);

  std::vector<rbd_mirror_image_state_t> states_vec;
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  states_vec.push_back(RBD_MIRROR_IMAGE_ENABLED);
  check_mirroring_on_mirror_mode_set(RBD_MIRROR_MODE_POOL, states_vec);
}

TEST_F(TestMirroring, MirrorModeSet_PoolMode_To_DisabledMode) {
  std::vector<uint64_t> features_vec;
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK);
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING);

  setup_images_with_mirror_mode(RBD_MIRROR_MODE_POOL, features_vec);

  std::vector<rbd_mirror_image_state_t> states_vec;
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  check_mirroring_on_mirror_mode_set(RBD_MIRROR_MODE_DISABLED, states_vec);
}

TEST_F(TestMirroring, MirrorModeSet_DisabledMode_To_ImageMode) {
  std::vector<uint64_t> features_vec;
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK);
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING);

  setup_images_with_mirror_mode(RBD_MIRROR_MODE_DISABLED, features_vec);

  std::vector<rbd_mirror_image_state_t> states_vec;
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  check_mirroring_on_mirror_mode_set(RBD_MIRROR_MODE_IMAGE, states_vec);
}


TEST_F(TestMirroring, MirrorModeSet_PoolMode_To_ImageMode) {
  std::vector<uint64_t> features_vec;
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK);
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING);

  setup_images_with_mirror_mode(RBD_MIRROR_MODE_POOL, features_vec);

  std::vector<rbd_mirror_image_state_t> states_vec;
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  states_vec.push_back(RBD_MIRROR_IMAGE_ENABLED);
  check_mirroring_on_mirror_mode_set(RBD_MIRROR_MODE_IMAGE, states_vec);
}

TEST_F(TestMirroring, MirrorModeSet_ImageMode_To_PoolMode) {
  std::vector<uint64_t> features_vec;
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK);
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING);

  setup_images_with_mirror_mode(RBD_MIRROR_MODE_IMAGE, features_vec);

  std::vector<rbd_mirror_image_state_t> states_vec;
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  states_vec.push_back(RBD_MIRROR_IMAGE_ENABLED);
  check_mirroring_on_mirror_mode_set(RBD_MIRROR_MODE_POOL, states_vec);
}

TEST_F(TestMirroring, MirrorModeSet_ImageMode_To_DisabledMode) {
  std::vector<uint64_t> features_vec;
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK);
  features_vec.push_back(RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING);

  setup_images_with_mirror_mode(RBD_MIRROR_MODE_POOL, features_vec);

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  ASSERT_EQ(-EINVAL, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_POOL));

  std::vector<rbd_mirror_image_state_t> states_vec;
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  states_vec.push_back(RBD_MIRROR_IMAGE_DISABLED);
  check_mirroring_on_mirror_mode_set(RBD_MIRROR_MODE_DISABLED, states_vec);
}

TEST_F(TestMirroring, RemoveImage_With_MirrorImageEnabled) {
  check_remove_image(RBD_MIRROR_MODE_IMAGE,
                     RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING,
                     true);
}

TEST_F(TestMirroring, RemoveImage_With_MirrorImageDisabled) {
  check_remove_image(RBD_MIRROR_MODE_IMAGE,
                     RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING,
                     false);
}

TEST_F(TestMirroring, RemoveImage_With_ImageWithoutJournal) {
  check_remove_image(RBD_MIRROR_MODE_IMAGE,
                     RBD_FEATURE_EXCLUSIVE_LOCK,
                     false);
}

TEST_F(TestMirroring, RemoveImage_With_MirrorImageDemoted) {
  check_remove_image(RBD_MIRROR_MODE_IMAGE,
                     RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING,
                     true, true);
}

TEST_F(TestMirroring, TrashMoveRestore_PoolMode) {
  check_trash_move_restore(RBD_MIRROR_MODE_POOL, false);
}

TEST_F(TestMirroring, TrashMoveRestore_ImageMode_MirroringDisabled) {
  check_trash_move_restore(RBD_MIRROR_MODE_IMAGE, false);
}

TEST_F(TestMirroring, TrashMoveRestore_ImageMode_MirroringEnabled) {
  check_trash_move_restore(RBD_MIRROR_MODE_IMAGE, true);
}

TEST_F(TestMirroring, MirrorStatusList) {
  std::vector<uint64_t>
      features_vec(5, RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING);
  setup_images_with_mirror_mode(RBD_MIRROR_MODE_POOL, features_vec);

  std::string last_read = "";
  std::map<std::string, librbd::mirror_image_status_t> images;
  ASSERT_EQ(0, m_rbd.mirror_image_status_list(m_ioctx, last_read, 2, &images));
  ASSERT_EQ(2U, images.size());

  last_read = images.rbegin()->first;
  images.clear();
  ASSERT_EQ(0, m_rbd.mirror_image_status_list(m_ioctx, last_read, 2, &images));
  ASSERT_EQ(2U, images.size());

  last_read = images.rbegin()->first;
  images.clear();
  ASSERT_EQ(0, m_rbd.mirror_image_status_list(m_ioctx, last_read, 4096, &images));
  ASSERT_EQ(1U, images.size());

  last_read = images.rbegin()->first;
  images.clear();
  ASSERT_EQ(0, m_rbd.mirror_image_status_list(m_ioctx, last_read, 4096, &images));
  ASSERT_EQ(0U, images.size());
}

TEST_F(TestMirroring, RemoveBootstrapped)
{
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_POOL));

  uint64_t features = RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
  int order = 20;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                             &order));
  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

  librbd::NoOpProgressContext no_op;
  ASSERT_EQ(-EBUSY, librbd::api::Image<>::remove(m_ioctx, image_name, no_op));

  // simulate the image is open by rbd-mirror bootstrap
  uint64_t handle;
  struct MirrorWatcher : public librados::WatchCtx2 {
    explicit MirrorWatcher(librados::IoCtx &ioctx) : m_ioctx(ioctx) {
    }
    void handle_notify(uint64_t notify_id, uint64_t cookie,
                               uint64_t notifier_id, bufferlist& bl) override {
      // received IMAGE_UPDATED notification from remove
      m_notified = true;
      m_ioctx.notify_ack(RBD_MIRRORING, notify_id, cookie, bl);
    }
    void handle_error(uint64_t cookie, int err) override {
    }
    librados::IoCtx &m_ioctx;
    bool m_notified = false;
  } watcher(m_ioctx);
  ASSERT_EQ(0, m_ioctx.create(RBD_MIRRORING, false));
  ASSERT_EQ(0, m_ioctx.watch2(RBD_MIRRORING, &handle, &watcher));
  // now remove should succeed
  ASSERT_EQ(0, librbd::api::Image<>::remove(m_ioctx, image_name, no_op));
  ASSERT_EQ(0, m_ioctx.unwatch2(handle));
  ASSERT_TRUE(watcher.m_notified);
  ASSERT_EQ(0, image.close());
}

TEST_F(TestMirroring, AioPromoteDemote) {
  std::list<std::string> image_names;
  for (size_t idx = 0; idx < 10; ++idx) {
    image_names.push_back(get_temp_image_name());
  }

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  // create mirror images
  int order = 20;
  std::list<librbd::Image> images;
  for (auto &image_name : image_names) {
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 2048,
                               RBD_FEATURE_EXCLUSIVE_LOCK |
                                 RBD_FEATURE_JOURNALING,
                               &order));

    images.emplace_back();
    ASSERT_EQ(0, m_rbd.open(m_ioctx, images.back(), image_name.c_str()));
    ASSERT_EQ(0, images.back().mirror_image_enable());
  }

  // demote all images
  std::list<librbd::RBD::AioCompletion *> aio_comps;
  for (auto &image : images) {
    aio_comps.push_back(new librbd::RBD::AioCompletion(nullptr, nullptr));
    ASSERT_EQ(0, image.aio_mirror_image_demote(aio_comps.back()));
  }
  for (auto aio_comp : aio_comps) {
    ASSERT_EQ(0, aio_comp->wait_for_complete());
    ASSERT_EQ(1, aio_comp->is_complete());
    ASSERT_EQ(0, aio_comp->get_return_value());
    aio_comp->release();
  }
  aio_comps.clear();

  // verify demotions
  for (auto &image : images) {
    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image,
                                             sizeof(mirror_image)));
    ASSERT_FALSE(mirror_image.primary);
  }

  // promote all images
  for (auto &image : images) {
    aio_comps.push_back(new librbd::RBD::AioCompletion(nullptr, nullptr));
    ASSERT_EQ(0, image.aio_mirror_image_promote(false, aio_comps.back()));
  }
  for (auto aio_comp : aio_comps) {
    ASSERT_EQ(0, aio_comp->wait_for_complete());
    ASSERT_EQ(1, aio_comp->is_complete());
    ASSERT_EQ(0, aio_comp->get_return_value());
    aio_comp->release();
  }

  // verify promotions
  for (auto &image : images) {
    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image,
                                             sizeof(mirror_image)));
    ASSERT_TRUE(mirror_image.primary);
  }
}

TEST_F(TestMirroring, AioGetInfo) {
  std::list<std::string> image_names;
  for (size_t idx = 0; idx < 10; ++idx) {
    image_names.push_back(get_temp_image_name());
  }

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_POOL));

  // create mirror images
  int order = 20;
  std::list<librbd::Image> images;
  for (auto &image_name : image_names) {
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 2048,
                               RBD_FEATURE_EXCLUSIVE_LOCK |
                                 RBD_FEATURE_JOURNALING,
                               &order));

    images.emplace_back();
    ASSERT_EQ(0, m_rbd.open(m_ioctx, images.back(), image_name.c_str()));
  }

  std::list<librbd::RBD::AioCompletion *> aio_comps;
  std::list<librbd::mirror_image_info_t> infos;
  for (auto &image : images) {
    aio_comps.push_back(new librbd::RBD::AioCompletion(nullptr, nullptr));
    infos.emplace_back();
    ASSERT_EQ(0, image.aio_mirror_image_get_info(&infos.back(),
                                                 sizeof(infos.back()),
                                                 aio_comps.back()));
  }
  for (auto aio_comp : aio_comps) {
    ASSERT_EQ(0, aio_comp->wait_for_complete());
    ASSERT_EQ(1, aio_comp->is_complete());
    ASSERT_EQ(0, aio_comp->get_return_value());
    aio_comp->release();
  }
  aio_comps.clear();

  for (auto &info : infos) {
    ASSERT_NE("", info.global_id);
    ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, info.state);
    ASSERT_TRUE(info.primary);
  }
}

TEST_F(TestMirroring, AioGetStatus) {
  std::list<std::string> image_names;
  for (size_t idx = 0; idx < 10; ++idx) {
    image_names.push_back(get_temp_image_name());
  }

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_POOL));

  // create mirror images
  int order = 20;
  std::list<librbd::Image> images;
  for (auto &image_name : image_names) {
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 2048,
                               RBD_FEATURE_EXCLUSIVE_LOCK |
                                 RBD_FEATURE_JOURNALING,
                               &order));

    images.emplace_back();
    ASSERT_EQ(0, m_rbd.open(m_ioctx, images.back(), image_name.c_str()));
  }

  std::list<librbd::RBD::AioCompletion *> aio_comps;
  std::list<librbd::mirror_image_status_t> statuses;
  for (auto &image : images) {
    aio_comps.push_back(new librbd::RBD::AioCompletion(nullptr, nullptr));
    statuses.emplace_back();
    ASSERT_EQ(0, image.aio_mirror_image_get_status(&statuses.back(),
                                                   sizeof(statuses.back()),
                                                   aio_comps.back()));
  }
  for (auto aio_comp : aio_comps) {
    ASSERT_EQ(0, aio_comp->wait_for_complete());
    ASSERT_EQ(1, aio_comp->is_complete());
    ASSERT_EQ(0, aio_comp->get_return_value());
    aio_comp->release();
  }
  aio_comps.clear();

  for (auto &status : statuses) {
    ASSERT_NE("", status.name);
    ASSERT_NE("", status.info.global_id);
    ASSERT_EQ(RBD_MIRROR_IMAGE_ENABLED, status.info.state);
    ASSERT_TRUE(status.info.primary);
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, status.state);
    ASSERT_EQ("status not found", status.description);
    ASSERT_FALSE(status.up);
    ASSERT_EQ(0, status.last_update);
  }
}
