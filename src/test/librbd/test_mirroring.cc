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
#include "librbd/api/Image.h"
#include "librbd/api/Namespace.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/journal/Types.h"
#include "librbd/mirror/snapshot/GetImageStateRequest.h"
#include "librbd/mirror/snapshot/RemoveImageStateRequest.h"
#include "librbd/mirror/snapshot/SetImageStateRequest.h"
#include "librbd/mirror/snapshot/UnlinkPeerRequest.h"
#include "journal/Journaler.h"
#include "journal/Settings.h"
#include "common/Cond.h"
#include <boost/scope_exit.hpp>
#include <boost/assign/list_of.hpp>
#include <utility>
#include <vector>

void register_test_mirroring() {
}

namespace librbd {

static bool operator==(const mirror_peer_site_t& lhs,
                       const mirror_peer_site_t& rhs) {
  return (lhs.uuid == rhs.uuid &&
          lhs.direction == rhs.direction &&
          lhs.site_name == rhs.site_name &&
          lhs.client_name == rhs.client_name &&
          lhs.last_seen == rhs.last_seen);
}

static std::ostream& operator<<(std::ostream& os,
                                const mirror_peer_site_t& rhs) {
  os << "uuid=" << rhs.uuid << ", "
     << "direction=" << rhs.direction << ", "
     << "site_name=" << rhs.site_name << ", "
     << "client_name=" << rhs.client_name << ", "
     << "last_seen=" << rhs.last_seen;
  return os;
}

};

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

  int get_local_mirror_image_site_status(
      const librbd::mirror_image_global_status_t& status,
      librbd::mirror_image_site_status_t* local_status) {
    auto it = std::find_if(status.site_statuses.begin(),
                           status.site_statuses.end(),
                           [](auto& site_status) {
        return (site_status.mirror_uuid ==
                  RBD_MIRROR_IMAGE_STATUS_LOCAL_MIRROR_UUID);
      });
    if (it == status.site_statuses.end()) {
      return -ENOENT;
    }

    *local_status = *it;
    return 0;
  }

  void check_mirror_image_enable(
      rbd_mirror_mode_t mirror_mode, uint64_t features, int expected_r,
      rbd_mirror_image_state_t mirror_state,
      rbd_mirror_image_mode_t mirror_image_mode = RBD_MIRROR_IMAGE_MODE_JOURNAL) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features, &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    ASSERT_EQ(expected_r, image.mirror_image_enable2(mirror_image_mode));

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_state, mirror_image.state);

    if (mirror_image.state == RBD_MIRROR_IMAGE_ENABLED) {
      librbd::mirror_image_mode_t mode;
      ASSERT_EQ(0, image.mirror_image_get_mode(&mode));
      ASSERT_EQ(mirror_image_mode, mode);
    }

    librbd::mirror_image_global_status_t status;
    ASSERT_EQ(0, image.mirror_image_get_global_status(&status, sizeof(status)));
    librbd::mirror_image_site_status_t local_status;
    ASSERT_EQ(0, get_local_mirror_image_site_status(status, &local_status));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, local_status.state);

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

    librbd::mirror_image_global_status_t status;
    ASSERT_EQ(0, image.mirror_image_get_global_status(&status, sizeof(status)));
    librbd::mirror_image_site_status_t local_status;
    ASSERT_EQ(0, get_local_mirror_image_site_status(status, &local_status));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, local_status.state);

    std::string instance_id;
    ASSERT_EQ(mirror_state == RBD_MIRROR_IMAGE_ENABLED ? -ENOENT : -EINVAL,
              image.mirror_image_get_instance_id(&instance_id));

    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
  }

  void check_mirroring_status(size_t *images_count) {
    std::map<std::string, librbd::mirror_image_global_status_t> images;
    ASSERT_EQ(0, m_rbd.mirror_image_global_status_list(m_ioctx, "", 4096,
                                                       &images));

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

    librbd::mirror_image_global_status_t status;
    ASSERT_EQ(0, image.mirror_image_get_global_status(&status, sizeof(status)));
    librbd::mirror_image_site_status_t local_status;
    ASSERT_EQ(0, get_local_mirror_image_site_status(status, &local_status));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, local_status.state);

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

  void check_mirroring_on_update_features(
      uint64_t init_features, bool enable, bool enable_mirroring,
      uint64_t features, int expected_r, rbd_mirror_mode_t mirror_mode,
      rbd_mirror_image_state_t mirror_state,
      rbd_mirror_image_mode_t mirror_image_mode = RBD_MIRROR_IMAGE_MODE_JOURNAL) {

    ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, mirror_mode));

    int order = 20;
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, init_features, &order));
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

    if (enable_mirroring) {
      ASSERT_EQ(0, image.mirror_image_enable2(mirror_image_mode));
    }

    ASSERT_EQ(expected_r, image.update_features(features, enable));

    librbd::mirror_image_info_t mirror_image;
    ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image, sizeof(mirror_image)));
    ASSERT_EQ(mirror_state, mirror_image.state);

    if (mirror_image.state == RBD_MIRROR_IMAGE_ENABLED) {
      librbd::mirror_image_mode_t mode;
      ASSERT_EQ(0, image.mirror_image_get_mode(&mode));
      ASSERT_EQ(mirror_image_mode, mode);
    }

    librbd::mirror_image_global_status_t status;
    ASSERT_EQ(0, image.mirror_image_get_global_status(&status, sizeof(status)));
    librbd::mirror_image_site_status_t local_status;
    ASSERT_EQ(0, get_local_mirror_image_site_status(status, &local_status));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, local_status.state);

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

      librbd::mirror_image_global_status_t status;
      ASSERT_EQ(0, image.mirror_image_get_global_status(&status,
                                                        sizeof(status)));
      librbd::mirror_image_site_status_t local_status;
      ASSERT_EQ(0, get_local_mirror_image_site_status(status, &local_status));
      ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, local_status.state);

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
      ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_JOURNAL));
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
      ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_JOURNAL));
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

    journal::Journaler journaler(io_ctx, image_id, "peer-client", {}, nullptr);
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
      RBD_MIRROR_IMAGE_ENABLED, RBD_MIRROR_IMAGE_MODE_JOURNAL);
}

TEST_F(TestMirroring, EnableImageMirror_In_MirrorModePool) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  features |= RBD_FEATURE_JOURNALING;
  check_mirror_image_enable(RBD_MIRROR_MODE_POOL, features, -EINVAL,
      RBD_MIRROR_IMAGE_ENABLED, RBD_MIRROR_IMAGE_MODE_JOURNAL);
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

TEST_F(TestMirroring, DisableImageMirror_In_MirrorModeImage_NoObjectMap) {
  uint64_t features = 0;
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
  ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_JOURNAL));

  setup_mirror_peer(m_ioctx, image);

  ASSERT_EQ(0, image.mirror_image_disable(false));

  std::vector<librbd::snap_info_t> snaps;
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_TRUE(snaps.empty());

  librbd::mirror_image_info_t mirror_image;
  ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image,
                                           sizeof(mirror_image)));
  ASSERT_EQ(RBD_MIRROR_IMAGE_DISABLED, mirror_image.state);

  librbd::mirror_image_global_status_t status;
  ASSERT_EQ(0, image.mirror_image_get_global_status(&status, sizeof(status)));
  librbd::mirror_image_site_status_t local_status;
  ASSERT_EQ(0, get_local_mirror_image_site_status(status, &local_status));
  ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, local_status.state);

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

  librbd::mirror_image_global_status_t status;
  ASSERT_EQ(0, image.mirror_image_get_global_status(&status, sizeof(status)));
  librbd::mirror_image_site_status_t local_status;
  ASSERT_EQ(0, get_local_mirror_image_site_status(status, &local_status));
  ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, local_status.state);

  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestMirroring, EnableImageMirror_In_MirrorModeDisabled_WithoutJournaling) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  check_mirror_image_enable(RBD_MIRROR_MODE_DISABLED, features, -EINVAL,
      RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, EnableImageMirror_In_MirrorModePool_WithoutJournaling) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  check_mirror_image_enable(RBD_MIRROR_MODE_POOL, features, -EINVAL,
      RBD_MIRROR_IMAGE_DISABLED);
}

TEST_F(TestMirroring, EnableImageMirror_In_MirrorModeImage_WithoutJournaling) {
  uint64_t features = 0;
  features |= RBD_FEATURE_OBJECT_MAP;
  features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  check_mirror_image_enable(RBD_MIRROR_MODE_IMAGE, features, 0,
      RBD_MIRROR_IMAGE_ENABLED, RBD_MIRROR_IMAGE_MODE_SNAPSHOT);
}

TEST_F(TestMirroring, EnableImageMirror_In_MirrorModeImage_WithoutExclusiveLock) {
  uint64_t features = 0;
  check_mirror_image_enable(RBD_MIRROR_MODE_IMAGE, features, 0,
      RBD_MIRROR_IMAGE_ENABLED, RBD_MIRROR_IMAGE_MODE_SNAPSHOT);
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

TEST_F(TestMirroring, EnableJournaling_In_MirrorModeImage_SnapshotMirroringEnabled) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  uint64_t features = RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, true, true, features,
                      0, RBD_MIRROR_MODE_IMAGE, RBD_MIRROR_IMAGE_ENABLED,
                      RBD_MIRROR_IMAGE_MODE_SNAPSHOT);
}

TEST_F(TestMirroring, EnableJournaling_In_MirrorModePool) {
  uint64_t init_features = 0;
  init_features |= RBD_FEATURE_OBJECT_MAP;
  init_features |= RBD_FEATURE_EXCLUSIVE_LOCK;
  uint64_t features = RBD_FEATURE_JOURNALING;
  check_mirroring_on_update_features(init_features, true, false, features, 0,
                      RBD_MIRROR_MODE_POOL, RBD_MIRROR_IMAGE_ENABLED,
                      RBD_MIRROR_IMAGE_MODE_JOURNAL);
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
  check_mirroring_on_update_features(init_features, false, true, features,
                      -EINVAL, RBD_MIRROR_MODE_IMAGE, RBD_MIRROR_IMAGE_ENABLED,
                      RBD_MIRROR_IMAGE_MODE_JOURNAL);
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
  std::map<std::string, librbd::mirror_image_global_status_t> images;
  ASSERT_EQ(0, m_rbd.mirror_image_global_status_list(m_ioctx, last_read, 2,
                                                     &images));
  ASSERT_EQ(2U, images.size());

  last_read = images.rbegin()->first;
  images.clear();
  ASSERT_EQ(0, m_rbd.mirror_image_global_status_list(m_ioctx, last_read, 2,
                                                     &images));
  ASSERT_EQ(2U, images.size());

  last_read = images.rbegin()->first;
  images.clear();
  ASSERT_EQ(0, m_rbd.mirror_image_global_status_list(m_ioctx, last_read, 4096,
                                                     &images));
  ASSERT_EQ(1U, images.size());

  last_read = images.rbegin()->first;
  images.clear();
  ASSERT_EQ(0, m_rbd.mirror_image_global_status_list(m_ioctx, last_read, 4096,
                                                     &images));
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
    ASSERT_EQ(0, images.back().mirror_image_enable2(
                RBD_MIRROR_IMAGE_MODE_JOURNAL));
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
  std::list<librbd::mirror_image_global_status_t> statuses;
  for (auto &image : images) {
    aio_comps.push_back(new librbd::RBD::AioCompletion(nullptr, nullptr));
    statuses.emplace_back();
    ASSERT_EQ(0, image.aio_mirror_image_get_global_status(
                   &statuses.back(), sizeof(statuses.back()),
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

    librbd::mirror_image_site_status_t local_status;
    ASSERT_EQ(0, get_local_mirror_image_site_status(status, &local_status));
    ASSERT_EQ(MIRROR_IMAGE_STATUS_STATE_UNKNOWN, local_status.state);
    ASSERT_EQ("status not found", local_status.description);
    ASSERT_FALSE(local_status.up);
    ASSERT_EQ(0, local_status.last_update);
  }
}

TEST_F(TestMirroring, SiteName) {
  REQUIRE(!is_librados_test_stub(_rados));

  const std::string expected_site_name("us-east-1a");
  ASSERT_EQ(0, m_rbd.mirror_site_name_set(_rados, expected_site_name));

  std::string site_name;
  ASSERT_EQ(0, m_rbd.mirror_site_name_get(_rados, &site_name));
  ASSERT_EQ(expected_site_name, site_name);

  ASSERT_EQ(0, m_rbd.mirror_site_name_set(_rados, ""));

  std::string fsid;
  ASSERT_EQ(0, _rados.cluster_fsid(&fsid));
  ASSERT_EQ(0, m_rbd.mirror_site_name_get(_rados, &site_name));
  ASSERT_EQ(fsid, site_name);
}

TEST_F(TestMirroring, Bootstrap) {
  REQUIRE(!is_librados_test_stub(_rados));

  std::string token_b64;
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
  ASSERT_EQ(-EINVAL, m_rbd.mirror_peer_bootstrap_create(m_ioctx, &token_b64));

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_POOL));
  ASSERT_EQ(0, m_rbd.mirror_peer_bootstrap_create(m_ioctx, &token_b64));

  bufferlist token_b64_bl;
  token_b64_bl.append(token_b64);

  bufferlist token_bl;
  token_bl.decode_base64(token_b64_bl);

  // cannot import token into same cluster
  ASSERT_EQ(-EINVAL,
            m_rbd.mirror_peer_bootstrap_import(
              m_ioctx, RBD_MIRROR_PEER_DIRECTION_RX, token_b64));
}

TEST_F(TestMirroring, PeerDirection) {
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_POOL));

  std::string uuid;
  ASSERT_EQ(-EINVAL, m_rbd.mirror_peer_site_add(
                       m_ioctx, &uuid, RBD_MIRROR_PEER_DIRECTION_TX, "siteA",
                       "client.admin"));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "siteA", "client.admin"));

  std::vector<librbd::mirror_peer_site_t> peers;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_list(m_ioctx, &peers));
  std::vector<librbd::mirror_peer_site_t> expected_peers = {
    {uuid, RBD_MIRROR_PEER_DIRECTION_RX_TX, "siteA", "", "client.admin", 0}};
  ASSERT_EQ(expected_peers, peers);

  ASSERT_EQ(0, m_rbd.mirror_peer_site_set_direction(
                 m_ioctx, uuid, RBD_MIRROR_PEER_DIRECTION_RX));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_list(m_ioctx, &peers));
  expected_peers = {
    {uuid, RBD_MIRROR_PEER_DIRECTION_RX, "siteA", "", "client.admin", 0}};
  ASSERT_EQ(expected_peers, peers);

  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, uuid));
}

TEST_F(TestMirroring, Snapshot)
{
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));

  uint64_t features;
  ASSERT_TRUE(get_features(&features));
  int order = 20;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                             &order));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));

  ASSERT_EQ(0, image.metadata_set(
              "conf_rbd_mirroring_max_mirroring_snapshots", "3"));

  uint64_t snap_id;

  ASSERT_EQ(-EINVAL, image.mirror_image_create_snapshot(&snap_id));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  ASSERT_EQ(-EINVAL, image.mirror_image_create_snapshot(&snap_id));
  ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  librbd::mirror_image_mode_t mode;
  ASSERT_EQ(0, image.mirror_image_get_mode(&mode));
  ASSERT_EQ(RBD_MIRROR_IMAGE_MODE_SNAPSHOT, mode);
  ASSERT_EQ(-EINVAL, image.mirror_image_create_snapshot(&snap_id));
  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &peer_uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "cluster", "client"));
  ASSERT_EQ(0, image.mirror_image_create_snapshot(&snap_id));
  vector<librbd::snap_info_t> snaps;
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_EQ(2U, snaps.size());
  ASSERT_EQ(snaps[1].id, snap_id);

  ASSERT_EQ(0, image.mirror_image_create_snapshot(&snap_id));
  ASSERT_EQ(0, image.mirror_image_create_snapshot(&snap_id));
  snaps.clear();
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_EQ(3U, snaps.size());
  ASSERT_EQ(snaps[2].id, snap_id);

  // automatic peer unlink on max_mirroring_snapshots reached
  ASSERT_EQ(0, image.mirror_image_create_snapshot(&snap_id));
  vector<librbd::snap_info_t> snaps1;
  ASSERT_EQ(0, image.snap_list(snaps1));
  ASSERT_EQ(3U, snaps1.size());
  ASSERT_EQ(snaps1[0].id, snaps[0].id);
  ASSERT_EQ(snaps1[1].id, snaps[1].id);
  ASSERT_EQ(snaps1[2].id, snap_id);

  librbd::snap_namespace_type_t snap_ns_type;
  ASSERT_EQ(0, image.snap_get_namespace_type(snap_id, &snap_ns_type));
  ASSERT_EQ(RBD_SNAP_NAMESPACE_TYPE_MIRROR, snap_ns_type);
  librbd::snap_mirror_namespace_t mirror_snap;
  ASSERT_EQ(0, image.snap_get_mirror_namespace(snap_id, &mirror_snap,
                                               sizeof(mirror_snap)));
  ASSERT_EQ(1U, mirror_snap.mirror_peer_uuids.size());
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer_uuid));

  for (auto &snap : snaps1) {
    ASSERT_EQ(0, image.snap_remove_by_id(snap.id));
  }

  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestMirroring, SnapshotRemoveOnDisable)
{
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &peer_uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "cluster", "client"));

  uint64_t features;
  ASSERT_TRUE(get_features(&features));
  int order = 20;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                             &order));
  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));
  ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  uint64_t snap_id;
  ASSERT_EQ(0, image.mirror_image_create_snapshot(&snap_id));

  vector<librbd::snap_info_t> snaps;
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_EQ(2U, snaps.size());
  ASSERT_EQ(snaps[1].id, snap_id);

  ASSERT_EQ(0, image.mirror_image_disable(false));

  snaps.clear();
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_TRUE(snaps.empty());

  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestMirroring, SnapshotUnlinkPeer)
{
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer1_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &peer1_uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "cluster1", "client"));
  std::string peer2_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &peer2_uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "cluster2", "client"));
  std::string peer3_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &peer3_uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "cluster3", "client"));
  uint64_t features;
  ASSERT_TRUE(get_features(&features));
  features &= ~RBD_FEATURE_JOURNALING;
  int order = 20;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                             &order));
  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));
  ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  uint64_t snap_id;
  ASSERT_EQ(0, image.mirror_image_create_snapshot(&snap_id));
  uint64_t snap_id2;
  ASSERT_EQ(0, image.mirror_image_create_snapshot(&snap_id2));
  librbd::snap_mirror_namespace_t mirror_snap;
  ASSERT_EQ(0, image.snap_get_mirror_namespace(snap_id, &mirror_snap,
                                               sizeof(mirror_snap)));
  ASSERT_EQ(3U, mirror_snap.mirror_peer_uuids.size());
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer1_uuid));
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer2_uuid));
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer3_uuid));

  auto ictx = new librbd::ImageCtx(image_name, "", nullptr, m_ioctx, false);
  ASSERT_EQ(0, ictx->state->open(0));
  BOOST_SCOPE_EXIT(&ictx) {
    if (ictx != nullptr) {
      ictx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  C_SaferCond cond1;
  auto req = librbd::mirror::snapshot::UnlinkPeerRequest<>::create(
    ictx, snap_id, peer1_uuid, &cond1);
  req->send();
  ASSERT_EQ(0, cond1.wait());

  ASSERT_EQ(0, image.snap_get_mirror_namespace(snap_id, &mirror_snap,
                                               sizeof(mirror_snap)));
  ASSERT_EQ(2U, mirror_snap.mirror_peer_uuids.size());
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer2_uuid));
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer3_uuid));

  ASSERT_EQ(0, librbd::api::Namespace<>::create(m_ioctx, "ns1"));
  librados::IoCtx ns_ioctx;
  ns_ioctx.dup(m_ioctx);
  ns_ioctx.set_namespace("ns1");
  ASSERT_EQ(0, m_rbd.mirror_mode_set(ns_ioctx, RBD_MIRROR_MODE_IMAGE));
  ASSERT_EQ(0, m_rbd.create2(ns_ioctx, image_name.c_str(), 4096, features,
                             &order));

  librbd::Image ns_image;
  ASSERT_EQ(0, m_rbd.open(ns_ioctx, ns_image, image_name.c_str()));
  ASSERT_EQ(0, ns_image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  uint64_t ns_snap_id;
  ASSERT_EQ(0, ns_image.mirror_image_create_snapshot(&ns_snap_id));
  ASSERT_EQ(0, ns_image.snap_get_mirror_namespace(ns_snap_id, &mirror_snap,
                                                  sizeof(mirror_snap)));
  ASSERT_EQ(3U, mirror_snap.mirror_peer_uuids.size());
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer1_uuid));
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer2_uuid));
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer3_uuid));

  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer3_uuid));

  ASSERT_EQ(0, image.snap_get_mirror_namespace(snap_id, &mirror_snap,
                                               sizeof(mirror_snap)));
  ASSERT_EQ(1U, mirror_snap.mirror_peer_uuids.size());
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer2_uuid));

  ASSERT_EQ(0, ns_image.snap_get_mirror_namespace(ns_snap_id, &mirror_snap,
                                                  sizeof(mirror_snap)));
  ASSERT_EQ(2U, mirror_snap.mirror_peer_uuids.size());
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer1_uuid));
  ASSERT_EQ(1, mirror_snap.mirror_peer_uuids.count(peer2_uuid));

  C_SaferCond cond2;
  req = librbd::mirror::snapshot::UnlinkPeerRequest<>::create(
    ictx, snap_id, peer2_uuid, &cond2);
  req->send();
  ASSERT_EQ(0, cond2.wait());

  ASSERT_EQ(-ENOENT, image.snap_get_mirror_namespace(snap_id, &mirror_snap,
                                                     sizeof(mirror_snap)));
  ictx->state->close();
  ictx = nullptr;
  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));

  ASSERT_EQ(0, ns_image.close());
  ASSERT_EQ(0, m_rbd.remove(ns_ioctx, image_name.c_str()));
  ASSERT_EQ(0, librbd::api::Namespace<>::remove(m_ioctx, "ns1"));

  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer1_uuid));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer2_uuid));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestMirroring, SnapshotImageState)
{
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));

  uint64_t features;
  ASSERT_TRUE(get_features(&features));
  int order = 20;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                             &order));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));
  ASSERT_EQ(0, image.snap_create("snap"));
  ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  std::vector<librbd::snap_info_t> snaps;
  ASSERT_EQ(0, image.snap_list(snaps));
  ASSERT_EQ(2U, snaps.size());
  auto snap_id = snaps[1].id;

  auto ictx = new librbd::ImageCtx(image_name, "", nullptr, m_ioctx, false);
  ASSERT_EQ(0, ictx->state->open(0));
  BOOST_SCOPE_EXIT(&ictx) {
    if (ictx != nullptr) {
      ictx->state->close();
    }
  } BOOST_SCOPE_EXIT_END;

  {
    C_SaferCond cond;
    auto req = librbd::mirror::snapshot::SetImageStateRequest<>::create(
      ictx, snap_id, &cond);
    req->send();
    ASSERT_EQ(0, cond.wait());
  }

  librbd::mirror::snapshot::ImageState image_state;
  {
    C_SaferCond cond;
    auto req = librbd::mirror::snapshot::GetImageStateRequest<>::create(
      ictx, snap_id, &image_state, &cond);
    req->send();
    ASSERT_EQ(0, cond.wait());
  }

  ASSERT_EQ(image_name, image_state.name);
  ASSERT_EQ(0, image.features(&features));
  ASSERT_EQ(features & ~RBD_FEATURES_IMPLICIT_ENABLE, image_state.features);
  ASSERT_EQ(1U, image_state.snapshots.size());
  ASSERT_EQ("snap", image_state.snapshots.begin()->second.name);
  ASSERT_TRUE(image_state.metadata.empty());

  {
    C_SaferCond cond;
    auto req = librbd::mirror::snapshot::RemoveImageStateRequest<>::create(
      ictx, snap_id, &cond);
    req->send();
    ASSERT_EQ(0, cond.wait());
  }

  // test storing "large" image state in multiple objects

  ASSERT_EQ(0, ictx->config.set_val("rbd_default_order", "8"));

  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(0, image.metadata_set(stringify(i), std::string(1024, 'A' + i)));
  }

  {
    C_SaferCond cond;
    auto req = librbd::mirror::snapshot::SetImageStateRequest<>::create(
      ictx, snap_id, &cond);
    req->send();
    ASSERT_EQ(0, cond.wait());
  }

  {
    C_SaferCond cond;
    auto req = librbd::mirror::snapshot::GetImageStateRequest<>::create(
      ictx, snap_id, &image_state, &cond);
    req->send();
    ASSERT_EQ(0, cond.wait());
  }

  ASSERT_EQ(image_name, image_state.name);
  ASSERT_EQ(features & ~RBD_FEATURES_IMPLICIT_ENABLE, image_state.features);
  ASSERT_EQ(10U, image_state.metadata.size());
  for (int i = 0; i < 10; i++) {
    auto &bl = image_state.metadata[stringify(i)];
    ASSERT_EQ(0, strncmp(std::string(1024, 'A' + i).c_str(), bl.c_str(),
                         bl.length()));
  }

  {
    C_SaferCond cond;
    auto req = librbd::mirror::snapshot::RemoveImageStateRequest<>::create(
      ictx, snap_id, &cond);
    req->send();
    ASSERT_EQ(0, cond.wait());
  }

  ASSERT_EQ(0, ictx->state->close());
  ictx = nullptr;

  ASSERT_EQ(0, image.snap_remove("snap"));
  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
}

TEST_F(TestMirroring, SnapshotPromoteDemote)
{
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &peer_uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "cluster", "client"));

  uint64_t features;
  ASSERT_TRUE(get_features(&features));
  int order = 20;
  ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 4096, features,
                             &order));

  librbd::Image image;
  ASSERT_EQ(0, m_rbd.open(m_ioctx, image, image_name.c_str()));
  ASSERT_EQ(0, image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  librbd::mirror_image_mode_t mode;
  ASSERT_EQ(0, image.mirror_image_get_mode(&mode));
  ASSERT_EQ(RBD_MIRROR_IMAGE_MODE_SNAPSHOT, mode);

  ASSERT_EQ(-EINVAL, image.mirror_image_promote(false));
  ASSERT_EQ(0, image.mirror_image_demote());
  ASSERT_EQ(0, image.mirror_image_promote(false));
  ASSERT_EQ(0, image.mirror_image_demote());
  ASSERT_EQ(0, image.mirror_image_promote(false));

  ASSERT_EQ(0, image.close());
  ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
}

TEST_F(TestMirroring, AioSnapshotCreate)
{
  REQUIRE_FORMAT_V2();

  std::list<std::string> image_names;
  for (size_t idx = 0; idx < 10; ++idx) {
    image_names.push_back(get_temp_image_name());
  }

  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_IMAGE));
  std::string peer_uuid;
  ASSERT_EQ(0, m_rbd.mirror_peer_site_add(m_ioctx, &peer_uuid,
                                          RBD_MIRROR_PEER_DIRECTION_RX_TX,
                                          "cluster", "client"));
  // create mirror images
  uint64_t features;
  ASSERT_TRUE(get_features(&features));
  int order = 20;
  std::list<librbd::Image> images;
  for (auto &image_name : image_names) {
    ASSERT_EQ(0, m_rbd.create2(m_ioctx, image_name.c_str(), 2048, features,
                               &order));
    images.emplace_back();
    ASSERT_EQ(0, m_rbd.open(m_ioctx, images.back(), image_name.c_str()));
    ASSERT_EQ(0, images.back().mirror_image_enable2(
                RBD_MIRROR_IMAGE_MODE_SNAPSHOT));
  }

  // create snapshots
  std::list<uint64_t> snap_ids;
  std::list<librbd::RBD::AioCompletion *> aio_comps;
  for (auto &image : images) {
    snap_ids.emplace_back();
    aio_comps.push_back(new librbd::RBD::AioCompletion(nullptr, nullptr));
    ASSERT_EQ(0, image.aio_mirror_image_create_snapshot(0, &snap_ids.back(),
                                                        aio_comps.back()));
  }
  for (auto aio_comp : aio_comps) {
    ASSERT_EQ(0, aio_comp->wait_for_complete());
    ASSERT_EQ(1, aio_comp->is_complete());
    ASSERT_EQ(0, aio_comp->get_return_value());
    aio_comp->release();
  }
  aio_comps.clear();

  // verify
  for (auto &image : images) {
    vector<librbd::snap_info_t> snaps;
    ASSERT_EQ(0, image.snap_list(snaps));
    ASSERT_EQ(2U, snaps.size());
    ASSERT_EQ(snaps[1].id, snap_ids.front());

    std::string image_name;
    ASSERT_EQ(0, image.get_name(&image_name));
    ASSERT_EQ(0, image.close());
    ASSERT_EQ(0, m_rbd.remove(m_ioctx, image_name.c_str()));
    snap_ids.pop_front();
  }

  ASSERT_EQ(0, m_rbd.mirror_peer_site_remove(m_ioctx, peer_uuid));
  ASSERT_EQ(0, m_rbd.mirror_mode_set(m_ioctx, RBD_MIRROR_MODE_DISABLED));
}
