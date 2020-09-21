// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/stringify.h"
#include "test/rbd_mirror/test_fixture.h"
#include "cls/rbd/cls_rbd_types.h"
#include "cls/rbd/cls_rbd_client.h"
#include "include/rbd_types.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "librbd/api/Mirror.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/ceph_mutex.h"
#include "tools/rbd_mirror/PoolWatcher.h"
#include "tools/rbd_mirror/Threads.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/pool_watcher/Types.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include <boost/scope_exit.hpp>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <vector>

using rbd::mirror::ImageId;
using rbd::mirror::ImageIds;
using rbd::mirror::PoolWatcher;
using rbd::mirror::PeerSpec;
using rbd::mirror::RadosRef;
using std::map;
using std::set;
using std::string;

void register_test_pool_watcher() {
}

class TestPoolWatcher : public ::rbd::mirror::TestFixture {
public:

  TestPoolWatcher()
    : m_pool_watcher_listener(this),
      m_image_number(0), m_snap_number(0)
  {
    m_cluster = std::make_shared<librados::Rados>();
    EXPECT_EQ("", connect_cluster_pp(*m_cluster));
  }

  void TearDown() override {
    if (m_pool_watcher) {
      C_SaferCond ctx;
      m_pool_watcher->shut_down(&ctx);
      EXPECT_EQ(0, ctx.wait());
    }

    m_cluster->wait_for_latest_osdmap();
    for (auto& pool : m_pools) {
      EXPECT_EQ(0, m_cluster->pool_delete(pool.c_str()));
    }

    TestFixture::TearDown();
  }

  struct PoolWatcherListener : public rbd::mirror::pool_watcher::Listener {
    TestPoolWatcher *test;
    ceph::condition_variable cond;
    ImageIds image_ids;

    explicit PoolWatcherListener(TestPoolWatcher *test) : test(test) {
    }

    void handle_update(const std::string &mirror_uuid,
                       ImageIds &&added_image_ids,
                       ImageIds &&removed_image_ids) override {
      std::lock_guard locker{test->m_lock};
      for (auto &image_id : removed_image_ids) {
        image_ids.erase(image_id);
      }
      image_ids.insert(added_image_ids.begin(), added_image_ids.end());
      cond.notify_all();
    }
  };

  void create_pool(bool enable_mirroring, const PeerSpec &peer, string *name=nullptr) {
    string pool_name = get_temp_pool_name("test-rbd-mirror-");
    ASSERT_EQ(0, m_cluster->pool_create(pool_name.c_str()));

    int64_t pool_id = m_cluster->pool_lookup(pool_name.c_str());
    ASSERT_GE(pool_id, 0);
    m_pools.insert(pool_name);

    librados::IoCtx ioctx;
    ASSERT_EQ(0, m_cluster->ioctx_create2(pool_id, ioctx));
    ioctx.application_enable("rbd", true);

    m_pool_watcher.reset(new PoolWatcher<>(m_threads, ioctx, "mirror uuid",
                                           m_pool_watcher_listener));

    if (enable_mirroring) {
      ASSERT_EQ(0, librbd::api::Mirror<>::mode_set(ioctx,
                                                   RBD_MIRROR_MODE_POOL));
      std::string uuid;
      ASSERT_EQ(0, librbd::api::Mirror<>::peer_site_add(
        ioctx, &uuid, RBD_MIRROR_PEER_DIRECTION_RX_TX, peer.cluster_name,
        peer.client_name));
    }
    if (name != nullptr) {
      *name = pool_name;
    }

    m_pool_watcher->init();
  }

  string get_image_id(librados::IoCtx *ioctx, const string &image_name) {
    string obj = librbd::util::id_obj_name(image_name);
    string id;
    EXPECT_EQ(0, librbd::cls_client::get_id(ioctx, obj, &id));
    return id;
  }

  void create_image(const string &pool_name, bool mirrored=true,
		    string *image_name=nullptr) {
    uint64_t features = librbd::util::get_rbd_default_features(g_ceph_context);
    string name = "image" + stringify(++m_image_number);
    if (mirrored) {
      features |= RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
    }

    librados::IoCtx ioctx;
    ASSERT_EQ(0, m_cluster->ioctx_create(pool_name.c_str(), ioctx));
    int order = 0;
    ASSERT_EQ(0, librbd::create(ioctx, name.c_str(), 1 << 22, false,
				features, &order, 0, 0));
    if (mirrored) {
      librbd::Image image;
      librbd::RBD rbd;
      rbd.open(ioctx, image, name.c_str());
      image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_JOURNAL);

      librbd::mirror_image_info_t mirror_image_info;
      ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image_info,
                                               sizeof(mirror_image_info)));
      image.close();

      m_mirrored_images.insert(ImageId(
        mirror_image_info.global_id, get_image_id(&ioctx, name)));
    }
    if (image_name != nullptr)
      *image_name = name;
  }

  void clone_image(const string &parent_pool_name,
		   const string &parent_image_name,
		   const string &clone_pool_name,
		   bool mirrored=true,
		   string *image_name=nullptr) {
    librados::IoCtx pioctx, cioctx;
    ASSERT_EQ(0, m_cluster->ioctx_create(parent_pool_name.c_str(), pioctx));
    ASSERT_EQ(0, m_cluster->ioctx_create(clone_pool_name.c_str(), cioctx));

    string snap_name = "snap" + stringify(++m_snap_number);
    {
      librbd::ImageCtx *ictx = new librbd::ImageCtx(parent_image_name.c_str(),
						    "", "", pioctx, false);
      ictx->state->open(0);
      librbd::NoOpProgressContext prog_ctx;
      EXPECT_EQ(0, ictx->operations->snap_create(cls::rbd::UserSnapshotNamespace(),
						 snap_name, 0, prog_ctx));
      EXPECT_EQ(0, ictx->operations->snap_protect(cls::rbd::UserSnapshotNamespace(),
						  snap_name));
      ictx->state->close();
    }

    uint64_t features = librbd::util::get_rbd_default_features(g_ceph_context);
    string name = "clone" + stringify(++m_image_number);
    if (mirrored) {
      features |= RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
    }
    int order = 0;
    librbd::clone(pioctx, parent_image_name.c_str(), snap_name.c_str(),
		  cioctx, name.c_str(), features, &order, 0, 0);
    if (mirrored) {
      librbd::Image image;
      librbd::RBD rbd;
      rbd.open(cioctx, image, name.c_str());
      image.mirror_image_enable2(RBD_MIRROR_IMAGE_MODE_JOURNAL);

      librbd::mirror_image_info_t mirror_image_info;
      ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image_info,
                                               sizeof(mirror_image_info)));
      image.close();

      m_mirrored_images.insert(ImageId(
        mirror_image_info.global_id, get_image_id(&cioctx, name)));
    }
    if (image_name != nullptr)
      *image_name = name;
  }

  void check_images() {
    std::unique_lock l{m_lock};
    while (m_mirrored_images != m_pool_watcher_listener.image_ids) {
      if (m_pool_watcher_listener.cond.wait_for(l, 10s) == std::cv_status::timeout) {
        break;
      }
    }

    ASSERT_EQ(m_mirrored_images, m_pool_watcher_listener.image_ids);
  }

  ceph::mutex m_lock = ceph::make_mutex("TestPoolWatcherLock");
  RadosRef m_cluster;
  PoolWatcherListener m_pool_watcher_listener;
  unique_ptr<PoolWatcher<> > m_pool_watcher;

  set<string> m_pools;
  ImageIds m_mirrored_images;

  uint64_t m_image_number;
  uint64_t m_snap_number;
};

TEST_F(TestPoolWatcher, EmptyPool) {
  string uuid1 = "00000000-0000-0000-0000-000000000001";
  PeerSpec site1(uuid1, "site1", "mirror1");
  create_pool(true, site1);
  check_images();
}

TEST_F(TestPoolWatcher, ReplicatedPools) {
  string uuid1 = "00000000-0000-0000-0000-000000000001";
  PeerSpec site1(uuid1, "site1", "mirror1");
  string first_pool, local_pool, last_pool;
  create_pool(true, site1, &first_pool);
  check_images();
  create_image(first_pool);
  check_images();
  string parent_image, parent_image2;
  create_image(first_pool, true, &parent_image);
  check_images();
  clone_image(first_pool, parent_image, first_pool);
  check_images();
  clone_image(first_pool, parent_image, first_pool, true, &parent_image2);
  check_images();
  create_image(first_pool, false);
  check_images();
}
