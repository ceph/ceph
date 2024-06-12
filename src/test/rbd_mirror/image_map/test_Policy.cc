// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/Context.h"
#include "test/rbd_mirror/test_fixture.h"
#include "tools/rbd_mirror/image_map/Types.h"
#include "tools/rbd_mirror/image_map/SimplePolicy.h"
#include "include/stringify.h"
#include "common/Thread.h"

void register_test_image_policy() {
}

namespace rbd {
namespace mirror {
namespace image_map {

class TestImageMapPolicy : public TestFixture {
public:
  void SetUp() override {
    TestFixture::SetUp();

    EXPECT_EQ(0, _rados->conf_set("rbd_mirror_image_policy_migration_throttle",
                                  "0"));

    CephContext *cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
    std::string policy_type = cct->_conf.get_val<std::string>("rbd_mirror_image_policy_type");

    if (policy_type == "none" || policy_type == "simple") {
      m_policy = image_map::SimplePolicy::create(m_local_io_ctx);
    } else {
      ceph_abort();
    }

    m_policy->init({});
  }

  void TearDown() override {
    TestFixture::TearDown();
    delete m_policy;
  }

  void map_image(const std::string &global_image_id) {
    auto global_id = image_map::GlobalId(MIRROR_ENTITY_TYPE_IMAGE,
                                         global_image_id);

    ASSERT_TRUE(m_policy->add_entity(global_id, 1));

    ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
    ASSERT_TRUE(m_policy->finish_action(global_id, 0));

    ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));
    ASSERT_FALSE(m_policy->finish_action(global_id, 0));
  }

  void unmap_image(const std::string &global_image_id) {
    auto global_id = image_map::GlobalId(MIRROR_ENTITY_TYPE_IMAGE,
                                         global_image_id);

    ASSERT_TRUE(m_policy->remove_entity(global_id));

    ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
    ASSERT_TRUE(m_policy->finish_action(global_id, 0));

    ASSERT_EQ(ACTION_TYPE_MAP_REMOVE, m_policy->start_action(global_id));
    ASSERT_FALSE(m_policy->finish_action(global_id, 0));
  }

  void shuffle_image(const std::string &global_image_id) {
    auto global_id = image_map::GlobalId(MIRROR_ENTITY_TYPE_IMAGE,
                                         global_image_id);

    ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
    ASSERT_TRUE(m_policy->finish_action(global_id, 0));

    ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
    ASSERT_TRUE(m_policy->finish_action(global_id, 0));

    ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));
    ASSERT_FALSE(m_policy->finish_action(global_id, 0));
  }

  Policy *m_policy;
};

TEST_F(TestImageMapPolicy, NegativeLookup) {
  const std::string global_image_id = "global id 1";

  LookupInfo info = m_policy->lookup({MIRROR_ENTITY_TYPE_IMAGE,
                                      global_image_id});
  ASSERT_TRUE(info.instance_id == UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImageMapPolicy, Init) {
  const std::string global_image_id = "global id 1";

  m_policy->init({{{MIRROR_ENTITY_TYPE_IMAGE, global_image_id},
                   {"9876", {}, {}}}});

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_image_id));
  ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));
}

TEST_F(TestImageMapPolicy, MapImage) {
  const std::string global_image_id = "global id 1";

  map_image(global_image_id);

  LookupInfo info = m_policy->lookup({MIRROR_ENTITY_TYPE_IMAGE,
                                      global_image_id});
  ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImageMapPolicy, UnmapImage) {
  const std::string global_image_id = "global id 1";

  // map image
  map_image(global_image_id);

  LookupInfo info = m_policy->lookup({MIRROR_ENTITY_TYPE_IMAGE,
                                      global_image_id});
  ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);

  // unmap image
  unmap_image(global_image_id);

  info = m_policy->lookup({MIRROR_ENTITY_TYPE_IMAGE, global_image_id});
  ASSERT_TRUE(info.instance_id == UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImageMapPolicy, ShuffleImageAddInstance) {
  std::set<std::string> global_image_ids {
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5", "global id 6"
  };

  for (auto const &global_image_id : global_image_ids) {
    // map image
    map_image(global_image_id);

    LookupInfo info = m_policy->lookup({MIRROR_ENTITY_TYPE_IMAGE,
                                        global_image_id});
    ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
  }

  image_map::GlobalIds shuffle_global_ids;
  m_policy->add_instances({"9876"}, &shuffle_global_ids);

  for (auto const &global_id : shuffle_global_ids) {
    ASSERT_EQ(global_id.type, MIRROR_ENTITY_TYPE_IMAGE);
    shuffle_image(global_id.id);

    LookupInfo info = m_policy->lookup(global_id);
    ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
  }
}

TEST_F(TestImageMapPolicy, ShuffleImageRemoveInstance) {
  std::set<std::string> global_image_ids {
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5"
  };

  image_map::GlobalIds shuffle_global_ids;
  m_policy->add_instances({stringify(m_local_io_ctx.get_instance_id())},
                          &shuffle_global_ids);
  for (auto const &global_image_id : global_image_ids) {
    // map image
    map_image(global_image_id);

    LookupInfo info = m_policy->lookup({MIRROR_ENTITY_TYPE_IMAGE,
                                        global_image_id});
    ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
  }

  m_policy->add_instances({"9876"}, &shuffle_global_ids);

  for (auto const &global_id : shuffle_global_ids) {
    ASSERT_EQ(global_id.type, MIRROR_ENTITY_TYPE_IMAGE);
    shuffle_image(global_id.id);

    LookupInfo info = m_policy->lookup(global_id);
    ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
  }

  // record which of the images got migrated to the new instance
  image_map::GlobalIds remapped_global_ids;
  for (auto const &global_id: shuffle_global_ids) {
    LookupInfo info = m_policy->lookup(global_id);
    if (info.instance_id == "9876") {
      remapped_global_ids.emplace(global_id);
    }
  }

  shuffle_global_ids.clear();
  m_policy->remove_instances({"9876"}, &shuffle_global_ids);

  ASSERT_TRUE(shuffle_global_ids == remapped_global_ids);

  for (auto const &global_id : shuffle_global_ids) {
    shuffle_image(global_id.id);

    LookupInfo info = m_policy->lookup(global_id);
    ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
  }
}

TEST_F(TestImageMapPolicy, RetryMapUpdate) {
  auto global_id = image_map::GlobalId(MIRROR_ENTITY_TYPE_IMAGE,
                                       "global id 1");

  ASSERT_TRUE(m_policy->add_entity(global_id, 1));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  // on-disk map update failed
  ASSERT_TRUE(m_policy->finish_action(global_id, -EIO));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));
  ASSERT_FALSE(m_policy->finish_action(global_id, 0));

  LookupInfo info = m_policy->lookup(global_id);
  ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImageMapPolicy, MapFailureAndUnmap) {
  auto global_id = image_map::GlobalId(MIRROR_ENTITY_TYPE_IMAGE,
                                       "global id 1");

  ASSERT_TRUE(m_policy->add_entity(global_id, 1));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));

  image_map::GlobalIds shuffle_global_ids;
  m_policy->add_instances({"9876"}, &shuffle_global_ids);
  ASSERT_TRUE(shuffle_global_ids.empty());

  m_policy->remove_instances({stringify(m_local_io_ctx.get_instance_id())},
                             &shuffle_global_ids);
  ASSERT_TRUE(shuffle_global_ids.empty());

  ASSERT_TRUE(m_policy->finish_action(global_id, -EBLOCKLISTED));

  ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, -ENOENT));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));
  ASSERT_FALSE(m_policy->finish_action(global_id, 0));

  ASSERT_TRUE(m_policy->remove_entity(global_id));

  ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_MAP_REMOVE, m_policy->start_action(global_id));
  ASSERT_FALSE(m_policy->finish_action(global_id, 0));
}

TEST_F(TestImageMapPolicy, ReshuffleWithMapFailure) {
  std::set<std::string> global_image_ids {
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5",
    "global id 6"
  };

  image_map::GlobalIds shuffle_global_ids;
  m_policy->add_instances({stringify(m_local_io_ctx.get_instance_id())},
                          &shuffle_global_ids);
  for (auto const &global_image_id : global_image_ids) {
    // map image
    map_image(global_image_id);

    LookupInfo info = m_policy->lookup({MIRROR_ENTITY_TYPE_IMAGE,
                                        global_image_id});
    ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
  }

  m_policy->add_instances({"9876"}, &shuffle_global_ids);
  ASSERT_FALSE(shuffle_global_ids.empty());

  auto global_id = *(shuffle_global_ids.begin());
  shuffle_global_ids.clear();

  ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));

  // peer unavailable
  m_policy->remove_instances({"9876"}, &shuffle_global_ids);
  ASSERT_TRUE(shuffle_global_ids.empty());

  ASSERT_TRUE(m_policy->finish_action(global_id, -EBLOCKLISTED));

  ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));
  ASSERT_FALSE(m_policy->finish_action(global_id, 0));
}

TEST_F(TestImageMapPolicy, ShuffleFailureAndRemove) {
  std::set<std::string> global_image_ids {
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5",
    "global id 6"
  };

  image_map::GlobalIds shuffle_global_ids;
  m_policy->add_instances({stringify(m_local_io_ctx.get_instance_id())},
                          &shuffle_global_ids);
  for (auto const &global_image_id : global_image_ids) {
    // map image
    map_image(global_image_id);

    LookupInfo info = m_policy->lookup({MIRROR_ENTITY_TYPE_IMAGE,
                                        global_image_id});
    ASSERT_TRUE(info.instance_id != UNMAPPED_INSTANCE_ID);
  }

  m_policy->add_instances({"9876"}, &shuffle_global_ids);
  ASSERT_FALSE(shuffle_global_ids.empty());

  auto global_id = *(shuffle_global_ids.begin());
  shuffle_global_ids.clear();

  ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));

  // peer unavailable
  m_policy->remove_instances({"9876"}, &shuffle_global_ids);
  ASSERT_TRUE(shuffle_global_ids.empty());

  ASSERT_TRUE(m_policy->finish_action(global_id, -EBLOCKLISTED));

  ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));
  ASSERT_FALSE(m_policy->finish_action(global_id, 0));

  ASSERT_TRUE(m_policy->remove_entity(global_id));

  ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_MAP_REMOVE, m_policy->start_action(global_id));
  ASSERT_FALSE(m_policy->finish_action(global_id, 0));

  LookupInfo info = m_policy->lookup(global_id);
  ASSERT_TRUE(info.instance_id == UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImageMapPolicy, InitialInstanceUpdate) {
  auto global_id = image_map::GlobalId(MIRROR_ENTITY_TYPE_IMAGE,
                                       "global id 1");

  m_policy->init({{global_id, {"9876", {}, {}}}});

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));

  auto instance_id = stringify(m_local_io_ctx.get_instance_id());
  image_map::GlobalIds shuffle_global_ids;
  m_policy->add_instances({instance_id}, &shuffle_global_ids);

  ASSERT_EQ(0U, shuffle_global_ids.size());
  ASSERT_TRUE(m_policy->finish_action(global_id, -ENOENT));

  ASSERT_EQ(ACTION_TYPE_RELEASE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_MAP_UPDATE, m_policy->start_action(global_id));
  ASSERT_TRUE(m_policy->finish_action(global_id, 0));

  ASSERT_EQ(ACTION_TYPE_ACQUIRE, m_policy->start_action(global_id));
  ASSERT_FALSE(m_policy->finish_action(global_id, 0));
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
