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

class TestImagePolicy : public TestFixture {
public:
  void SetUp() override {
    TestFixture::SetUp();

    CephContext *cct = reinterpret_cast<CephContext *>(m_local_io_ctx.cct());
    std::string policy_type = cct->_conf->get_val<string>("rbd_mirror_image_policy_type");

    if (policy_type == "simple") {
      m_policy = image_map::SimplePolicy::create(m_local_io_ctx);
    } else {
      assert(false);
    }

    m_policy->init({});
  }

  void TearDown() override {
    TestFixture::TearDown();
    delete m_policy;
  }

  struct C_UpdateMap : Context {
    TestImagePolicy *test;
    std::string global_image_id;

    C_UpdateMap(TestImagePolicy *test, const std::string &global_image_id)
      : test(test),
        global_image_id(global_image_id) {
    }

    void finish(int r) override {
      test->m_updated = true;
    }

    void complete(int r) override {
      finish(r);
    }
  };
  struct C_RemoveMap : Context {
    TestImagePolicy *test;
    std::string global_image_id;

    C_RemoveMap(TestImagePolicy *test, const std::string &global_image_id)
      : test(test),
        global_image_id(global_image_id) {
    }

    void finish(int r) override {
      test->m_removed = true;
    }

    void complete(int r) override {
      finish(r);
    }
  };

  struct C_AcquireImage : Context {
    TestImagePolicy *test;
    std::string global_image_id;

    C_AcquireImage(TestImagePolicy *test, const std::string &global_image_id)
      : test(test),
        global_image_id(global_image_id) {
    }

    void finish(int r) override {
      test->m_acquired = true;
    }

    void complete(int r) override {
      finish(r);
    }
  };

  void reset_flags() {
    m_updated = false;
    m_removed = false;
    m_acquired = false;
    m_released = false;
  }

  void map_image(const std::string &global_image_id) {
    Context *on_update = new C_UpdateMap(this, global_image_id);
    Context *on_acquire = new C_AcquireImage(this, global_image_id);

    ASSERT_TRUE(m_policy->add_image(global_image_id, on_update, on_acquire, nullptr));

    m_policy->start_next_action(global_image_id);
    ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

    m_policy->start_next_action(global_image_id);
    ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

    ASSERT_TRUE(m_updated && m_acquired);
  }

  void unmap_image(const std::string &global_image_id) {
    Context *on_release = new FunctionContext([this, global_image_id](int r) {
        m_released = true;
      });
    Context *on_remove = new C_RemoveMap(this, global_image_id);

    ASSERT_TRUE(m_policy->remove_image(global_image_id, on_release, on_remove, nullptr));

    m_policy->start_next_action(global_image_id);
    ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

    m_policy->start_next_action(global_image_id);
    ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

    ASSERT_TRUE(m_released && m_removed);
  }

  void shuffle_image(const std::string &global_image_id) {
    Context *on_release = new FunctionContext([this, global_image_id](int r) {
        m_released = true;
      });
    Context *on_update = new C_UpdateMap(this, global_image_id);
    Context *on_acquire = new C_AcquireImage(this, global_image_id);

    ASSERT_TRUE(m_policy->shuffle_image(global_image_id, on_release,
                                        on_update, on_acquire, nullptr));

    m_policy->start_next_action(global_image_id);
    ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

    m_policy->start_next_action(global_image_id);
    ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

    m_policy->start_next_action(global_image_id);
    ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

    ASSERT_TRUE(m_released && m_updated && m_acquired);
  }

  Policy *m_policy;
  bool m_updated = false;
  bool m_removed = false;
  bool m_acquired = false;
  bool m_released = false;
};

TEST_F(TestImagePolicy, NegativeLookup) {
  const std::string global_image_id = "global id 1";

  Policy::LookupInfo info = m_policy->lookup(global_image_id);
  ASSERT_TRUE(info.instance_id == Policy::UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImagePolicy, MapImage) {
  const std::string global_image_id = "global id 1";

  map_image(global_image_id);

  Policy::LookupInfo info = m_policy->lookup(global_image_id);
  ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImagePolicy, UnmapImage) {
  const std::string global_image_id = "global id 1";

  // map image
  map_image(global_image_id);

  Policy::LookupInfo info = m_policy->lookup(global_image_id);
  ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);

  reset_flags();

  // unmap image
  unmap_image(global_image_id);

  info = m_policy->lookup(global_image_id);
  ASSERT_TRUE(info.instance_id == Policy::UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImagePolicy, ShuffleImageAddInstance) {
  std::set<std::string> global_image_ids {
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5", "global id 6"
  };

  for (auto const &global_image_id : global_image_ids) {
    // map image
    map_image(global_image_id);

    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
  }

  reset_flags();

  std::set<std::string> shuffle_global_image_ids;
  m_policy->add_instances({"9876"}, &shuffle_global_image_ids);

  for (auto const &global_image_id : shuffle_global_image_ids) {
    shuffle_image(global_image_id);

    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
  }
}

TEST_F(TestImagePolicy, ShuffleImageRemoveInstance) {
  std::set<std::string> global_image_ids {
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5"
  };

  for (auto const &global_image_id : global_image_ids) {
    // map image
    map_image(global_image_id);

    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
  }

  reset_flags();

  std::set<std::string> shuffle_global_image_ids;
  m_policy->add_instances({"9876"}, &shuffle_global_image_ids);

  for (auto const &global_image_id : shuffle_global_image_ids) {
    shuffle_image(global_image_id);

    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
  }

  // record which of the images got migrated to the new instance
  std::set<std::string> remapped_global_image_ids;
  for (auto const &global_image_id: shuffle_global_image_ids) {
    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    if (info.instance_id == "9876") {
      remapped_global_image_ids.emplace(global_image_id);
    }
  }

  reset_flags();

  shuffle_global_image_ids.clear();
  m_policy->remove_instances({"9876"}, &shuffle_global_image_ids);

  ASSERT_TRUE(shuffle_global_image_ids == remapped_global_image_ids);

  for (auto const &global_image_id : shuffle_global_image_ids) {
    shuffle_image(global_image_id);

    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
  }
}

TEST_F(TestImagePolicy, RetryMapUpdate) {
  const std::string global_image_id = "global id 1";

  Context *on_update = new C_UpdateMap(this, global_image_id);
  Context *on_acquire = new C_AcquireImage(this, global_image_id);

  ASSERT_TRUE(m_policy->add_image(global_image_id, on_update, on_acquire, nullptr));

  m_policy->start_next_action(global_image_id);
  // on-disk map update failed
  ASSERT_TRUE(m_policy->finish_action(global_image_id, -EIO));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

  ASSERT_TRUE(m_updated && m_acquired);

  Policy::LookupInfo info = m_policy->lookup(global_image_id);
  ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
}

TEST_F(TestImagePolicy, MapFailureAndUnmap) {
  const std::string global_image_id = "global id 1";

  Context *on_update = new C_UpdateMap(this, global_image_id);
  Context *on_acquire = new C_AcquireImage(this, global_image_id);

  ASSERT_TRUE(m_policy->add_image(global_image_id, on_update, on_acquire, nullptr));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, -EBLACKLISTED));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

  ASSERT_TRUE(m_updated && m_acquired);

  reset_flags();

  Context *on_release = new FunctionContext([this, global_image_id](int r) {
      m_released = true;
    });
  Context *on_remove = new C_RemoveMap(this, global_image_id);
  ASSERT_TRUE(m_policy->remove_image(global_image_id, on_release, on_remove, nullptr));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

  ASSERT_TRUE(m_removed && m_released);
}

TEST_F(TestImagePolicy, ReshuffleWithMapFailure) {
  std::set<std::string> global_image_ids {
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5", "global id 6"
  };

  for (auto const &global_image_id : global_image_ids) {
    // map image
    map_image(global_image_id);

    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
  }

  std::set<std::string> shuffle_global_image_ids;
  m_policy->add_instances({"9876"}, &shuffle_global_image_ids);

  if (shuffle_global_image_ids.empty()) {
    return;
  }

  const std::string global_image_id = *(shuffle_global_image_ids.begin());
  shuffle_global_image_ids.clear();

  reset_flags();

  Context *on_release = new FunctionContext([this, global_image_id](int r) {
        m_released = true;
      });
  Context *on_update = new C_UpdateMap(this, global_image_id);
  Context *on_acquire = new C_AcquireImage(this, global_image_id);

  ASSERT_TRUE(m_policy->shuffle_image(global_image_id, on_release,
                                      on_update, on_acquire, nullptr));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);

  // peer unavailable
  m_policy->remove_instances({"9876"}, &shuffle_global_image_ids);
  ASSERT_TRUE(shuffle_global_image_ids.empty());

  ASSERT_TRUE(m_policy->finish_action(global_image_id, -EBLACKLISTED));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

  ASSERT_TRUE(m_released && m_updated && m_acquired);
}

TEST_F(TestImagePolicy, ShuffleFailureAndRemove) {
  std::set<std::string> global_image_ids {
    "global id 1", "global id 2", "global id 3", "global id 4", "global id 5", "global id 6"
  };

  for (auto const &global_image_id : global_image_ids) {
    // map image
    map_image(global_image_id);

    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    ASSERT_TRUE(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);
  }

  std::set<std::string> shuffle_global_image_ids;
  m_policy->add_instances({"9876"}, &shuffle_global_image_ids);
  if (shuffle_global_image_ids.empty()) {
    return;
  }

  std::string global_image_id = *(shuffle_global_image_ids.begin());
  shuffle_global_image_ids.clear();

  reset_flags();

  Context *on_release = new FunctionContext([this, global_image_id](int r) {
      m_released = true;
    });
  Context *on_update = new C_UpdateMap(this, global_image_id);
  Context *on_acquire = new C_AcquireImage(this, global_image_id);

  ASSERT_TRUE(m_policy->shuffle_image(global_image_id, on_release,
                                      on_update, on_acquire, nullptr));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);

  // peer unavailable
  m_policy->remove_instances({"9876"}, &shuffle_global_image_ids);
  ASSERT_TRUE(shuffle_global_image_ids.empty());

  ASSERT_TRUE(m_policy->finish_action(global_image_id, -EBLACKLISTED));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

  ASSERT_TRUE(m_released && m_updated && m_acquired);

  reset_flags();

  on_release = new FunctionContext([this, global_image_id](int r) {
      m_released = true;
    });
  Context *on_remove = new C_RemoveMap(this, global_image_id);

  ASSERT_TRUE(m_policy->remove_image(global_image_id, on_release, on_remove, nullptr));

  m_policy->start_next_action(global_image_id);
  ASSERT_TRUE(m_policy->finish_action(global_image_id, 0));

  m_policy->start_next_action(global_image_id);
  ASSERT_FALSE(m_policy->finish_action(global_image_id, 0));

  ASSERT_TRUE(m_released && m_removed);

  Policy::LookupInfo info = m_policy->lookup(global_image_id);
  ASSERT_TRUE(info.instance_id == Policy::UNMAPPED_INSTANCE_ID);
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
