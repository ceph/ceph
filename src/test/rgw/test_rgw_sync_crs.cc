// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include <boost/intrusive_ptr.hpp>

#include "global/global_init.h"
#include "common/ceph_argparse.h"

#include "rgw/rgw_sync.h"
#include "rgw/rgw_cr_rados.h"
#include "rgw/rgw_boost_asio_yield.h"


// coroutine to call the given function object
class MockCR : public RGWCoroutine {
  std::function<int()> func;
 public:
  MockCR(std::function<int()>&& func)
    : RGWCoroutine(g_ceph_context), func(std::move(func))
  {}
  int operate() override {
    reenter(this) {
      int ret = func();
      if (ret < 0) {
        return set_cr_error(ret);
      }
      return set_cr_done();
    }
    return 0;
  }
};

// mock lease cr to start locked and sleep until unlocked
class MockLeaseCR : public RGWBaseContinuousLeaseCR {
  bool locked{true};
 public:
  MockLeaseCR() : RGWBaseContinuousLeaseCR(g_ceph_context) {}

  bool is_locked() override { return locked; }
  void set_locked(bool l) override { locked = l; set_sleeping(false); }
  void go_down() override { set_locked(false); }
  void abort() override { set_locked(false); }

  int operate() override {
    reenter(this) {
      while (locked) {
        yield set_sleeping(true);
      }
      return set_cr_done();
    }
    return 0;
  }
};

// mock omap mgr stores entries in memory
class MockShardedOmapManager : public RGWBaseShardedOmapCRManager {
  // store the entries for each shard
  using Shard = std::vector<std::string>;
  std::vector<Shard> shards;
  boost::intrusive_ptr<RGWBaseContinuousLeaseCR> cr;

 public:
  MockShardedOmapManager(RGWCoroutine *parent, int num_shards)
    : shards(num_shards)
  {
    // spawn a coroutine that will remain running until finish(). this
    // simulates the RGWOmapAppend crs, to make sure we clean up properly on
    // error paths - failure to call finish() will lead to a coroutine deadlock
    cr = new MockLeaseCR();
    parent->spawn(cr.get(), false);
  }
  bool append(const string& entry, int shard_id) override {
    shards[shard_id].push_back(entry);
    return true;
  }
  bool finish() override {
    cr->go_down();
    return true;
  }
  uint64_t get_total_entries(int shard_id) override {
    return shards[shard_id].size();
  }
};

// default no-op mock for RGWFetchAllMetaEnv
class MockFetchAllMetaEnv : public RGWFetchAllMetaEnv {
 public:
  RGWBaseContinuousLeaseCR* create_lease(RGWCoroutine *parent) override {
    return new MockLeaseCR();
  }
  RGWBaseShardedOmapCRManager* create_omap_mgr(RGWCoroutine *parent,
                                               int num_shards) override {
    return new MockShardedOmapManager(parent, num_shards);
  }
  RGWCoroutine* create_read_sections(std::list<std::string> *res) override {
    return new MockCR([] { return 0; });
  }
  RGWCoroutine* create_read_keys(const std::string& section,
                                 std::list<std::string> *res) override {
    return new MockCR([] { return 0; });
  }
  RGWCoroutine* create_write_marker(int shard_id,
                                    const rgw_meta_sync_marker& m) override {
    return new MockCR([] { return 0; });
  }
  int get_log_shard_id(const std::string& section,
                       const std::string& key, int *shard_id) override {
    *shard_id = 0; // map everything to the first shard
    return 0;
  }
};

using Markers = std::map<uint32_t, rgw_meta_sync_marker>;


// test lease failure

// mock lease that injects the given error
class FailedLeaseCR : public RGWBaseContinuousLeaseCR {
  const int err;
 public:
  FailedLeaseCR(int err) : RGWBaseContinuousLeaseCR(g_ceph_context), err(err) {}
  bool is_locked() override { return false; }
  void set_locked(bool status) override {}
  void go_down() override {}
  void abort() override {}
  int operate() override { return set_cr_error(retcode = err); }
};

TEST(FetchAllMeta, LeaseBusy)
{
  struct MockEnv : public MockFetchAllMetaEnv {
    RGWBaseContinuousLeaseCR* create_lease(RGWCoroutine *parent) override {
      return new FailedLeaseCR(-EBUSY);
    }
  } env;
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(-EBUSY, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                     markers.size(), markers)));
}

TEST(FetchAllMeta, LeaseLost)
{
  class MockEnv : public MockFetchAllMetaEnv {
    boost::intrusive_ptr<RGWBaseContinuousLeaseCR> lease;
   public:
    RGWBaseContinuousLeaseCR* create_lease(RGWCoroutine *parent) override {
      lease = new MockLeaseCR();
      return lease.get();
    }
    RGWCoroutine* create_read_sections(std::list<std::string> *res) override {
      return new MockCR([res] { res->emplace_back("section"); return 0; });
    }
    RGWCoroutine* create_read_keys(const std::string& section,
                                   std::list<std::string> *res) override {
      lease->set_locked(false);
      return new MockCR([res] { res->emplace_back("key"); return 0; });
    }
  } env;
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(-EBUSY, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                     markers.size(), markers)));
}


// test omap append failures

TEST(FetchAllMeta, OmapAppendErr)
{
  // test that append() failures are non-fatal, as long as finish() succeeds
  struct MockEnv : public MockFetchAllMetaEnv {
    RGWBaseShardedOmapCRManager* create_omap_mgr(RGWCoroutine *parent,
                                                 int num_shards) override {
      class MockOmapMgr : public RGWBaseShardedOmapCRManager {
       public:
        bool append(const string&, int) override { return false; }
        bool finish() override { return true; }
        uint64_t get_total_entries(int) override { return 0; }
      };
      return new MockOmapMgr();
    }
  } env;
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(0, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                markers.size(), markers)));
}

TEST(FetchAllMeta, OmapFinishErr)
{
  // test that finish() failures lead to -EIO
  struct MockEnv : public MockFetchAllMetaEnv {
    RGWBaseShardedOmapCRManager* create_omap_mgr(RGWCoroutine *parent,
                                                 int num_shards) override {
      class MockOmapMgr : public RGWBaseShardedOmapCRManager {
       public:
        bool append(const string&, int) override { return true; }
        bool finish() override { return false; }
        uint64_t get_total_entries(int) override { return 0; }
      };
      return new MockOmapMgr();
    }
  } env;
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(-EIO, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                   markers.size(), markers)));
}


// test get_log_shard_id errors

TEST(FetchAllMeta, LogShardErr)
{
  struct MockEnv : public MockFetchAllMetaEnv {
    // return an obscure error code
    int get_log_shard_id(const std::string&, const std::string&, int*) override {
      return -EXDEV;
    }
    // return a section/key so we have something to call get_log_shard_id() on
    RGWCoroutine* create_read_sections(std::list<std::string> *res) override {
      return new MockCR([res] { res->emplace_back("section"); return 0; });
    }
    RGWCoroutine* create_read_keys(const std::string& section,
                                   std::list<std::string> *res) override {
      return new MockCR([res] { res->emplace_back("key"); return 0; });
    }
  } env;
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(-EXDEV, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                     markers.size(), markers)));
}


// test successful crs with varying section data

// in-memory representation of the full sync map
using AllMetaMap = std::map<std::string, std::list<std::string>>;

// mock that satisfies reads from the given sync map
class Readable_FetchAllMetaEnv : public MockFetchAllMetaEnv {
  AllMetaMap sections;
 public:
  Readable_FetchAllMetaEnv() = default;
  Readable_FetchAllMetaEnv(AllMetaMap&& sections)
    : sections(std::move(sections)) {}

  RGWCoroutine* create_read_sections(std::list<std::string> *res) override {
    return new MockCR([this, res] {
                            for (auto& s : sections) {
                              res->push_back(s.first);
                            }
                            return 0;
                          });
  }
  RGWCoroutine* create_read_keys(const std::string& section,
                                 std::list<std::string> *res) override {
    return new MockCR([this, &section, res] {
                            auto i = sections.find(section);
                            if (i == sections.end()) {
                              return -ENOENT;
                            }
                            *res = i->second; // return a copy
                            return 0;
                          });
  }
};

TEST(FetchAllMeta, ReadNoSections)
{
  Readable_FetchAllMetaEnv env;
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(0, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                markers.size(), markers)));
  ASSERT_EQ(0u, markers[0].total_entries);
}

TEST(FetchAllMeta, ReadEmptySection)
{
  AllMetaMap sections{
    {"bucket", {}},
  };
  Readable_FetchAllMetaEnv env(std::move(sections));
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(0, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                markers.size(), markers)));
  ASSERT_EQ(0u, markers[0].total_entries);
}

TEST(FetchAllMeta, ReadTwoSections)
{
  AllMetaMap sections{
    {"bucket", {"a","b","c"}},
    {"user", {"d", "e", "f"}},
  };
  // TODO: test for rearrange_sections()
  Readable_FetchAllMetaEnv env(std::move(sections));
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(0, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                markers.size(), markers)));
  ASSERT_EQ(6u, markers[0].total_entries);
}


// test read failures

TEST(FetchAllMeta, ReadSectionsEIO)
{
  struct MockEnv : public MockFetchAllMetaEnv {
    RGWCoroutine* create_read_sections(std::list<std::string> *res) override {
      return new MockCR([] { return -EIO; });
    }
  } env;
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(-EIO, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                   markers.size(), markers)));
}

TEST(FetchAllMeta, ReadKeysENOENT)
{
  // return a single section "foo", and fail to read that section with ENOENT
  struct MockEnv : public MockFetchAllMetaEnv {
    RGWCoroutine* create_read_sections(std::list<std::string> *res) override {
      return new MockCR([res] { res->emplace_back("foo"); return 0; });
    }
    RGWCoroutine* create_read_keys(const std::string& section,
                                   std::list<std::string> *res) override {
      return new MockCR([] { return -ENOENT; });
    }
  } env;
  RGWCoroutinesManager crs(g_ceph_context, nullptr);
  Markers markers{{0, {}}};
  ASSERT_EQ(-ENOENT, crs.run(create_fetch_all_meta_cr(g_ceph_context, &env,
                                                      markers.size(), markers)));
}


// global context init
int main(int argc, char** argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
