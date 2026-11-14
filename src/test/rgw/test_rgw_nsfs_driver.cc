// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "rgw_sal_nsfs.h"
#include <gtest/gtest.h>
#include <array>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <sys/xattr.h>
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "rgw_mime.h"

using namespace rgw::sal;

const std::string ATTR1{"attr1"};
const std::string ATTR2{"attr2"};
const std::string ATTR3{"attr3"};
const std::string ATTR_OBJECT_TYPE{"object_type"};

namespace {
  bool do_create = false;
  bool do_delete = false;
  bool verbose = false;
}

namespace sf = std::filesystem;
class Environment* env;
sf::path base_path{"nsfstest"};
std::unique_ptr<nsfs::Directory> root;
std::unique_ptr<nsfs::FSStrategy> fs_strategy;
std::vector<const char*> args;

/* The strategies a driverless FSEnt runs with.
 *
 * An FSEnt reads and writes attributes and names scaffolding, so it
 * carries a strategy for each and dereferences them;  the tests below
 * build Directory and File objects with no driver, and until this
 * existed every one of them died on a null pointer.
 *
 * Fixed rather than probed, deliberately.  A unit test wants a known
 * layout, not whatever the filesystem under the build directory
 * happens to support, and a driver's own selection is factored into
 * NSFSDriver::init_strategies() for the cases that want it. */
class DriverlessStrategies {
public:
  nsfs::POSIXStrategy fs;
  nsfs::PerPartMPUStrategy mpu;
  nsfs::PrefixedXattrStrategy xattr;
  nsfs::SentinelPathStrategy path;
  nsfs::ReservedNames reserved;

  DriverlessStrategies() {
    auto add = [this](const nsfs::ReservedNames& r) {
      reserved.exact.insert(reserved.exact.end(),
			    r.exact.begin(), r.exact.end());
      reserved.prefixes.insert(reserved.prefixes.end(),
			       r.prefixes.begin(), r.prefixes.end());
      reserved.staging_prefixes.insert(reserved.staging_prefixes.end(),
				       r.staging_prefixes.begin(),
				       r.staging_prefixes.end());
      reserved.content_exact.insert(reserved.content_exact.end(),
				    r.content_exact.begin(),
				    r.content_exact.end());
    };
    add(path.reserved_names());
    add(mpu.reserved_names());
  }

  void seed(nsfs::FSEnt* ent) {
    ent->set_mpu_strategy(&mpu);
    ent->set_xattr_strategy(&xattr);
    ent->set_path_strategy(&path);
    ent->set_reserved_names(&reserved);
  }
};

std::unique_ptr<DriverlessStrategies> strategies;

class Environment : public ::testing::Environment {
public:
  boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<DoutPrefix> dp;
  DoutPrefixProvider* dpp{nullptr};

  Environment() {}

  virtual ~Environment() {}

  void SetUp() override {
    if (do_create) {
      sf::remove_all(base_path);
    }
    sf::create_directories(base_path);

    args.push_back("--rgw_multipart_min_part_size=32");
    args.push_back("--debug-rgw=20");
    args.push_back("--debug-ms=1");

    cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                      CODE_ENVIRONMENT_UTILITY,
                      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

    /* A real prefix provider, not nullptr.  The driver reads
     * configuration through dpp->get_cct() on the write path, so a null
     * one crashes every test that puts an object. */
    dp = std::make_unique<DoutPrefix>(cct.get(), ceph_subsys_rgw,
				      "nsfs unittest: ");
    dpp = dp.get();

    rgw_mime_init(dpp, cct.get());

    strategies = std::make_unique<DriverlessStrategies>();
    root = std::make_unique<nsfs::Directory>(base_path, nullptr, cct.get(),
					     &strategies->fs);
    strategies->seed(root.get());
    ASSERT_EQ(root->open(dpp), 0);

    if (verbose) {
      std::cout << "=== Environment::SetUp base_path=" << base_path << std::endl;
    }
  }

  void TearDown() override {
    if (do_delete) {
      sf::remove_all(base_path);
      if (verbose) {
        std::cout << "=== Environment::TearDown removed " << base_path << std::endl;
      }
    } else if (verbose) {
      std::cout << "=== Environment::TearDown preserved " << base_path << std::endl;
    }
  }
};


static inline void add_attr(Attrs& attrs, const std::string& name, const std::string& value)
{
  bufferlist bl;
  encode(value, bl);

  attrs[name] = bl;
}

static inline bool get_attr(Attrs& attrs, const char* name, bufferlist& bl)
{
  auto iter = attrs.find(name);
  if (iter == attrs.end()) {
    return false;
  }

  bl = iter->second;
  return true;
}

template <typename F>
static bool decode_attr(Attrs &attrs, const char *name, F &f) {
  bufferlist bl;
  if (!get_attr(attrs, name, bl)) {
    return false;
  }
  F tmpf;
  try {
    auto bufit = bl.cbegin();
    decode(tmpf, bufit);
  } catch (buffer::error &err) {
    return false;
  }

  f = tmpf;
  return true;
}

class TestDirectory : public nsfs::Directory {
public:
  TestDirectory(std::string _name, nsfs::Directory* _parent, CephContext* _ctx)
    : nsfs::Directory(_name, _parent, _ctx) {}
  TestDirectory(std::string _name, nsfs::Directory* _parent, struct statx& _stx, CephContext* _ctx)
    : nsfs::Directory(_name, _parent, _stx, _ctx) {}
  virtual ~TestDirectory() { close(); }

  bool get_stat_done() { return stat_done; }
};

class TestFile : public nsfs::File {
public:
  TestFile(std::string _name, nsfs::Directory* _parent, CephContext* _ctx)
    : nsfs::File(_name, _parent, _ctx) {}
  TestFile(std::string _name, nsfs::Directory* _parent, struct statx& _stx, CephContext* _ctx)
    : nsfs::File(_name, _parent, _stx, _ctx) {}
  virtual ~TestFile() { close(); }

  bool get_stat_done() { return stat_done; }
};

std::string get_test_name()
{
  std::string suitename =
      testing::UnitTest::GetInstance()->current_test_info()->test_suite_name();
  std::string testname =
      testing::UnitTest::GetInstance()->current_test_info()->name();

  return suitename + testname;
}


// Directory

TEST(FSEnt, DirCreate)
{
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<nsfs::Directory> testdir =
    std::make_unique<nsfs::Directory>(dirname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));

  bool existed{false};
  int ret = testdir->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
}

TEST(FSEnt, DirBase)
{
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<TestDirectory> testdir =
    std::make_unique<TestDirectory>(dirname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));

  bool existed{false};
  int ret = testdir->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));

  EXPECT_EQ(testdir->get_fd(), -1);
  EXPECT_EQ(testdir->get_name(), dirname);
  EXPECT_EQ(testdir->get_parent(), root.get());
  EXPECT_FALSE(testdir->exists());
  EXPECT_EQ(testdir->get_type(), nsfs::ObjectType::DIRECTORY);
  EXPECT_FALSE(testdir->get_stat_done());

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(testdir->get_fd(), 0);

  ret = testdir->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(testdir->get_stat_done());
  EXPECT_TRUE(S_ISDIR(testdir->get_stx().stx_mode));

  Attrs attrs;
  add_attr(attrs, ATTR1, ATTR1);
  add_attr(attrs, ATTR2, ATTR2);
  Attrs extra_attrs;
  add_attr(extra_attrs, ATTR3, ATTR3);

  ret = testdir->write_attrs(env->dpp, null_yield, attrs, &extra_attrs);
  EXPECT_EQ(ret, 0);

  attrs.clear();
  ret = testdir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  std::string val;
  bool success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);
  nsfs::ObjectType type;
  success = decode_attr(attrs, ATTR_OBJECT_TYPE.c_str(), type);
  EXPECT_TRUE(success);
  EXPECT_EQ(type.type, nsfs::ObjectType::DIRECTORY);

  ret = testdir->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(testdir->get_fd(), -1);

  bufferlist bl;
  ret = testdir->write(0, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, -EINVAL);

  ret = testdir->read(0, 50, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, -EINVAL);

  ret = testdir->link_temp_file(env->dpp, null_yield, dirname);
  EXPECT_EQ(ret, -EINVAL);

  std::string copyname{dirname + "-copy"};
  sf::path cp{base_path / copyname};
  sf::remove_all(cp);
  EXPECT_FALSE(sf::exists(cp));
  ret = testdir->copy(env->dpp, null_yield, root.get(), copyname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(cp));
  EXPECT_TRUE(sf::is_directory(tp));

  std::unique_ptr<TestDirectory> copydir =
    std::make_unique<TestDirectory>(copyname, root.get(), env->cct.get());
  ret = copydir->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(copydir->get_fd(), 0);

  ret = copydir->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(copydir->get_stat_done());
  EXPECT_TRUE(S_ISDIR(copydir->get_stx().stx_mode));

  attrs.clear();
  ret = copydir->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);

  ret = copydir->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(copydir->get_fd(), -1);

  std::unique_ptr<nsfs::FSEnt> ent;
  ret = root->get_ent(env->dpp, null_yield, dirname, std::string(), ent);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ent->get_type(), nsfs::ObjectType::DIRECTORY);

  ret = testdir->remove(env->dpp, null_yield, false);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(sf::exists(tp));
}

TEST(FSEnt, DirAddDir)
{
  bool existed{false};
  std::string dirname = get_test_name();
  sf::path tp{base_path / dirname};
  std::unique_ptr<nsfs::Directory> testdir =
    std::make_unique<nsfs::Directory>(dirname, root.get(), env->cct.get());
  int ret = testdir->create(env->dpp, &existed);
  EXPECT_EQ(ret, 0);

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  std::string subdirname{"SubDir"};
  sf::path sp{base_path / dirname / subdirname};
  std::unique_ptr<nsfs::Directory> subdir =
    std::make_unique<nsfs::Directory>(subdirname, testdir.get(), env->cct.get());
  ret = subdir->create(env->dpp, &existed);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(sp));
  EXPECT_TRUE(sf::is_directory(sp));

  ret = subdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  std::string subsubdirname{"SubSubDir"};
  sf::path ssp{base_path / dirname / subdirname / subsubdirname};
  std::unique_ptr<nsfs::Directory> subsubdir =
    std::make_unique<nsfs::Directory>(subsubdirname, subdir.get(), env->cct.get());
  ret = subsubdir->create(env->dpp, &existed);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(ssp));
  EXPECT_TRUE(sf::is_directory(ssp));
}


// File

TEST(FSEnt, FileCreateReal)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  TestFile testfile{fname, root.get(), env->cct.get()};

  EXPECT_FALSE(sf::exists(tp));

  bool existed{false};
  int ret = testfile.create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
}

TEST(FSEnt, FileCreateTemp)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  TestFile testfile{fname, root.get(), env->cct.get()};

  EXPECT_FALSE(sf::exists(tp));

  bool existed{false};
  int ret = testfile.create(env->dpp, &existed, true);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_FALSE(sf::exists(tp));

  std::string temp_fname{fname + "-blargh"};
  ret = testfile.link_temp_file(env->dpp, null_yield, temp_fname);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
}

TEST(FSEnt, FileBase)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  std::unique_ptr<TestFile> testfile =
    std::make_unique<TestFile>(fname, root.get(), env->cct.get());

  EXPECT_FALSE(sf::exists(tp));
  EXPECT_EQ(testfile->get_fd(), -1);

  bool existed{false};
  int ret = testfile->create(env->dpp, &existed);

  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(existed);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
  // create() opens
  EXPECT_GT(testfile->get_fd(), 0);

  EXPECT_EQ(testfile->get_name(), fname);
  EXPECT_EQ(testfile->get_parent(), root.get());
  EXPECT_FALSE(testfile->exists());
  EXPECT_EQ(testfile->get_type(), nsfs::ObjectType::FILE);
  EXPECT_FALSE(testfile->get_stat_done());

  ret = testfile->open(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_GT(testfile->get_fd(), 0);

  ret = testfile->stat(env->dpp, false);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(testfile->get_stat_done());
  EXPECT_TRUE(S_ISREG(testfile->get_stx().stx_mode));

  Attrs attrs;
  add_attr(attrs, ATTR1, ATTR1);
  add_attr(attrs, ATTR2, ATTR2);
  Attrs extra_attrs;
  add_attr(extra_attrs, ATTR3, ATTR3);

  ret = testfile->write_attrs(env->dpp, null_yield, attrs, &extra_attrs);
  EXPECT_EQ(ret, 0);

  attrs.clear();
  ret = testfile->read_attrs(env->dpp, null_yield, attrs);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(attrs.size(), 4);
  std::string val;
  bool success = decode_attr(attrs, ATTR1.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR1);
  success = decode_attr(attrs, ATTR2.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR2);
  success = decode_attr(attrs, ATTR3.c_str(), val);
  EXPECT_TRUE(success);
  EXPECT_EQ(val, ATTR3);
  nsfs::ObjectType type;
  success = decode_attr(attrs, ATTR_OBJECT_TYPE.c_str(), type);
  EXPECT_TRUE(success);
  EXPECT_EQ(type.type, nsfs::ObjectType::FILE);

  ret = testfile->close();
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(testfile->get_fd(), -1);

  std::unique_ptr<nsfs::FSEnt> ent;
  ret = root->get_ent(env->dpp, null_yield, fname, std::string(), ent);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(ent->get_type(), nsfs::ObjectType::FILE);

  ret = testfile->remove(env->dpp, null_yield, false);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(sf::exists(tp));
}

TEST(FSEnt, FileReadWrite)
{
  std::string fname = get_test_name();
  sf::path tp{base_path / fname};
  std::unique_ptr<nsfs::File> testfile{
    std::make_unique<nsfs::File>(fname, root.get(), env->cct.get())};

  int ret = testfile->create(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));

  bufferlist bl;
  encode(fname, bl);
  int len = bl.length();
  ret = testfile->write(0, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(sf::file_size(tp), len);

  bl.clear();
  ret = testfile->read(0, 50, bl, env->dpp, null_yield);
  EXPECT_EQ(ret, len);

  std::string result;
  EXPECT_NO_THROW({
    auto bufit = bl.cbegin();
    decode(result, bufit);
  });

  EXPECT_EQ(result, fname);
}


template <typename YP>
struct TestMultipartCache : public nsfs::MultipartCache
{
public:
  TestMultipartCache() :
    MultipartCache(1, 1, 1, 1, file::listing::MultipartCachePolicy::writethrough)
  { }
  virtual ~TestMultipartCache() { }

  file::listing::MultipartListResult list_parts(const file::listing::MultipartCacheKey& key,
				 uint32_t marker, uint32_t max_parts,
				 const file::listing::fill_parts_fn_t& fill_fn) override {
    file::listing::MultipartListResult result;
    boost::container::flat_map<uint32_t, file::listing::MultipartPartInfo> parts;

    fill_fn(parts);
    auto it = parts.lower_bound(marker + 1);
    uint32_t count = 0;
    while (it != parts.end() && count < max_parts) {
      result.parts.push_back(it->second);
      result.next_marker = it->first;
      ++it;
      ++count;
    }
    result.truncated = (it != parts.end());
    return result;
  };

  void remove(const file::listing::MultipartCacheKey& key) override { }

  bool add_part(const file::listing::MultipartCacheKey& key,
                file::listing::MultipartPartInfo&& info) override {
    return false;
  }
};

// Driver

class TestUser;
class TestDriver : public NSFSDriver
{
public:
  std::string driver_base;

  TestDriver(std::string _base_path) : NSFSDriver(nullptr), driver_base(_base_path)
  { }
  virtual ~TestDriver() = default;

  int init(const DoutPrefixProvider* dpp,
	   std::optional<bool> shares_extents = std::nullopt,
	   file::listing::MultipartCachePolicy mp_policy =
	     file::listing::MultipartCachePolicy::writethrough,
	   uint64_t mp_entries = 16, uint64_t mp_lanes = 2,
	   uint64_t mp_parts = 2, uint64_t mp_budget = 64)
  {
    std::string cache_base = driver_base + "/cache";
    base_path = driver_base + "/root";

      fs_strategy = std::make_unique<nsfs::POSIXStrategy>();
    root_dir = std::make_unique<nsfs::Directory>(base_path, nullptr, env->cct.get(),
                                                 fs_strategy.get());
    int ret = root_dir->open(env->dpp);

    std::error_code ec;
    if (ec) {
      return -ec.value();
    }

    init_strategies(dpp, shares_extents);

    if (ret < 0) {
      return ret;
    }

    quota_handler = RGWQuotaHandler::generate_handler(this, false);
    bucket_cache.reset(new nsfs::BucketCache(
        this, base_path, cache_base, 100, 3, 3, 3));
    multipart_cache.reset(new TestMultipartCache<cohort::lru::NullYieldPolicy>());
    init_multipart_cache(dpp, mp_entries, mp_lanes, mp_parts, mp_budget,
			 mp_policy);

    ldpp_dout(env->dpp, 20) << "SUCCESS" << dendl;
    return 0;
  }
  virtual CephContext* ctx(void) override {
    return get_pointer(env->cct);
  }

  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
};

class TestUser : public StoreUser {
  Attrs attrs;

public:
  TestUser(TestDriver *_dr, const rgw_user& _u) : StoreUser(_u) { }
  TestUser(TestDriver *_dr, const RGWUserInfo& _i) : StoreUser(_i) { }
  TestUser(TestDriver *_dr)  { }
  TestUser(TestUser& _o) = default;
  virtual ~TestUser() = default;

  virtual std::unique_ptr<User> clone() override {
    return std::unique_ptr<User>(new TestUser(*this));
  }
  virtual Attrs& get_attrs() override { return attrs; }
  virtual void set_attrs(Attrs &_attrs) override { attrs = _attrs; }
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs&
				    new_attrs, optional_yield y) override { return 0; }
  virtual int read_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
             uint64_t end_epoch, uint32_t max_entries, bool* is_truncated,
             RGWUsageIter &usage_iter,
             std::map<rgw_user_bucket, rgw_usage_log_entry> &usage) override { return 0; }
  virtual int trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch, optional_yield y) override { return 0; }
  virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool
			 exclusive, RGWUserInfo* old_info = nullptr) override { return 0; }
  virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override { return 0; }
  virtual int verify_mfa(const std::string &mfa_str, bool *verified,
                         const DoutPrefixProvider* dpp,
                         optional_yield y) override { return 0; }
  virtual int list_groups(const DoutPrefixProvider *dpp, optional_yield y,
                          std::string_view marker, uint32_t max_items,
                          GroupList &listing) override { return -ENOTSUP; }
};

std::unique_ptr<User> TestDriver::get_user(const rgw_user &u)
{
  return std::make_unique<TestUser>(this, u);
}

TEST(NSFSDriver, CreateDriver)
{
  std::string name = get_test_name();
  sf::path bp{sf::absolute(sf::path{base_path / name})};
  sf::create_directory(bp);
  sf::create_directory(bp / "cache");
  sf::create_directory(bp / "root");
  TestDriver driver{bp};

  sf::path tp{bp / "root"};

  int ret = driver.init(env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
}

class NSFSDriverTest : public ::testing::Test {
  protected:
    std::unique_ptr<TestDriver> driver;
    rgw_owner owner;
    ACLOwner acl_owner;
    sf::path bp;
    std::string testname;

  public:
    NSFSDriverTest() {}

    /* Probe the filesystem, which on a developer machine always reports
     * that extents are shared.  A fixture which means to exercise the
     * strided layout overrides this. */
    virtual std::optional<bool> shares_extents() const {
      return std::nullopt;
    }

    /* writethrough, as a deployment gets by default */
    virtual file::listing::MultipartCachePolicy mp_cache_policy() const {
      return file::listing::MultipartCachePolicy::writethrough;
    }

    /* max entries, lanes, partitions, parts budget.
     *
     * Large enough that nothing is evicted, which is what a
     * deployment gets.  A fixture that means to reach the eviction
     * path -- the only thing that writes a record under writeback --
     * shrinks it until a second upload displaces the first. */
    virtual std::array<uint64_t, 4> mp_cache_sizes() const {
      return {16, 2, 2, 64};
    }

    void SetUp() {
      testname = get_test_name();
      bp = sf::path{sf::absolute(sf::path{base_path / testname})};
      sf::create_directories(bp / "cache");
      sf::create_directories(bp / "root");
      driver = std::make_unique<TestDriver>(bp);
      const auto sz = mp_cache_sizes();
      int ret = driver->init(env->dpp, shares_extents(), mp_cache_policy(),
			     sz[0], sz[1], sz[2], sz[3]);
      EXPECT_EQ(ret, 0);

      rgw_user uid{"tenant", testname};
      owner = uid;
      acl_owner.id = owner;

      if (verbose) {
        std::cout << "--- " << testname << " SetUp bp=" << bp << std::endl;
      }
    }

    void TearDown() {
      if (do_delete) {
        sf::remove_all(bp);
      }
    }
};

TEST_F(NSFSDriverTest, Bucket)
{
  RGWBucketInfo info;
  info.bucket.name = testname;
  info.owner = owner;
  info.creation_time = ceph::real_clock::now();

  std::unique_ptr<rgw::sal::Bucket> bucket = driver->get_bucket(info);
  EXPECT_NE(bucket.get(), nullptr);
  EXPECT_EQ(bucket->get_name(), testname);
  EXPECT_EQ(bucket->get_key().name, testname);
  EXPECT_EQ(bucket->get_key().tenant, "");
  EXPECT_EQ(bucket->get_key().bucket_id, "");
  EXPECT_FALSE(bucket->versioned());
  EXPECT_FALSE(bucket->versioning_enabled());
}

TEST_F(NSFSDriverTest, BucketCreate)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  rgw::sal::Bucket::CreateParams createparams;

  RGWBucketInfo info;
  info.bucket.name = testname;
  info.owner = owner;
  info.creation_time = ceph::real_clock::now();
  bucket = driver->get_bucket(info);
  EXPECT_NE(bucket.get(), nullptr);

  createparams.owner = owner;

  int ret = bucket->create(env->dpp, createparams, null_yield, rgw::sal::no_authenticated_user);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(bucket->get_name(), testname);
  EXPECT_EQ(bucket->get_key().name, testname);
  EXPECT_EQ(bucket->get_key().tenant, "");
  // bucket_id is set to a random value

  sf::path tp{bp / "root" / testname};
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
}

class NSFSBucketTest : public NSFSDriverTest {
protected:
  std::unique_ptr<rgw::sal::Bucket> bucket;

public:
  NSFSBucketTest() {}

  void SetUp() {
    NSFSDriverTest::SetUp();

    RGWBucketInfo info;
    info.bucket.name = testname;
    info.owner = owner;
    info.creation_time = ceph::real_clock::now();

    bucket = driver->get_bucket(info);
    EXPECT_NE(bucket.get(), nullptr);

    rgw::sal::Bucket::CreateParams createparams;
    createparams.owner = owner;
    int ret = bucket->create(env->dpp, createparams, null_yield, rgw::sal::no_authenticated_user);
    EXPECT_EQ(ret, 0);
  }

  void TearDown() {
    NSFSDriverTest::TearDown();
  }
};

/* A bucket created strong, for the tests that exercise FSIO.
 *
 * New buckets are shared by default (rgw_nsfs_bucket_profile), and FSIO
 * is rejected on a bucket without the shadow subtree.  The option is
 * read when the bucket is created and marked, so it is restored as soon
 * as the fixture's bucket exists. */
class NSFSStrongBucketTest : public NSFSBucketTest {
public:
  void SetUp() override {
    g_conf().set_val("rgw_nsfs_bucket_profile", "strong");
    NSFSBucketTest::SetUp();
    g_conf().set_val("rgw_nsfs_bucket_profile", "shared");
    auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
    ASSERT_EQ(b->resolve_profile(env->dpp), 0);
    ASSERT_STREQ(b->profile_name(), "strong");
  }
};

  static constexpr const char* key = "user.nsfs:bucket_info";


TEST_F(NSFSBucketTest, Object)
{
  std::unique_ptr<rgw::sal::Object> object =
    bucket->get_object(rgw_obj_key(testname));
  EXPECT_NE(object.get(), nullptr);
  EXPECT_EQ(object->get_name(), testname);
  EXPECT_EQ(object->get_key().name, testname);
  EXPECT_EQ(object->get_bucket(), bucket.get());
}

TEST_F(NSFSBucketTest, ObjectWrite)
{
  sf::path tp{bp / "root" / testname / testname};
  EXPECT_FALSE(sf::exists(tp));

  std::unique_ptr<rgw::sal::Object> object =
    bucket->get_object(rgw_obj_key(testname));
  EXPECT_NE(object.get(), nullptr);

  std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
      env->dpp, null_yield, object.get(), acl_owner, nullptr, 0, testname,
      rgw::sal::no_authenticated_user);
  EXPECT_NE(writer.get(), nullptr);

  int ret = writer->prepare(null_yield);
  EXPECT_EQ(ret, 0);

  int ofs{0};
  std::string etag;
  for (int i = 0; i < 4; ++i) {
    bufferlist bl;
    encode(testname, bl);
    int len = bl.length();

    ret = writer->process(std::move(bl), ofs);
    EXPECT_EQ(ret, 0);

    ofs += len;
  }

  ret = writer->process({}, ofs);
  EXPECT_EQ(ret, 0);

  ceph::real_time mtime;
  Attrs attrs;
  bufferlist bl;
  encode(ATTR1, bl);
  attrs[ATTR1] = bl;
  req_context rctx{env->dpp, null_yield, nullptr};

  ret = writer->complete(ofs, etag, &mtime, real_time(), attrs, std::nullopt,
                         real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(object->get_size(), ofs);

  bufferlist getbl = object->get_attrs()[ATTR1];
  EXPECT_EQ(bl, getbl);

  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_regular_file(tp));
}

class NSFSObjectTest : public NSFSBucketTest {
protected:
  std::unique_ptr<rgw::sal::Object> object;
  uint64_t write_size{0};
  bufferlist write_data;

public:
  NSFSObjectTest() {}

  void SetUp() {
    NSFSBucketTest::SetUp();
    object = write_object(testname);
  }

  std::unique_ptr<rgw::sal::Object> write_object(std::string objname) {
    std::unique_ptr<rgw::sal::Object> obj =
      bucket->get_object(rgw_obj_key(objname));
    EXPECT_NE(obj.get(), nullptr);

    std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    EXPECT_NE(writer.get(), nullptr);

    int ret = writer->prepare(null_yield);
    EXPECT_EQ(ret, 0);

    std::string etag;
    for (int i = 0; i < 4; ++i) {
      bufferlist bl;
      encode(objname, bl);
      int len = bl.length();

      write_data.append(bl);

      ret = writer->process(std::move(bl), write_size);
      EXPECT_EQ(ret, 0);

      write_size += len;
    }

    ret = writer->process({}, write_size);
    EXPECT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs attrs;
    add_attr(attrs, ATTR1, ATTR1);
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(write_size, etag, &mtime, real_time(), attrs,
                           std::nullopt, real_time(), nullptr, nullptr, nullptr,
                           nullptr, nullptr, rctx, 0);
    EXPECT_EQ(ret, 0);

    return obj;
  }

  void TearDown() { NSFSBucketTest::TearDown(); }
};

class Read_CB : public RGWGetDataCB
{
public:
  bufferlist *save_bl;
  explicit Read_CB(bufferlist *_bl) : save_bl(_bl) {}
  ~Read_CB() override {}

  int handle_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) override {
    save_bl->append(bl);
    return 0;
  }
};

TEST_F(NSFSObjectTest, ObjectRead)
{
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(object->get_read_op(rgw::sal::no_authenticated_user));

  int ret = read_op->prepare(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(object->get_size(), write_size);

  bufferlist bl;
  Read_CB cb(&bl);
  ret = read_op->iterate(env->dpp, 0, write_size, &cb, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(write_data, bl);
}

TEST_F(NSFSObjectTest, ObjectDelete)
{
  sf::path tp{bp / "root" / testname / testname};
  EXPECT_TRUE(sf::exists(tp));

  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = object->get_delete_op(rgw::sal::no_authenticated_user);
  int ret = del_op->delete_obj(env->dpp, null_yield, 0);
  EXPECT_EQ(ret, 0);

  EXPECT_FALSE(sf::exists(tp));
}

TEST_F(NSFSBucketTest, HierarchicalDelete)
{
  // Write a deep object
  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);
    bufferlist bl;
    encode(name, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  };

  write("dir1/dir2/file.txt");

  sf::path dir1{bp / "root" / testname / "dir1"};
  sf::path dir2{bp / "root" / testname / "dir1" / "dir2"};
  sf::path file{bp / "root" / testname / "dir1" / "dir2" / "file.txt"};
  ASSERT_TRUE(sf::is_directory(dir1));
  ASSERT_TRUE(sf::is_directory(dir2));
  ASSERT_TRUE(sf::is_regular_file(file));

  // Delete the object
  std::unique_ptr<rgw::sal::Object> obj =
    bucket->get_object(rgw_obj_key("dir1/dir2/file.txt"));
  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op(rgw::sal::no_authenticated_user);
  int ret = del_op->delete_obj(env->dpp, null_yield, 0);
  EXPECT_EQ(ret, 0);

  // File and empty parent dirs should be gone
  EXPECT_FALSE(sf::exists(file));
  EXPECT_FALSE(sf::exists(dir2));
  EXPECT_FALSE(sf::exists(dir1));
}

TEST_F(NSFSBucketTest, DeletePreservesNeighbors)
{
  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);
    bufferlist bl;
    encode(name, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  };

  write("dir1/a.txt");
  write("dir1/b.txt");

  sf::path dir1{bp / "root" / testname / "dir1"};
  sf::path file_a{bp / "root" / testname / "dir1" / "a.txt"};
  sf::path file_b{bp / "root" / testname / "dir1" / "b.txt"};
  ASSERT_TRUE(sf::is_regular_file(file_a));
  ASSERT_TRUE(sf::is_regular_file(file_b));

  // Delete only a.txt
  std::unique_ptr<rgw::sal::Object> obj =
    bucket->get_object(rgw_obj_key("dir1/a.txt"));
  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op(rgw::sal::no_authenticated_user);
  int ret = del_op->delete_obj(env->dpp, null_yield, 0);
  EXPECT_EQ(ret, 0);

  // a.txt gone, but dir1/ and b.txt preserved
  EXPECT_FALSE(sf::exists(file_a));
  EXPECT_TRUE(sf::is_directory(dir1));
  EXPECT_TRUE(sf::is_regular_file(file_b));
}

TEST_F(NSFSObjectTest, BucketList)
{
  std::unique_ptr<rgw::sal::Object> obj1 = write_object(testname + "-1");
  EXPECT_NE(obj1.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj2 = write_object(testname + "-2");
  EXPECT_NE(obj2.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj3 = write_object(testname + "-3");
  EXPECT_NE(obj3.get(), nullptr);

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  int ret = bucket->list(env->dpp, params, 128, results, null_yield);
  EXPECT_EQ(ret, 0);

  EXPECT_EQ(results.is_truncated, false);

  EXPECT_EQ(results.objs.size(), 4);

  rgw_obj_key key(results.objs[0].key);
  EXPECT_EQ(key, object->get_key());
  rgw_obj_key key1(results.objs[1].key);
  EXPECT_EQ(key1, obj1->get_key());
  rgw_obj_key key2(results.objs[2].key);
  EXPECT_EQ(key2, obj2->get_key());
  rgw_obj_key key3(results.objs[3].key);
  EXPECT_EQ(key3, obj3->get_key());
}

TEST_F(NSFSObjectTest, ObjectAttrs)
{
  int ret = object->get_obj_attrs(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);

  bufferlist origbl;
  encode(ATTR1, origbl);

  // attr1 + object_type + synthesized etag
  EXPECT_EQ(object->get_attrs().size(), 3);
  EXPECT_EQ(object->get_attrs()[ATTR1], origbl);
  EXPECT_TRUE(object->get_attrs().contains(ATTR_OBJECT_TYPE));
  EXPECT_TRUE(object->get_attrs().contains(RGW_ATTR_ETAG));
}

TEST_F(NSFSObjectTest, XattrOnDisk)
{
  sf::path obj_path{bp / "root" / testname / testname};
  ASSERT_TRUE(sf::is_regular_file(obj_path));

  char buf[8192];
  ssize_t len = listxattr(obj_path.c_str(), buf, sizeof(buf));
  ASSERT_GT(len, 0);

  std::set<std::string> xattr_names;
  const char* p = buf;
  while (p < buf + len) {
    xattr_names.insert(p);
    p += strlen(p) + 1;
  }

  if (verbose) {
    std::cout << "  on-disk xattrs for " << obj_path << ":" << std::endl;
    for (auto& x : xattr_names) {
      std::cout << "    " << x << std::endl;
    }
  }

  // nsfs-specific attrs use user.nsfs.* prefix
  EXPECT_FALSE(xattr_names.contains("user.nsfs:object_type"));
  EXPECT_FALSE(xattr_names.contains("user.nsfs:owner"));

  // user-supplied attrs use user.nsfs:* prefix
  EXPECT_TRUE(xattr_names.contains("user.nsfs:" + ATTR1));

  // RGW common attrs use user.nsfs:rgw.* prefix (etag present only if set)
  // The write_object helper doesn't set an explicit etag, so skip that check

  // no old-style prefixes
  for (auto& x : xattr_names) {
    EXPECT_EQ(x.find("user.X-RGW-"), std::string::npos)
      << "stale prefix in xattr: " << x;
    EXPECT_EQ(x.find("NSFS-"), std::string::npos)
      << "old NSFS- key in xattr: " << x;
  }
}

TEST_F(NSFSBucketTest, HierarchicalPut)
{
  std::string objname = "dir1/dir2/file.txt";
  sf::path obj_path{bp / "root" / testname / "dir1" / "dir2" / "file.txt"};
  sf::path dir1_path{bp / "root" / testname / "dir1"};
  sf::path dir2_path{bp / "root" / testname / "dir1" / "dir2"};

  EXPECT_FALSE(sf::exists(obj_path));

  std::unique_ptr<rgw::sal::Object> obj =
    bucket->get_object(rgw_obj_key(objname));
  EXPECT_NE(obj.get(), nullptr);

  std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
      env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
      rgw::sal::no_authenticated_user);
  EXPECT_NE(writer.get(), nullptr);

  int ret = writer->prepare(null_yield);
  EXPECT_EQ(ret, 0);

  bufferlist bl;
  std::string content{"hello hierarchical world"};
  encode(content, bl);
  int len = bl.length();

  ret = writer->process(std::move(bl), 0);
  EXPECT_EQ(ret, 0);

  ret = writer->process({}, len);
  EXPECT_EQ(ret, 0);

  ceph::real_time mtime;
  Attrs attrs;
  std::string etag;
  req_context rctx{env->dpp, null_yield, nullptr};

  ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                         real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
  EXPECT_EQ(ret, 0);

  EXPECT_TRUE(sf::is_directory(dir1_path));
  EXPECT_TRUE(sf::is_directory(dir2_path));
  EXPECT_TRUE(sf::is_regular_file(obj_path));
}

TEST_F(NSFSBucketTest, HierarchicalGet)
{
  std::string objname = "dir1/dir2/file.txt";
  std::string content{"hello hierarchical world"};

  // PUT
  {
    std::unique_ptr<rgw::sal::Object> obj =
      bucket->get_object(rgw_obj_key(objname));
    std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);

    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);

    bufferlist bl;
    encode(content, bl);
    int len = bl.length();

    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  }

  // GET
  {
    std::unique_ptr<rgw::sal::Object> obj =
      bucket->get_object(rgw_obj_key(objname));
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op(rgw::sal::no_authenticated_user));

    int ret = read_op->prepare(null_yield, env->dpp);
    EXPECT_EQ(ret, 0);

    bufferlist bl;
    Read_CB cb(&bl);
    ret = read_op->iterate(env->dpp, 0, obj->get_size(), &cb, null_yield);
    EXPECT_EQ(ret, 0);

    std::string result;
    auto bufit = bl.cbegin();
    decode(result, bufit);
    EXPECT_EQ(result, content);
  }
}

TEST_F(NSFSBucketTest, FlatAndHierarchicalCoexist)
{
  std::string flat_name = "flat.txt";
  std::string hier_name = "subdir/nested.txt";
  std::string flat_content{"flat content"};
  std::string hier_content{"nested content"};

  // PUT flat
  {
    std::unique_ptr<rgw::sal::Object> obj =
      bucket->get_object(rgw_obj_key(flat_name));
    std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);

    bufferlist bl;
    encode(flat_content, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  }

  // PUT hierarchical
  {
    std::unique_ptr<rgw::sal::Object> obj =
      bucket->get_object(rgw_obj_key(hier_name));
    std::unique_ptr<rgw::sal::Writer> writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);

    bufferlist bl;
    encode(hier_content, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  }

  // Verify filesystem layout
  sf::path flat_path{bp / "root" / testname / "flat.txt"};
  sf::path subdir_path{bp / "root" / testname / "subdir"};
  sf::path nested_path{bp / "root" / testname / "subdir" / "nested.txt"};
  EXPECT_TRUE(sf::is_regular_file(flat_path));
  EXPECT_TRUE(sf::is_directory(subdir_path));
  EXPECT_TRUE(sf::is_regular_file(nested_path));

  // GET flat
  {
    std::unique_ptr<rgw::sal::Object> obj =
      bucket->get_object(rgw_obj_key(flat_name));
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op(rgw::sal::no_authenticated_user));
    int ret = read_op->prepare(null_yield, env->dpp);
    EXPECT_EQ(ret, 0);

    bufferlist bl;
    Read_CB cb(&bl);
    ret = read_op->iterate(env->dpp, 0, obj->get_size(), &cb, null_yield);
    EXPECT_EQ(ret, 0);

    std::string result;
    auto bufit = bl.cbegin();
    decode(result, bufit);
    EXPECT_EQ(result, flat_content);
  }

  // GET hierarchical
  {
    std::unique_ptr<rgw::sal::Object> obj =
      bucket->get_object(rgw_obj_key(hier_name));
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op(rgw::sal::no_authenticated_user));
    int ret = read_op->prepare(null_yield, env->dpp);
    EXPECT_EQ(ret, 0);

    bufferlist bl;
    Read_CB cb(&bl);
    ret = read_op->iterate(env->dpp, 0, obj->get_size(), &cb, null_yield);
    EXPECT_EQ(ret, 0);

    std::string result;
    auto bufit = bl.cbegin();
    decode(result, bufit);
    EXPECT_EQ(result, hier_content);
  }
}

TEST_F(NSFSBucketTest, MultipartUploadComplete)
{
  std::string objname = testname + "-mp";
  std::string upload_id = "c0ffee";
  std::unique_ptr<rgw::sal::MultipartUpload> upload =
    bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  int ret = upload->init(env->dpp, null_yield, acl_owner, placement, attrs);
  ASSERT_EQ(ret, 0);

  // Write 4 parts
  bufferlist total_data;
  std::map<int, std::string> part_etags;
  for (int i = 1; i <= 4; ++i) {
    std::string part_name = "part-" + fmt::format("{:0>5}", i);
    std::unique_ptr<rgw::sal::Writer> writer =
      upload->get_writer(env->dpp, null_yield, nullptr, acl_owner,
                         &placement, i, part_name,
                         rgw::sal::no_authenticated_user);
    ASSERT_NE(writer.get(), nullptr);

    ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);

    bufferlist bl;
    std::string chunk = objname + "-part" + std::to_string(i);
    encode(chunk, bl);
    total_data.append(bl);
    int len = bl.length();

    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);

    ceph::real_time mtime;
    Attrs part_attrs;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, part_name, &mtime, real_time(), part_attrs,
                           std::nullopt, real_time(), nullptr, nullptr, nullptr,
                           nullptr, nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
    part_etags[i] = part_name;
  }

  // Complete
  std::list<rgw_obj_index_key> remove_objs;
  bool compressed = false;
  RGWCompressionInfo cs_info;
  off_t ofs{0};
  uint64_t accounted_size{0};
  std::string tag;
  rgw::sal::MultipartUpload::prefix_map_t processed_prefixes;
  ACLOwner mp_owner;
  mp_owner.id = bucket->get_owner();

  std::unique_ptr<rgw::sal::Object> mp_obj =
    bucket->get_object(rgw_obj_key(objname));

  ret = upload->complete(env->dpp, null_yield, get_pointer(env->cct),
                         part_etags, remove_objs, accounted_size, compressed,
                         cs_info, ofs, tag, mp_owner, 0, mp_obj.get(),
                         processed_prefixes);
  EXPECT_EQ(ret, 0);

  // Verify final object is a regular file, not a directory
  sf::path obj_path{bp / "root" / testname / objname};
  EXPECT_TRUE(sf::is_regular_file(obj_path));
  EXPECT_FALSE(sf::is_directory(obj_path));

  // Verify staging dir is cleaned up
  sf::path staging_path{bp / "root" / testname / (".multipart_" + upload_id)};
  EXPECT_FALSE(sf::exists(staging_path));

  // Verify content via GET
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(mp_obj->get_read_op(rgw::sal::no_authenticated_user));
  ret = read_op->prepare(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);

  bufferlist read_bl;
  Read_CB cb(&read_bl);
  ret = read_op->iterate(env->dpp, 0, mp_obj->get_size(), &cb, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(read_bl, total_data);
}

TEST_F(NSFSBucketTest, HierarchicalMultipart)
{
  std::string objname = "subdir/mp-object.bin";
  std::string upload_id = "beef42";
  std::unique_ptr<rgw::sal::MultipartUpload> upload =
    bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  int ret = upload->init(env->dpp, null_yield, acl_owner, placement, attrs);
  ASSERT_EQ(ret, 0);

  // Write one part
  std::string part_name = "part-00001";
  std::unique_ptr<rgw::sal::Writer> writer =
    upload->get_writer(env->dpp, null_yield, nullptr, acl_owner,
                       &placement, 1, part_name,
                       rgw::sal::no_authenticated_user);
  ret = writer->prepare(null_yield);
  ASSERT_EQ(ret, 0);

  bufferlist bl;
  std::string content = "hierarchical multipart content";
  encode(content, bl);
  int len = bl.length();
  ret = writer->process(std::move(bl), 0);
  ASSERT_EQ(ret, 0);
  ret = writer->process({}, len);
  ASSERT_EQ(ret, 0);

  ceph::real_time mtime;
  Attrs part_attrs;
  req_context rctx{env->dpp, null_yield, nullptr};
  ret = writer->complete(len, part_name, &mtime, real_time(), part_attrs,
                         std::nullopt, real_time(), nullptr, nullptr, nullptr,
                         nullptr, nullptr, rctx, 0);
  ASSERT_EQ(ret, 0);

  // Complete
  std::map<int, std::string> part_etags;
  part_etags[1] = part_name;
  std::list<rgw_obj_index_key> remove_objs;
  bool compressed = false;
  RGWCompressionInfo cs_info;
  off_t ofs{0};
  uint64_t accounted_size{0};
  std::string tag;
  rgw::sal::MultipartUpload::prefix_map_t processed_prefixes;
  ACLOwner mp_owner;
  mp_owner.id = bucket->get_owner();

  std::unique_ptr<rgw::sal::Object> mp_obj =
    bucket->get_object(rgw_obj_key(objname));

  ret = upload->complete(env->dpp, null_yield, get_pointer(env->cct),
                         part_etags, remove_objs, accounted_size, compressed,
                         cs_info, ofs, tag, mp_owner, 0, mp_obj.get(),
                         processed_prefixes);
  EXPECT_EQ(ret, 0);

  // Verify hierarchical placement — regular file
  sf::path subdir{bp / "root" / testname / "subdir"};
  sf::path obj_path{bp / "root" / testname / "subdir" / "mp-object.bin"};
  EXPECT_TRUE(sf::is_directory(subdir));
  EXPECT_TRUE(sf::is_regular_file(obj_path));
}

TEST_F(NSFSObjectTest, HierarchicalList)
{
  // SetUp already wrote one flat object (testname)
  // Write hierarchical objects
  std::unique_ptr<rgw::sal::Object> obj1 = write_object("dir1/a.txt");
  EXPECT_NE(obj1.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj2 = write_object("dir1/b.txt");
  EXPECT_NE(obj2.get(), nullptr);
  std::unique_ptr<rgw::sal::Object> obj3 = write_object("dir2/c.txt");
  EXPECT_NE(obj3.get(), nullptr);

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  // List all (no delimiter) — should return all 4 objects in lex order
  int ret = bucket->list(env->dpp, params, 128, results, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(results.is_truncated);
  EXPECT_EQ(results.objs.size(), 4);

  if (verbose) {
    std::cout << "  all objects:" << std::endl;
    for (auto& e : results.objs) {
      std::cout << "    " << e.key.name << std::endl;
    }
  }

  // Verify lexicographic order: uppercase 'N' < lowercase 'd' in ASCII
  EXPECT_EQ(results.objs[0].key.name, testname);
  EXPECT_EQ(results.objs[1].key.name, "dir1/a.txt");
  EXPECT_EQ(results.objs[2].key.name, "dir1/b.txt");
  EXPECT_EQ(results.objs[3].key.name, "dir2/c.txt");
}

TEST_F(NSFSBucketTest, ListWithDelimiter)
{
  // Write flat + hierarchical objects
  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);
    bufferlist bl;
    encode(name, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  };

  write("top.txt");
  write("dir1/a.txt");
  write("dir1/b.txt");
  write("dir2/c.txt");

  // List with delimiter "/"
  rgw::sal::Bucket::ListParams params;
  params.delim = "/";
  rgw::sal::Bucket::ListResults results;

  int ret = bucket->list(env->dpp, params, 128, results, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(results.is_truncated);

  if (verbose) {
    std::cout << "  objects:" << std::endl;
    for (auto& e : results.objs) {
      std::cout << "    " << e.key.name << std::endl;
    }
    std::cout << "  common_prefixes:" << std::endl;
    for (auto& cp : results.common_prefixes) {
      std::cout << "    " << cp.first << std::endl;
    }
  }

  // Contents: just top.txt
  EXPECT_EQ(results.objs.size(), 1);
  EXPECT_EQ(results.objs[0].key.name, "top.txt");

  // CommonPrefixes: dir1/, dir2/
  EXPECT_EQ(results.common_prefixes.size(), 2);
  EXPECT_TRUE(results.common_prefixes.contains("dir1/"));
  EXPECT_TRUE(results.common_prefixes.contains("dir2/"));
}

TEST_F(NSFSBucketTest, ListWithPrefix)
{
  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);
    bufferlist bl;
    encode(name, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  };

  write("top.txt");
  write("dir1/a.txt");
  write("dir1/b.txt");
  write("dir2/c.txt");

  // List with prefix "dir1/" and delimiter "/"
  rgw::sal::Bucket::ListParams params;
  params.prefix = "dir1/";
  params.delim = "/";
  rgw::sal::Bucket::ListResults results;

  int ret = bucket->list(env->dpp, params, 128, results, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_FALSE(results.is_truncated);

  if (verbose) {
    std::cout << "  prefix=dir1/ objects:" << std::endl;
    for (auto& e : results.objs) {
      std::cout << "    " << e.key.name << std::endl;
    }
  }

  EXPECT_EQ(results.objs.size(), 2);
  EXPECT_EQ(results.objs[0].key.name, "dir1/a.txt");
  EXPECT_EQ(results.objs[1].key.name, "dir1/b.txt");
  EXPECT_EQ(results.common_prefixes.size(), 0);
}


TEST_F(NSFSBucketTest, SideloadedGet)
{
  // create a file directly on disk (bypassing RGW)
  sf::path sideloaded{bp / "root" / testname / "external.txt"};
  {
    int fd = ::open(sideloaded.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GT(fd, 0);
    const char *data = "sideloaded content";
    ASSERT_EQ(::write(fd, data, strlen(data)), (ssize_t)strlen(data));
    ::close(fd);
  }
  ASSERT_TRUE(sf::is_regular_file(sideloaded));

  // GET via SAL
  auto obj = bucket->get_object(rgw_obj_key("external.txt"));
  int ret = obj->get_obj_attrs(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);

  // etag should be synthesized (stat-based, contains a dash)
  auto& attrs = obj->get_attrs();
  EXPECT_TRUE(attrs.contains(RGW_ATTR_ETAG));
  std::string etag = attrs[RGW_ATTR_ETAG].to_str();
  EXPECT_NE(etag.find('-'), std::string::npos);

  // content-type should be synthesized from .txt extension
  EXPECT_TRUE(attrs.contains(RGW_ATTR_CONTENT_TYPE));
  std::string ct = attrs[RGW_ATTR_CONTENT_TYPE].to_str();
  EXPECT_EQ(ct, "text/plain");

  // read the data
  ret = obj->load_obj_state(env->dpp, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(obj->get_size(), strlen("sideloaded content"));
}

TEST_F(NSFSBucketTest, SideloadedList)
{
  // create an RGW object
  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);
    bufferlist bl;
    encode(name, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  };

  write("rgw-created.txt");

  // sideload a file directly on disk
  sf::path sideloaded{bp / "root" / testname / "sideloaded.txt"};
  {
    int fd = ::open(sideloaded.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GT(fd, 0);
    const char *data = "external";
    ASSERT_EQ(::write(fd, data, strlen(data)), (ssize_t)strlen(data));
    ::close(fd);
  }

  // LIST should show both
  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  int ret = bucket->list(env->dpp, params, 100, results, null_yield);
  EXPECT_EQ(ret, 0);

  std::set<std::string> names;
  for (auto& e : results.objs) {
    names.insert(e.key.name);
  }
  EXPECT_TRUE(names.contains("rgw-created.txt"));
  EXPECT_TRUE(names.contains("sideloaded.txt"));

  // sideloaded entry should have a synthesized etag with a dash
  for (auto& e : results.objs) {
    if (e.key.name == "sideloaded.txt") {
      EXPECT_NE(e.meta.etag.find('-'), std::string::npos);
    }
  }
}

TEST_F(NSFSBucketTest, DirectoryObjectPut)
{
  auto obj = bucket->get_object(rgw_obj_key("photos/"));
  auto writer = driver->get_atomic_writer(
      env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
      rgw::sal::no_authenticated_user);
  int ret = writer->prepare(null_yield);
  ASSERT_EQ(ret, 0);

  bufferlist bl;
  bl.append("folder body");
  ret = writer->process(std::move(bl), 0);
  ASSERT_EQ(ret, 0);
  ret = writer->process({}, 11);
  ASSERT_EQ(ret, 0);

  ceph::real_time mtime;
  Attrs attrs;
  std::string etag;
  req_context rctx{env->dpp, null_yield, nullptr};
  ret = writer->complete(11, etag, &mtime, real_time(), attrs, std::nullopt,
                         real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
  ASSERT_EQ(ret, 0);

  // directory exists on disk, .folder sentinel inside
  sf::path dir_path{bp / "root" / testname / "photos"};
  sf::path folder_path{bp / "root" / testname / "photos" / ".folder"};
  EXPECT_TRUE(sf::is_directory(dir_path));
  EXPECT_TRUE(sf::is_regular_file(folder_path));
}

TEST_F(NSFSBucketTest, DirectoryObjectGet)
{
  // PUT
  auto obj = bucket->get_object(rgw_obj_key("docs/"));
  auto writer = driver->get_atomic_writer(
      env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
      rgw::sal::no_authenticated_user);
  int ret = writer->prepare(null_yield);
  ASSERT_EQ(ret, 0);

  std::string body = "directory object content";
  bufferlist wbl;
  wbl.append(body);
  ret = writer->process(std::move(wbl), 0);
  ASSERT_EQ(ret, 0);
  ret = writer->process({}, body.size());
  ASSERT_EQ(ret, 0);

  ceph::real_time mtime;
  Attrs attrs;
  std::string etag;
  req_context rctx{env->dpp, null_yield, nullptr};
  ret = writer->complete(body.size(), etag, &mtime, real_time(), attrs,
                         std::nullopt, real_time(), nullptr, nullptr, nullptr,
                         nullptr, nullptr, rctx, 0);
  ASSERT_EQ(ret, 0);

  // GET
  auto robj = bucket->get_object(rgw_obj_key("docs/"));
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(robj->get_read_op(rgw::sal::no_authenticated_user));
  ret = read_op->prepare(null_yield, env->dpp);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(robj->get_size(), body.size());

  bufferlist rbl;
  Read_CB cb(&rbl);
  ret = read_op->iterate(env->dpp, 0, body.size(), &cb, null_yield);
  EXPECT_EQ(ret, 0);

  std::string got(rbl.c_str(), rbl.length());
  EXPECT_EQ(got, body);
}

TEST_F(NSFSBucketTest, DirectoryObjectList)
{
  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);
    bufferlist bl;
    encode(name, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  };

  write("photos/");
  write("photos/img.jpg");

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  int ret = bucket->list(env->dpp, params, 100, results, null_yield);
  EXPECT_EQ(ret, 0);

  std::set<std::string> names;
  for (auto& e : results.objs) {
    names.insert(e.key.name);
  }

  if (verbose) {
    std::cout << "  directory object listing:" << std::endl;
    for (auto& n : names) {
      std::cout << "    " << n << std::endl;
    }
  }

  EXPECT_TRUE(names.contains("photos/"));
  EXPECT_TRUE(names.contains("photos/img.jpg"));
}

TEST_F(NSFSBucketTest, DirectoryObjectDelete)
{
  // PUT directory object
  auto obj = bucket->get_object(rgw_obj_key("todelete/"));
  auto writer = driver->get_atomic_writer(
      env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
      rgw::sal::no_authenticated_user);
  int ret = writer->prepare(null_yield);
  ASSERT_EQ(ret, 0);
  bufferlist bl;
  bl.append("x");
  ret = writer->process(std::move(bl), 0);
  ASSERT_EQ(ret, 0);
  ret = writer->process({}, 1);
  ASSERT_EQ(ret, 0);
  ceph::real_time mtime;
  Attrs attrs;
  std::string etag;
  req_context rctx{env->dpp, null_yield, nullptr};
  ret = writer->complete(1, etag, &mtime, real_time(), attrs, std::nullopt,
                         real_time(), nullptr, nullptr, nullptr, nullptr,
                         nullptr, rctx, 0);
  ASSERT_EQ(ret, 0);

  sf::path folder_path{bp / "root" / testname / "todelete" / ".folder"};
  sf::path dir_path{bp / "root" / testname / "todelete"};
  ASSERT_TRUE(sf::is_regular_file(folder_path));
  ASSERT_TRUE(sf::is_directory(dir_path));

  // DELETE
  auto dobj = bucket->get_object(rgw_obj_key("todelete/"));
  auto del_op = dobj->get_delete_op(rgw::sal::no_authenticated_user);
  ret = del_op->delete_obj(env->dpp, null_yield, 0);
  EXPECT_EQ(ret, 0);

  EXPECT_FALSE(sf::exists(folder_path));
  EXPECT_FALSE(sf::exists(dir_path));
}

TEST_F(NSFSBucketTest, HierarchicalCopy)
{
  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);
    bufferlist bl;
    encode(name, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  };

  write("src.txt");

  sf::path src_path{bp / "root" / testname / "src.txt"};
  ASSERT_TRUE(sf::is_regular_file(src_path));

  auto src_obj = bucket->get_object(rgw_obj_key("src.txt"));
  auto dst_obj = bucket->get_object(rgw_obj_key("dir1/dir2/dst.txt"));

  Attrs attrs;
  add_attr(attrs, RGW_ATTR_ACL, "");
  std::string etag, tag;
  int ret = src_obj->copy_object(
      acl_owner, rgw_user(), rgw::sal::no_authenticated_user,
      nullptr, rgw_zone_id(),
      dst_obj.get(), bucket.get(), bucket.get(),
      rgw_placement_rule(), nullptr, nullptr,
      nullptr, nullptr, false,
      nullptr, nullptr,
      ATTRSMOD_NONE, false, attrs,
      RGWObjCategory::Main, 0,
      boost::none, nullptr, &tag, &etag,
      nullptr, nullptr, nullptr,
      env->dpp, null_yield);
  EXPECT_EQ(ret, 0);

  sf::path dst_path{bp / "root" / testname / "dir1" / "dir2" / "dst.txt"};
  EXPECT_TRUE(sf::is_regular_file(dst_path));
  EXPECT_TRUE(sf::is_regular_file(src_path));
}

TEST_F(NSFSBucketTest, HierarchicalChown)
{
  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    int ret = writer->prepare(null_yield);
    ASSERT_EQ(ret, 0);
    bufferlist bl;
    encode(name, bl);
    int len = bl.length();
    ret = writer->process(std::move(bl), 0);
    ASSERT_EQ(ret, 0);
    ret = writer->process({}, len);
    ASSERT_EQ(ret, 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ret = writer->complete(len, etag, &mtime, real_time(), attrs, std::nullopt,
                           real_time(), nullptr, nullptr, nullptr, nullptr,
                           nullptr, rctx, 0);
    ASSERT_EQ(ret, 0);
  };

  write("dir1/file.txt");

  sf::path file_path{bp / "root" / testname / "dir1" / "file.txt"};
  ASSERT_TRUE(sf::is_regular_file(file_path));

  auto obj = bucket->get_object(rgw_obj_key("dir1/file.txt"));
  TestUser user(driver.get());
  int ret = obj->chown(env->dpp, user.get_id(), user.get_display_name(), null_yield);
  // chown to uid/gid 0 requires root; EPERM is expected for non-root
  EXPECT_TRUE(ret == 0 || ret == -EPERM);
  nsfs::StridedMPUStrategy strided;

  EXPECT_EQ(nb.shared_name(1048576), strided.shared_name(1048576));

  EXPECT_NE(nb.meta_name(), strided.meta_name());
  EXPECT_NE(nb.assembled_name(), strided.assembled_name());
  EXPECT_NE(nb.part_name(1), strided.part_name(1));
}

  static constexpr const char* attr = "user.nsfs:test";

TEST(NooBaaXattr, NamesNooBaaAttributes)
{
  nsfs::PrefixedXattrStrategy prefixed;
  ASSERT_EQ(prefixed.object_owner(with, owner), 0);

  EXPECT_EQ(nb.disk_name("bucket_info"), "user.nsfs:bucket_info");
  EXPECT_EQ(nb.disk_name("rename_intent"), "user.nsfs:rename_intent");


/* Neither format claims a marker (DECISIONS.md 2026-11-02), so none
 * reaches an attribute set and no REPLACE_ALL prunes one.  The control is
 * a neighbour under user.nsfs: that each still claims. */
TEST(MarkerXattr, NeitherFormatClaimsAMarker)
{
  nsfs::PrefixedXattrStrategy prefixed;
  nsfs::NooBaaXattrStrategy nb;
  for (const nsfs::XattrStrategy* xs :
	 std::initializer_list<const nsfs::XattrStrategy*>{&prefixed, &nb}) {
    std::string key;
    for (const char* m : {nsfs::EXTENSIONS_XATTR, nsfs::CONVERTING_XATTR,
			  nsfs::OWNS_STORAGE_XATTR}) {
      EXPECT_TRUE(std::string_view(m).starts_with(nsfs::MARKER_XATTR_PREFIX))
	  << m;
      EXPECT_FALSE(xs->parse_disk_name(m, key)) << xs->name() << ":  " << m;
    }
    EXPECT_TRUE(xs->parse_disk_name("user.nsfs:bucket_info", key))
	<< xs->name();
    EXPECT_EQ(key, "bucket_info");
  }
}

TEST(NooBaaXattr, LeavesNooBaaStructuredAttributesAlone)
{
  nsfs::PrefixedXattrStrategy prefixed;

  nsfs::StridedMPUStrategy strided;
  EXPECT_FALSE(strided.is_staging_dir(tree.upload_id));
  EXPECT_FALSE(sf::exists(tree.staging / strided.meta_name()));
  EXPECT_FALSE(sf::exists(tree.staging / strided.part_name(1)));

  EXPECT_FALSE(strided.staged_upload(env->dpp, ofd, tree.upload_id, none));
TEST(NooBaaPath, KeyConventionsMatchSentinel)
{
  nsfs::SentinelPathStrategy sentinel;

    EXPECT_EQ(nb.object_name(key, false), sentinel.object_name(key, false))
      << k;
  }

  EXPECT_EQ(nb.folder_object_name(), sentinel.folder_object_name());
  EXPECT_TRUE(sentinel.names_directory_object(".folder"));
  nsfs::SentinelPathStrategy sentinel;
  EXPECT_EQ(sentinel.bucket_dir_name("b", mp), ".multipart_b");
TEST(NooBaaPath, ReservesNooBaaNamesAndNotSentinels)
{
  nsfs::SentinelPathStrategy sentinel;
  const auto& orn = sentinel.reserved_names();
  nsfs::SentinelPathStrategy sentinel;
  EXPECT_FALSE(sentinel.directory_object(env->dpp, fd, od));

/* The strided staging layout, on a filesystem that shares extents.
 *
 * The driver chooses strided only where assembling from per-part files
 * would copy the whole object, which is GPFS and nothing a developer
 * machine mounts.  So the layout, the extent guard and the divert path
 * have had no test outside a Storage Scale run.  These force the
 * result instead of measuring it.
 *
 * What they do not test is the premise:  XFS still shares extents
 * underneath, so this exercises placement, the guard and assembly, not
 * the cost that motivates them.
 */
class NSFSStridedBucketTest : public NSFSBucketTest {
public:
  std::optional<bool> shares_extents() const override { return false; }
};

/* Both polarities.  Forced false must select strided and forced true
 * must not -- an override stuck at one value would otherwise pass. */
TEST_F(NSFSStridedBucketTest, StridedLayoutIsSelected)
{
  EXPECT_STREQ(driver->get_mpu_strategy()->name(), "rgw-strided");

  TestDriver other{bp};
  ASSERT_EQ(other.init(env->dpp, true), 0);
  EXPECT_STREQ(other.get_mpu_strategy()->name(), "rgw");
}

namespace {

/* one part through the SAL;  returns the etag the upload will be
 * completed with */
std::string write_mp_part(rgw::sal::MultipartUpload* upload,
			  const ACLOwner& acl_owner,
			  rgw_placement_rule* placement,
			  int num, const std::string& payload)
{
  std::unique_ptr<rgw::sal::Writer> writer =
    upload->get_writer(env->dpp, null_yield, nullptr, acl_owner,
		       placement, num, std::to_string(num),
		       rgw::sal::no_authenticated_user);
  EXPECT_NE(writer.get(), nullptr);
  EXPECT_EQ(writer->prepare(null_yield), 0);

  bufferlist bl;
  bl.append(payload);
  const int len = bl.length();
  EXPECT_EQ(writer->process(std::move(bl), 0), 0);
  EXPECT_EQ(writer->process({}, len), 0);

  ceph::real_time mtime;
  Attrs part_attrs;
  req_context rctx{env->dpp, null_yield, nullptr};
  std::string etag = std::to_string(num);
  EXPECT_EQ(writer->complete(len, etag, &mtime, real_time(), part_attrs,
			     std::nullopt, real_time(), nullptr, nullptr,
			     nullptr, nullptr, nullptr, rctx, 0), 0);
  return etag;
}

int complete_mp(rgw::sal::Bucket* bucket, rgw::sal::MultipartUpload* upload,
		const rgw_owner& owner, const std::string& objname,
		std::map<int, std::string>& part_etags)
{
  std::list<rgw_obj_index_key> remove_objs;
  bool compressed = false;
  RGWCompressionInfo cs_info;
  off_t ofs{0};
  uint64_t accounted_size{0};
  std::string tag;
  rgw::sal::MultipartUpload::prefix_map_t processed_prefixes;
  ACLOwner mp_owner;
  mp_owner.id = owner;
  std::unique_ptr<rgw::sal::Object> mp_obj =
    bucket->get_object(rgw_obj_key(objname));
  return upload->complete(env->dpp, null_yield, get_pointer(env->cct),
			  part_etags, remove_objs, accounted_size, compressed,
			  cs_info, ofs, tag, mp_owner, 0, mp_obj.get(),
			  processed_prefixes);
}

} /* anonymous namespace */

/* Uniform parts land in one file at (K-1) * stride, and completing the
 * upload links that file rather than copying it. */
TEST_F(NSFSStridedBucketTest, StridedMultipartCompletes)
{
  const std::string objname = testname + "-mp";
  const std::string upload_id = "c0ffee";
  auto upload = bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  const size_t stride = 64;
  std::string expected;
  std::map<int, std::string> part_etags;
  for (int i = 1; i <= 3; ++i) {
    std::string payload(stride, static_cast<char>('a' + i));
    part_etags[i] = write_mp_part(upload.get(), acl_owner, &placement, i,
				  payload);
    expected += payload;
  }

  /* the shared file is named for the stride, and holds the parts in
   * order, before anything is completed */
  sf::path staging{bp / "root" / testname /
		   (".multipart_" + objname + "." + upload_id)};
  sf::path shared{staging / ("parts-size-" + std::to_string(stride))};
  ASSERT_TRUE(sf::exists(shared)) << "no shared file at " << shared;
  EXPECT_EQ(sf::file_size(shared), stride * 3);

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);

  sf::path obj_path{bp / "root" / testname / objname};
  ASSERT_TRUE(sf::is_regular_file(obj_path));
  EXPECT_EQ(sf::file_size(obj_path), expected.size());

  EXPECT_EQ(read_all(obj_path), expected);
}

/* A part longer than the stride must not write into the next part's
 * region.  Before the guard, the object came back the right length
 * with another part's bytes inside it -- found on GPFS, and until now
 * reachable nowhere else. */
TEST_F(NSFSStridedBucketTest, StridedOversizedPartDiverts)
{
  const std::string objname = testname + "-mp";
  const std::string upload_id = "c0ffee";
  auto upload = bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  const size_t stride = 64;
  std::string expected;
  std::map<int, std::string> part_etags;

  /* part 1 establishes the stride */
  std::string p1(stride, 'a');
  part_etags[1] = write_mp_part(upload.get(), acl_owner, &placement, 1, p1);
  expected += p1;

  /* part 2 exceeds it, so it cannot stay where the stride would put it */
  std::string p2(stride + 17, 'b');
  part_etags[2] = write_mp_part(upload.get(), acl_owner, &placement, 2, p2);
  expected += p2;

  /* part 3 would have been overwritten by part 2's overrun */
  std::string p3(stride, 'c');
  part_etags[3] = write_mp_part(upload.get(), acl_owner, &placement, 3, p3);
  expected += p3;

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);

  sf::path obj_path{bp / "root" / testname / objname};
  ASSERT_TRUE(sf::is_regular_file(obj_path));

  const std::string got = read_all(obj_path);
  EXPECT_EQ(got.size(), expected.size());
  /* the length was right even when the bytes were not, so compare the
   * bytes and say which part was corrupted if they differ */
  EXPECT_EQ(got, expected)
      << "part 2's overrun reached part 3's region";
}


/* The strided layout under the writeback cache policy.
 *
 * A part's record reaches its xattr only when the cache entry is
 * evicted, so part 1's stored length -- which is what establishes the
 * stride -- does not exist on disk while the upload is in flight.  A
 * reader consulting disk alone finds no stride and stages every part
 * after the first in its own file:  the objects are correct and the
 * layout is silently never used, on the one filesystem it exists for.
 *
 * established_stride() asks the cache first for that reason, and this
 * is what holds it to it. */
class NSFSStridedWritebackBucketTest : public NSFSStridedBucketTest {
public:
  file::listing::MultipartCachePolicy mp_cache_policy() const override {
    return file::listing::MultipartCachePolicy::writeback;
  }
};

TEST_F(NSFSStridedWritebackBucketTest, StrideSurvivesWriteback)
{
  const std::string objname = testname + "-mp";
  const std::string upload_id = "c0ffee";
  auto upload = bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  const size_t stride = 64;
  std::string expected;
  std::map<int, std::string> part_etags;
  for (int i = 1; i <= 3; ++i) {
    std::string payload(stride, static_cast<char>('a' + i));
    part_etags[i] = write_mp_part(upload.get(), acl_owner, &placement, i,
				  payload);
    expected += payload;
  }

  sf::path staging{bp / "root" / testname /
		   (".multipart_" + objname + "." + upload_id)};
  sf::path shared{staging / ("parts-size-" + std::to_string(stride))};
  ASSERT_TRUE(sf::exists(shared)) << "no shared file at " << shared;
  EXPECT_EQ(sf::file_size(shared), stride * 3)
      << "parts after the first did not reach the shared file, so the "
	 "stride was not found under writeback";

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);

  EXPECT_EQ(read_all(bp / "root" / testname / objname), expected);
}


/* The writeback cache, small enough to evict.
 *
 * Eviction is the only thing that writes a part's record under this
 * policy, so it is the only thing that can get it wrong -- and with
 * the configured sizes nothing is ever evicted, which is why the
 * existing writeback test never reached it.  One entry, so a second
 * upload displaces the first.
 */
class NSFSStridedEvictingBucketTest : public NSFSStridedBucketTest {
public:
  file::listing::MultipartCachePolicy mp_cache_policy() const override {
    return file::listing::MultipartCachePolicy::writeback;
  }
  std::array<uint64_t, 4> mp_cache_sizes() const override {
    return {1, 1, 1, 8};
  }
};

/* An evicted part's record says where its bytes are.
 *
 * Under the strided layout a part lives in the shared file at an
 * offset, and once part 1 is linked to that file the record is the
 * only source for the placement -- a stat reports the whole upload.
 * The eviction path copied num, size, etag, mtime and cksum and
 * stopped, so `shared`, `offset` and `stored` defaulted to "its own
 * file, offset zero, nothing stored".  Assembly then read a shared
 * part out of the empty record file:  the object came back the right
 * length, because `size` was copied, with another part's absence
 * inside it.
 */
TEST_F(NSFSStridedEvictingBucketTest, AnEvictedPartRecordsItsPlacement)
{
  const std::string objname = "evicted.bin";
  const std::string upload_id = "c0ffee";
  auto upload = bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  const size_t stride = 64;
  std::string expected;
  std::map<int, std::string> part_etags;
  for (int i = 1; i <= 3; ++i) {
    std::string payload(stride, static_cast<char>('a' + i));
    part_etags[i] = write_mp_part(upload.get(), acl_owner, &placement, i,
				  payload);
    expected += payload;
  }

  /* a second upload, which displaces the first and stabilises it.
   *
   * One is enough:  the entry reaches the evictable queue when its
   * last reference goes, and the release that puts the second one
   * there is what finds the queue over its high-water mark. */
  {
    auto other = bucket->get_multipart_upload("other.bin", rgw::sal::no_authenticated_user, "beef42");
    ASSERT_EQ(other->init(env->dpp, null_yield, acl_owner, placement,
			  attrs), 0);
    write_mp_part(other.get(), acl_owner, &placement, 1,
		  std::string(stride, 'z'));
  }

  /* the records are on disk now, and they have to say the parts are
   * in the shared file rather than in their own */
  const sf::path staging{bp / "root" / testname /
			 (".multipart_" + objname + "." + upload_id)};
  ASSERT_TRUE(sf::is_directory(staging)) << staging;
  for (int i = 2; i <= 3; ++i) {
    const sf::path rec{staging / fmt::format("part-{:0>5}", i)};
    ASSERT_TRUE(sf::exists(rec)) << rec;
    EXPECT_EQ(sf::file_size(rec), 0u)
	<< "a shared part's record file holds no bytes, which is why its "
	   "record has to say so";
  }

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);
  const sf::path obj{bp / "root" / testname / objname};
  ASSERT_TRUE(sf::is_regular_file(obj)) << obj;
  EXPECT_EQ(sf::file_size(obj), expected.size());
  EXPECT_EQ(read_all(obj), expected)
      << "the object was assembled from the record files rather than from "
	 "the shared file";
}


  auto marked = marked_bucket(testname + "-ours");
  EXPECT_EQ(copy(bucket.get(), "obj", marked.get(), "copied"), 0);
  EXPECT_EQ(put(marked.get(), "new"), 0) << "a marked bucket rejected a write";
}

  void remove_rgw_bucket_info() {
  }


  int read_nb_record(RGWBucketInfo& info, Attrs& attrs) {

    auto* nb_state = driver->get_noobaa_bucket_state_strategy();
    if (! nb_state) {
    }
    return nb_state->load(env->dpp, -1, testname, attrs, info);
  }
};

TEST_F(NSFSNooBaaStateTest, ForceMd5EtagIsCarriedInNooBaaShape)
{

  write_nb_record("DISABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);

  write_nb_record("DISABLED", ",\"force_md5_etag\":true");
  ASSERT_EQ(read_nb_record(info, attrs), 0);

  write_nb_record("DISABLED", ",\"force_md5_etag\":false");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
TEST_F(NSFSNooBaaStateTest, VersioningComesFromNooBaaRecord)
{

  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);

  write_nb_record("SUSPENDED");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
TEST_F(NSFSNooBaaStateTest, NsfsStateWinsOverNooBaas)
{
  write_nb_record("ENABLED");

  nsfs::PrefixedXattrStrategy prefixed;
  const std::string disk = prefixed.disk_name(RGW_ATTR_ACL);
TEST_F(NSFSNooBaaBucketTest, ALoadedBaseBucketListsInNooBaaFormat)
{
TEST_F(NSFSNooBaaStateTest, ObjectLockComesFromNooBaaRecord)
{
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);

  ASSERT_EQ(read_nb_record(info, attrs), 0);



  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}



  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);
TEST_F(NSFSNooBaaStateTest, NooBaaDisabledLockIsNoLock)
{


  ASSERT_EQ(read_nb_record(info, attrs), 0);


TEST_F(NSFSNooBaaStateTest, BucketPolicyComesFromNooBaaRecord)
{
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);


  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

TEST_F(NSFSNooBaaStateTest, PublicAccessBlockComesFromNooBaaRecord)
{
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);


  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

TEST_F(NSFSNooBaaStateTest, WebsiteComesFromNooBaaRecord)
{
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);


  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

TEST_F(NSFSNooBaaStateTest, EncryptionComesFromNooBaaRecord)
{
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);

  write_nb_record("ENABLED", R"(,"encryption":{})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
TEST_F(NSFSNooBaaStateTest, LoggingIsReadFromNooBaaRecord)
{


  ASSERT_EQ(read_nb_record(info, attrs), 0);

  write_nb_record("DISABLED", R"(,"logging":{"log_prefix":"p/"})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);

  write_nb_record("ENABLED", R"(,"encryption":{"algorithm":"rot13"})");

TEST_F(NSFSNooBaaStateTest, BucketTagsComeFromNooBaaRecord)
{
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);
  write_nb_record("ENABLED", arr);


  write_nb_record("ENABLED", R"(,"tag":[{"key":"","value":"orphan"}])");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

TEST_F(NSFSNooBaaStateTest, LifecycleComesFromNooBaaRecord)
{
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);




  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}



  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

TEST_F(NSFSNooBaaStateTest, CorsComesFromNooBaaRecord)
{
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);


  ASSERT_EQ(read_nb_record(info, attrs), 0);


  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}



  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);

  write_nb_record("PERHAPS");
  remove_rgw_bucket_info();
  write_nb_record("ENABLED");
TEST_F(NSFSNooBaaBucketTest, ListsThePartsOfNooBaaUpload)
{
TEST_F(NSFSNooBaaBucketTest, GetInfoCarriesNooBaaCreateParams)
{
TEST_F(NSFSBucketTest, GetInfoOnAnNsfsUpload)
{
TEST_F(NSFSBucketTest, ObjectsInAMarkedBucketKeepNsfsNames)
{
  }
  EXPECT_TRUE(names.contains("user.nsfs:rgw.content_type"));
    ASSERT_STREQ(b->profile_name(), "shared");
  void write_nb_object(const std::string& name) {
TEST_F(NSFSConvertingBucketTest, ReadsNooBaaAttributes)
{
  write_nb_object("theirs.bin");

TEST_F(NSFSBucketTest, DoesNotReadNooBaaAttributesWithoutTheMarker)
{
  const std::string ctype{"text/plain"};
TEST_F(NSFSConvertingBucketTest, ListsNooBaaEmptyDirectoryObject)
{
TEST_F(NSFSConvertingBucketTest, ReadsNooBaaDirectoryObjects)
{
  write_nb_object("w");

TEST_F(NSFSBucketTest, DoesNotListNooBaaDirectoryObjectWithoutTheMarker)
{
  write_nb_object("theirs.bin");
  EXPECT_TRUE(after.contains("user.nsfs:rgw.content_type"));
TEST_F(NSFSNooBaaBucketTest, ReadsNooBaaObjectLock)
{
  EXPECT_TRUE(names.contains("user.nsfs:rgw.object-legal-hold"));
}

TEST_F(NSFSNooBaaBucketTest, ReadsNooBaaStorageClass)
{
TEST_F(NSFSTagTest, ReadsNooBaaTags)
{
  nb_tagged_object("tagged.bin", {{"colour", "green"}, {"size", "large"}});

  bool encoded = false;
      encoded = true;
    }
  }
  EXPECT_TRUE(encoded) << "our encoded tag set was not written";
}

  void nb_object(const std::string& name, const char* ctype) {
  void nb_folder(const std::string& name, const char* ctype) {
  void nb_delete_marker(const std::string& leaf, const std::string& vid) {

TEST_F(NSFSUpgradeTest, ABaseBucketWritesItsStateAsRGWAttributes)
{
  static constexpr const char* base_name = "user.nsfs:user.rgw.iam-policy";
  static constexpr const char* nsfs_name = "user.nsfs:rgw.iam-policy";
  EXPECT_TRUE(before.contains(nsfs_name))
      << "a base bucket did not write its state as RGW attributes";
  EXPECT_TRUE(after.contains(nsfs_name));
  EXPECT_EQ(str_xattr(bucket_path(), nsfs_name), policy);

  nb_object("photo.jpg", "image/jpeg");
  nb_version("photo.jpg", v1, "image/jpeg");
  nb_version("my_photo.jpg", v2, "image/png");

  nb_object("photo.jpg", "image/jpeg");
  nb_delete_marker("gone.bin", vid);

  nb_version("doc.txt", vid, "text/plain");
  nb_delete_marker("doc.txt", dmid);

    ssize_t len = ::getxattr(p.c_str(), "user.nsfs:rgw.acl", buf, sizeof(buf));
  nb_version("only.bin", vid, "text/plain");

/* No marker is bucket metadata, so none reaches the S3 attribute set
 * -- including while a conversion is in flight, which is the only time
 * the converting marker exists -- and a write of the bucket's
 * attributes, which replaces them all, leaves every marker in place. */
  nb_object("flat.bin", "text/plain");
  ASSERT_EQ(static_cast<rgw::sal::NSFSBucket*>(bucket.get())
	      ->set_owns_storage(env->dpp, false), 0);

  auto no_marker = [](const rgw::sal::Attrs& attrs) {
    for (const auto& [k, _] : attrs) {
      EXPECT_FALSE(k.starts_with("marker.")) << k;
      EXPECT_NE(k, "converting");
      EXPECT_NE(k, "extensions");
      EXPECT_NE(k, "owns_storage");
    }
  };
  no_marker(bucket->get_attrs());

  /* a write of the bucket's state, as adoption's own */
  rgw::sal::Attrs more;
  more["user.rgw.x-amz-meta-probe"].append("x");
  ASSERT_EQ(b->merge_and_store_attrs(env->dpp, more, null_yield,
				     rgw::sal::MutationPurpose::adoption), 0);
  for (const char* m : {nsfs::EXTENSIONS_XATTR, nsfs::CONVERTING_XATTR,
			nsfs::OWNS_STORAGE_XATTR}) {
    EXPECT_GT(::getxattr(bucket_path().c_str(), m, nullptr, 0), 0)
	<< m << " was pruned by a write of the bucket's attributes";
  }
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  no_marker(bucket->get_attrs());
}


/* NooBaa user metadata whose key begins "nsfs." is user metadata, served
 * as found and after conversion.  NooBaa stores it as `user.` + the key,
 * so it lands under `user.nsfs.`;  ours is under `user.nsfs:`, which no
 * header name can reach (DECISIONS.md 2026-11-05).  Before the rename
 * the first of these read as our content type and the second as a
 * marker. */
TEST_F(NSFSUpgradeTest, NooBaaMetadataUnderNsfsStaysMetadata)
{
  nb_object("aliased.bin", "text/plain");
  const sf::path p{bucket_path() / "aliased.bin"};
  const std::map<std::string, std::string> meta{
    {"nsfs.rgw.content_type", "not/ours"},
    {"nsfs.marker.converting", "noobaa"},
  };
  for (const auto& [k, v] : meta) {
    const std::string n = "user." + k;
    ASSERT_EQ(::setxattr(p.c_str(), n.c_str(), v.data(), v.size(), 0), 0)
	<< n;
  }

  auto check = [&](const char* when) {
    auto obj = bucket->get_object(rgw_obj_key("aliased.bin"));
    ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0) << when;
    ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0) << when;
    auto& attrs = obj->get_attrs();
    ASSERT_TRUE(attrs.contains(RGW_ATTR_CONTENT_TYPE)) << when;
    EXPECT_EQ(rgw_bl_str(attrs[RGW_ATTR_CONTENT_TYPE]), "text/plain")
	<< when;
    for (const auto& [k, v] : meta) {
      const std::string key = std::string(RGW_ATTR_META_PREFIX) + k;
      ASSERT_TRUE(attrs.contains(key)) << when << ":  " << key;
      EXPECT_EQ(rgw_bl_str(attrs[key]), v) << when << ":  " << key;
    }
  };
  check("served as found");

  ASSERT_EQ(upgrade(nullptr, nullptr), 0);
  check("converted");

  /* carried as RGW user metadata, and nothing under the old prefix */
  const auto names = xattr_names(p);
  EXPECT_TRUE(names.contains("user.nsfs:rgw.x-amz-meta-nsfs.rgw.content_type"));
  EXPECT_TRUE(names.contains("user.nsfs:rgw.x-amz-meta-nsfs.marker.converting"));
  for (const auto& n : names) {
    EXPECT_FALSE(n.starts_with("user.nsfs.")) << n;
  }
}

/* A NooBaa bucket's root directory object keeps its metadata on the
 * bucket's directory, so `x-amz-meta-nsfs.marker.extensions` lands
 * beside where our marker would go.  It is not our marker:  the bucket
 * is still base, and the metadata is the root object's. */
TEST_F(NSFSUpgradeTest, RootMetadataCannotForgeAMarker)
{
  set_u64_xattr(bucket_path(), "user.noobaa.dir_content", 0);
  const std::string v{"3"};
  ASSERT_EQ(::setxattr(bucket_path().c_str(),
		       "user.nsfs.marker.extensions", v.data(), v.size(), 0),
	    0);

  std::unique_ptr<rgw::sal::Bucket> fresh;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", testname},
				&fresh, null_yield), 0);
  auto* b = static_cast<rgw::sal::NSFSBucket*>(fresh.get());
  EXPECT_STREQ(b->profile_name(), "base");

  ASSERT_EQ(upgrade(nullptr, nullptr), 0);
  auto obj = bucket->get_object(rgw_obj_key("/"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);
  auto& attrs = obj->get_attrs();
  const std::string key =
    std::string(RGW_ATTR_META_PREFIX) + "nsfs.marker.extensions";
  ASSERT_TRUE(attrs.contains(key));
  EXPECT_EQ(rgw_bl_str(attrs[key]), v);
}

  nb_object("flat.bin", "text/plain");
  nb_object("deep/nested.bin", "application/json");
  nb_folder("photos", "application/directory");

    EXPECT_TRUE(names.contains("user.nsfs:rgw.content_type")) << k;
    EXPECT_TRUE(names.contains("user.nsfs:rgw.etag")) << k;

  nb_object("a.bin", "text/plain");
  nb_object("b.bin", "text/plain");

  nb_object("held.bin", "text/plain");

      }
      /* both attributes are on the file right now */
      EXPECT_TRUE(names.contains("user.nsfs:rgw.content_type"));
    nb_object("obj-" + std::to_string(i) + ".bin", "text/plain");
  }

  int nsfs_count = 0, nb_count = 0;
      ++nb_count;
      ++nsfs_count;
    }
  }
  EXPECT_EQ(nsfs_count, 2);
  EXPECT_EQ(nb_count, 2);

  nb_object("once.bin", "text/plain");
  nb_object("beside.bin", "text/plain");

  const sf::path staging{bucket_path() / b->mpu_strategy()->staging_dir_name(meta)};
  ASSERT_TRUE(sf::is_directory(staging)) << staging;


/* An upload whose parts were listed before the upgrade still lists and
 * completes after it.
 *
 * The test above starts from a cold multipart cache.  A client does
 * not:  it lists the parts, the upgrade re-stages the upload into the
 * nsfs layout, and the client then lists again or completes with the
 * part ETags it already has.  If the cache kept what it read from
 * NooBaa's staging, which conversion has removed, that is where it would
 * show.  A bucket being converted must look to a client like the bucket
 * it becomes (DECISIONS.md 2026-10-13), so the ETags must not change
 * either. */
TEST_F(NSFSUpgradeTest, AnUploadListedBeforeTheUpgradeCompletesAfterIt)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  /* the client's view before:  warms the multipart cache */
  std::map<int, std::string> before;
  {
    auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
    int next = 0;
    bool truncated = false;
    ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
				 &truncated, null_yield), 0);
    for (auto& [num, part] : upload->get_parts()) {
      before[num] = part->get_etag();
    }
    ASSERT_EQ(before.size(), tree.parts);
  }

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  ASSERT_EQ(prog.uploads, 1u);
  ASSERT_FALSE(sf::exists(tree.staging)) << "the upload was not re-staged";

  /* the same listing after:  the same parts, the same ETags */
  auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
  int next = 0;
  bool truncated = false;
  ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
			       &truncated, null_yield), 0)
      << "the parts no longer list after the upgrade";
  std::map<int, std::string> after;
  for (auto& [num, part] : upload->get_parts()) {
    after[num] = part->get_etag();
    EXPECT_EQ(part->get_size(), tree.part_size) << num;
  }
  EXPECT_EQ(after, before) << "the upgrade changed the parts' ETags";

  /* and it completes with the ETags the client had before */
  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, tree.key,
			before), 0);
  EXPECT_EQ(read_all(bucket_path() / tree.key), tree.payload);
}


  nb_object("plain.bin", "text/plain");
  nb_object("v.bin", "text/plain");

/* Re-staging an upload drops its cached part records when a part's
 * placement changed, and only then (DECISIONS.md 2026-10-13).
 *
 * The multipart cache is keyed by the upload, not by where it is
 * staged, so a record cached before the upgrade survives it.  NooBaa
 * puts parts of one size in a shared file at an offset;  re-staging
 * gives each part its own file at offset 0.  A cached "shared, at offset
 * N" would then describe bytes that are no longer there. */
TEST_F(NSFSUpgradeTest, ReStagingDropsCachedPartsWhosePlacementChanged)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);
  ASSERT_GE(tree.parts, 2u);
  auto* mc = driver->get_multipart_cache();
  ASSERT_NE(mc, nullptr);
  const file::listing::MultipartCacheKey ck{bucket->get_name(),
					    tree.key + "." + tree.upload_id};

  {
    auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
    int next = 0;
    bool truncated = false;
    ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
				 &truncated, null_yield), 0);
  }

  /* the control:  the cache holds NooBaa's placement, which re-staging
   * will change, or nothing below could fail */
  file::listing::MultipartPartInfo pi;
  ASSERT_TRUE(mc->peek_part(ck, 2, pi)) << "listing cached no part record";
  ASSERT_TRUE(pi.shared) << "the fixture's part 2 is not in a shared file";
  ASSERT_EQ(pi.offset, tree.part_size);

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  ASSERT_EQ(prog.uploads, 1u);

  EXPECT_FALSE(mc->peek_part(ck, 2, pi))
      << "a record of where NooBaa kept part 2 survived its re-staging";

  /* and listing again records where the part is now */
  auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
  int next = 0;
  bool truncated = false;
  ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
			       &truncated, null_yield), 0);
  ASSERT_TRUE(mc->peek_part(ck, 2, pi));
  EXPECT_FALSE(pi.shared);
  EXPECT_EQ(pi.offset, 0u);
}

TEST_F(NSFSUpgradeTest, StoppingMidUploadKeepsNooBaaStaging)
{
  nb_object("gated.bin", "text/plain");

  nb_object("a.bin", "text/plain");
  nb_object("b.bin", "text/plain");

  nb_object("a.bin", "text/plain");

TEST_F(NSFSStrongBucketTest, AWriteHandleHoldsTheGateOpen)
{
TEST_F(NSFSStrongBucketTest, AClosedGateRejectsAWriteHandle)
{
TEST_F(NSFSStrongBucketTest, AnFSIOPublishedObjectCarriesTheObjectMode)
{
TEST_F(NSFSStrongBucketTest, AnFSIOClonedObjectCarriesTheObjectMode)
{
    nb_object("obj-" + std::to_string(i) + ".bin", "text/plain");
  }

  nb_object("a.bin", "text/plain");
  nb_object("b.bin", "text/plain");

TEST_F(NooBaaStoreTest, AStoreNooBaaToolsWroteReadsBack)
{
TEST_F(NooBaaKeysTest, NooBaaKeyOpensNooBaaSecret)
{
TEST_F(NooBaaPlanTest, AStoreNooBaaToolsWroteMapsCleanly)
{
TEST_F(NooBaaImportTest, ARenameInNooBaaIsStillTheSameIdentity)
{
  write_nb_record("DISABLED");

  write_nb_record("DISABLED");
  write_nb_record("DISABLED");
  write_nb_record("DISABLED");
  write_nb_record("DISABLED");
  write_nb_record("DISABLED");
  const sf::path nb_dir = bp / "noobaa-data" / "theirs";
  sf::create_directories(nb_dir / "inside");
  write_nb_record("DISABLED", "", "2026-01-02T03:04:05.000Z",
		     nb_dir.string() + "/");

  EXPECT_EQ(place("another-name", nb_dir), -EEXIST);
  EXPECT_EQ(place("another-name", nb_dir), 0);
}

  const sf::path nb_dir = bp / "noobaa-data" / "theirs";
  sf::create_directories(nb_dir);
  write_nb_record("DISABLED", "", "2026-01-02T03:04:05.000Z",
		     nb_dir.string() + "/");
  id.new_buckets_path = nb_dir.string();
  EXPECT_FALSE(sf::exists(nb_dir / "inner")) << "the directory was made";
  EXPECT_TRUE(sf::is_directory(nb_dir / "inner"));
}

  void add_nb_identity(const std::string& id, const std::string& record) {

      rec.replace(at, from.size(), NB_OWNER_ID);
    }
    add_nb_identity(NB_OWNER_ID, rec);
  }

  write_nb_record("ENABLED");

  write_nb_record("ENABLED");

  add_nb_identity("bad", "{ not json");
  write_nb_record("ENABLED");

  write_nb_record("ENABLED");
  write_nb_record("ENABLED");
  remove_rgw_bucket_info();

  {

    write_nb_record("ENABLED");
  }

  /* the `_id` write_nb_record() writes */
  static constexpr const char* NB_BUCKET_ID = "6560e1f1c0ffee0000000001";

  EXPECT_EQ(abandon(NB_BUCKET_ID, ""), -EINVAL);
  ASSERT_EQ(abandon(NB_BUCKET_ID, "left in NooBaa's format", &r), 0);
  ASSERT_EQ(abandon(NB_BUCKET_ID, "not this one"), 0);


  ASSERT_EQ(driver->unabandon_noobaa_record(env->dpp, NB_BUCKET_ID), 0);
  ASSERT_EQ(abandon(NB_BUCKET_ID, "the rest will not convert", &r), 0);
}

  write_nb_record("ENABLED");
  write_nb_record("ENABLED");

TEST_F(NooBaaModeTest, ConversionKeepsTheVersioningNooBaaRecordSupplied)
{
  remove_rgw_bucket_info();
  write_nb_record("ENABLED");


  auto own = make_bucket("mine");

  auto others = driver->get_bucket(info);
  ASSERT_EQ(others->create(env->dpp, cp, null_yield, rgw::sal::no_authenticated_user), 0);
      if (std::string_view(q).starts_with("user.nsfs:")) {
  static constexpr uid_t other_uid = 70170;
  row.uid = other_uid;
  row.gid = other_uid;
      return (::chown(at.c_str(), other_uid, other_uid) < 0) ? -errno : 0;
    });
  sf::path nb_storage() const { return bp / "their-filesystem"; }

TEST_F(NSFSRecordedPathTest, ABaseBucketIsServedAtNooBaaRecordedPath)
{
  const sf::path at = nb_storage() / "scattered";
TEST_F(NSFSRecordedPathTest, ResolvingNooBaaPathDoesNotAdoptTheBucket)
{
  const sf::path at = nb_storage() / "untouched";
TEST_F(NSFSRecordedPathTest, PlacementRowWinsOverNooBaaRecord)
{
  const sf::path recorded = nb_storage() / "theirs";
  const sf::path placed = nb_storage() / "ours";
  sf::create_directories(recorded);
  sf::create_directories(placed);
  record_at(recorded);
  row.path = placed.string();
  const sf::path recorded = nb_storage() / "elsewhere";
  sf::create_directories(recorded);
  record_at(recorded);

TEST_F(NSFSBucketTest, WithoutANooBaaStoreResolutionIsUnchanged)
{
}

int main(int argc, char *argv[]) {
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_flag(args, arg_iter, "--create", (char*) nullptr)) {
      do_create = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--delete", (char*) nullptr)) {
      do_delete = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--verbose", (char*) nullptr)) {
      verbose = true;
    } else {
      ++arg_iter;
    }
  }

  std::cout << "flags: do_create=" << do_create
            << " do_delete=" << do_delete
            << " verbose=" << verbose
            << " cwd=" << sf::current_path()
            << " base_path=" << sf::absolute(base_path)
            << std::endl;

  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
