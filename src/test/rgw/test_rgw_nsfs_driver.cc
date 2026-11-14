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
#include <algorithm>
#include <array>
#include <set>
#include <thread>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <sys/xattr.h>
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "rgw_mime.h"
#include "rgw_tag.h"
#include "rgw_lc.h"
#include "rgw_cors.h"
#include "rgw_object_lock.h"
#include "rgw_iam_policy.h"
#include "rgw_public_access.h"
#include "rgw_website.h"
#include "rgw_bucket_encryption.h"
#include "driver/nsfs/identity_db.h"
#include "driver/nsfs/impersonate.h"
#include "driver/nsfs/name_resolve.h"
#include <condition_variable>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <linux/capability.h>
#include <sys/syscall.h>
#include "driver/nsfs/personality.h"
#include "rgw_auth.h"
#include "common/ceph_json.h"

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

  /* A parameterised test's name carries the instantiation as
   * `Suite/Fixture.Test/param`, and the result is used as a directory
   * name.  Flatten it rather than create the intermediate levels:  the
   * bucket is named from this too, and a bucket name with a slash in
   * it is a hierarchy, not a bucket. */
  std::string name = suitename + testname;
  std::replace(name.begin(), name.end(), '/', '-');
  return name;
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
  int ret = testdir->create(env->dpp, null_yield, &existed);

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
  int ret = testdir->create(env->dpp, null_yield, &existed);

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
  ret = testdir->copy(env->dpp, null_yield, root.get(), copyname,
		      nsfs::CopyHints{});
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
  int ret = testdir->create(env->dpp, null_yield, &existed);
  EXPECT_EQ(ret, 0);

  ret = testdir->open(env->dpp);
  EXPECT_EQ(ret, 0);

  std::string subdirname{"SubDir"};
  sf::path sp{base_path / dirname / subdirname};
  std::unique_ptr<nsfs::Directory> subdir =
    std::make_unique<nsfs::Directory>(subdirname, testdir.get(), env->cct.get());
  ret = subdir->create(env->dpp, null_yield, &existed);
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
  ret = subsubdir->create(env->dpp, null_yield, &existed);
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
  int ret = testfile.create(env->dpp, null_yield, &existed);

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
  int ret = testfile.create(env->dpp, null_yield, &existed, true);
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
  int ret = testfile->create(env->dpp, null_yield, &existed);

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

  int ret = testfile->create(env->dpp, null_yield);
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


/* A file that arrives in a marked bucket without going through RGW is
 * its bucket owner's.
 *
 * An NFS or POSIX client writes no ACL, and no conversion walk will
 * visit the file afterwards.  NooBaa returns the bucket's owner for
 * such a file, and so does RGW core on the request path when an object
 * has no ACL (DECISIONS.md 2026-10-11) -- so the listing entry and
 * get_owner() return the same owner, rather than `unknown`.
 *
 * The control is a second sideloaded file carrying a recorded ACL for
 * somebody else:  a recorded owner is the object's, so the two must
 * differ, and the first assertion cannot pass by reporting the bucket
 * owner for everything. */
TEST_F(NSFSBucketTest, ASideloadedFileIsItsBucketOwners)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_STRNE(b->profile_name(), "base") << "the fixture is not marked";

  auto sideload = [&](const char* name) {
    const sf::path f{bucket_path() / name};
    int fd = ::open(f.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    EXPECT_GE(fd, 0) << cpp_strerror(errno);
    EXPECT_EQ(::write(fd, "x", 1), 1);
    ::close(fd);
    return f;
  };
  sideload("nobodys.txt");
  const sf::path other = sideload("somebodys.txt");

  const rgw_user someone{"", "somebody-else"};
  RGWAccessControlPolicy policy;
  policy.create_default(someone, "Somebody Else");
  bufferlist aclbl;
  policy.encode(aclbl);
  const std::string disk = b->xattr_strategy()->disk_name(RGW_ATTR_ACL);
  ASSERT_FALSE(disk.empty());
  ASSERT_EQ(::setxattr(other.c_str(), disk.c_str(), aclbl.c_str(),
		       aclbl.length(), 0), 0) << cpp_strerror(errno);

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  ASSERT_EQ(bucket->list(env->dpp, params, 128, results, null_yield), 0);
  std::map<std::string, std::string> owners;
  for (const auto& e : results.objs) {
    owners[e.key.name] = e.meta.owner;
  }
  EXPECT_EQ(owners["nobodys.txt"], to_string(bucket->get_info().owner))
      << "a file recording no owner did not list as its bucket's";
  EXPECT_EQ(owners["somebodys.txt"], to_string(rgw_owner{someone}))
      << "a recorded owner was replaced by the bucket's";

  /* and get_owner(), which HEAD uses, agrees with the listing */
  auto obj = bucket->get_object(rgw_obj_key("nobodys.txt"));
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);
  std::unique_ptr<rgw::sal::User> u;
  ASSERT_EQ(static_cast<rgw::sal::NSFSObject*>(obj.get())->get_owner(
		env->dpp, null_yield, &u), 0);
  EXPECT_EQ(rgw_owner{u->get_id()}, bucket->get_info().owner);
}


/* I9:  a user stored for the first time in an account gets a copy of
 * its account's identity row, as NooBaa's CreateUser copies the
 * account's nsfs_account_config, and the copy is never updated.  The
 * override decides only whether it is consulted.  An account with no
 * row gives none, and a later store does not make one. */
TEST_F(NSFSBucketTest, ANewAccountUserCopiesItsAccountsRow)
{
  auto* db = driver->get_identity_db();
  const rgw_account_id acct{"RGW66666666666666666"};
  nsfs::Identity arow;
  arow.key = acct;
  arow.uid = 4242; arow.gid = 4242;
  arow.groups = std::vector<uint32_t>{77};
  arow.new_buckets_path = "/elsewhere/acct";
  ASSERT_EQ(db->put_identity(env->dpp, arow), 0);

  /* an NSFSUser, not the fixture's TestUser, which stores through the
   * user table alone and would leave this test vacuous */
  auto store = [&] (const rgw_user& id, const rgw_account_id& in) {
    auto u = std::make_unique<rgw::sal::NSFSUser>(driver.get(), id);
    u->get_info().user_id = id;
    /* an existing user is updated as RGW's paths update one:  loaded
     * first, so the store carries its version */
    (void) u->load_user(env->dpp, null_yield);
    u->get_info().account_id = in;
    u->get_info().display_name = id.id;
    return u->store_user(env->dpp, null_yield, false, nullptr);
  };
  const rgw_user fred{"", "copy-fred"};
  ASSERT_EQ(store(fred, acct), 0);

  nsfs::Identity copy;
  ASSERT_EQ(db->get_identity(env->dpp, fred.to_str(), copy), 0)
      << "a new account user got no copy";
  EXPECT_EQ(copy.uid, arow.uid);
  EXPECT_EQ(copy.gid, arow.gid);
  EXPECT_EQ(copy.groups, arow.groups);
  EXPECT_EQ(copy.new_buckets_path, arow.new_buckets_path);

  /* the account changes;  the copy does not, and a second store of the
   * same user does not refresh it */
  arow.uid = 4343; arow.gid = 4343;
  ASSERT_EQ(db->put_identity(env->dpp, arow), 0);
  ASSERT_EQ(store(fred, acct), 0);
  ASSERT_EQ(db->get_identity(env->dpp, fred.to_str(), copy), 0);
  EXPECT_EQ(copy.uid, 4242u) << "the copy followed its account";

  /* which row fred runs as is the override's */
  nsfs::Credentials cred;
  db->force_user_identity_override(false);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, fred, acct, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 4343u) << "without the override fred is the account";
  db->force_user_identity_override(true);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, fred, acct, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 4242u) << "under the override fred is his copy";
  db->force_user_identity_override(std::nullopt);

  /* a user outside any account gets no row */
  const rgw_user plain{"", "copy-plain"};
  ASSERT_EQ(store(plain, rgw_account_id{}), 0);
  EXPECT_EQ(db->get_identity(env->dpp, plain.to_str(), copy), -ENOENT);

  /* an account with no row gives none, now or on a later store */
  const rgw_account_id bare{"RGW77777777777777777"};
  const rgw_user gail{"", "copy-gail"};
  ASSERT_EQ(store(gail, bare), 0);
  EXPECT_EQ(db->get_identity(env->dpp, gail.to_str(), copy), -ENOENT);
  nsfs::Identity brow;
  brow.key = bare;
  brow.uid = 5151; brow.gid = 5151;
  ASSERT_EQ(db->put_identity(env->dpp, brow), 0);
  ASSERT_EQ(store(gail, bare), 0);
  EXPECT_EQ(db->get_identity(env->dpp, gail.to_str(), copy), -ENOENT)
      << "a later store copied a row the account gained after the user";
}

/* A user's stored record is compare-and-swap on its version:  writers that
 * read it concurrently each either apply or get -ECANCELED and retry, as
 * RGW's callers do, and no write is lost.  dbstore checked the version and
 * then wrote with INSERT OR REPLACE, two steps nothing held together, so
 * two writers that read the same version both succeeded and the later one
 * wrote over the earlier -- a concurrent caps grant on one Admin Ops user
 * lost a cap that way (run 20261027-135946).  Each thread here adds its
 * own placement tags, a list nothing validates, so every one must be
 * there at the end. */
TEST_F(NSFSBucketTest, ConcurrentUserWritesLoseNothing)
{
  auto* udb = driver->get_user_db();
  RGWUserInfo info;
  info.user_id = rgw_user{"", "cas-user"};
  info.display_name = "cas";
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;
  ASSERT_EQ(udb->store_user(env->dpp, info, true, &attrs, &objv, nullptr), 0);

  constexpr int threads = 16;
  constexpr int each = 8;
  std::atomic<int> cancelled{0};
  std::atomic<int> failed{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < threads; ++t) {
    ts.emplace_back([&, t] {
      for (int i = 0; i < each; ++i) {
	const std::string tag = "t" + std::to_string(t) + "-" + std::to_string(i);
	for (;;) {
	  RGWUserInfo u;
	  u.user_id = rgw_user{"", "cas-user"};
	  rgw::sal::Attrs a;
	  RGWObjVersionTracker v;
	  if (udb->get_user(env->dpp, "user_id", "cas-user", u, &a, &v) < 0) {
	    ++failed;
	    return;
	  }
	  u.placement_tags.push_back(tag);
	  const int r = udb->store_user(env->dpp, u, false, &a, &v, nullptr);
	  if (r == -ECANCELED) {
	    ++cancelled;
	    continue;
	  }
	  if (r < 0) {
	    ++failed;
	  }
	  break;
	}
      }
    });
  }
  for (auto& t : ts) {
    t.join();
  }
  ASSERT_EQ(failed.load(), 0);

  RGWUserInfo end;
  end.user_id = rgw_user{"", "cas-user"};
  ASSERT_EQ(udb->get_user(env->dpp, "user_id", "cas-user", end, nullptr,
			  nullptr), 0);
  std::set<std::string> have(end.placement_tags.begin(),
			     end.placement_tags.end());
  EXPECT_EQ(have.size(), static_cast<size_t>(threads * each))
      << "writes were lost:  " << have.size() << " of " << threads * each
      << " tags stored, after " << cancelled.load() << " retries";
}

/* And the plain case:  a write carrying a version the row no longer has
 * is refused, and the row is left as the newer write made it. */
TEST_F(NSFSBucketTest, AStaleUserWriteIsRefused)
{
  auto* udb = driver->get_user_db();
  RGWUserInfo info;
  info.user_id = rgw_user{"", "stale-user"};
  info.display_name = "stale";
  rgw::sal::Attrs attrs;
  RGWObjVersionTracker objv;
  ASSERT_EQ(udb->store_user(env->dpp, info, true, &attrs, &objv, nullptr), 0);

  auto load = [&] (RGWUserInfo& u, RGWObjVersionTracker& v) {
    u.user_id = rgw_user{"", "stale-user"};
    rgw::sal::Attrs a;
    return udb->get_user(env->dpp, "user_id", "stale-user", u, &a, &v);
  };
  RGWUserInfo first, second;
  RGWObjVersionTracker v1, v2;
  ASSERT_EQ(load(first, v1), 0);
  ASSERT_EQ(load(second, v2), 0);
  first.max_buckets = 11;
  ASSERT_EQ(udb->store_user(env->dpp, first, false, &attrs, &v1, nullptr), 0);
  second.max_buckets = 22;
  EXPECT_EQ(udb->store_user(env->dpp, second, false, &attrs, &v2, nullptr),
	    -ECANCELED);
  RGWUserInfo now;
  RGWObjVersionTracker vn;
  ASSERT_EQ(load(now, vn), 0);
  EXPECT_EQ(now.max_buckets, 11) << "the stale write replaced the newer one";
}

/* Stored state that will not decode fails the bucket closed.
 *
 * Absent and unreadable are different.  A bucket with no stored state
 * has none and serves;  a bucket whose state is there and unreadable
 * would otherwise serve on defaults, and defaults are permissive --
 * no object lock, no policy, no public access block, versioning off.
 */
TEST_F(NSFSBucketTest, UnreadableStateFailsClosedAndAbsentDoesNot)
{
  const sf::path bpath{bp / "root" / testname};
  static constexpr const char* key = "user.nsfs:bucket_info";

  /* the control:  as created, it loads */
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0)
      << "the bucket does not load before anything was broken";

  /* present and undecodable */
  const std::string junk{"not an encoded RGWBucketInfo"};
  ASSERT_EQ(::setxattr(bpath.c_str(), key, junk.data(), junk.size(), 0), 0);
  EXPECT_EQ(bucket->load_bucket(env->dpp, null_yield), -EBADMSG);

  /* absent, which is a different result */
  ASSERT_EQ(::removexattr(bpath.c_str(), key), 0);
  EXPECT_EQ(bucket->load_bucket(env->dpp, null_yield), 0)
      << "a bucket with no stored state must still serve";
}

/* A bucket that fails closed is one bucket.
 *
 * ListBuckets skips it rather than denying the account every other
 * bucket, and the profile endpoint still responds -- a profile comes
 * from the marker, and an operator diagnosing the failure needs that
 * endpoint precisely then. */
TEST_F(NSFSBucketTest, UnreadableStateDoesNotDenyTheListingOrTheProfile)
{
  const sf::path bpath{bp / "root" / testname};
  const std::string junk{"not an encoded RGWBucketInfo"};
  ASSERT_EQ(::setxattr(bpath.c_str(), "user.nsfs:bucket_info",
		       junk.data(), junk.size(), 0), 0);

  rgw::sal::BucketList result;
  EXPECT_EQ(driver->list_buckets(env->dpp, owner, std::string(),
				 std::string(), std::string(), 100, false,
				 result, null_yield), 0)
      << "one unreadable bucket denied the whole listing";

  uint32_t ext = 0;
  std::string pname;
  bool converting = true;
  EXPECT_EQ(driver->get_bucket_profile(env->dpp, null_yield, testname,
				       &ext, &pname, &converting), 0)
      << "the profile endpoint went blind on the bucket that needs it";
}

/* The identity table.
 *
 * A fixture of its own, and not one deriving from NSFSDriverTest,
 * because the suite's TestDriver is constructed with a null
 * CephContext -- so the driver's own IdentityDB can never open.  The
 * database is a standalone object, which is what stage A is about,
 * so the fixture builds one directly against the test's own
 * directory. */
class NSFSIdentityDBTest : public NSFSDriverTest {
protected:
  std::unique_ptr<nsfs::IdentityDB> db;

public:
  /* The driver comes from the base fixture, for the tests that need
   * a sal::User to build an applier around.  The database does not:
   * the suite's TestDriver is constructed with a null CephContext,
   * so the driver's own IdentityDB can never open.  This one is
   * built directly against the test's directory. */
  void SetUp() {
    NSFSDriverTest::SetUp();
    db = std::make_unique<nsfs::IdentityDB>((bp / "identity").string(),
					    env->cct.get());
    ASSERT_EQ(db->Initialize("", -1), 0);
  }

  void TearDown() {
    db.reset();
    NSFSDriverTest::TearDown();
  }

  /* A real LocalApplier, which is what makes the account/user
   * distinction below a statement about RGW rather than about a
   * stub written to agree with me. */
  std::unique_ptr<rgw::auth::LocalApplier>
  local_applier(const rgw_user& uid, const char* account_id) {
    auto user = driver->get_user(uid);
    user->get_info().user_id = uid;
    std::optional<RGWAccountInfo> account;
    if (account_id) {
      RGWAccountInfo ai;
      ai.id = account_id;
      ai.name = "an-account";
      user->get_info().account_id = account_id;
      account = ai;
    }
    return std::make_unique<rgw::auth::LocalApplier>(
	env->cct.get(), std::move(user), std::move(account),
	std::vector<rgw::IAM::Policy>{},
	rgw::auth::LocalApplier::NO_SUBUSER, std::nullopt,
	rgw::auth::LocalApplier::NO_ACCESS_KEY);
  }

  /* every column populated, so a round trip proves the whole record
   * and not the subset impersonation reads */
  static nsfs::Identity local_identity(const std::string& key) {
    nsfs::Identity id;
    id.key = key;
    id.uid = 1001;
    id.gid = 2002;
    id.groups = std::vector<uint32_t>{10, 20, 30};
    id.new_buckets_path = "/gpfs/rgw1/nsfs/newbuckets";
    id.custom_bucket_path_allowed_list = "/gpfs/rgw1/nsfs/allowed";
    id.fs_backend = "GPFS";
    return id;
  }
};

/* Every column round-trips, including the ones no request path
 * reads.  A column no test writes and reads is a column whose first
 * real use finds its bugs. */
TEST_F(NSFSIdentityDBTest, EveryColumnRoundTrips)
{
  /* the control:  nothing is there before it is put */
  nsfs::Identity got;
  ASSERT_EQ(db->get_identity(env->dpp, "user$alice", got), -ENOENT);

  const auto put = local_identity("user$alice");
  ASSERT_EQ(db->put_identity(env->dpp, put), 0);
  ASSERT_EQ(db->get_identity(env->dpp, "user$alice", got), 0);

  EXPECT_EQ(got.key, put.key);
  ASSERT_TRUE(got.uid.has_value());
  EXPECT_EQ(*got.uid, 1001u);
  ASSERT_TRUE(got.gid.has_value());
  EXPECT_EQ(*got.gid, 2002u);
  ASSERT_TRUE(got.groups.has_value());
  EXPECT_EQ(*got.groups, (std::vector<uint32_t>{10, 20, 30}));
  EXPECT_EQ(got.new_buckets_path, put.new_buckets_path);
  EXPECT_EQ(got.custom_bucket_path_allowed_list,
	    put.custom_bucket_path_allowed_list);
  EXPECT_EQ(got.fs_backend, "GPFS");
  EXPECT_TRUE(got.distinguished_name.empty());
}

/* A directory-backed identity is the other form:  a name and no ids. */
TEST_F(NSFSIdentityDBTest, DirectoryBackedIdentityRoundTrips)
{
  nsfs::Identity id;
  id.key = "user$bob";
  id.distinguished_name = "bob";
  id.new_buckets_path = "/gpfs/rgw1/nsfs/bob";
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  nsfs::Identity got;
  ASSERT_EQ(db->get_identity(env->dpp, "user$bob", got), 0);
  EXPECT_EQ(got.distinguished_name, "bob");
  EXPECT_FALSE(got.uid.has_value());
  EXPECT_FALSE(got.gid.has_value());
  EXPECT_TRUE(got.directory_backed());
  EXPECT_FALSE(got.local());
}

/* Unset, explicitly empty, and populated are three distinct states.
 *
 * Collapsing the first two would make "this identity has no
 * supplementary groups" indistinguishable from "nobody has said",
 * which is the difference the directory-backed form will turn on. */
TEST_F(NSFSIdentityDBTest, GroupsDistinguishUnsetFromEmpty)
{
  nsfs::Identity id;
  id.key = "user$carol";
  id.uid = 1; id.gid = 1;

  id.groups = std::nullopt;
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  nsfs::Identity got;
  ASSERT_EQ(db->get_identity(env->dpp, "user$carol", got), 0);
  EXPECT_FALSE(got.groups.has_value()) << "unset became something";

  id.groups = std::vector<uint32_t>{};
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  ASSERT_EQ(db->get_identity(env->dpp, "user$carol", got), 0);
  ASSERT_TRUE(got.groups.has_value()) << "an explicit empty list became unset";
  EXPECT_TRUE(got.groups->empty());

  id.groups = std::vector<uint32_t>{7};
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  ASSERT_EQ(db->get_identity(env->dpp, "user$carol", got), 0);
  ASSERT_TRUE(got.groups.has_value());
  EXPECT_EQ(*got.groups, (std::vector<uint32_t>{7}));
}

/* Each field updates independently:  a put that changes one leaves
 * the others as they were. */
TEST_F(NSFSIdentityDBTest, FieldsUpdateIndependently)
{
  auto id = local_identity("user$dave");
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  id.fs_backend = "CEPH_FS";
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  nsfs::Identity got;
  ASSERT_EQ(db->get_identity(env->dpp, "user$dave", got), 0);
  EXPECT_EQ(got.fs_backend, "CEPH_FS");
  EXPECT_EQ(*got.uid, 1001u) << "an unrelated field moved";
  EXPECT_EQ(got.new_buckets_path, "/gpfs/rgw1/nsfs/newbuckets");
}

/* The exclusivity rule is enforced by the schema, not by a caller.
 *
 * NooBaa's own schema will not validate a record holding both forms,
 * so neither may this one -- otherwise an importer could produce a
 * record NooBaa's tooling would reject. */
TEST_F(NSFSIdentityDBTest, BothIdentityArmsAreRejected)
{
  /* the control:  each form alone is accepted */
  nsfs::Identity local;
  local.key = "user$ok-local";
  local.uid = 5; local.gid = 5;
  ASSERT_EQ(db->put_identity(env->dpp, local), 0);

  nsfs::Identity dir;
  dir.key = "user$ok-dir";
  dir.distinguished_name = "someone";
  ASSERT_EQ(db->put_identity(env->dpp, dir), 0);

  nsfs::Identity both;
  both.key = "user$both";
  both.uid = 5; both.gid = 5;
  both.distinguished_name = "someone";
  EXPECT_EQ(db->put_identity(env->dpp, both), -EINVAL);

  nsfs::Identity got;
  EXPECT_EQ(db->get_identity(env->dpp, "user$both", got), -ENOENT)
      << "a rejected record was stored anyway";
}

/* A uid without a gid is half an identity and is rejected. */
TEST_F(NSFSIdentityDBTest, AUidWithoutAGidIsRejected)
{
  nsfs::Identity id;
  id.key = "user$halfway";
  id.uid = 9;
  EXPECT_EQ(db->put_identity(env->dpp, id), -EINVAL);
}

/* The re-import lookup:  NooBaa's id to the owner it became. */
TEST_F(NSFSIdentityDBTest, LookupByNoobaaId)
{
  std::string owner;
  /* the control:  nothing is recorded before it is put */
  ASSERT_EQ(db->get_owner_by_noobaa_id(
	      env->dpp, "6a2bdeabf5c8e167f92cb079", owner), -ENOENT);

  ASSERT_EQ(db->put_noobaa_id(env->dpp, "6a2bdeabf5c8e167f92cb079",
			      "RGW12345678901234567"), 0);
  ASSERT_EQ(db->get_owner_by_noobaa_id(
	      env->dpp, "6a2bdeabf5c8e167f92cb079", owner), 0);
  EXPECT_EQ(owner, "RGW12345678901234567");

  EXPECT_EQ(db->get_owner_by_noobaa_id(env->dpp, "nosuchid", owner), -ENOENT);
}

/* Recording an id twice is rejected, and the first record survives.
 *
 * The table is written with a plain INSERT for exactly this:  under
 * OR REPLACE a second import of one id would silently repoint it,
 * which is the importer failing to recognise its own output. */
TEST_F(NSFSIdentityDBTest, ANoobaaIdIsRecordedOnce)
{
  ASSERT_EQ(db->put_noobaa_id(env->dpp, "6a2bdeabf5c8e167f92cb079",
			      "RGW12345678901234567"), 0);
  EXPECT_EQ(db->put_noobaa_id(env->dpp, "6a2bdeabf5c8e167f92cb079",
			      "RGW76543210987654321"), -EEXIST);

  std::string owner;
  ASSERT_EQ(db->get_owner_by_noobaa_id(
	      env->dpp, "6a2bdeabf5c8e167f92cb079", owner), 0);
  EXPECT_EQ(owner, "RGW12345678901234567") << "the second put repointed it";

  /* the control:  a different id is accepted, so the rejection above is
   * about the id and not about the table */
  EXPECT_EQ(db->put_noobaa_id(env->dpp, "6a2bdeabf5c8e167f92cb07a",
			      "RGW76543210987654321"), 0);
}

/* Delete removes it;  deleting what is absent is not an error,
 * because the caller asked for it to be gone and it is. */
TEST_F(NSFSIdentityDBTest, RemoveAndList)
{
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$a")), 0);
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$b")), 0);

  std::vector<nsfs::Identity> all;
  ASSERT_EQ(db->list_identities(env->dpp, all), 0);
  ASSERT_EQ(all.size(), 2u);

  ASSERT_EQ(db->remove_identity(env->dpp, "user$a"), 0);
  nsfs::Identity got;
  EXPECT_EQ(db->get_identity(env->dpp, "user$a", got), -ENOENT);
  EXPECT_EQ(db->remove_identity(env->dpp, "user$a"), 0)
      << "removing an absent identity should not be an error";

  all.clear();
  ASSERT_EQ(db->list_identities(env->dpp, all), 0);
  EXPECT_EQ(all.size(), 1u);
}

/* Resolution:  the stored row becomes the credentials a request is
 * served under. */

/* The process's own supplementary groups, which the resolved vector
 * must never be.  If this is empty the clear cases below cannot fail
 * for the reason they are testing, so they say so. */
static std::vector<gid_t> process_groups()
{
  int n = getgroups(0, nullptr);
  if (n <= 0) {
    return {};
  }
  std::vector<gid_t> g(n);
  n = getgroups(n, g.data());
  if (n < 0) {
    return {};
  }
  g.resize(n);
  return g;
}

TEST_F(NSFSIdentityDBTest, ResolveLocalIdentity)
{
  const rgw_owner owner = parse_owner("user$resolve");
  ASSERT_EQ(db->put_identity(env->dpp,
			     local_identity(to_string(owner))), 0);

  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred, null_yield), 0);
  EXPECT_EQ(cred.uid, 1001u);
  EXPECT_EQ(cred.gid, 2002u);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{10, 20, 30}));
}

/* An account id is an owner too, and the key resolution looks under
 * has to be the one the endpoint wrote. */
TEST_F(NSFSIdentityDBTest, ResolveAccountIdOwner)
{
  const rgw_owner owner = parse_owner("RGW12345678901234567");
  ASSERT_TRUE(std::holds_alternative<rgw_account_id>(owner))
      << "the fixture's id is not being read as an account id";
  ASSERT_EQ(db->put_identity(env->dpp,
			     local_identity(to_string(owner))), 0);

  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred, null_yield), 0);
  EXPECT_EQ(cred.uid, 1001u);
}

/* The sharpest case.  An absent group list means no supplementary
 * groups, installed, not "leave whatever the process holds" -- the
 * reading that grants access rather than denying it.
 *
 * The control is the process's own group set:  unless it is
 * non-empty, an empty result here is indistinguishable from having
 * inherited it, and the test proves nothing. */
TEST_F(NSFSIdentityDBTest, ResolveAbsentGroupListClears)
{
  const auto mine = process_groups();
  ASSERT_FALSE(mine.empty())
      << "this process holds no supplementary groups, so an empty "
	 "resolved vector cannot be distinguished from an inherited one";

  nsfs::Identity id;
  id.key = "user$nogroups";
  id.uid = 1; id.gid = 1;
  id.groups = std::nullopt;
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db,
				      parse_owner(id.key), cred, null_yield), 0);
  EXPECT_TRUE(cred.groups.empty())
      << "an absent list resolved to " << cred.groups.size() << " groups";
  for (auto g : mine) {
    EXPECT_EQ(std::find(cred.groups.begin(), cred.groups.end(), g),
	      cred.groups.end())
	<< "the process's own group " << g << " reached the credentials";
  }
}

/* An explicitly empty list resolves the same way.  The two stay
 * distinct in the record for the directory-backed form;  they are not
 * distinct here. */
TEST_F(NSFSIdentityDBTest, ResolveEmptyGroupListClears)
{
  nsfs::Identity id;
  id.key = "user$emptygroups";
  id.uid = 1; id.gid = 1;
  id.groups = std::vector<uint32_t>{};
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db,
				      parse_owner(id.key), cred, null_yield), 0);
  EXPECT_TRUE(cred.groups.empty());
}

/* No row is not an error:  it means nothing asked for impersonation,
 * and the caller does what it did before this existed. */
TEST_F(NSFSIdentityDBTest, ResolveNoRowIsNotAnError)
{
  nsfs::Credentials cred;
  cred.uid = 4242;			/* must not be written to */
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db,
				      parse_owner("user$absent"), cred, null_yield),
	    -ENOENT);
  EXPECT_EQ(cred.uid, 4242u) << "a failed resolve wrote to its output";
}

/* A row carrying only placement data asks for no impersonation.
 * That is the no-row case, not the cannot-supply one. */
TEST_F(NSFSIdentityDBTest, ResolvePlacementOnlyRowAsksForNothing)
{
  nsfs::Identity id;
  id.key = "user$placementonly";
  id.new_buckets_path = "/gpfs/rgw1/nsfs/somewhere";
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  nsfs::Credentials cred;
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db,
				      parse_owner(id.key), cred, null_yield), -ENOENT);
}

/* I7.  An nsswitch-resolved row -- `distinguished_name`, the field NooBaa
 * passes to getpwnam -- is resolved through nsswitch.  The lookup is
 * replaced here, so each outcome nsswitch can return is exercised;
 * NsswitchRealLookup below calls the real one. */
namespace {

struct FakeDirectory {
  std::mutex m;
  std::map<std::string, int> fail;	/* name -> -errno */
  std::map<std::string, nsfs::ResolvedName> names;
  int calls{0};
  /* I8:  users by uid, and each name's group list */
  std::map<uint32_t, std::pair<std::string, uint32_t>> users;
  std::map<std::string, std::vector<uint32_t>> groups;
  std::map<std::string, int> group_fail;	/* name -> -errno */
  int group_calls{0};

  nsfs::NameLookupFn fn() {
    return [this] (const std::string& name, nsfs::ResolvedName& out) {
      std::lock_guard l(m);
      ++calls;
      if (auto f = fail.find(name); f != fail.end()) {
	return f->second;
      }
      auto i = names.find(name);
      if (i == names.end()) {
	return -ENOENT;
      }
      out = i->second;
      return 0;
    };
  }

  nsfs::GroupLookupFn groups_fn() {
    return [this] (const std::string& name, uint32_t,
		   std::vector<uint32_t>& out) {
      std::lock_guard l(m);
      ++group_calls;
      if (auto f = group_fail.find(name); f != group_fail.end()) {
	return f->second;
      }
      auto i = groups.find(name);
      out = (i == groups.end()) ? std::vector<uint32_t>{} : i->second;
      return 0;
    };
  }

  nsfs::UserLookupFn user_fn() {
    return [this] (uint32_t uid, std::string& name, uint32_t& gid) {
      std::lock_guard l(m);
      auto i = users.find(uid);
      if (i == users.end()) {
	return -ENOENT;
      }
      name = i->second.first;
      gid = i->second.second;
      return 0;
    };
  }

  void install(nsfs::NsswitchCache& c) {
    c.set_lookup_for_test(fn());
    c.set_group_lookups_for_test(groups_fn(), user_fn());
  }
};

/* A local row with no list:  NooBaa's dynamic groups apply to it too,
 * by uid. */
nsfs::Identity listless_identity(const std::string& key, uint32_t uid,
				 uint32_t gid)
{
  nsfs::Identity id;
  id.key = key;
  id.uid = uid;
  id.gid = gid;
  return id;
}

nsfs::Identity name_identity(const std::string& key, const std::string& name)
{
  nsfs::Identity id;
  id.key = key;
  id.distinguished_name = name;
  return id;
}

} // namespace

/* Resolved and served:  the uid and gid are nsswitch's.  The control is
 * the same row before the directory knows the name, which must be
 * refused -- without it a resolver that returned a fixed answer would
 * pass. */
TEST_F(NSFSIdentityDBTest, NsswitchResolvesThroughNsswitch)
{
  FakeDirectory dir;
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  const rgw_owner owner = parse_owner("user$byname");
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "nsfsname")), 0);

  nsfs::Credentials cred;
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), -EPERM)
      << "control:  a name nsswitch does not know resolved";

  dir.names["nsfsname"] = {70101, 100};
  db->nsswitch_cache().refresh(env->dpp, "nsfsname");
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 70101u);
  EXPECT_EQ(cred.gid, 100u);
  EXPECT_TRUE(cred.groups.empty())
      << "no list on the row means none until I8";
}

/* The row's explicit list wins:  nsswitch supplies only the uid and the
 * primary gid. */
TEST_F(NSFSIdentityDBTest, NsswitchTakesTheRowsGroupList)
{
  FakeDirectory dir;
  dir.names["nsfsname"] = {70101, 100};
  db->nsswitch_cache().set_lookup_for_test(dir.fn());

  auto id = name_identity("user$byname", "nsfsname");
  id.groups = std::vector<uint32_t>{5, 6};
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{5, 6}));

  id.groups = std::vector<uint32_t>{};
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_TRUE(cred.groups.empty())
      << "control:  the list read back was not the row's";
}

/* A lookup that fails is refused, as NooBaa's request is, and is not
 * cached:  the control is the next call after the directory recovers,
 * which must resolve. */
TEST_F(NSFSIdentityDBTest, NsswitchLookupFailureIsRefusedAndNotCached)
{
  FakeDirectory dir;
  dir.names["nsfsname"] = {70101, 100};
  dir.fail["nsfsname"] = -EAGAIN;
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  const rgw_owner owner = parse_owner("user$byname");
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "nsfsname")), 0);

  nsfs::Credentials cred;
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), -EPERM);

  dir.fail.clear();
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0)
      << "a failed lookup was cached";
  EXPECT_EQ(cred.uid, 70101u);
}

/* A hit costs no lookup;  the control is a refresh, after which the
 * next lookup is the directory's. */
TEST_F(NSFSIdentityDBTest, NsswitchHitsDoNotLookUp)
{
  FakeDirectory dir;
  dir.names["nsfsname"] = {70101, 100};
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  const rgw_owner owner = parse_owner("user$byname");
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "nsfsname")), 0);

  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  EXPECT_EQ(dir.calls, 1);

  dir.names["nsfsname"] = {70102, 100};
  EXPECT_EQ(cred.uid, 70101u) << "the cache was bypassed";
  EXPECT_EQ(db->nsswitch_cache().refresh(env->dpp, "nsfsname"), 1);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 70102u);
}

/* The identity an unauthenticated request is served as may take the name
 * form too -- NooBaa's anonymous account can carry a distinguished name
 * (`ACCOUNT_IMPORT.md` §6.8) -- and it resolves under RGW's anonymous
 * user like any other key.  The control is the same row before the
 * directory knows the name, which must be refused. */
TEST_F(NSFSIdentityDBTest, NsswitchResolvesTheAnonymousIdentity)
{
  FakeDirectory dir;
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  const rgw_owner anon = rgw_user(RGW_USER_ANON_ID);
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(anon),
						     "nsfsanon")), 0);

  nsfs::Credentials cred;
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, anon, cred,
				      null_yield), -EPERM)
      << "control:  a name nsswitch does not know resolved";

  dir.names["nsfsanon"] = {70105, 100};
  db->nsswitch_cache().refresh(env->dpp, "nsfsanon");
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, anon, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 70105u);
}

/* The bound.  A personality hit resolves nothing, so the sweep is what
 * notices a change, and it does so by moving the identity generation.
 * Every outcome counts:  a new uid, a name that went, one that
 * appeared, a lookup that failed.  The control is a sweep over an
 * unchanged directory, which must move nothing. */
TEST_F(NSFSIdentityDBTest, NsswitchSweepInvalidatesOnChange)
{
  FakeDirectory dir;
  dir.names["nsfsname"] = {70101, 100};
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  const rgw_owner owner = parse_owner("user$byname");
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "nsfsname")), 0);
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);

  uint64_t gen = nsfs::identity_generation(owner);
  EXPECT_EQ(db->nsswitch_cache().sweep(env->dpp), 0);
  EXPECT_EQ(nsfs::identity_generation(owner), gen)
      << "control:  an unchanged directory invalidated";

  dir.names["nsfsname"] = {70102, 100};		/* a new uid */
  EXPECT_EQ(db->nsswitch_cache().sweep(env->dpp), 1);
  EXPECT_NE(nsfs::identity_generation(owner), gen);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 70102u);

  gen = nsfs::identity_generation(owner);
  dir.names.erase("nsfsname");			/* the name went */
  EXPECT_EQ(db->nsswitch_cache().sweep(env->dpp), 1);
  EXPECT_NE(nsfs::identity_generation(owner), gen);
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), -EPERM);

  gen = nsfs::identity_generation(owner);
  dir.names["nsfsname"] = {70101, 100};		/* and came back */
  EXPECT_EQ(db->nsswitch_cache().sweep(env->dpp), 1);
  EXPECT_NE(nsfs::identity_generation(owner), gen);

  gen = nsfs::identity_generation(owner);
  dir.fail["nsfsname"] = -EIO;			/* the lookup failed */
  EXPECT_EQ(db->nsswitch_cache().sweep(env->dpp), 1);
  EXPECT_NE(nsfs::identity_generation(owner), gen);
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), -EPERM)
      << "a warm answer outlived a failing directory";
}

/* Clearing an entry makes the next resolution a lookup and invalidates
 * identities, so a warm personality resolves again too.  The control is
 * clearing a name the cache does not hold, which must move nothing. */
TEST_F(NSFSIdentityDBTest, NsswitchForgetClearsAndInvalidates)
{
  FakeDirectory dir;
  dir.names["nsfsname"] = {70101, 100};
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  const rgw_owner owner = parse_owner("user$byname");
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "nsfsname")), 0);
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);

  uint64_t gen = nsfs::identity_generation(owner);
  EXPECT_FALSE(db->nsswitch_cache().forget("othername"));
  EXPECT_EQ(nsfs::identity_generation(owner), gen)
      << "control:  clearing an absent entry invalidated";

  dir.names["nsfsname"] = {70102, 100};
  EXPECT_TRUE(db->nsswitch_cache().forget("nsfsname"));
  EXPECT_NE(nsfs::identity_generation(owner), gen);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 70102u);
  EXPECT_EQ(dir.calls, 2);
}

/* A row written or removed is the gateway's own evidence that a name may
 * have changed, so the names it touches are cleared:  the same name
 * rewritten is looked up again, a name the row no longer carries is
 * dropped, and so is a removed row's.  The control is a write to an
 * unrelated row, which must leave the cached answer alone. */
TEST_F(NSFSIdentityDBTest, NsswitchRowWritesClearTheirNames)
{
  FakeDirectory dir;
  dir.names["nsfsname"] = {70101, 100};
  dir.names["othername"] = {70103, 100};
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  const rgw_owner owner = parse_owner("user$byname");
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "nsfsname")), 0);
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);

  dir.names["nsfsname"] = {70102, 100};
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$unrelated")), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 70101u) << "control:  an unrelated write cleared it";

  uint64_t gen = nsfs::identity_generation(owner);
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "nsfsname")), 0);
  EXPECT_NE(nsfs::identity_generation(owner), gen);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.uid, 70102u) << "the same name rewritten was not looked up";

  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "othername")), 0);
  EXPECT_TRUE(db->nsswitch_cache().names_for_uid(70102).empty())
      << "the name the row left behind was kept";

  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  ASSERT_EQ(db->nsswitch_cache().names_for_uid(70103).size(), 1u);
  ASSERT_EQ(db->remove_identity(env->dpp, to_string(owner)), 0);
  EXPECT_TRUE(db->nsswitch_cache().names_for_uid(70103).empty())
      << "a removed row's name was kept";
}

/* The sweep's first pass resolves every name the table carries, so one
 * nsswitch does not know is logged at startup.  The control is the
 * cache before the sweep starts, which holds nothing. */
TEST_F(NSFSIdentityDBTest, NsswitchSweepStartsByResolvingTheTable)
{
  FakeDirectory dir;
  dir.names["nsfsname"] = {70101, 100};
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  ASSERT_EQ(db->put_identity(env->dpp, name_identity("user$a", "nsfsname")),
	    0);
  ASSERT_EQ(db->put_identity(env->dpp, name_identity("user$b", "nobody")),
	    0);
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$c")), 0);
  EXPECT_TRUE(db->nsswitch_cache().names_for_uid(70101).empty())
      << "control:  the cache held a name before the sweep";

  db->start_name_sweep();
  for (int i = 0; i < 500; ++i) {
    {
      std::lock_guard l(dir.m);
      if (dir.calls >= 2) {
	break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  db->nsswitch_cache().stop();
  EXPECT_EQ(dir.calls, 2) << "the pass looked up other than the two names";
  EXPECT_EQ(db->nsswitch_cache().names_for_uid(70101),
	    (std::vector<std::string>{"nsfsname"}));

  /* both are held now:  neither is looked up again */
  nsfs::ResolvedName rn;
  EXPECT_EQ(db->nsswitch_cache().resolve(env->dpp, "nobody", rn, null_yield),
	    -ENOENT);
  EXPECT_EQ(dir.calls, 2);
}

/* With a coroutine the lookup runs on the pool and the coroutine
 * suspends, so a frontend thread serves other requests meanwhile.  The
 * lookup waits for a second coroutine on the same single-threaded
 * io_context;  it can run only if the first suspended.  The control is
 * `null_yield`, which runs the lookup on the calling thread and so
 * cannot see the second coroutine -- the lookup times out. */
TEST_F(NSFSIdentityDBTest, NsswitchLookupSuspendsTheCoroutine)
{
  const rgw_owner owner = parse_owner("user$byname");
  ASSERT_EQ(db->put_identity(env->dpp, name_identity(to_string(owner),
						     "nsfsname")), 0);

  for (const bool suspend : {true, false}) {
    std::mutex m;
    std::condition_variable cv;
    bool released = false;
    std::thread::id looked_up_on;
    db->nsswitch_cache().set_lookup_for_test(
      [&] (const std::string&, nsfs::ResolvedName& out) {
	std::unique_lock l(m);
	looked_up_on = std::this_thread::get_id();
	if (! cv.wait_for(l, std::chrono::seconds(1),
			  [&] { return released; })) {
	  return -ETIMEDOUT;
	}
	out = {70101, 100};
	return 0;
      });

    boost::asio::io_context io;
    int ret = 1;
    std::thread::id caller;
    boost::asio::spawn(io, [&] (boost::asio::yield_context yield) {
	caller = std::this_thread::get_id();
	nsfs::Credentials cred;
	ret = nsfs::resolve_credentials(
	  env->dpp, *db, owner, cred,
	  suspend ? optional_yield{yield} : null_yield);
      }, boost::asio::detached);
    boost::asio::spawn(io, [&] (boost::asio::yield_context) {
	std::lock_guard l(m);
	released = true;
	cv.notify_all();
      }, boost::asio::detached);
    io.run();

    if (suspend) {
      EXPECT_EQ(ret, 0) << "the coroutine held its thread";
      EXPECT_NE(looked_up_on, caller);
    } else {
      EXPECT_EQ(ret, -EPERM) << "control:  null_yield suspended";
      EXPECT_EQ(looked_up_on, caller);
    }
  }
}

/* The real nsswitch, on whatever source this host has.  root is in every
 * `files` source;  the control is a name no source has. */
TEST_F(NSFSIdentityDBTest, NsswitchRealLookup)
{
  nsfs::ResolvedName rn;
  ASSERT_EQ(nsfs::nsswitch_lookup("root", rn), 0);
  EXPECT_EQ(rn.uid, 0u);
  EXPECT_EQ(rn.gid, 0u);
  EXPECT_EQ(nsfs::nsswitch_lookup("uid=someone,ou=people,dc=example,dc=com",
				  rn), -ENOENT);
}

/* A credential built from a file carries a uid and no name, and a
 * nsswitch-resolved row stores no uid, so the row is found through the cache:
 * the names recently resolved to that uid.  The control is the same
 * completion before the name was resolved, which finds no row. */
TEST_F(NSFSIdentityDBTest, NsswitchCompletesFileCredentials)
{
  FakeDirectory dir;
  dir.names["nsfsname"] = {70101, 100};
  db->nsswitch_cache().set_lookup_for_test(dir.fn());
  auto id = name_identity("user$byname", "nsfsname");
  id.groups = std::vector<uint32_t>{7};
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  nsfs::Credentials file;
  file.uid = 70101;
  file.gid = 100;
  std::string key;
  EXPECT_EQ(nsfs::complete_file_credentials(env->dpp, *db, file, &key),
	    -ENOENT) << "control:  an unresolved name completed";

  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  ASSERT_EQ(nsfs::complete_file_credentials(env->dpp, *db, file, &key), 0);
  EXPECT_EQ(file.groups, (std::vector<gid_t>{7}));
  EXPECT_EQ(key, "user$byname");
}

/* I8.  Supplementary groups through nsswitch, under
 * rgw_nsfs_dynamic_supplemental_groups (`inflight/i8-plan.md`).  NooBaa's
 * rule, at the pin:  an explicit list wins, empty included;  otherwise,
 * switch on, the user's groups from getgrouplist (`object_sdk.js:313`).
 * The lookups are injected;  NsswitchRealGroups calls the real ones. */

/* Switch on, an explicit list wins over nsswitch.  The control is the
 * same row with no list, which takes nsswitch's. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsTheExplicitListWins)
{
  FakeDirectory dir;
  dir.users[70101] = {"nsfsuser", 70101};
  dir.groups["nsfsuser"] = {70101, 70010};
  dir.install(db->nsswitch_cache());
  db->nsswitch_cache().override_dynamic_groups(true);

  auto id = listless_identity("user$grouped", 70101, 70101);
  id.groups = std::vector<uint32_t>{5};
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{5}));

  id.groups.reset();
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{70101, 70010}))
      << "control:  a list-less row did not take nsswitch's groups";
}

/* The sharp edge:  an explicit *empty* list is no groups, not "ask
 * nsswitch" -- NooBaa tests the list's presence, and `[]` is present.
 * Getting it wrong grants access.  The control is the list-less row. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsTheExplicitEmptyListWins)
{
  FakeDirectory dir;
  dir.users[70101] = {"nsfsuser", 70101};
  dir.groups["nsfsuser"] = {70101, 70010};
  dir.install(db->nsswitch_cache());
  db->nsswitch_cache().override_dynamic_groups(true);

  auto id = listless_identity("user$grouped", 70101, 70101);
  id.groups = std::vector<uint32_t>{};
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_TRUE(cred.groups.empty()) << "an empty list asked nsswitch";
  EXPECT_EQ(dir.group_calls, 0);

  id.groups.reset();
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_FALSE(cred.groups.empty()) << "control:  no list, no groups";
}

/* Switch off, a list-less row has no groups and nsswitch is not asked --
 * today's behaviour, pinned.  The control is the switch on. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsOffGivesNone)
{
  FakeDirectory dir;
  dir.users[70101] = {"nsfsuser", 70101};
  dir.groups["nsfsuser"] = {70101, 70010};
  dir.install(db->nsswitch_cache());
  db->nsswitch_cache().override_dynamic_groups(false);

  const auto id = listless_identity("user$grouped", 70101, 70101);
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_TRUE(cred.groups.empty());
  EXPECT_EQ(dir.group_calls, 0) << "the switch off asked nsswitch";

  db->nsswitch_cache().override_dynamic_groups(true);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{70101, 70010}))
      << "control:  the switch on gave no groups";
}

/* Both forms:  an nsswitch-resolved row's groups come by its name and
 * resolved gid, a local row's by its uid.  Each is its own user's. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsForBothForms)
{
  FakeDirectory dir;
  dir.names["byname"] = {70102, 100};
  dir.users[70101] = {"bylocal", 70101};
  dir.groups["byname"] = {100, 70020};
  dir.groups["bylocal"] = {70101, 70010};
  dir.install(db->nsswitch_cache());
  db->nsswitch_cache().override_dynamic_groups(true);

  ASSERT_EQ(db->put_identity(env->dpp, name_identity("user$n", "byname")), 0);
  ASSERT_EQ(db->put_identity(env->dpp,
			     listless_identity("user$l", 70101, 70101)), 0);
  nsfs::Credentials n, l;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner("user$n"),
				      n, null_yield), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner("user$l"),
				      l, null_yield), 0);
  EXPECT_EQ(n.groups, (std::vector<gid_t>{100, 70020}));
  EXPECT_EQ(l.groups, (std::vector<gid_t>{70101, 70010}));
}

/* Under the switch a list-less local identity whose uid nsswitch does
 * not know is refused, as NooBaa's request is (`fs_napi.cpp:1508-1515`).
 * The control is the switch off, which serves it with no groups. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsUnknownUidIsRefused)
{
  FakeDirectory dir;
  dir.install(db->nsswitch_cache());
  const auto id = listless_identity("user$nouser", 70199, 70199);
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  nsfs::Credentials cred;

  db->nsswitch_cache().override_dynamic_groups(false);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0)
      << "control:  the switch off refused";

  db->nsswitch_cache().override_dynamic_groups(true);
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), -EPERM);
}

/* A group lookup that fails is refused and not cached:  the control is
 * the next call after it recovers, which resolves. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsLookupFailureIsRefusedAndNotCached)
{
  FakeDirectory dir;
  dir.users[70101] = {"nsfsuser", 70101};
  dir.groups["nsfsuser"] = {70101, 70010};
  dir.group_fail["nsfsuser"] = -EIO;
  dir.install(db->nsswitch_cache());
  db->nsswitch_cache().override_dynamic_groups(true);
  const auto id = listless_identity("user$grouped", 70101, 70101);
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);

  nsfs::Credentials cred;
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), -EPERM);
  dir.group_fail.clear();
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0)
      << "a failed group lookup was cached";
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{70101, 70010}));
}

/* A membership change reaches requests through the sweep:  a changed
 * list moves the identity generation, for a local identity and an
 * nsswitch-resolved one alike.  The control is a sweep over unchanged
 * groups, which moves nothing. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsSweepInvalidatesOnChange)
{
  FakeDirectory dir;
  dir.names["byname"] = {70102, 100};
  dir.users[70101] = {"bylocal", 70101};
  dir.groups["byname"] = {100, 70010};
  dir.groups["bylocal"] = {70101, 70010};
  dir.install(db->nsswitch_cache());
  db->nsswitch_cache().override_dynamic_groups(true);
  ASSERT_EQ(db->put_identity(env->dpp, name_identity("user$n", "byname")), 0);
  ASSERT_EQ(db->put_identity(env->dpp,
			     listless_identity("user$l", 70101, 70101)), 0);
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner("user$n"),
				      cred, null_yield), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner("user$l"),
				      cred, null_yield), 0);

  const rgw_owner owner = parse_owner("user$l");
  uint64_t gen = nsfs::identity_generation(owner);
  EXPECT_EQ(db->nsswitch_cache().sweep(env->dpp), 0);
  EXPECT_EQ(nsfs::identity_generation(owner), gen)
      << "control:  unchanged groups invalidated";

  dir.groups["bylocal"] = {70101};		/* gpasswd -d */
  EXPECT_EQ(db->nsswitch_cache().sweep(env->dpp), 1);
  EXPECT_NE(nsfs::identity_generation(owner), gen);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred,
				      null_yield), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{70101}));

  gen = nsfs::identity_generation(owner);
  dir.groups["byname"] = {100};
  EXPECT_EQ(db->nsswitch_cache().sweep(env->dpp), 1);
  EXPECT_NE(nsfs::identity_generation(owner), gen);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner("user$n"),
				      cred, null_yield), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{100}));
}

/* A local row written clears its uid's entry, so a rewrite looks the
 * groups up again.  The control is a write to an unrelated row. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsRowWritesClearTheirUid)
{
  FakeDirectory dir;
  dir.users[70101] = {"bylocal", 70101};
  dir.groups["bylocal"] = {70101, 70010};
  dir.install(db->nsswitch_cache());
  db->nsswitch_cache().override_dynamic_groups(true);
  const auto id = listless_identity("user$l", 70101, 70101);
  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);

  dir.groups["bylocal"] = {70101};
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$unrelated")), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{70101, 70010}))
      << "control:  an unrelated write cleared it";

  ASSERT_EQ(db->put_identity(env->dpp, id), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, parse_owner(id.key),
				      cred, null_yield), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{70101}))
      << "the row rewritten was not looked up again";
}

/* The driver hint flips the switch for the suite's dynamic-groups
 * profile, and invalidates every identity, since what a list-less row
 * means changes.  The control is `default`, which returns to the
 * option's value, off here. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsOverrideInvalidates)
{
  const rgw_owner owner = parse_owner("user$any");
  db->nsswitch_cache().override_dynamic_groups(std::nullopt);
  EXPECT_FALSE(db->nsswitch_cache().dynamic_groups());
  const uint64_t gen = nsfs::identity_generation(owner);
  db->nsswitch_cache().override_dynamic_groups(true);
  EXPECT_TRUE(db->nsswitch_cache().dynamic_groups());
  EXPECT_NE(nsfs::identity_generation(owner), gen);
  db->nsswitch_cache().override_dynamic_groups(std::nullopt);
  EXPECT_FALSE(db->nsswitch_cache().dynamic_groups())
      << "control:  default did not return to the option";
}

/* The sweep's first pass under the switch resolves every list-less
 * local row's uid as well as every name, so a uid nsswitch does not know
 * is logged at startup.  The control:  with the switch off, no uid is
 * looked up. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsSweepStartsWithTheTablesUids)
{
  FakeDirectory dir;
  dir.users[70101] = {"bylocal", 70101};
  dir.install(db->nsswitch_cache());
  ASSERT_EQ(db->put_identity(env->dpp,
			     listless_identity("user$l", 70101, 70101)), 0);
  ASSERT_EQ(db->put_identity(env->dpp,
			     listless_identity("user$x", 70199, 70199)), 0);
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$listed")), 0);

  db->nsswitch_cache().override_dynamic_groups(false);
  EXPECT_TRUE(db->sweep_table(env->dpp).uids.empty())
      << "control:  the switch off listed uids";

  db->nsswitch_cache().override_dynamic_groups(true);
  auto uids = db->sweep_table(env->dpp).uids;
  std::sort(uids.begin(), uids.end());
  EXPECT_EQ(uids, (std::vector<uint32_t>{70101, 70199}))
      << "a row with its own list, or none, was listed";
}

/* The real lookups, on whatever source this host has.  root is in every
 * `files` source, in its own group;  the control is a uid no source has. */
TEST_F(NSFSIdentityDBTest, NsswitchRealGroups)
{
  std::string name;
  uint32_t gid = 1;
  ASSERT_EQ(nsfs::nsswitch_user(0, name, gid), 0);
  EXPECT_EQ(name, "root");
  EXPECT_EQ(gid, 0u);
  std::vector<uint32_t> groups;
  ASSERT_EQ(nsfs::nsswitch_groups(name, gid, groups), 0);
  EXPECT_NE(std::find(groups.begin(), groups.end(), 0u), groups.end())
      << "getgrouplist left out the primary gid";
  EXPECT_EQ(nsfs::nsswitch_user(3999999999u, name, gid), -ENOENT);
}

/* Credentials built from a file, under the switch (decision 4):  (1) a
 * list-less row completes with its nsswitch groups;  (2) rows disagree
 * when what each would install differs;  (3) a failed lookup is
 * -ENODATA;  and a uid no row carries completes with its Unix groups,
 * -ENOENT when nsswitch does not know it either.  Each with its control:
 * the switch off. */
TEST_F(NSFSIdentityDBTest, NsswitchGroupsCompleteFileCredentials)
{
  FakeDirectory dir;
  dir.users[70101] = {"bylocal", 70101};
  dir.users[70103] = {"norow", 70103};
  dir.groups["bylocal"] = {70101, 70010};
  dir.groups["norow"] = {70103, 70030};
  dir.install(db->nsswitch_cache());
  ASSERT_EQ(db->put_identity(env->dpp,
			     listless_identity("user$l", 70101, 70101)), 0);
  auto file = [] (uint32_t uid) {
    nsfs::Credentials c;
    c.uid = uid;
    c.gid = uid;
    return c;
  };

  /* (1), and its control */
  db->nsswitch_cache().override_dynamic_groups(false);
  auto c = file(70101);
  ASSERT_EQ(nsfs::complete_file_credentials(env->dpp, *db, c), 0);
  EXPECT_TRUE(c.groups.empty()) << "control:  the switch off gave groups";
  db->nsswitch_cache().override_dynamic_groups(true);
  c = file(70101);
  std::string key;
  ASSERT_EQ(nsfs::complete_file_credentials(env->dpp, *db, c, &key), 0);
  EXPECT_EQ(c.groups, (std::vector<gid_t>{70101, 70010}));
  EXPECT_EQ(key, "user$l");

  /* a uid no row carries:  its Unix groups, and no key;  control off */
  c = file(70103);
  key = "untouched";
  ASSERT_EQ(nsfs::complete_file_credentials(env->dpp, *db, c, &key), 0);
  EXPECT_EQ(c.groups, (std::vector<gid_t>{70103, 70030}));
  EXPECT_EQ(key, "untouched");
  db->nsswitch_cache().override_dynamic_groups(false);
  c = file(70103);
  EXPECT_EQ(nsfs::complete_file_credentials(env->dpp, *db, c), -ENOENT)
      << "control:  the switch off completed a uid no row carries";
  db->nsswitch_cache().override_dynamic_groups(true);
  c = file(70199);
  EXPECT_EQ(nsfs::complete_file_credentials(env->dpp, *db, c), -ENOENT)
      << "a uid neither a row nor nsswitch knows completed";

  /* (3) */
  dir.group_fail["bylocal"] = -EIO;
  db->nsswitch_cache().forget_uid(70101);
  c = file(70101);
  EXPECT_EQ(nsfs::complete_file_credentials(env->dpp, *db, c), -ENODATA);
  dir.group_fail.clear();

  /* (2):  a second row on the uid with its own list, which differs */
  auto listed = listless_identity("user$l2", 70101, 70101);
  listed.groups = std::vector<uint32_t>{7};
  ASSERT_EQ(db->put_identity(env->dpp, listed), 0);
  c = file(70101);
  EXPECT_EQ(nsfs::complete_file_credentials(env->dpp, *db, c), -ENODATA)
      << "an explicit list and nsswitch's were taken to agree";
  listed.groups = std::vector<uint32_t>{70101, 70010};
  ASSERT_EQ(db->put_identity(env->dpp, listed), 0);
  c = file(70101);
  ASSERT_EQ(nsfs::complete_file_credentials(env->dpp, *db, c), 0)
      << "control:  rows that install the same groups disagreed";
  EXPECT_EQ(c.groups, (std::vector<gid_t>{70101, 70010}));
}

/* Completing a credential built from a file:  the groups come from
 * the row carrying the uid, and the uid and gid do not.  The file's
 * are authoritative for the file -- a member writing in a directory
 * that is not SGID creates files with its own primary gid, which the
 * account's row does not know. */
TEST_F(NSFSIdentityDBTest, CompleteFileCredentialsTakesTheRowsGroups)
{
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$fileowner")), 0);

  nsfs::Credentials cred;
  cred.uid = 1001;			/* as a stat of the file yields */
  cred.gid = 4242;			/* not the row's 2002 */
  std::string key;
  ASSERT_EQ(nsfs::complete_file_credentials(env->dpp, *db, cred, &key), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{10, 20, 30}));
  EXPECT_EQ(cred.uid, 1001u);
  EXPECT_EQ(cred.gid, 4242u) << "the row's gid displaced the file's";
  EXPECT_EQ(key, "user$fileowner")
      << "one row names an identity, which is what makes an FSIdentity "
	 "active";
}

/* Several rows carrying one uid is the normal case, not an error:
 * every member of an account runs as the account.  Where they agree
 * there is no ambiguity in fact. */
TEST_F(NSFSIdentityDBTest, CompleteFileCredentialsAgreeingRowsAreNotAmbiguous)
{
  auto a = local_identity("user$member1");
  auto b = local_identity("user$member2");
  ASSERT_EQ(a.uid, b.uid);
  ASSERT_EQ(db->put_identity(env->dpp, a), 0);
  ASSERT_EQ(db->put_identity(env->dpp, b), 0);

  nsfs::Credentials cred;
  cred.uid = *a.uid;
  std::string key = "untouched";
  ASSERT_EQ(nsfs::complete_file_credentials(env->dpp, *db, cred, &key), 0);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{10, 20, 30}));
  EXPECT_EQ(key, "untouched")
      << "two identities whose credentials coincide have no one key";
}

/* The polarity that matters.  Rows that disagree cannot be chosen
 * between -- a file names its owner by uid alone -- and guessing
 * would lend one identity's reach to another identity's file.  The
 * caller is left with the file owner's own rights. */
TEST_F(NSFSIdentityDBTest, CompleteFileCredentialsFailsOnDisagreeingRows)
{
  auto a = local_identity("user$member1");
  auto b = local_identity("user$member2");
  b.groups = std::vector<uint32_t>{10, 20, 30, 40};
  ASSERT_EQ(db->put_identity(env->dpp, a), 0);
  ASSERT_EQ(db->put_identity(env->dpp, b), 0);

  nsfs::Credentials cred;
  cred.uid = *a.uid;
  cred.groups = std::vector<gid_t>{7};	/* must not be written to */
  EXPECT_EQ(nsfs::complete_file_credentials(env->dpp, *db, cred),
	    -ENODATA);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{7}))
      << "a failed completion wrote to its output";
}

/* No row carrying the uid is not an error:  a uid no account maps to
 * is ordinary in a sideloaded tree. */
TEST_F(NSFSIdentityDBTest, CompleteFileCredentialsNoRowIsNotAnError)
{
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$someone")), 0);

  nsfs::Credentials cred;
  cred.uid = 65534;			/* no row carries it */
  cred.groups = std::vector<gid_t>{7};
  EXPECT_EQ(nsfs::complete_file_credentials(env->dpp, *db, cred), -ENOENT);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{7}));
}

/* A row carrying only placement data asks for no impersonation, and
 * has no uid to be found by. */
TEST_F(NSFSIdentityDBTest, CompleteFileCredentialsIgnoresRowsWithoutIds)
{
  nsfs::Identity placement;
  placement.key = "user$placementonly";
  placement.new_buckets_path = "/gpfs/rgw1/nsfs/newbuckets";
  ASSERT_EQ(db->put_identity(env->dpp, placement), 0);

  std::vector<nsfs::Identity> rows;
  ASSERT_EQ(db->get_identities_by_uid(env->dpp, 0, rows), 0);
  EXPECT_TRUE(rows.empty()) << "a NULL Uid was matched by a uid lookup";

  nsfs::Credentials cred;
  cred.uid = 0;
  EXPECT_EQ(nsfs::complete_file_credentials(env->dpp, *db, cred), -ENOENT);
}

/* A test's own raise, on a thread of its own.  Under impersonation a
 * raise leaves its thread holding CAP_DAC_READ_SEARCH, and every
 * thread started from it afterwards inherits that, which spoils the
 * fresh-thread controls of every case after it. */
static int with_capabilities_elsewhere(uint64_t caps,
				       const nsfs::ImpersonatedFn& fn)
{
  int r = 0;
  std::thread t([&] { r = nsfs::with_capabilities(env->dpp, caps, fn); });
  t.join();
  return r;
}

/* The raise primitive.  The rejections need a process not permitted
 * the capability, which the unit test is unless it was given the
 * gateway's file capabilities.  The other polarity, a raise that works and is
 * dropped again, needs the file capabilities and is the `raise`
 * driver hint's job on a real gateway.
 *
 * The effective set is read around every case:  a primitive that
 * leaves a capability behind is the failure that matters, and it
 * would not show up in a return value. */
class NSFSRaiseTest : public ::testing::Test {
protected:
  static uint64_t effective_set() {
    struct __user_cap_header_struct hdr = {};
    struct __user_cap_data_struct data[2] = {};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;
    if (::syscall(SYS_capget, &hdr, data) < 0) {
      return ~0ull;			/* unreadable, not empty */
    }
    return (static_cast<uint64_t>(data[1].effective) << 32) |
	   data[0].effective;
  }

  /* The rejections need the capability absent from the permitted set,
   * not merely from the effective one:  nothing starts effective, so
   * a binary given the gateway's file capabilities raises it and the
   * rejection cannot happen.  Such a binary skips them. */
  static bool permitted(int cap) {
    struct __user_cap_header_struct hdr = {};
    struct __user_cap_data_struct data[2] = {};
    hdr.version = _LINUX_CAPABILITY_VERSION_3;
    hdr.pid = 0;
    if (::syscall(SYS_capget, &hdr, data) < 0) {
      return true;			/* unreadable:  assume the worst */
    }
    return data[cap / 32].permitted & (1u << (cap % 32));
  }
};

/* An empty mask is not a raise:  it runs the call and touches
 * nothing. */
TEST_F(NSFSRaiseTest, AnEmptyMaskRunsTheCall)
{
  const uint64_t before = effective_set();
  bool ran = false;
  EXPECT_EQ(nsfs::with_capabilities(env->dpp, 0,
				    [&ran]() -> int { ran = true; return 7; }),
	    7);
  EXPECT_TRUE(ran);
  EXPECT_EQ(effective_set(), before);
}

/* A capability that is not permitted cannot be raised, and the call
 * does not run.  Fails closed:  running it unprivileged would report
 * whatever the kernel said about an ordinary call, which reads as a
 * permission bug rather than as a gateway started without its
 * capabilities. */
TEST_F(NSFSRaiseTest, AnUnpermittedCapabilityIsRejectedAndTheCallDoesNotRun)
{
  if (permitted(CAP_DAC_OVERRIDE)) {
    GTEST_SKIP() << "CAP_DAC_OVERRIDE is permitted, so the rejection "
		    "cannot happen";
  }
  const uint64_t before = effective_set();
  ASSERT_NE(before, ~0ull) << "the effective set could not be read";

  bool ran = false;
  EXPECT_EQ(nsfs::with_capabilities(env->dpp,
				    nsfs::cap_bit(CAP_DAC_OVERRIDE),
				    [&ran]() -> int { ran = true; return 0; }),
	    -EPERM);
  EXPECT_FALSE(ran) << "the call ran without the capability it asked for";
  EXPECT_EQ(effective_set(), before) << "a rejected raise changed the set";
}

/* Several at once are rejected on the first that is absent, and the
 * mask the operator is shown names what was missing rather than what
 * was asked for. */
TEST_F(NSFSRaiseTest, SeveralUnpermittedCapabilitiesAreRejectedTogether)
{
  if (permitted(CAP_DAC_OVERRIDE) && permitted(CAP_FOWNER)) {
    GTEST_SKIP() << "CAP_DAC_OVERRIDE and CAP_FOWNER are permitted, so "
		    "the rejection cannot happen";
  }
  const uint64_t before = effective_set();
  bool ran = false;
  EXPECT_EQ(nsfs::with_capabilities(
	      env->dpp,
	      nsfs::cap_bit(CAP_DAC_OVERRIDE) | nsfs::cap_bit(CAP_FOWNER),
	      [&ran]() -> int { ran = true; return 0; }),
	    -EPERM);
  EXPECT_FALSE(ran);
  EXPECT_EQ(effective_set(), before);
}

/* The upstream behaviour this design keys on, pinned.
 *
 * Two different owners come out of one applier.  `get_aclowner()`
 * says who a created object belongs to, and for a member of an
 * account that is the *account*;  `load_acct_info()` -- what becomes
 * `s->user` -- says who authenticated, and that is the *member*.
 * authenticated_user(), and so get_credentials_for(), uses the second.
 *
 * This is not nsfs code, so it is exactly the kind of thing that can
 * change under us.  If `load_acct_info()` ever starts returning the
 * account, every member of an account is silently served as one uid
 * and gid, and nothing else in the suite would notice. */
TEST_F(NSFSIdentityDBTest, AclOwnerIsTheAccountButAuthenticatedUserIsNot)
{
  const rgw_user uid{"", "alice"};
  const char* acct = "RGW12345678901234567";

  /* the control:  with no account the two agree, so the second half
   * below cannot pass merely because they always differ */
  auto plain = local_applier(uid, nullptr);
  EXPECT_EQ(plain->get_aclowner().id, rgw_owner{uid});
  EXPECT_EQ(plain->load_acct_info(env->dpp)->get_id(), uid);

  auto member = local_applier(uid, acct);
  EXPECT_EQ(member->get_aclowner().id, rgw_owner{rgw_account_id{acct}})
      << "the ACL owner should be the account";
  EXPECT_EQ(member->load_acct_info(env->dpp)->get_id(), uid)
      << "s->user collapsed onto the account;  per-member POSIX "
	 "identity is no longer expressible";
}

/* A row keyed on a member takes precedence over its account's.
 *
 * On Scale a member ordinarily has no row and runs as its account
 * (ACCOUNT_IMPORT.md §2.8);  a per-user row is what keeps a distinct
 * identity expressible.  It cannot be expressed through
 * get_aclowner() at all:  both members share an ACL owner, so keyed
 * on it they would be indistinguishable. */
TEST_F(NSFSIdentityDBTest, AccountMembersResolveIndependently)
{
  /* I9:  a member's own row is consulted only under the override */
  db->force_user_identity_override(true);
  const char* acct = "RGW12345678901234567";
  const rgw_user alice{"", "alice"};
  const rgw_user bob{"", "bob"};

  nsfs::Identity a;
  a.key = alice.to_str();
  a.uid = 1001; a.gid = 1001;
  a.groups = std::vector<uint32_t>{50};
  ASSERT_EQ(db->put_identity(env->dpp, a), 0);

  nsfs::Identity b;
  b.key = bob.to_str();
  b.uid = 1002; b.gid = 1002;
  ASSERT_EQ(db->put_identity(env->dpp, b), 0);

  /* and a row under the account itself, which must not be what
   * either member gets -- without it, "they differ" could be
   * satisfied by one of them simply failing */
  nsfs::Identity acct_row;
  acct_row.key = acct;
  acct_row.uid = 9999; acct_row.gid = 9999;
  ASSERT_EQ(db->put_identity(env->dpp, acct_row), 0);

  auto ia = local_applier(alice, acct);
  auto ib = local_applier(bob, acct);
  ASSERT_EQ(ia->get_aclowner().id, ib->get_aclowner().id)
      << "the fixture is wrong:  these should share an ACL owner";

  /* the key the driver uses:  s->user, which is what
   * load_acct_info() returns.  Called once per applier -- for
   * LocalApplier it releases the held user. */
  nsfs::Credentials ca, cb;
  std::string ra, rb;
  auto ua = ia->load_acct_info(env->dpp);
  auto ub = ib->load_acct_info(env->dpp);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, ua->get_id(),
				      ua->get_info().account_id, ca, null_yield, &ra), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, ub->get_id(),
				      ub->get_info().account_id, cb, null_yield, &rb), 0);
  EXPECT_EQ(ra, alice.to_str());
  EXPECT_EQ(rb, bob.to_str());

  EXPECT_EQ(ca.uid, 1001u);
  EXPECT_EQ(cb.uid, 1002u);
  EXPECT_NE(ca.uid, 9999u) << "a member was served as its account";
  EXPECT_NE(cb.uid, 9999u) << "a member was served as its account";
  EXPECT_EQ(ca.groups, (std::vector<gid_t>{50}));
  EXPECT_TRUE(cb.groups.empty());

  /* the control:  without the override both run as the account, their
   * rows kept but not consulted */
  db->force_user_identity_override(false);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, ua->get_id(),
				      ua->get_info().account_id, ca, null_yield, &ra), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, ub->get_id(),
				      ub->get_info().account_id, cb, null_yield, &rb), 0);
  EXPECT_EQ(ra, acct);
  EXPECT_EQ(rb, acct);
  EXPECT_EQ(ca.uid, 9999u);
  EXPECT_EQ(cb.uid, 9999u);
}

/* A member with no row of its own runs as its account.
 *
 * The Scale model (ACCOUNT_IMPORT.md §2.8):  an account has one POSIX
 * identity, and the import writes a row for the account and none for
 * its members. */
TEST_F(NSFSIdentityDBTest, AMemberWithNoRowRunsAsItsAccount)
{
  const char* acct = "RGW12345678901234567";
  auto member = local_applier(rgw_user{"", "carol"}, acct);
  auto u = member->load_acct_info(env->dpp);
  ASSERT_EQ(u->get_info().account_id, acct)
      << "the fixture is wrong:  carol should be a member";

  /* the control:  with no account row there is nothing to fall back
   * to, so the success below is the fallback and not something else */
  nsfs::Credentials cred;
  std::string row = "untouched";
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, u->get_id(),
				      u->get_info().account_id, cred, null_yield, &row),
	    -ENOENT);
  EXPECT_EQ(row, "untouched") << "-ENOENT named a row";

  nsfs::Identity acct_row;
  acct_row.key = acct;
  acct_row.uid = 9999; acct_row.gid = 9999;
  acct_row.groups = std::vector<uint32_t>{70};
  ASSERT_EQ(db->put_identity(env->dpp, acct_row), 0);

  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, u->get_id(),
				      u->get_info().account_id, cred, null_yield, &row), 0)
      << "a member with no row was not served as its account";
  EXPECT_EQ(row, acct) << "the fallback did not name the account's row";
  EXPECT_EQ(cred.uid, 9999u);
  EXPECT_EQ(cred.gid, 9999u);
  EXPECT_EQ(cred.groups, (std::vector<gid_t>{70}));

  /* and the owner form does not fall back:  it reads one row, which
   * is what the credentials diagnostic's `identity=` reports */
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, rgw_owner{u->get_id()},
				      cred, null_yield), -ENOENT);
}

/* A user outside any account has nothing to fall back to. */
TEST_F(NSFSIdentityDBTest, AUserOutsideAnAccountResolvesUnderItsOwnKey)
{
  const rgw_user dan{"", "dan"};
  nsfs::Credentials cred;
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, dan, rgw_account_id{},
				      cred, null_yield), -ENOENT);

  nsfs::Identity row;
  row.key = dan.to_str();
  row.uid = 1234; row.gid = 1234;
  ASSERT_EQ(db->put_identity(env->dpp, row), 0);
  /* the I9 override governs account users only */
  for (bool on : {false, true}) {
    db->force_user_identity_override(on);
    ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, dan, rgw_account_id{},
					cred, null_yield), 0) << on;
    EXPECT_EQ(cred.uid, 1234u) << on;
  }
}

/* A member row that asks for an identity it cannot be given is
 * denied, not replaced by its account's.
 *
 * -EPERM is a directory-backed row whose directory is not consulted;
 * serving the account instead would run the request as someone the
 * row did not name. */
TEST_F(NSFSIdentityDBTest, ADeniedMemberRowDoesNotFallBack)
{
  db->force_user_identity_override(true);
  const char* acct = "RGW12345678901234567";
  const rgw_user erin{"", "erin"};

  nsfs::Identity acct_row;
  acct_row.key = acct;
  acct_row.uid = 9999; acct_row.gid = 9999;
  ASSERT_EQ(db->put_identity(env->dpp, acct_row), 0);

  /* the control:  with no row of her own, erin runs as the account */
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, erin,
				      rgw_account_id{acct}, cred, null_yield), 0);
  ASSERT_EQ(cred.uid, 9999u);

  nsfs::Identity dir;
  dir.key = erin.to_str();
  dir.distinguished_name = "erin";
  ASSERT_EQ(db->put_identity(env->dpp, dir), 0);
  std::string row;
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, erin,
				      rgw_account_id{acct}, cred, null_yield, &row), -EPERM)
      << "a denied row fell through to the account";
  EXPECT_EQ(row, erin.to_str()) << "the denial named another row";

  /* without the override erin's row is not consulted at all, so it
   * cannot deny her:  she runs as the account */
  db->force_user_identity_override(false);
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, erin,
				      rgw_account_id{acct}, cred, null_yield, &row), 0);
  EXPECT_EQ(row, acct);
  EXPECT_EQ(cred.uid, 9999u);
}

/* A role's identity row (I9) is keyed apart from every owner's, and is
 * stored and read like any other row.  No owner's key begins `role:`,
 * since to_string(rgw_owner) is an account id or a user id. */
TEST_F(NSFSIdentityDBTest, ARoleRowIsKeyedApartFromOwners)
{
  const std::string key = nsfs::role_identity_key("4c7e5a1d-role-id");
  EXPECT_EQ(key, "role:4c7e5a1d-role-id");
  EXPECT_TRUE(nsfs::is_role_identity_key(key));
  EXPECT_FALSE(nsfs::is_role_identity_key(
		 to_string(rgw_owner{rgw_account_id{"RGW12345678901234567"}})));
  EXPECT_FALSE(nsfs::is_role_identity_key(
		 to_string(rgw_owner{rgw_user{"", "alice"}})));

  nsfs::Identity row;
  row.key = key;
  row.uid = 70109; row.gid = 70109;
  ASSERT_EQ(db->put_identity(env->dpp, row), 0);
  nsfs::Identity got;
  ASSERT_EQ(db->get_identity(env->dpp, key, got), 0);
  EXPECT_EQ(got.uid, 70109u);
  /* and resolving an owner never reaches it */
  nsfs::Credentials cred;
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, rgw_user{"", "4c7e5a1d-role-id"},
				      rgw_account_id{}, cred, null_yield), -ENOENT);
}

/* A role session's credentials (I9):  the role's own row under the role
 * override, when it was given one, else its account's -- and its
 * account's always without the override.  Each case is the others'
 * control. */
TEST_F(NSFSIdentityDBTest, ARoleSessionRunsAsItsRowOnlyUnderTheOverride)
{
  const rgw_account_id acct{"RGW13131313131313131"};
  const std::string role_id = "1f2e3d4c-role";
  nsfs::Identity arow;
  arow.key = acct;
  arow.uid = 70109; arow.gid = 70109;
  ASSERT_EQ(db->put_identity(env->dpp, arow), 0);
  nsfs::Identity rrow;
  rrow.key = nsfs::role_identity_key(role_id);
  rrow.uid = 70110; rrow.gid = 70110;
  ASSERT_EQ(db->put_identity(env->dpp, rrow), 0);

  nsfs::Credentials cred;
  std::string row;
  db->force_role_identity_override(false);
  ASSERT_EQ(nsfs::resolve_role_credentials(env->dpp, *db, role_id, acct, cred,
					   null_yield, &row), 0);
  EXPECT_EQ(cred.uid, 70109u) << "without the override the role was not its account";
  EXPECT_EQ(row, std::string(acct));

  db->force_role_identity_override(true);
  ASSERT_EQ(nsfs::resolve_role_credentials(env->dpp, *db, role_id, acct, cred,
					   null_yield, &row), 0);
  EXPECT_EQ(cred.uid, 70110u) << "under the override the role's row was not used";
  EXPECT_EQ(row, rrow.key);

  ASSERT_EQ(db->remove_identity(env->dpp, rrow.key), 0);
  ASSERT_EQ(nsfs::resolve_role_credentials(env->dpp, *db, role_id, acct, cred,
					   null_yield, &row), 0);
  EXPECT_EQ(cred.uid, 70109u)
      << "a role given no identity did not follow its account";

  /* and a role of no account, given no identity, has nothing */
  EXPECT_EQ(nsfs::resolve_role_credentials(env->dpp, *db, role_id,
					   rgw_account_id{}, cred, null_yield),
	    -ENOENT);
}

/* Changing an account's row reaches every member's cached
 * personality.
 *
 * Personalities are cached under the user a request authenticated
 * as, so a member that resolved through its account's row is cached
 * under the member's key.  Bumping the account key's own shard does
 * not reach it -- the first half shows that, and is why the second
 * mechanism exists. */
TEST_F(NSFSIdentityDBTest, AnAccountRowChangeReachesItsMembers)
{
  const rgw_user acct_key{"RGW12345678901234567"};
  const rgw_user alice{"", "alice"};
  const rgw_user bob{"", "bob"};

  const uint64_t a0 = nsfs::identity_generation(alice);
  const uint64_t b0 = nsfs::identity_generation(bob);
  nsfs::note_identity_changed(acct_key);
  ASSERT_EQ(nsfs::identity_generation(alice), a0)
      << "these keys share a shard;  pick names that do not";
  ASSERT_EQ(nsfs::identity_generation(bob), b0)
      << "these keys share a shard;  pick names that do not";

  nsfs::note_all_identities_changed();
  EXPECT_NE(nsfs::identity_generation(alice), a0)
      << "a member cached through its account keeps the old identity";
  EXPECT_NE(nsfs::identity_generation(bob), b0);
}

/* The personality table.
 *
 * Tested against a counting registrar rather than io_uring:  the
 * parts worth testing here are eviction order, pinning and the
 * all-pinned case, none of which involve a ring, and all of which
 * would otherwise need root to reach. */
class CountingRegistrar : public nsfs::PersonalityTable::Registrar {
public:
  int next_id{1};
  int fail_with{0};			/* non-zero: fail with this */
  std::vector<int> registered;		/* ids handed out, in order */
  std::vector<int> released;		/* ids taken back, in order */
  std::vector<nsfs::Credentials> creds;	/* what each registration saw */

  int register_personality(const DoutPrefixProvider*,
			   const nsfs::Credentials& cred) override {
    if (fail_with) {
      return fail_with;
    }
    creds.push_back(cred);
    registered.push_back(next_id);
    return next_id++;
  }
  void unregister_personality(const DoutPrefixProvider*,
			      uint16_t id) override {
    released.push_back(id);
  }
  bool live(int id) const {
    return std::count(registered.begin(), registered.end(), id) >
	   std::count(released.begin(), released.end(), id);
  }
};

static nsfs::Credentials creds_for(uid_t uid)
{
  nsfs::Credentials c;
  c.uid = uid;
  c.gid = uid;
  c.groups = {uid + 1000};
  return c;
}

/* A generation source a test drives by hand.
 *
 * The production one is a sharded atomic array bumped by the Admin
 * Ops identity writes;  what the table needs from it is only that a
 * changed identity returns something new. */
class ManualGenerations : public nsfs::GenerationSource {
public:
  std::map<std::string, uint64_t> gens;
  uint64_t generation(const rgw_owner& key) const override {
    auto it = gens.find(to_string(key));
    return it == gens.end() ? 0 : it->second;
  }
  void bump(const rgw_owner& key) { ++gens[to_string(key)]; }
};

class NSFSPersonalityTest : public ::testing::Test {
protected:
  CountingRegistrar reg;
};

/* A miss, then a hit, and the registrar is consulted exactly once. */
TEST_F(NSFSPersonalityTest, MissRegistersAndHitDoesNot)
{
  nsfs::PersonalityTable t{4, &reg};
  const rgw_user alice{"", "alice"};

  nsfs::PersonalityRef ref;
  ASSERT_EQ(t.find(alice, &ref), -ENOENT) << "empty table reported a hit";
  ASSERT_FALSE(ref.valid());

  ASSERT_EQ(t.insert(env->dpp, alice, creds_for(1001), &ref), 0);
  ASSERT_TRUE(ref.valid());
  const uint16_t id = ref.id();
  EXPECT_EQ(reg.registered.size(), 1u);
  ASSERT_EQ(reg.creds.size(), 1u);
  EXPECT_EQ(reg.creds[0].uid, 1001u) << "the registrar saw other credentials";

  nsfs::PersonalityRef second;
  ASSERT_EQ(t.find(alice, &second), 0) << "a registered identity missed";
  EXPECT_EQ(second.id(), id);
  EXPECT_EQ(reg.registered.size(), 1u)
      << "a hit registered a second personality";

  EXPECT_EQ(t.get_stats().hits, 1u);
  EXPECT_EQ(t.get_stats().misses, 1u);
  EXPECT_EQ(t.get_stats().registrations, 1u);
}

/* A hit carries the credentials, so the caller need not resolve them.
 *
 * The whole point of splitting find() from insert():  resolving an
 * identity is a database read on the S3 path, and a personality
 * already registered on this ring has had that read done once
 * already.  The thread-pool fallback assumes the identity itself, so
 * the id alone would not be enough -- a caller skipping resolution
 * would hand it a default-constructed Credentials and it would try
 * to become uid 0.
 *
 * The negative case is reachable:  before the slot kept them,
 * credentials() had nothing to return and this test could not have
 * been written. */
TEST_F(NSFSPersonalityTest, AHitCarriesTheCredentials)
{
  nsfs::PersonalityTable t{4, &reg};
  const rgw_user alice{"", "alice"};

  nsfs::PersonalityRef ref;
  ASSERT_EQ(t.insert(env->dpp, alice, creds_for(1001), &ref), 0);
  EXPECT_EQ(ref.credentials().uid, 1001u);
  EXPECT_EQ(ref.credentials().gid, 1001u);
  ASSERT_EQ(ref.credentials().groups.size(), 1u);
  EXPECT_EQ(ref.credentials().groups[0], 2001u);
  ref.reset();

  /* the hit, which is the case that matters:  no registrar call, and
   * the credentials still available */
  nsfs::PersonalityRef hit;
  ASSERT_EQ(t.find(alice, &hit), 0) << "a registered identity missed";
  EXPECT_EQ(reg.registered.size(), 1u) << "a hit registered again";
  EXPECT_EQ(hit.credentials().uid, 1001u)
      << "a hit did not carry the credentials it was registered with";
  EXPECT_EQ(hit.credentials().gid, 1001u);
  ASSERT_EQ(hit.credentials().groups.size(), 1u)
      << "the supplementary group vector was not kept";
  EXPECT_EQ(hit.credentials().groups[0], 2001u);
}

/* A reused slot carries the new identity's credentials, not the old.
 *
 * The failure this guards is silent and serious:  a slot recycled by
 * the FIFO cursor that kept its previous credentials would serve one
 * identity's requests as another. */
TEST_F(NSFSPersonalityTest, AReusedSlotDoesNotKeepStaleCredentials)
{
  nsfs::PersonalityTable t{1, &reg};
  const rgw_user alice{"", "alice"};
  const rgw_user bob{"", "bob"};

  {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.insert(env->dpp, alice, creds_for(1001), &r), 0);
    ASSERT_EQ(r.credentials().uid, 1001u);
  }

  /* one slot, so bob evicts alice and takes it */
  nsfs::PersonalityRef r;
  ASSERT_EQ(t.insert(env->dpp, bob, creds_for(2002), &r), 0);
  EXPECT_EQ(t.get_stats().evictions, 1u) << "alice was not evicted";
  EXPECT_EQ(r.credentials().uid, 2002u)
      << "the reused slot served bob with alice's credentials";
  ASSERT_EQ(r.credentials().groups.size(), 1u);
  EXPECT_EQ(r.credentials().groups[0], 3002u);

  EXPECT_EQ(t.find(alice, &r), -ENOENT) << "alice survived her eviction";
}

/* A changed identity record stops the old personality being served.
 *
 * The bug this closes:  nothing about a registered slot notices that
 * the row it was minted from has been rewritten, so an Admin Ops
 * change to a uid, gid or group vector was ignored until the FIFO
 * cursor happened to evict the slot -- never, on a table larger than
 * the working set.  Measured in the live suite as a group revocation
 * that did not take effect.
 *
 * Both polarities, and in this order:  the hit must happen without a
 * bump, so that its absence after one is attributable to the bump. */
TEST_F(NSFSPersonalityTest, AChangedIdentityInvalidatesItsPersonality)
{
  ManualGenerations gens;
  nsfs::PersonalityTable t{4, &reg, &gens};
  const rgw_user alice{"", "alice"};

  {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.insert(env->dpp, alice, creds_for(1001), &r), 0);
  }

  /* the control:  unchanged, so a hit */
  {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.find(alice, &r), 0) << "an unchanged identity missed";
    EXPECT_EQ(r.credentials().uid, 1001u);
  }
  ASSERT_EQ(t.get_stats().stale, 0u);

  gens.bump(alice);

  nsfs::PersonalityRef r;
  EXPECT_EQ(t.find(alice, &r), -ENOENT)
      << "a rewritten identity was still served from its old slot";
  EXPECT_EQ(t.get_stats().stale, 1u);
  EXPECT_EQ(reg.released.size(), 1u)
      << "the stale personality was not given back to the ring";

  /* and the next insert registers the new credentials */
  ASSERT_EQ(t.insert(env->dpp, alice, creds_for(2002), &r), 0);
  EXPECT_EQ(r.credentials().uid, 2002u);
  EXPECT_EQ(reg.registered.size(), 2u);
}

/* A stale slot a live operation still holds is not pulled out from
 * under it, and is not given to anyone else.
 *
 * Unregistering a pinned slot would pull a personality an in-flight
 * submission still names.  Serving it to a new lookup would hand a new
 * operation credentials its identity no longer has -- and that
 * operation keeps what it resolved for its whole life.  So the slot
 * leaves the index at once and is unregistered at its last unpin. */
TEST_F(NSFSPersonalityTest, AStaleSlotInUseIsNotPulled)
{
  ManualGenerations gens;
  nsfs::PersonalityTable t{4, &reg, &gens};
  const rgw_user alice{"", "alice"};

  nsfs::PersonalityRef held;
  ASSERT_EQ(t.insert(env->dpp, alice, creds_for(1001), &held), 0);
  gens.bump(alice);

  nsfs::PersonalityRef r;
  EXPECT_EQ(t.find(alice, &r), -ENOENT)
      << "a stale slot was served to a new lookup because it was pinned";
  EXPECT_TRUE(reg.released.empty())
      << "a personality still in use was unregistered";
  EXPECT_EQ(held.credentials().uid, 1001u)
      << "the live operation lost the credentials it was submitting under";
  held.reset();
  EXPECT_EQ(reg.released.size(), 1u)
      << "the stale personality was not given back at its last unpin";
}

/* Credentials resolved before an identity changed are never indexed
 * as current.
 *
 * The defect:  an operation resolves an identity once, and each
 * submission registers from those credentials if its slot is gone.
 * After an identity write, the operation's next submission found the
 * slot stale, dropped it and registered the old credentials -- stamped
 * with the current generation, so every later request was served the
 * old credentials as fresh, until the identity changed again.  A
 * revoked group stayed granted.
 *
 * The operation itself keeps what it resolved;  a new lookup must not
 * see it.  The control is a registration at the current generation,
 * which is indexed and found. */
TEST_F(NSFSPersonalityTest, OldCredentialsAreNotIndexedAsCurrent)
{
  ManualGenerations gens;
  nsfs::PersonalityTable t{4, &reg, &gens};
  const rgw_user alice{"", "alice"};
  const uint64_t g0 = gens.generation(alice);

  /* an operation's first submission */
  {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.acquire(env->dpp, alice, creds_for(1001), g0, &r), 0);
  }
  gens.bump(alice);

  /* its next submission, after the change, under what it resolved */
  {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.acquire(env->dpp, alice, creds_for(1001), g0, &r), 0);
    EXPECT_EQ(r.credentials().uid, 1001u)
        << "the operation's submission ran under other credentials";
  }

  /* a new operation */
  nsfs::PersonalityRef n;
  EXPECT_EQ(t.find(alice, &n), -ENOENT)
      << "credentials resolved before the change were indexed as current";
  n.reset();

  /* the control:  a registration at the current generation is indexed */
  const uint64_t g1 = gens.generation(alice);
  ASSERT_NE(g1, g0);
  {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.acquire(env->dpp, alice, creds_for(2002), g1, &r), 0);
    EXPECT_EQ(r.credentials().uid, 2002u);
  }
  ASSERT_EQ(t.find(alice, &n), 0) << "a current registration was not indexed";
  EXPECT_EQ(n.credentials().uid, 2002u);
  n.reset();
  EXPECT_EQ(reg.registered.size() - reg.released.size(), 1u)
      << "registrations from the old credentials were not given back";
}

/* A new operation gets the current credentials while an older one is
 * still submitting under the stale slot. */
TEST_F(NSFSPersonalityTest, ANewOperationIsNotGivenAPinnedStaleSlot)
{
  ManualGenerations gens;
  nsfs::PersonalityTable t{4, &reg, &gens};
  const rgw_user alice{"", "alice"};
  const uint64_t g0 = gens.generation(alice);

  nsfs::PersonalityRef older;
  ASSERT_EQ(t.acquire(env->dpp, alice, creds_for(1001), g0, &older), 0);
  gens.bump(alice);

  nsfs::PersonalityRef newer;
  ASSERT_EQ(t.acquire(env->dpp, alice, creds_for(2002),
		      gens.generation(alice), &newer), 0);
  EXPECT_EQ(newer.credentials().uid, 2002u)
      << "a new operation was submitted under the old credentials";
  EXPECT_NE(newer.id(), older.id());
  EXPECT_EQ(older.credentials().uid, 1001u);
  EXPECT_TRUE(reg.released.empty())
      << "the older operation's personality was pulled while in flight";
  older.reset();
  EXPECT_EQ(reg.released.size(), 1u)
      << "the stale personality was not given back at its last unpin";
}

/* An operation older than the index keeps its own credentials.
 *
 * Once a newer operation has registered the identity's current
 * credentials, an operation that resolved before the change still
 * submits under what it resolved, through a registration of its own
 * that is never indexed and is given back when the submission ends. */
TEST_F(NSFSPersonalityTest, AnOperationOlderThanTheIndexKeepsItsCredentials)
{
  ManualGenerations gens;
  nsfs::PersonalityTable t{4, &reg, &gens};
  const rgw_user alice{"", "alice"};
  const uint64_t g0 = gens.generation(alice);
  gens.bump(alice);
  const uint64_t g1 = gens.generation(alice);

  {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.acquire(env->dpp, alice, creds_for(2002), g1, &r), 0);
  }
  {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.acquire(env->dpp, alice, creds_for(1001), g0, &r), 0);
    EXPECT_EQ(r.credentials().uid, 1001u)
        << "an operation was switched to credentials it did not resolve";
  }
  nsfs::PersonalityRef n;
  ASSERT_EQ(t.find(alice, &n), 0);
  EXPECT_EQ(n.credentials().uid, 2002u)
      << "the older operation displaced the current registration";
  n.reset();
  EXPECT_EQ(reg.registered.size() - reg.released.size(), 1u)
      << "the older operation's registration was not given back";
}

/* Eviction is by age, and the evicted personality is given back. */
TEST_F(NSFSPersonalityTest, EvictionIsFifoAndUnregisters)
{
  nsfs::PersonalityTable t{3, &reg};

  /* fill it, holding nothing -- refs are released at each iteration */
  std::vector<rgw_user> users;
  for (int i = 0; i < 3; ++i) {
    users.emplace_back("", std::string("u") + std::to_string(i));
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.insert(env->dpp, users[i], creds_for(100 + i), &r), 0);
  }
  ASSERT_EQ(t.size(), 3u);
  const int oldest = reg.registered.front();

  /* the control:  all three are still findable before the overflow */
  for (const auto& u : users) {
    nsfs::PersonalityRef r;
    EXPECT_EQ(t.find(u, &r), 0) << u << " was lost before any eviction";
  }

  const rgw_user newcomer{"", "newcomer"};
  nsfs::PersonalityRef r;
  ASSERT_EQ(t.insert(env->dpp, newcomer, creds_for(999), &r), 0);

  EXPECT_EQ(t.size(), 3u) << "the table grew past its capacity";
  EXPECT_EQ(t.get_stats().evictions, 1u);
  EXPECT_FALSE(reg.live(oldest)) << "the evicted personality was not released";

  nsfs::PersonalityRef gone;
  EXPECT_EQ(t.find(users[0], &gone), -ENOENT)
      << "the oldest entry survived;  eviction is not FIFO";
  nsfs::PersonalityRef kept;
  EXPECT_EQ(t.find(users[1], &kept), 0) << "a younger entry was evicted";
}

/* A pinned slot is skipped, so a live operation keeps its
 * credentials even as the cursor comes round. */
TEST_F(NSFSPersonalityTest, PinnedSlotsAreNotReused)
{
  nsfs::PersonalityTable t{2, &reg};
  const rgw_user held{"", "held"};

  nsfs::PersonalityRef pin;
  ASSERT_EQ(t.insert(env->dpp, held, creds_for(1), &pin), 0);
  const uint16_t held_id = pin.id();

  /* the control:  with the pin released, this same sequence evicts
   * it -- so "survived" below is the pin's doing and not an
   * accident of the table being big enough */
  {
    nsfs::PersonalityTable t2{2, &reg};
    nsfs::PersonalityRef tmp;
    ASSERT_EQ(t2.insert(env->dpp, held, creds_for(1), &tmp), 0);
    tmp.reset();
    for (int i = 0; i < 2; ++i) {
      nsfs::PersonalityRef r;
      ASSERT_EQ(t2.insert(env->dpp, rgw_user{"", "p" + std::to_string(i)},
			  creds_for(50 + i), &r), 0);
      r.reset();
    }
    nsfs::PersonalityRef gone;
    ASSERT_EQ(t2.find(held, &gone), -ENOENT)
	<< "the control did not evict;  the real case proves nothing";
  }

  /* now with the pin held */
  for (int i = 0; i < 2; ++i) {
    nsfs::PersonalityRef r;
    ASSERT_EQ(t.insert(env->dpp, rgw_user{"", "x" + std::to_string(i)},
		       creds_for(60 + i), &r), 0);
    r.reset();
  }

  nsfs::PersonalityRef still;
  EXPECT_EQ(t.find(held, &still), 0) << "a pinned slot was reused";
  EXPECT_EQ(still.id(), held_id);
  EXPECT_TRUE(reg.live(held_id)) << "a pinned personality was unregistered";
}

/* Every slot pinned is a failure, not an improvisation.
 *
 * Serving the request unimpersonated would give it the gateway's
 * reach;  evicting a pinned slot would pull credentials out from
 * under a live operation.  Neither is acceptable, so it fails. */
TEST_F(NSFSPersonalityTest, AllSlotsPinnedFails)
{
  nsfs::PersonalityTable t{2, &reg};

  std::vector<nsfs::PersonalityRef> pins(2);
  for (int i = 0; i < 2; ++i) {
    ASSERT_EQ(t.insert(env->dpp, rgw_user{"", "h" + std::to_string(i)},
		       creds_for(70 + i), &pins[i]), 0);
  }

  nsfs::PersonalityRef denied;
  EXPECT_EQ(t.insert(env->dpp, rgw_user{"", "late"}, creds_for(80),
		     &denied), -EBUSY);
  EXPECT_FALSE(denied.valid());
  EXPECT_EQ(t.get_stats().exhausted, 1u);
  EXPECT_EQ(reg.registered.size(), 2u)
      << "a failed insert registered a personality anyway";

  /* and it recovers:  release one and the next insert succeeds */
  pins[0].reset();
  nsfs::PersonalityRef ok;
  EXPECT_EQ(t.insert(env->dpp, rgw_user{"", "late"}, creds_for(80), &ok), 0);
}

/* Two operations on one identity pin it twice;  it stays until both
 * are gone. */
TEST_F(NSFSPersonalityTest, PinsNest)
{
  nsfs::PersonalityTable t{1, &reg};
  const rgw_user u{"", "shared"};

  nsfs::PersonalityRef a, b;
  ASSERT_EQ(t.insert(env->dpp, u, creds_for(5), &a), 0);
  ASSERT_EQ(t.find(u, &b), 0);

  a.reset();
  nsfs::PersonalityRef blocked;
  EXPECT_EQ(t.insert(env->dpp, rgw_user{"", "other"}, creds_for(6),
		     &blocked), -EBUSY)
      << "one release freed a slot two operations were holding";

  b.reset();
  nsfs::PersonalityRef now;
  EXPECT_EQ(t.insert(env->dpp, rgw_user{"", "other"}, creds_for(6),
		     &now), 0);
}

/* A registrar that fails leaves nothing behind. */
TEST_F(NSFSPersonalityTest, AFailedRegistrationStoresNothing)
{
  nsfs::PersonalityTable t{4, &reg};
  const rgw_user u{"", "nope"};

  /* the control:  it works before the registrar starts failing */
  nsfs::PersonalityRef ok;
  ASSERT_EQ(t.insert(env->dpp, rgw_user{"", "fine"}, creds_for(1), &ok), 0);
  ok.reset();

  reg.fail_with = -EPERM;
  nsfs::PersonalityRef ref;
  EXPECT_EQ(t.insert(env->dpp, u, creds_for(2), &ref), -EPERM);
  EXPECT_FALSE(ref.valid());

  nsfs::PersonalityRef after;
  EXPECT_EQ(t.find(u, &after), -ENOENT)
      << "a failed registration left an entry";
  EXPECT_EQ(t.get_stats().failed, 1u);
}

/* The capacity is clamped to what an SQE can name. */
TEST_F(NSFSPersonalityTest, CapacityIsClampedToTheIdSpace)
{
  nsfs::PersonalityTable t{1000000, &reg};
  EXPECT_EQ(t.capacity(), 65535u);
}

/* The group text parser, which is the cost of storing the vector in
 * one column.  An empty list parses to an empty vector;  malformed
 * input is reported rather than silently yielding one, because an
 * empty vector is a meaningful value. */
TEST(NSFSIdentityGroups, TextRoundTripAndRejection)
{
  std::vector<uint32_t> out;

  EXPECT_EQ(nsfs::groups_to_text({}), "");
  EXPECT_EQ(nsfs::groups_to_text({1}), "1");
  EXPECT_EQ(nsfs::groups_to_text({1, 22, 333}), "1,22,333");

  ASSERT_TRUE(nsfs::groups_from_text("", out));
  EXPECT_TRUE(out.empty());
  ASSERT_TRUE(nsfs::groups_from_text("1,22,333", out));
  EXPECT_EQ(out, (std::vector<uint32_t>{1, 22, 333}));

  EXPECT_FALSE(nsfs::groups_from_text("1,,2", out));
  EXPECT_FALSE(nsfs::groups_from_text(",1", out));
  EXPECT_FALSE(nsfs::groups_from_text("1,", out));
  EXPECT_FALSE(nsfs::groups_from_text("1,x", out));
  EXPECT_FALSE(nsfs::groups_from_text("-1", out));
}

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

/* Deleting a bucket the gateway made removes its directory.
 *
 * The control for the test below:  without it, a directory surviving
 * a delete would prove nothing about the attribute, since it might
 * never have been removed by this path at all. */
TEST_F(NSFSBucketTest, DeletingABucketTheGatewayMadeRemovesTheDirectory)
{
  const sf::path d = bp / "root" / testname;
  ASSERT_TRUE(sf::is_directory(d));

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user), 0);
  EXPECT_FALSE(sf::exists(d));
}

TEST_F(NSFSBucketTest, DeletingAnAdoptedBucketLeavesItsDirectory)
{
  const sf::path d = bp / "root" / testname;
  {
    const sf::path f = d / "predates.txt";
    int fd = ::open(f.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GT(fd, 0);
    const char* data = "written before the bucket existed";
    ASSERT_EQ(::write(fd, data, strlen(data)), (ssize_t)strlen(data));
    ::close(fd);
  }
  ASSERT_EQ(::setxattr(d.c_str(), nsfs::OWNS_STORAGE_XATTR, "0", 1, 0), 0);

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user), 0);

  EXPECT_TRUE(sf::is_directory(d))
      << "removed a directory the bucket was only lent";

  char buf[8192];
  const ssize_t len = ::listxattr(d.c_str(), buf, sizeof(buf));
  ASSERT_GE(len, 0);
  for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
    EXPECT_FALSE(std::string_view(q).starts_with("user.nsfs:"))
	<< q << " survived the delete";
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

/* A key of slashes alone names the root object, the bucket root's
 * directory object, as it does in NooBaa (DECISIONS.md 2026-10-13):
 * written, read, overwritten and deleted through the root's `.folder`.
 * `//` is the same object as `/`, because an empty component is not a
 * name.  `.folder` stays reserved, so the control is that the key
 * `.folder` cannot reach the same file. */
TEST_F(NSFSBucketTest, ASlashOnlyKeyIsTheRootObject)
{
  auto put = [&](const char* k, const std::string& body) {
    auto obj = bucket->get_object(rgw_obj_key(k));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    ASSERT_EQ(writer->prepare(null_yield), 0) << k;
    bufferlist bl;
    bl.append(body);
    ASSERT_EQ(writer->process(std::move(bl), 0), 0);
    ASSERT_EQ(writer->process({}, body.size()), 0);
    ceph::real_time mtime;
    Attrs attrs;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ASSERT_EQ(writer->complete(body.size(), etag, &mtime, real_time(), attrs,
                               std::nullopt, real_time(), nullptr, nullptr,
                               nullptr, nullptr, nullptr, rctx, 0), 0) << k;
  };
  auto get = [&](const char* k) -> std::string {
    auto obj = bucket->get_object(rgw_obj_key(k));
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op(rgw::sal::no_authenticated_user));
    if (read_op->prepare(null_yield, env->dpp) < 0) {
      return "<absent>";
    }
    bufferlist bl;
    Read_CB cb(&bl);
    EXPECT_EQ(read_op->iterate(env->dpp, 0, obj->get_size(), &cb,
                               null_yield), 0);
    return std::string(bl.c_str(), bl.length());
  };

  put("/", "root");
  EXPECT_TRUE(sf::is_regular_file(bucket_path() / ".folder"));
  EXPECT_EQ(get("/"), "root");
  EXPECT_EQ(get("//"), "root") << "`//` is not the same object as `/`";

  put("//", "again");
  EXPECT_EQ(get("/"), "again");

  EXPECT_EQ(get(".folder"), "<absent>") << "`.folder` reached the root object";
  {
    auto obj = bucket->get_object(rgw_obj_key(".folder"));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    EXPECT_EQ(writer->prepare(null_yield), -ERR_INVALID_OBJECT_NAME);
  }
  EXPECT_EQ(get("/"), "again");

  auto obj = bucket->get_object(rgw_obj_key("/"));
  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op(rgw::sal::no_authenticated_user);
  EXPECT_EQ(del_op->delete_obj(env->dpp, null_yield, 0), 0);
  EXPECT_FALSE(sf::exists(bucket_path() / ".folder"));
  EXPECT_EQ(get("/"), "<absent>");
  EXPECT_TRUE(sf::is_directory(bucket_path())) << "deleting `/` took the bucket";
}

/* The root object is never listed, as in NooBaa (DECISIONS.md
 * 2026-10-13):  a listing cannot show it without a key, and the empty
 * key is not one.  Laid down by hand, as a NooBaa-written tree holds it.
 * The control is `sub/.folder`, which must still list as the directory
 * object `sub/`. */
TEST_F(NSFSBucketTest, TheRootObjectIsNotListed)
{
  sf::create_directories(bucket_path() / "sub");
  std::ofstream{bucket_path() / ".folder"} << "root";
  std::ofstream{bucket_path() / "sub" / ".folder"} << "sub";

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  ASSERT_EQ(bucket->list(env->dpp, params, 100, results, null_yield), 0);
  std::set<std::string> names;
  for (auto& e : results.objs) {
    names.insert(e.key.name);
  }
  EXPECT_FALSE(names.count("")) << "the root object listed as the empty key";
  EXPECT_FALSE(names.count("/"));
  EXPECT_FALSE(names.count(".folder"));
  EXPECT_TRUE(names.count("sub/")) << "a directory object stopped listing";
}

/* The root object does not keep its bucket alive, because NooBaa judges
 * a bucket empty by listing it (DECISIONS.md 2026-10-13).  A bucket that
 * owns its storage is removed with the root object.  The control is a
 * bucket holding the directory object `x/`, stored as `x/.folder`,
 * which is not empty and keeps everything. */
TEST_F(NSFSBucketTest, TheRootObjectDoesNotKeepABucket)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());

  sf::create_directories(bucket_path() / "x");
  std::ofstream{bucket_path() / "x" / ".folder"} << "x";
  EXPECT_EQ(b->check_empty(env->dpp, null_yield), -ENOTEMPTY)
      << "a directory object did not count as content";
  EXPECT_LT(bucket->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user), 0);
  EXPECT_TRUE(sf::exists(bucket_path() / "x" / ".folder"));

  sf::remove_all(bucket_path() / "x");
  std::ofstream{bucket_path() / ".folder"} << "root";
  EXPECT_EQ(b->check_empty(env->dpp, null_yield), 0)
      << "the root object counted as content";
  EXPECT_EQ(bucket->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user), 0);
  EXPECT_FALSE(sf::exists(bucket_path()));
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
  EXPECT_TRUE(ret == 0 || ret == -EPERM);

/* nsfs creates objects and their directories in NooBaa's modes, under
 * `rgw_nsfs_umask` (DECISIONS.md 2026-10-11).
 *
 * Both polarities:  the default, 0007, gives 0660 and 0770;  0027
 * gives 0640 and 0750.  The process umask is 022 throughout, which
 * takes the group write bit the default asks for -- so the default
 * case also requires the create mode to be restored on the new file
 * and directory, rather than left to what the umask made of it. */
TEST_F(NSFSBucketTest, CreateModesFollowRgwNsfsUmask)
{
  const mode_t saved = ::umask(022);
  auto restore = make_scope_guard([&] {
    ::umask(saved);
    g_conf().set_val("rgw_nsfs_umask", "0007");
    nsfs::init_create_modes(env->dpp, env->cct.get());
  });

  auto write = [&](const std::string& name) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto writer = driver->get_atomic_writer(
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname,
        rgw::sal::no_authenticated_user);
    ASSERT_EQ(writer->prepare(null_yield), 0);
    bufferlist bl;
    bl.append("x");
    ASSERT_EQ(writer->process(std::move(bl), 0), 0);
    ASSERT_EQ(writer->process({}, 1), 0);
    ceph::real_time mtime;
    Attrs attrs;
    req_context rctx{env->dpp, null_yield, nullptr};
    ASSERT_EQ(writer->complete(1, "", &mtime, real_time(), attrs,
			       std::nullopt, real_time(), nullptr, nullptr,
			       nullptr, nullptr, nullptr, rctx, 0), 0);
  };

  struct Case { const char* umask; mode_t file; mode_t dir; const char* d; };
  for (const Case& c : {Case{"0007", 0660, 0770, "dflt"},
			Case{"0027", 0640, 0750, "tight"}}) {
    g_conf().set_val("rgw_nsfs_umask", c.umask);
    nsfs::init_create_modes(env->dpp, env->cct.get());
    write(std::string(c.d) + "/obj.bin");
    EXPECT_EQ(file_mode(bucket_path() / c.d / "obj.bin"), c.file)
	<< "rgw_nsfs_umask " << c.umask;
    EXPECT_EQ(file_mode(bucket_path() / c.d), c.dir)
	<< "rgw_nsfs_umask " << c.umask << ", the object's directory";
  }
}

/* NooBaa's multipart staging layout.
 *
 * These assert on-disk names, offsets and sizes directly, because the
 * layout is somebody else's and this is where we state what we believe
 * it to be.  Read from noobaa-core 68ca22d33, `namespace_fs.js`.
 *
 * A fixture written by the nsfs emulator only proves we read what we
 * wrote, so a tree captured from an installation has to sit under this
 * eventually.  What these can settle without one is that the names and
 * the arithmetic match the source.
 */

namespace {

/* lay down a staging directory as NooBaa would, and hand back its fd */
/* a uuid-shaped string;  NooBaa uses crypto.randomUUID() for both the
 * bucket id and the upload id, and nothing infers either */
std::string fake_uuid(const std::string& seed)
{
  /* 8-4-4-4-12 exactly.  The shape is load-bearing, not decoration:
   * NooBaaMPUStrategy recognises NooBaa's temp directory by the
   * structural part of its name -- `_` and a UUID -- because the
   * prefix is a config default they can change.  An earlier version
   * of this produced a 20-character final group and was not a UUID at
   * all;  nothing noticed while the strategy matched the prefix. */
  const std::string h =
      fmt::format("{:0>16x}", std::hash<std::string>{}(seed));
  return h.substr(0, 8) + "-" + h.substr(8, 4) + "-4" + h.substr(12, 3) +
	 "-8" + h.substr(1, 3) + "-" + h.substr(0, 12);
}

class NBStaging {
public:
  sf::path root;     /* <base>/<test>/.noobaa-nsfs_<id>/multipart-uploads */
  sf::path upload;   /* root/<upload id> */
  int fd{-1};

  NBStaging(const std::string& test, const std::string& bucket_id,
	    const std::string& upload_id) {
    sf::path bucket{base_path / test};
    sf::create_directories(bucket);
    root = bucket / (".noobaa-nsfs_" + bucket_id) / "multipart-uploads";
    upload = root / upload_id;
    sf::create_directories(upload);
    fd = ::open(upload.c_str(), O_RDONLY | O_DIRECTORY);
  }
  ~NBStaging() { if (fd >= 0) { ::close(fd); } }

  /* write `count` parts of `size` bytes into parts-size-<size>, part K
   * at size * (K - 1), each filled with a distinct byte */
  void write_uniform(uint64_t size, uint32_t first, uint32_t count) {
    const sf::path f{upload / ("parts-size-" + std::to_string(size))};
    int pfd = ::open(f.c_str(), O_WRONLY | O_CREAT, 0600);
    ASSERT_GE(pfd, 0);
    for (uint32_t k = first; k < first + count; ++k) {
      std::string buf(size, static_cast<char>('a' + (k % 26)));
      ASSERT_EQ(::pwrite(pfd, buf.data(), buf.size(), (k - 1) * size),
		static_cast<ssize_t>(buf.size()));
    }
    ::close(pfd);
  }
};

std::string read_all(const sf::path& p)
{
  std::ifstream f(p, std::ios::binary);
  return std::string((std::istreambuf_iterator<char>(f)),
		     std::istreambuf_iterator<char>());
}

} /* anonymous namespace */

TEST(NooBaaMPU, Names)
{
  nsfs::NooBaaMPUStrategy nb;

  /* NooBaa's directory is the upload id alone;  the key lives inside */
  EXPECT_EQ(nb.staging_dir_name("some/key.2~abcdef"), "2~abcdef");
  EXPECT_EQ(nb.staging_dir_name("key.with.dots.UUID"), "UUID");

  EXPECT_EQ(nb.part_name(1), "part-1");
  EXPECT_EQ(nb.part_name(1000), "part-1000");
  EXPECT_EQ(nb.part_number("part-7"), std::optional<uint32_t>{7});
  EXPECT_EQ(nb.part_number("parts-size-64"), std::nullopt);
  EXPECT_EQ(nb.part_number("part-"), std::nullopt);
  EXPECT_EQ(nb.part_number("final"), std::nullopt);

  EXPECT_EQ(nb.meta_name(), "create_object_upload");
  EXPECT_EQ(nb.assembled_name(), "final");
  EXPECT_EQ(nb.shared_name(64), std::optional<std::string>{"parts-size-64"});
}

/* The shared file is spelled the same in both layouts, so the name
 * cannot tell them apart.  Asserted rather than commented, because the
 * two readings agree for every part but a short last one:  a layout
 * identified by this name would be wrong only at the tail, and
 * silently. */
TEST(NooBaaMPU, SharedFileNameIsNotADiscriminator)
{
  nsfs::NooBaaMPUStrategy nb;
  nsfs::StridedMPUStrategy strided;

  EXPECT_EQ(nb.shared_name(1048576), strided.shared_name(1048576));

  /* what does distinguish them */
  EXPECT_NE(nb.meta_name(), strided.meta_name());
  EXPECT_NE(nb.assembled_name(), strided.assembled_name());
  EXPECT_NE(nb.part_name(1), strided.part_name(1));
}

/* Staging is found by what the directory holds, not by its name.
 *
 * `config.NSFS_TEMP_DIR_NAME` is a default in NooBaa's config.js, so the
 * `.noobaa-nsfs` prefix is not something we may depend on.  A
 * directory holding `multipart-uploads/` IS the temp directory
 * whatever it is called, and that settles every tree the read path
 * cares about -- a tree with no uploads has nothing to find.
 *
 * `abc123` here is deliberately neither NooBaa's default prefix nor a
 * UUID:  it is the case the prefix match got wrong.
 */
TEST(NooBaaMPU, StagingRootIsFoundByContent)
{
  nsfs::NooBaaMPUStrategy nb;
  const std::string test = get_test_name();
  NBStaging st(test, "abc123", "u1");
  ASSERT_GE(st.fd, 0);

  int bfd = ::open((base_path / test).c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(bfd, 0);
  auto r = nb.staging_root(env->dpp, bfd);
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(*r, ".noobaa-nsfs_abc123/multipart-uploads");

  /* A quiescent temp directory beside it, recognised by its shape.
   * Content beats shape:  the one holding uploads is still the one
   * returned, because a directory that holds uploads cannot be the
   * wrong one. */
  sf::create_directories(base_path / test /
			 (".noobaa-nsfs_" + fake_uuid("quiescent")));
  r = nb.staging_root(env->dpp, bfd);
  ASSERT_TRUE(r.has_value()) << "a quiescent neighbour hid the real one";
  EXPECT_EQ(*r, ".noobaa-nsfs_abc123/multipart-uploads");

  /* But two directories BOTH holding uploads is undecidable, and it
   * rejects the write rather than picking. */
  NBStaging other(test, "def456", "u2");
  ASSERT_GE(other.fd, 0);
  EXPECT_EQ(nb.staging_root(env->dpp, bfd), std::nullopt);
  ::close(bfd);
}

/* The shape test, on its own.
 *
 * It is what finds NooBaa's temp directory on a tree that has never
 * taken an upload -- there is nothing inside it to recognise then, so
 * the name is all there is, and only the structural part of the name
 * is NooBaa's to keep:  `_` and the bucket id, which is a
 * crypto.randomUUID().  The prefix before it is configurable and is
 * not consulted.
 */
TEST(NooBaaMPU, AQuiescentTempDirectoryIsFoundByShape)
{
  nsfs::NooBaaMPUStrategy nb;
  const std::string test = get_test_name();
  const sf::path b{base_path / test};
  sf::remove_all(b);
  sf::create_directories(b);

  /* NooBaa's default prefix, with NooBaa's id shape */
  sf::create_directories(b / (".noobaa-nsfs_" + fake_uuid("one")));
  int bfd = ::open(b.c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(bfd, 0);

  /* no uploads, so the read side has nothing to report */
  EXPECT_EQ(nb.staging_root(env->dpp, bfd), std::nullopt);

  /* and the write side finds nothing and makes nothing:  NooBaa's
   * staging is read and never written (DECISIONS.md 2026-10-21) */
  EXPECT_EQ(nb.staging_root_for_write(env->dpp, bfd), std::nullopt);
  EXPECT_FALSE(sf::exists(b / (".noobaa-nsfs_" + fake_uuid("one"))
			  / "multipart-uploads"))
      << "the write side created NooBaa's staging";

  int n = 0;
  for (auto& e : sf::directory_iterator(b)) {
    if (sf::is_directory(e)) {
      ++n;
    }
  }
  EXPECT_EQ(n, 1) << "a second temp directory was created beside theirs";
  ::close(bfd);
}

/* A renamed prefix is still found, which is the whole point.
 *
 * A deployment that set config.NSFS_TEMP_DIR_NAME to something else
 * has a tree the prefix match could not read at all:  every upload in
 * flight invisible, and a second temp directory created beside
 * NooBaa's on the next write.
 */
TEST(NooBaaMPU, ARenamedTempDirectoryIsStillFound)
{
  nsfs::NooBaaMPUStrategy nb;
  const std::string test = get_test_name();
  const sf::path b{base_path / test};
  sf::remove_all(b);
  const std::string renamed = ".scale-nsfs_" + fake_uuid("renamed");
  sf::create_directories(b / renamed / "multipart-uploads");

  int bfd = ::open(b.c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(bfd, 0);
  auto r = nb.staging_root(env->dpp, bfd);
  ASSERT_TRUE(r.has_value()) << "a renamed temp directory was not found";
  EXPECT_EQ(*r, renamed + "/multipart-uploads");

  /* and the write side does not use it:  NooBaa's staging is read and
   * never written (DECISIONS.md 2026-10-21) */
  EXPECT_EQ(nb.staging_root_for_write(env->dpp, bfd), std::nullopt);
  int n = 0;
  for (auto& e : sf::directory_iterator(b)) {
    if (sf::is_directory(e)) {
      ++n;
    }
  }
  EXPECT_EQ(n, 1) << "a second temp directory was created";
  ::close(bfd);
}

TEST(NooBaaMPU, StagingRootAbsentIsNotAnError)
{
  nsfs::NooBaaMPUStrategy nb;
  const std::string test = get_test_name();
  sf::create_directories(base_path / test);
  int bfd = ::open((base_path / test).c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(bfd, 0);
  EXPECT_EQ(nb.staging_root(env->dpp, bfd), std::nullopt);
  ::close(bfd);
}

/* Completion reads the parts as their uploader.
 *
 * The parts are the uploader's files, and the gateway can read them
 * only with CAP_DAC_READ_SEARCH, which a thread takes the first time
 * it opens something with no identity and not before.  A completion
 * coroutine that resumed on a thread which had not yet done that read
 * its parts as the gateway without the capability, and failed with
 * EACCES (measured in the baseline, 2026-10-20).  So each test
 * assembles on a fresh thread.
 *
 * The control is on that thread too:  the gateway's own open of an
 * input must fail, or the assembly's success says nothing about whose
 * credentials read it. */
class NSFSImpersonatedAssembleTest : public NSFSRaiseTest {
protected:
  static constexpr uid_t uploader = 70150;
  static constexpr uint64_t size = 16;
  const nsfs::FSIdentity id{rgw_user{"", "uploader"}, creds_for(uploader)};
  sf::path dir;
  int dfd{-1};

  void SetUp() override {
    if (::geteuid() == 0) {
      GTEST_SKIP() << "root reads every part whatever the owner";
    }
    if (!permitted(CAP_SETUID) || !permitted(CAP_SETGID) ||
	!permitted(CAP_CHOWN)) {
      GTEST_SKIP() << "the binary cannot assume an identity or give it a "
		      "file;  run the setcap step";
    }
    dir = base_path / get_test_name();
    sf::create_directories(dir);
    ASSERT_EQ(::chmod(dir.c_str(), 01777), 0);
    dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    ASSERT_GE(dfd, 0);
  }

  void TearDown() override {
    if (dfd >= 0) {
      ::close(dfd);
    }
  }

  /* An input as completion finds it:  the uploader's, and 0660. */
  void give(const std::string& name, const std::string& body) {
    int fd = ::openat(dfd, name.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0660);
    ASSERT_GE(fd, 0) << name;
    auto close_fd = make_scope_guard([fd] { ::close(fd); });
    ASSERT_EQ(::write(fd, body.data(), body.size()), (ssize_t)body.size());
    ASSERT_EQ(::fchmod(fd, 0660), 0);
    ASSERT_EQ(with_capabilities_elsewhere(nsfs::cap_bit(CAP_CHOWN),
	[fd]() -> int {
	  return ::fchown(fd, uploader, uploader) < 0 ? -errno : 0;
	}), 0) << "could not give " << name << " to the uploader";
  }

  struct Fresh {
    bool had_read_search{true};
    std::vector<int> controls;
    int assembled{0};
  };

  /* `assemble` on a thread that has never raised read-search, after
   * the gateway's own open of each of `inputs` there. */
  Fresh on_a_fresh_thread(const std::vector<std::string>& inputs,
			  const std::function<int()>& assemble) {
    Fresh r;
    std::thread fresh([&] {
      r.had_read_search =
	  nsfs::capability_is_effective(CAP_DAC_READ_SEARCH);
      for (const auto& name : inputs) {
	const int fd = ::openat(dfd, name.c_str(), O_RDONLY);
	r.controls.push_back((fd < 0) ? -errno : 0);
	if (fd >= 0) {
	  ::close(fd);
	}
      }
      r.assembled = assemble();
    });
    fresh.join();
    return r;
  }

  /* the output is the uploader's, so it is read back with the
   * capability */
  std::string read_back(const std::string& name) {
    std::string out;
    EXPECT_EQ(nsfs::with_capabilities(env->dpp,
				      nsfs::cap_bit(CAP_DAC_READ_SEARCH),
	[&]() -> int {
	  out = read_all(dir / name);
	  return 0;
	}), 0);
    return out;
  }

  static void expect_gateway_denied(const Fresh& r) {
    ASSERT_FALSE(r.had_read_search)
	<< "a new thread started with CAP_DAC_READ_SEARCH effective";
    for (const int c : r.controls) {
      ASSERT_EQ(c, -EACCES)
	  << "the gateway could open an input itself, so the assembly "
	     "does not show whose credentials read it";
    }
  }
};

/* One file per part. */
TEST_F(NSFSImpersonatedAssembleTest, PartsAreReadAsTheUploader)
{
  nsfs::PerPartMPUStrategy mpu;
  nsfs::POSIXStrategy fs;

  std::vector<nsfs::MPUStrategy::PartPlacement> parts;
  for (uint32_t n = 1; n <= 2; ++n) {
    ASSERT_NO_FATAL_FAILURE(give(mpu.part_name(n),
				 std::string(size, static_cast<char>('a' + n))));
    parts.push_back({n, false, 0, size});
  }

  const Fresh r = on_a_fresh_thread({mpu.part_name(1)}, [&] {
    return mpu.assemble(env->dpp, &fs, dfd, parts, "final", size,
			null_yield, nsfs::CopyHints{}, id);
  });
  ASSERT_NO_FATAL_FAILURE(expect_gateway_denied(r));
  ASSERT_EQ(r.assembled, 0)
      << "the parts were not read as their uploader: "
      << cpp_strerror(-r.assembled);
  EXPECT_EQ(read_back("final"),
	    std::string(size, 'b') + std::string(size, 'c'));
}

/* The strided layout, where it copies:  part 1 in the shared file and
 * part 2 in its own, which is not contiguous and so opens both. */
TEST_F(NSFSImpersonatedAssembleTest, StridedInputsAreReadAsTheUploader)
{
  nsfs::StridedMPUStrategy mpu;
  nsfs::POSIXStrategy fs;

  const std::string shared = *mpu.shared_name(size);
  ASSERT_NO_FATAL_FAILURE(give(shared, std::string(size, 'b')));
  ASSERT_NO_FATAL_FAILURE(give(mpu.part_name(2), std::string(size, 'c')));
  const std::vector<nsfs::MPUStrategy::PartPlacement> parts{
    {1, true, 0, size}, {2, false, 0, size}};

  const Fresh r = on_a_fresh_thread({shared, mpu.part_name(2)}, [&] {
    return mpu.assemble(env->dpp, &fs, dfd, parts, "final", size,
			null_yield, nsfs::CopyHints{}, id);
  });
  ASSERT_NO_FATAL_FAILURE(expect_gateway_denied(r));
  ASSERT_EQ(r.assembled, 0)
      << "the shared file or a part was not read as its uploader: "
      << cpp_strerror(-r.assembled);
  EXPECT_EQ(read_back("final"),
	    std::string(size, 'b') + std::string(size, 'c'));
}

/* The gateway's attribute reads take the capability themselves.
 *
 * Read permission on a user.* attribute is checked at each
 * fgetxattr(2), against the calling thread, not at open.  So a
 * descriptor opened on a thread that held CAP_DAC_READ_SEARCH does not
 * carry it to the thread a coroutine resumes on.  The case opens the
 * uploader's object on one thread and reads it on another, after a
 * raw read there as the control:  it must fail, or the wrapper's
 * success says nothing.
 *
 * Impersonation is on for the length of the case, which is what the
 * wrapper asks before it takes the capability. */
class NSFSGatewayAttrReadTest : public NSFSImpersonatedAssembleTest {
protected:
  static constexpr const char* attr = "user.nsfs:test";
  int ofd{-1};

  void SetUp() override {
    NSFSImpersonatedAssembleTest::SetUp();
    if (IsSkipped() || HasFatalFailure()) {
      return;
    }
    set_impersonate("true");

    ASSERT_NO_FATAL_FAILURE(give("obj", "body"));
    const std::string path = (dir / "obj").string();
    ASSERT_EQ(with_capabilities_elsewhere(
				      nsfs::cap_bit(CAP_DAC_OVERRIDE),
	[&path]() -> int {
	  return ::setxattr(path.c_str(), attr, "v", 1, 0) < 0 ? -errno : 0;
	}), 0) << "could not give the object an attribute";

    /* opened on a thread that holds the capability, as the gateway
     * opens an object it reads with no identity */
    std::thread opener([this] {
      (void) nsfs::hold_thread_capabilities(env->dpp);
      ofd = ::openat(dfd, "obj", O_RDONLY);
    });
    opener.join();
    ASSERT_GE(ofd, 0) << "the gateway could not open the object";
  }

  void TearDown() override {
    if (ofd >= 0) {
      ::close(ofd);
    }
    set_impersonate("false");
    NSFSImpersonatedAssembleTest::TearDown();
  }

  static void set_impersonate(const char* v) {
    g_conf().set_val_or_die("rgw_nsfs_impersonate", v);
    g_conf().apply_changes(nullptr);
  }
};

TEST_F(NSFSGatewayAttrReadTest, AnAttributeIsReadOnAnotherThread)
{
  bool had_read_search = true;
  int control = 0;
  ssize_t len = -1;
  int err = 0;
  char buf[16] = {};
  std::thread reader([&] {
    had_read_search = nsfs::capability_is_effective(CAP_DAC_READ_SEARCH);
    control = (::fgetxattr(ofd, attr, buf, sizeof(buf)) < 0) ? -errno : 0;
    len = nsfs::fgetxattr(ofd, attr, buf, sizeof(buf));
    err = errno;
  });
  reader.join();

  ASSERT_FALSE(had_read_search)
      << "a new thread started with CAP_DAC_READ_SEARCH effective";
  ASSERT_EQ(control, -EACCES)
      << "the thread could read the attribute without the capability, so "
	 "the read below does not show the wrapper taking it";
  ASSERT_EQ(len, 1) << "the attribute was not read: " << cpp_strerror(err);
  EXPECT_EQ(std::string(buf, len), "v");
}

TEST_F(NSFSGatewayAttrReadTest, ARaiseKeepsReadSearch)
{
  if (!permitted(CAP_FOWNER) || !permitted(CAP_DAC_OVERRIDE)) {
    GTEST_SKIP() << "the binary cannot raise CAP_FOWNER;  run the setcap "
		    "step";
  }
  ASSERT_EQ(::mkdirat(dfd, "sub", 0700), 0);
  auto remove_sub = make_scope_guard([this] {
    (void) with_capabilities_elsewhere(
				   nsfs::cap_bit(CAP_DAC_OVERRIDE),
      [this]() -> int {
	std::error_code ec;
	sf::remove_all(dir / "sub", ec);
	return ec ? -ec.value() : 0;
      });
  });
  const int xfd = ::openat(dfd, "sub/x", O_WRONLY | O_CREAT | O_EXCL, 0600);
  ASSERT_GE(xfd, 0);
  ::close(xfd);
  ASSERT_EQ(with_capabilities_elsewhere(nsfs::cap_bit(CAP_CHOWN),
	[this]() -> int {
	  return ::fchownat(dfd, "sub", uploader, uploader, 0) < 0 ? -errno : 0;
	}), 0) << "could not give the directory to the uploader";

  bool had_read_search = true;
  int control = 0;
  int inside = -1;
  int after = -1;
  bool still_held = false;
  std::thread fresh([&] {
    struct ::statx stx;
    had_read_search = nsfs::capability_is_effective(CAP_DAC_READ_SEARCH);
    control = (::statx(dfd, "sub/x", AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS,
		       &stx) < 0) ? -errno : 0;
    inside = nsfs::with_capabilities(env->dpp, nsfs::cap_bit(CAP_FOWNER),
      [&]() -> int {
	return (nsfs::statx(dfd, "obj", AT_SYMLINK_NOFOLLOW,
			    STATX_BASIC_STATS, &stx) < 0) ? -errno : 0;
      });
    after = (nsfs::statx(dfd, "sub/x", AT_SYMLINK_NOFOLLOW,
			 STATX_BASIC_STATS, &stx) < 0) ? -errno : 0;
    still_held = nsfs::capability_is_effective(CAP_DAC_READ_SEARCH);
  });
  fresh.join();

  ASSERT_FALSE(had_read_search)
      << "a new thread started with CAP_DAC_READ_SEARCH effective";
  ASSERT_EQ(control, -EACCES)
      << "the thread could search the uploader's directory without the "
	 "capability, so the read below does not show it kept";
  ASSERT_EQ(inside, 0) << cpp_strerror(-inside);
  EXPECT_EQ(after, 0)
      << "the gateway's read after the raise was denied: "
      << cpp_strerror(-after);
  EXPECT_TRUE(still_held)
      << "the raise's restore dropped CAP_DAC_READ_SEARCH";
}

/* An object rename runs as the authenticated user.
 *
 * Under impersonation a key's directory and its `.versions` are made
 * by the authenticated user at `0770`, so the gateway can neither walk them
 * nor move a name out of them.  The rename has to run as that
 * user:  the walks on both sides, the version lock, the intent
 * record on `.versions`, each version's move and the leaf's.  The
 * control is the gateway's own open of the source key directory, on
 * a thread that has never taken read-search:  it must fail, or the
 * rename's success says nothing about whose credentials did it. */
class NSFSImpersonatedRenameTest : public NSFSBucketTest {
protected:
  static constexpr uid_t owner_uid = 70160;
  static constexpr uid_t other_uid = 70161;
  const rgw_user alice{"", "rename-alice"};
  const rgw_user bob{"", "rename-bob"};

  void SetUp() override {
    if (::geteuid() == 0) {
      GTEST_SKIP() << "root walks every directory whatever the owner";
    }
    std::string missing;
    if (! nsfs::have_credential_capabilities(&missing)) {
      GTEST_SKIP() << "the binary cannot assume an identity (" << missing
		   << ");  run the setcap step";
    }
    NSFSBucketTest::SetUp();
    if (HasFailure()) {
      return;
    }
    /* NooBaa's modes:  key directories 0770, objects 0660 */
    g_conf().set_val("rgw_nsfs_umask", "0007");
    nsfs::init_create_modes(env->dpp, env->cct.get());
    /* the bucket directory stays the gateway's, writable by every
     * identity, so each can make its own key directories in it */
    ASSERT_EQ(::chmod(bucket_path().c_str(), 01777), 0);

    bucket->get_info().flags |= BUCKET_VERSIONED;
    ASSERT_EQ(bucket->put_info(env->dpp, false, real_time(), null_yield), 0);

    for (const auto& [who, uid] : {std::pair{alice, owner_uid},
				   std::pair{bob, other_uid}}) {
      nsfs::Identity row;
      row.key = to_string(rgw_owner{who});
      row.uid = uid;
      row.gid = uid;
      ASSERT_EQ(driver->get_identity_db()->put_identity(env->dpp, row), 0);
    }
    set_impersonate("true");
  }

  void TearDown() override {
    }
    set_impersonate("false");
    /* Always, unlike the suite's other trees:  the key directories are
     * the identities' 0770, which the next run's clean_first cannot
     * empty without the capability. */
    {
      (void) nsfs::with_capabilities(env->dpp,
				     nsfs::cap_bit(CAP_DAC_OVERRIDE),
	[this]() -> int {
	  std::error_code ec;
	  sf::remove_all(bp, ec);
	  return ec ? -ec.value() : 0;
	});
    }
    NSFSBucketTest::TearDown();
  }

  static void set_impersonate(const char* v) {
    g_conf().set_val_or_die("rgw_nsfs_impersonate", v);
    g_conf().apply_changes(nullptr);
  }


  /* `who`'s hops to the helper pool since the last call, by kind */
  std::map<std::string, uint64_t> pool_hops(const rgw_user& who,
					    bool enable = true) {
    std::map<std::string, std::string> out;
    EXPECT_EQ(driver->driver_hint(env->dpp, "pool-hops",
				  {{"user", to_string(rgw_owner{who})},
				   {"enable", enable ? "true" : "false"}},
				  &out), 0);
    std::map<std::string, uint64_t> n;
    for (const char* k : {"link", "attr_write", "attr_read", "other"}) {
      n[k] = std::stoull(out[k]);
    }
    return n;
  }

  void inject_no_ring(const rgw_user& who, bool on) {
    std::map<std::string, std::string> out;
    EXPECT_EQ(driver->driver_hint(env->dpp, "inject-no-ring",
				  {{"user", to_string(rgw_owner{who})},
				   {"enable", on ? "true" : "false"}},
				  &out), 0);
  }


  int delete_version_as(const rgw_user& who, const std::string& key,
			const std::string& vid) {
    int ret = 0;
    std::thread deleter([&] {
      auto obj = bucket->get_object(rgw_obj_key(key, vid));
      auto del = obj->get_delete_op(who);
      ret = del->delete_obj(env->dpp, null_yield, 0);
    });
    deleter.join();
    return ret;
  }

  /* On a thread of its own:  the write holds read-search for the life
   * of the thread it runs on, and a thread created after that inherits
   * it, which would spoil the control below and other tests' controls
   * on this binary's main thread. */
  void put_as(const rgw_user& who, const std::string& key,
	      const std::string& body) {
    std::thread writer([&] { put_on_this_thread(who, key, body); });
    writer.join();
    /* the writer's failures are the test's, but only this thread's
     * stop it */
    ASSERT_FALSE(HasFatalFailure()) << "could not write " << key;
  }

  void put_on_this_thread(const rgw_user& who, const std::string& key,
			  const std::string& body) {
    auto obj = bucket->get_object(rgw_obj_key(key));
    auto writer = driver->get_atomic_writer(env->dpp, null_yield, obj.get(),
					    acl_owner, nullptr, 0, "put", who);
    ASSERT_EQ(writer->prepare(null_yield), 0) << key;
    bufferlist bl;
    bl.append(body);
    ASSERT_EQ(writer->process(std::move(bl), 0), 0);
    ASSERT_EQ(writer->process({}, body.size()), 0);
    Attrs attrs;
    ceph::real_time mtime;
    std::string etag;
    req_context rctx{env->dpp, null_yield, nullptr};
    ASSERT_EQ(writer->complete(body.size(), etag, &mtime, real_time(), attrs,
			       std::nullopt, real_time(), nullptr, nullptr,
			       nullptr, nullptr, nullptr, rctx, 0), 0) << key;
  }

  /* On a thread of its own too:  the rename's gateway listings hold
   * read-search for the life of the thread they run on, and this
   * binary's main thread runs the other tests' controls. */
  int rename_as(const rgw_user& who, const std::string& from,
		const std::string& to) {
    int ret = 0;
    std::thread renamer([&] {
      auto obj = bucket->get_object(rgw_obj_key(from));
      ret = obj->rename(env->dpp, null_yield, bucket.get(), rgw_obj_key(to),
			rgw::sal::Object::FLAG_RENAME_NONE, who);
    });
    renamer.join();
    return ret;
  }

  /* the gateway's open of `rel`, on a thread that has never taken
   * read-search */
  int gateway_open_on_a_fresh_thread(const std::string& rel) {
    int r = 0;
    bool had_read_search = true;
    const sf::path p = bucket_path() / rel;
    std::thread fresh([&] {
      had_read_search = nsfs::capability_is_effective(CAP_DAC_READ_SEARCH);
      const int fd = ::open(p.c_str(), O_RDONLY | O_DIRECTORY);
      r = (fd < 0) ? -errno : 0;
      if (fd >= 0) {
	::close(fd);
      }
    });
    fresh.join();
    EXPECT_FALSE(had_read_search)
	<< "a new thread started with CAP_DAC_READ_SEARCH effective";
    return r;
  }

  /* the gateway's unlink of `rel`, on a thread that has never taken
   * read-search */
  int gateway_unlink_on_a_fresh_thread(const std::string& rel) {
    int r = 0;
    const sf::path p = bucket_path() / rel;
    std::thread fresh([&] {
      r = (::unlink(p.c_str()) < 0) ? -errno : 0;
    });
    fresh.join();
    return r;
  }

  /* the tree is the identities', so it is read with the capability */
  struct Owned {
    int err{0};
    uid_t uid{0};
    mode_t mode{0};
  };
  Owned owned(const std::string& rel) {
    Owned o;
    const sf::path p = bucket_path() / rel;
    EXPECT_EQ(with_capabilities_elsewhere(
				      nsfs::cap_bit(CAP_DAC_READ_SEARCH),
	[&]() -> int {
	  struct stat st{};
	  o.err = (::lstat(p.c_str(), &st) < 0) ? -errno : 0;
	  o.uid = st.st_uid;
	  o.mode = st.st_mode & 07777;
	  return 0;
	}), 0);
    return o;
  }

  std::vector<std::string> versions_in(const std::string& keydir) {
    std::vector<std::string> out;
    const sf::path p = bucket_path() / keydir / ".versions";
    EXPECT_EQ(with_capabilities_elsewhere(
				      nsfs::cap_bit(CAP_DAC_READ_SEARCH),
	[&]() -> int {
	  std::error_code ec;
	  for (const auto& e : sf::directory_iterator(p, ec)) {
	    const std::string n = e.path().filename().string();
	    if (n.rfind(".", 0) != 0) {
	      out.push_back(n);
	    }
	  }
	  return 0;
	}), 0);
    return out;
  }
};

/* The owner's rename moves the leaf and its history, and everything it
 * makes on the destination side is the owner's. */
TEST_F(NSFSImpersonatedRenameTest, TheOwnersRenameRunsAsTheOwner)
{
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "src/k", "first"));
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "src/k", "second"));
  ASSERT_EQ(versions_in("src").size(), 1u) << "no history to move";

  ASSERT_EQ(owned("src").uid, owner_uid);
  ASSERT_EQ(gateway_open_on_a_fresh_thread("src"), -EACCES)
      << "the gateway could walk the owner's key directory itself, so the "
	 "rename does not show whose credentials ran it";

  const int ret = rename_as(alice, "src/k", "dst/k");
  ASSERT_EQ(ret, 0) << "the owner's rename failed: " << cpp_strerror(-ret);

  EXPECT_EQ(owned("src/k").err, -ENOENT) << "the leaf was not moved";
  const Owned leaf = owned("dst/k");
  ASSERT_EQ(leaf.err, 0) << "the leaf did not arrive";
  EXPECT_EQ(leaf.uid, owner_uid);

  const Owned ddir = owned("dst");
  EXPECT_EQ(ddir.uid, owner_uid) << "the destination key directory";
  EXPECT_EQ(ddir.mode, 0770u) << "the destination key directory";
  const Owned dvers = owned("dst/.versions");
  ASSERT_EQ(dvers.err, 0) << "the destination has no .versions";
  EXPECT_EQ(dvers.uid, owner_uid) << "the destination .versions";

  EXPECT_TRUE(versions_in("src").empty()) << "the history stayed behind";
  const auto moved = versions_in("dst");
  ASSERT_EQ(moved.size(), 1u) << "the history did not follow the leaf";
  EXPECT_EQ(owned("dst/.versions/" + moved[0]).uid, owner_uid);
}


/* A tag removal runs as the authenticated user (phase C's validation clause,
 * the call-site table's remove_x_attr row), under each access model.
 *
 * fremovexattr(2) of a `user.*` name is checked against the calling
 * thread, so on alice's 0660 object it succeeds only with credentials
 * that may write it.  Under posix-strict no raise is possible, so a
 * removal that succeeds there can only have run with that user's
 * credentials -- which is what remove_x_attr()'s old single-name branch,
 * acting as the gateway, failed (16c5f31a2e1).  The premise is that the
 * gateway, unraised, cannot;  bob, whom posix-strict denies, cannot
 * either, so the identity follows the authenticated user.  Under
 * s3-native the same denial is performed by the gateway under a raise,
 * which the access model's counter shows -- the case in which the file outcome
 * alone would not say whose credentials ran.  The model is set here,
 * not inherited;  TearDown() restores the configured one. */
TEST_F(NSFSImpersonatedRenameTest, ATagRemovalRunsAsTheAuthenticatedUserUnderEachModel)
{
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "k", "alice's"));
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  const std::string tag_x = b->xattr_strategy()->disk_name(RGW_ATTR_TAGS);
  ASSERT_FALSE(tag_x.empty());

  /* on a thread of its own, as put_as() */
  auto on_thread = [](const std::function<int()>& fn) {
    int r = 0;
    std::thread t([&] { r = fn(); });
    t.join();
    return r;
  };
  auto tag_as = [&](const rgw_user& who) {
    RGWObjTags tags;
    tags.add_tag("phase", "d");
    bufferlist bl;
    tags.encode(bl);
    return on_thread([&] {
      auto obj = bucket->get_object(rgw_obj_key("k"));
      /* as the core does before an attribute operation */
      if (const int lr = obj->load_obj_state(env->dpp, null_yield); lr < 0) {
	return lr;
      }
      return obj->modify_obj_attrs(RGW_ATTR_TAGS, bl, null_yield, env->dpp,
				   who);
    });
  };
  auto untag_as = [&](const rgw_user& who) {
    return on_thread([&] {
      auto obj = bucket->get_object(rgw_obj_key("k"));
      /* as the core does before an attribute operation */
      if (const int lr = obj->load_obj_state(env->dpp, null_yield); lr < 0) {
	return lr;
      }
      return obj->delete_obj_attrs(env->dpp, RGW_ATTR_TAGS, null_yield,
				   who);
    });
  };
  /* the attribute's presence, read with the capability */
  auto tagged = [&]() {
    ssize_t len = -1;
    EXPECT_EQ(with_capabilities_elsewhere(nsfs::cap_bit(CAP_DAC_READ_SEARCH),
	[&]() -> int {
	  len = ::getxattr((bucket_path() / "k").c_str(), tag_x.c_str(),
			   nullptr, 0);
	  return 0;
	}), 0);
    return len >= 0;
  };
  /* the gateway performing an attribute operation the filesystem denied
   * the authenticated user:  with_cross_account_retry()'s counter,
   * reported by the access-model hint as `attrs_served` -- not raised(), which
   * counts with_identity_or_raise()'s directory operations */
  auto attrs_served = [this]() {
    std::map<std::string, std::string> out;
    EXPECT_EQ(driver->driver_hint(env->dpp, "access-model", {}, &out), 0);
    return std::stoull(out["attrs_served"]);
  };

  /* posix-strict:  no raise, so the file's outcome names the credentials */
  ASSERT_EQ(set_model("posix-strict"), 0);
  ASSERT_EQ(tag_as(alice), 0);
  ASSERT_TRUE(tagged());
  EXPECT_EQ(on_thread([&] {
	      return (::removexattr((bucket_path() / "k").c_str(),
				    tag_x.c_str()) < 0) ? -errno : 0;
	    }), -EACCES)
      << "the premise:  the gateway, unraised, cannot remove it";
  ASSERT_TRUE(tagged());
  const uint64_t served_strict = attrs_served();
  const uint64_t raised_strict = raised();
  EXPECT_LT(untag_as(bob), 0) << "posix-strict let another identity remove it";
  EXPECT_TRUE(tagged()) << "bob's denied removal removed the tags";
  EXPECT_EQ(untag_as(alice), 0);
  EXPECT_FALSE(tagged()) << "alice's removal left the tags";
  EXPECT_EQ(attrs_served(), served_strict)
      << "posix-strict performed an attribute operation as the gateway, so "
	 "the outcomes above do not name the credentials";
  EXPECT_EQ(raised(), raised_strict)
      << "posix-strict raised a directory operation";

  /* s3-native:  bob's removal is performed, by the gateway under a raise */
  ASSERT_EQ(set_model("s3-native"), 0);
  ASSERT_EQ(tag_as(alice), 0);
  ASSERT_TRUE(tagged());
  const uint64_t served_native = attrs_served();
  EXPECT_EQ(untag_as(bob), 0) << "s3-native did not perform bob's removal";
  EXPECT_FALSE(tagged()) << "s3-native's removal left the tags";
  EXPECT_GT(attrs_served(), served_native)
      << "bob's removal succeeded without a raise, so it ran as something "
	 "other than the gateway";
}

/* The owner's demote and delete marker run as the owner (phase C's
 * validation clause, the safe_link pair:  the writer's demote,
 * `demote_current_version()`, and the delete marker's link and CAS
 * unlink in `delete_obj()`).  Under posix-strict nothing raises, so
 * each succeeds only with credentials that may write the owner's 0770
 * `.versions` and link the owner's file -- which the gateway, unraised,
 * cannot (the premise) -- and `raised()` not moving shows no raise did
 * it.  The marker is a file the delete creates, so its uid is the
 * identity observed directly.  The s3-native cases are
 * UnderS3NativeAnotherIdentitysPutIsRaised and ...DeleteIsRaised. */
TEST_F(NSFSImpersonatedRenameTest, TheOwnersDemoteAndMarkerRunAsTheOwner)
{
  ASSERT_EQ(set_model("posix-strict"), 0);
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "src/k", "first"));
  const uint64_t before = raised();

  /* the writer's demote */
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "src/k", "second"));
  ASSERT_EQ(versions_in("src").size(), 1u) << "the overwrite demoted nothing";

  /* the premise:  the gateway, unraised, cannot link into `.versions` */
  int premise = 0;
  std::thread fresh([&] {
    const int src = ::open((bucket_path() / "src").c_str(),
			   O_RDONLY | O_DIRECTORY);
    const int vfd = ::open((bucket_path() / "src" / ".versions").c_str(),
			   O_RDONLY | O_DIRECTORY);
    premise = ((src < 0) || (vfd < 0)) ? -errno
	: ((::linkat(src, "k", vfd, "probe", 0) < 0) ? -errno : 0);
    if (src >= 0) {
      ::close(src);
    }
    if (vfd >= 0) {
      ::close(vfd);
    }
  });
  fresh.join();
  EXPECT_TRUE((premise == -EACCES) || (premise == -EPERM))
      << "the gateway could link into the owner's .versions: "
      << cpp_strerror(-premise);

  /* the delete marker's demote, its CAS unlink, and the marker */
  const int ret = delete_as(alice, "src/k");
  ASSERT_EQ(ret, 0) << "the delete failed: " << cpp_strerror(-ret);
  EXPECT_EQ(owned("src/k").err, -ENOENT) << "the current version stayed";
  const auto hist = versions_in("src");
  ASSERT_EQ(hist.size(), 3u) << "two demoted versions and the marker";
  for (const auto& v : hist) {
    EXPECT_EQ(owned("src/.versions/" + v).uid, owner_uid)
	<< v << " is not the owner's";
  }

  EXPECT_EQ(raised(), before)
      << "posix-strict raised, so the outcomes above do not name the "
	 "credentials";
}

/* N-D3:  an identity's links and attribute operations run on the ring.
 * A put, an overwrite (a demote) and a versioned delete (a demote, the
 * CAS unlink, a delete marker) make no hop to the pool for either;  the
 * hops left are the mode changes, which have no opcode.  With the ring
 * withheld (`inject-no-ring`) the same sequence hops for each kind, so
 * the counter sees them and the fallback runs.  With metadata changes
 * kept off the ring, as on GPFS, links and attribute writes hop and
 * reads do not.  The outcome is the same every way. */
TEST_F(NSFSImpersonatedRenameTest, AnIdentitysLinksAndAttributesTakeNoHop)
{
  ASSERT_EQ(set_model("posix-strict"), 0);
  enum class Route { ring, withheld, metadata_on_pool };
  for (const Route route : {Route::ring, Route::withheld,
			    Route::metadata_on_pool}) {
    const char* what = (route == Route::ring) ? "on the ring"
      : (route == Route::withheld) ? "ring withheld"
      : "metadata changes on the pool (GPFS's route)";
    SCOPED_TRACE(what);
    inject_no_ring(alice, route == Route::withheld);
    nsfs::set_ring_metadata_changes(route != Route::metadata_on_pool);
    (void) pool_hops(alice);
    const std::string dir = (route == Route::ring) ? "ring"
      : (route == Route::withheld) ? "pool" : "meta";
    ASSERT_NO_FATAL_FAILURE(put_as(alice, dir + "/k", "first"));
    ASSERT_NO_FATAL_FAILURE(put_as(alice, dir + "/k", "second"));
    ASSERT_EQ(delete_as(alice, dir + "/k"), 0);
    auto hops = pool_hops(alice);
    nsfs::set_ring_metadata_changes(true);

    const auto hist = versions_in(dir);
    ASSERT_EQ(hist.size(), 3u) << "two demoted versions and the marker";
    for (const auto& v : hist) {
      EXPECT_EQ(owned(dir + "/.versions/" + v).uid, owner_uid)
	  << v << " is not the owner's";
    }
    switch (route) {
    case Route::ring:
      EXPECT_EQ(hops["link"], 0u);
      EXPECT_EQ(hops["attr_write"], 0u);
      EXPECT_EQ(hops["attr_read"], 0u);
      break;
    case Route::withheld:
      EXPECT_GT(hops["link"], 0u);
      EXPECT_GT(hops["attr_write"], 0u);
      EXPECT_GT(hops["attr_read"], 0u);
      break;
    case Route::metadata_on_pool:
      /* the changes go to the pool;  the reads stay on the ring */
      EXPECT_GT(hops["link"], 0u);
      EXPECT_GT(hops["attr_write"], 0u);
      EXPECT_EQ(hops["attr_read"], 0u);
      break;
    }
  }
  inject_no_ring(alice, false);
  (void) pool_hops(alice, false);
}

/* posix-strict:  another identity's rename out of the owner's key
 * directory is denied, and nothing moves. */
TEST_F(NSFSImpersonatedRenameTest, AnotherIdentitysRenameIsDenied)
{
  ASSERT_STREQ(driver->get_access_strategy()->name(), "posix-strict");
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "src/k", "body"));

  const int ret = rename_as(bob, "src/k", "dst/k");
  EXPECT_TRUE(ret == -EACCES || ret == -EPERM)
      << "another identity's rename was not denied: " << cpp_strerror(-ret);
  EXPECT_EQ(owned("src/k").err, 0) << "the leaf moved";
  EXPECT_EQ(owned("dst/k").err, -ENOENT) << "the leaf arrived";
}


static std::map<std::string, bool> listed_versions(rgw::sal::Bucket* bucket,
						   const DoutPrefixProvider* dpp,
						   const std::string& key);

/* A bucket whose versions were all deleted by their writer keeps
 * only scaffolding -- that writer's `.versions` at 0770 -- and the
 * gateway's bucket delete removes it (raise site 3).  The premise is
 * that the gateway, unraised, cannot.  Two versions, so one has been
 * demoted into `.versions` before both are deleted. */
TEST_F(NSFSImpersonatedRenameTest, ABucketEmptiedByItsWriterIsDeleted)
{
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "k", "first"));
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "k", "second"));
  /* the listings and the delete on threads of their own:  a gateway
   * listing holds read-search for the life of its thread, and this
   * binary's main thread runs the other tests' controls */
  auto listed_elsewhere = [this]() {
    std::map<std::string, bool> out;
    std::thread lister([&] {
      out = listed_versions(bucket.get(), env->dpp, "k");
    });
    lister.join();
    return out;
  };
  const auto listed = listed_elsewhere();
  ASSERT_EQ(listed.size(), 2u);
  for (const auto& [vid, _] : listed) {
    ASSERT_EQ(delete_version_as(alice, "k", vid), 0) << vid;
  }
  ASSERT_TRUE(listed_elsewhere().empty());

  const Owned v = owned(".versions");
  ASSERT_EQ(v.uid, owner_uid)
      << "the premise:  the scaffolding is the writer's";
  ASSERT_EQ(v.mode, 0770u);
  ASSERT_EQ(gateway_unlink_on_a_fresh_thread(".versions/.lock"), -EACCES)
      << "the premise:  the gateway, unraised, cannot remove it";

  int ret = 0;
  bool still_raised = true;
  std::thread remover([&] {
    ret = bucket->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user);
    still_raised = nsfs::capability_is_effective(CAP_DAC_OVERRIDE);
  });
  remover.join();
  ASSERT_EQ(ret, 0) << cpp_strerror(-ret);
  EXPECT_FALSE(still_raised) << "the delete left DAC_OVERRIDE raised";
  EXPECT_EQ(owned("").err, -ENOENT) << "the bucket directory stayed";
}


/* FSIO in a key directory an identity made (n plan D4a).  Under
 * impersonation the directory is its owner's at 0770, so a handle
 * opened as the gateway cannot make `.shadow` in it, and FSIO fails on
 * every key directory an identity made.  D4a carries librgw's mount
 * user -- here the owner -- through the handle, and `.shadow` is made
 * as `.versions` is:  the owner's at 0770. */
TEST_F(NSFSImpersonatedRenameTest, FSIOWritesInTheOwnersKeyDirectory)
{
  /* on a thread of its own, as put_as():  the profile change takes
   * read-search for the thread it runs on, which would spoil the
   * control below */
  int hret = 0, lret = 0;
  std::thread raiser([&] {
    std::map<std::string, std::string> out;
    hret = driver->driver_hint(env->dpp, "bucket-profile",
			       {{"bucket", testname}, {"profile", "strong"}},
			       &out);
    lret = driver->load_bucket(env->dpp, rgw_bucket{"", testname}, &bucket,
			       null_yield);
  });
  raiser.join();
  ASSERT_EQ(hret, 0);
  ASSERT_EQ(lret, 0);
  ASSERT_TRUE(static_cast<rgw::sal::NSFSBucket*>(bucket.get())
		  ->has_extension(nsfs::EXT_SHADOW));

  ASSERT_NO_FATAL_FAILURE(put_as(alice, "src/k", "first"));
  ASSERT_EQ(owned("src").uid, owner_uid);
  ASSERT_EQ(gateway_open_on_a_fresh_thread("src"), -EACCES)
      << "the gateway could walk the owner's key directory itself, so "
	 "this does not show whose credentials FSIO ran with";

  int rc = 0, crc = 0, prc = 0;
  std::thread fsio([&] {
    auto obj = bucket->get_object(rgw_obj_key("src/k"));
    auto [r, hdl] = obj->get_fsio_handle(
	env->dpp, alice, rgw::sal::Object::FSIOObject::OPEN_FLAG_WRITE);
    rc = r;
    if (r == 0) {
      crc = hdl->commit(env->dpp,
			rgw::sal::Object::FSIOObject::COMMIT_FLAG_NONE);
      prc = hdl->publish(env->dpp,
			 rgw::sal::Object::FSIOObject::PUBLISH_FLAG_NONE);
      hdl->close(env->dpp, rgw::sal::Object::FSIOObject::CLOSE_FLAG_NONE);
    }
  });
  fsio.join();

  ASSERT_EQ(rc, 0) << "FSIO could not open the owner's object to write: "
		   << cpp_strerror(-rc);
  EXPECT_EQ(crc, 0) << "commit";
  EXPECT_EQ(prc, 0) << "publish";
  const Owned shadow = owned("src/.shadow");
  ASSERT_EQ(shadow.err, 0) << "no .shadow in the owner's key directory";
  EXPECT_EQ(shadow.uid, owner_uid) << ".shadow is not the owner's";
  EXPECT_EQ(shadow.mode, 0770u) << ".shadow is not made as .versions is";
}

/* Lifecycle (n plan Phase F):  work no request asked for carries no
 * authenticated user, and names whom it is done for -- the bucket's
 * owner -- in `on_behalf_of`.  These build the operations as
 * rgw_lc.cc does, with the identity under test named directly. */
static const rgw_user lifecycle_nobody{"", "lc-nobody-has-a-row"};

/* A lifecycle delete runs as whom it is for, and refuses one with no
 * row rather than running as the gateway. */
TEST_F(NSFSImpersonatedRenameTest, ALifecycleDeleteRunsAsWhomItIsFor)
{
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "src/k", "alice's"));
  auto lc_delete = [this](const rgw_user& for_whom) {
    int ret = 0;
    std::thread t([&] {
      auto obj = bucket->get_object(rgw_obj_key("src/k"));
      auto del = obj->get_delete_op(rgw::sal::no_authenticated_user);
      del->params.versioning_status = bucket->get_info().versioning_status();
      del->params.bucket_owner = rgw_owner{for_whom};
      del->params.on_behalf_of = rgw_owner{for_whom};
      ret = del->delete_obj(env->dpp, null_yield, 0);
    });
    t.join();
    return ret;
  };

  const int nret = lc_delete(lifecycle_nobody);
  EXPECT_EQ(nret, -EACCES) << "work for an owner with no row was not "
      "refused: " << cpp_strerror(-nret);
  EXPECT_EQ(owned("src/k").err, 0) << "the refused delete removed the object";

  std::map<std::string, std::string> out;
  ASSERT_EQ(driver->driver_hint(env->dpp, "ran-as",
				{{"select", alice.to_str()}}, &out), 0);
  const int ret = lc_delete(alice);
  ASSERT_EQ(ret, 0) << "the delete failed: " << cpp_strerror(-ret);
  out.clear();
  EXPECT_EQ(driver->driver_hint(env->dpp, "ran-as", {}, &out), 0);
  EXPECT_EQ(out["ran_as_delete"], std::to_string(owner_uid))
      << "the delete did not run as whom it was for";
  EXPECT_EQ(owned("src/k").err, -ENOENT) << "the current version stayed";
}


/* A lifecycle abort runs as whom it is for:  one with no row is
 * refused and the upload stays, where an abort for nobody would run
 * as the gateway (the bucket-delete path's). */
TEST_F(NSFSImpersonatedRenameTest, ALifecycleAbortRunsAsWhomItIsFor)
{
  const std::string objname = "mpk";
  rgw_placement_rule placement;
  {
    auto upload = bucket->get_multipart_upload(objname, alice, "abcdef");
    std::thread t([&] {
      Attrs attrs;
      ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement,
			     attrs), 0);
      auto w = upload->get_writer(env->dpp, null_yield, nullptr, acl_owner,
				  &placement, 1, "1", alice);
      ASSERT_EQ(w->prepare(null_yield), 0);
      bufferlist bl;
      bl.append("part one");
      const size_t len = bl.length();
      ASSERT_EQ(w->process(std::move(bl), 0), 0);
      ASSERT_EQ(w->process({}, len), 0);
      ceph::real_time mtime;
      Attrs pa;
      req_context rctx{env->dpp, null_yield, nullptr};
      std::string etag = "1";
      ASSERT_EQ(w->complete(len, etag, &mtime, real_time(), pa, std::nullopt,
			    real_time(), nullptr, nullptr, nullptr, nullptr,
			    nullptr, rctx, 0), 0);
    });
    t.join();
    ASSERT_FALSE(HasFatalFailure());
  }
  const std::string staging = ".multipart_" + objname + ".abcdef";
  ASSERT_EQ(owned(staging).err, 0) << "the upload was not staged";

  auto lc_abort = [&](const rgw_user& for_whom) {
    int ret = 0;
    std::thread t([&] {
      auto u = bucket->get_multipart_upload(objname,
					    rgw::sal::no_authenticated_user,
					    "abcdef");
      u->set_on_behalf_of(rgw_owner{for_whom});
      ret = u->abort(env->dpp, env->cct.get(), null_yield);
    });
    t.join();
    return ret;
  };

  const int nret = lc_abort(lifecycle_nobody);
  EXPECT_EQ(nret, -EACCES) << "work for an owner with no row was not "
      "refused: " << cpp_strerror(-nret);
  EXPECT_EQ(owned(staging).err, 0) << "the refused abort removed the upload";

  const int ret = lc_abort(alice);
  ASSERT_EQ(ret, 0) << "the abort failed: " << cpp_strerror(-ret);
  EXPECT_EQ(owned(staging).err, -ENOENT) << "the upload stayed";
}

/* Lifecycle's tag read calls get_attr() with no prepare():  it reads as
 * whom it is for, so posix-strict denies it an object the filesystem
 * denies that identity, and one with no row is refused -- where the
 * read used to run as the gateway, which reads everything. */
TEST_F(NSFSImpersonatedRenameTest, ALifecycleTagReadRunsAsWhomItIsFor)
{
  ASSERT_NO_FATAL_FAILURE(put_as(alice, "src/k", "alice's"));
  ASSERT_EQ(set_model("posix-strict"), 0);
  auto lc_read = [this](const rgw_user& for_whom) {
    int ret = 0;
    std::thread t([&] {
      auto obj = bucket->get_object(rgw_obj_key("src/k"));
      auto rop = obj->get_read_op(rgw::sal::no_authenticated_user);
      rop->params.on_behalf_of = rgw_owner{for_whom};
      bufferlist bl;
      ret = rop->get_attr(env->dpp, RGW_ATTR_ETAG, bl, null_yield);
    });
    t.join();
    return ret;
  };

  const int aret = lc_read(alice);
  EXPECT_EQ(aret, 0) << "the owner could not read its own object's "
      "attributes: " << cpp_strerror(-aret);

  const int nret = lc_read(lifecycle_nobody);
  EXPECT_EQ(nret, -EACCES) << "work for an owner with no row was not "
      "refused: " << cpp_strerror(-nret);

  const int bret = lc_read(bob);
  EXPECT_NE(bret, 0) << "posix-strict read another identity's object's "
      "attributes, as the gateway would";
}


/* NooBaa's attribute names.
 *
 * Spelled out here rather than derived from the strategy, for the same
 * reason the tree fixtures are:  a test which asked the strategy what
 * it calls things would agree with it by construction.  These are read
 * from noobaa-core 68ca22d33, the constant block at the head of
 * `namespace_fs.js` and `to_fs_xattr()`.
 */

TEST(NooBaaXattr, NamesNooBaaAttributes)
{
  nsfs::NooBaaXattrStrategy nb;

  EXPECT_EQ(nb.disk_name(RGW_ATTR_ETAG), "user.content_md5");
  EXPECT_EQ(nb.disk_name(RGW_ATTR_CONTENT_TYPE), "user.noobaa.content_type");
  EXPECT_EQ(nb.disk_name(RGW_ATTR_CONTENT_ENC),
	    "user.noobaa.content_encoding");
  EXPECT_EQ(nb.disk_name("version_id"), "user.noobaa.version_id");
  EXPECT_EQ(nb.disk_name("delete_marker"), "user.noobaa.delete_marker");
  EXPECT_EQ(nb.disk_name("non_current_timestamp"),
	    "user.noobaa.non_current_timestamp");

  /* user metadata key for key */
  EXPECT_EQ(nb.disk_name(std::string(RGW_ATTR_META_PREFIX) + "colour"),
	    "user.colour");
}

/* Every mapped key survives the round trip, which is the property the
 * two functions have to hold together and which spelling them out
 * separately does not guarantee. */
TEST(NooBaaXattr, NamesRoundTrip)
{
  nsfs::NooBaaXattrStrategy nb;

  for (const std::string k : {std::string(RGW_ATTR_ETAG),
			      std::string(RGW_ATTR_CONTENT_TYPE),
			      std::string(RGW_ATTR_CONTENT_ENC),
			      std::string("version_id"),
			      std::string("delete_marker"),
			      std::string("non_current_timestamp"),
			      std::string(RGW_ATTR_META_PREFIX) + "colour",
			      std::string("bucket_info")}) {
    const std::string disk = nb.disk_name(k);
    ASSERT_FALSE(disk.empty()) << k;
    std::string back;
    EXPECT_TRUE(nb.parse_disk_name(disk, back)) << disk;
    EXPECT_EQ(back, k) << disk;
  }
}

/* PrefixedXattrStrategy -- what shared and strong both resolve to --
 * returns the owner the recorded ACL holds.  A file written by cp or
 * over NFS has none, for which it returns -ENODATA and the caller gives
 * it the bucket's owner;  an ACL that will not decode returns -EIO
 * (DECISIONS.md 2026-10-11). */
TEST(PrefixedXattr, OwnerIsRecordedOrNone)
{
  nsfs::PrefixedXattrStrategy prefixed;
  const rgw::sal::Attrs none;

  ACLOwner owner;
  EXPECT_EQ(prefixed.object_owner(none, owner), -ENODATA)
      << "a file with no ACL did not read as recording no owner";

  rgw::sal::Attrs with;
  RGWAccessControlPolicy policy;
  policy.create_default(rgw_user("someone"), "Someone");
  bufferlist bl;
  policy.encode(bl);
  with[RGW_ATTR_ACL] = bl;
  ASSERT_EQ(prefixed.object_owner(with, owner), 0);
  EXPECT_EQ(owner.id, rgw_owner(rgw_user("someone")));

  rgw::sal::Attrs broken;
  broken[RGW_ATTR_ACL].append("not an acl");
  EXPECT_EQ(prefixed.object_owner(broken, owner), -EIO)
      << "an unreadable ACL read as no owner recorded";
}

/* Three results, and the third is the one worth pinning.  An ACL has
 * nowhere to go, so the name is empty and the write path skips it --
 * which is what NooBaa's own gateway does.  An nsfs key with no
 * counterpart lands under user.nsfs:, inert to NooBaa. */
TEST(NooBaaXattr, WhatHasNowhereToGo)
{
  nsfs::NooBaaXattrStrategy nb;

  EXPECT_TRUE(nb.disk_name(RGW_ATTR_ACL).empty());

  EXPECT_EQ(nb.disk_name("bucket_info"), "user.nsfs:bucket_info");
  EXPECT_EQ(nb.disk_name("rename_intent"), "user.nsfs:rename_intent");

  /* and not the wrong third result:  never the key unchanged, which
   * would write under a name neither format owns */
  EXPECT_NE(nb.disk_name("rename_intent"), "rename_intent");
}

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

/* NooBaa's structured attributes are claimed as unmapped rather than
 * surfaced.  Handing a caller one of NooBaa's plain strings under an RGW
 * key would feed a NooBaa value to a ceph decoder. */
TEST(NooBaaXattr, LeavesNooBaaStructuredAttributesAlone)
{
  nsfs::NooBaaXattrStrategy nb;
  std::string key;

  EXPECT_FALSE(nb.parse_disk_name("user.noobaa.tag.project", key));
  EXPECT_FALSE(nb.parse_disk_name("user.noobaa.legal_hold", key));
  EXPECT_FALSE(nb.parse_disk_name("user.noobaa.retention_mode", key));
  EXPECT_FALSE(nb.parse_disk_name("user.noobaa.part_size", key));

  /* nor anything outside the namespaces either format owns */
  EXPECT_FALSE(nb.parse_disk_name("security.selinux", key));
  EXPECT_FALSE(nb.parse_disk_name("trusted.something", key));
}

/* NooBaa's format records no owner.  NooBaa returns the bucket's
 * `owner_account` for every object, so this returns -ENODATA and the
 * caller supplies the bucket's owner -- never the file's uid
 * (DECISIONS.md 2026-10-11). */
TEST(NooBaaXattr, RecordsNoOwner)
{
  nsfs::NooBaaXattrStrategy nb;
  Attrs none;
  ACLOwner owner;
  EXPECT_EQ(nb.object_owner(none, owner), -ENODATA);
}

/* Never counted.  The trailing NUL is an RGW-side accident the nsfs
 * writers append;  nothing in NooBaa's tree carries one. */
TEST(NooBaaXattr, ValuesAreNotCountedStrings)
{
  nsfs::NooBaaXattrStrategy nb;
  nsfs::PrefixedXattrStrategy prefixed;

  EXPECT_FALSE(nb.counted_string_value(
      std::string(RGW_ATTR_META_PREFIX) + "colour"));
  EXPECT_FALSE(nb.counted_string_value(RGW_ATTR_CONTENT_TYPE));

  /* and the nsfs strategy does count at least one, so this is a
   * difference rather than a predicate nobody implements */
  EXPECT_TRUE(prefixed.counted_string_value(
      std::string(RGW_ATTR_META_PREFIX) + "colour"));
}

/* Trees generated by hand, in a named format, for a driver to operate
 * on.
 *
 * Written out literally rather than through the format's own
 * MPUStrategy.  A fixture which asked the strategy for the spellings
 * would agree with it by construction, and the test would confirm only
 * that the strategy is self-consistent.  These say what the format is,
 * and the strategy has to match them.
 *
 * NooBaa's shapes, from noobaa-core 68ca22d33:  the bucket temp
 * directory is created for ordinary object writes as well as for
 * uploads (`namespace_fs.js:1303`, `:1569`), and `multipart-uploads/`
 * beneath it only by create_object_upload.  So a bucket can be in any
 * of three states, and only the last has a staging root to find.
 */
enum class TreeShape {
  empty,       /* the bucket directory and nothing else */
  quiescent,   /* written to, but no upload has ever been started */
  with_upload, /* one upload in flight */
};

struct TestTree {
  std::string key;          /* the object the upload is for */
  std::string upload_id;
  uint64_t part_size{0};
  uint32_t parts{0};
  std::string payload;      /* what completing it should produce */
  sf::path staging;         /* the upload's directory, when there is one */
};


void write_file(const sf::path& p, const std::string& data, uint64_t offset = 0)
{
  int fd = ::open(p.c_str(), O_WRONLY | O_CREAT, 0600);
  ASSERT_GE(fd, 0) << p;
  ASSERT_EQ(::pwrite(fd, data.data(), data.size(), offset),
	    static_cast<ssize_t>(data.size()));
  ::close(fd);
}

void set_u64_xattr(const sf::path& p, const std::string& name, uint64_t v)
{
  const std::string val = std::to_string(v);
  ASSERT_EQ(::setxattr(p.c_str(), name.c_str(), val.data(), val.size(), 0), 0)
      << name << " on " << p;
}

/* NooBaa's layout */
void make_noobaa_tree(const sf::path& bucket, TreeShape shape, TestTree& out)
{
  sf::create_directories(bucket);
  if (shape == TreeShape::empty) {
    return;
  }

  const sf::path tmpdir = bucket / (".noobaa-nsfs_" + fake_uuid("bucket"));
  sf::create_directories(tmpdir);
  if (shape == TreeShape::quiescent) {
    return;
  }

  out.key = "some/key.bin";
  out.upload_id = fake_uuid("upload");
  out.part_size = 64;
  out.parts = 3;
  out.staging = tmpdir / "multipart-uploads" / out.upload_id;
  sf::create_directories(out.staging);

  write_file(out.staging / "create_object_upload",
	     "{\"key\":\"" + out.key + "\",\"storage_class\":null}");

  const sf::path shared =
      out.staging / ("parts-size-" + std::to_string(out.part_size));
  for (uint32_t k = 1; k <= out.parts; ++k) {
    const std::string payload(out.part_size, static_cast<char>('a' + k));
    write_file(shared, payload, (k - 1) * out.part_size);
    out.payload += payload;

    const sf::path rec = out.staging / ("part-" + std::to_string(k));
    write_file(rec, "");
    set_u64_xattr(rec, "user.noobaa.part_size", out.part_size);
    set_u64_xattr(rec, "user.noobaa.part_offset", (k - 1) * out.part_size);
  }
}

/* The nsfs strategy.  There is no counterpart to `quiescent`:  nsfs
 * keeps no bucket temp directory, so a bucket which has been written
 * to and has no upload is indistinguishable from one which has not.
 * The shape is accepted and does nothing, which is the difference
 * stated rather than hidden. */
void make_nsfs_tree(const sf::path& bucket, TreeShape shape, TestTree& out)
{
  sf::create_directories(bucket);
  if (shape != TreeShape::with_upload) {
    return;
  }

  out.key = "some/key.bin";
  out.upload_id = "2~" + fake_uuid("upload").substr(0, 16);
  out.part_size = 64;
  out.parts = 3;

  const std::string meta = out.key + "." + out.upload_id;
  out.staging = bucket / (".multipart_" + url_encode(meta, true));
  sf::create_directories(out.staging);
  write_file(out.staging / ".meta", "");

  for (uint32_t k = 1; k <= out.parts; ++k) {
    const std::string payload(out.part_size, static_cast<char>('a' + k));
    write_file(out.staging / fmt::format("part-{:0>5}", k), payload);
    out.payload += payload;
  }
}

using make_tree_fn_t = void (*)(const sf::path&, TreeShape, TestTree&);

/* The three shapes, against the strategy that has to read them.
 *
 * A staging root exists only where an upload does:  NooBaa creates
 * `multipart-uploads/` at the first create_object_upload, and the temp
 * directory above it earlier, for ordinary writes.  Reporting a root
 * for a bucket which has none would send an enumeration somewhere that
 * does not exist. */
TEST(NooBaaMPU, StagingRootPerTreeShape)
{
  nsfs::NooBaaMPUStrategy nb;
  const sf::path root{base_path / "nbshapes"};

  struct {
    TreeShape shape;
    const char* label;
    bool expect_root;
  } cases[] = {
    {TreeShape::empty,       "empty",     false},
    {TreeShape::quiescent,   "quiescent", false},
    {TreeShape::with_upload, "upload",    true},
  };

  for (auto& c : cases) {
    TestTree tree;
    const sf::path b{root / c.label};
    sf::remove_all(b);
    make_noobaa_tree(b, c.shape, tree);

    int fd = ::open(b.c_str(), O_RDONLY | O_DIRECTORY);
    ASSERT_GE(fd, 0) << c.label;
    auto got = nb.staging_root(env->dpp, fd);
    ::close(fd);

    EXPECT_EQ(got.has_value(), c.expect_root) << c.label;
    if (c.expect_root) {
      EXPECT_TRUE(sf::is_directory(b / *got)) << c.label << ": " << *got;
      EXPECT_EQ(b / *got, tree.staging.parent_path()) << c.label;
    }
  }
}

/* The tree a fixture generated is the tree the strategy reads.
 *
 * The builder spells NooBaa's layout out literally and the strategy
 * derives it, so agreement between them is a real comparison rather
 * than a tautology.  This is what a captured tree would eventually
 * replace. */
TEST(NooBaaMPU, StrategyReadsTheGeneratedTree)
{
  nsfs::NooBaaMPUStrategy nb;
  const sf::path b{base_path / "nbread"};
  sf::remove_all(b);
  TestTree tree;
  make_noobaa_tree(b, TreeShape::with_upload, tree);

  int fd = ::open(b.c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(fd, 0);
  auto root = nb.staging_root(env->dpp, fd);
  ::close(fd);
  ASSERT_TRUE(root.has_value());

  /* the directory is named for the upload id alone, and the key comes
   * from the file inside -- which is the whole difference between the
   * two layouts' listing costs */
  EXPECT_EQ(nb.staging_dir_name(tree.key + "." + tree.upload_id),
	    tree.upload_id);

  int rfd = ::open((b / *root).c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(rfd, 0);
  nsfs::MPUStrategy::StagedUpload su;
  EXPECT_TRUE(nb.staged_upload(env->dpp, rfd, tree.upload_id, su));
  EXPECT_EQ(su.key, tree.key);
  EXPECT_EQ(su.upload_id, tree.upload_id);
  /* an entry which is not an upload is skipped, not an error */
  EXPECT_FALSE(nb.staged_upload(env->dpp, rfd, "not-an-upload", su));
  ::close(rfd);

  /* every name the strategy renders is one the fixture wrote */
  EXPECT_TRUE(sf::exists(tree.staging / nb.meta_name()));
  for (uint32_t k = 1; k <= tree.parts; ++k) {
    EXPECT_TRUE(sf::exists(tree.staging / nb.part_name(k))) << k;
  }
  /* the bytes, found as conversion finds them */
  auto sn = nb.shared_name(tree.part_size);
  ASSERT_TRUE(sn.has_value());
  EXPECT_TRUE(sf::exists(tree.staging / *sn)) << *sn;

  /* and the nsfs strategy misreads nothing:  NooBaa's directory is
   * not an nsfs one, NooBaa's record names are not nsfs names, and
   * asked directly it returns no upload rather than a wrong one */
  nsfs::StridedMPUStrategy strided;
  EXPECT_FALSE(strided.is_staging_dir(tree.upload_id));
  EXPECT_FALSE(sf::exists(tree.staging / strided.meta_name()));
  EXPECT_FALSE(sf::exists(tree.staging / strided.part_name(1)));

  int ofd = ::open((b / *root).c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(ofd, 0);
  nsfs::MPUStrategy::StagedUpload none;
  EXPECT_FALSE(strided.staged_upload(env->dpp, ofd, tree.upload_id, none));
  ::close(ofd);
}

/* NooBaa's path conventions.
 *
 * Most of them are the nsfs ones -- keys verbatim, `.folder`, `.versions` --
 * and asserting the sameness is the point:  it records that the
 * agreement is deliberate rather than a coincidence nobody may rely
 * on.  What differs is asserted against a tree written by hand.
 */
TEST(NooBaaPath, KeyConventionsMatchSentinel)
{
  nsfs::NooBaaPathStrategy nb;
  nsfs::SentinelPathStrategy sentinel;

  for (const char* k : {"plain", "some/deep/key.bin", "_leading_underscore",
			"__doubled", "trailing/"}) {
    rgw_obj_key key{k};
    EXPECT_EQ(nb.object_name(key, false), sentinel.object_name(key, false))
      << k;
  }

  /* The two formats' directory-object file names are equal, and since
   * they stopped being one constant that is a comparison rather than
   * a tautology.  They are equal by inheritance -- `61de07d3bf2` took
   * `.folder` from NooBaa, and posix has no such name -- so this
   * asserts an agreement that either side may end, not an invariant.
   * If it ever fails, the question to ask is which side moved and
   * whether the other should. */
  EXPECT_EQ(nb.folder_object_name(), sentinel.folder_object_name());
  EXPECT_EQ(nb.folder_object_name(), ".folder");
  EXPECT_EQ(nb.object_name(rgw_obj_key{"photos/"}, false), "photos/.folder");
  EXPECT_TRUE(nb.names_directory_object(".folder"));
  EXPECT_TRUE(sentinel.names_directory_object(".folder"));
  EXPECT_EQ(nb.key_from_name("some/key").name, "some/key");
}

/* NooBaa's staging is under the bucket temp directory, so nothing beside
 * the bucket is named for an upload and a namespace has nothing to
 * render.  The nsfs strategy spells one, which is the difference. */
TEST(NooBaaPath, NoStagingSpelling)
{
  nsfs::NooBaaPathStrategy nb;
  nsfs::SentinelPathStrategy sentinel;
  std::optional<std::string> mp{"multipart"};

  EXPECT_EQ(nb.bucket_dir_name("b", mp), "b");
  EXPECT_EQ(sentinel.bucket_dir_name("b", mp), ".multipart_b");
  EXPECT_EQ(nb.bucket_dir_name("b", std::nullopt), "b");
}

/* No .shadow -- they have none, and claiming the name would hide a
 * directory a user may create -- and the temp directory reserved by
 * prefix, since its name carries a bucket id. */
TEST(NooBaaPath, ReservesNooBaaNamesAndNotSentinels)
{
  nsfs::NooBaaPathStrategy nb;
  const auto& rn = nb.reserved_names();

  auto has_exact = [&](const char* n) {
    return std::find(rn.exact.begin(), rn.exact.end(), n) != rn.exact.end();
  };
  auto has_prefix = [&](const char* n) {
    return std::find(rn.prefixes.begin(), rn.prefixes.end(), n) !=
	   rn.prefixes.end();
  };

  EXPECT_TRUE(has_exact(".versions"));
  EXPECT_TRUE(has_exact(".folder"));
  EXPECT_FALSE(has_exact(".shadow"));
  EXPECT_TRUE(has_prefix(".noobaa-nsfs_"));

  /* and the nsfs strategy does claim .shadow, so this is a difference
   * rather than an empty set */
  nsfs::SentinelPathStrategy sentinel;
  const auto& orn = sentinel.reserved_names();
  EXPECT_NE(std::find(orn.exact.begin(), orn.exact.end(), ".shadow"),
	    orn.exact.end());
}

/* A NooBaa directory object, which is the interop case:  NooBaa's
 * empty one has no sentinel at all, so nothing in the directory
 * listing reveals it and the question has to be asked of the
 * directory. */
TEST(NooBaaPath, DirectoryObjectFromTheAttribute)
{
  nsfs::NooBaaPathStrategy nb;
  nsfs::SentinelPathStrategy sentinel;
  const sf::path b{base_path / "nbdirobj"};
  sf::remove_all(b);
  sf::create_directories(b / "empty");
  sf::create_directories(b / "withcontent");
  sf::create_directories(b / "plain");

  /* "0":  the directory alone is the object */
  set_u64_xattr(b / "empty", "user.noobaa.dir_content", 0);
  /* non-zero:  the bytes are in the sentinel */
  set_u64_xattr(b / "withcontent", "user.noobaa.dir_content", 17);
  write_file(b / "withcontent" / ".folder", std::string(17, 'x'));

  /* the directory's own descriptor, which is what the listing walk
   * holds by the time it asks */
  auto dfd = [&b](const char* n) {
    int fd = ::open((b / n).c_str(), O_RDONLY | O_DIRECTORY);
    EXPECT_GE(fd, 0) << n;
    return fd;
  };

  nsfs::PathStrategy::DirectoryObject d{};
  int fd = dfd("empty");
  ASSERT_TRUE(nb.directory_object(env->dpp, fd, d));
  EXPECT_EQ(d.size, 0u);
  EXPECT_FALSE(d.content_in_sentinel);
  /* the nsfs strategy never marks a directory, and says so without looking */
  nsfs::PathStrategy::DirectoryObject od{};
  EXPECT_FALSE(sentinel.directory_object(env->dpp, fd, od));
  ::close(fd);

  d = {};
  fd = dfd("withcontent");
  ASSERT_TRUE(nb.directory_object(env->dpp, fd, d));
  EXPECT_EQ(d.size, 17u);
  EXPECT_TRUE(d.content_in_sentinel);
  ::close(fd);

  /* a directory without the attribute is not an object;  NooBaa's own
   * read path throws NoSuchKey for one */
  d = {};
  fd = dfd("plain");
  EXPECT_FALSE(nb.directory_object(env->dpp, fd, d));
  ::close(fd);
}

/* A bucket's format, and the strategies it selects.
 *
 * One format exists, so every profile names it and the bucket's
 * accessors agree with the driver's.  That coincidence is the whole
 * assertion for now -- a null format, an unpopulated member or an
 * accessor wired to the wrong driver object all break it -- and it
 * stops coinciding when S5 gives base NooBaa's format, at which point
 * these become real comparisons.
 */
TEST_F(NSFSBucketTest, FormatSelectsTheDriversStrategies)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);

  const nsfs::Format* f = b->get_format();
  ASSERT_NE(f, nullptr);
  EXPECT_STREQ(f->name(), "rgw-meta");

  EXPECT_EQ(b->xattr_strategy(), driver->get_xattr_strategy());
  EXPECT_EQ(b->path_strategy(), driver->get_path_strategy());

  /* the format names no staging layout, so the bucket takes the one
   * the filesystem chose */
  EXPECT_EQ(f->mpu_strategy, nullptr);
  EXPECT_EQ(b->mpu_strategy(), driver->get_mpu_strategy());
}

/* A format which names its own staging layout is taken over the
 * driver's.  Composed here rather than shipped:  a format carrying
 * NooBaa's staging with nsfs attribute names is a combination no tree
 * is in, and the driver must not hold one -- but it is what gives the
 * selection something to be wrong about while one real format exists.
 */
TEST_F(NSFSBucketTest, FormatOverridesTheStagingLayout)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  ASSERT_EQ(b->mpu_strategy(), driver->get_mpu_strategy());

  nsfs::NooBaaMPUStrategy nb;
  nsfs::Format composed{};
  composed.xattr_strategy = driver->get_xattr_strategy();
  composed.path_strategy = driver->get_path_strategy();
  composed.mpu_strategy = &nb;
  composed.fname = "composed";

  nsfs::BucketProfile prof{};
  prof.extensions = nsfs::EXTENSIONS_STRONG;
  prof.format = &composed;
  b->set_profile_for_test(&prof);

  EXPECT_STREQ(b->get_format()->name(), "composed");
  EXPECT_EQ(b->mpu_strategy(), &nb);
  EXPECT_STREQ(b->mpu_strategy()->name(), "noobaa");
  EXPECT_NE(b->mpu_strategy(), driver->get_mpu_strategy());

  /* the members it does not override still return the driver's */
  EXPECT_EQ(b->xattr_strategy(), driver->get_xattr_strategy());
}

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

/* The impersonated fixture with the strided staging layout forced, as
 * NSFSStridedBucketTest forces it, so an oversized part diverts. */
class NSFSImpersonatedStridedTest : public NSFSImpersonatedRenameTest {
public:
  std::optional<bool> shares_extents() const override { return false; }
};

/* copy_range() moves bytes between descriptors already opened, so a copy runs
 * as whoever opened them:  the identity reaches the copies only through
 * set_identity() calls in NSFSMultipartWriter, far from them (call-site table
 * §9.2, row 1).  So every file a strided upload makes -- the shared file, part
 * 2's own file after its divert, and the object assembly produces -- is the
 * uploader's;  and the same upload by another identity makes them that
 * identity's, so the first case discriminates.  Not the gateway as the other
 * case:  under impersonation a part with no authenticated user is denied
 * (`RGW_NSFS_VADE_MECUM.md` §12, "No authenticated user means refuse").  The
 * upload's `.meta` is the gateway's by design -- staging stays its -- so the
 * data files are named rather than listed. */
TEST_F(NSFSImpersonatedStridedTest, EveryFileAStridedUploadMakesIsTheUploaders)
{
  ASSERT_STREQ(driver->get_mpu_strategy()->name(), "rgw-strided");
  const size_t stride = 64;
  const uint64_t diverts = driver->mpu_diverts_seen();

  /* unversioned:  the fixture versions its bucket, and then the second
   * uploader's completion needs the bucket root's .versions, which the
   * first made its own at 0770 -- the cross-identity case, not the
   * copy this asserts */
  std::thread unversion([&] {
    bucket->get_info().flags &= ~BUCKET_VERSIONED;
    ASSERT_EQ(bucket->put_info(env->dpp, false, real_time(), null_yield), 0);
  });
  unversion.join();
  ASSERT_FALSE(HasFatalFailure());

  /* one upload of three parts, the second oversized, as `who`, each
   * step on a thread of its own, as put_as() */
  auto upload_as = [&](const rgw_user& who, uid_t uid,
		       const std::string& objname) {
    auto upload = bucket->get_multipart_upload(objname, who, "c0ffee");
    rgw_placement_rule placement;
    std::map<int, std::string> etags;
    std::thread t([&] {
      Attrs attrs;
      ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement,
			     attrs), 0);
      const std::string bodies[] = {std::string(stride, 'a'),
				    std::string(stride + 17, 'b'),
				    std::string(stride, 'c')};
      for (int n = 1; n <= 3; ++n) {
	auto w = upload->get_writer(env->dpp, null_yield, nullptr, acl_owner,
				    &placement, n, std::to_string(n), who);
	ASSERT_EQ(w->prepare(null_yield), 0) << "part " << n;
	bufferlist bl;
	bl.append(bodies[n - 1]);
	const size_t len = bl.length();
	ASSERT_EQ(w->process(std::move(bl), 0), 0);
	ASSERT_EQ(w->process({}, len), 0);
	ceph::real_time mtime;
	Attrs pa;
	req_context rctx{env->dpp, null_yield, nullptr};
	std::string etag = std::to_string(n);
	ASSERT_EQ(w->complete(len, etag, &mtime, real_time(), pa, std::nullopt,
			      real_time(), nullptr, nullptr, nullptr, nullptr,
			      nullptr, rctx, 0), 0) << "part " << n;
	etags[n] = etag;
      }
    });
    t.join();
    ASSERT_FALSE(HasFatalFailure());

    /* the shared file and part 2's own file, which divert() copied
     * into, are the uploader's, before completion */
    const std::string staging = ".multipart_" + objname + ".c0ffee/";
    const auto* mpu = driver->get_mpu_strategy();
    for (const std::string& name : {*mpu->shared_name(stride),
				    mpu->part_name(2)}) {
      const Owned o = owned(staging + name);
      ASSERT_EQ(o.err, 0) << name << " is missing";
      EXPECT_EQ(o.uid, uid) << name << " is not its uploader's";
    }

    /* and the object assembly makes from them */
    std::thread c([&] {
      EXPECT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			    etags), 0);
    });
    c.join();
    EXPECT_EQ(owned(objname).uid, uid)
	<< "the assembled object is not its uploader's";
  };

  ASSERT_NO_FATAL_FAILURE(upload_as(alice, owner_uid, "mp"));
  EXPECT_GT(driver->mpu_diverts_seen(), diverts)
      << "part 2 did not divert, so divert()'s copy was not exercised";
  ASSERT_NO_FATAL_FAILURE(upload_as(bob, other_uid, "mp-bob"));
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

namespace {

std::string str_xattr(const sf::path& p, const char* name)
{
  char buf[64];
  ssize_t len = ::getxattr(p.c_str(), name, buf, sizeof(buf));
  return (len > 0) ? std::string(buf, len) : std::string{};
}

/* an upload of three parts, the last one short, through the SAL */
void short_tail_upload(rgw::sal::Bucket* bucket, ACLOwner& acl_owner,
		       const rgw_owner& owner, const std::string& objname,
		       size_t part_size, size_t tail_size,
		       std::string& expected)
{
  auto upload = bucket->get_multipart_upload(
      objname,
      rgw::sal::no_authenticated_user, "11111111-2222-4333-8444-555555555555");
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  std::map<int, std::string> part_etags;
  for (int i = 1; i <= 3; ++i) {
    const size_t len = (i == 3) ? tail_size : part_size;
    std::string payload(len, static_cast<char>('a' + i));
    part_etags[i] = write_mp_part(upload.get(), acl_owner, &placement, i,
				  payload);
    expected += payload;
  }
  ASSERT_EQ(complete_mp(bucket, upload.get(), owner, objname, part_etags), 0);
}

} /* anonymous namespace */

/* The nsfs layout does not move a short part.
 *
 * Assembly reads every shared part out of the file the stride names,
 * at the offset in its record, so the part is already where it can be
 * read from and a second file would only cost a copy.  The negative
 * case for the test below it.
 */
TEST_F(NSFSStridedBucketTest, ShortFinalPartStaysInTheStrideFile)
{
  const std::string objname = "short-tail.bin";
  const size_t stride = 64;
  const size_t tail = 17;
  std::string expected;
  short_tail_upload(bucket.get(), acl_owner, owner, objname, stride, tail,
		    expected);

  EXPECT_EQ(read_all(bp / "root" / testname / objname), expected);

  /* the staging directory is gone, so what is asserted is the absence
   * of the second file while it existed -- the upload completed, which
   * it could not have done had the bytes been moved somewhere this
   * layout's assembly does not read */
  EXPECT_FALSE(sf::exists(bp / "root" / testname /
			  (".multipart_" + objname +
			   ".11111111-2222-4333-8444-555555555555")));
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

class NSFSNooBaaBucketTest : public NSFSBucketTest {
public:
  void SetUp() override {
    NSFSBucketTest::SetUp();
    auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
    ASSERT_STREQ(b->profile_name(), "base");
    ASSERT_STREQ(b->get_format()->name(), "noobaa");
  }

  sf::path bucket_path() const { return bp / "root" / testname; }
};


/* A directory object as NooBaa lays it down:  the marker and the
 * object's attributes on the directory, and the bytes in `.folder` only
 * when there are any (`namespace_fs.js`, _create_empty_dir_content). */
static void lay_noobaa_directory_object(const sf::path& dir,
					const std::string& body,
					const std::string& content_type,
					const std::string& probe)
{
  sf::create_directories(dir);
  if (!body.empty()) {
    write_file(dir / ".folder", body);
  }
  set_u64_xattr(dir, "user.noobaa.dir_content", body.size());
  ASSERT_EQ(::setxattr(dir.c_str(), "user.noobaa.content_type",
		       content_type.data(), content_type.size(), 0), 0);
  ASSERT_EQ(::setxattr(dir.c_str(), "user.probe",
		       probe.data(), probe.size(), 0), 0);
}

/* An attribute value as the S3 front end puts it in a header:
 * rgw_bl_str() for the content type and rgw_sanitized_hdrval() for
 * metadata both drop the trailing NUL, which the nsfs format stores and
 * NooBaa's does not. */
static std::string as_sent(const bufferlist& bl)
{
  std::string v = bl.to_str();
  while (!v.empty() && v.back() == '\0') {
    v.pop_back();
  }
  return v;
}

/* What a client sees of one object:  HEAD, GET and its listing row. */

struct ClientView {
  bool exists{false};
  uint64_t size{0};
  std::string content_type;
  std::string probe;
  std::string etag;
  ceph::real_time mtime;
  std::string body;
  bool listed{false};
  uint64_t listed_size{0};
  std::string listed_etag;
  ceph::real_time listed_mtime;
};

static ClientView client_view(rgw::sal::Bucket* bucket,
			      const DoutPrefixProvider* dpp,
			      const std::string& key)
{
  ClientView v;
  auto obj = bucket->get_object(rgw_obj_key(key));
  std::unique_ptr<rgw::sal::Object::ReadOp> rop(obj->get_read_op(rgw::sal::no_authenticated_user));
  if (rop->prepare(null_yield, dpp) == 0) {
    v.exists = true;
    v.size = obj->get_size();
    v.mtime = obj->get_mtime();
    auto& attrs = obj->get_attrs();
    if (auto a = attrs.find(RGW_ATTR_CONTENT_TYPE); a != attrs.end()) {
      v.content_type = as_sent(a->second);
    }
    if (auto a = attrs.find(RGW_ATTR_META_PREFIX "probe"); a != attrs.end()) {
      v.probe = as_sent(a->second);
    }
    if (auto a = attrs.find(RGW_ATTR_ETAG); a != attrs.end()) {
      v.etag = as_sent(a->second);
    }
    if (v.size > 0) {
      bufferlist bl;
      Read_CB cb(&bl);
      EXPECT_EQ(rop->iterate(dpp, 0, v.size - 1, &cb, null_yield), 0) << key;
      v.body = bl.to_str();
    }
  }

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  EXPECT_EQ(bucket->list(dpp, params, 1000, results, null_yield), 0);
  for (auto& e : results.objs) {
    if (e.key.name == key) {
      EXPECT_FALSE(v.listed) << key << " listed twice";
      v.listed = true;
      v.listed_size = e.meta.size;
      v.listed_etag = e.meta.etag;
      v.listed_mtime = e.meta.mtime;
    }
  }
  return v;
}

/* A bucket being converted is, to a client, the bucket it becomes
 * (DECISIONS.md 2026-10-13).  Directory objects in NooBaa's format,
 * read before conversion and again after, must look the same.
 *
 * Before this, `e/` was 404 and unlisted, and `c/` lost its content type
 * and metadata, because the reader took `.folder` for the whole object.
 *
 * Everything is compared, an empty one's ETag and Last-Modified
 * included:  conversion gives it a new sentinel, and must not let the
 * new inode change either (DECISIONS.md 2026-10-13). */
TEST_F(NSFSNooBaaBucketTest, ADirectoryObjectReadsTheSameBeforeAndAfterConversion)
{
  lay_noobaa_directory_object(bucket_path() / "e", "", "text/x-e", "e");
  lay_noobaa_directory_object(bucket_path() / "c", "dir bytes", "text/x-c", "c");

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  const ClientView e0 = client_view(bucket.get(), env->dpp, "e/");
  const ClientView c0 = client_view(bucket.get(), env->dpp, "c/");

  ASSERT_TRUE(e0.exists) << "an empty directory object was not found";
  EXPECT_EQ(e0.size, 0u);
  EXPECT_EQ(e0.content_type, "text/x-e");
  EXPECT_EQ(e0.probe, "e");
  EXPECT_TRUE(e0.listed);
  ASSERT_TRUE(c0.exists);
  EXPECT_EQ(c0.size, 9u);
  EXPECT_EQ(c0.content_type, "text/x-c")
      << "attributes were read from .folder rather than the directory";
  EXPECT_EQ(c0.probe, "c");
  EXPECT_EQ(c0.body, "dir bytes");
  EXPECT_TRUE(c0.listed);
  EXPECT_EQ(c0.listed_size, 9u);
  EXPECT_EQ(c0.listed_etag, c0.etag);

  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);

  const ClientView e1 = client_view(bucket.get(), env->dpp, "e/");
  const ClientView c1 = client_view(bucket.get(), env->dpp, "c/");

  EXPECT_EQ(e1.exists, e0.exists);
  EXPECT_EQ(e1.size, e0.size);
  EXPECT_EQ(e1.content_type, e0.content_type);
  EXPECT_EQ(e1.probe, e0.probe);
  EXPECT_EQ(e1.etag, e0.etag) << "conversion changed an empty object's ETag";
  EXPECT_EQ(e1.mtime, e0.mtime)
      << "conversion changed an empty object's Last-Modified";
  EXPECT_EQ(e1.listed, e0.listed);
  EXPECT_EQ(e1.listed_size, e0.listed_size);
  EXPECT_EQ(e1.listed_etag, e0.listed_etag);
  EXPECT_EQ(e1.listed_mtime, e0.listed_mtime);

  EXPECT_EQ(c1.exists, c0.exists);
  EXPECT_EQ(c1.size, c0.size);
  EXPECT_EQ(c1.content_type, c0.content_type);
  EXPECT_EQ(c1.probe, c0.probe);
  EXPECT_EQ(c1.etag, c0.etag);
  EXPECT_EQ(c1.mtime, c0.mtime);
  EXPECT_EQ(c1.body, c0.body);
  EXPECT_EQ(c1.listed, c0.listed);
  EXPECT_EQ(c1.listed_size, c0.listed_size);
  EXPECT_EQ(c1.listed_etag, c0.listed_etag);
  EXPECT_EQ(c1.listed_mtime, c0.listed_mtime);
}

/* The root object is converted as the directory object it is:  its
 * bytes and attributes move to the root's `.folder`, and none of them
 * become the bucket's (DECISIONS.md 2026-10-13).  Before, the bucket
 * step took them as bucket attributes and the root object lost its
 * content type and metadata.  With content and empty, and with bucket
 * state beside it that must survive as the bucket's. */
TEST_F(NSFSNooBaaBucketTest, TheRootObjectReadsTheSameBeforeAndAfterConversion)
{
  for (const std::string body : {"root bytes", ""}) {
    SCOPED_TRACE(body.empty() ? "empty" : "with content");
    /* a fresh bucket each time:  the second case must not see the first */
    if (sf::exists(bucket_path() / ".folder")) {
      sf::remove(bucket_path() / ".folder");
    }

    if (body.empty()) {
      /* back to base for the second case;  the bucket is empty but for
       * the root object, which does not keep a bucket from changing */
    bufferlist tag;
    tag.append("bucket-tag");
    rgw::sal::Attrs add;
    add[RGW_ATTR_TAGS] = tag;
    lay_noobaa_directory_object(bucket_path(), body, "text/x-root", "root");

    const ClientView r0 = client_view(bucket.get(), env->dpp, "/");
    ASSERT_TRUE(r0.exists);
    EXPECT_EQ(r0.content_type, "text/x-root");
    EXPECT_EQ(r0.probe, "root");

    ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);

    const ClientView r1 = client_view(bucket.get(), env->dpp, "/");
    EXPECT_EQ(r1.exists, r0.exists);
    EXPECT_EQ(r1.size, r0.size);
    EXPECT_EQ(r1.content_type, r0.content_type);
    EXPECT_EQ(r1.probe, r0.probe);
    EXPECT_EQ(r1.body, r0.body);
    EXPECT_EQ(r1.etag, r0.etag);
    EXPECT_EQ(r1.mtime, r0.mtime);
    EXPECT_FALSE(r1.listed);

    ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
    const auto& battrs = bucket->get_attrs();
    EXPECT_EQ(battrs.count(RGW_ATTR_CONTENT_TYPE), 0u)
	<< "the root object's content type became the bucket's";
    EXPECT_EQ(battrs.count(RGW_ATTR_META_PREFIX "probe"), 0u)
	<< "the root object's metadata became the bucket's";
    auto t = battrs.find(RGW_ATTR_TAGS);
    ASSERT_NE(t, battrs.end()) << "the bucket's own state was lost";
    EXPECT_EQ(t->second.to_str(), "bucket-tag");
    EXPECT_LT(::getxattr(bucket_path().c_str(), "user.noobaa.dir_content",
			 nullptr, 0), 0)
	<< "the root's marker survived its conversion";
  }
}

/* Only what RGW wrote under `user.nsfs:` is the bucket's state.  Any
 * other name on a bucket directory with no root object marker is
 * neither bucket state nor an object, and is left as it is. */
TEST_F(NSFSNooBaaBucketTest, AStrayAttributeOnTheBucketDirectoryIsNotBucketState)
{
  const std::string v{"stray"};
  ASSERT_EQ(::setxattr(bucket_path().c_str(), "user.stray", v.data(),
		       v.size(), 0), 0);

  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  EXPECT_EQ(bucket->get_attrs().count(RGW_ATTR_META_PREFIX "stray"), 0u)
      << "a stray attribute was read as bucket state";

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  EXPECT_EQ(bucket->get_attrs().count(RGW_ATTR_META_PREFIX "stray"), 0u)
      << "conversion made a stray attribute bucket state";
  EXPECT_EQ(str_xattr(bucket_path(), "user.stray"), "stray")
      << "conversion removed a name that was not its to remove";
}

/* With MD5 on, NooBaa stores the ETag, and conversion carries it across:
 * the ETag recorded for an empty object must not replace it. */
TEST_F(NSFSNooBaaBucketTest, ConversionKeepsAnEmptyDirectoryObjectsStoredMD5)
{
  lay_noobaa_directory_object(bucket_path() / "e", "", "text/x-e", "e");
  const std::string md5{"d41d8cd98f00b204e9800998ecf8427e"};
  ASSERT_EQ(::setxattr((bucket_path() / "e").c_str(), "user.content_md5",
		       md5.data(), md5.size(), 0), 0);

  const ClientView e0 = client_view(bucket.get(), env->dpp, "e/");
  ASSERT_EQ(e0.etag, md5) << "the stored MD5 was not read as the ETag";

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);
  const ClientView e1 = client_view(bucket.get(), env->dpp, "e/");
  EXPECT_EQ(e1.etag, md5);
  EXPECT_EQ(e1.listed_etag, md5);
  EXPECT_EQ(e1.mtime, e0.mtime);
}

/* The root object in NooBaa's format:  the marker and its attributes on
 * the bucket's own directory, which also carries the bucket's state
 * under `user.nsfs:`.  Only the object's are its attributes. */
TEST_F(NSFSNooBaaBucketTest, TheRootObjectReadsWithoutTheBucketsState)
{
  lay_noobaa_directory_object(bucket_path(), "root bytes", "text/x-root", "root");
  ASSERT_EQ(::getxattr(bucket_path().c_str(), "user.nsfs:bucket_info",
		       nullptr, 0) > 0, true)
      << "the bucket directory carries no state;  nothing here would be "
	 "proving the exclusion";

  const ClientView r = client_view(bucket.get(), env->dpp, "/");
  ASSERT_TRUE(r.exists);
  EXPECT_EQ(r.size, 10u);
  EXPECT_EQ(r.content_type, "text/x-root");
  EXPECT_EQ(r.probe, "root");
  EXPECT_EQ(r.body, "root bytes");
  EXPECT_FALSE(r.listed);

  auto obj = bucket->get_object(rgw_obj_key("/"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  EXPECT_EQ(obj->get_attrs().count("bucket_info"), 0u)
      << "the bucket's state was read as the root object's";
}

/* A `.folder` without the marker is not a directory object to NooBaa,
 * whose reader returns NoSuchKey for a directory that lacks it.  It is
 * the control for the marker:  the same layout with the marker is. */
TEST_F(NSFSNooBaaBucketTest, ADirectoryWithoutTheMarkerIsNotItsObject)
{
  lay_noobaa_directory_object(bucket_path() / "m", "marked", "text/x-m", "m");
  sf::create_directories(bucket_path() / "u");
  write_file(bucket_path() / "u" / "x", "x");

  EXPECT_TRUE(client_view(bucket.get(), env->dpp, "m/").exists);
  const ClientView u = client_view(bucket.get(), env->dpp, "u/");
  EXPECT_FALSE(u.exists) << "a plain directory read as an object";
  EXPECT_FALSE(u.listed);
}

TEST_F(NSFSNooBaaBucketTest, AWriteToADirectoryObjectIsRejected)
{
  lay_noobaa_directory_object(bucket_path() / "e", "", "text/x-e", "e");

  auto obj = bucket->get_object(rgw_obj_key("e/"));
  EXPECT_TRUE(client_view(bucket.get(), env->dpp, "e/").exists);
  EXPECT_FALSE(sf::exists(bucket_path() / "e" / ".folder"))
      << "a sentinel was written into a base bucket";
}

  auto marked = marked_bucket(testname + "-ours");
  EXPECT_EQ(copy(bucket.get(), "obj", marked.get(), "copied"), 0);
  EXPECT_EQ(put(marked.get(), "new"), 0) << "a marked bucket rejected a write";
}


/* NooBaa's temp directory is content only while it holds something.
 *
 * `.noobaa-nsfs_<bucket id>` outlives every upload:  NooBaa makes it,
 * and `multipart-uploads/` inside it, and leaves both behind when the
 * last upload completes.  Matched by name alone it counted as an upload
 * in flight forever, and a bucket that listed empty answered
 * BucketNotEmpty. */
TEST_F(NSFSNooBaaBucketTest, AnEmptyTempDirectoryDoesNotKeepABucket)
{
  sf::create_directories(bucket_path() / ".noobaa-nsfs_6ace0000aaaabbbbccccdddd"
			 / "multipart-uploads");
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  EXPECT_EQ(b->check_empty(env->dpp, null_yield), 0)
      << "an empty temp directory counted as an upload in flight";
  EXPECT_EQ(bucket->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user), 0);
  EXPECT_FALSE(sf::exists(bucket_path()));
}

/* An upload in progress does not keep its bucket, and deleting the
 * bucket aborts it, as RADOS does:  check_bucket_empty() counts only
 * the objects' namespace and RadosBucket::remove() aborts the uploads.
 * One NooBaa started, carried through adoption. */
TEST_F(NSFSNooBaaBucketTest, DeletingABucketAbortsTheUploadTheyStarted)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);
  ASSERT_TRUE(sf::is_directory(tree.staging));
  /* adopted first:  a bucket not taken over takes no DeleteBucket */
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);

  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  std::string marker;
  bool truncated = false;
  ASSERT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 10, uploads,
				    nullptr, &truncated, null_yield,
				    rgw::sal::no_authenticated_user), 0);
  ASSERT_EQ(uploads.size(), 1u) << "nothing to abort:  no upload listed";

  EXPECT_EQ(b->check_empty(env->dpp, null_yield), 0)
      << "an upload in progress kept its bucket";
  EXPECT_EQ(bucket->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user), 0);
  EXPECT_FALSE(sf::exists(bucket_path()));
}

/* A base bucket whose state is where NooBaa keeps it.
 *
 * The config root is set before the driver is built, because that is
 * when the reader is constructed:  an address we were not given means
 * do not consult a store. */

class NSFSNooBaaStateTest : public NSFSNooBaaBucketTest {
public:
  sf::path conf_root() const {
    return sf::absolute(sf::path{base_path / get_test_name()}) / "noobaa-conf";
  }


  void SetUp() override {
    sf::create_directories(conf_root() / "buckets");
    g_conf().set_val("rgw_nsfs_noobaa_config_root", conf_root().string());
    NSFSNooBaaBucketTest::SetUp();

  void TearDown() override {
    NSFSNooBaaBucketTest::TearDown();
    g_conf().set_val("rgw_nsfs_noobaa_config_root", "");
    g_conf().apply_changes(nullptr);
  }


  /* A tree we did not write.  The fixture creates its bucket through
   * the SAL, which is the only way to get a Bucket, and that writes the nsfs
   * attribute;  a bucket NooBaa made carries no such thing. */
  void remove_rgw_bucket_info() {
    ASSERT_EQ(::removexattr((bp / "root" / testname).c_str(),
			    "user.nsfs:bucket_info"), 0);
  }

    write_file(conf_root() / "buckets" / (testname + ".json"), doc);
  }

  /* Read the record write_nb_record() wrote, through the strategy
   * that reads it.  No bucket is involved and no fallback.
   *
   * NooBaaBucketStateStrategy::load() takes a dir_fd and ignores it --
   * it finds <config_root>/buckets/<name>.json from the name alone --
   * so -1 is passed here.  This is the call the adopt step makes.
   *
   * load_bucket() reaches the same strategy through
   * bucket_state_fallback(), but that read happens on the request
   * path and the interchange design moves it to adopt time.  A test
   * which reached the translation only that way would have to be
   * rewritten when the fallback goes.  Tests of the fallback itself --
   * that it is consulted for a base bucket and not for a marked one --
   * are about the wiring and stay on load_bucket(). */
  int read_nb_record(RGWBucketInfo& info, Attrs& attrs) {
    /* Cleared first, so a test which reads twice does not see the
     * first read's keys in the second result.  load() adds what the
     * record carries and removes nothing. */
    info = RGWBucketInfo{};
    attrs.clear();

    auto* nb_state = driver->get_noobaa_bucket_state_strategy();
    if (! nb_state) {
      return -ENOENT;
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

/* NooBaa's versioning reaches RGW, which is what a base bucket loses today.
 *
 * A bucket that reads as unversioned takes a PUT in place instead of
 * demoting the current version into .versions/, so this is the silent
 * data loss and not a missing feature. */
TEST_F(NSFSNooBaaStateTest, VersioningComesFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* Nothing has written a record yet, so the read finds none and says
   * so.  That is a different outcome from a record which says
   * unversioned, and the assertion below depends on the difference. */
  ASSERT_EQ(read_nb_record(info, attrs), -ENOENT)
      << "a record was read where this test wrote none";

  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  EXPECT_TRUE(info.versioned());
  EXPECT_TRUE(info.versioning_enabled());
}

/* Suspended is versioned and suspended, not unversioned:  the bucket
 * still holds versions. */
TEST_F(NSFSNooBaaStateTest, SuspendedIsVersionedAndSuspended)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("SUSPENDED");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  EXPECT_TRUE(info.versioned());
  EXPECT_FALSE(info.versioning_enabled());
}

/* The nsfs state wins where it exists.
 *
 * The chain runs the opposite way to the object chain:  a base
 * bucket's objects are in NooBaa's format, but its state is in nsfs's
 * wherever we have written any. */
TEST_F(NSFSNooBaaStateTest, NsfsStateWinsOverNooBaas)
{
  write_nb_record("ENABLED");

  /* the nsfs attribute is still there, from create(), and says nothing
   * about versioning */
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  EXPECT_FALSE(bucket->get_info().versioned())
      << "their record was consulted for a bucket that has state of ours";
}

/* Conversion records the bucket's owner on an object that records none.
 *
 * NooBaa's format has nowhere to put an ACL, so
 * `NooBaaXattrStrategy::disk_name(RGW_ATTR_ACL)` is empty and nothing
 * is stored;  NooBaa returns the bucket's `owner_account` as every
 * object's owner.  Adoption is where the tree becomes consistent with
 * the imported accounts, so conversion writes the bucket's owner down
 * (DECISIONS.md 2026-10-11).  Not the file's uid:  that is the POSIX
 * identity the file was written as, and the assertion against it is
 * what distinguishes the two owners.
 *
 * The sideloaded file has no ACL by construction, which is the case. */
TEST_F(NSFSNooBaaBucketTest, ConversionRecordsTheBucketOwner)
{
  const sf::path obj{bucket_path() / "sideloaded.txt"};
  {
    int fd = ::open(obj.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0) << cpp_strerror(errno);
    ASSERT_EQ(::write(fd, "x", 1), 1);
    ::close(fd);
  }

  struct stat st{};
  ASSERT_EQ(::stat(obj.c_str(), &st), 0);

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_TRUE(b->xattr_strategy()->disk_name(RGW_ATTR_ACL).empty())
      << "their mapping has somewhere to put an ACL after all;  the "
	 "premise of this test is gone";

  /* Where the ACL will land once the bucket is shared.  Asked of the
   * strategy directly, because the bucket is still base and its own
   * strategy has no name for an ACL at all. */
  nsfs::PrefixedXattrStrategy prefixed;
  const std::string disk = prefixed.disk_name(RGW_ATTR_ACL);
  ASSERT_FALSE(disk.empty()) << "shared has nowhere to put an ACL";

  /* Both polarities, in one run.  Absent before, present after -- so
   * the assertion after the conversion is reachable, and what satisfies
   * it is the conversion rather than something that was already
   * there. */
  ASSERT_EQ(::getxattr(obj.c_str(), disk.c_str(), nullptr, 0), -1);
  ASSERT_EQ(errno, ENODATA)
      << "the sideloaded object already carries " << disk
      << ";  nothing here would then be proving the capture";

  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);
  ASSERT_STREQ(b->profile_name(), "shared");

  char buf[4096];
  ssize_t len = ::getxattr(obj.c_str(), disk.c_str(), buf, sizeof(buf));
  ASSERT_GT(len, 0)
      << "the converted object carries no " << disk
      << ";  it has no owner recorded and lists as unknown";

  bufferlist bl;
  bl.append(buf, len);
  RGWAccessControlPolicy policy;
  auto it = bl.cbegin();
  ASSERT_NO_THROW(policy.decode(it));

  EXPECT_EQ(policy.get_owner().id, bucket->get_info().owner)
      << "the recorded owner is not the bucket's";
  EXPECT_NE(to_string(policy.get_owner().id), std::to_string(st.st_uid))
      << "the file's uid was recorded as its S3 owner";

  /* RGW's default policy, not a bare owner.  An owner with no grants
   * is more restrictive than the absence it replaced:  with no ACL,
   * authorization falls through to the bucket. */
  EXPECT_FALSE(policy.get_acl().get_grant_map().empty())
      << "the captured ACL grants nobody anything";
}

/* A base bucket loaded the way a request loads it lists in NooBaa's format.
 *
 * The fixture reaches base through set_profile(), which applies the
 * format.  A request reaches it through load_bucket(), which recognises
 * the profile from the absent extensions attribute -- and recognising
 * base returned before applying the format.  The directory kept the
 * driver's seed, which is nsfs's, so a listing read NooBaa's objects with
 * the nsfs strategies:  no recorded ACL, and every owner listed as
 * `unknown`.  Object reads were unaffected, because they ask the bucket
 * for its format rather than the directory, which is why only the
 * listing showed it.
 *
 * A sideloaded file is the case:  it carries no ACL by construction.
 * The control is the same bucket marked shared, loaded the same way,
 * whose directory must then carry nsfs's -- so the strategy assertion
 * distinguishes the two rather than passing on either. */
TEST_F(NSFSNooBaaBucketTest, ALoadedBaseBucketListsInNooBaaFormat)
{
  const sf::path obj{bucket_path() / "sideloaded.txt"};
  {
    int fd = ::open(obj.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0) << cpp_strerror(errno);
    ASSERT_EQ(::write(fd, "x", 1), 1);
    ::close(fd);
  }
  auto load_fresh = [&] {
    RGWBucketInfo fi;
    fi.bucket.name = testname;
    auto fresh = driver->get_bucket(fi);
    EXPECT_NE(fresh.get(), nullptr);
    EXPECT_EQ(fresh->load_bucket(env->dpp, null_yield), 0);
    return fresh;
  };

  auto fresh = load_fresh();
  auto* fb = static_cast<rgw::sal::NSFSBucket*>(fresh.get());
  ASSERT_STREQ(fb->profile_name(), "base");
  EXPECT_STREQ(fb->get_dir()->get_xattr_strategy()->name(), "noobaa")
      << "a loaded base bucket's directory reads with our strategy";
  EXPECT_EQ(fb->get_dir()->get_path_strategy(), fb->path_strategy())
      << "a loaded base bucket's directory names entries by our conventions";

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  ASSERT_EQ(fresh->list(env->dpp, params, 128, results, null_yield), 0);
  bool found = false;
  for (const auto& e : results.objs) {
    if (e.key.name != "sideloaded.txt") {
      continue;
    }
    found = true;
    EXPECT_NE(e.meta.owner, "unknown")
	<< "the listing read their object with our strategy";
    EXPECT_EQ(e.meta.owner, to_string(fresh->get_info().owner))
	<< "an object recording no owner did not list as its bucket's";
  }
  EXPECT_TRUE(found) << "the sideloaded object was not listed";

  /* the control:  marked, the same load gives nsfs's */
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);
  auto marked = load_fresh();
  auto* mb = static_cast<rgw::sal::NSFSBucket*>(marked.get());
  ASSERT_STREQ(mb->profile_name(), "shared");
  EXPECT_STREQ(mb->get_dir()->get_xattr_strategy()->name(), "rgw");
}

/* The sentinel carries the mode every object nsfs creates carries:
 * NooBaa's, 0660 by default (DECISIONS.md 2026-10-11).
 *
 * umask is set for the duration, because `openat` applies it and a
 * developer's umask is not the test's to assume;  022 takes the group
 * write bit, so this also requires the create mode to be restored. */
TEST_F(NSFSNooBaaBucketTest, ConversionGivesTheSentinelAnObjectMode)
{
  const mode_t saved = ::umask(022);
  auto restore = make_scope_guard([saved] { ::umask(saved); });
  nsfs::init_create_modes(env->dpp, env->cct.get());

  const sf::path dir{bucket_path() / "dirobj"};
  ASSERT_TRUE(sf::create_directories(dir));
  {
    const char* v = "0";
    ASSERT_EQ(::setxattr(dir.c_str(), "user.noobaa.dir_content", v, 1, 0), 0)
	<< cpp_strerror(errno)
	<< ";  without their marker this is an ordinary directory and the "
	   "sentinel is never created";
  }

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);

  const sf::path sentinel{dir / ".folder"};
  ASSERT_TRUE(sf::exists(sentinel))
      << "the directory object was not converted to a sentinel";

  struct stat st{};
  ASSERT_EQ(::stat(sentinel.c_str(), &st), 0);
  EXPECT_EQ(st.st_mode & 07777, nsfs::file_create_mode())
      << "the sentinel does not carry the mode an object written through "
	 "S3 carries";
}

/* An object carrying a `user.noobaa.*` attribute no reviewed release of
 * NooBaa writes is not read (X4's third layer, DECISIONS.md 2026-10-26).
 *
 * The attribute might change what the bytes mean or who may read them,
 * and a bucket in NooBaa's format serves reads only, so the read is
 * refused with InvalidObjectState.  Three objects:  one with nothing
 * unusual, the control;  one with an attribute NooBaa writes and nsfs does
 * not carry, `restore.request`, which is known and so read;  and one
 * with an attribute nothing writes.  The last still lists:  a listing
 * row hands out no content, and one object must not end a listing. */
TEST_F(NSFSNooBaaBucketTest, AnObjectCarryingAnUnknownNooBaaAttributeIsNotRead)
{
  write_file(bucket_path() / "plain", "abc");
  write_file(bucket_path() / "restoring", "abc");
  write_file(bucket_path() / "odd", "abc");
  ASSERT_EQ(::setxattr((bucket_path() / "restoring").c_str(),
		       "user.noobaa.restore.request", "x", 1, 0), 0)
      << cpp_strerror(errno);
  ASSERT_EQ(::setxattr((bucket_path() / "odd").c_str(),
		       "user.noobaa.encryption", "x", 1, 0), 0)
      << cpp_strerror(errno);

  const ClientView plain = client_view(bucket.get(), env->dpp, "plain");
  EXPECT_TRUE(plain.exists) << "the control did not read";
  EXPECT_EQ(plain.body, "abc");
  const ClientView restoring = client_view(bucket.get(), env->dpp, "restoring");
  EXPECT_TRUE(restoring.exists)
      << "an attribute NooBaa writes at the pin refused the read";

  auto obj = bucket->get_object(rgw_obj_key("odd"));
  std::unique_ptr<rgw::sal::Object::ReadOp> rop(
      obj->get_read_op(rgw::sal::no_authenticated_user));
  EXPECT_EQ(rop->prepare(null_yield, env->dpp), -ERR_INVALID_OBJECT_STATE)
      << "an object carrying an attribute no reviewed release writes was "
	 "read";
  const ClientView odd = client_view(bucket.get(), env->dpp, "odd");
  EXPECT_FALSE(odd.exists);
  EXPECT_TRUE(odd.listed) << "the object ended or left the listing";
  EXPECT_TRUE(plain.listed);
}

/* The same attribute stops the bucket's conversion, and the converting
 * marker stays, so the conversion resumes once the attribute is gone or
 * its release is reviewed.  The resume is the control. */
TEST_F(NSFSNooBaaBucketTest, AnUnknownNooBaaAttributeStopsTheConversion)
{
  write_file(bucket_path() / "plain", "abc");
  write_file(bucket_path() / "odd", "abc");
  const sf::path odd = bucket_path() / "odd";
  ASSERT_EQ(::setxattr(odd.c_str(), "user.noobaa.encryption", "x", 1, 0), 0)
      << cpp_strerror(errno);

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  EXPECT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED),
	    -ERR_INVALID_OBJECT_STATE)
      << "a bucket holding an attribute no reviewed release writes converted";
  EXPECT_TRUE(b->is_converting()) << "the converting marker did not stay";

  ASSERT_EQ(::removexattr(odd.c_str(), "user.noobaa.encryption"), 0);
  EXPECT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0)
      << "the conversion did not resume";
  EXPECT_FALSE(b->is_converting());
  EXPECT_EQ(client_view(bucket.get(), env->dpp, "odd").body, "abc");
}

/* A group the process belongs to that is not its primary one.
 *
 * `chown(2)` lets an unprivileged owner change a file's group to any
 * group it is a member of.  That is what makes the inheritance path
 * reachable without root:  `inherit_ids()` skips only when the uid
 * *and* the gid already match the gateway's, so a source whose group
 * differs forces the `fchown` that carries ownership across. */
static std::optional<gid_t> other_group()
{
  int n = ::getgroups(0, nullptr);
  if (n <= 0) {
    return std::nullopt;
  }
  std::vector<gid_t> gs(n);
  if (::getgroups(n, gs.data()) != n) {
    return std::nullopt;
  }
  const gid_t mine = ::getegid();
  for (gid_t g : gs) {
    if (g != mine) {
      return g;
    }
  }
  return std::nullopt;
}

/* What conversion creates inherits the ownership of what it
 * re-expresses.
 *
 * The sentinel stands in for the directory, so it carries the
 * directory's uid and gid -- not the gateway's, which would leave a
 * converted directory object unreadable to the account that owns the
 * data.  The uid cannot be varied without privilege, so the group is
 * what this varies, and it exercises the same `fchown`. */
TEST_F(NSFSNooBaaBucketTest, ConversionGivesTheSentinelTheDirectorysGroup)
{
  const auto alt = other_group();
  if (!alt) {
    GTEST_SKIP() << "only one group;  nothing to inherit that differs";
  }

  const sf::path dir{bucket_path() / "dirobj"};
  ASSERT_TRUE(sf::create_directories(dir));
  {
    const char* v = "0";
    ASSERT_EQ(::setxattr(dir.c_str(), "user.noobaa.dir_content", v, 1, 0), 0)
	<< cpp_strerror(errno);
  }
  ASSERT_EQ(::chown(dir.c_str(), static_cast<uid_t>(-1), *alt), 0)
      << cpp_strerror(errno);

  struct stat dst{};
  ASSERT_EQ(::stat(dir.c_str(), &dst), 0);
  ASSERT_EQ(dst.st_gid, *alt) << "the premise is gone;  the chown did nothing";
  ASSERT_NE(dst.st_gid, ::getegid())
      << "the source group matches ours, so inherit_ids() would skip and "
	 "this test could not fail";

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);

  const sf::path sentinel{dir / ".folder"};
  ASSERT_TRUE(sf::exists(sentinel));
  struct stat st{};
  ASSERT_EQ(::stat(sentinel.c_str(), &st), 0);
  EXPECT_EQ(st.st_gid, *alt)
      << "the sentinel kept the gateway's group instead of the group of "
	 "the directory it re-expresses";
  EXPECT_EQ(st.st_uid, dst.st_uid) << "the sentinel's uid is not the "
      "directory's";
}


/* Without the capability, conversion fails and leaves nothing behind.
 *
 * `CAP_CHOWN` cannot be dropped in a test process -- a thread that
 * drops it cannot regain it, which would poison every later test -- so
 * the condition is injected through the same hint the suite uses.
 *
 * Both halves matter.  Failing is the point, and so is removing the
 * file it had already created:  a sentinel left behind with the
 * gateway's ownership is the outcome the failure exists to prevent. */
TEST_F(NSFSNooBaaBucketTest, ConversionFailsWhenItCannotInheritOwnership)
{
  const auto alt = other_group();
  if (!alt) {
    GTEST_SKIP() << "only one group;  the chown would be skipped anyway";
  }

  const sf::path dir{bucket_path() / "dirobj"};
  ASSERT_TRUE(sf::create_directories(dir));
  {
    const char* v = "0";
    ASSERT_EQ(::setxattr(dir.c_str(), "user.noobaa.dir_content", v, 1, 0), 0)
	<< cpp_strerror(errno);
  }
  ASSERT_EQ(::chown(dir.c_str(), static_cast<uid_t>(-1), *alt), 0)
      << cpp_strerror(errno);

  std::map<std::string, std::string> params{{"enable", "true"}};
  std::map<std::string, std::string> out;
  ASSERT_EQ(driver->driver_hint(env->dpp, "inject-no-chown", params, &out), 0);
  ASSERT_EQ(out["enabled"], "true");

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  EXPECT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED),
	    -EPERM)
      << "conversion proceeded without being able to set ownership";

  EXPECT_FALSE(sf::exists(dir / ".folder"))
      << "the failed conversion left a sentinel owned by the gateway, which "
	 "is the state failing is meant to prevent";

  /* and the other polarity:  with the injection off the same tree
   * converts, so the failure above was the injection and not some
   * unrelated failure */
  params["enable"] = "false";
  ASSERT_EQ(driver->driver_hint(env->dpp, "inject-no-chown", params, &out), 0);
  EXPECT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);
  EXPECT_TRUE(sf::exists(dir / ".folder"));
}

/* Object lock reaches RGW.
 *
 * The one whose absence is a compliance breach rather than a missing
 * feature:  a WORM bucket that reads as unlocked accepts overwrite and
 * delete. */
TEST_F(NSFSNooBaaStateTest, ObjectLockComesFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* A record without a lock reads as unlocked, so the assertion after
   * the next one is able to fail. */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  ASSERT_FALSE(info.obj_lock_enabled());

  write_nb_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"days":10,)"
	       R"("mode":"GOVERNANCE"}}})");
  ASSERT_EQ(read_nb_record(info, attrs), 0);

  EXPECT_TRUE(info.obj_lock_enabled());
  ASSERT_TRUE(info.obj_lock.has_rule());
  EXPECT_EQ(info.obj_lock.get_mode(), "GOVERNANCE");
  EXPECT_EQ(info.obj_lock.get_days(), 10);
  EXPECT_EQ(info.obj_lock.get_years(), 0);
}

/* A retention mode that is neither of NooBaa's fails the bucket closed.
 *
 * Nothing else checks it:  RGW validates the mode on its XML path and
 * not in DefaultRetention::decode_json(), so without this an
 * arbitrary string would be reported as a locked bucket's retention
 * mode. */
TEST_F(NSFSNooBaaStateTest, AnUnknownRetentionModeFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"days":10,)"
	       R"("mode":"ADVISORY"}}})");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* Years rather than days, and the other mode. */
TEST_F(NSFSNooBaaStateTest, ObjectLockInYearsAndCompliance)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"years":7,)"
	       R"("mode":"COMPLIANCE"}}})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  EXPECT_TRUE(info.obj_lock_enabled());
  ASSERT_TRUE(info.obj_lock.has_rule());
  EXPECT_EQ(info.obj_lock.get_mode(), "COMPLIANCE");
  EXPECT_EQ(info.obj_lock.get_years(), 7);
  EXPECT_EQ(info.obj_lock.get_days(), 0);
}

/* Locked with no default retention is a lock.
 *
 * S3 allows PutObjectLockConfiguration with ObjectLockEnabled and no
 * rule:  the bucket is locked and objects carry their own retention. */
TEST_F(NSFSNooBaaStateTest, ObjectLockWithoutARule)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled"})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  EXPECT_TRUE(info.obj_lock_enabled());
  EXPECT_FALSE(info.obj_lock.has_rule());
}

/* "Disabled" is NooBaa's internal state for a bucket that is not locked,
 * not a lock that is switched off.  S3 only ever validates "Enabled"
 * on the wire. */
TEST_F(NSFSNooBaaStateTest, NooBaaDisabledLockIsNoLock)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Disabled"})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  EXPECT_FALSE(info.obj_lock_enabled());
}

/* A retention we cannot honour fails the bucket closed rather than
 * being honoured wrongly.  Both S3 and NooBaa's schema require exactly
 * one of days or years. */
TEST_F(NSFSNooBaaStateTest, AnImpossibleRetentionFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"days":10,"years":7,)"
	       R"("mode":"GOVERNANCE"}}})");
  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG)
      << "a retention naming both days and years was accepted";

  write_nb_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"mode":"GOVERNANCE"}}})");
  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG)
      << "a retention naming neither days nor years was accepted";
}

/* NooBaa's bucket policy reaches the attribute map, normalised by RGW's
 * own parser the way one set through S3 would be. */
TEST_F(NSFSNooBaaStateTest, BucketPolicyComesFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no policy in NooBaa's record, none in the attributes */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_IAM_POLICY), attrs.end());

  write_nb_record("ENABLED",
	       R"(,"s3_policy":{"Version":"2012-10-17","Statement":[)"
	       R"({"Sid":"one","Effect":"Allow","Principal":{"AWS":"*"},)"
	       R"("Action":["s3:GetObject"],)"
	       R"("Resource":["arn:aws:s3:::)" + std::string(testname) +
	       R"(/*"]}]})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_IAM_POLICY);
  ASSERT_NE(i, attrs.end())
      << "their policy did not reach the attribute map";

  /* stored as a document RGW's own parser accepts, which is the thing
   * that matters -- not that the bytes match NooBaa's */
  const std::string stored = i->second.to_str();
  EXPECT_NE(stored.find("s3:GetObject"), std::string::npos);
  CephContext* cct = env->cct.get();
  EXPECT_NO_THROW(rgw::IAM::Policy(cct, &info.bucket.tenant,
				   stored, false));
}

/* A policy RGW cannot parse fails the bucket closed, rather than
 * being stored and failing later inside every request that evaluates
 * it. */
TEST_F(NSFSNooBaaStateTest, AnUnparsablePolicyFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"s3_policy":{"Version":"2012-10-17",)"
	       R"("Statement":[{"Effect":"Perhaps"}]})");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* NooBaa's public access block reaches the attribute map.
 *
 * Four booleans, the same four, snake_case against PascalCase. */
TEST_F(NSFSNooBaaStateTest, PublicAccessBlockComesFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no block in NooBaa's record, none in the attributes */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_PUBLIC_ACCESS), attrs.end());

  write_nb_record("ENABLED",
	       R"(,"public_access_block":{"block_public_acls":true,)"
	       R"("ignore_public_acls":false,"block_public_policy":true,)"
	       R"("restrict_public_buckets":false})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_PUBLIC_ACCESS);
  ASSERT_NE(i, attrs.end());

  PublicAccessBlockConfiguration conf;
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(conf, bi));
  EXPECT_TRUE(conf.BlockPublicAcls);
  EXPECT_FALSE(conf.IgnorePublicAcls);
  EXPECT_TRUE(conf.BlockPublicPolicy);
  EXPECT_FALSE(conf.RestrictPublicBuckets);
}

/* A field they omit stays false, the way S3 leaves one a client did
 * not send. */
TEST_F(NSFSNooBaaStateTest, AnOmittedPublicAccessFieldIsFalse)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"public_access_block":{"restrict_public_buckets":true})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_PUBLIC_ACCESS);
  ASSERT_NE(i, attrs.end());

  PublicAccessBlockConfiguration conf;
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(conf, bi));
  EXPECT_TRUE(conf.RestrictPublicBuckets);
  EXPECT_FALSE(conf.BlockPublicAcls);
}

/* A field of the wrong shape fails the bucket closed rather than
 * throwing out of load_bucket().
 *
 * JSONDecoder::decode_json() throws on a value it cannot decode, and
 * every field here comes from a file on disk, so without a boundary
 * that exception reaches RGW from the request path. */
TEST_F(NSFSNooBaaStateTest, AMisshapenFieldFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"days":"ten",)"
	       R"("mode":"GOVERNANCE"}}})");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* NooBaa's website configuration, index-document branch. */
TEST_F(NSFSNooBaaStateTest, WebsiteComesFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* A record with no website configures none, so the assertion after
   * the next one is able to fail. */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  ASSERT_FALSE(info.has_website);

  write_nb_record("ENABLED",
	       R"(,"website":{"website_configuration":{)"
	       R"("index_document":{"suffix":"index.html"},)"
	       R"("error_document":{"key":"oops.html"},)"
	       R"("routing_rules":[{)"
	       R"("condition":{"key_prefix_equals":"docs/",)"
	       R"("http_error_code_returned_equals":"404"},)"
	       R"("redirect":{"protocol":"https","host_name":"example.com",)"
	       R"("replace_key_prefix_with":"documents/",)"
	       R"("http_redirect_code":"301"}}]}})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  const auto& w = info.website_conf;
  ASSERT_TRUE(info.has_website);
  EXPECT_FALSE(w.is_redirect_all);
  EXPECT_TRUE(w.is_set_index_doc);
  EXPECT_EQ(w.index_doc_suffix, "index.html");
  EXPECT_EQ(w.error_doc, "oops.html");

  ASSERT_EQ(w.routing_rules.rules.size(), 1u);
  const auto& r = w.routing_rules.rules.front();
  EXPECT_EQ(r.condition.key_prefix_equals, "docs/");
  EXPECT_EQ(r.condition.http_error_code_returned_equals, 404);
  EXPECT_EQ(r.redirect_info.redirect.protocol, "https");
  EXPECT_EQ(r.redirect_info.redirect.hostname, "example.com");
  EXPECT_EQ(r.redirect_info.redirect.http_redirect_code, 301);
  EXPECT_EQ(r.redirect_info.replace_key_prefix_with, "documents/");
}

/* The other branch of NooBaa's anyOf, which is S3's own exclusion. */
TEST_F(NSFSNooBaaStateTest, WebsiteRedirectAllBranch)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"website":{"website_configuration":{)"
	       R"("redirect_all_requests_to":{"host_name":"elsewhere.net",)"
	       R"("protocol":"HTTPS"}}})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  const auto& w = info.website_conf;
  ASSERT_TRUE(info.has_website);
  EXPECT_TRUE(w.is_redirect_all);
  EXPECT_EQ(w.redirect_all.hostname, "elsewhere.net");
  EXPECT_EQ(w.redirect_all.protocol, "HTTPS");
  EXPECT_FALSE(w.is_set_index_doc);
}

/* A redirect code that is not a number fails the bucket closed.
 *
 * They type these as strings and RGW holds a uint16_t, so the
 * dangerous outcome is a silent zero:  a redirect that redirects with
 * code 0, or a condition on one error code turned into a condition on
 * any. */
TEST_F(NSFSNooBaaStateTest, ANonNumericRedirectCodeFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"website":{"website_configuration":{)"
	       R"("index_document":{"suffix":"index.html"},)"
	       R"("routing_rules":[{"redirect":{)"
	       R"("http_redirect_code":"moved"}}]}})");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* NooBaa's default encryption reaches the attribute map. */
TEST_F(NSFSNooBaaStateTest, EncryptionComesFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no encryption in NooBaa's record, none in the attrs */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY), attrs.end());

  write_nb_record("ENABLED",
	       R"(,"encryption":{"algorithm":"aws:kms",)"
	       R"("kms_key_id":"key-1234","bucket_key_enabled":true})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY);
  ASSERT_NE(i, attrs.end());

  RGWBucketEncryptionConfig conf;
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(conf, bi));
  ASSERT_TRUE(conf.has_rule());
  EXPECT_EQ(conf.sse_algorithm(), "aws:kms");
  EXPECT_EQ(conf.kms_master_key_id(), "key-1234");
  EXPECT_TRUE(conf.bucket_key_enabled());
}

/* NooBaa's schema requires none of the three, so a record that names no
 * algorithm configures nothing.  That is not a failure. */
TEST_F(NSFSNooBaaStateTest, AnEmptyEncryptionRecordConfiguresNothing)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED", R"(,"encryption":{})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  EXPECT_EQ(attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY), attrs.end());
}

TEST_F(NSFSNooBaaStateTest, LoggingIsReadFromNooBaaRecord)
{


  ASSERT_EQ(read_nb_record(info, attrs), 0);

  write_nb_record("DISABLED", R"(,"logging":{"log_prefix":"p/"})");

  ASSERT_EQ(read_nb_record(info, attrs), 0);

/* An encryption algorithm other than NooBaa's two, AES256 and aws:kms,
 * is stored as the bucket's configuration rather than failing its load.
 *
 * The string becomes the x-amz-server-side-encryption header value on
 * every PUT, so a write into a bucket configured with an algorithm RGW
 * does not know fails:  fail-closed, and only for writes.  Failing the
 * load would also fail reads of objects that are perfectly readable. */
TEST_F(NSFSNooBaaStateTest, AnUnknownEncryptionAlgorithmIsStoredNotRejected)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED", R"(,"encryption":{"algorithm":"rot13"})");

  ASSERT_EQ(read_nb_record(info, attrs), 0)
      << "the bucket was rejected over an algorithm that only affects writes";

  auto i = attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY);
  ASSERT_NE(i, attrs.end());
  RGWBucketEncryptionConfig conf;
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(conf, bi));
  EXPECT_EQ(conf.sse_algorithm(), "rot13");
}

/* NooBaa's bucket tag set reaches the attribute map. */
TEST_F(NSFSNooBaaStateTest, BucketTagsComeFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no tags in NooBaa's record, none in the attributes */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_TAGS), attrs.end());

  write_nb_record("ENABLED",
	       R"(,"tag":[{"key":"team","value":"storage"},)"
	       R"({"key":"env","value":"prod"}])");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_TAGS);
  ASSERT_NE(i, attrs.end());

  RGWObjTags tags;
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(tags, bi));
  const auto& m = tags.get_tags();
  ASSERT_EQ(m.size(), 2u);
  ASSERT_NE(m.find("team"), m.end());
  EXPECT_EQ(m.find("team")->second, "storage");
  ASSERT_NE(m.find("env"), m.end());
  EXPECT_EQ(m.find("env")->second, "prod");
}

/* A set larger than PutBucketTagging would accept is kept.
 *
 * NooBaa's schema constrains neither the count nor the sizes, so a
 * NooBaa bucket may carry more than fifty.  Rejecting it would fail
 * every request to it over metadata that gates nothing. */
TEST_F(NSFSNooBaaStateTest, ATagSetOverTheApiLimitIsKept)
{
  RGWBucketInfo info;
  Attrs attrs;

  std::string arr{",\"tag\":["};
  for (int n = 0; n < 60; ++n) {
    if (n) {
      arr += ",";
    }
    arr += "{\"key\":\"k" + std::to_string(n) + "\",\"value\":\"v\"}";
  }
  arr += "]";
  write_nb_record("ENABLED", arr);

  ASSERT_EQ(read_nb_record(info, attrs), 0)
      << "the bucket was rejected over a tag count";

  auto i = attrs.find(RGW_ATTR_TAGS);
  ASSERT_NE(i, attrs.end());
  RGWObjTags tags;
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(tags, bi));
  EXPECT_EQ(tags.get_tags().size(), 60u) << "tags were silently dropped";
}

/* A tag with no key is not a tag.  Nothing can be kept from it, so
 * the record is unreadable rather than partly usable. */
TEST_F(NSFSNooBaaStateTest, AKeylessTagFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED", R"(,"tag":[{"key":"","value":"orphan"}])");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* NooBaa's lifecycle rules reach the attribute map, through the same
 * parser PutBucketLifecycle uses. */
TEST_F(NSFSNooBaaStateTest, LifecycleComesFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no rules in NooBaa's record, no attribute */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_LC), attrs.end());

  write_nb_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"expire-logs","status":"Enabled",)"
	       R"("filter":{"prefix":"logs/"},)"
	       R"("expiration":{"days":30}}])");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_LC);
  ASSERT_NE(i, attrs.end());

  RGWLifecycleConfiguration lc(env->cct.get());
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(lc, bi));
  const auto& rules = lc.get_rule_map();
  ASSERT_EQ(rules.size(), 1u);
  const auto& rule = rules.begin()->second;
  EXPECT_EQ(rule.get_id(), "expire-logs");
  EXPECT_EQ(rule.get_status(), "Enabled");
  EXPECT_EQ(rule.get_filter().get_prefix(), "logs/");
  EXPECT_EQ(rule.get_expiration().get_days(), 30);
}

/* A multi-condition filter keeps its conditions.
 *
 * NooBaa's `and` flag records that the original XML wrapped them, and
 * LCFilter_S3 looks for that element first, so reproducing it is what
 * makes a prefix-and-tag filter read back as one. */
TEST_F(NSFSNooBaaStateTest, LifecycleFilterWithPrefixAndTag)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"cold","status":"Enabled",)"
	       R"("filter":{"and":true,"prefix":"data/",)"
	       R"("tags":[{"key":"class","value":"cold"}]},)"
	       R"("expiration":{"days":90}}])");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_LC);
  ASSERT_NE(i, attrs.end());

  RGWLifecycleConfiguration lc(env->cct.get());
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(lc, bi));
  ASSERT_EQ(lc.get_rule_map().size(), 1u);
  const auto& flt = lc.get_rule_map().begin()->second.get_filter();
  EXPECT_EQ(flt.get_prefix(), "data/");
  const auto& tags = flt.get_tags().get_tags();
  ASSERT_EQ(tags.size(), 1u);
  ASSERT_NE(tags.find("class"), tags.end());
  EXPECT_EQ(tags.find("class")->second, "cold");
}

/* A date-based expiration lands on the day they meant.
 *
 * NooBaa's dates are epoch milliseconds and RGW's are ISO 8601 strings
 * that check_date() requires to be exact midnight UTC.  Nothing else
 * in this file proves the conversion is right rather than merely
 * present, and it decides which day objects are deleted.
 * 1767225600000 is 2026-01-01T00:00:00Z. */
TEST_F(NSFSNooBaaStateTest, ALifecycleDateKeepsItsDay)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"newyear","status":"Enabled","filter":{},)"
	       R"("expiration":{"date":1767225600000}}])");

  ASSERT_EQ(read_nb_record(info, attrs), 0)
      << "a midnight date was rejected";

  auto i = attrs.find(RGW_ATTR_LC);
  ASSERT_NE(i, attrs.end());
  RGWLifecycleConfiguration lc(env->cct.get());
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(lc, bi));
  ASSERT_EQ(lc.get_rule_map().size(), 1u);

  const std::string date =
    lc.get_rule_map().begin()->second.get_expiration().get_date();
  EXPECT_EQ(date.substr(0, 10), "2026-01-01") << "stored date: " << date;
}

/* A rule NooBaa's schema allows and PutBucketLifecycle rejects fails the
 * bucket closed.
 *
 * NooBaa's expiration may carry days and a date together;
 * LCExpiration_S3 requires exactly one, and guessing which wins
 * deletes objects on the wrong day. */
TEST_F(NSFSNooBaaStateTest, ALifecycleRuleRgwWouldRejectFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"both","status":"Enabled","filter":{},)"
	       R"("expiration":{"days":30,"date":1767225600000}}])");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* A rule with no action at all is one RGW rejects too. */
TEST_F(NSFSNooBaaStateTest, ALifecycleRuleWithNoActionFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"empty","status":"Enabled","filter":{}}])");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* NooBaa's CORS rules reach the attribute map, through the same parser
 * PutBucketCors uses. */
TEST_F(NSFSNooBaaStateTest, CorsComesFromNooBaaRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no rules in NooBaa's record, no attribute */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_CORS), attrs.end());

  write_nb_record("ENABLED",
	       R"(,"cors_configuration_rules":[{"id":"web",)"
	       R"("allowed_methods":["GET","PUT"],)"
	       R"("allowed_origins":["https://example.com"],)"
	       R"("allowed_headers":["x-amz-date"],)"
	       R"("expose_headers":["ETag"],)"
	       R"("max_age_seconds":3000}])");

  ASSERT_EQ(read_nb_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_CORS);
  ASSERT_NE(i, attrs.end());

  RGWCORSConfiguration cors;
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(cors, bi));
  ASSERT_EQ(cors.get_rules().size(), 1u);

  RGWCORSRule* rule = cors.host_name_rule("https://example.com");
  ASSERT_NE(rule, nullptr) << "the origin did not survive";
  EXPECT_EQ(rule->get_max_age(), 3000u);
  EXPECT_TRUE(rule->is_header_allowed("x-amz-date", strlen("x-amz-date")));
}

/* A method outside the set RGW knows fails the bucket closed.
 *
 * NooBaa's schema constrains allowed_methods to nothing at all, while
 * RGWCORSRule_S3 rejects anything but GET, POST, DELETE, HEAD, PUT
 * and COPY -- and a CORS rule decides which origins a browser may
 * reach. */
TEST_F(NSFSNooBaaStateTest, AnUnknownCorsMethodFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"cors_configuration_rules":[{)"
	       R"("allowed_methods":["TRACE"],)"
	       R"("allowed_origins":["https://example.com"]}])");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* A rule with no origin is one NooBaa's own schema requires and RGW
 * rejects, so an empty list fails closed rather than becoming a rule
 * that matches nothing. */
TEST_F(NSFSNooBaaStateTest, ACorsRuleWithNoOriginFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_nb_record("ENABLED",
	       R"(,"cors_configuration_rules":[{)"
	       R"("allowed_methods":["GET"],"allowed_origins":[]}])");

  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);
}

/* A record we cannot make sense of fails the bucket closed, the same
 * as unreadable nsfs state. */
TEST_F(NSFSNooBaaStateTest, AnUnreadableRecordFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* A well-formed record is read first, so the -EBADMSG below is this
   * record being rejected rather than the reader rejecting everything
   * it is given. */
  write_nb_record("ENABLED");
  ASSERT_EQ(read_nb_record(info, attrs), 0);

  write_file(conf_root() / "buckets" / (testname + ".json"),
	     "this is not JSON");
  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG);

  write_nb_record("PERHAPS");
  EXPECT_EQ(read_nb_record(info, attrs), -EBADMSG)
      << "a versioning value that is none of theirs was accepted";
}

/* Every listing row reports the same owner for an object nothing wrote
 * through RGW.
 *
 * Such an object records no owner and is its bucket's owner's
 * (DECISIONS.md 2026-10-11).  The version entry once listed a
 * different owner from the object's own -- `unknown` beside an owner --
 * so both are asserted, and asserted equal.
 *
 * Two controls, because without them the comparison could not fail.
 * The first asserts the files carry no extended attribute at all, so
 * no recorded ACL can be supplying the owner.  The second asserts the
 * owner is the bucket's and not the file's uid, which is what this
 * listed before. */
TEST_F(NSFSNooBaaStateTest, EveryListingRowAgreesOnASideloadedOwner)
{
  remove_rgw_bucket_info();
  write_nb_record("ENABLED");
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  ASSERT_TRUE(bucket->get_info().versioned())
      << "the version rows are only walked on a versioned bucket";

  const sf::path leaf = bucket_path() / "external.txt";
  const sf::path vdir = bucket_path() / ".versions";
  sf::create_directories(vdir);
  write_file(leaf, "sideloaded content");
  /* a real version-id suffix, not _null:  the current object with no
   * version-id attribute already reads as the null version, so a
   * .versions/ entry named _null would collide with it on the cache
   * key (name '\0' instance) and one row would replace the other. */
  const std::string vent = "external.txt_mtime-zzzz-ino-zzzz";
  write_file(vdir / vent, "an older sideloaded body");

  /* control one:  neither file carries a user.* attribute, so no
   * recorded ACL can be supplying the owner.  Only user.* is tested
   * because the kernel supplies security.selinux on every file here,
   * and that is not a name any strategy reads. */
  auto no_user_xattrs = [](const sf::path& f) {
    char names[4096];
    ssize_t len = ::listxattr(f.c_str(), names, sizeof(names));
    if (len < 0) {
      return std::string{"listxattr failed on "} + f.string();
    }
    for (ssize_t i = 0; i < len; i += (ssize_t)strlen(names + i) + 1) {
      if (std::string_view(names + i).starts_with("user.")) {
        return std::string{names + i} + " on " + f.string();
      }
    }
    return std::string{};
  };
  ASSERT_EQ(no_user_xattrs(leaf), "");
  ASSERT_EQ(no_user_xattrs(vdir / vent), "");

  /* control two:  the owner is the bucket's, not the uid */
  const std::string expect = to_string(bucket->get_info().owner);
  ASSERT_NE(expect, std::to_string(::getuid()));

  rgw::sal::Bucket::ListParams params;
  params.list_versions = true;
  rgw::sal::Bucket::ListResults results;
  ASSERT_EQ(bucket->list(env->dpp, params, 1000, results, null_yield), 0);

  std::string current_owner, version_owner;
  for (const auto& e : results.objs) {
    if (e.key.name != "external.txt") {
      continue;
    }
    const bool is_noncurrent =
        (e.flags & rgw_bucket_dir_entry::FLAG_VER) &&
        !(e.flags & rgw_bucket_dir_entry::FLAG_CURRENT);
    if (is_noncurrent) {
      version_owner = e.meta.owner;
    } else {
      current_owner = e.meta.owner;
    }
  }
  ASSERT_FALSE(current_owner.empty()) << "no row for the object itself";
  ASSERT_FALSE(version_owner.empty()) << "no row for its version";

  EXPECT_EQ(current_owner, expect) << "the object's own row";
  EXPECT_EQ(version_owner, expect) << "the version row under .versions/";
  EXPECT_EQ(current_owner, version_owner)
      << "two rows for one object disagree about who owns it";
}

/* An upload NooBaa started, enumerated by us.
 *
 * The tree is written by hand in NooBaa's shape -- not through NooBaa's
 * strategy -- so agreement between the two is a comparison rather
 * than a tautology.  This is the S5 gate for the multipart step:  a
 * tree in NooBaa's shape which nsfs then reads correctly. */
TEST_F(NSFSNooBaaBucketTest, ListsAnUploadTheyStarted)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  std::string marker;
  bool truncated = false;
  ASSERT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 100, uploads,
				    nullptr, &truncated, null_yield,
				    rgw::sal::no_authenticated_user), 0);

  ASSERT_EQ(uploads.size(), 1u);
  EXPECT_EQ(uploads[0]->get_key(), tree.key);
  EXPECT_EQ(uploads[0]->get_upload_id(), tree.upload_id);
  EXPECT_FALSE(truncated);

  /* NooBaa's upload names no initiator, so it is the bucket's owner's
   * (DECISIONS.md 2026-10-11) -- not the staging directory's uid,
   * which is what this returned before. */
  EXPECT_EQ(uploads[0]->get_owner().id, bucket->get_info().owner);
  EXPECT_NE(to_string(uploads[0]->get_owner().id),
	    std::to_string(::getuid()));
}

/* An nsfs upload lists the initiator its record holds.
 *
 * The control for ListsAnUploadTheyStarted:  the bucket's owner is
 * only for an upload that records none, so an upload initiated by
 * somebody else must keep that somebody, or the assertion there could
 * pass by giving every upload the bucket's owner. */
TEST_F(NSFSBucketTest, ListsTheInitiatorAnNsfsUploadRecords)
{
  std::unique_ptr<rgw::sal::MultipartUpload> upload =
    bucket->get_multipart_upload("initiated.bin", rgw::sal::no_authenticated_user, "cafe01");
  ACLOwner initiator;
  initiator.id = rgw_user("", "somebody-else");
  initiator.display_name = "Somebody Else";
  ASSERT_NE(rgw_owner{initiator.id}, bucket->get_info().owner);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, initiator, placement, attrs),
	    0);

  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  std::string marker;
  bool truncated = false;
  ASSERT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 100, uploads,
				    nullptr, &truncated, null_yield,
				    rgw::sal::no_authenticated_user), 0);
  ASSERT_EQ(uploads.size(), 1u);
  EXPECT_EQ(uploads[0]->get_owner().id, rgw_owner{initiator.id})
      << "an upload's recorded initiator was replaced by the bucket's owner";
}

/* A NooBaa bucket which has never taken an upload has no
 * multipart-uploads/ directory, and that is an empty listing rather
 * than an error. */
TEST_F(NSFSNooBaaBucketTest, ListsNothingBeforeAnyUpload)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);

  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  std::string marker;
  bool truncated = false;
  EXPECT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 100, uploads,
				    nullptr, &truncated, null_yield,
				    rgw::sal::no_authenticated_user), 0);
  EXPECT_TRUE(uploads.empty());
  EXPECT_FALSE(truncated);
}

/* NooBaa's parts, read through NooBaa's record:  size and offset from
 * user.noobaa.part_size and part_offset, and an etag derived from the
 * stat because the fixture computed no digest -- which is what NooBaa's
 * own _get_etag() falls back to. */
TEST_F(NSFSNooBaaBucketTest, ListsThePartsOfNooBaaUpload)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
  ASSERT_NE(upload.get(), nullptr);

  int next = 0;
  bool truncated = false;
  ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
			       &truncated, null_yield), 0);

  const auto& parts = upload->get_parts();
  ASSERT_EQ(parts.size(), tree.parts);
  for (uint32_t k = 1; k <= tree.parts; ++k) {
    auto it = parts.find(k);
    ASSERT_NE(it, parts.end()) << k;
    EXPECT_EQ(it->second->get_size(), tree.part_size) << k;
    EXPECT_FALSE(it->second->get_etag().empty()) << k;
  }
}

TEST_F(NSFSNooBaaBucketTest, CompletesAnUploadTheyStarted)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
  ASSERT_NE(upload.get(), nullptr);

  int next = 0;
  bool truncated = false;
  ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
			       &truncated, null_yield), 0);

  /* complete with the etags the listing reported, as a client would */
  std::map<int, std::string> part_etags;
  for (auto& [num, part] : upload->get_parts()) {
    part_etags[num] = part->get_etag();
  }
  ASSERT_EQ(part_etags.size(), tree.parts);

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, tree.key,
			part_etags), 0);

  const sf::path obj{bucket_path() / tree.key};
  ASSERT_TRUE(sf::is_regular_file(obj)) << obj;
  EXPECT_EQ(read_all(obj), tree.payload);
}


/* get_info() on an upload they started.
 *
 * It decoded the nsfs record off the meta object and returned -EIO
 * when there was none, which is every upload NooBaa started -- NooBaa's
 * create_object_upload is a document and the file carries no
 * nsfs attribute.  Three ops ask for it, all of them through the
 * op layer:  UploadPart (`rgw_op.cc:4859`), CompleteMultipartUpload
 * (`:7713`) and ListParts (`:8120`).  So in-flight interop was 500 on
 * every one, and nothing here caught it because the tests call
 * complete() directly and never ask for a placement rule.
 *
 * What comes back is what NooBaa's format holds.  Placement has no
 * counterpart -- a placement rule is an RGW concept, inert on a
 * filesystem -- and neither does the checksum family, because NooBaa's
 * CreateMultipartUpload never persisted one.
 */
TEST_F(NSFSNooBaaBucketTest, GetInfoOnAnUploadTheyStarted)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule* rule{nullptr};
  Attrs attrs;
  ASSERT_EQ(upload->get_info(env->dpp, null_yield, &rule, &attrs), 0)
      << "get_info failed on an upload in their format";
  ASSERT_NE(rule, nullptr);
}

/* And it carries across what NooBaa's document holds.
 *
 * The fixture's upload has no create-time metadata, so this one is
 * written with some:  the storage class and the lock settings reach
 * multipart_upload_info, and the content type and user metadata reach
 * the attribute set, which is where RGW keeps them and where a
 * completion looks.
 */
TEST_F(NSFSNooBaaBucketTest, GetInfoCarriesNooBaaCreateParams)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);

  const std::string key = "rich/upload.bin";
  const std::string upload_id = "11111111-2222-4333-8444-555555555555";

  /* NooBaa's document, by hand, as NooBaa's gateway writes it */
  sf::path staging;
  for (auto& e : sf::directory_iterator(bucket_path())) {
    if (e.path().filename().string().starts_with(".noobaa-nsfs_")) {
      staging = e.path() / "multipart-uploads" / upload_id;
    }
  }
  ASSERT_FALSE(staging.empty());
  sf::create_directories(staging);
  write_file(staging / "create_object_upload",
	     "{\"key\":\"" + key + "\",\"bucket\":\"b\","
	     "\"content_type\":\"text/plain\","
	     "\"content_encoding\":\"gzip\","
	     "\"storage_class\":\"GLACIER\","
	     "\"xattr\":{\"colour\":\"green\"},"
	     "\"lock_settings\":{"
	     "\"retention\":{\"mode\":\"COMPLIANCE\","
	     "\"retain_until_date\":\"2030-01-01T00:00:00.000Z\"},"
	     "\"legal_hold\":{\"status\":\"ON\"}}}");

  auto upload = bucket->get_multipart_upload(key, rgw::sal::no_authenticated_user, upload_id);
  rgw_placement_rule* rule{nullptr};
  Attrs attrs;
  ASSERT_EQ(upload->get_info(env->dpp, null_yield, &rule, &attrs), 0);
  ASSERT_NE(rule, nullptr);

  EXPECT_EQ(rule->storage_class, "GLACIER");

  /* attributes, which is where RGW keeps these and where a completion
   * reads them */
  auto ct = attrs.find(RGW_ATTR_CONTENT_TYPE);
  ASSERT_NE(ct, attrs.end()) << "their content type did not come across";
  EXPECT_EQ(ct->second.to_str(), "text/plain");
  auto ce = attrs.find(RGW_ATTR_CONTENT_ENC);
  ASSERT_NE(ce, attrs.end());
  EXPECT_EQ(ce->second.to_str(), "gzip");
  auto meta = attrs.find(std::string(RGW_ATTR_META_PREFIX) + "colour");
  ASSERT_NE(meta, attrs.end()) << "their user metadata did not come across";
  EXPECT_EQ(meta->second.to_str(), "green");
}

/* The nsfs format still returns from its own record.
 *
 * The control:  without it the two above pass for a driver that reads
 * NooBaa's document everywhere, and an nsfs upload would lose the
 * placement and the checksum that only the nsfs record carries. */
TEST_F(NSFSBucketTest, GetInfoOnAnNsfsUpload)
{
  const std::string objname = "ours.bin";
  auto upload = bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, "c0ffee");
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  placement.name = "default-placement";
  /* not STANDARD:  an empty storage class IS standard --
   * get_canonical_storage_class() returns it for one -- so asserting
   * it would pass against a record that carried nothing */
  placement.storage_class = "GLACIER";
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  auto reread = bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, "c0ffee");
  rgw_placement_rule* rule{nullptr};
  ASSERT_EQ(reread->get_info(env->dpp, null_yield, &rule, nullptr), 0);
  ASSERT_NE(rule, nullptr);
  EXPECT_EQ(rule->name, "default-placement");
  EXPECT_EQ(rule->storage_class, "GLACIER");
}

/* The control:  the same write in a strong bucket keeps the nsfs names.
 *
 * Without it the test above passes for a driver that writes NooBaa's
 * names everywhere, which is the other half of the same defect. */
TEST_F(NSFSBucketTest, ObjectsInAMarkedBucketKeepNsfsNames)
{
  const std::string objname = "plain.bin";
  auto obj = bucket->get_object(rgw_obj_key(objname));
  auto writer = driver->get_atomic_writer(env->dpp, null_yield, obj.get(),
					  acl_owner, nullptr, 0, testname,
					  rgw::sal::no_authenticated_user);
  ASSERT_EQ(writer->prepare(null_yield), 0);
  bufferlist bl;
  bl.append("some bytes");
  const int len = bl.length();
  ASSERT_EQ(writer->process(std::move(bl), 0), 0);
  ASSERT_EQ(writer->process({}, len), 0);

  Attrs attrs;
  bufferlist ct;
  ct.append("text/plain");
  attrs[RGW_ATTR_CONTENT_TYPE] = ct;

  ceph::real_time mtime;
  std::string etag;
  req_context rctx{env->dpp, null_yield, nullptr};
  ASSERT_EQ(writer->complete(len, etag, &mtime, real_time(), attrs,
			     std::nullopt, real_time(), nullptr, nullptr,
			     nullptr, nullptr, nullptr, rctx, 0), 0);

  char buf[8192];
  const sf::path p{bp / "root" / testname / objname};
  ssize_t nlen = ::listxattr(p.c_str(), buf, sizeof(buf));
  ASSERT_GT(nlen, 0);
  std::set<std::string> names;
  for (const char* q = buf; q < buf + nlen; q += strlen(q) + 1) {
    names.insert(q);
  }
  EXPECT_TRUE(names.contains("user.nsfs:rgw.content_type"));
  EXPECT_FALSE(names.contains("user.noobaa.content_type"));
}

/* A bucket mid-upgrade:  marked as nsfs, and still holding objects in
 * NooBaa's format.
 *
 * The bucket is created marked, which is what every bucket this
 * gateway makes is, and then told it is converting -- which is what
 * the upgrade command will do at its start and undo when it finishes.
 * Nothing else may set that marker.
 */
class NSFSConvertingBucketTest : public NSFSBucketTest {
public:
  void SetUp() override {
    NSFSBucketTest::SetUp();
    auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
    ASSERT_EQ(b->resolve_profile(env->dpp), 0);
    ASSERT_STREQ(b->profile_name(), "shared");
    ASSERT_EQ(b->mark_converting(env->dpp), 0);
    ASSERT_TRUE(b->is_converting());
  }

  sf::path bucket_path() const { return bp / "root" / testname; }

  /* a NooBaa object, laid down by hand:  NooBaa's attribute names,
   * with no nsfs attribute anywhere on it */
  void write_nb_object(const std::string& name) {
    const sf::path p{bucket_path() / name};
    write_file(p, "their bytes");
    const std::string ct{"text/plain"};
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.content_type",
			 ct.data(), ct.size(), 0), 0);
    const std::string md5{"0123456789abcdef0123456789abcdef"};
    ASSERT_EQ(::setxattr(p.c_str(), "user.content_md5",
			 md5.data(), md5.size(), 0), 0);
  }
};

/* NooBaa's attributes are read on a converting bucket. */
TEST_F(NSFSConvertingBucketTest, ReadsNooBaaAttributes)
{
  write_nb_object("theirs.bin");

  auto obj = bucket->get_object(rgw_obj_key("theirs.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);

  auto& attrs = obj->get_attrs();
  auto ct = attrs.find(RGW_ATTR_CONTENT_TYPE);
  ASSERT_NE(ct, attrs.end()) << "content type was dropped as foreign";
  EXPECT_EQ(ct->second.to_str(), "text/plain");
  auto etag = attrs.find(RGW_ATTR_ETAG);
  ASSERT_NE(etag, attrs.end()) << "etag was dropped as foreign";
  EXPECT_EQ(etag->second.to_str(), "0123456789abcdef0123456789abcdef");
}

/* And are not, on a bucket that is not converting.
 *
 * The control the chain needs:  without it the test above passes for a
 * driver that reads both formats everywhere, which is the arrangement
 * the marker exists to avoid. */
TEST_F(NSFSBucketTest, DoesNotReadNooBaaAttributesWithoutTheMarker)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  ASSERT_FALSE(b->is_converting());

  const sf::path p{bp / "root" / testname / "theirs.bin"};
  write_file(p, "their bytes");
  const std::string ctype{"text/plain"};
  ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.content_type",
		       ctype.data(), ctype.size(), 0), 0);
  const std::string md5{"0123456789abcdef0123456789abcdef"};
  ASSERT_EQ(::setxattr(p.c_str(), "user.content_md5",
		       md5.data(), md5.size(), 0), 0);

  auto obj = bucket->get_object(rgw_obj_key("theirs.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);

  /* Both keys are present either way:  the driver guesses a content
   * type from the name and synthesizes an etag from the stat when an
   * object carries neither.  So the values are what distinguish the
   * two cases, and what is asserted is that neither came from NooBaa's
   * attribute. */
  auto& attrs = obj->get_attrs();
  auto ct = attrs.find(RGW_ATTR_CONTENT_TYPE);
  if (ct != attrs.end()) {
    EXPECT_NE(ct->second.to_str(), "text/plain")
	<< "their content type was read on a bucket that is not converting";
  }
  auto etag = attrs.find(RGW_ATTR_ETAG);
  if (etag != attrs.end()) {
    EXPECT_NE(etag->second.to_str(), "0123456789abcdef0123456789abcdef")
	<< "their etag was read on a bucket that is not converting";
  }
}

/* A NooBaa directory object, which the walk cannot see.
 *
 * NooBaa's empty one has no sentinel:  the fact is
 * user.noobaa.dir_content on the directory.  This is the consumer the
 * marker is worth having for -- it is a syscall per subdirectory, and
 * the attribute chain is not. */
TEST_F(NSFSConvertingBucketTest, ListsNooBaaEmptyDirectoryObject)
{
  sf::create_directories(bucket_path() / "photos");
  set_u64_xattr(bucket_path() / "photos", "user.noobaa.dir_content", 0);
  /* an ordinary prefix directory beside it, which must not be emitted */
  sf::create_directories(bucket_path() / "plain");
  write_file(bucket_path() / "plain" / "a.txt", "x");

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  ASSERT_EQ(bucket->list(env->dpp, params, 128, results, null_yield), 0);

  std::set<std::string> keys;
  for (auto& o : results.objs) {
    keys.insert(o.key.name);
  }
  EXPECT_TRUE(keys.contains("photos/")) << "their folder was invisible";
  EXPECT_TRUE(keys.contains("plain/a.txt"));
  EXPECT_FALSE(keys.contains("plain/")) << "a prefix was emitted as an object";
}

/* NooBaa's directory objects on a bucket mid-conversion, before the
 * sweep reaches them:  read through the fallback, with the directory's
 * attributes. */
TEST_F(NSFSConvertingBucketTest, ReadsNooBaaDirectoryObjects)
{
  lay_noobaa_directory_object(bucket_path() / "e", "", "text/x-e", "e");
  lay_noobaa_directory_object(bucket_path() / "c", "dir bytes", "text/x-c", "c");

  const ClientView e = client_view(bucket.get(), env->dpp, "e/");
  ASSERT_TRUE(e.exists);
  EXPECT_EQ(e.size, 0u);
  EXPECT_EQ(e.content_type, "text/x-e");
  EXPECT_EQ(e.probe, "e");
  EXPECT_TRUE(e.listed);

  const ClientView c = client_view(bucket.get(), env->dpp, "c/");
  ASSERT_TRUE(c.exists);
  EXPECT_EQ(c.size, 9u);
  EXPECT_EQ(c.content_type, "text/x-c");
  EXPECT_EQ(c.probe, "c");
  EXPECT_EQ(c.body, "dir bytes");
  EXPECT_TRUE(c.listed);
  EXPECT_EQ(c.listed_size, 9u);
}

  lay_noobaa_directory_object(bucket_path() / "e", "", "text/x-e", "e");
  lay_noobaa_directory_object(bucket_path() / "c", "dir bytes", "text/x-c", "c");

  for (const char* k : {"e/", "c/"}) {
    auto obj = bucket->get_object(rgw_obj_key(k));

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->convert_tree(env->dpp, null_yield, nullptr, nullptr), 0);
  for (const char* k : {"e/", "c/"}) {
    const ClientView v = client_view(bucket.get(), env->dpp, k);
  lay_noobaa_directory_object(bucket_path() / "c", "dir bytes", "text/x-c", "c");
  const ClientView v0 = client_view(bucket.get(), env->dpp, "c/");
  ASSERT_TRUE(v0.exists);

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->convert_tree(env->dpp, null_yield, nullptr, nullptr), 0);
  const ClientView v1 = client_view(bucket.get(), env->dpp, "c/");
  lay_noobaa_directory_object(bucket_path() / "e", "", "text/x-e", "e");

  Attrs attrs;
  bufferlist v;
  v.append("changed");
  attrs[RGW_ATTR_META_PREFIX "probe"] = v;
  {

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->convert_tree(env->dpp, null_yield, nullptr, nullptr), 0);
  const ClientView v1 = client_view(bucket.get(), env->dpp, "e/");
  write_nb_object("w");

TEST_F(NSFSBucketTest, DoesNotListNooBaaDirectoryObjectWithoutTheMarker)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  ASSERT_FALSE(b->is_converting());

  sf::create_directories(bp / "root" / testname / "photos");
  set_u64_xattr(bp / "root" / testname / "photos",
		"user.noobaa.dir_content", 0);

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  ASSERT_EQ(bucket->list(env->dpp, params, 128, results, null_yield), 0);
  for (auto& o : results.objs) {
    EXPECT_NE(o.key.name, "photos/");
  }
}

  write_nb_object("theirs.bin");
  const sf::path p{bucket_path() / "theirs.bin"};
  for (const auto& [k, v] : {std::pair{"a", "1"}, std::pair{"b", "2"}}) {
    const std::string n = std::string("user.noobaa.tag.") + k;
    ASSERT_EQ(::setxattr(p.c_str(), n.c_str(), v, strlen(v), 0), 0) << n;
  }
  Attrs del;
  del[RGW_ATTR_TAGS] = bufferlist{};

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->convert_tree(env->dpp, null_yield, nullptr, nullptr), 0);
  EXPECT_TRUE(after.contains("user.nsfs:rgw.content_type"));

/* Object lock:  three plain NooBaa attributes, two encoded nsfs
 * ones.
 *
 * Legal hold is one-to-one and still needs the widened pair, because
 * the shapes differ -- NooBaa's is the literal `ON` and nsfs's is an
 * encoded RGWObjectLegalHold.  Retention is two-to-one, and is both
 * attributes or neither:  NooBaa's reader returns undefined unless mode
 * and date are both present (`namespace_fs.js:2890`).
 */
TEST_F(NSFSNooBaaBucketTest, ReadsNooBaaObjectLock)
{
  const sf::path p{bucket_path() / "locked.bin"};
  write_file(p, "bytes");
  auto set = [&p](const char* n, const std::string& v) {
    ASSERT_EQ(::setxattr(p.c_str(), n, v.data(), v.size(), 0), 0) << n;
  };
  set("user.noobaa.legal_hold", "ON");
  set("user.noobaa.retention_mode", "COMPLIANCE");
  set("user.noobaa.retention_date", "2030-01-01T00:00:00.000Z");

  auto obj = bucket->get_object(rgw_obj_key("locked.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);
  auto& attrs = obj->get_attrs();

  auto lh = attrs.find(RGW_ATTR_OBJECT_LEGAL_HOLD);
  ASSERT_NE(lh, attrs.end()) << "their legal hold was not read";
  RGWObjectLegalHold hold;
  auto hi = lh->second.cbegin();
  hold.decode(hi);
  EXPECT_EQ(hold.get_status(), "ON");

  auto rt = attrs.find(RGW_ATTR_OBJECT_RETENTION);
  ASSERT_NE(rt, attrs.end()) << "their retention was not read";
  RGWObjectRetention ret;
  auto ri = rt->second.cbegin();
  ret.decode(ri);
  EXPECT_EQ(ret.get_mode(), "COMPLIANCE");
  /* the date survived the ISO 8601 round trip, which is the part that
   * could silently produce the epoch */
  std::string iso;
  rgw_to_iso8601(ret.get_retain_until_date(), &iso);
  EXPECT_EQ(iso.substr(0, 10), "2030-01-01") << "got " << iso;
}

/* A retention with only one of its two attributes is no retention.
 *
 * NooBaa's rule, and the safe one:  a retention holding the epoch
 * because a field was missing reads as expired, which is the one
 * wrong result object lock must never return. */
TEST_F(NSFSNooBaaBucketTest, AHalfWrittenRetentionIsNoRetention)
{
  const sf::path p{bucket_path() / "half.bin"};
  write_file(p, "bytes");
  const std::string mode{"GOVERNANCE"};
  ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.retention_mode",
		       mode.data(), mode.size(), 0), 0);

  auto obj = bucket->get_object(rgw_obj_key("half.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);
  EXPECT_EQ(obj->get_attrs().find(RGW_ATTR_OBJECT_RETENTION),
	    obj->get_attrs().end());

  /* and a date this gateway cannot parse is the same result */
  const sf::path q{bucket_path() / "bad.bin"};
  write_file(q, "bytes");
  ASSERT_EQ(::setxattr(q.c_str(), "user.noobaa.retention_mode",
		       mode.data(), mode.size(), 0), 0);
  const std::string bad{"not-a-date"};
  ASSERT_EQ(::setxattr(q.c_str(), "user.noobaa.retention_date",
		       bad.data(), bad.size(), 0), 0);
  auto b = bucket->get_object(rgw_obj_key("bad.bin"));
  ASSERT_EQ(b->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(b->get_obj_attrs(null_yield, env->dpp), 0);
  EXPECT_EQ(b->get_attrs().find(RGW_ATTR_OBJECT_RETENTION),
	    b->get_attrs().end())
      << "an unparseable date became a retention, which would read as "
	 "expired";
}

/* The control:  a marked bucket keeps nsfs's encoded. */
TEST_F(NSFSBucketTest, ObjectLockInAMarkedBucketStaysEncoded)
{
  const sf::path p{bp / "root" / testname / "mine.bin"};
  write_file(p, "bytes");

  auto obj = bucket->get_object(rgw_obj_key("mine.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  RGWObjectLegalHold hold("ON");
  Attrs set;
  bufferlist hb;
  hold.encode(hb);
  set[RGW_ATTR_OBJECT_LEGAL_HOLD] = hb;
  ASSERT_EQ(obj->set_obj_attrs(env->dpp, &set, nullptr, null_yield,
			       rgw::sal::FLAG_LOG_OP, rgw_user()), 0);

  char buf[8192];
  ssize_t len = ::listxattr(p.c_str(), buf, sizeof(buf));
  ASSERT_GT(len, 0);
  std::set<std::string> names;
  for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
    names.insert(q);
  }
  EXPECT_FALSE(names.contains("user.noobaa.legal_hold"))
      << "wrote their shape in a bucket of ours";
  EXPECT_TRUE(names.contains("user.nsfs:rgw.object-legal-hold"));
}

/* Storage class:  a plain rename that was missed, and a bogus
 * metadata key with it.
 *
 * NooBaa's is `user.storage_class` (`glacier.js:48`) and is not under
 * `user.noobaa.`, so before the mapping existed it fell through to
 * "what is left under user. is user metadata" and came back as
 * `x-amz-meta-storage_class` -- a key no client ever set and one
 * NooBaa's own gateway does not report, because it is the sole member of
 * XATTR_METADATA_IGNORE_LIST (`namespace_fs.js:111`).
 */
TEST_F(NSFSNooBaaBucketTest, ReadsNooBaaStorageClass)
{
  const sf::path p{bucket_path() / "cold.bin"};
  write_file(p, "bytes");
  const std::string sc{"GLACIER"};
  ASSERT_EQ(::setxattr(p.c_str(), "user.storage_class",
		       sc.data(), sc.size(), 0), 0);

  auto obj = bucket->get_object(rgw_obj_key("cold.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);

  auto& attrs = obj->get_attrs();
  auto i = attrs.find(RGW_ATTR_STORAGE_CLASS);
  ASSERT_NE(i, attrs.end()) << "their storage class was not read";
  EXPECT_EQ(i->second.to_str(), "GLACIER");

  /* and not as metadata a client never set */
  EXPECT_EQ(attrs.find(std::string(RGW_ATTR_META_PREFIX) + "storage_class"),
	    attrs.end())
      << "their storage class surfaced as user metadata";
}

/* Object tags, which are one attribute per tag in NooBaa's format and
 * one encoded RGWObjTags in nsfs's.
 *
 * The first attribute whose SHAPE differs rather than its name, so
 * disk_name()/parse_disk_name() cannot express it -- they rename a
 * key and let the bytes through.  It has to work through the
 * attribute map because that is how rgw_op.cc reads and writes tags
 * (`attrs.find(RGW_ATTR_TAGS)`, `modify_obj_attrs`,
 * `delete_obj_attrs`), which is generic code we do not control.
 */
class NSFSTagTest : public NSFSNooBaaBucketTest {
public:
  static std::set<std::string> xattr_names(const sf::path& p) {
    char buf[8192];
    std::set<std::string> names;
    ssize_t len = ::listxattr(p.c_str(), buf, sizeof(buf));
    if (len <= 0) {
      return names;
    }
    for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
      names.insert(q);
    }
    return names;
  }

  static std::string xattr(const sf::path& p, const char* n) {
    char buf[1024];
    ssize_t len = ::getxattr(p.c_str(), n, buf, sizeof(buf));
    return (len > 0) ? std::string(buf, len) : std::string{};
  }

  /* a NooBaa object with tags, laid down by hand */
  void nb_tagged_object(const std::string& name,
			   const std::map<std::string, std::string>& tags) {
    const sf::path p{bucket_path() / name};
    sf::create_directories(p.parent_path());
    write_file(p, "bytes");
    for (const auto& [k, v] : tags) {
      const std::string n = "user.noobaa.tag." + k;
      ASSERT_EQ(::setxattr(p.c_str(), n.c_str(), v.data(), v.size(), 0), 0)
	  << n;
    }
  }

  static RGWObjTags decode_tags(const Attrs& attrs) {
    RGWObjTags t;
    auto i = attrs.find(RGW_ATTR_TAGS);
    if (i != attrs.end()) {
      auto bufit = i->second.cbegin();
      t.decode(bufit);
    }
    return t;
  }
};

/* NooBaa's tags are read as nsfs's. */
TEST_F(NSFSTagTest, ReadsNooBaaTags)
{
  nb_tagged_object("tagged.bin", {{"colour", "green"}, {"size", "large"}});

  auto obj = bucket->get_object(rgw_obj_key("tagged.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);

  auto tags = decode_tags(obj->get_attrs());
  ASSERT_EQ(tags.get_tags().size(), 2u) << "their tags were not read";
  EXPECT_EQ(tags.get_tags().find("colour")->second, "green");
  EXPECT_EQ(tags.get_tags().find("size")->second, "large");
}

/* The control:  a marked bucket still keeps one encoded set.
 *
 * Without it the tests above pass for a driver that writes NooBaa's
 * tag shape everywhere, which would be the same defect in the other
 * direction. */
TEST_F(NSFSBucketTest, TagsInAMarkedBucketAreOneAttribute)
{
  const sf::path p{bp / "root" / testname / "mine.bin"};
  write_file(p, "bytes");

  auto obj = bucket->get_object(rgw_obj_key("mine.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  RGWObjTags tags;
  tags.add_tag("colour", "blue");
  bufferlist bl;
  tags.encode(bl);
  Attrs set;
  set[RGW_ATTR_TAGS] = bl;
  ASSERT_EQ(obj->set_obj_attrs(env->dpp, &set, nullptr, null_yield,
			       rgw::sal::FLAG_LOG_OP, rgw_user()), 0);

  char buf[8192];
  ssize_t len = ::listxattr(p.c_str(), buf, sizeof(buf));
  ASSERT_GT(len, 0);
  std::set<std::string> names;
  for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
    names.insert(q);
  }
  EXPECT_FALSE(names.contains("user.noobaa.tag.colour"))
      << "wrote their tag shape in a bucket of ours";
  bool encoded = false;
  for (const auto& n : names) {
    if (n.find("x-amz-tagging") != std::string::npos) {
      encoded = true;
    }
  }
  EXPECT_TRUE(encoded) << "our encoded tag set was not written";
}

/* The upgrade:  a bucket in NooBaa's format moved to an nsfs one,
 * with its tree rewritten.
 *
 * Starts from base, which is what a tree we adopted is, and asks for
 * strong -- which is what set_profile() does for an operator through
 * /admin/nsfs/profile.
 */
class NSFSUpgradeTest : public NSFSNooBaaBucketTest {
public:
  /* a NooBaa object, by hand:  NooBaa's names, nothing of nsfs's */
  void nb_object(const std::string& name, const char* ctype) {
    const sf::path p{bucket_path() / name};
    sf::create_directories(p.parent_path());
    write_file(p, "their bytes");
    const std::string ct{ctype};
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.content_type",
			 ct.data(), ct.size(), 0), 0);
    const std::string md5{"0123456789abcdef0123456789abcdef"};
    ASSERT_EQ(::setxattr(p.c_str(), "user.content_md5",
			 md5.data(), md5.size(), 0), 0);
  }

  /* an empty NooBaa directory object:  no sentinel, the fact is on
   * the directory */
  void nb_folder(const std::string& name, const char* ctype) {
    const sf::path p{bucket_path() / name};
    sf::create_directories(p);
    set_u64_xattr(p, "user.noobaa.dir_content", 0);
    const std::string ct{ctype};
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.content_type",
			 ct.data(), ct.size(), 0), 0);
  }

  /* a NooBaa version entry, under .versions/.  NooBaa's layout is
   * nsfs's -- nsfs borrowed it -- so only the attribute names differ. */
  void nb_version(const std::string& leaf, const std::string& vid,
		     const char* ctype) {
    const sf::path vdir{bucket_path() / ".versions"};
    sf::create_directories(vdir);
    const sf::path p{vdir / (leaf + "_" + vid)};
    write_file(p, "their old bytes");
    const std::string ct{ctype};
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.content_type",
			 ct.data(), ct.size(), 0), 0);
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.version_id",
			 vid.data(), vid.size(), 0), 0);
  }

  /* a NooBaa delete marker:  no bytes, and no ACL of any kind */
  void nb_delete_marker(const std::string& leaf, const std::string& vid) {
    const sf::path vdir{bucket_path() / ".versions"};
    sf::create_directories(vdir);
    const sf::path p{vdir / (leaf + "_" + vid)};
    write_file(p, "");
    const std::string t{"true"};
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.delete_marker",
			 t.data(), t.size(), 0), 0);
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.version_id",
			 vid.data(), vid.size(), 0), 0);
  }

  sf::path version_path(const std::string& leaf, const std::string& vid) {
    return bucket_path() / ".versions" / (leaf + "_" + vid);
  }

  /* RGW stores several string attributes as counted strings, so a
   * value read back through the nsfs strategy carries the terminator
   * and one read through NooBaa's does not -- NooBaa's is not a counted
   * format.  The conversion is not supposed to change the value, and
   * the byte is how RGW represents it rather than part of it. */
  static std::string unterminated(const bufferlist& bl) {
    std::string v = bl.to_str();
    while (!v.empty() && (v.back() == '\0')) {
      v.pop_back();
    }
    return v;
  }

  static std::set<std::string> xattr_names(const sf::path& p) {
    char buf[8192];
    std::set<std::string> names;
    ssize_t len = ::listxattr(p.c_str(), buf, sizeof(buf));
    if (len <= 0) {
      return names;
    }
    for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
      names.insert(q);
    }
    return names;
  }

  int upgrade(nsfs::convert_cb_t* cb, nsfs::ConvertProgress* progress) {
    auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
    return b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_STRONG,
			  cb, progress);
  }
};

/* A bucket's own state is held as RGW attributes whatever its format
 * (DECISIONS.md 2026-10-13), so a base bucket writes it there and the
 * upgrade has nothing to rewrite.
 *
 * Before, a base bucket named it through NooBaa's strategy, which put
 * RGW_ATTR_IAM_POLICY under `user.nsfs:user.rgw.iam-policy`, and the
 * upgrade carried a step to rename it. */
TEST_F(NSFSUpgradeTest, ABaseBucketWritesItsStateAsRGWAttributes)
{
  static constexpr const char* base_name = "user.nsfs:user.rgw.iam-policy";
  static constexpr const char* nsfs_name = "user.nsfs:rgw.iam-policy";
  const std::string policy{"{\"Version\":\"2012-10-17\"}"};

  bufferlist bl;
  bl.append(policy);
  rgw::sal::Attrs add;
  add[RGW_ATTR_IAM_POLICY] = bl;

  auto before = xattr_names(bucket_path());
  EXPECT_TRUE(before.contains(nsfs_name))
      << "a base bucket did not write its state as RGW attributes";
  EXPECT_FALSE(before.contains(base_name));

  /* and reads it back, as the bucket it is */
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  auto p = bucket->get_attrs().find(RGW_ATTR_IAM_POLICY);
  ASSERT_NE(p, bucket->get_attrs().end()) << "the policy was not read back";
  EXPECT_EQ(p->second.to_str(), policy);

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  ASSERT_TRUE(prog.complete);

  auto after = xattr_names(bucket_path());
  EXPECT_TRUE(after.contains(nsfs_name));
  EXPECT_FALSE(after.contains(base_name));
  EXPECT_EQ(str_xattr(bucket_path(), nsfs_name), policy);

  /* the profile markers are not bucket state and the pass leaves them
   * alone;  the mask is still there and still says strong */
  EXPECT_TRUE(after.contains(nsfs::EXTENSIONS_XATTR));
  EXPECT_FALSE(after.contains(nsfs::CONVERTING_XATTR));
}

/* .versions/ is a reserved name, so the tree walk skips it.  It skipped
 * the version entries with it until convert_versions() existed, which
 * left every one of them with NooBaa attributes while the bucket read
 * RGW's. */
TEST_F(NSFSUpgradeTest, ConvertsVersionEntries)
{
  const std::string v1{"mtime-dlyy3p93o4mt-ino-430o2"};
  const std::string v2{"mtime-dlyy3p96naq6-ino-430o3"};
  nb_object("photo.jpg", "image/jpeg");
  nb_version("photo.jpg", v1, "image/jpeg");
  /* a leaf with an underscore in it, which is what makes the entry
   * name ambiguous and the split non-trivial */
  nb_version("my_photo.jpg", v2, "image/png");

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.objects, 1u);
  EXPECT_EQ(prog.versions, 2u)
      << "the version entries were not visited";
  EXPECT_TRUE(prog.complete);

  for (const auto& [leaf, vid] :
       std::vector<std::pair<std::string, std::string>>{
	 {"photo.jpg", v1}, {"my_photo.jpg", v2}}) {
    auto names = xattr_names(version_path(leaf, vid));
    EXPECT_TRUE(names.contains("user.nsfs:rgw.content_type"))
	<< leaf << " version kept no content type we can read";
    EXPECT_FALSE(names.contains("user.noobaa.content_type"))
	<< leaf << " version kept their attribute;  a later write would "
	<< "add ours beside it";
  }
}

  nb_object("photo.jpg", "image/jpeg");

/* The severe one.  NooBaa's delete marker is user.noobaa.delete_marker and
 * nsfs's is user.nsfs:delete_marker;  every read resolves the name
 * through the bucket's strategy, and there is no chaining reader, so a
 * marker left as a NooBaa attribute stops being a marker once the mask is
 * written.  The object it deleted comes back -- 200 with no bytes where
 * S3 owes 404 -- and nothing reports it. */
TEST_F(NSFSUpgradeTest, ADeleteMarkerStillReadsAsOneAfterConversion)
{
  const std::string vid{"mtime-dlyy3p9ezde0-ino-hu35vm"};
  /* no leaf object:  a DELETE of a key that does not exist still
   * writes a marker, and that is the case with no ACL to inherit */
  nb_delete_marker("gone.bin", vid);

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.versions, 1u);

  auto names = xattr_names(version_path("gone.bin", vid));
  EXPECT_TRUE(names.contains("user.nsfs:delete_marker"))
      << "the marker is no longer a marker;  the deleted object is back";
  EXPECT_FALSE(names.contains("user.noobaa.delete_marker"));
}

/* Conversion records the bucket's owner on a version, as on any object
 * that records none (DECISIONS.md 2026-10-11).  A delete marker needs
 * this too -- RGW emits an <Owner> for one. */
TEST_F(NSFSUpgradeTest, ConversionCapturesTheOwnerOfAVersion)
{
  const std::string vid{"mtime-dlyy3p99mgtk-ino-430o4"};
  const std::string dmid{"mtime-dlyy3p9muo1r-ino-hu35vn"};
  nb_version("doc.txt", vid, "text/plain");
  nb_delete_marker("doc.txt", dmid);

  for (const auto& p : {version_path("doc.txt", vid),
			version_path("doc.txt", dmid)}) {
    ASSERT_FALSE(xattr_names(p).contains("user.nsfs:rgw.acl"))
	<< "nothing to prove:  " << p << " already had an ACL";
  }

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.versions, 2u);

  for (const auto& p : {version_path("doc.txt", vid),
			version_path("doc.txt", dmid)}) {
    EXPECT_TRUE(xattr_names(p).contains("user.nsfs:rgw.acl"))
	<< p << " has no owner after conversion, so it lists as unknown";
    char buf[4096];
    ssize_t len = ::getxattr(p.c_str(), "user.nsfs:rgw.acl", buf, sizeof(buf));
    ASSERT_GT(len, 0);
    bufferlist bl;
    bl.append(buf, len);
    RGWAccessControlPolicy policy;
    auto it = bl.cbegin();
    ASSERT_NO_THROW(policy.decode(it));
    EXPECT_EQ(policy.get_owner().id, bucket->get_info().owner)
	<< p << " records an owner that is not the bucket's";
  }
}

/* The lock file is nsfs's, not a version, and must not be counted or
 * rewritten as one. */
TEST_F(NSFSUpgradeTest, TheVersionsLockIsNotAVersion)
{
  const std::string vid{"mtime-dlyy3p9clmwx-ino-430o5"};
  nb_version("only.bin", vid, "text/plain");
  write_file(bucket_path() / ".versions" / ".lock", "");

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.versions, 1u);
}

/* No marker is bucket metadata, so none reaches the S3 attribute set
 * -- including while a conversion is in flight, which is the only time
 * the converting marker exists -- and a write of the bucket's
 * attributes, which replaces them all, leaves every marker in place. */
TEST_F(NSFSUpgradeTest, TheMarkersAreNotBucketAttributes)
{
  nb_object("flat.bin", "text/plain");
  ASSERT_EQ(static_cast<rgw::sal::NSFSBucket*>(bucket.get())
	      ->set_owns_storage(env->dpp, false), 0);

  nsfs::convert_cb_t stop =
    [](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      return (ev.kind == nsfs::ConvertEvent::Kind::object) ? -EIO : 0;
    };
  nsfs::ConvertProgress prog;
  ASSERT_NE(upgrade(&stop, &prog), 0);

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_TRUE(b->is_converting())
      << "nothing to check:  the conversion did not stop";

  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
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

/* Everything in the tree ends up in nsfs names, and the marker is gone. */
TEST_F(NSFSUpgradeTest, ConvertsTheTree)
{
  nb_object("flat.bin", "text/plain");
  nb_object("deep/nested.bin", "application/json");
  nb_folder("photos", "application/directory");

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.objects, 2u);
  EXPECT_EQ(prog.directory_objects, 1u);
  EXPECT_TRUE(prog.complete);

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  EXPECT_STREQ(b->profile_name(), "strong");
  EXPECT_FALSE(b->is_converting());

  for (const char* k : {"flat.bin", "deep/nested.bin"}) {
    auto names = xattr_names(bucket_path() / k);
    EXPECT_TRUE(names.contains("user.nsfs:rgw.content_type")) << k;
    EXPECT_TRUE(names.contains("user.nsfs:rgw.etag")) << k;
    EXPECT_FALSE(names.contains("user.noobaa.content_type")) << k;
    EXPECT_FALSE(names.contains("user.content_md5")) << k;
  }

  /* the folder's fact moved:  NooBaa's attribute on the directory, nsfs's
   * in the sentinel beside it */
  EXPECT_TRUE(sf::is_regular_file(bucket_path() / "photos" / ".folder"));
  EXPECT_FALSE(xattr_names(bucket_path() / "photos")
		   .contains("user.noobaa.dir_content"));
  EXPECT_TRUE(xattr_names(bucket_path() / "photos" / ".folder")
		  .contains("user.nsfs:rgw.content_type"));

  /* and the values survived the move */
  auto obj = bucket->get_object(rgw_obj_key("flat.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);
  auto& attrs = obj->get_attrs();
  ASSERT_NE(attrs.find(RGW_ATTR_CONTENT_TYPE), attrs.end());
  EXPECT_EQ(unterminated(attrs[RGW_ATTR_CONTENT_TYPE]), "text/plain");
  EXPECT_EQ(unterminated(attrs[RGW_ATTR_ETAG]),
	    "0123456789abcdef0123456789abcdef");
}

/* Every phase is reported, in order, for every item. */
TEST_F(NSFSUpgradeTest, ReportsEachPhase)
{
  nb_object("a.bin", "text/plain");
  nb_object("b.bin", "text/plain");

  std::vector<std::string> seen;
  nsfs::convert_cb_t cb =
    [&seen](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      seen.push_back(std::string(nsfs::to_string(ev.phase)) + ":" + ev.key);
      return 0;
    };

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(&cb, &prog), 0);
  /* No bucket phases:  the bucket's own state is held as RGW attributes
   * in every format, so there is nothing of it to convert */
  ASSERT_EQ(seen.size(), 6u) << "three phases for each of two objects";

  /* readdir order is the filesystem's, so what is asserted is that
   * each object's three phases arrive together and in order */
  for (size_t i = 0; i < seen.size(); i += 3) {
    const std::string key = seen[i].substr(seen[i].find(':') + 1);
    EXPECT_EQ(seen[i], "before:" + key);
    EXPECT_EQ(seen[i + 1], "written:" + key);
    EXPECT_EQ(seen[i + 2], "pruned:" + key);
  }
}

/* A reader held at `written` sees the new value, not the old.
 *
 * That instant is the only state the conversion creates which did not
 * exist before -- the RGW and NooBaa attributes on the file at once --
 * and it is where "nsfs first" in the read chain stops being
 * incidental.  The callback is what makes it reachable:  without a hold
 * there is no way to arrive inside one item's conversion.
 */
TEST_F(NSFSUpgradeTest, AReaderHeldAtWrittenSeesTheNewValue)
{
  nb_object("held.bin", "text/plain");

  std::string seen_ct, seen_etag;
  nsfs::convert_cb_t cb =
    [&](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if ((ev.phase != nsfs::ConvertEvent::Phase::written) ||
	  (ev.key != "held.bin")) {
	return 0;
      }
      /* both attributes are on the file right now */
      auto names = xattr_names(bucket_path() / "held.bin");
      EXPECT_TRUE(names.contains("user.nsfs:rgw.content_type"));
      EXPECT_TRUE(names.contains("user.noobaa.content_type"));

      auto rd = bucket->get_object(rgw_obj_key("held.bin"));
      EXPECT_EQ(rd->load_obj_state(env->dpp, null_yield), 0);
      EXPECT_EQ(rd->get_obj_attrs(null_yield, env->dpp), 0);
      auto& a = rd->get_attrs();
      auto ct = a.find(RGW_ATTR_CONTENT_TYPE);
      if (ct != a.end()) {
	seen_ct = ct->second.to_str();
      }
      auto et = a.find(RGW_ATTR_ETAG);
      if (et != a.end()) {
	seen_etag = et->second.to_str();
      }
      return 0;
    };

  ASSERT_EQ(upgrade(&cb, nullptr), 0);
  EXPECT_EQ(seen_ct, "text/plain");
  EXPECT_EQ(seen_etag, "0123456789abcdef0123456789abcdef");
}

/* Stopping leaves the marker, and a second run finishes the job.
 *
 * The control the resumability claim needs:  the abort has to leave a
 * tree that is genuinely half-converted, and the restart has to be
 * able to tell which half is which -- which is what the marker and
 * the chain are for.
 */
TEST_F(NSFSUpgradeTest, StopsAndResumes)
{
  for (int i = 0; i < 4; ++i) {
    nb_object("obj-" + std::to_string(i) + ".bin", "text/plain");
  }

  int seen = 0;
  nsfs::convert_cb_t stop =
    [&seen](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if ((ev.kind != nsfs::ConvertEvent::Kind::object) ||
	  (ev.phase != nsfs::ConvertEvent::Phase::pruned)) {
	return 0;
      }
      return (++seen == 2) ? -EINTR : 0;
    };

  nsfs::ConvertProgress first;
  EXPECT_EQ(upgrade(&stop, &first), -EINTR);
  EXPECT_FALSE(first.complete);

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  EXPECT_STREQ(b->profile_name(), "strong")
      << "the mask went down before the walk, so a stopped run is still "
	 "marked";
  EXPECT_TRUE(b->is_converting()) << "the marker was cleared by a run that "
				     "did not finish";

  /* half the tree is nsfs's and half is NooBaa's, and both read */
  int nsfs_count = 0, nb_count = 0;
  for (int i = 0; i < 4; ++i) {
    auto names = xattr_names(bucket_path() /
			     ("obj-" + std::to_string(i) + ".bin"));
    if (names.contains("user.noobaa.content_type")) {
      ++nb_count;
    } else {
      ++nsfs_count;
    }
    auto obj = bucket->get_object(
	rgw_obj_key("obj-" + std::to_string(i) + ".bin"));
    ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
    ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);
    EXPECT_EQ(unterminated(obj->get_attrs()[RGW_ATTR_ETAG]),
	      "0123456789abcdef0123456789abcdef")
	<< "obj-" << i << " unreadable mid-conversion";
  }
  EXPECT_EQ(nsfs_count, 2);
  EXPECT_EQ(nb_count, 2);

  /* and the rest of it converts */
  nsfs::ConvertProgress second;
  ASSERT_EQ(b->convert_tree(env->dpp, null_yield, nullptr, &second), 0);
  EXPECT_TRUE(second.complete);
  EXPECT_FALSE(b->is_converting());
  for (int i = 0; i < 4; ++i) {
    EXPECT_FALSE(xattr_names(bucket_path() /
			     ("obj-" + std::to_string(i) + ".bin"))
		     .contains("user.noobaa.content_type"));
  }
}

/* Converting a tree that is already nsfs's changes nothing.
 *
 * Resumption reruns items it already did, so the pass has to be
 * idempotent or a restart corrupts what the first run finished. */
TEST_F(NSFSUpgradeTest, ConvertingTwiceIsTheSameAsOnce)
{
  nb_object("once.bin", "text/plain");
  ASSERT_EQ(upgrade(nullptr, nullptr), 0);
  const auto after_one = xattr_names(bucket_path() / "once.bin");

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->mark_converting(env->dpp), 0);
  nsfs::ConvertProgress prog;
  ASSERT_EQ(b->convert_tree(env->dpp, null_yield, nullptr, &prog), 0);
  EXPECT_TRUE(prog.complete);

  EXPECT_EQ(xattr_names(bucket_path() / "once.bin"), after_one);
  auto obj = bucket->get_object(rgw_obj_key("once.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);
  EXPECT_EQ(unterminated(obj->get_attrs()[RGW_ATTR_CONTENT_TYPE]),
	    "text/plain");
}

/* An upload in flight crosses the upgrade.
 *
 * It cannot be drained:  UploadPart terminates, the upload it belongs
 * to does not, so the tree arrives at the conversion with staging in
 * NooBaa's layout.  The alternative is aborting it, which throws away
 * work the client already did.
 */
TEST_F(NSFSUpgradeTest, ReStagesAnUploadInFlight)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);
  nb_object("beside.bin", "text/plain");

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.uploads, 1u);
  EXPECT_EQ(prog.parts, tree.parts);
  EXPECT_TRUE(prog.complete);

  /* NooBaa's staging is gone */
  EXPECT_FALSE(sf::exists(tree.staging)) << tree.staging;

  /* and the nsfs one is there, named as nsfs names one */
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  const std::string meta = tree.key + "." + tree.upload_id;
  const sf::path staging{bucket_path() / b->mpu_strategy()->staging_dir_name(meta)};
  ASSERT_TRUE(sf::is_directory(staging)) << staging;

  /* the upload lists, through the nsfs layout */
  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  std::string marker;
  bool truncated = false;
  ASSERT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 100, uploads,
				    nullptr, &truncated, null_yield,
				    rgw::sal::no_authenticated_user), 0);
  ASSERT_EQ(uploads.size(), 1u);
  EXPECT_EQ(uploads[0]->get_key(), tree.key);
  EXPECT_EQ(uploads[0]->get_upload_id(), tree.upload_id);

  /* its parts report the sizes they were staged with */
  auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
  int next = 0;
  ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
			       &truncated, null_yield), 0);
  const auto& parts = upload->get_parts();
  ASSERT_EQ(parts.size(), tree.parts);
  for (uint32_t k = 1; k <= tree.parts; ++k) {
    ASSERT_NE(parts.find(k), parts.end()) << k;
    EXPECT_EQ(parts.at(k)->get_size(), tree.part_size) << k;
  }

  /* and it completes, into the object the client asked for */
  std::map<int, std::string> etags;
  for (auto& [num, part] : upload->get_parts()) {
    etags[num] = part->get_etag();
  }
  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, tree.key,
			etags), 0);
  EXPECT_EQ(read_all(bucket_path() / tree.key), tree.payload);
}

/* NooBaa's temp directory goes with the last upload in it.  Each upload's
 * staging is removed as it is re-staged, and the directory that held
 * them is NooBaa's alone:  left, it is no longer hidden once the bucket
 * stops converting, since only NooBaa's path strategy reserves the
 * name. */
TEST_F(NSFSUpgradeTest, ConversionRemovesTheirEmptiedTempDirectory)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);
  nb_object("beside.bin", "text/plain");
  const sf::path tmpdir = tree.staging.parent_path().parent_path();
  ASSERT_TRUE(sf::is_directory(tmpdir)) << tmpdir;
  /* an object write of theirs cut short before its move into place
   * (`namespace_fs.js:1301`):  never an object, and it goes too */
  sf::create_directories(tmpdir / "uploads");
  write_file(tmpdir / "uploads" / fake_uuid("partial"), "half");

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  ASSERT_TRUE(prog.complete);

  for (const char* delim : {"", "/"}) {
    rgw::sal::Bucket::ListParams params;
    params.delim = delim;
    rgw::sal::Bucket::ListResults results;
    ASSERT_EQ(bucket->list(env->dpp, params, 1000, results, null_yield), 0);
    for (const auto& e : results.objs) {
      EXPECT_FALSE(e.key.name.starts_with(".noobaa-nsfs"))
	<< "delim '" << delim << "' lists " << e.key.name;
    }
    for (const auto& [cp, _] : results.common_prefixes) {
      EXPECT_FALSE(cp.starts_with(".noobaa-nsfs"))
	<< "delim '" << delim << "' lists the prefix " << cp;
    }
  }
  EXPECT_FALSE(sf::exists(tmpdir)) << tmpdir;
}

/* The subtree marker's value, or nullopt when there is none. */
static std::optional<std::string> subtree_marker(const sf::path& p)
{
  char buf[64];
  const ssize_t n = ::getxattr(p.c_str(), nsfs::SUBTREE_CONVERTED_XATTR,
			       buf, sizeof(buf));
  if (n < 0) {
    return std::nullopt;
  }
  return std::string(buf, n);
}

/* What every subtree marker of an upgrade to strong names. */
static const std::string STRONG_SET = std::to_string(nsfs::EXTENSIONS_STRONG);

/* Stops each walk at its first upload, which comes after the tree:  so
 * every subdirectory is converted and marked, and the bucket is not. */
static nsfs::convert_cb_t stop_at_upload()
{
  return [](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
    return (ev.kind == nsfs::ConvertEvent::Kind::upload) ? -EINTR : 0;
  };
}

/* A finished subdirectory is marked with the target set, after what is
 * under it;  the root is not, its completion being the bucket's.  A
 * resumed walk passes each marked one with one read, reaching nothing
 * under it, and counts it.  The markers stay after completion. */
TEST_F(NSFSUpgradeTest, AResumedWalkPassesAConvertedSubtree)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);
  nb_object("top.bin", "text/plain");
  nb_object("a/one.bin", "text/plain");
  nb_object("a/deep/two.bin", "text/plain");
  nb_object("b/three.bin", "text/plain");
  nb_folder("c", "application/x-directory");

  auto stop = stop_at_upload();
  nsfs::ConvertProgress first;
  ASSERT_EQ(upgrade(&stop, &first), -EINTR);
  for (const char* d : {"a", "a/deep", "b", "c"}) {
    EXPECT_EQ(subtree_marker(bucket_path() / d), STRONG_SET) << d;
  }
  EXPECT_EQ(subtree_marker(bucket_path()), std::nullopt);

  std::vector<std::string> seen;
  nsfs::convert_cb_t record =
    [&seen](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if (ev.phase == nsfs::ConvertEvent::Phase::before) {
	seen.push_back(ev.key);
      }
      return 0;
    };
  nsfs::ConvertProgress second;
  ASSERT_EQ(upgrade(&record, &second), 0);
  EXPECT_TRUE(second.complete);
  /* a, b and c;  a/deep is under a, and not reached */
  EXPECT_EQ(second.directories_skipped, 3u);
  for (const auto& k : seen) {
    EXPECT_FALSE(k.starts_with("a/") || k.starts_with("b/") ||
		 k.starts_with("c/")) << k << " is under a converted subtree";
  }
  EXPECT_NE(std::find(seen.begin(), seen.end(), "top.bin"), seen.end());

  for (const char* d : {"a", "a/deep", "b", "c"}) {
    EXPECT_EQ(subtree_marker(bucket_path() / d), STRONG_SET)
	<< d << ":  the marker stays after completion";
  }
}

/* A subtree marker naming another extension set says nothing of this
 * conversion, and the walk goes through the subtree. */
TEST_F(NSFSUpgradeTest, AMarkerNamingAnotherSetIsNotPassed)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);
  nb_object("a/one.bin", "text/plain");
  nb_object("b/two.bin", "text/plain");

  auto stop = stop_at_upload();
  ASSERT_EQ(upgrade(&stop, nullptr), -EINTR);
  const std::string other = std::to_string(nsfs::EXTENSIONS_SHARED);
  ASSERT_EQ(::setxattr((bucket_path() / "a").c_str(),
		       nsfs::SUBTREE_CONVERTED_XATTR, other.data(),
		       other.size(), 0), 0);

  nsfs::ConvertProgress second;
  ASSERT_EQ(upgrade(nullptr, &second), 0);
  EXPECT_EQ(second.directories_skipped, 1u) << "b only";
  EXPECT_EQ(second.objects, 1u) << "a/one.bin, walked again";
  EXPECT_EQ(subtree_marker(bucket_path() / "a"), STRONG_SET);
}

/* A walk that reaches an object already converted writes no RGW
 * attribute to it again, which would move its ctime (DECISIONS.md
 * 2026-11-02).  The object had one-to-many NooBaa attributes, whose
 * names a key's removal used to list whether or not they were there. */
TEST_F(NSFSUpgradeTest, AWalkWritesNothingToAConvertedObject)
{
  nb_object("top.bin", "text/plain");
  nb_object("d/inner.bin", "text/plain");
  {
    const sf::path p{bucket_path() / "top.bin"};
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.tag.a", "1", 1, 0), 0);
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.legal_hold", "ON", 2, 0), 0);
  }
  ASSERT_EQ(upgrade(nullptr, nullptr), 0);

  auto ctime = [](const sf::path& p) {
    struct statx stx{};
    EXPECT_EQ(::statx(AT_FDCWD, p.c_str(), 0, STATX_CTIME, &stx), 0) << p;
    return std::pair{stx.stx_ctime.tv_sec, stx.stx_ctime.tv_nsec};
  };
  const auto top = ctime(bucket_path() / "top.bin");
  const auto inner = ctime(bucket_path() / "d/inner.bin");

  /* the bucket converting again, and d's marker gone, so the walk
   * reaches both objects */
  ASSERT_EQ(::setxattr(bucket_path().c_str(), nsfs::CONVERTING_XATTR,
		       nsfs::CONVERTING_FROM_NOOBAA,
		       std::strlen(nsfs::CONVERTING_FROM_NOOBAA), 0), 0);
  ASSERT_EQ(::removexattr((bucket_path() / "d").c_str(),
			  nsfs::SUBTREE_CONVERTED_XATTR), 0);
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", testname},
				&bucket, null_yield), 0);
  ASSERT_TRUE(static_cast<rgw::sal::NSFSBucket*>(bucket.get())
		->is_converting());

  nsfs::ConvertProgress again;
  ASSERT_EQ(upgrade(nullptr, &again), 0);
  EXPECT_EQ(again.objects, 2u) << "both reached";
  EXPECT_EQ(ctime(bucket_path() / "top.bin"), top);
  EXPECT_EQ(ctime(bucket_path() / "d/inner.bin"), inner);
}

/* What a directory object is served with:  its ETag and Last-Modified. */
static std::pair<std::string, ceph::real_time>
served_as(rgw::sal::Bucket* bucket, const DoutPrefixProvider* dpp,
	  const std::string& key)
{
  auto obj = bucket->get_object(rgw_obj_key(key));
  EXPECT_EQ(obj->load_obj_state(dpp, null_yield), 0) << key;
  EXPECT_EQ(obj->get_obj_attrs(null_yield, dpp), 0) << key;
  auto& attrs = obj->get_attrs();
  auto i = attrs.find(RGW_ATTR_ETAG);
  return {(i == attrs.end()) ? std::string() : rgw_bl_str(i->second),
	  obj->get_mtime()};
}

/* An empty directory object keeps the ETag and Last-Modified it was
 * served with, through a walk stopped after its sentinel is linked and
 * the walk that resumes it.  Both come from the directory's stat, which
 * making the sentinel changes, so they are on the sentinel before it
 * appears (DECISIONS.md 2026-10-13). */
TEST_F(NSFSUpgradeTest, AnEmptyDirectoryObjectKeepsItsETagAcrossAStop)
{
  nb_folder("photos", "application/x-directory");
  const auto before = served_as(bucket.get(), env->dpp, "photos/");
  ASSERT_FALSE(before.first.empty());

  nsfs::convert_cb_t stop =
    [](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      return ((ev.kind == nsfs::ConvertEvent::Kind::directory_object) &&
	      (ev.phase == nsfs::ConvertEvent::Phase::written)) ? -EINTR : 0;
    };
  ASSERT_EQ(upgrade(&stop, nullptr), -EINTR);
  ASSERT_TRUE(sf::exists(bucket_path() / "photos" / ".folder"));
  ASSERT_EQ(upgrade(nullptr, nullptr), 0);

  const auto after = served_as(bucket.get(), env->dpp, "photos/");
  EXPECT_EQ(after.first, before.first);
  EXPECT_EQ(after.second, before.second);
}

/* A walk on a filesystem without O_TMPFILE names its sentinel and gives
 * it the directory's mtime straight after.  Stopped there, the sentinel
 * exists and holds nothing, and dir_content still says the object is
 * empty:  the resumed walk records the ETag the object was served with,
 * from the directory's inode and the sentinel's mtime.  It used to take
 * the sentinel for NooBaa's and record none. */
TEST_F(NSFSUpgradeTest, AResumedWalkRecordsTheETagOfAnEmptyDirectoryObject)
{
  nb_folder("photos", "application/x-directory");
  const auto before = served_as(bucket.get(), env->dpp, "photos/");
  ASSERT_FALSE(before.first.empty());

  const sf::path d{bucket_path() / "photos"};
  struct statx dstx{};
  ASSERT_EQ(::statx(AT_FDCWD, d.c_str(), 0, STATX_MTIME, &dstx), 0);
  const sf::path sentinel{d / ".folder"};
  write_file(sentinel, "");
  const struct timespec times[2] = {
    { 0, UTIME_OMIT },
    { dstx.stx_mtime.tv_sec, dstx.stx_mtime.tv_nsec },
  };
  ASSERT_EQ(::utimensat(AT_FDCWD, sentinel.c_str(), times, 0), 0);

  ASSERT_EQ(upgrade(nullptr, nullptr), 0);
  const auto after = served_as(bucket.get(), env->dpp, "photos/");
  EXPECT_EQ(after.first, before.first);
  EXPECT_EQ(after.second, before.second);
}

/* Nothing changes a converting bucket's profile (DECISIONS.md
 * 2026-11-05):  between two nsfs profiles only the mask would be
 * written, reporting the bucket complete over a mixed tree.  Asking for
 * the profile it is converting to still resumes it. */
TEST_F(NSFSUpgradeTest, AConvertingBucketKeepsItsProfile)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);
  nb_object("a/one.bin", "text/plain");

  auto stop = stop_at_upload();
  ASSERT_EQ(upgrade(&stop, nullptr), -EINTR);
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_TRUE(b->is_converting());

  nsfs::ConvertProgress prog;
  EXPECT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED,
			   nullptr, &prog), -ERR_INVALID_BUCKET_STATE);
  EXPECT_FALSE(prog.complete);
  EXPECT_STREQ(b->profile_name(), "strong");
  EXPECT_TRUE(b->is_converting());

  ASSERT_EQ(upgrade(nullptr, &prog), 0) << "the resume is still asked for";
  EXPECT_TRUE(prog.complete);
}

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

/* Every row a listing gives, as one comparable string each:  what a
 * client sees of it. */
static std::vector<std::string> listing_rows(rgw::sal::Bucket* bucket,
					     const DoutPrefixProvider* dpp,
					     bool versions)
{
  rgw::sal::Bucket::ListParams params;
  params.list_versions = versions;
  rgw::sal::Bucket::ListResults results;
  EXPECT_EQ(bucket->list(dpp, params, 1000, results, null_yield), 0);
  std::vector<std::string> rows;
  for (auto& e : results.objs) {
    rows.push_back(fmt::format(
	"{} [{}] size={} etag={} mtime={} owner={} class={} flags={} exists={}",
	e.key.name, e.key.instance, e.meta.size, e.meta.etag,
	e.meta.mtime.time_since_epoch().count(), e.meta.owner,
	e.meta.storage_class, e.flags, e.exists));
  }
  return rows;
}


/* An upload in progress does not keep its bucket, and deleting the
 * bucket aborts it, as RADOS does (check_bucket_empty(),
 * RadosBucket::remove()).  The control is the same bucket with an
 * object:  refused, and the refusal leaves the upload in place. */
TEST_F(NSFSBucketTest, DeletingABucketAbortsItsUploads)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  std::unique_ptr<rgw::sal::MultipartUpload> upload =
    bucket->get_multipart_upload(testname + "-mp", rgw::sal::no_authenticated_user, "c0ffee");
  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs),
	    0);
  auto count_uploads = [&]() -> size_t {
    std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
    std::string marker;
    bool truncated = false;
    EXPECT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 10, uploads,
				      nullptr, &truncated, null_yield,
				      rgw::sal::no_authenticated_user), 0);
    return uploads.size();
  };
  ASSERT_EQ(count_uploads(), 1u);

  write_file(bucket_path() / "o00", "o00");
  EXPECT_EQ(b->check_empty(env->dpp, null_yield), -ENOTEMPTY)
      << "the control:  an object did not count as content";
  EXPECT_EQ(bucket->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user), -ENOTEMPTY);
  EXPECT_EQ(count_uploads(), 1u) << "a refused delete aborted the upload";
  sf::remove(bucket_path() / "o00");

  EXPECT_EQ(b->check_empty(env->dpp, null_yield), 0)
      << "an upload in progress kept its bucket";
  EXPECT_EQ(bucket->remove(env->dpp, false, null_yield, rgw::sal::no_authenticated_user), 0);
  EXPECT_FALSE(sf::exists(bucket_path()));
}


/* The listing cache needs no invalidation when a bucket is converted
 * (DECISIONS.md 2026-10-13).
 *
 * A row holds only facts conversion is required to keep, because a
 * bucket being converted must look to a client like the bucket it
 * becomes.  So rows cached before the upgrade, and served after it, must
 * equal rows built afresh.  This lists a base bucket to fill the cache,
 * upgrades it, lists again through that cache, then empties the cache and
 * lists a third time.  If conversion ever changes a listed fact, the
 * warm and cold listings disagree here, and that is the knowledge that
 * an invalidation, or a fix to conversion, is needed. */
TEST_F(NSFSUpgradeTest, AListingCachedBeforeTheUpgradeEqualsOneBuiltAfterIt)
{
  bucket->get_info().flags |= BUCKET_VERSIONED;

  nb_object("plain.bin", "text/plain");
  lay_noobaa_directory_object(bucket_path() / "e", "", "text/x-e", "e");
  lay_noobaa_directory_object(bucket_path() / "c", "dir bytes", "text/x-c", "c");

  /* a versioned object:  its current version and one earlier one, each
   * with the id NooBaa recorded */
  nb_object("v.bin", "text/plain");
  const std::string cur_vid = "mtime-abc123-ino-10";
  ASSERT_EQ(::setxattr((bucket_path() / "v.bin").c_str(),
		       "user.noobaa.version_id", cur_vid.data(),
		       cur_vid.size(), 0), 0);
  const std::string old_vid = "mtime-abc000-ino-11";
  sf::create_directories(bucket_path() / ".versions");
  const sf::path old_ver = bucket_path() / ".versions" / ("v.bin_" + old_vid);
  write_file(old_ver, "older");
  ASSERT_EQ(::setxattr(old_ver.c_str(), "user.noobaa.version_id",
		       old_vid.data(), old_vid.size(), 0), 0);

  const auto before = listing_rows(bucket.get(), env->dpp, false);
  const auto before_v = listing_rows(bucket.get(), env->dpp, true);
  ASSERT_FALSE(before.empty());
  ASSERT_GT(before_v.size(), before.size())
      << "the version listing shows no earlier version";

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  ASSERT_TRUE(prog.complete);

  /* served from the cache filled before the upgrade */
  const auto warm = listing_rows(bucket.get(), env->dpp, false);
  const auto warm_v = listing_rows(bucket.get(), env->dpp, true);

  /* and built afresh */
  ASSERT_EQ(driver->get_bucket_cache()->invalidate_bucket(env->dpp,
							  bucket->get_name()), 0);
  const auto cold = listing_rows(bucket.get(), env->dpp, false);
  const auto cold_v = listing_rows(bucket.get(), env->dpp, true);

  EXPECT_EQ(warm, cold)
      << "the listing cache served rows the upgrade had changed";
  EXPECT_EQ(warm_v, cold_v)
      << "the listing cache served version rows the upgrade had changed";

  /* and, the requirement behind it, nothing a listing shows changed */
  EXPECT_EQ(cold, before) << "conversion changed a listed fact";
  EXPECT_EQ(cold_v, before_v) << "conversion changed a listed version fact";
}

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

/* Every part is reported, and a stop inside one leaves the rest. */
TEST_F(NSFSUpgradeTest, ReportsPartsAndStopsInsideAnUpload)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  std::vector<std::string> seen;
  nsfs::convert_cb_t record =
    [&seen](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if ((ev.kind == nsfs::ConvertEvent::Kind::upload) ||
	  (ev.kind == nsfs::ConvertEvent::Kind::part)) {
	seen.push_back(std::string(nsfs::to_string(ev.kind)) + ":" +
		       std::string(nsfs::to_string(ev.phase)) + ":" +
		       std::to_string(ev.part));
      }
      return 0;
    };
  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(&record, &prog), 0);

  /* the upload brackets its parts */
  ASSERT_FALSE(seen.empty());
  EXPECT_EQ(seen.front(), "upload:before:0");
  EXPECT_EQ(seen.back(), "upload:pruned:0");
  size_t part_phases = 0;
  for (auto& e : seen) {
    if (e.rfind("part:", 0) == 0) {
      ++part_phases;
    }
  }
  EXPECT_EQ(part_phases, tree.parts * 3);
}

/* Stopping between parts leaves NooBaa's staging in place.
 *
 * NooBaa's directory is removed last for exactly this reason:  a run that
 * dies partway has copied some parts and must leave the source where
 * the next run can find it. */
TEST_F(NSFSUpgradeTest, StoppingMidUploadKeepsNooBaaStaging)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  nsfs::convert_cb_t stop =
    [](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if ((ev.kind == nsfs::ConvertEvent::Kind::part) && (ev.part == 2) &&
	  (ev.phase == nsfs::ConvertEvent::Phase::written)) {
	return -EINTR;
      }
      return 0;
    };
  nsfs::ConvertProgress first;
  EXPECT_EQ(upgrade(&stop, &first), -EINTR);
  EXPECT_FALSE(first.complete);
  EXPECT_TRUE(sf::exists(tree.staging)) << "the source went before the copy "
					   "had finished";

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_TRUE(b->is_converting());

  nsfs::ConvertProgress second;
  ASSERT_EQ(b->convert_tree(env->dpp, null_yield, nullptr, &second), 0);
  EXPECT_TRUE(second.complete);
  EXPECT_FALSE(sf::exists(tree.staging));

  auto upload = bucket->get_multipart_upload(tree.key, rgw::sal::no_authenticated_user, tree.upload_id);
  int next = 0;
  bool truncated = false;
  ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
			       &truncated, null_yield), 0);
  EXPECT_EQ(upload->get_parts().size(), tree.parts);
  std::map<int, std::string> etags;
  for (auto& [num, part] : upload->get_parts()) {
    etags[num] = part->get_etag();
  }
  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, tree.key,
			etags), 0);
  EXPECT_EQ(read_all(bucket_path() / tree.key), tree.payload);
}

/* An upload is listed throughout the conversion, not only at its
 * ends.
 *
 * The re-stage moves one upload at a time, so mid-run some are in the nsfs
 * root and the rest are still in NooBaa's.  A listing that scanned one
 * root would report half the uploads in flight and call itself
 * complete.  Held at the moment NooBaa's staging still exists and nsfs's
 * already does, the same upload must be reported once -- not twice,
 * which is what a naive union of the two roots would give.
 */
TEST_F(NSFSUpgradeTest, AnUploadIsListedThroughoutTheConversion)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  std::vector<size_t> counts;
  nsfs::convert_cb_t watch =
    [&](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if (ev.kind != nsfs::ConvertEvent::Kind::upload) {
	return 0;
      }
      std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
      std::string marker;
      bool truncated = false;
      EXPECT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 100,
					uploads, nullptr, &truncated,
					null_yield,
					rgw::sal::no_authenticated_user), 0);
      counts.push_back(uploads.size());
      for (auto& u : uploads) {
	EXPECT_EQ(u->get_key(), tree.key);
	EXPECT_EQ(u->get_upload_id(), tree.upload_id);
      }
      return 0;
    };

  ASSERT_EQ(upgrade(&watch, nullptr), 0);
  ASSERT_EQ(counts.size(), 3u) << "three phases for the upload";
  /* before:  only NooBaa's exists.  written:  both do, and it is one
   * upload either way.  pruned:  only nsfs's. */
  EXPECT_EQ(counts[0], 1u) << "invisible before it was moved";
  EXPECT_EQ(counts[1], 1u) << "reported twice while both roots hold it";
  EXPECT_EQ(counts[2], 1u);
}

/* Mutations are held off while the tree is rewritten, and reads are
 * not.
 *
 * The conversion reads an object's attributes and writes them back, so
 * a write landing between those two is lost.  Rados has the same
 * problem during a reshard and handles it the same way:  hold the
 * mutation server-side for a bound, then let a retryable error reach
 * the client.
 */
TEST_F(NSFSUpgradeTest, HoldsOffMutationsAndNotReads)
{
  /* the rejection is what is under test, not how long it takes to
   * arrive;  the production wait would put five seconds in the suite */
  driver->set_gate_waits(std::chrono::milliseconds(20),
			 std::chrono::milliseconds(60000));
  nb_object("gated.bin", "text/plain");

  bool checked = false;
  nsfs::convert_cb_t probe =
    [&](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if ((ev.phase != nsfs::ConvertEvent::Phase::written) || checked) {
	return 0;
      }
      checked = true;

      EXPECT_TRUE(driver->gate_is_closed(testname));

      /* a read goes through */
      auto rd = bucket->get_object(rgw_obj_key("gated.bin"));
      EXPECT_EQ(rd->load_obj_state(env->dpp, null_yield), 0);
      EXPECT_EQ(rd->get_obj_attrs(null_yield, env->dpp), 0);

      auto wr = bucket->get_object(rgw_obj_key("gated.bin"));
      EXPECT_EQ(wr->load_obj_state(env->dpp, null_yield), 0);
      Attrs a;
      bufferlist v;
      v.append("text/html");
      a[RGW_ATTR_CONTENT_TYPE] = v;
      return 0;
    };

  ASSERT_EQ(upgrade(&probe, nullptr), 0);
  EXPECT_TRUE(checked) << "the probe never ran, so nothing was asserted";

  /* and the gate is open again afterwards */
  EXPECT_FALSE(driver->gate_is_closed(testname));
  auto wr = bucket->get_object(rgw_obj_key("gated.bin"));
  ASSERT_EQ(wr->load_obj_state(env->dpp, null_yield), 0);
  Attrs a;
  bufferlist v;
  v.append("text/html");
  a[RGW_ATTR_CONTENT_TYPE] = v;
  EXPECT_EQ(wr->set_obj_attrs(env->dpp, &a, nullptr, null_yield,
			      rgw::sal::FLAG_LOG_OP, rgw_user()), 0);
}

TEST_F(NSFSUpgradeTest, AStoppedConversionReopensTheGate)
{
  nb_object("a.bin", "text/plain");
  nb_object("b.bin", "text/plain");

  nsfs::convert_cb_t stop =
    [](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      return (ev.phase == nsfs::ConvertEvent::Phase::pruned) ? -EINTR : 0;
    };
  EXPECT_EQ(upgrade(&stop, nullptr), -EINTR);

  EXPECT_FALSE(driver->gate_is_closed(testname));
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  EXPECT_TRUE(b->is_converting());

  auto wr = bucket->get_object(rgw_obj_key("b.bin"));
  ASSERT_EQ(wr->load_obj_state(env->dpp, null_yield), 0);
  Attrs a;
  bufferlist v;
  v.append("text/html");
  a[RGW_ATTR_CONTENT_TYPE] = v;
  EXPECT_EQ(wr->set_obj_attrs(env->dpp, &a, nullptr, null_yield,
			      rgw::sal::FLAG_LOG_OP, rgw_user()), 0);
}

/* Two conversions of one bucket do not run together.
 *
 * Each would read what the other had half written.  The second is
 * rejected rather than queued:  it has nothing to wait for that the
 * first is not already doing. */
TEST_F(NSFSUpgradeTest, RejectsASecondConversion)
{
  nb_object("a.bin", "text/plain");

  int inner = 0;
  nsfs::convert_cb_t reenter =
    [&](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if (ev.phase != nsfs::ConvertEvent::Phase::written) {
	return 0;
      }
      auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
      nsfs::ConvertProgress p;
      inner = b->convert_tree(env->dpp, null_yield, nullptr, &p);
      return 0;
    };

  ASSERT_EQ(upgrade(&reenter, nullptr), 0);
  EXPECT_EQ(inner, -EBUSY);
}

/* An FSIO write handle is a mutation for as long as it is open.
 *
 * It is not a request:  it is opened once and written through many
 * times, and those writes are parts of one mutation rather than
 * separate ones.  So opening one to write takes the slot and
 * releasing it gives the slot back, and a bucket drains as its write
 * handles are released.  A conversion cannot start while one is open,
 * which is the point -- converting under an open writer is what the
 * gate exists to prevent.
 *
 * Strong rather than the upgrade fixture:  FSIO needs EXT_SHADOW,
 * which base does not have and an upgrade to strong only acquires at
 * its end.
 */
TEST_F(NSFSStrongBucketTest, AWriteHandleHoldsTheGateOpen)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  ASSERT_TRUE(b->has_extension(nsfs::EXT_SHADOW));
  driver->set_gate_waits(std::chrono::milliseconds(20),
			 std::chrono::milliseconds(20));

  auto obj = bucket->get_object(rgw_obj_key("handle.bin"));
  auto [rc, hdl] = obj->get_fsio_handle(
      env->dpp, rgw::sal::no_authenticated_user,
      rgw::sal::Object::FSIOObject::OPEN_FLAG_CREATE |
		rgw::sal::Object::FSIOObject::OPEN_FLAG_WRITE);
  ASSERT_EQ(rc, 0) << "could not open a write handle";
  ASSERT_NE(hdl.get(), nullptr);

  /* the gate will not close while it is open */
  EXPECT_EQ(driver->gate_close(env->dpp, testname), -ETIMEDOUT);
  /* and having failed, it left the gate open rather than half shut */
  EXPECT_FALSE(driver->gate_is_closed(testname));

  hdl->close(env->dpp, rgw::sal::Object::FSIOObject::CLOSE_FLAG_NONE);
  hdl.reset();

  /* released, so the bucket has drained */
  EXPECT_EQ(driver->gate_close(env->dpp, testname), 0);
  driver->gate_open(testname);
}

/* And a closed gate rejects a new write handle while serving a read
 * one. */
TEST_F(NSFSStrongBucketTest, AClosedGateRejectsAWriteHandle)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  driver->set_gate_waits(std::chrono::milliseconds(20),
			 std::chrono::milliseconds(20));

  /* an object to read;  a read handle binds to the published object */
  {
    auto obj = bucket->get_object(rgw_obj_key("there.bin"));
    auto [rc, hdl] = obj->get_fsio_handle(
	env->dpp, rgw::sal::no_authenticated_user,
	rgw::sal::Object::FSIOObject::OPEN_FLAG_CREATE |
		  rgw::sal::Object::FSIOObject::OPEN_FLAG_WRITE);
    ASSERT_EQ(rc, 0);
    ASSERT_EQ(hdl->commit(env->dpp,
			  rgw::sal::Object::FSIOObject::COMMIT_FLAG_NONE), 0);
    ASSERT_EQ(hdl->publish(env->dpp,
			   rgw::sal::Object::FSIOObject::PUBLISH_FLAG_NONE), 0);
    hdl->close(env->dpp, rgw::sal::Object::FSIOObject::CLOSE_FLAG_NONE);
  }

  ASSERT_EQ(driver->gate_close(env->dpp, testname), 0);

  {
    auto obj = bucket->get_object(rgw_obj_key("rejected.bin"));
    auto [rc, hdl] = obj->get_fsio_handle(
	env->dpp, rgw::sal::no_authenticated_user,
	rgw::sal::Object::FSIOObject::OPEN_FLAG_CREATE |
		  rgw::sal::Object::FSIOObject::OPEN_FLAG_WRITE);
    EXPECT_EQ(rc, -ERR_SERVICE_UNAVAILABLE);
    EXPECT_EQ(hdl.get(), nullptr) << "a rejected open left a handle behind";
  }

  {
    auto obj = bucket->get_object(rgw_obj_key("there.bin"));
    auto [rc, hdl] = obj->get_fsio_handle(
	env->dpp, rgw::sal::no_authenticated_user,
	rgw::sal::Object::FSIOObject::OPEN_FLAG_NONE);
    EXPECT_EQ(rc, 0) << "a read handle was rejected";
    if (hdl) {
      hdl->close(env->dpp, rgw::sal::Object::FSIOObject::CLOSE_FLAG_NONE);
    }
  }

  driver->gate_open(testname);
}

  nsfs::init_create_modes(env->dpp, env->cct.get());

  EXPECT_EQ(after.st_mode & 07777, nsfs::file_create_mode());
}

TEST_F(NSFSStrongBucketTest, AnFSIOPublishedObjectCarriesTheObjectMode)
{
  nsfs::init_create_modes(env->dpp, env->cct.get());

  EXPECT_EQ(file_mode(published), nsfs::file_create_mode())
      << "an object written through the NFS gateway does not carry the "
	 "mode of the same object written through S3";
}

TEST_F(NSFSStrongBucketTest, AnFSIOClonedObjectCarriesTheObjectMode)
{
  nsfs::init_create_modes(env->dpp, env->cct.get());


/* Re-issuing the same request resumes a stopped conversion.
 *
 * That is what a resume looks like from outside:  the operator asks
 * for the profile again.  Before this it returned success and did
 * nothing, because the bucket was already in the target profile --
 * so a conversion that stopped could never be finished through the
 * only interface there is, and the marker stayed for ever.
 */
TEST_F(NSFSUpgradeTest, ReissuingTheSameProfileResumes)
{
  for (int i = 0; i < 4; ++i) {
    nb_object("obj-" + std::to_string(i) + ".bin", "text/plain");
  }

  int seen = 0;
  nsfs::convert_cb_t stop =
    [&seen](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if (ev.phase != nsfs::ConvertEvent::Phase::pruned) {
	return 0;
      }
      return (++seen == 2) ? -EINTR : 0;
    };

  nsfs::ConvertProgress first;
  EXPECT_EQ(upgrade(&stop, &first), -EINTR);
  EXPECT_TRUE(first.profile_set) << "the mask went down before the walk";
  EXPECT_FALSE(first.complete);

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_TRUE(b->is_converting());

  /* the same call again, through set_profile and not convert_tree */
  nsfs::ConvertProgress second;
  ASSERT_EQ(upgrade(nullptr, &second), 0);
  EXPECT_TRUE(second.profile_set);
  EXPECT_TRUE(second.complete);
  EXPECT_FALSE(b->is_converting());

  for (int i = 0; i < 4; ++i) {
    EXPECT_FALSE(xattr_names(bucket_path() /
			     ("obj-" + std::to_string(i) + ".bin"))
		     .contains("user.noobaa.content_type")) << i;
  }

  /* and once more, on a bucket with nothing left to do */
  nsfs::ConvertProgress third;
  ASSERT_EQ(upgrade(nullptr, &third), 0);
  EXPECT_EQ(third.objects, 0u) << "a settled bucket was walked again";
}

/* The driver reports the profile and whether it is still converting.
 *
 * What the GET renders, and what an operator polls while a long
 * rewrite runs. */
TEST_F(NSFSUpgradeTest, ReportsTheProfileAndTheConvertingState)
{
  nb_object("a.bin", "text/plain");
  nb_object("b.bin", "text/plain");

  uint32_t ext = 0;
  std::string pname;
  bool converting = true;
  ASSERT_EQ(driver->get_bucket_profile(env->dpp, null_yield, testname,
				       &ext, &pname, &converting), 0);
  EXPECT_EQ(pname, "base");
  EXPECT_FALSE(converting) << "a base bucket is not converting;  it is "
			      "simply in their format";

  nsfs::convert_cb_t stop =
    [](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      return (ev.phase == nsfs::ConvertEvent::Phase::pruned) ? -EINTR : 0;
    };
  EXPECT_EQ(upgrade(&stop, nullptr), -EINTR);

  ASSERT_EQ(driver->get_bucket_profile(env->dpp, null_yield, testname,
				       &ext, &pname, &converting), 0);
  EXPECT_EQ(pname, "strong");
  EXPECT_EQ(ext, nsfs::EXTENSIONS_STRONG);
  EXPECT_TRUE(converting) << "a stopped conversion did not report itself";

  ASSERT_EQ(upgrade(nullptr, nullptr), 0);
  ASSERT_EQ(driver->get_bucket_profile(env->dpp, null_yield, testname,
				       &ext, &pname, &converting), 0);
  EXPECT_FALSE(converting);
}

/* Two temp directories means two bucket ids have written here, and
 * which holds the uploads is not decidable.  Rejected rather than
 * guessed -- and no third is created. */
TEST_F(NSFSNooBaaBucketTest, RejectsAnAmbiguousTree)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);
  /* uuid-shaped, because that is what the strategy recognises */
  sf::create_directories(bucket_path() /
			 (".noobaa-nsfs_" + fake_uuid("someone-else")));

  auto upload = bucket->get_multipart_upload("k", rgw::sal::no_authenticated_user, "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
  rgw_placement_rule placement;
  Attrs attrs;
  EXPECT_LT(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  int n = 0;
  for (auto& e : sf::directory_iterator(bucket_path())) {
    if (e.path().filename().string().starts_with(".noobaa-nsfs_")) {
      ++n;
    }
  }
  EXPECT_EQ(n, 2) << "a third temp directory was created";
}

/* The same upload, in each format and staging layout the driver
 * serves.
 *
 * Three instantiations:  the nsfs format on a filesystem whose extents can
 * be shared, so parts land in their own files;  the nsfs format where they
 * cannot, so parts are placed in one file by stride;  and NooBaa's
 * format, whose staging layout comes with it.  The body is one body.
 * It does not branch on its parameter -- a branch written from the
 * implementation is checked by nothing -- so it asserts only what
 * holds of all three, and the spellings that differ are asserted in
 * the tests that name a format.
 *
 * What holds of all three is the contract:  an upload that is listed
 * while it is in flight, parts that report the sizes they were
 * written with, an object with the bytes in order, and staging gone
 * afterwards.
 *
 * The control is the first assertion in each body.  A case that
 * silently ran with another layout -- the probe override stuck, a
 * profile that did not take -- fails there rather than passing as a
 * duplicate of its neighbour.
 */
struct FormatCase {
  const char* label;
  std::optional<bool> shares_extents;
  uint32_t extensions;
  const char* mpu_name;
  make_tree_fn_t make_tree;
};

/* so a failure names the case rather than dumping the bytes */
void PrintTo(const FormatCase& c, std::ostream* os) { *os << c.label; }

class NSFSFormatTest : public NSFSBucketTest,
		       public ::testing::WithParamInterface<FormatCase> {
public:
  std::optional<bool> shares_extents() const override {
    return GetParam().shares_extents;
  }

  void SetUp() override {
    NSFSBucketTest::SetUp();

    auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
    ASSERT_EQ(b->resolve_profile(env->dpp), 0);
    ASSERT_EQ(b->set_profile(env->dpp, null_yield, GetParam().extensions), 0);

    /* Quiescent:  written to, no upload.  The nsfs format has no such state and
     * says so by doing nothing, so this starts every case from the
     * most furnished tree its format has without an upload in it. */
    TestTree tree;
    GetParam().make_tree(bucket_path(), TreeShape::quiescent, tree);
  }

  sf::path bucket_path() const { return bp / "root" / testname; }

  size_t uploads_listed() {
    std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
    std::string marker;
    bool truncated = false;
    EXPECT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 100, uploads,
				      nullptr, &truncated, null_yield,
				      rgw::sal::no_authenticated_user), 0);
    EXPECT_FALSE(truncated);
    return uploads.size();
  }
};

/* uuid-shaped, and used by every case:  neither layout parses an
 * upload id -- nsfs url-encodes `<key>.<id>` into a directory name,
 * NooBaa names the directory for the id alone -- so one id serves all
 * three without saying anything untrue about either */
static const std::string uniform_upload_id{
  "11111111-2222-4333-8444-555555555555"};

TEST_P(NSFSFormatTest, MultipartRoundTrip)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_STREQ(b->mpu_strategy()->name(), GetParam().mpu_name);

  const std::string objname = "written/by/us.bin";
  auto upload = bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, uniform_upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  const size_t part_size = 64;
  const int parts = 3;
  std::string expected;
  std::map<int, std::string> part_etags;
  for (int i = 1; i <= parts; ++i) {
    std::string payload(part_size, static_cast<char>('a' + i));
    part_etags[i] = write_mp_part(upload.get(), acl_owner, &placement, i,
				  payload);
    expected += payload;
  }

  EXPECT_EQ(uploads_listed(), 1u) << "in flight and not listed";

  int next = 0;
  bool truncated = false;
  ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
			       &truncated, null_yield), 0);
  ASSERT_EQ(upload->get_parts().size(), static_cast<size_t>(parts));
  for (auto& [num, part] : upload->get_parts()) {
    EXPECT_EQ(part->get_size(), part_size) << "part " << num;
  }

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);

  const sf::path obj{bucket_path() / objname};
  ASSERT_TRUE(sf::is_regular_file(obj)) << obj;
  EXPECT_EQ(read_all(obj), expected);

  EXPECT_EQ(uploads_listed(), 0u) << "staging outlived the upload";
}

/* A final part shorter than the others.
 *
 * Every layout has a case for it and they are different cases:  parts
 * in their own files do not care, the strided layout cannot place a
 * part whose size is not the stride, and NooBaa keys the file by the
 * size, so a short part is in a second file that assembly has to copy
 * from rather than link.  Uniform here, and what distinguishes them is
 * only the cost.
 *
 * It is also where `parts-size-<n>` -- the one name both layouts spell
 * the same -- is read by whichever layout did not write it.
 */
TEST_P(NSFSFormatTest, MultipartShortFinalPart)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_STREQ(b->mpu_strategy()->name(), GetParam().mpu_name);

  const std::string objname = "short/tail.bin";
  auto upload = bucket->get_multipart_upload(objname, rgw::sal::no_authenticated_user, uniform_upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  const size_t part_size = 64;
  const size_t tail_size = 17;
  std::string expected;
  std::map<int, std::string> part_etags;
  for (int i = 1; i <= 3; ++i) {
    const size_t len = (i == 3) ? tail_size : part_size;
    std::string payload(len, static_cast<char>('a' + i));
    part_etags[i] = write_mp_part(upload.get(), acl_owner, &placement, i,
				  payload);
    expected += payload;
  }

  int next = 0;
  bool truncated = false;
  ASSERT_EQ(upload->list_parts(env->dpp, env->cct.get(), 100, 0, &next,
			       &truncated, null_yield), 0);
  const auto& got = upload->get_parts();
  ASSERT_EQ(got.size(), 3u);
  ASSERT_NE(got.find(3), got.end());
  EXPECT_EQ(got.at(3)->get_size(), tail_size)
      << "the short part was reported at the others' size";

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);

  const sf::path obj{bucket_path() / objname};
  ASSERT_TRUE(sf::is_regular_file(obj)) << obj;
  EXPECT_EQ(sf::file_size(obj), expected.size());
  EXPECT_EQ(read_all(obj), expected);
}

      return std::string(i.param.label);
    });


TEST_F(NooBaaStoreTest, AStoreNooBaaToolsWroteReadsBack)
{

/* The object attributes NooBaa writes at the pin are known, and nothing
 * else under `user.noobaa.` is.  Names outside it -- user metadata, nsfs's
 * own -- are not this layer's to refuse. */
TEST(NooBaaSchema, ObjectAttributesKnownAreNooBaasAtThePin)
{
  namespace ns = rgw::sal::nsfs::noobaa_schema;
  for (const char* n : {"user.noobaa.content_type", "user.noobaa.version_id",
			"user.noobaa.dir_content", "user.noobaa.tag.colour",
			"user.noobaa.retention_date",
			"user.noobaa.restore.request",
			"user.noobaa.migrate.staged",
			"user.content_md5", "user.storage_class", "user.colour",
			"user.nsfs:bucket_info"}) {
    EXPECT_TRUE(ns::object_attribute_known(n)) << n;
  }
  for (const char* n : {"user.noobaa.encryption", "user.noobaa.tag",
			"user.noobaa.retention_until",
			"user.noobaa.restore"}) {
    EXPECT_FALSE(ns::object_attribute_known(n)) << n;
  }
}

/* Every `user.noobaa.*` name NooBaa wrote on the objects of a capture is
 * one the object-attribute table knows.
 *
 * `NSFS_NOOBAA_CAPTURE` names the corpus's capture of a tree NooBaa
 * wrote -- `capture/noobaa` in the stand-up's work directory, whose
 * `*.inventory` files list each entry's attributes as `path  xattr
 * name`.  Skipped when unset.  The table is read off NooBaa's source;
 * this holds it to what NooBaa actually wrote. */
TEST(NooBaaSchema, ObjectAttributesAgreeWithWhatNooBaaWrote)
{
  const char* dir = ::getenv("NSFS_NOOBAA_CAPTURE");
  if (!dir || !*dir) {
    GTEST_SKIP() << "NSFS_NOOBAA_CAPTURE is unset;  see "
		 << "src/test/rgw/adoption/standup.rs schema-agreement";
  }
  namespace ns = rgw::sal::nsfs::noobaa_schema;
  size_t seen = 0;
  std::string unknown;
  for (const auto& f : std::filesystem::directory_iterator(dir)) {
    if (f.path().extension() != ".inventory") {
      continue;
    }
    std::ifstream in(f.path());
    for (std::string line; std::getline(in, line);) {
      std::vector<std::string> col;
      for (auto c : ceph::split(line, "\t")) {
	col.emplace_back(c);
      }
      if ((col.size() != 3) || (col[1] != "xattr") ||
	  !col[2].starts_with("user.noobaa.")) {
	continue;
      }
      ++seen;
      if (!ns::object_attribute_known(col[2])) {
	unknown += "\n  " + f.path().filename().string() + ":  " + col[0] +
	  "  " + col[2];
      }
    }
  }
  EXPECT_GT(seen, 0u) << dir << " holds no NooBaa attribute";
  EXPECT_TRUE(unknown.empty())
      << "NooBaa wrote attributes the table does not know:" << unknown;
}

TEST_F(NooBaaKeysTest, NooBaaKeyOpensNooBaaSecret)
{
  EXPECT_EQ(got.fs_backend, "GPFS");
}
/* A write's result is its own, whatever other threads do on the
 * connection meanwhile.
 *
 * The connection is dbstore's, shared by every thread, and a write's
 * result is read after its step:  sqlite3_changes() for a placement
 * insert and an abandoned-row removal, sqlite3_extended_errcode() for a
 * duplicate.  Read as a separate call, either can return another
 * thread's statement's instead.  On rhel97-gpfs a bucket create was
 * answered 409 this way while its row went in, and the row stayed with
 * no directory.  Three kinds of thread run together:  inserts, each
 * then repeated as a duplicate;  removals of a row that is not there,
 * which change nothing;  and abandoned rows written and removed, which
 * change one.  Every result must be the one its own statement earned. */
TEST_F(NSFSIdentityDBTest, AWritesResultIsItsOwnUnderConcurrency)
{
  /* On tmpfs, where one exists:  on a disk each write waits for its
   * commit to sync, few statements land between a step and its read,
   * and the race goes unseen. */
  const sf::path dir = sf::is_directory("/dev/shm")
    ? sf::path{"/dev/shm"} / ("nsfs-identity-" + std::to_string(::getpid()))
    : bp / "identity-concurrency";
  sf::create_directories(dir);
  auto remove_dir = make_scope_guard([&dir] { sf::remove_all(dir); });
  auto sdb = std::make_unique<nsfs::IdentityDB>((dir / "identity").string(),
						 env->cct.get());
  ASSERT_EQ(sdb->Initialize("", -1), 0);

  constexpr int inserters = 8;
  constexpr int removers = 8;
  constexpr int abandoners = 4;
  constexpr int each = 200;
  std::atomic<bool> stop{false};
  std::atomic<int> wrong_insert{0};
  std::atomic<int> wrong_duplicate{0};
  std::atomic<int> wrong_removal{0};
  std::atomic<int> wrong_absent_removal{0};
  std::atomic<int> failed{0};

  std::vector<std::thread> ts;
  for (int t = 0; t < inserters; ++t) {
    ts.emplace_back([&, t] {
      for (int i = 0; i < each; ++i) {
	nsfs::BucketPlacement bp;
	bp.owner = "RGW00000000000000001";
	bp.bucket_name = "p" + std::to_string(t) + "-" + std::to_string(i);
	bp.path = "/ibm/gpfs/fs1/" + bp.bucket_name;
	if (sdb->insert_bucket_placement(env->dpp, bp) != 0) {
	  ++wrong_insert;
	}
	if (sdb->insert_bucket_placement(env->dpp, bp) != -EEXIST) {
	  ++wrong_duplicate;
	}
      }
    });
  }
  std::vector<std::thread> rivals;
  for (int t = 0; t < removers; ++t) {
    rivals.emplace_back([&, t] {
      const std::string absent = "absent-" + std::to_string(t);
      while (!stop.load()) {
	if (sdb->remove_bucket_placement(env->dpp, absent, "") != 0) {
	  ++failed;
	}
      }
    });
  }
  for (int t = 0; t < abandoners; ++t) {
    rivals.emplace_back([&, t] {
      nsfs::AbandonedRecord r;
      r.noobaa_id = "nb-" + std::to_string(t);
      r.kind = "bucket";
      r.name = "abandoned-" + std::to_string(t);
      r.stage = "not imported";
      r.reason = "concurrency";
      r.at = "2026-11-06T00:00:00Z";
      while (!stop.load()) {
	if (sdb->put_abandoned(env->dpp, r) != 0) {
	  ++failed;
	  return;
	}
	if (sdb->remove_abandoned(env->dpp, r.noobaa_id) != 0) {
	  ++wrong_removal;
	}
	if (sdb->remove_abandoned(env->dpp, r.noobaa_id) != -ENOENT) {
	  ++wrong_absent_removal;
	}
      }
    });
  }
  for (auto& t : ts) {
    t.join();
  }
  stop = true;
  for (auto& t : rivals) {
    t.join();
  }

  EXPECT_EQ(failed.load(), 0);
  EXPECT_EQ(wrong_insert.load(), 0)
      << "inserts that went in returned another statement's result";
  EXPECT_EQ(wrong_duplicate.load(), 0)
      << "duplicates returned another statement's error code";
  EXPECT_EQ(wrong_removal.load(), 0)
      << "removals of a row returned another statement's count";
  EXPECT_EQ(wrong_absent_removal.load(), 0)
      << "removals of no row returned another statement's count";

  for (int t = 0; t < inserters; ++t) {
    for (int i = 0; i < each; ++i) {
      const std::string name = "p" + std::to_string(t) + "-"
	+ std::to_string(i);
      nsfs::BucketPlacement got;
      ASSERT_EQ(sdb->get_bucket_placement(env->dpp, name, "", got), 0)
	  << name << " has no row";
    }
  }
}
TEST_F(NooBaaPlanTest, AStoreNooBaaToolsWroteMapsCleanly)
{
/* An nsswitch-resolved account whose name nsswitch does not know is imported as
 * NooBaa's record stood, and named in the result, so the operator
 * learns of it at the import rather than from a refused request.  The
 * control is the account whose name resolves, which must not be named. */
TEST_F(NooBaaImportTest, AnUnresolvedNameIsReported)
{
  FakeDirectory dir;
  dir.names["knownname"] = {70101, 100};
  driver->get_identity_db()->nsswitch_cache().set_lookup_for_test(dir.fn());

  auto dn_account = [] (const char* id, const char* name, const char* key,
		     const char* dn) {
    std::string j = NB_ACCOUNT_PLAIN;
    auto sub = [&j] (const std::string& from, const std::string& to) {
      const auto at = j.find(from);
      ASSERT_NE(at, std::string::npos) << from;
      j.replace(at, from.size(), to);
    };
    sub("6ac7cb27fd4af356919110a3", id);
    sub("\"name\": \"interchange\"", std::string("\"name\": \"") + name
	+ "\"");
    sub("\"email\": \"interchange\"", std::string("\"email\": \"") + name
	+ "\"");
    sub("INTERCHANGEACCESSKEY", key);
    sub("\"uid\": 1000,\n    \"gid\": 1000,",
	std::string("\"distinguished_name\": \"") + dn + "\",");
    return j;
  };
  const std::string known = dn_account("6ac7cb27fd4af356919110b1",
					"ix-known", "INTERCHANGEKNOWNKEY1",
					"knownname");
  const std::string unknown = dn_account("6ac7cb27fd4af356919110b2",
					  "ix-unknown", "INTERCHANGEUNKNOWNK1",
					  "unknownname");
  add_identity("6ac7cb27fd4af356919110b1", known.c_str());
  add_identity("6ac7cb27fd4af356919110b2", unknown.c_str());
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.identity_rows, 2u) << "an unresolved name was not written";
  ASSERT_EQ(result.unresolved_names.size(), 1u);
  EXPECT_NE(result.unresolved_names[0].find("unknownname"),
	    std::string::npos) << result.unresolved_names[0];
}

/* I8.  A store whose config.json has NooBaa's dynamic groups on is
 * refused by a gateway with rgw_nsfs_dynamic_supplemental_groups off:
 * its list-less accounts would lose the groups the site gives them.
 * Nothing is written.  The control is the gateway with the switch on,
 * which imports it, and reports a list-less account whose uid nsswitch
 * does not know. */
TEST_F(NooBaaImportTest, DynamicGroupsMustMatchTheStore)
{
  FakeDirectory dir;
  dir.users[1000] = {"known", 1000};
  dir.install(driver->get_identity_db()->nsswitch_cache());

  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  std::string other = NB_ACCOUNT_PLAIN;
  auto sub = [&other] (const std::string& from, const std::string& to) {
    const auto at = other.find(from);
    ASSERT_NE(at, std::string::npos) << from;
    other.replace(at, from.size(), to);
  };
  sub("6ac7cb27fd4af356919110a3", "6ac7cb27fd4af356919110c1");
  sub("\"name\": \"interchange\"", "\"name\": \"ix-nouser\"");
  sub("\"email\": \"interchange\"", "\"email\": \"ix-nouser\"");
  sub("INTERCHANGEACCESSKEY", "INTERCHANGENOUSERK01");
  sub("\"uid\": 1000,\n    \"gid\": 1000,", "\"uid\": 70199,\n    \"gid\": 70199,");
  add_identity("6ac7cb27fd4af356919110c1", other.c_str());
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);
  write_file(store + "/config.json",
	     R"({"NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_GROUPS": true})");

  driver->get_identity_db()->nsswitch_cache().override_dynamic_groups(false);
  EXPECT_LT(run(), 0) << "a store with dynamic groups on was imported "
			 "by a gateway with them off";
  EXPECT_EQ(result.identity_rows, 0u) << "the refusal wrote rows";
  ASSERT_FALSE(result.failures.empty());

  driver->get_identity_db()->nsswitch_cache().override_dynamic_groups(true);
  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.identity_rows, 2u);
  ASSERT_EQ(result.unresolved_uids.size(), 1u);
  EXPECT_NE(result.unresolved_uids[0].find("70199"), std::string::npos)
      << result.unresolved_uids[0];
}
  /* NooBaa's id, recorded against the account it became */
  std::string owner;
  EXPECT_EQ(owner, e->account_id);

  /* the identity row, keyed on the account */
  nsfs::Identity row;
  ASSERT_EQ(driver->get_identity_db()->get_identity(
		env->dpp, e->account_id, row), 0);
  std::string owner;
  ASSERT_EQ(driver->get_identity_db()->get_owner_by_noobaa_id(
		env->dpp, "6ac7cb27fd4af356919110a3", owner), 0);
TEST_F(NooBaaImportTest, ARenameInNooBaaIsStillTheSameIdentity)
{
  std::string owner;
  ASSERT_EQ(driver->get_identity_db()->get_owner_by_noobaa_id(
		env->dpp, "6ac7cb27fd4af356919110a3", owner), 0);
  EXPECT_EQ(owner, id);
}
  std::string owner;
  ASSERT_EQ(driver->get_identity_db()->get_owner_by_noobaa_id(
		env->dpp, "6ac7d213cac521f05aac9fb4", owner), 0);
  EXPECT_EQ(owner, RGW_USER_ANON_ID);
  EXPECT_EQ(result.identity_rows, 1u) << "an account row too many or few";
  EXPECT_EQ(result.user_rows, 1u) << "the IAM user's own row was not written";

  const std::string account = entry("interchange")->account_id;
  /* the account's row is the account's, not overwritten by the
   * member -- which is what keying every identity on the account did */
  ASSERT_EQ(driver->get_identity_db()->get_identity(
		env->dpp, account, row), 0);
  EXPECT_EQ(row.uid, 1000u) << "the member's copy replaced the account's";
  /* and the member's is its own NooBaa tuple, not a copy of the
   * account's */
  ASSERT_EQ(driver->get_identity_db()->get_identity(
		env->dpp, user->get_id().to_str(), row), 0)
      << "the member's own row was not written";
  EXPECT_EQ(row.uid, 1500u) << "the member's row is not its NooBaa tuple";

  /* which it runs as is the override's */
  auto* db = driver->get_identity_db();
  nsfs::Credentials cred;
  db->force_user_identity_override(false);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, user->get_id(),
				      user->get_info().account_id, cred, null_yield), 0);
  EXPECT_EQ(cred.uid, 1000u) << "without the override it is not the account";
  db->force_user_identity_override(true);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, user->get_id(),
				      user->get_info().account_id, cred, null_yield), 0);
  EXPECT_EQ(cred.uid, 1500u) << "under the override it is not its own row";
  db->force_user_identity_override(std::nullopt);

  /* both ids recorded, each against what it became */
  std::string owner;
  ASSERT_EQ(driver->get_identity_db()->get_owner_by_noobaa_id(
		env->dpp, "6ac7cb27fd4af356919110a3", owner), 0);
  EXPECT_EQ(owner, account);
  ASSERT_EQ(driver->get_identity_db()->get_owner_by_noobaa_id(
		env->dpp, "u1", owner), 0);
  EXPECT_EQ(owner, to_string(rgw_owner{user->get_id()}));

  /* the drift, named */
  ASSERT_EQ(plan.drift.size(), 1u);
  EXPECT_NE(plan.drift[0].find("a-user"), std::string::npos) << plan.drift[0];
  EXPECT_NE(plan.drift[0].find("uid/gid"), std::string::npos) << plan.drift[0];
  EXPECT_EQ(plan.drift[0].find("new_buckets_path"), std::string::npos)
      << "a field that matches was reported:  " << plan.drift[0];
  EXPECT_NE(plan.drift[0].find("rgw_nsfs_user_identity_override"),
	    std::string::npos)
      << "the report does not name the override:  " << plan.drift[0];
}

/* A member whose copy matches its account is not drift.
 *
 * The control for the report above:  NooBaa copies every field at
 * creation, so a user nobody changed the account under is the
 * ordinary case and must say nothing. */
TEST_F(NooBaaImportTest, AFaithfulCopyIsNotDrift)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("u1", R"({"_id":"u1","name":"a-user","email":"u@x",
    "owner":"6ac7cb27fd4af356919110a3",
    "nsfs_account_config":{"uid":1000,"gid":1000,
      "new_buckets_path":"/lv2tb/nsfs-interchange/data/root"},
    "access_keys":[{"access_key":"USERAAAAAAAAAAAAAAAA",
      "encrypted_secret_key":"AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51FijvRm3nJgdKQ=="}],
    "master_key_id":"6ac7cb2751b72219a4bae375"})");
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);

  ASSERT_EQ(run(), 0) << why();
  EXPECT_TRUE(plan.drift.empty()) << plan.drift[0];
}

/* Re-planning a store with an IAM user keeps the account's id.
 *
 * The regression `36cc060acb7` fixed, in the shape that brought it
 * back:  while every identity's row was keyed on the account, the IAM
 * user's write replaced the root's NooBaa id, the root was no longer
 * found, and a second plan -- the one `adopt_one_bucket()` builds --
 * minted a new account id.  Every adopted bucket was then owned by an
 * account no imported user was in. */
TEST_F(NooBaaImportTest, ReplanningWithAnIamUserKeepsTheAccountId)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("u1", R"({"_id":"u1","name":"a-user","email":"u@x",
    "owner":"6ac7cb27fd4af356919110a3",
    "nsfs_account_config":{"uid":1000,"gid":1000},
    "access_keys":[{"access_key":"USERAAAAAAAAAAAAAAAA",
      "encrypted_secret_key":"AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51FijvRm3nJgdKQ=="}],
    "master_key_id":"6ac7cb2751b72219a4bae375"})");
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);

  ASSERT_EQ(run(), 0) << why();
  const std::string first = entry("interchange")->account_id;

  /* the control:  with nothing to consult, a plan mints afresh, so the
   * equality below is the lookup and not a coincidence */
  auto c = read_store();
  nsfs::ImportPlan fresh;
  ASSERT_EQ(nsfs::build_import_plan(env->dpp, env->cct.get(), c, fresh), 0);
  const nsfs::ImportEntry* f = nullptr;
  for (const auto& e : fresh.entries) {
    if (e.noobaa_name == "interchange") {
      f = &e;
    }
  }
  ASSERT_NE(f, nullptr);
  ASSERT_NE(f->account_id, first);

  nsfs::ImportPlan again;
  ASSERT_EQ(nsfs::build_import_plan(env->dpp, env->cct.get(), c, again,
				    driver->get_identity_db()), 0);
  for (const auto& e : again.entries) {
    EXPECT_EQ(e.account_id, first)
	<< e.noobaa_name << " was planned into a different account";
  }
}
  /* The gateway set as a deployment adopting this store sets it:  the
   * import refuses a store with NooBaa's dynamic groups on when
   * rgw_nsfs_dynamic_supplemental_groups is off (I8), and the stand-up
   * writes one under impersonation. */
  driver->get_identity_db()->nsswitch_cache().override_dynamic_groups(
    c.settings.dynamic_supplemental_groups);
  EXPECT_TRUE(r.unresolved_names.empty()) << r.unresolved_names.front();
  EXPECT_TRUE(r.unresolved_uids.empty()) << r.unresolved_uids.front();
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

  std::map<std::string, std::string> out;
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

/* I9:  under rgw_nsfs_user_identity_override a new bucket goes under
 * its creating user's own new_buckets_path, as NooBaa's CreateBucket
 * reads the requesting record's;  the bucket is still the account's.
 * Without the override it goes under the account's, whatever the
 * user's row says. */
TEST_F(NSFSPlacedBucketTest, AUsersOwnNewBucketsPathIsUsedUnderTheOverride)
{
  auto* db = driver->get_identity_db();
  const rgw_account_id acct{"RGW88888888888888888"};
  const sf::path apath = elsewhere() / "acct-path";
  const sf::path upath = elsewhere() / "user-path";
  sf::create_directories(apath);
  sf::create_directories(upath);
  nsfs::Identity arow;
  arow.key = acct;
  arow.new_buckets_path = apath.string();
  ASSERT_EQ(db->put_identity(env->dpp, arow), 0);

  const rgw_user hal{"", "path-hal"};
  auto u = std::make_unique<rgw::sal::NSFSUser>(driver.get(), hal);
  u->get_info().user_id = hal;
  u->get_info().account_id = acct;
  u->get_info().display_name = "hal";
  ASSERT_EQ(u->store_user(env->dpp, null_yield, true, nullptr), 0);
  nsfs::Identity urow;
  ASSERT_EQ(db->get_identity(env->dpp, hal.to_str(), urow), 0)
      << "the user got no copy to move";
  urow.new_buckets_path = upath.string();
  ASSERT_EQ(db->put_identity(env->dpp, urow), 0);

  auto create = [&] (const std::string& name) {
    RGWBucketInfo info;
    info.bucket.name = name;
    info.owner = rgw_owner{acct};
    auto b = driver->get_bucket(info);
    rgw::sal::Bucket::CreateParams cp;
    cp.owner = rgw_owner{acct};
    return b->create(env->dpp, cp, null_yield, rgw_owner{hal});
  };

  db->force_user_identity_override(false);
  ASSERT_EQ(create("by-account-row"), 0);
  EXPECT_TRUE(sf::is_directory(apath / "by-account-row"))
      << "without the override the account's path did not place it";
  EXPECT_FALSE(sf::exists(upath / "by-account-row"));

  db->force_user_identity_override(true);
  ASSERT_EQ(create("by-user-row"), 0);
  EXPECT_TRUE(sf::is_directory(upath / "by-user-row"))
      << "under the override the user's own path did not place it";
  EXPECT_FALSE(sf::exists(apath / "by-user-row"));
  nsfs::BucketPlacement row;
  ASSERT_EQ(db->get_bucket_placement(env->dpp, "by-user-row", "", row), 0);
  EXPECT_EQ(row.owner, to_string(rgw_owner{acct}))
      << "the user's path was used, but the bucket is the account's";

  /* a request with no authenticated user uses the owner's path */
  RGWBucketInfo info;
  info.bucket.name = "by-nobody";
  info.owner = rgw_owner{acct};
  auto b = driver->get_bucket(info);
  rgw::sal::Bucket::CreateParams cp;
  cp.owner = rgw_owner{acct};
  ASSERT_EQ(b->create(env->dpp, cp, null_yield,
		      rgw::sal::no_authenticated_user), 0);
  EXPECT_TRUE(sf::is_directory(apath / "by-nobody"));
  db->force_user_identity_override(std::nullopt);
}

/* A role in an account, for the tests of a role session's new buckets:
 * created as IAM CreateRole creates one, which gives it its id. */
static std::string make_account_role(const DoutPrefixProvider* dpp,
				     rgw::sal::Driver* driver,
				     const std::string& name,
				     const rgw_account_id& acct)
{
  RGWRoleInfo ri;
  ri.name = name;
  ri.path = "/";
  ri.account_id = acct;
  ri.trust_policy = R"({"Version":"2012-10-17","Statement":[{"Effect":"Allow",)"
    R"("Principal":{"AWS":["arn:aws:iam::)" + std::string(acct) +
    R"(:root"]},"Action":["sts:AssumeRole"]}]})";
  ri.max_session_duration = RGWRole::SESSION_DURATION_MIN;
  std::unique_ptr<RGWRole> role = driver->get_role(ri);
  if (role->create(dpp, "", null_yield) < 0) {
    return {};
  }
  return role->get_info().id;
}

/* I9:  under rgw_nsfs_role_identity_override a role session's new
 * bucket goes under the role's own new_buckets_path, as a user's goes
 * under its own under the user override;  the bucket is still the
 * account's.  Without the override, and for a role of another account,
 * it goes under the account's. */
TEST_F(NSFSPlacedBucketTest, ARolesOwnNewBucketsPathIsUsedUnderTheOverride)
{
  auto* db = driver->get_identity_db();
  const rgw_account_id acct{"RGW77777777777777777"};
  const rgw_account_id other{"RGW77777777777777778"};
  const sf::path apath = elsewhere() / "acct-path";
  const sf::path rpath = elsewhere() / "role-path";
  sf::create_directories(apath);
  sf::create_directories(rpath);
  nsfs::Identity arow;
  arow.key = acct;
  arow.new_buckets_path = apath.string();
  ASSERT_EQ(db->put_identity(env->dpp, arow), 0);

  const std::string mine = make_account_role(env->dpp, driver.get(),
					     "path-role", acct);
  const std::string theirs = make_account_role(env->dpp, driver.get(),
					       "path-role", other);
  ASSERT_FALSE(mine.empty()) << "the role was not created";
  ASSERT_FALSE(theirs.empty()) << "the other account's role was not created";
  for (const auto& id : {mine, theirs}) {
    nsfs::Identity rrow;
    rrow.key = nsfs::role_identity_key(id);
    rrow.new_buckets_path = rpath.string();
    ASSERT_EQ(db->put_identity(env->dpp, rrow), 0);
  }

  auto create = [&] (const std::string& name, const std::string& role_id) {
    RGWBucketInfo info;
    info.bucket.name = name;
    info.owner = rgw_owner{acct};
    auto b = driver->get_bucket(info);
    rgw::sal::Bucket::CreateParams cp;
    cp.owner = rgw_owner{acct};
    const rgw_owner session{rgw_user{"", role_id,
				     std::string(rgw::sal::role_session_ns)}};
    return b->create(env->dpp, cp, null_yield, session);
  };

  db->force_role_identity_override(false);
  ASSERT_EQ(create("role-off", mine), 0);
  EXPECT_TRUE(sf::is_directory(apath / "role-off"))
      << "without the override the account's path did not place it";
  EXPECT_FALSE(sf::exists(rpath / "role-off"));

  db->force_role_identity_override(true);
  ASSERT_EQ(create("role-on", mine), 0);
  EXPECT_TRUE(sf::is_directory(rpath / "role-on"))
      << "under the override the role's own path did not place it";
  EXPECT_FALSE(sf::exists(apath / "role-on"));
  nsfs::BucketPlacement row;
  ASSERT_EQ(db->get_bucket_placement(env->dpp, "role-on", "", row), 0);
  EXPECT_EQ(row.owner, to_string(rgw_owner{acct}))
      << "the role's path was used, but the bucket is the account's";

  ASSERT_EQ(create("role-elsewhere", theirs), 0);
  EXPECT_TRUE(sf::is_directory(apath / "role-elsewhere"))
      << "a role of another account placed the account's bucket";
  EXPECT_FALSE(sf::exists(rpath / "role-elsewhere"));

  /* a role given no identity follows its account */
  ASSERT_EQ(db->remove_identity(env->dpp, nsfs::role_identity_key(mine)), 0);
  ASSERT_EQ(create("role-no-row", mine), 0);
  EXPECT_TRUE(sf::is_directory(apath / "role-no-row"));
  db->force_role_identity_override(std::nullopt);
}

  auto own = make_bucket("mine");

  auto others = driver->get_bucket(info);
  ASSERT_EQ(others->create(env->dpp, cp, null_yield, rgw::sal::no_authenticated_user), 0);
/* I9:  under rgw_nsfs_user_identity_override the creating user's own
 * allowlist admits a custom path, as NooBaa checks the requesting
 * record's;  without it the account's does.  The account allows one
 * directory and the user another, so each setting admits exactly one. */
TEST_F(NSFSCustomPathTest, TheUsersOwnAllowlistAdmitsUnderTheOverride)
{
  auto* db = driver->get_identity_db();
  const rgw_account_id acct{"RGW99999999999999999"};
  const sf::path aroot = elsewhere() / "acct-allowed";
  const sf::path uroot = elsewhere() / "user-allowed";
  sf::create_directories(aroot);
  sf::create_directories(uroot);
  nsfs::Identity arow;
  arow.key = acct;
  arow.custom_bucket_path_allowed_list = aroot.string();
  ASSERT_EQ(db->put_identity(env->dpp, arow), 0);

  const rgw_user ida{"", "allow-ida"};
  auto u = std::make_unique<rgw::sal::NSFSUser>(driver.get(), ida);
  u->get_info().user_id = ida;
  u->get_info().account_id = acct;
  u->get_info().display_name = "ida";
  ASSERT_EQ(u->store_user(env->dpp, null_yield, true, nullptr), 0);
  nsfs::Identity urow;
  ASSERT_EQ(db->get_identity(env->dpp, ida.to_str(), urow), 0);
  urow.custom_bucket_path_allowed_list = uroot.string();
  ASSERT_EQ(db->put_identity(env->dpp, urow), 0);

  auto create_at = [&] (const std::string& name, const sf::path& at) {
    env_.set("HTTP_X_NOOBAA_CUSTOM_BUCKET_PATH", at.string());
    RGWBucketInfo info;
    info.bucket.name = name;
    info.owner = rgw_owner{acct};
    auto b = driver->get_bucket(info);
    rgw::sal::Bucket::CreateParams cp;
    cp.owner = rgw_owner{acct};
    cp.env = &env_;
    return b->create(env->dpp, cp, null_yield, rgw_owner{ida});
  };

  db->force_user_identity_override(false);
  EXPECT_EQ(create_at("off-user", uroot / "off-user"), -EACCES)
      << "without the override the user's allowlist admitted a path";
  EXPECT_EQ(create_at("off-acct", aroot / "off-acct"), 0);

  db->force_user_identity_override(true);
  EXPECT_EQ(create_at("on-acct", aroot / "on-acct"), -EACCES)
      << "under the override the account's allowlist still admitted";
  EXPECT_EQ(create_at("on-user", uroot / "on-user"), 0);
  EXPECT_EQ(row_path("on-user"), (uroot / "on-user").string());
  db->force_user_identity_override(std::nullopt);
}

/* I9:  under rgw_nsfs_role_identity_override a role session's own
 * allowlist admits a custom path, as a user's does under the user
 * override;  without it the account's does. */
TEST_F(NSFSCustomPathTest, TheRolesOwnAllowlistAdmitsUnderTheOverride)
{
  auto* db = driver->get_identity_db();
  const rgw_account_id acct{"RGW66666666666666666"};
  const sf::path aroot = elsewhere() / "acct-allowed";
  const sf::path rroot = elsewhere() / "role-allowed";
  sf::create_directories(aroot);
  sf::create_directories(rroot);
  nsfs::Identity arow;
  arow.key = acct;
  arow.custom_bucket_path_allowed_list = aroot.string();
  ASSERT_EQ(db->put_identity(env->dpp, arow), 0);

  const std::string role_id = make_account_role(env->dpp, driver.get(),
						"allow-role", acct);
  ASSERT_FALSE(role_id.empty()) << "the role was not created";
  nsfs::Identity rrow;
  rrow.key = nsfs::role_identity_key(role_id);
  rrow.custom_bucket_path_allowed_list = rroot.string();
  ASSERT_EQ(db->put_identity(env->dpp, rrow), 0);

  auto create_at = [&] (const std::string& name, const sf::path& at) {
    env_.set("HTTP_X_NOOBAA_CUSTOM_BUCKET_PATH", at.string());
    RGWBucketInfo info;
    info.bucket.name = name;
    info.owner = rgw_owner{acct};
    auto b = driver->get_bucket(info);
    rgw::sal::Bucket::CreateParams cp;
    cp.owner = rgw_owner{acct};
    cp.env = &env_;
    const rgw_owner session{rgw_user{"", role_id,
				     std::string(rgw::sal::role_session_ns)}};
    return b->create(env->dpp, cp, null_yield, session);
  };

  db->force_role_identity_override(false);
  EXPECT_EQ(create_at("off-role", rroot / "off-role"), -EACCES)
      << "without the override the role's allowlist admitted a path";
  EXPECT_EQ(create_at("off-acct", aroot / "off-acct"), 0);

  db->force_role_identity_override(true);
  EXPECT_EQ(create_at("on-acct", aroot / "on-acct"), -EACCES)
      << "under the override the account's allowlist still admitted";
  EXPECT_EQ(create_at("on-role", rroot / "on-role"), 0);
  EXPECT_EQ(row_path("on-role"), (rroot / "on-role").string());
  db->force_role_identity_override(std::nullopt);
}
      if (std::string_view(q).starts_with("user.nsfs:")) {

/* A subtree marker is not bucket state:  it is written below a bucket's
 * root, and says the directory was converted as part of one.  A bucket
 * removed with its directory kept leaves its subdirectories marked, and
 * one of them can be placed (DECISIONS.md 2026-11-05). */
TEST_F(NSFSPlaceBucketTest, ASubtreeMarkerIsNotBucketState)
{
  const sf::path at = elsewhere() / "was-a-subdirectory";
  sf::create_directories(at);
  ASSERT_EQ(::setxattr(at.c_str(), nsfs::SUBTREE_CONVERTED_XATTR, "3", 1, 0),
	    0);
  EXPECT_EQ(place("was-a-subdirectory", at), 0);
}

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
