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
#include <gmock/gmock.h>
#include <algorithm>
#include <array>
#include <set>
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
#include "driver/nsfs/adopt/noobaa_config_store.h"
#include "driver/nsfs/adopt/noobaa_master_keys.h"
#include "driver/nsfs/adopt/noobaa_import_plan.h"
#include "driver/nsfs/adopt/noobaa_import.h"
#include "driver/nsfs/adopt/noobaa_bucket_import.h"
#include "rgw_bucket_logging.h"
#include "rgw_account.h"
#include "driver/nsfs/personality.h"
#include "rgw_auth.h"
#include "common/ceph_json.h"

using namespace rgw::sal;

const std::string ATTR1{"attr1"};
const std::string ATTR2{"attr2"};
const std::string ATTR3{"attr3"};
const std::string ATTR_OBJECT_TYPE{"object_type"};

namespace {
  /* Start each run from an empty tree.
   *
   * **Default, and it was not.**  The wipe existed behind `--create`,
   * off unless asked for, so the suite passed on its first run against
   * a fresh directory and failed 40 tests on every run after it:  a
   * test asserting a path is absent read the path the previous run
   * created.  Worse, one of those stale states left the multipart
   * cache non-empty at teardown and the process segfaulted.  A suite
   * that only passes with an undocumented flag reports "the unit tests
   * pass" from a state nobody can reproduce.
   *
   * `--reuse` restores the old behaviour for anyone who wants to run
   * against a tree a previous run left.  `--create` is kept, and now
   * asks for what already happens. */
  bool clean_first = true;
  /* Remove the tree on the way out.  Off by default, so the tree
   * survives a run and can be inspected after a failure -- which is
   * safe now that the next run starts by clearing it. */
  bool do_delete = false;
  bool verbose = false;
}

namespace sf = std::filesystem;
class Environment* env;
sf::path base_path{"nsfstest"};
std::unique_ptr<nsfs::Directory> root;
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
    if (clean_first) {
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
  /* the three written here.  object_type was removed in 4cd543aa56c:
   * it answered one question, staging directory or ordinary, which the
   * directory's name already settles. */
  EXPECT_EQ(attrs.size(), 3);
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
  EXPECT_EQ(attrs.size(), 3);
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
  /* see FSEnt.DirBase:  object_type went in 4cd543aa56c */
  EXPECT_EQ(attrs.size(), 3);
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

    /* A database of this test's own.
     *
     * `NSFSDriver`'s constructor builds `userDB` from
     * `rgw_nsfs_userdb_dir`, which is a global and defaults to a path
     * no test may write -- and one database shared by every test is
     * the cross-test interference the suite just finished removing.
     * `userDB` is protected, so a subclass can simply point it
     * somewhere per-test;  no hook in the driver is needed for it.
     *
     * Without this the account and user write paths cannot run at
     * all, because both go through this handle. */
    userDB = std::make_unique<nsfs::IdentityDB>(driver_base + "/userdb",
						env->cct.get());
    if (int r = userDB->Initialize("", -1); r < 0) {
      ldpp_dout(env->dpp, 0) << " ERROR: could not open the test userdb: "
			     << cpp_strerror(-r) << dendl;
      return r;
    }
    set_context(env->cct.get());

    /* The base path first, because the extents probe reads it, then the
     * strategies, then the root directory -- which takes the FSStrategy
     * as a constructor argument and passes it to every child.  The real
     * driver orders these the same way;  building the root first leaves
     * every File below it with a null fs_strategy. */
    std::error_code ec;
    sf::create_directories(base_path, ec);
    if (ec) {
      ldpp_dout(env->dpp, 0) << " ERROR: could not create base path ("
			     << base_path << "): " << ec.message() << dendl;
      return -ec.value();
    }

    init_strategies(dpp, shares_extents);

    root_dir = std::make_unique<nsfs::Directory>(base_path, nullptr,
						 env->cct.get(),
						 fs_strategy.get());
    root_dir->set_mpu_strategy(mpu_strategy.get());
    root_dir->set_xattr_strategy(xattr_strategy.get());
    root_dir->set_path_strategy(path_strategy.get());
    root_dir->set_reserved_names(&reserved_names);

    int ret = root_dir->open(env->dpp);
    if (ret < 0) {
      ldpp_dout(env->dpp, 0) << " ERROR: could not open base path ("
			     << base_path << "): " << cpp_strerror(-ret)
			     << dendl;
      return ret;
    }

    quota_handler = RGWQuotaHandler::generate_handler(env->dpp, this, false);
    bucket_cache.reset(new nsfs::BucketCache(
        this, base_path, cache_base, 100, 3, 3, 3));

    /* Small, like the bucket cache above:  the configured defaults are
     * large enough that nothing is ever evicted, and eviction is what
     * runs the stabilize callback.  writeback is the policy that has
     * one, so it is the policy worth testing against. */
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
  TestDriver* dr{nullptr};

public:
  TestUser(TestDriver *_dr, const rgw_user& _u) : StoreUser(_u), dr(_dr) { }
  TestUser(TestDriver *_dr, const RGWUserInfo& _i) : StoreUser(_i), dr(_dr) { }
  TestUser(TestDriver *_dr) : dr(_dr) { }
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
  /* Real, not stubbed.
   *
   * These were `{ return 0; }`, which makes any test of a write
   * through them vacuous:  the import reported a user created, the
   * user table was empty, and nothing failed until an assertion went
   * looking.  They now do what `NSFSUser` does -- the driver's one
   * handle -- so a test can ask whether a user was really written. */
  virtual int load_user(const DoutPrefixProvider* dpp,
			optional_yield y) override {
    return dr ? dr->get_user_db()->get_user(dpp, "user_id", get_id().id,
					    get_info(), &get_attrs(),
					    &get_version_tracker())
	      : 0;
  }
  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool
			 exclusive, RGWUserInfo* old_info = nullptr) override {
    return dr ? dr->get_user_db()->store_user(dpp, get_info(), exclusive,
					      &get_attrs(),
					      &get_version_tracker(),
					      old_info)
	      : 0;
  }
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

    /* Probe the filesystem, which on a developer machine always answers
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
  bool bucket_exists;
  rgw::sal::Bucket::CreateParams createparams;

  RGWBucketInfo info;
  info.bucket.name = testname;
  info.owner = owner;
  info.creation_time = ceph::real_clock::now();
  bucket = driver->get_bucket(info);
  EXPECT_NE(bucket.get(), nullptr);

  createparams.owner = owner;

  int ret = bucket->create(env->dpp, createparams, null_yield);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(bucket->get_name(), testname);
  EXPECT_EQ(bucket->get_key().name, testname);
  EXPECT_EQ(bucket->get_key().tenant, "");
  /* create() generates marker and bucket_id as `<name>.<random>` when
   * the caller supplies no marker, and sets the id from the marker.
   * The posix driver does the same, and its own test carries the same
   * stale expectation of "". */
  EXPECT_EQ(bucket->get_key().bucket_id, bucket->get_info().bucket.marker);
  EXPECT_THAT(bucket->get_key().bucket_id, ::testing::StartsWith(testname + "."));
  EXPECT_FALSE(bucket_exists);

  sf::path tp{bp / "root" / testname};
  EXPECT_TRUE(sf::exists(tp));
  EXPECT_TRUE(sf::is_directory(tp));
}

class NSFSBucketTest : public NSFSDriverTest {
protected:
  std::unique_ptr<rgw::sal::Bucket> bucket;

public:
  NSFSBucketTest() {}

  sf::path bucket_path() const { return bp / "root" / testname; }

  static mode_t file_mode(const sf::path& p) {
    struct stat st{};
    if (::stat(p.c_str(), &st) != 0) {
      return 0;
    }
    return st.st_mode & 07777;
  }

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
    int ret = bucket->create(env->dpp, createparams, null_yield);
    EXPECT_EQ(ret, 0);
  }

  void TearDown() {
    NSFSDriverTest::TearDown();
  }
};

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
  static constexpr const char* key = "user.nsfs.bucket_info";

  /* the control:  as created, it loads */
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0)
      << "the bucket does not load before anything was broken";

  /* present and undecodable */
  const std::string junk{"not an encoded RGWBucketInfo"};
  ASSERT_EQ(::setxattr(bpath.c_str(), key, junk.data(), junk.size(), 0), 0);
  EXPECT_EQ(bucket->load_bucket(env->dpp, null_yield), -EBADMSG);

  /* absent, which is a different answer */
  ASSERT_EQ(::removexattr(bpath.c_str(), key), 0);
  EXPECT_EQ(bucket->load_bucket(env->dpp, null_yield), 0)
      << "a bucket with no stored state must still serve";
}

/* A bucket that fails closed is one bucket.
 *
 * ListBuckets skips it rather than denying the account every other
 * bucket, and the profile endpoint still answers -- a profile comes
 * from the marker, and an operator diagnosing the failure needs that
 * endpoint precisely then. */
TEST_F(NSFSBucketTest, UnreadableStateDoesNotDenyTheListingOrTheProfile)
{
  const sf::path bpath{bp / "root" / testname};
  const std::string junk{"not an encoded RGWBucketInfo"};
  ASSERT_EQ(::setxattr(bpath.c_str(), "user.nsfs.bucket_info",
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
    id.noobaa_id = "6a2bdeabf5c8e167f92cb079";
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
  EXPECT_EQ(got.noobaa_id, "6a2bdeabf5c8e167f92cb079");
  EXPECT_TRUE(got.distinguished_name.empty());
}

/* A directory-backed identity is the other arm:  a name and no ids. */
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
 * which is the difference the directory arm will turn on. */
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
  EXPECT_EQ(got.noobaa_id, "6a2bdeabf5c8e167f92cb079");
}

/* The exclusivity rule is enforced by the schema, not by a caller.
 *
 * NooBaa's own schema will not validate a record holding both arms,
 * so neither may this one -- otherwise an importer could produce a
 * record their tooling would reject. */
TEST_F(NSFSIdentityDBTest, BothIdentityArmsAreRefused)
{
  /* the control:  each arm alone is accepted */
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
      << "a refused record was stored anyway";
}

/* A uid without a gid is half an identity and is refused. */
TEST_F(NSFSIdentityDBTest, AUidWithoutAGidIsRefused)
{
  nsfs::Identity id;
  id.key = "user$halfway";
  id.uid = 9;
  EXPECT_EQ(db->put_identity(env->dpp, id), -EINVAL);
}

/* The re-import lookup, which is why the NooBaa id is indexed. */
TEST_F(NSFSIdentityDBTest, LookupByNoobaaId)
{
  ASSERT_EQ(db->put_identity(env->dpp, local_identity("user$erin")), 0);

  nsfs::Identity got;
  ASSERT_EQ(db->get_identity_by_noobaa_id(
	      env->dpp, "6a2bdeabf5c8e167f92cb079", got), 0);
  EXPECT_EQ(got.key, "user$erin");

  EXPECT_EQ(db->get_identity_by_noobaa_id(env->dpp, "nosuchid", got), -ENOENT);
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
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred), 0);
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
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred), 0);
  EXPECT_EQ(cred.uid, 1001u);
}

/* The sharpest case.  An absent group list means no supplementary
 * groups, installed, not "leave whatever the process holds" -- the
 * reading that grants access rather than denying it.
 *
 * The control is the process's own group set:  unless it is
 * non-empty, an empty answer here is indistinguishable from having
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
				      parse_owner(id.key), cred), 0);
  EXPECT_TRUE(cred.groups.empty())
      << "an absent list resolved to " << cred.groups.size() << " groups";
  for (auto g : mine) {
    EXPECT_EQ(std::find(cred.groups.begin(), cred.groups.end(), g),
	      cred.groups.end())
	<< "the process's own group " << g << " reached the credentials";
  }
}

/* An explicitly empty list resolves the same way.  The two stay
 * distinct in the record for the directory arm;  they are not
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
				      parse_owner(id.key), cred), 0);
  EXPECT_TRUE(cred.groups.empty());
}

/* No row is not an error:  it means nothing asked for impersonation,
 * and the caller does what it did before this existed. */
TEST_F(NSFSIdentityDBTest, ResolveNoRowIsNotAnError)
{
  nsfs::Credentials cred;
  cred.uid = 4242;			/* must not be written to */
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db,
				      parse_owner("user$absent"), cred),
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
				      parse_owner(id.key), cred), -ENOENT);
}

/* A directory-backed row asks for an impersonation whose uid and gid
 * live in the directory.  Serving it unimpersonated would run the
 * request as the daemon, which holds more access than the user, so
 * it fails closed.
 *
 * The control is the same key carrying the local arm, which must
 * resolve -- without it a resolver that refused everything would
 * pass. */
TEST_F(NSFSIdentityDBTest, ResolveDirectoryBackedFailsClosed)
{
  const rgw_owner owner = parse_owner("user$fromdirectory");
  nsfs::Credentials cred;

  nsfs::Identity local;
  local.key = to_string(owner);
  local.uid = 77; local.gid = 77;
  ASSERT_EQ(db->put_identity(env->dpp, local), 0);
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred), 0);
  ASSERT_EQ(cred.uid, 77u);

  nsfs::Identity dir;
  dir.key = to_string(owner);
  dir.distinguished_name = "uid=someone,ou=people,dc=example,dc=com";
  ASSERT_EQ(db->put_identity(env->dpp, dir), 0);
  EXPECT_EQ(nsfs::resolve_credentials(env->dpp, *db, owner, cred), -EPERM);
}

/* The upstream behaviour this design keys on, pinned.
 *
 * Two different answers come out of one applier.  `get_aclowner()`
 * says who a created object belongs to, and for a member of an
 * account that is the *account*;  `load_acct_info()` -- what becomes
 * `s->user` -- says who authenticated, and that is the *member*.
 * get_credentials() uses the second.
 *
 * This is not our code, so it is exactly the kind of thing that can
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

/* Two members of one account resolve to different credentials.
 *
 * This is the property that cannot be expressed through
 * get_aclowner() at all:  both members share an ACL owner, so keyed
 * on it they would be indistinguishable. */
TEST_F(NSFSIdentityDBTest, AccountMembersResolveIndependently)
{
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
  ASSERT_EQ(nsfs::resolve_credentials(
	      env->dpp, *db, ia->load_acct_info(env->dpp)->get_id(), ca), 0);
  ASSERT_EQ(nsfs::resolve_credentials(
	      env->dpp, *db, ib->load_acct_info(env->dpp)->get_id(), cb), 0);

  EXPECT_EQ(ca.uid, 1001u);
  EXPECT_EQ(cb.uid, 1002u);
  EXPECT_NE(ca.uid, 9999u) << "a member was served as its account";
  EXPECT_NE(cb.uid, 9999u) << "a member was served as its account";
  EXPECT_EQ(ca.groups, (std::vector<gid_t>{50}));
  EXPECT_TRUE(cb.groups.empty());
}

/* A member with no row of its own does not fall back to its
 * account's.  Inheriting would hand a user the account root's uid,
 * which is a grant, not a default. */
TEST_F(NSFSIdentityDBTest, AMemberDoesNotInheritItsAccountsRow)
{
  const char* acct = "RGW12345678901234567";

  nsfs::Identity acct_row;
  acct_row.key = acct;
  acct_row.uid = 9999; acct_row.gid = 9999;
  ASSERT_EQ(db->put_identity(env->dpp, acct_row), 0);

  /* the control:  the account's own row does resolve */
  nsfs::Credentials cred;
  ASSERT_EQ(nsfs::resolve_credentials(env->dpp, *db,
				      parse_owner(acct), cred), 0);
  ASSERT_EQ(cred.uid, 9999u);

  auto member = local_applier(rgw_user{"", "carol"}, acct);
  EXPECT_EQ(nsfs::resolve_credentials(
	      env->dpp, *db,
	      member->load_acct_info(env->dpp)->get_id(), cred),
	    -ENOENT)
      << "a member with no row was served as its account";
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
  int fail_with{0};			/* non-zero: refuse with this */
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
  uint64_t generation(const rgw_user& key) const override {
    auto it = gens.find(key.id);
    return it == gens.end() ? 0 : it->second;
  }
  void bump(const rgw_user& key) { ++gens[key.id]; }
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
 * under it.
 *
 * Dropping a pinned slot would unregister a personality an in-flight
 * submission still names.  The old credentials are served for the
 * life of that operation and the slot goes on the next lookup. */
TEST_F(NSFSPersonalityTest, AStaleSlotInUseIsNotPulled)
{
  ManualGenerations gens;
  nsfs::PersonalityTable t{4, &reg, &gens};
  const rgw_user alice{"", "alice"};

  nsfs::PersonalityRef held;
  ASSERT_EQ(t.insert(env->dpp, alice, creds_for(1001), &held), 0);
  gens.bump(alice);

  nsfs::PersonalityRef r;
  EXPECT_EQ(t.find(alice, &r), 0)
      << "a pinned stale slot was dropped under a live operation";
  EXPECT_TRUE(reg.released.empty())
      << "a personality still in use was unregistered";
  r.reset();
  held.reset();

  /* released, so now it goes */
  nsfs::PersonalityRef after;
  EXPECT_EQ(t.find(alice, &after), -ENOENT)
      << "the stale slot survived its last pin";
  EXPECT_EQ(reg.released.size(), 1u);
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

/* Every slot pinned is a denial, not an improvisation.
 *
 * Serving the request unimpersonated would give it the gateway's
 * reach;  evicting a pinned slot would pull credentials out from
 * under a live operation.  Neither is acceptable, so it fails. */
TEST_F(NSFSPersonalityTest, AllSlotsPinnedIsRefused)
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
      << "a refused insert registered a personality anyway";

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

/* A registrar that refuses leaves nothing behind. */
TEST_F(NSFSPersonalityTest, AFailedRegistrationStoresNothing)
{
  nsfs::PersonalityTable t{4, &reg};
  const rgw_user u{"", "nope"};

  /* the control:  it works before the registrar starts refusing */
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
      env->dpp, null_yield, object.get(), acl_owner, nullptr, 0, testname);
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(object->get_read_op());

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

  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = object->get_delete_op();
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();
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

  /* attr1 and the synthesized etag.  object_type went in 4cd543aa56c,
   * and there is no `owner` attribute:  ownership is read from the ACL,
   * which XattrStrategy::object_owner() takes from RGW_ATTR_ACL. */
  EXPECT_EQ(object->get_attrs().size(), 2);
  EXPECT_EQ(object->get_attrs()[ATTR1], origbl);
  EXPECT_TRUE(object->get_attrs().contains(RGW_ATTR_ETAG));
  EXPECT_FALSE(object->get_attrs().contains(ATTR_OBJECT_TYPE));
  EXPECT_FALSE(object->get_attrs().contains("owner"));
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

  /* neither is written any more -- see NSFSObjectTest.ObjectAttrs.
   * Asserted absent rather than dropped, because their reappearance
   * would mean something started writing them again. */
  EXPECT_FALSE(xattr_names.contains("user.nsfs.object_type"));
  EXPECT_FALSE(xattr_names.contains("user.nsfs.owner"));

  // user-supplied attrs use user.nsfs.* prefix
  EXPECT_TRUE(xattr_names.contains("user.nsfs." + ATTR1));

  // RGW common attrs use user.nsfs.rgw.* prefix (etag present only if set)
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
      env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);

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
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op());

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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op());
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
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op());
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
    bucket->get_multipart_upload(objname, upload_id);
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
                         &placement, i, part_name);
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
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(mp_obj->get_read_op());
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
    bucket->get_multipart_upload(objname, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  int ret = upload->init(env->dpp, null_yield, acl_owner, placement, attrs);
  ASSERT_EQ(ret, 0);

  // Write one part
  std::string part_name = "part-00001";
  std::unique_ptr<rgw::sal::Writer> writer =
    upload->get_writer(env->dpp, null_yield, nullptr, acl_owner,
                       &placement, 1, part_name);
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
  ASSERT_EQ(b->remove(env->dpp, false, null_yield), 0);
  EXPECT_FALSE(sf::exists(d));
}

/* Deleting a bucket adopted over a directory that predates it leaves
 * the directory, and its contents, alone.
 *
 * The directory may be a fileset junction or anything else an
 * administrator made and pointed the gateway at;  removing it is the
 * one unrecoverable thing this path can do.  What goes instead is
 * everything the gateway wrote -- the whole user.nsfs. namespace --
 * which returns the directory to the state it was in before it was
 * adopted. */
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
  ASSERT_EQ(b->remove(env->dpp, false, null_yield), 0);

  EXPECT_TRUE(sf::is_directory(d))
      << "removed a directory the bucket was only lent";
  EXPECT_TRUE(sf::is_regular_file(d / "predates.txt"))
      << "removed contents the bucket did not put there";

  char buf[8192];
  const ssize_t len = ::listxattr(d.c_str(), buf, sizeof(buf));
  ASSERT_GE(len, 0);
  for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
    EXPECT_FALSE(std::string_view(q).starts_with("user.nsfs."))
	<< q << " survived the delete";
  }
}

TEST_F(NSFSBucketTest, DirectoryObjectPut)
{
  auto obj = bucket->get_object(rgw_obj_key("photos/"));
  auto writer = driver->get_atomic_writer(
      env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
      env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op(robj->get_read_op());
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
      env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
  auto del_op = dobj->get_delete_op();
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
      acl_owner, rgw_user(), nullptr, rgw_zone_id(),
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
        env->dpp, null_yield, obj.get(), acl_owner, nullptr, 0, testname);
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
  int ret = obj->chown(user, env->dpp, null_yield);
  // chown to uid/gid 0 requires root; EPERM is expected for non-root
  EXPECT_TRUE(ret == 0 || ret == -EPERM);
}

/* NooBaa's multipart staging layout.
 *
 * These assert on-disk names, offsets and sizes directly, because the
 * layout is somebody else's and this is where we state what we believe
 * it to be.  Read from noobaa-core 68ca22d33, `namespace_fs.js`.
 *
 * A fixture written by our own emulator only proves we read what we
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
   * NooBaaMPUStrategy recognises their temp directory by the
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

  /* their directory is the upload id alone;  the key lives inside */
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
  nsfs::StridedMPUStrategy ours;

  EXPECT_EQ(nb.shared_name(1048576), ours.shared_name(1048576));

  /* what does distinguish them */
  EXPECT_NE(nb.meta_name(), ours.meta_name());
  EXPECT_NE(nb.assembled_name(), ours.assembled_name());
  EXPECT_NE(nb.part_name(1), ours.part_name(1));
}

/* Their offset is the part's own size times (n - 1), not an upload-wide
 * stride (`namespace_fs.js` upload_multipart). */
TEST(NooBaaMPU, PartTargetIsKeyedByTheSize)
{
  nsfs::NooBaaMPUStrategy nb;

  auto t1 = nb.part_target(1, 64);
  ASSERT_TRUE(t1.has_value());
  EXPECT_EQ(t1->name, "parts-size-64");
  EXPECT_EQ(t1->offset, 0u);
  EXPECT_EQ(t1->extent, std::optional<uint64_t>{64});
  EXPECT_TRUE(t1->shared);

  auto t3 = nb.part_target(3, 64);
  ASSERT_TRUE(t3.has_value());
  EXPECT_EQ(t3->offset, 128u);

  /* no size known yet:  the data goes to the part's own file, which for
   * them is the record, and is copied into a size file afterwards */
  auto t0 = nb.part_target(2, std::nullopt);
  ASSERT_TRUE(t0.has_value());
  EXPECT_EQ(t0->name, "part-2");
  EXPECT_EQ(t0->offset, 0u);
  EXPECT_FALSE(t0->shared);
  EXPECT_EQ(t0->extent, std::nullopt);
}

/* Staging is found by what the directory holds, not by its name.
 *
 * `config.NSFS_TEMP_DIR_NAME` is a default in their config.js, so the
 * `.noobaa-nsfs` prefix is not something we may depend on.  A
 * directory holding `multipart-uploads/` IS the temp directory
 * whatever it is called, and that settles every tree the read path
 * cares about -- a tree with no uploads has nothing to find.
 *
 * `abc123` here is deliberately neither their default prefix nor a
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
   * Content beats shape:  the one holding uploads is still the
   * answer, because a directory that holds uploads cannot be the
   * wrong one. */
  sf::create_directories(base_path / test /
			 (".noobaa-nsfs_" + fake_uuid("quiescent")));
  r = nb.staging_root(env->dpp, bfd);
  ASSERT_TRUE(r.has_value()) << "a quiescent neighbour hid the real one";
  EXPECT_EQ(*r, ".noobaa-nsfs_abc123/multipart-uploads");

  /* But two directories BOTH holding uploads is undecidable, and it
   * refuses rather than picking. */
  NBStaging other(test, "def456", "u2");
  ASSERT_GE(other.fd, 0);
  EXPECT_EQ(nb.staging_root(env->dpp, bfd), std::nullopt);
  ::close(bfd);
}

/* The shape test, on its own.
 *
 * It is what finds their temp directory on a tree that has never
 * taken an upload -- there is nothing inside it to recognise then, so
 * the name is all there is, and only the structural part of the name
 * is theirs to keep:  `_` and the bucket id, which is a
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

  /* their default prefix, with their id shape */
  sf::create_directories(b / (".noobaa-nsfs_" + fake_uuid("one")));
  int bfd = ::open(b.c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(bfd, 0);

  /* no uploads, so the read side has nothing to report */
  EXPECT_EQ(nb.staging_root(env->dpp, bfd), std::nullopt);

  /* but the write side finds it and creates multipart-uploads/ inside
   * it rather than inventing a second temp directory */
  auto w = nb.staging_root_for_write(env->dpp, bfd);
  ASSERT_TRUE(w.has_value());
  EXPECT_EQ(*w, ".noobaa-nsfs_" + fake_uuid("one") + "/multipart-uploads");

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
 * theirs on the next write.
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

  /* and the write side uses it rather than creating one of ours */
  auto w = nb.staging_root_for_write(env->dpp, bfd);
  ASSERT_TRUE(w.has_value());
  EXPECT_EQ(*w, renamed + "/multipart-uploads");
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

/* Uniform parts:  the size file already is the object, so completing
 * links it and copies nothing. */
TEST(NooBaaMPU, AssembleUniformLinks)
{
  nsfs::NooBaaMPUStrategy nb;
  nsfs::POSIXStrategy fs;
  const std::string test = get_test_name();
  NBStaging st(test, "b", "u");
  ASSERT_GE(st.fd, 0);

  const uint64_t size = 16;
  st.write_uniform(size, 1, 3);

  std::vector<nsfs::MPUStrategy::PartPlacement> parts{
    {1, true, 0, size}, {2, true, size, size}, {3, true, 2 * size, size}};

  ASSERT_EQ(nb.assemble(env->dpp, &fs, st.fd, parts, "final", size), 0);

  const std::string out = read_all(st.upload / "final");
  EXPECT_EQ(out, std::string(size, 'b') + std::string(size, 'c') +
		 std::string(size, 'd'));
  /* linked, not copied */
  EXPECT_EQ(sf::hard_link_count(st.upload / "final"), 2u);
}

/* A short final part is the case their scheme cannot place:  it lives
 * in a file of its own size, so completing links the body and copies
 * the tail. */
TEST(NooBaaMPU, AssembleShortTailCopiesTheTail)
{
  nsfs::NooBaaMPUStrategy nb;
  nsfs::POSIXStrategy fs;
  const std::string test = get_test_name();
  NBStaging st(test, "b", "u");
  ASSERT_GE(st.fd, 0);

  const uint64_t size = 16, tail = 5;
  st.write_uniform(size, 1, 2);
  st.write_uniform(tail, 3, 1);

  std::vector<nsfs::MPUStrategy::PartPlacement> parts{
    {1, true, 0, size}, {2, true, size, size}, {3, true, 2 * tail, tail}};

  ASSERT_EQ(nb.assemble(env->dpp, &fs, st.fd, parts, "final", size), 0);

  const std::string out = read_all(st.upload / "final");
  EXPECT_EQ(out, std::string(size, 'b') + std::string(size, 'c') +
		 std::string(tail, 'd'));
}

/* A gap in the part numbers disables both fast paths, as it does for
 * them:  the file for a size no longer holds a contiguous prefix. */
TEST(NooBaaMPU, AssembleSparseNumberingCopies)
{
  nsfs::NooBaaMPUStrategy nb;
  nsfs::POSIXStrategy fs;
  const std::string test = get_test_name();
  NBStaging st(test, "b", "u");
  ASSERT_GE(st.fd, 0);

  const uint64_t size = 16;
  st.write_uniform(size, 1, 1);
  st.write_uniform(size, 5, 1);

  std::vector<nsfs::MPUStrategy::PartPlacement> parts{
    {1, true, 0, size}, {5, true, 4 * size, size}};

  ASSERT_EQ(nb.assemble(env->dpp, &fs, st.fd, parts, "final", size), 0);

  const std::string out = read_all(st.upload / "final");
  EXPECT_EQ(out, std::string(size, 'b') + std::string(size, 'f'));
  EXPECT_EQ(sf::hard_link_count(st.upload / "final"), 1u);
}

/* NooBaa's attribute names.
 *
 * Spelled out here rather than derived from the strategy, for the same
 * reason the tree fixtures are:  a test which asked the strategy what
 * it calls things would agree with it by construction.  These are read
 * from noobaa-core 68ca22d33, the constant block at the head of
 * `namespace_fs.js` and `to_fs_xattr()`.
 */
TEST(NooBaaXattr, NamesTheirAttributes)
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

/* The contrast with OwnerComesFromTheInode below, and the reason
 * adopting a tree of unattributed files leaves the bucket base.
 *
 * PrefixedXattrStrategy -- what shared and strong both resolve to --
 * answers from the recorded ACL and never consults the statx.  A file
 * written by cp or over NFS has no ACL, so a marked profile reports no
 * owner for it however the statx is passed, while base reports the
 * uid.  See docs/NSFS_ADOPTION.md 5. */
TEST(PrefixedXattr, OwnerDoesNotComeFromTheStatx)
{
  nsfs::PrefixedXattrStrategy ours;
  const rgw::sal::Attrs none;

  struct statx stx{};
  stx.stx_mask = STATX_UID;
  stx.stx_uid = 4242;

  ACLOwner owner;
  EXPECT_EQ(ours.object_owner(none, &stx, owner), -EINVAL)
      << "a marked profile reported an owner for a file with no ACL";
  EXPECT_EQ(ours.object_owner(none, nullptr, owner), -EINVAL);
}

/* Three answers, and the third is the one worth pinning.  An ACL has
 * nowhere to go, so the name is empty and the write path skips it --
 * which is what their own gateway does.  Ours with no counterpart
 * lands under user.nsfs., inert to them. */
TEST(NooBaaXattr, WhatHasNowhereToGo)
{
  nsfs::NooBaaXattrStrategy nb;

  EXPECT_TRUE(nb.disk_name(RGW_ATTR_ACL).empty());

  EXPECT_EQ(nb.disk_name("bucket_info"), "user.nsfs.bucket_info");
  EXPECT_EQ(nb.disk_name("rename_intent"), "user.nsfs.rename_intent");

  /* and not the wrong third answer:  never the key unchanged, which
   * would write under a name neither format owns */
  EXPECT_NE(nb.disk_name("rename_intent"), "rename_intent");
}

/* Their structured attributes are claimed as unmapped rather than
 * surfaced.  Handing a caller one of their plain strings under an RGW
 * key would feed a NooBaa value to a ceph decoder. */
TEST(NooBaaXattr, LeavesTheirStructuredAttributesAlone)
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

/* Ownership is the inode's.  They record no owner, so a file a native
 * client created has a uid and nothing else -- which is the case that
 * currently lists as unknown/unknown. */
TEST(NooBaaXattr, OwnerComesFromTheInode)
{
  nsfs::NooBaaXattrStrategy nb;
  Attrs none;

  struct statx stx{};
  stx.stx_mask = STATX_UID;
  stx.stx_uid = 4242;
  ACLOwner owner;
  ASSERT_EQ(nb.object_owner(none, &stx, owner), 0);
  EXPECT_EQ(owner.id, rgw_owner(rgw_user("4242")));

  /* without a stat there is no answer, and saying so is the contract */
  ACLOwner none_owner;
  EXPECT_LT(nb.object_owner(none, nullptr, none_owner), 0);

  struct statx no_uid{};
  no_uid.stx_mask = STATX_MTIME;
  EXPECT_LT(nb.object_owner(none, &no_uid, none_owner), 0);
}

/* Never counted.  The trailing NUL is an RGW-side accident our own
 * writers append;  nothing in their tree carries one. */
TEST(NooBaaXattr, ValuesAreNotCountedStrings)
{
  nsfs::NooBaaXattrStrategy nb;
  nsfs::PrefixedXattrStrategy ours;

  EXPECT_FALSE(nb.counted_string_value(
      std::string(RGW_ATTR_META_PREFIX) + "colour"));
  EXPECT_FALSE(nb.counted_string_value(RGW_ATTR_CONTENT_TYPE));

  /* and ours does count at least one, so this is a difference rather
   * than a predicate nobody implements */
  EXPECT_TRUE(ours.counted_string_value(
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

/* Ours.  There is no counterpart to `quiescent`:  we keep no bucket
 * temp directory, so a bucket which has been written to and has no
 * upload is indistinguishable from one which has not.  The shape is
 * accepted and does nothing, which is the difference stated rather
 * than hidden. */
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
    auto t = nb.part_target(k, tree.part_size);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->offset, (k - 1) * tree.part_size);
    EXPECT_TRUE(sf::exists(tree.staging / t->name)) << t->name;
  }

  /* and ours misreads nothing:  their directory is not one of ours,
   * their record names are not ours, and asked directly it reports no
   * upload rather than a wrong one */
  nsfs::StridedMPUStrategy ours;
  EXPECT_FALSE(ours.is_staging_dir(tree.upload_id));
  EXPECT_FALSE(sf::exists(tree.staging / ours.meta_name()));
  EXPECT_FALSE(sf::exists(tree.staging / ours.part_name(1)));

  int ofd = ::open((b / *root).c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(ofd, 0);
  nsfs::MPUStrategy::StagedUpload none;
  EXPECT_FALSE(ours.staged_upload(env->dpp, ofd, tree.upload_id, none));
  ::close(ofd);
}

/* NooBaa's path conventions.
 *
 * Most of them are ours -- keys verbatim, `.folder`, `.versions` --
 * and asserting the sameness is the point:  it records that the
 * agreement is deliberate rather than a coincidence nobody may rely
 * on.  What differs is asserted against a tree written by hand.
 */
TEST(NooBaaPath, KeyConventionsMatchOurs)
{
  nsfs::NooBaaPathStrategy nb;
  nsfs::SentinelPathStrategy ours;

  for (const char* k : {"plain", "some/deep/key.bin", "_leading_underscore",
			"__doubled", "trailing/"}) {
    rgw_obj_key key{k};
    EXPECT_EQ(nb.object_name(key, false), ours.object_name(key, false)) << k;
  }

  /* The two formats' directory-object file names are equal, and since
   * they stopped being one constant that is a comparison rather than
   * a tautology.  They are equal by inheritance -- `61de07d3bf2` took
   * `.folder` from NooBaa, and posix has no such name -- so this
   * asserts an agreement that either side may end, not an invariant.
   * If it ever fails, the question to ask is which side moved and
   * whether the other should. */
  EXPECT_EQ(nb.folder_object_name(), ours.folder_object_name());
  EXPECT_EQ(nb.folder_object_name(), ".folder");
  EXPECT_EQ(nb.object_name(rgw_obj_key{"photos/"}, false), "photos/.folder");
  EXPECT_TRUE(nb.names_directory_object(".folder"));
  EXPECT_TRUE(ours.names_directory_object(".folder"));
  EXPECT_EQ(nb.key_from_name("some/key").name, "some/key");
}

/* Their staging is under the bucket temp directory, so nothing beside
 * the bucket is named for an upload and a namespace has nothing to
 * render.  Ours spells one, which is the difference. */
TEST(NooBaaPath, NoStagingSpelling)
{
  nsfs::NooBaaPathStrategy nb;
  nsfs::SentinelPathStrategy ours;
  std::optional<std::string> mp{"multipart"};

  EXPECT_EQ(nb.bucket_dir_name("b", mp), "b");
  EXPECT_EQ(ours.bucket_dir_name("b", mp), ".multipart_b");
  EXPECT_EQ(nb.bucket_dir_name("b", std::nullopt), "b");
}

/* No .shadow -- they have none, and claiming the name would hide a
 * directory a user may create -- and the temp directory reserved by
 * prefix, since its name carries a bucket id. */
TEST(NooBaaPath, ReservesTheirNamesAndNotOurs)
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

  /* and ours does claim .shadow, so this is a difference rather than
   * an empty set */
  nsfs::SentinelPathStrategy ours;
  const auto& orn = ours.reserved_names();
  EXPECT_NE(std::find(orn.exact.begin(), orn.exact.end(), ".shadow"),
	    orn.exact.end());
}

/* A directory object of theirs, which is the interop case:  their
 * empty one has no sentinel at all, so nothing in the directory
 * listing reveals it and the question has to be asked of the
 * directory. */
TEST(NooBaaPath, DirectoryObjectFromTheAttribute)
{
  nsfs::NooBaaPathStrategy nb;
  nsfs::SentinelPathStrategy ours;
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
  /* ours never marks a directory, and says so without looking */
  nsfs::PathStrategy::DirectoryObject od{};
  EXPECT_FALSE(ours.directory_object(env->dpp, fd, od));
  ::close(fd);

  d = {};
  fd = dfd("withcontent");
  ASSERT_TRUE(nb.directory_object(env->dpp, fd, d));
  EXPECT_EQ(d.size, 17u);
  EXPECT_TRUE(d.content_in_sentinel);
  ::close(fd);

  /* a directory without the attribute is not an object;  their own
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
 * NooBaa's staging with our attribute names is a combination no tree
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

  /* the members it does not override still answer */
  EXPECT_EQ(b->xattr_strategy(), driver->get_xattr_strategy());
}

/* The strided staging layout, on a filesystem that shares extents.
 *
 * The driver chooses strided only where assembling from per-part files
 * would copy the whole object, which is GPFS and nothing a developer
 * machine mounts.  So the layout, the extent guard and the divert path
 * have had no test outside a Storage Scale run.  These force the
 * answer instead of measuring it.
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
 * must not -- an override stuck at one answer would otherwise pass. */
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
		       placement, num, std::to_string(num));
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
  auto upload = bucket->get_multipart_upload(objname, upload_id);
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
  auto upload = bucket->get_multipart_upload(objname, upload_id);
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
  auto upload = bucket->get_multipart_upload(objname, upload_id);
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
      objname, "11111111-2222-4333-8444-555555555555");
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

/* Ours does not move a short part.
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
  auto upload = bucket->get_multipart_upload(objname, upload_id);
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
    auto other = bucket->get_multipart_upload("other.bin", "beef42");
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

/* A bucket in NooBaa's format, served by the driver.
 *
 * The bucket is created marked -- every bucket this gateway makes is
 * -- and then reduced to base while it is still empty, which is the
 * only transition the profile rules allow into their format.  From
 * there every strategy the bucket uses is theirs. */
class NSFSNooBaaBucketTest : public NSFSBucketTest {
public:
  void SetUp() override {
    NSFSBucketTest::SetUp();
    auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
    ASSERT_EQ(b->resolve_profile(env->dpp), 0);
    ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_BASE), 0);
    ASSERT_STREQ(b->profile_name(), "base");
    ASSERT_STREQ(b->get_format()->name(), "noobaa");
  }

  sf::path bucket_path() const { return bp / "root" / testname; }
};

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
  }

  void TearDown() override {
    NSFSNooBaaBucketTest::TearDown();
    g_conf().set_val("rgw_nsfs_noobaa_config_root", "");
    g_conf().apply_changes(nullptr);
  }

  /* A tree we did not write.  The fixture creates its bucket through
   * the SAL, which is the only way to get a Bucket, and that writes our
   * attribute;  a bucket NooBaa made carries no such thing. */
  void remove_our_bucket_info() {
    ASSERT_EQ(::removexattr((bp / "root" / testname).c_str(),
			    "user.nsfs.bucket_info"), 0);
  }

  /* `path` names the bucket's real directory.
   *
   * It used to be `/ibm/gpfs/noobaadata/<name>`, which does not
   * exist -- harmless while nothing read `path`, and wrong once P5
   * resolves a base bucket from it.  A real NooBaa record names where
   * the storage actually is, so the fixture does too. */
  void write_their_record(const char* versioning,
		    const std::string& extra = std::string{},
		    const char* created = "2026-01-02T03:04:05.000Z",
		    const std::string& at = std::string{}) {
    std::string doc = std::string(
      "{\"_id\":\"6560e1f1c0ffee0000000001\",\"name\":\"") + testname +
      "\",\"owner_account\":\"6560e1f1c0ffee0000000002\""
      ",\"versioning\":\"" + versioning + "\""
      ",\"path\":\"" + (at.empty() ? bucket_path().string() : at) + "\""
      ",\"should_create_underlying_storage\":true"
      ",\"creation_date\":\"" + created + "\"" +
      extra + "}";
    write_file(conf_root() / "buckets" / (testname + ".json"), doc);
  }

  /* Read the record write_their_record() wrote, through the strategy
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
  int read_their_record(RGWBucketInfo& info, Attrs& attrs) {
    /* Cleared first, so a test which reads twice does not see the
     * first read's keys in the second result.  load() adds what the
     * record carries and removes nothing. */
    info = RGWBucketInfo{};
    attrs.clear();

    auto* theirs = driver->get_noobaa_bucket_state_strategy();
    if (! theirs) {
      return -ENOENT;
    }
    return theirs->load(env->dpp, -1, testname, attrs, info);
  }
};

/* X1.  Their per-bucket force_md5_etag is carried into the attribute
 * map, in their shape, with the tri-state intact.
 *
 * Carried and **not enforced**:  `rgw_non_md5_etag` is the single
 * deployment-wide control and their global maps to it
 * (`ACCOUNT_IMPORT.md` §6.1a).  What this asserts is that the value
 * survives -- into an attribute rather than only into their record,
 * because once a bucket is adopted nothing reads their record again.
 *
 * All three states, because their precedence is a cascade and `false`
 * counts as defined:  a bucket that turns it off must not arrive
 * looking like a bucket that said nothing. */
TEST_F(NSFSNooBaaStateTest, ForceMd5EtagIsCarriedInTheirShape)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("DISABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  EXPECT_EQ(attrs.find(RGW_NSFS_ATTR_FORCE_MD5_ETAG), attrs.end())
      << "a record that says nothing produced an attribute";

  write_their_record("DISABLED", ",\"force_md5_etag\":true");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  auto on = attrs.find(RGW_NSFS_ATTR_FORCE_MD5_ETAG);
  ASSERT_NE(on, attrs.end()) << "their true was dropped";
  EXPECT_EQ(on->second.to_str(), "true");

  write_their_record("DISABLED", ",\"force_md5_etag\":false");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  auto off = attrs.find(RGW_NSFS_ATTR_FORCE_MD5_ETAG);
  ASSERT_NE(off, attrs.end())
      << "their explicit false read as unset -- the cascade is lost";
  EXPECT_EQ(off->second.to_str(), "false");

  /* and nothing about the gateway's own behaviour changed:  the flag
   * is carried, and no request path consults it */
  EXPECT_FALSE(info.flags & BUCKET_VERSIONED);
}

/* Their versioning reaches RGW, which is what a base bucket loses today.
 *
 * A bucket that reads as unversioned takes a PUT in place instead of
 * demoting the current version into .versions/, so this is the silent
 * data loss and not a missing feature. */
TEST_F(NSFSNooBaaStateTest, VersioningComesFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* Nothing has written a record yet, so the read finds none and says
   * so.  That is a different outcome from a record which says
   * unversioned, and the assertion below depends on the difference. */
  ASSERT_EQ(read_their_record(info, attrs), -ENOENT)
      << "a record was read where this test wrote none";

  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  EXPECT_TRUE(info.versioned());
  EXPECT_TRUE(info.versioning_enabled());
}

/* Converting a base bucket must not drop the state their record was
 * supplying.
 *
 * bucket_state_fallback() returns null the moment the bucket carries
 * an extension bit, so after the conversion their record is no longer
 * read.  set_profile() therefore has to leave our own bucket_info
 * behind -- or the bucket loads from seeded defaults and a versioned
 * bucket comes back unversioned, after which the next PUT overwrites
 * in place instead of demoting into .versions/.
 *
 * The control is the assertion before the conversion:  the bucket is
 * versioned there, and only from their record, so the assertion after
 * it is able to fail. */
TEST_F(NSFSNooBaaStateTest, ConversionKeepsTheVersioningTheirRecordSupplied)
{
  remove_our_bucket_info();
  write_their_record("ENABLED");

  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  ASSERT_TRUE(bucket->get_info().versioned())
      << "not versioned before the conversion;  the premise is gone";

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->set_profile(env->dpp, null_yield, nsfs::EXTENSIONS_SHARED), 0);
  ASSERT_STREQ(b->profile_name(), "shared");

  /* A fresh handle, which is what a request gets.  Re-loading the
   * handle used above would prove nothing:  load_bucket() keeps the
   * info it already holds when nothing on disk answers
   * (`info = bak_info`), so the versioning read from their record the
   * first time would survive in memory whatever the conversion did. */
  RGWBucketInfo fresh_info;
  fresh_info.bucket.name = testname;
  auto fresh = driver->get_bucket(fresh_info);
  ASSERT_NE(fresh.get(), nullptr);
  ASSERT_EQ(fresh->load_bucket(env->dpp, null_yield), 0);

  EXPECT_TRUE(fresh->get_info().versioned())
      << "the conversion dropped the versioning their record supplied;  "
	 "the next PUT overwrites in place instead of demoting";
  EXPECT_TRUE(fresh->get_info().versioning_enabled())
      << "versioned but not enabled after the conversion";
}

/* Suspended is versioned and suspended, not unversioned:  the bucket
 * still holds versions. */
TEST_F(NSFSNooBaaStateTest, SuspendedIsVersionedAndSuspended)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("SUSPENDED");

  ASSERT_EQ(read_their_record(info, attrs), 0);
  EXPECT_TRUE(info.versioned());
  EXPECT_FALSE(info.versioning_enabled());
}

/* Ours wins where it exists.
 *
 * The chain runs the opposite way to the object chain:  a base
 * bucket's objects are in their format, but its state is in ours
 * wherever we have written any. */
TEST_F(NSFSNooBaaStateTest, OurStateWinsOverTheirs)
{
  write_their_record("ENABLED");

  /* our attribute is still there, from create(), and says nothing
   * about versioning */
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  EXPECT_FALSE(bucket->get_info().versioned())
      << "their record was consulted for a bucket that has state of ours";
}

/* Conversion must write down the owner the inode was answering.
 *
 * An object with no recorded ACL answers its owner from the inode
 * while the bucket is unmarked -- their format has nowhere to put an
 * ACL, so `NooBaaXattrStrategy::disk_name(RGW_ATTR_ACL)` is empty and
 * nothing is stored.  Marking the bucket retires that reader and makes
 * the recorded ACL the only source, so without a capture the object
 * comes out of conversion with no owner at all.
 *
 * The control is the sideloaded file:  it has no ACL by construction,
 * which is the case the capture exists for.  Before the capture this
 * test fails on the first assertion after the conversion. */
TEST_F(NSFSNooBaaBucketTest, ConversionCapturesTheInodeOwnerAsAnAcl)
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
  nsfs::PrefixedXattrStrategy ours;
  const std::string disk = ours.disk_name(RGW_ATTR_ACL);
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
      << ";  its owner was answered from the inode and is now lost";

  bufferlist bl;
  bl.append(buf, len);
  RGWAccessControlPolicy policy;
  auto it = bl.cbegin();
  ASSERT_NO_THROW(policy.decode(it));

  EXPECT_EQ(to_string(policy.get_owner().id), std::to_string(st.st_uid))
      << "the captured owner is not the uid the unconverted reader "
	 "would have answered";

  /* RGW's default policy, not a bare owner.  An owner with no grants
   * is more restrictive than the absence it replaced:  with no ACL,
   * authorization falls through to the bucket. */
  EXPECT_FALSE(policy.get_acl().get_grant_map().empty())
      << "the captured ACL grants nobody anything";
}

/* The sentinel carries the mode every object on this backend carries.
 *
 * 0600:  the publish path fchmods to S_IRUSR|S_IWUSR as the owner, so
 * access is decided by ownership and impersonation rather than by the
 * mode, and a converted directory object must not be more permissive
 * than an ordinary one.
 *
 * umask is set for the duration, because `openat` applies it and a
 * developer's umask is not the test's to assume. */
TEST_F(NSFSNooBaaBucketTest, ConversionGivesTheSentinelAnObjectMode)
{
  const mode_t saved = ::umask(022);
  auto restore = make_scope_guard([saved] { ::umask(saved); });

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
  EXPECT_EQ(st.st_mode & 07777, 0600u)
      << "the sentinel does not carry the mode an object written through "
	 "S3 carries;  it must not be more permissive than one";
}

/* A group we belong to that is not our primary one.
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

/* Without the capability, conversion refuses and leaves nothing behind.
 *
 * `CAP_CHOWN` cannot be dropped in a test process -- a thread that
 * drops it cannot regain it, which would poison every later test -- so
 * the condition is injected through the same hint the suite uses.
 *
 * Both halves matter.  Refusing is the point, and so is removing the
 * file it had already created:  a sentinel left behind with the
 * gateway's ownership is the outcome the refusal exists to prevent. */
TEST_F(NSFSNooBaaBucketTest, ConversionRefusesWhenItCannotInheritOwnership)
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
      << "the refusal left a sentinel owned by the gateway, which is the "
	 "state it exists to prevent";

  /* and the other polarity:  with the injection off the same tree
   * converts, so the refusal above was the injection and not some
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
TEST_F(NSFSNooBaaStateTest, ObjectLockComesFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* A record without a lock reads as unlocked, so the assertion after
   * the next one is able to fail. */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  ASSERT_FALSE(info.obj_lock_enabled());

  write_their_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"days":10,)"
	       R"("mode":"GOVERNANCE"}}})");
  ASSERT_EQ(read_their_record(info, attrs), 0);

  EXPECT_TRUE(info.obj_lock_enabled());
  ASSERT_TRUE(info.obj_lock.has_rule());
  EXPECT_EQ(info.obj_lock.get_mode(), "GOVERNANCE");
  EXPECT_EQ(info.obj_lock.get_days(), 10);
  EXPECT_EQ(info.obj_lock.get_years(), 0);
}

/* A retention mode that is neither of theirs fails the bucket closed.
 *
 * Nothing else checks it:  RGW validates the mode on its XML path and
 * not in DefaultRetention::decode_json(), so without this an
 * arbitrary string would be reported as a locked bucket's retention
 * mode. */
TEST_F(NSFSNooBaaStateTest, AnUnknownRetentionModeFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"days":10,)"
	       R"("mode":"ADVISORY"}}})");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* Years rather than days, and the other mode. */
TEST_F(NSFSNooBaaStateTest, ObjectLockInYearsAndCompliance)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"years":7,)"
	       R"("mode":"COMPLIANCE"}}})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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

  write_their_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled"})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
  EXPECT_TRUE(info.obj_lock_enabled());
  EXPECT_FALSE(info.obj_lock.has_rule());
}

/* "Disabled" is their internal state for a bucket that is not locked,
 * not a lock that is switched off.  S3 only ever validates "Enabled"
 * on the wire. */
TEST_F(NSFSNooBaaStateTest, TheirDisabledLockIsNoLock)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Disabled"})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
  EXPECT_FALSE(info.obj_lock_enabled());
}

/* A retention we cannot honour fails the bucket closed rather than
 * being honoured wrongly.  Both S3 and their schema require exactly
 * one of days or years. */
TEST_F(NSFSNooBaaStateTest, AnImpossibleRetentionFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"days":10,"years":7,)"
	       R"("mode":"GOVERNANCE"}}})");
  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG)
      << "a retention naming both days and years was accepted";

  write_their_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"mode":"GOVERNANCE"}}})");
  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG)
      << "a retention naming neither days nor years was accepted";
}

/* Their bucket policy reaches the attribute map, normalised by RGW's
 * own parser the way one set through S3 would be. */
TEST_F(NSFSNooBaaStateTest, BucketPolicyComesFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no policy in their record, none in the attributes */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_IAM_POLICY), attrs.end());

  write_their_record("ENABLED",
	       R"(,"s3_policy":{"Version":"2012-10-17","Statement":[)"
	       R"({"Sid":"one","Effect":"Allow","Principal":{"AWS":"*"},)"
	       R"("Action":["s3:GetObject"],)"
	       R"("Resource":["arn:aws:s3:::)" + std::string(testname) +
	       R"(/*"]}]})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
  auto i = attrs.find(RGW_ATTR_IAM_POLICY);
  ASSERT_NE(i, attrs.end())
      << "their policy did not reach the attribute map";

  /* stored as a document RGW's own parser accepts, which is the thing
   * that matters -- not that the bytes match theirs */
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

  write_their_record("ENABLED",
	       R"(,"s3_policy":{"Version":"2012-10-17",)"
	       R"("Statement":[{"Effect":"Perhaps"}]})");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* Their public access block reaches the attribute map.
 *
 * Four booleans, the same four, snake_case against PascalCase. */
TEST_F(NSFSNooBaaStateTest, PublicAccessBlockComesFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no block in their record, none in the attributes */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_PUBLIC_ACCESS), attrs.end());

  write_their_record("ENABLED",
	       R"(,"public_access_block":{"block_public_acls":true,)"
	       R"("ignore_public_acls":false,"block_public_policy":true,)"
	       R"("restrict_public_buckets":false})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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

  write_their_record("ENABLED",
	       R"(,"public_access_block":{"restrict_public_buckets":true})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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

  write_their_record("ENABLED",
	       R"(,"object_lock_configuration":{)"
	       R"("object_lock_enabled":"Enabled",)"
	       R"("rule":{"default_retention":{"days":"ten",)"
	       R"("mode":"GOVERNANCE"}}})");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* Their website configuration, index-document branch. */
TEST_F(NSFSNooBaaStateTest, WebsiteComesFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* A record with no website configures none, so the assertion after
   * the next one is able to fail. */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  ASSERT_FALSE(info.has_website);

  write_their_record("ENABLED",
	       R"(,"website":{"website_configuration":{)"
	       R"("index_document":{"suffix":"index.html"},)"
	       R"("error_document":{"key":"oops.html"},)"
	       R"("routing_rules":[{)"
	       R"("condition":{"key_prefix_equals":"docs/",)"
	       R"("http_error_code_returned_equals":"404"},)"
	       R"("redirect":{"protocol":"https","host_name":"example.com",)"
	       R"("replace_key_prefix_with":"documents/",)"
	       R"("http_redirect_code":"301"}}]}})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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

/* The other branch of their anyOf, which is S3's own exclusion. */
TEST_F(NSFSNooBaaStateTest, WebsiteRedirectAllBranch)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"website":{"website_configuration":{)"
	       R"("redirect_all_requests_to":{"host_name":"elsewhere.net",)"
	       R"("protocol":"HTTPS"}}})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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

  write_their_record("ENABLED",
	       R"(,"website":{"website_configuration":{)"
	       R"("index_document":{"suffix":"index.html"},)"
	       R"("routing_rules":[{"redirect":{)"
	       R"("http_redirect_code":"moved"}}]}})");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* Their default encryption reaches the attribute map. */
TEST_F(NSFSNooBaaStateTest, EncryptionComesFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no encryption in their record, none in the attrs */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY), attrs.end());

  write_their_record("ENABLED",
	       R"(,"encryption":{"algorithm":"aws:kms",)"
	       R"("kms_key_id":"key-1234","bucket_key_enabled":true})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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

/* Their schema requires none of the three, so a record that names no
 * algorithm configures nothing.  That is not a failure. */
TEST_F(NSFSNooBaaStateTest, AnEmptyEncryptionRecordConfiguresNothing)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED", R"(,"encryption":{})");

  ASSERT_EQ(read_their_record(info, attrs), 0);
  EXPECT_EQ(attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY), attrs.end());
}

/* An algorithm that is neither of theirs is kept, not refused.
 *
 * The string becomes the x-amz-server-side-encryption header value on
 * every PUT, so RGW already refuses writes into a bucket configured
 * with one it does not know -- fail-closed at the granularity the gap
 * has.  Refusing the bucket at load would also deny reads of objects
 * that are sitting there perfectly readable. */
TEST_F(NSFSNooBaaStateTest, AnUnknownEncryptionAlgorithmIsKeptNotRefused)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED", R"(,"encryption":{"algorithm":"rot13"})");

  ASSERT_EQ(read_their_record(info, attrs), 0)
      << "the bucket was refused over an algorithm that only affects writes";

  auto i = attrs.find(RGW_ATTR_BUCKET_ENCRYPTION_POLICY);
  ASSERT_NE(i, attrs.end());
  RGWBucketEncryptionConfig conf;
  auto bi = i->second.cbegin();
  ASSERT_NO_THROW(decode(conf, bi));
  EXPECT_EQ(conf.sse_algorithm(), "rot13");
}

/* Their bucket tag set reaches the attribute map. */
TEST_F(NSFSNooBaaStateTest, BucketTagsComeFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no tags in their record, none in the attributes */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_TAGS), attrs.end());

  write_their_record("ENABLED",
	       R"(,"tag":[{"key":"team","value":"storage"},)"
	       R"({"key":"env","value":"prod"}])");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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
 * Their schema constrains neither the count nor the sizes, so a
 * bucket of theirs may carry more than fifty.  Refusing to serve it
 * would deny everything over metadata that gates nothing. */
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
  write_their_record("ENABLED", arr);

  ASSERT_EQ(read_their_record(info, attrs), 0)
      << "the bucket was refused over a tag count";

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

  write_their_record("ENABLED", R"(,"tag":[{"key":"","value":"orphan"}])");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* Their lifecycle rules reach the attribute map, through the same
 * parser PutBucketLifecycle uses. */
TEST_F(NSFSNooBaaStateTest, LifecycleComesFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no rules in their record, no attribute */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_LC), attrs.end());

  write_their_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"expire-logs","status":"Enabled",)"
	       R"("filter":{"prefix":"logs/"},)"
	       R"("expiration":{"days":30}}])");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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
 * Their `and` flag records that the original XML wrapped them, and
 * LCFilter_S3 looks for that element first, so reproducing it is what
 * makes a prefix-and-tag filter read back as one. */
TEST_F(NSFSNooBaaStateTest, LifecycleFilterWithPrefixAndTag)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"cold","status":"Enabled",)"
	       R"("filter":{"and":true,"prefix":"data/",)"
	       R"("tags":[{"key":"class","value":"cold"}]},)"
	       R"("expiration":{"days":90}}])");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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
 * Their dates are epoch milliseconds and RGW's are ISO 8601 strings
 * that check_date() requires to be exact midnight UTC.  Nothing else
 * in this file proves the conversion is right rather than merely
 * present, and it decides which day objects are deleted.
 * 1767225600000 is 2026-01-01T00:00:00Z. */
TEST_F(NSFSNooBaaStateTest, ALifecycleDateKeepsItsDay)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"newyear","status":"Enabled","filter":{},)"
	       R"("expiration":{"date":1767225600000}}])");

  ASSERT_EQ(read_their_record(info, attrs), 0)
      << "a midnight date was refused";

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

/* A rule their schema allows and PutBucketLifecycle refuses fails the
 * bucket closed.
 *
 * Their expiration may carry days and a date together;
 * LCExpiration_S3 requires exactly one, and guessing which wins
 * deletes objects on the wrong day. */
TEST_F(NSFSNooBaaStateTest, ALifecycleRuleRgwWouldRefuseFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"both","status":"Enabled","filter":{},)"
	       R"("expiration":{"days":30,"date":1767225600000}}])");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* A rule with no action at all is one RGW refuses too. */
TEST_F(NSFSNooBaaStateTest, ALifecycleRuleWithNoActionFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"lifecycle_configuration_rules":[{)"
	       R"("id":"empty","status":"Enabled","filter":{}}])");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* Their CORS rules reach the attribute map, through the same parser
 * PutBucketCors uses. */
TEST_F(NSFSNooBaaStateTest, CorsComesFromTheirRecord)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* the control:  no rules in their record, no attribute */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);
  ASSERT_EQ(attrs.find(RGW_ATTR_CORS), attrs.end());

  write_their_record("ENABLED",
	       R"(,"cors_configuration_rules":[{"id":"web",)"
	       R"("allowed_methods":["GET","PUT"],)"
	       R"("allowed_origins":["https://example.com"],)"
	       R"("allowed_headers":["x-amz-date"],)"
	       R"("expose_headers":["ETag"],)"
	       R"("max_age_seconds":3000}])");

  ASSERT_EQ(read_their_record(info, attrs), 0);
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
 * Their schema constrains allowed_methods to nothing at all, while
 * RGWCORSRule_S3 refuses anything but GET, POST, DELETE, HEAD, PUT
 * and COPY -- and a CORS rule decides which origins a browser may
 * reach. */
TEST_F(NSFSNooBaaStateTest, AnUnknownCorsMethodFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"cors_configuration_rules":[{)"
	       R"("allowed_methods":["TRACE"],)"
	       R"("allowed_origins":["https://example.com"]}])");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* A rule with no origin is one their own schema requires and RGW
 * refuses, so an empty list fails closed rather than becoming a rule
 * that matches nothing. */
TEST_F(NSFSNooBaaStateTest, ACorsRuleWithNoOriginFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  write_their_record("ENABLED",
	       R"(,"cors_configuration_rules":[{)"
	       R"("allowed_methods":["GET"],"allowed_origins":[]}])");

  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);
}

/* A record we cannot make sense of fails the bucket closed, the same
 * as unreadable state of our own. */
TEST_F(NSFSNooBaaStateTest, AnUnreadableRecordFailsClosed)
{
  RGWBucketInfo info;
  Attrs attrs;

  /* A well-formed record is read first, so the -EBADMSG below is this
   * record being rejected rather than the reader refusing everything
   * it is given. */
  write_their_record("ENABLED");
  ASSERT_EQ(read_their_record(info, attrs), 0);

  write_file(conf_root() / "buckets" / (testname + ".json"),
	     "this is not JSON");
  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG);

  write_their_record("PERHAPS");
  EXPECT_EQ(read_their_record(info, attrs), -EBADMSG)
      << "a versioning value that is none of theirs was accepted";
}

/* Every listing row reports the same owner for an object nothing wrote
 * through RGW.
 *
 * On a base bucket ownership is answered from the file's uid
 * (NooBaaXattrStrategy::object_owner), which is the only answer an
 * object with no extended attributes has.  object_owner() takes the
 * statx that carries the uid, and the version-entry row used to pass
 * nullptr for it -- so a sideloaded object listed with its owner while
 * its own versions listed as "unknown".
 *
 * Two controls, because without them the comparison could not fail.
 * The first asserts the files carry no extended attribute at all, so
 * no recorded ACL can be supplying the answer.  The second asserts the
 * expected owner is this process's uid rather than anything RGW
 * stamped. */
TEST_F(NSFSNooBaaStateTest, EveryListingRowAgreesOnASideloadedOwner)
{
  remove_our_bucket_info();
  write_their_record("ENABLED");
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
   * recorded ACL can be supplying the answer.  Only user.* is tested
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

  /* control two:  the answer is the filesystem's, not RGW's */
  const std::string expect = std::to_string(::getuid());

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
 * The tree is written by hand in their shape -- not through their
 * strategy -- so agreement between the two is a comparison rather
 * than a tautology.  This is the S5 gate for the multipart step:  a
 * tree in their shape which nsfs then reads correctly. */
TEST_F(NSFSNooBaaBucketTest, ListsAnUploadTheyStarted)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  std::string marker;
  bool truncated = false;
  ASSERT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 100, uploads,
				    nullptr, &truncated, null_yield), 0);

  ASSERT_EQ(uploads.size(), 1u);
  EXPECT_EQ(uploads[0]->get_key(), tree.key);
  EXPECT_EQ(uploads[0]->get_upload_id(), tree.upload_id);
  EXPECT_FALSE(truncated);
}

/* A bucket of theirs which has never taken an upload has no
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
				    nullptr, &truncated, null_yield), 0);
  EXPECT_TRUE(uploads.empty());
  EXPECT_FALSE(truncated);
}

/* Their parts, read through their record:  size and offset from
 * user.noobaa.part_size and part_offset, and an etag derived from the
 * stat because the fixture computed no digest -- which is what their
 * own _get_etag() falls back to. */
TEST_F(NSFSNooBaaBucketTest, ListsThePartsOfTheirUpload)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  auto upload = bucket->get_multipart_upload(tree.key, tree.upload_id);
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

/* And completing one.  The object is assembled from their staging and
 * written in their format, which is what makes this a round trip
 * rather than a read. */
TEST_F(NSFSNooBaaBucketTest, CompletesAnUploadTheyStarted)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  auto upload = bucket->get_multipart_upload(tree.key, tree.upload_id);
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
 * It decoded our own record off the meta object and returned -EIO
 * when there was none, which is every upload NooBaa started -- their
 * create_object_upload is a document and the file carries no
 * attribute of ours.  Three ops ask for it, all of them through the
 * op layer:  UploadPart (`rgw_op.cc:4859`), CompleteMultipartUpload
 * (`:7713`) and ListParts (`:8120`).  So in-flight interop was 500 on
 * every one, and nothing here caught it because the tests call
 * complete() directly and never ask for a placement rule.
 *
 * What comes back is what their format holds.  Placement has no
 * counterpart -- a placement rule is an RGW concept, inert on a
 * filesystem -- and neither does the checksum family, because their
 * CreateMultipartUpload never persisted one.
 */
TEST_F(NSFSNooBaaBucketTest, GetInfoOnAnUploadTheyStarted)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);

  auto upload = bucket->get_multipart_upload(tree.key, tree.upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule* rule{nullptr};
  Attrs attrs;
  ASSERT_EQ(upload->get_info(env->dpp, null_yield, &rule, &attrs), 0)
      << "get_info failed on an upload in their format";
  ASSERT_NE(rule, nullptr);
}

/* And it carries across what their document holds.
 *
 * The fixture's upload has no create-time metadata, so this one is
 * written with some:  the storage class and the lock settings reach
 * multipart_upload_info, and the content type and user metadata reach
 * the attribute set, which is where RGW keeps them and where a
 * completion looks.
 */
TEST_F(NSFSNooBaaBucketTest, GetInfoCarriesTheirCreateParams)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);

  const std::string key = "rich/upload.bin";
  const std::string upload_id = "11111111-2222-4333-8444-555555555555";

  /* their document, by hand, as their gateway writes it */
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

  auto upload = bucket->get_multipart_upload(key, upload_id);
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

/* Ours still answers from its own record.
 *
 * The control:  without it the two above pass for a driver that reads
 * NooBaa's document everywhere, and an upload of ours would lose the
 * placement and the checksum that only our record carries. */
TEST_F(NSFSBucketTest, GetInfoOnOurOwnUpload)
{
  const std::string objname = "ours.bin";
  auto upload = bucket->get_multipart_upload(objname, "c0ffee");
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  placement.name = "default-placement";
  /* not STANDARD:  an empty storage class IS standard --
   * get_canonical_storage_class() says so -- so asserting it would
   * pass against a record that carried nothing */
  placement.storage_class = "GLACIER";
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  auto reread = bucket->get_multipart_upload(objname, "c0ffee");
  rgw_placement_rule* rule{nullptr};
  ASSERT_EQ(reread->get_info(env->dpp, null_yield, &rule, nullptr), 0);
  ASSERT_NE(rule, nullptr);
  EXPECT_EQ(rule->name, "default-placement");
  EXPECT_EQ(rule->storage_class, "GLACIER");
}

/* Base can start an upload, not only finish one of theirs.
 *
 * The staging root is two questions.  Reading asks where staging IS,
 * and must not name a directory that is absent -- a bucket with no
 * uploads lists empty.  Writing asks where it WOULD go, and may
 * create the intermediate directory, because the bucket id is already
 * on disk and `multipart-uploads/` beneath it is what their own
 * create_object_upload makes.
 */
TEST_F(NSFSNooBaaBucketTest, CreatesAnUploadOnAQuiescentTree)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);

  const std::string objname = "written/by/us.bin";
  const std::string upload_id = "11111111-2222-4333-8444-555555555555";
  auto upload = bucket->get_multipart_upload(objname, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  /* under their temp directory, which we did not invent */
  std::vector<sf::path> tmpdirs;
  for (auto& e : sf::directory_iterator(bucket_path())) {
    if (e.path().filename().string().starts_with(".noobaa-nsfs_")) {
      tmpdirs.push_back(e.path());
    }
  }
  ASSERT_EQ(tmpdirs.size(), 1u) << "a second temp directory was created";
  EXPECT_TRUE(sf::is_directory(tmpdirs[0] / "multipart-uploads" / upload_id));

  const size_t stride = 64;
  std::string expected;
  std::map<int, std::string> part_etags;
  for (int i = 1; i <= 3; ++i) {
    std::string payload(stride, static_cast<char>('a' + i));
    part_etags[i] = write_mp_part(upload.get(), acl_owner, &placement, i,
				  payload);
    expected += payload;
  }

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);
  EXPECT_EQ(read_all(bucket_path() / objname), expected);
}

/* What their completion path reads out of the meta file.
 *
 * `complete_object_upload` takes content_type, content_encoding,
 * xattr, storage_class and lock_settings out of create_object_upload
 * and puts them on the finished object (`namespace_fs.js:2024`), so
 * an upload we start and their gateway finishes loses each of these
 * if we do not write it.  Nothing of ours reads them back, which is
 * why they need a test of their own.
 *
 * The absences are asserted too.  Their document is JSON.stringify of
 * the request parameters and stringify omits what is undefined, so a
 * field the request did not carry must be missing rather than empty --
 * an empty content_type would become the object's content type.
 */
TEST_F(NSFSNooBaaBucketTest, RecordsWhatTheirCompletionReads)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);

  const std::string upload_id = "11111111-2222-4333-8444-555555555555";
  const std::string key = "meta/data.bin";
  auto upload = bucket->get_multipart_upload(key, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  auto bl_of = [](const char* v) {
    bufferlist bl;
    bl.append(v);
    return bl;
  };

  rgw_placement_rule placement;
  placement.storage_class = "STANDARD";
  Attrs attrs;
  attrs[RGW_ATTR_CONTENT_TYPE] = bl_of("text/plain");
  attrs[RGW_ATTR_CONTENT_ENC] = bl_of("gzip");
  attrs[std::string(RGW_ATTR_META_PREFIX) + "colour"] = bl_of("green");
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  sf::path doc;
  for (auto& e : sf::directory_iterator(bucket_path())) {
    if (e.path().filename().string().starts_with(".noobaa-nsfs_")) {
      doc = e.path() / "multipart-uploads" / upload_id /
	    "create_object_upload";
    }
  }
  ASSERT_FALSE(doc.empty());
  ASSERT_TRUE(sf::exists(doc)) << doc;

  const std::string text = read_all(doc);
  JSONParser parser;
  ASSERT_TRUE(parser.parse(text.data(), text.size())) << text;

  auto field = [&parser](const char* n) -> std::string {
    JSONObj* o = parser.find_obj(n);
    return o ? o->get_data() : std::string{"<absent>"};
  };
  EXPECT_EQ(field("key"), key);
  EXPECT_EQ(field("content_type"), "text/plain");
  EXPECT_EQ(field("content_encoding"), "gzip");
  EXPECT_EQ(field("storage_class"), "STANDARD");

  JSONObj* xattr = parser.find_obj("xattr");
  ASSERT_NE(xattr, nullptr) << text;
  JSONObj* colour = xattr->find_obj("colour");
  ASSERT_NE(colour, nullptr) << text;
  EXPECT_EQ(colour->get_data(), "green");
  /* stored under the bare name, as theirs is:  `user.<key>` on disk,
   * and `x-amz-meta-` belongs to the protocol */
  EXPECT_EQ(xattr->find_obj("x-amz-meta-colour"), nullptr);

  /* their spelling of the upload id, which their ListMultipartUploads
   * reports as UploadId;  under that name and not ours */
  EXPECT_EQ(field("obj_id"), upload_id);
  EXPECT_EQ(field("bucket"), bucket->get_name());
  EXPECT_EQ(parser.find_obj("upload_id"), nullptr) << text;
  EXPECT_EQ(parser.find_obj("lock_settings"), nullptr) << text;
}

/* An ordinary object in a base bucket is written in their names.
 *
 * Not multipart:  the multipart paths were converted when they moved
 * onto NSFSBucket::mpu_strategy(), and the object paths were not,
 * because an FSEnt carries the strategies it inherited from the
 * driver's root.  So a bucket whose profile was base, whose format was
 * noobaa and whose staging layout was theirs still wrote
 * `user.nsfs.rgw.etag` where their reader wants `user.content_md5`.
 *
 * The names are asserted on disk rather than through the driver.
 * Reading back through the same strategy that wrote would agree with
 * itself whichever format it used, which is the tautology this has to
 * avoid;  their gateway reads the names.
 */
TEST_F(NSFSNooBaaBucketTest, ObjectsAreWrittenInTheirNames)
{
  const std::string objname = "plain.bin";
  auto obj = bucket->get_object(rgw_obj_key(objname));
  ASSERT_NE(obj.get(), nullptr);

  auto writer = driver->get_atomic_writer(env->dpp, null_yield, obj.get(),
					  acl_owner, nullptr, 0, testname);
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
  bufferlist meta;
  meta.append("green");
  attrs[std::string(RGW_ATTR_META_PREFIX) + "colour"] = meta;

  ceph::real_time mtime;
  std::string etag;
  req_context rctx{env->dpp, null_yield, nullptr};
  ASSERT_EQ(writer->complete(len, etag, &mtime, real_time(), attrs,
			     std::nullopt, real_time(), nullptr, nullptr,
			     nullptr, nullptr, nullptr, rctx, 0), 0);

  const sf::path p{bucket_path() / objname};
  ASSERT_TRUE(sf::is_regular_file(p)) << p;

  char buf[8192];
  ssize_t nlen = ::listxattr(p.c_str(), buf, sizeof(buf));
  ASSERT_GT(nlen, 0);
  std::set<std::string> names;
  for (const char* q = buf; q < buf + nlen; q += strlen(q) + 1) {
    names.insert(q);
  }

  EXPECT_TRUE(names.contains("user.noobaa.content_type")) << "content type";
  EXPECT_TRUE(names.contains("user.colour")) << "user metadata";
  EXPECT_FALSE(names.contains("user.nsfs.rgw.content_type"));
  EXPECT_FALSE(names.contains("user.nsfs.user.rgw.x-amz-meta-colour"));

  /* and reading it back gives the logical keys, through their parser */
  auto rd = bucket->get_object(rgw_obj_key(objname));
  ASSERT_EQ(rd->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(rd->get_obj_attrs(null_yield, env->dpp), 0);
  auto& got = rd->get_attrs();
  auto it = got.find(RGW_ATTR_CONTENT_TYPE);
  ASSERT_NE(it, got.end()) << "content type did not come back";
  EXPECT_EQ(it->second.to_str(), "text/plain");
}

/* The control:  the same write in a strong bucket keeps our names.
 *
 * Without it the test above passes for a driver that writes NooBaa's
 * names everywhere, which is the other half of the same defect. */
TEST_F(NSFSBucketTest, ObjectsInAMarkedBucketKeepOurNames)
{
  const std::string objname = "plain.bin";
  auto obj = bucket->get_object(rgw_obj_key(objname));
  auto writer = driver->get_atomic_writer(env->dpp, null_yield, obj.get(),
					  acl_owner, nullptr, 0, testname);
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
  EXPECT_TRUE(names.contains("user.nsfs.rgw.content_type"));
  EXPECT_FALSE(names.contains("user.noobaa.content_type"));
}

/* A bucket mid-upgrade:  marked as ours, and still holding objects in
 * their format.
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
    ASSERT_STREQ(b->profile_name(), "strong");
    ASSERT_EQ(b->mark_converting(env->dpp, true), 0);
    ASSERT_TRUE(b->is_converting());
  }

  sf::path bucket_path() const { return bp / "root" / testname; }

  /* an object of theirs, laid down by hand:  their attribute names,
   * with no attribute of ours anywhere on it */
  void write_their_object(const std::string& name) {
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

/* Their attributes are read on a converting bucket. */
TEST_F(NSFSConvertingBucketTest, ReadsTheirAttributes)
{
  write_their_object("theirs.bin");

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
TEST_F(NSFSBucketTest, DoesNotReadTheirAttributesWithoutTheMarker)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  ASSERT_FALSE(b->is_converting());

  const sf::path p{bp / "root" / testname / "theirs.bin"};
  write_file(p, "their bytes");
  const std::string theirs{"text/plain"};
  ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.content_type",
		       theirs.data(), theirs.size(), 0), 0);
  const std::string md5{"0123456789abcdef0123456789abcdef"};
  ASSERT_EQ(::setxattr(p.c_str(), "user.content_md5",
		       md5.data(), md5.size(), 0), 0);

  auto obj = bucket->get_object(rgw_obj_key("theirs.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);

  /* Both keys are present either way:  the driver guesses a content
   * type from the name and synthesizes an etag from the stat when an
   * object carries neither.  So the values are what distinguish the
   * two cases, and what is asserted is that neither came from their
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

/* A directory object of theirs, which the walk cannot see.
 *
 * Their empty one has no sentinel:  the fact is
 * user.noobaa.dir_content on the directory.  This is the consumer the
 * marker is worth having for -- it is a syscall per subdirectory, and
 * the attribute chain is not. */
TEST_F(NSFSConvertingBucketTest, ListsTheirEmptyDirectoryObject)
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

TEST_F(NSFSBucketTest, DoesNotListTheirDirectoryObjectWithoutTheMarker)
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

/* Writing over a converting object drops the foreign spelling.
 *
 * The REPLACE_ALL prune walks the on-disk names and asks the chain for
 * the logical key;  a superseded attribute of theirs then resolves to
 * the key being written and lands in to_remove.  Without the chain in
 * the prune it parses as nothing, is left alone, and the object ends
 * up carrying both spellings of its content type -- with the reader
 * preferring whichever the chain asks first. */
TEST_F(NSFSConvertingBucketTest, WritingPrunesTheForeignSpelling)
{
  write_their_object("theirs.bin");
  const sf::path p{bucket_path() / "theirs.bin"};

  auto obj = bucket->get_object(rgw_obj_key("theirs.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);

  Attrs attrs;
  bufferlist ct;
  ct.append("application/octet-stream");
  attrs[RGW_ATTR_CONTENT_TYPE] = ct;
  ASSERT_EQ(obj->set_obj_attrs(env->dpp, &attrs, nullptr, null_yield,
			       rgw::sal::FLAG_LOG_OP), 0);

  char buf[8192];
  ssize_t nlen = ::listxattr(p.c_str(), buf, sizeof(buf));
  ASSERT_GT(nlen, 0);
  std::set<std::string> names;
  for (const char* q = buf; q < buf + nlen; q += strlen(q) + 1) {
    names.insert(q);
  }
  EXPECT_TRUE(names.contains("user.nsfs.rgw.content_type"));
  EXPECT_FALSE(names.contains("user.noobaa.content_type"))
      << "the object carries both spellings of its content type";
}

/* And the same, on a bucket in their format.
 *
 * The callback computed the staging directory with the driver's own
 * path strategy, which names ours.  A base bucket stages under
 * `.noobaa-nsfs_<id>/multipart-uploads/`, so the openat failed and it
 * returned having written nothing:  under writeback the records lived
 * only in the cache, and eviction -- the one thing that was supposed
 * to persist them -- dropped them instead.
 */
class NSFSNooBaaEvictingBucketTest : public NSFSNooBaaBucketTest {
public:
  file::listing::MultipartCachePolicy mp_cache_policy() const override {
    return file::listing::MultipartCachePolicy::writeback;
  }
  std::array<uint64_t, 4> mp_cache_sizes() const override {
    return {1, 1, 1, 8};
  }
};

TEST_F(NSFSNooBaaEvictingBucketTest, AnEvictedPartReachesTheirStaging)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);

  const std::string objname = "written/by/us.bin";
  const std::string upload_id = "11111111-2222-4333-8444-555555555555";
  auto upload = bucket->get_multipart_upload(objname, upload_id);
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

  {
    auto other = bucket->get_multipart_upload(
	"other.bin", "22222222-3333-4444-8555-666666666666");
    ASSERT_EQ(other->init(env->dpp, null_yield, acl_owner, placement,
			  attrs), 0);
    write_mp_part(other.get(), acl_owner, &placement, 1,
		  std::string(stride, 'z'));
  }

  /* the records are on disk, in their staging and in both formats:
   * ours because it is what our reader decodes, theirs because it is
   * what their gateway reads */
  sf::path staging;
  for (auto& e : sf::directory_iterator(bucket_path())) {
    if (e.path().filename().string().starts_with(".noobaa-nsfs_")) {
      staging = e.path() / "multipart-uploads" / upload_id;
    }
  }
  ASSERT_TRUE(sf::is_directory(staging)) << staging;

  for (int i = 1; i <= 3; ++i) {
    const sf::path rec{staging / ("part-" + std::to_string(i))};
    ASSERT_TRUE(sf::exists(rec)) << rec;
    char buf[8192];
    ssize_t len = ::listxattr(rec.c_str(), buf, sizeof(buf));
    ASSERT_GT(len, 0) << rec << " carries no record at all";
    std::set<std::string> names;
    for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
      names.insert(q);
    }
    EXPECT_TRUE(names.contains("user.nsfs.mp_upload")) << i;
    EXPECT_TRUE(names.contains("user.noobaa.part_size")) << i;
  }

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);
  EXPECT_EQ(read_all(bucket_path() / objname), expected);
}

/* Object lock:  three plain attributes of theirs, two encoded ones of
 * ours.
 *
 * Legal hold is one-to-one and still needs the widened pair, because
 * the shapes differ -- theirs is the literal `ON` and ours is an
 * encoded RGWObjectLegalHold.  Retention is two-to-one, and is both
 * attributes or neither:  their reader returns undefined unless mode
 * and date are both present (`namespace_fs.js:2890`).
 */
TEST_F(NSFSNooBaaBucketTest, ReadsTheirObjectLock)
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
 * Their rule, and the safe one:  a retention holding the epoch
 * because a field was missing reads as expired, which is the one
 * wrong answer object lock must never give. */
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

  /* and a date this gateway cannot parse is the same answer */
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

/* And ours is written in their three attributes. */
TEST_F(NSFSNooBaaBucketTest, WritesObjectLockInTheirShape)
{
  const sf::path p{bucket_path() / "mine.bin"};
  write_file(p, "bytes");

  auto obj = bucket->get_object(rgw_obj_key("mine.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);

  ceph::real_time until;
  ASSERT_EQ(parse_time("2031-06-15T12:00:00.000Z", &until), 0);
  RGWObjectRetention ret("GOVERNANCE", until);
  RGWObjectLegalHold hold("OFF");
  Attrs set;
  bufferlist rb, hb;
  ret.encode(rb);
  hold.encode(hb);
  set[RGW_ATTR_OBJECT_RETENTION] = rb;
  set[RGW_ATTR_OBJECT_LEGAL_HOLD] = hb;
  ASSERT_EQ(obj->set_obj_attrs(env->dpp, &set, nullptr, null_yield,
			       rgw::sal::FLAG_LOG_OP), 0);

  auto xattr = [&p](const char* n) {
    char buf[256];
    ssize_t len = ::getxattr(p.c_str(), n, buf, sizeof(buf));
    return (len > 0) ? std::string(buf, len) : std::string{};
  };
  EXPECT_EQ(xattr("user.noobaa.legal_hold"), "OFF");
  EXPECT_EQ(xattr("user.noobaa.retention_mode"), "GOVERNANCE");
  EXPECT_EQ(xattr("user.noobaa.retention_date").substr(0, 10), "2031-06-15")
      << "got " << xattr("user.noobaa.retention_date");

  /* read back through the driver:  the round trip is what a
   * converting bucket depends on */
  auto rd = bucket->get_object(rgw_obj_key("mine.bin"));
  ASSERT_EQ(rd->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(rd->get_obj_attrs(null_yield, env->dpp), 0);
  auto i = rd->get_attrs().find(RGW_ATTR_OBJECT_RETENTION);
  ASSERT_NE(i, rd->get_attrs().end());
  RGWObjectRetention back;
  auto bi = i->second.cbegin();
  back.decode(bi);
  EXPECT_EQ(back.get_mode(), "GOVERNANCE");
  EXPECT_EQ(back.get_retain_until_date(), until);
}

/* The control:  a marked bucket keeps ours encoded. */
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
			       rgw::sal::FLAG_LOG_OP), 0);

  char buf[8192];
  ssize_t len = ::listxattr(p.c_str(), buf, sizeof(buf));
  ASSERT_GT(len, 0);
  std::set<std::string> names;
  for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
    names.insert(q);
  }
  EXPECT_FALSE(names.contains("user.noobaa.legal_hold"))
      << "wrote their shape in a bucket of ours";
  EXPECT_TRUE(names.contains("user.nsfs.rgw.object-legal-hold"));
}

/* Storage class:  a plain rename that was missed, and a bogus
 * metadata key with it.
 *
 * Theirs is `user.storage_class` (`glacier.js:48`) and is not under
 * `user.noobaa.`, so before the mapping existed it fell through to
 * "what is left under user. is user metadata" and came back as
 * `x-amz-meta-storage_class` -- a key no client ever set and one
 * their own gateway does not report, because it is the sole member of
 * XATTR_METADATA_IGNORE_LIST (`namespace_fs.js:111`).
 */
TEST_F(NSFSNooBaaBucketTest, ReadsTheirStorageClass)
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

/* And ours is written under their name. */
TEST_F(NSFSNooBaaBucketTest, WritesStorageClassUnderTheirName)
{
  const sf::path p{bucket_path() / "warm.bin"};
  write_file(p, "bytes");

  auto obj = bucket->get_object(rgw_obj_key("warm.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  Attrs set;
  bufferlist bl;
  bl.append("GLACIER");
  set[RGW_ATTR_STORAGE_CLASS] = bl;
  ASSERT_EQ(obj->set_obj_attrs(env->dpp, &set, nullptr, null_yield,
			       rgw::sal::FLAG_LOG_OP), 0);

  char buf[256];
  ssize_t len = ::getxattr(p.c_str(), "user.storage_class", buf, sizeof(buf));
  ASSERT_GT(len, 0) << "not written under their name";
  EXPECT_EQ(std::string(buf, len), "GLACIER");

  char nb[8192];
  ssize_t nlen = ::listxattr(p.c_str(), nb, sizeof(nb));
  ASSERT_GT(nlen, 0);
  for (const char* q = nb; q < nb + nlen; q += strlen(q) + 1) {
    EXPECT_EQ(std::string(q).find("nsfs.rgw.storage_class"),
	      std::string::npos)
	<< "also wrote our name: " << q;
  }
}

/* Object tags, which are one attribute per tag in their format and
 * one encoded RGWObjTags in ours.
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

  /* an object of theirs with tags, laid down by hand */
  void their_tagged_object(const std::string& name,
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

/* Their tags are read as ours. */
TEST_F(NSFSTagTest, ReadsTheirTags)
{
  their_tagged_object("tagged.bin", {{"colour", "green"}, {"size", "large"}});

  auto obj = bucket->get_object(rgw_obj_key("tagged.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);

  auto tags = decode_tags(obj->get_attrs());
  ASSERT_EQ(tags.get_tags().size(), 2u) << "their tags were not read";
  EXPECT_EQ(tags.get_tags().find("colour")->second, "green");
  EXPECT_EQ(tags.get_tags().find("size")->second, "large");
}

/* And ours are written as theirs. */
TEST_F(NSFSTagTest, WritesTagsInTheirShape)
{
  const sf::path p{bucket_path() / "mine.bin"};
  write_file(p, "bytes");

  auto obj = bucket->get_object(rgw_obj_key("mine.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);

  RGWObjTags tags;
  tags.add_tag("colour", "blue");
  tags.add_tag("shape", "round");
  bufferlist bl;
  tags.encode(bl);
  Attrs set;
  set[RGW_ATTR_TAGS] = bl;
  ASSERT_EQ(obj->set_obj_attrs(env->dpp, &set, nullptr, null_yield,
			       rgw::sal::FLAG_LOG_OP), 0);

  auto names = xattr_names(p);
  EXPECT_TRUE(names.contains("user.noobaa.tag.colour"));
  EXPECT_TRUE(names.contains("user.noobaa.tag.shape"));
  EXPECT_EQ(xattr(p, "user.noobaa.tag.colour"), "blue");
  EXPECT_EQ(xattr(p, "user.noobaa.tag.shape"), "round");
  /* and not as one blob of ours */
  for (const auto& n : names) {
    EXPECT_EQ(n.find("x-amz-tagging"), std::string::npos)
	<< "wrote our encoded tag set on their tree: " << n;
  }

  /* read back through the driver, which is the round trip */
  auto rd = bucket->get_object(rgw_obj_key("mine.bin"));
  ASSERT_EQ(rd->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(rd->get_obj_attrs(null_yield, env->dpp), 0);
  EXPECT_EQ(decode_tags(rd->get_attrs()).get_tags().size(), 2u);
}

/* Writing the set replaces it, which is S3's contract and theirs. */
TEST_F(NSFSTagTest, WritingTheSetReplacesIt)
{
  their_tagged_object("tagged.bin", {{"old", "1"}, {"stale", "2"}});

  auto obj = bucket->get_object(rgw_obj_key("tagged.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  RGWObjTags tags;
  tags.add_tag("fresh", "3");
  bufferlist bl;
  tags.encode(bl);
  Attrs set;
  set[RGW_ATTR_TAGS] = bl;
  ASSERT_EQ(obj->set_obj_attrs(env->dpp, &set, nullptr, null_yield,
			       rgw::sal::FLAG_LOG_OP), 0);

  auto names = xattr_names(bucket_path() / "tagged.bin");
  EXPECT_TRUE(names.contains("user.noobaa.tag.fresh"));
  EXPECT_FALSE(names.contains("user.noobaa.tag.old"))
      << "the replaced set was merged rather than replaced";
  EXPECT_FALSE(names.contains("user.noobaa.tag.stale"));
}

/* DeleteObjectTagging removes all of them, not one name of ours. */
TEST_F(NSFSTagTest, DeletingTagsRemovesTheirAttributes)
{
  their_tagged_object("tagged.bin", {{"a", "1"}, {"b", "2"}});
  const sf::path p{bucket_path() / "tagged.bin"};

  auto obj = bucket->get_object(rgw_obj_key("tagged.bin"));
  ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(obj->delete_obj_attrs(env->dpp, RGW_ATTR_TAGS, null_yield), 0);

  auto names = xattr_names(p);
  EXPECT_FALSE(names.contains("user.noobaa.tag.a"));
  EXPECT_FALSE(names.contains("user.noobaa.tag.b"));

  auto rd = bucket->get_object(rgw_obj_key("tagged.bin"));
  ASSERT_EQ(rd->load_obj_state(env->dpp, null_yield), 0);
  ASSERT_EQ(rd->get_obj_attrs(null_yield, env->dpp), 0);
  EXPECT_EQ(rd->get_attrs().find(RGW_ATTR_TAGS), rd->get_attrs().end());
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
			       rgw::sal::FLAG_LOG_OP), 0);

  char buf[8192];
  ssize_t len = ::listxattr(p.c_str(), buf, sizeof(buf));
  ASSERT_GT(len, 0);
  std::set<std::string> names;
  for (const char* q = buf; q < buf + len; q += strlen(q) + 1) {
    names.insert(q);
  }
  EXPECT_FALSE(names.contains("user.noobaa.tag.colour"))
      << "wrote their tag shape in a bucket of ours";
  bool ours = false;
  for (const auto& n : names) {
    if (n.find("x-amz-tagging") != std::string::npos) {
      ours = true;
    }
  }
  EXPECT_TRUE(ours) << "our encoded tag set was not written";
}

/* The upgrade:  a bucket in NooBaa's format moved to one of ours,
 * with its tree rewritten.
 *
 * Starts from base, which is what a tree we adopted is, and asks for
 * strong -- which is what set_profile() does for an operator through
 * /admin/nsfs/profile.
 */
class NSFSUpgradeTest : public NSFSNooBaaBucketTest {
public:
  /* an object of theirs, by hand:  their names, nothing of ours */
  void their_object(const std::string& name, const char* ctype) {
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

  /* an empty directory object of theirs:  no sentinel, the fact is on
   * the directory */
  void their_folder(const std::string& name, const char* ctype) {
    const sf::path p{bucket_path() / name};
    sf::create_directories(p);
    set_u64_xattr(p, "user.noobaa.dir_content", 0);
    const std::string ct{ctype};
    ASSERT_EQ(::setxattr(p.c_str(), "user.noobaa.content_type",
			 ct.data(), ct.size(), 0), 0);
  }

  /* a version entry of theirs, under .versions/.  Their layout is
   * ours -- we borrowed it -- so only the attribute names differ. */
  void their_version(const std::string& leaf, const std::string& vid,
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

  /* a delete marker of theirs:  no bytes, and no ACL of any kind */
  void their_delete_marker(const std::string& leaf, const std::string& vid) {
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
   * value read back through our own strategy carries the terminator
   * and one read through theirs does not -- theirs is not a counted
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

/* The bucket's own attributes are converted with its tree.
 *
 * Bucket configuration is the one thing written in our form while the
 * bucket is still base, so it lands under the base spelling.  An
 * attribute write merges rather than replaces, so a base spelling left
 * behind would be joined by ours on the next write and the reader
 * would have two names for one key. */
TEST_F(NSFSUpgradeTest, ConvertsTheBucketsOwnAttributes)
{
  static constexpr const char* base_name = "user.nsfs.user.rgw.iam-policy";
  static constexpr const char* our_name = "user.nsfs.rgw.iam-policy";
  const std::string policy{"{\"Version\":\"2012-10-17\"}"};

  bufferlist bl;
  bl.append(policy);
  rgw::sal::Attrs add;
  add[RGW_ATTR_IAM_POLICY] = bl;
  ASSERT_EQ(bucket->merge_and_store_attrs(env->dpp, add, null_yield), 0);

  /* the control:  base has to have written the base spelling, or the
   * assertion after the upgrade cannot fail */
  auto before = xattr_names(bucket_path());
  ASSERT_TRUE(before.contains(base_name))
      << "base did not write the base spelling, so this test proves nothing";
  ASSERT_FALSE(before.contains(our_name));

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  ASSERT_TRUE(prog.complete);

  auto after = xattr_names(bucket_path());
  EXPECT_FALSE(after.contains(base_name))
      << "the base spelling survived the upgrade;  a later write would "
      << "add ours beside it";
  EXPECT_TRUE(after.contains(our_name));
  EXPECT_EQ(str_xattr(bucket_path(), our_name), policy);

  /* the profile markers are not bucket state and the pass leaves them
   * alone;  the mask is still there and still says strong */
  EXPECT_TRUE(after.contains(nsfs::EXTENSIONS_XATTR));
  EXPECT_FALSE(after.contains(nsfs::CONVERTING_XATTR));
}

/* .versions/ is a reserved name, so the tree walk skips it.  It skipped
 * the version entries with it until convert_versions() existed, which
 * left every one of them in their spelling while the bucket read in
 * ours. */
TEST_F(NSFSUpgradeTest, ConvertsVersionEntries)
{
  const std::string v1{"mtime-dlyy3p93o4mt-ino-430o2"};
  const std::string v2{"mtime-dlyy3p96naq6-ino-430o3"};
  their_object("photo.jpg", "image/jpeg");
  their_version("photo.jpg", v1, "image/jpeg");
  /* a leaf with an underscore in it, which is what makes the entry
   * name ambiguous and the split non-trivial */
  their_version("my_photo.jpg", v2, "image/png");

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
    EXPECT_TRUE(names.contains("user.nsfs.rgw.content_type"))
	<< leaf << " version kept no content type we can read";
    EXPECT_FALSE(names.contains("user.noobaa.content_type"))
	<< leaf << " version kept their spelling;  a later write would "
	<< "add ours beside it";
  }
}

/* The severe one.  Their delete marker is user.noobaa.delete_marker and
 * ours is user.nsfs.delete_marker;  every read resolves the name
 * through the bucket's strategy, and there is no chaining reader, so a
 * marker left in their spelling stops being a marker once the mask is
 * written.  The object it deleted comes back -- 200 with no bytes where
 * S3 owes 404 -- and nothing reports it. */
TEST_F(NSFSUpgradeTest, ADeleteMarkerStillReadsAsOneAfterConversion)
{
  const std::string vid{"mtime-dlyy3p9ezde0-ino-hu35vm"};
  /* no leaf object:  a DELETE of a key that does not exist still
   * writes a marker, and that is the case with no ACL to inherit */
  their_delete_marker("gone.bin", vid);

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.versions, 1u);

  auto names = xattr_names(version_path("gone.bin", vid));
  EXPECT_TRUE(names.contains("user.nsfs.delete_marker"))
      << "the marker is no longer a marker;  the deleted object is back";
  EXPECT_FALSE(names.contains("user.noobaa.delete_marker"));
}

/* Conversion used to make a version owner *worse*:  unconverted, the
 * bucket reads with their strategy and object_owner() answers from the
 * inode;  converted, ours demands a recorded ACL and has no fallback.
 * An entry left untouched therefore went from a correct owner to
 * unknown/unknown.  A delete marker needs this too -- RGW emits an
 * <Owner> for one. */
TEST_F(NSFSUpgradeTest, ConversionCapturesTheOwnerOfAVersion)
{
  const std::string vid{"mtime-dlyy3p99mgtk-ino-430o4"};
  const std::string dmid{"mtime-dlyy3p9muo1r-ino-hu35vn"};
  their_version("doc.txt", vid, "text/plain");
  their_delete_marker("doc.txt", dmid);

  for (const auto& p : {version_path("doc.txt", vid),
			version_path("doc.txt", dmid)}) {
    ASSERT_FALSE(xattr_names(p).contains("user.nsfs.rgw.acl"))
	<< "nothing to prove:  " << p << " already had an ACL";
  }

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.versions, 2u);

  for (const auto& p : {version_path("doc.txt", vid),
			version_path("doc.txt", dmid)}) {
    EXPECT_TRUE(xattr_names(p).contains("user.nsfs.rgw.acl"))
	<< p << " has no owner after conversion, so it lists as unknown";
  }
}

/* The lock file is ours, not a version, and must not be counted or
 * rewritten as one. */
TEST_F(NSFSUpgradeTest, TheVersionsLockIsNotAVersion)
{
  const std::string vid{"mtime-dlyy3p9clmwx-ino-430o5"};
  their_version("only.bin", vid, "text/plain");
  write_file(bucket_path() / ".versions" / ".lock", "");

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.versions, 1u);
}

/* Neither marker is bucket metadata, so neither reaches the S3
 * attribute set -- including while a conversion is in flight, which is
 * the only time the converting marker exists. */
TEST_F(NSFSUpgradeTest, TheMarkersAreNotBucketAttributes)
{
  their_object("flat.bin", "text/plain");

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
  const auto& attrs = bucket->get_attrs();
  EXPECT_EQ(attrs.find("converting"), attrs.end());
  EXPECT_EQ(attrs.find("extensions"), attrs.end());
}

/* Everything in the tree ends up in our names, and the marker is gone. */
TEST_F(NSFSUpgradeTest, ConvertsTheTree)
{
  their_object("flat.bin", "text/plain");
  their_object("deep/nested.bin", "application/json");
  their_folder("photos", "application/directory");

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
    EXPECT_TRUE(names.contains("user.nsfs.rgw.content_type")) << k;
    EXPECT_TRUE(names.contains("user.nsfs.rgw.etag")) << k;
    EXPECT_FALSE(names.contains("user.noobaa.content_type")) << k;
    EXPECT_FALSE(names.contains("user.content_md5")) << k;
  }

  /* the folder's fact moved:  their attribute on the directory, ours
   * in the sentinel beside it */
  EXPECT_TRUE(sf::is_regular_file(bucket_path() / "photos" / ".folder"));
  EXPECT_FALSE(xattr_names(bucket_path() / "photos")
		   .contains("user.noobaa.dir_content"));
  EXPECT_TRUE(xattr_names(bucket_path() / "photos" / ".folder")
		  .contains("user.nsfs.rgw.content_type"));

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
  their_object("a.bin", "text/plain");
  their_object("b.bin", "text/plain");

  std::vector<std::string> seen;
  nsfs::convert_cb_t cb =
    [&seen](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      seen.push_back(std::string(nsfs::to_string(ev.phase)) + ":" + ev.key);
      return 0;
    };

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(&cb, &prog), 0);
  ASSERT_EQ(seen.size(), 9u)
      << "three phases for the bucket and for each of two objects";

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
 * exist before -- both spellings on the file at once -- and it is
 * where "ours first" in the read chain stops being incidental.  The
 * callback is what makes it reachable:  without a hold there is no
 * way to arrive inside one item's conversion.
 */
TEST_F(NSFSUpgradeTest, AReaderHeldAtWrittenSeesTheNewValue)
{
  their_object("held.bin", "text/plain");

  std::string seen_ct, seen_etag;
  nsfs::convert_cb_t cb =
    [&](const DoutPrefixProvider*, const nsfs::ConvertEvent& ev) {
      if ((ev.phase != nsfs::ConvertEvent::Phase::written) ||
	  (ev.key != "held.bin")) {
	return 0;
      }
      /* both spellings are on the file right now */
      auto names = xattr_names(bucket_path() / "held.bin");
      EXPECT_TRUE(names.contains("user.nsfs.rgw.content_type"));
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
    their_object("obj-" + std::to_string(i) + ".bin", "text/plain");
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

  /* half the tree is ours and half is theirs, and both read */
  int ours = 0, theirs = 0;
  for (int i = 0; i < 4; ++i) {
    auto names = xattr_names(bucket_path() /
			     ("obj-" + std::to_string(i) + ".bin"));
    if (names.contains("user.noobaa.content_type")) {
      ++theirs;
    } else {
      ++ours;
    }
    auto obj = bucket->get_object(
	rgw_obj_key("obj-" + std::to_string(i) + ".bin"));
    ASSERT_EQ(obj->load_obj_state(env->dpp, null_yield), 0);
    ASSERT_EQ(obj->get_obj_attrs(null_yield, env->dpp), 0);
    EXPECT_EQ(unterminated(obj->get_attrs()[RGW_ATTR_ETAG]),
	      "0123456789abcdef0123456789abcdef")
	<< "obj-" << i << " unreadable mid-conversion";
  }
  EXPECT_EQ(ours, 2);
  EXPECT_EQ(theirs, 2);

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

/* Converting a tree that is already ours changes nothing.
 *
 * Resumption reruns items it already did, so the pass has to be
 * idempotent or a restart corrupts what the first run finished. */
TEST_F(NSFSUpgradeTest, ConvertingTwiceIsTheSameAsOnce)
{
  their_object("once.bin", "text/plain");
  ASSERT_EQ(upgrade(nullptr, nullptr), 0);
  const auto after_one = xattr_names(bucket_path() / "once.bin");

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->mark_converting(env->dpp, true), 0);
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
 * their layout.  The alternative is aborting it, which throws away
 * work the client already did.
 */
TEST_F(NSFSUpgradeTest, ReStagesAnUploadInFlight)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::with_upload, tree);
  their_object("beside.bin", "text/plain");

  nsfs::ConvertProgress prog;
  ASSERT_EQ(upgrade(nullptr, &prog), 0);
  EXPECT_EQ(prog.uploads, 1u);
  EXPECT_EQ(prog.parts, tree.parts);
  EXPECT_TRUE(prog.complete);

  /* their staging is gone */
  EXPECT_FALSE(sf::exists(tree.staging)) << tree.staging;

  /* and ours is there, named as we name one */
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  const std::string meta = tree.key + "." + tree.upload_id;
  const sf::path ours{bucket_path() / b->mpu_strategy()->staging_dir_name(meta)};
  ASSERT_TRUE(sf::is_directory(ours)) << ours;

  /* the upload lists, through our layout */
  std::vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  std::string marker;
  bool truncated = false;
  ASSERT_EQ(bucket->list_multiparts(env->dpp, "", marker, "", 100, uploads,
				    nullptr, &truncated, null_yield), 0);
  ASSERT_EQ(uploads.size(), 1u);
  EXPECT_EQ(uploads[0]->get_key(), tree.key);
  EXPECT_EQ(uploads[0]->get_upload_id(), tree.upload_id);

  /* its parts report the sizes they were staged with */
  auto upload = bucket->get_multipart_upload(tree.key, tree.upload_id);
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

/* Stopping between parts leaves their staging in place.
 *
 * Their directory is removed last for exactly this reason:  a run that
 * dies partway has copied some parts and must leave the source where
 * the next run can find it. */
TEST_F(NSFSUpgradeTest, StoppingMidUploadKeepsTheirStaging)
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

  auto upload = bucket->get_multipart_upload(tree.key, tree.upload_id);
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
 * The re-stage moves one upload at a time, so mid-run some are in our
 * root and the rest are still in theirs.  A listing that scanned one
 * root would report half the uploads in flight and call itself
 * complete.  Held at the moment their staging still exists and ours
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
					null_yield), 0);
      counts.push_back(uploads.size());
      for (auto& u : uploads) {
	EXPECT_EQ(u->get_key(), tree.key);
	EXPECT_EQ(u->get_upload_id(), tree.upload_id);
      }
      return 0;
    };

  ASSERT_EQ(upgrade(&watch, nullptr), 0);
  ASSERT_EQ(counts.size(), 3u) << "three phases for the upload";
  /* before:  only theirs exists.  written:  both do, and it is one
   * upload either way.  pruned:  only ours. */
  EXPECT_EQ(counts[0], 1u) << "invisible before it was moved";
  EXPECT_EQ(counts[1], 1u) << "reported twice while both roots hold it";
  EXPECT_EQ(counts[2], 1u);
}

/* Mutations are held off while the tree is rewritten, and reads are
 * not.
 *
 * The conversion reads an object's attributes and writes them back, so
 * a write landing between those two is lost.  Rados has the same
 * problem during a reshard and answers it the same way:  hold the
 * mutation server-side for a bound, then let a retryable error reach
 * the client.
 */
TEST_F(NSFSUpgradeTest, HoldsOffMutationsAndNotReads)
{
  /* the refusal is what is under test, not how long it takes to
   * arrive;  the production wait would put five seconds in the suite */
  driver->set_gate_waits(std::chrono::milliseconds(20),
			 std::chrono::milliseconds(60000));
  their_object("gated.bin", "text/plain");

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

      /* a write does not.  Not -EBUSY:  503 is what a client retries,
       * and the operation will succeed once the rewrite is done. */
      auto wr = bucket->get_object(rgw_obj_key("gated.bin"));
      EXPECT_EQ(wr->load_obj_state(env->dpp, null_yield), 0);
      Attrs a;
      bufferlist v;
      v.append("text/html");
      a[RGW_ATTR_CONTENT_TYPE] = v;
      EXPECT_EQ(wr->set_obj_attrs(env->dpp, &a, nullptr, null_yield,
				  rgw::sal::FLAG_LOG_OP),
		-ERR_SERVICE_UNAVAILABLE);
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
			      rgw::sal::FLAG_LOG_OP), 0);
}

/* A stopped conversion reopens the gate.
 *
 * The bucket stays marked and half-converted, which is a state it can
 * be served in -- so leaving mutations held off until somebody
 * resumes would turn a partial conversion into an outage. */
TEST_F(NSFSUpgradeTest, AStoppedConversionReopensTheGate)
{
  their_object("a.bin", "text/plain");
  their_object("b.bin", "text/plain");

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
			      rgw::sal::FLAG_LOG_OP), 0);
}

/* Two conversions of one bucket do not run together.
 *
 * Each would read what the other had half written.  The second is
 * refused rather than queued:  it has nothing to wait for that the
 * first is not already doing. */
TEST_F(NSFSUpgradeTest, RefusesASecondConversion)
{
  their_object("a.bin", "text/plain");

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
TEST_F(NSFSBucketTest, AWriteHandleHoldsTheGateOpen)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  ASSERT_TRUE(b->has_extension(nsfs::EXT_SHADOW));
  driver->set_gate_waits(std::chrono::milliseconds(20),
			 std::chrono::milliseconds(20));

  auto obj = bucket->get_object(rgw_obj_key("handle.bin"));
  auto [rc, hdl] = obj->get_fsio_handle(
      env->dpp, rgw::sal::Object::FSIOObject::OPEN_FLAG_CREATE |
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

/* And a closed gate refuses a new write handle while serving a read
 * one. */
TEST_F(NSFSBucketTest, AClosedGateRefusesAWriteHandle)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  driver->set_gate_waits(std::chrono::milliseconds(20),
			 std::chrono::milliseconds(20));

  /* an object to read;  a read handle binds to the published object */
  {
    auto obj = bucket->get_object(rgw_obj_key("there.bin"));
    auto [rc, hdl] = obj->get_fsio_handle(
	env->dpp, rgw::sal::Object::FSIOObject::OPEN_FLAG_CREATE |
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
    auto obj = bucket->get_object(rgw_obj_key("refused.bin"));
    auto [rc, hdl] = obj->get_fsio_handle(
	env->dpp, rgw::sal::Object::FSIOObject::OPEN_FLAG_CREATE |
		  rgw::sal::Object::FSIOObject::OPEN_FLAG_WRITE);
    EXPECT_EQ(rc, -ERR_SERVICE_UNAVAILABLE);
    EXPECT_EQ(hdl.get(), nullptr) << "a refused open left a handle behind";
  }

  {
    auto obj = bucket->get_object(rgw_obj_key("there.bin"));
    auto [rc, hdl] = obj->get_fsio_handle(
	env->dpp, rgw::sal::Object::FSIOObject::OPEN_FLAG_NONE);
    EXPECT_EQ(rc, 0) << "a read handle was refused";
    if (hdl) {
      hdl->close(env->dpp, rgw::sal::Object::FSIOObject::CLOSE_FLAG_NONE);
    }
  }

  driver->gate_open(testname);
}

/* A PUT over a side-loaded file replaces the inode.
 *
 * This is the premise the mode narrowings rested on, and the reason
 * removing them is safe.  The write path narrowed the mode with an
 * fchmod on every write call;  if a PUT wrote *into* a file that was
 * already there, that fchmod would be re-permissioning somebody
 * else's file, and dropping it would change behaviour on a tree that
 * is shared with NFS clients and with NooBaa -- side-loading is a
 * first-class case here, not an edge.
 *
 * It does not write into it.  File::create() opens O_TMPFILE and
 * link_temp_file() links the result over the name, so the published
 * object is always an inode this gateway made, and the mode comes
 * from that create.  Asserting the inode *changed* is what
 * distinguishes the two, because the mode alone comes out 0600 either
 * way.
 */
TEST_F(NSFSBucketTest, APutOverASideLoadedFileReplacesTheInode)
{
  const mode_t saved = ::umask(022);
  auto restore = make_scope_guard([saved] { ::umask(saved); });

  /* a file somebody else put in the tree, with somebody else's mode */
  const sf::path p{bucket_path() / "sideloaded.bin"};
  {
    std::ofstream f(p);
    f << "not ours";
  }
  ASSERT_EQ(::chmod(p.c_str(), 0644), 0);
  struct stat before{};
  ASSERT_EQ(::stat(p.c_str(), &before), 0);
  ASSERT_EQ(before.st_mode & 07777, 0644u) << "the side-load did not take";

  auto obj = bucket->get_object(rgw_obj_key("sideloaded.bin"));
  rgw_owner acl_owner_id = owner;
  ACLOwner acl_owner{acl_owner_id, ""};
  auto writer = driver->get_atomic_writer(env->dpp, null_yield, obj.get(),
					  acl_owner, nullptr, 0, testname);
  ASSERT_NE(writer.get(), nullptr);
  ASSERT_EQ(writer->prepare(null_yield), 0);

  bufferlist bl;
  bl.append("ours");
  const uint64_t len = bl.length();
  ASSERT_EQ(writer->process(std::move(bl), 0), 0);
  ASSERT_EQ(writer->process({}, len), 0);

  ceph::real_time mtime;
  Attrs attrs;
  std::string etag;
  req_context rctx{env->dpp, null_yield, nullptr};
  ASSERT_EQ(writer->complete(len, etag, &mtime, real_time(), attrs,
			     std::nullopt, real_time(), nullptr, nullptr,
			     nullptr, nullptr, nullptr, rctx, 0), 0);

  struct stat after{};
  ASSERT_EQ(::stat(p.c_str(), &after), 0);
  EXPECT_NE(after.st_ino, before.st_ino)
      << "the PUT wrote into the side-loaded file rather than replacing "
	 "it;  the mode narrowing in the write path is therefore not "
	 "redundant and must not be removed";
  EXPECT_EQ(after.st_mode & 07777, 0600u);
}

/* An object published through FSIO carries the mode an object carries.
 *
 * The NFS gateway writes through FSIO rather than through
 * NSFSAtomicWriter, so it reaches none of the three places the mode is
 * narrowed:  File::write(), UringWriteWindow and
 * NSFSAtomicWriter::complete().  Whatever mode the shadow was created
 * with is the mode the published object ends up with, because publish
 * renames the shadow into place.
 *
 * umask is pinned, and that is the whole reason this test can mean
 * anything.  umask only *clears* bits, so a shadow created 0644 lands
 * at 0600 under a umask of 077 and at 0644 under 022.  A developer
 * with a restrictive umask would see this pass while the defect was
 * there -- and so would a radosgw whose umask happened to be tight.
 */
TEST_F(NSFSBucketTest, AnFSIOPublishedObjectCarriesTheObjectMode)
{
  const mode_t saved = ::umask(022);
  auto restore = make_scope_guard([saved] { ::umask(saved); });

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  ASSERT_TRUE(b->has_extension(nsfs::EXT_SHADOW))
      << "nothing to test:  FSIO needs EXT_SHADOW";

  auto obj = bucket->get_object(rgw_obj_key("fsio-new.bin"));
  auto [rc, hdl] = obj->get_fsio_handle(
      env->dpp, rgw::sal::Object::FSIOObject::OPEN_FLAG_CREATE |
		rgw::sal::Object::FSIOObject::OPEN_FLAG_WRITE);
  ASSERT_EQ(rc, 0);
  ASSERT_NE(hdl.get(), nullptr);
  ASSERT_EQ(hdl->commit(env->dpp,
			rgw::sal::Object::FSIOObject::COMMIT_FLAG_NONE), 0);
  ASSERT_EQ(hdl->publish(env->dpp,
			 rgw::sal::Object::FSIOObject::PUBLISH_FLAG_NONE), 0);
  hdl->close(env->dpp, rgw::sal::Object::FSIOObject::CLOSE_FLAG_NONE);
  hdl.reset();

  const sf::path published{bucket_path() / "fsio-new.bin"};
  ASSERT_TRUE(sf::exists(published)) << "the handle published nothing";
  EXPECT_EQ(file_mode(published), 0600u)
      << "an object written through the NFS gateway is more permissive "
	 "than the same object written through S3";
}

/* And re-opening an existing object for write, which clones it.
 *
 * A second write handle on an object that already exists takes
 * FSStrategy::clone_file() rather than the O_TMPFILE branch, so the
 * shadow comes from a different create with its own mode.  Same
 * question, different line.
 */
TEST_F(NSFSBucketTest, AnFSIOClonedObjectCarriesTheObjectMode)
{
  const mode_t saved = ::umask(022);
  auto restore = make_scope_guard([saved] { ::umask(saved); });

  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_EQ(b->resolve_profile(env->dpp), 0);
  ASSERT_TRUE(b->has_extension(nsfs::EXT_SHADOW));

  const char* name = "fsio-clone.bin";
  for (int pass = 0; pass < 2; ++pass) {
    auto obj = bucket->get_object(rgw_obj_key(name));
    auto [rc, hdl] = obj->get_fsio_handle(
	env->dpp, rgw::sal::Object::FSIOObject::OPEN_FLAG_CREATE |
		  rgw::sal::Object::FSIOObject::OPEN_FLAG_WRITE);
    ASSERT_EQ(rc, 0) << "pass " << pass;
    ASSERT_NE(hdl.get(), nullptr);
    ASSERT_EQ(hdl->commit(env->dpp,
			  rgw::sal::Object::FSIOObject::COMMIT_FLAG_NONE), 0);
    ASSERT_EQ(hdl->publish(env->dpp,
			   rgw::sal::Object::FSIOObject::PUBLISH_FLAG_NONE), 0);
    hdl->close(env->dpp, rgw::sal::Object::FSIOObject::CLOSE_FLAG_NONE);
  }

  const sf::path published{bucket_path() / name};
  ASSERT_TRUE(sf::exists(published));
  EXPECT_EQ(file_mode(published), 0600u)
      << "the clone path left the object more permissive than an object "
	 "written through S3";
}

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
    their_object("obj-" + std::to_string(i) + ".bin", "text/plain");
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
  their_object("a.bin", "text/plain");
  their_object("b.bin", "text/plain");

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

/* Theirs does move one, because their file IS the size.
 *
 * `parts-size-17` at 17 * (num - 1) is the only place their reader
 * looks for a 17-byte part -- it derives the name from the part's own
 * record -- so a short final part left in the stride's file is one
 * their gateway cannot read, and one our own assembly cannot either,
 * since it implements their scheme.
 *
 * The record is asserted as well as the bytes.  The two together are
 * what makes the tree theirs;  bytes in the right file with a record
 * pointing at the wrong offset reads as zeros.
 */
TEST_F(NSFSNooBaaBucketTest, ShortFinalPartMovesToItsOwnSizeFile)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);

  const std::string objname = "short-tail.bin";
  const size_t stride = 64;
  const size_t tail = 17;

  /* the staging directory before it is cleaned up, so the layout can
   * be looked at:  the upload is driven by hand rather than through
   * the helper, which completes it */
  const std::string upload_id = "11111111-2222-4333-8444-555555555555";
  auto upload = bucket->get_multipart_upload(objname, upload_id);
  ASSERT_NE(upload.get(), nullptr);

  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  std::string expected;
  std::map<int, std::string> part_etags;
  for (int i = 1; i <= 3; ++i) {
    const size_t len = (i == 3) ? tail : stride;
    std::string payload(len, static_cast<char>('a' + i));
    part_etags[i] = write_mp_part(upload.get(), acl_owner, &placement, i,
				  payload);
    expected += payload;
  }

  sf::path staging;
  for (auto& e : sf::directory_iterator(bucket_path())) {
    if (e.path().filename().string().starts_with(".noobaa-nsfs_")) {
      staging = e.path() / "multipart-uploads" / upload_id;
    }
  }
  ASSERT_TRUE(sf::is_directory(staging)) << staging;

  const sf::path tail_file{staging / ("parts-size-" + std::to_string(tail))};
  ASSERT_TRUE(sf::exists(tail_file)) << "the short part was left in the "
				     << "stride's file";
  /* 17 * (3 - 1) = 34, and the part is the 17 bytes after it */
  const uint64_t tail_offset = tail * 2;
  ASSERT_EQ(sf::file_size(tail_file), tail_offset + tail);
  EXPECT_EQ(read_all(tail_file).substr(tail_offset, tail),
	    std::string(tail, 'd'));

  const sf::path rec{staging / ("part-" + std::to_string(3))};
  EXPECT_EQ(str_xattr(rec, "user.noobaa.part_size"), std::to_string(tail));
  EXPECT_EQ(str_xattr(rec, "user.noobaa.part_offset"),
	    std::to_string(tail_offset));

  /* and the two uniform parts are still in the stride's file */
  const sf::path body{staging / ("parts-size-" + std::to_string(stride))};
  ASSERT_TRUE(sf::exists(body)) << body;
  EXPECT_EQ(str_xattr(staging / "part-2", "user.noobaa.part_offset"),
	    std::to_string(stride));

  ASSERT_EQ(complete_mp(bucket.get(), upload.get(), owner, objname,
			part_etags), 0);
  EXPECT_EQ(read_all(bucket_path() / objname), expected);
}

/* A tree with no temp directory at all:  one is created, because
 * there is nothing to conflict with. */
TEST_F(NSFSNooBaaBucketTest, CreatesATempDirectoryWhenThereIsNone)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::empty, tree);

  auto upload = bucket->get_multipart_upload("k", "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
  rgw_placement_rule placement;
  Attrs attrs;
  ASSERT_EQ(upload->init(env->dpp, null_yield, acl_owner, placement, attrs), 0);

  int n = 0;
  for (auto& e : sf::directory_iterator(bucket_path())) {
    if (e.path().filename().string().starts_with(".noobaa-nsfs_")) {
      ++n;
    }
  }
  EXPECT_EQ(n, 1);
}

/* Two temp directories means two bucket ids have written here, and
 * which holds the uploads is not decidable.  Refused rather than
 * guessed -- and no third is created. */
TEST_F(NSFSNooBaaBucketTest, RefusesAnAmbiguousTree)
{
  TestTree tree;
  make_noobaa_tree(bucket_path(), TreeShape::quiescent, tree);
  /* uuid-shaped, because that is what the strategy recognises */
  sf::create_directories(bucket_path() /
			 (".noobaa-nsfs_" + fake_uuid("someone-else")));

  auto upload = bucket->get_multipart_upload("k", "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
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
 * Three instantiations:  our format on a filesystem whose extents can
 * be shared, so parts land in their own files;  our format where they
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

    /* Quiescent:  written to, no upload.  Ours has no such state and
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
				      nullptr, &truncated, null_yield), 0);
    EXPECT_FALSE(truncated);
    return uploads.size();
  }
};

/* uuid-shaped, and used by every case:  neither layout parses an
 * upload id -- ours url-encodes `<key>.<id>` into a directory name,
 * theirs names the directory for the id alone -- so one id serves all
 * three without saying anything untrue about either */
static const std::string uniform_upload_id{
  "11111111-2222-4333-8444-555555555555"};

TEST_P(NSFSFormatTest, MultipartRoundTrip)
{
  auto* b = static_cast<rgw::sal::NSFSBucket*>(bucket.get());
  ASSERT_STREQ(b->mpu_strategy()->name(), GetParam().mpu_name);

  const std::string objname = "written/by/us.bin";
  auto upload = bucket->get_multipart_upload(objname, uniform_upload_id);
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
  auto upload = bucket->get_multipart_upload(objname, uniform_upload_id);
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

INSTANTIATE_TEST_SUITE_P(
    Formats, NSFSFormatTest,
    ::testing::Values(
	FormatCase{"rgw", true, nsfs::EXTENSIONS_STRONG, "rgw",
		   make_nsfs_tree},
	FormatCase{"rgwstrided", false, nsfs::EXTENSIONS_STRONG,
		   "rgw-strided", make_nsfs_tree},
	/* the format names the layout, so the probe does not reach it;
	 * forced true all the same, so the case does not vary with the
	 * filesystem the build directory is on */
	FormatCase{"noobaa", true, nsfs::EXTENSIONS_BASE, "noobaa",
		   make_noobaa_tree}),
    [](const ::testing::TestParamInfo<FormatCase>& i) {
      return std::string(i.param.label);
    });


/* ------------------------------------------------------------------ *
 * I1 -- reading NooBaa's configuration store.
 *
 * The records below are **verbatim what their CLI and gateway wrote**,
 * copied out of a store stood up at noobaa-core 08d063f86 by
 * `src/test/rgw/interchange/standup.rs`.  That matters:  every earlier
 * control in the import work was composed from our reading of their
 * schema, so it demonstrated that we read what we *believe* they
 * write.  These are their bytes.  The directory is the test's, the
 * content is not.
 * ------------------------------------------------------------------ */

/* A root account with credentials.  Eight required fields and nothing
 * optional, which is what `account add` with no extra flags produces. */
static constexpr const char* NB_ACCOUNT_PLAIN = R"({
  "_id": "6ac7cb27fd4af356919110a3",
  "name": "interchange",
  "email": "interchange",
  "creation_date": "2026-10-08T16:56:07.864Z",
  "access_keys": [
    {
      "access_key": "INTERCHANGEACCESSKEY",
      "encrypted_secret_key": "AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51FijvRm3nJgdKQ=="
    }
  ],
  "nsfs_account_config": {
    "uid": 1000,
    "gid": 1000,
    "new_buckets_path": "/lv2tb/nsfs-interchange/data/root"
  },
  "allow_bucket_creation": true,
  "master_key_id": "6ac7cb2751b72219a4bae375"
})";

/* The same, plus the two optional fields that change behaviour:  an
 * explicit supplemental group list, and force_md5_etag. */
static constexpr const char* NB_ACCOUNT_GROUPS = R"({
  "_id": "6ac7d2121e63a748e37663e0",
  "name": "ix-groups",
  "email": "ix-groups",
  "creation_date": "2026-10-08T17:25:38.981Z",
  "access_keys": [
    {
      "access_key": "IXGROUPSACCESSKEY001",
      "encrypted_secret_key": "DEi0ds53sqdzVH3H8QM7FhsgNwouyCHIXK+RHH5n1lmpqxuhm5YbIg=="
    }
  ],
  "force_md5_etag": true,
  "nsfs_account_config": {
    "uid": 1000,
    "gid": 1000,
    "new_buckets_path": "/lv2tb/nsfs-interchange/data/root",
    "supplemental_groups": [1000, 4, 27]
  },
  "allow_bucket_creation": true,
  "master_key_id": "6ac7cb2751b72219a4bae375"
})";

/* The anonymous account, `account add --anonymous`.  Note what it is
 * and is not:  an empty key list, no new_buckets_path, bucket creation
 * off -- and it still carries a master_key_id. */
static constexpr const char* NB_ACCOUNT_ANONYMOUS = R"({
  "_id": "6ac7d213cac521f05aac9fb4",
  "name": "anonymous",
  "email": "anonymous",
  "creation_date": "2026-10-08T17:25:39.650Z",
  "access_keys": [],
  "nsfs_account_config": {
    "uid": 65534,
    "gid": 65534
  },
  "allow_bucket_creation": false,
  "master_key_id": "6ac7cb2751b72219a4bae375"
})";

class NooBaaStoreTest : public NSFSDriverTest {
protected:
  std::string store;

public:
  void SetUp() {
    NSFSDriverTest::SetUp();
    store = (bp / "noobaa.conf.d").string();
    std::filesystem::create_directories(store + "/identities");
    std::filesystem::create_directories(store + "/buckets");
  }

  void write_file(const std::string& path, const std::string& text) {
    std::filesystem::create_directories(
	std::filesystem::path(path).parent_path());
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    ASSERT_GE(fd, 0) << path << ": " << cpp_strerror(errno);
    ASSERT_EQ(::write(fd, text.data(), text.size()),
	      static_cast<ssize_t>(text.size()));
    ::close(fd);
  }

  /* Their layout:  a directory per identity holding identity.json, not
   * a file per identity.  Read off a store their CLI wrote. */
  void add_identity(const std::string& id, const char* record) {
    write_file(store + "/identities/" + id + "/identity.json", record);
  }

  void add_bucket(const std::string& name, const std::string& record) {
    write_file(store + "/buckets/" + name + ".json", record);
  }

  nsfs::NooBaaStoreContents read_store() {
    nsfs::NooBaaConfigStore cs(store);
    nsfs::NooBaaStoreContents out;
    EXPECT_EQ(cs.read(env->dpp, out), 0);
    return out;
  }

  const nsfs::NooBaaIdentity* by_name(const nsfs::NooBaaStoreContents& c,
				      const std::string& name) {
    for (const auto& i : c.identities) {
      if (i.name == name) {
	return &i;
      }
    }
    return nullptr;
  }
};

/* The three records their CLI wrote, read back whole.
 *
 * One test over all three rather than one each, because the thing
 * being established is that a store reads as a store. */
TEST_F(NooBaaStoreTest, AStoreTheirToolsWroteReadsBack)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("6ac7d2121e63a748e37663e0", NB_ACCOUNT_GROUPS);
  add_identity("6ac7d213cac521f05aac9fb4", NB_ACCOUNT_ANONYMOUS);
  add_bucket("a-bucket", R"({"_id":"1","name":"a-bucket","versioning":"ENABLED"})");
  write_file(store + "/master_keys.json", R"({"active_master_key":"x"})");

  auto c = read_store();
  EXPECT_TRUE(c.unreadable.empty()) << c.unreadable[0].path << ": "
				    << c.unreadable[0].why;
  EXPECT_EQ(c.identities.size(), 3u);
  EXPECT_EQ(c.accounts(), 3u);
  EXPECT_EQ(c.iam_users(), 0u);
  EXPECT_EQ(c.buckets.size(), 1u);
  EXPECT_EQ(c.buckets[0].name, "a-bucket");
  EXPECT_TRUE(c.master_keys_present);

  const auto* plain = by_name(c, "interchange");
  ASSERT_NE(plain, nullptr);
  EXPECT_EQ(plain->id, "6ac7cb27fd4af356919110a3");
  /* their email is a copy of the name, which is I3's likeliest
   * collision against RGW's cross-account email uniqueness */
  EXPECT_EQ(plain->email, "interchange");
  EXPECT_EQ(plain->master_key_id, "6ac7cb2751b72219a4bae375");
  EXPECT_TRUE(plain->allow_bucket_creation);
  ASSERT_EQ(plain->access_keys.size(), 1u);
  EXPECT_EQ(plain->access_keys[0].access_key, "INTERCHANGEACCESSKEY");
  /* the secret is still theirs, encrypted -- decrypting is I2 */
  EXPECT_EQ(plain->access_keys[0].encrypted_secret_key,
	    "AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51FijvRm3nJgdKQ==");
  EXPECT_EQ(plain->posix.kind, nsfs::NooBaaPosixConfig::Kind::Ids);
  EXPECT_EQ(plain->posix.uid, 1000);
  EXPECT_EQ(plain->posix.gid, 1000);
  EXPECT_EQ(plain->posix.new_buckets_path,
	    "/lv2tb/nsfs-interchange/data/root");
  EXPECT_FALSE(plain->have_force_md5_etag);
  EXPECT_FALSE(plain->posix.have_supplemental_groups);
  EXPECT_FALSE(plain->is_anonymous());
  EXPECT_FALSE(plain->is_iam_user());

  const auto* groups = by_name(c, "ix-groups");
  ASSERT_NE(groups, nullptr);
  EXPECT_TRUE(groups->have_force_md5_etag);
  EXPECT_TRUE(groups->force_md5_etag);
  ASSERT_TRUE(groups->posix.have_supplemental_groups);
  EXPECT_EQ(groups->posix.supplemental_groups,
	    (std::vector<int64_t>{1000, 4, 27}));

  const auto* anon = by_name(c, "anonymous");
  ASSERT_NE(anon, nullptr);
  EXPECT_TRUE(anon->is_anonymous());
  EXPECT_TRUE(anon->access_keys.empty());
  EXPECT_FALSE(anon->allow_bucket_creation);
  EXPECT_TRUE(anon->posix.new_buckets_path.empty());
  EXPECT_EQ(anon->posix.uid, 65534);
  /* it carries one even with no secret to protect */
  EXPECT_FALSE(anon->master_key_id.empty());
}

/* A record that will not parse is reported, and the rest are still
 * read.
 *
 * The failure mode this exists for:  an import that discovered its
 * third bad record after writing two accounts leaves a state nobody
 * asked for.  So the read must report everything wrong with a store in
 * one pass.
 *
 * Both polarities, and the negative case is reachable:  a reader that
 * stopped at the first bad record would return fewer than two good
 * identities, and a reader that ignored bad records would report none
 * unreadable. */
TEST_F(NooBaaStoreTest, EveryUnreadableRecordIsReportedAndTheRestAreRead)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("aaaaaaaaaaaaaaaaaaaaaaaa", "{ this is not json");
  add_identity("6ac7d2121e63a748e37663e0", NB_ACCOUNT_GROUPS);
  /* right shape, wrong type:  JSONDecoder throws rather than the
   * parser failing, which is a different path through the reader */
  add_identity("bbbbbbbbbbbbbbbbbbbbbbbb",
	       R"({"_id":"b","name":"n","nsfs_account_config":{"uid":"ten","gid":1}})");
  /* no identifier:  parses, and cannot be imported */
  add_identity("cccccccccccccccccccccccc", R"({"name":"no-id"})");
  add_bucket("good", R"({"_id":"1","name":"good"})");
  add_bucket("bad", "}{");

  auto c = read_store();
  EXPECT_EQ(c.identities.size(), 2u)
      << "a bad record stopped the traversal";
  EXPECT_EQ(c.unreadable.size(), 4u) << [&] {
    std::string s;
    for (const auto& u : c.unreadable) {
      s += "\n  " + u.path + ": " + u.why;
    }
    return s;
  }();
  EXPECT_EQ(c.buckets.size(), 1u);
  EXPECT_EQ(c.buckets[0].name, "good");

  /* each complaint names the file, which is what makes a preflight
   * report actionable rather than a count */
  for (const auto& u : c.unreadable) {
    EXPECT_FALSE(u.path.empty());
    EXPECT_FALSE(u.why.empty());
  }
}

/* An absent supplemental group list is not an empty one.
 *
 * §2.4:  an explicit list overrides the directory entirely, and with
 * no list and no dynamic resolution there are *no* supplementary
 * groups at all.  Violating this one grants access, so the reader has
 * to distinguish "no list" from "an empty list" -- which a bare
 * vector cannot. */
TEST_F(NooBaaStoreTest, AnAbsentGroupListIsNotAnEmptyOne)
{
  add_identity("a", NB_ACCOUNT_PLAIN);
  add_identity("b", R"({"_id":"b","name":"empty-list",
    "nsfs_account_config":{"uid":1,"gid":1,"supplemental_groups":[]}})");

  auto c = read_store();
  ASSERT_EQ(c.identities.size(), 2u);

  const auto* absent = by_name(c, "interchange");
  ASSERT_NE(absent, nullptr);
  EXPECT_FALSE(absent->posix.have_supplemental_groups);
  EXPECT_TRUE(absent->posix.supplemental_groups.empty());

  const auto* empty = by_name(c, "empty-list");
  ASSERT_NE(empty, nullptr);
  EXPECT_TRUE(empty->posix.have_supplemental_groups)
      << "an explicit empty list read as no list at all";
  EXPECT_TRUE(empty->posix.supplemental_groups.empty());
}

/* A missing config.json is not a stored false.
 *
 * NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_GROUPS defaults to false in their
 * config.js, and false is a live permission fact:  with it off, a
 * directory-backed identity carrying no list runs with zero
 * supplementary groups.  So "the file did not say" and "the file said
 * false" must not arrive as the same answer -- §2.5 point 4 is that
 * the value travels with the deployment. */
TEST_F(NooBaaStoreTest, AMissingSettingIsNotAStoredFalse)
{
  add_identity("a", NB_ACCOUNT_PLAIN);

  auto absent = read_store();
  EXPECT_FALSE(absent.settings.have_config_json);
  EXPECT_FALSE(absent.settings.have_dynamic_supplemental_groups);
  EXPECT_FALSE(absent.settings.dynamic_supplemental_groups);

  write_file(store + "/config.json",
	     R"({"NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_GROUPS": false})");
  auto stored_false = read_store();
  EXPECT_TRUE(stored_false.settings.have_config_json);
  EXPECT_TRUE(stored_false.settings.have_dynamic_supplemental_groups)
      << "a stored false is indistinguishable from the file being absent";
  EXPECT_FALSE(stored_false.settings.dynamic_supplemental_groups);

  write_file(store + "/config.json",
	     R"({"NSFS_ENABLE_DYNAMIC_SUPPLEMENTAL_GROUPS": true})");
  auto stored_true = read_store();
  EXPECT_TRUE(stored_true.settings.have_dynamic_supplemental_groups);
  EXPECT_TRUE(stored_true.settings.dynamic_supplemental_groups);

  /* a config.json that exists and does not mention it */
  write_file(store + "/config.json", R"({"ALLOW_HTTP": true})");
  auto silent = read_store();
  EXPECT_TRUE(silent.settings.have_config_json);
  EXPECT_FALSE(silent.settings.have_dynamic_supplemental_groups);
}

/* uid-and-gid and distinguished_name are different identities.
 *
 * Their schema is a `oneOf` and the two resolve groups differently
 * (§2.3), so flattening both into an optional uid would lose which
 * one a record was. */
TEST_F(NooBaaStoreTest, ADistinguishedNameIdentityIsNotAUidOne)
{
  add_identity("a", NB_ACCOUNT_PLAIN);
  add_identity("b", R"({"_id":"b","name":"dn-backed",
    "nsfs_account_config":{"distinguished_name":"uid=jo,ou=people,dc=x"}})");
  add_identity("c", R"({"_id":"c","name":"no-posix-config"})");

  auto c = read_store();
  ASSERT_EQ(c.identities.size(), 3u);

  EXPECT_EQ(by_name(c, "interchange")->posix.kind,
	    nsfs::NooBaaPosixConfig::Kind::Ids);

  const auto* dn = by_name(c, "dn-backed");
  ASSERT_NE(dn, nullptr);
  EXPECT_EQ(dn->posix.kind, nsfs::NooBaaPosixConfig::Kind::DistinguishedName);
  EXPECT_EQ(dn->posix.distinguished_name, "uid=jo,ou=people,dc=x");
  EXPECT_EQ(dn->posix.uid, -1) << "a dn identity was given a uid";

  EXPECT_EQ(by_name(c, "no-posix-config")->posix.kind,
	    nsfs::NooBaaPosixConfig::Kind::Unset);
}

/* An IAM user is the one with an owner, and a role is the one with a
 * trust policy.
 *
 * One schema serves accounts, IAM users and roles, so the reader has
 * to tell them apart from the record rather than from where it sat
 * (§2.8).  The trust policy is kept as text:  translating it is I4. */
TEST_F(NooBaaStoreTest, AnIamUserIsTheOneWithAnOwner)
{
  add_identity("a", NB_ACCOUNT_PLAIN);
  add_identity("b", R"({"_id":"b","name":"a-user","email":"u@x",
    "owner":"6ac7cb27fd4af356919110a3","creator":"6ac7cb27fd4af356919110a3",
    "iam_path":"/eng/"})");
  add_identity("c", R"({"_id":"c","name":"a-role",
    "role_config":{"role_name":"r","assume_role_policy_document":{
      "Version":"2012-10-17","Statement":[{"Effect":"Allow",
      "Principal":{"AWS":["u@x"]},"Action":["sts:AssumeRole"]}]}}})");

  auto c = read_store();
  ASSERT_EQ(c.identities.size(), 3u);
  EXPECT_EQ(c.accounts(), 2u);
  EXPECT_EQ(c.iam_users(), 1u);

  const auto* user = by_name(c, "a-user");
  ASSERT_NE(user, nullptr);
  EXPECT_TRUE(user->is_iam_user());
  EXPECT_EQ(user->owner, "6ac7cb27fd4af356919110a3");
  EXPECT_EQ(user->iam_path, "/eng/");

  const auto* role = by_name(c, "a-role");
  ASSERT_NE(role, nullptr);
  EXPECT_FALSE(role->is_iam_user());
  EXPECT_FALSE(role->role_config.empty())
      << "the trust policy was dropped";
  EXPECT_NE(role->role_config.find("sts:AssumeRole"), std::string::npos);
}

/* A store that is not there, and one that is not a directory.
 *
 * Distinguished, because they mean different things to an operator:
 * one is a path that was never created, the other is a path that names
 * something else. */
TEST_F(NooBaaStoreTest, ADirectoryThatIsNotAStoreIsRefused)
{
  nsfs::NooBaaStoreContents out;

  nsfs::NooBaaConfigStore missing((bp / "nowhere").string());
  EXPECT_EQ(missing.read(env->dpp, out), -ENOENT);

  const std::string file = (bp / "a-file").string();
  write_file(file, "{}");
  nsfs::NooBaaConfigStore notdir(file);
  EXPECT_EQ(notdir.read(env->dpp, out), -ENOTDIR);

  nsfs::NooBaaConfigStore nowhere("");
  EXPECT_EQ(nowhere.read(env->dpp, out), -ENOENT);
}

/* A directory with no identities is reported rather than read as
 * empty.
 *
 * An import with nothing to import is almost certainly pointed at the
 * wrong directory, and saying so costs one line.  Their lock files
 * sitting beside the bucket records are not records and must not be
 * counted as unreadable ones. */
TEST_F(NooBaaStoreTest, AStoreWithNoIdentitiesSaysSo)
{
  std::filesystem::remove_all(store + "/identities");
  add_bucket("b", R"({"_id":"1","name":"b"})");
  write_file(store + "/buckets/b.json.lock", "");

  auto c = read_store();
  EXPECT_TRUE(c.identities.empty());
  ASSERT_EQ(c.unreadable.size(), 1u);
  EXPECT_NE(c.unreadable[0].why.find("identities"), std::string::npos);
  EXPECT_EQ(c.buckets.size(), 1u) << "a lock file was read as a record";
}

/* A store on disk, read without complaint.
 *
 * `NSFS_NOOBAA_STORE` names a real NooBaa configuration directory and
 * this reads it.  Skipped when unset, the way the suite's other
 * tests that need something stood up behave -- so it is a no-op
 * everywhere except on a machine where
 * `src/test/rgw/interchange/standup.rs` has run.
 *
 * The fixtures above are their bytes but the test's layout.  This is
 * the other way round, and it is what catches an assumption about the
 * tree rather than about a record:  that identities are directories,
 * that a `.json.lock` beside a record is not a record, that the
 * symlink indexes are elsewhere. */
TEST_F(NooBaaStoreTest, AStoreOnDiskReadsWithoutComplaint)
{
  const char* dir = ::getenv("NSFS_NOOBAA_STORE");
  if (!dir || !*dir) {
    GTEST_SKIP() << "NSFS_NOOBAA_STORE is unset;  see "
		 << "src/test/rgw/interchange/standup.rs";
  }

  nsfs::NooBaaConfigStore cs(dir);
  nsfs::NooBaaStoreContents c;
  ASSERT_EQ(cs.read(env->dpp, c), 0);

  std::string complaints;
  for (const auto& u : c.unreadable) {
    complaints += "\n  " + u.path + ": " + u.why;
  }
  EXPECT_TRUE(c.unreadable.empty()) << complaints;

  /* A store with no identities is not one an import can act on, and
   * reading one as success would make this test vacuous. */
  EXPECT_GT(c.identities.size(), 0u);
  EXPECT_TRUE(c.master_keys_present)
      << "no master_keys.json, so no secret could be decrypted";

  for (const auto& i : c.identities) {
    EXPECT_FALSE(i.id.empty());
    EXPECT_FALSE(i.name.empty());
    /* Every account at this pin has access_keys and master_key_id --
     * eight required fields, and those two among them.  The anonymous
     * account is the one with an empty key list, not a missing one. */
    EXPECT_FALSE(i.master_key_id.empty()) << i.name;
    if (!i.is_anonymous() && !i.is_iam_user()) {
      EXPECT_FALSE(i.access_keys.empty()) << i.name;
    }
  }
}

/* Nothing in the store is written, which is the whole of I1's
 * contract.
 *
 * Checked by mtime over every file and directory in the store rather
 * than by reading the code:  a read that opened a record O_RDWR, or
 * created an index, or dropped a lock file, would change one.  The
 * negative case is reachable -- the same comparison catches a write
 * made deliberately below. */
TEST_F(NooBaaStoreTest, ReadingTheStoreWritesNothing)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("6ac7d213cac521f05aac9fb4", NB_ACCOUNT_ANONYMOUS);
  add_bucket("a-bucket", R"({"_id":"1","name":"a-bucket"})");
  write_file(store + "/config.json", R"({"ALLOW_HTTP": true})");
  write_file(store + "/master_keys.json", R"({"active_master_key":"x"})");

  auto snapshot = [this] {
    std::map<std::string, std::pair<time_t, off_t>> m;
    for (const auto& e :
	 std::filesystem::recursive_directory_iterator(store)) {
      struct stat st;
      if (::lstat(e.path().c_str(), &st) == 0) {
	m[e.path().string()] = {st.st_mtime, st.st_size};
      }
    }
    return m;
  };

  const auto before = snapshot();
  ASSERT_FALSE(before.empty());

  auto c = read_store();
  ASSERT_TRUE(c.unreadable.empty());
  EXPECT_EQ(snapshot(), before) << "reading the store changed it";

  /* the comparison can fail:  one deliberate write, detected */
  write_file(store + "/buckets/a-bucket.json",
	     R"({"_id":"1","name":"a-bucket","versioning":"ENABLED"})");
  EXPECT_NE(snapshot(), before)
      << "the mtime comparison cannot detect a write, so it proves nothing";
}

/* ------------------------------------------------------------------ *
 * I2 -- the master key, and the secrets it opens.
 *
 * The master key below is **the one NooBaa generated** in the store
 * `standup.rs` stood up, and `NB_ACCOUNT_PLAIN` above carries the
 * ciphertext **their CLI produced** under it.  The plaintext is known
 * because the stand-up chose the secret:  so these tests decrypt real
 * NooBaa output and compare against a known answer, which is the
 * strongest control available for this stage and the one §9 asks for.
 * ------------------------------------------------------------------ */

static constexpr const char* NB_MASTER_KEYS = R"({
  "timestamp": 1791478567852,
  "active_master_key": "6ac7cb2751b72219a4bae375",
  "master_keys_by_id": {
    "6ac7cb2751b72219a4bae375": {
      "id": "6ac7cb2751b72219a4bae375",
      "cipher_key": "I/WLyx1+z7KYouUZAwp6b7WT9QdBFqstDu+67OPlnX4=",
      "cipher_iv": "EUWSw9er4NMWTkuJkB2PsQ==",
      "encryption_type": "aes-256-gcm"
    }
  }
})";

/* what `account add --secret_key` was given */
static constexpr const char* NB_PLAIN_SECRET =
  "interchangesecretkeyinterchangesecretkey";

class NooBaaKeysTest : public NooBaaStoreTest {
public:
  nsfs::NooBaaSettings file_store() {
    nsfs::NooBaaSettings s;
    s.master_keys_store_type = "file";
    return s;
  }

  nsfs::NooBaaMasterKeys load(const nsfs::NooBaaSettings& s, int expect = 0) {
    nsfs::NooBaaMasterKeys keys;
    EXPECT_EQ(keys.load(env->dpp, s, store), expect);
    return keys;
  }
};

/* The plausibility check, which is the only thing that reports a wrong
 * master key.
 *
 * Pure, so both polarities are here without a cipher.  Their
 * construction computes no authentication tag, so a wrong key
 * decrypts to keystream of exactly the right length and the cipher
 * reports success -- this function is what stands between that and an
 * import full of credentials that do not authenticate. */
TEST(NooBaaSecretShape, OnlyACredentialIsPlausible)
{
  EXPECT_TRUE(nsfs::secret_is_plausible(NB_PLAIN_SECRET));
  EXPECT_TRUE(nsfs::secret_is_plausible(
      "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="));
  EXPECT_TRUE(nsfs::secret_is_plausible("12345678"));

  /* keystream:  what a wrong key actually produced against the real
   * ciphertext, measured before this was written */
  EXPECT_FALSE(nsfs::secret_is_plausible(
      std::string("\x19\x21\xb7\xe7o'9Y\x8a\x83;\xb8\xf2""aPp", 16)));
  /* a single control character is enough, which is the point */
  EXPECT_FALSE(nsfs::secret_is_plausible(std::string("abcdefgh\x01", 9)));
  EXPECT_FALSE(nsfs::secret_is_plausible(std::string("abcdefgh\0", 9)));
  EXPECT_FALSE(nsfs::secret_is_plausible(""));
  EXPECT_FALSE(nsfs::secret_is_plausible("short"));
  EXPECT_FALSE(nsfs::secret_is_plausible(std::string(257, 'a')));
}

/* The real key opens the real ciphertext.
 *
 * **This is also the IV-length control.**  Their IV is 16 bytes and
 * GCM's default is 12;  a decryption that did not set the length
 * explicitly would run a different construction and this comparison
 * would fail rather than merely warn. */
TEST_F(NooBaaKeysTest, TheirKeyOpensTheirSecret)
{
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);
  auto keys = load(file_store());
  ASSERT_EQ(keys.size(), 1u);
  EXPECT_EQ(keys.active(), "6ac7cb2751b72219a4bae375");
  EXPECT_TRUE(keys.have("6ac7cb2751b72219a4bae375"));

  std::string secret;
  ASSERT_EQ(keys.decrypt(env->dpp, "6ac7cb2751b72219a4bae375",
			 "AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51Fijv"
			 "Rm3nJgdKQ==", secret), 0);
  EXPECT_EQ(secret, NB_PLAIN_SECRET);
}

/* A wrong key is detected, and that is the stage.
 *
 * The cipher cannot report it -- no tag is computed -- so the only
 * thing that can is what came out.  Both polarities against one
 * ciphertext:  the right key yields the secret, a key of the right
 * shape and the wrong bytes is refused with -EKEYREJECTED rather than
 * returning garbage a caller would store. */
TEST_F(NooBaaKeysTest, AWrongKeyIsDetectedRatherThanReturned)
{
  const std::string ct = "AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51Fijv"
			 "Rm3nJgdKQ==";
  const std::string id = "6ac7cb2751b72219a4bae375";

  write_file(store + "/master_keys.json", NB_MASTER_KEYS);
  std::string good;
  ASSERT_EQ(load(file_store()).decrypt(env->dpp, id, ct, good), 0);
  ASSERT_EQ(good, NB_PLAIN_SECRET);

  /* the same store with 32 zero bytes for a key:  valid base64, right
   * length, wrong key */
  std::string wrong{NB_MASTER_KEYS};
  const std::string zeros = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  auto at = wrong.find("I/WLyx1+z7KYouUZAwp6b7WT9QdBFqstDu+67OPlnX4=");
  ASSERT_NE(at, std::string::npos);
  wrong.replace(at, 44, zeros);
  write_file(store + "/master_keys.json", wrong);

  std::string bad{"untouched"};
  EXPECT_EQ(load(file_store()).decrypt(env->dpp, id, ct, bad),
	    -EKEYREJECTED);
  EXPECT_TRUE(bad.empty()) << "a rejected secret was still handed back";
}

/* The executable store form.
 *
 * `NC_MASTER_KEYS_STORE_TYPE` may be `executable`, and then no key
 * file exists at all -- a reader that assumed `master_keys.json` would
 * report a broken deployment rather than a differently configured one.
 * The same key, delivered on standard output, opens the same secret. */
TEST_F(NooBaaKeysTest, TheKeysMayComeFromAProgram)
{
  const std::string prog = (bp / "say-keys").string();
  write_file(prog, std::string("#!/bin/sh\ncat <<'EOJ'\n") +
		   NB_MASTER_KEYS + "\nEOJ\n");
  ASSERT_EQ(::chmod(prog.c_str(), 0700), 0);

  nsfs::NooBaaSettings s;
  s.master_keys_store_type = "executable";
  s.master_keys_get_executable = prog;

  /* and no key file:  this must not be reading one by accident */
  ASSERT_FALSE(std::filesystem::exists(store + "/master_keys.json"));

  auto keys = load(s);
  std::string secret;
  ASSERT_EQ(keys.decrypt(env->dpp, "6ac7cb2751b72219a4bae375",
			 "AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51Fijv"
			 "Rm3nJgdKQ==", secret), 0);
  EXPECT_EQ(secret, NB_PLAIN_SECRET);

  /* a program that fails is not an empty key store */
  nsfs::NooBaaSettings broken = s;
  broken.master_keys_get_executable = (bp / "no-such-program").string();
  nsfs::NooBaaMasterKeys none;
  EXPECT_LT(none.load(env->dpp, broken, store), 0);
  EXPECT_TRUE(none.empty());

  nsfs::NooBaaSettings unset;
  unset.master_keys_store_type = "executable";
  EXPECT_EQ(none.load(env->dpp, unset, store), -ENOENT);
}

/* A store this cannot read says so, each in its own way.
 *
 * Distinguished because they mean different things:  a cipher we do
 * not implement is a deployment we cannot import, a corrupt file is a
 * file, and a key a record names and the store lacks is §5's hard
 * failure with nothing to fall back to. */
TEST_F(NooBaaKeysTest, AStoreThisCannotUseIsRefusedSpecifically)
{
  nsfs::NooBaaMasterKeys keys;

  EXPECT_EQ(keys.load(env->dpp, file_store(), store), -ENOENT);

  write_file(store + "/master_keys.json", "{ not json");
  EXPECT_EQ(keys.load(env->dpp, file_store(), store), -EBADMSG);

  write_file(store + "/master_keys.json", R"({"active_master_key":"x"})");
  EXPECT_EQ(keys.load(env->dpp, file_store(), store), -ENOKEY);

  /* a cipher we do not implement.  Refused rather than attempted:
   * decrypting it as though it were theirs would produce garbage, and
   * then the plausibility check would be the only thing to notice. */
  std::string other{NB_MASTER_KEYS};
  auto at = other.find("aes-256-gcm");
  ASSERT_NE(at, std::string::npos);
  other.replace(at, 11, "aes-256-ctr");
  write_file(store + "/master_keys.json", other);
  EXPECT_EQ(keys.load(env->dpp, file_store(), store), -EPROTONOSUPPORT);

  /* a 16-byte key, which is not AES-256 */
  std::string shortkey{NB_MASTER_KEYS};
  at = shortkey.find("I/WLyx1+z7KYouUZAwp6b7WT9QdBFqstDu+67OPlnX4=");
  ASSERT_NE(at, std::string::npos);
  shortkey.replace(at, 44, "AAAAAAAAAAAAAAAAAAAAAA==");
  write_file(store + "/master_keys.json", shortkey);
  EXPECT_EQ(keys.load(env->dpp, file_store(), store), -EBADMSG);

  /* a key a record names and the store does not hold */
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);
  auto good = load(file_store());
  std::string secret;
  EXPECT_EQ(good.decrypt(env->dpp, "6ac7cb2751b72219a4bae999",
			 "AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51Fijv"
			 "Rm3nJgdKQ==", secret), -ENOKEY);

  /* ciphertext that is not base64 */
  EXPECT_EQ(good.decrypt(env->dpp, "6ac7cb2751b72219a4bae375", "", secret),
	    -EINVAL);
}

/* Every secret in a store, opened in one pass, and every failure
 * reported.
 *
 * Collected rather than stopped at the first, for the reason I1
 * collects unreadable records -- one run should say everything that is
 * wrong -- and still an error, so no caller proceeds past a wrong key
 * by ignoring the list. */
TEST_F(NooBaaKeysTest, EverySecretOpensOrTheImportRefuses)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("6ac7d213cac521f05aac9fb4", NB_ACCOUNT_ANONYMOUS);
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);

  auto c = read_store();
  ASSERT_EQ(c.identities.size(), 2u);
  auto keys = load(file_store());

  std::vector<std::string> rejected;
  ASSERT_EQ(nsfs::decrypt_store_secrets(env->dpp, keys, c, rejected), 0)
      << rejected.size() << " rejected, first: "
      << (rejected.empty() ? std::string{} : rejected[0]);

  const auto* plain = by_name(c, "interchange");
  ASSERT_NE(plain, nullptr);
  ASSERT_EQ(plain->access_keys.size(), 1u);
  EXPECT_EQ(plain->access_keys[0].secret_key, NB_PLAIN_SECRET);
  /* the ciphertext is still there:  nothing is rewritten in place */
  EXPECT_FALSE(plain->access_keys[0].encrypted_secret_key.empty());

  /* the anonymous account has no keys, so there is nothing to open and
   * that is not a failure */
  const auto* anon = by_name(c, "anonymous");
  ASSERT_NE(anon, nullptr);
  EXPECT_TRUE(anon->access_keys.empty());

  /* and the negative case:  the wrong key refuses the whole store
   * rather than importing two accounts with garbage credentials */
  std::string wrong{NB_MASTER_KEYS};
  auto at = wrong.find("I/WLyx1+z7KYouUZAwp6b7WT9QdBFqstDu+67OPlnX4=");
  ASSERT_NE(at, std::string::npos);
  wrong.replace(at, 44, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
  write_file(store + "/master_keys.json", wrong);

  auto c2 = read_store();
  auto badkeys = load(file_store());
  std::vector<std::string> rejected2;
  EXPECT_EQ(nsfs::decrypt_store_secrets(env->dpp, badkeys, c2, rejected2),
	    -EKEYREJECTED);
  ASSERT_EQ(rejected2.size(), 1u);
  EXPECT_NE(rejected2[0].find("interchange"), std::string::npos)
      << "the report does not name the identity: " << rejected2[0];
  for (const auto& i : c2.identities) {
    for (const auto& ak : i.access_keys) {
      EXPECT_TRUE(ak.secret_key.empty())
	  << i.name << " was given a secret from a rejected key";
    }
  }
}

/* A real store on disk, opened with its own key.
 *
 * `NSFS_NOOBAA_STORE` names a NooBaa configuration directory;  skipped
 * when unset.  Where the tests above use their bytes in this test's
 * layout, this is their store entire -- the key file where they put
 * it, read through whatever `config.json` says the store type is. */
TEST_F(NooBaaKeysTest, AStoreOnDiskOpensItsOwnSecrets)
{
  const char* dir = ::getenv("NSFS_NOOBAA_STORE");
  if (!dir || !*dir) {
    GTEST_SKIP() << "NSFS_NOOBAA_STORE is unset";
  }

  nsfs::NooBaaConfigStore cs(dir);
  nsfs::NooBaaStoreContents c;
  ASSERT_EQ(cs.read(env->dpp, c), 0);
  ASSERT_GT(c.identities.size(), 0u);

  nsfs::NooBaaMasterKeys keys;
  ASSERT_EQ(keys.load(env->dpp, c.settings, dir), 0);

  std::vector<std::string> rejected;
  std::string why;
  for (const auto& r : rejected) {
    why += "\n  " + r;
  }
  ASSERT_EQ(nsfs::decrypt_store_secrets(env->dpp, keys, c, rejected), 0)
      << why;

  size_t opened = 0;
  for (const auto& i : c.identities) {
    for (const auto& ak : i.access_keys) {
      EXPECT_TRUE(nsfs::secret_is_plausible(ak.secret_key)) << i.name;
      ++opened;
    }
  }
  /* an assertion over an empty set proves nothing */
  EXPECT_GT(opened, 0u) << "no secret in the store was opened";
}

/* ------------------------------------------------------------------ *
 * X1 -- carrying force_md5_etag, and enforcing nothing.
 *
 * Three states everywhere, because their precedence is a cascade --
 * bucket if defined, else account if defined, else their global -- and
 * `false` counts as defined.  So "off" and "said nothing" inherit
 * differently and collapsing them changes behaviour a later decision
 * would honour.  `ACCOUNT_IMPORT.md` §6.1a.
 * ------------------------------------------------------------------ */

/* The account flag round-trips in all three states.
 *
 * The same shape as `GroupsDistinguishUnsetFromEmpty` above, for the
 * same reason:  a column that cannot tell NULL from 0 cannot carry a
 * cascade. */
TEST_F(NSFSIdentityDBTest, ForceMd5EtagDistinguishesUnsetFromFalse)
{
  nsfs::Identity unset = local_identity("x1-unset");
  ASSERT_FALSE(unset.force_md5_etag.has_value());
  ASSERT_EQ(db->put_identity(env->dpp, unset), 0);

  nsfs::Identity off = local_identity("x1-off");
  off.force_md5_etag = false;
  ASSERT_EQ(db->put_identity(env->dpp, off), 0);

  nsfs::Identity on = local_identity("x1-on");
  on.force_md5_etag = true;
  ASSERT_EQ(db->put_identity(env->dpp, on), 0);

  nsfs::Identity got;
  ASSERT_EQ(db->get_identity(env->dpp, "x1-unset", got), 0);
  EXPECT_FALSE(got.force_md5_etag.has_value())
      << "an unset flag came back set";

  ASSERT_EQ(db->get_identity(env->dpp, "x1-off", got), 0);
  ASSERT_TRUE(got.force_md5_etag.has_value())
      << "an explicit false came back as unset -- the cascade is lost";
  EXPECT_FALSE(*got.force_md5_etag);

  ASSERT_EQ(db->get_identity(env->dpp, "x1-on", got), 0);
  ASSERT_TRUE(got.force_md5_etag.has_value());
  EXPECT_TRUE(*got.force_md5_etag);

  /* and it updates independently, including back to unset */
  off.force_md5_etag = std::nullopt;
  ASSERT_EQ(db->put_identity(env->dpp, off), 0);
  ASSERT_EQ(db->get_identity(env->dpp, "x1-off", got), 0);
  EXPECT_FALSE(got.force_md5_etag.has_value());
  /* the other columns survived the rewrite */
  EXPECT_EQ(got.uid, 1001u);
  EXPECT_EQ(got.noobaa_id, "6a2bdeabf5c8e167f92cb079");
}

/* I1 carries each bucket's value, in three states.
 *
 * Read in I1 as well as by the state strategy because the question
 * X1 exists to answer -- does this deployment vary it per bucket? --
 * is about the deployment and cannot be answered one bucket at a
 * time. */
TEST_F(NooBaaStoreTest, EachBucketsForceMd5EtagIsCarried)
{
  add_bucket("says-nothing", R"({"_id":"1","name":"says-nothing"})");
  add_bucket("says-true",
	     R"({"_id":"2","name":"says-true","force_md5_etag":true})");
  add_bucket("says-false",
	     R"({"_id":"3","name":"says-false","force_md5_etag":false})");

  auto c = read_store();
  ASSERT_EQ(c.buckets.size(), 3u);

  std::map<std::string, std::optional<bool>> got;
  for (const auto& b : c.buckets) {
    got[b.name] = b.force_md5_etag;
  }
  EXPECT_FALSE(got["says-nothing"].has_value());
  ASSERT_TRUE(got["says-true"].has_value());
  EXPECT_TRUE(*got["says-true"]);
  ASSERT_TRUE(got["says-false"].has_value())
      << "an explicit false read as unset -- the cascade is lost";
  EXPECT_FALSE(*got["says-false"]);
}

/* A deployment that varies it per bucket is reported.
 *
 * X1 enforces nothing, so this report is the whole of what an
 * operator gets.  Both polarities:  a uniform deployment is silent,
 * and each kind of divergence is named. */
TEST_F(NooBaaStoreTest, VaryingItPerBucketIsReported)
{
  /* uniform:  one account says true, every bucket agrees or is silent */
  add_identity("a", R"({"_id":"a","name":"acct","force_md5_etag":true})");
  add_bucket("quiet", R"({"_id":"1","name":"quiet"})");
  add_bucket("agrees",
	     R"({"_id":"2","name":"agrees","force_md5_etag":true})");
  EXPECT_TRUE(read_store().md5_etag_divergence().empty())
      << "a uniform deployment was reported as varying";

  /* one bucket turns it off against the account */
  add_bucket("differs",
	     R"({"_id":"3","name":"differs","force_md5_etag":false})");
  auto one = read_store().md5_etag_divergence();
  ASSERT_EQ(one.size(), 1u);
  EXPECT_NE(one[0].find("differs"), std::string::npos) << one[0];
  EXPECT_NE(one[0].find("false"), std::string::npos) << one[0];

  /* a second account that disagrees with the first */
  add_identity("b", R"({"_id":"b","name":"other","force_md5_etag":false})");
  auto two = read_store().md5_etag_divergence();
  EXPECT_GE(two.size(), 2u);
  bool said_accounts = false;
  for (const auto& line : two) {
    if (line.find("accounts do not agree") != std::string::npos) {
      said_accounts = true;
    }
  }
  EXPECT_TRUE(said_accounts) << "two accounts disagreed and nothing said so";
}

/* A bucket with no account to compare against is still reported.
 *
 * The value cannot be inherited from anything, so a bucket stating it
 * is the only thing stating it -- which is the per-bucket posture X1
 * exists to surface, not an absence of information. */
TEST_F(NooBaaStoreTest, ABucketStatingItAloneIsReported)
{
  add_identity("a", R"({"_id":"a","name":"acct"})");
  add_bucket("alone",
	     R"({"_id":"1","name":"alone","force_md5_etag":true})");

  auto d = read_store().md5_etag_divergence();
  ASSERT_EQ(d.size(), 1u);
  EXPECT_NE(d[0].find("no account states"), std::string::npos) << d[0];
}

/* ------------------------------------------------------------------ *
 * P1 -- bucket placement:  the column and the accessor.
 *
 * Nothing reads a path yet.  Resolution is P2 and enumeration is P3,
 * so what is asserted here is that the row survives a round trip and
 * that a path we will not accept is refused where it is stored --
 * never on the request path, where it is a row we wrote.
 * ------------------------------------------------------------------ */

/* The lexical rule, as a pure function.
 *
 * It rules out a stored path that *means* something other than it
 * reads.  It does not make a path safe to open -- that is `openat2`
 * with RESOLVE_NO_SYMLINKS, a later stage -- and it does not make one
 * authorized, which is the per-account allowlist their
 * `custom_bucket_path_allowed_list` already expresses. */
TEST(NSFSPlacementPath, OnlyAnAbsoluteCanonicalPathIsStorable)
{
  EXPECT_TRUE(nsfs::placement_path_is_acceptable("/ibm/gpfs/fs1/bucket"));
  EXPECT_TRUE(nsfs::placement_path_is_acceptable("/a"));
  EXPECT_TRUE(nsfs::placement_path_is_acceptable("/a/b-c_d.e"));
  /* a space is legal in a path and is not a reason to refuse one */
  EXPECT_TRUE(nsfs::placement_path_is_acceptable("/mnt/some where"));

  /* the empty path is the default location, which is not a path */
  EXPECT_FALSE(nsfs::placement_path_is_acceptable(""));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("relative/path"));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/"));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/trailing/"));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/double//slash"));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/a/../b"));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/a/.."));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/.."));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/a/./b"));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/a/."));
  /* a newline would be legal on the filesystem and would make every
   * log line that quotes the path ambiguous */
  EXPECT_FALSE(nsfs::placement_path_is_acceptable("/a/b\nc"));
  EXPECT_FALSE(nsfs::placement_path_is_acceptable(
      "/" + std::string(PATH_MAX, 'x')));

  /* `..` inside a component is a name, not a traversal */
  EXPECT_TRUE(nsfs::placement_path_is_acceptable("/a/b..c"));
  EXPECT_TRUE(nsfs::placement_path_is_acceptable("/a/...."));
}

/* A row round-trips, and a bucket without one is the ordinary case.
 *
 * `get` returning -ENOENT is what every bucket looks like today:  the
 * default location needs no row, which is why no deployment changes
 * behaviour when this lands. */
TEST_F(NSFSIdentityDBTest, BucketPlacementRoundTrips)
{
  nsfs::BucketPlacement got;
  EXPECT_EQ(db->get_bucket_placement(env->dpp, "absent", "", got), -ENOENT)
      << "a bucket with no row was not the ordinary case";

  nsfs::BucketPlacement bp;
  bp.bucket_name = "elsewhere";
  bp.path = "/ibm/gpfs/fs1/elsewhere";
  bp.owns_storage = false;
  ASSERT_EQ(db->put_bucket_placement(env->dpp, bp), 0);

  ASSERT_EQ(db->get_bucket_placement(env->dpp, "elsewhere", "", got), 0);
  EXPECT_EQ(got.bucket_name, "elsewhere");
  EXPECT_EQ(got.tenant, "");
  EXPECT_EQ(got.path, "/ibm/gpfs/fs1/elsewhere");
  EXPECT_FALSE(got.owns_storage);

  /* the default location, stored explicitly:  empty is accepted and
   * means what a missing row means */
  nsfs::BucketPlacement deflt;
  deflt.bucket_name = "ordinary";
  ASSERT_EQ(db->put_bucket_placement(env->dpp, deflt), 0);
  ASSERT_EQ(db->get_bucket_placement(env->dpp, "ordinary", "", got), 0);
  EXPECT_TRUE(got.path.empty());
  EXPECT_TRUE(got.owns_storage) << "the default for owns_storage changed";

  /* rewritten in place rather than duplicated */
  bp.path = "/ibm/gpfs/fs2/elsewhere";
  ASSERT_EQ(db->put_bucket_placement(env->dpp, bp), 0);
  ASSERT_EQ(db->get_bucket_placement(env->dpp, "elsewhere", "", got), 0);
  EXPECT_EQ(got.path, "/ibm/gpfs/fs2/elsewhere");

  ASSERT_EQ(db->remove_bucket_placement(env->dpp, "elsewhere", ""), 0);
  EXPECT_EQ(db->get_bucket_placement(env->dpp, "elsewhere", "", got),
	    -ENOENT);
  /* removing a row that is not there is not a failure */
  EXPECT_EQ(db->remove_bucket_placement(env->dpp, "elsewhere", ""), 0);
}

/* The tenant is part of the key.
 *
 * dbstore's own bucket table makes BucketName unique and leaves the
 * tenant out of the key;  its comment has said for years that both
 * should be the key once one handle holds several tenants.  A table of
 * ours does not inherit that, and this is the assertion that says so
 * -- two tenants' buckets of the same name are two rows. */
TEST_F(NSFSIdentityDBTest, TheTenantIsPartOfTheKey)
{
  nsfs::BucketPlacement one;
  one.bucket_name = "shared-name";
  one.tenant = "tenant-a";
  one.path = "/ibm/gpfs/fs1/a";
  ASSERT_EQ(db->put_bucket_placement(env->dpp, one), 0);

  nsfs::BucketPlacement two;
  two.bucket_name = "shared-name";
  two.tenant = "tenant-b";
  two.path = "/ibm/gpfs/fs1/b";
  ASSERT_EQ(db->put_bucket_placement(env->dpp, two), 0)
      << "the second tenant's row replaced the first";

  nsfs::BucketPlacement got;
  ASSERT_EQ(db->get_bucket_placement(env->dpp, "shared-name", "tenant-a",
				     got), 0);
  EXPECT_EQ(got.path, "/ibm/gpfs/fs1/a");
  ASSERT_EQ(db->get_bucket_placement(env->dpp, "shared-name", "tenant-b",
				     got), 0);
  EXPECT_EQ(got.path, "/ibm/gpfs/fs1/b");

  /* and the untenanted row is a third */
  EXPECT_EQ(db->get_bucket_placement(env->dpp, "shared-name", "", got),
	    -ENOENT);
}

/* A path we will not accept is refused where it is stored.
 *
 * Both the accessor's check and the schema's, because the schema is
 * the only place a guard survives a caller written later -- the same
 * reason the group list carries a GLOB.  The second half here writes
 * a row behind the accessor's back to prove the CHECK is live. */
TEST_F(NSFSIdentityDBTest, AnUnacceptablePathIsRefusedWhereItIsStored)
{
  nsfs::BucketPlacement place;
  place.bucket_name = "b";

  for (const char* bad : {"relative/path", "/a/../b", "/trailing/",
			  "/double//slash", "/", "/.."}) {
    place.path = bad;
    EXPECT_EQ(db->put_bucket_placement(env->dpp, place), -EINVAL)
	<< "stored " << bad;
  }
  nsfs::BucketPlacement got;
  EXPECT_EQ(db->get_bucket_placement(env->dpp, "b", "", got), -ENOENT)
      << "a refused path left a row behind";

  /* a nameless bucket is not a row either */
  nsfs::BucketPlacement noname;
  noname.path = "/ibm/gpfs/fs1/x";
  EXPECT_EQ(db->put_bucket_placement(env->dpp, noname), -EINVAL);

  /* And the schema's own guard, reached behind the accessor.
   *
   * This is the half that matters:  the C++ check can be bypassed by
   * a caller written later, and the CHECK cannot.  A test that only
   * exercised the accessor would not know whether the constraint in
   * the DDL does anything at all. */
  const std::string file = (bp / "identity").string() + ".db";
  const std::string table =
    std::filesystem::path((bp / "identity").string()).filename().string()
    + "_nsfs_bucket_placement";
  sqlite3* raw = nullptr;
  ASSERT_EQ(sqlite3_open(file.c_str(), &raw), SQLITE_OK);
  auto close_raw = make_scope_guard([raw] { sqlite3_close(raw); });

  for (const char* bad : {"relative/path", "/a/../b", "/double//slash",
			  "/a/.."}) {
    const std::string sql =
      "INSERT INTO '" + table + "' (BucketName, Tenant, Path, OwnsStorage)"
      " VALUES ('direct', '', '" + bad + "', 1);";
    char* err = nullptr;
    EXPECT_NE(sqlite3_exec(raw, sql.c_str(), nullptr, nullptr, &err),
	      SQLITE_OK)
	<< "the schema accepted " << bad;
    sqlite3_free(err);
  }

  /* the comparison can succeed:  an acceptable path goes in this way */
  const std::string good =
    "INSERT INTO '" + table + "' (BucketName, Tenant, Path, OwnsStorage)"
    " VALUES ('direct', '', '/ibm/gpfs/fs1/direct', 1);";
  char* err = nullptr;
  EXPECT_EQ(sqlite3_exec(raw, good.c_str(), nullptr, nullptr, &err),
	    SQLITE_OK) << (err ? err : "");
  sqlite3_free(err);

  /* and an out-of-range OwnsStorage is refused too */
  const std::string two =
    "INSERT INTO '" + table + "' (BucketName, Tenant, Path, OwnsStorage)"
    " VALUES ('two', '', '', 2);";
  EXPECT_NE(sqlite3_exec(raw, two.c_str(), nullptr, nullptr, nullptr),
	    SQLITE_OK) << "the schema accepted OwnsStorage=2";
}

/* ------------------------------------------------------------------ *
 * I3 -- the mapping, and the collision gate.
 *
 * No secrets:  an access key's id is cleartext, so this runs against
 * a store whose master key is missing.  Nothing is written.
 * ------------------------------------------------------------------ */

class NooBaaPlanTest : public NooBaaStoreTest {
public:
  nsfs::ImportPlan plan;

  int build(int expect_entries = -1) {
    auto c = read_store();
    int ret = nsfs::build_import_plan(env->dpp, env->cct.get(), c, plan);
    if (expect_entries >= 0) {
      EXPECT_EQ(static_cast<int>(plan.entries.size()), expect_entries);
    }
    return ret;
  }

  const nsfs::ImportEntry* entry(const std::string& name) const {
    for (const auto& e : plan.entries) {
      if (e.noobaa_name == name) {
	return &e;
      }
    }
    return nullptr;
  }

  bool said(const std::string& fragment) const {
    for (const auto& c : plan.collisions) {
      if (c.find(fragment) != std::string::npos) {
	return true;
      }
    }
    return false;
  }

  std::string why() const {
    std::string s;
    for (const auto& c : plan.collisions) {
      s += "\n  " + c;
    }
    return s;
  }
};

/* A store their tools wrote maps cleanly.
 *
 * The three records are NooBaa's own bytes.  Note what the ids look
 * like:  their `_id` is a 24-character hex ObjectId and the minted
 * account id is `RGW` followed by digits, which is why no
 * transformation of one is the other. */
TEST_F(NooBaaPlanTest, AStoreTheirToolsWroteMapsCleanly)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("6ac7d2121e63a748e37663e0", NB_ACCOUNT_GROUPS);
  add_identity("6ac7d213cac521f05aac9fb4", NB_ACCOUNT_ANONYMOUS);

  ASSERT_EQ(build(3), 0) << why();
  EXPECT_TRUE(plan.collisions.empty()) << why();
  EXPECT_EQ(plan.accounts(), 3u);
  EXPECT_EQ(plan.iam_users(), 0u);

  const auto* e = entry("interchange");
  ASSERT_NE(e, nullptr);
  EXPECT_EQ(e->noobaa_id, "6ac7cb27fd4af356919110a3");
  EXPECT_EQ(e->name, "interchange");
  /* their email is a copy of the name, carried as it is */
  EXPECT_EQ(e->email, "interchange");
  /* what principal matching consults */
  EXPECT_EQ(e->display_name, "interchange");
  EXPECT_TRUE(e->tenant.empty());
  ASSERT_EQ(e->access_key_ids.size(), 1u);
  EXPECT_EQ(e->access_key_ids[0], "INTERCHANGEACCESSKEY");
  EXPECT_FALSE(e->iam_user);

  /* a minted id in RGW's own form, not a transformation of theirs */
  EXPECT_TRUE(rgw::account::validate_id(e->account_id))
      << e->account_id << " is not an RGW account id";
  EXPECT_NE(e->account_id, e->noobaa_id);
  EXPECT_EQ(e->account_id.compare(0, 3, "RGW"), 0);

  /* every account gets its own */
  std::set<std::string> ids;
  for (const auto& x : plan.entries) {
    EXPECT_TRUE(ids.insert(x.account_id).second)
	<< "two accounts were minted the same id";
  }

  EXPECT_TRUE(entry("anonymous")->anonymous);
  EXPECT_TRUE(entry("anonymous")->access_key_ids.empty());
}

/* It needs no secrets.
 *
 * The same store with no master_keys.json maps identically:  the gate
 * is about ids and names, so a deployment whose key is missing can
 * still be told what its import would hit. */
TEST_F(NooBaaPlanTest, ThePlanNeedsNoMasterKey)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  ASSERT_FALSE(std::filesystem::exists(store + "/master_keys.json"));

  ASSERT_EQ(build(1), 0) << why();
  EXPECT_EQ(entry("interchange")->access_key_ids[0],
	    "INTERCHANGEACCESSKEY");
}

/* An IAM user takes its owner's account id.
 *
 * Root accounts are mapped first so a user can be placed without a
 * second pass, and an orphan user is reported rather than given an
 * invented account. */
TEST_F(NooBaaPlanTest, AnIamUserTakesItsOwnersAccount)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("b", R"({"_id":"b","name":"a-user","email":"u@x",
    "owner":"6ac7cb27fd4af356919110a3"})");

  ASSERT_EQ(build(2), 0) << why();
  EXPECT_EQ(plan.accounts(), 1u);
  EXPECT_EQ(plan.iam_users(), 1u);

  const auto* acct = entry("interchange");
  const auto* user = entry("a-user");
  ASSERT_NE(acct, nullptr);
  ASSERT_NE(user, nullptr);
  EXPECT_TRUE(user->iam_user);
  EXPECT_EQ(user->account_id, acct->account_id)
      << "the user was not placed in its owner's account";

  /* an owner that is not in the store */
  add_identity("c", R"({"_id":"c","name":"orphan","email":"o@x",
    "owner":"6ac7cb27fd4af3569191ffff"})");
  EXPECT_EQ(build(3), -EINVAL);
  EXPECT_TRUE(said("whose owner")) << why();
  EXPECT_TRUE(said("6ac7cb27fd4af3569191ffff")) << why();
}

/* The email collision, which is the likeliest first failure.
 *
 * Their email is a copy of the name, and RGW's email index is shared
 * across users and accounts.  So two IAM users named `alice` under
 * different root accounts collide -- which is §6.3's own example, and
 * nothing about it is visible one record at a time. */
TEST_F(NooBaaPlanTest, TwoUsersOfOneNameUnderDifferentAccountsCollide)
{
  add_identity("a1", R"({"_id":"a1","name":"acct-one","email":"acct-one"})");
  add_identity("a2", R"({"_id":"a2","name":"acct-two","email":"acct-two"})");
  ASSERT_EQ(build(2), 0) << why();

  add_identity("u1", R"({"_id":"u1","name":"alice","email":"alice",
    "owner":"a1"})");
  add_identity("u2", R"({"_id":"u2","name":"alice","email":"alice",
    "owner":"a2"})");

  EXPECT_EQ(build(4), -EEXIST);
  EXPECT_TRUE(said("both carry the email \"alice\"")) << why();
  EXPECT_TRUE(said("u1")) << why();
  EXPECT_TRUE(said("u2")) << why();
  /* the plan is still returned, because an operator reading a refusal
   * wants to see what would have been imported */
  EXPECT_EQ(plan.entries.size(), 4u);
}

/* Every collision is reported, not the first.
 *
 * Fixing them one import at a time means one failed import per
 * collision, so the list is the deliverable.  Four kinds at once, and
 * the count is asserted so a gate that stopped early would fail. */
TEST_F(NooBaaPlanTest, EveryCollisionIsReportedTogether)
{
  add_identity("a1", R"({"_id":"a1","name":"same-name","email":"e1",
    "access_keys":[{"access_key":"KEYAAAAAAAAAAAAAAAAA",
    "encrypted_secret_key":"x"}]})");
  add_identity("a2", R"({"_id":"a2","name":"same-name","email":"e2",
    "access_keys":[{"access_key":"KEYAAAAAAAAAAAAAAAAA",
    "encrypted_secret_key":"x"}]})");
  add_identity("a3", R"({"_id":"a3","name":"third","email":"e1"})");
  add_identity("a4", R"({"_id":"a4","name":"has$dollar","email":"e4"})");

  EXPECT_EQ(build(4), -EEXIST);

  EXPECT_TRUE(said("two accounts are named same-name")) << why();
  EXPECT_TRUE(said("both carry the email \"e1\"")) << why();
  EXPECT_TRUE(said("both carry the access key KEYAAAAAAAAAAAAAAAAA"))
      << why();
  EXPECT_TRUE(said("has$dollar")) << why();
  EXPECT_GE(plan.collisions.size(), 4u)
      << "the gate stopped before reporting everything:" << why();
}

/* A name RGW will not accept is reported, not rewritten.
 *
 * NooBaa validates only *new* names and says so, so a legacy name may
 * contain anything.  Rewriting one silently invalidates every
 * name-form principal referring to it, so this reports. */
TEST_F(NooBaaPlanTest, ANameRgwRefusesIsReportedNotRewritten)
{
  add_identity("a", R"({"_id":"a","name":"has:colon","email":"a"})");
  EXPECT_EQ(build(1), -EINVAL);
  EXPECT_TRUE(said("has:colon")) << why();
  EXPECT_TRUE(said("will not accept")) << why();
  /* the name is carried through unchanged */
  EXPECT_EQ(plan.entries[0].name, "has:colon");

  /* and a plain name is accepted, so the check is not refusing
   * everything */
  nsfs::ImportPlan clean;
  std::filesystem::remove_all(store + "/identities");
  std::filesystem::create_directories(store + "/identities");
  add_identity("b", R"({"_id":"b","name":"ordinary","email":"b"})");
  auto c = read_store();
  EXPECT_EQ(nsfs::build_import_plan(env->dpp, env->cct.get(), c, clean), 0);
}

/* A duplicated NooBaa id is refused, because re-import depends on it.
 *
 * The id is what a second pass recognises an identity by -- the name
 * is not, since a rename rewrites the name and leaves the id -- so two
 * records claiming one id make that recognition ambiguous. */
TEST_F(NooBaaPlanTest, ADuplicatedNooBaaIdIsRefused)
{
  /* two directories, one `_id` inside both */
  add_identity("dir-one", R"({"_id":"same","name":"one","email":"one"})");
  add_identity("dir-two", R"({"_id":"same","name":"two","email":"two"})");

  EXPECT_EQ(build(2), -EEXIST);
  EXPECT_TRUE(said("appears on more than one record")) << why();
}

/* A real store on disk plans without collisions.
 *
 * Skipped when `NSFS_NOOBAA_STORE` is unset.  Three accounts their CLI
 * and gateway made, which is the case an import will actually meet
 * first. */
TEST_F(NooBaaPlanTest, AStoreOnDiskPlansCleanly)
{
  const char* dir = ::getenv("NSFS_NOOBAA_STORE");
  if (!dir || !*dir) {
    GTEST_SKIP() << "NSFS_NOOBAA_STORE is unset";
  }

  nsfs::NooBaaConfigStore cs(dir);
  nsfs::NooBaaStoreContents c;
  ASSERT_EQ(cs.read(env->dpp, c), 0);
  ASSERT_GT(c.identities.size(), 0u);

  nsfs::ImportPlan p;
  const int ret = nsfs::build_import_plan(env->dpp, env->cct.get(), c, p);
  std::string s;
  for (const auto& x : p.collisions) {
    s += "\n  " + x;
  }
  EXPECT_EQ(ret, 0) << s;
  EXPECT_EQ(p.entries.size(), c.identities.size());
  for (const auto& e : p.entries) {
    EXPECT_FALSE(e.account_id.empty()) << e.noobaa_name;
    EXPECT_TRUE(rgw::account::validate_id(e.account_id)) << e.account_id;
  }
}

/* ------------------------------------------------------------------ *
 * I4 -- writing identities into RGW.
 *
 * The first stage that writes, so these tests look at what is in RGW
 * afterwards rather than at a returned structure.  The driver gets a
 * database of its own per test (see TestDriver::init), which is what
 * makes the account and user write paths reachable at all.
 * ------------------------------------------------------------------ */

class NooBaaImportTest : public NooBaaStoreTest {
public:
  nsfs::ImportPlan plan;
  nsfs::ImportResult result;

  /* read, plan, write -- the three stages in the order the import
   * runs them */
  int run() {
    auto c = read_store();
    nsfs::NooBaaMasterKeys keys;
    nsfs::NooBaaSettings fs;
    fs.master_keys_store_type = "file";
    if (keys.load(env->dpp, fs, store) == 0) {
      std::vector<std::string> rejected;
      nsfs::decrypt_store_secrets(env->dpp, keys, c, rejected);
    }
    plan = nsfs::ImportPlan{};
    result = nsfs::ImportResult{};
    if (int r = nsfs::build_import_plan(env->dpp, env->cct.get(), c, plan);
	r < 0) {
      return r;
    }
    return nsfs::import_identities(env->dpp, null_yield, driver.get(), c,
				   plan, result);
  }

  std::string why() const {
    std::string s;
    for (const auto& f : result.failures) {
      s += "\n  " + f;
    }
    return s;
  }

  const nsfs::ImportEntry* entry(const std::string& name) const {
    for (const auto& e : plan.entries) {
      if (e.noobaa_name == name) {
	return &e;
      }
    }
    return nullptr;
  }
};

/* A store their tools wrote lands in RGW.
 *
 * The account, the user that holds its keys, and the identity row --
 * three writes per root account, because `RGWAccountInfo` holds no
 * credentials and neither holds the POSIX fields. */
TEST_F(NooBaaImportTest, AnAccountBecomesAnAccountAUserAndARow)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.accounts_created, 1u);
  EXPECT_EQ(result.users_created, 1u);
  EXPECT_EQ(result.identity_rows, 1u);
  EXPECT_EQ(result.accounts_skipped, 0u);

  const auto* e = entry("interchange");
  ASSERT_NE(e, nullptr);

  /* the account, by the id that was minted for it */
  RGWAccountInfo got;
  Attrs attrs;
  RGWObjVersionTracker objv;
  ASSERT_EQ(driver->load_account_by_id(env->dpp, null_yield, e->account_id,
				       got, attrs, objv), 0);
  EXPECT_EQ(got.name, "interchange");
  EXPECT_EQ(got.email, "interchange");

  /* and by name and by email, which are the indexes a collision would
   * have hit */
  RGWAccountInfo byname;
  ASSERT_EQ(driver->load_account_by_name(env->dpp, null_yield, "",
					 "interchange", byname, attrs,
					 objv), 0);
  EXPECT_EQ(byname.id, e->account_id);

  /* the user that carries the credential, found the way an
   * authenticated request finds it */
  std::unique_ptr<rgw::sal::User> user;
  ASSERT_EQ(driver->get_user_by_access_key(env->dpp, "INTERCHANGEACCESSKEY",
					   null_yield, &user), 0)
      << "the imported credential does not authenticate";
  EXPECT_EQ(user->get_info().account_id, e->account_id);
  EXPECT_EQ(user->get_display_name(), "interchange");
  const auto& keys = user->get_info().access_keys;
  ASSERT_EQ(keys.size(), 1u);
  /* the decrypted secret, which is what makes the credential usable
   * and is why I2 comes first */
  EXPECT_EQ(keys.begin()->second.key, NB_PLAIN_SECRET);

  /* the identity row, keyed on the account and carrying their id */
  nsfs::Identity row;
  ASSERT_EQ(driver->get_identity_db()->get_identity_by_noobaa_id(
		env->dpp, "6ac7cb27fd4af356919110a3", row), 0)
      << "re-import could not recognise what it just wrote";
  EXPECT_EQ(row.key, e->account_id);
  EXPECT_EQ(row.uid, 1000u);
  EXPECT_EQ(row.gid, 1000u);
  EXPECT_EQ(row.new_buckets_path, "/lv2tb/nsfs-interchange/data/root");
}

/* Running it twice writes nothing the second time.
 *
 * Keyed on their `_id`, because the name is not stable:  a rename
 * rewrites `name` and `email` and leaves `_id` alone.  A name-keyed
 * re-run would collide on the name it wrote itself, or worse, mint a
 * second id for an identity already imported. */
TEST_F(NooBaaImportTest, ASecondRunRecognisesWhatTheFirstWrote)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("6ac7d2121e63a748e37663e0", NB_ACCOUNT_GROUPS);
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);

  ASSERT_EQ(run(), 0) << why();
  ASSERT_EQ(result.accounts_created, 2u);
  const std::string first_id = entry("interchange")->account_id;

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.accounts_created, 0u) << "it wrote again";
  EXPECT_EQ(result.users_created, 0u);
  EXPECT_EQ(result.identity_rows, 0u);
  EXPECT_EQ(result.accounts_skipped, 2u);

  /* and the account it already had is still the one it had:  the
   * second run minted a new id in its plan, and must not have used
   * it */
  nsfs::Identity row;
  ASSERT_EQ(driver->get_identity_db()->get_identity_by_noobaa_id(
		env->dpp, "6ac7cb27fd4af356919110a3", row), 0);
  EXPECT_EQ(row.key, first_id)
      << "the second run re-minted an id for an identity already imported";
}

/* A rename on their side does not produce a duplicate.
 *
 * This is why the id is the key and the name is not.  The same record
 * under a new name is the same identity. */
TEST_F(NooBaaImportTest, ARenameOnTheirSideIsStillTheSameIdentity)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);
  ASSERT_EQ(run(), 0) << why();
  const std::string id = entry("interchange")->account_id;

  /* their rename:  name and email change, `_id` does not */
  std::string renamed{NB_ACCOUNT_PLAIN};
  auto at = renamed.find("\"name\": \"interchange\"");
  ASSERT_NE(at, std::string::npos);
  renamed.replace(at, 21, "\"name\": \"renamed-acct\"");
  at = renamed.find("\"email\": \"interchange\"");
  ASSERT_NE(at, std::string::npos);
  renamed.replace(at, 22, "\"email\": \"renamed-acct\"");
  add_identity("6ac7cb27fd4af356919110a3", renamed.c_str());

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.accounts_created, 0u)
      << "a rename produced a second account";
  EXPECT_EQ(result.accounts_skipped, 1u);

  nsfs::Identity row;
  ASSERT_EQ(driver->get_identity_db()->get_identity_by_noobaa_id(
		env->dpp, "6ac7cb27fd4af356919110a3", row), 0);
  EXPECT_EQ(row.key, id);
}

/* The anonymous account gets a row and nothing else.
 *
 * RGW's anonymous identity is built in and is not an account, so
 * importing it as one is wrong;  skipping it loses the uid mapping
 * nsfs needs, and then an unauthenticated request has no POSIX
 * identity to act as.  Keyed on RGW's own owner string, so resolution
 * reaches it by the same path as any other owner (§6.8). */
TEST_F(NooBaaImportTest, AnonymousGetsARowAndNothingElse)
{
  add_identity("6ac7d213cac521f05aac9fb4", NB_ACCOUNT_ANONYMOUS);

  ASSERT_EQ(run(), 0) << why();
  EXPECT_TRUE(result.anonymous_imported);
  EXPECT_EQ(result.identity_rows, 1u);
  EXPECT_EQ(result.accounts_created, 0u) << "anonymous became an account";
  EXPECT_EQ(result.users_created, 0u) << "anonymous got a user";

  nsfs::Identity row;
  ASSERT_EQ(driver->get_identity_db()->get_identity(
		env->dpp, RGW_USER_ANON_ID, row), 0)
      << "nothing resolves for " << RGW_USER_ANON_ID
      << ", so an unauthenticated request has no identity";
  EXPECT_EQ(row.uid, 65534u);
  EXPECT_EQ(row.gid, 65534u);
  EXPECT_EQ(row.noobaa_id, "6ac7d213cac521f05aac9fb4");

  /* nothing was minted for it */
  const auto* e = entry("anonymous");
  ASSERT_NE(e, nullptr);
  RGWAccountInfo got;
  Attrs attrs;
  RGWObjVersionTracker objv;
  EXPECT_NE(driver->load_account_by_name(env->dpp, null_yield, "",
					 "anonymous", got, attrs, objv), 0)
      << "an RGW account named anonymous was created";
}

/* An IAM user lands in its owner's account.
 *
 * One schema serves accounts and users, so the import tells them apart
 * by `owner` and places the user in the account that owner became. */
TEST_F(NooBaaImportTest, AnIamUserLandsInItsOwnersAccount)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("u1", R"({"_id":"u1","name":"a-user","email":"u@x",
    "owner":"6ac7cb27fd4af356919110a3","iam_path":"/eng/",
    "nsfs_account_config":{"uid":1500,"gid":1500},
    "access_keys":[{"access_key":"USERAAAAAAAAAAAAAAAA",
      "encrypted_secret_key":"AlSvZsxnqaN+QX3A/wUwFB0kJwM01TDOXKmNFnV51FijvRm3nJgdKQ=="}],
    "master_key_id":"6ac7cb2751b72219a4bae375"})");
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.accounts_created, 1u);
  EXPECT_EQ(result.users_created, 2u) << "the account's own user, and the "
					 "IAM user";
  EXPECT_EQ(result.identity_rows, 2u);

  std::unique_ptr<rgw::sal::User> user;
  ASSERT_EQ(driver->get_user_by_access_key(env->dpp, "USERAAAAAAAAAAAAAAAA",
					   null_yield, &user), 0);
  EXPECT_EQ(user->get_info().account_id,
	    entry("interchange")->account_id)
      << "the user was not placed in its owner's account";
  EXPECT_EQ(user->get_info().path, "/eng/");
  EXPECT_EQ(user->get_display_name(), "a-user");

  /* its own row, with its own uid */
  nsfs::Identity row;
  ASSERT_EQ(driver->get_identity_db()->get_identity_by_noobaa_id(
		env->dpp, "u1", row), 0);
  EXPECT_EQ(row.uid, 1500u);
}

/* A key with no decrypted secret is refused, not written.
 *
 * A user written with an unusable credential is worse than one not
 * written:  the credential silently does not authenticate and nothing
 * says why.  So the master-key step comes first and this is what
 * happens when it did not. */
TEST_F(NooBaaImportTest, AnUndecryptedSecretRefusesTheIdentity)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  /* no master_keys.json, so no secret is opened */
  ASSERT_FALSE(std::filesystem::exists(store + "/master_keys.json"));

  EXPECT_EQ(run(), -EIO);
  EXPECT_EQ(result.users_created, 0u);
  bool said = false;
  for (const auto& f : result.failures) {
    if (f.find("no decrypted secret") != std::string::npos) {
      said = true;
    }
  }
  EXPECT_TRUE(said) << why();

  /* and nothing authenticates with it */
  std::unique_ptr<rgw::sal::User> user;
  EXPECT_NE(driver->get_user_by_access_key(env->dpp, "INTERCHANGEACCESSKEY",
					   null_yield, &user), 0)
      << "a credential was written with no usable secret";
}

/* allow_bypass_governance becomes an IAM grant.
 *
 * Their flag is a boolean;  RGW expresses the same permission as an
 * action, so the import writes a policy rather than inventing a
 * column (§2.2). */
TEST_F(NooBaaImportTest, BypassGovernanceBecomesAGrant)
{
  std::string with{NB_ACCOUNT_PLAIN};
  auto at = with.find("\"gid\": 1000");
  ASSERT_NE(at, std::string::npos);
  with.replace(at, 11, "\"gid\": 1000,\n    \"allow_bypass_governance\": true");
  add_identity("6ac7cb27fd4af356919110a3", with.c_str());
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.grants_written, 1u);

  std::unique_ptr<rgw::sal::User> user;
  ASSERT_EQ(driver->get_user_by_access_key(env->dpp, "INTERCHANGEACCESSKEY",
					   null_yield, &user), 0);
  auto p = user->get_attrs().find(RGW_ATTR_USER_POLICY);
  ASSERT_NE(p, user->get_attrs().end()) << "no policy was attached";
  std::map<std::string, std::string> policies;
  auto bl = p->second.cbegin();
  ceph::decode(policies, bl);
  ASSERT_EQ(policies.size(), 1u);
  EXPECT_NE(policies.begin()->second.find("s3:BypassGovernanceRetention"),
	    std::string::npos);

  /* and the negative case:  without the flag, no policy at all */
  std::filesystem::remove_all(store + "/identities");
  std::filesystem::create_directories(store + "/identities");
  add_identity("other", NB_ACCOUNT_GROUPS);
  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.grants_written, 0u);
}

/* A collision against RGW's existing state is caught before writing.
 *
 * I3 sees the collisions in the store;  this is the other half, and it
 * needs a lookup.  An identity already imported is **not** one -- it is
 * recognised by its id and skipped, which is what makes a re-run safe,
 * so the check has to ask that question first. */
TEST_F(NooBaaImportTest, ACollisionWithExistingRgwStateIsCaught)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  write_file(store + "/master_keys.json", NB_MASTER_KEYS);
  ASSERT_EQ(run(), 0) << why();

  /* a second, different NooBaa identity claiming the same name */
  add_identity("different", R"({"_id":"different","name":"interchange",
    "email":"interchange"})");

  auto c = read_store();
  nsfs::ImportPlan p;
  ASSERT_EQ(nsfs::build_import_plan(env->dpp, env->cct.get(), c, p),
	    -EEXIST)
      << "two records of one name should collide in the store alone";

  /* and with only the new one in the store, the store looks clean and
   * RGW is the only thing that knows */
  std::filesystem::remove_all(store + "/identities");
  std::filesystem::create_directories(store + "/identities");
  add_identity("different", R"({"_id":"different","name":"interchange",
    "email":"interchange"})");
  auto c2 = read_store();
  nsfs::ImportPlan p2;
  ASSERT_EQ(nsfs::build_import_plan(env->dpp, env->cct.get(), c2, p2), 0)
      << "the store half should be clean here";

  std::vector<std::string> collisions;
  EXPECT_EQ(nsfs::check_rgw_state(env->dpp, null_yield, driver.get(), p2,
				  collisions), -EEXIST)
      << "a name already in RGW was not noticed";
  ASSERT_FALSE(collisions.empty());
  EXPECT_NE(collisions[0].find("interchange"), std::string::npos)
      << collisions[0];

  /* and the same check passes for an identity already imported, which
   * is the case it must not call a collision */
  std::filesystem::remove_all(store + "/identities");
  std::filesystem::create_directories(store + "/identities");
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  auto c3 = read_store();
  nsfs::ImportPlan p3;
  ASSERT_EQ(nsfs::build_import_plan(env->dpp, env->cct.get(), c3, p3), 0);
  std::vector<std::string> none;
  EXPECT_EQ(nsfs::check_rgw_state(env->dpp, null_yield, driver.get(), p3,
				  none), 0)
      << "an identity already imported was called a collision";
}

/* A real store on disk imports.
 *
 * Skipped when `NSFS_NOOBAA_STORE` is unset.  Three accounts their CLI
 * and gateway wrote, their master key, their ciphertext -- read,
 * planned, decrypted and written, which is the whole of phases 0 and 1
 * against output nobody composed. */
TEST_F(NooBaaImportTest, AStoreOnDiskImports)
{
  const char* dir = ::getenv("NSFS_NOOBAA_STORE");
  if (!dir || !*dir) {
    GTEST_SKIP() << "NSFS_NOOBAA_STORE is unset";
  }

  nsfs::NooBaaConfigStore cs(dir);
  nsfs::NooBaaStoreContents c;
  ASSERT_EQ(cs.read(env->dpp, c), 0);

  nsfs::NooBaaMasterKeys keys;
  ASSERT_EQ(keys.load(env->dpp, c.settings, dir), 0);
  std::vector<std::string> rejected;
  ASSERT_EQ(nsfs::decrypt_store_secrets(env->dpp, keys, c, rejected), 0);

  nsfs::ImportPlan p;
  ASSERT_EQ(nsfs::build_import_plan(env->dpp, env->cct.get(), c, p), 0);

  std::vector<std::string> collisions;
  ASSERT_EQ(nsfs::check_rgw_state(env->dpp, null_yield, driver.get(), p,
				  collisions), 0);

  nsfs::ImportResult r;
  std::string s;
  int ret = nsfs::import_identities(env->dpp, null_yield, driver.get(), c,
				    p, r);
  for (const auto& f : r.failures) {
    s += "\n  " + f;
  }
  ASSERT_EQ(ret, 0) << s;
  EXPECT_GT(r.accounts_created, 0u);
  EXPECT_GT(r.identity_rows, 0u);
  /* their store has an anonymous account, and it must have become a
   * row rather than an account */
  EXPECT_TRUE(r.anonymous_imported);

  /* every imported credential authenticates */
  size_t checked = 0;
  for (const auto& id : c.identities) {
    for (const auto& ak : id.access_keys) {
      std::unique_ptr<rgw::sal::User> user;
      EXPECT_EQ(driver->get_user_by_access_key(env->dpp, ak.access_key,
					       null_yield, &user), 0)
	  << ak.access_key << " does not authenticate";
      ++checked;
    }
  }
  EXPECT_GT(checked, 0u) << "no credential was checked";

  /* and a second run writes nothing */
  nsfs::ImportResult again;
  ASSERT_EQ(nsfs::import_identities(env->dpp, null_yield, driver.get(), c,
				    p, again), 0);
  EXPECT_EQ(again.accounts_created, 0u);
  EXPECT_EQ(again.users_created, 0u);
}

/* ------------------------------------------------------------------ *
 * I5 -- bucket state, and the principals in it.
 *
 * Most of the translation already existed;  what is new is the owner,
 * the principal rewriting, and `logging`.  The rewriting gets the most
 * tests because it is the part where being subtly wrong changes who
 * may do what, and nobody notices until an access decision goes the
 * wrong way.
 * ------------------------------------------------------------------ */

class NooBaaPolicyTest : public ::testing::Test {
public:
  nsfs::IdentityMapping mapping;

  void SetUp() override {
    mapping.account_by_noobaa_id["6ac7cb27fd4af356919110a3"] =
      "RGW11111111111111111";
    mapping.noobaa_id_by_name["acct-one"] = "6ac7cb27fd4af356919110a3";
    mapping.user_arn_by_noobaa_id["u1"] =
      "arn:aws:iam::RGW11111111111111111:user/alice";
    mapping.noobaa_id_by_name["alice"] = "u1";
  }

  std::string rewrite(const std::string& doc, int expect = 0) {
    std::string out;
    std::vector<std::string> unresolved;
    EXPECT_EQ(nsfs::rewrite_policy_principals(env->dpp, doc, mapping, out,
					      unresolved), expect)
	<< (unresolved.empty() ? std::string{} : unresolved[0]);
    return out;
  }
};

/* Id first, then name -- which is the whole of the disambiguation.
 *
 * A name and an id are both bare strings and cannot be told apart
 * syntactically:  an account may legally be *named* twenty-four hex
 * characters.  So the order matters, and it mirrors theirs. */
TEST_F(NooBaaPolicyTest, APrincipalResolvesByIdThenByName)
{
  EXPECT_EQ(*mapping.resolve("6ac7cb27fd4af356919110a3"),
	    "RGW11111111111111111");
  EXPECT_EQ(*mapping.resolve("acct-one"), "RGW11111111111111111");
  EXPECT_EQ(*mapping.resolve("u1"),
	    "arn:aws:iam::RGW11111111111111111:user/alice");
  EXPECT_EQ(*mapping.resolve("alice"),
	    "arn:aws:iam::RGW11111111111111111:user/alice");
  EXPECT_FALSE(mapping.resolve("nobody").has_value());

  /* an account *named* like an id resolves as the id it is, not the
   * name it looks like */
  nsfs::IdentityMapping ambiguous = mapping;
  ambiguous.noobaa_id_by_name["6ac7cb27fd4af356919110a3"] = "u1";
  EXPECT_EQ(*ambiguous.resolve("6ac7cb27fd4af356919110a3"),
	    "RGW11111111111111111")
      << "the name arm won, so the order is wrong";
}

/* Everything outside Principal survives byte-for-byte.
 *
 * This is why it is a DOM and not a string substitution.  A policy's
 * `Condition` is the difference between a narrow grant and a broad
 * one, and a value that happens to equal a principal must not be
 * rewritten because it is a resource or a condition value. */
TEST_F(NooBaaPolicyTest, OnlyPrincipalsAreTouched)
{
  const std::string doc = R"({
    "Version":"2012-10-17",
    "Statement":[{
      "Sid":"acct-one",
      "Effect":"Allow",
      "Principal":{"AWS":["acct-one"]},
      "Action":["s3:GetObject"],
      "Resource":["arn:aws:s3:::acct-one/*"],
      "Condition":{"StringEquals":{"s3:prefix":"acct-one"}}
    }]
  })";

  const std::string out = rewrite(doc);
  /* the principal moved */
  EXPECT_NE(out.find("RGW11111111111111111"), std::string::npos) << out;
  /* and nothing else did -- a Sid, a Resource and a Condition value
   * that all equal the principal's name are untouched */
  EXPECT_NE(out.find("\"Sid\":\"acct-one\""), std::string::npos) << out;
  EXPECT_NE(out.find("arn:aws:s3:::acct-one/*"), std::string::npos) << out;
  EXPECT_NE(out.find("\"s3:prefix\":\"acct-one\""), std::string::npos)
      << out;
  /* the condition survived at all, which a model-and-re-emit rewrite
   * is what would quietly drop */
  EXPECT_NE(out.find("StringEquals"), std::string::npos) << out;
}

/* Their three principal shapes, and the arms that are not ours.
 *
 * A bare string, an array, and an object with an `AWS` member;
 * `Service` and `Federated` are AWS's own namespaces and are left
 * exactly as they are. */
TEST_F(NooBaaPolicyTest, EveryPrincipalShapeIsRewritten)
{
  EXPECT_NE(rewrite(R"({"Statement":[{"Principal":"alice"}]})")
		.find("user/alice"), std::string::npos);
  EXPECT_NE(rewrite(R"({"Statement":[{"Principal":["alice","acct-one"]}]})")
		.find("RGW11111111111111111"), std::string::npos);

  const std::string obj =
    rewrite(R"({"Statement":[{"Principal":{"AWS":"acct-one",
      "Service":"acct-one"}}]})");
  EXPECT_NE(obj.find("\"AWS\":\"RGW11111111111111111\""),
	    std::string::npos) << obj;
  EXPECT_NE(obj.find("\"Service\":\"acct-one\""), std::string::npos)
      << "a Service principal was rewritten: " << obj;

  /* NotPrincipal is a denial's subject and matters just as much */
  EXPECT_NE(rewrite(R"({"Statement":[{"NotPrincipal":{"AWS":"alice"}}]})")
		.find("user/alice"), std::string::npos);

  /* `*` means everyone on both sides */
  const std::string star = rewrite(R"({"Statement":[{"Principal":"*"}]})");
  EXPECT_NE(star.find("\"*\""), std::string::npos) << star;

  /* a single statement rather than an array */
  EXPECT_NE(rewrite(R"({"Statement":{"Principal":"alice"}})")
		.find("user/alice"), std::string::npos);
}

/* An unresolved principal refuses the policy and names every one.
 *
 * A principal resolving to nothing is a grant or a denial that will
 * not take effect.  Every one is listed, because an operator fixing
 * them needs the list rather than the first. */
TEST_F(NooBaaPolicyTest, AnUnresolvedPrincipalRefusesThePolicy)
{
  std::string out{"untouched"};
  std::vector<std::string> unresolved;
  EXPECT_EQ(nsfs::rewrite_policy_principals(
		env->dpp,
		R"({"Statement":[{"Principal":{"AWS":["alice","ghost",
		  "phantom"]}}]})",
		mapping, out, unresolved), -ENOENT);
  ASSERT_EQ(unresolved.size(), 2u);
  EXPECT_EQ(unresolved[0], "ghost");
  EXPECT_EQ(unresolved[1], "phantom");

  /* and a document that will not parse is a different answer */
  unresolved.clear();
  EXPECT_EQ(nsfs::rewrite_policy_principals(env->dpp, "{not json",
					    mapping, out, unresolved),
	    -EBADMSG);
  EXPECT_TRUE(unresolved.empty());
}

/* The mapping is built from the plan, so it is what I4 wrote by. */
TEST_F(NooBaaPlanTest, TheMappingFollowsThePlan)
{
  add_identity("6ac7cb27fd4af356919110a3", NB_ACCOUNT_PLAIN);
  add_identity("u1", R"({"_id":"u1","name":"alice","email":"alice",
    "owner":"6ac7cb27fd4af356919110a3"})");
  ASSERT_EQ(build(2), 0);

  const auto m = nsfs::mapping_from(plan);
  const std::string acct = entry("interchange")->account_id;

  EXPECT_EQ(*m.resolve("6ac7cb27fd4af356919110a3"), acct);
  EXPECT_EQ(*m.resolve("interchange"), acct);
  /* an IAM user resolves to an ARN, because that is what an RGW user
   * principal is, and it matches on display_name */
  EXPECT_EQ(*m.resolve("u1"), "arn:aws:iam::" + acct + ":user/alice");
  EXPECT_EQ(*m.resolve("alice"), "arn:aws:iam::" + acct + ":user/alice");
}

/* Their bucket state reaches RGW:  the owner, the policy and logging.
 *
 * The owner is the field the state strategy reads and does not apply,
 * because turning their account id into an `rgw_owner` needs the
 * mapping.  `logging` is one of three fields nothing read before, and
 * a bucket converted without it loses its logging silently. */
class NooBaaBucketImportTest : public NSFSNooBaaStateTest {
public:
  nsfs::IdentityMapping mapping;
  nsfs::BucketImportResult result;

  void SetUp() override {
    NSFSNooBaaStateTest::SetUp();

    /* The mode has to be `adopt`, because writing a bucket's state is
     * a mutation and `serve` refuses mutations -- correctly:  a
     * read-only view of a deployment is exactly what serve is for.
     * These tests exercise the import, which is what `adopt` runs, so
     * they say so.  Found by the full-suite run after they passed on
     * their own, which is the mode gate working. */
    std::map<std::string, std::string> out;
    EXPECT_EQ(driver->driver_hint(env->dpp, "adoption",
				  {{"mode", "adopt"}}, &out), 0);

    mapping.account_by_noobaa_id["6560e1f1c0ffee0000000002"] =
      "RGW11111111111111111";
    mapping.noobaa_id_by_name["their-acct"] = "6560e1f1c0ffee0000000002";
  }

  int run() {
    nsfs::NooBaaStoreContents c;
    nsfs::NooBaaBucketRecord rec;
    rec.name = testname;
    c.buckets.push_back(rec);
    result = nsfs::BucketImportResult{};
    /* The fixture's bucket exists on disk;  release the handle it
     * holds so the import opens its own, which is what the real
     * sequence does -- identities, then buckets, with nothing else
     * holding them. */
    bucket.reset();
    return nsfs::import_bucket_state(env->dpp, null_yield, driver.get(),
				     conf_root().string(), c, mapping,
				     result);
  }

  std::string why() const {
    std::string s;
    for (const auto& f : result.failures) {
      s += "\n  " + f;
    }
    for (const auto& r : result.policies_refused) {
      s += "\n  " + r;
    }
    return s;
  }
};

TEST_F(NooBaaBucketImportTest, TheOwnerAndLoggingReachRgw)
{
  write_their_record("ENABLED",
      R"(,"logging":{"log_bucket":"audit","log_prefix":"logs/"})");

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.buckets_written, 1u);
  EXPECT_EQ(result.logging_carried, 1u);

  /* read it back through the bucket, which is what a request would */
  RGWBucketInfo info;
  info.bucket.name = testname;
  auto bucket = driver->get_bucket(info);
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);

  /* the owner the strategy would not apply */
  EXPECT_EQ(to_string(bucket->get_owner()), "RGW11111111111111111")
      << "owner_account was read and not applied";
  EXPECT_TRUE(bucket->versioned()) << "versioning was lost on the write";

  /* The attribute's presence is what says logging is configured:
   * `enabled` is an in-memory field and is not part of the encoded
   * form, so a round trip cannot carry it and asserting it would be
   * asserting nothing. */
  auto lg = bucket->get_attrs().find(RGW_ATTR_BUCKET_LOGGING);
  ASSERT_NE(lg, bucket->get_attrs().end()) << "logging was dropped";
  rgw::bucketlogging::configuration conf;
  auto bl = lg->second.cbegin();
  conf.decode(bl);
  EXPECT_EQ(conf.target_bucket, "audit");
  EXPECT_EQ(conf.target_prefix, "logs/");
}

/* A bucket whose owner this import did not write is refused, not
 * guessed.  There is no correct account to invent. */
TEST_F(NooBaaBucketImportTest, AnUnknownOwnerRefusesTheBucket)
{
  write_their_record("DISABLED");
  mapping.account_by_noobaa_id.clear();

  EXPECT_EQ(run(), -EIO);
  EXPECT_EQ(result.buckets_written, 0u);
  bool said = false;
  for (const auto& f : result.failures) {
    if (f.find("is not an account this import wrote") != std::string::npos) {
      said = true;
    }
  }
  EXPECT_TRUE(said) << why();
}

/* A policy whose principals do not resolve is dropped, the rest of
 * the bucket is written, and the refusal is reported.
 *
 * The bucket's other state is correct and worth having;  a policy
 * pointing at nothing is not. */
TEST_F(NooBaaBucketImportTest, ARefusedPolicyDoesNotRefuseTheBucket)
{
  write_their_record("ENABLED",
      R"(,"s3_policy":{"Version":"2012-10-17","Statement":[{
        "Effect":"Allow","Principal":{"AWS":["ghost"]},
        "Action":["s3:GetObject"],
        "Resource":["arn:aws:s3:::b/*"]}]})");

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.buckets_written, 1u) << "the bucket was refused too";
  EXPECT_EQ(result.policies_rewritten, 0u);
  ASSERT_EQ(result.policies_refused.size(), 1u);
  EXPECT_NE(result.policies_refused[0].find("ghost"), std::string::npos)
      << result.policies_refused[0];

  RGWBucketInfo info;
  info.bucket.name = testname;
  auto bucket = driver->get_bucket(info);
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  EXPECT_EQ(bucket->get_attrs().find(RGW_ATTR_IAM_POLICY),
	    bucket->get_attrs().end())
      << "a policy pointing at nothing was written anyway";
  EXPECT_TRUE(bucket->versioned()) << "the rest of the state was lost";
}

/* And a resolvable one is written, rewritten. */
TEST_F(NooBaaBucketImportTest, AResolvablePolicyIsRewrittenAndWritten)
{
  write_their_record("DISABLED",
      R"(,"s3_policy":{"Version":"2012-10-17","Statement":[{
        "Effect":"Allow","Principal":{"AWS":["their-acct"]},
        "Action":["s3:GetObject"],
        "Resource":["arn:aws:s3:::b/*"]}]})");

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.policies_rewritten, 1u);

  RGWBucketInfo info;
  info.bucket.name = testname;
  auto bucket = driver->get_bucket(info);
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  auto p = bucket->get_attrs().find(RGW_ATTR_IAM_POLICY);
  ASSERT_NE(p, bucket->get_attrs().end());
  const std::string stored = p->second.to_str();
  EXPECT_NE(stored.find("RGW11111111111111111"), std::string::npos)
      << stored;
  EXPECT_EQ(stored.find("their-acct"), std::string::npos)
      << "their name survived in the stored policy: " << stored;
}

/* What is read and not translated is reported rather than silent.
 *
 * Notifications need an RGW topic per destination;  a path outside the
 * data root needs P2 to serve it and P6 to record it.  Both are
 * deliberate boundaries, and an operator should learn them from the
 * report. */
TEST_F(NooBaaBucketImportTest, WhatWaitsForAnotherStageIsReported)
{
  /* A path genuinely outside the data root, said explicitly:  the
   * deferral is about that, and inheriting it from the fixture's
   * default made the test depend on a detail that then changed. */
  const sf::path elsewhere = bp / "another-filesystem" / testname;
  sf::create_directories(elsewhere);
  write_their_record("DISABLED",
      R"(,"notifications":[{"id":["n1"],"event":["s3:ObjectCreated:*"],
        "topic":["arn:noobaa:sns:::t1"]}])",
      "2026-01-02T03:04:05.000Z", elsewhere.string());

  ASSERT_EQ(run(), 0) << why();
  EXPECT_EQ(result.buckets_written, 1u);

  bool notif = false, path = false;
  for (const auto& d : result.deferred) {
    if (d.find("notifications") != std::string::npos) {
      notif = true;
    }
    if (d.find("not under the data root") != std::string::npos) {
      path = true;
    }
  }
  EXPECT_TRUE(notif) << "notifications were dropped silently";
  /* write_their_record() always writes a path under /ibm/gpfs, so the
   * path deferral is reached too */
  EXPECT_TRUE(path) << "a path outside the data root was not reported";
}

/* ------------------------------------------------------------------ *
 * I6 -- the mode, and what it does.
 *
 * There is no endpoint and no operator verb:  the mode is read at
 * startup and the import runs because of it.  The suite reaches it
 * through the `adoption` driver hint, which is how an intrusive
 * trigger stays out of a production surface.
 * ------------------------------------------------------------------ */

class NooBaaModeTest : public NSFSNooBaaStateTest {
public:
  /* The fixture's write_file() does not create parents, and an
   * identity lives at identities/<id>/identity.json. */
  void add_their_identity(const std::string& id, const std::string& record) {
    const auto dir = conf_root() / "identities" / id;
    std::filesystem::create_directories(dir);
    write_file(dir / "identity.json", record);
  }

  /* The account `write_their_record()`'s bucket is owned by.
   *
   * Their bucket record names its owner by `_id`, and the bucket
   * import refuses a bucket whose owner this run did not write -- so
   * the account has to carry the id the record names, not an
   * arbitrary one. */
  static constexpr const char* THEIR_OWNER_ID =
    "6560e1f1c0ffee0000000002";

  void add_the_owning_account() {
    std::string rec{NB_ACCOUNT_PLAIN};
    const std::string from = "6ac7cb27fd4af356919110a3";
    for (auto at = rec.find(from); at != std::string::npos;
	 at = rec.find(from)) {
      rec.replace(at, from.size(), THEIR_OWNER_ID);
    }
    add_their_identity(THEIR_OWNER_ID, rec);
  }

  std::map<std::string, std::string> ask(
      const std::map<std::string, std::string>& params = {}) {
    std::map<std::string, std::string> out;
    EXPECT_EQ(driver->driver_hint(env->dpp, "adoption", params, &out), 0);
    return out;
  }
};

/* The default is serve, and serve serves read-only.
 *
 * Writes are refused because they would be incomplete, not because
 * they are impossible:  their arrangement has nowhere to put an ACL,
 * so a write cannot record one, and an object written half into their
 * shape is one neither gateway reads correctly afterwards.  What is
 * left is a readable view of a deployment nothing has changed on
 * disk, which is what keeps the decision reversible.
 */
TEST_F(NooBaaModeTest, ServeIsTheDefaultAndRefusesWrites)
{
  auto st = ask();
  EXPECT_EQ(st["configured"], "true") << "the fixture has a store";
  EXPECT_EQ(st["mode"], "serve") << "the default changed";
  EXPECT_EQ(st["read_only"], "true");

  /* A write, through the gate every mutation takes. */
  EXPECT_EQ(driver->mutation_enter(env->dpp, testname),
	    -ERR_METHOD_NOT_ALLOWED);

  /* adopt lifts it, which is the other polarity -- a refusal that
   * cannot be lifted would say nothing about the mode */
  st = ask({{"mode", "adopt"}});
  EXPECT_EQ(st["mode"], "adopt");
  EXPECT_EQ(st["read_only"], "false");
  EXPECT_EQ(driver->mutation_enter(env->dpp, testname), 0);
  driver->mutation_leave(testname);
}

/* The mode is inert without a store.
 *
 * An ordinary nsfs deployment has nothing to adopt, so the option must
 * not change it by existing.  This is the fixture *without* a NooBaa
 * configuration root, which is every deployment today. */
TEST_F(NSFSBucketTest, TheAdoptionModeIsInertWithoutAStore)
{
  std::map<std::string, std::string> out;
  ASSERT_EQ(driver->driver_hint(env->dpp, "adoption", {}, &out), 0);
  EXPECT_EQ(out["configured"], "false");
  EXPECT_EQ(out["read_only"], "false")
      << "a deployment with nothing to adopt was made read-only";

  /* and writes work, which is the whole point */
  EXPECT_EQ(driver->mutation_enter(env->dpp, testname), 0);
  driver->mutation_leave(testname);
}

/* serve runs no import, and says why.
 *
 * Asking it to run in serve mode is not an error -- it is the mode
 * doing what it says -- so the hint reports rather than failing. */
TEST_F(NooBaaModeTest, ServeImportsNothing)
{
  auto st = ask({{"run", "true"}});
  EXPECT_EQ(st["ran"], "false");
  EXPECT_EQ(st["why"], "the mode is serve");
  EXPECT_TRUE(st.find("accounts") == st.end())
      << "serve got as far as importing";
}

/* adopt runs the import, over the store the fixture points at.
 *
 * End to end through the one entry point a deployment has:  phases 0
 * to 2, reported back through the hint so a test can see what each
 * phase did. */
TEST_F(NooBaaModeTest, AdoptRunsTheImport)
{
  /* an identity and a bucket record of theirs */
  add_the_owning_account();
  write_file(conf_root() / "master_keys.json", NB_MASTER_KEYS);
  write_their_record("ENABLED");

  auto st = ask({{"mode", "adopt"}, {"run", "true"}});
  EXPECT_EQ(st["ran"], "true") << "mode=" << st["mode"]
			       << " error=" << st["error"];
  EXPECT_EQ(st["accounts"], "1");
  EXPECT_EQ(st["users"], "1");
  EXPECT_EQ(st["identity_rows"], "1");

  /* the credential their record carried now authenticates */
  std::unique_ptr<rgw::sal::User> user;
  EXPECT_EQ(driver->get_user_by_access_key(env->dpp, "INTERCHANGEACCESSKEY",
					   null_yield, &user), 0);

  /* and a second run is a no-op, because re-running is the recovery */
  auto again = ask({{"run", "true"}});
  EXPECT_EQ(again["ran"], "true");
  EXPECT_EQ(again["accounts"], "0");
  EXPECT_EQ(again["accounts_present"], "1");
}

/* A store that will not preflight is refused before anything is
 * written, which is the point of phase 0. */
TEST_F(NooBaaModeTest, APreflightFailureWritesNothing)
{
  /* a record that does not parse */
  add_their_identity("bad", "{ not json");
  write_file(conf_root() / "master_keys.json", NB_MASTER_KEYS);

  std::map<std::string, std::string> out;
  const int ret = driver->driver_hint(
      env->dpp, "adoption", {{"mode", "adopt"}, {"run", "true"}}, &out);
  EXPECT_LT(ret, 0);
  EXPECT_EQ(out["ran"], "false");
  EXPECT_EQ(out["unreadable"], "1");
  EXPECT_TRUE(out.find("accounts") == out.end())
      << "a preflight failure got as far as writing";
}

/* Serve mode refuses an attribute write.
 *
 * `mutation_enter()` guards the mutating SAL entry points, which is
 * the right layer for a mutation and the wrong one for a write that
 * happens inside some other operation.  So the refusal is also taken
 * where the attribute is written.
 *
 * Both polarities, from one build:  the `serve-write-refusal` hint
 * turns the refusal off, and the same call then succeeds.  Without
 * that the assertion would only show that the write did not happen,
 * not that the refusal is what stopped it. */
TEST_F(NooBaaModeTest, ServeModeRefusesAnAttributeWrite)
{
  /* A real object, with an entry behind it:  `write_attrs()` writes
   * through the FSEnt, and one that was never opened has none. */
  ASSERT_EQ(ask({{"mode", "adopt"}})["mode"], "adopt");
  std::ofstream{(bp / "root" / testname / testname).string()} << "x";

  auto object = bucket->get_object(rgw_obj_key(testname));
  ASSERT_NE(object.get(), nullptr);
  auto* nsfs_obj = static_cast<rgw::sal::NSFSObject*>(object.get());
  ASSERT_EQ(nsfs_obj->load_obj_state(env->dpp, null_yield), 0)
      << "the object has no entry, so write_attrs would fault";

  bufferlist bl;
  bl.append("x");
  nsfs_obj->get_attrs()[ATTR1] = bl;

  ASSERT_EQ(ask({{"mode", "serve"}})["read_only"], "true");
  EXPECT_EQ(nsfs_obj->write_attrs(env->dpp, null_yield, nullptr), -EROFS)
      << "serve mode let an attribute write through";

  std::map<std::string, std::string> h;
  ASSERT_EQ(driver->driver_hint(env->dpp, "serve-write-refusal",
				{{"enable", "false"}}, &h), 0);
  EXPECT_EQ(h["enabled"], "false");
  EXPECT_EQ(nsfs_obj->write_attrs(env->dpp, null_yield, nullptr), 0)
      << "with the refusal off the write failed for some other reason, "
      << "so the assertion above is not about the refusal";
  ASSERT_EQ(driver->driver_hint(env->dpp, "serve-write-refusal",
				{{"enable", "true"}}, &h), 0);
}

/* `convert` was a mode and is not one.
 *
 * It meant "adopt and then rewrite the objects", which is what adopt
 * does -- adoption brings a bucket's schema and its data into our
 * profile -- so there was no operation for a third value to name.  A
 * deployment whose configuration still says `convert` is served
 * read-only, which is the reading that changes nothing on disk. */
TEST_F(NooBaaModeTest, ConvertIsNoLongerAMode)
{
  add_the_owning_account();
  write_file(conf_root() / "master_keys.json", NB_MASTER_KEYS);
  write_their_record("DISABLED");

  std::map<std::string, std::string> out;
  EXPECT_EQ(driver->driver_hint(env->dpp, "adoption",
				{{"mode", "convert"}}, &out),
	    -EINVAL)
      << "convert was accepted as a mode";

  /* and the mode it was asked to leave is untouched */
  auto st = ask({});
  EXPECT_EQ(st["mode"], "serve");
  EXPECT_EQ(st["read_only"], "true");
}

/* One bucket, adopted now.
 *
 * The increment the background sweep will drive, and the granularity
 * validation wants.  Identities are store-wide because a principal
 * resolves against the complete mapping;  buckets are the unit that
 * can be done one at a time. */
TEST_F(NooBaaModeTest, OneBucketCanBeAdoptedOnTheSpot)
{
  add_the_owning_account();
  write_file(conf_root() / "master_keys.json", NB_MASTER_KEYS);
  write_their_record("ENABLED");

  /* identities first, which is the store-wide half */
  auto st = ask({{"mode", "adopt"}, {"run", "true"}});
  ASSERT_EQ(st["ran"], "true") << st["error"];

  /* then the one bucket, which is the increment */
  auto one = ask({{"bucket", testname}});
  EXPECT_EQ(one["configured"], "true") << one["error"];
  EXPECT_EQ(one["buckets"], "1");

  /* Adopted means the objects are ours too, not just the record.
   *
   * Asserted because the report used to say "adopted" after writing
   * bucket configuration alone, while every object kept NooBaa's
   * arrangement -- so the startup path and this hint both reported a
   * state the tree was not in.  `profile` is what makes the claim
   * checkable:  a bucket that was only configured still reads base. */
  EXPECT_EQ(one["adopted"], "true") << one["error"];
  EXPECT_EQ(one["profile_set"], "true");
  /* Equality, not "not base":  the old report had no `profile` key at
   * all, and an absent key is not base either -- so a negative
   * assertion would have passed against the defect it is here to
   * catch.  `strong` is EXTENSIONS_DEFAULT, which is what a bucket
   * this gateway creates is marked with. */
  EXPECT_EQ(one["profile"], "strong")
      << "the bucket claims adoption and is not in our profile";

  /* and the state is on the bucket */
  RGWBucketInfo info;
  info.bucket.name = testname;
  auto bucket = driver->get_bucket(info);
  ASSERT_EQ(bucket->load_bucket(env->dpp, null_yield), 0);
  EXPECT_TRUE(bucket->versioned())
      << "their versioning did not reach the adopted bucket";

  /* a name the store has no record of is -ENOENT, not a silent
   * success -- an operator adopting a bucket by name needs to know
   * they named one that exists */
  std::map<std::string, std::string> out;
  EXPECT_EQ(driver->driver_hint(env->dpp, "adoption",
				{{"bucket", "no-such-bucket"}}, &out),
	    -ENOENT);
  EXPECT_EQ(out["adopted"], "false");
}

/* An unknown mode is refused rather than guessed. */
TEST_F(NooBaaModeTest, AnUnknownModeIsRefused)
{
  std::map<std::string, std::string> out;
  EXPECT_EQ(driver->driver_hint(env->dpp, "adoption",
				{{"mode", "sideways"}}, &out), -EINVAL);
  /* and the mode it had is unchanged */
  EXPECT_EQ(ask()["mode"], "serve");
}

/* ------------------------------------------------------------------ *
 * P2 -- resolution from a stored path.
 *
 * Two arms (NSFS_BUCKET_PLACEMENT_PLAN.md §7):  an index row's Path,
 * else `<base>/<name>`.  The second is what every deployment has, so
 * the test that matters most is that nothing changes without a row.
 * ------------------------------------------------------------------ */

class NSFSPlacedBucketTest : public NSFSDriverTest {
public:
  sf::path elsewhere() const { return bp / "second-filesystem"; }

  /* A row written by hand, which is how the plan asks for this to be
   * exercised:  P4 is what writes one in the course of creating a
   * bucket, and P6 is what the import writes. */
  void place(const std::string& name, const sf::path& at,
	     const std::string& tenant = "") {
    nsfs::BucketPlacement bp2;
    bp2.bucket_name = name;
    bp2.tenant = tenant;
    bp2.path = at.string();
    ASSERT_EQ(driver->get_identity_db()->put_bucket_placement(env->dpp,
							      bp2), 0);
  }

  std::unique_ptr<rgw::sal::Bucket> make_bucket(const std::string& name) {
    RGWBucketInfo info;
    info.bucket.name = name;
    info.owner = owner;
    info.creation_time = ceph::real_clock::now();
    auto b = driver->get_bucket(info);
    EXPECT_NE(b.get(), nullptr);
    rgw::sal::Bucket::CreateParams cp;
    cp.owner = owner;
    EXPECT_EQ(b->create(env->dpp, cp, null_yield), 0);
    return b;
  }
};

/* Without a row, nothing changes.
 *
 * This is the arm every existing deployment is on -- no index, no
 * migration -- so it is the one whose breaking would matter most. */
TEST_F(NSFSPlacedBucketTest, WithoutARowTheBucketIsWhereItAlwaysWas)
{
  auto bucket = make_bucket("ordinary");
  EXPECT_TRUE(sf::is_directory(bp / "root" / "ordinary"))
      << "a bucket with no placement row moved";

  std::unique_ptr<rgw::sal::Bucket> loaded;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", "ordinary"},
				&loaded, null_yield), 0);
  EXPECT_EQ(loaded->get_name(), "ordinary");
}

/* With a row, the bucket is served from the stored path.
 *
 * A second directory, outside the data root entirely, which is the
 * case the whole plan exists for:  `path` is required on every NooBaa
 * bucket record, so a real deployment's buckets are scattered. */
TEST_F(NSFSPlacedBucketTest, ARowServesTheBucketFromItsStoredPath)
{
  const sf::path at = elsewhere() / "placed";
  sf::create_directories(at);
  place("placed", at);

  std::unique_ptr<rgw::sal::Bucket> loaded;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", "placed"},
				&loaded, null_yield), 0)
      << "the bucket at " << at << " did not load";
  EXPECT_EQ(loaded->get_name(), "placed");

  /* and it is really that directory:  an object written to the bucket
   * lands there, and nothing appears under the data root */
  RGWBucketInfo info;
  info.bucket.name = "placed";
  info.owner = owner;
  auto b = driver->get_bucket(info);
  ASSERT_EQ(b->load_bucket(env->dpp, null_yield), 0);

  Attrs attrs;
  bufferlist bl;
  bl.append("x");
  attrs["user.rgw.probe"] = bl;
  ASSERT_EQ(b->merge_and_store_attrs(env->dpp, attrs, null_yield), 0);

  /* size 0 asks for the length;  a real buffer would have to be big
   * enough for an encoded RGWBucketInfo, and the question here is only
   * whether the attribute is there. */
  EXPECT_GT(::getxattr(at.c_str(), "user.nsfs.bucket_info", nullptr, 0), 0)
      << "the bucket's own state did not land at " << at;
  EXPECT_FALSE(sf::exists(bp / "root" / "placed"))
      << "a placed bucket also appeared under the data root";
}

/* The path is read on every lookup, not bound once.
 *
 * Theirs is read per lookup, and storage is attachable to a running
 * Scale cluster at any time -- so a design that binds a location once
 * is wrong on the platform this driver exists for (§6).  Moving the
 * row must move the next lookup. */
TEST_F(NSFSPlacedBucketTest, ThePathIsBoundPerLookup)
{
  const sf::path first = elsewhere() / "first";
  const sf::path second = elsewhere() / "second";
  sf::create_directories(first);
  sf::create_directories(second);

  place("moving", first);
  std::unique_ptr<rgw::sal::Bucket> a;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", "moving"}, &a,
				null_yield), 0);

  /* the row moves, and the *next* lookup follows it */
  place("moving", second);
  std::unique_ptr<rgw::sal::Bucket> b;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", "moving"}, &b,
				null_yield), 0);

  Attrs attrs;
  bufferlist bl;
  bl.append("y");
  attrs["user.rgw.probe"] = bl;
  ASSERT_EQ(b->merge_and_store_attrs(env->dpp, attrs, null_yield), 0);

  EXPECT_GT(::getxattr(second.c_str(), "user.nsfs.bucket_info", nullptr,
		       0), 0)
      << "the second lookup did not follow the moved row";
}

/* A row recording the default location is the same answer as no row.
 *
 * P4 writes a row for every bucket it creates, including the ones at
 * the default location, so an empty Path has to mean exactly what an
 * absent row means or every ordinary bucket breaks. */
TEST_F(NSFSPlacedBucketTest, AnEmptyPathMeansTheDefault)
{
  nsfs::BucketPlacement bp2;
  bp2.bucket_name = "defaulted";
  ASSERT_EQ(driver->get_identity_db()->put_bucket_placement(env->dpp,
							    bp2), 0);

  auto bucket = make_bucket("defaulted");
  EXPECT_TRUE(sf::is_directory(bp / "root" / "defaulted"))
      << "an empty Path did not mean the default location";
}

/* The tenant is part of the lookup, as it is of the key.
 *
 * Two tenants' buckets of the same name are two rows, so they must be
 * two directories. */
TEST_F(NSFSPlacedBucketTest, TheTenantSelectsTheRow)
{
  const sf::path a = elsewhere() / "tenant-a";
  sf::create_directories(a);
  place("shared", a, "tenant-a");

  /* the untenanted bucket has no row and stays at the default */
  auto plain = make_bucket("shared");
  EXPECT_TRUE(sf::is_directory(bp / "root" / "shared"));

  /* and the tenanted one resolves to its row */
  std::unique_ptr<rgw::sal::Bucket> loaded;
  ASSERT_EQ(driver->load_bucket(env->dpp,
				rgw_bucket{"tenant-a", "shared"}, &loaded,
				null_yield), 0);
  Attrs attrs;
  bufferlist bl;
  bl.append("z");
  attrs["user.rgw.probe"] = bl;
  ASSERT_EQ(loaded->merge_and_store_attrs(env->dpp, attrs, null_yield), 0);
  EXPECT_GT(::getxattr(a.c_str(), "user.nsfs.bucket_info", nullptr, 0), 0)
      << "the tenanted bucket did not resolve to its row";
}

/* ------------------------------------------------------------------ *
 * P5 -- a base bucket served at the path their record names.
 *
 * `path` is required on every NooBaa bucket record, so a real
 * deployment's buckets are scattered -- and in `serve` mode nothing is
 * written, so there is no index row to read instead.  Without this,
 * serve mode works only for buckets at the default location, which is
 * not what a deployment we have not touched looks like.
 * ------------------------------------------------------------------ */

class NSFSRecordedPathTest : public NSFSNooBaaStateTest {
public:
  sf::path their_storage() const { return bp / "their-filesystem"; }

  /* Where a bucket resolved to, read rather than written.
   *
   * These tests are about `serve` mode, which refuses mutations -- so
   * probing by writing would contradict the thing under test.  The
   * directory's own name is the answer and asking for it changes
   * nothing;  intrusive inspection is the right tool here. */
  static std::string resolved_at(rgw::sal::Bucket* b) {
    auto* nb = static_cast<rgw::sal::NSFSBucket*>(b);
    return nb->get_dir() ? nb->get_dir()->get_name() : std::string{};
  }

  /* Their record, naming a path of their choosing. */
  void record_at(const sf::path& at, const char* versioning = "DISABLED") {
    std::string doc = std::string(
      "{\"_id\":\"6560e1f1c0ffee0000000001\",\"name\":\"") + testname +
      "\",\"owner_account\":\"6560e1f1c0ffee0000000002\""
      ",\"versioning\":\"" + versioning + "\""
      ",\"path\":\"" + at.string() + "\""
      ",\"should_create_underlying_storage\":true"
      ",\"creation_date\":\"2026-01-02T03:04:05.000Z\"}";
    write_file(conf_root() / "buckets" / (testname + ".json"), doc);
  }
};

/* Their recorded path is where the bucket is served from.
 *
 * The read is the one serve mode already does for a bucket's
 * configuration, so this adds no consumer of their store. */
TEST_F(NSFSRecordedPathTest, ABaseBucketIsServedAtTheirRecordedPath)
{
  const sf::path at = their_storage() / "scattered";
  sf::create_directories(at);
  record_at(at, "ENABLED");

  std::unique_ptr<rgw::sal::Bucket> loaded;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", testname},
				&loaded, null_yield), 0)
      << "a bucket at the path their record names did not load";
  EXPECT_TRUE(loaded->versioned())
      << "their versioning did not come with it";
}

/* And resolving it writes nothing.
 *
 * Serve mode's whole value is that nothing changes on disk, so the
 * decision to adopt stays reversible.  Reading where a bucket lives
 * must not adopt it -- adoption happens because the mode says so, or
 * by explicit command. */
TEST_F(NSFSRecordedPathTest, ResolvingTheirPathDoesNotAdoptTheBucket)
{
  const sf::path at = their_storage() / "untouched";
  sf::create_directories(at);
  record_at(at);

  auto snapshot = [this, &at] {
    std::map<std::string, std::pair<time_t, off_t>> m;
    for (const auto& root : {at, conf_root(), bp / "root"}) {
      if (!sf::exists(root)) {
	continue;
      }
      for (const auto& e : sf::recursive_directory_iterator(root)) {
	struct stat st{};
	if (::lstat(e.path().c_str(), &st) == 0) {
	  m[e.path().string()] = {st.st_mtime, st.st_size};
	}
      }
    }
    return m;
  };

  const auto before = snapshot();

  std::unique_ptr<rgw::sal::Bucket> loaded;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", testname},
				&loaded, null_yield), 0);

  EXPECT_EQ(snapshot(), before)
      << "resolving their path changed something on disk";

  /* and no index row was invented for it */
  nsfs::BucketPlacement bp2;
  EXPECT_EQ(driver->get_identity_db()->get_bucket_placement(
		env->dpp, testname, "", bp2), -ENOENT)
      << "resolution wrote a placement row, so it adopted";

  /* The comparison can fail, shown without going through the driver:
   * serve mode refuses a write, which is the point, so the negative
   * case is made by touching the tree directly. */
  write_file(at / "probe", "x");
  EXPECT_NE(snapshot(), before)
      << "the comparison cannot detect a write, so it proves nothing";
}

/* A row of ours wins over their record.
 *
 * Once a bucket is adopted the index is the answer, and their store is
 * not consulted for it again -- which is what makes adoption an
 * adoption rather than a permanent bridge. */
TEST_F(NSFSRecordedPathTest, OurRowWinsOverTheirRecord)
{
  const sf::path theirs = their_storage() / "theirs";
  const sf::path ours = their_storage() / "ours";
  sf::create_directories(theirs);
  sf::create_directories(ours);
  record_at(theirs);

  nsfs::BucketPlacement row;
  row.bucket_name = testname;
  row.path = ours.string();
  ASSERT_EQ(driver->get_identity_db()->put_bucket_placement(env->dpp,
							    row), 0);

  std::unique_ptr<rgw::sal::Bucket> loaded;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", testname},
				&loaded, null_yield), 0);
  EXPECT_EQ(resolved_at(loaded.get()), ours.string())
      << "their recorded path was used although we had a row";
}

/* A row recording the default location stops there.
 *
 * It is our answer, so it must not fall through to their record --
 * otherwise adopting a bucket to the default location would be undone
 * by the record that named somewhere else. */
TEST_F(NSFSRecordedPathTest, ARowForTheDefaultDoesNotFallThrough)
{
  const sf::path theirs = their_storage() / "elsewhere";
  sf::create_directories(theirs);
  record_at(theirs);

  nsfs::BucketPlacement row;
  row.bucket_name = testname;	/* empty path:  the default location */
  ASSERT_EQ(driver->get_identity_db()->put_bucket_placement(env->dpp,
							    row), 0);

  std::unique_ptr<rgw::sal::Bucket> loaded;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", testname},
				&loaded, null_yield), 0);
  /* the default location renders as the bucket's own name under the
   * data root, not as an absolute path of theirs */
  EXPECT_NE(resolved_at(loaded.get()), theirs.string())
      << "a row for the default location fell through to their record";
}

/* A path their record names that we would not store is refused.
 *
 * Their store is one this driver did not write, so its contents are
 * input -- and a path we would reject at the point of storing is one
 * we will not serve from either (§9). */
TEST_F(NSFSRecordedPathTest, APathWeWouldNotStoreIsNotServedFrom)
{
  for (const char* bad : {"relative/path", "/a/../b", "/trailing/"}) {
    std::string doc = std::string(
      "{\"_id\":\"1\",\"name\":\"") + testname +
      "\",\"versioning\":\"DISABLED\",\"path\":\"" + bad + "\"}";
    write_file(conf_root() / "buckets" / (testname + ".json"), doc);

    /* falls back to the default location rather than following it */
    std::unique_ptr<rgw::sal::Bucket> loaded;
    const int ret = driver->load_bucket(env->dpp,
					rgw_bucket{"", testname}, &loaded,
					null_yield);
    EXPECT_EQ(ret, 0) << "refusing the path should fall back, not fail: "
		      << bad;
  }
}

/* With no store configured, nothing about resolution changes.
 *
 * Every deployment today, and the arm that must not break. */
TEST_F(NSFSBucketTest, WithoutATheirStoreResolutionIsUnchanged)
{
  std::unique_ptr<rgw::sal::Bucket> loaded;
  ASSERT_EQ(driver->load_bucket(env->dpp, rgw_bucket{"", testname},
				&loaded, null_yield), 0);
  EXPECT_TRUE(sf::is_directory(bp / "root" / testname));
}

int main(int argc, char *argv[]) {
  auto args = argv_to_vec(argc, argv);
  env_to_vec(args);

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_flag(args, arg_iter, "--create", (char*) nullptr)) {
      /* what already happens;  accepted so an existing invocation
       * keeps working */
      clean_first = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--reuse", (char*) nullptr)) {
      clean_first = false;
    } else if (ceph_argparse_flag(args, arg_iter, "--delete", (char*) nullptr)) {
      do_delete = true;
    } else if (ceph_argparse_flag(args, arg_iter, "--verbose", (char*) nullptr)) {
      verbose = true;
    } else {
      ++arg_iter;
    }
  }

  std::cout << "flags: clean_first=" << clean_first
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
