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
	     file::listing::MultipartCachePolicy::writethrough)
  {
    std::string cache_base = driver_base + "/cache";
    base_path = driver_base + "/root";

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
    init_multipart_cache(dpp, 16, 2, 2, 64, mp_policy);

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

    void SetUp() {
      testname = get_test_name();
      bp = sf::path{sf::absolute(sf::path{base_path / testname})};
      sf::create_directories(bp / "cache");
      sf::create_directories(bp / "root");
      driver = std::make_unique<TestDriver>(bp);
      int ret = driver->init(env->dpp, shares_extents(), mp_cache_policy());
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

TEST(NooBaaMPU, StagingRootIsFoundByPrefix)
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

  /* a second bucket id means two trees have written here, and which
   * holds the uploads is not decidable, so it refuses */
  sf::create_directories(base_path / test / ".noobaa-nsfs_def456");
  EXPECT_EQ(nb.staging_root(env->dpp, bfd), std::nullopt);
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

/* a uuid-shaped string;  NooBaa uses crypto.randomUUID() for both the
 * bucket id and the upload id, and nothing infers either */
std::string fake_uuid(const std::string& seed)
{
  std::string h = fmt::format("{:0>8x}", std::hash<std::string>{}(seed));
  return h.substr(0, 8) + "-" + h.substr(0, 4) + "-4" + h.substr(1, 3) +
	 "-8" + h.substr(2, 3) + "-" + h + h.substr(0, 4);
}

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

  EXPECT_EQ(nb.folder_object_name(), ".folder");
  EXPECT_EQ(nb.object_name(rgw_obj_key{"photos/"}, false), "photos/.folder");
  EXPECT_TRUE(nb.names_directory_object(".folder"));
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

  int bfd = ::open(b.c_str(), O_RDONLY | O_DIRECTORY);
  ASSERT_GE(bfd, 0);

  nsfs::PathStrategy::DirectoryObject d{};
  ASSERT_TRUE(nb.directory_object(env->dpp, bfd, "empty", d));
  EXPECT_EQ(d.size, 0u);
  EXPECT_FALSE(d.content_in_sentinel);

  d = {};
  ASSERT_TRUE(nb.directory_object(env->dpp, bfd, "withcontent", d));
  EXPECT_EQ(d.size, 17u);
  EXPECT_TRUE(d.content_in_sentinel);

  /* a directory without the attribute is not an object;  their own
   * read path throws NoSuchKey for one */
  d = {};
  EXPECT_FALSE(nb.directory_object(env->dpp, bfd, "plain", d));

  /* ours never marks a directory, and says so without looking */
  d = {};
  EXPECT_FALSE(ours.directory_object(env->dpp, bfd, "empty", d));

  ::close(bfd);
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
