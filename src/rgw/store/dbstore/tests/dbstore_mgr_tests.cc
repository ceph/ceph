// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "rgw/store/dbstore/dbstore_mgr.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <memory>

using namespace rgw;
namespace fs = std::filesystem;
const static std::string TEST_DIR = "rgw_dbstore_tests";

bool endsWith(const std::string &mainStr, const std::string &toMatch)
{
    if(mainStr.size() >= toMatch.size() &&
            mainStr.compare(mainStr.size() - toMatch.size(), toMatch.size(), toMatch) == 0)
            return true;
        else
            return false;
}

class TestDBStoreManager : public ::testing::Test {
protected:
  void SetUp() override {
    ctx_ = std::make_shared<CephContext>(CEPH_ENTITY_TYPE_CLIENT);
    g_ceph_context = ctx_.get();
    fs::current_path(fs::temp_directory_path());
    fs::create_directory(TEST_DIR);
  }

  void TearDown() override {
    fs::current_path(fs::temp_directory_path());
    fs::remove_all(TEST_DIR);
  }

  std::string getTestDir() const {
    auto test_dir = fs::temp_directory_path() / TEST_DIR;
    return test_dir.string();
  }

  fs::path getDBFullPath(const std::string & base_dir,
                         const std::string & tenant) const {
    auto db_path = ctx_->_conf.get_val<std::string>("dbstore_db_dir");
    const auto& db_name = ctx_->_conf.get_val<std::string>("dbstore_db_name_prefix") + "-" + tenant + ".db";

    auto db_full_path = std::filesystem::path(db_path) / db_name;
    auto db_full_path_test = fs::path(base_dir) / db_full_path;
    return db_full_path_test;
  }

  std::string getDBTenant(const std::string & base_dir,
                          const std::string & tenant) const {
    auto db_name = ctx_->_conf.get_val<std::string>("dbstore_db_name_prefix");
    db_name += "-" + tenant;
    auto db_full_path = fs::path(base_dir) /  db_name;
    return db_full_path.string();
  }

  std::string getDBTenant(const std::string & tenant = default_tenant) const {
    return getDBTenant(getTestDir(), tenant);
  }

  fs::path getDBFullPath(const std::string & tenant) const {
    return getDBFullPath(getTestDir(), tenant);
  }

  fs::path getLogFilePath(const std::string & log_file) {
    return fs::temp_directory_path() / log_file;
  }

  std::shared_ptr<CephContext> getContext() const {
    return ctx_;
  }

 private:
    std::shared_ptr<CephContext> ctx_;
};

TEST_F(TestDBStoreManager, BasicInstantiateUsingDBDir) {
  getContext()->_conf.set_val("dbstore_db_dir", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath(default_tenant)));
  auto dbstore_mgr = std::make_shared<DBStoreManager>(getContext().get());
  EXPECT_TRUE(fs::exists(getDBFullPath(default_tenant)));
}

TEST_F(TestDBStoreManager, DBNamePrefix) {
  getContext()->_conf.set_val("dbstore_db_dir", getTestDir());
  std::string prefix = "testprefix";
  getContext()->_conf.set_val("dbstore_db_name_prefix", prefix);

  EXPECT_FALSE(fs::exists(getDBFullPath(default_tenant)));
  auto dbstore_mgr = std::make_shared<DBStoreManager>(getContext().get());
  EXPECT_TRUE(fs::exists(getDBFullPath(default_tenant)));

  // check that the database name contains the given prefix
  std::string expected_db_name = prefix + "-" + default_tenant + ".db";
  EXPECT_TRUE(endsWith(getDBFullPath(default_tenant), expected_db_name));
}

TEST_F(TestDBStoreManager, BasicInstantiateSecondConstructor) {
  getContext()->_conf.set_val("dbstore_db_dir", getTestDir());

  EXPECT_FALSE(fs::exists(getDBFullPath(default_tenant)));
  auto dbstore_mgr = std::make_shared<DBStoreManager>(getContext().get(), getLogFilePath("test.log").string(), 10);
  EXPECT_TRUE(fs::exists(getDBFullPath(default_tenant)));
}

TEST_F(TestDBStoreManager, TestDBName) {
  getContext()->_conf.set_val("dbstore_db_dir", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(getContext().get());
  auto db = dbstore_mgr->getDB(default_tenant, false);
  ASSERT_NE(nullptr, db);
  EXPECT_EQ(getDBTenant(), db->getDBname());
}


TEST_F(TestDBStoreManager, TestDBNameDefaultDB) {
  getContext()->_conf.set_val("dbstore_db_dir", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(getContext().get());
  // passing an empty tenant should return the default_db
  auto db = dbstore_mgr->getDB("", false);
  ASSERT_NE(nullptr, db);
  EXPECT_EQ(getDBTenant(), db->getDBname());
}

TEST_F(TestDBStoreManager, TestDBBadTenant) {
  getContext()->_conf.set_val("dbstore_db_dir", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(getContext().get());
  auto db = dbstore_mgr->getDB("does-not-exist", false);
  ASSERT_EQ(nullptr, db);
}

TEST_F(TestDBStoreManager, TestGetNewDB) {
  getContext()->_conf.set_val("dbstore_db_dir", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(getContext().get());

  auto new_tenant_path = "new_tenant";
  auto db = dbstore_mgr->getDB(new_tenant_path, true);
  ASSERT_NE(nullptr, db);
  EXPECT_EQ(getDBTenant(new_tenant_path), db->getDBname());
}

TEST_F(TestDBStoreManager, TestDelete) {
  getContext()->_conf.set_val("dbstore_db_dir", getTestDir());

  auto dbstore_mgr = std::make_shared<DBStoreManager>(getContext().get());
  dbstore_mgr->deleteDB(default_tenant);
  auto db = dbstore_mgr->getDB(default_tenant, false);
  ASSERT_EQ(nullptr, db);
}
