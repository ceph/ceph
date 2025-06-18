#include <boost/asio/io_context.hpp>
#include <boost/asio/detached.hpp>
#include <boost/redis/connection.hpp>

#include "common/async/context_pool.h"

#include <sys/xattr.h>
#include <filesystem>
#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "rgw_auth_registry.h"
#include "rgw_aio_throttle.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"
#include "driver/dbstore/common/dbstore.h"
#include "rgw_sal_d4n.h"
#include "rgw_sal_filter.h"

#define dout_subsys ceph_subsys_rgw

const static std::string TEST_DIR = "d4n_filter_tests";
const static std::string CACHE_DIR = "/tmp/rgw_d4n_datacache";
const static std::string TEST_BUCKET = "test_bucket_";
const static std::string TEST_OBJ = "test_object_";
uint64_t ofs;

extern "C" {
extern rgw::sal::Driver* newD4NFilter(rgw::sal::Driver* next, boost::asio::io_context& io_context);
}

namespace fs = std::filesystem;
namespace net = boost::asio;
using boost::redis::config;
using boost::redis::connection;
using boost::redis::request;
using boost::redis::response;

std::string getTestDir() {
  auto test_dir = fs::temp_directory_path() / TEST_DIR;
  return test_dir.string();
}

void rethrow(std::exception_ptr eptr) {
  if (eptr) std::rethrow_exception(eptr);
}

class Environment* env;

class Environment : public ::testing::Environment {
  public:
    Environment() {}

    virtual ~Environment() {}

    void SetUp() override {
      std::vector<const char*> args;
      cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			CODE_ENVIRONMENT_UTILITY,
			CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

      env->cct.get()->_conf.set_val_or_die("dbstore_db_dir", getTestDir());
      common_init_finish(g_ceph_context);

      dpp = new DoutPrefix(cct->get(), dout_subsys, "D4N Object Directory Test: ");

      redisHost = cct->_conf->rgw_d4n_address; 
    }

    virtual void TearDown() {
      delete dpp;
      fs::remove_all(TEST_DIR);
    }

    std::string redisHost;
    boost::intrusive_ptr<ceph::common::CephContext> cct;
    DoutPrefixProvider* dpp;
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

class D4NFilterFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      fs::current_path(fs::temp_directory_path());
      fs::remove_all(TEST_DIR);
      fs::create_directory(TEST_DIR);

      env->cct->_conf->rgw_d4n_cache_cleaning_interval = 1;
      rgw_user uid{"test_tenant", "test_filter"};
      owner = uid;
      acl_owner.id = owner; 

      conn = new connection{net::make_strand(io)};
      ASSERT_NE(conn, nullptr);

      /* Run fixture's connection */
      config conf;
      conf.addr.host = env->redisHost.substr(0, env->redisHost.find(":"));
      conf.addr.port = env->redisHost.substr(env->redisHost.find(":") + 1, env->redisHost.length()); 

      conn->async_run(conf, {}, net::detached);

      const rgw::SiteConfig site_config;
      DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);
      cfg.store_name = "dbstore";
      cfg.filter_name = "d4n";

      auto filterDriver = DriverManager::get_raw_storage(env->dpp, g_ceph_context,
							  cfg, io, site_config);

      rgw::sal::Driver* next = filterDriver;
      driver = newD4NFilter(next, io);
      d4nFilter = dynamic_cast<rgw::sal::D4NFilterDriver*>(driver);

      /* Reset Redis state */
      net::spawn(io, [this] (net::yield_context yield) {
	boost::system::error_code ec;
	request req;
	req.push("FLUSHALL");
	response<boost::redis::ignore_t> resp;
	conn->async_exec(req, resp, yield[ec]);
      }, rethrow);
    } 

    virtual void TearDown() {
      delete conn;
    }

    void init_driver(net::yield_context yield) {
      dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});
      driver->initialize(env->cct.get(), env->dpp);

      ASSERT_NE(driver, nullptr);
    }

    void create_user(net::yield_context yield) {
      rgw_user u("test_tenant", "test_user", "ns");

      testUser = driver->get_user(u);
      testUser->get_info().user_id = u;

      ASSERT_EQ(testUser->store_user(env->dpp, optional_yield{yield}, false), 0);
    }

    void create_bucket(std::string name, net::yield_context yield) {
      rgw::sal::Bucket::CreateParams createParams;
      rgw_bucket b;
      init_bucket(&b, "test_tenant", "test_name", "test_data_pool", "test_index_pool", "test_marker", "test_id");

      EXPECT_EQ(driver->load_bucket(env->dpp, b, &testBucket, optional_yield{yield}), -2);
      ASSERT_EQ(testBucket->create(env->dpp, createParams, optional_yield{yield}), 0);
      testBucket->get_info().bucket.bucket_id = "test_bucket_" + name;
    }

    void put_object(std::string name, net::yield_context yield) {
      std::string object_name = "test_object_" + name;
      obj = testBucket->get_object(rgw_obj_key(object_name));
      ASSERT_NE(obj.get(), nullptr);
      obj.get()->set_obj_size(9);

      testWriter = driver->get_atomic_writer(env->dpp, 
					      optional_yield{yield},
					      obj.get(),
					      acl_owner,
					      nullptr,
					      0,
					      "test_filter");

      const req_context rctx{env->dpp, optional_yield{yield}, nullptr};
      ceph::real_time mtime; 

      buffer::list bl;
      bl.append("test_version\0", 13);
      /* DBStore does not provide the RGW_ATTR_ID_TAG attr, so it is being manually written
         here since D4N cache backend versioning relies on it. */
      rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};
      bl.append("test_etag");
      attrs.insert({RGW_ATTR_ETAG, std::move(bl)});

      bl.append("test data", 9);
      ASSERT_EQ(testWriter->prepare(optional_yield{yield}), 0);
      ASSERT_EQ(testWriter->process(std::move(bl), 0), 0);
      ASSERT_EQ(testWriter->complete(ofs, etag,
				     &mtime, real_time(),
				     attrs, std::nullopt,
				     real_time(),
				     nullptr, nullptr, nullptr,
				     nullptr, nullptr, rctx, 0), 0);
    }

    void put_version_enabled_object(std::string name, std::string& instance, net::yield_context yield) {
      testBucket->get_info().flags |= BUCKET_VERSIONED;
      std::string object_name = "test_object_" + name;
      objEnabled = testBucket->get_object(rgw_obj_key(object_name));
      ASSERT_NE(objEnabled.get(), nullptr);
      objEnabled.get()->set_obj_size(9);
      objEnabled->gen_rand_obj_instance_name();
      instance = objEnabled->get_instance();

      testWriter = driver->get_atomic_writer(env->dpp, 
					      optional_yield{yield},
					      objEnabled.get(),
					      acl_owner,
					      nullptr,
					      0,
					      "test_filter");

      const req_context rctx{env->dpp, optional_yield{yield}, nullptr};
      ceph::real_time mtime; 

      buffer::list bl;
      bl.append("test_version\0", 13);
      rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};
      bl.append("test_etag");
      attrs.insert({RGW_ATTR_ETAG, std::move(bl)});

      bl.append("test data", 9);
      ASSERT_EQ(testWriter->prepare(optional_yield{yield}), 0);
      ASSERT_EQ(testWriter->process(std::move(bl), 0), 0);
      ASSERT_EQ(testWriter->complete(ofs, etag,
				     &mtime, real_time(),
				     attrs, std::nullopt,
				     real_time(),
				     nullptr, nullptr, nullptr,
				     nullptr, nullptr, rctx, 0), 0);
    }

    void put_version_suspended_object(std::string name, net::yield_context yield) {
      testBucket->get_info().flags |= BUCKET_VERSIONS_SUSPENDED;
      std::string object_name = "test_object_" + name;
      objSuspended = testBucket->get_object(rgw_obj_key(object_name));
      ASSERT_NE(objSuspended.get(), nullptr);
      objSuspended.get()->set_obj_size(9);

      testWriter = driver->get_atomic_writer(env->dpp, 
					      optional_yield{yield},
					      objSuspended.get(),
					      acl_owner,
					      nullptr,
					      0,
					      "test_filter");

      const req_context rctx{env->dpp, optional_yield{yield}, nullptr};
      ceph::real_time mtime; 

      buffer::list bl;
      bl.append("test_version\0", 13);
      rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};
      bl.append("test_etag");
      attrs.insert({RGW_ATTR_ETAG, std::move(bl)});

      bl.append("test data", 9);
      ASSERT_EQ(testWriter->prepare(optional_yield{yield}), 0);
      ASSERT_EQ(testWriter->process(std::move(bl), 0), 0);
      ASSERT_EQ(testWriter->complete(ofs, etag,
				     &mtime, real_time(),
				     attrs, std::nullopt,
				     real_time(),
				     nullptr, nullptr, nullptr,
				     nullptr, nullptr, rctx, 0), 0);
    }


    rgw_owner owner;
    ACLOwner acl_owner;
    size_t ofs = 9;
    std::string etag = "test_etag";

    net::io_context io;
    connection* conn; 

    rgw::sal::Driver* driver;
    rgw::sal::D4NFilterDriver* d4nFilter;
    std::unique_ptr<rgw::sal::Object> obj;
    std::unique_ptr<rgw::sal::Object> objEnabled;
    std::unique_ptr<rgw::sal::Object> objSuspended;
    std::unique_ptr<rgw::sal::User> testUser = nullptr;
    std::unique_ptr<rgw::sal::Bucket> testBucket = nullptr;
    std::unique_ptr<rgw::sal::Writer> testWriter = nullptr;
};

class DriverDestructor {
  rgw::sal::Driver* driver;

public:
  explicit DriverDestructor(rgw::sal::D4NFilterDriver* _s) : driver(_s) {}
  ~DriverDestructor() {
    DriverManager::close_storage(driver);
  }
};

// Read cache tests, unversioned
TEST_F(D4NFilterFixture, PutObjectRead)
{
  const std::string testName = "PutObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    // Check directory values
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("HGET", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0", "version"); // To check cache block(s)

    response< int, int, 
             std::map<std::string, std::string>,
             std::map<std::string, std::string>,
             std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ(std::get<2>(resp).value().size(), 14);
    EXPECT_EQ(std::get<3>(resp).value().size(), 14);

    std::string version = std::get<4>(resp).value();
    std::error_code err;

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), true);  

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, GetObjectRead)
{
  const std::string testName = "GetObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    bufferlist bl;
    Read_CB cb(&bl);
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op());
    EXPECT_EQ(read_op->prepare(optional_yield{yield}, env->dpp), 0);
    EXPECT_EQ(read_op->iterate(env->dpp, 0, ofs, &cb, optional_yield{yield}), 0);
    
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs)); // Data block entry
    req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
    req.push("HGET", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0", "version"); // To check cache contents

    response< int, int, int, 
             std::map<std::string, std::string>,
             std::map<std::string, std::string>,
             std::map<std::string, std::string>,
             std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ((int)std::get<2>(resp).value(), 1);
    EXPECT_EQ(std::get<3>(resp).value().size(), 14);
    EXPECT_EQ(std::get<4>(resp).value().size(), 14);
    EXPECT_EQ(std::get<5>(resp).value().size(), 14);

    std::string version = std::get<6>(resp).value();
    std::error_code err;
    std::string testData; 
    std::ifstream testFile; 

    // Check cache contents
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), true);     
    testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    ASSERT_EQ(testData.empty(), false);
    EXPECT_EQ(testData, "test data");
 
    // Ensure data returned from GET op matches cached data
    EXPECT_EQ(testData, cb.save_bl->to_str());

    testFile.close();
    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver)); 
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyNoneObjectRead)
{
  const std::string testName = "CopyNoneObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    bl.append("dest_object_version\0", 20);
    rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};

    std::string tag;

    std::string destName = "dest_object";
    std::unique_ptr<rgw::sal::Object> destObj = testBucket->get_object(rgw_obj_key(destName));
    EXPECT_NE(destObj.get(), nullptr);

    int ret = obj->copy_object(acl_owner,
	     std::get<rgw_user>(owner),
	     &info,
	     zone,
	     destObj.get(),
	     testBucket.get(),
	     testBucket.get(),
	     placement,
	     &mtime,
	     &mtime,
	     nullptr,
	     nullptr,
	     false,
	     nullptr,
	     nullptr,
	     rgw::sal::ATTRSMOD_NONE,
	     false,
	     attrs, 
	     RGWObjCategory::Main,
	     0,
	     boost::none,
	     nullptr,
	     &tag, 
	     &tag,
	     nullptr,
	     nullptr,
	     env->dpp,
	     optional_yield({yield}));
    EXPECT_EQ(ret, 0);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");

    response< int, int, 
	     std::map<std::string, std::string>,
	     std::map<std::string, std::string> > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ(std::get<2>(resp).value().size(), 14);
    EXPECT_EQ(std::get<3>(resp).value().size(), 14);

    std::error_code err;
    std::string version = "test_version"; // Expected version for copy object
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + version, err), true);  

    /* TODO: DBStore has no copy_object implementation, so the below code will fail if uncommented. Once it is implemented, the following
       should be uncommented and added to the other copy_object unit tests for the read cache. */
    /*
    // Read copy object
    Read_CB cb(&bl);
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(destObj->get_read_op());
    EXPECT_EQ(read_op->prepare(optional_yield{yield}, env->dpp), 0);
    EXPECT_EQ(read_op->iterate(env->dpp, 0, ofs, &cb, optional_yield{yield}), 0);

    // Check cache contents for copy object
    std::string testData; 
    std::ifstream testFile; 
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + destName + "/" + oid, err), true);     
    testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + destName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    ASSERT_EQ(testData.empty(), false);
    EXPECT_EQ(testData, "test data");
 
    // Ensure data returned from GET op matches copy object's cached data
    EXPECT_EQ(testData, cb.save_bl->to_str());
    */    

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyMergeObjectRead)
{
  const std::string testName = "CopyMergeObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    bl.append("dest_object_version\0", 20);
    rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};

    std::string tag;

    std::string destName = "dest_object";
    std::unique_ptr<rgw::sal::Object> destObj = testBucket->get_object(rgw_obj_key(destName));
    EXPECT_NE(destObj.get(), nullptr);

    int ret = obj->copy_object(acl_owner,
	     std::get<rgw_user>(owner),
	     &info,
	     zone,
	     destObj.get(),
	     testBucket.get(),
	     testBucket.get(),
	     placement,
	     &mtime,
	     &mtime,
	     nullptr,
	     nullptr,
	     false,
	     nullptr,
	     nullptr,
	     rgw::sal::ATTRSMOD_MERGE,
	     false,
	     attrs, 
	     RGWObjCategory::Main,
	     0,
	     boost::none,
	     nullptr,
	     &tag, 
	     &tag,
	     nullptr,
	     nullptr,
	     env->dpp,
	     optional_yield({yield}));
    EXPECT_EQ(ret, 0);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");

    response< int, int, 
	     std::map<std::string, std::string>,
	     std::map<std::string, std::string> > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ(std::get<2>(resp).value().size(), 14);
    EXPECT_EQ(std::get<3>(resp).value().size(), 14);

    std::error_code err;
    std::string version = "dest_object_version"; // Expected version for copy object
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + version, err), true);  

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyReplaceObjectRead)
{
  const std::string testName = "CopyReplaceObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    bl.append("dest_object_version\0", 20);
    rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};

    std::string tag;
    
    std::string destName = "dest_object";
    std::unique_ptr<rgw::sal::Object> destObj = testBucket->get_object(rgw_obj_key(destName));
    EXPECT_NE(destObj.get(), nullptr);

    int ret = obj->copy_object(acl_owner,
	     std::get<rgw_user>(owner),
	     &info,
	     zone,
	     destObj.get(),
	     testBucket.get(),
	     testBucket.get(),
	     placement,
	     &mtime,
	     &mtime,
	     nullptr,
	     nullptr,
	     false,
	     nullptr,
	     nullptr,
	     rgw::sal::ATTRSMOD_REPLACE,
	     false,
	     attrs, 
	     RGWObjCategory::Main,
	     0,
	     boost::none,
	     nullptr,
	     &tag, 
	     &tag,
	     nullptr,
	     nullptr,
	     env->dpp,
	     optional_yield({yield}));
    EXPECT_EQ(ret, 0);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");

    response< int, int, 
	     std::map<std::string, std::string>,
	     std::map<std::string, std::string> > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ(std::get<2>(resp).value().size(), 14);
    EXPECT_EQ(std::get<3>(resp).value().size(), 14);

    std::error_code err;
    std::string version = "dest_object_version"; // Expected version for copy object
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + version, err), true);  

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, DeleteObjectRead)
{
  const std::string testName = "DeleteObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op());
    ASSERT_EQ(read_op->prepare(optional_yield{yield}, env->dpp), 0);
    ASSERT_EQ(read_op->iterate(env->dpp, 0, ofs, nullptr, optional_yield{yield}), 0);

    std::string version;
    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("HGET", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0", "version"); 

      response< int, int, int, std::string > resp;

      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      
      version = std::get<3>(resp).value();
    }

    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), true);     

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();
    EXPECT_EQ(del_op->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));

      response< int, int, int > resp;

      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 0);
      EXPECT_EQ((int)std::get<1>(resp).value(), 0);
      EXPECT_EQ((int)std::get<2>(resp).value(), 0);
    }

    /* TODO: Eviction cycle to delete cache blocks
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), false);  
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), false);     
    */

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

// Read cache tests, versioned
TEST_F(D4NFilterFixture, PutVersionedObjectRead)
{
  const std::string testName = "PutVersionedObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance;
    put_version_enabled_object(testName, instance, yield);
    put_version_suspended_object(testName, yield);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");

    response< int, int, 
              std::map<std::string, std::string>,
              std::map<std::string, std::string> > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ(std::get<2>(resp).value().size(), 14);
    EXPECT_EQ(std::get<3>(resp).value().size(), 14);

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, GetVersionedObjectRead)
{
  const std::string testName = "GetVersionedObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance;
    put_version_enabled_object(testName, instance, yield);
    put_version_suspended_object(testName, yield);

    // For version enabled object
    bufferlist blEnabled;
    Read_CB cbEnabled(&blEnabled);
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op_enabled(objEnabled->get_read_op());
    EXPECT_EQ(read_op_enabled->prepare(optional_yield{yield}, env->dpp), 0);
    EXPECT_EQ(read_op_enabled->iterate(env->dpp, 0, ofs, &cbEnabled, optional_yield{yield}), 0);

    // For version suspended object
    bufferlist blSuspended;
    Read_CB cbSuspended(&blSuspended);
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op_suspended(objSuspended->get_read_op());
    EXPECT_EQ(read_op_suspended->prepare(optional_yield{yield}, env->dpp), 0);
    EXPECT_EQ(read_op_suspended->iterate(env->dpp, 0, ofs, &cbSuspended, optional_yield{yield}), 0);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
    req.push("HGET", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0", "version");

    response< int, int, int, 
              std::map<std::string, std::string>, 
              std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ((int)std::get<2>(resp).value(), 1);
    EXPECT_EQ(std::get<3>(resp).value().size(), 14);

    std::string version = std::get<4>(resp).value();
    std::error_code err;
    std::string testData; 
    std::ifstream testFile; 

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + instance, err), true);  
    std::string oid = instance + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), true);     
    testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    ASSERT_EQ(testData.empty(), false);
    EXPECT_EQ(testData, "test data");
 
    // Ensure data returned from GET op matches cached data
    EXPECT_EQ(testData, cbEnabled.save_bl->to_str());

    testFile.close();

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), true);     
    testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    ASSERT_EQ(testData.empty(), false);
    EXPECT_EQ(testData, "test data");
 
    // Ensure data returned from GET op matches cached data
    EXPECT_EQ(testData, cbSuspended.save_bl->to_str());

    testFile.close();
    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyNoneVersionedObjectRead)
{
  const std::string testName = "CopyNoneVersionedObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance;
    put_version_enabled_object(testName, instance, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    bl.append("dest_object_version\0", 20);
    rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};

    std::string tag;
    
    {
      std::string destNameEnabled = "dest_object_enabled";
      std::unique_ptr<rgw::sal::Object> destObjEnabled = testBucket->get_object(rgw_obj_key(destNameEnabled));
      EXPECT_NE(destObjEnabled.get(), nullptr);
      destObjEnabled->gen_rand_obj_instance_name();
      instance = destObjEnabled->get_instance();

      int ret = objEnabled->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjEnabled.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_NONE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");

      response< int, std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value().size(), 14);

      std::error_code err;
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + instance, err), true);  
    }

    put_version_suspended_object(testName, yield);

    {
      std::string destNameSuspended = "dest_object_suspended";
      std::unique_ptr<rgw::sal::Object> destObjSuspended = testBucket->get_object(rgw_obj_key(destNameSuspended));
      EXPECT_NE(destObjSuspended.get(), nullptr);

      int ret = objSuspended->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjSuspended.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_NONE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");

      response< int, std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value().size(), 14);

      std::error_code err;
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/test_version", err), true);  
    }

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyMergeVersionedObjectRead)
{
  const std::string testName = "CopyMergeVersionedObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance;
    put_version_enabled_object(testName, instance, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    bl.append("dest_object_version\0", 20);
    rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};

    std::string tag;
    
    {
      std::string destNameEnabled = "dest_object_enabled";
      std::unique_ptr<rgw::sal::Object> destObjEnabled = testBucket->get_object(rgw_obj_key(destNameEnabled));
      EXPECT_NE(destObjEnabled.get(), nullptr);
      destObjEnabled->gen_rand_obj_instance_name();
      instance = destObjEnabled->get_instance();

      int ret = objEnabled->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjEnabled.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_MERGE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");

      response< int, std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value().size(), 14);

      std::error_code err;
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + instance, err), true);  
    }

    put_version_suspended_object(testName, yield);

    {
      std::string destNameSuspended = "dest_object_suspended";
      std::unique_ptr<rgw::sal::Object> destObjSuspended = testBucket->get_object(rgw_obj_key(destNameSuspended));
      EXPECT_NE(destObjSuspended.get(), nullptr);

      int ret = objEnabled->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjSuspended.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_MERGE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");

      response< int, std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value().size(), 14);

      std::error_code err;
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/dest_object_version", err), true);  
    }

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyReplaceVersionedObjectRead)
{
  const std::string testName = "CopyReplaceVersionedObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance;
    put_version_enabled_object(testName, instance, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    bl.append("dest_object_version\0", 20);
    rgw::sal::Attrs attrs{{RGW_ATTR_ID_TAG, std::move(bl)}};

    std::string tag;
    
    {
      std::string destNameEnabled = "dest_object_enabled";
      std::unique_ptr<rgw::sal::Object> destObjEnabled = testBucket->get_object(rgw_obj_key(destNameEnabled));
      EXPECT_NE(destObjEnabled.get(), nullptr);
      destObjEnabled->gen_rand_obj_instance_name();
      instance = destObjEnabled->get_instance();

      int ret = objEnabled->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjEnabled.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_REPLACE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");

      response< int, std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value().size(), 14);

      std::error_code err;
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + instance, err), true);  
    }

    put_version_suspended_object(testName, yield);

    {
      std::string destNameSuspended = "dest_object_suspended";
      std::unique_ptr<rgw::sal::Object> destObjSuspended = testBucket->get_object(rgw_obj_key(destNameSuspended));
      EXPECT_NE(destObjSuspended.get(), nullptr);

      int ret = objEnabled->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjSuspended.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_REPLACE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");

      response< int, std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ(std::get<1>(resp).value().size(), 14);

      std::error_code err;
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/dest_object_version", err), true);  
    }

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, DeleteVersionedObjectRead)
{
  const std::string testName = "DeleteVersionedObjectRead";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance;
    put_version_enabled_object(testName, instance, yield);
    put_version_suspended_object(testName, yield);

    std::unique_ptr<rgw::sal::Object::ReadOp> read_op_enabled(objEnabled->get_read_op());
    EXPECT_EQ(read_op_enabled->prepare(optional_yield{yield}, env->dpp), 0);
    EXPECT_EQ(read_op_enabled->iterate(env->dpp, 0, ofs, nullptr, optional_yield{yield}), 0);

    std::unique_ptr<rgw::sal::Object::ReadOp> read_op_suspended(objSuspended->get_read_op());
    EXPECT_EQ(read_op_suspended->prepare(optional_yield{yield}, env->dpp), 0);
    EXPECT_EQ(read_op_suspended->iterate(env->dpp, 0, ofs, nullptr, optional_yield{yield}), 0);
    std::string version;
    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
      req.push("HGET", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0", "version");

      response< int, int, int, std::string > resp;

      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      
      version = std::get<3>(resp).value();
    }

    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + instance, err), true);  
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), true);     
    oid = instance + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), true);     

    // For version enabled object
    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op_enabled = objEnabled->get_delete_op();
    EXPECT_EQ(del_op_enabled->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);

    // For version suspended object
    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op_suspended = objSuspended->get_delete_op();
    EXPECT_EQ(del_op_suspended->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");

      response< int, int, int, int, int > resp;

      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 0);
      EXPECT_EQ((int)std::get<1>(resp).value(), 0);
      EXPECT_EQ((int)std::get<2>(resp).value(), 0);
      EXPECT_EQ((int)std::get<3>(resp).value(), 0);
      EXPECT_EQ((int)std::get<4>(resp).value(), 0);
    }

    /* TODO: Eviction cycle to delete cache blocks
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + instance, err), false);  
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), false);     
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), false);  
    oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), false);     
    */

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

// Write cache tests, unversioned
TEST_F(D4NFilterFixture, PutObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "PutObjectWrite";
  const std::string bucketName = "/tmp/d4n_filter_tests/dbstore-default_ns.1";
  std::string version;
 
  net::spawn(io, [this, &testName, &bucketName, &version] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    // Overwrite testName because the cleaning method derives the bucket name differently from other ops.
    testBucket->get_info().bucket.bucket_id = bucketName;
    put_object(testName, yield);

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName); // obj dir entry
      req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", bucketName + "__:null_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("ZREVRANGE", bucketName + "_" + TEST_OBJ + testName, "0", "-1");
      req.push("HGETALL", bucketName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("HGETALL", bucketName + "__:null_"  + TEST_OBJ + testName + "_0_0");
      req.push("HGETALL", bucketName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("HGET", bucketName + "_" + TEST_OBJ + testName + "_0_0", "dirty");
      req.push("HGET", bucketName + "_" + TEST_OBJ + testName + "_0_0", "version");

      response< int, int, int, int, 
		std::vector<std::string>,
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::string, std::string > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], "null");
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);
      EXPECT_EQ(std::get<8>(resp).value(), "1");

      version = std::get<9>(resp).value();
    }

    std::error_code err;
    std::string testData; 
    std::ifstream testFile; 

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    std::string oid = version + "#0#" + std::to_string(ofs);
    std::string location = CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid;
    EXPECT_EQ(fs::exists(location, err), true);     
    
    std::string attr_val;
    EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
    EXPECT_EQ(attr_val, "1");

    testFile.open(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    ASSERT_EQ(testData.empty(), false);
    EXPECT_EQ(testData, "test data");
    testFile.close();

    // The cleaning method must use a yield that is not out of scope, so set back to null_yield before exiting this coroutine
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &testName, &bucketName, &version] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    {
      boost::system::error_code ec;
      request req;
      req.push("HGET", bucketName + "_" + TEST_OBJ + testName + "_0_0", "dirty");

      response< std::string > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), "0");
    }

    // Ensure object is written to backend
    auto next = dynamic_cast<rgw::sal::FilterObject*>(obj.get())->get_next();
    EXPECT_EQ(next->load_obj_state(env->dpp, optional_yield{yield}), 0);
    EXPECT_EQ(next->exists(), 1);

    std::string oid = version + "#0#" + std::to_string(ofs);
    std::string location = CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid;
    std::string attr_val;
    EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
    EXPECT_EQ(attr_val, "0");

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);
 
  io.run();
}

TEST_F(D4NFilterFixture, GetObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  const std::string testName = "GetObjectWrite";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    bufferlist bl;
    Read_CB cb(&bl);
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(obj->get_read_op());
    EXPECT_EQ(read_op->prepare(optional_yield{yield}, env->dpp), 0);
    EXPECT_EQ(read_op->iterate(env->dpp, 0, ofs, &cb, optional_yield{yield}), 0);
    
    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName);
    req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
    req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + TEST_OBJ + testName, "0", "-1");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_"  + TEST_OBJ + testName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
    req.push("HGET", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0", "version");

    response< int, int, int, int, 
              std::vector<std::string>,
              std::map<std::string, std::string>,
              std::map<std::string, std::string>,
              std::map<std::string, std::string>,
              std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ((int)std::get<2>(resp).value(), 1);
    EXPECT_EQ((int)std::get<3>(resp).value(), 1);
    EXPECT_EQ(std::get<4>(resp).value()[0], "null");
    EXPECT_EQ(std::get<5>(resp).value().size(), 14);
    EXPECT_EQ(std::get<6>(resp).value().size(), 14);
    EXPECT_EQ(std::get<7>(resp).value().size(), 14);

    std::string version = std::get<8>(resp).value();
    std::error_code err;
    std::string testData; 
    std::ifstream testFile; 

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), true);     
    testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    EXPECT_EQ(testData, "test data");
 
    // Ensure data returned from GET op matches cached data
    EXPECT_EQ(testData, cb.save_bl->c_str());

    testFile.close();
    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyNoneObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  const std::string testName = "CopyNoneObjectWrite";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    {
      bl.append("test_value", 10);
      rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
      obj->set_obj_attrs(env->dpp, &attrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);
    }
    bl.append("copy_value", 10);
    rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
    
    std::string tag;
    
    std::string destName = "dest_object";
    std::unique_ptr<rgw::sal::Object> destObj = testBucket->get_object(rgw_obj_key(destName));
    EXPECT_NE(destObj.get(), nullptr);

    int ret = obj->copy_object(acl_owner,
	     std::get<rgw_user>(owner),
	     &info,
	     zone,
	     destObj.get(),
	     testBucket.get(),
	     testBucket.get(),
	     placement,
	     &mtime,
	     &mtime,
	     nullptr,
	     nullptr,
	     false,
	     nullptr,
	     nullptr,
	     rgw::sal::ATTRSMOD_NONE,
	     false,
	     attrs, 
	     RGWObjCategory::Main,
	     0,
	     boost::none,
	     nullptr,
	     &tag, 
	     &tag,
	     nullptr,
	     nullptr,
	     env->dpp,
	     optional_yield({yield}));
    EXPECT_EQ(ret, 0);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName);
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_" + std::to_string(ofs));
    req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destName, "0", "-1");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_" + std::to_string(ofs));
    req.push("HGET", TEST_BUCKET + testName + "_" + destName + "_0_0", "version");

    response< int, int, int, int, 
              std::vector<std::string>,
              std::map<std::string, std::string>,
              std::map<std::string, std::string>,
              std::map<std::string, std::string>,
              std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ((int)std::get<2>(resp).value(), 1);
    EXPECT_EQ((int)std::get<3>(resp).value(), 1);
    EXPECT_EQ(std::get<4>(resp).value()[0], "null");
    EXPECT_EQ(std::get<5>(resp).value().size(), 14);
    EXPECT_EQ(std::get<6>(resp).value().size(), 14);
    EXPECT_EQ(std::get<7>(resp).value().size(), 14);

    std::string version = std::get<8>(resp).value();
    std::error_code err;
    std::string testData; 
    std::ifstream testFile; 

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + version, err), true);
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + oid, err), true);     
    testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    EXPECT_EQ(testData, "test data");

    // Ensure attr is not modified
    rgw_obj copyObj = destObj->get_obj();
    ASSERT_EQ(destObj->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
    rgw::sal::Attrs copyAttrs = destObj->get_attrs();
    buffer::list val = copyAttrs["user.rgw.test_attr"];
    EXPECT_EQ(val.to_str(), "test_value");  

    testFile.close();
    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyMergeObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  const std::string testName = "CopyMergeObjectWrite";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    {
      bl.append("test_value", 10);
      rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
      obj->set_obj_attrs(env->dpp, &attrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);
    }
    bl.append("copy_value", 10);
    rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
    
    std::string tag;

    std::string destName = "dest_object";
    std::unique_ptr<rgw::sal::Object> destObj = testBucket->get_object(rgw_obj_key(destName));
    EXPECT_NE(destObj.get(), nullptr);

    int ret = obj->copy_object(acl_owner,
	     std::get<rgw_user>(owner),
	     &info,
	     zone,
	     destObj.get(),
	     testBucket.get(),
	     testBucket.get(),
	     placement,
	     &mtime,
	     &mtime,
	     nullptr,
	     nullptr,
	     false,
	     nullptr,
	     nullptr,
	     rgw::sal::ATTRSMOD_MERGE,
	     false,
	     attrs, 
	     RGWObjCategory::Main,
	     0,
	     boost::none,
	     nullptr,
	     &tag, 
	     &tag,
	     nullptr,
	     nullptr,
	     env->dpp,
	     optional_yield({yield}));
    EXPECT_EQ(ret, 0);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName);
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_" + std::to_string(ofs));
    req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destName, "0", "-1");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_" + std::to_string(ofs));
    req.push("HGET", TEST_BUCKET + testName + "_" + destName + "_0_0", "version");

    response< int, int, int, int, 
              std::vector<std::string>,
              std::map<std::string, std::string>,
              std::map<std::string, std::string>,
              std::map<std::string, std::string>,
              std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ((int)std::get<2>(resp).value(), 1);
    EXPECT_EQ((int)std::get<3>(resp).value(), 1);
    EXPECT_EQ(std::get<4>(resp).value()[0], "null");
    EXPECT_EQ(std::get<5>(resp).value().size(), 14);
    EXPECT_EQ(std::get<6>(resp).value().size(), 14);
    EXPECT_EQ(std::get<7>(resp).value().size(), 14);

    std::string version = std::get<8>(resp).value();
    std::error_code err;
    std::string testData; 
    std::ifstream testFile; 

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + version, err), true);
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + oid, err), true);     
    testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    EXPECT_EQ(testData, "test data");

    // Ensure attr is merged 
    rgw_obj copyObj = destObj->get_obj();
    ASSERT_EQ(destObj->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
    rgw::sal::Attrs copyAttrs = destObj->get_attrs();
    buffer::list val = copyAttrs["user.rgw.test_attr"];
    EXPECT_EQ(val.to_str(), "copy_value");  

    testFile.close();
    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyReplaceObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  const std::string testName = "CopyReplaceObjectWrite";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    {
      bl.append("test_value", 10);
      rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
      obj->set_obj_attrs(env->dpp, &attrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);
    }
    bl.append("copy_value", 10);
    rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
    
    std::string tag;
    
    std::string destName = "dest_object";
    std::unique_ptr<rgw::sal::Object> destObj = testBucket->get_object(rgw_obj_key(destName));
    EXPECT_NE(destObj.get(), nullptr);

    int ret = obj->copy_object(acl_owner,
	     std::get<rgw_user>(owner),
	     &info,
	     zone,
	     destObj.get(),
	     testBucket.get(),
	     testBucket.get(),
	     placement,
	     &mtime,
	     &mtime,
	     nullptr,
	     nullptr,
	     false,
	     nullptr,
	     nullptr,
	     rgw::sal::ATTRSMOD_REPLACE,
	     false,
	     attrs, 
	     RGWObjCategory::Main,
	     0,
	     boost::none,
	     nullptr,
	     &tag, 
	     &tag,
	     nullptr,
	     nullptr,
	     env->dpp,
	     optional_yield({yield}));
    EXPECT_EQ(ret, 0);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName);
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("EXISTS", TEST_BUCKET + testName + "_" + destName + "_0_" + std::to_string(ofs));
    req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destName, "0", "-1");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destName + "_0_0");
    req.push("HGETALL", TEST_BUCKET + testName + "_" + destName + "_0_" + std::to_string(ofs));
    req.push("HGET", TEST_BUCKET + testName + "_" + destName + "_0_0", "version");

    response< int, int, int, int, 
              std::vector<std::string>,
              std::map<std::string, std::string>,
              std::map<std::string, std::string>,
              std::map<std::string, std::string>,
              std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ((int)std::get<2>(resp).value(), 1);
    EXPECT_EQ((int)std::get<3>(resp).value(), 1);
    EXPECT_EQ(std::get<4>(resp).value()[0], "null");
    EXPECT_EQ(std::get<5>(resp).value().size(), 14);
    EXPECT_EQ(std::get<6>(resp).value().size(), 14);
    EXPECT_EQ(std::get<7>(resp).value().size(), 14);

    std::string version = std::get<8>(resp).value();
    std::error_code err;
    std::string testData; 
    std::ifstream testFile; 

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + version, err), true); 
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + oid, err), true);     
    testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    EXPECT_EQ(testData, "test data");
    
    // Ensure attr is replaced
    rgw_obj copyObj = destObj->get_obj();
    ASSERT_EQ(destObj->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
    rgw::sal::Attrs copyAttrs = destObj->get_attrs();
    buffer::list val = copyAttrs["user.rgw.test_attr"];
    EXPECT_EQ(val.to_str(), "copy_value");  

    testFile.close();
    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, DeleteObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "DeleteObjectWrite";
  std::string version;
 
  net::spawn(io, [this, &testName, &version] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    put_object(testName, yield);

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("HGET", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0", "version"); 

      response< int, int, int, int, std::string > resp;

      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      
      version = std::get<4>(resp).value();
    }

    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), true);     

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = obj->get_delete_op();
    EXPECT_EQ(del_op->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);

    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &testName, &version] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));

      response< int, int, int, int > resp;

      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 0);
      EXPECT_EQ((int)std::get<1>(resp).value(), 0);
      EXPECT_EQ((int)std::get<2>(resp).value(), 0);
      EXPECT_EQ((int)std::get<3>(resp).value(), 0);
    }

    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), false);  
    std::string oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid, err), false);     

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

// Write cache tests, versioned
TEST_F(D4NFilterFixture, PutVersionedObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "PutVersionedObjectWrite";
  const std::string bucketName = "/tmp/d4n_filter_tests/dbstore-default_ns.1";
  std::string version, instance;
 
  net::spawn(io, [this, &testName, &bucketName, &version, &instance] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    testBucket->get_info().bucket.bucket_id = bucketName;
    put_version_enabled_object(testName, instance, yield);
    put_version_suspended_object(testName, yield);

    {
      boost::system::error_code ec;
      request req;
      req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName);
      req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("EXISTS", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("EXISTS", bucketName + "__:null_"  + TEST_OBJ + testName + "_0_0");
      req.push("ZREVRANGE", bucketName + "_" + TEST_OBJ + testName, "0", "-1");
      req.push("HGETALL", bucketName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("HGETALL", bucketName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("HGETALL", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
      req.push("HGETALL", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("HGETALL", bucketName + "__:null_"  + TEST_OBJ + testName + "_0_0");
      req.push("HGET", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0", "dirty");
      req.push("HGET", bucketName + "__:null_" + TEST_OBJ + testName + "_0_0", "dirty");
      req.push("HGET", bucketName + "__:null_" + TEST_OBJ + testName + "_0_0", "version");

      response< int, int, int, int, int, int, 
		std::vector<std::string>, 
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::string, std::string,
		std::string > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ((int)std::get<4>(resp).value(), 1);
      EXPECT_EQ((int)std::get<5>(resp).value(), 1);
      EXPECT_EQ(std::get<6>(resp).value()[0], "null");
      EXPECT_EQ(std::get<6>(resp).value()[1], instance);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);
      EXPECT_EQ(std::get<8>(resp).value().size(), 14);
      EXPECT_EQ(std::get<9>(resp).value().size(), 14);
      EXPECT_EQ(std::get<10>(resp).value().size(), 14);
      EXPECT_EQ(std::get<11>(resp).value().size(), 14);
      EXPECT_EQ(std::get<12>(resp).value(), "1");
      EXPECT_EQ(std::get<13>(resp).value(), "1");

      version = std::get<14>(resp).value();
    }

    std::error_code err;
    std::string testData; 
    std::ifstream testFile; 
    std::string attr_val;

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + instance, err), true);  
    std::string oid = instance + "#0#" + std::to_string(ofs);
    std::string location = CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid;
    EXPECT_EQ(fs::exists(location, err), true);     

    EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
    EXPECT_EQ(attr_val, "1");

    testFile.open(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    EXPECT_EQ(testData, "test data");
    testFile.close();

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    oid = version + "#0#" + std::to_string(ofs);
    location = CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid;
    EXPECT_EQ(fs::exists(location, err), true);     
    
    EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
    EXPECT_EQ(attr_val, "1");

    testFile.open(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid);
    ASSERT_EQ(testFile.is_open(), true);
    getline(testFile, testData);
    EXPECT_EQ(testData, "test data");
    testFile.close();

    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &testName, &bucketName, &version, &instance] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    {
      boost::system::error_code ec;
      request req;
      req.push("HGET", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0", "dirty");
      req.push("HGET", bucketName + "__:null_" + TEST_OBJ + testName + "_0_0", "dirty");

      response< std::string, std::string > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value(), "0");
      EXPECT_EQ(std::get<1>(resp).value(), "0");
    }

    // Ensure object with versioning enabled is written to backend
    {
      auto next = dynamic_cast<rgw::sal::FilterObject*>(objEnabled.get())->get_next();
      EXPECT_EQ(next->load_obj_state(env->dpp, optional_yield{yield}), 0);
      EXPECT_EQ(next->exists(), 1);
    }

    // Ensure object with versioning suspended is written to backend
    {
      auto next = dynamic_cast<rgw::sal::FilterObject*>(objSuspended.get())->get_next();
      EXPECT_EQ(next->load_obj_state(env->dpp, optional_yield{yield}), 0);
      EXPECT_EQ(next->exists(), 1);
    }

    std::string attr_val;
    std::string oid = instance + "#0#" + std::to_string(ofs);
    std::string location = CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid;
    EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
    EXPECT_EQ(attr_val, "0");

    oid = version + "#0#" + std::to_string(ofs);
    location = CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid;
    EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
    EXPECT_EQ(attr_val, "0");

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);
 
  io.run();
}

TEST_F(D4NFilterFixture, GetVersionedObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  const std::string testName = "GetVersionedObjectWrite";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance;
    put_version_enabled_object(testName, instance, yield);

    {
      bufferlist bl;
      Read_CB cb(&bl);
      std::unique_ptr<rgw::sal::Object::ReadOp> read_op_enabled(objEnabled->get_read_op());
      EXPECT_EQ(read_op_enabled->prepare(optional_yield{yield}, env->dpp), 0);
      EXPECT_EQ(read_op_enabled->iterate(env->dpp, 0, ofs, &cb, optional_yield{yield}), 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + TEST_OBJ + testName, "0", "-1");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));

      response< int, int, int, int, 
		std::vector<std::string>, 
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], instance);
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);

      std::error_code err;
      std::string testData; 
      std::ifstream testFile; 

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + instance, err), true);  
      std::string oid = instance + "#0#" + std::to_string(ofs);
      std::string location = CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid;
      EXPECT_EQ(fs::exists(location, err), true);     

      testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid);
      ASSERT_EQ(testFile.is_open(), true);
      getline(testFile, testData);
      EXPECT_EQ(testData, "test data");
 
      // Ensure data returned from GET op matches cached data
      EXPECT_EQ(testData, cb.save_bl->c_str());

      testFile.close();
    }

    // Remove object entry so the sorted set order will not interfere with the checks for objSuspended
    {
      boost::system::error_code ec;
      request req;
      req.push("DEL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName);
      response<int> resp;
      conn->async_exec(req, resp, yield[ec]);
      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    }

    put_version_suspended_object(testName, yield);

    {
      bufferlist bl;
      Read_CB cb(&bl);
      std::unique_ptr<rgw::sal::Object::ReadOp> read_op_suspended(objSuspended->get_read_op());
      EXPECT_EQ(read_op_suspended->prepare(optional_yield{yield}, env->dpp), 0);
      EXPECT_EQ(read_op_suspended->iterate(env->dpp, 0, ofs, &cb, optional_yield{yield}), 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_"  + TEST_OBJ + testName + "_0_0");
      req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + TEST_OBJ + testName, "0", "-1");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
      req.push("HGETALL", TEST_BUCKET + testName + "__:null_"  + TEST_OBJ + testName + "_0_0");
      req.push("HGET", TEST_BUCKET + testName + "__:null_" + TEST_OBJ + testName + "_0_0", "version");

      response< int, int, int, int, 
		std::vector<std::string>, 
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::map<std::string, std::string>,
		std::string > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], "null");
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);

      std::string version = std::get<8>(resp).value();

      std::error_code err;
      std::string testData; 
      std::ifstream testFile; 

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + instance, err), true);  

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + version, err), true);  
      std::string oid = version + "#0#" + std::to_string(ofs);
      std::string location = CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid;
      EXPECT_EQ(fs::exists(location, err), true);     
      
      testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + TEST_OBJ + testName + "/" + oid);
      ASSERT_EQ(testFile.is_open(), true);
      getline(testFile, testData);
      EXPECT_EQ(testData, "test data");
 
      // Ensure data returned from GET op matches cached data
      EXPECT_EQ(testData, cb.save_bl->c_str());

      testFile.close();
    }

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyNoneVersionedObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  const std::string testName = "CopyNoneVersionedObjectWrite";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance; 
    put_version_enabled_object(testName, instance, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    {
      bl.append("test_value", 10);
      rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
      objEnabled->set_obj_attrs(env->dpp, &attrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);
    }
    bl.append("copy_value", 10);
    rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
    
    std::string tag;
    
    {
      std::string destNameEnabled = "dest_object_enabled";
      std::unique_ptr<rgw::sal::Object> destObjEnabled = testBucket->get_object(rgw_obj_key(destNameEnabled));
      EXPECT_NE(destObjEnabled.get(), nullptr);
      destObjEnabled->gen_rand_obj_instance_name();
      instance = destObjEnabled->get_instance();

      int ret = objEnabled->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjEnabled.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_NONE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameEnabled);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameEnabled + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_" + std::to_string(ofs));
      req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destNameEnabled, "0", "-1");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_" + std::to_string(ofs));

      response< int, int, int, int,
                std::vector<std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], instance);
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);
     
      std::error_code err;
      std::string testData; 
      std::ifstream testFile; 

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + instance, err), true);  
      std::string oid = instance + "#0#" + std::to_string(ofs);
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + oid, err), true);  

      // Ensure attr is not modified
      rgw_obj copyObj = destObjEnabled->get_obj();
      ASSERT_EQ(destObjEnabled->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
      rgw::sal::Attrs copyAttrs = destObjEnabled->get_attrs();
      buffer::list val = copyAttrs["user.rgw.test_attr"];
      EXPECT_EQ(val.to_str(), "test_value");  

      testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + oid);
      ASSERT_EQ(testFile.is_open(), true);
      getline(testFile, testData);
      EXPECT_EQ(testData, "test data");

      testFile.close();
    }

    put_version_suspended_object(testName, yield);
    bl.append("test_value", 10);
    rgw::sal::Attrs testAttrs{{"user.rgw.test_attr", std::move(bl)}};
    objSuspended->set_obj_attrs(env->dpp, &testAttrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);

    {
      std::string destNameSuspended = "dest_object_suspended";
      std::unique_ptr<rgw::sal::Object> destObjSuspended = testBucket->get_object(rgw_obj_key(destNameSuspended));
      EXPECT_NE(destObjSuspended.get(), nullptr);

      int ret = objSuspended->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjSuspended.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_NONE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_" + std::to_string(ofs));
      req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destNameSuspended, "0", "-1");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_" + std::to_string(ofs));
      req.push("HGET", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0", "version");

      response< int, int, int, int,
                std::vector<std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::string > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], "null");
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);

      std::string version = std::get<8>(resp).value();
      std::error_code err;
      std::string testData; 
      std::ifstream testFile; 

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + version, err), true);  
      std::string oid = version + "#0#" + std::to_string(ofs);
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + oid, err), true);  

      // Ensure attr is not modified
      rgw_obj copyObj = destObjSuspended->get_obj();
      ASSERT_EQ(destObjSuspended->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
      rgw::sal::Attrs copyAttrs = destObjSuspended->get_attrs();
      buffer::list val = copyAttrs["user.rgw.test_attr"];
      EXPECT_EQ(val.to_str(), "test_value");  

      testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + oid);
      ASSERT_EQ(testFile.is_open(), true);
      getline(testFile, testData);
      EXPECT_EQ(testData, "test data");

      testFile.close();
    }

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyMergeVersionedObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  const std::string testName = "CopyMergeVersionedObjectWrite";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance; 
    put_version_enabled_object(testName, instance, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    {
      bl.append("test_value", 10);
      rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
      objEnabled->set_obj_attrs(env->dpp, &attrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);
    }
    bl.append("copy_value", 10);
    rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
    
    std::string tag;
    
    {
      std::string destNameEnabled = "dest_object_enabled";
      std::unique_ptr<rgw::sal::Object> destObjEnabled = testBucket->get_object(rgw_obj_key(destNameEnabled));
      EXPECT_NE(destObjEnabled.get(), nullptr);
      destObjEnabled->gen_rand_obj_instance_name();
      instance = destObjEnabled->get_instance();

      int ret = objEnabled->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjEnabled.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_MERGE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameEnabled);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameEnabled + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_" + std::to_string(ofs));
      req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destNameEnabled, "0", "-1");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_" + std::to_string(ofs));

      response< int, int, int, int,
                std::vector<std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], instance);
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);
     
      std::error_code err;
      std::string testData; 
      std::ifstream testFile; 

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + instance, err), true);  
      std::string oid = instance + "#0#" + std::to_string(ofs);
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + oid, err), true);  

      // Ensure attr is merged
      rgw_obj copyObj = destObjEnabled->get_obj();
      ASSERT_EQ(destObjEnabled->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
      rgw::sal::Attrs copyAttrs = destObjEnabled->get_attrs();
      buffer::list val = copyAttrs["user.rgw.test_attr"];
      EXPECT_EQ(val.to_str(), "copy_value");  

      testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + oid);
      ASSERT_EQ(testFile.is_open(), true);
      getline(testFile, testData);
      EXPECT_EQ(testData, "test data");

      testFile.close();
    }

    put_version_suspended_object(testName, yield);
    bl.append("test_value", 10);
    rgw::sal::Attrs testAttrs{{"user.rgw.test_attr", std::move(bl)}};
    objSuspended->set_obj_attrs(env->dpp, &testAttrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);

    {
      std::string destNameSuspended = "dest_object_suspended";
      std::unique_ptr<rgw::sal::Object> destObjSuspended = testBucket->get_object(rgw_obj_key(destNameSuspended));
      EXPECT_NE(destObjSuspended.get(), nullptr);

      int ret = objSuspended->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjSuspended.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_MERGE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_" + std::to_string(ofs));
      req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destNameSuspended, "0", "-1");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_" + std::to_string(ofs));
      req.push("HGET", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0", "version");

      response< int, int, int, int,
                std::vector<std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::string > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], "null");
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);

      std::string version = std::get<8>(resp).value();
      std::error_code err;
      std::string testData; 
      std::ifstream testFile; 

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + version, err), true);  
      std::string oid = version + "#0#" + std::to_string(ofs);
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + oid, err), true);  

      // Ensure attr is merged  
      rgw_obj copyObj = destObjSuspended->get_obj();
      ASSERT_EQ(destObjSuspended->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
      rgw::sal::Attrs copyAttrs = destObjSuspended->get_attrs();
      buffer::list val = copyAttrs["user.rgw.test_attr"];
      EXPECT_EQ(val.to_str(), "copy_value");  

      testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + oid);
      ASSERT_EQ(testFile.is_open(), true);
      getline(testFile, testData);
      EXPECT_EQ(testData, "test data");

      testFile.close();
    }

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, CopyReplaceVersionedObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  const std::string testName = "CopyReplaceVersionedObjectWrite";
 
  net::spawn(io, [this, &testName] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    std::string instance; 
    put_version_enabled_object(testName, instance, yield);

    RGWEnv rgw_env;
    req_info info(env->cct.get(), &rgw_env);
    rgw_zone_id zone;
    rgw_placement_rule placement;
    ceph::real_time mtime;

    buffer::list bl;
    {
      bl.append("test_value", 10);
      rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
      objEnabled->set_obj_attrs(env->dpp, &attrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);
    }
    bl.append("copy_value", 10);
    rgw::sal::Attrs attrs{{"user.rgw.test_attr", std::move(bl)}};
    
    std::string tag;
    
    {
      std::string destNameEnabled = "dest_object_enabled";
      std::unique_ptr<rgw::sal::Object> destObjEnabled = testBucket->get_object(rgw_obj_key(destNameEnabled));
      EXPECT_NE(destObjEnabled.get(), nullptr);
      destObjEnabled->gen_rand_obj_instance_name();
      instance = destObjEnabled->get_instance();

      int ret = objEnabled->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjEnabled.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_REPLACE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameEnabled);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameEnabled + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_" + std::to_string(ofs));
      req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destNameEnabled, "0", "-1");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:" + instance + "_" + destNameEnabled + "_0_" + std::to_string(ofs));

      response< int, int, int, int,
                std::vector<std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], instance);
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);
     
      std::error_code err;
      std::string testData; 
      std::ifstream testFile; 

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + instance, err), true);  
      std::string oid = instance + "#0#" + std::to_string(ofs);
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + oid, err), true);  

      // Ensure attr is replaced
      rgw_obj copyObj = destObjEnabled->get_obj();
      ASSERT_EQ(destObjEnabled->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
      rgw::sal::Attrs copyAttrs = destObjEnabled->get_attrs();
      buffer::list val = copyAttrs["user.rgw.test_attr"];
      EXPECT_EQ(val.to_str(), "copy_value");  

      testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameEnabled + "/" + oid);
      ASSERT_EQ(testFile.is_open(), true);
      getline(testFile, testData);
      EXPECT_EQ(testData, "test data");

      testFile.close();
    }

    put_version_suspended_object(testName, yield);
    bl.append("test_value", 10);
    rgw::sal::Attrs testAttrs{{"user.rgw.test_attr", std::move(bl)}};
    objSuspended->set_obj_attrs(env->dpp, &testAttrs, nullptr, optional_yield{yield}, rgw::sal::FLAG_LOG_OP);

    {
      std::string destNameSuspended = "dest_object_suspended";
      std::unique_ptr<rgw::sal::Object> destObjSuspended = testBucket->get_object(rgw_obj_key(destNameSuspended));
      EXPECT_NE(destObjSuspended.get(), nullptr);

      int ret = objSuspended->copy_object(acl_owner,
	       std::get<rgw_user>(owner),
	       &info,
	       zone,
	       destObjSuspended.get(),
	       testBucket.get(),
	       testBucket.get(),
	       placement,
	       &mtime,
	       &mtime,
	       nullptr,
	       nullptr,
	       false,
	       nullptr,
	       nullptr,
	       rgw::sal::ATTRSMOD_REPLACE,
	       false,
	       attrs, 
	       RGWObjCategory::Main,
	       0,
	       boost::none,
	       nullptr,
	       &tag, 
	       &tag,
	       nullptr,
	       nullptr,
	       env->dpp,
	       optional_yield({yield}));
      EXPECT_EQ(ret, 0);

      boost::system::error_code ec;
      request req;
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended);
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("EXISTS", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_" + std::to_string(ofs));
      req.push("ZREVRANGE", TEST_BUCKET + testName + "_" + destNameSuspended, "0", "-1");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0");
      req.push("HGETALL", TEST_BUCKET + testName + "_" + destNameSuspended + "_0_" + std::to_string(ofs));
      req.push("HGET", TEST_BUCKET + testName + "__:null_" + destNameSuspended + "_0_0", "version");

      response< int, int, int, int,
                std::vector<std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::map<std::string, std::string>,
                std::string > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ((int)std::get<0>(resp).value(), 1);
      EXPECT_EQ((int)std::get<1>(resp).value(), 1);
      EXPECT_EQ((int)std::get<2>(resp).value(), 1);
      EXPECT_EQ((int)std::get<3>(resp).value(), 1);
      EXPECT_EQ(std::get<4>(resp).value()[0], "null");
      EXPECT_EQ(std::get<5>(resp).value().size(), 14);
      EXPECT_EQ(std::get<6>(resp).value().size(), 14);
      EXPECT_EQ(std::get<7>(resp).value().size(), 14);

      std::string version = std::get<8>(resp).value();
      std::error_code err;
      std::string testData; 
      std::ifstream testFile; 

      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + version, err), true);  
      std::string oid = version + "#0#" + std::to_string(ofs);
      EXPECT_EQ(fs::exists(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + oid, err), true);  

      // Ensure attr is replaced
      rgw_obj copyObj = destObjSuspended->get_obj();
      ASSERT_EQ(destObjSuspended->get_obj_attrs(optional_yield{yield}, env->dpp, &copyObj), 0);
      rgw::sal::Attrs copyAttrs = destObjSuspended->get_attrs();
      buffer::list val = copyAttrs["user.rgw.test_attr"];
      EXPECT_EQ(val.to_str(), "copy_value");  

      testFile.open(CACHE_DIR + "/" + TEST_BUCKET + testName + "/" + destNameSuspended + "/" + oid);
      ASSERT_EQ(testFile.is_open(), true);
      getline(testFile, testData);
      EXPECT_EQ(testData, "test data");

      testFile.close();
    }

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, DeleteVersionedObjectWrite)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "DeleteVersionedObjectRead";
  const std::string bucketName = "/tmp/d4n_filter_tests/dbstore-default_ns.1";
  std::string version, instance;
 
  net::spawn(io, [this, &testName, &bucketName, &version, &instance] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    testBucket->get_info().bucket.bucket_id = bucketName;
    put_version_enabled_object(testName, instance, yield);
    put_version_suspended_object(testName, yield);

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName);
    req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
    req.push("EXISTS", bucketName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
    req.push("HGET", bucketName + "__:null_" + TEST_OBJ + testName + "_0_0", "version");

    response< int, int, int, int, 
	      int, int, std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    EXPECT_EQ((int)std::get<1>(resp).value(), 1);
    EXPECT_EQ((int)std::get<2>(resp).value(), 1);
    EXPECT_EQ((int)std::get<3>(resp).value(), 1);
    EXPECT_EQ((int)std::get<4>(resp).value(), 1);
    EXPECT_EQ((int)std::get<5>(resp).value(), 1);
    
    version = std::get<6>(resp).value();

    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + instance, err), true);  
    std::string oid = instance + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid, err), true);  

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + version, err), true);  
    oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid, err), true);  

    // For version enabled object
    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op_enabled = objEnabled->get_delete_op();
    EXPECT_EQ(del_op_enabled->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);

    // For version suspended object
    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op_suspended = objSuspended->get_delete_op();
    EXPECT_EQ(del_op_suspended->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);

    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &testName, &bucketName, &version, &instance] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    boost::system::error_code ec;
    request req;
    req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName);
    req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", bucketName + "__:" + instance + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));
    req.push("EXISTS", bucketName + "__:null_" + TEST_OBJ + testName + "_0_0");
    req.push("EXISTS", bucketName + "_" + TEST_OBJ + testName + "_0_" + std::to_string(ofs));

    response< int, int, int, int, 
	      int, int, std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    //EXPECT_EQ((int)std::get<0>(resp).value(), 0); // TODO: Object entry is not deleted
    EXPECT_EQ((int)std::get<1>(resp).value(), 0);
    EXPECT_EQ((int)std::get<2>(resp).value(), 0);
    EXPECT_EQ((int)std::get<3>(resp).value(), 0);
    EXPECT_EQ((int)std::get<4>(resp).value(), 0);
    EXPECT_EQ((int)std::get<5>(resp).value(), 0);

    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + instance, err), false);  
    std::string oid = instance + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid, err), false);  

    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + version, err), false);  
    oid = version + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid, err), false);  

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

// Additional workflow tests
TEST_F(D4NFilterFixture, SimpleDeleteBeforeCleaning)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "SimpleDeleteBeforeCleaning";
  const std::string bucketName = "/tmp/d4n_filter_tests/dbstore-default_ns.1";
  std::vector<std::string> instances;
  std::string deleteMarker, location;
 
  net::spawn(io, [this, &testName, &bucketName, &instances, &deleteMarker, &location] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    testBucket->get_info().bucket.bucket_id = bucketName;
    
    for (int i = 0; i <= 1; ++i) {
      std::string instance;
      put_version_enabled_object(testName, instance, yield); // Upload two versions 
      instances.push_back(instance);
    }

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op_enabled = objEnabled->get_delete_op();
    objEnabled->set_instance(""); // Simple delete
    EXPECT_EQ(del_op_enabled->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);

    boost::system::error_code ec;
    request req;
    req.push("HGET", bucketName + "_" + TEST_OBJ + testName + "_0_0", "deleteMarker");
    req.push("HGET", bucketName + "_" + TEST_OBJ + testName + "_0_0", "version");

    response< int, std::string > resp;

    conn->async_exec(req, resp, yield[ec]);

    ASSERT_EQ((bool)ec, false);
    EXPECT_EQ((int)std::get<0>(resp).value(), 1);
    deleteMarker = std::get<1>(resp).value();

    // Simple delete's head object in cache
    std::error_code err;
    location = CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + deleteMarker;
    EXPECT_EQ(fs::exists(location, err), true);  

    std::string attr_val;
    EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
    EXPECT_EQ(attr_val, "1");

    /* TODO: The following code allows the DB::Object::Delete::delete_obj op to succeed in the cleaning method by removing the instance value.
       However, this causes the wrong head_oid_in_cache key to be generated since it uses the empty version rather than the delete marker. As
       a result, the head block for the delete marker does not get cleaned. If this code is not used, then the DBStore delete_obj method returns
       -ENOENT, which also prevents the head block from being cleaned.
    objEnabled->set_instance("");
    std::string key = url_encode(bucketName, true) + "#" + deleteMarker + "#" + TEST_OBJ + testName;
    std::string etag = "test_etag";
    auto creationTime = ceph::real_clock::to_time_t(objEnabled->get_mtime());
    d4nFilter->get_policy_driver()->get_cache_policy()->erase_dirty_object(env->dpp, key, optional_yield{yield}); 
    d4nFilter->get_policy_driver()->get_cache_policy()->update_dirty_object(env->dpp, key, "", true, objEnabled->get_accounted_size(), creationTime, 
                                                         std::get<rgw_user>(objEnabled->get_bucket()->get_owner()), etag, 
                                                         objEnabled->get_bucket()->get_name(), objEnabled->get_bucket()->get_bucket_id(), 
                                                         objEnabled->get_key(), rgw::d4n::REFCOUNT_NOOP, optional_yield{yield});*/
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &testName, &bucketName, &deleteMarker, &location] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(objEnabled->get_read_op());
    EXPECT_EQ(read_op->prepare(optional_yield{yield}, env->dpp), -2); // Simple read; should return -ENOENT
    EXPECT_EQ(read_op->iterate(env->dpp, 0, ofs, nullptr, optional_yield{yield}), -2);

    /* TODO: 
    std::string attr_val;
    EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
    EXPECT_EQ(attr_val, "0");*/ 

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, VersionedDeleteBeforeCleaning)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "VersionedDeleteBeforeCleaning";
  const std::string bucketName = "/tmp/d4n_filter_tests/dbstore-default_ns.1";
  std::vector<std::string> instances;
 
  net::spawn(io, [this, &testName, &bucketName, &instances] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    testBucket->get_info().bucket.bucket_id = bucketName;
    
    for (int i = 0; i <= 1; ++i) {
      std::string instance;
      put_version_enabled_object(testName, instance, yield); // Upload two versions 
      instances.push_back(instance);
    }

    {
      boost::system::error_code ec;
      request req;
      req.push("ZREVRANGE", bucketName + "_" + TEST_OBJ + testName, "0", "-1");

      response< std::vector<std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value().size(), 2);
      EXPECT_EQ(std::get<0>(resp).value()[0], instances[1]);
      EXPECT_EQ(std::get<0>(resp).value()[1], instances[0]);
    }

    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + instances[1], err), true);  
    std::string oid = instances[1] + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid, err), true);  

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op_enabled = objEnabled->get_delete_op();
    objEnabled->set_instance(instances[1]); // Latest version
    EXPECT_EQ(del_op_enabled->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);

    {
      boost::system::error_code ec;
      request req;
      req.push("ZREVRANGE", bucketName + "_" + TEST_OBJ + testName, "0", "-1");

      response< std::vector<std::string> > resp;

      conn->async_exec(req, resp, yield[ec]);

      ASSERT_EQ((bool)ec, false);
      EXPECT_EQ(std::get<0>(resp).value().size(), 1);
      EXPECT_EQ(std::get<0>(resp).value()[0], instances[0]);
    }

    objEnabled->set_instance(""); // Simple get

    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(objEnabled->get_read_op());
    EXPECT_EQ(read_op->prepare(optional_yield{yield}, env->dpp), 0);
    EXPECT_EQ(read_op->iterate(env->dpp, 0, ofs, nullptr, optional_yield{yield}), 0);
    EXPECT_EQ(objEnabled->get_instance(), instances[0]); // Next latest version

    objEnabled->set_instance(instances[0]);
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &testName, &bucketName, &instances] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + instances[1], err), false);  
    std::string oid = instances[1] + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid, err), false);  

    objEnabled->set_instance(instances[1]);

    // Make sure deleted instance isn't written to backend
    {
      auto next = dynamic_cast<rgw::sal::FilterObject*>(objEnabled.get())->get_next();
      EXPECT_EQ(next->load_obj_state(env->dpp, optional_yield{yield}), -2);
    }

    objEnabled->set_instance(instances[0]);

    // First instance should still be available in backend
    {
      auto next = dynamic_cast<rgw::sal::FilterObject*>(objEnabled.get())->get_next();
      EXPECT_EQ(next->load_obj_state(env->dpp, optional_yield{yield}), 0);
      EXPECT_EQ(next->exists(), 1);

      bufferlist bl;
      Read_CB cb(&bl);
      next->set_instance("");
      std::unique_ptr<rgw::sal::Object::ReadOp> read_op(next->get_read_op());
      EXPECT_EQ(read_op->prepare(optional_yield{yield}, env->dpp), 0);
      EXPECT_EQ(read_op->iterate(env->dpp, 0, ofs, &cb, optional_yield{yield}), 0);
      EXPECT_EQ(next->get_instance(), instances[0]); // Next latest version
    }

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, SimpleDeleteAfterCleaning)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "SimpleDeleteAfterCleaning";
  const std::string bucketName = "/tmp/d4n_filter_tests/dbstore-default_ns.1";
  std::vector<std::string> instances;
 
  net::spawn(io, [this, &testName, &bucketName, &instances] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    testBucket->get_info().bucket.bucket_id = bucketName;
    
    for (int i = 0; i <= 1; ++i) {
      std::string instance;
      put_version_enabled_object(testName, instance, yield); // Upload two versions 
      instances.push_back(instance);
    }

    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &testName, &bucketName] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op_enabled = objEnabled->get_delete_op();
    objEnabled->set_instance(""); // Simple delete
    EXPECT_EQ(del_op_enabled->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);
    
    // Retrieve delete marker
    std::string deleteMarker = objEnabled->get_instance();

    // Ensure delete marker head block is written to cache
    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + deleteMarker, err), true);  

    // Make sure delete marker is written to backend
    objEnabled->set_instance(deleteMarker);
    auto next = dynamic_cast<rgw::sal::FilterObject*>(objEnabled.get())->get_next();
    EXPECT_EQ(next->load_obj_state(env->dpp, optional_yield{yield}), 0);
    EXPECT_EQ(next->exists(), 1);

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, VersionedDeleteAfterCleaning)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "VersionedDeleteAfterCleaning";
  const std::string bucketName = "/tmp/d4n_filter_tests/dbstore-default_ns.1";
  std::vector<std::string> instances;
 
  net::spawn(io, [this, &testName, &bucketName, &instances] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    testBucket->get_info().bucket.bucket_id = bucketName;
    
    for (int i = 0; i <= 1; ++i) {
      std::string instance;
      put_version_enabled_object(testName, instance, yield); // Upload two versions 
      instances.push_back(instance);
    }

    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &testName, &bucketName, &instances] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    std::error_code err;
    std::string location = CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + instances[1];
    EXPECT_EQ(fs::exists(location, err), true);  
    std::string oid = "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(location + oid, err), true);  

    {
      std::string attr_val;
      EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
      EXPECT_EQ(attr_val, "0");
    }

    {
      std::string attr_val;
      EXPECT_EQ(d4nFilter->get_cache_driver()->get_attr(env->dpp, location + oid, RGW_CACHE_ATTR_DIRTY, attr_val, optional_yield({yield})), 0);
      EXPECT_EQ(attr_val, "0");
    }

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op_enabled = objEnabled->get_delete_op();
    objEnabled->set_instance(instances[1]); // Latest version
    EXPECT_EQ(del_op_enabled->delete_obj(env->dpp, optional_yield{yield}, rgw::sal::FLAG_LOG_OP), 0);
    
    // Make sure deleted instance is deleted from backend
    {
      auto next = dynamic_cast<rgw::sal::FilterObject*>(objEnabled.get())->get_next();
      EXPECT_EQ(next->load_obj_state(env->dpp, optional_yield{yield}), -2);
    }

    objEnabled->set_instance(instances[0]);

    // First instance should still be available in backend
    {
      auto next = dynamic_cast<rgw::sal::FilterObject*>(objEnabled.get())->get_next();
      EXPECT_EQ(next->load_obj_state(env->dpp, optional_yield{yield}), 0);
      EXPECT_EQ(next->exists(), 1);

      bufferlist bl;
      Read_CB cb(&bl);
      next->set_instance("");
      std::unique_ptr<rgw::sal::Object::ReadOp> read_op(next->get_read_op());
      EXPECT_EQ(read_op->prepare(optional_yield{yield}, env->dpp), 0);
      EXPECT_EQ(read_op->iterate(env->dpp, 0, ofs, &cb, optional_yield{yield}), 0);
      EXPECT_EQ(next->get_instance(), instances[0]); // Next latest version
    }

    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2));

  net::spawn(io, [this, &testName, &bucketName, &instances] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    /* TODO: Cleaning method removes head object, so delete_obj calls after a cleaning cycle will not find the head object and will immediately call the backend's delete_obj,
       resulting in the cache block not getting deleted properly
    std::error_code err;
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + instances[1], err), false);  
    std::string oid = instances[1] + "#0#" + std::to_string(ofs);
    EXPECT_EQ(fs::exists(CACHE_DIR + "/" + url_encode(bucketName, true) + "/" + TEST_OBJ + testName + "/" + oid, err), false);  
    */

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

TEST_F(D4NFilterFixture, ListObjectVersions)
{
  env->cct->_conf->d4n_writecache_enabled = true;
  env->cct->_conf->rgw_d4n_cache_cleaning_interval = 0;
  const std::string testName = "ListObjectVersions";
  const std::string bucketName = "/tmp/d4n_filter_tests/dbstore-default_ns.1";
  std::string instance;
 
  net::spawn(io, [this, &testName, &bucketName, &instance] (net::yield_context yield) {
    init_driver(yield);
    create_bucket(testName, yield);
    testBucket->get_info().bucket.bucket_id = bucketName;
    put_object(testName, yield);
    put_version_enabled_object(testName, instance, yield);
    put_version_suspended_object(testName, yield);

    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(null_yield);
  }, rethrow);

  io.run_for(std::chrono::seconds(2)); // Allow cleaning cycle to complete

  net::spawn(io, [this, &instance] (net::yield_context yield) {
    dynamic_cast<rgw::d4n::LFUDAPolicy*>(d4nFilter->get_policy_driver()->get_cache_policy())->save_y(optional_yield{yield});

    // Ensure versions are correctly ordered in backend
    size_t max = env->dpp->get_cct()->_conf->rgw_list_buckets_max_chunk;
    rgw::sal::Bucket::ListParams params;
    params.list_versions = true;
    rgw::sal::Bucket::ListResults results;

    EXPECT_EQ(testBucket->list(env->dpp, params, max, results, optional_yield{yield}), 0);
    EXPECT_EQ(results.objs[0].key.instance, "null");
    EXPECT_EQ(results.objs[1].key.instance, instance);

    conn->cancel();
    testBucket->remove(env->dpp, true, optional_yield{yield});
    DriverDestructor driver_destructor(static_cast<rgw::sal::D4NFilterDriver*>(driver));
  }, rethrow);

  io.run();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
