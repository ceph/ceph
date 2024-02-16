#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include <iostream>
#include <string>
#include "rgw_process_env.h"
#include <cpp_redis/cpp_redis>
#include "driver/dbstore/common/dbstore.h"
#include "rgw_sal_store.h"
#include "driver/d4n/rgw_sal_d4n.h"

#include "rgw_sal.h"
#include "rgw_auth.h"
#include "rgw_auth_registry.h"
#include "driver/rados/rgw_zone.h"
#include "rgw_sal_config.h"

#include <boost/asio/io_context.hpp>

#define dout_subsys ceph_subsys_rgw

#define METADATA_LENGTH 22

using namespace std;

string portStr;
string hostStr;
string redisHost = "";

vector<const char*> args;
class Environment* env;
const DoutPrefixProvider* dpp;
const req_context rctx{dpp, null_yield, nullptr};

class StoreObject : public rgw::sal::StoreObject {
  friend class D4NFilterFixture;
  FRIEND_TEST(D4NFilterFixture, StoreGetMetadata);
};

class Environment : public ::testing::Environment {
  boost::asio::io_context ioc;
  public:
    Environment() {}
    
    virtual ~Environment() {}

    void SetUp() override {
      /* Ensure redis instance is running */
      try {
        env_client.connect(hostStr, stoi(portStr), nullptr, 0, 5, 1000);
      } catch (std::exception &e) {
        std::cerr << "[          ] ERROR: Redis instance not running." << std::endl;
      }

      ASSERT_EQ((bool)env_client.is_connected(), (bool)1);

      /* Proceed with environment setup */
      cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT, 
		        CODE_ENVIRONMENT_UTILITY, 
			CINIT_FLAG_NO_MON_CONFIG);

      dpp = new DoutPrefix(cct->get(), dout_subsys, "d4n test: ");
      DriverManager::Config cfg;

      cfg.store_name = "dbstore";
      cfg.filter_name = "d4n";
      auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
      std::unique_ptr<rgw::sal::ConfigStore> cfgstore
        = DriverManager::create_config_store(dpp, config_store_type);
      ASSERT_TRUE(cfgstore);
      rgw::SiteConfig site;
      auto r = site.load(dpp, null_yield, cfgstore.get());
      ASSERT_GT(r, 0);

      driver = DriverManager::get_storage(dpp, dpp->get_cct(),
              cfg,
              ioc,
              site,
              false,
              false,
              false,
              false,
              false,
              false, null_yield,
	      false); 
    
      ASSERT_NE(driver, nullptr);
    }

    void TearDown() override {
      if (env_client.is_connected()) {
        delete driver;
        delete dpp;
        
	env_client.disconnect();
      }
    }

    boost::intrusive_ptr<CephContext> cct;
    rgw::sal::Driver* driver;
    cpp_redis::client env_client;
};

class D4NFilterFixture : public ::testing::Test {
  protected:
    rgw::sal::Driver* driver;
    unique_ptr<rgw::sal::User> testUser = nullptr;
    unique_ptr<rgw::sal::Bucket> testBucket = nullptr;
    unique_ptr<rgw::sal::Writer> testWriter = nullptr;

  public:
    D4NFilterFixture() {}
    
    void SetUp() {
      driver = env->driver;
    }

    void TearDown() {}
    
    int createUser() {
      rgw_user u("test_tenant", "test_user", "ns");

      testUser = driver->get_user(u);
      testUser->get_info().user_id = u;

      int ret = testUser->store_user(dpp, null_yield, false);

      return ret;
    }

    int createBucket() {
      RGWBucketInfo info;
      info.bucket.name = "test_bucket";

      testBucket = driver->get_bucket(info);

      rgw::sal::Bucket::CreateParams params;
      params.zonegroup_id = "test_id";
      params.placement_rule.storage_class = "test_sc";
      params.swift_ver_location = "test_location";

      return testBucket->create(dpp, params, null_yield);
    }

    int putObject(string name) {
      string object_name = "test_object_" + name;
      unique_ptr<rgw::sal::Object> obj = testBucket->get_object(rgw_obj_key(object_name));
      rgw_user owner;
      rgw_placement_rule ptail_placement_rule;
      uint64_t olh_epoch = 123;
      string unique_tag;

      obj->get_obj_attrs(null_yield, dpp);

      testWriter = driver->get_atomic_writer(dpp, 
		  null_yield,
		  obj.get(),
		  owner,
		  &ptail_placement_rule,
		  olh_epoch,
		  unique_tag);
  
      size_t accounted_size = 4;
      string etag("test_etag");
      ceph::real_time mtime; 
      ceph::real_time set_mtime;

      buffer::list bl;
      string tmp = "test_attrs_value_" + name;
      bl.append("test_attrs_value_" + name);
      map<string, bufferlist> attrs{{"test_attrs_key_" + name, bl}};

      ceph::real_time delete_at;
      char if_match;
      char if_nomatch;
      string user_data;
      rgw_zone_set zones_trace;
      bool canceled;
      
      int ret = testWriter->complete(accounted_size, etag,
                       &mtime, set_mtime,
                       attrs,
                       delete_at,
                       &if_match, &if_nomatch,
                       &user_data,
                       &zones_trace, &canceled,
                       rctx, rgw::sal::FLAG_LOG_OP);

      return ret;
    }

    void clientSetUp(cpp_redis::client* client) {
      client->connect(hostStr, stoi(portStr), nullptr, 0, 5, 1000);
      ASSERT_EQ((bool)client->is_connected(), (bool)1);

      client->flushdb([](cpp_redis::reply& reply) {});
      client->sync_commit();
    }

    void clientReset(cpp_redis::client* client) {
      client->flushdb([](cpp_redis::reply& reply) {});
      client->sync_commit();
    }
};

/* General operation-related tests */
TEST_F(D4NFilterFixture, CreateUser) {
  EXPECT_EQ(createUser(), 0);
  EXPECT_NE(testUser, nullptr);
}

TEST_F(D4NFilterFixture, CreateBucket) {
  ASSERT_EQ(createUser(), 0);
  ASSERT_NE(testUser, nullptr);
   
  EXPECT_EQ(createBucket(), 0);
  EXPECT_NE(testBucket, nullptr);
}

TEST_F(D4NFilterFixture, PutObject) {
  cpp_redis::client client;
  vector<string> fields;
  fields.push_back("test_attrs_key_0");
  clientSetUp(&client); 

  ASSERT_EQ(createUser(), 0);
  ASSERT_NE(testUser, nullptr);
   
  ASSERT_EQ(createBucket(), 0);
  ASSERT_NE(testBucket, nullptr);
  
  EXPECT_EQ(putObject("PutObject"), 0);
  EXPECT_NE(testWriter, nullptr);

  client.hgetall("rgw-object:test_object_PutObject:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_PutObject:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_PutObject");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, GetObject) {
  cpp_redis::client client;
  vector<string> fields;
  fields.push_back("test_attrs_key_GetObject");
  clientSetUp(&client); 

  ASSERT_EQ(createUser(), 0);
  ASSERT_NE(testUser, nullptr);
   
  ASSERT_EQ(createBucket(), 0);
  ASSERT_NE(testBucket, nullptr);
  
  ASSERT_EQ(putObject("GetObject"), 0);
  ASSERT_NE(testWriter, nullptr);

  unique_ptr<rgw::sal::Object> testObject_GetObject = testBucket->get_object(rgw_obj_key("test_object_GetObject"));

  EXPECT_NE(testObject_GetObject, nullptr);
  
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_GetObject.get())->get_next();

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  
  unique_ptr<rgw::sal::Object::ReadOp> testROp = testObject_GetObject->get_read_op();

  EXPECT_NE(testROp, nullptr);
  EXPECT_EQ(testROp->prepare(null_yield, dpp), 0);

  client.hgetall("rgw-object:test_object_GetObject:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_GetObject:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_GetObject");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, CopyObjectNone) {
  cpp_redis::client client;
  vector<string> fields;
  fields.push_back("test_attrs_key_CopyObjectNone");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("CopyObjectNone");
  unique_ptr<rgw::sal::Object> testObject_CopyObjectNone = testBucket->get_object(rgw_obj_key("test_object_CopyObjectNone"));

  ASSERT_NE(testObject_CopyObjectNone, nullptr);

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_CopyObjectNone.get())->get_next();

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Update object */
  RGWEnv rgw_env;
  req_info info(get_pointer(env->cct), &rgw_env);
  rgw_zone_id source_zone;
  rgw_placement_rule dest_placement; 
  ceph::real_time src_mtime;
  ceph::real_time mtime;
  ceph::real_time mod_ptr;
  ceph::real_time unmod_ptr;
  char if_match;
  char if_nomatch;
  rgw::sal::AttrsMod attrs_mod = rgw::sal::ATTRSMOD_NONE;
  rgw::sal::Attrs attrs;
  RGWObjCategory category = RGWObjCategory::Main;
  uint64_t olh_epoch = 0;
  ceph::real_time delete_at;
  string tag;
  string etag;

  EXPECT_EQ(testObject_CopyObjectNone->copy_object(testUser.get(),
			      &info, source_zone, testObject_CopyObjectNone.get(),
			      testBucket.get(), testBucket.get(),
                              dest_placement, &src_mtime, &mtime,
			      &mod_ptr, &unmod_ptr, false,
			      &if_match, &if_nomatch, attrs_mod,
			      false, attrs, category, olh_epoch,
			      delete_at, NULL, &tag, &etag,
			      NULL, NULL, dpp, null_yield), 0);

  client.hgetall("rgw-object:test_object_CopyObjectNone:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();
  
  client.hmget("rgw-object:test_object_CopyObjectNone:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_CopyObjectNone");
    }
  });

  client.sync_commit();
}

TEST_F(D4NFilterFixture, CopyObjectReplace) {
  cpp_redis::client client;
  vector<string> fields;
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("CopyObjectReplace");
  unique_ptr<rgw::sal::Object> testObject_CopyObjectReplace = testBucket->get_object(rgw_obj_key("test_object_CopyObjectReplace"));

  ASSERT_NE(testObject_CopyObjectReplace, nullptr);

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_CopyObjectReplace.get())->get_next();

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Copy to new object */
  unique_ptr<rgw::sal::Writer> testWriterCopy = nullptr;
  unique_ptr<rgw::sal::Object> obj = testBucket->get_object(rgw_obj_key("test_object_copy"));
  rgw_user owner;
  rgw_placement_rule ptail_placement_rule;
  uint64_t olh_epoch_copy = 123;
  string unique_tag;

  obj->get_obj_attrs(null_yield, dpp);

  testWriterCopy = driver->get_atomic_writer(dpp, 
	      null_yield,
	      obj.get(),
	      owner,
	      &ptail_placement_rule,
	      olh_epoch_copy,
	      unique_tag);

  RGWEnv rgw_env;
  size_t accounted_size = 0;
  req_info info(get_pointer(env->cct), &rgw_env);
  rgw_zone_id source_zone;
  rgw_placement_rule dest_placement; 
  ceph::real_time src_mtime;
  ceph::real_time mtime; 
  ceph::real_time set_mtime;
  ceph::real_time mod_ptr;
  ceph::real_time unmod_ptr;
  rgw::sal::AttrsMod attrs_mod = rgw::sal::ATTRSMOD_REPLACE;
  char if_match;
  char if_nomatch;
  RGWObjCategory category = RGWObjCategory::Main;
  uint64_t olh_epoch = 0;
  ceph::real_time delete_at;
  string tag;
  string etag("test_etag_copy");

  /* Attribute to replace */
  buffer::list bl;
  bl.append("test_attrs_copy_value");
  rgw::sal::Attrs attrs{{"test_attrs_key_CopyObjectReplace", bl}};

  string user_data;
  rgw_zone_set zones_trace;
  bool canceled;
  
  ASSERT_EQ(testWriterCopy->complete(accounted_size, etag,
		   &mtime, set_mtime,
		   attrs,
		   delete_at,
		   &if_match, &if_nomatch,
		   &user_data,
		   &zones_trace, &canceled,
		   rctx, rgw::sal::FLAG_LOG_OP), 0);

  unique_ptr<rgw::sal::Object> testObject_copy = testBucket->get_object(rgw_obj_key("test_object_copy"));

  EXPECT_EQ(testObject_CopyObjectReplace->copy_object(testUser.get(),
			      &info, source_zone, testObject_copy.get(),
			      testBucket.get(), testBucket.get(),
                              dest_placement, &src_mtime, &mtime,
			      &mod_ptr, &unmod_ptr, false,
			      &if_match, &if_nomatch, attrs_mod,
			      false, attrs, category, olh_epoch,
			      delete_at, NULL, &tag, &etag,
			      NULL, NULL, dpp, null_yield), 0);

  /* Ensure the original object is still in the cache */
  vector<string> keys;
  keys.push_back("rgw-object:test_object_CopyObjectReplace:cache");

  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      EXPECT_EQ(reply.as_integer(), 1);
    }
  });

  client.sync_commit();

  /* Check copy */
  client.hgetall("rgw-object:test_object_copy:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH); /* With etag */
    }
  });

  client.sync_commit();
  
  fields.push_back("test_attrs_key_CopyObjectReplace");
  
  client.hmget("rgw-object:test_object_copy:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_copy_value");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, CopyObjectMerge) {
  cpp_redis::client client;
  vector<string> fields;
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("CopyObjectMerge");
  unique_ptr<rgw::sal::Object> testObject_CopyObjectMerge = testBucket->get_object(rgw_obj_key("test_object_CopyObjectMerge"));

  ASSERT_NE(testObject_CopyObjectMerge, nullptr);

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_CopyObjectMerge.get())->get_next();

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Copy to new object */
  unique_ptr<rgw::sal::Writer> testWriterCopy = nullptr;
  string object_name = "test_object_copy";
  unique_ptr<rgw::sal::Object> obj = testBucket->get_object(rgw_obj_key(object_name));
  rgw_user owner;
  rgw_placement_rule ptail_placement_rule;
  uint64_t olh_epoch_copy = 123;
  string unique_tag;

  obj->get_obj_attrs(null_yield, dpp);

  testWriterCopy = driver->get_atomic_writer(dpp, 
	      null_yield,
	      obj.get(),
	      owner,
	      &ptail_placement_rule,
	      olh_epoch_copy,
	      unique_tag);

  RGWEnv rgw_env;
  size_t accounted_size = 4;
  req_info info(get_pointer(env->cct), &rgw_env);
  rgw_zone_id source_zone;
  rgw_placement_rule dest_placement; 
  ceph::real_time src_mtime;
  ceph::real_time mtime; 
  ceph::real_time set_mtime;
  ceph::real_time mod_ptr;
  ceph::real_time unmod_ptr;
  rgw::sal::AttrsMod attrs_mod = rgw::sal::ATTRSMOD_MERGE;
  char if_match;
  char if_nomatch;
  RGWObjCategory category = RGWObjCategory::Main;
  uint64_t olh_epoch = 0;
  ceph::real_time delete_at;
  string tag;
  string etag("test_etag_copy");

  buffer::list bl;
  bl.append("bad_value");
  rgw::sal::Attrs attrs{{"test_attrs_key_CopyObjectMerge", bl}}; /* Existing attr */
  bl.clear();
  bl.append("test_attrs_copy_extra_value");
  attrs.insert({"test_attrs_copy_extra_key", bl}); /* New attr */

  string user_data;
  rgw_zone_set zones_trace;
  bool canceled;
  
  ASSERT_EQ(testWriterCopy->complete(accounted_size, etag,
		   &mtime, set_mtime,
		   attrs,
		   delete_at,
		   &if_match, &if_nomatch,
		   &user_data,
		   &zones_trace, &canceled,
		   rctx, rgw::sal::FLAG_LOG_OP), 0);

  unique_ptr<rgw::sal::Object> testObject_copy = testBucket->get_object(rgw_obj_key("test_object_copy"));

  EXPECT_EQ(testObject_CopyObjectMerge->copy_object(testUser.get(),
			      &info, source_zone, testObject_copy.get(),
			      testBucket.get(), testBucket.get(),
                              dest_placement, &src_mtime, &mtime,
			      &mod_ptr, &unmod_ptr, false,
			      &if_match, &if_nomatch, attrs_mod,
			      false, attrs, category, olh_epoch,
			      delete_at, NULL, &tag, &etag,
			      NULL, NULL, dpp, null_yield), 0);

  /* Ensure the original object is still in the cache */
  vector<string> keys;
  keys.push_back("rgw-object:test_object_CopyObjectMerge:cache");

  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      EXPECT_EQ(reply.as_integer(), 1);
    }
  });

  client.sync_commit();

  /* Check copy */
  client.hgetall("rgw-object:test_object_copy:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 6 + METADATA_LENGTH); /* With etag */
    }
  });

  client.sync_commit();
  
  fields.push_back("test_attrs_key_CopyObjectMerge");
  fields.push_back("test_attrs_copy_extra_key");
  
  client.hmget("rgw-object:test_object_copy:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_CopyObjectMerge");
      EXPECT_EQ(arr[1].as_string(), "test_attrs_copy_extra_value");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, DelObject) {
  cpp_redis::client client;
  vector<string> keys;
  keys.push_back("rgw-object:test_object_DelObject:cache");
  clientSetUp(&client); 

  ASSERT_EQ(createUser(), 0);
  ASSERT_NE(testUser, nullptr);
   
  ASSERT_EQ(createBucket(), 0);
  ASSERT_NE(testBucket, nullptr);
  
  ASSERT_EQ(putObject("DelObject"), 0);
  ASSERT_NE(testWriter, nullptr);

  /* Check the object exists before delete op */
  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      EXPECT_EQ(reply.as_integer(), 1);
    }
  });

  client.sync_commit();

  unique_ptr<rgw::sal::Object> testObject_DelObject = testBucket->get_object(rgw_obj_key("test_object_DelObject"));

  EXPECT_NE(testObject_DelObject, nullptr);
  
  unique_ptr<rgw::sal::Object::DeleteOp> testDOp = testObject_DelObject->get_delete_op();

  EXPECT_NE(testDOp, nullptr);
  EXPECT_EQ(testDOp->delete_obj(dpp, null_yield, true), 0);

  /* Check the object does not exist after delete op */
  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      EXPECT_EQ(reply.as_integer(), 0); /* Zero keys exist */
    }
  });

  client.sync_commit();

  clientReset(&client);
}

/* Attribute-related tests */
TEST_F(D4NFilterFixture, SetObjectAttrs) {
  cpp_redis::client client;
  vector<string> fields;
  fields.push_back("test_attrs_key_SetObjectAttrs");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("SetObjectAttrs");
  unique_ptr<rgw::sal::Object> testObject_SetObjectAttrs = testBucket->get_object(rgw_obj_key("test_object_SetObjectAttrs"));

  ASSERT_NE(testObject_SetObjectAttrs, nullptr);

  buffer::list bl;
  bl.append("test_attrs_value_extra");
  map<string, bufferlist> test_attrs{{"test_attrs_key_extra", bl}};
  fields.push_back("test_attrs_key_extra");

  EXPECT_EQ(testObject_SetObjectAttrs->set_obj_attrs(dpp, &test_attrs, NULL, null_yield), 0);

  client.hgetall("rgw-object:test_object_SetObjectAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_SetObjectAttrs:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_SetObjectAttrs");
      EXPECT_EQ(arr[1].as_string(), "test_attrs_value_extra");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, GetObjectAttrs) {
  cpp_redis::client client;
  vector<string> fields;
  fields.push_back("test_attrs_key_GetObjectAttrs");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("GetObjectAttrs");
  unique_ptr<rgw::sal::Object> testObject_GetObjectAttrs = testBucket->get_object(rgw_obj_key("test_object_GetObjectAttrs"));

  ASSERT_NE(testObject_GetObjectAttrs, nullptr);

  buffer::list bl;
  bl.append("test_attrs_value_extra");
  map<string, bufferlist> test_attrs{{"test_attrs_key_extra", bl}};
  fields.push_back("test_attrs_key_extra");

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_GetObjectAttrs.get())->get_next();

  ASSERT_EQ(testObject_GetObjectAttrs->set_obj_attrs(dpp, &test_attrs, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  EXPECT_EQ(testObject_GetObjectAttrs->get_obj_attrs(null_yield, dpp, NULL), 0);

  client.hgetall("rgw-object:test_object_GetObjectAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_GetObjectAttrs:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_GetObjectAttrs");
      EXPECT_EQ(arr[1].as_string(), "test_attrs_value_extra");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, DelObjectAttrs) {
  cpp_redis::client client;
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("DelObjectAttrs");
  unique_ptr<rgw::sal::Object> testObject_DelObjectAttrs = testBucket->get_object(rgw_obj_key("test_object_DelObjectAttrs"));

  ASSERT_NE(testObject_DelObjectAttrs, nullptr);

  buffer::list bl;
  bl.append("test_attrs_value_extra");
  map<string, bufferlist> test_attrs{{"test_attrs_key_extra", bl}};
 
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_DelObjectAttrs.get())->get_next();

  ASSERT_EQ(testObject_DelObjectAttrs->set_obj_attrs(dpp, &test_attrs, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Check that the attributes exist before deletion */ 
  client.hgetall("rgw-object:test_object_DelObjectAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  EXPECT_EQ(testObject_DelObjectAttrs->set_obj_attrs(dpp, NULL, &test_attrs, null_yield), 0);

  /* Check that the attribute does not exist after deletion */ 
  client.hgetall("rgw-object:test_object_DelObjectAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hexists("rgw-object:test_object_DelObjectAttrs:cache", "test_attrs_key_extra", [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      EXPECT_EQ(reply.as_integer(), 0);
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, SetLongObjectAttrs) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_long;
  vector<string> fields;
  fields.push_back("test_attrs_key_SetLongObjectAttrs");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("SetLongObjectAttrs");
  unique_ptr<rgw::sal::Object> testObject_SetLongObjectAttrs = testBucket->get_object(rgw_obj_key("test_object_SetLongObjectAttrs"));

  ASSERT_NE(testObject_SetLongObjectAttrs, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  EXPECT_EQ(testObject_SetLongObjectAttrs->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);

  client.hgetall("rgw-object:test_object_SetLongObjectAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_SetLongObjectAttrs:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_SetLongObjectAttrs");

      for (int i = 1; i < 11; ++i) {
	EXPECT_EQ(arr[i].as_string(), "test_attrs_value_extra_" + to_string(i - 1));
      }
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, GetLongObjectAttrs) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_long;
  vector<string> fields;
  fields.push_back("test_attrs_key_GetLongObjectAttrs");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("GetLongObjectAttrs");
  unique_ptr<rgw::sal::Object> testObject_GetLongObjectAttrs = testBucket->get_object(rgw_obj_key("test_object_GetLongObjectAttrs"));

  ASSERT_NE(testObject_GetLongObjectAttrs, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_GetLongObjectAttrs.get())->get_next();

  ASSERT_EQ(testObject_GetLongObjectAttrs->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  EXPECT_EQ(testObject_GetLongObjectAttrs->get_obj_attrs(null_yield, dpp, NULL), 0);

  client.hgetall("rgw-object:test_object_GetLongObjectAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_GetLongObjectAttrs:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_GetLongObjectAttrs");

      for (int i = 1; i < 11; ++i) {
	EXPECT_EQ(arr[i].as_string(), "test_attrs_value_extra_" + to_string(i - 1));
      }
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, ModifyObjectAttr) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_long;
  vector<string> fields;
  fields.push_back("test_attrs_key_ModifyObjectAttr");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("ModifyObjectAttr");
  unique_ptr<rgw::sal::Object> testObject_ModifyObjectAttr = testBucket->get_object(rgw_obj_key("test_object_ModifyObjectAttr"));

  ASSERT_NE(testObject_ModifyObjectAttr, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_ModifyObjectAttr.get())->get_next();

  ASSERT_EQ(testObject_ModifyObjectAttr->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  buffer::list bl_tmp;
  string tmp_value = "new_test_attrs_value_extra_5";
  bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));

  EXPECT_EQ(testObject_ModifyObjectAttr->modify_obj_attrs("test_attrs_key_extra_5", bl_tmp, null_yield, dpp), 0);

  client.hgetall("rgw-object:test_object_ModifyObjectAttr:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_ModifyObjectAttr:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_ModifyObjectAttr");

      for (int i = 1; i < 11; ++i) {
	if (i == 6) {
          EXPECT_EQ(arr[i].as_string(), "new_test_attrs_value_extra_" + to_string(i - 1));
        } else {
          EXPECT_EQ(arr[i].as_string(), "test_attrs_value_extra_" + to_string(i - 1));
	}
      }
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, DelLongObjectAttrs) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_long;
  vector<string> fields;
  fields.push_back("test_attrs_key_DelLongObjectAttrs");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("DelLongObjectAttrs");
  unique_ptr<rgw::sal::Object> testObject_DelLongObjectAttrs = testBucket->get_object(rgw_obj_key("test_object_DelLongObjectAttrs"));

  ASSERT_NE(testObject_DelLongObjectAttrs, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_DelLongObjectAttrs.get())->get_next();

  ASSERT_EQ(testObject_DelLongObjectAttrs->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);
  
  /* Check that the attributes exist before deletion */
  client.hgetall("rgw-object:test_object_DelLongObjectAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  EXPECT_EQ(testObject_DelLongObjectAttrs->set_obj_attrs(dpp, NULL, &test_attrs_long, null_yield), 0);

  /* Check that the attributes do not exist after deletion */
  client.hgetall("rgw-object:test_object_DelLongObjectAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);

      for (int i = 0; i < (int)arr.size(); ++i) {
          EXPECT_EQ((int)arr[i].as_string().find("extra"), -1);
      }
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, DelObjectAttr) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_long;
  vector<string> fields;
  fields.push_back("test_attrs_key_DelObjectAttr");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("DelObjectAttr");
  unique_ptr<rgw::sal::Object> testObject_DelObjectAttr = testBucket->get_object(rgw_obj_key("test_object_DelObjectAttr"));

  ASSERT_NE(testObject_DelObjectAttr, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }
  
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_DelObjectAttr.get())->get_next();

  ASSERT_EQ(testObject_DelObjectAttr->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);
  
  /* Check that the attribute exists before deletion */
  client.hgetall("rgw-object:test_object_DelObjectAttr:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  EXPECT_EQ(testObject_DelObjectAttr->delete_obj_attrs(dpp, "test_attrs_key_extra_5", null_yield), 0);

  /* Check that the attribute does not exist after deletion */
  client.hgetall("rgw-object:test_object_DelObjectAttr:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 20 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hexists("rgw-object:test_object_DelObjectAttr:cache", "test_attrs_key_extra_5", [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      EXPECT_EQ(reply.as_integer(), 0);
    }
  });

  client.sync_commit();

  clientReset(&client);
}

/* Edge cases */
TEST_F(D4NFilterFixture, PrepareCopyObject) {
  cpp_redis::client client;
  vector<string> fields;
  fields.push_back("test_attrs_key_PrepareCopyObject");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("PrepareCopyObject");
  unique_ptr<rgw::sal::Object> testObject_PrepareCopyObject = testBucket->get_object(rgw_obj_key("test_object_PrepareCopyObject"));

  ASSERT_NE(testObject_PrepareCopyObject, nullptr);

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_PrepareCopyObject.get())->get_next();

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  unique_ptr<rgw::sal::Object::ReadOp> testROp = testObject_PrepareCopyObject->get_read_op();

  ASSERT_NE(testROp, nullptr);
  ASSERT_EQ(testROp->prepare(null_yield, dpp), 0);

  /* Update object */
  RGWEnv rgw_env;
  req_info info(get_pointer(env->cct), &rgw_env);
  rgw_zone_id source_zone;
  rgw_placement_rule dest_placement; 
  ceph::real_time src_mtime;
  ceph::real_time mtime;
  ceph::real_time mod_ptr;
  ceph::real_time unmod_ptr;
  char if_match;
  char if_nomatch;
  rgw::sal::AttrsMod attrs_mod = rgw::sal::ATTRSMOD_NONE;
  rgw::sal::Attrs attrs;
  RGWObjCategory category = RGWObjCategory::Main;
  uint64_t olh_epoch = 0;
  ceph::real_time delete_at;
  string tag;
  string etag;

  EXPECT_EQ(testObject_PrepareCopyObject->copy_object(testUser.get(),
			      &info, source_zone, testObject_PrepareCopyObject.get(),
			      testBucket.get(), testBucket.get(),
                              dest_placement, &src_mtime, &mtime,
			      &mod_ptr, &unmod_ptr, false,
			      &if_match, &if_nomatch, attrs_mod,
			      false, attrs, category, olh_epoch,
			      delete_at, NULL, &tag, &etag,
			      NULL, NULL, dpp, null_yield), 0);

  client.hgetall("rgw-object:test_object_PrepareCopyObject:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();
  
  client.hmget("rgw-object:test_object_PrepareCopyObject:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_PrepareCopyObject");
    }
  });

  client.sync_commit();
  
  clientReset(&client);
}

TEST_F(D4NFilterFixture, SetDelAttrs) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_SetDelAttrs");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("SetDelAttrs");
  unique_ptr<rgw::sal::Object> testObject_SetDelAttrs = testBucket->get_object(rgw_obj_key("test_object_SetDelAttrs"));

  ASSERT_NE(testObject_SetDelAttrs, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_SetDelAttrs.get())->get_next();

  ASSERT_EQ(testObject_SetDelAttrs->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Attempt to set and delete attrs with the same API call */
  buffer::list bl;
  bl.append("test_attrs_value_extra");
  map<string, bufferlist> test_attrs_new{{"test_attrs_key_extra", bl}};
  fields.push_back("test_attrs_key_extra");
  
  EXPECT_EQ(testObject_SetDelAttrs->set_obj_attrs(dpp, &test_attrs_new, &test_attrs_base, null_yield), 0);

  client.hgetall("rgw-object:test_object_SetDelAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_SetDelAttrs:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_SetDelAttrs");
      EXPECT_EQ(arr[1].as_string(), "test_attrs_value_extra");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, ModifyNonexistentAttr) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_ModifyNonexistentAttr");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("ModifyNonexistentAttr");
  unique_ptr<rgw::sal::Object> testObject_ModifyNonexistentAttr = testBucket->get_object(rgw_obj_key("test_object_ModifyNonexistentAttr"));

  ASSERT_NE(testObject_ModifyNonexistentAttr, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_ModifyNonexistentAttr.get())->get_next();

  ASSERT_EQ(testObject_ModifyNonexistentAttr->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);
  
  buffer::list bl_tmp;
  bl_tmp.append("new_test_attrs_value_extra_ModifyNonexistentAttr");

  EXPECT_EQ(testObject_ModifyNonexistentAttr->modify_obj_attrs("test_attrs_key_extra_ModifyNonexistentAttr", bl_tmp, null_yield, dpp), 0);

  fields.push_back("test_attrs_key_extra_ModifyNonexistentAttr");

  client.hgetall("rgw-object:test_object_ModifyNonexistentAttr:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 24 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_ModifyNonexistentAttr:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_ModifyNonexistentAttr");

      for (int i = 1; i < 11; ++i) {
	EXPECT_EQ(arr[i].as_string(), "test_attrs_value_extra_" + to_string(i - 1));
      }

      /* New attribute will be created and stored since it was not found in the existing attributes */
      EXPECT_EQ(arr[11].as_string(), "new_test_attrs_value_extra_ModifyNonexistentAttr");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, ModifyGetAttrs) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_ModifyGetAttrs");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("ModifyGetAttrs");
  unique_ptr<rgw::sal::Object> testObject_ModifyGetAttrs = testBucket->get_object(rgw_obj_key("test_object_ModifyGetAttrs"));

  ASSERT_NE(testObject_ModifyGetAttrs, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_ModifyGetAttrs.get())->get_next();

  ASSERT_EQ(testObject_ModifyGetAttrs->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Attempt to get immediately after a modification */
  buffer::list bl_tmp;
  bl_tmp.append("new_test_attrs_value_extra_5");

  ASSERT_EQ(testObject_ModifyGetAttrs->modify_obj_attrs("test_attrs_key_extra_5", bl_tmp, null_yield, dpp), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  EXPECT_EQ(testObject_ModifyGetAttrs->get_obj_attrs(null_yield, dpp, NULL), 0);

  client.hgetall("rgw-object:test_object_ModifyGetAttrs:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_ModifyGetAttrs:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_ModifyGetAttrs");

      for (int i = 1; i < 11; ++i) {
	if (i == 6) {
          EXPECT_EQ(arr[i].as_string(), "new_test_attrs_value_extra_5");
        } else {
          EXPECT_EQ(arr[i].as_string(), "test_attrs_value_extra_" + to_string(i - 1));
	}
      }
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, DelNonexistentAttr) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_DelNonexistentAttr");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("DelNonexistentAttr");
  unique_ptr<rgw::sal::Object> testObject_DelNonexistentAttr = testBucket->get_object(rgw_obj_key("test_object_DelNonexistentAttr"));

  ASSERT_NE(testObject_DelNonexistentAttr, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_DelNonexistentAttr.get())->get_next();

  ASSERT_EQ(testObject_DelNonexistentAttr->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);
 
  /* Attempt to delete an attribute that does not exist */
  ASSERT_EQ(testObject_DelNonexistentAttr->delete_obj_attrs(dpp, "test_attrs_key_extra_12", null_yield), 0);

  client.hgetall("rgw-object:test_object_DelNonexistentAttr:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_DelNonexistentAttr:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_DelNonexistentAttr");

      for (int i = 1; i < 11; ++i) {
	EXPECT_EQ(arr[i].as_string(), "test_attrs_value_extra_" + to_string(i - 1));
      }
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, DelSetWithNonexisentAttr) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_DelSetWithNonexistentAttr");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("DelSetWithNonexistentAttr");
  unique_ptr<rgw::sal::Object> testObject_DelSetWithNonexistentAttr = testBucket->get_object(rgw_obj_key("test_object_DelSetWithNonexistentAttr"));

  ASSERT_NE(testObject_DelSetWithNonexistentAttr, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  ASSERT_EQ(testObject_DelSetWithNonexistentAttr->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_DelSetWithNonexistentAttr.get())->get_next();

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  EXPECT_EQ(testObject_DelSetWithNonexistentAttr->delete_obj_attrs(dpp, "test_attrs_key_extra_5", null_yield), 0);
  
  /* Attempt to delete a set of attrs, including one that does not exist */
  EXPECT_EQ(testObject_DelSetWithNonexistentAttr->set_obj_attrs(dpp, NULL, &test_attrs_base, null_yield), 0);

  client.hgetall("rgw-object:test_object_DelSetWithNonexistentAttr:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  clientReset(&client);
}

/* Underlying store attribute check */
TEST_F(D4NFilterFixture, StoreSetAttr) {
  createUser();
  createBucket();
  putObject("StoreSetAttr");
  unique_ptr<rgw::sal::Object> testObject_StoreSetAttr = testBucket->get_object(rgw_obj_key("test_object_StoreSetAttr"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_StoreSetAttr.get())->get_next();

  EXPECT_NE(nextObject, nullptr);

  /* Set one attribute */
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Check the attribute */ 
  rgw::sal::Attrs driverAttrs = nextObject->get_attrs();
  pair<string, string> value(driverAttrs.begin()->first, driverAttrs.begin()->second.to_str());

  EXPECT_EQ(value, make_pair(string("test_attrs_key_StoreSetAttr"), string("test_attrs_value_StoreSetAttr")));
}

TEST_F(D4NFilterFixture, StoreSetAttrs) {
  createUser();
  createBucket();
  putObject("StoreSetAttrs");
  unique_ptr<rgw::sal::Object> testObject_StoreSetAttrs = testBucket->get_object(rgw_obj_key("test_object_StoreSetAttrs"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_StoreSetAttrs.get())->get_next();

  EXPECT_NE(nextObject, nullptr);

  /* Delete base attribute for easier comparison */
  testObject_StoreSetAttrs->delete_obj_attrs(dpp, "test_attrs_key_StoreSetAttrs", null_yield);

  /* Set more attributes */
  map<string, bufferlist> test_attrs_base;

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  testObject_StoreSetAttrs->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Check the attributes */ 
  rgw::sal::Attrs driverAttrs = nextObject->get_attrs();
  rgw::sal::Attrs::iterator attrs;
  vector< pair<string, string> > values;

  for (attrs = driverAttrs.begin(); attrs != driverAttrs.end(); ++attrs) {
    values.push_back(make_pair(attrs->first, attrs->second.to_str()));
  }

  int i = 0;

  for (const auto& pair : values) {
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    string tmp_value = "test_attrs_value_extra_" + to_string(i);

    EXPECT_EQ(pair, make_pair(tmp_key, tmp_value));
    ++i;
  }
}

TEST_F(D4NFilterFixture, StoreGetAttrs) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("StoreGetAttrs");
  unique_ptr<rgw::sal::Object> testObject_StoreGetAttrs = testBucket->get_object(rgw_obj_key("test_object_StoreGetAttrs"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_StoreGetAttrs.get())->get_next();

  EXPECT_NE(nextObject, nullptr);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Delete base attribute for easier comparison */
  testObject_StoreGetAttrs->delete_obj_attrs(dpp, "test_attrs_key_StoreGetAttrs", null_yield);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Set more attributes */
  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  testObject_StoreGetAttrs->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield);
  nextObject->get_obj_attrs(null_yield, dpp, NULL);

  /* Change an attribute through redis */
  vector< pair<string, string> > value;
  value.push_back(make_pair("test_attrs_key_extra_5", "new_test_attrs_value_extra_5"));

  client.hmset("rgw-object:test_object_StoreGetAttrs:cache", value, [&](cpp_redis::reply& reply) {
    if (!reply.is_null()) {
      EXPECT_EQ(reply.as_string(), "OK");
    }
  });

  client.sync_commit();

  /* Artificially adding the data field so getObject will succeed 
     for the purposes of this test                                */
  value.clear();
  value.push_back(make_pair("data", ""));

  client.hmset("rgw-object:test_object_StoreGetAttrs:cache", value, [&](cpp_redis::reply& reply) {
    if (!reply.is_null()) {
      ASSERT_EQ(reply.as_string(), "OK");
    }
  });

  client.sync_commit();

  ASSERT_EQ(testObject_StoreGetAttrs->get_obj_attrs(null_yield, dpp, NULL), 0); /* Cache attributes */

  /* Check the attributes on the store layer */ 
  rgw::sal::Attrs driverAttrs = nextObject->get_attrs();
  rgw::sal::Attrs::iterator driverattrs;
  vector< pair<string, string> > driverValues;

  for (driverattrs = driverAttrs.begin(); driverattrs != driverAttrs.end(); ++driverattrs) {
    driverValues.push_back(make_pair(driverattrs->first, driverattrs->second.to_str()));
  }

  EXPECT_EQ((int)driverValues.size(), 10);

  int i = 0;

  for (const auto& pair : driverValues) {
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    string tmp_value = "test_attrs_value_extra_" + to_string(i);

    if (i == 5) {
      tmp_value = "new_" + tmp_value;
    }

    EXPECT_EQ(pair, make_pair(tmp_key, tmp_value));
    ++i;
  }

  /* Restore and check original attributes */
  nextObject->get_obj_attrs(null_yield, dpp, NULL);
  driverAttrs = nextObject->get_attrs();
  driverValues.clear();

  for (driverattrs = driverAttrs.begin(); driverattrs != driverAttrs.end(); ++driverattrs) {
    driverValues.push_back(make_pair(driverattrs->first, driverattrs->second.to_str()));
  }

  EXPECT_EQ((int)driverValues.size(), 10);

  i = 0;

  for (const auto& pair : driverValues) {
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    string tmp_value = "test_attrs_value_extra_" + to_string(i);

    EXPECT_EQ(pair, make_pair(tmp_key, tmp_value));
    ++i;
  }

  clientReset(&client);
}

TEST_F(D4NFilterFixture, StoreGetMetadata) {
  cpp_redis::client client;
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject("StoreGetMetadata");
  unique_ptr<rgw::sal::Object> testObject_StoreGetMetadata = testBucket->get_object(rgw_obj_key("test_object_StoreGetMetadata"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_StoreGetMetadata.get())->get_next();

  EXPECT_NE(nextObject, nullptr);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Change metadata values through redis */
  vector< pair<string, string> > value;
  value.push_back(make_pair("mtime", "2021-11-08T21:13:38.334696731Z"));
  value.push_back(make_pair("object_size", "100"));
  value.push_back(make_pair("accounted_size", "200"));
  value.push_back(make_pair("epoch", "3")); /* version_id is not tested because the object does not have an instance */
  value.push_back(make_pair("source_zone_short_id", "300"));
  value.push_back(make_pair("bucket_count", "10"));
  value.push_back(make_pair("bucket_size", "20"));

  client.hmset("rgw-object:test_object_StoreGetMetadata:cache", value, [](cpp_redis::reply& reply) {
    if (!reply.is_null()) {
      EXPECT_EQ(reply.as_string(), "OK");
    }
  });

  client.sync_commit();

  /* Artificially adding the data field so getObject will succeed 
     for the purposes of this test                                */
  value.clear();
  value.push_back(make_pair("data", ""));

  client.hmset("rgw-object:test_object_StoreGetMetadata:cache", value, [](cpp_redis::reply& reply) {
    if (!reply.is_null()) {
      ASSERT_EQ(reply.as_string(), "OK");
    }
  });

  client.sync_commit();

  unique_ptr<rgw::sal::Object::ReadOp> testROp = testObject_StoreGetMetadata->get_read_op();

  ASSERT_NE(testROp, nullptr);
  ASSERT_EQ(testROp->prepare(null_yield, dpp), 0);

  /* Check updated metadata values */ 
  static StoreObject* storeObject = static_cast<StoreObject*>(dynamic_cast<rgw::sal::FilterObject*>(testObject_StoreGetMetadata.get())->get_next());

  EXPECT_EQ(to_iso_8601(storeObject->state.mtime), "2021-11-08T21:13:38.334696731Z");
  EXPECT_EQ(testObject_StoreGetMetadata->get_obj_size(), (uint64_t)100);
  EXPECT_EQ(storeObject->state.accounted_size, (uint64_t)200);
  EXPECT_EQ(storeObject->state.epoch, (uint64_t)3);
  EXPECT_EQ(storeObject->state.zone_short_id, (uint32_t)300);
}

TEST_F(D4NFilterFixture, StoreModifyAttr) {
  createUser();
  createBucket();
  putObject("StoreModifyAttr");
  unique_ptr<rgw::sal::Object> testObject_StoreModifyAttr = testBucket->get_object(rgw_obj_key("test_object_StoreModifyAttr"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_StoreModifyAttr.get())->get_next();

  ASSERT_NE(nextObject, nullptr);

  /* Modify existing attribute */
  buffer::list bl_tmp;
  string tmp_value = "new_test_attrs_value_StoreModifyAttr";
  bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
  
  testObject_StoreModifyAttr->modify_obj_attrs("test_attrs_key_StoreModifyAttr", bl_tmp, null_yield, dpp);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Check the attribute */ 
  rgw::sal::Attrs driverAttrs = nextObject->get_attrs();
  pair<string, string> value(driverAttrs.begin()->first, driverAttrs.begin()->second.to_str());

  EXPECT_EQ(value, make_pair(string("test_attrs_key_StoreModifyAttr"), string("new_test_attrs_value_StoreModifyAttr")));
}

TEST_F(D4NFilterFixture, StoreDelAttrs) {
  createUser();
  createBucket();
  putObject("StoreDelAttrs");
  unique_ptr<rgw::sal::Object> testObject_StoreDelAttrs = testBucket->get_object(rgw_obj_key("test_object_StoreDelAttrs"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_StoreDelAttrs.get())->get_next();

  ASSERT_NE(nextObject, nullptr);

  /* Set more attributes */
  map<string, bufferlist> test_attrs_base;

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  testObject_StoreDelAttrs->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Check that the attributes exist before deletion */
  rgw::sal::Attrs driverAttrs = nextObject->get_attrs();

  EXPECT_EQ(driverAttrs.size(), (long unsigned int)11);

  rgw::sal::Attrs::iterator driverattrs;
  vector< pair<string, string> > driverValues;

  for (driverattrs = ++driverAttrs.begin(); driverattrs != driverAttrs.end(); ++driverattrs) {
    driverValues.push_back(make_pair(driverattrs->first, driverattrs->second.to_str()));
  }

  int i = 0;

  for (const auto& pair : driverValues) {
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    string tmp_value = "test_attrs_value_extra_" + to_string(i);

    EXPECT_EQ(pair, make_pair(tmp_key, tmp_value));
    ++i;
  }

  testObject_StoreDelAttrs->set_obj_attrs(dpp, NULL, &test_attrs_base, null_yield);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Check that the attributes do not exist after deletion */ 
  driverAttrs = nextObject->get_attrs();

  EXPECT_EQ(driverAttrs.size(), (long unsigned int)1);

  pair<string, string> value(driverAttrs.begin()->first, driverAttrs.begin()->second.to_str());

  EXPECT_EQ(value, make_pair(string("test_attrs_key_StoreDelAttrs"), string("test_attrs_value_StoreDelAttrs")));
}

/* SAL object data storage check */
TEST_F(D4NFilterFixture, DataCheck) {
  cpp_redis::client client;
  clientSetUp(&client); 

  createUser();
  createBucket();
  
  /* Prepare, process, and complete object write */
  unique_ptr<rgw::sal::Object> obj = testBucket->get_object(rgw_obj_key("test_object_DataCheck"));
  rgw_user owner;
  rgw_placement_rule ptail_placement_rule;
  uint64_t olh_epoch = 123;
  string unique_tag;

  obj->get_obj_attrs(null_yield, dpp);

  testWriter = driver->get_atomic_writer(dpp, 
	    null_yield,
	    obj.get(),
	    owner,
	    &ptail_placement_rule,
	    olh_epoch,
	    unique_tag);

  size_t accounted_size = 4;
  string etag("test_etag");
  ceph::real_time mtime; 
  ceph::real_time set_mtime;

  buffer::list bl;
  string tmp = "test_attrs_value_DataCheck";
  bl.append("test_attrs_value_DataCheck");
  map<string, bufferlist> attrs{{"test_attrs_key_DataCheck", bl}};
  buffer::list data;
  data.append("test data");

  ceph::real_time delete_at;
  char if_match;
  char if_nomatch;
  string user_data;
  rgw_zone_set zones_trace;
  bool canceled;

  ASSERT_EQ(testWriter->prepare(null_yield), 0);
  
  ASSERT_EQ(testWriter->process(std::move(data), 0), 0);

  ASSERT_EQ(testWriter->complete(accounted_size, etag,
		 &mtime, set_mtime,
		 attrs,
		 delete_at,
		 &if_match, &if_nomatch,
		 &user_data,
		 &zones_trace, &canceled,
		 rctx, rgw::sal::FLAG_LOG_OP), 0);
 
  client.hget("rgw-object:test_object_DataCheck:cache", "data", [&data](cpp_redis::reply& reply) {
    if (reply.is_string()) {
      EXPECT_EQ(reply.as_string(), data.to_str());
    }
  });

  client.sync_commit();

  /* Change data and ensure redis stores the new value */
  buffer::list dataNew;
  dataNew.append("new test data");

  ASSERT_EQ(testWriter->prepare(null_yield), 0);
  
  ASSERT_EQ(testWriter->process(std::move(dataNew), 0), 0);

  ASSERT_EQ(testWriter->complete(accounted_size, etag,
		 &mtime, set_mtime,
		 attrs,
		 delete_at,
		 &if_match, &if_nomatch,
		 &user_data,
		 &zones_trace, &canceled,
		 rctx, rgw::sal::FLAG_LOG_OP), 0);

  client.hget("rgw-object:test_object_DataCheck:cache", "data", [&dataNew](cpp_redis::reply& reply) {
    if (reply.is_string()) {
      EXPECT_EQ(reply.as_string(), dataNew.to_str());
    }
  });

  client.sync_commit();

  clientReset(&client);
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  /* Other host and port can be passed to the program */
  if (argc == 1) {
    portStr = "6379";
    hostStr = "127.0.0.1";
  } else if (argc == 3) {
    hostStr = argv[1];
    portStr = argv[2];
  } else {
    std::cout << "Incorrect number of arguments." << std::endl;
    return -1;
  }

  redisHost = hostStr + ":" + portStr;

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
