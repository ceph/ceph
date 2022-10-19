#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include <iostream>
#include <string>
#include "dbstore.h"
#include <cpp_redis/cpp_redis>
#include "rgw_sal_d4n.h"

#define dout_subsys ceph_subsys_rgw

#define METADATA_LENGTH 20

using namespace std;

string portStr;
string hostStr;
string redisHost = "";

vector<const char*> args;
class Environment* env;
const DoutPrefixProvider* dpp;

class Environment : public ::testing::Environment {
  public:
    Environment() {}
    
    virtual ~Environment() {}

    void SetUp() override {
      global_pre_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
      cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, CINIT_FLAG_NO_MON_CONFIG, false);
      
      dpp = new DoutPrefix(cct->get(), dout_subsys, "d4n test: ");
      StoreManager::Config cfg;

      cfg.store_name = "dbstore";
      cfg.filter_name = "d4n";
      
      store = StoreManager::get_storage(dpp, dpp->get_cct(),
              cfg,
              false,
              false,
              false,
              false,
              false,
              false,
	      false); 
    
      ASSERT_NE(store, nullptr);
    }

    void TearDown() override {
      delete store;
      delete dpp;
    }

    boost::intrusive_ptr<CephContext> cct;
    rgw::sal::Store* store;
};

class D4NFilterFixture : public ::testing::Test {
  protected:
    rgw::sal::Store* store;
    unique_ptr<rgw::sal::User> testUser = nullptr;
    unique_ptr<rgw::sal::Bucket> testBucket = nullptr;
    unique_ptr<rgw::sal::Writer> testWriter  = nullptr;

  public:
    D4NFilterFixture() {}
    
    void SetUp() {
      store = env->store;
    }

    void TearDown() {}
    
    int createUser() {
      rgw_user u("test_tenant", "test_user", "ns");

      testUser = store->get_user(u);
      testUser->get_info().user_id = u;

      int ret = testUser->store_user(dpp, null_yield, false);

      return ret;
    }

    int createBucket() {
      rgw_bucket b;
      std::string zonegroup_id = "test_id";
      rgw_placement_rule placement_rule;
      string swift_ver_location = "test_location";
      const RGWAccessControlPolicy policy;
      rgw::sal::Attrs attrs;
      RGWBucketInfo info;
      obj_version ep_objv;
      bool bucket_exists;
      int ret;
      
      CephContext* cct = get_pointer(env->cct);
      RGWEnv rgw_env;
      req_state s(cct->get(), &rgw_env, 0);
      req_info _req_info = s.info;

      b.name = "test_bucket";
      placement_rule.storage_class = "test_sc";

      ret = testUser->create_bucket(dpp, b,
	    zonegroup_id,
	    placement_rule,
	    swift_ver_location,
	    nullptr,
	    policy,
	    attrs,
	    info,
	    ep_objv,
	    false,
	    false,
	    &bucket_exists,
	    _req_info,
	    &testBucket,
	    null_yield);
	
      return ret;
    }

    int putObject(int i) {
      string object_name = "test_object_" + to_string(i);
      unique_ptr<rgw::sal::Object> head_obj = testBucket->get_object(rgw_obj_key(object_name));
      rgw_user owner;
      rgw_placement_rule ptail_placement_rule;
      uint64_t olh_epoch = 123;
      string unique_tag;

      head_obj->get_obj_attrs(null_yield, dpp);

      testWriter = store->get_atomic_writer(dpp, 
		  null_yield,
		  head_obj->clone(),
		  owner,
		  &ptail_placement_rule,
		  olh_epoch,
		  unique_tag);
  
      size_t accounted_size = 4;
      string etag("test_etag");
      ceph::real_time mtime; 
      ceph::real_time set_mtime;

      buffer::list bl;
      string tmp = "test_attrs_value_" + to_string(i);
      bl.append("test_attrs_value_" + to_string(i));
      map<string, bufferlist> attrs{{"test_attrs_key_" + to_string(i), bl}};

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
                       null_yield);

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
  
  EXPECT_EQ(putObject(0), 0);
  EXPECT_NE(testWriter, nullptr);

  client.hgetall("rgw-object:test_object_0:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_0:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_0");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, GetObject) {
  cpp_redis::client client;
  vector<string> fields;
  fields.push_back("test_attrs_key_1");
  clientSetUp(&client); 

  ASSERT_EQ(createUser(), 0);
  ASSERT_NE(testUser, nullptr);
   
  ASSERT_EQ(createBucket(), 0);
  ASSERT_NE(testBucket, nullptr);
  
  ASSERT_EQ(putObject(1), 0);
  ASSERT_NE(testWriter, nullptr);

  unique_ptr<rgw::sal::Object> testObject_1 = testBucket->get_object(rgw_obj_key("test_object_1"));

  EXPECT_NE(testObject_1, nullptr);
  
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_1.get())->get_next();

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  
  unique_ptr<rgw::sal::Object::ReadOp> testROp = testObject_1->get_read_op();

  EXPECT_NE(testROp, nullptr);
  EXPECT_EQ(testROp->prepare(null_yield, dpp), 0);

  client.hgetall("rgw-object:test_object_1:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_1:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_1");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, DelObject) {
  cpp_redis::client client;
  vector<string> keys;
  keys.push_back("rgw-object:test_object_2:cache");
  clientSetUp(&client); 

  ASSERT_EQ(createUser(), 0);
  ASSERT_NE(testUser, nullptr);
   
  ASSERT_EQ(createBucket(), 0);
  ASSERT_NE(testBucket, nullptr);
  
  ASSERT_EQ(putObject(2), 0);
  ASSERT_NE(testWriter, nullptr);

  /* Check the object exists before delete op */
  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      EXPECT_EQ(reply.as_integer(), 1);
    }
  });

  client.sync_commit();

  unique_ptr<rgw::sal::Object> testObject_2 = testBucket->get_object(rgw_obj_key("test_object_2"));

  EXPECT_NE(testObject_2, nullptr);
  
  unique_ptr<rgw::sal::Object::DeleteOp> testDOp = testObject_2->get_delete_op();

  EXPECT_NE(testDOp, nullptr);
  EXPECT_EQ(testDOp->delete_obj(dpp, null_yield), 0);

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
  fields.push_back("test_attrs_key_3");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(3);
  unique_ptr<rgw::sal::Object> testObject_3 = testBucket->get_object(rgw_obj_key("test_object_3"));

  ASSERT_NE(testObject_3, nullptr);

  buffer::list bl;
  bl.append("test_attrs_value_extra");
  map<string, bufferlist> test_attrs{{"test_attrs_key_extra", bl}};
  fields.push_back("test_attrs_key_extra");

  EXPECT_EQ(testObject_3->set_obj_attrs(dpp, &test_attrs, NULL, null_yield), 0);

  client.hgetall("rgw-object:test_object_3:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_3:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_3");
      EXPECT_EQ(arr[1].as_string(), "test_attrs_value_extra");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, GetObjectAttrs) {
  cpp_redis::client client;
  vector<string> fields;
  fields.push_back("test_attrs_key_4");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(4);
  unique_ptr<rgw::sal::Object> testObject_4 = testBucket->get_object(rgw_obj_key("test_object_4"));

  ASSERT_NE(testObject_4, nullptr);

  buffer::list bl;
  bl.append("test_attrs_value_extra");
  map<string, bufferlist> test_attrs{{"test_attrs_key_extra", bl}};
  fields.push_back("test_attrs_key_extra");

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_4.get())->get_next();

  ASSERT_EQ(testObject_4->set_obj_attrs(dpp, &test_attrs, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  EXPECT_EQ(testObject_4->get_obj_attrs(null_yield, dpp, NULL), 0);

  client.hgetall("rgw-object:test_object_4:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_4:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_4");
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
  putObject(5);
  unique_ptr<rgw::sal::Object> testObject_5 = testBucket->get_object(rgw_obj_key("test_object_5"));

  ASSERT_NE(testObject_5, nullptr);

  buffer::list bl;
  bl.append("test_attrs_value_extra");
  map<string, bufferlist> test_attrs{{"test_attrs_key_extra", bl}};
 
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_5.get())->get_next();

  ASSERT_EQ(testObject_5->set_obj_attrs(dpp, &test_attrs, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Check that the attributes exist before deletion */ 
  client.hgetall("rgw-object:test_object_5:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  EXPECT_EQ(testObject_5->set_obj_attrs(dpp, NULL, &test_attrs, null_yield), 0);

  /* Check that the attribute does not exist after deletion */ 
  client.hgetall("rgw-object:test_object_5:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hexists("rgw-object:test_object_5:cache", "test_attrs_key_extra", [](cpp_redis::reply& reply) {
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
  fields.push_back("test_attrs_key_6");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(6);
  unique_ptr<rgw::sal::Object> testObject_6 = testBucket->get_object(rgw_obj_key("test_object_6"));

  ASSERT_NE(testObject_6, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  EXPECT_EQ(testObject_6->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);

  client.hgetall("rgw-object:test_object_6:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_6:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_6");

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
  fields.push_back("test_attrs_key_7");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(7);
  unique_ptr<rgw::sal::Object> testObject_7 = testBucket->get_object(rgw_obj_key("test_object_7"));

  ASSERT_NE(testObject_7, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_7.get())->get_next();

  ASSERT_EQ(testObject_7->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  EXPECT_EQ(testObject_7->get_obj_attrs(null_yield, dpp, NULL), 0);

  client.hgetall("rgw-object:test_object_7:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_7:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_7");

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
  fields.push_back("test_attrs_key_8");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(8);
  unique_ptr<rgw::sal::Object> testObject_8 = testBucket->get_object(rgw_obj_key("test_object_8"));

  ASSERT_NE(testObject_8, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_8.get())->get_next();

  ASSERT_EQ(testObject_8->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  buffer::list bl_tmp;
  string tmp_value = "new_test_attrs_value_extra_5";
  bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));

  EXPECT_EQ(testObject_8->modify_obj_attrs("test_attrs_key_extra_5", bl_tmp, null_yield, dpp), 0);

  client.hgetall("rgw-object:test_object_8:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_8:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_8");

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
  fields.push_back("test_attrs_key_9");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(9);
  unique_ptr<rgw::sal::Object> testObject_9 = testBucket->get_object(rgw_obj_key("test_object_9"));

  ASSERT_NE(testObject_9, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_9.get())->get_next();

  ASSERT_EQ(testObject_9->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);
  
  /* Check that the attributes exist before deletion */
  client.hgetall("rgw-object:test_object_9:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  EXPECT_EQ(testObject_9->set_obj_attrs(dpp, NULL, &test_attrs_long, null_yield), 0);

  /* Check that the attributes do not exist after deletion */
  client.hgetall("rgw-object:test_object_9:cache", [](cpp_redis::reply& reply) {
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
  fields.push_back("test_attrs_key_10");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(10);
  unique_ptr<rgw::sal::Object> testObject_10 = testBucket->get_object(rgw_obj_key("test_object_10"));

  ASSERT_NE(testObject_10, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_long.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }
  
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_10.get())->get_next();

  ASSERT_EQ(testObject_10->set_obj_attrs(dpp, &test_attrs_long, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);
  
  /* Check that the attribute exists before deletion */
  client.hgetall("rgw-object:test_object_10:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  EXPECT_EQ(testObject_10->delete_obj_attrs(dpp, "test_attrs_key_extra_5", null_yield), 0);

  /* Check that the attribute does not exist after deletion */
  client.hgetall("rgw-object:test_object_10:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 20 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hexists("rgw-object:test_object_10:cache", "test_attrs_key_extra_5", [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      EXPECT_EQ(reply.as_integer(), 0);
    }
  });

  client.sync_commit();

  clientReset(&client);
}

/* Edge cases */
TEST_F(D4NFilterFixture, SetDeleteAttrsTest) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_11");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(11);
  unique_ptr<rgw::sal::Object> testObject_11 = testBucket->get_object(rgw_obj_key("test_object_11"));

  ASSERT_NE(testObject_11, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_11.get())->get_next();

  ASSERT_EQ(testObject_11->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Attempt to set and delete attrs with the same API call */
  buffer::list bl;
  bl.append("test_attrs_value_extra");
  map<string, bufferlist> test_attrs_new{{"test_attrs_key_extra", bl}};
  fields.push_back("test_attrs_key_extra");
  
  EXPECT_EQ(testObject_11->set_obj_attrs(dpp, &test_attrs_new, &test_attrs_base, null_yield), 0);

  client.hgetall("rgw-object:test_object_11:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 4 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_11:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_11");
      EXPECT_EQ(arr[1].as_string(), "test_attrs_value_extra");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, ModifyNonexistentAttrTest) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_12");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(12);
  unique_ptr<rgw::sal::Object> testObject_12 = testBucket->get_object(rgw_obj_key("test_object_12"));

  ASSERT_NE(testObject_12, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_12.get())->get_next();

  ASSERT_EQ(testObject_12->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);
  
  buffer::list bl_tmp;
  bl_tmp.append("new_test_attrs_value_extra_12");

  EXPECT_EQ(testObject_12->modify_obj_attrs("test_attrs_key_extra_12", bl_tmp, null_yield, dpp), 0);

  fields.push_back("test_attrs_key_extra_12");
  client.hgetall("rgw-object:test_object_12:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 24 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_12:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_12");

      for (int i = 1; i < 11; ++i) {
	EXPECT_EQ(arr[i].as_string(), "test_attrs_value_extra_" + to_string(i - 1));
      }

      /* New attribute will be created and stored since it was not found in the existing attributes */
      EXPECT_EQ(arr[11].as_string(), "new_test_attrs_value_extra_12");
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, ModifyGetTest) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_13");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(13);
  unique_ptr<rgw::sal::Object> testObject_13 = testBucket->get_object(rgw_obj_key("test_object_13"));

  ASSERT_NE(testObject_13, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_13.get())->get_next();

  ASSERT_EQ(testObject_13->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Attempt to get immediately after a modification */
  buffer::list bl_tmp;
  bl_tmp.append("new_test_attrs_value_extra_5");

  ASSERT_EQ(testObject_13->modify_obj_attrs("test_attrs_key_extra_5", bl_tmp, null_yield, dpp), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  EXPECT_EQ(testObject_13->get_obj_attrs(null_yield, dpp, NULL), 0);

  client.hgetall("rgw-object:test_object_13:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_13:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_13");

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

TEST_F(D4NFilterFixture, DeleteNonexistentAttrTest) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_14");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(14);
  unique_ptr<rgw::sal::Object> testObject_14 = testBucket->get_object(rgw_obj_key("test_object_14"));

  ASSERT_NE(testObject_14, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
    fields.push_back(tmp_key);
  }

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_14.get())->get_next();

  ASSERT_EQ(testObject_14->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);
 
  /* Attempt to delete an attribute that does not exist */
  ASSERT_EQ(testObject_14->delete_obj_attrs(dpp, "test_attrs_key_extra_12", null_yield), 0);

  client.hgetall("rgw-object:test_object_14:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 22 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  client.hmget("rgw-object:test_object_14:cache", fields, [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ(arr[0].as_string(), "test_attrs_value_14");

      for (int i = 1; i < 11; ++i) {
	EXPECT_EQ(arr[i].as_string(), "test_attrs_value_extra_" + to_string(i - 1));
      }
    }
  });

  client.sync_commit();

  clientReset(&client);
}

TEST_F(D4NFilterFixture, DeleteSetWithNonexisentAttrTest) {
  cpp_redis::client client;
  map<string, bufferlist> test_attrs_base;
  vector<string> fields;
  fields.push_back("test_attrs_key_15");
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(15);
  unique_ptr<rgw::sal::Object> testObject_15 = testBucket->get_object(rgw_obj_key("test_object_15"));

  ASSERT_NE(testObject_15, nullptr);

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  ASSERT_EQ(testObject_15->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield), 0);

  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_15.get())->get_next();

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  EXPECT_EQ(testObject_15->delete_obj_attrs(dpp, "test_attrs_key_extra_5", null_yield), 0);
  
  /* Attempt to delete a set of attrs, including one that does not exist */
  EXPECT_EQ(testObject_15->set_obj_attrs(dpp, NULL, &test_attrs_base, null_yield), 0);

  client.hgetall("rgw-object:test_object_15:cache", [](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      EXPECT_EQ((int)arr.size(), 2 + METADATA_LENGTH);
    }
  });

  client.sync_commit();

  clientReset(&client);
}

/* Underlying store attribute check */
TEST_F(D4NFilterFixture, StoreSetAttrTest) {
  createUser();
  createBucket();
  putObject(16);
  unique_ptr<rgw::sal::Object> testObject_16 = testBucket->get_object(rgw_obj_key("test_object_16"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_16.get())->get_next();

  EXPECT_NE(nextObject, nullptr);

  /* Set one attribute */
  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);
  ASSERT_NE(nextObject->get_attrs().empty(), true);

  /* Check the attribute */ 
  rgw::sal::Attrs storeAttrs = nextObject->get_attrs();
  pair<string, string> value(storeAttrs.begin()->first, storeAttrs.begin()->second.to_str());

  EXPECT_EQ(value, make_pair(string("test_attrs_key_16"), string("test_attrs_value_16")));
}

TEST_F(D4NFilterFixture, StoreSetAttrsTest) {
  createUser();
  createBucket();
  putObject(17);
  unique_ptr<rgw::sal::Object> testObject_17 = testBucket->get_object(rgw_obj_key("test_object_17"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_17.get())->get_next();

  EXPECT_NE(nextObject, nullptr);

  /* Delete base attribute for easier comparison */
  testObject_17->delete_obj_attrs(dpp, "test_attrs_key_17", null_yield);

  /* Set more attributes */
  map<string, bufferlist> test_attrs_base;

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  testObject_17->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Check the attributes */ 
  rgw::sal::Attrs storeAttrs = nextObject->get_attrs();
  rgw::sal::Attrs::iterator attrs;
  vector< pair<string, string> > values;

  for (attrs = storeAttrs.begin(); attrs != storeAttrs.end(); ++attrs) {
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

TEST_F(D4NFilterFixture, StoreGetAttrsTest) {
  cpp_redis::client client;
  string result;
  map<string, bufferlist> test_attrs_base;
  clientSetUp(&client); 

  createUser();
  createBucket();
  putObject(18);
  unique_ptr<rgw::sal::Object> testObject_18 = testBucket->get_object(rgw_obj_key("test_object_18"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_18.get())->get_next();

  EXPECT_NE(nextObject, nullptr);

  /* Delete base attribute for easier comparison */
  testObject_18->delete_obj_attrs(dpp, "test_attrs_key_18", null_yield);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Set more attributes */
  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  testObject_18->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield);
  nextObject->get_obj_attrs(null_yield, dpp, NULL);

  /* Change an attribute through redis */
  buffer::list bl;
  bl.append("new_test_attrs_value_extra_5");
  vector< pair<string, string> > value;
  value.push_back(make_pair("test_attrs_key_extra_5", bl.to_str()));

  client.hmset("rgw-object:test_object_18:cache", value, [&result](cpp_redis::reply& reply) {
    if (!reply.is_null()) {
      result = reply.as_string();
    }
  });

  client.sync_commit();

  EXPECT_EQ(result, "OK");

  ASSERT_EQ(testObject_18->get_obj_attrs(null_yield, dpp, NULL), 0); /* Cache attributes */

  /* Check the attributes on the store layer */ 
  rgw::sal::Attrs storeAttrs = nextObject->get_attrs();
  rgw::sal::Attrs::iterator storeattrs;
  vector< pair<string, string> > storeValues;

  for (storeattrs = storeAttrs.begin(); storeattrs != storeAttrs.end(); ++storeattrs) {
    storeValues.push_back(make_pair(storeattrs->first, storeattrs->second.to_str()));
  }

  EXPECT_EQ((int)storeValues.size(), 10);

  int i = 0;

  for (const auto& pair : storeValues) {
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    string tmp_value = "test_attrs_value_extra_" + to_string(i);

    if (i == 5) {
      tmp_value = "new_test_attrs_value_extra_5";
    }

    EXPECT_EQ(pair, make_pair(tmp_key, tmp_value));
    ++i;
  }

  /* Restore and check original attributes */
  nextObject->get_obj_attrs(null_yield, dpp, NULL);
  storeAttrs = nextObject->get_attrs();
  storeValues.clear();

  for (storeattrs = storeAttrs.begin(); storeattrs != storeAttrs.end(); ++storeattrs) {
    storeValues.push_back(make_pair(storeattrs->first, storeattrs->second.to_str()));
  }

  EXPECT_EQ((int)storeValues.size(), 10);

  i = 0;

  for (const auto& pair : storeValues) {
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    string tmp_value = "test_attrs_value_extra_" + to_string(i);

    EXPECT_EQ(pair, make_pair(tmp_key, tmp_value));
    ++i;
  }

  clientReset(&client);
}

TEST_F(D4NFilterFixture, StoreModifyAttrTest) {
  createUser();
  createBucket();
  putObject(19);
  unique_ptr<rgw::sal::Object> testObject_19 = testBucket->get_object(rgw_obj_key("test_object_19"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_19.get())->get_next();

  ASSERT_NE(nextObject, nullptr);

  /* Set one attribute */
  buffer::list bl_tmp;
  string tmp_value = "new_test_attrs_value_19";
  bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
  
  testObject_19->modify_obj_attrs("test_attrs_key_19", bl_tmp, null_yield, dpp);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Check the attribute */ 
  rgw::sal::Attrs storeAttrs = nextObject->get_attrs();
  pair<string, string> value(storeAttrs.begin()->first, storeAttrs.begin()->second.to_str());

  EXPECT_EQ(value, make_pair(string("test_attrs_key_19"), string("new_test_attrs_value_19")));
}

TEST_F(D4NFilterFixture, StoreDeleteAttrsTest) {
  createUser();
  createBucket();
  putObject(20);
  unique_ptr<rgw::sal::Object> testObject_20 = testBucket->get_object(rgw_obj_key("test_object_20"));

  /* Get the underlying store */
  static rgw::sal::Object* nextObject = dynamic_cast<rgw::sal::FilterObject*>(testObject_20.get())->get_next();

  ASSERT_NE(nextObject, nullptr);

  /* Set more attributes */
  map<string, bufferlist> test_attrs_base;

  for (int i = 0; i < 10; ++i) {
    buffer::list bl_tmp;
    string tmp_value = "test_attrs_value_extra_" + to_string(i);
    bl_tmp.append(tmp_value.data(), std::strlen(tmp_value.data()));
    
    string tmp_key = "test_attrs_key_extra_" + to_string(i);
    test_attrs_base.insert({tmp_key, bl_tmp});
  }

  testObject_20->set_obj_attrs(dpp, &test_attrs_base, NULL, null_yield);
  testObject_20->set_obj_attrs(dpp, NULL, &test_attrs_base, null_yield);

  ASSERT_EQ(nextObject->get_obj_attrs(null_yield, dpp, NULL), 0);

  /* Check the attributes */ 
  rgw::sal::Attrs storeAttrs = nextObject->get_attrs();

  EXPECT_EQ(storeAttrs.size(), (long unsigned int)1);

  pair<string, string> value(storeAttrs.begin()->first, storeAttrs.begin()->second.to_str());

  EXPECT_EQ(value, make_pair(string("test_attrs_key_20"), string("test_attrs_value_20")));
}

/* SAL object data storage check */
TEST_F(D4NFilterFixture, DataCheckTest) {
  cpp_redis::client client;
  clientSetUp(&client); 

  createUser();
  createBucket();
  
  /* Prepare, process, and complete object write */
  unique_ptr<rgw::sal::Object> head_obj = testBucket->get_object(rgw_obj_key("test_object_21"));
  rgw_user owner;
  rgw_placement_rule ptail_placement_rule;
  uint64_t olh_epoch = 123;
  string unique_tag;

  head_obj->get_obj_attrs(null_yield, dpp);

  testWriter = store->get_atomic_writer(dpp, 
	    null_yield,
	    head_obj->clone(),
	    owner,
	    &ptail_placement_rule,
	    olh_epoch,
	    unique_tag);

  size_t accounted_size = 4;
  string etag("test_etag");
  ceph::real_time mtime; 
  ceph::real_time set_mtime;

  buffer::list bl;
  string tmp = "test_attrs_value_21";
  bl.append("test_attrs_value_21");
  map<string, bufferlist> attrs{{"test_attrs_key_21", bl}};
  buffer::list data;
  data.append("test data");

  ceph::real_time delete_at;
  char if_match;
  char if_nomatch;
  string user_data;
  rgw_zone_set zones_trace;
  bool canceled;

  ASSERT_EQ(testWriter->prepare(null_yield), 0);
  
  ASSERT_EQ(testWriter->process(move(data), 0), 0);

  ASSERT_EQ(testWriter->complete(accounted_size, etag,
		 &mtime, set_mtime,
		 attrs,
		 delete_at,
		 &if_match, &if_nomatch,
		 &user_data,
		 &zones_trace, &canceled,
		 null_yield), 0);
 
  client.hget("rgw-object:test_object_21:cache", "data", [&data](cpp_redis::reply& reply) {
    if (reply.is_string()) {
      EXPECT_EQ(reply.as_string(), data.to_str());
    }
  });

  client.sync_commit();

  /* Change data and ensure redis stores the new value */
  buffer::list dataNew;
  dataNew.append("new test data");

  ASSERT_EQ(testWriter->prepare(null_yield), 0);
  
  ASSERT_EQ(testWriter->process(move(dataNew), 0), 0);

  ASSERT_EQ(testWriter->complete(accounted_size, etag,
		 &mtime, set_mtime,
		 attrs,
		 delete_at,
		 &if_match, &if_nomatch,
		 &user_data,
		 &zones_trace, &canceled,
		 null_yield), 0);

  client.hget("rgw-object:test_object_21:cache", "data", [&dataNew](cpp_redis::reply& reply) {
    if (reply.is_string()) {
      EXPECT_EQ(reply.as_string(), dataNew.to_str());
    }
  });

  client.sync_commit();
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
    cout << "Incorrect number of arguments." << std::endl;
    return -1;
  }

  redisHost = hostStr + ":" + portStr;

  env = new Environment();
  ::testing::AddGlobalTestEnvironment(env);

  return RUN_ALL_TESTS();
}
