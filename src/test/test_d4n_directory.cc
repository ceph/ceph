#include "d4n_directory.h"
#include "rgw_process_env.h"
#include <cpp_redis/cpp_redis>
#include <iostream>
#include <string>
#include "gtest/gtest.h"

using namespace std;

string portStr;
string hostStr;
string redisHost = "";
string oid = "samoid";
string bucketName = "testBucket";
int blkSize = 123;

class DirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      blk_dir = new RGWBlockDirectory(hostStr, stoi(portStr));
      c_blk = new cache_block();

      c_blk->hosts_list.push_back(redisHost);
      c_blk->size_in_bytes = blkSize; 
      c_blk->c_obj.bucket_name = bucketName;
      c_blk->c_obj.obj_name = oid;
    } 

    virtual void TearDown() {
      delete blk_dir;
      blk_dir = nullptr;

      delete c_blk;
      c_blk = nullptr;
    }

    RGWBlockDirectory* blk_dir;
    cache_block* c_blk;
};

/* Successful initialization */
TEST_F(DirectoryFixture, DirectoryInit) {
  ASSERT_NE(blk_dir, nullptr);
  ASSERT_NE(c_blk, nullptr);
  ASSERT_NE(redisHost.length(), (long unsigned int)0);
}

/* Successful setValue Call and Redis Check */
TEST_F(DirectoryFixture, SetValueTest) {
  cpp_redis::client client;
  int key_exist = -1;
  string key;
  string hosts;
  string size;
  string bucket_name;
  string obj_name;
  std::vector<std::string> fields;
  int setReturn = blk_dir->setValue(c_blk);

  ASSERT_EQ(setReturn, 0);

  fields.push_back("key");
  fields.push_back("hosts");
  fields.push_back("size");
  fields.push_back("bucket_name");
  fields.push_back("obj_name");

  client.connect(hostStr, stoi(portStr), nullptr, 0, 5, 1000);
  ASSERT_EQ((bool)client.is_connected(), (bool)1);

  client.hmget("rgw-object:" + oid + ":directory", fields, [&key, &hosts, &size, &bucket_name, &obj_name, &key_exist](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      key_exist = 0;
      key = arr[0].as_string();
      hosts = arr[1].as_string();
      size = arr[2].as_string();
      bucket_name = arr[3].as_string();
      obj_name = arr[4].as_string();
    }
  });

  client.sync_commit();

  EXPECT_EQ(key_exist, 0);
  EXPECT_EQ(key, "rgw-object:" + oid + ":directory");
  EXPECT_EQ(hosts, redisHost);
  EXPECT_EQ(size, to_string(blkSize));
  EXPECT_EQ(bucket_name, bucketName);
  EXPECT_EQ(obj_name, oid);

  client.flushall();
}

/* Successful getValue Calls and Redis Check */
TEST_F(DirectoryFixture, GetValueTest) {
  cpp_redis::client client;
  int key_exist = -1;
  string key;
  string hosts;
  string size;
  string bucket_name;
  string obj_name;
  std::vector<std::string> fields;
  int setReturn = blk_dir->setValue(c_blk);

  ASSERT_EQ(setReturn, 0);

  fields.push_back("key");
  fields.push_back("hosts");
  fields.push_back("size");
  fields.push_back("bucket_name");
  fields.push_back("obj_name");

  client.connect(hostStr, stoi(portStr), nullptr, 0, 5, 1000);
  ASSERT_EQ((bool)client.is_connected(), (bool)1);

  client.hmget("rgw-object:" + oid + ":directory", fields, [&key, &hosts, &size, &bucket_name, &obj_name, &key_exist](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      key_exist = 0;
      key = arr[0].as_string();
      hosts = arr[1].as_string();
      size = arr[2].as_string();
      bucket_name = arr[3].as_string();
      obj_name = arr[4].as_string();
    }
  });

  client.sync_commit();

  EXPECT_EQ(key_exist, 0);
  EXPECT_EQ(key, "rgw-object:" + oid + ":directory");
  EXPECT_EQ(hosts, redisHost);
  EXPECT_EQ(size, to_string(blkSize));
  EXPECT_EQ(bucket_name, bucketName);
  EXPECT_EQ(obj_name, oid);

  /* Check if object name in directory instance matches redis update */
  client.hset("rgw-object:" + oid + ":directory", "obj_name", "newoid", [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      ASSERT_EQ(reply.as_integer(), 0); /* Zero keys exist */
    }
  });

  client.sync_commit();

  int getReturn = blk_dir->getValue(c_blk);

  ASSERT_EQ(getReturn, 0);
  EXPECT_EQ(c_blk->c_obj.obj_name, "newoid");

  client.flushall();
}

/* Successful delValue Call and Redis Check */
TEST_F(DirectoryFixture, DelValueTest) {
  cpp_redis::client client;
  vector<string> keys;
  int setReturn = blk_dir->setValue(c_blk);

  ASSERT_EQ(setReturn, 0);

  /* Ensure cache entry exists in cache before deletion */
  keys.push_back("rgw-object:" + oid + ":directory");

  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      ASSERT_EQ(reply.as_integer(), 1);
    }
  });

  int delReturn = blk_dir->delValue(c_blk);

  ASSERT_EQ(delReturn, 0);

  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      ASSERT_EQ(reply.as_integer(), 0); /* Zero keys exist */
    }
  });

  client.flushall();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  /* Other ports can be passed to the program */
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

  return RUN_ALL_TESTS();
}
