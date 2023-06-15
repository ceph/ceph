#include "../rgw/driver/d4n/d4n_directory.h" // Fix -Sam
#include "rgw_process_env.h"
#include <cpp_redis/cpp_redis>
#include <iostream>
#include <string>
#include "gtest/gtest.h"

using namespace std;

string portStr;
string hostStr;
string redisHost = "";
string oid = "testName";
string bucketName = "testBucket";
int blockSize = 123;

class DirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      blockDir = new rgw::d4n::BlockDirectory(hostStr, stoi(portStr));
      cacheBlock = new rgw::d4n::CacheBlock();

      cacheBlock->hostsList.push_back(redisHost);
      cacheBlock->size = blockSize; 
      cacheBlock->cacheObj.bucketName = bucketName;
      cacheBlock->cacheObj.objName = oid;
    } 

    virtual void TearDown() {
      delete blockDir;
      blockDir = nullptr;

      delete cacheBlock;
      cacheBlock = nullptr;
    }

    rgw::d4n::BlockDirectory* blockDir;
    rgw::d4n::CacheBlock* cacheBlock;
};

/* Successful initialization */
TEST_F(DirectoryFixture, DirectoryInit) {
  ASSERT_NE(blockDir, nullptr);
  ASSERT_NE(cacheBlock, nullptr);
  ASSERT_NE(redisHost.length(), (long unsigned int)0);
}

/* Successful set_value Call and Redis Check */
TEST_F(DirectoryFixture, SetValueTest) {
  cpp_redis::client client;
  int key_exist = -1;
  string key;
  string hosts;
  string size;
  string bucketName;
  string objName;
  std::vector<std::string> fields;
  int setReturn = blockDir->set_value(cacheBlock);

  ASSERT_EQ(setReturn, 0);

  fields.push_back("key");
  fields.push_back("hosts");
  fields.push_back("size");
  fields.push_back("bucketName");
  fields.push_back("objName");

  client.connect(hostStr, stoi(portStr), nullptr, 0, 5, 1000);
  ASSERT_EQ((bool)client.is_connected(), (bool)1);

  client.hmget("rgw-object:" + oid + ":block-directory", fields, [&key, &hosts, &size, &bucketName, &objName, &key_exist](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      key_exist = 0;
      key = arr[0].as_string();
      hosts = arr[1].as_string();
      size = arr[2].as_string();
      bucketName = arr[3].as_string();
      objName = arr[4].as_string();
    }
  });

  client.sync_commit();

  EXPECT_EQ(key_exist, 0);
  EXPECT_EQ(key, "rgw-object:" + oid + ":block-directory");
  EXPECT_EQ(hosts, redisHost);
  EXPECT_EQ(size, to_string(blockSize));
  EXPECT_EQ(bucketName, bucketName);
  EXPECT_EQ(objName, oid);

  client.flushall();
}

/* Successful get_value Calls and Redis Check */
TEST_F(DirectoryFixture, GetValueTest) {
  cpp_redis::client client;
  int key_exist = -1;
  string key;
  string hosts;
  string size;
  string bucketName;
  string objName;
  std::vector<std::string> fields;
  int setReturn = blockDir->set_value(cacheBlock);

  ASSERT_EQ(setReturn, 0);

  fields.push_back("key");
  fields.push_back("hosts");
  fields.push_back("size");
  fields.push_back("bucketName");
  fields.push_back("objName");

  client.connect(hostStr, stoi(portStr), nullptr, 0, 5, 1000);
  ASSERT_EQ((bool)client.is_connected(), (bool)1);

  client.hmget("rgw-object:" + oid + ":block-directory", fields, [&key, &hosts, &size, &bucketName, &objName, &key_exist](cpp_redis::reply& reply) {
    auto arr = reply.as_array();

    if (!arr[0].is_null()) {
      key_exist = 0;
      key = arr[0].as_string();
      hosts = arr[1].as_string();
      size = arr[2].as_string();
      bucketName = arr[3].as_string();
      objName = arr[4].as_string();
    }
  });

  client.sync_commit();

  EXPECT_EQ(key_exist, 0);
  EXPECT_EQ(key, "rgw-object:" + oid + ":block-directory");
  EXPECT_EQ(hosts, redisHost);
  EXPECT_EQ(size, to_string(blockSize));
  EXPECT_EQ(bucketName, bucketName);
  EXPECT_EQ(objName, oid);

  /* Check if object name in directory instance matches redis update */
  client.hset("rgw-object:" + oid + ":block-directory", "objName", "newoid", [](cpp_redis::reply& reply) {
    if (!reply.is_null()) {
      ASSERT_EQ(reply.as_integer(), 0);
    }
  });

  client.sync_commit();

  int getReturn = blockDir->get_value(cacheBlock);

  ASSERT_EQ(getReturn, 0);
  EXPECT_EQ(cacheBlock->cacheObj.objName, "newoid");

  client.flushall();
}

/* Successful del_value Call and Redis Check */
TEST_F(DirectoryFixture, DelValueTest) {
  cpp_redis::client client;
  vector<string> keys;
  int setReturn = blockDir->set_value(cacheBlock);

  ASSERT_EQ(setReturn, 0);

  /* Ensure entry exists in directory before deletion */
  keys.push_back("rgw-object:" + oid + ":block-directory");

  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      ASSERT_EQ(reply.as_integer(), 1);
    }
  });

  int delReturn = blockDir->del_value(cacheBlock);

  ASSERT_EQ(delReturn, 0);

  client.exists(keys, [](cpp_redis::reply& reply) {
    if (reply.is_integer()) {
      ASSERT_EQ(reply.as_integer(), 0);  /* Zero keys exist */
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
