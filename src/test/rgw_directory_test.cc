#include "rgw_directory.h"
#include <cpp_redis/cpp_redis>
#include <iostream>
#include <string>
#include "gtest/gtest.h"

using namespace std;

string portStr;
string oid = "sam-oid";
string redisHost = "127.0.0.1:" + portStr;

class DirectoryFixture: public ::testing::Test {
  protected:
    virtual void SetUp() {
      blk_dir = new RGWBlockDirectory();
      c_blk = new cache_block();

      c_blk->hosts_list.push_back("127.0.0.1:8000");
      c_blk->hosts_list.push_back(redisHost);
      
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

TEST_F(DirectoryFixture, DirectoryInit) {
  ASSERT_NE(blk_dir, nullptr);
  ASSERT_NE(c_blk, nullptr);
  ASSERT_NE((int)portStr.length(), (int)0);
  ASSERT_EQ(c_blk->hosts_list[0], "127.0.0.1:8000");
  ASSERT_EQ(c_blk->hosts_list[1], redisHost);
}

TEST_F(DirectoryFixture, SetGetValueTest) {
  int setReturn = blk_dir->setValue(c_blk, stoi(portStr));
  int getReturn = blk_dir->getValue(c_blk, stoi(portStr));

  EXPECT_EQ(setReturn, 0);
  EXPECT_EQ(getReturn, 0);
}

TEST(DirectoryTest, RedisTest) {
  cpp_redis::client client;
  string host = "127.0.0.1";

  client.connect(host, stoi(portStr), nullptr, 0, 5, 1000);

  client.get(oid, [](cpp_redis::reply& reply) {
    EXPECT_EQ(reply.as_string(), oid);
  });

  ASSERT_EQ((bool)client.is_connected(), (bool)1);
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);

  if (argc == 1) 
    portStr = "6379";
  else
    portStr = argv[1];

  return RUN_ALL_TESTS();
}
