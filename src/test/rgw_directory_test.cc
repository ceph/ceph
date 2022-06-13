#include "rgw_directory.h"
#include <cpp_redis/cpp_redis>
#include <iostream>
#include <string>
#include "gtest/gtest.h"

using namespace std;

int main(int argc, char *argv[]) {
  RGWBlockDirectory* blk_dir = new RGWBlockDirectory();
  cache_block* c_blk = new cache_block();

  //string portStr(argv[1]);
  string portStr = "50104";
  string redisHost = "127.0.0.1:" + portStr;
  string oid = "sam-oid";
  c_blk->hosts_list.push_back("127.0.0.1:8000");
  c_blk->hosts_list.push_back(redisHost);
  c_blk->c_obj.obj_name = oid;

  cout << "Sam: setObjValue: " << blk_dir->setValue(c_blk) << std::endl;
  cout << "Sam: getObjValue: " << blk_dir->getValue(c_blk) << std::endl;

  delete c_blk;
  c_blk = nullptr;

  delete blk_dir;
  blk_dir = nullptr;

  return 0;
}
