#include <set>
#include <string>

#include "common/ceph_json.h"
#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include "crimson_utils.h"

using namespace librados;

TEST(ClsTestRemoteReads, TestGather) {
  SKIP_IF_CRIMSON();
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  int src_objs = 300;
  int object_size = 4096;
  char buf[object_size];
  memset(buf, 1, sizeof(buf));

  // create src_objs source objects from which data are gathered
  for (int i = 0; i < src_objs; i++) {
    std::string oid = "src_object." + std::to_string(i);
    in.append(buf, sizeof(buf));
    ASSERT_EQ(0, ioctx.write_full(oid, in));
  }

  
  // construct JSON request passed to "test_gather" method, and in turn, to "test_read" method
  JSONFormatter *formatter = new JSONFormatter(true);
  formatter->open_object_section("foo");
  std::set<std::string> src_objects;
  for (int i = 0; i < src_objs; i++) {
    src_objects.insert("src_object." + std::to_string(i));
  }
  encode_json("src_objects", src_objects, formatter);
  encode_json("cls", "test_remote_reads", formatter);
  encode_json("method", "test_read", formatter);
  encode_json("pool", pool_name, formatter);
  formatter->close_section();
  in.clear();
  formatter->flush(in);

  // create target object by combining data gathered from source objects using "test_read" method
  ASSERT_EQ(0, ioctx.exec("tgt_object", "test_remote_reads", "test_gather", in, out));

  // read target object and check its size
  ASSERT_EQ(src_objs*object_size, ioctx.read("tgt_object", out, 0, 0));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
