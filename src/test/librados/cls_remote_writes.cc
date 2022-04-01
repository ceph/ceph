#include <set>
#include <string>

#include "common/ceph_json.h"
#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

using namespace librados;

TEST(ClsTestRemoteWrites, TestScatter) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));

  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);

  bufferlist in, out;
  int object_size = 4096;
  char buf[object_size];
  memset(buf, 1, sizeof(buf));

  // create source object from which data are scattered
  in.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write_full("src_object", in));

  // construct JSON request passed to "test_scatter" method, and in turn, to "test_write" method
  JSONFormatter *formatter = new JSONFormatter(true);
  formatter->open_object_section("foo");
  std::set<std::string> tgt_objects;
  tgt_objects.insert("tgt_object.1");
  tgt_objects.insert("tgt_object.2");
  tgt_objects.insert("tgt_object.3");
  encode_json("tgt_objects", tgt_objects, formatter);
  encode_json("cls", "test_remote_operations", formatter);
  encode_json("method", "test_write", formatter);
  encode_json("pool", pool_name, formatter);
  formatter->close_section();
  in.clear();
  formatter->flush(in);

  // create target object by copying data from source object using "test_write" method
  ASSERT_EQ(0, ioctx.exec("src_object", "test_remote_operations", "test_scatter", in, out));

  // read target object and check its size
  ASSERT_EQ(object_size, ioctx.read("tgt_object.1", out, 0, 0));
  ASSERT_EQ(object_size, ioctx.read("tgt_object.2", out, 0, 0));
  ASSERT_EQ(object_size, ioctx.read("tgt_object.3", out, 0, 0));

  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
