#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include <algorithm>
#include <errno.h>
#include "gtest/gtest.h"
#include <string>

using namespace librados;
using std::string;

TEST(LibRadosSnapshots, SnapList) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  rados_snap_t snaps[10];
  ASSERT_EQ(1, rados_ioctx_snap_list(ioctx, snaps,
	sizeof(snaps) / sizeof(snaps[0])));
  rados_snap_t rid;
  ASSERT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
  ASSERT_EQ(rid, snaps[0]);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosSnapshots, SnapRemove) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  rados_snap_t rid;
  ASSERT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
  ASSERT_EQ(0, rados_ioctx_snap_remove(ioctx, "snap1"));
  ASSERT_EQ(-ENOENT, rados_ioctx_snap_lookup(ioctx, "snap1", &rid));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosSnapshots, Rollback) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snap1"));
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_write_full(ioctx, "foo", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, rados_rollback(ioctx, "foo", "snap1"));
  char buf3[sizeof(buf)];
  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf, buf3, sizeof(buf)));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosSnapshots, SnapGetName) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_ioctx_snap_create(ioctx, "snapfoo"));
  rados_snap_t rid;
  ASSERT_EQ(0, rados_ioctx_snap_lookup(ioctx, "snapfoo", &rid));
  ASSERT_EQ(-ENOENT, rados_ioctx_snap_lookup(ioctx, "snapbar", &rid));
  char name[128];
  memset(name, 0, sizeof(name));
  ASSERT_EQ(0, rados_ioctx_snap_get_name(ioctx, rid, name, sizeof(name)));
  time_t snaptime;
  ASSERT_EQ(0, rados_ioctx_snap_get_stamp(ioctx, rid, &snaptime));
  ASSERT_EQ(0, strcmp(name, "snapfoo"));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosSnapshots, SelfManagedSnapTest) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  uint64_t snapid;
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_create(ioctx, &snapid));
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_write_full(ioctx, "foo", buf2, sizeof(buf2), 0));
  //rados_ioctx_snap_set_read(ioctx, snapid);
  //ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_rollback(ioctx, "foo", snapid));
//  char buf3[sizeof(buf)];
//  memset(buf3, 0, sizeof(buf3));
//  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
//  ASSERT_EQ(0, memcmp(buf, buf3, sizeof(buf3)));
  ASSERT_EQ(0, rados_ioctx_selfmanaged_snap_remove(ioctx, snapid));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosSnapshots, SelfManagedSnapTestPP) {
  std::vector<uint64_t> my_snaps;
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl1, sizeof(buf), 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), ioctx.write("foo", bl2, sizeof(buf2), 0));

  ioctx.snap_set_read(my_snaps[1]);
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosSnapshots, SelfManagedSnapRollbackPP) {
  std::vector<uint64_t> my_snaps;
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl1, sizeof(buf), 0));

  my_snaps.push_back(-2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps.back()));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0], my_snaps));
  ::std::reverse(my_snaps.begin(), my_snaps.end());
  char buf2[sizeof(buf)];
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), ioctx.write("foo", bl2, sizeof(buf2), 0));

  string foo_str("foo");
  ioctx.selfmanaged_snap_rollback(foo_str, my_snaps[1]);
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, sizeof(buf)));

  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps.back()));
  my_snaps.pop_back();
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
