#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include <errno.h>
#include "gtest/gtest.h"

///* snapshots */
//int rados_ioctx_snap_create(rados_ioctx_t io, const char *snapname);
//int rados_ioctx_snap_remove(rados_ioctx_t io, const char *snapname);
//int rados_rollback(rados_ioctx_t io, const char *oid,
//		   const char *snapname);
//void rados_ioctx_snap_set_read(rados_ioctx_t io, rados_snap_t snap);
//int rados_ioctx_selfmanaged_snap_create(rados_ioctx_t io, uint64_t *snapid);
//int rados_ioctx_selfmanaged_snap_remove(rados_ioctx_t io, uint64_t snapid);
//int rados_ioctx_selfmanaged_snap_rollback(rados_ioctx_t io, const char *oid, uint64_t snapid);
//int rados_ioctx_selfmanaged_snap_set_write_ctx(rados_ioctx_t io, rados_snap_t seq, rados_snap_t *snaps, int num_snaps);
//
//int rados_ioctx_snap_get_name(rados_ioctx_t io, rados_snap_t id, char *name, int maxlen);
//int rados_ioctx_snap_get_stamp(rados_ioctx_t io, rados_snap_t id, time_t *t);

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
