// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/snap_types.h"
#include "include/encoding.h"
#include "include/rados.h"
#include "include/rados/librados.h"
#include "include/types.h"
#include "librbd/cls_rbd_client.h"

#include "gtest/gtest.h"
#include "test/rados-api/test.h"

#include <errno.h>
#include <string>
#include <vector>

using namespace std;
using ::librbd::cls_client::create_image;
using ::librbd::cls_client::get_features;
using ::librbd::cls_client::get_size;
using ::librbd::cls_client::get_object_prefix;
using ::librbd::cls_client::set_size;
using ::librbd::cls_client::snapshot_add;
using ::librbd::cls_client::snapshot_remove;
using ::librbd::cls_client::get_snapcontext;
using ::librbd::cls_client::snapshot_list;
using ::librbd::cls_client::old_snapshot_add;

TEST(cls_rbd, create)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "testobj";
  uint64_t size = 20 << 30;
  uint64_t features = 0;
  uint8_t order = 22;
  string object_prefix = "foo";

  ASSERT_EQ(0, create_image(&ioctx, oid, size, order,
			    features, object_prefix));
  ASSERT_EQ(-EEXIST, create_image(&ioctx, oid, size, order,
				  features, object_prefix));
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(-EINVAL, create_image(&ioctx, oid, size, order,
				  features, ""));
  ASSERT_EQ(-ENOENT, ioctx.remove(oid));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, order,
			    features, object_prefix));
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(-ENOSYS, create_image(&ioctx, oid, size, order,
				  -1, object_prefix));
  ASSERT_EQ(-ENOENT, ioctx.remove(oid));

  bufferlist inbl, outbl;
  ASSERT_EQ(-EINVAL, ioctx.exec(oid, "rbd", "create", inbl, outbl));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, get_features)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  uint64_t features;
  ASSERT_EQ(-ENOENT, get_features(&ioctx, "foo", CEPH_NOSNAP, &features));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 0, 22, 0, "foo"));
  ASSERT_EQ(0, get_features(&ioctx, "foo", CEPH_NOSNAP, &features));
  ASSERT_EQ(0u, features);

  ASSERT_EQ(-ENOENT, get_features(&ioctx, "foo", 1, &features));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, get_object_prefix)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string object_prefix;
  ASSERT_EQ(-ENOENT, get_object_prefix(&ioctx, "foo", &object_prefix));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 0, 22, 0, "foo"));
  ASSERT_EQ(0, get_object_prefix(&ioctx, "foo", &object_prefix));
  ASSERT_EQ("foo", object_prefix);

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, get_size)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  uint64_t size;
  uint8_t order;
  ASSERT_EQ(-ENOENT, get_size(&ioctx, "foo", CEPH_NOSNAP, &size, &order));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 0, 22, 0, "foo"));
  ASSERT_EQ(0, get_size(&ioctx, "foo", CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);
  ASSERT_EQ(0, ioctx.remove("foo"));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 2 << 22, 0, 0, "foo"));
  ASSERT_EQ(0, get_size(&ioctx, "foo", CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(2u << 22, size);
  ASSERT_EQ(0, order);

  ASSERT_EQ(-ENOENT, get_size(&ioctx, "foo", 1, &size, &order));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, set_size)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  ASSERT_EQ(-ENOENT, set_size(&ioctx, "foo", 5));

  uint64_t size;
  uint8_t order;
  ASSERT_EQ(0, create_image(&ioctx, "foo", 0, 22, 0, "foo"));
  ASSERT_EQ(0, get_size(&ioctx, "foo", CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);

  ASSERT_EQ(0, set_size(&ioctx, "foo", 0));
  ASSERT_EQ(0, get_size(&ioctx, "foo", CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);

  ASSERT_EQ(0, set_size(&ioctx, "foo", 3 << 22));
  ASSERT_EQ(0, get_size(&ioctx, "foo", CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(3u << 22, size);
  ASSERT_EQ(22, order);

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, snapshots)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  ASSERT_EQ(-ENOENT, snapshot_add(&ioctx, "foo", 0, "snap1"));

  ASSERT_EQ(0, create_image(&ioctx, "foo", 10, 22, 0, "foo"));

  vector<string> snap_names;
  vector<uint64_t> snap_sizes;
  vector<uint64_t> snap_features;
  SnapContext snapc;
  
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(0u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(0u, snap_names.size());
  ASSERT_EQ(0u, snap_sizes.size());
  ASSERT_EQ(0u, snap_features.size());

  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 0, "snap1"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap1", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0u, snap_features[0]);

  // snap with same id and name
  ASSERT_EQ(-EEXIST, snapshot_add(&ioctx, "foo", 0, "snap1"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap1", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0u, snap_features[0]);

  // snap with same id, different name
  ASSERT_EQ(-EEXIST, snapshot_add(&ioctx, "foo", 0, "snap2"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap1", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0u, snap_features[0]);

  // snap with different id, same name
  ASSERT_EQ(-EEXIST, snapshot_add(&ioctx, "foo", 1, "snap1"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(snap_names.size(), 1u);
  ASSERT_EQ(snap_names[0], "snap1");
  ASSERT_EQ(snap_sizes[0], 10u);
  ASSERT_EQ(snap_features[0], 0u);

  // snap with different id, different name
  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 1, "snap2"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(2u, snapc.snaps.size());
  ASSERT_EQ(1u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.snaps[1]);
  ASSERT_EQ(1u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(2u, snap_names.size());
  ASSERT_EQ("snap2", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0u, snap_features[0]);
  ASSERT_EQ("snap1", snap_names[1]);
  ASSERT_EQ(10u, snap_sizes[1]);
  ASSERT_EQ(0u, snap_features[1]);

  ASSERT_EQ(0, snapshot_remove(&ioctx, "foo", 0));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(1u, snapc.snaps[0]);
  ASSERT_EQ(1u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap2", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0u, snap_features[0]);

  uint64_t size;
  uint8_t order;
  ASSERT_EQ(0, set_size(&ioctx, "foo", 0));
  ASSERT_EQ(0, get_size(&ioctx, "foo", CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22u, order);

  uint64_t large_snap_id = 1ull << 63;
  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", large_snap_id, "snap3"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(2u, snapc.snaps.size());
  ASSERT_EQ(large_snap_id, snapc.snaps[0]);
  ASSERT_EQ(1u, snapc.snaps[1]);
  ASSERT_EQ(large_snap_id, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(2u, snap_names.size());
  ASSERT_EQ("snap3", snap_names[0]);
  ASSERT_EQ(0u, snap_sizes[0]);
  ASSERT_EQ(0u, snap_features[0]);
  ASSERT_EQ("snap2", snap_names[1]);
  ASSERT_EQ(10u, snap_sizes[1]);
  ASSERT_EQ(0u, snap_features[1]);

  ASSERT_EQ(0, get_size(&ioctx, "foo", large_snap_id, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22u, order);

  ASSERT_EQ(0, get_size(&ioctx, "foo", 1, &size, &order));
  ASSERT_EQ(10u, size);
  ASSERT_EQ(22u, order);

  ASSERT_EQ(0, snapshot_remove(&ioctx, "foo", large_snap_id));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(1u, snapc.snaps[0]);
  ASSERT_EQ(large_snap_id, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap2", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0u, snap_features[0]);

  ASSERT_EQ(-ENOENT, snapshot_remove(&ioctx, "foo", large_snap_id));
  ASSERT_EQ(0, snapshot_remove(&ioctx, "foo", 1));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(0u, snapc.snaps.size());
  ASSERT_EQ(large_snap_id, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features));
  ASSERT_EQ(0u, snap_names.size());
  ASSERT_EQ(0u, snap_sizes.size());
  ASSERT_EQ(0u, snap_features.size());

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, snapid_race)
{
   librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  buffer::list bl;
  buffer::ptr bp(4096);
  bp.zero();
  bl.append(bp);

  string oid = "foo";
  ASSERT_EQ(4096, ioctx.write(oid, bl, 4096, 0));
  ASSERT_EQ(0, old_snapshot_add(&ioctx, oid, 1, "test1"));
  ASSERT_EQ(0, old_snapshot_add(&ioctx, oid, 3, "test3"));
  ASSERT_EQ(-ESTALE, old_snapshot_add(&ioctx, oid, 2, "test2"));

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}
