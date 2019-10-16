// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/config.h"
#include "common/snap_types.h"
#include "common/Clock.h"
#include "common/bit_vector.hpp"
#include "include/encoding.h"
#include "include/types.h"
#include "include/rados/librados.h"
#include "include/rbd/object_map_types.h"
#include "include/rbd_types.h"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include <errno.h>
#include <string>
#include <vector>

using namespace std;
using namespace librbd::cls_client;
using ::librbd::ParentInfo;
using ::librbd::ParentSpec;
using ceph::encode;
using ceph::decode;

static int snapshot_add(librados::IoCtx *ioctx, const std::string &oid,
                        uint64_t snap_id, const std::string &snap_name) {
  librados::ObjectWriteOperation op;
  ::librbd::cls_client::snapshot_add(&op, snap_id, snap_name, cls::rbd::UserSnapshotNamespace());
  return ioctx->operate(oid, &op);
}

static int snapshot_remove(librados::IoCtx *ioctx, const std::string &oid,
                           uint64_t snap_id) {
  librados::ObjectWriteOperation op;
  ::librbd::cls_client::snapshot_remove(&op, snap_id);
  return ioctx->operate(oid, &op);
}

static int snapshot_rename(librados::IoCtx *ioctx, const std::string &oid,
                           uint64_t snap_id, const std::string &snap_name) {
  librados::ObjectWriteOperation op;
  ::librbd::cls_client::snapshot_rename(&op, snap_id, snap_name);
  return ioctx->operate(oid, &op);
}

static int old_snapshot_add(librados::IoCtx *ioctx, const std::string &oid,
                            uint64_t snap_id, const std::string &snap_name) {
  librados::ObjectWriteOperation op;
  ::librbd::cls_client::old_snapshot_add(&op, snap_id, snap_name);
  return ioctx->operate(oid, &op);
}

static char *random_buf(size_t len)
{
  char *b = new char[len];
  for (size_t i = 0; i < len; i++)
    b[i] = (rand() % (128 - 32)) + 32;
  return b;
}

class TestClsRbd : public ::testing::Test {
public:

  static void SetUpTestCase() {
    _pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(_pool_name, _rados));
  }

  static void TearDownTestCase() {
    ASSERT_EQ(0, destroy_one_pool_pp(_pool_name, _rados));
  }

  std::string get_temp_image_name() {
    ++_image_number;
    return "image" + stringify(_image_number);
  }

  static std::string _pool_name;
  static librados::Rados _rados;
  static uint64_t _image_number;

};

std::string TestClsRbd::_pool_name;
librados::Rados TestClsRbd::_rados;
uint64_t TestClsRbd::_image_number = 0;

TEST_F(TestClsRbd, get_all_features)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, ioctx.create(oid, false));

  uint64_t all_features = 0;
  ASSERT_EQ(0, get_all_features(&ioctx, oid, &all_features));
  ASSERT_EQ(static_cast<uint64_t>(RBD_FEATURES_ALL),
            static_cast<uint64_t>(all_features & RBD_FEATURES_ALL));

  ioctx.close();
}

TEST_F(TestClsRbd, copyup)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  bufferlist inbl, outbl;

  // copyup of 0-len nonexistent object should create new 0-len object
  ioctx.remove(oid);
  ASSERT_EQ(0, copyup(&ioctx, oid, inbl));
  uint64_t size;
  ASSERT_EQ(0, ioctx.stat(oid, &size, NULL));
  ASSERT_EQ(0U, size);

  // create some random data to write
  size_t l = 4 << 20;
  char *b = random_buf(l);
  inbl.append(b, l);
  delete [] b;
  ASSERT_EQ(l, inbl.length());

  // copyup to nonexistent object should create new object
  ioctx.remove(oid);
  ASSERT_EQ(-ENOENT, ioctx.remove(oid));
  ASSERT_EQ(0, copyup(&ioctx, oid, inbl));
  // and its contents should match
  ASSERT_EQ(l, (size_t)ioctx.read(oid, outbl, l, 0));
  ASSERT_TRUE(outbl.contents_equal(inbl));

  // now send different data, but with a preexisting object
  bufferlist inbl2;
  b = random_buf(l);
  inbl2.append(b, l);
  delete [] b;
  ASSERT_EQ(l, inbl2.length());

  // should still succeed
  ASSERT_EQ(0, copyup(&ioctx, oid, inbl));
  ASSERT_EQ(l, (size_t)ioctx.read(oid, outbl, l, 0));
  // but contents should not have changed
  ASSERT_FALSE(outbl.contents_equal(inbl2));
  ASSERT_TRUE(outbl.contents_equal(inbl));

  ASSERT_EQ(0, ioctx.remove(oid));
  ioctx.close();
}

TEST_F(TestClsRbd, get_and_set_id)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  string id;
  string valid_id = "0123abcxyzZYXCBA";
  string invalid_id = ".abc";
  string empty_id;

  ASSERT_EQ(-ENOENT, get_id(&ioctx, oid, &id));
  ASSERT_EQ(-ENOENT, set_id(&ioctx, oid, valid_id));

  ASSERT_EQ(0, ioctx.create(oid, true));
  ASSERT_EQ(-EINVAL, set_id(&ioctx, oid, invalid_id));
  ASSERT_EQ(-EINVAL, set_id(&ioctx, oid, empty_id));
  ASSERT_EQ(-ENOENT, get_id(&ioctx, oid, &id));

  ASSERT_EQ(0, set_id(&ioctx, oid, valid_id));
  ASSERT_EQ(-EEXIST, set_id(&ioctx, oid, valid_id));
  ASSERT_EQ(-EEXIST, set_id(&ioctx, oid, valid_id + valid_id));
  ASSERT_EQ(0, get_id(&ioctx, oid, &id));
  ASSERT_EQ(id, valid_id);

  ioctx.close();
}

TEST_F(TestClsRbd, add_remove_child)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, ioctx.create(oid, true));

  string snapname = "parent_snap";
  snapid_t snapid(10);
  string parent_image = "parent_id";
  set<string>children;
  ParentSpec pspec(ioctx.get_id(), parent_image, snapid);

  // nonexistent children cannot be listed or removed
  ASSERT_EQ(-ENOENT, get_children(&ioctx, oid, pspec, children));
  ASSERT_EQ(-ENOENT, remove_child(&ioctx, oid, pspec, "child1"));

  // create the parent and snapshot
  ASSERT_EQ(0, create_image(&ioctx, parent_image, 2<<20, 0,
			    RBD_FEATURE_LAYERING, parent_image, -1));
  ASSERT_EQ(0, snapshot_add(&ioctx, parent_image, snapid, snapname));

  // add child to it, verify it showed up
  ASSERT_EQ(0, add_child(&ioctx, oid, pspec, "child1"));
  ASSERT_EQ(0, get_children(&ioctx, oid, pspec, children));
  ASSERT_TRUE(children.find("child1") != children.end());
  // add another child to it, verify it showed up
  ASSERT_EQ(0, add_child(&ioctx, oid, pspec, "child2"));
  ASSERT_EQ(0, get_children(&ioctx, oid, pspec, children));
  ASSERT_TRUE(children.find("child2") != children.end());
  // add child2 again, expect -EEXIST
  ASSERT_EQ(-EEXIST, add_child(&ioctx, oid, pspec, "child2"));
  // remove first, verify it's gone
  ASSERT_EQ(0, remove_child(&ioctx, oid, pspec, "child1"));
  ASSERT_EQ(0, get_children(&ioctx, oid, pspec, children));
  ASSERT_FALSE(children.find("child1") != children.end());
  // remove second, verify list empty
  ASSERT_EQ(0, remove_child(&ioctx, oid, pspec, "child2"));
  ASSERT_EQ(-ENOENT, get_children(&ioctx, oid, pspec, children));
  // try to remove again, validate -ENOENT to that as well
  ASSERT_EQ(-ENOENT, remove_child(&ioctx, oid, pspec, "child2"));

  ioctx.close();
}

TEST_F(TestClsRbd, directory_methods)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  string id, name;
  string imgname = get_temp_image_name();
  string imgname2 = get_temp_image_name();
  string imgname3 = get_temp_image_name();
  string valid_id = "0123abcxyzZYXCBA";
  string valid_id2 = "5";
  string invalid_id = ".abc";
  string empty;

  ASSERT_EQ(-ENOENT, dir_get_id(&ioctx, oid, imgname, &id));
  ASSERT_EQ(-ENOENT, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(-ENOENT, dir_remove_image(&ioctx, oid, imgname, valid_id));

  ASSERT_EQ(-EINVAL, dir_add_image(&ioctx, oid, imgname, invalid_id));
  ASSERT_EQ(-EINVAL, dir_add_image(&ioctx, oid, imgname, empty));
  ASSERT_EQ(-EINVAL, dir_add_image(&ioctx, oid, empty, valid_id));

  map<string, string> images;
  ASSERT_EQ(-ENOENT, dir_list(&ioctx, oid, "", 30, &images));

  ASSERT_EQ(0, ioctx.create(oid, true));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(0u, images.size());
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(0, dir_add_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(-EEXIST, dir_add_image(&ioctx, oid, imgname, valid_id2));
  ASSERT_EQ(-EBADF, dir_add_image(&ioctx, oid, imgname2, valid_id));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(1u, images.size());
  ASSERT_EQ(valid_id, images[imgname]);
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 0, &images));
  ASSERT_EQ(0u, images.size());
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(imgname, name);
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname, &id));
  ASSERT_EQ(valid_id, id);

  ASSERT_EQ(0, dir_add_image(&ioctx, oid, imgname2, valid_id2));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(2u, images.size());
  ASSERT_EQ(valid_id, images[imgname]);
  ASSERT_EQ(valid_id2, images[imgname2]);
  ASSERT_EQ(0, dir_list(&ioctx, oid, imgname, 0, &images));
  ASSERT_EQ(0u, images.size());
  ASSERT_EQ(0, dir_list(&ioctx, oid, imgname, 2, &images));
  ASSERT_EQ(1u, images.size());
  ASSERT_EQ(valid_id2, images[imgname2]);
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id2, &name));
  ASSERT_EQ(imgname2, name);
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname2, &id));
  ASSERT_EQ(valid_id2, id);

  librados::ObjectWriteOperation op1;
  dir_rename_image(&op1, imgname, imgname2, valid_id2);
  ASSERT_EQ(-ESTALE, ioctx.operate(oid, &op1));
  ASSERT_EQ(-ESTALE, dir_remove_image(&ioctx, oid, imgname, valid_id2));
  librados::ObjectWriteOperation op2;
  dir_rename_image(&op2, imgname, imgname2, valid_id);
  ASSERT_EQ(-EEXIST, ioctx.operate(oid, &op2));
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname, &id));
  ASSERT_EQ(valid_id, id);
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id2, &name));
  ASSERT_EQ(imgname2, name);

  librados::ObjectWriteOperation op3;
  dir_rename_image(&op3, imgname, imgname3, valid_id);
  ASSERT_EQ(0, ioctx.operate(oid, &op3));
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname3, &id));
  ASSERT_EQ(valid_id, id);
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(imgname3, name);
  librados::ObjectWriteOperation op4;
  dir_rename_image(&op4, imgname3, imgname, valid_id);
  ASSERT_EQ(0, ioctx.operate(oid, &op4));

  ASSERT_EQ(0, dir_remove_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(1u, images.size());
  ASSERT_EQ(valid_id2, images[imgname2]);
  ASSERT_EQ(0, dir_list(&ioctx, oid, imgname2, 30, &images));
  ASSERT_EQ(0u, images.size());
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id2, &name));
  ASSERT_EQ(imgname2, name);
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname2, &id));
  ASSERT_EQ(valid_id2, id);
  ASSERT_EQ(-ENOENT, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(-ENOENT, dir_get_id(&ioctx, oid, imgname, &id));

  ASSERT_EQ(0, dir_add_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(2u, images.size());
  ASSERT_EQ(valid_id, images[imgname]);
  ASSERT_EQ(valid_id2, images[imgname2]);
  ASSERT_EQ(0, dir_remove_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(-ENOENT, dir_remove_image(&ioctx, oid, imgname, valid_id));
  ASSERT_EQ(0, dir_remove_image(&ioctx, oid, imgname2, valid_id2));
  ASSERT_EQ(0, dir_list(&ioctx, oid, "", 30, &images));
  ASSERT_EQ(0u, images.size());

  ioctx.close();
}

TEST_F(TestClsRbd, create)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  uint64_t size = 20ULL << 30;
  uint64_t features = 0;
  uint8_t order = 22;
  string object_prefix = oid;

  ASSERT_EQ(0, create_image(&ioctx, oid, size, order,
			    features, object_prefix, -1));
  ASSERT_EQ(-EEXIST, create_image(&ioctx, oid, size, order,
				  features, object_prefix, -1));
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(-EINVAL, create_image(&ioctx, oid, size, order,
				  features, "", -1));
  ASSERT_EQ(-ENOENT, ioctx.remove(oid));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, order,
			    features, object_prefix, -1));
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(-ENOSYS, create_image(&ioctx, oid, size, order,
				  -1, object_prefix, -1));
  ASSERT_EQ(-ENOENT, ioctx.remove(oid));

  ASSERT_EQ(0, create_image(&ioctx, oid, size, order, RBD_FEATURE_DATA_POOL,
                            object_prefix, 123));
  ASSERT_EQ(0, ioctx.remove(oid));
  ASSERT_EQ(-EINVAL, create_image(&ioctx, oid, size, order,
                                  RBD_FEATURE_OPERATIONS, object_prefix, -1));
  ASSERT_EQ(-EINVAL, create_image(&ioctx, oid, size, order,
                                  RBD_FEATURE_DATA_POOL, object_prefix, -1));
  ASSERT_EQ(-EINVAL, create_image(&ioctx, oid, size, order, 0, object_prefix,
                                  123));

  bufferlist inbl, outbl;
  ASSERT_EQ(-EINVAL, ioctx.exec(oid, "rbd", "create", inbl, outbl));

  ioctx.close();
}

TEST_F(TestClsRbd, get_features)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();

  uint64_t features;
  ASSERT_EQ(-ENOENT, get_features(&ioctx, oid, CEPH_NOSNAP, &features));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));
  ASSERT_EQ(0, get_features(&ioctx, oid, CEPH_NOSNAP, &features));
  ASSERT_EQ(0u, features);

  int r = get_features(&ioctx, oid, 1, &features);
  if (r == 0) {
    ASSERT_EQ(0u, features);
  } else {
    // deprecated snapshot handling
    ASSERT_EQ(-ENOENT, r);
  }

  ioctx.close();
}

TEST_F(TestClsRbd, get_object_prefix)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();

  string object_prefix;
  ASSERT_EQ(-ENOENT, get_object_prefix(&ioctx, oid, &object_prefix));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));
  ASSERT_EQ(0, get_object_prefix(&ioctx, oid, &object_prefix));
  ASSERT_EQ(oid, object_prefix);

  ioctx.close();
}

TEST_F(TestClsRbd, get_create_timestamp)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));

  utime_t timestamp;
  ASSERT_EQ(0, get_create_timestamp(&ioctx, oid, &timestamp));
  ASSERT_LT(0U, timestamp.tv.tv_sec);

  ioctx.close();
}

TEST_F(TestClsRbd, get_data_pool)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();

  int64_t data_pool_id;
  ASSERT_EQ(0, ioctx.create(oid, true));
  ASSERT_EQ(0, get_data_pool(&ioctx, oid, &data_pool_id));
  ASSERT_EQ(-1, data_pool_id);
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, RBD_FEATURE_DATA_POOL, oid,
                            12));
  ASSERT_EQ(0, get_data_pool(&ioctx, oid, &data_pool_id));
  ASSERT_EQ(12, data_pool_id);
}

TEST_F(TestClsRbd, get_size)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  uint64_t size;
  uint8_t order;
  ASSERT_EQ(-ENOENT, get_size(&ioctx, oid, CEPH_NOSNAP, &size, &order));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));
  ASSERT_EQ(0, get_size(&ioctx, oid, CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);
  ASSERT_EQ(0, ioctx.remove(oid));

  ASSERT_EQ(0, create_image(&ioctx, oid, 2 << 22, 0, 0, oid, -1));
  ASSERT_EQ(0, get_size(&ioctx, oid, CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(2u << 22, size);
  ASSERT_EQ(0, order);

  ASSERT_EQ(-ENOENT, get_size(&ioctx, oid, 1, &size, &order));

  ioctx.close();
}

TEST_F(TestClsRbd, set_size)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(-ENOENT, set_size(&ioctx, oid, 5));

  uint64_t size;
  uint8_t order;
  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));
  ASSERT_EQ(0, get_size(&ioctx, oid, CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);

  ASSERT_EQ(0, set_size(&ioctx, oid, 0));
  ASSERT_EQ(0, get_size(&ioctx, oid, CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22, order);

  ASSERT_EQ(0, set_size(&ioctx, oid, 3 << 22));
  ASSERT_EQ(0, get_size(&ioctx, oid, CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(3u << 22, size);
  ASSERT_EQ(22, order);

  ioctx.close();
}

TEST_F(TestClsRbd, protection_status)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  string oid2 = get_temp_image_name();
  uint8_t status = RBD_PROTECTION_STATUS_UNPROTECTED;
  ASSERT_EQ(-ENOENT, get_protection_status(&ioctx, oid,
					   CEPH_NOSNAP, &status));
  ASSERT_EQ(-ENOENT, set_protection_status(&ioctx, oid,
					   CEPH_NOSNAP, status));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, RBD_FEATURE_LAYERING, oid, -1));
  ASSERT_EQ(0, create_image(&ioctx, oid2, 0, 22, 0, oid, -1));
  ASSERT_EQ(-EINVAL, get_protection_status(&ioctx, oid2,
					   CEPH_NOSNAP, &status));
  ASSERT_EQ(-ENOEXEC, set_protection_status(&ioctx, oid2,
					   CEPH_NOSNAP, status));
  ASSERT_EQ(-EINVAL, get_protection_status(&ioctx, oid,
					   CEPH_NOSNAP, &status));
  ASSERT_EQ(-EINVAL, set_protection_status(&ioctx, oid,
					   CEPH_NOSNAP, status));
  ASSERT_EQ(-ENOENT, get_protection_status(&ioctx, oid,
					   2, &status));
  ASSERT_EQ(-ENOENT, set_protection_status(&ioctx, oid,
					   2, status));

  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 10, "snap1"));
  ASSERT_EQ(0, get_protection_status(&ioctx, oid,
				     10, &status));
  ASSERT_EQ(+RBD_PROTECTION_STATUS_UNPROTECTED, status);

  ASSERT_EQ(0, set_protection_status(&ioctx, oid,
				     10, RBD_PROTECTION_STATUS_PROTECTED));
  ASSERT_EQ(0, get_protection_status(&ioctx, oid,
				     10, &status));
  ASSERT_EQ(+RBD_PROTECTION_STATUS_PROTECTED, status);
  ASSERT_EQ(-EBUSY, snapshot_remove(&ioctx, oid, 10));

  ASSERT_EQ(0, set_protection_status(&ioctx, oid,
				     10, RBD_PROTECTION_STATUS_UNPROTECTING));
  ASSERT_EQ(0, get_protection_status(&ioctx, oid,
				     10, &status));
  ASSERT_EQ(+RBD_PROTECTION_STATUS_UNPROTECTING, status);
  ASSERT_EQ(-EBUSY, snapshot_remove(&ioctx, oid, 10));

  ASSERT_EQ(-EINVAL, set_protection_status(&ioctx, oid,
					   10, RBD_PROTECTION_STATUS_LAST));
  ASSERT_EQ(0, get_protection_status(&ioctx, oid,
				     10, &status));
  ASSERT_EQ(+RBD_PROTECTION_STATUS_UNPROTECTING, status);

  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 20, "snap2"));
  ASSERT_EQ(0, get_protection_status(&ioctx, oid,
				     20, &status));
  ASSERT_EQ(+RBD_PROTECTION_STATUS_UNPROTECTED, status);
  ASSERT_EQ(0, set_protection_status(&ioctx, oid,
				     10, RBD_PROTECTION_STATUS_UNPROTECTED));
  ASSERT_EQ(0, get_protection_status(&ioctx, oid,
				     10, &status));
  ASSERT_EQ(+RBD_PROTECTION_STATUS_UNPROTECTED, status);

  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 10));
  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 20));

  ioctx.close();
}

TEST_F(TestClsRbd, snapshot_limits)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  librados::ObjectWriteOperation op;
  string oid = get_temp_image_name();
  uint64_t limit;

  ASSERT_EQ(-ENOENT, snapshot_get_limit(&ioctx, oid, &limit));

  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, RBD_FEATURE_LAYERING, oid, -1));

  snapshot_set_limit(&op, 2);

  ASSERT_EQ(0, ioctx.operate(oid, &op));

  ASSERT_EQ(0, snapshot_get_limit(&ioctx, oid, &limit));
  ASSERT_EQ(2U, limit);

  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 10, "snap1"));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 20, "snap2"));
  ASSERT_EQ(-EDQUOT, snapshot_add(&ioctx, oid, 30, "snap3"));

  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 10));
  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 20));

  ioctx.close();
}

TEST_F(TestClsRbd, parents)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ParentSpec pspec;
  uint64_t size;

  ASSERT_EQ(-ENOENT, get_parent(&ioctx, "doesnotexist", CEPH_NOSNAP, &pspec, &size));

  // old image should fail
  ASSERT_EQ(0, create_image(&ioctx, "old", 33<<20, 22, 0, "old_blk.", -1));
  // get nonexistent parent: succeed, return (-1, "", CEPH_NOSNAP), overlap 0
  ASSERT_EQ(0, get_parent(&ioctx, "old", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, -1);
  ASSERT_STREQ("", pspec.image_id.c_str());
  ASSERT_EQ(pspec.snap_id, CEPH_NOSNAP);
  ASSERT_EQ(size, 0ULL);
  pspec = ParentSpec(-1, "parent", 3);
  ASSERT_EQ(-ENOEXEC, set_parent(&ioctx, "old", ParentSpec(-1, "parent", 3), 10<<20));
  ASSERT_EQ(-ENOEXEC, remove_parent(&ioctx, "old"));

  // new image will work
  ASSERT_EQ(0, create_image(&ioctx, oid, 33<<20, 22, RBD_FEATURE_LAYERING,
                            "foo.", -1));

  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);
  ASSERT_EQ(0, get_parent(&ioctx, oid, 123, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);

  ASSERT_EQ(-EINVAL, set_parent(&ioctx, oid, ParentSpec(-1, "parent", 3), 10<<20));
  ASSERT_EQ(-EINVAL, set_parent(&ioctx, oid, ParentSpec(1, "", 3), 10<<20));
  ASSERT_EQ(-EINVAL, set_parent(&ioctx, oid, ParentSpec(1, "parent", CEPH_NOSNAP), 10<<20));
  ASSERT_EQ(-EINVAL, set_parent(&ioctx, oid, ParentSpec(1, "parent", 3), 0));

  pspec = ParentSpec(1, "parent", 3);
  ASSERT_EQ(0, set_parent(&ioctx, oid, pspec, 10<<20));
  ASSERT_EQ(-EEXIST, set_parent(&ioctx, oid, pspec, 10<<20));
  ASSERT_EQ(-EEXIST, set_parent(&ioctx, oid, ParentSpec(2, "parent", 34), 10<<20));

  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));

  ASSERT_EQ(0, remove_parent(&ioctx, oid));
  ASSERT_EQ(-ENOENT, remove_parent(&ioctx, oid));
  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);

  // snapshots
  ASSERT_EQ(0, set_parent(&ioctx, oid, ParentSpec(1, "parent", 3), 10<<20));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 10, "snap1"));
  ASSERT_EQ(0, get_parent(&ioctx, oid, 10, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);

  ASSERT_EQ(0, remove_parent(&ioctx, oid));
  ASSERT_EQ(0, set_parent(&ioctx, oid, ParentSpec(1, "parent", 3), 5<<20));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 11, "snap2"));
  ASSERT_EQ(0, get_parent(&ioctx, oid, 10, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, oid, 11, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 5ull<<20);

  ASSERT_EQ(0, remove_parent(&ioctx, oid));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 12, "snap3"));
  ASSERT_EQ(0, get_parent(&ioctx, oid, 10, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, oid, 11, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 5ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, oid, 12, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);

  // make sure set_parent takes min of our size and parent's size
  ASSERT_EQ(0, set_parent(&ioctx, oid, ParentSpec(1, "parent", 3), 1<<20));
  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 1ull<<20);
  ASSERT_EQ(0, remove_parent(&ioctx, oid));

  ASSERT_EQ(0, set_parent(&ioctx, oid, ParentSpec(1, "parent", 3), 100<<20));
  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 33ull<<20);
  ASSERT_EQ(0, remove_parent(&ioctx, oid));

  // make sure resize adjust parent overlap
  ASSERT_EQ(0, set_parent(&ioctx, oid, ParentSpec(1, "parent", 3), 10<<20));

  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 14, "snap4"));
  ASSERT_EQ(0, set_size(&ioctx, oid, 3 << 20));
  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 3ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, oid, 14, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);

  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 15, "snap5"));
  ASSERT_EQ(0, set_size(&ioctx, oid, 30 << 20));
  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 3ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, oid, 14, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, oid, 15, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 3ull<<20);

  ASSERT_EQ(0, set_size(&ioctx, oid, 2 << 20));
  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 2ull<<20);

  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 16, "snap6"));
  ASSERT_EQ(0, get_parent(&ioctx, oid, 16, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 2ull<<20);

  ASSERT_EQ(0, ioctx.remove(oid));
  ASSERT_EQ(0, create_image(&ioctx, oid, 33<<20, 22,
                            RBD_FEATURE_LAYERING | RBD_FEATURE_DEEP_FLATTEN,
                            "foo.", -1));
  ASSERT_EQ(0, set_parent(&ioctx, oid, ParentSpec(1, "parent", 3), 100<<20));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 1, "snap1"));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 2, "snap2"));
  ASSERT_EQ(0, remove_parent(&ioctx, oid));

  ASSERT_EQ(0, get_parent(&ioctx, oid, 1, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);
  ASSERT_EQ(0, get_parent(&ioctx, oid, 2, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);
  ASSERT_EQ(0, get_parent(&ioctx, oid, CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);

  ioctx.close();
}

TEST_F(TestClsRbd, snapshots)
{
  cls::rbd::SnapshotNamespace userSnapNamespace = cls::rbd::UserSnapshotNamespace();
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(-ENOENT, snapshot_add(&ioctx, oid, 0, "snap1"));

  ASSERT_EQ(0, create_image(&ioctx, oid, 10, 22, 0, oid, -1));

  vector<cls::rbd::SnapshotInfo> snaps;
  vector<string> snap_names;
  vector<uint64_t> snap_sizes;
  SnapContext snapc;
  vector<ParentInfo> parents;
  vector<uint8_t> protection_status;
  vector<utime_t> snap_timestamps;

  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(0u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_get(&ioctx, oid, snapc.snaps, &snaps,
                            &parents, &protection_status));
  ASSERT_EQ(0u, snaps.size());
  ASSERT_EQ(0u, parents.size());
  ASSERT_EQ(0u, protection_status.size());
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(0u, snap_names.size());
  ASSERT_EQ(0u, snap_sizes.size());
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(0u, snap_timestamps.size());

  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 0, "snap1"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_get(&ioctx, oid, snapc.snaps, &snaps,
                            &parents, &protection_status));
  ASSERT_EQ(1u, snaps.size());
  ASSERT_EQ(1u, parents.size());
  ASSERT_EQ(1u, protection_status.size());
  ASSERT_EQ("snap1", snaps[0].name);
  ASSERT_EQ(userSnapNamespace, snaps[0].snapshot_namespace);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap1", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(1u, snap_timestamps.size());

  // snap with same id and name
  ASSERT_EQ(-EEXIST, snapshot_add(&ioctx, oid, 0, "snap1"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap1", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(1u, snap_timestamps.size());


  // snap with same id, different name
  ASSERT_EQ(-EEXIST, snapshot_add(&ioctx, oid, 0, "snap2"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap1", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(1u, snap_timestamps.size());

  // snap with different id, same name
  ASSERT_EQ(-EEXIST, snapshot_add(&ioctx, oid, 1, "snap1"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(snap_names.size(), 1u);
  ASSERT_EQ(snap_names[0], "snap1");
  ASSERT_EQ(snap_sizes[0], 10u);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(1u, snap_timestamps.size());

  // snap with different id, different name
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 1, "snap2"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(2u, snapc.snaps.size());
  ASSERT_EQ(1u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.snaps[1]);
  ASSERT_EQ(1u, snapc.seq);
  ASSERT_EQ(0, snapshot_get(&ioctx, oid, snapc.snaps, &snaps,
                            &parents, &protection_status));
  ASSERT_EQ(2u, snaps.size());
  ASSERT_EQ(2u, parents.size());
  ASSERT_EQ(2u, protection_status.size());
  ASSERT_EQ("snap2", snaps[0].name);
  ASSERT_EQ("snap1", snaps[1].name);
  ASSERT_EQ(userSnapNamespace, snaps[0].snapshot_namespace);
  ASSERT_EQ(userSnapNamespace, snaps[1].snapshot_namespace);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(2u, snap_names.size());
  ASSERT_EQ("snap2", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ("snap1", snap_names[1]);
  ASSERT_EQ(10u, snap_sizes[1]);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(2u, snap_timestamps.size());

  ASSERT_EQ(0, snapshot_rename(&ioctx, oid, 0, "snap1-rename"));
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(2u, snap_names.size());
  ASSERT_EQ("snap2", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ("snap1-rename", snap_names[1]);
  ASSERT_EQ(10u, snap_sizes[1]);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(2u, snap_timestamps.size());

  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 0));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(1u, snapc.snaps[0]);
  ASSERT_EQ(1u, snapc.seq);
  ASSERT_EQ(0, snapshot_get(&ioctx, oid, snapc.snaps, &snaps,
                            &parents, &protection_status));
  ASSERT_EQ(1u, snaps.size());
  ASSERT_EQ(1u, parents.size());
  ASSERT_EQ(1u, protection_status.size());
  ASSERT_EQ("snap2", snaps[0].name);
  ASSERT_EQ(userSnapNamespace, snaps[0].snapshot_namespace);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap2", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(1u, snap_timestamps.size());

  uint64_t size;
  uint8_t order;
  ASSERT_EQ(0, set_size(&ioctx, oid, 0));
  ASSERT_EQ(0, get_size(&ioctx, oid, CEPH_NOSNAP, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22u, order);

  uint64_t large_snap_id = 1ull << 63;
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, large_snap_id, "snap3"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(2u, snapc.snaps.size());
  ASSERT_EQ(large_snap_id, snapc.snaps[0]);
  ASSERT_EQ(1u, snapc.snaps[1]);
  ASSERT_EQ(large_snap_id, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(2u, snap_names.size());
  ASSERT_EQ("snap3", snap_names[0]);
  ASSERT_EQ(0u, snap_sizes[0]);
  ASSERT_EQ("snap2", snap_names[1]);
  ASSERT_EQ(10u, snap_sizes[1]);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(2u, snap_timestamps.size());

  ASSERT_EQ(0, get_size(&ioctx, oid, large_snap_id, &size, &order));
  ASSERT_EQ(0u, size);
  ASSERT_EQ(22u, order);

  ASSERT_EQ(0, get_size(&ioctx, oid, 1, &size, &order));
  ASSERT_EQ(10u, size);
  ASSERT_EQ(22u, order);

  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, large_snap_id));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(1u, snapc.snaps[0]);
  ASSERT_EQ(large_snap_id, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(1u, snap_names.size());
  ASSERT_EQ("snap2", snap_names[0]);
  ASSERT_EQ(10u, snap_sizes[0]);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(1u, snap_timestamps.size());

  ASSERT_EQ(-ENOENT, snapshot_remove(&ioctx, oid, large_snap_id));
  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 1));
  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(0u, snapc.snaps.size());
  ASSERT_EQ(large_snap_id, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, oid, snapc.snaps, &snap_names,
			     &snap_sizes, &parents, &protection_status));
  ASSERT_EQ(0u, snap_names.size());
  ASSERT_EQ(0u, snap_sizes.size());
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(0u, snap_timestamps.size());

  ioctx.close();
}

TEST_F(TestClsRbd, snapshots_timestamps)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();

  ASSERT_EQ(0, create_image(&ioctx, oid, 10, 22, 0, oid, -1));

  vector<utime_t> snap_timestamps;
  SnapContext snapc;

  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(0u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_EQ(0u, snap_timestamps.size());

  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 0, "snap1"));

  ASSERT_EQ(0, get_snapcontext(&ioctx, oid, &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0, snapshot_timestamp_list(&ioctx, oid, snapc.snaps, &snap_timestamps));
  ASSERT_LT(0U, snap_timestamps[0].tv.tv_sec);
  ioctx.close();
}

TEST_F(TestClsRbd, snapid_race)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  buffer::list bl;
  buffer::ptr bp(4096);
  bp.zero();
  bl.append(bp);

  string oid = get_temp_image_name();
  ASSERT_EQ(0, ioctx.write(oid, bl, 4096, 0));
  ASSERT_EQ(0, old_snapshot_add(&ioctx, oid, 1, "test1"));
  ASSERT_EQ(0, old_snapshot_add(&ioctx, oid, 3, "test3"));
  ASSERT_EQ(-ESTALE, old_snapshot_add(&ioctx, oid, 2, "test2"));

  ioctx.close();
}

TEST_F(TestClsRbd, stripingv2)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  string oid2 = get_temp_image_name();
  ASSERT_EQ(0, create_image(&ioctx, oid, 10, 22, 0, oid, -1));

  uint64_t su = 65536, sc = 12;
  ASSERT_EQ(-ENOEXEC, get_stripe_unit_count(&ioctx, oid, &su, &sc));
  ASSERT_EQ(-ENOEXEC, set_stripe_unit_count(&ioctx, oid, su, sc));

  ASSERT_EQ(0, create_image(&ioctx, oid2, 10, 22, RBD_FEATURE_STRIPINGV2,
                            oid2, -1));
  ASSERT_EQ(0, get_stripe_unit_count(&ioctx, oid2, &su, &sc));
  ASSERT_EQ(1ull << 22, su);
  ASSERT_EQ(1ull, sc);
  su = 8192;
  sc = 456;
  ASSERT_EQ(0, set_stripe_unit_count(&ioctx, oid2, su, sc));
  su = sc = 0;
  ASSERT_EQ(0, get_stripe_unit_count(&ioctx, oid2, &su, &sc));
  ASSERT_EQ(8192ull, su);
  ASSERT_EQ(456ull, sc);

  // su must not be larger than an object
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, oid2, 1 << 23, 1));
  // su must be a factor of object size
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, oid2, 511, 1));
  // su and sc must be non-zero
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, oid2, 0, 1));
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, oid2, 1, 0));
  ASSERT_EQ(-EINVAL, set_stripe_unit_count(&ioctx, oid2, 0, 0));

  ioctx.close();
}

TEST_F(TestClsRbd, get_mutable_metadata_features)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, create_image(&ioctx, oid, 10, 22, RBD_FEATURE_EXCLUSIVE_LOCK,
                            oid, -1));

  uint64_t size, features, incompatible_features;
  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  bool exclusive_lock;
  std::string lock_tag;
  ::SnapContext snapc;
  ParentInfo parent;

  ASSERT_EQ(0, get_mutable_metadata(&ioctx, oid, true, &size, &features,
				    &incompatible_features, &lockers,
				    &exclusive_lock, &lock_tag, &snapc,
                                    &parent));
  ASSERT_EQ(static_cast<uint64_t>(RBD_FEATURE_EXCLUSIVE_LOCK), features);
  ASSERT_EQ(0U, incompatible_features);

  ASSERT_EQ(0, get_mutable_metadata(&ioctx, oid, false, &size, &features,
                                    &incompatible_features, &lockers,
                                    &exclusive_lock, &lock_tag, &snapc,
				    &parent));
  ASSERT_EQ(static_cast<uint64_t>(RBD_FEATURE_EXCLUSIVE_LOCK), features);
  ASSERT_EQ(static_cast<uint64_t>(RBD_FEATURE_EXCLUSIVE_LOCK),
	    incompatible_features);

  ioctx.close();
}

TEST_F(TestClsRbd, object_map_save)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  BitVector<2> ref_bit_vector;
  ref_bit_vector.resize(32);
  for (uint64_t i = 0; i < ref_bit_vector.size(); ++i) {
    ref_bit_vector[i] = 1;
  }

  librados::ObjectWriteOperation op;
  object_map_save(&op, ref_bit_vector);
  ASSERT_EQ(0, ioctx.operate(oid, &op));

  BitVector<2> osd_bit_vector;
  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);
}

TEST_F(TestClsRbd, object_map_resize)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  BitVector<2> ref_bit_vector;
  ref_bit_vector.resize(32);
  for (uint64_t i = 0; i < ref_bit_vector.size(); ++i) {
    ref_bit_vector[i] = 1;
  }

  librados::ObjectWriteOperation op1;
  object_map_resize(&op1, ref_bit_vector.size(), 1);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  BitVector<2> osd_bit_vector;
  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);

  ref_bit_vector.resize(64);
  for (uint64_t i = 32; i < ref_bit_vector.size(); ++i) {
    ref_bit_vector[i] = 2;
  }

  librados::ObjectWriteOperation op2;
  object_map_resize(&op2, ref_bit_vector.size(), 2);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));
  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);

  ref_bit_vector.resize(32);

  librados::ObjectWriteOperation op3;
  object_map_resize(&op3, ref_bit_vector.size(), 1);
  ASSERT_EQ(-ESTALE, ioctx.operate(oid, &op3));

  librados::ObjectWriteOperation op4;
  object_map_resize(&op4, ref_bit_vector.size(), 2);
  ASSERT_EQ(0, ioctx.operate(oid, &op4));

  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);

  ioctx.close();
}

TEST_F(TestClsRbd, object_map_update)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  BitVector<2> ref_bit_vector;
  ref_bit_vector.resize(16);
  for (uint64_t i = 0; i < ref_bit_vector.size(); ++i) {
    ref_bit_vector[i] = 2;
  }

  BitVector<2> osd_bit_vector;

  librados::ObjectWriteOperation op1;
  object_map_resize(&op1, ref_bit_vector.size(), 2);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));
  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);

  ref_bit_vector[7] = 1;
  ref_bit_vector[8] = 1;

  librados::ObjectWriteOperation op2;
  object_map_update(&op2, 7, 9, 1, boost::optional<uint8_t>());
  ASSERT_EQ(0, ioctx.operate(oid, &op2));
  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);

  ref_bit_vector[7] = 3;
  ref_bit_vector[8] = 3;

  librados::ObjectWriteOperation op3;
  object_map_update(&op3, 6, 10, 3, 1);
  ASSERT_EQ(0, ioctx.operate(oid, &op3));
  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);

  ioctx.close();
}

TEST_F(TestClsRbd, object_map_load_enoent)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  BitVector<2> osd_bit_vector;
  ASSERT_EQ(-ENOENT, object_map_load(&ioctx, oid, &osd_bit_vector));

  ioctx.close();
}

TEST_F(TestClsRbd, object_map_snap_add)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  BitVector<2> ref_bit_vector;
  ref_bit_vector.resize(16);
  for (uint64_t i = 0; i < ref_bit_vector.size(); ++i) {
    if (i < 4) {
      ref_bit_vector[i] = OBJECT_NONEXISTENT;
    } else {
      ref_bit_vector[i] = OBJECT_EXISTS;
    }
  }

  BitVector<2> osd_bit_vector;

  librados::ObjectWriteOperation op1;
  object_map_resize(&op1, ref_bit_vector.size(), OBJECT_EXISTS);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  librados::ObjectWriteOperation op2;
  object_map_update(&op2, 0, 4, OBJECT_NONEXISTENT, boost::optional<uint8_t>());
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);

  librados::ObjectWriteOperation op3;
  object_map_snap_add(&op3);
  ASSERT_EQ(0, ioctx.operate(oid, &op3));

  for (uint64_t i = 0; i < ref_bit_vector.size(); ++i) {
    if (ref_bit_vector[i] == OBJECT_EXISTS) {
      ref_bit_vector[i] = OBJECT_EXISTS_CLEAN;
    }
  }

  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);
}

TEST_F(TestClsRbd, object_map_snap_remove)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  BitVector<2> ref_bit_vector;
  ref_bit_vector.resize(16);
  for (uint64_t i = 0; i < ref_bit_vector.size(); ++i) {
    if (i < 4) {
      ref_bit_vector[i] = OBJECT_EXISTS_CLEAN;
    } else {
      ref_bit_vector[i] = OBJECT_EXISTS;
    }
  }

  BitVector<2> osd_bit_vector;

  librados::ObjectWriteOperation op1;
  object_map_resize(&op1, ref_bit_vector.size(), OBJECT_EXISTS);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));

  librados::ObjectWriteOperation op2;
  object_map_update(&op2, 0, 4, OBJECT_EXISTS_CLEAN, boost::optional<uint8_t>());
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);

  BitVector<2> snap_bit_vector;
  snap_bit_vector.resize(4);
  for (uint64_t i = 0; i < snap_bit_vector.size(); ++i) {
    if (i == 1 || i == 2) {
      snap_bit_vector[i] = OBJECT_EXISTS;
    } else {
      snap_bit_vector[i] = OBJECT_NONEXISTENT;
    }
  }

  librados::ObjectWriteOperation op3;
  object_map_snap_remove(&op3, snap_bit_vector);
  ASSERT_EQ(0, ioctx.operate(oid, &op3));

  ref_bit_vector[1] = OBJECT_EXISTS;
  ref_bit_vector[2] = OBJECT_EXISTS;
  ASSERT_EQ(0, object_map_load(&ioctx, oid, &osd_bit_vector));
  ASSERT_EQ(ref_bit_vector, osd_bit_vector);
}

TEST_F(TestClsRbd, flags)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));

  uint64_t flags;
  std::vector<snapid_t> snap_ids;
  std::vector<uint64_t> snap_flags;
  ASSERT_EQ(0, get_flags(&ioctx, oid, &flags, snap_ids, &snap_flags));
  ASSERT_EQ(0U, flags);

  librados::ObjectWriteOperation op1;
  set_flags(&op1, CEPH_NOSNAP, 3, 2);
  ASSERT_EQ(0, ioctx.operate(oid, &op1));
  ASSERT_EQ(0, get_flags(&ioctx, oid, &flags, snap_ids, &snap_flags));
  ASSERT_EQ(2U, flags);

  uint64_t snap_id = 10;
  snap_ids.push_back(snap_id);
  ASSERT_EQ(-ENOENT, get_flags(&ioctx, oid, &flags, snap_ids, &snap_flags));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, snap_id, "snap"));

  librados::ObjectWriteOperation op2;
  set_flags(&op2, snap_id, 31, 4);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));
  ASSERT_EQ(0, get_flags(&ioctx, oid, &flags, snap_ids, &snap_flags));
  ASSERT_EQ(2U, flags);
  ASSERT_EQ(snap_ids.size(), snap_flags.size());
  ASSERT_EQ(6U, snap_flags[0]);

  ioctx.close();
}

TEST_F(TestClsRbd, metadata)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));

  map<string, bufferlist> pairs;
  string value;
  ASSERT_EQ(0, metadata_list(&ioctx, oid, "", 0, &pairs));
  ASSERT_TRUE(pairs.empty());

  pairs["key1"].append("value1");
  pairs["key2"].append("value2");
  ASSERT_EQ(0, metadata_set(&ioctx, oid, pairs));
  ASSERT_EQ(0, metadata_get(&ioctx, oid, "key1", &value));
  ASSERT_EQ(0, strcmp("value1", value.c_str()));
  pairs.clear();
  ASSERT_EQ(0, metadata_list(&ioctx, oid, "", 0, &pairs));
  ASSERT_EQ(2U, pairs.size());
  ASSERT_EQ(0, strncmp("value1", pairs["key1"].c_str(), 6));
  ASSERT_EQ(0, strncmp("value2", pairs["key2"].c_str(), 6));

  pairs.clear();
  ASSERT_EQ(0, metadata_remove(&ioctx, oid, "key1"));
  ASSERT_EQ(0, metadata_remove(&ioctx, oid, "key3"));
  ASSERT_TRUE(metadata_get(&ioctx, oid, "key1", &value) < 0);
  ASSERT_EQ(0, metadata_list(&ioctx, oid, "", 0, &pairs));
  ASSERT_EQ(1U, pairs.size());
  ASSERT_EQ(0, strncmp("value2", pairs["key2"].c_str(), 6));

  pairs.clear();
  char key[10], val[20];
  for (int i = 0; i < 1024; i++) {
    sprintf(key, "key%d", i);
    sprintf(val, "value%d", i);
    pairs[key].append(val, strlen(val));
  }
  ASSERT_EQ(0, metadata_set(&ioctx, oid, pairs));

  string last_read = "";
  uint64_t max_read = 48, r;
  uint64_t size = 0;
  map<string, bufferlist> data;
  do {
    map<string, bufferlist> cur;
    metadata_list(&ioctx, oid, last_read, max_read, &cur);
    size += cur.size();
    for (map<string, bufferlist>::iterator it = cur.begin();
         it != cur.end(); ++it)
      data[it->first] = it->second;
    last_read = cur.rbegin()->first;
    r = cur.size();
  } while (r == max_read);
  ASSERT_EQ(size, 1024U);
  for (map<string, bufferlist>::iterator it = data.begin();
       it != data.end(); ++it) {
    ASSERT_TRUE(it->second.contents_equal(pairs[it->first]));
  }

  ioctx.close();
}

TEST_F(TestClsRbd, set_features)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  uint64_t base_features = RBD_FEATURE_LAYERING | RBD_FEATURE_DEEP_FLATTEN;
  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, base_features, oid, -1));

  uint64_t features = RBD_FEATURES_MUTABLE;
  uint64_t mask = RBD_FEATURES_MUTABLE;
  ASSERT_EQ(0, set_features(&ioctx, oid, features, mask));

  uint64_t actual_features;
  ASSERT_EQ(0, get_features(&ioctx, oid, CEPH_NOSNAP, &actual_features));

  uint64_t expected_features = RBD_FEATURES_MUTABLE | base_features;
  ASSERT_EQ(expected_features, actual_features);

  features = 0;
  mask = RBD_FEATURE_OBJECT_MAP;
  ASSERT_EQ(0, set_features(&ioctx, oid, features, mask));

  ASSERT_EQ(0, get_features(&ioctx, oid, CEPH_NOSNAP, &actual_features));

  expected_features = (RBD_FEATURES_MUTABLE | base_features) &
                      ~RBD_FEATURE_OBJECT_MAP;
  ASSERT_EQ(expected_features, actual_features);

  ASSERT_EQ(0, set_features(&ioctx, oid, 0, RBD_FEATURE_DEEP_FLATTEN));
  ASSERT_EQ(-EINVAL, set_features(&ioctx, oid, RBD_FEATURE_DEEP_FLATTEN,
                                  RBD_FEATURE_DEEP_FLATTEN));

  ASSERT_EQ(-EINVAL, set_features(&ioctx, oid, 0, RBD_FEATURE_LAYERING));
  ASSERT_EQ(-EINVAL, set_features(&ioctx, oid, 0, RBD_FEATURE_OPERATIONS));
}

TEST_F(TestClsRbd, mirror) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_MIRRORING);

  std::vector<cls::rbd::MirrorPeer> peers;
  ASSERT_EQ(-ENOENT, mirror_peer_list(&ioctx, &peers));

  std::string uuid;
  ASSERT_EQ(-ENOENT, mirror_uuid_get(&ioctx, &uuid));
  ASSERT_EQ(-EINVAL, mirror_peer_add(&ioctx, "uuid1", "cluster1", "client"));

  cls::rbd::MirrorMode mirror_mode;
  ASSERT_EQ(0, mirror_mode_get(&ioctx, &mirror_mode));
  ASSERT_EQ(cls::rbd::MIRROR_MODE_DISABLED, mirror_mode);

  ASSERT_EQ(-EINVAL, mirror_mode_set(&ioctx, cls::rbd::MIRROR_MODE_IMAGE));
  ASSERT_EQ(-EINVAL, mirror_uuid_set(&ioctx, ""));
  ASSERT_EQ(0, mirror_uuid_set(&ioctx, "mirror-uuid"));
  ASSERT_EQ(0, mirror_uuid_get(&ioctx, &uuid));
  ASSERT_EQ("mirror-uuid", uuid);

  ASSERT_EQ(0, mirror_mode_set(&ioctx, cls::rbd::MIRROR_MODE_IMAGE));
  ASSERT_EQ(0, mirror_mode_get(&ioctx, &mirror_mode));
  ASSERT_EQ(cls::rbd::MIRROR_MODE_IMAGE, mirror_mode);

  ASSERT_EQ(-EINVAL, mirror_uuid_set(&ioctx, "new-mirror-uuid"));

  ASSERT_EQ(0, mirror_mode_set(&ioctx, cls::rbd::MIRROR_MODE_POOL));
  ASSERT_EQ(0, mirror_mode_get(&ioctx, &mirror_mode));
  ASSERT_EQ(cls::rbd::MIRROR_MODE_POOL, mirror_mode);

  ASSERT_EQ(-EINVAL, mirror_peer_add(&ioctx, "mirror-uuid", "cluster1", "client"));
  ASSERT_EQ(0, mirror_peer_add(&ioctx, "uuid1", "cluster1", "client"));
  ASSERT_EQ(0, mirror_peer_add(&ioctx, "uuid2", "cluster2", "admin"));
  ASSERT_EQ(-ESTALE, mirror_peer_add(&ioctx, "uuid2", "cluster3", "foo"));
  ASSERT_EQ(-EEXIST, mirror_peer_add(&ioctx, "uuid3", "cluster1", "foo"));
  ASSERT_EQ(0, mirror_peer_add(&ioctx, "uuid3", "cluster3", "admin"));
  ASSERT_EQ(-EEXIST, mirror_peer_add(&ioctx, "uuid4", "cluster3", "admin"));

  ASSERT_EQ(0, mirror_peer_list(&ioctx, &peers));
  std::vector<cls::rbd::MirrorPeer> expected_peers = {
    {"uuid1", "cluster1", "client", -1},
    {"uuid2", "cluster2", "admin", -1},
    {"uuid3", "cluster3", "admin", -1}};
  ASSERT_EQ(expected_peers, peers);

  ASSERT_EQ(0, mirror_peer_remove(&ioctx, "uuid5"));
  ASSERT_EQ(0, mirror_peer_remove(&ioctx, "uuid2"));

  ASSERT_EQ(-ENOENT, mirror_peer_set_client(&ioctx, "uuid4", "new client"));
  ASSERT_EQ(0, mirror_peer_set_client(&ioctx, "uuid1", "new client"));

  ASSERT_EQ(-ENOENT, mirror_peer_set_cluster(&ioctx, "uuid4", "new cluster"));
  ASSERT_EQ(0, mirror_peer_set_cluster(&ioctx, "uuid3", "new cluster"));

  ASSERT_EQ(0, mirror_peer_list(&ioctx, &peers));
  expected_peers = {
    {"uuid1", "cluster1", "new client", -1},
    {"uuid3", "new cluster", "admin", -1}};
  ASSERT_EQ(expected_peers, peers);
  ASSERT_EQ(-EBUSY, mirror_mode_set(&ioctx, cls::rbd::MIRROR_MODE_DISABLED));

  ASSERT_EQ(0, mirror_peer_remove(&ioctx, "uuid3"));
  ASSERT_EQ(0, mirror_peer_remove(&ioctx, "uuid1"));
  ASSERT_EQ(0, mirror_peer_list(&ioctx, &peers));
  expected_peers = {};
  ASSERT_EQ(expected_peers, peers);

  ASSERT_EQ(0, mirror_mode_set(&ioctx, cls::rbd::MIRROR_MODE_DISABLED));
  ASSERT_EQ(0, mirror_mode_get(&ioctx, &mirror_mode));
  ASSERT_EQ(cls::rbd::MIRROR_MODE_DISABLED, mirror_mode);
  ASSERT_EQ(-ENOENT, mirror_uuid_get(&ioctx, &uuid));
}

TEST_F(TestClsRbd, mirror_image) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_MIRRORING);

  std::map<std::string, std::string> mirror_image_ids;
  ASSERT_EQ(-ENOENT, mirror_image_list(&ioctx, "", 0, &mirror_image_ids));

  cls::rbd::MirrorImage image1("uuid1", cls::rbd::MIRROR_IMAGE_STATE_ENABLED);
  cls::rbd::MirrorImage image2("uuid2", cls::rbd::MIRROR_IMAGE_STATE_DISABLING);
  cls::rbd::MirrorImage image3("uuid3", cls::rbd::MIRROR_IMAGE_STATE_ENABLED);

  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id1", image1));
  ASSERT_EQ(-ENOENT, mirror_image_set(&ioctx, "image_id2", image2));
  image2.state = cls::rbd::MIRROR_IMAGE_STATE_ENABLED;
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id2", image2));
  image2.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id2", image2));
  ASSERT_EQ(-EINVAL, mirror_image_set(&ioctx, "image_id1", image2));
  ASSERT_EQ(-EEXIST, mirror_image_set(&ioctx, "image_id3", image2));
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id3", image3));

  std::string image_id;
  ASSERT_EQ(0, mirror_image_get_image_id(&ioctx, "uuid2", &image_id));
  ASSERT_EQ("image_id2", image_id);

  cls::rbd::MirrorImage read_image;
  ASSERT_EQ(0, mirror_image_get(&ioctx, "image_id1", &read_image));
  ASSERT_EQ(read_image, image1);
  ASSERT_EQ(0, mirror_image_get(&ioctx, "image_id2", &read_image));
  ASSERT_EQ(read_image, image2);
  ASSERT_EQ(0, mirror_image_get(&ioctx, "image_id3", &read_image));
  ASSERT_EQ(read_image, image3);

  ASSERT_EQ(0, mirror_image_list(&ioctx, "", 1, &mirror_image_ids));
  std::map<std::string, std::string> expected_mirror_image_ids = {
    {"image_id1", "uuid1"}};
  ASSERT_EQ(expected_mirror_image_ids, mirror_image_ids);

  ASSERT_EQ(0, mirror_image_list(&ioctx, "image_id1", 2, &mirror_image_ids));
  expected_mirror_image_ids = {{"image_id2", "uuid2"}, {"image_id3", "uuid3"}};
  ASSERT_EQ(expected_mirror_image_ids, mirror_image_ids);

  ASSERT_EQ(0, mirror_image_remove(&ioctx, "image_id2"));
  ASSERT_EQ(-ENOENT, mirror_image_get_image_id(&ioctx, "uuid2", &image_id));
  ASSERT_EQ(-EBUSY, mirror_image_remove(&ioctx, "image_id1"));

  ASSERT_EQ(0, mirror_image_list(&ioctx, "", 3, &mirror_image_ids));
  expected_mirror_image_ids = {{"image_id1", "uuid1"}, {"image_id3", "uuid3"}};
  ASSERT_EQ(expected_mirror_image_ids, mirror_image_ids);

  image1.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;
  image3.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id1", image1));
  ASSERT_EQ(0, mirror_image_get(&ioctx, "image_id1", &read_image));
  ASSERT_EQ(read_image, image1);
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id3", image3));
  ASSERT_EQ(0, mirror_image_remove(&ioctx, "image_id1"));
  ASSERT_EQ(0, mirror_image_remove(&ioctx, "image_id3"));

  ASSERT_EQ(0, mirror_image_list(&ioctx, "", 3, &mirror_image_ids));
  expected_mirror_image_ids = {};
  ASSERT_EQ(expected_mirror_image_ids, mirror_image_ids);
}

TEST_F(TestClsRbd, mirror_image_status) {
  struct WatchCtx : public librados::WatchCtx2 {
    librados::IoCtx *m_ioctx;

    WatchCtx(librados::IoCtx *ioctx) : m_ioctx(ioctx) {}
    void handle_notify(uint64_t notify_id, uint64_t cookie,
			     uint64_t notifier_id, bufferlist& bl_) override {
      bufferlist bl;
      m_ioctx->notify_ack(RBD_MIRRORING, notify_id, cookie, bl);
    }
    void handle_error(uint64_t cookie, int err) override {}
  };

  map<std::string, cls::rbd::MirrorImage> images;
  map<std::string, cls::rbd::MirrorImageStatus> statuses;
  std::map<cls::rbd::MirrorImageStatusState, int> states;
  cls::rbd::MirrorImageStatus read_status;
  uint64_t watch_handle;
  librados::IoCtx ioctx;

  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_MIRRORING);

  // Test list fails on nonexistent RBD_MIRRORING object

  ASSERT_EQ(-ENOENT, mirror_image_status_list(&ioctx, "", 1024, &images,
	  &statuses));

  // Test status set

  cls::rbd::MirrorImage image1("uuid1", cls::rbd::MIRROR_IMAGE_STATE_ENABLED);
  cls::rbd::MirrorImage image2("uuid2", cls::rbd::MIRROR_IMAGE_STATE_ENABLED);
  cls::rbd::MirrorImage image3("uuid3", cls::rbd::MIRROR_IMAGE_STATE_ENABLED);

  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id1", image1));
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id2", image2));
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id3", image3));

  cls::rbd::MirrorImageStatus status1(cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN);
  cls::rbd::MirrorImageStatus status2(cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING);
  cls::rbd::MirrorImageStatus status3(cls::rbd::MIRROR_IMAGE_STATUS_STATE_ERROR);

  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid1", status1));
  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, "", 1024, &images, &statuses));
  ASSERT_EQ(3U, images.size());
  ASSERT_EQ(1U, statuses.size());

  // Test status is down due to RBD_MIRRORING is not watched

  status1.up = false;
  ASSERT_EQ(statuses["image_id1"], status1);
  ASSERT_EQ(0, mirror_image_status_get(&ioctx, "uuid1", &read_status));
  ASSERT_EQ(read_status, status1);

  // Test status summary. All statuses are unknown due to down.
  states.clear();
  ASSERT_EQ(0, mirror_image_status_get_summary(&ioctx, &states));
  ASSERT_EQ(1U, states.size());
  ASSERT_EQ(3, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN]);

  // Test remove_down removes stale statuses

  ASSERT_EQ(0, mirror_image_status_remove_down(&ioctx));
  ASSERT_EQ(-ENOENT, mirror_image_status_get(&ioctx, "uuid1", &read_status));
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, "", 1024, &images, &statuses));
  ASSERT_EQ(3U, images.size());
  ASSERT_TRUE(statuses.empty());
  ASSERT_EQ(0, mirror_image_status_get_summary(&ioctx, &states));
  ASSERT_EQ(1U, states.size());
  ASSERT_EQ(3, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN]);

  // Test statuses are not down after watcher is started

  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid1", status1));

  WatchCtx watch_ctx(&ioctx);
  ASSERT_EQ(0, ioctx.watch2(RBD_MIRRORING, &watch_handle, &watch_ctx));

  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid2", status2));
  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid3", status3));

  ASSERT_EQ(0, mirror_image_status_get(&ioctx, "uuid1", &read_status));
  status1.up = true;
  ASSERT_EQ(read_status, status1);
  ASSERT_EQ(0, mirror_image_status_get(&ioctx, "uuid2", &read_status));
  status2.up = true;
  ASSERT_EQ(read_status, status2);
  ASSERT_EQ(0, mirror_image_status_get(&ioctx, "uuid3", &read_status));
  status3.up = true;
  ASSERT_EQ(read_status, status3);

  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, "", 1024, &images, &statuses));
  ASSERT_EQ(3U, images.size());
  ASSERT_EQ(3U, statuses.size());
  ASSERT_EQ(statuses["image_id1"], status1);
  ASSERT_EQ(statuses["image_id2"], status2);
  ASSERT_EQ(statuses["image_id3"], status3);

  ASSERT_EQ(0, mirror_image_status_remove_down(&ioctx));
  ASSERT_EQ(0, mirror_image_status_get(&ioctx, "uuid1", &read_status));
  ASSERT_EQ(read_status, status1);
  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, "", 1024, &images, &statuses));
  ASSERT_EQ(3U, images.size());
  ASSERT_EQ(3U, statuses.size());
  ASSERT_EQ(statuses["image_id1"], status1);
  ASSERT_EQ(statuses["image_id2"], status2);
  ASSERT_EQ(statuses["image_id3"], status3);

  states.clear();
  ASSERT_EQ(0, mirror_image_status_get_summary(&ioctx, &states));
  ASSERT_EQ(3U, states.size());
  ASSERT_EQ(1, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN]);
  ASSERT_EQ(1, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING]);
  ASSERT_EQ(1, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_ERROR]);

  // Test update

  status1.state = status3.state = cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING;
  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid1", status1));
  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid3", status3));
  ASSERT_EQ(0, mirror_image_status_get(&ioctx, "uuid3", &read_status));
  ASSERT_EQ(read_status, status3);

  states.clear();
  ASSERT_EQ(0, mirror_image_status_get_summary(&ioctx, &states));
  ASSERT_EQ(1U, states.size());
  ASSERT_EQ(3, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING]);

  // Test remove

  ASSERT_EQ(0, mirror_image_status_remove(&ioctx, "uuid3"));
  ASSERT_EQ(-ENOENT, mirror_image_status_get(&ioctx, "uuid3", &read_status));
  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, "", 1024, &images, &statuses));
  ASSERT_EQ(3U, images.size());
  ASSERT_EQ(2U, statuses.size());
  ASSERT_EQ(statuses["image_id1"], status1);
  ASSERT_EQ(statuses["image_id2"], status2);

  states.clear();
  ASSERT_EQ(0, mirror_image_status_get_summary(&ioctx, &states));
  ASSERT_EQ(2U, states.size());
  ASSERT_EQ(1, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN]);
  ASSERT_EQ(2, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_REPLAYING]);

  // Test statuses are down after removing watcher

  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid1", status1));
  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid2", status2));
  ASSERT_EQ(0, mirror_image_status_set(&ioctx, "uuid3", status3));

  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, "", 1024, &images, &statuses));
  ASSERT_EQ(3U, images.size());
  ASSERT_EQ(3U, statuses.size());
  ASSERT_EQ(statuses["image_id1"], status1);
  ASSERT_EQ(statuses["image_id2"], status2);
  ASSERT_EQ(statuses["image_id3"], status3);

  ioctx.unwatch2(watch_handle);

  ASSERT_EQ(0, mirror_image_status_list(&ioctx, "", 1024, &images, &statuses));
  ASSERT_EQ(3U, images.size());
  ASSERT_EQ(3U, statuses.size());
  status1.up = false;
  ASSERT_EQ(statuses["image_id1"], status1);
  status2.up = false;
  ASSERT_EQ(statuses["image_id2"], status2);
  status3.up = false;
  ASSERT_EQ(statuses["image_id3"], status3);

  ASSERT_EQ(0, mirror_image_status_get(&ioctx, "uuid1", &read_status));
  ASSERT_EQ(read_status, status1);

  states.clear();
  ASSERT_EQ(0, mirror_image_status_get_summary(&ioctx, &states));
  ASSERT_EQ(1U, states.size());
  ASSERT_EQ(3, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN]);

  ASSERT_EQ(0, mirror_image_status_remove_down(&ioctx));
  ASSERT_EQ(-ENOENT, mirror_image_status_get(&ioctx, "uuid1", &read_status));

  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, "", 1024, &images, &statuses));
  ASSERT_EQ(3U, images.size());
  ASSERT_TRUE(statuses.empty());

  states.clear();
  ASSERT_EQ(0, mirror_image_status_get_summary(&ioctx, &states));
  ASSERT_EQ(1U, states.size());
  ASSERT_EQ(3, states[cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN]);

  // Remove images

  image1.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;
  image2.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;
  image3.state = cls::rbd::MIRROR_IMAGE_STATE_DISABLING;

  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id1", image1));
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id2", image2));
  ASSERT_EQ(0, mirror_image_set(&ioctx, "image_id3", image3));

  ASSERT_EQ(0, mirror_image_remove(&ioctx, "image_id1"));
  ASSERT_EQ(0, mirror_image_remove(&ioctx, "image_id2"));
  ASSERT_EQ(0, mirror_image_remove(&ioctx, "image_id3"));

  states.clear();
  ASSERT_EQ(0, mirror_image_status_get_summary(&ioctx, &states));
  ASSERT_EQ(0U, states.size());

  // Test status list with large number of images

  size_t N = 1024;
  ASSERT_EQ(0U, N % 2);

  for (size_t i = 0; i < N; i++) {
    std::string id = "id" + stringify(i);
    std::string uuid = "uuid" + stringify(i);
    cls::rbd::MirrorImage image(uuid, cls::rbd::MIRROR_IMAGE_STATE_ENABLED);
    cls::rbd::MirrorImageStatus status(cls::rbd::MIRROR_IMAGE_STATUS_STATE_UNKNOWN);
    ASSERT_EQ(0, mirror_image_set(&ioctx, id, image));
    ASSERT_EQ(0, mirror_image_status_set(&ioctx, uuid, status));
  }

  std::string last_read = "";
  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, last_read, N * 2, &images,
	  &statuses));
  ASSERT_EQ(N, images.size());
  ASSERT_EQ(N, statuses.size());

  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, last_read, N / 2, &images,
	  &statuses));
  ASSERT_EQ(N / 2, images.size());
  ASSERT_EQ(N / 2, statuses.size());

  last_read = images.rbegin()->first;
  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, last_read, N / 2, &images,
	  &statuses));
  ASSERT_EQ(N / 2, images.size());
  ASSERT_EQ(N / 2, statuses.size());

  last_read = images.rbegin()->first;
  images.clear();
  statuses.clear();
  ASSERT_EQ(0, mirror_image_status_list(&ioctx, last_read, N / 2, &images,
	  &statuses));
  ASSERT_EQ(0U, images.size());
  ASSERT_EQ(0U, statuses.size());
}

TEST_F(TestClsRbd, mirror_image_map)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_MIRRORING);

  std::map<std::string, cls::rbd::MirrorImageMap> image_mapping;
  ASSERT_EQ(-ENOENT, mirror_image_map_list(&ioctx, "", 0, &image_mapping));

  utime_t expected_time = ceph_clock_now();

  bufferlist expected_data;
  expected_data.append("test");

  std::map<std::string, cls::rbd::MirrorImageMap> expected_image_mapping;
  while (expected_image_mapping.size() < 1024) {
    librados::ObjectWriteOperation op;
    for (uint32_t i = 0; i < 32; ++i) {
      std::string global_image_id{stringify(expected_image_mapping.size())};
      cls::rbd::MirrorImageMap mirror_image_map{
        stringify(i), expected_time, expected_data};
      expected_image_mapping.emplace(global_image_id, mirror_image_map);

      mirror_image_map_update(&op, global_image_id, mirror_image_map);
    }
    ASSERT_EQ(0, ioctx.operate(RBD_MIRRORING, &op));
  }

  ASSERT_EQ(0, mirror_image_map_list(&ioctx, "", 1000, &image_mapping));
  ASSERT_EQ(1000U, image_mapping.size());

  ASSERT_EQ(0, mirror_image_map_list(&ioctx, image_mapping.rbegin()->first,
                                     1000, &image_mapping));
  ASSERT_EQ(24U, image_mapping.size());

  const auto& image_map = *image_mapping.begin();
  ASSERT_EQ("978", image_map.first);

  cls::rbd::MirrorImageMap expected_mirror_image_map{
    stringify(18), expected_time, expected_data};
  ASSERT_EQ(expected_mirror_image_map, image_map.second);

  expected_time = ceph_clock_now();
  expected_mirror_image_map.mapped_time = expected_time;

  expected_data.append("update");
  expected_mirror_image_map.data = expected_data;

  librados::ObjectWriteOperation op;
  mirror_image_map_remove(&op, "1");
  mirror_image_map_update(&op, "10", expected_mirror_image_map);
  ASSERT_EQ(0, ioctx.operate(RBD_MIRRORING, &op));

  ASSERT_EQ(0, mirror_image_map_list(&ioctx, "0", 1, &image_mapping));
  ASSERT_EQ(1U, image_mapping.size());

  const auto& updated_image_map = *image_mapping.begin();
  ASSERT_EQ("10", updated_image_map.first);
  ASSERT_EQ(expected_mirror_image_map, updated_image_map.second);
}

TEST_F(TestClsRbd, mirror_instances) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_MIRROR_LEADER);

  std::vector<std::string> instance_ids;
  ASSERT_EQ(-ENOENT, mirror_instances_list(&ioctx, &instance_ids));

  ASSERT_EQ(0, ioctx.create(RBD_MIRROR_LEADER, true));
  ASSERT_EQ(0, mirror_instances_list(&ioctx, &instance_ids));
  ASSERT_EQ(0U, instance_ids.size());

  ASSERT_EQ(0, mirror_instances_add(&ioctx, "instance_id1"));
  ASSERT_EQ(0, mirror_instances_list(&ioctx, &instance_ids));
  ASSERT_EQ(1U, instance_ids.size());
  ASSERT_EQ(instance_ids[0], "instance_id1");

  ASSERT_EQ(0, mirror_instances_add(&ioctx, "instance_id1"));
  ASSERT_EQ(0, mirror_instances_add(&ioctx, "instance_id2"));
  ASSERT_EQ(0, mirror_instances_list(&ioctx, &instance_ids));
  ASSERT_EQ(2U, instance_ids.size());

  ASSERT_EQ(0, mirror_instances_remove(&ioctx, "instance_id1"));
  ASSERT_EQ(0, mirror_instances_list(&ioctx, &instance_ids));
  ASSERT_EQ(1U, instance_ids.size());
  ASSERT_EQ(instance_ids[0], "instance_id2");

  ASSERT_EQ(0, mirror_instances_remove(&ioctx, "instance_id2"));
  ASSERT_EQ(0, mirror_instances_list(&ioctx, &instance_ids));
  ASSERT_EQ(0U, instance_ids.size());
}

TEST_F(TestClsRbd, group_dir_list) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string group_id1 = "cgid1";
  string group_name1 = "cgname1";
  string group_id2 = "cgid2";
  string group_name2 = "cgname2";
  ASSERT_EQ(0, group_dir_add(&ioctx, RBD_GROUP_DIRECTORY, group_name1, group_id1));
  ASSERT_EQ(0, group_dir_add(&ioctx, RBD_GROUP_DIRECTORY, group_name2, group_id2));

  map<string, string> cgs;
  ASSERT_EQ(0, group_dir_list(&ioctx, RBD_GROUP_DIRECTORY, "", 10, &cgs));

  ASSERT_EQ(2U, cgs.size());

  auto it = cgs.begin();
  ASSERT_EQ(group_id1, it->second);
  ASSERT_EQ(group_name1, it->first);

  ++it;
  ASSERT_EQ(group_id2, it->second);
  ASSERT_EQ(group_name2, it->first);
}

void add_group_to_dir(librados::IoCtx ioctx, string group_id, string group_name) {
  ASSERT_EQ(0, group_dir_add(&ioctx, RBD_GROUP_DIRECTORY, group_name, group_id));

  set<string> keys;
  ASSERT_EQ(0, ioctx.omap_get_keys(RBD_GROUP_DIRECTORY, "", 10, &keys));
  ASSERT_EQ(2U, keys.size());
  ASSERT_EQ("id_" + group_id, *keys.begin());
  ASSERT_EQ("name_" + group_name, *keys.rbegin());
}

TEST_F(TestClsRbd, group_dir_add) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_GROUP_DIRECTORY);

  string group_id = "cgid";
  string group_name = "cgname";
  add_group_to_dir(ioctx, group_id, group_name);
}

TEST_F(TestClsRbd, dir_add_already_existing) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_GROUP_DIRECTORY);

  string group_id = "cgidexisting";
  string group_name = "cgnameexisting";
  add_group_to_dir(ioctx, group_id, group_name);

  ASSERT_EQ(-EEXIST, group_dir_add(&ioctx, RBD_GROUP_DIRECTORY, group_name, group_id));
}

TEST_F(TestClsRbd, group_dir_rename) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_GROUP_DIRECTORY);

  string group_id = "cgid";
  string src_name = "cgnamesrc";
  string dest_name = "cgnamedest";
  add_group_to_dir(ioctx, group_id, src_name);

  ASSERT_EQ(0, group_dir_rename(&ioctx, RBD_GROUP_DIRECTORY,
                                src_name, dest_name, group_id));
  map<string, string> cgs;
  ASSERT_EQ(0, group_dir_list(&ioctx, RBD_GROUP_DIRECTORY, "", 10, &cgs));
  ASSERT_EQ(1U, cgs.size());
  auto it = cgs.begin();
  ASSERT_EQ(group_id, it->second);
  ASSERT_EQ(dest_name, it->first);

  // destination group name existing
  ASSERT_EQ(-EEXIST, group_dir_rename(&ioctx, RBD_GROUP_DIRECTORY,
                                      dest_name, dest_name, group_id));
  ASSERT_EQ(0, group_dir_remove(&ioctx, RBD_GROUP_DIRECTORY, dest_name, group_id));
  // source group name missing
  ASSERT_EQ(-ENOENT, group_dir_rename(&ioctx, RBD_GROUP_DIRECTORY,
                                      dest_name, src_name, group_id));
}

TEST_F(TestClsRbd, group_dir_remove) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_GROUP_DIRECTORY);

  string group_id = "cgidtodel";
  string group_name = "cgnametodel";
  add_group_to_dir(ioctx, group_id, group_name);

  ASSERT_EQ(0, group_dir_remove(&ioctx, RBD_GROUP_DIRECTORY, group_name, group_id));

  set<string> keys;
  ASSERT_EQ(0, ioctx.omap_get_keys(RBD_GROUP_DIRECTORY, "", 10, &keys));
  ASSERT_EQ(0U, keys.size());
}

TEST_F(TestClsRbd, group_dir_remove_missing) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));
  ioctx.remove(RBD_GROUP_DIRECTORY);

  string group_id = "cgidtodelmissing";
  string group_name = "cgnametodelmissing";
  // These two lines ensure that RBD_GROUP_DIRECTORY exists. It's important for the
  // last two lines.
  add_group_to_dir(ioctx, group_id, group_name);

  ASSERT_EQ(0, group_dir_remove(&ioctx, RBD_GROUP_DIRECTORY, group_name, group_id));

  // Removing missing
  ASSERT_EQ(-ENOENT, group_dir_remove(&ioctx, RBD_GROUP_DIRECTORY, group_name, group_id));

  set<string> keys;
  ASSERT_EQ(0, ioctx.omap_get_keys(RBD_GROUP_DIRECTORY, "", 10, &keys));
  ASSERT_EQ(0U, keys.size());
}

void test_image_add(librados::IoCtx &ioctx, const string& group_id,
		    const string& image_id, int64_t pool_id) {

  cls::rbd::GroupImageStatus st(image_id, pool_id,
			       cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE);
  ASSERT_EQ(0, group_image_set(&ioctx, group_id, st));

  set<string> keys;
  ASSERT_EQ(0, ioctx.omap_get_keys(group_id, "", 10, &keys));

  auto it = keys.begin();
  ASSERT_EQ(1U, keys.size());

  string image_key = cls::rbd::GroupImageSpec(image_id, pool_id).image_key();
  ASSERT_EQ(image_key, *it);
}

TEST_F(TestClsRbd, group_image_add) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string group_id = "group_id";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  int64_t pool_id = ioctx.get_id();
  string image_id = "image_id";
  test_image_add(ioctx, group_id, image_id, pool_id);
}

TEST_F(TestClsRbd, group_image_remove) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string group_id = "group_id1";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  int64_t pool_id = ioctx.get_id();
  string image_id = "image_id";
  test_image_add(ioctx, group_id, image_id, pool_id);

  cls::rbd::GroupImageSpec spec(image_id, pool_id);
  ASSERT_EQ(0, group_image_remove(&ioctx, group_id, spec));
  set<string> keys;
  ASSERT_EQ(0, ioctx.omap_get_keys(group_id, "", 10, &keys));
  ASSERT_EQ(0U, keys.size());
}

TEST_F(TestClsRbd, group_image_list) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string group_id = "group_id2";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  int64_t pool_id = ioctx.get_id();
  string image_id = "imageid"; // Image id shouldn't contain underscores
  test_image_add(ioctx, group_id, image_id, pool_id);

  vector<cls::rbd::GroupImageStatus> images;
  cls::rbd::GroupImageSpec empty_image_spec = cls::rbd::GroupImageSpec();
  ASSERT_EQ(0, group_image_list(&ioctx, group_id, empty_image_spec, 1024,
                                &images));
  ASSERT_EQ(1U, images.size());
  ASSERT_EQ(image_id, images[0].spec.image_id);
  ASSERT_EQ(pool_id, images[0].spec.pool_id);
  ASSERT_EQ(cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE, images[0].state);

  cls::rbd::GroupImageStatus last_image = *images.rbegin();
  ASSERT_EQ(0, group_image_list(&ioctx, group_id, last_image.spec, 1024,
                                &images));
  ASSERT_EQ(0U, images.size());
}

TEST_F(TestClsRbd, group_image_clean) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string group_id = "group_id3";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  int64_t pool_id = ioctx.get_id();
  string image_id = "image_id";
  test_image_add(ioctx, group_id, image_id, pool_id);

  cls::rbd::GroupImageStatus incomplete_st(image_id, pool_id,
			       cls::rbd::GROUP_IMAGE_LINK_STATE_INCOMPLETE);

  ASSERT_EQ(0, group_image_set(&ioctx, group_id, incomplete_st));
  // Set to dirty first in order to make sure that group_image_clean
  // actually does something.
  cls::rbd::GroupImageStatus attached_st(image_id, pool_id,
			       cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED);
  ASSERT_EQ(0, group_image_set(&ioctx, group_id, attached_st));

  string image_key = cls::rbd::GroupImageSpec(image_id, pool_id).image_key();

  map<string, bufferlist> vals;
  ASSERT_EQ(0, ioctx.omap_get_vals(group_id, "", 10, &vals));

  cls::rbd::GroupImageLinkState ref_state;
  bufferlist::iterator it = vals[image_key].begin();
  decode(ref_state, it);
  ASSERT_EQ(cls::rbd::GROUP_IMAGE_LINK_STATE_ATTACHED, ref_state);
}

TEST_F(TestClsRbd, image_group_add) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  int64_t pool_id = ioctx.get_id();
  string image_id = "imageid";

  ASSERT_EQ(0, create_image(&ioctx, image_id, 2<<20, 0,
			    RBD_FEATURE_LAYERING, image_id, -1));

  string group_id = "group_id";

  cls::rbd::GroupSpec spec(group_id, pool_id);
  ASSERT_EQ(0, image_group_add(&ioctx, image_id, spec));

  map<string, bufferlist> vals;
  ASSERT_EQ(0, ioctx.omap_get_vals(image_id, "", RBD_GROUP_REF, 10, &vals));

  cls::rbd::GroupSpec val_spec;
  bufferlist::iterator it = vals[RBD_GROUP_REF].begin();
  decode(val_spec, it);

  ASSERT_EQ(group_id, val_spec.group_id);
  ASSERT_EQ(pool_id, val_spec.pool_id);
}

TEST_F(TestClsRbd, image_group_remove) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  int64_t pool_id = ioctx.get_id();
  string image_id = "image_id";

  ASSERT_EQ(0, create_image(&ioctx, image_id, 2<<20, 0,
			    RBD_FEATURE_LAYERING, image_id, -1));

  string group_id = "group_id";

  cls::rbd::GroupSpec spec(group_id, pool_id);
  ASSERT_EQ(0, image_group_add(&ioctx, image_id, spec));
  // Add reference in order to make sure that image_group_remove actually
  // does something.
  ASSERT_EQ(0, image_group_remove(&ioctx, image_id, spec));

  map<string, bufferlist> vals;
  ASSERT_EQ(0, ioctx.omap_get_vals(image_id, "", RBD_GROUP_REF, 10, &vals));

  ASSERT_EQ(0U, vals.size());
}

TEST_F(TestClsRbd, image_group_get) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  int64_t pool_id = ioctx.get_id();
  string image_id = "imageidgroupspec";

  ASSERT_EQ(0, create_image(&ioctx, image_id, 2<<20, 0,
			    RBD_FEATURE_LAYERING, image_id, -1));

  string group_id = "group_id_get_group_spec";

  cls::rbd::GroupSpec spec_add(group_id, pool_id);
  ASSERT_EQ(0, image_group_add(&ioctx, image_id, spec_add));

  cls::rbd::GroupSpec spec;
  ASSERT_EQ(0, image_group_get(&ioctx, image_id, &spec));

  ASSERT_EQ(group_id, spec.group_id);
  ASSERT_EQ(pool_id, spec.pool_id);
}

TEST_F(TestClsRbd, group_snap_set_empty_name) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_add_emtpy_name";

  string group_id = "group_id_snap_add_empty_name";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  string snap_id = "snap_id";
  cls::rbd::GroupSnapshot snap = {snap_id, "", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(-EINVAL, group_snap_set(&ioctx, group_id, snap));
}

TEST_F(TestClsRbd, group_snap_set_empty_id) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_add_empty_id";

  string group_id = "group_id_snap_add_empty_id";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  string snap_id = "snap_id";
  cls::rbd::GroupSnapshot snap = {"", "snap_name", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(-EINVAL, group_snap_set(&ioctx, group_id, snap));
}

TEST_F(TestClsRbd, group_snap_set_duplicate_id) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_add_duplicate_id";

  string group_id = "group_id_snap_add_duplicate_id";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  string snap_id = "snap_id";
  cls::rbd::GroupSnapshot snap = {snap_id, "snap_name", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(0, group_snap_set(&ioctx, group_id, snap));

  cls::rbd::GroupSnapshot snap1 = {snap_id, "snap_name1", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(-EEXIST, group_snap_set(&ioctx, group_id, snap1));
}

TEST_F(TestClsRbd, group_snap_set_duplicate_name) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_add_duplicate_name";

  string group_id = "group_id_snap_add_duplicate_name";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  string snap_id1 = "snap_id1";
  cls::rbd::GroupSnapshot snap = {snap_id1, "snap_name", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(0, group_snap_set(&ioctx, group_id, snap));

  string snap_id2 = "snap_id2";
  cls::rbd::GroupSnapshot snap1 = {snap_id2, "snap_name", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(-EEXIST, group_snap_set(&ioctx, group_id, snap1));
}

TEST_F(TestClsRbd, group_snap_set) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_add";

  string group_id = "group_id_snap_add";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  string snap_id = "snap_id";
  cls::rbd::GroupSnapshot snap = {snap_id, "test_snapshot", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(0, group_snap_set(&ioctx, group_id, snap));

  set<string> keys;
  ASSERT_EQ(0, ioctx.omap_get_keys(group_id, "", 10, &keys));

  auto it = keys.begin();
  ASSERT_EQ(1U, keys.size());

  string snap_key = "snapshot_" + stringify(snap.id);
  ASSERT_EQ(snap_key, *it);
}

TEST_F(TestClsRbd, group_snap_list) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_list";

  string group_id = "group_id_snap_list";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  string snap_id1 = "snap_id1";
  cls::rbd::GroupSnapshot snap1 = {snap_id1, "test_snapshot1", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(0, group_snap_set(&ioctx, group_id, snap1));

  string snap_id2 = "snap_id2";
  cls::rbd::GroupSnapshot snap2 = {snap_id2, "test_snapshot2", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(0, group_snap_set(&ioctx, group_id, snap2));

  std::vector<cls::rbd::GroupSnapshot> snapshots;
  ASSERT_EQ(0, group_snap_list(&ioctx, group_id, cls::rbd::GroupSnapshot(), 10, &snapshots));
  ASSERT_EQ(2U, snapshots.size());
  ASSERT_EQ(snap_id1, snapshots[0].id);
  ASSERT_EQ(snap_id2, snapshots[1].id);
}

static std::string hexify(int v) {
  ostringstream oss;
  oss << std::setw(8) << std::setfill('0') << std::hex << v;
  //oss << v;
  return oss.str();
}

TEST_F(TestClsRbd, group_snap_list_max_return) {
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_list_max_return";

  string group_id = "group_id_snap_list_max_return";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  for (int i = 0; i < 15; ++i) {
    string snap_id = "snap_id" + hexify(i);
    cls::rbd::GroupSnapshot snap = {snap_id,
				    "test_snapshot" + hexify(i),
				    cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
    ASSERT_EQ(0, group_snap_set(&ioctx, group_id, snap));
  }

  std::vector<cls::rbd::GroupSnapshot> snapshots;
  ASSERT_EQ(0, group_snap_list(&ioctx, group_id, cls::rbd::GroupSnapshot(), 10, &snapshots));
  ASSERT_EQ(10U, snapshots.size());

  for (int i = 0; i < 10; ++i) {
    string snap_id = "snap_id" + hexify(i);
    ASSERT_EQ(snap_id, snapshots[i].id);
  }

  cls::rbd::GroupSnapshot last_snap = *snapshots.rbegin();

  ASSERT_EQ(0, group_snap_list(&ioctx, group_id, last_snap, 10, &snapshots));
  ASSERT_EQ(5U, snapshots.size());
  for (int i = 10; i < 15; ++i) {
    string snap_id = "snap_id" + hexify(i);
    ASSERT_EQ(snap_id, snapshots[i - 10].id);
  }
}

TEST_F(TestClsRbd, group_snap_remove) {
  librados::IoCtx ioctx;

  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_remove";

  string group_id = "group_id_snap_remove";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  string snap_id = "snap_id";
  cls::rbd::GroupSnapshot snap = {snap_id, "test_snapshot", cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(0, group_snap_set(&ioctx, group_id, snap));

  set<string> keys;
  ASSERT_EQ(0, ioctx.omap_get_keys(group_id, "", 10, &keys));

  auto it = keys.begin();
  ASSERT_EQ(1U, keys.size());

  string snap_key = "snapshot_" + stringify(snap.id);
  ASSERT_EQ(snap_key, *it);

  // Remove the snapshot

  ASSERT_EQ(0, group_snap_remove(&ioctx, group_id, snap_id));

  ASSERT_EQ(0, ioctx.omap_get_keys(group_id, "", 10, &keys));

  ASSERT_EQ(0U, keys.size());
}

TEST_F(TestClsRbd, group_snap_get_by_id) {
  librados::IoCtx ioctx;

  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string image_id = "image_id_snap_get_by_id";

  string group_id = "group_id_snap_get_by_id";
  ASSERT_EQ(0, ioctx.create(group_id, true));

  string snap_id = "snap_id";
  cls::rbd::GroupSnapshot snap = {snap_id,
                                  "test_snapshot",
                                  cls::rbd::GROUP_SNAPSHOT_STATE_INCOMPLETE};
  ASSERT_EQ(0, group_snap_set(&ioctx, group_id, snap));

  cls::rbd::GroupSnapshot received_snap;
  ASSERT_EQ(0, group_snap_get_by_id(&ioctx, group_id, snap_id, &received_snap));

  ASSERT_EQ(snap.id, received_snap.id);
  ASSERT_EQ(snap.name, received_snap.name);
  ASSERT_EQ(snap.state, received_snap.state);
}
TEST_F(TestClsRbd, trash_methods)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string id = "123456789";
  string id2 = "123456780";

  std::map<string, cls::rbd::TrashImageSpec> entries;
  ASSERT_EQ(-ENOENT, trash_list(&ioctx, "", 1024, &entries));

  utime_t now1 = ceph_clock_now();
  utime_t now1_delay = now1;
  now1_delay += 380;
  cls::rbd::TrashImageSpec trash_spec(cls::rbd::TRASH_IMAGE_SOURCE_USER, "name",
                                      now1, now1_delay);
  ASSERT_EQ(0, trash_add(&ioctx, id, trash_spec));

  utime_t now2 = ceph_clock_now();
  utime_t now2_delay = now2;
  now2_delay += 480;
  cls::rbd::TrashImageSpec trash_spec2(cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING,
                                       "name2", now2, now2_delay);
  ASSERT_EQ(-EEXIST, trash_add(&ioctx, id, trash_spec2));

  ASSERT_EQ(0, trash_remove(&ioctx, id));
  ASSERT_EQ(-ENOENT, trash_remove(&ioctx, id));

  ASSERT_EQ(0, trash_list(&ioctx, "", 1024, &entries));
  ASSERT_TRUE(entries.empty());

  ASSERT_EQ(0, trash_add(&ioctx, id, trash_spec2));
  ASSERT_EQ(0, trash_add(&ioctx, id2, trash_spec));

  ASSERT_EQ(0, trash_list(&ioctx, "", 1, &entries));
  ASSERT_TRUE(entries.find(id2) != entries.end());
  ASSERT_EQ(cls::rbd::TRASH_IMAGE_SOURCE_USER, entries[id2].source);
  ASSERT_EQ(std::string("name"), entries[id2].name);
  ASSERT_EQ(now1, entries[id2].deletion_time);
  ASSERT_EQ(now1_delay, entries[id2].deferment_end_time);

  ASSERT_EQ(0, trash_list(&ioctx, id2, 1, &entries));
  ASSERT_TRUE(entries.find(id) != entries.end());
  ASSERT_EQ(cls::rbd::TRASH_IMAGE_SOURCE_MIRRORING, entries[id].source);
  ASSERT_EQ(std::string("name2"), entries[id].name);
  ASSERT_EQ(now2, entries[id].deletion_time);
  ASSERT_EQ(now2_delay, entries[id].deferment_end_time);

  ASSERT_EQ(0, trash_list(&ioctx, id, 1, &entries));
  ASSERT_TRUE(entries.empty());

  cls::rbd::TrashImageSpec spec_res1;
  ASSERT_EQ(0, trash_get(&ioctx, id, &spec_res1));
  cls::rbd::TrashImageSpec spec_res2;
  ASSERT_EQ(0, trash_get(&ioctx, id2, &spec_res2));

  ASSERT_EQ(spec_res1.name, "name2");
  ASSERT_EQ(spec_res1.deletion_time, now2);
  ASSERT_EQ(spec_res1.deferment_end_time, now2_delay);

  ASSERT_EQ(spec_res2.name, "name");
  ASSERT_EQ(spec_res2.deletion_time, now1);
  ASSERT_EQ(spec_res2.deferment_end_time, now1_delay);

  ioctx.close();
}

TEST_F(TestClsRbd, op_features)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));

  uint64_t op_features = RBD_OPERATION_FEATURE_CLONE_PARENT;
  uint64_t mask = ~RBD_OPERATION_FEATURES_ALL;
  ASSERT_EQ(-EINVAL, op_features_set(&ioctx, oid, op_features, mask));

  mask = 0;
  ASSERT_EQ(0, op_features_set(&ioctx, oid, op_features, mask));

  uint64_t actual_op_features;
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &actual_op_features));
  ASSERT_EQ(0, actual_op_features);

  uint64_t features;
  ASSERT_EQ(0, get_features(&ioctx, oid, CEPH_NOSNAP, &features));
  ASSERT_EQ(0u, features);

  op_features = RBD_OPERATION_FEATURES_ALL;
  mask = RBD_OPERATION_FEATURES_ALL;
  ASSERT_EQ(0, op_features_set(&ioctx, oid, op_features, mask));
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &actual_op_features));
  ASSERT_EQ(mask, actual_op_features);

  ASSERT_EQ(0, get_features(&ioctx, oid, CEPH_NOSNAP, &features));
  ASSERT_EQ(RBD_FEATURE_OPERATIONS, features);

  op_features = 0;
  mask = RBD_OPERATION_FEATURE_CLONE_PARENT;
  ASSERT_EQ(0, op_features_set(&ioctx, oid, op_features, mask));
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &actual_op_features));

  uint64_t expected_op_features = RBD_OPERATION_FEATURES_ALL &
                                    ~RBD_OPERATION_FEATURE_CLONE_PARENT;
  ASSERT_EQ(expected_op_features, actual_op_features);

  mask = RBD_OPERATION_FEATURES_ALL;
  ASSERT_EQ(0, op_features_set(&ioctx, oid, op_features, mask));
  ASSERT_EQ(0, get_features(&ioctx, oid, CEPH_NOSNAP, &features));
  ASSERT_EQ(0u, features);
}

TEST_F(TestClsRbd, clone_parent)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22, 0, oid, -1));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 123, "user_snap"));

  ASSERT_EQ(-ENOENT, child_attach(&ioctx, oid, 345, {}));
  ASSERT_EQ(-ENOENT, child_detach(&ioctx, oid, 123, {}));
  ASSERT_EQ(-ENOENT, child_detach(&ioctx, oid, 345, {}));

  ASSERT_EQ(0, child_attach(&ioctx, oid, 123, {1, "image1"}));
  ASSERT_EQ(-EEXIST, child_attach(&ioctx, oid, 123, {1, "image1"}));
  ASSERT_EQ(0, child_attach(&ioctx, oid, 123, {1, "image2"}));
  ASSERT_EQ(0, child_attach(&ioctx, oid, 123, {2, "image2"}));

  std::vector<cls::rbd::SnapshotInfo> snaps;
  std::vector<ParentInfo> parents;
  std::vector<uint8_t> protection_status;
  ASSERT_EQ(0, snapshot_get(&ioctx, oid, {123}, &snaps,
                            &parents, &protection_status));
  ASSERT_EQ(1U, snaps.size());
  ASSERT_EQ(3U, snaps[0].child_count);

  // op feature should have been enabled
  uint64_t op_features;
  uint64_t expected_op_features = RBD_OPERATION_FEATURE_CLONE_PARENT;
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &op_features));
  ASSERT_TRUE((op_features & expected_op_features) == expected_op_features);

  // cannot attach to trashed snapshot
  librados::ObjectWriteOperation op1;
  ::librbd::cls_client::snapshot_add(&op1, 234, "trash_snap",
                                     cls::rbd::UserSnapshotNamespace());
  ASSERT_EQ(0, ioctx.operate(oid, &op1));
  librados::ObjectWriteOperation op2;
  ::librbd::cls_client::snapshot_trash_add(&op2, 234);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));
  ASSERT_EQ(-ENOENT, child_attach(&ioctx, oid, 234, {}));

  cls::rbd::ChildImageSpecs child_images;
  ASSERT_EQ(0, children_list(&ioctx, oid, 123, &child_images));

  cls::rbd::ChildImageSpecs expected_child_images = {
    {1, "image1"}, {1, "image2"}, {2, "image2"}};
  ASSERT_EQ(expected_child_images, child_images);

  // move snapshot to the trash
  ASSERT_EQ(-EBUSY, snapshot_remove(&ioctx, oid, 123));
  librados::ObjectWriteOperation op3;
  ::librbd::cls_client::snapshot_trash_add(&op3, 123);
  ASSERT_EQ(0, ioctx.operate(oid, &op3));
  ASSERT_EQ(0, snapshot_get(&ioctx, oid, {123}, &snaps,
                            &parents, &protection_status));
  ASSERT_EQ(1U, snaps.size());
  ASSERT_EQ(cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH,
            cls::rbd::get_snap_namespace_type(snaps[0].snapshot_namespace));

  expected_op_features |= RBD_OPERATION_FEATURE_SNAP_TRASH;
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &op_features));
  ASSERT_TRUE((op_features & expected_op_features) == expected_op_features);

  expected_child_images = {{1, "image1"}, {2, "image2"}};
  ASSERT_EQ(0, child_detach(&ioctx, oid, 123, {1, "image2"}));
  ASSERT_EQ(0, children_list(&ioctx, oid, 123, &child_images));
  ASSERT_EQ(expected_child_images, child_images);

  ASSERT_EQ(0, child_detach(&ioctx, oid, 123, {2, "image2"}));

  ASSERT_EQ(0, op_features_get(&ioctx, oid, &op_features));
  ASSERT_TRUE((op_features & expected_op_features) == expected_op_features);

  ASSERT_EQ(0, child_detach(&ioctx, oid, 123, {1, "image1"}));
  ASSERT_EQ(-ENOENT, children_list(&ioctx, oid, 123, &child_images));

  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 234));
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &op_features));
  ASSERT_TRUE((op_features & expected_op_features) ==
                RBD_OPERATION_FEATURE_SNAP_TRASH);

  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 123));
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &op_features));
  ASSERT_TRUE((op_features & expected_op_features) == 0);
}

TEST_F(TestClsRbd, clone_child)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  string oid = get_temp_image_name();
  ASSERT_EQ(0, create_image(&ioctx, oid, 0, 22,
                            RBD_FEATURE_LAYERING | RBD_FEATURE_DEEP_FLATTEN,
                            oid, -1));
  ASSERT_EQ(0, set_parent(&ioctx, oid, {1, "parent", 2}, 1));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 123, "user_snap1"));
  ASSERT_EQ(0, op_features_set(&ioctx, oid, RBD_OPERATION_FEATURE_CLONE_CHILD,
                               RBD_OPERATION_FEATURE_CLONE_CHILD));

  // clone child should be disabled due to deep flatten
  ASSERT_EQ(0, remove_parent(&ioctx, oid));
  uint64_t op_features;
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_CLONE_CHILD) == 0ULL);

  ASSERT_EQ(0, set_features(&ioctx, oid, 0, RBD_FEATURE_DEEP_FLATTEN));
  ASSERT_EQ(0, set_parent(&ioctx, oid, {1, "parent", 2}, 1));
  ASSERT_EQ(0, snapshot_add(&ioctx, oid, 124, "user_snap2"));
  ASSERT_EQ(0, op_features_set(&ioctx, oid, RBD_OPERATION_FEATURE_CLONE_CHILD,
                               RBD_OPERATION_FEATURE_CLONE_CHILD));

  // clone child should remain enabled w/o deep flatten
  ASSERT_EQ(0, remove_parent(&ioctx, oid));
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_CLONE_CHILD) ==
                RBD_OPERATION_FEATURE_CLONE_CHILD);

  // ... but removing the last linked snapshot should disable it
  ASSERT_EQ(0, snapshot_remove(&ioctx, oid, 124));
  ASSERT_EQ(0, op_features_get(&ioctx, oid, &op_features));
  ASSERT_TRUE((op_features & RBD_OPERATION_FEATURE_CLONE_CHILD) == 0ULL);
}

