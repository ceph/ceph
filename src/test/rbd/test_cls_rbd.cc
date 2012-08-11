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
using ::librbd::cls_client::get_parent;
using ::librbd::cls_client::set_parent;
using ::librbd::cls_client::remove_parent;
using ::librbd::cls_client::snapshot_add;
using ::librbd::cls_client::snapshot_remove;
using ::librbd::cls_client::add_child;
using ::librbd::cls_client::remove_child;
using ::librbd::cls_client::get_children;
using ::librbd::cls_client::get_snapcontext;
using ::librbd::cls_client::snapshot_list;
using ::librbd::cls_client::list_locks;
using ::librbd::cls_client::lock_image_exclusive;
using ::librbd::cls_client::lock_image_shared;
using ::librbd::cls_client::unlock_image;
using ::librbd::cls_client::break_lock;
using ::librbd::cls_client::copyup;
using ::librbd::cls_client::get_id;
using ::librbd::cls_client::set_id;
using ::librbd::cls_client::dir_get_id;
using ::librbd::cls_client::dir_get_name;
using ::librbd::cls_client::dir_list;
using ::librbd::cls_client::dir_add_image;
using ::librbd::cls_client::dir_remove_image;
using ::librbd::cls_client::dir_rename_image;
using ::librbd::cls_client::parent_info;
using ::librbd::cls_client::parent_spec;

static char *random_buf(size_t len)
{
  char *b = new char[len];
  for (size_t i = 0; i < len; i++)
    b[i] = (rand() % (128 - 32)) + 32;
  return b;
}

TEST(cls_rbd, copyup)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "rbd_copyup_test";
  bufferlist inbl, outbl;

  // copyup of 0-len nonexistent object should create new 0-len object
  ioctx.remove(oid);
  ASSERT_EQ(0, copyup(&ioctx, oid, inbl));
  size_t size;
  ASSERT_EQ(0, ioctx.stat(oid, &size, NULL));
  ASSERT_EQ(0U, size);

  // create some random data to write
  size_t l = 4 << 20;
  char *b = random_buf(l);
  inbl.append(b, l);
  delete b;
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
  delete b;
  ASSERT_EQ(l, inbl2.length());

  // should still succeed
  ASSERT_EQ(0, copyup(&ioctx, oid, inbl));
  ASSERT_EQ(l, (size_t)ioctx.read(oid, outbl, l, 0));
  // but contents should not have changed
  ASSERT_FALSE(outbl.contents_equal(inbl2));
  ASSERT_TRUE(outbl.contents_equal(inbl));

  ASSERT_EQ(0, ioctx.remove(oid));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, get_and_set_id)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "rbd_id_test";
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
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, add_remove_child)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "rbd_children_test";
  ASSERT_EQ(0, ioctx.create(oid, true));

  string snapname = "parent_snap";
  snapid_t snapid(10);
  string parent_image = "parent_id";
  string child_image = "child_id";
  set<string>children;
  parent_spec pspec(ioctx.get_id(), parent_image, snapid);

  // nonexistent children cannot be listed or removed
  ASSERT_EQ(-ENOENT, get_children(&ioctx, oid, pspec, children));
  ASSERT_EQ(-ENOENT, remove_child(&ioctx, oid, pspec, child_image));

  // make a parent with a snapshot
  ASSERT_EQ(0, create_image(&ioctx, parent_image, 2<<20, 0,
			    RBD_FEATURE_LAYERING, parent_image));
  ASSERT_EQ(0, snapshot_add(&ioctx, parent_image, snapid, snapname));

  // add, verify it showed up
  ASSERT_EQ(0, add_child(&ioctx, oid, pspec, child_image));
  ASSERT_EQ(0, get_children(&ioctx, oid, pspec, children));
  bool found = (children.find(child_image) != children.end());
  ASSERT_EQ(found, true);

  ASSERT_EQ(0, remove_child(&ioctx, oid, pspec, child_image));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rbd, directory_methods)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "rbd_id_test";
  string id, name;
  string imgname = "bar";
  string imgname2 = "foo";
  string imgname3 = "baz";
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

  ASSERT_EQ(-ESTALE, dir_rename_image(&ioctx, oid, imgname, imgname2, valid_id2));
  ASSERT_EQ(-ESTALE, dir_remove_image(&ioctx, oid, imgname, valid_id2));
  ASSERT_EQ(-EEXIST, dir_rename_image(&ioctx, oid, imgname, imgname2, valid_id));
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname, &id));
  ASSERT_EQ(valid_id, id);
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id2, &name));
  ASSERT_EQ(imgname2, name);

  ASSERT_EQ(0, dir_rename_image(&ioctx, oid, imgname, imgname3, valid_id));
  ASSERT_EQ(0, dir_get_id(&ioctx, oid, imgname3, &id));
  ASSERT_EQ(valid_id, id);
  ASSERT_EQ(0, dir_get_name(&ioctx, oid, valid_id, &name));
  ASSERT_EQ(imgname3, name);
  ASSERT_EQ(0, dir_rename_image(&ioctx, oid, imgname3, imgname, valid_id));

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
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

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

TEST(cls_rbd, image_locking)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "test_locking_image";
  uint64_t size = 20 << 30;
  uint64_t features = 0;
  uint8_t order = 22;
  string object_prefix = "foo";
  string cookies[4];
  cookies[0] = "cookie0";
  cookies[1] = "cookie1";
  cookies[2] = "cookie2";
  cookies[3] = "cookie3";

  ASSERT_EQ(0, create_image(&ioctx, oid, size, order,
                            features, object_prefix));

  // test that we can lock
  ASSERT_EQ(0, lock_image_exclusive(&ioctx, oid, cookies[0]));
  // and that we can't lock again
  ASSERT_EQ(-EBUSY, lock_image_exclusive(&ioctx, oid, cookies[1]));
  // and that unlock works
  ASSERT_EQ(0, unlock_image(&ioctx, oid, cookies[0]));
  // and that we can do shared locking
  ASSERT_EQ(0, lock_image_shared(&ioctx, oid, cookies[2]));
  ASSERT_EQ(0, lock_image_shared(&ioctx, oid, cookies[3]));
  // and that you can't exclusive lock with shared lockers
  ASSERT_EQ(-EBUSY, lock_image_exclusive(&ioctx, oid, cookies[1]));
  // but that you can after unlocking the shared lockers
  ASSERT_EQ(0, unlock_image(&ioctx, oid, cookies[2]));
  ASSERT_EQ(0, unlock_image(&ioctx, oid, cookies[3]));
  ASSERT_EQ(0, lock_image_exclusive(&ioctx, oid, cookies[1]));
  ASSERT_EQ(0, unlock_image(&ioctx, oid, cookies[1]));

  // test that we can list locks
  std::set<std::pair<std::string, std::string> > lockers;
  bool exclusive;
  ASSERT_EQ(0, list_locks(&ioctx, oid, lockers, exclusive));
  // and that no locks makes for an empty set
  int lockers_size = lockers.size();
  ASSERT_EQ(0, lockers_size);

  // test that two shared lockers compare properly
  ASSERT_EQ(0, lock_image_shared(&ioctx, oid, cookies[2]));
  ASSERT_EQ(0, lock_image_shared(&ioctx, oid, cookies[3]));
  ASSERT_EQ(0, list_locks(&ioctx, oid, lockers, exclusive));
  ASSERT_FALSE(exclusive);
  lockers_size = lockers.size();
  ASSERT_EQ(2, lockers_size);
  std::set<std::pair<std::string, std::string> >::iterator first, second;
  second = lockers.begin();
  first = second++;
  ASSERT_EQ(0, first->first.compare(second->first));
  std::string our_entity = first->first; // saved for later
  ASSERT_EQ(0, unlock_image(&ioctx, oid, cookies[2]));
  ASSERT_EQ(0, unlock_image(&ioctx, oid, cookies[3]));

  // and that a single exclusive looks right
  ASSERT_EQ(0, lock_image_exclusive(&ioctx, oid, cookies[0]));
  ASSERT_EQ(0, list_locks(&ioctx, oid, lockers, exclusive));
  lockers_size = lockers.size();
  ASSERT_EQ(1, lockers_size);
  ASSERT_TRUE(exclusive);

  // and do our best to test lock breaking
  ASSERT_EQ(0, break_lock(&ioctx, oid, our_entity, cookies[0]));
  // and that it actually removes the lock
  ASSERT_EQ(0, list_locks(&ioctx, oid, lockers, exclusive));
  lockers_size = lockers.size();
  ASSERT_EQ(0, lockers_size);

  // test that non-existent locks return errors correctly
  ASSERT_EQ(-ENOENT, unlock_image(&ioctx, oid, cookies[1]));
  ASSERT_EQ(-ENOENT, unlock_image(&ioctx, oid, cookies[0]));
  ASSERT_EQ(-ENOENT, break_lock(&ioctx, oid, our_entity, cookies[0]));
  // and make sure they still do that when somebody else does hold a lock
  ASSERT_EQ(0, lock_image_shared(&ioctx, oid, cookies[2]));
  ASSERT_EQ(-ENOENT, unlock_image(&ioctx, oid, cookies[1]));
  ASSERT_EQ(-ENOENT, unlock_image(&ioctx, oid, cookies[0]));
  ASSERT_EQ(-ENOENT, break_lock(&ioctx, oid, our_entity, cookies[0]));
  ASSERT_EQ(0, unlock_image(&ioctx, oid, cookies[2]));

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

TEST(cls_rbd, parents)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  parent_spec pspec;
  uint64_t size;

  ASSERT_EQ(-ENOENT, get_parent(&ioctx, "doesnotexist", CEPH_NOSNAP, &pspec, &size));

  // old image should fail
  ASSERT_EQ(0, create_image(&ioctx, "old", 33<<20, 22, 0, "old_blk."));
  // this next test is no longer valid after 56bc369983e197d29496eb75bc3f7b9b05d98bf6
  // ASSERT_EQ(-ENOEXEC, get_parent(&ioctx, "old", CEPH_NOSNAP, &pspec, &size));
  pspec = parent_spec(-1, "parent", 3);
  ASSERT_EQ(-ENOEXEC, set_parent(&ioctx, "old", parent_spec(-1, "parent", 3), 10<<20));
  ASSERT_EQ(-ENOEXEC, remove_parent(&ioctx, "old"));

  // new image will work
  ASSERT_EQ(0, create_image(&ioctx, "foo", 33<<20, 22, RBD_FEATURE_LAYERING, "foo."));

  ASSERT_EQ(0, get_parent(&ioctx, "foo", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 123, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);

  ASSERT_EQ(-EINVAL, set_parent(&ioctx, "foo", parent_spec(-1, "parent", 3), 10<<20));
  ASSERT_EQ(-EINVAL, set_parent(&ioctx, "foo", parent_spec(1, "", 3), 10<<20));
  ASSERT_EQ(-EINVAL, set_parent(&ioctx, "foo", parent_spec(1, "parent", CEPH_NOSNAP), 10<<20));
  ASSERT_EQ(-EINVAL, set_parent(&ioctx, "foo", parent_spec(1, "parent", 3), 0));

  pspec = parent_spec(1, "parent", 3);
  ASSERT_EQ(0, set_parent(&ioctx, "foo", pspec, 10<<20));
  ASSERT_EQ(-EEXIST, set_parent(&ioctx, "foo", pspec, 10<<20));
  ASSERT_EQ(-EEXIST, set_parent(&ioctx, "foo", parent_spec(2, "parent", 34), 10<<20));

  ASSERT_EQ(0, get_parent(&ioctx, "foo", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));

  ASSERT_EQ(0, remove_parent(&ioctx, "foo"));
  ASSERT_EQ(-ENOENT, remove_parent(&ioctx, "foo"));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);

  // snapshots
  ASSERT_EQ(0, set_parent(&ioctx, "foo", parent_spec(1, "parent", 3), 10<<20));
  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 10, "snap1"));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 10, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);

  ASSERT_EQ(0, remove_parent(&ioctx, "foo"));
  ASSERT_EQ(0, set_parent(&ioctx, "foo", parent_spec(4, "parent2", 6), 5<<20));
  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 11, "snap2"));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 10, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 11, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 4);
  ASSERT_EQ(pspec.image_id, "parent2");
  ASSERT_EQ(pspec.snap_id, snapid_t(6));
  ASSERT_EQ(size, 5ull<<20);

  ASSERT_EQ(0, remove_parent(&ioctx, "foo"));
  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 12, "snap3"));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 10, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 11, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 4);
  ASSERT_EQ(pspec.image_id, "parent2");
  ASSERT_EQ(pspec.snap_id, snapid_t(6));
  ASSERT_EQ(size, 5ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 12, &pspec, &size));
  ASSERT_EQ(-1, pspec.pool_id);

  // make sure set_parent takes min of our size and parent's size
  ASSERT_EQ(0, set_parent(&ioctx, "foo", parent_spec(1, "parent", 3), 1<<20));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 1ull<<20);
  ASSERT_EQ(0, remove_parent(&ioctx, "foo"));

  ASSERT_EQ(0, set_parent(&ioctx, "foo", parent_spec(1, "parent", 3), 100<<20));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 33ull<<20);
  ASSERT_EQ(0, remove_parent(&ioctx, "foo"));

  // make sure resize adjust parent overlap
  ASSERT_EQ(0, set_parent(&ioctx, "foo", parent_spec(1, "parent", 3), 10<<20));

  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 14, "snap4"));
  ASSERT_EQ(0, set_size(&ioctx, "foo", 3 << 20));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 3ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 14, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);

  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 15, "snap5"));
  ASSERT_EQ(0, set_size(&ioctx, "foo", 30 << 20));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 3ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 14, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 10ull<<20);
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 15, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 3ull<<20);

  ASSERT_EQ(0, set_size(&ioctx, "foo", 2 << 20));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", CEPH_NOSNAP, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 2ull<<20);

  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 16, "snap6"));
  ASSERT_EQ(0, get_parent(&ioctx, "foo", 16, &pspec, &size));
  ASSERT_EQ(pspec.pool_id, 1);
  ASSERT_EQ(pspec.image_id, "parent");
  ASSERT_EQ(pspec.snap_id, snapid_t(3));
  ASSERT_EQ(size, 2ull<<20);

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
  vector<parent_info> parents;

  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(0u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features, &parents));
  ASSERT_EQ(0u, snap_names.size());
  ASSERT_EQ(0u, snap_sizes.size());
  ASSERT_EQ(0u, snap_features.size());

  ASSERT_EQ(0, snapshot_add(&ioctx, "foo", 0, "snap1"));
  ASSERT_EQ(0, get_snapcontext(&ioctx, "foo", &snapc));
  ASSERT_EQ(1u, snapc.snaps.size());
  ASSERT_EQ(0u, snapc.snaps[0]);
  ASSERT_EQ(0u, snapc.seq);
  ASSERT_EQ(0, snapshot_list(&ioctx, "foo", snapc.snaps, &snap_names,
			     &snap_sizes, &snap_features, &parents));
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
			     &snap_sizes, &snap_features, &parents));
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
			     &snap_sizes, &snap_features, &parents));
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
			     &snap_sizes, &snap_features, &parents));
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
			     &snap_sizes, &snap_features, &parents));
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
			     &snap_sizes, &snap_features, &parents));
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
			     &snap_sizes, &snap_features, &parents));
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
			     &snap_sizes, &snap_features, &parents));
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
			     &snap_sizes, &snap_features, &parents));
  ASSERT_EQ(0u, snap_names.size());
  ASSERT_EQ(0u, snap_sizes.size());
  ASSERT_EQ(0u, snap_features.size());

  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}
