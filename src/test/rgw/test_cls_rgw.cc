// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/rgw/cls_rgw_client.h"

#include "gtest/gtest.h"
#include "test/rados-api/test.h"

#include <errno.h>
#include <string>
#include <vector>

static void create_obj(cls_rgw_obj& obj, int i, int j)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "-%d.%d", i, j);
  obj.pool = "pool";
  obj.pool.append(buf);
  obj.oid = "oid";
  obj.oid.append(buf);
  obj.key = "key";
  obj.key.append(buf);
}

static bool cmp_objs(cls_rgw_obj& obj1, cls_rgw_obj& obj2)
{
  return (obj1.pool == obj2.pool) &&
         (obj1.oid == obj2.oid) &&
         (obj1.key == obj2.key);
}


TEST(cls_rgw, set)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";
  for (int i = 0; i < 10; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;
    librados::ObjectWriteOperation op;
    cls_rgw_gc_obj_info info;

    cls_rgw_obj obj1, obj2;
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);
    info.chain.objs.push_back(obj1);
    info.chain.objs.push_back(obj2);

    op.create(false); // create object

    info.tag = tag;
    cls_rgw_gc_set_entry(op, 0, info);

    ASSERT_EQ(0, ioctx.operate(oid, &op));
  }

  bool truncated;
  list<cls_rgw_gc_obj_info> entries;
  string marker;

  /* list chains, verify truncated */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 8, entries, &truncated));
  ASSERT_EQ(8, (int)entries.size());
  ASSERT_EQ(1, truncated);

  entries.clear();

  /* list all chains, verify not truncated */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 10, entries, &truncated));
  ASSERT_EQ(10, (int)entries.size());
  ASSERT_EQ(0, truncated);
 
  /* verify all chains are valid */
  list<cls_rgw_gc_obj_info>::iterator iter = entries.begin();
  for (int i = 0; i < 10; i++, ++iter) {
    cls_rgw_gc_obj_info& entry = *iter;

    /* create expected chain name */
    char buf[32];
    snprintf(buf, sizeof(buf), "chain-%d", i);
    string tag = buf;

    /* verify chain name as expected */
    ASSERT_EQ(entry.tag, tag);

    /* verify expected num of objects in chain */
    ASSERT_EQ(2, (int)entry.chain.objs.size());

    list<cls_rgw_obj>::iterator oiter = entry.chain.objs.begin();
    cls_rgw_obj obj1, obj2;

    /* create expected objects */
    create_obj(obj1, i, 1);
    create_obj(obj2, i, 2);

    /* assign returned object names */
    cls_rgw_obj& ret_obj1 = *oiter++;
    cls_rgw_obj& ret_obj2 = *oiter;

    /* verify objects are as expected */
    ASSERT_EQ(1, (int)cmp_objs(obj1, ret_obj1));
    ASSERT_EQ(1, (int)cmp_objs(obj2, ret_obj2));
  }

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_rgw, defer)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  string oid = "obj";
  string tag = "mychain";

  librados::ObjectWriteOperation op;
  cls_rgw_gc_obj_info info;

  op.create(false);

  info.tag = tag;

  /* create chain */
  cls_rgw_gc_set_entry(op, 0, info);

  ASSERT_EQ(0, ioctx.operate(oid, &op));

  bool truncated;
  list<cls_rgw_gc_obj_info> entries;
  string marker;

  /* list chains, verify num entries as expected */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 1, entries, &truncated));
  ASSERT_EQ(1, (int)entries.size());
  ASSERT_EQ(0, truncated);

  librados::ObjectWriteOperation op2;

  /* defer chain */
  cls_rgw_gc_defer_entry(op2, 5, tag);
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  entries.clear();

  /* verify list doesn't show deferred entry (this may fail if cluster is thrashing) */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 1, entries, &truncated));
  ASSERT_EQ(0, (int)entries.size());
  ASSERT_EQ(0, truncated);

  /* wait enough */
  sleep(5);

  /* verify list shows deferred entry */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 1, entries, &truncated));
  ASSERT_EQ(1, (int)entries.size());
  ASSERT_EQ(0, truncated);

  librados::ObjectWriteOperation op3;
  list<string> tags;
  tags.push_back(tag);

  /* remove chain */
  cls_rgw_gc_remove(op3, tags);
  ASSERT_EQ(0, ioctx.operate(oid, &op3));

  entries.clear();

  /* verify entry was removed */
  ASSERT_EQ(0, cls_rgw_gc_list(ioctx, oid, marker, 1, entries, &truncated));
  ASSERT_EQ(0, (int)entries.size());
  ASSERT_EQ(0, truncated);

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

