// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/refcount/cls_refcount_client.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include <errno.h>
#include <string>
#include <vector>

using namespace std;

static librados::ObjectWriteOperation *new_op() {
  return new librados::ObjectWriteOperation();
}

TEST(cls_refcount, test_implicit) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";
  string oldtag = "oldtag";
  string newtag = "newtag";


  /* get on a missing object will fail */
  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(-ENOENT, ioctx.operate(oid, op));
  delete op;

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  string wildcard_tag;
  string tag = refs.front();

  ASSERT_EQ(wildcard_tag, tag);

  /* take another reference, verify */
  op = new_op();
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(wildcard_tag));
  ASSERT_EQ(1, (int)refs_map.count(newtag));

  delete op;

  /* drop reference to oldtag */

  op = new_op();
  cls_refcount_put(*op, oldtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop oldtag reference again, op should return success, wouldn't do anything */

  op = new_op();
  cls_refcount_put(*op, oldtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop newtag reference, make sure object removed */
  op = new_op();
  cls_refcount_put(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

/*
 * similar to test_implicit, just changes the order of the tags removal
 * see issue #20107
 */
TEST(cls_refcount, test_implicit_idempotent) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";
  string oldtag = "oldtag";
  string newtag = "newtag";


  /* get on a missing object will fail */
  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(-ENOENT, ioctx.operate(oid, op));
  delete op;

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  string wildcard_tag;
  string tag = refs.front();

  ASSERT_EQ(wildcard_tag, tag);

  /* take another reference, verify */
  op = new_op();
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(wildcard_tag));
  ASSERT_EQ(1, (int)refs_map.count(newtag));

  delete op;

  /* drop reference to newtag */

  op = new_op();
  cls_refcount_put(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(string(), tag);

  delete op;

  /* drop newtag reference again, op should return success, wouldn't do anything */

  op = new_op();
  cls_refcount_put(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(string(), tag);

  delete op;

  /* drop oldtag reference, make sure object removed */
  op = new_op();
  cls_refcount_put(*op, oldtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}


TEST(cls_refcount, test_put_snap) {
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  bufferlist bl;
  bl.append("hi there");
  ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  ASSERT_EQ(0, ioctx.snap_create("snapfoo"));
  ASSERT_EQ(0, ioctx.remove("foo"));

  sleep(2);

  ASSERT_EQ(0, ioctx.snap_create("snapbar"));

  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_put(*op, "notag", true);
  ASSERT_EQ(-ENOENT, ioctx.operate("foo", op));

  EXPECT_EQ(0, ioctx.snap_remove("snapfoo"));
  EXPECT_EQ(0, ioctx.snap_remove("snapbar"));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_explicit) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";


  /* create object */

  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(0, (int)refs.size());


  /* take first reference, verify */

  string newtag = "newtag";

  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_get(*op, newtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(newtag));

  delete op;

  /* try to drop reference to unexisting tag */

  string nosuchtag = "nosuchtag";

  op = new_op();
  cls_refcount_put(*op, nosuchtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  string tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop newtag reference, make sure object removed */
  op = new_op();
  cls_refcount_put(*op, newtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, set) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";


  /* create object */

  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> tag_refs, refs;

#define TAGS_NUM 5
  string tags[TAGS_NUM];

  char buf[16];
  for (int i = 0; i < TAGS_NUM; i++) {
    snprintf(buf, sizeof(buf), "tag%d", i);
    tags[i] = buf;
    tag_refs.push_back(tags[i]);
  }

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(0, (int)refs.size());

  /* set reference list, verify */

  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_set(*op, tag_refs);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(TAGS_NUM, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  for (int i = 0; i < TAGS_NUM; i++) {
    ASSERT_EQ(1, (int)refs_map.count(tags[i]));
  }

  delete op;

  /* remove all refs */

  for (int i = 0; i < TAGS_NUM; i++) {
    op = new_op();
    cls_refcount_put(*op, tags[i]);
    ASSERT_EQ(0, ioctx.operate(oid, op));
    delete op;
  }

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_implicit_ec) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";
  string oldtag = "oldtag";
  string newtag = "newtag";


  /* get on a missing object will fail */
  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(-ENOENT, ioctx.operate(oid, op));
  delete op;

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  string wildcard_tag;
  string tag = refs.front();

  ASSERT_EQ(wildcard_tag, tag);

  /* take another reference, verify */
  op = new_op();
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(wildcard_tag));
  ASSERT_EQ(1, (int)refs_map.count(newtag));

  delete op;

  /* drop reference to oldtag */

  op = new_op();
  cls_refcount_put(*op, oldtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop oldtag reference again, op should return success, wouldn't do anything */

  op = new_op();
  cls_refcount_put(*op, oldtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop newtag reference, make sure object removed */
  op = new_op();
  cls_refcount_put(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}

/*
 * similar to test_implicit, just changes the order of the tags removal
 * see issue #20107
 */
TEST(cls_refcount, test_implicit_idempotent_ec) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";
  string oldtag = "oldtag";
  string newtag = "newtag";


  /* get on a missing object will fail */
  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(-ENOENT, ioctx.operate(oid, op));
  delete op;

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  string wildcard_tag;
  string tag = refs.front();

  ASSERT_EQ(wildcard_tag, tag);

  /* take another reference, verify */
  op = new_op();
  cls_refcount_get(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(wildcard_tag));
  ASSERT_EQ(1, (int)refs_map.count(newtag));

  delete op;

  /* drop reference to newtag */

  op = new_op();
  cls_refcount_put(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(string(), tag);

  delete op;

  /* drop newtag reference again, op should return success, wouldn't do anything */

  op = new_op();
  cls_refcount_put(*op, newtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs, true));
  ASSERT_EQ(1, (int)refs.size());

  tag = refs.front();
  ASSERT_EQ(string(), tag);

  delete op;

  /* drop oldtag reference, make sure object removed */
  op = new_op();
  cls_refcount_put(*op, oldtag, true);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}


TEST(cls_refcount, test_put_snap_ec) {
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  bufferlist bl;
  bl.append("hi there");
  ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  ASSERT_EQ(0, ioctx.snap_create("snapfoo"));
  ASSERT_EQ(0, ioctx.remove("foo"));

  sleep(2);

  ASSERT_EQ(0, ioctx.snap_create("snapbar"));

  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_put(*op, "notag", true);
  ASSERT_EQ(-ENOENT, ioctx.operate("foo", op));

  EXPECT_EQ(0, ioctx.snap_remove("snapfoo"));
  EXPECT_EQ(0, ioctx.snap_remove("snapbar"));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_explicit_ec) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";


  /* create object */

  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(0, (int)refs.size());


  /* take first reference, verify */

  string newtag = "newtag";

  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_get(*op, newtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(newtag));

  delete op;

  /* try to drop reference to unexisting tag */

  string nosuchtag = "nosuchtag";

  op = new_op();
  cls_refcount_put(*op, nosuchtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  string tag = refs.front();
  ASSERT_EQ(newtag, tag);

  delete op;

  /* drop newtag reference, make sure object removed */
  op = new_op();
  cls_refcount_put(*op, newtag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}

TEST(cls_refcount, set_ec) /* test refcount using implicit referencing of newly created objects */
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";


  /* create object */

  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> tag_refs, refs;

#define TAGS_NUM 5
  string tags[TAGS_NUM];

  char buf[16];
  for (int i = 0; i < TAGS_NUM; i++) {
    snprintf(buf, sizeof(buf), "tag%d", i);
    tags[i] = buf;
    tag_refs.push_back(tags[i]);
  }

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(0, (int)refs.size());

  /* set reference list, verify */

  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_set(*op, tag_refs);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(TAGS_NUM, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  for (int i = 0; i < TAGS_NUM; i++) {
    ASSERT_EQ(1, (int)refs_map.count(tags[i]));
  }

  delete op;

  /* remove all refs */

  for (int i = 0; i < TAGS_NUM; i++) {
    op = new_op();
    cls_refcount_put(*op, tags[i]);
    ASSERT_EQ(0, ioctx.operate(oid, op));
    delete op;
  }

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}
