// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

TEST(cls_refcount, test_idempotent)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* add chains */
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";


  /* get on a missing object will fail */
  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_get(*op, tgt_tag, src_tag);
  ASSERT_EQ(-ENOENT, ioctx.operate(oid, op));
  delete op;

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* read reference, should return a single wildcard entry */

  list<string> refs;

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(0, (int)refs.size());

  /* take reference, verify */
  op = new_op();
  cls_refcount_get(*op, tgt_tag, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));

  delete op;

  /* take reference to tgt_tag again, op should return success, wouldn't do anything */
  op = new_op();
  cls_refcount_get(*op, tgt_tag, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));

  delete op;


  /* drop reference to tgt_tag */

  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  ASSERT_EQ(src_tag, refs.front());

  delete op;

  /* drop tgt_tag reference again, op should return success, wouldn't do anything */

  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  ASSERT_EQ(src_tag, refs.front());
  delete op;

  /* take reference to src_tag again, op should return success, wouldn't do anything */
  op = new_op();
  // can't pass the same tag for src and tgt so use dummy_tag
  // the dummy_tag will be ignored because the object ref_set is not empty
  cls_refcount_get(*op, src_tag, "dummy_tag");
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());

  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(src_tag));

  delete op;

  /* drop src_tag reference, make sure object removed */
  op = new_op();
  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_bad_parms)
{
  string tag = "tag";
  string empty_tag = "";
  librados::ObjectWriteOperation op;

  // empty tags are illegal
  ASSERT_EQ(-EINVAL, cls_refcount_get(op, empty_tag, empty_tag));

  // empty tags are illegal
  ASSERT_EQ(-EINVAL, cls_refcount_get(op, tag, empty_tag));

  // empty tags are illegal
  ASSERT_EQ(-EINVAL, cls_refcount_get(op, empty_tag, tag));

  // tags must be different
  ASSERT_EQ(-EINVAL, cls_refcount_get(op, tag, tag));

  // empty tags are illegal
  ASSERT_EQ(-EINVAL, cls_refcount_put(op, empty_tag));

  list<string> tag_refs;
  tag_refs.push_back("foo");
  tag_refs.push_back(empty_tag);
  tag_refs.push_back("bar");

  // empty tags are illegal
  ASSERT_EQ(-EINVAL, cls_refcount_set(op, tag_refs));
}

TEST(cls_refcount, test_set_bad_params)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string tag = "tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  librados::ObjectWriteOperation op;
  list<string> tag_refs;
  tag_refs.push_back("foo");
  tag_refs.push_back("bar");
  tag_refs.push_back("foo");

  // we don't check the full list before sending the CLS
  ASSERT_EQ(0, cls_refcount_set(op, tag_refs));
  // tag "foo" is repeated
  ASSERT_EQ(-EEXIST, ioctx.operate(oid, &op));

  // force delete with a single put since no tag was set
  librados::ObjectWriteOperation op2;
  cls_refcount_put(op2, "notag");
  ASSERT_EQ(0, ioctx.operate(oid, &op2));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_put_without_get)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string tag = "tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* call put on an object without a ref_count */
  librados::ObjectWriteOperation *op = new_op();

  cls_refcount_put(*op, tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  // the put operation deleted the object since it had no active ref_tag
  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_put_without_get_ec)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string tag = "tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* call put on an object without a ref_count */
  librados::ObjectWriteOperation *op = new_op();

  cls_refcount_put(*op, tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  // the put operation deleted the object since it had no active ref_tag
  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_get_with_src_tag_1)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* take a reference to tgt_tag adding src_tag on-the-fly */
  librados::ObjectWriteOperation *op = new_op();

  /* call cls_refcount_get() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_get(*op, tgt_tag, ""));

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  list<string> refs;
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop reference to tgt_tag */
  op = new_op();
  /* call cls_refcount_put() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_put(*op, ""));
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop src_tag reference and make sure the obj was deleted */
  op = new_op();
  /* call cls_refcount_put() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_put(*op, ""));

  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_get_with_src_tag_1_ec)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* take a reference to tgt_tag adding src_tag on-the-fly */
  librados::ObjectWriteOperation *op = new_op();
  /* call cls_refcount_get() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_get(*op, tgt_tag, ""));

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  list<string> refs;
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop reference to tgt_tag */
  op = new_op();
  /* call cls_refcount_put() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_put(*op, ""));

  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop src_tag reference and make sure the obj was deleted */
  op = new_op();
  /* call cls_refcount_put() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_put(*op, ""));

  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_get_with_src_tag_2)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* take a reference to tgt_tag adding src_tag on-the-fly */
  librados::ObjectWriteOperation *op = new_op();
  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  list<string> refs;
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop reference to src_tag */
  op = new_op();
  /* call cls_refcount_put() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_put(*op, ""));

  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  delete op;

  /* drop tgt_tag reference and make sure the obj was deleted */
  op = new_op();
  /* call cls_refcount_put() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_put(*op, ""));

  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_get_with_src_tag_2_ec)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* take a reference to tgt_tag adding src_tag on-the-fly */
  librados::ObjectWriteOperation *op = new_op();
  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  list<string> refs;
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop reference to src_tag */
  op = new_op();
  /* call cls_refcount_put() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_put(*op, ""));

  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  delete op;

  /* drop tgt_tag reference and make sure the obj was deleted */
  op = new_op();
  /* call cls_refcount_put() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_put(*op, ""));

  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_second_get_with_src_tag)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* take a reference to tgt_tag adding src_tag on-the-fly */
  librados::ObjectWriteOperation *op = new_op();
  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  list<string> refs;
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  // Try adding new tgt/src tags on a populated object
  // The new tgt_tag will be accepted while the new src_tag will be ignored
  op = new_op();
  string src_tag2 = "src_tag2";
  string tgt_tag2 = "tgt_tag2";

  /* call cls_refcount_get() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_get(*op, tgt_tag, ""));

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag2, src_tag2));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(3, (int)refs.size());

  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag2));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop reference to src_tag2 (which should not exist) */
  op = new_op();
  cls_refcount_put(*op, src_tag2);
  ASSERT_EQ(-EINVAL, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(3, (int)refs.size());

  delete op;

  /* drop reference to tgt_tag */
  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag2));
  delete op;

  /* drop again reference to tgt_tag (NOP) */
  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag2));
  delete op;

  /* drop reference to tgt_tag2 */
  op = new_op();
  cls_refcount_put(*op, tgt_tag2);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop src_tag reference and make sure the obj was deleted */
  op = new_op();
  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_second_get_with_src_tag_ec)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* take a reference to tgt_tag adding src_tag on-the-fly */
  librados::ObjectWriteOperation *op = new_op();
  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  list<string> refs;
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  // Try adding new tgt/src tags on a populated object
  // The new tgt_tag will be accepted while the new src_tag will be ignored
  op = new_op();
  string src_tag2 = "src_tag2";
  string tgt_tag2 = "tgt_tag2";
  /* call cls_refcount_get() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_get(*op, tgt_tag, ""));

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag2, src_tag2));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(3, (int)refs.size());

  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag2));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop reference to src_tag2 (which should not exist) */
  op = new_op();
  cls_refcount_put(*op, src_tag2);
  ASSERT_EQ(-EINVAL, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(3, (int)refs.size());

  delete op;

  /* drop reference to tgt_tag */
  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag2));
  delete op;

  /* drop again reference to tgt_tag (NOP) */
  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  ASSERT_EQ(1, (int)refs_map.count(tgt_tag2));
  delete op;

  /* drop reference to tgt_tag2 */
  op = new_op();
  cls_refcount_put(*op, tgt_tag2);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  refs.clear();
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop src_tag reference and make sure the obj was deleted */
  op = new_op();
  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_get_put_get)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* take a reference to tgt_tag adding src_tag on-the-fly */
  librados::ObjectWriteOperation *op = new_op();
  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  list<string> refs;
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop reference to tgt_tag */
  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* take again a reference to tgt_tag */
  op = new_op();

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* take a reference to tgt_tag on top of existing tgt_tag */
  op = new_op();

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop tag reference again */
  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop src_tag reference and make sure the obj was deleted */
  op = new_op();
  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
}

TEST(cls_refcount, test_get_put_get_ec)
{
  librados::Rados rados;
  librados::IoCtx ioctx;
  string oid = "obj";
  string src_tag = "src_tag";
  string tgt_tag = "tgt_tag";
  string pool_name = get_temp_pool_name();

  /* create pool */
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, rados));
  ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));

  /* create object */
  ASSERT_EQ(0, ioctx.create(oid, true));

  /* take a reference to tgt_tag adding src_tag on-the-fly */
  librados::ObjectWriteOperation *op = new_op();
  /* call cls_refcount_get() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_get(*op, tgt_tag, ""));

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  list<string> refs;
  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  map<string, bool> refs_map;
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop reference to tgt_tag */
  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* take again a reference to tgt_tag */
  op = new_op();
  /* call cls_refcount_get() with an empty src_tag*/
  ASSERT_EQ(-EINVAL, cls_refcount_get(*op, tgt_tag, ""));

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* take a reference to tgt_tag on top of existing tgt_tag */
  op = new_op();

  ASSERT_EQ(0, cls_refcount_get(*op, tgt_tag, src_tag));
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(2, (int)refs.size());

  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }

  ASSERT_EQ(1, (int)refs_map.count(tgt_tag));
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop tag reference again */
  op = new_op();
  cls_refcount_put(*op, tgt_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_refcount_read(ioctx, oid, &refs));
  ASSERT_EQ(1, (int)refs.size());
  refs_map.clear();
  for (list<string>::iterator iter = refs.begin(); iter != refs.end(); ++iter) {
    refs_map[*iter] = true;
  }
  ASSERT_EQ(1, (int)refs_map.count(src_tag));
  delete op;

  /* drop src_tag reference and make sure the obj was deleted */
  op = new_op();
  cls_refcount_put(*op, src_tag);
  ASSERT_EQ(0, ioctx.operate(oid, op));
  delete op;

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, rados));
}

TEST(cls_refcount, set)
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

TEST(cls_refcount, set2)
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

  /* remove all refs using an empty set */
  tag_refs.clear();
  op = new_op();
  cls_refcount_set(*op, tag_refs);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(-ENOENT, ioctx.stat(oid, NULL, NULL));

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

  // object "foo" was removed so we should fail with ENOENT
  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_put(*op, "notag");
  ASSERT_EQ(-ENOENT, ioctx.operate("foo", op));

  EXPECT_EQ(0, ioctx.snap_remove("snapfoo"));
  EXPECT_EQ(0, ioctx.snap_remove("snapbar"));

  delete op;

  /* remove pool */
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
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

  // object "foo" was removed so we should fail with ENOENT
  librados::ObjectWriteOperation *op = new_op();
  cls_refcount_put(*op, "notag");
  ASSERT_EQ(-ENOENT, ioctx.operate("foo", op));

  EXPECT_EQ(0, ioctx.snap_remove("snapfoo"));
  EXPECT_EQ(0, ioctx.snap_remove("snapbar"));

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

