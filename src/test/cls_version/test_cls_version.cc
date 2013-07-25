// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/version/cls_version_types.h"
#include "cls/version/cls_version_client.h"

#include "gtest/gtest.h"
#include "test/librados/test.h"

#include <errno.h>
#include <string>
#include <vector>

static librados::ObjectWriteOperation *new_op() {
  return new librados::ObjectWriteOperation();
}

static librados::ObjectReadOperation *new_rop() {
  return new librados::ObjectReadOperation();
}

TEST(cls_rgw, test_version_inc_read)
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

  obj_version ver;

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver));
  ASSERT_EQ(0, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver.tag.size());
  

  /* inc version */
  librados::ObjectWriteOperation *op = new_op();
  cls_version_inc(*op);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver));
  ASSERT_GT((long long)ver.ver, 0);
  ASSERT_NE(0, (int)ver.tag.size());

  /* inc version again! */
  delete op;
  op = new_op();
  cls_version_inc(*op);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  obj_version ver2;

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver2));
  ASSERT_GT((long long)ver2.ver, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver2.tag.compare(ver.tag));

  delete op;

  obj_version ver3;

  librados::ObjectReadOperation *rop = new_rop();
  cls_version_read(*rop, &ver3);
  bufferlist outbl;
  ASSERT_EQ(0, ioctx.operate(oid, rop, &outbl));
  ASSERT_EQ(ver2.ver, ver3.ver);
  ASSERT_EQ(1, (long long)ver2.compare(&ver3));

  delete rop;
}


TEST(cls_rgw, test_version_set)
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

  obj_version ver;

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver));
  ASSERT_EQ(0, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver.tag.size());


  ver.ver = 123;
  ver.tag = "foo";

  /* set version */
  librados::ObjectWriteOperation *op = new_op();
  cls_version_set(*op, ver);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  /* read version */
  obj_version ver2;

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver2));
  ASSERT_EQ((long long)ver2.ver, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver2.tag.compare(ver.tag));

  delete op;
}

TEST(cls_rgw, test_version_inc_cond)
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

  obj_version ver;

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver));
  ASSERT_EQ(0, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver.tag.size());
  
  /* inc version */
  librados::ObjectWriteOperation *op = new_op();
  cls_version_inc(*op);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver));
  ASSERT_GT((long long)ver.ver, 0);
  ASSERT_NE(0, (int)ver.tag.size());

  obj_version cond_ver = ver;


  /* inc version again! */
  delete op;
  op = new_op();
  cls_version_inc(*op);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  obj_version ver2;

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver2));
  ASSERT_GT((long long)ver2.ver, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver2.tag.compare(ver.tag));


  /* now check various condition tests */
  cls_version_inc(*op, cond_ver, VER_COND_NONE);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver2));
  ASSERT_GT((long long)ver2.ver, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver2.tag.compare(ver.tag));

  /* a bunch of conditions that should fail */
  delete op;
  op = new_op();
  cls_version_inc(*op, cond_ver, VER_COND_EQ);
  ASSERT_EQ(-ECANCELED, ioctx.operate(oid, op));

  delete op;
  op = new_op();
  cls_version_inc(*op, cond_ver, VER_COND_LT);
  ASSERT_EQ(-ECANCELED, ioctx.operate(oid, op));

  delete op;
  op = new_op();
  cls_version_inc(*op, cond_ver, VER_COND_LE);
  ASSERT_EQ(-ECANCELED, ioctx.operate(oid, op));

  delete op;
  op = new_op();
  cls_version_inc(*op, cond_ver, VER_COND_TAG_NE);
  ASSERT_EQ(-ECANCELED, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver2));
  ASSERT_GT((long long)ver2.ver, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver2.tag.compare(ver.tag));

  /* a bunch of conditions that should succeed */
  delete op;
  op = new_op();
  cls_version_inc(*op, ver2, VER_COND_EQ);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  delete op;
  op = new_op();
  cls_version_inc(*op, cond_ver, VER_COND_GT);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  delete op;
  op = new_op();
  cls_version_inc(*op, cond_ver, VER_COND_GE);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  delete op;
  op = new_op();
  cls_version_inc(*op, cond_ver, VER_COND_TAG_EQ);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  delete op;
}

TEST(cls_rgw, test_version_inc_check)
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

  obj_version ver;

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver));
  ASSERT_EQ(0, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver.tag.size());
  
  /* inc version */
  librados::ObjectWriteOperation *op = new_op();
  cls_version_inc(*op);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver));
  ASSERT_GT((long long)ver.ver, 0);
  ASSERT_NE(0, (int)ver.tag.size());

  obj_version cond_ver = ver;

  /* a bunch of conditions that should succeed */
  librados::ObjectReadOperation *rop = new_rop();
  cls_version_check(*rop, cond_ver, VER_COND_EQ);
  bufferlist bl;
  ASSERT_EQ(0, ioctx.operate(oid, rop, &bl));

  delete rop;
  rop = new_rop();
  cls_version_check(*rop, cond_ver, VER_COND_GE);
  ASSERT_EQ(0, ioctx.operate(oid, rop, &bl));

  delete rop;
  rop = new_rop();
  cls_version_check(*rop, cond_ver, VER_COND_LE);
  ASSERT_EQ(0, ioctx.operate(oid, rop, &bl));

  delete rop;
  rop = new_rop();
  cls_version_check(*rop, cond_ver, VER_COND_TAG_EQ);
  ASSERT_EQ(0, ioctx.operate(oid, rop, &bl));

  obj_version ver2;

  delete op;
  op = new_op();
  cls_version_inc(*op);
  ASSERT_EQ(0, ioctx.operate(oid, op));

  ASSERT_EQ(0, cls_version_read(ioctx, oid, &ver2));
  ASSERT_GT((long long)ver2.ver, (long long)ver.ver);
  ASSERT_EQ(0, (int)ver2.tag.compare(ver.tag));

  delete op;

  /* a bunch of conditions that should fail */
  delete rop;
  rop = new_rop();
  cls_version_check(*rop, ver, VER_COND_LT);
  ASSERT_EQ(-ECANCELED, ioctx.operate(oid, rop, &bl));

  delete rop;
  rop = new_rop();
  cls_version_check(*rop, cond_ver, VER_COND_LE);
  ASSERT_EQ(-ECANCELED, ioctx.operate(oid, rop, &bl));

  delete rop;
  rop = new_rop();
  cls_version_check(*rop, cond_ver, VER_COND_TAG_NE);
  ASSERT_EQ(-ECANCELED, ioctx.operate(oid, rop, &bl));

  delete rop;
}
