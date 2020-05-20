// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "cls/cas/cls_cas_client.h"
#include "cls/cas/cls_cas_internal.h"

#include "include/utime.h"
#include "common/Clock.h"
#include "global/global_context.h"

#include "gtest/gtest.h"
#include "test/librados/test_cxx.h"

#include <errno.h>
#include <string>
#include <vector>


/// creates a temporary pool and initializes an IoCtx for each test
class cls_cas : public ::testing::Test {
  librados::Rados rados;
  std::string pool_name;
 protected:
  librados::IoCtx ioctx;

  void SetUp() {
    pool_name = get_temp_pool_name();
    /* create pool */
    ASSERT_EQ("", create_one_pool_pp(pool_name, rados));
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
  }
  void TearDown() {
    /* remove pool */
    ioctx.close();
    ASSERT_EQ(0, destroy_one_pool_pp(pool_name, rados));
  }
};

static librados::ObjectWriteOperation *new_op() {
  return new librados::ObjectWriteOperation();
}

/*
static librados::ObjectReadOperation *new_rop() {
  return new librados::ObjectReadOperation();
}
*/

TEST_F(cls_cas, get_put)
{
  bufferlist bl;
  bl.append("my data");
  string oid = "mychunk";
  hobject_t ref1, ref2, ref3;
  ref1.oid.name = "foo1";
  ref2.oid.name = "foo2";
  ref3.oid.name = "foo3";

  // initially the object does not exist
  bufferlist t;
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));

  // write
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref1, bl);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));
  
  // get x3
  {
    auto op = new_op();
    cls_cas_chunk_get_ref(*op, ref2);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));

  // get again
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref3, bl);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));

  // 3x puts to remove
  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref1);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));
  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref3);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));
  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref2);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));

  
  // get
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref1, bl);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));

  // put
  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref1);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));
}

TEST_F(cls_cas, wrong_put)
{
  bufferlist bl;
  bl.append("my data");
  string oid = "mychunk";
  hobject_t ref1, ref2;
  ref1.oid.name = "foo1";
  ref2.oid.name = "foo2";

  // initially the object does not exist
  bufferlist t;
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));

  // write
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref1, bl);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));

  // put wrong thing
  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref2);
    ASSERT_EQ(-ENOLINK, ioctx.operate(oid, op));
  }

  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref1);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));
}

TEST_F(cls_cas, dup_get)
{
  bufferlist bl;
  bl.append("my data");
  string oid = "mychunk";
  hobject_t ref1;
  ref1.oid.name = "foo1";

  // initially the object does not exist
  bufferlist t;
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));

  // write
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref1, bl);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));

  // dup create_or_get_ref, get_ref
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref1, bl);
    ASSERT_EQ(-EEXIST, ioctx.operate(oid, op));
  }
  {
    auto op = new_op();
    cls_cas_chunk_get_ref(*op, ref1);
    ASSERT_EQ(-EEXIST, ioctx.operate(oid, op));
  }

  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref1);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));
}

TEST_F(cls_cas, dup_put)
{
  bufferlist bl;
  bl.append("my data");
  string oid = "mychunk";
  hobject_t ref1;
  ref1.oid.name = "foo1";

  // initially the object does not exist
  bufferlist t;
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));

  // write
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref1, bl);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));

  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref1);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));

  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref1);
    ASSERT_EQ(-ENOENT, ioctx.operate(oid, op));
  }
}


TEST_F(cls_cas, get_wrong_data)
{
  bufferlist bl, bl2;
  bl.append("my data");
  bl2.append("my different data");
  string oid = "mychunk";
  hobject_t ref1, ref2;
  ref1.oid.name = "foo1";
  ref2.oid.name = "foo2";

  // initially the object does not exist
  bufferlist t;
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));

  // get
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref1, bl);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));

  // get w/ different data
  // with verify
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref2, bl2, true);
    ASSERT_EQ(-ENOMSG, ioctx.operate(oid, op));
  }
  // without verify
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref2, bl2, false);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));

  // put
  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref1);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(bl.length(), ioctx.read(oid, t, 0, 0));
  {
    auto op = new_op();
    cls_cas_chunk_put_ref(*op, ref2);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  ASSERT_EQ(-ENOENT, ioctx.read(oid, t, 0, 0));
}
