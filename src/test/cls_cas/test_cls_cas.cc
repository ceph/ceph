// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "include/stringify.h"
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

  // dup create_or_get_ref, get_ref will succeed but take no additional ref.
  {
    auto op = new_op();
    cls_cas_chunk_create_or_get_ref(*op, ref1, bl);
    ASSERT_EQ(0, ioctx.operate(oid, op));
  }
  {
    auto op = new_op();
    cls_cas_chunk_get_ref(*op, ref1);
    ASSERT_EQ(0, ioctx.operate(oid, op));
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

static int count_bits(unsigned long n)
{
    // base case
    if (n == 0)
        return 0;
    else
        return 1 + count_bits(n & (n - 1));
}

TEST(chunk_refs_t, size)
{
  chunk_refs_t r;
  size_t max = 1048576;

  // mix in pool changes as i gets bigger
  size_t pool_mask = 0xfff5110;

  // eventually add in a zillion different pools to force us to a raw count
  size_t pool_cutoff = max/2;

  for (size_t i = 1; i <= max; ++i) {
    hobject_t h(sobject_t(object_t("foo"s + stringify(i)), i));
    h.pool = i > pool_cutoff ? i : (i & pool_mask);
    r.get(h);
    if (count_bits(i) <= 2) {
      bufferlist bl;
      r.dynamic_encode(bl, 512);
      if (count_bits(i) == 1) {
	cout << i << "\t" << bl.length()
	     << "\t" << r.describe_encoding()
	     << std::endl;
      }

      // verify reencoding is correct
      chunk_refs_t a;
      auto t = bl.cbegin();
      decode(a, t);
      bufferlist bl2;
      encode(a, bl2);
      if (!bl.contents_equal(bl2)) {
	Formatter *f = Formatter::create("json-pretty");
	cout << "original:\n";
	f->dump_object("refs", r);
	f->flush(cout);
	cout << "decoded:\n";
	f->dump_object("refs", a);
	f->flush(cout);
	cout << "original encoding:\n";
	bl.hexdump(cout);
	cout << "decoded re-encoding:\n";
	bl2.hexdump(cout);
	ASSERT_TRUE(bl.contents_equal(bl2));
      }
    }
  }
  ASSERT_EQ(max, r.count());
  for (size_t i = 1; i <= max; ++i) {
    hobject_t h(sobject_t(object_t("foo"s + stringify(i)), 1));
    h.pool = i > pool_cutoff ? i : (i & pool_mask);
    bool ret = r.put(h);
    ASSERT_TRUE(ret);
  }
  ASSERT_EQ(0, r.count());
}
