// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// Tests for the C API coverage of atomic read operations

#include <errno.h>
#include <string>

#include "include/rados/librados.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

const char *data = "testdata";
const char *obj = "testobj";
const int len = strlen(data);

class CReadOpsTest : public RadosTest {
protected:
  void write_object() {
    // Create an object and write to it
    ASSERT_EQ(len, rados_write(ioctx, obj, data, len, 0));
  }
  void remove_object() {
    ASSERT_EQ(0, rados_remove(ioctx, obj));
  }
};

TEST_F(CReadOpsTest, NewDelete) {
  rados_read_op_t op = rados_create_read_op();
  ASSERT_TRUE(op);
  rados_release_read_op(op);
}

TEST_F(CReadOpsTest, SetOpFlags) {
  write_object();

  rados_read_op_t op = rados_create_read_op();
  size_t bytes_read = 0;
  char *out = NULL;
  int rval = 0;
  rados_read_op_exec(op, "rbd", "get_id", NULL, 0, &out,
		     &bytes_read, &rval);
  rados_read_op_set_flags(op, LIBRADOS_OP_FLAG_FAILOK);
  EXPECT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  EXPECT_EQ(0, rval);
  EXPECT_EQ(0u, bytes_read);
  EXPECT_EQ((char*)NULL, out);
  rados_release_read_op(op);

  remove_object();
}

TEST_F(CReadOpsTest, AssertExists) {
  rados_read_op_t op = rados_create_read_op();
  rados_read_op_assert_exists(op);

  ASSERT_EQ(-ENOENT, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);

  op = rados_create_read_op();
  rados_read_op_assert_exists(op);

  rados_completion_t completion;
  ASSERT_EQ(0, rados_aio_create_completion(NULL, NULL, NULL, &completion));
  ASSERT_EQ(0, rados_aio_read_op_operate(op, ioctx, completion, obj, 0));
  rados_aio_wait_for_complete(completion);
  ASSERT_EQ(-ENOENT, rados_aio_get_return_value(completion));
  rados_release_read_op(op);

  write_object();

  op = rados_create_read_op();
  rados_read_op_assert_exists(op);
  ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);

  remove_object();
}

TEST_F(CReadOpsTest, Stat) {
  rados_read_op_t op = rados_create_read_op();
  uint64_t size = 1;
  int rval;
  rados_read_op_stat(op, &size, NULL, &rval);
  EXPECT_EQ(-ENOENT, rados_read_op_operate(op, ioctx, obj, 0));
  EXPECT_EQ(-EIO, rval);
  EXPECT_EQ(1u, size);
  rados_release_read_op(op);

  write_object();

  op = rados_create_read_op();
  rados_read_op_stat(op, &size, NULL, &rval);
  EXPECT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  EXPECT_EQ(0, rval);
  EXPECT_EQ(len, (int)size);
  rados_release_read_op(op);

  op = rados_create_read_op();
  rados_read_op_stat(op, NULL, NULL, NULL);
  EXPECT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);

  remove_object();

  op = rados_create_read_op();
  rados_read_op_stat(op, NULL, NULL, NULL);
  EXPECT_EQ(-ENOENT, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);
}
