// Tests for the C API coverage of atomic write operations

#include <errno.h>
#include "include/rados/librados.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"

TEST(LibradosCWriteOps, NewDelete) {
  rados_write_op_t op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_release_write_op(op);
}

TEST(LibRadosCWriteOps, assertExists) {
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rados_write_op_t op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_assert_exists(op);

  // -2, ENOENT
  ASSERT_EQ(-2, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);

  rados_write_op_t op2 = rados_create_write_op();
  ASSERT_TRUE(op2);
  rados_write_op_assert_exists(op2);

  rados_completion_t completion;
  ASSERT_EQ(0, rados_aio_create_completion(NULL, NULL, NULL, &completion));
  ASSERT_EQ(0, rados_aio_write_op_operate(op2, ioctx, completion, "test", NULL, 0));
  rados_aio_wait_for_complete(completion);
  ASSERT_EQ(-2, rados_aio_get_return_value(completion));
  rados_aio_release(completion);

  rados_ioctx_destroy(ioctx);
  rados_release_write_op(op2);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosCWriteOps, WriteOpAssertVersion) {
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  rados_write_op_t op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_create(op, LIBRADOS_CREATE_EXCLUSIVE, NULL);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);

  // Write to the object a second time to guarantee that its
  // version number is greater than 0
  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_write_full(op, "hi", 2);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);

  uint64_t v = rados_get_last_version(ioctx);

  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_assert_version(op, v+1);
  ASSERT_EQ(-EOVERFLOW, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);

  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_assert_version(op, v-1);
  ASSERT_EQ(-ERANGE, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);

  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_assert_version(op, v);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosCWriteOps, Xattrs) {
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  // Create an object with an xattr
  rados_write_op_t op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_create(op, LIBRADOS_CREATE_EXCLUSIVE, NULL);
  rados_write_op_setxattr(op, "key", "value", 5);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);

  // Check that xattr exists, if it does, delete it.
  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_create(op, LIBRADOS_CREATE_IDEMPOTENT, NULL);
  rados_write_op_cmpxattr(op, "key", LIBRADOS_CMPXATTR_OP_EQ, "value", 5);
  rados_write_op_rmxattr(op, "key");
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);

  // Check the xattr exits, if it does, add it again (will fail) with -125
  // (ECANCELED)
  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_cmpxattr(op, "key", LIBRADOS_CMPXATTR_OP_EQ, "value", 5);
  rados_write_op_setxattr(op, "key", "value", 5);
  ASSERT_EQ(-ECANCELED, rados_write_op_operate(op, ioctx, "test", NULL, 0));

  rados_release_write_op(op);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosCWriteOps, Write) {
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  // Create an object, write and write full to it
  rados_write_op_t op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_create(op, LIBRADOS_CREATE_EXCLUSIVE, NULL);
  rados_write_op_write(op, "four", 4, 0);
  rados_write_op_write_full(op, "hi", 2);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  char hi[4];
  ASSERT_EQ(2, rados_read(ioctx, "test", hi, 4, 0));
  rados_release_write_op(op);

  //create write op with iohint
  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_write_full(op, "ceph", 4);
  rados_write_op_set_flags(op, LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  ASSERT_EQ(4, rados_read(ioctx, "test", hi, 4, 0));
  rados_release_write_op(op);

  // Truncate and append
  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_truncate(op, 1);
  rados_write_op_append(op, "hi", 2);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  ASSERT_EQ(3, rados_read(ioctx, "test", hi, 4, 0));
  rados_release_write_op(op);

  // zero and remove
  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_zero(op, 0, 3);
  rados_write_op_remove(op);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  // ENOENT
  ASSERT_EQ(-2, rados_read(ioctx, "test", hi, 4, 0));
  rados_release_write_op(op);

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosCWriteOps, Exec) {
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  int rval = 1;
  rados_write_op_t op = rados_create_write_op();
  rados_write_op_exec(op, "hello", "record_hello", "test", 4, &rval);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  rados_release_write_op(op);
  ASSERT_EQ(0, rval);

  char hi[100];
  ASSERT_EQ(12, rados_read(ioctx, "test", hi, 100, 0));
  hi[12] = '\0';
  ASSERT_EQ(0, strcmp("Hello, test!", hi));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosCWriteOps, WriteSame) {
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);

  // Create an object, write to it using writesame
  rados_write_op_t op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_create(op, LIBRADOS_CREATE_EXCLUSIVE, NULL);
  rados_write_op_writesame(op, "four", 4, 4 * 4, 0);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));
  char hi[4 * 4];
  ASSERT_EQ(sizeof(hi), rados_read(ioctx, "test", hi, sizeof(hi), 0));
  rados_release_write_op(op);
  ASSERT_EQ(0, memcmp("fourfourfourfour", hi, sizeof(hi)));

  // cleanup
  op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_remove(op);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "test", NULL, 0));

  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}
