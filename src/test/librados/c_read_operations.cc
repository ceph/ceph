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
    ASSERT_EQ(0, rados_write(ioctx, obj, data, len, 0));
  }
  void remove_object() {
    ASSERT_EQ(0, rados_remove(ioctx, obj));
  }
  int cmp_xattr(const char *xattr, const char *value, size_t value_len,
		uint8_t cmp_op)
  {
    rados_read_op_t op = rados_create_read_op();
    rados_read_op_cmpxattr(op, xattr, cmp_op, value, value_len);
    int r = rados_read_op_operate(op, ioctx, obj, 0);
    rados_release_read_op(op);
    return r;
  }

  void fetch_and_verify_omap_vals(char const* const* keys,
				  char const* const* vals,
				  const size_t *lens,
				  size_t len)
  {
    rados_omap_iter_t iter_vals, iter_keys, iter_vals_by_key;
    int r_vals, r_keys, r_vals_by_key;
    rados_read_op_t op = rados_create_read_op();
    rados_read_op_omap_get_vals(op, NULL, NULL, 100, &iter_vals, &r_vals);
    rados_read_op_omap_get_keys(op, NULL, 100, &iter_keys, &r_keys);
    rados_read_op_omap_get_vals_by_keys(op, keys, len,
					&iter_vals_by_key, &r_vals_by_key);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    rados_release_read_op(op);
    ASSERT_EQ(0, r_vals);
    ASSERT_EQ(0, r_keys);
    ASSERT_EQ(0, r_vals_by_key);

    const char *zeros[len];
    size_t zero_lens[len];
    memset(zeros, 0, len);
    memset(zero_lens, 0, len * sizeof(size_t));
    compare_omap_vals(keys, vals, lens, len, iter_vals);
    compare_omap_vals(keys, zeros, zero_lens, len, iter_keys);
    compare_omap_vals(keys, vals, lens, len, iter_vals_by_key);
  }

  void compare_omap_vals(char const* const* keys,
			 char const* const* vals,
			 const size_t *lens,
			 size_t len,
			 rados_omap_iter_t iter)
  {
    size_t i = 0;
    char *key = NULL;
    char *val = NULL;
    size_t val_len = 0;
    while (i < len) {
      ASSERT_EQ(0, rados_omap_get_next(iter, &key, &val, &val_len));
      if (val_len == 0 && key == NULL && val == NULL)
	break;
      if (key)
	EXPECT_EQ(std::string(keys[i]), std::string(key));
      else
	EXPECT_EQ(keys[i], key);
      ASSERT_EQ(0, memcmp(vals[i], val, val_len));
      ASSERT_EQ(lens[i], val_len);
      ++i;
    }
    ASSERT_EQ(i, len);
    ASSERT_EQ(0, rados_omap_get_next(iter, &key, &val, &val_len));
    ASSERT_EQ((char*)NULL, key);
    ASSERT_EQ((char*)NULL, val);
    ASSERT_EQ(0u, val_len);
    rados_omap_get_end(iter);
  }

  void compare_xattrs(char const* const* keys,
		      char const* const* vals,
		      const size_t *lens,
		      size_t len,
		      rados_xattrs_iter_t iter)
  {
    size_t i = 0;
    char *key = NULL;
    char *val = NULL;
    size_t val_len = 0;
    while (i < len) {
      ASSERT_EQ(0, rados_getxattrs_next(iter, (const char**) &key,
					(const char**) &val, &val_len));
      if (key == NULL)
	break;
      EXPECT_EQ(std::string(keys[i]), std::string(key));
      if (val != NULL)
        EXPECT_EQ(0, memcmp(vals[i], val, val_len));
      EXPECT_EQ(lens[i], val_len);
      ++i;
    }
    ASSERT_EQ(i, len);
    ASSERT_EQ(0, rados_getxattrs_next(iter, (const char**)&key,
				      (const char**)&val, &val_len));
    ASSERT_EQ((char*)NULL, key);
    ASSERT_EQ((char*)NULL, val);
    ASSERT_EQ(0u, val_len);
    rados_getxattrs_end(iter);
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
  EXPECT_EQ(-EIO, rval);
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

TEST_F(CReadOpsTest, AssertVersion) {
  write_object();
  // Write to the object a second time to guarantee that its
  // version number is greater than 0
  write_object();
  uint64_t v = rados_get_last_version(ioctx);

  rados_read_op_t op = rados_create_read_op();
  rados_read_op_assert_version(op, v+1);
  ASSERT_EQ(-EOVERFLOW, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);

  op = rados_create_read_op();
  rados_read_op_assert_version(op, v-1);
  ASSERT_EQ(-ERANGE, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);

  op = rados_create_read_op();
  rados_read_op_assert_version(op, v);
  ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);

  remove_object();
}

TEST_F(CReadOpsTest, CmpXattr) {
  write_object();

  char buf[len];
  memset(buf, 0xcc, sizeof(buf));

  const char *xattr = "test";
  rados_setxattr(ioctx, obj, xattr, buf, sizeof(buf));

  // equal value
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_EQ));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_NE));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_GT));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_GTE));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_LT));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_LTE));

  // < value
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf) - 1, LIBRADOS_CMPXATTR_OP_EQ));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf) - 1, LIBRADOS_CMPXATTR_OP_NE));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf) - 1, LIBRADOS_CMPXATTR_OP_GT));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf) - 1, LIBRADOS_CMPXATTR_OP_GTE));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf) - 1, LIBRADOS_CMPXATTR_OP_LT));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf) - 1, LIBRADOS_CMPXATTR_OP_LTE));

  // > value
  memset(buf, 0xcd, sizeof(buf));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_EQ));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_NE));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_GT));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_GTE));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_LT));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, sizeof(buf), LIBRADOS_CMPXATTR_OP_LTE));

  // check that null bytes are compared correctly
  rados_setxattr(ioctx, obj, xattr, "\0\0", 2);
  buf[0] = '\0';
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_EQ));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_NE));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_GT));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_GTE));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_LT));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_LTE));

  buf[1] = '\0';
  EXPECT_EQ(1, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_EQ));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_NE));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_GT));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_GTE));
  EXPECT_EQ(-ECANCELED, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_LT));
  EXPECT_EQ(1, cmp_xattr(xattr, buf, 2, LIBRADOS_CMPXATTR_OP_LTE));

  remove_object();
}

TEST_F(CReadOpsTest, Read) {
  write_object();

  char buf[len];
  // check that using read_ops returns the same data with
  // or without bytes_read and rval out params
  {
    rados_read_op_t op = rados_create_read_op();
    rados_read_op_read(op, 0, len, buf, NULL, NULL);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  {
    rados_read_op_t op = rados_create_read_op();
    int rval;
    rados_read_op_read(op, 0, len, buf, NULL, &rval);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(0, rval);
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  {
    rados_read_op_t op = rados_create_read_op();
    size_t bytes_read = 0;
    rados_read_op_read(op, 0, len, buf, &bytes_read, NULL);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(len, (int)bytes_read);
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  {
    rados_read_op_t op = rados_create_read_op();
    size_t bytes_read = 0;
    int rval;
    rados_read_op_read(op, 0, len, buf, &bytes_read, &rval);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(len, (int)bytes_read);
    ASSERT_EQ(0, rval);
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  {
    rados_read_op_t op = rados_create_read_op();
    size_t bytes_read = 0;
    int rval;
    rados_read_op_read(op, 0, len, buf, &bytes_read, &rval);
    rados_read_op_set_flags(op, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(len, (int)bytes_read);
    ASSERT_EQ(0, rval);
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  remove_object();
}


TEST_F(CReadOpsTest, RWOrderedRead) {
  write_object();

  char buf[len];
  rados_read_op_t op = rados_create_read_op();
  size_t bytes_read = 0;
  int rval;
  rados_read_op_read(op, 0, len, buf, &bytes_read, &rval);
  rados_read_op_set_flags(op, LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj,
				     LIBRADOS_OPERATION_ORDER_READS_WRITES));
  ASSERT_EQ(len, (int)bytes_read);
  ASSERT_EQ(0, rval);
  ASSERT_EQ(0, memcmp(data, buf, len));
  rados_release_read_op(op);

  remove_object();
}

TEST_F(CReadOpsTest, ShortRead) {
  write_object();

  char buf[len * 2];
  // check that using read_ops returns the same data with
  // or without bytes_read and rval out params
  {
    rados_read_op_t op = rados_create_read_op();
    rados_read_op_read(op, 0, len * 2, buf, NULL, NULL);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  {
    rados_read_op_t op = rados_create_read_op();
    int rval;
    rados_read_op_read(op, 0, len * 2, buf, NULL, &rval);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(0, rval);
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  {
    rados_read_op_t op = rados_create_read_op();
    size_t bytes_read = 0;
    rados_read_op_read(op, 0, len * 2, buf, &bytes_read, NULL);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(len, (int)bytes_read);
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  {
    rados_read_op_t op = rados_create_read_op();
    size_t bytes_read = 0;
    int rval;
    rados_read_op_read(op, 0, len * 2, buf, &bytes_read, &rval);
    ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
    ASSERT_EQ(len, (int)bytes_read);
    ASSERT_EQ(0, rval);
    ASSERT_EQ(0, memcmp(data, buf, len));
    rados_release_read_op(op);
  }

  remove_object();
}

TEST_F(CReadOpsTest, Exec) {
  // create object so we don't get -ENOENT
  write_object();

  rados_read_op_t op = rados_create_read_op();
  ASSERT_TRUE(op);
  size_t bytes_read = 0;
  char *out = NULL;
  int rval = 0;
  rados_read_op_exec(op, "rbd", "get_all_features", NULL, 0, &out,
		     &bytes_read, &rval);
  ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);
  EXPECT_EQ(0, rval);
  EXPECT_TRUE(out);
  uint64_t features;
  EXPECT_EQ(sizeof(features), bytes_read);
  // make sure buffer is at least as long as it claims
  ASSERT_TRUE(out[bytes_read-1] == out[bytes_read-1]);
  rados_buffer_free(out);

  remove_object();
}

TEST_F(CReadOpsTest, ExecUserBuf) {
  // create object so we don't get -ENOENT
  write_object();

  rados_read_op_t op = rados_create_read_op();
  size_t bytes_read = 0;
  uint64_t features;
  char out[sizeof(features)];
  int rval = 0;
  rados_read_op_exec_user_buf(op, "rbd", "get_all_features", NULL, 0, out,
			      sizeof(out), &bytes_read, &rval);
  ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  rados_release_read_op(op);
  EXPECT_EQ(0, rval);
  EXPECT_EQ(sizeof(features), bytes_read);

  // buffer too short
  bytes_read = 1024;
  op = rados_create_read_op();
  rados_read_op_exec_user_buf(op, "rbd", "get_all_features", NULL, 0, out,
			      sizeof(features) - 1, &bytes_read, &rval);
  ASSERT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  EXPECT_EQ(0u, bytes_read);
  EXPECT_EQ(-ERANGE, rval);

  // input buffer and no rval or bytes_read
  op = rados_create_read_op();
  rados_read_op_exec_user_buf(op, "rbd", "get_all_features", out, sizeof(out),
			      out, sizeof(out), NULL, NULL);
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

TEST_F(CReadOpsTest, Omap) {
  char *keys[] = {(char*)"bar",
		  (char*)"foo",
		  (char*)"test1",
		  (char*)"test2"};
  char *vals[] = {(char*)"",
		  (char*)"\0",
		  (char*)"abc",
		  (char*)"va\0lue"};
  size_t lens[] = {0, 1, 3, 6};

  // check for -ENOENT before the object exists and when it exists
  // with no omap entries
  rados_omap_iter_t iter_vals;
  rados_read_op_t rop = rados_create_read_op();
  rados_read_op_omap_get_vals(rop, "", "", 10, &iter_vals, NULL);
  ASSERT_EQ(-ENOENT, rados_read_op_operate(rop, ioctx, obj, 0));
  rados_release_read_op(rop);
  compare_omap_vals(NULL, NULL, NULL, 0, iter_vals);

  write_object();

  fetch_and_verify_omap_vals(NULL, NULL, NULL, 0);

  // write and check for the k/v pairs
  rados_write_op_t op = rados_create_write_op();
  rados_write_op_omap_set(op, keys, vals, lens, 4);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, obj, NULL, 0));
  rados_release_write_op(op);

  fetch_and_verify_omap_vals(keys, vals, lens, 4);

  rados_omap_iter_t iter_keys;
  int r_vals = -1, r_keys = -1;
  rop = rados_create_read_op();
  rados_read_op_omap_get_vals(rop, "", "test", 1, &iter_vals, &r_vals);
  rados_read_op_omap_get_keys(rop, "test", 1, &iter_keys, &r_keys);
  ASSERT_EQ(0, rados_read_op_operate(rop, ioctx, obj, 0));
  rados_release_read_op(rop);
  EXPECT_EQ(0, r_vals);
  EXPECT_EQ(0, r_keys);

  compare_omap_vals(&keys[2], &vals[2], &lens[2], 1, iter_vals);
  compare_omap_vals(&keys[2], &vals[0], &lens[0], 1, iter_keys);

  // check omap_cmp finds all expected values
  rop = rados_create_read_op();
  int rvals[4];
  for (int i = 0; i < 4; ++i)
    rados_read_op_omap_cmp(rop, keys[i], LIBRADOS_CMPXATTR_OP_EQ,
			   vals[i], lens[i], &rvals[i]);
  EXPECT_EQ(0, rados_read_op_operate(rop, ioctx, obj, 0));
  rados_release_read_op(rop);
  for (int i = 0; i < 4; ++i)
    EXPECT_EQ(0, rvals[i]);

  // try to remove keys with a guard that should fail
  op = rados_create_write_op();
  rados_write_op_omap_cmp(op, keys[2], LIBRADOS_CMPXATTR_OP_LT,
			  vals[2], lens[2], &r_vals);
  rados_write_op_omap_rm_keys(op, keys, 2);
  EXPECT_EQ(-ECANCELED, rados_write_op_operate(op, ioctx, obj, NULL, 0));
  rados_release_write_op(op);
  ASSERT_EQ(-ECANCELED, r_vals);

  // verifying the keys are still there, and then remove them
  op = rados_create_write_op();
  rados_write_op_omap_cmp(op, keys[0], LIBRADOS_CMPXATTR_OP_EQ,
			  vals[0], lens[0], NULL);
  rados_write_op_omap_cmp(op, keys[1], LIBRADOS_CMPXATTR_OP_EQ,
			  vals[1], lens[1], NULL);
  rados_write_op_omap_rm_keys(op, keys, 2);
  EXPECT_EQ(0, rados_write_op_operate(op, ioctx, obj, NULL, 0));
  rados_release_write_op(op);

  fetch_and_verify_omap_vals(&keys[2], &vals[2], &lens[2], 2);

  // clear the rest and check there are none left
  op = rados_create_write_op();
  rados_write_op_omap_clear(op);
  EXPECT_EQ(0, rados_write_op_operate(op, ioctx, obj, NULL, 0));
  rados_release_write_op(op);

  fetch_and_verify_omap_vals(NULL, NULL, NULL, 0);

  remove_object();
}

TEST_F(CReadOpsTest, GetXattrs) {
  write_object();

  char *keys[] = {(char*)"bar",
		  (char*)"foo",
		  (char*)"test1",
		  (char*)"test2"};
  char *vals[] = {(char*)"",
		  (char*)"\0",
		  (char*)"abc",
		  (char*)"va\0lue"};
  size_t lens[] = {0, 1, 3, 6};

  int rval = 1;
  rados_read_op_t op = rados_create_read_op();
  rados_xattrs_iter_t it;
  rados_read_op_getxattrs(op, &it, &rval);
  EXPECT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  EXPECT_EQ(0, rval);
  rados_release_read_op(op);
  compare_xattrs(keys, vals, lens, 0, it);

  for (int i = 0; i < 4; ++i)
    rados_setxattr(ioctx, obj, keys[i], vals[i], lens[i]);

  rval = 1;
  op = rados_create_read_op();
  rados_read_op_getxattrs(op, &it, &rval);
  EXPECT_EQ(0, rados_read_op_operate(op, ioctx, obj, 0));
  EXPECT_EQ(0, rval);
  rados_release_read_op(op);
  compare_xattrs(keys, vals, lens, 4, it);

  remove_object();
}
