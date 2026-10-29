// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*
// vim: ts=8 sw=2 sts=2 expandtab

#include <climits>

#include "include/rados/librados.h"
#include "include/encoding.h"
#include "include/err.h"
#include "include/scope_guard.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <errno.h>
#include "gtest/gtest.h"
#include "crimson_utils.h"

using std::string;

typedef RadosTest LibRadosIo;
typedef RadosTestEC LibRadosIoEC;

TEST_P(LibRadosIo, SimpleWrite) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "nspace");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
}

TEST_P(LibRadosIo, TooBig) {
  char buf[1] = { 0 };
  ASSERT_EQ(-E2BIG, rados_write(ioctx, "A", buf, UINT_MAX, 0));
  ASSERT_EQ(-E2BIG, rados_append(ioctx, "A", buf, UINT_MAX));
  ASSERT_EQ(-E2BIG, rados_write_full(ioctx, "A", buf, UINT_MAX));
  ASSERT_EQ(-E2BIG, rados_writesame(ioctx, "A", buf, sizeof(buf), UINT_MAX, 0));
}

TEST_P(LibRadosIo, ReadTimeout) {
  char buf[128];
  memset(buf, 'a', sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));

  {
    // set up a second client
    rados_t cluster;
    rados_ioctx_t ioctx;
    ASSERT_EQ(0, rados_create(&cluster, "admin"));
    ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
    ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));
    ASSERT_EQ(0, rados_conf_set(cluster, "rados_osd_op_timeout", "1")); // use any small value that will result in a timeout
    ASSERT_EQ(0, rados_conf_set(cluster, "ms_inject_internal_delays", "2")); // create a 2 second delay
    ASSERT_EQ(0, rados_connect(cluster));
    ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
    rados_ioctx_set_namespace(ioctx, nspace.c_str());

    // then we show that the buffer is changed after rados_read returned
    // with a timeout
    for (int i=0; i<5; i++) {
      char buf2[sizeof(buf)];
      memset(buf2, 0, sizeof(buf2));
      int err = rados_read(ioctx, "foo", buf2, sizeof(buf2), 0);
      if (err == -110) {
	int startIndex = 0;
	// find the index until which librados already read the object before the timeout occurred
	for (unsigned b=0; b<sizeof(buf); b++) {
	  if (buf2[b] != buf[b]) {
	    startIndex = b;
	    break;
	  }
	}

	// wait some time to give librados a change to do something
	sleep(1);

	// then check if the buffer was changed after the call
	if (buf2[startIndex] == 'a') {
	  printf("byte at index %d was changed after the timeout to %d\n",
		 startIndex, (int)buf[startIndex]);
	  ASSERT_TRUE(0);
	  break;
	}
      } else {
	printf("no timeout :/\n");
      }
    }
    rados_ioctx_destroy(ioctx);
    rados_shutdown(cluster);
  }
}


TEST_P(LibRadosIo, RoundTrip) {
  char buf[128];
  char buf2[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));

  uint64_t off = 19;
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "bar", buf, sizeof(buf), off));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "bar", buf2, sizeof(buf2), off));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
}

TEST_P(LibRadosIo, Checksum) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));

  uint32_t expected_crc = ceph_crc32c(-1, reinterpret_cast<const uint8_t*>(buf),
                                      sizeof(buf));
  ceph_le32 init_value(-1);
  ceph_le32 crc[2];
  ASSERT_EQ(0, rados_checksum(ioctx, "foo", LIBRADOS_CHECKSUM_TYPE_CRC32C,
			      reinterpret_cast<char*>(&init_value),
			      sizeof(init_value), sizeof(buf), 0, 0,
			      reinterpret_cast<char*>(&crc), sizeof(crc)));
  ASSERT_EQ(1U, crc[0]);
  ASSERT_EQ(expected_crc, crc[1]);
}

TEST_P(LibRadosIo, OverlappingWriteRoundTrip) {
  char buf[128];
  char buf2[64];
  char buf3[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));
  memset(buf3, 0xdd, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf2)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf2), buf, sizeof(buf) - sizeof(buf2)));
}

TEST_P(LibRadosIo, WriteFullRoundTrip) {
  char buf[128];
  char buf2[64];
  char buf3[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_write_full(ioctx, "foo", buf2, sizeof(buf2)));
  memset(buf3, 0x00, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf2, buf3, sizeof(buf2)));
}

TEST_P(LibRadosIo, AppendRoundTrip) {
  char buf[64];
  char buf2[64];
  char buf3[sizeof(buf) + sizeof(buf2)];
  memset(buf, 0xde, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  memset(buf2, 0xad, sizeof(buf2));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf2, sizeof(buf2)));
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf), buf2, sizeof(buf2)));
}

TEST_P(LibRadosIo, ZeroLenZero) {
  rados_write_op_t op = rados_create_write_op();
  ASSERT_TRUE(op);
  rados_write_op_zero(op, 0, 0);
  ASSERT_EQ(0, rados_write_op_operate(op, ioctx, "foo", NULL, 0));
  rados_release_write_op(op);
}

TEST_P(LibRadosIo, TruncTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_trunc(ioctx, "foo", sizeof(buf) / 2));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)(sizeof(buf)/2), rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)/2));
}

TEST_P(LibRadosIo, RemoveTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_remove(ioctx, "foo"));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ(-ENOENT, rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
}

TEST_P(LibRadosIo, XattrsRoundTrip) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(-ENODATA, rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));
  ASSERT_EQ(0, rados_setxattr(ioctx, "foo", attr1, attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ((int)sizeof(attr1_buf),
	    rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(attr1_buf, buf, sizeof(attr1_buf)));
}

TEST_P(LibRadosIo, RmXattr) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0,
      rados_setxattr(ioctx, "foo", attr1, attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ(0, rados_rmxattr(ioctx, "foo", attr1));
  ASSERT_EQ(-ENODATA, rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));

  // Test rmxattr on a removed object
  char buf2[128];
  char attr2[] = "attr2";
  char attr2_buf[] = "foo bar baz";
  memset(buf2, 0xbb, sizeof(buf2));
  ASSERT_EQ(0, rados_write(ioctx, "foo_rmxattr", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0,
      rados_setxattr(ioctx, "foo_rmxattr", attr2, attr2_buf, sizeof(attr2_buf)));
  ASSERT_EQ(0, rados_remove(ioctx, "foo_rmxattr"));
  ASSERT_EQ(-ENOENT, rados_rmxattr(ioctx, "foo_rmxattr", attr2));
}

TEST_P(LibRadosIo, XattrIter) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  char attr2[] = "attr2";
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_setxattr(ioctx, "foo", attr1, attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ(0, rados_setxattr(ioctx, "foo", attr2, attr2_buf, sizeof(attr2_buf)));
  rados_xattrs_iter_t iter;
  ASSERT_EQ(0, rados_getxattrs(ioctx, "foo", &iter));
  int num_seen = 0;
  while (true) {
    const char *name;
    const char *val;
    size_t len;
    ASSERT_EQ(0, rados_getxattrs_next(iter, &name, &val, &len));
    if (name == NULL) {
      break;
    }
    ASSERT_LT(num_seen, 2);
    if ((strcmp(name, attr1) == 0) && (val != NULL) && (memcmp(val, attr1_buf, len) == 0)) {
      num_seen++;
      continue;
    }
    else if ((strcmp(name, attr2) == 0) && (val != NULL) && (memcmp(val, attr2_buf, len) == 0)) {
      num_seen++;
      continue;
    }
    else {
      ASSERT_EQ(0, 1);
    }
  }
  rados_getxattrs_end(iter);
}

TEST_P(LibRadosIoEC, SimpleWrite) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "nspace");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
}

TEST_P(LibRadosIoEC, RoundTrip) {
  char buf[128];
  char buf2[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));

  if (!is_crimson_cluster()) {
    uint64_t off = 19;
    ASSERT_EQ(-EOPNOTSUPP, rados_write(ioctx, "bar", buf, sizeof(buf), off));
  }
}

TEST_P(LibRadosIoEC, OverlappingWriteRoundTrip) {
  int bsize = alignment;
  int dbsize = bsize * 2;
  char *buf = (char *)new char[dbsize];
  char *buf2 = (char *)new char[bsize];
  char *buf3 = (char *)new char[dbsize];
  auto cleanup = [&] {
    delete[] buf;
    delete[] buf2;
    delete[] buf3;
  };
  scope_guard<decltype(cleanup)> sg(std::move(cleanup));
  memset(buf, 0xcc, dbsize);
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, dbsize, 0));
  if (!is_crimson_cluster()) {
    memset(buf2, 0xdd, bsize);
    ASSERT_EQ(-EOPNOTSUPP, rados_write(ioctx, "foo", buf2, bsize, 0));
    memset(buf3, 0xdd, dbsize);
    ASSERT_EQ(dbsize, rados_read(ioctx, "foo", buf3, dbsize, 0));
    // Read the same as first write
    ASSERT_EQ(0, memcmp(buf3, buf, dbsize));
  }
}

TEST_P(LibRadosIoEC, WriteFullRoundTrip) {
  char buf[128];
  char buf2[64];
  char buf3[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_write_full(ioctx, "foo", buf2, sizeof(buf2)));
  memset(buf3, 0xee, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf2)));
}

TEST_P(LibRadosIoEC, AppendRoundTrip) {
  char *buf = (char *)new char[alignment];
  char *buf2 = (char *)new char[alignment];
  char *buf3 = (char *)new char[alignment *2];
  int uasize = alignment/2;
  char *unalignedbuf = (char *)new char[uasize];
  auto cleanup = [&] {
    delete[] buf;
    delete[] buf2;
    delete[] buf3;
    delete[] unalignedbuf;
  };
  scope_guard<decltype(cleanup)> sg(std::move(cleanup));
  memset(buf, 0xde, alignment);
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, alignment));
  memset(buf2, 0xad, alignment);
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf2, alignment));
  memset(buf3, 0, alignment*2);
  ASSERT_EQ((int)alignment*2, rados_read(ioctx, "foo", buf3, alignment*2, 0));
  ASSERT_EQ(0, memcmp(buf3, buf, alignment));
  ASSERT_EQ(0, memcmp(buf3 + alignment, buf2, alignment));
  memset(unalignedbuf, 0, uasize);
  ASSERT_EQ(0, rados_append(ioctx, "foo", unalignedbuf, uasize));
  if (!is_crimson_cluster()) {
    ASSERT_EQ(-EOPNOTSUPP, rados_append(ioctx, "foo", unalignedbuf, uasize));
  }
}

TEST_P(LibRadosIoEC, TruncTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  if (!is_crimson_cluster()) {
    ASSERT_EQ(-EOPNOTSUPP, rados_trunc(ioctx, "foo", sizeof(buf) / 2));
    memset(buf2, 0, sizeof(buf2));
    // Same size
    ASSERT_EQ((int)sizeof(buf), rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
    // No change
    ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  }
}

TEST_P(LibRadosIoEC, RemoveTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_remove(ioctx, "foo"));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ(-ENOENT, rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
}

TEST_P(LibRadosIoEC, XattrsRoundTrip) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(-ENODATA, rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));
  ASSERT_EQ(0, rados_setxattr(ioctx, "foo", attr1, attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ((int)sizeof(attr1_buf),
	    rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(attr1_buf, buf, sizeof(attr1_buf)));
}

TEST_P(LibRadosIoEC, RmXattr) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0,
      rados_setxattr(ioctx, "foo", attr1, attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ(0, rados_rmxattr(ioctx, "foo", attr1));
  ASSERT_EQ(-ENODATA, rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));

  // Test rmxattr on a removed object
  char buf2[128];
  char attr2[] = "attr2";
  char attr2_buf[] = "foo bar baz";
  memset(buf2, 0xbb, sizeof(buf2));
  ASSERT_EQ(0, rados_write(ioctx, "foo_rmxattr", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0,
      rados_setxattr(ioctx, "foo_rmxattr", attr2, attr2_buf, sizeof(attr2_buf)));
  ASSERT_EQ(0, rados_remove(ioctx, "foo_rmxattr"));
  ASSERT_EQ(-ENOENT, rados_rmxattr(ioctx, "foo_rmxattr", attr2));
}

TEST_P(LibRadosIoEC, XattrIter) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  char attr2[] = "attr2";
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ(0, rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_setxattr(ioctx, "foo", attr1, attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ(0, rados_setxattr(ioctx, "foo", attr2, attr2_buf, sizeof(attr2_buf)));
  rados_xattrs_iter_t iter;
  ASSERT_EQ(0, rados_getxattrs(ioctx, "foo", &iter));
  int num_seen = 0;
  while (true) {
    const char *name;
    const char *val;
    size_t len;
    ASSERT_EQ(0, rados_getxattrs_next(iter, &name, &val, &len));
    if (name == NULL) {
      break;
    }
    ASSERT_LT(num_seen, 2);
    if ((strcmp(name, attr1) == 0) && (val != NULL) && (memcmp(val, attr1_buf, len) == 0)) {
      num_seen++;
      continue;
    }
    else if ((strcmp(name, attr2) == 0) && (val != NULL) && (memcmp(val, attr2_buf, len) == 0)) {
      num_seen++;
      continue;
    }
    else {
      ASSERT_EQ(0, 1);
    }
  }
  rados_getxattrs_end(iter);
}

// A failed op must not leave xattr changes from earlier ops of the same
// request visible to later reads.
static void check_xattr_rollback_on_failed_op(rados_ioctx_t ioctx)
{
  const char *oid = "xattr_rollback";
  char data[] = "ceph";
  char v1[] = "value1";
  char v2[] = "value2";

  rados_write_op_t create = rados_create_write_op();
  rados_write_op_write_full(create, data, sizeof(data));
  rados_write_op_setxattr(create, "key", v1, sizeof(v1));
  int r = rados_write_op_operate(create, ioctx, oid, NULL, 0);
  rados_release_write_op(create);
  ASSERT_EQ(0, r);
  const uint64_t ver = rados_get_last_version(ioctx);

  rados_write_op_t set_then_fail = rados_create_write_op();
  rados_write_op_setxattr(set_then_fail, "key", v2, sizeof(v2));
  rados_write_op_setxattr(set_then_fail, "new_key", v2, sizeof(v2));
  rados_write_op_assert_version(set_then_fail, ver + 1);
  r = rados_write_op_operate(set_then_fail, ioctx, oid, NULL, 0);
  rados_release_write_op(set_then_fail);
  ASSERT_EQ(-EOVERFLOW, r);

  rados_write_op_t rm_then_fail = rados_create_write_op();
  rados_write_op_rmxattr(rm_then_fail, "key");
  rados_write_op_assert_version(rm_then_fail, ver + 1);
  r = rados_write_op_operate(rm_then_fail, ioctx, oid, NULL, 0);
  rados_release_write_op(rm_then_fail);
  ASSERT_EQ(-EOVERFLOW, r);

  char buf[128];
  ASSERT_EQ((int)sizeof(v1), rados_getxattr(ioctx, oid, "key", buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(v1, buf, sizeof(v1)));
  ASSERT_EQ(-ENODATA, rados_getxattr(ioctx, oid, "new_key", buf, sizeof(buf)));

  rados_xattrs_iter_t iter;
  ASSERT_EQ(0, rados_getxattrs(ioctx, oid, &iter));
  int num_seen = 0;
  while (true) {
    const char *name;
    const char *val;
    size_t len;
    ASSERT_EQ(0, rados_getxattrs_next(iter, &name, &val, &len));
    if (name == NULL) {
      break;
    }
    ASSERT_STREQ("key", name);
    ASSERT_EQ(sizeof(v1), len);
    ASSERT_EQ(0, memcmp(v1, val, len));
    num_seen++;
  }
  rados_getxattrs_end(iter);
  ASSERT_EQ(1, num_seen);
}

TEST_P(LibRadosIo, XattrRollbackOnFailedOp) {
  check_xattr_rollback_on_failed_op(ioctx);
}

TEST_P(LibRadosIoEC, XattrRollbackOnFailedOp) {
  check_xattr_rollback_on_failed_op(ioctx);
}

static int cmp_xattr(rados_ioctx_t ioctx, const char *oid, const char *name,
                     uint8_t cmp_op, const char *value, size_t value_len)
{
  rados_read_op_t op = rados_create_read_op();
  rados_read_op_cmpxattr(op, name, cmp_op, value, value_len);
  int r = rados_read_op_operate(op, ioctx, oid, 0);
  rados_release_read_op(op);
  return r;
}

static void check_cmpxattr(rados_ioctx_t ioctx)
{
  const char *oid = "cmpxattr";
  char data[] = "ceph";
  char v1[] = "value1";
  char v2[] = "value2";

  rados_write_op_t create = rados_create_write_op();
  rados_write_op_write_full(create, data, sizeof(data));
  rados_write_op_setxattr(create, "key", v1, sizeof(v1));
  int r = rados_write_op_operate(create, ioctx, oid, NULL, 0);
  rados_release_write_op(create);
  ASSERT_EQ(0, r);

  // a true comparison returns 1 on a read op
  ASSERT_EQ(1, cmp_xattr(ioctx, oid, "key", LIBRADOS_CMPXATTR_OP_EQ,
                         v1, sizeof(v1)));
  ASSERT_EQ(-ECANCELED, cmp_xattr(ioctx, oid, "key", LIBRADOS_CMPXATTR_OP_EQ,
                                  v2, sizeof(v2)));
  ASSERT_EQ(1, cmp_xattr(ioctx, oid, "key", LIBRADOS_CMPXATTR_OP_NE,
                         v2, sizeof(v2)));
  ASSERT_EQ(-ECANCELED, cmp_xattr(ioctx, oid, "no_such_key",
                                  LIBRADOS_CMPXATTR_OP_EQ, v1, sizeof(v1)));

  // guarded write, as RGW does with its id tag
  rados_write_op_t guarded = rados_create_write_op();
  rados_write_op_cmpxattr(guarded, "key", LIBRADOS_CMPXATTR_OP_EQ,
                          v1, sizeof(v1));
  rados_write_op_setxattr(guarded, "key", v2, sizeof(v2));
  r = rados_write_op_operate(guarded, ioctx, oid, NULL, 0);
  rados_release_write_op(guarded);
  ASSERT_EQ(0, r);

  char buf[128];
  ASSERT_EQ((int)sizeof(v2), rados_getxattr(ioctx, oid, "key", buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(v2, buf, sizeof(v2)));
}

TEST_P(LibRadosIo, CmpXattr) {
  check_cmpxattr(ioctx);
}

TEST_P(LibRadosIoEC, CmpXattr) {
  check_cmpxattr(ioctx);
}

INSTANTIATE_TEST_SUITE_P_REPLICA(LibRadosIo);
INSTANTIATE_TEST_SUITE_P_EC(LibRadosIoEC);