#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"

#include <errno.h>
#include "gtest/gtest.h"

using namespace librados;
using std::string;

TEST(LibRadosIo, SimpleWrite) {
  char buf[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, SimpleWritePP) {
  char buf[128];
  std::string pool_name = get_temp_pool_name();
  Rados cluster;
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl, sizeof(buf), 0));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, RoundTrip) {
  char buf[128];
  char buf2[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, RoundTripPP) {
  char buf[128];
  char buf2[128];
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl, sizeof(buf), 0));
  bufferlist cl;
  ASSERT_EQ((int)sizeof(buf2), ioctx.read("foo", cl, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(buf, cl.c_str(), sizeof(buf)));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, OverlappingWriteRoundTrip) {
  char buf[128];
  char buf2[64];
  char buf3[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_write(ioctx, "foo", buf2, sizeof(buf2), 0));
  memset(buf3, 0xdd, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf2, sizeof(buf2)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf2), buf, sizeof(buf) - sizeof(buf2)));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, OverlappingWriteRoundTripPP) {
  char buf[128];
  char buf2[64];
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  IoCtx ioctx;
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl1, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), ioctx.write("foo", bl2, sizeof(buf2), 0));
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  ASSERT_EQ(0, memcmp(bl3.c_str() + sizeof(buf2), buf, sizeof(buf) - sizeof(buf2)));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, WriteFullRoundTrip) {
  char buf[128];
  char buf2[64];
  char buf3[128];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  ASSERT_EQ(0, rados_write_full(ioctx, "foo", buf2, sizeof(buf2)));
  memset(buf3, 0xdd, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf2, buf2, sizeof(buf2)));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, WriteFullRoundTripPP) {
  char buf[128];
  char buf2[64];
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.write("foo", bl1, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.write_full("foo", bl2));
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf2), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, AppendRoundTrip) {
  char buf[64];
  char buf2[64];
  char buf3[sizeof(buf) + sizeof(buf2)];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xde, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_append(ioctx, "foo", buf, sizeof(buf)));
  memset(buf2, 0xad, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_append(ioctx, "foo", buf2, sizeof(buf2)));
  memset(buf3, 0, sizeof(buf3));
  ASSERT_EQ((int)sizeof(buf3), rados_read(ioctx, "foo", buf3, sizeof(buf3), 0));
  ASSERT_EQ(0, memcmp(buf3, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf3 + sizeof(buf), buf2, sizeof(buf2)));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, AppendRoundTripPP) {
  char buf[64];
  char buf2[64];
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xde, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.append("foo", bl1, sizeof(buf)));
  memset(buf2, 0xad, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), ioctx.append("foo", bl2, sizeof(buf2)));
  bufferlist bl3;
  ASSERT_EQ((int)(sizeof(buf) + sizeof(buf2)),
	    ioctx.read("foo", bl3, (sizeof(buf) + sizeof(buf2)), 0));
  const char *bl3_str = bl3.c_str();
  ASSERT_EQ(0, memcmp(bl3_str, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(bl3_str + sizeof(buf), buf2, sizeof(buf2)));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, TruncTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_trunc(ioctx, "foo", sizeof(buf) / 2));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)(sizeof(buf)/2), rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)/2));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, TruncTestPP) {
  char buf[128];
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.append("foo", bl, sizeof(buf)));
  ASSERT_EQ(0, ioctx.trunc("foo", sizeof(buf) / 2));
  bufferlist bl2;
  ASSERT_EQ((int)(sizeof(buf)/2), ioctx.read("foo", bl2, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl2.c_str(), buf, sizeof(buf)/2));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, RemoveTest) {
  char buf[128];
  char buf2[sizeof(buf)];
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0, rados_remove(ioctx, "foo"));
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ(-ENOENT, rados_read(ioctx, "foo", buf2, sizeof(buf2), 0));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, RemoveTestPP) {
  char buf[128];
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.append("foo", bl1, sizeof(buf)));
  ASSERT_EQ(0, ioctx.remove("foo"));
  bufferlist bl2;
  ASSERT_EQ(-ENOENT, ioctx.read("foo", bl2, sizeof(buf), 0));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, XattrsRoundTrip) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(-ENODATA, rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));
  ASSERT_EQ(0, rados_setxattr(ioctx, "foo", attr1, attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ((int)sizeof(attr1_buf),
	    rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(attr1_buf, buf, sizeof(attr1_buf)));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, XattrsRoundTripPP) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  ASSERT_EQ(-ENODATA, ioctx.getxattr("foo", attr1, bl2));
  bufferlist bl3;
  bl3.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl3));
  bufferlist bl4;
  ASSERT_EQ((int)sizeof(attr1_buf),
      ioctx.getxattr("foo", attr1, bl4));
  ASSERT_EQ(0, memcmp(bl4.c_str(), attr1_buf, sizeof(attr1_buf)));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, RmXattr) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_append(ioctx, "foo", buf, sizeof(buf)));
  ASSERT_EQ(0,
      rados_setxattr(ioctx, "foo", attr1, attr1_buf, sizeof(attr1_buf)));
  ASSERT_EQ(0, rados_rmxattr(ioctx, "foo", attr1));
  ASSERT_EQ(-ENODATA, rados_getxattr(ioctx, "foo", attr1, buf, sizeof(buf)));
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, RmXattrPP) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl2));
  ASSERT_EQ(0, ioctx.rmxattr("foo", attr1));
  bufferlist bl3;
  ASSERT_EQ(-ENODATA, ioctx.getxattr("foo", attr1, bl3));
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST(LibRadosIo, XattrIter) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  char attr2[] = "attr2";
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  rados_t cluster;
  rados_ioctx_t ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  rados_ioctx_create(cluster, pool_name.c_str(), &ioctx);
  memset(buf, 0xaa, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), rados_append(ioctx, "foo", buf, sizeof(buf)));
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
    if ((strcmp(name, attr1) == 0) && (memcmp(val, attr1_buf, len) == 0)) {
      num_seen++;
      continue;
    }
    else if ((strcmp(name, attr2) == 0) && (memcmp(val, attr2_buf, len) == 0)) {
      num_seen++;
      continue;
    }
    else {
      ASSERT_EQ(0, 1);
    }
  }
  rados_getxattrs_end(iter);
  rados_ioctx_destroy(ioctx);
  ASSERT_EQ(0, destroy_one_pool(pool_name, &cluster));
}

TEST(LibRadosIo, XattrListPP) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  char attr2[] = "attr2";
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  Rados cluster;
  IoCtx ioctx;
  std::string pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  cluster.ioctx_create(pool_name.c_str(), ioctx);
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ((int)sizeof(buf), ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl2));
  bufferlist bl3;
  bl3.append(attr2_buf, sizeof(attr2_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr2, bl3));
  std::map<std::string, bufferlist> attrset;
  ASSERT_EQ(0, ioctx.getxattrs("foo", attrset));
  for (std::map<std::string, bufferlist>::iterator i = attrset.begin();
       i != attrset.end(); ++i) {
    if (i->first == string(attr1)) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr1_buf, sizeof(attr1_buf)));
    }
    else if (i->first == string(attr2)) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr2_buf, sizeof(attr2_buf)));
    }
    else {
      ASSERT_EQ(0, 1);
    }
  }
  ioctx.close();
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}
