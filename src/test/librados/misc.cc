// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "mds/mdstypes.h"
#include "include/buffer.h"
#include "include/rbd_types.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include <errno.h>
#include <map>
#include <sstream>
#include <string>

using namespace librados;
using ceph::buffer;
using std::map;
using std::ostringstream;
using std::string;

typedef RadosTest LibRadosMisc;
typedef RadosTestPP LibRadosMiscPP;

TEST(LibRadosMiscVersion, Version) {
  int major, minor, extra;
  rados_version(&major, &minor, &extra);
}

TEST(LibRadosMiscVersion, VersionPP) {
  int major, minor, extra;
  Rados::version(&major, &minor, &extra);
}

TEST(LibRadosMiscConnectFailure, ConnectFailure) {
  rados_t cluster;

  char *id = getenv("CEPH_CLIENT_ID");
  if (id)
    std::cerr << "Client id is: " << id << std::endl;

  ASSERT_EQ(0, rados_create(&cluster, NULL));
  ASSERT_EQ(0, rados_conf_read_file(cluster, NULL));
  ASSERT_EQ(0, rados_conf_parse_env(cluster, NULL));

  ASSERT_EQ(0, rados_conf_set(cluster, "client_mount_timeout", "0.000001"));

  ASSERT_NE(0, rados_connect(cluster));
  ASSERT_NE(0, rados_connect(cluster));

  rados_shutdown(cluster);
}

TEST_F(LibRadosMisc, ClusterFSID) {
  char fsid[37];
  ASSERT_EQ(-ERANGE, rados_cluster_fsid(cluster, fsid, sizeof(fsid) - 1));
  ASSERT_EQ(sizeof(fsid) - 1,
            (size_t)rados_cluster_fsid(cluster, fsid, sizeof(fsid)));
}

TEST_F(LibRadosMiscPP, WaitOSDMapPP) {
  ASSERT_EQ(0, cluster.wait_for_latest_osdmap());
}

static std::string read_key_from_tmap(IoCtx& ioctx, const std::string &obj,
				      const std::string &key)
{
  bufferlist bl;
  int r = ioctx.read(obj, bl, 0, 0);
  if (r <= 0) {
    ostringstream oss;
    oss << "ioctx.read(" << obj << ", bl, 0, 0) returned " << r;
    return oss.str();
  }
  bufferlist::iterator p = bl.begin();
  bufferlist header;
  map<string, bufferlist> m;
  ::decode(header, p);
  ::decode(m, p);
  map<string, bufferlist>::iterator i = m.find(key);
  if (i == m.end())
    return "";
  std::string retstring;
  ::decode(retstring, i->second);
  return retstring;
}

static std::string add_key_to_tmap(IoCtx &ioctx, const std::string &obj,
	  const std::string &key, const std::string &val)
{
  __u8 c = CEPH_OSD_TMAP_SET;

  bufferlist tmbl;
  ::encode(c, tmbl);
  ::encode(key, tmbl);
  bufferlist blbl;
  ::encode(val, blbl);
  ::encode(blbl, tmbl);
  int ret = ioctx.tmap_update(obj, tmbl);
  if (ret) {
    ostringstream oss;
    oss << "ioctx.tmap_update(obj=" << obj << ", key="
	<< key << ", val=" << val << ") failed with error " << ret;
    return oss.str();
  }
  return "";
}

static int remove_key_from_tmap(IoCtx &ioctx, const std::string &obj,
					const std::string &key)
{
  __u8 c = CEPH_OSD_TMAP_RM;

  bufferlist tmbl;
  ::encode(c, tmbl);
  ::encode(key, tmbl);
  int ret = ioctx.tmap_update(obj, tmbl);
  if (ret) {
    ostringstream oss;
    oss << "ioctx.tmap_update(obj=" << obj << ", key="
	<< key << ") failed with error " << ret;
  }
  return ret;
}

TEST_F(LibRadosMiscPP, TmapUpdatePP) {
  // create tmap
  {
    __u8 c = CEPH_OSD_TMAP_CREATE;
    std::string my_tmap("my_tmap");
    bufferlist emptybl;

    bufferlist tmbl;
    ::encode(c, tmbl);
    ::encode(my_tmap, tmbl);
    ::encode(emptybl, tmbl);
    ASSERT_EQ(0, ioctx.tmap_update("foo", tmbl));
  }

  ASSERT_EQ(string(""), add_key_to_tmap(ioctx, "foo", "key1", "val1"));

  ASSERT_EQ(string(""), add_key_to_tmap(ioctx, "foo", "key2", "val2"));

  // read key1 from the tmap
  ASSERT_EQ(string("val1"), read_key_from_tmap(ioctx, "foo", "key1"));

  // remove key1 from tmap
  ASSERT_EQ(0, remove_key_from_tmap(ioctx, "foo", "key1"));
  ASSERT_EQ(-ENOENT, remove_key_from_tmap(ioctx, "foo", "key1"));

  // key should be removed
  ASSERT_EQ(string(""), read_key_from_tmap(ioctx, "foo", "key1"));
}

TEST_F(LibRadosMiscPP, TmapUpdateMisorderedPP) {
  // create tmap
  {
    __u8 c = CEPH_OSD_TMAP_CREATE;
    std::string my_tmap("my_tmap");
    bufferlist emptybl;

    bufferlist tmbl;
    ::encode(c, tmbl);
    ::encode(my_tmap, tmbl);
    ::encode(emptybl, tmbl);
    ASSERT_EQ(0, ioctx.tmap_update("foo", tmbl));
  }

  // good update
  {
    __u8 c = CEPH_OSD_TMAP_SET;
    bufferlist tmbl;
    ::encode(c, tmbl);
    ::encode("a", tmbl);
    bufferlist blbl;
    ::encode("old", blbl);
    ::encode(blbl, tmbl);

    ::encode(c, tmbl);
    ::encode("b", tmbl);
    ::encode(blbl, tmbl);

    ::encode(c, tmbl);
    ::encode("c", tmbl);
    ::encode(blbl, tmbl);

    ASSERT_EQ(0, ioctx.tmap_update("foo", tmbl));
  }

  // bad update
  {
    __u8 c = CEPH_OSD_TMAP_SET;
    bufferlist tmbl;
    ::encode(c, tmbl);
    ::encode("b", tmbl);
    bufferlist blbl;
    ::encode("new", blbl);
    ::encode(blbl, tmbl);

    ::encode(c, tmbl);
    ::encode("a", tmbl);
    ::encode(blbl, tmbl);

    ::encode(c, tmbl);
    ::encode("c", tmbl);
    ::encode(blbl, tmbl);

    ASSERT_EQ(0, ioctx.tmap_update("foo", tmbl));
  }

  // check
  ASSERT_EQ(string("new"), read_key_from_tmap(ioctx, "foo", "a"));
  ASSERT_EQ(string("new"), read_key_from_tmap(ioctx, "foo", "b"));
  ASSERT_EQ(string("new"), read_key_from_tmap(ioctx, "foo", "c"));

  ASSERT_EQ(0, remove_key_from_tmap(ioctx, "foo", "a"));
  ASSERT_EQ(string(""), read_key_from_tmap(ioctx, "foo", "a"));

  ASSERT_EQ(0, remove_key_from_tmap(ioctx, "foo", "b"));
  ASSERT_EQ(string(""), read_key_from_tmap(ioctx, "foo", "a"));
}

TEST_F(LibRadosMiscPP, TmapUpdateMisorderedPutPP) {
  // create unsorted tmap
  string h("header");
  bufferlist bl;
  ::encode(h, bl);
  uint32_t n = 3;
  ::encode(n, bl);
  ::encode(string("b"), bl);
  ::encode(string("bval"), bl);
  ::encode(string("a"), bl);
  ::encode(string("aval"), bl);
  ::encode(string("c"), bl);
  ::encode(string("cval"), bl);
  bufferlist orig = bl;  // tmap_put steals bl content
  ASSERT_EQ(0, ioctx.tmap_put("foo", bl));

  // check
  bufferlist newbl;
  ioctx.read("foo", newbl, orig.length(), 0);
  ASSERT_EQ(orig.contents_equal(newbl), false);
}

TEST_F(LibRadosMiscPP, Tmap2OmapPP) {
  // create tmap
  bufferlist hdr;
  hdr.append("header");
  map<string, bufferlist> omap;
  omap["1"].append("a");
  omap["2"].append("b");
  omap["3"].append("c");
  {
    bufferlist bl;
    ::encode(hdr, bl);
    ::encode(omap, bl);
    ASSERT_EQ(0, ioctx.tmap_put("foo", bl));
  }

  // convert tmap to omap
  ASSERT_EQ(0, ioctx.tmap_to_omap("foo", false));

  // if tmap was truncated ?
  {
    uint64_t size;
    time_t mtime;
    ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
    ASSERT_EQ(0U, size);
  }

  // if 'nullok' works
  ASSERT_EQ(0, ioctx.tmap_to_omap("foo", true));
  ASSERT_LE(ioctx.tmap_to_omap("foo", false), 0);

  {
    // read omap
    bufferlist got;
    map<string, bufferlist> m;
    ObjectReadOperation o;
    o.omap_get_header(&got, NULL);
    o.omap_get_vals("", 1024, &m, NULL);
    ASSERT_EQ(0, ioctx.operate("foo", &o, NULL));

    // compare header
    ASSERT_TRUE(hdr.contents_equal(got));

    // compare values
    ASSERT_EQ(omap.size(), m.size());
    bool same = true;
    for (map<string, bufferlist>::iterator p = omap.begin(); p != omap.end(); ++p) {
      map<string, bufferlist>::iterator q = m.find(p->first);
      if (q == m.end() || !p->second.contents_equal(q->second)) {
	same = false;
	break;
      }
    }
    ASSERT_TRUE(same);
  }
}

TEST_F(LibRadosMisc, Exec) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  char buf2[512];
  int res = rados_exec(ioctx, "foo", "rbd", "get_all_features",
			  NULL, 0, buf2, sizeof(buf2));
  ASSERT_GT(res, 0);
  bufferlist bl;
  bl.append(buf2, res);
  bufferlist::iterator iter = bl.begin();
  uint64_t all_features;
  ::decode(all_features, iter);
  // make sure *some* features are specified; don't care which ones
  ASSERT_NE(all_features, 0);
}

TEST_F(LibRadosMiscPP, ExecPP) {
  bufferlist bl;
  ASSERT_EQ(0, ioctx.write("foo", bl, 0, 0));
  bufferlist bl2, out;
  int r = ioctx.exec("foo", "rbd", "get_all_features", bl2, out);
  ASSERT_EQ(0, r);
  bufferlist::iterator iter = out.begin();
  uint64_t all_features;
  ::decode(all_features, iter);
  // make sure *some* features are specified; don't care which ones
  ASSERT_NE(all_features, 0);
}

TEST_F(LibRadosMiscPP, Operate1PP) {
  ObjectWriteOperation o;
  {
    bufferlist bl;
    o.write(0, bl);
  }
  std::string val1("val1");
  {
    bufferlist bl;
    bl.append(val1.c_str(), val1.size() + 1);
    o.setxattr("key1", bl);
    o.omap_clear(); // shouldn't affect attrs!
  }
  ASSERT_EQ(0, ioctx.operate("foo", &o));

  ObjectWriteOperation empty;
  ASSERT_EQ(0, ioctx.operate("foo", &empty));

  {
    bufferlist bl;
    ASSERT_GT(ioctx.getxattr("foo", "key1", bl), 0);
    ASSERT_EQ(0, strcmp(bl.c_str(), val1.c_str()));
  }
  ObjectWriteOperation o2;
  {
    bufferlist bl;
    bl.append(val1);
    o2.cmpxattr("key1", CEPH_OSD_CMPXATTR_OP_EQ, bl);
    o2.rmxattr("key1");
  }
  ASSERT_EQ(-ECANCELED, ioctx.operate("foo", &o2));
  ObjectWriteOperation o3;
  {
    bufferlist bl;
    bl.append(val1);
    o3.cmpxattr("key1", CEPH_OSD_CMPXATTR_OP_EQ, bl);
  }
  ASSERT_EQ(-ECANCELED, ioctx.operate("foo", &o3));
}

TEST_F(LibRadosMiscPP, Operate2PP) {
  ObjectWriteOperation o;
  {
    bufferlist bl;
    bl.append("abcdefg");
    o.write(0, bl);
  }
  std::string val1("val1");
  {
    bufferlist bl;
    bl.append(val1.c_str(), val1.size() + 1);
    o.setxattr("key1", bl);
    o.truncate(0);
  }
  ASSERT_EQ(0, ioctx.operate("foo", &o));
  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(0U, size);
}

TEST_F(LibRadosMiscPP, BigObjectPP) {
  bufferlist bl;
  bl.append("abcdefg");
  ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));

  {
    ObjectWriteOperation o;
    o.truncate(500000000000ull);
    ASSERT_EQ(-EFBIG, ioctx.operate("foo", &o));
  }
  {
    ObjectWriteOperation o;
    o.zero(500000000000ull, 1);
    ASSERT_EQ(-EFBIG, ioctx.operate("foo", &o));
  }
  {
    ObjectWriteOperation o;
    o.zero(1, 500000000000ull);
    ASSERT_EQ(-EFBIG, ioctx.operate("foo", &o));
  }
  {
    ObjectWriteOperation o;
    o.zero(500000000000ull, 500000000000ull);
    ASSERT_EQ(-EFBIG, ioctx.operate("foo", &o));
  }

#ifdef __LP64__
  // this test only works on 64-bit platforms
  ASSERT_EQ(-EFBIG, ioctx.write("foo", bl, bl.length(), 500000000000ull));
#endif
}

void set_completion_complete(rados_completion_t cb, void *arg)
{
  bool *my_aio_complete = (bool*)arg;
  *my_aio_complete = true;
}

TEST_F(LibRadosMiscPP, AioOperatePP) {
  bool my_aio_complete = false;
  AioCompletion *my_completion = cluster.aio_create_completion(
	  (void*)&my_aio_complete, set_completion_complete, NULL);
  AioCompletion *my_completion_null = NULL;
  ASSERT_NE(my_completion, my_completion_null);

  ObjectWriteOperation o;
  {
    bufferlist bl;
    o.write(0, bl);
  }
  std::string val1("val1");
  {
    bufferlist bl;
    bl.append(val1.c_str(), val1.size() + 1);
    o.setxattr("key1", bl);
    bufferlist bl2;
    char buf2[1024];
    memset(buf2, 0xdd, sizeof(buf2));
    bl2.append(buf2, sizeof(buf2));
    o.append(bl2);
  }
  ASSERT_EQ(0, ioctx.aio_operate("foo", my_completion, &o));
  ASSERT_EQ(0, my_completion->wait_for_complete_and_cb());
  ASSERT_EQ(my_aio_complete, true);

  uint64_t size;
  time_t mtime;
  ASSERT_EQ(0, ioctx.stat("foo", &size, &mtime));
  ASSERT_EQ(1024U, size);
}

TEST_F(LibRadosMiscPP, CloneRangePP) {
  char buf[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  ioctx.locator_set_key("foo");
  ASSERT_EQ(0, ioctx.clone_range("bar", 0, "foo", 0, sizeof(buf)));
  bufferlist bl2;
  ASSERT_EQ(sizeof(buf), (size_t)ioctx.read("bar", bl2, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
}

TEST_F(LibRadosMisc, CloneRange) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "src", buf, sizeof(buf), 0));
  rados_ioctx_locator_set_key(ioctx, "src");
  ASSERT_EQ(0, rados_clone_range(ioctx, "dst", 0, "src", 0, sizeof(buf)));
  char buf2[sizeof(buf)];
  memset(buf2, 0, sizeof(buf2));
  ASSERT_EQ((int)sizeof(buf2), rados_read(ioctx, "dst", buf2, sizeof(buf2), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
}

TEST_F(LibRadosMiscPP, AssertExistsPP) {
  char buf[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  ObjectWriteOperation op;
  op.assert_exists();
  op.write(0, bl);
  ASSERT_EQ(-ENOENT, ioctx.operate("asdffoo", &op));
  ASSERT_EQ(0, ioctx.create("asdffoo", true));
  ASSERT_EQ(0, ioctx.operate("asdffoo", &op));
  ASSERT_EQ(-EEXIST, ioctx.create("asdffoo", true));
}

TEST_F(LibRadosMiscPP, BigAttrPP) {
  char buf[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  ASSERT_EQ(0, ioctx.create("foo", true));

  bufferlist got;

  cout << "osd_max_attr_size = " << g_conf->osd_max_attr_size << std::endl;
  if (g_conf->osd_max_attr_size) {
    bl.clear();
    got.clear();
    bl.append(buffer::create(g_conf->osd_max_attr_size));
    ASSERT_EQ(0, ioctx.setxattr("foo", "one", bl));
    ASSERT_EQ((int)bl.length(), ioctx.getxattr("foo", "one", got));
    ASSERT_TRUE(bl.contents_equal(got));

    bl.clear();
    bl.append(buffer::create(g_conf->osd_max_attr_size+1));
    ASSERT_EQ(-EFBIG, ioctx.setxattr("foo", "one", bl));
  } else {
    cout << "osd_max_attr_size == 0; skipping test" << std::endl;
  }

  for (int i=0; i<1000; i++) {
    bl.clear();
    got.clear();
    bl.append(buffer::create(MIN(g_conf->osd_max_attr_size, 1024)));
    char n[10];
    snprintf(n, sizeof(n), "a%d", i);
    ASSERT_EQ(0, ioctx.setxattr("foo", n, bl));
    ASSERT_EQ((int)bl.length(), ioctx.getxattr("foo", n, got));
    ASSERT_TRUE(bl.contents_equal(got));
  }
}

TEST_F(LibRadosMiscPP, CopyPP) {
  bufferlist bl, x;
  bl.append("hi there");
  x.append("bar");

  // small object
  bufferlist blc = bl;
  bufferlist xc = x;
  ASSERT_EQ(0, ioctx.write_full("foo", blc));
  ASSERT_EQ(0, ioctx.setxattr("foo", "myattr", xc));

  version_t uv = ioctx.get_last_version();
  {
    // pass future version
    ObjectWriteOperation op;
    op.copy_from("foo", ioctx, uv + 1);
    ASSERT_EQ(-EOVERFLOW, ioctx.operate("foo.copy", &op));
  }
  {
    // pass old version
    ObjectWriteOperation op;
    op.copy_from("foo", ioctx, uv - 1);
    ASSERT_EQ(-ERANGE, ioctx.operate("foo.copy", &op));
  }
  {
    ObjectWriteOperation op;
    op.copy_from("foo", ioctx, uv);
    ASSERT_EQ(0, ioctx.operate("foo.copy", &op));

    bufferlist bl2, x2;
    ASSERT_EQ((int)bl.length(), ioctx.read("foo.copy", bl2, 10000, 0));
    ASSERT_TRUE(bl.contents_equal(bl2));
    ASSERT_EQ((int)x.length(), ioctx.getxattr("foo.copy", "myattr", x2));
    ASSERT_TRUE(x.contents_equal(x2));
  }

  // small object without a version
  {
    ObjectWriteOperation op;
    op.copy_from("foo", ioctx, 0);
    ASSERT_EQ(0, ioctx.operate("foo.copy2", &op));

    bufferlist bl2, x2;
    ASSERT_EQ((int)bl.length(), ioctx.read("foo.copy2", bl2, 10000, 0));
    ASSERT_TRUE(bl.contents_equal(bl2));
    ASSERT_EQ((int)x.length(), ioctx.getxattr("foo.copy2", "myattr", x2));
    ASSERT_TRUE(x.contents_equal(x2));
  }

  // do a big object
  bl.append(buffer::create(g_conf->osd_copyfrom_max_chunk * 3));
  bl.zero();
  bl.append("tail");
  blc = bl;
  xc = x;
  ASSERT_EQ(0, ioctx.write_full("big", blc));
  ASSERT_EQ(0, ioctx.setxattr("big", "myattr", xc));

  {
    ObjectWriteOperation op;
    op.copy_from("big", ioctx, ioctx.get_last_version());
    ASSERT_EQ(0, ioctx.operate("big.copy", &op));

    bufferlist bl2, x2;
    ASSERT_EQ((int)bl.length(), ioctx.read("big.copy", bl2, bl.length(), 0));
    ASSERT_TRUE(bl.contents_equal(bl2));
    ASSERT_EQ((int)x.length(), ioctx.getxattr("foo.copy", "myattr", x2));
    ASSERT_TRUE(x.contents_equal(x2));
  }

  {
    ObjectWriteOperation op;
    op.copy_from("big", ioctx, 0);
    ASSERT_EQ(0, ioctx.operate("big.copy2", &op));

    bufferlist bl2, x2;
    ASSERT_EQ((int)bl.length(), ioctx.read("big.copy2", bl2, bl.length(), 0));
    ASSERT_TRUE(bl.contents_equal(bl2));
    ASSERT_EQ((int)x.length(), ioctx.getxattr("foo.copy2", "myattr", x2));
    ASSERT_TRUE(x.contents_equal(x2));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  return RUN_ALL_TESTS();
}
