// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*
// vim: ts=8 sw=2 smarttab

#include <climits>
#include <vector>

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include "common/ceph_argparse.h"

#include <errno.h>
#include "gtest/gtest.h"

using namespace librados;
using std::string;
using std::vector;

typedef RadosTestPP LibRadosMultiObjectOperationPP;

TEST_F(LibRadosMultiObjectOperationPP, SimplePP) {
  string b1("abcd"), b2("pqrs");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  MultiObjectWriteOperation writes;
  writes.master.write(0, bl1);
  writes.slave("bar").write(0, bl2);
  ASSERT_EQ(0, ioctx.operate("foo", &writes));

  ObjectReadOperation read1;
  read1.read(0, b1.size(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read1, &bl1));
  ASSERT_EQ(0, b1.compare(0, b1.size(), bl1.c_str(), bl1.length()));

  ObjectReadOperation read2;
  read2.read(0, b2.size(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("bar", &read2, &bl2));
  ASSERT_EQ(0, b2.compare(0, b2.size(), bl2.c_str(), bl2.length()));
}

struct ObjectsWriter
{
  struct obj {
    string oid;
    string value;
    bufferlist bl;
  };
  vector<obj> list;
  obj &master;

  ObjectsWriter(int count, int _master, const char *value)
    : list(count)
    , master(list[_master])
  {
    for (int i = 0; i < count; ++i) {
      char buf[1024];
      obj &c = list[i];

      snprintf(buf, sizeof(buf), "oid%doid", i);
      c.oid = buf;
      snprintf(buf, sizeof(buf), "%s%d%s", value, i, value);
      c.value = buf;
    }
  }

  void fill(MultiObjectWriteOperation &w)
  {
    master.bl.clear();
    master.bl.append(master.value);
    w.master.write(0, master.bl);
    for (vector<obj>::iterator p = list.begin();
         p != list.end(); ++p) {
      if (p->oid == master.oid)
        continue;
      p->bl.clear();
      p->bl.append(p->value);
      w.slave(p->oid).write(0, p->bl);
    }
  }
};

TEST_F(LibRadosMultiObjectOperationPP, WriteObjects64PP) {
  ObjectsWriter writer(64, 0, "value");

  {
    MultiObjectWriteOperation w;
    writer.fill(w);
    ASSERT_EQ(0, ioctx.operate(writer.master.oid, &w));
  }

  for (vector<ObjectsWriter::obj>::iterator p = writer.list.begin();
       p != writer.list.end(); ++p) {
    ObjectReadOperation r;
    r.read(0, p->value.size(), NULL, NULL);
    std::cout << "Read " << p->oid;
    ASSERT_EQ(0, ioctx.operate(p->oid, &r, &p->bl));
    ASSERT_EQ(0, p->value.compare(0, p->value.size(), p->bl.c_str(), p->bl.length()));
    std::cout << " OK" << std::endl;
  }
}

TEST_F(LibRadosMultiObjectOperationPP, AtomicPP) {
  ObjectsWriter ow1(8, 0, "xxx");
  ObjectsWriter ow2(8, 4, "yyy");

  int r1, r2;
  MultiObjectWriteOperation w1, w2;
  ow1.fill(w1);
  ow2.fill(w2);
  AioCompletion *c1 = cluster.aio_create_completion();
  AioCompletion *c2 = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_operate(ow1.master.oid, c1, &w1));
  ASSERT_EQ(0, ioctx.aio_operate(ow2.master.oid, c2, &w2));
  c1->wait_for_safe();
  c2->wait_for_safe();
  r1 = c1->get_return_value();
  r2 = c2->get_return_value();
  c1->release();
  c2->release();

  ASSERT_EQ(true, ((r1 == 0) || (r1 == -EDEADLK)));
  ASSERT_EQ(true, ((r2 == 0) || (r2 == -EDEADLK)));

  std::cout << "op1 r: " << r1 << std::endl;
  std::cout << "op2 r: " << r2 << std::endl;

  if (r1 == 0 || r2 == 0) {
    string match;
    for (vector<ObjectsWriter::obj>::iterator p = ow1.list.begin();
         p != ow1.list.end(); ++p) {
      ObjectReadOperation r;
      r.read(0, 3, NULL, NULL);
      ASSERT_EQ(0, ioctx.operate(p->oid, &r, &p->bl));
      if (match.empty())
        match = p->bl.c_str();
      else
        ASSERT_EQ(0, match.compare(0, 3, p->bl.c_str(), p->bl.length()));
    }
  } else {
    for (vector<ObjectsWriter::obj>::iterator p = ow1.list.begin();
         p != ow1.list.end(); ++p) {
      ObjectReadOperation r;
      r.stat(NULL, NULL, NULL);
      ASSERT_EQ(-ENOENT, ioctx.operate(p->oid, &r, &p->bl));
    }
  }
}

TEST_F(LibRadosMultiObjectOperationPP, AtomicSameMasterPP) {
  ObjectsWriter ow1(8, 0, "xxx");
  ObjectsWriter ow2(8, 0, "yyy");

  int r1, r2;
  MultiObjectWriteOperation w1, w2;
  ow1.fill(w1);
  ow2.fill(w2);
  AioCompletion *c1 = cluster.aio_create_completion();
  AioCompletion *c2 = cluster.aio_create_completion();
  ASSERT_EQ(0, ioctx.aio_operate(ow1.master.oid, c1, &w1));
  ASSERT_EQ(0, ioctx.aio_operate(ow2.master.oid, c2, &w2));
  c1->wait_for_safe();
  c2->wait_for_safe();
  r1 = c1->get_return_value();
  r2 = c2->get_return_value();
  c1->release();
  c2->release();

  ASSERT_EQ(true, ((r1 == 0) || (r1 == -EDEADLK)));
  ASSERT_EQ(true, ((r2 == 0) || (r2 == -EDEADLK)));

  std::cout << "op1 r: " << r1 << std::endl;
  std::cout << "op2 r: " << r2 << std::endl;

  if (r1 == 0 || r2 == 0) {
    string match;
    for (vector<ObjectsWriter::obj>::iterator p = ow1.list.begin();
         p != ow1.list.end(); ++p) {
      ObjectReadOperation r;
      r.read(0, 3, NULL, NULL);
      ASSERT_EQ(0, ioctx.operate(p->oid, &r, &p->bl));
      if (match.empty())
        match = p->bl.c_str();
      else
        ASSERT_EQ(0, match.compare(0, 3, p->bl.c_str(), p->bl.length()));
    }
  } else {
    for (vector<ObjectsWriter::obj>::iterator p = ow1.list.begin();
         p != ow1.list.end(); ++p) {
      ObjectReadOperation r;
      r.stat(NULL, NULL, NULL);
      ASSERT_EQ(-ENOENT, ioctx.operate(p->oid, &r, &p->bl));
    }
  }
}

TEST_F(LibRadosMultiObjectOperationPP, SlaveFailureOperationPP) {
  string b1("abcd"), b2("pqrs");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  ASSERT_EQ(0, ioctx.create("bar", true));

  MultiObjectWriteOperation writes;
  writes.master.write(0, bl1);
  writes.slave("bar").create(true);
  writes.slave("bar").write(0, bl2);
  ASSERT_EQ(-EEXIST, ioctx.operate("foo", &writes));

  ASSERT_EQ(-ENOENT, ioctx.stat("foo", NULL, NULL));
}

TEST_F(LibRadosMultiObjectOperationPP, MasterFailureOperationPP) {
  string b1("abcd"), b2("pqrs");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  ASSERT_EQ(0, ioctx.create("foo", true));

  MultiObjectWriteOperation writes;
  writes.master.create(true);
  writes.master.write(0, bl1);
  writes.slave("bar").write(0, bl2);
  ASSERT_EQ(-EEXIST, ioctx.operate("foo", &writes));

  ASSERT_EQ(-ENOENT, ioctx.stat("bar", NULL, NULL));
}

TEST_F(LibRadosMultiObjectOperationPP, SlaveOperationCombinationDataPP) {
  string b1("mmmm"), b2("abcdef");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("create", w));
  }

  {
    MultiObjectWriteOperation writes;
    writes.master.create(false);
    writes.master.write(0, bl1);
    writes.slave("write").write(8, bl2);
    writes.slave("writefull").write_full(bl2);
    writes.slave("truncate").write_full(bl2);
    writes.slave("truncate").truncate(4);
    writes.slave("zero").write_full(bl2);
    writes.slave("zero").zero(2, 3);
    writes.slave("delete").create(false);
    writes.slave("delete").remove();
    writes.slave("append").write_full(bl2);
    writes.slave("append").append(bl1);
    writes.slave("create").remove();
    writes.slave("create").create(true);
    writes.slave("create").write_full(bl2);
    ASSERT_EQ(0, ioctx.operate("foo", &writes));
  }

  {
    bufferlist r;
    bufferlist m;
    m.append(bl1);
    ASSERT_EQ(m.length(), ioctx.read("foo", r, 1024, 0));
    ASSERT_EQ(true, r.contents_equal(m));
  }
  {
    bufferlist r;
    bufferlist m;
    m.append_zero(8);
    m.append(bl2);
    ASSERT_EQ(m.length(), ioctx.read("write", r, 1024, 0));
    ASSERT_EQ(true, r.contents_equal(m));
  }
  {
    bufferlist r;
    bufferlist m;
    m.append(bl2);
    ASSERT_EQ(m.length(), ioctx.read("writefull", r, 1024, 0));
    ASSERT_EQ(true, r.contents_equal(m));
  }
  {
    bufferlist r;
    bufferlist m;
    m.substr_of(bl2, 0, 4);
    ASSERT_EQ(m.length(), ioctx.read("truncate", r, 1024, 0));
    ASSERT_EQ(true, r.contents_equal(m));
  }
  {
    bufferlist r;
    bufferlist m;
    m.append(bl2);
    m.rebuild();
    m.zero(2, 3);
    ASSERT_EQ(m.length(), ioctx.read("zero", r, 1024, 0));
    ASSERT_EQ(true, r.contents_equal(m));
  }
  {
    ASSERT_EQ(-ENOENT, ioctx.stat("delete", NULL, NULL));
  }
  {
    bufferlist r;
    bufferlist m;
    m.append(bl2);
    m.append(bl1);
    ASSERT_EQ(m.length(), ioctx.read("append", r, 1024, 0));
    ASSERT_EQ(true, r.contents_equal(m));
  }
  {
    bufferlist r;
    bufferlist m;
    m.append(bl2);
    ASSERT_EQ(m.length(), ioctx.read("create", r, 1024, 0));
    ASSERT_EQ(true, r.contents_equal(m));
  }
}

TEST_F(LibRadosMultiObjectOperationPP, SlaveOperationCombinationXattrPP) {
  string b1("mmmm"), b2("abcdef");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.setxattr("foo", "init", w));
    ASSERT_EQ(0, ioctx.setxattr("foo", "do", w));
  }
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.setxattr("remove", "init", w));
    ASSERT_EQ(0, ioctx.setxattr("remove", "do", w));
  }
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.setxattr("modify", "init", w));
    ASSERT_EQ(0, ioctx.setxattr("modify", "do", w));
  }
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.setxattr("create", "init", w));
  }

  {
    MultiObjectWriteOperation writes;
    writes.master.cmpxattr("init", CEPH_OSD_CMPXATTR_OP_EQ, bl1);
    writes.master.rmxattr("do");
    writes.master.setxattr("do2", bl2);
    writes.slave("remove").cmpxattr("init", CEPH_OSD_CMPXATTR_OP_EQ, bl1);
    writes.slave("remove").rmxattr("do");
    writes.slave("modify").cmpxattr("init", CEPH_OSD_CMPXATTR_OP_EQ, bl1);
    writes.slave("modify").setxattr("do", bl2);
    writes.slave("create").cmpxattr("init", CEPH_OSD_CMPXATTR_OP_EQ, bl1);
    writes.slave("create").setxattr("do", bl2);
    ASSERT_EQ(0, ioctx.operate("foo", &writes));
  }

  {
    bufferlist r;
    ASSERT_EQ(-ENODATA, ioctx.getxattr("foo", "do", r));
    ASSERT_EQ(bl2.length(), ioctx.getxattr("foo", "do2", r));
    ASSERT_EQ(true, r.contents_equal(bl2));
  }
  {
    bufferlist r;
    ASSERT_EQ(-ENODATA, ioctx.getxattr("remove", "do", r));
  }
  {
    bufferlist r;
    ASSERT_EQ(bl2.length(), ioctx.getxattr("modify", "do", r));
    ASSERT_EQ(true, r.contents_equal(bl2));
  }
  {
    bufferlist r;
    ASSERT_EQ(bl2.length(), ioctx.getxattr("create", "do", r));
    ASSERT_EQ(true, r.contents_equal(bl2));
  }
}

TEST_F(LibRadosMultiObjectOperationPP, SlaveOperationCombinationOmapPP) {
  string b1("mmmm"), b2("abcdef");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  {
    map<string, bufferlist> m;
    m.insert(make_pair("init", bl1));
    ASSERT_EQ(0, ioctx.omap_set("create", m));
    m.insert(make_pair("do", bl1));
    ASSERT_EQ(0, ioctx.omap_set("foo", m));
    ASSERT_EQ(0, ioctx.omap_set("remove", m));
    ASSERT_EQ(0, ioctx.omap_set("modify", m));
    ASSERT_EQ(0, ioctx.omap_set("clear", m));
  }

  {
    std::map<std::string, std::pair<bufferlist, int> > assertions;
    assertions.insert(make_pair("init", make_pair(bl1, CEPH_OSD_CMPXATTR_OP_EQ)));

    std::set<std::string> rm_do;
    rm_do.insert("do");
    std::map<std::string, bufferlist> set_do;
    set_do.insert(make_pair("do", bl2));
    std::map<std::string, bufferlist> set_do2;
    set_do2.insert(make_pair("do2", bl2));

    MultiObjectWriteOperation writes;
    writes.master.omap_cmp(assertions, NULL);
    writes.master.omap_rm_keys(rm_do);
    writes.master.omap_set(set_do2);
    writes.master.omap_set_header(bl2);
    writes.slave("remove").omap_cmp(assertions, NULL);
    writes.slave("remove").omap_rm_keys(rm_do);
    writes.slave("modify").omap_cmp(assertions, NULL);
    writes.slave("modify").omap_set(set_do);
    writes.slave("create").omap_cmp(assertions, NULL);
    writes.slave("create").omap_set(set_do);
    writes.slave("clear").omap_cmp(assertions, NULL);
    writes.slave("clear").omap_clear();
    ASSERT_EQ(0, ioctx.operate("foo", &writes));
  }

  {
    map<std::string, bufferlist> vals;
    ASSERT_EQ(0, ioctx.omap_get_vals("foo", "", 10, &vals));
    ASSERT_EQ(2, vals.size());
    ASSERT_EQ(true, vals.count("do") == 0);
    ASSERT_EQ(true, vals.count("do2") > 0);
    ASSERT_EQ(true, bl2.contents_equal(vals["do2"]));
    bufferlist r;
    ASSERT_EQ(0, ioctx.omap_get_header("foo", &r));
    ASSERT_EQ(true, r.contents_equal(bl2));
  }
  {
    map<std::string, bufferlist> vals;
    ASSERT_EQ(0, ioctx.omap_get_vals("remove", "", 10, &vals));
    ASSERT_EQ(1, vals.size());
    ASSERT_EQ(true, vals.count("do") == 0);
  }
  {
    map<std::string, bufferlist> vals;
    ASSERT_EQ(0, ioctx.omap_get_vals("modify", "", 10, &vals));
    ASSERT_EQ(2, vals.size());
    ASSERT_EQ(true, vals.count("do") > 0);
    ASSERT_EQ(true, bl2.contents_equal(vals["do"]));
  }
  {
    map<std::string, bufferlist> vals;
    ASSERT_EQ(0, ioctx.omap_get_vals("create", "", 10, &vals));
    ASSERT_EQ(2, vals.size());
    ASSERT_EQ(true, vals.count("do") > 0);
    ASSERT_EQ(true, bl2.contents_equal(vals["do"]));
  }
  {
    map<std::string, bufferlist> vals;
    ASSERT_EQ(0, ioctx.omap_get_vals("clear", "", 10, &vals));
    ASSERT_EQ(0, vals.size());
  }
}

TEST_F(LibRadosMultiObjectOperationPP, MasterFailureAssertVersionPP) {
  string b1("mmmm"), b2("abcdef");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  uint64_t v1, v2;
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("v1", w));
    ASSERT_EQ(bl1.length(), ioctx.read("v1", w, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(w));
    v1 = ioctx.get_last_version();
  }
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("v2", w));
    ASSERT_EQ(bl1.length(), ioctx.read("v2", w, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(w));
    v2 = ioctx.get_last_version();
  }

  {
    MultiObjectWriteOperation writes;
    writes.master.create(true);
    writes.master.write_full(bl2);
    writes.slave("v2").write_full(bl2);
    ASSERT_EQ(-EEXIST, ioctx.operate("v1", &writes));
  }

  {
    bufferlist r;
    ObjectReadOperation o;
    o.assert_version(v1);
    o.read(0, 1024, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("v1", &o, &r));
    ASSERT_EQ(true, bl1.contents_equal(r));
  }
  {
    bufferlist r;
    ObjectReadOperation o;
    o.assert_version(v2);
    o.read(0, 1024, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("v2", &o, &r));
    ASSERT_EQ(true, bl1.contents_equal(r));
  }
}

TEST_F(LibRadosMultiObjectOperationPP, SlaveFailureAssertVersionPP) {
  string b1("mmmm"), b2("abcdef");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  uint64_t v1, v2, v3;
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("v1", w));
    ASSERT_EQ(bl1.length(), ioctx.read("v1", w, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(w));
    v1 = ioctx.get_last_version();
  }
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("v2", w));
    ASSERT_EQ(bl1.length(), ioctx.read("v2", w, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(w));
    v2 = ioctx.get_last_version();
  }
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("v3", w));
    ASSERT_EQ(bl1.length(), ioctx.read("v3", w, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(w));
    v3 = ioctx.get_last_version();
  }

  {
    MultiObjectWriteOperation writes;
    writes.master.write_full(bl2);
    writes.slave("v2").create(true);
    writes.slave("v2").write_full(bl2);
    writes.slave("v3").write_full(bl2);
    ASSERT_EQ(-EEXIST, ioctx.operate("v1", &writes));
  }

  {
    bufferlist r;
    ObjectReadOperation o;
    o.assert_version(v1);
    o.read(0, 1024, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("v1", &o, &r));
    ASSERT_EQ(true, bl1.contents_equal(r));
  }
  {
    bufferlist r;
    ObjectReadOperation o;
    o.assert_version(v2);
    o.read(0, 1024, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("v2", &o, &r));
    ASSERT_EQ(true, bl1.contents_equal(r));
  }
  {
    bufferlist r;
    ObjectReadOperation o;
    o.assert_version(v3);
    o.read(0, 1024, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("v3", &o, &r));
    ASSERT_EQ(true, bl1.contents_equal(r));
  }
}

TEST_F(LibRadosMultiObjectOperationPP, SuccessAssertVersionPP) {
  string b1("mmmm"), b2("abcdef");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  uint64_t v1_old, v1, v2, v3;
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("v1", w));
    ASSERT_EQ(bl1.length(), ioctx.read("v1", w, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(w));
    v1_old = ioctx.get_last_version();
  }
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("v2", w));
    ASSERT_EQ(bl1.length(), ioctx.read("v2", w, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(w));
    v2 = ioctx.get_last_version();
  }
  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("v3", w));
    ASSERT_EQ(bl1.length(), ioctx.read("v3", w, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(w));
    v3 = ioctx.get_last_version();
  }
  {
    MultiObjectWriteOperation writes;
    writes.master.assert_version(v1_old);
    writes.master.write_full(bl2);
    writes.slave("v2").write_full(bl2);
    writes.slave("v3").assert_exists();
    ASSERT_EQ(0, ioctx.operate("v1", &writes));
    v1 = ioctx.get_last_version();
    ASSERT_EQ(true, v1 > v1_old);
  }
  {
    bufferlist r;
    ObjectReadOperation o;
    o.assert_version(v1);
    o.read(0, 1024, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("v1", &o, &r));
    ASSERT_EQ(true, bl2.contents_equal(r));
  }
  {
    bufferlist r;
    ObjectReadOperation o;
    o.assert_version(v2);
    o.read(0, 1024, NULL, NULL);
    ASSERT_EQ(-ERANGE, ioctx.operate("v2", &o, &r));
  }
  {
    bufferlist r;
    ObjectReadOperation o;
    o.assert_version(v3);
    o.read(0, 1024, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("v3", &o, &r));
    ASSERT_EQ(true, bl1.contents_equal(r));
  }
}

TEST_F(LibRadosMultiObjectOperationPP, SuccessOperationWithSnapshotPP) {
  string b1("abcd"), b2("pqrs");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("bar", w));
  }
  ASSERT_EQ(0, ioctx.snap_create("test"));
  ASSERT_EQ(0, ioctx.remove("bar"));
  ASSERT_EQ(-ENOENT, ioctx.stat("bar", NULL, NULL));
  snap_t snap;
  ASSERT_EQ(0, ioctx.snap_lookup("test", &snap));
  ioctx.snap_set_read(snap);
  ASSERT_EQ(0, ioctx.stat("bar", NULL, NULL));
  ioctx.snap_set_read(0);

  MultiObjectWriteOperation writes;
  writes.master.create(true);
  writes.slave("bar").write(0, bl2);
  ASSERT_EQ(0, ioctx.operate("foo", &writes));

  ASSERT_EQ(0, ioctx.stat("foo", NULL, NULL));
  {
    bufferlist r;
    ASSERT_EQ(bl2.length(), ioctx.read("bar", r, 1024, 0));
    ASSERT_EQ(true, bl2.contents_equal(r));
  }

  ASSERT_EQ(0, ioctx.snap_remove("test"));
}

TEST_F(LibRadosMultiObjectOperationPP, FailureOperationWithSnapshotPP) {
  string b1("abcd"), b2("pqrs");
  bufferlist bl1, bl2;
  bl1.append(b1.c_str(), b1.size());
  bl2.append(b2.c_str(), b2.size());

  {
    bufferlist w;
    w.append(bl1);
    ASSERT_EQ(0, ioctx.write_full("bar", w));
  }
  ASSERT_EQ(0, ioctx.snap_create("test"));
  ASSERT_EQ(0, ioctx.remove("bar"));
  ASSERT_EQ(-ENOENT, ioctx.stat("bar", NULL, NULL));
  snap_t snap;
  ASSERT_EQ(0, ioctx.snap_lookup("test", &snap));
  ioctx.snap_set_read(snap);
  ASSERT_EQ(0, ioctx.stat("bar", NULL, NULL));
  ioctx.snap_set_read(0);

  ASSERT_EQ(0, ioctx.create("foo", true));

  MultiObjectWriteOperation writes;
  writes.master.create(true);
  writes.slave("bar").write(0, bl2);
  ASSERT_EQ(-EEXIST, ioctx.operate("foo", &writes));

  ASSERT_EQ(-ENOENT, ioctx.stat("bar", NULL, NULL));
  {
    ioctx.snap_set_read(snap);
    bufferlist r;
    ASSERT_EQ(bl1.length(), ioctx.read("bar", r, 1024, 0));
    ASSERT_EQ(true, bl1.contents_equal(r));
  }

  ASSERT_EQ(0, ioctx.snap_remove("test"));
}
