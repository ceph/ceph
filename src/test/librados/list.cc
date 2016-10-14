// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"

#include "include/types.h"
#include "common/hobject.h"
#include "gtest/gtest.h"
#include <errno.h>
#include <string>
#include <stdexcept>

using namespace librados;

typedef RadosTestNS LibRadosList;
typedef RadosTestPPNS LibRadosListPP;
typedef RadosTestECNS LibRadosListEC;
typedef RadosTestECPPNS LibRadosListECPP;
typedef RadosTestNP LibRadosListNP;

TEST_F(LibRadosList, ListObjects) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  const char *entry;
  bool foundit = false;
  while (rados_objects_list_next(ctx, &entry, NULL) != -ENOENT) {
    foundit = true;
    ASSERT_EQ(std::string(entry), "foo");
  }
  ASSERT_TRUE(foundit);
  rados_objects_list_close(ctx);
}


#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

TEST_F(LibRadosListPP, ListObjectsPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ObjectIterator iter(ioctx.objects_begin());
  bool foundit = false;
  while (iter != ioctx.objects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).first, "foo");
    ++iter;
  }
  ASSERT_TRUE(foundit);
}

TEST_F(LibRadosListPP, ListObjectsTwicePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ObjectIterator iter(ioctx.objects_begin());
  bool foundit = false;
  while (iter != ioctx.objects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).first, "foo");
    ++iter;
  }
  ASSERT_TRUE(foundit);
  ++iter;
  ASSERT_TRUE(iter == ioctx.objects_end());
  foundit = false;
  iter.seek(0);
  while (iter != ioctx.objects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).first, "foo");
    ++iter;
  }
  ASSERT_TRUE(foundit);
}

TEST_F(LibRadosListPP, ListObjectsCopyIterPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  // make sure this is still valid after the original iterators are gone
  ObjectIterator iter3;
  {
    ObjectIterator iter(ioctx.objects_begin());
    ObjectIterator iter2(iter);
    iter3 = iter2;
    ASSERT_EQ((*iter).first, "foo");
    ++iter;
    ASSERT_TRUE(iter == ioctx.objects_end());
    ++iter;
    ASSERT_TRUE(iter == ioctx.objects_end());

    ASSERT_EQ(iter2->first, "foo");
    ASSERT_EQ(iter3->first, "foo");
    ++iter2;
    ASSERT_TRUE(iter2 == ioctx.objects_end());
  }

  ASSERT_EQ(iter3->first, "foo");
  iter3 = iter3;
  ASSERT_EQ(iter3->first, "foo");
  ++iter3;
  ASSERT_TRUE(iter3 == ioctx.objects_end());
}

TEST_F(LibRadosListPP, ListObjectsEndIter) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  ObjectIterator iter(ioctx.objects_begin());
  ObjectIterator iter_end(ioctx.objects_end());
  ObjectIterator iter_end2 = ioctx.objects_end();
  ASSERT_TRUE(iter_end == iter_end2);
  ASSERT_TRUE(iter_end == ioctx.objects_end());
  ASSERT_TRUE(iter_end2 == ioctx.objects_end());

  ASSERT_EQ(iter->first, "foo");
  ++iter;
  ASSERT_TRUE(iter == ioctx.objects_end());
  ASSERT_TRUE(iter == iter_end);
  ASSERT_TRUE(iter == iter_end2);
  ObjectIterator iter2 = iter;
  ASSERT_TRUE(iter2 == ioctx.objects_end());
  ASSERT_TRUE(iter2 == iter_end);
  ASSERT_TRUE(iter2 == iter_end2);
}

static void check_list(std::set<std::string>& myset, rados_list_ctx_t& ctx)
{
  const char *entry;
  std::set<std::string> orig_set(myset);
  /**
   * During splitting, we might see duplicate items.
   * We assert that every object returned is in myset and that
   * we don't hit ENOENT until we have hit every item in myset
   * at least once.
   */
  while (rados_objects_list_next(ctx, &entry, NULL) != -ENOENT) {
    ASSERT_TRUE(orig_set.end() != orig_set.find(std::string(entry)));
    myset.erase(std::string(entry));
  }
  ASSERT_TRUE(myset.empty());
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

TEST_F(LibRadosList, ListObjectsNS) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  // Create :foo1, :foo2, :foo3, n1:foo1, ns1:foo4, ns1:foo5, ns2:foo6, n2:foo7
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_write(ioctx, "foo1", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "ns1");
  ASSERT_EQ(0, rados_write(ioctx, "foo1", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_write(ioctx, "foo2", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo3", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "ns1");
  ASSERT_EQ(0, rados_write(ioctx, "foo4", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo5", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "ns2");
  ASSERT_EQ(0, rados_write(ioctx, "foo6", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo7", buf, sizeof(buf), 0));

  std::set<std::string> def, ns1, ns2;
  def.insert(std::string("foo1"));
  def.insert(std::string("foo2"));
  def.insert(std::string("foo3"));
  ns1.insert(std::string("foo1"));
  ns1.insert(std::string("foo4"));
  ns1.insert(std::string("foo5"));
  ns2.insert(std::string("foo6"));
  ns2.insert(std::string("foo7"));

  rados_list_ctx_t ctx;
  // Check default namespace ""
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  check_list(def, ctx);
  rados_objects_list_close(ctx);

  // Check default namespace "ns1"
  rados_ioctx_set_namespace(ioctx, "ns1");
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  check_list(ns1, ctx);
  rados_objects_list_close(ctx);

  // Check default namespace "ns2"
  rados_ioctx_set_namespace(ioctx, "ns2");
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  check_list(ns2, ctx);
  rados_objects_list_close(ctx);

  // Can't specify all namespaces using old interface
  rados_ioctx_set_namespace(ioctx, LIBRADOS_ALL_NSPACES);
  ASSERT_EQ(-EINVAL, rados_objects_list_open(ioctx, &ctx));
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

static void check_listpp(std::set<std::string>& myset, IoCtx& ioctx)
{
  ObjectIterator iter(ioctx.objects_begin());
  std::set<std::string> orig_set(myset);
  /**
   * During splitting, we might see duplicate items.
   * We assert that every object returned is in myset and that
   * we don't hit ENOENT until we have hit every item in myset
   * at least once.
   */
  while (iter != ioctx.objects_end()) {
    ASSERT_TRUE(orig_set.end() != orig_set.find(std::string((*iter).first)));
    myset.erase(std::string((*iter).first));
    ++iter;
  }
  ASSERT_TRUE(myset.empty());
}

TEST_F(LibRadosListPP, ListObjectsPPNS) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  // Create :foo1, :foo2, :foo3, n1:foo1, ns1:foo4, ns1:foo5, ns2:foo6, n2:foo7
  ioctx.set_namespace("");
  ASSERT_EQ(0, ioctx.write("foo1", bl1, sizeof(buf), 0));
  ioctx.set_namespace("ns1");
  ASSERT_EQ(0, ioctx.write("foo1", bl1, sizeof(buf), 0));
  ioctx.set_namespace("");
  ASSERT_EQ(0, ioctx.write("foo2", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo3", bl1, sizeof(buf), 0));
  ioctx.set_namespace("ns1");
  ASSERT_EQ(0, ioctx.write("foo4", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo5", bl1, sizeof(buf), 0));
  ioctx.set_namespace("ns2");
  ASSERT_EQ(0, ioctx.write("foo6", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo7", bl1, sizeof(buf), 0));

  std::set<std::string> def, ns1, ns2;
  def.insert(std::string("foo1"));
  def.insert(std::string("foo2"));
  def.insert(std::string("foo3"));
  ns1.insert(std::string("foo1"));
  ns1.insert(std::string("foo4"));
  ns1.insert(std::string("foo5"));
  ns2.insert(std::string("foo6"));
  ns2.insert(std::string("foo7"));

  ioctx.set_namespace("");
  check_listpp(def, ioctx);

  ioctx.set_namespace("ns1");
  check_listpp(ns1, ioctx);

  ioctx.set_namespace("ns2");
  check_listpp(ns2, ioctx);

  ioctx.set_namespace(all_nspaces);
  EXPECT_THROW(check_listpp(def, ioctx), std::runtime_error);
}

TEST_F(LibRadosListPP, ListObjectsManyPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  for (int i=0; i<256; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, bl.length(), 0));
  }

  librados::ObjectIterator it = ioctx.objects_begin();
  std::set<std::string> saw_obj;
  std::set<int> saw_pg;
  for (; it != ioctx.objects_end(); ++it) {
    std::cout << it->first
	      << " " << it.get_pg_hash_position() << std::endl;
    saw_obj.insert(it->first);
    saw_pg.insert(it.get_pg_hash_position());
  }
  std::cout << "saw " << saw_pg.size() << " pgs " << std::endl;

  // make sure they are 0..n
  for (unsigned i = 0; i < saw_pg.size(); ++i)
    ASSERT_TRUE(saw_pg.count(i));
}

TEST_F(LibRadosList, ListObjectsStart) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));

  for (int i=0; i<16; ++i) {
    string n = stringify(i);
    ASSERT_EQ(0, rados_write(ioctx, n.c_str(), buf, sizeof(buf), 0));
  }

  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  std::map<int, std::set<std::string> > pg_to_obj;
  const char *entry;
  while (rados_objects_list_next(ctx, &entry, NULL) == 0) {
    uint32_t pos = rados_objects_list_get_pg_hash_position(ctx);
    std::cout << entry << " " << pos << std::endl;
    pg_to_obj[pos].insert(entry);
  }
  rados_objects_list_close(ctx);

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, rados_objects_list_seek(ctx, p->first));
    ASSERT_EQ(0, rados_objects_list_next(ctx, &entry, NULL));
    std::cout << "have " << entry << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(entry));
    ++p;
  }
  rados_objects_list_close(ctx);
}

TEST_F(LibRadosListPP, ListObjectsStartPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  for (int i=0; i<16; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, bl.length(), 0));
  }

  librados::ObjectIterator it = ioctx.objects_begin();
  std::map<int, std::set<std::string> > pg_to_obj;
  for (; it != ioctx.objects_end(); ++it) {
    std::cout << it->first << " " << it.get_pg_hash_position() << std::endl;
    pg_to_obj[it.get_pg_hash_position()].insert(it->first);
  }

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  it = ioctx.objects_begin(p->first);
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, it.seek(p->first));
    std::cout << "have " << it->first << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(it->first));
    ++p;
  }
}

TEST_F(LibRadosListEC, ListObjects) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  const char *entry;
  bool foundit = false;
  while (rados_objects_list_next(ctx, &entry, NULL) != -ENOENT) {
    foundit = true;
    ASSERT_EQ(std::string(entry), "foo");
  }
  ASSERT_TRUE(foundit);
  rados_objects_list_close(ctx);
}

TEST_F(LibRadosListECPP, ListObjectsPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ObjectIterator iter(ioctx.objects_begin());
  bool foundit = false;
  while (iter != ioctx.objects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).first, "foo");
    ++iter;
  }
  ASSERT_TRUE(foundit);
}

TEST_F(LibRadosListECPP, ListObjectsTwicePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  ObjectIterator iter(ioctx.objects_begin());
  bool foundit = false;
  while (iter != ioctx.objects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).first, "foo");
    ++iter;
  }
  ASSERT_TRUE(foundit);
  ++iter;
  ASSERT_TRUE(iter == ioctx.objects_end());
  foundit = false;
  iter.seek(0);
  while (iter != ioctx.objects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).first, "foo");
    ++iter;
  }
  ASSERT_TRUE(foundit);
}

TEST_F(LibRadosListECPP, ListObjectsCopyIterPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  // make sure this is still valid after the original iterators are gone
  ObjectIterator iter3;
  {
    ObjectIterator iter(ioctx.objects_begin());
    ObjectIterator iter2(iter);
    iter3 = iter2;
    ASSERT_EQ((*iter).first, "foo");
    ++iter;
    ASSERT_TRUE(iter == ioctx.objects_end());
    ++iter;
    ASSERT_TRUE(iter == ioctx.objects_end());

    ASSERT_EQ(iter2->first, "foo");
    ASSERT_EQ(iter3->first, "foo");
    ++iter2;
    ASSERT_TRUE(iter2 == ioctx.objects_end());
  }

  ASSERT_EQ(iter3->first, "foo");
  iter3 = iter3;
  ASSERT_EQ(iter3->first, "foo");
  ++iter3;
  ASSERT_TRUE(iter3 == ioctx.objects_end());
}

TEST_F(LibRadosListECPP, ListObjectsEndIter) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  ObjectIterator iter(ioctx.objects_begin());
  ObjectIterator iter_end(ioctx.objects_end());
  ObjectIterator iter_end2 = ioctx.objects_end();
  ASSERT_TRUE(iter_end == iter_end2);
  ASSERT_TRUE(iter_end == ioctx.objects_end());
  ASSERT_TRUE(iter_end2 == ioctx.objects_end());

  ASSERT_EQ(iter->first, "foo");
  ++iter;
  ASSERT_TRUE(iter == ioctx.objects_end());
  ASSERT_TRUE(iter == iter_end);
  ASSERT_TRUE(iter == iter_end2);
  ObjectIterator iter2 = iter;
  ASSERT_TRUE(iter2 == ioctx.objects_end());
  ASSERT_TRUE(iter2 == iter_end);
  ASSERT_TRUE(iter2 == iter_end2);
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

TEST_F(LibRadosListEC, ListObjectsNS) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  // Create :foo1, :foo2, :foo3, n1:foo1, ns1:foo4, ns1:foo5, ns2:foo6, n2:foo7
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_write(ioctx, "foo1", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "ns1");
  ASSERT_EQ(0, rados_write(ioctx, "foo1", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_write(ioctx, "foo2", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo3", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "ns1");
  ASSERT_EQ(0, rados_write(ioctx, "foo4", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo5", buf, sizeof(buf), 0));
  rados_ioctx_set_namespace(ioctx, "ns2");
  ASSERT_EQ(0, rados_write(ioctx, "foo6", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_write(ioctx, "foo7", buf, sizeof(buf), 0));

  std::set<std::string> def, ns1, ns2;
  def.insert(std::string("foo1"));
  def.insert(std::string("foo2"));
  def.insert(std::string("foo3"));
  ns1.insert(std::string("foo1"));
  ns1.insert(std::string("foo4"));
  ns1.insert(std::string("foo5"));
  ns2.insert(std::string("foo6"));
  ns2.insert(std::string("foo7"));

  rados_list_ctx_t ctx;
  // Check default namespace ""
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  check_list(def, ctx);
  rados_objects_list_close(ctx);

  // Check default namespace "ns1"
  rados_ioctx_set_namespace(ioctx, "ns1");
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  check_list(ns1, ctx);
  rados_objects_list_close(ctx);

  // Check default namespace "ns2"
  rados_ioctx_set_namespace(ioctx, "ns2");
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  check_list(ns2, ctx);
  rados_objects_list_close(ctx);

  // Can't specify all namespaces using old interface
  rados_ioctx_set_namespace(ioctx, LIBRADOS_ALL_NSPACES);
  ASSERT_EQ(-EINVAL, rados_objects_list_open(ioctx, &ctx));
}

TEST_F(LibRadosListECPP, ListObjectsPPNS) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  // Create :foo1, :foo2, :foo3, n1:foo1, ns1:foo4, ns1:foo5, ns2:foo6, n2:foo7
  ioctx.set_namespace("");
  ASSERT_EQ(0, ioctx.write("foo1", bl1, sizeof(buf), 0));
  ioctx.set_namespace("ns1");
  ASSERT_EQ(0, ioctx.write("foo1", bl1, sizeof(buf), 0));
  ioctx.set_namespace("");
  ASSERT_EQ(0, ioctx.write("foo2", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo3", bl1, sizeof(buf), 0));
  ioctx.set_namespace("ns1");
  ASSERT_EQ(0, ioctx.write("foo4", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo5", bl1, sizeof(buf), 0));
  ioctx.set_namespace("ns2");
  ASSERT_EQ(0, ioctx.write("foo6", bl1, sizeof(buf), 0));
  ASSERT_EQ(0, ioctx.write("foo7", bl1, sizeof(buf), 0));

  std::set<std::string> def, ns1, ns2;
  def.insert(std::string("foo1"));
  def.insert(std::string("foo2"));
  def.insert(std::string("foo3"));
  ns1.insert(std::string("foo1"));
  ns1.insert(std::string("foo4"));
  ns1.insert(std::string("foo5"));
  ns2.insert(std::string("foo6"));
  ns2.insert(std::string("foo7"));

  ioctx.set_namespace("");
  check_listpp(def, ioctx);

  ioctx.set_namespace("ns1");
  check_listpp(ns1, ioctx);

  ioctx.set_namespace("ns2");
  check_listpp(ns2, ioctx);

  ioctx.set_namespace(all_nspaces);
  EXPECT_THROW(check_listpp(def, ioctx), std::runtime_error);
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

TEST_F(LibRadosListECPP, ListObjectsManyPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  for (int i=0; i<256; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, bl.length(), 0));
  }

  librados::ObjectIterator it = ioctx.objects_begin();
  std::set<std::string> saw_obj;
  std::set<int> saw_pg;
  for (; it != ioctx.objects_end(); ++it) {
    std::cout << it->first
	      << " " << it.get_pg_hash_position() << std::endl;
    saw_obj.insert(it->first);
    saw_pg.insert(it.get_pg_hash_position());
  }
  std::cout << "saw " << saw_pg.size() << " pgs " << std::endl;

  // make sure they are 0..n
  for (unsigned i = 0; i < saw_pg.size(); ++i)
    ASSERT_TRUE(saw_pg.count(i));
}

TEST_F(LibRadosListEC, ListObjectsStart) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));

  for (int i=0; i<16; ++i) {
    string n = stringify(i);
    ASSERT_EQ(0, rados_write(ioctx, n.c_str(), buf, sizeof(buf), 0));
  }

  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  std::map<int, std::set<std::string> > pg_to_obj;
  const char *entry;
  while (rados_objects_list_next(ctx, &entry, NULL) == 0) {
    uint32_t pos = rados_objects_list_get_pg_hash_position(ctx);
    std::cout << entry << " " << pos << std::endl;
    pg_to_obj[pos].insert(entry);
  }
  rados_objects_list_close(ctx);

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, rados_objects_list_seek(ctx, p->first));
    ASSERT_EQ(0, rados_objects_list_next(ctx, &entry, NULL));
    std::cout << "have " << entry << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(entry));
    ++p;
  }
  rados_objects_list_close(ctx);
}

TEST_F(LibRadosListECPP, ListObjectsStartPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  for (int i=0; i<16; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, bl.length(), 0));
  }

  librados::ObjectIterator it = ioctx.objects_begin();
  std::map<int, std::set<std::string> > pg_to_obj;
  for (; it != ioctx.objects_end(); ++it) {
    std::cout << it->first << " " << it.get_pg_hash_position() << std::endl;
    pg_to_obj[it.get_pg_hash_position()].insert(it->first);
  }

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  it = ioctx.objects_begin(p->first);
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, it.seek(p->first));
    std::cout << "have " << it->first << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(it->first));
    ++p;
  }
}


TEST_F(LibRadosList, EnumerateObjects) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));

  const uint32_t n_objects = 16;
  for (unsigned i=0; i<n_objects; ++i) {
    ASSERT_EQ(0, rados_write(ioctx, stringify(i).c_str(), buf, sizeof(buf), 0));
  }

  // Ensure a non-power-of-two PG count to avoid only
  // touching the easy path.
  std::string err_str = set_pg_num(&s_cluster, pool_name, 11);
  ASSERT_TRUE(err_str.empty());

  std::set<std::string> saw_obj;
  rados_object_list_cursor c = rados_object_list_begin(ioctx);
  rados_object_list_cursor end = rados_object_list_end(ioctx);
  while(!rados_object_list_is_end(ioctx, c))
  {
    rados_object_list_item results[12];
    memset(results, 0, sizeof(rados_object_list_item) * 12);
    int r = rados_object_list(ioctx,
            c, rados_object_list_end(ioctx),
            12, NULL, 0, results, &c);
    ASSERT_GE(r, 0);
    for (int i = 0; i < r; ++i) {
      std::string oid(results[i].oid, results[i].oid_length);
      if (saw_obj.count(oid)) {
          std::cerr << "duplicate obj " << oid << std::endl;
      }
      ASSERT_FALSE(saw_obj.count(oid));
      saw_obj.insert(oid);
    }
    rados_object_list_free(12, results);
  }
  rados_object_list_cursor_free(ioctx, c);
  rados_object_list_cursor_free(ioctx, end);

  for (unsigned i=0; i<n_objects; ++i) {
    if (!saw_obj.count(stringify(i))) {
        std::cerr << "missing object " << i << std::endl;
    }
    ASSERT_TRUE(saw_obj.count(stringify(i)));
  }
  ASSERT_EQ(n_objects, saw_obj.size());
}

TEST_F(LibRadosList, EnumerateObjectsSplit) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));

  const uint32_t n_objects = 16;
  for (unsigned i=0; i<n_objects; ++i) {
    ASSERT_EQ(0, rados_write(ioctx, stringify(i).c_str(), buf, sizeof(buf), 0));
  }

  // Ensure a non-power-of-two PG count to avoid only
  // touching the easy path.
  std::string err_str = set_pg_num(&s_cluster, pool_name, 11);
  ASSERT_TRUE(err_str.empty());

  rados_object_list_cursor begin = rados_object_list_begin(ioctx);
  rados_object_list_cursor end = rados_object_list_end(ioctx);

  // Step through an odd number of shards
  unsigned m = 5;
  std::set<std::string> saw_obj;
  for (unsigned n = 0; n < m; ++n) {
      rados_object_list_cursor shard_start = rados_object_list_begin(ioctx);;
      rados_object_list_cursor shard_end = rados_object_list_end(ioctx);;

      rados_object_list_slice(
        ioctx,
        begin,
        end,
        n,
        m,
        &shard_start,
        &shard_end);
      std::cout << "split " << n << "/" << m << " -> "
		<< *(hobject_t*)shard_start << " "
		<< *(hobject_t*)shard_end << std::endl;

      rados_object_list_cursor c = shard_start;
      //while(c < shard_end)
      while(rados_object_list_cursor_cmp(ioctx, c, shard_end) == -1)
      {
        rados_object_list_item results[12];
        memset(results, 0, sizeof(rados_object_list_item) * 12);
        int r = rados_object_list(ioctx,
                c, shard_end,
                12, NULL, 0, results, &c);
        ASSERT_GE(r, 0);
        for (int i = 0; i < r; ++i) {
          std::string oid(results[i].oid, results[i].oid_length);
          if (saw_obj.count(oid)) {
              std::cerr << "duplicate obj " << oid << std::endl;
          }
          ASSERT_FALSE(saw_obj.count(oid));
          saw_obj.insert(oid);
        }
        rados_object_list_free(12, results);
      }
      rados_object_list_cursor_free(ioctx, shard_start);
      rados_object_list_cursor_free(ioctx, shard_end);
  }

  rados_object_list_cursor_free(ioctx, begin);
  rados_object_list_cursor_free(ioctx, end);

  for (unsigned i=0; i<n_objects; ++i) {
    if (!saw_obj.count(stringify(i))) {
        std::cerr << "missing object " << i << std::endl;
    }
    ASSERT_TRUE(saw_obj.count(stringify(i)));
  }
  ASSERT_EQ(n_objects, saw_obj.size());
}

TEST_F(LibRadosListPP, EnumerateObjectsPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  const uint32_t n_objects = 16;
  for (unsigned i=0; i<n_objects; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, sizeof(buf), 0));
  }

  std::set<std::string> saw_obj;
  ObjectCursor c = ioctx.object_list_begin();
  ObjectCursor end = ioctx.object_list_end();
  while(!ioctx.object_list_is_end(c))
  {
    std::vector<ObjectItem> result;
    int r = ioctx.object_list(c, end, 12, {}, &result, &c);
    ASSERT_GE(r, 0);
    ASSERT_EQ(r, (int)result.size());
    for (int i = 0; i < r; ++i) {
      auto oid = result[i].oid;
      if (saw_obj.count(oid)) {
          std::cerr << "duplicate obj " << oid << std::endl;
      }
      ASSERT_FALSE(saw_obj.count(oid));
      saw_obj.insert(oid);
    }
  }

  for (unsigned i=0; i<n_objects; ++i) {
    if (!saw_obj.count(stringify(i))) {
        std::cerr << "missing object " << i << std::endl;
    }
    ASSERT_TRUE(saw_obj.count(stringify(i)));
  }
  ASSERT_EQ(n_objects, saw_obj.size());
}

TEST_F(LibRadosListPP, EnumerateObjectsSplitPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  const uint32_t n_objects = 16;
  for (unsigned i=0; i<n_objects; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, sizeof(buf), 0));
  }

  ObjectCursor begin = ioctx.object_list_begin();
  ObjectCursor end = ioctx.object_list_end();

  // Step through an odd number of shards
  unsigned m = 5;
  std::set<std::string> saw_obj;
  for (unsigned n = 0; n < m; ++n) {
      ObjectCursor shard_start;
      ObjectCursor shard_end;

      ioctx.object_list_slice(
        begin,
        end,
        n,
        m,
        &shard_start,
        &shard_end);

      ObjectCursor c(shard_start);
      while(c < shard_end)
      {
        std::vector<ObjectItem> result;
        int r = ioctx.object_list(c, shard_end, 12, {}, &result, &c);
        ASSERT_GE(r, 0);

        for (const auto & i : result) {
          const auto &oid = i.oid;
          if (saw_obj.count(oid)) {
              std::cerr << "duplicate obj " << oid << std::endl;
          }
          ASSERT_FALSE(saw_obj.count(oid));
          saw_obj.insert(oid);
        }
      }
  }

  for (unsigned i=0; i<n_objects; ++i) {
    if (!saw_obj.count(stringify(i))) {
        std::cerr << "missing object " << i << std::endl;
    }
    ASSERT_TRUE(saw_obj.count(stringify(i)));
  }
  ASSERT_EQ(n_objects, saw_obj.size());
}

TEST_F(LibRadosListNP, ListObjectsError) {
  std::string pool_name;
  rados_t cluster;
  rados_ioctx_t ioctx;
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &cluster));
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  ASSERT_EQ(0, rados_pool_delete(cluster, pool_name.c_str()));
  
  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &ctx));
  const char *entry;
  ASSERT_EQ(-ENOENT, rados_objects_list_next(ctx, &entry, NULL));
  rados_objects_list_close(ctx);
  rados_ioctx_destroy(ioctx);
  rados_shutdown(cluster);
}

TEST_F(LibRadosListPP, EnumerateObjectsFilterPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist obj_content;
  obj_content.append(buf, sizeof(buf));

  std::string target_str = "content";

  // Write xattr bare, no ::encod'ing
  bufferlist target_val;
  target_val.append(target_str);
  bufferlist nontarget_val;
  nontarget_val.append("rhubarb");

  ASSERT_EQ(0, ioctx.write("has_xattr", obj_content, obj_content.length(), 0));
  ASSERT_EQ(0, ioctx.write("has_wrong_xattr", obj_content, obj_content.length(), 0));
  ASSERT_EQ(0, ioctx.write("no_xattr", obj_content, obj_content.length(), 0));

  ASSERT_EQ(0, ioctx.setxattr("has_xattr", "theattr", target_val));
  ASSERT_EQ(0, ioctx.setxattr("has_wrong_xattr", "theattr", nontarget_val));

  bufferlist filter_bl;
  std::string filter_name = "plain";
  ::encode(filter_name, filter_bl);
  ::encode("_theattr", filter_bl);
  ::encode(target_str, filter_bl);

  ObjectCursor c = ioctx.object_list_begin();
  ObjectCursor end = ioctx.object_list_end();
  bool foundit = false;
  while(!ioctx.object_list_is_end(c))
  {
    std::vector<ObjectItem> result;
    int r = ioctx.object_list(c, end, 12, filter_bl, &result, &c);
    ASSERT_GE(r, 0);
    ASSERT_EQ(r, (int)result.size());
    for (int i = 0; i < r; ++i) {
      auto oid = result[i].oid;
      // We should only see the object that matches the filter
      ASSERT_EQ(oid, "has_xattr");
      // We should only see it once
      ASSERT_FALSE(foundit);
      foundit = true;
    }
  }
  ASSERT_TRUE(foundit);
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);
  cout << args << std::endl;

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  return RUN_ALL_TESTS();
}
