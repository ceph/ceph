// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

#include "include/types.h"
#include "gtest/gtest.h"
#include <errno.h>
#include <string>

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
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  const char *entry;
  bool foundit = false;
  while (rados_nobjects_list_next(ctx, &entry, NULL, NULL) != -ENOENT) {
    foundit = true;
    ASSERT_EQ(std::string(entry), "foo");
  }
  ASSERT_TRUE(foundit);
  rados_nobjects_list_close(ctx);
}

TEST_F(LibRadosListPP, ListObjectsPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  NObjectIterator iter(ioctx.nobjects_begin());
  bool foundit = false;
  while (iter != ioctx.nobjects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).get_oid(), "foo");
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
  NObjectIterator iter(ioctx.nobjects_begin());
  bool foundit = false;
  while (iter != ioctx.nobjects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).get_oid(), "foo");
    ++iter;
  }
  ASSERT_TRUE(foundit);
  ++iter;
  ASSERT_TRUE(iter == ioctx.nobjects_end());
  foundit = false;
  iter.seek(0);
  while (iter != ioctx.nobjects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).get_oid(), "foo");
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
  NObjectIterator iter3;
  {
    NObjectIterator iter(ioctx.nobjects_begin());
    NObjectIterator iter2(iter);
    iter3 = iter2;
    ASSERT_EQ((*iter).get_oid(), "foo");
    ++iter;
    ASSERT_TRUE(iter == ioctx.nobjects_end());
    ++iter;
    ASSERT_TRUE(iter == ioctx.nobjects_end());

    ASSERT_EQ(iter2->get_oid(), "foo");
    ASSERT_EQ(iter3->get_oid(), "foo");
    ++iter2;
    ASSERT_TRUE(iter2 == ioctx.nobjects_end());
  }

  ASSERT_EQ(iter3->get_oid(), "foo");
  iter3 = iter3;
  ASSERT_EQ(iter3->get_oid(), "foo");
  ++iter3;
  ASSERT_TRUE(iter3 == ioctx.nobjects_end());
}

TEST_F(LibRadosListPP, ListObjectsEndIter) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  NObjectIterator iter(ioctx.nobjects_begin());
  NObjectIterator iter_end(ioctx.nobjects_end());
  NObjectIterator iter_end2 = ioctx.nobjects_end();
  ASSERT_TRUE(iter_end == iter_end2);
  ASSERT_TRUE(iter_end == ioctx.nobjects_end());
  ASSERT_TRUE(iter_end2 == ioctx.nobjects_end());

  ASSERT_EQ(iter->get_oid(), "foo");
  ++iter;
  ASSERT_TRUE(iter == ioctx.nobjects_end());
  ASSERT_TRUE(iter == iter_end);
  ASSERT_TRUE(iter == iter_end2);
  NObjectIterator iter2 = iter;
  ASSERT_TRUE(iter2 == ioctx.nobjects_end());
  ASSERT_TRUE(iter2 == iter_end);
  ASSERT_TRUE(iter2 == iter_end2);
}

static void check_list(std::set<std::string>& myset, rados_list_ctx_t& ctx, std::string check_nspace)
{
  const char *entry, *nspace;
  std::set<std::string> orig_set(myset);
  /**
   * During splitting, we might see duplicate items.
   * We assert that every object returned is in myset and that
   * we don't hit ENOENT until we have hit every item in myset
   * at least once.
   */
  int ret;
  while ((ret = rados_nobjects_list_next(ctx, &entry, NULL, &nspace)) == 0) {
    std::string test_name;
    if (check_nspace == all_nspaces) {
      test_name = std::string(nspace) + ":" + std::string(entry);
    } else {
      ASSERT_TRUE(std::string(nspace) == check_nspace);
      test_name = std::string(entry);
    }

    ASSERT_TRUE(orig_set.end() != orig_set.find(test_name));
    myset.erase(test_name);
  }
  ASSERT_EQ(-ENOENT, ret);
  ASSERT_TRUE(myset.empty());
}

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

  std::set<std::string> def, ns1, ns2, all;
  def.insert(std::string("foo1"));
  def.insert(std::string("foo2"));
  def.insert(std::string("foo3"));
  ns1.insert(std::string("foo1"));
  ns1.insert(std::string("foo4"));
  ns1.insert(std::string("foo5"));
  ns2.insert(std::string("foo6"));
  ns2.insert(std::string("foo7"));
  all.insert(std::string(":foo1"));
  all.insert(std::string(":foo2"));
  all.insert(std::string(":foo3"));
  all.insert(std::string("ns1:foo1"));
  all.insert(std::string("ns1:foo4"));
  all.insert(std::string("ns1:foo5"));
  all.insert(std::string("ns2:foo6"));
  all.insert(std::string("ns2:foo7"));

  rados_list_ctx_t ctx;
  // Check default namespace ""
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  check_list(def, ctx, "");
  rados_nobjects_list_close(ctx);

  // Check namespace "ns1"
  rados_ioctx_set_namespace(ioctx, "ns1");
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  check_list(ns1, ctx, "ns1");
  rados_nobjects_list_close(ctx);

  // Check namespace "ns2"
  rados_ioctx_set_namespace(ioctx, "ns2");
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  check_list(ns2, ctx, "ns2");
  rados_nobjects_list_close(ctx);

  // Check ALL namespaces
  rados_ioctx_set_namespace(ioctx, LIBRADOS_ALL_NSPACES);
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  check_list(all, ctx, all_nspaces);
  rados_nobjects_list_close(ctx);
}

static void check_listpp(std::set<std::string>& myset, IoCtx& ioctx, std::string check_nspace)
{
  NObjectIterator iter(ioctx.nobjects_begin());
  std::set<std::string> orig_set(myset);
  /**
   * During splitting, we might see duplicate items.
   * We assert that every object returned is in myset and that
   * we don't hit ENOENT until we have hit every item in myset
   * at least once.
   */
  while (iter != ioctx.nobjects_end()) {
    std::string test_name;
    if (check_nspace == all_nspaces) {
      test_name = iter->get_nspace() + ":" + iter->get_oid();
    } else {
      ASSERT_TRUE(iter->get_nspace() == check_nspace);
      test_name = iter->get_oid();
    }
    ASSERT_TRUE(orig_set.end() != orig_set.find(test_name));
    myset.erase(test_name);
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

  std::set<std::string> def, ns1, ns2, all;
  def.insert(std::string("foo1"));
  def.insert(std::string("foo2"));
  def.insert(std::string("foo3"));
  ns1.insert(std::string("foo1"));
  ns1.insert(std::string("foo4"));
  ns1.insert(std::string("foo5"));
  ns2.insert(std::string("foo6"));
  ns2.insert(std::string("foo7"));
  all.insert(std::string(":foo1"));
  all.insert(std::string(":foo2"));
  all.insert(std::string(":foo3"));
  all.insert(std::string("ns1:foo1"));
  all.insert(std::string("ns1:foo4"));
  all.insert(std::string("ns1:foo5"));
  all.insert(std::string("ns2:foo6"));
  all.insert(std::string("ns2:foo7"));

  ioctx.set_namespace("");
  check_listpp(def, ioctx, "");

  ioctx.set_namespace("ns1");
  check_listpp(ns1, ioctx, "ns1");

  ioctx.set_namespace("ns2");
  check_listpp(ns2, ioctx, "ns2");

  ioctx.set_namespace(all_nspaces);
  check_listpp(all, ioctx, all_nspaces);
}

TEST_F(LibRadosListPP, ListObjectsManyPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  for (int i=0; i<256; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, bl.length(), 0));
  }

  librados::NObjectIterator it = ioctx.nobjects_begin();
  std::set<std::string> saw_obj;
  std::set<int> saw_pg;
  for (; it != ioctx.nobjects_end(); ++it) {
    std::cout << it->get_oid()
	      << " " << it.get_pg_hash_position() << std::endl;
    saw_obj.insert(it->get_oid());
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
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  std::map<int, std::set<std::string> > pg_to_obj;
  const char *entry;
  while (rados_nobjects_list_next(ctx, &entry, NULL, NULL) == 0) {
    uint32_t pos = rados_objects_list_get_pg_hash_position(ctx);
    std::cout << entry << " " << pos << std::endl;
    pg_to_obj[pos].insert(entry);
  }
  rados_nobjects_list_close(ctx);

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, rados_objects_list_seek(ctx, p->first));
    ASSERT_EQ(0, rados_nobjects_list_next(ctx, &entry, NULL, NULL));
    std::cout << "have " << entry << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(entry));
    ++p;
  }
  rados_nobjects_list_close(ctx);
}

TEST_F(LibRadosListPP, ListObjectsStartPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  for (int i=0; i<16; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, bl.length(), 0));
  }

  librados::NObjectIterator it = ioctx.nobjects_begin();
  std::map<int, std::set<std::string> > pg_to_obj;
  for (; it != ioctx.nobjects_end(); ++it) {
    std::cout << it->get_oid() << " " << it.get_pg_hash_position() << std::endl;
    pg_to_obj[it.get_pg_hash_position()].insert(it->get_oid());
  }

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  it = ioctx.nobjects_begin(p->first);
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, it.seek(p->first));
    std::cout << "have " << it->get_oid() << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(it->get_oid()));
    ++p;
  }
}

TEST_F(LibRadosListEC, ListObjects) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  ASSERT_EQ(0, rados_write(ioctx, "foo", buf, sizeof(buf), 0));
  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  const char *entry;
  bool foundit = false;
  while (rados_nobjects_list_next(ctx, &entry, NULL, NULL) != -ENOENT) {
    foundit = true;
    ASSERT_EQ(std::string(entry), "foo");
  }
  ASSERT_TRUE(foundit);
  rados_nobjects_list_close(ctx);
}

TEST_F(LibRadosListECPP, ListObjectsPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  NObjectIterator iter(ioctx.nobjects_begin());
  bool foundit = false;
  while (iter != ioctx.nobjects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).get_oid(), "foo");
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
  NObjectIterator iter(ioctx.nobjects_begin());
  bool foundit = false;
  while (iter != ioctx.nobjects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).get_oid(), "foo");
    ++iter;
  }
  ASSERT_TRUE(foundit);
  ++iter;
  ASSERT_TRUE(iter == ioctx.nobjects_end());
  foundit = false;
  iter.seek(0);
  while (iter != ioctx.nobjects_end()) {
    foundit = true;
    ASSERT_EQ((*iter).get_oid(), "foo");
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
  NObjectIterator iter3;
  {
    NObjectIterator iter(ioctx.nobjects_begin());
    NObjectIterator iter2(iter);
    iter3 = iter2;
    ASSERT_EQ((*iter).get_oid(), "foo");
    ++iter;
    ASSERT_TRUE(iter == ioctx.nobjects_end());
    ++iter;
    ASSERT_TRUE(iter == ioctx.nobjects_end());

    ASSERT_EQ(iter2->get_oid(), "foo");
    ASSERT_EQ(iter3->get_oid(), "foo");
    ++iter2;
    ASSERT_TRUE(iter2 == ioctx.nobjects_end());
  }

  ASSERT_EQ(iter3->get_oid(), "foo");
  iter3 = iter3;
  ASSERT_EQ(iter3->get_oid(), "foo");
  ++iter3;
  ASSERT_TRUE(iter3 == ioctx.nobjects_end());
}

TEST_F(LibRadosListECPP, ListObjectsEndIter) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));

  NObjectIterator iter(ioctx.nobjects_begin());
  NObjectIterator iter_end(ioctx.nobjects_end());
  NObjectIterator iter_end2 = ioctx.nobjects_end();
  ASSERT_TRUE(iter_end == iter_end2);
  ASSERT_TRUE(iter_end == ioctx.nobjects_end());
  ASSERT_TRUE(iter_end2 == ioctx.nobjects_end());

  ASSERT_EQ(iter->get_oid(), "foo");
  ++iter;
  ASSERT_TRUE(iter == ioctx.nobjects_end());
  ASSERT_TRUE(iter == iter_end);
  ASSERT_TRUE(iter == iter_end2);
  NObjectIterator iter2 = iter;
  ASSERT_TRUE(iter2 == ioctx.nobjects_end());
  ASSERT_TRUE(iter2 == iter_end);
  ASSERT_TRUE(iter2 == iter_end2);
}

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

  std::set<std::string> def, ns1, ns2, all;
  def.insert(std::string("foo1"));
  def.insert(std::string("foo2"));
  def.insert(std::string("foo3"));
  ns1.insert(std::string("foo1"));
  ns1.insert(std::string("foo4"));
  ns1.insert(std::string("foo5"));
  ns2.insert(std::string("foo6"));
  ns2.insert(std::string("foo7"));
  all.insert(std::string(":foo1"));
  all.insert(std::string(":foo2"));
  all.insert(std::string(":foo3"));
  all.insert(std::string("ns1:foo1"));
  all.insert(std::string("ns1:foo4"));
  all.insert(std::string("ns1:foo5"));
  all.insert(std::string("ns2:foo6"));
  all.insert(std::string("ns2:foo7"));

  rados_list_ctx_t ctx;
  // Check default namespace ""
  rados_ioctx_set_namespace(ioctx, "");
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  check_list(def, ctx, "");
  rados_nobjects_list_close(ctx);

  // Check default namespace "ns1"
  rados_ioctx_set_namespace(ioctx, "ns1");
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  check_list(ns1, ctx, "ns1");
  rados_nobjects_list_close(ctx);

  // Check default namespace "ns2"
  rados_ioctx_set_namespace(ioctx, "ns2");
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  check_list(ns2, ctx, "ns2");
  rados_nobjects_list_close(ctx);

  // Check all namespaces
  rados_ioctx_set_namespace(ioctx, LIBRADOS_ALL_NSPACES);
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  check_list(all, ctx, all_nspaces);
  rados_nobjects_list_close(ctx);
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
  check_listpp(def, ioctx, "");

  ioctx.set_namespace("ns1");
  check_listpp(ns1, ioctx, "ns1");

  ioctx.set_namespace("ns2");
  check_listpp(ns2, ioctx, "ns2");
}

TEST_F(LibRadosListECPP, ListObjectsManyPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  for (int i=0; i<256; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, bl.length(), 0));
  }

  librados::NObjectIterator it = ioctx.nobjects_begin();
  std::set<std::string> saw_obj;
  std::set<int> saw_pg;
  for (; it != ioctx.nobjects_end(); ++it) {
    std::cout << it->get_oid()
	      << " " << it.get_pg_hash_position() << std::endl;
    saw_obj.insert(it->get_oid());
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
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  std::map<int, std::set<std::string> > pg_to_obj;
  const char *entry;
  while (rados_nobjects_list_next(ctx, &entry, NULL, NULL) == 0) {
    uint32_t pos = rados_objects_list_get_pg_hash_position(ctx);
    std::cout << entry << " " << pos << std::endl;
    pg_to_obj[pos].insert(entry);
  }
  rados_nobjects_list_close(ctx);

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, rados_objects_list_seek(ctx, p->first));
    ASSERT_EQ(0, rados_nobjects_list_next(ctx, &entry, NULL, NULL));
    std::cout << "have " << entry << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(entry));
    ++p;
  }
  rados_nobjects_list_close(ctx);
}

TEST_F(LibRadosListECPP, ListObjectsStartPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  for (int i=0; i<16; ++i) {
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, bl.length(), 0));
  }

  librados::NObjectIterator it = ioctx.nobjects_begin();
  std::map<int, std::set<std::string> > pg_to_obj;
  for (; it != ioctx.nobjects_end(); ++it) {
    std::cout << it->get_oid() << " " << it.get_pg_hash_position() << std::endl;
    pg_to_obj[it.get_pg_hash_position()].insert(it->get_oid());
  }

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  it = ioctx.nobjects_begin(p->first);
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, it.seek(p->first));
    std::cout << "have " << it->get_oid() << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(it->get_oid()));
    ++p;
  }
}

TEST_F(LibRadosListPP, ListObjectsFilterPP) {
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

  NObjectIterator iter(ioctx.nobjects_begin(filter_bl));
  bool foundit = false;
  int k = 0;
  while (iter != ioctx.nobjects_end()) {
    foundit = true;
    // We should only see the object that matches the filter
    ASSERT_EQ((*iter).get_oid(), "has_xattr");
    // We should only see it once
    ASSERT_EQ(k, 0);
    ++iter;
    ++k;
  }
  ASSERT_TRUE(foundit);
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
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  const char *entry;
  ASSERT_EQ(-ENOENT, rados_nobjects_list_next(ctx, &entry, NULL, NULL));
  rados_nobjects_list_close(ctx);
  rados_ioctx_destroy(ioctx);
  rados_shutdown(cluster);
}

