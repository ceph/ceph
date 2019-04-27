// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "test/librados/test.h"
#include "test/librados/test_common.h"
#include "test/librados/TestCase.h"
#include "global/global_context.h"

#include "include/types.h"
#include "common/hobject.h"
#include "gtest/gtest.h"
#include <errno.h>
#include <string>
#include <stdexcept>

using namespace librados;

typedef RadosTestNSCleanup LibRadosList;
typedef RadosTestECNSCleanup LibRadosListEC;
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


static void check_list(
  std::set<std::string>& myset,
  rados_list_ctx_t& ctx,
  const std::string &check_nspace)
{
  const char *entry, *nspace;
  cout << "myset " << myset << std::endl;
  // we should see every item exactly once.
  int ret;
  while ((ret = rados_nobjects_list_next(ctx, &entry, NULL, &nspace)) == 0) {
    std::string test_name;
    if (check_nspace == all_nspaces) {
      test_name = std::string(nspace) + ":" + std::string(entry);
    } else {
      ASSERT_TRUE(std::string(nspace) == check_nspace);
      test_name = std::string(entry);
    }
    cout << test_name << std::endl;

    ASSERT_TRUE(myset.end() != myset.find(test_name));
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

  char nspace[4];
  ASSERT_EQ(-ERANGE, rados_ioctx_get_namespace(ioctx, nspace, 3));
  ASSERT_EQ(static_cast<int>(strlen("ns2")),
	    rados_ioctx_get_namespace(ioctx, nspace, sizeof(nspace)));
  ASSERT_EQ(0, strcmp("ns2", nspace));

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
    uint32_t pos = rados_nobjects_list_get_pg_hash_position(ctx);
    std::cout << entry << " " << pos << std::endl;
    pg_to_obj[pos].insert(entry);
  }
  rados_nobjects_list_close(ctx);

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, rados_nobjects_list_seek(ctx, p->first));
    ASSERT_EQ(0, rados_nobjects_list_next(ctx, &entry, NULL, NULL));
    std::cout << "have " << entry << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(entry));
    ++p;
  }
  rados_nobjects_list_close(ctx);
}

// this function replicates
// librados::operator<<(std::ostream& os, const librados::ObjectCursor& oc)
// because we don't want to use librados in librados client.
std::ostream& operator<<(std::ostream&os, const rados_object_list_cursor& oc)
{
  if (oc) {
    os << *(hobject_t *)oc;
  } else {
    os << hobject_t{};
  }
  return os;
}

TEST_F(LibRadosList, ListObjectsCursor) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));

  const int max_objs = 16;

  for (int i=0; i<max_objs; ++i) {
    string n = stringify(i);
    ASSERT_EQ(0, rados_write(ioctx, n.c_str(), buf, sizeof(buf), 0));
  }

  {
    rados_list_ctx_t ctx;
    const char *entry;
    rados_object_list_cursor cursor;
    ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
    ASSERT_EQ(rados_nobjects_list_get_cursor(ctx, &cursor), 0);
    rados_object_list_cursor first_cursor = cursor;
    cout << "x cursor=" << cursor << std::endl;
    while (rados_nobjects_list_next(ctx, &entry, NULL, NULL) == 0) {
      string oid = entry;
      ASSERT_EQ(rados_nobjects_list_get_cursor(ctx, &cursor), 0);
      cout << "> oid=" << oid << " cursor=" << cursor << std::endl;
    }
    rados_nobjects_list_seek_cursor(ctx, first_cursor);
    ASSERT_EQ(rados_nobjects_list_next(ctx, &entry, NULL, NULL), 0);
    cout << "FIRST> seek to " << first_cursor << " oid=" << string(entry) << std::endl;
  }
  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));

  std::map<rados_object_list_cursor, string> cursor_to_obj;
  int count = 0;

  const char *entry;
  while (rados_nobjects_list_next(ctx, &entry, NULL, NULL) == 0) {
    rados_object_list_cursor cursor;
    ASSERT_EQ(rados_nobjects_list_get_cursor(ctx, &cursor), 0);
    string oid = entry;
    cout << ": oid=" << oid << " cursor=" << cursor << std::endl;
    cursor_to_obj[cursor] = oid;

    rados_nobjects_list_seek_cursor(ctx, cursor);
    cout << ": seek to " << cursor << std::endl;
    ASSERT_EQ(rados_nobjects_list_next(ctx, &entry, NULL, NULL), 0);
    cout << "> " << cursor << " -> " << entry << std::endl;
    ASSERT_EQ(string(entry), oid);
    ASSERT_LT(count, max_objs); /* avoid infinite loops due to bad seek */

    ++count;
  }

  ASSERT_EQ(count, max_objs);

  auto p = cursor_to_obj.rbegin();
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  while (p != cursor_to_obj.rend()) {
    cout << ": seek to " << p->first << std::endl;
    rados_object_list_cursor cursor;
    rados_object_list_cursor oid(p->first);
    rados_nobjects_list_seek_cursor(ctx, oid);
    ASSERT_EQ(rados_nobjects_list_get_cursor(ctx, &cursor), 0);
    cout << ": cursor()=" << cursor << " expected=" << oid << std::endl;
    // ASSERT_EQ(ObjectCursor(oid), ObjectCursor(cursor));
    ASSERT_EQ(rados_nobjects_list_next(ctx, &entry, NULL, NULL), 0);
    cout << "> " << cursor << " -> " << entry << std::endl;
    cout << ": entry=" << entry << " expected=" << p->second << std::endl;
    ASSERT_EQ(p->second, string(entry));

    ++p;

    rados_object_list_cursor_free(ctx, cursor);
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
    uint32_t pos = rados_nobjects_list_get_pg_hash_position(ctx);
    std::cout << entry << " " << pos << std::endl;
    pg_to_obj[pos].insert(entry);
  }
  rados_nobjects_list_close(ctx);

  std::map<int, std::set<std::string> >::reverse_iterator p =
    pg_to_obj.rbegin();
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  while (p != pg_to_obj.rend()) {
    ASSERT_EQ((uint32_t)p->first, rados_nobjects_list_seek(ctx, p->first));
    ASSERT_EQ(0, rados_nobjects_list_next(ctx, &entry, NULL, NULL));
    std::cout << "have " << entry << " expect one of " << p->second << std::endl;
    ASSERT_TRUE(p->second.count(entry));
    ++p;
  }
  rados_nobjects_list_close(ctx);
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

  //ASSERT_EQ(0, rados_pool_delete(cluster, pool_name.c_str()));
  {
    char *buf, *st;
    size_t buflen, stlen;
    string c = "{\"prefix\":\"osd pool rm\",\"pool\": \"" + pool_name +
      "\",\"pool2\":\"" + pool_name +
      "\",\"yes_i_really_really_mean_it_not_faking\": true}";
    const char *cmd[2] = { c.c_str(), 0 };
    ASSERT_EQ(0, rados_mon_command(cluster, (const char **)cmd, 1, "", 0, &buf, &buflen, &st, &stlen));
    ASSERT_EQ(0, rados_wait_for_latest_osdmap(cluster));
  }

  rados_list_ctx_t ctx;
  ASSERT_EQ(0, rados_nobjects_list_open(ioctx, &ctx));
  const char *entry;
  ASSERT_EQ(-ENOENT, rados_nobjects_list_next(ctx, &entry, NULL, NULL));
  rados_nobjects_list_close(ctx);
  rados_ioctx_destroy(ioctx);
  rados_shutdown(cluster);
}



// ---------------------------------------------

TEST_F(LibRadosList, EnumerateObjects) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));

  const uint32_t n_objects = 16;
  for (unsigned i=0; i<n_objects; ++i) {
    ASSERT_EQ(0, rados_write(ioctx, stringify(i).c_str(), buf, sizeof(buf), 0));
  }

  // Ensure a non-power-of-two PG count to avoid only
  // touching the easy path.
  ASSERT_TRUE(set_pg_num(&s_cluster, pool_name, 11).empty());
  ASSERT_TRUE(set_pgp_num(&s_cluster, pool_name, 11).empty());

  std::set<std::string> saw_obj;
  rados_object_list_cursor c = rados_object_list_begin(ioctx);
  rados_object_list_cursor end = rados_object_list_end(ioctx);
  while(!rados_object_list_is_end(ioctx, c))
  {
    rados_object_list_item results[12];
    memset(results, 0, sizeof(rados_object_list_item) * 12);
    rados_object_list_cursor temp_end = rados_object_list_end(ioctx);
    int r = rados_object_list(ioctx, c, temp_end,
            12, NULL, 0, results, &c);
    rados_object_list_cursor_free(ioctx, temp_end);
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
  ASSERT_TRUE(set_pg_num(&s_cluster, pool_name, 11).empty());
  ASSERT_TRUE(set_pgp_num(&s_cluster, pool_name, 11).empty());

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
