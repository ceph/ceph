// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <iostream>
#include <sstream>

#include "test/crimson/gtest_seastar.h"

#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/seastore/seastore.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using CTransaction = ceph::os::Transaction;
using namespace std;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}


struct seastore_test_t :
  public seastar_test_suite_t,
  SeaStoreTestState {

  coll_t coll_name{spg_t{pg_t{0, 0}}};
  CollectionRef coll;

  seastore_test_t() {}

  seastar::future<> set_up_fut() final {
    return tm_setup(
    ).then([this] {
      return seastore->create_new_collection(coll_name);
    }).then([this](auto coll_ref) {
      coll = coll_ref;
    });
  }

  seastar::future<> tear_down_fut() final {
    return tm_teardown();
  }

  void do_transaction(CTransaction &&t) {
    return seastore->do_transaction(
      coll,
      std::move(t)).get0();
  }

  struct object_state_t {
    const coll_t cid;
    const CollectionRef coll;
    const ghobject_t oid;

    std::map<string, bufferlist> omap;

    void set_omap(
      CTransaction &t,
      const string &key,
      const bufferlist &val) {
      omap[key] = val;
      std::map<string, bufferlist> arg;
      arg[key] = val;
      t.omap_setkeys(
	cid,
	oid,
	arg);
    }

    void set_omap(
      SeaStore &seastore,
      const string &key,
      const bufferlist &val) {
      CTransaction t;
      set_omap(t, key, val);
      seastore.do_transaction(
	coll,
	std::move(t)).get0();
    }

    void check_omap_key(
      SeaStore &seastore,
      const string &key) {
      std::set<string> to_check;
      to_check.insert(key);
      auto result = seastore.omap_get_values(
	coll,
	oid,
	to_check).unsafe_get0();
      if (result.empty()) {
	EXPECT_EQ(omap.find(key), omap.end());
      } else {
	auto iter = omap.find(key);
	EXPECT_NE(iter, omap.end());
	if (iter != omap.end()) {
	  EXPECT_EQ(result.size(), 1);
	  EXPECT_EQ(iter->second, result.begin()->second);
	}
      }
    }

    void check_omap(SeaStore &seastore) {
      auto iter = seastore.get_omap_iterator(coll, oid).get0();
      iter->seek_to_first().get0();
      auto refiter = omap.begin();
      while (true) {
	if (!iter->valid() && refiter == omap.end())
	  break;

	if (!iter->valid() || refiter->first < iter->key()) {
	  logger().debug(
	    "check_omap: missing omap key {}",
	    refiter->first);
	  GTEST_FAIL() << "missing omap key " << refiter->first;
	  ++refiter;
	} else if (refiter == omap.end() || refiter->first > iter->key()) {
	  logger().debug(
	    "check_omap: extra omap key {}",
	    iter->key());
	  GTEST_FAIL() << "extra omap key" << iter->key();
	  iter->next().get0();
	} else {
	  EXPECT_EQ(iter->value(), refiter->second);
	  iter->next().get0();
	  ++refiter;
	}
      }
    }
  };

  map<ghobject_t, object_state_t> test_objects;
  object_state_t &get_object(
    const ghobject_t &oid) {
    return test_objects.emplace(
      std::make_pair(
	oid,
	object_state_t{coll_name, coll, oid})).first->second;
  }
};

ghobject_t make_oid(int i) {
  stringstream ss;
  ss << "object_" << i;
  auto ret = ghobject_t(
    hobject_t(
      sobject_t(ss.str(), CEPH_NOSNAP)));
  ret.hobj.nspace = "asdf";
  return ret;
}

template <typename T, typename V>
auto contains(const T &t, const V &v) {
  return std::find(
    t.begin(),
    t.end(),
    v) != t.end();
}

TEST_F(seastore_test_t, collection_create_list_remove)
{
  run_async([this] {
    coll_t test_coll{spg_t{pg_t{1, 0}}};
    {
      seastore->create_new_collection(test_coll).get0();
      auto collections = seastore->list_collections().get0();
      EXPECT_EQ(collections.size(), 2);
      EXPECT_TRUE(contains(collections, coll_name));
      EXPECT_TRUE(contains(collections,  test_coll));
    }

    {
      CTransaction t;
      t.remove_collection(test_coll);
      do_transaction(std::move(t));
      auto collections = seastore->list_collections().get0();
      EXPECT_EQ(collections.size(), 1);
      EXPECT_TRUE(contains(collections, coll_name));
    }
  });
}

TEST_F(seastore_test_t, touch_stat)
{
  run_async([this] {
    auto test = make_oid(0);
    {
      CTransaction t;
      t.touch(coll_name, test);
      do_transaction(std::move(t));
    }

    auto result = seastore->stat(
      coll,
      test).get0();
    EXPECT_EQ(result.st_size, 0);
  });
}

bufferlist make_bufferlist(size_t len) {
  bufferptr ptr(len);
  bufferlist bl;
  bl.append(ptr);
  return bl;
}

TEST_F(seastore_test_t, omap_test_simple)
{
  run_async([this] {
    auto &test_obj = get_object(make_oid(0));
    test_obj.set_omap(
      *seastore,
      "asdf",
      make_bufferlist(128));
    test_obj.check_omap_key(
      *seastore,
      "asdf");
  });
}

TEST_F(seastore_test_t, omap_test_iterator)
{
  run_async([this] {
    auto make_key = [](unsigned i) {
      std::stringstream ss;
      ss << "key" << i;
      return ss.str();
    };
    auto &test_obj = get_object(make_oid(0));
    for (unsigned i = 0; i < 20; ++i) {
      test_obj.set_omap(
	*seastore,
	make_key(i),
	make_bufferlist(128));
    }
    test_obj.check_omap(*seastore);
  });
}
