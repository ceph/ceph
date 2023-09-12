// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "os/ObjectStore.h"
#include "test/crimson/gtest_seastar.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/collection_manager.h"

#include "test/crimson/seastore/test_block.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}


#define TEST_COLL_FORWARD(METHOD)					\
  template <typename... Args>				\
  auto METHOD(coll_root_t &root, Transaction &t, Args&&... args) const { \
    return with_trans_intr(						\
    t,									\
    [this](auto &t, auto &root, auto&&... args) {			\
      return collection_manager->METHOD(				\
        root,								\
        t,								\
        std::forward<decltype(args)>(args)...);				\
      },								\
      root,								\
      std::forward<Args>(args)...).unsafe_get0();			\
  }

struct collection_manager_test_t :
  public seastar_test_suite_t,
  TMTestState {

  CollectionManagerRef collection_manager;

  collection_manager_test_t() {}

  seastar::future<> set_up_fut() final {
    return tm_setup().then([this] {
      collection_manager = collection_manager::create_coll_manager(*tm);
      return seastar::now();
    });
  }

  seastar::future<> tear_down_fut() final {
    return tm_teardown().then([this] {
      collection_manager.reset();
      return seastar::now();
    });
  }

  using test_collection_t = std::map<coll_t, coll_info_t>;
  test_collection_t test_coll_mappings;

  void replay() {
    restart();
    collection_manager = collection_manager::create_coll_manager(*tm);
  }

  auto get_root() {
    auto tref = create_mutate_transaction();
    auto coll_root = with_trans_intr(
      *tref,
      [this](auto &t) {
	return collection_manager->mkfs(t);
      }).unsafe_get0();
    submit_transaction(std::move(tref));
    return coll_root;
  }

  TEST_COLL_FORWARD(remove)
  TEST_COLL_FORWARD(list)
  TEST_COLL_FORWARD(create)
  TEST_COLL_FORWARD(update)

  void checking_mappings(coll_root_t &coll_root, Transaction &t) {
    auto coll_list = list(coll_root, t);
    EXPECT_EQ(test_coll_mappings.size(), coll_list.size());
    for (std::pair<coll_t, coll_info_t> p : test_coll_mappings) {
      EXPECT_NE(
        std::find(coll_list.begin(), coll_list.end(), p),
        coll_list.end());
    }
  }

  void checking_mappings(coll_root_t &coll_root) {
    auto t = create_read_transaction();
    checking_mappings(coll_root, *t);
  }
};

TEST_P(collection_manager_test_t, basic)
{
  run_async([this] {
    coll_root_t coll_root = get_root();
    {
      auto t = create_mutate_transaction();
      for (int i = 0; i < 20; i++) {
        coll_t cid(spg_t(pg_t(i+1,i+2), shard_id_t::NO_SHARD));
        create(coll_root, *t, cid, coll_info_t(i));
        test_coll_mappings.emplace(cid, coll_info_t(i));
      }
      checking_mappings(coll_root, *t);
      submit_transaction(std::move(t));
      EXPECT_EQ(test_coll_mappings.size(), 20);
    }

    replay();
    checking_mappings(coll_root);
    {
      auto t = create_mutate_transaction();
      for (auto iter = test_coll_mappings.begin();
           iter != test_coll_mappings.end();) {
        remove(coll_root, *t, iter->first);
        iter = test_coll_mappings.erase(iter);
      }
      submit_transaction(std::move(t));
    }
    replay();
    {
      auto t = create_mutate_transaction();
      auto list_ret = list(coll_root, *t);
      submit_transaction(std::move(t));
      EXPECT_EQ(list_ret.size(), test_coll_mappings.size());
    }
  });
}

TEST_P(collection_manager_test_t, overflow)
{
  run_async([this] {
    coll_root_t coll_root = get_root();
    auto old_location = coll_root.get_location();

    auto t = create_mutate_transaction();
    for (int i = 0; i < 412; i++) {
      coll_t cid(spg_t(pg_t(i+1,i+2), shard_id_t::NO_SHARD));
      create(coll_root, *t, cid, coll_info_t(i));
      test_coll_mappings.emplace(cid, coll_info_t(i));
    }
    submit_transaction(std::move(t));
    EXPECT_NE(old_location, coll_root.get_location());
    checking_mappings(coll_root);

    replay();
    checking_mappings(coll_root);
  });
}

TEST_P(collection_manager_test_t, update)
{
  run_async([this] {
    coll_root_t coll_root = get_root();
    {
      auto t = create_mutate_transaction();
      for (int i = 0; i < 2; i++) {
        coll_t cid(spg_t(pg_t(1,i+1), shard_id_t::NO_SHARD));
	create(coll_root, *t, cid, coll_info_t(i));
        test_coll_mappings.emplace(cid, coll_info_t(i));
      }
      submit_transaction(std::move(t));
    }
    {
       auto iter1= test_coll_mappings.begin();
       auto iter2 = std::next(test_coll_mappings.begin(), 1);
       EXPECT_NE(iter1->second.split_bits, iter2->second.split_bits);
       auto t = create_mutate_transaction();
       update(coll_root, *t, iter1->first, iter2->second);
       submit_transaction(std::move(t));
       iter1->second.split_bits = iter2->second.split_bits;
    }
    replay();
    checking_mappings(coll_root);
  });
}

INSTANTIATE_TEST_SUITE_P(
  collection_manager_test,
  collection_manager_test_t,
  ::testing::Values (
    "segmented",
    "circularbounded"
  )
);
