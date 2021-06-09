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
    logger().debug("{}: begin", __func__);
    restart();
    collection_manager = collection_manager::create_coll_manager(*tm);
    logger().debug("{}: end", __func__);
  }

  void checking_mappings(coll_root_t &coll_root, Transaction &t) {
    auto coll_list = collection_manager->list(coll_root, t).unsafe_get0();
    EXPECT_EQ(test_coll_mappings.size(), coll_list.size());
    for (std::pair<coll_t, coll_info_t> p : test_coll_mappings) {
      EXPECT_NE(
        std::find(coll_list.begin(), coll_list.end(), p),
        coll_list.end());
    }
  }

  void checking_mappings(coll_root_t &coll_root) {
    auto t = tm->create_transaction();
    checking_mappings(coll_root, *t);
  }

  void submit_transaction(TransactionRef &&t) {
    tm->submit_transaction(std::move(t)).unsafe_get0();
    segment_cleaner->run_until_halt().get0();
  }
};

TEST_F(collection_manager_test_t, basic)
{
  run_async([this] {
    coll_root_t coll_root;
    {
      auto t = tm->create_transaction();
      coll_root = collection_manager->mkfs(*t).unsafe_get0();
      submit_transaction(std::move(t));
    }
    {
      auto t = tm->create_transaction();
      for (int i = 0; i < 20; i++) {
        coll_t cid(spg_t(pg_t(i+1,i+2), shard_id_t::NO_SHARD));
        collection_manager->create(coll_root, *t, cid, coll_info_t(i)).unsafe_get0();
        test_coll_mappings.emplace(cid, coll_info_t(i));
      }
      checking_mappings(coll_root, *t);
      submit_transaction(std::move(t));
      EXPECT_EQ(test_coll_mappings.size(), 20);
    }

    replay();
    checking_mappings(coll_root);
    {
      auto t = tm->create_transaction();
      for (auto& ite : test_coll_mappings) {
        collection_manager->remove(coll_root, *t, ite.first).unsafe_get0();
        test_coll_mappings.erase(ite.first);
      }
      submit_transaction(std::move(t));
    }
    replay();
    {
      auto t = tm->create_transaction();
      auto list_ret = collection_manager->list(coll_root, *t).unsafe_get0();
      submit_transaction(std::move(t));
      EXPECT_EQ(list_ret.size(), test_coll_mappings.size());
    }
  });
}

TEST_F(collection_manager_test_t, overflow)
{
  run_async([this] {
    coll_root_t coll_root;
    {
      auto t = tm->create_transaction();
      coll_root = collection_manager->mkfs(*t).unsafe_get0();
      submit_transaction(std::move(t));
    }
    auto old_location = coll_root.get_location();

    auto t = tm->create_transaction();
    for (int i = 0; i < 412; i++) {
      coll_t cid(spg_t(pg_t(i+1,i+2), shard_id_t::NO_SHARD));
      collection_manager->create(coll_root, *t, cid, coll_info_t(i)).unsafe_get0();
      test_coll_mappings.emplace(cid, coll_info_t(i));
    }
    submit_transaction(std::move(t));
    EXPECT_NE(old_location, coll_root.get_location());
    checking_mappings(coll_root);

    replay();
    checking_mappings(coll_root);
  });
}

TEST_F(collection_manager_test_t, update)
{
  run_async([this] {
    coll_root_t coll_root;
    {
      auto t = tm->create_transaction();
      coll_root = collection_manager->mkfs(*t).unsafe_get0();
      submit_transaction(std::move(t));
    }
    {
      auto t = tm->create_transaction();
      for (int i = 0; i < 2; i++) {
        coll_t cid(spg_t(pg_t(1,i+1), shard_id_t::NO_SHARD));
        collection_manager->create(coll_root, *t, cid, coll_info_t(i)).unsafe_get0();
        test_coll_mappings.emplace(cid, coll_info_t(i));
      }
      submit_transaction(std::move(t));
    }
    {
       auto iter1= test_coll_mappings.begin();
       auto iter2 = std::next(test_coll_mappings.begin(), 1);
       EXPECT_NE(iter1->second.split_bits, iter2->second.split_bits);
       auto t = tm->create_transaction();
       collection_manager->update(coll_root, *t, iter1->first, iter2->second).unsafe_get0();
       submit_transaction(std::move(t));
       iter1->second.split_bits = iter2->second.split_bits;
    }
    replay();
    checking_mappings(coll_root);
  });
}
