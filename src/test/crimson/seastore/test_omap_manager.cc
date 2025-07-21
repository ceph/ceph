// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/omap_manager.h"

#include "test/crimson/seastore/test_block.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace std;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

const int STR_LEN = 50;

std::string rand_name(const int len)
{
  std::string ret;
  ret.reserve(len);
  for (int i = 0; i < len; ++i) {
    ret.append(1, (char)(rand() % ('z' - '0')) + '0');
  }
  return ret;
}

bufferlist rand_buffer(const int len) {
  bufferptr ptr(len);
  for (auto i = ptr.c_str(); i < ptr.c_str() + len; ++i) {
    *i = (char)rand();
  }
  bufferlist bl;
  bl.append(ptr);
  return bl;
}

struct omap_manager_test_t :
  public seastar_test_suite_t,
  TMTestState {

  OMapManagerRef omap_manager;

  omap_manager_test_t() {}

  seastar::future<> set_up_fut() final {
    return tm_setup().then([this] {
      omap_manager = omap_manager::create_omap_manager(*tm);
      return seastar::now();
    });
  }

  seastar::future<> tear_down_fut() final {
    return tm_teardown().then([this] {
      omap_manager.reset();
      return seastar::now();
    });
  }

  using test_omap_t = std::map<std::string, ceph::bufferlist>;
  test_omap_t test_omap_mappings;

  void set_key(
    omap_root_t &omap_root,
    Transaction &t,
    const string &key,
    const bufferlist &val) {
    with_trans_intr(
      t,
      [&, this](auto &t) {
	return omap_manager->omap_set_key(omap_root, t, key, val);
      }).unsafe_get();
    test_omap_mappings[key] = val;
  }

  void set_key(
    omap_root_t &omap_root,
    Transaction &t,
    const string &key,
    const string &val) {
    bufferlist bl;
    bl.append(val);
    set_key(omap_root, t, key, bl);
  }

  std::string set_random_key(
    omap_root_t &omap_root,
    Transaction &t) {
    auto key = rand_name(STR_LEN);
    set_key(
      omap_root,
      t,
      key,
      rand_buffer(STR_LEN));
    return key;
  }

  void get_value(
    omap_root_t &omap_root,
    Transaction &t,
    const string &key) {
    auto ret = with_trans_intr(
      t,
      [&, this](auto &t) {
	return omap_manager->omap_get_value(omap_root, t, key);
      }).unsafe_get();
    auto iter = test_omap_mappings.find(key);
    if (iter == test_omap_mappings.end()) {
      EXPECT_FALSE(ret);
    } else {
      EXPECT_TRUE(ret);
      if (ret) {
	EXPECT_TRUE(*ret == iter->second);
      }
    }
  }

  void rm_key(
    omap_root_t &omap_root,
    Transaction &t,
    const string &key) {
    with_trans_intr(
      t,
      [&, this](auto &t) {
	return omap_manager->omap_rm_key(omap_root, t, key);
      }).unsafe_get();
    test_omap_mappings.erase(test_omap_mappings.find(key));
  }

  std::vector<std::string> rm_key_range(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &first,
    const std::string &last) {
    logger().debug("rm keys in range {} ~ {}", first, last);
    auto config = OMapManager::omap_list_config_t()
      .with_max(3000)
      .with_inclusive(true, false);

    with_trans_intr(
      t,
      [&, this](auto &t) {
      return omap_manager->omap_rm_key_range(
	omap_root, t, first, last, config);
    }).unsafe_get();

    std::vector<std::string> keys;
    size_t count = 0;
    for (auto iter = test_omap_mappings.begin();
	iter != test_omap_mappings.end(); ) {
      if (iter->first >= first && iter->first < last) {
	keys.push_back(iter->first);
	iter = test_omap_mappings.erase(iter);
	count++;
      } else {
	iter++;
      }
      if (count == config.max_result_size) {
	break;
      }
    }
    return keys;
  }

  void list(
    const omap_root_t &omap_root,
    Transaction &t,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    size_t max = 128,
    bool inclusive = false) {

    if (first && last) {
      logger().debug("list on {} ~ {}", *first, *last);
    } else if (first) {
      logger().debug("list on {} ~ end", *first);
    } else if (last) {
      logger().debug("list on start ~ {}", *last);
    } else {
      logger().debug("list on start ~ end");
    }

    auto config = OMapManager::omap_list_config_t()
      .with_max(max)
      .with_inclusive(inclusive, false);

    auto [complete, results] = with_trans_intr(
      t,
      [&, this](auto &t) {
	return omap_manager->omap_list(omap_root, t, first, last, config);
      }).unsafe_get();

    test_omap_t::iterator it, lit;
    if (first) {
      it = config.first_inclusive ?
	test_omap_mappings.lower_bound(*first) :
	test_omap_mappings.upper_bound(*first);
    } else {
      it = test_omap_mappings.begin();
    }
    if (last) {
      lit = config.last_inclusive ?
	test_omap_mappings.upper_bound(*last) :
	test_omap_mappings.lower_bound(*last);
    } else {
      lit = test_omap_mappings.end();
    }

    for (auto &&[k, v]: results) {
      EXPECT_NE(it, test_omap_mappings.end());
      if (it == test_omap_mappings.end()) {
	return;
      }
      EXPECT_EQ(k, it->first);
      EXPECT_EQ(v, it->second);
      it++;
    }
    if (it == lit) {
      EXPECT_TRUE(complete);
    } else {
      EXPECT_EQ(results.size(), max);
    }
  }

  ObjectStore::omap_iter_ret_t  check_iterate(std::string_view key,
                                              std::string_view val,
                                              ObjectStore::omap_iter_seek_t &start_from)
  {
    static uint32_t current_index = 0;
    static uint32_t last_index = 0;
    static bool check_start = true;

    if (check_start && start_from.seek_position != "") {
      if (start_from.seek_type == ObjectStore::omap_iter_seek_t::LOWER_BOUND) {
        EXPECT_TRUE(start_from.seek_position == key);
      } else {
        EXPECT_TRUE(start_from.seek_position < key);
      }
      check_start = false;
    }

    auto iter = test_omap_mappings.find(std::string(key));
    EXPECT_TRUE(iter != test_omap_mappings.end());
    ceph::bufferlist bl = iter->second;
    std::string result(bl.c_str(), bl.length());
    EXPECT_TRUE(result == val);
    current_index = std::distance(test_omap_mappings.begin(), iter);
    if (last_index != 0) {
      EXPECT_EQ(last_index + 1, current_index);
    }
    last_index = current_index;

    if (current_index > test_omap_mappings.size() - 10) {
      current_index = 0;
      last_index = 0;
      check_start = true;
      return ObjectStore::omap_iter_ret_t::STOP;
    } else {
      return ObjectStore::omap_iter_ret_t::NEXT;
    }
 }

  void iterate(
    const omap_root_t &omap_root,
    Transaction &t,
    ObjectStore::omap_iter_seek_t &start_from,
    OMapManager::omap_iterate_cb_t callback) {

    if (start_from.seek_type == ObjectStore::omap_iter_seek_t::LOWER_BOUND) {
      logger().debug("iterate lower bound on {}", start_from.seek_position);
    } else {
      logger().debug("iterate upper bound on {}", start_from.seek_position);
    }

    auto ret = with_trans_intr(
      t,
      [&, this](auto &t) {
        return omap_manager->omap_iterate(omap_root, t, start_from, callback);
      }).unsafe_get();

    EXPECT_EQ(ret, ObjectStore::omap_iter_ret_t::STOP);
  }

  void clear(
    omap_root_t &omap_root,
    Transaction &t) {
    with_trans_intr(
      t,
      [&, this](auto &t) {
	return omap_manager->omap_clear(omap_root, t);
      }).unsafe_get();
    EXPECT_EQ(omap_root.get_location(), L_ADDR_NULL);
  }

  void check_mappings(omap_root_t &omap_root, Transaction &t) {
    for (const auto &i: test_omap_mappings){
      get_value(omap_root, t, i.first);
    }
  }

  void check_mappings(omap_root_t &omap_root) {
    auto t = create_read_transaction();
    check_mappings(omap_root, *t);
  }

  std::vector<std::string> get_mapped_keys() {
    std::vector<std::string> mkeys;
    mkeys.reserve(test_omap_mappings.size());
    for (auto &k: test_omap_mappings) {
      mkeys.push_back(k.first);
    }
    return mkeys;
  }

  void replay() {
    restart();
    omap_manager = omap_manager::create_omap_manager(*tm);
  }

  auto initialize() {
    auto t = create_mutate_transaction();
    omap_root_t omap_root = with_trans_intr(
      *t,
      [this](auto &t) {
	return omap_manager->initialize_omap(t, L_ADDR_MIN,
	  omap_type_t::OMAP);
      }).unsafe_get();
    submit_transaction(std::move(t));
    return omap_root;
  }
};

TEST_P(omap_manager_test_t, basic)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    string key = "owner";
    string val = "test";

    {
      auto t = create_mutate_transaction();
      logger().debug("first transaction");
      set_key(omap_root, *t, key, val);
      get_value(omap_root, *t, key);
      submit_transaction(std::move(t));
    }
    {
      auto t = create_mutate_transaction();
      logger().debug("second transaction");
      get_value(omap_root, *t, key);
      rm_key(omap_root, *t, key);
      get_value(omap_root, *t, key);
      submit_transaction(std::move(t));
    }
    {
      auto t = create_mutate_transaction();
      logger().debug("third transaction");
      get_value(omap_root, *t, key);
      submit_transaction(std::move(t));
    }
  });
}

TEST_P(omap_manager_test_t, leafnode_split_merge_balancing)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    // Insert enough keys to grow tree depth to 2, ensuring the first
    // internal node is created via leaf node split.
    logger().debug("== first split");
    while (omap_root.get_depth() < 2) {
      auto t = create_mutate_transaction();
      for (int i = 0; i < 64; i++) {
        set_random_key(omap_root, *t);
      }
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }

    // Insert the same total number of keys again to force additional
    // leaf node splits under the same internal node.
    logger().debug("== second split");
    auto keys_for_leaf_split = test_omap_mappings.size();
    auto t = create_mutate_transaction();
    for (unsigned i = 0; i < keys_for_leaf_split; ++i) {
      set_random_key(omap_root, *t);
    }
    check_mappings(omap_root, *t);
    submit_transaction(std::move(t));
    check_mappings(omap_root);

    // Remove keys to trigger leaf node merges and balancing,
    // eventually contracting the tree back to depth 1.
    logger().debug("== merges and balancing");
    while (omap_root.get_depth() > 1) {
      auto t = create_mutate_transaction();
      for (int i = 0; i < 64; i++) {
        rm_key(omap_root, *t,
               std::next(test_omap_mappings.begin(),
		         test_omap_mappings.size()/2)->first);
      }
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }
  });
}

TEST_P(omap_manager_test_t, force_split_listkeys_list_rmkey_range_clear)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    string first, last;
    for (unsigned i = 0; i < 40; i++) {
      auto t = create_mutate_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 10; ++j) {
        auto key = set_random_key(omap_root, *t);
        if (i == 10) {
          first = key;
	}
	if (i == 30) {
	  last = key;
	  if (first > last) {
	    std::swap(first, last);
	  }
	}
        if ((i % 20 == 0) && (j == 5)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("force split submit transaction i = {}", i);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }

    std::optional<std::string> first_temp;
    std::optional<std::string> last_temp;
    {
      auto t = create_read_transaction();
      first_temp = std::nullopt;
      last_temp = std::nullopt;
      list(omap_root, *t, first_temp, last_temp);
    }

    {
      auto t = create_read_transaction();
      first_temp = first;
      last_temp = std::nullopt;
      list(omap_root, *t, first_temp, last_temp, 100);
    }

    {
      auto t = create_read_transaction();
      first_temp = first;
      last_temp = std::nullopt;
      list(omap_root, *t, first_temp, last_temp, 100, true);
    }

    {
      auto t = create_read_transaction();
      first_temp = std::nullopt;
      last_temp = last;
      list(omap_root, *t, first_temp, last_temp, 10240);
    }

    {
      auto t = create_read_transaction();
      first_temp = first;
      last_temp = last;
      list(omap_root, *t, first_temp, last_temp, 10240, true);
    }

    {
      auto t = create_read_transaction();
      list(omap_root, *t, first, last, 10240, true);
    }

    {
      auto t = create_mutate_transaction();
      auto keys = rm_key_range(omap_root, *t, first, last);
      for (const auto& key : keys) {
	get_value(omap_root, *t, key);
      }
      submit_transaction(std::move(t));
    }

    {
      auto t = create_mutate_transaction();
      clear(omap_root, *t);
      submit_transaction(std::move(t));
    }
  });
}

TEST_P(omap_manager_test_t, force_inner_node_split_list_rmkey_range)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    string first = "";
    string last;
    while (cache->get_omap_tree_depth() < 3) {
      for (unsigned i = 0; i < 40; i++) {
	auto t = create_mutate_transaction();
	logger().debug("opened transaction");
	for (unsigned j = 0; j < 10; ++j) {
	  auto key = set_random_key(omap_root, *t);
	  if (key.compare(first) < 0 || !first.length()) {
	    first = key;
	  }
	  if (i == 10) {
	    last = key;
	  }
	}
	logger().debug("force split submit transaction i = {}", i);
	submit_transaction(std::move(t));
      }
    }

    std::optional<std::string> first_temp;
    std::optional<std::string> last_temp;
    {
      auto t = create_read_transaction();
      first_temp = first;
      last_temp = std::nullopt;
      list(omap_root, *t, first_temp, last_temp, 10240);
    }

    {
      auto t = create_read_transaction();
      first_temp = first;
      last_temp = std::nullopt;
      list(omap_root, *t, first_temp, last_temp, 10240, true);
    }

    {
      auto t = create_read_transaction();
      first_temp = std::nullopt;
      last_temp = last;
      list(omap_root, *t, first_temp, last_temp, 10240);
    }

    {
      auto t = create_read_transaction();
      first_temp = first;
      last_temp = last;
      list(omap_root, *t, first_temp, last_temp, 10240, true);
    }

    {
      auto t = create_mutate_transaction();
      auto keys = rm_key_range(omap_root, *t, first, last);
      for (const auto& key : keys) {
	get_value(omap_root, *t, key);
      }
      submit_transaction(std::move(t));
    }

    {
      auto t = create_mutate_transaction();
      clear(omap_root, *t);
      submit_transaction(std::move(t));
    }
  });
}

TEST_P(omap_manager_test_t, internal_force_split)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    for (unsigned i = 0; i < 10; i++) {
      logger().debug("opened split transaction");
      auto t = create_mutate_transaction();

      for (unsigned j = 0; j < 80; ++j) {
        set_random_key(omap_root, *t);
        if ((i % 2 == 0) && (j % 50 == 0)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction i = {}", i);
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);
  });
}

TEST_P(omap_manager_test_t, internal_force_merge_fullandbalanced)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    for (unsigned i = 0; i < 8; i++) {
      logger().debug("opened split transaction");
      auto t = create_mutate_transaction();

      for (unsigned j = 0; j < 80; ++j) {
        set_random_key(omap_root, *t);
        if ((i % 2 == 0) && (j % 50 == 0)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction");
      submit_transaction(std::move(t));
    }
    auto mkeys = get_mapped_keys();
    auto t = create_mutate_transaction();
    for (unsigned i = 0; i < mkeys.size(); i++) {
      rm_key(omap_root, *t, mkeys[i]);

      if (i % 10 == 0) {
        logger().debug("submitting transaction i= {}", i);
        submit_transaction(std::move(t));
        t = create_mutate_transaction();
      }
      if (i % 50 == 0) {
        logger().debug("check_mappings  i= {}", i);
        check_mappings(omap_root, *t);
        check_mappings(omap_root);
      }
    }
    logger().debug("finally submitting transaction ");
    submit_transaction(std::move(t));
    check_mappings(omap_root);
  });
}

TEST_P(omap_manager_test_t, replay)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    // Repeatedly apply set/rm operations until the omap tree reaches
    // depth 2. This simulates real-world scenarios where data is
    // inserted and deleted over time, and ensures that replay after
    // structural transitions does not corrupt tree state.
    //
    // Each iteration inserts 256 keys and removes 128, driving split
    // pressure with a controlled amount of churn.
    while (omap_root.get_depth() < 2) {
      auto t = create_mutate_transaction();
      logger().debug("== begin split-churn cycle (num_keys = {})",
	             test_omap_mappings.size());

      for (int i = 0; i < 128; i++) {
        set_random_key(omap_root, *t);
        set_random_key(omap_root, *t);
        rm_key(omap_root, *t, test_omap_mappings.begin()->first);
      }
      submit_transaction(std::move(t));

      replay();
      check_mappings(omap_root);
    }

    // Gradually remove 128 keys at a time — matching the number
    // inserted per iteration earlier. This triggers a merge that
    // shrinks the tree back to depth 1, allowing us to verify that
    // replay remains correct after structural contraction.
    while (omap_root.get_depth() > 1) {
      auto t = create_mutate_transaction();
      logger().debug("== begin full deletion to trigger merge");

      auto first = test_omap_mappings.begin()->first;
      auto last = std::next(test_omap_mappings.begin(), 128)->first;
      rm_key_range(omap_root, *t, first, last);
      submit_transaction(std::move(t));

      replay();
      check_mappings(omap_root);
    }
  });
}


TEST_P(omap_manager_test_t, internal_force_split_to_root)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    logger().debug("set big keys");
    for (unsigned i = 0; i < 53; i++) {
      auto t = create_mutate_transaction();

      for (unsigned j = 0; j < 8; ++j) {
        set_random_key(omap_root, *t);
      }
      logger().debug("submitting transaction i = {}", i);
      submit_transaction(std::move(t));
    }
     logger().debug("set small keys");
     for (unsigned i = 0; i < 100; i++) {
       auto t = create_mutate_transaction();
       for (unsigned j = 0; j < 8; ++j) {
         set_random_key(omap_root, *t);
       }
      logger().debug("submitting transaction last");
      submit_transaction(std::move(t));
     }
    check_mappings(omap_root);
  });
}

TEST_P(omap_manager_test_t, omap_iterate)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    std::string lower_key;
    std::string upper_key;
    ObjectStore::omap_iter_seek_t start_from;

    auto insert_batches = [&](unsigned num_batches) {
      for (unsigned i = 0; i < num_batches; ++i) {
        auto t = create_mutate_transaction();
        logger().debug("opened transaction");
        for (unsigned j = 0; j < 64; ++j) {
	  // Use large value size to accelerate tree growth.
          auto key = rand_name(STR_LEN);
          set_key(omap_root, *t, key, rand_buffer(512));
          if (i == 3) {
            lower_key = key;
          }
	  if (i == 5) {
	    upper_key = key;
	  }
	}
        submit_transaction(std::move(t));
      }
    };

    while (omap_root.get_depth() < 3) {
      insert_batches(10);
    }
    // Insert the same number of random key-value pairs again
    // to ensure that depth 2 contains more than two inner nodes.
    // This is necessary to evaluate iteration that spans across
    // multiple inner nodes.
    auto target_size = test_omap_mappings.size() * 2;
    while (test_omap_mappings.size() < target_size) {
      insert_batches(10);
    }

    std::function<ObjectStore::omap_iter_ret_t(std::string_view, std::string_view)> callback =
      [this, &start_from](std::string_view key, std::string_view val) {
        return this->check_iterate(key, val, start_from);
    };
    {
      start_from.seek_position = lower_key;
      start_from.seek_type = ObjectStore::omap_iter_seek_t::LOWER_BOUND;
      auto t = create_read_transaction();
      iterate(omap_root, *t, start_from, callback);
    }

    {
      start_from.seek_position = upper_key;
      start_from.seek_type = ObjectStore::omap_iter_seek_t::UPPER_BOUND;
      auto t = create_read_transaction();
      iterate(omap_root, *t, start_from, callback);
    }

    {
      start_from = ObjectStore::omap_iter_seek_t::min_lower_bound();
      auto t = create_read_transaction();
      iterate(omap_root, *t, start_from, callback);
    }
    {
      auto t = create_mutate_transaction();
      clear(omap_root, *t);
      submit_transaction(std::move(t));
    }
  });
}

INSTANTIATE_TEST_SUITE_P(
  omap_manager_test,
  omap_manager_test_t,
  ::testing::Combine(
    ::testing::Values (
      "segmented",
      "circularbounded"
    ),
    ::testing::Values(
      integrity_check_t::FULL_CHECK)
  )
);
