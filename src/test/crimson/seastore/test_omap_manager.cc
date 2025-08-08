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
    Transaction &t,
    bool variable_length = false) {
    // Generates a random number in the range [4, 128].
    auto randu = []() -> int { return 4 + (std::rand() % 125); };
    auto key = rand_name(variable_length ? randu() : STR_LEN);
    set_key(omap_root, t, key,
	    rand_buffer(variable_length ? randu() : STR_LEN));

    return key;
  }

  std::vector<std::string> set_random_keys(
    omap_root_t &omap_root,
    Transaction &t,
    size_t count) {
    std::map<std::string, ceph::bufferlist> kvs;
    std::vector<std::string> keys;
    keys.reserve(count);

    while (kvs.size() < count) {
      auto k = rand_name(STR_LEN);
      auto v = rand_buffer(STR_LEN);
      // 'inserted' will be true only if the key did not already exist.
      if (auto [_, inserted] = kvs.emplace(k, v); inserted) {
        test_omap_mappings[k] = v;
        keys.push_back(k);
      }
    }

    with_trans_intr(
      t,
      [&, this](auto &t) {
        return omap_manager->omap_set_keys(omap_root, t, std::move(kvs));
      }).unsafe_get();

    return keys;
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
    bool first_inclusive = false,
    bool last_inclusive = false) {

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
      .with_inclusive(first_inclusive, last_inclusive);

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

  bool check_mappings_fastpath(omap_root_t &omap_root, Transaction &t) {
    auto config = OMapManager::omap_list_config_t()
      .without_max()
      .with_inclusive(true, true);

    std::optional<std::string> null_key = std::nullopt;
    auto [complete, omap_list] = with_trans_intr(t, [&, this](auto &t) {
      return omap_manager->omap_list(
        omap_root, t, null_key, null_key, config); /* from start to end */
    }).unsafe_get();

    if (omap_list.size() != test_omap_mappings.size()) { return false; }
    return std::equal(test_omap_mappings.begin(),
                      test_omap_mappings.end(),
                      omap_list.begin(),
                      [](const auto &a, const auto &b) {
                        return a.first == b.first && a.second == b.second;
    });
  }

  void check_mappings_slowpath(omap_root_t &omap_root, Transaction &t) {
    for (const auto &i: test_omap_mappings){
      get_value(omap_root, t, i.first);
    }
  }

  void check_mappings(omap_root_t &omap_root, Transaction &t) {
    if (!check_mappings_fastpath(omap_root, t)) {
      check_mappings_slowpath(omap_root, t);
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

TEST_P(omap_manager_test_t, set_key)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    for (int i = 0; i < 8; ++i) {
      auto t = create_mutate_transaction();
      set_random_key(omap_root, *t);
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }
  });
}

TEST_P(omap_manager_test_t, set_keys)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    for (int i = 0; i < 8; ++i) {
      auto t = create_mutate_transaction();
      set_random_keys(omap_root, *t, std::pow(2, i));
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }
  });
}

TEST_P(omap_manager_test_t, rm_key)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    auto t = create_mutate_transaction();
    set_random_keys(omap_root, *t, 8);
    submit_transaction(std::move(t));
    check_mappings(omap_root);

    while (test_omap_mappings.size() > 0) {
      auto t = create_mutate_transaction();
      rm_key(omap_root, *t, test_omap_mappings.begin()->first);
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }
  });
}

TEST_P(omap_manager_test_t, rm_key_range)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    auto t = create_mutate_transaction();
    set_random_keys(omap_root, *t, 1200);
    submit_transaction(std::move(t));
    check_mappings(omap_root);

    std::vector<std::string> keys;
    for (const auto& [k, _] : test_omap_mappings) {
      keys.push_back(k);
    }

    logger().debug("== delete the middle 400 keys");
    {
      auto t = create_mutate_transaction();
      rm_key_range(omap_root, *t, keys[400], keys[799]);
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }

    logger().debug("== delete the first 400 keys");
    {
      auto t = create_mutate_transaction();
      rm_key_range(omap_root, *t, keys[0], keys[399]);
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }

    logger().debug("== delete the last 400 keys");
    {
      auto t = create_mutate_transaction();
      rm_key_range(omap_root, *t, keys[800], keys[1199]);
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }
  });
}

TEST_P(omap_manager_test_t, leafnode_split_merge_balancing)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    // Insert enough keys to grow tree depth to 2, ensuring the first
    // internal node is created via leaf node split.
    logger().debug("== first leaf node split");
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
    logger().debug("== second leaf node split");
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
      auto it = std::next(test_omap_mappings.begin(), test_omap_mappings.size()/2);
      std::string first = it->first;
      std::string last = std::next(it, 64)->first;

      auto t = create_mutate_transaction();
      rm_key_range(omap_root, *t, first, last);
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }
  });
}

TEST_P(omap_manager_test_t, innernode_split_merge_balancing)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    // Grow tree depth to 3 so that inner nodes are created
    // at depth 2 via inner node splits.
    logger().debug("== first inner node split");
    while (omap_root.get_depth() < 3) {
      auto t = create_mutate_transaction();
      for (int i = 0; i < 64; i++) {
        // Use large value size to accelerate tree growth.
        auto key = rand_name(STR_LEN);
        set_key(omap_root, *t, key, rand_buffer(512));
      }
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);

    // Insert the same total number of keys again to force additional
    // inner node splits under the same internal node.
    logger().debug("== second inner node split");
    auto keys_for_leaf_split = test_omap_mappings.size();
    auto t = create_mutate_transaction();
    for (unsigned i = 0; i < keys_for_leaf_split; ++i) {
      // Use large value size to accelerate tree growth.
      auto key = rand_name(STR_LEN);
      set_key(omap_root, *t, key, rand_buffer(512));
      if (i % 64 == 0) {
        submit_transaction(std::move(t));
        t = create_mutate_transaction();
      }
    }
    submit_transaction(std::move(t));
    check_mappings(omap_root);

    // Remove keys to trigger leaf node merges and balancing,
    // eventually contracting the tree back to depth 2.
    logger().debug("== merges and balancing");
    while (omap_root.get_depth() > 2) {
      auto it = std::next(test_omap_mappings.begin(), test_omap_mappings.size()/2);
      std::string first = it->first;
      std::string last = std::next(it, 64)->first;

      auto t = create_mutate_transaction();
      rm_key_range(omap_root, *t, first, last);
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);
  });
}

TEST_P(omap_manager_test_t, clear)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    logger().debug("== filling tree to depth 2");
    while (omap_root.get_depth() < 2) {
      auto t = create_mutate_transaction();
      for (int i = 0; i < 64; i++) {
        set_random_key(omap_root, *t);
      }
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);

    logger().debug("== clearing entire tree");
    auto t = create_mutate_transaction();
    clear(omap_root, *t);
    submit_transaction(std::move(t));
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

TEST_P(omap_manager_test_t, variable_key_value_sizes)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    while (omap_root.get_depth() < 2) {
      auto t = create_mutate_transaction();
      for (unsigned i = 0; i < 128; ++i) {
        set_random_key(omap_root, *t, true /* variable sizes */);
      }
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }
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
  });
}

TEST_P(omap_manager_test_t, list)
{
  run_async([this] {
    omap_root_t omap_root = initialize();
    std::optional<std::string> first;
    std::optional<std::string> last;
    std::vector<std::string> generated_keys;

    auto list_and_log = [&](unsigned target_depth, std::string_view label) {
      do {
        auto t = create_mutate_transaction();
        for (unsigned i = 0; i < 20; ++i) {
          // Use large value size to accelerate tree growth.
          auto key = rand_name(STR_LEN);
          generated_keys.push_back(key);
          set_key(omap_root, *t, key, rand_buffer(512));
        }
        submit_transaction(std::move(t));
      } while (omap_root.depth < target_depth);

      std::sort(generated_keys.begin(), generated_keys.end());
      logger().debug("[depth={}] {}", target_depth, label);
      check_mappings(omap_root);

      // full range list
      auto t = create_read_transaction();
      first = last = std::nullopt;
      list(omap_root, *t, first, last, test_omap_mappings.size(), true, true);
      list(omap_root, *t, first, last, test_omap_mappings.size(), false, false);

      // 1/3 ~ 2/3 list.
      t = create_read_transaction();
      auto i1 = generated_keys.size() / 3;
      auto i2 = generated_keys.size() / 3 * 2;
      first = generated_keys[i1];
      last = generated_keys[i2];
      list(omap_root, *t, first, last, i2-i1, true, true);
      list(omap_root, *t, first, last, i2-i1, false, false);
    };

    list_and_log(1, "list single leaf node");
    list_and_log(2, "list single inner node with multiple leaf nodes");
    list_and_log(3, "list multiple inner and leaf nodes");
  });
}

TEST_P(omap_manager_test_t, long_key_stress_test)
{
  // reproduces https://tracker.ceph.com/issues/72270
  run_async([this] {
    omap_root_t omap_root = initialize();

    size_t target_size = 10 * 1000; // 10MB
    while (test_omap_mappings.size() < target_size) {
      auto t = create_mutate_transaction();
      for (unsigned i = 0; i < 64; i++) {
        auto key = rand_name(1000);
        set_key(omap_root, *t, key, rand_buffer(1));
        key = rand_name(1000);
        set_key(omap_root, *t, key, rand_buffer(1));
        rm_key(omap_root, *t, test_omap_mappings.begin()->first);
      }
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);
  });
}

TEST_P(omap_manager_test_t, increasing_key_size)
{
  // reproduces https://tracker.ceph.com/issues/72303
  run_async([this] {
    omap_root_t omap_root = initialize();

    for (int i = 0; i < 1000; i++) {
      auto t = create_mutate_transaction();
      std::string key(i, 'A');
      set_key(omap_root, *t, key, rand_buffer(1024));
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);

    while (test_omap_mappings.size() > 0) {
      auto t = create_mutate_transaction();
      rm_key(omap_root, *t, test_omap_mappings.begin()->first);
      submit_transaction(std::move(t));
    }
  });
}

TEST_P(omap_manager_test_t, heavy_update)
{
  run_async([this] {
    omap_root_t omap_root = initialize();

    std::vector<std::string> inserted_keys;
    while (omap_root.get_depth() < 2) {
      auto t = create_mutate_transaction();
      for (unsigned i = 0; i < 64; ++i) {
        auto key = set_random_key(omap_root, *t);
        inserted_keys.push_back(key);
      }
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);

    for (unsigned round = 0; round < 10; ++round) {
      // For each round, select 1024 random keys (including possible
      // duplicates) and update their values with data of size
      // pow(2, round) bytes. This covers a wide range of update
      // scenarios with varying value sizes.
      auto t = create_mutate_transaction();
      for (unsigned batch = 0; batch < 1024; ++batch) {
        auto key = inserted_keys[rand() % inserted_keys.size()];
        auto val = rand_buffer(std::pow(2, round));
        set_key(omap_root, *t, key, val);
      }
      check_mappings(omap_root, *t);
      submit_transaction(std::move(t));
      check_mappings(omap_root);
    }
  });
}

TEST_P(omap_manager_test_t, monotonic_inc)
{
  // This test simulate a real-world common pattern of inserting
  // monotonically increasing new keys and sequentially deleting
  // the oldest keys. The goal is to detect any abnormal splits
  // (imbalances) that may occur during right-skewed growth, and
  // then repeatedly delete the oldest keys to trigger left-skewed
  // deletions, checking for bugs in repeated left-side rebalancing.
  run_async([this] {
    omap_root_t omap_root = initialize();

    auto monotonic_inc = []() {
      static uint64_t counter = 0;
      char buf[32];
      snprintf(buf, sizeof(buf), "%16lu", counter++);
      return std::string(buf);
    };

    while (omap_root.get_depth() < 3) {
      auto t = create_mutate_transaction();
      for (int i = 0; i < 128; i++) {
        auto key = monotonic_inc();
        auto val = rand_buffer(512);
        set_key(omap_root, *t, key, val);
      }
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);

    while (test_omap_mappings.size() > 128) {
      auto t = create_mutate_transaction();
      auto first = test_omap_mappings.begin()->first;
      auto last = std::next(test_omap_mappings.begin(), 128)->first;
      rm_key_range(omap_root, *t, first, last);
      submit_transaction(std::move(t));
    }
    check_mappings(omap_root);
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
