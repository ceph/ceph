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

  using test_omap_t = std::map<std::string, std::string>;
  test_omap_t test_omap_mappings;

  bool set_key(
    omap_root_t &omap_root,
    Transaction &t,
    string &key,
    string &val) {
    auto ret = omap_manager->omap_set_key(omap_root, t, key, val).unsafe_get0();
    EXPECT_EQ(ret, true);
    test_omap_mappings[key] = val;
    return ret;
  }

  std::pair<string, string> get_value(
    omap_root_t &omap_root,
    Transaction &t,
    const string &key) {
    auto ret = omap_manager->omap_get_value(omap_root, t, key).unsafe_get0();
    EXPECT_EQ(key, ret.first);
    return ret;
  }

  bool rm_key(
    omap_root_t &omap_root,
    Transaction &t,
    const string &key) {
    auto ret = omap_manager->omap_rm_key(omap_root, t, key).unsafe_get0();
    EXPECT_EQ(ret, true);
    test_omap_mappings.erase(test_omap_mappings.find(key));
    return ret;
  }

  list_keys_result_t list_keys(
    omap_root_t &omap_root,
    Transaction &t,
    std::string &start,
    size_t max = MAX_SIZE) {
    auto ret = omap_manager->omap_list_keys(omap_root, t, start, max).unsafe_get0();
    if (start == "" && max == MAX_SIZE) {
      EXPECT_EQ(test_omap_mappings.size(), ret.keys.size());
      for ( auto &i : ret.keys) {
        auto it = test_omap_mappings.find(i);
        EXPECT_NE(it, test_omap_mappings.end());
        EXPECT_EQ(i, it->first);
      }
      EXPECT_EQ(ret.next, std::nullopt);
    } else {
      size_t i =0;
      auto it = test_omap_mappings.find(start);
      for (; it != test_omap_mappings.end() && i < max; it++) {
        EXPECT_EQ(ret.keys[i], it->first);
        i++;
      }
      if (it == test_omap_mappings.end()) {
        EXPECT_EQ(ret.next, std::nullopt);
      } else {
        EXPECT_EQ(ret.keys.size(), max);
        EXPECT_EQ(ret.next, it->first);
      }
    }
    return ret;
  }

  list_kvs_result_t list(
    omap_root_t &omap_root,
    Transaction &t,
    std::string &start,
    size_t max = MAX_SIZE) {
    auto ret = omap_manager->omap_list(omap_root, t, start, max).unsafe_get0();
    if (start == "" && max == MAX_SIZE) {
      EXPECT_EQ(test_omap_mappings.size(), ret.kvs.size());
      for ( auto &i : ret.kvs) {
        auto it = test_omap_mappings.find(i.first);
        EXPECT_NE(it, test_omap_mappings.end());
        EXPECT_EQ(i.second, it->second);
      }
      EXPECT_EQ(ret.next, std::nullopt);
    } else {
      size_t i = 0;
      auto it = test_omap_mappings.find(start);
      for (; it != test_omap_mappings.end() && i < max; it++) {
        EXPECT_EQ(ret.kvs[i].first, it->first);
        i++;
      }
      if (it == test_omap_mappings.end()) {
        EXPECT_EQ(ret.next, std::nullopt);
      } else {
        EXPECT_EQ(ret.kvs.size(), max);
        EXPECT_EQ(ret.next, it->first);
      }
    }

    return ret;
  }

  void clear(
    omap_root_t &omap_root,
    Transaction &t) {
    omap_manager->omap_clear(omap_root, t).unsafe_get0();
    EXPECT_EQ(omap_root.omap_root_laddr, L_ADDR_NULL);
  }

  void check_mappings(omap_root_t &omap_root, Transaction &t) {
    for (const auto &i: test_omap_mappings){
      auto ret = get_value(omap_root, t, i.first);
      EXPECT_EQ(i.first, ret.first);
      EXPECT_EQ(i.second, ret.second);
    }
  }

  void check_mappings(omap_root_t &omap_root) {
    auto t = tm->create_transaction();
    check_mappings(omap_root, *t);
  }

  void replay() {
    logger().debug("{}: begin", __func__);
    restart();
    omap_manager = omap_manager::create_omap_manager(*tm);
    logger().debug("{}: end", __func__);
  }
};

char* rand_string(char* str, const int len)
{
  int i;
  for (i = 0; i < len; ++i) {
    switch (rand() % 3) {
      case 1:
        str[i] = 'A' + rand() % 26;
        break;
      case 2:
        str[i] = 'a' +rand() % 26;
        break;
      case 0:
        str[i] = '0' + rand() % 10;
        break;
    }
  }
  str[len] = '\0';
  return str;
}

TEST_F(omap_manager_test_t, basic)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
    }

    string key = "owner";
    string val = "test";
    {
      auto t = tm->create_transaction();
      logger().debug("first transaction");
      [[maybe_unused]] auto setret = set_key(omap_root, *t, key, val);
      [[maybe_unused]] auto getret = get_value(omap_root, *t, key);
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    {
      auto t = tm->create_transaction();
      logger().debug("second transaction");
      [[maybe_unused]] auto getret = get_value(omap_root, *t, key);
      [[maybe_unused]] auto rmret = rm_key(omap_root, *t, key);
      [[maybe_unused]] auto getret2 = get_value(omap_root, *t, key);
      EXPECT_EQ(getret2.second, "");
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    {
      auto t = tm->create_transaction();
      logger().debug("third transaction");
      [[maybe_unused]] auto getret = get_value(omap_root, *t, key);
      EXPECT_EQ(getret.second, "");
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
  });
}

TEST_F(omap_manager_test_t, force_leafnode_split)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 50;
    char str[STR_LEN + 1];
    for (unsigned i = 0; i < 40; i++) {
      auto t = tm->create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 10; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 20 == 0) && (j == 5)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("force split submit transaction i = {}", i);
      tm->submit_transaction(std::move(t)).unsafe_get();
      check_mappings(omap_root);
    }
  });
}

TEST_F(omap_manager_test_t, force_leafnode_split_merge)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 50;
    char str[STR_LEN + 1];

    for (unsigned i = 0; i < 80; i++) {
      auto t = tm->create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction");
      tm->submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
        check_mappings(omap_root);
      }
    }
    auto t = tm->create_transaction();
    int i = 0;
    for (auto &e: test_omap_mappings) {
      if (i % 3 != 0) {
        [[maybe_unused]] auto rmref= rm_key(omap_root, *t, e.first);
      }

      if (i % 10 == 0) {
        logger().debug("submitting transaction i= {}", i);
        tm->submit_transaction(std::move(t)).unsafe_get();
        t = tm->create_transaction();
      }
      if (i % 100 == 0) {
        logger().debug("check_mappings  i= {}", i);
        check_mappings(omap_root, *t);
        check_mappings(omap_root);
      }
      i++;
    }
    logger().debug("finally submitting transaction ");
    tm->submit_transaction(std::move(t)).unsafe_get();
  });
}

TEST_F(omap_manager_test_t, force_leafnode_split_merge_fullandbalanced)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 50;
    char str[STR_LEN + 1];

    for (unsigned i = 0; i < 50; i++) {
      auto t = tm->create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction");
      tm->submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
        check_mappings(omap_root);
      }
    }
    auto t = tm->create_transaction();
    int i = 0;
    for (auto &e: test_omap_mappings) {
      if (30 < i && i < 100) {
        auto val = e;
        [[maybe_unused]] auto rmref= rm_key(omap_root, *t, e.first);
      }

      if (i % 10 == 0) {
      logger().debug("submitting transaction i= {}", i);
        tm->submit_transaction(std::move(t)).unsafe_get();
        t = tm->create_transaction();
      }
      if (i % 50 == 0) {
      logger().debug("check_mappings  i= {}", i);
        check_mappings(omap_root, *t);
        check_mappings(omap_root);
      }
      i++;
      if (i == 100)
 break;
    }
    logger().debug("finally submitting transaction ");
    tm->submit_transaction(std::move(t)).unsafe_get();
    check_mappings(omap_root);
  });
}


TEST_F(omap_manager_test_t, force_split_listkeys_list_clear)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 300;
    char str[STR_LEN + 1];
    string temp;
    for (unsigned i = 0; i < 40; i++) {
      auto t = tm->create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 10; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if (i == 10)
          temp = key;
        if ((i % 20 == 0) && (j == 5)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("force split submit transaction i = {}", i);
      tm->submit_transaction(std::move(t)).unsafe_get();
      check_mappings(omap_root);
    }
    std::string empty = "";
    auto t = tm->create_transaction();
    [[maybe_unused]] auto keys = list_keys(omap_root, *t, empty);
    tm->submit_transaction(std::move(t)).unsafe_get();

    t = tm->create_transaction();
    keys = list_keys(omap_root, *t, temp, 100);
    tm->submit_transaction(std::move(t)).unsafe_get();

    t = tm->create_transaction();
    [[maybe_unused]] auto ls = list(omap_root, *t, empty);
    tm->submit_transaction(std::move(t)).unsafe_get();

    t = tm->create_transaction();
    ls = list(omap_root, *t, temp, 100);
    tm->submit_transaction(std::move(t)).unsafe_get();

    t = tm->create_transaction();
    clear(omap_root, *t);
    tm->submit_transaction(std::move(t)).unsafe_get();

  });
}

TEST_F(omap_manager_test_t, internal_force_split)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 300;
    char str[STR_LEN + 1];
    for (unsigned i = 0; i < 10; i++) {
      logger().debug("opened split transaction");
      auto t = tm->create_transaction();

      for (unsigned j = 0; j < 80; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 2 == 0) && (j % 50 == 0)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction i = {}", i);
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    check_mappings(omap_root);
  });
}

TEST_F(omap_manager_test_t, internal_force_merge_fullandbalanced)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 300;
    char str[STR_LEN + 1];

    for (unsigned i = 0; i < 8; i++) {
      logger().debug("opened split transaction");
      auto t = tm->create_transaction();

      for (unsigned j = 0; j < 80; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 2 == 0) && (j % 50 == 0)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction");
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    auto t = tm->create_transaction();
    int i = 0;
    for (auto &e: test_omap_mappings) {
        auto val = e;
        [[maybe_unused]] auto rmref= rm_key(omap_root, *t, e.first);

      if (i % 10 == 0) {
      logger().debug("submitting transaction i= {}", i);
        tm->submit_transaction(std::move(t)).unsafe_get();
        t = tm->create_transaction();
      }
      if (i % 50 == 0) {
      logger().debug("check_mappings  i= {}", i);
        check_mappings(omap_root, *t);
        check_mappings(omap_root);
      }
      i++;
    }
    logger().debug("finally submitting transaction ");
    tm->submit_transaction(std::move(t)).unsafe_get();
    check_mappings(omap_root);
  });
}

TEST_F(omap_manager_test_t, replay)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
      replay();
    }
    const int STR_LEN = 300;
    char str[STR_LEN + 1];

    for (unsigned i = 0; i < 8; i++) {
      logger().debug("opened split transaction");
      auto t = tm->create_transaction();

      for (unsigned j = 0; j < 80; ++j) {
        string key(rand_string(str, rand() % STR_LEN));
        string val(rand_string(str, rand() % STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
        if ((i % 2 == 0) && (j % 50 == 0)) {
          check_mappings(omap_root, *t);
        }
      }
      logger().debug("submitting transaction i = {}", i);
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    replay();
    check_mappings(omap_root);

    auto t = tm->create_transaction();
    int i = 0;
    for (auto &e: test_omap_mappings) {
        auto val = e;
        [[maybe_unused]] auto rmref= rm_key(omap_root, *t, e.first);

      if (i % 10 == 0) {
      logger().debug("submitting transaction i= {}", i);
        tm->submit_transaction(std::move(t)).unsafe_get();
        replay();
        t = tm->create_transaction();
      }
      if (i % 50 == 0) {
      logger().debug("check_mappings  i= {}", i);
        check_mappings(omap_root, *t);
        check_mappings(omap_root);
      }
      i++;
    }
    logger().debug("finally submitting transaction ");
    tm->submit_transaction(std::move(t)).unsafe_get();
    replay();
    check_mappings(omap_root);
  });
}


TEST_F(omap_manager_test_t, internal_force_split_to_root)
{
  run_async([this] {
    omap_root_t omap_root(0, L_ADDR_NULL);
    {
      auto t = tm->create_transaction();
      omap_root = omap_manager->initialize_omap(*t).unsafe_get0();
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
    const int STR_LEN = 300;
    char str[STR_LEN + 1];

    logger().debug("set big keys");
    for (unsigned i = 0; i < 53; i++) {
      auto t = tm->create_transaction();

      for (unsigned j = 0; j < 8; ++j) {
        string key(rand_string(str, STR_LEN));
        string val(rand_string(str, STR_LEN));
        [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
      }
      logger().debug("submitting transaction i = {}", i);
      tm->submit_transaction(std::move(t)).unsafe_get();
    }
     logger().debug("set small keys");
     const int STR_LEN_2 = 100;
     char str_2[STR_LEN_2 + 1];
     for (unsigned i = 0; i < 100; i++) {
       auto t = tm->create_transaction();

       for (unsigned j = 0; j < 8; ++j) {
         string key(rand_string(str_2, STR_LEN_2));
         string val(rand_string(str_2, STR_LEN_2));
         [[maybe_unused]] auto addref = set_key(omap_root, *t, key, val);
       }
      logger().debug("submitting transaction last");
      tm->submit_transaction(std::move(t)).unsafe_get();
     }
    check_mappings(omap_root);
  });
}
