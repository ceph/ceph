// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <array>
#include <cstring>
#include <memory>
#include <set>
#include <sstream>
#include <vector>

#include "crimson/common/log.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/dummy.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_layout.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree_utils.h"

#include "test/crimson/gtest_seastar.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"
#include "test_value.h"

using namespace crimson::os::seastore::onode;

#define INTR(fun, t)            \
  with_trans_intr(              \
    t,                          \
    [&] (auto &tr) {            \
      return fun(tr);           \
    }                           \
  )

#define INTR_R(fun, t, args...)         \
  with_trans_intr(                      \
    t,                                  \
    [&] (auto &tr) {                    \
      return fun(tr, args);             \
    }                                   \
  )

#define INTR_WITH_PARAM(fun, c, b, v)   \
  with_trans_intr(                      \
    c.t,                                \
    [=] (auto &t) {                     \
      return fun(c, L_ADDR_MIN, b, v);  \
    }                                   \
  )

namespace {
  constexpr bool IS_DUMMY_SYNC = false;
  using DummyManager = DummyNodeExtentManager<IS_DUMMY_SYNC>;

  using UnboundedBtree = Btree<UnboundedValue>;

  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }

  ghobject_t make_ghobj(
      shard_t shard, pool_t pool, crush_hash_t crush,
      std::string ns, std::string oid, snap_t snap, gen_t gen) {
    return ghobject_t{shard_id_t{shard}, pool, crush, ns, oid, snap, gen};
  }

  // return a key_view_t and its underlying memory buffer.
  // the buffer needs to be freed manually.
  std::pair<key_view_t, void*> build_key_view(const ghobject_t& hobj) {
    key_hobj_t key_hobj(hobj);
    size_t key_size = sizeof(shard_pool_crush_t) + sizeof(snap_gen_t) +
                      ns_oid_view_t::estimate_size(key_hobj);
    void* p_mem = std::malloc(key_size);

    key_view_t key_view;
    char* p_fill = (char*)p_mem + key_size;

    auto spc = shard_pool_crush_t::from_key(key_hobj);
    p_fill -= sizeof(shard_pool_crush_t);
    std::memcpy(p_fill, &spc, sizeof(shard_pool_crush_t));
    key_view.set(*reinterpret_cast<const shard_pool_crush_t*>(p_fill));

    auto p_ns_oid = p_fill;
    ns_oid_view_t::test_append(key_hobj, p_fill);
    ns_oid_view_t ns_oid_view(p_ns_oid);
    key_view.set(ns_oid_view);

    auto sg = snap_gen_t::from_key(key_hobj);
    p_fill -= sizeof(snap_gen_t);
    ceph_assert(p_fill == (char*)p_mem);
    std::memcpy(p_fill, &sg, sizeof(snap_gen_t));
    key_view.set(*reinterpret_cast<const snap_gen_t*>(p_fill));

    return {key_view, p_mem};
  }
}

struct a_basic_test_t : public seastar_test_suite_t {};

TEST_F(a_basic_test_t, 1_basic_sizes)
{
  logger().info("\n"
    "Bytes of struct:\n"
    "  node_header_t: {}\n"
    "  shard_pool_t: {}\n"
    "  shard_pool_crush_t: {}\n"
    "  crush_t: {}\n"
    "  snap_gen_t: {}\n"
    "  slot_0_t: {}\n"
    "  slot_1_t: {}\n"
    "  slot_3_t: {}\n"
    "  node_fields_0_t: {}\n"
    "  node_fields_1_t: {}\n"
    "  node_fields_2_t: {}\n"
    "  internal_fields_3_t: {}\n"
    "  leaf_fields_3_t: {}\n"
    "  internal_sub_item_t: {}",
    sizeof(node_header_t), sizeof(shard_pool_t),
    sizeof(shard_pool_crush_t), sizeof(crush_t), sizeof(snap_gen_t),
    sizeof(slot_0_t), sizeof(slot_1_t), sizeof(slot_3_t),
    sizeof(node_fields_0_t), sizeof(node_fields_1_t), sizeof(node_fields_2_t),
    sizeof(internal_fields_3_t), sizeof(leaf_fields_3_t), sizeof(internal_sub_item_t)
  );

  auto hobj = make_ghobj(0, 0, 0, "n", "o", 0, 0);
  key_hobj_t key(hobj);
  auto [key_view, p_mem] = build_key_view(hobj);
  value_config_t value;
  value.payload_size = 8;
#define _STAGE_T(NodeType) node_to_stage_t<typename NodeType::node_stage_t>
#define NXT_T(StageType)  staged<typename StageType::next_param_t>
  laddr_t i_value{0};
  logger().info("\n"
    "Bytes of a key-value insertion (full-string):\n"
    "  s-p-c, 'n'-'o', s-g => value_payload(8): typically internal 43B, leaf 59B\n"
    "  InternalNode0: {} {} {}\n"
    "  InternalNode1: {} {} {}\n"
    "  InternalNode2: {} {}\n"
    "  InternalNode3: {}\n"
    "  LeafNode0: {} {} {}\n"
    "  LeafNode1: {} {} {}\n"
    "  LeafNode2: {} {}\n"
    "  LeafNode3: {}",
    _STAGE_T(InternalNode0)::insert_size(key_view, i_value),
    NXT_T(_STAGE_T(InternalNode0))::insert_size(key_view, i_value),
    NXT_T(NXT_T(_STAGE_T(InternalNode0)))::insert_size(key_view, i_value),
    _STAGE_T(InternalNode1)::insert_size(key_view, i_value),
    NXT_T(_STAGE_T(InternalNode1))::insert_size(key_view, i_value),
    NXT_T(NXT_T(_STAGE_T(InternalNode1)))::insert_size(key_view, i_value),
    _STAGE_T(InternalNode2)::insert_size(key_view, i_value),
    NXT_T(_STAGE_T(InternalNode2))::insert_size(key_view, i_value),
    _STAGE_T(InternalNode3)::insert_size(key_view, i_value),
    _STAGE_T(LeafNode0)::insert_size(key, value),
    NXT_T(_STAGE_T(LeafNode0))::insert_size(key, value),
    NXT_T(NXT_T(_STAGE_T(LeafNode0)))::insert_size(key, value),
    _STAGE_T(LeafNode1)::insert_size(key, value),
    NXT_T(_STAGE_T(LeafNode1))::insert_size(key, value),
    NXT_T(NXT_T(_STAGE_T(LeafNode1)))::insert_size(key, value),
    _STAGE_T(LeafNode2)::insert_size(key, value),
    NXT_T(_STAGE_T(LeafNode2))::insert_size(key, value),
    _STAGE_T(LeafNode3)::insert_size(key, value)
  );
  std::free(p_mem);
}

TEST_F(a_basic_test_t, 2_node_sizes)
{
  run_async([] {
    auto nm = NodeExtentManager::create_dummy(IS_DUMMY_SYNC);
    auto t = make_test_transaction();
    ValueBuilderImpl<UnboundedValue> vb;
    context_t c{*nm, vb, *t};
    std::array<std::pair<NodeImplURef, NodeExtentMutable>, 16> nodes = {
      INTR_WITH_PARAM(InternalNode0::allocate, c, false, 1u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(InternalNode1::allocate, c, false, 1u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(InternalNode2::allocate, c, false, 1u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(InternalNode3::allocate, c, false, 1u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(InternalNode0::allocate, c, true, 1u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(InternalNode1::allocate, c, true, 1u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(InternalNode2::allocate, c, true, 1u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(InternalNode3::allocate, c, true, 1u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(LeafNode0::allocate, c, false, 0u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(LeafNode1::allocate, c, false, 0u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(LeafNode2::allocate, c, false, 0u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(LeafNode3::allocate, c, false, 0u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(LeafNode0::allocate, c, true, 0u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(LeafNode1::allocate, c, true, 0u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(LeafNode2::allocate, c, true, 0u).unsafe_get0().make_pair(),
      INTR_WITH_PARAM(LeafNode3::allocate, c, true, 0u).unsafe_get0().make_pair()
    };
    std::ostringstream oss;
    oss << "\nallocated nodes:";
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
      oss << "\n  ";
      auto& ref_node = iter->first;
      ref_node->dump_brief(oss);
    }
    logger().info("{}", oss.str());
  });
}

struct b_dummy_tree_test_t : public seastar_test_suite_t {
  TransactionRef ref_t;
  std::unique_ptr<UnboundedBtree> tree;

  b_dummy_tree_test_t() = default;

  seastar::future<> set_up_fut() override final {
    ref_t = make_test_transaction();
    tree.reset(
      new UnboundedBtree(NodeExtentManager::create_dummy(IS_DUMMY_SYNC))
    );
    return INTR(tree->mkfs, *ref_t).handle_error(
      crimson::ct_error::all_same_way([] {
        ASSERT_FALSE("Unable to mkfs");
      })
    );
  }

  seastar::future<> tear_down_fut() final {
    ref_t.reset();
    tree.reset();
    return seastar::now();
  }
};

TEST_F(b_dummy_tree_test_t, 3_random_insert_erase_leaf_node)
{
  run_async([this] {
    logger().info("\n---------------------------------------------"
                  "\nrandomized leaf node insert:\n");
    auto key_s = ghobject_t();
    auto key_e = ghobject_t::get_max();
    ASSERT_TRUE(INTR_R(tree->find, *ref_t, key_s).unsafe_get0().is_end());
    ASSERT_TRUE(INTR(tree->begin, *ref_t).unsafe_get0().is_end());
    ASSERT_TRUE(INTR(tree->last, *ref_t).unsafe_get0().is_end());

    std::map<ghobject_t,
             std::tuple<test_item_t, UnboundedBtree::Cursor>> insert_history;

    auto f_validate_insert_new = [this, &insert_history] (
        const ghobject_t& key, const test_item_t& value) {
      auto conf = UnboundedBtree::tree_value_config_t{value.get_payload_size()};
      auto [cursor, success] = INTR_R(tree->insert,
          *ref_t, key, conf).unsafe_get0();
      initialize_cursor_from_item(*ref_t, key, value, cursor, success);
      insert_history.emplace(key, std::make_tuple(value, cursor));
      auto cursor_ = INTR_R(tree->find, *ref_t, key).unsafe_get0();
      ceph_assert(cursor_ != tree->end());
      ceph_assert(cursor_.value() == cursor.value());
      validate_cursor_from_item(key, value, cursor_);
      return cursor.value();
    };

    auto f_validate_erase = [this, &insert_history] (const ghobject_t& key) {
      auto cursor_erase = INTR_R(tree->find, *ref_t, key).unsafe_get0();
      auto cursor_next = INTR(cursor_erase.get_next, *ref_t).unsafe_get0();
      auto cursor_ret = INTR_R(tree->erase, *ref_t, cursor_erase).unsafe_get0();
      ceph_assert(cursor_erase.is_end());
      ceph_assert(cursor_ret == cursor_next);
      auto cursor_lb = INTR_R(tree->lower_bound, *ref_t, key).unsafe_get0();
      ceph_assert(cursor_lb == cursor_next);
      auto it = insert_history.find(key);
      ceph_assert(std::get<1>(it->second).is_end());
      insert_history.erase(it);
    };

    auto f_insert_erase_insert = [&f_validate_insert_new, &f_validate_erase] (
        const ghobject_t& key, const test_item_t& value) {
      f_validate_insert_new(key, value);
      f_validate_erase(key);
      return f_validate_insert_new(key, value);
    };

    auto values = Values<test_item_t>(15);

    // insert key1, value1 at STAGE_LEFT
    auto key1 = make_ghobj(3, 3, 3, "ns3", "oid3", 3, 3);
    auto value1 = values.pick();
    auto test_value1 = f_insert_erase_insert(key1, value1);

    // validate lookup
    {
      auto cursor1_s = INTR_R(tree->lower_bound, *ref_t, key_s).unsafe_get0();
      ASSERT_EQ(cursor1_s.get_ghobj(), key1);
      ASSERT_EQ(cursor1_s.value(), test_value1);
      auto cursor1_e = INTR_R(tree->lower_bound, *ref_t, key_e).unsafe_get0();
      ASSERT_TRUE(cursor1_e.is_end());
    }

    // insert the same key1 with a different value
    {
      auto value1_dup = values.pick();
      auto conf = UnboundedBtree::tree_value_config_t{value1_dup.get_payload_size()};
      auto [cursor1_dup, ret1_dup] = INTR_R(tree->insert,
          *ref_t, key1, conf).unsafe_get0();
      ASSERT_FALSE(ret1_dup);
      validate_cursor_from_item(key1, value1, cursor1_dup);
    }

    // insert key2, value2 to key1's left at STAGE_LEFT
    // insert node front at STAGE_LEFT
    auto key2 = make_ghobj(2, 2, 2, "ns3", "oid3", 3, 3);
    auto value2 = values.pick();
    f_insert_erase_insert(key2, value2);

    // insert key3, value3 to key1's right at STAGE_LEFT
    // insert node last at STAGE_LEFT
    auto key3 = make_ghobj(4, 4, 4, "ns3", "oid3", 3, 3);
    auto value3 = values.pick();
    f_insert_erase_insert(key3, value3);

    // insert key4, value4 to key1's left at STAGE_STRING (collision)
    auto key4 = make_ghobj(3, 3, 3, "ns2", "oid2", 3, 3);
    auto value4 = values.pick();
    f_insert_erase_insert(key4, value4);

    // insert key5, value5 to key1's right at STAGE_STRING (collision)
    auto key5 = make_ghobj(3, 3, 3, "ns4", "oid4", 3, 3);
    auto value5 = values.pick();
    f_insert_erase_insert(key5, value5);

    // insert key6, value6 to key1's left at STAGE_RIGHT
    auto key6 = make_ghobj(3, 3, 3, "ns3", "oid3", 2, 2);
    auto value6 = values.pick();
    f_insert_erase_insert(key6, value6);

    // insert key7, value7 to key1's right at STAGE_RIGHT
    auto key7 = make_ghobj(3, 3, 3, "ns3", "oid3", 4, 4);
    auto value7 = values.pick();
    f_insert_erase_insert(key7, value7);

    // insert node front at STAGE_RIGHT
    auto key8 = make_ghobj(2, 2, 2, "ns3", "oid3", 2, 2);
    auto value8 = values.pick();
    f_insert_erase_insert(key8, value8);

    // insert node front at STAGE_STRING (collision)
    auto key9 = make_ghobj(2, 2, 2, "ns2", "oid2", 3, 3);
    auto value9 = values.pick();
    f_insert_erase_insert(key9, value9);

    // insert node last at STAGE_RIGHT
    auto key10 = make_ghobj(4, 4, 4, "ns3", "oid3", 4, 4);
    auto value10 = values.pick();
    f_insert_erase_insert(key10, value10);

    // insert node last at STAGE_STRING (collision)
    auto key11 = make_ghobj(4, 4, 4, "ns4", "oid4", 3, 3);
    auto value11 = values.pick();
    f_insert_erase_insert(key11, value11);

    // insert key, value randomly until a perfect 3-ary tree is formed
    std::vector<std::pair<ghobject_t, test_item_t>> kvs{
      {make_ghobj(2, 2, 2, "ns2", "oid2", 2, 2), values.pick()},
      {make_ghobj(2, 2, 2, "ns2", "oid2", 4, 4), values.pick()},
      {make_ghobj(2, 2, 2, "ns3", "oid3", 4, 4), values.pick()},
      {make_ghobj(2, 2, 2, "ns4", "oid4", 2, 2), values.pick()},
      {make_ghobj(2, 2, 2, "ns4", "oid4", 3, 3), values.pick()},
      {make_ghobj(2, 2, 2, "ns4", "oid4", 4, 4), values.pick()},
      {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2), values.pick()},
      {make_ghobj(3, 3, 3, "ns2", "oid2", 4, 4), values.pick()},
      {make_ghobj(3, 3, 3, "ns4", "oid4", 2, 2), values.pick()},
      {make_ghobj(3, 3, 3, "ns4", "oid4", 4, 4), values.pick()},
      {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2), values.pick()},
      {make_ghobj(4, 4, 4, "ns2", "oid2", 3, 3), values.pick()},
      {make_ghobj(4, 4, 4, "ns2", "oid2", 4, 4), values.pick()},
      {make_ghobj(4, 4, 4, "ns3", "oid3", 2, 2), values.pick()},
      {make_ghobj(4, 4, 4, "ns4", "oid4", 2, 2), values.pick()},
      {make_ghobj(4, 4, 4, "ns4", "oid4", 4, 4), values.pick()}};
    auto [smallest_key, smallest_value] = kvs[0];
    auto [largest_key, largest_value] = kvs[kvs.size() - 1];
    std::shuffle(kvs.begin(), kvs.end(), std::default_random_engine{});
    std::for_each(kvs.begin(), kvs.end(), [&f_insert_erase_insert] (auto& kv) {
      f_insert_erase_insert(kv.first, kv.second);
    });
    ASSERT_EQ(INTR(tree->height, *ref_t).unsafe_get0(), 1);
    ASSERT_FALSE(tree->test_is_clean());

    for (auto& [k, val] : insert_history) {
      auto& [v, c] = val;
      // validate values in tree keep intact
      auto cursor = with_trans_intr(*ref_t, [this, &k=k](auto& tr) {
        return tree->find(tr, k);
      }).unsafe_get0();
      EXPECT_NE(cursor, tree->end());
      validate_cursor_from_item(k, v, cursor);
      // validate values in cursors keep intact
      validate_cursor_from_item(k, v, c);
    }
    {
      auto cursor = INTR_R(tree->lower_bound, *ref_t, key_s).unsafe_get0();
      validate_cursor_from_item(smallest_key, smallest_value, cursor);
    }
    {
      auto cursor = INTR(tree->begin, *ref_t).unsafe_get0();
      validate_cursor_from_item(smallest_key, smallest_value, cursor);
    }
    {
      auto cursor = INTR(tree->last, *ref_t).unsafe_get0();
      validate_cursor_from_item(largest_key, largest_value, cursor);
    }

    // validate range query
    {
      kvs.clear();
      for (auto& [k, val] : insert_history) {
        auto& [v, c] = val;
        kvs.emplace_back(k, v);
      }
      insert_history.clear();
      std::sort(kvs.begin(), kvs.end(), [](auto& l, auto& r) {
        return l.first < r.first;
      });
      auto cursor = INTR(tree->begin, *ref_t).unsafe_get0();
      for (auto& [k, v] : kvs) {
        ASSERT_FALSE(cursor.is_end());
        validate_cursor_from_item(k, v, cursor);
        cursor = INTR(cursor.get_next, *ref_t).unsafe_get0();
      }
      ASSERT_TRUE(cursor.is_end());
    }

    std::ostringstream oss;
    tree->dump(*ref_t, oss);
    logger().info("\n{}\n", oss.str());

    // randomized erase until empty
    std::shuffle(kvs.begin(), kvs.end(), std::default_random_engine{});
    for (auto& [k, v] : kvs) {
      auto e_size = with_trans_intr(*ref_t, [this, &k=k](auto& tr) {
        return tree->erase(tr, k);
      }).unsafe_get0();
      ASSERT_EQ(e_size, 1);
    }
    auto cursor = INTR(tree->begin, *ref_t).unsafe_get0();
    ASSERT_TRUE(cursor.is_end());
    ASSERT_EQ(INTR(tree->height, *ref_t).unsafe_get0(), 1);
  });
}

static std::set<ghobject_t> build_key_set(
    std::pair<unsigned, unsigned> range_2,
    std::pair<unsigned, unsigned> range_1,
    std::pair<unsigned, unsigned> range_0,
    std::string padding = "",
    bool is_internal = false) {
  ceph_assert(range_1.second <= 10);
  std::set<ghobject_t> ret;
  ghobject_t key;
  for (unsigned i = range_2.first; i < range_2.second; ++i) {
    for (unsigned j = range_1.first; j < range_1.second; ++j) {
      for (unsigned k = range_0.first; k < range_0.second; ++k) {
        std::ostringstream os_ns;
        os_ns << "ns" << j;
        std::ostringstream os_oid;
        os_oid << "oid" << j << padding;
        key = make_ghobj(i, i, i, os_ns.str(), os_oid.str(), k, k);
        ret.insert(key);
      }
    }
  }
  if (is_internal) {
    ret.insert(make_ghobj(9, 9, 9, "ns~last", "oid~last", 9, 9));
  }
  return ret;
}

class TestTree {
 public:
  TestTree()
    : moved_nm{NodeExtentManager::create_dummy(IS_DUMMY_SYNC)},
      ref_t{make_test_transaction()},
      t{*ref_t},
      c{*moved_nm, vb, t},
      tree{std::move(moved_nm)},
      values{0} {}

  seastar::future<> build_tree(
      std::pair<unsigned, unsigned> range_2,
      std::pair<unsigned, unsigned> range_1,
      std::pair<unsigned, unsigned> range_0,
      size_t value_size) {
    return seastar::async([this, range_2, range_1, range_0, value_size] {
      INTR(tree.mkfs, t).unsafe_get0();
      //logger().info("\n---------------------------------------------"
      //              "\nbefore leaf node split:\n");
      auto keys = build_key_set(range_2, range_1, range_0);
      for (auto& key : keys) {
        auto value = values.create(value_size);
        insert_tree(key, value).get0();
      }
      ASSERT_EQ(INTR(tree.height, t).unsafe_get0(), 1);
      ASSERT_FALSE(tree.test_is_clean());
      //std::ostringstream oss;
      //tree.dump(t, oss);
      //logger().info("\n{}\n", oss.str());
    });
  }

  seastar::future<> build_tree(
      const std::vector<ghobject_t>& keys, const std::vector<test_item_t>& values) {
    return seastar::async([this, keys, values] {
      INTR(tree.mkfs, t).unsafe_get0();
      //logger().info("\n---------------------------------------------"
      //              "\nbefore leaf node split:\n");
      ASSERT_EQ(keys.size(), values.size());
      auto key_iter = keys.begin();
      auto value_iter = values.begin();
      while (key_iter != keys.end()) {
        insert_tree(*key_iter, *value_iter).get0();
        ++key_iter;
        ++value_iter;
      }
      ASSERT_EQ(INTR(tree.height, t).unsafe_get0(), 1);
      ASSERT_FALSE(tree.test_is_clean());
      //std::ostringstream oss;
      //tree.dump(t, oss);
      //logger().info("\n{}\n", oss.str());
    });
  }

  seastar::future<> split_merge(
      const ghobject_t& key,
      const test_item_t& value,
      const split_expectation_t& expected,
      std::optional<ghobject_t> next_key) {
    return seastar::async([this, key, value, expected, next_key] {
      // clone
      auto ref_dummy = NodeExtentManager::create_dummy(IS_DUMMY_SYNC);
      auto p_dummy = static_cast<DummyManager*>(ref_dummy.get());
      UnboundedBtree tree_clone(std::move(ref_dummy));
      auto ref_t_clone = make_test_transaction();
      Transaction& t_clone = *ref_t_clone;
      INTR_R(tree_clone.test_clone_from, t_clone, t, tree).unsafe_get0();

      // insert and split
      logger().info("\n\nINSERT-SPLIT {}:", key_hobj_t(key));
      auto conf = UnboundedBtree::tree_value_config_t{value.get_payload_size()};
      auto [cursor, success] = INTR_R(tree_clone.insert,
          t_clone, key, conf).unsafe_get0();
      initialize_cursor_from_item(t, key, value, cursor, success);

      {
        std::ostringstream oss;
        tree_clone.dump(t_clone, oss);
        logger().info("dump new root:\n{}", oss.str());
      }
      EXPECT_EQ(INTR(tree_clone.height, t_clone).unsafe_get0(), 2);

      for (auto& [k, val] : insert_history) {
        auto& [v, c] = val;
        auto result = with_trans_intr(t_clone, [&tree_clone, &k=k] (auto& tr) {
          return tree_clone.find(tr, k);
        }).unsafe_get0();
        EXPECT_NE(result, tree_clone.end());
        validate_cursor_from_item(k, v, result);
      }
      auto result = INTR_R(tree_clone.find, t_clone, key).unsafe_get0();
      EXPECT_NE(result, tree_clone.end());
      validate_cursor_from_item(key, value, result);
      EXPECT_TRUE(last_split.match(expected));
      EXPECT_EQ(p_dummy->size(), 3);

      // erase and merge
      logger().info("\n\nERASE-MERGE {}:", key_hobj_t(key));
      auto nxt_cursor = with_trans_intr(t_clone, [&cursor=cursor](auto& tr) {
        return cursor.erase<true>(tr);
      }).unsafe_get0();

      {
        // track root again to dump
        auto begin = INTR(tree_clone.begin, t_clone).unsafe_get0();
        std::ignore = begin;
        std::ostringstream oss;
        tree_clone.dump(t_clone, oss);
        logger().info("dump root:\n{}", oss.str());
      }

      if (next_key.has_value()) {
        auto found = insert_history.find(*next_key);
        ceph_assert(found != insert_history.end());
        validate_cursor_from_item(
            *next_key, std::get<0>(found->second), nxt_cursor);
      } else {
        EXPECT_TRUE(nxt_cursor.is_end());
      }

      for (auto& [k, val] : insert_history) {
        auto& [v, c] = val;
        auto result = with_trans_intr(t_clone, [&tree_clone, &k=k](auto& tr) {
          return tree_clone.find(tr, k);
        }).unsafe_get0();
        EXPECT_NE(result, tree_clone.end());
        validate_cursor_from_item(k, v, result);
      }
      EXPECT_EQ(INTR(tree_clone.height, t_clone).unsafe_get0(), 1);
      EXPECT_EQ(p_dummy->size(), 1);
    });
  }

  test_item_t create_value(size_t size) {
    return values.create(size);
  }

 private:
  seastar::future<> insert_tree(const ghobject_t& key, const test_item_t& value) {
    return seastar::async([this, &key, &value] {
      auto conf = UnboundedBtree::tree_value_config_t{value.get_payload_size()};
      auto [cursor, success] = INTR_R(tree.insert,
          t, key, conf).unsafe_get0();
      initialize_cursor_from_item(t, key, value, cursor, success);
      insert_history.emplace(key, std::make_tuple(value, cursor));
    });
  }

  NodeExtentManagerURef moved_nm;
  TransactionRef ref_t;
  Transaction& t;
  ValueBuilderImpl<UnboundedValue> vb;
  context_t c;
  UnboundedBtree tree;
  Values<test_item_t> values;
  std::map<ghobject_t,
           std::tuple<test_item_t, UnboundedBtree::Cursor>> insert_history;
};

struct c_dummy_test_t : public seastar_test_suite_t {};

TEST_F(c_dummy_test_t, 4_split_merge_leaf_node)
{
  run_async([] {
    {
      TestTree test;
      test.build_tree({2, 5}, {2, 5}, {2, 5}, 120).get0();

      auto value = test.create_value(1144);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to left front at stage 2, 1, 0\n");
      test.split_merge(make_ghobj(1, 1, 1, "ns3", "oid3", 3, 3), value,
                       {2u, 2u, true, InsertType::BEGIN},
                       {make_ghobj(2, 2, 2, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(2, 2, 2, "ns1", "oid1", 3, 3), value,
                       {2u, 1u, true, InsertType::BEGIN},
                       {make_ghobj(2, 2, 2, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(2, 2, 2, "ns2", "oid2", 1, 1), value,
                       {2u, 0u, true, InsertType::BEGIN},
                       {make_ghobj(2, 2, 2, "ns2", "oid2", 2, 2)}).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to left back at stage 0, 1, 2, 1, 0\n");
      test.split_merge(make_ghobj(2, 2, 2, "ns4", "oid4", 5, 5), value,
                       {2u, 0u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(2, 2, 2, "ns5", "oid5", 3, 3), value,
                       {2u, 1u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(2, 3, 3, "ns3", "oid3", 3, 3), value,
                       {2u, 2u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns1", "oid1", 3, 3), value,
                       {2u, 1u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns2", "oid2", 1, 1), value,
                       {2u, 0u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();

      auto value0 = test.create_value(1416);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right front at stage 0, 1, 2, 1, 0\n");
      test.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4", 5, 5), value0,
                       {2u, 0u, false, InsertType::BEGIN},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), value0,
                       {2u, 1u, false, InsertType::BEGIN},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 4, 4, "ns3", "oid3", 3, 3), value0,
                       {2u, 2u, false, InsertType::BEGIN},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(4, 4, 4, "ns1", "oid1", 3, 3), value0,
                       {2u, 1u, false, InsertType::BEGIN},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(4, 4, 4, "ns2", "oid2", 1, 1), value0,
                       {2u, 0u, false, InsertType::BEGIN},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right back at stage 0, 1, 2\n");
      test.split_merge(make_ghobj(4, 4, 4, "ns4", "oid4", 5, 5), value0,
                       {2u, 0u, false, InsertType::LAST},
                       std::nullopt).get0();
      test.split_merge(make_ghobj(4, 4, 4, "ns5", "oid5", 3, 3), value0,
                       {2u, 1u, false, InsertType::LAST},
                       std::nullopt).get0();
      test.split_merge(make_ghobj(5, 5, 5, "ns3", "oid3", 3, 3), value0,
                       {2u, 2u, false, InsertType::LAST},
                       std::nullopt).get0();

      auto value1 = test.create_value(316);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to left middle at stage 0, 1, 2, 1, 0\n");
      test.split_merge(make_ghobj(2, 2, 2, "ns4", "oid4", 5, 5), value1,
                       {1u, 0u, true, InsertType::MID},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(2, 2, 2, "ns5", "oid5", 3, 3), value1,
                       {1u, 1u, true, InsertType::MID},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(2, 2, 3, "ns3", "oid3", 3, 3), value1,
                       {1u, 2u, true, InsertType::MID},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns1", "oid1", 3, 3), value1,
                       {1u, 1u, true, InsertType::MID},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns2", "oid2", 1, 1), value1,
                       {1u, 0u, true, InsertType::MID},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2)}).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to left back at stage 0, 1, 0\n");
      test.split_merge(make_ghobj(3, 3, 3, "ns2", "oid2", 5, 5), value1,
                       {1u, 0u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns3", "oid3", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns2", "oid3", 3, 3), value1,
                       {1u, 1u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns3", "oid3", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns3", "oid3", 1, 1), value1,
                       {1u, 0u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns3", "oid3", 2, 2)}).get0();

      auto value2 = test.create_value(452);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to right front at stage 0, 1, 0\n");
      test.split_merge(make_ghobj(3, 3, 3, "ns3", "oid3", 5, 5), value2,
                       {1u, 0u, false, InsertType::BEGIN},
                       {make_ghobj(3, 3, 3, "ns4", "oid4", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns3", "oid4", 3, 3), value2,
                       {1u, 1u, false, InsertType::BEGIN},
                       {make_ghobj(3, 3, 3, "ns4", "oid4", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4", 1, 1), value2,
                       {1u, 0u, false, InsertType::BEGIN},
                       {make_ghobj(3, 3, 3, "ns4", "oid4", 2, 2)}).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to right middle at stage 0, 1, 2, 1, 0\n");
      test.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4", 5, 5), value2,
                       {1u, 0u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), value2,
                       {1u, 1u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 4, "ns3", "oid3", 3, 3), value2,
                       {1u, 2u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(4, 4, 4, "ns1", "oid1", 3, 3), value2,
                       {1u, 1u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(4, 4, 4, "ns2", "oid2", 1, 1), value2,
                       {1u, 0u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();

      auto value3 = test.create_value(834);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to right middle at stage 0, 1, 2, 1, 0\n");
      test.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4", 5, 5), value3,
                       {0u, 0u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), value3,
                       {0u, 1u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(3, 3, 4, "ns3", "oid3", 3, 3), value3,
                       {0u, 2u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(4, 4, 4, "ns1", "oid1", 3, 3), value3,
                       {0u, 1u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();
      test.split_merge(make_ghobj(4, 4, 4, "ns2", "oid2", 1, 1), value3,
                       {0u, 0u, false, InsertType::MID},
                       {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2)}).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to right front at stage 0\n");
      test.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4", 2, 3), value3,
                       {0u, 0u, false, InsertType::BEGIN},
                       {make_ghobj(3, 3, 3, "ns4", "oid4", 3, 3)}).get0();

      auto value4 = test.create_value(572);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to left back at stage 0\n");
      test.split_merge(make_ghobj(3, 3, 3, "ns2", "oid2", 3, 4), value4,
                       {0u, 0u, true, InsertType::LAST},
                       {make_ghobj(3, 3, 3, "ns2", "oid2", 4, 4)}).get0();
    }

    {
      TestTree test;
      test.build_tree({2, 4}, {2, 4}, {2, 4}, 232).get0();
      auto value = test.create_value(1996);
      logger().info("\n---------------------------------------------"
                    "\nsplit at [0, 0, 0]; insert to left front at stage 2, 1, 0\n");
      test.split_merge(make_ghobj(1, 1, 1, "ns3", "oid3", 3, 3), value,
                       {2u, 2u, true, InsertType::BEGIN},
                       {make_ghobj(2, 2, 2, "ns2", "oid2", 2, 2)}).get0();
      EXPECT_TRUE(last_split.match_split_pos({0, {0, {0}}}));
      test.split_merge(make_ghobj(2, 2, 2, "ns1", "oid1", 3, 3), value,
                       {2u, 1u, true, InsertType::BEGIN},
                       {make_ghobj(2, 2, 2, "ns2", "oid2", 2, 2)}).get0();
      EXPECT_TRUE(last_split.match_split_pos({0, {0, {0}}}));
      test.split_merge(make_ghobj(2, 2, 2, "ns2", "oid2", 1, 1), value,
                       {2u, 0u, true, InsertType::BEGIN},
                       {make_ghobj(2, 2, 2, "ns2", "oid2", 2, 2)}).get0();
      EXPECT_TRUE(last_split.match_split_pos({0, {0, {0}}}));
    }

    {
      TestTree test;
      std::vector<ghobject_t> keys = {
        make_ghobj(2, 2, 2, "ns3", "oid3", 3, 3),
        make_ghobj(3, 3, 3, "ns3", "oid3", 3, 3)};
      std::vector<test_item_t> values = {
        test.create_value(1360),
        test.create_value(1632)};
      test.build_tree(keys, values).get0();
      auto value = test.create_value(1640);
      logger().info("\n---------------------------------------------"
                    "\nsplit at [END, END, END]; insert to right at stage 0, 1, 2\n");
      test.split_merge(make_ghobj(3, 3, 3, "ns3", "oid3", 4, 4), value,
                       {0u, 0u, false, InsertType::BEGIN},
                       std::nullopt).get0();
      EXPECT_TRUE(last_split.match_split_pos({1, {0, {1}}}));
      test.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4", 3, 3), value,
                       {1u, 1u, false, InsertType::BEGIN},
                       std::nullopt).get0();
      EXPECT_TRUE(last_split.match_split_pos({1, {1, {0}}}));
      test.split_merge(make_ghobj(4, 4, 4, "ns3", "oid3", 3, 3), value,
                       {2u, 2u, false, InsertType::BEGIN},
                       std::nullopt).get0();
      EXPECT_TRUE(last_split.match_split_pos({2, {0, {0}}}));
    }
  });
}

namespace crimson::os::seastore::onode {

class DummyChildPool {
  class DummyChildImpl final : public NodeImpl {
   public:
    using URef = std::unique_ptr<DummyChildImpl>;
    DummyChildImpl(const std::set<ghobject_t>& keys, bool is_level_tail, laddr_t laddr)
        : keys{keys}, _is_level_tail{is_level_tail}, _laddr{laddr} {
      std::tie(key_view, p_mem_key_view) = build_key_view(*keys.crbegin());
      build_name();
    }
    ~DummyChildImpl() override {
      std::free(p_mem_key_view);
    }

    const std::set<ghobject_t>& get_keys() const { return keys; }

    void reset(const std::set<ghobject_t>& _keys, bool level_tail) {
      keys = _keys;
      _is_level_tail = level_tail;
      std::free(p_mem_key_view);
      std::tie(key_view, p_mem_key_view) = build_key_view(*keys.crbegin());
      build_name();
    }

   public:
    laddr_t laddr() const override { return _laddr; }
    bool is_level_tail() const override { return _is_level_tail; }
    std::optional<key_view_t> get_pivot_index() const override { return {key_view}; }
    bool is_extent_retired() const override { return _is_extent_retired; }
    const std::string& get_name() const override { return name; }
    search_position_t make_tail() override {
      _is_level_tail = true;
      build_name();
      return search_position_t::end();
    }
    eagain_ifuture<> retire_extent(context_t) override {
      assert(!_is_extent_retired);
      _is_extent_retired = true;
      return eagain_iertr::now();
    }

   protected:
    node_type_t node_type() const override { return node_type_t::LEAF; }
    field_type_t field_type() const override { return field_type_t::N0; }
    const char* read() const override {
      ceph_abort("impossible path"); }
    extent_len_t get_node_size() const override {
      ceph_abort("impossible path"); }
    nextent_state_t get_extent_state() const override {
      ceph_abort("impossible path"); }
    level_t level() const override { return 0u; }
    void prepare_mutate(context_t) override {
      ceph_abort("impossible path"); }
    void validate_non_empty() const override {
      ceph_abort("impossible path"); }
    bool is_keys_empty() const override {
      ceph_abort("impossible path"); }
    bool has_single_value() const override {
      ceph_abort("impossible path"); }
    node_offset_t free_size() const override {
      ceph_abort("impossible path"); }
    extent_len_t total_size() const override {
      ceph_abort("impossible path"); }
    bool is_size_underflow() const override {
      ceph_abort("impossible path"); }
    std::tuple<match_stage_t, search_position_t> erase(const search_position_t&) override {
      ceph_abort("impossible path"); }
    std::tuple<match_stage_t, std::size_t> evaluate_merge(NodeImpl&) override {
      ceph_abort("impossible path"); }
    search_position_t merge(NodeExtentMutable&, NodeImpl&, match_stage_t, extent_len_t) override {
      ceph_abort("impossible path"); }
    eagain_ifuture<NodeExtentMutable> rebuild_extent(context_t) override {
      ceph_abort("impossible path"); }
    node_stats_t get_stats() const override {
      ceph_abort("impossible path"); }
    std::ostream& dump(std::ostream&) const override {
      ceph_abort("impossible path"); }
    std::ostream& dump_brief(std::ostream&) const override {
      ceph_abort("impossible path"); }
    void validate_layout() const override {
      ceph_abort("impossible path"); }
    void test_copy_to(NodeExtentMutable&) const override {
      ceph_abort("impossible path"); }
    void test_set_tail(NodeExtentMutable&) override {
      ceph_abort("impossible path"); }

   private:
    void build_name() {
      std::ostringstream sos;
      sos << "DummyNode"
          << "@0x" << std::hex << laddr() << std::dec
          << "Lv" << (unsigned)level()
          << (is_level_tail() ? "$" : "")
          << "(" << key_view << ")";
      name = sos.str();
    }

    std::set<ghobject_t> keys;
    bool _is_level_tail;
    laddr_t _laddr;
    std::string name;
    bool _is_extent_retired = false;

    key_view_t key_view;
    void* p_mem_key_view;
  };

  class DummyChild final : public Node {
   public:
    ~DummyChild() override = default;

    key_view_t get_pivot_key() const { return *impl->get_pivot_index(); }

    eagain_ifuture<> populate_split(
        context_t c, std::set<Ref<DummyChild>>& splitable_nodes) {
      ceph_assert(can_split());
      ceph_assert(splitable_nodes.find(this) != splitable_nodes.end());

      size_t index;
      const auto& keys = impl->get_keys();
      if (keys.size() == 2) {
        index = 1;
      } else {
        index = rd() % (keys.size() - 2) + 1;
      }
      auto iter = keys.begin();
      std::advance(iter, index);

      std::set<ghobject_t> left_keys(keys.begin(), iter);
      std::set<ghobject_t> right_keys(iter, keys.end());
      bool right_is_tail = impl->is_level_tail();
      impl->reset(left_keys, false);
      auto right_child = DummyChild::create_new(right_keys, right_is_tail, pool);
      if (!can_split()) {
        splitable_nodes.erase(this);
      }
      if (right_child->can_split()) {
        splitable_nodes.insert(right_child);
      }
      Ref<Node> this_ref = this;
      return apply_split_to_parent(
            c, std::move(this_ref), std::move(right_child), false);
    }

    eagain_ifuture<> insert_and_split(
        context_t c, const ghobject_t& insert_key,
        std::set<Ref<DummyChild>>& splitable_nodes) {
      const auto& keys = impl->get_keys();
      ceph_assert(keys.size() == 1);
      auto& key = *keys.begin();
      ceph_assert(insert_key < key);

      std::set<ghobject_t> new_keys;
      new_keys.insert(insert_key);
      new_keys.insert(key);
      impl->reset(new_keys, impl->is_level_tail());

      splitable_nodes.clear();
      splitable_nodes.insert(this);
      auto fut = populate_split(c, splitable_nodes);
      ceph_assert(splitable_nodes.size() == 0);
      return fut;
    }

    eagain_ifuture<> merge(context_t c, Ref<DummyChild>&& this_ref) {
      return parent_info().ptr->get_child_peers(c, parent_info().position
      ).si_then([c, this_ref = std::move(this_ref), this] (auto lr_nodes) mutable {
        auto& [lnode, rnode] = lr_nodes;
        if (rnode) {
          lnode.reset();
          Ref<DummyChild> r_dummy(static_cast<DummyChild*>(rnode.get()));
          rnode.reset();
          pool.untrack_node(r_dummy);
          assert(r_dummy->use_count() == 1);
          return do_merge(c, std::move(this_ref), std::move(r_dummy), true);
        } else {
          ceph_assert(lnode);
          Ref<DummyChild> l_dummy(static_cast<DummyChild*>(lnode.get()));
          pool.untrack_node(this_ref);
          assert(this_ref->use_count() == 1);
          return do_merge(c, std::move(l_dummy), std::move(this_ref), false);
        }
      });
    }

    eagain_ifuture<> fix_key(context_t c, const ghobject_t& new_key) {
      const auto& keys = impl->get_keys();
      ceph_assert(keys.size() == 1);
      assert(impl->is_level_tail() == false);

      std::set<ghobject_t> new_keys;
      new_keys.insert(new_key);
      impl->reset(new_keys, impl->is_level_tail());
      Ref<Node> this_ref = this;
      return fix_parent_index<true>(c, std::move(this_ref), false);
    }

    bool match_pos(const search_position_t& pos) const {
      ceph_assert(!is_root());
      return pos == parent_info().position;
    }

    static Ref<DummyChild> create(
        const std::set<ghobject_t>& keys, bool is_level_tail,
        laddr_t addr, DummyChildPool& pool) {
      auto ref_impl = std::make_unique<DummyChildImpl>(keys, is_level_tail, addr);
      return new DummyChild(ref_impl.get(), std::move(ref_impl), pool);
    }

    static Ref<DummyChild> create_new(
        const std::set<ghobject_t>& keys, bool is_level_tail, DummyChildPool& pool) {
      static laddr_t seed = 0;
      return create(keys, is_level_tail, seed++, pool);
    }

    static eagain_ifuture<Ref<DummyChild>> create_initial(
        context_t c, const std::set<ghobject_t>& keys,
        DummyChildPool& pool, RootNodeTracker& root_tracker) {
      auto initial = create_new(keys, true, pool);
      return c.nm.get_super(c.t, root_tracker
      ).handle_error_interruptible(
        eagain_iertr::pass_further{},
        crimson::ct_error::assert_all{"Invalid error during create_initial()"}
      ).si_then([c, initial](auto super) {
        initial->make_root_new(c, std::move(super));
        return initial->upgrade_root(c, L_ADDR_MIN).si_then([initial] {
          return initial;
        });
      });
    }

   protected:
    eagain_ifuture<> test_clone_non_root(
        context_t, Ref<InternalNode> new_parent) const override {
      ceph_assert(!is_root());
      auto p_pool_clone = pool.pool_clone_in_progress;
      ceph_assert(p_pool_clone != nullptr);
      auto clone = create(
          impl->get_keys(), impl->is_level_tail(), impl->laddr(), *p_pool_clone);
      clone->as_child(parent_info().position, new_parent);
      return eagain_iertr::now();
    }
    eagain_ifuture<Ref<tree_cursor_t>> lookup_smallest(context_t) override {
      ceph_abort("impossible path"); }
    eagain_ifuture<Ref<tree_cursor_t>> lookup_largest(context_t) override {
      ceph_abort("impossible path"); }
    eagain_ifuture<> test_clone_root(context_t, RootNodeTracker&) const override {
      ceph_abort("impossible path"); }
    eagain_ifuture<search_result_t> lower_bound_tracked(
        context_t, const key_hobj_t&, MatchHistory&) override {
      ceph_abort("impossible path"); }
    eagain_ifuture<> do_get_tree_stats(context_t, tree_stats_t&) override {
      ceph_abort("impossible path"); }
    bool is_tracking() const override { return false; }
    void track_merge(Ref<Node>, match_stage_t, search_position_t&) override {
      ceph_abort("impossible path"); }

   private:
    DummyChild(DummyChildImpl* impl, DummyChildImpl::URef&& ref, DummyChildPool& pool)
      : Node(std::move(ref)), impl{impl}, pool{pool} {
      pool.track_node(this);
    }

    bool can_split() const { return impl->get_keys().size() > 1; }

    static eagain_ifuture<> do_merge(
        context_t c, Ref<DummyChild>&& left, Ref<DummyChild>&& right, bool stole_key) {
      assert(right->use_count() == 1);
      assert(left->impl->get_keys().size() == 1);
      assert(right->impl->get_keys().size() == 1);
      bool left_is_tail = right->impl->is_level_tail();
      const std::set<ghobject_t>* p_keys;
      if (stole_key) {
        p_keys = &right->impl->get_keys();
      } else {
        p_keys = &left->impl->get_keys();
      }
      left->impl->reset(*p_keys, left_is_tail);
      auto left_addr = left->impl->laddr();
      return left->parent_info().ptr->apply_children_merge<true>(
          c, std::move(left), left_addr, std::move(right), !stole_key);
    }

    DummyChildImpl* impl;
    DummyChildPool& pool;
    mutable std::random_device rd;
  };

 public:
  DummyChildPool() = default;
  ~DummyChildPool() { reset(); }

  auto build_tree(const std::set<ghobject_t>& keys) {
    reset();
    // create tree
    auto ref_dummy = NodeExtentManager::create_dummy(IS_DUMMY_SYNC);
    p_dummy = static_cast<DummyManager*>(ref_dummy.get());
    p_btree.emplace(std::move(ref_dummy));
    return with_trans_intr(get_context().t, [this, &keys] (auto &tr) {
      return DummyChild::create_initial(get_context(), keys, *this, *p_btree->root_tracker
      ).si_then([this](auto initial_child) {
        // split
        splitable_nodes.insert(initial_child);
        return trans_intr::repeat([this] ()
          -> eagain_ifuture<seastar::stop_iteration> {
          if (splitable_nodes.empty()) {
            return seastar::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::yes);
          }
          auto index = rd() % splitable_nodes.size();
          auto iter = splitable_nodes.begin();
          std::advance(iter, index);
          Ref<DummyChild> child = *iter;
          return child->populate_split(get_context(), splitable_nodes
          ).si_then([] {
            return seastar::stop_iteration::no;
          });
        });
      }).si_then([this] {
      //std::ostringstream oss;
      //p_btree->dump(t(), oss);
      //logger().info("\n{}\n", oss.str());
        return p_btree->height(t());
      }).si_then([](auto height) {
        ceph_assert(height == 2);
      });
    });
  }

  seastar::future<> split_merge(ghobject_t key, search_position_t pos,
                                const split_expectation_t& expected) {
    return seastar::async([this, key, pos, expected] {
      DummyChildPool pool_clone;
      clone_to(pool_clone);

      // insert and split
      logger().info("\n\nINSERT-SPLIT {} at pos({}):", key_hobj_t(key), pos);
      auto node_to_split = pool_clone.get_node_by_pos(pos);
      with_trans_intr(pool_clone.get_context().t, [&] (auto &t) {
        return node_to_split->insert_and_split(
          pool_clone.get_context(), key, pool_clone.splitable_nodes);
      }).unsafe_get0();
      {
        std::ostringstream oss;
        pool_clone.p_btree->dump(pool_clone.t(), oss);
        logger().info("dump new root:\n{}", oss.str());
      }
      auto &pt = pool_clone.t();
      EXPECT_EQ(INTR(pool_clone.p_btree->height, pt).unsafe_get0(), 3);
      EXPECT_TRUE(last_split.match(expected));
      EXPECT_EQ(pool_clone.p_dummy->size(), 3);

      // erase and merge
      [[maybe_unused]] auto pivot_key = node_to_split->get_pivot_key();
      logger().info("\n\nERASE-MERGE {}:", node_to_split->get_name());
      assert(pivot_key == key_hobj_t(key));
      with_trans_intr(pool_clone.get_context().t, [&] (auto &t) {
        return node_to_split->merge(
          pool_clone.get_context(), std::move(node_to_split));
      }).unsafe_get0();
      auto &pt2 = pool_clone.t();
      EXPECT_EQ(INTR(pool_clone.p_btree->height ,pt2).unsafe_get0(), 2);
      EXPECT_EQ(pool_clone.p_dummy->size(), 1);
    });
  }

  seastar::future<> fix_index(
      ghobject_t new_key, search_position_t pos, bool expect_split) {
    return seastar::async([this, new_key, pos, expect_split] {
      DummyChildPool pool_clone;
      clone_to(pool_clone);

      // fix
      auto node_to_fix = pool_clone.get_node_by_pos(pos);
      auto old_key = node_to_fix->get_pivot_key().to_ghobj();
      logger().info("\n\nFIX pos({}) from {} to {}, expect_split={}:",
                    pos, node_to_fix->get_name(), key_hobj_t(new_key), expect_split);
      with_trans_intr(pool_clone.get_context().t, [&] (auto &t) {
        return node_to_fix->fix_key(pool_clone.get_context(), new_key);
      }).unsafe_get0();
      if (expect_split) {
        std::ostringstream oss;
        pool_clone.p_btree->dump(pool_clone.t(), oss);
        logger().info("dump new root:\n{}", oss.str());
        auto &pt = pool_clone.t();
        EXPECT_EQ(INTR(pool_clone.p_btree->height, pt).unsafe_get0(), 3);
        EXPECT_EQ(pool_clone.p_dummy->size(), 3);
      } else {
        auto &pt = pool_clone.t();
        EXPECT_EQ(INTR(pool_clone.p_btree->height, pt).unsafe_get0(), 2);
        EXPECT_EQ(pool_clone.p_dummy->size(), 1);
      }

      // fix back
      logger().info("\n\nFIX pos({}) from {} back to {}:",
                    pos, node_to_fix->get_name(), key_hobj_t(old_key));
      with_trans_intr(pool_clone.get_context().t, [&] (auto &t) {
          return node_to_fix->fix_key(pool_clone.get_context(), old_key);
      }).unsafe_get0();
      auto &pt = pool_clone.t();
      EXPECT_EQ(INTR(pool_clone.p_btree->height, pt).unsafe_get0(), 2);
      EXPECT_EQ(pool_clone.p_dummy->size(), 1);
    });
  }

 private:
  void clone_to(DummyChildPool& pool_clone) {
    pool_clone_in_progress = &pool_clone;
    auto ref_dummy = NodeExtentManager::create_dummy(IS_DUMMY_SYNC);
    pool_clone.p_dummy = static_cast<DummyManager*>(ref_dummy.get());
    pool_clone.p_btree.emplace(std::move(ref_dummy));
    auto &pt = pool_clone.t();
    [[maybe_unused]] auto &tr = t();
    INTR_R(pool_clone.p_btree->test_clone_from,
      pt, tr, *p_btree).unsafe_get0();
    pool_clone_in_progress = nullptr;
  }

  void reset() {
    ceph_assert(pool_clone_in_progress == nullptr);
    if (tracked_children.size()) {
      ceph_assert(!p_btree->test_is_clean());
      tracked_children.clear();
      ceph_assert(p_btree->test_is_clean());
      p_dummy = nullptr;
      p_btree.reset();
    } else {
      ceph_assert(!p_btree.has_value());
    }
    splitable_nodes.clear();
  }

  void track_node(Ref<DummyChild> node) {
    ceph_assert(tracked_children.find(node) == tracked_children.end());
    tracked_children.insert(node);
  }

  void untrack_node(Ref<DummyChild> node) {
    auto ret = tracked_children.erase(node);
    ceph_assert(ret == 1);
  }

  Ref<DummyChild> get_node_by_pos(const search_position_t& pos) const {
    auto iter = std::find_if(
        tracked_children.begin(), tracked_children.end(), [&pos](auto& child) {
      return child->match_pos(pos);
    });
    ceph_assert(iter != tracked_children.end());
    return *iter;
  }

  context_t get_context() {
    ceph_assert(p_dummy != nullptr);
    return {*p_dummy, vb, t()};
  }

  Transaction& t() const { return *ref_t; }

  std::set<Ref<DummyChild>> tracked_children;
  std::optional<UnboundedBtree> p_btree;
  DummyManager* p_dummy = nullptr;
  ValueBuilderImpl<UnboundedValue> vb;
  TransactionRef ref_t = make_test_transaction();

  std::random_device rd;
  std::set<Ref<DummyChild>> splitable_nodes;

  DummyChildPool* pool_clone_in_progress = nullptr;
};

}

TEST_F(c_dummy_test_t, 5_split_merge_internal_node)
{
  run_async([] {
    DummyChildPool pool;
    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert:\n");
      auto padding = std::string(250, '_');
      auto keys = build_key_set({2, 6}, {2, 5}, {2, 5}, padding, true);
      keys.erase(make_ghobj(2, 2, 2, "ns2", "oid2" + padding, 2, 2));
      keys.erase(make_ghobj(2, 2, 2, "ns2", "oid2" + padding, 3, 3));
      keys.erase(make_ghobj(2, 2, 2, "ns2", "oid2" + padding, 4, 4));
      keys.erase(make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 2, 2));
      keys.erase(make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 3, 3));
      keys.erase(make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 4, 4));
      auto padding_s = std::string(257, '_');
      keys.insert(make_ghobj(2, 2, 2, "ns2", "oid2" + padding_s, 2, 2));
      keys.insert(make_ghobj(2, 2, 2, "ns2", "oid2" + padding_s, 3, 3));
      keys.insert(make_ghobj(2, 2, 2, "ns2", "oid2" + padding_s, 4, 4));
      auto padding_e = std::string(247, '_');
      keys.insert(make_ghobj(5, 5, 5, "ns4", "oid4" + padding_e, 2, 2));
      keys.insert(make_ghobj(5, 5, 5, "ns4", "oid4" + padding_e, 3, 3));
      keys.insert(make_ghobj(5, 5, 5, "ns4", "oid4" + padding_e, 4, 4));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right front at stage 0, 1, 2, 1, 0\n");
      pool.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4" + padding, 5, 5), {2, {0, {0}}},
                       {2u, 0u, false, InsertType::BEGIN}).get();
      pool.split_merge(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), {2, {0, {0}}},
                       {2u, 1u, false, InsertType::BEGIN}).get();
      pool.split_merge(make_ghobj(3, 4, 4, "ns3", "oid3", 3, 3), {2, {0, {0}}},
                       {2u, 2u, false, InsertType::BEGIN}).get();
      pool.split_merge(make_ghobj(4, 4, 4, "ns1", "oid1", 3, 3), {2, {0, {0}}},
                       {2u, 1u, false, InsertType::BEGIN}).get();
      pool.split_merge(make_ghobj(4, 4, 4, "ns2", "oid2" + padding, 1, 1), {2, {0, {0}}},
                       {2u, 0u, false, InsertType::BEGIN}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right middle at stage 0, 1, 2, 1, 0\n");
      pool.split_merge(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 5, 5), {3, {0, {0}}},
                       {2u, 0u, false, InsertType::MID}).get();
      pool.split_merge(make_ghobj(4, 4, 4, "ns5", "oid5", 3, 3), {3, {0, {0}}},
                       {2u, 1u, false, InsertType::MID}).get();
      pool.split_merge(make_ghobj(4, 4, 5, "ns3", "oid3", 3, 3), {3, {0, {0}}},
                       {2u, 2u, false, InsertType::MID}).get();
      pool.split_merge(make_ghobj(5, 5, 5, "ns1", "oid1", 3, 3), {3, {0, {0}}},
                       {2u, 1u, false, InsertType::MID}).get();
      pool.split_merge(make_ghobj(5, 5, 5, "ns2", "oid2" + padding, 1, 1), {3, {0, {0}}},
                       {2u, 0u, false, InsertType::MID}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right back at stage 0, 1, 2\n");
      pool.split_merge(make_ghobj(5, 5, 5, "ns4", "oid4" + padding_e, 5, 5), search_position_t::end() ,
                      {2u, 0u, false, InsertType::LAST}).get();
      pool.split_merge(make_ghobj(5, 5, 5, "ns5", "oid5", 3, 3), search_position_t::end(),
                       {2u, 1u, false, InsertType::LAST}).get();
      pool.split_merge(make_ghobj(6, 6, 6, "ns3", "oid3", 3, 3), search_position_t::end(),
                       {2u, 2u, false, InsertType::LAST}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to left front at stage 2, 1, 0\n");
      pool.split_merge(make_ghobj(1, 1, 1, "ns3", "oid3", 3, 3), {0, {0, {0}}},
                       {0u, 2u, true, InsertType::BEGIN}).get();
      pool.split_merge(make_ghobj(2, 2, 2, "ns1", "oid1", 3, 3), {0, {0, {0}}},
                       {0u, 1u, true, InsertType::BEGIN}).get();
      pool.split_merge(make_ghobj(2, 2, 2, "ns2", "oid2" + padding_s, 1, 1), {0, {0, {0}}},
                       {0u, 0u, true, InsertType::BEGIN}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to left middle at stage 0, 1, 2, 1, 0\n");
      pool.split_merge(make_ghobj(2, 2, 2, "ns4", "oid4" + padding, 5, 5), {1, {0, {0}}},
                       {0u, 0u, true, InsertType::MID}).get();
      pool.split_merge(make_ghobj(2, 2, 2, "ns5", "oid5", 3, 3), {1, {0, {0}}},
                       {0u, 1u, true, InsertType::MID}).get();
      pool.split_merge(make_ghobj(2, 2, 3, "ns3", "oid3" + std::string(80, '_'), 3, 3), {1, {0, {0}}} ,
                      {0u, 2u, true, InsertType::MID}).get();
      pool.split_merge(make_ghobj(3, 3, 3, "ns1", "oid1", 3, 3), {1, {0, {0}}},
                       {0u, 1u, true, InsertType::MID}).get();
      pool.split_merge(make_ghobj(3, 3, 3, "ns2", "oid2" + padding, 1, 1), {1, {0, {0}}},
                       {0u, 0u, true, InsertType::MID}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to left back at stage 0\n");
      pool.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4" + padding, 3, 4), {1, {2, {2}}},
                       {0u, 0u, true, InsertType::LAST}).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (1):\n");
      auto padding = std::string(244, '_');
      auto keys = build_key_set({2, 6}, {2, 5}, {2, 5}, padding, true);
      keys.insert(make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 5, 5));
      keys.insert(make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 6, 6));
      keys.insert(make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 7, 7));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to left back at stage 0, 1, 2, 1\n");
      pool.split_merge(make_ghobj(3, 3, 3, "ns4", "oid4" + padding, 5, 5), {2, {0, {0}}},
                       {2u, 0u, true, InsertType::LAST}).get();
      pool.split_merge(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), {2, {0, {0}}},
                       {2u, 1u, true, InsertType::LAST}).get();
      pool.split_merge(make_ghobj(3, 4, 4, "n", "o", 3, 3), {2, {0, {0}}},
                       {2u, 2u, true, InsertType::LAST}).get();
      pool.split_merge(make_ghobj(4, 4, 4, "n", "o", 3, 3), {2, {0, {0}}},
                       {2u, 1u, true, InsertType::LAST}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to left middle at stage 2\n");
      pool.split_merge(make_ghobj(2, 3, 3, "n", "o", 3, 3), {1, {0, {0}}},
                       {2u, 2u, true, InsertType::MID}).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (2):\n");
      auto padding = std::string(243, '_');
      auto keys = build_key_set({2, 6}, {2, 5}, {2, 5}, padding, true);
      keys.insert(make_ghobj(4, 4, 4, "n", "o", 3, 3));
      keys.insert(make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 5, 5));
      keys.insert(make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 6, 6));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to left back at stage (0, 1, 2, 1,) 0\n");
      pool.split_merge(make_ghobj(4, 4, 4, "n", "o", 2, 2), {2, {0, {0}}},
                       {2u, 0u, true, InsertType::LAST}).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (3):\n");
      auto padding = std::string(419, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding, true);
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 2, 2));
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 3, 3));
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 4, 4));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to right front at stage 0, 1, 0\n");
      pool.split_merge(make_ghobj(3, 3, 3, "ns2", "oid2" + padding, 5, 5), {1, {1, {0}}},
                       {1u, 0u, false, InsertType::BEGIN}).get();
      pool.split_merge(make_ghobj(3, 3, 3, "ns2", "oid3", 3, 3), {1, {1, {0}}},
                       {1u, 1u, false, InsertType::BEGIN}).get();
      pool.split_merge(make_ghobj(3, 3, 3, "ns3", "oid3" + padding, 1, 1), {1, {1, {0}}},
                       {1u, 0u, false, InsertType::BEGIN}).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (4):\n");
      auto padding = std::string(361, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding, true);
      keys.erase(make_ghobj(2, 2, 2, "ns2", "oid2" + padding, 2, 2));
      keys.erase(make_ghobj(2, 2, 2, "ns2", "oid2" + padding, 3, 3));
      keys.erase(make_ghobj(2, 2, 2, "ns2", "oid2" + padding, 4, 4));
      auto padding_s = std::string(386, '_');
      keys.insert(make_ghobj(2, 2, 2, "ns2", "oid2" + padding_s, 2, 2));
      keys.insert(make_ghobj(2, 2, 2, "ns2", "oid2" + padding_s, 3, 3));
      keys.insert(make_ghobj(2, 2, 2, "ns2", "oid2" + padding_s, 4, 4));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to left back at stage 0, 1\n");
      pool.split_merge(make_ghobj(3, 3, 3, "ns2", "oid2" + padding, 5, 5), {1, {1, {0}}},
                       {1u, 0u, true, InsertType::LAST}).get();
      pool.split_merge(make_ghobj(3, 3, 3, "ns2", "oid3", 3, 3), {1, {1, {0}}},
                       {1u, 1u, true, InsertType::LAST}).get();

      logger().info("\n---------------------------------------------"
                    "\nfix end index from stage 0 to 0, 1, 2\n");
      auto padding1 = std::string(400, '_');
      pool.fix_index(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 5, 5),
                     {2, {2, {2}}}, false).get();
      pool.fix_index(make_ghobj(4, 4, 4, "ns5", "oid5" + padding1, 3, 3),
                     {2, {2, {2}}}, true).get();
      pool.fix_index(make_ghobj(5, 5, 5, "ns3", "oid3" + padding1, 3, 3),
                     {2, {2, {2}}}, true).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (5):\n");
      auto padding = std::string(412, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding);
      keys.insert(make_ghobj(3, 3, 3, "ns2", "oid3", 3, 3));
      keys.insert(make_ghobj(4, 4, 4, "ns3", "oid3" + padding, 5, 5));
      keys.insert(make_ghobj(9, 9, 9, "ns~last", "oid~last", 9, 9));
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 2, 2));
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 3, 3));
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 4, 4));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to left back at stage (0, 1,) 0\n");
      pool.split_merge(make_ghobj(3, 3, 3, "ns2", "oid3", 2, 2), {1, {1, {0}}},
                       {1u, 0u, true, InsertType::LAST}).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (6):\n");
      auto padding = std::string(328, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding);
      keys.insert(make_ghobj(5, 5, 5, "ns3", "oid3" + std::string(270, '_'), 3, 3));
      keys.insert(make_ghobj(9, 9, 9, "ns~last", "oid~last", 9, 9));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to right front at stage 0\n");
      pool.split_merge(make_ghobj(3, 3, 3, "ns3", "oid3" + padding, 2, 3), {1, {1, {1}}},
                       {0u, 0u, false, InsertType::BEGIN}).get();

      logger().info("\n---------------------------------------------"
                    "\nfix end index from stage 2 to 0, 1, 2\n");
      auto padding1 = std::string(400, '_');
      pool.fix_index(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 5, 5),
                     {3, {0, {0}}}, false).get();
      pool.fix_index(make_ghobj(4, 4, 4, "ns5", "oid5" + padding1, 3, 3),
                     {3, {0, {0}}}, true).get();
      pool.fix_index(make_ghobj(5, 5, 5, "ns4", "oid4" + padding1, 3, 3),
                     {3, {0, {0}}}, true).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (7):\n");
      auto padding = std::string(323, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding);
      keys.insert(make_ghobj(4, 4, 4, "ns5", "oid5" + padding, 3, 3));
      keys.insert(make_ghobj(9, 9, 9, "ns~last", "oid~last", 9, 9));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nfix end index from stage 1 to 0, 1, 2\n");
      auto padding1 = std::string(400, '_');
      pool.fix_index(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 5, 5),
                     {2, {3, {0}}}, false).get();
      pool.fix_index(make_ghobj(4, 4, 4, "ns6", "oid6" + padding1, 3, 3),
                     {2, {3, {0}}}, true).get();
      pool.fix_index(make_ghobj(5, 5, 5, "ns3", "oid3" + padding1, 3, 3),
                     {2, {3, {0}}}, true).get();
    }

    // Impossible to split at {0, 0, 0}
    // Impossible to split at [END, END, END]
  });
}

struct d_seastore_tm_test_t :
    public seastar_test_suite_t, TMTestState {
  seastar::future<> set_up_fut() override final {
    return tm_setup();
  }
  seastar::future<> tear_down_fut() override final {
    return tm_teardown();
  }
};

TEST_P(d_seastore_tm_test_t, 6_random_tree_insert_erase)
{
  run_async([this] {
    constexpr bool TEST_SEASTORE = true;
    constexpr bool TRACK_CURSORS = true;
    auto kvs = KVPool<test_item_t>::create_raw_range(
        {8, 11,  64, 256, 301, 320},
        {8, 11,  64, 256, 301, 320},
        {8, 16, 128, 512, 576, 640},
        {0, 16}, {0, 10}, {0, 4});
    auto moved_nm = (TEST_SEASTORE ? NodeExtentManager::create_seastore(*tm)
                                   : NodeExtentManager::create_dummy(IS_DUMMY_SYNC));
    auto p_nm = moved_nm.get();
    auto tree = std::make_unique<TreeBuilder<TRACK_CURSORS, BoundedValue>>(
        kvs, std::move(moved_nm));
    {
      auto t = create_mutate_transaction();
      INTR(tree->bootstrap, *t).unsafe_get();
      submit_transaction(std::move(t));
    }

    // test insert
    {
      auto t = create_mutate_transaction();
      INTR(tree->insert, *t).unsafe_get();
      submit_transaction(std::move(t));
    }
    {
      auto t = create_read_transaction();
      INTR(tree->get_stats, *t).unsafe_get();
    }
    if constexpr (TEST_SEASTORE) {
      restart();
      tree->reload(NodeExtentManager::create_seastore(*tm));
    }
    {
      // Note: create_weak_transaction() can also work, but too slow.
      auto t = create_read_transaction();
      INTR(tree->validate, *t).unsafe_get();
    }

    // test erase 3/4
    {
      auto t = create_mutate_transaction();
      auto size = kvs.size() / 4 * 3;
      INTR_R(tree->erase, *t, size).unsafe_get();
      submit_transaction(std::move(t));
    }
    {
      auto t = create_read_transaction();
      INTR(tree->get_stats, *t).unsafe_get();
    }
    if constexpr (TEST_SEASTORE) {
      restart();
      tree->reload(NodeExtentManager::create_seastore(*tm));
    }
    {
      auto t = create_read_transaction();
      INTR(tree->validate, *t).unsafe_get();
    }

    // test erase remaining
    {
      auto t = create_mutate_transaction();
      auto size = kvs.size();
      INTR_R(tree->erase, *t, size).unsafe_get();
      submit_transaction(std::move(t));
    }
    {
      auto t = create_read_transaction();
      INTR(tree->get_stats, *t).unsafe_get();
    }
    if constexpr (TEST_SEASTORE) {
      restart();
      tree->reload(NodeExtentManager::create_seastore(*tm));
    }
    {
      auto t = create_read_transaction();
      INTR(tree->validate, *t).unsafe_get();
      EXPECT_EQ(INTR(tree->height, *t).unsafe_get0(), 1);
    }

    if constexpr (!TEST_SEASTORE) {
      auto p_dummy = static_cast<DummyManager*>(p_nm);
      EXPECT_EQ(p_dummy->size(), 1);
    }
    tree.reset();
  });
}

TEST_P(d_seastore_tm_test_t, 7_tree_insert_erase_eagain)
{
  run_async([this] {
    constexpr double EAGAIN_PROBABILITY = 0.1;
    constexpr bool TRACK_CURSORS = false;
    auto kvs = KVPool<test_item_t>::create_raw_range(
        {8, 11,  64, 128,  255,  256},
        {8, 13,  64, 512, 2035, 2048},
        {8, 16, 128, 576,  992, 1200},
        {0, 8}, {0, 10}, {0, 4});
    auto moved_nm = NodeExtentManager::create_seastore(
        *tm, L_ADDR_MIN, EAGAIN_PROBABILITY);
    auto p_nm = static_cast<SeastoreNodeExtentManager<true>*>(moved_nm.get());
    auto tree = std::make_unique<TreeBuilder<TRACK_CURSORS, ExtendedValue>>(
        kvs, std::move(moved_nm));
    unsigned num_ops = 0;
    unsigned num_ops_eagain = 0;

    // bootstrap
    ++num_ops;
    repeat_eagain([this, &tree, &num_ops_eagain] {
      ++num_ops_eagain;
      return seastar::do_with(
	create_mutate_transaction(),
	[this, &tree](auto &t) {
	  return INTR(tree->bootstrap, *t
	  ).safe_then([this, &t] {
	    return submit_transaction_fut(*t);
	  });
	});
    }).unsafe_get0();
    epm->run_background_work_until_halt().get0();

    // insert
    logger().warn("start inserting {} kvs ...", kvs.size());
    {
      auto iter = kvs.random_begin();
      while (iter != kvs.random_end()) {
        ++num_ops;
        repeat_eagain([this, &tree, &num_ops_eagain, &iter] {
          ++num_ops_eagain;
	  return seastar::do_with(
	    create_mutate_transaction(),
	    [this, &tree, &iter](auto &t) {
	      return INTR_R(tree->insert_one, *t, iter
	      ).safe_then([this, &t](auto cursor) {
		cursor.invalidate();
		return submit_transaction_fut(*t);
	      });
	    });
        }).unsafe_get0();
        epm->run_background_work_until_halt().get0();
        ++iter;
      }
    }

    {
      p_nm->set_generate_eagain(false);
      auto t = create_read_transaction();
      INTR(tree->get_stats, *t).unsafe_get0();
      p_nm->set_generate_eagain(true);
    }

    // lookup
    logger().warn("start lookup {} kvs ...", kvs.size());
    {
      auto iter = kvs.begin();
      while (iter != kvs.end()) {
        ++num_ops;
        repeat_eagain([this, &tree, &num_ops_eagain, &iter] {
          ++num_ops_eagain;
          auto t = create_read_transaction();
          return INTR_R(tree->validate_one, *t, iter
          ).safe_then([t=std::move(t)]{});
        }).unsafe_get0();
        ++iter;
      }
    }

    // erase
    logger().warn("start erase {} kvs ...", kvs.size());
    {
      kvs.shuffle();
      auto iter = kvs.random_begin();
      while (iter != kvs.random_end()) {
        ++num_ops;
        repeat_eagain([this, &tree, &num_ops_eagain, &iter] {
          ++num_ops_eagain;
	  return seastar::do_with(
	    create_mutate_transaction(),
	    [this, &tree, &iter](auto &t) {
	      return INTR_R(tree->erase_one, *t, iter
	      ).safe_then([this, &t] () mutable {
		return submit_transaction_fut(*t);
	      });
	    });
        }).unsafe_get0();
        epm->run_background_work_until_halt().get0();
        ++iter;
      }
      kvs.erase_from_random(kvs.random_begin(), kvs.random_end());
    }

    {
      p_nm->set_generate_eagain(false);
      auto t = create_read_transaction();
      INTR(tree->get_stats, *t).unsafe_get0();
      INTR(tree->validate, *t).unsafe_get0();
      EXPECT_EQ(INTR(tree->height,*t).unsafe_get0(), 1);
    }

    // we can adjust EAGAIN_PROBABILITY to get a proper eagain_rate
    double eagain_rate = num_ops_eagain;
    eagain_rate /= num_ops;
    logger().info("eagain rate: {}", eagain_rate);

    tree.reset();
  });
}

INSTANTIATE_TEST_SUITE_P(
  d_seastore_tm_test,
  d_seastore_tm_test_t,
  ::testing::Values (
    "segmented",
    "circularbounded"
  )
);
