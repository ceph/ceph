// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <array>
#include <cstring>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <vector>

#include "crimson/common/log.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_layout.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree.h"

#include "test/crimson/gtest_seastar.h"

// TODO: use assert instead of logging

using namespace crimson::os::seastore::onode;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }

  ghobject_t make_ghobj(
      shard_t shard, pool_t pool, crush_hash_t crush,
      std::string ns, std::string oid, snap_t snap, gen_t gen) {
    ghobject_t ghobj;
    ghobj.shard_id.id = shard;
    ghobj.hobj.pool = pool;
    ghobj.hobj.set_hash(crush);
    ghobj.hobj.nspace = ns;
    ghobj.hobj.oid.name = oid;
    ghobj.hobj.snap = snap;
    ghobj.generation = gen;
    return ghobj;
  }

  // return a key_view_t and its underlying memory buffer.
  // the buffer needs to be freed manually.
  std::pair<key_view_t, void*> build_key_view(const ghobject_t& hobj) {
    key_hobj_t key_hobj(hobj);
    size_t key_size = sizeof(shard_pool_crush_t) + sizeof(snap_gen_t) +
                      ns_oid_view_t::estimate_size<KeyT::HOBJ>(key_hobj);
    void* p_mem = std::malloc(key_size);

    key_view_t key_view;
    char* p_fill = (char*)p_mem + key_size;

    auto spc = shard_pool_crush_t::from_key<KeyT::HOBJ>(key_hobj);
    p_fill -= sizeof(shard_pool_crush_t);
    std::memcpy(p_fill, &spc, sizeof(shard_pool_crush_t));
    key_view.set(*reinterpret_cast<const shard_pool_crush_t*>(p_fill));

    auto p_ns_oid = p_fill;
    ns_oid_view_t::append<KeyT::HOBJ>(key_hobj, p_fill);
    ns_oid_view_t ns_oid_view(p_ns_oid);
    key_view.set(ns_oid_view);

    auto sg = snap_gen_t::from_key<KeyT::HOBJ>(key_hobj);
    p_fill -= sizeof(snap_gen_t);
    assert(p_fill == (char*)p_mem);
    std::memcpy(p_fill, &sg, sizeof(snap_gen_t));
    key_view.set(*reinterpret_cast<const snap_gen_t*>(p_fill));

    return {key_view, p_mem};
  }

  class Onodes {
   public:
    Onodes(size_t n) {
      for (size_t i = 1; i <= n; ++i) {
        auto p_onode = &create(i * 8);
        onodes.push_back(p_onode);
      }
    }

    ~Onodes() {
      std::for_each(tracked_onodes.begin(), tracked_onodes.end(),
                    [] (onode_t* onode) {
        std::free(onode);
      });
    }

    const onode_t& create(size_t size) {
      assert(size >= sizeof(onode_t) + sizeof(uint32_t));
      uint32_t target = size * 137;
      auto p_mem = (char*)std::malloc(size);
      auto p_onode = (onode_t*)p_mem;
      tracked_onodes.push_back(p_onode);
      p_onode->size = size;
      p_onode->id = id++;
      p_mem += (size - sizeof(uint32_t));
      std::memcpy(p_mem, &target, sizeof(uint32_t));
      validate(*p_onode);
      return *p_onode;
    }

    const onode_t& pick() const {
      auto index = rd() % onodes.size();
      return *onodes[index];
    }

    const onode_t& pick_largest() const {
      return *onodes[onodes.size() - 1];
    }

    static void validate_cursor(
        const Btree::Cursor& cursor, const ghobject_t& key, const onode_t& onode) {
      assert(!cursor.is_end());
      assert(cursor.get_ghobj() == key);
      assert(cursor.value());
      assert(cursor.value() != &onode);
      assert(*cursor.value() == onode);
      validate(*cursor.value());
    }

   private:
    static void validate(const onode_t& node) {
      auto p_target = (const char*)&node + node.size - sizeof(uint32_t);
      uint32_t target;
      std::memcpy(&target, p_target, sizeof(uint32_t));
      assert(target == node.size * 137);
    }

    uint16_t id = 0;
    mutable std::random_device rd;
    std::vector<const onode_t*> onodes;
    std::vector<onode_t*> tracked_onodes;
  };
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
  onode_t value = {2};
#define _STAGE_T(NodeType) node_to_stage_t<typename NodeType::node_stage_t>
#define NXT_T(StageType)  staged<typename StageType::next_param_t>
  laddr_packed_t i_value{0};
  logger().info("\n"
    "Bytes of a key-value insertion (full-string):\n"
    "  s-p-c, 'n'-'o', s-g => onode_t(2): typically internal 41B, leaf 35B\n"
    "  InternalNode0: {} {} {}\n"
    "  InternalNode1: {} {} {}\n"
    "  InternalNode2: {} {}\n"
    "  InternalNode3: {}\n"
    "  LeafNode0: {} {} {}\n"
    "  LeafNode1: {} {} {}\n"
    "  LeafNode2: {} {}\n"
    "  LeafNode3: {}",
    _STAGE_T(InternalNode0)::template insert_size<KeyT::VIEW>(key_view, i_value),
    NXT_T(_STAGE_T(InternalNode0))::template insert_size<KeyT::VIEW>(key_view, i_value),
    NXT_T(NXT_T(_STAGE_T(InternalNode0)))::template insert_size<KeyT::VIEW>(key_view, i_value),
    _STAGE_T(InternalNode1)::template insert_size<KeyT::VIEW>(key_view, i_value),
    NXT_T(_STAGE_T(InternalNode1))::template insert_size<KeyT::VIEW>(key_view, i_value),
    NXT_T(NXT_T(_STAGE_T(InternalNode1)))::template insert_size<KeyT::VIEW>(key_view, i_value),
    _STAGE_T(InternalNode2)::template insert_size<KeyT::VIEW>(key_view, i_value),
    NXT_T(_STAGE_T(InternalNode2))::template insert_size<KeyT::VIEW>(key_view, i_value),
    _STAGE_T(InternalNode3)::template insert_size<KeyT::VIEW>(key_view, i_value),
    _STAGE_T(LeafNode0)::template insert_size<KeyT::HOBJ>(key, value),
    NXT_T(_STAGE_T(LeafNode0))::template insert_size<KeyT::HOBJ>(key, value),
    NXT_T(NXT_T(_STAGE_T(LeafNode0)))::template insert_size<KeyT::HOBJ>(key, value),
    _STAGE_T(LeafNode1)::template insert_size<KeyT::HOBJ>(key, value),
    NXT_T(_STAGE_T(LeafNode1))::template insert_size<KeyT::HOBJ>(key, value),
    NXT_T(NXT_T(_STAGE_T(LeafNode1)))::template insert_size<KeyT::HOBJ>(key, value),
    _STAGE_T(LeafNode2)::template insert_size<KeyT::HOBJ>(key, value),
    NXT_T(_STAGE_T(LeafNode2))::template insert_size<KeyT::HOBJ>(key, value),
    _STAGE_T(LeafNode3)::template insert_size<KeyT::HOBJ>(key, value)
  );
  std::free(p_mem);
}

TEST_F(a_basic_test_t, 2_node_sizes)
{
  run_async([this] {
    auto nm = NodeExtentManager::create_dummy();
    auto t = make_transaction();
    context_t c{*nm, *t};
    std::array<std::pair<NodeImplURef, NodeExtentMutable>, 16> nodes = {
      InternalNode0::allocate(c, false, 1u).unsafe_get0().make_pair(),
      InternalNode1::allocate(c, false, 1u).unsafe_get0().make_pair(),
      InternalNode2::allocate(c, false, 1u).unsafe_get0().make_pair(),
      InternalNode3::allocate(c, false, 1u).unsafe_get0().make_pair(),
      InternalNode0::allocate(c, true, 1u).unsafe_get0().make_pair(),
      InternalNode1::allocate(c, true, 1u).unsafe_get0().make_pair(),
      InternalNode2::allocate(c, true, 1u).unsafe_get0().make_pair(),
      InternalNode3::allocate(c, true, 1u).unsafe_get0().make_pair(),
      LeafNode0::allocate(c, false, 0u).unsafe_get0().make_pair(),
      LeafNode1::allocate(c, false, 0u).unsafe_get0().make_pair(),
      LeafNode2::allocate(c, false, 0u).unsafe_get0().make_pair(),
      LeafNode3::allocate(c, false, 0u).unsafe_get0().make_pair(),
      LeafNode0::allocate(c, true, 0u).unsafe_get0().make_pair(),
      LeafNode1::allocate(c, true, 0u).unsafe_get0().make_pair(),
      LeafNode2::allocate(c, true, 0u).unsafe_get0().make_pair(),
      LeafNode3::allocate(c, true, 0u).unsafe_get0().make_pair()
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
  NodeExtentManagerURef moved_nm;
  TransactionRef ref_t;
  Transaction& t;
  context_t c;
  Btree tree;

  b_dummy_tree_test_t()
    : moved_nm{NodeExtentManager::create_dummy()},
      ref_t{make_transaction()},
      t{*ref_t},
      c{*moved_nm, t},
      tree{std::move(moved_nm)} {}

  seastar::future<> set_up_fut() override final {
    return tree.mkfs(t).handle_error(
      crimson::ct_error::all_same_way([] {
        ASSERT_FALSE("Unable to mkfs");
      })
    );
  }
};

TEST_F(b_dummy_tree_test_t, 3_random_insert_leaf_node)
{
  run_async([this] {
    logger().info("\n---------------------------------------------"
                  "\nrandomized leaf node insert:\n");
    auto key_s = make_ghobj(0, 0, 0, "ns", "oid", 0, 0);
    auto key_e = make_ghobj(
        std::numeric_limits<shard_t>::max(), 0, 0, "ns", "oid", 0, 0);
    assert(tree.find(t, key_s).unsafe_get0().is_end());
    assert(tree.begin(t).unsafe_get0().is_end());
    assert(tree.last(t).unsafe_get0().is_end());

    std::vector<std::tuple<ghobject_t,
                           const onode_t*,
                           Btree::Cursor>> insert_history;
    auto f_validate_insert_new = [this, &insert_history] (
        const ghobject_t& key, const onode_t& value) {
      auto [cursor, success] = tree.insert(t, key, value).unsafe_get0();
      assert(success == true);
      insert_history.emplace_back(key, &value, cursor);
      Onodes::validate_cursor(cursor, key, value);
      auto cursor_ = tree.lower_bound(t, key).unsafe_get0();
      assert(cursor_.get_ghobj() == key);
      assert(cursor_.value() == cursor.value());
      return cursor.value();
    };
    auto onodes = Onodes(15);

    // insert key1, onode1 at STAGE_LEFT
    auto key1 = make_ghobj(3, 3, 3, "ns3", "oid3", 3, 3);
    auto& onode1 = onodes.pick();
    auto p_value1 = f_validate_insert_new(key1, onode1);

    // validate lookup
    {
      auto cursor1_s = tree.lower_bound(t, key_s).unsafe_get0();
      assert(cursor1_s.get_ghobj() == key1);
      assert(cursor1_s.value() == p_value1);
      auto cursor1_e = tree.lower_bound(t, key_e).unsafe_get0();
      assert(cursor1_e.is_end());
    }

    // insert the same key1 with a different onode
    {
      auto& onode1_dup = onodes.pick();
      auto [cursor1_dup, ret1_dup] = tree.insert(
          t, key1, onode1_dup).unsafe_get0();
      assert(ret1_dup == false);
      Onodes::validate_cursor(cursor1_dup, key1, onode1);
    }

    // insert key2, onode2 to key1's left at STAGE_LEFT
    // insert node front at STAGE_LEFT
    auto key2 = make_ghobj(2, 2, 2, "ns3", "oid3", 3, 3);
    auto& onode2 = onodes.pick();
    f_validate_insert_new(key2, onode2);

    // insert key3, onode3 to key1's right at STAGE_LEFT
    // insert node last at STAGE_LEFT
    auto key3 = make_ghobj(4, 4, 4, "ns3", "oid3", 3, 3);
    auto& onode3 = onodes.pick();
    f_validate_insert_new(key3, onode3);

    // insert key4, onode4 to key1's left at STAGE_STRING (collision)
    auto key4 = make_ghobj(3, 3, 3, "ns2", "oid2", 3, 3);
    auto& onode4 = onodes.pick();
    f_validate_insert_new(key4, onode4);

    // insert key5, onode5 to key1's right at STAGE_STRING (collision)
    auto key5 = make_ghobj(3, 3, 3, "ns4", "oid4", 3, 3);
    auto& onode5 = onodes.pick();
    f_validate_insert_new(key5, onode5);

    // insert key6, onode6 to key1's left at STAGE_RIGHT
    auto key6 = make_ghobj(3, 3, 3, "ns3", "oid3", 2, 2);
    auto& onode6 = onodes.pick();
    f_validate_insert_new(key6, onode6);

    // insert key7, onode7 to key1's right at STAGE_RIGHT
    auto key7 = make_ghobj(3, 3, 3, "ns3", "oid3", 4, 4);
    auto& onode7 = onodes.pick();
    f_validate_insert_new(key7, onode7);

    // insert node front at STAGE_RIGHT
    auto key8 = make_ghobj(2, 2, 2, "ns3", "oid3", 2, 2);
    auto& onode8 = onodes.pick();
    f_validate_insert_new(key8, onode8);

    // insert node front at STAGE_STRING (collision)
    auto key9 = make_ghobj(2, 2, 2, "ns2", "oid2", 3, 3);
    auto& onode9 = onodes.pick();
    f_validate_insert_new(key9, onode9);

    // insert node last at STAGE_RIGHT
    auto key10 = make_ghobj(4, 4, 4, "ns3", "oid3", 4, 4);
    auto& onode10 = onodes.pick();
    f_validate_insert_new(key10, onode10);

    // insert node last at STAGE_STRING (collision)
    auto key11 = make_ghobj(4, 4, 4, "ns4", "oid4", 3, 3);
    auto& onode11 = onodes.pick();
    f_validate_insert_new(key11, onode11);

    // insert key, value randomly until a perfect 3-ary tree is formed
    std::vector<std::pair<ghobject_t, const onode_t*>> kvs{
      {make_ghobj(2, 2, 2, "ns2", "oid2", 2, 2), &onodes.pick()},
      {make_ghobj(2, 2, 2, "ns2", "oid2", 4, 4), &onodes.pick()},
      {make_ghobj(2, 2, 2, "ns3", "oid3", 4, 4), &onodes.pick()},
      {make_ghobj(2, 2, 2, "ns4", "oid4", 2, 2), &onodes.pick()},
      {make_ghobj(2, 2, 2, "ns4", "oid4", 3, 3), &onodes.pick()},
      {make_ghobj(2, 2, 2, "ns4", "oid4", 4, 4), &onodes.pick()},
      {make_ghobj(3, 3, 3, "ns2", "oid2", 2, 2), &onodes.pick()},
      {make_ghobj(3, 3, 3, "ns2", "oid2", 4, 4), &onodes.pick()},
      {make_ghobj(3, 3, 3, "ns4", "oid4", 2, 2), &onodes.pick()},
      {make_ghobj(3, 3, 3, "ns4", "oid4", 4, 4), &onodes.pick()},
      {make_ghobj(4, 4, 4, "ns2", "oid2", 2, 2), &onodes.pick()},
      {make_ghobj(4, 4, 4, "ns2", "oid2", 3, 3), &onodes.pick()},
      {make_ghobj(4, 4, 4, "ns2", "oid2", 4, 4), &onodes.pick()},
      {make_ghobj(4, 4, 4, "ns3", "oid3", 2, 2), &onodes.pick()},
      {make_ghobj(4, 4, 4, "ns4", "oid4", 2, 2), &onodes.pick()},
      {make_ghobj(4, 4, 4, "ns4", "oid4", 4, 4), &onodes.pick()}};
    auto [smallest_key, smallest_value] = kvs[0];
    auto [largest_key, largest_value] = kvs[kvs.size() - 1];
    std::random_shuffle(kvs.begin(), kvs.end());
    std::for_each(kvs.begin(), kvs.end(), [&f_validate_insert_new] (auto& kv) {
      f_validate_insert_new(kv.first, *kv.second);
    });
    assert(tree.height(t).unsafe_get0() == 1);
    assert(!tree.test_is_clean());

    for (auto& [k, v, c] : insert_history) {
      // validate values in tree keep intact
      auto cursor = tree.lower_bound(t, k).unsafe_get0();
      Onodes::validate_cursor(cursor, k, *v);
      // validate values in cursors keep intact
      Onodes::validate_cursor(c, k, *v);
    }
    Onodes::validate_cursor(
        tree.lower_bound(t, key_s).unsafe_get0(), smallest_key, *smallest_value);
    Onodes::validate_cursor(
        tree.begin(t).unsafe_get0(), smallest_key, *smallest_value);
    Onodes::validate_cursor(
        tree.last(t).unsafe_get0(), largest_key, *largest_value);

    std::ostringstream oss;
    tree.dump(t, oss);
    logger().info("\n{}\n", oss.str());

    insert_history.clear();
  });
}

static std::set<ghobject_t> build_key_set(
    std::pair<unsigned, unsigned> range_2,
    std::pair<unsigned, unsigned> range_1,
    std::pair<unsigned, unsigned> range_0,
    std::string padding = "",
    bool is_internal = false) {
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
  NodeExtentManagerURef moved_nm;
  TransactionRef ref_t;
  Transaction& t;
  context_t c;
  Btree tree;
  Onodes onodes;
  std::vector<std::tuple<
    ghobject_t, const onode_t*, Btree::Cursor>> insert_history;

 public:
  TestTree()
    : moved_nm{NodeExtentManager::create_dummy()},
      ref_t{make_transaction()},
      t{*ref_t},
      c{*moved_nm, t},
      tree{std::move(moved_nm)},
      onodes{0} {}

  seastar::future<> build_tree(
      std::pair<unsigned, unsigned> range_2,
      std::pair<unsigned, unsigned> range_1,
      std::pair<unsigned, unsigned> range_0,
      size_t onode_size) {
    return seastar::async([this, range_2, range_1, range_0, onode_size] {
      tree.mkfs(t).unsafe_get0();
      logger().info("\n---------------------------------------------"
                    "\nbefore leaf node split:\n");
      auto keys = build_key_set(range_2, range_1, range_0);
      for (auto& key : keys) {
        auto& value = onodes.create(onode_size);
        auto [cursor, success] = tree.insert(t, key, value).unsafe_get0();
        assert(success == true);
        Onodes::validate_cursor(cursor, key, value);
        insert_history.emplace_back(key, &value, cursor);
      }
      assert(tree.height(t).unsafe_get0() == 1);
      assert(!tree.test_is_clean());
      std::ostringstream oss;
      tree.dump(t, oss);
      logger().info("\n{}\n", oss.str());
    });
  }

  seastar::future<> split(const ghobject_t& key, const onode_t& value) {
    return seastar::async([this, key, &value] {
      Btree tree_clone(NodeExtentManager::create_dummy());
      auto ref_t_clone = make_transaction();
      Transaction& t_clone = *ref_t_clone;
      tree_clone.test_clone_from(t_clone, t, tree).unsafe_get0();

      logger().info("insert {}:", key_hobj_t(key));
      auto [cursor, success] = tree_clone.insert(t_clone, key, value).unsafe_get0();
      assert(success == true);
      Onodes::validate_cursor(cursor, key, value);

      std::ostringstream oss;
      tree_clone.dump(t_clone, oss);
      logger().info("\n{}\n", oss.str());
      assert(tree_clone.height(t_clone).unsafe_get0() == 2);

      for (auto& [k, v, c] : insert_history) {
        auto result = tree_clone.lower_bound(t_clone, k).unsafe_get0();
        Onodes::validate_cursor(result, k, *v);
      }
      auto result = tree_clone.lower_bound(t_clone, key).unsafe_get0();
      Onodes::validate_cursor(result, key, value);
    });
  }

  const onode_t& create_onode(size_t size) {
    return onodes.create(size);
  }
};

struct c_dummy_test_t : public seastar_test_suite_t {};

TEST_F(c_dummy_test_t, 4_split_leaf_node)
{
  run_async([this] {
    {
      TestTree test;
      test.build_tree({2, 5}, {2, 5}, {2, 5}, 120).get0();

      auto& onode = test.create_onode(1084);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to left front at stage 2, 1, 0\n");
      test.split(make_ghobj(1, 1, 1, "ns3", "oid3", 3, 3), onode).get0();
      test.split(make_ghobj(2, 2, 2, "ns1", "oid1", 3, 3), onode).get0();
      test.split(make_ghobj(2, 2, 2, "ns2", "oid2", 1, 1), onode).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to left back at stage 0, 1, 2, 1, 0\n");
      test.split(make_ghobj(2, 2, 2, "ns4", "oid4", 5, 5), onode).get0();
      test.split(make_ghobj(2, 2, 2, "ns5", "oid5", 3, 3), onode).get0();
      test.split(make_ghobj(2, 3, 3, "ns3", "oid3", 3, 3), onode).get0();
      test.split(make_ghobj(3, 3, 3, "ns1", "oid1", 3, 3), onode).get0();
      test.split(make_ghobj(3, 3, 3, "ns2", "oid2", 1, 1), onode).get0();

      auto& onode0 = test.create_onode(1476);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right front at stage 0, 1, 2, 1, 0\n");
      test.split(make_ghobj(3, 3, 3, "ns4", "oid4", 5, 5), onode0).get0();
      test.split(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), onode0).get0();
      test.split(make_ghobj(3, 4, 4, "ns3", "oid3", 3, 3), onode0).get0();
      test.split(make_ghobj(4, 4, 4, "ns1", "oid1", 3, 3), onode0).get0();
      test.split(make_ghobj(4, 4, 4, "ns2", "oid2", 1, 1), onode0).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right back at stage 0, 1, 2\n");
      test.split(make_ghobj(4, 4, 4, "ns4", "oid4", 5, 5), onode0).get0();
      test.split(make_ghobj(4, 4, 4, "ns5", "oid5", 3, 3), onode0).get0();
      test.split(make_ghobj(5, 5, 5, "ns3", "oid3", 3, 3), onode0).get0();

      auto& onode1 = test.create_onode(316);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to left middle at stage 0, 1, 2, 1, 0\n");
      test.split(make_ghobj(2, 2, 2, "ns4", "oid4", 5, 5), onode1).get0();
      test.split(make_ghobj(2, 2, 2, "ns5", "oid5", 3, 3), onode1).get0();
      test.split(make_ghobj(2, 2, 3, "ns3", "oid3", 3, 3), onode1).get0();
      test.split(make_ghobj(3, 3, 3, "ns1", "oid1", 3, 3), onode1).get0();
      test.split(make_ghobj(3, 3, 3, "ns2", "oid2", 1, 1), onode1).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to left back at stage 0, 1, 0\n");
      test.split(make_ghobj(3, 3, 3, "ns2", "oid2", 5, 5), onode1).get0();
      test.split(make_ghobj(3, 3, 3, "ns2", "oid3", 3, 3), onode1).get0();
      test.split(make_ghobj(3, 3, 3, "ns3", "oid3", 1, 1), onode1).get0();

      auto& onode2 = test.create_onode(452);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to right front at stage 0, 1, 0\n");
      test.split(make_ghobj(3, 3, 3, "ns3", "oid3", 5, 5), onode2).get0();
      test.split(make_ghobj(3, 3, 3, "ns3", "oid4", 3, 3), onode2).get0();
      test.split(make_ghobj(3, 3, 3, "ns4", "oid4", 1, 1), onode2).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to right middle at stage 0, 1, 2, 1, 0\n");
      test.split(make_ghobj(3, 3, 3, "ns4", "oid4", 5, 5), onode2).get0();
      test.split(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), onode2).get0();
      test.split(make_ghobj(3, 3, 4, "ns3", "oid3", 3, 3), onode2).get0();
      test.split(make_ghobj(4, 4, 4, "ns1", "oid1", 3, 3), onode2).get0();
      test.split(make_ghobj(4, 4, 4, "ns2", "oid2", 1, 1), onode2).get0();

      auto& onode3 = test.create_onode(964);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to right middle at stage 0, 1, 2, 1, 0\n");
      test.split(make_ghobj(3, 3, 3, "ns4", "oid4", 5, 5), onode3).get0();
      test.split(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), onode3).get0();
      test.split(make_ghobj(3, 3, 4, "ns3", "oid3", 3, 3), onode3).get0();
      test.split(make_ghobj(4, 4, 4, "ns1", "oid1", 3, 3), onode3).get0();
      test.split(make_ghobj(4, 4, 4, "ns2", "oid2", 1, 1), onode3).get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to right front at stage 0\n");
      test.split(make_ghobj(3, 3, 3, "ns4", "oid4", 2, 3), onode3).get0();

      auto& onode4 = test.create_onode(572);
      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to left back at stage 0\n");
      test.split(make_ghobj(3, 3, 3, "ns2", "oid2", 3, 4), onode4).get0();
    }

    // TODO: test split at {0, 0, 0}
    // TODO: test split at {END, END, END}
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
    }

   public:
    laddr_t laddr() const override { return _laddr; }
    bool is_level_tail() const override { return _is_level_tail; }

   protected:
    field_type_t field_type() const override { return field_type_t::N0; }
    level_t level() const override { return 0u; }
    key_view_t get_largest_key_view() const override { return key_view; }
    void prepare_mutate(context_t) override {
      assert(false && "impossible path"); }
    bool is_empty() const override {
      assert(false && "impossible path"); }
    node_offset_t free_size() const override {
      assert(false && "impossible path"); }
    key_view_t get_key_view(const search_position_t&) const override {
      assert(false && "impossible path"); }
    std::ostream& dump(std::ostream&) const override {
      assert(false && "impossible path"); }
    std::ostream& dump_brief(std::ostream&) const override {
      assert(false && "impossible path"); }
    void test_copy_to(NodeExtentMutable&) const override {
      assert(false && "impossible path"); }
    void test_set_tail(NodeExtentMutable&) override {
      assert(false && "impossible path"); }

   private:
    std::set<ghobject_t> keys;
    bool _is_level_tail;
    laddr_t _laddr;

    key_view_t key_view;
    void* p_mem_key_view;
  };

  class DummyChild final : public Node {
   public:
    ~DummyChild() override = default;

    node_future<> populate_split(
        context_t c, std::set<Ref<DummyChild>>& splitable_nodes) {
      assert(can_split());
      assert(splitable_nodes.find(this) != splitable_nodes.end());

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
      return insert_parent(c, right_child);
    }

    node_future<> insert_and_split(
        context_t c, const ghobject_t& insert_key,
        std::set<Ref<DummyChild>>& splitable_nodes) {
      const auto& keys = impl->get_keys();
      assert(keys.size() == 1);
      auto& key = *keys.begin();
      assert(insert_key < key);

      std::set<ghobject_t> new_keys;
      new_keys.insert(insert_key);
      new_keys.insert(key);
      impl->reset(new_keys, impl->is_level_tail());

      splitable_nodes.clear();
      splitable_nodes.insert(this);
      auto fut = populate_split(c, splitable_nodes);
      assert(!splitable_nodes.size());
      return fut;
    }

    bool match_pos(const search_position_t& pos) const {
      assert(!is_root());
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

    static node_future<Ref<DummyChild>> create_initial(
        context_t c, const std::set<ghobject_t>& keys,
        DummyChildPool& pool, RootNodeTracker& root_tracker) {
      auto initial = create_new(keys, true, pool);
      return c.nm.get_super(c.t, root_tracker
      ).safe_then([c, &pool, initial](auto super) {
        initial->make_root_new(c, std::move(super));
        return initial->upgrade_root(c).safe_then([initial] {
          return initial;
        });
      });
    }

   protected:
    node_future<> test_clone_non_root(
        context_t, Ref<InternalNode> new_parent) const override {
      assert(!is_root());
      auto p_pool_clone = pool.pool_clone_in_progress;
      assert(p_pool_clone);
      auto clone = create(
          impl->get_keys(), impl->is_level_tail(), impl->laddr(), *p_pool_clone);
      clone->as_child(parent_info().position, new_parent);
      return node_ertr::now();
    }
    node_future<Ref<tree_cursor_t>> lookup_smallest(context_t) override {
      assert(false && "impossible path"); }
    node_future<Ref<tree_cursor_t>> lookup_largest(context_t) override {
      assert(false && "impossible path"); }
    node_future<> test_clone_root(context_t, RootNodeTracker&) const override {
      assert(false && "impossible path"); }
    node_future<search_result_t> lower_bound_tracked(
        context_t, const key_hobj_t&, MatchHistory&) override {
      assert(false && "impossible path"); }

   private:
    DummyChild(DummyChildImpl* impl, DummyChildImpl::URef&& ref, DummyChildPool& pool)
      : Node(std::move(ref)), impl{impl}, pool{pool} {
      pool.track_node(this);
    }

    bool can_split() const { return impl->get_keys().size() > 1; }

    DummyChildImpl* impl;
    DummyChildPool& pool;
    mutable std::random_device rd;
  };

 public:
  using node_ertr = Node::node_ertr;
  template <class... ValuesT>
  using node_future = Node::node_future<ValuesT...>;

  DummyChildPool() = default;
  ~DummyChildPool() { reset(); }

  node_future<> build_tree(const std::set<ghobject_t>& keys) {
    reset();

    // create tree
    auto ref_nm = NodeExtentManager::create_dummy();
    p_nm = ref_nm.get();
    p_btree.emplace(std::move(ref_nm));
    return DummyChild::create_initial(get_context(), keys, *this, *p_btree->root_tracker
    ).safe_then([this](auto initial_child) {
      // split
      splitable_nodes.insert(initial_child);
      return crimson::do_until([this] {
        if (splitable_nodes.empty()) {
          return node_ertr::make_ready_future<bool>(true);
        }
        auto index = rd() % splitable_nodes.size();
        auto iter = splitable_nodes.begin();
        std::advance(iter, index);
        Ref<DummyChild> child = *iter;
        return child->populate_split(get_context(), splitable_nodes
        ).safe_then([] {
          return node_ertr::make_ready_future<bool>(false);
        });
      });
    }).safe_then([this] {
      std::ostringstream oss;
      p_btree->dump(t(), oss);
      logger().info("\n{}\n", oss.str());
      return p_btree->height(t());
    }).safe_then([](auto height) {
      assert(height == 2);
    });
  }

  seastar::future<> test_split(ghobject_t key, search_position_t pos) {
    return seastar::async([this, key, pos] {
      logger().info("insert {} at {}:", key_hobj_t(key), pos);
      DummyChildPool pool_clone;
      pool_clone_in_progress = &pool_clone;
      auto ref_nm = NodeExtentManager::create_dummy();
      pool_clone.p_nm = ref_nm.get();
      pool_clone.p_btree.emplace(std::move(ref_nm));
      pool_clone.p_btree->test_clone_from(
        pool_clone.t(), t(), *p_btree).unsafe_get0();
      pool_clone_in_progress = nullptr;
      auto node_to_split = pool_clone.get_node_by_pos(pos);
      node_to_split->insert_and_split(
        pool_clone.get_context(), key, pool_clone.splitable_nodes).unsafe_get0();
      std::ostringstream oss;
      pool_clone.p_btree->dump(pool_clone.t(), oss);
      logger().info("\n{}\n", oss.str());
      assert(pool_clone.p_btree->height(pool_clone.t()).unsafe_get0() == 3);
    });
  }

 private:
  void reset() {
    assert(!pool_clone_in_progress);
    if (tracked_children.size()) {
      assert(!p_btree->test_is_clean());
      tracked_children.clear();
      assert(p_btree->test_is_clean());
      p_nm = nullptr;
      p_btree.reset();
    } else {
      assert(!p_btree);
    }
    splitable_nodes.clear();
  }

  void track_node(Ref<DummyChild> node) {
    assert(tracked_children.find(node) == tracked_children.end());
    tracked_children.insert(node);
  }

  Ref<DummyChild> get_node_by_pos(const search_position_t& pos) const {
    auto iter = std::find_if(
        tracked_children.begin(), tracked_children.end(), [&pos](auto& child) {
      return child->match_pos(pos);
    });
    assert(iter != tracked_children.end());
    return *iter;
  }

  context_t get_context() {
    assert(p_nm != nullptr);
    return {*p_nm, t()};
  }

  Transaction& t() const { return *ref_t; }

  std::set<Ref<DummyChild>> tracked_children;
  std::optional<Btree> p_btree;
  NodeExtentManager* p_nm = nullptr;
  TransactionRef ref_t = make_transaction();

  std::random_device rd;
  std::set<Ref<DummyChild>> splitable_nodes;

  DummyChildPool* pool_clone_in_progress = nullptr;
};

}

TEST_F(c_dummy_test_t, 5_split_internal_node)
{
  run_async([this] {
    DummyChildPool pool;
    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert:\n");
      auto padding = std::string(250, '_');
      auto keys = build_key_set({2, 6}, {2, 5}, {2, 5}, padding, true);
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right front at stage 0, 1, 2, 1, 0\n");
      pool.test_split(make_ghobj(3, 3, 3, "ns4", "oid4" + padding, 5, 5), {2, {0, {0}}}).get();
      pool.test_split(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), {2, {0, {0}}}).get();
      pool.test_split(make_ghobj(3, 4, 4, "ns3", "oid3", 3, 3), {2, {0, {0}}}).get();
      pool.test_split(make_ghobj(4, 4, 4, "ns1", "oid1", 3, 3), {2, {0, {0}}}).get();
      pool.test_split(make_ghobj(4, 4, 4, "ns2", "oid2" + padding, 1, 1), {2, {0, {0}}}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right middle at stage 0, 1, 2, 1, 0\n");
      pool.test_split(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 5, 5), {3, {0, {0}}}).get();
      pool.test_split(make_ghobj(4, 4, 4, "ns5", "oid5", 3, 3), {3, {0, {0}}}).get();
      pool.test_split(make_ghobj(4, 4, 5, "ns3", "oid3", 3, 3), {3, {0, {0}}}).get();
      pool.test_split(make_ghobj(5, 5, 5, "ns1", "oid1", 3, 3), {3, {0, {0}}}).get();
      pool.test_split(make_ghobj(5, 5, 5, "ns2", "oid2" + padding, 1, 1), {3, {0, {0}}}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to right back at stage 0, 1, 2\n");
      pool.test_split(
          make_ghobj(5, 5, 5, "ns4", "oid4" + padding, 5, 5), search_position_t::end()).get();
      pool.test_split(make_ghobj(5, 5, 5, "ns5", "oid5", 3, 3), search_position_t::end()).get();
      pool.test_split(make_ghobj(6, 6, 6, "ns3", "oid3", 3, 3), search_position_t::end()).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to left front at stage 2, 1, 0\n");
      pool.test_split(make_ghobj(1, 1, 1, "ns3", "oid3", 3, 3), {0, {0, {0}}}).get();
      pool.test_split(make_ghobj(2, 2, 2, "ns1", "oid1", 3, 3), {0, {0, {0}}}).get();
      pool.test_split(make_ghobj(2, 2, 2, "ns2", "oid2" + padding, 1, 1), {0, {0, {0}}}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0/1; insert to left middle at stage 0, 1, 2, 1, 0\n");
      pool.test_split(make_ghobj(2, 2, 2, "ns4", "oid4" + padding, 5, 5), {1, {0, {0}}}).get();
      pool.test_split(make_ghobj(2, 2, 2, "ns5", "oid5", 3, 3), {1, {0, {0}}}).get();
      pool.test_split(
          make_ghobj(2, 2, 3, "ns3", "oid3" + std::string(80, '_'), 3, 3), {1, {0, {0}}}).get();
      pool.test_split(make_ghobj(3, 3, 3, "ns1", "oid1", 3, 3), {1, {0, {0}}}).get();
      pool.test_split(make_ghobj(3, 3, 3, "ns2", "oid2" + padding, 1, 1), {1, {0, {0}}}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to left back at stage 0\n");
      pool.test_split(make_ghobj(3, 3, 3, "ns4", "oid4" + padding, 3, 4), {1, {2, {2}}}).get();
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
      pool.test_split(make_ghobj(3, 3, 3, "ns4", "oid4" + padding, 5, 5), {2, {0, {0}}}).get();
      pool.test_split(make_ghobj(3, 3, 3, "ns5", "oid5", 3, 3), {2, {0, {0}}}).get();
      pool.test_split(make_ghobj(3, 4, 4, "n", "o", 3, 3), {2, {0, {0}}}).get();
      pool.test_split(make_ghobj(4, 4, 4, "n", "o", 3, 3), {2, {0, {0}}}).get();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 2; insert to left middle at stage 2\n");
      pool.test_split(make_ghobj(2, 3, 3, "n", "o", 3, 3), {1, {0, {0}}}).get();
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
      pool.test_split(make_ghobj(4, 4, 4, "n", "o", 2, 2), {2, {0, {0}}}).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (3):\n");
      auto padding = std::string(417, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding, true);
      keys.insert(make_ghobj(4, 4, 4, "ns3", "oid3" + padding, 5, 5));
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 2, 2));
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 3, 3));
      keys.erase(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 4, 4));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to right front at stage 0, 1, 0\n");
      pool.test_split(make_ghobj(3, 3, 3, "ns2", "oid2" + padding, 5, 5), {1, {1, {0}}}).get();
      pool.test_split(make_ghobj(3, 3, 3, "ns2", "oid3", 3, 3), {1, {1, {0}}}).get();
      pool.test_split(make_ghobj(3, 3, 3, "ns3", "oid3" + padding, 1, 1), {1, {1, {0}}}).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (4):\n");
      auto padding = std::string(360, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding, true);
      keys.insert(make_ghobj(4, 4, 4, "ns4", "oid4" + padding, 5, 5));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 1; insert to left back at stage 0, 1\n");
      pool.test_split(make_ghobj(3, 3, 3, "ns2", "oid2" + padding, 5, 5), {1, {1, {0}}}).get();
      pool.test_split(make_ghobj(3, 3, 3, "ns2", "oid3", 3, 3), {1, {1, {0}}}).get();
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
      pool.test_split(make_ghobj(3, 3, 3, "ns2", "oid3", 2, 2), {1, {1, {0}}}).get();
    }

    {
      logger().info("\n---------------------------------------------"
                    "\nbefore internal node insert (6):\n");
      auto padding = std::string(328, '_');
      auto keys = build_key_set({2, 5}, {2, 5}, {2, 5}, padding);
      keys.insert(make_ghobj(5, 5, 5, "ns3", "oid3" + std::string(271, '_'), 3, 3));
      keys.insert(make_ghobj(9, 9, 9, "ns~last", "oid~last", 9, 9));
      pool.build_tree(keys).unsafe_get0();

      logger().info("\n---------------------------------------------"
                    "\nsplit at stage 0; insert to right front at stage 0\n");
      pool.test_split(make_ghobj(3, 3, 3, "ns3", "oid3" + padding, 2, 3), {1, {1, {1}}}).get();
    }

    // TODO: test split at {0, 0, 0}
    // TODO: test split at {END, END, END}
  });
}
