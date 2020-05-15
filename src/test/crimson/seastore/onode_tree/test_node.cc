// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>

#include "crimson/os/seastore/onode_manager/simple-fltree/onode_node.h"

using crimson::os::seastore::Onode;
using crimson::os::seastore::OnodeRef;

TEST(OnodeNode, denc)
{
  Onode onode{"hello"};
  bufferlist bl;
  ceph::encode(onode, bl);
  bl.rebuild();
  auto flattened = reinterpret_cast<const onode_t*>(bl.c_str());
  auto actual_onode = flattened->decode();
  ASSERT_EQ(*actual_onode, onode);
}

TEST(OnodeNode, lookup)
{
  static constexpr size_t BLOCK_SIZE = 512;
  char buf[BLOCK_SIZE];
  using leaf_node_0 = node_t<BLOCK_SIZE, 0, ntype_t::leaf>;
  auto leaf = new (buf) leaf_node_0;
  ghobject_t oid{hobject_t{object_t{"saturn"}, "", 0, 0, 0, "solar"}};
  {
    auto [slot, found] = leaf->lower_bound(oid);
    ASSERT_FALSE(found);
    ASSERT_EQ(0, slot);
  }
  Onode onode{"hello"};
  bufferlist bl;
  ceph::encode(onode, bl);
  bl.rebuild();
  auto flattened = reinterpret_cast<const onode_t*>(bl.c_str());
  leaf->insert_at(0, oid, *flattened);
  {
    auto [slot, found] = leaf->lower_bound(oid);
    ASSERT_TRUE(found);
    ASSERT_EQ(0, slot);
    const auto& [key1, key2] = leaf->key_at(slot);
    auto& item = leaf->item_at(key1);
    auto actual_onode = item.decode();
    ASSERT_EQ(*actual_onode, onode);
  }
}

TEST(OnodeNode, grab_from_right)
{
  static constexpr size_t BLOCK_SIZE = 512;
  char buf1[BLOCK_SIZE];
  char buf2[BLOCK_SIZE];
  using leaf_node_0 = node_t<BLOCK_SIZE, 0, ntype_t::leaf>;
  auto leaf1 = new (buf1) leaf_node_0;
  auto leaf2 = new (buf2) leaf_node_0;
  auto& dummy_parent = *leaf1;

  ghobject_t oid1{hobject_t{object_t{"earth"}, "", 0, 0, 0, "solar"}};
  ghobject_t oid2{hobject_t{object_t{"jupiter"}, "", 0, 0, 0, "solar"}};
  ghobject_t oid3{hobject_t{object_t{"saturn"}, "", 0, 0, 0, "solar"}};
  Onode onode{"hello"};
  bufferlist bl;
  ceph::encode(onode, bl);
  bl.rebuild();
  auto flattened = reinterpret_cast<const onode_t*>(bl.c_str());
  // so they are ordered as they should
  leaf1->insert_at(0, oid1, *flattened);
  ASSERT_EQ(1, leaf1->count);
  {
    auto [slot, found] = leaf1->lower_bound(oid1);
    ASSERT_TRUE(found);
    ASSERT_EQ(0, slot);
  }
  {
    leaf2->insert_at(0, oid2, *flattened);
    auto [slot, found] = leaf2->lower_bound(oid2);
    ASSERT_TRUE(found);
    ASSERT_EQ(0, slot);
  }
  {
    leaf2->insert_at(1, oid3, *flattened);
    auto [slot, found] = leaf2->lower_bound(oid3);
    ASSERT_TRUE(found);
    ASSERT_EQ(1, slot);
  }
  ASSERT_EQ(2, leaf2->count);

  // normally we let left merge right, so we just need to remove an
  // entry in parent, let's keep this convention here
  auto mover = make_mover(dummy_parent, *leaf2, *leaf1, 0);
  // just grab a single item from right
  mover.move_from(0, 1, 1);
  auto to_delta = mover.to_delta();
  ASSERT_EQ(to_delta.op_t::insert_back, to_delta.op);
  leaf1->insert_back(std::move(to_delta.keys), std::move(to_delta.cells));

  ASSERT_EQ(2, leaf1->count);
  {
    auto [slot, found] = leaf1->lower_bound(oid2);
    ASSERT_TRUE(found);
    ASSERT_EQ(1, slot);
  }

  auto from_delta = mover.from_delta();
  ASSERT_EQ(from_delta.op_t::shift_left, from_delta.op);
  leaf2->shift_left(from_delta.n, 0);
  ASSERT_EQ(1, leaf2->count);
}

TEST(OnodeNode, merge_right)
{
  static constexpr size_t BLOCK_SIZE = 512;
  char buf1[BLOCK_SIZE];
  char buf2[BLOCK_SIZE];
  using leaf_node_0 = node_t<BLOCK_SIZE, 0, ntype_t::leaf>;
  auto leaf1 = new (buf1) leaf_node_0;
  auto leaf2 = new (buf2) leaf_node_0;
  auto& dummy_parent = leaf1;

  ghobject_t oid1{hobject_t{object_t{"earth"}, "", 0, 0, 0, "solar"}};
  ghobject_t oid2{hobject_t{object_t{"jupiter"}, "", 0, 0, 0, "solar"}};
  ghobject_t oid3{hobject_t{object_t{"saturn"}, "", 0, 0, 0, "solar"}};
  Onode onode{"hello"};
  bufferlist bl;
  ceph::encode(onode, bl);
  bl.rebuild();
  auto flattened = reinterpret_cast<const onode_t*>(bl.c_str());
  // so they are ordered as they should
  leaf1->insert_at(0, oid1, *flattened);
  ASSERT_EQ(1, leaf1->count);
  {
    auto [slot, found] = leaf1->lower_bound(oid1);
    ASSERT_TRUE(found);
    ASSERT_EQ(0, slot);
  }
  {
    leaf2->insert_at(0, oid2, *flattened);
    auto [slot, found] = leaf2->lower_bound(oid2);
    ASSERT_TRUE(found);
    ASSERT_EQ(0, slot);
  }
  {
    leaf2->insert_at(1, oid3, *flattened);
    auto [slot, found] = leaf2->lower_bound(oid3);
    ASSERT_TRUE(found);
    ASSERT_EQ(1, slot);
  }
  ASSERT_EQ(2, leaf2->count);

  // normally we let left merge right, so we just need to remove an
  // entry in parent, let's keep this convention here
  auto mover = make_mover(dummy_parent, *leaf2, *leaf1, 0);
  // just grab a single item from right
  mover.move_from(0, 1, 2);
  auto to_delta = mover.to_delta();
  ASSERT_EQ(to_delta.op_t::insert_back, to_delta.op);
  leaf1->insert_back(std::move(to_delta.keys), std::move(to_delta.cells));

  ASSERT_EQ(3, leaf1->count);
  {
    auto [slot, found] = leaf1->lower_bound(oid2);
    ASSERT_TRUE(found);
    ASSERT_EQ(1, slot);
  }
  {
    auto [slot, found] = leaf1->lower_bound(oid3);
    ASSERT_TRUE(found);
    ASSERT_EQ(2, slot);
  }

  // its onode tree's responsibility to retire the node
  auto from_delta = mover.from_delta();
  ASSERT_EQ(from_delta.op_t::nop, from_delta.op);
}

TEST(OnodeNode, remove_basic)
{
  static constexpr size_t BLOCK_SIZE = 512;
  char buf[BLOCK_SIZE];
  using leaf_node_0 = node_t<BLOCK_SIZE, 0, ntype_t::leaf>;
  auto leaf = new (buf) leaf_node_0;
  ghobject_t oid{hobject_t{object_t{"saturn"}, "", 0, 0, 0, "solar"}};
  {
    auto [slot, found] = leaf->lower_bound(oid);
    ASSERT_FALSE(found);
    ASSERT_EQ(0, slot);
  }
  Onode onode{"hello"};
  bufferlist bl;
  ceph::encode(onode, bl);
  bl.rebuild();
  auto flattened = reinterpret_cast<const onode_t*>(bl.c_str());
  leaf->insert_at(0, oid, *flattened);
  {
    auto [slot, found] = leaf->lower_bound(oid);
    ASSERT_TRUE(found);
    ASSERT_EQ(0, slot);
    leaf->remove_from(slot);
  }
  {
    auto [slot, found] = leaf->lower_bound(oid);
    ASSERT_FALSE(found);
  }
}
