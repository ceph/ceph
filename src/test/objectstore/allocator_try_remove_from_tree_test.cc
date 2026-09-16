// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Regression tests for _try_remove_from_tree() in the AVL / Btree / Btree2
 * allocators.  See https://tracker.ceph.com/issues/80417.
 *
 */

#include <vector>
#include <gtest/gtest.h>

#include "global/global_context.h"
#include "os/bluestore/AvlAllocator.h"

namespace {

constexpr uint64_t _64k = 64 * 1024;   // per-run granularity used below
constexpr uint64_t block_size = 0x1000; // 4 KiB
constexpr uint64_t device_size = 1ull << 30; // 1 GiB

struct cb_rec_t {
  uint64_t offset;
  uint64_t length;
  bool found;
  bool operator==(const cb_rec_t&) const = default;
};

std::ostream& operator<<(std::ostream& os, const cb_rec_t& r) {
  return os << "{0x" << std::hex << r.offset << "~0x" << r.length << std::dec
            << (r.found ? " found" : " missing") << "}";
}

// AvlAllocator::_try_remove_from_tree is protected - re-expose it.
class TestAvlAllocator : public AvlAllocator {
public:
  using AvlAllocator::AvlAllocator;
  using AvlAllocator::_try_remove_from_tree;
};

template <class Alloc>
std::vector<cb_rec_t> collect(Alloc& a, uint64_t start, uint64_t size)
{
  std::vector<cb_rec_t> recs;
  auto cb = [&](uint64_t o, uint64_t l, bool found) {
    recs.push_back({o, l, found});
  };
  a._try_remove_from_tree(start, size, cb);
  return recs;
}

template <class Alloc>
std::vector<std::pair<uint64_t, uint64_t>> extents(Alloc& a)
{
  std::vector<std::pair<uint64_t, uint64_t>> out;
  a.foreach([&](uint64_t o, uint64_t l) { out.emplace_back(o, l); });
  return out;
}

} // namespace

/*
 * AVL: range_tree uses an overlap comparator, so every free run overlapping
 * the query compares equal to it.  Check that a query spanning several
 * disjoint free runs still starts from the leftmost one and visits all of
 * them in address order: find() is lower_bound() plus an equality check, so
 * it returns the leftmost overlapping run rather than whichever one a tree
 * traversal happens to reach first.
 *
 * Runs are inserted right-to-left so the run that begins first is not the
 * root of the tree.
 */
TEST(AvlAllocator, try_remove_spanning_disjoint_runs)
{
  TestAvlAllocator a(g_ceph_context, device_size, block_size, "avl");
  a.init_add_free(40 * _64k, 10 * _64k);   // C = [40, 50)
  a.init_add_free(20 * _64k, 10 * _64k);   // B = [20, 30)
  a.init_add_free(0 * _64k, 10 * _64k);    // A = [0, 10)

  const uint64_t start = 5 * _64k;
  const uint64_t size = 40 * _64k;      // query [5, 45): trims A and C, eats B
  const uint64_t before = a.get_free();

  auto recs = collect(a, start, size);
  const uint64_t freed = before - a.get_free();

  EXPECT_EQ((std::vector<cb_rec_t>{
    {5 * _64k, 5 * _64k, true},     // A tail [5, 10)
    {10 * _64k, 10 * _64k, false},  // gap [10, 20)
    {20 * _64k, 10 * _64k, true},   // B [20, 30)
    {30 * _64k, 10 * _64k, false},  // gap [30, 40)
    {40 * _64k, 5 * _64k, true},    // C head [40, 45)
  }), recs);
  EXPECT_EQ(20u * _64k, freed);
  EXPECT_EQ((std::vector<std::pair<uint64_t, uint64_t>>{
    {0, 5 * _64k}, {45 * _64k, 5 * _64k},
  }), extents(a));
}

// A query fully inside a single run must trim it on both sides and touch
// nothing else.
TEST(AvlAllocator, try_remove_inside_single_run)
{
  TestAvlAllocator a(g_ceph_context, device_size, block_size, "avl");
  a.init_add_free(0, 10 * _64k);
  a.init_add_free(20 * _64k, 10 * _64k);

  auto recs = collect(a, 2 * _64k, 5 * _64k);   // [2, 7) inside [0, 10)
  EXPECT_EQ((std::vector<cb_rec_t>{{2 * _64k, 5 * _64k, true}}), recs);
  EXPECT_EQ((std::vector<std::pair<uint64_t, uint64_t>>{
    {0, 2 * _64k}, {7 * _64k, 3 * _64k}, {20 * _64k, 10 * _64k},
  }), extents(a));
}
