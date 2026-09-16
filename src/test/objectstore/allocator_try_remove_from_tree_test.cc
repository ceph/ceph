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
#include "os/bluestore/BtreeAllocator.h"

namespace {

constexpr uint64_t _64k = 64 * 1024;   // per-run granularity used below
constexpr uint64_t block_size = 0x1000; // 4 KiB
constexpr uint64_t device_size = 1ull << 30; // 1 GiB

// Print [offset, offset + length) in _64k units, e.g. "[5,10)", to match the
// comments in the tests.  Falls back to hex if not aligned to _64k.
void print_range(std::ostream& os, uint64_t offset, uint64_t length) {
  const uint64_t end = offset + length;
  if (offset % _64k == 0 && end % _64k == 0) {
    os << "[" << offset / _64k << "," << end / _64k << ")";
  } else {
    os << "[0x" << std::hex << offset << ",0x" << end << std::dec << ")";
  }
}

struct cb_rec_t {
  uint64_t offset;
  uint64_t length;
  bool found;
  bool operator==(const cb_rec_t&) const = default;
};

std::ostream& operator<<(std::ostream& os, const cb_rec_t& r) {
  os << "{";
  print_range(os, r.offset, r.length);
  return os << (r.found ? " found" : " missing") << "}";
}

struct extent_t {
  uint64_t offset;
  uint64_t length;
  bool operator==(const extent_t&) const = default;
};

std::ostream& operator<<(std::ostream& os, const extent_t& e) {
  print_range(os, e.offset, e.length);
  return os;
}

// AvlAllocator::_try_remove_from_tree is protected - re-expose it.
class TestAvlAllocator : public AvlAllocator {
public:
  using AvlAllocator::AvlAllocator;
  using AvlAllocator::_try_remove_from_tree;
};

// BtreeAllocator::_try_remove_from_tree is protected - re-expose it.
class TestBtreeAllocator : public BtreeAllocator {
public:
  using BtreeAllocator::BtreeAllocator;
  using BtreeAllocator::_try_remove_from_tree;
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
std::vector<extent_t> extents(Alloc& a)
{
  std::vector<extent_t> out;
  a.foreach([&](uint64_t o, uint64_t l) { out.push_back({o, l}); });
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
  EXPECT_EQ((std::vector<extent_t>{
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
  EXPECT_EQ((std::vector<extent_t>{
    {0, 2 * _64k}, {7 * _64k, 3 * _64k}, {20 * _64k, 10 * _64k},
  }), extents(a));
}

/*
 * Btree (btree_map): _try_remove_from_tree() used range_tree.find(start), an
 * exact-key lookup that returns end() unless a run begins exactly at 'start'.
 * A run beginning before 'start' and reaching into the query was missed
 * entirely and the whole query reported as not-free.
 *
 * Runs [100,200) and [300,350); query [150,180) sits wholly inside the first.
 */
TEST(BtreeAllocator, try_remove_run_starting_before_query)
{
  TestBtreeAllocator a(g_ceph_context, device_size, block_size, "btree");
  a.init_add_free(100 * _64k, 100 * _64k);
  a.init_add_free(300 * _64k, 50 * _64k);

  const uint64_t start = 150 * _64k;
  const uint64_t size = 30 * _64k;          // [150, 180)
  const uint64_t before = a.get_free();

  auto recs = collect(a, start, size);
  const uint64_t freed = before - a.get_free();

  EXPECT_EQ((std::vector<cb_rec_t>{{150 * _64k, 30 * _64k, true}}), recs);
  EXPECT_EQ(30u * _64k, freed);
  EXPECT_EQ((std::vector<extent_t>{
    {100 * _64k, 50 * _64k}, {180 * _64k, 20 * _64k}, {300 * _64k, 50 * _64k},
  }), extents(a));
}

/*
 * Btree: the successor iterator was cached before _process_range_removal(),
 * which can erase from / emplace into range_tree and invalidate it.  The fix
 * has _process_range_removal() return range_tree.lower_bound(end) - an
 * iterator derived after its own mutations - and the caller advances via that.
 *
 * Runs [0,10),[20,30),[40,50); query [0,45).  The query starts exactly at the
 * first run, so the lookup is not involved.  [0,10) is erased whole, which
 * shifts the remaining slots of the btree leaf down by one: a cached
 * successor iterator would then land on [40,50) and skip [20,30), reporting
 * it as not free.
 */
TEST(BtreeAllocator, try_remove_iterator_stays_valid_across_removal)
{
  TestBtreeAllocator a(g_ceph_context, device_size, block_size, "btree");
  a.init_add_free(0, 10 * _64k);
  a.init_add_free(20 * _64k, 10 * _64k);
  a.init_add_free(40 * _64k, 10 * _64k);

  const uint64_t start = 0;
  const uint64_t size = 45 * _64k;          // [0, 45)
  const uint64_t before = a.get_free();

  auto recs = collect(a, start, size);
  const uint64_t freed = before - a.get_free();

  EXPECT_EQ((std::vector<cb_rec_t>{
    {0, 10 * _64k, true},           // [0, 10)
    {10 * _64k, 10 * _64k, false},  // gap [10, 20)
    {20 * _64k, 10 * _64k, true},   // [20, 30)
    {30 * _64k, 10 * _64k, false},  // gap [30, 40)
    {40 * _64k, 5 * _64k, true},    // [40, 45)
  }), recs);
  EXPECT_EQ(25u * _64k, freed);
  EXPECT_EQ((std::vector<extent_t>{
    {45 * _64k, 5 * _64k},
  }), extents(a));
}

// Query starting past every free run: single not-found record, tree untouched.
TEST(BtreeAllocator, try_remove_entirely_past_free_runs)
{
  TestBtreeAllocator a(g_ceph_context, device_size, block_size, "btree");
  a.init_add_free(0, 10 * _64k);
  a.init_add_free(20 * _64k, 10 * _64k);

  auto recs = collect(a, 30 * _64k, 10 * _64k);   // [30, 40): nothing free
  EXPECT_EQ((std::vector<cb_rec_t>{{30 * _64k, 10 * _64k, false}}), recs);
  EXPECT_EQ((std::vector<extent_t>{
    {0, 10 * _64k}, {20 * _64k, 10 * _64k},
  }), extents(a));
}
