// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Allocator.h"
#include "AllocatorBase.h"
#include <bit>
#include "StupidAllocator.h"
#include "BitmapAllocator.h"
#include "AvlAllocator.h"
#include "BtreeAllocator.h"
#include "Btree2Allocator.h"
#include "HybridAllocator.h"
#include "common/debug.h"
#include "common/admin_socket.h"

#define dout_subsys ceph_subsys_bluestore
using TOPNSPC::common::cmd_getval;

using std::string;
using std::to_string;

using ceph::bufferlist;
using ceph::Formatter;


Allocator::Allocator(std::string_view name,
                     int64_t _capacity,
                     int64_t _block_size)
 : device_size(_capacity),
   block_size(_block_size)
{}


Allocator::~Allocator()
{}


Allocator *Allocator::create(
  CephContext* cct,
  std::string_view type,
  int64_t size,
  int64_t block_size,
  std::string_view name)
{
  Allocator* alloc = nullptr;
  if (type == "stupid") {
    alloc = new StupidAllocator(cct, size, block_size, name);
  } else if (type == "bitmap") {
    alloc = new BitmapAllocator(cct, size, block_size, name);
  } else if (type == "avl") {
    return new AvlAllocator(cct, size, block_size, name);
  } else if (type == "btree") {
    return new BtreeAllocator(cct, size, block_size, name);
  } else if (type == "hybrid") {
    return new HybridAvlAllocator(cct, size, block_size,
      cct->_conf.get_val<uint64_t>("bluestore_hybrid_alloc_mem_cap"),
      name);
  }  else if (type == "hybrid_btree2") {
    return new HybridBtree2Allocator(cct, size, block_size,
      cct->_conf.get_val<uint64_t>("bluestore_hybrid_alloc_mem_cap"),
      cct->_conf.get_val<double>("bluestore_btree2_alloc_weight_factor"),
      name);
  }
  if (alloc == nullptr) {
    lderr(cct) << "Allocator::" << __func__ << " unknown alloc type "
	     << type << dendl;
  }
  return alloc;
}

void Allocator::release(const PExtentVector& release_vec)
{
  release_set_t release_set;
  for (auto e : release_vec) {
    release_set.insert(e.offset, e.length);
  }
  release(release_set);
}

/**
 * Gives fragmentation a numeric value.
 *
 * Following algorithm applies value to each existing free unallocated block.
 * Value of single block is a multiply of size and per-byte-value.
 * Per-byte-value is greater for larger blocks.
 * Assume block size X has value per-byte p; then block size 2*X will have per-byte value 1.1*p.
 *
 * This could be expressed in logarithms, but for speed this is interpolated inside ranges.
 * [1]  [2..3] [4..7] [8..15] ...
 * ^    ^      ^      ^
 * 1.1  1.1^2  1.1^3  1.1^4 ...
 *
 * Final score is obtained by proportion between score that would have been obtained
 * in condition of absolute fragmentation and score in no fragmentation at all.
 */
double Allocator::get_fragmentation_score()
{
  // this value represents how much worth is 2X bytes in one chunk then in X + X bytes
  static const double double_size_worth_small = 1.2;
  // chunks larger then 128MB are large enough that should be counted without penalty
  static const double double_size_worth_huge = 1;
  static const size_t small_chunk_p2 = 20; // 1MB
  static const size_t huge_chunk_p2 = 27; // 128MB
  // for chunks 1MB - 128MB penalty coeffs are linearly weighted 1.2 (at small) ... 1 (at huge)
  static std::vector<double> scales{1};
  double score_sum = 0;
  size_t sum = 0;

  auto get_score = [&](size_t v) -> double {
    size_t sc = sizeof(v) * 8 - std::countl_zero(v) - 1; //assign to grade depending on log2(len)
    while (scales.size() <= sc + 1) {
      //unlikely expand scales vector
      auto ss = scales.size();
      double scale = double_size_worth_small;
      if (ss >= huge_chunk_p2) {
	scale = double_size_worth_huge;
      } else if (ss > small_chunk_p2) {
	// linear decrease 1.2 ... 1
	scale = (double_size_worth_huge * (ss - small_chunk_p2) + double_size_worth_small * (huge_chunk_p2 - ss)) /
	  (huge_chunk_p2 - small_chunk_p2);
      }
      scales.push_back(scales[scales.size() - 1] * scale);
    }
    size_t sc_shifted = size_t(1) << sc;
    double x = double(v - sc_shifted) / sc_shifted; //x is <0,1) in its scale grade
    // linear extrapolation in its scale grade
    double score = (sc_shifted    ) * scales[sc]   * (1-x) +
                   (sc_shifted * 2) * scales[sc+1] * x;
    return score;
  };

  auto iterated_allocation = [&](size_t off, size_t len) {
    ceph_assert(len > 0);
    score_sum += get_score(len);
    sum += len;
  };
  foreach(iterated_allocation);

  double ideal = get_score(sum);
  double terrible = (sum / block_size) * get_score(block_size);
  return (ideal - score_sum) / (ideal - terrible);
}

