// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Allocator.h"
#include "StupidAllocator.h"
#include "BitmapAllocator.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore

Allocator::Allocator(const std::string& name)
{
  asok_hook = new SocketHook(this, name);
}


Allocator::~Allocator()
{
}


Allocator *Allocator::create(CephContext* cct, string type,
                             int64_t size, int64_t block_size, const std::string& name)
{
  Allocator* alloc = nullptr;
  if (type == "stupid") {
    alloc = new StupidAllocator(cct, name);
  } else if (type == "bitmap") {
    alloc = new BitmapAllocator(cct, size, block_size, name);
  }
  if (alloc == nullptr) {
    lderr(cct) << "Allocator::" << __func__ << " unknown alloc type "
	     << type << dendl;
  }
  return alloc;
}

void Allocator::release(const PExtentVector& release_vec)
{
  interval_set<uint64_t> release_set;
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
  static const double double_size_worth = 1.1 ;
  std::vector<double> scales{1};
  double score_sum = 0;
  size_t sum = 0;

  auto get_score = [&](size_t v) -> double {
    size_t sc = sizeof(v) * 8 - clz(v) - 1; //assign to grade depending on log2(len)
    while (scales.size() <= sc + 1) {
      //unlikely expand scales vector
      scales.push_back(scales[scales.size() - 1] * double_size_worth);
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
  dump(iterated_allocation);


  double ideal = get_score(sum);
  double terrible = sum * get_score(1);
  return (ideal - score_sum) / (ideal - terrible);
}
