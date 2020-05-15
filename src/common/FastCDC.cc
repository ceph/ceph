// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include "FastCDC.h"
#include "rabin.h"


// if we are close to the target, use the target mask.  if we are very
// small or very large, use an adjusted mask.  this tries to keep /most/
// cut points using the same mask.

//  how many bits to set/clear in the small/large masks
#define TARGET_WINDOW_MASK_BITS  2

//  how big the 'target window' is (in which we use the target mask)
#define TARGET_WINDOW_BITS       1

//  hard limits on size
#define SIZE_WINDOW_BITS         2

void FastCDC::_setup(int target, int size_window_bits)
{
  target_bits = target;

  if (!size_window_bits) {
    size_window_bits = SIZE_WINDOW_BITS;
  }
  min_bits = target - size_window_bits;
  max_bits = target + size_window_bits;

  std::mt19937_64 engine;

  // prefill table
  for (unsigned i = 0; i < 256; ++i) {
    table[i] = engine();
  }

  // set mask
  int did = 0;
  uint64_t m = 0;
  while (did < target_bits + TARGET_WINDOW_MASK_BITS) {
    uint64_t bit = 1ull << (engine() & 63);
    if (m & bit) {
      continue;	// this bit is already set
    }
    m |= bit;
    ++did;
    if (did == target_bits - TARGET_WINDOW_MASK_BITS) {
      large_mask = m;
    } else if (did == target_bits) {
      target_mask = m;
    } else if (did == target_bits + TARGET_WINDOW_MASK_BITS) {
      small_mask = m;
    }
  }
}

static inline bool _scan(
  unsigned char *ptr, size_t& pos, size_t max, uint64_t mask, uint64_t& fp,
  uint64_t *table)
{
  while (true) {
    if ((fp & mask) == mask) {
      return false;
    }
    if (pos >= max) {
      return true;
    }
    fp = (fp << 1) ^ table[ptr[pos]];
    ++pos;
  }
}

void FastCDC::calc_chunks(
  bufferlist& bl,
  std::vector<std::pair<uint64_t, uint64_t>> *chunks)
{
  unsigned char *ptr = (unsigned char *)bl.c_str();
  size_t len = bl.length();

  size_t pos = 0;
  while (pos < len) {
    size_t cstart = pos;
    uint64_t fp = 0;

    // are we left with a <= min chunk?
    if (len - pos <= (1ul << min_bits)) {
      chunks->push_back(std::pair<uint64_t,uint64_t>(pos, len - pos));
      break;
    }

    // skip forward to the min chunk size cut point (minus the window, so
    // we can initialize the rolling fingerprint).
    pos += (1 << min_bits) - window;

    // first fill the window
    size_t max = pos + window;
    while (pos < max) {
      fp = (fp << 1) ^ table[ptr[pos]];
      ++pos;
    }
    ceph_assert(pos < len);

    // find an end marker
    if (_scan(ptr, pos,
	      std::min(len,
		       cstart + (1 << (target_bits - TARGET_WINDOW_BITS))),
	      small_mask, fp, table) &&
	_scan(ptr, pos,
	      std::min(len,
		       cstart + (1 << (target_bits + TARGET_WINDOW_BITS))),
	      target_mask, fp, table) &&
	_scan(ptr, pos,
	      std::min(len,
		       cstart + (1 << max_bits)),
	      large_mask, fp, table)) ;

    chunks->push_back(std::pair<uint64_t,uint64_t>(cstart, pos - cstart));
  }
}
