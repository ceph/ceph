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

    // skip forward to the min chunk size cut point (minus the window, so
    // we can initialize the rolling fingerprint).
    pos = std::min(pos + (1 << min_bits) - window, len);

    // first fill the window
    while (pos < std::min(window, len)) {
      fp = (fp << 1) ^ table[ptr[pos]];
      ++pos;
    }
    if (pos >= len) {
      chunks->push_back(std::pair<uint64_t,uint64_t>(cstart, pos - cstart));
      break;
    }

    // find an end marker
    // small
    size_t max = std::min(len,
			  cstart + (1 << (target_bits - TARGET_WINDOW_BITS)));
    while ((fp & small_mask) != small_mask && pos < max) {
      fp = (fp << 1) ^ table[ptr[pos]];
      ++pos;
    }
    if (pos >= max) {
      // target
      max = std::min(len, cstart + (1 << (target_bits + TARGET_WINDOW_BITS)));
      while ((fp & target_mask) != target_mask && pos < max) {
	fp = (fp << 1) ^ table[ptr[pos]];
	++pos;
      }
      if (pos >= max) {
	// large
	max = std::min(len, cstart + (1 << max_bits));
	while ((fp & large_mask) != large_mask && pos < max) {
	  fp = (fp << 1) ^ table[ptr[pos]];
	  ++pos;
	}
      }
    }

    chunks->push_back(std::pair<uint64_t,uint64_t>(cstart, pos - cstart));
  }
}
