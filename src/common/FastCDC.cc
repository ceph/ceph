// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include "FastCDC.h"


// Unlike FastCDC described in the paper, if we are close to the
// target, use the target mask.  If we are very small or very large,
// use an adjusted mask--like the paper.  This tries to keep more
// cut points using the same mask, and fewer using the small or large
// masks.

// How many more/fewer bits to set in the small/large masks.
//
// This is the "normalization level" or "NC level" in the FastCDC
// paper.
#define TARGET_WINDOW_MASK_BITS  2

// How big the 'target window' is (in which we use the target mask).
//
// In the FastCDC paper, this is always 0: there is not "target
// window," and either small_mask (maskS) or large_mask (maskL) is
// used--never target_mask (maskA).
#define TARGET_WINDOW_BITS       1

// How many bits larger/smaller than target for hard limits on chunk
// size.
//
// We assume the min and max sizes are always this many bits
// larger/smaller than the target.  (Note that the FastCDC paper 8KB
// example has a min of 2KB (2 bits smaller) and max of 64 KB (3 bits
// larger), although it is not clear why they chose those values.)
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
  // these are our cursor/postion...
  bufferlist::buffers_t::const_iterator *p,
  const char **pp, const char **pe,
  size_t& pos,
  size_t max,   // how much to read
  uint64_t& fp, // fingerprint
  uint64_t mask, const uint64_t *table)
{
  while (pos < max) {
    if (*pp == *pe) {
      ++(*p);
      *pp = (*p)->c_str();
      *pe = *pp + (*p)->length();
    }
    const char *te = std::min(*pe, *pp + max - pos);
    for (; *pp < te; ++(*pp), ++pos) {
      if ((fp & mask) == mask) {
	return false;
      }
      fp = (fp << 1) ^ table[*(unsigned char*)*pp];
    }
    if (pos >= max) {
      return true;
    }
  }
  return true;
}

void FastCDC::calc_chunks(
  const bufferlist& bl,
  std::vector<std::pair<uint64_t, uint64_t>> *chunks) const
{
  if (bl.length() == 0) {
    return;
  }
  auto p = bl.buffers().begin();
  const char *pp = p->c_str();
  const char *pe = pp + p->length();

  size_t pos = 0;
  size_t len = bl.length();
  while (pos < len) {
    size_t cstart = pos;
    uint64_t fp = 0;

    // are we left with a min-sized (or smaller) chunk?
    if (len - pos <= (1ul << min_bits)) {
      chunks->push_back(std::pair<uint64_t,uint64_t>(pos, len - pos));
      break;
    }

    // skip forward to the min chunk size cut point (minus the window, so
    // we can initialize the rolling fingerprint).
    size_t skip = (1 << min_bits) - window;
    pos += skip;
    while (skip) {
      size_t s = std::min<size_t>(pe - pp, skip);
      skip -= s;
      pp += s;
      if (pp == pe) {
	++p;
	pp = p->c_str();
	pe = pp + p->length();
      }
    }

    // first fill the window
    size_t max = pos + window;
    while (pos < max) {
      if (pp == pe) {
	++p;
	pp = p->c_str();
	pe = pp + p->length();
      }
      const char *te = std::min(pe, pp + (max - pos));
      for (; pp < te; ++pp, ++pos) {
	fp = (fp << 1) ^ table[*(unsigned char*)pp];
      }
    }
    ceph_assert(pos < len);

    // find an end marker
    if (
      // for the first "small" region
      _scan(&p, &pp, &pe, pos,
	    std::min(len, cstart + (1 << (target_bits - TARGET_WINDOW_BITS))),
	    fp, small_mask, table) &&
      // for the middle range (close to our target)
      (TARGET_WINDOW_BITS == 0 ||
       _scan(&p, &pp, &pe, pos,
	     std::min(len, cstart + (1 << (target_bits + TARGET_WINDOW_BITS))),
	     fp, target_mask, table)) &&
      // we're past target, use large_mask!
      _scan(&p, &pp, &pe, pos,
	    std::min(len,
		     cstart + (1 << max_bits)),
	    fp, large_mask, table))
      ;

    chunks->push_back(std::pair<uint64_t,uint64_t>(cstart, pos - cstart));
  }
}
