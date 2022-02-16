// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "simple_bitmap.h"

#include "include/ceph_assert.h"
#include "bluestore_types.h"
#include "common/debug.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << __func__ << "::SBMAP::" << this << " "

static struct extent_t null_extent = {0, 0};

//----------------------------------------------------------------------------
//throw bad_alloc
SimpleBitmap::SimpleBitmap(CephContext *_cct, uint64_t num_bits) :cct(_cct)
{
  m_num_bits   = num_bits;
  m_word_count = bits_to_words(num_bits);
  if (num_bits & BITS_IN_WORD_MASK) {
    m_word_count++;
  }
  m_arr = new uint64_t [m_word_count];
  clear_all();
}

//----------------------------------------------------------------------------
SimpleBitmap::~SimpleBitmap()
{
  delete [] m_arr;
}

//----------------------------------------------------------------------------
bool SimpleBitmap::set(uint64_t offset, uint64_t length)
{
  dout(20) <<" [" << std::hex << offset << ", " << length << "]" << dendl;

  if (offset + length >= m_num_bits) {
    derr << __func__ << "::offset + length = " << offset + length << " exceeds map size = " << m_num_bits << dendl;
    ceph_assert(offset + length < m_num_bits);
    return false;
  }

  auto [word_index, first_bit_set] = split(offset);
  // special case optimization
  if (length == 1) {
    uint64_t set_mask  = 1ULL << first_bit_set;
    m_arr[word_index] |= set_mask;
    return true;
  }

  // handle the first word which might be incomplete
  if (first_bit_set != 0) {
    uint64_t   set_mask      = FULL_MASK << first_bit_set;
    uint64_t   first_bit_clr = first_bit_set + length;
    if (first_bit_clr <= BITS_IN_WORD) {
      if (first_bit_clr < BITS_IN_WORD) {
	uint64_t clr_bits = BITS_IN_WORD - first_bit_clr;
	uint64_t clr_mask = FULL_MASK >> clr_bits;
	set_mask     &= clr_mask;
      }
      m_arr[word_index] |= set_mask;
      return true;
    } else {
      // set all bits in this word starting from first_bit_set
      m_arr[word_index] |= set_mask;
      word_index ++;
      length -= (BITS_IN_WORD - first_bit_set);
    }
  }

  // set a range of full words
  uint64_t full_words_count = bits_to_words(length);
  uint64_t end              = word_index + full_words_count;
  for (; word_index < end; word_index++) {
    m_arr[word_index] = FULL_MASK;
  }
  length -= words_to_bits(full_words_count);

  // set bits in the last word
  if (length) {
    uint64_t set_mask = ~(FULL_MASK << length);
    m_arr[word_index] |= set_mask;
  }

  return true;
}

//----------------------------------------------------------------------------
bool SimpleBitmap::clr(uint64_t offset, uint64_t length)
{
  if (offset + length >= m_num_bits) {
    derr << __func__ << "::offset + length = " << offset + length << " exceeds map size = " << m_num_bits << dendl;
    ceph_assert(offset + length < m_num_bits);
    return false;
  }

  auto [word_index, first_bit_clr] = split(offset);
  // special case optimization
  if (length == 1) {
    uint64_t set_mask   = 1ULL << first_bit_clr;
    uint64_t clr_mask   = ~set_mask;
    m_arr[word_index] &= clr_mask;

    return true;
  }

  // handle the first word when we we are unaligned on word boundaries
  if (first_bit_clr != 0) {
    uint64_t clr_mask      = ~(FULL_MASK << first_bit_clr);
    uint64_t first_bit_set = first_bit_clr + length;
    // special case - we only work on a single word
    if (first_bit_set <= BITS_IN_WORD) {
      if (first_bit_set < BITS_IN_WORD) {
	uint64_t set_mask = FULL_MASK << first_bit_set;
	clr_mask         |= set_mask;
      }
      m_arr[word_index]     &= clr_mask;
      return true;
    }
    else {
      // clear all bits in this word starting from first_bit_clr
      // and continue to the next word
      m_arr[word_index] &= clr_mask;
      word_index ++;
      length -= (BITS_IN_WORD - first_bit_clr);
    }
  }


  // clear a range of full words
  uint64_t full_words_count = bits_to_words(length);
  uint64_t end              = word_index + full_words_count;
  for (; word_index < end; word_index++) {
    m_arr[word_index] = 0;
  }
  length -= words_to_bits(full_words_count);

  // set bits in the last word
  if (length) {
    uint64_t clr_mask = (FULL_MASK << length);
    m_arr[word_index] &= clr_mask;
  }

  return true;
}

//----------------------------------------------------------------------------
extent_t SimpleBitmap::get_next_set_extent(uint64_t offset)
{
  if (offset >= m_num_bits ) {
    return null_extent;
  }

  auto [word_idx, bits_to_clear] = split(offset);
  uint64_t word     = m_arr[word_idx];
  word &= (FULL_MASK << bits_to_clear);

  // if there are no set bits in this word
  if (word == 0) {
      // skip past all clear words
    while (++word_idx < m_word_count && !m_arr[word_idx]);

    if (word_idx < m_word_count ) {
      word = m_arr[word_idx];
    } else {
      return null_extent;
    }
  }

  // ffs is 1 based, must dec by one as we are zero based
  int           ffs = __builtin_ffsll(word) - 1;
  extent_t      ext;
  ext.offset = words_to_bits(word_idx) + ffs;

  // set all bits from current to LSB
  uint64_t      clr_mask = FULL_MASK << ffs;
  uint64_t      set_mask = ~clr_mask;
  word |= set_mask;

  // skipped past fully set words
  if (word == FULL_MASK) {
    while ( (++word_idx < m_word_count) && (m_arr[word_idx] == FULL_MASK) );

    if (word_idx < m_word_count) {
      word = m_arr[word_idx];
    } else {
      // bitmap is set from ext.offset until the last bit
      ext.length = (m_num_bits - ext.offset);
      return ext;
    }
  }

  ceph_assert(word != FULL_MASK);
  int      ffz     = __builtin_ffsll(~word) - 1;
  uint64_t zoffset = words_to_bits(word_idx) + ffz;
  ext.length       = (zoffset - ext.offset);

  return ext;
}

//----------------------------------------------------------------------------
extent_t SimpleBitmap::get_next_clr_extent(uint64_t offset)
{
  if (offset >= m_num_bits ) {
    return null_extent;
  }

  uint64_t word_idx = offset_to_index(offset);
  uint64_t word     = m_arr[word_idx];

  // set all bit set before offset
  offset &= BITS_IN_WORD_MASK;
  if (offset != 0) {
    uint64_t bits_to_set = BITS_IN_WORD - offset;
    uint64_t set_mask    = FULL_MASK >> bits_to_set;
    word |= set_mask;
  }
  if (word == FULL_MASK) {
    // skipped past fully set words
    while ( (++word_idx < m_word_count) && (m_arr[word_idx] == FULL_MASK) );

    if (word_idx < m_word_count) {
      word = m_arr[word_idx];
    } else {
      dout(10) << "2)Reached the end of the bitmap" << dendl;
      return null_extent;
    }
  }

  int      ffz = __builtin_ffsll(~word) - 1;
  extent_t ext;
  ext.offset = words_to_bits(word_idx) + ffz;

  // clear all bits from current position to LSB
  word &= (FULL_MASK << ffz);

  // skip past all clear words
  if (word == 0) {
    while ( (++word_idx < m_word_count) && (m_arr[word_idx] == 0) );

    if (word_idx < m_word_count) {
      word = m_arr[word_idx];
    } else {
      // bitmap is set from ext.offset until the last bit
      ext.length = (m_num_bits - ext.offset);
      return ext;
    }
  }

  // ffs is 1 based, must dec by one as we are zero based
  int           ffs     = __builtin_ffsll(word) - 1;
  uint64_t      soffset = words_to_bits(word_idx) + ffs;
  ext.length = (soffset - ext.offset);
  return ext;
}
