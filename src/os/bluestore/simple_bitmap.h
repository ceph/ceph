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
#pragma once
#include <cstdint>
#include <iostream>
#include <string>
#include <cstring>
#include <cmath>
#include <iomanip>

#include "include/ceph_assert.h"

struct extent_t {
  uint64_t offset;
  uint64_t length;
  bool operator==(const extent_t& other) const {
    return (this->offset == other.offset && this->length == other.length);
  }
};

class SimpleBitmap {
public:
  SimpleBitmap(CephContext *_cct, uint64_t num_bits);
  ~SimpleBitmap();

  SimpleBitmap(const SimpleBitmap&) = delete;
  SimpleBitmap& operator=(const SimpleBitmap&) = delete;


  // set a bit range range of @length starting at @offset
  bool     set(uint64_t offset, uint64_t length);
  // clear a bit range range of @length starting at @offset
  bool     clr(uint64_t offset, uint64_t length);

  // returns a copy of the next set extent starting at @offset
  extent_t get_next_set_extent(uint64_t offset);

  // returns a copy of the next clear extent starting at @offset
  extent_t get_next_clr_extent(uint64_t offset);

  //----------------------------------------------------------------------------
  inline uint64_t get_size() {
    return m_num_bits;
  }

  //----------------------------------------------------------------------------
  // clears all bits in the bitmap
  inline void clear_all() {
    std::memset(m_arr, 0, words_to_bytes(m_word_count));
  }

  //----------------------------------------------------------------------------
  // sets all bits in the bitmap
  inline void set_all() {
    std::memset(m_arr, 0xFF,  words_to_bytes(m_word_count));
    // clear bits in the last word past the last legal bit
    uint64_t incomplete_word_bit_offset = (m_num_bits & BITS_IN_WORD_MASK);
    if (incomplete_word_bit_offset) {
      uint64_t clr_mask   = ~(FULL_MASK << incomplete_word_bit_offset);
      m_arr[m_word_count - 1] &= clr_mask;
    }
  }

  //----------------------------------------------------------------------------
  bool bit_is_set(uint64_t offset) {
    if (offset < m_num_bits) {
      auto [word_index, bit_offset] = split(offset);
      uint64_t mask       = 1ULL << bit_offset;
      return (m_arr[word_index] & mask);
    } else {
      ceph_assert(offset < m_num_bits);
      return false;
    }
  }

  //----------------------------------------------------------------------------
  bool bit_is_clr(uint64_t offset) {
    if (offset < m_num_bits) {
      auto [word_index, bit_offset] = split(offset);
      uint64_t mask       = 1ULL << bit_offset;
      return ( (m_arr[word_index] & mask) == 0 );
    } else {
      ceph_assert(offset < m_num_bits);
      return false;
    }
  }

private:
  //----------------------------------------------------------------------------
  static inline std::pair<uint64_t, uint64_t> split(uint64_t offset) {
    return { offset_to_index(offset), (offset & BITS_IN_WORD_MASK) };
  }

  //---------------------------------------------------------------------------
  static inline uint64_t offset_to_index(uint64_t offset) {
    return offset >> BITS_IN_WORD_SHIFT;
  }

  //---------------------------------------------------------------------------
  static inline uint64_t index_to_offset(uint64_t index) {
    return index << BITS_IN_WORD_SHIFT;
  }

  //---------------------------------------------------------------------------
  static  inline uint64_t bits_to_words(uint64_t bit_count) {
    return bit_count >> BITS_IN_WORD_SHIFT;
  }

  //---------------------------------------------------------------------------
  static  inline uint64_t words_to_bits(uint64_t words_count) {
    return words_count << BITS_IN_WORD_SHIFT;
  }

  //---------------------------------------------------------------------------
  static  inline uint64_t bytes_to_words(uint64_t byte_count) {
    return byte_count >> BYTES_IN_WORD_SHIFT;
  }

  //---------------------------------------------------------------------------
  static  inline uint64_t words_to_bytes(uint64_t words_count) {
    return (words_count << BYTES_IN_WORD_SHIFT);
  }

  constexpr static uint64_t      BYTES_IN_WORD       = sizeof(uint64_t);
  constexpr static uint64_t      BYTES_IN_WORD_SHIFT = 3;
  constexpr static uint64_t      BITS_IN_WORD        = (BYTES_IN_WORD * 8);
  constexpr static uint64_t      BITS_IN_WORD_MASK   = (BITS_IN_WORD - 1);
  constexpr static uint64_t      BITS_IN_WORD_SHIFT  = 6;
  constexpr static uint64_t      FULL_MASK           = (~((uint64_t)0));

  CephContext *cct;
  uint64_t    *m_arr;
  uint64_t     m_num_bits;
  uint64_t     m_word_count;
};
