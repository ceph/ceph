// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Authors : Yuan-Ting Hsieh, Hsuan-Heng Wu, Myoungwon Oh
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_RABIN_H_
#define CEPH_COMMON_RABIN_H_

#include <cstdint>
#include <utility>
#include <vector>

#include "include/buffer_fwd.h"

class RabinChunk {
public:
  RabinChunk(uint32_t window_size, uint32_t rabin_prime,
	     uint64_t mod_prime, uint64_t pow, std::vector<uint64_t> rabin_mask,
	     uint64_t min, uint64_t max, uint32_t num_bits):
	      window_size(window_size), rabin_prime(rabin_prime),
	      mod_prime(mod_prime), pow(pow), rabin_mask(rabin_mask), min(min),
	      max(max), num_bits(num_bits) {}
  RabinChunk() {
    default_init_rabin_options();
  }

  void default_init_rabin_options() {
    std::vector<uint64_t> _rabin_mask;
    for (size_t i = 0; i < 63; ++i) {
      _rabin_mask.push_back((1ull << i) - 1);
    }
    window_size = 48;
    rabin_prime = 3;
    mod_prime = 6148914691236517051;
    pow = 907234050803559263; // pow(prime, window_size)
    min = 8000;
    max = 16000;
    num_bits = 3;
    rabin_mask = _rabin_mask;
  }

  int do_rabin_chunks(ceph::buffer::list& inputdata,
		      std::vector<std::pair<uint64_t, uint64_t>>& chunks,
		      uint64_t min=0, uint64_t max=0);
  uint64_t gen_rabin_hash(char* chunk_data, uint64_t off, uint64_t len = 0);
  void set_window_size(uint32_t size) { window_size = size; }
  void set_rabin_prime(uint32_t r_prime) { rabin_prime = r_prime; }
  void set_mod_prime(uint64_t m_prime) { mod_prime = m_prime; }
  void set_pow(uint64_t p) { pow = p; }
  void set_rabin_mask(std::vector<uint64_t> & mask) { rabin_mask = mask; }
  void set_min_chunk(uint32_t c_min) { min = c_min; }
  void set_max_chunk(uint32_t c_max) { max = c_max; }

  int add_rabin_mask(uint64_t mask) {
    rabin_mask.push_back(mask);
    for (int i = 0; rabin_mask.size(); i++) {
      if (rabin_mask[i] == mask) {
	return i;
      }
    }
    return -1;
  }
  void set_numbits(uint32_t bits) {
    ceph_assert(bits > 0);
    ceph_assert(bits < 63);
    num_bits = bits;
  }

  // most users should use this
  void set_target_bits(int bits, int windowbits = 2) {
    set_numbits(bits);
    set_min_chunk(1 << (bits - windowbits));
    set_max_chunk(1 << (bits + windowbits));
  }

private:
  bool end_of_chunk(const uint64_t fp , int numbits);

  uint32_t window_size;
  uint32_t rabin_prime;
  uint64_t mod_prime;
  uint64_t pow;
  std::vector<uint64_t> rabin_mask;
  uint64_t min;
  uint64_t max;
  uint32_t num_bits;
};


#endif // CEPH_COMMON_RABIN_H_
