// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstring>

#include "include/types.h"
#include "rabin.h"


uint64_t RabinChunk::gen_rabin_hash(char* chunk_data, uint64_t off, uint64_t len) {
  uint64_t roll_sum = 0;
  uint64_t data_len = len;
  if (data_len == 0) {
    data_len = window_size;
  }
  for (uint64_t i = off; i < data_len; i++) {
    char cur_byte = *(chunk_data + i);
    roll_sum = (roll_sum * rabin_prime + cur_byte ) %  (mod_prime) ;
  }
  return roll_sum;
}

bool RabinChunk::end_of_chunk(const uint64_t fp , int numbits) {
  return ((fp & rabin_mask[numbits]) == 0) ;
}

/*
 * Given a bufferlist of inputdata, use Rabin-fingerprint to
 * chunk it and return the chunked result
 *
 * Arguments:
 *   min: min data chunk size
 *   max: max data chunk size
 *
 * Returns:
 *   output_chunks split by Rabin
 */

int RabinChunk::do_rabin_chunks(ceph::buffer::list& inputdata,
				std::vector<std::pair<uint64_t, uint64_t>>& chunks,
				uint64_t min_val, uint64_t max_val)
{
  char *ptr = inputdata.c_str();
  uint64_t data_size = inputdata.length();
  uint64_t min, max;
  min = min_val;
  max = max_val;


  if (min == 0 || max == 0) {
    min = this->min;
    max = this->max;
  }

  if (min < window_size) {
    return -ERANGE;
  }

  if (data_size < min) {
    chunks.push_back(std::make_pair(0, data_size));
    return 0;
  }

  uint64_t c_offset = 0;
  uint64_t c_size = 0;
  uint64_t c_start = 0;
  uint64_t rabin_hash;
  bool start_new_chunk = true;
  bool store_chunk = false;


  while (c_offset + window_size < data_size) { // if it is still possible to calculate rabin hash

    if (start_new_chunk) {
      rabin_hash = gen_rabin_hash(ptr, c_offset); // start calculating for a new chunk
      c_size = window_size; // don't forget set c_size
      start_new_chunk = false;
    } else {
      // use existing rabin to calculate a new rabin hash
      // note c_offset already increased by 1
      // old byte pointed by ptr + c_offset - 1
      // new byte pointed by ptr + c_offset + WINDOW_SIZE -1;

      char new_byte = *(ptr + c_offset + window_size - 1);
      char old_byte = *(ptr + c_offset - 1);

      // TODO modulus POW_47 is too large a constant in c++ even for 64 bit unsinged int
      rabin_hash = (rabin_hash * rabin_prime + new_byte - old_byte * pow) % (mod_prime);
    }

    /*
      Case 1 : Fingerprint Found
        subcase 1 : if c_size < min -> ignore
        subcase 2 : if min <= c_size <= max -> store
        subcase 3 : if c_size >  max -> won't happen
      Case 2 : Fingerprint not Found
        subcase 1 : if c_size < min -> ignore
        subcase 2 : if min <= c_size < max -> ignore
        subcase 3 : if c_size == max -> (force) store
    */

    if (end_of_chunk(rabin_hash, num_bits)) {
      if((c_size >= min && c_size <= max)) { // a valid chunk with rabin
	store_chunk = true;
      } else {
	store_chunk = false;
      }
    } else {
      if (c_size == max) {
	store_chunk = true;
      } else {
	store_chunk = false;
      }
    }

    if (store_chunk) {
      chunks.push_back(std::make_pair(c_start, c_size));
      c_start += c_size;
      c_offset = c_start;
      start_new_chunk = true;
      continue;

    }

    c_size++;
    c_offset++;
  }

  if (c_start < data_size) {
    c_size = data_size - c_start;
    chunks.push_back(std::make_pair(c_start, c_size));
  }

  return 0;
}
