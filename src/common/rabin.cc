// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "rabin.h"
#include <string.h>

char * const_zero = 0;

uint64_t rabin_mask[] = {0,1,3,7,15,31,63,127,255,511,1023,2047,4095,8191,16383,32767,65535};


uint64_t gen_rabin_hash(char* chunk_data, uint64_t off) {
  uint64_t roll_sum = 0;
  for (uint64_t i = off; i < WINDOW_SIZE; i++) {
    char cur_byte = *(chunk_data + i);
    roll_sum = (roll_sum * RABIN_PRIME + cur_byte ) %  (MOD_PRIME) ;
  }
  return roll_sum;
}

bool end_of_chunk(const uint64_t fp) {
  return ((fp & RABIN_MASK) == 0) ;
}

bool end_of_chunk(const uint64_t fp , int numbits) {
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

void get_rabin_chunks(
                  size_t min,
                  size_t max,
                  bufferlist& inputdata,
                  vector<bufferlist> * out, int numbits)
{
  char * ptr = inputdata.c_str(); // always points at the start to copy
  uint64_t data_size = inputdata.length();
  if(const_zero ==0 ) {
    const_zero = (char * ) malloc( sizeof(char) * (min+1));
    memset(const_zero,0,min+1);
  }
  // Special Case, really small object that can't fit a chunk
  // or can't calculate rabin hash
  if (data_size < min || data_size < WINDOW_SIZE){
    bufferlist chunk;
    bufferptr bptr(min);
    chunk.push_back(std::move(bptr));
    chunk.copy_in(0, data_size, ptr);
	chunk.copy_in(data_size,min-data_size , const_zero);
    out->push_back(chunk);
    return;
  }

  uint64_t c_offset = 0; // points at where rabin hash starts calculating
  uint64_t c_size = 0; // size of currently calculating chunk
  uint64_t c_start = 0; // points to start of current chunk.
  bool start_chunk = true;
  uint64_t rabin_hash;

  while (c_offset + WINDOW_SIZE < data_size) { // if it is still possible to calculate rabin hash
    assert(c_size <= max);
    if (start_chunk) {
      rabin_hash = gen_rabin_hash(ptr, c_offset); // start calculating for a new chunk
      c_size = WINDOW_SIZE; // don't forget set c_size
      start_chunk = false;
    } else {
      // use existing rabin to calculate a new rabin hash
      // note c_offset already increased by 1
      // old byte pointed by ptr + c_offset - 1
      // new byte pointed by ptr + c_offset + WINDOW_SIZE -1;

      char new_byte = *(ptr + c_offset + WINDOW_SIZE-1);
      char old_byte = *(ptr + c_offset-1);

      // TODO modulus POW_47 is too large a constant in c++ even for 64 bit unsinged int
      rabin_hash = (rabin_hash * RABIN_PRIME + new_byte - old_byte * POW_47) % (MOD_PRIME);
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

    if (end_of_chunk(rabin_hash,numbits)) {
      if((c_size >= min && c_size <= max)) { // a valid chunk with rabin

        bufferlist chunk;
        bufferptr bptr(c_size);
        chunk.push_back(std::move(bptr));
        chunk.copy_in(0, c_size, ptr+c_start);

        out->push_back(chunk);
        c_start += c_size;
        c_offset = c_start;
        start_chunk = true;
        continue;
      }
    } else {
      if (c_size == max) {
        bufferlist chunk;
        bufferptr bptr(c_size);
        chunk.push_back(std::move(bptr));
        chunk.copy_in(0, c_size, ptr+c_start);
        out->push_back(chunk);

        c_start += c_size;
        c_offset = c_start;

        start_chunk = true;
        continue;
      }
    }
    c_size++;
    c_offset++;
  }


  /*
    Now c_offset + WINDOW_SIZE == data_size -> We can't compute rabinhash anymore
    Last chunk of data from c_offset to data_size - 1
    c_size = data_size - c_offset;
  */

  if (start_chunk) {
    // we need to calculate a new chunk, but there isn't enough bits to calculate rabin hash
    
    if (data_size -c_start < min) {
      bufferlist chunk;
      c_size = data_size - c_start;
      bufferptr bptr(min);
      chunk.push_back(std::move(bptr));
      chunk.copy_in(0,c_size,ptr+c_start);
      chunk.copy_in(c_size,min-c_size,const_zero);
      out->push_back(chunk);

    }
    else if (c_start < data_size) { // if we still have data to copy
      bufferlist chunk;
      c_size = data_size - c_start;
      bufferptr bptr(c_size);
      chunk.push_back(std::move(bptr));
      chunk.copy_in(0, c_size, ptr+c_start);
      out->push_back(chunk);
    }

  } else {
    // we are in the process of calculating rabin hash, but don't have enough bits left to find a fingerprint
    if (data_size -c_start < min) {
      bufferlist chunk;
      c_size = data_size - c_start;
      bufferptr bptr(min);
      chunk.push_back(std::move(bptr));
      chunk.copy_in(0,c_size,ptr+c_start);
      chunk.copy_in(c_size,min-c_size,const_zero);
      out->push_back(chunk);

    } else {
      bufferlist chunk;
      c_size = data_size - c_start;
      bufferptr bptr(c_size);
      chunk.push_back(std::move(bptr));
      chunk.copy_in(0, c_size, ptr+c_start);
      out->push_back(chunk);
      }
  }
}

