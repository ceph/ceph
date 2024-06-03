
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <openssl/md5.h>
#include "CDC.h"

// Based on this paper:
//   https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
//
// Changes:
//   - window size fixed at 64 bytes (to match our word size)
//   - use XOR instead of +
//   - match mask instead of 0
//   - use target mask when close to target size (instead of
//     small/large mask).  The idea here is to try to use a consistent (target)
//     mask for most cut points if we can, and only resort to small/large mask
//     when we are (very) small or (very) large.

// Note about the target_bits: The goal is an average chunk size of 1
// << target_bits.  However, in reality the average is ~1.25x that
// because of the hard mininum chunk size.
#define SymbolCount 256   //the prehash of each char
#define DigistLength 16   //length of each prehash

class JCCDC : public CDC {
private:
  int target_bits;  ///< target chunk size bits (1 << target_bits)
  int min_bits;     ///< hard minimum chunk size bits (1 << min_bits)
  int max_bits;     ///< hard maximum chunk size bits (1 << max_bits)

  uint64_t chunk_mask;  // mask used for chunking
  uint64_t jump_mask;   // mask used for jump
  uint64_t jump_len;    // the length of each jump
  uint64_t prehash[SymbolCount];  //prehash table for rolling hash calculating


  /// window size in bytes
  const size_t window = sizeof(uint64_t)*8; // bits in uint64_t

  void _setup(int target, int window_bits);

  // mask for different chunk sizes
  uint64_t JC_mask[] = {
      //Do not use 1-32B, for aligent usage
          0x0000000000000000,// 1B
          0x0000000001000000,// 2B
          0x0000000003000000,// 4B
          0x0000010003000000,// 8B
          0x0000090003000000,// 16B
          0x0000190003000000,// 32B

          0x0000590003000000,// 64B
          0x0000590003100000,// 128B
          0x0000590003500000,// 256B
          0x0000590003510000,// 512B
          0x0000590003530000,// 1KB
          0x0000590103530000,// 2KB
          0x0000d90103530000,// 4KB
          0x0000d90303530000,// 8KB
          0x0000d90303531000,// 16KB
          0x0000d90303533000,// 32KB
          0x0000d90303537000,// 64KB
          0x0000d90703537000// 128KB
  };

public:
  JCCDC(int target = 18, int window_bits = 0) {
    _setup(target, window_bits);
  };

  void set_target_bits(int target, int window_bits) override {
    _setup(target, window_bits);
  }

  void calc_chunks(
    const bufferlist& bl,
    std::vector<std::pair<uint64_t, uint64_t>> *chunks) const override;
};
