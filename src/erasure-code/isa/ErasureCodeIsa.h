/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CERN (Switzerland)
 *
 * Author: Andreas-Joachim Peters <Andreas.Joachim.Peters@cern.ch>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

/**
 * @file   ErasureCodeIsa.cc
 *
 * @brief  Erasure Code CODEC using the INTEL ISA-L library.
 *
 * The INTEL ISA-L library supports two pre-defined encoding matrices (cauchy = default, reed_sol_van = default)
 * The default CODEC implementation using these two matrices is implemented in class ErasureCodeIsaDefault.
 * ISA-L allows to use custom matrices which might be added later as implementations deriving from the base class ErasoreCodeIsa.
 */

#ifndef CEPH_ERASURE_CODE_ISA_L_H
#define CEPH_ERASURE_CODE_ISA_L_H

// -----------------------------------------------------------------------------
#include <string_view>
#include "erasure-code/ErasureCode.h"
#include "ErasureCodeIsaTableCache.h"
// -----------------------------------------------------------------------------

using namespace std::literals;

#define EC_ISA_ADDRESS_ALIGNMENT 32u

#define is_aligned(POINTER, BYTE_COUNT) \
  (((uintptr_t)(const void *)(POINTER)) % (BYTE_COUNT) == 0)

class ErasureCodeIsa : public ceph::ErasureCode {
public:

  enum eMatrix {
    kVandermonde = 0, kCauchy = 1
  };

  static constexpr int MAX_K = 32;
  static constexpr int MAX_M = 32;

  int k;
  int m;
  int w;

  ErasureCodeIsaTableCache &tcache;
  const char *technique;
  uint64_t flags;

  ErasureCodeIsa(const char *_technique,
                 ErasureCodeIsaTableCache &_tcache) :
  k(0),
  m(0),
  w(0),
  tcache(_tcache),
  technique(_technique)
  {
    flags = FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION |
            FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION |
            FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION |
            FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION;

    if (technique == "reed_sol_van"sv ||
        technique == "default"sv) {
      flags |= FLAG_EC_PLUGIN_OPTIMIZED_SUPPORTED;
    }
  }

  
  ~ErasureCodeIsa() override
  {
  }

  uint64_t get_supported_optimizations() const override {
    return flags;
  }

  unsigned int
  get_chunk_count() const override
  {
    return k + m;
  }

  unsigned int
  get_data_chunk_count() const override
  {
    return k;
  }

  unsigned int get_chunk_size(unsigned int stripe_width) const override;

  [[deprecated]]
  int encode_chunks(const std::set<int> &want_to_encode,
                    std::map<int, ceph::buffer::list> *encoded) override;
  int encode_chunks(const shard_id_map<bufferptr> &in,
                    shard_id_map<bufferptr> &out) override;

  [[deprecated]]
  int decode_chunks(const std::set<int> &want_to_read,
                            const std::map<int, ceph::buffer::list> &chunks,
                            std::map<int, ceph::buffer::list> *decoded) override;
  int decode_chunks(const shard_id_set &want_to_read,
                    shard_id_map<bufferptr> &in,
                    shard_id_map<bufferptr> &out) override;

  int init(ceph::ErasureCodeProfile &profile, std::ostream *ss) override;

  void isa_xor(char **data, char *coding, int blocksize, int data_vectors);

  void byte_xor(int data_vects, int blocksize, char **array);

  virtual void isa_encode(char **data,
                          char **coding,
                          int blocksize) = 0;

  virtual int isa_decode(int *erasures,
                         char **data,
                         char **coding,
                         int blocksize) = 0;

  virtual unsigned get_alignment() const = 0;

  virtual void prepare() = 0;

 private:
  virtual int parse(ceph::ErasureCodeProfile &profile,
                    std::ostream *ss) = 0;
};

// -----------------------------------------------------------------------------

class ErasureCodeIsaDefault : public ErasureCodeIsa {
private:
  int matrixtype;

public:

  static const std::string DEFAULT_K;
  static const std::string DEFAULT_M;

  unsigned char* encode_coeff; // encoding coefficient
  unsigned char* encode_tbls; // encoding table

  ErasureCodeIsaDefault(ErasureCodeIsaTableCache &_tcache,
                        int matrix = kVandermonde) :

  ErasureCodeIsa("default", _tcache),
  encode_coeff(0), encode_tbls(0)
  {
    matrixtype = matrix;
  }

  
  ~ErasureCodeIsaDefault() override
  {

  }

  void isa_encode(char **data,
                          char **coding,
                          int blocksize) override;

  virtual bool erasure_contains(int *erasures, int i);

  int isa_decode(int *erasures,
                         char **data,
                         char **coding,
                         int blocksize) override;

  void encode_delta(const ceph::bufferptr &old_data,
                    const ceph::bufferptr &new_data,
                    ceph::bufferptr *delta_maybe_in_place) override;

  void apply_delta(const shard_id_map<ceph::bufferptr> &in,
                   shard_id_map<ceph::bufferptr> &out);

  unsigned get_alignment() const override;

  size_t get_minimum_granularity() override
  {
    return 1;
  }

  void prepare() override;

 private:
  int parse(ceph::ErasureCodeProfile &profile,
            std::ostream *ss) override;
};
static_assert(!std::is_abstract<ErasureCodeIsaDefault>());

#endif
