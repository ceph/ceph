// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 FUJITSU LIMITED
 * Copyright (C) 2013, 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Takanori Nakao <nakao.takanori@jp.fujitsu.com>
 * Author: Takeshi Miyamae <miyamae.takeshi@jp.fujitsu.com>
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_SHEC_H
#define CEPH_ERASURE_CODE_SHEC_H

#include "erasure-code/ErasureCode.h"
#include "ErasureCodeShecTableCache.h"

class ErasureCodeShec : public ceph::ErasureCode {

public:
  enum {
    MULTIPLE = 0,
    SINGLE = 1
  };

  ErasureCodeShecTableCache &tcache;
  int k;
  int DEFAULT_K;
  int m;
  int DEFAULT_M;
  int c;
  int DEFAULT_C;
  int w;
  int DEFAULT_W;
  int technique;
  int *matrix;

  ErasureCodeShec(const int _technique,
		  ErasureCodeShecTableCache &_tcache) :
    tcache(_tcache),
    k(0),
    DEFAULT_K(4),
    m(0),
    DEFAULT_M(3),
    c(0),
    DEFAULT_C(2),
    w(0),
    DEFAULT_W(8),
    technique(_technique),
    matrix(nullptr)
  {}

  ~ErasureCodeShec() override {}

  uint64_t get_supported_optimizations() const override {
    return FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION |
      FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION |
      FLAG_EC_PLUGIN_ZERO_INPUT_ZERO_OUTPUT_OPTIMIZATION |
      FLAG_EC_PLUGIN_PARITY_DELTA_OPTIMIZATION;
  }

  unsigned int get_chunk_count() const override {
    return k + m;
  }

  unsigned int get_data_chunk_count() const override {
    return k;
  }

  unsigned int get_chunk_size(unsigned int stripe_width) const override;

  using ErasureCode::_minimum_to_decode;
  [[deprecated]]
  int _minimum_to_decode(const std::set<int> &want_to_read,
			 const std::set<int> &available_chunks,
			 std::set<int> *minimum) override;

  int _minimum_to_decode(const shard_id_set &want_to_read,
			 const shard_id_set &available_chunks,
			 shard_id_set *minimum) override;

  [[deprecated]]
  int minimum_to_decode_with_cost(const std::set<int> &want_to_read,
				  const std::map<int, int> &available,
				  std::set<int> *minimum) override;

  int minimum_to_decode_with_cost(const shard_id_set &want_to_read,
				  const shard_id_map<int> &available,
				  shard_id_set *minimum) override;

  using ErasureCode::encode;
  [[deprecated]]
  int encode(const std::set<int> &want_to_encode,
		     const ceph::buffer::list &in,
		     std::map<int, ceph::buffer::list> *encoded) override;


  [[deprecated]]
  int encode_chunks(const std::set<int> &want_to_encode,
			    std::map<int, ceph::buffer::list> *encoded) override;
  int encode_chunks(const shard_id_map<bufferptr> &in,
                    shard_id_map<bufferptr> &out) override;

  using ErasureCode::_decode;
  [[deprecated]]
  int _decode(const std::set<int> &want_to_read,
	      const std::map<int, ceph::buffer::list> &chunks,
	      std::map<int, ceph::buffer::list> *decoded) override;
  [[deprecated]]
  int decode_chunks(const std::set<int> &want_to_read,
		    const std::map<int, ceph::buffer::list> &chunks,
		    std::map<int, ceph::buffer::list> *decoded) override;
  int decode_chunks(const shard_id_set &want_to_read,
                    shard_id_map<bufferptr> &in,
                    shard_id_map<bufferptr> &out) override;

  int init(ceph::ErasureCodeProfile &profile, std::ostream *ss) override;
  virtual void shec_encode(char **data,
			   char **coding,
			   int blocksize) = 0;
  virtual int shec_decode(int *erasures,
			  int *avails,
			  char **data,
			  char **coding,
			  int blocksize) = 0;
  virtual unsigned get_alignment() const = 0;
  virtual void prepare() = 0;

  virtual int shec_matrix_decode(int *erased, int *avails,
                                 char **data_ptrs, char **coding_ptrs, int size);
  virtual int* shec_reedsolomon_coding_matrix(int is_single);

private:
  virtual int parse(const ceph::ErasureCodeProfile &profile) = 0;

  virtual double shec_calc_recovery_efficiency1(int k, int m1, int m2, int c1, int c2);
  virtual int shec_make_decoding_matrix(bool prepare,
                                        int *want, int *avails,
                                        int *decoding_matrix,
                                        int *dm_row, int *dm_column,
                                        int *minimum);
};

class ErasureCodeShecReedSolomonVandermonde final : public ErasureCodeShec {
public:

  ErasureCodeShecReedSolomonVandermonde(ErasureCodeShecTableCache &_tcache,
					int technique = MULTIPLE) :
    ErasureCodeShec(technique, _tcache)
  {}

  ~ErasureCodeShecReedSolomonVandermonde() override {
  }

  void shec_encode(char **data,
			   char **coding,
			   int blocksize) override;
  int shec_decode(int *erasures,
			  int *avails,
			  char **data,
			  char **coding,
			  int blocksize) override;

  void encode_delta(const ceph::bufferptr &old_data,
                    const ceph::bufferptr &new_data,
                    ceph::bufferptr *delta_maybe_in_place) override;
  void apply_delta(const shard_id_map<ceph::bufferptr> &in,
	           shard_id_map<ceph::bufferptr> &out) override;

  unsigned get_alignment() const override;
  size_t get_minimum_granularity() override
  {
    return 1;
  }
  void prepare() override;
private:
  int parse(const ceph::ErasureCodeProfile &profile) override;
};
static_assert(!std::is_abstract<ErasureCodeShecReedSolomonVandermonde>());

#endif
