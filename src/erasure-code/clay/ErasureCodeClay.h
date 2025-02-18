// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Indian Institute of Science <office.ece@iisc.ac.in>
 *
 * Author: Myna Vajha <mynaramana@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_CLAY_H
#define CEPH_ERASURE_CODE_CLAY_H

#include "include/err.h"
#include "include/buffer_fwd.h"
#include "erasure-code/ErasureCode.h"

class ErasureCodeClay final : public ceph::ErasureCode
{
public:
  std::string DEFAULT_K{"4"};
  std::string DEFAULT_M{"2"};
  std::string DEFAULT_W{"8"};
  int k = 0, m = 0, d = 0, w = 8;
  int q = 0, t = 0, nu = 0;
  int sub_chunk_no = 0;

  std::optional<shard_id_map<ceph::bufferlist>> U_buf;

  struct ScalarMDS
  {
    ceph::ErasureCodeInterfaceRef erasure_code;
    ceph::ErasureCodeProfile profile;
  };

  ScalarMDS mds;
  ScalarMDS pft;
  const std::string directory;

  explicit ErasureCodeClay(const std::string &dir)
    : directory(dir)
  {}

  ~ErasureCodeClay() override;

  uint64_t get_supported_optimizations() const override
  {
    if (m == 1) {
      // PARTIAL_WRITE optimization can be supported in
      // the corner case of m = 1
      return FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION |
        FLAG_EC_PLUGIN_PARTIAL_WRITE_OPTIMIZATION |
        FLAG_EC_PLUGIN_REQUIRE_SUB_CHUNKS;
    }
    return FLAG_EC_PLUGIN_PARTIAL_READ_OPTIMIZATION |
      FLAG_EC_PLUGIN_REQUIRE_SUB_CHUNKS;
  }

  unsigned int get_chunk_count() const override
  {
    return k + m;
  }

  unsigned int get_data_chunk_count() const override
  {
    return k;
  }

  int get_sub_chunk_count() override
  {
    return sub_chunk_no;
  }

  unsigned int get_chunk_size(unsigned int stripe_width) const override;

  unsigned int get_minimum_granularity() override;

  // See https://stackoverflow.com/questions/9995421/gcc-woverloaded-virtual-warnings
  using ErasureCode::minimum_to_decode;
  int minimum_to_decode(const shard_id_set &want_to_read,
                        const shard_id_set &available,
                        shard_id_set &minimum_set,
                        shard_id_map<std::vector<std::pair<int, int>>> *
                        minimum) override;

  // See https://stackoverflow.com/questions/9995421/gcc-woverloaded-virtual-warnings
  using ErasureCode::decode;
  int decode(const shard_id_set &want_to_read,
             const shard_id_map<ceph::bufferlist> &chunks,
             shard_id_map<ceph::bufferlist> *decoded, int chunk_size) override;

  int encode_chunks(const shard_id_map<ceph::bufferptr> &in,
                    shard_id_map<ceph::bufferptr> &out) override;

  int decode_chunks(const shard_id_set &want_to_read,
                    shard_id_map<ceph::bufferptr> &in,
                    shard_id_map<ceph::bufferptr> &out) override;

  void encode_delta(const ceph::bufferptr &old_data,
                    const ceph::bufferptr &new_data,
                    ceph::bufferptr *delta);

  void apply_delta(const shard_id_map<ceph::bufferptr> &in,
                   shard_id_map<ceph::bufferptr> &out);

  int init(ceph::ErasureCodeProfile &profile, std::ostream *ss) override;

  int is_repair(const shard_id_set &want_to_read,
                const shard_id_set &available_chunks);

  int get_repair_sub_chunk_count(const shard_id_set &want_to_read);

  virtual int parse(ceph::ErasureCodeProfile &profile, std::ostream *ss);

private:
  int minimum_to_repair(const shard_id_set &want_to_read,
                        const shard_id_set &available_chunks,
                        shard_id_map<std::vector<std::pair<int, int>>> *
                        minimum);

  int repair(const shard_id_set &want_to_read,
             const shard_id_map<ceph::bufferlist> &chunks,
             shard_id_map<ceph::bufferlist> *recovered, int chunk_size);

  int decode_layered(shard_id_set &erased_chunks
                     , shard_id_map<ceph::bufferlist> *chunks);

  int repair_one_lost_chunk(shard_id_map<ceph::bufferlist> &recovered_data
                            , shard_id_set &aloof_nodes,
                            shard_id_map<ceph::bufferlist> &helper_data
                            , int repair_blocksize,
                            std::vector<std::pair<int, int>> &
                            repair_sub_chunks_ind);

  void get_repair_subchunks(const int &lost_node,
                            std::vector<std::pair<int, int>> &
                            repair_sub_chunks_ind);

  int decode_erasures(const shard_id_set &erased_chunks, int z,
                      shard_id_map<ceph::bufferlist> *chunks, int sc_size);

  int decode_uncoupled(const shard_id_set &erasures, int z, int ss_size);

  void set_planes_sequential_decoding_order(int *order, shard_id_set &erasures);

  void recover_type1_erasure(shard_id_map<ceph::bufferlist> *chunks, int x
                             , int y, int z,
                             int *z_vec, int sc_size);

  void get_uncoupled_from_coupled(shard_id_map<ceph::bufferlist> *chunks, int x
                                  , int y, int z,
                                  int *z_vec, int sc_size);

  void get_coupled_from_uncoupled(shard_id_map<ceph::bufferlist> *chunks, int x
                                  , int y, int z,
                                  int *z_vec, int sc_size);

  void get_plane_vector(int z, int *z_vec);

  int get_max_iscore(shard_id_set &erased_chunks);
};

#endif
