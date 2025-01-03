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

class ErasureCodeClay final : public ceph::ErasureCode {
public:
  std::string DEFAULT_K{"4"};
  std::string DEFAULT_M{"2"};
  std::string DEFAULT_W{"8"};
  int k = 0, m = 0, d = 0, w = 8;
  int q = 0, t = 0, nu = 0;
  int sub_chunk_no = 0;

  std::map<int, ceph::bufferlist> U_buf;

  struct ScalarMDS {
    ceph::ErasureCodeInterfaceRef erasure_code;
    ceph::ErasureCodeProfile profile;
  };
  ScalarMDS mds;
  ScalarMDS pft;
  const std::string directory;

  explicit ErasureCodeClay(const std::string& dir)
    : directory(dir)
  {}

  ~ErasureCodeClay() override;

  unsigned int get_chunk_count() const override {
    return k+m;
  }

  unsigned int get_data_chunk_count() const override {
    return k;
  }

  int get_sub_chunk_count() override {
    return sub_chunk_no;
  }

  unsigned int get_chunk_size(unsigned int stripe_width) const override;

  int minimum_to_decode(const std::set<int> &want_to_read,
			const std::set<int> &available,
			std::map<int, std::vector<std::pair<int, int>>> *minimum) override;

  int decode(const std::set<int> &want_to_read,
             const std::map<int, ceph::bufferlist> &chunks,
             std::map<int, ceph::bufferlist> *decoded, int chunk_size) override;

  int encode_chunks(const std::set<int> &want_to_encode,
	            std::map<int, ceph::bufferlist> *encoded) override;

  int decode_chunks(const std::set<int> &want_to_read,
		    const std::map<int, ceph::bufferlist> &chunks,
		    std::map<int, ceph::bufferlist> *decoded) override;

  int init(ceph::ErasureCodeProfile &profile, std::ostream *ss) override;

  int is_repair(const std::set<int> &want_to_read,
                const std::set<int> &available_chunks);

  int get_repair_sub_chunk_count(const std::set<int> &want_to_read);

  virtual int parse(ceph::ErasureCodeProfile &profile, std::ostream *ss);

private:
  int minimum_to_repair(const std::set<int> &want_to_read,
                        const std::set<int> &available_chunks,
                        std::map<int, std::vector<std::pair<int, int>>> *minimum);

  int repair(const std::set<int> &want_to_read,
             const std::map<int, ceph::bufferlist> &chunks,
             std::map<int, ceph::bufferlist> *recovered, int chunk_size);

  int decode_layered(std::set<int>& erased_chunks, std::map<int, ceph::bufferlist>* chunks);

  int repair_one_lost_chunk(std::map<int, ceph::bufferlist> &recovered_data, std::set<int> &aloof_nodes,
                            std::map<int, ceph::bufferlist> &helper_data, int repair_blocksize,
                            std::vector<std::pair<int,int>> &repair_sub_chunks_ind);

  void get_repair_subchunks(const int &lost_node,
			    std::vector<std::pair<int, int>> &repair_sub_chunks_ind);

  int decode_erasures(const std::set<int>& erased_chunks, int z,
                      std::map<int, ceph::bufferlist>* chunks, int sc_size);

  int decode_uncoupled(const std::set<int>& erasures, int z, int ss_size);

  void set_planes_sequential_decoding_order(int* order, std::set<int>& erasures);

  void recover_type1_erasure(std::map<int, ceph::bufferlist>* chunks, int x, int y, int z,
                             int* z_vec, int sc_size);

  void get_uncoupled_from_coupled(std::map<int, ceph::bufferlist>* chunks, int x, int y, int z,
                                  int* z_vec, int sc_size);

  void get_coupled_from_uncoupled(std::map<int, ceph::bufferlist>* chunks, int x, int y, int z,
                                  int* z_vec, int sc_size);

  void get_plane_vector(int z, int* z_vec);

  int get_max_iscore(std::set<int>& erased_chunks);
};

#endif
