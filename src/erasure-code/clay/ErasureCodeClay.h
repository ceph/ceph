// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
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
#include "json_spirit/json_spirit.h"
#include "erasure-code/ErasureCode.h"

typedef enum mds_block{
  VANDERMONDE_RS=0,
  CAUCHY_MDS=1
}mds_block_t;

typedef struct erasure
{
  int x;
  int y;
}erasure_t;

class ErasureCodeClay final : public ErasureCode {
public:
  std::string DEFAULT_K;
  std::string DEFAULT_M;
  std::string DEFAULT_W;
  int k, m, d, w;
  int q, t, nu;
  int sub_chunk_no;

  map<int, bufferlist> U_buf;
  
  struct ScalarMDS {
	explicit ScalarMDS() { }
    ErasureCodeInterfaceRef erasure_code;
    ErasureCodeProfile profile;
  };
  ScalarMDS mds;
  ScalarMDS pft;
  std::string directory;
  
  explicit ErasureCodeClay(const std::string &dir)
    :DEFAULT_K("6"),
    DEFAULT_M("4"),
	DEFAULT_W("8"),
	k(0), m(0), w(0),
	directory(dir)
  {
  }

  ~ErasureCodeClay() override 
  {
	for(int i=0; i<q*t; i++){
      if(U_buf[i].length() != 0) U_buf[i].clear();
	}
  }

  unsigned int get_chunk_count() const override {
    return k+m;
  }

  unsigned int get_data_chunk_count() const override {
    return k;
  }

  unsigned int get_chunk_size(unsigned int object_size) const override;
  
  int minimum_to_decode(const set<int> &want_to_read,
			  const set<int> &available,
			  map<int, vector<pair<int, int>>> *minimum) override;
			  
  int decode(const set<int> &want_to_read,
                const map<int, bufferlist> &chunks,
                map<int, bufferlist> *decoded, int chunk_size) override;
			  
  int encode_chunks(const std::set<int> &want_to_encode,
			    std::map<int, bufferlist> *encoded) override;

  int decode_chunks(const std::set<int> &want_to_read,
			    const std::map<int, bufferlist> &chunks,
			    std::map<int, bufferlist> *decoded) override;

  int init(ErasureCodeProfile &profile, std::ostream *ss) override;
  
  int is_repair(const set<int> &want_to_read,
                                   const set<int> &available_chunks);

  int minimum_to_repair(const set<int> &want_to_read,
                                   const set<int> &available_chunks,
                                   set<int> *minimum);
								   
  int repair(const set<int> &want_to_read,
                        const map<int, bufferlist> &chunks,
                        map<int, bufferlist> *recovered);
							   
  int get_repair_sub_chunk_count(const set<int> &want_to_read);
  
  virtual int parse(ErasureCodeProfile &profile, std::ostream *ss);
  unsigned get_alignment() const;
private:
  int decode_layered(set<int>& erased_chunks, map<int, bufferlist>* chunks);
  int repair_lost_chunks(map<int, bufferlist> &recovered_data, set<int> &aloof_nodes,
                           map<int, bufferlist> &helper_data, int repair_blocksize, map<int,int> &repair_sub_chunks_ind);
  void get_repair_subchunks(const set<int> &to_repair,
                                   const set<int> &helper_chunks,
                                   int helper_chunk_ind,
                                   map<int, int> &repair_sub_chunks_ind);
  int decode_erasures(const set<int>& erased_chunks, int z, int* z_vec,
                            map<int, bufferlist>& chunks, int sc_size);
  void group_repair_subchunks(map<int,int> &repair_subchunks, vector<pair<int,int> > &grouped_subchunks);	
  int decode_uncoupled(const set<int>& erasures, int z, int ss_size);
  void set_planes_sequential_decoding_order(int* order, erasure_t* erasures);
  int is_erasure_type_1(int ind, erasure_t* erasures, int* z_vec);
  void recover_type1_erasure(map<int, bufferlist>& chunks, int x, int y, int z, int* z_vec, int sc_size);
  void get_uncoupled_from_coupled(map<int, bufferlist>& chunks, int x, int y, int z, int* z_vec, int sc_size);
  void get_coupled_from_uncoupled(map<int, bufferlist>& chunks, int x, int y, int z, int* z_vec, int sc_size);
  void get_plane_vector(int z, int* z_vec);
  void get_weight_vector(erasure_t* erasures, int* weight_vec);
  void get_erasure_coordinates(const set<int>& erasure_locations, erasure_t* erasures);
  int get_hamming_weight(int* weight_vec);
};

#endif
