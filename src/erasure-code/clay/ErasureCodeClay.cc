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

#include <errno.h>
#include <algorithm>

#include "ErasureCodeClay.h"

#include "common/debug.h"
#include "erasure-code/ErasureCodePlugin.h"
#include "include/ceph_assert.h"
#include "include/str_map.h"
#include "include/stringify.h"
#include "osd/osd_types.h"


#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

#define LARGEST_VECTOR_WORDSIZE 16
#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

using namespace std;
using namespace ceph;
static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeClay: ";
}

static int pow_int(int a, int x) {
  int power = 1;
  while (x) {
    if (x & 1) power *= a;
    x /= 2;
    a *= a;
  }
  return power;
}

ErasureCodeClay::~ErasureCodeClay()
{
  for (shard_id_t i; i < q*t; ++i) {
    if ((*U_buf)[i].length() != 0) (*U_buf)[i].clear();
  }
}

int ErasureCodeClay::init(ErasureCodeProfile &profile,
			  ostream *ss)
{
  int r;
  r = parse(profile, ss);
  if (r)
    return r;

  r = ErasureCode::init(profile, ss);
  if (r)
    return r;
  ErasureCodePluginRegistry &registry = ErasureCodePluginRegistry::instance();
  r = registry.factory(mds.profile["plugin"],
		       directory,
		       mds.profile,
		       &mds.erasure_code,
		       ss);
  if (r)
    return r;
  r = registry.factory(pft.profile["plugin"],
		       directory,
		       pft.profile,
		       &pft.erasure_code,
		       ss);
  return r;

}

unsigned int ErasureCodeClay::get_chunk_size(unsigned int stripe_width) const
{
  unsigned int alignment_scalar_code = pft.erasure_code->get_chunk_size(1);
  unsigned int alignment = sub_chunk_no * k * alignment_scalar_code;
  
  return round_up_to(stripe_width, alignment) / k;
}

unsigned int ErasureCodeClay::get_minimum_granularity()
{
  return mds.erasure_code->get_minimum_granularity();
}

int ErasureCodeClay::minimum_to_decode(const shard_id_set &want_to_read,
				       const shard_id_set &available,
				       shard_id_set &minimum_set,
				       shard_id_map<vector<pair<int, int>>> *minimum)
{
  if (is_repair(want_to_read, available)) {
    int r = minimum_to_repair(want_to_read, available, minimum);
    minimum->populate_bitset_set(minimum_set);
    return r;
  }
  return ErasureCode::minimum_to_decode(want_to_read, available, minimum_set, minimum);
}

int ErasureCodeClay::decode(const shard_id_set &want_to_read,
			    const shard_id_map<bufferlist> &chunks,
			    shard_id_map<bufferlist> *decoded, int chunk_size)
{
  shard_id_set avail;
  for ([[maybe_unused]] auto& [node, bl] : chunks) {
    avail.insert(node);
    (void)bl;  // silence -Wunused-variable
  }

  if (is_repair(want_to_read, avail) && 
         ((unsigned int)chunk_size > chunks.begin()->second.length())) {
    return repair(want_to_read, chunks, decoded, chunk_size);
  } else {
    return ErasureCode::_decode(want_to_read, chunks, decoded);
  }
}

void p(const shard_id_set &s) { cerr << s; } // for gdb

int ErasureCodeClay::encode_chunks(const shard_id_map<bufferptr> &in,
                                   shard_id_map<bufferptr> &out)
{
  shard_id_map<bufferlist> chunks(q * t);
  shard_id_set parity_chunks;
  unsigned int size = 0;
  auto& nonconst_in = const_cast<shard_id_map<bufferptr>&>(in);

  for (auto &&[shard, ptr] : nonconst_in) {
    if (size == 0) size = ptr.length();
    else ceph_assert(size == ptr.length());
    chunks[shard].append(nonconst_in[shard]);
  }

  for (auto &&[shard, ptr] : out) {
    if (size == 0) size = ptr.length();
    else ceph_assert(size == ptr.length());
    chunks[shard+nu].append(out[shard]);
    parity_chunks.insert(shard+nu);
  }

  for (shard_id_t i(k); i < k + nu; ++i) {
    bufferptr buf(buffer::create_aligned(size, SIMD_ALIGN));
    buf.zero();
    chunks[i].push_back(std::move(buf));
  }

  int res = decode_layered(parity_chunks, &chunks);
  for (shard_id_t i(k) ; i < k + nu; ++i) {
  // need to clean some of the intermediate chunks here!!
    chunks[i].clear();
  }
  return res;
}

int ErasureCodeClay::decode_chunks(const shard_id_set &want_to_read,
                                   shard_id_map<bufferptr> &in,
                                   shard_id_map<bufferptr> &out)
{
  unsigned int size = 0;
  shard_id_set erasures;
  shard_id_map<bufferlist> coded_chunks(q * t);

  ceph_assert(out.size() > 0);

  for (auto &&[shard, ptr] : in) {
    if (size == 0) size = ptr.length();
    else ceph_assert(size == ptr.length());
    if (shard < k)
      coded_chunks[shard].append(in[shard]);
    else
      coded_chunks[shard+nu].append(in[shard]);
  }

  for (auto &&[shard, ptr] : out) {
    if (size == 0) size = ptr.length();
    else ceph_assert(size == ptr.length());
    erasures.insert(shard < k ? shard : shard_id_t(shard + nu));
    if (shard < k)
      coded_chunks[shard].append(out[shard]);
    else
      coded_chunks[shard+nu].append(out[shard]);
  }

  for (shard_id_t i(k); i < k+nu; ++i) {
    bufferptr buf(buffer::create_aligned(size, SIMD_ALIGN));
    buf.zero();
    coded_chunks[i].push_back(std::move(buf));
  }

  int res = decode_layered(erasures, &coded_chunks);
  for (shard_id_t i(k); i < k+nu; ++i) {
    coded_chunks[i].clear();
  }
  return res;
}

void ErasureCodeClay::encode_delta(const bufferptr &old_data,
                                   const bufferptr &new_data,
                                   bufferptr *delta)
{
  ceph_abort("Not yet supported by this plugin");
}

void ErasureCodeClay::apply_delta(const shard_id_map<bufferptr> &in,
                                  shard_id_map<bufferptr> &out)
{
  ceph_abort("Not yet supported by this plugin");
}

int ErasureCodeClay::parse(ErasureCodeProfile &profile,
			   ostream *ss)
{
  int err = 0;
  err = ErasureCode::parse(profile, ss);
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  err |= to_int("m", profile, &m, DEFAULT_M, ss);

  err |= sanity_check_k_m(k, m, ss);

  err |= to_int("d", profile, &d, std::to_string(k+m-1), ss);

  // check for scalar_mds in profile input
  if (profile.find("scalar_mds") == profile.end() ||
      profile.find("scalar_mds")->second.empty()) {
    mds.profile["plugin"] = "jerasure";
    pft.profile["plugin"] = "jerasure";
  } else {
    std::string p = profile.find("scalar_mds")->second;
    if ((p == "jerasure") || (p == "isa") || (p == "shec")) {
      mds.profile["plugin"] = p;
      pft.profile["plugin"] = p;
    } else {
        *ss << "scalar_mds " << mds.profile["plugin"] <<
               "is not currently supported, use one of 'jerasure',"<<
               " 'isa', 'shec'" << std::endl;
        err = -EINVAL;
        return err;
    }
  }

  if (profile.find("technique") == profile.end() ||
      profile.find("technique")->second.empty()) {
    if ((mds.profile["plugin"]=="jerasure") || (mds.profile["plugin"]=="isa") ) {
      mds.profile["technique"] = "reed_sol_van";
      pft.profile["technique"] = "reed_sol_van";
    } else {
      mds.profile["technique"] = "single";
      pft.profile["technique"] = "single";
    }
  } else {
    std::string p = profile.find("technique")->second;
    if (mds.profile["plugin"] == "jerasure") {
      if ( (p == "reed_sol_van") || (p == "reed_sol_r6_op") || (p == "cauchy_orig")
           || (p == "cauchy_good") || (p == "liber8tion")) {
        mds.profile["technique"] = p;
        pft.profile["technique"] = p;
      } else {
        *ss << "technique " << p << "is not currently supported, use one of "
	    << "reed_sol_van', 'reed_sol_r6_op','cauchy_orig',"
	    << "'cauchy_good','liber8tion'"<< std::endl;
        err = -EINVAL;
        return err;
      }
    } else if (mds.profile["plugin"] == "isa") {
      if ( (p == "reed_sol_van") || (p == "cauchy")) {
        mds.profile["technique"] = p;
        pft.profile["technique"] = p;
      } else {
        *ss << "technique " << p << "is not currently supported, use one of"
	    << "'reed_sol_van','cauchy'"<< std::endl;
        err = -EINVAL;
        return err;
      }
    } else {
      if ( (p == "single") || (p == "multiple")) {
        mds.profile["technique"] = p;
        pft.profile["technique"] = p;
      } else {
        *ss << "technique " << p << "is not currently supported, use one of"<<
               "'single','multiple'"<< std::endl;
        err = -EINVAL;
        return err;
      }
    }
  }
  if ((d < k) || (d > k + m - 1)) {
    *ss << "value of d " << d
        << " must be within [ " << k << "," << k+m-1 << "]" << std::endl;
    err = -EINVAL;
    return err;
  }

  q = d - k + 1;
  if ((k + m) % q) {
    nu = q - (k + m) % q;
  } else {
    nu = 0;
  }

  if (k+m+nu > 254) {
    err = -EINVAL;
    return err;
  }

  if (mds.profile["plugin"] == "shec") {
    mds.profile["c"] = '2';
    pft.profile["c"] = '2';
  }
  mds.profile["k"] = std::to_string(k+nu);
  mds.profile["m"] = std::to_string(m);
  mds.profile["w"] = '8';

  pft.profile["k"] = '2';
  pft.profile["m"] = '2';
  pft.profile["w"] = '8';

  t = (k + m + nu) / q;
  sub_chunk_no = pow_int(q, t);

  U_buf.emplace(q*t);

  dout(10) << __func__
	   << " (q,t,nu)=(" << q << "," << t << "," << nu <<")" << dendl;

  return err;
}

int ErasureCodeClay::is_repair(const shard_id_set &want_to_read,
			       const shard_id_set &available_chunks) {

  if (includes(available_chunks.begin(), available_chunks.end(),
               want_to_read.begin(), want_to_read.end())) return 0;
  // Oops, before the attempt to EC partial reads the fellowing
  // condition was always true as `get_want_to_read_shards()` yields
  // entire stripe. Unfortunately, we built upon this assumption and
  // even `ECUtil::decode()` asserts on chunks being multiply of
  // `chunk_size`.
  // XXX: for now returning 0 and knocking the optimization out.
  if (want_to_read.size() > 1) return 0;
  else return 0;

  shard_id_t shard = *want_to_read.begin();
  int i = static_cast<int>(shard);
  int lost_node_id = (i < k) ? i: i+nu;
  for (int x = 0; x < q; x++) {
    int node = (lost_node_id/q)*q+x;
    node = (node < k) ? node : node-nu;
    if (node != i) { // node in the same group other than erased node
      if (available_chunks.count(shard_id_t(node)) == 0) return 0;
    }
  }

  if (available_chunks.size() < (unsigned)d) return 0;
  return 1;
}

int ErasureCodeClay::minimum_to_repair(const shard_id_set &want_to_read,
				       const shard_id_set &available_chunks,
				       shard_id_map<vector<pair<int, int>>> *minimum)
{
  shard_id_t i = *want_to_read.begin();
  int lost_node_index = static_cast<int>((i < k) ? i : i+nu);
  int rep_node_index = 0;

  // add all the nodes in lost node's y column.
  vector<pair<int, int>> sub_chunk_ind;
  get_repair_subchunks(lost_node_index, sub_chunk_ind);
  if ((available_chunks.size() >= (unsigned)d)) {
    for (int j = 0; j < q; j++) {
      if (j != lost_node_index%q) {
        rep_node_index = (lost_node_index/q)*q+j;
        if (rep_node_index < k) {
          minimum->emplace(shard_id_t(rep_node_index), sub_chunk_ind);
        } else if (rep_node_index >= k+nu) {
          minimum->emplace(shard_id_t(rep_node_index-nu), sub_chunk_ind);
        }
      }
    }
    for (auto chunk : available_chunks) {
      if (minimum->size() >= (unsigned)d) {
	break;
      }
      if (!minimum->count(chunk)) {
	minimum->emplace(chunk, sub_chunk_ind);
      }
    }
  } else {
    dout(0) << "minimum_to_repair: shouldn't have come here" << dendl;
    ceph_assert(0);
  }
  ceph_assert(minimum->size() == (unsigned)d);
  return 0;
}

void ErasureCodeClay::get_repair_subchunks(const int &lost_node,
					   vector<pair<int, int>> &repair_sub_chunks_ind)
{
  const int y_lost = lost_node / q;
  const int x_lost = lost_node % q;

  const int seq_sc_count = pow_int(q, t-1-y_lost);
  const int num_seq = pow_int(q, y_lost);

  int index = x_lost * seq_sc_count;
  for (int ind_seq = 0; ind_seq < num_seq; ind_seq++) {
    repair_sub_chunks_ind.push_back(make_pair(index, seq_sc_count));
    index += q * seq_sc_count;
  }
}

int ErasureCodeClay::get_repair_sub_chunk_count(const shard_id_set &want_to_read)
{
  int weight_vector[t];
  std::fill(weight_vector, weight_vector + t, 0);
  for (auto to_read : want_to_read) {
    weight_vector[static_cast<int>(to_read) / q]++;
  }

  int repair_subchunks_count = 1;
  for (int y = 0; y < t; y++) {
    repair_subchunks_count = repair_subchunks_count*(q-weight_vector[y]);
  }

  return sub_chunk_no - repair_subchunks_count;
}

int ErasureCodeClay::repair(const shard_id_set &want_to_read,
			    const shard_id_map<bufferlist> &chunks,
			    shard_id_map<bufferlist> *repaired, int chunk_size)
{

  ceph_assert((want_to_read.size() == 1) && (chunks.size() == (unsigned)d));

  int repair_sub_chunk_no = get_repair_sub_chunk_count(want_to_read);
  vector<pair<int, int>> repair_sub_chunks_ind;

  unsigned repair_blocksize = chunks.begin()->second.length();
  assert(repair_blocksize%repair_sub_chunk_no == 0);

  unsigned sub_chunksize = repair_blocksize/repair_sub_chunk_no;
  unsigned chunksize = sub_chunk_no*sub_chunksize;

  ceph_assert(chunksize == (unsigned)chunk_size);

  shard_id_map<bufferlist> recovered_data(q * t);
  shard_id_map<bufferlist> helper_data(q * t);
  shard_id_set aloof_nodes;

  for (shard_id_t i; i < k + m; ++i) {
    // included helper data only for d+nu nodes.
    if (auto found = chunks.find(shard_id_t(i)); found != chunks.end()) { // i is a helper
      if (i<k) {
	helper_data[i] = found->second;
      } else {
	helper_data[i+nu] = found->second;
      }
    } else {
      if (i != *want_to_read.begin()) { // aloof node case.
        shard_id_t aloof_node_id = (i < k) ? i: i+nu;
        aloof_nodes.insert(aloof_node_id);
      } else {
        bufferptr ptr(buffer::create_aligned(chunksize, SIMD_ALIGN));
	ptr.zero();
        shard_id_t lost_node_id = (i < k) ? i : i+nu;
        (*repaired)[i].push_back(ptr);
        recovered_data[lost_node_id] = (*repaired)[i];
        get_repair_subchunks(static_cast<int>(lost_node_id), repair_sub_chunks_ind);
      }
    }
  }

  // this is for shortened codes i.e., when nu > 0
  for (shard_id_t i(k); i < k+nu; ++i) {
    bufferptr ptr(buffer::create_aligned(repair_blocksize, SIMD_ALIGN));
    ptr.zero();
    helper_data[i].push_back(ptr);
  }

  ceph_assert(helper_data.size()+aloof_nodes.size()+recovered_data.size() ==
	      (unsigned) q*t);

  int r = repair_one_lost_chunk(recovered_data, aloof_nodes,
				helper_data, repair_blocksize,
				repair_sub_chunks_ind);

  // clear buffers created for the purpose of shortening
  for (shard_id_t i(k); i < k+nu; ++i) {
    helper_data[i].clear();
  }

  return r;
}

int ErasureCodeClay::repair_one_lost_chunk(shard_id_map<bufferlist> &recovered_data,
					   shard_id_set &aloof_nodes,
					   shard_id_map<bufferlist> &helper_data,
					   int repair_blocksize,
					   vector<pair<int,int>> &repair_sub_chunks_ind)
{
  unsigned repair_subchunks = (unsigned)sub_chunk_no / q;
  unsigned sub_chunksize = repair_blocksize / repair_subchunks;

  int z_vec[t];
  shard_id_map<shard_id_set> ordered_planes(q * t);
  shard_id_map<int> repair_plane_to_ind(q * t);
  int plane_ind = 0;

  bufferptr buf(buffer::create_aligned(sub_chunksize, SIMD_ALIGN));
  bufferlist temp_buf;
  temp_buf.push_back(buf);

  for (auto [index,count] : repair_sub_chunks_ind) {
    for (shard_id_t j(index); j < index + count; ++j) {
      get_plane_vector(static_cast<int>(j), z_vec);
      shard_id_t order ;
      // check across all erasures and aloof nodes
      for ([[maybe_unused]] auto& [node, bl] : recovered_data) {
        if (static_cast<int>(node) % q == z_vec[static_cast<int>(node) / q]) ++order;
        (void)bl;  // silence -Wunused-variable
      }
      for (auto node : aloof_nodes) {
        if (static_cast<int>(node) % q == z_vec[static_cast<int>(node) / q]) ++order;
      }
      ceph_assert(order > 0);
      ordered_planes[order].insert(j);
      // to keep track of a sub chunk within helper buffer recieved
      repair_plane_to_ind[j] = plane_ind;
      plane_ind++;
    }
  }
  assert((unsigned)plane_ind == repair_subchunks);

  for (shard_id_t i(0); i < q*t; ++i) {
    if ((*U_buf)[i].length() == 0) {
      bufferptr buf(buffer::create_aligned(sub_chunk_no*sub_chunksize, SIMD_ALIGN));
      buf.zero();
      (*U_buf)[i].push_back(std::move(buf));
    }
  }

  int lost_chunk;
  int count = 0;
  for ([[maybe_unused]] auto& [node, bl] : recovered_data) {
    lost_chunk = static_cast<int>(node);
    count++;
    (void)bl;  // silence -Wunused-variable
  }
  ceph_assert(count == 1);

  shard_id_set erasures;
  for (int i = 0; i < q; ++i) {
    erasures.insert(shard_id_t(lost_chunk - lost_chunk % q + i));
  }
  for (auto node : aloof_nodes) {
    erasures.insert(node);
  }

  for (shard_id_t order(1); ; ++order) {
    if (ordered_planes.count(order) == 0) {
      break;
    }
    for (auto z_shard : ordered_planes[order]) {
      int z = static_cast<int>(z_shard);
      get_plane_vector(z, z_vec);

      for (int y = 0; y < t; y++) {
	for (int x = 0; x < q; x++) {
	  shard_id_t node_xy(y*q + x);
	  shard_id_map<bufferlist> known_subchunks(q * t);
	  shard_id_map<bufferlist> pftsubchunks(q * t);
	  shard_id_set pft_erasures;
	  if (erasures.count(node_xy) == 0) {
	    assert(helper_data.count(node_xy) > 0);
	    shard_id_t z_sw(z + (x - z_vec[y])*pow_int(q,t-1-y));
	    shard_id_t node_sw(y*q + z_vec[y]);
	    shard_id_t i0, i1(1), i2(2), i3(3);
	    if (z_vec[y] > x) {
	      i0 = 1;
	      i1 = 0;
	      i2 = 3;
	      i3 = 2;
	    }
	    if (aloof_nodes.count(node_sw) > 0) {
	      assert(repair_plane_to_ind.count(z_shard) > 0);
	      assert(repair_plane_to_ind.count(z_sw) > 0);
	      pft_erasures.insert(i2);
	      known_subchunks[i0].substr_of(helper_data[node_xy], repair_plane_to_ind[z_shard]*sub_chunksize, sub_chunksize);
	      known_subchunks[i3].substr_of((*U_buf)[node_sw], static_cast<int>(z_sw)*sub_chunksize, sub_chunksize);
	      pftsubchunks[i0] = known_subchunks[i0];
	      pftsubchunks[i1] = temp_buf;
	      pftsubchunks[i2].substr_of((*U_buf)[node_xy], z*sub_chunksize, sub_chunksize);
	      pftsubchunks[i3] = known_subchunks[i3];
	      for (shard_id_t i; i<3; ++i) {
		pftsubchunks[i].rebuild_aligned(SIMD_ALIGN);
	      }
              shard_id_map<bufferptr> in(get_chunk_count());
              shard_id_map<bufferptr> out(get_chunk_count());
              for (auto&& [shard, list] : pftsubchunks) {
                auto bp = list.begin().get_current_ptr();
                if (shard == i2) out[shard] = bp;
                else in[shard] = bp;
              }
              pft.erasure_code->decode_chunks(pft_erasures, in, out);
	    } else {
	      ceph_assert(helper_data.count(node_sw) > 0);
	      ceph_assert(repair_plane_to_ind.count(z_shard) > 0);
	      if (z_vec[y] != x){
		pft_erasures.insert(i2);
		ceph_assert(repair_plane_to_ind.count(z_sw) > 0);
		known_subchunks[i0].substr_of(helper_data[node_xy], repair_plane_to_ind[z_shard]*sub_chunksize, sub_chunksize);
		known_subchunks[i1].substr_of(helper_data[node_sw], repair_plane_to_ind[z_sw]*sub_chunksize, sub_chunksize);
		pftsubchunks[i0] = known_subchunks[i0];
		pftsubchunks[i1] = known_subchunks[i1];
		pftsubchunks[i2].substr_of((*U_buf)[node_xy], z*sub_chunksize, sub_chunksize);
		pftsubchunks[i3].substr_of(temp_buf, 0, sub_chunksize);
		for (shard_id_t i; i<3; ++i) {
		  pftsubchunks[i].rebuild_aligned(SIMD_ALIGN);
		}
	        shard_id_map<bufferptr> in(get_chunk_count());
	        shard_id_map<bufferptr> out(get_chunk_count());
                for (auto&& [shard, list] : pftsubchunks) {
                  auto bp = list.begin().get_current_ptr();
                  if (shard == i2) out[shard] = bp;
                  else in[shard] = bp;
                }
                pft.erasure_code->decode_chunks(pft_erasures, in, out);
	      } else {
		char* uncoupled_chunk = (*U_buf)[node_xy].c_str();
		char* coupled_chunk = helper_data[node_xy].c_str();
		memcpy(&uncoupled_chunk[z*sub_chunksize],
		       &coupled_chunk[repair_plane_to_ind[z_shard]*sub_chunksize],
		       sub_chunksize);
	      }
	    }
	  }
	} // x
      } // y
      ceph_assert(erasures.size() <= (unsigned)m);
      decode_uncoupled(erasures, z, sub_chunksize);

      for (auto shard : erasures) {
        int i = static_cast<int>(shard);
	int x = i % q;
	int y = i / q;
	int node_sw = y*q+z_vec[y];
	int z_sw = z + (x - z_vec[y]) * pow_int(q,t-1-y);
	shard_id_set pft_erasures;
	shard_id_map<bufferlist> known_subchunks(q * t);
	shard_id_map<bufferlist> pftsubchunks(q * t);
	shard_id_t i0(0), i1(1), i2(2), i3(3);
	if (z_vec[y] > x) {
	  i0 = 1;
	  i1 = 0;
	  i2 = 3;
	  i3 = 2;
	}
	// make sure it is not an aloof node before you retrieve repaired_data
	if (aloof_nodes.count(shard) == 0) {
	  if (x == z_vec[y]) { // hole-dot pair (type 0)
	    char* coupled_chunk = recovered_data[shard].c_str();
	    char* uncoupled_chunk = (*U_buf)[shard].c_str();
	    memcpy(&coupled_chunk[z*sub_chunksize],
		   &uncoupled_chunk[z*sub_chunksize],
		   sub_chunksize);
	  } else {
	    ceph_assert(y == lost_chunk / q);
	    ceph_assert(node_sw == lost_chunk);
	    ceph_assert(helper_data.count(shard) > 0);
	    pft_erasures.insert(i1);
	    known_subchunks[i0].substr_of(helper_data[shard], repair_plane_to_ind[z_shard]*sub_chunksize, sub_chunksize);
	    known_subchunks[i2].substr_of((*U_buf)[shard], z*sub_chunksize, sub_chunksize);

	    pftsubchunks[i0] = known_subchunks[i0];
	    pftsubchunks[i1].substr_of(recovered_data[shard_id_t(node_sw)], z_sw*sub_chunksize, sub_chunksize);
	    pftsubchunks[i2] = known_subchunks[i2];
	    pftsubchunks[i3] = temp_buf;
	    for (shard_id_t i; i<3; ++i) {
	      pftsubchunks[i].rebuild_aligned(SIMD_ALIGN);
	    }
	    shard_id_map<bufferptr> in(q * t);
	    shard_id_map<bufferptr> out(q * t);
            for (auto&& [shard, list] : pftsubchunks) {
              auto bp = list.begin().get_current_ptr();
              if (shard == i1) out[shard] = bp;
              else in[shard] = bp;
            }
            pft.erasure_code->decode_chunks(pft_erasures, in, out);
	  }
	}
      } // recover all erasures
    } // planes of particular order
  } // order

  return 0;
}


int ErasureCodeClay::decode_layered(shard_id_set &erased_chunks,
                                    shard_id_map<bufferlist> *chunks)
{
  int num_erasures = erased_chunks.size();

  int size = (*chunks)[shard_id_t()].length();
  ceph_assert(size%sub_chunk_no == 0);
  int sc_size = size / sub_chunk_no;

  ceph_assert(num_erasures > 0);

  for (shard_id_t i(k+nu); (num_erasures < m) && (i < q*t); ++i) {
    if ([[maybe_unused]] auto [it, added] = erased_chunks.emplace(i); added) {
      num_erasures++;
      (void)it;  // silence -Wunused-variable
    }
  }
  ceph_assert(num_erasures == m);

  int max_iscore = get_max_iscore(erased_chunks);
  int order[sub_chunk_no];
  int z_vec[t];
  for (shard_id_t i(0); i < q*t; ++i) {
    if ((*U_buf)[i].length() == 0) {
      bufferptr buf(buffer::create_aligned(size, SIMD_ALIGN));
      buf.zero();
      (*U_buf)[i].push_back(std::move(buf));
    }
  }

  set_planes_sequential_decoding_order(order, erased_chunks);

  for (int iscore = 0; iscore <= max_iscore; iscore++) {
   for (int z = 0; z < sub_chunk_no; z++) {
      if (order[z] == iscore) {
        decode_erasures(erased_chunks, z, chunks, sc_size);
      }
    }

    for (int z = 0; z < sub_chunk_no; z++) {
      if (order[z] == iscore) {
	get_plane_vector(z, z_vec);
        for (auto shard : erased_chunks) {
          int node_xy = static_cast<int>(shard);
          int x = node_xy % q;
          int y = node_xy / q;
	  shard_id_t node_sw(y*q+z_vec[y]);
          if (z_vec[y] != x) {
            if (erased_chunks.count(node_sw) == 0) {
	      recover_type1_erasure(chunks, x, y, z, z_vec, sc_size);
	    } else if (z_vec[y] < x){
              ceph_assert(erased_chunks.count(node_sw) > 0);
              ceph_assert(z_vec[y] != x);
              get_coupled_from_uncoupled(chunks, x, y, z, z_vec, sc_size);
	    }
	  } else {
	    char* C = (*chunks)[shard_id_t(node_xy)].c_str();
            char* U = (*U_buf)[shard_id_t(node_xy)].c_str();
            memcpy(&C[z*sc_size], &U[z*sc_size], sc_size);
          }
        }
      }
    } // plane
  } // iscore, order

  return 0;
}

int ErasureCodeClay::decode_erasures(const shard_id_set& erased_chunks, int z,
				     shard_id_map<bufferlist>* chunks, int sc_size)
{
  int z_vec[t];

  get_plane_vector(z, z_vec);

  for (int x = 0; x < q; x++) {
    for (int y = 0; y < t; y++) {
      shard_id_t node_xy(q*y+x);
      shard_id_t node_sw(q*y+z_vec[y]);
      if (erased_chunks.count(node_xy) == 0) {
	if (z_vec[y] < x) {
	  get_uncoupled_from_coupled(chunks, x, y, z, z_vec, sc_size);
	} else if (z_vec[y] == x) {
	  char* uncoupled_chunk = (*U_buf)[node_xy].c_str();
	  char* coupled_chunk = (*chunks)[node_xy].c_str();
          memcpy(&uncoupled_chunk[z*sc_size], &coupled_chunk[z*sc_size], sc_size);
        } else {
          if (erased_chunks.count(node_sw) > 0) {
            get_uncoupled_from_coupled(chunks, x, y, z, z_vec, sc_size);
          }
        }
      }
    }
  }
  return decode_uncoupled(erased_chunks, z, sc_size);
}

int ErasureCodeClay::decode_uncoupled(const shard_id_set& erased_chunks, int z, int sc_size)
{
  shard_id_map<bufferlist> known_subchunks(q * t);
  shard_id_map<bufferlist> all_subchunks(q * t);

  for (shard_id_t i(0); i < q*t; ++i) {
    if (erased_chunks.count(i) == 0) {
      known_subchunks[i].substr_of((*U_buf)[i], z*sc_size, sc_size);
      all_subchunks[i] = known_subchunks[i];
    } else {
      all_subchunks[i].substr_of((*U_buf)[i], z*sc_size, sc_size);
    }
    all_subchunks[i].rebuild_aligned_size_and_memory(sc_size, SIMD_ALIGN);
    assert(all_subchunks[i].is_contiguous());
  }

  shard_id_map<bufferptr> in(q * t);
  shard_id_map<bufferptr> out(q * t);
  for (auto&& [shard, list] : all_subchunks) {
    auto bp = list.begin().get_current_ptr();
    if (erased_chunks.count(shard) == 0) in[shard] = bp;
    else out[shard] = bp;
  }
  mds.erasure_code->decode_chunks(erased_chunks, in, out);
  return 0;
}

void ErasureCodeClay::set_planes_sequential_decoding_order(int* order, shard_id_set& erasures) {
  int z_vec[t];
  for (int z = 0; z < sub_chunk_no; z++) {
    get_plane_vector(z,z_vec);
    order[z] = 0;
    for (auto shard : erasures) {
      int i = static_cast<int>(shard);
      if (i % q == z_vec[i / q]) {
	order[z] = order[z] + 1;
      }
    }
  }
}

void ErasureCodeClay::recover_type1_erasure(shard_id_map<bufferlist>* chunks,
					    int x, int y, int z,
					    int* z_vec, int sc_size)
{
  shard_id_set erased_chunks;

  shard_id_t node_xy(y*q+x);
  shard_id_t node_sw(y*q+z_vec[y]);
  int z_sw = z + (x - z_vec[y]) * pow_int(q,t-1-y);

  shard_id_map<bufferlist> known_subchunks(q * t);
  shard_id_map<bufferlist> pftsubchunks(q * t);
  bufferptr ptr(buffer::create_aligned(sc_size, SIMD_ALIGN));
  ptr.zero();

  shard_id_t i0, i1(1), i2(2), i3(3);
  if (z_vec[y] > x) {
    i0 = 1;
    i1 = 0;
    i2 = 3;
    i3 = 2;
  }

  erased_chunks.insert(i0);
  pftsubchunks[i0].substr_of((*chunks)[node_xy], z * sc_size, sc_size);
  known_subchunks[i1].substr_of((*chunks)[node_sw], z_sw * sc_size, sc_size);
  known_subchunks[i2].substr_of((*U_buf)[node_xy], z * sc_size, sc_size);
  pftsubchunks[i1] = known_subchunks[i1];
  pftsubchunks[i2] = known_subchunks[i2];
  pftsubchunks[i3].push_back(ptr);

  for (shard_id_t i; i<3; ++i) {
    pftsubchunks[i].rebuild_aligned_size_and_memory(sc_size, SIMD_ALIGN);
  }

  shard_id_map<bufferptr> in(q * t);
  shard_id_map<bufferptr> out(q * t);
  for (auto&& [shard, list] : pftsubchunks) {
    auto bp = list.begin().get_current_ptr();
    if (shard == i0) out[shard] = bp;
    else in[shard] = bp;
  }
  pft.erasure_code->decode_chunks(erased_chunks, in, out);
}

void ErasureCodeClay::get_coupled_from_uncoupled(shard_id_map<bufferlist>* chunks,
						 int x, int y, int z,
						 int* z_vec, int sc_size)
{
  shard_id_set erased_chunks;
  erased_chunks.insert_range(shard_id_t(0),2);

  shard_id_t node_xy(y*q+x);
  shard_id_t node_sw(y*q+z_vec[y]);
  int z_sw = z + (x - z_vec[y]) * pow_int(q,t-1-y);

  ceph_assert(z_vec[y] < x);
  shard_id_map<bufferlist> uncoupled_subchunks(q * t);
  uncoupled_subchunks[shard_id_t(2)].substr_of((*U_buf)[node_xy], z * sc_size, sc_size);
  uncoupled_subchunks[shard_id_t(3)].substr_of((*U_buf)[node_sw], z_sw * sc_size, sc_size);

  shard_id_map<bufferlist> pftsubchunks(q * t);
  pftsubchunks[shard_id_t(0)].substr_of((*chunks)[node_xy], z * sc_size, sc_size);
  pftsubchunks[shard_id_t(1)].substr_of((*chunks)[node_sw], z_sw * sc_size, sc_size);
  pftsubchunks[shard_id_t(2)] = uncoupled_subchunks[shard_id_t(2)];
  pftsubchunks[shard_id_t(3)] = uncoupled_subchunks[shard_id_t(3)];

  for (shard_id_t i; i<3; ++i) {
    pftsubchunks[i].rebuild_aligned_size_and_memory(sc_size, SIMD_ALIGN);
  }
  shard_id_map<bufferptr> in(q * t);
  shard_id_map<bufferptr> out(q * t);
  for (auto&& [shard, list] : pftsubchunks) {
    auto bp = list.begin().get_current_ptr();
    if (shard <= 1) out[shard] = bp;
    else in[shard] = bp;
  }
  pft.erasure_code->decode_chunks(erased_chunks, in, out);
}

void ErasureCodeClay::get_uncoupled_from_coupled(shard_id_map<bufferlist>* chunks,
						 int x, int y, int z,
						 int* z_vec, int sc_size)
{
  shard_id_set erased_chunks;
  erased_chunks.insert_range(shard_id_t(2),2);

  shard_id_t node_xy(y*q+x);
  shard_id_t node_sw(y*q+z_vec[y]);
  int z_sw = z + (x - z_vec[y]) * pow_int(q,t-1-y);

  shard_id_t i0, i1(1), i2(2), i3(3);
  if (z_vec[y] > x) {
    i0 = 1;
    i1 = 0;
    i2 = 3;
    i3 = 2;
  }
  shard_id_map<bufferlist> coupled_subchunks(k+m);
  coupled_subchunks[i0].substr_of((*chunks)[node_xy], z * sc_size, sc_size);
  coupled_subchunks[i1].substr_of((*chunks)[node_sw], z_sw * sc_size, sc_size);

  shard_id_map<bufferlist> pftsubchunks(k+m);
  pftsubchunks[shard_id_t(0)] = coupled_subchunks[shard_id_t(0)];
  pftsubchunks[shard_id_t(1)] = coupled_subchunks[shard_id_t(1)];
  pftsubchunks[i2].substr_of((*U_buf)[node_xy], z * sc_size, sc_size);
  pftsubchunks[i3].substr_of((*U_buf)[node_sw], z_sw * sc_size, sc_size);
  for (shard_id_t i; i<3; ++i) {
    pftsubchunks[i].rebuild_aligned_size_and_memory(sc_size, SIMD_ALIGN);
  }
  shard_id_map<bufferptr> in(q * t);
  shard_id_map<bufferptr> out(q * t);
  for (auto&& [shard, list] : pftsubchunks) {
    auto bp = list.begin().get_current_ptr();
    if (shard <= 1) in[shard] = bp;
    else out[shard] = bp;
  }
  pft.erasure_code->decode_chunks(erased_chunks, in, out);
}

int ErasureCodeClay::get_max_iscore(shard_id_set& erased_chunks)
{
  int weight_vec[t];
  int iscore = 0;
  memset(weight_vec, 0, sizeof(int)*t);

  for (auto shard : erased_chunks) {
    int i = static_cast<int>(shard);
    if (weight_vec[i / q] == 0) {
      weight_vec[i / q] = 1;
      iscore++;
    }
  }
  return iscore;
}

void ErasureCodeClay::get_plane_vector(int z, int* z_vec)
{
  for (int i = 0; i < t; i++) {
    z_vec[t-1-i] = z % q;
    z = (z - z_vec[t-1-i]) / q;
  }
}
