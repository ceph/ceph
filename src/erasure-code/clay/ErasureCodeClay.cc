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

#include <errno.h>
#include <algorithm>

#include "include/str_map.h"
#include "common/debug.h"
#include "osd/osd_types.h"
#include "include/stringify.h"
#include "erasure-code/ErasureCodePlugin.h"

#include "ErasureCodeClay.h"
#include "include/assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

#define LARGEST_VECTOR_WORDSIZE 16
#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

using namespace std;

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeClay: ";
}

static int pow_int(int a, int x){
  int power = 1;
  while (x)
    {
      if (x & 1) power *= a;
      x /= 2;
      a *= a;
    }
  return power;
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
  if (r) return r;
  r = registry.factory(pft.profile["plugin"],
		       directory,
		       pft.profile,
		       &pft.erasure_code,
		       ss);
  return r;

}

unsigned int ErasureCodeClay::get_chunk_size(unsigned int object_size) const
{
  unsigned alignment = get_alignment();
  unsigned tail = object_size % alignment;
  unsigned padded_length = object_size + ( tail ?  ( alignment - tail ) : 0 );
  
  assert(padded_length % (k*sub_chunk_no) == 0);
  
  return padded_length/k;
}

int ErasureCodeClay::minimum_to_decode(const set<int> &want_to_read,
				       const set<int> &available,
				       map<int, vector<pair<int, int>>> *minimum)
{
  set<int> minimum_shard_ids;

  if (is_repair(want_to_read, available)) {
    return minimum_to_repair(want_to_read, available, minimum);
  } else {
    return ErasureCode::minimum_to_decode(want_to_read, available, minimum);
  }
}

int ErasureCodeClay::decode(const set<int> &want_to_read,
			    const map<int, bufferlist> &chunks,
			    map<int, bufferlist> *decoded, int chunk_size)
{
  set<int> avail;
  for (map<int, bufferlist>::const_iterator i = chunks.begin();
       i != chunks.end(); ++i) {
    avail.insert(i->first);
  }

  if (is_repair(want_to_read, avail)) {
    return repair(want_to_read, chunks, decoded, chunk_size);
  } else {
    return ErasureCode::_decode(want_to_read, chunks, decoded);
  }
}

void p(const set<int> &s) { cerr << s; } // for gdb

int ErasureCodeClay::encode_chunks(const set<int> &want_to_encode,
				   map<int, bufferlist> *encoded)
{
  map<int, bufferlist> chunks;
  set<int> parity_chunks;
  int chunk_size = (*encoded)[0].length();
  
  for (int i = 0; i < k + m; i++) {
    if (i < k) {
      chunks[i] = (*encoded)[i];
    } else {
      chunks[i+nu] = (*encoded)[i];
      parity_chunks.insert(i+nu);
    }
  }
  
  for (int i = k; i < k+nu; i++) {
    bufferptr buf(buffer::create_aligned(chunk_size, SIMD_ALIGN));
    buf.zero();
    chunks[i].push_back(std::move(buf));  
  }
  
  int res = decode_layered(parity_chunks, &chunks);
  for (int i = k ; i < k+nu; i++) {
    //need to clean some of the intermediate chunks here!!
    chunks[i].clear();
  }
  return res;
}

int ErasureCodeClay::decode_chunks(const set<int> &want_to_read,
				   const map<int, bufferlist> &chunks,
				   map<int, bufferlist> *decoded)
{
  set<int> erasures;
  map<int, bufferlist> coded_chunks;
 
  for (int i = 0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      erasures.insert(i < k ? i : i+nu);
    }
    assert((*decoded).find(i) != (*decoded).end());
    coded_chunks[i < k ? i : i+nu] = (*decoded)[i];
  }
  int chunk_size = coded_chunks[0].length();

  for (int i = k; i < k+nu; i++) {
    bufferptr buf(buffer::create_aligned(chunk_size, SIMD_ALIGN));
    buf.zero();
    coded_chunks[i].push_back(std::move(buf));  
  }
	
  int res = decode_layered(erasures, &coded_chunks);
  for (int i = k; i < k+nu; i++) {
    coded_chunks[i].clear();  
  }
  return res;
}

unsigned int ErasureCodeClay::get_alignment() const
{
  unsigned alignment = k*sub_chunk_no*w*sizeof(int);
  if ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE)
    alignment = k*sub_chunk_no*w*LARGEST_VECTOR_WORDSIZE;
  return alignment;
}

int ErasureCodeClay::parse(ErasureCodeProfile &profile,
			   ostream *ss)
{
  int err = 0;
  err = ErasureCode::parse(profile, ss);
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  err |= to_int("m", profile, &m, DEFAULT_M, ss);
  
  err |= sanity_check_k(k, ss);

  err |= to_int("d", profile, &d, std::to_string(k+m-1), ss);

  //check for scalar_mds in profile input
  if (profile.find("scalar_mds") == profile.end() ||
      profile.find("scalar_mds")->second.size() == 0) {
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
      profile.find("technique")->second.size() == 0) {
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
        *ss << "technique " << p << "is not currently supported, use one of "<< 
               "reed_sol_van', 'reed_sol_r6_op','cauchy_orig'," <<
               "'cauchy_good','liber8tion'"<< std::endl;
        err = -EINVAL;
        return err;
      }
    } else if (mds.profile["plugin"] == "isa") {
      if ( (p == "reed_sol_van") || (p == "cauchy")) {
        mds.profile["technique"] = p;
        pft.profile["technique"] = p;
      } else {
        *ss << "technique " << p << "is not currently supported, use one of"<<
               "'reed_sol_van','cauchy'"<< std::endl;
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
  if ((d < k) || (d > k+m-1)) {
    *ss << "value of d " << d
        << " must be within [ " << k << "," << k+m-1 << "]" << std::endl;
    err = -EINVAL;
    return err;
  }
  
  q = d-k+1;
  if ((k+m)%q) {
    nu = q - (k+m)%q;
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
    
  t = (k+m+nu)/q;
  sub_chunk_no = pow_int(q, t); 

  dout(10) << __func__ << " (q,t,nu)=("<<q<<","<<t<<","<<nu<<")"<<dendl;

  return err;
}

int ErasureCodeClay::is_repair(const set<int> &want_to_read,
			       const set<int> &available_chunks) {

  if (includes(available_chunks.begin(), available_chunks.end(), 
               want_to_read.begin(), want_to_read.end())) return 0;
  if (want_to_read.size() > 1) return 0;

  int i = *want_to_read.begin();
  int lost_node_id = (i < k) ? i: i+nu;
  for (int x = 0; x < q; x++) {
    int node = (lost_node_id/q)*q+x;
    node = (node < k) ? node : node-nu;
    if (node != i) {//node in the same group other than erased node
      if (available_chunks.find(node) == available_chunks.end()) return 0;
    }
  }

  if (available_chunks.size() < (unsigned)d) return 0;
  return 1;
}

int ErasureCodeClay::minimum_to_repair(const set<int> &want_to_read,
				       const set<int> &available_chunks,
				       map<int, vector<pair<int, int>>> *minimum)
{
  int i = *want_to_read.begin();
  int lost_node_index = (i < k) ? i : i+nu;
  int rep_node_index = 0;

  //add all the nodes in lost node's y column.
  vector<pair<int, int>> sub_chunk_ind;
  get_repair_subchunks(lost_node_index, sub_chunk_ind);
  if ((available_chunks.size() >= (unsigned)d)) {
    for (int j = 0; j < q; j++) {  
      if (j != lost_node_index%q) {
        rep_node_index = (lost_node_index/q)*q+j;
        if (rep_node_index < k) {
          minimum->insert(make_pair(rep_node_index, sub_chunk_ind));
        }
        else if (rep_node_index >= k+nu) {
          minimum->insert(make_pair(rep_node_index-nu, sub_chunk_ind));
        }
      }
    }
    for (set<int>::iterator i = available_chunks.begin();
	   i != available_chunks.end(); ++i) {
      if (minimum->size() < (unsigned)d) {
        if (minimum->find(*i) == minimum->end()) minimum->insert(make_pair(*i, sub_chunk_ind));
      } else break;
    }
  } else {
    dout(0) << "minimum_to_repair: shouldn't have come here" << dendl;
    assert(0);
  }
  assert(minimum->size() == (unsigned)d);
  return 0;
}

void ErasureCodeClay::get_repair_subchunks(const int &lost_node,
					   vector<pair<int, int>> &repair_sub_chunks_ind)
{
  int y_lost = lost_node/q;
  int x_lost = lost_node%q;

  int seq_sc_count = pow(q, t-1-y_lost);
  int num_seq = pow(q, y_lost);

  for (int ind_seq = 0; ind_seq < num_seq; ind_seq++) {
    repair_sub_chunks_ind.push_back(make_pair(ind_seq*q*seq_sc_count+x_lost*seq_sc_count, seq_sc_count));
  }
}

int ErasureCodeClay::get_repair_sub_chunk_count(const set<int> &want_to_read)
{
  int repair_subchunks_count = 1;

  int weight_vector[t];
  memset(weight_vector, 0, t*sizeof(int));

  for (set<int>::iterator i = want_to_read.begin();
       i != want_to_read.end(); ++i) {
    weight_vector[(*i)/q]++;
  }

  for (int y = 0; y < t; y++) repair_subchunks_count = repair_subchunks_count*(q-weight_vector[y]);

  return sub_chunk_no - repair_subchunks_count;
}

int ErasureCodeClay::repair(const set<int> &want_to_read,
			    const map<int, bufferlist> &chunks,
			    map<int, bufferlist> *repaired, int chunk_size)
{

  assert((want_to_read.size() == 1) && (chunks.size() == (unsigned)d));

  int repair_sub_chunk_no = get_repair_sub_chunk_count(want_to_read);
  vector<pair<int, int>> repair_sub_chunks_ind;

  unsigned repair_blocksize = (*chunks.begin()).second.length();
  assert(repair_blocksize%repair_sub_chunk_no == 0);

  unsigned sub_chunksize = repair_blocksize/repair_sub_chunk_no;
  unsigned chunksize = sub_chunk_no*sub_chunksize;

  assert(chunksize == (unsigned)chunk_size);

  map<int, bufferlist> recovered_data;
  map<int, bufferlist> helper_data;
  set<int> aloof_nodes;

  for (int i =  0; i < k + m; i++) {
    //included helper data only for d+nu nodes.
    if (chunks.find(i) != chunks.end()) {//i is a helper 
      if (i<k) helper_data[i] = (chunks.find(i))->second;
      else helper_data[i+nu] = (chunks.find(i))->second;  
    } else {
      if (i != *want_to_read.begin()) {//aloof node case.
        int aloof_node_id = (i < k) ? i: i+nu;
        aloof_nodes.insert(aloof_node_id);
      } else {
        bufferptr ptr(buffer::create_aligned(chunksize, SIMD_ALIGN)); 
	ptr.zero();
        int lost_node_id = (i < k) ? i : i+nu;
        (*repaired)[i].push_back(ptr);
        recovered_data[lost_node_id] = (*repaired)[i];
        get_repair_subchunks(lost_node_id, repair_sub_chunks_ind);
      }
    }
  }

  //this is for shortened codes i.e., when nu > 0
  for (int i=k; i < k+nu; i++) {
    bufferptr ptr(buffer::create_aligned(repair_blocksize, SIMD_ALIGN)); 
    ptr.zero();
    helper_data[i].push_back(ptr);
  }

  assert(helper_data.size()+aloof_nodes.size()+recovered_data.size() == (unsigned) q*t);
  
  int r = repair_one_lost_chunk(recovered_data, aloof_nodes,
			     helper_data, repair_blocksize, repair_sub_chunks_ind);

  //clear buffers created for the purpose of shortening
  for (int i = k; i < k+nu; i++) {
    helper_data[i].clear();
  }

  return r;
}

int ErasureCodeClay::repair_one_lost_chunk(map<int, bufferlist> &recovered_data, set<int> &aloof_nodes,
                                        map<int, bufferlist> &helper_data, int repair_blocksize, vector<pair<int,int>> &repair_sub_chunks_ind)
{
  unsigned repair_subchunks = (unsigned)sub_chunk_no/q;
  unsigned sub_chunksize = repair_blocksize/repair_subchunks;

  int z_vec[t];
  map<int, set<int> > ordered_planes;
  map<int, int> repair_plane_to_ind;
  int order = 0;
  int x,y, node_xy, node_sw, z_sw;
  int count_retrieved_sub_chunks = 0;
  int plane_ind = 0;

  bufferptr buf(buffer::create_aligned(sub_chunksize, SIMD_ALIGN));
  bufferlist temp_buf;
  temp_buf.push_back(buf);

  for (vector<pair<int,int>>::iterator i = repair_sub_chunks_ind.begin();
       i != repair_sub_chunks_ind.end(); ++i) {
    for (int j = i->first; j < i->first+i->second; j++) {
      get_plane_vector(j, z_vec);
      order = 0;
      //check across all erasures and aloof nodes
      for (map<int, bufferlist>::iterator l = recovered_data.begin();
           l != recovered_data.end(); ++l)
      {
        if ((l->first)%q == z_vec[(l->first)/q]) order++;
      }
      for (set<int>::iterator l = aloof_nodes.begin();
           l != aloof_nodes.end(); ++l) {
        if ((*l)%q == z_vec[(*l)/q]) order++;
      }
      assert(order>0);
      ordered_planes[order].insert(j);
      //to keep track of a sub chunk within helper buffer recieved
      repair_plane_to_ind[j] = plane_ind;
      plane_ind++;
    }
  }
  assert((unsigned)plane_ind == repair_subchunks);
  
  int plane_count = 0;
  for (int i = 0; i < q*t; i++) {
    if (U_buf[i].length() == 0) {
      bufferptr buf(buffer::create_aligned(sub_chunk_no*sub_chunksize, SIMD_ALIGN));
      buf.zero();
      U_buf[i].push_back(std::move(buf));
    }
  }

  int lost_chunk;
  int count = 0;
  for (map<int, bufferlist>::iterator i=recovered_data.begin();
      i != recovered_data.end(); ++i) {
    lost_chunk = i->first;
    count++;
  }
  assert(count==1);

  set<int> erasures;
  for (int i=0; i<q; i++) {
    erasures.insert(lost_chunk - lost_chunk%q + i);
  }
  for (set<int>::iterator i=aloof_nodes.begin();
       i != aloof_nodes.end(); ++i) {
    erasures.insert(*i);
  }
  
  for (order=1; ;order++) {
    if (ordered_planes.find(order) == ordered_planes.end()) break;
    else {
      plane_count += ordered_planes[order].size();
      for (set<int>::iterator z=ordered_planes[order].begin();
          z != ordered_planes[order].end(); ++z)
      { 
        get_plane_vector(*z, z_vec);
          
        for (y=0; y < t; y++) {
          for (x = 0; x < q; x++) {
            node_xy = y*q + x;
            map<int, bufferlist> known_subchunks;
            map<int, bufferlist> pftsubchunks;
            set<int> pft_erasures;
            if (erasures.find(node_xy) == erasures.end())
            { 
              assert(helper_data.find(node_xy) != helper_data.end());
              z_sw = (*z) + (x - z_vec[y])*pow_int(q,t-1-y);
              node_sw = y*q + z_vec[y];
              int i0=0, i1=1, i2=2, i3=3;
              if (z_vec[y] > x) {
                  i0=1;
                  i1=0;
                  i2=3;
                  i3=2;
              }
              if (aloof_nodes.find(node_sw) != aloof_nodes.end()) {
                assert(repair_plane_to_ind.find(*z) != repair_plane_to_ind.end());
                assert(repair_plane_to_ind.find(z_sw) != repair_plane_to_ind.end());
                pft_erasures.insert(i2);
                known_subchunks[i0].substr_of(helper_data[node_xy], repair_plane_to_ind[*z]*sub_chunksize, sub_chunksize);
                known_subchunks[i3].substr_of(U_buf[node_sw], z_sw*sub_chunksize, sub_chunksize);
                pftsubchunks[i0] = known_subchunks[i0];
                pftsubchunks[i1] = temp_buf;
                pftsubchunks[i2].substr_of(U_buf[node_xy], (*z)*sub_chunksize, sub_chunksize);
                pftsubchunks[i3] = known_subchunks[i3];
                for (int i=0; i<3; i++) {
                  pftsubchunks[i].rebuild_aligned(SIMD_ALIGN);
                }
                pft.erasure_code->decode_chunks(pft_erasures, known_subchunks, &pftsubchunks);
              } else {
                assert(helper_data.find(node_sw) != helper_data.end());
                assert(repair_plane_to_ind.find(*z) != repair_plane_to_ind.end());
                if( z_vec[y] != x){
                  pft_erasures.insert(i2);
                  assert(repair_plane_to_ind.find(z_sw) != repair_plane_to_ind.end());
                  known_subchunks[i0].substr_of(helper_data[node_xy], repair_plane_to_ind[*z]*sub_chunksize, sub_chunksize);
                  known_subchunks[i1].substr_of(helper_data[node_sw], repair_plane_to_ind[z_sw]*sub_chunksize, sub_chunksize);
                  pftsubchunks[i0] = known_subchunks[i0];
                  pftsubchunks[i1] = known_subchunks[i1];
                  pftsubchunks[i2].substr_of(U_buf[node_xy], (*z)*sub_chunksize, sub_chunksize);
                  pftsubchunks[i3].substr_of(temp_buf, 0, sub_chunksize);
                  for (int i=0; i<3; i++) {
                    pftsubchunks[i].rebuild_aligned(SIMD_ALIGN);
                  }
                  pft.erasure_code->decode_chunks(pft_erasures, known_subchunks, &pftsubchunks);
                } else {
                  char* uncoupled_chunk = U_buf[node_xy].c_str();
                  char* coupled_chunk = helper_data[node_xy].c_str();
                  memcpy(&uncoupled_chunk[(*z)*sub_chunksize], &coupled_chunk[repair_plane_to_ind[*z]*sub_chunksize], sub_chunksize);
                }
              }
            }
          }//x
        }//y
        assert(erasures.size() <= (unsigned)m);
        decode_uncoupled(erasures, *z, sub_chunksize);
       
        for (set<int>::iterator i = erasures.begin();
             i != erasures.end(); ++i) {
          x = (*i)%q;
          y = (*i)/q;
          node_sw = y*q+z_vec[y];
          z_sw = (*z) + (x - z_vec[y]) * pow_int(q,t-1-y);
          set<int> pft_erasures;
          map<int, bufferlist> known_subchunks;
          map<int, bufferlist> pftsubchunks;
          int i0=0, i1=1, i2=2, i3=3;
          if (z_vec[y] > x) {
            i0=1;
            i1=0;
            i2=3;
            i3=2;
          }
          //make sure it is not an aloof node before you retrieve repaired_data
          if (aloof_nodes.find(*i) == aloof_nodes.end()) {
            if (x == z_vec[y]) {//hole-dot pair (type 0)
              char* coupled_chunk = recovered_data[*i].c_str();
              char* uncoupled_chunk = U_buf[*i].c_str();
              memcpy(&coupled_chunk[(*z)*sub_chunksize], &uncoupled_chunk[(*z)*sub_chunksize], sub_chunksize);
              count_retrieved_sub_chunks++;
            }
            else {
              assert(y == lost_chunk/q);
              assert(node_sw == lost_chunk);
              assert(helper_data.find(*i) != helper_data.end());
              pft_erasures.insert(i1);
              known_subchunks[i0].substr_of(helper_data[*i], repair_plane_to_ind[*z]*sub_chunksize, sub_chunksize);
              known_subchunks[i2].substr_of(U_buf[*i], (*z)*sub_chunksize, sub_chunksize);

              pftsubchunks[i0] = known_subchunks[i0];
              pftsubchunks[i1].substr_of(recovered_data[node_sw], z_sw*sub_chunksize, sub_chunksize);
              pftsubchunks[i2] = known_subchunks[i2];
              pftsubchunks[i3] = temp_buf;
              for ( int i=0; i<3; i++) {
               pftsubchunks[i].rebuild_aligned(SIMD_ALIGN);
              }
              pft.erasure_code->decode_chunks(pft_erasures, known_subchunks, &pftsubchunks);
            }
          }
        }//recover all erasures
      }//planes of particular order
    }
  }//order

  return 0;
}


int ErasureCodeClay::decode_layered(set<int> &erased_chunks,
                                    map<int, bufferlist> *chunks)
{
  int x, y;
  int z, node_sw;
  int num_erasures = erased_chunks.size();

  int size = (*chunks)[0].length();
  assert(size%sub_chunk_no == 0);
  int sc_size = size/sub_chunk_no;

  if (!num_erasures) assert(0);
  
  int i = k+nu;
  while ((num_erasures < m) && (i < q*t)) {
    if (erased_chunks.find(i) == erased_chunks.end()) {     
      erased_chunks.insert(i);
      num_erasures++;
    }
    i++;
  }
  assert(num_erasures == m);
  
  int max_iscore = get_max_iscore(erased_chunks);
  int order[sub_chunk_no];
  int z_vec[t];
  for (i = 0; i < q*t; i++) {
    if (U_buf[i].length() == 0) {
      bufferptr buf(buffer::create_aligned(size, SIMD_ALIGN));
      buf.zero();
      U_buf[i].push_back(std::move(buf));  
    }
  } 

  set_planes_sequential_decoding_order(order, erased_chunks);

  for (int iscore = 0; iscore <= max_iscore; iscore++) {
   for (z = 0; z < sub_chunk_no; z++) {
      if (order[z] == iscore) {
        decode_erasures(erased_chunks, z, chunks, sc_size);
      }
    }

    for (z = 0; z < sub_chunk_no; z++) {
      if (order[z] == iscore) {
	get_plane_vector(z,z_vec);
        for (set<int>::iterator node_xy = erased_chunks.begin(); node_xy != erased_chunks.end(); ++node_xy) {
          x = (*node_xy)%q;
          y = (*node_xy)/q;
	  node_sw = y*q+z_vec[y];
          if (z_vec[y] != x) {
            if (erased_chunks.find(node_sw) == erased_chunks.end()) {
	      recover_type1_erasure(chunks, x, y, z, z_vec, sc_size);
	    } else if (z_vec[y] < x){
              assert(erased_chunks.find(node_sw) != erased_chunks.end());
              assert(z_vec[y] != x);
              get_coupled_from_uncoupled(chunks, x, y, z, z_vec, sc_size);
	    }
	  } else {
	    char* C = (*chunks)[*node_xy].c_str();
            char* U = U_buf[*node_xy].c_str();
            memcpy(&C[z*sc_size], &U[z*sc_size], sc_size);
          }
        }
      }
    }//plane
  }//iscore, order

  return 0;
}

int ErasureCodeClay::decode_erasures(const set<int>& erased_chunks, int z,
				     map<int, bufferlist>* chunks, int sc_size)
{
  int x, y;
  int node_xy, node_sw;
  int z_vec[t];

  get_plane_vector(z,z_vec);

  for (x=0; x < q; x++) {
    for (y=0; y < t; y++) {
      node_xy = q*y+x;
      node_sw = q*y+z_vec[y];
      if (erased_chunks.find(node_xy) == erased_chunks.end()) { 
	if (z_vec[y] < x) {
	  get_uncoupled_from_coupled(chunks, x, y, z, z_vec, sc_size);
	} else if (z_vec[y] == x) {
	  char* uncoupled_chunk = U_buf[node_xy].c_str();
	  char* coupled_chunk = (*chunks)[node_xy].c_str();
          memcpy(&uncoupled_chunk[z*sc_size], &coupled_chunk[z*sc_size], sc_size);
        } else {
          if (erased_chunks.find(node_sw) != erased_chunks.end()) {
            get_uncoupled_from_coupled(chunks, x, y, z, z_vec, sc_size);
          } 
        }
      }
    }
  }
  int r = decode_uncoupled(erased_chunks, z, sc_size);
  return r;
}

int ErasureCodeClay::decode_uncoupled(const set<int>& erased_chunks, int z, int sc_size)
{
  map<int, bufferlist> known_subchunks;
  map<int, bufferlist> allsubchunks;
	
  for (int i = 0; i < q*t; i++) {
    if ( erased_chunks.find(i) == erased_chunks.end() ) {
      known_subchunks[i].substr_of(U_buf[i], z*sc_size, sc_size);
      allsubchunks[i] = known_subchunks[i];
    } else {
      allsubchunks[i].substr_of(U_buf[i], z*sc_size, sc_size);
    }
    allsubchunks[i].rebuild_aligned_size_and_memory(sc_size, SIMD_ALIGN);
    assert(allsubchunks[i].is_contiguous());
  }

  mds.erasure_code->decode_chunks(erased_chunks, known_subchunks, &allsubchunks);
  return 0;
}

void ErasureCodeClay::set_planes_sequential_decoding_order(int* order, set<int>& erasures){
  int z;
  int z_vec[t];
  for (z = 0; z< sub_chunk_no; z++) {
    get_plane_vector(z,z_vec);
    order[z] = 0;
    for (set<int>::iterator i = erasures.begin(); i != erasures.end() ; ++i) {
      if ((*i)%q == z_vec[(*i)/q]) {
	order[z] = order[z]+1;
      }
    }
  }
}

void ErasureCodeClay::recover_type1_erasure(map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size)
{
  set<int> erased_chunks;
	
  int node_xy = y*q+x; 
  int node_sw = y*q+z_vec[y];
  int z_sw = z + (x - z_vec[y]) * pow_int(q,t-1-y);

  map<int, bufferlist> known_subchunks;
  map<int, bufferlist> pftsubchunks;
  bufferptr ptr(buffer::create_aligned(sc_size, SIMD_ALIGN));
  ptr.zero();

  int i0=0, i1=1, i2=2, i3=3;
  if (z_vec[y] > x) {
    i0 = 1;
    i1 = 0;
    i2 = 3;
    i3 = 2;
  }
  
  erased_chunks.insert(i0);
  pftsubchunks[i0].substr_of((*chunks)[node_xy], z * sc_size, sc_size);
  known_subchunks[i1].substr_of((*chunks)[node_sw], z_sw * sc_size, sc_size);
  known_subchunks[i2].substr_of(U_buf[node_xy], z * sc_size, sc_size);
  pftsubchunks[i1] = known_subchunks[i1];
  pftsubchunks[i2] = known_subchunks[i2]; 
  pftsubchunks[i3].push_back(ptr);

  for (int i=0; i<3; i++) {
    pftsubchunks[i].rebuild_aligned_size_and_memory(sc_size, SIMD_ALIGN);
  }

  pft.erasure_code->decode_chunks(erased_chunks, known_subchunks, &pftsubchunks);	
}

void ErasureCodeClay::get_coupled_from_uncoupled(map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size)
{
  set<int> erased_chunks;
  erased_chunks.insert(0);
  erased_chunks.insert(1);
	
  int node_xy = y*q+x; 
  int node_sw = y*q+z_vec[y];
  int z_sw = z + (x - z_vec[y]) * pow_int(q,t-1-y);

  assert(z_vec[y] < x);
  map<int, bufferlist> uncoupled_subchunks;
  uncoupled_subchunks[2].substr_of(U_buf[node_xy], z * sc_size, sc_size);
  uncoupled_subchunks[3].substr_of(U_buf[node_sw], z_sw * sc_size, sc_size);

  map<int, bufferlist> pftsubchunks;
  pftsubchunks[0].substr_of((*chunks)[node_xy], z * sc_size, sc_size);
  pftsubchunks[1].substr_of((*chunks)[node_sw], z_sw * sc_size, sc_size);
  pftsubchunks[2] = uncoupled_subchunks[2];
  pftsubchunks[3] = uncoupled_subchunks[3];

  for (int i=0; i<3; i++) {
    pftsubchunks[i].rebuild_aligned_size_and_memory(sc_size, SIMD_ALIGN);
  }
  pft.erasure_code->decode_chunks(erased_chunks, uncoupled_subchunks, &pftsubchunks);
}

void ErasureCodeClay::get_uncoupled_from_coupled(map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size)
{
  set<int> erased_chunks;
  erased_chunks.insert(2);
  erased_chunks.insert(3);

  int node_xy = y*q+x; 
  int node_sw = y*q+z_vec[y];
  int z_sw = z + (x - z_vec[y]) * pow_int(q,t-1-y);

  int i0=0, i1=1, i2=2, i3=3;
  if (z_vec[y] > x) {
    i0=1;
    i1=0;
    i2=3;
    i3=2;
  }
  map<int, bufferlist> coupled_subchunks;
  coupled_subchunks[i0].substr_of((*chunks)[node_xy], z * sc_size, sc_size);
  coupled_subchunks[i1].substr_of((*chunks)[node_sw], z_sw * sc_size, sc_size);

  map<int, bufferlist> pftsubchunks;
  pftsubchunks[0] = coupled_subchunks[0];
  pftsubchunks[1] = coupled_subchunks[1];
  pftsubchunks[i2].substr_of(U_buf[node_xy], z * sc_size, sc_size);
  pftsubchunks[i3].substr_of(U_buf[node_sw], z_sw * sc_size, sc_size);
  for (int i=0; i<3; i++) {
    pftsubchunks[i].rebuild_aligned_size_and_memory(sc_size, SIMD_ALIGN);
  }
  pft.erasure_code->decode_chunks(erased_chunks, coupled_subchunks, &pftsubchunks);
}

int ErasureCodeClay::get_max_iscore(set<int>& erased_chunks)
{
  int weight_vec[t];
  int iscore=0;
  memset(weight_vec, 0, sizeof(int)*t);

  for (set<int>::iterator i = erased_chunks.begin(); 
             i != erased_chunks.end(); ++i) {
    if (weight_vec[(*i)/q]==0) {
      weight_vec[(*i)/q]=1;
      iscore++;
    }
  }
  return iscore;
}

void ErasureCodeClay::get_plane_vector(int z, int* z_vec)
{
  int i ;

  for (i = 0; i < t; i++ ) {
    z_vec[t-1-i] = z%q;
    z = (z - z_vec[t-1-i])/q;
  }
  return;
}
