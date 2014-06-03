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

// -----------------------------------------------------------------------------
#include <algorithm>
#include <dlfcn.h>
#include <errno.h>
// -----------------------------------------------------------------------------
#include "common/debug.h"
#include "ErasureCodeIsa.h"
#include "xor_op.h"
#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
// -----------------------------------------------------------------------------
extern "C" {
#include "isa-l/include/erasure_code.h"
}
// -----------------------------------------------------------------------------
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

static ostream&
_prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeIsa: ";
}
// -----------------------------------------------------------------------------

int
ErasureCodeIsa::create_ruleset(const string &name,
                               CrushWrapper &crush,
                               ostream *ss) const
{
  return crush.add_simple_ruleset(name, ruleset_root, ruleset_failure_domain,
                                  "indep", pg_pool_t::TYPE_ERASURE, ss);
}

// -----------------------------------------------------------------------------

void
ErasureCodeIsa::init(const map<string, string> &parameters)
{
  dout(10) << "technique=" << technique << dendl;
  map<string, string>::const_iterator parameter;
  parameter = parameters.find("ruleset-root");
  if (parameter != parameters.end())
    ruleset_root = parameter->second;
  parameter = parameters.find("ruleset-failure-domain");
  if (parameter != parameters.end())
    ruleset_failure_domain = parameter->second;
  parse(parameters);
  prepare();
}

// -----------------------------------------------------------------------------

unsigned int
ErasureCodeIsa::get_chunk_size(unsigned int object_size) const
{
  unsigned alignment = get_alignment();
  unsigned tail = object_size % alignment;
  unsigned padded_length = object_size + (tail ? (alignment - tail) : 0);
  assert(padded_length % k == 0);
  return padded_length / k;
}

// -----------------------------------------------------------------------------

int
ErasureCodeIsa::minimum_to_decode(const set<int> &want_to_read,
                                  const set<int> &available_chunks,
                                  set<int> *minimum)
{
  if (includes(available_chunks.begin(), available_chunks.end(),
               want_to_read.begin(), want_to_read.end())) {
    *minimum = want_to_read;
  } else {
    if (available_chunks.size() < (unsigned) k)
      return -EIO;
    set<int>::iterator i;
    unsigned j;
    for (i = available_chunks.begin(), j = 0; j < (unsigned) k; ++i, j++)
      minimum->insert(*i);
  }
  return 0;
}

// -----------------------------------------------------------------------------

int
ErasureCodeIsa::minimum_to_decode_with_cost(const set<int> &want_to_read,
                                            const map<int, int> &available,
                                            set<int> *minimum)
{
  set <int> available_chunks;
  for (map<int, int>::const_iterator i = available.begin();
       i != available.end();
       ++i)
    available_chunks.insert(i->first);
  return minimum_to_decode(want_to_read, available_chunks, minimum);
}

int ErasureCodeIsa::encode_chunks(const set<int> &want_to_encode,
                                  map<int, bufferlist> *encoded)
{
  char *chunks[k + m];
  for (int i = 0; i < k + m; i++)
    chunks[i] = (*encoded)[i].c_str();
  isa_encode(&chunks[0], &chunks[k], (*encoded)[0].length());
  return 0;
}

int ErasureCodeIsa::decode_chunks(const set<int> &want_to_read,
                                  const map<int, bufferlist> &chunks,
                                  map<int, bufferlist> *decoded)
{
  unsigned blocksize = (*chunks.begin()).second.length();
  int erasures[k + m + 1];
  int erasures_count = 0;
  char *data[k];
  char *coding[m];
  for (int i =  0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      erasures[erasures_count] = i;
      erasures_count++;
    }
    if (i < k)
      data[i] = (*decoded)[i].c_str();
    else
      coding[i - k] = (*decoded)[i].c_str();
  }
  erasures[erasures_count] = -1;
  assert(erasures_count > 0);
  return isa_decode(erasures, data, coding, blocksize);
}

// -----------------------------------------------------------------------------

int
ErasureCodeIsa::to_int(const std::string &name,
                       const map<std::string, std::string> &parameters,
                       int default_value)
{
  if (parameters.find(name) == parameters.end() ||
      parameters.find(name)->second.size() == 0) {
    dout(10) << name << " defaults to " << default_value << dendl;
    return default_value;
  }
  const std::string value = parameters.find(name)->second;
  std::string p = value;
  std::string err;
  int r = strict_strtol(p.c_str(), 10, &err);
  if (!err.empty()) {
    derr << "could not convert " << name << "=" << value
      << " to int because " << err
      << ", set to default " << default_value << dendl;
    return default_value;
  }
  dout(10) << name << " set to " << r << dendl;
  return r;
}

// -----------------------------------------------------------------------------

void
ErasureCodeIsaDefault::isa_encode(char **data,
                                  char **coding,
                                  int blocksize)
{
  
  if (m==1)
    // single parity stripe
    region_xor( (unsigned char**) data, (unsigned char*) coding[0], k, blocksize );
  else
    ec_encode_data(blocksize, k, m, g_encode_tbls,
		   (unsigned char**) data, (unsigned char**) coding);
}

// -----------------------------------------------------------------------------

bool
ErasureCodeIsaDefault::erasure_contains(int *erasures, int i)
{
  for (int l = 0; erasures[l] != -1; l++) {
    if (erasures[l] == i)
      return true;
  }
  return false;
}

// -----------------------------------------------------------------------------

bool
ErasureCodeIsaDefault::get_decoding_table_from_cache(std::string &signature, unsigned char* &table)
{
  // --------------------------------------------------------------------------
  // LRU decoding matrix cache
  // --------------------------------------------------------------------------

  dout(12) << "[ get table    ] = " << signature << dendl;

  // we try to fetch a decoding table from an LRU cache
  bool found = false;

  Mutex::Locker lock(g_decode_tbls_guard);
  if (g_decode_tbls_map.count(signature)) {
    dout(12) << "[ cached table ] = " << signature << dendl;
    // copy the table out of the cache
    memcpy(table, g_decode_tbls_map[signature].second.c_str(), k * (m + k)*32);
    // find item in LRU queue and push back
    dout(12) << "[ cache size   ] = " << g_decode_tbls_lru.size() << dendl;
    g_decode_tbls_lru.splice(g_decode_tbls_map[signature].first, g_decode_tbls_lru, g_decode_tbls_lru.end());
    found = true;
  }

  return found;
}

// -----------------------------------------------------------------------------

void
ErasureCodeIsaDefault::put_decoding_table_to_cache(std::string &signature, unsigned char* &table)
{
  // --------------------------------------------------------------------------
  // LRU decoding matrix cache
  // --------------------------------------------------------------------------

  dout(12) << "[ put table    ] = " << signature << dendl;

  // we store a new table to the cache

  bufferptr cachetable;

  Mutex::Locker lock(g_decode_tbls_guard);

  // evt. shrink the LRU queue/map
  if ((int)g_decode_tbls_lru.size() >= g_decode_tbls_lru_length) {
    dout(12) << "[ shrink lru   ] = " << signature << dendl;
    // reuse old buffer
    cachetable = g_decode_tbls_map[g_decode_tbls_lru.front()].second;
    // remove from map
    g_decode_tbls_map.erase(g_decode_tbls_lru.front());
    // remove from lru
    g_decode_tbls_lru.pop_front();
    // add the new to the map
    g_decode_tbls_map[signature] = std::make_pair(g_decode_tbls_lru.begin(), cachetable);
    // add to the end of lru
    g_decode_tbls_lru.push_back(signature);
  } else {
    dout(12) << "[ store table  ] = " << signature << dendl;
    // allocate a new buffer
    cachetable = buffer::create(k * (m + k)*32);
    g_decode_tbls_lru.push_back(signature);
    g_decode_tbls_map[signature] = std::make_pair(g_decode_tbls_lru.begin(), cachetable);
    dout(12) << "[ cache size   ] = " << g_decode_tbls_lru.size() << dendl;
  }

  // copy-in the new table
  memcpy(cachetable.c_str(), table, k * (m + k)*32);
}

// -----------------------------------------------------------------------------

int
ErasureCodeIsaDefault::isa_decode(int *erasures,
                                  char **data,
                                  char **coding,
                                  int blocksize)
{
  int nerrs = 0;
  int i, j, r, s;

  // count the errors
  for (int l = 0; erasures[l] != -1; l++) {
    nerrs++;
  }

  unsigned char *recover_source[k];
  unsigned char *recover_target[m];

  memset(recover_source, 0, sizeof (recover_source));
  memset(recover_target, 0, sizeof (recover_target));

  // ---------------------------------------------
  // Assign source and target buffers
  // ---------------------------------------------
  for (i = 0, s = 0, r = 0; ((r < k) || (s < nerrs)) && (i < (k + m)); i++) {
    if (!erasure_contains(erasures, i)) {
      if (r < k) {
        if (i < k) {
          recover_source[r] = (unsigned char*) data[i];
        } else {
          recover_source[r] = (unsigned char*) coding[i - k];
        }
        r++;
      }
    } else {
      if (s < m) {
        if (i < k) {
          recover_target[s] = (unsigned char*) data[i];
        } else {
          recover_target[s] = (unsigned char*) coding[i - k];
        }
        s++;
      }
    }
  }

  if (m==1) {
    // single parity decoding
    assert (1 == nerrs);
    dout(20) << "isa_decode: reconstruct using region xor [" << erasures[0] << "]" << dendl;
    region_xor(recover_source, recover_target[0], k, blocksize);
    return 0;
  }


  if ((matrixtype == kVandermonde) &&
      (nerrs == 1) &&
      (erasures[0] < (k + 1))) {
    // use xor decoding if a data chunk is missing or the first coding chunk
    dout(20) << "isa_decode: reconstruct using region xor [" << erasures[0] << "]" << dendl;
    assert(1 == s);
    assert(k == r);
    region_xor(recover_source, recover_target[0], k, blocksize);
    return 0;
  }

  unsigned char b[k * (m + k)];
  unsigned char c[k * (m + k)];
  unsigned char d[k * (m + k)];
  unsigned char g_decode_tbls[k * (m + k)*32];
  unsigned char *p_tbls = g_decode_tbls;

  int decode_index[k];

  if (nerrs > m)
    return -1;

  std::string erasure_signature; // describes a matrix configuration for caching

  // ---------------------------------------------
  // Construct b by removing error rows
  // ---------------------------------------------

  for (i = 0, r = 0; i < k; i++, r++) {
    char id[128];
    while (erasure_contains(erasures, r))
      r++;

    decode_index[i] = r;

    snprintf(id, sizeof (id), "+%d", r);
    erasure_signature += id;
  }

  for (int p = 0; p < nerrs; p++) {
    char id[128];
    snprintf(id, sizeof (id), "-%d", erasures[p]);
    erasure_signature += id;
  }

  // ---------------------------------------------
  // Try to get an already computed matrix
  // ---------------------------------------------
  if (!get_decoding_table_from_cache(erasure_signature, p_tbls)) {
    for (i = 0; i < k; i++) {
      r = decode_index[i];
      for (j = 0; j < k; j++)
        b[k * i + j] = a[k * r + j];
    }
    // ---------------------------------------------
    // Compute inverted matrix
    // ---------------------------------------------

    // --------------------------------------------------------
    // Remark: this may fail for certain Vandermonde matrices !
    // There is an advanced way trying to use different
    // source chunks to get an invertible matrix, however
    // there are also (k,m) combinations which cannot be
    // inverted when m chunks are lost and this optimizations
    // does not help. Therefor we keep the code simpler.
    // --------------------------------------------------------
    if (gf_invert_matrix(b, d, k) < 0) {
      dout(0) << "isa_decode: bad matrix" << dendl;
      return -1;
    }

    for (int p = 0; p < nerrs; p++) {
      if (erasures[p] < k) {
        // decoding matrix elements for data chunks
        for (j = 0; j < k; j++) {
          c[k * p + j] = d[k * erasures[p] + j];
        }
      } else {
        int s = 0;
        // decoding matrix element for coding chunks
        for (i = 0; i < k; i++) {
          s = 0;
          for (j = 0; j < k; j++)
            s ^= gf_mul(d[j * k + i],
                        a[k * erasures[p] + j]);

          c[k * p + i] = s;
        }
      }
    }

    // ---------------------------------------------
    // Initialize Decoding Table
    // ---------------------------------------------
    ec_init_tables(k, nerrs, c, g_decode_tbls);
    put_decoding_table_to_cache(erasure_signature, p_tbls);
  }
  // Recover data sources
  ec_encode_data(blocksize,
                 k, nerrs, g_decode_tbls, recover_source, recover_target);


  return 0;
}

// -----------------------------------------------------------------------------

unsigned
ErasureCodeIsaDefault::get_alignment() const
{
  return k * EC_ISA_VECTOR_OP_WORDSIZE;
}

// -----------------------------------------------------------------------------

void
ErasureCodeIsaDefault::parse(const map<std::string,
                             std::string> &parameters)
{
  k = to_int("k", parameters, DEFAULT_K);
  m = to_int("m", parameters, DEFAULT_M);

  if (matrixtype == kVandermonde) {
    // these are verified safe values evaluted using the benchmarktool and 10*(combinatoric for maximum loss) random full erasures
    if (k > 32) {
      derr << "Vandermonde: m=" << m
        << " should be less/equal than 32 : revert to k=32" << dendl;
      k = 32;
    }

    if (m > 4) {
      derr << "Vandermonde: m=" << m
        << " should be less than 5 to guarantee an MDS codec: revert to m=4" << dendl;
      m = 4;
    }
    switch (m) {
    case 4:
      if (k > 21) {
        derr << "Vandermonde: K=" << k
          << " should be less than 22 to guarantee an MDS codec with m=4: revert to k=21" << dendl;
        k = 21;
      }
      break;
    default:
      ;
    }
  }
}

// -----------------------------------------------------------------------------

void
ErasureCodeIsaDefault::prepare()
{
  a = (unsigned char*) malloc(k * (m + k));
  g_encode_tbls = (unsigned char*) malloc(k * (m + k)*32);

  unsigned memory_lru_cache = k * (m + k) * 32 * g_decode_tbls_lru_length;
  dout(10) << "[ cache memory ] = " << memory_lru_cache << " bytes" << dendl;
  // build encoding table which needs to be computed once for a configure (k,m)
  assert((matrixtype == kVandermonde) || (matrixtype == kCauchy));
  if (matrixtype == kVandermonde)
    gf_gen_rs_matrix(a, k + m, k);
  if (matrixtype == kCauchy)
    gf_gen_cauchy1_matrix(a, k + m, k);

  ec_init_tables(k, m, &a[k * k], g_encode_tbls);
}
// -----------------------------------------------------------------------------
