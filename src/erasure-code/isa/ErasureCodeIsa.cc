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
#include <cerrno>
// -----------------------------------------------------------------------------
#include "common/debug.h"
#include "ErasureCodeIsa.h"
#include "xor_op.h"
#include "include/ceph_assert.h"
using namespace std;
using namespace ceph;

// -----------------------------------------------------------------------------
extern "C" {
#include "isa-l/include/erasure_code.h"
}
// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
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

const std::string ErasureCodeIsaDefault::DEFAULT_K("7");
const std::string ErasureCodeIsaDefault::DEFAULT_M("3");


// -----------------------------------------------------------------------------

int
ErasureCodeIsa::init(ErasureCodeProfile &profile, ostream *ss)
{
  int err = 0;
  err |= parse(profile, ss);
  if (err)
    return err;
  prepare();
  return ErasureCode::init(profile, ss);
}

// -----------------------------------------------------------------------------

unsigned int
ErasureCodeIsa::get_chunk_size(unsigned int object_size) const
{
  unsigned alignment = get_alignment();
  unsigned chunk_size = ( object_size + k - 1 ) / k;
  dout(20) << "get_chunk_size: chunk_size " << chunk_size
           << " must be modulo " << alignment << dendl;
  unsigned modulo = chunk_size % alignment;
  if (modulo) {
    dout(10) << "get_chunk_size: " << chunk_size
             << " padded to " << chunk_size + alignment - modulo << dendl;
    chunk_size += alignment - modulo;
  }
  return chunk_size;
}

// -----------------------------------------------------------------------------

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
  for (int i = 0; i < k + m; i++) {
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
  ceph_assert(erasures_count > 0);
  return isa_decode(erasures, data, coding, blocksize);
}

// -----------------------------------------------------------------------------

void
ErasureCodeIsaDefault::isa_encode(char **data,
                                  char **coding,
                                  int blocksize)
{

  if (m == 1)
    // single parity stripe
    region_xor((unsigned char**) data, (unsigned char*) coding[0], k, blocksize);
  else
    ec_encode_data(blocksize, k, m, encode_tbls,
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



// -----------------------------------------------------------------------------

int
ErasureCodeIsaDefault::isa_decode(int *erasures,
                                  char **data,
                                  char **coding,
                                  int blocksize)
{
  int nerrs = 0;
  int i, r, s;

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

  if (m == 1) {
    // single parity decoding
    ceph_assert(1 == nerrs);
    dout(20) << "isa_decode: reconstruct using region xor [" <<
      erasures[0] << "]" << dendl;
    region_xor(recover_source, recover_target[0], k, blocksize);
    return 0;
  }


  if ((matrixtype == kVandermonde) &&
      (nerrs == 1) &&
      (erasures[0] < (k + 1))) {
    // use xor decoding if a data chunk is missing or the first coding chunk
    dout(20) << "isa_decode: reconstruct using region xor [" <<
      erasures[0] << "]" << dendl;
    ceph_assert(1 == s);
    ceph_assert(k == r);
    region_xor(recover_source, recover_target[0], k, blocksize);
    return 0;
  }

  unsigned char d[k * (m + k)];
  unsigned char decode_tbls[k * (m + k)*32];
  unsigned char *p_tbls = decode_tbls;

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
  if (!tcache.getDecodingTableFromCache(erasure_signature, p_tbls, matrixtype, k, m)) {
    int j;
    unsigned char b[k * (m + k)];
    unsigned char c[k * (m + k)];

    for (i = 0; i < k; i++) {
      r = decode_index[i];
      for (j = 0; j < k; j++)
        b[k * i + j] = encode_coeff[k * r + j];
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
        // decoding matrix element for coding chunks
        for (i = 0; i < k; i++) {
          int s = 0;
          for (j = 0; j < k; j++)
            s ^= gf_mul(d[j * k + i],
                        encode_coeff[k * erasures[p] + j]);

          c[k * p + i] = s;
        }
      }
    }

    // ---------------------------------------------
    // Initialize Decoding Table
    // ---------------------------------------------
    ec_init_tables(k, nerrs, c, decode_tbls);
    tcache.putDecodingTableToCache(erasure_signature, p_tbls, matrixtype, k, m);
  }
  // Recover data sources
  ec_encode_data(blocksize,
                 k, nerrs, decode_tbls, recover_source, recover_target);


  return 0;
}

// -----------------------------------------------------------------------------

unsigned
ErasureCodeIsaDefault::get_alignment() const
{
  return EC_ISA_ADDRESS_ALIGNMENT;
}

// -----------------------------------------------------------------------------

int ErasureCodeIsaDefault::parse(ErasureCodeProfile &profile,
                                 ostream *ss)
{
  int err = ErasureCode::parse(profile, ss);
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  err |= to_int("m", profile, &m, DEFAULT_M, ss);
  err |= sanity_check_k_m(k, m, ss);

  if (matrixtype == kVandermonde) {
    // these are verified safe values evaluated using the
    // benchmarktool and 10*(combinatoric for maximum loss) random
    // full erasures
    if (k > 32) {
      *ss << "Vandermonde: m=" << m
        << " should be less/equal than 32 : revert to k=32" << std::endl;
      k = 32;
      err = -EINVAL;
    }

    if (m > 4) {
      *ss << "Vandermonde: m=" << m
        << " should be less than 5 to guarantee an MDS codec:"
        << " revert to m=4" << std::endl;
      m = 4;
      err = -EINVAL;
    }
    switch (m) {
    case 4:
      if (k > 21) {
        *ss << "Vandermonde: k=" << k
          << " should be less than 22 to guarantee an MDS"
          << " codec with m=4: revert to k=21" << std::endl;
        k = 21;
        err = -EINVAL;
      }
      break;
    default:
      ;
    }
  }
  return err;
}

// -----------------------------------------------------------------------------

void
ErasureCodeIsaDefault::prepare()
{
  // setup shared encoding table and coefficients
  unsigned char** p_enc_table =
    tcache.getEncodingTable(matrixtype, k, m);

  unsigned char** p_enc_coeff =
    tcache.getEncodingCoefficient(matrixtype, k, m);

  if (!*p_enc_coeff) {
    dout(10) << "[ cache tables ] creating coeff for k=" <<
      k << " m=" << m << dendl;
    // build encoding coefficients which need to be computed once for each (k,m)
    encode_coeff = (unsigned char*) malloc(k * (m + k));

    if (matrixtype == kVandermonde)
      gf_gen_rs_matrix(encode_coeff, k + m, k);
    if (matrixtype == kCauchy)
      gf_gen_cauchy1_matrix(encode_coeff, k + m, k);

      // either our new created coefficients are stored or if they have been
      // created in the meanwhile the locally allocated coefficients will be
      // freed by setEncodingCoefficient
    encode_coeff = tcache.setEncodingCoefficient(matrixtype, k, m, encode_coeff);
  } else {
    encode_coeff = *p_enc_coeff;
  }

  if (!*p_enc_table) {
    dout(10) << "[ cache tables ] creating tables for k=" <<
      k << " m=" << m << dendl;
    // build encoding table which needs to be computed once for each (k,m)
    encode_tbls = (unsigned char*) malloc(k * (m + k)*32);
    ec_init_tables(k, m, &encode_coeff[k * k], encode_tbls);

    // either our new created table is stored or if it has been
    // created in the meanwhile the locally allocated table will be
    // freed by setEncodingTable
    encode_tbls = tcache.setEncodingTable(matrixtype, k, m, encode_tbls);
  } else {
    encode_tbls = *p_enc_table;
  }

  unsigned memory_lru_cache =
    k * (m + k) * 32 * tcache.decoding_tables_lru_length;

  dout(10) << "[ cache memory ] = " << memory_lru_cache << " bytes" <<
    " [ matrix ] = " <<
    ((matrixtype == kVandermonde) ? "Vandermonde" : "Cauchy") << dendl;

  ceph_assert((matrixtype == kVandermonde) || (matrixtype == kCauchy));

}
// -----------------------------------------------------------------------------
