// -*- mode:C++; tab-width:8; c-basic-offset:2
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 * Copyright (C) 2025 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "objclass/objclass.h"
#include "ops.h"
#include "common/ceph_crypto.h"
#include "BLAKE3/c/blake3.h"

#include "include/rados/rados_types.hpp"
#include "include/rados/buffer.h"
#include "include/rados/librados.hpp"

//#include "../rgw/rgw_blake3_digest.h"
//using namespace rgw::digest;
#include "common/errno.h"
CLS_VER(1,0)
CLS_NAME(blake3)

using namespace cls::blake3_hash;
//#define DEBUG_ENDIANNESS

#ifndef DEBUG_ENDIANNESS
#define CEPHTOH_16 be16toh
#define CEPHTOH_32 be32toh
#define CEPHTOH_64 be64toh
#define HTOCEPH_16 htobe16
#define HTOCEPH_32 htobe32
#define HTOCEPH_64 htobe64

#else
// DEBUG Mode - use BE format to test the code
#define CEPHTOH_16 le16toh
#define CEPHTOH_32 le32toh
#define CEPHTOH_64 le64toh
#define HTOCEPH_16 htole16
#define HTOCEPH_32 htole32
#define HTOCEPH_64 htole64
#endif

//----------------------------------------------------------------------------
static inline void blake3_to_ceph(blake3_hasher *hasher)
{
  char *p_end = (char*)hasher->key + sizeof(hasher->key);
  for (uint32_t *p = hasher->key; p < (uint32_t *)p_end; p++) {
    *p = HTOCEPH_32(*p);
  }

  p_end = (char*)hasher->chunk.cv + sizeof(hasher->chunk.cv);
  for (uint32_t *p = hasher->chunk.cv; p < (uint32_t *)p_end; p++) {
    *p = HTOCEPH_32(*p);
  }
  hasher->chunk.chunk_counter = HTOCEPH_64(hasher->chunk.chunk_counter);
}

//----------------------------------------------------------------------------
static inline void blake3_to_host(blake3_hasher *hasher)
{
  char *p_end = (char*)hasher->key + sizeof(hasher->key);
  for (uint32_t *p = hasher->key; p < (uint32_t *)p_end; p++) {
    *p = CEPHTOH_32(*p);
  }

  p_end = (char*)hasher->chunk.cv + sizeof(hasher->chunk.cv);
  for (uint32_t *p = hasher->chunk.cv; p < (uint32_t *)p_end; p++) {
    *p = CEPHTOH_32(*p);
  }
  hasher->chunk.chunk_counter = CEPHTOH_64(hasher->chunk.chunk_counter);
}

//---------------------------------------------------------------------------
[[maybe_unused]]static std::string stringToHex(const std::string& input)
{
  std::stringstream ss;
  for (char c : input) {
    ss << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<int>(static_cast<unsigned char>(c));
  }
  return ss.str();
}

//------------------------------------------------------------------------------
static int blake3_hash_data(cls_method_context_t hctx, bufferlist *in,
                            bufferlist *out)
{
  CLS_LOG(20, "%s: was called", __func__);
  cls_blake3_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(0, "ERROR: %s: failed to decode input", __func__);
    return -EINVAL;
  }

  if (op.flags.is_first_part()) {
    if (unlikely(op.blake3_state_bl.length() != 0)) {
      CLS_LOG(0, "ERROR: %s: Non empty blake3_state_bl on first chunk (%u)",
              __func__, op.blake3_state_bl.length());
      return -EOVERFLOW;
    }
  }
  else if (unlikely(op.blake3_state_bl.length() > sizeof(blake3_hasher))) {
    CLS_LOG(0, "ERROR: %s: Overflow blake3_state_bl len (%u/%lu)",
            __func__, op.blake3_state_bl.length(), sizeof(blake3_hasher));
    return -EOVERFLOW;
  }

  // TBD: Should we follow deep scrub behavior and bypass ObjectStore cache using
  // CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE ???
  uint32_t read_flags = CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  int ofs = 0, len = 0;
  ceph::buffer::list bl;
  int ret = cls_cxx_read2(hctx, ofs, len, &bl, read_flags);
  if (ret < 0) {
    CLS_LOG(0, "%s:: failed cls_cxx_read2() ret=%d (%s)",
            __func__, ret, cpp_strerror(-ret).c_str());
    return ret;
  }

  blake3_hasher hmac;
  if (op.flags.is_first_part()) {
    CLS_LOG(20, "%s: first part", __func__);
    blake3_hasher_init(&hmac);
  }
  else {
    const char *p_bl = op.blake3_state_bl.c_str();
    memcpy((char*)&hmac, p_bl, op.blake3_state_bl.length());
    blake3_to_host(&hmac);
    if (unlikely(hmac.cv_stack_len > (BLAKE3_MAX_DEPTH + 1) ||
                 hmac.chunk.buf_len > BLAKE3_BLOCK_LEN)) {
      CLS_LOG(0, "ERROR: %s: bad blake3_state_bl", __func__);
      return -EINVAL;
    }
  }

  for (const auto& bptr : bl.buffers()) {
    blake3_hasher_update(&hmac, (const unsigned char *)bptr.c_str(), bptr.length());
  }

  //set the results in the returned bl
  if (op.flags.is_last_part()) {
    uint8_t hash[BLAKE3_OUT_LEN];
    blake3_hasher_finalize(&hmac, hash, BLAKE3_OUT_LEN);
    CLS_LOG(20, "%s: last part chunk_counter=%lu, hash=%s",
            __func__, hmac.chunk.chunk_counter,
            stringToHex(std::string((const char*)hash, BLAKE3_OUT_LEN)).c_str());
    out->append((const char *)hash, BLAKE3_OUT_LEN);
  }
  else {
    const char *p_hmac = (const char *)&hmac;
    unsigned transfer_len = sizeof(blake3_hasher);
    transfer_len -= ((BLAKE3_MAX_DEPTH - hmac.cv_stack_len - 1) * BLAKE3_OUT_LEN);
    CLS_LOG(20, "%s: cv_stack_len=%d, transfer_len=%d",
            __func__, hmac.cv_stack_len, transfer_len);
    blake3_to_ceph(&hmac);
    out->append(p_hmac, transfer_len);
  }

  return 0;
}

//------------------------------------------------------------------------------
static void verify_blake3_lib()
{
  // first make sure the defined macros were not changed
  static_assert(BLAKE3_KEY_LEN == 32);
  static_assert(BLAKE3_OUT_LEN == 32);
  static_assert(BLAKE3_BLOCK_LEN == 64);
  static_assert(BLAKE3_CHUNK_LEN == 1024);
  static_assert(BLAKE3_MAX_DEPTH == 54);

  // Then check struct blake3_hasher, making sure offsets, types and sizes
  // were not changed
  blake3_hasher hasher;
  static_assert(sizeof(blake3_hasher) == 1912);
  static_assert(offsetof(blake3_hasher, key) == 0);
  static_assert(std::is_array<decltype(hasher.key)>::value);
  static_assert(sizeof(hasher.key) == 8*sizeof(uint32_t));

  static_assert(offsetof(blake3_hasher, chunk) == 8*sizeof(uint32_t));
  static_assert(std::is_same<decltype(hasher.chunk), blake3_chunk_state>::value);

  static_assert(offsetof(blake3_hasher, cv_stack_len) == offsetof(blake3_hasher, chunk) + sizeof(blake3_chunk_state));
  static_assert(std::is_same<decltype(hasher.cv_stack_len), uint8_t>::value);

  // TBD: The offset might change with alignment
  const size_t off = offsetof(blake3_hasher, cv_stack_len) + 1;
  static_assert(offsetof(blake3_hasher, cv_stack) == off);
  static_assert(std::is_array<decltype(hasher.cv_stack)>::value);
  static_assert(sizeof(hasher.cv_stack) == (BLAKE3_MAX_DEPTH + 1) * BLAKE3_OUT_LEN);

  // check sub-struct blake3_chunk_state
  blake3_chunk_state &chunk_state = hasher.chunk;
  static_assert(sizeof(chunk_state) == 112);
  static_assert(offsetof(blake3_chunk_state, cv) == 0);
  static_assert(std::is_array<decltype(chunk_state.cv)>::value);
  static_assert(sizeof(chunk_state.cv) == 8*sizeof(uint32_t));

  const size_t off2 = 8*sizeof(uint32_t);
  static_assert(offsetof(blake3_chunk_state, chunk_counter) == off2);
  static_assert(std::is_same<decltype(chunk_state.chunk_counter), uint64_t>::value);

  const size_t off3 = off2 + sizeof(uint64_t);
  static_assert(offsetof(blake3_chunk_state, buf) == off3);
  static_assert(std::is_array<decltype(chunk_state.buf)>::value);
  static_assert(sizeof(chunk_state.buf) == BLAKE3_BLOCK_LEN);

  const size_t off4 = off3 + BLAKE3_BLOCK_LEN;
  static_assert(offsetof(blake3_chunk_state, buf_len) == off4);
  static_assert(std::is_same<decltype(chunk_state.buf_len), uint8_t>::value);

  static_assert(offsetof(blake3_chunk_state, blocks_compressed) == off4+1);
  static_assert(std::is_same<decltype(chunk_state.blocks_compressed), uint8_t>::value);

  static_assert(offsetof(blake3_chunk_state, flags) == off4+2);
  static_assert(std::is_same<decltype(chunk_state.flags), uint8_t>::value);
}

//------------------------------------------------------------------------------
CLS_INIT(blake3)
{
  verify_blake3_lib();
  CLS_LOG(0, "Loaded hash class (blake3)!");
  cls_handle_t h_class;
  cls_method_handle_t h_hash_data;
  cls_register("blake3", &h_class);
  cls_register_cxx_method(h_class, "blake3_hash_data", CLS_METHOD_RD,
                          blake3_hash_data, &h_hash_data);
}
