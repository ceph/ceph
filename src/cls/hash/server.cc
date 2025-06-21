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
CLS_NAME(hash)

using namespace cls::hash;

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
static int sanity_check_input_blake3_stats(const blake3_hasher &hmac,
                                           const cls_hash_op &op)
{
  // need to reduce the bufferd unprocessed data from the offset
  uint64_t expected_chunk_counter = (op.offset-hmac.chunk.buf_len)/BLAKE3_CHUNK_LEN;
  if ( (expected_chunk_counter == hmac.chunk.chunk_counter) &&
       (hmac.cv_stack_len  <= BLAKE3_MAX_DEPTH + 1)         &&
       (hmac.chunk.buf_len <= BLAKE3_BLOCK_LEN) ) {
    // everything look legit
    return 0;
  }

  if (expected_chunk_counter != hmac.chunk.chunk_counter) {
    CLS_LOG(0, "ERR: %s: chunk_counter=%lu, expected_chunk_counter=%lu, offset=%lu",
            __func__, hmac.chunk.chunk_counter, expected_chunk_counter, op.offset);
    return -EINVAL;
  }

  if (hmac.cv_stack_len > BLAKE3_MAX_DEPTH + 1) {
    CLS_LOG(0, "ERR: %s: cv_stack_len is too high (%d/%d)",
            __func__, hmac.cv_stack_len, BLAKE3_MAX_DEPTH + 1);
    return -EINVAL;
  }

  if (hmac.chunk.buf_len > BLAKE3_BLOCK_LEN) {
    CLS_LOG(0, "ERR: %s: buf_len too large (%d/%d)",
            __func__, hmac.chunk.buf_len, BLAKE3_BLOCK_LEN);
    return -EINVAL;
  }

  // should never reach here
  ceph_assert(0);
  return -EINVAL;
}

//------------------------------------------------------------------------------
static int blake3_hash(cls_method_context_t hctx,
                       cls_hash_op &op,
                       bufferlist *out)
{
  blake3_hasher hmac;
  if (op.flags.is_first_part()) {
    if (op.hash_state_bl.length() == 0) {
      CLS_LOG(20, "%s: first part", __func__);
      blake3_hasher_init(&hmac);
    }
    else {
      CLS_LOG(0, "ERR: %s: Non empty hash_state_bl on first chunk (%u)",
              __func__, op.hash_state_bl.length());
      return -EOVERFLOW;
    }
  }
  else {
    if (op.hash_state_bl.length() <= sizeof(blake3_hasher)) {
      const char *p_bl = op.hash_state_bl.c_str();
      memcpy((char*)&hmac, p_bl, op.hash_state_bl.length());
      blake3_to_host(&hmac);
      int ret = sanity_check_input_blake3_stats(hmac, op);
      if (unlikely(ret != 0)) {
        return ret;
      }
    }
    else {
      CLS_LOG(0, "ERR: %s: Overflow hash_state_bl len (%u/%lu)",
              __func__, op.hash_state_bl.length(), sizeof(blake3_hasher));
      return -EOVERFLOW;
    }
  }

  // TBD: Should we follow deep scrub behavior and bypass ObjectStore cache using
  // CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE ???
  uint32_t read_flags = CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  int ofs = 0, len = 0;
  ceph::buffer::list bl;
  int ret = cls_cxx_read2(hctx, ofs, len, &bl, read_flags);
  if (ret < 0) {
    CLS_LOG(1, "%s:: failed cls_cxx_read2() ret=%d (%s)",
            __func__, ret, cpp_strerror(-ret).c_str());
    return ret;
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
static int md5_hash(cls_method_context_t hctx,
                    cls_hash_op &op,
                    bufferlist *out)
{
  if (unlikely(!op.flags.is_first_part() || !op.flags.is_last_part())) {
    // we don't know how to serialize md5_hash state so only single part
    // operations are supported
    CLS_LOG(0, "ERR: %s: only single part hash is supported", __func__);
    return -EOPNOTSUPP;
  }

  // TBD: Should we follow deep scrub behavior and bypass ObjectStore cache using
  // CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE ???
  uint32_t read_flags = CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  int ofs = 0, len = 0;
  ceph::buffer::list bl;
  int ret = cls_cxx_read2(hctx, ofs, len, &bl, read_flags);
  if (ret < 0) {
    CLS_LOG(1, "%s:: failed cls_cxx_read2() ret=%d (%s)",
            __func__, ret, cpp_strerror(-ret).c_str());
    return ret;
  }

  ceph::crypto::MD5 hmac;
  for (const auto& bptr : bl.buffers()) {
    hmac.Update((const unsigned char *)bptr.c_str(), bptr.length());
  }

  uint8_t hash[CEPH_CRYPTO_MD5_DIGESTSIZE];
  hmac.Final(hash);
  CLS_LOG(20, "%s: hash=%s", __func__,
          stringToHex(std::string((const char*)hash, sizeof(hash))).c_str());
  out->append((const char *)hash, sizeof(hash));

  return 0;
}

//------------------------------------------------------------------------------
static int sha256_hash(cls_method_context_t hctx,
                       cls_hash_op &op,
                       bufferlist *out)
{
  if (unlikely(!op.flags.is_first_part() || !op.flags.is_last_part())) {
    // we don't know how to serialize sha256_hash state so only single part
    // operations are supported
    CLS_LOG(0, "ERR: %s: only single part hash is supported", __func__);
    return -EOPNOTSUPP;
  }

  // TBD: Should we follow deep scrub behavior and bypass ObjectStore cache using
  // CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE ???
  uint32_t read_flags = CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  int ofs = 0, len = 0;
  ceph::buffer::list bl;
  int ret = cls_cxx_read2(hctx, ofs, len, &bl, read_flags);
  if (ret < 0) {
    CLS_LOG(1, "%s:: failed cls_cxx_read2() ret=%d (%s)",
            __func__, ret, cpp_strerror(-ret).c_str());
    return ret;
  }

  ceph::crypto::SHA256 hmac;
  for (const auto& bptr : bl.buffers()) {
    hmac.Update((const unsigned char *)bptr.c_str(), bptr.length());
  }

  uint8_t hash[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  hmac.Final(hash);
  CLS_LOG(20, "%s: hash=%s", __func__,
          stringToHex(std::string((const char*)hash, sizeof(hash))).c_str());
  out->append((const char *)hash, sizeof(hash));

  return 0;
}

//------------------------------------------------------------------------------
static int hash_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "hash_data main function");
  cls_hash_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(0, "ERROR: %s: failed to decode input", __func__);
    return -EINVAL;
  }

  switch (op.hash_type) {
  case HASH_MD5:
    return md5_hash(hctx, op, out);
  case HASH_SHA256:
    return sha256_hash(hctx, op, out);
  case HASH_BLAKE3:
    return blake3_hash(hctx, op, out);
  default:
    CLS_LOG(0, "ERR: %s: unexpcted hash_type (%u)", __func__, op.hash_type);
    return -EINVAL;
  }
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

  // The BLAKE3-CLS understands the internal data-structures holding the hash state
  // (blake3_hasher and blake3_chunk_state)
  // The embedded version used by ceph is 1.5.0 (from Sep 21, 2023)
  // Current version in https://github.com/BLAKE3-team/BLAKE3 (June-19-2025)
  // is 1.8.2.
  // Blake-CLS will work with any version up-to 1.8.2
  // The data-structures holding the hash state were not changed since version 1.0.0
  // so it is very unlikely they will be changed in the future, but we still lock
  // to the last known version (1.8.2 ) to be on the safe side
  constexpr std::string_view ver(BLAKE3_VERSION_STRING);
  constexpr std::string_view max_ver("1.8.2");
  static_assert(ver <= max_ver);

  // Then check struct blake3_hasher, making sure offsets, types and sizes
  // were not changed
  blake3_hasher hasher;
  static_assert(sizeof(blake3_hasher) == 1912);
  static_assert(offsetof(blake3_hasher, key) == 0);
  static_assert(std::is_array<decltype(hasher.key)>::value);
  static_assert(sizeof(hasher.key) == 8*sizeof(uint32_t));
  static_assert(std::is_same<decltype(hasher.chunk), blake3_chunk_state>::value);
  static_assert(std::is_same<decltype(hasher.cv_stack_len), uint8_t>::value);
  static_assert(std::is_array<decltype(hasher.cv_stack)>::value);
  static_assert(sizeof(hasher.cv_stack) == (BLAKE3_MAX_DEPTH + 1) * BLAKE3_OUT_LEN);

  // check sub-struct blake3_chunk_state
  blake3_chunk_state &chunk_state = hasher.chunk;
  static_assert(sizeof(chunk_state) == 112);
  static_assert(offsetof(blake3_chunk_state, cv) == 0);
  static_assert(std::is_array<decltype(chunk_state.cv)>::value);
  static_assert(sizeof(chunk_state.cv) == 8*sizeof(uint32_t));
  static_assert(std::is_same<decltype(chunk_state.chunk_counter), uint64_t>::value);
  static_assert(std::is_array<decltype(chunk_state.buf)>::value);
  static_assert(sizeof(chunk_state.buf) == BLAKE3_BLOCK_LEN);
  static_assert(std::is_same<decltype(chunk_state.buf_len), uint8_t>::value);
  static_assert(std::is_same<decltype(chunk_state.blocks_compressed), uint8_t>::value);
  static_assert(std::is_same<decltype(chunk_state.flags), uint8_t>::value);
}

//------------------------------------------------------------------------------
CLS_INIT(hash)
{
  verify_blake3_lib();
  CLS_LOG(0, "Loaded -->cls_hash class");
  cls_handle_t h_class;
  cls_method_handle_t h_hash_data;
  cls_register("hash", &h_class);
  cls_register_cxx_method(h_class, "hash_data", CLS_METHOD_RD,
                          hash_data, &h_hash_data);
  CLS_LOG(0, "Registered -->cls_hash class");
}
