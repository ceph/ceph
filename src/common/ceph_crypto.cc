// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010-2011 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/config.h"
#include "ceph_crypto.h"
#include "include/scope_guard.h"

#ifdef USE_CRYPTOPP
void ceph::crypto::init(CephContext *cct)
{
}

void ceph::crypto::shutdown(bool)
{
}

// nothing
ceph::crypto::HMACSHA1::~HMACSHA1()
{
}

ceph::crypto::HMACSHA256::~HMACSHA256()
{
}

#elif defined(USE_NSS)

// for SECMOD_RestartModules()
#include <secmod.h>
#include <nspr.h>

static pthread_mutex_t crypto_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint32_t crypto_refs = 0;
static NSSInitContext *crypto_context = NULL;
static pid_t crypto_init_pid = 0;

PK11SymKey *ceph::crypto::PK11_ImportSymKey_FIPS(
    PK11SlotInfo * const slot,
    const CK_MECHANISM_TYPE type,
    const PK11Origin origin,
    const CK_ATTRIBUTE_TYPE operation,
    SECItem * const raw_key,
    void * const wincx)
{
  if (PK11_IsFIPS() == PR_FALSE) {
    // This isn't the FIPS mode, and thus PK11_ImportSymKey is available. Let's
    // make use of it to avoid overhead related to e.g. creating extra PK11Ctx.
    PK11SymKey *ret_key = nullptr;
    ret_key = PK11_ImportSymKey(slot, type, origin, operation, raw_key, wincx);

    return ret_key;
  }

  ceph_assert_always(wincx == nullptr);

  std::vector<unsigned char> wrapped_key;

  // getting 306 on my system which is CKM_DES3_ECB.
  const CK_MECHANISM_TYPE wrap_mechanism = PK11_GetBestWrapMechanism(slot);

  // Generate a wrapping key. It will be used exactly twice over the scope:
  //   * to encrypt raw_key giving wrapped_key,
  //   * to decrypt wrapped_key in the internals of PK11_UnwrapSymKey().
  PK11SymKey * const wrapping_key = PK11_KeyGen(
    slot,
    wrap_mechanism,
    nullptr,
    PK11_GetBestKeyLength(slot, wrap_mechanism),
    nullptr);
  if (wrapping_key == nullptr) {
    return nullptr;
  }
  auto wk_guard = make_scope_guard([wrapping_key] {
    PK11_FreeSymKey(wrapping_key);
  });

  // Prepare a PK11 context for the raw_key -> wrapped_key encryption.
  SECItem tmp_sec_item;
  ::memset(&tmp_sec_item, 0, sizeof(tmp_sec_item));
  PK11Context * const wrap_key_crypt_context = PK11_CreateContextBySymKey(
    wrap_mechanism,
    CKA_ENCRYPT,
    wrapping_key,
    &tmp_sec_item);
  if (wrap_key_crypt_context == nullptr) {
    return nullptr;
  }
  auto wkcc_guard = make_scope_guard([wrap_key_crypt_context] {
    PK11_DestroyContext(wrap_key_crypt_context, PR_TRUE);
  });


  // Finally wrap the key. Important note is that the wrapping mechanism
  // selection (read: just grabbing a cipher) offers, at least in my NSS
  // copy, mostly CKM_*_ECB ciphers (with 3DES as the leading one, see
  // wrapMechanismList[] in pk11mech.c). There is no CKM_*_*_PAD variant
  // which means that plaintext we are providing to PK11_CipherOp() must
  // be aligned to cipher's block size. For 3DES it's 64 bits.
  {
    const auto block_size = PK11_GetBlockSize(wrap_mechanism, nullptr);
    SECItem * const raw_key_aligned = PK11_BlockData(raw_key, block_size);
    if (raw_key_aligned == nullptr) {
      return nullptr;
    }
    auto rka_guard = make_scope_guard([raw_key_aligned] {
      SECITEM_FreeItem(raw_key_aligned, PR_TRUE);
    });

    // PARANOIA: always add space for one extra cipher's block. This seems
    // unnecessary at the moment as padding is never used (see the comment
    // above) but let's assume it can change in the future. Just in case.
    wrapped_key.resize(raw_key_aligned->len + block_size, 0x0);
    int out_len = 0;

    int ret = PK11_CipherOp(
      wrap_key_crypt_context,
      wrapped_key.data(),
      &out_len,
      wrapped_key.size(), // max space
      raw_key_aligned->data,
      raw_key_aligned->len);
    if (ret != SECSuccess) {
      return nullptr;
    }

    ret = PK11_Finalize(wrap_key_crypt_context);
    if (ret != SECSuccess) {
      return nullptr;
    }

    ceph_assert(out_len <= static_cast<int>(wrapped_key.size()));
    wrapped_key.resize(out_len);
  }

  // Key is wrapped now so we can acquire the ultimate PK11SymKey through
  // unwrapping it. Of course these two opposite operations form NOP with
  // a side effect: FIPS level 1 compatibility.
  ::memset(&tmp_sec_item, 0, sizeof(tmp_sec_item));

  SECItem wrapped_key_item;
  ::memset(&wrapped_key_item, 0, sizeof(wrapped_key_item));
  wrapped_key_item.data = wrapped_key.data();
  wrapped_key_item.len = wrapped_key.size();

  return PK11_UnwrapSymKey(
    wrapping_key,
    wrap_mechanism,
    &tmp_sec_item,
    &wrapped_key_item,
    type,
    operation,
    raw_key->len);
}

void ceph::crypto::init(CephContext *cct)
{
  pid_t pid = getpid();
  pthread_mutex_lock(&crypto_init_mutex);
  if (crypto_init_pid != pid) {
    if (crypto_init_pid > 0) {
      SECMOD_RestartModules(PR_FALSE);
    }
    crypto_init_pid = pid;
  }

  if (++crypto_refs == 1) {
    NSSInitParameters init_params;
    memset(&init_params, 0, sizeof(init_params));
    init_params.length = sizeof(init_params);

    uint32_t flags = (NSS_INIT_READONLY | NSS_INIT_PK11RELOAD);
    if (cct->_conf->nss_db_path.empty()) {
      flags |= (NSS_INIT_NOCERTDB | NSS_INIT_NOMODDB);
    }
    crypto_context = NSS_InitContext(cct->_conf->nss_db_path.c_str(), "", "",
                                     SECMOD_DB, &init_params, flags);
  }
  pthread_mutex_unlock(&crypto_init_mutex);
  assert(crypto_context != NULL);
}

void ceph::crypto::shutdown(bool shared)
{
  pthread_mutex_lock(&crypto_init_mutex);
  assert(crypto_refs > 0);
  if (--crypto_refs == 0) {
    NSS_ShutdownContext(crypto_context);
    if (!shared) {
      PR_Cleanup();
    }
    crypto_context = NULL;
    crypto_init_pid = 0;
  }
  pthread_mutex_unlock(&crypto_init_mutex);
}

ceph::crypto::HMAC::~HMAC()
{
  PK11_DestroyContext(ctx, PR_TRUE);
  PK11_FreeSymKey(symkey);
  PK11_FreeSlot(slot);
}

#else
# error "No supported crypto implementation found."
#endif
