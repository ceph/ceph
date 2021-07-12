/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Intel Corporation
 *
 * Author: Liang Fang <liang.a.fang@intel.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include "crypto/ipsec_mb/ipsec_crypto_accel.h"

IPSECCryptoEngine::IPSECCryptoEngine() {
  bool ret = false;

  ret = detect_arch_and_init_mgr();
  if (!ret)
    return;
}

IPSECCryptoEngine::IPSECCryptoEngine(const unsigned char(&key)[AES_256_KEYSIZE]) {
  bool ret = false;

  ret = detect_arch_and_init_mgr();
  if (!ret)
    return;

  set_key(key);
}

IPSECCryptoEngine::~IPSECCryptoEngine() {
  if (m_mgr)
    free_mb_mgr(m_mgr);
}

void IPSECCryptoEngine::set_key(const unsigned char(&key)[AES_256_KEYSIZE]) {
  if(memcmp(m_key, key, AES_256_KEYSIZE)) { //key changed
    memcpy(m_key, key, sizeof(key));
    IMB_AES_KEYEXP_256(m_mgr, m_key, enc_keys, dec_keys);
  }
}

bool IPSECCryptoEngine::detect_arch_and_init_mgr()
{
  const uint64_t detect_avx = IMB_FEATURE_AVX | IMB_FEATURE_CMOV | IMB_FEATURE_AESNI;
  const uint64_t detect_avx2 = IMB_FEATURE_AVX2 | detect_avx;
  const uint64_t detect_avx512 = IMB_FEATURE_AVX512_SKX | detect_avx2;

  if (m_mgr)
    return true;

  m_mgr = alloc_mb_mgr(0);
  if (m_mgr == NULL) {
    return false;
  }

  if ((m_mgr->features & detect_avx512) == detect_avx512)
    init_mb_mgr_avx512(m_mgr);
  else if ((m_mgr->features & detect_avx2) == detect_avx2)
    init_mb_mgr_avx2(m_mgr);
  else if ((m_mgr->features & detect_avx) == detect_avx)
    init_mb_mgr_avx(m_mgr);
  else
    init_mb_mgr_sse(m_mgr);

  return true;
}

bool IPSECCryptoAccel::cbc_encrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE])
{
  CryptoEngine* engine = get_engine(key);
  bool ret;

  ret = cbc_encrypt_mb(engine, out, in, size, iv);
  flush(engine);
  put_engine(engine);

  return ret;
}

bool IPSECCryptoAccel::cbc_decrypt(unsigned char* out, const unsigned char* in, size_t size,
                             const unsigned char (&iv)[AES_256_IVSIZE],
                             const unsigned char (&key)[AES_256_KEYSIZE])
{
  CryptoEngine* engine = get_engine(key);
  bool ret;

  ret = cbc_decrypt_mb(engine, out, in, size, iv);
  flush(engine);
  put_engine(engine);

  return ret;
}

/*
 * global variables for getting engine
 *
 * get_engine is called very frequently, so should make it efficient:
 * - Don't put these variables inside get_engine as static, otherwise compiler will
 *   add instructions to check if the variables been initialized or not
 * - Don't use locks such as mutex, otherwise thread may be scheduled out, use atomic instead
 */
#define MAX_ENG 256                     //256 is a big enough CPU core number
static IPSECCryptoEngine engs[MAX_ENG]; //array is most efficient
static std::atomic<int> initer = 0;     //Only initer 1 win the right to do initialization
static std::atomic<int> inited = 0;     //1: inited 0: not yet
static std::atomic<int> last_i = 0;     //last index of engine that in-use, get next engine from last_i+1
CryptoEngine* IPSECCryptoAccel::get_engine(const unsigned char(&key)[AES_256_KEYSIZE])
{
  int i;

  if(initer == 0) {
    if((++initer) == 1) { //returned 1, I'm the right initer
      //do initialization
      for(i=0; i<MAX_ENG; i++) {
        engs[i].in_use.clear();
        engs[i].set_key(key);	//initialize the key, in future can just compare the key
      }

      //open the door
      inited = 1;
    }
  }

  while(inited != 1){} //wait for the door to be opened

  //find free engine
  for(i=(last_i+1)%MAX_ENG;;i=(i+1)%MAX_ENG) {
    if(!engs[i].in_use.test_and_set()){  //found a free one and already accupied by me after this line
      last_i = i;                        //i may not be the lastest one, but worthless to make it accurate
      engs[i].set_key(key);
      break;
    }
  }
  return &engs[i];
}

void IPSECCryptoAccel::put_engine(CryptoEngine* eng)
{
  IPSECCryptoEngine* ipsec = (IPSECCryptoEngine*)eng;
  ipsec->in_use.clear();
}

bool IPSECCryptoAccel::flush(CryptoEngine* engine)
{
  IPSECCryptoEngine* ipsec_engine = (IPSECCryptoEngine *)engine;
  if (!ipsec_engine || !ipsec_engine->m_mgr)
    return false;

  while (IMB_FLUSH_JOB(ipsec_engine->m_mgr))
    ;
  return true;
}

bool IPSECCryptoAccel::cbc_encrypt_mb(CryptoEngine* engine, unsigned char* out, const unsigned char* in, size_t size,
  const unsigned char(&iv)[AES_256_IVSIZE])
{
  struct IMB_JOB* job;
  int err;
  IPSECCryptoEngine* ipsec_engine = (IPSECCryptoEngine *)engine;
  if (!ipsec_engine || !ipsec_engine->m_mgr)
    return false;

  job = IMB_GET_NEXT_JOB(ipsec_engine->m_mgr);
  if (job == NULL)
    return false;
  job->cipher_direction = IMB_DIR_ENCRYPT;
  job->chain_order = IMB_ORDER_CIPHER_HASH;
  job->dst = out;
  job->src = in;
  job->cipher_mode = IMB_CIPHER_CBC;
  job->enc_keys = ipsec_engine->enc_keys;
  job->dec_keys = ipsec_engine->dec_keys;
  job->key_len_in_bytes = AES_256_KEYSIZE;

  job->iv = &iv[0];
  job->iv_len_in_bytes = AES_256_IVSIZE;
  job->cipher_start_src_offset_in_bytes = 0;
  job->msg_len_to_cipher_in_bytes = size;
  job->hash_alg = IMB_AUTH_NULL;
  job = IMB_SUBMIT_JOB(ipsec_engine->m_mgr);
  if (job == NULL) {
    /* no job returned - check for error */
    err = imb_get_errno(ipsec_engine->m_mgr);
    if (err != 0) {
      return false;
    }
  }
  else {
    if (job->status != STS_COMPLETED) {
      return false;
    }
  }

  return true;
}

bool IPSECCryptoAccel::cbc_decrypt_mb(CryptoEngine* engine, unsigned char* out, const unsigned char* in, size_t size,
  const unsigned char(&iv)[AES_256_IVSIZE])
{
  struct IMB_JOB* job;
  int err;
  IPSECCryptoEngine* ipsec_engine = (IPSECCryptoEngine*)engine;
  if (!ipsec_engine || !ipsec_engine->m_mgr)
    return false;

  job = IMB_GET_NEXT_JOB(ipsec_engine->m_mgr);
  if (job == NULL)
    return false;
  job->cipher_direction = IMB_DIR_DECRYPT;
  job->chain_order = IMB_ORDER_HASH_CIPHER;
  job->dst = out;
  job->src = in;
  job->cipher_mode = IMB_CIPHER_CBC;
  job->enc_keys = ipsec_engine->enc_keys;
  job->dec_keys = ipsec_engine->dec_keys;
  job->key_len_in_bytes = AES_256_KEYSIZE;

  job->iv = &iv[0];
  job->iv_len_in_bytes = AES_256_IVSIZE;
  job->cipher_start_src_offset_in_bytes = 0;
  job->msg_len_to_cipher_in_bytes = size;
  job->hash_alg = IMB_AUTH_NULL;
  job = IMB_SUBMIT_JOB(ipsec_engine->m_mgr);
  if (job == NULL) {
    /* no job returned - check for error */
    err = imb_get_errno(ipsec_engine->m_mgr);
    if (err != 0) {
      return false;
    }
  }
  else {
    if (job->status != STS_COMPLETED) {
      return false;
    }
  }

  return true;
}
