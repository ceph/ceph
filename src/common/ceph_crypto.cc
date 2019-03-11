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

#include "common/ceph_context.h"
#include "common/config.h"
#include "ceph_crypto.h"

#ifdef USE_NSS

// for SECMOD_RestartModules()
#include <secmod.h>
#include <nspr.h>

#endif /*USE_NSS*/

#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif /*USE_OPENSSL*/

#ifdef USE_NSS

static pthread_mutex_t crypto_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint32_t crypto_refs = 0;
static NSSInitContext *crypto_context = NULL;
static pid_t crypto_init_pid = 0;

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
  ceph_assert_always(crypto_context != NULL);
}

void ceph::crypto::shutdown(bool shared)
{
  pthread_mutex_lock(&crypto_init_mutex);
  ceph_assert_always(crypto_refs > 0);
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

ceph::crypto::nss::HMAC::~HMAC()
{
  PK11_DestroyContext(ctx, PR_TRUE);
  PK11_FreeSymKey(symkey);
  PK11_FreeSlot(slot);
}

#else
# error "No supported crypto implementation found."
#endif /*USE_NSS*/

#ifdef USE_OPENSSL

ceph::crypto::ssl::OpenSSLDigest::OpenSSLDigest(const EVP_MD * _type)
  : mpContext(EVP_MD_CTX_create())
  , mpType(_type) {
  this->Restart();
}

ceph::crypto::ssl::OpenSSLDigest::~OpenSSLDigest() {
  EVP_MD_CTX_destroy(mpContext);
}

void ceph::crypto::ssl::OpenSSLDigest::Restart() {
  EVP_DigestInit_ex(mpContext, mpType, NULL);
}

void ceph::crypto::ssl::OpenSSLDigest::Update(const unsigned char *input, size_t length) {
  if (length) {
    EVP_DigestUpdate(mpContext, const_cast<void *>(reinterpret_cast<const void *>(input)), length);
  }
}

void ceph::crypto::ssl::OpenSSLDigest::Final(unsigned char *digest) {
  unsigned int s;
  EVP_DigestFinal_ex(mpContext, digest, &s);
}
#endif /*USE_OPENSSL*/
