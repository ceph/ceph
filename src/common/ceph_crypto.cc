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

#include "include/int_types.h"
#include "common/config.h"
#include "common/ceph_context.h"
#include "ceph_crypto.h"
#include "auth/Crypto.h"

#include <pthread.h>
#include <stdlib.h>


#ifdef USE_CRYPTOPP
void ceph::crypto::init(CephContext *cct)
{
}

void ceph::crypto::shutdown()
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

    uint32_t flags = NSS_INIT_READONLY;
    if (cct->_conf->nss_db_path.empty()) {
      flags |= (NSS_INIT_NOCERTDB | NSS_INIT_NOMODDB);
    }
    crypto_context = NSS_InitContext(cct->_conf->nss_db_path.c_str(), "", "",
                                     SECMOD_DB, &init_params, flags);
  }
  pthread_mutex_unlock(&crypto_init_mutex);
  assert(crypto_context != NULL);
}

void ceph::crypto::shutdown()
{
  pthread_mutex_lock(&crypto_init_mutex);
  assert(crypto_refs > 0);
  if (--crypto_refs == 0) {
    NSS_ShutdownContext(crypto_context);
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
