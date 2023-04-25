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

#include <vector>
#include <utility>

#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/config.h"
#include "ceph_crypto.h"

#include <openssl/evp.h>

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#  include <openssl/conf.h>
#  include <openssl/engine.h>
#  include <openssl/err.h>
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

namespace TOPNSPC::crypto::ssl {

#if OPENSSL_VERSION_NUMBER < 0x10100000L
static std::atomic_uint32_t crypto_refs;


static auto ssl_mutexes = ceph::make_lock_container<ceph::shared_mutex>(
  static_cast<size_t>(std::max(CRYPTO_num_locks(), 0)),
  [](const size_t i) {
    return ceph::make_shared_mutex(
      std::string("ssl-mutex-") + std::to_string(i));
  });

static struct {
  // we could use e.g. unordered_set instead at the price of providing
  // std::hash<...> specialization. However, we can live with duplicates
  // quite well while the benefit is not worth the effort.
  std::vector<CRYPTO_THREADID> tids;
  ceph::mutex lock = ceph::make_mutex("crypto::ssl::init_records::lock");;
} init_records;

static void
ssl_locking_callback(
  const int mode,
  const int mutex_num,
  [[maybe_unused]] const char *file,
  [[maybe_unused]] const int line)
{
  if (mutex_num < 0 || static_cast<size_t>(mutex_num) >= ssl_mutexes.size()) {
    ceph_assert_always("openssl passed wrong mutex index" == nullptr);
  }

  if (mode & CRYPTO_READ) {
    if (mode & CRYPTO_LOCK) {
      ssl_mutexes[mutex_num].lock_shared();
    } else if (mode & CRYPTO_UNLOCK) {
      ssl_mutexes[mutex_num].unlock_shared();
    }
  } else if (mode & CRYPTO_WRITE) {
    if (mode & CRYPTO_LOCK) {
      ssl_mutexes[mutex_num].lock();
    } else if (mode & CRYPTO_UNLOCK) {
      ssl_mutexes[mutex_num].unlock();
    }
  }
}

static unsigned long
ssl_get_thread_id(void)
{
  static_assert(sizeof(unsigned long) >= sizeof(pthread_t));
  /* pthread_t may be any data type, so a simple cast to unsigned long
   * can rise a warning/error, depending on the platform.
   * Here memcpy is used as an anything-to-anything cast. */
  unsigned long ret = 0;
  pthread_t t = pthread_self();
  memcpy(&ret, &t, sizeof(pthread_t));
  return ret;
}
#endif /* not OPENSSL_VERSION_NUMBER < 0x10100000L */

static void init() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  if (++crypto_refs == 1) {
    // according to
    // https://wiki.openssl.org/index.php/Library_Initialization#libcrypto_Initialization
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();

    // initialize locking callbacks, needed for thread safety.
    // http://www.openssl.org/support/faq.html#PROG1
    CRYPTO_set_locking_callback(&ssl_locking_callback);
    CRYPTO_set_id_callback(&ssl_get_thread_id);

    OPENSSL_config(nullptr);
  }

  // we need to record IDs of all threads calling the initialization in
  // order to *manually* free per-thread memory OpenSSL *automagically*
  // allocated in ERR_get_state().
  // XXX: this solution/nasty hack is IMPERFECT. A leak will appear when
  // a client init()ializes the crypto subsystem with one thread and then
  // uses it from another one in a way that results in ERR_get_state().
  // XXX: for discussion about another approaches please refer to:
  // https://www.mail-archive.com/openssl-users@openssl.org/msg59070.html
  {
    std::lock_guard l(init_records.lock);
    CRYPTO_THREADID tmp;
    CRYPTO_THREADID_current(&tmp);
    init_records.tids.emplace_back(std::move(tmp));
  }
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
}

static void shutdown() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  if (--crypto_refs != 0) {
    return;
  }

  // drop error queue for each thread that called the init() function to
  // satisfy valgrind.
  {
    std::lock_guard l(init_records.lock);

    // NOTE: in OpenSSL 1.0.2g the signature is:
    //    void ERR_remove_thread_state(const CRYPTO_THREADID *tid);
    // but in 1.1.0j it has been changed to
    //    void ERR_remove_thread_state(void *);
    // We're basing on the OPENSSL_VERSION_NUMBER check to preserve
    // const-correctness without failing builds on modern envs.
    for (const auto& tid : init_records.tids) {
      ERR_remove_thread_state(&tid);
    }
  }

  // Shutdown according to
  // https://wiki.openssl.org/index.php/Library_Initialization#Cleanup
  // http://stackoverflow.com/questions/29845527/how-to-properly-uninitialize-openssl
  //
  // The call to CONF_modules_free() has been introduced after a valgring run.
  CRYPTO_set_locking_callback(nullptr);
  CRYPTO_set_id_callback(nullptr);
  ENGINE_cleanup();
  CONF_modules_free();
  CONF_modules_unload(1);
  ERR_free_strings();
  EVP_cleanup();
  CRYPTO_cleanup_all_ex_data();

  // NOTE: don't clear ssl_mutexes as we should be ready for init-deinit-init
  // sequence.
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
}

void zeroize_for_security(void* const s, const size_t n) {
  OPENSSL_cleanse(s, n);
}

} // namespace TOPNSPC::crypto::openssl


namespace TOPNSPC::crypto {
void init() {
  ssl::init();
}

void shutdown([[maybe_unused]] const bool shared) {
  ssl::shutdown();
}

void zeroize_for_security(void* const s, const size_t n) {
  ssl::zeroize_for_security(s, n);
}

ssl::OpenSSLDigest::OpenSSLDigest(const EVP_MD * _type)
  : mpContext(EVP_MD_CTX_create())
  , mpType(_type) {
  this->Restart();
}

ssl::OpenSSLDigest::~OpenSSLDigest() {
  EVP_MD_CTX_destroy(mpContext);
  if (mpType_FIPS) {
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
    EVP_MD_free(mpType_FIPS);
#endif  // OPENSSL_VERSION_NUMBER >= 0x30000000L
  }
}

ssl::OpenSSLDigest::OpenSSLDigest(OpenSSLDigest&& o) noexcept
  : mpContext(std::exchange(o.mpContext, nullptr)),
    mpType(std::exchange(o.mpType, nullptr)),
    mpType_FIPS(std::exchange(o.mpType_FIPS, nullptr))
{
}

ssl::OpenSSLDigest& ssl::OpenSSLDigest::operator=(OpenSSLDigest&& o) noexcept
{
  std::swap(mpContext, o.mpContext);
  std::swap(mpType, o.mpType);
  std::swap(mpType_FIPS, o.mpType_FIPS);
  return *this;
}

void ssl::OpenSSLDigest::Restart() {
  if (mpType_FIPS) {
    EVP_DigestInit_ex(mpContext, mpType_FIPS, NULL);
  } else {
    EVP_DigestInit_ex(mpContext, mpType, NULL);
  }
}

void ssl::OpenSSLDigest::SetFlags(int flags) {
  if (flags == EVP_MD_CTX_FLAG_NON_FIPS_ALLOW && OpenSSL_version_num() >= 0x30000000L && mpType == EVP_md5() && !mpType_FIPS) {
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
    mpType_FIPS = EVP_MD_fetch(NULL, "MD5", "fips=no");
#endif  // OPENSSL_VERSION_NUMBER >= 0x30000000L
  } else {
    EVP_MD_CTX_set_flags(mpContext, flags);
  }
  this->Restart();
}

void ssl::OpenSSLDigest::Update(const unsigned char *input, size_t length) {
  if (length) {
    EVP_DigestUpdate(mpContext, const_cast<void *>(reinterpret_cast<const void *>(input)), length);
  }
}

void ssl::OpenSSLDigest::Final(unsigned char *digest) {
  unsigned int s;
  EVP_DigestFinal_ex(mpContext, digest, &s);
}

}

#pragma clang diagnostic pop
#pragma GCC diagnostic pop
