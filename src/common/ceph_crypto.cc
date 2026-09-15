// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <utility>

#include "common/ceph_context.h"
#include "common/config.h"
#include "ceph_crypto.h"

#include <openssl/evp.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

namespace TOPNSPC::crypto::ssl {

void zeroize_for_security(void* const s, const size_t n) {
  OPENSSL_cleanse(s, n);
}

} // namespace TOPNSPC::crypto::openssl


namespace TOPNSPC::crypto {
void init() {
  // OpenSSL >= 1.1.0 self-initializes (OPENSSL_init_crypto) and
  // registers its own atexit cleanup; nothing to do.
}

void shutdown([[maybe_unused]] const bool shared) {
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
}

ssl::OpenSSLDigest::OpenSSLDigest(OpenSSLDigest&& o) noexcept
  : mpContext(std::exchange(o.mpContext, nullptr)),
    mpType(std::exchange(o.mpType, nullptr))
{
}

ssl::OpenSSLDigest& ssl::OpenSSLDigest::operator=(OpenSSLDigest&& o) noexcept
{
  std::swap(mpContext, o.mpContext);
  std::swap(mpType, o.mpType);
  return *this;
}

void ssl::OpenSSLDigest::Restart() {
  EVP_DigestInit_ex(mpContext, mpType, NULL);
}

const EVP_MD *ssl::MD5NonCrypto::digest_type() {
#if OPENSSL_VERSION_NUMBER >= 0x30000000L
  // An explicit "fips=no" query term overrides the same term in the
  // default property query, and matches the default provider's MD5
  // (which does not define the "fips" property at all).
  // Process-lifetime cache, deliberately never freed.
  static const EVP_MD * const md = []() -> const EVP_MD * {
    if (EVP_MD * const fetched = EVP_MD_fetch(nullptr, "MD5", "fips=no")) {
      return fetched;
    }
    return EVP_md5();  // no provider offers non-FIPS MD5; legacy fallback
  }();
  return md;
#else
  // Pre-3.0: EVP_MD_CTX_FLAG_NON_FIPS_ALLOW was already a no-op outside
  // the ancient 1.0.x FIPS module, so plain MD5 is behavior-identical.
  return EVP_md5();
#endif
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
