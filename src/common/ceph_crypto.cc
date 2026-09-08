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
