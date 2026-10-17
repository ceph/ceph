// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <cstring>

#include <openssl/evp.h>

#include "gtest/gtest.h"
#include "common/ceph_crypto.h"

// MD5NonCrypto exists so that the protocol-mandated MD5 uses in rgw (ETags,
// object and cache keys) keep working when the default property query is
// "fips=yes", where an unqualified MD5 fetch resolves to nothing.
//
// This is its own test binary because turning on the fips default property
// is process-global and cannot be undone faithfully: OpenSSL 3 has no getter
// for the default property query, and EVP_default_properties_enable_fips(ctx,
// 0) clears the "fips" term rather than restoring whatever openssl.cnf set.
// MD5NonCrypto also caches its EVP_MD for the life of the process, so the
// property has to be set before the first instance is built.  Keep this the
// only test in the binary.
TEST(MD5NonCrypto, FipsDefaultProperty) {
#if OPENSSL_VERSION_NUMBER < 0x30000000L
  GTEST_SKIP() << "needs OpenSSL 3 property queries";
#else
  ASSERT_EQ(1, EVP_default_properties_enable_fips(nullptr, 1));

  // Control.  Everything below passes trivially if MD5 is still reachable
  // under "fips=yes", so establish that the query really does bite first.
  if (EVP_MD * const plain = EVP_MD_fetch(nullptr, "MD5", nullptr)) {
    EVP_MD_free(plain);
    GTEST_SKIP() << "MD5 resolves even with fips=yes; test would be vacuous";
  }

  // The mechanism MD5NonCrypto relies on: an explicit "fips=no" term
  // overrides the same term in the default query.
  EVP_MD * const non_fips = EVP_MD_fetch(nullptr, "MD5", "fips=no");
  ASSERT_NE(nullptr, non_fips)
    << "MD5 with an explicit fips=no query did not resolve";
  EVP_MD_free(non_fips);

  // ...and what it produces is still MD5.  Vector from RFC 1321 appendix A.5.
  ceph::crypto::MD5NonCrypto h;
  h.Update((const unsigned char*)"abc", 3);
  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  h.Final(digest);
  const unsigned char want_digest[CEPH_CRYPTO_MD5_DIGESTSIZE] = {
    0x90, 0x01, 0x50, 0x98, 0x3c, 0xd2, 0x4f, 0xb0,
    0xd6, 0x96, 0x3f, 0x7d, 0x28, 0xe1, 0x7f, 0x72,
  };
  ASSERT_EQ(0, memcmp(digest, want_digest, CEPH_CRYPTO_MD5_DIGESTSIZE));
#endif
}
