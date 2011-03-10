#include "common/ceph_crypto.h"

#include "gtest/gtest.h"

class CryptoEnvironment: public ::testing::Environment {
public:
  void SetUp() {
    ceph::crypto::init();
  }
};

::testing::Environment* const crypto_env = ::testing::AddGlobalTestEnvironment(new CryptoEnvironment);

TEST(MD5, DigestSize) {
  int s = ceph::crypto::MD5::DIGESTSIZE;
  ASSERT_EQ(16, s);
}

TEST(MD5, Simple) {
  ceph::crypto::MD5 h;
  h.Update((const byte*)"foo", 3);
  unsigned char digest[ceph::crypto::MD5::DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[ceph::crypto::MD5::DIGESTSIZE] = {
    0xac, 0xbd, 0x18, 0xdb, 0x4c, 0xc2, 0xf8, 0x5c,
    0xed, 0xef, 0x65, 0x4f, 0xcc, 0xc4, 0xa4, 0xd8,
  };
  err = memcmp(digest, want_digest, ceph::crypto::MD5::DIGESTSIZE);
  ASSERT_EQ(0, err);
}

TEST(MD5, MultiUpdate) {
  ceph::crypto::MD5 h;
  h.Update((const byte*)"", 0);
  h.Update((const byte*)"fo", 2);
  h.Update((const byte*)"", 0);
  h.Update((const byte*)"o", 1);
  h.Update((const byte*)"", 0);
  unsigned char digest[ceph::crypto::MD5::DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[ceph::crypto::MD5::DIGESTSIZE] = {
    0xac, 0xbd, 0x18, 0xdb, 0x4c, 0xc2, 0xf8, 0x5c,
    0xed, 0xef, 0x65, 0x4f, 0xcc, 0xc4, 0xa4, 0xd8,
  };
  err = memcmp(digest, want_digest, ceph::crypto::MD5::DIGESTSIZE);
  ASSERT_EQ(0, err);
}

TEST(MD5, Restart) {
  ceph::crypto::MD5 h;
  h.Update((const byte*)"bar", 3);
  h.Restart();
  h.Update((const byte*)"foo", 3);
  unsigned char digest[ceph::crypto::MD5::DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[ceph::crypto::MD5::DIGESTSIZE] = {
    0xac, 0xbd, 0x18, 0xdb, 0x4c, 0xc2, 0xf8, 0x5c,
    0xed, 0xef, 0x65, 0x4f, 0xcc, 0xc4, 0xa4, 0xd8,
  };
  err = memcmp(digest, want_digest, ceph::crypto::MD5::DIGESTSIZE);
  ASSERT_EQ(0, err);
}
