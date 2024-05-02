#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/ceph_crypto.h"
#include "common/common_init.h"
#include "global/global_init.h"
#include "global/global_context.h"

class CryptoEnvironment: public ::testing::Environment {
public:
  void SetUp() override {
    ceph::crypto::init();
  }
};

TEST(MD5, Simple) {
  ceph::crypto::MD5 h;
  h.Update((const unsigned char*)"foo", 3);
  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[CEPH_CRYPTO_MD5_DIGESTSIZE] = {
    0xac, 0xbd, 0x18, 0xdb, 0x4c, 0xc2, 0xf8, 0x5c,
    0xed, 0xef, 0x65, 0x4f, 0xcc, 0xc4, 0xa4, 0xd8,
  };
  err = memcmp(digest, want_digest, CEPH_CRYPTO_MD5_DIGESTSIZE);
  ASSERT_EQ(0, err);
}

TEST(MD5, MultiUpdate) {
  ceph::crypto::MD5 h;
  h.Update((const unsigned char*)"", 0);
  h.Update((const unsigned char*)"fo", 2);
  h.Update((const unsigned char*)"", 0);
  h.Update((const unsigned char*)"o", 1);
  h.Update((const unsigned char*)"", 0);
  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[CEPH_CRYPTO_MD5_DIGESTSIZE] = {
    0xac, 0xbd, 0x18, 0xdb, 0x4c, 0xc2, 0xf8, 0x5c,
    0xed, 0xef, 0x65, 0x4f, 0xcc, 0xc4, 0xa4, 0xd8,
  };
  err = memcmp(digest, want_digest, CEPH_CRYPTO_MD5_DIGESTSIZE);
  ASSERT_EQ(0, err);
}

TEST(MD5, Restart) {
  ceph::crypto::MD5 h;
  h.Update((const unsigned char*)"bar", 3);
  h.Restart();
  h.Update((const unsigned char*)"foo", 3);
  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[CEPH_CRYPTO_MD5_DIGESTSIZE] = {
    0xac, 0xbd, 0x18, 0xdb, 0x4c, 0xc2, 0xf8, 0x5c,
    0xed, 0xef, 0x65, 0x4f, 0xcc, 0xc4, 0xa4, 0xd8,
  };
  err = memcmp(digest, want_digest, CEPH_CRYPTO_MD5_DIGESTSIZE);
  ASSERT_EQ(0, err);
}

TEST(HMACSHA1, Simple) {
  ceph::crypto::HMACSHA1 h((const unsigned char*)"sekrit", 6);
  h.Update((const unsigned char*)"foo", 3);
  unsigned char digest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE] = {
    0x04, 0xbc, 0x52, 0x66, 0xb6, 0xff, 0xad, 0xad, 0x9d, 0x57,
    0xce, 0x13, 0xea, 0x8c, 0xf5, 0x6b, 0xf9, 0x95, 0x2f, 0xd6,
  };
  err = memcmp(digest, want_digest, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
  ASSERT_EQ(0, err);
}

TEST(HMACSHA1, MultiUpdate) {
  ceph::crypto::HMACSHA1 h((const unsigned char*)"sekrit", 6);
  h.Update((const unsigned char*)"", 0);
  h.Update((const unsigned char*)"fo", 2);
  h.Update((const unsigned char*)"", 0);
  h.Update((const unsigned char*)"o", 1);
  h.Update((const unsigned char*)"", 0);
  unsigned char digest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE] = {
    0x04, 0xbc, 0x52, 0x66, 0xb6, 0xff, 0xad, 0xad, 0x9d, 0x57,
    0xce, 0x13, 0xea, 0x8c, 0xf5, 0x6b, 0xf9, 0x95, 0x2f, 0xd6,
  };
  err = memcmp(digest, want_digest, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
  ASSERT_EQ(0, err);
}

TEST(HMACSHA1, Restart) {
  ceph::crypto::HMACSHA1 h((const unsigned char*)"sekrit", 6);
  h.Update((const unsigned char*)"bar", 3);
  h.Restart();
  h.Update((const unsigned char*)"foo", 3);
  unsigned char digest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  h.Final(digest);
  int err;
  unsigned char want_digest[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE] = {
    0x04, 0xbc, 0x52, 0x66, 0xb6, 0xff, 0xad, 0xad, 0x9d, 0x57,
    0xce, 0x13, 0xea, 0x8c, 0xf5, 0x6b, 0xf9, 0x95, 0x2f, 0xd6,
  };
  err = memcmp(digest, want_digest, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
  ASSERT_EQ(0, err);
}

TEST(Digest, SHA1) {
  auto digest = [](const bufferlist& bl) {
    return ceph::crypto::digest<ceph::crypto::SHA1>(bl);
  };
  {
    bufferlist bl;
    sha1_digest_t sha1 = digest(bl);
    EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", sha1.to_str());
  }
  {
    bufferlist bl;
    bl.append("");
    sha1_digest_t sha1 = digest(bl);
    EXPECT_EQ("da39a3ee5e6b4b0d3255bfef95601890afd80709", sha1.to_str());
  }
  {
    bufferlist bl;
    bl.append("Hello");
    sha1_digest_t sha1 = digest(bl);
    EXPECT_EQ("f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0", sha1.to_str());
  }
  {
    bufferlist bl, bl2;
    bl.append("Hello");
    bl2.append(", world!");
    bl.claim_append(bl2);
    sha1_digest_t sha1 = digest(bl);
    EXPECT_EQ("943a702d06f34599aee1f8da8ef9f7296031d699", sha1.to_str());
    bl2.append("  How are you today?");
    bl.claim_append(bl2);
    sha1 = digest(bl);
    EXPECT_EQ("778b5d10e5133aa28fb8de71d35b6999b9a25eb4", sha1.to_str());
  }
  {
    bufferptr p(65536);
    memset(p.c_str(), 0, 65536);
    bufferlist bl;
    bl.append(p);
    sha1_digest_t sha1 = digest(bl);
    EXPECT_EQ("1adc95bebe9eea8c112d40cd04ab7a8d75c4f961", sha1.to_str());
  }
}

TEST(Digest, SHA256) {
  auto digest = [](const bufferlist& bl) {
    return ceph::crypto::digest<ceph::crypto::SHA256>(bl);
  };
  {
    bufferlist bl;
    sha256_digest_t sha256 = digest(bl);
    EXPECT_EQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", sha256.to_str());
  }
  {
    bufferlist bl;
    bl.append("");
    sha256_digest_t sha256 = digest(bl);
    EXPECT_EQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", sha256.to_str());
  }
  {
    bufferlist bl;
    bl.append("Hello");
    sha256_digest_t sha256 = digest(bl);
    EXPECT_EQ("185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", sha256.to_str());
  }
  {
    bufferlist bl, bl2;
    bl.append("Hello");
    bl2.append(", world!");
    bl.claim_append(bl2);
    sha256_digest_t sha256 = digest(bl);
    EXPECT_EQ("315f5bdb76d078c43b8ac0064e4a0164612b1fce77c869345bfc94c75894edd3", sha256.to_str());
    bl2.append("  How are you today?");
    bl.claim_append(bl2);
    sha256 = digest(bl);
    EXPECT_EQ("e85f57f8bb018bd4f7beed6f27488cef22b13d5e06e8b8a27cac8b087c2a549e", sha256.to_str());
  }
  {
    bufferptr p(65536);
    memset(p.c_str(), 0, 65536);
    bufferlist bl;
    bl.append(p);
    sha256_digest_t sha256 = digest(bl);
    EXPECT_EQ("de2f256064a0af797747c2b97505dc0b9f3df0de4f489eac731c23ae9ca9cc31", sha256.to_str());
  }
}

TEST(Digest, SHA512) {
  auto digest = [](const bufferlist& bl) {
    return ceph::crypto::digest<ceph::crypto::SHA512>(bl);
  };
  {
    bufferlist bl;
    sha512_digest_t sha512 = digest(bl);
    EXPECT_EQ("cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e", sha512.to_str());
  }
  {
    bufferlist bl;
    bl.append("");
    sha512_digest_t sha512 = digest(bl);
    EXPECT_EQ("cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e", sha512.to_str());
  }
  {
    bufferlist bl;
    bl.append("Hello");
    sha512_digest_t sha512 = digest(bl);
    EXPECT_EQ("3615f80c9d293ed7402687f94b22d58e529b8cc7916f8fac7fddf7fbd5af4cf777d3d795a7a00a16bf7e7f3fb9561ee9baae480da9fe7a18769e71886b03f315", sha512.to_str());
  }
  {
    bufferlist bl, bl2;
    bl.append("Hello");
    bl2.append(", world!");
    bl.claim_append(bl2);
    sha512_digest_t sha512 = digest(bl);
    EXPECT_EQ("c1527cd893c124773d811911970c8fe6e857d6df5dc9226bd8a160614c0cd963a4ddea2b94bb7d36021ef9d865d5cea294a82dd49a0bb269f51f6e7a57f79421", sha512.to_str());
    bl2.append("  How are you today?");
    bl.claim_append(bl2);
    sha512 = digest(bl);
    EXPECT_EQ("7d50e299496754f9a0d158e018d4b733f2ef51c487b43b50719ffdabe3c3da5a347029741056887b4ffa2ddd0aa9e0dd358b8ed9da9a4f3455f44896fc8e5395", sha512.to_str());
  }
  {
    bufferptr p(65536);
    memset(p.c_str(), 0, 65536);
    bufferlist bl;
    bl.append(p);
    sha512_digest_t sha512 = digest(bl);
    EXPECT_EQ("73e4153936dab198397b74ee9efc26093dda721eaab2f8d92786891153b45b04265a161b169c988edb0db2c53124607b6eaaa816559c5ce54f3dbc9fa6a7a4b2", sha512.to_str());
  }
}

class ForkDeathTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // shutdown NSS so it can be reinitialized after the fork
    // some data structures used by NSPR are only initialized once, and they
    // will be cleaned up with ceph::crypto::shutdown(false), so we need to
    // keep them around after fork.
    ceph::crypto::shutdown(true);
  }

  void TearDown() override {
    // undo the NSS shutdown we did in the parent process, after the
    // test is done
    ceph::crypto::init();
  }
};

void do_simple_crypto() {
  // ensure that the shutdown/fork/init sequence results in a working
  // NSS crypto library; this function is run in the child, after the
  // fork, and if you comment out the ceph::crypto::init, or if the
  // trick were to fail, you would see this ending in an assert and
  // not exit status 0
  ceph::crypto::init();
  ceph::crypto::MD5 h;
  h.Update((const unsigned char*)"foo", 3);
  unsigned char digest[CEPH_CRYPTO_MD5_DIGESTSIZE];
  h.Final(digest);
  exit(0);
}

#if GTEST_HAS_DEATH_TEST && !defined(_WIN32)

#ifndef __has_feature
#define __has_feature(x) 0
#endif

TEST_F(ForkDeathTest, MD5) {
#if __has_feature(address_sanitizer) || defined(__SANITIZE_ADDRESS__)
  // sanitizer warns like:
  // ==3798016==Running thread 3797882 was not suspended. False leaks are possible.
  // but we should not take it as a fatal error.
  const std::string matcher = ".*False leaks are possible.*";
#else
  const std::string matcher = "^$";
#endif
  ASSERT_EXIT(do_simple_crypto(), ::testing::ExitedWithCode(0), matcher);
}
#endif // GTEST_HAS_DEATH_TEST && !defined(_WIN32)

int main(int argc, char **argv) {
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(NULL, args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
