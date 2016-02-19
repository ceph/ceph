#include <errno.h>
#include <time.h>

#include "include/types.h"
#include "auth/Crypto.h"
#include "common/Clock.h"
#include "common/ceph_crypto.h"
#include "common/ceph_context.h"
#include "global/global_context.h"

#include "test/unit.h"

class CryptoEnvironment: public ::testing::Environment {
public:
  void SetUp() {
    ceph::crypto::init(g_ceph_context);
  }
};

TEST(AES, ValidateSecret) {
  CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);
  int l;

  for (l=0; l<16; l++) {
    bufferptr bp(l);
    int err;
    err = h->validate_secret(bp);
    EXPECT_EQ(-EINVAL, err);
  }

  for (l=16; l<50; l++) {
    bufferptr bp(l);
    int err;
    err = h->validate_secret(bp);
    EXPECT_EQ(0, err);
  }
}

TEST(AES, Encrypt) {
  CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);
  char secret_s[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  };
  bufferptr secret(secret_s, sizeof(secret_s));

  unsigned char plaintext_s[] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
  };
  bufferlist plaintext;
  plaintext.append((char *)plaintext_s, sizeof(plaintext_s));

  bufferlist cipher;
  std::string error;
  CryptoKeyHandler *kh = h->get_key_handler(secret, error);
  int r = kh->encrypt(plaintext, cipher, &error);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(error, "");

  unsigned char want_cipher[] = {
    0xb3, 0x8f, 0x5b, 0xc9, 0x35, 0x4c, 0xf8, 0xc6,
    0x13, 0x15, 0x66, 0x6f, 0x37, 0xd7, 0x79, 0x3a,
    0x11, 0x90, 0x7b, 0xe9, 0xd8, 0x3c, 0x35, 0x70,
    0x58, 0x7b, 0x97, 0x9b, 0x03, 0xd2, 0xa5, 0x01,
  };
  char cipher_s[sizeof(want_cipher)];

  ASSERT_EQ(sizeof(cipher_s), cipher.length());
  cipher.copy(0, sizeof(cipher_s), &cipher_s[0]);

  int err;
  err = memcmp(cipher_s, want_cipher, sizeof(want_cipher));
  ASSERT_EQ(0, err);
}

TEST(AES, Decrypt) {
  CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);
  char secret_s[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  };
  bufferptr secret(secret_s, sizeof(secret_s));

  unsigned char cipher_s[] = {
    0xb3, 0x8f, 0x5b, 0xc9, 0x35, 0x4c, 0xf8, 0xc6,
    0x13, 0x15, 0x66, 0x6f, 0x37, 0xd7, 0x79, 0x3a,
    0x11, 0x90, 0x7b, 0xe9, 0xd8, 0x3c, 0x35, 0x70,
    0x58, 0x7b, 0x97, 0x9b, 0x03, 0xd2, 0xa5, 0x01,
  };
  bufferlist cipher;
  cipher.append((char *)cipher_s, sizeof(cipher_s));

  unsigned char want_plaintext[] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
  };
  char plaintext_s[sizeof(want_plaintext)];

  std::string error;
  bufferlist plaintext;
  CryptoKeyHandler *kh = h->get_key_handler(secret, error);
  int r = kh->decrypt(cipher, plaintext, &error);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(error, "");

  ASSERT_EQ(sizeof(plaintext_s), plaintext.length());
  plaintext.copy(0, sizeof(plaintext_s), &plaintext_s[0]);

  int err;
  err = memcmp(plaintext_s, want_plaintext, sizeof(want_plaintext));
  ASSERT_EQ(0, err);
}

TEST(AES, Loop) {
  int err;

  char secret_s[16];
  err = get_random_bytes(secret_s, sizeof(secret_s));
  ASSERT_EQ(0, err);
  bufferptr secret(secret_s, sizeof(secret_s));

  char orig_plaintext_s[1024];
  err = get_random_bytes(orig_plaintext_s, sizeof(orig_plaintext_s));
  ASSERT_EQ(0, err);

  bufferlist plaintext;
  plaintext.append(orig_plaintext_s, sizeof(orig_plaintext_s));

  for (int i=0; i<10000; i++) {
    bufferlist cipher;
    {
      CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);

      std::string error;
      CryptoKeyHandler *kh = h->get_key_handler(secret, error);
      int r = kh->encrypt(plaintext, cipher, &error);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(error, "");
    }
    plaintext.clear();

    {
      CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);
      std::string error;
      CryptoKeyHandler *ckh = h->get_key_handler(secret, error);
      int r = ckh->decrypt(cipher, plaintext, &error);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(error, "");
    }
  }

  char plaintext_s[sizeof(orig_plaintext_s)];
  plaintext.copy(0, sizeof(plaintext_s), &plaintext_s[0]);
  err = memcmp(plaintext_s, orig_plaintext_s, sizeof(orig_plaintext_s));
  ASSERT_EQ(0, err);
}

TEST(AES, LoopKey) {
  bufferptr k(16);
  get_random_bytes(k.c_str(), k.length());
  CryptoKey key(CEPH_CRYPTO_AES, ceph_clock_now(NULL), k);

  bufferlist data;
  bufferptr r(128);
  get_random_bytes(r.c_str(), r.length());
  data.append(r);

  utime_t start = ceph_clock_now(NULL);
  int n = 100000;

  for (int i=0; i<n; ++i) {
    bufferlist encoded;
    string error;
    int r = key.encrypt(g_ceph_context, data, encoded, &error);
    ASSERT_EQ(r, 0);
  }

  utime_t end = ceph_clock_now(NULL);
  utime_t dur = end - start;
  cout << n << " encoded in " << dur << std::endl;
}
