#include <errno.h>
#include <time.h>

#include "gtest/gtest.h"
#include "include/types.h"
#include "auth/Crypto.h"
#include "common/Clock.h"
#include "common/ceph_crypto.h"
#include "common/ceph_context.h"
#include "global/global_context.h"

class CryptoEnvironment: public ::testing::Environment {
public:
  void SetUp() override {
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
  CryptoKeyHandler *kh = h->get_key_handler(secret, AES_IV, error);
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

  delete kh;
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
  CryptoKeyHandler *kh = h->get_key_handler(secret, AES_IV, error);
  int r = kh->decrypt(cipher, plaintext, &error);
  ASSERT_EQ(r, 0);
  ASSERT_EQ(error, "");

  ASSERT_EQ(sizeof(plaintext_s), plaintext.length());
  plaintext.copy(0, sizeof(plaintext_s), &plaintext_s[0]);

  int err;
  err = memcmp(plaintext_s, want_plaintext, sizeof(want_plaintext));
  ASSERT_EQ(0, err);

  delete kh;
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
      CryptoKeyHandler *kh = h->get_key_handler(secret, AES_IV, error);
      int r = kh->encrypt(plaintext, cipher, &error);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(error, "");

      delete kh;
    }
    plaintext.clear();

    {
      CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);
      std::string error;
      CryptoKeyHandler *ckh = h->get_key_handler(secret, AES_IV, error);
      int r = ckh->decrypt(cipher, plaintext, &error);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(error, "");

      delete ckh;
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
  CryptoKey key(CEPH_CRYPTO_AES, ceph_clock_now(), k);

  bufferlist data;
  bufferptr r(128);
  get_random_bytes(r.c_str(), r.length());
  data.append(r);

  utime_t start = ceph_clock_now();
  int n = 100000;

  for (int i=0; i<n; ++i) {
    bufferlist encoded;
    string error;
    int r = key.encrypt(g_ceph_context, data, encoded, &error);
    ASSERT_EQ(r, 0);
  }

  utime_t end = ceph_clock_now();
  utime_t dur = end - start;
  cout << n << " encoded in " << dur << std::endl;
}


void check_encryption(int mode, const bufferlist& expected)
{
  CryptoHandler* ch = CryptoHandler::create(mode);
  ASSERT_NE(ch, nullptr);
  string error;
  static char key_s[] =
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
  static char iv_s[] =
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  bufferptr key(buffer::create_static(sizeof(key_s), key_s));
  bufferptr iv(buffer::create_static(sizeof(iv_s), iv_s));

  static char data_in[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  bufferlist in;
  in.append(data_in, sizeof(data_in));
  bufferlist out;
  bufferlist out_decrypt;
  CryptoKeyHandler* ckh;
  if (mode == CEPH_CRYPTO_AES_256_ECB)
    ckh = ch->get_key_handler(key, EMPTY_IV, error);
  else
    ckh = ch->get_key_handler(key, iv, error);
  ASSERT_NE(ckh, nullptr);
  ASSERT_EQ(ckh->encrypt(in, out, &error), 0);
  ASSERT_EQ(ckh->decrypt(out, out_decrypt, &error), 0);
  ASSERT_EQ(in, out_decrypt);
  delete ckh;
  delete ch;
}

void check_encryption_errors(int mode, bool check_iv)
{
  CryptoHandler* ch = CryptoHandler::create(mode);
  ASSERT_NE(ch, nullptr);
  string error;
  static char key_s[] =
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31};
  static char key_s_too_long[] =
      {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
      16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};
  static char key_s_too_short[] =
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30};

  static char iv_s[] =
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  static char iv_s_too_long[] =
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  static char iv_s_too_short[] =
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};

  bufferptr key(buffer::create_static(sizeof(key_s), key_s));
  bufferptr key_too_long(buffer::create_static(sizeof(key_s_too_long), key_s_too_long));
  bufferptr key_too_short(buffer::create_static(sizeof(key_s_too_short), key_s_too_short));

  bufferptr iv(buffer::create_static(sizeof(iv_s), iv_s));
  bufferptr iv_too_long(buffer::create_static(sizeof(iv_s_too_long), iv_s_too_long));
  bufferptr iv_too_short(buffer::create_static(sizeof(iv_s_too_short), iv_s_too_short));

  CryptoKeyHandler* ckh;
  ckh = ch->get_key_handler(key_too_long, iv, error);
  ASSERT_EQ(ckh, nullptr);
  cout << "Key too long: error = " << error << endl;

  ckh = ch->get_key_handler(key_too_short, iv, error);
  ASSERT_EQ(ckh, nullptr);
  cout << "Key too short: error = " << error << endl;

  if (check_iv) {
    ckh = ch->get_key_handler(key, iv_too_long, error);
    ASSERT_EQ(ckh, nullptr);
    cout << "IV too long: error = " << error << endl;

    ckh = ch->get_key_handler(key, iv_too_short, error);
    ASSERT_EQ(ckh, nullptr);
    cout << "IV too short: error = " << error << endl;
  }
  if (mode != CEPH_CRYPTO_AES_256_ECB) {
    ckh = ch->get_key_handler(key, iv, error);
  }
  else {
    ckh = ch->get_key_handler(key, EMPTY_IV, error);
  }
  ASSERT_NE(ckh, nullptr);


  static char data_in[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  static char data_in_uneven[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  bufferlist in;
  in.append(data_in, sizeof(data_in));
  bufferlist in_uneven;
  in_uneven.append(data_in_uneven, sizeof(data_in_uneven));
  bufferlist out;

  ASSERT_NE(ckh->encrypt(in_uneven, out, &error), 0);

  cout << "Improper input length: error = " << error << endl;
  delete ckh;
  delete ch;
}

TEST(AES256, AES_256_CBC) {
  char expected_s[] = {-14,-112,0,-74,42,73,-97,-48,-87,-13,-102,106,-35,46,119,-128};
  bufferlist expected;
  expected.append(expected_s, sizeof(expected_s));
  check_encryption(CEPH_CRYPTO_AES_256_CBC, expected);
}

TEST(AES256, AES_256_ECB) {
  char expected_s[] = {90,110,4,87,8,-5,113,-106,-16,46,85,61,2,-61,-90,-110};
  bufferlist expected;
  expected.append(expected_s, sizeof(expected_s));
  check_encryption(CEPH_CRYPTO_AES_256_ECB, expected);
}

TEST(AES256, AES_256_CTR) {
  char expected_s[] = {90,111,6,84,12,-2,119,-111,-8,39,95,54,14,-50,-88,-99};
  bufferlist expected;
  expected.append(expected_s, sizeof(expected_s));
  check_encryption(CEPH_CRYPTO_AES_256_CTR, expected);
}

TEST(AES256, AES_256_CBC_errors) {
  check_encryption_errors(CEPH_CRYPTO_AES_256_CBC, true);
}

TEST(AES256, AES_256_ECB_errors) {
  check_encryption_errors(CEPH_CRYPTO_AES_256_ECB, false);
}

TEST(AES256, AES_256_CTR_errors) {
  check_encryption_errors(CEPH_CRYPTO_AES_256_CTR, true);
}
