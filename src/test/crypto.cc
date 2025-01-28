#include <errno.h>
#include <time.h>

#include <iostream> // for std::cout

#include <boost/container/small_vector.hpp>

#include "gtest/gtest.h"
#include "include/types.h"
#include "auth/Crypto.h"
#include "common/Clock.h"
#include "common/ceph_crypto.h"
#include "common/ceph_context.h"
#include "global/global_context.h"

using namespace std;

class CryptoEnvironment: public ::testing::Environment {
public:
  void SetUp() override {
    ceph::crypto::init();
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
  cipher.cbegin().copy(sizeof(cipher_s), &cipher_s[0]);

  int err;
  err = memcmp(cipher_s, want_cipher, sizeof(want_cipher));
  ASSERT_EQ(0, err);

  delete kh;
}

TEST(AES, EncryptNoBl) {
  CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);
  char secret_s[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  };
  bufferptr secret(secret_s, sizeof(secret_s));

  const unsigned char plaintext[] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
  };

  std::string error;
  std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler(secret, error));

  const CryptoKey::in_slice_t plain_slice { sizeof(plaintext), plaintext };

  // we need to deduce size first
  const CryptoKey::out_slice_t probe_slice { 0, nullptr };
  const auto needed = kh->encrypt(plain_slice, probe_slice);
  ASSERT_GE(needed, plain_slice.length);

  boost::container::small_vector<
    // FIXME?
    //unsigned char, sizeof(plaintext) + kh->get_block_size()> buf;
    unsigned char, sizeof(plaintext) + 16> buf(needed);
  const CryptoKey::out_slice_t cipher_slice { needed, buf.data() };
  const auto cipher_size = kh->encrypt(plain_slice, cipher_slice);
  ASSERT_EQ(cipher_size, needed);

  const unsigned char want_cipher[] = {
    0xb3, 0x8f, 0x5b, 0xc9, 0x35, 0x4c, 0xf8, 0xc6,
    0x13, 0x15, 0x66, 0x6f, 0x37, 0xd7, 0x79, 0x3a,
    0x11, 0x90, 0x7b, 0xe9, 0xd8, 0x3c, 0x35, 0x70,
    0x58, 0x7b, 0x97, 0x9b, 0x03, 0xd2, 0xa5, 0x01,
  };

  ASSERT_EQ(sizeof(want_cipher), cipher_size);

  const int err = memcmp(buf.data(), want_cipher, sizeof(want_cipher));
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
  plaintext.cbegin().copy(sizeof(plaintext_s), &plaintext_s[0]);

  int err;
  err = memcmp(plaintext_s, want_plaintext, sizeof(want_plaintext));
  ASSERT_EQ(0, err);

  delete kh;
}

TEST(AES, DecryptNoBl) {
  CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);
  const char secret_s[] = {
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  };
  bufferptr secret(secret_s, sizeof(secret_s));

  const unsigned char ciphertext[] = {
    0xb3, 0x8f, 0x5b, 0xc9, 0x35, 0x4c, 0xf8, 0xc6,
    0x13, 0x15, 0x66, 0x6f, 0x37, 0xd7, 0x79, 0x3a,
    0x11, 0x90, 0x7b, 0xe9, 0xd8, 0x3c, 0x35, 0x70,
    0x58, 0x7b, 0x97, 0x9b, 0x03, 0xd2, 0xa5, 0x01,
  };

  const unsigned char want_plaintext[] = {
    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
    0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff,
  };
  constexpr static std::size_t plain_buf_size = \
    CryptoKey::get_max_outbuf_size(sizeof(want_plaintext));
  unsigned char plaintext[plain_buf_size];

  std::string error;
  std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler(secret, error));

  CryptoKey::in_slice_t cipher_slice { sizeof(ciphertext), ciphertext };
  CryptoKey::out_slice_t plain_slice { sizeof(plaintext), plaintext };
  const auto plain_size = kh->decrypt(cipher_slice, plain_slice);

  ASSERT_EQ(plain_size, sizeof(want_plaintext));

  const int err = memcmp(plaintext, want_plaintext, sizeof(plain_size));
  ASSERT_EQ(0, err);
}

template <std::size_t TextSizeV>
static void aes_loop_cephx() {
  CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);

  CryptoRandom random;

  bufferptr secret(16);
  random.get_bytes(secret.c_str(), secret.length());
  std::string error;
  std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler(secret, error));

  unsigned char plaintext[TextSizeV];
  random.get_bytes(reinterpret_cast<char*>(plaintext), sizeof(plaintext));

  const CryptoKey::in_slice_t plain_slice { sizeof(plaintext), plaintext };

  // we need to deduce size first
  const CryptoKey::out_slice_t probe_slice { 0, nullptr };
  const auto needed = kh->encrypt(plain_slice, probe_slice);
  ASSERT_GE(needed, plain_slice.length);

  boost::container::small_vector<
    // FIXME?
    //unsigned char, sizeof(plaintext) + kh->get_block_size()> buf;
    unsigned char, sizeof(plaintext) + 16> buf(needed);

  std::size_t cipher_size;
  for (std::size_t i = 0; i < 1000000; i++) {
    const CryptoKey::out_slice_t cipher_slice { needed, buf.data() };
    cipher_size = kh->encrypt(plain_slice, cipher_slice);
    ASSERT_EQ(cipher_size, needed);
  }
}

// These magics reflects Cephx's signature size. Please consult
// CephxSessionHandler::_calc_signature() for more details.
TEST(AES, LoopCephx) {
  aes_loop_cephx<29>();
}

TEST(AES, LoopCephxV2) {
  aes_loop_cephx<32>();
}

static void aes_loop(const std::size_t text_size) {
  CryptoRandom random;

  bufferptr secret(16);
  random.get_bytes(secret.c_str(), secret.length());

  bufferptr orig_plaintext(text_size);
  random.get_bytes(orig_plaintext.c_str(), orig_plaintext.length());

  bufferlist plaintext;
  plaintext.append(orig_plaintext.c_str(), orig_plaintext.length());

  for (int i=0; i<10000; i++) {
    bufferlist cipher;
    {
      CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);

      std::string error;
      CryptoKeyHandler *kh = h->get_key_handler(secret, error);
      int r = kh->encrypt(plaintext, cipher, &error);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(error, "");

      delete kh;
    }
    plaintext.clear();

    {
      CryptoHandler *h = g_ceph_context->get_crypto_handler(CEPH_CRYPTO_AES);
      std::string error;
      CryptoKeyHandler *ckh = h->get_key_handler(secret, error);
      int r = ckh->decrypt(cipher, plaintext, &error);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(error, "");

      delete ckh;
    }
  }

  bufferlist orig;
  orig.append(orig_plaintext);
  ASSERT_EQ(orig, plaintext);
}

TEST(AES, Loop) {
  aes_loop(256);
}

// These magics reflects Cephx's signature size. Please consult
// CephxSessionHandler::_calc_signature() for more details.
TEST(AES, Loop_29) {
  aes_loop(29);
}

TEST(AES, Loop_32) {
  aes_loop(32);
}

void aes_loopkey(const std::size_t text_size) {
  CryptoRandom random;
  bufferptr k(16);
  random.get_bytes(k.c_str(), k.length());
  CryptoKey key(CEPH_CRYPTO_AES, ceph_clock_now(), k);

  bufferlist data;
  bufferptr r(text_size);
  random.get_bytes(r.c_str(), r.length());
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

TEST(AES, LoopKey) {
  aes_loopkey(128);
}

// These magics reflects Cephx's signature size. Please consult
// CephxSessionHandler::_calc_signature() for more details.
TEST(AES, LoopKey_29) {
  aes_loopkey(29);
}

TEST(AES, LoopKey_32) {
  aes_loopkey(32);
}
