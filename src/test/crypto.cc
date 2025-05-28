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
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES);
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
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES);
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
  int r = kh->encrypt(g_ceph_context, plaintext, cipher, &error);
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
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES);
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
  const auto needed = kh->encrypt(g_ceph_context, plain_slice, probe_slice);
  ASSERT_GE(needed, plain_slice.length);

  boost::container::small_vector<
    // FIXME?
    //unsigned char, sizeof(plaintext) + kh->get_block_size()> buf;
    unsigned char, sizeof(plaintext) + 16> buf(needed);
  const CryptoKey::out_slice_t cipher_slice { needed, buf.data() };
  const auto cipher_size = kh->encrypt(g_ceph_context, plain_slice, cipher_slice);
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
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES);
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
  int r = kh->decrypt(g_ceph_context, cipher, plaintext, &error);
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
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES);
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
  const auto plain_size = kh->decrypt(g_ceph_context, cipher_slice, plain_slice);

  ASSERT_EQ(plain_size, sizeof(want_plaintext));

  const int err = memcmp(plaintext, want_plaintext, plain_size);
  ASSERT_EQ(0, err);
}

template <std::size_t TextSizeV>
static void aes_loop_cephx() {
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES);

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
  const auto needed = kh->encrypt(g_ceph_context, plain_slice, probe_slice);
  ASSERT_GE(needed, plain_slice.length);

  boost::container::small_vector<
    // FIXME?
    //unsigned char, sizeof(plaintext) + kh->get_block_size()> buf;
    unsigned char, sizeof(plaintext) + 16> buf(needed);

  std::size_t cipher_size;
  for (std::size_t i = 0; i < 1000000; i++) {
    const CryptoKey::out_slice_t cipher_slice { needed, buf.data() };
    cipher_size = kh->encrypt(g_ceph_context, plain_slice, cipher_slice);
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

static void cipher_loop(const std::size_t text_size, int type, int cipher_len) {
  CryptoRandom random;

  bufferptr secret(cipher_len);
  random.get_bytes(secret.c_str(), secret.length());

  bufferptr orig_plaintext(text_size);
  random.get_bytes(orig_plaintext.c_str(), orig_plaintext.length());

  bufferlist plaintext;
  plaintext.append(orig_plaintext.c_str(), orig_plaintext.length());

  for (int i=0; i<10000; i++) {
    bufferlist cipher;
    {
      auto h = g_ceph_context->get_crypto_manager()->get_handler(type);

      std::string error;
      CryptoKeyHandler *kh = h->get_key_handler(secret, error);
      int r = kh->encrypt(g_ceph_context, plaintext, cipher, &error);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(error, "");

      delete kh;
    }
    plaintext.clear();

    {
      auto h = g_ceph_context->get_crypto_manager()->get_handler(type);
      std::string error;
      CryptoKeyHandler *ckh = h->get_key_handler(secret, error);
      int r = ckh->decrypt(g_ceph_context, cipher, plaintext, &error);
      ASSERT_EQ(r, 0);
      ASSERT_EQ(error, "");

      delete ckh;
    }
  }

  bufferlist orig;
  orig.append(orig_plaintext);
  ASSERT_EQ(orig, plaintext);
}

static void aes_loop(const std::size_t text_size) {
  cipher_loop(text_size, CEPH_CRYPTO_AES, 16);
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

static void dump_buf(string title, const unsigned char *buf, int len)
{
  std::cout << title << std::endl;
  for (int i = 0; i < len; ++i) {
    if (i != 0 && i % 16 == 0) {
      cout << std::endl;
    }
    std::cout << std::format("{:02x} ", buf[i]);
  }
  std::cout << std::endl << std::endl;
}

TEST(AES256KRB5, ValidateSecret) {
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES256KRB5);
  int l;

  for (l=0; l<32; l++) {
    bufferptr bp(l);
    int err;
    err = h->validate_secret(bp);
    EXPECT_EQ(-EINVAL, err);
  }

  for (l=32; l<50; l++) {
    bufferptr bp(l);
    int err;
    err = h->validate_secret(bp);
    EXPECT_EQ(0, err);
  }
}

struct test_vec {
  vector<unsigned char> secret;
  vector<unsigned char> confounder;
  vector<unsigned char> plaintext;
  vector<unsigned char> ciphertext;
  uint32_t usage = 0;
};

/*
 * AES256KRB5 test vectors off RFC8009
 */
static test_vec tv[] = {
  {
    .secret = {
      0x6D, 0x40, 0x4D, 0x37, 0xFA, 0xF7, 0x9F, 0x9D, 0xF0, 0xD3, 0x35, 0x68, 0xD3, 0x20, 0x66, 0x98,
      0x00, 0xEB, 0x48, 0x36, 0x47, 0x2E, 0xA8, 0xA0, 0x26, 0xD1, 0x6B, 0x71, 0x82, 0x46, 0x0C, 0x52 },
    .confounder = {
      0xF7, 0x64, 0xE9, 0xFA, 0x15, 0xC2, 0x76, 0x47, 0x8B, 0x2C, 0x7D, 0x0C, 0x4E, 0x5F, 0x58, 0xE4 },
    .plaintext = { },
    .ciphertext = {
      0x41, 0xF5, 0x3F, 0xA5, 0xBF, 0xE7, 0x02, 0x6D, 0x91, 0xFA, 0xF9, 0xBE, 0x95, 0x91, 0x95, 0xA0,
      0x58, 0x70, 0x72, 0x73, 0xA9, 0x6A, 0x40, 0xF0, 0xA0, 0x19, 0x60, 0x62, 0x1A, 0xC6, 0x12, 0x74,
      0x8B, 0x9B, 0xBF, 0xBE, 0x7E, 0xB4, 0xCE, 0x3C },
    .usage = 2
  },
  {
    .secret = {
      0x6D, 0x40, 0x4D, 0x37, 0xFA, 0xF7, 0x9F, 0x9D, 0xF0, 0xD3, 0x35, 0x68, 0xD3, 0x20, 0x66, 0x98,
      0x00, 0xEB, 0x48, 0x36, 0x47, 0x2E, 0xA8, 0xA0, 0x26, 0xD1, 0x6B, 0x71, 0x82, 0x46, 0x0C, 0x52 },
    .confounder = {
      0xB8, 0x0D, 0x32, 0x51, 0xC1, 0xF6, 0x47, 0x14, 0x94, 0x25, 0x6F, 0xFE, 0x71, 0x2D, 0x0B, 0x9A },
    .plaintext = { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05 },
    .ciphertext = {
      0x4E, 0xD7, 0xB3, 0x7C, 0x2B, 0xCA, 0xC8, 0xF7, 0x4F, 0x23, 0xC1, 0xCF, 0x07, 0xE6, 0x2B, 0xC7,
      0xB7, 0x5F, 0xB3, 0xF6, 0x37, 0xB9, 0xF5, 0x59, 0xC7, 0xF6, 0x64, 0xF6, 0x9E, 0xAB, 0x7B, 0x60,
      0x92, 0x23, 0x75, 0x26, 0xEA, 0x0D, 0x1F, 0x61, 0xCB, 0x20, 0xD6, 0x9D, 0x10, 0xF2 },
    .usage = 2
  },
  {
    .secret = {
      0x6D, 0x40, 0x4D, 0x37, 0xFA, 0xF7, 0x9F, 0x9D, 0xF0, 0xD3, 0x35, 0x68, 0xD3, 0x20, 0x66, 0x98,
      0x00, 0xEB, 0x48, 0x36, 0x47, 0x2E, 0xA8, 0xA0, 0x26, 0xD1, 0x6B, 0x71, 0x82, 0x46, 0x0C, 0x52 },
    .confounder = {
      0x53, 0xBF, 0x8A, 0x0D, 0x10, 0x52, 0x65, 0xD4, 0xE2, 0x76, 0x42, 0x86, 0x24, 0xCE, 0x5E, 0x63 },
    .plaintext = {
      0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F },
    .ciphertext = {
      0xBC, 0x47, 0xFF, 0xEC, 0x79, 0x98, 0xEB, 0x91, 0xE8, 0x11, 0x5C, 0xF8, 0xD1, 0x9D, 0xAC, 0x4B,
      0xBB, 0xE2, 0xE1, 0x63, 0xE8, 0x7D, 0xD3, 0x7F, 0x49, 0xBE, 0xCA, 0x92, 0x02, 0x77, 0x64, 0xF6,
      0x8C, 0xF5, 0x1F, 0x14, 0xD7, 0x98, 0xC2, 0x27, 0x3F, 0x35, 0xDF, 0x57, 0x4D, 0x1F, 0x93, 0x2E,
      0x40, 0xC4, 0xFF, 0x25, 0x5B, 0x36, 0xA2, 0x66 },
    .usage = 2
  },
  {
    .secret = {
      0x6D, 0x40, 0x4D, 0x37, 0xFA, 0xF7, 0x9F, 0x9D, 0xF0, 0xD3, 0x35, 0x68, 0xD3, 0x20, 0x66, 0x98,
      0x00, 0xEB, 0x48, 0x36, 0x47, 0x2E, 0xA8, 0xA0, 0x26, 0xD1, 0x6B, 0x71, 0x82, 0x46, 0x0C, 0x52 },
    .confounder = {
      0x76, 0x3E, 0x65, 0x36, 0x7E, 0x86, 0x4F, 0x02, 0xF5, 0x51, 0x53, 0xC7, 0xE3, 0xB5, 0x8A, 0xF1 },
    .plaintext = {
      0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
      0x10, 0x11, 0x12, 0x13, 0x14 },
    .ciphertext = {
      0x40, 0x01, 0x3E, 0x2D, 0xF5, 0x8E, 0x87, 0x51, 0x95, 0x7D, 0x28, 0x78, 0xBC, 0xD2, 0xD6, 0xFE,
      0x10, 0x1C, 0xCF, 0xD5, 0x56, 0xCB, 0x1E, 0xAE, 0x79, 0xDB, 0x3C, 0x3E, 0xE8, 0x64, 0x29, 0xF2,
      0xB2, 0xA6, 0x02, 0xAC, 0x86, 0xFE, 0xF6, 0xEC, 0xB6, 0x47, 0xD6, 0x29, 0x5F, 0xAE, 0x07, 0x7A,
      0x1F, 0xEB, 0x51, 0x75, 0x08, 0xD2, 0xC1, 0x6B, 0x41, 0x92, 0xE0, 0x1F, 0x62 },
    .usage = 2
  },
  {}
};

struct tvbl {
  bufferptr secret;
  bufferlist confounder;
  bufferlist plaintext;
  bufferlist ciphertext;
  uint32_t usage;

  tvbl(test_vec& tv) : secret((char *)tv.secret.data(), tv.secret.size()),
                                  usage(tv.usage) {
    confounder.append((char *)tv.confounder.data(), tv.confounder.size());
    plaintext.append((char *)tv.plaintext.data(), tv.plaintext.size());
    ciphertext.append((char *)tv.ciphertext.data(), tv.ciphertext.size());
  }
};

TEST(AES256KRB5, Encrypt) {
  for (int i = 0; !tv[i].secret.empty(); ++i) {
    auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES256KRB5);
    
    tvbl t(tv[i]);

    auto& secret = t.secret;
    auto& confounder = t.confounder;
    auto& plaintext = t.plaintext;
    auto& want_cipher = t.ciphertext;

    bufferlist cipher;
    std::string error;

    std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler_ext(secret, t.usage, error));
    int r = kh->encrypt_ext(g_ceph_context, plaintext, &confounder, cipher, &error);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(error, "");

    ASSERT_EQ(want_cipher.length(), cipher.length());

    dump_buf("ENCRYPTED:", (unsigned char *)cipher.c_str(), cipher.length());
    dump_buf("EXPECTED:", (unsigned char *)want_cipher.c_str(), want_cipher.length());

    int err;
    err = memcmp(cipher.c_str(), want_cipher.c_str(), want_cipher.length());
    ASSERT_EQ(0, err);
  }
}

TEST(AES256KRB5, EncryptNoBl) {
  for (int i = 0; !tv[i].secret.empty(); ++i) {
    auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES256KRB5);

    tvbl t(tv[i]);

    auto& secret = t.secret;
    auto& confounder = t.confounder;
    auto& plaintext = t.plaintext;
    auto& want_cipher = t.ciphertext;

    const CryptoKey::in_slice_t confounder_slice { confounder.length(), (const unsigned char *)confounder.c_str() };

    std::string error;
    std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler_ext(secret, t.usage, error));

    const CryptoKey::in_slice_t plain_slice { plaintext.length(), (const unsigned char *)plaintext.c_str() };

    // we need to deduce size first
    const CryptoKey::out_slice_t probe_slice { 0, nullptr };
    const auto needed = kh->encrypt_ext(g_ceph_context, plain_slice, &confounder_slice, probe_slice);
    ASSERT_GE(needed, plain_slice.length);

    std::vector<unsigned char> buf(needed);
    const CryptoKey::out_slice_t cipher_slice { needed, buf.data() };
    const auto cipher_size = kh->encrypt_ext(g_ceph_context, plain_slice, &confounder_slice, cipher_slice);
    ASSERT_EQ(cipher_size, needed);

    dump_buf("ENCRYPTED:", buf.data(), cipher_size);
    dump_buf("EXPECTED:", (unsigned char *)want_cipher.c_str(), want_cipher.length());
    ASSERT_EQ(want_cipher.length(), cipher_size);

    const int err = memcmp(buf.data(), want_cipher.c_str(), want_cipher.length());
    ASSERT_EQ(0, err);
  }
}

TEST(AES256KRB5, Decrypt) {
  for (int i = 0; !tv[i].secret.empty(); ++i) {
    auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES256KRB5);

    tvbl t(tv[i]);

    auto& secret = t.secret;
    auto& want_plaintext = t.plaintext;
    auto& ciphertext = t.ciphertext;

    std::string error;
    bufferlist plaintext;
    std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler_ext(secret, t.usage, error));
    int r = kh->decrypt(g_ceph_context, ciphertext, plaintext, &error);
    ASSERT_EQ(r, 0);
    ASSERT_EQ(error, "");

    dump_buf("DECRYPTED:", (unsigned char *)plaintext.c_str(), plaintext.length());
    dump_buf("EXPECTED:", (unsigned char *)want_plaintext.c_str(), want_plaintext.length());
    ASSERT_EQ(want_plaintext.length(), plaintext.length());

    int err;
    err = memcmp(plaintext.c_str(), want_plaintext.c_str(), plaintext.length());
    ASSERT_EQ(0, err);
  }
}

TEST(AES256KRB5, DecryptNoBl) {
  for (int i = 0; !tv[i].secret.empty(); ++i)  {
    auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES256KRB5);

    tvbl t(tv[i]);

    auto& secret = t.secret;
    auto& want_plaintext = t.plaintext;
    auto& ciphertext = t.ciphertext;

    std::size_t plain_buf_size = want_plaintext.length();
    unsigned char plaintext[plain_buf_size];

    std::string error;
    std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler_ext(secret, t.usage, error));

    CryptoKey::in_slice_t cipher_slice { ciphertext.length(), (const unsigned char *)ciphertext.c_str() };
    CryptoKey::out_slice_t plain_slice { plain_buf_size, plaintext };
    const auto plain_size = kh->decrypt(g_ceph_context, cipher_slice, plain_slice);

    dump_buf("DECRYPTED:", plaintext, plain_size);
    dump_buf("EXPECTED:", (unsigned char *)want_plaintext.c_str(), want_plaintext.length());
    ASSERT_EQ(plain_size, want_plaintext.length());

    const int err = memcmp(plaintext, want_plaintext.c_str(), plain_size);
    ASSERT_EQ(0, err);
  }
}

TEST(AES256KRB5, HMAC_SHA256) {
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES256KRB5);

  unsigned char secret_s[] = { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                               0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };
  bufferptr secret((const char *)secret_s, sizeof(secret_s));
  std::string plaintext = "blablabla";

  std::string error;

  std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler(secret, error));

  bufferlist bl;
  bl.append((const char *)plaintext.c_str(), plaintext.size());
  auto hash = kh->hmac_sha256(bl);

  dump_buf("HMAC_SHA256:", (const unsigned char *)&hash, sizeof(hash));

  unsigned char expected_s[] = { 0x42, 0xc7, 0x02, 0x7e, 0x8b, 0xe0, 0x6d, 0xca,
                                 0x2c, 0x0b, 0x44, 0x43, 0x73, 0xfe, 0xfd, 0xbe,
                                 0xac, 0x5b, 0x40, 0x34, 0xec, 0xa4, 0x4a, 0x69,
                                 0xde, 0x3a, 0x29, 0x16, 0x34, 0xed, 0x8d, 0xf9 };


  ASSERT_EQ(0, memcmp(expected_s, (const char *)&hash, sizeof(hash)));
}

TEST(AES256KRB5, HMAC_SHA256_NoBl) {
  auto h = g_ceph_context->get_crypto_manager()->get_handler(CEPH_CRYPTO_AES256KRB5);

  unsigned char secret_s[] = { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
                               0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };
  bufferptr secret((const char *)secret_s, sizeof(secret_s));
  std::string plaintext = "testing1234blablabla";

  std::string error;

  std::unique_ptr<CryptoKeyHandler> kh(h->get_key_handler(secret, error));

  CryptoKey::in_slice_t plaintext_slice { plaintext.size(), (const unsigned char *)plaintext.c_str() };
  auto hash = kh->hmac_sha256(plaintext_slice);

  dump_buf("HMAC_SHA256:", (const unsigned char *)&hash, sizeof(hash));

  unsigned char expected_s[] = { 0x4b, 0xd3, 0xac, 0x39, 0x4a, 0xcc, 0x97, 0x06,
                                 0xdd, 0x09, 0xe6, 0x5c, 0x68, 0xad, 0xd4, 0xcf,
                                 0x09, 0x2c, 0xcd, 0xa1, 0xe7, 0x99, 0xe3, 0x5c,
                                 0x52, 0x73, 0x85, 0xbd, 0x79, 0x73, 0xc6, 0x98 };

  ASSERT_EQ(0, memcmp(expected_s, (const char *)&hash, sizeof(hash)));
}

static void aes256krb5_loop(const std::size_t text_size) {
  cipher_loop(text_size, CEPH_CRYPTO_AES256KRB5, 32);
}

TEST(AES256KRB5, Loop) {
  aes256krb5_loop(256);
}

TEST(AES256KRB5, Loop_29) {
  aes256krb5_loop(29);
}

TEST(AES256KRB5, Loop_32) {
  aes256krb5_loop(32);
}

