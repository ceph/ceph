// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <librbd/crypto/DataCryptor.h>
#include "test/librbd/test_fixture.h"
#include "librbd/crypto/openssl/DataCryptor.h"

#include <sys/socket.h>
#include <linux/if_alg.h>
#include <linux/rtnetlink.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

namespace librbd {
namespace crypto {
namespace openssl {

const char* TEST_CIPHER_NAME = "aes-256-xts";
const unsigned char TEST_KEY[64] = {1};
const unsigned char TEST_IV[16] = {2};
const unsigned char TEST_IV_2[16] = {3};
const unsigned char TEST_DATA[4096] = {4};

struct TestCryptoOpensslDataCryptor : public TestFixture {
    DataCryptor *cryptor;

    void SetUp() override {
      TestFixture::SetUp();
      cryptor = new DataCryptor(reinterpret_cast<CephContext*>(m_ioctx.cct()));
      ASSERT_EQ(0,
                cryptor->init(TEST_CIPHER_NAME, TEST_KEY, sizeof(TEST_KEY)));
    }

    void TearDown() override {
      delete cryptor;
      TestFixture::TearDown();
    }
};

TEST_F(TestCryptoOpensslDataCryptor, InvalidCipherName) {
  EXPECT_EQ(-EINVAL, cryptor->init(nullptr, TEST_KEY, sizeof(TEST_KEY)));
  EXPECT_EQ(-EINVAL, cryptor->init("", TEST_KEY, sizeof(TEST_KEY)));
  EXPECT_EQ(-EINVAL, cryptor->init("Invalid", TEST_KEY, sizeof(TEST_KEY)));
}

TEST_F(TestCryptoOpensslDataCryptor, InvalidKey) {
  EXPECT_EQ(-EINVAL, cryptor->init(TEST_CIPHER_NAME, nullptr, 0));
  EXPECT_EQ(-EINVAL, cryptor->init(TEST_CIPHER_NAME, nullptr,
                                   sizeof(TEST_KEY)));
  EXPECT_EQ(-EINVAL, cryptor->init(TEST_CIPHER_NAME, TEST_KEY, 1));
}

TEST_F(TestCryptoOpensslDataCryptor, GetContextInvalidMode) {
  EXPECT_EQ(nullptr, cryptor->get_context(static_cast<CipherMode>(-1)));
}

TEST_F(TestCryptoOpensslDataCryptor, ReturnNullContext) {
  cryptor->return_context(nullptr, static_cast<CipherMode>(-1));
}

TEST_F(TestCryptoOpensslDataCryptor, ReturnContextInvalidMode) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  cryptor->return_context(ctx, static_cast<CipherMode>(-1));
}

TEST_F(TestCryptoOpensslDataCryptor, EncryptDecrypt) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  cryptor->init_context(ctx, TEST_IV, sizeof(TEST_IV));

  unsigned char out[sizeof(TEST_DATA)];
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx, CryptArgs{TEST_DATA, out, sizeof(TEST_DATA)}));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);
  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_DEC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, TEST_IV, sizeof(TEST_IV)));
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx, CryptArgs{out, out, sizeof(TEST_DATA)}));
  ASSERT_EQ(0, memcmp(out, TEST_DATA, sizeof(TEST_DATA)));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
}

TEST_F(TestCryptoOpensslDataCryptor, ReuseContext) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);

  ASSERT_EQ(0, cryptor->init_context(ctx, TEST_IV, sizeof(TEST_IV)));
  unsigned char out[sizeof(TEST_DATA)];
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx, CryptArgs{TEST_DATA, out, sizeof(TEST_DATA)}));

  ASSERT_EQ(0, cryptor->init_context(ctx, TEST_IV_2, sizeof(TEST_IV_2)));
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx, CryptArgs{TEST_DATA, out, sizeof(TEST_DATA)}));

  auto ctx2 = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx2, nullptr);

  ASSERT_EQ(0, cryptor->init_context(ctx2, TEST_IV_2, sizeof(TEST_IV_2)));
  unsigned char out2[sizeof(TEST_DATA)];
  ASSERT_EQ(sizeof(TEST_DATA),
            cryptor->update_context(ctx2, CryptArgs{TEST_DATA, out2, sizeof(TEST_DATA)}));

  ASSERT_EQ(0, memcmp(out, out2, sizeof(TEST_DATA)));

  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);
  cryptor->return_context(ctx2, CipherMode::CIPHER_MODE_ENC);
}

TEST_F(TestCryptoOpensslDataCryptor, InvalidIVLength) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);

  ASSERT_EQ(-EINVAL, cryptor->init_context(ctx, TEST_IV, 1));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);
}

// AuthEncDataCryptor tests: authenc(hmac(sha256),xts(aes))

const unsigned char TEST_AUTHENC_KEY[96] = {5};
const unsigned char TEST_SECTOR_IV[8] = {6};

struct TestCryptoOpensslAuthEncDataCryptor : public TestFixture {
    AuthEncDataCryptor *cryptor;

    void SetUp() override {
      TestFixture::SetUp();
      cryptor = new AuthEncDataCryptor(
          reinterpret_cast<CephContext*>(m_ioctx.cct()));
      ASSERT_EQ(0,
                cryptor->init(TEST_CIPHER_NAME, TEST_AUTHENC_KEY,
                              sizeof(TEST_AUTHENC_KEY)));
    }

    void TearDown() override {
      delete cryptor;
      TestFixture::TearDown();
    }
};

TEST_F(TestCryptoOpensslAuthEncDataCryptor, InvalidKeySize) {
  AuthEncDataCryptor bad(reinterpret_cast<CephContext*>(m_ioctx.cct()));
  EXPECT_EQ(-EINVAL, bad.init(TEST_CIPHER_NAME, TEST_KEY, sizeof(TEST_KEY)));
}

TEST_F(TestCryptoOpensslAuthEncDataCryptor, GetKeyLength) {
  ASSERT_EQ(96, cryptor->get_key_length());
}

TEST_F(TestCryptoOpensslAuthEncDataCryptor, EncryptDecrypt) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char out[sizeof(TEST_DATA)];
  unsigned char meta[48];  // HMAC tag (32) + IV (16)
  CryptArgs enc_args{TEST_DATA, out, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ((int)sizeof(TEST_DATA), cryptor->update_context(ctx, enc_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);

  // Verify ciphertext differs from plaintext
  ASSERT_NE(0, memcmp(out, TEST_DATA, sizeof(TEST_DATA)));

  // Decrypt
  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_DEC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char dec_out[sizeof(TEST_DATA)];
  CryptArgs dec_args{out, dec_out, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ((int)sizeof(TEST_DATA), cryptor->decrypt(ctx, dec_args));
  ASSERT_EQ(0, memcmp(dec_out, TEST_DATA, sizeof(TEST_DATA)));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
}

TEST_F(TestCryptoOpensslAuthEncDataCryptor, TamperedTag) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char out[sizeof(TEST_DATA)];
  unsigned char meta[48];
  CryptArgs enc_args{TEST_DATA, out, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ((int)sizeof(TEST_DATA), cryptor->update_context(ctx, enc_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);

  // Tamper with HMAC tag
  meta[0] ^= 0xFF;

  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_DEC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char dec_out[sizeof(TEST_DATA)];
  CryptArgs dec_args{out, dec_out, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ(-EBADMSG, cryptor->decrypt(ctx, dec_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
}

TEST_F(TestCryptoOpensslAuthEncDataCryptor, TamperedCiphertext) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char out[sizeof(TEST_DATA)];
  unsigned char meta[48];
  CryptArgs enc_args{TEST_DATA, out, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ((int)sizeof(TEST_DATA), cryptor->update_context(ctx, enc_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);

  // Tamper with ciphertext
  out[0] ^= 0xFF;

  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_DEC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char dec_out[sizeof(TEST_DATA)];
  CryptArgs dec_args{out, dec_out, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ(-EBADMSG, cryptor->decrypt(ctx, dec_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
}

TEST_F(TestCryptoOpensslAuthEncDataCryptor, WrongSectorIV) {
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char out[sizeof(TEST_DATA)];
  unsigned char meta[48];
  CryptArgs enc_args{TEST_DATA, out, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ((int)sizeof(TEST_DATA), cryptor->update_context(ctx, enc_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);

  // Decrypt with different sector IV (sector reassignment attack)
  unsigned char wrong_sector_iv[8] = {99};
  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_DEC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char dec_out[sizeof(TEST_DATA)];
  CryptArgs dec_args{out, dec_out, sizeof(TEST_DATA),
                     wrong_sector_iv, sizeof(wrong_sector_iv), meta, 48};
  ASSERT_EQ(-EBADMSG, cryptor->decrypt(ctx, dec_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
}

// AF_ALG kernel cross-validation helpers

static const size_t AF_ALG_AUTHSIZE = 32;  // HMAC-SHA256 tag
static const size_t AF_ALG_XTS_KEY_SIZE = 64;
static const size_t AF_ALG_HMAC_KEY_SIZE = 32;

// Build RTA-formatted key for kernel authenc:
// [rtattr hdr | enckeylen be32 | HMAC_KEY | XTS_KEY]
// librbd key layout: [XTS_KEY(64) | HMAC_KEY(32)]
// kernel expects:    [HMAC_KEY(32) | XTS_KEY(64)] after RTA header
static std::vector<unsigned char> build_rta_key(
    const unsigned char* librbd_key, size_t librbd_key_len) {
  // RTA_SPACE(sizeof(crypto_authenc_key_param)) = RTA header + 4 bytes param
  size_t rta_hdr_size = RTA_SPACE(sizeof(uint32_t));
  size_t total = rta_hdr_size + AF_ALG_HMAC_KEY_SIZE + AF_ALG_XTS_KEY_SIZE;
  std::vector<unsigned char> buf(total, 0);

  auto* rta = reinterpret_cast<struct rtattr*>(buf.data());
  rta->rta_type = 1;  // CRYPTO_AUTHENC_KEYA_PARAM
  rta->rta_len = RTA_LENGTH(sizeof(uint32_t));

  // enckeylen in big-endian
  auto* param = reinterpret_cast<uint32_t*>(RTA_DATA(rta));
  *param = htonl(AF_ALG_XTS_KEY_SIZE);

  // After RTA header: HMAC key first, then XTS key
  unsigned char* key_data = buf.data() + rta_hdr_size;
  memcpy(key_data, librbd_key + AF_ALG_XTS_KEY_SIZE, AF_ALG_HMAC_KEY_SIZE);
  memcpy(key_data + AF_ALG_HMAC_KEY_SIZE, librbd_key, AF_ALG_XTS_KEY_SIZE);

  return buf;
}

// Returns -1 if AF_ALG not available, 0 on success.
// On encrypt: out = ciphertext, tag_out = 32-byte HMAC tag
// On decrypt: out = plaintext (tag verified by kernel)
static int af_alg_authenc_crypt(
    const unsigned char* librbd_key, size_t key_len,
    const unsigned char* iv, size_t iv_len,          // 16-byte random IV (XTS tweak)
    const unsigned char* sector_iv, size_t sector_iv_len,  // 8-byte sector LE
    const unsigned char* in, size_t in_len,          // plaintext or ciphertext
    unsigned char* out, size_t out_len,
    unsigned char* tag, size_t tag_len,              // 32-byte tag (in for decrypt, out for encrypt)
    bool encrypt) {
  // Open AF_ALG AEAD socket
  int tfmfd = socket(AF_ALG, SOCK_SEQPACKET, 0);
  if (tfmfd < 0) {
    return -1;
  }

  struct sockaddr_alg sa = {};
  sa.salg_family = AF_ALG;
  strcpy(reinterpret_cast<char*>(sa.salg_type), "aead");
  strcpy(reinterpret_cast<char*>(sa.salg_name),
         "authenc(hmac(sha256),xts(aes))");

  if (bind(tfmfd, reinterpret_cast<struct sockaddr*>(&sa), sizeof(sa)) < 0) {
    close(tfmfd);
    return -1;
  }

  // Set key (RTA format)
  auto rta_key = build_rta_key(librbd_key, key_len);
  if (setsockopt(tfmfd, SOL_ALG, ALG_SET_KEY,
                 rta_key.data(), rta_key.size()) < 0) {
    close(tfmfd);
    return -1;
  }

  // Set auth tag size
  if (setsockopt(tfmfd, SOL_ALG, ALG_SET_AEAD_AUTHSIZE,
                 NULL, AF_ALG_AUTHSIZE) < 0) {
    close(tfmfd);
    return -1;
  }

  int opfd = accept(tfmfd, NULL, NULL);
  if (opfd < 0) {
    close(tfmfd);
    return -1;
  }

  // Build sendmsg with cmsg headers: operation, IV, assoclen
  // Data layout for encrypt: [AAD(24) | plaintext]
  // Data layout for decrypt: [AAD(24) | ciphertext | tag(32)]
  size_t aad_len = sector_iv_len + iv_len;  // 8 + 16 = 24
  size_t send_data_len;
  if (encrypt) {
    send_data_len = aad_len + in_len;
  } else {
    send_data_len = aad_len + in_len + tag_len;
  }

  std::vector<unsigned char> send_buf(send_data_len);
  // AAD: sector_LE || random_IV
  memcpy(send_buf.data(), sector_iv, sector_iv_len);
  memcpy(send_buf.data() + sector_iv_len, iv, iv_len);
  // Data
  memcpy(send_buf.data() + aad_len, in, in_len);
  if (!encrypt) {
    // Append tag for decryption
    memcpy(send_buf.data() + aad_len + in_len, tag, tag_len);
  }

  struct iovec iov = {};
  iov.iov_base = send_buf.data();
  iov.iov_len = send_data_len;

  // cmsg buffer for: ALG_SET_OP + ALG_SET_IV + ALG_SET_AEAD_ASSOCLEN
  size_t cmsg_iv_len = sizeof(struct af_alg_iv) + iv_len;
  size_t cbuf_len = CMSG_SPACE(sizeof(uint32_t)) +   // ALG_SET_OP
                    CMSG_SPACE(cmsg_iv_len) +         // ALG_SET_IV
                    CMSG_SPACE(sizeof(uint32_t));      // ALG_SET_AEAD_ASSOCLEN
  std::vector<char> cbuf(cbuf_len, 0);

  struct msghdr msg = {};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = cbuf.data();
  msg.msg_controllen = cbuf_len;

  // cmsg 1: ALG_SET_OP
  struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_ALG;
  cmsg->cmsg_type = ALG_SET_OP;
  cmsg->cmsg_len = CMSG_LEN(sizeof(uint32_t));
  *reinterpret_cast<uint32_t*>(CMSG_DATA(cmsg)) =
      encrypt ? ALG_OP_ENCRYPT : ALG_OP_DECRYPT;

  // cmsg 2: ALG_SET_IV
  cmsg = CMSG_NXTHDR(&msg, cmsg);
  cmsg->cmsg_level = SOL_ALG;
  cmsg->cmsg_type = ALG_SET_IV;
  cmsg->cmsg_len = CMSG_LEN(cmsg_iv_len);
  auto* aiv = reinterpret_cast<struct af_alg_iv*>(CMSG_DATA(cmsg));
  aiv->ivlen = iv_len;
  memcpy(aiv->iv, iv, iv_len);

  // cmsg 3: ALG_SET_AEAD_ASSOCLEN
  cmsg = CMSG_NXTHDR(&msg, cmsg);
  cmsg->cmsg_level = SOL_ALG;
  cmsg->cmsg_type = ALG_SET_AEAD_ASSOCLEN;
  cmsg->cmsg_len = CMSG_LEN(sizeof(uint32_t));
  *reinterpret_cast<uint32_t*>(CMSG_DATA(cmsg)) = aad_len;

  ssize_t sent = sendmsg(opfd, &msg, 0);
  if (sent < 0 || static_cast<size_t>(sent) != send_data_len) {
    close(opfd);
    close(tfmfd);
    return -1;
  }

  // Receive result
  // Encrypt output: [AAD(24) | ciphertext | tag(32)]
  // Decrypt output: [AAD(24) | plaintext]
  size_t recv_data_len;
  if (encrypt) {
    recv_data_len = aad_len + in_len + AF_ALG_AUTHSIZE;
  } else {
    recv_data_len = aad_len + in_len;
  }
  std::vector<unsigned char> recv_buf(recv_data_len);

  ssize_t received = read(opfd, recv_buf.data(), recv_data_len);
  close(opfd);
  close(tfmfd);

  if (received < 0) {
    return received;
  }
  if (static_cast<size_t>(received) != recv_data_len) {
    return -1;
  }

  // Extract output (skip AAD prefix)
  if (encrypt) {
    memcpy(out, recv_buf.data() + aad_len, in_len);
    memcpy(tag, recv_buf.data() + aad_len + in_len, AF_ALG_AUTHSIZE);
  } else {
    memcpy(out, recv_buf.data() + aad_len, in_len);
  }

  return 0;
}

// Test: encrypt with librbd, encrypt with kernel, compare ciphertext + tag
TEST_F(TestCryptoOpensslAuthEncDataCryptor, KernelCrossValidateEncrypt) {
  // 1. Encrypt with librbd
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char librbd_ct[sizeof(TEST_DATA)];
  unsigned char meta[48];  // [HMAC tag(32) | random_IV(16)]
  CryptArgs enc_args{TEST_DATA, librbd_ct, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ((int)sizeof(TEST_DATA), cryptor->update_context(ctx, enc_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);

  // Extract librbd's random IV and HMAC tag from metadata
  const unsigned char* librbd_tag = meta;        // first 32 bytes
  const unsigned char* random_iv = meta + 32;    // last 16 bytes

  // 2. Encrypt with kernel AF_ALG using the same random IV
  unsigned char kernel_ct[sizeof(TEST_DATA)];
  unsigned char kernel_tag[32];
  int rc = af_alg_authenc_crypt(
      TEST_AUTHENC_KEY, sizeof(TEST_AUTHENC_KEY),
      random_iv, 16,
      TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV),
      TEST_DATA, sizeof(TEST_DATA),
      kernel_ct, sizeof(kernel_ct),
      kernel_tag, sizeof(kernel_tag),
      true /* encrypt */);
  if (rc == -1 && (errno == EAFNOSUPPORT || errno == ENOENT)) {
    GTEST_SKIP() << "AF_ALG not available (kernel module not loaded?)";
  }
  ASSERT_EQ(0, rc) << "AF_ALG encrypt failed, errno=" << errno;

  // 3. Compare ciphertext byte-for-byte
  ASSERT_EQ(0, memcmp(librbd_ct, kernel_ct, sizeof(TEST_DATA)))
      << "Ciphertext mismatch between librbd and kernel";

  // 4. Compare HMAC tags byte-for-byte
  ASSERT_EQ(0, memcmp(librbd_tag, kernel_tag, 32))
      << "HMAC tag mismatch between librbd and kernel";
}

// Test: encrypt with one side, decrypt with the other
TEST_F(TestCryptoOpensslAuthEncDataCryptor, KernelCrossDecrypt) {
  // --- Part A: Encrypt with librbd, decrypt with kernel ---
  auto ctx = cryptor->get_context(CipherMode::CIPHER_MODE_ENC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char librbd_ct[sizeof(TEST_DATA)];
  unsigned char meta[48];
  CryptArgs enc_args{TEST_DATA, librbd_ct, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), meta, 48};
  ASSERT_EQ((int)sizeof(TEST_DATA), cryptor->update_context(ctx, enc_args));
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_ENC);

  const unsigned char* librbd_tag = meta;
  const unsigned char* random_iv = meta + 32;

  // Decrypt librbd's ciphertext with kernel
  unsigned char kernel_pt[sizeof(TEST_DATA)];
  unsigned char tag_copy[32];
  memcpy(tag_copy, librbd_tag, 32);
  int rc = af_alg_authenc_crypt(
      TEST_AUTHENC_KEY, sizeof(TEST_AUTHENC_KEY),
      random_iv, 16,
      TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV),
      librbd_ct, sizeof(TEST_DATA),
      kernel_pt, sizeof(kernel_pt),
      tag_copy, sizeof(tag_copy),
      false /* decrypt */);
  if (rc == -1 && (errno == EAFNOSUPPORT || errno == ENOENT)) {
    GTEST_SKIP() << "AF_ALG not available (kernel module not loaded?)";
  }
  ASSERT_EQ(0, rc) << "Kernel failed to decrypt librbd ciphertext, errno=" << errno;

  ASSERT_EQ(0, memcmp(kernel_pt, TEST_DATA, sizeof(TEST_DATA)))
      << "Kernel decryption of librbd ciphertext produced wrong plaintext";

  // --- Part B: Encrypt with kernel, decrypt with librbd ---
  unsigned char fixed_iv[16];
  memset(fixed_iv, 0xAB, sizeof(fixed_iv));

  unsigned char kernel_ct[sizeof(TEST_DATA)];
  unsigned char kernel_tag[32];
  rc = af_alg_authenc_crypt(
      TEST_AUTHENC_KEY, sizeof(TEST_AUTHENC_KEY),
      fixed_iv, 16,
      TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV),
      TEST_DATA, sizeof(TEST_DATA),
      kernel_ct, sizeof(kernel_ct),
      kernel_tag, sizeof(kernel_tag),
      true /* encrypt */);
  ASSERT_EQ(0, rc) << "AF_ALG encrypt failed, errno=" << errno;

  // Build meta buffer for librbd: [tag(32) | iv(16)]
  unsigned char kernel_meta[48];
  memcpy(kernel_meta, kernel_tag, 32);
  memcpy(kernel_meta + 32, fixed_iv, 16);

  ctx = cryptor->get_context(CipherMode::CIPHER_MODE_DEC);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(0, cryptor->init_context(ctx, nullptr, 0));

  unsigned char librbd_pt[sizeof(TEST_DATA)];
  CryptArgs dec_args{kernel_ct, librbd_pt, sizeof(TEST_DATA),
                     TEST_SECTOR_IV, sizeof(TEST_SECTOR_IV), kernel_meta, 48};
  ASSERT_EQ((int)sizeof(TEST_DATA), cryptor->decrypt(ctx, dec_args))
      << "librbd failed to decrypt kernel ciphertext";
  ASSERT_EQ(0, memcmp(librbd_pt, TEST_DATA, sizeof(TEST_DATA)))
      << "librbd decryption of kernel ciphertext produced wrong plaintext";
  cryptor->return_context(ctx, CipherMode::CIPHER_MODE_DEC);
}

} // namespace openssl
} // namespace crypto
} // namespace librbd
