// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <array>
#include <sstream>
#include <limits>
#include <fcntl.h>

#include <openssl/aes.h>
#include <openssl/core_names.h>

#include "Crypto.h"

#include "include/ceph_assert.h"
#include "common/Clock.h"
#include "common/armor.h"
#include "common/ceph_context.h"
#include "common/ceph_crypto.h"
#include "common/hex.h"
#include "common/safe_io.h"
#include "include/ceph_fs.h"
#include "include/compat.h"
#include "include/intarith.h" // for p2align()
#include "common/Formatter.h"
#include "common/debug.h"
#include <errno.h>

#include <boost/endian/conversion.hpp>

#define dout_subsys ceph_subsys_auth

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

using std::ostringstream;
using std::string;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::Formatter;

using boost::endian::native_to_big;


// use getentropy() if available. it uses the same source of randomness
// as /dev/urandom without the filesystem overhead
#ifdef HAVE_GETENTROPY

#include <unistd.h>

static bool getentropy_works()
{
  char buf;
  auto ret = TEMP_FAILURE_RETRY(::getentropy(&buf, sizeof(buf)));
  if (ret == 0) {
    return true;
  } else if (errno == ENOSYS || errno == EPERM) {
    return false;
  } else {
    throw std::system_error(errno, std::system_category());
  }
}

namespace TOPNSPC::auth {

CryptoRandom::CryptoRandom() : fd(getentropy_works() ? -1 : open_urandom())
{}

CryptoRandom::~CryptoRandom()
{
  if (fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }
}

void CryptoRandom::get_bytes(char *buf, int len)
{
  ssize_t ret = 0;
  if (unlikely(fd >= 0)) {
    ret = safe_read_exact(fd, buf, len);
  } else {
    // getentropy() reads up to 256 bytes
    assert(len <= 256);
    ret = TEMP_FAILURE_RETRY(::getentropy(buf, len));
  }
  if (ret < 0) {
    throw std::system_error(errno, std::system_category());
  }
}

#elif defined(_WIN32) // !HAVE_GETENTROPY

#include <bcrypt.h>

namespace TOPNSPC::auth {

CryptoRandom::CryptoRandom() : fd(0) {}
CryptoRandom::~CryptoRandom() = default;

void CryptoRandom::get_bytes(char *buf, int len)
{
  auto ret = BCryptGenRandom (
    NULL,
    (unsigned char*)buf,
    len,
    BCRYPT_USE_SYSTEM_PREFERRED_RNG);
  if (ret != 0) {
    throw std::system_error(ret, std::system_category());
  }
}

#else // !HAVE_GETENTROPY && !_WIN32
// open /dev/urandom once on construction and reuse the fd for all reads
CryptoRandom::CryptoRandom()
  : fd{open_urandom()}
{
  if (fd < 0) {
    throw std::system_error(errno, std::system_category());
  }
}

CryptoRandom::~CryptoRandom()
{
  VOID_TEMP_FAILURE_RETRY(::close(fd));
}

void CryptoRandom::get_bytes(char *buf, int len)
{
  auto ret = safe_read_exact(fd, buf, len);
  if (ret < 0) {
    throw std::system_error(-ret, std::system_category());
  }
}

#endif

int CryptoRandom::open_urandom()
{
  int fd = TEMP_FAILURE_RETRY(::open("/dev/urandom", O_CLOEXEC|O_RDONLY));
  if (fd < 0) {
    throw std::system_error(errno, std::system_category());
  }
  return fd;
}

// ---------------------------------------------------
// fallback implementation of the bufferlist-free
// interface.

std::size_t CryptoKeyHandler::encrypt_ext(
  CephContext *cct,
  const CryptoKeyHandler::in_slice_t& in,
  const CryptoKeyHandler::in_slice_t *confounder,
  const CryptoKeyHandler::out_slice_t& out) const
{
  if (out.buf == nullptr) {
    return enc_size(in, confounder);
  }

  ceph::bufferptr inptr(reinterpret_cast<const char*>(in.buf), in.length);
  ceph::bufferlist plaintext;
  plaintext.append(std::move(inptr));

  ceph::bufferlist confounder_bl;
  if (confounder) {
    ceph::bufferptr cfp(reinterpret_cast<const char*>(confounder->buf), confounder->length);
    confounder_bl.append(std::move(cfp));
  }
  ceph::bufferlist ciphertext;
  std::string error;
  const int ret = encrypt_ext(cct, plaintext, (confounder ? &confounder_bl : nullptr), ciphertext, &error);
  if (ret != 0 || !error.empty()) {
    throw std::runtime_error(std::move(error));
  }

  // we need to specify the template parameter explicitly as ::length()
  // returns unsigned int, not size_t.
  const auto todo_len = \
    std::min<std::size_t>(ciphertext.length(), out.max_length);
  memcpy(out.buf, ciphertext.c_str(), todo_len);

  return todo_len;
}

std::size_t CryptoKeyHandler::decrypt(
  CephContext *cct,
  const CryptoKeyHandler::in_slice_t& in,
  const CryptoKeyHandler::out_slice_t& out) const
{
  ceph::bufferptr inptr(reinterpret_cast<const char*>(in.buf), in.length);
  ceph::bufferlist ciphertext;
  ciphertext.append(std::move(inptr));

  ceph::bufferlist plaintext;
  std::string error;
  const int ret = decrypt(cct, ciphertext, plaintext, &error);
  if (ret != 0 || !error.empty()) {
    throw std::runtime_error(std::move(error));
  }

  // we need to specify the template parameter explicitly as ::length()
  // returns unsigned int, not size_t.
  const auto todo_len = \
    std::min<std::size_t>(plaintext.length(), out.max_length);
  memcpy(out.buf, plaintext.c_str(), todo_len);

  return todo_len;
}

sha256_digest_t CryptoKeyHandler::hmac_sha256(
  const ceph::bufferlist& in) const
{
  TOPNSPC::crypto::HMACSHA256 hmac((const unsigned char*)secret.c_str(), secret.length());

  for (const auto& bptr : in.buffers()) {
    hmac.Update((const unsigned char *)bptr.c_str(), bptr.length());
  }
  sha256_digest_t ret;
  hmac.Final(ret.v);

  return ret;
}

sha256_digest_t CryptoKeyHandler::hmac_sha256(
  const in_slice_t& in) const
{
  TOPNSPC::crypto::HMACSHA256 hmac((const unsigned char*)secret.c_str(), secret.length());

  hmac.Update(in.buf, in.length);

  sha256_digest_t ret;
  hmac.Final(ret.v);

  return ret;
}

// ---------------------------------------------------

class CryptoNoneKeyHandler : public CryptoKeyHandler {
public:
  CryptoNoneKeyHandler()
    : CryptoKeyHandler(CryptoKeyHandler::BLOCK_SIZE_0B()) {
  }

  using CryptoKeyHandler::encrypt_ext;
  using CryptoKeyHandler::decrypt;

  int encrypt_ext(CephContext *cct, const bufferlist& in, const bufferlist *confounder,
                  bufferlist& out, std::string *error) const override {
    out = in;
    return 0;
  }
  int decrypt(CephContext *cct, const bufferlist& in,
	      bufferlist& out, std::string *error) const override {
    out = in;
    return 0;
  }
};

class CryptoNone : public CryptoHandler {
public:
  CryptoNone() { }
  ~CryptoNone() override {}
  int get_type() const override {
    return CEPH_CRYPTO_NONE;
  }
  int create(CryptoRandom *random, bufferptr& secret) override {
    return 0;
  }
  int validate_secret(const bufferptr& secret) override {
    return 0;
  }
  CryptoKeyHandler *get_key_handler_ext(const bufferptr& secret, uint32_t usage, string& error) override {
    return new CryptoNoneKeyHandler;
  }
};


// ---------------------------------------------------


class CryptoAES : public CryptoHandler {
public:
  CryptoAES() { }
  ~CryptoAES() override {}
  int get_type() const override {
    return CEPH_CRYPTO_AES;
  }
  int create(CryptoRandom *random, bufferptr& secret) override;
  int validate_secret(const bufferptr& secret) override;
  CryptoKeyHandler *get_key_handler_ext(const bufferptr& secret, uint32_t usage /* unused */, string& error) override;
};

// when we say AES, we mean AES-128
static constexpr const std::size_t AES_KEY_LEN{16};
static constexpr const std::size_t AES_BLOCK_LEN{16};

class CryptoAESKeyHandler : public CryptoKeyHandler {
  AES_KEY enc_key;
  AES_KEY dec_key;

public:
  CryptoAESKeyHandler()
    : CryptoKeyHandler(CryptoKeyHandler::BLOCK_SIZE_16B()) {
  }

  using CryptoKeyHandler::encrypt;
  using CryptoKeyHandler::encrypt_ext;

  int init(const bufferptr& s, ostringstream& err) {
    secret = s;

    const int enc_key_ret = \
      AES_set_encrypt_key((const unsigned char*)secret.c_str(),
			  AES_KEY_LEN * CHAR_BIT, &enc_key);
    if (enc_key_ret != 0) {
      err << "cannot set OpenSSL encrypt key for AES: " << enc_key_ret;
      return -1;
    }

    const int dec_key_ret = \
      AES_set_decrypt_key((const unsigned char*)secret.c_str(),
			  AES_KEY_LEN * CHAR_BIT, &dec_key);
    if (dec_key_ret != 0) {
      err << "cannot set OpenSSL decrypt key for AES: " << dec_key_ret;
      return -1;
    }

    return 0;
  }

  int encrypt_ext(CephContext *cct, const ceph::bufferlist& in,
                  const bufferlist *confounder /* ignored */,
	      ceph::bufferlist& out,
              std::string* /* unused */) const override {
    ldout(cct, 20) << "CryptoAESKeyHandler::encrypt()" << dendl;
    // we need to take into account the PKCS#7 padding. There *always* will
    // be at least one byte of padding. This stays even to input aligned to
    // AES_BLOCK_LEN. Otherwise we would face ambiguities during decryption.
    // To exemplify:
    //   16 + p2align(10, 16) -> 16
    //   16 + p2align(16, 16) -> 32 including 16 bytes for padding.
    ceph::bufferptr out_tmp{static_cast<unsigned>(
      AES_BLOCK_LEN + p2align<std::size_t>(in.length(), AES_BLOCK_LEN))};

    // let's pad the data
    std::uint8_t pad_len = out_tmp.length() - in.length();
    ceph::bufferptr pad_buf{pad_len};
    // FIPS zeroization audit 20191115: this memset is not intended to
    // wipe out a secret after use.
    memset(pad_buf.c_str(), pad_len, pad_len);

    // form contiguous buffer for block cipher. The ctor copies shallowly.
    ceph::bufferlist incopy(in);
    incopy.append(std::move(pad_buf));
    const auto in_buf = reinterpret_cast<unsigned char*>(incopy.c_str());

    // reinitialize IV each time. It might be unnecessary depending on
    // actual implementation but at the interface layer we are obliged
    // to deliver IV as non-const.
    static_assert(strlen_ct(CEPH_AES_IV) == AES_BLOCK_LEN);
    unsigned char iv[AES_BLOCK_LEN];
    memcpy(iv, CEPH_AES_IV, AES_BLOCK_LEN);

    // we aren't using EVP because of performance concerns. Profiling
    // shows the cost is quite high. Endianness might be an issue.
    // However, as they would affect Cephx, any fallout should pop up
    // rather early, hopefully.
    AES_cbc_encrypt(in_buf, reinterpret_cast<unsigned char*>(out_tmp.c_str()),
		    out_tmp.length(), &enc_key, iv, AES_ENCRYPT);

    out.append(out_tmp);
    return 0;
  }

  int decrypt(CephContext *cct, const ceph::bufferlist& in,
	      ceph::bufferlist& out,
	      std::string* /* unused */) const override {
    ldout(cct, 20) << "CryptoAESKeyHandler::decrypt()" << dendl;
    // PKCS#7 padding enlarges even empty plain-text to take 16 bytes.
    if (in.length() < AES_BLOCK_LEN || in.length() % AES_BLOCK_LEN) {
      return -1;
    }

    // needed because of .c_str() on const. It's a shallow copy.
    ceph::bufferlist incopy(in);
    const auto in_buf = reinterpret_cast<unsigned char*>(incopy.c_str());

    // make a local, modifiable copy of IV.
    static_assert(strlen_ct(CEPH_AES_IV) == AES_BLOCK_LEN);
    unsigned char iv[AES_BLOCK_LEN];
    memcpy(iv, CEPH_AES_IV, AES_BLOCK_LEN);

    ceph::bufferptr out_tmp{in.length()};
    AES_cbc_encrypt(in_buf, reinterpret_cast<unsigned char*>(out_tmp.c_str()),
		    in.length(), &dec_key, iv, AES_DECRYPT);

    // BE CAREFUL: we cannot expose any single bit of information about
    // the cause of failure. Otherwise we'll face padding oracle attack.
    // See: https://en.wikipedia.org/wiki/Padding_oracle_attack.
    const auto pad_len = \
      std::min<std::uint8_t>(out_tmp[in.length() - 1], AES_BLOCK_LEN);
    out_tmp.set_length(in.length() - pad_len);
    out.append(std::move(out_tmp));

    return 0;
  }

  std::size_t enc_size(const in_slice_t& in,
                       const in_slice_t *confounder) const override {
    // 16 + p2align(10, 16) -> 16
    // 16 + p2align(16, 16) -> 32
    return AES_BLOCK_LEN + p2align<std::size_t>(in.length, AES_BLOCK_LEN);
  }

  std::size_t encrypt(CephContext *cct, const in_slice_t& in,
		      const out_slice_t& out) const override {
    if (out.buf == nullptr) {
      return enc_size(in, nullptr);
    }

    // how many bytes of in.buf hang outside the alignment boundary and how
    // much padding we need.
    //   length = 23 -> tail_len = 7, pad_len = 9
    //   length = 32 -> tail_len = 0, pad_len = 16
    const std::uint8_t tail_len = in.length % AES_BLOCK_LEN;
    const std::uint8_t pad_len = AES_BLOCK_LEN - tail_len;
    static_assert(std::numeric_limits<std::uint8_t>::max() > AES_BLOCK_LEN);

    std::array<unsigned char, AES_BLOCK_LEN> last_block;
    memcpy(last_block.data(), in.buf + in.length - tail_len, tail_len);
    // FIPS zeroization audit 20191115: this memset is not intended to
    // wipe out a secret after use.
    memset(last_block.data() + tail_len, pad_len, pad_len);

    // need a local copy because AES_cbc_encrypt takes `iv` as non-const.
    // Useful because it allows us to encrypt in two steps: main + tail.
    static_assert(strlen_ct(CEPH_AES_IV) == AES_BLOCK_LEN);
    std::array<unsigned char, AES_BLOCK_LEN> iv;
    memcpy(iv.data(), CEPH_AES_IV, AES_BLOCK_LEN);

    const std::size_t main_encrypt_size = \
      std::min(in.length - tail_len, out.max_length);
    AES_cbc_encrypt(in.buf, out.buf, main_encrypt_size, &enc_key, iv.data(),
		    AES_ENCRYPT);

    const std::size_t tail_encrypt_size = \
      std::min(AES_BLOCK_LEN, out.max_length - main_encrypt_size);
    AES_cbc_encrypt(last_block.data(), out.buf + main_encrypt_size,
		    tail_encrypt_size, &enc_key, iv.data(), AES_ENCRYPT);

    return main_encrypt_size + tail_encrypt_size;
  }

  std::size_t decrypt(CephContext *cct, const in_slice_t& in,
		      const out_slice_t& out) const override {
    if (in.length % AES_BLOCK_LEN != 0 || in.length < AES_BLOCK_LEN) {
      throw std::runtime_error("input not aligned to AES_BLOCK_LEN");
    } else if (out.buf == nullptr) {
      // essentially it would be possible to decrypt into a buffer that
      // doesn't include space for any PKCS#7 padding. We don't do that
      // for the sake of performance and simplicity.
      return in.length;
    } else if (out.max_length < in.length) {
      throw std::runtime_error("output buffer too small");
    }

    static_assert(strlen_ct(CEPH_AES_IV) == AES_BLOCK_LEN);
    std::array<unsigned char, AES_BLOCK_LEN> iv;
    memcpy(iv.data(), CEPH_AES_IV, AES_BLOCK_LEN);

    AES_cbc_encrypt(in.buf, out.buf, in.length, &dec_key, iv.data(),
		    AES_DECRYPT);

    // NOTE: we aren't handling partial decrypt. PKCS#7 padding must be
    // at the end. If it's malformed, don't say a word to avoid risk of
    // having an oracle. All we need to ensure is valid buffer boundary.
    const auto pad_len = \
      std::min<std::uint8_t>(out.buf[in.length - 1], AES_BLOCK_LEN);
    return in.length - pad_len;
  }
};


// ------------------------------------------------------------

int CryptoAES::create(CryptoRandom *random, bufferptr& secret)
{
  bufferptr buf(AES_KEY_LEN);
  random->get_bytes(buf.c_str(), buf.length());
  secret = std::move(buf);
  return 0;
}

int CryptoAES::validate_secret(const bufferptr& secret)
{
  if (secret.length() < AES_KEY_LEN) {
    return -EINVAL;
  }

  return 0;
}

CryptoKeyHandler *CryptoAES::get_key_handler_ext(const bufferptr& secret,
                                                 uint32_t usage,
                                                 string& error)
{
  CryptoAESKeyHandler *ckh = new CryptoAESKeyHandler;
  ostringstream oss;
  if (ckh->init(secret, oss) < 0) {
    error = oss.str();
    delete ckh;
    return NULL;
  }
  return ckh;
}


// ---------------------------------------------------

/*
 * AES256CTS-HMAC384-192
 */
class CryptoAES256KRB5 : public CryptoHandler {
public:
  CryptoAES256KRB5() { }
  ~CryptoAES256KRB5() override {}
  int get_type() const override {
    return CEPH_CRYPTO_AES256KRB5;
  }
  int create(CryptoRandom *random, bufferptr& secret) override;
  int validate_secret(const bufferptr& secret) override;
  CryptoKeyHandler *get_key_handler_ext(const bufferptr& secret, uint32_t usage, string& error) override;
};

static constexpr const std::size_t AES256KRB5_KEY_LEN{32};
static constexpr const std::size_t AES256KRB5_BLOCK_LEN{16};
static constexpr const std::size_t AES256KRB5_HASH_LEN{24};
static constexpr const std::size_t SHA384_LEN{48};

class CryptoAES256KRB5KeyHandler : public CryptoKeyHandler {
  EVP_CIPHER *cipher{nullptr};

  ceph::bufferlist ki;
  const unsigned char *ki_raw;
  ceph::bufferlist ke;
  const unsigned char *ke_raw;

static void dump_buf(CephContext *cct, string title, const unsigned char *buf, int len)
{
  std::stringstream ss;
  ss << std::endl << title << std::endl;
  for (int i = 0; i < len; ++i) {
    if (i != 0 && i % 16 == 0) {
      ss << std::endl;
    }
    ss << fmt::format("{:02x} ", buf[i]);
  }
  ss << std::endl;
  ldout(cct, 0) << ss.str() << dendl;
}
    
  int calc_hmac_sha384(const unsigned char *data, 
                       int data_len,
                       const unsigned char* hmac_key,
                       int key_size,
                       const unsigned char *iv,
                       int iv_size,
                       char *out,
                       int out_size,
                       ostringstream& err) const {
    unsigned int len = 0;
    char _out[SHA384_LEN];
    char *pout;
    bool need_trim = (out_size < (int)sizeof(_out));
    if (need_trim) {
      pout = _out;
    } else {
      pout = out;
    }

    /* IV is prepended to the plaintext */
    ceph::bufferptr iv_buf(reinterpret_cast<const char *>(iv), iv_size);
    ceph::bufferlist source;
    source.push_back(iv_buf);
    source.append((const char *)data, data_len);

    HMAC(EVP_sha384(), hmac_key, key_size,
         reinterpret_cast<const unsigned char *>(source.c_str()), source.length(),
         (unsigned char *)pout, &len);

    if (len != SHA384_LEN) {
      err << "Unexpected calculated SHA384 length";
      return -EIO;
    }

    if (need_trim) {
      memcpy(out, pout, out_size);
      len = out_size;
    }

    return len;
  }

  int calc_kx(const ceph::bufferptr& secret,
              uint32_t usage,
              uint8_t type,
              int k,
              ceph::bufferlist& out,
              ostringstream& err) {

    struct plain_data {
      unsigned char prefix[4] = { 0, 0, 0, 1 };
      uint32_t usage;
      uint8_t type;
      uint8_t c = 0;
      uint32_t k;

      plain_data(uint32_t _usage, uint8_t _type, uint32_t _k) :
        usage(native_to_big<uint32_t>(_usage)),
        type(_type),
        k(native_to_big<uint32_t>(_k * 8)) {}
    } __attribute__((packed)) data(usage, type, k);

    ceph::bufferptr bp(reinterpret_cast<char *>(&data), sizeof(data));

    ceph::bufferptr sha384(SHA384_LEN);
    int r = calc_hmac_sha384((const unsigned char *)bp.c_str(), bp.length(),
                             reinterpret_cast<const unsigned char *>(secret.c_str()),
                             secret.length(),
                             nullptr, 0, /* no IV */
                             sha384.c_str(),
                             SHA384_LEN,
                             err);

    bufferlist bl;
    bl.append(sha384);
    bl.splice(0, k, &out);

    return r;
  }

  int encrypt_AES256_CTS(CephContext *cct,
                         ceph::bufferlist& plaintext,
                         const unsigned char* iv, int iv_size,
                         unsigned char *ciphertext,
                         int ciphertext_len) const {
    if (!cipher) {
      return -EINVAL; /* initialization error */
    }

    if ((size_t)ciphertext_len < plaintext.length()) {
      return -EINVAL;
    }

    OSSL_PARAM params[2] = { OSSL_PARAM_construct_utf8_string(OSSL_CIPHER_PARAM_CTS_MODE, (char *)"CS3", 0),
      OSSL_PARAM_construct_end()};

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
      ldout(cct, 20) << "EVP_CIPHER_CTX_new() returned null" << dendl;
      return -EIO;
    }

    if (!EVP_EncryptInit_ex2(ctx, cipher, ke_raw, iv, params)) {
      ldout(cct, 20) << "EVP_EncryptInit() failed" << dendl;
      return -EIO;
    }

    int encrypted_len = 0;
    int len;

    auto ret = EVP_EncryptUpdate(ctx, ciphertext + encrypted_len, &len, (const unsigned char *)plaintext.c_str(), plaintext.length());
    if (ret != 1) {
      ldout(cct, 20) << "EVP_EncryptUpdate(len=" << plaintext.length() << ") returned " << ret << dendl;
      return -EIO;
    }
    encrypted_len += len;

    ret = EVP_EncryptFinal_ex(ctx, ciphertext + encrypted_len, &len);
    if (ret != 1) {
      ldout(cct, 20) << "EVP_EncryptFinal_ex() returned " << ret << dendl;
      return -EIO;
    }
    encrypted_len += len;

    EVP_CIPHER_CTX_free(ctx);

    return encrypted_len;
  }

  int decrypt_AES256_CTS(ceph::bufferlist& ciphertext, 
                         const unsigned char* key, const unsigned char* iv, 
                         int iv_size,
                         ceph::bufferptr& plaintext) const {
    if (!cipher) {
      return -EINVAL; /* initialization error really */
    }

    OSSL_PARAM params[2] = { OSSL_PARAM_construct_utf8_string(OSSL_CIPHER_PARAM_CTS_MODE, (char *)"CS3", 0),
      OSSL_PARAM_construct_end()};

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
      return -EIO;
    }

    if (!EVP_DecryptInit_ex2(ctx, cipher, key, iv, params)) {
      return -EIO;
    }

    int len;
    auto dest = reinterpret_cast<unsigned char *>(plaintext.c_str());
    int plaintext_len = 0;

    int max = ciphertext.length();
    auto iter = ciphertext.cbegin();
    while (!iter.end()) {
      const char *p;
      int chunk_len = iter.get_ptr_and_advance(max, &p);

      if (EVP_DecryptUpdate(ctx, dest + plaintext_len, &len, (const unsigned char *)p, chunk_len) != 1) {
        return -EIO;
      }
      plaintext_len += len;
    }

    if (EVP_DecryptFinal_ex(ctx, dest + plaintext_len, &len) != 1) {
      return -EIO;
    }
    plaintext_len += len;

    EVP_CIPHER_CTX_free(ctx);

    return 0;
  }

public:
  CryptoAES256KRB5KeyHandler() : CryptoKeyHandler(CryptoKeyHandler::BLOCK_SIZE_16B()) {
  }

  using CryptoKeyHandler::encrypt_ext;
  using CryptoKeyHandler::decrypt;
  using CryptoKeyHandler::encrypt;

  int init(const ceph::bufferptr& s, uint32_t usage, ostringstream& err) {
    cipher = EVP_CIPHER_fetch(NULL, "AES-256-CBC-CTS", NULL);
    secret = s;

    int r = calc_kx(secret, usage,
                    0x55 /* Ki type */,
                    AES256KRB5_HASH_LEN /* 192 bit */,
                    ki,
                    err);
    if (r < 0) {
      return r;
    }
    ki_raw = reinterpret_cast<const unsigned char *>(ki.c_str()); /* needed so that we can use ki in const methods */

    r = calc_kx(secret, usage,
                0xAA /* Ke type */,
                32 /* 256 bit */,
                ke,
                err);
    if (r < 0) {
      return r;
    }
    ke_raw = reinterpret_cast<const unsigned char *>(ke.c_str()); /* same reason as with ki */

    return 0;
  }

  int encrypt_ext(CephContext *cct, const ceph::bufferlist& in,
                  const ceph::bufferlist *confounder,
                  ceph::bufferlist& out,
                  std::string* /* unused */) const override {
    ldout(cct, 20) << "CryptoAES256KRB5KeyHandler::encrypt()" << dendl;
    // encrypted (confounder | data) | hash
    ceph::bufferptr out_tmp{static_cast<unsigned>(
      AES256KRB5_BLOCK_LEN + in.length() + AES256KRB5_HASH_LEN)};

    /* encrypted (confounder data) */
    char *aes_enc = out_tmp.c_str();
    int aes_enc_len = AES256KRB5_BLOCK_LEN + in.length();

    ceph::bufferlist incopy;
    bufferptr confounder_buf(AES256KRB5_BLOCK_LEN);

    if (!confounder) {
      cct->random()->get_bytes(confounder_buf.c_str(), confounder_buf.length());
      incopy.append(confounder_buf);
    } else {
      if (confounder->length() != AES256KRB5_BLOCK_LEN) {
        ldout(cct, 0) << "ERROR: confounder length is expected to be equal to block size (" << AES256KRB5_BLOCK_LEN << ")" << dendl;
        return -EINVAL;
      }
      incopy.append(*confounder);
    }

    // combine confounder with input data
    incopy.append(in);

    // reinitialize IV each time. It might be unnecessary depending on
    // actual implementation but at the interface layer we are obliged
    // to deliver IV as non-const.
    static_assert(strlen_ct(CEPH_AES_IV) == AES256KRB5_BLOCK_LEN);
    unsigned char iv[AES_BLOCK_LEN];
    memset(iv, 0, sizeof(iv));

    int r = encrypt_AES256_CTS(cct, incopy, iv, sizeof(iv), (unsigned char *)aes_enc, aes_enc_len);
    if (r < 0) {
      return r;
    }
    aes_enc_len = r;

    char *hmac = out_tmp.c_str() + AES256KRB5_BLOCK_LEN + in.length();

    ostringstream err;
    r = calc_hmac_sha384((const unsigned char *)aes_enc, aes_enc_len, 
                         ki_raw, ki.length(),
                         iv, sizeof(iv),
                         hmac, AES256KRB5_HASH_LEN, err);
    if (r < 0) {
      return r;
    }

    out.append(out_tmp);
    return 0;
  }

  int decrypt(CephContext *cct, const ceph::bufferlist& in,
	      ceph::bufferlist& out,
	      std::string* /* unused */) const override {

    ldout(cct, 20) << "CryptoAES256KRB5KeyHandler::decrypt()" << dendl;
    if (in.length() < AES256KRB5_BLOCK_LEN + AES256KRB5_HASH_LEN) { /* minimum size: confounder + hmac */
      return -EINVAL;
    }

    // needed because of .c_str() on const. It's a shallow copy.
    bufferlist incopy(in);

    ceph::bufferlist indata;

    /* after this:
     * indata holds: encrypted (confounder | plaintext)
     * incopy holds: hmac hash of indata
     */
    incopy.splice(0, in.length() - AES256KRB5_HASH_LEN, &indata);

    auto& inhash = incopy;

    // make a local, modifiable copy of IV.
    static_assert(strlen_ct(CEPH_AES_IV) == AES_BLOCK_LEN);
    unsigned char iv[AES_BLOCK_LEN];
    memset(iv, 0, sizeof(iv));


    /* first need to compare hmac to calculated hmac */
    char hmac[AES256KRB5_HASH_LEN];
    ostringstream err;
    int r = calc_hmac_sha384((const unsigned char *)indata.c_str(), indata.length(),
                             ki_raw, ki.length(),
                             iv, sizeof(iv),
                             hmac, sizeof(hmac), err);
    if (r < 0) {
      return r;
    }

    int len = r;

    if ((size_t)len != inhash.length()) {
      return -EPERM;
    }

    if (memcmp(hmac, inhash.c_str(), sizeof(hmac)) != 0) {
      return -EPERM;
    }

    /* will consist of confounder | plaintext */
    bufferptr tmp_out(indata.length());

    r = decrypt_AES256_CTS(indata, 
                           ke_raw, iv, sizeof(iv),
                           tmp_out);
    if (r < 0) {
      return r;
    }

    auto confounder_len = AES256KRB5_BLOCK_LEN;

    if (tmp_out.length() < confounder_len) {
      /* should at least consist of the confounder */
      return -EPERM;
    }

    int data_len = tmp_out.length() - AES256KRB5_BLOCK_LEN;

    out.append(tmp_out.c_str() + AES256KRB5_BLOCK_LEN, data_len);

    return 0;
  }

  int encrypt(CephContext *cct, const ceph::bufferlist& in,
	      ceph::bufferlist& out,
              std::string* unused) const override {
    return encrypt_ext(cct, in, nullptr,  out, unused);
  }

  std::size_t enc_size(const in_slice_t& in,
                       const in_slice_t *confounder) const override {
    return 24 + (confounder ? confounder->length : 0 ) + in.length;
  }
};


// ------------------------------------------------------------

int CryptoAES256KRB5::create(CryptoRandom *random, bufferptr& secret)
{
  bufferptr buf(AES256KRB5_KEY_LEN);
  random->get_bytes(buf.c_str(), buf.length());
  secret = std::move(buf);
  return 0;
}

int CryptoAES256KRB5::validate_secret(const bufferptr& secret)
{
  if (secret.length() < AES256KRB5_KEY_LEN) {
    return -EINVAL;
  }

  return 0;
}

CryptoKeyHandler *CryptoAES256KRB5::get_key_handler_ext(const bufferptr& secret,
                                                        uint32_t usage,
                                                        string& error)
{
  CryptoAES256KRB5KeyHandler *ckh = new CryptoAES256KRB5KeyHandler;
  ostringstream oss;
  if (ckh->init(secret, usage, oss) < 0) {
    error = oss.str();
    delete ckh;
    return NULL;
  }
  return ckh;
}




// --


// ---------------------------------------------------


void CryptoKey::encode(bufferlist& bl) const
{
  using ceph::encode;
  encode(type, bl);
  encode(created, bl);
  __u16 len = secret.length();
  encode(len, bl);
  bl.append(secret);
}

void CryptoKey::decode(bufferlist::const_iterator& bl)
{
  using ceph::decode;
  decode(type, bl);
  decode(created, bl);
  __u16 len;
  decode(len, bl);
  bufferptr tmp;
  bl.copy_deep(len, tmp);
  if (_set_secret(type, tmp) < 0)
    throw ceph::buffer::malformed_input("malformed secret");
}

void CryptoKey::dump(Formatter *f) const
{
  f->dump_int("type", type);
  f->dump_stream("created") << created;
  f->dump_int("secret.length", secret.length());
}

std::list<CryptoKey> CryptoKey::generate_test_instances()
{
  std::list<CryptoKey> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().type = CEPH_CRYPTO_AES;
  ls.back().set_secret(
    CEPH_CRYPTO_AES, bufferptr("1234567890123456", 16), utime_t(123, 456));
  ls.back().created = utime_t(123, 456);
  return ls;
}

int CryptoKey::set_secret(int type, const bufferptr& s, utime_t c)
{
  int r = _set_secret(type, s);
  if (r < 0)
    return r;
  this->created = c;
  return 0;
}

int CryptoKey::_set_secret(int t, const bufferptr& s)
{
  if (s.length() == 0) {
    secret = s;
    ckh.reset();
    return 0;
  }

  CryptoHandler *ch = CryptoHandler::create(t);
  if (ch) {
    int ret = ch->validate_secret(s);
    if (ret < 0) {
      delete ch;
      return ret;
    }
    string error;
    ckh.reset(ch->get_key_handler(s, error));
    delete ch;
    if (error.length()) {
      return -EIO;
    }
  } else {
      return -EOPNOTSUPP;
  }
  type = t;
  secret = s;
  return 0;
}

int CryptoKey::create(CephContext *cct, int t)
{
  CryptoHandler *ch = CryptoHandler::create(t);
  if (!ch) {
    if (cct)
      lderr(cct) << "ERROR: cct->get_crypto_handler(type=" << t << ") returned NULL" << dendl;
    return -EOPNOTSUPP;
  }
  bufferptr s;
  int r = ch->create(cct->random(), s);
  delete ch;
  if (r < 0)
    return r;

  r = _set_secret(t, s);
  if (r < 0)
    return r;
  created = ceph_clock_now();
  return r;
}

void CryptoKey::print(std::ostream &out) const
{
  out << encode_base64();
}

void CryptoKey::to_str(std::string& s) const
{
  int len = secret.length() * 4;
  char buf[len];
  hex2str(secret.c_str(), secret.length(), buf, len);
  s = buf;
}

void CryptoKey::encode_formatted(string label, Formatter *f, bufferlist &bl)
{
  f->open_object_section(label.c_str());
  f->dump_string("key", encode_base64());
  f->close_section();
  f->flush(bl);
}

void CryptoKey::encode_plaintext(bufferlist &bl)
{
  bl.append(encode_base64());
}


// ------------------

CryptoHandler *CryptoHandler::create(int type)
{
  switch (type) {
  case CEPH_CRYPTO_NONE:
    return new CryptoNone;
  case CEPH_CRYPTO_AES:
    return new CryptoAES;
  case CEPH_CRYPTO_AES256KRB5:
    return new CryptoAES256KRB5;
  default:
    return NULL;
  }
}

CryptoManager::CryptoManager(CephContext *_cct) : cct(_cct) {
  crypto_none.reset(CryptoHandler::create(CEPH_CRYPTO_NONE));
  crypto_aes.reset(CryptoHandler::create(CEPH_CRYPTO_AES));
  crypto_aes256krb5.reset(CryptoHandler::create(CEPH_CRYPTO_AES256KRB5));

  supported_crypto_types = { CEPH_CRYPTO_NONE, CEPH_CRYPTO_AES, CEPH_CRYPTO_AES256KRB5 };
}

std::shared_ptr<CryptoHandler> CryptoManager::get_handler(int type)
{
  switch (type) {
    case CEPH_CRYPTO_NONE:
      return crypto_none;
    case CEPH_CRYPTO_AES:
      return crypto_aes;
    case CEPH_CRYPTO_AES256KRB5:
      return crypto_aes256krb5;
    default:
      break;
  };
  return nullptr;
}

int CryptoManager::get_key_type(const std::string& s)
{
  auto l = s;
  std::transform(l.begin(), l.end(), l.begin(), ::tolower);
  if (l == "recommended") {
    return CEPH_CRYPTO_AES256KRB5;
  }
  if (l == "aes") {
    return CEPH_CRYPTO_AES;
  }
  if (l == "aes256k") {
    return CEPH_CRYPTO_AES256KRB5;
  }
  if (l == "none") {
    return CEPH_CRYPTO_NONE;
  }
  return -ENOENT;
}

const std::set<int>& CryptoManager::get_secure_key_types()
{
  static const std::set<int> secure_keys{CEPH_CRYPTO_AES256KRB5};
  return secure_keys;
}

bool CryptoManager::crypto_type_supported(int type) const
{
  return supported_crypto_types.find(type) != supported_crypto_types.end();
}

} // namespace TOPNSPC::auth

#pragma clang diagnostic pop
#pragma GCC diagnostic pop
