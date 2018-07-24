// vim: ts=8 sw=2 smarttab
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

#include "Crypto.h"
#ifdef USE_OPENSSL
# include <openssl/evp.h>
#endif

#include "include/ceph_assert.h"
#include "common/Clock.h"
#include "common/armor.h"
#include "common/ceph_context.h"
#include "common/ceph_crypto.h"
#include "common/hex.h"
#include "common/safe_io.h"
#include "include/ceph_fs.h"
#include "include/compat.h"
#include "common/Formatter.h"
#include "common/debug.h"
#include <errno.h>

// use getentropy() if available. it uses the same source of randomness
// as /dev/urandom without the filesystem overhead
#ifdef HAVE_GETENTROPY

#include <unistd.h>

CryptoRandom::CryptoRandom() : fd(0) {}
CryptoRandom::~CryptoRandom() = default;

void CryptoRandom::get_bytes(char *buf, int len)
{
  auto ret = TEMP_FAILURE_RETRY(::getentropy(buf, len));
  if (ret < 0) {
    throw std::system_error(errno, std::system_category());
  }
}

#else // !HAVE_GETENTROPY

// open /dev/urandom once on construction and reuse the fd for all reads
CryptoRandom::CryptoRandom()
  : fd(TEMP_FAILURE_RETRY(::open("/dev/urandom", O_CLOEXEC|O_RDONLY)))
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


// ---------------------------------------------------
// fallback implementation of the bufferlist-free
// interface.

std::size_t CryptoKeyHandler::encrypt(
  const CryptoKeyHandler::in_slice_t& in,
  const CryptoKeyHandler::out_slice_t& out) const
{
  ceph::bufferptr inptr(reinterpret_cast<const char*>(in.buf), in.length);
  ceph::bufferlist plaintext;
  plaintext.append(std::move(inptr));

  ceph::bufferlist ciphertext;
  std::string error;
  const int ret = encrypt(plaintext, ciphertext, &error);
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
  const CryptoKeyHandler::in_slice_t& in,
  const CryptoKeyHandler::out_slice_t& out) const
{
  ceph::bufferptr inptr(reinterpret_cast<const char*>(in.buf), in.length);
  ceph::bufferlist ciphertext;
  ciphertext.append(std::move(inptr));

  ceph::bufferlist plaintext;
  std::string error;
  const int ret = decrypt(ciphertext, plaintext, &error);
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

// ---------------------------------------------------

class CryptoNoneKeyHandler : public CryptoKeyHandler {
public:
  CryptoNoneKeyHandler()
    : CryptoKeyHandler(CryptoKeyHandler::BLOCK_SIZE_0B()) {
  }

  using CryptoKeyHandler::encrypt;
  using CryptoKeyHandler::decrypt;

  int encrypt(const bufferlist& in,
	       bufferlist& out, std::string *error) const override {
    out = in;
    return 0;
  }
  int decrypt(const bufferlist& in,
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
  CryptoKeyHandler *get_key_handler(const bufferptr& secret, string& error) override {
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
  CryptoKeyHandler *get_key_handler(const bufferptr& secret, string& error) override;
};

#ifdef USE_OPENSSL
// when we say AES, we mean AES-128
static constexpr const std::size_t AES_KEY_LEN{16};
static constexpr const std::size_t AES_BLOCK_LEN{16};

// private functions wrapping evp code
std::size_t encrypt_aes_evp(const CryptoKeyHandler::in_slice_t& in,
                              const CryptoKeyHandler::out_slice_t& out,
                              const unsigned char* key)
 {
  if (out.buf == nullptr) {
    // 16 + p2align(10, 16) -> 16
    // 16 + p2align(16, 16) -> 32
    const std::size_t needed = \
      AES_BLOCK_LEN + p2align(in.length, AES_BLOCK_LEN);
    return needed;
  }

  // how many bytes of in.buf hang outside the alignment boundary and how
  // much padding we need.
  //   length = 23 -> tail_len = 7, pad_len = 9
  //   length = 32 -> tail_len = 0, pad_len = 16
  const std::uint8_t tail_len = in.length % AES_BLOCK_LEN;
  const std::uint8_t pad_len = AES_BLOCK_LEN - tail_len;
  static_assert(std::numeric_limits<std::uint8_t>::max() > AES_BLOCK_LEN);
  static_assert(strlen_ct(CEPH_AES_IV) == AES_BLOCK_LEN);

  std::size_t encrypt_size = in.length + pad_len;

  // crash if user specified too small buffer
  assert(encrypt_size <= out.max_length);

  int outl = out.max_length;

  // copy all data from in.buf to out.buf so we can do encrypt in one step
  maybe_inline_memcpy(out.buf, in.buf, in.length, 64);
  memset(out.buf + in.length, pad_len, pad_len);

  EVP_CIPHER_CTX* ectx = EVP_CIPHER_CTX_new();
  assert(ectx);

  if (!EVP_EncryptInit_ex(ectx, EVP_aes_128_cbc(), NULL, key, (const unsigned char*)CEPH_AES_IV)) {
      encrypt_size = 0;
      goto fallback;
  }

  // we don't want OpenSSL to do the padding, it's slower
  EVP_CIPHER_CTX_set_padding(ectx, 0);

  if (!EVP_EncryptUpdate(ectx, out.buf, &outl, out.buf, encrypt_size)) {
    encrypt_size = 0;
    goto fallback;
  }

fallback:
  EVP_CIPHER_CTX_free(ectx);

  return encrypt_size;
}

std::size_t decrypt_aes_evp(const CryptoKeyHandler::in_slice_t& in,
                              const CryptoKeyHandler::out_slice_t& out,
                              const unsigned char* key)
 {
  std::size_t decrypt_size = in.length;
  std::size_t pad_len = 0;
  int outl = out.max_length; // needed for EVP_DecryptUpdate

  assert(decrypt_size > 0 && ((decrypt_size % AES_BLOCK_LEN) == 0));

  // crash if user specified too small buffer
  assert(out.max_length >= decrypt_size);

  EVP_CIPHER_CTX* ectx = EVP_CIPHER_CTX_new();
  assert(ectx);

  if (!EVP_DecryptInit_ex(ectx, EVP_aes_128_cbc(), NULL, key, (const unsigned char*)CEPH_AES_IV)) {
      decrypt_size = 0;
      goto fallback;
  }

  // we don't want OpenSSL to do the padding.
  EVP_CIPHER_CTX_set_padding(ectx, 0);

  if (!EVP_DecryptUpdate(ectx, out.buf, &outl, in.buf, decrypt_size)) {
    decrypt_size = 0;
    goto fallback;
  }

  pad_len = std::min<std::uint8_t>(out.buf[decrypt_size - 1], AES_BLOCK_LEN);

fallback:
  EVP_CIPHER_CTX_free(ectx);

  return decrypt_size - pad_len;
}

class CryptoAESKeyHandler : public CryptoKeyHandler {

public:
  CryptoAESKeyHandler()
    : CryptoKeyHandler(CryptoKeyHandler::BLOCK_SIZE_16B()) {
  }

  int init(const bufferptr& s, ostringstream& err) {
    secret = s;

    return 0;
  }

  int encrypt(const ceph::bufferlist& in,
	      ceph::bufferlist& out,
              std::string* /* unused */) const override {

    // we need to take into account the PKCS#7 padding. There *always* will
    // be at least one byte of padding. This stays even to input aligned to
    // AES_BLOCK_LEN. Otherwise we would face ambiguities during decryption.
    // To exemplify:
    //   16 + p2align(10, 16) -> 16
    //   16 + p2align(16, 16) -> 32 including 16 bytes for padding.
    ceph::bufferptr out_tmp{static_cast<unsigned>(
      AES_BLOCK_LEN + p2align(in.length(), AES_BLOCK_LEN))};

    // form contiguous buffer for block cipher. The ctor copies shallowly.
    // TODO: encrypt each bufferptr separately if possible?
    ceph::bufferlist incopy(in);

    const CryptoKeyHandler::in_slice_t sin {
        incopy.length(), reinterpret_cast<const unsigned char*>(incopy.c_str())
    };
    const CryptoKeyHandler::out_slice_t sout {
        out_tmp.length(), reinterpret_cast<unsigned char*>(out_tmp.c_str())
    };

    encrypt_aes_evp(sin, sout, reinterpret_cast<const unsigned char*>(secret.c_str()));
    out.append(out_tmp);
    return 0;
  }

  int decrypt(const ceph::bufferlist& in,
	      ceph::bufferlist& out,
	      std::string* /* unused */) const override {
    // PKCS#7 padding enlarges even empty plain-text to take 16 bytes.
    if (in.length() < AES_BLOCK_LEN || in.length() % AES_BLOCK_LEN) {
      return -1;
    }

    // needed because of .c_str() on const. It's a shallow copy.
    ceph::bufferlist incopy(in);
    ceph::bufferptr out_tmp{in.length()};
    const CryptoKeyHandler::in_slice_t sin {
        incopy.length(), reinterpret_cast<const unsigned char*>(incopy.c_str())
    };
    const CryptoKeyHandler::out_slice_t sout {
        out_tmp.length(), reinterpret_cast<unsigned char*>(out_tmp.c_str())
    };

    size_t decrypt_len = decrypt_aes_evp(sin, sout, reinterpret_cast<const unsigned char*>(secret.c_str()));
    out_tmp.set_length(decrypt_len);
    out.append(std::move(out_tmp));

    return 0;
  }

  std::size_t encrypt(const in_slice_t& in,
		      const out_slice_t& out) const override {
    return encrypt_aes_evp(in, out, reinterpret_cast<const unsigned char*>(secret.c_str()));
  }

  std::size_t decrypt(const in_slice_t& in,
		      const out_slice_t& out) const override {
    return decrypt_aes_evp(in, out, reinterpret_cast<const unsigned char*>(secret.c_str()));
  }
};

#else
# error "No supported crypto implementation found."
#endif



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

CryptoKeyHandler *CryptoAES::get_key_handler(const bufferptr& secret,
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
    throw buffer::malformed_input("malformed secret");
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
  default:
    return NULL;
  }
}
