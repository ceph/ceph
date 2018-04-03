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

#include <sstream>
#include "Crypto.h"
#ifdef USE_NSS
# include <nspr.h>
# include <nss.h>
# include <pk11pub.h>
#endif

#include "include/assert.h"
#include "common/Clock.h"
#include "common/armor.h"
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
  : fd(TEMP_FAILURE_RETRY(::open("/dev/urandom", O_RDONLY)))
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

class CryptoNoneKeyHandler : public CryptoKeyHandler {
public:
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

#ifdef USE_NSS
// when we say AES, we mean AES-128
# define AES_KEY_LEN	16
# define AES_BLOCK_LEN   16

static int nss_aes_operation(
			     PK11Context* ectx,
			     const bufferlist& in, bufferlist& out,
			     std::string *error)
{
  // we are using CEPH_AES_IV for the IV param, so take it into consideration.
  bufferptr out_tmp{round_up_to(in.length() + sizeof(CEPH_AES_IV),
                                AES_BLOCK_LEN)};
  bufferlist incopy;

  SECStatus ret;
  int written;
  unsigned char *in_buf;

  incopy = in;  // it's a shallow copy!
  in_buf = (unsigned char*)incopy.c_str();
  ret = PK11_CipherOp(ectx,
		      (unsigned char*)out_tmp.c_str(), &written, out_tmp.length(),
		      in_buf, in.length());
  if (ret != SECSuccess) {
    if (error) {
      ostringstream oss;
      oss << "NSS AES failed: " << PR_GetError();
      *error = oss.str();
    }
    return -1;
  }

  unsigned int written2;
  ret = PK11_DigestFinal(ectx,
			 (unsigned char*)out_tmp.c_str()+written, &written2,
			 out_tmp.length()-written);
  if (ret != SECSuccess) {
    if (error) {
      ostringstream oss;
      oss << "NSS AES final round failed: " << PR_GetError();
      *error = oss.str();
    }
    return -1;
  }

  out_tmp.set_length(written + written2);
  out.append(out_tmp);
  return 0;
}

// transplated from ad2f92594bd88ba8cf4163c1f9a0562c53ed96a8
namespace ceph::crypto {

struct ScopedPK11Context {
  PK11Context* ctx;
  ScopedPK11Context(PK11Context *c = nullptr)
  : ctx(c)
  {}
  ~ScopedPK11Context() {
    PK11_DestroyContext(ctx, PR_TRUE);
  }
  void reset(PK11Context* c) noexcept {
   ctx = c;
  }
  PK11Context* get() const noexcept {
    return ctx;
  }
  explicit operator bool() const noexcept {
   return get() != nullptr;
  }
};

}

class CryptoAESKeyHandler : public CryptoKeyHandler {
  static constexpr CK_MECHANISM_TYPE mechanism = CKM_AES_CBC_PAD;
  ceph::crypto::ScopedPK11Context enc_ctx;
  ceph::crypto::ScopedPK11Context dec_ctx;

public:
  int init(const bufferptr& s, ostringstream& err) {
    secret = s;

    PK11SlotInfo *slot = nullptr;
    slot = PK11_GetBestSlot(mechanism, NULL);
    if (!slot) {
      err << "cannot find NSS slot to use: " << PR_GetError();
      return -1;
    }

    SECItem keyItem;
    keyItem.type = siBuffer;
    keyItem.data = (unsigned char*)secret.c_str();
    keyItem.len = secret.length();
    PK11SymKey* key = nullptr;
    key = PK11_ImportSymKey(slot, mechanism, PK11_OriginUnwrap, CKA_ENCRYPT,
			    &keyItem, NULL);
    PK11_FreeSlot(slot);

    if (!key) {
      err << "cannot convert AES key for NSS: " << PR_GetError();
      return -1;
    }

    SECItem ivItem;
    ivItem.type = siBuffer;
    // losing constness due to SECItem.data; IV should never be
    // modified, regardless
    ivItem.data = (unsigned char*)CEPH_AES_IV;
    ivItem.len = sizeof(CEPH_AES_IV);

    SECItem *param = nullptr;
    param = PK11_ParamFromIV(mechanism, &ivItem);
    if (!param) {
      err << "cannot set NSS IV param: " << PR_GetError();
      return -1;
    }

    enc_ctx.reset(PK11_CreateContextBySymKey(mechanism, CKA_ENCRYPT, key, param));
    dec_ctx.reset(PK11_CreateContextBySymKey(mechanism, CKA_DECRYPT, key, param));

    SECITEM_FreeItem(param, PR_TRUE);
    PK11_FreeSymKey(key);
    return 0;
  }

  int encrypt(const bufferlist& in,
	      bufferlist& out, std::string *error) const override {
    PK11_DigestBegin(enc_ctx.get());
    return nss_aes_operation(enc_ctx.get(), in, out, error);
  }
  int decrypt(const bufferlist& in,
	       bufferlist& out, std::string *error) const override {
    PK11_DigestBegin(dec_ctx.get());
    return nss_aes_operation(dec_ctx.get(), in, out, error);
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
  if (secret.length() < (size_t)AES_KEY_LEN) {
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

void CryptoKey::decode(bufferlist::iterator& bl)
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
