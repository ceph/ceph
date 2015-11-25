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
#ifdef USE_CRYPTOPP
# include <cryptopp/modes.h>
# include <cryptopp/aes.h>
# include <cryptopp/filters.h>
#elif USE_NSS
# include <nspr.h>
# include <nss.h>
# include <pk11pub.h>
#endif

#include "include/assert.h"
#include "common/Clock.h"
#include "common/armor.h"
#include "common/ceph_crypto.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/hex.h"
#include "common/safe_io.h"
#include "include/ceph_fs.h"
#include "include/compat.h"

#include <errno.h>

int get_random_bytes(char *buf, int len)
{
  int fd = TEMP_FAILURE_RETRY(::open("/dev/urandom", O_RDONLY));
  if (fd < 0)
    return -errno;
  int ret = safe_read_exact(fd, buf, len);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  return ret;
}

static int get_random_bytes(int len, bufferlist& bl)
{
  char buf[len];
  get_random_bytes(buf, len);
  bl.append(buf, len);
  return 0;
}

uint64_t get_random(uint64_t min_val, uint64_t max_val)
{
  uint64_t r;
  get_random_bytes((char *)&r, sizeof(r));
  r = min_val + r % (max_val - min_val + 1);
  return r;
}


// ---------------------------------------------------

class CryptoNoneKeyHandler : public CryptoKeyHandler {
public:
  int encrypt(const bufferlist& in,
	       bufferlist& out, std::string *error) const {
    out = in;
    return 0;
  }
  int decrypt(const bufferlist& in,
	      bufferlist& out, std::string *error) const {
    out = in;
    return 0;
  }
};

class CryptoNone : public CryptoHandler {
public:
  CryptoNone() { }
  ~CryptoNone() {}
  int get_type() const {
    return CEPH_CRYPTO_NONE;
  }
  int create(bufferptr& secret) {
    return 0;
  }
  int validate_secret(const bufferptr& secret) {
    return 0;
  }
  CryptoKeyHandler *get_key_handler(const bufferptr& secret, string& error) {
    return new CryptoNoneKeyHandler;
  }
};


// ---------------------------------------------------


class CryptoAES : public CryptoHandler {
public:
  CryptoAES() { }
  ~CryptoAES() {}
  int get_type() const {
    return CEPH_CRYPTO_AES;
  }
  int create(bufferptr& secret);
  int validate_secret(const bufferptr& secret);
  CryptoKeyHandler *get_key_handler(const bufferptr& secret, string& error);
};

#ifdef USE_CRYPTOPP
# define AES_KEY_LEN     ((size_t)CryptoPP::AES::DEFAULT_KEYLENGTH)
# define AES_BLOCK_LEN   ((size_t)CryptoPP::AES::BLOCKSIZE)

class CryptoAESKeyHandler : public CryptoKeyHandler {
public:
  CryptoPP::AES::Encryption *enc_key;
  CryptoPP::AES::Decryption *dec_key;

  CryptoAESKeyHandler()
    : enc_key(NULL),
      dec_key(NULL) {}
  ~CryptoAESKeyHandler() {
    delete enc_key;
    delete dec_key;
  }

  int init(const bufferptr& s, ostringstream& err) {
    secret = s;

    enc_key = new CryptoPP::AES::Encryption(
      (byte*)secret.c_str(), CryptoPP::AES::DEFAULT_KEYLENGTH);
    dec_key = new CryptoPP::AES::Decryption(
      (byte*)secret.c_str(), CryptoPP::AES::DEFAULT_KEYLENGTH);

    return 0;
  }

  int encrypt(const bufferlist& in,
	      bufferlist& out, std::string *error) const {
    string ciphertext;
    CryptoPP::StringSink *sink = new CryptoPP::StringSink(ciphertext);
    CryptoPP::CBC_Mode_ExternalCipher::Encryption cbc(
      *enc_key, (const byte*)CEPH_AES_IV);
    CryptoPP::StreamTransformationFilter stfEncryptor(cbc, sink);

    for (std::list<bufferptr>::const_iterator it = in.buffers().begin();
	 it != in.buffers().end(); ++it) {
      const unsigned char *in_buf = (const unsigned char *)it->c_str();
      stfEncryptor.Put(in_buf, it->length());
    }
    try {
      stfEncryptor.MessageEnd();
    } catch (CryptoPP::Exception& e) {
      if (error) {
	ostringstream oss;
	oss << "encryptor.MessageEnd::Exception: " << e.GetWhat();
	*error = oss.str();
      }
      return -1;
    }
    out.append((const char *)ciphertext.c_str(), ciphertext.length());
    return 0;
  }

  int decrypt(const bufferlist& in,
	      bufferlist& out, std::string *error) const {
    string decryptedtext;
    CryptoPP::StringSink *sink = new CryptoPP::StringSink(decryptedtext);
    CryptoPP::CBC_Mode_ExternalCipher::Decryption cbc(
      *dec_key, (const byte*)CEPH_AES_IV );
    CryptoPP::StreamTransformationFilter stfDecryptor(cbc, sink);
    for (std::list<bufferptr>::const_iterator it = in.buffers().begin();
	 it != in.buffers().end(); ++it) {
      const unsigned char *in_buf = (const unsigned char *)it->c_str();
      stfDecryptor.Put(in_buf, it->length());
    }

    try {
      stfDecryptor.MessageEnd();
    } catch (CryptoPP::Exception& e) {
      if (error) {
	ostringstream oss;
	oss << "decryptor.MessageEnd::Exception: " << e.GetWhat();
	*error = oss.str();
      }
      return -1;
    }

    out.append((const char *)decryptedtext.c_str(), decryptedtext.length());
    return 0;
  }
};

#elif USE_NSS
// when we say AES, we mean AES-128
# define AES_KEY_LEN	16
# define AES_BLOCK_LEN   16

static int nss_aes_operation(CK_ATTRIBUTE_TYPE op,
			     CK_MECHANISM_TYPE mechanism,
			     PK11SymKey *key,
			     SECItem *param,
			     const bufferlist& in, bufferlist& out,
			     std::string *error)
{
  // sample source said this has to be at least size of input + 8,
  // but i see 15 still fail with SEC_ERROR_OUTPUT_LEN
  bufferptr out_tmp(in.length()+16);
  bufferlist incopy;

  SECStatus ret;
  int written;
  unsigned char *in_buf;

  PK11Context *ectx;
  ectx = PK11_CreateContextBySymKey(mechanism, op, key, param);
  assert(ectx);

  incopy = in;  // it's a shallow copy!
  in_buf = (unsigned char*)incopy.c_str();
  ret = PK11_CipherOp(ectx,
		      (unsigned char*)out_tmp.c_str(), &written, out_tmp.length(),
		      in_buf, in.length());
  if (ret != SECSuccess) {
    PK11_DestroyContext(ectx, PR_TRUE);
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
  PK11_DestroyContext(ectx, PR_TRUE);
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

class CryptoAESKeyHandler : public CryptoKeyHandler {
  CK_MECHANISM_TYPE mechanism;
  PK11SlotInfo *slot;
  PK11SymKey *key;
  SECItem *param;

public:
  CryptoAESKeyHandler()
    : mechanism(CKM_AES_CBC_PAD),
      slot(NULL),
      key(NULL),
      param(NULL) {}
  ~CryptoAESKeyHandler() {
    SECITEM_FreeItem(param, PR_TRUE);
    PK11_FreeSymKey(key);
    PK11_FreeSlot(slot);
  }

  int init(const bufferptr& s, ostringstream& err) {
    secret = s;

    slot = PK11_GetBestSlot(mechanism, NULL);
    if (!slot) {
      err << "cannot find NSS slot to use: " << PR_GetError();
      return -1;
    }

    SECItem keyItem;
    keyItem.type = siBuffer;
    keyItem.data = (unsigned char*)secret.c_str();
    keyItem.len = secret.length();
    key = PK11_ImportSymKey(slot, mechanism, PK11_OriginUnwrap, CKA_ENCRYPT,
			    &keyItem, NULL);
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

    param = PK11_ParamFromIV(mechanism, &ivItem);
    if (!param) {
      err << "cannot set NSS IV param: " << PR_GetError();
      return -1;
    }

    return 0;
  }

  int encrypt(const bufferlist& in,
	      bufferlist& out, std::string *error) const {
    return nss_aes_operation(CKA_ENCRYPT, mechanism, key, param, in, out, error);
  }
  int decrypt(const bufferlist& in,
	       bufferlist& out, std::string *error) const {
    return nss_aes_operation(CKA_DECRYPT, mechanism, key, param, in, out, error);
  }
};

#else
# error "No supported crypto implementation found."
#endif



// ------------------------------------------------------------

int CryptoAES::create(bufferptr& secret)
{
  bufferlist bl;
  int r = get_random_bytes(AES_KEY_LEN, bl);
  if (r < 0)
    return r;
  secret = buffer::ptr(bl.c_str(), bl.length());
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
    return NULL;
  }
  return ckh;
}




// --


// ---------------------------------------------------


void CryptoKey::encode(bufferlist& bl) const
{
  ::encode(type, bl);
  ::encode(created, bl);
  __u16 len = secret.length();
  ::encode(len, bl);
  bl.append(secret);
}

void CryptoKey::decode(bufferlist::iterator& bl)
{
  ::decode(type, bl);
  ::decode(created, bl);
  __u16 len;
  ::decode(len, bl);
  bufferptr tmp;
  bl.copy(len, tmp);
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
  int r = ch->create(s);
  delete ch;
  if (r < 0)
    return r;

  r = _set_secret(t, s);
  if (r < 0)
    return r;
  created = ceph_clock_now(cct);
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
