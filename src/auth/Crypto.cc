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

int CryptoNone::create(bufferptr& secret)
{
  return 0;
}

int CryptoNone::validate_secret(bufferptr& secret)
{
  return 0;
}

void CryptoNone::encrypt(const bufferptr& secret, const bufferlist& in,
			 bufferlist& out, std::string &error) const
{
  out = in;
}

void CryptoNone::decrypt(const bufferptr& secret, const bufferlist& in,
			 bufferlist& out, std::string &error) const
{
  out = in;
}


// ---------------------------------------------------
#ifdef USE_CRYPTOPP
# define AES_KEY_LEN     ((size_t)CryptoPP::AES::DEFAULT_KEYLENGTH)
# define AES_BLOCK_LEN   ((size_t)CryptoPP::AES::BLOCKSIZE)
#elif USE_NSS
// when we say AES, we mean AES-128
# define AES_KEY_LEN	16
# define AES_BLOCK_LEN   16

static void nss_aes_operation(CK_ATTRIBUTE_TYPE op, const bufferptr& secret,
			     const bufferlist& in, bufferlist& out, std::string &error)
{
  const CK_MECHANISM_TYPE mechanism = CKM_AES_CBC_PAD;

  // sample source said this has to be at least size of input + 8,
  // but i see 15 still fail with SEC_ERROR_OUTPUT_LEN
  bufferptr out_tmp(in.length()+16);

  PK11SlotInfo *slot;

  slot = PK11_GetBestSlot(mechanism, NULL);
  if (!slot) {
    ostringstream oss;
    oss << "cannot find NSS slot to use: " << PR_GetError();
    error = oss.str();
    goto err;
  }

  SECItem keyItem;

  keyItem.type = siBuffer;
  keyItem.data = (unsigned char*)secret.c_str();
  keyItem.len = secret.length();

  PK11SymKey *key;

  key = PK11_ImportSymKey(slot, mechanism, PK11_OriginUnwrap, CKA_ENCRYPT,
			  &keyItem, NULL);
  if (!key) {
    ostringstream oss;
    oss << "cannot convert AES key for NSS: " << PR_GetError();
    error = oss.str();
    goto err_slot;
  }

  SECItem ivItem;

  ivItem.type = siBuffer;
  // losing constness due to SECItem.data; IV should never be
  // modified, regardless
  ivItem.data = (unsigned char*)CEPH_AES_IV;
  ivItem.len = sizeof(CEPH_AES_IV);

  SECItem *param;

  param = PK11_ParamFromIV(mechanism, &ivItem);
  if (!param) {
    ostringstream oss;
    oss << "cannot set NSS IV param: " << PR_GetError();
    error = oss.str();
    goto err_key;
  }

  PK11Context *ctx;

  ctx = PK11_CreateContextBySymKey(mechanism, op, key, param);
  if (!ctx) {
    ostringstream oss;
    oss << "cannot create NSS context: " << PR_GetError();
    error = oss.str();
    goto err_param;
  }

  SECStatus ret;
  int written;
  // in is const, and PK11_CipherOp is not; C++ makes this hard to cheat,
  // so just copy it to a temp buffer, at least for now
  unsigned in_len;
  unsigned char *in_buf;
  in_len = in.length();
  in_buf = (unsigned char*)malloc(in_len);
  if (!in_buf)
    throw std::bad_alloc();
  in.copy(0, in_len, (char*)in_buf);
  ret = PK11_CipherOp(ctx, (unsigned char*)out_tmp.c_str(), &written, out_tmp.length(),
		      in_buf, in.length());
  free(in_buf);
  if (ret != SECSuccess) {
    ostringstream oss;
    oss << "NSS AES failed: " << PR_GetError();
    error = oss.str();
    goto err_op;
  }

  unsigned int written2;
  ret = PK11_DigestFinal(ctx, (unsigned char*)out_tmp.c_str()+written, &written2,
			 out_tmp.length()-written);
  if (ret != SECSuccess) {
    ostringstream oss;
    oss << "NSS AES final round failed: " << PR_GetError();
    error = oss.str();
    goto err_op;
  }

  out_tmp.set_length(written + written2);
  out.append(out_tmp);

  PK11_DestroyContext(ctx, PR_TRUE);
  SECITEM_FreeItem(param, PR_TRUE);
  PK11_FreeSymKey(key);
  PK11_FreeSlot(slot);
  return;

 err_op:
  PK11_DestroyContext(ctx, PR_TRUE);
 err_param:
  SECITEM_FreeItem(param, PR_TRUE);
 err_key:
  PK11_FreeSymKey(key);
 err_slot:
  PK11_FreeSlot(slot);
 err:
  ;
}

#else
# error "No supported crypto implementation found."
#endif

int CryptoAES::create(bufferptr& secret)
{
  bufferlist bl;
  int r = get_random_bytes(AES_KEY_LEN, bl);
  if (r < 0)
    return r;
  secret = buffer::ptr(bl.c_str(), bl.length());
  return 0;
}

int CryptoAES::validate_secret(bufferptr& secret)
{
  if (secret.length() < (size_t)AES_KEY_LEN) {
    return -EINVAL;
  }

  return 0;
}

void CryptoAES::encrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out,
			std::string &error) const
{
  if (secret.length() < AES_KEY_LEN) {
    error = "key is too short";
    return;
  }
#ifdef USE_CRYPTOPP
  {
    const unsigned char *key = (const unsigned char *)secret.c_str();

    string ciphertext;
    CryptoPP::AES::Encryption aesEncryption(key, CryptoPP::AES::DEFAULT_KEYLENGTH);
    CryptoPP::CBC_Mode_ExternalCipher::Encryption cbcEncryption( aesEncryption, (const byte*)CEPH_AES_IV );
    CryptoPP::StringSink *sink = new CryptoPP::StringSink(ciphertext);
    CryptoPP::StreamTransformationFilter stfEncryptor(cbcEncryption, sink);

    for (std::list<bufferptr>::const_iterator it = in.buffers().begin();
	 it != in.buffers().end(); ++it) {
      const unsigned char *in_buf = (const unsigned char *)it->c_str();
      stfEncryptor.Put(in_buf, it->length());
    }
    try {
      stfEncryptor.MessageEnd();
    } catch (CryptoPP::Exception& e) {
      ostringstream oss;
      oss << "encryptor.MessageEnd::Exception: " << e.GetWhat();
      error = oss.str();
      return;
    }
    out.append((const char *)ciphertext.c_str(), ciphertext.length());
  }
#elif USE_NSS
  nss_aes_operation(CKA_ENCRYPT, secret, in, out, error);
#else
# error "No supported crypto implementation found."
#endif
}

void CryptoAES::decrypt(const bufferptr& secret, const bufferlist& in, 
			bufferlist& out, std::string &error) const
{
#ifdef USE_CRYPTOPP
  const unsigned char *key = (const unsigned char *)secret.c_str();

  CryptoPP::AES::Decryption aesDecryption(key, CryptoPP::AES::DEFAULT_KEYLENGTH);
  CryptoPP::CBC_Mode_ExternalCipher::Decryption cbcDecryption( aesDecryption, (const byte*)CEPH_AES_IV );

  string decryptedtext;
  CryptoPP::StringSink *sink = new CryptoPP::StringSink(decryptedtext);
  CryptoPP::StreamTransformationFilter stfDecryptor(cbcDecryption, sink);
  for (std::list<bufferptr>::const_iterator it = in.buffers().begin(); 
       it != in.buffers().end(); ++it) {
      const unsigned char *in_buf = (const unsigned char *)it->c_str();
      stfDecryptor.Put(in_buf, it->length());
  }

  try {
    stfDecryptor.MessageEnd();
  } catch (CryptoPP::Exception& e) {
    ostringstream oss;
    oss << "decryptor.MessageEnd::Exception: " << e.GetWhat();
    error = oss.str();
    return;
  }

  out.append((const char *)decryptedtext.c_str(), decryptedtext.length());
#elif USE_NSS
  nss_aes_operation(CKA_DECRYPT, secret, in, out, error);
#else
# error "No supported crypto implementation found."
#endif
}


// ---------------------------------------------------

int CryptoKey::set_secret(CephContext *cct, int type, bufferptr& s)
{
  this->type = type;
  created = ceph_clock_now(cct);

  CryptoHandler *h = cct->get_crypto_handler(type);
  if (!h) {
    lderr(cct) << "ERROR: cct->get_crypto_handler(type=" << type << ") returned NULL" << dendl;
    return -EOPNOTSUPP;
  }
  int ret = h->validate_secret(s);

  if (ret < 0)
    return ret;

  secret = s;

  return 0;
}

int CryptoKey::create(CephContext *cct, int t)
{
  type = t;
  created = ceph_clock_now(cct);

  CryptoHandler *h = cct->get_crypto_handler(type);
  if (!h) {
    lderr(cct) << "ERROR: cct->get_crypto_handler(type=" << type << ") returned NULL" << dendl;
    return -EOPNOTSUPP;
  }
  return h->create(secret);
}

void CryptoKey::encrypt(CephContext *cct, const bufferlist& in, bufferlist& out, std::string &error) const
{
  if (!ch || ch->get_type() != type) {
    ch = cct->get_crypto_handler(type);
    if (!ch) {
      ostringstream oss;
      oss << "CryptoKey::encrypt: key type " << type << " not supported.";
      return;
    }
  }
  ch->encrypt(this->secret, in, out, error);
}

void CryptoKey::decrypt(CephContext *cct, const bufferlist& in, bufferlist& out, std::string &error) const
{
  if (!ch || ch->get_type() != type) {
    ch = cct->get_crypto_handler(type);
    if (!ch) {
      ostringstream oss;
      oss << "CryptoKey::decrypt: key type " << type << " not supported.";
      return;
    }
  }
  ch->decrypt(this->secret, in, out, error);
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
