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

#include <assert.h>
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

#include "include/ceph_fs.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/armor.h"
#include "common/Clock.h"
#include "common/hex.h"
#include "common/safe_io.h"

#include <errno.h>

int get_random_bytes(char *buf, int len)
{
  int fd = TEMP_FAILURE_RETRY(::open("/dev/urandom", O_RDONLY));
  if (fd < 0)
    return -errno;
  int ret = safe_read_exact(fd, buf, len);
  if (ret)
    return ret;
  TEMP_FAILURE_RETRY(::close(fd));
  return 0;
}

static int get_random_bytes(int len, bufferlist& bl)
{
  char buf[len];
  get_random_bytes(buf, len);
  bl.append(buf, len);
  return 0;
}

// ---------------------------------------------------

class CryptoNone : public CryptoHandler {
public:
  CryptoNone() {}
  ~CryptoNone() {}
  int create(bufferptr& secret);
  int validate_secret(bufferptr& secret);
  int encrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out) const;
  int decrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out) const;
};

int CryptoNone::
create(bufferptr& secret)
{
  return 0;
}

int CryptoNone::
validate_secret(bufferptr& secret)
{
  return 0;
}

int CryptoNone::
encrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out) const
{
  out = in;
  return 0;
}

int CryptoNone::
decrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out) const
{
  out = in;
  return 0;
}


// ---------------------------------------------------
#ifdef USE_CRYPTOPP
# define AES_KEY_LEN     ((size_t)CryptoPP::AES::DEFAULT_KEYLENGTH)
# define AES_BLOCK_LEN   ((size_t)CryptoPP::AES::BLOCKSIZE)
#elif USE_NSS
// when we say AES, we mean AES-128
# define AES_KEY_LEN	16
# define AES_BLOCK_LEN   16

static int nss_aes_operation(CK_ATTRIBUTE_TYPE op, bufferptr& secret, const bufferlist& in, bufferlist& out) {
  const CK_MECHANISM_TYPE mechanism = CKM_AES_CBC_PAD;

  // sample source said this has to be at least size of input + 8,
  // but i see 15 still fail with SEC_ERROR_OUTPUT_LEN
  bufferptr out_tmp(in.length()+16);
  int err = -EINVAL;

  PK11SlotInfo *slot;

  slot = PK11_GetBestSlot(mechanism, NULL);
  if (!slot) {
    dout(0) << "cannot find NSS slot to use: " << PR_GetError() << dendl;
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
    dout(0) << "cannot convert AES key for NSS: " << PR_GetError() << dendl;
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
    dout(0) << "cannot set NSS IV param: " << PR_GetError() << dendl;
    goto err_key;
  }

  PK11Context *ctx;

  ctx = PK11_CreateContextBySymKey(mechanism, op, key, param);
  if (!ctx) {
    dout(0) << "cannot create NSS context: " << PR_GetError() << dendl;
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
  if (!in_buf) {
    dout(0) << "NSS out of memory" << dendl;
    err = -ENOMEM;
    goto err_ctx;
  }
  in.copy(0, in_len, (char*)in_buf);
  ret = PK11_CipherOp(ctx, (unsigned char*)out_tmp.c_str(), &written, out_tmp.length(),
		      in_buf, in.length());
  free(in_buf);
  if (ret != SECSuccess) {
    dout(0) << "NSS AES failed: " << PR_GetError() << dendl;
    goto err_op;
  }

  unsigned int written2;
  ret = PK11_DigestFinal(ctx, (unsigned char*)out_tmp.c_str()+written, &written2,
			 out_tmp.length()-written);
  if (ret != SECSuccess) {
    dout(0) << "NSS AES final round failed: " << PR_GetError() << dendl;
    goto err_op;
  }

  PK11_DestroyContext(ctx, PR_TRUE);
  out_tmp.set_length(written + written2);
  out.append(out_tmp);
  return out_tmp.length();

 err_op:
 err_ctx:
  PK11_DestroyContext(ctx, PR_TRUE);
 err_param:
  SECITEM_FreeItem(param, PR_TRUE);
 err_key:
  PK11_FreeSymKey(key);
 err_slot:
  PK11_FreeSlot(slot);
 err:
  return err;
}

#else
# error "No supported crypto implementation found."
#endif

class CryptoAES : public CryptoHandler {
public:
  CryptoAES() {}
  ~CryptoAES() {}
  int create(bufferptr& secret);
  int validate_secret(bufferptr& secret);
  int encrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out) const;
  int decrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out) const;
};

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
    dout(0) << "key is too short: " << secret.length() << "<" << AES_KEY_LEN << dendl;
    return -EINVAL;
  }

  return 0;
}

int CryptoAES::
encrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out) const
{
  if (secret.length() < AES_KEY_LEN) {
    dout(0) << "key is too short" << dendl;
    return false;
  }
#ifdef USE_CRYPTOPP
  {
    const unsigned char *key = (const unsigned char *)secret.c_str();
    const unsigned char *in_buf;

    string ciphertext;
    CryptoPP::AES::Encryption aesEncryption(key, CryptoPP::AES::DEFAULT_KEYLENGTH);
    CryptoPP::CBC_Mode_ExternalCipher::Encryption cbcEncryption( aesEncryption, (const byte*)CEPH_AES_IV );
    CryptoPP::StringSink *sink = new CryptoPP::StringSink(ciphertext);
    if (!sink)
      return false;
    CryptoPP::StreamTransformationFilter stfEncryptor(cbcEncryption, sink);

    for (std::list<bufferptr>::const_iterator it = in.buffers().begin();
	 it != in.buffers().end(); it++) {
      in_buf = (const unsigned char *)it->c_str();

      stfEncryptor.Put(in_buf, it->length());
    }
    try {
      stfEncryptor.MessageEnd();
    } catch (CryptoPP::Exception& e) {
      dout(0) << "encryptor.MessageEnd::Exception: " << e.GetWhat() << dendl;
      return false;
    }
    out.append((const char *)ciphertext.c_str(), ciphertext.length());
  }
#elif USE_NSS
  // the return type may be int but this CryptoAES::encrypt returns bools
  return (nss_aes_operation(CKA_ENCRYPT, secret, in, out) >= 0);
#else
# error "No supported crypto implementation found."
#endif
  return true;
}

int CryptoAES::
decrypt(const bufferptr& secret, const bufferlist& in, bufferlist& out) const
{
#ifdef USE_CRYPTOPP
  const unsigned char *key = (const unsigned char *)secret.c_str();

  CryptoPP::AES::Decryption aesDecryption(key, CryptoPP::AES::DEFAULT_KEYLENGTH);
  CryptoPP::CBC_Mode_ExternalCipher::Decryption cbcDecryption( aesDecryption, (const byte*)CEPH_AES_IV );

  string decryptedtext;
  CryptoPP::StringSink *sink = new CryptoPP::StringSink(decryptedtext);
  if (!sink)
    return -ENOMEM;
  CryptoPP::StreamTransformationFilter stfDecryptor(cbcDecryption, sink);
  for (std::list<bufferptr>::const_iterator it = in.buffers().begin(); 
       it != in.buffers().end(); it++) {
      const unsigned char *in_buf = (const unsigned char *)it->c_str();
      stfDecryptor.Put(in_buf, it->length());
  }

  try {
    stfDecryptor.MessageEnd();
  } catch (CryptoPP::Exception& e) {
    dout(0) << "decryptor.MessageEnd::Exception: " << e.GetWhat() << dendl;
    return -EINVAL;
  }

  out.append((const char *)decryptedtext.c_str(), decryptedtext.length());
  return decryptedtext.length();
#elif USE_NSS
  return nss_aes_operation(CKA_DECRYPT, secret, in, out);
#else
# error "No supported crypto implementation found."
#endif
}


// ---------------------------------------------------

static CryptoNone crypto_none;
static CryptoAES crypto_aes;

CryptoHandler *CryptoManager::get_crypto(int type)
{
  switch (type) {
    case CEPH_CRYPTO_NONE:
      return &crypto_none;
    case CEPH_CRYPTO_AES:
      return &crypto_aes;
    default:
      return NULL;
  }
}

CryptoManager ceph_crypto_mgr;


// ---------------------------------------------------

int CryptoKey::set_secret(int type, bufferptr& s)
{
  this->type = type;
  created = g_clock.now();

  CryptoHandler *h = ceph_crypto_mgr.get_crypto(type);
  if (!h)
    return -EOPNOTSUPP;

  int ret = h->validate_secret(s);

  if (ret < 0)
    return ret;

  secret = s;

  return 0;
}

int CryptoKey::create(int t)
{
  type = t;
  created = g_clock.now();

  CryptoHandler *h = ceph_crypto_mgr.get_crypto(type);
  if (!h)
    return -EOPNOTSUPP;
  return h->create(secret);
}

int CryptoKey::encrypt(const bufferlist& in, bufferlist& out) const
{
  CryptoHandler *h = ceph_crypto_mgr.get_crypto(type);
  if (!h)
    return -EOPNOTSUPP;
  return h->encrypt(this->secret, in, out);
}

int CryptoKey::decrypt(const bufferlist& in, bufferlist& out) const
{
  CryptoHandler *h = ceph_crypto_mgr.get_crypto(type);
  if (!h)
    return -EOPNOTSUPP;
  return h->decrypt(this->secret, in, out);
}

void CryptoKey::print(std::ostream &out) const
{
  string a;
  encode_base64(a);
  out << a;
}

void CryptoKey::to_str(std::string& s) const
{
  int len = secret.length() * 4;
  char buf[len];
  hex2str(secret.c_str(), secret.length(), buf, len);
  s = buf;
}
