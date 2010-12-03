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

#include "Crypto.h"
#include <cryptopp/modes.h>
#include <cryptopp/aes.h>
#include <cryptopp/filters.h>

#include "include/ceph_fs.h"
#include "config.h"
#include "common/armor.h"

#include <errno.h>

using namespace CryptoPP;

int get_random_bytes(char *buf, int len)
{
  char *t = buf;
  int fd = ::open("/dev/urandom", O_RDONLY);
  int l = len;
  if (fd < 0)
    return -errno;
  while (l) {
    int r = ::read(fd, t, l);
    if (r < 0) {
      ::close(fd);
      return -errno;
    }
    t += r;
    l -= r;
  }
  ::close(fd);
  return 0;
}

static int get_random_bytes(int len, bufferlist& bl)
{
  char buf[len];
  get_random_bytes(buf, len);
  bl.append(buf, len);
  return 0;
}

void generate_random_string(string& s, int len)
{
  char buf[len+1];
  get_random_bytes(buf, len);
  buf[len] = 0;
  s = buf;
}

// ---------------------------------------------------

class CryptoNone : public CryptoHandler {
public:
  CryptoNone() {}
  ~CryptoNone() {}
  int create(bufferptr& secret);
  int validate_secret(bufferptr& secret);
  int encrypt(bufferptr& secret, const bufferlist& in, bufferlist& out);
  int decrypt(bufferptr& secret, const bufferlist& in, bufferlist& out);
};

int CryptoNone::create(bufferptr& secret)
{
  return 0;
}

int CryptoNone::validate_secret(bufferptr& secret)
{
  return 0;
}

int CryptoNone::encrypt(bufferptr& secret, const bufferlist& in, bufferlist& out)
{
  out = in;
  return 0;
}

int CryptoNone::decrypt(bufferptr& secret, const bufferlist& in, bufferlist& out)
{
  out = in;
  return 0;
}


// ---------------------------------------------------
#define AES_KEY_LEN     ((size_t)AES::DEFAULT_KEYLENGTH)
#define AES_BLOCK_LEN   ((size_t)AES::BLOCKSIZE)

class CryptoAES : public CryptoHandler {
public:
  CryptoAES() {}
  ~CryptoAES() {}
  int create(bufferptr& secret);
  int validate_secret(bufferptr& secret);
  int encrypt(bufferptr& secret, const bufferlist& in, bufferlist& out);
  int decrypt(bufferptr& secret, const bufferlist& in, bufferlist& out);
};

static const unsigned char *aes_iv = (const unsigned char *)CEPH_AES_IV;

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
    dout(0) << "key is too short" << dendl;
    return -EINVAL;
  }

  return 0;
}

int CryptoAES::encrypt(bufferptr& secret, const bufferlist& in, bufferlist& out)
{
  const unsigned char *key = (const unsigned char *)secret.c_str();
  const unsigned char *in_buf;

  if (secret.length() < AES_KEY_LEN) {
    derr(0) << "key is too short" << dendl;
    return false;
  }
  string ciphertext;
  CryptoPP::AES::Encryption aesEncryption(key, CryptoPP::AES::DEFAULT_KEYLENGTH);
  CryptoPP::CBC_Mode_ExternalCipher::Encryption cbcEncryption( aesEncryption, aes_iv );
  CryptoPP::StringSink *sink = new CryptoPP::StringSink(ciphertext);
  if (!sink)
    return false;
  CryptoPP::StreamTransformationFilter stfEncryptor(cbcEncryption, sink);

  for (std::list<bufferptr>::const_iterator it = in.buffers().begin(); 
       it != in.buffers().end(); it++) {
    in_buf = (const unsigned char *)it->c_str();

    stfEncryptor.Put(in_buf, it->length());
  }
  stfEncryptor.MessageEnd();
  out.append((const char *)ciphertext.c_str(), ciphertext.length());

  return true;
}

int CryptoAES::decrypt(bufferptr& secret, const bufferlist& in, bufferlist& out)
{
  const unsigned char *key = (const unsigned char *)secret.c_str();

  CryptoPP::AES::Decryption aesDecryption(key, CryptoPP::AES::DEFAULT_KEYLENGTH);
  CryptoPP::CBC_Mode_ExternalCipher::Decryption cbcDecryption( aesDecryption, aes_iv );

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

  stfDecryptor.MessageEnd();

  out.append((const char *)decryptedtext.c_str(), decryptedtext.length());
  return decryptedtext.length();
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

int CryptoKey::encrypt(const bufferlist& in, bufferlist& out)
{
  CryptoHandler *h = ceph_crypto_mgr.get_crypto(type);
  if (!h)
    return -EOPNOTSUPP;
  return h->encrypt(this->secret, in, out);
}

int CryptoKey::decrypt(const bufferlist& in, bufferlist& out)
{
  CryptoHandler *h = ceph_crypto_mgr.get_crypto(type);
  if (!h)
    return -EOPNOTSUPP;
  return h->decrypt(this->secret, in, out);
}

void CryptoKey::print(ostream &out) const
{
  string a;
  encode_base64(a);
  out << a;
}

void CryptoKey::to_str(string& s)
{
  int len = secret.length() * 4;
  char buf[len];
  hex2str(secret.c_str(), secret.length(), buf, len);
  s = buf;
}
