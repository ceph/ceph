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

#include "openssl/evp.h"
#include "openssl/aes.h"

#include "include/ceph_fs.h"
#include "config.h"
#include "common/armor.h"

#include <errno.h>

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

#define AES_KEY_LEN     AES_BLOCK_SIZE

class CryptoAES : public CryptoHandler {
public:
  CryptoAES() {}
  ~CryptoAES() {}
  int create(bufferptr& secret);
  int validate_secret(bufferptr& secret);
  int encrypt(bufferptr& secret, const bufferlist& in, bufferlist& out);
  int decrypt(bufferptr& secret, const bufferlist& in, bufferlist& out);
};

static const unsigned char *aes_iv = (const unsigned char *)"cephsageyudagreg";

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
  if (secret.length() < AES_KEY_LEN) {
    dout(0) << "key is too short" << dendl;
    return -EINVAL;
  }

  return 0;
}

int CryptoAES::encrypt(bufferptr& secret, const bufferlist& in, bufferlist& out)
{
  const unsigned char *key = (const unsigned char *)secret.c_str();
  int in_len = in.length();
  const unsigned char *in_buf;
  int max_out = (in_len + AES_BLOCK_SIZE) & ~(AES_BLOCK_SIZE -1);
  int total_out = 0;
  int outlen;
#define OUT_BUF_EXTRA 128
  unsigned char outbuf[max_out + OUT_BUF_EXTRA];

  if (secret.length() < AES_KEY_LEN) {
    derr(0) << "key is too short" << dendl;
    return false;
  }

  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);
  EVP_EncryptInit_ex(&ctx, EVP_aes_128_cbc(), NULL, key, aes_iv);

  bool ret = false;
  for (std::list<bufferptr>::const_iterator it = in.buffers().begin(); 
       it != in.buffers().end(); it++) {
    outlen = max_out - total_out;
    in_buf = (const unsigned char *)it->c_str();
    if (!EVP_EncryptUpdate(&ctx, &outbuf[total_out], &outlen, in_buf, it->length()))
      goto out;
    total_out += outlen;
  }
  if (!EVP_EncryptFinal_ex(&ctx, outbuf + total_out, &outlen))
    goto out;
  total_out += outlen;

  out.append((const char *)outbuf, total_out);
  ret = true;
 out:
  EVP_CIPHER_CTX_cleanup(&ctx);
  return ret;
}

int CryptoAES::decrypt(bufferptr& secret, const bufferlist& in, bufferlist& out)
{
  const unsigned char *key = (const unsigned char *)secret.c_str();

  int in_len = in.length();
  int dec_len = 0;
  int total_dec_len = 0;

  unsigned char dec_data[in_len];
  int result = 0;

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  EVP_CIPHER_CTX_init(ctx);

  int res = EVP_DecryptInit_ex(ctx, EVP_aes_128_cbc(), NULL, key, aes_iv);
  if (res == 1) {
    for (std::list<bufferptr>::const_iterator it = in.buffers().begin(); 
       it != in.buffers().end(); it++) {
      const unsigned char *in_buf = (const unsigned char *)it->c_str();
      res = EVP_DecryptUpdate(ctx, &dec_data[total_dec_len],
            &dec_len, in_buf, it->length());
      total_dec_len += dec_len;

      if (res != 1) {
        dout(0) << "EVP_DecryptUpdate error" << dendl;
        result = -1;
      }
    }
  } else {
    dout(0) << "EVP_DecryptInit_ex error" << dendl;
    result = -1;
  }

  if (!result) {
    EVP_DecryptFinal_ex(ctx,
              &dec_data[total_dec_len],
              &dec_len);

    total_dec_len += dec_len;
    out.append((const char *)dec_data, total_dec_len);
  }

  EVP_CIPHER_CTX_free(ctx);
  EVP_cleanup();
  return result;
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
