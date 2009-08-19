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

#include "AuthTypes.h"
#include "openssl/evp.h"

#define CRYPTO_STUPID   0x0
#define CRYPTO_AES      0x1


class CryptoHandler {
public:
  virtual bool encrypt(EntitySecret& secret, bufferlist& in, bufferlist& out) = 0;
  virtual bool decrypt(EntitySecret& secret, bufferlist& in, bufferlist& out) = 0;
};

class CryptoStupid : public CryptoHandler {
public:
  CryptoStupid() {}
  ~CryptoStupid() {}
  bool encrypt(EntitySecret& secret, bufferlist& in, bufferlist& out);
  bool decrypt(EntitySecret& secret, bufferlist& in, bufferlist& out);
};

bool CryptoStupid::encrypt(EntitySecret& secret, bufferlist& in, bufferlist& out)
{
  bufferlist sec_bl = secret.get_secret();
  const char *sec = sec_bl.c_str();
  int sec_len = sec_bl.length();

  int in_len = in.length();
  bufferptr outptr(in_len);
  out.append(outptr);
  const char *inbuf = in.c_str();
  char *outbuf = outptr.c_str();

  for (int i=0; i<in_len; i++) {
    outbuf[i] = inbuf[i] ^ sec[i % sec_len];
  }

  return true;
}

bool CryptoStupid::decrypt(EntitySecret& secret, bufferlist& in, bufferlist& out)
{
  return encrypt(secret, in, out);
}

#define AES_KEY_LEN     16

class CryptoAES : public CryptoHandler {
public:
  CryptoStupid() {}
  ~CryptoStupid() {}
  bool encrypt(EntitySecret& secret, bufferlist& in, bufferlist& out);
  bool decrypt(EntitySecret& secret, bufferlist& in, bufferlist& out);
};

static const unsigned char *aes_iv = "cephsageyudagreg";

bool CryptoStupid::encrypt(EntitySecret& secret, bufferlist& in, bufferlist& out)
{
  bufferlist sec_bl = secret.get_secret();
  int outlen, tmplen;
  bufferptr outptr(outlen);

  if (sec_bl.length() < AES_KEY_LEN)
    return false;

  const char *key = sec_bl.c_str();
  char intext[] = "12345678901234567890123456789012";
  EVP_CIPHER_CTX ctx;
  FILE *out;
  EVP_CIPHER_CTX_init(&ctx);
  EVP_EncryptInit_ex(&ctx, EVP_aes_128_cbc(), NULL, key, aes_iv);

  if(!EVP_EncryptUpdate(&ctx, outbuf, &outlen, intext, strlen(intext))) {
    dout(0) << "EVP_EncryptUpdate error" << dendl;
    return false;
  }
  if(!EVP_EncryptFinal_ex(&ctx, outbuf + outlen, &tmplen)) {
    dout(0) << "EVP_EncryptFinal error" << dendl;
    return false;
  }

  return true;
}

static CryptoStupid crypto_stupid;


class CryptoManager {
public:
  CryptoHandler *get_crypto(int type);
};


CryptoHandler *CryptoManager::get_crypto(int type)
{
  switch (type) {
    case CRYPTO_STUPID:
      return &crypto_stupid;
    default:
      return NULL;
  }
}
