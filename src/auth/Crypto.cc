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
#elif defined(USE_NSS)
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

namespace ceph {

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

}

// ---------------------------------------------------
namespace ceph {
const bufferptr AES_IV =
    buffer::create_static(sizeof(CEPH_AES_IV)-1, (char*)CEPH_AES_IV);
const bufferptr EMPTY_IV =
    buffer::create_static(0, nullptr);
}

namespace ceph {

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
  int create(bufferptr& secret) override {
    return 0;
  }
  int validate_secret(const bufferptr& secret) override {
    return 0;
  }
  CryptoKeyHandler *get_key_handler(const bufferptr& secret, const bufferptr& iv, string& error) override {
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
  int create(bufferptr& secret) override;
  int validate_secret(const bufferptr& secret) override;
  CryptoKeyHandler *get_key_handler(const bufferptr& secret, const bufferptr& iv, string& error) override;
};

#ifdef USE_CRYPTOPP
# define AES_KEY_LEN     ((size_t)CryptoPP::AES::DEFAULT_KEYLENGTH)
# define AES_BLOCK_LEN   ((size_t)CryptoPP::AES::BLOCKSIZE)

class CryptoAESKeyHandler : public CryptoKeyHandler {
public:
  CryptoPP::AES::Encryption *enc_key;
  CryptoPP::AES::Decryption *dec_key;
  bufferptr secret;

  CryptoAESKeyHandler()
    : enc_key(NULL),
      dec_key(NULL) {}
  ~CryptoAESKeyHandler() {
    delete enc_key;
    delete dec_key;
  }

  int init(const bufferptr& s, const bufferptr& iv, ostringstream& err) {
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

#elif defined(USE_NSS)
// when we say AES, we mean AES-128
# define AES_KEY_LEN	16
# define AES_BLOCK_LEN   16
# define AES_IV_LEN 16

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
  bufferptr secret;

public:
  CryptoAESKeyHandler()
    : mechanism(CKM_AES_CBC_PAD),
      slot(NULL),
      key(NULL),
      param(NULL) {}
  ~CryptoAESKeyHandler() override {
    SECITEM_FreeItem(param, PR_TRUE);
    if (key)
      PK11_FreeSymKey(key);
    if (slot)
      PK11_FreeSlot(slot);
  }

  int init(const bufferptr& s, const bufferptr& iv, ostringstream& err) {
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
    static_assert(AES_IV_LEN == sizeof(CEPH_AES_IV)-1, "Internal error");
    SECItem ivItem;
    ivItem.type = siBuffer;
    // losing constness due to SECItem.data; IV should never be
    // modified, regardless
    ivItem.data = (unsigned char*)iv.c_str();
    ivItem.len = iv.length();

    param = PK11_ParamFromIV(mechanism, &ivItem);
    if (!param) {
      err << "cannot set NSS IV param: " << PR_GetError();
      return -1;
    }

    return 0;
  }

  int encrypt(const bufferlist& in,
	      bufferlist& out, std::string *error) const override {
    return nss_aes_operation(CKA_ENCRYPT, mechanism, key, param, in, out, error);
  }
  int decrypt(const bufferlist& in,
	       bufferlist& out, std::string *error) const override {
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

CryptoKeyHandler *CryptoAES::get_key_handler(
    const bufferptr& secret,
    const bufferptr& iv,
    string& error)
{
  CryptoAESKeyHandler *ckh = new CryptoAESKeyHandler;
    ostringstream oss;
    if (ckh->init(secret, iv, oss) < 0) {
      error = oss.str();
      delete ckh;
      return NULL;
    }
    return ckh;
}


class CryptoAES_256_CBC : public CryptoHandler {
public:
  int get_type() const override {
    return CEPH_CRYPTO_AES_256_CBC;
  }
  int create(bufferptr& secret) override {
    bufferptr p(KEY_SIZE);
    int r = get_random_bytes(p.c_str(), p.length());
    if (r < 0)
      return r;
    secret = std::move(p);
    return 0;
  }
  int validate_secret(const bufferptr& secret) override {
    if (secret.length() != KEY_SIZE) {
      return -EINVAL;
    }
    return 0;
  }
  CryptoKeyHandler *get_key_handler(
      const bufferptr& secret,
      const bufferptr& iv,
      string& error) override;

  static constexpr size_t KEY_SIZE = 256 / 8;
  static constexpr size_t IV_SIZE = 128 / 8;
  static constexpr size_t BLOCK_SIZE = 128 / 8;
};


class CryptoAES_256_CTR : public CryptoHandler {
public:
  int get_type() const override {
    return CEPH_CRYPTO_AES_256_CTR;
  }
  int create(bufferptr& secret) override {
    bufferptr p(KEY_SIZE);
    int r = get_random_bytes(p.c_str(), p.length());
    if (r < 0)
      return r;
    secret = std::move(p);
    return 0;
  }
  int validate_secret(const bufferptr& secret) override {
    if (secret.length() != KEY_SIZE) {
      return -EINVAL;
    }
    return 0;
  }
  CryptoKeyHandler *get_key_handler(
      const bufferptr& secret,
      const bufferptr& iv,
      string& error) override;

  static constexpr size_t KEY_SIZE = 256 / 8;
  static constexpr size_t IV_SIZE = 128 / 8;
  static constexpr size_t BLOCK_SIZE = 128 / 8;
};


class CryptoAES_256_ECB : public CryptoHandler {
public:
  int get_type() const override {
    return CEPH_CRYPTO_AES_256_ECB;
  }
  int create(bufferptr& secret) override {
    bufferptr p(KEY_SIZE);
    int r = get_random_bytes(p.c_str(), p.length());
    if (r < 0)
      return r;
    secret = std::move(p);
    return 0;
  }
  int validate_secret(const bufferptr& secret) override {
    if (secret.length() != KEY_SIZE) {
      return -EINVAL;
    }
    return 0;
  }
  CryptoKeyHandler *get_key_handler(
      const bufferptr& secret,
      const bufferptr& iv,
      string& error) override;

  static constexpr size_t KEY_SIZE = 256 / 8;
  static constexpr size_t BLOCK_SIZE = 128 / 8;
};


class CryptoAES_256_CBCKeyHandler : public CryptoKeyHandler {
public:
  CryptoAES_256_CBCKeyHandler(
      const bufferptr& key,
      const bufferptr& iv)
    :secret(key), iv(iv) {}
  int encrypt(
      const bufferlist& in,
      bufferlist& out,
      std::string *error) const {
    return cbc_transform(in, out, true, error);
  }
  int decrypt(
      const bufferlist& in,
      bufferlist& out,
      std::string *error) const {
    return cbc_transform(in, out, false, error);
  }
private:
  bufferptr secret;
  bufferptr iv;
  int cbc_transform(
      const bufferlist& in,
      bufferlist& out,
      bool encrypt,
      std::string *error) const {
    if ((in.length() % CryptoAES_256_CBC::BLOCK_SIZE) != 0) {
      if (error) {
        *error = "Input length not multiple of 16.";
      }
      return -EINVAL;
    }
    int result = -EINVAL;
    bufferptr p(in.length());
    bufferlist _in(in);
    if (cbc_transform(
        reinterpret_cast<const unsigned char*>(_in.c_str()),
        in.length(),
        reinterpret_cast<unsigned char*>(p.c_str()),
        encrypt, error)) {
      result = 0;
      out.append(p);
    }
    return result;
  }

  bool cbc_transform(
      const unsigned char* in,
      size_t size,
      unsigned char* out,
      bool encrypt,
      std::string *error) const ;
};

#ifdef USE_CRYPTOPP

bool CryptoAES_256_CBCKeyHandler::cbc_transform(
    const unsigned char* in,
    size_t size,
    unsigned char* out,
    bool encrypt,
    std::string *error) const
{
  using namespace CryptoPP;
  if (encrypt) {
    CBC_Mode< AES >::Encryption e;
    e.SetKeyWithIV(
        reinterpret_cast<const byte*>(secret.c_str()), CryptoAES_256_CBC::KEY_SIZE,
        reinterpret_cast<const byte*>(iv.c_str()), CryptoAES_256_CBC::IV_SIZE);
    e.ProcessData((byte*)out, (byte*)in, size);
  } else {
    CBC_Mode< AES >::Decryption d;
    d.SetKeyWithIV(
        reinterpret_cast<const byte*>(secret.c_str()), CryptoAES_256_CBC::KEY_SIZE,
        reinterpret_cast<const byte*>(iv.c_str()), CryptoAES_256_CBC::IV_SIZE);
    d.ProcessData((byte*)out, (byte*)in, size);
  }
  return true;
}

#elif defined(USE_NSS)

bool CryptoAES_256_CBCKeyHandler::cbc_transform(
    const unsigned char* in,
    size_t size,
    unsigned char* out,
    bool encrypt,
    std::string *error) const
{
  int result = false;
  PK11SlotInfo *slot;
  SECItem keyItem;
  PK11SymKey *symkey;
  CK_AES_CBC_ENCRYPT_DATA_PARAMS cbc_params = {0};
  SECItem ivItem;
  SECItem *param;
  SECStatus ret;
  PK11Context *ectx;
  int written;

  slot = PK11_GetBestSlot(CKM_AES_CBC, NULL);
  if (slot) {
    keyItem.type = siBuffer;
    keyItem.data = reinterpret_cast<unsigned char*>(const_cast<char*>(secret.c_str()));
    keyItem.len = CryptoAES_256_CBC::KEY_SIZE;
    symkey = PK11_ImportSymKey(slot, CKM_AES_CBC, PK11_OriginUnwrap, CKA_UNWRAP, &keyItem, NULL);
    if (symkey) {
      memcpy(cbc_params.iv, iv.c_str(), CryptoAES_256_CBC::IV_SIZE);
      ivItem.type = siBuffer;
      ivItem.data = (unsigned char*)&cbc_params;
      ivItem.len = sizeof(cbc_params);

      param = PK11_ParamFromIV(CKM_AES_CBC, &ivItem);
      if (param) {
        ectx = PK11_CreateContextBySymKey(CKM_AES_CBC, encrypt?CKA_ENCRYPT:CKA_DECRYPT, symkey, param);
        if (ectx) {
          ret = PK11_CipherOp(ectx,
              out, &written, size,
              in, size);
          if ((ret == SECSuccess) && (written == (int)size)) {
            result = true;
          }
          PK11_DestroyContext(ectx, PR_TRUE);
        }
        SECITEM_FreeItem(param, PR_TRUE);
      }
      PK11_FreeSymKey(symkey);
    }
    PK11_FreeSlot(slot);
  }
  if (!result) {
    if (error != nullptr) {
      std::ostringstream ss;
      ss << "Failed to perform AES-CBC encryption: " << PR_GetError();
      *error = ss.str();
    }
  }
  return result;
}

#else
#error Must define USE_CRYPTOPP or USE_NSS
#endif

CryptoKeyHandler *CryptoAES_256_CBC::get_key_handler(
    const bufferptr& secret, const bufferptr& iv, string& error)
{
  if (secret.length() != KEY_SIZE) {
    error = "Improper key size.";
    return nullptr;
  }
  if (iv.length() != IV_SIZE) {
    error = "Improper iv size.";
    return nullptr;
  }
  CryptoAES_256_CBCKeyHandler *ckh = new CryptoAES_256_CBCKeyHandler(secret, iv);
  return ckh;
}

// --

class CryptoAES_256_ECBKeyHandler : public CryptoKeyHandler {
public:
  CryptoAES_256_ECBKeyHandler(const bufferptr& key)
  :secret(key) {}

  int encrypt(const bufferlist& in, bufferlist& out, std::string *error) const {
    return ecb_transform(in, out, true, error);
  }
  int decrypt(const bufferlist& in, bufferlist& out, std::string *error) const {
    return ecb_transform(in, out, false, error);
  }
private:
  bufferptr secret;
  int ecb_transform(
      const bufferlist& in,
      bufferlist& out,
      bool do_encrypt,
      std::string *error) const;
  bool ecb_transform(
      const unsigned char* in,
      size_t size,
      unsigned char* out,
      bool do_encrypt,
      std::string *error) const;
};

int CryptoAES_256_ECBKeyHandler::ecb_transform(
    const bufferlist& in,
    bufferlist& out,
    bool do_encrypt,
    std::string *error) const
{
  if ((in.length() % CryptoAES_256_ECB::BLOCK_SIZE) != 0) {
    if (error) {
      *error = "Input size is not multiple of 16";
    }
    return -EINVAL;
  }
  int result = -EINVAL;
  bufferptr p(in.length());
  bufferlist _in(in);
  if (ecb_transform(
      reinterpret_cast<const unsigned char*>(_in.c_str()),
      in.length(),
      reinterpret_cast<unsigned char*>(p.c_str()),
      do_encrypt,
      error)) {
    result = 0;
    out.append(p);
  }
  return result;
}

#ifdef USE_CRYPTOPP

bool CryptoAES_256_ECBKeyHandler::ecb_transform(
    const unsigned char* data_in,
    size_t data_size,
    unsigned char* data_out,
    bool do_encrypt,
    std::string *error) const
{
  using namespace CryptoPP;
  int res = false;
  if (do_encrypt) {
    try {
      ECB_Mode< AES >::Encryption e;
      e.SetKey(reinterpret_cast<const unsigned char*>(secret.c_str()), CryptoAES_256_ECB::KEY_SIZE);
      e.ProcessData(data_out, data_in, data_size);
      res = true;
    } catch( CryptoPP::Exception& ex ) {
      if (error) {
        std::ostringstream ss;
        ss << "AES-ECB encryption failed with: " << ex.GetWhat();
        *error = ss.str();
      }
    }
  } else {
    try {
      ECB_Mode< AES >::Decryption d;
      d.SetKey(reinterpret_cast<const unsigned char*>(secret.c_str()), CryptoAES_256_ECB::KEY_SIZE);
      d.ProcessData(data_out, data_in, data_size);
      res = true;
    } catch( CryptoPP::Exception& ex ) {
      if (error) {
        std::ostringstream ss;
        ss << "AES-ECB encryption failed with: " << ex.GetWhat();
        *error = ss.str();
      }
    }
  }
  return res;
}

#elif defined USE_NSS

bool CryptoAES_256_ECBKeyHandler::ecb_transform(
    const unsigned char* data_in,
    size_t data_size,
    unsigned char* data_out,
    bool do_encrypt,
    std::string *error) const
{
  bool result = false;
  PK11SlotInfo *slot;
  SECItem keyItem;
  PK11SymKey *symkey;
  SECItem *param;
  SECStatus ret;
  PK11Context *ectx;
  int written;
  unsigned int written2;
  slot = PK11_GetBestSlot(CKM_AES_ECB, NULL);
  if (slot) {
    keyItem.type = siBuffer;
    keyItem.data = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(secret.c_str()));
    keyItem.len = CryptoAES_256_ECB::KEY_SIZE;

    param = PK11_ParamFromIV(CKM_AES_ECB, NULL);
    if (param) {
      symkey = PK11_ImportSymKey(slot, CKM_AES_ECB, PK11_OriginUnwrap,
          CKA_UNWRAP, &keyItem, NULL);
      if (symkey) {
        ectx = PK11_CreateContextBySymKey(
            CKM_AES_ECB, do_encrypt?CKA_ENCRYPT:CKA_DECRYPT, symkey, param);
        if (ectx) {
          ret = PK11_CipherOp(ectx, data_out, &written,
              data_size, data_in, data_size);
          if (ret == SECSuccess) {
            ret = PK11_DigestFinal(ectx,
                data_out + written, &written2,
                data_size - written);
            if (ret == SECSuccess) {
              result = true;
            }
          }
          PK11_DestroyContext(ectx, PR_TRUE);
        }
        PK11_FreeSymKey(symkey);
      }
      SECITEM_FreeItem(param, PR_TRUE);
    }
    PK11_FreeSlot(slot);
  }
  if (!result) {
    if (error) {
      std::ostringstream ss;
      ss << "Failed to perform AES-ECB encryption: " << PR_GetError();
      *error = ss.str();
    }
  }
  return result;
}

#else
#error Must define USE_CRYPTOPP or USE_NSS
#endif

CryptoKeyHandler *CryptoAES_256_ECB::get_key_handler(
    const bufferptr& secret, const bufferptr& iv, string& error)
{
  if (iv.length() != 0) {
    error = "IV size must be 0 for ECB mode";
    return nullptr;
  }
  if (secret.length() != KEY_SIZE) {
    error = "Invalid key size";
    return nullptr;
  }
  CryptoAES_256_ECBKeyHandler *ckh = new CryptoAES_256_ECBKeyHandler(secret);
  return ckh;
}


// --


class CryptoAES_256_CTRKeyHandler : public CryptoKeyHandler {
public:
  CryptoAES_256_CTRKeyHandler(const bufferptr& key, const bufferptr& iv)
  :secret(key), iv(iv) {}

  int encrypt(
      const bufferlist& in,
      bufferlist& out,
      std::string *error) const {
    if (ctr_transform(in, out, error)) {
      return 0;
    } else {
      return -EINVAL;
    }
  }
  int decrypt(
      const bufferlist& in,
      bufferlist& out,
      std::string *error) const {
    return encrypt(in, out, error);
  }
private:
  bufferptr secret;
  bufferptr iv;
  bool ctr_transform(const bufferlist& in, bufferlist& out,
      std::string *error) const;
  int ctr_transform(
      const unsigned char* in, size_t size, unsigned char* out,
      std::string *error) const;
};

CryptoKeyHandler *CryptoAES_256_CTR::get_key_handler(
    const bufferptr& secret, const bufferptr& iv, string& error)
{
  if (secret.length() != KEY_SIZE) {
    error = "Improper key size.";
    return nullptr;
  }
  if (iv.length() != BLOCK_SIZE) {
    error = "Improper iv size.";
    return nullptr;
  }
  CryptoAES_256_CTRKeyHandler *ckh = new CryptoAES_256_CTRKeyHandler(secret, iv);
  return ckh;
}


#ifdef USE_CRYPTOPP
bool CryptoAES_256_CTRKeyHandler::ctr_transform(
      const bufferlist& input,
      bufferlist& output,
      std::string *error
      ) const {
    using namespace CryptoPP;
    size_t size = input.length();
    buffer::ptr buf(
        (size + CryptoAES_256_CTR::KEY_SIZE - 1)
        / CryptoAES_256_CTR::KEY_SIZE * CryptoAES_256_CTR::KEY_SIZE);

    if ((input.length() % CryptoAES_256_CTR::BLOCK_SIZE) != 0) {
      if (error) {
        *error = "Input size is not multiple of 16";
      }
      return false;
    }

    CTR_Mode< AES >::Encryption e;
    e.SetKeyWithIV( reinterpret_cast<const byte*>(secret.c_str()),
                    CryptoAES_256_CTR::KEY_SIZE,
                    reinterpret_cast<const byte*>(iv.c_str()),
                    CryptoAES_256_CTR::IV_SIZE);
    buf.zero();
    e.ProcessData((byte*)buf.c_str(), (byte*)buf.c_str(), buf.length());
    buf.set_length(size);

    off_t crypt_pos = 0;
    auto iter = input.buffers().begin();
    while (iter != input.buffers().end()) {
      off_t cnt = iter->length();
      byte* src = (byte*)iter->c_str();
      byte* dst = (byte*)buf.c_str() + crypt_pos;
      for (off_t i = 0; i < cnt; i++) {
        dst[i] ^= src[i];
      }
      ++iter;
      crypt_pos += cnt;
    }
    output.append(buf);
    return true;
  }

#elif defined(USE_NSS)
bool CryptoAES_256_CTRKeyHandler::ctr_transform(
      const bufferlist& input,
      bufferlist& output,
      std::string *error
      ) const {
    bool result = false;
    PK11SlotInfo *slot;
    SECItem keyItem;
    PK11SymKey *symkey;
    CK_AES_CTR_PARAMS ctr_params = {0};
    SECItem ivItem;
    SECItem *param;
    SECStatus ret;
    PK11Context *ectx;
    int written;
    unsigned int written2;
    size_t size = input.length();
    bufferlist _input(input);

    if ((input.length() % CryptoAES_256_CTR::BLOCK_SIZE) != 0) {
      if (error) {
        *error = "Input size is not multiple of 16";
      }
      return false;
    }

    slot = PK11_GetBestSlot(CKM_AES_CTR, NULL);
    if (slot) {
      keyItem.type = siBuffer;
      keyItem.data = const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(secret.c_str()));
      keyItem.len = CryptoAES_256_CTR::KEY_SIZE;

      symkey = PK11_ImportSymKey(slot, CKM_AES_CTR, PK11_OriginUnwrap, CKA_UNWRAP, &keyItem, NULL);
      if (symkey) {
        static_assert(sizeof(ctr_params.cb) >= CryptoAES_256_CTR::IV_SIZE, "Must fit counter");
        ctr_params.ulCounterBits = 128;
        memcpy(&ctr_params.cb, iv.c_str(), CryptoAES_256_CTR::IV_SIZE);
        ivItem.type = siBuffer;
        ivItem.data = (unsigned char*)&ctr_params;
        ivItem.len = sizeof(ctr_params);

        param = PK11_ParamFromIV(CKM_AES_CTR, &ivItem);
        if (param) {
          ectx = PK11_CreateContextBySymKey(CKM_AES_CTR, CKA_ENCRYPT, symkey, param);
          if (ectx) {
            buffer::ptr buf(
                (size + CryptoAES_256_CTR::KEY_SIZE - 1)
                / CryptoAES_256_CTR::KEY_SIZE * CryptoAES_256_CTR::KEY_SIZE);
            ret = PK11_CipherOp(ectx,
                                (unsigned char*)buf.c_str(), &written, buf.length(),
                                (unsigned char*)_input.c_str(), size);
            if (ret == SECSuccess) {
              ret = PK11_DigestFinal(ectx,
                                     (unsigned char*)buf.c_str() + written, &written2,
                                     buf.length() - written);
              if (ret == SECSuccess) {
                buf.set_length(written + written2);
                output.append(buf);
                result = true;
              }
            }
            PK11_DestroyContext(ectx, PR_TRUE);
          }
          SECITEM_FreeItem(param, PR_TRUE);
        }
        PK11_FreeSymKey(symkey);
      }
      PK11_FreeSlot(slot);
    }
    if (result == false) {
      if (error) {
        *error = "Failed to perform AES-CTR encryption: " + PR_GetError();
      }
    }
    return result;
  }

#else
#error Must define USE_CRYPTOPP or USE_NSS
#endif

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
    ckh.reset(ch->get_key_handler(s, AES_IV, error));
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
  using namespace ceph;
  switch (type) {
  case CEPH_CRYPTO_NONE:
    return new CryptoNone;
  case CEPH_CRYPTO_AES:
    return new CryptoAES;
  case CEPH_CRYPTO_AES_256_CBC:
      return new CryptoAES_256_CBC;
  case CEPH_CRYPTO_AES_256_ECB:
      return new CryptoAES_256_ECB;
  case CEPH_CRYPTO_AES_256_CTR:
      return new CryptoAES_256_CTR;
  default:
    return nullptr;
  }
}

}
