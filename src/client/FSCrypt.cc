
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/config.h"
#include "common/ceph_crypto.h"
#include "common/debug.h"
#include "include/ceph_assert.h"

#include "client/FSCrypt.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/core_names.h>

#include <string.h>

#define dout_context g_ceph_context

using ceph::crypto::HMACSHA512;
/*
 * base64 encode/decode.
 */



/* FIXME: this was copy pasted from common/armor.c with slight modification
 * as needed to use alternative translation table. Code can and should be
 * combined, but need to make sure we do it in a way that doesn't hurt
 * compiler optimizations in the general case.
 * Also relaxed decoding to make it compatible with the kernel client */
static const char *pem_key = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+,";

static int encode_bits(int c)
{
	return pem_key[c];
}

static int decode_bits(char c)
{
	if (c >= 'A' && c <= 'Z')
		return c - 'A';
	if (c >= 'a' && c <= 'z')
		return c - 'a' + 26;
	if (c >= '0' && c <= '9')
		return c - '0' + 52;
	if (c == '+' || c == '-')
		return 62;
	if (c == ',' || c == '/' || c == '_')
		return 63;
	if (c == '=')
		return 0; /* just non-negative, please */
	return -EINVAL;	
}

static int set_str_val(char **pdst, const char *end, char c)
{
	if (*pdst < end) {
		char *p = *pdst;
		*p = c;
		(*pdst)++;
	} else
		return -ERANGE;

	return 0;
}

static int b64_encode(char *dst, char * const dst_end, const char *src, const char *end)
{
        char *orig_dst = dst;
	int olen = 0;
	int line = 0;

#define SET_DST(c) do { \
	int __ret = set_str_val(&dst, dst_end, c); \
	if (__ret < 0) \
		return __ret; \
} while (0);

	while (src < end) {
		unsigned char a;

		a = *src++;
		SET_DST(encode_bits(a >> 2));
		if (src < end) {
			unsigned char b;
			b = *src++;
			SET_DST(encode_bits(((a & 3) << 4) | (b >> 4)));
			if (src < end) {
				unsigned char c;
				c = *src++;
				SET_DST(encode_bits(((b & 15) << 2) |
								(c >> 6)));
				SET_DST(encode_bits(c & 63));
			} else {
				SET_DST(encode_bits((b & 15) << 2));
				SET_DST('=');
			}
		} else {
			SET_DST(encode_bits(((a & 3) << 4)));
			SET_DST('=');
			SET_DST('=');
		}
		olen += 4;
		line += 4;
	}
	*dst = '\0';
	return (dst - orig_dst);
}

static char get_unarmor_src(const char *src, const char *end, int ofs)
{
  if (src + ofs < end) {
    return src[ofs];
  }
  return '=';
}

int b64_decode(char *dst, char * const dst_end, const char *src, const char *end)
{
	int olen = 0;

	while (src < end) {
		int a, b, c, d;

		if (src[0] == '\n') {
			src++;
			continue;
		}

		a = decode_bits(get_unarmor_src(src, end, 0));
		b = decode_bits(get_unarmor_src(src, end, 1));
		c = decode_bits(get_unarmor_src(src, end, 2));
		d = decode_bits(get_unarmor_src(src, end, 3));
		if (a < 0 || b < 0 || c < 0 || d < 0) {
			return -EINVAL;
                }

		SET_DST((a << 2) | (b >> 4));
		if (get_unarmor_src(src, end, 2) == '=')
			return olen + 1;
		SET_DST(((b & 15) << 4) | (c >> 2));
		if (get_unarmor_src(src, end, 3) == '=')
			return olen + 2;
		SET_DST(((c & 3) << 6) | d);
		olen += 3;
		src += 4;
	}
	return olen;
}

static int calc_hmac_sha512(const char *key, int key_len,
                             const char *msg, int msg_len,
                             char *dest, int dest_len)
{
  char hash_sha512[CEPH_CRYPTO_HMACSHA512_DIGESTSIZE];

  HMACSHA512 hmac((const unsigned char *)key, key_len);
  hmac.Update((const unsigned char *)msg, msg_len);
  hmac.Final((unsigned char *)hash_sha512);

  auto len = std::min(dest_len, CEPH_CRYPTO_HMACSHA512_DIGESTSIZE);

  memcpy(dest, hash_sha512, len);

  return len;
}

#define SALT_LEN_DEFAULT 32

static char default_salt[SALT_LEN_DEFAULT] = { 0 };

static int hkdf_extract(const char *_salt, int salt_len,
                         const char *ikm, int ikm_len,
                         char *dest, int dest_len) {
  const char *salt = _salt;
  if (!_salt) {
    salt = default_salt;
    salt_len = SALT_LEN_DEFAULT;
  }

  return calc_hmac_sha512(salt, salt_len, ikm, ikm_len, dest, dest_len);
}

static int hkdf_expand(const char *data, int data_len,
                       const char *info, int info_len,
                       char *dest, int dest_len)
{
  int total_len = 0;

  char info_buf[info_len + 16];
  memcpy(info_buf, info, info_len);

  char *p = dest;

  for (char i = 1; total_len < dest_len; i++) {
    *(char *)(info_buf + info_len) =  i;

    int r = calc_hmac_sha512(data, data_len,
                         info_buf, info_len  + 1,
                         p, dest_len - total_len);
    if (r < 0) {
      return r;
    }
    if (r == 0) {
      return -EINVAL;
    }

    total_len += r;
  }

  return total_len;
}

int fscrypt_fname_unarmor(const char *src, int src_len,
                          char *result, int max_len)
{
  return b64_decode(result, result + max_len,
                    src, src + src_len);
}

int fscrypt_decrypt_fname(const uint8_t *enc, int enc_len,
                          uint8_t *key, uint8_t *iv,
                          uint8_t *result)
{
    EVP_CIPHER *cipher = EVP_CIPHER_fetch(NULL, "AES-256-CBC-CTS", NULL);
    EVP_CIPHER_CTX *ctx;
    OSSL_PARAM params[2] = {
	    OSSL_PARAM_construct_utf8_string(OSSL_CIPHER_PARAM_CTS_MODE, (char *)"CS3", 0),
	    OSSL_PARAM_construct_end()
    };

    int total_len;

    if(!(ctx = EVP_CIPHER_CTX_new())) {
      return -EIO;
    }

    if (!EVP_CipherInit_ex2(ctx, cipher, key, iv,
			    0, params)) {
      return -EINVAL;
    }

    int len;

    if (EVP_DecryptUpdate(ctx, result, &len, enc, enc_len) != 1) {
      return -EINVAL;
    }

    total_len = len;

    if(EVP_DecryptFinal_ex(ctx, result + len, &len) != 1) {
      return -EINVAL;
    }

    total_len += len;

    /* Clean up */
    EVP_CIPHER_CTX_free(ctx);

    return total_len;
}

int fscrypt_calc_hkdf(char hkdf_context,
                      const char *nonce, int nonce_len,
                      const char *salt, int salt_len,
                      const char *key, int key_len,
                      char *dest, int dest_len)
{
  char extract_buf[CEPH_CRYPTO_HMACSHA512_DIGESTSIZE];
  int r = hkdf_extract(salt, salt_len,
                       key, key_len,
                       extract_buf, sizeof(extract_buf));
  if (r < 0) {
    return r;
  }

  int extract_len = r;

#define FSCRYPT_INFO_STR "fscrypt\x00?"

  char info_str[sizeof(FSCRYPT_INFO_STR) + nonce_len];

  int len = sizeof(FSCRYPT_INFO_STR) - 1;
  memcpy(info_str, FSCRYPT_INFO_STR, len);

  info_str[len - 1] = hkdf_context;

  if (nonce && nonce_len) {
    memcpy(info_str + len, nonce, nonce_len);
    len += nonce_len;
  }

  r =  hkdf_expand(extract_buf, extract_len,
                   info_str, len,
                   dest, dest_len);

  return r;
}

static std::string hex_str(const void *p, int len)
{
  bufferlist bl;
  bl.append_hole(len);
  memcpy(bl.c_str(), p, len);
  std::stringstream ss;
  bl.hexdump(ss);
  return ss.str();
}

std::ostream& operator<<(std::ostream& out, const ceph_fscrypt_key_identifier& kid) {
  out << hex_str(kid.raw, sizeof(kid.raw));
  return out;
}

int FSCryptKey::init(const char *k, int klen) {
  int r = fscrypt_calc_hkdf(HKDF_CONTEXT_KEY_IDENTIFIER,
                            nullptr, 0, /* nonce */
                            nullptr, 0, /* salt */
                            (const char *)k, klen,
                            identifier.raw, sizeof(identifier.raw));
  if (r < 0) {
    return r;
  }

  key.append_hole(klen);
  memcpy(key.c_str(), k, klen);

  return 0;
}

int FSCryptKey::calc_hkdf(char ctx_identifier,
                          const char *nonce, int nonce_len,
                          char *result, int result_len) {
  int r = fscrypt_calc_hkdf(ctx_identifier,
                            nonce, nonce_len, /* nonce */
                            nullptr, 0, /* salt */
                            (const char *)key.c_str(), key.length(),
                            result, result_len);
  if (r < 0) {
    return r;
  }

  return 0;
}

int ceph_fscrypt_key_identifier::init(const char *k, int klen) {
  if (klen != sizeof(raw)) {
    return -EINVAL;
  }
  memcpy(raw, k, klen);

  return 0;
}

int ceph_fscrypt_key_identifier::init(const struct fscrypt_key_specifier& k) {
  if (k.type != FSCRYPT_KEY_SPEC_TYPE_IDENTIFIER) {
    return -ENOTSUP;
  }

  return init((const char *)k.u.identifier, sizeof(k.u.identifier));
}

bool ceph_fscrypt_key_identifier::operator<(const struct ceph_fscrypt_key_identifier& r) const {
  return (memcmp(raw, r.raw, sizeof(raw)) < 0);
}

void FSCryptContext::generate_iv(uint64_t block_num, FSCryptIV& iv) const
{
  memset(&iv, 0, sizeof(iv));

  // memcpy(iv.u.nonce, nonce, FSCRYPT_FILE_NONCE_SIZE);
  iv.u.block_num = block_num;
}

void FSCryptKeyHandler::reset(int64_t _epoch, FSCryptKeyRef k)
{
  std::unique_lock wl{lock};
  epoch = _epoch;
  key = k;
}

int64_t FSCryptKeyHandler::get_epoch()
{
  std::shared_lock rl{lock};
  return epoch;
}

FSCryptKeyRef& FSCryptKeyHandler::get_key()
{
  std::shared_lock rl{lock};
  return key;
}

int FSCryptKeyStore::create(const char *k, int klen, FSCryptKeyHandlerRef& key_handler)
{
  auto key = std::make_shared<FSCryptKey>();

  int r = key->init(k, klen);
  if (r < 0) {
    return r;
  }

  std::unique_lock wl{lock};

  const auto& id = key->get_identifier();
  
  auto iter = m.find(id);
  if (iter != m.end()) {
    /* found a key handler entry, check that there is a key there */
    key_handler = iter->second;
    if (key_handler->get_key()) {
      return -EEXIST;
    }
    key_handler->reset(++epoch, key);
  } else {
    key_handler = std::make_shared<FSCryptKeyHandler>(++epoch, key);
    m[id] = key_handler;
  }

  return 0;
}

int FSCryptKeyStore::_find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyHandlerRef& kh)
{
  auto iter = m.find(id);
  if (iter == m.end()) {
    return -ENOENT;
  }

  kh = iter->second;

  return 0;
}

int FSCryptKeyStore::find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyHandlerRef& kh)
{
  std::shared_lock rl{lock};

  return _find(id, kh);
}

int FSCryptKeyStore::invalidate(const struct ceph_fscrypt_key_identifier& id)
{
  std::unique_lock rl{lock};

  FSCryptKeyHandlerRef kh;
  int r = _find(id, kh);
  if (r == -ENOENT) {
    return 0;
  } else if (r < 0) {
    return r;
  }

  kh->reset(++epoch, nullptr);

  m.erase(id);

  return 0;
}

FSCryptKeyValidator::FSCryptKeyValidator(CephContext *cct, FSCryptKeyHandlerRef& kh, int64_t e) : cct(cct), handler(kh), epoch(e) {
}

bool FSCryptKeyValidator::is_valid() const {
  return (handler->get_epoch() == epoch);
}

FSCryptDenc::FSCryptDenc(EVP_CIPHER *cipher, std::vector<OSSL_PARAM> params) : cipher(cipher),
                                                                               cipher_ctx(EVP_CIPHER_CTX_new()),
                                                                               cipher_params(std::move(params)) {}

FSCryptFNameDenc::FSCryptFNameDenc(): FSCryptDenc(EVP_CIPHER_fetch(NULL, "AES-256-CBC-CTS", NULL),
  { OSSL_PARAM_construct_utf8_string(OSSL_CIPHER_PARAM_CTS_MODE, (char *)"CS3", 0),
                                                 OSSL_PARAM_construct_end()} ) {}

FSCryptFDataDenc::FSCryptFDataDenc(): FSCryptDenc(EVP_CIPHER_fetch(NULL, "AES-256-XTS", NULL), {} ) {}

void FSCryptDenc::setup(FSCryptContextRef& _ctx,
                        FSCryptKeyRef& _master_key)
{
  ctx = _ctx;
  master_key = _master_key;
}

int FSCryptDenc::calc_key(char ctx_identifier,
                          int key_size,
                          uint64_t block_num)
{
  key.resize(key_size);
  int r = master_key->calc_hkdf(ctx_identifier,
                                (const char *)ctx->nonce, sizeof(ctx->nonce),
                                key.data(), key_size);
  if (r < 0) {
    return r;
  }
  ctx->generate_iv(block_num, iv);

  return 0;
}

int FSCryptDenc::decrypt(const char *in_data, int in_len,
                         char *out_data, int out_len)
{
    int total_len;

    int key_size = key.size();

    if (out_len < (fscrypt_align_ofs(in_len))) {
      return -ERANGE;
    }

    if (!EVP_CipherInit_ex2(cipher_ctx, cipher, (const uint8_t *)key.data(), iv.raw,
			    0, cipher_params.data())) {
      return -EINVAL;
    }

    int len;

    if (EVP_DecryptUpdate(cipher_ctx, (uint8_t *)out_data, &len, (const uint8_t *)in_data, in_len) != 1) {
      return -EINVAL;
    }

    total_len = len;

    if (EVP_DecryptFinal_ex(cipher_ctx, (uint8_t *)out_data + len, &len) != 1) {
      return -EINVAL;
    }

    total_len += len;

    return total_len;
}

FSCryptDenc::~FSCryptDenc()
{
  EVP_CIPHER_CTX_free(cipher_ctx);
}

FSCryptDencRef FSCrypt::init_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv,
                                  std::function<FSCryptDenc *()> gen_denc)
{
  if (!ctx) {
    return nullptr;
  }

  FSCryptKeyHandlerRef master_kh;
  int r = key_store.find(ctx->master_key_identifier, master_kh);
  if (r == 0) {
    generic_dout(0) << __FILE__ << ":" << __LINE__ << ": fscrypt_key handler found" << dendl;
  } else if (r == -ENOENT) {
    generic_dout(0) << __FILE__ << ":" << __LINE__ << ": fscrypt_key handler not found" << dendl;
    return nullptr;
  } else {
    generic_dout(0) << __FILE__ << ":" << __LINE__ << ": error: r=" << r << dendl;
    return nullptr;
  }

  if (kv) {
    *kv = make_shared<FSCryptKeyValidator>(cct, master_kh, master_kh->get_epoch());
  }

  auto& master_key = master_kh->get_key();

  if (!master_key) {
    generic_dout(0) << __FILE__ << ":" << __LINE__ << ": fscrypt_key key is null" << dendl;
    return nullptr;
  }

  auto fscrypt_denc = std::shared_ptr<FSCryptDenc>(gen_denc());

  fscrypt_denc->setup(ctx, master_key);

  return fscrypt_denc;
}

FSCryptDencRef FSCrypt::get_fname_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv, bool calc_key)
{
  auto denc = init_denc(ctx, kv,
                         []() { return new FSCryptFNameDenc(); });
  if (!denc) {
    return nullptr;
  }

  if (calc_key) {
    int r = denc->calc_fname_key();
    if (r < 0) {
      generic_dout(0) << __FILE__ << ":" << __LINE__ << ": failed to init dencoder: r=" << r << dendl;
      return nullptr;
    }
  }

  return denc;
}

FSCryptDencRef FSCrypt::get_fdata_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv)
{
  return init_denc(ctx, kv,
                   []() { return new FSCryptFDataDenc(); });
}

