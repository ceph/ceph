
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
#include "auth/Crypto.h"

#include "client/FSCrypt.h"

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/core_names.h>

#include <string.h>

#define dout_subsys ceph_subsys_client


using ceph::crypto::HMACSHA512;
/*
 * base64 encode/decode.
 */

#define CEPH_NOHASH_NAME_MAX (180 - CEPH_CRYPTO_SHA256_DIGESTSIZE)



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
			}
		} else {
			SET_DST(encode_bits(((a & 3) << 4)));
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

int fscrypt_fname_armor(const char *src, int src_len,
                        char *result, int max_len)
{
  return b64_encode(result, result + max_len,
                    src, src + src_len);
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

//#define FSCRYPT_INFO_STR "fscrypt\x00\x01"
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
    return -EINVAL;
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

void FSCryptContext::generate_new_nonce()
{
  cct->random()->get_bytes((char *)nonce, sizeof(nonce));
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

//taken from fs/crypto/keyring.h
bool FSCryptKeyStore::valid_key_spec(const struct fscrypt_key_specifier& k)
{
  if (k.__reserved)
    return false;
  return master_key_spec_len(k) != 0;
}

//taken from fs/crypto/fscrypt_private.h
int FSCryptKeyStore::master_key_spec_len(const struct fscrypt_key_specifier& spec)
{
        switch (spec.type) {
        case FSCRYPT_KEY_SPEC_TYPE_DESCRIPTOR:
                return FSCRYPT_KEY_DESCRIPTOR_SIZE;
        case FSCRYPT_KEY_SPEC_TYPE_IDENTIFIER:
                return FSCRYPT_KEY_IDENTIFIER_SIZE;
        }
        return 0;
}



int FSCryptKeyStore::maybe_add_user(std::list<int>* users, int user)
{
  ldout(cct, 10) << __FILE__ << ":" << __LINE__ << " user=" << user << dendl;

  auto it = std::find(users->begin(), users->end(), user);
  if (it != users->end()) {
    ldout(cct, 10) << "maybe_add_user user already added!" << dendl;
    return -EEXIST;
  }
  
  ldout(cct, 10) << "maybe_add_user is not found!, adding" << dendl;
  users->push_back(user);
  ldout(cct, 10) << "maybe_add_user size is now=" << users->size() << dendl;
  return 0;
}

int FSCryptKeyStore::maybe_remove_user(struct fscrypt_remove_key_arg* arg, std::list<int>* users, int user)
{
  ldout(cct, 10) << __FILE__ << ":" << __LINE__ << " user=" << user << dendl;
  uint32_t status_flags = 0;
  int err = 0;
  bool removed = false;
  if (!valid_key_spec(arg->key_spec)) {
    return -EINVAL;
  }

  auto it = std::find(users->begin(), users->end(), user);
  if (it != users->end()) {
    ldout(cct, 10) << "maybe_remove_user, user found removing!" << dendl;
    removed = true;
    users->erase(it);
  } else {
    return -EUSERS;
  }
  ldout(cct, 10) << "maybe_add_user size is now=" << users->size() << dendl;

  if (users->size() != 0 && removed) {

    //set bits for removed for requested user
    status_flags |= FSCRYPT_KEY_REMOVAL_STATUS_FLAG_OTHER_USERS;
    err = -EBUSY;
  }

  arg->removal_status_flags = status_flags;
  return err;
}

int FSCryptKeyStore::create(const char *k, int klen, FSCryptKeyHandlerRef& key_handler, int user)
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

    auto& users = key_handler->get_users();
    r = maybe_add_user(&users, user);

    if (r == -EEXIST) {
      return 0; //returns 0 regardless
    }
    key_handler->reset(++epoch, key);
  } else {
    key_handler = std::make_shared<FSCryptKeyHandler>(++epoch, key);

    auto& users = key_handler->get_users();
    r = maybe_add_user(&users, user);
    if (r == -EEXIST) {
      return 0; //returns 0 regardless
    }

    m[id] = key_handler;
  }

  return 0;
}

int FSCryptKeyStore::_find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyHandlerRef& kh)
{
  auto iter = m.find(id);
  if (iter == m.end()) {
    return -ENOKEY;
  }

  kh = iter->second;

  return 0;
}

int FSCryptKeyStore::find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyHandlerRef& kh)
{
  std::shared_lock rl{lock};

  return _find(id, kh);
}

int FSCryptKeyStore::invalidate(struct fscrypt_remove_key_arg* arg, int user)
{
  std::unique_lock rl{lock};

  ceph_fscrypt_key_identifier id;
  int r = id.init(arg->key_spec);
  if (r < 0) {
    return r;
  }

  FSCryptKeyHandlerRef kh;
  r = _find(id, kh);
  if (r < 0) {
    return r;
  }

  auto& users = kh->get_users();
  r = maybe_remove_user(arg, &users, user);
  if (r < 0) {
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

FSCryptDenc::FSCryptDenc(CephContext *_cct) : cct(_cct), cipher_ctx(EVP_CIPHER_CTX_new()) {}

void FSCryptDenc::init_cipher(EVP_CIPHER *_cipher, std::vector<OSSL_PARAM> params)
{
  cipher = _cipher;
  cipher_params = std::move(params);
}

struct fscrypt_cipher_opt {
  const char *str;
  bool cts_mode{false};
  bool essiv{false};
  int key_size;
  int iv_size;
};

static std::map<int, fscrypt_cipher_opt> cipher_opt_map = {
  {
    FSCRYPT_MODE_AES_256_XTS, {
      .str = "AES-256-XTS",
      .key_size = 64,
      .iv_size = 16,
    }
  },
  {
    FSCRYPT_MODE_AES_256_CTS, {
      .str = "AES-256-CBC-CTS",
      .cts_mode = true,
      .key_size = 32,
      .iv_size = 16,
    }
  },
};

bool FSCryptDenc::do_setup_cipher(int enc_mode)
{
  auto iter = cipher_opt_map.find(enc_mode);
  if (iter == cipher_opt_map.end()) {
    return false;
  }

  auto& opts = iter->second;
  if (opts.cts_mode) {
    init_cipher(EVP_CIPHER_fetch(NULL, opts.str, NULL),
                { OSSL_PARAM_construct_utf8_string(OSSL_CIPHER_PARAM_CTS_MODE, (char *)"CS3", 0),
                OSSL_PARAM_construct_end()} );
  } else {
    init_cipher(EVP_CIPHER_fetch(NULL, opts.str, NULL), {} );
  }

  padding = 4 << (ctx->flags & FSCRYPT_POLICY_FLAGS_PAD_MASK);
  key_size = opts.key_size;
  iv_size = opts.iv_size;

  return true;
}

bool FSCryptFNameDenc::setup_cipher()
{
  return do_setup_cipher(ctx->filenames_encryption_mode);
}

bool FSCryptFDataDenc::setup_cipher()
{
  return do_setup_cipher(ctx->contents_encryption_mode);
}

bool FSCryptDenc::setup(FSCryptContextRef& _ctx,
                        FSCryptKeyRef& _master_key)
{
  ctx = _ctx;
  master_key = _master_key;

  return setup_cipher();
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

static void sha256(const char *buf, int len, char *hash)
{   
  ceph::crypto::ssl::SHA256 hasher;
  hasher.Update((const unsigned char *)buf, len);
  hasher.Final((unsigned char *)hash);
}   

int FSCryptDenc::decrypt(const char *in_data, int in_len,
                         char *out_data, int out_len)
{
  int total_len;

  if ((int)key.size() != key_size) {
    ldout(cct, 0) << "ERROR: unexpected encryption key size: " << key.size() << " (expected: " << key_size << ")" << dendl;
    return -EINVAL;
  }

  if ((uint64_t)out_len < (fscrypt_align_ofs(in_len))) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << dendl;
    return -ERANGE;
  }

  if (!EVP_CipherInit_ex2(cipher_ctx, cipher, (const uint8_t *)key.data(), iv.raw,
                          0, cipher_params.data())) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << dendl;
    return -EINVAL;
  }

  int len;

  if (EVP_DecryptUpdate(cipher_ctx, (uint8_t *)out_data, &len, (const uint8_t *)in_data, in_len) != 1) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << dendl;
    return -EINVAL;
  }

  total_len = len;

    int ret = EVP_DecryptFinal_ex(cipher_ctx, (uint8_t *)out_data + len, &len);
    if (ret != 1) {
      return -EINVAL;
    }

    total_len += len;

    return total_len;
}

int FSCryptDenc::encrypt(const char *in_data, int in_len,
                         char *out_data, int out_len)
{
    int total_len;

    if ((int)key.size() != key_size) {
      ldout(cct, 0) << "ERROR: unexpected encryption key size: " << key.size() << " (expected: " << key_size << ")" << dendl;
      return -EINVAL;
    }

    if ((uint64_t)out_len < (fscrypt_align_ofs(in_len))) {
      return -ERANGE;
    }

    if (!EVP_CipherInit_ex2(cipher_ctx, cipher, (const uint8_t *)key.data(), iv.raw,
			    1, cipher_params.data())) {
      return -EINVAL;
    }

    int len;

    if (EVP_EncryptUpdate(cipher_ctx, (uint8_t *)out_data, &len, (const uint8_t *)in_data, in_len) != 1) {
      return -EINVAL;
    }

    total_len = len;

    if (EVP_EncryptFinal_ex(cipher_ctx, (uint8_t *)out_data + len, &len) != 1) {
      return -EINVAL;
    }

    total_len += len;

    return total_len;
}

FSCryptDenc::~FSCryptDenc()
{
  EVP_CIPHER_CTX_free(cipher_ctx);
}

int FSCryptFNameDenc::get_encrypted_fname(const std::string& plain, std::string *encrypted, std::string *alt_name)
{
  auto plain_size = plain.size();
  int dec_size = (plain.size() + padding - 1) & ~(padding - 1); // FIXME, need to be based on policy
  if (dec_size > NAME_MAX) {
    dec_size = NAME_MAX;
  }

  char orig[dec_size];
  memcpy(orig, plain.c_str(), plain_size);
  memset(orig + plain_size, 0, dec_size - plain_size);

  char enc_name[NAME_MAX + 64]; /* some extra just in case */
  int r = encrypt(orig, dec_size,
                  enc_name, sizeof(enc_name));

  if (r < 0) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ": failed to encrypt filename" << dendl;
    return r;
  }

  int enc_len = r;

  if (enc_len > CEPH_NOHASH_NAME_MAX) {
    *alt_name = std::string(enc_name, enc_len);
    char hash[CEPH_CRYPTO_SHA256_DIGESTSIZE];
    char *extra = enc_name + CEPH_NOHASH_NAME_MAX;

    /* hash the extra bytes and overwrite crypttext beyond that point with it */
    int extra_len = enc_len - CEPH_NOHASH_NAME_MAX;
    sha256(extra, extra_len, hash);
    memcpy(extra, hash, sizeof(hash));
    enc_len = CEPH_NOHASH_NAME_MAX + sizeof(hash);
  } else {
    alt_name->clear();
  }

  int b64_len = NAME_MAX * 2; // name.size() * 2;
  char b64_name[b64_len]; // large enough
  int len = fscrypt_fname_armor(enc_name, enc_len, b64_name, b64_len);

  *encrypted = std::string(b64_name, len);

  return len;
}

int FSCryptFNameDenc::get_decrypted_fname(const std::string& b64enc, const std::string& alt_name, std::string *decrypted)
{
  char enc[NAME_MAX];
  int len = alt_name.size();

  const char *penc = (len == 0 ? enc : alt_name.c_str());

  if (len == 0) {
    len = fscrypt_fname_unarmor(b64enc.c_str(), b64enc.size(),
                                enc, sizeof(enc));
  }

  char dec_fname[NAME_MAX + 64]; /* some extra just in case */
  int r = decrypt(penc, len, dec_fname, sizeof(dec_fname));

  if (r >= 0) {
    dec_fname[r] = '\0';
    *decrypted = dec_fname;
  } else {
    return r;
  }

  return r;
}

struct fscrypt_slink_data {
  ceph_le16 len;
  char enc[NAME_MAX - 2];
};

int FSCryptFNameDenc::get_encrypted_symlink(const std::string& plain, std::string *encrypted)
{
  auto plain_size = plain.size();
  int dec_size = (plain.size() + 31) & ~31; // FIXME, need to be based on policy

  char orig[dec_size];
  memcpy(orig, plain.c_str(), plain_size);
  memset(orig + plain_size, 0, dec_size - plain_size);

  fscrypt_slink_data slink_data;
  int r = encrypt(orig, dec_size,
                  slink_data.enc, sizeof(slink_data.enc));

  if (r < 0) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ": failed to encrypt filename" << dendl;
    return r;
  }

  slink_data.len = r;

  int b64_len = NAME_MAX * 2; // name.size() * 2;
  char b64_name[b64_len]; // large enough
  int len = fscrypt_fname_armor((const char *)&slink_data, slink_data.len + sizeof(slink_data.len), b64_name, b64_len);

  *encrypted = std::string(b64_name, len);

  return len;
}

int FSCryptFNameDenc::get_decrypted_symlink(const std::string& b64enc, std::string *decrypted)
{
  fscrypt_slink_data slink_data;

  int len = fscrypt_fname_unarmor(b64enc.c_str(), b64enc.size(),
                                  (char *)&slink_data, sizeof(slink_data));

  char dec_fname[NAME_MAX + 64]; /* some extra just in case */

  if (slink_data.len > len) { /* should never happen */
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "(): ERROR: slink_data.len greater than decrypted buffer (slink_data.len=" << slink_data.len << ", len=" << len << ")" << dendl;
    return -EIO;
  }

  int r = decrypt(slink_data.enc, slink_data.len, dec_fname, sizeof(dec_fname));

  if (r >= 0) {
    dec_fname[r] = '\0';
    *decrypted = dec_fname;
  } else {
    return r;
  }

  return r;
}

int FSCryptFDataDenc::decrypt_bl(uint64_t off, uint64_t len, uint64_t pos, const std::vector<Segment>& holes, bufferlist *bl)
{
  auto data_len = bl->length();

  auto target_end = off + len;

  bufferlist newbl;

  uint64_t end = off + data_len;
  uint64_t cur_block = fscrypt_block_from_ofs(pos);
  uint64_t block_off = fscrypt_block_start(pos);

  uint64_t start_block_off = block_off;

  auto hiter = holes.begin();

  while (pos < target_end) {
    bool has_hole = false;

    while (hiter != holes.end()) {
      uint64_t hofs = hiter->first;
      uint64_t hlen = hiter->second;
      uint64_t hend = hofs + hlen - 1;

      if (hend < pos) {
        ++hiter;
        continue;
      }

      if (hofs >= target_end) {
        hiter = holes.end();
        break;
      }

      has_hole = true;
      break;
    }
#warning is this the way to do it?
    uint64_t needed_pos = (pos > off ? pos : off);
    void *data_pos = bl->c_str() + needed_pos - start_block_off;
    if (!has_hole && *(uint64_t *)data_pos == 0) {
      has_hole = true;
    }

    uint64_t read_end = std::min(end, block_off + FSCRYPT_BLOCK_SIZE);
    uint64_t read_end_aligned = fscrypt_align_ofs(read_end);
    auto chunk_len = read_end_aligned - block_off;

    bufferlist chunk;

    if (!has_hole) {
      int r = calc_fdata_key(cur_block);
      if (r  < 0) {
        break;
      }

      /* since writes are aligned to block size, if there is a hole then it covers the whole block */

      chunk.append_hole(chunk_len);

      uint64_t bl_off = pos - start_block_off;
      r = decrypt(bl->c_str() + bl_off, chunk_len,
                  chunk.c_str(), chunk_len);
      if (r < 0) {
        return r;
      }
    } else {
      chunk.append_zero(chunk_len);
    }

    uint64_t needed_end = std::min(target_end, read_end);
    int needed_len = needed_end - needed_pos;
    chunk.splice(fscrypt_ofs_in_block(needed_pos), needed_len, &newbl);

    pos = read_end;
    ++cur_block;
    block_off += FSCRYPT_BLOCK_SIZE;
  }

  bl->swap(newbl);

  return 0;
}

int FSCryptFDataDenc::encrypt_bl(uint64_t off, uint64_t len, bufferlist& bl, bufferlist *encbl)
{
  if (off != fscrypt_block_start(off)) {
    return -EINVAL;
  }

  auto pos = off;
  auto target_end = off + len;
  auto target_end_block_ofs = fscrypt_block_start(target_end - 1);

  target_end = target_end_block_ofs + FSCRYPT_BLOCK_SIZE;

  if (bl.length() < target_end - off) {
    /* fill in zeros at the end if last block is partial */
    bl.append_zero(target_end - off - bl.length());
  }

  auto data_len = bl.length();

  bufferlist newbl;

  uint64_t end = off + data_len;
  uint64_t cur_block = fscrypt_block_from_ofs(pos);
  uint64_t block_off = fscrypt_block_start(pos);

  while (pos < target_end) {
    uint64_t write_end = std::min(end, block_off + FSCRYPT_BLOCK_SIZE);
    uint64_t write_end_aligned = fscrypt_align_ofs(write_end);
    auto chunk_len = write_end_aligned - block_off;

    int r = calc_fdata_key(cur_block);
    if (r  < 0) {
      break;
    }

    bufferlist chunk;
    chunk.append_hole(chunk_len);

    r = encrypt(bl.c_str() + pos - off, chunk_len,
                chunk.c_str(), chunk_len);
    if (r < 0) {
      return r;
    }

    newbl.claim_append(chunk);

    pos = write_end;
    ++cur_block;
    block_off += FSCRYPT_BLOCK_SIZE;
  }

  encbl->swap(newbl);

  return 0;
}

FSCryptContextRef FSCrypt::init_ctx(const std::vector<unsigned char>& fscrypt_auth)
{
  if (fscrypt_auth.size() == 0) {
    return nullptr;
  }

  FSCryptContextRef ctx = std::make_shared<FSCryptContext>(cct);

  bufferlist bl;
  bl.append((const char *)fscrypt_auth.data(), fscrypt_auth.size());

  auto bliter = bl.cbegin();
  try {
    ctx->decode(bliter);
  } catch (buffer::error& err) {
    if (fscrypt_auth.size()) {
      ldout(cct, 0) << __func__ << " " << " failed to decode fscrypt_auth:" << fscrypt_hex_str(fscrypt_auth.data(), fscrypt_auth.size()) << dendl;
    } else {
      ldout(cct, 0) << __func__ << " " << " failed to decode fscrypt_auth: fscrypt_auth.size() == 0"  << dendl;
    }
    return nullptr;
  }

  return ctx;
}

FSCryptDenc *FSCrypt::init_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv,
                                  std::function<FSCryptDenc *()> gen_denc)
{
  if (!ctx) {
    return nullptr;
  }

  FSCryptKeyHandlerRef master_kh;
  int r = key_store.find(ctx->master_key_identifier, master_kh);
  if (r == 0) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ": fscrypt_key handler found" << dendl;
  } else if (r == -ENOENT) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ": fscrypt_key handler not found" << dendl;
    return nullptr;
  } else {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ": error: r=" << r << dendl;
    return nullptr;
  }

  if (kv) {
    *kv = make_shared<FSCryptKeyValidator>(cct, master_kh, master_kh->get_epoch());
  }

  auto& master_key = master_kh->get_key();

  if (!master_key) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ": fscrypt_key key is null" << dendl;
    return nullptr;
  }

  auto fscrypt_denc = gen_denc();

  if (!fscrypt_denc->setup(ctx, master_key)) {
    ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ":" << __func__ << "(): ERROR: failed to setup denc" << dendl;
    return nullptr;
  }

  return fscrypt_denc;
}

FSCryptFNameDencRef FSCrypt::get_fname_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv, bool calc_key)
{
  auto pdenc = init_denc(ctx, kv,
                         [&]() { return new FSCryptFNameDenc(cct); });
  if (!pdenc) {
    return nullptr;
  }

  auto denc = std::shared_ptr<FSCryptFNameDenc>((FSCryptFNameDenc *)pdenc);

  if (calc_key) {
    int r = denc->calc_fname_key();
    if (r < 0) {
      ldout(cct, 0) << __FILE__ << ":" << __LINE__ << ": failed to init dencoder: r=" << r << dendl;
      return nullptr;
    }
  }

  return denc;
}

FSCryptFDataDencRef FSCrypt::get_fdata_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv)
{
  auto pdenc = init_denc(ctx, kv,
                         [&]() { return new FSCryptFDataDenc(cct); });
  if (!pdenc) {
    return nullptr;
  }

  return std::shared_ptr<FSCryptFDataDenc>((FSCryptFDataDenc *)pdenc);
}

void FSCrypt::prepare_data_read(FSCryptContextRef& ctx,
                                FSCryptKeyValidatorRef *kv,
                                uint64_t off,
                                uint64_t len,
                                uint64_t file_raw_size,
                                uint64_t *read_start,
                                uint64_t *read_len,
                                FSCryptFDataDencRef *denc)
{
  *denc = get_fdata_denc(ctx, kv);

  auto& fscrypt_denc = *denc;

  *read_start = (!fscrypt_denc ? off : fscrypt_block_start(off));
  uint64_t end = off + len;
  if (fscrypt_denc) {
    end = fscrypt_align_ofs(end);
  }
  *read_len = end - *read_start;
}
