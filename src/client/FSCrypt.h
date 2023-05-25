#pragma once

#include "fscrypt_uapi.h"

#include "common/ceph_mutex.h"

#include <map>

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/core_names.h>

#define FSCRYPT_FILE_NONCE_SIZE 16

#define HKDF_CONTEXT_KEY_IDENTIFIER 1
#define HKDF_CONTEXT_PER_FILE_ENC_KEY 2

static inline std::string fscrypt_hex_str(const void *p, int len)
{
  if (!p) {
    return "<null>";
  }

  bufferlist bl;
  bl.append_hole(len);
  memcpy(bl.c_str(), p, len);
  std::stringstream ss;
  bl.hexdump(ss);
  return ss.str();
}

int fscrypt_fname_unarmor(const char *src, int src_len,
                          char *result, int max_len);
int fscrypt_decrypt_fname(const uint8_t *enc, int enc_len,
                          uint8_t *key, uint8_t *iv,
                          uint8_t *result);

int fscrypt_calc_hkdf(char hkdf_context,
                      const char *nonce, int nonce_len,
                      const char *salt, int salt_len,
                      const char *key, int key_len,
                      char *dest, int dest_len);


struct ceph_fscrypt_key_identifier {
#define FSCRYPT_KEY_IDENTIFIER_LEN 16
  char raw[FSCRYPT_KEY_IDENTIFIER_LEN];

  int init(const char *k, int klen);
  int init(const struct fscrypt_key_specifier& k);

  void decode(bufferlist::const_iterator& bl) {
    bl.copy(sizeof(raw), raw);
  }

  bool operator<(const struct ceph_fscrypt_key_identifier& r) const;
};

std::ostream& operator<<(std::ostream& out, const ceph_fscrypt_key_identifier& kid);

class FSCryptKey {
  bufferlist key;
  ceph_fscrypt_key_identifier identifier;

public:
  int init(const char *k, int klen);

  int calc_hkdf(char ctx_indentifier,
                const char *nonce, int nonce_len,
                char *result, int result_len);

  const ceph_fscrypt_key_identifier& get_identifier() const {
    return identifier;
  }

  bufferlist& get_key() { return key; }
};

using FSCryptKeyRef = std::shared_ptr<FSCryptKey>;

#define FSCRYPT_MAX_IV_SIZE 32
union FSCryptIV {
  uint8_t raw[FSCRYPT_MAX_IV_SIZE];
  struct {
    ceph_le64 block_num;
    uint8_t nonce[FSCRYPT_FILE_NONCE_SIZE];
  } u;
};

struct FSCryptPolicy {
  uint8_t version;
  uint8_t contents_encryption_mode;
  uint8_t filenames_encryption_mode;
  uint8_t flags;
  ceph_fscrypt_key_identifier master_key_identifier;

  virtual ~FSCryptPolicy() {}

  void decode(bufferlist::const_iterator& env_bl) {
    uint32_t v;

    ceph::decode(v, env_bl);

    bufferlist _bl;
    ceph::decode(_bl, env_bl);

    auto bl = _bl.cbegin();

    ceph::decode(version, bl);
    ceph::decode(contents_encryption_mode, bl);
    ceph::decode(filenames_encryption_mode, bl);
    ceph::decode(flags, bl);

    uint32_t __reserved;
    ceph::decode(__reserved, bl);

    master_key_identifier.decode(bl);

    decode_extra(bl);
  }

  virtual void decode_extra(bufferlist::const_iterator& bl) {}

  void convert_to(struct fscrypt_policy_v2 *dest) {
    dest->version = version;
    dest->contents_encryption_mode = contents_encryption_mode;
    dest->filenames_encryption_mode = filenames_encryption_mode;
    dest->flags = flags;
    memset(dest->__reserved, 0, sizeof(dest->__reserved));
    memcpy(dest->master_key_identifier, master_key_identifier.raw, sizeof(master_key_identifier.raw));
  }
};

using FSCryptPolicyRef = std::shared_ptr<FSCryptPolicy>;

struct FSCryptContext : public FSCryptPolicy {
  uint8_t nonce[FSCRYPT_FILE_NONCE_SIZE];

  void decode_extra(bufferlist::const_iterator& bl) override {
    bl.copy(sizeof(nonce), (char *)nonce);
  }

  void generate_iv(uint64_t block_num, FSCryptIV& iv) const;
};

using FSCryptContextRef = std::shared_ptr<FSCryptContext>;

class FSCryptDenc {
  FSCryptContextRef ctx;
  FSCryptKeyRef master_key;

  std::vector<char> key;
  FSCryptIV iv;

  EVP_CIPHER *cipher;
  EVP_CIPHER_CTX *cipher_ctx;
  std::vector<OSSL_PARAM> cipher_params;

  int calc_key(char ctx_identifier,
               int key_size,
               uint64_t block_num);
public:
  FSCryptDenc(EVP_CIPHER *cipher, std::vector<OSSL_PARAM> params);
  ~FSCryptDenc();

  void setup(FSCryptContextRef& _ctx,
             FSCryptKeyRef& _master_key);

  int calc_fname_key() {
    return calc_key(HKDF_CONTEXT_PER_FILE_ENC_KEY, 32, 0);
  }

  int calc_fdata_key(uint64_t block_num) {
    return calc_key(HKDF_CONTEXT_PER_FILE_ENC_KEY, 64, block_num);
  }

  int decrypt(const char *in_data, int in_len,
              char *out_data, int out_len);
};

using FSCryptDencRef = std::shared_ptr<FSCryptDenc>;

class FSCryptFNameDenc : public FSCryptDenc {
public:
  FSCryptFNameDenc();
};

class FSCryptFDataDenc : public FSCryptDenc {
public:
  FSCryptFDataDenc();
};

class FSCryptKeyHandler {
  ceph::shared_mutex lock = ceph::make_shared_mutex("FSCryptKeyHandler");
  int64_t epoch = -1;
  FSCryptKeyRef key;
public:
  FSCryptKeyHandler() {}
  FSCryptKeyHandler(int64_t epoch, FSCryptKeyRef k) : epoch(epoch), key(k) {}

  void reset(int64_t epoch, FSCryptKeyRef k);

  int64_t get_epoch();
  FSCryptKeyRef& get_key();
};

using FSCryptKeyHandlerRef = std::shared_ptr<FSCryptKeyHandler>;

class FSCryptKeyStore {
  ceph::shared_mutex lock = ceph::make_shared_mutex("FSCryptKeyStore");
  int64_t epoch = 0;
  std::map<ceph_fscrypt_key_identifier, FSCryptKeyHandlerRef> m;

  int _find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyHandlerRef& key);
public:
  FSCryptKeyStore() {}

  int create(const char *k, int klen, FSCryptKeyHandlerRef& key);
  int find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyHandlerRef& key);
  int invalidate(const struct ceph_fscrypt_key_identifier& id);
};

struct FSCryptKeyValidator {
  CephContext *cct;
  FSCryptKeyHandlerRef handler;
  int64_t epoch;

  FSCryptKeyValidator(CephContext *cct, FSCryptKeyHandlerRef& kh, int64_t e);

  bool is_valid() const;
};

using FSCryptKeyValidatorRef = std::shared_ptr<FSCryptKeyValidator>;


class FSCrypt {
  FSCryptKeyStore key_store;

  FSCryptDencRef init_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv,
                           std::function<FSCryptDenc *()> gen_denc);
public:
  FSCrypt() {}

  FSCryptKeyStore& get_key_store() {
    return key_store;
  }

  FSCryptDencRef get_fname_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv, bool calc_key);
  FSCryptDencRef get_fdata_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv);
};
