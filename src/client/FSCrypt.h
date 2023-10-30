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

#define FSCRYPT_BLOCK_SIZE 4096
#define FSCRYPT_BLOCK_BITS 12

#define FSCRYPT_DATA_ALIGNMENT 16

static inline uint64_t fscrypt_align_ofs(uint64_t ofs) {
  return (ofs + FSCRYPT_DATA_ALIGNMENT - 1) & ~(FSCRYPT_DATA_ALIGNMENT - 1);
}

static inline int fscrypt_ofs_in_block(uint64_t pos) {
  return pos & (FSCRYPT_BLOCK_SIZE - 1);
}

static inline int fscrypt_block_from_ofs(uint64_t ofs) {
  return ofs >> FSCRYPT_BLOCK_BITS;
}

static inline uint64_t fscrypt_block_start(uint64_t ofs) {
  return ofs & ~(FSCRYPT_BLOCK_SIZE - 1);
}

static inline uint64_t fscrypt_next_block_start(uint64_t ofs) {
  return (ofs + FSCRYPT_BLOCK_SIZE - 1) & ~(FSCRYPT_BLOCK_SIZE - 1);
}

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

int fscrypt_fname_armor(const char *src, int src_len,
                        char *result, int max_len);
int fscrypt_fname_unarmor(const char *src, int src_len,
                          char *result, int max_len);

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

  void encode(bufferlist& bl) const {
    bl.append(raw, sizeof(raw));
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
public:
  uint8_t version;
  uint8_t contents_encryption_mode;
  uint8_t filenames_encryption_mode;
  uint8_t flags;
  ceph_fscrypt_key_identifier master_key_identifier;

  virtual ~FSCryptPolicy() {}

  void init(const struct fscrypt_policy_v2& policy) {
    version = policy.version;
    contents_encryption_mode = policy.contents_encryption_mode;
    filenames_encryption_mode = policy.filenames_encryption_mode;
    flags = policy.flags;
    memcpy(master_key_identifier.raw, policy.master_key_identifier, sizeof(master_key_identifier.raw));
  }

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

  void encode(bufferlist& env_bl) const {
    uint32_t v = 1;
    ceph::encode(v, env_bl);

    bufferlist bl;

    ceph::encode(version, bl);
    ceph::encode(contents_encryption_mode, bl);
    ceph::encode(filenames_encryption_mode, bl);
    ceph::encode(flags, bl);

    uint32_t __reserved = 0;
    ceph::encode(__reserved, bl);

    master_key_identifier.encode(bl);

    encode_extra(bl);

    ceph::encode(bl, env_bl);

  }

  virtual void encode_extra(bufferlist& bl) const {}

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
  CephContext *cct;

  FSCryptContext(CephContext *_cct) : cct(_cct) {}

  uint8_t nonce[FSCRYPT_FILE_NONCE_SIZE];

  void decode_extra(bufferlist::const_iterator& bl) override {
    bl.copy(sizeof(nonce), (char *)nonce);
  }

  void encode_extra(bufferlist& bl) const override {
    bl.append((char *)nonce, sizeof(nonce));
  }

  void generate_new_nonce();
  void generate_iv(uint64_t block_num, FSCryptIV& iv) const;
};

using FSCryptContextRef = std::shared_ptr<FSCryptContext>;

class FSCryptDenc {
protected:
  CephContext *cct;

  FSCryptContextRef ctx;
  FSCryptKeyRef master_key;

  std::vector<char> key;
  FSCryptIV iv;

  int padding = 1;
  int key_size = 0;
  int iv_size = 0;

  EVP_CIPHER *cipher;
  EVP_CIPHER_CTX *cipher_ctx;
  std::vector<OSSL_PARAM> cipher_params;

  int calc_key(char ctx_identifier,
               int key_size,
               uint64_t block_num);

  bool do_setup_cipher(int encryption_mode);

public:
  FSCryptDenc(CephContext *_cct);
  virtual ~FSCryptDenc();

  virtual bool setup_cipher() = 0;

  void init_cipher(EVP_CIPHER *cipher, std::vector<OSSL_PARAM> params);
  bool setup(FSCryptContextRef& _ctx,
             FSCryptKeyRef& _master_key);

  int calc_fname_key() {
    return calc_key(HKDF_CONTEXT_PER_FILE_ENC_KEY, key_size, 0);
  }

  int calc_fdata_key(uint64_t block_num) {
    return calc_key(HKDF_CONTEXT_PER_FILE_ENC_KEY, key_size, block_num);
  }

  int decrypt(const char *in_data, int in_len,
              char *out_data, int out_len);
  int encrypt(const char *in_data, int in_len,
              char *out_data, int out_len);
};

using FSCryptDencRef = std::shared_ptr<FSCryptDenc>;

class FSCryptFNameDenc : public FSCryptDenc {
public:
  FSCryptFNameDenc(CephContext *_cct) : FSCryptDenc(_cct) {}

  bool setup_cipher() override;

  int get_encrypted_fname(const std::string& plain, std::string *encrypted, std::string *alt_name);
  int get_decrypted_fname(const std::string& b64enc, const std::string& alt_name, std::string *decrypted);

  int get_encrypted_symlink(const std::string& plain, std::string *encrypted);
  int get_decrypted_symlink(const std::string& b64enc, std::string *decrypted);
};

using FSCryptFNameDencRef = std::shared_ptr<FSCryptFNameDenc>;

class FSCryptFDataDenc : public FSCryptDenc {
public:
  FSCryptFDataDenc(CephContext *_cct) : FSCryptDenc(_cct) {}

  using Segment = std::pair<uint64_t, uint64_t>;

  bool setup_cipher() override;

  int decrypt_bl(uint64_t off, uint64_t len, uint64_t pos, const std::vector<Segment>& holes, bufferlist *bl);
  int encrypt_bl(uint64_t off, uint64_t len, bufferlist& bl, bufferlist *encbl);
};

using FSCryptFDataDencRef = std::shared_ptr<FSCryptFDataDenc>;

class FSCryptKeyHandler {
  ceph::shared_mutex lock = ceph::make_shared_mutex("FSCryptKeyHandler");
  int64_t epoch = -1;
  FSCryptKeyRef key;
  std::list<int> users;
public:
  FSCryptKeyHandler() {}
  FSCryptKeyHandler(int64_t epoch, FSCryptKeyRef k) : epoch(epoch), key(k) {}

  void reset(int64_t epoch, FSCryptKeyRef k);

  int64_t get_epoch();
  std::list<int>& get_users() { return users; }
  FSCryptKeyRef& get_key();
};

using FSCryptKeyHandlerRef = std::shared_ptr<FSCryptKeyHandler>;

class FSCryptKeyStore {
  CephContext *cct;

  ceph::shared_mutex lock = ceph::make_shared_mutex("FSCryptKeyStore");
  int64_t epoch = 0;
  std::map<ceph_fscrypt_key_identifier, FSCryptKeyHandlerRef> m;

  int _find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyHandlerRef& key);
public:
  FSCryptKeyStore(CephContext *_cct) : cct(_cct) {}

  bool valid_key_spec(const struct fscrypt_key_specifier& k);
  int master_key_spec_len(const struct fscrypt_key_specifier& spec);
  int maybe_add_user(std::list<int>* users, int user);
  int maybe_remove_user(struct fscrypt_remove_key_arg* arg, std::list<int>* users, int user);
  int create(const char *k, int klen, FSCryptKeyHandlerRef& key, int user);
  int find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyHandlerRef& key);
  int invalidate(struct fscrypt_remove_key_arg* id, int user);
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
  CephContext *cct;

  FSCryptKeyStore key_store;

  FSCryptDenc *init_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv,
                         std::function<FSCryptDenc *()> gen_denc);
public:
  FSCrypt(CephContext *_cct) : cct(_cct), key_store(cct) {}

  FSCryptContextRef init_ctx(const std::vector<unsigned char>& fscrypt_auth);

  FSCryptKeyStore& get_key_store() {
    return key_store;
  }

  FSCryptFNameDencRef get_fname_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv, bool calc_key);
  FSCryptFDataDencRef get_fdata_denc(FSCryptContextRef& ctx, FSCryptKeyValidatorRef *kv);

  void prepare_data_read(FSCryptContextRef& ctx,
                         FSCryptKeyValidatorRef *kv,
                         uint64_t off,
                         uint64_t len,
                         uint64_t file_raw_size,
                         uint64_t *read_start,
                         uint64_t *read_len,
                         FSCryptFDataDencRef *denc);
};
