#pragma once

#include "fscrypt_uapi.h"

#include "common/ceph_mutex.h"

#include <map>


#define FSCRYPT_FILE_NONCE_SIZE 16

int fscrypt_calc_hkdf(const char *salt, int salt_len,
                      const char *key, int key_len,
                      char *dest, int dest_len);


struct ceph_fscrypt_key_identifier {
#define FSCRYPT_KEY_IDENTIFIER_LEN 16
  char raw[FSCRYPT_KEY_IDENTIFIER_LEN];

  int init(const char *k, int klen);
  int init(const struct fscrypt_key_specifier& k);

  bool operator<(const struct ceph_fscrypt_key_identifier& r) const;
};


class FSCryptKey {
  bufferlist key;
  ceph_fscrypt_key_identifier identifier;

public:
  int init(const char *k, int klen);

  const ceph_fscrypt_key_identifier& get_identifier() const {
    return identifier;
  }
};

using FSCryptKeyRef = std::shared_ptr<FSCryptKey>;

struct FSCryptPolicy {
  uint8_t version;
  uint8_t contents_encryption_mode;
  uint8_t filenames_encryption_mode;
  uint8_t flags;
  uint8_t master_key_identifier[FSCRYPT_KEY_IDENTIFIER_SIZE];

  virtual ~FSCryptPolicy() {}

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(version, bl);
    decode(contents_encryption_mode, bl);
    decode(filenames_encryption_mode, bl);
    decode(flags, bl);

    uint32_t __reserved;
    decode(__reserved, bl);

    bl.copy(sizeof(master_key_identifier), (char *)master_key_identifier);

    decode_extra(bl);
    DECODE_FINISH(bl);
  }

  virtual void decode_extra(bufferlist::const_iterator& bl) {}

  void convert_to(struct fscrypt_policy_v2 *dest) {
    dest->version = version;
    dest->contents_encryption_mode = contents_encryption_mode;
    dest->filenames_encryption_mode = filenames_encryption_mode;
    dest->flags = flags;
    memset(dest->__reserved, 0, sizeof(dest->__reserved));
    memcpy(dest->master_key_identifier, master_key_identifier, sizeof(master_key_identifier));
  }
};

using FSCryptPolicyRef = std::shared_ptr<FSCryptPolicy>;

struct FSCryptContext : public FSCryptPolicy {
  uint8_t nonce[FSCRYPT_FILE_NONCE_SIZE];

  void decode_extra(bufferlist::const_iterator& bl) override {
    bl.copy(sizeof(nonce), (char *)nonce);
  }
};

using FSCryptContextRef = std::shared_ptr<FSCryptContext>;


class FSCryptKeyStore {
  ceph::shared_mutex lock = ceph::make_shared_mutex("FSCryptKeyStore");
  std::map<ceph_fscrypt_key_identifier, FSCryptKeyRef> m;
public:
  FSCryptKeyStore() {}

  int create(const char *k, int klen, FSCryptKeyRef& key);
  int find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyRef& key);
  int remove(const struct ceph_fscrypt_key_identifier& id);
};


class FSCrypt {
  FSCryptKeyStore key_store;

public:
  FSCrypt() {}

  FSCryptKeyStore& get_key_store() {
    return key_store;
  }
};
