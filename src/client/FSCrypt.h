#pragma once

#include "fscrypt_uapi.h"

#include "common/ceph_mutex.h"

#include <map>


int fscrypt_calc_hkdf(const char *salt, int salt_len,
                      const char *key, int key_len,
                      char *dest, int dest_len);


struct ceph_fscrypt_key_identifier {
#define FSCRYPT_KEY_IDENTIFIER_LEN 16
  char raw[FSCRYPT_KEY_IDENTIFIER_LEN];

  int init(const char *k, int klen);

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


class FSCryptKeyStore {
  ceph::shared_mutex lock = ceph::make_shared_mutex("FSCryptKeyStore");
  std::map<ceph_fscrypt_key_identifier, FSCryptKeyRef> m;
public:
  FSCryptKeyStore() {}

  int create(const char *k, int klen, FSCryptKeyRef& key);
  int find(const struct ceph_fscrypt_key_identifier& id, FSCryptKeyRef& key);
};


class FSCrypt {
  FSCryptKeyStore key_store;

public:
  FSCrypt() {}

  FSCryptKeyStore& get_key_store() {
    return key_store;
  }
};
