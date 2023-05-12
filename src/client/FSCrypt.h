#pragma once

#include "fscrypt_uapi.h"


int fscrypt_calc_hkdf(const char *salt, int salt_len,
                      const char *key, int key_len,
                      char *dest, int dest_len);

class FSCryptKey {
public:
  int init(const struct fscrypt_key_specifier& key_spec);
};
