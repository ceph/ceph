#pragma once

#include "fscrypt_uapi.h"


class FSCryptKey {
public:
  int init(const struct fscrypt_key_specifier& key_spec);
};
