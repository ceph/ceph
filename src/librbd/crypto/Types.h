// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_TYPES_H
#define CEPH_LIBRBD_CRYPTO_TYPES_H

#include <list>
#include "include/encoding.h"

namespace ceph {
    class Formatter;
}

namespace librbd {
namespace crypto {

enum CipherMode {
    CIPHER_MODE_ENC,
    CIPHER_MODE_DEC,
};

struct ParentCryptoParams {
  std::string wrapped_key;
  uint64_t block_size;
  uint64_t data_offset;

  ParentCryptoParams() {
  }
  ParentCryptoParams(std::string _wrapped_key, uint64_t _block_size,
                     uint64_t _data_offset)
    : wrapped_key(_wrapped_key), block_size(_block_size),
      data_offset(_data_offset) {
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<ParentCryptoParams *> &o);
};

WRITE_CLASS_ENCODER(ParentCryptoParams);

} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_DATA_CRYPTOR_H
