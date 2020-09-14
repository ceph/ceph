// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_CRYPTO_INTERFACE_H
#define CEPH_LIBRBD_CRYPTO_CRYPTO_INTERFACE_H

#include "common/RefCountedObj.h"
#include "include/buffer.h"

namespace librbd {
namespace crypto {

class CryptoInterface : public RefCountedObject {

public:
  virtual void encrypt(ceph::bufferlist&& data,
                       uint64_t image_offset) const = 0;
  virtual void decrypt(ceph::bufferlist&& data,
                       uint64_t image_offset) const = 0;

};

} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_CRYPTO_INTERFACE_H
