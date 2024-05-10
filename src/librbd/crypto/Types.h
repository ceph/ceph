// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_TYPES_H
#define CEPH_LIBRBD_CRYPTO_TYPES_H

namespace librbd {
namespace crypto {

enum CipherMode {
    CIPHER_MODE_ENC,
    CIPHER_MODE_DEC,
};

} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_DATA_CRYPTOR_H
