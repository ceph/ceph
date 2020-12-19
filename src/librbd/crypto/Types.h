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

enum DiskEncryptionFormat {
    DISK_ENCRYPTION_FORMAT_LUKS1,
    DISK_ENCRYPTION_FORMAT_LUKS2,
};

enum CipherAlgorithm {
    CIPHER_ALGORITHM_AES128,
    CIPHER_ALGORITHM_AES256,
};

} // namespace crypto
} // namespace librbd

#endif // CEPH_LIBRBD_CRYPTO_DATA_CRYPTOR_H
