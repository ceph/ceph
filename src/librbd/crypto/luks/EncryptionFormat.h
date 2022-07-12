// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_LUKS_ENCRYPTION_FORMAT_H
#define CEPH_LIBRBD_CRYPTO_LUKS_ENCRYPTION_FORMAT_H

#include "include/rbd/librbd.hpp"
#include "librbd/crypto/EncryptionFormat.h"

namespace librbd {

struct ImageCtx;

namespace crypto {
namespace luks {

template <typename ImageCtxT>
class EncryptionFormat : public crypto::EncryptionFormat<ImageCtxT> {

public:
    EncryptionFormat(encryption_algorithm_t alg, std::string&& passphrase);
    ~EncryptionFormat();

    void format(ImageCtxT* ictx, Context* on_finish) override;
    void load(ImageCtxT* ictx, Context* on_finish) override;

    ceph::ref_t<CryptoInterface> get_crypto() override {
      return m_crypto;
    }

private:
    virtual encryption_format_t get_format() = 0;

    encryption_algorithm_t m_alg;
    std::string m_passphrase;
    ceph::ref_t<CryptoInterface> m_crypto;
};

template <typename ImageCtxT>
class LUKS1EncryptionFormat : public EncryptionFormat<ImageCtxT> {
    using EncryptionFormat<ImageCtxT>::EncryptionFormat;

    encryption_format_t get_format() override {
      return RBD_ENCRYPTION_FORMAT_LUKS1;
    }
};

template <typename ImageCtxT>
class LUKS2EncryptionFormat : public EncryptionFormat<ImageCtxT> {
    using EncryptionFormat<ImageCtxT>::EncryptionFormat;

    encryption_format_t get_format() override {
      return RBD_ENCRYPTION_FORMAT_LUKS2;
    }
};

} // namespace luks
} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::luks::EncryptionFormat<librbd::ImageCtx>;
extern template class librbd::crypto::luks::LUKS1EncryptionFormat<
        librbd::ImageCtx>;
extern template class librbd::crypto::luks::LUKS2EncryptionFormat<
        librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_LUKS_ENCRYPTION_FORMAT_H
