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

// This class is derived from only for the sake of the 'format' operation.
// The 'load' operation is handled the same for all three variants.
template <typename ImageCtxT>
class LUKSEncryptionFormat : public crypto::EncryptionFormat<ImageCtxT> {

public:
    LUKSEncryptionFormat(std::string&& passphrase);
    LUKSEncryptionFormat(encryption_algorithm_t alg, std::string&& passphrase);
    ~LUKSEncryptionFormat();

    void format(ImageCtxT* ictx, Context* on_finish) override;
    void load(ImageCtxT* ictx, Context* on_finish) override;

    ceph::ref_t<CryptoInterface> get_crypto() override {
      return m_crypto;
    }

protected:
    virtual encryption_format_t get_format() const {
      return RBD_ENCRYPTION_FORMAT_LUKS;
    }

    std::string m_passphrase;
    encryption_algorithm_t m_alg;
    ceph::ref_t<CryptoInterface> m_crypto;
};

template <typename ImageCtxT>
class LUKS1EncryptionFormat : public LUKSEncryptionFormat<ImageCtxT> {
    using LUKSEncryptionFormat<ImageCtxT>::LUKSEncryptionFormat;

    encryption_format_t get_format() const override {
      return RBD_ENCRYPTION_FORMAT_LUKS1;
    }
};

template <typename ImageCtxT>
class LUKS2EncryptionFormat : public LUKSEncryptionFormat<ImageCtxT> {
    using LUKSEncryptionFormat<ImageCtxT>::LUKSEncryptionFormat;

    encryption_format_t get_format() const override {
      return RBD_ENCRYPTION_FORMAT_LUKS2;
    }
};

} // namespace luks
} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::luks::LUKSEncryptionFormat<
        librbd::ImageCtx>;
extern template class librbd::crypto::luks::LUKS1EncryptionFormat<
        librbd::ImageCtx>;
extern template class librbd::crypto::luks::LUKS2EncryptionFormat<
        librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_LUKS_ENCRYPTION_FORMAT_H
