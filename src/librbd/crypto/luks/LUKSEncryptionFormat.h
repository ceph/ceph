// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_LUKS_ENCRYPTION_FORMAT_H
#define CEPH_LIBRBD_CRYPTO_LUKS_ENCRYPTION_FORMAT_H

#include <string_view>
#include "include/rbd/librbd.hpp"
#include "librbd/crypto/CryptoInterface.h"
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
    LUKSEncryptionFormat(std::string_view passphrase);
    LUKSEncryptionFormat(encryption_algorithm_t alg,
                         std::string_view passphrase);

    std::unique_ptr<crypto::EncryptionFormat<ImageCtxT>>
    clone() const override {
      // clone() should be called only when handling the 'load' operation,
      // so decaying LUKS{1,2}EncryptionFormat into LUKSEncryptionFormat is fine
      return std::unique_ptr<crypto::EncryptionFormat<ImageCtxT>>(
              new LUKSEncryptionFormat(m_passphrase));
    }

    void format(ImageCtxT* ictx, Context* on_finish) override;
    void load(ImageCtxT* ictx, Context* on_finish) override;

    CryptoInterface* get_crypto() override {
      ceph_assert(m_crypto);
      return m_crypto.get();
    }

protected:
    virtual encryption_format_t get_format() const {
      return RBD_ENCRYPTION_FORMAT_LUKS;
    }

    std::string_view m_passphrase;
    encryption_algorithm_t m_alg;
    std::unique_ptr<CryptoInterface> m_crypto;
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
