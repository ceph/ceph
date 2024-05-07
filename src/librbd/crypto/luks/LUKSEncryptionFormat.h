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

template <typename ImageCtxT>
class EncryptionFormat : public crypto::EncryptionFormat<ImageCtxT> {
public:
  void flatten(ImageCtxT* ictx, Context* on_finish) override;

  CryptoInterface* get_crypto() override {
    ceph_assert(m_crypto);
    return m_crypto.get();
  }

protected:
  std::unique_ptr<CryptoInterface> m_crypto;
};

template <typename ImageCtxT>
class LUKSEncryptionFormat : public EncryptionFormat<ImageCtxT> {
public:
  LUKSEncryptionFormat(std::string_view passphrase)
      : m_passphrase(passphrase) {}

  std::unique_ptr<crypto::EncryptionFormat<ImageCtxT>> clone() const override {
    return std::make_unique<LUKSEncryptionFormat>(m_passphrase);
  }

  void format(ImageCtxT* ictx, Context* on_finish) override;
  void load(ImageCtxT* ictx, std::string* detected_format_name,
            Context* on_finish) override;

private:
  std::string_view m_passphrase;
};

template <typename ImageCtxT>
class LUKS1EncryptionFormat : public EncryptionFormat<ImageCtxT> {
public:
  LUKS1EncryptionFormat(encryption_algorithm_t alg, std::string_view passphrase)
      : m_alg(alg), m_passphrase(passphrase) {}

  std::unique_ptr<crypto::EncryptionFormat<ImageCtxT>> clone() const override {
    return std::make_unique<LUKS1EncryptionFormat>(m_alg, m_passphrase);
  }

  void format(ImageCtxT* ictx, Context* on_finish) override;
  void load(ImageCtxT* ictx, std::string* detected_format_name,
            Context* on_finish) override;

private:
  encryption_algorithm_t m_alg;
  std::string_view m_passphrase;
};

template <typename ImageCtxT>
class LUKS2EncryptionFormat : public EncryptionFormat<ImageCtxT> {
public:
  LUKS2EncryptionFormat(encryption_algorithm_t alg, std::string_view passphrase)
      : m_alg(alg), m_passphrase(passphrase) {}

  std::unique_ptr<crypto::EncryptionFormat<ImageCtxT>> clone() const override {
    return std::make_unique<LUKS2EncryptionFormat>(m_alg, m_passphrase);
  }

  void format(ImageCtxT* ictx, Context* on_finish) override;
  void load(ImageCtxT* ictx, std::string* detected_format_name,
            Context* on_finish) override;

private:
  encryption_algorithm_t m_alg;
  std::string_view m_passphrase;
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
