// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "EncryptionFormat.h"
#include "include/compat.h"
#include "librbd/crypto/luks/FormatRequest.h"
#include "librbd/crypto/luks/LoadRequest.h"

namespace librbd {
namespace crypto {
namespace luks {

template <typename I>
EncryptionFormat<I>::EncryptionFormat(
        encryption_algorithm_t alg,
        std::string&& passphrase) : m_alg(alg),
                                    m_passphrase(std::move(passphrase)) {
}

template <typename I>
EncryptionFormat<I>::~EncryptionFormat() {
  ceph_memzero_s(
          &m_passphrase[0], m_passphrase.capacity(), m_passphrase.size());
}

template <typename I>
void EncryptionFormat<I>::format(I* image_ctx, Context* on_finish) {
  auto req = luks::FormatRequest<I>::create(
          image_ctx, get_format(), m_alg, std::move(m_passphrase), &m_crypto,
          on_finish, false);
  req->send();
}

template <typename I>
void EncryptionFormat<I>::load(I* image_ctx, Context* on_finish) {
  auto req = luks::LoadRequest<I>::create(
          image_ctx, get_format(), std::move(m_passphrase), &m_crypto,
          on_finish);
  req->send();
}

} // namespace luks
} // namespace crypto
} // namespace librbd

template class librbd::crypto::luks::EncryptionFormat<librbd::ImageCtx>;
template class librbd::crypto::luks::LUKS1EncryptionFormat<librbd::ImageCtx>;
template class librbd::crypto::luks::LUKS2EncryptionFormat<librbd::ImageCtx>;
