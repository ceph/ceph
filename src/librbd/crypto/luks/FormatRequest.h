// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_LUKS_FORMAT_REQUEST_H
#define CEPH_LIBRBD_CRYPTO_LUKS_FORMAT_REQUEST_H

#include "librbd/ImageCtx.h"
#include "librbd/crypto/Types.h"
#include "librbd/crypto/luks/Header.h"

namespace librbd {

class ImageCtx;

namespace crypto {
namespace luks {

template <typename I>
class FormatRequest {
public:
    static FormatRequest* create(
            I* image_ctx, DiskEncryptionFormat type, CipherAlgorithm cipher,
            std::string&& passphrase, Context* on_finish,
            bool insecure_fast_mode) {
      return new FormatRequest(image_ctx, type, cipher, std::move(passphrase),
                               on_finish, insecure_fast_mode);
    }

    FormatRequest(I* image_ctx, DiskEncryptionFormat type,
                  CipherAlgorithm cipher, std::string&& passphrase,
                  Context* on_finish, bool insecure_fast_mode);
    void send();
    void finish(int r);

private:
    I* m_image_ctx;

    DiskEncryptionFormat m_type;
    CipherAlgorithm m_cipher;
    Context* m_on_finish;
    bool m_insecure_fast_mode;
    Header m_header;
    std::string m_passphrase;

    void handle_write_header(int r);
};

} // namespace luks
} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::luks::FormatRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_LUKS_FORMAT_REQUEST_H
