// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_LUKS_FORMAT_REQUEST_H
#define CEPH_LIBRBD_CRYPTO_LUKS_FORMAT_REQUEST_H

#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/crypto/luks/Header.h"

namespace librbd {

class ImageCtx;

namespace crypto {
namespace luks {

template <typename I>
class FormatRequest {
public:
    static FormatRequest* create(
            I* image_ctx, encryption_format_t format,
            encryption_algorithm_t alg, std::string&& passphrase,
            ceph::ref_t<CryptoInterface>* result_crypto, Context* on_finish,
            bool insecure_fast_mode) {
      return new FormatRequest(image_ctx, format, alg, std::move(passphrase),
                               result_crypto, on_finish, insecure_fast_mode);
    }

    FormatRequest(I* image_ctx, encryption_format_t format,
                  encryption_algorithm_t alg, std::string&& passphrase,
                  ceph::ref_t<CryptoInterface>* result_crypto,
                  Context* on_finish, bool insecure_fast_mode);
    void send();
    void finish(int r);

private:
    I* m_image_ctx;

    encryption_format_t m_format;
    encryption_algorithm_t m_alg;
    std::string m_passphrase;
    ceph::ref_t<CryptoInterface>* m_result_crypto;
    Context* m_on_finish;
    bool m_insecure_fast_mode;
    Header m_header;

    void handle_write_header(int r);
};

} // namespace luks
} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::luks::FormatRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_LUKS_FORMAT_REQUEST_H
