// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_FORMAT_REQUEST_H
#define CEPH_LIBRBD_CRYPTO_FORMAT_REQUEST_H

#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"

struct Context;

namespace librbd {

class ImageCtx;

namespace crypto {

template <typename I>
class FormatRequest {
public:
    using EncryptionFormat = decltype(I::encryption_format);

    static FormatRequest* create(
            I* image_ctx, EncryptionFormat format, Context* on_finish) {
      return new FormatRequest(image_ctx, std::move(format), on_finish);
    }

    FormatRequest(I* image_ctx, EncryptionFormat format, Context* on_finish);
    void send();
    void handle_shutdown_crypto(int r);
    void format();
    void handle_format(int r);
    void flush();
    void handle_flush(int r);
    void finish(int r);

private:
    I* m_image_ctx;

    EncryptionFormat m_format;
    Context* m_on_finish;
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::FormatRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_FORMAT_REQUEST_H
