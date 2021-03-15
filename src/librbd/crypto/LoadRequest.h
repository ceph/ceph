// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_LOAD_REQUEST_H
#define CEPH_LIBRBD_CRYPTO_LOAD_REQUEST_H

#include "include/rbd/librbd.hpp"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/crypto/EncryptionFormat.h"

struct Context;

namespace librbd {

class ImageCtx;

namespace crypto {

template <typename I>
class LoadRequest {
public:
    static LoadRequest* create(
            I* image_ctx, std::unique_ptr<EncryptionFormat<I>> format,
            Context* on_finish) {
      return new LoadRequest(image_ctx, std::move(format), on_finish);
    }

    LoadRequest(I* image_ctx, std::unique_ptr<EncryptionFormat<I>> format,
                Context* on_finish);
    void send();
    void finish(int r);

private:
    I* m_image_ctx;
    std::unique_ptr<EncryptionFormat<I>> m_format;
    Context* m_on_finish;
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::LoadRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_LOAD_REQUEST_H
