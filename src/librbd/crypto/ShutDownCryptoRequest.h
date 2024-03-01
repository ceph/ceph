// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_SHUT_DOWN_CRYPTO_REQUEST_H
#define CEPH_LIBRBD_CRYPTO_SHUT_DOWN_CRYPTO_REQUEST_H

#include "librbd/ImageCtx.h"

struct Context;

namespace librbd {

class ImageCtx;

namespace crypto {

template <typename I>
class ShutDownCryptoRequest {
public:
    static ShutDownCryptoRequest* create(I* image_ctx, Context* on_finish) {
      return new ShutDownCryptoRequest(image_ctx, on_finish);
    }

    ShutDownCryptoRequest(I* image_ctx, Context* on_finish);

    void send();
    void shut_down_object_dispatch();
    void handle_shut_down_object_dispatch(int r);
    void finish(int r);

private:
    I* m_image_ctx;
    Context* m_on_finish;
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::ShutDownCryptoRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_SHUT_DOWN_CRYPTO_REQUEST_H
