// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_FLATTEN_REQUEST_H
#define CEPH_LIBRBD_CRYPTO_FLATTEN_REQUEST_H

#include "include/rbd/librbd.hpp"

struct Context;

namespace librbd {

class ImageCtx;

namespace crypto {

template <typename> class EncryptionFormat;

template <typename I>
class FlattenRequest {
public:
    static FlattenRequest* create(
            I* image_ctx, EncryptionFormat<I>* format, Context* on_finish) {
      return new FlattenRequest(image_ctx, format, on_finish);
    }

    FlattenRequest(I* image_ctx, EncryptionFormat<I>* format, Context* on_finish);
    void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * CREATE_RAW_CTX (ImageCtx without encryption loaded)
   *    |
   *    v
   * FLATTEN_CRYPTO_HEADER
   *    |
   *    v
   * METADATA_REMOVE
   *    |
   *    v
   * CRYPTO_FLATTEN (Format specific flattening)
   *    |
   *    v
   * CLOSE_RAW_CTX
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
    void handle_create_raw_ctx(int r);
    void flatten_crypto_header();
    void handle_flatten_crypto_header(int r);
    void metadata_remove();
    void handle_metadata_remove(int r);
    void crypto_flatten();
    void handle_crypto_flatten(int r);
    void close_raw_ctx();
    void handle_raw_ctx_close(int r);
    void finish(int r);

private:
    I* m_image_ctx;
    EncryptionFormat<I>* m_format;
    I* m_raw_image_ctx;
    int m_return;

    Context* m_on_finish;
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::FlattenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_FLATTEN_REQUEST_H
