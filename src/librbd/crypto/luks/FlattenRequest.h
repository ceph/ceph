// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_LUKS_FLATTEN_REQUEST_H
#define CEPH_LIBRBD_CRYPTO_LUKS_FLATTEN_REQUEST_H

#include "librbd/ImageCtx.h"

namespace librbd {

namespace crypto {
namespace luks {

template <typename I>
class FlattenRequest {
public:
    using EncryptionFormat = decltype(I::encryption_format);

    static FlattenRequest* create(I* image_ctx, Context* on_finish) {
      return new FlattenRequest(image_ctx, on_finish);
    }

    FlattenRequest(I* image_ctx, Context* on_finish);
    void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * READ_HEADER
   *    |
   *    v
   * WRITE_HEADER (replacing magic back from RBDL to LUKS if needed)
   *    |
   *    v
   * FLUSH
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
    I* m_image_ctx;
    Context* m_on_finish;
    ceph::bufferlist m_bl;

    void read_header();
    void handle_read_header(int r);
    void write_header();
    void handle_write_header(int r);
    void flush();
    void handle_flush(int r);
    void finish(int r);
};

} // namespace luks
} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::luks::FlattenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_LUKS_FLATTEN_REQUEST_H
