// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_LOAD_REQUEST_H
#define CEPH_LIBRBD_CRYPTO_LOAD_REQUEST_H

#include "include/rbd/librbd.hpp"
#include "librbd/ImageCtx.h"

struct Context;

namespace librbd {

class ImageCtx;

namespace crypto {

template <typename I>
class LoadRequest {
public:
    using EncryptionFormat = decltype(I::encryption_format);

    static constexpr char UNKNOWN_FORMAT[] = "<unknown>";

    static LoadRequest* create(
            I* image_ctx, std::vector<EncryptionFormat>&& formats,
            Context* on_finish) {
      return new LoadRequest(image_ctx, std::move(formats), on_finish);
    }

    LoadRequest(I* image_ctx, std::vector<EncryptionFormat>&& formats,
                Context* on_finish);
    void send();
    void flush();
    void handle_flush(int r);
    void load();
    void handle_load(int r);
    void invalidate_cache();
    void handle_invalidate_cache(int r);
    void finish(int r);

private:
    I* m_image_ctx;
    Context* m_on_finish;

    size_t m_format_idx;
    bool m_is_current_format_assumed;
    std::vector<EncryptionFormat> m_formats;
    I* m_current_image_ctx;
    std::string m_detected_format_name;
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::LoadRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_LOAD_REQUEST_H
