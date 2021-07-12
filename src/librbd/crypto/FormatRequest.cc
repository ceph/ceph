// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FormatRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/crypto/ShutDownCryptoRequest.h"
#include "librbd/crypto/Types.h"
#include "librbd/crypto/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/Types.h"
#include "librbd/operation/MetadataSetRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::FormatRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
FormatRequest<I>::FormatRequest(
        I* image_ctx, std::unique_ptr<EncryptionFormat<I>> format,
        Context* on_finish) : m_image_ctx(image_ctx),
                              m_format(std::move(format)),
                              m_on_finish(on_finish) {
}

template <typename I>
void FormatRequest<I>::send() {
  if (m_image_ctx->test_features(RBD_FEATURE_JOURNALING)) {
    lderr(m_image_ctx->cct) << "cannot use encryption with journal" << dendl;
    finish(-ENOTSUP);
    return;
  }

  if (m_image_ctx->is_formatted_clone) {
    lderr(m_image_ctx->cct) << "cloned image already formatted" << dendl;
    finish(-EINVAL);
    return;
  }

  if (m_image_ctx->crypto == nullptr) {
    format();
    return;
  }

  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_shutdown_crypto>(this);
  auto *req = ShutDownCryptoRequest<I>::create(m_image_ctx, ctx);
  req->send();
}

template <typename I>
void FormatRequest<I>::handle_shutdown_crypto(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r != 0) {
    lderr(m_image_ctx->cct) << "unable to unload existing crypto: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  format();
}

template <typename I>
void FormatRequest<I>::format() {
  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_format>(this);
  m_format->format(m_image_ctx, ctx);
}

template <typename I>
void FormatRequest<I>::handle_format(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r != 0) {
    lderr(m_image_ctx->cct) << "unable to format image: " << cpp_strerror(r)
                            << dendl;
    finish(r);
    return;
  }

  flush();
}

template <typename I>
void FormatRequest<I>::flush() {
  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_flush>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_FLUSH);
  auto req = io::ImageDispatchSpec::create_flush(
    *m_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
}

template <typename I>
void FormatRequest<I>::handle_flush(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r != 0) {
    lderr(m_image_ctx->cct) << "unable to flush image: " << cpp_strerror(r)
                            << dendl;
    finish(r);
    return;
  }

  if (m_image_ctx->parent != nullptr) {
    metadata_set();
  } else {
    finish(0);
  }
}

template <typename I>
void FormatRequest<I>::metadata_set() {
  std::string wrapped_key;
  uint64_t block_size = 0;
  uint64_t data_offset = 0;

  auto parent_crypto = m_image_ctx->parent->get_crypto();
  if (parent_crypto != nullptr) {
    auto crypto = m_format->get_crypto();
    block_size = parent_crypto->get_block_size();
    data_offset = parent_crypto->get_data_offset();

    // wrap key
    int r = util::key_wrap(
            m_image_ctx->cct, CipherMode::CIPHER_MODE_ENC,
            crypto->get_key(), crypto->get_key_length(),
            parent_crypto->get_key(), parent_crypto->get_key_length(),
            &wrapped_key);
    parent_crypto->put();
    if (r != 0) {
      lderr(m_image_ctx->cct) << "error wrapping parent key: "
                              << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  }

  // serialize
  ParentCryptoParams parent_cryptor(wrapped_key, block_size, data_offset);
  bufferlist parent_cryptor_bl;
  parent_cryptor.encode(parent_cryptor_bl);
  m_serialized_parent_cryptor = parent_cryptor_bl.to_str();

  // store parent cryptor to image meta
  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_metadata_set>(this);
  auto *request = operation::MetadataSetRequest<I>::create(
          *m_image_ctx, ctx, EncryptionFormat<I>::PARENT_CRYPTOR_METADATA_KEY,
          m_serialized_parent_cryptor);
  request->send();
}

template <typename I>
void FormatRequest<I>::handle_metadata_set(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r != 0) {
    lderr(m_image_ctx->cct) << "unable to persist parent cryptor: "
                            << cpp_strerror(r) << dendl;
  }

  finish(r);
}

template <typename I>
void FormatRequest<I>::finish(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r == 0) {
    util::set_crypto(m_image_ctx, m_format->get_crypto());
    std::unique_lock image_locker{m_image_ctx->image_lock};
    m_image_ctx->encryption_format = std::move(m_format);
    if (m_image_ctx->parent != nullptr) {
      m_image_ctx->is_formatted_clone = true;
    }
  }
  m_on_finish->complete(r);
  delete this;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::FormatRequest<librbd::ImageCtx>;
