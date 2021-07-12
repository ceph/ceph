// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LoadRequest.h"

#include "cls/rbd/cls_rbd_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/ImageCtx.h"
#include "librbd/crypto/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::LoadRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
LoadRequest<I>::LoadRequest(
        I* image_ctx, std::unique_ptr<EncryptionFormat<I>> format,
        Context* on_finish) : m_image_ctx(image_ctx),
                              m_format(std::move(format)),
                              m_on_finish(on_finish) {
}

template <typename I>
void LoadRequest<I>::send() {
  if (m_image_ctx->crypto != nullptr) {
    lderr(m_image_ctx->cct) << "encryption already loaded" << dendl;
    finish(-EEXIST);
    return;
  }

  auto ictx = m_image_ctx;
  while (ictx != nullptr) {
    if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
      lderr(m_image_ctx->cct) << "cannot use encryption with journal."
                              << " image name: " << ictx->name << dendl;
      finish(-ENOTSUP);
      return;
    }
    ictx = ictx->parent;
  }

  auto ctx = create_context_callback<
          LoadRequest<I>, &LoadRequest<I>::handle_load>(this);
  m_format->load(m_image_ctx, ctx);
}

template <typename I>
void LoadRequest<I>::handle_load(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r != 0) {
    lderr(m_image_ctx->cct) << "failed to load encryption" << cpp_strerror(r)
                            << dendl;
    finish(r);
    return;
  }

  m_current_ctx = m_image_ctx;
  m_current_crypto = m_format->get_crypto();
  read_metadata();
}

template <typename I>
void LoadRequest<I>::read_metadata() {
  m_current_ctx->set_crypto(m_current_crypto.get());

  if (m_current_ctx->parent == nullptr) {
    finish(0);
    return;
  }

  // read parent cryptor from image meta
  librados::ObjectReadOperation op;
  cls_client::metadata_get_start(
          &op, EncryptionFormat<I>::PARENT_CRYPTOR_METADATA_KEY);
  m_metadata_bl.clear();
  auto ctx = create_context_callback<
          LoadRequest<I>, &LoadRequest<I>::handle_read_metadata>(this);
  auto aio_comp = librbd::util::create_rados_callback(ctx);
  int r = m_current_ctx->md_ctx.aio_operate(
          m_current_ctx->header_oid, aio_comp, &op, &m_metadata_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void LoadRequest<I>::handle_read_metadata(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    // parent has same crypto as child
  } else if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to read image metadata."
                            << " image name: " << m_current_ctx->name
                            << "; error: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else {
    // current image has been formatted
    // decode parent crypto
    std::string data;
    auto it = m_metadata_bl.cbegin();
    r = cls_client::metadata_get_finish(&it, &data);

    if (r < 0) {
      lderr(m_image_ctx->cct) << "failed to decode image metadata."
                              << " image name: " << m_current_ctx->name
                              << "; error: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    // deserialize
    ParentCryptoParams ancestor_cryptor;
    auto encoded_cryptor = bufferlist::static_from_string(data);
    auto cryptor_it = encoded_cryptor.cbegin();
    ancestor_cryptor.decode(cryptor_it);

    if (ancestor_cryptor.wrapped_key.empty()) {
      // parent (incl. all other ancestors) is not encrypted
      finish(0);
      return;
    }

    // unwrap key
    std::string key;
    r = util::key_wrap(
            m_image_ctx->cct, CipherMode::CIPHER_MODE_DEC,
            m_current_crypto->get_key(), m_current_crypto->get_key_length(),
            reinterpret_cast<unsigned char*>(
                    ancestor_cryptor.wrapped_key.data()),
            ancestor_cryptor.wrapped_key.size(), &key);
    if (r != 0) {
      lderr(m_image_ctx->cct) << "error unwrapping parent key: "
                              << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    r = util::build_crypto(
            m_image_ctx->cct, reinterpret_cast<unsigned char*>(key.data()),
            key.size(), ancestor_cryptor.block_size,
            ancestor_cryptor.data_offset, &m_current_crypto);
    ceph_memzero_s(&key[0], key.size(), key.size());
    if (r != 0) {
      finish(r);
      return;
    }
  }

  // move to next ancestor
  m_current_ctx = m_current_ctx->parent;
  read_metadata();
}

template <typename I>
void LoadRequest<I>::finish(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  auto ictx = m_image_ctx;
  if (r == 0) {
    // load crypto layers to image and its ancestors
    while (ictx != nullptr) {
      auto crypto = ictx->get_crypto();
      if (crypto == nullptr) {
        break;
      }
      util::set_crypto(ictx, crypto);
      crypto->put();
      ictx = ictx->parent;
    }

    std::unique_lock image_locker{m_image_ctx->image_lock};
    m_image_ctx->encryption_format = std::move(m_format);
  } else {
    // clean up builded cryptors
    while (ictx != nullptr && ictx->crypto != nullptr) {
      ictx->set_crypto(nullptr);
      ictx = ictx->parent;
    }
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::LoadRequest<librbd::ImageCtx>;
