// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LoadRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/crypto/Utils.h"
#include "librbd/crypto/luks/Magic.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ReadResult.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::luks::LoadRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {
namespace luks {

using librbd::util::create_context_callback;

template <typename I>
LoadRequest<I>::LoadRequest(
        I* image_ctx, std::string_view passphrase,
        std::unique_ptr<CryptoInterface>* result_crypto,
        Context* on_finish) : m_image_ctx(image_ctx),
                              m_passphrase(passphrase),
                              m_on_finish(on_finish),
                              m_result_crypto(result_crypto),
                              m_initial_read_size(DEFAULT_INITIAL_READ_SIZE),
                              m_header(image_ctx->cct), m_offset(0) {
}

template <typename I>
void LoadRequest<I>::set_initial_read_size(uint64_t read_size) {
  m_initial_read_size = read_size;
}

template <typename I>
void LoadRequest<I>::send() {
  auto ctx = create_context_callback<
          LoadRequest<I>, &LoadRequest<I>::handle_read_header>(this);
  read(m_initial_read_size, ctx);
}

template <typename I>
void LoadRequest<I>::read(uint64_t end_offset, Context* on_finish) {
  auto length = end_offset - m_offset;
  auto aio_comp = io::AioCompletion::create_and_start(
          on_finish, librbd::util::get_image_ctx(m_image_ctx),
          io::AIO_TYPE_READ);
  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_read(
          *m_image_ctx, io::IMAGE_DISPATCH_LAYER_API_START, aio_comp,
          {{m_offset, length}}, io::ReadResult{&m_bl},
          m_image_ctx->get_data_io_context(), 0, 0, trace);
  req->send();
}

template <typename I>
bool LoadRequest<I>::handle_read(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "error reading from image: " << cpp_strerror(r)
                            << dendl;
    finish(r);
    return false;
  }

  m_offset += m_bl.length();

  if (m_last_read_bl.length() > 0) {
    m_last_read_bl.claim_append(m_bl);
    m_bl = std::move(m_last_read_bl);
  }

  if (m_image_ctx->parent != nullptr && m_bl.length() == m_offset &&
      Magic::is_rbd_clone(m_bl) > 0) {
    r = Magic::replace_magic(m_image_ctx->cct, m_bl);
    if (r < 0) {
      if (r == -EINVAL && m_offset < MAXIMUM_HEADER_SIZE) {
        m_last_read_bl = std::move(m_bl);
        auto ctx = create_context_callback<
              LoadRequest<I>, &LoadRequest<I>::handle_read_header>(this);
        read(MAXIMUM_HEADER_SIZE, ctx);
        return false;
      }

      lderr(m_image_ctx->cct) << "error replacing rbd clone magic: "
                              << cpp_strerror(r) << dendl;
      finish(r);
      return false;
    }
  }
  
  // setup interface with libcryptsetup
  r = m_header.init();
  if (r < 0) {
    finish(r);
    return false;
  }

  // write header to libcryptsetup interface
  r = m_header.write(m_bl);
  if (r < 0) {
    finish(r);
    return false;
  }

  m_bl.clear();

  return true;
}

template <typename I>
void LoadRequest<I>::handle_read_header(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (!handle_read(r)) {
    return;
  }

  // parse header via libcryptsetup
  r = m_header.load(CRYPT_LUKS);
  if (r != 0) {
    if (m_offset < MAXIMUM_HEADER_SIZE) {
      // perhaps we did not feed the entire header to libcryptsetup, retry
      auto ctx = create_context_callback<
              LoadRequest<I>, &LoadRequest<I>::handle_read_header>(this);
      read(MAXIMUM_HEADER_SIZE, ctx);
      return;
    }

    finish(r);
    return;
  }

  auto cipher = m_header.get_cipher();
  if (strcmp(cipher, "aes") != 0) {
    lderr(m_image_ctx->cct) << "unsupported cipher: " << cipher << dendl;
    finish(-ENOTSUP);
    return;
  }

  auto cipher_mode = m_header.get_cipher_mode();
  if (strcmp(cipher_mode, "xts-plain64") != 0) {
    lderr(m_image_ctx->cct) << "unsupported cipher mode: " << cipher_mode
                            << dendl;
    finish(-ENOTSUP);
    return;
  }

  read_volume_key();
  return;
}

template <typename I>
void LoadRequest<I>::handle_read_keyslots(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (!handle_read(r)) {
    return;
  }

  read_volume_key();
}

template <typename I>
void LoadRequest<I>::read_volume_key() {
  char volume_key[64];
  size_t volume_key_size = sizeof(volume_key);

  auto r = m_header.read_volume_key(
          m_passphrase.data(), m_passphrase.size(),
          reinterpret_cast<char*>(volume_key), &volume_key_size);
  if (r != 0) {
    auto keyslots_end_offset = m_header.get_data_offset();
    if (m_offset < keyslots_end_offset) {
      // perhaps we did not feed the the necessary keyslot, retry
      auto ctx = create_context_callback<
              LoadRequest<I>, &LoadRequest<I>::handle_read_keyslots>(this);
      read(keyslots_end_offset, ctx);
      return;
    }

    finish(r);
    return;
  }

  r = util::build_crypto(
          m_image_ctx->cct, reinterpret_cast<unsigned char*>(volume_key),
          volume_key_size, m_header.get_sector_size(),
          m_header.get_data_offset(), m_result_crypto);
  ceph_memzero_s(volume_key, 64, 64);
  finish(r);
}

template <typename I>
void LoadRequest<I>::finish(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;
  
  m_on_finish->complete(r);
  delete this;
}

} // namespace luks
} // namespace crypto
} // namespace librbd

template class librbd::crypto::luks::LoadRequest<librbd::ImageCtx>;
