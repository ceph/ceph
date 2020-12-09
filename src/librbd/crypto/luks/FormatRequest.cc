// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FormatRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/crypto/luks/Header.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::luks::FormatRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {
namespace luks {

using librbd::util::create_context_callback;

template <typename I>
FormatRequest<I>::FormatRequest(
        I* image_ctx, DiskEncryptionFormat type, CipherAlgorithm cipher,
        std::string&& passphrase, Context* on_finish,
        bool insecure_fast_mode) : m_image_ctx(image_ctx), m_type(type),
                                   m_cipher(cipher), m_on_finish(on_finish),
                                   m_insecure_fast_mode(insecure_fast_mode),
                                   m_header(image_ctx->cct),
                                   m_passphrase(std::move(passphrase)) {
}

template <typename I>
void FormatRequest<I>::send() {
  if (m_image_ctx->io_object_dispatcher->exists(
          io::OBJECT_DISPATCH_LAYER_CRYPTO)) {
    finish(-EEXIST);
    return;
  }

  const char* type;
  size_t sector_size;
  switch(m_type) {
    case DISK_ENCRYPTION_FORMAT_LUKS1:
      type = CRYPT_LUKS1;
      sector_size = 512;
      break;
    case DISK_ENCRYPTION_FORMAT_LUKS2:
      type = CRYPT_LUKS2;
      sector_size = 4096;
      break;
    default:
      lderr(m_image_ctx->cct) << "unsupported disk encryption type: " << m_type
                              << dendl;
      finish(-EINVAL);
      return;
  }

  const char* alg;
  size_t key_size;
  switch (m_cipher) {
    case CIPHER_ALGORITHM_AES128:
      alg = "aes";
      key_size = 32;
      break;
    case CIPHER_ALGORITHM_AES256:
      alg = "aes";
      key_size = 64;
      break;
    default:
      lderr(m_image_ctx->cct) << "unsupported cipher algorithm: " << m_cipher
                              << dendl;
      finish(-EINVAL);
      return;
  }

  // setup interface with libcryptsetup
  auto r = m_header.init();
  if (r < 0) {
    finish(r);
    return;
  }

  // format (create LUKS header)
  r = m_header.format(type, alg, key_size, "xts-plain64", sector_size,
                      m_image_ctx->get_object_size(), m_insecure_fast_mode);
  if (r != 0) {
    finish(r);
    return;
  }

  // add keyslot (volume key encrypted with passphrase)
  r = m_header.add_keyslot(m_passphrase.c_str(), m_passphrase.size());
  if (r != 0) {
    finish(r);
    return;
  }

  // read header from libcryptsetup interface
  ceph::bufferlist bl;
  r = m_header.read(&bl);
  if (r < 0) {
    finish(r);
    return;
  }

  // write header to offset 0 of the image
  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_write_header>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
          ctx, util::get_image_ctx(m_image_ctx), io::AIO_TYPE_WRITE);

  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_write(
          *m_image_ctx, io::IMAGE_DISPATCH_LAYER_API_START, aio_comp,
          {{0, bl.length()}}, std::move(bl),
          m_image_ctx->get_data_io_context(), 0, trace);
  req->send();
}

template <typename I>
void FormatRequest<I>::handle_write_header(int r) {
  if (r < 0) {
    lderr(m_image_ctx->cct) << "error writing header to image: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void FormatRequest<I>::finish(int r) {
  ceph_memzero_s(&m_passphrase[0], m_passphrase.capacity(), m_passphrase.size());
  m_on_finish->complete(r);
  delete this;
}

} // namespace luks
} // namespace crypto
} // namespace librbd

template class librbd::crypto::luks::FormatRequest<librbd::ImageCtx>;
