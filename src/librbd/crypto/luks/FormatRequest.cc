// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FormatRequest.h"

#include <stdlib.h>
#include <openssl/rand.h>
#include "common/dout.h"
#include "common/errno.h"
#include "include/compat.h"
#include "librbd/Utils.h"
#include "librbd/crypto/Utils.h"
#include "librbd/crypto/luks/Header.h"
#include "librbd/crypto/luks/Magic.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"

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
        I* image_ctx, encryption_format_t format, encryption_algorithm_t alg,
        std::string_view passphrase,
        std::unique_ptr<CryptoInterface>* result_crypto, Context* on_finish,
        bool insecure_fast_mode) : m_image_ctx(image_ctx), m_format(format),
                                   m_alg(alg),
                                   m_passphrase(passphrase),
                                   m_result_crypto(result_crypto),
                                   m_on_finish(on_finish),
                                   m_insecure_fast_mode(insecure_fast_mode),
                                   m_header(image_ctx->cct) {
}

template <typename I>
void FormatRequest<I>::send() {
  const char* type;
  size_t sector_size;
  switch (m_format) {
    case RBD_ENCRYPTION_FORMAT_LUKS1:
      type = CRYPT_LUKS1;
      sector_size = 512;
      break;
    case RBD_ENCRYPTION_FORMAT_LUKS2:
      type = CRYPT_LUKS2;
      sector_size = 4096;
      break;
    default:
      lderr(m_image_ctx->cct) << "unsupported format type: " << m_format
                              << dendl;
      finish(-EINVAL);
      return;
  }

  const char* cipher;
  size_t key_size;
  switch (m_alg) {
    case RBD_ENCRYPTION_ALGORITHM_AES128:
      cipher = "aes";
      key_size = 32;
      break;
    case RBD_ENCRYPTION_ALGORITHM_AES256:
      cipher = "aes";
      key_size = 64;
      break;
    default:
      lderr(m_image_ctx->cct) << "unsupported cipher algorithm: " << m_alg
                              << dendl;
      finish(-EINVAL);
      return;
  }

  // generate encryption key
  unsigned char* key = (unsigned char*)alloca(key_size);
  if (RAND_bytes((unsigned char *)key, key_size) != 1) {
    lderr(m_image_ctx->cct) << "cannot generate random encryption key"
                            << dendl;
    finish(-EAGAIN);
    return;
  }

  // setup interface with libcryptsetup
  auto r = m_header.init();
  if (r < 0) {
    finish(r);
    return;
  }

  // format (create LUKS header)
  auto stripe_period = m_image_ctx->get_stripe_period();
  r = m_header.format(type, cipher, reinterpret_cast<char*>(key), key_size,
                      "xts-plain64", sector_size, stripe_period,
                      m_insecure_fast_mode);
  if (r != 0) {
    finish(r);
    return;
  }

  m_image_ctx->image_lock.lock_shared();
  uint64_t image_size = m_image_ctx->get_image_size(CEPH_NOSNAP);
  m_image_ctx->image_lock.unlock_shared();

  if (m_header.get_data_offset() > image_size) {
    lderr(m_image_ctx->cct) << "image is too small, format requires "
                            << m_header.get_data_offset() << " bytes" << dendl;
    finish(-ENOSPC);
    return;
  }

  // add keyslot (volume key encrypted with passphrase)
  r = m_header.add_keyslot(m_passphrase.data(), m_passphrase.size());
  if (r != 0) {
    finish(r);
    return;
  }

  r = util::build_crypto(m_image_ctx->cct, key, key_size,
                         m_header.get_sector_size(),
                         m_header.get_data_offset(), m_result_crypto);
  ceph_memzero_s(key, key_size, key_size);
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

  if (m_image_ctx->parent != nullptr) {
    // parent is not encrypted with same key
    // change LUKS magic to prevent decryption by other LUKS implementations
    r = Magic::replace_magic(m_image_ctx->cct, bl);
    if (r < 0) {
      lderr(m_image_ctx->cct) << "error replacing LUKS magic: "
                              << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  }

  // pad header to stripe period alignment to prevent copyup of parent data
  // when writing encryption header to the child image
  auto alignment = bl.length() % stripe_period;
  if (alignment > 0) {
    bl.append_zero(stripe_period - alignment);
  }

  // write header to offset 0 of the image
  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_write_header>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
          ctx, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_WRITE);

  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_write(
          *m_image_ctx, io::IMAGE_DISPATCH_LAYER_API_START, aio_comp,
          {{0, bl.length()}}, io::ImageArea::DATA, std::move(bl), 0, trace);
  req->send();
}

template <typename I>
void FormatRequest<I>::handle_write_header(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

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
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace luks
} // namespace crypto
} // namespace librbd

template class librbd::crypto::luks::FormatRequest<librbd::ImageCtx>;
