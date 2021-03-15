// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Utils.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/crypto/BlockCrypto.h"
#include "librbd/crypto/CryptoImageDispatch.h"
#include "librbd/crypto/CryptoObjectDispatch.h"
#include "librbd/crypto/openssl/DataCryptor.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/ObjectDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::util: " << __func__ << ": "

namespace librbd {
namespace crypto {
namespace util {

template <typename I>
void set_crypto(I *image_ctx, ceph::ref_t<CryptoInterface> crypto) {
  {
    std::unique_lock image_locker{image_ctx->image_lock};
    ceph_assert(image_ctx->crypto == nullptr);
    image_ctx->crypto = crypto.get();
  }
  auto object_dispatch = CryptoObjectDispatch<I>::create(image_ctx, crypto);
  auto image_dispatch = CryptoImageDispatch::create(crypto->get_data_offset());
  image_ctx->io_object_dispatcher->register_dispatch(object_dispatch);
  image_ctx->io_image_dispatcher->register_dispatch(image_dispatch);
}

int build_crypto(
        CephContext* cct, const unsigned char* key, uint32_t key_length,
        uint64_t block_size, uint64_t data_offset,
        ceph::ref_t<CryptoInterface>* result_crypto) {
  const char* cipher_suite;
  switch (key_length) {
    case 32:
      cipher_suite = "aes-128-xts";
      break;
    case 64:
      cipher_suite = "aes-256-xts";
      break;
    default:
      lderr(cct) << "unsupported key length: " << key_length << dendl;
      return -ENOTSUP;
  }

  auto data_cryptor = new openssl::DataCryptor(cct);
  int r = data_cryptor->init(cipher_suite, key, key_length);
  if (r != 0) {
    lderr(cct) << "error initializing data cryptor: " << cpp_strerror(r)
               << dendl;
    delete data_cryptor;
    return r;
  }

  *result_crypto = BlockCrypto<EVP_CIPHER_CTX>::create(
          cct, data_cryptor, block_size, data_offset);
  return 0;
}

} // namespace util
} // namespace crypto
} // namespace librbd

template void librbd::crypto::util::set_crypto(
    librbd::ImageCtx *image_ctx, ceph::ref_t<CryptoInterface> crypto);
