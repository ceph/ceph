// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FlattenRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "librbd/io/Utils.h"
#include "librbd/operation/MetadataRemoveRequest.h"
#include "osdc/Striper.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::FlattenRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
class C_FlattenObject : public C_AsyncObjectThrottle<I> {
public:
  C_FlattenObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                  IOContext io_context, uint64_t object_no)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_io_context(io_context),
      m_object_no(object_no) {
  }

  int send() override {
    if (this->m_image_ctx.object_map != nullptr &&
        !this->m_image_ctx.object_map->object_may_not_exist(m_object_no)) {
      // can skip because the object already exists
      return 1;
    }

    if (!io::util::trigger_copyup(
            &this->m_image_ctx, m_object_no, m_io_context, this)) {
      // stop early if the parent went away - it just means
      // another flatten finished first or the image was resized
      return 1;
    }

    return 0;
  }

private:
  IOContext m_io_context;
  uint64_t m_object_no;
};

template <typename I>
FlattenRequest<I>::FlattenRequest(
        I* image_ctx, EncryptionFormat<I>* format,
        Context* on_finish) : m_image_ctx(image_ctx),
                              m_format(format),
                              m_raw_image_ctx(nullptr),
                              m_on_finish(on_finish) {
}

template <typename I>
void FlattenRequest<I>::send() {
  {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    if (m_format == nullptr) {
      if (m_image_ctx->crypto == nullptr) {
        // no encryption
        finish(0);
        return;
      }

      lderr(m_image_ctx->cct) << "missing encryption format" << dendl;
      finish(-EINVAL);
      return;
    }
  }

  m_raw_image_ctx = I::create(m_image_ctx->name, m_image_ctx->id, nullptr,
                              m_image_ctx->data_ctx, false);

  auto ctx = create_context_callback<
          FlattenRequest<I>, &FlattenRequest<I>::handle_create_raw_ctx>(this);
  m_raw_image_ctx->state->open(0, ctx);
}

template <typename I>
void FlattenRequest<I>::handle_create_raw_ctx(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "unable to open raw image context: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  ceph_assert(m_image_ctx->exclusive_lock == nullptr ||
              m_image_ctx->exclusive_lock->is_lock_owner());
  if (m_raw_image_ctx->exclusive_lock != nullptr) {
    m_raw_image_ctx->exclusive_lock->unset_require_lock(io::DIRECTION_BOTH);
  }

  flatten_crypto_header();
}

template <typename I>
void FlattenRequest<I>::flatten_crypto_header() {
  auto crypto = m_image_ctx->get_crypto();
  if (crypto == nullptr) {
    // no encryption
    m_return = 0;
    close_raw_ctx();
    return;
  }

  uint64_t header_objects = Striper::get_num_objects(
          m_raw_image_ctx->layout, crypto->get_data_offset());
  crypto->put();

  auto ctx = create_context_callback<
          FlattenRequest<I>,
          &FlattenRequest<I>::handle_flatten_crypto_header>(this);
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_FlattenObject<I> >(),
                        boost::lambda::_1, m_raw_image_ctx,
                        m_raw_image_ctx->get_data_io_context(),
                        boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    nullptr, *m_raw_image_ctx, context_factory, ctx, nullptr, 0,
    header_objects);
  throttle->start_ops(
    m_image_ctx->config.template get_val<uint64_t>(
            "rbd_concurrent_management_ops"));
}

template <typename I>
void FlattenRequest<I>::handle_flatten_crypto_header(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "error flattening crypto header: "
                            << cpp_strerror(r) << dendl;
    m_return = r;
    close_raw_ctx();
    return;
  }

  metadata_remove();
}

template <typename I>
void FlattenRequest<I>::metadata_remove() {
  auto ctx = create_context_callback<
          FlattenRequest<I>, &FlattenRequest<I>::handle_metadata_remove>(this);
  auto *request = operation::MetadataRemoveRequest<I>::create(
          *m_image_ctx, ctx, EncryptionFormat<I>::PARENT_CRYPTOR_METADATA_KEY);
  request->send();
}

template <typename I>
void FlattenRequest<I>::handle_metadata_remove(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "error removing parent cryptor metadata: "
                            << cpp_strerror(r) << dendl;
    m_return = r;
    close_raw_ctx();
    return;
  }

  crypto_flatten();
}

template <typename I>
void FlattenRequest<I>::crypto_flatten() {
  auto ctx = create_context_callback<
          FlattenRequest<I>, &FlattenRequest<I>::handle_crypto_flatten>(this);
  m_format->flatten(m_raw_image_ctx, ctx);
}

template <typename I>
void FlattenRequest<I>::handle_crypto_flatten(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "unable to crypto flatten: " << cpp_strerror(r)
                            << dendl;
  }

  m_return = r;
  close_raw_ctx();
}

template <typename I>
void FlattenRequest<I>::close_raw_ctx() {
  auto ctx = create_context_callback<
          FlattenRequest<I>, &FlattenRequest<I>::handle_raw_ctx_close>(this);
  m_raw_image_ctx->state->close(ctx);
}

template <typename I>
void FlattenRequest<I>::handle_raw_ctx_close(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    if (m_return >= 0) {
      m_return = r;
    }
    lderr(m_image_ctx->cct) << "unable to close raw image: " << cpp_strerror(r)
                            << dendl;
  }

  finish(m_return);
}

template <typename I>
void FlattenRequest<I>::finish(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::FlattenRequest<librbd::ImageCtx>;
