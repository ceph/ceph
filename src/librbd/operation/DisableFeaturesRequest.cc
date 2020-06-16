// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/DisableFeaturesRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"
#include "librbd/image/SetFlagsRequest.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/journal/RemoveRequest.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/object_map/RemoveRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::DisableFeaturesRequest: "

namespace librbd {
namespace operation {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
DisableFeaturesRequest<I>::DisableFeaturesRequest(I &image_ctx,
                                                  Context *on_finish,
                                                  uint64_t journal_op_tid,
                                                  uint64_t features,
                                                  bool force)
  : Request<I>(image_ctx, on_finish, journal_op_tid), m_features(features),
    m_force(force) {
}

template <typename I>
void DisableFeaturesRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  ldout(cct, 20) << this << " " << __func__ << ": features=" << m_features
		 << dendl;

  send_prepare_lock();
}

template <typename I>
bool DisableFeaturesRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << " r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "encountered error: " << cpp_strerror(r) << dendl;
  }
  return true;
}

template <typename I>
void DisableFeaturesRequest<I>::send_prepare_lock() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  image_ctx.state->prepare_lock(create_async_context_callback(
    image_ctx, create_context_callback<
    DisableFeaturesRequest<I>,
    &DisableFeaturesRequest<I>::handle_prepare_lock>(this)));
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_prepare_lock(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to lock image: " << cpp_strerror(*result) << dendl;
    return this->create_context_finisher(*result);
  }

  send_block_writes();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_block_writes() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  std::unique_lock locker{image_ctx.owner_lock};
  image_ctx.io_image_dispatcher->block_writes(create_context_callback<
    DisableFeaturesRequest<I>,
    &DisableFeaturesRequest<I>::handle_block_writes>(this));
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_block_writes(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result) << dendl;
    return handle_finish(*result);
  }
  m_writes_blocked = true;

  {
    std::unique_lock locker{image_ctx.owner_lock};
    // avoid accepting new requests from peers while we manipulate
    // the image features
    if (image_ctx.exclusive_lock != nullptr &&
	(image_ctx.journal == nullptr ||
	 !image_ctx.journal->is_journal_replaying())) {
      image_ctx.exclusive_lock->block_requests(0);
      m_requests_blocked = true;
    }
  }

  return send_acquire_exclusive_lock(result);
}

template <typename I>
Context *DisableFeaturesRequest<I>::send_acquire_exclusive_lock(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  {
    std::unique_lock locker{image_ctx.owner_lock};
    // if disabling features w/ exclusive lock supported, we need to
    // acquire the lock to temporarily block IO against the image
    if (image_ctx.exclusive_lock != nullptr &&
        !image_ctx.exclusive_lock->is_lock_owner()) {
      m_acquired_lock = true;

      Context *ctx = create_context_callback<
        DisableFeaturesRequest<I>,
        &DisableFeaturesRequest<I>::handle_acquire_exclusive_lock>(
          this, image_ctx.exclusive_lock);
      image_ctx.exclusive_lock->acquire_lock(ctx);
      return nullptr;
    }
  }

  return handle_acquire_exclusive_lock(result);
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_acquire_exclusive_lock(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  image_ctx.owner_lock.lock_shared();
  if (*result < 0) {
    lderr(cct) << "failed to lock image: " << cpp_strerror(*result) << dendl;
    image_ctx.owner_lock.unlock_shared();
    return handle_finish(*result);
  } else if (image_ctx.exclusive_lock != nullptr &&
             !image_ctx.exclusive_lock->is_lock_owner()) {
    lderr(cct) << "failed to acquire exclusive lock" << dendl;
    *result = image_ctx.exclusive_lock->get_unlocked_op_error();
    image_ctx.owner_lock.unlock_shared();
    return handle_finish(*result);
  }

  do {
    m_features &= image_ctx.features;

    // interlock object-map and fast-diff together
    if (((m_features & RBD_FEATURE_OBJECT_MAP) != 0) ||
        ((m_features & RBD_FEATURE_FAST_DIFF) != 0)) {
      m_features |= (RBD_FEATURE_OBJECT_MAP | RBD_FEATURE_FAST_DIFF);
    }

    m_new_features = image_ctx.features & ~m_features;
    m_features_mask = m_features;

    if ((m_features & RBD_FEATURE_EXCLUSIVE_LOCK) != 0) {
      if ((m_new_features & RBD_FEATURE_OBJECT_MAP) != 0 ||
          (m_new_features & RBD_FEATURE_JOURNALING) != 0) {
        lderr(cct) << "cannot disable exclusive-lock. object-map "
                      "or journaling must be disabled before "
                      "disabling exclusive-lock." << dendl;
        *result = -EINVAL;
        break;
      }
      m_features_mask |= (RBD_FEATURE_OBJECT_MAP |
                          RBD_FEATURE_FAST_DIFF |
                          RBD_FEATURE_JOURNALING);
    }
    if ((m_features & RBD_FEATURE_FAST_DIFF) != 0) {
      m_disable_flags |= RBD_FLAG_FAST_DIFF_INVALID;
    }
    if ((m_features & RBD_FEATURE_OBJECT_MAP) != 0) {
      m_disable_flags |= RBD_FLAG_OBJECT_MAP_INVALID;
    }
  } while (false);
  image_ctx.owner_lock.unlock_shared();

  if (*result < 0) {
    return handle_finish(*result);
  }

  send_get_mirror_mode();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_get_mirror_mode() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if ((m_features & RBD_FEATURE_JOURNALING) == 0) {
    send_append_op_event();
    return;
  }

  ldout(cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_mode_get_start(&op);

  using klass = DisableFeaturesRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_mirror_mode>(this);
  m_out_bl.clear();
  int r = image_ctx.md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_get_mirror_mode(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == 0) {
    auto it = m_out_bl.cbegin();
    *result = cls_client::mirror_mode_get_finish(&it, &m_mirror_mode);
  }

  if (*result < 0 && *result != -ENOENT) {
    lderr(cct) << "failed to retrieve pool mirror mode: "
               << cpp_strerror(*result) << dendl;
    return handle_finish(*result);
  }

  ldout(cct, 20) << this << " " << __func__ << ": m_mirror_mode="
                 << m_mirror_mode << dendl;

  send_get_mirror_image();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_get_mirror_image() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (m_mirror_mode != cls::rbd::MIRROR_MODE_IMAGE) {
    send_disable_mirror_image();
    return;
  }

  ldout(cct, 20) << this << " " << __func__ << dendl;

  librados::ObjectReadOperation op;
  cls_client::mirror_image_get_start(&op, image_ctx.id);

  using klass = DisableFeaturesRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_get_mirror_image>(this);
  m_out_bl.clear();
  int r = image_ctx.md_ctx.aio_operate(RBD_MIRRORING, comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_get_mirror_image(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  cls::rbd::MirrorImage mirror_image;

  if (*result == 0) {
    auto it = m_out_bl.cbegin();
    *result = cls_client::mirror_image_get_finish(&it, &mirror_image);
  }

  if (*result < 0 && *result != -ENOENT) {
    lderr(cct) << "failed to retrieve pool mirror image: "
               << cpp_strerror(*result) << dendl;
    return handle_finish(*result);
  }

  if ((mirror_image.state == cls::rbd::MIRROR_IMAGE_STATE_ENABLED) && !m_force) {
    lderr(cct) << "cannot disable journaling: image mirroring "
               << "enabled and mirror pool mode set to image"
               << dendl;
    *result = -EINVAL;
    return handle_finish(*result);
  }

  send_disable_mirror_image();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_disable_mirror_image() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  ldout(cct, 20) << this << " " << __func__ << dendl;

  Context *ctx = create_context_callback<
    DisableFeaturesRequest<I>,
    &DisableFeaturesRequest<I>::handle_disable_mirror_image>(this);

  mirror::DisableRequest<I> *req =
    mirror::DisableRequest<I>::create(&image_ctx, m_force, true, ctx);
  req->send();
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_disable_mirror_image(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to disable image mirroring: " << cpp_strerror(*result)
               << dendl;
    // not fatal
  }

  send_close_journal();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_close_journal() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  {
    std::unique_lock locker{image_ctx.owner_lock};
    if (image_ctx.journal != nullptr) {
      ldout(cct, 20) << this << " " << __func__ << dendl;

      std::swap(m_journal, image_ctx.journal);
      Context *ctx = create_context_callback<
	DisableFeaturesRequest<I>,
	&DisableFeaturesRequest<I>::handle_close_journal>(this);

      m_journal->close(ctx);
      return;
    }
  }

  send_remove_journal();
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_close_journal(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to close image journal: " << cpp_strerror(*result)
               << dendl;
  }

  ceph_assert(m_journal != nullptr);
  m_journal->put();
  m_journal = nullptr;

  send_remove_journal();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_remove_journal() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Context *ctx = create_context_callback<
    DisableFeaturesRequest<I>,
    &DisableFeaturesRequest<I>::handle_remove_journal>(this);

  typename journal::TypeTraits<I>::ContextWQ* context_wq;
  Journal<I>::get_work_queue(cct, &context_wq);

  journal::RemoveRequest<I> *req = journal::RemoveRequest<I>::create(
    image_ctx.md_ctx, image_ctx.id, librbd::Journal<>::IMAGE_CLIENT_ID,
    context_wq, ctx);

  req->send();
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_remove_journal(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to remove image journal: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_append_op_event();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_append_op_event() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (!this->template append_op_event<
      DisableFeaturesRequest<I>,
      &DisableFeaturesRequest<I>::handle_append_op_event>(this)) {
    send_remove_object_map();
  }

  ldout(cct, 20) << this << " " << __func__ << dendl;
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_append_op_event(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to commit journal entry: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_remove_object_map();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_remove_object_map() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  if ((m_features & RBD_FEATURE_OBJECT_MAP) == 0) {
    send_set_features();
    return;
  }

  Context *ctx = create_context_callback<
    DisableFeaturesRequest<I>,
    &DisableFeaturesRequest<I>::handle_remove_object_map>(this);

  object_map::RemoveRequest<I> *req =
    object_map::RemoveRequest<I>::create(&image_ctx, ctx);
  req->send();
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_remove_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to remove object map: " << cpp_strerror(*result) << dendl;
    return handle_finish(*result);
  }

  send_set_features();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_set_features() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": new_features="
		 << m_new_features << ", features_mask=" << m_features_mask
		 << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::set_features(&op, m_new_features, m_features_mask);

  using klass = DisableFeaturesRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_set_features>(this);
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_set_features(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result == -EINVAL && (m_features_mask & RBD_FEATURE_JOURNALING) != 0) {
    // NOTE: infernalis OSDs will not accept a mask with new features, so
    // re-attempt with a reduced mask.
    ldout(cct, 5) << this << " " << __func__
                  << ": re-attempt with a reduced mask" << dendl;
    m_features_mask &= ~RBD_FEATURE_JOURNALING;
    send_set_features();
  }

  if (*result < 0) {
    lderr(cct) << "failed to update features: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_update_flags();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_update_flags() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (m_disable_flags == 0) {
    send_notify_update();
    return;
  }

  ldout(cct, 20) << this << " " << __func__ << ": disable_flags="
		 << m_disable_flags << dendl;

  Context *ctx = create_context_callback<
    DisableFeaturesRequest<I>,
    &DisableFeaturesRequest<I>::handle_update_flags>(this);

  image::SetFlagsRequest<I> *req =
    image::SetFlagsRequest<I>::create(&image_ctx, 0, m_disable_flags, ctx);
  req->send();
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_update_flags(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to update image flags: " << cpp_strerror(*result)
               << dendl;
    return handle_finish(*result);
  }

  send_notify_update();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_notify_update() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Context *ctx = create_context_callback<
    DisableFeaturesRequest<I>,
    &DisableFeaturesRequest<I>::handle_notify_update>(this);

  image_ctx.notify_update(ctx);
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_notify_update(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  if (image_ctx.exclusive_lock == nullptr || !m_acquired_lock) {
    return handle_finish(*result);
  }

  send_release_exclusive_lock();
  return nullptr;
}

template <typename I>
void DisableFeaturesRequest<I>::send_release_exclusive_lock() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << dendl;

  Context *ctx = create_context_callback<
    DisableFeaturesRequest<I>,
    &DisableFeaturesRequest<I>::handle_release_exclusive_lock>(
      this, image_ctx.exclusive_lock);

  image_ctx.exclusive_lock->release_lock(ctx);
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_release_exclusive_lock(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << *result << dendl;

  return handle_finish(*result);
}

template <typename I>
Context *DisableFeaturesRequest<I>::handle_finish(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 20) << this << " " << __func__ << ": r=" << r << dendl;

  {
    std::unique_lock locker{image_ctx.owner_lock};
    if (image_ctx.exclusive_lock != nullptr && m_requests_blocked) {
      image_ctx.exclusive_lock->unblock_requests();
    }

    image_ctx.io_image_dispatcher->unblock_writes();
  }
  image_ctx.state->handle_prepare_lock_complete();

  return this->create_context_finisher(r);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::DisableFeaturesRequest<librbd::ImageCtx>;
