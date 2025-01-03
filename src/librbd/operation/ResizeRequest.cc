// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/ResizeRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/operation/TrimRequest.h"
#include "common/dout.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::operation::ResizeRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace operation {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
ResizeRequest<I>::ResizeRequest(I &image_ctx, Context *on_finish,
                                uint64_t new_size, bool allow_shrink, ProgressContext &prog_ctx,
                                uint64_t journal_op_tid, bool disable_journal)
  : Request<I>(image_ctx, on_finish, journal_op_tid),
    m_original_size(0), m_new_size(new_size), m_allow_shrink(allow_shrink),
    m_prog_ctx(prog_ctx), m_new_parent_overlap(0), m_disable_journal(disable_journal),
    m_xlist_item(this)
{
}

template <typename I>
ResizeRequest<I>::~ResizeRequest() {
  I &image_ctx = this->m_image_ctx;
  ResizeRequest *next_req = NULL;
  {
    std::unique_lock image_locker{image_ctx.image_lock};
    ceph_assert(m_xlist_item.remove_myself());
    if (!image_ctx.resize_reqs.empty()) {
      next_req = image_ctx.resize_reqs.front();
    }
  }

  if (next_req != NULL) {
    std::shared_lock owner_locker{image_ctx.owner_lock};
    next_req->send();
  }
}

template <typename I>
void ResizeRequest<I>::send() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  {
    std::unique_lock image_locker{image_ctx.image_lock};
    if (!m_xlist_item.is_on_list()) {
      image_ctx.resize_reqs.push_back(&m_xlist_item);
      if (image_ctx.resize_reqs.front() != this) {
        return;
      }
    }

    ceph_assert(image_ctx.resize_reqs.front() == this);
    m_original_size = image_ctx.size;
    compute_parent_overlap();
  }

  Request<I>::send();
}

template <typename I>
void ResizeRequest<I>::send_op() {
  [[maybe_unused]] I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  if (this->is_canceled()) {
    this->async_complete(-ERESTART);
  } else {
    send_pre_block_writes();
  }
}

template <typename I>
void ResizeRequest<I>::send_pre_block_writes() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  image_ctx.io_image_dispatcher->block_writes(create_context_callback<
    ResizeRequest<I>, &ResizeRequest<I>::handle_pre_block_writes>(this));
}

template <typename I>
Context *ResizeRequest<I>::handle_pre_block_writes(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*result) << dendl;
    image_ctx.io_image_dispatcher->unblock_writes();
    return this->create_context_finisher(*result);
  }

  return send_append_op_event();
}

template <typename I>
Context *ResizeRequest<I>::send_append_op_event() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;

  if (m_new_size < m_original_size && !m_allow_shrink) {
    ldout(cct, 1) << "shrinking the image is not permitted" << dendl;
    image_ctx.io_image_dispatcher->unblock_writes();
    this->async_complete(-EINVAL);
    return nullptr;
  }

  if (m_disable_journal || !this->template append_op_event<
        ResizeRequest<I>, &ResizeRequest<I>::handle_append_op_event>(this)) {
    return send_grow_object_map();
  }

  ldout(cct, 5) << dendl;
  return nullptr;
}

template <typename I>
Context *ResizeRequest<I>::handle_append_op_event(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to commit journal entry: " << cpp_strerror(*result)
               << dendl;
    image_ctx.io_image_dispatcher->unblock_writes();
    return this->create_context_finisher(*result);
  }

  return send_grow_object_map();
}

template <typename I>
void ResizeRequest<I>::send_trim_image() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  TrimRequest<I> *req = TrimRequest<I>::create(
    image_ctx, create_context_callback<
      ResizeRequest<I>, &ResizeRequest<I>::handle_trim_image>(this),
    m_original_size, m_new_size, m_prog_ctx);
  req->send();
}

template <typename I>
Context *ResizeRequest<I>::handle_trim_image(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  if (*result == -ERESTART) {
    ldout(cct, 5) << "resize operation interrupted" << dendl;
    return this->create_context_finisher(*result);
  } else if (*result < 0) {
    lderr(cct) << "failed to trim image: " << cpp_strerror(*result) << dendl;
    return this->create_context_finisher(*result);
  }

  send_post_block_writes();
  return nullptr;
}

template <typename I>
void ResizeRequest<I>::send_flush_cache() {
  I &image_ctx = this->m_image_ctx;

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  auto ctx = create_context_callback<
    ResizeRequest<I>, &ResizeRequest<I>::handle_flush_cache>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, util::get_image_ctx(&image_ctx), io::AIO_TYPE_FLUSH);
  auto req = io::ImageDispatchSpec::create_flush(
    image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
}

template <typename I>
Context *ResizeRequest<I>::handle_flush_cache(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to flush cache: " << cpp_strerror(*result) << dendl;
    return this->create_context_finisher(*result);
  }

  send_invalidate_cache();
  return nullptr;
}

template <typename I>
void ResizeRequest<I>::send_invalidate_cache() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  // need to invalidate since we're deleting objects, and
  // ObjectCacher doesn't track non-existent objects
  image_ctx.io_image_dispatcher->invalidate_cache(create_context_callback<
    ResizeRequest<I>, &ResizeRequest<I>::handle_invalidate_cache>(this));
}

template <typename I>
Context *ResizeRequest<I>::handle_invalidate_cache(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  // ignore busy error -- writeback was successfully flushed so we might be
  // wasting some cache space for trimmed objects, but they will get purged
  // eventually. Most likely cause of the issue was a in-flight cache read
  if (*result < 0 && *result != -EBUSY) {
    lderr(cct) << "failed to invalidate cache: " << cpp_strerror(*result)
               << dendl;
    return this->create_context_finisher(*result);
  }

  send_trim_image();
  return nullptr;
}

template <typename I>
Context *ResizeRequest<I>::send_grow_object_map() {
  I &image_ctx = this->m_image_ctx;

  {
    std::unique_lock image_locker{image_ctx.image_lock};
    m_shrink_size_visible = true;
  }

  if (m_original_size == m_new_size) {
    image_ctx.io_image_dispatcher->unblock_writes();
    return this->create_context_finisher(0);
  } else if (m_new_size < m_original_size) {
    image_ctx.io_image_dispatcher->unblock_writes();
    send_flush_cache();
    return nullptr;
  }

  image_ctx.owner_lock.lock_shared();
  image_ctx.image_lock.lock_shared();
  if (image_ctx.object_map == nullptr) {
    image_ctx.image_lock.unlock_shared();
    image_ctx.owner_lock.unlock_shared();

    // IO is still blocked
    send_update_header();
    return nullptr;
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  // should have been canceled prior to releasing lock
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  image_ctx.object_map->aio_resize(
    m_new_size, OBJECT_NONEXISTENT, create_context_callback<
      ResizeRequest<I>, &ResizeRequest<I>::handle_grow_object_map>(this));
  image_ctx.image_lock.unlock_shared();
  image_ctx.owner_lock.unlock_shared();
  return nullptr;
}

template <typename I>
Context *ResizeRequest<I>::handle_grow_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to resize object map: "
               << cpp_strerror(*result) << dendl;
    image_ctx.io_image_dispatcher->unblock_writes();
    return this->create_context_finisher(*result);
  }

  // IO is still blocked
  send_update_header();
  return nullptr;
}

template <typename I>
Context *ResizeRequest<I>::send_shrink_object_map() {
  I &image_ctx = this->m_image_ctx;

  image_ctx.owner_lock.lock_shared();
  image_ctx.image_lock.lock_shared();
  if (image_ctx.object_map == nullptr || m_new_size > m_original_size) {
    image_ctx.image_lock.unlock_shared();
    image_ctx.owner_lock.unlock_shared();

    update_size_and_overlap();
    return this->create_context_finisher(0);
  }

  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "original_size=" << m_original_size << ", "
                << "new_size=" << m_new_size << dendl;

  // should have been canceled prior to releasing lock
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  image_ctx.object_map->aio_resize(
    m_new_size, OBJECT_NONEXISTENT, create_context_callback<
      ResizeRequest<I>, &ResizeRequest<I>::handle_shrink_object_map>(this));
  image_ctx.image_lock.unlock_shared();
  image_ctx.owner_lock.unlock_shared();
  return nullptr;
}

template <typename I>
Context *ResizeRequest<I>::handle_shrink_object_map(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to resize object map: "
               << cpp_strerror(*result) << dendl;
    image_ctx.io_image_dispatcher->unblock_writes();
    return this->create_context_finisher(*result);
  }

  update_size_and_overlap();
  return this->create_context_finisher(0);
}

template <typename I>
void ResizeRequest<I>::send_post_block_writes() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << dendl;

  std::shared_lock owner_locker{image_ctx.owner_lock};
  image_ctx.io_image_dispatcher->block_writes(create_context_callback<
    ResizeRequest<I>, &ResizeRequest<I>::handle_post_block_writes>(this));
}

template <typename I>
Context *ResizeRequest<I>::handle_post_block_writes(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  if (*result < 0) {
    image_ctx.io_image_dispatcher->unblock_writes();
    lderr(cct) << "failed to block writes prior to header update: "
               << cpp_strerror(*result) << dendl;
    return this->create_context_finisher(*result);
  }

  send_update_header();
  return nullptr;
}

template <typename I>
void ResizeRequest<I>::send_update_header() {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "original_size=" << m_original_size << ", "
                << "new_size=" << m_new_size << dendl;;

  // should have been canceled prior to releasing lock
  std::shared_lock owner_locker{image_ctx.owner_lock};
  ceph_assert(image_ctx.exclusive_lock == nullptr ||
              image_ctx.exclusive_lock->is_lock_owner());

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    // rewrite only the size field of the header
    ceph_le64 new_size(m_new_size);
    bufferlist bl;
    bl.append(reinterpret_cast<const char*>(&new_size), sizeof(new_size));
    op.write(offsetof(rbd_obj_header_ondisk, image_size), bl);
  } else {
    cls_client::set_size(&op, m_new_size);
  }

  librados::AioCompletion *rados_completion = create_rados_callback<
    ResizeRequest<I>, &ResizeRequest<I>::handle_update_header>(this);
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid,
    				       rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *ResizeRequest<I>::handle_update_header(int *result) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << "r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to update image header: " << cpp_strerror(*result)
               << dendl;
    image_ctx.io_image_dispatcher->unblock_writes();
    return this->create_context_finisher(*result);
  }

  return send_shrink_object_map();
}

template <typename I>
void ResizeRequest<I>::compute_parent_overlap() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.image_lock));

  if (image_ctx.parent == NULL) {
    m_new_parent_overlap = 0;
  } else {
    m_new_parent_overlap = std::min(m_new_size, image_ctx.parent_md.overlap);
  }
}

template <typename I>
void ResizeRequest<I>::update_size_and_overlap() {
  I &image_ctx = this->m_image_ctx;
  {
    std::unique_lock image_locker{image_ctx.image_lock};
    image_ctx.size = m_new_size;

    if (image_ctx.parent != NULL && m_new_size < m_original_size) {
      image_ctx.parent_md.overlap = m_new_parent_overlap;
    }
  }

  // blocked by PRE_BLOCK_WRITES (grow) or POST_BLOCK_WRITES (shrink) state
  image_ctx.io_image_dispatcher->unblock_writes();
}

} // namespace operation
} // namespace librbd

template class librbd::operation::ResizeRequest<librbd::ImageCtx>;
