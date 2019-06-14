// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageCopyRequest.h"
#include "ObjectCopyRequest.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/Utils.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/OpenRequest.h"
#include "librbd/image/SetSnapRequest.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::ImageCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace deep_copy {

using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;

template <typename I>
ImageCopyRequest<I>::ImageCopyRequest(I *src_image_ctx, I *dst_image_ctx,
                                      librados::snap_t snap_id_start,
                                      librados::snap_t snap_id_end,
                                      bool flatten,
                                      const ObjectNumber &object_number,
                                      const SnapSeqs &snap_seqs,
                                      ProgressContext *prog_ctx,
                                      Context *on_finish)
  : RefCountedObject(dst_image_ctx->cct, 1), m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_snap_id_start(snap_id_start),
    m_snap_id_end(snap_id_end), m_flatten(flatten),
    m_object_number(object_number), m_snap_seqs(snap_seqs),
    m_prog_ctx(prog_ctx), m_on_finish(on_finish), m_cct(dst_image_ctx->cct),
    m_lock(unique_lock_name("ImageCopyRequest::m_lock", this)) {
}

template <typename I>
void ImageCopyRequest<I>::send() {
  util::compute_snap_map(m_snap_id_start, m_snap_id_end, m_snap_seqs,
                         &m_snap_map);
  if (m_snap_map.empty()) {
    lderr(m_cct) << "failed to map snapshots within boundary" << dendl;
    finish(-EINVAL);
    return;
  }

  send_open_parent();
}

template <typename I>
void ImageCopyRequest<I>::cancel() {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << dendl;
  m_canceled = true;
}

template <typename I>
void ImageCopyRequest<I>::send_open_parent() {
  {
    RWLock::RLocker snap_locker(m_src_image_ctx->snap_lock);
    RWLock::RLocker parent_locker(m_src_image_ctx->parent_lock);

    auto snap_id = m_snap_map.begin()->first;
    auto parent_info = m_src_image_ctx->get_parent_info(snap_id);
    if (parent_info == nullptr) {
        ldout(m_cct, 20) << "could not find parent info for snap id " << snap_id
                         << dendl;
    } else {
      m_parent_spec = parent_info->spec;
    }
  }

  if (m_parent_spec.pool_id == -1) {
    send_object_copies();
    return;
  }

  ldout(m_cct, 20) << "pool_id=" << m_parent_spec.pool_id << ", image_id="
                   << m_parent_spec.image_id << ", snap_id="
                   << m_parent_spec.snap_id << dendl;

  librados::Rados rados(m_src_image_ctx->md_ctx);
  librados::IoCtx parent_io_ctx;
  int r = rados.ioctx_create2(m_parent_spec.pool_id, parent_io_ctx);
  if (r < 0) {
    lderr(m_cct) << "failed to access parent pool (id=" << m_parent_spec.pool_id
                 << "): " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_src_parent_image_ctx = I::create("", m_parent_spec.image_id, nullptr, parent_io_ctx, true);

  auto ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_open_parent>(this);

  auto req = image::OpenRequest<I>::create(m_src_parent_image_ctx, false, ctx);
  req->send();
}

template <typename I>
void ImageCopyRequest<I>::handle_open_parent(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to open parent: " << cpp_strerror(r) << dendl;
    m_src_parent_image_ctx->destroy();
    m_src_parent_image_ctx = nullptr;
    finish(r);
    return;
  }

  send_set_parent_snap();
}

template <typename I>
void ImageCopyRequest<I>::send_set_parent_snap() {
  ldout(m_cct, 20) << dendl;

  auto ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_set_parent_snap>(this);
  auto req = image::SetSnapRequest<I>::create(*m_src_parent_image_ctx,
                                              m_parent_spec.snap_id, ctx);
  req->send();
}

template <typename I>
void ImageCopyRequest<I>::handle_set_parent_snap(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to set parent snap: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
    send_close_parent();
    return;
  }

  send_object_copies();
}

template <typename I>
void ImageCopyRequest<I>::send_object_copies() {
  m_object_no = 0;
  if (m_object_number) {
    m_object_no = *m_object_number + 1;
  }

  uint64_t size;
  {
    RWLock::RLocker snap_locker(m_src_image_ctx->snap_lock);
    size =  m_src_image_ctx->get_image_size(CEPH_NOSNAP);
    for (auto snap_id : m_src_image_ctx->snaps) {
      size = std::max(size, m_src_image_ctx->get_image_size(snap_id));
    }
  }
  m_end_object_no = Striper::get_num_objects(m_dst_image_ctx->layout, size);

  ldout(m_cct, 20) << "start_object=" << m_object_no << ", "
                   << "end_object=" << m_end_object_no << dendl;

  bool complete;
  {
    Mutex::Locker locker(m_lock);
    for (int i = 0;
         i < m_cct->_conf->get_val<int64_t>("rbd_concurrent_management_ops");
         ++i) {
      send_next_object_copy();
      if (m_ret_val < 0 && m_current_ops == 0) {
        break;
      }
    }
    complete = (m_current_ops == 0) && !m_updating_progress;
  }

  if (complete) {
    send_close_parent();
  }
}

template <typename I>
void ImageCopyRequest<I>::send_next_object_copy() {
  assert(m_lock.is_locked());

  if (m_canceled && m_ret_val == 0) {
    ldout(m_cct, 10) << "image copy canceled" << dendl;
    m_ret_val = -ECANCELED;
  }

  if (m_ret_val < 0 || m_object_no >= m_end_object_no) {
    return;
  }

  uint64_t ono = m_object_no++;

  ldout(m_cct, 20) << "object_num=" << ono << dendl;

  ++m_current_ops;

  Context *ctx = new FunctionContext(
    [this, ono](int r) {
      handle_object_copy(ono, r);
    });
  ObjectCopyRequest<I> *req = ObjectCopyRequest<I>::create(
      m_src_image_ctx, m_src_parent_image_ctx, m_dst_image_ctx, m_snap_map, ono,
      m_flatten, ctx);
  req->send();
}

template <typename I>
void ImageCopyRequest<I>::handle_object_copy(uint64_t object_no, int r) {
  ldout(m_cct, 20) << "object_no=" << object_no << ", r=" << r << dendl;

  bool complete;
  {
    Mutex::Locker locker(m_lock);
    assert(m_current_ops > 0);
    --m_current_ops;

    if (r < 0) {
      lderr(m_cct) << "object copy failed: " << cpp_strerror(r) << dendl;
      if (m_ret_val == 0) {
        m_ret_val = r;
      }
    } else {
      m_copied_objects.push(object_no);
      while (!m_updating_progress && !m_copied_objects.empty() &&
             m_copied_objects.top() ==
               (m_object_number ? *m_object_number + 1 : 0)) {
        m_object_number = m_copied_objects.top();
        m_copied_objects.pop();
        uint64_t progress_object_no = *m_object_number + 1;
        m_updating_progress = true;
        m_lock.Unlock();
        m_prog_ctx->update_progress(progress_object_no, m_end_object_no);
        m_lock.Lock();
        assert(m_updating_progress);
        m_updating_progress = false;
      }
    }

    send_next_object_copy();
    complete = (m_current_ops == 0) && !m_updating_progress;
  }

  if (complete) {
    send_close_parent();
  }
}

template <typename I>
void ImageCopyRequest<I>::send_close_parent() {
  if (m_src_parent_image_ctx == nullptr) {
    finish(m_ret_val);
    return;
  }

  ldout(m_cct, 20) << dendl;

  auto ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_close_parent>(this);
  auto req = image::CloseRequest<I>::create(m_src_parent_image_ctx, ctx);
  req->send();
}

template <typename I>
void ImageCopyRequest<I>::handle_close_parent(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to close parent: " << cpp_strerror(r) << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
  }

  m_src_parent_image_ctx->destroy();
  m_src_parent_image_ctx = nullptr;

  finish(m_ret_val);
}

template <typename I>
void ImageCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  put();
}

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::ImageCopyRequest<librbd::ImageCtx>;
