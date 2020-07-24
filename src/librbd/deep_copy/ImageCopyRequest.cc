// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageCopyRequest.h"
#include "ObjectCopyRequest.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/deep_copy/Utils.h"
#include "librbd/object_map/DiffRequest.h"
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
                                      librados::snap_t src_snap_id_start,
                                      librados::snap_t src_snap_id_end,
                                      librados::snap_t dst_snap_id_start,
                                      bool flatten,
                                      const ObjectNumber &object_number,
                                      const SnapSeqs &snap_seqs,
                                      Handler *handler,
                                      Context *on_finish)
  : RefCountedObject(dst_image_ctx->cct), m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_src_snap_id_start(src_snap_id_start),
    m_src_snap_id_end(src_snap_id_end), m_dst_snap_id_start(dst_snap_id_start),
    m_flatten(flatten), m_object_number(object_number), m_snap_seqs(snap_seqs),
    m_handler(handler), m_on_finish(on_finish), m_cct(dst_image_ctx->cct),
    m_lock(ceph::make_mutex(unique_lock_name("ImageCopyRequest::m_lock", this))) {
}

template <typename I>
void ImageCopyRequest<I>::send() {
  m_dst_image_ctx->image_lock.lock_shared();
  util::compute_snap_map(m_dst_image_ctx->cct, m_src_snap_id_start,
                         m_src_snap_id_end, m_dst_image_ctx->snaps, m_snap_seqs,
                         &m_snap_map);
  m_dst_image_ctx->image_lock.unlock_shared();

  if (m_snap_map.empty()) {
    lderr(m_cct) << "failed to map snapshots within boundary" << dendl;
    finish(-EINVAL);
    return;
  }

  compute_diff();
}

template <typename I>
void ImageCopyRequest<I>::cancel() {
  std::lock_guard locker{m_lock};

  ldout(m_cct, 20) << dendl;
  m_canceled = true;
}

template <typename I>
void ImageCopyRequest<I>::compute_diff() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_compute_diff>(this);
  auto req = object_map::DiffRequest<I>::create(m_src_image_ctx, m_src_snap_id_start,
                                                m_src_snap_id_end, &m_object_diff_state,
                                                ctx);
  req->send();
}

template <typename I>
void ImageCopyRequest<I>::handle_compute_diff(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    ldout(m_cct, 10) << "fast-diff optimization disabled" << dendl;
    m_object_diff_state.resize(0);
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
    std::shared_lock image_locker{m_src_image_ctx->image_lock};
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
    std::lock_guard locker{m_lock};
    auto max_ops = m_src_image_ctx->config.template get_val<uint64_t>(
      "rbd_concurrent_management_ops");

    // attempt to schedule at least 'max_ops' initial requests where
    // some objects might be skipped if fast-diff notes no change
    while (m_current_ops < max_ops) {
      int r = send_next_object_copy();
      if (r < 0) {
        break;
      }
    }

    complete = (m_current_ops == 0) && !m_updating_progress;
  }

  if (complete) {
    finish(m_ret_val);
  }
}

template <typename I>
int ImageCopyRequest<I>::send_next_object_copy() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  if (m_canceled && m_ret_val == 0) {
    ldout(m_cct, 10) << "image copy canceled" << dendl;
    m_ret_val = -ECANCELED;
  }

  if (m_ret_val < 0) {
    return m_ret_val;
  } else if (m_object_no >= m_end_object_no) {
    return -ENODATA;
  }

  uint64_t ono = m_object_no++;
  if (ono < m_object_diff_state.size() &&
      m_object_diff_state[ono] == object_map::DIFF_STATE_NONE) {
    ldout(m_cct, 20) << "skipping clean object " << ono << dendl;
    return 1;
  }

  ldout(m_cct, 20) << "object_num=" << ono << dendl;
  ++m_current_ops;

  Context *ctx = new LambdaContext(
    [this, ono](int r) {
      handle_object_copy(ono, r);
    });
  auto req = ObjectCopyRequest<I>::create(
    m_src_image_ctx, m_dst_image_ctx, m_src_snap_id_start, m_dst_snap_id_start,
    m_snap_map, ono, m_flatten, m_handler, ctx);
  req->send();
  return 0;
}

template <typename I>
void ImageCopyRequest<I>::handle_object_copy(uint64_t object_no, int r) {
  ldout(m_cct, 20) << "object_no=" << object_no << ", r=" << r << dendl;

  bool complete;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_current_ops > 0);
    --m_current_ops;

    if (r < 0 && r != -ENOENT) {
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
        m_lock.unlock();
        m_handler->update_progress(progress_object_no, m_end_object_no);
        m_lock.lock();
        ceph_assert(m_updating_progress);
        m_updating_progress = false;
      }
    }

    while (true) {
      r = send_next_object_copy();
      if (r != 1) {
        break;
      }
    }

    complete = (m_current_ops == 0) && !m_updating_progress;
  }

  if (complete) {
    finish(m_ret_val);
  }
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
