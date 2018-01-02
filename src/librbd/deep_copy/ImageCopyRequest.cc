// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageCopyRequest.h"
#include "ObjectCopyRequest.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/deep_copy/Utils.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::ImageCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace deep_copy {

using librbd::util::unique_lock_name;

template <typename I>
ImageCopyRequest<I>::ImageCopyRequest(I *src_image_ctx, I *dst_image_ctx,
                                      librados::snap_t snap_id_start,
                                      librados::snap_t snap_id_end,
                                      const ObjectNumber &object_number,
                                      const SnapSeqs &snap_seqs,
                                      ProgressContext *prog_ctx,
                                      Context *on_finish)
  : RefCountedObject(dst_image_ctx->cct, 1), m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_snap_id_start(snap_id_start),
    m_snap_id_end(snap_id_end), m_object_number(object_number),
    m_snap_seqs(snap_seqs), m_prog_ctx(prog_ctx), m_on_finish(on_finish),
    m_cct(dst_image_ctx->cct),
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

  send_object_copies();
}

template <typename I>
void ImageCopyRequest<I>::cancel() {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 20) << dendl;
  m_canceled = true;
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
    complete = (m_current_ops == 0);
  }

  if (complete) {
    finish(m_ret_val);
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
      m_src_image_ctx, m_dst_image_ctx, m_snap_map, ono, ctx);
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
      while (m_copied_objects.top() ==
             (m_object_number ? *m_object_number + 1 : 0)) {
        m_object_number = m_copied_objects.top();
        m_copied_objects.pop();
        m_prog_ctx->update_progress(*m_object_number + 1, m_end_object_no);
      }
    }

    send_next_object_copy();
    complete = (m_current_ops == 0);
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
