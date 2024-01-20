// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageCopyRequest.h"
#include "ObjectCopyRequest.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
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

using librbd::util::create_async_context_callback;
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

    ldout(m_cct, 20) << "src_image_id=" << m_src_image_ctx->id
		     << ", dst_image_id=" << m_dst_image_ctx->id
	             << ", src_snap_id_start=" << m_src_snap_id_start
                     << ", src_snap_id_end=" << m_src_snap_id_end
		     << ", dst_snap_id_start=" << m_dst_snap_id_start
		     << dendl;
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
void ImageCopyRequest<I>::map_src_objects(uint64_t dst_object,
                                          std::set<uint64_t> *src_objects) {
  std::vector<std::pair<uint64_t, uint64_t>> image_extents;
  Striper::extent_to_file(m_cct, &m_dst_image_ctx->layout, dst_object, 0,
                          m_dst_image_ctx->layout.object_size, image_extents);

  for (auto &e : image_extents) {
    std::map<object_t, std::vector<ObjectExtent>> src_object_extents;
    Striper::file_to_extents(m_cct, m_src_image_ctx->format_string,
                             &m_src_image_ctx->layout, e.first, e.second, 0,
                             src_object_extents);
    for (auto &p : src_object_extents) {
      for (auto &s : p.second) {
        src_objects->insert(s.objectno);
      }
    }
  }

  ceph_assert(!src_objects->empty());

  ldout(m_cct, 20) << dst_object << " -> " << *src_objects << dendl;
}

template <typename I>
void ImageCopyRequest<I>::compute_diff() {
  if (m_flatten) {
    send_object_copies();
    return;
  }

  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
    ImageCopyRequest<I>, &ImageCopyRequest<I>::handle_compute_diff>(this);
  auto req = object_map::DiffRequest<I>::create(m_src_image_ctx,
                                                m_src_snap_id_start,
                                                m_src_snap_id_end, 0, UINT64_MAX,
                                                &m_object_diff_state, ctx);
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
    for (uint64_t i = 0; i < max_ops; i++) {
      send_next_object_copy();
    }

    complete = (m_current_ops == 0) && !m_updating_progress;
  }

  if (complete) {
    finish(m_ret_val);
  }
}

template <typename I>
void ImageCopyRequest<I>::send_next_object_copy() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  if (m_canceled && m_ret_val == 0) {
    ldout(m_cct, 10) << "image copy canceled" << dendl;
    m_ret_val = -ECANCELED;
  }

  if (m_ret_val < 0 || m_object_no >= m_end_object_no) {
    return;
  }

  uint64_t ono = m_object_no++;
  Context *ctx = new LambdaContext(
    [this, ono](int r) {
      handle_object_copy(ono, r);
    });

  ldout(m_cct, 20) << "object_num=" << ono << dendl;
  ++m_current_ops;

  uint8_t object_diff_state = object_map::DIFF_STATE_HOLE;
  if (m_object_diff_state.size() > 0) {
    std::set<uint64_t> src_objects;
    map_src_objects(ono, &src_objects);

    for (auto src_ono : src_objects) {
      if (src_ono >= m_object_diff_state.size()) {
        object_diff_state = object_map::DIFF_STATE_DATA_UPDATED;
      } else {
        auto state = m_object_diff_state[src_ono];
        if ((state == object_map::DIFF_STATE_HOLE_UPDATED &&
             object_diff_state != object_map::DIFF_STATE_DATA_UPDATED) ||
            (state == object_map::DIFF_STATE_DATA &&
             object_diff_state == object_map::DIFF_STATE_HOLE) ||
            (state == object_map::DIFF_STATE_DATA_UPDATED)) {
          object_diff_state = state;
        }
      }
    }

    if (object_diff_state == object_map::DIFF_STATE_HOLE) {
      ldout(m_cct, 20) << "skipping non-existent object " << ono << dendl;
      create_async_context_callback(*m_src_image_ctx, ctx)->complete(0);
      return;
    }
  }

  uint32_t flags = 0;
  if (m_flatten) {
    flags |= OBJECT_COPY_REQUEST_FLAG_FLATTEN;
  }
  if (object_diff_state == object_map::DIFF_STATE_DATA) {
    // no source objects have been updated and at least one has clean data
    flags |= OBJECT_COPY_REQUEST_FLAG_EXISTS_CLEAN;
  }

  auto req = ObjectCopyRequest<I>::create(
    m_src_image_ctx, m_dst_image_ctx, m_src_snap_id_start, m_dst_snap_id_start,
    m_snap_map, ono, flags, m_handler, ctx);
  req->send();
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

    send_next_object_copy();
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
