// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/DiffRequest.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "osdc/Striper.h"
#include <string>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::DiffRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace object_map {

using util::create_rados_callback;

template <typename I>
void DiffRequest<I>::send() {
  std::shared_lock image_locker{m_image_ctx->image_lock};

  m_diff_from_start = (m_snap_id_start == 0);
  if (m_snap_id_start == 0) {
    if (!m_image_ctx->snap_info.empty()) {
      m_snap_id_start = m_image_ctx->snap_info.begin()->first;
    } else {
      m_snap_id_start = CEPH_NOSNAP;
    }
  }

  m_object_diff_state->clear();
  m_current_snap_id = m_snap_id_start;
  m_next_snap_id = m_snap_id_end;

  load_object_map(&image_locker);
}

template <typename I>
void DiffRequest<I>::load_object_map(
    std::shared_lock<ceph::shared_mutex>* image_locker) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->image_lock));

  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  m_current_size = m_image_ctx->size;
  if (m_current_snap_id != CEPH_NOSNAP) {
    auto snap_it = m_image_ctx->snap_info.find(m_current_snap_id);
    ceph_assert(snap_it != m_image_ctx->snap_info.end());
    m_current_size = snap_it->second.size;

    ++snap_it;
    if (snap_it != m_image_ctx->snap_info.end()) {
      m_next_snap_id = snap_it->first;
    } else {
      m_next_snap_id = CEPH_NOSNAP;
    }
  }

  if ((m_image_ctx->features & RBD_FEATURE_FAST_DIFF) != 0) {
    image_locker->unlock();

    ldout(cct, 10) << "fast-diff feature not enabled" << dendl;
    finish(-EINVAL);
    return;
  }

  uint64_t flags = 0;
  int r = m_image_ctx->get_flags(m_current_snap_id, &flags);
  if (r < 0) {
    image_locker->unlock();

    lderr(cct) << "failed to retrieve image flags: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }
  if ((flags & RBD_FLAG_FAST_DIFF_INVALID) != 0) {
    image_locker->unlock();

    ldout(cct, 1) << "cannot perform fast diff on invalid object map"
                  << dendl;
    finish(-EINVAL);
    return;
  }

  std::string oid(ObjectMap<>::object_map_name(m_image_ctx->id,
                                               m_current_snap_id));

  librados::ObjectReadOperation op;
  cls_client::object_map_load_start(&op);

  m_out_bl.clear();
  auto aio_comp = create_rados_callback<
    DiffRequest<I>, &DiffRequest<I>::handle_load_object_map>(this);
  r = m_image_ctx->md_ctx.aio_operate(oid, aio_comp, &op, &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void DiffRequest<I>::handle_load_object_map(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto bl_it = m_out_bl.cbegin();
    r = cls_client::object_map_load_finish(&bl_it, &m_object_map);
  }

  std::string oid(ObjectMap<>::object_map_name(m_image_ctx->id,
                                               m_current_snap_id));
  if (r < 0) {
    lderr(cct) << "failed to load object map: " << oid << dendl;
    finish(r);
    return;
  }
  ldout(cct, 20) << "loaded object map " << oid << dendl;

  uint64_t num_objs = Striper::get_num_objects(m_image_ctx->layout,
                                               m_current_size);
  if (m_object_map.size() < num_objs) {
    ldout(cct, 1) << "object map too small: "
                  << m_object_map.size() << " < " << num_objs << dendl;
    finish(-EINVAL);
    return;
  } else {
    m_object_map.resize(num_objs);
  }

  if (m_object_diff_state->size() < num_objs) {
    // the diff state should be the largest of all snapshots in the set
    m_object_diff_state->resize(num_objs);
  }
  if (m_object_map.size() < m_object_diff_state->size()) {
    // the image was shrunk so expanding the object map will flag end objects
    // as non-existent and they will be compared against the previous object
    // map
    m_object_map.resize(m_object_diff_state->size());
  }

  uint64_t overlap = std::min(m_object_map.size(), m_prev_object_map.size());
  auto it = m_object_map.begin();
  auto overlap_end_it = it + overlap;
  auto pre_it = m_prev_object_map.begin();
  auto diff_it = m_object_diff_state->begin();
  uint64_t i = 0;
  for (; it != overlap_end_it; ++it, ++pre_it, ++diff_it, ++i) {
    ldout(cct, 20) << "object state: " << i << " "
                   << static_cast<uint32_t>(*pre_it)
                   << "->" << static_cast<uint32_t>(*it) << dendl;
    if (*it == OBJECT_NONEXISTENT) {
      if (*pre_it != OBJECT_NONEXISTENT) {
        *diff_it = DIFF_STATE_HOLE;
      }
    } else if (*it == OBJECT_EXISTS ||
               (*pre_it != *it &&
                !(*pre_it == OBJECT_EXISTS &&
                  *it == OBJECT_EXISTS_CLEAN))) {
      *diff_it = DIFF_STATE_UPDATED;
    }
  }
  ldout(cct, 20) << "computed overlap diffs" << dendl;

  auto end_it = m_object_map.end();
  if (m_object_map.size() > m_prev_object_map.size() &&
      (m_diff_from_start || m_prev_object_map_valid)) {
    for (; it != end_it; ++it,++diff_it, ++i) {
      ldout(cct, 20) << "object state: " << i << " "
                     << "->" << static_cast<uint32_t>(*it) << dendl;
      if (*it == OBJECT_NONEXISTENT) {
        *diff_it = DIFF_STATE_NONE;
      } else {
        *diff_it = DIFF_STATE_UPDATED;
      }
    }
  }
  ldout(cct, 20) << "computed resize diffs" << dendl;

  if (m_current_snap_id == m_next_snap_id || m_next_snap_id > m_snap_id_end) {
    finish(0);
    return;
  }

  m_current_snap_id = m_next_snap_id;
  m_prev_object_map = m_object_map;
  m_prev_object_map_valid = true;

  std::shared_lock image_locker{m_image_ctx->image_lock};
  load_object_map(&image_locker);
}

template <typename I>
void DiffRequest<I>::finish(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace object_map
} // namespace librbd

template class librbd::object_map::DiffRequest<librbd::ImageCtx>;
