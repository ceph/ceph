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
  auto cct = m_image_ctx->cct;

  if (m_snap_id_start == CEPH_NOSNAP || m_snap_id_start > m_snap_id_end) {
    lderr(cct) << "invalid start/end snap ids: "
               << "snap_id_start=" << m_snap_id_start << ", "
               << "snap_id_end=" << m_snap_id_end << dendl;
    finish(-EINVAL);
    return;
  } else if (m_snap_id_start == m_snap_id_end) {
    // no delta between the same snapshot
    finish(0);
    return;
  }

  m_object_diff_state->clear();

  // collect all the snap ids in the provided range (inclusive) unless
  // this is diff-iterate against the beginning of time, in which case
  // only the end version matters
  std::shared_lock image_locker{m_image_ctx->image_lock};
  if (!m_diff_iterate_range || m_snap_id_start != 0) {
    if (m_snap_id_start != 0) {
      m_snap_ids.insert(m_snap_id_start);
    }
    auto snap_info_it = m_image_ctx->snap_info.upper_bound(m_snap_id_start);
    auto snap_info_it_end = m_image_ctx->snap_info.lower_bound(m_snap_id_end);
    for (; snap_info_it != snap_info_it_end; ++snap_info_it) {
      m_snap_ids.insert(snap_info_it->first);
    }
  }
  m_snap_ids.insert(m_snap_id_end);

  load_object_map(&image_locker);
}

template <typename I>
void DiffRequest<I>::load_object_map(
    std::shared_lock<ceph::shared_mutex>* image_locker) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx->image_lock));

  if (m_snap_ids.empty()) {
    image_locker->unlock();

    finish(0);
    return;
  }

  m_current_snap_id = *m_snap_ids.begin();
  m_snap_ids.erase(m_current_snap_id);

  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "snap_id=" << m_current_snap_id << dendl;

  if ((m_image_ctx->features & RBD_FEATURE_FAST_DIFF) == 0) {
    image_locker->unlock();

    ldout(cct, 10) << "fast-diff feature not enabled" << dendl;
    finish(-EINVAL);
    return;
  }

  // ignore ENOENT with intermediate snapshots since deleted
  // snaps will get merged with later snapshots
  m_ignore_enoent = (m_current_snap_id != m_snap_id_start &&
                     m_current_snap_id != m_snap_id_end);

  if (m_current_snap_id == CEPH_NOSNAP) {
    m_current_size = m_image_ctx->size;
  } else {
    auto snap_it = m_image_ctx->snap_info.find(m_current_snap_id);
    if (snap_it == m_image_ctx->snap_info.end()) {
      ldout(cct, 10) << "snapshot " << m_current_snap_id << " does not exist"
                     << dendl;
      if (!m_ignore_enoent) {
        image_locker->unlock();

        finish(-ENOENT);
        return;
      }

      load_object_map(image_locker);
      return;
    }

    m_current_size = snap_it->second.size;
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
  image_locker->unlock();

  if ((flags & RBD_FLAG_FAST_DIFF_INVALID) != 0) {
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
  if (r == -ENOENT && m_ignore_enoent) {
    ldout(cct, 10) << "object map " << oid << " does not exist" << dendl;

    std::shared_lock image_locker{m_image_ctx->image_lock};
    load_object_map(&image_locker);
    return;
  } else if (r < 0) {
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

  uint64_t prev_object_diff_state_size = m_object_diff_state->size();
  if (m_diff_iterate_range) {
    if (m_object_diff_state->size() != m_object_map.size()) {
      m_object_diff_state->resize(m_object_map.size());
    }
  } else {
    // for deep-copy, the object diff state should be the largest of
    // all versions in the set, so it's only ever grown
    if (m_object_diff_state->size() < m_object_map.size()) {
      m_object_diff_state->resize(m_object_map.size());
    } else if (m_object_diff_state->size() > m_object_map.size()) {
      // the image was shrunk so expanding the object map will flag end objects
      // as non-existent and they will be compared against the previous object
      // diff state
      m_object_map.resize(m_object_diff_state->size());
    }
  }

  uint64_t overlap = std::min(m_object_map.size(), prev_object_diff_state_size);
  auto it = m_object_map.begin();
  auto overlap_end_it = it + overlap;
  auto diff_it = m_object_diff_state->begin();
  uint64_t i = 0;
  for (; it != overlap_end_it; ++it, ++diff_it, ++i) {
    uint8_t object_map_state = *it;
    uint8_t prev_object_diff_state = *diff_it;
    switch (prev_object_diff_state) {
    case DIFF_STATE_HOLE:
      if (object_map_state != OBJECT_NONEXISTENT) {
        // stay in HOLE on intermediate snapshots for diff-iterate
        if (!m_diff_iterate_range || m_current_snap_id == m_snap_id_end) {
          *diff_it = DIFF_STATE_DATA_UPDATED;
        }
      }
      break;
    case DIFF_STATE_DATA:
      if (object_map_state == OBJECT_NONEXISTENT) {
        *diff_it = DIFF_STATE_HOLE_UPDATED;
      } else if (object_map_state != OBJECT_EXISTS_CLEAN) {
        *diff_it = DIFF_STATE_DATA_UPDATED;
      }
      break;
    case DIFF_STATE_HOLE_UPDATED:
      if (object_map_state != OBJECT_NONEXISTENT) {
        *diff_it = DIFF_STATE_DATA_UPDATED;
      }
      break;
    case DIFF_STATE_DATA_UPDATED:
      if (object_map_state == OBJECT_NONEXISTENT) {
        *diff_it = DIFF_STATE_HOLE_UPDATED;
      }
      break;
    default:
      ceph_abort();
    }

    ldout(cct, 20) << "object state: " << i << " "
                   << static_cast<uint32_t>(prev_object_diff_state)
                   << "->" << static_cast<uint32_t>(*diff_it) << " ("
                   << static_cast<uint32_t>(object_map_state) << ")"
                   << dendl;
  }
  ldout(cct, 20) << "computed overlap diffs" << dendl;

  auto end_it = m_object_map.end();
  for (; it != end_it; ++it, ++diff_it, ++i) {
    uint8_t object_map_state = *it;
    if (object_map_state == OBJECT_NONEXISTENT) {
      *diff_it = DIFF_STATE_HOLE;
    } else if (m_current_snap_id != m_snap_id_start) {
      // diffing against the beginning of time or image was grown
      // (implicit) starting state is HOLE, this is the first object
      // map after
      if (m_diff_iterate_range) {
        // for diff-iterate, if the object is discarded prior to or
        // in the end version, result should be HOLE
        // since DATA_UPDATED can transition only to HOLE_UPDATED,
        // stay in HOLE on intermediate snapshots -- another way to
        // put this is that when starting with a hole, intermediate
        // snapshots can be ignored as the result depends only on the
        // end version
        if (m_current_snap_id == m_snap_id_end) {
          *diff_it = DIFF_STATE_DATA_UPDATED;
        } else {
          *diff_it = DIFF_STATE_HOLE;
        }
      } else {
        // for deep-copy, if the object is discarded prior to or
        // in the end version, result should be HOLE_UPDATED
        *diff_it = DIFF_STATE_DATA_UPDATED;
      }
    } else {
      // diffing against a snapshot, this is its object map
      if (object_map_state != OBJECT_PENDING) {
        *diff_it = DIFF_STATE_DATA;
      } else {
        *diff_it = DIFF_STATE_DATA_UPDATED;
      }
    }

    ldout(cct, 20) << "object state: " << i << " "
                   << "->" << static_cast<uint32_t>(*diff_it) << " ("
                   << static_cast<uint32_t>(*it) << ")" << dendl;
  }
  ldout(cct, 20) << "computed resize diffs" << dendl;

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
