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
DiffRequest<I>::DiffRequest(I* image_ctx,
                            uint64_t snap_id_start, uint64_t snap_id_end,
                            uint64_t start_object_no, uint64_t end_object_no,
                            BitVector<2>* object_diff_state,
                            Context* on_finish)
    : m_image_ctx(image_ctx), m_snap_id_start(snap_id_start),
      m_snap_id_end(snap_id_end), m_start_object_no(start_object_no),
      m_end_object_no(end_object_no), m_object_diff_state(object_diff_state),
      m_on_finish(on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "snap_id_start=" << m_snap_id_start
                 << ", snap_id_end=" << m_snap_id_end
                 << ", start_object_no=" << m_start_object_no
                 << ", end_object_no=" << m_end_object_no
                 << dendl;
}

template <typename I>
bool DiffRequest<I>::is_diff_iterate() const {
  return m_start_object_no != 0 || m_end_object_no != UINT64_MAX;
}

template <typename I>
void DiffRequest<I>::send() {
  auto cct = m_image_ctx->cct;

  if (m_snap_id_start == CEPH_NOSNAP || m_snap_id_start > m_snap_id_end) {
    lderr(cct) << "invalid start/end snap ids: "
               << "snap_id_start=" << m_snap_id_start << ", "
               << "snap_id_end=" << m_snap_id_end << dendl;
    finish(-EINVAL);
    return;
  }
  if (m_start_object_no == UINT64_MAX || m_start_object_no > m_end_object_no ||
      (m_start_object_no != 0 && m_end_object_no == UINT64_MAX)) {
    lderr(cct) << "invalid start/end object numbers: "
               << "start_object_no=" << m_start_object_no << ", "
               << "end_object_no=" << m_end_object_no << dendl;
    finish(-EINVAL);
    return;
  }

  m_object_diff_state->clear();

  if (m_snap_id_start == m_snap_id_end) {
    // no delta between the same snapshot
    finish(0);
    return;
  }
  if (m_start_object_no == m_end_object_no) {
    // no objects in the provided range (half-open)
    finish(0);
    return;
  }

  // collect all the snap ids in the provided range (inclusive) unless
  // this is diff-iterate against the beginning of time, in which case
  // only the end version matters
  std::shared_lock image_locker{m_image_ctx->image_lock};
  if (!is_diff_iterate() || m_snap_id_start != 0) {
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
  }

  uint64_t start_object_no, end_object_no;
  uint64_t prev_object_diff_state_size = m_object_diff_state->size();
  if (is_diff_iterate()) {
    start_object_no = std::min(m_start_object_no, num_objs);
    end_object_no = std::min(m_end_object_no, num_objs);
    uint64_t num_objs_in_range = end_object_no - start_object_no;
    if (m_object_diff_state->size() != num_objs_in_range) {
      m_object_diff_state->resize(num_objs_in_range);
    }
  } else {
    // for deep-copy, the object diff state should be the largest of
    // all versions in the set, so it's only ever grown
    // shrink is handled by flagging trimmed objects as non-existent
    // and comparing against the previous object diff state as usual
    if (m_object_diff_state->size() < num_objs) {
      m_object_diff_state->resize(num_objs);
    }
    start_object_no = 0;
    end_object_no = m_object_diff_state->size();
  }

  uint64_t overlap = std::min(m_object_diff_state->size(),
                              prev_object_diff_state_size);
  auto it = m_object_map.begin() + start_object_no;
  auto diff_it = m_object_diff_state->begin();
  uint64_t ono = start_object_no;
  for (; ono < start_object_no + overlap; ++diff_it, ++ono) {
    uint8_t object_map_state = (ono < num_objs ? *it++ : OBJECT_NONEXISTENT);
    uint8_t prev_object_diff_state = *diff_it;
    switch (prev_object_diff_state) {
    case DIFF_STATE_HOLE:
      if (object_map_state != OBJECT_NONEXISTENT) {
        // stay in HOLE on intermediate snapshots for diff-iterate
        if (!is_diff_iterate() || m_current_snap_id == m_snap_id_end) {
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

    ldout(cct, 20) << "object state: " << ono << " "
                   << static_cast<uint32_t>(prev_object_diff_state)
                   << "->" << static_cast<uint32_t>(*diff_it) << " ("
                   << static_cast<uint32_t>(object_map_state) << ")"
                   << dendl;
  }
  ldout(cct, 20) << "computed overlap diffs" << dendl;

  ceph_assert(diff_it == m_object_diff_state->end() ||
              end_object_no <= num_objs);
  for (; ono < end_object_no; ++it, ++diff_it, ++ono) {
    uint8_t object_map_state = *it;
    if (object_map_state == OBJECT_NONEXISTENT) {
      *diff_it = DIFF_STATE_HOLE;
    } else if (m_current_snap_id != m_snap_id_start) {
      // diffing against the beginning of time or image was grown
      // (implicit) starting state is HOLE, this is the first object
      // map after
      if (is_diff_iterate()) {
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

    ldout(cct, 20) << "object state: " << ono << " "
                   << "->" << static_cast<uint32_t>(*diff_it) << " ("
                   << static_cast<uint32_t>(*it) << ")" << dendl;
  }
  ldout(cct, 20) << "computed resize diffs" << dendl;

  ceph_assert(diff_it == m_object_diff_state->end());
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
