// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCopyRequest.h"
#include "librados/snap_set_diff.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_sync::ObjectCopyRequest: " \
                           << this << " " << __func__

namespace rbd {
namespace mirror {
namespace image_sync {

using librbd::util::create_context_callback;
using librbd::util::create_rados_ack_callback;
using librbd::util::create_rados_safe_callback;

template <typename I>
ObjectCopyRequest<I>::ObjectCopyRequest(I *local_image_ctx, I *remote_image_ctx,
                                        const SnapMap *snap_map,
                                        uint64_t object_number,
                                        Context *on_finish)
  : m_local_image_ctx(local_image_ctx), m_remote_image_ctx(remote_image_ctx),
    m_snap_map(snap_map), m_object_number(object_number),
    m_on_finish(on_finish) {
  assert(!snap_map->empty());

  m_local_io_ctx.dup(m_local_image_ctx->data_ctx);
  m_local_oid = m_local_image_ctx->get_object_name(object_number);

  m_remote_io_ctx.dup(m_remote_image_ctx->data_ctx);
  m_remote_oid = m_remote_image_ctx->get_object_name(object_number);

  dout(20) << ": "
           << "remote_oid=" << m_remote_oid << ", "
           << "local_oid=" << m_local_oid << dendl;
}

template <typename I>
void ObjectCopyRequest<I>::send() {
  send_list_snaps();
}

template <typename I>
void ObjectCopyRequest<I>::send_list_snaps() {
  dout(20) << dendl;

  librados::AioCompletion *rados_completion = create_rados_ack_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_list_snaps>(this);

  librados::ObjectReadOperation op;
  op.list_snaps(&m_snap_set, &m_snap_ret);

  m_remote_io_ctx.snap_set_read(CEPH_SNAPDIR);
  int r = m_remote_io_ctx.aio_operate(m_remote_oid, rados_completion, &op,
                                      nullptr);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_list_snaps(int r) {
  if (r == 0 && m_snap_ret < 0) {
    r = m_snap_ret;
  }

  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {
    finish(0);
    return;
  }
  if (r < 0) {
    derr << ": failed to list snaps: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  compute_diffs();
  send_read_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_read_object() {
  if (m_snap_sync_ops.empty()) {
    // no more snapshot diffs to read from remote
    finish(0);
    return;
  }

  // build the read request
  auto &sync_ops = m_snap_sync_ops.begin()->second;
  assert(!sync_ops.empty());

  // map the sync op start snap id back to the necessary read snap id
  librados::snap_t remote_snap_seq = m_snap_sync_ops.begin()->first;
  m_remote_io_ctx.snap_set_read(remote_snap_seq);

  bool read_required = false;
  librados::ObjectReadOperation op;
  for (auto &sync_op : sync_ops) {
    switch (std::get<0>(sync_op)) {
    case SYNC_OP_TYPE_WRITE:
      if (!read_required) {
        dout(20) << ": remote_snap_seq=" << remote_snap_seq << dendl;
        read_required = true;
      }

      dout(20) << ": read op: " << std::get<1>(sync_op) << "~"
               << std::get<2>(sync_op) << dendl;
      op.read(std::get<1>(sync_op), std::get<2>(sync_op),
              &std::get<3>(sync_op), nullptr);
      break;
    default:
      break;
    }
  }

  if (!read_required) {
    // nothing written to this object for this snapshot (must be trunc/remove)
    send_write_object();
    return;
  }

  librados::AioCompletion *comp = create_rados_safe_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_read_object>(this);
  int r = m_remote_io_ctx.aio_operate(m_remote_oid, comp, &op, nullptr);
  assert(r == 0);
  comp->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_read_object(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to read from remote object: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  send_write_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_write_object() {
  // retrieve the local snap context for the op
  SnapIds local_snap_ids;
  librados::snap_t local_snap_seq = 0;
  librados::snap_t remote_snap_seq = m_snap_sync_ops.begin()->first;
  if (remote_snap_seq != 0) {
    auto snap_map_it = m_snap_map->find(remote_snap_seq);
    assert(snap_map_it != m_snap_map->end());

    // write snapshot context should be before actual snapshot
    if (snap_map_it != m_snap_map->begin()) {
      --snap_map_it;
      assert(!snap_map_it->second.empty());
      local_snap_seq = snap_map_it->second.front();
      local_snap_ids = snap_map_it->second;
    }
  }

  dout(20) << ": "
           << "local_snap_seq=" << local_snap_seq << ", "
           << "local_snaps=" << local_snap_ids << dendl;

  auto &sync_ops = m_snap_sync_ops.begin()->second;
  assert(!sync_ops.empty());

  librados::ObjectWriteOperation op;
  for (auto &sync_op : sync_ops) {
    switch (std::get<0>(sync_op)) {
    case SYNC_OP_TYPE_WRITE:
      dout(20) << ": write op: " << std::get<1>(sync_op) << "~"
               << std::get<3>(sync_op).length() << dendl;
      op.write(std::get<1>(sync_op), std::get<3>(sync_op));
      break;
    case SYNC_OP_TYPE_TRUNC:
      dout(20) << ": trunc op: " << std::get<1>(sync_op) << dendl;
      op.truncate(std::get<1>(sync_op));
      break;
    case SYNC_OP_TYPE_REMOVE:
      dout(20) << ": remove op" << dendl;
      op.remove();
      break;
    default:
      assert(false);
    }
  }

  librados::AioCompletion *comp = create_rados_safe_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_write_object>(this);
  int r = m_local_io_ctx.aio_operate(m_local_oid, comp, &op, local_snap_seq,
                                     local_snap_ids);
  assert(r == 0);
  comp->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_write_object(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -ENOENT) {
    r = 0;
  }
  if (r < 0) {
    derr << ": failed to write to local object: " << cpp_strerror(r)
         << dendl;
    finish(r);
    return;
  }

  m_snap_sync_ops.erase(m_snap_sync_ops.begin());
  if (!m_snap_sync_ops.empty()) {
    send_read_object();
    return;
  }

  send_update_object_map();
}

template <typename I>
void ObjectCopyRequest<I>::send_update_object_map() {
  m_local_image_ctx->snap_lock.get_read();
  if (!m_local_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP,
                                        m_local_image_ctx->snap_lock) ||
      m_snap_object_states.empty()) {
    m_local_image_ctx->snap_lock.put_read();
    finish(0);
    return;
  } else if (m_local_image_ctx->object_map == nullptr) {
    // possible that exclusive lock was lost in background
    derr << ": object map is not initialized" << dendl;

    m_local_image_ctx->snap_lock.put_read();
    finish(-EINVAL);
    return;
  }

  assert(m_local_image_ctx->object_map != nullptr);

  auto snap_object_state = *m_snap_object_states.begin();
  m_snap_object_states.erase(m_snap_object_states.begin());

  dout(20) << ": "
           << "local_snap_id=" << snap_object_state.first << ", "
           << "object_state=" << static_cast<uint32_t>(snap_object_state.second)
           << dendl;

  RWLock::WLocker object_map_locker(m_local_image_ctx->object_map_lock);
  Context *ctx = create_context_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_update_object_map>(
      this);
  m_local_image_ctx->object_map->aio_update(snap_object_state.first,
                                            m_object_number,
                                            m_object_number + 1,
                                            snap_object_state.second,
                                            boost::none, ctx);
  m_local_image_ctx->snap_lock.put_read();
}

template <typename I>
void ObjectCopyRequest<I>::handle_update_object_map(int r) {
  dout(20) << ": r=" << r << dendl;

  assert(r == 0);
  if (!m_snap_object_states.empty()) {
    send_update_object_map();
    return;
  }
  finish(0);
}

template <typename I>
void ObjectCopyRequest<I>::compute_diffs() {
  CephContext *cct = m_local_image_ctx->cct;

  uint64_t prev_end_size = 0;
  bool prev_exists = false;
  librados::snap_t start_remote_snap_id = 0;
  for (auto &pair : *m_snap_map) {
    assert(!pair.second.empty());
    librados::snap_t end_remote_snap_id = pair.first;
    librados::snap_t end_local_snap_id = pair.second.front();

    interval_set<uint64_t> diff;
    uint64_t end_size;
    bool exists;
    calc_snap_set_diff(cct, m_snap_set, start_remote_snap_id,
                       end_remote_snap_id, &diff, &end_size, &exists);

    dout(20) << ": "
             << "start_remote_snap=" << start_remote_snap_id << ", "
             << "end_remote_snap_id=" << end_remote_snap_id << ", "
             << "end_local_snap_id=" << end_local_snap_id << ", "
             << "diff=" << diff << ", "
             << "end_size=" << end_size << ", "
             << "exists=" << exists << dendl;

    if (exists) {
      // clip diff to size of object (in case it was truncated)
      if (end_size < prev_end_size) {
        interval_set<uint64_t> trunc;
        trunc.insert(end_size, prev_end_size);
        trunc.intersection_of(diff);
        diff.subtract(trunc);
        dout(20) << ": clearing truncate diff: " << trunc << dendl;
      }

      // prepare the object map state
      {
        RWLock::RLocker snap_locker(m_local_image_ctx->snap_lock);
        uint8_t object_state = OBJECT_EXISTS;
        if (m_local_image_ctx->test_features(RBD_FEATURE_FAST_DIFF,
                                             m_local_image_ctx->snap_lock) &&
            diff.empty() && end_size == prev_end_size) {
          object_state = OBJECT_EXISTS_CLEAN;
        }
        m_snap_object_states[end_local_snap_id] = object_state;
      }

      // object write/zero, or truncate
      for (auto it = diff.begin(); it != diff.end(); ++it) {
        dout(20) << ": read/write op: " << it.get_start() << "~"
                 << it.get_len() << dendl;
        m_snap_sync_ops[end_remote_snap_id].emplace_back(SYNC_OP_TYPE_WRITE,
                                                         it.get_start(),
                                                         it.get_len(),
                                                         bufferlist());
      }
      if (end_size < prev_end_size) {
        dout(20) << ": trunc op: " << end_size << dendl;
        m_snap_sync_ops[end_remote_snap_id].emplace_back(SYNC_OP_TYPE_TRUNC,
                                                         end_size, 0U,
                                                         bufferlist());
      }
    } else {
      if (prev_exists) {
        // object remove
        dout(20) << ": remove op" << dendl;
        m_snap_sync_ops[end_remote_snap_id].emplace_back(SYNC_OP_TYPE_REMOVE,
                                                         0U, 0U, bufferlist());
      }
    }

    prev_end_size = end_size;
    prev_exists = exists;
    start_remote_snap_id = end_remote_snap_id;
  }
}

template <typename I>
void ObjectCopyRequest<I>::finish(int r) {
  dout(20) << ": r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image_sync
} // namespace mirror
} // namespace rbd

template class rbd::mirror::image_sync::ObjectCopyRequest<librbd::ImageCtx>;
