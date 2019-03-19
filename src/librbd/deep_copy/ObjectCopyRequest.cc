// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCopyRequest.h"
#include "common/errno.h"
#include "librados/snap_set_diff.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ReadResult.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::ObjectCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librados {

inline bool operator==(const clone_info_t& rhs, const clone_info_t& lhs) {
  return (rhs.cloneid == lhs.cloneid &&
          rhs.snaps == lhs.snaps &&
          rhs.overlap == lhs.overlap &&
          rhs.size == lhs.size);
}

inline bool operator==(const snap_set_t& rhs, const snap_set_t& lhs) {
  return (rhs.clones == lhs.clones &&
          rhs.seq == lhs.seq);
}

} // namespace librados

namespace librbd {
namespace deep_copy {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
ObjectCopyRequest<I>::ObjectCopyRequest(I *src_image_ctx,
                                        I *dst_image_ctx,
                                        const SnapMap &snap_map,
                                        uint64_t dst_object_number,
                                        bool flatten, Context *on_finish)
  : m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_cct(dst_image_ctx->cct),
    m_snap_map(snap_map), m_dst_object_number(dst_object_number),
    m_flatten(flatten), m_on_finish(on_finish) {
  ceph_assert(!m_snap_map.empty());

  m_src_io_ctx.dup(m_src_image_ctx->data_ctx);
  m_dst_io_ctx.dup(m_dst_image_ctx->data_ctx);

  m_dst_oid = m_dst_image_ctx->get_object_name(dst_object_number);

  ldout(m_cct, 20) << "dst_oid=" << m_dst_oid << dendl;

  compute_src_object_extents();
  compute_dst_object_may_exist();
}

template <typename I>
void ObjectCopyRequest<I>::send() {
  send_list_snaps();
}

template <typename I>
void ObjectCopyRequest<I>::send_list_snaps() {
  ceph_assert(!m_src_objects.empty());
  m_src_ono = *m_src_objects.begin();
  m_src_oid = m_src_image_ctx->get_object_name(m_src_ono);

  ldout(m_cct, 20) << "src_oid=" << m_src_oid << dendl;

  librados::AioCompletion *rados_completion = create_rados_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_list_snaps>(this);

  librados::ObjectReadOperation op;
  m_snap_set = {};
  m_snap_ret = 0;
  op.list_snaps(&m_snap_set, &m_snap_ret);

  m_src_io_ctx.snap_set_read(CEPH_SNAPDIR);
  int r = m_src_io_ctx.aio_operate(m_src_oid, rados_completion, &op,
                                   nullptr);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_list_snaps(int r) {
  if (r == 0 && m_snap_ret < 0) {
    r = m_snap_ret;
  }

  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to list snaps: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_retry_missing_read) {
    if (m_snap_set == m_retry_snap_set) {
      lderr(m_cct) << "read encountered missing object using up-to-date snap set"
                   << dendl;
      finish(-ENOENT);
      return;
    }

    ldout(m_cct, 20) << "retrying using updated snap set" << dendl;
    m_retry_missing_read = false;
    m_retry_snap_set = {};
  }

  if (r == -ENOENT) {
    for (auto &it : m_src_object_extents) {
      auto &e = it.second;
      if (e.object_no == m_src_ono) {
        e.noent = true;
      }
    }
    m_read_ops = {};
    m_read_snaps = {};
    m_zero_interval = {};
  } else {
    compute_read_ops();
  }
  send_read_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_read_object() {

  if (m_read_snaps.empty()) {
    // all snapshots have been read
    merge_write_ops();

    ceph_assert(!m_src_objects.empty());
    m_src_objects.erase(m_src_objects.begin());

    if (!m_src_objects.empty()) {
      send_list_snaps();
      return;
    }

    // all objects have been read
    send_read_from_parent();
    return;
  }

  auto index = *m_read_snaps.begin();
  auto src_snap_seq = index.second;

  bool read_required = false;
  librados::ObjectReadOperation op;

  for (auto &copy_op : m_read_ops[index]) {
    if (!read_required) {
      // map the copy op start snap id back to the necessary read snap id
      m_src_io_ctx.snap_set_read(src_snap_seq);

      ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << dendl;
      read_required = true;
    }
    ldout(m_cct, 20) << "read op: " << copy_op.src_offset << "~"
                     << copy_op.length << dendl;
    op.sparse_read(copy_op.src_offset, copy_op.length, &copy_op.src_extent_map,
                   &copy_op.out_bl, nullptr);
    op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                     LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
  }

  if (!read_required) {
    // nothing written to this object for this snapshot (must be trunc/remove)
    handle_read_object(0);
    return;
  }

  auto ctx = create_context_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_read_object>(this);
  auto comp = create_rados_callback(ctx);

  ldout(m_cct, 20) << "read " << m_src_oid << dendl;

  int r = m_src_io_ctx.aio_operate(m_src_oid, comp, &op, nullptr);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_read_object(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    m_retry_snap_set = m_snap_set;
    m_retry_missing_read = true;

    ldout(m_cct, 5) << "object missing potentially due to removed snapshot"
                    << dendl;
    send_list_snaps();
    return;
  }

  if (r < 0) {
    lderr(m_cct) << "failed to read from source object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  ceph_assert(!m_read_snaps.empty());
  m_read_snaps.erase(m_read_snaps.begin());

  send_read_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_read_from_parent() {
  m_src_image_ctx->snap_lock.get_read();
  m_src_image_ctx->parent_lock.get_read();
  io::Extents image_extents;
  compute_read_from_parent_ops(&image_extents);
  m_src_image_ctx->snap_lock.put_read();

  if (image_extents.empty()) {
    m_src_image_ctx->parent_lock.put_read();
    handle_read_from_parent(0);
    return;
  }

  ldout(m_cct, 20) << dendl;

  ceph_assert(m_src_image_ctx->parent != nullptr);

  auto ctx = create_context_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_read_from_parent>(this);
  auto comp = io::AioCompletion::create_and_start(
    ctx, util::get_image_ctx(m_src_image_ctx->parent), io::AIO_TYPE_READ);
  ldout(m_cct, 20) << "completion " << comp << ", extents " << image_extents
                   << dendl;

  auto src_image_ctx = m_src_image_ctx;
  io::ImageRequest<I>::aio_read(src_image_ctx->parent, comp,
                                std::move(image_extents),
                                io::ReadResult{&m_read_from_parent_data}, 0,
                                ZTracer::Trace());
  src_image_ctx->parent_lock.put_read();
}

template <typename I>
void ObjectCopyRequest<I>::handle_read_from_parent(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to read from parent: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_read_ops.empty()) {
    ceph_assert(m_read_ops.size() == 1);
    auto src_snap_seq = m_read_ops.begin()->first.first;
    auto &copy_ops = m_read_ops.begin()->second;
    uint64_t offset = 0;
    for (auto it = copy_ops.begin(); it != copy_ops.end(); ) {
      it->out_bl.substr_of(m_read_from_parent_data, offset, it->length);
      offset += it->length;
      if (it->out_bl.is_zero()) {
        m_zero_interval[src_snap_seq].insert(it->dst_offset, it->length);
        it = copy_ops.erase(it);
      } else {
        it++;
      }
    }
    merge_write_ops();
  }

  compute_zero_ops();

  if (m_write_ops.empty()) {
    // nothing to copy
    finish(-ENOENT);
    return;
  }

  send_write_object();
  return;
}

template <typename I>
void ObjectCopyRequest<I>::send_write_object() {
  ceph_assert(!m_write_ops.empty());
  auto& copy_ops = m_write_ops.begin()->second;

  // retrieve the destination snap context for the op
  SnapIds dst_snap_ids;
  librados::snap_t dst_snap_seq = 0;
  librados::snap_t src_snap_seq = m_write_ops.begin()->first;
  if (src_snap_seq != 0) {
    auto snap_map_it = m_snap_map.find(src_snap_seq);
    ceph_assert(snap_map_it != m_snap_map.end());

    auto dst_snap_id = snap_map_it->second.front();
    auto dst_may_exist_it = m_dst_object_may_exist.find(dst_snap_id);
    ceph_assert(dst_may_exist_it != m_dst_object_may_exist.end());
    if (!dst_may_exist_it->second && !copy_ops.empty()) {
      // if the object cannot exist, the only valid op is to remove it
      ceph_assert(copy_ops.size() == 1U);
      ceph_assert(copy_ops.begin()->type == COPY_OP_TYPE_REMOVE);
    }

    // write snapshot context should be before actual snapshot
    if (snap_map_it != m_snap_map.begin()) {
      --snap_map_it;
      ceph_assert(!snap_map_it->second.empty());
      dst_snap_seq = snap_map_it->second.front();
      dst_snap_ids = snap_map_it->second;
    }
  }

  ldout(m_cct, 20) << "dst_snap_seq=" << dst_snap_seq << ", "
                   << "dst_snaps=" << dst_snap_ids << dendl;

  librados::ObjectWriteOperation op;
  uint64_t buffer_offset;

  if (!m_dst_image_ctx->migration_info.empty()) {
    cls_client::assert_snapc_seq(&op, dst_snap_seq,
                                 cls::rbd::ASSERT_SNAPC_SEQ_GT_SNAPSET_SEQ);
  }

  for (auto &copy_op : copy_ops) {
    switch (copy_op.type) {
    case COPY_OP_TYPE_WRITE:
      buffer_offset = 0;
      for (auto &e : copy_op.dst_extent_map) {
        ldout(m_cct, 20) << "write op: " << e.first << "~" << e.second
                         << dendl;
        bufferlist tmpbl;
        tmpbl.substr_of(copy_op.out_bl, buffer_offset, e.second);
        op.write(e.first, tmpbl);
        op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                         LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
        buffer_offset += e.second;
      }
      break;
    case COPY_OP_TYPE_ZERO:
      ldout(m_cct, 20) << "zero op: " << copy_op.dst_offset << "~"
                       << copy_op.length << dendl;
      op.zero(copy_op.dst_offset, copy_op.length);
      break;
    case COPY_OP_TYPE_REMOVE_TRUNC:
      ldout(m_cct, 20) << "create op" << dendl;
      op.create(false);
      // fall through
    case COPY_OP_TYPE_TRUNC:
      ldout(m_cct, 20) << "trunc op: " << copy_op.dst_offset << dendl;
      op.truncate(copy_op.dst_offset);
      break;
    case COPY_OP_TYPE_REMOVE:
      ldout(m_cct, 20) << "remove op" << dendl;
      op.remove();
      break;
    default:
      ceph_abort();
    }
  }

  if (op.size() == (m_dst_image_ctx->migration_info.empty() ? 0 : 1)) {
    handle_write_object(0);
    return;
  }

  int r;
  Context *finish_op_ctx;
  {
    RWLock::RLocker owner_locker(m_dst_image_ctx->owner_lock);
    finish_op_ctx = start_lock_op(m_dst_image_ctx->owner_lock, &r);
  }
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_write_object(r);
      finish_op_ctx->complete(0);
    });
  librados::AioCompletion *comp = create_rados_callback(ctx);
  r = m_dst_io_ctx.aio_operate(m_dst_oid, comp, &op, dst_snap_seq, dst_snap_ids,
                               nullptr);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void ObjectCopyRequest<I>::handle_write_object(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == -ENOENT) {
    r = 0;
  } else if (r == -ERANGE) {
    ldout(m_cct, 10) << "concurrent deep copy" << dendl;
    r = 0;
  }
  if (r < 0) {
    lderr(m_cct) << "failed to write to destination object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  m_write_ops.erase(m_write_ops.begin());
  if (!m_write_ops.empty()) {
    send_write_object();
    return;
  }

  send_update_object_map();
}

template <typename I>
void ObjectCopyRequest<I>::send_update_object_map() {
  if (!m_dst_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP) ||
      m_dst_object_state.empty()) {
    finish(0);
    return;
  }

  m_dst_image_ctx->owner_lock.get_read();
  m_dst_image_ctx->snap_lock.get_read();
  if (m_dst_image_ctx->object_map == nullptr) {
    // possible that exclusive lock was lost in background
    lderr(m_cct) << "object map is not initialized" << dendl;

    m_dst_image_ctx->snap_lock.put_read();
    m_dst_image_ctx->owner_lock.put_read();
    finish(-EINVAL);
    return;
  }

  auto &dst_object_state = *m_dst_object_state.begin();
  auto it = m_snap_map.find(dst_object_state.first);
  ceph_assert(it != m_snap_map.end());
  auto dst_snap_id = it->second.front();
  auto object_state = dst_object_state.second;
  m_dst_object_state.erase(m_dst_object_state.begin());

  ldout(m_cct, 20) << "dst_snap_id=" << dst_snap_id << ", object_state="
                   << static_cast<uint32_t>(object_state) << dendl;

  int r;
  auto finish_op_ctx = start_lock_op(m_dst_image_ctx->owner_lock, &r);
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    m_dst_image_ctx->snap_lock.put_read();
    m_dst_image_ctx->owner_lock.put_read();
    finish(r);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_update_object_map(r);
      finish_op_ctx->complete(0);
    });

  auto dst_image_ctx = m_dst_image_ctx;
  dst_image_ctx->object_map_lock.get_write();
  bool sent = dst_image_ctx->object_map->template aio_update<
    Context, &Context::complete>(dst_snap_id, m_dst_object_number, object_state,
                                 {}, {}, false, ctx);

  // NOTE: state machine might complete before we reach here
  dst_image_ctx->object_map_lock.put_write();
  dst_image_ctx->snap_lock.put_read();
  dst_image_ctx->owner_lock.put_read();
  if (!sent) {
    ceph_assert(dst_snap_id == CEPH_NOSNAP);
    ctx->complete(0);
  }
}

template <typename I>
void ObjectCopyRequest<I>::handle_update_object_map(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update object map: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (!m_dst_object_state.empty()) {
    send_update_object_map();
    return;
  }
  finish(0);
}

template <typename I>
Context *ObjectCopyRequest<I>::start_lock_op(RWLock &owner_lock, int* r) {
  ceph_assert(m_dst_image_ctx->owner_lock.is_locked());
  if (m_dst_image_ctx->exclusive_lock == nullptr) {
    return new FunctionContext([](int r) {});
  }
  return m_dst_image_ctx->exclusive_lock->start_op(r);
}

template <typename I>
uint64_t ObjectCopyRequest<I>::src_to_dst_object_offset(uint64_t objectno,
                                                        uint64_t offset) {
  std::vector<std::pair<uint64_t, uint64_t>> image_extents;
  Striper::extent_to_file(m_cct, &m_src_image_ctx->layout, objectno, offset, 1,
                          image_extents);
  ceph_assert(image_extents.size() == 1);
  auto dst_object_offset = image_extents.begin()->first;

  std::map<object_t, std::vector<ObjectExtent>> dst_object_extents;
  Striper::file_to_extents(m_cct, m_dst_image_ctx->format_string,
                           &m_dst_image_ctx->layout, dst_object_offset, 1, 0,
                           dst_object_extents);
  ceph_assert(dst_object_extents.size() == 1);
  ceph_assert(dst_object_extents.begin()->second.size() == 1);
  auto &e = *dst_object_extents.begin()->second.begin();
  ceph_assert(e.objectno == m_dst_object_number);

  return e.offset;
}

template <typename I>
void ObjectCopyRequest<I>::compute_src_object_extents() {
  std::vector<std::pair<uint64_t, uint64_t>> image_extents;
  Striper::extent_to_file(m_cct, &m_dst_image_ctx->layout, m_dst_object_number,
                          0, m_dst_image_ctx->layout.object_size, image_extents);

  size_t total = 0;
  for (auto &e : image_extents) {
    std::map<object_t, std::vector<ObjectExtent>> src_object_extents;
    Striper::file_to_extents(m_cct, m_src_image_ctx->format_string,
                             &m_src_image_ctx->layout, e.first, e.second, 0,
                             src_object_extents);
    auto stripe_unit = std::min(m_src_image_ctx->layout.stripe_unit,
                                m_dst_image_ctx->layout.stripe_unit);
    for (auto &p : src_object_extents) {
      for (auto &s : p.second) {
        m_src_objects.insert(s.objectno);
        total += s.length;
        while (s.length > 0) {
          ceph_assert(s.length >= stripe_unit);
          auto dst_object_offset = src_to_dst_object_offset(s.objectno, s.offset);
          m_src_object_extents[dst_object_offset] = {s.objectno, s.offset,
                                                     stripe_unit};
          s.offset += stripe_unit;
          s.length -= stripe_unit;
        }
      }
    }
  }

  ceph_assert(total == m_dst_image_ctx->layout.object_size);

  ldout(m_cct, 20) << m_src_object_extents.size() << " src extents" << dendl;
}

template <typename I>
void ObjectCopyRequest<I>::compute_read_ops() {
  m_read_ops = {};
  m_read_snaps = {};
  m_zero_interval = {};

  m_src_image_ctx->parent_lock.get_read();
  bool hide_parent = (m_src_image_ctx->parent != nullptr);
  m_src_image_ctx->parent_lock.put_read();

  librados::snap_t src_copy_point_snap_id = m_snap_map.rbegin()->first;
  bool prev_exists = hide_parent;
  uint64_t prev_end_size = prev_exists ?
      m_src_image_ctx->layout.object_size : 0;
  librados::snap_t start_src_snap_id = 0;

  for (auto &pair : m_snap_map) {
    ceph_assert(!pair.second.empty());
    librados::snap_t end_src_snap_id = pair.first;
    librados::snap_t end_dst_snap_id = pair.second.front();

    interval_set<uint64_t> diff;
    uint64_t end_size;
    bool exists;
    librados::snap_t clone_end_snap_id;
    calc_snap_set_diff(m_cct, m_snap_set, start_src_snap_id,
                       end_src_snap_id, &diff, &end_size, &exists,
                       &clone_end_snap_id, &m_read_whole_object);

    if (m_read_whole_object) {
      ldout(m_cct, 1) << "need to read full object" << dendl;
      diff.insert(0, m_src_image_ctx->layout.object_size);
      exists = true;
      end_size = m_src_image_ctx->layout.object_size;
      clone_end_snap_id = end_src_snap_id;
    } else if (!exists) {
      end_size = 0;
      if (hide_parent && end_src_snap_id == m_snap_map.begin()->first &&
          m_snap_set.clones.empty()) {
        ldout(m_cct, 20) << "no clones for existing object" << dendl;
        exists = true;
        diff.insert(0, m_src_image_ctx->layout.object_size);
        clone_end_snap_id = end_src_snap_id;
      }
    }

    ldout(m_cct, 20) << "start_src_snap_id=" << start_src_snap_id << ", "
                     << "end_src_snap_id=" << end_src_snap_id << ", "
                     << "clone_end_snap_id=" << clone_end_snap_id << ", "
                     << "end_dst_snap_id=" << end_dst_snap_id << ", "
                     << "diff=" << diff << ", "
                     << "end_size=" << end_size << ", "
                     << "exists=" << exists << dendl;

    m_zero_interval[end_src_snap_id] = {};

    if (exists || prev_exists) {
      // clip diff to size of object (in case it was truncated)
      if (end_size < prev_end_size) {
        interval_set<uint64_t> trunc;
        trunc.insert(end_size, prev_end_size - end_size);
        trunc.intersection_of(diff);
        diff.subtract(trunc);
        ldout(m_cct, 20) << "clearing truncate diff: " << trunc << dendl;
      }

      if (exists) {
        // reads should be issued against the newest (existing) snapshot within
        // the associated snapshot object clone. writes should be issued
        // against the oldest snapshot in the snap_map.
        ceph_assert(clone_end_snap_id >= end_src_snap_id);
        if (clone_end_snap_id > src_copy_point_snap_id) {
          // do not read past the copy point snapshot
          clone_end_snap_id = src_copy_point_snap_id;
        }
      }

      for (auto &it : m_src_object_extents) {
        auto dst_object_offset = it.first;
        auto &e = it.second;

        if (e.object_no != m_src_ono) {
          continue;
        }

        interval_set<uint64_t> read_interval;
        read_interval.insert(e.offset, e.length);

        if (end_size < prev_end_size) {
          interval_set<uint64_t> zero_interval;
          zero_interval.insert(end_size, prev_end_size - end_size);
          zero_interval.intersection_of(read_interval);
          if (!zero_interval.empty()) {
            auto it = zero_interval.begin();
            auto offset = it.get_start() - e.offset;
            m_zero_interval[end_src_snap_id].insert(dst_object_offset + offset,
                                                    it.get_len());
            ldout(m_cct, 20) << "extent " << e.offset << "~" << e.length
                             << " intersects truncation " << end_size << "~"
                             << prev_end_size - end_size << ", inserting zero "
                             << dst_object_offset + offset << "~"
                             << it.get_len() << dendl;
          }
        }

        // limit read interval to diff
        read_interval.intersection_of(diff);

        ldout(m_cct, 20) << "src_object_extent: " << e.offset << "~" << e.length
                         << ", dst_object_offset=" << dst_object_offset
                         << ", read: " << read_interval << dendl;

        ceph_assert(exists || read_interval.empty());

        for (auto it = read_interval.begin(); it != read_interval.end();
             it++) {
            ceph_assert(it.get_start() >= e.offset);
            auto offset = it.get_start() - e.offset;
            ldout(m_cct, 20) << "read/write op: " << it.get_start() << "~"
                             << it.get_len() << " dst: "
                             << dst_object_offset + offset << dendl;
            m_read_ops[{end_src_snap_id, clone_end_snap_id}]
              .emplace_back(COPY_OP_TYPE_WRITE, it.get_start(),
                            dst_object_offset + offset, it.get_len());
        }
      }
    }

    prev_end_size = end_size;
    prev_exists = exists;
    if (hide_parent && prev_exists && prev_end_size == 0) {
      // hide parent
      prev_end_size = m_src_image_ctx->layout.object_size;
    }
    start_src_snap_id = end_src_snap_id;
  }

  for (auto &it : m_read_ops) {
    m_read_snaps.push_back(it.first);
  }
}

template <typename I>
void ObjectCopyRequest<I>::compute_read_from_parent_ops(
    io::Extents *parent_image_extents) {
  assert(m_src_image_ctx->snap_lock.is_locked());
  assert(m_src_image_ctx->parent_lock.is_locked());

  m_read_ops = {};
  m_zero_interval = {};
  parent_image_extents->clear();

  if (m_src_image_ctx->parent == nullptr) {
    ldout(m_cct, 20) << "no parent" << dendl;
    return;
  }

  size_t noent_count = 0;
  for (auto &it : m_src_object_extents) {
    if (it.second.noent) {
      noent_count++;
    }
  }

  if (noent_count == 0) {
    ldout(m_cct, 20) << "no extents need read from parent" << dendl;
    return;
  }

  if (noent_count == m_src_object_extents.size() && !m_flatten) {
    ldout(m_cct, 20) << "reading all extents skipped when no flatten"
                     << dendl;
    return;
  }

  ldout(m_cct, 20) << dendl;

  auto src_snap_seq = m_snap_map.begin()->first;

  uint64_t parent_overlap;
  int r = m_src_image_ctx->get_parent_overlap(src_snap_seq, &parent_overlap);
  if (r < 0) {
    ldout(m_cct, 5) << "failed getting parent overlap for snap_id: "
                    << src_snap_seq << ": " << cpp_strerror(r) << dendl;
    return;
  }
  if (parent_overlap == 0) {
    ldout(m_cct, 20) << "no parent overlap" << dendl;
    return;
  }

  for (auto &it : m_src_object_extents) {
    auto dst_object_offset = it.first;
    auto &e = it.second;

    if (!e.noent) {
      continue;
    }

    std::vector<std::pair<uint64_t, uint64_t>> image_extents;
    Striper::extent_to_file(m_cct, &m_src_image_ctx->layout, e.object_no,
                            e.offset, e.length, image_extents);

    uint64_t overlap = m_src_image_ctx->prune_parent_extents(image_extents,
                                                             parent_overlap);
    if (overlap == 0) {
      ldout(m_cct, 20) << "no parent overlap for object_no " << e.object_no
                       << " extent " << e.offset << "~" << e.length << dendl;
      continue;
    }

    ldout(m_cct, 20) << "object_no " << e.object_no << " extent " << e.offset
                     << "~" << e.length << " overlap " << parent_overlap
                     << " parent extents " << image_extents << dendl;

    ceph_assert(image_extents.size() == 1);

    auto src_image_offset = image_extents.begin()->first;
    auto length = image_extents.begin()->second;
    m_read_ops[{src_snap_seq, 0}].emplace_back(COPY_OP_TYPE_WRITE, e.offset,
                                               dst_object_offset, length);
    m_read_ops[{src_snap_seq, 0}].rbegin()->src_extent_map[e.offset] = length;
    parent_image_extents->emplace_back(src_image_offset, length);
  }

  if (!parent_image_extents->empty()) {
    m_dst_object_state[src_snap_seq] = OBJECT_EXISTS;
  }
}

template <typename I>
void ObjectCopyRequest<I>::merge_write_ops() {
  ldout(m_cct, 20) << dendl;

  for (auto &it : m_zero_interval) {
    m_dst_zero_interval[it.first].insert(it.second);
  }

  for (auto &it : m_read_ops) {
    auto src_snap_seq = it.first.first;
    auto &copy_ops = it.second;
    for (auto &copy_op : copy_ops) {
      uint64_t src_offset = copy_op.src_offset;
      uint64_t dst_offset = copy_op.dst_offset;
      for (auto &e : copy_op.src_extent_map) {
        uint64_t zero_len = e.first - src_offset;
        if (zero_len > 0) {
          ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq
                           << ", inserting zero " << dst_offset << "~"
                           << zero_len << dendl;
          m_dst_zero_interval[src_snap_seq].insert(dst_offset, zero_len);
          src_offset += zero_len;
          dst_offset += zero_len;
        }
        copy_op.dst_extent_map[dst_offset] = e.second;
        src_offset += e.second;
        dst_offset += e.second;
      }
      if (dst_offset < copy_op.dst_offset + copy_op.length) {
        uint64_t zero_len = copy_op.dst_offset + copy_op.length - dst_offset;
        ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq
                         << ", inserting zero " << dst_offset << "~"
                         << zero_len << dendl;
        m_dst_zero_interval[src_snap_seq].insert(dst_offset, zero_len);
      } else {
        ceph_assert(dst_offset == copy_op.dst_offset + copy_op.length);
      }
      m_write_ops[src_snap_seq].emplace_back(std::move(copy_op));
    }
  }
}

template <typename I>
void ObjectCopyRequest<I>::compute_zero_ops() {
  ldout(m_cct, 20) << dendl;

  bool fast_diff = m_dst_image_ctx->test_features(RBD_FEATURE_FAST_DIFF);
  uint64_t prev_end_size = 0;

  m_src_image_ctx->parent_lock.get_read();
  bool hide_parent = (m_src_image_ctx->parent != nullptr);
  m_src_image_ctx->parent_lock.put_read();

  for (auto &it : m_dst_zero_interval) {
    auto src_snap_seq = it.first;
    auto &zero_interval = it.second;

    auto snap_map_it = m_snap_map.find(src_snap_seq);
    ceph_assert(snap_map_it != m_snap_map.end());
    auto dst_snap_seq = snap_map_it->second.front();

    auto dst_may_exist_it = m_dst_object_may_exist.find(dst_snap_seq);
    ceph_assert(dst_may_exist_it != m_dst_object_may_exist.end());
    if (!dst_may_exist_it->second && prev_end_size > 0) {
      ldout(m_cct, 5) << "object DNE for snap_id: " << dst_snap_seq << dendl;
      m_write_ops[src_snap_seq].emplace_back(COPY_OP_TYPE_REMOVE, 0, 0, 0);
      prev_end_size = 0;
      continue;
    }

    if (hide_parent) {
      RWLock::RLocker snap_locker(m_dst_image_ctx->snap_lock);
      RWLock::RLocker parent_locker(m_dst_image_ctx->parent_lock);
      uint64_t parent_overlap = 0;
      int r = m_dst_image_ctx->get_parent_overlap(dst_snap_seq, &parent_overlap);
      if (r < 0) {
        ldout(m_cct, 5) << "failed getting parent overlap for snap_id: "
                        << dst_snap_seq << ": " << cpp_strerror(r) << dendl;
      }
      if (parent_overlap == 0) {
        ldout(m_cct, 20) << "no parent overlap" << dendl;
        hide_parent = false;
      } else {
        std::vector<std::pair<uint64_t, uint64_t>> image_extents;
        Striper::extent_to_file(m_cct, &m_dst_image_ctx->layout,
                                m_dst_object_number, 0,
                                m_dst_image_ctx->layout.object_size,
                                image_extents);
        uint64_t overlap = m_dst_image_ctx->prune_parent_extents(image_extents,
                                                                 parent_overlap);
        if (overlap == 0) {
          ldout(m_cct, 20) << "no parent overlap" << dendl;
          hide_parent = false;
        } else if (src_snap_seq == m_dst_zero_interval.begin()->first) {
          for (auto e : image_extents) {
            prev_end_size += e.second;
          }
          ceph_assert(prev_end_size <= m_dst_image_ctx->layout.object_size);
        }
      }
    }

    uint64_t end_size = prev_end_size;

    // update end_size if there are writes into higher offsets
    auto iter = m_write_ops.find(src_snap_seq);
    if (iter != m_write_ops.end()) {
      for (auto &copy_op : iter->second) {
        for (auto &e : copy_op.dst_extent_map) {
          end_size = std::max(end_size, e.first + e.second);
        }
      }
    }

    for (auto z = zero_interval.begin(); z != zero_interval.end(); z++) {
      if (z.get_start() + z.get_len() >= end_size) {
        // zero interval at the object end
        if (z.get_start() == 0 && hide_parent) {
          m_write_ops[src_snap_seq]
            .emplace_back(COPY_OP_TYPE_REMOVE_TRUNC, 0, 0, 0);
          ldout(m_cct, 20) << "COPY_OP_TYPE_REMOVE_TRUNC" << dendl;
        } else if (z.get_start() < prev_end_size) {
          if (z.get_start() == 0) {
            m_write_ops[src_snap_seq]
              .emplace_back(COPY_OP_TYPE_REMOVE, 0, 0, 0);
            ldout(m_cct, 20) << "COPY_OP_TYPE_REMOVE" << dendl;
          } else {
            m_write_ops[src_snap_seq]
              .emplace_back(COPY_OP_TYPE_TRUNC, 0, z.get_start(), 0);
            ldout(m_cct, 20) << "COPY_OP_TYPE_TRUNC " << z.get_start() << dendl;
          }
        }
        end_size = std::min(end_size, z.get_start());
      } else {
        // zero interval inside the object
        m_write_ops[src_snap_seq]
          .emplace_back(COPY_OP_TYPE_ZERO, 0, z.get_start(), z.get_len());
        ldout(m_cct, 20) << "COPY_OP_TYPE_ZERO " << z.get_start() << "~"
                         << z.get_len() << dendl;
      }
    }
    ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", end_size="
                     << end_size << dendl;
    if (end_size > 0 || hide_parent) {
      m_dst_object_state[src_snap_seq] = OBJECT_EXISTS;
      if (fast_diff && end_size == prev_end_size &&
          m_write_ops[src_snap_seq].empty()) {
        m_dst_object_state[src_snap_seq] = OBJECT_EXISTS_CLEAN;
      }
    }
    prev_end_size = end_size;
  }
}

template <typename I>
void ObjectCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  // ensure IoCtxs are closed prior to proceeding
  auto on_finish = m_on_finish;
  delete this;

  on_finish->complete(r);
}

template <typename I>
void ObjectCopyRequest<I>::compute_dst_object_may_exist() {
  RWLock::RLocker snap_locker(m_dst_image_ctx->snap_lock);

  auto snap_ids = m_dst_image_ctx->snaps;
  snap_ids.push_back(CEPH_NOSNAP);

  for (auto snap_id : snap_ids) {
    m_dst_object_may_exist[snap_id] =
      (m_dst_object_number < m_dst_image_ctx->get_object_count(snap_id));
  }
}

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::ObjectCopyRequest<librbd::ImageCtx>;
