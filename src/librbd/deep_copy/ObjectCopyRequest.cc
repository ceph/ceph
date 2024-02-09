// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ObjectCopyRequest.h"
#include "include/neorados/RADOS.hpp"
#include "common/errno.h"
#include "librados/snap_set_diff.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/deep_copy/Handler.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/AsyncOperation.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Utils.h"
#include "osdc/Striper.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::ObjectCopyRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace deep_copy {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;
using librbd::util::get_image_ctx;

template <typename I>
ObjectCopyRequest<I>::ObjectCopyRequest(I *src_image_ctx,
                                        I *dst_image_ctx,
                                        librados::snap_t src_snap_id_start,
                                        librados::snap_t dst_snap_id_start,
                                        const SnapMap &snap_map,
                                        uint64_t dst_object_number,
                                        uint32_t flags, Handler* handler,
                                        Context *on_finish)
  : m_src_image_ctx(src_image_ctx),
    m_dst_image_ctx(dst_image_ctx), m_cct(dst_image_ctx->cct),
    m_src_snap_id_start(src_snap_id_start),
    m_dst_snap_id_start(dst_snap_id_start), m_snap_map(snap_map),
    m_dst_object_number(dst_object_number), m_flags(flags),
    m_handler(handler), m_on_finish(on_finish) {
  ceph_assert(src_image_ctx->data_ctx.is_valid());
  ceph_assert(dst_image_ctx->data_ctx.is_valid());
  ceph_assert(!m_snap_map.empty());

  m_src_async_op = new io::AsyncOperation();
  m_src_async_op->start_op(*get_image_ctx(m_src_image_ctx));

  m_src_io_ctx.dup(m_src_image_ctx->data_ctx);
  m_dst_io_ctx.dup(m_dst_image_ctx->data_ctx);

  m_dst_oid = m_dst_image_ctx->get_object_name(dst_object_number);

  ldout(m_cct, 20) << "src_image_id=" << m_src_image_ctx->id
		   << ", dst_image_id=" << m_dst_image_ctx->id
	           << ", dst_oid=" << m_dst_oid
		   << ", src_snap_id_start=" << m_src_snap_id_start
		   << ", dst_snap_id_start=" << m_dst_snap_id_start
                   << ", snap_map=" << m_snap_map << dendl;
}

template <typename I>
void ObjectCopyRequest<I>::send() {
  send_list_snaps();
}

template <typename I>
void ObjectCopyRequest<I>::send_list_snaps() {
  // image extents are consistent across src and dst so compute once
  std::tie(m_image_extents, m_image_area) = io::util::object_to_area_extents(
      m_dst_image_ctx, m_dst_object_number,
      {{0, m_dst_image_ctx->layout.object_size}});
  ldout(m_cct, 20) << "image_extents=" << m_image_extents
                   << " area=" << m_image_area << dendl;

  auto ctx = create_async_context_callback(
    *m_src_image_ctx, create_context_callback<
      ObjectCopyRequest, &ObjectCopyRequest<I>::handle_list_snaps>(this));
  if ((m_flags & OBJECT_COPY_REQUEST_FLAG_EXISTS_CLEAN) != 0) {
    // skip listing the snaps if we know the destination exists and is clean,
    // but we do need to update the object-map
    ctx->complete(0);
    return;
  }

  io::SnapIds snap_ids;
  snap_ids.reserve(1 + m_snap_map.size());
  snap_ids.push_back(m_src_snap_id_start);
  for (auto& [src_snap_id, _] : m_snap_map) {
    if (m_src_snap_id_start < src_snap_id) {
      snap_ids.push_back(src_snap_id);
    }
  }

  auto list_snaps_flags = io::LIST_SNAPS_FLAG_DISABLE_LIST_FROM_PARENT;

  m_snapshot_delta.clear();

  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, get_image_ctx(m_src_image_ctx), io::AIO_TYPE_GENERIC);
  auto req = io::ImageDispatchSpec::create_list_snaps(
    *m_src_image_ctx, io::IMAGE_DISPATCH_LAYER_NONE, aio_comp,
    io::Extents{m_image_extents}, m_image_area, std::move(snap_ids),
    list_snaps_flags, &m_snapshot_delta, {});
  req->send();
}

template <typename I>
void ObjectCopyRequest<I>::handle_list_snaps(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to list snaps: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  ldout(m_cct, 20) << "snapshot_delta=" << m_snapshot_delta << dendl;

  compute_dst_object_may_exist();
  compute_read_ops();

  send_read();
}

template <typename I>
void ObjectCopyRequest<I>::send_read() {
  if (m_read_snaps.empty()) {
    // all snapshots have been read
    merge_write_ops();
    compute_zero_ops();

    trigger_copyup();
    return;
  }

  auto index = *m_read_snaps.begin();
  auto& read_op = m_read_ops[index];
  if (read_op.image_interval.empty()) {
    // nothing written to this object for this snapshot (must be trunc/remove)
    handle_read(0);
    return;
  }

  auto io_context = m_src_image_ctx->duplicate_data_io_context();
  io_context->set_read_snap(index.second);

  io::Extents image_extents{read_op.image_interval.begin(),
                            read_op.image_interval.end()};
  io::ReadResult read_result{&read_op.image_extent_map,
                             &read_op.out_bl};

  ldout(m_cct, 20) << "read: src_snap_seq=" << index.second << ", "
                   << "image_extents=" << image_extents << dendl;

  int op_flags = (LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                  LIBRADOS_OP_FLAG_FADVISE_NOCACHE);

  int read_flags = 0;
  if (index.second != m_src_image_ctx->snap_id) {
    read_flags |= io::READ_FLAG_DISABLE_CLIPPING;
  }

  auto ctx = create_context_callback<
    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_read>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, get_image_ctx(m_src_image_ctx), io::AIO_TYPE_READ);

  auto req = io::ImageDispatchSpec::create_read(
    *m_src_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    std::move(image_extents), m_image_area, std::move(read_result),
    io_context, op_flags, read_flags, {});
  req->send();
}

template <typename I>
void ObjectCopyRequest<I>::handle_read(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to read from source object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  if (m_handler != nullptr) {
    auto index = *m_read_snaps.begin();
    auto& read_op = m_read_ops[index];
    m_handler->handle_read(read_op.out_bl.length());
  }

  ceph_assert(!m_read_snaps.empty());
  m_read_snaps.erase(m_read_snaps.begin());

  send_read();
}

template <typename I>
void ObjectCopyRequest<I>::send_update_object_map() {
  if (!m_dst_image_ctx->test_features(RBD_FEATURE_OBJECT_MAP) ||
      m_dst_object_state.empty()) {
    process_copyup();
    return;
  }

  m_dst_image_ctx->owner_lock.lock_shared();
  m_dst_image_ctx->image_lock.lock_shared();
  if (m_dst_image_ctx->object_map == nullptr) {
    // possible that exclusive lock was lost in background
    lderr(m_cct) << "object map is not initialized" << dendl;

    m_dst_image_ctx->image_lock.unlock_shared();
    m_dst_image_ctx->owner_lock.unlock_shared();
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
    m_dst_image_ctx->image_lock.unlock_shared();
    m_dst_image_ctx->owner_lock.unlock_shared();
    finish(r);
    return;
  }

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
      handle_update_object_map(r);
      finish_op_ctx->complete(0);
    });

  auto dst_image_ctx = m_dst_image_ctx;
  bool sent = dst_image_ctx->object_map->template aio_update<
    Context, &Context::complete>(dst_snap_id, m_dst_object_number, object_state,
                                 {}, {}, false, ctx);

  // NOTE: state machine might complete before we reach here
  dst_image_ctx->image_lock.unlock_shared();
  dst_image_ctx->owner_lock.unlock_shared();
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

  process_copyup();
}

template <typename I>
void ObjectCopyRequest<I>::process_copyup() {
  if (m_snapshot_sparse_bufferlist.empty()) {
    // no data to copy or truncate/zero. only the copyup state machine cares
    // about whether the object exists or not, and it always copies from
    // snap id 0.
    finish(m_src_snap_id_start > 0 ? 0 : -ENOENT);
    return;
  }

  ldout(m_cct, 20) << dendl;

  // let dispatch layers have a chance to process the data but
  // assume that the dispatch layer will only touch the sparse bufferlist
  auto r = m_dst_image_ctx->io_object_dispatcher->prepare_copyup(
    m_dst_object_number, &m_snapshot_sparse_bufferlist);
  if (r < 0) {
    lderr(m_cct) << "failed to prepare copyup data: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  send_write_object();
}

template <typename I>
void ObjectCopyRequest<I>::send_write_object() {
  ceph_assert(!m_snapshot_sparse_bufferlist.empty());
  auto& sparse_bufferlist = m_snapshot_sparse_bufferlist.begin()->second;

  m_src_image_ctx->image_lock.lock_shared();
  bool hide_parent = (m_src_snap_id_start == 0 &&
                      m_src_image_ctx->parent != nullptr);
  m_src_image_ctx->image_lock.unlock_shared();

  // retrieve the destination snap context for the op
  SnapIds dst_snap_ids;
  librados::snap_t dst_snap_seq = 0;
  librados::snap_t src_snap_seq = m_snapshot_sparse_bufferlist.begin()->first;
  if (src_snap_seq != 0) {
    auto snap_map_it = m_snap_map.find(src_snap_seq);
    ceph_assert(snap_map_it != m_snap_map.end());

    auto dst_snap_id = snap_map_it->second.front();
    auto dst_may_exist_it = m_dst_object_may_exist.find(dst_snap_id);
    ceph_assert(dst_may_exist_it != m_dst_object_may_exist.end());
    if (!dst_may_exist_it->second && !sparse_bufferlist.empty()) {
      // if the object cannot exist, the only valid op is to remove it
      ldout(m_cct, 20) << "object DNE: src_snap_seq=" << src_snap_seq << dendl;
      ceph_assert(sparse_bufferlist.ext_count() == 1U);
      ceph_assert(sparse_bufferlist.begin().get_val().state ==
                    io::SPARSE_EXTENT_STATE_ZEROED &&
                  sparse_bufferlist.begin().get_off() == 0 &&
                  sparse_bufferlist.begin().get_len() ==
                    m_dst_image_ctx->layout.object_size);
    }

    // write snapshot context should be before actual snapshot
    ceph_assert(!snap_map_it->second.empty());
    auto dst_snap_ids_it = snap_map_it->second.begin();
    ++dst_snap_ids_it;

    dst_snap_ids = SnapIds{dst_snap_ids_it, snap_map_it->second.end()};
    if (!dst_snap_ids.empty()) {
      dst_snap_seq = dst_snap_ids.front();
    }
    ceph_assert(dst_snap_seq != CEPH_NOSNAP);
  }

  ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                   << "dst_snap_seq=" << dst_snap_seq << ", "
                   << "dst_snaps=" << dst_snap_ids << dendl;

  librados::ObjectWriteOperation op;

  bool migration = ((m_flags & OBJECT_COPY_REQUEST_FLAG_MIGRATION) != 0);
  if (migration) {
    ldout(m_cct, 20) << "assert_snapc_seq=" << dst_snap_seq << dendl;
    cls_client::assert_snapc_seq(&op, dst_snap_seq,
                                 cls::rbd::ASSERT_SNAPC_SEQ_GT_SNAPSET_SEQ);
  }

  for (auto& sbe : sparse_bufferlist) {
    switch (sbe.get_val().state) {
    case io::SPARSE_EXTENT_STATE_DATA:
      ldout(m_cct, 20) << "write op: " << sbe.get_off() << "~"
                       << sbe.get_len() << dendl;
      op.write(sbe.get_off(), std::move(sbe.get_val().bl));
      op.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                       LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
      break;
    case io::SPARSE_EXTENT_STATE_ZEROED:
      if (sbe.get_off() + sbe.get_len() ==
            m_dst_image_ctx->layout.object_size) {
        if (sbe.get_off() == 0) {
          if (hide_parent) {
            ldout(m_cct, 20) << "create+truncate op" << dendl;
            op.create(false);
            op.truncate(0);
          } else {
            ldout(m_cct, 20) << "remove op" << dendl;
            op.remove();
          }
        } else {
          ldout(m_cct, 20) << "trunc op: " << sbe.get_off() << dendl;
          op.truncate(sbe.get_off());
        }
      } else {
        ldout(m_cct, 20) << "zero op: " << sbe.get_off() << "~"
                         << sbe.get_len() << dendl;
        op.zero(sbe.get_off(), sbe.get_len());
      }
      break;
    default:
      ceph_abort();
    }
  }

  if (op.size() == (migration ? 1 : 0)) {
    handle_write_object(0);
    return;
  }

  int r;
  Context *finish_op_ctx;
  {
    std::shared_lock owner_locker{m_dst_image_ctx->owner_lock};
    finish_op_ctx = start_lock_op(m_dst_image_ctx->owner_lock, &r);
  }
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  auto ctx = new LambdaContext([this, finish_op_ctx](int r) {
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

  m_snapshot_sparse_bufferlist.erase(m_snapshot_sparse_bufferlist.begin());
  if (!m_snapshot_sparse_bufferlist.empty()) {
    send_write_object();
    return;
  }

  finish(0);
}

template <typename I>
Context *ObjectCopyRequest<I>::start_lock_op(ceph::shared_mutex &owner_lock,
					     int* r) {
  ceph_assert(ceph_mutex_is_locked(m_dst_image_ctx->owner_lock));
  if (m_dst_image_ctx->exclusive_lock == nullptr) {
    return new LambdaContext([](int r) {});
  }
  return m_dst_image_ctx->exclusive_lock->start_op(r);
}

template <typename I>
void ObjectCopyRequest<I>::compute_read_ops() {
  ldout(m_cct, 20) << dendl;

  m_src_image_ctx->image_lock.lock_shared();
  bool read_from_parent = (m_src_snap_id_start == 0 &&
                           m_src_image_ctx->parent != nullptr);
  m_src_image_ctx->image_lock.unlock_shared();

  bool only_dne_extents = true;
  interval_set<uint64_t> dne_image_interval;

  // compute read ops for any data sections or for any extents that we need to
  // read from our parent
  for (auto& [key, image_intervals] : m_snapshot_delta) {
    io::WriteReadSnapIds write_read_snap_ids{key};

    // advance the src write snap id to the first valid snap id
    if (write_read_snap_ids.first > m_src_snap_id_start) {
      // don't attempt to read from snapshots that shouldn't exist in
      // case the OSD fails to give a correct snap list
      auto snap_map_it = m_snap_map.find(write_read_snap_ids.first);
      ceph_assert(snap_map_it != m_snap_map.end());
      auto dst_snap_seq = snap_map_it->second.front();

      auto dst_may_exist_it = m_dst_object_may_exist.find(dst_snap_seq);
      ceph_assert(dst_may_exist_it != m_dst_object_may_exist.end());
      if (!dst_may_exist_it->second) {
        ldout(m_cct, 20) << "DNE snapshot: " << write_read_snap_ids.first
                         << dendl;
        continue;
      }
    }

    for (auto& image_interval : image_intervals) {
      auto state = image_interval.get_val().state;
      switch (state) {
      case io::SPARSE_EXTENT_STATE_DNE:
        if (write_read_snap_ids == io::INITIAL_WRITE_READ_SNAP_IDS &&
            read_from_parent) {
          // special-case for DNE initial object-extents since when flattening
          // we need to read data from the parent images extents
          ldout(m_cct, 20) << "DNE extent: "
                           << image_interval.get_off() << "~"
                           << image_interval.get_len() << dendl;
          dne_image_interval.insert(
            image_interval.get_off(), image_interval.get_len());
        }
        break;
      case io::SPARSE_EXTENT_STATE_ZEROED:
        only_dne_extents = false;
        break;
      case io::SPARSE_EXTENT_STATE_DATA:
        ldout(m_cct, 20) << "read op: "
                         << "snap_ids=" << write_read_snap_ids << " "
                         << image_interval.get_off() << "~"
                         << image_interval.get_len() << dendl;
        m_read_ops[write_read_snap_ids].image_interval.union_insert(
          image_interval.get_off(), image_interval.get_len());
        only_dne_extents = false;
        break;
      default:
        ceph_abort();
        break;
      }
    }
  }

  bool flatten = ((m_flags & OBJECT_COPY_REQUEST_FLAG_FLATTEN) != 0);
  if (!dne_image_interval.empty() && (!only_dne_extents || flatten)) {
    auto snap_map_it = m_snap_map.begin();
    ceph_assert(snap_map_it != m_snap_map.end());

    auto src_snap_seq = snap_map_it->first;
    WriteReadSnapIds write_read_snap_ids{src_snap_seq, src_snap_seq};

    // prepare to prune the extents to the maximum parent overlap
    std::shared_lock image_locker(m_src_image_ctx->image_lock);
    uint64_t raw_overlap = 0;
    int r = m_src_image_ctx->get_parent_overlap(src_snap_seq, &raw_overlap);
    if (r < 0) {
      ldout(m_cct, 5) << "failed getting parent overlap for snap_id: "
                      << src_snap_seq << ": " << cpp_strerror(r) << dendl;
    } else if (raw_overlap > 0) {
      ldout(m_cct, 20) << "raw_overlap=" << raw_overlap << dendl;
      io::Extents parent_extents;
      for (auto [image_offset, image_length] : dne_image_interval) {
        parent_extents.emplace_back(image_offset, image_length);
      }
      m_src_image_ctx->prune_parent_extents(parent_extents, m_image_area,
                                            raw_overlap, false);
      for (auto [image_offset, image_length] : parent_extents) {
        ldout(m_cct, 20) << "parent read op: "
                         << "snap_ids=" << write_read_snap_ids << " "
                         << image_offset << "~" << image_length << dendl;
        m_read_ops[write_read_snap_ids].image_interval.union_insert(
          image_offset, image_length);
      }
    }
  }

  for (auto& [write_read_snap_ids, _] : m_read_ops) {
    m_read_snaps.push_back(write_read_snap_ids);
  }
}

template <typename I>
void ObjectCopyRequest<I>::merge_write_ops() {
  ldout(m_cct, 20) << dendl;

  for (auto& [write_read_snap_ids, read_op] : m_read_ops) {
    auto src_snap_seq = write_read_snap_ids.first;

    // convert the resulting sparse image extent map to an interval ...
    auto& image_data_interval = m_dst_data_interval[src_snap_seq];
    for (auto [image_offset, image_length] : read_op.image_extent_map) {
      image_data_interval.union_insert(image_offset, image_length);
    }

    // ... and compute the difference between it and the image extents since
    // that indicates zeroed extents
    interval_set<uint64_t> intersection;
    intersection.intersection_of(read_op.image_interval, image_data_interval);
    read_op.image_interval.subtract(intersection);

    for (auto& [image_offset, image_length] : read_op.image_interval) {
      ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                       << "inserting sparse-read zero " << image_offset << "~"
                       << image_length << dendl;
      m_dst_zero_interval[src_snap_seq].union_insert(
        image_offset, image_length);
    }

    uint64_t buffer_offset = 0;
    for (auto [image_offset, image_length] : read_op.image_extent_map) {
      // convert image extents back to object extents for the write op
      striper::LightweightObjectExtents object_extents;
      io::util::area_to_object_extents(m_dst_image_ctx, image_offset,
                                       image_length, m_image_area,
                                       buffer_offset, &object_extents);
      for (auto& object_extent : object_extents) {
        ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                         << "object_offset=" << object_extent.offset << ", "
                         << "object_length=" << object_extent.length << dendl;

        bufferlist sub_bl;
        sub_bl.substr_of(read_op.out_bl, buffer_offset, object_extent.length);

        m_snapshot_sparse_bufferlist[src_snap_seq].insert(
          object_extent.offset, object_extent.length,
          {io::SPARSE_EXTENT_STATE_DATA, object_extent.length,\
           std::move(sub_bl)});

        buffer_offset += object_extent.length;
      }
    }
  }
}

template <typename I>
void ObjectCopyRequest<I>::compute_zero_ops() {
  ldout(m_cct, 20) << dendl;

  m_src_image_ctx->image_lock.lock_shared();
  bool hide_parent = (m_src_snap_id_start == 0 &&
                      m_src_image_ctx->parent != nullptr);
  m_src_image_ctx->image_lock.unlock_shared();

  // ensure we have a zeroed interval for each snapshot
  for (auto& [src_snap_seq, _] : m_snap_map) {
    if (m_src_snap_id_start < src_snap_seq) {
      m_dst_zero_interval[src_snap_seq];
    }
  }

  // exists if copying from an arbitrary snapshot w/o any deltas in the
  // start snapshot slot (i.e. DNE)
  bool object_exists = (
      m_src_snap_id_start > 0 &&
      m_snapshot_delta.count({m_src_snap_id_start, m_src_snap_id_start}) == 0);
  bool fast_diff = m_dst_image_ctx->test_features(RBD_FEATURE_FAST_DIFF);
  uint64_t prev_end_size = 0;

  // compute zero ops from the zeroed intervals
  for (auto &it : m_dst_zero_interval) {
    auto src_snap_seq = it.first;
    auto &zero_interval = it.second;

    auto snap_map_it = m_snap_map.find(src_snap_seq);
    ceph_assert(snap_map_it != m_snap_map.end());
    auto dst_snap_seq = snap_map_it->second.front();

    auto dst_may_exist_it = m_dst_object_may_exist.find(dst_snap_seq);
    ceph_assert(dst_may_exist_it != m_dst_object_may_exist.end());
    if (!dst_may_exist_it->second && object_exists) {
      ldout(m_cct, 5) << "object DNE for snap_id: " << dst_snap_seq << dendl;
      m_snapshot_sparse_bufferlist[src_snap_seq].insert(
        0, m_dst_image_ctx->layout.object_size,
        {io::SPARSE_EXTENT_STATE_ZEROED, m_dst_image_ctx->layout.object_size});
      object_exists = false;
      prev_end_size = 0;
      continue;
    }

    if (hide_parent) {
      std::shared_lock image_locker{m_dst_image_ctx->image_lock};
      uint64_t raw_overlap = 0;
      uint64_t object_overlap = 0;
      int r = m_dst_image_ctx->get_parent_overlap(dst_snap_seq, &raw_overlap);
      if (r < 0) {
        ldout(m_cct, 5) << "failed getting parent overlap for snap_id: "
                        << dst_snap_seq << ": " << cpp_strerror(r) << dendl;
      } else if (raw_overlap > 0) {
        auto parent_extents = m_image_extents;
        object_overlap = m_dst_image_ctx->prune_parent_extents(
            parent_extents, m_image_area, raw_overlap, false);
      }
      if (object_overlap == 0) {
        ldout(m_cct, 20) << "no parent overlap" << dendl;
        hide_parent = false;
      }
    }

    // collect known zeroed extents from the snapshot delta for the current
    // src snapshot. If this is the first snapshot, we might need to handle
    // the whiteout case if it overlaps with the parent
    auto first_src_snap_id = m_snap_map.begin()->first;
    auto snapshot_delta_it = m_snapshot_delta.lower_bound(
      {(hide_parent && src_snap_seq == first_src_snap_id ?
         0 : src_snap_seq), 0});
    for (; snapshot_delta_it != m_snapshot_delta.end() &&
           snapshot_delta_it->first.first <= src_snap_seq;
         ++snapshot_delta_it) {
      auto& write_read_snap_ids = snapshot_delta_it->first;
      auto& image_intervals = snapshot_delta_it->second;
      for (auto& image_interval : image_intervals) {
        auto state = image_interval.get_val().state;
        switch (state) {
        case io::SPARSE_EXTENT_STATE_ZEROED:
          if (write_read_snap_ids != io::INITIAL_WRITE_READ_SNAP_IDS) {
            ldout(m_cct, 20) << "zeroed extent: "
                             << "src_snap_seq=" << src_snap_seq << " "
                             << image_interval.get_off() << "~"
                             << image_interval.get_len() << dendl;
            zero_interval.union_insert(
              image_interval.get_off(), image_interval.get_len());
          } else if (hide_parent &&
                     write_read_snap_ids == io::INITIAL_WRITE_READ_SNAP_IDS) {
            ldout(m_cct, 20) << "zeroed (hide parent) extent: "
                             << "src_snap_seq=" << src_snap_seq << "  "
                             << image_interval.get_off() << "~"
                             << image_interval.get_len() << dendl;
            zero_interval.union_insert(
              image_interval.get_off(), image_interval.get_len());
          }
          break;
        case io::SPARSE_EXTENT_STATE_DNE:
        case io::SPARSE_EXTENT_STATE_DATA:
          break;
        default:
          ceph_abort();
          break;
        }
      }
    }

    // subtract any data intervals from our zero intervals
    auto& data_interval = m_dst_data_interval[src_snap_seq];
    interval_set<uint64_t> intersection;
    intersection.intersection_of(zero_interval, data_interval);
    zero_interval.subtract(intersection);

    // update end_size if there are writes into higher offsets
    uint64_t end_size = prev_end_size;
    auto iter = m_snapshot_sparse_bufferlist.find(src_snap_seq);
    if (iter != m_snapshot_sparse_bufferlist.end()) {
      for (auto &sparse_bufferlist : iter->second) {
        object_exists = true;
        end_size = std::max(
          end_size, sparse_bufferlist.get_off() + sparse_bufferlist.get_len());
      }
    }

    ldout(m_cct, 20) << "src_snap_seq=" << src_snap_seq << ", "
                     << "dst_snap_seq=" << dst_snap_seq << ", "
                     << "zero_interval=" << zero_interval << ", "
                     << "end_size=" << end_size << dendl;
    for (auto z = zero_interval.begin(); z != zero_interval.end(); ++z) {
      // convert image extents back to object extents for the write op
      striper::LightweightObjectExtents object_extents;
      io::util::area_to_object_extents(m_dst_image_ctx, z.get_start(),
                                       z.get_len(), m_image_area, 0,
                                       &object_extents);
      for (auto& object_extent : object_extents) {
        ceph_assert(object_extent.offset + object_extent.length <=
                      m_dst_image_ctx->layout.object_size);

        if (object_extent.offset + object_extent.length >= end_size) {
          // zero interval at the object end
          if ((object_extent.offset == 0 && hide_parent) ||
              (object_extent.offset < prev_end_size)) {
            ldout(m_cct, 20) << "truncate " << object_extent.offset
                             << dendl;
            auto length =
              m_dst_image_ctx->layout.object_size - object_extent.offset;
            m_snapshot_sparse_bufferlist[src_snap_seq].insert(
              object_extent.offset, length,
              {io::SPARSE_EXTENT_STATE_ZEROED, length});
          }

          object_exists = (object_extent.offset > 0 || hide_parent);
          end_size = std::min(end_size, object_extent.offset);
        } else {
          // zero interval inside the object
          ldout(m_cct, 20) << "zero "
                           << object_extent.offset << "~"
                           << object_extent.length << dendl;
          m_snapshot_sparse_bufferlist[src_snap_seq].insert(
            object_extent.offset, object_extent.length,
            {io::SPARSE_EXTENT_STATE_ZEROED, object_extent.length});
          object_exists = true;
        }
      }
    }

    uint8_t dst_object_map_state = OBJECT_NONEXISTENT;
    if (object_exists) {
      dst_object_map_state = OBJECT_EXISTS;
      if (fast_diff && m_snapshot_sparse_bufferlist.count(src_snap_seq) == 0) {
        dst_object_map_state = OBJECT_EXISTS_CLEAN;
      }
      m_dst_object_state[src_snap_seq] = dst_object_map_state;
    }

    ldout(m_cct, 20) << "dst_snap_seq=" << dst_snap_seq << ", "
                     << "end_size=" << end_size << ", "
                     << "dst_object_map_state="
                     << static_cast<uint32_t>(dst_object_map_state) << dendl;
    prev_end_size = end_size;
  }
}

template <typename I>
void ObjectCopyRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  // ensure IoCtxs are closed prior to proceeding
  auto on_finish = m_on_finish;

  m_src_async_op->finish_op();
  delete m_src_async_op;
  delete this;

  on_finish->complete(r);
}

template <typename I>
void ObjectCopyRequest<I>::compute_dst_object_may_exist() {
  std::shared_lock image_locker{m_dst_image_ctx->image_lock};

  auto snap_ids = m_dst_image_ctx->snaps;
  snap_ids.push_back(CEPH_NOSNAP);

  for (auto snap_id : snap_ids) {
    m_dst_object_may_exist[snap_id] =
      (m_dst_object_number < m_dst_image_ctx->get_object_count(snap_id));
  }

  ldout(m_cct, 20) << "dst_object_may_exist=" << m_dst_object_may_exist
                   << dendl;
}

template <typename I>
void ObjectCopyRequest<I>::trigger_copyup() {
  // When performing a deep copy with incremental snapshots, it is possible
  // that not all data will be synced correctly. A write to a non-existent
  // object in a cloned image triggers a copyup which updates the older synced
  // snaps with the parent data. As this snapshot was already processed, the changes
  // are not copied again and the parent data will not be synced, causing data
  // corruption.
  // For cloned images, we now trigger a copyup on the object and write the parent
  // data to the object before writing the new clone image data.
  if ((m_dst_image_ctx->parent != nullptr) &&
      (m_src_snap_id_start != 0 && m_src_image_ctx->parent != nullptr)) {
    ldout(m_cct, 20) << dendl;
    auto io_context = m_dst_image_ctx->get_data_io_context();
    auto ctx = create_context_callback<
	    ObjectCopyRequest<I>, &ObjectCopyRequest<I>::handle_trigger_copyup>(this);
    bool r = io::util::trigger_copyup(m_dst_image_ctx, m_dst_object_number,
				      io_context, ctx);
    if (r == false) {
      // No parent found
      delete ctx;
      send_update_object_map();
      return;
    }
  } else {
    send_update_object_map();
    return;
  }
}


template <typename I>
void ObjectCopyRequest<I>::handle_trigger_copyup(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to trigger copyup for destination object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  send_update_object_map();
}

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::ObjectCopyRequest<librbd::ImageCtx>;
