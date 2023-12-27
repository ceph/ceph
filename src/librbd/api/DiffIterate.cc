// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/api/DiffIterate.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/internal.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/object_map/DiffRequest.h"
#include "include/rados/librados.hpp"
#include "include/interval_set.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "common/Throttle.h"
#include "osdc/Striper.h"
#include <boost/tuple/tuple.hpp>
#include <list>
#include <map>
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::DiffIterate: "

namespace librbd {
namespace api {

namespace {

struct DiffContext {
  DiffIterate<>::Callback callback;
  void *callback_arg;
  bool whole_object;
  bool include_parent;
  uint64_t from_snap_id;
  uint64_t end_snap_id;
  OrderedThrottle throttle;

  template <typename I>
  DiffContext(I &image_ctx, DiffIterate<>::Callback callback,
              void *callback_arg, bool _whole_object, bool _include_parent,
              uint64_t _from_snap_id, uint64_t _end_snap_id)
    : callback(callback), callback_arg(callback_arg),
      whole_object(_whole_object), include_parent(_include_parent),
      from_snap_id(_from_snap_id), end_snap_id(_end_snap_id),
      throttle(image_ctx.config.template get_val<uint64_t>("rbd_concurrent_management_ops"), true) {
  }
};

template <typename I>
class C_DiffObject : public Context {
public:
  C_DiffObject(I &image_ctx, DiffContext &diff_context, uint64_t image_offset,
               uint64_t image_length)
    : m_image_ctx(image_ctx), m_cct(image_ctx.cct),
      m_diff_context(diff_context), m_image_offset(image_offset),
      m_image_length(image_length) {
  }

  void send() {
    Context* ctx = m_diff_context.throttle.start_op(this);
    auto aio_comp = io::AioCompletion::create_and_start(
      ctx, util::get_image_ctx(&m_image_ctx), io::AIO_TYPE_GENERIC);
    int list_snaps_flags = 0;
    if (!m_diff_context.include_parent || m_diff_context.from_snap_id != 0) {
      list_snaps_flags |= io::LIST_SNAPS_FLAG_DISABLE_LIST_FROM_PARENT;
    }
    if (m_diff_context.whole_object) {
      list_snaps_flags |= io::LIST_SNAPS_FLAG_WHOLE_OBJECT;
    }
    auto req = io::ImageDispatchSpec::create_list_snaps(
      m_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START,
      aio_comp, {{m_image_offset, m_image_length}}, io::ImageArea::DATA,
      {m_diff_context.from_snap_id, m_diff_context.end_snap_id},
      list_snaps_flags, &m_snapshot_delta, {});
    req->send();
  }

protected:
  typedef boost::tuple<uint64_t, size_t, bool> Diff;
  typedef std::list<Diff> Diffs;

  void finish(int r) override {
    CephContext *cct = m_cct;

    if (r < 0) {
      ldout(cct, 20) << "list_snaps failed: " << m_image_offset << "~"
                     << m_image_length << ": " << cpp_strerror(r) << dendl;
    }

    Diffs diffs;
    ldout(cct, 20) << "image extent " << m_image_offset << "~"
                     << m_image_length << ": list_snaps complete" << dendl;

    compute_diffs(&diffs);
    for (Diffs::const_iterator d = diffs.begin(); d != diffs.end(); ++d) {
      r = m_diff_context.callback(d->get<0>(), d->get<1>(), d->get<2>(),
                                  m_diff_context.callback_arg);
      if (r < 0) {
        break;
      }
    }
    m_diff_context.throttle.end_op(r);
  }

private:
  I& m_image_ctx;
  CephContext *m_cct;
  DiffContext &m_diff_context;
  uint64_t m_image_offset;
  uint64_t m_image_length;

  io::SnapshotDelta m_snapshot_delta;

  void compute_diffs(Diffs *diffs) {
    CephContext *cct = m_cct;

    // merge per-snapshot deltas into an aggregate
    io::SparseExtents aggregate_snapshot_extents;
    for (auto& [key, snapshot_extents] : m_snapshot_delta) {
      for (auto& snapshot_extent : snapshot_extents) {
        auto state = snapshot_extent.get_val().state;

        // ignore DNE object (and parent)
        if ((state == io::SPARSE_EXTENT_STATE_DNE) ||
            (key == io::INITIAL_WRITE_READ_SNAP_IDS &&
             state == io::SPARSE_EXTENT_STATE_ZEROED)) {
          continue;
        }

        aggregate_snapshot_extents.insert(
          snapshot_extent.get_off(), snapshot_extent.get_len(),
          {state, snapshot_extent.get_len()});
      }
    }

    // build delta callback set
    for (auto& snapshot_extent : aggregate_snapshot_extents) {
      ldout(cct, 20) << "off=" << snapshot_extent.get_off() << ", "
                     << "len=" << snapshot_extent.get_len() << ", "
                     << "state=" << snapshot_extent.get_val().state << dendl;
      diffs->emplace_back(
        snapshot_extent.get_off(), snapshot_extent.get_len(),
        snapshot_extent.get_val().state == io::SPARSE_EXTENT_STATE_DATA);
    }
  }
};

int simple_diff_cb(uint64_t off, size_t len, int exists, void *arg) {
  // This reads the existing extents in a parent from the beginning
  // of time.  Since images are thin-provisioned, the extents will
  // always represent data, not holes.
  ceph_assert(exists);
  auto diff = static_cast<interval_set<uint64_t>*>(arg);
  diff->insert(off, len);
  return 0;
}

} // anonymous namespace

template <typename I>
int DiffIterate<I>::diff_iterate(I *ictx,
				 const cls::rbd::SnapshotNamespace& from_snap_namespace,
				 const char *fromsnapname,
                                 uint64_t off, uint64_t len,
                                 bool include_parent, bool whole_object,
                                 int (*cb)(uint64_t, size_t, int, void *),
                                 void *arg)
{
  ldout(ictx->cct, 20) << "diff_iterate " << ictx << " off = " << off
      		 << " len = " << len << dendl;

  if (!ictx->data_ctx.is_valid()) {
    return -ENODEV;
  }

  // ensure previous writes are visible to listsnaps
  C_SaferCond flush_ctx;
  {
    std::shared_lock owner_locker{ictx->owner_lock};
    auto aio_comp = io::AioCompletion::create_and_start(&flush_ctx, ictx,
                                                        io::AIO_TYPE_FLUSH);
    auto req = io::ImageDispatchSpec::create_flush(
      *ictx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START,
      aio_comp, io::FLUSH_SOURCE_INTERNAL, {});
    req->send();
  }
  int r = flush_ctx.wait();
  if (r < 0) {
    return r;
  }

  r = ictx->state->refresh_if_required();
  if (r < 0) {
    return r;
  }

  ictx->image_lock.lock_shared();
  r = clip_io(ictx, off, &len, io::ImageArea::DATA);
  ictx->image_lock.unlock_shared();
  if (r < 0) {
    return r;
  }

  DiffIterate command(*ictx, from_snap_namespace, fromsnapname, off, len,
		      include_parent, whole_object, cb, arg);
  r = command.execute();
  return r;
}

template <typename I>
int DiffIterate<I>::execute() {
  CephContext* cct = m_image_ctx.cct;

  ceph_assert(m_image_ctx.data_ctx.is_valid());

  librados::snap_t from_snap_id = 0;
  librados::snap_t end_snap_id;
  uint64_t from_size = 0;
  uint64_t end_size;
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    if (m_from_snap_name) {
      from_snap_id = m_image_ctx.get_snap_id(m_from_snap_namespace,
                                             m_from_snap_name);
      from_size = m_image_ctx.get_image_size(from_snap_id);
    }
    end_snap_id = m_image_ctx.snap_id;
    end_size = m_image_ctx.get_image_size(end_snap_id);
  }

  if (from_snap_id == CEPH_NOSNAP) {
    return -ENOENT;
  }
  if (from_snap_id == end_snap_id) {
    // no diff.
    return 0;
  }
  if (from_snap_id >= end_snap_id) {
    return -EINVAL;
  }

  int r;
  bool fast_diff_enabled = false;
  BitVector<2> object_diff_state;
  interval_set<uint64_t> parent_diff;
  if (m_whole_object) {
    C_SaferCond ctx;
    auto req = object_map::DiffRequest<I>::create(&m_image_ctx, from_snap_id,
                                                  end_snap_id, true,
                                                  &object_diff_state, &ctx);
    req->send();

    r = ctx.wait();
    if (r < 0) {
      ldout(cct, 5) << "fast diff disabled" << dendl;
    } else {
      ldout(cct, 5) << "fast diff enabled" << dendl;
      fast_diff_enabled = true;

      // check parent overlap only if we are comparing to the beginning of time
      if (m_include_parent && from_snap_id == 0) {
        std::shared_lock image_locker{m_image_ctx.image_lock};
        uint64_t raw_overlap = 0;
        m_image_ctx.get_parent_overlap(m_image_ctx.snap_id, &raw_overlap);
        auto overlap = m_image_ctx.reduce_parent_overlap(raw_overlap, false);
        if (overlap.first > 0 && overlap.second == io::ImageArea::DATA) {
          ldout(cct, 10) << " first getting parent diff" << dendl;
          DiffIterate diff_parent(*m_image_ctx.parent, {}, nullptr, 0,
                                  overlap.first, true, true, &simple_diff_cb,
                                  &parent_diff);
          r = diff_parent.execute();
          if (r < 0) {
            return r;
          }
        }
      }
    }
  }

  ldout(cct, 5) << "diff_iterate from " << from_snap_id << " to "
                << end_snap_id << " size from " << from_size
                << " to " << end_size << dendl;
  DiffContext diff_context(m_image_ctx, m_callback, m_callback_arg,
                           m_whole_object, m_include_parent, from_snap_id,
                           end_snap_id);

  uint64_t period = m_image_ctx.get_stripe_period();
  uint64_t off = m_offset;
  uint64_t left = m_length;

  while (left > 0) {
    uint64_t period_off = round_down_to(off, period);
    uint64_t read_len = std::min(period_off + period - off, left);

    if (fast_diff_enabled) {
      // map to extents
      std::map<object_t,std::vector<ObjectExtent> > object_extents;
      Striper::file_to_extents(cct, m_image_ctx.format_string,
                               &m_image_ctx.layout, off, read_len, 0,
                               object_extents, 0);

      // get diff info for each object and merge adjacent stripe units
      // into an aggregate (this also sorts them)
      io::SparseExtents aggregate_sparse_extents;
      for (auto& [object, extents] : object_extents) {
        const uint64_t object_no = extents.front().objectno;
        uint8_t diff_state = object_diff_state[object_no];
        ldout(cct, 20) << "object " << object << ": diff_state="
                       << (int)diff_state << dendl;

        if (diff_state == object_map::DIFF_STATE_HOLE &&
            from_snap_id == 0 && !parent_diff.empty()) {
          // no data in child object -- report parent diff instead
          for (auto& oe : extents) {
            for (auto& be : oe.buffer_extents) {
              interval_set<uint64_t> o;
              o.insert(off + be.first, be.second);
              o.intersection_of(parent_diff);
              ldout(cct, 20) << " reporting parent overlap " << o << dendl;
              for (auto e = o.begin(); e != o.end(); ++e) {
                aggregate_sparse_extents.insert(e.get_start(), e.get_len(),
                                                {io::SPARSE_EXTENT_STATE_DATA,
                                                 e.get_len()});
              }
            }
          }
        } else if (diff_state == object_map::DIFF_STATE_HOLE_UPDATED ||
                   diff_state == object_map::DIFF_STATE_DATA_UPDATED) {
          auto state = (diff_state == object_map::DIFF_STATE_HOLE_UPDATED ?
              io::SPARSE_EXTENT_STATE_ZEROED : io::SPARSE_EXTENT_STATE_DATA);
          for (auto& oe : extents) {
            for (auto& be : oe.buffer_extents) {
              aggregate_sparse_extents.insert(off + be.first, be.second,
                                              {state, be.second});
            }
          }
        }
      }

      for (const auto& se : aggregate_sparse_extents) {
        ldout(cct, 20) << "off=" << se.get_off() << ", len=" << se.get_len()
                       << ", state=" << se.get_val().state << dendl;
        r = m_callback(se.get_off(), se.get_len(),
                       se.get_val().state == io::SPARSE_EXTENT_STATE_DATA,
                       m_callback_arg);
        if (r < 0) {
          return r;
        }
      }
    } else {
      auto diff_object = new C_DiffObject<I>(m_image_ctx, diff_context, off,
                                             read_len);
      diff_object->send();

      if (diff_context.throttle.pending_error()) {
        r = diff_context.throttle.wait_for_ret();
        return r;
      }
    }

    left -= read_len;
    off += read_len;
  }

  r = diff_context.throttle.wait_for_ret();
  if (r < 0) {
    return r;
  }
  return 0;
}

} // namespace api
} // namespace librbd

template class librbd::api::DiffIterate<librbd::ImageCtx>;
