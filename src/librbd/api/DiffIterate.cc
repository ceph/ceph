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
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/object_map/DiffRequest.h"
#include "include/rados/librados.hpp"
#include "include/interval_set.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "common/Throttle.h"
#include "osdc/Striper.h"
#include "librados/snap_set_diff.h"
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
  uint64_t from_snap_id;
  uint64_t end_snap_id;
  interval_set<uint64_t> parent_diff;
  OrderedThrottle throttle;

  template <typename I>
  DiffContext(I &image_ctx, DiffIterate<>::Callback callback,
              void *callback_arg, bool _whole_object, uint64_t _from_snap_id,
              uint64_t _end_snap_id)
    : callback(callback), callback_arg(callback_arg),
      whole_object(_whole_object), from_snap_id(_from_snap_id),
      end_snap_id(_end_snap_id),
      throttle(image_ctx.config.template get_val<uint64_t>("rbd_concurrent_management_ops"), true) {
  }
};

class C_DiffObject : public Context {
public:
  template <typename I>
  C_DiffObject(I &image_ctx, librados::IoCtx &head_ctx,
               DiffContext &diff_context, const std::string &oid,
               uint64_t offset, const std::vector<ObjectExtent> &object_extents)
    : m_cct(image_ctx.cct), m_head_ctx(head_ctx),
      m_diff_context(diff_context), m_oid(oid), m_offset(offset),
      m_object_extents(object_extents), m_snap_ret(0) {
  }

  void send() {
    C_OrderedThrottle *ctx = m_diff_context.throttle.start_op(this);
    librados::AioCompletion *rados_completion =
      util::create_rados_callback(ctx);

    librados::ObjectReadOperation op;
    op.list_snaps(&m_snap_set, &m_snap_ret);

    int r = m_head_ctx.aio_operate(m_oid, rados_completion, &op, NULL);
    ceph_assert(r == 0);
    rados_completion->release();
  }

protected:
  typedef boost::tuple<uint64_t, size_t, bool> Diff;
  typedef std::list<Diff> Diffs;

  void finish(int r) override {
    CephContext *cct = m_cct;
    if (r == 0 && m_snap_ret < 0) {
      r = m_snap_ret;
    }

    Diffs diffs;
    if (r == 0) {
      ldout(cct, 20) << "object " << m_oid << ": list_snaps complete" << dendl;
      compute_diffs(&diffs);
    } else if (r == -ENOENT) {
      ldout(cct, 20) << "object " << m_oid << ": list_snaps (not found)"
                     << dendl;
      r = 0;
      compute_parent_overlap(&diffs);
    } else {
      ldout(cct, 20) << "object " << m_oid << ": list_snaps failed: "
                     << cpp_strerror(r) << dendl;
    }

    if (r == 0) {
      for (Diffs::const_iterator d = diffs.begin(); d != diffs.end(); ++d) {
        r = m_diff_context.callback(d->get<0>(), d->get<1>(), d->get<2>(),
                                    m_diff_context.callback_arg);
        if (r < 0) {
          break;
        }
      }
    }
    m_diff_context.throttle.end_op(r);
  }

private:
  CephContext *m_cct;
  librados::IoCtx &m_head_ctx;
  DiffContext &m_diff_context;
  std::string m_oid;
  uint64_t m_offset;
  std::vector<ObjectExtent> m_object_extents;

  librados::snap_set_t m_snap_set;
  int m_snap_ret;

  void compute_diffs(Diffs *diffs) {
    CephContext *cct = m_cct;

    // calc diff from from_snap_id -> to_snap_id
    interval_set<uint64_t> diff;
    uint64_t end_size;
    bool end_exists;
    librados::snap_t clone_end_snap_id;
    bool whole_object;
    calc_snap_set_diff(cct, m_snap_set, m_diff_context.from_snap_id,
                       m_diff_context.end_snap_id, &diff, &end_size,
                       &end_exists, &clone_end_snap_id, &whole_object);
    if (whole_object) {
      ldout(cct, 1) << "object " << m_oid << ": need to provide full object"
                    << dendl;
    }
    ldout(cct, 20) << "  diff " << diff << " end_exists=" << end_exists
                   << dendl;
    if (diff.empty() && !whole_object) {
      if (m_diff_context.from_snap_id == 0 && !end_exists) {
        compute_parent_overlap(diffs);
      }
      return;
    } else if (m_diff_context.whole_object || whole_object) {
      // provide the full object extents to the callback
      for (vector<ObjectExtent>::iterator q = m_object_extents.begin();
           q != m_object_extents.end(); ++q) {
        diffs->push_back(boost::make_tuple(m_offset + q->offset, q->length,
                                           end_exists));
      }
      return;
    }

    for (vector<ObjectExtent>::iterator q = m_object_extents.begin();
         q != m_object_extents.end(); ++q) {
      ldout(cct, 20) << "diff_iterate object " << m_oid << " extent "
                     << q->offset << "~" << q->length << " from "
                     << q->buffer_extents << dendl;
      uint64_t opos = q->offset;
      for (vector<pair<uint64_t,uint64_t> >::iterator r =
             q->buffer_extents.begin();
           r != q->buffer_extents.end(); ++r) {
        interval_set<uint64_t> overlap;  // object extents
        overlap.insert(opos, r->second);
        overlap.intersection_of(diff);
        ldout(cct, 20) << " opos " << opos
                       << " buf " << r->first << "~" << r->second
                       << " overlap " << overlap << dendl;
        for (interval_set<uint64_t>::iterator s = overlap.begin();
    	       s != overlap.end(); ++s) {
          uint64_t su_off = s.get_start() - opos;
          uint64_t logical_off = m_offset + r->first + su_off;
          ldout(cct, 20) << "   overlap extent " << s.get_start() << "~"
                         << s.get_len() << " logical " << logical_off << "~"
                         << s.get_len() << dendl;
          diffs->push_back(boost::make_tuple(logical_off, s.get_len(),
                           end_exists));
        }
        opos += r->second;
      }
      ceph_assert(opos == q->offset + q->length);
    }
  }

  void compute_parent_overlap(Diffs *diffs) {
    if (m_diff_context.from_snap_id == 0 &&
        !m_diff_context.parent_diff.empty()) {
      // report parent diff instead
      for (vector<ObjectExtent>::iterator q = m_object_extents.begin();
           q != m_object_extents.end(); ++q) {
        for (vector<pair<uint64_t,uint64_t> >::iterator r =
               q->buffer_extents.begin();
             r != q->buffer_extents.end(); ++r) {
          interval_set<uint64_t> o;
          o.insert(m_offset + r->first, r->second);
          o.intersection_of(m_diff_context.parent_diff);
          ldout(m_cct, 20) << " reporting parent overlap " << o << dendl;
          for (interval_set<uint64_t>::iterator s = o.begin(); s != o.end();
               ++s) {
            diffs->push_back(boost::make_tuple(s.get_start(), s.get_len(),
                             true));
          }
        }
      }
    }
  }
};

int simple_diff_cb(uint64_t off, size_t len, int exists, void *arg) {
  // it's possible for a discard to create a hole in the parent image -- ignore
  if (exists) {
    interval_set<uint64_t> *diff = static_cast<interval_set<uint64_t> *>(arg);
    diff->insert(off, len);
  }
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
    auto req = io::ImageDispatchSpec<I>::create_flush_request(
      *ictx, aio_comp, io::FLUSH_SOURCE_INTERNAL, {});
    req->send();
    delete req;
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
  r = clip_io(ictx, off, &len);
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

  librados::IoCtx head_ctx;
  librados::snap_t from_snap_id = 0;
  librados::snap_t end_snap_id;
  uint64_t from_size = 0;
  uint64_t end_size;
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    head_ctx.dup(m_image_ctx.data_ctx);
    if (m_from_snap_name) {
      from_snap_id = m_image_ctx.get_snap_id(m_from_snap_namespace, m_from_snap_name);
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
  if (m_whole_object) {
    C_SaferCond ctx;
    auto req = object_map::DiffRequest<I>::create(&m_image_ctx, from_snap_id,
                                                  end_snap_id,
                                                  &object_diff_state, &ctx);
    req->send();

    r = ctx.wait();
    if (r < 0) {
      ldout(cct, 5) << "fast diff disabled" << dendl;
    } else {
      ldout(cct, 5) << "fast diff enabled" << dendl;
      fast_diff_enabled = true;
    }
  }

  // we must list snaps via the head, not end snap
  head_ctx.snap_set_read(CEPH_SNAPDIR);

  ldout(cct, 5) << "diff_iterate from " << from_snap_id << " to "
                << end_snap_id << " size from " << from_size
                << " to " << end_size << dendl;

  // check parent overlap only if we are comparing to the beginning of time
  DiffContext diff_context(m_image_ctx, m_callback, m_callback_arg,
                           m_whole_object, from_snap_id, end_snap_id);
  if (m_include_parent && from_snap_id == 0) {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    uint64_t overlap = 0;
    m_image_ctx.get_parent_overlap(m_image_ctx.snap_id, &overlap);
    r = 0;
    if (m_image_ctx.parent && overlap > 0) {
      ldout(cct, 10) << " first getting parent diff" << dendl;
      DiffIterate diff_parent(*m_image_ctx.parent, {},
			      nullptr, 0, overlap,
                              m_include_parent, m_whole_object,
                              &simple_diff_cb,
                              &diff_context.parent_diff);
      r = diff_parent.execute();
    }
    if (r < 0) {
      return r;
    }
  }

  uint64_t period = m_image_ctx.get_stripe_period();
  uint64_t off = m_offset;
  uint64_t left = m_length;

  while (left > 0) {
    uint64_t period_off = off - (off % period);
    uint64_t read_len = min(period_off + period - off, left);

    // map to extents
    map<object_t,vector<ObjectExtent> > object_extents;
    Striper::file_to_extents(cct, m_image_ctx.format_string,
                             &m_image_ctx.layout, off, read_len, 0,
                             object_extents, 0);

    // get snap info for each object
    for (map<object_t,vector<ObjectExtent> >::iterator p =
           object_extents.begin();
         p != object_extents.end(); ++p) {
      ldout(cct, 20) << "object " << p->first << dendl;

      if (fast_diff_enabled) {
        const uint64_t object_no = p->second.front().objectno;
        uint8_t diff_state = object_diff_state[object_no];
        if (diff_state == object_map::DIFF_STATE_HOLE &&
            from_snap_id == 0 && !diff_context.parent_diff.empty()) {
          // no data in child object -- report parent diff instead
          for (auto& oe : p->second) {
            for (auto& be : oe.buffer_extents) {
              interval_set<uint64_t> o;
              o.insert(off + be.first, be.second);
              o.intersection_of(diff_context.parent_diff);
              ldout(cct, 20) << " reporting parent overlap " << o << dendl;
              for (auto e = o.begin(); e != o.end(); ++e) {
                r = m_callback(e.get_start(), e.get_len(), true,
                               m_callback_arg);
                if (r < 0) {
                  return r;
                }
              }
            }
          }
        } else if (diff_state == object_map::DIFF_STATE_HOLE_UPDATED ||
                   diff_state == object_map::DIFF_STATE_DATA_UPDATED) {
          bool updated = (diff_state == object_map::DIFF_STATE_DATA_UPDATED);
          for (std::vector<ObjectExtent>::iterator q = p->second.begin();
               q != p->second.end(); ++q) {
            r = m_callback(off + q->offset, q->length, updated, m_callback_arg);
            if (r < 0) {
              return r;
            }
          }
        }
      } else {
        C_DiffObject *diff_object = new C_DiffObject(m_image_ctx, head_ctx,
                                                     diff_context,
                                                     p->first.name, off,
                                                     p->second);
        diff_object->send();

        if (diff_context.throttle.pending_error()) {
          r = diff_context.throttle.wait_for_ret();
          return r;
        }
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
