// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ListWatchersRequest.h"
#include "common/RWLock.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Utils.h"

#include <algorithm>
#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::ListWatchersRequest: " << this \
                           << " " << __func__ << ": "

static std::ostream& operator<<(std::ostream& os, const obj_watch_t& watch) {
  os << "{addr=" << watch.addr << ", "
     << "watcher_id=" << watch.watcher_id << ", "
     << "cookie=" << watch.cookie << "}";
  return os;
}

namespace librbd {
namespace image {

using librados::IoCtx;
using util::create_rados_callback;

template<typename I>
ListWatchersRequest<I>::ListWatchersRequest(I &image_ctx, int flags,
                                            std::list<obj_watch_t> *watchers,
                                            Context *on_finish)
  : m_image_ctx(image_ctx), m_flags(flags), m_watchers(watchers),
    m_on_finish(on_finish), m_cct(m_image_ctx.cct) {
  ceph_assert((m_flags & LIST_WATCHERS_FILTER_OUT_MIRROR_INSTANCES) == 0 ||
              (m_flags & LIST_WATCHERS_MIRROR_INSTANCES_ONLY) == 0);
}

template<typename I>
void ListWatchersRequest<I>::send() {
  ldout(m_cct, 20) << dendl;

  list_image_watchers();
}

template<typename I>
void ListWatchersRequest<I>::list_image_watchers() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  op.list_watchers(&m_object_watchers, &m_ret_val);

  using klass = ListWatchersRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_list_image_watchers>(this);

  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op, &m_out_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void ListWatchersRequest<I>::handle_list_image_watchers(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == 0 && m_ret_val < 0) {
    r = m_ret_val;
  }
  if (r < 0) {
    lderr(m_cct) << "error listing image watchers: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  ldout(m_cct, 20) << "object_watchers=" << m_object_watchers << dendl;
  list_mirror_watchers();
}

template<typename I>
void ListWatchersRequest<I>::list_mirror_watchers() {
  if ((m_object_watchers.empty()) ||
      (m_flags & (LIST_WATCHERS_FILTER_OUT_MIRROR_INSTANCES |
                  LIST_WATCHERS_MIRROR_INSTANCES_ONLY)) == 0) {
    finish(0);
    return;
  }

  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  op.list_watchers(&m_mirror_watchers, &m_ret_val);

  using klass = ListWatchersRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_list_mirror_watchers>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(RBD_MIRRORING, rados_completion,
                                         &op, &m_out_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void ListWatchersRequest<I>::handle_list_mirror_watchers(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == 0 && m_ret_val < 0) {
    r = m_ret_val;
  }
  if (r < 0 && r != -ENOENT) {
    ldout(m_cct, 1) << "error listing mirror watchers: " << cpp_strerror(r)
                    << dendl;
  }

  ldout(m_cct, 20) << "mirror_watchers=" << m_mirror_watchers << dendl;
  finish(0);
}

template<typename I>
void ListWatchersRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == 0) {
    m_watchers->clear();

    if (m_object_watchers.size() > 0) {
      std::shared_lock owner_locker{m_image_ctx.owner_lock};
      uint64_t watch_handle = m_image_ctx.image_watcher != nullptr ?
        m_image_ctx.image_watcher->get_watch_handle() : 0;

      for (auto &w : m_object_watchers) {
        if ((m_flags & LIST_WATCHERS_FILTER_OUT_MY_INSTANCE) != 0) {
          if (w.cookie == watch_handle) {
            ldout(m_cct, 20) << "filtering out my instance: " << w << dendl;
            continue;
          }
        }
        auto it = std::find_if(m_mirror_watchers.begin(),
                               m_mirror_watchers.end(),
                               [w] (obj_watch_t &watcher) {
                                 return (strncmp(w.addr, watcher.addr,
                                                 sizeof(w.addr)) == 0);
                               });
        if ((m_flags & LIST_WATCHERS_FILTER_OUT_MIRROR_INSTANCES) != 0) {
          if (it != m_mirror_watchers.end()) {
            ldout(m_cct, 20) << "filtering out mirror instance: " << w << dendl;
            continue;
          }
        } else if ((m_flags & LIST_WATCHERS_MIRROR_INSTANCES_ONLY) != 0) {
          if (it == m_mirror_watchers.end()) {
            ldout(m_cct, 20) << "filtering out non-mirror instance: " << w
                             << dendl;
            continue;
          }
        }
        m_watchers->push_back(w);
      }
    }
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::ListWatchersRequest<librbd::ImageCtx>;
