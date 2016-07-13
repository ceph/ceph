// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ImageSyncThrottler.h"
#include "ImageSync.h"
#include "common/ceph_context.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageSyncThrottler:: " << this \
                           << " " << __func__ << ": "
using std::unique_ptr;
using std::string;
using std::set;

namespace rbd {
namespace mirror {

template <typename I>
ImageSyncThrottler<I>::ImageSyncThrottler()
  : m_max_concurrent_syncs(g_ceph_context->_conf->rbd_mirror_concurrent_image_syncs),
    m_lock("rbd::mirror::ImageSyncThrottler")
{
  dout(20) << "Initialized max_concurrent_syncs=" << m_max_concurrent_syncs
           << dendl;
  g_ceph_context->_conf->add_observer(this);
}

template <typename I>
ImageSyncThrottler<I>::~ImageSyncThrottler() {
  {
    Mutex::Locker l(m_lock);
    assert(m_sync_queue.empty());
    assert(m_inflight_syncs.empty());
  }

  g_ceph_context->_conf->remove_observer(this);
}

template <typename I>
void ImageSyncThrottler<I>::start_sync(I *local_image_ctx, I *remote_image_ctx,
                                       SafeTimer *timer, Mutex *timer_lock,
                                       const std::string &mirror_uuid,
                                       Journaler *journaler,
                                       MirrorPeerClientMeta *client_meta,
                                       ContextWQ *work_queue,
                                       Context *on_finish,
                                       ProgressContext *progress_ctx) {
  dout(20) << dendl;

  PoolImageId pool_image_id(local_image_ctx->md_ctx.get_id(),
                            local_image_ctx->id);
  C_SyncHolder *sync_holder_ctx = new C_SyncHolder(this, pool_image_id,
                                                   on_finish);
  sync_holder_ctx->m_sync = ImageSync<I>::create(local_image_ctx,
                                                 remote_image_ctx, timer,
                                                 timer_lock, mirror_uuid,
                                                 journaler, client_meta,
                                                 work_queue, sync_holder_ctx,
                                                 progress_ctx);
  sync_holder_ctx->m_sync->get();

  bool start = false;
  {
    Mutex::Locker l(m_lock);

    if (m_inflight_syncs.size() < m_max_concurrent_syncs) {
      assert(m_inflight_syncs.count(pool_image_id) == 0);
      m_inflight_syncs[pool_image_id] = sync_holder_ctx;
      start = true;
      dout(10) << "ready to start image sync for local_image_id "
               << local_image_ctx->id << " [" << m_inflight_syncs.size() << "/"
               << m_max_concurrent_syncs << "]" << dendl;
    } else {
      m_sync_queue.push_front(sync_holder_ctx);
      dout(10) << "image sync for local_image_id " << local_image_ctx->id
               << " has been queued" << dendl;
    }
  }

  if (start) {
    sync_holder_ctx->m_sync->send();
  }
}

template <typename I>
void ImageSyncThrottler<I>::cancel_sync(librados::IoCtx &local_io_ctx,
                                        const std::string local_image_id) {
  dout(20) << dendl;

  C_SyncHolder *sync_holder = nullptr;
  bool running_sync = true;

  {
    Mutex::Locker l(m_lock);
    if (m_inflight_syncs.empty()) {
      // no image sync currently running and neither waiting
      return;
    }

    PoolImageId local_pool_image_id(local_io_ctx.get_id(),
                                    local_image_id);
    auto it = m_inflight_syncs.find(local_pool_image_id);
    if (it != m_inflight_syncs.end()) {
      sync_holder = it->second;
    }

    if (!sync_holder) {
      for (auto it = m_sync_queue.begin(); it != m_sync_queue.end(); ++it) {
        if ((*it)->m_local_pool_image_id == local_pool_image_id) {
          sync_holder = (*it);
          m_sync_queue.erase(it);
          running_sync = false;
          break;
        }
      }
    }
  }

  if (sync_holder) {
    if (running_sync) {
      dout(10) << "canceled running image sync for local_image_id "
               << sync_holder->m_local_pool_image_id.second << dendl;
      sync_holder->m_sync->cancel();
    } else {
      dout(10) << "canceled waiting image sync for local_image_id "
               << sync_holder->m_local_pool_image_id.second << dendl;
      sync_holder->m_on_finish->complete(-ECANCELED);
      sync_holder->m_sync->put();
      delete sync_holder;
    }
  }
}

template <typename I>
void ImageSyncThrottler<I>::handle_sync_finished(C_SyncHolder *sync_holder) {
  dout(20) << dendl;

  C_SyncHolder *next_sync_holder = nullptr;

  {
    Mutex::Locker l(m_lock);
    m_inflight_syncs.erase(sync_holder->m_local_pool_image_id);

    if (m_inflight_syncs.size() < m_max_concurrent_syncs &&
        !m_sync_queue.empty()) {
      next_sync_holder = m_sync_queue.back();
      m_sync_queue.pop_back();

      assert(
        m_inflight_syncs.count(next_sync_holder->m_local_pool_image_id) == 0);
      m_inflight_syncs[next_sync_holder->m_local_pool_image_id] =
        next_sync_holder;
      dout(10) << "ready to start image sync for local_image_id "
               << next_sync_holder->m_local_pool_image_id.second
               << " [" << m_inflight_syncs.size() << "/"
               << m_max_concurrent_syncs << "]" << dendl;
    }

    dout(10) << "currently running image syncs [" << m_inflight_syncs.size()
             << "/" << m_max_concurrent_syncs << "]" << dendl;
  }

  if (next_sync_holder) {
    next_sync_holder->m_sync->send();
  }
}

template <typename I>
void ImageSyncThrottler<I>::set_max_concurrent_syncs(uint32_t max) {
  dout(20) << " max=" << max << dendl;

  assert(max > 0);

  std::list<C_SyncHolder *> next_sync_holders;
  {
    Mutex::Locker l(m_lock);
    this->m_max_concurrent_syncs = max;

    // Start waiting syncs in the case of available free slots
    while(m_inflight_syncs.size() < m_max_concurrent_syncs
          && !m_sync_queue.empty()) {
        C_SyncHolder *next_sync_holder = m_sync_queue.back();
        next_sync_holders.push_back(next_sync_holder);
        m_sync_queue.pop_back();

        assert(
          m_inflight_syncs.count(next_sync_holder->m_local_pool_image_id) == 0);
        m_inflight_syncs[next_sync_holder->m_local_pool_image_id] =
          next_sync_holder;

        dout(10) << "ready to start image sync for local_image_id "
                 << next_sync_holder->m_local_pool_image_id.second
                 << " [" << m_inflight_syncs.size() << "/"
                 << m_max_concurrent_syncs << "]" << dendl;
    }
  }

  for (const auto& sync_holder : next_sync_holders) {
    sync_holder->m_sync->send();
  }
}

template <typename I>
void ImageSyncThrottler<I>::print_status(Formatter *f, stringstream *ss) {
  Mutex::Locker l(m_lock);

  if (f) {
    f->dump_int("max_parallel_syncs", m_max_concurrent_syncs);
    f->dump_int("running_syncs", m_inflight_syncs.size());
    f->dump_int("waiting_syncs", m_sync_queue.size());
    f->flush(*ss);
  } else {
    *ss << "[ ";
    *ss << "max_parallel_syncs=" << m_max_concurrent_syncs << ", ";
    *ss << "running_syncs=" << m_inflight_syncs.size() << ", ";
    *ss << "waiting_syncs=" << m_sync_queue.size() << " ]";
  }
}

template <typename I>
const char** ImageSyncThrottler<I>::get_tracked_conf_keys() const {
  static const char* KEYS[] = {
    "rbd_mirror_concurrent_image_syncs",
    NULL
  };
  return KEYS;
}

template <typename I>
void ImageSyncThrottler<I>::handle_conf_change(
                                              const struct md_config_t *conf,
                                              const set<string> &changed) {
  if (changed.count("rbd_mirror_concurrent_image_syncs")) {
    set_max_concurrent_syncs(conf->rbd_mirror_concurrent_image_syncs);
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageSyncThrottler<librbd::ImageCtx>;
