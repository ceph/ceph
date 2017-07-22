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
#include "common/Formatter.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageSyncThrottler:: " << this \
                           << " " << __func__ << ": "

namespace rbd {
namespace mirror {

template <typename I>
ImageSyncThrottler<I>::ImageSyncThrottler()
  : m_lock(librbd::util::unique_lock_name("rbd::mirror::ImageSyncThrottler",
                                          this)),
    m_max_concurrent_syncs(
      g_ceph_context->_conf->rbd_mirror_concurrent_image_syncs) {
  dout(20) << "max_concurrent_syncs=" << m_max_concurrent_syncs << dendl;
  g_ceph_context->_conf->add_observer(this);
}

template <typename I>
ImageSyncThrottler<I>::~ImageSyncThrottler() {
  g_ceph_context->_conf->remove_observer(this);

  Mutex::Locker locker(m_lock);
  assert(m_inflight_ops.empty());
  assert(m_queue.empty());
}

template <typename I>
void ImageSyncThrottler<I>::start_op(const std::string &id, Context *on_start) {
  dout(20) << "id=" << id << dendl;

  {
    Mutex::Locker locker(m_lock);

    if (m_inflight_ops.count(id) > 0) {
      dout(20) << "duplicate for already started op " << id << dendl;
    } else if (m_max_concurrent_syncs == 0 ||
               m_inflight_ops.size() < m_max_concurrent_syncs) {
      assert(m_queue.empty());
      m_inflight_ops.insert(id);
      dout(20) << "ready to start sync for " << id << " ["
               << m_inflight_ops.size() << "/" << m_max_concurrent_syncs << "]"
               << dendl;
    } else {
      m_queue.push_back(std::make_pair(id, on_start));
      on_start = nullptr;
      dout(20) << "image sync for " << id << " has been queued" << dendl;
    }
  }

  if (on_start != nullptr) {
    on_start->complete(0);
  }
}

template <typename I>
bool ImageSyncThrottler<I>::cancel_op(const std::string &id) {
  dout(20) << "id=" << id << dendl;

  Context *on_start = nullptr;
  {
    Mutex::Locker locker(m_lock);
    for (auto it = m_queue.begin(); it != m_queue.end(); ++it) {
      if (it->first == id) {
        on_start = it->second;
        dout(20) << "canceled queued sync for " << id << dendl;
        m_queue.erase(it);
        break;
      }
    }
  }

  if (on_start == nullptr) {
    return false;
  }

  on_start->complete(-ECANCELED);
  return true;
}

template <typename I>
void ImageSyncThrottler<I>::finish_op(const std::string &id) {
  dout(20) << "id=" << id << dendl;

  if (cancel_op(id)) {
    return;
  }

  Context *on_start = nullptr;
  {
    Mutex::Locker locker(m_lock);

    m_inflight_ops.erase(id);

    if (m_inflight_ops.size() < m_max_concurrent_syncs && !m_queue.empty()) {
      auto pair = m_queue.front();
      m_inflight_ops.insert(pair.first);
      dout(20) << "ready to start sync for " << pair.first << " ["
               << m_inflight_ops.size() << "/" << m_max_concurrent_syncs << "]"
               << dendl;
      on_start= pair.second;
      m_queue.pop_front();
    }
  }

  if (on_start != nullptr) {
    on_start->complete(0);
  }
}

template <typename I>
void ImageSyncThrottler<I>::drain(int r) {
  dout(20) << dendl;

  std::list<std::pair<std::string, Context *>> queue;
  {
    Mutex::Locker locker(m_lock);
    std::swap(m_queue, queue);
    m_inflight_ops.clear();
  }

  for (auto &pair : queue) {
    pair.second->complete(r);
  }
}

template <typename I>
void ImageSyncThrottler<I>::set_max_concurrent_syncs(uint32_t max) {
  dout(20) << "max=" << max << dendl;

  std::list<Context *> ops;
  {
    Mutex::Locker locker(m_lock);
    m_max_concurrent_syncs = max;

    // Start waiting ops in the case of available free slots
    while ((m_max_concurrent_syncs == 0 ||
            m_inflight_ops.size() < m_max_concurrent_syncs) &&
           !m_queue.empty()) {
      auto pair = m_queue.front();
      m_inflight_ops.insert(pair.first);
      dout(20) << "ready to start sync for " << pair.first << " ["
               << m_inflight_ops.size() << "/" << m_max_concurrent_syncs << "]"
               << dendl;
      ops.push_back(pair.second);
      m_queue.pop_front();
    }
  }

  for (const auto& ctx : ops) {
    ctx->complete(0);
  }
}

template <typename I>
void ImageSyncThrottler<I>::print_status(Formatter *f, std::stringstream *ss) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  if (f) {
    f->dump_int("max_parallel_syncs", m_max_concurrent_syncs);
    f->dump_int("running_syncs", m_inflight_ops.size());
    f->dump_int("waiting_syncs", m_queue.size());
    f->flush(*ss);
  } else {
    *ss << "[ ";
    *ss << "max_parallel_syncs=" << m_max_concurrent_syncs << ", ";
    *ss << "running_syncs=" << m_inflight_ops.size() << ", ";
    *ss << "waiting_syncs=" << m_queue.size() << " ]";
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
void ImageSyncThrottler<I>::handle_conf_change(const struct md_config_t *conf,
                                      const set<string> &changed) {
  if (changed.count("rbd_mirror_concurrent_image_syncs")) {
    set_max_concurrent_syncs(conf->rbd_mirror_concurrent_image_syncs);
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageSyncThrottler<librbd::ImageCtx>;
