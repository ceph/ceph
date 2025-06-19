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

#include "Throttler.h"
#include "common/Formatter.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/Utils.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::Throttler:: " << this \
                           << " " << __func__ << ": "

namespace rbd {
namespace mirror {

template <typename I>
Throttler<I>::Throttler(CephContext *cct, const std::string &config_key)
  : m_cct(cct), m_config_key(config_key),
    m_lock(ceph::make_mutex(
      librbd::util::unique_lock_name("rbd::mirror::Throttler", this))),
    m_max_concurrent_ops(cct->_conf.get_val<uint64_t>(m_config_key)) {
  dout(20) << m_config_key << "=" << m_max_concurrent_ops << dendl;
  m_cct->_conf.add_observer(this);
}

template <typename I>
Throttler<I>::~Throttler() {
  m_cct->_conf.remove_observer(this);

  std::lock_guard locker{m_lock};
  ceph_assert(m_inflight_ops.empty());
  ceph_assert(m_queue.empty());
}

template <typename I>
void Throttler<I>::start_op(const std::string &ns,
                                     const std::string &id_,
                                     Context *on_start) {
  Id id{ns, id_};

  dout(20) << "id=" << id << dendl;

  int r = 0;
  {
    std::lock_guard locker{m_lock};

    if (m_inflight_ops.count(id) > 0) {
      dout(20) << "duplicate for already started op " << id << dendl;
    } else if (m_queued_ops.count(id) > 0) {
      dout(20) << "duplicate for already queued op " << id << dendl;
      std::swap(m_queued_ops[id], on_start);
      r = -ENOENT;
    } else if (m_max_concurrent_ops == 0 ||
               m_inflight_ops.size() < m_max_concurrent_ops) {
      ceph_assert(m_queue.empty());
      m_inflight_ops.insert(id);
      dout(20) << "ready to start op for " << id << " ["
               << m_inflight_ops.size() << "/" << m_max_concurrent_ops << "]"
               << dendl;
    } else {
      m_queue.push_back(id);
      std::swap(m_queued_ops[id], on_start);
      dout(20) << "op for " << id << " has been queued" << dendl;
    }
  }

  if (on_start != nullptr) {
    on_start->complete(r);
  }
}

template <typename I>
bool Throttler<I>::cancel_op(const std::string &ns,
                                      const std::string &id_) {
  Id id{ns, id_};

  dout(20) << "id=" << id << dendl;

  Context *on_start = nullptr;
  {
    std::lock_guard locker{m_lock};
    auto it = m_queued_ops.find(id);
    if (it != m_queued_ops.end()) {
      dout(20) << "canceled queued op for " << id << dendl;
      m_queue.remove(id);
      on_start = it->second;
      m_queued_ops.erase(it);
    }
  }

  if (on_start == nullptr) {
    return false;
  }

  on_start->complete(-ECANCELED);
  return true;
}

template <typename I>
void Throttler<I>::finish_op(const std::string &ns,
                                      const std::string &id_) {
  Id id{ns, id_};

  dout(20) << "id=" << id << dendl;

  if (cancel_op(ns, id_)) {
    return;
  }

  Context *on_start = nullptr;
  {
    std::lock_guard locker{m_lock};

    m_inflight_ops.erase(id);

    if (m_inflight_ops.size() < m_max_concurrent_ops && !m_queue.empty()) {
      auto id = m_queue.front();
      auto it = m_queued_ops.find(id);
      ceph_assert(it != m_queued_ops.end());
      m_inflight_ops.insert(id);
      dout(20) << "ready to start op for " << id << " ["
               << m_inflight_ops.size() << "/" << m_max_concurrent_ops << "]"
               << dendl;
      on_start = it->second;
      m_queued_ops.erase(it);
      m_queue.pop_front();
    }
  }

  if (on_start != nullptr) {
    on_start->complete(0);
  }
}

template <typename I>
void Throttler<I>::drain(const std::string &ns, int r) {
  dout(20) << "ns=" << ns << dendl;

  std::map<Id, Context *> queued_ops;
  {
    std::lock_guard locker{m_lock};
    for (auto it = m_queued_ops.begin(); it != m_queued_ops.end(); ) {
      if (it->first.first == ns) {
        queued_ops[it->first] = it->second;
        m_queue.remove(it->first);
        it = m_queued_ops.erase(it);
      } else {
        it++;
      }
    }
    for (auto it = m_inflight_ops.begin(); it != m_inflight_ops.end(); ) {
      if (it->first == ns) {
        dout(20) << "inflight_op " << *it << dendl;
        it = m_inflight_ops.erase(it);
      } else {
        it++;
      }
    }
  }

  for (auto &it : queued_ops) {
    dout(20) << "queued_op " << it.first << dendl;
    it.second->complete(r);
  }
}

template <typename I>
void Throttler<I>::set_max_concurrent_ops(uint32_t max) {
  dout(20) << "max=" << max << dendl;

  std::list<Context *> ops;
  {
    std::lock_guard locker{m_lock};
    m_max_concurrent_ops = max;

    // Start waiting ops in the case of available free slots
    while ((m_max_concurrent_ops == 0 ||
            m_inflight_ops.size() < m_max_concurrent_ops) &&
           !m_queue.empty()) {
      auto id = m_queue.front();
      m_inflight_ops.insert(id);
      dout(20) << "ready to start op for " << id << " ["
               << m_inflight_ops.size() << "/" << m_max_concurrent_ops << "]"
               << dendl;
      auto it = m_queued_ops.find(id);
      ceph_assert(it != m_queued_ops.end());
      ops.push_back(it->second);
      m_queued_ops.erase(it);
      m_queue.pop_front();
    }
  }

  for (const auto& ctx : ops) {
    ctx->complete(0);
  }
}

template <typename I>
void Throttler<I>::print_status(ceph::Formatter *f) {
  dout(20) << dendl;

  std::lock_guard locker{m_lock};

  f->dump_int("max_parallel_requests", m_max_concurrent_ops);
  f->dump_int("running_requests", m_inflight_ops.size());
  f->dump_int("waiting_requests", m_queue.size());
}

template <typename I>
void Throttler<I>::handle_conf_change(const ConfigProxy& conf,
                                      const std::set<std::string> &changed) {
  if (changed.count(m_config_key)) {
    set_max_concurrent_ops(conf.get_val<uint64_t>(m_config_key));
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::Throttler<librbd::ImageCtx>;
