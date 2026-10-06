// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Open Edge LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ChangeNotifier.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>

#include "common/Clock.h"
#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/compat.h"

#include "CDentry.h"
#include "CInode.h"
#include "LogEvent.h"
#include "events/EUpdate.h"

#define dout_context cct
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.change_notifier "

// The mask values, record helpers and op classification live in
// cephfs_notify (ChangeNotifyFormat.h).
using cephfs_notify::NOTIFY_CLOSE_WRITE;
using cephfs_notify::NOTIFY_MOVED_FROM;
using cephfs_notify::NOTIFY_MOVED_TO;
using cephfs_notify::NOTIFY_ONLYDIR;

// ---------------------------------------------------------------------------
// FileEndpoint
// ---------------------------------------------------------------------------

FileEndpoint::FileEndpoint(CephContext *cct, const std::string &path)
  : cct(cct), path(path)
{
  fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0)
    derr << "cannot open file endpoint " << path << ": " << cpp_strerror(errno)
         << dendl;
  else
    dout(1) << "file endpoint " << path << dendl;
}

FileEndpoint::~FileEndpoint()
{
  if (fd >= 0)
    ::close(fd);
}

bool FileEndpoint::send(const std::string &json)
{
  if (fd < 0)
    return false;
  std::string line = json;
  line += '\n';
  std::lock_guard l(lock);
  ssize_t r = ::write(fd, line.data(), line.size());
  if (r != static_cast<ssize_t>(line.size())) {
    derr << "file endpoint write failed: " << cpp_strerror(errno) << dendl;
    return false;
  }
  return true;
}

void FileEndpoint::dump_status(ceph::Formatter *f) const
{
  f->dump_string("path", path);
}

// ---------------------------------------------------------------------------
// ChangeNotifier
// ---------------------------------------------------------------------------

ChangeNotifier::ChangeNotifier(CephContext *cct)
  : cct(cct)
{
  if (!cct)
    return;
  configure_endpoint(cct->_conf);
}

ChangeNotifier::~ChangeNotifier()
{
  stopping_.store(true);
  queue_cv.notify_all();
  if (drain_thread.joinable())
    drain_thread.join();
  if (endpoint)
    endpoint->flush();
}

void ChangeNotifier::configure_endpoint(const ConfigProxy &conf)
{
  std::string file = conf.get_val<std::string>("mds_notify_file");
  bool want = conf.get_val<bool>("mds_notify_enable");

  queue_cap = conf.get_val<Option::size_t>("mds_notify_queue_size");
  if (queue_cap == 0)
    queue_cap = 1;
  {
    std::lock_guard l(root_lock);
    root = conf.get_val<std::string>("mds_notify_root");
    while (root.size() > 1 && root.back() == '/')
      root.pop_back();
  }

  if (!file.empty()) {
    auto ep = std::make_unique<FileEndpoint>(cct, file);
    if (ep->ok())
      endpoint = std::move(ep);
  }

  if (!endpoint) {
    enabled_.store(false);
    if (want || !file.empty())
      derr << "no usable notification endpoint; change notifications stay off"
           << dendl;
    return;
  }

  enabled_.store(want);
  drain_thread = std::thread(&ChangeNotifier::drain, this);

  std::string root_copy;
  {
    std::lock_guard l(root_lock);
    root_copy = root;
  }
  dout(0) << "endpoint " << endpoint->type() << " root "
          << root_copy << " queue capacity " << queue_cap
          << (want ? " (enabled)" : " (disabled)") << dendl;
}

void ChangeNotifier::submit(std::string json)
{
  if (!enabled())
    return;
  bool dropped = false;
  {
    std::lock_guard l(queue_lock);
    if (queue.size() >= queue_cap) {
      // drop-the-newest: keep the queue bounded, never block the commit path
      dropped = true;
    } else {
      queue.push_back(std::move(json));
      n_queued.fetch_add(1);
    }
  }
  if (dropped) {
    n_dropped.fetch_add(1);
    std::lock_guard l(err_lock);
    last_drop_at = ceph_clock_now();
  } else {
    queue_cv.notify_one();
  }
}

void ChangeNotifier::drain()
{
  ceph_pthread_setname("mds_notify");
  std::deque<std::string> batch;
  while (!stopping_.load()) {
    {
      std::unique_lock l(queue_lock);
      queue_cv.wait_for(l, std::chrono::milliseconds(50), [this] {
        return stopping_.load() || !queue.empty();
      });
      batch.swap(queue);
    }
    for (auto &rec : batch) {
      if (endpoint && endpoint->send(rec)) {
        n_sent.fetch_add(1);
      } else {
        n_dropped.fetch_add(1);
        {
          std::lock_guard l(err_lock);
          last_drop_at = ceph_clock_now();
        }
        record_error(endpoint ? endpoint->last_error()
                              : "no endpoint configured");
      }
    }
    batch.clear();
    if (endpoint)
      endpoint->poll(0);
  }
  // final drain on shutdown (best effort)
  {
    std::lock_guard l(queue_lock);
    batch.swap(queue);
  }
  for (auto &rec : batch) {
    if (endpoint && endpoint->send(rec))
      n_sent.fetch_add(1);
  }
  if (endpoint)
    endpoint->poll(0);
}

void ChangeNotifier::record_error(const std::string &err)
{
  if (err.empty())
    return;
  std::lock_guard l(err_lock);
  if (last_error != err)
    dout(1) << "endpoint error: " << err << dendl;
  last_error = err;
  last_error_at = ceph_clock_now();
}

bool ChangeNotifier::set_enabled(bool enable, std::ostream &err)
{
  if (enable && !endpoint) {
    err << "no notification endpoint is configured (set mds_notify_file; "
           "endpoint options take effect on daemon restart)";
    return false;
  }
  bool was = enabled_.exchange(enable);
  if (was != enable)
    dout(0) << (enable ? "enabled" : "disabled") << " by admin request" << dendl;
  return true;
}

void ChangeNotifier::handle_conf_change(const ConfigProxy &conf,
                                        const std::set<std::string> &changed)
{
  if (changed.count("mds_notify_enable")) {
    bool want = conf.get_val<bool>("mds_notify_enable");
    if (want && !endpoint) {
      dout(0) << "mds_notify_enable is set but no endpoint is configured; "
                 "staying disabled" << dendl;
      enabled_.store(false);
    } else {
      bool was = enabled_.exchange(want);
      if (was != want)
        dout(0) << (want ? "enabled" : "disabled") << " by configuration"
                << dendl;
    }
  }
  if (changed.count("mds_notify_root")) {
    std::string r = conf.get_val<std::string>("mds_notify_root");
    while (r.size() > 1 && r.back() == '/')
      r.pop_back();
    std::lock_guard l(root_lock);
    root = r;
    dout(0) << "watch root is now " << root << dendl;
  }
}

void ChangeNotifier::dump_status(ceph::Formatter *f) const
{
  f->open_object_section("change_notifier");
  f->dump_bool("enabled", enabled());
  {
    std::lock_guard l(root_lock);
    f->dump_string("root", root);
  }
  f->dump_string("endpoint", endpoint ? endpoint->type() : "none");
  if (endpoint) {
    f->open_object_section("endpoint_config");
    endpoint->dump_status(f);
    f->close_section();
  }
  f->dump_unsigned("queue_capacity", queue_cap);
  {
    std::lock_guard l(queue_lock);
    f->dump_unsigned("queue_depth", queue.size());
  }
  f->dump_unsigned("queued", n_queued.load());
  f->dump_unsigned("sent", n_sent.load());
  f->dump_unsigned("dropped", n_dropped.load());
  {
    std::lock_guard l(err_lock);
    f->dump_string("last_error", last_error);
    f->dump_stream("last_error_at") << last_error_at;
    f->dump_stream("last_drop_at") << last_drop_at;
  }
  f->close_section();
}

std::optional<std::string> ChangeNotifier::relative_path(std::string_view path) const
{
  std::lock_guard l(root_lock);
  return cephfs_notify::relative_path(root, path);
}

void ChangeNotifier::emit(uint32_t mask, const std::string &abs_path)
{
  auto rel = relative_path(abs_path);
  if (!rel)
    return;
  submit(cephfs_notify::event_record(mask, *rel));
}

void ChangeNotifier::emit_move(uint32_t src_mask, const std::string &src_abs_path,
                               uint32_t dest_mask, const std::string &dest_abs_path)
{
  auto src_rel = relative_path(src_abs_path);
  auto dest_rel = relative_path(dest_abs_path);

  if (src_rel && dest_rel) {
    // both ends inside the root: one move message carrying both halves
    submit(cephfs_notify::move_record(src_mask, *src_rel, dest_mask, *dest_rel));
  } else if (dest_rel) {
    // moved in from outside the root: emit the in-root half only. The
    // consumer treats a MOVED_TO without a source as a move of the
    // destination (scan + assimilate), which is what has to happen here.
    submit(cephfs_notify::move_in_record(dest_mask, *dest_rel));
  } else if (src_rel) {
    // moved out of the root: emit the in-root half. The consumer has no
    // MOVED_FROM-only action (it ignores one), so the closest action for
    // "this path is no longer in the tree" is DELETE, keeping the ONLYDIR
    // bit from the source mask.
    submit(cephfs_notify::move_out_record(src_mask, *src_rel));
  }
  // both ends outside the root: nothing to report
}

void ChangeNotifier::journal_op(LogEvent *le, CInode *in, CDentry *dn)
{
  if (!enabled() || le->get_type() != EVENT_UPDATE)
    return;
  EUpdate *eu = static_cast<EUpdate *>(le);
  const std::string &op = eu->type;
  dout(10) << "journal_op " << op << " in " << in << " dn " << dn << dendl;

  if (op == "rename") {
    // call site is journal_and_reply(mdr, srci, destdn, ...); the source
    // dentry is still the inode's actual parent (the move is applied later,
    // from the finish callback), so it carries the source path.
    if (!in || !dn) {
      dout(1) << "rename: missing in/dn, not emitted" << dendl;
      return;
    }
    const CDentry *srcdn = in->get_parent_dn();
    if (!srcdn) {
      // The inode's create may still be committing when the rename is
      // journaled ("touch; mv" pipelines the two): the actual parent is not
      // linked yet, but the create pushed the source dentry onto the inode's
      // projected parent stack, where it is the oldest entry.
      srcdn = in->get_oldest_parent_dn();
      dout(10) << "rename: source via oldest projected parent" << dendl;
    }
    if (!srcdn) {
      dout(1) << "rename: no source dentry, not emitted" << dendl;
      return;
    }
    std::string sp, dp;
    srcdn->make_path_string(sp, true);
    dn->make_path_string(dp, true);
    dout(10) << "rename emit " << sp << " -> " << dp << dendl;
    uint32_t onlydir = in->is_dir() ? NOTIFY_ONLYDIR : 0;
    emit_move(NOTIFY_MOVED_FROM | onlydir, sp, NOTIFY_MOVED_TO | onlydir, dp);
    return;
  }

  // The single-path operations: the affected dentry is `dn`. link_local and
  // link_remote differ in where the *target inode* is authoritative
  // (link_local: here, link_remote: another rank); in both cases dn is the
  // new dentry, so the path is the new name. unlink_remote unlinks a dentry
  // whose inode is authoritative on another rank; it is never a directory
  // (hardlinks to directories are rejected).
  if (!dn)
    return;

  bool is_dir = false;
  if (op == "unlink_local") {
    // for a directory unlink the metablob records the dir inode; for a file
    // the actual dentry linkage is still populated at journal time
    is_dir = eu->metablob.renamed_dirino != 0;
    if (!is_dir) {
      CDentry::linkage_t *dnl = dn->get_linkage();
      is_dir = dnl && dnl->get_inode() && dnl->get_inode()->is_dir();
    }
  }
  uint32_t mask =
    cephfs_notify::op_mask(cephfs_notify::classify_op(op, is_dir));
  if (mask == 0) {
    dout(10) << "journal_op " << op << ": no event" << dendl;
    return;
  }
  std::string p;
  dn->make_path_string(p, true);
  dout(10) << "journal_op emit " << p << dendl;
  emit(mask, p);
}

void ChangeNotifier::cap_update(CInode *in)
{
  if (!enabled() || !in)
    return;
  if (in->is_dir() || in->last != CEPH_NOSNAP)
    return;
  const CDentry *dn = in->get_projected_parent_dn();
  if (!dn)
    return;
  if (dn->get_dir()->inode->is_stray()) {
    // unlinked inodes flush from the stray dir; the path is not
    // user-visible and DELETE was already emitted for it
    dout(10) << "cap_update: skipping stray-dir flush" << dendl;
    return;
  }
  std::string p;
  dn->make_path_string(p, true);
  dout(10) << "cap_update emit " << p << dendl;
  emit(NOTIFY_CLOSE_WRITE, p);
}
