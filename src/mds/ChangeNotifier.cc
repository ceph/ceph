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
#include <unistd.h>

#include <deque>
#include <string>

#include "acconfig.h"
#include "common/Clock.h"
#include "common/Formatter.h"
#include "common/Thread.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"

#include "CDentry.h"
#include "CDir.h"
#include "CInode.h"
#include "LogEvent.h"
#include "events/EUpdate.h"

#define dout_context cct
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.change_notifier "

using cephfs_notify::NOTIFY_CLOSE_WRITE;
using cephfs_notify::NOTIFY_DELETE;
using cephfs_notify::NOTIFY_MOVED_FROM;
using cephfs_notify::NOTIFY_MOVED_TO;
using cephfs_notify::NOTIFY_ONLYDIR;
using cephfs_notify::RenameKind;

namespace {

/// The dentry is in the MDS-internal tree (~mdsN and its stray directories).
bool is_internal_dentry(const CDentry *dn)
{
  const CInode *diri = dn->get_dir()->get_inode();
  return diri->is_stray() || diri->is_mdsdir();
}

} // namespace

FileEndpoint::FileEndpoint(CephContext *cct, const std::string &path)
  : cct(cct), path(path)
{
  fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) {
    set_error("cannot open file endpoint " + path + ": " + cpp_strerror(errno));
    derr << last_error() << dendl;
  }
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
  std::string line = json + '\n';
  ssize_t r = ::write(fd, line.data(), line.size());
  if (r != static_cast<ssize_t>(line.size())) {
    set_error(std::string("file endpoint write failed: ") +
              (r < 0 ? cpp_strerror(errno) : "short write"));
    return false;
  }
  return true;
}

void FileEndpoint::dump_status(ceph::Formatter *f) const
{
  f->dump_string("path", path);
}

void FileEndpoint::set_error(const std::string &e)
{
  std::lock_guard l(err_lock);
  err = e;
}

std::string FileEndpoint::last_error() const
{
  std::lock_guard l(err_lock);
  return err;
}

std::string notify_stream_key(const std::string &fs_name, int rank)
{
  return fs_name + ".mds." + std::to_string(rank);
}

ChangeNotifier::ChangeNotifier(CephContext *cct, int rank, std::string fs_name)
  : cct(cct), rank(rank), fs_name(std::move(fs_name))
{
  configure(cct->_conf);
}

ChangeNotifier::~ChangeNotifier()
{
  shutdown();
}

void ChangeNotifier::configure(const ConfigProxy &conf)
{
  queue.set_capacity(conf.get_val<uint64_t>("mds_notify_queue_size"));
  root = conf.get_val<std::string>("mds_notify_root");
  while (root.size() > 1 && root.back() == '/')
    root.pop_back();

  const auto file = conf.get_val<std::string>("mds_notify_file");
  KafkaOptions kafka;
  kafka.brokers = conf.get_val<std::string>("mds_notify_kafka_brokers");
  kafka.topic = conf.get_val<std::string>("mds_notify_kafka_topic");
  if (!file.empty()) {
    // the file endpoint is for tests and wins when both are set
    auto ep = std::make_unique<FileEndpoint>(cct, file);
    if (ep->ok())
      endpoint = std::move(ep);
    else
      record_error(ep->last_error());
  } else if (!kafka.brokers.empty() && !kafka.topic.empty()) {
#ifdef WITH_MDS_KAFKA_ENDPOINT
    kafka.message_timeout_ms =
      conf.get_val<uint64_t>("mds_notify_kafka_message_timeout");
    kafka.max_queue = conf.get_val<uint64_t>("mds_notify_kafka_max_queue");
    kafka.security_protocol =
      conf.get_val<std::string>("mds_notify_kafka_security_protocol");
    kafka.ssl_ca_location =
      conf.get_val<std::string>("mds_notify_kafka_ssl_ca_location");
    kafka.ssl_certificate_location =
      conf.get_val<std::string>("mds_notify_kafka_ssl_certificate_location");
    kafka.ssl_key_location =
      conf.get_val<std::string>("mds_notify_kafka_ssl_key_location");
    kafka.ssl_key_password_file =
      conf.get_val<std::string>("mds_notify_kafka_ssl_key_password_file");
    kafka.ssl_verify = conf.get_val<bool>("mds_notify_kafka_ssl_verify");
    kafka.sasl_mechanism =
      conf.get_val<std::string>("mds_notify_kafka_sasl_mechanism");
    kafka.sasl_username =
      conf.get_val<std::string>("mds_notify_kafka_sasl_username");
    kafka.sasl_password_file =
      conf.get_val<std::string>("mds_notify_kafka_sasl_password_file");
    kafka.rank = rank;
    kafka.fs_name = fs_name;
    std::string err;
    endpoint = make_kafka_endpoint(cct, kafka, &err);
    if (!endpoint)
      record_error(err);
#else
    record_error("mds_notify_kafka_brokers is set, but this ceph-mds is "
                 "built without the Kafka endpoint (WITH_MDS_KAFKA_ENDPOINT)");
#endif
  }

  const bool want = conf.get_val<bool>("mds_notify_enable");
  if (!endpoint) {
    if (want || !file.empty() || !kafka.brokers.empty()) {
      std::lock_guard l(err_lock);
      derr << "no usable change notification endpoint (" << last_error
           << "); notification stays off" << dendl;
    }
    return;
  }
  drain_thread = make_named_thread("mds_notify", &ChangeNotifier::drain, this);
  enabled_.store(want);
  dout(1) << "rank " << rank << " endpoint " << endpoint->type() << " root "
          << root << (want ? " (enabled)" : " (disabled)") << dendl;
}

void ChangeNotifier::shutdown(int flush_ms)
{
  if (stopping_.exchange(true))
    return;
  // the drain thread hands what is still queued to the endpoint and exits
  queue_cv.notify_all();
  if (drain_thread.joinable())
    drain_thread.join();
  if (endpoint)
    endpoint->flush(flush_ms);
  dout(1) << "rank " << rank << " shut down: sent " << n_sent.load()
          << ", dropped at the endpoint " << n_dropped_endpoint.load() << dendl;
}

void ChangeNotifier::set_epoch(uint64_t epoch)
{
  std::lock_guard l(queue_lock);
  if (epoch != queue.epoch())
    dout(1) << "rank " << rank << " epoch " << epoch << dendl;
  queue.set_epoch(epoch);
}

void ChangeNotifier::submit(std::string json)
{
  if (!enabled() || stopping_.load(std::memory_order_relaxed))
    return;
  bool accepted, wake;
  {
    // numbered before the capacity check: a dropped event leaves a gap
    std::lock_guard l(queue_lock);
    accepted = queue.push(std::move(json));
    wake = accepted && queue.size() == queue.capacity() / 2 + 1;
  }
  if (!accepted) {
    std::lock_guard l(err_lock);
    last_drop_at = ceph_clock_now();
  } else if (wake) {
    queue_cv.notify_one();
  }
}

void ChangeNotifier::deliver(cephfs_notify::QueuedEvent &ev)
{
  cephfs_notify::append_position(ev.record, rank, ev.epoch, ev.seq);
  if (endpoint->send(ev.record)) {
    n_sent.fetch_add(1);
    return;
  }
  n_dropped_endpoint.fetch_add(1);
  {
    std::lock_guard l(err_lock);
    last_drop_at = ceph_clock_now();
  }
  record_error(endpoint->last_error());
}

void ChangeNotifier::drain()
{
  std::deque<cephfs_notify::QueuedEvent> batch;
  bool done = false;
  while (!done) {
    {
      std::unique_lock l(queue_lock);
      queue_cv.wait_for(l, drain_period, [this] {
        return stopping_.load() || queue.size() > queue.capacity() / 2;
      });
      done = stopping_.load();
      queue.take_all(batch);
    }
    for (auto &ev : batch)
      deliver(ev);
    batch.clear();
    endpoint->poll(0);
    // errors the endpoint reports asynchronously
    std::string e = endpoint->last_error();
    if (e != seen_endpoint_error) {
      seen_endpoint_error = e;
      record_error(e);
    }
  }
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

void ChangeNotifier::handle_conf_change(const ConfigProxy &conf,
                                        const std::set<std::string> &changed)
{
  if (changed.count("mds_notify_enable")) {
    const bool want = conf.get_val<bool>("mds_notify_enable");
    if (want && !endpoint)
      dout(0) << "mds_notify_enable is set but no endpoint is usable" << dendl;
    else if (enabled_.exchange(want) != want)
      dout(1) << (want ? "enabled" : "disabled") << dendl;
  }
  if (changed.count("mds_notify_root")) {
    std::string r = conf.get_val<std::string>("mds_notify_root");
    while (r.size() > 1 && r.back() == '/')
      r.pop_back();
    std::lock_guard l(root_lock);
    root = r;
    dout(1) << "watch root is now " << root << dendl;
  }
}

void ChangeNotifier::dump_status(ceph::Formatter *f) const
{
  f->open_object_section("change_notifier");
  f->dump_string("fs_name", fs_name);
  f->dump_int("rank", rank);
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
  uint64_t queued, dropped_queue;
  {
    std::lock_guard l(queue_lock);
    f->dump_unsigned("queue_capacity", queue.capacity());
    f->dump_unsigned("queue_depth", queue.size());
    f->dump_unsigned("epoch", queue.epoch());
    f->dump_unsigned("next_seq", queue.next_seq());
    queued = queue.accepted();
    dropped_queue = queue.dropped();
  }
  const uint64_t dropped_delivery = endpoint ? endpoint->dropped_delivery() : 0;
  f->dump_unsigned("queued", queued);
  f->dump_unsigned("sent", n_sent.load());
  f->dump_unsigned("dropped_queue", dropped_queue);
  f->dump_unsigned("dropped_endpoint", n_dropped_endpoint.load());
  f->dump_unsigned("dropped_delivery", dropped_delivery);
  f->dump_unsigned("dropped", dropped_queue + n_dropped_endpoint.load() +
                              dropped_delivery);
  f->dump_unsigned("invalid_utf8", n_invalid_utf8.load());
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
  if (!cephfs_notify::is_valid_utf8(*rel))
    n_invalid_utf8.fetch_add(1, std::memory_order_relaxed);
  submit(cephfs_notify::event_record(mask, *rel));
}

void ChangeNotifier::emit_move(uint32_t src_mask, const std::string &src_abs_path,
                               uint32_t dest_mask, const std::string &dest_abs_path)
{
  auto src_rel = relative_path(src_abs_path);
  auto dest_rel = relative_path(dest_abs_path);
  // count the event once if either reported end is not UTF-8
  if ((src_rel && !cephfs_notify::is_valid_utf8(*src_rel)) ||
      (dest_rel && !cephfs_notify::is_valid_utf8(*dest_rel)))
    n_invalid_utf8.fetch_add(1, std::memory_order_relaxed);

  if (src_rel && dest_rel)
    submit(cephfs_notify::move_record(src_mask, *src_rel, dest_mask, *dest_rel));
  else if (dest_rel)   // moved into the watch root
    submit(cephfs_notify::move_in_record(dest_mask, *dest_rel));
  else if (src_rel)    // moved out of the watch root: a DELETE
    submit(cephfs_notify::move_out_record(src_mask, *src_rel));
}

void ChangeNotifier::journal_op(LogEvent *le, CInode *in, CDentry *dn,
                                const OpHint &hint)
{
  if (!enabled() || le->get_type() != EVENT_UPDATE)
    return;
  EUpdate *eu = static_cast<EUpdate *>(le);
  const std::string &op = eu->type;
  dout(10) << "journal_op " << op << " in " << in << " dn " << dn << dendl;

  if (op == "rename") {
    // journal_and_reply(mdr, srci, destdn, ...): the source is the dentry
    // the request named, not the inode's primary dentry (a hard link may
    // have been renamed, and the primary may not be linked yet)
    const CDentry *srcdn = hint.srcdn;
    if (!in || !dn || !srcdn) {
      dout(1) << "rename: missing in/dn/srcdn, not emitted" << dendl;
      return;
    }
    RenameKind kind = cephfs_notify::classify_rename(is_internal_dentry(srcdn),
                                                     is_internal_dentry(dn));
    if (kind == RenameKind::none) {
      dout(10) << "rename: internal source " << *srcdn << ", not emitted"
               << dendl;
      return;
    }
    uint32_t onlydir = in->is_dir() ? NOTIFY_ONLYDIR : 0;
    std::string sp, dp;
    srcdn->make_path_string(sp, true);
    if (kind == RenameKind::source_gone) {
      dout(10) << "rename emit (source gone) " << sp << dendl;
      emit(NOTIFY_DELETE | onlydir, sp);
      return;
    }
    dn->make_path_string(dp, true);
    dout(10) << "rename emit " << sp << " -> " << dp << dendl;
    emit_move(NOTIFY_MOVED_FROM | onlydir, sp, NOTIFY_MOVED_TO | onlydir, dp);
    return;
  }

  // a truncate (size-changing setattr, open with O_TRUNC) is a content
  // change; setattr has no dentry, so use the inode's primary dentry
  if (cephfs_notify::classify_op(op, false, hint.size_change) ==
      cephfs_notify::OpKind::content) {
    if (!in || in->is_dir() || in->last != CEPH_NOSNAP)
      return;
    const CDentry *pdn = dn ? dn : in->get_projected_parent_dn();
    if (!pdn || is_internal_dentry(pdn))
      return;
    std::string p;
    pdn->make_path_string(p, true);
    dout(10) << "journal_op " << op << " (truncate) emit " << p << dendl;
    emit(NOTIFY_CLOSE_WRITE, p);
    return;
  }

  // single-path operations: `dn` is the affected dentry
  if (!dn)
    return;

  bool is_dir = false;
  if (op == "unlink_local") {
    // a directory unlink records the dir inode in the metablob
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

void ChangeNotifier::cap_update(CInode *in, const CDentry *link_dn)
{
  if (!enabled() || !in)
    return;
  if (in->is_dir() || in->last != CEPH_NOSNAP)
    return;
  const CDentry *dn = in->get_projected_parent_dn();
  if (!dn)
    return;
  if (is_internal_dentry(dn)) {
    // an unlinked inode flushing from a stray directory: report it under a
    // hard link that still names it, otherwise it is gone (DELETE was sent)
    if (!link_dn || is_internal_dentry(link_dn)) {
      dout(10) << "cap_update: skipping stray-dir flush" << dendl;
      return;
    }
    dn = link_dn;
  }
  std::string p;
  dn->make_path_string(p, true);
  dout(10) << "cap_update emit " << p << dendl;
  emit(NOTIFY_CLOSE_WRITE, p);
}
