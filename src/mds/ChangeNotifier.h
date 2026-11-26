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

#ifndef CEPH_MDS_CHANGE_NOTIFIER_H
#define CEPH_MDS_CHANGE_NOTIFIER_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>

#include "common/ceph_mutex.h"
#include "include/utime.h"

#include "ChangeNotifyFormat.h"
#include "ChangeNotifyQueue.h"

class CephContext;
class CInode;
class CDentry;
class ConfigProxy;
namespace ceph { class Formatter; }
class LogEvent;

/**
 * NotifyEndpoint - destination for change events.
 *
 * All calls come from the notifier's drain thread. send() takes the record
 * or returns false (counted as a drop); it must not block for long.
 */
class NotifyEndpoint {
public:
  virtual ~NotifyEndpoint() = default;
  virtual bool send(const std::string &json) = 0;
  /// called on shutdown; returns within about `timeout_ms`
  virtual void flush(int timeout_ms) { (void)timeout_ms; }
  /// let the endpoint make progress (delivery reports)
  virtual void poll(int timeout_ms) { (void)timeout_ms; }
  virtual std::string type() const = 0;
  virtual void dump_status(ceph::Formatter *f) const = 0;
  virtual std::string last_error() const { return {}; }
  /// events accepted by send() and lost afterwards
  virtual uint64_t dropped_delivery() const { return 0; }
};

/// Appends one JSON object per line to a regular file on a local file
/// system. Meant for tests and debugging.
class FileEndpoint : public NotifyEndpoint {
public:
  FileEndpoint(CephContext *cct, const std::string &path);
  ~FileEndpoint() override;

  bool ok() const { return fd >= 0; }
  bool send(const std::string &json) override;
  std::string type() const override { return "file"; }
  void dump_status(ceph::Formatter *f) const override;
  std::string last_error() const override;
private:
  void set_error(const std::string &e);

  CephContext *const cct;
  int fd = -1;
  std::string path;
  mutable ceph::mutex err_lock = ceph::make_mutex("FileEndpoint::err_lock");
  std::string err;
};

/// mds_notify_kafka_* options. Secrets are named by file path and read when
/// the endpoint is built; they are never stored, logged or reported.
struct KafkaOptions {
  std::string brokers;
  std::string topic;
  uint32_t message_timeout_ms = 5000;
  uint64_t max_queue = 0;
  std::string security_protocol;
  std::string ssl_ca_location;
  std::string ssl_certificate_location;
  std::string ssl_key_location;
  std::string ssl_key_password_file;
  bool ssl_verify = true;
  std::string sasl_mechanism;
  std::string sasl_username;
  std::string sasl_password_file;
  /// message key: notify_stream_key(fs_name, rank)
  int rank = -1;
  std::string fs_name;
};

/// Kafka message key of a rank's stream: "<fs_name>.mds.<rank>".
std::string notify_stream_key(const std::string &fs_name, int rank);

/// Defined in ChangeNotifyKafka.cc, built with WITH_MDS_KAFKA_ENDPOINT.
/// Returns nullptr and sets `err` if the producer cannot be created.
std::unique_ptr<NotifyEndpoint> make_kafka_endpoint(CephContext *cct,
                                                    const KafkaOptions &opts,
                                                    std::string *err);

/**
 * ChangeNotifier - MDS-side change notification producer, one per MDSRank.
 *
 * Namespace operations are classified in Server::journal_and_reply() and
 * client data flushes in Locker::_do_cap_update(). The hooks build a record
 * and push it onto a bounded queue (mds_notify_queue_size, drop-the-newest);
 * a drain thread hands records to the endpoint every drain_period. Nothing
 * on the metadata path waits for the endpoint.
 *
 * Every record carries the rank, its epoch (the MDSMap incarnation) and a
 * sequence number assigned before any drop decision, so a consumer can see
 * gaps. Events are produced when an update is submitted to the journal,
 * before it is durable.
 *
 * The endpoint and the drain thread exist only when an endpoint is
 * configured; mds_notify_enable switches event production at runtime.
 */
class ChangeNotifier {
public:
  ChangeNotifier(CephContext *cct, int rank, std::string fs_name);
  ~ChangeNotifier();

  ChangeNotifier(const ChangeNotifier &) = delete;
  ChangeNotifier &operator=(const ChangeNotifier &) = delete;

  bool enabled() const { return enabled_.load(std::memory_order_relaxed); }

  /// What the request knows that the journal entry does not.
  struct OpHint {
    /// rename: the source dentry named by the request (for a hard link this
    /// is not the inode's primary dentry)
    const CDentry *srcdn = nullptr;
    /// setattr: the mask has CEPH_SETATTR_SIZE
    bool size_change = false;
  };

  /// Called from Server::journal_and_reply().
  void journal_op(LogEvent *le, CInode *in, CDentry *dn, const OpHint &hint);

  /// Called from Locker::_do_cap_update() for a flush of dirty file data
  /// state. `link_dn`, if set, is a hard link that names the inode while its
  /// primary dentry is in a stray directory.
  void cap_update(CInode *in, const CDentry *link_dn = nullptr);

  /// The rank's incarnation; a new epoch restarts the sequence at 1.
  void set_epoch(uint64_t epoch);

  /// Stop accepting events, drain the queue and flush the endpoint for at
  /// most `flush_ms`. Idempotent. Not reached on respawn or crash.
  void shutdown(int flush_ms = 500);

  /// `notify status`
  void dump_status(ceph::Formatter *f) const;

  /// mds_notify_enable, mds_notify_root
  void handle_conf_change(const ConfigProxy &conf,
                          const std::set<std::string> &changed);

private:
  void configure(const ConfigProxy &conf);
  void emit(uint32_t mask, const std::string &abs_path);
  void emit_move(uint32_t src_mask, const std::string &src_abs_path,
                 uint32_t dest_mask, const std::string &dest_abs_path);
  void submit(std::string json);
  void drain();
  void deliver(cephfs_notify::QueuedEvent &ev);
  void record_error(const std::string &err);
  std::optional<std::string> relative_path(std::string_view path) const;

  CephContext *const cct;
  const int rank;
  const std::string fs_name;

  mutable ceph::mutex root_lock = ceph::make_mutex("ChangeNotifier::root_lock");
  std::string root = "/";

  std::atomic<bool> enabled_{false};
  std::atomic<bool> stopping_{false};

  /// set at construction, never changed afterwards
  std::unique_ptr<NotifyEndpoint> endpoint;
  std::thread drain_thread;
  std::string seen_endpoint_error;  // drain thread only

  /// The drain thread is not woken per event (a futex wake under mds_lock
  /// per event costs more than building the record); it runs on this
  /// period, and early when the queue is half full.
  static constexpr std::chrono::milliseconds drain_period{10};

  mutable ceph::mutex queue_lock = ceph::make_mutex("ChangeNotifier::queue_lock");
  ceph::condition_variable queue_cv;
  cephfs_notify::PositionedQueue queue{8192};

  std::atomic<uint64_t> n_sent{0};
  std::atomic<uint64_t> n_dropped_endpoint{0};
  std::atomic<uint64_t> n_invalid_utf8{0};
  mutable ceph::mutex err_lock = ceph::make_mutex("ChangeNotifier::err_lock");
  std::string last_error;
  utime_t last_error_at;
  utime_t last_drop_at;
};

#endif // CEPH_MDS_CHANGE_NOTIFIER_H
