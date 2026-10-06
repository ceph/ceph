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
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <thread>

#include "include/utime.h"

#include "ChangeNotifyFormat.h"

class CephContext;
class CInode;
class CDentry;
class ConfigProxy;
namespace ceph { class Formatter; }
class LogEvent;
struct rd_kafka_s;
struct rd_kafka_topic_s;

/**
 * NotifyEndpoint - destination for accepted change events.
 *
 * send() must never block the caller: it either takes the event or returns
 * false, and the event is then counted as a drop by the notifier. The
 * notifier serializes all calls on its single drain thread.
 */
class NotifyEndpoint {
public:
  virtual ~NotifyEndpoint() = default;
  /// non-blocking; false means the event was not taken
  virtual bool send(const std::string &json) = 0;
  /// called on shutdown; must be bounded in time
  virtual void flush() {}
  /// let the endpoint make progress (e.g. serve delivery reports);
  /// called from the drain thread, never from the commit path
  virtual void poll(int timeout_ms) { (void)timeout_ms; }
  /// short name used in logs and `notify status`
  virtual std::string type() const = 0;
  /// endpoint-specific configuration, as a nested status object
  virtual void dump_status(ceph::Formatter *f) const = 0;
  /// last transport error, empty when there was none
  virtual std::string last_error() const { return {}; }
};

/// Debug/test endpoint: appends one JSON object per line to a file.
class FileEndpoint : public NotifyEndpoint {
public:
  FileEndpoint(CephContext *cct, const std::string &path);
  ~FileEndpoint() override;

  bool ok() const { return fd >= 0; }
  bool send(const std::string &json) override;
  std::string type() const override { return "file"; }
  void dump_status(ceph::Formatter *f) const override;
private:
  CephContext *const cct;
  int fd = -1;
  std::string path;
  std::mutex lock;
};

/// Kafka endpoint configuration, straight from the mds_notify_kafka_* options.
struct KafkaOptions {
  std::string brokers;
  std::string topic;
  uint32_t message_timeout_ms = 5000;
  uint64_t max_queue = 0;
};

/// Kafka endpoint: fire-and-forget librdkafka producer.
///
/// librdkafka retries and buffers internally (message.timeout.ms bounds the
/// retries); a full internal queue makes rd_kafka_produce() fail, which is
/// reported as a drop. Nothing here blocks the caller.
class KafkaEndpoint : public NotifyEndpoint {
public:
  KafkaEndpoint(CephContext *cct, const KafkaOptions &opts);
  ~KafkaEndpoint() override;

  bool ok() const { return producer != nullptr; }
  bool send(const std::string &json) override;
  void flush() override;
  std::string type() const override { return "kafka"; }
  void dump_status(ceph::Formatter *f) const override;
  std::string last_error() const override;

  /// serve librdkafka delivery reports / keep protocol state moving
  void poll(int timeout_ms) override;

private:
  void set_error(const std::string &err);

  CephContext *const cct;
  rd_kafka_s *producer = nullptr;
  rd_kafka_topic_s *topic = nullptr;
  std::string brokers;
  std::string topic_name;
  uint32_t message_timeout_ms = 5000;
  uint64_t max_queue = 0;
  mutable std::mutex err_lock;
  std::string err;
};

/**
 * ChangeNotifier - MDS-side change notification producer.
 *
 * Namespace ops are classified at Server::journal_and_reply() and client
 * write flushes at Locker::_do_cap_update(); accepted events are converted
 * to the OpenCloud (reva) posixfs watcher JSON contract and handed to a
 * NotifyEndpoint by a single drain thread.
 *
 * The commit path never blocks and never allocates unboundedly: events are
 * pushed onto a bounded in-memory queue (mds_notify_queue_size) with a
 * drop-the-newest policy, and drops are counted and reported by
 * `notify status`. Delivery is fire-and-forget: there are no journal
 * entries, no acks and no retries beyond what the endpoint does internally,
 * so the producer must document what can be lost.
 *
 * Configuration is via mds_notify_* options (see mds.yaml.in):
 *   - mds_notify_enable          master switch (runtime)
 *   - mds_notify_root            paths are emitted relative to this (runtime)
 *   - mds_notify_queue_size      bound on the in-memory queue (startup)
 *   - mds_notify_file            debug/test file endpoint (startup)
 *   - mds_notify_kafka_brokers   Kafka bootstrap servers (startup)
 *   - mds_notify_kafka_topic     Kafka topic (startup)
 *   - mds_notify_kafka_message_timeout  librdkafka message.timeout.ms (startup)
 *   - mds_notify_kafka_max_queue librdkafka queue.buffering.max.messages (startup)
 *
 * The file endpoint wins when both it and Kafka are configured; it exists
 * so tests can assert the emitted records without a broker.
 */
class ChangeNotifier {
public:
  ChangeNotifier(CephContext *cct);
  ~ChangeNotifier();

  ChangeNotifier(const ChangeNotifier &) = delete;
  ChangeNotifier &operator=(const ChangeNotifier &) = delete;

  bool enabled() const { return enabled_.load(std::memory_order_relaxed); }

  /// Namespace-op hook, called from Server::journal_and_reply().
  void journal_op(LogEvent *le, CInode *in, CDentry *dn);

  /// Cap-flush hook (write/truncate), called from Locker::_do_cap_update().
  void cap_update(CInode *in);

  /// Admin socket: `notify status`.
  void dump_status(ceph::Formatter *f) const;

  /// Admin socket: `notify enable` / `notify disable`.
  /// Returns false (with a message) when there is no usable endpoint.
  bool set_enabled(bool enable, std::ostream &err);

  /// React to runtime-changeable options (mds_notify_enable/_root).
  void handle_conf_change(const ConfigProxy &conf,
                          const std::set<std::string> &changed);

private:
  void configure_endpoint(const ConfigProxy &conf);
  void emit(uint32_t mask, const std::string &abs_path);
  void emit_move(uint32_t src_mask, const std::string &src_abs_path,
                 uint32_t dest_mask, const std::string &dest_abs_path);
  void submit(std::string json);
  void drain();
  void record_error(const std::string &err);

  /// Make an MDS-internal absolute path relative to the watch root.
  /// nullopt when the path is outside the root.
  std::optional<std::string> relative_path(std::string_view path) const;

  CephContext *const cct;

  /// watch root; runtime-changeable, guarded by root_lock
  mutable std::mutex root_lock;
  std::string root = "/";

  std::atomic<bool> enabled_{false};
  std::atomic<bool> stopping_{false};

  std::unique_ptr<NotifyEndpoint> endpoint;
  std::thread drain_thread;

  // bounded queue: drop-the-newest when full
  mutable std::mutex queue_lock;
  std::condition_variable queue_cv;
  std::deque<std::string> queue;
  size_t queue_cap = 8192;

  // counters (approximate by design: read for status, not for billing).
  std::atomic<uint64_t> n_queued{0};
  std::atomic<uint64_t> n_sent{0};
  std::atomic<uint64_t> n_dropped{0};
  mutable std::mutex err_lock;
  std::string last_error;
  utime_t last_error_at;
  utime_t last_drop_at;
};

#endif // CEPH_MDS_CHANGE_NOTIFIER_H
