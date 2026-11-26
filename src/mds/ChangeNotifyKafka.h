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

#ifndef CEPH_MDS_CHANGE_NOTIFY_KAFKA_H
#define CEPH_MDS_CHANGE_NOTIFY_KAFKA_H

// Kafka endpoint for change notification; built with WITH_MDS_KAFKA_ENDPOINT.

#include <atomic>
#include <cstdint>
#include <string>

#include "common/ceph_mutex.h"

#include "ChangeNotifier.h"

struct rd_kafka_s;
struct rd_kafka_topic_s;

/// Fire-and-forget idempotent librdkafka producer. rd_kafka_produce() never
/// blocks; a full librdkafka queue is a refused send, and a message that
/// librdkafka gives up on is counted from its delivery report.
class KafkaEndpoint : public NotifyEndpoint {
public:
  KafkaEndpoint(CephContext *cct, const KafkaOptions &opts);
  ~KafkaEndpoint() override;

  bool ok() const { return producer != nullptr; }
  bool send(const std::string &json) override;
  void flush(int timeout_ms) override;
  void poll(int timeout_ms) override;
  std::string type() const override { return "kafka"; }
  void dump_status(ceph::Formatter *f) const override;
  std::string last_error() const override;
  uint64_t dropped_delivery() const override {
    return n_dropped_delivery.load();
  }

  // librdkafka callbacks (dr_msg_cb, error_cb, log_cb)
  void on_delivery_report(int err);
  void on_error(const char *reason) { set_error(reason); }
  void on_log(int level, const char *fac, const char *buf) const;

private:
  void set_error(const std::string &e);
  bool read_secret(const std::string &path, std::string &out);

  CephContext *const cct;
  const KafkaOptions opts;
  const std::string partition_key;
  rd_kafka_s *producer = nullptr;
  rd_kafka_topic_s *topic = nullptr;
  std::atomic<uint64_t> n_delivered{0};
  std::atomic<uint64_t> n_dropped_delivery{0};
  mutable ceph::mutex err_lock = ceph::make_mutex("KafkaEndpoint::err_lock");
  std::string err;
};

#endif // CEPH_MDS_CHANGE_NOTIFY_KAFKA_H
