// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#ifndef CEPH_MMGRCONFIGURE_H_
#define CEPH_MMGRCONFIGURE_H_

#include "msg/Message.h"
#include "mgr/MetricTypes.h"
#include "mgr/OSDPerfMetricTypes.h"

/**
 * This message is sent from ceph-mgr to MgrClient, instructing it
 * it about what data to send back to ceph-mgr at what frequency.
 */
class MMgrConfigure : public Message {
private:
  static constexpr int HEAD_VERSION = 4;
  static constexpr int COMPAT_VERSION = 1;

public:
  uint32_t stats_period = 0;

  // Default 0 means if unspecified will include all stats
  uint32_t stats_threshold = 0;

  std::map<OSDPerfMetricQuery, OSDPerfMetricLimits> osd_perf_metric_queries;

  boost::optional<MetricConfigMessage> metric_config_message;

  void decode_payload() override
  {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(stats_period, p);
    if (header.version >= 2) {
      decode(stats_threshold, p);
    }
    if (header.version >= 3) {
      decode(osd_perf_metric_queries, p);
    }
    if (header.version >= 4) {
      decode(metric_config_message, p);
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(stats_period, payload);
    encode(stats_threshold, payload);
    encode(osd_perf_metric_queries, payload);
    if (metric_config_message && metric_config_message->should_encode(features)) {
      encode(metric_config_message, payload);
    } else {
      boost::optional<MetricConfigMessage> empty;
      encode(empty, payload);
    }
  }

  std::string_view get_type_name() const override { return "mgrconfigure"; }
  void print(std::ostream& out) const override {
    out << get_type_name() << "(period=" << stats_period
			   << ", threshold=" << stats_threshold << ")";
  }

private:
  MMgrConfigure()
    : Message{MSG_MGR_CONFIGURE, HEAD_VERSION, COMPAT_VERSION}
  {}
  using RefCountedObject::put;
  using RefCountedObject::get;
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif

