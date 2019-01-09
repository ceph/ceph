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
#include "mgr/OSDPerfMetricTypes.h"

/**
 * This message is sent from ceph-mgr to MgrClient, instructing it
 * it about what data to send back to ceph-mgr at what frequency.
 */
class MMgrConfigure : public MessageInstance<MMgrConfigure> {
public:
  friend factory;
private:

  static constexpr int HEAD_VERSION = 3;
  static constexpr int COMPAT_VERSION = 1;

public:
  uint32_t stats_period = 0;

  // Default 0 means if unspecified will include all stats
  uint32_t stats_threshold = 0;

  std::map<OSDPerfMetricQuery, OSDPerfMetricLimits> osd_perf_metric_queries;

  void decode_payload() override
  {
    auto p = payload.cbegin();
    decode(stats_period, p);
    if (header.version >= 2) {
      decode(stats_threshold, p);
    }
    if (header.version >= 3) {
      decode(osd_perf_metric_queries, p);
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(stats_period, payload);
    encode(stats_threshold, payload);
    encode(osd_perf_metric_queries, payload);
  }

  std::string_view get_type_name() const override { return "mgrconfigure"; }
  void print(ostream& out) const override {
    out << get_type_name() << "(period=" << stats_period
                           << ", threshold=" << stats_threshold << ")";
  }

  MMgrConfigure()
    : MessageInstance(MSG_MGR_CONFIGURE, HEAD_VERSION, COMPAT_VERSION)
  {}
};

#endif

