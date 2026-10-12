// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_MGR_METRIC_TYPES_H
#define CEPH_MGR_METRIC_TYPES_H

#include <list>
#include <map>
#include <variant>

#include "include/denc.h"
#include "include/ceph_features.h"
#include "mgr/OSDPerfMetricTypes.h"
#include "mgr/MDSPerfMetricTypes.h"

namespace ceph { class Formatter; }

enum class MetricReportType {
  METRIC_REPORT_TYPE_OSD = 0,
  METRIC_REPORT_TYPE_MDS = 1,
};

struct OSDMetricPayload {
  static const MetricReportType METRIC_REPORT_TYPE = MetricReportType::METRIC_REPORT_TYPE_OSD;
  std::map<OSDPerfMetricQuery, OSDPerfMetricReport> report;

  OSDMetricPayload() {
  }
  OSDMetricPayload(const std::map<OSDPerfMetricQuery, OSDPerfMetricReport> &report)
    : report(report) {
  }

  DENC(OSDMetricPayload, v, p) {
    DENC_START(1, 1, p);
    denc(v.report, p);
    DENC_FINISH(p);
  }

  void dump(ceph::Formatter *f) const;
  static std::list<OSDMetricPayload> generate_test_instances();
};

struct MDSMetricPayload {
  static const MetricReportType METRIC_REPORT_TYPE = MetricReportType::METRIC_REPORT_TYPE_MDS;
  MDSPerfMetricReport metric_report;

  MDSMetricPayload() {
  }
  MDSMetricPayload(const MDSPerfMetricReport &metric_report)
    : metric_report(metric_report) {
  }

  DENC(MDSMetricPayload, v, p) {
    DENC_START(1, 1, p);
    denc(v.metric_report, p);
    DENC_FINISH(p);
  }
  void dump(ceph::Formatter *f) const {
    metric_report.dump(f);
  }
  static std::list<MDSMetricPayload> generate_test_instances();
};

struct UnknownMetricPayload {
  static const MetricReportType METRIC_REPORT_TYPE = static_cast<MetricReportType>(-1);

  UnknownMetricPayload() { }

  DENC(UnknownMetricPayload, v, p) {
    ceph_abort();
  }

  void dump(ceph::Formatter *f) const {
    ceph_abort();
  }
};

WRITE_CLASS_DENC(OSDMetricPayload)
WRITE_CLASS_DENC(MDSMetricPayload)
WRITE_CLASS_DENC(UnknownMetricPayload)

typedef std::variant<OSDMetricPayload,
		     MDSMetricPayload,
		     UnknownMetricPayload> MetricPayload;

struct MetricReportMessage {
  MetricPayload payload;

  MetricReportMessage(const MetricPayload &payload = UnknownMetricPayload())
    : payload(payload) {
  }

  bool should_encode(uint64_t features) const {
    if (!HAVE_FEATURE(features, SERVER_PACIFIC) &&
	std::get_if<MDSMetricPayload>(&payload)) {
      return false;
    }
    return true;
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &iter);

  void dump(ceph::Formatter *f) const;
  static std::list<MetricReportMessage> generate_test_instances();
};

WRITE_CLASS_ENCODER(MetricReportMessage);

// variant for sending configure message to mgr clients

enum MetricConfigType : uint32_t {
  METRIC_CONFIG_TYPE_OSD = 0,
  METRIC_CONFIG_TYPE_MDS = 1,
};

struct OSDConfigPayload {
  static const MetricConfigType METRIC_CONFIG_TYPE = MetricConfigType::METRIC_CONFIG_TYPE_OSD;
  std::map<OSDPerfMetricQuery, OSDPerfMetricLimits> config;

  OSDConfigPayload() {
  }
  OSDConfigPayload(const std::map<OSDPerfMetricQuery, OSDPerfMetricLimits> &config)
    : config(config) {
  }

  DENC(OSDConfigPayload, v, p) {
    DENC_START(1, 1, p);
    denc(v.config, p);
    DENC_FINISH(p);
  }
};

struct MDSConfigPayload {
  static const MetricConfigType METRIC_CONFIG_TYPE = MetricConfigType::METRIC_CONFIG_TYPE_MDS;
  std::map<MDSPerfMetricQuery, MDSPerfMetricLimits> config;

  MDSConfigPayload() {
  }
  MDSConfigPayload(const std::map<MDSPerfMetricQuery, MDSPerfMetricLimits> &config)
    : config(config) {
  }

  DENC(MDSConfigPayload, v, p) {
    DENC_START(1, 1, p);
    denc(v.config, p);
    DENC_FINISH(p);
  }

  void dump(ceph::Formatter *f) const;
  static std::list<MDSConfigPayload> generate_test_instances();
};

struct UnknownConfigPayload {
  static const MetricConfigType METRIC_CONFIG_TYPE = static_cast<MetricConfigType>(-1);

  UnknownConfigPayload() { }

  DENC(UnknownConfigPayload, v, p) {
    ceph_abort();
  }
};

WRITE_CLASS_DENC(OSDConfigPayload)
WRITE_CLASS_DENC(MDSConfigPayload)
WRITE_CLASS_DENC(UnknownConfigPayload)

typedef std::variant<OSDConfigPayload,
		     MDSConfigPayload,
		     UnknownConfigPayload> ConfigPayload;

struct MetricConfigMessage {
  ConfigPayload payload;

  MetricConfigMessage(const ConfigPayload &payload = UnknownConfigPayload())
    : payload(payload) {
  }

  bool should_encode(uint64_t features) const {
    if (!HAVE_FEATURE(features, SERVER_PACIFIC) &&
	std::get_if<MDSConfigPayload>(&payload)) {
      return false;
    }
    return true;
  }

  void encode(ceph::buffer::list &bl) const;
  void decode(ceph::buffer::list::const_iterator &iter);
};

WRITE_CLASS_ENCODER(MetricConfigMessage);

#endif // CEPH_MGR_METRIC_TYPES_H
