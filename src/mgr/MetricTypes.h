// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MGR_METRIC_TYPES_H
#define CEPH_MGR_METRIC_TYPES_H

#include <boost/variant.hpp>
#include "include/denc.h"
#include "mgr/OSDPerfMetricTypes.h"
#include "mgr/MDSPerfMetricTypes.h"

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
};

struct UnknownMetricPayload {
  static const MetricReportType METRIC_REPORT_TYPE = static_cast<MetricReportType>(-1);

  UnknownMetricPayload() { }

  DENC(UnknownMetricPayload, v, p) {
    ceph_abort();
  }
};

WRITE_CLASS_DENC(OSDMetricPayload)
WRITE_CLASS_DENC(MDSMetricPayload)
WRITE_CLASS_DENC(UnknownMetricPayload)

typedef boost::variant<OSDMetricPayload,
                       MDSMetricPayload,
                       UnknownMetricPayload> MetricPayload;

class EncodeMetricPayloadVisitor : public boost::static_visitor<void> {
public:
  explicit EncodeMetricPayloadVisitor(ceph::buffer::list &bl) : m_bl(bl) {
  }

  template <typename MetricPayload>
  inline void operator()(const MetricPayload &payload) const {
    using ceph::encode;
    encode(static_cast<uint32_t>(MetricPayload::METRIC_REPORT_TYPE), m_bl);
    encode(payload, m_bl);
  }

private:
  ceph::buffer::list &m_bl;
};

class DecodeMetricPayloadVisitor : public boost::static_visitor<void> {
public:
  DecodeMetricPayloadVisitor(ceph::buffer::list::const_iterator &iter) : m_iter(iter) {
  }

  template <typename MetricPayload>
  inline void operator()(MetricPayload &payload) const {
    using ceph::decode;
    decode(payload, m_iter);
  }

private:
  ceph::buffer::list::const_iterator &m_iter;
};

struct MetricReportMessage {
  MetricPayload payload;

  MetricReportMessage(const MetricPayload &payload = UnknownMetricPayload())
    : payload(payload) {
  }

  void encode(ceph::buffer::list &bl) const {
    boost::apply_visitor(EncodeMetricPayloadVisitor(bl), payload);
  }

  void decode(ceph::buffer::list::const_iterator &iter) {
    using ceph::decode;

    uint32_t metric_report_type;
    decode(metric_report_type, iter);

    switch (static_cast<MetricReportType>(metric_report_type)) {
    case MetricReportType::METRIC_REPORT_TYPE_OSD:
      payload = OSDMetricPayload();
      break;
    case MetricReportType::METRIC_REPORT_TYPE_MDS:
      payload = MDSMetricPayload();
      break;
    default:
      payload = UnknownMetricPayload();
      break;
  }

  boost::apply_visitor(DecodeMetricPayloadVisitor(iter), payload);
  }
};

WRITE_CLASS_ENCODER(MetricReportMessage);

// variant for sending configure message to mgr clients

enum MetricConfigType {
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

typedef boost::variant<OSDConfigPayload,
                       MDSConfigPayload,
                       UnknownConfigPayload> ConfigPayload;

class EncodeConfigPayloadVisitor : public boost::static_visitor<void> {
public:
  explicit EncodeConfigPayloadVisitor(ceph::buffer::list &bl) : m_bl(bl) {
  }

  template <typename ConfigPayload>
  inline void operator()(const ConfigPayload &payload) const {
    using ceph::encode;
    encode(static_cast<uint32_t>(ConfigPayload::METRIC_CONFIG_TYPE), m_bl);
    encode(payload, m_bl);
  }

private:
  ceph::buffer::list &m_bl;
};

class DecodeConfigPayloadVisitor : public boost::static_visitor<void> {
public:
  DecodeConfigPayloadVisitor(ceph::buffer::list::const_iterator &iter) : m_iter(iter) {
  }

  template <typename ConfigPayload>
  inline void operator()(ConfigPayload &payload) const {
    using ceph::decode;
    decode(payload, m_iter);
  }

private:
  ceph::buffer::list::const_iterator &m_iter;
};

struct MetricConfigMessage {
  ConfigPayload payload;

  MetricConfigMessage(const ConfigPayload &payload = UnknownConfigPayload())
    : payload(payload) {
  }

  void encode(ceph::buffer::list &bl) const {
    boost::apply_visitor(EncodeConfigPayloadVisitor(bl), payload);
  }

  void decode(ceph::buffer::list::const_iterator &iter) {
    using ceph::decode;

    uint32_t metric_config_type;
    decode(metric_config_type, iter);

    switch (metric_config_type) {
    case MetricConfigType::METRIC_CONFIG_TYPE_OSD:
      payload = OSDConfigPayload();
      break;
    case MetricConfigType::METRIC_CONFIG_TYPE_MDS:
      payload = MDSConfigPayload();
      break;
    default:
      payload = UnknownConfigPayload();
      break;
  }

  boost::apply_visitor(DecodeConfigPayloadVisitor(iter), payload);
  }
};

WRITE_CLASS_ENCODER(MetricConfigMessage);

#endif // CEPH_MGR_METRIC_TYPES_H
