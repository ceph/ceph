// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "MetricTypes.h"

#include <boost/variant/static_visitor.hpp>

#include "common/Formatter.h"

void OSDMetricPayload::dump(ceph::Formatter *f) const {
  f->open_array_section("report");
  for (auto& i : report) {
    f->open_object_section("query");
    i.first.dump(f);
    f->close_section();
    f->open_object_section("report");
    i.second.dump(f);
    f->close_section();
  }
  f->close_section();
}

std::list<OSDMetricPayload> OSDMetricPayload::generate_test_instances() {
  std::list<OSDMetricPayload> ls;
  ls.push_back(OSDMetricPayload());
  return ls;
}

std::list<MDSMetricPayload> MDSMetricPayload::generate_test_instances() {
  std::list<MDSMetricPayload> ls;
  ls.push_back(MDSMetricPayload());
  return ls;
}

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

void MetricReportMessage::encode(ceph::buffer::list &bl) const {
  std::visit(EncodeMetricPayloadVisitor(bl), payload);
}

void MetricReportMessage::decode(ceph::buffer::list::const_iterator &iter) {
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

  std::visit(DecodeMetricPayloadVisitor(iter), payload);
}

void MetricReportMessage::dump(ceph::Formatter *f) const {
  f->open_object_section("payload");
  if (const OSDMetricPayload* osdPayload = std::get_if<OSDMetricPayload>(&payload)) {
    osdPayload->dump(f);
  } else if (const MDSMetricPayload* mdsPayload = std::get_if<MDSMetricPayload>(&payload)) {
    mdsPayload->dump(f);
  } else if (const UnknownMetricPayload* unknownPayload = std::get_if<UnknownMetricPayload>(&payload)) {
    unknownPayload->dump(f);
  } else {
    ceph_abort();
  }
  f->close_section();
}

std::list<MetricReportMessage> MetricReportMessage::generate_test_instances() {
  std::list<MetricReportMessage> ls;
  ls.push_back(MetricReportMessage(OSDMetricPayload()));
  ls.push_back(MetricReportMessage(MDSMetricPayload()));
  return ls;
}

void MDSConfigPayload::dump(ceph::Formatter *f) const {
  f->open_object_section("config");
  for (auto& i : config) {
    f->dump_object("query", i.first);
    f->open_object_section("limits");
    for (auto& j : i.second) {
      f->dump_object("limit", j);
    }
    f->close_section();
  }
  f->close_section();
}

std::list<MDSConfigPayload> MDSConfigPayload::generate_test_instances() {
  std::list<MDSConfigPayload> ls;
  ls.emplace_back();
  return ls;
}

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

void MetricConfigMessage::encode(ceph::buffer::list &bl) const {
  std::visit(EncodeConfigPayloadVisitor(bl), payload);
}

void MetricConfigMessage::decode(ceph::buffer::list::const_iterator &iter) {
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

  std::visit(DecodeConfigPayloadVisitor(iter), payload);
}
