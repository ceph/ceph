// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_INCLUDE_CEPHFS_METRICS_TYPES_H
#define CEPH_INCLUDE_CEPHFS_METRICS_TYPES_H

#include <string>
#include <boost/variant.hpp>

#include "common/Formatter.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "include/int_types.h"
#include "include/stringify.h"
#include "include/utime.h"

namespace ceph { class Formatter; }

enum ClientMetricType {
  CLIENT_METRIC_TYPE_CAP_INFO,
  CLIENT_METRIC_TYPE_READ_LATENCY,
  CLIENT_METRIC_TYPE_WRITE_LATENCY,
  CLIENT_METRIC_TYPE_METADATA_LATENCY,
};
inline std::ostream &operator<<(std::ostream &os, const ClientMetricType &type) {
  switch(type) {
  case ClientMetricType::CLIENT_METRIC_TYPE_CAP_INFO:
    os << "CAP_INFO";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_READ_LATENCY:
    os << "READ_LATENCY";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_WRITE_LATENCY:
    os << "WRITE_LATENCY";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_METADATA_LATENCY:
    os << "METADATA_LATENCY";
    break;
  default:
    ceph_abort();
  }

  return os;
}

struct CapInfoPayload {
  static const ClientMetricType METRIC_TYPE = ClientMetricType::CLIENT_METRIC_TYPE_CAP_INFO;

  uint64_t cap_hits = 0;
  uint64_t cap_misses = 0;
  uint64_t nr_caps = 0;

  CapInfoPayload() { }
  CapInfoPayload(uint64_t cap_hits, uint64_t cap_misses, uint64_t nr_caps)
    : cap_hits(cap_hits), cap_misses(cap_misses), nr_caps(nr_caps) {
  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(cap_hits, bl);
    encode(cap_misses, bl);
    encode(nr_caps, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(cap_hits, iter);
    decode(cap_misses, iter);
    decode(nr_caps, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("cap_hits", cap_hits);
    f->dump_int("cap_misses", cap_misses);
    f->dump_int("num_caps", nr_caps);
  }
};

struct ReadLatencyPayload {
  static const ClientMetricType METRIC_TYPE = ClientMetricType::CLIENT_METRIC_TYPE_READ_LATENCY;

  utime_t lat;

  ReadLatencyPayload() { }
  ReadLatencyPayload(utime_t lat)
    : lat(lat) {
  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(lat, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(lat, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("latency", lat);
  }
};

struct WriteLatencyPayload {
  static const ClientMetricType METRIC_TYPE = ClientMetricType::CLIENT_METRIC_TYPE_WRITE_LATENCY;

  utime_t lat;

  WriteLatencyPayload() { }
  WriteLatencyPayload(utime_t lat)
    : lat(lat) {
  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(lat, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(lat, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("latency", lat);
  }
};

struct MetadataLatencyPayload {
  static const ClientMetricType METRIC_TYPE = ClientMetricType::CLIENT_METRIC_TYPE_METADATA_LATENCY;

  utime_t lat;

  MetadataLatencyPayload() { }
  MetadataLatencyPayload(utime_t lat)
    : lat(lat) {
  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(lat, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(lat, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("latency", lat);
  }
};

struct UnknownPayload {
  static const ClientMetricType METRIC_TYPE = static_cast<ClientMetricType>(-1);

  UnknownPayload() { }

  void encode(bufferlist &bl) const {
  }

  void decode(bufferlist::const_iterator &iter) {
  }

  void dump(Formatter *f) const {
  }
};

typedef boost::variant<CapInfoPayload,
                       ReadLatencyPayload,
                       WriteLatencyPayload,
                       MetadataLatencyPayload,
                       UnknownPayload> ClientMetricPayload;

// metric update message sent by clients
struct ClientMetricMessage {
public:
  ClientMetricMessage(const ClientMetricPayload &payload = UnknownPayload())
    : payload(payload) {
  }

  class EncodePayloadVisitor : public boost::static_visitor<void> {
  public:
    explicit EncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {
    }

    template <typename ClientMetricPayload>
    inline void operator()(const ClientMetricPayload &payload) const {
      using ceph::encode;
      encode(static_cast<uint32_t>(ClientMetricPayload::METRIC_TYPE), m_bl);
      payload.encode(m_bl);
    }

  private:
    bufferlist &m_bl;
  };

  class DecodePayloadVisitor : public boost::static_visitor<void> {
  public:
    DecodePayloadVisitor(bufferlist::const_iterator &iter) : m_iter(iter) {
    }

    template <typename ClientMetricPayload>
    inline void operator()(ClientMetricPayload &payload) const {
      using ceph::decode;
      payload.decode(m_iter);
    }

  private:
    bufferlist::const_iterator &m_iter;
  };

  class DumpPayloadVisitor : public boost::static_visitor<void> {
  public:
    explicit DumpPayloadVisitor(Formatter *formatter) : m_formatter(formatter) {
    }

    template <typename ClientMetricPayload>
    inline void operator()(const ClientMetricPayload &payload) const {
      ClientMetricType metric_type = ClientMetricPayload::METRIC_TYPE;
      m_formatter->dump_string("client_metric_type", stringify(metric_type));
      payload.dump(m_formatter);
    }

  private:
    Formatter *m_formatter;
  };

  void encode(bufferlist &bl) const {
    boost::apply_visitor(EncodePayloadVisitor(bl), payload);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;

    uint32_t metric_type;
    decode(metric_type, iter);

    switch (metric_type) {
    case ClientMetricType::CLIENT_METRIC_TYPE_CAP_INFO:
      payload = CapInfoPayload();
      break;
    case ClientMetricType::CLIENT_METRIC_TYPE_READ_LATENCY:
      payload = ReadLatencyPayload();
      break;
    case ClientMetricType::CLIENT_METRIC_TYPE_WRITE_LATENCY:
      payload = WriteLatencyPayload();
      break;
    case ClientMetricType::CLIENT_METRIC_TYPE_METADATA_LATENCY:
      payload = MetadataLatencyPayload();
      break;
    default:
      payload = UnknownPayload();
      break;
    }

    boost::apply_visitor(DecodePayloadVisitor(iter), payload);
  }

  void dump(Formatter *f) const {
    apply_visitor(DumpPayloadVisitor(f), payload);
  }

  ClientMetricPayload payload;
};
WRITE_CLASS_ENCODER(ClientMetricMessage);

#endif // CEPH_INCLUDE_CEPHFS_METRICS_TYPES_H
