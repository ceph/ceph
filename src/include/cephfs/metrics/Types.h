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
  CLIENT_METRIC_TYPE_DENTRY_LEASE,
  CLIENT_METRIC_TYPE_OPENED_FILES,
  CLIENT_METRIC_TYPE_PINNED_ICAPS,
  CLIENT_METRIC_TYPE_OPENED_INODES,
  CLIENT_METRIC_TYPE_READ_IO_SIZES,
  CLIENT_METRIC_TYPE_WRITE_IO_SIZES,
  CLIENT_METRIC_TYPE_AVG_READ_LATENCY,
  CLIENT_METRIC_TYPE_STDEV_READ_LATENCY,
  CLIENT_METRIC_TYPE_AVG_WRITE_LATENCY,
  CLIENT_METRIC_TYPE_STDEV_WRITE_LATENCY,
  CLIENT_METRIC_TYPE_AVG_METADATA_LATENCY,
  CLIENT_METRIC_TYPE_STDEV_METADATA_LATENCY,
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
  case ClientMetricType::CLIENT_METRIC_TYPE_DENTRY_LEASE:
    os << "DENTRY_LEASE";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_OPENED_FILES:
    os << "OPENED_FILES";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_PINNED_ICAPS:
    os << "PINNED_ICAPS";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_OPENED_INODES:
    os << "OPENED_INODES";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_READ_IO_SIZES:
    os << "READ_IO_SIZES";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_WRITE_IO_SIZES:
    os << "WRITE_IO_SIZES";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_AVG_READ_LATENCY:
    os << "AVG_READ_LATENCY";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_STDEV_READ_LATENCY:
    os << "STDEV_READ_LATENCY";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_AVG_WRITE_LATENCY:
    os << "AVG_WRITE_LATENCY";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_STDEV_WRITE_LATENCY:
    os << "STDEV_WRITE_LATENCY";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_AVG_METADATA_LATENCY:
    os << "AVG_METADATA_LATENCY";
    break;
  case ClientMetricType::CLIENT_METRIC_TYPE_STDEV_METADATA_LATENCY:
    os << "STDEV_METADATA_LATENCY";
    break;
  default:
    os << "(UNKNOWN:" << static_cast<std::underlying_type<ClientMetricType>::type>(type) << ")";
    break;
  }

  return os;
}

struct ClientMetricPayloadBase {
  ClientMetricPayloadBase(ClientMetricType type) : metric_type(type) {}

  ClientMetricType get_type() const {
    return metric_type;
  }

  void print_type(ostream *out) const {
    *out << metric_type;
  }

  private:
    ClientMetricType metric_type;
};

struct CapInfoPayload : public ClientMetricPayloadBase {
  uint64_t cap_hits = 0;
  uint64_t cap_misses = 0;
  uint64_t nr_caps = 0;

  CapInfoPayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_CAP_INFO) { }
  CapInfoPayload(uint64_t cap_hits, uint64_t cap_misses, uint64_t nr_caps)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_CAP_INFO),
    cap_hits(cap_hits), cap_misses(cap_misses), nr_caps(nr_caps) {
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

  void print(ostream *out) const {
    *out << "cap_hits: " << cap_hits << " "
	 << "cap_misses: " << cap_misses << " "
	 << "num_caps: " << nr_caps;
  }
};

struct ReadLatencyPayload : public ClientMetricPayloadBase {
  utime_t lat;
  utime_t mean;
  uint64_t sq_sum;  // sum of squares
  uint64_t count;   // IO count

  ReadLatencyPayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_READ_LATENCY) { }
  ReadLatencyPayload(utime_t lat, utime_t mean, uint64_t sq_sum, uint64_t count)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_READ_LATENCY),
      lat(lat),
      mean(mean),
      sq_sum(sq_sum),
      count(count) {
  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(2, 1, bl);
    encode(lat, bl);
    encode(mean, bl);
    encode(sq_sum, bl);
    encode(count, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(2, iter);
    decode(lat, iter);
    if (struct_v >= 2) {
      decode(mean, iter);
      decode(sq_sum, iter);
      decode(count, iter);
    }
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("latency", lat);
    f->dump_int("avg_latency", mean);
    f->dump_unsigned("sq_sum", sq_sum);
    f->dump_unsigned("count", count);
  }

  void print(ostream *out) const {
    *out << "latency: " << lat << ", avg_latency: " << mean
         << ", sq_sum: " << sq_sum << ", count=" << count;
  }
};

struct WriteLatencyPayload : public ClientMetricPayloadBase {
  utime_t lat;
  utime_t mean;
  uint64_t sq_sum;  // sum of squares
  uint64_t count;   // IO count

  WriteLatencyPayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_WRITE_LATENCY) { }
  WriteLatencyPayload(utime_t lat, utime_t mean, uint64_t sq_sum, uint64_t count)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_WRITE_LATENCY),
      lat(lat),
      mean(mean),
      sq_sum(sq_sum),
      count(count){
  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(2, 1, bl);
    encode(lat, bl);
    encode(mean, bl);
    encode(sq_sum, bl);
    encode(count, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(2, iter);
    decode(lat, iter);
    if (struct_v >= 2) {
      decode(mean, iter);
      decode(sq_sum, iter);
      decode(count, iter);
    }
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("latency", lat);
    f->dump_int("avg_latency", mean);
    f->dump_unsigned("sq_sum", sq_sum);
    f->dump_unsigned("count", count);
  }

  void print(ostream *out) const {
    *out << "latency: " << lat << ", avg_latency: " << mean
         << ", sq_sum: " << sq_sum << ", count=" << count;
  }
};

struct MetadataLatencyPayload : public ClientMetricPayloadBase {
  utime_t lat;
  utime_t mean;
  uint64_t sq_sum;  // sum of squares
  uint64_t count;   // IO count

  MetadataLatencyPayload()
  : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_METADATA_LATENCY) { }
  MetadataLatencyPayload(utime_t lat, utime_t mean, uint64_t sq_sum, uint64_t count)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_METADATA_LATENCY),
      lat(lat),
      mean(mean),
      sq_sum(sq_sum),
      count(count) {
  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(2, 1, bl);
    encode(lat, bl);
    encode(mean, bl);
    encode(sq_sum, bl);
    encode(count, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(2, iter);
    decode(lat, iter);
    if (struct_v >= 2) {
      decode(mean, iter);
      decode(sq_sum, iter);
      decode(count, iter);
    }
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("latency", lat);
    f->dump_int("avg_latency", mean);
    f->dump_unsigned("sq_sum", sq_sum);
    f->dump_unsigned("count", count);
  }

  void print(ostream *out) const {
    *out << "latency: " << lat << ", avg_latency: " << mean
         << ", sq_sum: " << sq_sum << ", count=" << count;
  }
};

struct DentryLeasePayload : public ClientMetricPayloadBase {
  uint64_t dlease_hits = 0;
  uint64_t dlease_misses = 0;
  uint64_t nr_dentries = 0;

  DentryLeasePayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_DENTRY_LEASE) { }
  DentryLeasePayload(uint64_t dlease_hits, uint64_t dlease_misses, uint64_t nr_dentries)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_DENTRY_LEASE),
    dlease_hits(dlease_hits), dlease_misses(dlease_misses), nr_dentries(nr_dentries) { }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(dlease_hits, bl);
    encode(dlease_misses, bl);
    encode(nr_dentries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(dlease_hits, iter);
    decode(dlease_misses, iter);
    decode(nr_dentries, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("dlease_hits", dlease_hits);
    f->dump_int("dlease_misses", dlease_misses);
    f->dump_int("num_dentries", nr_dentries);
  }

  void print(ostream *out) const {
    *out << "dlease_hits: " << dlease_hits << " "
	 << "dlease_misses: " << dlease_misses << " "
	 << "num_dentries: " << nr_dentries;
  }
};

struct OpenedFilesPayload : public ClientMetricPayloadBase {
  uint64_t opened_files = 0;
  uint64_t total_inodes = 0;

  OpenedFilesPayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_OPENED_FILES) { }
  OpenedFilesPayload(uint64_t opened_files, uint64_t total_inodes)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_OPENED_FILES),
    opened_files(opened_files), total_inodes(total_inodes) { }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(opened_files, bl);
    encode(total_inodes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(opened_files, iter);
    decode(total_inodes, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("opened_files", opened_files);
    f->dump_int("total_inodes", total_inodes);
  }

  void print(ostream *out) const {
    *out << "opened_files: " << opened_files << " "
	 << "total_inodes: " << total_inodes;
  }
};

struct PinnedIcapsPayload : public ClientMetricPayloadBase {
  uint64_t pinned_icaps = 0;
  uint64_t total_inodes = 0;

  PinnedIcapsPayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_PINNED_ICAPS) { }
  PinnedIcapsPayload(uint64_t pinned_icaps, uint64_t total_inodes)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_PINNED_ICAPS),
    pinned_icaps(pinned_icaps), total_inodes(total_inodes) { }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(pinned_icaps, bl);
    encode(total_inodes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(pinned_icaps, iter);
    decode(total_inodes, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("pinned_icaps", pinned_icaps);
    f->dump_int("total_inodes", total_inodes);
  }

  void print(ostream *out) const {
    *out << "pinned_icaps: " << pinned_icaps << " "
	 << "total_inodes: " << total_inodes;
  }
};

struct OpenedInodesPayload : public ClientMetricPayloadBase {
  uint64_t opened_inodes = 0;
  uint64_t total_inodes = 0;

  OpenedInodesPayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_OPENED_INODES) { }
  OpenedInodesPayload(uint64_t opened_inodes, uint64_t total_inodes)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_OPENED_INODES),
    opened_inodes(opened_inodes), total_inodes(total_inodes) { }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(opened_inodes, bl);
    encode(total_inodes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(opened_inodes, iter);
    decode(total_inodes, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("opened_inodes", opened_inodes);
    f->dump_int("total_inodes", total_inodes);
  }

  void print(ostream *out) const {
    *out << "opened_inodes: " << opened_inodes << " "
	 << "total_inodes: " << total_inodes;
  }
};

struct ReadIoSizesPayload : public ClientMetricPayloadBase {
  uint64_t total_ops = 0;
  uint64_t total_size = 0;

  ReadIoSizesPayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_READ_IO_SIZES) { }
  ReadIoSizesPayload(uint64_t total_ops, uint64_t total_size)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_READ_IO_SIZES),
    total_ops(total_ops), total_size(total_size) {  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(total_ops, bl);
    encode(total_size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(total_ops, iter);
    decode(total_size, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("total_ops", total_ops);
    f->dump_int("total_size", total_size);
  }

  void print(std::ostream *out) const {
    *out << "total_ops: " << total_ops << " total_size: " << total_size;
  }
};

struct WriteIoSizesPayload : public ClientMetricPayloadBase {
  uint64_t total_ops = 0;
  uint64_t total_size = 0;

  WriteIoSizesPayload()
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_WRITE_IO_SIZES) { }
  WriteIoSizesPayload(uint64_t total_ops, uint64_t total_size)
    : ClientMetricPayloadBase(ClientMetricType::CLIENT_METRIC_TYPE_WRITE_IO_SIZES),
    total_ops(total_ops), total_size(total_size) {
  }

  void encode(bufferlist &bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    encode(total_ops, bl);
    encode(total_size, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(1, iter);
    decode(total_ops, iter);
    decode(total_size, iter);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
    f->dump_int("total_ops", total_ops);
    f->dump_int("total_size", total_size);
  }

  void print(std::ostream *out) const {
    *out << "total_ops: " << total_ops << " total_size: " << total_size;
  }
};

struct UnknownPayload : public ClientMetricPayloadBase {
  UnknownPayload()
    : ClientMetricPayloadBase(static_cast<ClientMetricType>(-1)) { }
  UnknownPayload(ClientMetricType metric_type)
    : ClientMetricPayloadBase(metric_type) { }

  void encode(bufferlist &bl) const {
  }

  void decode(bufferlist::const_iterator &iter) {
    using ceph::decode;
    DECODE_START(254, iter);
    iter.seek(struct_len);
    DECODE_FINISH(iter);
  }

  void dump(Formatter *f) const {
  }

  void print(ostream *out) const {
  }
};

typedef boost::variant<CapInfoPayload,
                       ReadLatencyPayload,
                       WriteLatencyPayload,
                       MetadataLatencyPayload,
                       DentryLeasePayload,
                       OpenedFilesPayload,
                       PinnedIcapsPayload,
                       OpenedInodesPayload,
                       ReadIoSizesPayload,
                       WriteIoSizesPayload,
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
      encode(static_cast<uint32_t>(payload.get_type()), m_bl);
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
      m_formatter->dump_string("client_metric_type", stringify(payload.get_type()));
      payload.dump(m_formatter);
    }

  private:
    Formatter *m_formatter;
  };

  class PrintPayloadVisitor : public boost::static_visitor<void> {
  public:
    explicit PrintPayloadVisitor(ostream *out) : _out(out) {
    }

    template <typename ClientMetricPayload>
    inline void operator()(const ClientMetricPayload &payload) const {
      *_out << "[client_metric_type: ";
      payload.print_type(_out);
      *_out << " ";
      payload.print(_out);
      *_out << "]";
    }

  private:
    ostream *_out;
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
    case ClientMetricType::CLIENT_METRIC_TYPE_DENTRY_LEASE:
      payload = DentryLeasePayload();
      break;
    case ClientMetricType::CLIENT_METRIC_TYPE_OPENED_FILES:
      payload = OpenedFilesPayload();
      break;
    case ClientMetricType::CLIENT_METRIC_TYPE_PINNED_ICAPS:
      payload = PinnedIcapsPayload();
      break;
    case ClientMetricType::CLIENT_METRIC_TYPE_OPENED_INODES:
      payload = OpenedInodesPayload();
      break;
    case ClientMetricType::CLIENT_METRIC_TYPE_READ_IO_SIZES:
      payload = ReadIoSizesPayload();
      break;
    case ClientMetricType::CLIENT_METRIC_TYPE_WRITE_IO_SIZES:
      payload = WriteIoSizesPayload();
      break;
    default:
      payload = UnknownPayload(static_cast<ClientMetricType>(metric_type));
      break;
    }

    boost::apply_visitor(DecodePayloadVisitor(iter), payload);
  }

  void dump(Formatter *f) const {
    apply_visitor(DumpPayloadVisitor(f), payload);
  }

  void print(ostream *out) const {
    apply_visitor(PrintPayloadVisitor(out), payload);
  }

  ClientMetricPayload payload;
};
WRITE_CLASS_ENCODER(ClientMetricMessage);

#endif // CEPH_INCLUDE_CEPHFS_METRICS_TYPES_H
