// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "msg/Message.h"

class MKVData : public Message {
public:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;

  /**
   * A removed range of keys, as a half-open interval [begin, end).
   *
   * An empty 'end' means the range extends past the last key. Keys are
   * ordered, so a recipient holding them in an ordered container can apply
   * this directly instead of being told each removed key individually.
   */
  struct range_delete_t {
    std::string begin;
    std::string end;

    range_delete_t() = default;
    range_delete_t(const std::string& b, const std::string& e)
      : begin(b), end(e) {}

    void encode(ceph::buffer::list& bl) const {
      using ceph::encode;
      ENCODE_START(1, 1, bl);
      encode(begin, bl);
      encode(end, bl);
      ENCODE_FINISH(bl);
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      using ceph::decode;
      DECODE_START(1, bl);
      decode(begin, bl);
      decode(end, bl);
      DECODE_FINISH(bl);
    }
  };

  version_t version;
  std::string prefix;
  bool incremental = false;

  // use transparent comparator so we can lookup in it by std::string_view keys
  std::map<std::string,std::optional<bufferlist>,std::less<>> data;

  /// ranges removed by this update; only sent when incremental
  std::vector<range_delete_t> range_deletes;

  MKVData() : Message{MSG_KV_DATA, HEAD_VERSION, COMPAT_VERSION} { }

  std::string_view get_type_name() const override {
    return "kv_data";
  }
  void print(std::ostream& o) const override {
    o << "kv_data(v" << version
      << " prefix " << prefix << ", "
      << (incremental ? "incremental, " : "full, ")
      << data.size() << " keys";
    if (!range_deletes.empty()) {
      o << ", " << range_deletes.size() << " ranges";
    }
    o << ")";
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(version, p);
    decode(prefix, p);
    decode(incremental, p);
    decode(data, p);
    if (header.version >= 2) {
      decode(range_deletes, p);
    }
  }

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    // A peer that predates range removals would silently ignore the extra
    // field and keep stale keys, so only send it to one that understands it.
    // Callers must arrange an alternative for the rest.
    bool with_ranges = HAVE_FEATURE(features, SERVER_UMBRELLA);
    header.version = with_ranges ? HEAD_VERSION : 1;
    encode(version, payload);
    encode(prefix, payload);
    encode(incremental, payload);
    encode(data, payload);
    if (with_ranges) {
      encode(range_deletes, payload);
    }
  }
};
WRITE_CLASS_ENCODER(MKVData::range_delete_t)
