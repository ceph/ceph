// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "msg/Message.h"

/// client -> Crimson OSD: request the OSD's reactor placement table.
/// Sent only after an MOSDOpReply from this OSD carried a nonzero
/// shard_map_version that differs from the client's cached one, so older
/// OSDs never receive it and no feature bit is needed.
class MOSDShardMapGet final : public Message {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  uint64_t have_version = 0;

  MOSDShardMapGet()
    : Message{CEPH_MSG_OSD_SHARD_MAP_GET, HEAD_VERSION, COMPAT_VERSION} {}
  explicit MOSDShardMapGet(uint64_t have)
    : Message{CEPH_MSG_OSD_SHARD_MAP_GET, HEAD_VERSION, COMPAT_VERSION},
      have_version(have) {}

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(have_version, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(have_version, p);
  }
  std::string_view get_type_name() const override {
    return "osd_shard_map_get";
  }
  void print(std::ostream &out) const override {
    out << "osd_shard_map_get(have v" << have_version << ")";
  }

private:
  ~MOSDShardMapGet() final {}
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
