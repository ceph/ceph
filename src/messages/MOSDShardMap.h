// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "msg/Message.h"
#include "osd/osd_shard_map.h"

/// Crimson OSD -> client: the full reactor placement table.
class MOSDShardMap final : public Message {
public:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

  osd_shard_map_t shard_map;

  MOSDShardMap()
    : Message{CEPH_MSG_OSD_SHARD_MAP, HEAD_VERSION, COMPAT_VERSION} {}
  explicit MOSDShardMap(osd_shard_map_t m)
    : Message{CEPH_MSG_OSD_SHARD_MAP, HEAD_VERSION, COMPAT_VERSION},
      shard_map(std::move(m)) {}

  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(shard_map, payload);
  }
  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(shard_map, p);
  }
  std::string_view get_type_name() const override { return "osd_shard_map"; }
  void print(std::ostream &out) const override { out << shard_map; }

private:
  ~MOSDShardMap() final {}
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};
