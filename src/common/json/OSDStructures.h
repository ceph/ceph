#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/ceph_json.h"
#include "common/io_exerciser/OpType.h"
#include "include/types.h"

class JSONObj;

namespace ceph {
namespace messaging {
namespace osd {
struct OSDMapRequest {
  std::string pool;
  std::string object;
  std::string nspace;
  std::string format = "json";

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct OSDMapReply {
  epoch_t epoch;
  std::string pool;
  uint64_t pool_id;
  std::string objname;
  std::string raw_pgid;
  std::string pgid;
  std::vector<int> up;
  int up_primary;
  std::vector<int> acting;
  int acting_primary;

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct OSDPoolGetRequest {
  std::string pool;
  std::string var = "erasure_code_profile";
  std::string format = "json";

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct OSDPoolGetReply {
  std::string erasure_code_profile;

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct OSDECProfileGetRequest {
  std::string name;
  std::string format = "json";

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct OSDECProfileGetReply {
  std::string crush_device_class;
  std::string crush_failure_domain;
  int crush_num_failure_domains;
  int crush_osds_per_failure_domain;
  std::string crush_root;
  std::string plugin;
  int k;
  int m;
  std::optional<int> l;
  std::optional<int> w;
  std::optional<int> c;
  std::optional<uint64_t> packetsize;
  std::optional<std::string> technique;
  std::optional<std::string> layers;
  std::optional<std::string> mapping;
  std::optional<bool> jerasure_per_chunk_alignment;

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct OSDECProfileSetRequest {
  std::string name;
  std::vector<std::string> profile;
  bool force;

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct OSDECPoolCreateRequest {
  std::string pool;
  std::string pool_type;
  int pg_num;
  int pgp_num;
  std::string erasure_code_profile;

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

struct OSDSetRequest {
  std::string key;
  std::optional<bool> yes_i_really_mean_it = std::nullopt;

  void dump(Formatter* f) const;
  void decode_json(JSONObj* obj);
};

// These structures are sent directly to the relevant OSD
// rather than the monitor
template <io_exerciser::InjectOpType op_type>
struct InjectECErrorRequest {
  std::string pool;
  std::string objname;
  int shardid;
  std::optional<uint64_t> type;
  std::optional<uint64_t> when;
  std::optional<uint64_t> duration;

  void dump(Formatter* f) const {
    switch (op_type) {
      case io_exerciser::InjectOpType::ReadEIO:
        [[fallthrough]];
      case io_exerciser::InjectOpType::ReadMissingShard:
        ::encode_json("prefix", "injectecreaderr", f);
        break;
      case io_exerciser::InjectOpType::WriteFailAndRollback:
        [[fallthrough]];
      case io_exerciser::InjectOpType::WriteOSDAbort:
        ::encode_json("prefix", "injectecwriteerr", f);
        break;
      default:
        ceph_abort_msg("Unsupported Inject Type");
    }
    ::encode_json("pool", pool, f);
    ::encode_json("objname", objname, f);
    ::encode_json("shardid", shardid, f);
    ::encode_json("type", type, f);
    ::encode_json("when", when, f);
    ::encode_json("duration", duration, f);
  }
  void decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("pool", pool, obj);
    JSONDecoder::decode_json("objname", objname, obj);
    JSONDecoder::decode_json("shardid", shardid, obj);
    JSONDecoder::decode_json("type", type, obj);
    JSONDecoder::decode_json("when", when, obj);
    JSONDecoder::decode_json("duration", duration, obj);
  }
};

template <io_exerciser::InjectOpType op_type>
struct InjectECClearErrorRequest {
  std::string pool;
  std::string objname;
  int shardid;
  std::optional<uint64_t> type;

  void dump(Formatter* f) const {
    switch (op_type) {
      case io_exerciser::InjectOpType::ReadEIO:
        [[fallthrough]];
      case io_exerciser::InjectOpType::ReadMissingShard:
        ::encode_json("prefix", "injectecclearreaderr", f);
        break;
      case io_exerciser::InjectOpType::WriteFailAndRollback:
        [[fallthrough]];
      case io_exerciser::InjectOpType::WriteOSDAbort:
        ::encode_json("prefix", "injectecclearwriteerr", f);
        break;
      default:
        ceph_abort_msg("Unsupported Inject Type");
    }
    ::encode_json("pool", pool, f);
    ::encode_json("objname", objname, f);
    ::encode_json("shardid", shardid, f);
    ::encode_json("type", type, f);
  }
  void decode_json(JSONObj* obj) {
    JSONDecoder::decode_json("pool", pool, obj);
    JSONDecoder::decode_json("objname", objname, obj);
    JSONDecoder::decode_json("shardid", shardid, obj);
    JSONDecoder::decode_json("type", type, obj);
  }
};
}  // namespace osd
}  // namespace messaging
}  // namespace ceph