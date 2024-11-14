#include "JsonStructures.h"

#include "OpType.h"
#include "common/ceph_json.h"

using namespace ceph::io_exerciser::json;

JSONStructure::JSONStructure(std::shared_ptr<ceph::Formatter> formatter)
    : formatter(formatter) {}

std::string JSONStructure::encode_json() {
  oss.clear();

  dump();
  formatter->flush(oss);
  return oss.str();
}

OSDMapRequest::OSDMapRequest(const std::string& pool_name,
                             const std::string& object,
                             const std::string& nspace,
                             std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter),
      pool(pool_name),
      object(object),
      nspace(nspace) {}

OSDMapRequest::OSDMapRequest(std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void OSDMapRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("object", object, obj);
  JSONDecoder::decode_json("nspace", nspace, obj);
  JSONDecoder::decode_json("format", format, obj);
}

void OSDMapRequest::dump() const {
  formatter->open_object_section("OSDMapRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("object", object, formatter.get());
  ::encode_json("nspace", nspace, formatter.get());
  ::encode_json("format", format, formatter.get());
  formatter->close_section();
}

OSDMapReply::OSDMapReply(std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void OSDMapReply::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("epoch", epoch, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("pool_id", pool_id, obj);
  JSONDecoder::decode_json("objname", objname, obj);
  JSONDecoder::decode_json("raw_pgid", raw_pgid, obj);
  JSONDecoder::decode_json("pgid", pgid, obj);
  JSONDecoder::decode_json("up", up, obj);
  JSONDecoder::decode_json("up_primary", up_primary, obj);
  JSONDecoder::decode_json("acting", acting, obj);
  JSONDecoder::decode_json("acting_primary", acting_primary, obj);
}

void OSDMapReply::dump() const {
  formatter->open_object_section("OSDMapReply");
  ::encode_json("epoch", epoch, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("pool_id", pool_id, formatter.get());
  ::encode_json("objname", objname, formatter.get());
  ::encode_json("raw_pgid", raw_pgid, formatter.get());
  ::encode_json("pgid", pgid, formatter.get());
  ::encode_json("up", up, formatter.get());
  ::encode_json("up_primary", up_primary, formatter.get());
  ::encode_json("acting", acting, formatter.get());
  ::encode_json("acting_primary", acting_primary, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDPoolGetRequest ::OSDPoolGetRequest(
    const std::string& pool_name, std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter), pool(pool_name) {}

ceph::io_exerciser::json::OSDPoolGetRequest ::OSDPoolGetRequest(
    JSONObj* obj, std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {
  ceph::io_exerciser::json::OSDPoolGetRequest::decode_json(obj);
}

void ceph::io_exerciser::json::OSDPoolGetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("var", var, obj);
  JSONDecoder::decode_json("format", format, obj);
}

void ceph::io_exerciser::json::OSDPoolGetRequest::dump() const {
  formatter->open_object_section("OSDPoolGetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("var", var, formatter.get());
  ::encode_json("format", format, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDPoolGetReply ::OSDPoolGetReply(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void ceph::io_exerciser::json::OSDPoolGetReply::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("erasure_code_profile", erasure_code_profile, obj);
}

void ceph::io_exerciser::json::OSDPoolGetReply::dump() const {
  formatter->open_object_section("OSDPoolGetReply");
  ::encode_json("erasure_code_profile", erasure_code_profile, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDECProfileGetRequest ::OSDECProfileGetRequest(
    const std::string& profile_name, std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter), name(profile_name) {}

ceph::io_exerciser::json::OSDECProfileGetRequest ::OSDECProfileGetRequest(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void ceph::io_exerciser::json::OSDECProfileGetRequest::decode_json(
    JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("format", format, obj);
}

void ceph::io_exerciser::json::OSDECProfileGetRequest::dump() const {
  formatter->open_object_section("OSDECProfileGetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("name", name, formatter.get());
  ::encode_json("format", format, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDECProfileGetReply ::OSDECProfileGetReply(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void ceph::io_exerciser::json::OSDECProfileGetReply::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("crush-device-class", crush_device_class, obj);
  JSONDecoder::decode_json("crush-failure-domain", crush_failure_domain, obj);
  JSONDecoder::decode_json("crush-num-failure-domains",
                           crush_num_failure_domains, obj);
  JSONDecoder::decode_json("crush-osds-per-failure-domain",
                           crush_osds_per_failure_domain, obj);
  JSONDecoder::decode_json("crush-root", crush_root, obj);
  JSONDecoder::decode_json("jerasure-per-chunk-alignment",
                           jerasure_per_chunk_alignment, obj);
  JSONDecoder::decode_json("k", k, obj);
  JSONDecoder::decode_json("m", m, obj);
  JSONDecoder::decode_json("plugin", plugin, obj);
  JSONDecoder::decode_json("technique", technique, obj);
  JSONDecoder::decode_json("w", w, obj);
}

void ceph::io_exerciser::json::OSDECProfileGetReply::dump() const {
  formatter->open_object_section("OSDECProfileGetReply");
  ::encode_json("crush-device-class", crush_device_class, formatter.get());
  ::encode_json("crush-failure-domain", crush_failure_domain, formatter.get());
  ::encode_json("crush-num-failure-domains", crush_num_failure_domains,
                formatter.get());
  ::encode_json("crush-osds-per-failure-domain", crush_osds_per_failure_domain,
                formatter.get());
  ::encode_json("crush-root", crush_root, formatter.get());
  ::encode_json("jerasure-per-chunk-alignment", jerasure_per_chunk_alignment,
                formatter.get());
  ::encode_json("k", k, formatter.get());
  ::encode_json("m", m, formatter.get());
  ::encode_json("plugin", plugin, formatter.get());
  ::encode_json("technique", technique, formatter.get());
  ::encode_json("w", w, formatter.get());
  formatter->close_section();
}

ceph::io_exerciser::json::OSDECProfileSetRequest ::OSDECProfileSetRequest(
    const std::string& name, const std::vector<std::string>& profile,
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter), name(name), profile(profile) {}

OSDECProfileSetRequest ::OSDECProfileSetRequest(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void OSDECProfileSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("profile", profile, obj);
}

void OSDECProfileSetRequest::dump() const {
  formatter->open_object_section("OSDECProfileSetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("name", name, formatter.get());
  ::encode_json("profile", profile, formatter.get());
  formatter->close_section();
}

OSDECPoolCreateRequest::OSDECPoolCreateRequest(
    const std::string& pool, const std::string& erasure_code_profile,
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter),
      pool(pool),
      erasure_code_profile(erasure_code_profile) {}

OSDECPoolCreateRequest ::OSDECPoolCreateRequest(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void OSDECPoolCreateRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("pool_type", pool_type, obj);
  JSONDecoder::decode_json("pg_num", pg_num, obj);
  JSONDecoder::decode_json("pgp_num", pgp_num, obj);
  JSONDecoder::decode_json("erasure_code_profile", erasure_code_profile, obj);
}

void OSDECPoolCreateRequest::dump() const {
  formatter->open_object_section("OSDECPoolCreateRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("pool_type", pool_type, formatter.get());
  ::encode_json("pg_num", pg_num, formatter.get());
  ::encode_json("pgp_num", pgp_num, formatter.get());
  ::encode_json("erasure_code_profile", erasure_code_profile, formatter.get());
  formatter->close_section();
}

OSDSetRequest::OSDSetRequest(const std::string& key,
                             const std::optional<bool>& yes_i_really_mean_it,
                             std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter),
      key(key),
      yes_i_really_mean_it(yes_i_really_mean_it) {}

OSDSetRequest::OSDSetRequest(std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void OSDSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("yes_i_really_mean_it", yes_i_really_mean_it, obj);
}

void OSDSetRequest::dump() const {
  formatter->open_object_section("OSDSetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("key", key, formatter.get());
  ::encode_json("yes_i_really_mean_it", yes_i_really_mean_it, formatter.get());
  formatter->close_section();
}

BalancerOffRequest::BalancerOffRequest(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void ceph::io_exerciser::json::BalancerOffRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
}

void BalancerOffRequest::dump() const {
  formatter->open_object_section("BalancerOffRequest");
  ::encode_json("prefix", prefix, formatter.get());
  formatter->close_section();
}

BalancerStatusRequest ::BalancerStatusRequest(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void ceph::io_exerciser::json::BalancerStatusRequest::decode_json(
    JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
}

void BalancerStatusRequest::dump() const {
  formatter->open_object_section("BalancerStatusRequest");
  ::encode_json("prefix", prefix, formatter.get());
  formatter->close_section();
}

BalancerStatusReply::BalancerStatusReply(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void BalancerStatusReply::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("active", active, obj);
  JSONDecoder::decode_json("last_optimization_duration",
                           last_optimization_duration, obj);
  JSONDecoder::decode_json("last_optimization_started",
                           last_optimization_started, obj);
  JSONDecoder::decode_json("mode", mode, obj);
  JSONDecoder::decode_json("no_optimization_needed", no_optimization_needed,
                           obj);
  JSONDecoder::decode_json("optimize_result", optimize_result, obj);
}

void BalancerStatusReply::dump() const {
  formatter->open_object_section("BalancerStatusReply");
  ::encode_json("active", active, formatter.get());
  ::encode_json("last_optimization_duration", last_optimization_duration,
                formatter.get());
  ::encode_json("last_optimization_started", last_optimization_started,
                formatter.get());
  ::encode_json("mode", mode, formatter.get());
  ::encode_json("no_optimization_needed", no_optimization_needed,
                formatter.get());
  ::encode_json("optimize_result", optimize_result, formatter.get());
  formatter->close_section();
}

ConfigSetRequest::ConfigSetRequest(const std::string& who,
                                   const std::string& name,
                                   const std::string& value,
                                   const std::optional<bool>& force,
                                   std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter),
      who(who),
      name(name),
      value(value),
      force(force) {}

ConfigSetRequest::ConfigSetRequest(std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void ConfigSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("who", who, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("value", value, obj);
  JSONDecoder::decode_json("force", force, obj);
}

void ConfigSetRequest::dump() const {
  formatter->open_object_section("ConfigSetRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("who", who, formatter.get());
  ::encode_json("name", name, formatter.get());
  ::encode_json("value", value, formatter.get());
  ::encode_json("force", force, formatter.get());
  formatter->close_section();
}

InjectECErrorRequest::InjectECErrorRequest(
    InjectOpType injectOpType, const std::string& pool,
    const std::string& objname, int shardid,
    const std::optional<uint64_t>& type, const std::optional<uint64_t>& when,
    const std::optional<uint64_t>& duration,
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter),
      pool(pool),
      objname(objname),
      shardid(shardid),
      type(type),
      when(when),
      duration(duration) {
  switch (injectOpType) {
    case InjectOpType::ReadEIO:
      [[fallthrough]];
    case InjectOpType::ReadMissingShard:
      prefix = "injectecreaderr";
      break;
    case InjectOpType::WriteFailAndRollback:
      [[fallthrough]];
    case InjectOpType::WriteOSDAbort:
      prefix = "injectecwriteerr";
      break;
    default:
      ceph_abort_msg("Invalid OP type to inject");
  }
}

InjectECErrorRequest ::InjectECErrorRequest(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void InjectECErrorRequest::dump() const {
  formatter->open_object_section("InjectECErrorRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("objname", objname, formatter.get());
  ::encode_json("shardid", shardid, formatter.get());
  ::encode_json("type", type, formatter.get());
  ::encode_json("when", when, formatter.get());
  ::encode_json("duration", duration, formatter.get());
  formatter->close_section();
}

void InjectECErrorRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("objname", objname, obj);
  JSONDecoder::decode_json("shardid", shardid, obj);
  JSONDecoder::decode_json("type", type, obj);
  JSONDecoder::decode_json("when", when, obj);
  JSONDecoder::decode_json("duration", duration, obj);
}

InjectECClearErrorRequest::InjectECClearErrorRequest(
    InjectOpType injectOpType, const std::string& pool,
    const std::string& objname, int shardid,
    const std::optional<uint64_t>& type,
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter),
      pool(pool),
      objname(objname),
      shardid(shardid),
      type(type) {
  switch (injectOpType) {
    case InjectOpType::ReadEIO:
      [[fallthrough]];
    case InjectOpType::ReadMissingShard:
      prefix = "injectecclearreaderr";
      break;
    case InjectOpType::WriteFailAndRollback:
      [[fallthrough]];
    case InjectOpType::WriteOSDAbort:
      prefix = "injectecclearwriteerr";
      break;
    default:
      ceph_abort_msg("Invalid OP type to inject");
  }
}

InjectECClearErrorRequest ::InjectECClearErrorRequest(
    std::shared_ptr<ceph::Formatter> formatter)
    : JSONStructure(formatter) {}

void InjectECClearErrorRequest::dump() const {
  formatter->open_object_section("InjectECErrorRequest");
  ::encode_json("prefix", prefix, formatter.get());
  ::encode_json("pool", pool, formatter.get());
  ::encode_json("objname", objname, formatter.get());
  ::encode_json("shardid", shardid, formatter.get());
  ::encode_json("type", type, formatter.get());
  formatter->close_section();
}

void InjectECClearErrorRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("prefix", prefix, obj);
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("objname", objname, obj);
  JSONDecoder::decode_json("shardid", shardid, obj);
  JSONDecoder::decode_json("type", type, obj);
}