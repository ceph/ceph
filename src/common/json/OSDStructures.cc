#include "OSDStructures.h"

#include "common/ceph_json.h"
#include "common/io_exerciser/OpType.h"

using namespace ceph::messaging::osd;

void OSDMapRequest::dump(Formatter* f) const {
  encode_json("prefix", "osd map", f);
  encode_json("pool", pool, f);
  encode_json("object", object, f);
  encode_json("nspace", nspace, f);
  encode_json("format", format, f);
}

void OSDMapRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("object", object, obj);
  JSONDecoder::decode_json("nspace", nspace, obj);
  JSONDecoder::decode_json("format", format, obj);
}

void OSDMapReply::dump(Formatter* f) const {
  encode_json("epoch", epoch, f);
  encode_json("pool", pool, f);
  encode_json("pool_id", pool_id, f);
  encode_json("objname", objname, f);
  encode_json("raw_pgid", raw_pgid, f);
  encode_json("pgid", pgid, f);
  encode_json("up", up, f);
  encode_json("up_primary", up_primary, f);
  encode_json("acting", acting, f);
  encode_json("acting_primary", acting_primary, f);
}

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

void OSDPoolGetRequest::dump(Formatter* f) const {
  encode_json("prefix", "osd pool get", f);
  encode_json("pool", pool, f);
  encode_json("var", var, f);
  encode_json("format", format, f);
}

void OSDPoolGetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("var", var, obj);
  JSONDecoder::decode_json("format", format, obj);
}

void OSDPoolGetReply::dump(Formatter* f) const {
  encode_json("erasure_code_profile", erasure_code_profile, f);
}

void OSDPoolGetReply::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("erasure_code_profile", erasure_code_profile, obj);
}

void OSDECProfileGetRequest::dump(Formatter* f) const {
  encode_json("prefix", "osd pool get", f);
  encode_json("name", name, f);
  encode_json("format", format, f);
}

void OSDECProfileGetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("format", format, obj);
}

void OSDECProfileGetReply::dump(Formatter* f) const {
  encode_json("crush-device-class", crush_device_class, f);
  encode_json("crush-failure-domain", crush_failure_domain, f);
  encode_json("crush-num-failure-domains", crush_num_failure_domains, f);
  encode_json("crush-osds-per-failure-domain", crush_osds_per_failure_domain,
              f);
  encode_json("crush-root", crush_root, f);
  encode_json("plugin", plugin, f);
  encode_json("k", k, f);
  encode_json("m", m, f);
  encode_json("l", l, f);
  encode_json("w", w, f);
  encode_json("c", c, f);
  encode_json("packetsize", packetsize, f);
  encode_json("technique", technique, f);
  encode_json("layers", layers, f);
  encode_json("mapping", mapping, f);
  encode_json("jerasure-per-chunk-alignment", jerasure_per_chunk_alignment, f);
}

void OSDECProfileGetReply::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("crush-device-class", crush_device_class, obj);
  JSONDecoder::decode_json("crush-failure-domain", crush_failure_domain, obj);
  JSONDecoder::decode_json("crush-num-failure-domains",
                           crush_num_failure_domains, obj);
  JSONDecoder::decode_json("crush-osds-per-failure-domain",
                           crush_osds_per_failure_domain, obj);
  JSONDecoder::decode_json("crush-root", crush_root, obj);
  JSONDecoder::decode_json("plugin", plugin, obj);
  JSONDecoder::decode_json("k", k, obj);
  JSONDecoder::decode_json("m", m, obj);
  JSONDecoder::decode_json("l", l, obj);
  JSONDecoder::decode_json("w", w, obj);
  JSONDecoder::decode_json("c", c, obj);
  JSONDecoder::decode_json("packetsize", packetsize, obj);
  JSONDecoder::decode_json("technique", technique, obj);
  JSONDecoder::decode_json("layers", layers, obj);
  JSONDecoder::decode_json("mapping", mapping, obj);
  JSONDecoder::decode_json("jerasure-per-chunk-alignment",
                           jerasure_per_chunk_alignment, obj);
}

void OSDECProfileSetRequest::dump(Formatter* f) const {
  encode_json("prefix", "osd erasure-code-profile set", f);
  encode_json("name", name, f);
  encode_json("profile", profile, f);
  encode_json("force", force, f);
}

void OSDECProfileSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("profile", profile, obj);
  JSONDecoder::decode_json("force", force, obj);
}

void OSDECPoolCreateRequest::dump(Formatter* f) const {
  encode_json("prefix", "osd pool create", f);
  encode_json("pool", pool, f);
  encode_json("pool_type", pool_type, f);
  encode_json("pg_num", pg_num, f);
  encode_json("pgp_num", pgp_num, f);
  encode_json("erasure_code_profile", erasure_code_profile, f);
}

void OSDECPoolCreateRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("pool", pool, obj);
  JSONDecoder::decode_json("pool_type", pool_type, obj);
  JSONDecoder::decode_json("pg_num", pg_num, obj);
  JSONDecoder::decode_json("pgp_num", pgp_num, obj);
  JSONDecoder::decode_json("erasure_code_profile", erasure_code_profile, obj);
}

void OSDSetRequest::dump(Formatter* f) const {
  encode_json("prefix", "osd set", f);
  encode_json("key", key, f);
  encode_json("yes_i_really_mean_it", yes_i_really_mean_it, f);
}

void OSDSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("yes_i_really_mean_it", yes_i_really_mean_it, obj);
}