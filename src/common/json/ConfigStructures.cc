#include "ConfigStructures.h"

#include "common/ceph_json.h"

using namespace ceph::messaging::config;

void ConfigSetRequest::dump(Formatter* f) const {
  encode_json("prefix", "config set", f);
  encode_json("who", who, f);
  encode_json("name", name, f);
  encode_json("value", value, f);
  encode_json("force", force, f);
}

void ConfigSetRequest::decode_json(JSONObj* obj) {
  JSONDecoder::decode_json("who", who, obj);
  JSONDecoder::decode_json("name", name, obj);
  JSONDecoder::decode_json("value", value, obj);
  JSONDecoder::decode_json("force", force, obj);
}