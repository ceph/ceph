#include "BalancerStructures.h"

#include "common/ceph_json.h"

using namespace ceph::messaging::balancer;

void BalancerOffRequest::dump(Formatter* f) const {
  encode_json("prefix", "balancer off", f);
}

void BalancerOffRequest::decode_json(JSONObj* obj) {}

void BalancerStatusRequest::dump(Formatter* f) const {
  encode_json("prefix", "balancer status", f);
}

void BalancerStatusRequest::decode_json(JSONObj* obj) {}

void BalancerStatusReply::dump(Formatter* f) const {
  encode_json("active", active, f);
  encode_json("last_optimization_duration", last_optimization_duration, f);
  encode_json("last_optimization_started", last_optimization_started, f);
  encode_json("mode", mode, f);
  encode_json("no_optimization_needed", no_optimization_needed, f);
  encode_json("optimize_result", optimize_result, f);
}

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