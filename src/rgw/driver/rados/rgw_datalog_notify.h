// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <cstdint>
#include <ostream>
#include <string>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

namespace bc = boost::container;

namespace ceph { class Formatter; }
class JSONObj;

class RGWCoroutine;
class RGWHTTPManager;
class RGWRESTConn;

struct rgw_data_notify_entry {
  std::string key;
  uint64_t gen = 0;

  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);

  rgw_data_notify_entry& operator=(const rgw_data_notify_entry&) = default;

  bool operator <(const rgw_data_notify_entry& d) const {
    if (key < d.key) {
      return true;
    }
    if (d.key < key) {
      return false;
    }
    return gen < d.gen;
  }
  friend std::ostream& operator <<(std::ostream& m,
				   const rgw_data_notify_entry& e) {
    return m << "[key: " << e.key << ", gen: " << e.gen << "]";
  }
};

// json encoder and decoder for notify v1 API
struct rgw_data_notify_v1_encoder {
  const bc::flat_map<int, bc::flat_set<rgw_data_notify_entry>>& shards;
};
void encode_json(const char *name, const rgw_data_notify_v1_encoder& e,
                 ceph::Formatter *f);
struct rgw_data_notify_v1_decoder {
  bc::flat_map<int, bc::flat_set<rgw_data_notify_entry>>& shards;
};
void decode_json_obj(rgw_data_notify_v1_decoder& d, JSONObj *obj);
