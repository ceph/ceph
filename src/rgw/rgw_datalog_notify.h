// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

#include "rgw/rgw_datalog.h"

namespace bc = boost::container;

namespace ceph { class Formatter; }
class JSONObj;

class RGWCoroutine;
class RGWHTTPManager;
class RGWRESTConn;

struct rgw_data_notify_entry;

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
