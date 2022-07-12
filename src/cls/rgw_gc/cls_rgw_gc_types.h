// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h"
#include <unordered_map>

struct cls_rgw_gc_urgent_data
{
  std::unordered_map<std::string, ceph::real_time> urgent_data_map;
  uint32_t num_urgent_data_entries{0}; // requested by user
  uint32_t num_head_urgent_entries{0}; // actual number of entries in queue head
  uint32_t num_xattr_urgent_entries{0}; // actual number of entries in xattr in case of spill over

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(urgent_data_map, bl);
    encode(num_urgent_data_entries, bl);
    encode(num_head_urgent_entries, bl);
    encode(num_xattr_urgent_entries, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(urgent_data_map, bl);
    decode(num_urgent_data_entries, bl);
    decode(num_head_urgent_entries, bl);
    decode(num_xattr_urgent_entries, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_rgw_gc_urgent_data)
