// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <ostream>

#include "include/encoding.h"

struct backfill_reservation_space_info_t {

  double relieved_usage_before = 0;
  double relieved_usage_after = 0;
  double target_usage_before = 0;
  double target_usage_after = 0;

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(relieved_usage_before, bl);
    ceph::encode(relieved_usage_after, bl);
    ceph::encode(target_usage_before, bl);
    ceph::encode(target_usage_after, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(relieved_usage_before, p);
    ceph::decode(relieved_usage_after, p);
    ceph::decode(target_usage_before, p);
    ceph::decode(target_usage_after, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(backfill_reservation_space_info_t)

inline std::ostream& operator<<(
  std::ostream& out,
  const backfill_reservation_space_info_t& info)
{
  return out << "relieved " << info.relieved_usage_before
	     << "->" << info.relieved_usage_after
	     << " target " << info.target_usage_before
	     << "->" << info.target_usage_after
	     << " ppm";
}
