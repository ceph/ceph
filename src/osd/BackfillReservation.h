// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <algorithm>
#include <cstdint>
#include <ostream>

#include "include/encoding.h"

struct backfill_reservation_space_info_t {

  static constexpr uint64_t SCORE_SCALE = 100000000;

  double relieved_usage_before = 0;
  double relieved_usage_after = 0;
  double target_usage_before = 0;
  double target_usage_after = 0;

  static double encode_usage_ratio(double ratio) {
    return std::clamp(ratio, 0.0, 1.0);
  }

  static double encode_usage_ratio(uint64_t used, uint64_t total) {
    if (total == 0) {
      return 0;
    }
    return encode_usage_ratio(static_cast<double>(used) / total);
  }

  double raw_score(const double near_full_ratio, const double full_ratio) const {
    int64_t scaled_r_before = relieved_usage_before * SCORE_SCALE;
    int64_t scaled_r_after = relieved_usage_after * SCORE_SCALE;
    int64_t scaled_t_after = target_usage_after * SCORE_SCALE;
    double f = 0.0;
    if (scaled_r_before > scaled_r_after) {
      double element = scaled_r_before - std::max(scaled_r_after, scaled_t_after);
      double denominator = scaled_r_before - scaled_r_after;
      f = std::clamp(element / denominator, 0.0, 1.0);
    }
    int64_t scaled_near_full_ratio = near_full_ratio * SCORE_SCALE;
    int64_t scaled_full_ratio = full_ratio * SCORE_SCALE;
    double to_near_full = scaled_r_before - scaled_near_full_ratio;
    double span = scaled_full_ratio - scaled_near_full_ratio;
    double w = 0.0;
    if (span > 0) {
      w = std::clamp(to_near_full / span, 0.0, 1.0);
    }
    return f * w;
  }

  unsigned priority_boost(
    unsigned priority_band_remaining,
    const double near_full_ratio,
    const double full_ratio) const
  {
    double score = raw_score(near_full_ratio, full_ratio);
    score = std::clamp(score, 0.0, 1.0);
    return std::roundl(priority_band_remaining * score);
  }

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

struct backfill_osd_space_usage_t {
  uint64_t used_bytes = 0;
  uint64_t total_bytes = 0;

  double usage_ratio() const {
    return backfill_reservation_space_info_t::encode_usage_ratio(
      used_bytes, total_bytes);
  }

  double projected_usage_ratio(int64_t delta_bytes) const {
    uint64_t projected = used_bytes;
    if (delta_bytes < 0) {
      const uint64_t reduction = static_cast<uint64_t>(-delta_bytes);
      projected = reduction > projected ? 0 : projected - reduction;
    } else if (delta_bytes > 0) {
      const uint64_t increase = static_cast<uint64_t>(delta_bytes);
      projected = total_bytes - projected < increase ?
	total_bytes : projected + increase;
    }
    return backfill_reservation_space_info_t::encode_usage_ratio(
      projected, total_bytes);
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    ceph::encode(used_bytes, bl);
    ceph::encode(total_bytes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& p) {
    DECODE_START(1, p);
    ceph::decode(used_bytes, p);
    ceph::decode(total_bytes, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(backfill_osd_space_usage_t)

inline std::ostream& operator<<(
  std::ostream& out,
  const backfill_reservation_space_info_t& info)
{
  return out << "relieved " << info.relieved_usage_before
	     << "->" << info.relieved_usage_after
	     << " target " << info.target_usage_before
	     << "->" << info.target_usage_after
	     << " ratio";
}
