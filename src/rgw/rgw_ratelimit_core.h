#pragma once

#include <cstdint>
#include <string_view>

#include "include/buffer.h"
#include "rgw_common.h"

enum class OpType { Read, Write, List, Delete };

struct RGWRateLimitCounterState {
  static constexpr int64_t fixed_point = 1000;

  int64_t read_ops = 0;
  int64_t read_bytes = 0;
  int64_t write_ops = 0;
  int64_t write_bytes = 0;
  int64_t list_ops = 0;
  int64_t del_ops = 0;
  int64_t ts_ns = 0;
  bool first_run = true;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& bl);
};

OpType rgw_ratelimit_op_type(const std::string_view method,
                             const std::string_view resource,
                             const RGWRateLimitInfo* ratelimit_info);

namespace rgw::ratelimit {

int64_t compute_delay(int64_t limit, int64_t needed, int64_t interval);

int64_t consume(RGWRateLimitCounterState& state,
                OpType op_type,
                const RGWRateLimitInfo* info,
                ceph::timespan curr_timestamp,
                int64_t interval);

void giveback(RGWRateLimitCounterState& state, OpType op_type);

void decrease_bytes(RGWRateLimitCounterState& state,
                    bool is_read,
                    int64_t amount,
                    const RGWRateLimitInfo* info);

} // namespace rgw::ratelimit
