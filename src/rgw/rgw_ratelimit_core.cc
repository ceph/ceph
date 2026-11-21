#include "rgw_ratelimit_core.h"

#include <algorithm>
#include <chrono>

using namespace std::chrono_literals;

void RGWRateLimitCounterState::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(read_ops, bl);
  encode(read_bytes, bl);
  encode(write_ops, bl);
  encode(write_bytes, bl);
  encode(list_ops, bl);
  encode(del_ops, bl);
  encode(ts_ns, bl);
  encode(first_run, bl);
  ENCODE_FINISH(bl);
}

void RGWRateLimitCounterState::decode(bufferlist::const_iterator& bl)
{
  DECODE_START(1, bl);
  decode(read_ops, bl);
  decode(read_bytes, bl);
  decode(write_ops, bl);
  decode(write_bytes, bl);
  decode(list_ops, bl);
  decode(del_ops, bl);
  decode(ts_ns, bl);
  decode(first_run, bl);
  DECODE_FINISH(bl);
}

static inline constexpr std::string_view RESOURCE_PATTERN_LIST_TYPE = "list-type=";
static inline constexpr std::string_view RESOURCE_PATTERN_PREFIX = "prefix=";
static inline constexpr std::string_view RESOURCE_PATTERN_DELIMITER = "delimiter=";

OpType rgw_ratelimit_op_type(const std::string_view method,
                             const std::string_view resource,
                             const RGWRateLimitInfo* ratelimit_info)
{
  const bool ratelimit_list = ratelimit_info && (ratelimit_info->max_list_ops > 0);
  const bool ratelimit_delete = ratelimit_info && (ratelimit_info->max_delete_ops > 0);

  auto contains_any = [](std::string_view s, auto&&... patterns) {
    return ((s.find(patterns) != std::string::npos) || ...);
  };
  if (ratelimit_list && method == "GET" &&
      !resource.empty() &&
      contains_any(resource, RESOURCE_PATTERN_LIST_TYPE,
                   RESOURCE_PATTERN_PREFIX, RESOURCE_PATTERN_DELIMITER)) {
    return OpType::List;
  }
  if (method == "GET" || method == "HEAD") {
    return OpType::Read;
  }
  if (ratelimit_delete && method == "DELETE") {
    return OpType::Delete;
  }
  return OpType::Write;
}

namespace rgw::ratelimit {

int64_t compute_delay(int64_t limit, int64_t needed, int64_t interval)
{
  if (limit <= 0 || needed <= 0) {
    return 0;
  }
  return (needed * interval + limit - 1) / limit;
}

static int64_t read_ops_tokens(const RGWRateLimitCounterState& state)
{
  return state.read_ops / RGWRateLimitCounterState::fixed_point;
}

static int64_t write_ops_tokens(const RGWRateLimitCounterState& state)
{
  return state.write_ops / RGWRateLimitCounterState::fixed_point;
}

static int64_t list_ops_tokens(const RGWRateLimitCounterState& state)
{
  return state.list_ops / RGWRateLimitCounterState::fixed_point;
}

static int64_t delete_ops_tokens(const RGWRateLimitCounterState& state)
{
  return state.del_ops / RGWRateLimitCounterState::fixed_point;
}

static bool minimum_time_reached(const RGWRateLimitCounterState& state,
                                 ceph::timespan curr_timestamp,
                                 int64_t interval)
{
  const auto min_duration = std::chrono::duration_cast<ceph::timespan>(
      std::chrono::seconds(interval)) / RGWRateLimitCounterState::fixed_point;
  const ceph::timespan ts{std::chrono::nanoseconds(state.ts_ns)};
  const auto delta = curr_timestamp - ts;
  return delta >= min_duration;
}

static void increase_tokens(RGWRateLimitCounterState& state,
                            ceph::timespan curr_timestamp,
                            const RGWRateLimitInfo* info,
                            int64_t interval)
{
  constexpr int fp = RGWRateLimitCounterState::fixed_point;
  if (state.first_run) {
    state.write_ops = info->max_write_ops * fp;
    state.write_bytes = info->max_write_bytes * fp;
    state.read_ops = info->max_read_ops * fp;
    state.read_bytes = info->max_read_bytes * fp;
    state.list_ops = info->max_list_ops * fp;
    state.del_ops = info->max_delete_ops * fp;
    state.ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        curr_timestamp).count();
    state.first_run = false;
    return;
  }

  const ceph::timespan ts{std::chrono::nanoseconds(state.ts_ns)};
  if (curr_timestamp > ts && minimum_time_reached(state, curr_timestamp, interval)) {
    const int64_t time_in_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(curr_timestamp - ts).count() /
        static_cast<double>(interval) / std::milli::den * fp;
    state.ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        curr_timestamp).count();
    const int64_t write_ops = info->max_write_ops * time_in_ms;
    const int64_t write_bw = info->max_write_bytes * time_in_ms;
    const int64_t read_ops = info->max_read_ops * time_in_ms;
    const int64_t read_bw = info->max_read_bytes * time_in_ms;
    const int64_t list_ops = info->max_list_ops * time_in_ms;
    const int64_t delete_ops = info->max_delete_ops * time_in_ms;
    state.read_ops = std::min(info->max_read_ops * fp, read_ops + state.read_ops);
    state.read_bytes = std::min(info->max_read_bytes * fp, read_bw + state.read_bytes);
    state.write_ops = std::min(info->max_write_ops * fp, write_ops + state.write_ops);
    state.write_bytes = std::min(info->max_write_bytes * fp, write_bw + state.write_bytes);
    state.list_ops = std::min(info->max_list_ops * fp, list_ops + state.list_ops);
    state.del_ops = std::min(info->max_delete_ops * fp, delete_ops + state.del_ops);
  }
}

static int64_t should_rate_limit_read(RGWRateLimitCounterState& state,
                                      int64_t ops_limit,
                                      int64_t bw_limit,
                                      int64_t interval)
{
  constexpr int fp = RGWRateLimitCounterState::fixed_point;
  const int64_t ops_delay = compute_delay(ops_limit * fp,
                                          fp - state.read_ops,
                                          interval);
  const int64_t bw_delay = compute_delay(bw_limit * fp,
                                         -state.read_bytes,
                                         interval);
  const int64_t delay = std::max(ops_delay, bw_delay);
  if (delay > 0) {
    return delay;
  }
  if (read_ops_tokens(state) > 0) {
    state.read_ops -= fp;
  }
  return 0;
}

static int64_t should_rate_limit_write(RGWRateLimitCounterState& state,
                                       int64_t ops_limit,
                                       int64_t bw_limit,
                                       int64_t interval)
{
  constexpr int fp = RGWRateLimitCounterState::fixed_point;
  const int64_t ops_delay = compute_delay(ops_limit * fp,
                                          fp - state.write_ops,
                                          interval);
  const int64_t bw_delay = compute_delay(bw_limit * fp,
                                         -state.write_bytes,
                                         interval);
  const int64_t delay = std::max(ops_delay, bw_delay);
  if (delay > 0) {
    return delay;
  }
  if (write_ops_tokens(state) > 0) {
    state.write_ops -= fp;
  }
  return 0;
}

static int64_t should_rate_limit_list(RGWRateLimitCounterState& state,
                                      int64_t ops_limit,
                                      int64_t interval)
{
  constexpr int fp = RGWRateLimitCounterState::fixed_point;
  const int64_t delay = compute_delay(ops_limit * fp,
                                      fp - state.list_ops,
                                      interval);
  if (delay > 0) {
    return delay;
  }
  if (list_ops_tokens(state) > 0) {
    state.list_ops -= fp;
  }
  return 0;
}

static int64_t should_rate_limit_delete(RGWRateLimitCounterState& state,
                                        int64_t ops_limit,
                                        int64_t interval)
{
  constexpr int fp = RGWRateLimitCounterState::fixed_point;
  const int64_t delay = compute_delay(ops_limit * fp,
                                      fp - state.del_ops,
                                      interval);
  if (delay > 0) {
    return delay;
  }
  if (delete_ops_tokens(state) > 0) {
    state.del_ops -= fp;
  }
  return 0;
}

int64_t consume(RGWRateLimitCounterState& state,
                OpType op_type,
                const RGWRateLimitInfo* info,
                ceph::timespan curr_timestamp,
                int64_t interval)
{
  increase_tokens(state, curr_timestamp, info, interval);
  switch (op_type) {
  case OpType::Read:
    return should_rate_limit_read(state, info->max_read_ops, info->max_read_bytes, interval);
  case OpType::Write:
    return should_rate_limit_write(state, info->max_write_ops, info->max_write_bytes, interval);
  case OpType::List:
    return should_rate_limit_list(state, info->max_list_ops, interval);
  case OpType::Delete:
    return should_rate_limit_delete(state, info->max_delete_ops, interval);
  }
  return 0;
}

void giveback(RGWRateLimitCounterState& state, OpType op_type)
{
  constexpr int fp = RGWRateLimitCounterState::fixed_point;
  switch (op_type) {
  case OpType::Read:
    state.read_ops += fp;
    break;
  case OpType::Write:
    state.write_ops += fp;
    break;
  case OpType::List:
    state.list_ops += fp;
    break;
  case OpType::Delete:
    state.del_ops += fp;
    break;
  }
}

void decrease_bytes(RGWRateLimitCounterState& state,
                    bool is_read,
                    int64_t amount,
                    const RGWRateLimitInfo* info)
{
  constexpr int fp = RGWRateLimitCounterState::fixed_point;
  if (is_read) {
    state.read_bytes = std::max(state.read_bytes - amount * fp,
                                info->max_read_bytes * fp * -2);
  } else {
    state.write_bytes = std::max(state.write_bytes - amount * fp,
                                 info->max_write_bytes * fp * -2);
  }
}

} // namespace rgw::ratelimit
