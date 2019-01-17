#ifndef RGW_DMCLOCK_SCHEDULER_CTX_H
#define RGW_DMCLOCK_SCHEDULER_CTX_H

#include "common/perf_counters.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "rgw_dmclock.h"

namespace queue_counters {

  enum {
        l_first = 427150,
        l_qlen,
        l_cost,
        l_res,
        l_res_cost,
        l_prio,
        l_prio_cost,
        l_limit,
        l_limit_cost,
        l_cancel,
        l_cancel_cost,
        l_res_latency,
        l_prio_latency,
        l_last,
  };

  PerfCountersRef build(CephContext *cct, const std::string& name);

} // namespace queue_counters

namespace throttle_counters {
  enum {
        l_first = 437219,
        l_throttle,
        l_last
  };

  PerfCountersRef build(CephContext *cct, const std::string& name);
} // namespace throttle

namespace rgw::dmclock {

// the last client counter would be for global scheduler stats
static constexpr auto counter_size = static_cast<size_t>(client_id::count) + 1;
/// array of per-client counters to serve as GetClientCounters
class ClientCounters {
  std::array<PerfCountersRef, counter_size> clients;
 public:
  ClientCounters(CephContext *cct);

  PerfCounters* operator()(client_id client) const {
    return clients[static_cast<size_t>(client)].get();
  }
};

class ThrottleCounters {
  PerfCountersRef counters;
public:
  ThrottleCounters(CephContext* const cct,const std::string& name):
    counters(throttle_counters::build(cct, name)) {}

  PerfCounters* operator()() const {
    return counters.get();
  }
};


struct ClientSum {
  uint64_t count{0};
  Cost cost{0};
};

constexpr auto client_count = static_cast<size_t>(client_id::count);
using ClientSums = std::array<ClientSum, client_count>;

void inc(ClientSums& sums, client_id client, Cost cost);
void on_cancel(PerfCounters *c, const ClientSum& sum);
void on_process(PerfCounters* c, const ClientSum& rsum, const ClientSum& psum);


class ClientConfig : public md_config_obs_t {
  std::vector<ClientInfo> clients;

  void update(const ConfigProxy &conf);

public:
  ClientConfig(CephContext *cct);

  ClientInfo* operator()(client_id client);

  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override;
};

} // namespace rgw::dmclock

#endif /* RGW_DMCLOCK_SCHEDULER_CTX_H */
