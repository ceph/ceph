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

namespace rgw::dmclock {

/// array of per-client counters to serve as GetClientCounters
class ClientCounters {
  std::array<PerfCountersRef, static_cast<size_t>(client_id::count)> clients;
 public:
  ClientCounters(CephContext *cct);

  PerfCounters* operator()(client_id client) const {
    return clients[static_cast<size_t>(client)].get();
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

/// a simple wrapper to hold client config. objects needed to construct a
/// scheduler instance, the primary utility of this being to optionally
/// construct scheduler only when configured in the frontends.
class optional_scheduler_ctx {
  std::optional<ClientConfig> clients;
  std::optional<ClientCounters> counters;
public:
  optional_scheduler_ctx(CephContext *cct) {
    if(cct->_conf.get_val<bool>("rgw_dmclock_enabled")){
      clients.emplace(ClientConfig(cct));
      counters.emplace(ClientCounters(cct));
    }
  }
  operator bool() const noexcept { return counters && clients; }

  // both the get functions below will throw
  ClientCounters& get_counters() { return counters.value(); }
  ClientConfig& get_clients() { return clients.value(); }
};

} // namespace rgw::dmclock

#endif /* RGW_DMCLOCK_SCHEDULER_CTX_H */
