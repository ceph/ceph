#ifndef RGW_DMCLOCK_H
#define RGW_DMCLOCK_H
#include "dmclock/src/dmclock_server.h"

namespace rgw::dmclock {
enum class client_id {
                      admin, //< /admin apis
                      auth, //< swift auth, sts
                      data, //< PutObj, GetObj
                      metadata, //< bucket operations, object metadata
                      count
};

// TODO move these to dmclock/types or so in submodule
using crimson::dmclock::Cost;
using crimson::dmclock::ClientInfo;

enum class scheduler_t {
                        none,
                        throttler,
                        dmclock
};

inline scheduler_t get_scheduler_t(CephContext* const cct)
{
  const auto scheduler_type = cct->_conf.get_val<std::string>("rgw_scheduler_type");
  if (scheduler_type == "dmclock")
    return scheduler_t::dmclock;
  else if (scheduler_type == "throttler")
    return scheduler_t::throttler;
  else
    return scheduler_t::none;
}

} // namespace rgw::dmclock

#endif /* RGW_DMCLOCK_H */
