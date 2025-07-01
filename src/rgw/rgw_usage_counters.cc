#include "rgw_usage_counters.h"

namespace rgw::usage_counters {

extern void rgw_usage_counters_init(CephContext* cct);
extern void rgw_usage_counters_shutdown(CephContext* cct);

void init(CephContext *cct)
{
  rgw_usage_counters_init(cct);
}

void shutdown(CephContext *cct)
{
  rgw_usage_counters_shutdown(cct);
}

} // namespace rgw::usage_counters
