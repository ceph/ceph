// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/seastore_perf_counters.h"

#ifdef WITH_SEASTORE_WAF_COUNTERS

#include "common/ceph_context.h"
#include "common/perf_counters.h"
#include "common/perf_counters_key.h"

namespace crimson::os::seastore {

PerfCountersRef build_seastore_waf_logger(CephContext *cct)
{
  PerfCountersBuilder b(cct, "seastore_waf",
                        l_seastore_waf_first, l_seastore_waf_last);
  b.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);

  b.add_u64_counter(
      l_seastore_bytes_user_written,
      "bytes_user_written",
      "Logical bytes written by clients into seastore (pre journal/metadata "
      "overhead). Incremented at the seastore write entry point; the value "
      "is the ground truth denominator for write amplification.",
      "uw",
      PerfCountersBuilder::PRIO_USEFUL,
      unit_t(UNIT_BYTES));

  b.add_u64_counter(
      l_seastore_bytes_device_written,
      "bytes_device_written",
      "Physical bytes written to the underlying block device, summed across "
      "the journal record submitter, ool/metadata writer, GC rewrite path, "
      "and any journal-replay-driven writes. Numerator for write amplification.",
      "dw",
      PerfCountersBuilder::PRIO_USEFUL,
      unit_t(UNIT_BYTES));

  auto logger = PerfCountersRef{b.create_perf_counters(), cct};
  cct->get_perfcounters_collection()->add(logger.get());
  return logger;
}

}  // namespace crimson::os::seastore

#endif  // WITH_SEASTORE_WAF_COUNTERS
