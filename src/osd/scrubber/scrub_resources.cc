// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "./scrub_resources.h"

#include <fmt/format.h>
#include <fmt/ranges.h>
#ifdef WITH_CRIMSON
#include "crimson/common/log.h"
#define Scrub crimson::osd::scrub
SET_SUBSYS(osd);
#else
#include "common/debug.h"
#endif
#include "include/ceph_assert.h"
#include "osd/osd_types_fmt.h"


using ScrubResources = Scrub::ScrubResources;
using LocalResourceWrapper = Scrub::LocalResourceWrapper;

#ifndef WITH_CRIMSON
ScrubResources::ScrubResources(
    log_upwards_t log_access,
    const ceph::common::ConfigProxy& config)
    : log_upwards{log_access}
    , conf{config}
{}
#endif
// ------------------------- scrubbing as primary on this OSD -----------------

// can we increase the number of concurrent scrubs performed by Primaries
// on this OSD? note that counted separately from the number of scrubs
// performed by replicas.
bool ScrubResources::can_inc_scrubs() const
{
#ifndef WITH_CRIMSON
  std::lock_guard lck{resource_lock};
#endif
  return can_inc_local_scrubs_unlocked();
}

std::unique_ptr<LocalResourceWrapper> ScrubResources::inc_scrubs_local(
    bool is_high_priority)
{
#ifndef WITH_CRIMSON
  std::lock_guard lck{resource_lock};
#endif
  if (is_high_priority || can_inc_local_scrubs_unlocked()) {
    ++scrubs_local;
#ifndef WITH_CRIMSON
    log_upwards(fmt::format(
	"{}: {} -> {} (max {})", __func__, (scrubs_local - 1), scrubs_local,
	conf->osd_max_scrubs));
#else
  LOG_PREFIX(ScrubResources::inc_scrubs_local);
  DEBUG(
        "{} -> {} (max {})", "", (scrubs_local - 1),
        scrubs_local,
        crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs")/seastar::smp::count);
#endif
    return std::make_unique<LocalResourceWrapper>(*this);
  }
  return nullptr;
}

bool ScrubResources::can_inc_local_scrubs_unlocked() const
{
#ifdef WITH_CRIMSON
  LOG_PREFIX(ScrubResources::can_inc_scrubs);
  if (scrubs_local < crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs")/seastar::smp::count) {
    return true;
  }
  DEBUG(
      "Cannot add local scrubs. Current counter ({}) >= max ({})", "",
      scrubs_local,
      crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs")/seastar::smp::count);
#else
  if (scrubs_local < conf->osd_max_scrubs) {
    return true;
  }
  log_upwards(fmt::format(
      "{}: Cannot add local scrubs. Current counter ({}) >= max ({})", __func__,
      scrubs_local, conf->osd_max_scrubs));
#endif
  return false;
}

void ScrubResources::dec_scrubs_local()
{
#ifndef WITH_CRIMSON
   std::lock_guard lck{resource_lock};
   log_upwards(fmt::format(
       "{}:  {} -> {} (max {})", __func__, scrubs_local, (scrubs_local - 1),
       conf->osd_max_scrubs));
#else
  LOG_PREFIX(ScrubResources::dec_scrubs_local);
  DEBUG(
      "{} -> {} (max {})", "", scrubs_local,
      (scrubs_local - 1),
      crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs")/seastar::smp::count);
#endif
  --scrubs_local;
  ceph_assert(scrubs_local >= 0);
}


void ScrubResources::dump_scrub_reservations(ceph::Formatter* f) const
{
#ifndef WITH_CRIMSON
  std::lock_guard lck{resource_lock};
#endif
  f->dump_int("scrubs_local", scrubs_local);
#ifdef WITH_CRIMSON
f->dump_int("osd_max_scrubs", crimson::common::local_conf().get_val<int64_t>("osd_max_scrubs"));
#else
  f->dump_int("osd_max_scrubs", conf->osd_max_scrubs);
#endif
}

// --------------- LocalResourceWrapper

Scrub::LocalResourceWrapper::LocalResourceWrapper(
    ScrubResources& resource_bookkeeper)
    : m_resource_bookkeeper{resource_bookkeeper}
{}

Scrub::LocalResourceWrapper::~LocalResourceWrapper()
{
  m_resource_bookkeeper.dec_scrubs_local();
}

