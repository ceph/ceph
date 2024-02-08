// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_resources.h"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "common/debug.h"

#include "include/ceph_assert.h"
#include "osd/osd_types_fmt.h"


using ScrubResources = Scrub::ScrubResources;
using LocalResourceWrapper = Scrub::LocalResourceWrapper;

ScrubResources::ScrubResources(
    log_upwards_t log_access,
    const ceph::common::ConfigProxy& config)
    : log_upwards{log_access}
    , conf{config}
{}

// ------------------------- scrubbing as primary on this OSD -----------------

// can we increase the number of concurrent scrubs performed by Primaries
// on this OSD? note that counted separately from the number of scrubs
// performed by replicas.
bool ScrubResources::can_inc_scrubs() const
{
  std::lock_guard lck{resource_lock};
  return can_inc_local_scrubs_unlocked();
}

std::unique_ptr<LocalResourceWrapper> ScrubResources::inc_scrubs_local(
    bool is_high_priority)
{
  std::lock_guard lck{resource_lock};
  if (is_high_priority || can_inc_local_scrubs_unlocked()) {
    ++scrubs_local;
    log_upwards(fmt::format(
	"{}: {} -> {} (max {})", __func__, (scrubs_local - 1), scrubs_local,
	conf->osd_max_scrubs));
    return std::make_unique<LocalResourceWrapper>(*this);
  }
  return nullptr;
}

bool ScrubResources::can_inc_local_scrubs_unlocked() const
{
  if (scrubs_local < conf->osd_max_scrubs) {
    return true;
  }
  log_upwards(fmt::format(
      "{}: Cannot add local scrubs. Current counter ({}) >= max ({})", __func__,
      scrubs_local, conf->osd_max_scrubs));
  return false;
}

void ScrubResources::dec_scrubs_local()
{
  std::lock_guard lck{resource_lock};
  log_upwards(fmt::format(
      "{}:  {} -> {} (max {})", __func__, scrubs_local, (scrubs_local - 1),
      conf->osd_max_scrubs));
  --scrubs_local;
  ceph_assert(scrubs_local >= 0);
}


void ScrubResources::dump_scrub_reservations(ceph::Formatter* f) const
{
  std::lock_guard lck{resource_lock};
  f->dump_int("scrubs_local", scrubs_local);
  f->dump_int("osd_max_scrubs", conf->osd_max_scrubs);
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

