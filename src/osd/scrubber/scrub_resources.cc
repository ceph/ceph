// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_resources.h"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "common/debug.h"

#include "include/ceph_assert.h"
#include "osd/osd_types_fmt.h"


using ScrubResources = Scrub::ScrubResources;

ScrubResources::ScrubResources(
    log_upwards_t log_access,
    const ceph::common::ConfigProxy& config)
    : log_upwards{log_access}
    , conf{config}
{}

bool ScrubResources::can_inc_scrubs() const
{
  std::lock_guard lck{resource_lock};
  if (scrubs_local + granted_reservations.size() < conf->osd_max_scrubs) {
    return true;
  }
  log_upwards(fmt::format(
      "{}== false. {} (local) + {} (remote) >= max ({})", __func__,
      scrubs_local, granted_reservations.size(), conf->osd_max_scrubs));
  return false;
}

bool ScrubResources::inc_scrubs_local()
{
  std::lock_guard lck{resource_lock};
  if (scrubs_local + granted_reservations.size() < conf->osd_max_scrubs) {
    ++scrubs_local;
    return true;
  }
  log_upwards(fmt::format(
      "{}: {} (local) + {} (remote) >= max ({})", __func__, scrubs_local,
      granted_reservations.size(), conf->osd_max_scrubs));
  return false;
}

void ScrubResources::dec_scrubs_local()
{
  std::lock_guard lck{resource_lock};
  log_upwards(fmt::format(
      "{}: {} -> {} (max {}, remote {})", __func__, scrubs_local,
      (scrubs_local - 1), conf->osd_max_scrubs, granted_reservations.size()));
  --scrubs_local;
  ceph_assert(scrubs_local >= 0);
}

bool ScrubResources::inc_scrubs_remote(pg_t pgid)
{
  std::lock_guard lck{resource_lock};

  // if this PG is already reserved - it's probably a benign bug.
  // report it, but do not fail the reservation.
  if (granted_reservations.contains(pgid)) {
    log_upwards(fmt::format("{}: pg[{}] already reserved", __func__, pgid));
    return true;
  }

  auto prev = granted_reservations.size();
  if (scrubs_local + prev < conf->osd_max_scrubs) {
    granted_reservations.insert(pgid);
    log_upwards(fmt::format(
	"{}: pg[{}] {} -> {} (max {}, local {})", __func__, pgid, prev,
	granted_reservations.size(), conf->osd_max_scrubs, scrubs_local));
    return true;
  }

  log_upwards(fmt::format(
      "{}: pg[{}] {} (local) + {} (remote) >= max ({})", __func__, pgid,
      scrubs_local, granted_reservations.size(), conf->osd_max_scrubs));
  return false;
}

void ScrubResources::dec_scrubs_remote(pg_t pgid)
{
  std::lock_guard lck{resource_lock};
  // we might not have this PG in the set (e.g. if we are concluding a
  // high priority scrub, one that does not require reservations)
  auto cnt = granted_reservations.erase(pgid);
  if (cnt) {
    log_upwards(fmt::format(
	"{}: remote reservation for {} removed -> {} (max {}, local {})",
	__func__, pgid, granted_reservations.size(), conf->osd_max_scrubs,
	scrubs_local));
  }
}

void ScrubResources::dump_scrub_reservations(ceph::Formatter* f) const
{
  std::lock_guard lck{resource_lock};
  f->dump_int("scrubs_local", scrubs_local);
  f->dump_int("granted_reservations", granted_reservations.size());
  f->dump_string("PGs being served", fmt::format("{}", granted_reservations));
  f->dump_int("osd_max_scrubs", conf->osd_max_scrubs);
}
