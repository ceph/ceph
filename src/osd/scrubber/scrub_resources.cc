// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_resources.h"

#include <fmt/format.h>

#include "common/debug.h"

#include "include/ceph_assert.h"


using ScrubResources = Scrub::ScrubResources;
using LocalResourceWrapper = Scrub::LocalResourceWrapper;

ScrubResources::ScrubResources(
    log_upwards_t log_access,
    const ceph::common::ConfigProxy& config)
    : log_upwards{log_access}
    , conf{config}
{}

std::unique_ptr<LocalResourceWrapper> ScrubResources::inc_scrubs_local(
    bool is_high_priority)
{
  std::lock_guard lck{resource_lock};
  const int total_scrubs = scrubs_local + scrubs_remote;

  if (is_high_priority || total_scrubs < conf->osd_max_scrubs) {
    ++scrubs_local;
    return std::make_unique<LocalResourceWrapper>(*this, is_high_priority);
  }
  log_upwards(fmt::format(
      "{}: {} (local) + {} (remote) >= max ({})", __func__, scrubs_local,
      scrubs_remote, conf->osd_max_scrubs));
  return nullptr;
}

void ScrubResources::dec_scrubs_local()
{
  std::lock_guard lck{resource_lock};
  log_upwards(fmt::format(
      "{}: {} -> {} (max {}, remote {})", __func__, scrubs_local,
      (scrubs_local - 1), conf->osd_max_scrubs, scrubs_remote));
  --scrubs_local;
  ceph_assert(scrubs_local >= 0);
}

bool ScrubResources::inc_scrubs_remote()
{
  std::lock_guard lck{resource_lock};
  if (scrubs_local + scrubs_remote < conf->osd_max_scrubs) {
    log_upwards(fmt::format(
	"{}: {} -> {} (max {}, local {})", __func__, scrubs_remote,
	(scrubs_remote + 1), conf->osd_max_scrubs, scrubs_local));
    ++scrubs_remote;
    return true;
  }

  log_upwards(fmt::format(
      "{}: {} (local) + {} (remote) >= max ({})", __func__, scrubs_local,
      scrubs_remote, conf->osd_max_scrubs));
  return false;
}

void ScrubResources::dec_scrubs_remote()
{
  std::lock_guard lck{resource_lock};
  log_upwards(fmt::format(
      "{}: {} -> {} (max {}, local {})", __func__, scrubs_remote,
      (scrubs_remote - 1), conf->osd_max_scrubs, scrubs_local));
  --scrubs_remote;
  ceph_assert(scrubs_remote >= 0);
}

void ScrubResources::dump_scrub_reservations(ceph::Formatter* f) const
{
  std::lock_guard lck{resource_lock};
  f->dump_int("scrubs_local", scrubs_local);
  f->dump_int("scrubs_remote", scrubs_remote);
  f->dump_int("osd_max_scrubs", conf->osd_max_scrubs);
}

// --------------- LocalResourceWrapper

Scrub::LocalResourceWrapper::LocalResourceWrapper(
    ScrubResources& resource_bookkeeper,
    bool is_high_priority)
    : m_resource_bookkeeper{resource_bookkeeper}
{}

Scrub::LocalResourceWrapper::~LocalResourceWrapper()
{
  m_resource_bookkeeper.dec_scrubs_local();
}


