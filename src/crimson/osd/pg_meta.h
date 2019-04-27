// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include "osd/osd_types.h"

namespace ceph::os {
  class CyanStore;
}

/// PG related metadata
class PGMeta
{
  ceph::os::CyanStore* store;
  const spg_t pgid;
public:
  PGMeta(ceph::os::CyanStore *store, spg_t pgid);
  seastar::future<epoch_t> get_epoch();
  seastar::future<pg_info_t, PastIntervals> load();
};
