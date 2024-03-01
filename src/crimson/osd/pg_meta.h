// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <tuple>
#include <seastar/core/future.hh>
#include "osd/osd_types.h"
#include "crimson/os/futurized_store.h"

/// PG related metadata
class PGMeta
{
  crimson::os::FuturizedStore::Shard& store;
  const spg_t pgid;
public:
  PGMeta(crimson::os::FuturizedStore::Shard& store, spg_t pgid);
  seastar::future<epoch_t> get_epoch();
  seastar::future<std::tuple<pg_info_t, PastIntervals>> load();
};
