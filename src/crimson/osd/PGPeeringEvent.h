// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <boost/statechart/event.hpp>

#include "osd/osd_types.h"
/// what we need to instantiate a pg
struct PGCreateInfo {
  spg_t pgid;
  epoch_t epoch = 0;
  pg_history_t history;
  PastIntervals past_intervals;
  bool by_mon;
  PGCreateInfo(spg_t p, epoch_t e,
	       const pg_history_t& h,
	       const PastIntervals& pi,
	       bool mon)
    : pgid(p), epoch(e), history(h), past_intervals(pi), by_mon(mon) {}
};
