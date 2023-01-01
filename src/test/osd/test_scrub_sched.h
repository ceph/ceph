// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/// \file common objects used in scrub scheduling tests

#include <optional>

#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/osd_scrub_sched.h"
#include "osd/scrubber/scrub_queue_if.h"
#include "osd/scrubber_common.h"

struct expected_entry_t {

  std::optional<bool> tst_is_valid;
  std::optional<bool> tst_is_ripe;
  std::optional<scrub_level_t> tst_level;
  std::optional<Scrub::urgency_t> tst_urgency;
  std::optional<utime_t> tst_target_time;
  std::optional<utime_t> tst_target_time_min;
  std::optional<utime_t> tst_target_time_max;
  std::optional<utime_t> tst_nb_time;
  std::optional<utime_t> tst_nb_time_min;
  std::optional<utime_t> tst_nb_time_max;
};

struct expected_target_t {

  std::optional<expected_entry_t> tst_sched_info;
  std::optional<bool> tst_in_queue;
  std::optional<bool> tst_is_viable;
  std::optional<bool> tst_is_overdue;
  std::optional<Scrub::delay_cause_t> tst_delay_cause;
};

struct level_config_t {
  utime_t history_stamp;
  // more to come (for planned tests)
};

struct sjob_config_t {
  spg_t spg;
  bool are_stats_valid;
  Scrub::sched_conf_t sched_cnf;

  std::array<level_config_t, 2> levels;	 // 0=shallow, 1=deep
};

struct schedentry_blueprint_t {
  spg_t spg;
  scrub_level_t level;
  Scrub::urgency_t urgency;
  utime_t not_before;
  utime_t deadline;
  utime_t target;

  Scrub::SchedEntry make_entry() const
  {
    Scrub::SchedEntry e{spg, level};
    e.urgency = urgency;
    e.not_before = not_before;
    e.deadline = deadline;
    e.target = target;
    return e;
  }

  bool is_equiv(const Scrub::SchedEntry& e) const
  {
    return spg == e.pgid && level == e.level && urgency == e.urgency &&
	   not_before == e.not_before && deadline == e.deadline &&
	   target == e.target;
  }
  bool is_equiv(std::optional<Scrub::SchedEntry> maybe_e) const
  {
    return maybe_e && spg == maybe_e->pgid && level == maybe_e->level &&
	   urgency == maybe_e->urgency && not_before == maybe_e->not_before &&
	   deadline == maybe_e->deadline && target == maybe_e->target;
  }
};

struct poolopts_blueprint_t {
  std::optional<double> min_interval;
  std::optional<double> deep_interval;
  std::optional<double> max_interval;
  pool_opts_t make_conf()
  {
    pool_opts_t c;
    if (min_interval) {
      c.set(pool_opts_t::SCRUB_MIN_INTERVAL, *min_interval);
    }
    if (deep_interval) {
      c.set(pool_opts_t::DEEP_SCRUB_INTERVAL, *deep_interval);
    }
    if (max_interval) {
      c.set(pool_opts_t::SCRUB_MAX_INTERVAL, *max_interval);
    }
    return c;
  }
};