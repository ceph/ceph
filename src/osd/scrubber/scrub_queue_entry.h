// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <compare>
#include <string_view>

#include "include/utime.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

namespace Scrub {

/**
 * Possible urgency levels for a specific scheduling target (shallow or deep):
 *
 * (note: the 'urgency' attribute conveys both the relative priority for
 * scheduling and the behavior of the scrub). The urgency levels are:
 *                    ^^^^^^^^^^^^^^^^^^^^^
 *
 * periodic scrubs:
 * ---------------
 *
 * 'periodic_regular' - the "standard" shallow/deep scrub performed
 *      periodically on each PG.
 *
 *
 * priority scrubs (termed 'required' or 'must' in the legacy code):
 * ---------------------------------------------------------------
 * In order of ascending priority:
 *
 * 'must_scrub' - the PG info is not valid (i.e. we do not have a valid
 *     'last-scrub' stamp). A high-priority shallow scrub is required.
 *
 * 'after_repair' - triggered immediately after a recovery process
 *   ('m_after_repair_scrub_required' was set).
 *   This type of scrub is always deep.
 *   (note: this urgency level is not implemented in this commit)
 *
 * 'repairing' - the target is currently being deep-scrubbed with the repair
 *   flag set. Triggered by a previous shallow scrub that ended with errors.
 *
 * 'operator_requested' - the target was manually requested for scrubbing by
 *   an administrator.
 *
 * 'must_repair' - the target is required to be deep-scrubbed with the
 *   repair flag set, initiated by a message specifying 'do_repair'.
 */
enum class urgency_t {
  periodic_regular,
  must_scrub,
  after_repair,
  repairing,
  operator_requested,
  must_repair,
};

/**
 * SchedEntry holds the scheduling details for scrubbing a specific PG at
 * a specific scrub level. Namely - it identifies the [pg,level] combination,
 * the 'urgency' attribute of the scheduled scrub (which determines most of
 * its behavior and scheduling decisions) and the actual time attributes
 * for scheduling (target, deadline, not_before).
 *
 * In this commit - the 'urgency' attribute is not fully used yet, and some
 * of the scrub behavior is still controlled by the 'planned scrub' flags.
 */
struct SchedEntry {
  constexpr SchedEntry(spg_t pgid, scrub_level_t level)
      : pgid{pgid}
      , level{level}
  {}

  SchedEntry(const SchedEntry&) = default;
  SchedEntry(SchedEntry&&) = default;
  SchedEntry& operator=(const SchedEntry&) = default;
  SchedEntry& operator=(SchedEntry&&) = default;

  spg_t pgid;
  scrub_level_t level;

  urgency_t urgency{urgency_t::periodic_regular};

  /// scheduled_at, not-before & the deadline times
  Scrub::scrub_schedule_t schedule;

  /// either 'none', or the reason for the latest failure/delay (for
  /// logging/reporting purposes)
  delay_cause_t last_issue{delay_cause_t::none};
};


static inline std::weak_ordering cmp_ripe_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r) noexcept
{
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  // the 'utime_t' operator<=> is 'partial_ordering', it seems.
  if (auto cmp = std::weak_order(
	  double(l.schedule.scheduled_at), double(r.schedule.scheduled_at));
      cmp != 0) {
    return cmp;
  }
  if (r.level < l.level) {
    return std::weak_ordering::less;
  }
  if (auto cmp = std::weak_order(
	  double(l.schedule.not_before), double(r.schedule.not_before));
      cmp != 0) {
    return cmp;
  }
  return std::weak_ordering::greater;
}

static inline std::weak_ordering cmp_future_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r) noexcept
{
  if (auto cmp = std::weak_order(
	  double(l.schedule.not_before), double(r.schedule.not_before));
      cmp != 0) {
    return cmp;
  }
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  if (auto cmp = std::weak_order(
	  double(l.schedule.scheduled_at), double(r.schedule.scheduled_at));
      cmp != 0) {
    return cmp;
  }
  if (r.level < l.level) {
    return std::weak_ordering::less;
  }
  return std::weak_ordering::greater;
}

static inline std::weak_ordering cmp_entries(
    utime_t t,
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r) noexcept
{
  bool l_ripe = l.schedule.not_before <= t;
  bool r_ripe = r.schedule.not_before <= t;
  if (l_ripe) {
    if (r_ripe) {
      return cmp_ripe_entries(l, r);
    }
    return std::weak_ordering::less;
  }
  if (r_ripe) {
    return std::weak_ordering::greater;
  }
  return cmp_future_entries(l, r);
}

// ---  the interface required by 'not_before_queue_t':

static inline const utime_t& project_not_before(const Scrub::SchedEntry& e)
{
  return e.schedule.not_before;
}

static inline const spg_t& project_removal_class(const Scrub::SchedEntry& e)
{
  return e.pgid;
}


/// 'not_before_queue_t' requires a '<' operator, to be used for
/// eligible entries:
static inline bool operator<(
    const Scrub::SchedEntry& lhs,
    const Scrub::SchedEntry& rhs)
{
  return cmp_ripe_entries(lhs, rhs) == std::weak_ordering::less;
}

}  // namespace Scrub


namespace fmt {

// clang-format off
template <>
struct formatter<Scrub::urgency_t> : formatter<std::string_view> {
  template <typename FormatContext>
  auto format(Scrub::urgency_t urg, FormatContext& ctx) const
  {
    using enum Scrub::urgency_t;
    std::string_view desc;
    switch (urg) {
      case periodic_regular:    desc = "periodic-regular"; break;
      case must_scrub:          desc = "must-scrub"; break;
      case after_repair:        desc = "after-repair"; break;
      case repairing:           desc = "repairing"; break;
      case operator_requested:  desc = "operator-requested"; break;
      case must_repair:         desc = "must-repair"; break;
      // better to not have a default case, so that the compiler will warn
    }
    return formatter<string_view>::format(desc, ctx);
  }
};
// clang-format on

template <>
struct formatter<Scrub::SchedEntry> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedEntry& st, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "{}/{},nb:{:s},({},tr:{:s},dl:{:s})", st.pgid,
	(st.level == scrub_level_t::deep ? "dp" : "sh"), st.schedule.not_before,
	st.urgency, st.schedule.scheduled_at, st.schedule.deadline);
  }
};
}  // namespace fmt
