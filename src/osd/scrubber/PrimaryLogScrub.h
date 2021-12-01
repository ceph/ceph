// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

// the './' includes are marked this way to affect clang-format
#include "./pg_scrubber.h"

#include "debug.h"

#include "common/errno.h"
#include "common/scrub_types.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDScrub.h"
#include "messages/MOSDScrubReserve.h"

#include "osd/OSD.h"
#include "scrub_machine.h"

class PrimaryLogPG;

/**
 * The derivative of PgScrubber that is used by PrimaryLogPG.
 */
class PrimaryLogScrub : public PgScrubber {
 public:
  explicit PrimaryLogScrub(PrimaryLogPG* pg);

  void _scrub_finish() final;

  bool get_store_errors(const scrub_ls_arg_t& arg,
			scrub_ls_result_t& res_inout) const final;

  void stats_of_handled_objects(const object_stat_sum_t& delta_stats,
				const hobject_t& soid) final;

  // the interface used by the scrubber-backend:

  void add_to_stats(const object_stat_sum_t& stat) final;

  void submit_digest_fixes(const digests_fixes_t& fixes) final;

 private:
  // we know our PG is actually a PrimaryLogPG. Let's alias the pointer to that object:
  PrimaryLogPG* const m_pl_pg;

  // handle our part in stats collection
  object_stat_collection_t m_scrub_cstat;
  void _scrub_clear_state() final;  // which just clears the stats
};
