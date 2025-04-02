// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./PrimaryLogScrub.h"

#include <sstream>

#include "common/debug.h"
#include "common/scrub_types.h"
#include "osd/PeeringState.h"
#include "osd/PrimaryLogPG.h"
#include "osd/osd_types_fmt.h"

#include "scrub_machine.h"

#define dout_context (m_osds->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

template <class T>
static ostream& _prefix(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}

using namespace Scrub;

bool PrimaryLogScrub::get_store_errors(const scrub_ls_arg_t& arg,
				       scrub_ls_result_t& res_inout) const
{
  if (!m_store) {
    return false;
  }

  if (arg.get_snapsets) {
    res_inout.vals = m_store->get_snap_errors(m_pg->get_pgid().pool(),
					      arg.start_after,
					      arg.max_return);
  } else {
    res_inout.vals = m_store->get_object_errors(m_pg->get_pgid().pool(),
						arg.start_after,
						arg.max_return);
  }
  return true;
}

/// \todo combine the multiple transactions into a single one
void PrimaryLogScrub::submit_digest_fixes(const digests_fixes_t& fixes)
{
  // note: the following line was modified from '+=' to '=', as we should not
  // encounter previous-chunk digest updates after starting a new chunk
  num_digest_updates_pending = fixes.size();
  dout(10) << __func__
	   << ": num_digest_updates_pending: " << num_digest_updates_pending
	   << dendl;

  for (auto& [obj, dgs] : fixes) {

    ObjectContextRef obc = m_pl_pg->get_object_context(obj, false);
    if (!obc) {
      m_osds->clog->error() << m_pg_id << " " << m_mode_desc
			    << " cannot get object context for object " << obj;
      num_digest_updates_pending--;
      continue;
    }
    dout(15) << fmt::format(
		  "{}: {}, pg[{}] {}/{}", __func__, num_digest_updates_pending,
		  m_pg_id, obj, dgs)
	     << dendl;
    if (obc->obs.oi.soid != obj) {
      m_osds->clog->error()
	<< m_pg_id << " " << m_mode_desc << " " << obj
	<< " : object has a valid oi attr with a mismatched name, "
	<< " obc->obs.oi.soid: " << obc->obs.oi.soid;
      num_digest_updates_pending--;
      continue;
    }

    PrimaryLogPG::OpContextUPtr ctx = m_pl_pg->simple_opc_create(obc);
    ctx->at_version = m_pl_pg->get_next_version();
    ctx->mtime = utime_t();  // do not update mtime
    if (dgs.first) {
      ctx->new_obs.oi.set_data_digest(*dgs.first);
    } else {
      ctx->new_obs.oi.clear_data_digest();
    }
    if (dgs.second) {
      ctx->new_obs.oi.set_omap_digest(*dgs.second);
    } else {
      ctx->new_obs.oi.clear_omap_digest();
    }
    m_pl_pg->finish_ctx(ctx.get(), pg_log_entry_t::MODIFY);


    ctx->register_on_success([this]() {
      if ((num_digest_updates_pending >= 1) &&
	  (--num_digest_updates_pending == 0)) {
	m_osds->queue_scrub_digest_update(m_pl_pg,
					  m_pl_pg->is_scrub_blocking_ops());
      }
    });

    m_pl_pg->simple_opc_submit(std::move(ctx));
  }
}

//  a forwarder used by the scrubber backend

void PrimaryLogScrub::add_to_stats(const object_stat_sum_t& stat)
{
  m_scrub_cstat.add(stat);
}


void PrimaryLogScrub::_scrub_finish()
{
  auto& info = m_pg->get_pg_info(ScrubberPasskey{});  ///< a temporary alias

  dout(10) << __func__ << " info stats: "
	   << (info.stats.stats_invalid ? "invalid" : "valid")
	   << " m_is_repair: " << m_is_repair << dendl;

  if (info.stats.stats_invalid) {
    m_pl_pg->recovery_state.update_stats([=, this](auto& history, auto& stats) {
      stats.stats = m_scrub_cstat;
      stats.stats_invalid = false;
      return false;
    });

    if (m_pl_pg->agent_state) {
      m_pl_pg->agent_choose_mode();
    }
  }

  dout(10) << m_mode_desc << " got " << m_scrub_cstat.sum.num_objects << "/"
	   << info.stats.stats.sum.num_objects << " objects, "
	   << m_scrub_cstat.sum.num_object_clones << "/"
	   << info.stats.stats.sum.num_object_clones << " clones, "
	   << m_scrub_cstat.sum.num_objects_dirty << "/"
	   << info.stats.stats.sum.num_objects_dirty << " dirty, "
	   << m_scrub_cstat.sum.num_objects_omap << "/"
	   << info.stats.stats.sum.num_objects_omap << " omap, "
	   << m_scrub_cstat.sum.num_objects_pinned << "/"
	   << info.stats.stats.sum.num_objects_pinned << " pinned, "
	   << m_scrub_cstat.sum.num_objects_hit_set_archive << "/"
	   << info.stats.stats.sum.num_objects_hit_set_archive
	   << " hit_set_archive, " << m_scrub_cstat.sum.num_bytes << "/"
	   << info.stats.stats.sum.num_bytes << " bytes, "
	   << m_scrub_cstat.sum.num_objects_manifest << "/"
	   << info.stats.stats.sum.num_objects_manifest << " manifest objects, "
	   << m_scrub_cstat.sum.num_bytes_hit_set_archive << "/"
	   << info.stats.stats.sum.num_bytes_hit_set_archive
	   << " hit_set_archive bytes." << dendl;

  if (m_scrub_cstat.sum.num_objects != info.stats.stats.sum.num_objects ||
      m_scrub_cstat.sum.num_object_clones !=
	info.stats.stats.sum.num_object_clones ||
      (m_scrub_cstat.sum.num_objects_dirty !=
	 info.stats.stats.sum.num_objects_dirty &&
       !info.stats.dirty_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_omap !=
	 info.stats.stats.sum.num_objects_omap &&
       !info.stats.omap_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_pinned !=
	 info.stats.stats.sum.num_objects_pinned &&
       !info.stats.pin_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_hit_set_archive !=
	 info.stats.stats.sum.num_objects_hit_set_archive &&
       !info.stats.hitset_stats_invalid) ||
      (m_scrub_cstat.sum.num_bytes_hit_set_archive !=
	 info.stats.stats.sum.num_bytes_hit_set_archive &&
       !info.stats.hitset_bytes_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_manifest !=
	 info.stats.stats.sum.num_objects_manifest &&
       !info.stats.manifest_stats_invalid) ||
      m_scrub_cstat.sum.num_whiteouts != info.stats.stats.sum.num_whiteouts ||
      m_scrub_cstat.sum.num_bytes != info.stats.stats.sum.num_bytes) {

    m_osds->clog->error() << info.pgid << " " << m_mode_desc
			  << " : stat mismatch, got "
			  << m_scrub_cstat.sum.num_objects << "/"
			  << info.stats.stats.sum.num_objects << " objects, "
			  << m_scrub_cstat.sum.num_object_clones << "/"
			  << info.stats.stats.sum.num_object_clones
			  << " clones, " << m_scrub_cstat.sum.num_objects_dirty
			  << "/" << info.stats.stats.sum.num_objects_dirty
			  << " dirty, " << m_scrub_cstat.sum.num_objects_omap
			  << "/" << info.stats.stats.sum.num_objects_omap
			  << " omap, " << m_scrub_cstat.sum.num_objects_pinned
			  << "/" << info.stats.stats.sum.num_objects_pinned
			  << " pinned, "
			  << m_scrub_cstat.sum.num_objects_hit_set_archive
			  << "/"
			  << info.stats.stats.sum.num_objects_hit_set_archive
			  << " hit_set_archive, "
			  << m_scrub_cstat.sum.num_whiteouts << "/"
			  << info.stats.stats.sum.num_whiteouts
			  << " whiteouts, " << m_scrub_cstat.sum.num_bytes
			  << "/" << info.stats.stats.sum.num_bytes << " bytes, "
			  << m_scrub_cstat.sum.num_objects_manifest << "/"
			  << info.stats.stats.sum.num_objects_manifest
			  << " manifest objects, "
			  << m_scrub_cstat.sum.num_bytes_hit_set_archive << "/"
			  << info.stats.stats.sum.num_bytes_hit_set_archive
			  << " hit_set_archive bytes.";
    ++m_shallow_errors;

    if (m_is_repair) {
      ++m_fixed_count;
      m_pl_pg->recovery_state.update_stats([this](auto& history, auto& stats) {
	stats.stats = m_scrub_cstat;
	stats.dirty_stats_invalid = false;
	stats.omap_stats_invalid = false;
	stats.hitset_stats_invalid = false;
	stats.hitset_bytes_stats_invalid = false;
	stats.pin_stats_invalid = false;
	stats.manifest_stats_invalid = false;
	return false;
      });
      m_pl_pg->publish_stats_to_osd();
      m_pl_pg->recovery_state.share_pg_info();
    }
  }
  // Clear object context cache to get repair information
  if (m_is_repair)
    m_pl_pg->object_contexts.clear();
}

PrimaryLogScrub::PrimaryLogScrub(PrimaryLogPG* pg) : PgScrubber{pg}, m_pl_pg{pg}
{}

void PrimaryLogScrub::_scrub_clear_state()
{
  m_scrub_cstat = object_stat_collection_t();
}

void PrimaryLogScrub::stats_of_handled_objects(
  const object_stat_sum_t& delta_stats,
  const hobject_t& soid)
{
  // We scrub objects in hobject_t order, so objects before m_start have already
  // been scrubbed and their stats have already been added to the scrubber.
  // Objects after that point haven't been included in the scrubber's stats
  // accounting yet, so they will be included when the scrubber gets to that
  // object.
  if (is_primary() && is_scrub_active()) {
    if (soid < m_start) {

      dout(20) << fmt::format("{} {} < [{},{})", __func__, soid, m_start, m_end)
	       << dendl;
      m_scrub_cstat.add(delta_stats);

    } else {

      dout(25)
	<< fmt::format("{} {} >= [{},{})", __func__, soid, m_start, m_end)
	<< dendl;
    }
  }
}
