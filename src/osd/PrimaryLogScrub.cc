// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PrimaryLogScrub.h"

#include "common/scrub_types.h"

#include "PeeringState.h"
#include "PrimaryLogPG.h"
#include "scrub_machine.h"


#define dout_context (pg_->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "primLogScrb.pg(" << pg_->pg_id << ") "

using namespace Scrub;
using Scrub::ScrubMachine;

bool PrimaryLogScrub::get_store_errors(const scrub_ls_arg_t& arg,
				       scrub_ls_result_t& res_inout)
{
  if (!store_) {
    return false;
  }

  if (arg.get_snapsets) {
    res_inout.vals = store_->get_snap_errors(osds_->store, pg_->get_pgid().pool(),
					     arg.start_after, arg.max_return);
  } else {
    res_inout.vals = store_->get_object_errors(osds_->store, pg_->get_pgid().pool(),
					       arg.start_after, arg.max_return);
  }
  return true;
}


void PrimaryLogScrub::_scrub_finish()
{
  auto& info = pg_->info;  ///< a temporary alias

  dout(11) << __func__ << " info stats invalid? "
	   << (info.stats.stats_invalid ? "inv" : "vld") << dendl;

  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));

  if (info.stats.stats_invalid) {
    pl_pg_->recovery_state.update_stats([=](auto& history, auto& stats) {
      stats.stats = scrub_cstat;
      stats.stats_invalid = false;
      return false;
    });

    if (pl_pg_->agent_state)
      pl_pg_->agent_choose_mode();
  }

  dout(10) << mode << " got " << scrub_cstat.sum.num_objects << "/"
	   << info.stats.stats.sum.num_objects << " objects, "
	   << scrub_cstat.sum.num_object_clones << "/"
	   << info.stats.stats.sum.num_object_clones << " clones, "
	   << scrub_cstat.sum.num_objects_dirty << "/"
	   << info.stats.stats.sum.num_objects_dirty << " dirty, "
	   << scrub_cstat.sum.num_objects_omap << "/"
	   << info.stats.stats.sum.num_objects_omap << " omap, "
	   << scrub_cstat.sum.num_objects_pinned << "/"
	   << info.stats.stats.sum.num_objects_pinned << " pinned, "
	   << scrub_cstat.sum.num_objects_hit_set_archive << "/"
	   << info.stats.stats.sum.num_objects_hit_set_archive << " hit_set_archive, "
	   << scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes
	   << " bytes, " << scrub_cstat.sum.num_objects_manifest << "/"
	   << info.stats.stats.sum.num_objects_manifest << " manifest objects, "
	   << scrub_cstat.sum.num_bytes_hit_set_archive << "/"
	   << info.stats.stats.sum.num_bytes_hit_set_archive << " hit_set_archive bytes."
	   << dendl;

  if (scrub_cstat.sum.num_objects != info.stats.stats.sum.num_objects ||
      scrub_cstat.sum.num_object_clones != info.stats.stats.sum.num_object_clones ||
      (scrub_cstat.sum.num_objects_dirty != info.stats.stats.sum.num_objects_dirty &&
       !info.stats.dirty_stats_invalid) ||
      (scrub_cstat.sum.num_objects_omap != info.stats.stats.sum.num_objects_omap &&
       !info.stats.omap_stats_invalid) ||
      (scrub_cstat.sum.num_objects_pinned != info.stats.stats.sum.num_objects_pinned &&
       !info.stats.pin_stats_invalid) ||
      (scrub_cstat.sum.num_objects_hit_set_archive !=
	 info.stats.stats.sum.num_objects_hit_set_archive &&
       !info.stats.hitset_stats_invalid) ||
      (scrub_cstat.sum.num_bytes_hit_set_archive !=
	 info.stats.stats.sum.num_bytes_hit_set_archive &&
       !info.stats.hitset_bytes_stats_invalid) ||
      (scrub_cstat.sum.num_objects_manifest !=
	 info.stats.stats.sum.num_objects_manifest &&
       !info.stats.manifest_stats_invalid) ||
      scrub_cstat.sum.num_whiteouts != info.stats.stats.sum.num_whiteouts ||
      scrub_cstat.sum.num_bytes != info.stats.stats.sum.num_bytes) {
    osds_->clog->error() << info.pgid << " " << mode << " : stat mismatch, got "
			 << scrub_cstat.sum.num_objects << "/"
			 << info.stats.stats.sum.num_objects << " objects, "
			 << scrub_cstat.sum.num_object_clones << "/"
			 << info.stats.stats.sum.num_object_clones << " clones, "
			 << scrub_cstat.sum.num_objects_dirty << "/"
			 << info.stats.stats.sum.num_objects_dirty << " dirty, "
			 << scrub_cstat.sum.num_objects_omap << "/"
			 << info.stats.stats.sum.num_objects_omap << " omap, "
			 << scrub_cstat.sum.num_objects_pinned << "/"
			 << info.stats.stats.sum.num_objects_pinned << " pinned, "
			 << scrub_cstat.sum.num_objects_hit_set_archive << "/"
			 << info.stats.stats.sum.num_objects_hit_set_archive
			 << " hit_set_archive, " << scrub_cstat.sum.num_whiteouts << "/"
			 << info.stats.stats.sum.num_whiteouts << " whiteouts, "
			 << scrub_cstat.sum.num_bytes << "/"
			 << info.stats.stats.sum.num_bytes << " bytes, "
			 << scrub_cstat.sum.num_objects_manifest << "/"
			 << info.stats.stats.sum.num_objects_manifest
			 << " manifest objects, "
			 << scrub_cstat.sum.num_bytes_hit_set_archive << "/"
			 << info.stats.stats.sum.num_bytes_hit_set_archive
			 << " hit_set_archive bytes.";
    ++shallow_errors;

    if (repair) {
      ++fixed_count;
      pl_pg_->recovery_state.update_stats([this](auto& history, auto& stats) {
	stats.stats = scrub_cstat;
	stats.dirty_stats_invalid = false;
	stats.omap_stats_invalid = false;
	stats.hitset_stats_invalid = false;
	stats.hitset_bytes_stats_invalid = false;
	stats.pin_stats_invalid = false;
	stats.manifest_stats_invalid = false;
	return false;
      });
      pl_pg_->publish_stats_to_osd();
      pl_pg_->recovery_state.share_pg_info();
    }
  }
  // Clear object context cache to get repair information
  if (repair)
    pl_pg_->object_contexts.clear();
}

static bool doing_clones(const std::optional<SnapSet>& snapset,
			 const vector<snapid_t>::reverse_iterator& curclone)
{
  return snapset && curclone != snapset->clones.rend();
}



void PrimaryLogScrub::log_missing(int missing,
				  const std::optional<hobject_t>& head,
				  LogChannelRef clog,
				  const spg_t& pgid,
				  const char* func,
				  const char* mode,
				  bool allow_incomplete_clones)
{
  ceph_assert(head);
  if (allow_incomplete_clones) {
    dout(20) << func << " " << mode << " " << pgid << " " << *head << " skipped "
	     << missing << " clone(s) in cache tier" << dendl;
  } else {
    clog->info() << mode << " " << pgid << " " << *head << " : " << missing
		 << " missing clone(s)";
  }
}



int PrimaryLogScrub::process_clones_to(const std::optional<hobject_t>& head,
				       const std::optional<SnapSet>& snapset,
				       LogChannelRef clog,
				       const spg_t& pgid,
				       const char* mode,
				       bool allow_incomplete_clones,
				       std::optional<snapid_t> target,
				       vector<snapid_t>::reverse_iterator* curclone,
				       inconsistent_snapset_wrapper& e)
{
  ceph_assert(head);
  ceph_assert(snapset);
  int missing_count = 0;

  // NOTE: clones are in descending order, thus **curclone > target test here
  hobject_t next_clone(*head);
  while (doing_clones(snapset, *curclone) && (!target || **curclone > *target)) {

    ++missing_count;
    // it is okay to be missing one or more clones in a cache tier.
    // skip higher-numbered clones in the list.
    if (!allow_incomplete_clones) {
      next_clone.snap = **curclone;
      clog->error() << mode << " " << pgid << " " << *head << " : expected clone "
		    << next_clone << " " << missing << " missing";
      ++shallow_errors;
      e.set_clone_missing(next_clone.snap);
    }
    // Clones are descending
    ++(*curclone);
  }
  return missing_count;
}


/*
 * Validate consistency of the object info and snap sets.
 *
 * We are sort of comparing 2 lists. The main loop is on objmap.objects. But
 * the comparison of the objects is against multiple snapset.clones. There are
 * multiple clone lists and in between lists we expect head.
 *
 * Example
 *
 * objects              expected
 * =======              =======
 * obj1 snap 1          head, unexpected obj1 snap 1
 * obj2 head            head, match
 *              [SnapSet clones 6 4 2 1]
 * obj2 snap 7          obj2 snap 6, unexpected obj2 snap 7
 * obj2 snap 6          obj2 snap 6, match
 * obj2 snap 4          obj2 snap 4, match
 * obj3 head            obj2 snap 2 (expected), obj2 snap 1 (expected), match
 *              [Snapset clones 3 1]
 * obj3 snap 3          obj3 snap 3 match
 * obj3 snap 1          obj3 snap 1 match
 * obj4 head            head, match
 *              [Snapset clones 4]
 * EOL                  obj4 snap 4, (expected)
 */
void PrimaryLogScrub::scrub_snapshot_metadata(ScrubMap& scrubmap,
					      const missing_map_t& missing_digest)
{
  dout(10) << __func__ << " num stat obj " << pl_pg_->info.stats.stats.sum.num_objects
	   << dendl;

  auto& info = pl_pg_->info;
  const PGPool& pool = pl_pg_->pool;  // pl_pg_->get_pool();
  // auto& pool = pl_pg_->recovery_state.pool;
  bool allow_incomplete_clones = pool.info.allow_incomplete_clones();


  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));
  dout(10) << __func__ << " - " << mode << dendl;

  std::optional<snapid_t> all_clones;  // Unspecified snapid_t or std::nullopt

  // traverse in reverse order.
  std::optional<hobject_t> head;
  std::optional<SnapSet> snapset;		// If initialized so will head (above)
  vector<snapid_t>::reverse_iterator curclone;	// Defined only if snapset initialized
  int missing = 0;
  inconsistent_snapset_wrapper soid_error, head_error;
  int soid_error_count = 0;

  for (auto p = scrubmap.objects.rbegin(); p != scrubmap.objects.rend(); ++p) {

    /// \todo RRR make this into a range-loop

    const hobject_t& soid = p->first;
    ceph_assert(!soid.is_snapdir());
    soid_error = inconsistent_snapset_wrapper{soid};
    object_stat_sum_t stat;
    std::optional<object_info_t> oi;

    stat.num_objects++;

    if (soid.nspace == pl_pg_->cct->_conf->osd_hit_set_namespace)
      stat.num_objects_hit_set_archive++;

    if (soid.is_snap()) {
      // it's a clone
      stat.num_object_clones++;
    }

    // basic checks.
    if (p->second.attrs.count(OI_ATTR) == 0) {
      oi = std::nullopt;
      osds_->clog->error() << mode << " " << info.pgid << " " << soid << " : no '"
			   << OI_ATTR << "' attr";
      ++shallow_errors;
      soid_error.set_info_missing();
    } else {
      bufferlist bv;
      bv.push_back(p->second.attrs[OI_ATTR]);
      try {
	oi = object_info_t();  // Initialize optional<> before decode into it
	oi->decode(bv);
      } catch (ceph::buffer::error& e) {
	oi = std::nullopt;
	osds_->clog->error() << mode << " " << info.pgid << " " << soid
			     << " : can't decode '" << OI_ATTR << "' attr " << e.what();
	++shallow_errors;
	soid_error.set_info_corrupted();
	soid_error.set_info_missing();	// Not available too
      }
    }

    if (oi) {
      if (pl_pg_->pgbackend->be_get_ondisk_size(oi->size) != p->second.size) {
	osds_->clog->error() << mode << " " << info.pgid << " " << soid
			     << " : on disk size (" << p->second.size
			     << ") does not match object info size (" << oi->size
			     << ") adjusted for ondisk to ("
			     << pl_pg_->pgbackend->be_get_ondisk_size(oi->size) << ")";
	soid_error.set_size_mismatch();
	++shallow_errors;
      }

      dout(20) << mode << "  " << soid << " " << *oi << dendl;

      // A clone num_bytes will be added later when we have snapset
      if (!soid.is_snap()) {
	stat.num_bytes += oi->size;
      }
      if (soid.nspace == pl_pg_->cct->_conf->osd_hit_set_namespace)
	stat.num_bytes_hit_set_archive += oi->size;

      if (oi->is_dirty())
	++stat.num_objects_dirty;
      if (oi->is_whiteout())
	++stat.num_whiteouts;
      if (oi->is_omap())
	++stat.num_objects_omap;
      if (oi->is_cache_pinned())
	++stat.num_objects_pinned;
      if (oi->has_manifest())
	++stat.num_objects_manifest;
    }

    // Check for any problems while processing clones
    if (doing_clones(snapset, curclone)) {
      std::optional<snapid_t> target;
      // Expecting an object with snap for current head
      if (soid.has_snapset() || soid.get_head() != head->get_head()) {

	dout(10) << __func__ << " " << mode << " " << info.pgid << " new object " << soid
		 << " while processing " << *head << dendl;

	target = all_clones;
      } else {
	ceph_assert(soid.is_snap());
	target = soid.snap;
      }

      // Log any clones we were expecting to be there up to target
      // This will set missing, but will be a no-op if snap.soid == *curclone.
      missing +=
	process_clones_to(head, snapset, osds_->clog, info.pgid, mode,
			  allow_incomplete_clones, target, &curclone, head_error);
    }

    bool expected;
    // Check doing_clones() again in case we ran process_clones_to()
    if (doing_clones(snapset, curclone)) {
      // A head would have processed all clones above
      // or all greater than *curclone.
      ceph_assert(soid.is_snap() && *curclone <= soid.snap);

      // After processing above clone snap should match the expected curclone
      expected = (*curclone == soid.snap);
    } else {
      // If we aren't doing clones any longer, then expecting head
      expected = soid.has_snapset();
    }
    if (!expected) {
      // If we couldn't read the head's snapset, just ignore clones
      if (head && !snapset) {
	osds_->clog->error() << mode << " " << info.pgid << " " << soid
			     << " : clone ignored due to missing snapset";
      } else {
	osds_->clog->error() << mode << " " << info.pgid << " " << soid
			     << " : is an unexpected clone";
      }
      ++shallow_errors;
      soid_error.set_headless();
      store_->add_snap_error(pool.id, soid_error);
      ++soid_error_count;
      if (head && soid.get_head() == head->get_head())
	head_error.set_clone(soid.snap);
      continue;
    }

    // new snapset?
    if (soid.has_snapset()) {

      if (missing) {
	log_missing(missing, head, osds_->clog, info.pgid, __func__, mode,
		    pool.info.allow_incomplete_clones());
      }

      // Save previous head error information
      if (head && (head_error.errors || soid_error_count))
	store_->add_snap_error(pool.id, head_error);
      // Set this as a new head object
      head = soid;
      missing = 0;
      head_error = soid_error;
      soid_error_count = 0;

      dout(20) << __func__ << " " << mode << " new head " << head << dendl;

      if (p->second.attrs.count(SS_ATTR) == 0) {
	osds_->clog->error() << mode << " " << info.pgid << " " << soid << " : no '"
			     << SS_ATTR << "' attr";
	++shallow_errors;
	snapset = std::nullopt;
	head_error.set_snapset_missing();
      } else {
	bufferlist bl;
	bl.push_back(p->second.attrs[SS_ATTR]);
	auto blp = bl.cbegin();
	try {
	  snapset = SnapSet();	// Initialize optional<> before decoding into it
	  decode(*snapset, blp);
	  head_error.ss_bl.push_back(p->second.attrs[SS_ATTR]);
	} catch (ceph::buffer::error& e) {
	  snapset = std::nullopt;
	  osds_->clog->error() << mode << " " << info.pgid << " " << soid
			       << " : can't decode '" << SS_ATTR << "' attr " << e.what();
	  ++shallow_errors;
	  head_error.set_snapset_corrupted();
	}
      }

      if (snapset) {
	// what will be next?
	curclone = snapset->clones.rbegin();

	if (!snapset->clones.empty()) {
	  dout(20) << "  snapset " << *snapset << dendl;
	  if (snapset->seq == 0) {
	    osds_->clog->error()
	      << mode << " " << info.pgid << " " << soid << " : snaps.seq not set";
	    ++shallow_errors;
	    head_error.set_snapset_error();
	  }
	}
      }
    } else {
      ceph_assert(soid.is_snap());
      ceph_assert(head);
      ceph_assert(snapset);
      ceph_assert(soid.snap == *curclone);

      dout(20) << __func__ << " " << mode << " matched clone " << soid << dendl;

      if (snapset->clone_size.count(soid.snap) == 0) {
	osds_->clog->error() << mode << " " << info.pgid << " " << soid
			     << " : is missing in clone_size";
	++shallow_errors;
	soid_error.set_size_mismatch();
      } else {
	if (oi && oi->size != snapset->clone_size[soid.snap]) {
	  osds_->clog->error() << mode << " " << info.pgid << " " << soid << " : size "
			       << oi->size << " != clone_size "
			       << snapset->clone_size[*curclone];
	  ++shallow_errors;
	  soid_error.set_size_mismatch();
	}

	if (snapset->clone_overlap.count(soid.snap) == 0) {
	  osds_->clog->error() << mode << " " << info.pgid << " " << soid
			       << " : is missing in clone_overlap";
	  ++shallow_errors;
	  soid_error.set_size_mismatch();
	} else {
	  // This checking is based on get_clone_bytes().  The first 2 asserts
	  // can't happen because we know we have a clone_size and
	  // a clone_overlap.  Now we check that the interval_set won't
	  // cause the last assert.
	  uint64_t size = snapset->clone_size.find(soid.snap)->second;
	  const interval_set<uint64_t>& overlap =
	    snapset->clone_overlap.find(soid.snap)->second;
	  bool bad_interval_set = false;
	  for (interval_set<uint64_t>::const_iterator i = overlap.begin();
	       i != overlap.end(); ++i) {
	    if (size < i.get_len()) {
	      bad_interval_set = true;
	      break;
	    }
	    size -= i.get_len();
	  }

	  if (bad_interval_set) {
	    osds_->clog->error() << mode << " " << info.pgid << " " << soid
				 << " : bad interval_set in clone_overlap";
	    ++shallow_errors;
	    soid_error.set_size_mismatch();
	  } else {
	    stat.num_bytes += snapset->get_clone_bytes(soid.snap);
	  }
	}
      }

      // what's next?
      ++curclone;
      if (soid_error.errors) {
	store_->add_snap_error(pool.id, soid_error);
	++soid_error_count;
      }
    }
    // dout(10) << __func__ << stat.num_objects << " ---RRR --- " <<
    // scrub_cstat.sum.num_objects << dendl;
    scrub_cstat.add(stat);
    // dout(10) << __func__ << stat.num_objects << " ---RRR --- " <<
    // scrub_cstat.sum.num_objects << dendl;
  }

  // dout(10) << __func__ << dendl;

  if (doing_clones(snapset, curclone)) {
    dout(10) << __func__ << " " << mode << " " << info.pgid
	     << " No more objects while processing " << *head << dendl;

    missing +=
      process_clones_to(head, snapset, osds_->clog, info.pgid, mode,
			allow_incomplete_clones, all_clones, &curclone, head_error);
  }
  // There could be missing found by the test above or even
  // before dropping out of the loop for the last head.
  if (missing) {
    log_missing(missing, head, osds_->clog, info.pgid, __func__, mode,
		allow_incomplete_clones);
  }
  if (head && (head_error.errors || soid_error_count))
    store_->add_snap_error(pool.id, head_error);

  dout(10) << __func__ << " - " << missing << " (" << missing_digest.size() << ") missing"
	   << dendl;
  for (auto p = missing_digest.begin(); p != missing_digest.end(); ++p) {

    ceph_assert(!p->first.is_snapdir());
    dout(10) << __func__ << " recording digests for " << p->first << dendl;

    ObjectContextRef obc = pl_pg_->get_object_context(p->first, false);
    if (!obc) {
      osds_->clog->error() << info.pgid << " " << mode
			   << " cannot get object context for object " << p->first;
      continue;
    } else if (obc->obs.oi.soid != p->first) {
      osds_->clog->error() << info.pgid << " " << mode << " " << p->first
			   << " : object has a valid oi attr with a mismatched name, "
			   << " obc->obs.oi.soid: " << obc->obs.oi.soid;
      continue;
    }
    std::unique_ptr<PrimaryLogPG::OpContext> ctx = pl_pg_->simple_opc_create(obc);
    ctx->at_version = pl_pg_->get_next_version();
    ctx->mtime = utime_t();  // do not update mtime
    if (p->second.first) {
      ctx->new_obs.oi.set_data_digest(*p->second.first);
    } else {
      ctx->new_obs.oi.clear_data_digest();
    }
    if (p->second.second) {
      ctx->new_obs.oi.set_omap_digest(*p->second.second);
    } else {
      ctx->new_obs.oi.clear_omap_digest();
    }
    pl_pg_->finish_ctx(ctx.get(), pg_log_entry_t::MODIFY);

    ctx->register_on_success([this]() {
      dout(7 /* 20 */) << "updating scrub digest " << num_digest_updates_pending << dendl;
      if (--num_digest_updates_pending <= 0) {
	osds_->queue_scrub_digest_update(pl_pg_, pl_pg_->ops_blocked_by_scrub());
      }
    });

    ++num_digest_updates_pending;  // RRR why not move to before the _submit?
    pl_pg_->simple_opc_submit(std::move(ctx));
  }

  dout(10) << __func__ << " (" << mode << ") finish" << dendl;
}

PrimaryLogScrub::PrimaryLogScrub(PrimaryLogPG* pg)
    : PgScrubber{static_cast<PG*>(pg)}, pl_pg_{pg}, backend_{pl_pg_->get_pgbackend()}
{}

void PrimaryLogScrub::_scrub_clear_state()
{
  dout(7) << __func__ << " - pg(" << pl_pg_->pg_id << dendl;
  scrub_cstat = object_stat_collection_t();
}

void PrimaryLogScrub::add_stats_if_lower(const object_stat_sum_t& delta_stats,
					 const hobject_t& soid)
{
  // RRR not sure I understand the idea here...

  dout(8) << __func__ << " " << soid << " a? " << is_scrub_active() << dendl;
  if (is_primary() && is_scrub_active()) {

    if (soid < start_) {
      dout(8 /*20*/) << __func__ << " " << soid << " < [" << start_ << "," << end_ << ")"
		     << dendl;
      scrub_cstat.add(delta_stats);
    } else {
      dout(8 /*20*/) << __func__ << " " << soid << " >= [" << start_ << "," << end_ << ")"
		     << dendl;
    }
  }
}

bool PrimaryLogScrub::should_requeue_blocked_ops(eversion_t last_recovery_applied)
{
  if (!is_scrub_active()) {
    // just verify that things indeed are quiet
    ceph_assert(start_ == end_);
    return false;
  }

  return last_recovery_applied >= subset_last_update;  // RRR ask why >= and not >
}
