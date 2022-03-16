// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PrimaryLogScrub.h"

#include "common/scrub_types.h"
#include "osd/osd_types_fmt.h"

#include "osd/PeeringState.h"
#include "osd/PrimaryLogPG.h"
#include "scrub_machine.h"

#define dout_context (m_osds->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

using std::vector;

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
    res_inout.vals =
      m_store->get_snap_errors(m_pg->get_pgid().pool(), arg.start_after, arg.max_return);
  } else {
    res_inout.vals = m_store->get_object_errors(m_pg->get_pgid().pool(), arg.start_after,
						arg.max_return);
  }
  return true;
}

void PrimaryLogScrub::_scrub_finish()
{
  auto& info = m_pg->get_pg_info(ScrubberPasskey{});  ///< a temporary alias

  dout(10) << __func__
	   << " info stats: " << (info.stats.stats_invalid ? "invalid" : "valid")
	   << dendl;

  if (info.stats.stats_invalid) {
    m_pl_pg->recovery_state.update_stats([=](auto& history, auto& stats) {
      stats.stats = m_scrub_cstat;
      stats.stats_invalid = false;
      return false;
    });

    if (m_pl_pg->agent_state)
      m_pl_pg->agent_choose_mode();
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
	   << info.stats.stats.sum.num_objects_hit_set_archive << " hit_set_archive, "
	   << m_scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes
	   << " bytes, " << m_scrub_cstat.sum.num_objects_manifest << "/"
	   << info.stats.stats.sum.num_objects_manifest << " manifest objects, "
	   << m_scrub_cstat.sum.num_bytes_hit_set_archive << "/"
	   << info.stats.stats.sum.num_bytes_hit_set_archive << " hit_set_archive bytes."
	   << dendl;

  if (m_scrub_cstat.sum.num_objects != info.stats.stats.sum.num_objects ||
      m_scrub_cstat.sum.num_object_clones != info.stats.stats.sum.num_object_clones ||
      (m_scrub_cstat.sum.num_objects_dirty != info.stats.stats.sum.num_objects_dirty &&
       !info.stats.dirty_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_omap != info.stats.stats.sum.num_objects_omap &&
       !info.stats.omap_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_pinned != info.stats.stats.sum.num_objects_pinned &&
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
    m_osds->clog->error() << info.pgid << " " << m_mode_desc << " : stat mismatch, got "
			  << m_scrub_cstat.sum.num_objects << "/"
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
			  << " hit_set_archive, " << m_scrub_cstat.sum.num_whiteouts
			  << "/" << info.stats.stats.sum.num_whiteouts << " whiteouts, "
			  << m_scrub_cstat.sum.num_bytes << "/"
			  << info.stats.stats.sum.num_bytes << " bytes, "
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
				  bool allow_incomplete_clones)
{
  ceph_assert(head);
  if (allow_incomplete_clones) {
    dout(20) << func << " " << m_mode_desc << " " << pgid << " " << *head << " skipped "
	     << missing << " clone(s) in cache tier" << dendl;
  } else {
    clog->info() << m_mode_desc << " " << pgid << " " << *head << " : " << missing
		 << " missing clone(s)";
  }
}

int PrimaryLogScrub::process_clones_to(const std::optional<hobject_t>& head,
				       const std::optional<SnapSet>& snapset,
				       LogChannelRef clog,
				       const spg_t& pgid,
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
      clog->error() << m_mode_desc << " " << pgid << " " << *head << " : expected clone "
		    << next_clone << " " << m_missing << " missing";
      ++m_shallow_errors;
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
  dout(10) << __func__ << " num stat obj " << m_pl_pg->info.stats.stats.sum.num_objects
	   << dendl;

  auto& info = m_pl_pg->info;
  const PGPool& pool = m_pl_pg->pool;
  bool allow_incomplete_clones = pool.info.allow_incomplete_clones();

  std::optional<snapid_t> all_clones;  // Unspecified snapid_t or std::nullopt

  // traverse in reverse order.
  std::optional<hobject_t> head;
  std::optional<SnapSet> snapset;		// If initialized so will head (above)
  vector<snapid_t>::reverse_iterator curclone;	// Defined only if snapset initialized
  int missing = 0;
  inconsistent_snapset_wrapper soid_error, head_error;
  int soid_error_count = 0;

  for (auto p = scrubmap.objects.rbegin(); p != scrubmap.objects.rend(); ++p) {

    const hobject_t& soid = p->first;
    ceph_assert(!soid.is_snapdir());
    soid_error = inconsistent_snapset_wrapper{soid};
    object_stat_sum_t stat;
    std::optional<object_info_t> oi;

    stat.num_objects++;

    if (soid.nspace == m_pl_pg->cct->_conf->osd_hit_set_namespace)
      stat.num_objects_hit_set_archive++;

    if (soid.is_snap()) {
      // it's a clone
      stat.num_object_clones++;
    }

    // basic checks.
    if (p->second.attrs.count(OI_ATTR) == 0) {
      oi = std::nullopt;
      m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid << " : no '"
			    << OI_ATTR << "' attr";
      ++m_shallow_errors;
      soid_error.set_info_missing();
    } else {
      bufferlist bv;
      bv.push_back(p->second.attrs[OI_ATTR]);
      try {
	oi = object_info_t(bv);
      } catch (ceph::buffer::error& e) {
	oi = std::nullopt;
	m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid
			      << " : can't decode '" << OI_ATTR << "' attr " << e.what();
	++m_shallow_errors;
	soid_error.set_info_corrupted();
	soid_error.set_info_missing();	// Not available too
      }
    }

    if (oi) {
      if (m_pl_pg->pgbackend->be_get_ondisk_size(oi->size) != p->second.size) {
	m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid
			      << " : on disk size (" << p->second.size
			      << ") does not match object info size (" << oi->size
			      << ") adjusted for ondisk to ("
			      << m_pl_pg->pgbackend->be_get_ondisk_size(oi->size) << ")";
	soid_error.set_size_mismatch();
	++m_shallow_errors;
      }

      dout(20) << m_mode_desc << "  " << soid << " " << *oi << dendl;

      // A clone num_bytes will be added later when we have snapset
      if (!soid.is_snap()) {
	stat.num_bytes += oi->size;
      }
      if (soid.nspace == m_pl_pg->cct->_conf->osd_hit_set_namespace)
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

	dout(10) << __func__ << " " << m_mode_desc << " " << info.pgid << " new object " << soid
		 << " while processing " << *head << dendl;

	target = all_clones;
      } else {
	ceph_assert(soid.is_snap());
	target = soid.snap;
      }

      // Log any clones we were expecting to be there up to target
      // This will set missing, but will be a no-op if snap.soid == *curclone.
      missing +=
	process_clones_to(head, snapset, m_osds->clog, info.pgid,
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
	m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid
			      << " : clone ignored due to missing snapset";
      } else {
	m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid
			      << " : is an unexpected clone";
      }
      ++m_shallow_errors;
      soid_error.set_headless();
      m_store->add_snap_error(pool.id, soid_error);
      ++soid_error_count;
      if (head && soid.get_head() == head->get_head())
	head_error.set_clone(soid.snap);
      continue;
    }

    // new snapset?
    if (soid.has_snapset()) {

      if (missing) {
	log_missing(missing, head, m_osds->clog, info.pgid, __func__,
		    pool.info.allow_incomplete_clones());
      }

      // Save previous head error information
      if (head && (head_error.errors || soid_error_count))
	m_store->add_snap_error(pool.id, head_error);
      // Set this as a new head object
      head = soid;
      missing = 0;
      head_error = soid_error;
      soid_error_count = 0;

      dout(20) << __func__ << " " << m_mode_desc << " new head " << head << dendl;

      if (p->second.attrs.count(SS_ATTR) == 0) {
	m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid << " : no '"
			      << SS_ATTR << "' attr";
	++m_shallow_errors;
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
	  m_osds->clog->error()
	    << m_mode_desc << " " << info.pgid << " " << soid << " : can't decode '" << SS_ATTR
	    << "' attr " << e.what();
	  ++m_shallow_errors;
	  head_error.set_snapset_corrupted();
	}
      }

      if (snapset) {
	// what will be next?
	curclone = snapset->clones.rbegin();

	if (!snapset->clones.empty()) {
	  dout(20) << "  snapset " << *snapset << dendl;
	  if (snapset->seq == 0) {
	    m_osds->clog->error()
	      << m_mode_desc << " " << info.pgid << " " << soid << " : snaps.seq not set";
	    ++m_shallow_errors;
	    head_error.set_snapset_error();
	  }
	}
      }
    } else {
      ceph_assert(soid.is_snap());
      ceph_assert(head);
      ceph_assert(snapset);
      ceph_assert(soid.snap == *curclone);

      dout(20) << __func__ << " " << m_mode_desc << " matched clone " << soid << dendl;

      if (snapset->clone_size.count(soid.snap) == 0) {
	m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid
			      << " : is missing in clone_size";
	++m_shallow_errors;
	soid_error.set_size_mismatch();
      } else {
	if (oi && oi->size != snapset->clone_size[soid.snap]) {
	  m_osds->clog->error()
	    << m_mode_desc << " " << info.pgid << " " << soid << " : size " << oi->size
	    << " != clone_size " << snapset->clone_size[*curclone];
	  ++m_shallow_errors;
	  soid_error.set_size_mismatch();
	}

	if (snapset->clone_overlap.count(soid.snap) == 0) {
	  m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid
				<< " : is missing in clone_overlap";
	  ++m_shallow_errors;
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
	    m_osds->clog->error() << m_mode_desc << " " << info.pgid << " " << soid
				  << " : bad interval_set in clone_overlap";
	    ++m_shallow_errors;
	    soid_error.set_size_mismatch();
	  } else {
	    stat.num_bytes += snapset->get_clone_bytes(soid.snap);
	  }
	}
      }

      // what's next?
      ++curclone;
      if (soid_error.errors) {
	m_store->add_snap_error(pool.id, soid_error);
	++soid_error_count;
      }
    }
    m_scrub_cstat.add(stat);
  }

  if (doing_clones(snapset, curclone)) {
    dout(10) << __func__ << " " << m_mode_desc << " " << info.pgid
	     << " No more objects while processing " << *head << dendl;

    missing +=
      process_clones_to(head, snapset, m_osds->clog, info.pgid,
			allow_incomplete_clones, all_clones, &curclone, head_error);
  }

  // There could be missing found by the test above or even
  // before dropping out of the loop for the last head.
  if (missing) {
    log_missing(missing, head, m_osds->clog, info.pgid, __func__,
		allow_incomplete_clones);
  }
  if (head && (head_error.errors || soid_error_count))
    m_store->add_snap_error(pool.id, head_error);

  dout(20) << __func__ << " - " << missing << " (" << missing_digest.size() << ") missing"
	   << dendl;
  for (auto p = missing_digest.begin(); p != missing_digest.end(); ++p) {

    ceph_assert(!p->first.is_snapdir());
    dout(10) << __func__ << " recording digests for " << p->first << dendl;

    ObjectContextRef obc = m_pl_pg->get_object_context(p->first, false);
    if (!obc) {
      m_osds->clog->error() << info.pgid << " " << m_mode_desc
			    << " cannot get object context for object " << p->first;
      continue;
    }
    if (obc->obs.oi.soid != p->first) {
      m_osds->clog->error() << info.pgid << " " << m_mode_desc << " " << p->first
			    << " : object has a valid oi attr with a mismatched name, "
			    << " obc->obs.oi.soid: " << obc->obs.oi.soid;
      continue;
    }
    PrimaryLogPG::OpContextUPtr ctx = m_pl_pg->simple_opc_create(obc);
    ctx->at_version = m_pl_pg->get_next_version();
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
    m_pl_pg->finish_ctx(ctx.get(), pg_log_entry_t::MODIFY);

    ++num_digest_updates_pending;
    ctx->register_on_success([this]() {
      if ((num_digest_updates_pending >= 1) && (--num_digest_updates_pending == 0)) {
	m_osds->queue_scrub_digest_update(m_pl_pg, m_pl_pg->is_scrub_blocking_ops());
      }
    });

    m_pl_pg->simple_opc_submit(std::move(ctx));
  }

  dout(10) << __func__ << " (" << m_mode_desc << ") finish" << dendl;
}

PrimaryLogScrub::PrimaryLogScrub(PrimaryLogPG* pg) : PgScrubber{pg}, m_pl_pg{pg} {}

void PrimaryLogScrub::_scrub_clear_state()
{
  dout(15) << __func__ << dendl;
  m_scrub_cstat = object_stat_collection_t();
}

void PrimaryLogScrub::stats_of_handled_objects(const object_stat_sum_t& delta_stats,
					       const hobject_t& soid)
{
  // We scrub objects in hobject_t order, so objects before m_start have already been
  // scrubbed and their stats have already been added to the scrubber. Objects after that
  // point haven't been included in the scrubber's stats accounting yet, so they will be
  // included when the scrubber gets to that object.
  if (is_primary() && is_scrub_active()) {
    if (soid < m_start) {

      dout(20) << fmt::format("{} {} < [{},{})", __func__, soid, m_start, m_end) << dendl;
      m_scrub_cstat.add(delta_stats);

    } else {

      dout(25) << fmt::format("{} {} >= [{},{})", __func__, soid, m_start, m_end) << dendl;
    }
  }
}
