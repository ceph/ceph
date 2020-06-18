// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "MissingLoc.h"

#define dout_context cct
#undef dout_prefix
#define dout_prefix (gen_prefix(*_dout))
#define dout_subsys ceph_subsys_osd

using std::set;

bool MissingLoc::readable_with_acting(
  const hobject_t &hoid,
  const set<pg_shard_t> &acting,
  eversion_t* v) const {
  if (!needs_recovery(hoid, v))
    return true;
  if (is_deleted(hoid))
    return false;
  auto missing_loc_entry = missing_loc.find(hoid);
  if (missing_loc_entry == missing_loc.end())
    return false;
  const set<pg_shard_t> &locs = missing_loc_entry->second;
  ldout(cct, 10) << __func__ << ": locs:" << locs << dendl;
  set<pg_shard_t> have_acting;
  for (auto i = locs.begin(); i != locs.end(); ++i) {
    if (acting.count(*i))
      have_acting.insert(*i);
  }
  return (*is_readable)(have_acting);
}

void MissingLoc::add_batch_sources_info(
  const set<pg_shard_t> &sources,
  HBHandle *handle)
{
  ldout(cct, 10) << __func__ << ": adding sources in batch "
		     << sources.size() << dendl;
  unsigned loop = 0;
  bool sources_updated = false;
  for (auto i = needs_recovery_map.begin();
      i != needs_recovery_map.end();
      ++i) {
    if (handle && ++loop >= cct->_conf->osd_loop_before_reset_tphandle) {
      handle->reset_tp_timeout();
      loop = 0;
    }
    if (i->second.is_delete())
      continue;

    auto p = missing_loc.find(i->first);
    if (p == missing_loc.end()) {
      p = missing_loc.emplace(i->first, set<pg_shard_t>()).first;
    } else {
      _dec_count(p->second);
    }
    missing_loc[i->first].insert(sources.begin(), sources.end());
    _inc_count(p->second);

    if (!sources_updated) {
      missing_loc_sources.insert(sources.begin(), sources.end());
      sources_updated = true;
    }
  }
}

bool MissingLoc::add_source_info(
  pg_shard_t fromosd,
  const pg_info_t &oinfo,
  const pg_missing_t &omissing,
  HBHandle *handle)
{
  bool found_missing = false;
  unsigned loop = 0;
  bool sources_updated = false;
  // found items?
  for (auto p = needs_recovery_map.begin();
       p != needs_recovery_map.end();
       ++p) {
    const hobject_t &soid(p->first);
    eversion_t need = p->second.need;
    if (handle && ++loop >= cct->_conf->osd_loop_before_reset_tphandle) {
      handle->reset_tp_timeout();
      loop = 0;
    }
    if (p->second.is_delete()) {
      ldout(cct, 10) << __func__ << " " << soid
		     << " delete, ignoring source" << dendl;
      continue;
    }
    if (oinfo.last_update < need) {
      ldout(cct, 10) << "search_for_missing " << soid << " " << need
		     << " also missing on osd." << fromosd
		     << " (last_update " << oinfo.last_update
		     << " < needed " << need << ")" << dendl;
      continue;
    }
    if (p->first >= oinfo.last_backfill) {
      // FIXME: this is _probably_ true, although it could conceivably
      // be in the undefined region!  Hmm!
      ldout(cct, 10) << "search_for_missing " << soid << " " << need
		     << " also missing on osd." << fromosd
		     << " (past last_backfill " << oinfo.last_backfill
		     << ")" << dendl;
      continue;
    }
    if (omissing.is_missing(soid)) {
      ldout(cct, 10) << "search_for_missing " << soid << " " << need
		     << " also missing on osd." << fromosd << dendl;
      continue;
    }

    ldout(cct, 10) << "search_for_missing " << soid << " " << need
		   << " is on osd." << fromosd << dendl;

    {
      auto p = missing_loc.find(soid);
      if (p == missing_loc.end()) {
	p = missing_loc.emplace(soid, set<pg_shard_t>()).first;
      } else {
	_dec_count(p->second);
      }
      p->second.insert(fromosd);
      _inc_count(p->second);
    }

    if (!sources_updated) {
      missing_loc_sources.insert(fromosd);
      sources_updated = true;
    }
    found_missing = true;
  }

  ldout(cct, 20) << "needs_recovery_map missing " << needs_recovery_map
		 << dendl;
  return found_missing;
}

void MissingLoc::check_recovery_sources(const OSDMapRef& osdmap)
{
  set<pg_shard_t> now_down;
  for (auto p = missing_loc_sources.begin();
       p != missing_loc_sources.end();
       ) {
    if (osdmap->is_up(p->osd)) {
      ++p;
      continue;
    }
    ldout(cct, 10) << __func__ << " source osd." << *p << " now down" << dendl;
    now_down.insert(*p);
    missing_loc_sources.erase(p++);
  }

  if (now_down.empty()) {
    ldout(cct, 10) << __func__ << " no source osds (" << missing_loc_sources << ") went down" << dendl;
  } else {
    ldout(cct, 10) << __func__ << " sources osds " << now_down << " now down, remaining sources are "
		       << missing_loc_sources << dendl;

    // filter missing_loc
    auto p = missing_loc.begin();
    while (p != missing_loc.end()) {
      auto q = p->second.begin();
      bool changed = false;
      while (q != p->second.end()) {
	if (now_down.count(*q)) {
	  if (!changed) {
	    changed = true;
	    _dec_count(p->second);
	  }
	  p->second.erase(q++);
	} else {
	  ++q;
	}
      }
      if (p->second.empty()) {
	missing_loc.erase(p++);
      } else {
	if (changed) {
	  _inc_count(p->second);
	}
	++p;
      }
    }
  }
}

void MissingLoc::remove_stray_recovery_sources(pg_shard_t stray)
{
  ldout(cct, 10) << __func__ << " remove osd " << stray << " from missing_loc" << dendl;
  // filter missing_loc
  auto p = missing_loc.begin();
  while (p != missing_loc.end()) {
    auto q = p->second.begin();
    bool changed = false;
    while (q != p->second.end()) {
      if (*q == stray) {
        if (!changed) {
          changed = true;
          _dec_count(p->second);
        }
        p->second.erase(q++);
      } else {
        ++q;
      }
    }
    if (p->second.empty()) {
      missing_loc.erase(p++);
    } else {
      if (changed) {
        _inc_count(p->second);
      }
      ++p;
    }
  }
  // filter missing_loc_sources
  for (auto p = missing_loc_sources.begin(); p != missing_loc_sources.end();) {
    if (*p != stray) {
      ++p;
      continue;
    }
    ldout(cct, 10) << __func__ << " remove osd" << stray << " from missing_loc_sources" << dendl;
    missing_loc_sources.erase(p++);
  }
}
