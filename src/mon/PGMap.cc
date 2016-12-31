// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PGMap.h"

#define dout_subsys ceph_subsys_mon
#include "common/debug.h"
#include "common/TextTable.h"
#include "include/stringify.h"
#include "common/Formatter.h"
#include "include/ceph_features.h"
#include "mon/MonitorDBStore.h"

// --

void PGMap::Incremental::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MONENC) == 0) {
    __u8 v = 4;
    ::encode(v, bl);
    ::encode(version, bl);
    ::encode(pg_stat_updates, bl);
    ::encode(osd_stat_updates, bl);
    ::encode(osd_stat_rm, bl);
    ::encode(osdmap_epoch, bl);
    ::encode(pg_scan, bl);
    ::encode(full_ratio, bl);
    ::encode(nearfull_ratio, bl);
    ::encode(pg_remove, bl);
    return;
  }

  ENCODE_START(6, 5, bl);
  ::encode(version, bl);
  ::encode(pg_stat_updates, bl);
  ::encode(osd_stat_updates, bl);
  ::encode(osd_stat_rm, bl);
  ::encode(osdmap_epoch, bl);
  ::encode(pg_scan, bl);
  ::encode(full_ratio, bl);
  ::encode(nearfull_ratio, bl);
  ::encode(pg_remove, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}

void PGMap::Incremental::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
  ::decode(version, bl);
  if (struct_v < 3) {
    pg_stat_updates.clear();
    __u32 n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_t pgid = opgid;
      ::decode(pg_stat_updates[pgid], bl);
    }
  } else {
    ::decode(pg_stat_updates, bl);
  }
  ::decode(osd_stat_updates, bl);
  ::decode(osd_stat_rm, bl);
  ::decode(osdmap_epoch, bl);
  ::decode(pg_scan, bl);
  if (struct_v >= 2) {
    ::decode(full_ratio, bl);
    ::decode(nearfull_ratio, bl);
  }
  if (struct_v < 3) {
    pg_remove.clear();
    __u32 n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_remove.insert(pg_t(opgid));
    }
  } else {
    ::decode(pg_remove, bl);
  }
  if (struct_v < 4 && full_ratio == 0) {
    full_ratio = -1;
  }
  if (struct_v < 4 && nearfull_ratio == 0) {
    nearfull_ratio = -1;
  }
  if (struct_v >= 6)
    ::decode(stamp, bl);
  DECODE_FINISH(bl);
}

void PGMap::Incremental::dump(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("osdmap_epoch", osdmap_epoch);
  f->dump_unsigned("pg_scan_epoch", pg_scan);
  f->dump_float("full_ratio", full_ratio);
  f->dump_float("nearfull_ratio", nearfull_ratio);

  f->open_array_section("pg_stat_updates");
  for (map<pg_t,pg_stat_t>::const_iterator p = pg_stat_updates.begin(); p != pg_stat_updates.end(); ++p) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << p->first;
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_stat_updates");
  for (map<int32_t,osd_stat_t>::const_iterator p = osd_stat_updates.begin(); p != osd_stat_updates.end(); ++p) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();

  f->open_array_section("osd_stat_removals");
  for (set<int>::const_iterator p = osd_stat_rm.begin(); p != osd_stat_rm.end(); ++p)
    f->dump_int("osd", *p);
  f->close_section();

  f->open_array_section("pg_removals");
  for (set<pg_t>::const_iterator p = pg_remove.begin(); p != pg_remove.end(); ++p)
    f->dump_stream("pgid") << *p;
  f->close_section();
}

void PGMap::Incremental::generate_test_instances(list<PGMap::Incremental*>& o)
{
  o.push_back(new Incremental);
  o.push_back(new Incremental);
  o.back()->version = 1;
  o.back()->stamp = utime_t(123,345);
  o.push_back(new Incremental);
  o.back()->version = 2;
  o.back()->pg_stat_updates[pg_t(1,2,3)] = pg_stat_t();
  o.back()->osd_stat_updates[5] = osd_stat_t();
  o.push_back(new Incremental);
  o.back()->version = 3;
  o.back()->osdmap_epoch = 1;
  o.back()->pg_scan = 2;
  o.back()->full_ratio = .2;
  o.back()->nearfull_ratio = .3;
  o.back()->pg_stat_updates[pg_t(4,5,6)] = pg_stat_t();
  o.back()->osd_stat_updates[6] = osd_stat_t();
  o.back()->pg_remove.insert(pg_t(1,2,3));
  o.back()->osd_stat_rm.insert(5);
}


// --

void PGMap::apply_incremental(CephContext *cct, const Incremental& inc)
{
  assert(inc.version == version+1);
  version++;

  utime_t delta_t;
  delta_t = inc.stamp;
  delta_t -= stamp;
  stamp = inc.stamp;

  pool_stat_t pg_sum_old = pg_sum;

  bool ratios_changed = false;
  if (inc.full_ratio != full_ratio && inc.full_ratio != -1) {
    full_ratio = inc.full_ratio;
    ratios_changed = true;
  }
  if (inc.nearfull_ratio != nearfull_ratio && inc.nearfull_ratio != -1) {
    nearfull_ratio = inc.nearfull_ratio;
    ratios_changed = true;
  }
  if (ratios_changed)
    redo_full_sets();

  for (map<pg_t,pg_stat_t>::const_iterator p = inc.pg_stat_updates.begin();
       p != inc.pg_stat_updates.end();
       ++p) {
    const pg_t &update_pg(p->first);
    const pg_stat_t &update_stat(p->second);

    hash_map<pg_t,pg_stat_t>::iterator t = pg_stat.find(update_pg);
    if (t == pg_stat.end()) {
      hash_map<pg_t,pg_stat_t>::value_type v(update_pg, update_stat);
      pg_stat.insert(v);
    } else {
      stat_pg_sub(update_pg, t->second);
      t->second = update_stat;
    }
    stat_pg_add(update_pg, update_stat);
  }
  for (map<int32_t,osd_stat_t>::const_iterator p = inc.osd_stat_updates.begin();
       p != inc.osd_stat_updates.end();
       ++p) {
    int osd = p->first;
    const osd_stat_t &new_stats(p->second);
    
    hash_map<int32_t,osd_stat_t>::iterator t = osd_stat.find(osd);
    if (t == osd_stat.end()) {
      hash_map<int32_t,osd_stat_t>::value_type v(osd, new_stats);
      osd_stat.insert(v);
    } else {
      stat_osd_sub(t->second);
      t->second = new_stats;
    }

    stat_osd_add(new_stats);
    
    // adjust [near]full status
    register_nearfull_status(osd, new_stats);
  }
  for (set<pg_t>::const_iterator p = inc.pg_remove.begin();
       p != inc.pg_remove.end();
       ++p) {
    const pg_t &removed_pg(*p);
    hash_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(removed_pg);
    if (s != pg_stat.end()) {
      stat_pg_sub(removed_pg, s->second);
      pg_stat.erase(s);
    }
  }
  
  for (set<int>::iterator p = inc.osd_stat_rm.begin();
       p != inc.osd_stat_rm.end();
       ++p) {
    hash_map<int32_t,osd_stat_t>::iterator t = osd_stat.find(*p);
    if (t != osd_stat.end()) {
      stat_osd_sub(t->second);
      osd_stat.erase(t);
    }

    // remove these old osds from full/nearfull set(s), too
    nearfull_osds.erase(*p);
    full_osds.erase(*p);
  }

  // calculate a delta, and average over the last 2 deltas.
  pool_stat_t d = pg_sum;
  d.stats.sub(pg_sum_old.stats);
  pg_sum_deltas.push_back(make_pair(d, delta_t));
  stamp_delta += delta_t;

  pg_sum_delta.stats.add(d.stats);
  if (pg_sum_deltas.size() > (std::list< pair<pool_stat_t, utime_t> >::size_type)MAX(1, cct ? cct->_conf->mon_stat_smooth_intervals : 1)) {
    pg_sum_delta.stats.sub(pg_sum_deltas.front().first.stats);
    stamp_delta -= pg_sum_deltas.front().second;
    pg_sum_deltas.pop_front();
  }
  
  if (inc.osdmap_epoch)
    last_osdmap_epoch = inc.osdmap_epoch;
  if (inc.pg_scan)
    last_pg_scan = inc.pg_scan;
}

void PGMap::redo_full_sets()
{
  full_osds.clear();
  nearfull_osds.clear();
  for (hash_map<int32_t, osd_stat_t>::iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    register_nearfull_status(i->first, i->second);
  }
}

void PGMap::register_nearfull_status(int osd, const osd_stat_t& s)
{
  float ratio = ((float)s.kb_used) / ((float)s.kb);

  if (full_ratio > 0 && ratio > full_ratio) {
    // full
    full_osds.insert(osd);
    nearfull_osds.erase(osd);
  } else if (nearfull_ratio > 0 && ratio > nearfull_ratio) {
    // nearfull
    full_osds.erase(osd);
    nearfull_osds.insert(osd);
  } else {
    // ok
    full_osds.erase(osd);
    nearfull_osds.erase(osd);
  }
}

void PGMap::calc_stats()
{
  num_pg_by_state.clear();
  num_pg = 0;
  num_osd = 0;
  pg_pool_sum.clear();
  pg_sum = pool_stat_t();
  osd_sum = osd_stat_t();

  for (hash_map<pg_t,pg_stat_t>::iterator p = pg_stat.begin();
       p != pg_stat.end();
       ++p) {
    stat_pg_add(p->first, p->second);
  }
  for (hash_map<int32_t,osd_stat_t>::iterator p = osd_stat.begin();
       p != osd_stat.end();
       ++p)
    stat_osd_add(p->second);

  redo_full_sets();
}

void PGMap::update_pg(pg_t pgid, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  hash_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(pgid);
  epoch_t old_lec = 0;
  if (s != pg_stat.end()) {
    old_lec = s->second.get_effective_last_epoch_clean();
    stat_pg_sub(pgid, s->second);
  }
  pg_stat_t& r = pg_stat[pgid];
  ::decode(r, p);
  stat_pg_add(pgid, r);

  epoch_t lec = r.get_effective_last_epoch_clean();
  if (min_last_epoch_clean &&
      (lec < min_last_epoch_clean ||  // we did
       (lec > min_last_epoch_clean && // we might
	old_lec == min_last_epoch_clean)
       ))
    min_last_epoch_clean = 0;
}

void PGMap::remove_pg(pg_t pgid)
{
  hash_map<pg_t,pg_stat_t>::iterator s = pg_stat.find(pgid);
  if (s != pg_stat.end()) {
    if (min_last_epoch_clean &&
	s->second.get_effective_last_epoch_clean() == min_last_epoch_clean)
      min_last_epoch_clean = 0;
    stat_pg_sub(pgid, s->second);
    pg_stat.erase(s);
  }
}

void PGMap::update_osd(int osd, bufferlist& bl)
{
  bufferlist::iterator p = bl.begin();
  hash_map<int32_t,osd_stat_t>::iterator o = osd_stat.find(osd);
  epoch_t old_lec = 0;
  if (o != osd_stat.end()) {
    ceph::unordered_map<int32_t,epoch_t>::iterator i = osd_epochs.find(osd);
    if (i != osd_epochs.end())
      old_lec = i->second;
    stat_osd_sub(o->second);
  }
  osd_stat_t& r = osd_stat[osd];
  ::decode(r, p);
  stat_osd_add(r);

  // adjust [near]full status
  register_nearfull_status(osd, r);

  // epoch?
  if (!p.end()) {
    epoch_t e;
    ::decode(e, p);

    if (e < min_last_epoch_clean ||
	(e > min_last_epoch_clean &&
	 old_lec == min_last_epoch_clean))
      min_last_epoch_clean = 0;
  } else {
    // WARNING: we are not refreshing min_last_epoch_clean!  must be old store
    // or old mon running.
  }
}

void PGMap::remove_osd(int osd)
{
  hash_map<int32_t,osd_stat_t>::iterator o = osd_stat.find(osd);
  if (o != osd_stat.end()) {
    stat_osd_sub(o->second);
    osd_stat.erase(o);

    // remove these old osds from full/nearfull set(s), too
    nearfull_osds.erase(osd);
    full_osds.erase(osd);
  }
}

void PGMap::stat_pg_add(const pg_t &pgid, const pg_stat_t &s)
{
  num_pg++;
  num_pg_by_state[s.state]++;
  pg_pool_sum[pgid.pool()].add(s);
  pg_sum.add(s);
  if (s.state & PG_STATE_CREATING) {
    creating_pgs.insert(pgid);
    if (s.acting.size())
      creating_pgs_by_osd[s.acting[0]].insert(pgid);
  }
}

void PGMap::stat_pg_sub(const pg_t &pgid, const pg_stat_t &s)
{
  num_pg--;
  if (--num_pg_by_state[s.state] == 0)
    num_pg_by_state.erase(s.state);

  pool_stat_t& ps = pg_pool_sum[pgid.pool()];
  ps.sub(s);
  if (ps.is_zero())
    pg_pool_sum.erase(pgid.pool());

  pg_sum.sub(s);
  if (s.state & PG_STATE_CREATING) {
    creating_pgs.erase(pgid);
    if (s.acting.size()) {
      creating_pgs_by_osd[s.acting[0]].erase(pgid);
      if (creating_pgs_by_osd[s.acting[0]].size() == 0)
        creating_pgs_by_osd.erase(s.acting[0]);
    }
  }
}

void PGMap::stat_osd_add(const osd_stat_t &s)
{
  num_osd++;
  osd_sum.add(s);
}

void PGMap::stat_osd_sub(const osd_stat_t &s)
{
  num_osd--;
  osd_sum.sub(s);
}

epoch_t PGMap::calc_min_last_epoch_clean() const
{
  if (pg_stat.empty())
    return 0;
  hash_map<pg_t,pg_stat_t>::const_iterator p = pg_stat.begin();
  epoch_t min = p->second.get_effective_last_epoch_clean();
  for (++p; p != pg_stat.end(); ++p) {
    epoch_t lec = p->second.get_effective_last_epoch_clean();
    if (lec < min)
      min = lec;
  }
  return min;
}

void PGMap::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MONENC) == 0) {
    __u8 v = 3;
    ::encode(v, bl);
    ::encode(version, bl);
    ::encode(pg_stat, bl);
    ::encode(osd_stat, bl);
    ::encode(last_osdmap_epoch, bl);
    ::encode(last_pg_scan, bl);
    ::encode(full_ratio, bl);
    ::encode(nearfull_ratio, bl);
    return;
  }

  ENCODE_START(5, 4, bl);
  ::encode(version, bl);
  ::encode(pg_stat, bl);
  ::encode(osd_stat, bl);
  ::encode(last_osdmap_epoch, bl);
  ::encode(last_pg_scan, bl);
  ::encode(full_ratio, bl);
  ::encode(nearfull_ratio, bl);
  ::encode(stamp, bl);
  ENCODE_FINISH(bl);
}

void PGMap::decode(bufferlist::iterator &bl)
{
  DECODE_START_LEGACY_COMPAT_LEN(5, 4, 4, bl);
  ::decode(version, bl);
  if (struct_v < 3) {
    pg_stat.clear();
    __u32 n;
    ::decode(n, bl);
    while (n--) {
      old_pg_t opgid;
      ::decode(opgid, bl);
      pg_t pgid = opgid;
      ::decode(pg_stat[pgid], bl);
    }
  } else {
    ::decode(pg_stat, bl);
  }
  ::decode(osd_stat, bl);
  ::decode(last_osdmap_epoch, bl);
  ::decode(last_pg_scan, bl);
  if (struct_v >= 2) {
    ::decode(full_ratio, bl);
    ::decode(nearfull_ratio, bl);
  }
  if (struct_v >= 5)
    ::decode(stamp, bl);
  DECODE_FINISH(bl);

  calc_stats();
}

void PGMap::dirty_all(Incremental& inc)
{
  inc.osdmap_epoch = last_osdmap_epoch;
  inc.pg_scan = last_pg_scan;
  inc.full_ratio = full_ratio;
  inc.nearfull_ratio = nearfull_ratio;

  for (hash_map<pg_t,pg_stat_t>::const_iterator p = pg_stat.begin(); p != pg_stat.end(); ++p) {
    inc.pg_stat_updates[p->first] = p->second;
  }
  for (hash_map<int32_t, osd_stat_t>::const_iterator p = osd_stat.begin(); p != osd_stat.end(); ++p) {
    inc.osd_stat_updates[p->first] = p->second;
  }
}

void PGMap::dump(Formatter *f) const
{
  dump_basic(f);
  dump_pg_stats(f, false);
  dump_pool_stats(f);
  dump_osd_stats(f);
}

void PGMap::dump_basic(Formatter *f) const
{
  f->dump_unsigned("version", version);
  f->dump_stream("stamp") << stamp;
  f->dump_unsigned("last_osdmap_epoch", last_osdmap_epoch);
  f->dump_unsigned("last_pg_scan", last_pg_scan);
  f->dump_float("full_ratio", full_ratio);
  f->dump_float("near_full_ratio", nearfull_ratio);
  
  f->open_object_section("pg_stats_sum");
  pg_sum.dump(f);
  f->close_section();

  f->open_object_section("pg_stats_delta");
  pg_sum_delta.dump(f);
  f->close_section();
  
  f->open_object_section("osd_stats_sum");
  osd_sum.dump(f);
  f->close_section();
}

void PGMap::dump_pg_stats(Formatter *f, bool brief) const
{
  f->open_array_section("pg_stats");
  for (hash_map<pg_t,pg_stat_t>::const_iterator i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << i->first;
    if (brief)
      i->second.dump_brief(f);
    else
      i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_pool_stats(Formatter *f) const
{
  f->open_array_section("pool_stats");
  for (hash_map<int,pool_stat_t>::const_iterator p = pg_pool_sum.begin();
       p != pg_pool_sum.end();
       ++p) {
    f->open_object_section("pool_stat");
    f->dump_int("poolid", p->first);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_osd_stats(Formatter *f) const
{
  f->open_array_section("osd_stats");
  for (hash_map<int32_t,osd_stat_t>::const_iterator q = osd_stat.begin();
       q != osd_stat.end();
       ++q) {
    f->open_object_section("osd_stat");
    f->dump_int("osd", q->first);
    q->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_pg_stats_plain(ostream& ss,
				const hash_map<pg_t, pg_stat_t>& pg_stats) const
{
  ss << "pg_stat\tobjects\tmip\tdegr\tunf\tbytes\tlog\tdisklog\tstate\tstate_stamp\tv\treported\tup\tacting\tlast_scrub\tscrub_stamp\tlast_deep_scrub\tdeep_scrub_stamp" << std::endl;
  for (hash_map<pg_t, pg_stat_t>::const_iterator i = pg_stats.begin();
       i != pg_stats.end(); ++i) {
    const pg_stat_t &st(i->second);
    ss << i->first
       << "\t" << st.stats.sum.num_objects
      //<< "\t" << st.num_object_copies
       << "\t" << st.stats.sum.num_objects_missing_on_primary
       << "\t" << st.stats.sum.num_objects_degraded
       << "\t" << st.stats.sum.num_objects_unfound
       << "\t" << st.stats.sum.num_bytes
       << "\t" << st.log_size
       << "\t" << st.ondisk_log_size
       << "\t" << pg_state_string(st.state)
       << "\t" << st.last_change
       << "\t" << st.version
       << "\t" << st.reported_epoch << ":" << st.reported_seq
       << "\t" << st.up
       << "\t" << st.acting
       << "\t" << st.last_scrub << "\t" << st.last_scrub_stamp
       << "\t" << st.last_deep_scrub << "\t" << st.last_deep_scrub_stamp
       << std::endl;
  }
}

void PGMap::dump(ostream& ss) const
{
  ss << "version " << version << std::endl;
  ss << "stamp " << stamp << std::endl;
  ss << "last_osdmap_epoch " << last_osdmap_epoch << std::endl;
  ss << "last_pg_scan " << last_pg_scan << std::endl;
  ss << "full_ratio " << full_ratio << std::endl;
  ss << "nearfull_ratio " << nearfull_ratio << std::endl;
  dump_pg_stats_plain(ss, pg_stat);
  for (hash_map<int,pool_stat_t>::const_iterator p = pg_pool_sum.begin();
       p != pg_pool_sum.end();
       ++p)
    ss << "pool " << p->first
       << "\t" << p->second.stats.sum.num_objects
      //<< "\t" << p->second.num_object_copies
       << "\t" << p->second.stats.sum.num_objects_missing_on_primary
       << "\t" << p->second.stats.sum.num_objects_degraded
       << "\t" << p->second.stats.sum.num_objects_unfound
       << "\t" << p->second.stats.sum.num_bytes
       << "\t" << p->second.log_size
       << "\t" << p->second.ondisk_log_size
       << std::endl;
  ss << " sum\t" << pg_sum.stats.sum.num_objects
    //<< "\t" << pg_sum.num_object_copies
     << "\t" << pg_sum.stats.sum.num_objects_missing_on_primary
     << "\t" << pg_sum.stats.sum.num_objects_degraded
     << "\t" << pg_sum.stats.sum.num_objects_unfound
     << "\t" << pg_sum.stats.sum.num_bytes
     << "\t" << pg_sum.log_size
     << "\t" << pg_sum.ondisk_log_size
     << std::endl;
  ss << "osdstat\tkbused\tkbavail\tkb\thb in\thb out" << std::endl;
  for (hash_map<int32_t,osd_stat_t>::const_iterator p = osd_stat.begin();
       p != osd_stat.end();
       ++p)
    ss << p->first
       << "\t" << p->second.kb_used
       << "\t" << p->second.kb_avail 
       << "\t" << p->second.kb
       << "\t" << p->second.hb_in
       << "\t" << p->second.hb_out
       << std::endl;
  ss << " sum\t" << osd_sum.kb_used
     << "\t" << osd_sum.kb_avail 
     << "\t" << osd_sum.kb
     << std::endl;
}

void PGMap::get_stuck_stats(PGMap::StuckPG type, utime_t cutoff,
			    hash_map<pg_t, pg_stat_t>& stuck_pgs) const
{
  for (hash_map<pg_t, pg_stat_t>::const_iterator i = pg_stat.begin();
       i != pg_stat.end();
       ++i) {
    utime_t val;
    switch (type) {
    case STUCK_INACTIVE:
      if (i->second.state & PG_STATE_ACTIVE)
	continue;
      val = i->second.last_active;
      break;
    case STUCK_UNCLEAN:
      if (i->second.state & PG_STATE_CLEAN)
	continue;
      val = i->second.last_clean;
      break;
    case STUCK_STALE:
      if ((i->second.state & PG_STATE_STALE) == 0)
	continue;
      val = i->second.last_unstale;
      break;
    default:
      assert(0 == "invalid type");
    }

    if (val < cutoff) {
      stuck_pgs[i->first] = i->second;
    }
  }
}

void PGMap::dump_stuck(Formatter *f, PGMap::StuckPG type, utime_t cutoff) const
{
  hash_map<pg_t, pg_stat_t> stuck_pg_stats;
  get_stuck_stats(type, cutoff, stuck_pg_stats);
  f->open_array_section("stuck_pg_stats");
  for (hash_map<pg_t,pg_stat_t>::const_iterator i = stuck_pg_stats.begin();
       i != stuck_pg_stats.end();
       ++i) {
    f->open_object_section("pg_stat");
    f->dump_stream("pgid") << i->first;
    i->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void PGMap::dump_stuck_plain(ostream& ss, PGMap::StuckPG type, utime_t cutoff) const
{
  hash_map<pg_t, pg_stat_t> stuck_pg_stats;
  get_stuck_stats(type, cutoff, stuck_pg_stats);
  dump_pg_stats_plain(ss, stuck_pg_stats);
}

void PGMap::dump_osd_perf_stats(Formatter *f) const
{
  f->open_array_section("osd_perf_infos");
  for (hash_map<int32_t, osd_stat_t>::const_iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    f->open_object_section("osd");
    f->dump_int("id", i->first);
    {
      f->open_object_section("perf_stats");
      i->second.fs_perf_stat.dump(f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}
void PGMap::print_osd_perf_stats(std::ostream *ss) const
{
  TextTable tab;
  tab.define_column("osdid", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("fs_commit_latency(ms)", TextTable::LEFT, TextTable::RIGHT);
  tab.define_column("fs_apply_latency(ms)", TextTable::LEFT, TextTable::RIGHT);
  for (hash_map<int32_t, osd_stat_t>::const_iterator i = osd_stat.begin();
       i != osd_stat.end();
       ++i) {
    tab << i->first;
    tab << i->second.fs_perf_stat.filestore_commit_latency;
    tab << i->second.fs_perf_stat.filestore_apply_latency;
    tab << TextTable::endrow;
  }
  (*ss) << tab;
}

void PGMap::recovery_summary(Formatter *f, ostream *out) const
{
  bool first = true;
  if (pg_sum.stats.sum.num_objects_degraded) {
    double pc = (double)pg_sum.stats.sum.num_objects_degraded / (double)pg_sum.stats.sum.num_object_copies * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    if (f) {
      f->dump_unsigned("degraded_objects", pg_sum.stats.sum.num_objects_degraded);
      f->dump_unsigned("degraded_total", pg_sum.stats.sum.num_object_copies);
      f->dump_string("degrated_ratio", b);
    } else {
      *out << pg_sum.stats.sum.num_objects_degraded 
	   << "/" << pg_sum.stats.sum.num_object_copies << " degraded (" << b << "%)";
    }
    first = false;
  }
  if (pg_sum.stats.sum.num_objects_unfound) {
    double pc = (double)pg_sum.stats.sum.num_objects_unfound / (double)pg_sum.stats.sum.num_objects * (double)100.0;
    char b[20];
    snprintf(b, sizeof(b), "%.3lf", pc);
    if (f) {
      f->dump_unsigned("unfound_objects", pg_sum.stats.sum.num_objects_unfound);
      f->dump_unsigned("unfound_total", pg_sum.stats.sum.num_objects);
      f->dump_string("unfound_ratio", b);
    } else {
      if (!first)
	*out << "; ";
      *out << pg_sum.stats.sum.num_objects_unfound
	   << "/" << pg_sum.stats.sum.num_objects << " unfound (" << b << "%)";
    }
    first = false;
  }

  // make non-negative; we can get negative values if osds send
  // uncommitted stats and then "go backward" or if they are just
  // buggy/wrong.
  pool_stat_t pos_delta = pg_sum_delta;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_objects_recovered ||
      pos_delta.stats.sum.num_bytes_recovered ||
      pos_delta.stats.sum.num_keys_recovered) {
    int64_t objps = pos_delta.stats.sum.num_objects_recovered / (double)stamp_delta;
    int64_t bps = pos_delta.stats.sum.num_bytes_recovered / (double)stamp_delta;
    int64_t kps = pos_delta.stats.sum.num_keys_recovered / (double)stamp_delta;
    if (f) {
      f->dump_int("recovering_objects_per_sec", objps);
      f->dump_int("recovering_bytes_per_sec", bps);
      f->dump_int("recovering_keys_per_sec", kps);
    } else {
      if (!first)
	*out << "; ";
      *out << " recovering "
	   << si_t(objps) << " o/s, "
	   << si_t(bps) << "B/s";
      if (pos_delta.stats.sum.num_keys_recovered)
	*out << ", " << si_t(kps) << " key/s";
    }
  }
}

void PGMap::update_delta(CephContext *cct, utime_t inc_stamp, pool_stat_t& pg_sum_old)
{
  utime_t delta_t;
  delta_t = inc_stamp;
  delta_t -= stamp;
  stamp = inc_stamp;

  // calculate a delta, and average over the last 2 deltas.
  pool_stat_t d = pg_sum;
  d.stats.sub(pg_sum_old.stats);
  pg_sum_deltas.push_back(make_pair(d, delta_t));
  stamp_delta += delta_t;

  pg_sum_delta.stats.add(d.stats);
  if (pg_sum_deltas.size() > (std::list< pair<pool_stat_t, utime_t> >::size_type)MAX(1, cct ? cct->_conf->mon_stat_smooth_intervals : 1)) {
    pg_sum_delta.stats.sub(pg_sum_deltas.front().first.stats);
    stamp_delta -= pg_sum_deltas.front().second;
    pg_sum_deltas.pop_front();
  }
}

void PGMap::clear_delta()
{
  pg_sum_delta = pool_stat_t();
  pg_sum_deltas.clear();
  stamp_delta = utime_t();
}

void PGMap::print_summary(Formatter *f, ostream *out) const
{
  std::stringstream ss;
  if (f)
    f->open_array_section("pgs_by_state");

  for (hash_map<int,int>::const_iterator p = num_pg_by_state.begin();
       p != num_pg_by_state.end();
       ++p) {
    if (f) {
      f->open_object_section("pgs_by_state_element");
      f->dump_string("state_name", pg_state_string(p->first));
      f->dump_unsigned("count", p->second);
      f->close_section();
    } else {
      if (p != num_pg_by_state.begin())
	ss << ", ";
      ss << p->second << " " << pg_state_string(p->first);
    }
  }
  if (f)
    f->close_section();

  if (f) {
    f->dump_unsigned("version", version);
    f->dump_unsigned("num_pgs", pg_stat.size());
    f->dump_unsigned("data_bytes", pg_sum.stats.sum.num_bytes);
    f->dump_unsigned("bytes_used", osd_sum.kb_used * 1024ull);
    f->dump_unsigned("bytes_avail", osd_sum.kb_avail * 1024ull);
    f->dump_unsigned("bytes_total", osd_sum.kb * 1024ull);
  } else {
    string states = ss.str();
    *out << "v" << version << ": "
	 << pg_stat.size() << " pgs: "
	 << states << "; "
	 << prettybyte_t(pg_sum.stats.sum.num_bytes) << " data, " 
	 << kb_t(osd_sum.kb_used) << " used, "
	 << kb_t(osd_sum.kb_avail) << " / "
	 << kb_t(osd_sum.kb) << " avail";
  }

  // make non-negative; we can get negative values if osds send
  // uncommitted stats and then "go backward" or if they are just
  // buggy/wrong.
  pool_stat_t pos_delta = pg_sum_delta;
  pos_delta.floor(0);
  if (pos_delta.stats.sum.num_rd ||
      pos_delta.stats.sum.num_wr) {
    if (!f)
      *out << "; ";
    if (pos_delta.stats.sum.num_rd) {
      int64_t rd = (pos_delta.stats.sum.num_rd_kb << 10) / (double)stamp_delta;
      if (f) {
	f->dump_int("read_bytes_sec", rd);
      } else {
	*out << si_t(rd) << "B/s rd, ";
      }
    }
    if (pos_delta.stats.sum.num_wr) {
      int64_t wr = (pos_delta.stats.sum.num_wr_kb << 10) / (double)stamp_delta;
      if (f) {
	f->dump_int("write_bytes_sec", wr);
      } else {
	*out << si_t(wr) << "B/s wr, ";
      }
    }
    int64_t iops = (pos_delta.stats.sum.num_rd + pos_delta.stats.sum.num_wr) / (double)stamp_delta;
    if (f) {
      f->dump_int("op_per_sec", iops);
    } else {
      *out << si_t(iops) << "op/s";
    }
  }

  std::stringstream ssr;
  recovery_summary(f, &ssr);
  if (!f && ssr.str().length())
    *out << "; " << ssr.str();
}

void PGMap::generate_test_instances(list<PGMap*>& o)
{
  o.push_back(new PGMap);
  o.push_back(new PGMap);
  list<Incremental*> inc;
  Incremental::generate_test_instances(inc);
  inc.pop_front();
  while (!inc.empty()) {
    o.back()->apply_incremental(NULL, *inc.front());
    delete inc.front();
    inc.pop_front();
  }
}
