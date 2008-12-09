// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef __OSDMAP_H
#define __OSDMAP_H

/*
 * describe properties of the OSD cluster.
 *   disks, disk groups, total # osds,
 *
 */
#include "config.h"
#include "include/types.h"
#include "osd_types.h"
#include "msg/Message.h"
#include "common/Mutex.h"
#include "common/Clock.h"

#include "crush/CrushWrapper.h"

#include "include/interval_set.h"

#include <vector>
#include <list>
#include <set>
#include <map>
using namespace std;

#include <ext/hash_set>
using __gnu_cxx::hash_set;



/*
 * some system constants
 */

// pg roles
#define PG_ROLE_STRAY   -1
#define PG_ROLE_HEAD     0
#define PG_ROLE_ACKER    1
#define PG_ROLE_MIDDLE   2  // der.. misnomer
//#define PG_ROLE_TAIL     2


inline int calc_bits_of(int t) {
  int b = 0;
  while (t > 0) {
    t = t >> 1;
    b++;
  }
  return b;
}



/*
 * we track up to two intervals during which the osd was alive and
 * healthy.  the most recent is [up_from,up_thru), where up_thru is
 * the last epoch the osd is known to have _started_.  i.e., a lower
 * bound on the actual osd death.  down_at (if it is > up_from) is an
 * upper bound on the actual osd death.
 *
 * the second is the last_clean interval [first,last].  in that case,
 * the last interval is the last epoch known to have been either
 * _finished_, or during which the osd cleanly shut down.  when
 * possible, we push this forward to the epoch the osd was eventually
 * marked down.
 *
 * the lost_at is used to allow build_prior to proceed without waiting
 * for an osd to recover.  In certain cases, progress may be blocked 
 * because an osd is down that may contain updates (i.e., a pg may have
 * gone rw during an interval).  If the osd can't be brought online, we
 * can force things to proceed knowing that we _might_ be losing some
 * acked writes.  If the osd comes back to life later, that's fine to,
 * but those writes will still be lost (the divergent objects will be
 * thrown out).
 */
struct osd_info_t {
  epoch_t last_clean_first;  // last interval that ended with a clean osd shutdown
  epoch_t last_clean_last;
  epoch_t up_from;   // epoch osd marked up
  epoch_t up_thru;   // lower bound on actual osd death (if > up_from)
  epoch_t down_at;   // upper bound on actual osd death (if > up_from)
  epoch_t lost_at;   // last epoch we decided data was "lost"
  
  osd_info_t() : last_clean_first(0), last_clean_last(0),
		 up_from(0), up_thru(0), down_at(0), lost_at(0) {}
  void encode(bufferlist& bl) const {
    ::encode(last_clean_first, bl);
    ::encode(last_clean_last, bl);
    ::encode(up_from, bl);
    ::encode(up_thru, bl);
    ::encode(down_at, bl);
    ::encode(lost_at, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(last_clean_first, bl);
    ::decode(last_clean_last, bl);
    ::decode(up_from, bl);
    ::decode(up_thru, bl);
    ::decode(down_at, bl);
    ::decode(lost_at, bl);
  }
};
WRITE_CLASS_ENCODER(osd_info_t)

inline ostream& operator<<(ostream& out, const osd_info_t& info) {
  return out << "up_from " << info.up_from
	     << " up_thru " << info.up_thru
	     << " down_at " << info.down_at
	     << " last_clean_interval " << info.last_clean_first << "-" << info.last_clean_last;
}


/** OSDMap
 */
class OSDMap {

public:
  class Incremental {
  public:
    ceph_fsid fsid;
    epoch_t epoch;   // new epoch; we are a diff from epoch-1 to epoch
    utime_t ctime;
    int32_t new_flags;

    bool is_pg_change() {
      return (fullmap.length() ||
	      crush.length() ||
	      new_pg_num ||
	      new_lpg_num);
    }

    // full (rare)
    bufferlist fullmap;  // in leiu of below.
    bufferlist crush;

    // incremental
    int32_t new_max_osd;
    int32_t new_pg_num, new_pgp_num, new_lpg_num, new_lpgp_num;
    map<int32_t,entity_addr_t> new_up;
    map<int32_t,uint8_t> new_down;
    map<int32_t,uint32_t> new_weight;
    map<int32_t,epoch_t> new_up_thru;
    map<int32_t,pair<epoch_t,epoch_t> > new_last_clean_interval;
    map<int32_t,epoch_t> new_lost;
    map<pg_t,uint32_t> new_pg_swap_primary;
    list<pg_t> old_pg_swap_primary;

    vector<entity_addr_t> new_blacklist;
    vector<entity_addr_t> old_blacklist;

    snapid_t new_max_snap;
    interval_set<snapid_t> removed_snaps;
    
    void encode(bufferlist& bl) {
      // base
      ::encode(fsid, bl);
      ::encode(epoch, bl); 
      ::encode(ctime, bl);
      ::encode(new_flags, bl);
      ::encode(fullmap, bl);
      ::encode(crush, bl);

      ::encode(new_max_osd, bl);
      ::encode(new_pg_num, bl);
      ::encode(new_pgp_num, bl);
      ::encode(new_lpg_num, bl);
      ::encode(new_lpgp_num, bl);
      ::encode(new_up, bl);
      ::encode(new_down, bl);
      ::encode(new_weight, bl);

      // extended
      ::encode(new_up_thru, bl);
      ::encode(new_last_clean_interval, bl);
      ::encode(new_lost, bl);
      ::encode(new_pg_swap_primary, bl);
      ::encode(old_pg_swap_primary, bl);
      ::encode(new_max_snap, bl);
      ::encode(removed_snaps.m, bl);
      ::encode(new_blacklist, bl);
      ::encode(old_blacklist, bl);
    }
    void decode(bufferlist::iterator &p) {
      // base
      ::decode(fsid, p);
      ::decode(epoch, p);
      ::decode(ctime, p);
      ::decode(new_flags, p);
      ::decode(fullmap, p);
      ::decode(crush, p);

      ::decode(new_max_osd, p);
      ::decode(new_pg_num, p);
      ::decode(new_pgp_num, p);
      ::decode(new_lpg_num, p);
      ::decode(new_lpgp_num, p);
      ::decode(new_up, p);
      ::decode(new_down, p);
      ::decode(new_weight, p);
      
      // extended
      ::decode(new_up_thru, p);
      ::decode(new_last_clean_interval, p);
      ::decode(new_lost, p);
      ::decode(new_pg_swap_primary, p);
      ::decode(old_pg_swap_primary, p);
      ::decode(new_max_snap, p);
      ::decode(removed_snaps.m, p);
      ::decode(new_blacklist, p);
      ::decode(old_blacklist, p);
    }

    Incremental(epoch_t e=0) : epoch(e), new_flags(-1), new_max_osd(-1), 
			       new_pg_num(0), new_pgp_num(0), new_lpg_num(0), new_lpgp_num(0) {
      fsid.major = fsid.minor = 0;
    }
    Incremental(bufferlist &bl) {
      bufferlist::iterator p = bl.begin();
      decode(p);
    }
    Incremental(bufferlist::iterator &p) {
      decode(p);
    }
  };
  
private:
  ceph_fsid fsid;
  epoch_t epoch;        // what epoch of the osd cluster descriptor is this
  utime_t ctime, mtime; // epoch start time

  /*
   * placement groups 
   *
   *  pg_num -- base number of pseudorandomly placed pgs
   *
   *  pgp_num -- effective number when calculating pg placement.  this
   * is used for pg_num increases.  new pgs result in data being
   * "split" into new pgs.  for this to proceed smoothly, new pgs are
   * intiially colocated with their parents; that is, pgp_num doesn't
   * increase until the new pgs have successfully split.  only _then_
   * are the new pgs placed independently.
   *      
   *  lpg_num -- localized pg count (per device).  replicas are
   * randomly selected.
   *
   *  lpgp_num -- as above.
   */
  int32_t pg_num, pg_num_mask;     // placement group count and bitmask
  int32_t pgp_num, pgp_num_mask;   // pg placement num (for placing pg's.. <= pg_num)
  int32_t lpg_num, lpg_num_mask;   // localized placement group count
  int32_t lpgp_num, lpgp_num_mask; // as above

  // new pgs
  epoch_t last_pg_change;  // most recent epoch initiating possible pg creation

  uint32_t flags;

  int32_t max_osd;
  vector<uint8_t> osd_state;
  vector<entity_addr_t> osd_addr;
  vector<__u32>   osd_weight;   // 16.16 fixed point, 0x10000 = "in", 0 = "out"
  vector<osd_info_t> osd_info;

  map<pg_t,uint32_t> pg_swap_primary;  // force new osd to be pg primary (if already a member)
  snapid_t max_snap;
  interval_set<snapid_t> removed_snaps;

  hash_set<entity_addr_t> blacklist;

 public:
  CrushWrapper     crush;       // hierarchical map

  friend class OSDMonitor;
  friend class MDS;

 public:
  OSDMap() : epoch(0), 
	     pg_num(0), pgp_num(0), lpg_num(0), lpgp_num(0),
	     last_pg_change(0),
	     flags(0),
	     max_osd(0), max_snap(0) { 
    fsid.major = fsid.minor = 0;
    calc_pg_masks();
  }

  // map info
  ceph_fsid& get_fsid() { return fsid; }
  void set_fsid(ceph_fsid& f) { fsid = f; }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }
  void set_epoch(epoch_t e) { epoch = e; }

  /* pg num / masks */
  void calc_pg_masks() {
    pg_num_mask = (1 << calc_bits_of(pg_num-1)) - 1;
    pgp_num_mask = (1 << calc_bits_of(pgp_num-1)) - 1;
    lpg_num_mask = (1 << calc_bits_of(lpg_num-1)) - 1;
    lpgp_num_mask = (1 << calc_bits_of(lpgp_num-1)) - 1;
  }

  int get_pg_num() const { return pg_num; }
  int get_pgp_num() const { return pgp_num; }
  int get_lpg_num() const { return lpg_num; }
  int get_lpgp_num() const { return lpgp_num; }

  int get_pg_num_mask() const { return pg_num_mask; }
  int get_pgp_num_mask() const { return pgp_num_mask; }
  int get_lpg_num_mask() const { return lpg_num_mask; }
  int get_lpgp_num_mask() const { return lpgp_num_mask; }

  /* stamps etc */
  const utime_t& get_ctime() const { return ctime; }
  const utime_t& get_mtime() const { return mtime; }

  epoch_t get_last_pg_change() const {
    return last_pg_change;
  }

  snapid_t get_max_snap() { return max_snap; }
  bool is_removed_snap(snapid_t sn) { 
    if (sn > max_snap)
      return false;
    return removed_snaps.contains(sn); 
  }
  interval_set<snapid_t>& get_removed_snaps() { return removed_snaps; }

  bool is_blacklisted(const entity_addr_t& a) {
    return !blacklist.empty() && blacklist.count(a);
  }

  /***** cluster state *****/
  /* osds */
  int get_max_osd() const { return max_osd; }
  void set_max_osd(int m) { 
    int o = max_osd;
    max_osd = m;
    osd_state.resize(m);
    osd_info.resize(m);
    osd_weight.resize(m);
    for (; o<max_osd; o++) {
      osd_state[o] = 0;
      osd_weight[o] = CEPH_OSD_OUT;
    }
    osd_addr.resize(m);
  }

  void get_all_osds(set<int32_t>& ls) { 
    for (int i=0; i<max_osd; i++)
      if (exists(i))
	ls.insert(i);
  }
  int get_num_osds() { 
    int n = 0;
    for (int i=0; i<max_osd; i++)
      //if (osd_state[i] & CEPH_OSD_EXISTS) 
	n++;
    return n;
  }
  int get_num_up_osds() {
    int n = 0;
    for (int i=0; i<max_osd; i++)
      if (//osd_state[i] & CEPH_OSD_EXISTS &&
	  osd_state[i] & CEPH_OSD_UP) n++;
    return n;
  }
  int get_num_in_osds() {
    int n = 0;
    for (int i=0; i<max_osd; i++)
      if (//osd_state[i] & CEPH_OSD_EXISTS &&
	  get_weight(i) != CEPH_OSD_OUT) n++;
    return n;
  }

  int get_state(int o) {
    assert(o < max_osd);
    return osd_state[o];
  }
  void set_state(int o, unsigned s) {
    assert(o < max_osd);
    osd_state[o] = s;
  }
  void set_weightf(int o, float w) {
    set_weight(o, (int)((float)CEPH_OSD_IN * w));
  }
  void set_weight(int o, unsigned w) {
    assert(o < max_osd);
    osd_weight[o] = w;
  }
  unsigned get_weight(int o) {
    assert(o < max_osd);
    return osd_weight[o];
  }
  float get_weightf(int o) {
    return (float)get_weight(o) / (float)CEPH_OSD_IN;
  }
  void adjust_osd_weights(map<int,double>& weights, Incremental& inc) {
    float max = 0;
    for (map<int,double>::iterator p = weights.begin(); p != weights.end(); p++)
      if (p->second > max)
	max = p->second;

    for (map<int,double>::iterator p = weights.begin(); p != weights.end(); p++)
      inc.new_weight[p->first] = (unsigned)((p->second / max) * CEPH_OSD_IN);
  }



  bool exists(int osd) { return osd < max_osd/* && osd_state[osd] & CEPH_OSD_EXISTS*/; }
  bool is_up(int osd) { return exists(osd) && osd_state[osd] & CEPH_OSD_UP; }
  bool is_down(int osd) { assert(exists(osd)); return !is_up(osd); }
  bool is_out(int osd) { return !exists(osd) || get_weight(osd) == CEPH_OSD_OUT; }
  bool is_in(int osd) { return exists(osd) && !is_out(osd); }
  
  bool have_inst(int osd) {
    return exists(osd) && is_up(osd); 
  }
  const entity_addr_t &get_addr(int osd) {
    assert(exists(osd));
    return osd_addr[osd];
  }
  entity_inst_t get_inst(int osd) {
    assert(exists(osd) && is_up(osd));
    return entity_inst_t(entity_name_t::OSD(osd),
			 osd_addr[osd]);
  }
  bool get_inst(int osd, entity_inst_t& inst) { 
    if (have_inst(osd)) {
      inst.name = entity_name_t::OSD(osd);
      inst.addr = osd_addr[osd];
      return true;
    } 
    return false;
  }
  entity_inst_t get_hb_inst(int osd) {
    assert(exists(osd) && is_up(osd));
    entity_inst_t i(entity_name_t::OSD(osd),
		    osd_addr[osd]);
    i.addr.erank++;  // heartbeat addr erank is regular addr erank + 1
    return i;
  }

  epoch_t get_up_from(int osd) {
    assert(exists(osd));
    return osd_info[osd].up_from;
  }
  epoch_t get_up_thru(int osd) {
    assert(exists(osd));
    return osd_info[osd].up_thru;
  }
  epoch_t get_down_at(int osd) {
    assert(exists(osd));
    return osd_info[osd].down_at;
  }
  osd_info_t& get_info(int osd) {
    assert(exists(osd));
    return osd_info[osd];
  }
  
  int get_any_up_osd() {
    for (int i=0; i<max_osd; i++)
      if (is_up(i))
	return i;
    return -1;
  }


  void apply_incremental(Incremental &inc) {
    if (inc.epoch == 1)
      fsid = inc.fsid;
    else
      assert(ceph_fsid_equal(&inc.fsid, &fsid));
    assert(inc.epoch == epoch+1);
    epoch++;
    ctime = inc.ctime;

    // full map?
    if (inc.fullmap.length()) 
      decode(inc.fullmap);
    
    if (inc.is_pg_change())
      last_pg_change = epoch;
    
    if (inc.fullmap.length())
      return;

    // nope, incremental.
    if (inc.new_flags >= 0)
      flags = inc.new_flags;

    if (inc.new_pg_num) {
      assert(inc.new_pg_num >= pg_num);
      pg_num = inc.new_pg_num;
    }
    if (inc.new_lpg_num) {
      assert(inc.new_lpg_num >= lpg_num);
      lpg_num = inc.new_lpg_num;
    }
    if (inc.new_pgp_num) {
      assert(inc.new_pgp_num >= pgp_num);
      pgp_num = inc.new_pgp_num;
    }
    if (inc.new_lpgp_num) {
      assert(inc.new_lpgp_num >= lpgp_num);
      lpgp_num = inc.new_lpgp_num;
    }

    if (inc.new_max_osd >= 0) 
      set_max_osd(inc.new_max_osd);

    for (map<int32_t,uint32_t>::iterator i = inc.new_weight.begin();
         i != inc.new_weight.end();
         i++)
      set_weight(i->first, i->second);

    // up/down
    for (map<int32_t,uint8_t>::iterator i = inc.new_down.begin();
         i != inc.new_down.end();
         i++) {
      assert(osd_state[i->first] & CEPH_OSD_UP);
      osd_state[i->first] &= ~CEPH_OSD_UP;
      osd_info[i->first].down_at = epoch;
      //cout << "epoch " << epoch << " down osd" << i->first << endl;
    }
    for (map<int32_t,entity_addr_t>::iterator i = inc.new_up.begin();
         i != inc.new_up.end(); 
         i++) {
      osd_state[i->first] |= CEPH_OSD_UP;
      osd_addr[i->first] = i->second;
      osd_info[i->first].up_from = epoch;
      //cout << "epoch " << epoch << " up osd" << i->first << " at " << i->second << endl;
    }

    // info
    for (map<int32_t,epoch_t>::iterator i = inc.new_up_thru.begin();
         i != inc.new_up_thru.end();
         i++)
      osd_info[i->first].up_thru = i->second;
    for (map<int32_t,pair<epoch_t,epoch_t> >::iterator i = inc.new_last_clean_interval.begin();
         i != inc.new_last_clean_interval.end();
         i++) {
      osd_info[i->first].last_clean_first = i->second.first;
      osd_info[i->first].last_clean_last = i->second.second;
    }
    for (map<int32_t,epoch_t>::iterator p = inc.new_lost.begin(); p != inc.new_lost.end(); p++)
      osd_info[p->first].lost_at = p->second;

    // pg swap
    for (map<pg_t,uint32_t>::iterator i = inc.new_pg_swap_primary.begin();
	 i != inc.new_pg_swap_primary.end();
	 i++)
      pg_swap_primary[i->first] = i->second;
    for (list<pg_t>::iterator i = inc.old_pg_swap_primary.begin();
	 i != inc.old_pg_swap_primary.end();
	 i++)
      pg_swap_primary.erase(*i);

    // snaps
    if (inc.new_max_snap > 0)
      max_snap = inc.new_max_snap;
    removed_snaps.union_of(inc.removed_snaps);

    // blacklist
    for (vector<entity_addr_t>::iterator p = inc.new_blacklist.begin();
	 p != inc.new_blacklist.end();
	 p++)
      blacklist.insert(*p);
    for (vector<entity_addr_t>::iterator p = inc.old_blacklist.begin();
	 p != inc.old_blacklist.end();
	 p++)
      blacklist.erase(*p);

    // do new crush map last (after up/down stuff)
    if (inc.crush.length()) {
      bufferlist::iterator blp = inc.crush.begin();
      crush.decode(blp);
    }
  }

  // serialize, unserialize
  void encode(bufferlist& blist) {
    // base
    ::encode(fsid, blist);
    ::encode(epoch, blist);
    ::encode(ctime, blist);
    ::encode(mtime, blist);
    ::encode(pg_num, blist);
    ::encode(pgp_num, blist);
    ::encode(lpg_num, blist);
    ::encode(lpgp_num, blist);
    ::encode(last_pg_change, blist);
    ::encode(flags, blist);
    
    ::encode(max_osd, blist);
    ::encode(osd_state, blist);
    ::encode(osd_weight, blist);
    ::encode(osd_addr, blist);

    // crush
    bufferlist cbl;
    crush.encode(cbl);
    ::encode(cbl, blist);

    // extended
    ::encode(osd_info, blist);
    ::encode(pg_swap_primary, blist);

    ::encode(max_snap, blist);
    ::encode(removed_snaps.m, blist);
    ::encode(blacklist, blist);
  }
  
  void decode(bufferlist& blist) {
    bufferlist::iterator p = blist.begin();
    // base
    ::decode(fsid, p);
    ::decode(epoch, p);
    ::decode(ctime, p);
    ::decode(mtime, p);
    ::decode(pg_num, p);
    ::decode(pgp_num, p);
    ::decode(lpg_num, p);
    ::decode(lpgp_num, p);
    calc_pg_masks();
    ::decode(last_pg_change, p);
    ::decode(flags, p);

    ::decode(max_osd, p);
    ::decode(osd_state, p);
    ::decode(osd_weight, p);
    ::decode(osd_addr, p);
    
    // crush
    bufferlist cbl;
    ::decode(cbl, p);
    bufferlist::iterator cblp = cbl.begin();
    crush.decode(cblp);

    // extended
    ::decode(osd_info, p);
    ::decode(pg_swap_primary, p);
    
    ::decode(max_snap, p);
    ::decode(removed_snaps.m, p);
    ::decode(blacklist, p);
  }
 



  /****   mapping facilities   ****/

  // oid -> pg
  ceph_object_layout file_to_object_layout(object_t oid, ceph_file_layout& layout) {
    return make_object_layout(oid, layout.fl_pg_type, layout.fl_pg_size, 
			      layout.fl_pg_pool,
			      ceph_file_layout_pg_preferred(layout),
			      ceph_file_layout_object_su(layout));
  }

  ceph_object_layout make_object_layout(object_t oid, int pg_type, int pg_size, int pg_pool, int preferred=-1, int object_stripe_unit = 0) {
    // calculate ps (placement seed)
    ps_t ps;  // NOTE: keep full precision, here!
    switch (g_conf.osd_object_layout) {
    case CEPH_OBJECT_LAYOUT_LINEAR:
      ps = oid.bno + oid.ino;
      break;
      
    case CEPH_OBJECT_LAYOUT_HASHINO:
      //ps = stable_mod(oid.bno + H(oid.bno+oid.ino)^H(oid.ino>>32), num, num_mask);
      ps = oid.bno + crush_hash32_2(oid.ino, oid.ino>>32);
      break;

    case CEPH_OBJECT_LAYOUT_HASH:
      //ps = stable_mod(H( (oid.bno & oid.ino) ^ ((oid.bno^oid.ino) >> 32) ), num, num_mask);
      //ps = stable_mod(H(oid.bno) + H(oid.ino)^H(oid.ino>>32), num, num_mask);
      //ps = stable_mod(oid.bno + H(oid.bno+oid.ino)^H(oid.bno+oid.ino>>32), num, num_mask);
      ps = oid.bno + crush_hash32_2(oid.ino, oid.ino>>32);
      break;

    default:
      assert(0);
    }

    //cout << "preferred " << preferred << " num " << num << " mask " << num_mask << " ps " << ps << endl;

    // construct object layout
    pg_t pgid = pg_t(pg_type, pg_size, ps, pg_pool, preferred);
    ceph_object_layout layout;
    layout.ol_pgid = pgid.u.pg64;
    layout.ol_stripe_unit = object_stripe_unit;
    return layout;
  }


  /*
   * map a raw pg (with full precision ps) into an actual pg, for storage
   */
  pg_t raw_pg_to_pg(pg_t pg) {
    if (pg.preferred() >= 0)
      pg.u.pg.ps = ceph_stable_mod(pg.ps(), lpg_num, lpg_num_mask);
    else
      pg.u.pg.ps = ceph_stable_mod(pg.ps(), pg_num, pg_num_mask);
    return pg;
  }
  
  /*
   * map raw pg (full precision ps) into a placement ps
   */
  ps_t raw_pg_to_pps(pg_t pg) {
    if (pg.preferred() >= 0)
      return ceph_stable_mod(pg.ps(), lpgp_num, lpgp_num_mask);
    else
      return ceph_stable_mod(pg.ps(), pgp_num, pgp_num_mask);
  }

  // pg -> (osd list)
  int pg_to_osds(pg_t pg, vector<int>& osds) {
    // map to osds[]

    ps_t pps = raw_pg_to_pps(pg);  // placement ps

    switch (g_conf.osd_pg_layout) {
    case CEPH_PG_LAYOUT_CRUSH:
      {
	// what crush rule?
	int ruleno = crush.find_rule(pg.pool(), pg.type(), pg.size());
	if (ruleno >= 0)
	  crush.do_rule(ruleno, pps, osds, pg.size(), pg.preferred(), osd_weight);
      }
      break;
      
    case CEPH_PG_LAYOUT_LINEAR:
      for (unsigned i=0; i<pg.size(); i++) 
	osds.push_back( (i + pps*pg.size()) % g_conf.num_osd );
      break;
      
    case CEPH_PG_LAYOUT_HYBRID:
      {
	int h = crush_hash32(pps);
	for (unsigned i=0; i<pg.size(); i++) 
	  osds.push_back( (h+i) % g_conf.num_osd );
      }
      break;
      
    case CEPH_PG_LAYOUT_HASH:
      {
	for (unsigned i=0; i<pg.size(); i++) {
	  int t = 1;
	  int osd = 0;
	  while (t++) {
	    osd = crush_hash32_3(i, pps, t) % g_conf.num_osd;
	    unsigned j = 0;
	    for (; j<i; j++) 
	      if (osds[j] == osd) break;
	    if (j == i) break;
	  }
	  osds.push_back(osd);
	}      
      }
      break;
      
    default:
      assert(0);
    }
  
    // no crush, but forcefeeding?
    if (pg.preferred() >= 0 &&
	g_conf.osd_pg_layout != CEPH_PG_LAYOUT_CRUSH) {
      int osd = pg.preferred();
      
      // already in there?
      if (osds.empty()) {
        osds.push_back(osd);
      } else {
        assert(pg.size() > 0);
        for (unsigned i=1; i<pg.size(); i++)
          if (osds[i] == osd) {
            // swap with position 0
            osds[i] = osds[0];
          }
        osds[0] = osd;
      }
      
      if (is_out(osd))
        osds.erase(osds.begin());  // oops, but it's out
    }

    // swap primary?
    if (pg_swap_primary.count(pg)) {
      for (unsigned i=1; i<osds.size(); i++) {
	if (osds[i] == (int)pg_swap_primary[pg]) {
	  uint32_t t = osds[0];	  // swap primary for this pg
	  osds[0] = osds[i];
	  osds[i] = t;
	  break;
	}
      }
    }
    
    return osds.size();
  }

  // pg -> (up osd list)
  int pg_to_acting_osds(pg_t pg,
                        vector<int>& osds) {         // list of osd addr's
    // get rush list
    vector<int> raw;
    pg_to_osds(pg, raw);
    
    osds.clear();
    for (unsigned i=0; i<raw.size(); i++) {
      if (!exists(raw[i]) || is_down(raw[i])) continue;
      osds.push_back( raw[i] );
    }
    return osds.size();
  }



  // pg -> primary osd
  int get_pg_primary(pg_t pg) {
    vector<int> group;
    int nrep = pg_to_osds(pg, group);
    if (nrep)
      return group[0];
    return -1;  // we fail!
  }

  // pg -> acting primary osd
  int get_pg_acting_primary(pg_t pg) {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    if (nrep > 0)
      return group[0];
    return -1;  // we fail!
  }
  int get_pg_acting_tail(pg_t pg) {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    if (nrep > 0)
      return group[group.size()-1];
    return -1;  // we fail!
  }


  /* what replica # is a given osd? 0 primary, -1 for none. */
  int calc_pg_rank(int osd, vector<int>& acting, int nrep=0) {
    if (!nrep) nrep = acting.size();
    for (int i=0; i<nrep; i++) 
      if (acting[i] == osd) return i;
    return -1;
  }
  int calc_pg_role(int osd, vector<int>& acting, int nrep=0) {
    if (!nrep) nrep = acting.size();
    int rank = calc_pg_rank(osd, acting, nrep);
    
    if (rank < 0) return PG_ROLE_STRAY;
    else if (rank == 0) return PG_ROLE_HEAD;
    else if (rank == 1) return PG_ROLE_ACKER;
    else return PG_ROLE_MIDDLE;
  }
  
  int get_pg_role(pg_t pg, int osd) {
    vector<int> group;
    int nrep = pg_to_osds(pg, group);
    return calc_pg_role(osd, group, nrep);
  }
  
  /* rank is -1 (stray), 0 (primary), 1,2,3,... (replica) */
  int get_pg_acting_rank(pg_t pg, int osd) {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    return calc_pg_rank(osd, group, nrep);
  }
  /* role is -1 (stray), 0 (primary), 1 (replica) */
  int get_pg_acting_role(pg_t pg, int osd) {
    vector<int> group;
    int nrep = pg_to_acting_osds(pg, group);
    return calc_pg_role(osd, group, nrep);
  }


  /*
   * handy helpers to build simple maps...
   */
  void build_simple(epoch_t e, ceph_fsid &fsid,
		    int num_osd, int num_dom,
		    int pg_bits, int lpg_bits,
		    int mds_local_osd);
  static void build_simple_crush_map(CrushWrapper& crush, int num_osd, int num_dom=0);


  void print(ostream& out);
  void print_summary(ostream& out);

};


#endif
