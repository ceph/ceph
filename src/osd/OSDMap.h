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


#ifndef CEPH_OSDMAP_H
#define CEPH_OSDMAP_H

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
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(last_clean_first, bl);
    ::encode(last_clean_last, bl);
    ::encode(up_from, bl);
    ::encode(up_thru, bl);
    ::encode(down_at, bl);
    ::encode(lost_at, bl);
  }
  void decode(bufferlist::iterator& bl) {
    __u8 struct_v;
    ::decode(struct_v, bl);
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
  out << "up_from " << info.up_from
      << " up_thru " << info.up_thru
      << " down_at " << info.down_at
      << " last_clean_interval " << info.last_clean_first << "-" << info.last_clean_last;
  if (info.lost_at)
    out << " lost_at " << info.lost_at;
  return out;
}


/** OSDMap
 */
class OSDMap {

public:
  class Incremental {
  public:
    ceph_fsid_t fsid;
    epoch_t epoch;   // new epoch; we are a diff from epoch-1 to epoch
    utime_t modified;
    int32_t new_pool_max; //incremented by the OSDMonitor on each pool create
    int32_t new_flags;

    /*
    bool is_pg_change() {
      return (fullmap.length() ||
	      crush.length() ||
	      new_pg_num ||
	      new_lpg_num);
    }
    */

    // full (rare)
    bufferlist fullmap;  // in leiu of below.
    bufferlist crush;

    // incremental
    int32_t new_max_osd;
    map<int32_t,pg_pool_t> new_pools;
    map<int32_t,string> new_pool_names;
    set<int32_t> old_pools;
    map<int32_t,entity_addr_t> new_up_client;
    map<int32_t,entity_addr_t> new_up_internal;
    map<int32_t,uint8_t> new_down;
    map<int32_t,uint32_t> new_weight;
    map<pg_t,vector<int32_t> > new_pg_temp;     // [] to remove
    map<int32_t,epoch_t> new_up_thru;
    map<int32_t,pair<epoch_t,epoch_t> > new_last_clean_interval;
    map<int32_t,epoch_t> new_lost;

    map<entity_addr_t,utime_t> new_blacklist;
    vector<entity_addr_t> old_blacklist;
    map<int32_t, entity_addr_t> new_hb_up;

    void encode(bufferlist& bl) {
      // base
      __u16 v = CEPH_OSDMAP_INC_VERSION;
      ::encode(v, bl);
      ::encode(fsid, bl);
      ::encode(epoch, bl); 
      ::encode(modified, bl);
      ::encode(new_pool_max, bl);
      ::encode(new_flags, bl);
      ::encode(fullmap, bl);
      ::encode(crush, bl);

      ::encode(new_max_osd, bl);
      ::encode(new_pools, bl);
      ::encode(new_pool_names, bl);
      ::encode(old_pools, bl);
      ::encode(new_up_client, bl);
      ::encode(new_down, bl);
      ::encode(new_weight, bl);
      ::encode(new_pg_temp, bl);

      // extended
      __u16 ev = CEPH_OSDMAP_INC_VERSION_EXT;
      ::encode(ev, bl);
      ::encode(new_hb_up, bl);
      ::encode(new_up_thru, bl);
      ::encode(new_last_clean_interval, bl);
      ::encode(new_lost, bl);
      ::encode(new_blacklist, bl);
      ::encode(old_blacklist, bl);
      ::encode(new_up_internal, bl);
    }
    void decode(bufferlist::iterator &p) {
      // base
      __u16 v;
      ::decode(v, p);
      ::decode(fsid, p);
      ::decode(epoch, p);
      ::decode(modified, p);
      if (v >= 4)
	::decode(new_pool_max, p);
      ::decode(new_flags, p);
      ::decode(fullmap, p);
      ::decode(crush, p);

      ::decode(new_max_osd, p);
      ::decode(new_pools, p);
      if (v >= 5)
	::decode(new_pool_names, p);
      ::decode(old_pools, p);
      ::decode(new_up_client, p);
      ::decode(new_down, p);
      ::decode(new_weight, p);
      ::decode(new_pg_temp, p);

      // extended
      __u16 ev = 0;
      if (v >= 5)
	::decode(ev, p);
      ::decode(new_hb_up, p);
      if (v < 5)
	::decode(new_pool_names, p);
      ::decode(new_up_thru, p);
      ::decode(new_last_clean_interval, p);
      ::decode(new_lost, p);
      ::decode(new_blacklist, p);
      ::decode(old_blacklist, p);
      ::decode(new_up_internal, p);
    }

    Incremental(epoch_t e=0) :
      epoch(e), new_pool_max(-1), new_flags(-1), new_max_osd(-1) {
      memset(&fsid, 0, sizeof(fsid));
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
  ceph_fsid_t fsid;
  epoch_t epoch;        // what epoch of the osd cluster descriptor is this
  utime_t created, modified; // epoch start time
  int32_t pool_max;     // the largest pool num, ever

  uint32_t flags;

  int num_osd;         // not saved
  int32_t max_osd;
  vector<uint8_t> osd_state;
  vector<entity_addr_t> osd_addr;
  vector<entity_addr_t> osd_hb_addr;
  vector<__u32>   osd_weight;   // 16.16 fixed point, 0x10000 = "in", 0 = "out"
  vector<osd_info_t> osd_info;
  map<pg_t,vector<int> > pg_temp;  // temp pg mapping (e.g. while we rebuild)

  map<int,pg_pool_t> pools;
  map<int32_t,string> pool_name;
  map<string,int> name_pool;

  hash_map<entity_addr_t,utime_t> blacklist;

 public:
  CrushWrapper     crush;       // hierarchical map

  friend class OSDMonitor;
  friend class PGMonitor;
  friend class MDS;

 public:
  OSDMap() : epoch(0), 
	     pool_max(-1),
	     flags(0),
	     num_osd(0), max_osd(0) { 
    memset(&fsid, 0, sizeof(fsid));
  }

  // map info
  ceph_fsid_t& get_fsid() { return fsid; }
  void set_fsid(ceph_fsid_t& f) { fsid = f; }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  void set_epoch(epoch_t e) {
    epoch = e;
    for (map<int,pg_pool_t>::iterator p = pools.begin();
	 p != pools.end();
	 p++)
      p->second.v.last_change = e;
  }

  /* stamps etc */
  const utime_t& get_created() const { return created; }
  const utime_t& get_modified() const { return modified; }

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
    osd_weight.resize(m);
    for (; o<max_osd; o++) {
      osd_state[o] = 0;
      osd_weight[o] = CEPH_OSD_OUT;
    }
    osd_info.resize(m);
    osd_addr.resize(m);
    osd_hb_addr.resize(m);

    calc_num_osds();
  }

  int get_num_osds() {
    return num_osd;
  }
  int calc_num_osds() {
    num_osd = 0;
    for (int i=0; i<max_osd; i++)
      if (osd_state[i] & CEPH_OSD_EXISTS)
	num_osd++;
    return num_osd;
  }

  void get_all_osds(set<int32_t>& ls) { 
    for (int i=0; i<max_osd; i++)
      if (exists(i))
	ls.insert(i);
  }
  int get_num_up_osds() {
    int n = 0;
    for (int i=0; i<max_osd; i++)
      if (osd_state[i] & CEPH_OSD_EXISTS &&
	  osd_state[i] & CEPH_OSD_UP) n++;
    return n;
  }
  int get_num_in_osds() {
    int n = 0;
    for (int i=0; i<max_osd; i++)
      if (osd_state[i] & CEPH_OSD_EXISTS &&
	  get_weight(i) != CEPH_OSD_OUT) n++;
    return n;
  }

  int get_flags() const { return flags; }
  int test_flag(int f) const { return flags & f; }
  void set_flag(int f) { flags |= f; }
  void clear_flag(int f) { flags &= ~f; }

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
    if (w)
      osd_state[o] |= CEPH_OSD_EXISTS;
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



  bool exists(int osd) {
    //assert(osd >= 0);
    return osd >= 0 && osd < max_osd && (osd_state[osd] & CEPH_OSD_EXISTS);
  }
  bool is_up(int osd) { return exists(osd) && osd_state[osd] & CEPH_OSD_UP; }
  bool is_down(int osd) { return !exists(osd) || !is_up(osd); }
  bool is_out(int osd) { return !exists(osd) || get_weight(osd) == CEPH_OSD_OUT; }
  bool is_in(int osd) { return exists(osd) && !is_out(osd); }
  
  int identify_osd(const entity_addr_t& addr) const {
    for (unsigned i=0; i<osd_addr.size(); i++)
      if (osd_addr[i] == addr)
	return i;
    return -1;
  }
  bool have_addr(const entity_addr_t& addr) const {
    return identify_osd(addr) >= 0;
  }
  bool find_osd_on_ip(const entity_addr_t& ip) const {
    for (unsigned i=0; i<osd_addr.size(); i++)
      if (osd_addr[i].is_same_host(ip))
	return i;
    return -1;
  }
  bool have_inst(int osd) {
    return exists(osd) && is_up(osd); 
  }
  const entity_addr_t &get_addr(int osd) {
    assert(exists(osd));
    return osd_addr[osd];
  }
  const entity_addr_t &get_hb_addr(int osd) {
    assert(exists(osd));
    return osd_hb_addr[osd];
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
    assert(exists(osd));
    entity_inst_t i(entity_name_t::OSD(osd),
		    osd_hb_addr[osd]);
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
    assert(osd < max_osd);
    return osd_info[osd];
  }
  
  int get_any_up_osd() {
    for (int i=0; i<max_osd; i++)
      if (is_up(i))
	return i;
    return -1;
  }


  int apply_incremental(Incremental &inc) {
    if (inc.epoch == 1)
      fsid = inc.fsid;
    else
      if (ceph_fsid_compare(&inc.fsid, &fsid) != 0) {
	return -EINVAL;
      }
    assert(inc.epoch == epoch+1);
    epoch++;
    modified = inc.modified;

    // full map?
    if (inc.fullmap.length()) {
      decode(inc.fullmap);
      return 0;
    }

    // nope, incremental.
    if (inc.new_flags >= 0)
      flags = inc.new_flags;

    if (inc.new_max_osd >= 0) 
      set_max_osd(inc.new_max_osd);

    if (inc.new_pool_max != -1)
      pool_max = inc.new_pool_max;

    for (set<int32_t>::iterator p = inc.old_pools.begin();
	 p != inc.old_pools.end();
	 p++) {
      pools.erase(*p);
      name_pool.erase(pool_name[*p]);
      pool_name.erase(*p);
    }
    for (map<int32_t,pg_pool_t>::iterator p = inc.new_pools.begin();
	 p != inc.new_pools.end();
	 p++) {
      pools[p->first] = p->second;
      pools[p->first].v.last_change = epoch;
    }
    for (map<int32_t,string>::iterator p = inc.new_pool_names.begin();
	 p != inc.new_pool_names.end();
	 p++) {
      pool_name[p->first] = p->second;
      name_pool[p->second] = p->first;
    }

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
    for (map<int32_t,entity_addr_t>::iterator i = inc.new_up_client.begin();
         i != inc.new_up_client.end();
         i++) {
      osd_state[i->first] |= CEPH_OSD_EXISTS | CEPH_OSD_UP;
      osd_addr[i->first] = i->second;
      if (inc.new_hb_up.empty()) {
	//this is a backward-compatibility hack
	osd_hb_addr[i->first] = i->second;
	//osd_hb_addr[i->first].erank = osd_hb_addr[i->first].erank + 1;
      }
      else osd_hb_addr[i->first] = inc.new_hb_up[i->first];
      osd_info[i->first].up_from = epoch;
      //cout << "epoch " << epoch << " up osd" << i->first << " at " << i->second << "with hb addr" << osd_hb_addr[i->first] << std::endl;
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

    // pg rebuild
    for (map<pg_t, vector<int> >::iterator p = inc.new_pg_temp.begin(); p != inc.new_pg_temp.end(); p++) {
      if (p->second.empty())
	pg_temp.erase(p->first);
      else
	pg_temp[p->first] = p->second;
    }

    // blacklist
    for (map<entity_addr_t,utime_t>::iterator p = inc.new_blacklist.begin();
	 p != inc.new_blacklist.end();
	 p++)
      blacklist[p->first] = p->second;
    for (vector<entity_addr_t>::iterator p = inc.old_blacklist.begin();
	 p != inc.old_blacklist.end();
	 p++)
      blacklist.erase(*p);

    // do new crush map last (after up/down stuff)
    if (inc.crush.length()) {
      bufferlist::iterator blp = inc.crush.begin();
      crush.decode(blp);
    }

    calc_num_osds();
    return 0;
  }

  // serialize, unserialize
  void encode(bufferlist& bl) {
    __u16 v = CEPH_OSDMAP_VERSION;
    ::encode(v, bl);

    // base
    ::encode(fsid, bl);
    ::encode(epoch, bl);
    ::encode(created, bl);
    ::encode(modified, bl);

    ::encode(pools, bl);
    ::encode(pool_name, bl);
    ::encode(pool_max, bl);

    ::encode(flags, bl);
    
    ::encode(max_osd, bl);
    ::encode(osd_state, bl);
    ::encode(osd_weight, bl);
    ::encode(osd_addr, bl);

    ::encode(pg_temp, bl);

    // crush
    bufferlist cbl;
    crush.encode(cbl);
    ::encode(cbl, bl);

    // extended
    __u16 ev = CEPH_OSDMAP_VERSION_EXT;
    ::encode(ev, bl);
    ::encode(osd_hb_addr, bl);
    ::encode(osd_info, bl);
    ::encode(blacklist, bl);
  }
  
  void decode(bufferlist& bl) {
    bufferlist::iterator p = bl.begin();
    __u16 v;
    ::decode(v, p);

    // base
    ::decode(fsid, p);
    ::decode(epoch, p);
    ::decode(created, p);
    ::decode(modified, p);

    int32_t max_pools = 0;
    if (v < 4) {
      ::decode(max_pools, p);
    }
    ::decode(pools, p);
    if (v >= 5)
      ::decode(pool_name, p);
    if (v >= 4)
      ::decode(pool_max, p);
    else
      pool_max = max_pools;

    ::decode(flags, p);

    ::decode(max_osd, p);
    ::decode(osd_state, p);
    ::decode(osd_weight, p);
    ::decode(osd_addr, p);
    ::decode(pg_temp, p);

    // crush
    bufferlist cbl;
    ::decode(cbl, p);
    bufferlist::iterator cblp = cbl.begin();
    crush.decode(cblp);

    // extended
    __u16 ev = 0;
    if (v >= 5)
      ::decode(ev, p);
    ::decode(osd_hb_addr, p);
    ::decode(osd_info, p);
    if (v < 5)
      ::decode(pool_name, p);
   
    ::decode(blacklist, p);

    // index pool names
    name_pool.clear();
    for (map<int,string>::iterator i = pool_name.begin(); i != pool_name.end(); i++)
      name_pool[i->second] = i->first;
    
    calc_num_osds();
  }
 



  /****   mapping facilities   ****/

  // oid -> pg
  ceph_object_layout file_to_object_layout(object_t oid, ceph_file_layout& layout) {
    return make_object_layout(oid, layout.fl_pg_pool,
			      layout.fl_pg_preferred,
			      layout.fl_stripe_unit);
  }

  ceph_object_layout make_object_layout(object_t oid, int pg_pool, int preferred=-1, int object_stripe_unit = 0) {
    // calculate ps (placement seed)
    const pg_pool_t *pool = get_pg_pool(pg_pool);
    ps_t ps = ceph_str_hash(pool->v.object_hash, oid.name.c_str(), oid.name.length());

    // mix in preferred osd, so we don't get the same peers for
    // all of the placement pgs (e.g. 0.0p*)
    if (preferred >= 0)
      ps += preferred;

    //cout << "preferred " << preferred << " num "
    // << num << " mask " << num_mask << " ps " << ps << endl;

    // construct object layout
    pg_t pgid = pg_t(ps, pg_pool, preferred);
    ceph_object_layout layout;
    layout.ol_pgid = pgid.v;
    layout.ol_stripe_unit = object_stripe_unit;
    return layout;
  }

  int get_pg_num(int pg_pool)
  {
    const pg_pool_t *pool = get_pg_pool(pg_pool);
    return pool->get_pg_num();
  }

  int get_pg_layout(int pg_pool, int seed, ceph_object_layout& layout) {
    const pg_pool_t *pool = get_pg_pool(pg_pool);

    pg_t pgid = pg_t(seed, pg_pool, -1);
    layout.ol_pgid = pgid.v;
    layout.ol_stripe_unit = 0;

    return pool->get_pg_num();
  }

  // pg -> (osd list)
  int pg_to_osds(pg_t pg, vector<int>& osds) {
    // map to osds[]
    int p = pg.pool();
    if (!pools.count(p)) {
      return osds.size();
    }
    pg_pool_t &pool = pools[p];
    ps_t pps = pool.raw_pg_to_pps(pg);  // placement ps
    unsigned size = pool.get_size();

    switch (g_conf.osd_pg_layout) {
    case CEPH_PG_LAYOUT_CRUSH:
      {
	int preferred = pg.preferred();
	if (preferred >= max_osd || preferred >= crush.get_max_devices())
	  preferred = -1;

	// what crush rule?
	int ruleno = crush.find_rule(pool.get_crush_ruleset(), pool.get_type(), size);
	if (ruleno >= 0)
	  crush.do_rule(ruleno, pps, osds, size, preferred, osd_weight);
      }
      break;
      
    case CEPH_PG_LAYOUT_LINEAR:
      for (unsigned i=0; i<size; i++) 
	osds.push_back( (i + pps*size) % g_conf.num_osd );
      break;
      
#if 0
    case CEPH_PG_LAYOUT_HYBRID:
      {
	int h = crush_hash32(CRUSH_HASH_RJENKINS1, pps);
	for (unsigned i=0; i<size; i++) 
	  osds.push_back( (h+i) % g_conf.num_osd );
      }
      break;
      
    case CEPH_PG_LAYOUT_HASH:
      {
	for (unsigned i=0; i<size; i++) {
	  int t = 1;
	  int osd = 0;
	  while (t++) {
	    osd = crush_hash32_3(CRUSH_HASH_RJENKINS1, i, pps, t) % g_conf.num_osd;
	    unsigned j = 0;
	    for (; j<i; j++) 
	      if (osds[j] == osd) break;
	    if (j == i) break;
	  }
	  osds.push_back(osd);
	}      
      }
      break;
#endif
      
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
        assert(size > 0);
        for (unsigned i=1; i<size; i++)
          if (osds[i] == osd) {
            // swap with position 0
            osds[i] = osds[0];
          }
        osds[0] = osd;
      }
      
      if (is_out(osd))
        osds.erase(osds.begin());  // oops, but it's out
    }
    return osds.size();
  }

  // pg -> (up osd list)
  void raw_to_up_osds(pg_t pg, vector<int>& raw, vector<int>& up) {
    up.clear();
    for (unsigned i=0; i<raw.size(); i++) {
      if (!exists(raw[i]) || is_down(raw[i])) 
	continue;
      up.push_back(raw[i]);
    }
  }
  
  bool raw_to_temp_osds(pg_t pg, vector<int>& raw, vector<int>& temp) {
    map<pg_t,vector<int> >::iterator p = pg_temp.find(pg);
    if (p != pg_temp.end()) {
      temp.clear();
      for (unsigned i=0; i<p->second.size(); i++) {
	if (!exists(p->second[i]) || is_down(p->second[i]))
	  continue;
	temp.push_back(p->second[i]);
      }
      return true;
    }
    return false;
  }

  int pg_to_acting_osds(pg_t pg, vector<int>& acting) {         // list of osd addr's
    vector<int> raw;
    pg_to_osds(pg, raw);
    if (!raw_to_temp_osds(pg, raw, acting))
      raw_to_up_osds(pg, raw, acting);
    return acting.size();
  }

  void pg_to_raw_up(pg_t pg, vector<int>& up) {
    vector<int> raw;
    pg_to_osds(pg, raw);
    raw_to_up_osds(pg, raw, up);
  }
  
  void pg_to_up_acting_osds(pg_t pg, vector<int>& up, vector<int>& acting) {
    vector<int> raw;
    pg_to_osds(pg, raw);
    raw_to_up_osds(pg, raw, up);
    if (!raw_to_temp_osds(pg, raw, acting))
      acting = up;
  }

  int lookup_pg_pool_name(const char *name) {
    if (name_pool.count(name))
      return name_pool[name];
    return -1;
  }

  const map<int,pg_pool_t>& get_pools() { return pools; }
  const char *get_pool_name(int p) {
    if (pool_name.count(p))
      return pool_name[p].c_str();
    return 0;
  }
  bool have_pg_pool(int p) const {
    return pools.count(p);
  }
  const pg_pool_t* get_pg_pool(int p) {
    if(pools.count(p))
      return &pools[p];
    return NULL;
  }
  unsigned get_pg_size(pg_t pg) {
    assert(pools.count(pg.pool()));
    pg_pool_t &pool = pools[pg.pool()];
    return pool.get_size();
  }
  int get_pg_type(pg_t pg) {
    assert(pools.count(pg.pool()));
    pg_pool_t &pool = pools[pg.pool()];
    return pool.get_type();
  }


  pg_t raw_pg_to_pg(pg_t pg) {
    assert(pools.count(pg.pool()));
    pg_pool_t &pool = pools[pg.pool()];
    return pool.raw_pg_to_pg(pg);
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
  void build_simple(epoch_t e, ceph_fsid_t &fsid,
		    int num_osd, int num_dom,
		    int pg_bits, int lpg_bits,
		    int mds_local_osd);
  static void build_simple_crush_map(CrushWrapper& crush, map<int, const char*>& poolsets, int num_osd, int num_dom=0);


  void print(ostream& out);
  void print_summary(ostream& out);

};

inline ostream& operator<<(ostream& out, OSDMap& m) {
  m.print_summary(out);
  return out;
}


#endif
