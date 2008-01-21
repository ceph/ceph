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

#include <vector>
#include <list>
#include <set>
#include <map>
using namespace std;


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
  while (t) {
    t = t >> 1;
    b++;
  }
  return b;
}



/** OSDMap
 */
class OSDMap {

public:
  class Incremental {
  public:
    ceph_fsid fsid;
    epoch_t epoch;   // new epoch; we are a diff from epoch-1 to epoch
    epoch_t mon_epoch;  // monitor epoch (election iteration)
    utime_t ctime;

    // full (rare)
    bufferlist fullmap;  // in leiu of below.
    bufferlist crush;

    // incremental
    int32_t new_max_osd;
    map<int32_t,entity_addr_t> new_up;
    map<int32_t,uint8_t> new_down;
    map<int32_t,uint32_t> new_offload;
    map<pg_t,uint32_t> new_pg_swap_primary;
    list<pg_t> old_pg_swap_primary;
    
    void encode(bufferlist& bl) {
      ::_encode(fsid, bl);
      ::_encode(epoch, bl); 
      ::_encode(mon_epoch, bl);
      ctime._encode(bl);
      ::_encode(fullmap, bl);
      ::_encode(crush, bl);
      ::_encode(new_max_osd, bl);
      ::_encode(new_up, bl);
      ::_encode(new_down, bl);
      ::_encode(new_offload, bl);
      ::_encode(new_pg_swap_primary, bl);
      ::_encode(old_pg_swap_primary, bl);
    }
    void decode(bufferlist& bl, int& off) {
      ::_decode(fsid, bl, off);
      ::_decode(epoch, bl, off);
      ::_decode(mon_epoch, bl, off);
      ctime._decode(bl, off);
      ::_decode(fullmap, bl, off);
      ::_decode(crush, bl, off);
      ::_decode(new_max_osd, bl, off);
      ::_decode(new_up, bl, off);
      ::_decode(new_down, bl, off);
      ::_decode(new_offload, bl, off);
      ::_decode(new_pg_swap_primary, bl, off);
      ::_decode(old_pg_swap_primary, bl, off);
    }

    Incremental(epoch_t e=0) : epoch(e), mon_epoch(0), new_max_osd(-1) {
      fsid.major = fsid.minor = 0;
    }
  };

private:
  ceph_fsid fsid;
  epoch_t epoch;       // what epoch of the osd cluster descriptor is this
  epoch_t mon_epoch;  // monitor epoch (election iteration)
  utime_t ctime, mtime;       // epoch start time
  int32_t pg_num;       // placement group count
  int32_t pg_num_mask;  // bitmask for above
  int32_t localized_pg_num;      // localized place group count
  int32_t localized_pg_num_mask; // ditto

  int32_t max_osd;
  vector<uint8_t>  osd_state;
  vector<entity_addr_t> osd_addr;
  map<pg_t,uint32_t> pg_swap_primary;  // force new osd to be pg primary (if already a member)
  
 public:
  CrushWrapper     crush;       // hierarchical map

  friend class OSDMonitor;
  friend class MDS;

 public:
  OSDMap() : epoch(0), mon_epoch(0), 
	     pg_num(1<<5),
	     localized_pg_num(1<<3),
	     max_osd(0) { 
    fsid.major = fsid.minor = 0;
    calc_pg_masks();
  }

  // map info
  ceph_fsid& get_fsid() { return fsid; }
  void set_fsid(ceph_fsid& f) { fsid = f; }

  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  /* pg num / masks */
  void calc_pg_masks() {
    pg_num_mask = (1 << calc_bits_of(pg_num-1)) - 1;
    localized_pg_num_mask = (1 << calc_bits_of(localized_pg_num-1)) - 1;
  }

  int get_pg_num() const { return pg_num; }
  void set_pg_num(int m) { pg_num = m; calc_pg_masks(); }
  int get_localized_pg_num() const { return localized_pg_num; }

  /* stamps etc */
  const utime_t& get_ctime() const { return ctime; }
  const utime_t& get_mtime() const { return mtime; }

  bool is_mkfs() const { return epoch == 2; }
  bool post_mkfs() const { return epoch > 2; }

  /***** cluster state *****/
  /* osds */
  int get_max_osd() const { return max_osd; }
  void set_max_osd(int m) { 
    int o = max_osd;
    max_osd = m;
    osd_state.resize(m);
    for (; o<max_osd; o++) 
      osd_state[o] = 0;
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
      if (osd_state[i] & CEPH_OSD_EXISTS) n++;
    return n;
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
	  crush.get_offload(i) != CEPH_OSD_OUT) n++;
    return n;
  }

  void set_state(int o, unsigned s) {
    assert(o < max_osd);
    osd_state[o] = s;
  }
  void set_offload(int o, unsigned off) {
    crush.set_offload(o, off);
  }

  bool exists(int osd) { return osd < max_osd && osd_state[osd] & CEPH_OSD_EXISTS; }
  bool is_up(int osd) { assert(exists(osd)); return osd_state[osd] & CEPH_OSD_UP; }
  bool is_down(int osd) { assert(exists(osd)); return !is_up(osd); }
  bool is_down_clean(int osd) { 
    assert(exists(osd)); 
    return is_down(osd) && osd_state[osd] & CEPH_OSD_CLEAN; 
  }
  bool is_out(int osd) { return !exists(osd) || crush.get_offload(osd) == CEPH_OSD_OUT; }
  bool is_in(int osd) { return exists(osd) && !is_out(osd); }
  
  bool have_inst(int osd) {
    return exists(osd) && is_up(osd); 
  }
  const entity_addr_t &get_addr(int osd) {
    assert(exists(osd));
    return osd_addr[osd];
  }
  entity_inst_t get_inst(int osd) {
    assert(have_inst(osd));
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
  
  int get_any_up_osd() {
    for (int i=0; i<max_osd; i++)
      if (is_up(i))
	return i;
    return -1;
  }

  void mark_down(int o, bool clean) { 
    osd_state[o] &= ~CEPH_OSD_UP;
  }
  void mark_up(int o) { 
    osd_state[o] |= CEPH_OSD_UP;
  }
  void mark_out(int o) { 
    set_offload(o, CEPH_OSD_OUT);
  }
  void mark_in(int o) { 
    set_offload(o, CEPH_OSD_IN);
  }

  void apply_incremental(Incremental &inc) {
    assert(ceph_fsid_equal(&inc.fsid, &fsid) || inc.epoch == 1);
    assert(inc.epoch == epoch+1);
    epoch++;
    mon_epoch = inc.mon_epoch;
    ctime = inc.ctime;

    // full map?
    if (inc.fullmap.length()) {
      decode(inc.fullmap);
      return;
    }
    if (inc.crush.length()) {
      bufferlist::iterator blp = inc.crush.begin();
      crush._decode(blp);
    }

    // nope, incremental.
    if (inc.new_max_osd >= 0) 
      set_max_osd(inc.new_max_osd);

    for (map<int32_t,uint8_t>::iterator i = inc.new_down.begin();
         i != inc.new_down.end();
         i++) {
      assert(osd_state[i->first] & CEPH_OSD_UP);
      osd_state[i->first] &= ~CEPH_OSD_UP;
      //cout << "epoch " << epoch << " down osd" << i->first << endl;
    }
    for (map<int32_t,uint32_t>::iterator i = inc.new_offload.begin();
         i != inc.new_offload.end();
         i++) {
      crush.set_offload(i->first, i->second);
    }

    for (map<int32_t,entity_addr_t>::iterator i = inc.new_up.begin();
         i != inc.new_up.end(); 
         i++) {
      osd_state[i->first] |= CEPH_OSD_UP;
      osd_addr[i->first] = i->second;
      //cout << "epoch " << epoch << " up osd" << i->first << " at " << i->second << endl;
    }

    for (map<pg_t,uint32_t>::iterator i = inc.new_pg_swap_primary.begin();
	 i != inc.new_pg_swap_primary.end();
	 i++)
      pg_swap_primary[i->first] = i->second;
    for (list<pg_t>::iterator i = inc.old_pg_swap_primary.begin();
	 i != inc.old_pg_swap_primary.end();
	 i++)
      pg_swap_primary.erase(*i);
  }

  // serialize, unserialize
  void encode(bufferlist& blist) {
    ::_encode(fsid, blist);
    ::_encode(epoch, blist);
    ::_encode(mon_epoch, blist);
    ::_encode(ctime, blist);
    ::_encode(mtime, blist);
    ::_encode(pg_num, blist);
    ::_encode(localized_pg_num, blist);
    
    ::_encode(max_osd, blist);
    ::_encode(osd_state, blist);
    ::_encode(osd_addr, blist);
    ::_encode(pg_swap_primary, blist);
    
    bufferlist cbl;
    crush._encode(cbl);
    ::_encode(cbl, blist);
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    ::_decode(fsid, blist, off);
    ::_decode(epoch, blist, off);
    ::_decode(mon_epoch, blist, off);
    ::_decode(ctime, blist, off);
    ::_decode(mtime, blist, off);
    ::_decode(pg_num, blist, off);
    ::_decode(localized_pg_num, blist, off);
    calc_pg_masks();

    ::_decode(max_osd, blist, off);
    ::_decode(osd_state, blist, off);
    ::_decode(osd_addr, blist, off);
    ::_decode(pg_swap_primary, blist, off);
    
    bufferlist cbl;
    ::_decode(cbl, blist, off);
    bufferlist::iterator cblp = cbl.begin();
    crush._decode(cblp);

    //crush.update_offload_map(out_osds, overload_osds);
  }
 



  /****   mapping facilities   ****/

  // oid -> pg
  ceph_object_layout file_to_object_layout(object_t oid, FileLayout& layout) {
    return make_object_layout(oid, layout.fl_pg_type, layout.fl_pg_size, layout.fl_pg_preferred, layout.fl_object_stripe_unit);
  }

  ceph_object_layout make_object_layout(object_t oid, int pg_type, int pg_size, int preferred=-1, int object_stripe_unit = 0) {
    int num = preferred >= 0 ? localized_pg_num:pg_num;
    int num_mask = preferred >= 0 ? localized_pg_num_mask:pg_num_mask;

    // calculate ps (placement seed)
    ps_t ps;
    switch (g_conf.osd_object_layout) {
    case CEPH_OBJECT_LAYOUT_LINEAR:
      ps = ceph_stable_mod(oid.bno + oid.ino, num, num_mask);
      break;
      
    case CEPH_OBJECT_LAYOUT_HASHINO:
      //ps = stable_mod(oid.bno + H(oid.bno+oid.ino)^H(oid.ino>>32), num, num_mask);
      ps = ceph_stable_mod(oid.bno + crush_hash32_2(oid.ino, oid.ino>>32), num, num_mask);
      break;

    case CEPH_OBJECT_LAYOUT_HASH:
      //ps = stable_mod(H( (oid.bno & oid.ino) ^ ((oid.bno^oid.ino) >> 32) ), num, num_mask);
      //ps = stable_mod(H(oid.bno) + H(oid.ino)^H(oid.ino>>32), num, num_mask);
      //ps = stable_mod(oid.bno + H(oid.bno+oid.ino)^H(oid.bno+oid.ino>>32), num, num_mask);
      ps = ceph_stable_mod(oid.bno + crush_hash32_2(oid.ino, oid.ino>>32), num, num_mask);
      break;

    default:
      assert(0);
    }

    //cout << "preferred " << preferred << " num " << num << " mask " << num_mask << " ps " << ps << endl;

    // construct object layout
    pg_t pgid = pg_t(pg_type, pg_size, ps, preferred);
    ceph_object_layout layout;
    layout.ol_pgid = pgid.u;
    layout.ol_stripe_unit = object_stripe_unit;
    return layout;
  }


  // pg -> (osd list)
  int pg_to_osds(pg_t pg, vector<int>& osds) {
    // map to osds[]
    switch (g_conf.osd_pg_layout) {
    case CEPH_PG_LAYOUT_CRUSH:
      {
	// what crush rule?
	int rule;
	if (pg.is_rep()) rule = CRUSH_REP_RULE(pg.size());
	else if (pg.is_raid4()) rule = CRUSH_RAID_RULE(pg.size());
	else assert(0);
	crush.do_rule(rule,
		      pg.ps(),
		      osds, pg.size(),
		      pg.preferred());
      }
      break;
      
    case CEPH_PG_LAYOUT_LINEAR:
      for (int i=0; i<pg.size(); i++) 
	osds.push_back( (i + pg.ps()*pg.size()) % g_conf.num_osd );
      break;
      
    case CEPH_PG_LAYOUT_HYBRID:
      {
	int h = crush_hash32(pg.ps());
	for (int i=0; i<pg.size(); i++) 
	  osds.push_back( (h+i) % g_conf.num_osd );
      }
      break;
      
    case CEPH_PG_LAYOUT_HASH:
      {
	for (int i=0; i<pg.size(); i++) {
	  int t = 1;
	  int osd = 0;
	  while (t++) {
	    osd = crush_hash32_3(i, pg.ps(), t) % g_conf.num_osd;
	    int j = 0;
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
        for (int i=1; i<pg.size(); i++)
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
      if (is_down(raw[i])) continue;
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




};


#endif
