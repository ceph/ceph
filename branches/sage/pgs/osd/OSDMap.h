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

#include "crush/crush.h"
using namespace crush;

#include <vector>
#include <list>
#include <set>
#include <map>
using namespace std;


/*
 * some system constants
 */

// from LSB to MSB,
#define PG_PS_BITS         16  // max bits for placement seed/group portion of PG
#define PG_REP_BITS        6   // up to 64 replicas   
#define PG_TYPE_BITS       2
#define PG_PS_MASK         ((1LL<<PG_PS_BITS)-1)

#define PG_TYPE_RAND     1   // default: distribution randomly
#define PG_TYPE_STARTOSD 2   // place primary on a specific OSD

// pg roles
#define PG_ROLE_STRAY   -1
#define PG_ROLE_HEAD     0
#define PG_ROLE_ACKER    1
#define PG_ROLE_MIDDLE   2  // der.. misnomer
//#define PG_ROLE_TAIL     2


inline int stable_mod(int x, int b, int bmask) {
  if ((x & bmask) < b) 
    return x & bmask;
  else
    return (x & (bmask>>1));
}

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
    epoch_t epoch;   // new epoch; we are a diff from epoch-1 to epoch
    epoch_t mon_epoch;  // monitor epoch (election iteration)
    utime_t ctime;
    map<int,entity_inst_t> new_up;
    map<int,entity_inst_t> new_down;
    list<int> new_in;
    list<int> new_out;
    map<int,float> new_overload;  // updated overload value
    list<int>      old_overload;  // no longer overload
    
    void encode(bufferlist& bl) {
      bl.append((char*)&epoch, sizeof(epoch));
      bl.append((char*)&mon_epoch, sizeof(mon_epoch));
      bl.append((char*)&ctime, sizeof(ctime));
      ::_encode(new_up, bl);
      ::_encode(new_down, bl);
      ::_encode(new_in, bl);
      ::_encode(new_out, bl);
      ::_encode(new_overload, bl);
    }
    void decode(bufferlist& bl, int& off) {
      bl.copy(off, sizeof(epoch), (char*)&epoch);
      off += sizeof(epoch);
      bl.copy(off, sizeof(mon_epoch), (char*)&mon_epoch);
      off += sizeof(mon_epoch);
      bl.copy(off, sizeof(ctime), (char*)&ctime);
      off += sizeof(ctime);
      ::_decode(new_up, bl, off);
      ::_decode(new_down, bl, off);
      ::_decode(new_in, bl, off);
      ::_decode(new_out, bl, off);
      ::_decode(new_overload, bl, off);
    }

    Incremental(epoch_t e=0) : epoch(e), mon_epoch(0) {}
  };

private:
  epoch_t   epoch;       // what epoch of the osd cluster descriptor is this
  epoch_t   mon_epoch;  // monitor epoch (election iteration)
  utime_t   ctime;       // epoch start time
  int pg_num;       // placement group count
  int pg_num_mask;  // bitmask for above
  int localized_pg_num;      // localized place group count
  int localized_pg_num_mask; // ditto

  set<int>  osds;        // all osds
  set<int>  down_osds;   // list of down disks
  set<int>  out_osds;    // list of unmapped disks
  map<int,float> overload_osds; 
  map<int,entity_inst_t> osd_inst;

 public:
  Crush     crush;       // hierarchical map

  friend class OSDMonitor;
  friend class MDS;

 public:
  OSDMap() : epoch(0), mon_epoch(0), 
	     pg_num(1<<5),
	     localized_pg_num(1<<3) { 
    calc_pg_masks();
  }

  // map info
  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  void calc_pg_masks() {
    pg_num_mask = (1 << calc_bits_of(pg_num-1)) - 1;
    localized_pg_num_mask = (1 << calc_bits_of(localized_pg_num-1)) - 1;
  }

  int get_pg_num() const { return pg_num; }
  void set_pg_num(int m) { pg_num = m; calc_pg_masks(); }
  int get_localized_pg_num() const { return localized_pg_num; }

  const utime_t& get_ctime() const { return ctime; }

  bool is_mkfs() const { return epoch == 1; }
  //void set_mkfs() { assert(epoch == 1); }

  /***** cluster state *****/
  int num_osds() { return osds.size(); }
  void get_all_osds(set<int>& ls) { ls = osds; }

  const set<int>& get_osds() { return osds; }
  const set<int>& get_down_osds() { return down_osds; }
  const set<int>& get_out_osds() { return out_osds; }
  const map<int,float>& get_overload_osds() { return overload_osds; }
  
  bool is_down(int osd) { return down_osds.count(osd); }
  bool is_up(int osd) { return !is_down(osd); }
  bool is_out(int osd) { return out_osds.count(osd); }
  bool is_in(int osd) { return !is_out(osd); }
  
  const entity_inst_t& get_inst(int osd) {
    assert(osd_inst.count(osd));
    return osd_inst[osd];
  }
  bool get_inst(int osd, entity_inst_t& inst) { 
    if (osd_inst.count(osd)) {
      inst = osd_inst[osd];
      return true;
    } 
    return false;
  }
  
  void mark_down(int o) { down_osds.insert(o); }
  void mark_up(int o) { down_osds.erase(o); }
  void mark_out(int o) { out_osds.insert(o); }
  void mark_in(int o) { out_osds.erase(o); }


  void apply_incremental(Incremental &inc) {
    assert(inc.epoch == epoch+1);
    epoch++;
    mon_epoch = inc.mon_epoch;
    ctime = inc.ctime;

    for (map<int,entity_inst_t>::iterator i = inc.new_up.begin();
         i != inc.new_up.end(); 
         i++) {
      assert(down_osds.count(i->first));
      down_osds.erase(i->first);
      assert(osd_inst.count(i->first) == 0);
      osd_inst[i->first] = i->second;
      //cout << "epoch " << epoch << " up osd" << i->first << endl;
    }
    for (map<int,entity_inst_t>::iterator i = inc.new_down.begin();
         i != inc.new_down.end();
         i++) {
      assert(down_osds.count(i->first) == 0);
      down_osds.insert(i->first);
      assert(osd_inst.count(i->first) == 0 ||
             osd_inst[i->first] == i->second);
      osd_inst.erase(i->first);
      //cout << "epoch " << epoch << " down osd" << i->first << endl;
    }
    for (list<int>::iterator i = inc.new_in.begin();
         i != inc.new_in.end();
         i++) {
      assert(out_osds.count(*i));
      out_osds.erase(*i);
      //cout << "epoch " << epoch << " in osd" << *i << endl;
    }
    for (list<int>::iterator i = inc.new_out.begin();
         i != inc.new_out.end();
         i++) {
      assert(out_osds.count(*i) == 0);
      out_osds.insert(*i);
      //cout << "epoch " << epoch << " out osd" << *i << endl;
    }
    for (map<int,float>::iterator i = inc.new_overload.begin();
         i != inc.new_overload.end();
         i++) {
      overload_osds[i->first] = i->second;
    }
    for (list<int>::iterator i = inc.old_overload.begin();
         i != inc.old_overload.end();
         i++) {
      assert(overload_osds.count(*i));
      overload_osds.erase(*i);
    }
  }

  // serialize, unserialize
  void encode(bufferlist& blist) {
    ::_encode(epoch, blist);
    ::_encode(mon_epoch, blist);
    ::_encode(ctime, blist);
    ::_encode(pg_num, blist);
    ::_encode(localized_pg_num, blist);
    
    ::_encode(osds, blist);
    ::_encode(down_osds, blist);
    ::_encode(out_osds, blist);
    ::_encode(overload_osds, blist);
    ::_encode(osd_inst, blist);
    
    crush._encode(blist);
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    ::_decode(epoch, blist, off);
    ::_decode(mon_epoch, blist, off);
    ::_decode(ctime, blist, off);
    ::_decode(pg_num, blist, off);
    ::_decode(localized_pg_num, blist, off);
    calc_pg_masks();

    ::_decode(osds, blist, off);
    ::_decode(down_osds, blist, off);
    ::_decode(out_osds, blist, off);
    ::_decode(overload_osds, blist, off);
    ::_decode(osd_inst, blist, off);
    
    crush._decode(blist, off);
  }
 



  /****   mapping facilities   ****/

  // oid -> pg
  ObjectLayout file_to_object_layout(object_t oid, FileLayout& layout) {
    return make_object_layout(oid, layout.pg_type, layout.pg_size, layout.preferred, layout.object_stripe_unit);
  }

  ObjectLayout make_object_layout(object_t oid, int pg_type, int pg_size, int preferred=-1, int object_stripe_unit = 0) {
    static crush::Hash H(777);

    // calculate ps (placement seed)
    ps_t ps;
    switch (g_conf.osd_object_layout) {
    case OBJECT_LAYOUT_LINEAR:
      ps = stable_mod(oid.bno + oid.ino, pg_num, pg_num_mask);
      break;
      
    case OBJECT_LAYOUT_HASHINO:
      ps = stable_mod(oid.bno + H(oid.ino), pg_num, pg_num_mask);
      break;

    case OBJECT_LAYOUT_HASH:
      ps = stable_mod(H( (oid.bno & oid.ino) ^ ((oid.bno^oid.ino) >> 32) ), pg_num, pg_num_mask);
      break;

    default:
      assert(0);
    }

    // construct object layout
    return ObjectLayout(pg_t(pg_type, pg_size, ps, preferred), 
			object_stripe_unit);
  }


  // pg -> (osd list)
  int pg_to_osds(pg_t pg,
                 vector<int>& osds) {       // list of osd addr's
    // map to osds[]
    switch (g_conf.osd_pg_layout) {
    case PG_LAYOUT_CRUSH:
      {
	// what crush rule?
	int rule;
	if (pg.is_rep()) rule = CRUSH_REP_RULE(pg.size());
	else if (pg.is_raid4()) rule = CRUSH_RAID_RULE(pg.size());
	else assert(0);

	// forcefeed?
	int forcefeed = -1;
	if (pg.preferred() >= 0 &&
	    out_osds.count(pg.preferred()) == 0) 
	  forcefeed = pg.preferred();
	crush.do_rule(crush.rules[rule],
		      pg.ps(),
		      osds, 
		      out_osds, overload_osds,
		      forcefeed);
      }
      break;
      
    case PG_LAYOUT_LINEAR:
      for (int i=0; i<pg.size(); i++) 
	osds.push_back( (i + pg.ps()*pg.size()) % g_conf.num_osd );
      break;
      
    case PG_LAYOUT_HYBRID:
      {
	static crush::Hash H(777);
	int h = H(pg.ps());
	for (int i=0; i<pg.size(); i++) 
	  osds.push_back( (h+i) % g_conf.num_osd );
      }
      break;
      
    case PG_LAYOUT_HASH:
      {
	static crush::Hash H(777);
	for (int i=0; i<pg.size(); i++) {
	  int t = 1;
	  int osd = 0;
	  while (t++) {
	    osd = H(i, pg.ps(), t) % g_conf.num_osd;
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
	g_conf.osd_pg_layout != PG_LAYOUT_CRUSH) {
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
        osds.erase(osds.begin());  // oops, but it's down!
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
