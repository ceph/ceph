// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include"crypto/CryptoLib.h"
using namespace CryptoLib;


/*
 * some system constants
 */

// from LSB to MSB,
#define PG_PS_BITS         16  // max bits for placement seed/group portion of PG
#define PG_REP_BITS        6   // up to 64 replicas   
#define PG_TYPE_BITS       2
#define PG_PS_MASK         ((1LL<<PG_PS_BITS)-1)

#define PG_TYPE_RAND     1   // default: distribution randomly
#define PG_TYPE_STARTOSD 2   // place primary on a specific OSD (named by the pg_bits)

// pg roles
#define PG_ROLE_STRAY   -1
#define PG_ROLE_HEAD     0
#define PG_ROLE_ACKER    1
#define PG_ROLE_MIDDLE   2  // der.. misnomer
//#define PG_ROLE_TAIL     2



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
    //map<int,string> added_osd_keys; // new public keys
    //list<int> removed_osd_keys; // public keys to remove
    
    void encode(bufferlist& bl) {
      bl.append((char*)&epoch, sizeof(epoch));
      bl.append((char*)&mon_epoch, sizeof(mon_epoch));
      bl.append((char*)&ctime, sizeof(ctime));
      ::_encode(new_up, bl);
      ::_encode(new_down, bl);
      ::_encode(new_in, bl);
      ::_encode(new_out, bl);
      ::_encode(new_overload, bl);
      //::_encode(added_osd_keys, bl);
      //::_encode(removed_osd_keys, bl);
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
      //::_decode(added_osd_keys, bl, off);
      //::_decode(removed_osd_keys, bl, off);
    }

    Incremental(epoch_t e=0) : epoch(e), mon_epoch(0) {}
  };

private:
  epoch_t   epoch;       // what epoch of the osd cluster descriptor is this
  epoch_t   mon_epoch;  // monitor epoch (election iteration)
  utime_t   ctime;       // epoch start time
  int       pg_bits;     // placement group bits 
  int       localized_pg_bits;  // bits for localized pgs

  set<int>  osds;        // all osds
  set<int>  down_osds;   // list of down disks
  set<int>  out_osds;    // list of unmapped disks
  map<int,float> overload_osds; 
  map<int,entity_inst_t> osd_inst;
  //map<int,string> osd_str_keys; //all public keys in str form
  //map<int,esignPub> osd_keys; // all public key objects (cache)

 public:
  Crush     crush;       // hierarchical map

  friend class OSDMonitor;
  friend class MDS;

 public:
  OSDMap() : epoch(0), mon_epoch(0), pg_bits(5), localized_pg_bits(3) {}

  // map info
  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; }

  int get_pg_bits() const { return pg_bits; }
  void set_pg_bits(int b) { pg_bits = b; }
  int get_localized_pg_bits() const { return localized_pg_bits; }

  const utime_t& get_ctime() const { return ctime; }

  bool is_mkfs() const { return epoch == 1; }

  /***** cluster state *****/
  int num_osds() { return osds.size(); }
  void get_all_osds(set<int>& ls) { ls = osds; }

  const set<int>& get_osds() { return osds; }
  const set<int>& get_down_osds() { return down_osds; }
  const set<int>& get_out_osds() { return out_osds; }
  const map<int,float>& get_overload_osds() { return overload_osds; }
  /*
  const map<int,string>& get_key_str_map() { return osd_str_keys; }
  const map<int,esignPub>& get_key_map() { return osd_keys; }
  const esignPub get_key(int client) { 
    // I have a cached key object
    if (osd_keys.count(client)) {
      return osd_keys[client];
    }
    // there must be a str key atleast for the client
    assert(osd_str_keys.count(client));
    esignPub tempPub = _fromStr_esignPubKey(osd_str_keys[client]);
    osd_keys[client] = tempPub;
    return tempPub;
  }
  const string& get_str_keys(int client) { return osd_str_keys[client]; }
  */
  
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
    // add the incremental keys to osd_keys
    /*
    for (map<int,string>::iterator i = inc.added_osd_keys.begin();
         i != inc.added_osd_keys.end();
         i++) {
      osd_str_keys[i->first] = i->second;
    }
    for (list<int>::iterator i = inc.removed_osd_keys.begin();
         i != inc.removed_osd_keys.end();
         i++) {
      assert(osd_str_keys.count(*i)); // sanity check
      osd_str_keys.erase(*i);
    }
    */
  }

  // serialize, unserialize
  void encode(bufferlist& blist) {
    blist.append((char*)&epoch, sizeof(epoch));
    blist.append((char*)&mon_epoch, sizeof(mon_epoch));
    blist.append((char*)&ctime, sizeof(ctime));
    blist.append((char*)&pg_bits, sizeof(pg_bits));
    
    _encode(osds, blist);
    _encode(down_osds, blist);
    _encode(out_osds, blist);
    _encode(overload_osds, blist);
    _encode(osd_inst, blist);
    //_encode(osd_keys, blist);
    
    crush._encode(blist);
  }
  
  void decode(bufferlist& blist) {
    int off = 0;
    blist.copy(off, sizeof(epoch), (char*)&epoch);
    off += sizeof(epoch);
    blist.copy(off, sizeof(mon_epoch), (char*)&mon_epoch);
    off += sizeof(mon_epoch);
    blist.copy(off, sizeof(ctime), (char*)&ctime);
    off += sizeof(ctime);
    blist.copy(off, sizeof(pg_bits), (char*)&pg_bits);
    off += sizeof(pg_bits);
    
    _decode(osds, blist, off);
    _decode(down_osds, blist, off);
    _decode(out_osds, blist, off);
    _decode(overload_osds, blist, off);
    _decode(osd_inst, blist, off);
    //_decode(osd_keys, blist, off);
    
    crush._decode(blist, off);
  }
 



  /****   mapping facilities   ****/

  // oid -> pg
  pg_t object_to_pg(object_t oid, FileLayout& layout) {
    static crush::Hash H(777);
        
    int policy = layout.object_layout;
    if (policy == 0) 
      policy = g_conf.osd_object_layout;

    int type = PG_TYPE_RAND;
    ps_t ps;

    switch (policy) {
    case OBJECT_LAYOUT_LINEAR:
      {
        //const object_t ono = oid.bno;
        //const inodeno_t ino = oid >> OID_ONO_BITS;
        ps = (oid.bno + oid.ino) & PG_PS_MASK;
        ps &= ((1ULL<<pg_bits)-1ULL);
      }
      break;
      
    case OBJECT_LAYOUT_HASHINO:
      {
        //const object_t ono = oid & ((1ULL << OID_ONO_BITS)-1ULL);
        //const inodeno_t ino = oid >> OID_ONO_BITS;
        ps = (oid.bno + H(oid.ino)) & PG_PS_MASK;
        ps &= ((1ULL<<pg_bits)-1ULL);
      }
      break;

    case OBJECT_LAYOUT_HASH:
      {
        ps = H( (oid.bno & oid.ino) ^ ((oid.bno^oid.ino) >> 32) ) & PG_PS_MASK;
        ps &= ((1ULL<<pg_bits)-1ULL);
      }
      break;

    case OBJECT_LAYOUT_STARTOSD:
      {
        ps = layout.osd;
        type = PG_TYPE_STARTOSD;
      }
      break;
      
    default:
      assert(0);
    }

    // construct final PG
    /*pg_t pg = type;
    pg = (pg << PG_REP_BITS) | (pg_t)layout.num_rep;
    pg = (pg << PG_PS_BITS) | ps;
    */
    //cout << "pg " << hex << pg << dec << endl;
    return pg_t(ps, 0, layout.num_rep);
  }

  // (ps, nrep) -> pg
  pg_t ps_nrep_to_pg(ps_t ps, int nrep) {
    /*return ((pg_t)ps & ((1ULL<<pg_bits)-1ULL)) 
      | ((pg_t)nrep << PG_PS_BITS)
      | ((pg_t)PG_TYPE_RAND << (PG_PS_BITS+PG_REP_BITS));
    */
    return pg_t(ps, 0, nrep, 0);
  }
  pg_t ps_osd_nrep_to_pg(ps_t ps, int osd, int nrep) {
    /*return ((pg_t)osd)
      | ((pg_t)nrep << PG_PS_BITS)
      | ((pg_t)PG_TYPE_STARTOSD << (PG_PS_BITS+PG_REP_BITS));
    */
    return pg_t(ps, osd+1, nrep, 0);
  }

  // pg -> nrep
  int pg_to_nrep(pg_t pg) {
    return pg.u.fields.nrep;
    //return (pg >> PG_PS_BITS) & ((1ULL << PG_REP_BITS)-1);
  }

  // pg -> ps
  int pg_to_ps(pg_t pg) {
    //return pg & PG_PS_MASK;
    return pg.u.fields.ps;
  }

  // pg -> (osd list)
  int pg_to_osds(pg_t pg,
                 vector<int>& osds) {       // list of osd addr's
    pg_t ps = pg_to_ps(pg);
    int num_rep = pg_to_nrep(pg);
    assert(num_rep > 0);
    
    // map to osds[]
    switch (g_conf.osd_pg_layout) {
    case PG_LAYOUT_CRUSH:
      {
	int forcefeed = -1;
	if (pg.u.fields.preferred > 0 &&
	    out_osds.count(pg.u.fields.preferred-1) == 0) 
	  forcefeed = pg.u.fields.preferred-1;
	crush.do_rule(crush.rules[num_rep],     // FIXME rule thing.
		      ps, 
		      osds,
		      out_osds, overload_osds,
		      forcefeed);
      }
      break;
      
    case PG_LAYOUT_LINEAR:
      for (int i=0; i<num_rep; i++) 
	osds.push_back( (i + ps*num_rep) % g_conf.num_osd );
      break;
      
    case PG_LAYOUT_HYBRID:
      {
	static crush::Hash H(777);
	int h = H(ps);
	for (int i=0; i<num_rep; i++) 
	  osds.push_back( (h+i) % g_conf.num_osd );
      }
      break;
      
    case PG_LAYOUT_HASH:
      {
	static crush::Hash H(777);
	for (int i=0; i<num_rep; i++) {
	  int t = 1;
	  int osd = 0;
	  while (t++) {
	    osd = H(i, ps, t) % g_conf.num_osd;
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
  
    if (pg.u.fields.preferred > 0 &&
	g_conf.osd_pg_layout != PG_LAYOUT_CRUSH) {
      int osd = pg.u.fields.preferred-1;

      // already in there?
      if (osds.empty()) {
        osds.push_back(osd);
      } else {
        assert(num_rep > 0);
        for (int i=1; i<num_rep; i++)
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
