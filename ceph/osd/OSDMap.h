// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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
#include "msg/Message.h"
#include "common/Mutex.h"

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
#define PG_PS_BITS         32   // max bits for placement seed/group portion of PG
#define PG_REP_BITS        10   
#define PG_TYPE_BITS       2
#define PG_PS_MASK         ((1LL<<PG_PS_BITS)-1)

#define PG_TYPE_RAND     1   // default: distribution randomly
#define PG_TYPE_STARTOSD 2   // place primary on a specific OSD (named by the pg_bits)




/** OSDMap
 */
class OSDMap {
  epoch_t   epoch;         // what epoch of the osd cluster descriptor is this
  int       pg_bits;     // placement group bits 

  set<int>  osds;        // all osds
  set<int>  down_osds;   // list of down disks
  set<int>  out_osds;    // list of unmapped disks
  map<int,float> overload_osds; 

  bool  mkfs;

 public:
  Crush     crush;       // hierarchical map

  friend class OSDMonitor;
  friend class MDS;

 public:
  OSDMap() : epoch(0), pg_bits(5), mkfs(false) { }

  // map info
  epoch_t get_epoch() const { return epoch; }
  void inc_epoch() { epoch++; mkfs = false; }

  int get_pg_bits() const { return pg_bits; }
  void set_pg_bits(int b) { pg_bits = b; }

  bool is_mkfs() const { return mkfs; }
  void set_mkfs() { mkfs = true; }

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
  bool is_in(int osd) { return !is_in(osd); }
  
  void mark_down(int o) { down_osds.insert(o); }
  void mark_up(int o) { down_osds.erase(o); }
  void mark_out(int o) { out_osds.insert(o); }
  void mark_in(int o) { out_osds.erase(o); }

  // serialize, unserialize
  void encode(bufferlist& blist);
  void decode(bufferlist& blist);




  /****   mapping facilities   ****/

  // oid -> ps
  ps_t object_to_pg(object_t oid, FileLayout& layout) {
	static crush::Hash H(777);
		
	int policy = layout.object_layout;
	if (policy == 0) 
	  policy = g_conf.osd_object_layout;

	int type = PG_TYPE_RAND;
	pg_t ps;

	switch (policy) {
	case OBJECT_LAYOUT_LINEAR:
	  {
		const object_t ono = oid & ((1ULL << OID_ONO_BITS)-1ULL);
		const inodeno_t ino = oid >> OID_ONO_BITS;
		ps = (ono + ino) & PG_PS_MASK;
		ps &= ((1ULL<<pg_bits)-1ULL);
	  }
	  break;
	  
	case OBJECT_LAYOUT_HASHINO:
	  {
		const object_t ono = oid & ((1ULL << OID_ONO_BITS)-1ULL);
		const inodeno_t ino = oid >> OID_ONO_BITS;
		ps = (ono + H(ino)) & PG_PS_MASK;
		ps &= ((1ULL<<pg_bits)-1ULL);
	  }
	  break;

	case OBJECT_LAYOUT_HASH:
	  {
		ps = H( oid ^ (oid >> 32) ) & PG_PS_MASK;
		ps &= ((1ULL<<pg_bits)-1ULL);
	  }
	  break;

	case OBJECT_LAYOUT_STARTOSD:
	  {
		ps = layout.osd;
		type = PG_TYPE_STARTOSD;
	  }
	  break;
	}

	// construct final PG
	pg_t pg = type;
	pg = (pg << PG_REP_BITS) | (pg_t)layout.num_rep;
	pg = (pg << PG_PS_BITS) | ps;
	//cout << "pg " << hex << pg << dec << endl;
	return pg;
  }

  // (ps, nrep) -> pg
  pg_t ps_nrep_to_pg(ps_t ps, int nrep) {
	return ((pg_t)ps & ((1ULL<<pg_bits)-1ULL)) 
	  | ((pg_t)nrep << PG_PS_BITS)
	  | ((pg_t)PG_TYPE_RAND << (PG_PS_BITS+PG_REP_BITS));
  }
  pg_t osd_nrep_to_pg(int osd, int nrep) {
	return ((pg_t)osd)
	  | ((pg_t)nrep << PG_PS_BITS)
	  | ((pg_t)PG_TYPE_STARTOSD << (PG_PS_BITS+PG_REP_BITS));

  }

  // pg -> nrep
  int pg_to_nrep(pg_t pg) {
	return (pg >> PG_PS_BITS) & ((1ULL << PG_REP_BITS)-1);
  }

  // pg -> ps
  int pg_to_ps(pg_t pg) {
	return pg & PG_PS_MASK;
  }

  // pg -> pg_type
  int pg_to_type(pg_t pg) {
	return pg >> (PG_PS_BITS + PG_REP_BITS);
  }
  

  // pg -> (osd list)
  int pg_to_osds(pg_t pg,
				 vector<int>& osds) {       // list of osd addr's
	pg_t ps = pg_to_ps(pg);
	int num_rep = pg_to_nrep(pg);
	assert(num_rep > 0);
	int type = pg_to_type(pg);

	// spread "on" ps bits around a bit (usually only low bits are set bc of pg_bits)
	int hps = ((ps >> 32) ^ ps);
	hps = hps ^ (hps >> 16);
	
	if (num_rep > 0) {
	  switch(g_conf.osd_pg_layout) {
	  case PG_LAYOUT_CRUSH:
		crush.do_rule(crush.rules[num_rep],
					  hps,
					  osds,
					  out_osds, overload_osds);
		break;
		
	  case PG_LAYOUT_LINEAR:
		for (int i=0; i<num_rep; i++) 
		  osds.push_back( (i + ps*num_rep) % g_conf.num_osd );
		break;
		
	  case PG_LAYOUT_HYBRID:
		{
		  static crush::Hash H(777);
		  int h = H(hps);
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
			  osd = H(i, hps, t) % g_conf.num_osd;
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
	}

	if (type == PG_TYPE_STARTOSD) {
	  // already in there?
	  for (int i=1; i<num_rep; i++)
		if (osds[i] == (int)ps) {
		  // swap with position 0
		  osds[i] = osds[0];
		}
	  osds[0] = (int)ps;
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


  /* what replica # is a given osd? 0 primary, -1 for none. */
  int get_pg_role(pg_t pg, int osd) {
	vector<int> group;
	int nrep = pg_to_osds(pg, group);
	for (int i=0; i<nrep; i++) {
	  if (group[i] == osd) return i;
	}
	return -1;  // none
  }

  /* rank is -1 (stray), 0 (primary), 1,2,3,... (replica) */
  int get_pg_acting_rank(pg_t pg, int osd) {
	vector<int> group;
	int nrep = pg_to_acting_osds(pg, group);
	for (int i=0; i<nrep; i++) {
	  if (group[i] == osd) return i;
	}
	return -1;  // none
  }
  /* role is -1 (stray), 0 (primary), 1 (replica) */
  int get_pg_acting_role(pg_t pg, int osd) {
	vector<int> group;
	int nrep = pg_to_acting_osds(pg, group);
	for (int i=0; i<nrep; i++) {
	  if (group[i] == osd) return i>0 ? 1:0;
	}
	return -1;  // none
  }




};


#endif
