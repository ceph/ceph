// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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
#define PG_PS_MASK         ((1LL<<PG_PS_BITS)-1)



/** OSDMap
 */
class OSDMap {
  version_t version;     // what version of the osd cluster descriptor is this
  int       pg_bits;     // placement group bits 

  set<int>  osds;        // all osds
  set<int>  down_osds;   // list of down disks
  //set<int>  out_osds;    // list of unmapped disks

 public:
  Crush     crush;       // hierarchical map

  friend class OSDMonitor;
  friend class MDS;

 public:
  OSDMap() : version(0), pg_bits(5) { }

  // map info
  version_t get_version() { return version; }
  void inc_version() { version++; }

  int get_pg_bits() { return pg_bits; }
  void set_pg_bits(int b) { pg_bits = b; }


  /****  cluster state *****/
  int num_osds() { return osds.size(); }
  void get_all_osds(set<int>& ls) { ls = osds; }

  const set<int>& get_osds() { return osds; }
  const set<int>& get_down_osds() { return down_osds; }
  const set<int>& get_out_osds() { return crush.out; }
  
  bool is_down(int osd) { return down_osds.count(osd); }
  bool is_up(int osd) { return !is_down(osd); }
  bool is_out(int osd) { return crush.out.count(osd); }
  bool is_in(int osd) { return !is_in(osd); }
  
  void mark_down(int o) { down_osds.insert(o); }
  void mark_up(int o) { down_osds.erase(o); }
  void mark_out(int o) { crush.out.insert(o); }
  void mark_in(int o) { crush.out.erase(o); }

  // serialize, unserialize
  void encode(bufferlist& blist);
  void decode(bufferlist& blist);




  /****   mapping facilities   ****/

  // oid -> ps
  ps_t object_to_ps(object_t oid) {
	static crush::Hash H(777);
		
	switch (g_conf.osd_object_layout) {
	case OBJECT_LAYOUT_LINEAR:
	  {
		const object_t ono = oid & ((1 << OID_ONO_BITS)-1);
		const inodeno_t ino = oid >> OID_ONO_BITS;
		return (ono + ino) & PG_PS_MASK;
	  }

	case OBJECT_LAYOUT_HASHINO:
	  {
		const object_t ono = oid & ((1 << OID_ONO_BITS)-1);
		const inodeno_t ino = oid >> OID_ONO_BITS;
		return (ono + H(ino)) & PG_PS_MASK;
	  }

	case OBJECT_LAYOUT_HASH:
	  {
		return H( oid ^ (oid >> 32) ) & PG_PS_MASK;
	  }
	}
	assert(0);
	return 0;
  }

  // (ps, nrep) -> pg
  pg_t ps_nrep_to_pg(ps_t ps, int nrep) {
	return ((pg_t)ps & ((1ULL<<pg_bits)-1ULL)) 
	  | ((pg_t)nrep << PG_PS_BITS);
  }

  // pg -> nrep
  int pg_to_nrep(pg_t pg) {
	return pg >> PG_PS_BITS;
  }

  // pg -> ps
  int pg_to_ps(pg_t pg) {
	return pg & PG_PS_MASK;
  }
  

  // pg -> (osd list)
  int pg_to_osds(pg_t pg,
				 vector<int>& osds) {       // list of osd addr's
	int num_rep = pg_to_nrep(pg);
	pg_t ps = pg_to_ps(pg);
	
	// spread "on" ps bits around a bit (usually only low bits are set bc of pg_bits)
	int hps = ((ps >> 32) ^ ps);
	hps = hps ^ (hps >> 16);
	
	switch(g_conf.osd_pg_layout) {
	case PG_LAYOUT_CRUSH:
	  crush.do_rule(crush.rules[num_rep],
					hps,
					osds);
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
  int get_pg_acting_role(pg_t pg, int osd) {
	vector<int> group;
	int nrep = pg_to_acting_osds(pg, group);
	for (int i=0; i<nrep; i++) {
	  if (group[i] == osd) return i;
	}
	return -1;  // none
  }




};


#endif
