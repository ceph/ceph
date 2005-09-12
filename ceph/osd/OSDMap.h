#ifndef __OSDMAP_H
#define __OSDMAP_H

/*
 * describe properties of the OSD cluster.
 *   disks, disk groups, total # osds,
 *   whether we're in limbo/recovery state, etc.
 *
 */
#include "config.h"
#include "include/types.h"
#include "msg/Message.h"
#include "common/Mutex.h"
#include "rush.h"

#include <vector>
#include <list>
#include <set>
#include <map>
using namespace std;


/*
 * some system constants
 */
#define NUM_RUSH_REPLICAS         4   // this should be big enough to cope w/ failing disks.

// from LSB to MSB:

#define OID_ONO_BITS       30       // 1mb * 10^9 = 1 petabyte files
#define OID_INO_BITS       (64-30)  // 2^34 =~ 16 billion files

#define PG_PS_BITS         32       // max bits for placement seed/group portion of placement group
#define PG_REP_BITS        10   
#define PG_PS_MASK         ((1LL<<PG_PS_BITS)-1)



/** OSDGroup
 * a group of identical disks added to the OSD cluster
 */
class OSDGroup {
 public:
  int         num_osds; // num disks in this group           (aka num_disks_in_cluster[])
  float       weight;   // weight (for data migration etc.)  (aka weight_cluster[])
  size_t      osd_size; // osd size (in MB)
  vector<int> osds;     // the list of actual osd's

  void _encode(bufferlist& bl) {
	bl.append((char*)&num_osds, sizeof(num_osds));
	bl.append((char*)&weight, sizeof(weight));
	bl.append((char*)&osd_size, sizeof(osd_size));
	::_encode(osds, bl);
  }
  void _decode(bufferlist& bl, int& off) {
	bl.copy(off, sizeof(num_osds), (char*)&num_osds);
	off += sizeof(num_osds);
	bl.copy(off, sizeof(weight), (char*)&weight);
	off += sizeof(weight);
	bl.copy(off, sizeof(osd_size), (char*)&osd_size);
	off += sizeof(osd_size);
	::_decode(osds, bl, off);
  }
};


/** OSDExtent
 * for mapping (ino, offset, len) to a (list of) byte extents in objects on osds
 */
class OSDExtent {
 public:
  int         osd;       // (acting) primary osd
  object_t    oid;       // object id
  pg_t        pg;        // placement group
  size_t      offset, len;   // extent within the object
  map<size_t, size_t>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)

  OSDExtent() : osd(0), oid(0), pg(0), offset(0), len(0) { }
};


/** OSDMap
 */
class OSDMap {
  __uint64_t version;           // what version of the osd cluster descriptor is this
  int pg_bits;                  // placement group bits 

  // RUSH disk groups
  vector<OSDGroup> osd_groups;  // RUSH disk groups

  set<int>         down_osds;   // list of down disks
  set<int>         failed_osds; // list of failed disks

  Rush             *rush;       // rush implementation

  Mutex  osd_cluster_lock;

 public:
  void init_rush() {

    // SAB
    osd_cluster_lock.Lock();

	if (rush) delete rush;
	rush = new Rush();
	
	int ngroups = osd_groups.size();
	for (int i=0; i<ngroups; i++) {
	  rush->AddCluster(osd_groups[i].num_osds,
					   osd_groups[i].weight);
	}

    // SAB
    osd_cluster_lock.Unlock();
  }


 public:
  OSDMap() : version(0), pg_bits(5), rush(0) { }
  ~OSDMap() {
	if (rush) { delete rush; rush = 0; }
  }

  __uint64_t get_version() { return version; }
  void inc_version() { version++; }

  int get_pg_bits() { return pg_bits; }
  void set_pg_bits(int b) { pg_bits = b; }

  // cluster state
  bool is_failed(int osd) { return failed_osds.count(osd) ? true:false; }
  
  int num_osds() {
	int n = 0;
	for (vector<OSDGroup>::iterator it = osd_groups.begin();
		 it != osd_groups.end();
		 it++) 
	  n += it->num_osds;
	return n;
  }
  void get_all_osds(set<int>& ls) {
	for (vector<OSDGroup>::iterator it = osd_groups.begin();
		 it != osd_groups.end();
		 it++) {
	  for (unsigned i=0; i<it->osds.size(); i++)
		ls.insert(it->osds[i]);
	}
  }

  int get_num_groups() { return osd_groups.size(); }
  OSDGroup& get_group(int i) { return osd_groups[i]; }
  void add_group(OSDGroup& g) { 
	osd_groups.push_back(g); 
	init_rush();
  }

  // serialize, unserialize
  void encode(bufferlist& blist);
  void decode(bufferlist& blist);



  /****   mapping facilities   ****/

  /* map (ino, ono) to an object name
	 (to be used on any osd in the proper replica group) */
  object_t file_to_object(inodeno_t ino,
						  size_t    ono) {  
	assert(ino < (1LL<<OID_INO_BITS));       // legal ino can't be too big
	assert(ono < (1LL<<OID_ONO_BITS));
	return (ino << OID_INO_BITS) + ono;
  }

  /* map (ino, blockno, nrep) into a placement group */
  ps_t object_to_ps(object_t oid) {
	static hash<int> H;
	return H(oid) & PG_PS_MASK;
  }

  pg_t ps_nrep_to_pg(ps_t ps, int nrep) {
	return ((pg_t)ps & ((1LL<<pg_bits)-1LL)) 
	  | ((pg_t)nrep << PG_PS_BITS);
  }

  pg_t file_to_pg(inode_t& inode, size_t ono) {
	return ps_nrep_to_pg( object_to_ps( file_to_object(inode.ino, ono) ),
						  inode.layout.num_rep );
  }
  
  /* get nrep from pgid */
  int pg_to_nrep(pg_t pg) {
	return pg >> PG_PS_BITS;
  }
  
  /* map (repgroup) to a raw list of osds.  
	 this is where we invoke RUSH. */
  int pg_to_raw_osds(pg_t pg,
					 int *osds) {       // list of osd addr's
	// get rush list
	assert(rush);
	int num_rep = pg_to_nrep(pg);
	rush->GetServersByKey( pg, num_rep, osds );
	return num_rep;
  }

  int pg_to_nonfailed_osds(pg_t pg,
						   vector<int>& osds) { // list of osd addr's
	// get rush list
	assert(rush);
	int raw[NUM_RUSH_REPLICAS];
	pg_to_raw_osds(pg, raw);
	
	int nrep = pg_to_nrep(pg);
	osds = vector<int>(nrep);
	int o = 0;
	for (int i=0; i<NUM_RUSH_REPLICAS && o<nrep; i++) {
	  if (failed_osds.count(raw[i])) continue;
	  osds[o++] = raw[i];
	}
	return o;
  }

  int pg_to_acting_osds(pg_t pg,
						vector<int>& osds) {         // list of osd addr's
	// get rush list
	assert(rush);
	int raw[NUM_RUSH_REPLICAS];
	pg_to_raw_osds(pg, raw);

	int nrep = pg_to_nrep(pg);
	osds = vector<int>(nrep);
	int o = 0;
	for (int i=0; i<NUM_RUSH_REPLICAS && o<nrep; i++) {
	  if (failed_osds.count(raw[i])) continue;
	  if (down_osds.count(raw[i])) continue;
	  osds[o++] = raw[i];
	}
	return o;
  }




  
  /****  ****/

  /* map pg to the primary osd */
  int get_pg_primary(pg_t pg) {
	vector<int> group;
	int nrep = pg_to_nonfailed_osds(pg, group);
	assert(nrep > 0);   // we fail!
	return group[0];
  }
  /* map pg to the _acting_ primary osd (primary may be down) */
  int get_pg_acting_primary(pg_t pg) {
	vector<int> group;
	int nrep = pg_to_acting_osds(pg, group);
	assert(nrep > 0);  // we fail!
	return group[0];
  }

  /* what replica # is a given osd? 0 primary, -1 for none. */
  int get_pg_role(pg_t pg, int osd) {
	vector<int> group;
	int nrep = pg_to_nonfailed_osds(pg, group);
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

  /* map (ino, offset, len) to a (list of) OSDExtents 
	 (byte ranges in objects on (primary) osds) */
  void file_to_extents(inode_t inode,
					   size_t len,
					   size_t offset,
					   list<OSDExtent>& extents) {
	/* we want only one extent per object!
	 * this means that each extent we read may map into different bits of the 
	 * final read buffer.. hence OSDExtent.buffer_extents
	 */
	map< object_t, OSDExtent > object_extents;

	// RUSHSTRIPE?
	if (inode.layout.policy == FILE_LAYOUT_RUSHSTRIPE) {
	  // layout constant
	  size_t stripes_per_object = inode.layout.object_size / inode.layout.stripe_size;
	  
	  size_t cur = offset;
	  size_t left = len;
	  while (left > 0) {
		// layout into objects
		size_t blockno = cur / inode.layout.stripe_size;
		size_t stripeno = blockno / inode.layout.stripe_count;
		size_t stripepos = blockno % inode.layout.stripe_count;
		size_t objectsetno = stripeno / stripes_per_object;
		size_t objectno = objectsetno * inode.layout.stripe_count + stripepos;
		
		// find oid, extent
		OSDExtent *ex = 0;
		object_t oid = file_to_object( inode.ino, objectno );
		if (object_extents.count(oid)) 
		  ex = &object_extents[oid];
		else {
		  ex = &object_extents[oid];
		  ex->oid = oid;
		  ex->pg = file_to_pg( inode, objectno );
		  ex->osd = get_pg_acting_primary( ex->pg );
		}
		
		// map range into object
		size_t block_start = (stripeno % stripes_per_object)*inode.layout.stripe_size;
		size_t block_off = cur % inode.layout.stripe_size;
		size_t max = inode.layout.stripe_size - block_off;
		
		size_t x_offset = block_start + block_off;
		size_t x_len;
		if (left > max)
		  x_len = max;
		else
		  x_len = left;
		
		if (ex->offset + ex->len == x_offset) {
		  // add to extent
		  ex->len += x_len;
		} else {
		  // new extent
		  assert(ex->len == 0);
		  assert(ex->offset == 0);
		  ex->offset = x_offset;
		  ex->len = x_len;
		}
		ex->buffer_extents[cur-offset] = x_len;
		
		//cout << "map: ino " << ino << " oid " << ex.oid << " osd " << ex.osd << " offset " << ex.offset << " len " << ex.len << " ... left " << left << endl;
		
		left -= x_len;
		cur += x_len;
	  }
	  
	  // make final list
	  for (map<object_t, OSDExtent>::iterator it = object_extents.begin();
		   it != object_extents.end();
		   it++) {
		extents.push_back(it->second);
	  }
	}
	else if (inode.layout.policy == FILE_LAYOUT_OSDLOCAL) {
	  // all in one object, on a specific OSD.
	  OSDExtent ex;
	  ex.osd = inode.layout.osd;
	  ex.oid = file_to_object( inode.ino, 0 );
	  ex.pg = PG_NONE;
	  ex.len = len;
	  ex.offset = offset;
	  ex.buffer_extents[0] = len;

	  extents.push_back(ex);
	}
	else {
	  assert(0);
	}
  }

};


#endif
