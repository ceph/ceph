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
#define NUM_REPLICA_GROUPS   (1<<20)  // ~1M
#define NUM_RUSH_REPLICAS         4   // this should be big enough to cope w/ failing disks.
#define MAX_REPLICAS              3

//#define FILE_OBJECT_SIZE     (1<<20)  // 1 MB object size

#define OID_ONO_BITS       30       // 1mb * 10^9 = 1 petabyte files
#define OID_INO_BITS       (64-30)  // 2^34 =~ 16 billion files

//#define MAX_FILE_SIZE      (FILE_OBJECT_SIZE << OID_ONO_BITS)  // 1 PB


/** OSDFileLayout 
 * specifies a striping strategy
 */

class OSDFileLayout {
 public:
  int stripe_size;     // stripe unit, in bytes
  int stripe_count;    // over this many objects
  int object_size;     // until objects are this big, then use a new set of objects.

  OSDFileLayout(int ss, int sc, int os) :
	stripe_size(ss), stripe_count(sc), object_size(os) { }
};



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
  repgroup_t  rg;        // replica group
  size_t      offset, len;   // extent within the object
  map<size_t, size_t>  buffer_extents;  // off -> len.  extents in buffer being mapped (may be fragmented bc of striping!)

  OSDExtent() : osd(0), oid(0), rg(0), offset(0), len(0) { }
};


/** OSDMap
 */
class OSDMap {
  __uint64_t version;           // what version of the osd cluster descriptor is this

  // RUSH disk groups
  vector<OSDGroup> osd_groups;  // RUSH disk groups

  set<int>         down_osds;   // list of down disks
  set<int>         failed_osds; // list of failed disks

  Rush             *rush;       // rush implementation

  Mutex  osd_cluster_lock;

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
  OSDMap() : version(0), rush(0) { }
  ~OSDMap() {
	if (rush) { delete rush; rush = 0; }
  }

  __uint64_t get_version() { return version; }

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

  /* map (ino, blockno) into a replica group */
  repgroup_t file_to_repgroup(inodeno_t ino, 
							  size_t ono) {
	// something simple for now
	// hash this eventually!
	return (ino+ono) % NUM_REPLICA_GROUPS;
  }


  /* map (repgroup) to a list of osds.  
	 this is where we invoke RUSH. */
  int repgroup_to_osds(repgroup_t rg,
					   int *osds,         // list of osd addr's
					   int num_rep) {     // num replicas we want
	// get rush list
	assert(rush);
	rush->GetServersByKey( rg, num_rep, osds );
	return num_rep;
  }

  int repgroup_to_nonfailed_osds(repgroup_t rg,
								 int *osds,         // list of osd addr's
								 int num_rep) {     // num replicas we want
	// get rush list
	assert(rush);
	int group[NUM_RUSH_REPLICAS];
	rush->GetServersByKey( rg, NUM_RUSH_REPLICAS, group );
	int o = 0;
	for (int i=0; i<NUM_RUSH_REPLICAS && o<num_rep; i++) {
	  if (failed_osds.count(group[i])) continue;
	  osds[o++] = group[i];
	}
	return o;
  }

  int repgroup_to_acting_osds(repgroup_t rg,
							  int *osds,         // list of osd addr's
							  int num_rep) {     // num replicas we want
	// get rush list
	assert(rush);
	int group[NUM_RUSH_REPLICAS];
	rush->GetServersByKey( rg, NUM_RUSH_REPLICAS, group );
	int o = 0;
	for (int i=0; i<NUM_RUSH_REPLICAS && o<num_rep; i++) {
	  if (failed_osds.count(group[i])) continue;
	  if (down_osds.count(group[i])) continue;
	  osds[o++] = group[i];
	}
	return o;
  }



  /* map (ino, ono) to an object name
	 (to be used on any osd in the proper replica group) */
  object_t file_to_object(inodeno_t ino,
						  size_t    ono) {  
	assert(ino < (1LL<<OID_INO_BITS));       // legal ino can't be too big
	assert(ono < (1LL<<OID_ONO_BITS));
	return (ino << OID_INO_BITS) + ono;
  }

  
  /****  ****/

  /* map rg to the primary osd */
  int get_rg_primary(repgroup_t rg) {
	int group[1];
	int nrep = repgroup_to_nonfailed_osds(rg, group, 1);
	assert(nrep > 0);   // we fail!
	return group[0];
  }
  /* map rg to the _acting_ primary osd (primary may be down) */
  int get_rg_acting_primary(repgroup_t rg) {
	int group[1];
	int nrep = repgroup_to_acting_osds(rg, group, 1);
	assert(nrep > 0);  // we fail!
	return group[0];
  }

  /* what replica # is a given osd? 0 primary, -1 for none. */
  int get_rg_role(repgroup_t rg, int osd) {
	int group[NUM_RUSH_REPLICAS];
	int nrep = repgroup_to_osds(rg, group, NUM_RUSH_REPLICAS);
	int role = 0;
	for (int i=0; i<nrep; i++) {
	  if (failed_osds.count(group[i])) continue;
	  if (group[i] == osd) return role;
	  role++;
	}
	return -1;  // none
  }
  int get_rg_acting_role(repgroup_t rg, int osd) {
	int group[NUM_RUSH_REPLICAS];
	int nrep = repgroup_to_osds(rg, group, NUM_RUSH_REPLICAS);
	int role = 0;
	for (int i=0; i<nrep; i++) {
	  if (failed_osds.count(group[i])) continue;
	  if (down_osds.count(group[i])) continue;
	  if (group[i] == osd) return role;
	  role++;
	}
	return -1;  // none
  }

  /* map (ino, offset, len) to a (list of) OSDExtents 
	 (byte ranges in objects on osds) */
  void file_to_extents(inodeno_t ino,
					   OSDFileLayout& layout,
					   size_t len,
					   size_t offset,
					   list<OSDExtent>& extents) {
	/* we want only one extent per object!
	 * this means that each extent we read may map into different bits of the 
	 * final read buffer.. hence OSDExtent.buffer_extents
	 */
	map< object_t, OSDExtent > object_extents;

	// layout constant
	size_t stripes_per_object = layout.object_size / layout.stripe_size;

	size_t cur = offset;
	size_t left = len;
	while (left > 0) {
	  // layout into objects
	  size_t blockno = cur / layout.stripe_size;
	  size_t stripeno = blockno / layout.stripe_count;
	  size_t stripepos = blockno % layout.stripe_count;
	  size_t objectsetno = stripeno / stripes_per_object;
	  size_t objectno = objectsetno * layout.stripe_count + stripepos;

	  // find oid, extent
	  OSDExtent *ex = 0;
	  object_t oid = file_to_object( ino, objectno );
	  if (object_extents.count(oid)) 
		ex = &object_extents[oid];
	  else {
		ex = &object_extents[oid];
		ex->oid = oid;
		ex->rg = file_to_repgroup( ino, objectno );
		ex->osd = get_rg_acting_primary( ex->rg );
	  }

	  // map range into object
	  size_t block_start = (stripeno % stripes_per_object)*layout.stripe_size;
	  size_t block_off = cur % layout.stripe_size;
	  size_t max = layout.stripe_size - block_off;

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

};


#endif
