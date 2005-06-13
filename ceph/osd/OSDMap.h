#ifndef __OSDCLUSTER_H
#define __OSDCLUSTER_H

/*
 * describe properties of the OSD cluster.
 *   disks, disk groups, total # osds,
 *   whether we're in limbo/recovery state, etc.
 *
 */
#include "include/config.h"
#include "include/types.h"
#include "msg/Message.h"
#include "rush.h"

#include <vector>
#include <list>
#include <set>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;


/*
 * some system constants
 */
#define NUM_REPLICA_GROUPS   (1<<20)  // ~1M
#define NUM_RUSH_REPLICAS        10   // this should be big enough to cope w/ failing disks.
#define MAX_REPLICAS              3

#define FILE_OBJECT_SIZE    (1<<20)  // 1 MB object size

#define OID_BLOCK_BITS     30       // 1mb * 10^9 = 1 petabyte files
#define OID_INO_BITS       (64-30)  // 2^34 =~ 16 billion files

#define MAX_FILE_SIZE      (FILE_OBJECT_SIZE << OID_BLOCK_BITS)  // 1 PB


/** OSDGroup
 * a group of identical disks added to the OSD cluster
 */
struct OSDGroup {
  int         num_osds; // num disks in this group           (aka num_disks_in_cluster[])
  float       weight;   // weight (for data migration etc.)  (aka weight_cluster[])
  size_t      osd_size; // osd size (in MB)
  vector<int> osds;     // the list of actual osd's
};


/** OSDExtent
 * for mapping (ino, offset, len) to a (list of) byte extents in objects on osds
 */
struct OSDExtent {
  int         osd;
  object_t    oid;
  repgroup_t  rg;
  size_t      offset, len;
};


/** OSDCluster
 */
class OSDCluster {
  __uint64_t version;           // what version of the osd cluster descriptor is this

  // RUSH disk groups
  vector<OSDGroup> osd_groups;  // RUSH disk groups

  set<int>         down_osds;   // list of down disks
  set<int>         failed_osds; // list of failed disks

  Rush             *rush;       // rush implementation

  void init_rush() {
	if (rush) delete rush;
	rush = new Rush();
	
	int ngroups = osd_groups.size();
	for (int i=0; i<ngroups; i++) 
	  rush->AddCluster(osd_groups[i].num_osds,
					   osd_groups[i].weight);
  }


 public:
  OSDCluster() : version(0), rush(0) { }

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
	  for (int i=0; i<it->osds.size(); i++)
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
  //void _rope(crope& r);
  //void _unrope(crope& r, int& off);
  void encode(bufferlist& blist);
  void decode(bufferlist& blist);



  /****   mapping facilities   ****/

  /* map (ino, blockno) into a replica group */
  repgroup_t file_to_repgroup(inodeno_t ino, 
							  size_t blockno) {
	// something simple for now
	// hash this eventually
	return (ino+blockno) % NUM_REPLICA_GROUPS;
  }


  /* map (repgroup) to a list of osds.  
	 this is where we (will eventually) use RUSH. */
  int repgroup_to_osds(repgroup_t rg,
					   int *osds,         // list of osd addr's
					   int num_rep) {     // num replicas we want
	// get rush list
	assert(rush);
	rush->GetServersByKey( rg, num_rep, osds );
	return 0;
  }


  /* map (ino, block) to an object name
	 (to be used on any osd in the proper replica group) */
  object_t file_to_object(inodeno_t ino,
						  size_t    blockno) {  
	assert(ino < (1LL<<OID_INO_BITS));       // legal ino can't be too big
	assert(blockno < (1LL<<OID_BLOCK_BITS));
	return (ino << OID_INO_BITS) + blockno;
  }

  
  /****  ****/

  /* map rg to the primary osd */
  int get_rg_primary(repgroup_t rg) {
	int group[NUM_RUSH_REPLICAS];
	repgroup_to_osds(rg, group, NUM_RUSH_REPLICAS);
	for (int i=0; i<NUM_RUSH_REPLICAS; i++) {
	  if (failed_osds.count(group[i])) continue;
	  return group[i];
	}
	assert(0);
	return -1;  // we fail!

  }
  /* map rg to the _acting_ primary osd (primary may be down) */
  int get_rg_acting_primary(repgroup_t rg) {
	int group[NUM_RUSH_REPLICAS];
	repgroup_to_osds(rg, group, NUM_RUSH_REPLICAS);
	for (int i=0; i<NUM_RUSH_REPLICAS; i++) {
	  if (down_osds.count(group[i])) continue;
	  if (failed_osds.count(group[i])) continue;
	  return group[i];
	}
	assert(0);
	return -1;  // we fail!
  }

  /* map (ino, offset, len) to a (list of) OSDExtents 
	 (byte ranges in objects on osds) */
  void file_to_extents(inodeno_t ino,
					   size_t len,
					   size_t offset,
					   list<OSDExtent>& extents) {
	size_t cur = offset;
	size_t left = len;
	while (left > 0) {
	  OSDExtent ex;
	  
	  // find oid, osds
	  size_t blockno = cur / FILE_OBJECT_SIZE;
	  ex.oid = file_to_object( ino, blockno );
	  ex.rg = file_to_repgroup(ino, blockno );
	  ex.osd = get_rg_acting_primary( ex.rg );

	  // map range into object
	  ex.offset = cur % FILE_OBJECT_SIZE;
	  if (left + ex.offset > FILE_OBJECT_SIZE) 
		ex.len = FILE_OBJECT_SIZE - ex.offset;	 // doesn't fully fit
	  else
		ex.len = left;		                     // fits!

	  left -= ex.len;
	  cur += ex.len;

	  // add it
	  extents.push_back(ex);
	}
  }

};


#endif
