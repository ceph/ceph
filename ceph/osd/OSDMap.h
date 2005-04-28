#ifndef __OSDCLUSTER_H
#define __OSDCLUSTER_H

/*
 * describe properties of the OSD cluster.
 */

class OSDCluster {
  int size;
  
  // state to handle migration to new size.

 public:
  OSDCluster(int size) {
	this->size = size;
  }

  // cluster state
  bool is_migrating(); /* true if we're in the middle of adding/removing drives and in limbo.
						  data will be migrating, etc.
					   */
  
  // mapping facilities
  int file_to_osds(inodeno_t ino,
				   size_t    blockno,
				   list<int>& osds);  /* map (file, block) to a list of osds.  this is RUSH. */

  int file_to_object(inodeno_t ino,
					 size_t    blockno);  /* map (file, block) to an object name */
  
  // cluster modification
  void queue_add_disk_group();      // ...
  void queue_remove_disk_group();   // ...
  void start_migration();   // sets migrating=true?
  void finish_migration();  // ... whatever
  
};


#endif
