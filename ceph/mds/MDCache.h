
#ifndef __MDCACHE_H
#define __MDCACHE_H

#include <string>
#include <vector>
#include <map>
#include <ext/hash_map>

#include "../include/types.h"
#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"


class MDS;
class Message;
class MExportDir;
class MExportDirAck;
class MDiscover;
class MInodeUpdate;
class MDirUpdate;
class MInodeSyncStart;
class MInodeSyncAck;
class MInodeSyncRelease;

// DCache

typedef hash_map<inodeno_t, CInode*> inode_map_t;

class MDCache {
 protected:
  CInode                       *root;        // root inode
  LRU                          *lru;         // lru for expiring items
  inode_map_t                   inode_map;   // map of inodes by ino             
  MDS *mds;

  bool               opening_root;
  list<Context*>     waiting_for_root;


 public:
  MDCache(MDS *m) {
	mds = m;
	root = NULL;
	opening_root = false;
	lru = new LRU();
  }
  ~MDCache() {
	if (lru) { delete lru; lru = NULL; }
  }
  

  // accessors
  CInode *get_root() {
	return root;
  }
  void set_root(CInode *r) {
	root = r;
	add_inode(root);
  }

  // fn
  size_t set_cache_size(size_t max) {
	lru->lru_set_max(max);
  }
  bool trim(__int32_t max = -1);   // trim cache
  bool clear() {                  // clear cache
	return trim(0);  
  }

  // have_inode?
  bool have_inode( inodeno_t ino ) {
	inode_map_t::iterator it = inode_map.find(ino);
	if (it == inode_map.end()) return false;
	return true;
  }

  // return inode* or null
  CInode* get_inode( inodeno_t ino ) {
	if (have_inode(ino))
	  return inode_map[ ino ];
	return NULL;
  }

  // adding/removing
  bool remove_inode(CInode *in);
  bool add_inode(CInode *in);

  int link_inode( CInode *parent, string& dname, CInode *inode );


  int open_root(Context *c);
  int path_traverse(string& path, vector<CInode*>& trace, vector<string>& trace_dn, Message *req, int onfail);

  // messages
  int proc_message(Message *m);

  int handle_discover(MDiscover *dis);
  void handle_export_dir_ack(MExportDirAck *m);
  void handle_export_dir(MExportDir *m);

  void export_dir(CInode *in,
				  int mds);

  int send_inode_updates(CInode *in);
  void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CDir *in);
  void handle_dir_update(MDirUpdate *m);

  void handle_inode_sync_start(MInodeSyncStart *m);
  void handle_inode_sync_ack(MInodeSyncAck *m);
  void handle_inode_sync_release(MInodeSyncRelease *m);

  int read_start(CInode *in, Message *m);
  int read_wait(CInode *in, Message *m);
  int read_finish(CInode *in);
  int write_start(CInode *in, Message *m);
  int write_finish(CInode *in);
			  

  // crap fns
  CInode* get_file(string& fn);
  void add_file(string& fn, CInode* in);

  void dump() {
	if (root) root->dump();
  }

  void dump_to_disk(MDS *m) {
	if (root) root->dump_to_disk(m);
  }
};


#endif
