
#ifndef __MDCACHE_H
#define __MDCACHE_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <ext/hash_map>

#include "../include/types.h"
#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"


class MDS;
class Message;
class MExportDirPrep;
class MExportDirPrepAck;
class MExportDir;
class MExportDirAck;
class MExportDirNotify;
class MDiscover;
class MInodeUpdate;
class MDirUpdate;
class MInodeExpire;
class MInodeSyncStart;
class MInodeSyncAck;
class MInodeSyncRelease;
class MInodeSyncRecall;
class MInodeLockStart;
class MInodeLockAck;
class MInodeLockRelease;
class C_MDS_ExportFinish;

// DCache

typedef hash_map<inodeno_t, CInode*> inode_map_t;

typedef const char* pchar;

class MDCache {
 protected:
  CInode                       *root;        // root inode
 public:
  LRU                          *lru;         // lru for expiring items
 protected:
  inode_map_t                   inode_map;   // map of inodes by ino             
  MDS *mds;

  bool               opening_root;
  list<Context*>     waiting_for_root;

  set<CInode*>       imports;                // includes root (on mds0)
  set<CInode*>       exports;
  multimap<CInode*,CInode*>  nested_exports; // nested exports of (imports|root)


  friend class MDBalancer;

 public:
  MDCache(MDS *m);
  ~MDCache();
  

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
  size_t get_cache_size() { lru->lru_get_size(); }
  bool trim(__int32_t max = -1);   // trim cache

  void shutdown_start();
  bool shutdown_pass();
  bool shutdown();                    // clear cache (ie at shutodwn)

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

  CInode *get_containing_import(CInode *in);
  CInode *get_containing_export(CInode *in);


  // adding/removing
  bool remove_inode(CInode *in);
  bool add_inode(CInode *in);

  int link_inode( CInode *parent, string& dname, CInode *inode );


  int open_root(Context *c);
  int path_traverse(string& path, 
					vector<CInode*>& trace, 
					Message *req, 
					int onfail);

  // messages
  int proc_message(Message *m);

  int handle_discover(MDiscover *dis);

  // exporter
  void handle_export_dir_prep_ack(MExportDirPrepAck *m);
  void export_dir(CInode *in,
				  int mds);
  void export_dir_frozen(CInode *in,
						 int dest, 
						 double pop);
  void export_dir_walk(MExportDir *req,
					   class C_MDS_ExportFinish *fin,
					   CInode *idir);
  void export_dir_purge(CInode *idir, int newauth);
  void handle_export_dir_ack(MExportDirAck *m);
  
  // importer
  void handle_export_dir_prep(MExportDirPrep *m);
  void handle_export_dir(MExportDir *m);
  void import_dir_block(pchar& p, 
						CInode *containing_import, 
						int oldauth,
						list<Context*>& waiting_on_imported);

  // dir authoirty bystander
  void handle_export_dir_notify(MExportDirNotify *m);

  int send_inode_updates(CInode *in);
  void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CDir *in, int except=-1);
  void handle_dir_update(MDirUpdate *m);

  void handle_inode_expire(MInodeExpire *m);

  // soft sync locks
  bool read_soft_start(CInode *in, Message *m);
  int read_soft_finish(CInode *in);
  bool write_soft_start(CInode *in, Message *m);
  int write_soft_finish(CInode *in);

  void sync_start(CInode *in);
  void sync_release(CInode *in);
  void sync_wait(CInode *in);

  void handle_inode_sync_start(MInodeSyncStart *m);
  void handle_inode_sync_ack(MInodeSyncAck *m);
  void handle_inode_sync_release(MInodeSyncRelease *m);
  void handle_inode_sync_recall(MInodeSyncRecall *m);

  void inode_sync_ack(CInode *in, MInodeSyncStart *m);

  // hard locks  
  bool read_hard_try(CInode *in, Message *m);
  bool write_hard_start(CInode *in, Message *m);
  void write_hard_finish(CInode *in);

  void inode_lock_start(CInode *in);
  void inode_lock_release(CInode *in);
  void inode_lock_wait(CInode *in);

  void handle_inode_lock_start(MInodeLockStart *m);
  void handle_inode_lock_ack(MInodeLockAck *m);
  void handle_inode_lock_release(MInodeLockRelease *m);
			  

  // crap fns
  CInode* hack_get_file(string& fn);
  vector<CInode*> hack_add_file(string& fn, CInode* in);

  void dump() {
	if (root) root->dump();
  }

  void show_imports();
  void show_cache();

  void dump_to_disk(MDS *m) {
	if (root) root->dump_to_disk(m);
  }
};


#endif
