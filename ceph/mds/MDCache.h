
#ifndef __MDCACHE_H
#define __MDCACHE_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <ext/hash_map>

#include "include/types.h"
#include "include/filepath.h"
#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"
//#include "InoProxySet.h"

class MDS;
class Message;
class MExportDirDiscover;
class MExportDirDiscoverAck;
class MExportDirPrep;
class MExportDirPrepAck;
class MExportDirWarning;
class MExportDir;
class MExportDirNotify;
class MExportDirNotifyAck;
class MExportDirFinish;
class MDiscover;
class MDiscoverReply;
class MInodeGetReplica;
class MInodeGetReplicaAck;
class MInodeUpdate;
class MCacheExpire;
class MDirUpdate;
class MInodeUnlink;
class MInodeUnlinkAck;
class MInodeSyncStart;
class MInodeSyncAck;
class MInodeSyncRelease;
class MInodeSyncRecall;
class MInodeLockStart;
class MInodeLockAck;
class MInodeLockRelease;
class MDirSyncStart;
class MDirSyncAck;
class MDirSyncRelease;
class C_MDS_ExportFinish;
class InoAllocator;

class MHashDir;
class MHashDirAck;
class MUnhashDir;
class MUnhashDirAck;


// MDCache

typedef hash_map<inodeno_t, CInode*> inode_map_t;

typedef const char* pchar;

class MDCache {
 protected:
  // the cache
  CInode                       *root;        // root inode
  LRU                          *lru;         // lru for expiring items
  inode_map_t                   inode_map;   // map of inodes by ino            
  InoAllocator                 *inoalloc;
 
  MDS *mds;

  // root
  list<Context*>     waiting_for_root;

  // imports and exports
  set<CDir*>             imports;                // includes root (on mds0)
  set<CDir*>             exports;
  multimap<CDir*,CDir*>  nested_exports;   // nested exports of (imports|root)
  
  multimap<CDir*, int>   unhash_waiting;  // nodes i am waiting for UnhashDirAck's from
  multimap<inodeno_t, inodeno_t>    import_hashed_replicate_waiting;  // nodes i am waiting to discover to complete my import of a hashed dir
        // maps frozen_dir_ino's to waiting-for-discover ino's.
  multimap<inodeno_t, inodeno_t>    import_hashed_frozen_waiting;    // dirs i froze (for the above)
        // maps import_root_ino's to frozen dir ino's (with pending discovers)

  map<CDir*, set<int> >   export_notify_ack_waiting; // nodes i am waiting to get export_notify_ack's from
  map<CDir*, set<inodeno_t> > export_proxy_inos;
  map<CDir*, set<inodeno_t> > export_proxy_dirinos;

  set<inodeno_t>         stray_export_warnings; // warnings for export dirs i don't have open
  map<inodeno_t, MExportDirNotify*> stray_export_notifies;

  // inoproxysets
  //list<InoProxySet*>      ino_proxy_sets;

  
  friend class MDBalancer;

 public:
  MDCache(MDS *m);
  ~MDCache();
  

  // accessors
  CInode *get_root() { return root; }
  void set_root(CInode *r) {
	root = r;
	add_inode(root);
  }
  LRU *get_lru() { return lru; }


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

  CDir *get_containing_import(CDir *in);
  CDir *get_containing_export(CDir *in);


  // adding/removing
  CInode *create_inode();
  void add_inode(CInode *in);
  void remove_inode(CInode *in);
  void destroy_inode(CInode *in);

  int link_inode( CDir *dir, string& dname, CInode *inode );
  void unlink_inode( CInode *inode );

  int open_root(Context *c);
  int path_traverse(filepath& path, 
					vector<CInode*>& trace, 
					Message *req, 
					int onfail);

  
  // == messages ==
  int proc_message(Message *m);

  // -- replicas --
  void handle_discover(MDiscover *dis);
  void handle_discover_reply(MDiscoverReply *m);
  void handle_inode_get_replica(MInodeGetReplica *m);
  void handle_inode_get_replica_ack(MInodeGetReplicaAck *m);  


  // -- namespace --
  // these handle logging, cache sync themselves.
  void inode_unlink(CInode *in, Context *c);
  void inode_unlink_finish(CInode *in);

  void handle_inode_unlink(MInodeUnlink *m);
  void handle_inode_unlink_ack(MInodeUnlinkAck *m);


  // -- misc auth --
  int ino_proxy_auth(inodeno_t ino, 
					 int frommds,
					 map<CDir*, set<inodeno_t> >& inomap);
  void do_ino_proxy(CInode *in, Message *m);
  void do_dir_proxy(CDir *dir, Message *m);


  // -- import/export --
  bool is_import(CDir *dir) {
	assert(dir->is_import() == imports.count(dir));
	return dir->is_import();
  }
  bool is_export(CDir *dir) {
	return exports.count(dir);
  }
  void find_nested_exports(CDir *dir, list<CDir*>& ls);

  // exporter
  void export_dir(CDir *dir,
				  int mds);
  void export_dir_dropsync(CDir *dir);
  void handle_export_dir_discover_ack(MExportDirDiscoverAck *m);
  void export_dir_frozen(CDir *dir, int dest);
  void handle_export_dir_prep_ack(MExportDirPrepAck *m);
  void export_dir_go(CDir *dir,
					 int dest,
					 double pop);
  void export_dir_walk(MExportDir *req,
					   class C_MDS_ExportFinish *fin,
					   CDir *basedir,
					   CDir *dir,
					   int newauth);
  void export_dir_finish(CDir *dir);
  void handle_export_dir_notify_ack(MExportDirNotifyAck *m);
  
  // importer
  /*CInode *import_dentry_inode(CDir *dir, 
							  pchar& p, 
							  int from, 
							  CDir *import_root=0,
							  int *would_be_dir_auth = 0); // need for normal import
  */
  void handle_export_dir_discover(MExportDirDiscover *m);
  void handle_export_dir_prep(MExportDirPrep *m);
  void handle_export_dir(MExportDir *m);
  void import_dir_finish(CDir *dir);
  void handle_export_dir_finish(MExportDirFinish *m);
  void import_dir_block(crope& r,
						int& off,
						int oldauth,
						CDir *import_root,
						list<inodeno_t>& imported_subdirs);
  void got_hashed_replica(CDir *import,
						  inodeno_t dir_ino,
						  inodeno_t replica_ino);

  // bystander
  void handle_export_dir_warning(MExportDirWarning *m);
  void handle_export_dir_notify(MExportDirNotify *m);


  // -- hashed directories --
  // hash on auth
  void hash_dir(CDir *dir);
  void hash_dir_complete(CDir *dir);
  void hash_dir_finish(CDir *dir);

  // hash on non-auth
  void handle_hash_dir(MHashDir *m);

  // unhash on auth
  void unhash_dir(CDir *dir);
  void unhash_dir_complete(CDir *dir);
  void handle_unhash_dir_ack(MUnhashDirAck *m);
  void unhash_dir_finish(CDir *dir);

  // unhash on non-auth
  void handle_unhash_dir(MUnhashDir *m);
  void handle_unhash_dir_complete(CDir *dir, int auth);
  void handle_unhash_dir_finish(CDir *dir, int auth);

  void drop_sync_in_dir(CDir *dir);

  // -- updates --
  int send_inode_updates(CInode *in);
  void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CDir *in, int except=-1);
  void handle_dir_update(MDirUpdate *m);

  void handle_cache_expire(MCacheExpire *m);


  // -- lock and sync : inodes --
  // soft sync locks
  bool read_soft_start(CInode *in, Message *m);
  int read_soft_finish(CInode *in);
  bool write_soft_start(CInode *in, Message *m);
  int write_soft_finish(CInode *in);

  void inode_sync_start(CInode *in);
  void inode_sync_release(CInode *in);
  void inode_sync_wait(CInode *in);

  void handle_inode_sync_start(MInodeSyncStart *m);
  void handle_inode_sync_ack(MInodeSyncAck *m);
  void handle_inode_sync_release(MInodeSyncRelease *m);
  void handle_inode_sync_recall(MInodeSyncRecall *m);

  void inode_sync_ack(CInode *in, MInodeSyncStart *m, bool wantback=false);

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
			  

  // -- sync : dirs --
  void dir_sync_start(CDir *dir);
  void dir_sync_release(CDir *dir);
  void dir_sync_wait(CDir *dir);

  void handle_dir_sync_start(MDirSyncStart *m);
  void handle_dir_sync_ack(MDirSyncAck *m);
  void handle_dir_sync_release(MDirSyncRelease *m);


  // == crap fns ==
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
