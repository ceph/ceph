
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
#include "Lock.h"


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
//class MInodeUpdate;
class MCacheExpire;
class MDirUpdate;
class MDentryUnlink;
class MInodeWriterClosed;
class MLock;

class MRenameWarning;
class MRenameNotify;
class MRenameNotifyAck;
class MRename;
class MRenamePrep;
class MRenameReq;
class MRenameAck;

class C_MDS_ExportFinish;

class MClientRequest;

class MHashDir;
class MHashDirAck;
class MUnhashDir;
class MUnhashDirAck;


// MDCache

typedef hash_map<inodeno_t, CInode*> inode_map_t;

typedef const char* pchar;

/** active_request_t
 * state we track for requests we are currently processing.
 * mostly information about locks held, so that we can drop them all
 * the request is finished or forwarded.  see request_*().
 */
typedef struct {
  CInode *ref;                                // reference inode
  map< CDentry*, vector<CDentry*> > traces;   // path pins held
  set< CDentry* >           xlocks;           // xlocks (local)
  set< CDentry* >           foreign_xlocks;   // xlocks on foreign hosts
} active_request_t;


class MDCache {
 protected:
  // the cache
  CInode                       *root;        // root inode
  LRU                           lru;         // lru for expiring items
  inode_map_t                   inode_map;   // map of inodes by ino            
 
  MDS *mds;

  // root
  list<Context*>     waiting_for_root;

  // imports and exports
  set<CDir*>             imports;                // includes root (on mds0)
  set<CDir*>             exports;
  map<CDir*,set<CDir*> > nested_exports;
  
  // export fun
  map<CDir*, set<int> >  export_notify_ack_waiting; // nodes i am waiting to get export_notify_ack's from
  map<CDir*, list<inodeno_t> > export_proxy_inos;
  map<CDir*, list<inodeno_t> > export_proxy_dirinos;

  set<inodeno_t>                    stray_export_warnings; // notifies i haven't seen
  map<inodeno_t, MExportDirNotify*> stray_export_notifies;

  // rename fun
  set<inodeno_t>                    stray_rename_warnings; // notifies i haven't seen
  map<inodeno_t, MRenameNotify*>    stray_rename_notifies;

  // hashing madness
  multimap<CDir*, int>   unhash_waiting;  // nodes i am waiting for UnhashDirAck's from
  multimap<inodeno_t, inodeno_t>    import_hashed_replicate_waiting;  // nodes i am waiting to discover to complete my import of a hashed dir
        // maps frozen_dir_ino's to waiting-for-discover ino's.
  multimap<inodeno_t, inodeno_t>    import_hashed_frozen_waiting;    // dirs i froze (for the above)
  // maps import_root_ino's to frozen dir ino's (with pending discovers)



 public:
  // active MDS requests
  map<Message*, active_request_t>   active_requests;
  

  friend class MDBalancer;

 public:
  MDCache(MDS *m);
  ~MDCache();
  
  // root inode
  CInode *get_root() { return root; }
  void set_root(CInode *r) {
	root = r;
	add_inode(root);
  }

  // cache
  size_t set_cache_size(size_t max) {
	lru.lru_set_max(max);
  }
  size_t get_cache_size() { lru.lru_get_size(); }
  bool trim(__int32_t max = -1);   // trim cache

  // shutdown
  void shutdown_start();
  bool shutdown_pass();
  bool shutdown();                    // clear cache (ie at shutodwn)

  // have_inode?
  bool have_inode( inodeno_t ino ) {
	return inode_map.count(ino) ? true:false;
  }

  // return inode* or null
  CInode* get_inode( inodeno_t ino ) {
	if (have_inode(ino))
	  return inode_map[ ino ];
	return NULL;
  }
  
 protected:
  CDir *get_containing_import(CDir *in);
  CDir *get_containing_export(CDir *in);


  // adding/removing
 public:
  CInode *create_inode();
  void add_inode(CInode *in);
 protected:
  void remove_inode(CInode *in);
  void destroy_inode(CInode *in);

 public:
  void export_empty_import(CDir *dir);

 protected:
  void rename_file(CDentry *srcdn, CDentry *destdn);
  void fix_renamed_dir(CDir *srcdir,
					   CInode *in,
					   CDir *destdir,
					   bool authchanged,   // _inode_ auth changed
					   int dirauth=-1);    // dirauth (for certain cases)

 public:
  int open_root(Context *c);
  int path_traverse(filepath& path, vector<CDentry*>& trace, bool follow_trailing_sym,
					Message *req, Context *ondelay,
					int onfail,
					Context *onfinish=0);
  void open_remote_dir(CInode *diri, Context *fin);

  bool path_pin(vector<CDentry*>& trace, Message *m, Context *c);
  void path_unpin(vector<CDentry*>& trace, Message *m);
  void make_trace(vector<CDentry*>& trace, CInode *in);
  
  bool request_start(Message *req,
					 CInode *ref,
					 vector<CDentry*>& trace);
  void request_cleanup(Message *req);
  void request_finish(Message *req);
  void request_forward(Message *req, int mds, int port=0);


  // == messages ==
  int proc_message(Message *m);

  // -- replicas --
  void handle_discover(MDiscover *dis);
  void handle_discover_reply(MDiscoverReply *m);

  void handle_inode_writer_closed(MInodeWriterClosed *m);

  // -- namespace --
  // these handle logging, cache sync themselves.
  void dentry_unlink(CDentry *in, Context *c);
  void handle_dentry_unlink(MDentryUnlink *m);

  // initiator
  void file_rename(CDentry *srcdn, CDentry *destdn, Context *c);
  void handle_rename_ack(MRenameAck *m);              // dest -> init (almost always)
  void file_rename_finish(CDir *srcdir, CInode *in, Context *c);

  // src
  void handle_rename_req(MRenameReq *m);              // dest -> src
  void file_rename_foreign_src(CDentry *srcdn, 
							   inodeno_t destdirino, string& destname, string& destpath, int destauth, 
							   int initiator);
  void file_rename_warn(CInode *in, set<int>& notify);
  void handle_rename_notify_ack(MRenameNotifyAck *m); // bystanders -> src
  void file_rename_ack(CInode *in, int initiator);

  // dest
  void handle_rename_prep(MRenamePrep *m);            // init -> dest
  void handle_rename(MRename *m);                     // src -> dest
  void file_rename_notify(CInode *in, 
						  CDir *srcdir, string& srcname, CDir *destdir, string& destname,
						  set<int>& notify, int srcauth);

  // bystander
  void handle_rename_warning(MRenameWarning *m);      // src -> bystanders
  void handle_rename_notify(MRenameNotify *m);        // dest -> bystanders




  


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
  void find_nested_exports(CDir *dir, set<CDir*>& s);
  void find_nested_exports_under(CDir *import, CDir *dir, set<CDir*>& s);

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
  
  void encode_export_inode(CInode *in, crope& r);
  

  // importer
  void handle_export_dir_discover(MExportDirDiscover *m);
  void handle_export_dir_discover_2(MExportDirDiscover *m, CInode *in, int r);
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

  void decode_import_inode(CDentry *dn, crope& r, int &off, int oldauth);

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
  //int send_inode_updates(CInode *in);
  //void handle_inode_update(MInodeUpdate *m);

  int send_dir_updates(CDir *in, int except=-1);
  void handle_dir_update(MDirUpdate *m);

  void handle_cache_expire(MCacheExpire *m);

  // -- locks --
  // high level interface
  bool inode_hard_read_try(CInode *in, Context *con);
  bool inode_hard_read_start(CInode *in, MClientRequest *m);
  void inode_hard_read_finish(CInode *in);
  bool inode_hard_write_start(CInode *in, MClientRequest *m);
  void inode_hard_write_finish(CInode *in);
  bool inode_soft_read_start(CInode *in, MClientRequest *m);
  void inode_soft_read_finish(CInode *in);
  bool inode_soft_write_start(CInode *in, MClientRequest *m);
  void inode_soft_write_finish(CInode *in);

  void inode_hard_mode(CInode *in, int mode);
  void inode_soft_mode(CInode *in, int mode);

  // low level triggers
  void inode_hard_sync(CInode *in);
  void inode_hard_lock(CInode *in);
  bool inode_soft_sync(CInode *in);
  void inode_soft_lock(CInode *in);
  void inode_soft_async(CInode *in);

  void inode_hard_eval(CInode *in);
  void inode_soft_eval(CInode *in);

  // messengers
  void handle_lock(MLock *m);
  void handle_lock_inode_hard(MLock *m);
  void handle_lock_inode_soft(MLock *m);


  // dirs
  void handle_lock_dir(MLock *m);

  // dentry locks
  bool dentry_xlock_start(CDentry *dn, 
						  Message *m, CInode *ref);
  void dentry_xlock_finish(CDentry *dn, bool quiet=false);
  void handle_lock_dn(MLock *m);
  void dentry_xlock_request(CDir *dir, string& dname, bool create,
							Message *req, Context *onfinish);

  

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
