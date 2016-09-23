#ifndef CEPH_MDCACHE_H
#define CEPH_MDCACHE_H

#include "mdstypes.h"
#include "CObject.h"
#include "include/elist.h"
#include "include/lru.h"

class Message;
class MClientRequest;
class MDSRank;
class Server;
class Locker;
class filepath;
class LogSegment;
class EMetaBlob;
class ESubtreeMap;
class Session;
class Filer;
class RecoveryQueue;
class StrayManager;

struct MutationImpl;
struct MDRequestImpl;
typedef ceph::shared_ptr<MutationImpl> MutationRef;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;

// flags for predirty_journal_parents()
static const int PREDIRTY_PRIMARY = 1; // primary dn, adjust nested accounting
static const int PREDIRTY_DIR = 2;     // update parent dir mtime/size
static const int PREDIRTY_SHALLOW = 4; // only go to immediate parent (for easier rollback)

class MDCache {
public:
  MDSRank* const mds;
  Server* const &server;
  Locker* const &locker;

protected:
  Mutex inode_map_lock;
  ceph::unordered_map<vinodeno_t,CInode*> inode_map;

  CInodeRef root;
  CInodeRef myin;
  CInodeRef strays[NUM_STRAY]; 
  int stray_index;

public:
  CInodeRef create_system_inode(inodeno_t ino, int mode);
  void create_empty_hierarchy(MDSGather *gather);
  void create_mydir_hierarchy(MDSGather *gather);

  void open_root_and_mydir(MDSContextBase *fin);
  void populate_mydir(MDSGatherBuilder& gather);

  void add_inode(CInode *in);
  void remove_inode(CInode *in);
  void remove_inode_recursive(CInode *in);

  CInodeRef get_inode(const vinodeno_t &vino);
  CInodeRef get_inode(inodeno_t ino, snapid_t s=CEPH_NOSNAP) {
    return get_inode(vinodeno_t(ino, s));
  }
  CDirRef get_dirfrag(const dirfrag_t &df);

  int path_traverse(const MDRequestRef& mdr,
		    const filepath& path, vector<CDentryRef> *pdnvec, CInodeRef *pin);
protected:
  RecoveryQueue *recovery_queue;
public:
  void queue_file_recovery(CInode *in);
  void prioritize_file_recovery(CInode *in);

protected:
  StrayManager *stray_manager;
  void scan_stray_dir(dirfrag_t dirfrag, string last_name);
  friend struct C_MDC_RetryScanStray;
public:
  void advance_stray() { stray_index = (stray_index + 1) % NUM_STRAY; }
  CDentryRef get_or_create_stray_dentry(CInode *in);
  void scan_strays();
  void eval_stray(CDentry *dn);

protected:
  LRU dentry_lru;
public:
  void dentry_lru_insert(CDentry *dn);
  void dentry_lru_remove(CDentry *dn);
  void touch_dentry(CDentry* dn);
  void touch_dentry_bottom(CDentry* dn);
  void touch_inode(CInode* in);

  void trim(int max=-1, int count=-1);
  bool trim_dentry(CDentry *dn, bool *null_straydn);
  bool trim_inode(CDentry *dn, CInode *in);
  void trim_dirfrag(CInode *in, CDir *dir);

protected:
  elist<CInode*>  replay_undef_inodes;
public:
  CInodeRef replay_invent_inode(inodeno_t ino);
  void open_replay_undef_inodes(MDSContextBase *fin);
  bool has_replay_undef_inodes() const { return !replay_undef_inodes.empty(); }

protected:
  int64_t metadata_pool;
  file_layout_t default_file_layout;
  file_layout_t default_log_layout;
public:
  void init_layouts();
  int64_t get_metadata_pool() const { return metadata_pool; }
  const file_layout_t& get_default_file_layout() const {
    return default_file_layout;
  }
  const file_layout_t& get_default_log_layout() const {
    return default_log_layout;
  }

protected:
  Mutex log_segments_lock;
public:
  void lock_log_segments() { log_segments_lock.Lock(); }
  void unlock_log_segments() { log_segments_lock.Unlock(); }

protected:
  Mutex request_map_lock;
  ceph::unordered_map<metareqid_t, MDRequestRef> request_map;

public:
  MDRequestRef request_start(MClientRequest *req);
  MDRequestRef request_get(metareqid_t reqid);
  void dispatch_request(const MDRequestRef& mdr);
  void request_finish(const MDRequestRef& mdr);
  void request_cleanup(const MDRequestRef& mdr);
  void request_kill(const MDRequestRef& mdr);

protected:
  Mutex rename_dir_mutex;
public:
  void unlock_rename_dir_mutex() { rename_dir_mutex.Unlock(); }
  void lock_parents_for_linkunlink(const MDRequestRef& mdr, CInode *in,
		  		   CDentry *dn, bool apply);
  int lock_parents_for_rename(const MDRequestRef& mdr, CInode *in, CInode *oldin,
			      CDentry *srcdn, CDentry *destdn, bool apply);
  void lock_objects_for_update(const MutationRef& mut, CInode *in, bool apply);

  void project_rstat_inode_to_frag(CInode *in, CDir* dir, int linkunlink);
  void project_rstat_frag_to_inode(const fnode_t *pf, inode_t *pi);
  void predirty_journal_parents(const MutationRef& mut, EMetaBlob *blob,
				CInode *in, CDir *parent, int flags,
				int linkunlink = 0);
  void journal_dirty_inode(const MutationRef& mut, EMetaBlob *metablob, CInode *in);

protected:
  Filer *filer;
  map<CInode*, LogSegment*> replayed_truncates;

  void _truncate_inode(CInode *in, LogSegment *ls);
  void truncate_inode_finish(CInode *in, LogSegment *ls);
  void truncate_inode_logged(CInode *in, MutationRef& mut);
  friend class C_MDC_TruncateFinish;
  friend class C_MDC_TruncateLogged;
public:
  Filer *get_filer() { return filer; }
  void truncate_inode(CInodeRef& in, LogSegment *ls);
  void replay_start_truncate(CInodeRef& in, LogSegment *ls);
  void replay_finish_truncate(CInodeRef& in, LogSegment *ls);
  void restart_replayed_truncates();

protected:
  Mutex open_inode_mutex;
  struct open_inode_info_t {
    int64_t pool;
    vector<inode_backpointer_t> ancestors;
    list<MDSContextBase*> waiters;
    open_inode_info_t() : pool(-1) {}
  };
  map<inodeno_t,open_inode_info_t> opening_inodes;

  void _open_inode_backtrace_fetched(inodeno_t ino, bufferlist& bl, int err);
  void _open_inode_lookup_dentry(inodeno_t ino, int64_t pool,
				 inode_backpointer_t& parent);
  void _open_inode_lookup_dentry(inodeno_t ino);
  void _open_inode_finish(inodeno_t ino, open_inode_info_t& info, int err);

  void _open_remote_dentry_finish(CDentryRef& dn, inodeno_t ino, int r);

  friend class C_MDC_OI_BacktraceFetched;
  friend class C_MDC_OI_LookupDentry;
  friend class C_MDC_OpenRemoteDentry;
public:
  void fetch_backtrace(inodeno_t ino, int64_t pool, bufferlist& bl,
		       MDSAsyncContextBase *fin);
  void open_inode(inodeno_t ino, int64_t pool, MDSContextBase* fin=NULL);
  void open_remote_dentry(CDentry *dn, inodeno_t ino, uint8_t d_type,
			  MDSContextBase *fin=NULL);

protected:
  std::atomic<uint64_t> last_cap_id;
public:
  uint64_t get_new_cap_id() { return std::atomic_fetch_add(&last_cap_id, (uint64_t)1) + 1; }
  void note_client_cap_id(uint64_t id) {
    uint64_t old = std::atomic_load(&last_cap_id);
    while (id > old && !std::atomic_compare_exchange_weak(&last_cap_id, &old, id));
  }
  float get_lease_duration() const { return 30.0; }

protected:
  struct reconnected_cap_info_t {
    inodeno_t realm_ino;
    snapid_t snap_follows;
    int dirty_caps;
    reconnected_cap_info_t() :
      realm_ino(0), snap_follows(0), dirty_caps(0) {}
  };
  map<inodeno_t,map<client_t,reconnected_cap_info_t> >  reconnected_caps;

  map<inodeno_t,map<client_t,cap_reconnect_t> > reconnecting_caps; 

  Mutex reconnect_lock;
public:
  void lock_client_reconnect() { reconnect_lock.Lock(); }
  void unlock_client_reconnect() { reconnect_lock.Unlock(); }

  void add_reconnected_cap(client_t client, inodeno_t ino, const cap_reconnect_t& icr) {
    Mutex::Locker l(reconnect_lock);
    reconnected_cap_info_t &info = reconnected_caps[ino][client];
    info.realm_ino = inodeno_t(icr.capinfo.snaprealm);
    info.snap_follows = icr.snap_follows;
  }
  void set_reconnected_dirty_caps(client_t client, inodeno_t ino, int dirty) {
    Mutex::Locker l(reconnect_lock);
    reconnected_cap_info_t &info = reconnected_caps[ino][client];
    info.dirty_caps |= dirty;
  }

  void add_cap_reconnect(inodeno_t ino, client_t client, const cap_reconnect_t& icr) {
    Mutex::Locker l(reconnect_lock);
    reconnecting_caps[ino][client] = icr;
  }
  const cap_reconnect_t *get_replay_cap_reconnect(inodeno_t ino, client_t client) {
    Mutex::Locker l(reconnect_lock);
    auto p = reconnecting_caps.find(ino);
    if (p != reconnecting_caps.end()) {
      auto q = p->second.find(client);
      if (q != p->second.end())
	return &q->second;
    }
    return NULL;
  }
  void remove_replay_cap_reconnect(inodeno_t ino, client_t client) {
    Mutex::Locker l(reconnect_lock);
    auto p = reconnecting_caps.find(ino);
    assert(p != reconnecting_caps.end());
    assert(p->second.size() == 1);
    reconnecting_caps.erase(p);
  }

protected:
  MDSContextBase *rejoin_done;
  set<inodeno_t> rejoin_missing_inodes;
  int rejoin_num_opening_inodes;
  void rejoin_open_inode_finish(inodeno_t ino, int ret);
  friend class C_MDC_RejoinOpenInodeFinish;

  vector<CInodeRef> rejoin_recover_q, rejoin_check_q;
  void identify_files_to_recover();

  void process_reconnecting_caps();
  void choose_inodes_lock_states();
public:
  void start_files_recovery();
  void rejoin_start(MDSContextBase *c);
  void try_reconnect_cap(CInode *in, Session *session);
public:

  MDCache(MDSRank *_mds);
  ~MDCache();
  void dispatch(Message *m) { assert(0); } // does not support cache message yet
  void shutdown() {}
public:

  ESubtreeMap *create_subtree_map();

  bool is_readonly() const { return false; }
  void standby_trim_segment(LogSegment *ls) {}
public:
  void dump_cache(const char *fn=NULL, Formatter *f=NULL);
};
#endif
