#ifndef CEPH_MDCACHE_H
#define CEPH_MDCACHE_H

#include "mdstypes.h"
#include "CObject.h"

class Message;
class MClientRequest;
class MDSRank;
class Server;
class Locker;
class filepath;
class LogSegment;
class EMetaBlob;
class ESubtreeMap;

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

  Mutex log_segments_lock;

  file_layout_t default_file_layout;
  file_layout_t default_log_layout;
public:

  CInodeRef create_system_inode(inodeno_t ino, int mode);
  void create_empty_hierarchy();
  void create_mydir_hierarchy();
  void add_inode(CInode *in);
  void remove_inode(CInode *in);
  void remove_inode_recursive(CInode *in);

  CInodeRef get_inode(const vinodeno_t &vino);
  CInodeRef get_inode(inodeno_t ino, snapid_t s=CEPH_NOSNAP) {
    return get_inode(vinodeno_t(ino, s));
  }
  CDirRef get_dirfrag(const dirfrag_t &df);
  bool trim_dentry(CDentry *dn);
  bool trim_inode(CDentry *dn, CInode *in);

  int path_traverse(const MDRequestRef& mdr,
		    const filepath& path, vector<CDentryRef> *pdnvec, CInodeRef *pin);

  void advance_stray() {}
  CDentryRef get_or_create_stray_dentry(CInode *in);

  void lock_log_segments() { log_segments_lock.Lock(); }
  void unlock_log_segments() { log_segments_lock.Unlock(); }

  const file_layout_t& get_default_file_layout() const {
    return default_file_layout;
  }
  const file_layout_t& get_default_log_layout() const {
    return default_log_layout;
  }

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

  void dispatch(Message *m) { assert(0); } // does not support cache message yet
  void shutdown() {}

protected:
  ceph::atomic64_t last_cap_id;
public:
  uint64_t get_new_cap_id() { return last_cap_id.inc(); }

  MDCache(MDSRank *_mds);
public:

  ESubtreeMap *create_subtree_map();

  bool is_readonly() const { return false; }
  bool trim(int max=-1, int count=-1) { return false; };
  void standby_trim_segment(LogSegment *ls) {}
};
#endif
