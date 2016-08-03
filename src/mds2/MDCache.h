#ifndef CEPH_MDCACHE_H
#define CEPH_MDCACHE_H

#include "mds/mdstypes.h"
#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"
#include "Mutation.h"

class Message;
class MClientRequest;
class MDSRank;
class Server;
class Locker;
class filepath;
class EMetaBlob;

// flags for predirty_journal_parents()
static const int PREDIRTY_PRIMARY = 1; // primary dn, adjust nested accounting
static const int PREDIRTY_DIR = 2;     // update parent dir mtime/size
static const int PREDIRTY_SHALLOW = 4; // only go to immediate parent (for easier rollback)

class MDCache {
protected:
  MDSRank *mds;
  Server *server;
  Locker *locker;

  Mutex inode_map_lock;
  ceph::unordered_map<vinodeno_t,CInode*> inode_map;

  CInodeRef root;
  CInodeRef myin;
  CInodeRef strays[NUM_STRAY]; 

  file_layout_t default_file_layout;

public:
  const file_layout_t& get_default_file_layout() const {
    return default_file_layout;
  }
  inodeno_t alloc_ino() { return last_ino.inc(); }

  CInodeRef create_system_inode(inodeno_t ino, int mode);
  void create_empty_hierarchy();
  void create_mydir_hierarchy();
  void add_inode(CInode *in);
  void remove_inode(CInode *in);



  CInodeRef get_inode(const vinodeno_t &vino);
  CInodeRef get_inode(inodeno_t ino, snapid_t s=CEPH_NOSNAP) {
    return get_inode(vinodeno_t(ino, s));
  }
  CDirRef get_dirfrag(const dirfrag_t &df) {
    CInodeRef in = get_inode(df.ino);
    if (!in)
      return NULL;
    return in->get_dirfrag(df.frag);
  }
  bool trim_dentry(CDentry *dn);
  bool trim_inode(CDentry *dn, CInode *in);

  int path_traverse(MDRequestRef& mdr,
		    const filepath& path, vector<CDentryRef> *pdnvec, CInodeRef *pin);

  CDentryRef get_or_create_stray_dentry(CInode *in);

protected:
  Mutex request_map_lock;
  ceph::unordered_map<metareqid_t, MDRequestRef> request_map;

public:
  MDRequestRef request_start(MClientRequest *req);
  MDRequestRef request_get(metareqid_t reqid);
  void dispatch_request(MDRequestRef& mdr);
  void request_finish(MDRequestRef& mdr);
  void request_cleanup(MDRequestRef& mdr);

protected:
  Mutex rename_dir_mutex;
public:
  void unlock_rename_dir_mutex() { rename_dir_mutex.Unlock(); }
  void lock_parents_for_linkunlink(MDRequestRef &mdr, CInode *in, CDentry *dn, bool apply);
  int lock_parents_for_rename(MDRequestRef& mdr, CInode *in, CInode *oldin,
			      CDentry *srcdn, CDentry *destdn, bool apply);
  void lock_objects_for_update(MutationImpl *mut, CInode *in, bool apply);
  void predirty_journal_parents(MutationImpl *mut, EMetaBlob *blob,
				CInode *in, CDir *parent, int flags,
				int linkunlink = 0);

  void dispatch(Message *m) { assert(0); } // does not support cache message yet
  void shutdown() {}

  MDCache(MDSRank *_mds);
private: // crap
  ceph::atomic64_t last_ino;
};
#endif
