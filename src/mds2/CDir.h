#ifndef CEPH_CDIR_H
#define CEPH_CDIR_H

#include "mds/mdstypes.h"
#include "CObject.h"

#include "include/elist.h"

class LogSegment;

class CDir : public CObject {
  // dir mutex should not be used
  void mutex_lock();
  void mutex_unlock();
  bool mutex_trylock();
  void mutex_assert_locked_by_me();
public:
  MDCache* const mdcache;
  CInode* const inode;

protected:
  fnode_t fnode;
  std::list<fnode_t> projected_fnode;
  version_t projected_version;

  void first_get();
  void last_put();
public:
  // -- pins --
  static const int PIN_CHILD =        3;

  // -- states ==
  static const unsigned STATE_ASSIMRSTAT =    (1<<17);  // assimilating inode->frag rstats


  typedef std::map<dentry_key_t, CDentry*> map_t;

  CDir(CInode *i);

  frag_t get_frag() const { return frag_t(); }
  dirfrag_t dirfrag() const;
  void make_string(std::string& s) const;

  bool is_lt(const CObject *r) const;

  CInode* get_inode() const { return inode; }
  version_t get_version() const { return fnode.version; }
  version_t get_projected_version() const { return projected_version; }
  void __set_version(version_t v) {
    assert(projected_fnode.empty());
    fnode.version = projected_version = v;
  }

  fnode_t *__get_fnode() { return &fnode; }
  const fnode_t *get_fnode() const { return &fnode; }
  fnode_t *__get_projected_fnode() {
    assert(!projected_fnode.empty());
    return &projected_fnode.back();
  }
  const fnode_t *get_projected_fnode() const {
    if (!projected_fnode.empty())
      return &projected_fnode.back();
    return &fnode;
  }

  fnode_t *project_fnode();
  void pop_and_dirty_projected_fnode(LogSegment *ls);
  bool is_projected() { return !projected_fnode.empty(); }
  version_t pre_dirty(version_t min=0);
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t pv, LogSegment *ls);
  void mark_clean();
  
  bool is_new() { return true; /* FIXME */ }
  void mark_new(LogSegment *ls) { /* FIXME */ }

  void inc_num_dirty() { }
  void dec_num_dirty() { }
protected:
  map_t items;

public:
  map_t::const_iterator begin() const { return items.begin(); }
  map_t::const_iterator end() const { return items.end(); }

  void link_remote_inode(CDentry *dn, inodeno_t ino, uint8_t d_type);
  void link_primary_inode(CDentry *dn, CInode *in);
  void unlink_inode(CDentry *dn);

  CDentryRef add_null_dentry(const string& dname);
  CDentryRef add_primary_dentry(const string& dname, CInode *in);
  CDentryRef add_remote_dentry(const string& dname, inodeno_t ino, uint8_t d_type);
  void remove_dentry(CDentry *dn);

  CDentry* __lookup(const char *nanme, snapid_t snap=CEPH_NOSNAP);
  CDentryRef lookup(const char *name, snapid_t snap=CEPH_NOSNAP) {
    return CDentryRef(__lookup(name, snap));
  }
  CDentryRef lookup(const string &name, snapid_t snap=CEPH_NOSNAP) {
    return CDentryRef(__lookup(name.c_str(), snap));
  }

  void encode_dirstat(bufferlist& bl, mds_rank_t whoami);
protected:

  elist<CInode*> dirty_rstat_inodes;
public:
  void resync_accounted_fragstat(fnode_t *pf);
  void resync_accounted_rstat(fnode_t *pf);
  void add_dirty_rstat_inode(CInode *in);
  void remove_dirty_rstat_inode(CInode *in);
  void assimilate_dirty_rstat_inodes(MutationRef& mut);
  void assimilate_dirty_rstat_inodes_finish(MutationRef& mut, EMetaBlob *blob);
};

ostream& operator<<(ostream& out, const CDir& dir);
#endif
