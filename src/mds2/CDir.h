#ifndef CEPH_CDIR_H
#define CEPH_CDIR_H

#include "mdstypes.h"
#include "CObject.h"

#include "include/elist.h"

class LogSegment;
class bloom_filter;

typedef std::map<dentry_key_t, CDentry*> dentry_map_t;

class CDir : public CObject {
  // dir mutex should not be used
  void mutex_lock();
  void mutex_unlock();
  bool mutex_trylock();
  void mutex_assert_locked_by_me();
public:
  // -- pins --
  static const int PIN_CHILD =        1;
  static const int PIN_COMMITTING =   2;
  static const int PIN_FETCHING =     3;

  // -- states ---
  static const unsigned STATE_NEW =		(1<<0);
  static const unsigned STATE_COMPLETE =        (1<< 1);
  static const unsigned STATE_COMMITTING =	(1<< 2);   // mid-commit
  static const unsigned STATE_FETCHING =	(1<< 3);   // mid-commit
  static const unsigned STATE_ASSIMRSTAT =	(1<<17);  // assimilating inode->frag rstats

  // -- waiters ---
  static const uint64_t WAIT_COMPLETE =		(1<<0);

  MDCache* const mdcache;
  CInode* const inode;
protected:
  fnode_t fnode;
  std::list<fnode_t> projected_fnode;
  version_t projected_version;

public:

  CDir(CInode *in);
  ~CDir();

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

protected:
  version_t committing_version;
  version_t committed_version;

  elist<CDentry*> dirty_dentries;
public:

  version_t pre_dirty(version_t min=0);
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t pv, LogSegment *ls);
  void mark_clean();

  void add_dirty_dentry(CDentry *dn);
  void remove_dirty_dentry(CDentry *dn);
  
  bool is_new() { return state_test(STATE_NEW); }
  void clear_new();

protected:
  dentry_map_t items;
  unsigned num_head_items;
  unsigned num_head_null;

public:
  bool empty() const { return items.empty(); }
  dentry_map_t::const_iterator begin() const { return items.begin(); }
  dentry_map_t::const_iterator end() const { return items.end(); }
  dentry_map_t::const_iterator lower_bound(dentry_key_t& key) const {
    return items.lower_bound(key);
  }
  dentry_map_t::const_iterator upper_bound(dentry_key_t& key) const {
    return items.upper_bound(key);
  }
  dentry_map_t::const_reverse_iterator rbegin() const { return items.rbegin(); }
  dentry_map_t::const_reverse_iterator rend() const { return items.rend(); }

  void link_remote_inode(CDentry *dn, inodeno_t ino, uint8_t d_type);
  void link_primary_inode(CDentry *dn, CInode *in);
  void unlink_inode(CDentry *dn);

  CDentryRef add_null_dentry(const string& dname);
  CDentryRef add_primary_dentry(const string& dname, CInode *in);
  CDentryRef add_remote_dentry(const string& dname, inodeno_t ino, uint8_t d_type);
  void remove_dentry(CDentry *dn);
  void touch_dentries_bottom();

  CDentry* __lookup(const string& name, snapid_t snap=CEPH_NOSNAP);
  CDentryRef lookup(const char *name, snapid_t snap=CEPH_NOSNAP) {
    return CDentryRef(__lookup(string(name), snap));
  }
  CDentryRef lookup(const string &name, snapid_t snap=CEPH_NOSNAP) {
    return CDentryRef(__lookup(name, snap));
  }

  void encode_dirstat(bufferlist& bl, mds_rank_t whoami);

protected:
  elist<CInode*> dirty_rstat_inodes;
public:
  void resync_accounted_fragstat(fnode_t *pf);
  void resync_accounted_rstat(fnode_t *pf);
  void add_dirty_rstat_inode(CInode *in);
  void remove_dirty_rstat_inode(CInode *in);
  void assimilate_dirty_rstat_inodes(const MutationRef& mut);
  void assimilate_dirty_rstat_inodes_finish(const MutationRef& mut, EMetaBlob *blob);

  utime_t last_stats_prop;
protected:
  map<version_t, std::list<MDSContextBase*> > waiting_for_commit;
  object_t get_ondisk_object() const;
  void _omap_commit(int op_prio);
  void _encode_dentry(CDentry *dn, bufferlist& bl);
  void _committed(int r, version_t v);  
  friend class C_Dir_Committed;

  void _omap_fetch(MDSContextBase *c, const std::set<dentry_key_t>& keys);
  CDentryRef _load_dentry(const std::string &dname, const snapid_t last,
			  bufferlist &bl, const int pos);
  void _omap_fetched(int r, bufferlist& hdrbl, map<string, bufferlist>& omap,
		     bool complete);
  // FIXME
  void go_bad_dentry(snapid_t last, const std::string &dname) { assert(0); } 
  void go_bad(bool complete) { assert(0); }
  friend class C_Dir_Fetched;

  bloom_filter *bloom;
public:
  void commit(MDSContextBase *c, int op_prio=-1);
  void fetch(MDSContextBase *c);

  bool is_complete() { return state_test(STATE_COMPLETE); };
  void clear_complete() { state_clear(STATE_COMPLETE); }
  void mark_complete();

  void add_to_bloom(CDentry *dn);
  bool is_in_bloom(const std::string& name);
  bool has_bloom() { return (bloom ? true : false); }
  void remove_bloom();

public:
   elist<CDir*>::item item_dirty, item_new;
public:
   void print(ostream& out);
protected:
  void first_get();
  void last_put();
};

ostream& operator<<(ostream& out, const CDir& dir);
#endif
