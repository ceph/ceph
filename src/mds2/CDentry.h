#ifndef CEPH_CDENTRY_H
#define CEPH_CDENTRY_H
#include "CObject.h"
#include "SimpleLock.h"
#include "LocalLock.h"

class LogSegment;

class CDentry : public CObject {
protected:
  CDir *dir;
  const std::string name;
  snapid_t first, last;

  void first_get();
  void last_put();
public:
  // -- pins --
  static const int PIN_INODEPIN =     1;  // linked inode is pinned

  // -- states --
  static const int STATE_NEW =          (1<<0);

  CDentry(CDir *d, const std::string &n, snapid_t f=2, snapid_t l=CEPH_NOSNAP);

  CDir *get_dir() const { return dir; }
  CInode *get_dir_inode() const;
  const std::string& get_name() const { return name; }
  snapid_t get_first() const { return first; }
  snapid_t get_last() const { return last; }
  dentry_key_t get_key() const { return dentry_key_t(last, name.c_str()); }

  bool is_lt(const CObject *r) const;

  struct linkage_t {
    CInode *inode;
    inodeno_t remote_ino;
    uint8_t remote_d_type;
    linkage_t() : inode(NULL), remote_ino(0), remote_d_type(0) {}
    bool is_primary() const { return remote_ino == 0 && inode != 0; }
    bool is_remote() const { return remote_ino > 0; }
    bool is_null() const { return remote_ino == 0 && inode == 0; }
    CInode *get_inode() const { return inode; }
    inodeno_t get_remote_ino() const { return remote_ino; }
    uint8_t get_remote_d_type() const { return remote_d_type; }
    void set_remote(inodeno_t ino, unsigned char d_type) {
      remote_ino = ino;
      remote_d_type = d_type;
      inode = NULL;
    }
  };
protected:
  linkage_t linkage;
  list<linkage_t> projected_linkages;

  version_t version;  // dir version when last touched.
  version_t projected_version;  // what it will be when i unlock/commit.

  linkage_t *_project_linkage();
public:

  const linkage_t* get_linkage() const { return &linkage; }
  const linkage_t* get_projected_linkage() const {
    if (projected_linkages.empty())
      return &linkage;
    return &projected_linkages.back();
  }
  void push_projected_linkage() {
    _project_linkage();
  }
  void push_projected_linkage(inodeno_t ino, uint8_t d_type) {
    linkage_t *p = _project_linkage();
    p->remote_ino = ino;
    p->remote_d_type = d_type;
  }
  void push_projected_linkage(CInode *in);
  void pop_projected_linkage();
  void link_inode_work(CInode *in);
  void link_inode_work(inodeno_t ino, uint8_t d_type);
  void unlink_inode_work();

  version_t pre_dirty(version_t min=0);
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t projected_dirv, LogSegment *ls);
  void mark_clean();

  bool is_new() const { return state_test(STATE_NEW); }
  void mark_new();
  void clear_new();

protected:
  static LockType lock_type;
  static LockType versionlock_type;
public:
  SimpleLock lock;
  LocalLock versionlock;
};

ostream& operator<<(ostream& out, const CDentry& dn);
#endif
