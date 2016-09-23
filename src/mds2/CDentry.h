#ifndef CEPH_CDENTRY_H
#define CEPH_CDENTRY_H

#include "CObject.h"
#include "SimpleLock.h"
#include "LocalLock.h"
#include "include/lru.h"

class LogSegment;
class DentryLease;

class CDentry : public CObject, public LRUObject {
public:
  // -- pins --
  static const int PIN_INODEPIN =     1;  // linked inode is pinned
  static const int PIN_CLIENTLEASE =  2;
  static const int PIN_PURGING =      3;

  // -- states --
  static const unsigned STATE_NEW =		(1<<0);
  static const unsigned STATE_STRAY =		(1<<1);
  static const unsigned STATE_ORPHANINODE =	(1<<2);
  static const unsigned STATE_PURGING =		(1<<3);
  static const unsigned STATE_EVALUATINGSTRAY =	(1<<4);
  static const unsigned STATE_BADREMOTEINO =	(1<<5);

  const std::string name;
protected:
  __u32 hash;
  CDir *dir;
public:

  CDentry(CDir *d, const std::string &n, __u32 h);

  CDir *get_dir() const { return dir; }
  CInode *get_dir_inode() const;
  dentry_key_t get_key() const { return dentry_key_t(CEPH_NOSNAP, name.c_str(), hash); }
  void make_string(std::string& s) const;

  bool is_lt(const CObject *r) const;

  class linkage_t {
    CInode *inode;
    inodeno_t remote_ino;
    uint8_t remote_d_type;
  public:
    linkage_t() : inode(NULL), remote_ino(0), remote_d_type(0) {}
    bool is_primary() const { return remote_ino == 0 && inode != 0; }
    bool is_remote() const { return remote_ino > 0; }
    bool is_null() const { return remote_ino == 0 && inode == 0; }
    CInode *get_inode() const { return inode; }
    void set_inode(CInode *in) { inode = in; }
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
  const linkage_t* get_linkage(client_t client, const MutationRef& mut) const;

  void push_projected_linkage() {
    _project_linkage();
  }
  void push_projected_linkage(inodeno_t ino, uint8_t d_type) {
    linkage_t *p = _project_linkage();
    p->set_remote(ino, d_type);
  }
  void push_projected_linkage(CInode *in);
  void pop_projected_linkage();
  bool is_projected() const { return !projected_linkages.empty(); }

  void link_inode_work(CInode *in);
  void link_inode_work(inodeno_t ino, uint8_t d_type);
  void unlink_inode_work();
  void link_remote(const CDentry::linkage_t *dnl, CInodeRef& in);
  bool is_bad_remote_ino(const CDentry::linkage_t *dnl) const {
    return dnl == get_linkage() && state_test(STATE_BADREMOTEINO);
  }

  version_t get_version() const { return version; }
  version_t get_projected_version() const { return projected_version; }
  void set_version(version_t v) { projected_version = version = v; }

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

protected:
  map<client_t,DentryLease*> client_leases;
public:
  const map<client_t,DentryLease*>& get_client_leases() const { return client_leases; }
  DentryLease *get_client_lease(client_t c) {
    auto p = client_leases.find(c);
    if (p != client_leases.end())
      return p->second;
    return NULL;
  }
  DentryLease* add_client_lease(Session *session, utime_t ttl);
  void remove_client_lease(Session *session);

public:
  elist<CDentry*>::item item_dirty, item_dir_dirty;

protected:
  void first_get();
  void last_put();
public: // crap
  static snapid_t first, last;
};

ostream& operator<<(ostream& out, const CDentry& dn);
#endif
