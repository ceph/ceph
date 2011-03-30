// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_CLIENT_H
#define CEPH_CLIENT_H

enum {
  l_c_first = 20000,
  l_c_reply,
  l_c_lat,
  l_c_owrlat,
  l_c_ordlat,
  l_c_wrlat,
  l_c_last,
};

#include "mds/MDSMap.h"
#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "mon/MonClient.h"

#include "msg/Message.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"

#include "messages/MClientReply.h"
#include "messages/MClientRequest.h"

#include "include/types.h"
#include "include/lru.h"
#include "include/filepath.h"
#include "include/interval_set.h"

#include "common/Mutex.h"
#include "common/Timer.h"

//#include "FileCache.h"


// stl
#include <string>
#include <set>
#include <map>
#include <fstream>
using std::set;
using std::map;
using std::fstream;

#include <ext/hash_map>
using namespace __gnu_cxx;


#include "osdc/ObjectCacher.h"

class MClientSession;
class MClientRequest;
class MClientRequestForward;
class MClientLease;
class MClientCaps;
class MClientCapRelease;

class Filer;
class Objecter;
class ObjectCacher;

extern class ProfLogType client_logtype;
extern class ProfLogger  *client_logger;


// ============================================
// types for my local metadata cache
/* basic structure:
   
 - Dentries live in an LRU loop.  they get expired based on last access.
      see include/lru.h.  items can be bumped to "mid" or "top" of list, etc.
 - Inode has ref count for each Fh, Dir, or Dentry that points to it.
 - when Inode ref goes to 0, it's expired.
 - when Dir is empty, it's removed (and it's Inode ref--)
 
*/
struct InodeCap;
class Inode;
class Dentry;

/* getdir result */
struct DirEntry {
  string d_name;
  struct stat st;
  int stmask;
  DirEntry(const string &s) : d_name(s), stmask(0) {}
  DirEntry(const string &n, struct stat& s, int stm) : d_name(n), st(s), stmask(stm) {}
};

struct MetaRequest {
  uint64_t tid;
  ceph_mds_request_head head;
  filepath path, path2;
  bufferlist data;
  int inode_drop; //the inode caps this operation will drop
  int inode_unless; //unless we have these caps already
  int old_inode_drop, old_inode_unless;
  int dentry_drop, dentry_unless;
  int old_dentry_drop, old_dentry_unless;
  vector<MClientRequest::Release> cap_releases;
  Inode *inode;
  Inode *old_inode;
  Dentry *dentry; //associated with path
  Dentry *old_dentry; //associated with path2

 
  utime_t  sent_stamp;
  int      mds;                // who i am asking
  int      resend_mds;         // someone wants you to (re)send the request here
  bool     send_to_auth;       // must send to auth mds
  __u32    sent_on_mseq;       // mseq at last submission of this request
  int      num_fwd;            // # of times i've been forwarded
  int      retry_attempt;
  int      ref;
  
  MClientReply *reply;         // the reply
  bool kick;
  
  // readdir result
  frag_t readdir_frag;
  string readdir_start;  // starting _after_ this name
  uint64_t readdir_offset;

  vector<pair<string,Inode*> > readdir_result;
  bool readdir_end;
  int readdir_num;
  string readdir_last_name;

  //possible responses
  bool got_safe;
  bool got_unsafe;

  xlist<MetaRequest*>::item item;
  xlist<MetaRequest*>::item unsafe_item;
  Mutex lock; //for get/set sync

  Cond  *caller_cond;          // who to take up
  Cond  *dispatch_cond;        // who to kick back

  Inode *target;

  MetaRequest(int op) : 
    inode_drop(0), inode_unless(0),
    old_inode_drop(0), old_inode_unless(0),
    dentry_drop(0), dentry_unless(0),
    old_dentry_drop(0), old_dentry_unless(0),
    inode(NULL), old_inode(NULL),
    dentry(NULL), old_dentry(NULL),
    mds(-1), resend_mds(-1), send_to_auth(false), sent_on_mseq(0),
    num_fwd(0), retry_attempt(0),
    ref(1), reply(0), 
    kick(false), got_safe(false), got_unsafe(false), item(this), unsafe_item(this),
    lock("MetaRequest lock"),
    caller_cond(0), dispatch_cond(0),
    target(0) {
    memset(&head, 0, sizeof(ceph_mds_request_head));
    head.op = op;
  }

  MetaRequest* get() {++ref; return this; }

  void put() {if(--ref == 0) delete this; }

  // normal fields
  void set_tid(tid_t t) { tid = t; }
  void set_oldest_client_tid(tid_t t) { head.oldest_client_tid = t; }
  void inc_num_fwd() { head.num_fwd = head.num_fwd + 1; }
  void set_retry_attempt(int a) { head.num_retry = a; }
  void set_filepath(const filepath& fp) { path = fp; }
  void set_filepath2(const filepath& fp) { path2 = fp; }
  void set_string2(const char *s) { path2.set_path(s, 0); }
  void set_caller_uid(unsigned u) { head.caller_uid = u; }
  void set_caller_gid(unsigned g) { head.caller_gid = g; }
  void set_data(const bufferlist &d) { data = d; }
  void set_dentry_wanted() {
    head.flags = head.flags | CEPH_MDS_FLAG_WANT_DENTRY;
  }
  int get_op() { return head.op; }
  tid_t get_tid() { return tid; }
  filepath& get_filepath() { return path; }
  filepath& get_filepath2() { return path2; }

  bool is_write() {
    return
      (head.op & CEPH_MDS_OP_WRITE) || 
      (head.op == CEPH_MDS_OP_OPEN && !(head.args.open.flags & (O_CREAT|O_TRUNC))) ||
      (head.op == CEPH_MDS_OP_CREATE && !(head.args.open.flags & (O_CREAT|O_TRUNC)));
  }
  bool can_forward() {
    if (is_write() ||
	head.op == CEPH_MDS_OP_OPEN ||   // do not forward _any_ open request.
	head.op == CEPH_MDS_OP_CREATE)   // do not forward _any_ open request.
      return false;
    return true;
  }
  bool auth_is_best() {
    if (is_write()) 
      return true;
    if (head.op == CEPH_MDS_OP_OPEN ||
	head.op == CEPH_MDS_OP_CREATE ||
	head.op == CEPH_MDS_OP_READDIR) 
      return true;
    return false;    
  }
};


struct MDSSession {
  int mds_num;
  version_t seq;
  uint64_t cap_gen;
  utime_t cap_ttl, last_cap_renew_request;
  uint64_t cap_renew_seq;
  int num_caps;
  entity_inst_t inst;
  bool closing;
  bool was_stale;

  xlist<InodeCap*> caps;
  xlist<Inode*> flushing_caps;
  xlist<MetaRequest*> requests;
  xlist<MetaRequest*> unsafe_requests;

  MClientCapRelease *release;
  
  MDSSession() : mds_num(-1), seq(0), cap_gen(0), cap_renew_seq(0), num_caps(0),
		 closing(false), was_stale(false), release(NULL) {}
};

class Dir;
class Inode;

class Dentry : public LRUObject {
 public:
  string  name;                      // sort of lame
  //const char *name;
  Dir     *dir;
  Inode   *inode;
  int     ref;                       // 1 if there's a dir beneath me.
  uint64_t offset;
  int lease_mds;
  utime_t lease_ttl;
  uint64_t lease_gen;
  ceph_seq_t lease_seq;
  int cap_shared_gen;
  
  void get() { 
    assert(ref == 0); ref++; lru_pin(); 
    //cout << "dentry.get on " << this << " " << name << " now " << ref << std::endl;
  }
  void put() { 
    assert(ref == 1); ref--; lru_unpin(); 
    //cout << "dentry.put on " << this << " " << name << " now " << ref << std::endl;
  }
  
  Dentry() : dir(0), inode(0), ref(0), offset(0), lease_mds(-1), lease_gen(0), lease_seq(0), cap_shared_gen(0) { }
};

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  hash_map<string, Dentry*> dentries;
  map<string, Dentry*> dentry_map;
  uint64_t release_count;
  uint64_t max_offset;

  Dir(Inode* in) : release_count(0), max_offset(2) { parent_inode = in; }

  bool is_empty() {  return dentries.empty(); }
};

struct SnapRealm {
  inodeno_t ino;
  int nref;
  snapid_t created;
  snapid_t seq;
  
  inodeno_t parent;
  snapid_t parent_since;
  vector<snapid_t> prior_parent_snaps;  // snaps prior to parent_since
  vector<snapid_t> my_snaps;

  SnapRealm *pparent;
  set<SnapRealm*> pchildren;

  SnapContext cached_snap_context;  // my_snaps + parent snaps + past_parent_snaps

  xlist<Inode*> inodes_with_caps;

  SnapRealm(inodeno_t i) : 
    ino(i), nref(0), created(0), seq(0),
    pparent(NULL) { }

  void build_snap_context();
  void invalidate_cache() {
    cached_snap_context.clear();
  }

  const SnapContext& get_snap_context() {
    if (cached_snap_context.seq == 0)
      build_snap_context();
    return cached_snap_context;
  }
};

inline ostream& operator<<(ostream& out, const SnapRealm& r) {
  return out << "snaprealm(" << r.ino << " nref=" << r.nref << " c=" << r.created << " seq=" << r.seq
	     << " parent=" << r.parent
	     << " my_snaps=" << r.my_snaps
	     << " cached_snapc=" << r.cached_snap_context
	     << ")";
}

struct InodeCap {
  MDSSession *session;
  Inode *inode;
  xlist<InodeCap*>::item cap_item;

  uint64_t cap_id;
  unsigned issued;
  unsigned implemented;
  unsigned wanted;   // as known to mds.
  uint64_t seq, issue_seq;
  __u32 mseq;  // migration seq
  __u32 gen;

  InodeCap() : session(NULL), inode(NULL), cap_item(this), issued(0),
	       implemented(0), wanted(0), seq(0), issue_seq(0), mseq(0), gen(0) {}
};

struct CapSnap {
  //snapid_t follows;  // map key
  SnapContext context;
  int issued, dirty;

  uint64_t size;
  utime_t ctime, mtime, atime;
  version_t time_warp_seq;
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;
  map<string,bufferptr> xattrs;
  version_t xattr_version;

  bool writing, dirty_data;
  uint64_t flush_tid;
  CapSnap() : issued(0), dirty(0), 
	      size(0), time_warp_seq(0), mode(0), uid(0), gid(0), xattr_version(0),
	      writing(false), dirty_data(false), flush_tid(0) {}
};


// inode flags
#define I_COMPLETE 1

class Inode {
 public:
  // -- the actual inode --
  inodeno_t ino;
  snapid_t  snapid;
  uint32_t   rdev;    // if special file

  // affected by any inode change...
  utime_t    ctime;   // inode change time

  // perm (namespace permissions)
  uint32_t   mode;
  uid_t      uid;
  gid_t      gid;

  // nlink
  int32_t    nlink;  

  // file (data access)
  ceph_dir_layout dir_layout;
  ceph_file_layout layout;
  uint64_t   size;        // on directory, # dentries
  uint32_t   truncate_seq;
  uint64_t   truncate_size;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint32_t   time_warp_seq;  // count of (potential) mtime/atime timewarps (i.e., utimes())

  uint64_t max_size;  // max size we can write to

  // dirfrag, recursive accountin
  frag_info_t dirstat;
  nest_info_t rstat;
 
  // special stuff
  version_t version;           // auth only
  version_t xattr_version;

  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  unsigned flags;

  // about the dir (if this is one!)
  set<int>  dir_contacts;
  bool      dir_hashed, dir_replicated;

  // per-mds caps
  map<int,InodeCap*> caps;            // mds -> InodeCap
  InodeCap *auth_cap;
  unsigned dirty_caps, flushing_caps;
  uint64_t flushing_cap_seq;
  __u16 flushing_cap_tid[CEPH_CAP_BITS];
  int shared_gen, cache_gen;
  int snap_caps, snap_cap_refs;
  unsigned exporting_issued;
  int exporting_mds;
  ceph_seq_t exporting_mseq;
  utime_t hold_caps_until;
  xlist<Inode*>::item cap_item, flushing_cap_item;
  tid_t last_flush_tid;

  SnapRealm *snaprealm;
  xlist<Inode*>::item snaprealm_item;
  Inode *snapdir_parent;  // only if we are a snapdir inode
  map<snapid_t,CapSnap> cap_snaps;   // pending flush to mds

  //int open_by_mode[CEPH_FILE_MODE_NUM];
  map<int,int> open_by_mode;
  map<int,int> cap_refs;

  ObjectCacher::ObjectSet oset;

  uint64_t     reported_size, wanted_max_size, requested_max_size;

  int       ref;      // ref count. 1 for each dentry, fh that links to me.
  int       ll_ref;   // separate ref count for ll client
  Dir       *dir;     // if i'm a dir.
  set<Dentry*> dn_set;      // if i'm linked to a dentry.
  string    symlink;  // symlink content, if it's a symlink
  fragtree_t dirfragtree;
  map<string,bufferptr> xattrs;
  map<frag_t,int> fragmap;  // known frag -> mds mappings

  list<Cond*>       waitfor_caps;
  list<Cond*>       waitfor_commit;

#define dentry_of(a) (*(a->dn_set.begin()))

  void make_long_path(filepath& p) {
    if (!dn_set.empty()) {
      assert((*dn_set.begin())->dir && (*dn_set.begin())->dir->parent_inode);
      (*dn_set.begin())->dir->parent_inode->make_long_path(p);
      p.push_dentry((*dn_set.begin())->name);
    } else if (snapdir_parent) {
      snapdir_parent->make_nosnap_relative_path(p);
      string empty;
      p.push_dentry(empty);
    } else
      p = filepath(ino);
  }

  /*
   * make a filepath suitable for an mds request:
   *  - if we are non-snapped/live, the ino is sufficient, e.g. #1234
   *  - if we are snapped, make filepath relative to first non-snapped parent.
   */
  void make_nosnap_relative_path(filepath& p) {
    if (snapid == CEPH_NOSNAP) {
      p = filepath(ino);
    } else if (snapdir_parent) {
      snapdir_parent->make_nosnap_relative_path(p);
      string empty;
      p.push_dentry(empty);
    } else if (!dn_set.empty()) {
      assert((*dn_set.begin())->dir && (*dn_set.begin())->dir->parent_inode);
      (*dn_set.begin())->dir->parent_inode->make_nosnap_relative_path(p);
      p.push_dentry((*dn_set.begin())->name);
    } else {
      p = filepath(ino);
    }
  }

  void get() { 
    ref++; 
    dout(30) << "inode.get on " << this << " " << hex << ino << dec << " now " << ref << dendl;
  }
  void put(int n=1) { 
    ref -= n; 
    dout(30) << "inode.put on " << this << " " << hex << ino << dec << " now " << ref << dendl;
    assert(ref >= 0);
  }

  void ll_get() {
    ll_ref++;
  }
  void ll_put(int n=1) {
    assert(ll_ref >= n);
    ll_ref -= n;
  }

  Inode(vinodeno_t vino, ceph_file_layout *layout) : 
    ino(vino.ino), snapid(vino.snapid),
    rdev(0), mode(0), uid(0), gid(0), nlink(0), size(0), truncate_seq(1), truncate_size(-1),
    time_warp_seq(0), max_size(0), version(0), xattr_version(0),
    flags(0),
    dir_hashed(false), dir_replicated(false), auth_cap(NULL),
    dirty_caps(0), flushing_caps(0), flushing_cap_seq(0), shared_gen(0), cache_gen(0),
    snap_caps(0), snap_cap_refs(0),
    exporting_issued(0), exporting_mds(-1), exporting_mseq(0),
    cap_item(this), flushing_cap_item(this), last_flush_tid(0),
    snaprealm(0), snaprealm_item(this), snapdir_parent(0),
    oset((void *)this, layout->fl_pg_pool, ino),
    reported_size(0), wanted_max_size(0), requested_max_size(0),
    ref(0), ll_ref(0), 
    dir(0), dn_set()
  {
    memset(&flushing_cap_tid, 0, sizeof(__u16)*CEPH_CAP_BITS);
  }
  ~Inode() { }

  vinodeno_t vino() { return vinodeno_t(ino, snapid); }


  // CAPS --------
  void get_open_ref(int mode) {
    open_by_mode[mode]++;
  }
  bool put_open_ref(int mode) {
    //cout << "open_by_mode[" << mode << "] " << open_by_mode[mode] << " -> " << (open_by_mode[mode]-1) << std::endl;
    if (--open_by_mode[mode] == 0)
      return true;
    return false;
  }

  void get_cap_ref(int cap);
  bool put_cap_ref(int cap);

  bool is_any_caps() {
    return caps.size() || exporting_mds >= 0;
  }

  bool cap_is_valid(InodeCap* cap) {
    /*cout << "cap_gen     " << cap->session-> cap_gen << std::endl
	 << "session gen " << cap->gen << std::endl
	 << "cap expire  " << cap->session->cap_ttl << std::endl
	 << "cur time    " << g_clock.now() << std::endl;*/
    if ((cap->session->cap_gen <= cap->gen)
	&& (g_clock.now() < cap->session->cap_ttl)) {
      return true;
    }
    //if we make it here, the capabilities aren't up-to-date
    cap->session->was_stale = true;
    return true;
  }

  int caps_issued(int *implemented = 0) {
    int c = exporting_issued | snap_caps;
    int i = 0;
    for (map<int,InodeCap*>::iterator it = caps.begin();
         it != caps.end();
         it++)
      if (cap_is_valid(it->second)) {
	c |= it->second->issued;
	i |= it->second->implemented;
      }
    if (implemented)
      *implemented = i;
    return c;
  }
  void touch_cap(InodeCap *cap) {
    // move to back of LRU
    cap->session->caps.push_back(&cap->cap_item);
  }
  bool caps_issued_mask(unsigned mask) {
    int c = exporting_issued | snap_caps;
    if ((c & mask) == mask)
      return true;
    for (map<int,InodeCap*>::iterator it = caps.begin();
         it != caps.end();
         it++) {
      if (cap_is_valid(it->second)) {
	if ((it->second->issued & mask) == mask) {
	  touch_cap(it->second);
	  return true;
	}
	c |= it->second->issued;
      }
    }
    if ((c & mask) == mask) {
      // bah.. touch them all
      for (map<int,InodeCap*>::iterator it = caps.begin();
	   it != caps.end();
	   it++)
	touch_cap(it->second);
      return true;
    }
    return false;
  }

  int caps_used() {
    int w = 0;
    for (map<int,int>::iterator p = cap_refs.begin();
	 p != cap_refs.end();
	 p++)
      if (p->second)
	w |= p->first;
    return w;
  }
  int caps_file_wanted() {
    int want = 0;
    for (map<int,int>::iterator p = open_by_mode.begin();
	 p != open_by_mode.end();
	 p++)
      if (p->second)
	want |= ceph_caps_for_mode(p->first);
    return want;
  }
  int caps_wanted() {
    int want = caps_file_wanted() | caps_used();
    if (want & CEPH_CAP_FILE_BUFFER)
      want |= CEPH_CAP_FILE_EXCL;
    return want;
  }
  int caps_dirty() {
    return dirty_caps | flushing_caps;
  }

  bool have_valid_size() {
    // RD+RDCACHE or WR+WRBUFFER => valid size
    if (caps_issued() & (CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_EXCL))
      return true;
    return false;
  }

  // open Dir for an inode.  if it's not open, allocated it (and pin dentry in memory).
  Dir *open_dir() {
    if (!dir) {
      assert(dn_set.size() < 2); // dirs can't be hard-linked
      if (!dn_set.empty()) (*dn_set.begin())->get();      // pin dentry
      get();                  // pin inode
      dir = new Dir(this);
    }
    return dir;
  }

};




// file handle for any open file state

struct Fh {
  Inode    *inode;
  loff_t     pos;
  int       mds;        // have to talk to mds we opened with (for now)
  int       mode;       // the mode i opened the file with

  bool is_lazy() { return mode & O_LAZY; }

  bool append;
  bool pos_locked;           // pos is currently in use
  list<Cond*> pos_waiters;   // waiters for pos

  // readahead state
  loff_t last_pos;
  loff_t consec_read_bytes;
  int nr_consec_read;

  Fh() : inode(0), pos(0), mds(0), mode(0), append(false), pos_locked(false),
	 last_pos(0), consec_read_bytes(0), nr_consec_read(0) {}
};





// ========================================================
// client interface

class Client : public Dispatcher {
 public:
  
  struct DirResult {
    static const int SHIFT = 28;
    static const int64_t MASK = (1 << SHIFT) - 1;
    static const loff_t END = 1ULL << (SHIFT + 32);

    static uint64_t make_fpos(unsigned frag, unsigned off) {
      return ((uint64_t)frag << SHIFT) | (uint64_t)off;
    }
    static unsigned fpos_frag(uint64_t p) {
      return p >> SHIFT;
    }
    static unsigned fpos_off(uint64_t p) {
      return p & MASK;
    }


    Inode *inode;

    int64_t offset;        // high bits: frag_t, low bits: an offset

    uint64_t this_offset;  // offset of last chunk, adjusted for . and ..
    uint64_t next_offset;  // offset of next chunk (last_name's + 1)
    string last_name;      // last entry in previous chunk

    uint64_t release_count;

    frag_t buffer_frag;
    vector<pair<string,Inode*> > *buffer;

    string at_cache_name;  // last entry we successfully returned

    DirResult(Inode *in) : inode(in), offset(0), next_offset(2),
			   release_count(0),
			   buffer(0) { 
      inode->get();
    }

    frag_t frag() { return frag_t(offset >> SHIFT); }
    unsigned fragpos() { return offset & MASK; }

    void next_frag() {
      frag_t fg = offset >> SHIFT;
      if (fg.is_rightmost())
	set_end();
      else 
	set_frag(fg.next());
    }
    void set_frag(frag_t f) {
      offset = (uint64_t)f << SHIFT;
      assert(sizeof(offset) == 8);
    }
    void set_end() { offset = END; }
    bool at_end() { return (offset == END); }

    void reset() {
      last_name.clear();
      next_offset = 2;
      this_offset = 0;
      offset = 0;
      delete buffer;
      buffer = 0;
    }
  };

  // **** WARNING: be sure to update the struct in libceph.h too! ****
  struct stat_precise {
    ino_t st_ino;
    dev_t st_dev;
    mode_t st_mode;
    nlink_t st_nlink;
    uid_t st_uid;
    gid_t st_gid;
    dev_t st_rdev;
    off_t st_size;
    blksize_t st_blksize;
    blkcnt_t st_blocks;
    time_t st_atime_sec;
    time_t st_atime_micro;
    time_t st_mtime_sec;
    time_t st_mtime_micro;
    time_t st_ctime_sec;
    time_t st_ctime_micro;
    stat_precise() {}
    stat_precise(struct stat attr): st_ino(attr.st_ino), st_dev(attr.st_dev),
			       st_mode(attr.st_mode), st_nlink(attr.st_nlink),
			       st_uid(attr.st_uid), st_gid(attr.st_gid),
			       st_size(attr.st_size),
			       st_blksize(attr.st_blksize),
			       st_blocks(attr.st_blocks),
			       st_atime_sec(attr.st_atime),
			       st_atime_micro(0),
			       st_mtime_sec(attr.st_mtime),
			       st_mtime_micro(0),
			       st_ctime_sec(attr.st_ctime),
			       st_ctime_micro(0) {}
  };

  // cluster descriptors
  MDSMap *mdsmap; 
  OSDMap *osdmap;

  SafeTimer timer;

  Context *tick_event;
  utime_t last_cap_renew;
  void renew_caps();
  void renew_caps(int s);
public:
  void tick();

 protected:
  MonClient *monclient;
  Messenger *messenger;  
  client_t whoami;

  // mds sessions
  map<int, MDSSession*> mds_sessions;  // mds -> push seq
  map<int, list<Cond*> > waiting_for_session;
  list<Cond*> waiting_for_mdsmap;

  void got_mds_push(int mds);
  void handle_client_session(MClientSession *m);
  void send_reconnect(int mds);
  void resend_unsafe_requests(int mds);

  // mds requests
  tid_t last_tid, last_flush_seq;
  map<tid_t, MetaRequest*> mds_requests;
  set<int>                 failed_mds;

  int make_request(MetaRequest *req, int uid, int gid,
		   //MClientRequest *req, int uid, int gid,
		   Inode **ptarget = 0,
		   int use_mds=-1, bufferlist *pdirbl=0);
  void encode_cap_releases(MetaRequest *request, int mds);
  int encode_inode_release(Inode *in, MetaRequest *req,
			   int mds, int drop,
			   int unless,int force=0);
  void encode_dentry_release(Dentry *dn, MetaRequest *req,
			     int mds, int drop, int unless);
  int choose_target_mds(MetaRequest *req);
  void connect_mds_targets(int mds);
  void send_request(MetaRequest *request, int mds);
  MClientRequest *build_client_request(MetaRequest *request);
  void kick_requests(int mds, bool signal);
  void handle_client_request_forward(MClientRequestForward *reply);
  void handle_client_reply(MClientReply *reply);

  bool   mounted;
  bool   unmounting;

  int local_osd;
  epoch_t local_osd_epoch;

  int unsafe_sync_write;

  int file_stripe_unit;
  int file_stripe_count;
  int object_size;
  int file_replication;
  int preferred_pg;
public:
  entity_name_t get_myname() { return messenger->get_myname(); } 
  void sync_write_commit(Inode *in);

protected:
  Filer                 *filer;     
  ObjectCacher          *objectcacher;
  Objecter              *objecter;     // (non-blocking) osd interface
  
  // cache
  hash_map<vinodeno_t, Inode*> inode_map;
  Inode*                 root;
  LRU                    lru;    // lru list of Dentry's in our local metadata cache.

  // all inodes with caps sit on either cap_list or delayed_caps.
  xlist<Inode*> delayed_caps, cap_list;
  int num_flushing_caps;
  hash_map<inodeno_t,SnapRealm*> snap_realms;

  SnapRealm *get_snap_realm(inodeno_t r) {
    SnapRealm *realm = snap_realms[r];
    if (!realm)
      snap_realms[r] = realm = new SnapRealm(r);
    realm->nref++;
    return realm;
  }
  SnapRealm *get_snap_realm_maybe(inodeno_t r) {
    if (snap_realms.count(r) == 0)
      return NULL;
    SnapRealm *realm = snap_realms[r];
    realm->nref++;
    return realm;
  }
  void put_snap_realm(SnapRealm *realm) {
    if (realm->nref-- == 0) {
      snap_realms.erase(realm->ino);
      delete realm;
    }
  }
  bool adjust_realm_parent(SnapRealm *realm, inodeno_t parent);
  inodeno_t update_snap_trace(bufferlist& bl, bool must_flush=true);
  inodeno_t _update_snap_trace(vector<SnapRealmInfo>& trace);
  void invalidate_snaprealm_and_children(SnapRealm *realm);

  Inode *open_snapdir(Inode *diri);


  // file handles, etc.
  interval_set<int> free_fd_set;  // unused fds
  hash_map<int, Fh*> fd_map;
  
  int get_fd() {
    int fd = free_fd_set.range_start();
    free_fd_set.erase(fd, 1);
    return fd;
  }
  void put_fd(int fd) {
    free_fd_set.insert(fd, 1);
  }

  // global client lock
  //  - protects Client and buffer cache both!
  Mutex                  client_lock;

  int filer_flags;

  // helpers
  void wake_inode_waiters(int mds);
  void wait_on_list(list<Cond*>& ls);
  void signal_cond_list(list<Cond*>& ls);

  // -- metadata cache stuff

  // decrease inode ref.  delete if dangling.
  void put_inode(Inode *in, int n=1);

  void close_dir(Dir *dir) {
    assert(dir->is_empty());
    
    Inode *in = dir->parent_inode;
    assert (in->dn_set.size() < 2); //dirs can't be hard-linked
    if (!in->dn_set.empty()) dentry_of(in)->put();   // unpin dentry
    
    delete in->dir;
    in->dir = 0;
    put_inode(in);               // unpin inode
  }

  //int get_cache_size() { return lru.lru_get_size(); }
  //void set_cache_size(int m) { lru.lru_set_max(m); }

  /**
   * Don't call this with in==NULL, use get_or_create for that
   * leave dn set to default NULL unless you're trying to add
   * a new inode to a pre-created Dentry
   */
  Dentry* link(Dir *dir, const string& name, Inode *in, Dentry *dn=NULL) {
    if (!dn) { //create a new Dentry
      dn = new Dentry;
      dn->name = name;
      
      // link to dir
      dn->dir = dir;
      //cout << "link dir " << dir->parent_inode->ino << " '" << name << "' -> inode " << in->ino << endl;
      dir->dentries[dn->name] = dn;
      dir->dentry_map[dn->name] = dn;
      lru.lru_insert_mid(dn);    // mid or top?
    }

    if (in) {    // link to inode
      dn->inode = in;
      if(!in->dn_set.empty())
        dout(5) << "adding new hard link to " << in->vino()
                << " from " << dn << dendl;
      in->dn_set.insert(dn);
      in->get();
      
      if (in->dir) dn->get();  // dir -> dn pin
    }

    return dn;
  }

  void unlink(Dentry *dn, bool keepdir = false) {
    Inode *in = dn->inode;

    // unlink from inode
    if (in) {
      if (in->dir) dn->put();        // dir -> dn pin
      dn->inode = 0;
      in->dn_set.erase(dn);
      put_inode(in);
    }
        
    // unlink from dir
    dn->dir->dentries.erase(dn->name);
    dn->dir->dentry_map.erase(dn->name);
    if (dn->dir->is_empty() && !keepdir) 
      close_dir(dn->dir);
    dn->dir = 0;

    // delete den
    lru.lru_remove(dn);
    delete dn;
  }

  /* If an inode's been moved from one dentry to another
   * (via rename, for instance), call this function to move it */
  Dentry *relink_inode(Dir *dir, const string& name, Inode *in, Dentry *olddn,
                       Dentry *newdn=NULL) {
    Dir *olddir = olddn->dir;  // note: might == dir!
    bool made_new = false;

    // newdn, attach to inode.  don't touch inode ref.
    if (!newdn) {
      made_new = true;
      newdn = new Dentry;
      newdn->dir = dir;
      newdn->name = name;
    } else {
      assert(newdn->inode == NULL);
    }
    newdn->inode = in;
    in->dn_set.erase(olddn);
    in->dn_set.insert(newdn);

    if (in->dir) { // dir -> dn pin
      newdn->get();
      olddn->put();
    }

    // unlink old dn from dir
    olddir->dentries.erase(olddn->name);
    olddir->dentry_map.erase(olddn->name);
    olddn->inode = 0;
    olddn->dir = 0;
    lru.lru_remove(olddn);
    delete olddn;
    
    // link new dn to dir
    dir->dentries[name] = newdn;
    dir->dentry_map[name] = newdn;
    if (made_new)
      lru.lru_insert_mid(newdn);
    else
      lru.lru_midtouch(newdn);
    
    // olddir now empty?  (remember, olddir might == dir)
    if (olddir->is_empty()) 
      close_dir(olddir);

    return newdn;
  }

  // path traversal for high-level interface
  Inode *cwd;
  int path_walk(const filepath& fp, Inode **end, bool followsym=true);
  int fill_stat(Inode *in, struct stat *st, frag_info_t *dirstat=0, nest_info_t *rstat=0);
  int fill_stat_precise(Inode *in, struct stat_precise *st, frag_info_t *dirstat=0, nest_info_t *rstat=0);
  void touch_dn(Dentry *dn) { lru.lru_touch(dn); }  

  // trim cache.
  void trim_cache();
  void dump_inode(Inode *in, set<Inode*>& did);
  void dump_cache();  // debug
  
  // trace generation
  ofstream traceout;


  Cond mount_cond, sync_cond;


  // friends
  friend class SyntheticClient;
  bool ms_dispatch(Message *m);

  void ms_handle_connect(Connection *con);
  bool ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);
  bool ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new);


 public:
  void set_filer_flags(int flags);
  void clear_filer_flags(int flags);

  Client(Messenger *m, MonClient *mc);
  ~Client();
  void tear_down_cache();   

  client_t get_nodeid() { return whoami; }
  inodeno_t get_root_ino() { return root->ino; }

  void init();
  void shutdown();

  // messaging
  void handle_mds_map(class MMDSMap *m);

  void handle_lease(MClientLease *m);

  void release_lease(Inode *in, Dentry *dn, int mask);

  // file caps
  void check_cap_issue(Inode *in, InodeCap *cap, unsigned issued);
  void add_update_cap(Inode *in, int mds, uint64_t cap_id,
		      unsigned issued, unsigned seq, unsigned mseq, inodeno_t realm,
		      int flags);
  void remove_cap(Inode *in, int mds);
  void remove_all_caps(Inode *in);
  void remove_session_caps(int mds_num);
  void trim_caps(int mds, int max);
  void mark_caps_dirty(Inode *in, int caps);
  void flush_caps();
  void flush_caps(Inode *in, int mds);
  void kick_flushing_caps(int mds);
  int get_caps(Inode *in, int need, int want, int *got, loff_t endoff);

  void maybe_update_snaprealm(SnapRealm *realm, snapid_t snap_created, snapid_t snap_highwater, 
			      vector<snapid_t>& snaps);

  void handle_snap(class MClientSnap *m);
  void handle_caps(class MClientCaps *m);
  void handle_cap_import(Inode *in, class MClientCaps *m);
  void handle_cap_export(Inode *in, class MClientCaps *m);
  void handle_cap_trunc(Inode *in, class MClientCaps *m);
  void handle_cap_flush_ack(Inode *in, int mds, InodeCap *cap, class MClientCaps *m);
  void handle_cap_flushsnap_ack(Inode *in, class MClientCaps *m);
  void handle_cap_grant(Inode *in, int mds, InodeCap *cap, class MClientCaps *m);
  void cap_delay_requeue(Inode *in);
  void send_cap(Inode *in, int mds, InodeCap *cap, int used, int want, int retain, int flush, uint64_t tid);
  void check_caps(Inode *in, bool is_delayed);
  void get_cap_ref(Inode *in, int cap);
  void put_cap_ref(Inode *in, int cap);
  void flush_snaps(Inode *in);
  void wait_sync_caps(uint64_t want);
  void queue_cap_snap(Inode *in, snapid_t seq=0);
  void finish_cap_snap(Inode *in, CapSnap *capsnap, int used);
  void _flushed_cap_snap(Inode *in, snapid_t seq);

  void _release(Inode *in, bool checkafter=true);
  void _flush(Inode *in, Context *onfinish=NULL);
  void _flushed(Inode *in);
  void flush_set_callback(ObjectCacher::ObjectSet *oset);

  void close_release(Inode *in);
  void close_safe(Inode *in);

  void lock_fh_pos(Fh *f);
  void unlock_fh_pos(Fh *f);
  
  // metadata cache
  void update_dir_dist(Inode *in, DirStat *st);

  Inode* insert_trace(MetaRequest *request, utime_t ttl, int mds);
  void update_inode_file_bits(Inode *in,
			      uint64_t truncate_seq, uint64_t truncate_size, uint64_t size,
			      uint64_t time_warp_seq, utime_t ctime, utime_t mtime, utime_t atime,
			      int issued);
  Inode *add_update_inode(InodeStat *st, utime_t ttl, int mds);
  Dentry *insert_dentry_inode(Dir *dir, const string& dname, LeaseStat *dlease, 
			      Inode *in, utime_t from, int mds, bool set_offset,
			      Dentry *old_dentry = NULL);


  // ----------------------
  // fs ops.
private:

  void fill_dirent(struct dirent *de, const char *name, int type, uint64_t ino, loff_t next_off);

  // some readdir helpers
  typedef int (*add_dirent_cb_t)(void *p, struct dirent *de, struct stat *st, int stmask, off_t off);

  int _opendir(Inode *in, DirResult **dirpp, int uid=-1, int gid=-1);
  void _readdir_drop_dirp_buffer(DirResult *dirp);
  bool _readdir_have_frag(DirResult *dirp);
  void _readdir_next_frag(DirResult *dirp);
  void _readdir_rechoose_frag(DirResult *dirp);
  int _readdir_get_frag(DirResult *dirp);
  int _readdir_cache_cb(DirResult *dirp, add_dirent_cb_t cb, void *p);
  void _closedir(DirResult *dirp);

  // other helpers
  void _ll_get(Inode *in);
  int _ll_put(Inode *in, int num);
  void _ll_drop_pins();

  Fh *_create_fh(Inode *in, int flags, int cmode);

  int _read_sync(Fh *f, uint64_t off, uint64_t len, bufferlist *bl);
  int _read_async(Fh *f, uint64_t off, uint64_t len, bufferlist *bl);

  // internal interface
  //   call these with client_lock held!
  int _do_lookup(Inode *dir, const char *name, Inode **target);
  int _lookup(Inode *dir, const string& dname, Inode **target);

  int _link(Inode *in, Inode *dir, const char *name, int uid=-1, int gid=-1);
  int _unlink(Inode *dir, const char *name, int uid=-1, int gid=-1);
  int _rename(Inode *olddir, const char *oname, Inode *ndir, const char *nname, int uid=-1, int gid=-1);
  int _mkdir(Inode *dir, const char *name, mode_t mode, int uid=-1, int gid=-1);
  int _rmdir(Inode *dir, const char *name, int uid=-1, int gid=-1);
  int _symlink(Inode *dir, const char *name, const char *target, int uid=-1, int gid=-1);
  int _mknod(Inode *dir, const char *name, mode_t mode, dev_t rdev, int uid=-1, int gid=-1);
  int _setattr(Inode *in, stat_precise *attr, int mask, int uid=-1, int gid=-1);
  int _getattr(Inode *in, int mask, int uid=-1, int gid=-1);
  int _getxattr(Inode *in, const char *name, void *value, size_t len, int uid=-1, int gid=-1);
  int _listxattr(Inode *in, char *names, size_t len, int uid=-1, int gid=-1);
  int _setxattr(Inode *in, const char *name, const void *value, size_t len, int flags, int uid=-1, int gid=-1);
  int _removexattr(Inode *in, const char *nm, int uid=-1, int gid=-1);
  int _open(Inode *in, int flags, mode_t mode, Fh **fhp, int uid=-1, int gid=-1);
  int _create(Inode *in, const char *name, int flags, mode_t mode, Inode **inp, Fh **fhp, int uid=-1, int gid=-1);
  int _release(Fh *fh);
  loff_t _lseek(Fh *fh, loff_t offset, int whence);
  int _read(Fh *fh, int64_t offset, uint64_t size, bufferlist *bl);
  int _write(Fh *fh, int64_t offset, uint64_t size, const char *buf);
  int _flush(Fh *fh);
  int _fsync(Fh *fh, bool syncdataonly);
  int _sync_fs();

  int get_or_create(Inode *dir, const char* name,
		    Dentry **pdn, bool expect_null=false);

public:
  int mount(const std::string &mount_root);
  int unmount();

  // these shoud (more or less) mirror the actual system calls.
  int statfs(const char *path, struct statvfs *stbuf);

  // crap
  int chdir(const char *s);
  void getcwd(std::string& cwd);

  // namespace ops
  int opendir(const char *name, DIR **dirpp);
  int closedir(DIR *dirp);

  int readdir_r_cb(DIR *dirp, add_dirent_cb_t cb, void *p);

  int readdir_r(DIR *dirp, struct dirent *de);
  int readdirplus_r(DIR *dirp, struct dirent *de, struct stat *st, int *stmask);

  int getdir(const char *relpath, list<string>& names);  // get the whole dir at once.

  int _getdents(DIR *dirp, char *buf, int buflen, bool ful);  // get a bunch of dentries at once
  int getdents(DIR *dirp, char *buf, int buflen) {
    return _getdents(dirp, buf, buflen, true);
  }
  int getdnames(DIR *dirp, char *buf, int buflen) {
    return _getdents(dirp, buf, buflen, false);
  }

  void rewinddir(DIR *dirp); 
  loff_t telldir(DIR *dirp);
  void seekdir(DIR *dirp, loff_t offset);

  int link(const char *existing, const char *newname);
  int unlink(const char *path);
  int rename(const char *from, const char *to);

  // dirs
  int mkdir(const char *path, mode_t mode);
  int mkdirs(const char *path, mode_t mode);
  int rmdir(const char *path);

  // symlinks
  int readlink(const char *path, char *buf, loff_t size);
  int symlink(const char *existing, const char *newname);

  // inode stuff
  int lstat(const char *path, struct stat *stbuf, frag_info_t *dirstat=0, int mask=CEPH_STAT_CAP_INODE_ALL);
  int lstat_precise(const char *relpath, struct stat_precise *stbuf, frag_info_t *dirstat=0, int mask=CEPH_STAT_CAP_INODE_ALL);
  int lstatlite(const char *path, struct statlite *buf);

  int setattr(const char *relpath, stat_precise *attr, int mask);
  int chmod(const char *path, mode_t mode);
  int chown(const char *path, uid_t uid, gid_t gid);
  int utime(const char *path, struct utimbuf *buf);
  int truncate(const char *path, loff_t size);

  // file ops
  int mknod(const char *path, mode_t mode, dev_t rdev=0);
  int open(const char *path, int flags, mode_t mode=0);
  int close(int fd);
  loff_t lseek(int fd, loff_t offset, int whence);
  int read(int fd, char *buf, loff_t size, loff_t offset=-1);
  int write(int fd, const char *buf, loff_t size, loff_t offset=-1);
  int fake_write_size(int fd, loff_t size);
  int ftruncate(int fd, loff_t size);
  int fsync(int fd, bool syncdataonly);
  int fstat(int fd, struct stat *stbuf);

  int sync_fs();
  int64_t drop_caches();

  // hpc lazyio
  int lazyio_propogate(int fd, loff_t offset, size_t count);
  int lazyio_synchronize(int fd, loff_t offset, size_t count);

  // expose file layout
  int describe_layout(int fd, ceph_file_layout* layout);
  int get_file_stripe_unit(int fd);
  int get_file_stripe_width(int fd);
  int get_file_stripe_period(int fd);
  int get_file_replication(int fd);
  int get_file_stripe_address(int fd, loff_t offset, string& address);
  int get_local_osd();
  int get_default_preferred_pg(int fd);

  void set_default_file_stripe_unit(int stripe_unit);
  void set_default_file_stripe_count(int count);
  void set_default_object_size(int size);
  void set_default_file_replication(int replication);
  void set_default_preferred_pg(int pg);

  int enumerate_layout(int fd, vector<ObjectExtent>& result,
		       loff_t length, loff_t offset);

  int mksnap(const char *path, const char *name);
  int rmsnap(const char *path, const char *name);

  // low-level interface
  int ll_lookup(vinodeno_t parent, const char *name, struct stat *attr, int uid = -1, int gid = -1);
  bool ll_forget(vinodeno_t vino, int count);
  Inode *_ll_get_inode(vinodeno_t vino);
  int ll_getattr(vinodeno_t vino, struct stat *st, int uid = -1, int gid = -1);
  int ll_setattr(vinodeno_t vino, struct stat *st, int mask, int uid = -1, int gid = -1);
  int ll_getxattr(vinodeno_t vino, const char *name, void *value, size_t size, int uid=-1, int gid=-1);
  int ll_setxattr(vinodeno_t vino, const char *name, const void *value, size_t size, int flags, int uid=-1, int gid=-1);
  int ll_removexattr(vinodeno_t vino, const char *name, int uid=-1, int gid=-1);
  int ll_listxattr(vinodeno_t vino, char *list, size_t size, int uid=-1, int gid=-1);
  int ll_opendir(vinodeno_t vino, void **dirpp, int uid = -1, int gid = -1);
  void ll_releasedir(void *dirp);
  int ll_readlink(vinodeno_t vino, const char **value, int uid = -1, int gid = -1);
  int ll_mknod(vinodeno_t vino, const char *name, mode_t mode, dev_t rdev, struct stat *attr, int uid = -1, int gid = -1);
  int ll_mkdir(vinodeno_t vino, const char *name, mode_t mode, struct stat *attr, int uid = -1, int gid = -1);
  int ll_symlink(vinodeno_t vino, const char *name, const char *value, struct stat *attr, int uid = -1, int gid = -1);
  int ll_unlink(vinodeno_t vino, const char *name, int uid = -1, int gid = -1);
  int ll_rmdir(vinodeno_t vino, const char *name, int uid = -1, int gid = -1);
  int ll_rename(vinodeno_t parent, const char *name, vinodeno_t newparent, const char *newname, int uid = -1, int gid = -1);
  int ll_link(vinodeno_t vino, vinodeno_t newparent, const char *newname, struct stat *attr, int uid = -1, int gid = -1);
  int ll_open(vinodeno_t vino, int flags, Fh **fh, int uid = -1, int gid = -1);
  int ll_create(vinodeno_t parent, const char *name, mode_t mode, int flags, struct stat *attr, Fh **fh, int uid = -1, int gid = -1);
  int ll_read(Fh *fh, loff_t off, loff_t len, bufferlist *bl);
  int ll_write(Fh *fh, loff_t off, loff_t len, const char *data);
  int ll_flush(Fh *fh);
  int ll_fsync(Fh *fh, bool syncdataonly);
  int ll_release(Fh *fh);
  int ll_statfs(vinodeno_t vino, struct statvfs *stbuf);
};

#endif
