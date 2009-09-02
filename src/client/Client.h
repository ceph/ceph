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


#ifndef __CLIENT_H
#define __CLIENT_H

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



class MClientSession;
class MClientRequest;
class MClientRequestForward;
class MClientLease;
class MClientCaps;
class MClientCapRelease;

class Filer;
class Objecter;
class ObjectCacher;

extern class LogType client_logtype;
extern class Logger  *client_logger;


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
struct Inode;

struct MetaRequest {
  tid_t tid;
  MClientRequest *request;    
  bufferlist request_payload;  // in case i have to retry
  
  int uid, gid;
  
  utime_t  sent_stamp;
  set<int> mds;                // who i am asking
  int      resend_mds;         // someone wants you to (re)send the request here
  int      num_fwd;            // # of times i've been forwarded
  int      retry_attempt;
  int      ref;
  
  MClientReply *reply;         // the reply
  
  //possible responses
  bool got_safe;
  bool got_unsafe;

  xlist<MetaRequest*>::item unsafe_item;
  Mutex lock; //for get/set sync

  Cond  *caller_cond;          // who to take up
  Cond  *dispatch_cond;        // who to kick back

  Inode *target;

  MetaRequest(MClientRequest *req, tid_t t) : 
    tid(t), request(req), 
    resend_mds(-1), num_fwd(0), retry_attempt(0),
    ref(1), reply(0), 
    got_safe(false), got_unsafe(false), unsafe_item(this),
    lock("MetaRequest lock"),
    caller_cond(0), dispatch_cond(0), target(0) { }

  MetaRequest* get() {++ref; return this; }

  void put() {if(--ref == 0) delete this; }
};


struct MDSSession {
  version_t seq;
  __u64 cap_gen;
  utime_t cap_ttl, last_cap_renew_request;
  int num_caps;
  entity_inst_t inst;
  bool closing;
  bool was_stale;

  xlist<InodeCap*> caps;
  xlist<Inode*> flushing_caps;
  xlist<MetaRequest*> unsafe_requests;

  MClientCapRelease *release;
  
  MDSSession() : seq(0), cap_gen(0), num_caps(0),
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
  int lease_mds;
  utime_t lease_ttl;
  __u64 lease_gen;
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
  
  Dentry() : dir(0), inode(0), ref(0), lease_mds(-1), lease_gen(0), lease_seq(0), cap_shared_gen(0) { }
};

class Dir {
 public:
  Inode    *parent_inode;  // my inode
  hash_map<nstring, Dentry*> dentries;

  Dir(Inode* in) { parent_inode = in; }

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

  __u64 cap_id;
  unsigned issued;
  unsigned implemented;
  unsigned wanted;   // as known to mds.
  __u64 seq, issue_seq;
  __u32 mseq;  // migration seq
  __u32 gen;

  InodeCap() : session(NULL), inode(NULL), cap_item(this), issued(0),
	       implemented(0), wanted(0), seq(0), issue_seq(0), mseq(0), gen(0) {}
};

struct CapSnap {
  //snapid_t follows;  // map key
  SnapContext context;
  int issued, dirty;
  __u64 size;
  utime_t ctime, mtime, atime;
  version_t time_warp_seq;
  bool writing, dirty_data;
  __u64 flush_tid;
  CapSnap() : issued(0), dirty(0), size(0), time_warp_seq(0), writing(false), dirty_data(false), flush_tid(0) {}
};


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
  ceph_file_layout layout;
  uint64_t   size;        // on directory, # dentries
  uint32_t   truncate_seq;
  uint64_t   truncate_size, truncate_from;
  utime_t    mtime;   // file data modify time.
  utime_t    atime;   // file data access time.
  uint32_t   time_warp_seq;  // count of (potential) mtime/atime timewarps (i.e., utimes())

  __u64 max_size;  // max size we can write to

  // dirfrag, recursive accountin
  frag_info_t dirstat;
  nest_info_t rstat;
 
  // special stuff
  version_t version;           // auth only
  version_t xattr_version;

  bool is_symlink() const { return (mode & S_IFMT) == S_IFLNK; }
  bool is_dir()     const { return (mode & S_IFMT) == S_IFDIR; }
  bool is_file()    const { return (mode & S_IFMT) == S_IFREG; }

  // about the dir (if this is one!)
  int       dir_auth;
  set<int>  dir_contacts;
  bool      dir_hashed, dir_replicated;

  // per-mds caps
  map<int,InodeCap*> caps;            // mds -> InodeCap
  InodeCap *auth_cap;
  unsigned dirty_caps, flushing_caps;
  __u64 flushing_cap_seq;
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

  __u64     reported_size, wanted_max_size, requested_max_size;

  int       ref;      // ref count. 1 for each dentry, fh that links to me.
  int       ll_ref;   // separate ref count for ll client
  Dir       *dir;     // if i'm a dir.
  Dentry    *dn;      // if i'm linked to a dentry.
  string    symlink;  // symlink content, if it's a symlink
  fragtree_t dirfragtree;
  map<string,bufferptr> xattrs;
  map<frag_t,int> fragmap;  // known frag -> mds mappings

  list<Cond*>       waitfor_caps;
  list<Cond*>       waitfor_commit;

  // <hack>
  bool hack_balance_reads;
  // </hack>

  void make_long_path(filepath& p) {
    if (dn) {
      assert(dn->dir && dn->dir->parent_inode);
      dn->dir->parent_inode->make_long_path(p);
      p.push_dentry(dn->name);
    } else if (snapdir_parent) {
      snapdir_parent->make_path(p);
      string empty;
      p.push_dentry(empty);
    } else
      p = filepath(ino);
  }

  void make_path(filepath& p) {
    if (snapid == CEPH_NOSNAP) {
      p = filepath(ino);
    } else if (snapdir_parent) {
      snapdir_parent->make_path(p);
      string empty;
      p.push_dentry(empty);
    } else if (dn) {
      assert(dn->dir && dn->dir->parent_inode);
      dn->dir->parent_inode->make_path(p);
      p.push_dentry(dn->name);
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
    //inode(_inode),
    ino(vino.ino), snapid(vino.snapid),
    rdev(0), mode(0), uid(0), gid(0), nlink(0), size(0), truncate_seq(0), truncate_size(0), truncate_from(0),
    time_warp_seq(0), max_size(0), version(0), xattr_version(0),
    dir_auth(-1), dir_hashed(false), dir_replicated(false), 
    dirty_caps(0), flushing_caps(0), flushing_cap_seq(0), shared_gen(0), cache_gen(0),
    snap_caps(0), snap_cap_refs(0),
    exporting_issued(0), exporting_mds(-1), exporting_mseq(0),
    cap_item(this), flushing_cap_item(this), last_flush_tid(0),
    snaprealm(0), snaprealm_item(this), snapdir_parent(0),
    reported_size(0), wanted_max_size(0), requested_max_size(0),
    ref(0), ll_ref(0), 
    dir(0), dn(0),
    hack_balance_reads(false)
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

 
  int authority(const string& dname) {
    if (!dirfragtree.empty()) {
      __gnu_cxx::hash<string> H;
      frag_t fg = dirfragtree[H(dname)];
      while (fg != frag_t()) {
	if (fragmap.count(fg) &&
	    fragmap[fg] >= 0) {
	  //cout << "picked frag ino " << inode.ino << " dname " << dname << " fg " << fg << " mds" << fragmap[fg] << std::endl;
	  return fragmap[fg];
	}
	fg = frag_t(fg.value(), fg.bits()-1); // try more general...
      }
    }
    return authority();
  }

  int authority() {
    if (dir_auth >= 0)
      return dir_auth;

    assert(dn);
    return dn->dir->parent_inode->authority(dn->name);
  }


  int pick_replica(MDSMap *mdsmap) {
    // replicas?
    /* fixme
    if (//ino() > 1ULL && 
	dir_contacts.size()) {
      set<int>::iterator it = dir_contacts.begin();
      if (dir_contacts.size() == 1)
        return *it;
      else {
	//cout << "dir_contacts on " << inode.ino << " is " << dir_contacts << std::endl;
	int r = 1 + (rand() % dir_contacts.size());
	int a = authority();
	while (r--) {
	  it++;
	  if (mdsmap->is_down(*it)) it++;
	  if (it == dir_contacts.end()) it = dir_contacts.begin();
	  if (*it == a) it++;  // skip the authority
	  if (it == dir_contacts.end()) it = dir_contacts.begin();
	}
	return *it;
      }
    }
    */

    if (dir_replicated) {// || ino() == 1) {
      // pick a random mds that isn't the auth
      set<int> s;
      mdsmap->get_mds_set(s);
      set<int>::iterator it = s.begin();
      if (s.empty())
	return 0;
      if (s.size() == 1)
        return *it;
      else {
	//cout << "dir_contacts on " << inode.ino << " is " << dir_contacts << std::endl;
	int r = 1 + (rand() % s.size());
	int a = authority();
	while (r--) {
	  it++;
	  if (mdsmap->is_down(*it)) it++;
	  if (it == s.end()) it = s.begin();
	  if (*it == a) it++;  // skip the authority
	  if (it == s.end()) it = s.begin();
	}
	//if (ino == 1) cout << "chose " << *it << " from " << s << std::endl;
	return *it;
      }
      //cout << "num_mds is " << mdcluster->get_num_mds() << endl;
      //return mdsmap->get_random_in_mds();
      //return rand() % mdsmap->get_num_mds();  // huh.. pick a random mds!
    }
    else
      return authority();
  }


  // open Dir for an inode.  if it's not open, allocated it (and pin dentry in memory).
  Dir *open_dir() {
    if (!dir) {
      if (dn) dn->get();      // pin dentry
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
  
  /* getdir result */
  struct DirEntry {
    string d_name;
    struct stat st;
    int stmask;
    DirEntry(const string &s) : d_name(s), stmask(0) {}
    DirEntry(const string &n, struct stat& s, int stm) : d_name(n), st(s), stmask(stm) {}
  };

  struct DirResult {
    static const int SHIFT = 28;
    static const int64_t MASK = (1 << SHIFT) - 1;
    static const loff_t END = 1ULL << (SHIFT + 32);

    Inode *inode;
    int64_t offset;   // high bits: frag_t, low bits: an offset
    map<frag_t, vector<DirEntry> > buffer;

    DirResult(Inode *in) : inode(in), offset(0) { 
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
  map<int, MDSSession> mds_sessions;  // mds -> push seq
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

  int make_request(MClientRequest *req, int uid, int gid,
		   Inode **ptarget = 0,
		   int use_mds=-1, bufferlist *pdirbl=0);
  int choose_target_mds(MClientRequest *req);
  void send_request(MetaRequest *request, int mds);
  void kick_requests(int mds, bool signal);
  void handle_client_request_forward(MClientRequestForward *reply);
  void handle_client_reply(MClientReply *reply);

  bool   mounted;
  bool   unmounting;

  int    unsafe_sync_write;

  int file_stripe_unit;
  int file_stripe_count;
  int object_size;
  int file_replication;
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
    int fd = free_fd_set.start();
    free_fd_set.erase(fd, 1);
    return fd;
  }
  void put_fd(int fd) {
    free_fd_set.insert(fd, 1);
  }

  // global client lock
  //  - protects Client and buffer cache both!
  Mutex                  client_lock;

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
    if (in->dn) in->dn->put();   // unpin dentry
    
    delete in->dir;
    in->dir = 0;
    put_inode(in);               // unpin inode
  }

  //int get_cache_size() { return lru.lru_get_size(); }
  //void set_cache_size(int m) { lru.lru_set_max(m); }

  Dentry* link(Dir *dir, const string& name, Inode *in) {
    Dentry *dn = new Dentry;
    dn->name = name;
    
    // link to dir
    dn->dir = dir;
    //cout << "link dir " << dir->parent_inode->ino << " '" << name << "' -> inode " << in->ino << endl;
    dir->dentries[dn->name] = dn;

    // link to inode
    dn->inode = in;
    assert(in->dn == 0);
    in->dn = dn;
    in->get();

    if (in->dir) dn->get();  // dir -> dn pin

    lru.lru_insert_mid(dn);    // mid or top?
    return dn;
  }

  void unlink(Dentry *dn, bool keepdir = false) {
    Inode *in = dn->inode;
    assert(in->dn == dn);

    // unlink from inode
    if (dn->inode->dir) dn->put();        // dir -> dn pin
    dn->inode = 0;
    in->dn = 0;
    put_inode(in);
        
    // unlink from dir
    dn->dir->dentries.erase(dn->name);
    if (dn->dir->is_empty() && !keepdir) 
      close_dir(dn->dir);
    dn->dir = 0;

    // delete den
    lru.lru_remove(dn);
    delete dn;
  }

  Dentry *relink_inode(Dir *dir, const string& name, Inode *in) {
    Dentry *olddn = in->dn;
    Dir *olddir = olddn->dir;  // note: might == dir!

    // newdn, attach to inode.  don't touch inode ref.
    Dentry *newdn = new Dentry;
    newdn->dir = dir;
    newdn->name = name;
    newdn->inode = in;
    in->dn = newdn;

    if (in->dir) { // dir -> dn pin
      newdn->get();
      olddn->put();
    }

    // unlink old dn from dir
    olddir->dentries.erase(olddn->name);
    olddn->inode = 0;
    olddn->dir = 0;
    lru.lru_remove(olddn);
    delete olddn;
    
    // link new dn to dir
    dir->dentries[name] = newdn;
    lru.lru_insert_mid(newdn);
    
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

 public:
  Client(Messenger *m, MonClient *mc);
  ~Client();
  void tear_down_cache();   

  client_t get_nodeid() { return whoami; }

  void init();
  void shutdown();

  // messaging
  void handle_mds_map(class MMDSMap *m);

  void handle_lease(MClientLease *m);

  void release_lease(Inode *in, Dentry *dn, int mask);

  // file caps
  void add_update_cap(Inode *in, int mds, __u64 cap_id,
		      unsigned issued, unsigned seq, unsigned mseq, inodeno_t realm,
		      int flags);
  void remove_cap(Inode *in, int mds);
  void remove_all_caps(Inode *in);
  void remove_session_caps(int mds_num);
  void trim_caps(int mds, int max);
  void mark_caps_dirty(Inode *in, int caps);
  void flush_caps();
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
  void send_cap(Inode *in, int mds, InodeCap *cap, int used, int want, int retain, int flush, __u64 tid);
  void check_caps(Inode *in, bool is_delayed);
  void get_cap_ref(Inode *in, int cap);
  void put_cap_ref(Inode *in, int cap);
  void flush_snaps(Inode *in);
  void wait_sync_caps(__u64 want);
  void queue_cap_snap(Inode *in, snapid_t seq=0);
  void finish_cap_snap(Inode *in, CapSnap *capsnap, int used);
  void _flushed_cap_snap(Inode *in, snapid_t seq);

  void _release(Inode *in, bool checkafter=true);
  void _flush(Inode *in, Context *onfinish=NULL);
  void _flushed(Inode *in);
  void flush_set_callback(inodeno_t ino);

  void close_release(Inode *in);
  void close_safe(Inode *in);

  void lock_fh_pos(Fh *f);
  void unlock_fh_pos(Fh *f);
  
  // metadata cache
  void update_dir_dist(Inode *in, DirStat *st);

  Inode* insert_trace(MetaRequest *request, utime_t ttl, int mds);
  void update_inode_file_bits(Inode *in,
			      __u64 truncate_seq, __u64 truncate_size, __u64 size,
			      __u64 time_warp_seq, utime_t ctime, utime_t mtime, utime_t atime,
			      int issued);
  Inode *add_update_inode(InodeStat *st, utime_t ttl, int mds);
  void insert_dentry_inode(Dir *dir, const string& dname, LeaseStat *dlease, 
			   Inode *in, utime_t from, int mds);


  // ----------------------
  // fs ops.
private:

  // some helpers
  int _opendir(Inode *in, DirResult **dirpp, int uid=-1, int gid=-1);
  void _readdir_add_dirent(DirResult *dirp, const string& name, Inode *in);
  void _readdir_fill_dirent(struct dirent *de, DirEntry *entry, loff_t);
  bool _readdir_have_frag(DirResult *dirp);
  void _readdir_next_frag(DirResult *dirp);
  void _readdir_rechoose_frag(DirResult *dirp);
  int _readdir_get_frag(DirResult *dirp);
  void _closedir(DirResult *dirp);
  void _ll_get(Inode *in);
  int _ll_put(Inode *in, int num);
  void _ll_drop_pins();

  Fh *_create_fh(Inode *in, int flags, int cmode);

  int _read_sync(Fh *f, __u64 off, __u64 len, bufferlist *bl);
  int _read_async(Fh *f, __u64 off, __u64 len, bufferlist *bl);

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
  int _read(Fh *fh, __s64 offset, __u64 size, bufferlist *bl);
  int _write(Fh *fh, __s64 offset, __u64 size, const char *buf);
  int _flush(Fh *fh);
  int _fsync(Fh *fh, bool syncdataonly);
  int _sync_fs();



public:
  int mount();
  int unmount();

  // these shoud (more or less) mirror the actual system calls.
  int statfs(const char *path, struct statvfs *stbuf);

  // crap
  int chdir(const char *s);
  void getcwd(std::string& cwd);

  // namespace ops
  int getdir(const char *relpath, list<string>& names);  // get the whole dir at once.

  int opendir(const char *name, DIR **dirpp);
  int closedir(DIR *dirp);
  int readdir_r(DIR *dirp, struct dirent *de);
  int readdirplus_r(DIR *dirp, struct dirent *de, struct stat *st, int *stmask);

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

  void set_default_file_stripe_unit(int stripe_unit);
  void set_default_file_stripe_count(int count);
  void set_default_object_size(int size);
  void set_default_file_replication(int replication);

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

  // failure
  void ms_handle_failure(Message*, const entity_inst_t& inst);
  void ms_handle_reset(const entity_addr_t& addr, entity_name_t last);
  void ms_handle_remote_reset(const entity_addr_t& addr, entity_name_t last);
};

#endif
