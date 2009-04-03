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



// unix-ey fs stuff
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <utime.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/statvfs.h>


#include <iostream>
using namespace std;

#include "config.h"

// ceph stuff
#include "Client.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"

#include "messages/MMonMap.h"

#include "messages/MClientMount.h"
#include "messages/MClientMountAck.h"
#include "messages/MClientUnmount.h"
#include "messages/MClientSession.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientReply.h"
#include "messages/MClientCaps.h"
#include "messages/MClientLease.h"
#include "messages/MClientSnap.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSGetMap.h"
#include "messages/MMDSMap.h"

#include "osdc/Filer.h"
#include "osdc/Objecter.h"
#include "osdc/ObjectCacher.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/Logger.h"



#include "config.h"

#define DOUT_SUBSYS client
#undef dout_prefix
#define dout_prefix *_dout << dbeginl << "client" << whoami << " "

#define  tout       if (g_conf.client_trace) traceout


// static logger
Mutex client_logger_lock("client_logger_lock");
LogType client_logtype(l_c_first, l_c_last);
Logger  *client_logger = 0;




ostream& operator<<(ostream &out, Inode &in)
{
  out << in.vino() << "("
      << " cap_refs=" << in.cap_refs
      << " open=" << in.open_by_mode
      << " ref=" << in.ref
      << " parent=" << in.dn
      << ")";
  return out;
}


void client_flush_set_callback(void *p, inodeno_t ino)
{
  Client *client = (Client*)p;
  client->flush_set_callback(ino);
}


// cons/des

Client::Client(Messenger *m, MonMap *mm) : timer(client_lock), client_lock("Client::client_lock")
{
  // which client am i?
  whoami = m->get_myname().num();
  monmap = mm;
  
  tick_event = 0;

  mounted = false;
  mounters = 0;
  mount_timeout_event = 0;
  unmounting = false;

  last_tid = 0;
  unsafe_sync_write = 0;

  mdsmap = 0;

  cwd = NULL;

  // 
  root = 0;

  lru.lru_set_max(g_conf.client_cache_size);

  // file handles
  free_fd_set.insert(10, 1<<30);

  // set up messengers
  messenger = m;

  // osd interfaces
  osdmap = new OSDMap();     // initially blank.. see mount()
  objecter = new Objecter(messenger, monmap, osdmap, client_lock);
  objecter->set_client_incarnation(0);  // client always 0, for now.
  objectcacher = new ObjectCacher(objecter, client_lock, 
				  0,                            // all ack callback
				  client_flush_set_callback,    // all commit callback
				  (void*)this);
  filer = new Filer(objecter);
}


Client::~Client() 
{
  tear_down_cache();

  if (objectcacher) { 
    delete objectcacher; 
    objectcacher = 0; 
  }

  if (filer) { delete filer; filer = 0; }
  if (objecter) { delete objecter; objecter = 0; }
  if (osdmap) { delete osdmap; osdmap = 0; }
  if (mdsmap) { delete mdsmap; mdsmap = 0; }
  if (messenger)
    messenger->destroy();
}


void Client::tear_down_cache()
{
  // fd's
  for (hash_map<int, Fh*>::iterator it = fd_map.begin();
       it != fd_map.end();
       it++) {
    Fh *fh = it->second;
    dout(1) << "tear_down_cache forcing close of fh " << it->first << " ino " << fh->inode->inode.ino << dendl;
    put_inode(fh->inode);
    delete fh;
  }
  fd_map.clear();

  // caps!
  // *** FIXME ***

  // empty lru
  lru.lru_set_max(0);
  trim_cache();
  assert(lru.lru_get_size() == 0);

  // close root ino
  assert(inode_map.size() <= 1);
  if (root && inode_map.size() == 1) {
    delete root;
    root = 0;
    inode_map.clear();
  }

  assert(inode_map.empty());
}



// debug crapola

void Client::dump_inode(Inode *in, set<Inode*>& did)
{
  dout(1) << "dump_inode: inode " << in->ino() << " ref " << in->ref << " dir " << in->dir << dendl;

  if (in->dir) {
    dout(1) << "  dir size " << in->dir->dentries.size() << dendl;
    //for (hash_map<const char*, Dentry*, hash<const char*>, eqstr>::iterator it = in->dir->dentries.begin();
    for (hash_map<nstring, Dentry*>::iterator it = in->dir->dentries.begin();
         it != in->dir->dentries.end();
         it++) {
      dout(1) << "    dn " << it->first << " ref " << it->second->ref << dendl;
      dump_inode(it->second->inode, did);
    }
  }
}

void Client::dump_cache()
{
  set<Inode*> did;

  if (root) dump_inode(root, did);

  for (hash_map<vinodeno_t, Inode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it++) {
    if (did.count(it->second)) continue;
    
    dout(1) << "dump_cache: inode " << it->first
            << " ref " << it->second->ref 
            << " dir " << it->second->dir << dendl;
    if (it->second->dir) {
      dout(1) << "  dir size " << it->second->dir->dentries.size() << dendl;
    }
  }
 
}


void Client::init() 
{
  Mutex::Locker lock(client_lock);

  tick();

  // do logger crap only once per process.
  static bool did_init = false;
  if (did_init) return;
  did_init = true;
  
  // logger?
  client_logger_lock.Lock();
  if (client_logger == 0) {
    client_logtype.add_inc(l_c_reply, "reply");
    client_logtype.add_avg(l_c_lat, "lat");
    client_logtype.add_avg(l_c_wrlat, "wrlat");
    client_logtype.add_avg(l_c_owrlat, "owrlat");
    client_logtype.add_avg(l_c_ordlat, "ordlat");
    client_logtype.validate();
    
    char s[80];
    char hostname[80];
    gethostname(hostname, 79);
    sprintf(s,"clients.%s.%d", hostname, getpid());
    client_logger = new Logger(s, &client_logtype);
  }
  client_logger_lock.Unlock();

}

void Client::shutdown() 
{
  dout(1) << "shutdown" << dendl;
  messenger->shutdown();
}




// ===================
// metadata cache stuff

void Client::trim_cache()
{
  unsigned last = 0;
  while (lru.lru_get_size() != last) {
    last = lru.lru_get_size();

    if (lru.lru_get_size() <= lru.lru_get_max())  break;

    // trim!
    Dentry *dn = (Dentry*)lru.lru_expire();
    if (!dn) break;  // done
    
    dout(15) << "trim_cache unlinking dn " << dn->name 
	     << " in dir " << hex << dn->dir->parent_inode->inode.ino 
	     << dendl;
    unlink(dn);
  }

  // hose root?
  if (lru.lru_get_size() == 0 && root && root->ref == 0 && inode_map.size() == 1) {
    dout(15) << "trim_cache trimmed root " << root << dendl;
    delete root;
    root = 0;
    inode_map.clear();
  }
}


void Client::update_inode_file_bits(Inode *in,
				    __u64 truncate_seq, __u64 truncate_size,
				    __u64 size,
				    __u64 time_warp_seq, utime_t ctime,
				    utime_t mtime,
				    utime_t atime,
				    int issued)
{
  bool warn = false;

  if (truncate_seq > in->inode.truncate_seq ||
      (truncate_seq == in->inode.truncate_seq && size > in->inode.size)) {
    dout(10) << "size " << in->inode.size << " -> " << size << dendl;
    in->inode.size = size;
    in->reported_size = size;
    in->inode.truncate_size = truncate_size;
  }

  // be careful with size, mtime, atime
  if (issued & CEPH_CAP_FILE_EXCL) {
    if (ctime > in->inode.ctime) 
      in->inode.ctime = ctime;
    if (time_warp_seq > in->inode.time_warp_seq)
      dout(0) << "WARNING: " << *in << " mds time_warp_seq "
	      << time_warp_seq << " > " << in->inode.time_warp_seq << dendl;
  } else if (issued & (CEPH_CAP_FILE_WR|CEPH_CAP_FILE_WRBUFFER)) {
    if (time_warp_seq > in->inode.time_warp_seq) {
      in->inode.ctime = ctime;
      in->inode.mtime = mtime;
      in->inode.atime = atime;
      in->inode.time_warp_seq = time_warp_seq;
    } else if (time_warp_seq == in->inode.time_warp_seq) {
      if (ctime > in->inode.ctime) 
	in->inode.ctime = ctime;
      if (mtime > in->inode.mtime) 
	in->inode.mtime = mtime;
      if (atime > in->inode.atime)
	in->inode.atime = atime;
    } else
      warn = true;
  } else {
    if (time_warp_seq >= in->inode.time_warp_seq) {
      in->inode.ctime = ctime;
      in->inode.mtime = mtime;
      in->inode.atime = atime;
      in->inode.time_warp_seq = time_warp_seq;
    } else
      warn = true;
  }
  if (warn) {
    dout(0) << *in << " mds time_warp_seq "
	    << in->inode.time_warp_seq << " -> "
	    << time_warp_seq
	    << dendl;
  }
}

Inode * Client::add_update_inode(InodeStat *st, utime_t from, int mds)
{
  Inode *in;
  if (inode_map.count(st->vino))
    in = inode_map[st->vino];
  else {
    in = new Inode(st->vino, &st->layout);
    inode_map[st->vino] = in;
    if (in->ino() == 1) {
      root = in;
      root->dir_auth = 0;
      cwd = root;
      cwd->get();
    }
  }

  //utime_t ttl = from;
  //ttl += (float)lease->duration_ms * 1000.0;

  //dout(12) << "update_inode mask " << lease->mask << " ttl " << ttl << dendl;
  dout(12) << "add_update_inode " << *in << " caps " << ccap_string(st->cap.caps) << dendl;

  int issued = in->caps_issued();

  if (st->cap.caps) {
    if (in->snapid == CEPH_NOSNAP)
      add_update_cap(in, mds, st->cap.cap_id, st->cap.caps, st->cap.seq, st->cap.mseq, inodeno_t(st->cap.realm));
    else {
      in->snap_caps |= st->cap.caps;
    }
  }
  
  if (st->cap.caps & CEPH_CAP_PIN) {
    in->inode.ino = st->vino.ino;
    in->snapid = st->vino.snapid;
    in->inode.rdev = st->rdev;
    in->dirfragtree = st->dirfragtree;  // FIXME look at the mask!

    if ((issued & CEPH_CAP_AUTH_EXCL) == 0) {
      in->inode.mode = st->mode;
      in->inode.uid = st->uid;
      in->inode.gid = st->gid;
    }

    if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
      in->inode.nlink = st->nlink;
      in->inode.anchored = false;  /* lie */
    }

    if ((issued & CEPH_CAP_XATTR_EXCL) == 0 &&
	st->xattrbl.length() &&
	st->xattr_version > in->inode.xattr_version) {
      bufferlist::iterator p = st->xattrbl.begin();
      ::decode(in->xattrs, p);
      in->inode.xattr_version = st->xattr_version;
    }

    in->inode.dirstat = st->dirstat;
    in->inode.rstat = st->rstat;

    in->inode.layout = st->layout;
    in->inode.ctime = st->ctime;
    in->inode.max_size = st->max_size;  // right?

    update_inode_file_bits(in, st->truncate_seq, st->truncate_size, st->size,
			   st->time_warp_seq, st->ctime, st->mtime, st->atime,
			   in->caps_issued());
  }

  // symlink?
  if (in->inode.is_symlink())
    in->symlink = st->symlink;

  return in;
}


/*
 * insert_dentry_inode - insert + link a single dentry + inode into the metadata cache.
 */
void Client::insert_dentry_inode(Dir *dir, const string& dname, LeaseStat *dlease, 
				 Inode *in, utime_t from, int mds)
{
  utime_t dttl = from;
  dttl += (float)dlease->duration_ms * 1000.0;
  
  Dentry *dn = NULL;
  if (dir->dentries.count(dname))
    dn = dir->dentries[dname];
  
  dout(12) << "insert_dentry_inode " << dname << " vino " << in->vino()
	   << " in dir " << dir->parent_inode->inode.ino
           << dendl;
  
  if (dn) {
    if (dn->inode->vino() == in->vino()) {
      touch_dn(dn);
      dout(12) << " had dentry " << dname
               << " with correct vino " << dn->inode->vino()
               << dendl;
    } else {
      dout(12) << " had dentry " << dname
               << " with WRONG vino " << dn->inode->vino()
               << dendl;
      unlink(dn, true);
      dn = NULL;
    }
  }
  
  if (!dn) {
    // have inode linked elsewhere?  -> unlink and relink!
    if (in->dn) {
      dout(12) << " had vino " << in->vino()
	       << " not linked or linked at the right position, relinking"
	       << dendl;
      dn = relink_inode(dir, dname, in);
    } else {
      // link
      dout(12) << " had vino " << in->vino()
	       << " unlinked, linking" << dendl;
      dn = link(dir, dname, in);
    }
  }

  assert(dn && dn->inode);

  if (dlease->mask & CEPH_LOCK_DN) {
    utime_t ttl = from;
    ttl += (float)dlease->duration_ms * 1000.0;
    if (ttl > dn->lease_ttl) {
      dout(10) << "got dentry lease on " << dname << " dur " << dlease->duration_ms << "ms ttl " << ttl << dendl;
      dn->lease_ttl = ttl;
      dn->lease_mds = from;
      dn->lease_seq = dlease->seq;
    }
  }
}


/*
 * update MDS location cache for a single inode
 */
void Client::update_dir_dist(Inode *in, DirStat *dst)
{
  // auth
  in->dir_auth = -1;
  if (dst->frag == frag_t()) {
    in->dir_auth = dst->auth;
  } else {
    dout(20) << "got dirfrag map for " << in->inode.ino << " frag " << dst->frag << " to mds " << dst->auth << dendl;
    in->fragmap[dst->frag] = dst->auth;
  }

  // replicated
  in->dir_replicated = !dst->dist.empty();  // FIXME that's just one frag!
  
  // dist
  /*
  if (!st->dirfrag_dist.empty()) {   // FIXME
    set<int> dist = st->dirfrag_dist.begin()->second;
    if (dist.empty() && !in->dir_contacts.empty())
      dout(9) << "lost dist spec for " << in->inode.ino 
              << " " << dist << dendl;
    if (!dist.empty() && in->dir_contacts.empty()) 
      dout(9) << "got dist spec for " << in->inode.ino 
              << " " << dist << dendl;
    in->dir_contacts = dist;
  }
  */
}


/** insert_trace
 *
 * insert a trace from a MDS reply into the cache.
 */
Inode* Client::insert_trace(MClientReply *reply, utime_t from, int mds)
{

  bufferlist::iterator p = reply->get_trace_bl().begin();
  if (p.end()) {
    dout(10) << "insert_trace -- no trace" << dendl;
    return NULL;
  }

  // snap trace
  if (reply->snapbl.length())
    update_snap_trace(reply->snapbl);

  Inode *in = 0;
  if (reply->head.is_target) {
    InodeStat ist;
    ist.decode(p);
    in = add_update_inode(&ist, from, mds);
  }

  if (reply->head.is_dentry) {
    InodeStat dirst;
    DirStat dst;
    string dname;
    LeaseStat dlease;

    dirst.decode(p);
    dst.decode(p);
    ::decode(dname, p);
    ::decode(dlease, p);

    vinodeno_t vino = dirst.vino;
    assert(inode_map.count(vino));
    Inode *diri = inode_map[vino];
    
    update_dir_dist(diri, &dst);  // dir stat info is attached to inode...

    if (in) {
      Dir *dir = diri->open_dir();
      insert_dentry_inode(dir, dname, &dlease, in, from, mds);
    } else {
      Dentry *dn = NULL;
      if (diri->dir && diri->dir->dentries.count(dname)) {
	dn = diri->dir->dentries[dname];
	unlink(dn, false);
      }
    }
  }

  return in;
}



/*
 * bleh, dentry vs inode semantics here are sloppy
 */
Dentry *Client::lookup(const filepath& path, snapid_t snapid)
{
  dout(14) << "lookup " << path << dendl;

  Inode *cur;
  if (path.get_ino()) {
    vinodeno_t vino(path.get_ino(), snapid);
    if (inode_map.count(vino)) 
      cur = inode_map[vino];
    else
      return NULL;
  } else
    cur = root;
  if (!cur) return NULL;

  Dentry *dn = 0;
  for (unsigned i=0; i<path.depth(); i++) {
    dout(14) << " seg " << i << " = " << path[i] << dendl;
    if (cur->inode.is_dir() && cur->dir) {
      // dir, we can descend
      Dir *dir = cur->dir;
      if (dir->dentries.count(path[i])) {
        dn = dir->dentries[path[i]];
        dout(14) << " hit dentry " << path[i] << " inode is " << dn->inode << dendl;
      } else {
        dout(14) << " dentry " << path[i] << " dne" << dendl;
        return NULL;
      }
      cur = dn->inode;
      assert(cur);
    } else {
      return NULL;  // not a dir
    }
  }

  if (!dn) 
    dn = cur->dn;
  if (dn) {
    dout(11) << "lookup '" << path << "' found " << dn->name << " inode " << dn->inode->inode.ino << dendl;
  }

  return dn;
}

// -------

int Client::choose_target_mds(MClientRequest *req) 
{
  int mds = 0;
    
  // find deepest known prefix
  Inode *diri = root;   // the deepest known containing dir
  Inode *item = 0;      // the actual item... if we know it
  int missing_dn = -1;  // which dn we miss on (if we miss)
  
  unsigned depth = req->get_filepath().depth();
  unsigned i;
  for (i=0; i<depth; i++) {
    // dir?
    if (diri && diri->inode.is_dir() && diri->dir) {
      Dir *dir = diri->dir;
      
      // do we have the next dentry?
      if (dir->dentries.count( req->get_filepath()[i] ) == 0) {
	missing_dn = i;  // no.
	break;
      }
      
      dout(7) << " have path seg " << i << " on " << diri->dir_auth << " ino " << diri->inode.ino << " " << req->get_filepath()[i] << dendl;
      
      if (i == depth-1) {  // last one!
	item = dir->dentries[ req->get_filepath()[i] ]->inode;
	break;
      } 
      
      // continue..
      diri = dir->dentries[ req->get_filepath()[i] ]->inode;
      assert(diri);
    } else {
      missing_dn = i;
      break;
    }
  }
  
  // pick mds
  if (!diri || g_conf.client_use_random_mds) {
    // no root info, pick a random MDS
    mds = mdsmap->get_random_up_mds();
    dout(10) << "random mds" << mds << dendl;
    if (mds < 0) mds = 0;

    if (0) {
      mds = 0;
      dout(0) << "hack: sending all requests to mds" << mds << dendl;
    }
  } else {
    if (req->auth_is_best()) {
      // pick the actual auth (as best we can)
      if (item) {
	mds = item->authority();
      } else {
	mds = diri->authority(req->get_filepath()[missing_dn]);
      }
    } else {
      // balance our traffic!
      mds = diri->pick_replica(mdsmap); // for the _inode_
      dout(20) << "for " << req->get_filepath() << " diri " << diri->inode.ino << " rep " 
	      << diri->dir_contacts
	      << " mds" << mds << dendl;
    }
  }
  dout(20) << "mds is " << mds << dendl;
  
  return mds;
}



MClientReply *Client::make_request(MClientRequest *req, 
				   int uid, int gid, 
				   utime_t *pfrom,
 				   Inode **ptarget,
				   int use_mds)
{
  // time the call
  utime_t start = g_clock.real_now();
  
  bool nojournal = false;
  int op = req->get_op();
  if (op == CEPH_MDS_OP_GETATTR ||
      op == CEPH_MDS_OP_READDIR ||
      op == CEPH_MDS_OP_OPEN)
    nojournal = true;


  // -- request --
  // assign a unique tid
  tid_t tid = ++last_tid;
  req->set_tid(tid);

  if (!mds_requests.empty()) 
    req->set_oldest_client_tid(mds_requests.begin()->first);
  else
    req->set_oldest_client_tid(tid); // this one is the oldest.

  // make note
  MetaRequest request(req, tid);
  mds_requests[tid] = &request;

  if (uid < 0) {
    uid = geteuid();
    gid = getegid();
  }
  request.uid = uid;
  request.gid = gid;
  req->set_caller_uid(uid);
  req->set_caller_gid(gid);

  // encode payload now, in case we have to resend (in case of mds failure)
  req->encode_payload();
  request.request_payload = req->get_payload();

  // hack target mds?
  if (use_mds >= 0)
    request.resend_mds = use_mds;

  // set up wait cond
  Cond cond;
  request.caller_cond = &cond;
  
  while (1) {
    // choose mds
    int mds;
    // force use of a particular mds?
    if (request.resend_mds >= 0) {
      mds = request.resend_mds;
      request.resend_mds = -1;
      dout(10) << "target resend_mds specified as mds" << mds << dendl;
    } else {
      mds = choose_target_mds(req);
      if (mds >= 0) {
	dout(10) << "chose target mds" << mds << " based on hierarchy" << dendl;
      } else {
	mds = mdsmap->get_random_up_mds();
	if (mds < 0) mds = 0;  // hrm.
	dout(10) << "chose random target mds" << mds << " for lack of anything better" << dendl;
      }
    }
    
    // open a session?
    if (mds_sessions.count(mds) == 0) {
      Cond cond;

      if (!mdsmap->is_active(mds)) {
	dout(10) << "no address for mds" << mds << ", requesting new mdsmap" << dendl;
	int mon = monmap->pick_mon();
	messenger->send_message(new MMDSGetMap(monmap->fsid, mdsmap->get_epoch()+1),
				monmap->get_inst(mon));
	waiting_for_mdsmap.push_back(&cond);
	cond.Wait(client_lock);

	if (!mdsmap->is_active(mds)) {
	  dout(10) << "hmm, still have no address for mds" << mds << ", trying a random mds" << dendl;
	  request.resend_mds = mdsmap->get_random_up_mds();
	  continue;
	}
      }	
      
      if (waiting_for_session.count(mds) == 0) {
	dout(10) << "opening session to mds" << mds << dendl;
	messenger->send_message(new MClientSession(CEPH_SESSION_REQUEST_OPEN),
				mdsmap->get_inst(mds));
      }
      
      // wait
      waiting_for_session[mds].push_back(&cond);
      while (waiting_for_session.count(mds)) {
	dout(10) << "waiting for session to mds" << mds << " to open" << dendl;
	cond.Wait(client_lock);
      }
    }

    // send request.
    send_request(&request, mds);

    // wait for signal
    dout(20) << "awaiting kick on " << &cond << dendl;
    cond.Wait(client_lock);
    
    // did we get a reply?
    if (request.reply) 
      break;
  }

  // got it!
  MClientReply *reply = request.reply;
  int mds = reply->get_source().num();

  // kick dispatcher (we've got it!)
  assert(request.dispatch_cond);
  request.dispatch_cond->Signal();
  dout(20) << "sendrecv kickback on tid " << tid << " " << request.dispatch_cond << dendl;
  
  // clean up.
  mds_requests.erase(tid);


  // insert trace
  utime_t from = request.sent_stamp;
  if (pfrom) 
    *pfrom = from;
  Inode *target = insert_trace(reply, from, mds);
  if (ptarget)
    *ptarget = target;

  // -- log times --
  if (client_logger) {
    utime_t lat = g_clock.real_now();
    lat -= start;
    dout(20) << "lat " << lat << dendl;
    client_logger->favg(l_c_lat,(double)lat);
    client_logger->favg(l_c_reply,(double)lat);
  }

  return reply;
}


void Client::handle_client_session(MClientSession *m) 
{
  dout(10) << "handle_client_session " << *m << dendl;
  int from = m->get_source().num();

  switch (m->op) {
  case CEPH_SESSION_OPEN:
    mds_sessions[from].seq = 0;
    break;

  case CEPH_SESSION_CLOSE:
    mds_sessions.erase(from);
    // FIXME: kick requests (hard) so that they are redirected.  or fail.
    break;

  case CEPH_SESSION_RENEWCAPS:
    mds_sessions[from].cap_ttl = 
      mds_sessions[from].last_cap_renew_request + mdsmap->get_session_timeout();
    break;

  case CEPH_SESSION_STALE:
    // FIXME
    break;

  default:
    assert(0);
  }

  // kick waiting threads
  signal_cond_list(waiting_for_session[from]);
  waiting_for_session.erase(from);

  delete m;
}


void Client::send_request(MetaRequest *request, int mds)
{
  MClientRequest *r = request->request;
  if (!r) {
    // make a new one
    dout(10) << "send_request rebuilding request " << request->tid
	     << " for mds" << mds << dendl;
    r = new MClientRequest;
    r->copy_payload(request->request_payload);
    r->decode_payload();
    r->set_retry_attempt(request->retry_attempt);
    r->set_num_dentries_wanted(1);
  }
  request->request = 0;

  r->set_mdsmap_epoch(mdsmap->get_epoch());

  if (request->mds.empty()) {
    request->sent_stamp = g_clock.now();
    dout(20) << "send_request set sent_stamp to " << request->sent_stamp << dendl;
  }

  dout(10) << "send_request " << *r << " to mds" << mds << dendl;
  messenger->send_message(r, mdsmap->get_inst(mds));
  
  request->mds.insert(mds);
}

void Client::handle_client_request_forward(MClientRequestForward *fwd)
{
  tid_t tid = fwd->get_tid();

  if (mds_requests.count(tid) == 0) {
    dout(10) << "handle_client_request_forward no pending request on tid " << tid << dendl;
    delete fwd;
    return;
  }

  MetaRequest *request = mds_requests[tid];
  assert(request);

  // reset retry counter
  request->retry_attempt = 0;

  if (!fwd->must_resend() && 
      mds_sessions.count(fwd->get_dest_mds())) {
    // dest mds has a session, and request was forwarded for us.

    // note new mds set.
    if (request->num_fwd < fwd->get_num_fwd()) {
      // there are now exactly two mds's whose failure should trigger a resend
      // of this request.
      request->mds.clear();
      request->mds.insert(fwd->get_source().num());
      request->mds.insert(fwd->get_dest_mds());
      request->num_fwd = fwd->get_num_fwd();
      dout(10) << "handle_client_request tid " << tid
	       << " fwd " << fwd->get_num_fwd() 
	       << " to mds" << fwd->get_dest_mds() 
	       << ", mds set now " << request->mds
	       << dendl;
    } else {
      dout(10) << "handle_client_request tid " << tid
	       << " previously forwarded to mds" << fwd->get_dest_mds() 
	       << ", mds still " << request->mds
		 << dendl;
    }
  } else {
    // request not forwarded, or dest mds has no session.
    // resend.
    dout(10) << "handle_client_request tid " << tid
	     << " fwd " << fwd->get_num_fwd() 
	     << " to mds" << fwd->get_dest_mds() 
	     << ", non-idempotent, resending to " << fwd->get_dest_mds()
	     << dendl;

    request->mds.clear();
    request->num_fwd = fwd->get_num_fwd();
    request->resend_mds = fwd->get_dest_mds();
    request->caller_cond->Signal();
  }

  delete fwd;
}

void Client::handle_client_reply(MClientReply *reply)
{
  tid_t tid = reply->get_tid();

  if (mds_requests.count(tid) == 0) {
    dout(10) << "handle_client_reply no pending request on tid " << tid << dendl;
    delete reply;
    return;
  }
  MetaRequest *request = mds_requests[tid];
  assert(request);

  // store reply
  request->reply = reply;

  // wake up waiter
  request->caller_cond->Signal();

  // wake for kick back
  Cond cond;
  request->dispatch_cond = &cond;
  while (mds_requests.count(tid)) {
    dout(20) << "handle_client_reply awaiting kickback on tid " << tid << " " << &cond << dendl;
    cond.Wait(client_lock);
  }
}


// ------------------------
// incoming messages

bool Client::dispatch_impl(Message *m)
{
  client_lock.Lock();

  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    handle_mon_map((MMonMap*)m);
    break;
  case CEPH_MSG_CLIENT_MOUNT_ACK:
    handle_mount_ack((MClientMountAck*)m);
    break;

    // osd
  case CEPH_MSG_OSD_OPREPLY:
    objecter->handle_osd_op_reply((MOSDOpReply*)m);
    break;

  case CEPH_MSG_OSD_MAP:
    objecter->handle_osd_map((class MOSDMap*)m);
    if (!mounted) mount_cond.Signal();
    break;
    
    // mounting and mds sessions
  case CEPH_MSG_MDS_MAP:
    handle_mds_map((MMDSMap*)m);
    break;
  case CEPH_MSG_CLIENT_UNMOUNT:
    handle_unmount(m);
    break;
  case CEPH_MSG_CLIENT_SESSION:
    handle_client_session((MClientSession*)m);
    break;

    // requests
  case CEPH_MSG_CLIENT_REQUEST_FORWARD:
    handle_client_request_forward((MClientRequestForward*)m);
    break;
  case CEPH_MSG_CLIENT_REPLY:
    handle_client_reply((MClientReply*)m);
    break;

  case CEPH_MSG_CLIENT_SNAP:
    handle_snap((MClientSnap*)m);
    break;
  case CEPH_MSG_CLIENT_CAPS:
    handle_caps((MClientCaps*)m);
    break;
  case CEPH_MSG_CLIENT_LEASE:
    handle_lease((MClientLease*)m);
    break;

  case CEPH_MSG_STATFS_REPLY:
    handle_statfs_reply((MStatfsReply*)m);
    break;

  default:
    return false;
  }

  // unmounting?
  if (unmounting) {
    dout(10) << "unmounting: trim pass, size was " << lru.lru_get_size() 
             << "+" << inode_map.size() << dendl;
    trim_cache();
    if (lru.lru_get_size() == 0 && inode_map.empty()) {
      dout(10) << "unmounting: trim pass, cache now empty, waking unmount()" << dendl;
      mount_cond.Signal();
    } else {
      dout(10) << "unmounting: trim pass, size still " << lru.lru_get_size() 
               << "+" << inode_map.size() << dendl;
      dump_cache();      
    }
  }

  client_lock.Unlock();

  return true;
}


void Client::handle_mds_map(MMDSMap* m)
{
  int frommds = -1;
  if (m->get_source().is_mds())
    frommds = m->get_source().num();

  if (mdsmap == 0) {
    mdsmap = new MDSMap;

    assert(m->get_source().is_mon());
    whoami = m->get_dest().num();
    messenger->reset_myname(entity_name_t::CLIENT(whoami));
    dout(1) << "handle_mds_map i am now " << m->get_dest() << dendl;
    
    mount_cond.Signal();  // mount might be waiting for this.
  } 

  if (m->get_epoch() < mdsmap->get_epoch()) {
    dout(1) << "handle_mds_map epoch " << m->get_epoch() << " is older than our "
	    << mdsmap->get_epoch() << dendl;
    delete m;
    return;
  }  

  dout(1) << "handle_mds_map epoch " << m->get_epoch() << dendl;
  mdsmap->decode(m->get_encoded());
  
  // send reconnect?
  if (frommds >= 0 && 
      mdsmap->get_state(frommds) == MDSMap::STATE_RECONNECT) {
    send_reconnect(frommds);
  }

  // kick requests?
  if (frommds >= 0 &&
      mdsmap->get_state(frommds) == MDSMap::STATE_ACTIVE) {
    kick_requests(frommds);
    //failed_mds.erase(from);
  }

  // kick any waiting threads
  list<Cond*> ls;
  ls.swap(waiting_for_mdsmap);
  signal_cond_list(ls);

  delete m;
}

void Client::send_reconnect(int mds)
{
  dout(10) << "send_reconnect to mds" << mds << dendl;

  MClientReconnect *m = new MClientReconnect;

  if (mds_sessions.count(mds)) {
    // i have an open session.
    hash_set<inodeno_t> did_snaprealm;
    for (hash_map<vinodeno_t, Inode*>::iterator p = inode_map.begin();
	 p != inode_map.end();
	 p++) {
      Inode *in = p->second;
      if (in->caps.count(mds)) {
	dout(10) << " caps on " << p->first
		 << " " << ccap_string(in->caps[mds]->issued)
		 << " wants " << ccap_string(in->caps_wanted())
		 << dendl;
	filepath path;
	in->make_path(path);
	dout(10) << "    path " << path << dendl;

	in->caps[mds]->seq = 0;  // reset seq.
	m->add_cap(p->first.ino, path.get_ino(), path.get_path(),   // ino
		   in->caps_wanted(), // wanted
		   in->caps[mds]->issued,     // issued
		   in->inode.size, in->inode.mtime, in->inode.atime, in->snaprealm->ino);

	if (did_snaprealm.count(in->snaprealm->ino) == 0) {
	  dout(10) << " snaprealm " << *in->snaprealm << dendl;
	  m->add_snaprealm(in->snaprealm->ino, in->snaprealm->seq, in->snaprealm->parent);
	  did_snaprealm.insert(in->snaprealm->ino);
	}	
      }
      if (in->exporting_mds == mds) {
	dout(10) << " clearing exporting_caps on " << p->first << dendl;
	in->exporting_mds = -1;
	in->exporting_issued = 0;
	in->exporting_mseq = 0;
      }
    }


    // reset my cap seq number
    mds_sessions[mds].seq = 0;
  } else {
    dout(10) << " i had no session with this mds" << dendl;
    m->closed = true;
  }

  messenger->send_message(m, mdsmap->get_inst(mds));
}


void Client::kick_requests(int mds)
{
  dout(10) << "kick_requests for mds" << mds << dendl;

  for (map<tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end();
       ++p) 
    if (p->second->mds.count(mds)) {
      p->second->retry_attempt++;   // inc retry counter
      send_request(p->second, mds);
    }
}



/************
 * leases
 */

void Client::handle_lease(MClientLease *m)
{
  dout(10) << "handle_lease " << *m << dendl;

  assert(m->get_action() == CEPH_MDS_LEASE_REVOKE);

  int mds = m->get_source().num();
  mds_sessions[mds].seq++;

  ceph_seq_t seq = m->get_seq();

  Inode *in;
  vinodeno_t vino(m->get_ino(), CEPH_NOSNAP);
  if (inode_map.count(vino) == 0) {
    dout(10) << " don't have vino " << vino << dendl;
    goto revoke;
  }
  in = inode_map[vino];

  if (m->get_mask() & CEPH_LOCK_DN) {
    if (!in->dir || in->dir->dentries.count(m->dname) == 0) {
      dout(10) << " don't have dir|dentry " << m->get_ino() << "/" << m->dname <<dendl;
      goto revoke;
    }
    Dentry *dn = in->dir->dentries[m->dname];
    dout(10) << " revoked DN lease on " << dn << dendl;
    if (dn->lease_mds == mds)
      seq = dn->lease_seq;
    dn->lease_mds = -1;
  } 
  
 revoke:
  messenger->send_message(new MClientLease(CEPH_MDS_LEASE_RELEASE, seq,
					   m->get_mask(), m->get_ino(), m->get_first(), m->get_last(), m->dname),
			  m->get_source_inst());
  delete m;
}


void Client::release_lease(Inode *in, Dentry *dn, int mask)
{
  utime_t now = g_clock.now();

  assert(dn);

  // dentry?
  if (dn->lease_mds >= 0 && now < dn->lease_ttl && mdsmap->is_up(dn->lease_mds)) {
    dout(10) << "release_lease mds" << dn->lease_mds << " mask " << mask
	     << " on " << in->ino() << " " << dn->name << dendl;
    messenger->send_message(new MClientLease(CEPH_MDS_LEASE_RELEASE, dn->lease_seq, 
					     CEPH_LOCK_DN,
					     in->ino(), in->snapid, in->snapid, dn->name),
			    mdsmap->get_inst(dn->lease_mds));
  }
}




void Client::put_inode(Inode *in, int n)
{
  //cout << "put_inode on " << in << " " << in->inode.ino << endl;
  in->put(n);
  if (in->ref == 0) {
    //cout << "put_inode deleting " << in << " " << in->inode.ino << std::endl;
    objectcacher->release_set(in->ino());
    if (in->snapdir_parent)
      put_inode(in->snapdir_parent);
    inode_map.erase(in->vino());
    in->cap_item.remove_myself();
    in->snaprealm_item.remove_myself();
    if (in == root) root = 0;
    delete in;
  }
}


/****
 * caps
 */



void Inode::get_cap_ref(int cap)
{
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      cap_refs[c]++;
      //cout << "inode " << *this << " get " << cap_string(c) << " " << (cap_refs[c]-1) << " -> " << cap_refs[c] << std::endl;
    }
    cap >>= 1;
    n++;
  }
}

bool Inode::put_cap_ref(int cap)
{
  bool last = false;
  int n = 0;
  while (cap) {
    if (cap & 1) {
      int c = 1 << n;
      if (--cap_refs[c] == 0)
	last = true;      
      //cout << "inode " << *this << " put " << cap_string(c) << " " << (cap_refs[c]+1) << " -> " << cap_refs[c] << std::endl;
    }
    cap >>= 1;
    n++;
  }
  return last;
}

void Client::put_cap_ref(Inode *in, int cap)
{
  if (in->put_cap_ref(cap) && in->snapid == CEPH_NOSNAP) {
    if ((cap & CEPH_CAP_FILE_WR) &&
	in->cap_snaps.size() &&
	in->cap_snaps.rbegin()->second.writing) {
      dout(10) << "put_cap_ref finishing pending cap_snap on " << *in << dendl;
      in->cap_snaps.rbegin()->second.writing = 0;
      finish_cap_snap(in, &in->cap_snaps.rbegin()->second, in->caps_used());
      signal_cond_list(in->waitfor_caps);  // wake up blocked sync writers
    }
    if (cap & CEPH_CAP_FILE_WRBUFFER) {
      for (map<snapid_t,CapSnap>::iterator p = in->cap_snaps.begin();
	   p != in->cap_snaps.end();
	   p++)
	p->second.dirty_data = 0;
      check_caps(in, false);
      signal_cond_list(in->waitfor_commit);
    }
  }
}

void Client::cap_delay_requeue(Inode *in)
{
  dout(10) << "cap_delay_requeue on " << *in << dendl;
  in->hold_caps_until = g_clock.now();
  in->hold_caps_until += 5.0;

  delayed_caps.push_back(&in->cap_item);
}

void Client::send_cap(Inode *in, int mds, InodeCap *cap, int used, int want, int retain)
{
  int held = cap->issued | cap->implemented;
  int revoking = cap->implemented & ~cap->issued;
  int dropping = cap->issued & ~retain;
  int op = (retain == 0) ? CEPH_CAP_OP_RELEASE : CEPH_CAP_OP_UPDATE;

  dout(10) << "send_cap " << *in
	   << " mds" << mds << " seq " << cap->seq
	   << " used " << ccap_string(used)
	   << " want " << ccap_string(want)
	   << " retain " << ccap_string(retain)
	   << " held "<< ccap_string(held)
	   << " revoking " << ccap_string(revoking)
	   << " dropping " << ccap_string(dropping)
	   << dendl;


  int dirty = in->caps_dirty();
  cap->flushing |= dirty & held;
  if (cap->flushing) {
    dout(10) << " flushing " << ccap_string(cap->flushing) << dendl;
    in->dirty_caps &= ~cap->flushing;
  }

  cap->issued &= retain;

  if (revoking && (revoking & used) == 0) {
    cap->implemented = cap->issued;
  }
  
  MClientCaps *m = new MClientCaps(op,
				   in->ino(),
				   0,
				   cap->cap_id, cap->seq,
				   cap->issued,
				   want,
				   cap->flushing,
				   cap->mseq);
    
  m->head.uid = in->inode.uid;
  m->head.gid = in->inode.gid;
  m->head.mode = in->inode.mode;
  
  m->head.nlink = in->inode.nlink;
  
  m->head.xattr_len = 0; // FIXME
  
  m->head.layout = in->inode.layout;
  m->head.size = in->inode.size;
  m->head.max_size = in->inode.max_size;
  m->head.truncate_seq = in->inode.truncate_seq;
  m->head.truncate_size = in->inode.truncate_size;
  in->inode.mtime.encode_timeval(&m->head.mtime);
  in->inode.atime.encode_timeval(&m->head.atime);
  in->inode.ctime.encode_timeval(&m->head.ctime);
  m->head.time_warp_seq = in->inode.time_warp_seq;
  
  
  in->reported_size = in->inode.size;
  m->set_max_size(in->wanted_max_size);
  in->requested_max_size = in->wanted_max_size;
  m->set_snap_follows(in->snaprealm->get_snap_context().seq);
  messenger->send_message(m, mdsmap->get_inst(mds));
  
  if (cap->flushing == 0 && cap->issued == 0)
    remove_cap(in, mds);
}


void Client::check_caps(Inode *in, bool is_delayed)
{
  unsigned wanted = in->caps_wanted();
  unsigned used = in->caps_used();

  int retain = wanted;
  if (!unmounting) {
    retain |= CEPH_CAP_PIN | CEPH_CAP_EXPIREABLE;
    
    /* keep any EXCL bits too, while we are holding caps anyway */
    if (wanted || in->inode.is_dir())
      retain |= CEPH_CAP_ANY_EXCL;
  }
  
  dout(10) << "check_caps on " << *in
	   << " wanted " << ccap_string(wanted)
	   << " used " << ccap_string(used)
	   << " is_delayed=" << is_delayed
	   << dendl;

  assert(in->snapid == CEPH_NOSNAP);
  
  if (in->caps.empty())
    return;   // guard if at end of func

  if (in->cap_snaps.size())
    flush_snaps(in);
  
  if (!is_delayed)
    cap_delay_requeue(in);
  else
    in->hold_caps_until = utime_t();

  utime_t now = g_clock.now();

  map<int,InodeCap*>::iterator it = in->caps.begin();
  while (it != in->caps.end()) {
    int mds = it->first;
    InodeCap *cap = it->second;
    it++;

    int revoking = cap->implemented & ~cap->issued;
    
    dout(10) << " cap mds" << mds
	     << " issued " << ccap_string(cap->issued)
	     << " implemented " << ccap_string(cap->implemented)
	     << " revoking " << ccap_string(revoking) << dendl;

    if (in->wanted_max_size > in->inode.max_size &&
	in->wanted_max_size > in->requested_max_size)
      goto ack;

    /* approaching file_max? */
    if ((cap->issued & CEPH_CAP_FILE_WR) &&
	(in->inode.size << 1) >= in->inode.max_size &&
	(in->reported_size << 1) < in->inode.max_size) {
      dout(10) << "size approaching max_size" << dendl;
      goto ack;
    }

    /* completed revocation? */
    if (revoking && (revoking && used) == 0) {
      dout(10) << "completed revocation of " << ccap_string(cap->implemented & ~cap->issued) << dendl;
      goto ack;
    }

    if (!revoking && unmounting && (used == 0))
      goto ack;

    if (wanted == cap->wanted &&         // mds knows what we want.
	(cap->issued & ~retain) == 0)    // and we don't have anything we wouldn't like
      continue;   

    if (now < in->hold_caps_until) {
      dout(10) << "delaying cap release" << dendl;
      continue;
    }
    
  ack:
    send_cap(in, mds, cap, used, wanted, retain);
  }
}

struct C_SnapFlush : public Context {
  Client *client;
  Inode *in;
  snapid_t seq;
  C_SnapFlush(Client *c, Inode *i, snapid_t s) : client(c), in(i), seq(s) {}
  void finish(int r) {
    client->_flushed_cap_snap(in, seq);
  }
};

void Client::queue_cap_snap(Inode *in, snapid_t seq)
{
  int used = in->caps_used();
  dout(10) << "queue_cap_snap " << *in << " seq " << seq << " used " << ccap_string(used) << dendl;

  if (in->cap_snaps.size() &&
      in->cap_snaps.rbegin()->second.writing) {
    dout(10) << "queue_cap_snap already have pending cap_snap on " << *in << dendl;
    return;
  }

  in->get();
  CapSnap *capsnap = &in->cap_snaps[seq];
  capsnap->context = in->snaprealm->cached_snap_context;
  capsnap->issued = in->caps_issued();
  capsnap->dirty = in->caps_dirty();  // a bit conservative?
  
  capsnap->dirty_data = (used & CEPH_CAP_FILE_WRBUFFER);

  if (used & CEPH_CAP_FILE_WR) {
    dout(10) << "queue_cap_snap WR used on " << *in << dendl;
    capsnap->writing = 1;
  } else {
    finish_cap_snap(in, capsnap, used);
  }
}

void Client::finish_cap_snap(Inode *in, CapSnap *capsnap, int used)
{
  dout(10) << "finish_cap_snap " << *in << " capsnap " << (void*)capsnap << " used " << ccap_string(used) << dendl;
  capsnap->size = in->inode.size;
  capsnap->mtime = in->inode.mtime;
  capsnap->atime = in->inode.atime;
  capsnap->ctime = in->inode.ctime;
  capsnap->time_warp_seq = in->inode.time_warp_seq;
  if (used & CEPH_CAP_FILE_WRBUFFER) {
    dout(10) << "finish_cap_snap " << *in << " cap_snap " << capsnap << " used " << used
	     << " WRBUFFER, delaying" << dendl;
  } else {
    capsnap->dirty_data = 0;
    flush_snaps(in);
  }
}

void Client::_flushed_cap_snap(Inode *in, snapid_t seq)
{
  dout(10) << "_flushed_cap_snap seq " << seq << " on " << *in << dendl;
  assert(in->cap_snaps.count(seq));
  in->cap_snaps[seq].dirty_data = 0;
  flush_snaps(in);
}

void Client::flush_snaps(Inode *in)
{
  dout(10) << "flush_snaps on " << *in << dendl;
  assert(in->cap_snaps.size());

  // pick auth mds
  int mds = -1;
  int mseq = 0;
  for (map<int,InodeCap*>::iterator p = in->caps.begin(); p != in->caps.end(); p++) {
    if (p->second->issued & CEPH_CAP_ANY_WR) {
      mds = p->first;
      mseq = p->second->mseq;
      break;
    }
  }
  assert(mds >= 0);

  for (map<snapid_t,CapSnap>::iterator p = in->cap_snaps.begin(); p != in->cap_snaps.end(); p++) {
    dout(10) << "flush_snaps mds" << mds
	     << " follows " << p->first
	     << " size " << p->second.size
	     << " mtime " << p->second.mtime
	     << " dirty_data=" << p->second.dirty_data
	     << " writing=" << p->second.writing
	     << " on " << *in << dendl;
    if (p->second.dirty_data || p->second.writing)
      continue;
    MClientCaps *m = new MClientCaps(CEPH_CAP_OP_FLUSHSNAP, in->ino(), in->snaprealm->ino, 0, mseq);
    m->head.snap_follows = p->first;
    m->head.size = p->second.size;
    m->head.caps = p->second.issued;
    m->head.dirty = p->second.dirty;
    p->second.ctime.encode_timeval(&m->head.ctime);
    p->second.mtime.encode_timeval(&m->head.mtime);
    p->second.atime.encode_timeval(&m->head.atime);
    messenger->send_message(m, mdsmap->get_inst(mds));
  }
}



void Client::wait_on_list(list<Cond*>& ls)
{
  Cond cond;
  ls.push_back(&cond);
  cond.Wait(client_lock);
}

void Client::signal_cond_list(list<Cond*>& ls)
{
  for (list<Cond*>::iterator it = ls.begin(); it != ls.end(); it++)
    (*it)->Signal();
  ls.clear();
}



// flush dirty data (from objectcache)

void Client::_release(Inode *in, bool checkafter)
{
  if (in->cap_refs[CEPH_CAP_FILE_RDCACHE]) {
    objectcacher->release_set(in->inode.ino);
    if (checkafter)
      put_cap_ref(in, CEPH_CAP_FILE_RDCACHE);
    else 
      in->put_cap_ref(CEPH_CAP_FILE_RDCACHE);
  }
}


void Client::_flush(Inode *in, Context *onfinish)
{
  dout(10) << "_flush " << *in << dendl;
  if (!onfinish)
    onfinish = new C_NoopContext;
  bool safe = objectcacher->commit_set(in->inode.ino, onfinish);
  if (safe && onfinish) {
    onfinish->finish(0);
    delete onfinish;
  }
}

void Client::flush_set_callback(inodeno_t ino)
{
  //  Mutex::Locker l(client_lock);
  assert(client_lock.is_locked());   // will be called via dispatch() -> objecter -> ...
  Inode *in = inode_map[vinodeno_t(ino,CEPH_NOSNAP)];
  assert(in);
  _flushed(in);
}

void Client::_flushed(Inode *in)
{
  dout(10) << "_flushed " << *in << dendl;
  assert(in->cap_refs[CEPH_CAP_FILE_WRBUFFER] == 1);

  // release clean pages too, if we dont hold RDCACHE reference
  if (in->cap_refs[CEPH_CAP_FILE_RDCACHE] == 0)
    objectcacher->release_set(in->inode.ino);

  put_cap_ref(in, CEPH_CAP_FILE_WRBUFFER);
}



/** handle_file_caps
 * handle caps update from mds.  including mds to mds caps transitions.
 * do not block.
 */
void Client::add_update_cap(Inode *in, int mds, __u64 cap_id,
			    unsigned issued, unsigned seq, unsigned mseq, inodeno_t realm)
{
  InodeCap *cap = 0;
  if (in->caps.count(mds)) {
    cap = in->caps[mds];
  } else {
    mds_sessions[mds].num_caps++;
    if (in->caps.empty()) {
      assert(in->snaprealm == 0);
      in->snaprealm = get_snap_realm(realm);
      in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
      in->get();
      dout(15) << "add_update_cap first one, opened snaprealm " << in->snaprealm << dendl;
    }
    if (in->exporting_mds == mds) {
      dout(10) << "add_update_cap clearing exporting_caps on " << mds << dendl;
      in->exporting_mds = -1;
      in->exporting_issued = 0;
      in->exporting_mseq = 0;
    }
    in->caps[mds] = cap = new InodeCap;
    cap_list.push_back(&in->cap_item);
  }

  unsigned old_caps = cap->issued;
  cap->cap_id = cap_id;
  cap->issued |= issued;
  cap->implemented |= issued;
  cap->seq = seq;
  cap->mseq = mseq;
  dout(10) << "add_update_cap issued " << ccap_string(old_caps) << " -> " << ccap_string(cap->issued)
	   << " from mds" << mds
	   << " on " << *in
	   << dendl;

  if (issued & ~old_caps)
    signal_cond_list(in->waitfor_caps);
}

void Client::remove_cap(Inode *in, int mds)
{
  dout(10) << "remove_cap mds" << mds << " on " << *in << dendl;
  assert(in->caps.count(mds));
  in->caps.erase(mds);
  if (!in->is_any_caps()) {
    dout(15) << "remove_cap last one, closing snaprealm " << in->snaprealm << dendl;
    put_snap_realm(in->snaprealm);
    in->snaprealm = 0;
    in->snaprealm_item.remove_myself();
    put_inode(in);
  }
}

void Client::remove_all_caps(Inode *in)
{
  bool wasempty = in->caps.empty();
  in->caps.clear();
  if (!wasempty) {
    dout(15) << "remove_all_caps closing snaprealm " << in->snaprealm << dendl;
    put_snap_realm(in->snaprealm);
    in->snaprealm = 0;
    in->snaprealm_item.remove_myself();
    put_inode(in);
  }
}

void SnapRealm::build_snap_context()
{
  set<snapid_t> snaps;
  snapid_t max_seq = seq;
  
  // start with prior_parents?
  for (unsigned i=0; i<prior_parent_snaps.size(); i++)
    snaps.insert(prior_parent_snaps[i]);

  // current parent's snaps
  if (pparent) {
    const SnapContext& psnapc = pparent->get_snap_context();
    for (unsigned i=0; i<psnapc.snaps.size(); i++)
      if (psnapc.snaps[i] >= parent_since)
	snaps.insert(psnapc.snaps[i]);
    if (psnapc.seq > max_seq)
      max_seq = psnapc.seq;
  }

  // my snaps
  for (unsigned i=0; i<my_snaps.size(); i++)
    snaps.insert(my_snaps[i]);

  // ok!
  cached_snap_context.seq = max_seq;
  cached_snap_context.snaps.resize(0);
  cached_snap_context.snaps.reserve(snaps.size());
  for (set<snapid_t>::reverse_iterator p = snaps.rbegin(); p != snaps.rend(); p++)
    cached_snap_context.snaps.push_back(*p);

  cout << *this << " build_snap_context got " << cached_snap_context << std::endl;
}

void Client::invalidate_snaprealm_and_children(SnapRealm *realm)
{
  list<SnapRealm*> q;
  q.push_back(realm);

  while (!q.empty()) {
    realm = q.front();
    q.pop_front();

    dout(10) << "invalidate_snaprealm_and_children " << *realm << dendl;
    realm->invalidate_cache();

    for (set<SnapRealm*>::iterator p = realm->pchildren.begin();
	 p != realm->pchildren.end(); 
	 p++)
      q.push_back(*p);
  }
}

bool Client::adjust_realm_parent(SnapRealm *realm, inodeno_t parent)
{
  if (realm->parent != parent) {
    dout(10) << "adjust_realm_parent " << *realm
	     << " " << realm->parent << " -> " << parent << dendl;
    realm->parent = parent;
    if (realm->pparent) {
      realm->pparent->pchildren.erase(realm);
      put_snap_realm(realm->pparent);
    }
    realm->pparent = get_snap_realm(parent);
    realm->pparent->pchildren.insert(realm);
    return true;
  }
  return false;
}

inodeno_t Client::update_snap_trace(bufferlist& bl, bool flush)
{
  inodeno_t first_realm = 0;
  dout(10) << "update_snap_trace len " << bl.length() << dendl;

  bufferlist::iterator p = bl.begin();
  while (!p.end()) {
    SnapRealmInfo info;
    ::decode(info, p);
    if (first_realm == 0)
      first_realm = info.ino();
    SnapRealm *realm = get_snap_realm(info.ino());

    if (info.seq() > realm->seq) {
      dout(10) << "update_snap_trace " << *realm << " seq " << info.seq() << " > " << realm->seq
	       << dendl;

      if (flush) {
	// writeback any dirty caps _before_ updating snap list (i.e. with old snap info)
	//  flush me + children
	list<SnapRealm*> q;
	q.push_back(realm);
	while (!q.empty()) {
	  SnapRealm *realm = q.front();
	  q.pop_front();
	  dout(10) << " flushing caps on " << *realm << dendl;
	  
	  xlist<Inode*>::iterator p = realm->inodes_with_caps.begin();
	  while (!p.end()) {
	    Inode *in = *p;
	    ++p;
	    queue_cap_snap(in, realm->cached_snap_context.seq);
	  }
	  
	  for (set<SnapRealm*>::iterator p = realm->pchildren.begin(); 
	       p != realm->pchildren.end(); 
	       p++)
	    q.push_back(*p);
	}
      }

    }

    // _always_ verify parent
    bool invalidate = adjust_realm_parent(realm, info.parent());

    if (info.seq() > realm->seq) {
      // update
      realm->seq = info.seq();
      realm->created = info.created();
      realm->parent_since = info.parent_since();
      realm->prior_parent_snaps = info.prior_parent_snaps;
      realm->my_snaps = info.my_snaps;
      invalidate = true;
    }
    if (invalidate) {
      invalidate_snaprealm_and_children(realm);
      dout(15) << "update_snap_trace " << *realm << " self|parent updated" << dendl;
      dout(15) << "  snapc " << realm->get_snap_context() << dendl;
    } else {
      dout(10) << "update_snap_trace " << *realm << " seq " << info.seq()
	       << " <= " << realm->seq << " and same parent, SKIPPING" << dendl;
    }
        
    put_snap_realm(realm);
  }

  return first_realm;
}

void Client::handle_snap(MClientSnap *m)
{
  dout(10) << "handle_snap " << *m << dendl;
  int mds = m->get_source().num();

  mds_sessions[mds].seq++;

  list<Inode*> to_move;
  SnapRealm *realm = 0;

  if (m->head.op == CEPH_SNAP_OP_SPLIT) {
    assert(m->head.split);
    SnapRealmInfo info;
    bufferlist::iterator p = m->bl.begin();    
    ::decode(info, p);
    assert(info.ino() == m->head.split);
    
    // flush, then move, ino's.
    realm = get_snap_realm(info.ino());
    dout(10) << " splitting off " << *realm << dendl;
    for (vector<inodeno_t>::iterator p = m->split_inos.begin();
	 p != m->split_inos.end();
	 p++) {
      vinodeno_t vino(*p, CEPH_NOSNAP);
      if (inode_map.count(vino)) {
	Inode *in = inode_map[vino];
	if (!in->snaprealm || in->snaprealm == realm)
	  continue;
	if (in->snaprealm->created > info.created()) {
	  dout(10) << " NOT moving " << *in << " from _newer_ realm " 
		   << *in->snaprealm << dendl;
	  continue;
	}
	dout(10) << " moving " << *in << " from " << *in->snaprealm << dendl;

	// queue for snap writeback
	queue_cap_snap(in, in->snaprealm->cached_snap_context.seq);

	put_snap_realm(in->snaprealm);
	in->snaprealm_item.remove_myself();
	to_move.push_back(in);
      }
    }

    // move child snaprealms, too
    for (vector<inodeno_t>::iterator p = m->split_realms.begin();
	 p != m->split_realms.end();
	 p++) {
      dout(10) << "adjusting snaprealm " << *p << " parent" << dendl;
      SnapRealm *child = get_snap_realm_maybe(*p);
      if (!child)
	continue;
      adjust_realm_parent(child, realm->ino);
      put_snap_realm(child);
    }
  }

  update_snap_trace(m->bl, m->head.op != CEPH_SNAP_OP_DESTROY);

  if (realm) {
    for (list<Inode*>::iterator p = to_move.begin(); p != to_move.end(); p++) {
      Inode *in = *p;
      in->snaprealm = realm;
      realm->inodes_with_caps.push_back(&in->snaprealm_item);
      realm->nref++;
    }
    put_snap_realm(realm);
  }

  delete m;
}

void Client::handle_caps(MClientCaps *m)
{
  int mds = m->get_source().num();

  m->clear_payload();  // for if/when we send back to MDS

  // note push seq increment
  if (mds_sessions.count(mds) == 0) 
    dout(0) << "got file_caps without session from mds" << mds << " msg " << *m << dendl;
  //assert(mds_sessions.count(mds));   // HACK FIXME SOON
  mds_sessions[mds].seq++;

  Inode *in = 0;
  vinodeno_t vino(m->get_ino(), CEPH_NOSNAP);
  if (inode_map.count(vino)) in = inode_map[vino];
  if (!in) {
    dout(5) << "handle_caps don't have vino " << vino << dendl;

    // release.
    m->set_op(CEPH_CAP_OP_RELEASE);
    m->head.caps = 0;
    m->head.dirty = 0;
    messenger->send_message(m, m->get_source_inst());
    return;
  }

  switch (m->get_op()) {
  case CEPH_CAP_OP_IMPORT: return handle_cap_import(in, m);
  case CEPH_CAP_OP_EXPORT: return handle_cap_export(in, m);
  case CEPH_CAP_OP_FLUSHSNAP_ACK: return handle_cap_flushsnap_ack(in, m);
  }

  if (in->caps.count(mds) == 0) {
    m->set_op(CEPH_CAP_OP_RELEASE);
    m->head.caps = 0;
    m->head.dirty = 0;
    messenger->send_message(m, m->get_source_inst());
    return;
  }
  InodeCap *cap = in->caps[mds];

  switch (m->get_op()) {
  case CEPH_CAP_OP_TRUNC: return handle_cap_trunc(in, m);
  case CEPH_CAP_OP_GRANT: return handle_cap_grant(in, mds, cap, m);
  case CEPH_CAP_OP_FLUSH_ACK: return handle_cap_flush_ack(in, mds, cap, m);
  default:
    delete m;
  }
}

void Client::handle_cap_import(Inode *in, MClientCaps *m)
{
  int mds = m->get_source().num();

  // add/update it
  update_snap_trace(m->snapbl);
  add_update_cap(in, mds, m->get_cap_id(),
		 m->get_caps(), m->get_seq(), m->get_mseq(), m->get_realm());
  
  if (m->get_mseq() > in->exporting_mseq) {
    dout(5) << "handle_cap_import ino " << m->get_ino() << " mseq " << m->get_mseq()
	    << " IMPORT from mds" << mds
	    << ", clearing exporting_issued " << ccap_string(in->exporting_issued) 
	    << " mseq " << in->exporting_mseq << dendl;
    in->exporting_issued = 0;
    in->exporting_mseq = 0;
    in->exporting_mds = -1;
  } else {
    dout(5) << "handle_cap_import ino " << m->get_ino() << " mseq " << m->get_mseq()
	    << " IMPORT from mds" << mds 
	    << ", keeping exporting_issued " << ccap_string(in->exporting_issued) 
	    << " mseq " << in->exporting_mseq << " by mds" << in->exporting_mds << dendl;
  }
  delete m;
}

void Client::handle_cap_export(Inode *in, MClientCaps *m)
{
  int mds = m->get_source().num();
  assert(in->caps[mds]);
  InodeCap *cap = in->caps[mds];

  // note?
  bool found_higher_mseq = false;
  for (map<int,InodeCap*>::iterator p = in->caps.begin();
       p != in->caps.end();
       p++) {
    if (p->second->mseq > m->get_mseq()) {
      found_higher_mseq = true;
      dout(5) << "handle_cap_export ino " << m->get_ino() << " mseq " << m->get_mseq() 
	      << " EXPORT from mds" << mds
	      << ", but mds" << p->first << " has higher mseq " << p->second->mseq << dendl;
      break;
    }
  }

  if (!found_higher_mseq) {
    dout(5) << "handle_cap_export ino " << m->get_ino() << " mseq " << m->get_mseq() 
	    << " EXPORT from mds" << mds
	    << ", setting exporting_issued " << ccap_string(cap->issued) << dendl;
    in->exporting_issued = cap->issued;
    in->exporting_mseq = m->get_mseq();
    in->exporting_mds = mds;
  } else 
    dout(5) << "handle_cap_export ino " << m->get_ino() << " mseq " << m->get_mseq() 
	    << " EXPORT from mds" << mds
	    << ", just removing old cap" << dendl;

  remove_cap(in, mds);

  delete m;
}

void Client::handle_cap_trunc(Inode *in, MClientCaps *m)
{
  int mds = m->get_source().num();
  assert(in->caps[mds]);

  dout(10) << "handle_cap_trunc on ino " << *in
	   << " size " << in->inode.size << " -> " << m->get_size()
	   << dendl;
  // trim filecache?
  if (g_conf.client_oc &&
      m->get_size() < in->inode.size) {
    // map range to objects
    vector<ObjectExtent> ls;
    filer->file_to_extents(in->inode.ino, &in->inode.layout, CEPH_NOSNAP,
			   m->get_size(), in->inode.size - m->get_size(),
			   ls);
    objectcacher->truncate_set(in->inode.ino, ls);
  }
  
  in->reported_size = in->inode.size = m->get_size(); 
  delete m;
}

void Client::handle_cap_flush_ack(Inode *in, int mds, InodeCap *cap, MClientCaps *m)
{
  int cleaned = m->get_dirty() & ~m->get_caps();
  dout(5) << "handle_cap_flush_ack mds" << mds
	  << " cleaned " << ccap_string(cleaned) << " on " << *in << dendl;
  cap->flushing &= ~cleaned;
  dout(5) << "  cap->flushing now " << ccap_string(cap->flushing)
	  << ", in->caps_dirty() now " << ccap_string(in->caps_dirty()) << dendl;

  if (m->get_caps() == 0 && m->get_seq() == cap->seq) {
    assert(in->caps_dirty() == 0);
    remove_cap(in, mds);
  }
  delete m;
}


void Client::handle_cap_flushsnap_ack(Inode *in, MClientCaps *m)
{
  int mds = m->get_source().num();
  assert(in->caps[mds]);
  snapid_t follows = m->get_snap_follows();

  if (in->cap_snaps.count(follows)) {
    dout(5) << "handle_cap_flushedsnap mds" << mds << " flushed snap follows " << follows
	    << " on " << *in << dendl;
    in->cap_snaps.erase(follows);
    put_inode(in);
  } else {
    dout(5) << "handle_cap_flushedsnap DUP(?) mds" << mds << " flushed snap follows " << follows
	    << " on " << *in << dendl;
    // we may not have it if we send multiple FLUSHSNAP requests and (got multiple FLUSHEDSNAPs back)
  }
    
  delete m;
}


void Client::handle_cap_grant(Inode *in, int mds, InodeCap *cap, MClientCaps *m)
{
  int used = in->caps_used();

  const int old_caps = cap->issued;
  const int new_caps = m->get_caps();
  dout(5) << "handle_cap_grant on in " << m->get_ino() 
          << " mds" << mds << " seq " << m->get_seq() 
          << " caps now " << ccap_string(new_caps) 
          << " was " << ccap_string(old_caps) << dendl;

  cap->seq = m->get_seq();

  in->inode.layout = m->get_layout();
  
  // update inode
  int issued = in->caps_issued();
  if ((issued & CEPH_CAP_AUTH_EXCL) == 0) {
    in->inode.mode = m->head.mode;
    in->inode.uid = m->head.uid;
    in->inode.gid = m->head.gid;
  }
  if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
    in->inode.nlink = m->head.nlink;
    in->inode.anchored = false;  /* lie */
  }
  if ((issued & CEPH_CAP_XATTR_EXCL) == 0 &&
      m->xattrbl.length() &&
      m->head.xattr_version > in->inode.xattr_version) {
    bufferlist::iterator p = m->xattrbl.begin();
    ::decode(in->xattrs, p);
    in->inode.xattr_version = m->head.xattr_version;
  }
  update_inode_file_bits(in, m->get_truncate_seq(), m->get_truncate_size(), m->get_size(),
			 m->get_time_warp_seq(), m->get_ctime(), m->get_mtime(), m->get_atime(), old_caps);

  // max_size
  bool kick_writers = false;
  if (m->get_max_size() != in->inode.max_size) {
    dout(10) << "max_size " << in->inode.max_size << " -> " << m->get_max_size() << dendl;
    in->inode.max_size = m->get_max_size();
    if (in->inode.max_size > in->wanted_max_size) {
      in->wanted_max_size = 0;
      in->requested_max_size = 0;
    }
    kick_writers = true;
  }

  // update caps
  if (old_caps & ~new_caps) { 
    dout(10) << "  revocation of " << ccap_string(~new_caps & old_caps) << dendl;
    cap->issued = new_caps;

    if ((cap->issued & ~new_caps) & CEPH_CAP_FILE_RDCACHE)
      _release(in, false);

    if ((used & ~new_caps) & CEPH_CAP_FILE_WRBUFFER)
      _flush(in);
    else {
      check_caps(in, false);
    }

  } else if (old_caps == new_caps) {
    dout(10) << "  caps unchanged at " << ccap_string(old_caps) << dendl;
  } else {
    dout(10) << "  grant, new caps are " << ccap_string(new_caps & ~old_caps) << dendl;
    cap->issued = new_caps;
    cap->implemented |= new_caps;
  }

  // wake up waiters
  if (new_caps)
    signal_cond_list(in->waitfor_caps);

  delete m;
}


// -------------------
// MOUNT

void Client::_try_mount()
{
  dout(10) << "_try_mount" << dendl;
  int mon = monmap->pick_mon();
  dout(2) << "sending client_mount to mon" << mon << dendl;
  messenger->set_dispatcher(this);
  messenger->send_message(new MClientMount, monmap->get_inst(mon));

  // schedule timeout?
  assert(mount_timeout_event == 0);
  mount_timeout_event = new C_MountTimeout(this);
  timer.add_event_after(g_conf.client_mount_timeout, mount_timeout_event);
}

void Client::_mount_timeout()
{
  dout(10) << "_mount_timeout" << dendl;
  mount_timeout_event = 0;
  _try_mount();
}

int Client::mount()
{
  Mutex::Locker lock(client_lock);

  if (mounted) {
    dout(5) << "already mounted" << dendl;;
    return 0;
  }

  // only first mounter does the work
  bool itsme = false;
  if (!mounters) {
    itsme = true;
    objecter->init();
    _try_mount();
  } else {
    dout(5) << "additional mounter" << dendl;
  }
  mounters++;
  
  while (signed_ticket.length() == 0 ||
	 !mdsmap ||
	 !osdmap || 
	 osdmap->get_epoch() == 0 ||
	 (!itsme && !mounted))       // non-doers wait a little longer
    mount_cond.Wait(client_lock);
  
  if (!itsme) {
    dout(5) << "additional mounter returning" << dendl;
    assert(mounted);
    return 0; 
  }

  // finish.
  timer.cancel_event(mount_timeout_event);
  mount_timeout_event = 0;
  
  mounted = true;
  mount_cond.SignalAll(); // wake up non-doers
  
  dout(2) << "mounted: have osdmap " << osdmap->get_epoch() 
	  << " and mdsmap " << mdsmap->get_epoch() 
	  << dendl;
  
  // hack: get+pin root inode.
  //  fuse assumes it's always there.
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_GETATTR);
  filepath fp(CEPH_INO_ROOT);
  req->set_filepath(fp);
  req->head.args.stat.mask = CEPH_STAT_CAP_INODE_ALL;
  MClientReply *reply = make_request(req, -1, -1);
  int res = reply->get_result();
  dout(10) << "root getattr result=" << res << dendl;
  if (res < 0)
    return res;

  assert(root);
  _ll_get(root);

  // trace?
  if (g_conf.client_trace) {
    traceout.open(g_conf.client_trace);
    if (traceout.is_open()) {
      dout(1) << "opened trace file '" << g_conf.client_trace << "'" << dendl;
    } else {
      dout(1) << "FAILED to open trace file '" << g_conf.client_trace << "'" << dendl;
    }
  }

  /*
  dout(3) << "op: // client trace data structs" << dendl;
  dout(3) << "op: struct stat st;" << dendl;
  dout(3) << "op: struct utimbuf utim;" << dendl;
  dout(3) << "op: int readlinkbuf_len = 1000;" << dendl;
  dout(3) << "op: char readlinkbuf[readlinkbuf_len];" << dendl;
  dout(3) << "op: map<string, inode_t*> dir_contents;" << dendl;
  dout(3) << "op: map<int, int> open_files;" << dendl;
  dout(3) << "op: int fd;" << dendl;
  */
  return 0;
}

void Client::handle_mon_map(MMonMap *m)
{
  dout(10) << "handle_mon_map " << *m << dendl;
  // just ignore it for now.
  delete m;
}

void Client::handle_mount_ack(MClientMountAck* m)
{
  dout(10) << "handle_mount_ack " << *m << dendl;

  signed_ticket = m->signed_ticket;
  bufferlist::iterator p = signed_ticket.begin();
  ::decode(ticket, p);

  mount_cond.Signal();
}


// UNMOUNT


int Client::unmount()
{
  Mutex::Locker lock(client_lock);

  assert(mounted);  // caller is confused?
  assert(mounters > 0);
  if (--mounters > 0) {
    dout(0) << "umount -- still " << mounters << " others, doing nothing" << dendl;
    return -1;  // i'm not the last mounter.
  }


  dout(2) << "unmounting" << dendl;
  unmounting = true;

  if (tick_event)
    timer.cancel_event(tick_event);
  tick_event = 0;

  if (cwd)
    put_inode(cwd);
  cwd = 0;

  // NOTE: i'm assuming all caches are already flushing (because all files are closed).
  assert(fd_map.empty());

  _ll_drop_pins();

  // empty lru cache
  lru.lru_set_max(0);
  trim_cache();

  if (g_conf.client_oc) {
    // flush/release all buffered data
    hash_map<vinodeno_t, Inode*>::iterator next;
    for (hash_map<vinodeno_t, Inode*>::iterator p = inode_map.begin();
         p != inode_map.end(); 
         p = next) {
      next = p;
      next++;
      Inode *in = p->second;
      if (!in) {
	dout(0) << "null inode_map entry ino " << p->first << dendl;
	assert(in);
      }      
      if (!in->caps.empty()) {
	in->get();
	_release(in);
	_flush(in);
	put_inode(in);
      }
    }
  }

  // flush delayed caps
  xlist<Inode*>::iterator p = delayed_caps.begin();
  while (!p.end()) {
    Inode *in = *p;
    ++p;
    delayed_caps.pop_front();
    check_caps(in, true);
  }

  // other caps, too
  p = cap_list.begin();
  while (!p.end()) {
    Inode *in = *p;
    ++p;
    check_caps(in, true);
  }

  //if (0) {// hack
  while (lru.lru_get_size() > 0 || 
         !inode_map.empty()) {
    dout(2) << "cache still has " << lru.lru_get_size() 
            << "+" << inode_map.size() << " items" 
	    << ", waiting (for caps to release?)"
            << dendl;
    dump_cache();
    mount_cond.Wait(client_lock);
  }
  assert(lru.lru_get_size() == 0);
  assert(inode_map.empty());
  //}

  // unsafe writes
  if (!g_conf.client_oc) {
    while (unsafe_sync_write > 0) {
      dout(0) << unsafe_sync_write << " unsafe_sync_writes, waiting" 
              << dendl;
      mount_cond.Wait(client_lock);
    }
  }

  // stop tracing
  if (g_conf.client_trace) {
    dout(1) << "closing trace file '" << g_conf.client_trace << "'" << dendl;
    traceout.close();
  }

  
  // send session closes!
  for (map<int,MDSSession>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    dout(2) << "sending client_session close to mds" << p->first
	    << " seq " << p->second.seq << dendl;
    messenger->send_message(new MClientSession(CEPH_SESSION_REQUEST_CLOSE, p->second.seq),
			    mdsmap->get_inst(p->first));
  }

  // send unmount!
  int mon = monmap->pick_mon();
  dout(2) << "sending client_unmount to mon" << mon << dendl;
  messenger->send_message(new MClientUnmount, monmap->get_inst(mon));
  
  while (mounted)
    mount_cond.Wait(client_lock);

  dout(2) << "unmounted." << dendl;

  objecter->shutdown();

  return 0;
}

void Client::handle_unmount(Message* m)
{
  dout(1) << "handle_unmount got ack" << dendl;

  mounted = false;

  delete mdsmap;
  mdsmap = 0;

  mount_cond.Signal();

  delete m;
}


class C_C_Tick : public Context {
  Client *client;
public:
  C_C_Tick(Client *c) : client(c) {}
  void finish(int r) {
    client->tick();
  }
};

void Client::tick()
{
  dout(21) << "tick" << dendl;
  tick_event = new C_C_Tick(this);
  timer.add_event_after(g_conf.client_tick_interval, tick_event);
  
  utime_t now = g_clock.now();
  utime_t el = now - last_cap_renew;
  if (mdsmap && el > mdsmap->get_session_timeout() / 3.0)
    renew_caps();

  // delayed caps
  xlist<Inode*>::iterator p = delayed_caps.begin();
  while (!p.end()) {
    Inode *in = *p;
    ++p;
    if (in->hold_caps_until > now)
      break;
    delayed_caps.pop_front();
    cap_list.push_back(&in->cap_item);
    check_caps(in, true);
  }
}

void Client::renew_caps()
{
  dout(10) << "renew_caps" << dendl;
  last_cap_renew = g_clock.now();
  
  for (map<int,MDSSession>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       p++) {
    dout(15) << "renew_caps requesting from mds" << p->first << dendl;
    p->second.last_cap_renew_request = g_clock.now();
    messenger->send_message(new MClientSession(CEPH_SESSION_REQUEST_RENEWCAPS),
			    mdsmap->get_inst(p->first));
  }
}


// ===============================================================
// high level (POSIXy) interface

int Client::_do_lookup(Inode *dir, const char *name, Inode **target)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_LOOKUP);
  filepath path(name, dir->ino());
  req->set_filepath(path);
  req->head.args.stat.mask = 0;
  dout(10) << "_lookup on " << path << dendl;

  MClientReply *reply = make_request(req, 0, 0, 0, target);
  int r = reply->get_result();
  dout(10) << "_lookup res is " << r << dendl;
  delete reply;
  return r;
}

int Client::_lookup(Inode *dir, const string& dname, Inode **target)
{
  if (dname == "..") {
    if (!dir->dn)
      return -ENOENT;
    *target = dir->dn->dir->parent_inode;
    return 0;
  }
    
  if (!dir->inode.is_dir())
    return -ENOTDIR;

  if (dname == g_conf.client_snapdir &&
      dir->snapid == CEPH_NOSNAP) {
    *target = open_snapdir(dir);
    return 0;
  }

  if (dir->dir &&
      dir->dir->dentries.count(dname)) {
    Dentry *dn = dir->dir->dentries[dname];
    *target = dn->inode;
    return 0;
  }

  return _lookup(dir, dname.c_str(), target);
}

int Client::path_walk(const filepath& origpath, Inode **final, bool followsym)
{
  filepath path = origpath;
  Inode *cur = cwd;
  assert(cur);

  dout(10) << "path_walk " << path << dendl;

  for (unsigned i=0; i<path.depth() && cur; i++) {
    const string &dname = path[i];
    dout(10) << " " << i << " " << *cur << " " << dname << dendl;
    Inode *next;
    int r = _lookup(cur, dname.c_str(), &next);
    if (r < 0)
      return r;
    cur = next;
    if (i == path.depth() - 1 && followsym &&
	cur && cur->inode.is_symlink()) {
      // resolve symlink
      if (cur->symlink[0] == '/') {
	path = cur->symlink.c_str();
	cur = root;
      } else {
	filepath more(cur->symlink.c_str());
	path.append(more);
      }
    }
  }
  if (!cur)
    return -ENOENT;
  if (final)
    *final = cur;
  return 0;
}


// namespace ops

int Client::link(const char *relexisting, const char *relpath) 
{
  Mutex::Locker lock(client_lock);
  tout << "link" << std::endl;
  tout << relexisting << std::endl;
  tout << relpath << std::endl;

  filepath existing(relexisting);
  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();

  Inode *in, *dir;
  int r;
  r = path_walk(existing, &in);
  if (r < 0)
    goto out;
  in->get();
  r = path_walk(path, &dir);
  if (r < 0)
    goto out_unlock;
  r = _link(in, dir, name.c_str());
  put_inode(dir);
 out_unlock:
  put_inode(in);
 out:
  return r;
}

int Client::unlink(const char *relpath)
{
  Mutex::Locker lock(client_lock);
  tout << "unlink" << std::endl;
  tout << relpath << std::endl;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  Inode *dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  return _unlink(dir, name.c_str());
}

int Client::rename(const char *relfrom, const char *relto)
{
  Mutex::Locker lock(client_lock);
  tout << "rename" << std::endl;
  tout << relfrom << std::endl;
  tout << relto << std::endl;

  filepath from(relfrom);
  filepath to(relto);
  string fromname = from.last_dentry();
  from.pop_dentry();
  string toname = to.last_dentry();
  to.pop_dentry();

  Inode *fromdir, *todir;
  int r;

  r = path_walk(from, &fromdir);
  if (r < 0)
    goto out;
  fromdir->get();
  r = path_walk(to, &todir);
  if (r < 0)
    goto out_unlock;
  r = _rename(fromdir, fromname.c_str(), todir, toname.c_str());
  put_inode(todir);
 out_unlock:
  put_inode(fromdir);
 out:
  return r;
}

// dirs

int Client::mkdir(const char *relpath, mode_t mode)
{
  Mutex::Locker lock(client_lock);
  tout << "mkdir" << std::endl;
  tout << relpath << std::endl;
  tout << mode << std::endl;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  Inode *dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  return _mkdir(dir, name.c_str(), mode);
}

int Client::rmdir(const char *relpath)
{
  Mutex::Locker lock(client_lock);
  tout << "rmdir" << std::endl;
  tout << relpath << std::endl;
  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  Inode *dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  return _rmdir(dir, name.c_str());
}

int Client::mknod(const char *relpath, mode_t mode, dev_t rdev) 
{ 
  Mutex::Locker lock(client_lock);
  tout << "mknod" << std::endl;
  tout << relpath << std::endl;
  tout << mode << std::endl;
  tout << rdev << std::endl;
  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  return _mknod(in, name.c_str(), mode, rdev);
}

// symlinks
  
int Client::symlink(const char *target, const char *relpath)
{
  Mutex::Locker lock(client_lock);
  tout << "symlink" << std::endl;
  tout << target << std::endl;
  tout << relpath << std::endl;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  Inode *dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  return _symlink(dir, name.c_str(), target);
}

int Client::readlink(const char *relpath, char *buf, loff_t size) 
{
  Mutex::Locker lock(client_lock);
  tout << "readlink" << std::endl;
  tout << relpath << std::endl;

  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  
  if (!in->inode.is_symlink())
    return -EINVAL;

  // copy into buf (at most size bytes)
  r = in->symlink.length();
  if (r > size)
    r = size;
  memcpy(buf, in->symlink.c_str(), r);
  return r;
}


// inode stuff

int Client::_getattr(Inode *in, int mask, int uid, int gid)
{
  int issued = in ? in->caps_issued():0;

  dout(10) << "_getattr mask " << ccap_string(mask) << " issued " << ccap_string(issued) << dendl;
  if ((issued & mask) == mask) {
    return 0;
  }

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_GETATTR);
  filepath fp(in ? in->ino() : inodeno_t(CEPH_INO_ROOT));
  req->set_filepath(fp);
  req->head.args.stat.mask = mask;
  
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  dout(10) << "_getattr result=" << res << dendl;
  delete reply;
  return res;
}

int Client::_setattr(Inode *in, struct stat *attr, int mask, int uid, int gid)
{
  int issued = in->caps_issued();

  dout(10) << "_setattr mask " << mask << " issued " << ccap_string(issued) << dendl;

  // make the change locally?
  if (issued & CEPH_CAP_AUTH_EXCL) {
    if (mask & CEPH_SETATTR_MODE) {
      in->inode.mode = attr->st_mode;
      in->dirty_caps |= CEPH_CAP_AUTH_EXCL;
      mask &= ~CEPH_SETATTR_MODE;
    }
    if (mask & CEPH_SETATTR_UID) {
      in->inode.uid = attr->st_uid;
      in->dirty_caps |= CEPH_CAP_AUTH_EXCL;
      mask &= ~CEPH_SETATTR_UID;
    }
    if (mask & CEPH_SETATTR_GID) {
      in->inode.gid = attr->st_gid;
      in->dirty_caps |= CEPH_CAP_AUTH_EXCL;
      mask &= ~CEPH_SETATTR_GID;
    }
  }
  if (issued & CEPH_CAP_FILE_EXCL) {
    if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME)) {
      if (mask & CEPH_SETATTR_MTIME)
	in->inode.mtime = utime_t(attr->st_mtime, 0);
      if (mask & CEPH_SETATTR_ATIME)
	in->inode.atime = utime_t(attr->st_atime, 0);
      in->inode.time_warp_seq++;
      in->dirty_caps |= CEPH_CAP_FILE_EXCL;
      mask &= ~(CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME);
    }
  }
  if (!mask)
    return 0;

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_SETATTR);
  filepath fp(in->ino());
  req->set_filepath(fp);
  if (mask & CEPH_SETATTR_MODE)
    req->head.args.setattr.mode = attr->st_mode;
  if (mask & CEPH_SETATTR_UID)
    req->head.args.setattr.uid = attr->st_uid;
  if (mask & CEPH_SETATTR_GID)
    req->head.args.setattr.gid = attr->st_gid;
  if (mask & CEPH_SETATTR_MTIME)
    req->head.args.setattr.mtime = utime_t(attr->st_mtime, 0);
  if (mask & CEPH_SETATTR_ATIME)
    req->head.args.setattr.atime = utime_t(attr->st_atime, 0);
  if (mask & CEPH_SETATTR_SIZE)
    req->head.args.setattr.size = attr->st_size;
  req->head.args.setattr.mask = mask;

  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  dout(10) << "_setattr result=" << res << dendl;
  delete reply;
  return res;
}

int Client::setattr(const char *relpath, struct stat *attr, int mask)
{
  Mutex::Locker lock(client_lock);
  tout << "setattr" << std::endl;
  tout << mask  << std::endl;

  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  return _setattr(in, attr, mask); 
}

int Client::fill_stat(Inode *in, struct stat *st, frag_info_t *dirstat, nest_info_t *rstat) 
{
  dout(10) << "fill_stat on " << in->inode.ino << " snap/dev" << in->snapid 
	   << " mode 0" << oct << in->inode.mode << dec
	   << " mtime " << in->inode.mtime << " ctime " << in->inode.ctime << dendl;
  memset(st, 0, sizeof(struct stat));
  st->st_ino = in->inode.ino;
  st->st_dev = in->snapid;
  st->st_mode = in->inode.mode;
  st->st_rdev = in->inode.rdev;
  st->st_nlink = in->inode.nlink;
  st->st_uid = in->inode.uid;
  st->st_gid = in->inode.gid;
  st->st_ctime = MAX(in->inode.ctime, in->inode.mtime);
  st->st_atime = in->inode.atime;
  st->st_mtime = in->inode.mtime;
  if (in->inode.is_dir()) {
    //st->st_size = in->inode.dirstat.size();
    st->st_size = in->inode.rstat.rbytes;
    st->st_blocks = 1;
  } else {
    st->st_size = in->inode.size;
    st->st_blocks = (in->inode.size + 511) >> 9;
  }
  st->st_blksize = MAX(ceph_file_layout_su(in->inode.layout), 4096);

  if (dirstat)
    *dirstat = in->inode.dirstat;
  if (rstat)
    *rstat = in->inode.rstat;

  return in->caps_issued();
}


int Client::lstat(const char *relpath, struct stat *stbuf, frag_info_t *dirstat)
{
  Mutex::Locker lock(client_lock);
  tout << "lstat" << std::endl;
  tout << relpath << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  r = _getattr(in, CEPH_STAT_CAP_INODE_ALL);
  if (r < 0)
    return r;
  fill_stat(in, stbuf, dirstat);
  return r;
}

int Client::chmod(const char *relpath, mode_t mode)
{
  Mutex::Locker lock(client_lock);
  tout << "chmod" << std::endl;
  tout << relpath << std::endl;
  tout << mode << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_mode = mode;
  return _setattr(in, &attr, CEPH_SETATTR_MODE);
}

int Client::chown(const char *relpath, uid_t uid, gid_t gid)
{
  Mutex::Locker lock(client_lock);
  tout << "chown" << std::endl;
  tout << relpath << std::endl;
  tout << uid << std::endl;
  tout << gid << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_uid = uid;
  attr.st_gid = gid;
  return _setattr(in, &attr, CEPH_SETATTR_UID|CEPH_SETATTR_GID);
}

int Client::utime(const char *relpath, struct utimbuf *buf)
{
  Mutex::Locker lock(client_lock);
  tout << "utime" << std::endl;
  tout << relpath << std::endl;
  tout << buf->modtime << std::endl;
  tout << buf->actime << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_mtime = buf->modtime;
  attr.st_atime = buf->actime;
  return _setattr(in, &attr, CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME);
}


int Client::getdir(const char *relpath, list<string>& contents)
{
  dout(3) << "getdir(" << relpath << ")" << dendl;
  {
    Mutex::Locker lock(client_lock);
    tout << "getdir" << std::endl;
    tout << relpath << std::endl;
  }

  DIR *d;
  int r = opendir(relpath, &d);
  if (r < 0) return r;

  struct dirent de;
  int n = 0;
  while (readdir_r(d, &de) == 0) {
    contents.push_back(de.d_name);
    n++;
  }
  closedir(d);

  return n;
}

int Client::opendir(const char *relpath, DIR **dirpp) 
{
  Mutex::Locker lock(client_lock);
  tout << "opendir" << std::endl;
  tout << relpath << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  r = _opendir(in, (DirResult**)dirpp);
  tout << (unsigned long)*dirpp << std::endl;
  return r;
}

int Client::_opendir(Inode *in, DirResult **dirpp, int uid, int gid) 
{
  *dirpp = new DirResult(in);
  (*dirpp)->set_frag(in->dirfragtree[0]);
  dout(10) << "_opendir " << in->ino() << ", our cache says the first dirfrag is " << (*dirpp)->frag() << dendl;

  // get the first frag
  int r = _readdir_get_frag(*dirpp);
  if (r < 0) {
    _closedir(*dirpp);
    *dirpp = 0;
  } else {
    r = 0;
  }
  dout(3) << "_opendir(" << in->ino() << ") = " << r << " (" << *dirpp << ")" << dendl;
  return r;
}

void Client::_readdir_add_dirent(DirResult *dirp, const string& name, Inode *in)
{
  struct stat st;
  int stmask = fill_stat(in, &st);  
  frag_t fg = dirp->frag();
  dirp->buffer[fg].push_back(DirEntry(name, st, stmask));
  dout(10) << "_readdir_add_dirent " << dirp << " added '" << name << "' -> " << in->inode.ino
	   << ", size now " << dirp->buffer[fg].size() << dendl;
}

//struct dirent {
//  ino_t          d_ino;       /* inode number */
//  off_t          d_off;       /* offset to the next dirent */
//  unsigned short d_reclen;    /* length of this record */
//  unsigned char  d_type;      /* type of file */
//  char           d_name[256]; /* filename */
//};
void Client::_readdir_fill_dirent(struct dirent *de, DirEntry *entry, loff_t off)
{
  strncpy(de->d_name, entry->d_name.c_str(), 256);
#ifndef __CYGWIN__
  de->d_ino = entry->st.st_ino;
#ifndef DARWIN
  de->d_off = off + 1;
#endif
  de->d_reclen = 1;
  de->d_type = MODE_TO_DT(entry->st.st_mode);
  dout(10) << "_readdir_fill_dirent '" << de->d_name << "' -> " << de->d_ino
	   << " type " << (int)de->d_type << " at off " << off << dendl;
#endif
}

void Client::_readdir_next_frag(DirResult *dirp)
{
  frag_t fg = dirp->frag();

  // hose old data
  assert(dirp->buffer.count(fg));
  dirp->buffer.erase(fg);

  // advance
  dirp->next_frag();
  if (dirp->at_end()) {
    dout(10) << "_readdir_next_frag advance from " << fg << " to END" << dendl;
  } else {
    dout(10) << "_readdir_next_frag advance from " << fg << " to " << dirp->frag() << dendl;
    _readdir_rechoose_frag(dirp);
  }
}

void Client::_readdir_rechoose_frag(DirResult *dirp)
{
  assert(dirp->inode);
  frag_t cur = dirp->frag();
  frag_t f = dirp->inode->dirfragtree[cur.value()];
  if (f != cur) {
    dout(10) << "_readdir_rechoose_frag frag " << cur << " maps to " << f << dendl;
    dirp->set_frag(f);
  }
}

int Client::_readdir_get_frag(DirResult *dirp)
{
  // get the current frag.
  frag_t fg = dirp->frag();
  assert(dirp->buffer.count(fg) == 0);
  
  dout(10) << "_readdir_get_frag " << dirp << " on " << dirp->inode->ino() << " fg " << fg << dendl;

  int op = CEPH_MDS_OP_READDIR;
  if (dirp->inode && dirp->inode->snapid == CEPH_SNAPDIR)
    op = CEPH_MDS_OP_LSSNAP;

  Inode *diri = dirp->inode;

  MClientRequest *req = new MClientRequest(op);
  filepath path(diri->ino());
  req->set_filepath(path); 
  req->head.args.readdir.frag = fg;
  
  utime_t from;
  MClientReply *reply = make_request(req, -1, -1, &from);
  int res = reply->get_result();
  int mds = reply->get_source().num();
  
  if (res == -EAGAIN) {
    dout(10) << "_readdir_get_frag got EAGAIN, retrying" << dendl;
    _readdir_rechoose_frag(dirp);
    return _readdir_get_frag(dirp);
  }

  if (res == 0) {
    // stuff dir contents to cache, DirResult
    assert(diri);

    // create empty result vector
    dirp->buffer[fg].clear();

    if (fg.is_leftmost()) {
      // add . and ..?
      string dot(".");
      _readdir_add_dirent(dirp, dot, diri);
      string dotdot("..");
      if (diri->dn)
	_readdir_add_dirent(dirp, dotdot, diri->dn->dir->parent_inode);
      //else
      //_readdir_add_dirent(dirp, dotdot, DT_DIR);
    }
    
    // the rest?
    bufferlist::iterator p = reply->get_dir_bl().begin();
    if (!p.end()) {
      // only open dir if we're actually adding stuff to it!
      Dir *dir = diri->open_dir();
      assert(dir);

      // dirstat
      DirStat dst(p);
      __u32 numdn;
      __u8 complete, end;
      ::decode(numdn, p);
      ::decode(end, p);
      ::decode(complete, p);

      string dname;
      LeaseStat dlease;
      while (numdn) {
	::decode(dname, p);
	::decode(dlease, p);
	InodeStat ist(p);

	Inode *in = add_update_inode(&ist, from, mds);
	insert_dentry_inode(dir, dname, &dlease, in, from, mds);

	// caller
	dout(15) << "_readdir_get_frag got " << dname << " to " << in->inode.ino << dendl;
	_readdir_add_dirent(dirp, dname, in);

	numdn--;
      }
      
      if (dir->is_empty())
	close_dir(dir);
    }

    // FIXME: remove items in cache that weren't in my readdir?
    // ***
  } else {
    dout(10) << "_readdir_get_frag got error " << res << ", setting end flag" << dendl;
    dirp->set_end();
  }

  delete reply;

  return res;
}

int Client::readdir_r(DIR *d, struct dirent *de)
{  
  return readdirplus_r(d, de, 0, 0);
}

int Client::readdirplus_r(DIR *d, struct dirent *de, struct stat *st, int *stmask)
{  
  DirResult *dirp = (DirResult*)d;
  
  while (1) {
    if (dirp->at_end()) return -1;

    if (dirp->buffer.count(dirp->frag()) == 0) {
      Mutex::Locker lock(client_lock);
      _readdir_get_frag(dirp);
      if (dirp->at_end()) return -1;
    }

    frag_t fg = dirp->frag();
    uint32_t pos = dirp->fragpos();
    assert(dirp->buffer.count(fg));   
    vector<DirEntry> &ent = dirp->buffer[fg];

    if (ent.empty()) {
      dout(10) << "empty frag " << fg << ", moving on to next" << dendl;
      _readdir_next_frag(dirp);
      continue;
    }

    assert(pos < ent.size());
    _readdir_fill_dirent(de, &ent[pos], dirp->offset);
    if (st) *st = ent[pos].st;
    if (stmask) *stmask = ent[pos].stmask;
    pos++;
    dirp->offset++;

    if (pos == ent.size()) 
      _readdir_next_frag(dirp);

    break;
  }

  return 0;
}


int Client::closedir(DIR *dir) 
{
  Mutex::Locker lock(client_lock);
  tout << "closedir" << std::endl;
  tout << (unsigned long)dir << std::endl;

  dout(3) << "closedir(" << dir << ") = 0" << dendl;
  _closedir((DirResult*)dir);
  return 0;
}

void Client::_closedir(DirResult *dirp)
{
  dout(10) << "_closedir(" << dirp << ")" << dendl;
  if (dirp->inode) {
    dout(10) << "_closedir detaching inode " << dirp->inode << dendl;
    put_inode(dirp->inode);
    dirp->inode = 0;
  }
  delete dirp;
}

void Client::rewinddir(DIR *dirp)
{
  dout(3) << "rewinddir(" << dirp << ")" << dendl;
  DirResult *d = (DirResult*)dirp;
  d->offset = 0;
  d->buffer.clear();
}
 
loff_t Client::telldir(DIR *dirp)
{
  DirResult *d = (DirResult*)dirp;
  dout(3) << "telldir(" << dirp << ") = " << d->offset << dendl;
  return d->offset;
}

void Client::seekdir(DIR *dirp, loff_t offset)
{
  dout(3) << "seekdir(" << dirp << ", " << offset << ")" << dendl;
  DirResult *d = (DirResult*)dirp;
  d->offset = offset;
}







/****** file i/o **********/

int Client::open(const char *relpath, int flags, mode_t mode) 
{
  Mutex::Locker lock(client_lock);
  tout << "open" << std::endl;
  tout << relpath << std::endl;
  tout << flags << std::endl;

  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;

  Fh *fh;
  r = _open(in, flags, mode, &fh);
  if (r >= 0) {
    // allocate a integer file descriptor
    assert(fh);
    r = get_fd();
    assert(fd_map.count(r) == 0);
    fd_map[r] = fh;
  }
  
  tout << r << std::endl;
  dout(3) << "open(" << path << ", " << flags << ") = " << r << dendl;
  return r;
}

int Client::_open(Inode *in, int flags, mode_t mode, Fh **fhp, int uid, int gid) 
{
  // go
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_OPEN);
  filepath path(in->ino());
  req->set_filepath(path); 
  req->head.args.open.flags = flags;
  req->head.args.open.mode = mode;

  int cmode = ceph_flags_to_mode(flags);
  in->get_open_ref(cmode);  // make note of pending open, since it effects _wanted_ caps.
  
  MClientReply *reply = make_request(req, uid, gid);
  int result = reply->get_result();

  // success?
  if (result >= 0) {
    // yay
    Fh *f = new Fh;
    if (fhp) *fhp = f;
    f->mode = cmode;
    if (flags & O_APPEND)
      f->append = true;

    // inode
    assert(in);
    f->inode = in;
    f->inode->get();

    dout(10) << in->inode.ino << " mode " << cmode << dendl;

    if (in->snapid != CEPH_NOSNAP) {
      in->snap_cap_refs++;
      dout(5) << "open success, fh is " << f << " combined IMMUTABLE SNAP caps " 
	      << ccap_string(in->caps_issued()) << dendl;
    }
  } else {
    in->put_open_ref(cmode);
  }

  delete reply;

  trim_cache();

  return result;
}




int Client::close(int fd)
{
  Mutex::Locker lock(client_lock);
  tout << "close" << std::endl;
  tout << fd << std::endl;

  dout(3) << "close(" << fd << ")" << dendl;
  assert(fd_map.count(fd));
  Fh *fh = fd_map[fd];
  _release(fh);
  fd_map.erase(fd);
  return 0;
}

int Client::_release(Fh *f)
{
  //dout(3) << "op: client->close(open_files[ " << fh << " ]);" << dendl;
  //dout(3) << "op: open_files.erase( " << fh << " );" << dendl;
  Inode *in = f->inode;
  dout(5) << "_release " << f << " mode " << f->mode << " on " << *in << dendl;

  if (in->snapid == CEPH_NOSNAP) {
    if (in->put_open_ref(f->mode)) {
      if (in->caps_used() & (CEPH_CAP_FILE_WRBUFFER|CEPH_CAP_FILE_WR))
	_flush(in);
      check_caps(in, false);
    }
  } else {
    assert(in->snap_cap_refs > 0);
    in->snap_cap_refs--;
  }

  put_inode( in );
  delete f;

  return 0;
}



// ------------
// read, write

loff_t Client::lseek(int fd, loff_t offset, int whence)
{
  Mutex::Locker lock(client_lock);
  tout << "lseek" << std::endl;
  tout << fd << std::endl;
  tout << offset << std::endl;
  tout << whence << std::endl;

  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];
  Inode *in = f->inode;

  switch (whence) {
  case SEEK_SET: 
    f->pos = offset;
    break;

  case SEEK_CUR:
    f->pos += offset;
    break;

  case SEEK_END:
    f->pos = in->inode.size + offset;
    break;

  default:
    assert(0);
  }
  
  loff_t pos = f->pos;

  dout(3) << "lseek(" << fd << ", " << offset << ", " << whence << ") = " << pos << dendl;
  return pos;
}


void Client::lock_fh_pos(Fh *f)
{
  dout(10) << "lock_fh_pos " << f << dendl;

  if (f->pos_locked || !f->pos_waiters.empty()) {
    Cond cond;
    f->pos_waiters.push_back(&cond);
    dout(10) << "lock_fh_pos BLOCKING on " << f << dendl;
    while (f->pos_locked || f->pos_waiters.front() != &cond)
      cond.Wait(client_lock);
    dout(10) << "lock_fh_pos UNBLOCKING on " << f << dendl;
    assert(f->pos_waiters.front() == &cond);
    f->pos_waiters.pop_front();
  }
  
  f->pos_locked = true;
}

void Client::unlock_fh_pos(Fh *f)
{
  dout(10) << "unlock_fh_pos " << f << dendl;
  f->pos_locked = false;
}



//char *hackbuf = 0;


// blocking osd interface

int Client::read(int fd, char *buf, loff_t size, loff_t offset) 
{
  Mutex::Locker lock(client_lock);
  tout << "read" << std::endl;
  tout << fd << std::endl;
  tout << size << std::endl;
  tout << offset << std::endl;

  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];
  bufferlist bl;
  int r = _read(f, offset, size, &bl);
  dout(3) << "read(" << fd << ", " << (void*)buf << ", " << size << ", " << offset << ") = " << r << dendl;
  if (r >= 0) {
    bl.copy(0, bl.length(), buf);
    r = bl.length();
  }
  return r;
}

int Client::_read(Fh *f, __s64 offset, __u64 size, bufferlist *bl)
{
  Inode *in = f->inode;

  bool movepos = false;
  if (offset < 0) {
    lock_fh_pos(f);
    offset = f->pos;
    movepos = true;
  }

  bool lazy = f->mode == CEPH_FILE_MODE_LAZY;

  // wait for RD cap and/or a valid file size
  int issued;
  while (1) {
    issued = in->caps_issued();

    if (lazy) {
      // wait for lazy cap
      if ((issued & CEPH_CAP_FILE_LAZYIO) == 0) {
	dout(7) << " don't have lazy cap, waiting" << dendl;
	goto wait;
      }
    } else {
      // wait for RD cap?
      while ((issued & CEPH_CAP_FILE_RD) == 0) {
	dout(7) << " don't have read cap, waiting" << dendl;
	goto wait;
      }
    }
    
    // async i/o?
    if ((issued & (CEPH_CAP_FILE_WRBUFFER|CEPH_CAP_FILE_RDCACHE))) {

      // FIXME: this logic needs to move info FileCache!

      // wait for valid file size
      if (!in->have_valid_size()) {
	dout(7) << " don't have (rd+rdcache)|lease|excl for valid file size, waiting" << dendl;
	goto wait;
      }

      dout(10) << "file size: " << in->inode.size << dendl;
      if (offset > 0 && (__u64)offset >= in->inode.size) {
	if (movepos) unlock_fh_pos(f);
	return 0;
      }
      if ((__u64)(offset + size) > in->inode.size) 
	size = in->inode.size - offset;
      
      if (size == 0) {
	dout(10) << "read is size=0, returning 0" << dendl;
	if (movepos) unlock_fh_pos(f);
	return 0;
      }
      break;
    } else {
      // unbuffered, sync i/o.  we will defer to osd.
      break;
    }
    
  wait:
    wait_on_list(in->waitfor_caps);
  }
  
  in->get_cap_ref(CEPH_CAP_FILE_RD);

  int rvalue = 0;
  Mutex flock("Client::_read flock");
  Cond cond;
  bool done = false;
  Context *onfinish = new C_SafeCond(&flock, &cond, &done, &rvalue);
  
  int r = 0;
  if (g_conf.client_oc) {

    if (issued & CEPH_CAP_FILE_RDCACHE) {
      // we will populate the cache here
      if (in->cap_refs[CEPH_CAP_FILE_RDCACHE] == 0)
	in->get_cap_ref(CEPH_CAP_FILE_RDCACHE);

      // readahead?
      if (f->nr_consec_read &&
	  (g_conf.client_readahead_max_bytes ||
	   g_conf.client_readahead_max_periods)) {
	loff_t l = f->consec_read_bytes * 2;
	if (g_conf.client_readahead_min)
	  l = MAX(l, g_conf.client_readahead_min);
	if (g_conf.client_readahead_max_bytes)
	  l = MIN(l, g_conf.client_readahead_max_bytes);
	loff_t p = ceph_file_layout_period(in->inode.layout);
	if (g_conf.client_readahead_max_periods)
	  l = MIN(l, g_conf.client_readahead_max_periods * p);
	if (l >= 2*p)
	  // align with period
	  l -= (offset+l) % p;
	// don't read past end of file
	if (offset+l > (loff_t)in->inode.size)
	  l = in->inode.size - offset;

	dout(10) << "readahead " << f->nr_consec_read << " reads " 
		 << f->consec_read_bytes << " bytes ... readahead " << offset << "~" << l
		 << " (caller wants " << offset << "~" << size << ")" << dendl;
	objectcacher->file_read(in->inode.ino, &in->inode.layout, in->snapid,
				offset, l, NULL, 0, 0);
	dout(10) << "readahead initiated" << dendl;
      }

      // read (and possibly block)
      if (in->snapid == CEPH_NOSNAP)
	r = objectcacher->file_read(in->inode.ino, &in->inode.layout, in->snapid,
				    offset, size, bl, 0, onfinish);
      else
	r = objectcacher->file_read(in->inode.ino, &in->inode.layout, in->snapid,
				    offset, size, bl, 0, onfinish);

      
      if (r == 0) {
	while (!done) 
	  cond.Wait(client_lock);
	r = rvalue;
      } else {
	// it was cached.
	delete onfinish;
      }
    } else {
      r = objectcacher->file_atomic_sync_read(in->inode.ino, &in->inode.layout, in->snapid,
					      offset, size, bl, 0, client_lock);
    }
    
  } else {
    // object cache OFF -- non-atomic sync read from osd
  
    // do sync read
    int flags = 0;
    if (in->hack_balance_reads || g_conf.client_hack_balance_reads)
      flags |= CEPH_OSD_FLAG_BALANCE_READS;
    filer->read(in->inode.ino, &in->inode.layout, in->snapid,
		offset, size, bl, flags, onfinish);

    while (!done)
      cond.Wait(client_lock);
    r = rvalue;
  }
 
  if (movepos) {
    // adjust fd pos
    f->pos = offset+bl->length();
    unlock_fh_pos(f);
  }

  // adjust readahead state
  if (f->last_pos != offset) {
    f->nr_consec_read = f->consec_read_bytes = 0;
  } else {
    f->nr_consec_read++;
  }
  f->consec_read_bytes += bl->length();
  dout(10) << "readahead nr_consec_read " << f->nr_consec_read
	   << " for " << f->consec_read_bytes << " bytes" 
	   << " .. last_pos " << f->last_pos << " .. offset " << offset
	   << dendl;
  f->last_pos = offset+bl->length();

  // done!
  put_cap_ref(in, CEPH_CAP_FILE_RD);

  return rvalue;
}



/*
 * we keep count of uncommitted sync writes on the inode, so that
 * fsync can DDRT.
 */
class C_Client_SyncCommit : public Context {
  Client *cl;
  Inode *in;
public:
  C_Client_SyncCommit(Client *c, Inode *i) : cl(c), in(i) {
    in->get();
  }
  void finish(int) {
    cl->sync_write_commit(in);
  }
};

void Client::sync_write_commit(Inode *in)
{
  client_lock.Lock();
  assert(unsafe_sync_write > 0);
  unsafe_sync_write--;

  put_cap_ref(in, CEPH_CAP_FILE_WRBUFFER);

  dout(15) << "sync_write_commit unsafe_sync_write = " << unsafe_sync_write << dendl;
  if (unsafe_sync_write == 0 && unmounting) {
    dout(10) << "sync_write_comit -- no more unsafe writes, unmount can proceed" << dendl;
    mount_cond.Signal();
  }

  put_inode(in);

  client_lock.Unlock();
}

int Client::write(int fd, const char *buf, loff_t size, loff_t offset) 
{
  Mutex::Locker lock(client_lock);
  tout << "write" << std::endl;
  tout << fd << std::endl;
  tout << size << std::endl;
  tout << offset << std::endl;

  assert(fd_map.count(fd));
  Fh *fh = fd_map[fd];
  int r = _write(fh, offset, size, buf);
  dout(3) << "write(" << fd << ", \"...\", " << size << ", " << offset << ") = " << r << dendl;
  return r;
}


int Client::_write(Fh *f, __s64 offset, __u64 size, const char *buf)
{
  //dout(7) << "write fh " << fh << " size " << size << " offset " << offset << dendl;
  Inode *in = f->inode;

  assert(in->snapid == CEPH_NOSNAP);

  // use/adjust fd pos?
  if (offset < 0) {
    lock_fh_pos(f);
    /* 
     * FIXME: this is racy in that we may block _after_ this point waiting for caps, and inode.size may
     * change out from under us.
     */
    if (f->append)
      f->pos = in->inode.size;   // O_APPEND.
    offset = f->pos;
    f->pos = offset+size;    
    unlock_fh_pos(f);
  }

  bool lazy = f->mode == CEPH_FILE_MODE_LAZY;

  dout(10) << "cur file size is " << in->inode.size << dendl;

  // time it.
  utime_t start = g_clock.real_now();
    
  // copy into fresh buffer (since our write may be resub, async)
  bufferptr bp;
  if (size > 0) bp = buffer::copy(buf, size);
  bufferlist bl;
  bl.push_back( bp );

  // request larger max_size?
  __u64 endoff = offset + size;
  if ((endoff >= in->inode.max_size ||
       endoff > (in->inode.size << 1)) &&
      endoff > in->wanted_max_size) {
    dout(10) << "wanted_max_size " << in->wanted_max_size << " -> " << endoff << dendl;
    in->wanted_max_size = endoff;
    check_caps(in, false);
  }
  
  // wait for caps, max_size
  while ((lazy && (in->caps_issued() & CEPH_CAP_FILE_LAZYIO) == 0) ||
	 (!lazy && (in->caps_issued() & CEPH_CAP_FILE_WR) == 0 &&
	  (in->cap_snaps.empty() || !in->cap_snaps.rbegin()->second.writing)) ||
	 endoff > in->inode.max_size) {
    dout(7) << "missing wr|lazy cap OR endoff " << endoff
	    << " > max_size " << in->inode.max_size 
	    << ", waiting" << dendl;
    wait_on_list(in->waitfor_caps);
  }

  in->get_cap_ref(CEPH_CAP_FILE_WR);

  // avoid livelock with fsync?
  // FIXME
  
  dout(10) << " snaprealm " << *in->snaprealm << dendl;

  if (g_conf.client_oc) {
    if (in->caps_issued() & CEPH_CAP_FILE_WRBUFFER) {
      // do buffered write
      if (in->cap_refs[CEPH_CAP_FILE_WRBUFFER] == 0)
	in->get_cap_ref(CEPH_CAP_FILE_WRBUFFER);
      
      // wait? (this may block!)
      objectcacher->wait_for_write(size, client_lock);
      
      // async, caching, non-blocking.
      objectcacher->file_write(in->inode.ino, &in->inode.layout, in->snaprealm->get_snap_context(),
			       offset, size, bl, g_clock.now(), 0);
    } else {
      // atomic, synchronous, blocking.
      objectcacher->file_atomic_sync_write(in->inode.ino, &in->inode.layout, in->snaprealm->get_snap_context(),
					   offset, size, bl, g_clock.now(), 0, client_lock);
    }   
  } else {
    // simple, non-atomic sync write
    Mutex flock("Client::_write flock");
    Cond cond;
    bool done = false;
    Context *onfinish = new C_SafeCond(&flock, &cond, &done);
    Context *onsafe = new C_Client_SyncCommit(this, in);

    unsafe_sync_write++;
    in->get_cap_ref(CEPH_CAP_FILE_WRBUFFER);
    
    filer->write(in->inode.ino, &in->inode.layout, in->snaprealm->get_snap_context(),
		 offset, size, bl, g_clock.now(), 0, onfinish, onsafe);
    
    while (!done)
      cond.Wait(client_lock);
  }

  // time
  utime_t lat = g_clock.real_now();
  lat -= start;
  if (client_logger)
    client_logger->favg(l_c_wrlat,(double)lat);
    
  // assume success for now.  FIXME.
  __u64 totalwritten = size;
  
  // extend file?
  if (totalwritten + offset > in->inode.size) {
    in->inode.size = totalwritten + offset;
    in->dirty_caps |= CEPH_CAP_FILE_WR;
    
    if ((in->inode.size << 1) >= in->inode.max_size &&
	(in->reported_size << 1) < in->inode.max_size)
      check_caps(in, false);
      
    dout(7) << "wrote to " << totalwritten+offset << ", extending file size" << dendl;
  } else {
    dout(7) << "wrote to " << totalwritten+offset << ", leaving file size at " << in->inode.size << dendl;
  }

  // mtime
  in->inode.mtime = g_clock.real_now();
  in->dirty_caps |= CEPH_CAP_FILE_WR;

  put_cap_ref(in, CEPH_CAP_FILE_WR);
  
  // ok!
  return totalwritten;  
}

int Client::_flush(Fh *f)
{
  // no-op, for now.  hrm.
  return 0;
}



int Client::truncate(const char *relpath, loff_t length) 
{
  struct stat attr;
  attr.st_size = length;
  return setattr(relpath, &attr, CEPH_SETATTR_SIZE);
}

int Client::ftruncate(int fd, loff_t length) 
{
  Mutex::Locker lock(client_lock);
  tout << "ftruncate" << std::endl;
  tout << fd << std::endl;
  tout << length << std::endl;

  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];
  struct stat attr;
  attr.st_size = length;
  return _setattr(f->inode, &attr, CEPH_SETATTR_SIZE);
}

int Client::fsync(int fd, bool syncdataonly) 
{
  Mutex::Locker lock(client_lock);
  tout << "fsync" << std::endl;
  tout << fd << std::endl;
  tout << syncdataonly << std::endl;

  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];
  int r = _fsync(f, syncdataonly);
  dout(3) << "fsync(" << fd << ", " << syncdataonly << ") = " << r << dendl;
  return r;
}

int Client::_fsync(Fh *f, bool syncdataonly)
{
  int r = 0;

  Inode *in = f->inode;

  dout(3) << "_fsync(" << f << ", " << (syncdataonly ? "dataonly)":"data+metadata)") << dendl;
  
  // metadata?
  if (!syncdataonly)
    dout(0) << "fsync - not syncing metadata yet.. implement me" << dendl;

  if (g_conf.client_oc)
    _flush(in);
  
  while (in->cap_refs[CEPH_CAP_FILE_WRBUFFER] > 0) {
    dout(10) << "ino " << in->inode.ino << " has " << in->cap_refs[CEPH_CAP_FILE_WRBUFFER]
	     << " uncommitted, waiting" << dendl;
    wait_on_list(in->waitfor_commit);
  }    
  dout(10) << "ino " << in->inode.ino << " has no uncommitted writes" << dendl;

  return r;
}

int Client::fstat(int fd, struct stat *stbuf) 
{
  Mutex::Locker lock(client_lock);
  tout << "fstat" << std::endl;
  tout << fd << std::endl;

  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];
  int r = _getattr(f->inode, -1);
  if (r < 0)
    return r;
  fill_stat(f->inode, stbuf, NULL);
  dout(3) << "fstat(" << fd << ", " << stbuf << ") = " << r << dendl;
  return r;
}


// not written yet, but i want to link!

int Client::chdir(const char *relpath)
{
  Mutex::Locker lock(client_lock);
  tout << "chdir" << std::endl;
  tout << relpath << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  if (cwd && cwd != in)
    put_inode(cwd);
  cwd = in;
  in->get();
  dout(3) << "chdir(" << relpath << ")  cwd now " << cwd->ino() << dendl;
  return 0;
}

int Client::statfs(const char *path, struct statvfs *stbuf)
{
  Mutex::Locker lock(client_lock);
  tout << "statfs" << std::endl;
  return _statfs(stbuf);
}

int Client::ll_statfs(vinodeno_t vino, struct statvfs *stbuf)
{
  Mutex::Locker lock(client_lock);
  tout << "ll_statfs" << std::endl;
  return _statfs(stbuf);
}

int Client::_statfs(struct statvfs *stbuf)
{
  dout(3) << "_statfs" << dendl;

  Cond cond;
  tid_t tid = ++last_tid;
  StatfsRequest *req = new StatfsRequest(tid, &cond);
  statfs_requests[tid] = req;

  int mon = monmap->pick_mon();
  messenger->send_message(new MStatfs(req->tid), monmap->get_inst(mon));

  while (req->reply == 0)
    cond.Wait(client_lock);

  // fill it in
  memset(stbuf, 0, sizeof(*stbuf));
  stbuf->f_bsize = 4096;
  stbuf->f_frsize = 4096;
  stbuf->f_blocks = req->reply->h.st.f_total / 4;
  stbuf->f_bfree = req->reply->h.st.f_free / 4;
  stbuf->f_bavail = req->reply->h.st.f_avail / 4;
  stbuf->f_files = req->reply->h.st.f_objects;
  stbuf->f_ffree = -1;
  stbuf->f_favail = -1;
  stbuf->f_fsid = -1;       // ??
  stbuf->f_flag = 0;        // ??
  stbuf->f_namemax = 1024;  // ??

  statfs_requests.erase(req->tid);
  delete req->reply;
  delete req;

  int r = 0;
  dout(3) << "_statfs = " << r << dendl;
  return r;
}

void Client::handle_statfs_reply(MStatfsReply *reply)
{
  if (statfs_requests.count(reply->h.tid) &&
      statfs_requests[reply->h.tid]->reply == 0) {
    dout(10) << "handle_statfs_reply " << *reply << ", kicking waiter" << dendl;
    statfs_requests[reply->h.tid]->reply = reply;
    statfs_requests[reply->h.tid]->caller_cond->Signal();
  } else {
    dout(10) << "handle_statfs_reply " << *reply << ", dup or old, dropping" << dendl;
    delete reply;
  }
}


int Client::lazyio_propogate(int fd, loff_t offset, size_t count)
{
  client_lock.Lock();
  dout(3) << "op: client->lazyio_propogate(" << fd
          << ", " << offset << ", " << count << ")" << dendl;
  
  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];

  // for now
  _fsync(f, true);

  client_lock.Unlock();
  return 0;
}

int Client::lazyio_synchronize(int fd, loff_t offset, size_t count)
{
  client_lock.Lock();
  dout(3) << "op: client->lazyio_synchronize(" << fd
          << ", " << offset << ", " << count << ")" << dendl;
  
  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];
  Inode *in = f->inode;
  
  _fsync(f, true);
  _release(in);
  
  client_lock.Unlock();
  return 0;
}


// =============================
// snaps

int Client::mksnap(const char *relpath, const char *name)
{
  Mutex::Locker l(client_lock);
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  return _mksnap(in, name);
}
int Client::rmsnap(const char *relpath, const char *name)
{
  Mutex::Locker l(client_lock);
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  return _rmsnap(in, name);
}


int Client::_mksnap(Inode *dir, const char *name, int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_MKSNAP);
  filepath path(dir->ino());
  req->set_filepath(path);
  req->set_string2(name);

  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  delete reply;

  trim_cache();
  dout(3) << "mksnap(" << path << ", '" << name << "'\") = " << res << dendl;
  return res;
}

int Client::_rmsnap(Inode *dir, const char *name, int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RMSNAP);
  filepath path(dir->ino());
  req->set_filepath(path);
  req->set_string2(name);

  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  delete reply;

  trim_cache();
  dout(3) << "rmsnap(" << path << ", '" << name << "'\") = " << res << dendl;
  return res;
}



// =========================================
// low level

Inode *Client::open_snapdir(Inode *diri)
{
  Inode *in;
  vinodeno_t vino(diri->ino(), CEPH_SNAPDIR);
  if (!inode_map.count(vino)) {
    in = new Inode(vino, &diri->inode.layout);
    in->inode = diri->inode;
    in->snapid = CEPH_SNAPDIR;
    in->inode.mode = S_IFDIR | 0600;
    in->dirfragtree.clear();
    inode_map[vino] = in;
    in->snapdir_parent = diri;
    diri->get();
    dout(10) << "open_snapdir created snapshot inode " << *in << dendl;
  } else {
    in = inode_map[vino];
    dout(10) << "open_snapdir had snapshot inode " << *in << dendl;
  }
  return in;
}

int Client::ll_lookup(vinodeno_t parent, const char *name, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_lookup " << parent << " " << name << dendl;
  tout << "ll_lookup" << std::endl;
  tout << parent.ino.val << std::endl;
  tout << name << std::endl;

  string dname = name;
  Inode *diri = 0;
  Inode *in = 0;
  int r = 0;
  utime_t now = g_clock.now();

  if (inode_map.count(parent) == 0) {
    dout(1) << "ll_lookup " << parent << " " << name << " -> ENOENT (parent DNE... WTF)" << dendl;
    r = -ENOENT;
    attr->st_ino = 0;
    goto out;
  }
  diri = inode_map[parent];
  if (!diri->inode.is_dir()) {
    dout(1) << "ll_lookup " << parent << " " << name << " -> ENOTDIR (parent not a dir... WTF)" << dendl;
    r = -ENOTDIR;
    attr->st_ino = 0;
    goto out;
  }

  r = _lookup(diri, dname.c_str(), &in);
  if (r < 0) {
    attr->st_ino = 0;
    goto out;
  }

  assert(in);
  fill_stat(in, attr);
  _ll_get(in);

 out:
  dout(3) << "ll_lookup " << parent << " " << name
	  << " -> " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  tout << attr->st_ino << std::endl;
  return r;
}

void Client::_ll_get(Inode *in)
{
  if (in->ll_ref == 0) 
    in->get();
  in->ll_get();
  dout(20) << "_ll_get " << in << " " << in->inode.ino << " -> " << in->ll_ref << dendl;
}

int Client::_ll_put(Inode *in, int num)
{
  in->ll_put(num);
  dout(20) << "_ll_put " << in << " " << in->inode.ino << " " << num << " -> " << in->ll_ref << dendl;
  if (in->ll_ref == 0) {
    put_inode(in);
    return 0;
  } else {
    return in->ll_ref;
  }
}

void Client::_ll_drop_pins()
{
  dout(10) << "_ll_drop_pins" << dendl;
  hash_map<vinodeno_t, Inode*>::iterator next;
  for (hash_map<vinodeno_t, Inode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it = next) {
    Inode *in = it->second;
    next = it;
    next++;
    if (in->ll_ref)
      _ll_put(in, in->ll_ref);
  }
}

bool Client::ll_forget(vinodeno_t vino, int num)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_forget " << vino << " " << num << dendl;
  tout << "ll_forget" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << num << std::endl;

  if (vino.ino == 1) return true;  // ignore forget on root.

  bool last = false;
  if (inode_map.count(vino) == 0) {
    dout(1) << "WARNING: ll_forget on " << vino << " " << num 
	    << ", which I don't have" << dendl;
  } else {
    Inode *in = inode_map[vino];
    assert(in);
    if (in->ll_ref < num) {
      dout(1) << "WARNING: ll_forget on " << vino << " " << num << ", which only has ll_ref=" << in->ll_ref << dendl;
      _ll_put(in, in->ll_ref);
      last = true;
    } else {
      if (_ll_put(in, num) == 0)
	last = true;
    }
  }
  return last;
}

Inode *Client::_ll_get_inode(vinodeno_t vino)
{
  assert(inode_map.count(vino));
  return inode_map[vino];
}


int Client::ll_getattr(vinodeno_t vino, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_getattr " << vino << dendl;
  tout << "ll_getattr" << std::endl;
  tout << vino.ino.val << std::endl;

  Inode *in = _ll_get_inode(vino);
  int res;
  if (vino.snapid < CEPH_NOSNAP)
    res = 0;
  else
    res = _getattr(in, CEPH_STAT_CAP_INODE_ALL, uid, gid);
  if (res == 0)
    fill_stat(in, attr);
  dout(3) << "ll_getattr " << vino << " = " << res << dendl;
  return res;
}

int Client::ll_setattr(vinodeno_t vino, struct stat *attr, int mask, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_setattr " << vino << " mask " << hex << mask << dec << dendl;
  tout << "ll_setattr" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << attr->st_mode << std::endl;
  tout << attr->st_uid << std::endl;
  tout << attr->st_gid << std::endl;
  tout << attr->st_size << std::endl;
  tout << attr->st_mtime << std::endl;
  tout << attr->st_atime << std::endl;
  tout << mask << std::endl;

  Inode *in = _ll_get_inode(vino);

  int res = _setattr(in, attr, mask, uid, gid);
  if (res == 0)
    fill_stat(in, attr);
  dout(3) << "ll_setattr " << vino << " = " << res << dendl;
  return 0;
}


// ----------
// xattrs

int Client::_getxattr(Inode *in, const char *name, void *value, size_t size,
		      int uid, int gid)
{
  int r = _getattr(in, CEPH_STAT_CAP_XATTR, uid, gid);
  if (r == 0) {
    string n(name);
    r = -ENODATA;
    if (in->xattrs.count(n)) {
      r = in->xattrs[n].length();
      if (size != 0) {
	if (size >= (unsigned)r)
	  memcpy(value, in->xattrs[n].c_str(), r);
	else
	  r = -ERANGE;
      }
    }
  }
  dout(3) << "_getxattr(" << in->ino() << ", \"" << name << "\", " << size << ") = " << r << dendl;
  return r;
}

int Client::ll_getxattr(vinodeno_t vino, const char *name, void *value, size_t size, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_getxattr " << vino << " " << name << " size " << size << dendl;
  tout << "ll_getxattr" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << name << std::endl;

  Inode *in = _ll_get_inode(vino);
  return _getxattr(in, name, value, size, uid, gid);
}

int Client::_listxattr(Inode *in, char *name, size_t size, int uid, int gid)
{
  int r = _getattr(in, CEPH_STAT_CAP_XATTR, uid, gid);
  if (r == 0) {
    for (map<string,bufferptr>::iterator p = in->xattrs.begin();
	 p != in->xattrs.end();
	 p++)
      r += p->first.length() + 1;
    
    if (size != 0) {
      if (size >= (unsigned)r) {
	for (map<string,bufferptr>::iterator p = in->xattrs.begin();
	     p != in->xattrs.end();
	     p++) {
	  memcpy(name, p->first.c_str(), p->first.length());
	  name += p->first.length();
	  *name = '\0';
	  name++;
	}
      } else
	r = -ERANGE;
    }
  }
  dout(3) << "_listxattr(" << in->ino() << ", " << size << ") = " << r << dendl;
  return r;
}

int Client::ll_listxattr(vinodeno_t vino, char *names, size_t size, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_listxattr " << vino << " size " << size << dendl;
  tout << "ll_listxattr" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << size << std::endl;

  Inode *in = _ll_get_inode(vino);
  return _listxattr(in, names, size, uid, gid);
}

int Client::_setxattr(Inode *in, const char *name, const void *value, size_t size, int flags,
		      int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_SETXATTR);
  filepath fp(in->ino());
  req->set_filepath(fp);
  req->set_string2(name);
  req->head.args.setxattr.flags = flags;

  bufferlist bl;
  bl.append((const char*)value, size);
  req->set_data(bl);
  
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  delete reply;

  trim_cache();
  dout(3) << "_setxattr(" << in->ino() << ", \"" << name << "\") = " << res << dendl;
  return res;
}

int Client::ll_setxattr(vinodeno_t vino, const char *name, const void *value, size_t size, int flags,
			int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_setxattr " << vino << " " << name << " size " << size << dendl;
  tout << "ll_setxattr" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << name << std::endl;

  // only user xattrs, for now
  if (strncmp(name, "user.", 5))
    return -EOPNOTSUPP;

  Inode *in = _ll_get_inode(vino);
  return _setxattr(in, name, value, size, flags, uid, gid);
}

int Client::_removexattr(Inode *in, const char *name, int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RMXATTR);
  filepath fp(in->ino());
  req->set_filepath(fp);
 
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  delete reply;

  trim_cache();
  dout(3) << "_removexattr(" << in->ino() << ", \"" << name << "\") = " << res << dendl;
  return res;
}


int Client::ll_removexattr(vinodeno_t vino, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_removexattr " << vino << " " << name << dendl;
  tout << "ll_removexattr" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << name << std::endl;

  // only user xattrs, for now
  if (strncmp(name, "user.", 5))
    return -EOPNOTSUPP;

  Inode *in = _ll_get_inode(vino);
  return _removexattr(in, name, uid, gid);
}


int Client::ll_readlink(vinodeno_t vino, const char **value, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_readlink " << vino << dendl;
  tout << "ll_readlink" << std::endl;
  tout << vino.ino.val << std::endl;

  Inode *in = _ll_get_inode(vino);
  if (in->dn) touch_dn(in->dn);

  int r = 0;
  if (in->inode.is_symlink()) {
    *value = in->symlink.c_str();
  } else {
    *value = "";
    r = -EINVAL;
  }
  dout(3) << "ll_readlink " << vino << " = " << r << " (" << *value << ")" << dendl;
  return r;
}

int Client::_mknod(Inode *dir, const char *name, mode_t mode, dev_t rdev, int uid, int gid) 
{ 
  dout(3) << "_mknod(" << dir->ino() << " " << name << ", 0" << oct << mode << dec << ", " << rdev << ")" << dendl;

  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_MKNOD);
  filepath path(name, dir->ino());
  req->set_filepath(path); 
  req->head.args.mknod.mode = mode;
  req->head.args.mknod.rdev = rdev;

  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();

  delete reply;

  trim_cache();

  dout(3) << "mknod(" << path << ", 0" << oct << mode << dec << ") = " << res << dendl;
  return res;
}

int Client::ll_mknod(vinodeno_t parent, const char *name, mode_t mode, dev_t rdev, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_mknod " << parent << " " << name << dendl;
  tout << "ll_mknod" << std::endl;
  tout << parent.ino.val << std::endl;
  tout << name << std::endl;
  tout << mode << std::endl;
  tout << rdev << std::endl;

  Inode *diri = _ll_get_inode(parent);

  int r = _mknod(diri, name, mode, rdev, uid, gid);
  if (r == 0) {
    string dname(name);
    Inode *in = diri->dir->dentries[dname]->inode;
    fill_stat(in, attr);
    _ll_get(in);
  }
  tout << attr->st_ino << std::endl;
  dout(3) << "ll_mknod " << parent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  return r;
}

int Client::_mkdir(Inode *dir, const char *name, mode_t mode, int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_MKDIR);
  filepath path(name, dir->ino());
  req->set_filepath(path);
  req->head.args.mkdir.mode = mode;
 
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  delete reply;
  dout(10) << "mkdir result is " << res << dendl;

  trim_cache();

  dout(3) << "mkdir(" << path << ", 0" << oct << mode << dec << ") = " << res << dendl;
  return res;
}

int Client::ll_mkdir(vinodeno_t parent, const char *name, mode_t mode, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_mkdir " << parent << " " << name << dendl;
  tout << "ll_mkdir" << std::endl;
  tout << parent.ino.val << std::endl;
  tout << name << std::endl;
  tout << mode << std::endl;

  Inode *diri = _ll_get_inode(parent);

  int r;
  if (diri->snapid == CEPH_SNAPDIR) {
    MClientRequest *req = new MClientRequest(CEPH_MDS_OP_MKSNAP);
    filepath path(name, diri->ino());
    req->set_filepath(path);
    req->set_string2(name);

    Inode *in;
    MClientReply *reply = make_request(req, uid, gid);
    r = reply->get_result();
    update_snap_trace(reply->snapbl);
    delete reply;
    dout(10) << "mksnap result is " << r << " vino " << in->vino() << dendl;
    if (r == 0) {
      fill_stat(in, attr);
      _ll_get(in);
    }
  } else {
    r = _mkdir(diri, name, mode, uid, gid);
    if (r == 0) {
      string dname(name);
      Inode *in = diri->dir->dentries[dname]->inode;
      fill_stat(in, attr);
      _ll_get(in);
    }
  }
  tout << attr->st_ino << std::endl;
  dout(3) << "ll_mkdir " << parent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  return r;
}

int Client::_symlink(Inode *dir, const char *name, const char *target, int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_SYMLINK);
  filepath path(name, dir->ino());
  req->set_filepath(path);
  req->set_string2(target);
 
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  delete reply;

  trim_cache();
  dout(3) << "_symlink(\"" << path << "\", \"" << target << "\") = " << res << dendl;
  return res;
}

int Client::ll_symlink(vinodeno_t parent, const char *name, const char *value, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_symlink " << parent << " " << name << " -> " << value << dendl;
  tout << "ll_symlink" << std::endl;
  tout << parent.ino.val << std::endl;
  tout << name << std::endl;
  tout << value << std::endl;

  Inode *diri = _ll_get_inode(parent);
  int r = _symlink(diri, name, value, uid, gid);
  if (r == 0) {
    string dname(name);
    Inode *in = diri->dir->dentries[dname]->inode;
    fill_stat(in, attr);
    _ll_get(in);
  }
  tout << attr->st_ino << std::endl;
  dout(3) << "ll_symlink " << parent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  return r;
}

int Client::_unlink(Inode *dir, const char *name, int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_UNLINK);
  filepath path(name, dir->ino());
  req->set_filepath(path);
 
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  if (res == 0) {
    Dentry *dn = dir->dir->dentries[name];
    if (dn)
      unlink(dn);
  }
  delete reply;
  dout(10) << "unlink result is " << res << dendl;

  trim_cache();
  dout(3) << "unlink(" << path << ") = " << res << dendl;
  return res;
}

int Client::ll_unlink(vinodeno_t vino, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_unlink " << vino << " " << name << dendl;
  tout << "ll_unlink" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << name << std::endl;

  Inode *diri = _ll_get_inode(vino);
  return _unlink(diri, name, uid, gid);
}

int Client::_rmdir(Inode *dir, const char *name, int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RMDIR);
  filepath path(name, dir->ino());
  req->set_filepath(path);
 
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  if (res == 0) {
    Dentry *dn = dir->dir->dentries[name];
    if (dn) {
      if (dn->inode->dir && dn->inode->dir->is_empty()) 
        close_dir(dn->inode->dir);  // FIXME: maybe i shoudl proactively hose the whole subtree from cache?
      unlink(dn);
    }
  }
  delete reply;

  trim_cache();
  dout(3) << "rmdir(" << path << ") = " << res << dendl;
  return res;
}

int Client::ll_rmdir(vinodeno_t vino, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_rmdir " << vino << " " << name << dendl;
  tout << "ll_rmdir" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << name << std::endl;

  Inode *diri = _ll_get_inode(vino);

  if (diri->snapid == CEPH_SNAPDIR) {
    MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RMSNAP);
    filepath path;
    diri->make_path(path);
    req->set_filepath(path);
    req->set_string2(name);
    MClientReply *reply = make_request(req, uid, gid);
    int r = reply->get_result();
    delete reply;
    return r;
  }
  return _rmdir(diri, name, uid, gid);
}

int Client::_rename(Inode *fromdir, const char *fromname, Inode *todir, const char *toname, int uid, int gid)
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_RENAME);
  filepath from(fromname, fromdir->ino());
  filepath to(toname, todir->ino());
  req->set_filepath(to);
  req->set_filepath2(from);
 
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  if (res == 0) {
    // remove from local cache
    Dentry *dn = fromdir->dir->dentries[fromname];
    if (dn)
      unlink(dn);
  }
  delete reply;
  dout(10) << "rename result is " << res << dendl;

  // renamed item from our cache

  trim_cache();
  dout(3) << "rename(" << from << ", " << to << ") = " << res << dendl;
  return res;
}

int Client::ll_rename(vinodeno_t parent, const char *name, vinodeno_t newparent, const char *newname, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_rename " << parent << " " << name << " to "
	  << newparent << " " << newname << dendl;
  tout << "ll_rename" << std::endl;
  tout << parent.ino.val << std::endl;
  tout << name << std::endl;
  tout << newparent.ino.val << std::endl;
  tout << newname << std::endl;

  Inode *fromdiri = _ll_get_inode(parent);
  Inode *todiri = _ll_get_inode(newparent);
  return _rename(fromdiri, name, todiri, newname, uid, gid);
}

int Client::_link(Inode *in, Inode *dir, const char *newname, int uid, int gid) 
{
  MClientRequest *req = new MClientRequest(CEPH_MDS_OP_LINK);
  filepath path(newname, dir->ino());
  req->set_filepath(path);
  filepath existing(in->ino());
  req->set_filepath2(existing);
  
  MClientReply *reply = make_request(req, uid, gid);
  int res = reply->get_result();
  delete reply;
  dout(10) << "link result is " << res << dendl;

  trim_cache();
  dout(3) << "link(" << existing << ", " << path << ") = " << res << dendl;
  return res;
}

int Client::ll_link(vinodeno_t vino, vinodeno_t newparent, const char *newname, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_link " << vino << " to " << newparent << " " << newname << dendl;
  tout << "ll_link" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << newparent << std::endl;
  tout << newname << std::endl;

  Inode *old = _ll_get_inode(vino);
  Inode *diri = _ll_get_inode(newparent);

  int r = _link(old, diri, newname, uid, gid);
  if (r == 0) {
    Inode *in = _ll_get_inode(vino);
    fill_stat(in, attr);
    _ll_get(in);
  }
  return r;
}

int Client::ll_opendir(vinodeno_t vino, void **dirpp, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_opendir " << vino << dendl;
  tout << "ll_opendir" << std::endl;
  tout << vino.ino.val << std::endl;
  
  Inode *diri = inode_map[vino];
  assert(diri);

  filepath path;
  if (vino.snapid == CEPH_SNAPDIR) {
    Inode *livediri = inode_map[vinodeno_t(vino.ino, CEPH_NOSNAP)];
    assert(livediri);
    livediri->make_path(path);
  } else
    diri->make_path(path);
  dout(10) << " ino path is " << path << dendl;

  int r = 0;
  if (vino.snapid == CEPH_SNAPDIR) {
    *dirpp = new DirResult(diri);
  } else {
    r = _opendir(diri, (DirResult**)dirpp);
  }

  tout << (unsigned long)*dirpp << std::endl;

  dout(3) << "ll_opendir " << vino << " = " << r << " (" << *dirpp << ")" << dendl;
  return r;
}

void Client::ll_releasedir(void *dirp)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_releasedir " << dirp << dendl;
  tout << "ll_releasedir" << std::endl;
  tout << (unsigned long)dirp << std::endl;
  _closedir((DirResult*)dirp);
}

int Client::ll_open(vinodeno_t vino, int flags, Fh **fhp, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_open " << vino << " " << flags << dendl;
  tout << "ll_open" << std::endl;
  tout << vino.ino.val << std::endl;
  tout << flags << std::endl;

  Inode *in = _ll_get_inode(vino);
  int r = _open(in, flags, 0, fhp, uid, gid);

  tout << (unsigned long)*fhp << std::endl;
  dout(3) << "ll_open " << vino << " " << flags << " = " << r << " (" << *fhp << ")" << dendl;
  return r;
}

int Client::ll_create(vinodeno_t parent, const char *name, mode_t mode, int flags, 
		      struct stat *attr, Fh **fhp, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_create " << parent << " " << name << " 0" << oct << mode << dec << " " << flags << dendl;
  tout << "ll_create" << std::endl;
  tout << parent.ino.val << std::endl;
  tout << name << std::endl;
  tout << mode << std::endl;
  tout << flags << std::endl;

  Inode *dir = _ll_get_inode(parent);
  int r = _mknod(dir, name, 0, 0);
  if (r < 0)
    return r;
  Dentry *dn = dir->dir->dentries[name];
  Inode *in = dn->inode;

  r = _open(in, flags, mode, fhp, uid, gid);
  if (r >= 0) {
    Inode *in = (*fhp)->inode;
    fill_stat(in, attr);
    _ll_get(in);
  } else {
    attr->st_ino = 0;
  }
  tout << (unsigned long)*fhp << std::endl;
  tout << attr->st_ino << std::endl;
  dout(3) << "ll_create " << parent << " " << name << " 0" << oct << mode << dec << " " << flags
	  << " = " << r << " (" << *fhp << " " << hex << attr->st_ino << dec << ")" << dendl;
  return 0;
}

int Client::ll_read(Fh *fh, loff_t off, loff_t len, bufferlist *bl)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_read " << fh << " " << off << "~" << len << dendl;
  tout << "ll_read" << std::endl;
  tout << (unsigned long)fh << std::endl;
  tout << off << std::endl;
  tout << len << std::endl;

  return _read(fh, off, len, bl);
}

int Client::ll_write(Fh *fh, loff_t off, loff_t len, const char *data)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_write " << fh << " " << off << "~" << len << dendl;
  tout << "ll_write" << std::endl;
  tout << (unsigned long)fh << std::endl;
  tout << off << std::endl;
  tout << len << std::endl;

  int r = _write(fh, off, len, data);
  dout(3) << "ll_write " << fh << " " << off << "~" << len << " = " << r << dendl;
  return r;
}

int Client::ll_flush(Fh *fh)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_flush " << fh << dendl;
  tout << "ll_flush" << std::endl;
  tout << (unsigned long)fh << std::endl;

  return _flush(fh);
}

int Client::ll_fsync(Fh *fh, bool syncdataonly) 
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_fsync " << fh << dendl;
  tout << "ll_fsync" << std::endl;
  tout << (unsigned long)fh << std::endl;

  return _fsync(fh, syncdataonly);
}


int Client::ll_release(Fh *fh)
{
  Mutex::Locker lock(client_lock);
  dout(3) << "ll_release " << fh << dendl;
  tout << "ll_release" << std::endl;
  tout << (unsigned long)fh << std::endl;

  _release(fh);
  return 0;
}






// =========================================
// layout


int Client::describe_layout(int fd, ceph_file_layout *lp)
{
  Mutex::Locker lock(client_lock);

  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];
  Inode *in = f->inode;

  *lp = in->inode.layout;

  dout(3) << "describe_layout(" << fd << ") = 0" << dendl;
  return 0;
}

int Client::get_stripe_unit(int fd)
{
  ceph_file_layout layout;
  describe_layout(fd, &layout);
  return ceph_file_layout_su(layout);
}

int Client::get_stripe_width(int fd)
{
  ceph_file_layout layout;
  describe_layout(fd, &layout);
  return ceph_file_layout_stripe_width(layout);
}

int Client::get_stripe_period(int fd)
{
  ceph_file_layout layout;
  describe_layout(fd, &layout);
  return ceph_file_layout_period(layout);
}

int Client::enumerate_layout(int fd, vector<ObjectExtent>& result,
			     loff_t length, loff_t offset)
{
  Mutex::Locker lock(client_lock);

  assert(fd_map.count(fd));
  Fh *f = fd_map[fd];
  Inode *in = f->inode;

  // map to a list of extents
  filer->file_to_extents(in->inode.ino, &in->inode.layout, CEPH_NOSNAP, offset, length, result);

  dout(3) << "enumerate_layout(" << fd << ", " << length << ", " << offset << ") = 0" << dendl;
  return 0;
}





// ===============================

void Client::ms_handle_failure(Message *m, const entity_inst_t& inst)
{
  entity_name_t dest = inst.name;
  dout(0) << "ms_handle_failure " << *m << " to " << inst << dendl;
}

void Client::ms_handle_reset(const entity_addr_t& addr, entity_name_t last) 
{
  dout(0) << "ms_handle_reset on " << addr << dendl;
}


void Client::ms_handle_remote_reset(const entity_addr_t& addr, entity_name_t last) 
{
  dout(0) << "ms_handle_remote_reset on " << addr << ", last " << last << dendl;
  if (last.is_mds()) {
    int mds = last.num();
    dout(0) << "ms_handle_remote_reset on " << last << ", " << mds_sessions[mds].num_caps
	    << " caps, kicking requests" << dendl;

    mds_sessions.erase(mds); // "kill" session

    // reopen if caps
    if (mds_sessions[mds].num_caps > 0) {
      waiting_for_session[mds].size();  // make sure entry exists
      messenger->send_message(new MClientSession(CEPH_SESSION_REQUEST_OPEN),
			      mdsmap->get_inst(mds));
      
      /*
       * FIXME: actually, we need to do a reconnect or similar to reestablish
       * our caps (where possible)
       */
      dout(0) << "FIXME: client needs to reconnect to restablish existing caps ****" << dendl;
    }

    // or requests
    kick_requests(mds);
  }
  else 
    objecter->ms_handle_remote_reset(addr, last);

}
