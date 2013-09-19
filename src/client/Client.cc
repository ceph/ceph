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
#include <sys/param.h>
#include <fcntl.h>

#if defined(__linux__)
#include <linux/falloc.h>
#endif

#include <sys/statvfs.h>

#include <iostream>
using namespace std;

#include "common/config.h"

// ceph stuff

#include "messages/MMonMap.h"

#include "messages/MClientSession.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientReply.h"
#include "messages/MClientCaps.h"
#include "messages/MClientCapRelease.h"
#include "messages/MClientLease.h"
#include "messages/MClientSnap.h"

#include "messages/MGenericMessage.h"

#include "messages/MMDSMap.h"

#include "mon/MonClient.h"

#include "mds/MDSMap.h"
#include "osd/OSDMap.h"
#include "mon/MonMap.h"

#include "osdc/Filer.h"
#include "osdc/WritebackHandler.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_client

#include "include/lru.h"

#include "include/compat.h"

#include "Client.h"
#include "Inode.h"
#include "Dentry.h"
#include "Dir.h"
#include "ClientSnapRealm.h"
#include "Fh.h"
#include "MetaSession.h"
#include "MetaRequest.h"
#include "ObjecterWriteback.h"

#include "include/assert.h"

#undef dout_prefix
#define dout_prefix *_dout << "client." << whoami << " "

#define  tout(cct)       if (!cct->_conf->client_trace.empty()) traceout



void client_flush_set_callback(void *p, ObjectCacher::ObjectSet *oset)
{
  Client *client = static_cast<Client*>(p);
  client->flush_set_callback(oset);
}


// -------------

Client::CommandHook::CommandHook(Client *client) :
  m_client(client)
{
}

bool Client::CommandHook::call(std::string command, cmdmap_t& cmdmap,
			       std::string format, bufferlist& out)
{
  stringstream ss;
  Formatter *f = new_formatter(format);
  f->open_object_section("result");
  m_client->client_lock.Lock();
  if (command == "mds_requests")
    m_client->dump_mds_requests(f);
  else if (command == "mds_sessions")
    m_client->dump_mds_sessions(f);
  else if (command == "dump_cache")
    m_client->dump_cache(f);
  else
    assert(0 == "bad command registered");
  m_client->client_lock.Unlock();
  f->close_section();
  f->flush(out);
  delete f;
  return true;
}


// -------------

dir_result_t::dir_result_t(Inode *in)
  : inode(in), offset(0), this_offset(2), next_offset(2),
    release_count(0), start_shared_gen(0),
    buffer(0) { 
  inode->get();
}

// cons/des

Client::Client(Messenger *m, MonClient *mc)
  : Dispatcher(m->cct),
    cct(m->cct),
    logger(NULL),
    m_command_hook(this),
    timer(m->cct, client_lock),
    ino_invalidate_cb(NULL),
    ino_invalidate_cb_handle(NULL),
    getgroups_cb(NULL),
    getgroups_cb_handle(NULL),
    async_ino_invalidator(m->cct),
    tick_event(NULL),
    monclient(mc), messenger(m), whoami(m->get_myname().num()),
    initialized(false), mounted(false), unmounting(false),
    local_osd(-1), local_osd_epoch(0),
    unsafe_sync_write(0),
    client_lock("Client::client_lock")
{
  monclient->set_messenger(m);

  last_tid = 0;
  last_flush_seq = 0;

  cwd = NULL;

  // 
  root = 0;

  num_flushing_caps = 0;

  lru.lru_set_max(cct->_conf->client_cache_size);
  lru.lru_set_midpoint(cct->_conf->client_cache_mid);

  // file handles
  free_fd_set.insert(10, 1<<30);

  // set up messengers
  messenger = m;

  // osd interfaces
  osdmap = new OSDMap;     // initially blank.. see mount()
  mdsmap = new MDSMap;
  objecter = new Objecter(cct, messenger, monclient, osdmap, client_lock, timer);
  objecter->set_client_incarnation(0);  // client always 0, for now.
  writeback_handler = new ObjecterWriteback(objecter);
  objectcacher = new ObjectCacher(cct, "libcephfs", *writeback_handler, client_lock,
				  client_flush_set_callback,    // all commit callback
				  (void*)this,
				  cct->_conf->client_oc_size,
				  cct->_conf->client_oc_max_objects,
				  cct->_conf->client_oc_max_dirty,
				  cct->_conf->client_oc_target_dirty,
				  cct->_conf->client_oc_max_dirty_age,
				  true);
  filer = new Filer(objecter);
}


Client::~Client() 
{
  assert(!client_lock.is_locked());

  tear_down_cache();

  delete objectcacher;
  delete writeback_handler;

  delete filer;
  delete objecter;
  delete osdmap;
  delete mdsmap;

  delete logger;
}






void Client::tear_down_cache()
{
  // fd's
  for (hash_map<int, Fh*>::iterator it = fd_map.begin();
       it != fd_map.end();
       ++it) {
    Fh *fh = it->second;
    ldout(cct, 1) << "tear_down_cache forcing close of fh " << it->first << " ino " << fh->inode->ino << dendl;
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

inodeno_t Client::get_root_ino()
{
  return root->ino;
}


// debug crapola

void Client::dump_inode(Formatter *f, Inode *in, set<Inode*>& did, bool disconnected)
{
  filepath path;
  in->make_long_path(path);
  ldout(cct, 1) << "dump_inode: "
		<< (disconnected ? "DISCONNECTED ":"")
		<< "inode " << in->ino
		<< " " << path
		<< " ref " << in->get_num_ref()
		<< *in << dendl;

  if (f) {
    f->open_object_section("inode");
    f->dump_stream("path") << path;
    if (disconnected)
      f->dump_int("disconnected", 1);
    in->dump(f);
    f->close_section();
  }

  did.insert(in);
  if (in->dir) {
    ldout(cct, 1) << "  dir " << in->dir << " size " << in->dir->dentries.size() << dendl;
    for (hash_map<string, Dentry*>::iterator it = in->dir->dentries.begin();
         it != in->dir->dentries.end();
         ++it) {
      ldout(cct, 1) << "   " << in->ino << " dn " << it->first << " " << it->second << " ref " << it->second->ref << dendl;
      if (f) {
	f->open_object_section("dentry");
	it->second->dump(f);
	f->close_section();
      }	
      if (it->second->inode)
	dump_inode(f, it->second->inode, did, false);
    }
  }
}

void Client::dump_cache(Formatter *f)
{
  set<Inode*> did;

  ldout(cct, 1) << "dump_cache" << dendl;

  if (f)
    f->open_array_section("cache");

  if (root)
    dump_inode(f, root, did, true);

  // make a second pass to catch anything disconnected
  for (hash_map<vinodeno_t, Inode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       ++it) {
    if (did.count(it->second))
      continue;
    dump_inode(f, it->second, did, true);
  }

  if (f)
    f->close_section();
}

int Client::init()
{
  client_lock.Lock();
  assert(!initialized);

  timer.init();

  objectcacher->start();

  // ok!
  messenger->add_dispatcher_head(this);

  int r = monclient->init();
  if (r < 0) {
    // need to do cleanup because we're in an intermediate init state
    timer.shutdown();
    client_lock.Unlock();
    objectcacher->stop();
    monclient->shutdown();
    return r;
  }

  client_lock.Unlock();
  objecter->init_unlocked();
  client_lock.Lock();

  objecter->init_locked();

  monclient->set_want_keys(CEPH_ENTITY_TYPE_MDS | CEPH_ENTITY_TYPE_OSD);
  monclient->sub_want("mdsmap", 0, 0);
  monclient->sub_want("osdmap", 0, CEPH_SUBSCRIBE_ONETIME);
  monclient->renew_subs();

  // logger
  PerfCountersBuilder plb(cct, "client", l_c_first, l_c_last);
  plb.add_time_avg(l_c_reply, "reply");
  plb.add_time_avg(l_c_lat, "lat");
  plb.add_time_avg(l_c_wrlat, "wrlat");
  plb.add_time_avg(l_c_owrlat, "owrlat");
  plb.add_time_avg(l_c_ordlat, "ordlat");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);

  client_lock.Unlock();

  AdminSocket* admin_socket = cct->get_admin_socket();
  int ret = admin_socket->register_command("mds_requests",
					   "mds_requests",
					   &m_command_hook,
					   "show in-progress mds requests");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }
  ret = admin_socket->register_command("mds_sessions",
				       "mds_sessions",
				       &m_command_hook,
				       "show mds session state");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }
  ret = admin_socket->register_command("dump_cache",
				       "dump_cache",
				       &m_command_hook,
				       "show in-memory metadata cache contents");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }

  client_lock.Lock();
  initialized = true;
  client_lock.Unlock();
  return r;
}

void Client::shutdown() 
{
  ldout(cct, 1) << "shutdown" << dendl;

  AdminSocket* admin_socket = cct->get_admin_socket();
  admin_socket->unregister_command("mds_requests");
  admin_socket->unregister_command("mds_sessions");
  admin_socket->unregister_command("dump_cache");

  if (ino_invalidate_cb) {
    ldout(cct, 10) << "shutdown stopping invalidator finisher" << dendl;
    async_ino_invalidator.wait_for_empty();
    async_ino_invalidator.stop();
  }

  objectcacher->stop();  // outside of client_lock! this does a join.

  client_lock.Lock();
  assert(initialized);
  initialized = false;
  timer.shutdown();
  objecter->shutdown_locked();
  client_lock.Unlock();
  objecter->shutdown_unlocked();
  monclient->shutdown();

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = NULL;
  }
}




// ===================
// metadata cache stuff

void Client::trim_cache()
{
  ldout(cct, 20) << "trim_cache size " << lru.lru_get_size() << " max " << lru.lru_get_max() << dendl;
  unsigned last = 0;
  while (lru.lru_get_size() != last) {
    last = lru.lru_get_size();

    if (lru.lru_get_size() <= lru.lru_get_max())  break;

    // trim!
    Dentry *dn = static_cast<Dentry*>(lru.lru_expire());
    if (!dn)
      break;  // done
    
    trim_dentry(dn);
  }

  // hose root?
  if (lru.lru_get_size() == 0 && root && root->get_num_ref() == 0 && inode_map.size() == 1) {
    ldout(cct, 15) << "trim_cache trimmed root " << root << dendl;
    delete root;
    root = 0;
    inode_map.clear();
  }
}

void Client::trim_dentry(Dentry *dn)
{
  ldout(cct, 15) << "trim_dentry unlinking dn " << dn->name 
		 << " in dir " << hex << dn->dir->parent_inode->ino 
		 << dendl;
  if (dn->dir->parent_inode->flags & I_COMPLETE) {
    ldout(cct, 10) << " clearing I_COMPLETE on " << *dn->dir->parent_inode << dendl;
    dn->dir->parent_inode->flags &= ~I_COMPLETE;
    dn->dir->release_count++;
  }
  unlink(dn, false);
}


void Client::update_inode_file_bits(Inode *in,
				    uint64_t truncate_seq, uint64_t truncate_size,
				    uint64_t size,
				    uint64_t time_warp_seq, utime_t ctime,
				    utime_t mtime,
				    utime_t atime,
				    int issued)
{
  bool warn = false;
  ldout(cct, 10) << "update_inode_file_bits " << *in << " " << ccap_string(issued)
	   << " mtime " << mtime << dendl;
  ldout(cct, 25) << "truncate_seq: mds " << truncate_seq <<  " local "
	   << in->truncate_seq << " time_warp_seq: mds " << time_warp_seq
	   << " local " << in->time_warp_seq << dendl;
  uint64_t prior_size = in->size;

  if (truncate_seq > in->truncate_seq ||
      (truncate_seq == in->truncate_seq && size > in->size)) {
    ldout(cct, 10) << "size " << in->size << " -> " << size << dendl;
    in->size = size;
    in->reported_size = size;
    if (truncate_seq != in->truncate_seq) {
      ldout(cct, 10) << "truncate_seq " << in->truncate_seq << " -> "
	       << truncate_seq << dendl;
      in->truncate_seq = truncate_seq;
      in->oset.truncate_seq = truncate_seq;

      // truncate cached file data
      if (prior_size > size) {
	_invalidate_inode_cache(in, truncate_size, prior_size - truncate_size, true);
      }
    }
  }
  if (truncate_seq >= in->truncate_seq &&
      in->truncate_size != truncate_size) {
    if (in->is_file()) {
      ldout(cct, 10) << "truncate_size " << in->truncate_size << " -> "
	       << truncate_size << dendl;
      in->truncate_size = truncate_size;
      in->oset.truncate_size = truncate_size;
    } else {
      ldout(cct, 0) << "Hmmm, truncate_seq && truncate_size changed on non-file inode!" << dendl;
    }
  }
  
  // be careful with size, mtime, atime
  if (issued & (CEPH_CAP_FILE_EXCL|
		CEPH_CAP_FILE_WR|
		CEPH_CAP_FILE_BUFFER|
		CEPH_CAP_AUTH_EXCL|
		CEPH_CAP_XATTR_EXCL)) {
    ldout(cct, 30) << "Yay have enough caps to look at our times" << dendl;
    if (ctime > in->ctime) 
      in->ctime = ctime;
    if (time_warp_seq > in->time_warp_seq) {
      ldout(cct, 10) << "mds time_warp_seq " << time_warp_seq << " on inode " << *in
	       << " is higher than local time_warp_seq "
	       << in->time_warp_seq << dendl;
      //the mds updated times, so take those!
      in->mtime = mtime;
      in->atime = atime;
      in->time_warp_seq = time_warp_seq;
    } else if (time_warp_seq == in->time_warp_seq) {
      //take max times
      if (mtime > in->mtime)
	in->mtime = mtime;
      if (atime > in->atime)
	in->atime = atime;
    } else if (issued & CEPH_CAP_FILE_EXCL) {
      //ignore mds values as we have a higher seq
    } else warn = true;
  } else {
    ldout(cct, 30) << "Don't have enough caps, just taking mds' time values" << dendl;
    if (time_warp_seq >= in->time_warp_seq) {
      in->ctime = ctime;
      in->mtime = mtime;
      in->atime = atime;
      in->time_warp_seq = time_warp_seq;
    } else warn = true;
  }
  if (warn) {
    ldout(cct, 0) << "WARNING: " << *in << " mds time_warp_seq "
	    << time_warp_seq << " is lower than local time_warp_seq "
	    << in->time_warp_seq
	    << dendl;
  }
}

Inode * Client::add_update_inode(InodeStat *st, utime_t from, MetaSession *session)
{
  Inode *in;
  bool was_new = false;
  if (inode_map.count(st->vino)) {
    in = inode_map[st->vino];
    ldout(cct, 12) << "add_update_inode had " << *in << " caps " << ccap_string(st->cap.caps) << dendl;
  } else {
    in = new Inode(cct, st->vino, &st->layout);
    inode_map[st->vino] = in;
    if (!root) {
      root = in;
      cwd = root;
      cwd->get();
    }

    // immutable bits
    in->ino = st->vino.ino;
    in->snapid = st->vino.snapid;
    in->mode = st->mode & S_IFMT;
    was_new = true;
  }

  in->rdev = st->rdev;
  if (in->is_symlink())
    in->symlink = st->symlink;

  if (was_new)
    ldout(cct, 12) << "add_update_inode adding " << *in << " caps " << ccap_string(st->cap.caps) << dendl;

  if (!st->cap.caps)
    return in;   // as with readdir returning indoes in different snaprealms (no caps!)

  // only update inode if mds info is strictly newer, or it is the same and projected (odd).
  bool updating_inode = false;
  int issued = 0;
  if (st->version == 0 ||
      (in->version & ~1) < st->version) {
    updating_inode = true;

    int implemented = 0;
    issued = in->caps_issued(&implemented) | in->caps_dirty();
    issued |= implemented;

    in->version = st->version;

    if ((issued & CEPH_CAP_AUTH_EXCL) == 0) {
      in->mode = st->mode;
      in->uid = st->uid;
      in->gid = st->gid;
    }

    if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
      in->nlink = st->nlink;
    }

    if ((issued & CEPH_CAP_XATTR_EXCL) == 0 &&
	st->xattrbl.length() &&
	st->xattr_version > in->xattr_version) {
      bufferlist::iterator p = st->xattrbl.begin();
      ::decode(in->xattrs, p);
      in->xattr_version = st->xattr_version;
    }

    in->dirstat = st->dirstat;
    in->rstat = st->rstat;

    if (in->is_dir()) {
      in->dir_layout = st->dir_layout;
      ldout(cct, 20) << " dir hash is " << (int)in->dir_layout.dl_dir_hash << dendl;
    }

    in->layout = st->layout;
    in->ctime = st->ctime;
    in->max_size = st->max_size;  // right?
  
    update_inode_file_bits(in, st->truncate_seq, st->truncate_size, st->size,
			   st->time_warp_seq, st->ctime, st->mtime, st->atime,
			   issued);
  }

  // move me if/when version reflects fragtree changes.
  in->dirfragtree = st->dirfragtree;

  if (in->snapid == CEPH_NOSNAP)
    add_update_cap(in, session, st->cap.cap_id, st->cap.caps, st->cap.seq, st->cap.mseq, inodeno_t(st->cap.realm), st->cap.flags);
  else
    in->snap_caps |= st->cap.caps;

  // setting I_COMPLETE needs to happen after adding the cap
  if (updating_inode &&
      in->is_dir() &&
      (st->cap.caps & CEPH_CAP_FILE_SHARED) &&
      (issued & CEPH_CAP_FILE_EXCL) == 0 &&
      in->dirstat.nfiles == 0 &&
      in->dirstat.nsubdirs == 0) {
    ldout(cct, 10) << " marking I_COMPLETE on empty dir " << *in << dendl;
    in->flags |= I_COMPLETE;
    if (in->dir) {
      ldout(cct, 10) << " dir is open on empty dir " << in->ino << " with "
		     << in->dir->dentry_map.size() << " entries, tearing down" << dendl;
      while (!in->dir->dentry_map.empty())
	unlink(in->dir->dentry_map.begin()->second, true);
      close_dir(in->dir);
    }
  }  

  return in;
}


/*
 * insert_dentry_inode - insert + link a single dentry + inode into the metadata cache.
 */
Dentry *Client::insert_dentry_inode(Dir *dir, const string& dname, LeaseStat *dlease, 
				    Inode *in, utime_t from, MetaSession *session, bool set_offset,
				    Dentry *old_dentry)
{
  Dentry *dn = NULL;
  if (dir->dentries.count(dname))
    dn = dir->dentries[dname];
  
  ldout(cct, 12) << "insert_dentry_inode '" << dname << "' vino " << in->vino()
		 << " in dir " << dir->parent_inode->vino() << " dn " << dn
		 << dendl;
  
  if (dn && dn->inode) {
    if (dn->inode->vino() == in->vino()) {
      touch_dn(dn);
      ldout(cct, 12) << " had dentry " << dname
	       << " with correct vino " << dn->inode->vino()
	       << dendl;
    } else {
      ldout(cct, 12) << " had dentry " << dname
	       << " with WRONG vino " << dn->inode->vino()
	       << dendl;
      unlink(dn, true);
      dn = NULL;
    }
  }
  
  if (!dn || dn->inode == 0) {
    in->get();
    if (old_dentry)
      unlink(old_dentry, dir == old_dentry->dir);  // keep dir open if its the same dir
    dn = link(dir, dname, in, dn);
    put_inode(in);
    if (set_offset) {
      ldout(cct, 15) << " setting dn offset to " << dir->max_offset << dendl;
      dn->offset = dir->max_offset++;
    }
  }

  update_dentry_lease(dn, dlease, from, session);
  return dn;
}

void Client::update_dentry_lease(Dentry *dn, LeaseStat *dlease, utime_t from, MetaSession *session)
{
  utime_t dttl = from;
  dttl += (float)dlease->duration_ms / 1000.0;
  
  assert(dn && dn->inode);

  if (dlease->mask & CEPH_LOCK_DN) {
    if (dttl > dn->lease_ttl) {
      ldout(cct, 10) << "got dentry lease on " << dn->name
	       << " dur " << dlease->duration_ms << "ms ttl " << dttl << dendl;
      dn->lease_ttl = dttl;
      dn->lease_mds = session->mds_num;
      dn->lease_seq = dlease->seq;
      dn->lease_gen = session->cap_gen;
    }
  }
  dn->cap_shared_gen = dn->dir->parent_inode->shared_gen;
}


/*
 * update MDS location cache for a single inode
 */
void Client::update_dir_dist(Inode *in, DirStat *dst)
{
  // auth
  ldout(cct, 20) << "got dirfrag map for " << in->ino << " frag " << dst->frag << " to mds " << dst->auth << dendl;
  if (dst->auth >= 0) {
    in->fragmap[dst->frag] = dst->auth;
  } else {
    in->fragmap.erase(dst->frag);
  }
  assert(in->dirfragtree.is_leaf(dst->frag));

  // replicated
  in->dir_replicated = !dst->dist.empty();  // FIXME that's just one frag!
  
  // dist
  /*
  if (!st->dirfrag_dist.empty()) {   // FIXME
    set<int> dist = st->dirfrag_dist.begin()->second;
    if (dist.empty() && !in->dir_contacts.empty())
      ldout(cct, 9) << "lost dist spec for " << in->ino 
              << " " << dist << dendl;
    if (!dist.empty() && in->dir_contacts.empty()) 
      ldout(cct, 9) << "got dist spec for " << in->ino 
              << " " << dist << dendl;
    in->dir_contacts = dist;
  }
  */
}

/*
 * insert results from readdir or lssnap into the metadata cache.
 */
void Client::insert_readdir_results(MetaRequest *request, MetaSession *session, Inode *diri) {

  MClientReply *reply = request->reply;
  ConnectionRef con = request->reply->get_connection();
  uint64_t features = con->get_features();

  assert(request->readdir_result.empty());

  // the extra buffer list is only set for readdir and lssnap replies
  bufferlist::iterator p = reply->get_extra_bl().begin();
  if (!p.end()) {
    // snapdir?
    if (request->head.op == CEPH_MDS_OP_LSSNAP) {
      assert(diri);
      diri = open_snapdir(diri);
    }

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

    frag_t fg = request->readdir_frag;
    uint64_t readdir_offset = request->readdir_offset;
    string readdir_start = request->readdir_start;
    if (fg != dst.frag) {
      ldout(cct, 10) << "insert_trace got new frag " << fg << " -> " << dst.frag << dendl;
      fg = dst.frag;
      if (fg.is_leftmost())
	readdir_offset = 2;
      else
	readdir_offset = 0;
      readdir_start.clear();
    }

    ldout(cct, 10) << "insert_trace " << numdn << " readdir items, end=" << (int)end
		   << ", offset " << readdir_offset
		   << ", readdir_start " << readdir_start << dendl;

    request->readdir_reply_frag = fg;
    request->readdir_end = end;
    request->readdir_num = numdn;

    map<string,Dentry*>::iterator pd = dir->dentry_map.upper_bound(readdir_start);

    string dname;
    LeaseStat dlease;
    for (unsigned i=0; i<numdn; i++) {
      ::decode(dname, p);
      ::decode(dlease, p);
      InodeStat ist(p, features);

      ldout(cct, 15) << "" << i << ": '" << dname << "'" << dendl;

      // remove any skipped names
      while (pd != dir->dentry_map.end() && pd->first < dname) {
	if (pd->first < dname &&
	    diri->dirfragtree[ceph_str_hash_linux(pd->first.c_str(),
						  pd->first.length())] == fg) {  // do not remove items in earlier frags
	  ldout(cct, 15) << "insert_trace  unlink '" << pd->first << "'" << dendl;
	  Dentry *dn = pd->second;
	  ++pd;
	  unlink(dn, true);
	} else {
	  ++pd;
	}
      }

      if (pd == dir->dentry_map.end())
	ldout(cct, 15) << " pd is at end" << dendl;
      else
	ldout(cct, 15) << " pd is '" << pd->first << "' dn " << pd->second << dendl;

      Inode *in = add_update_inode(&ist, request->sent_stamp, session);
      Dentry *dn;
      if (pd != dir->dentry_map.end() &&
	  pd->first == dname) {
	Dentry *olddn = pd->second;
	if (pd->second->inode != in) {
	  // replace incorrect dentry
	  ++pd;  // we are about to unlink this guy, move past it.
	  unlink(olddn, true);
	  dn = link(dir, dname, in, NULL);
	} else {
	  // keep existing dn
	  dn = olddn;
	  touch_dn(dn);
	  ++pd;  // move past the dentry we just touched.
	}
      } else {
	// new dn
	dn = link(dir, dname, in, NULL);
      }
      update_dentry_lease(dn, &dlease, request->sent_stamp, session);
      dn->offset = dir_result_t::make_fpos(fg, i + readdir_offset);

      // add to cached result list
      in->get();
      request->readdir_result.push_back(pair<string,Inode*>(dname, in));

      ldout(cct, 15) << "insert_trace  " << hex << dn->offset << dec << ": '" << dname << "' -> " << in->ino << dendl;
    }
    request->readdir_last_name = dname;

    // remove trailing names
    if (end) {
      while (pd != dir->dentry_map.end()) {
	if (diri->dirfragtree[ceph_str_hash_linux(pd->first.c_str(),
						  pd->first.length())] == fg) {
	  ldout(cct, 15) << "insert_trace  unlink '" << pd->first << "'" << dendl;
	  Dentry *dn = pd->second;
	  ++pd;
	  unlink(dn, true);
	} else
	  ++pd;
      }
    }

    if (dir->is_empty())
      close_dir(dir);
  }
}

/** insert_trace
 *
 * insert a trace from a MDS reply into the cache.
 */
Inode* Client::insert_trace(MetaRequest *request, MetaSession *session)
{
  MClientReply *reply = request->reply;

  ldout(cct, 10) << "insert_trace from " << request->sent_stamp << " mds." << session->mds_num
	   << " is_target=" << (int)reply->head.is_target
	   << " is_dentry=" << (int)reply->head.is_dentry
	   << dendl;

  bufferlist::iterator p = reply->get_trace_bl().begin();
  if (p.end()) {
    ldout(cct, 10) << "insert_trace -- no trace" << dendl;

    Dentry *d = request->dentry();
    if (d && d->dir &&
	(d->dir->parent_inode->flags & I_COMPLETE)) {
      ldout(cct, 10) << " clearing I_COMPLETE on " << *d->dir->parent_inode << dendl;
      d->dir->parent_inode->flags &= ~I_COMPLETE;
      d->dir->release_count++;
    }
    return NULL;
  }

  ConnectionRef con = request->reply->get_connection();
  uint64_t features = con->get_features();
  ldout(cct, 10) << " features 0x" << hex << features << dec << dendl;

  // snap trace
  if (reply->snapbl.length())
    update_snap_trace(reply->snapbl);

  ldout(cct, 10) << " hrm " 
	   << " is_target=" << (int)reply->head.is_target
	   << " is_dentry=" << (int)reply->head.is_dentry
	   << dendl;

  InodeStat dirst;
  DirStat dst;
  string dname;
  LeaseStat dlease;
  InodeStat ist;

  if (reply->head.is_dentry) {
    dirst.decode(p, features);
    dst.decode(p);
    ::decode(dname, p);
    ::decode(dlease, p);
  }

  Inode *in = 0;
  if (reply->head.is_target) {
    ist.decode(p, features);

    in = add_update_inode(&ist, request->sent_stamp, session);
  }

  if (reply->head.is_dentry) {
    Inode *diri = add_update_inode(&dirst, request->sent_stamp, session);
    update_dir_dist(diri, &dst);  // dir stat info is attached to ..

    if (in) {
      Dir *dir = diri->open_dir();
      insert_dentry_inode(dir, dname, &dlease, in, request->sent_stamp, session, true,
                          ((request->head.op == CEPH_MDS_OP_RENAME) ?
                                        request->old_dentry() : NULL));
    } else {
      if (diri->dir && diri->dir->dentries.count(dname)) {
	Dentry *dn = diri->dir->dentries[dname];
	if (dn->inode)
	  unlink(dn, false);
      }
    }
  } else if (reply->head.op == CEPH_MDS_OP_LOOKUPSNAP ||
	     reply->head.op == CEPH_MDS_OP_MKSNAP) {
    ldout(cct, 10) << " faking snap lookup weirdness" << dendl;
    // fake it for snap lookup
    vinodeno_t vino = ist.vino;
    vino.snapid = CEPH_SNAPDIR;
    assert(inode_map.count(vino));
    Inode *diri = inode_map[vino];
    
    string dname = request->path.last_dentry();
    
    LeaseStat dlease;
    dlease.duration_ms = 0;

    if (in) {
      Dir *dir = diri->open_dir();
      insert_dentry_inode(dir, dname, &dlease, in, request->sent_stamp, session, true);
    } else {
      if (diri->dir && diri->dir->dentries.count(dname)) {
	Dentry *dn = diri->dir->dentries[dname];
	if (dn->inode)
	  unlink(dn, false);
      }
    }
  }

  if (in && (reply->head.op == CEPH_MDS_OP_READDIR ||
	     reply->head.op == CEPH_MDS_OP_LSSNAP)) {
    insert_readdir_results(request, session, in);
  }

  request->target = in;
  return in;
}

// -------

int Client::choose_target_mds(MetaRequest *req) 
{
  int mds = -1;
  __u32 hash = 0;
  bool is_hash = false;

  Inode *in = NULL;
  Dentry *de = NULL;
  Cap *cap = NULL;

  if (req->resend_mds >= 0) {
    mds = req->resend_mds;
    req->resend_mds = -1;
    ldout(cct, 10) << "choose_target_mds resend_mds specified as mds." << mds << dendl;
    goto out;
  }

  if (cct->_conf->client_use_random_mds)
    goto random_mds;

  in = req->inode();
  de = req->dentry();
  if (in) {
    ldout(cct, 20) << "choose_target_mds starting with req->inode " << *in << dendl;
    if (req->path.depth()) {
      hash = ceph_str_hash(in->dir_layout.dl_dir_hash,
			   req->path[0].data(),
			   req->path[0].length());
      ldout(cct, 20) << "choose_target_mds inode dir hash is " << (int)in->dir_layout.dl_dir_hash
	       << " on " << req->path[0]
	       << " => " << hash << dendl;
      is_hash = true;
    }
  } else if (de) {
    if (de->inode) {
      in = de->inode;
      ldout(cct, 20) << "choose_target_mds starting with req->dentry inode " << *in << dendl;
    } else {
      in = de->dir->parent_inode;
      hash = ceph_str_hash(in->dir_layout.dl_dir_hash,
			   de->name.data(),
			   de->name.length());
      ldout(cct, 20) << "choose_target_mds dentry dir hash is " << (int)in->dir_layout.dl_dir_hash
	       << " on " << de->name
	       << " => " << hash << dendl;
      is_hash = true;
    }
  }
  if (in && in->snapid != CEPH_NOSNAP) {
    ldout(cct, 10) << "choose_target_mds " << *in << " is snapped, using nonsnap parent" << dendl;
    while (in->snapid != CEPH_NOSNAP) {
      if (in->snapid == CEPH_SNAPDIR)
	in = in->snapdir_parent;
      else if (!in->dn_set.empty())
        /* In most cases there will only be one dentry, so getting it
         * will be the correct action. If there are multiple hard links,
         * I think the MDS should be able to redirect as needed*/
	in = in->get_first_parent()->dir->parent_inode;
      else {
        ldout(cct, 10) << "got unlinked inode, can't look at parent" << dendl;
        break;
      }
    }
    is_hash = false;
  }
  
  if (!in)
    goto random_mds;

  ldout(cct, 20) << "choose_target_mds " << *in << " is_hash=" << is_hash
           << " hash=" << hash << dendl;

  if (is_hash && S_ISDIR(in->mode) && !in->dirfragtree.empty()) {
    frag_t fg = in->dirfragtree[hash];
    if (in->fragmap.count(fg)) {
      mds = in->fragmap[fg];
      ldout(cct, 10) << "choose_target_mds from dirfragtree hash" << dendl;
      goto out;
    }
  }

  if (req->auth_is_best())
    cap = in->auth_cap;
  if (!cap && !in->caps.empty())
    cap = in->caps.begin()->second;
  if (!cap)
    goto random_mds;
  mds = cap->session->mds_num;
  ldout(cct, 10) << "choose_target_mds from caps on inode " << *in << dendl;

  goto out;

random_mds:
  if (mds < 0) {
    mds = mdsmap->get_random_up_mds();
    ldout(cct, 10) << "did not get mds through better means, so chose random mds " << mds << dendl;
  }

out:
  ldout(cct, 20) << "mds is " << mds << dendl;
  return mds;
}


void Client::connect_mds_targets(int mds)
{
  ldout(cct, 10) << "connect_mds_targets for mds." << mds << dendl;
  assert(mds_sessions.count(mds));
  const MDSMap::mds_info_t& info = mdsmap->get_mds_info(mds);
  for (set<int>::const_iterator q = info.export_targets.begin();
       q != info.export_targets.end();
       ++q) {
    if (mds_sessions.count(*q) == 0 &&
	mdsmap->is_clientreplay_or_active_or_stopping(*q)) {
      ldout(cct, 10) << "check_mds_sessions opening mds." << mds
		     << " export target mds." << *q << dendl;
      _open_mds_session(*q);
    }
  }
}

void Client::dump_mds_sessions(Formatter *f)
{
  f->open_array_section("sessions");
  for (map<int,MetaSession*>::const_iterator p = mds_sessions.begin(); p != mds_sessions.end(); ++p) {
    f->open_object_section("session");
    p->second->dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_int("mdsmap_epoch", mdsmap->get_epoch());
}
void Client::dump_mds_requests(Formatter *f)
{
  for (map<tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end();
       ++p) {
    f->open_object_section("request");
    p->second->dump(f);
    f->close_section();
  }
}

int Client::verify_reply_trace(int r,
			       MetaRequest *request, MClientReply *reply,
			       Inode **ptarget, bool *pcreated,
			       int uid, int gid)
{
  // check whether this request actually did the create, and set created flag
  bufferlist extra_bl;
  inodeno_t created_ino;
  bool got_created_ino = false;
  hash_map<vinodeno_t, Inode*>::iterator p;

  extra_bl.claim(reply->get_extra_bl());
  if (extra_bl.length() >= 8) {
    // if the extra bufferlist has a buffer, we assume its the created inode
    // and that this request to create succeeded in actually creating
    // the inode (won the race with other create requests)
    ::decode(created_ino, extra_bl);
    got_created_ino = true;
    ldout(cct, 10) << "make_request created ino " << created_ino << dendl;
  }

  if (pcreated)
    *pcreated = got_created_ino;

  if (request->target) {
    *ptarget = request->target;
    ldout(cct, 20) << "make_request target is " << *request->target << dendl;
  } else {
    if (got_created_ino && (p = inode_map.find(vinodeno_t(created_ino, CEPH_NOSNAP))) != inode_map.end()) {
      (*ptarget) = p->second;
      ldout(cct, 20) << "make_request created, target is " << **ptarget << dendl;
    } else {
      // we got a traceless reply, and need to look up what we just
      // created.  for now, do this by name.  someday, do this by the
      // ino... which we know!  FIXME.
      Inode *target = 0;  // ptarget may be NULL
      Dentry *d = request->dentry();
      if (d) {
	// rename is special: we handle old_dentry unlink explicitly in insert_dentry_inode(), so
	// we need to compensate and do the same here.
	Dentry *od = request->old_dentry();
	if (od) {
	  unlink(od, false);
	}
	ldout(cct, 10) << "make_request got traceless reply, looking up #"
		       << d->dir->parent_inode->ino << "/" << d->name
		       << " got_ino " << got_created_ino
		       << " ino " << created_ino
		       << dendl;
	r = _do_lookup(d->dir->parent_inode, d->name, &target);
      } else {
	Inode *in = request->inode();
	ldout(cct, 10) << "make_request got traceless reply, forcing getattr on #"
		       << in->ino << dendl;
	r = _getattr(in, request->regetattr_mask, uid, gid, true);
	target = in;
      }
      if (r >= 0) {
	if (ptarget)
	  *ptarget = target;

	// verify ino returned in reply and trace_dist are the same
	if (got_created_ino &&
	    created_ino.val != target->ino.val) {
	  ldout(cct, 5) << "create got ino " << created_ino << " but then failed on lookup; EINTR?" << dendl;
	  r = -EINTR;
	}
      }
    }
  }

  return r;
}


/**
 * make a request
 *
 * Blocking helper to make an MDS request.
 *
 * If the ptarget flag is set, behavior changes slightly: the caller
 * expects to get a pointer to the inode we are creating or operating
 * on.  As a result, we will follow up any traceless mutation reply
 * with a getattr or lookup to transparently handle a traceless reply
 * from the MDS (as when the MDS restarts and the client has to replay
 * a request).
 *
 * @param request the MetaRequest to execute
 * @param uid uid to execute as
 * @param gid gid to execute as
 * @param ptarget [optional] address to store a pointer to the target inode we want to create or operate on
 * @param pcreated [optional; required if ptarget] where to store a bool of whether our create atomically created a file
 * @param use_mds [optional] prefer a specific mds (-1 for default)
 * @param pdirbl [optional; disallowed if ptarget] where to pass extra reply payload to the caller
 */
int Client::make_request(MetaRequest *request, 
			 int uid, int gid, 
			 Inode **ptarget, bool *pcreated,
			 int use_mds,
			 bufferlist *pdirbl)
{
  int r = 0;

  // assign a unique tid
  tid_t tid = ++last_tid;
  request->set_tid(tid);
  // make note
  mds_requests[tid] = request->get();
  if (uid < 0) {
    uid = geteuid();
    gid = getegid();
  }
  request->set_caller_uid(uid);
  request->set_caller_gid(gid);

  if (!mds_requests.empty()) 
    request->set_oldest_client_tid(mds_requests.begin()->first);
  else
    request->set_oldest_client_tid(tid); // this one is the oldest.

  // hack target mds?
  if (use_mds >= 0)
    request->resend_mds = use_mds;

  while (1) {
    // set up wait cond
    Cond caller_cond;
    request->caller_cond = &caller_cond;

    // choose mds
    int mds = choose_target_mds(request);
    if (mds < 0 || !mdsmap->is_active_or_stopping(mds)) {
      ldout(cct, 10) << " target mds." << mds << " not active, waiting for new mdsmap" << dendl;
      wait_on_list(waiting_for_mdsmap);
      continue;
    }

    // open a session?
    MetaSession *session = NULL;
    if (!have_open_session(mds)) {
      if (!mdsmap->is_active_or_stopping(mds)) {
	ldout(cct, 10) << "no address for mds." << mds << ", waiting for new mdsmap" << dendl;
	wait_on_list(waiting_for_mdsmap);

	if (!mdsmap->is_active_or_stopping(mds)) {
	  ldout(cct, 10) << "hmm, still have no address for mds." << mds << ", trying a random mds" << dendl;
	  request->resend_mds = mdsmap->get_random_up_mds();
	  continue;
	}
      }
      
      session = _get_or_open_mds_session(mds);

      // wait
      if (session->state == MetaSession::STATE_OPENING) {
	ldout(cct, 10) << "waiting for session to mds." << mds << " to open" << dendl;
	wait_on_context_list(session->waiting_for_open);
	continue;
      }

      if (!have_open_session(mds))
	continue;
    } else {
      session = mds_sessions[mds];
    }

    // send request.
    send_request(request, session);

    // wait for signal
    ldout(cct, 20) << "awaiting reply|forward|kick on " << &caller_cond << dendl;
    request->kick = false;
    while (!request->reply &&         // reply
	   request->resend_mds < 0 && // forward
	   !request->kick)
      caller_cond.Wait(client_lock);
    request->caller_cond = NULL;

    // did we get a reply?
    if (request->reply) 
      break;
  }

  // got it!
  MClientReply *reply = request->reply;
  request->reply = NULL;
  r = reply->get_result();

  // kick dispatcher (we've got it!)
  assert(request->dispatch_cond);
  request->dispatch_cond->Signal();
  ldout(cct, 20) << "sendrecv kickback on tid " << tid << " " << request->dispatch_cond << dendl;
  request->dispatch_cond = 0;
  
  if (r >= 0 && ptarget)
    r = verify_reply_trace(r, request, reply, ptarget, pcreated, uid, gid);

  if (pdirbl)
    pdirbl->claim(reply->get_extra_bl());

  // -- log times --
  utime_t lat = ceph_clock_now(cct);
  lat -= request->sent_stamp;
  ldout(cct, 20) << "lat " << lat << dendl;
  logger->tinc(l_c_lat, lat);
  logger->tinc(l_c_reply, lat);

  put_request(request);

  reply->put();
  return r;
}

void Client::put_request(MetaRequest *request)
{
  if (request->get_num_ref() == 1) {
    if (request->inode())
      put_inode(request->take_inode());
    if (request->old_inode())
      put_inode(request->take_old_inode());
    if (request->other_inode())
      put_inode(request->take_other_inode());
  }
  request->_put();
}

int Client::encode_inode_release(Inode *in, MetaRequest *req,
			 int mds, int drop,
			 int unless, int force)
{
  ldout(cct, 20) << "encode_inode_release enter(in:" << *in << ", req:" << req
	   << " mds:" << mds << ", drop:" << drop << ", unless:" << unless
	   << ", have:" << ", force:" << force << ")" << dendl;
  int released = 0;
  Cap *caps = NULL;
  if (in->caps.count(mds))
    caps = in->caps[mds];
  if (caps &&
      (drop & caps->issued) &&
      !(unless & caps->issued)) {
    ldout(cct, 25) << "Dropping caps. Initial " << ccap_string(caps->issued) << dendl;
    caps->issued &= ~drop;
    caps->implemented &= ~drop;
    released = 1;
    force = 1;
    ldout(cct, 25) << "Now have: " << ccap_string(caps->issued) << dendl;
  }
  if (force && caps) {
    ceph_mds_request_release rel;
    rel.ino = in->ino;
    rel.cap_id = caps->cap_id;
    rel.seq = caps->seq;
    rel.issue_seq = caps->issue_seq;
    rel.mseq = caps->mseq;
    rel.caps = caps->issued;
    rel.wanted = caps->wanted;
    rel.dname_len = 0;
    rel.dname_seq = 0;
    req->cap_releases.push_back(MClientRequest::Release(rel,""));
  }
  ldout(cct, 25) << "encode_inode_release exit(in:" << *in << ") released:"
	   << released << dendl;
  return released;
}

void Client::encode_dentry_release(Dentry *dn, MetaRequest *req,
			   int mds, int drop, int unless)
{
  ldout(cct, 20) << "encode_dentry_release enter(dn:"
	   << dn << ")" << dendl;
  int released = 0;
  if (dn->dir)
    released = encode_inode_release(dn->dir->parent_inode, req,
				    mds, drop, unless, 1);
  if (released && dn->lease_mds == mds) {
    ldout(cct, 25) << "preemptively releasing dn to mds" << dendl;
    MClientRequest::Release& rel = req->cap_releases.back();
    rel.item.dname_len = dn->name.length();
    rel.item.dname_seq = dn->lease_seq;
    rel.dname = dn->name;
  }
  ldout(cct, 25) << "encode_dentry_release exit(dn:"
	   << dn << ")" << dendl;
}


/*
 * This requires the MClientRequest *request member to be set.
 * It will error out horribly without one.
 * Additionally, if you set any *drop member, you'd better have
 * set the corresponding dentry!
 */
void Client::encode_cap_releases(MetaRequest *req, int mds)
{
  ldout(cct, 20) << "encode_cap_releases enter (req: "
		 << req << ", mds: " << mds << ")" << dendl;
  if (req->inode_drop && req->inode())
    encode_inode_release(req->inode(), req,
			 mds, req->inode_drop,
			 req->inode_unless);
  
  if (req->old_inode_drop && req->old_inode())
    encode_inode_release(req->old_inode(), req,
			 mds, req->old_inode_drop,
			 req->old_inode_unless);
  if (req->other_inode_drop && req->other_inode())
    encode_inode_release(req->other_inode(), req,
			 mds, req->other_inode_drop,
			 req->other_inode_unless);
  
  if (req->dentry_drop && req->dentry())
    encode_dentry_release(req->dentry(), req,
			  mds, req->dentry_drop,
			  req->dentry_unless);
  
  if (req->old_dentry_drop && req->old_dentry())
    encode_dentry_release(req->old_dentry(), req,
			  mds, req->old_dentry_drop,
			  req->old_dentry_unless);
  ldout(cct, 25) << "encode_cap_releases exit (req: "
	   << req << ", mds " << mds <<dendl;
}

bool Client::have_open_session(int mds)
{
  return
    mds_sessions.count(mds) &&
    mds_sessions[mds]->state == MetaSession::STATE_OPEN;
}

MetaSession *Client::_get_mds_session(int mds, Connection *con)
{
  if (mds_sessions.count(mds) == 0)
    return NULL;
  MetaSession *s = mds_sessions[mds];
  if (s->con != con)
    return NULL;
  return s;
}

MetaSession *Client::_get_or_open_mds_session(int mds)
{
  if (mds_sessions.count(mds))
    return mds_sessions[mds];
  return _open_mds_session(mds);
}

MetaSession *Client::_open_mds_session(int mds)
{
  ldout(cct, 10) << "_open_mds_session mds." << mds << dendl;
  assert(mds_sessions.count(mds) == 0);
  MetaSession *session = new MetaSession;
  session->mds_num = mds;
  session->seq = 0;
  session->inst = mdsmap->get_inst(mds);
  session->con = messenger->get_connection(session->inst);
  session->state = MetaSession::STATE_OPENING;
  mds_sessions[mds] = session;
  messenger->send_message(new MClientSession(CEPH_SESSION_REQUEST_OPEN),
			  session->con);
  return session;
}

void Client::_close_mds_session(MetaSession *s)
{
  ldout(cct, 2) << "_close_mds_session mds." << s->mds_num << " seq " << s->seq << dendl;
  s->state = MetaSession::STATE_CLOSING;
  messenger->send_message(new MClientSession(CEPH_SESSION_REQUEST_CLOSE, s->seq),
			  s->con);
}

void Client::_closed_mds_session(MetaSession *s)
{
  s->state = MetaSession::STATE_CLOSED;
  messenger->mark_down(s->con);
  signal_context_list(s->waiting_for_open);
  mount_cond.Signal();
  remove_session_caps(s);
  kick_requests(s, true);
  mds_sessions.erase(s->mds_num);
  delete s;
}

void Client::handle_client_session(MClientSession *m) 
{
  int from = m->get_source().num();
  ldout(cct, 10) << "handle_client_session " << *m << " from mds." << from << dendl;

  MetaSession *session = _get_mds_session(from, m->get_connection().get());
  if (!session) {
    ldout(cct, 10) << " discarding session message from sessionless mds " << m->get_source_inst() << dendl;
    m->put();
    return;
  }

  switch (m->get_op()) {
  case CEPH_SESSION_OPEN:
    renew_caps(session);
    session->state = MetaSession::STATE_OPEN;
    if (!unmounting) {
      connect_mds_targets(from);
    }
    signal_context_list(session->waiting_for_open);
    break;

  case CEPH_SESSION_CLOSE:
    _closed_mds_session(session);
    break;

  case CEPH_SESSION_RENEWCAPS:
    if (session->cap_renew_seq == m->get_seq()) {
      session->cap_ttl =
	session->last_cap_renew_request + mdsmap->get_session_timeout();
      wake_inode_waiters(session);
    }
    break;

  case CEPH_SESSION_STALE:
    renew_caps(session);
    break;

  case CEPH_SESSION_RECALL_STATE:
    trim_caps(session, m->get_max_caps());
    break;

  default:
    assert(0);
  }

  m->put();
}

void Client::send_request(MetaRequest *request, MetaSession *session)
{
  // make the request
  int mds = session->mds_num;
  ldout(cct, 10) << "send_request rebuilding request " << request->get_tid()
		 << " for mds." << mds << dendl;
  MClientRequest *r = build_client_request(request);
  if (request->dentry()) {
    r->set_dentry_wanted();
  }
  if (request->got_unsafe) {
    r->set_replayed_op();
  } else {
    encode_cap_releases(request, mds);
    r->releases.swap(request->cap_releases);
  }
  r->set_mdsmap_epoch(mdsmap->get_epoch());

  if (request->mds == -1) {
    request->sent_stamp = ceph_clock_now(cct);
    ldout(cct, 20) << "send_request set sent_stamp to " << request->sent_stamp << dendl;
  }
  request->mds = mds;

  Inode *in = request->inode();
  if (in && in->caps.count(mds))
    request->sent_on_mseq = in->caps[mds]->mseq;

  session->requests.push_back(&request->item);

  ldout(cct, 10) << "send_request " << *r << " to mds." << mds << dendl;
  messenger->send_message(r, session->con);
}

MClientRequest* Client::build_client_request(MetaRequest *request)
{
  MClientRequest *req = new MClientRequest(request->get_op());
  req->set_tid(request->tid);
  memcpy(&req->head, &request->head, sizeof(ceph_mds_request_head));

  // if the filepath's haven't been set, set them!
  if (request->path.empty()) {
    Inode *in = request->inode();
    Dentry *de = request->dentry();
    if (in)
      in->make_nosnap_relative_path(request->path);
    else if (de) {
      if (de->inode)
	de->inode->make_nosnap_relative_path(request->path);
      else if (de->dir) {
	de->dir->parent_inode->make_nosnap_relative_path(request->path);
	request->path.push_dentry(de->name);
      }
      else ldout(cct, 1) << "Warning -- unable to construct a filepath!"
		   << " No path, inode, or appropriately-endowed dentry given!"
		   << dendl;
    } else ldout(cct, 1) << "Warning -- unable to construct a filepath!"
		   << " No path, inode, or dentry given!"
		   << dendl;
  }
  req->set_filepath(request->get_filepath());
  req->set_filepath2(request->get_filepath2());
  req->set_data(request->data);
  req->set_retry_attempt(request->retry_attempt);
  req->head.num_fwd = request->num_fwd;
  return req;
}



void Client::handle_client_request_forward(MClientRequestForward *fwd)
{
  int mds = fwd->get_source().num();
  MetaSession *session = _get_mds_session(mds, fwd->get_connection().get());
  if (!session) {
    fwd->put();
    return;
  }
  tid_t tid = fwd->get_tid();

  if (mds_requests.count(tid) == 0) {
    ldout(cct, 10) << "handle_client_request_forward no pending request on tid " << tid << dendl;
    fwd->put();
    return;
  }

  MetaRequest *request = mds_requests[tid];
  assert(request);

  // reset retry counter
  request->retry_attempt = 0;

  // request not forwarded, or dest mds has no session.
  // resend.
  ldout(cct, 10) << "handle_client_request tid " << tid
	   << " fwd " << fwd->get_num_fwd() 
	   << " to mds." << fwd->get_dest_mds() 
	   << ", resending to " << fwd->get_dest_mds()
	   << dendl;
  
  request->mds = -1;
  request->num_fwd = fwd->get_num_fwd();
  request->resend_mds = fwd->get_dest_mds();
  request->caller_cond->Signal();

  fwd->put();
}

void Client::handle_client_reply(MClientReply *reply)
{
  int mds_num = reply->get_source().num();
  MetaSession *session = _get_mds_session(mds_num, reply->get_connection().get());
  if (!session) {
    reply->put();
    return;
  }

  tid_t tid = reply->get_tid();
  bool is_safe = reply->is_safe();

  if (mds_requests.count(tid) == 0) {
    lderr(cct) << "handle_client_reply no pending request on tid " << tid
	       << " safe is:" << is_safe << dendl;
    reply->put();
    return;
  }

  ldout(cct, 20) << "handle_client_reply got a reply. Safe:" << is_safe
		 << " tid " << tid << dendl;
  MetaRequest *request = mds_requests[tid];
  if (!request) {
    ldout(cct, 0) << "got an unknown reply (probably duplicate) on tid " << tid << " from mds "
      << mds_num << " safe: " << is_safe << dendl;
    reply->put();
    return;
  }
    
  if (request->got_unsafe && !is_safe) {
    //duplicate response
    ldout(cct, 0) << "got a duplicate reply on tid " << tid << " from mds "
	    << mds_num << " safe:" << is_safe << dendl;
    reply->put();
    return;
  }

  if (-ESTALE == reply->get_result()) { // see if we can get to proper MDS
    ldout(cct, 20) << "got ESTALE on tid " << request->tid
		   << " from mds." << request->mds << dendl;
    request->send_to_auth = true;
    request->resend_mds = choose_target_mds(request);
    Inode *in = request->inode();
    if (request->resend_mds >= 0 &&
	request->resend_mds == request->mds &&
	(in == NULL ||
	 in->caps.count(request->resend_mds) == 0 ||
	 request->sent_on_mseq == in->caps[request->resend_mds]->mseq)) {
      // have to return ESTALE
    } else {
      request->caller_cond->Signal();
      return;
    }
    ldout(cct, 20) << "have to return ESTALE" << dendl;
  }
  
  assert(request->reply == NULL);
  request->reply = reply;
  insert_trace(request, session);

  // Handle unsafe reply
  if (!is_safe) {
    request->got_unsafe = true;
    session->unsafe_requests.push_back(&request->unsafe_item);
  }

  // Only signal the caller once (on the first reply):
  // Either its an unsafe reply, or its a safe reply and no unsafe reply was sent.
  if (!is_safe || !request->got_unsafe) {
    Cond cond;
    request->dispatch_cond = &cond;

    // wake up waiter
    ldout(cct, 20) << "handle_client_reply signalling caller " << (void*)request->caller_cond << dendl;
    request->caller_cond->Signal();

    // wake for kick back
    while (request->dispatch_cond) {
      ldout(cct, 20) << "handle_client_reply awaiting kickback on tid " << tid << " " << &cond << dendl;
      cond.Wait(client_lock);
    }
  }

  if (is_safe) {
    // the filesystem change is committed to disk
    // we're done, clean up
    if (request->got_unsafe) {
      request->unsafe_item.remove_myself();
    }
    request->item.remove_myself();
    mds_requests.erase(tid);
    put_request(request);
  }
  if (unmounting)
    mount_cond.Signal();
}


// ------------------------
// incoming messages

bool Client::ms_dispatch(Message *m)
{
  Mutex::Locker l(client_lock);
  if (!initialized) {
    ldout(cct, 10) << "inactive, discarding " << *m << dendl;
    m->put();
    return true;
  }

  switch (m->get_type()) {
    // osd
  case CEPH_MSG_OSD_OPREPLY:
    objecter->handle_osd_op_reply((MOSDOpReply*)m);
    break;
  case CEPH_MSG_OSD_MAP:
    objecter->handle_osd_map((class MOSDMap*)m);
    break;
  case CEPH_MSG_STATFS_REPLY:
    objecter->handle_fs_stats_reply((MStatfsReply*)m);
    break;

    
    // mounting and mds sessions
  case CEPH_MSG_MDS_MAP:
    handle_mds_map(static_cast<MMDSMap*>(m));
    break;
  case CEPH_MSG_CLIENT_SESSION:
    handle_client_session(static_cast<MClientSession*>(m));
    break;

    // requests
  case CEPH_MSG_CLIENT_REQUEST_FORWARD:
    handle_client_request_forward(static_cast<MClientRequestForward*>(m));
    break;
  case CEPH_MSG_CLIENT_REPLY:
    handle_client_reply(static_cast<MClientReply*>(m));
    break;

  case CEPH_MSG_CLIENT_SNAP:
    handle_snap(static_cast<MClientSnap*>(m));
    break;
  case CEPH_MSG_CLIENT_CAPS:
    handle_caps(static_cast<MClientCaps*>(m));
    break;
  case CEPH_MSG_CLIENT_LEASE:
    handle_lease(static_cast<MClientLease*>(m));
    break;

  default:
    return false;
  }

  // unmounting?
  if (unmounting) {
    ldout(cct, 10) << "unmounting: trim pass, size was " << lru.lru_get_size() 
             << "+" << inode_map.size() << dendl;
    long unsigned size = lru.lru_get_size() + inode_map.size();
    trim_cache();
    if (size < lru.lru_get_size() + inode_map.size()) {
      ldout(cct, 10) << "unmounting: trim pass, cache shrank, poking unmount()" << dendl;
      mount_cond.Signal();
    } else {
      ldout(cct, 10) << "unmounting: trim pass, size still " << lru.lru_get_size() 
               << "+" << inode_map.size() << dendl;
    }
  }

  return true;
}


void Client::handle_mds_map(MMDSMap* m)
{
  if (m->get_epoch() < mdsmap->get_epoch()) {
    ldout(cct, 1) << "handle_mds_map epoch " << m->get_epoch() << " is older than our "
	    << mdsmap->get_epoch() << dendl;
    m->put();
    return;
  }  

  ldout(cct, 1) << "handle_mds_map epoch " << m->get_epoch() << dendl;

  MDSMap *oldmap = mdsmap;
  mdsmap = new MDSMap;
  mdsmap->decode(m->get_encoded());

  // reset session
  for (map<int,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    int oldstate = oldmap->get_state(p->first);
    int newstate = mdsmap->get_state(p->first);
    if (!mdsmap->is_up(p->first) ||
	mdsmap->get_inst(p->first) != p->second->inst) {
      messenger->mark_down(p->second->con);
      if (mdsmap->is_up(p->first))
	p->second->inst = mdsmap->get_inst(p->first);
    } else if (oldstate == newstate)
      continue;  // no change
    
    if (newstate == MDSMap::STATE_RECONNECT &&
	mds_sessions.count(p->first)) {
      MetaSession *session = mds_sessions[p->first];
      session->inst = mdsmap->get_inst(p->first);
      session->con = messenger->get_connection(session->inst);
      send_reconnect(session);
    }

    if (newstate >= MDSMap::STATE_ACTIVE) {
      if (oldstate < MDSMap::STATE_ACTIVE) {
	kick_requests(p->second, false);
	kick_flushing_caps(p->second);
	signal_context_list(p->second->waiting_for_open);
	kick_maxsize_requests(p->second);
	wake_inode_waiters(p->second);
      }
      connect_mds_targets(p->first);
    }
  }

  // kick any waiting threads
  signal_cond_list(waiting_for_mdsmap);

  delete oldmap;
  m->put();

  monclient->sub_got("mdsmap", mdsmap->get_epoch());
}

void Client::send_reconnect(MetaSession *session)
{
  int mds = session->mds_num;
  ldout(cct, 10) << "send_reconnect to mds." << mds << dendl;

  MClientReconnect *m = new MClientReconnect;

  // i have an open session.
  hash_set<inodeno_t> did_snaprealm;
  for (hash_map<vinodeno_t, Inode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    Inode *in = p->second;
    if (in->caps.count(mds)) {
      ldout(cct, 10) << " caps on " << p->first
	       << " " << ccap_string(in->caps[mds]->issued)
	       << " wants " << ccap_string(in->caps_wanted())
	       << dendl;
      filepath path;
      in->make_long_path(path);
      ldout(cct, 10) << "    path " << path << dendl;

      in->caps[mds]->seq = 0;  // reset seq.
      in->caps[mds]->issue_seq = 0;  // reset seq.
      m->add_cap(p->first.ino, 
		 in->caps[mds]->cap_id,
		 path.get_ino(), path.get_path(),   // ino
		 in->caps_wanted(), // wanted
		 in->caps[mds]->issued,     // issued
		 in->snaprealm->ino);

      if (did_snaprealm.count(in->snaprealm->ino) == 0) {
	ldout(cct, 10) << " snaprealm " << *in->snaprealm << dendl;
	m->add_snaprealm(in->snaprealm->ino, in->snaprealm->seq, in->snaprealm->parent);
	did_snaprealm.insert(in->snaprealm->ino);
      }	
    }
    if (in->exporting_mds == mds) {
      ldout(cct, 10) << " clearing exporting_caps on " << p->first << dendl;
      in->exporting_mds = -1;
      in->exporting_issued = 0;
      in->exporting_mseq = 0;
      if (!in->is_any_caps()) {
	ldout(cct, 10) << "  removing last cap, closing snaprealm" << dendl;
	in->snaprealm_item.remove_myself();
	put_snap_realm(in->snaprealm);
	in->snaprealm = 0;
      }
    }
  }
  
  // reset my cap seq number
  session->seq = 0;
  
  //connect to the mds' offload targets
  connect_mds_targets(mds);
  //make sure unsafe requests get saved
  resend_unsafe_requests(session);

  messenger->send_message(m, session->con);

  mount_cond.Signal();
}


void Client::kick_requests(MetaSession *session, bool signal)
{
  ldout(cct, 10) << "kick_requests for mds." << session->mds_num << dendl;

  for (map<tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end();
       ++p) 
    if (p->second->mds == session->mds_num) {
      if (signal) {
	// only signal caller if there is a caller
	// otherwise, let resend_unsafe handle it
	if (p->second->caller_cond) {
	  p->second->kick = true;
	  p->second->caller_cond->Signal();
	}
      } else {
	send_request(p->second, session);
      }
    }
}

void Client::resend_unsafe_requests(MetaSession *session)
{
  for (xlist<MetaRequest*>::iterator iter = session->unsafe_requests.begin();
       !iter.end();
       ++iter)
    send_request(*iter, session);
}




/************
 * leases
 */

void Client::got_mds_push(MetaSession *s)
{
  s->seq++;
  ldout(cct, 10) << " mds." << s->mds_num << " seq now " << s->seq << dendl;
  if (s->state == MetaSession::STATE_CLOSING) {
    messenger->send_message(new MClientSession(CEPH_SESSION_REQUEST_CLOSE, s->seq),
			    s->con);
  }
}

void Client::handle_lease(MClientLease *m)
{
  ldout(cct, 10) << "handle_lease " << *m << dendl;

  assert(m->get_action() == CEPH_MDS_LEASE_REVOKE);

  int mds = m->get_source().num();
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
    m->put();
    return;
  }

  got_mds_push(session);

  ceph_seq_t seq = m->get_seq();

  Inode *in;
  vinodeno_t vino(m->get_ino(), CEPH_NOSNAP);
  if (inode_map.count(vino) == 0) {
    ldout(cct, 10) << " don't have vino " << vino << dendl;
    goto revoke;
  }
  in = inode_map[vino];

  if (m->get_mask() & CEPH_LOCK_DN) {
    if (!in->dir || in->dir->dentries.count(m->dname) == 0) {
      ldout(cct, 10) << " don't have dir|dentry " << m->get_ino() << "/" << m->dname <<dendl;
      goto revoke;
    }
    Dentry *dn = in->dir->dentries[m->dname];
    ldout(cct, 10) << " revoked DN lease on " << dn << dendl;
    dn->lease_mds = -1;
  } 
  
 revoke:
  messenger->send_message(new MClientLease(CEPH_MDS_LEASE_RELEASE, seq,
					   m->get_mask(), m->get_ino(), m->get_first(), m->get_last(), m->dname),
			  m->get_source_inst());
  m->put();
}

void Client::put_inode(Inode *in, int n)
{
  ldout(cct, 10) << "put_inode on " << *in << dendl;
  int left = in->_put(n);
  if (left == 0) {
    // release any caps
    remove_all_caps(in);

    ldout(cct, 10) << "put_inode deleting " << *in << dendl;
    bool unclean = objectcacher->release_set(&in->oset);
    assert(!unclean);
    if (in->snapdir_parent)
      put_inode(in->snapdir_parent);
    inode_map.erase(in->vino());
    in->cap_item.remove_myself();
    in->snaprealm_item.remove_myself();
    if (in == root)
      root = 0;
    delete in;
  }
}

void Client::close_dir(Dir *dir)
{
  Inode *in = dir->parent_inode;
  ldout(cct, 15) << "close_dir dir " << dir << " on " << in << dendl;
  assert(dir->is_empty());
  assert(in->dir == dir);
  assert(in->dn_set.size() < 2);     // dirs can't be hard-linked
  if (!in->dn_set.empty())
    in->get_first_parent()->put();   // unpin dentry
  
  delete in->dir;
  in->dir = 0;
  put_inode(in);               // unpin inode
}

  /**
   * Don't call this with in==NULL, use get_or_create for that
   * leave dn set to default NULL unless you're trying to add
   * a new inode to a pre-created Dentry
   */
Dentry* Client::link(Dir *dir, const string& name, Inode *in, Dentry *dn)
{
  if (!dn) {
    // create a new Dentry
    dn = new Dentry;
    dn->name = name;
    
    // link to dir
    dn->dir = dir;
    dir->dentries[dn->name] = dn;
    dir->dentry_map[dn->name] = dn;
    lru.lru_insert_mid(dn);    // mid or top?

    ldout(cct, 15) << "link dir " << dir->parent_inode << " '" << name << "' to inode " << in
		   << " dn " << dn << " (new dn)" << dendl;
  } else {
    ldout(cct, 15) << "link dir " << dir->parent_inode << " '" << name << "' to inode " << in
		   << " dn " << dn << " (old dn)" << dendl;
  }

  if (in) {    // link to inode
    dn->inode = in;
    in->get();
    if (in->dir)
      dn->get();  // dir -> dn pin

    assert(in->dn_set.count(dn) == 0);

    // only one parent for directories!
    if (in->is_dir() && !in->dn_set.empty()) {
      Dentry *olddn = in->get_first_parent();
      assert(olddn->dir != dir || olddn->name != name);
      unlink(olddn, false);
    }

    in->dn_set.insert(dn);

    ldout(cct, 20) << "link  inode " << in << " parents now " << in->dn_set << dendl; 
  }
  
  return dn;
}

void Client::unlink(Dentry *dn, bool keepdir)
{
  Inode *in = dn->inode;
  ldout(cct, 15) << "unlink dir " << dn->dir->parent_inode << " '" << dn->name << "' dn " << dn
		 << " inode " << dn->inode << dendl;

  // unlink from inode
  if (in) {
    if (in->dir)
      dn->put();        // dir -> dn pin
    dn->inode = 0;
    assert(in->dn_set.count(dn));
    in->dn_set.erase(dn);
    ldout(cct, 20) << "unlink  inode " << in << " parents now " << in->dn_set << dendl; 
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
  dn->put();
}


/****
 * caps
 */

void Client::get_cap_ref(Inode *in, int cap)
{
  if ((cap & CEPH_CAP_FILE_BUFFER) &&
      in->cap_refs[CEPH_CAP_FILE_BUFFER] == 0) {
    ldout(cct, 5) << "get_cap_ref got first FILE_BUFFER ref on " << *in << dendl;
    in->get();
  }
  in->get_cap_ref(cap);
}

void Client::put_cap_ref(Inode *in, int cap)
{
  bool last = in->put_cap_ref(cap);
  if (last) {
    if (in->snapid == CEPH_NOSNAP) {
      if ((cap & CEPH_CAP_FILE_WR) &&
	  in->cap_snaps.size() &&
	  in->cap_snaps.rbegin()->second->writing) {
	ldout(cct, 10) << "put_cap_ref finishing pending cap_snap on " << *in << dendl;
	in->cap_snaps.rbegin()->second->writing = 0;
	finish_cap_snap(in, in->cap_snaps.rbegin()->second, in->caps_used());
	signal_cond_list(in->waitfor_caps);  // wake up blocked sync writers
      }
      if (cap & CEPH_CAP_FILE_BUFFER) {
	for (map<snapid_t,CapSnap*>::iterator p = in->cap_snaps.begin();
	    p != in->cap_snaps.end();
	    ++p)
	  p->second->dirty_data = 0;
	check_caps(in, false);
	signal_cond_list(in->waitfor_commit);
	ldout(cct, 5) << "put_cap_ref dropped last FILE_BUFFER ref on " << *in << dendl;
	put_inode(in);
      }
    }
    if (cap & CEPH_CAP_FILE_CACHE) {
      check_caps(in, false);
      ldout(cct, 5) << "put_cap_ref dropped last FILE_CACHE ref on " << *in << dendl;
    }
  }
}


int Client::get_caps(Inode *in, int need, int want, int *phave, loff_t endoff)
{
  while (1) {
    if (endoff > 0 &&
	(endoff >= (loff_t)in->max_size ||
	 endoff > (loff_t)(in->size << 1)) &&
	endoff > (loff_t)in->wanted_max_size) {
      ldout(cct, 10) << "wanted_max_size " << in->wanted_max_size << " -> " << endoff << dendl;
      in->wanted_max_size = endoff;
      check_caps(in, false);
    }
    
    if (endoff >= 0 && endoff > (loff_t)in->max_size) {
      ldout(cct, 10) << "waiting on max_size, endoff " << endoff << " max_size " << in->max_size << " on " << *in << dendl;
    } else if (!in->cap_snaps.empty() && in->cap_snaps.rbegin()->second->writing) {
      ldout(cct, 10) << "waiting on cap_snap write to complete" << dendl;
    } else {
      int implemented;
      int have = in->caps_issued(&implemented);
      if ((have & need) == need) {
	int butnot = want & ~(have & need);
	int revoking = implemented & ~have;
	ldout(cct, 10) << "get_caps " << *in << " have " << ccap_string(have)
		 << " need " << ccap_string(need) << " want " << ccap_string(want)
		 << " but not " << ccap_string(butnot) << " revoking " << ccap_string(revoking)
		 << dendl;
	if ((revoking & butnot) == 0) {
	  *phave = need | (have & want);
	  in->get_cap_ref(need);
	  return 0;
	}
      }
      ldout(cct, 10) << "waiting for caps need " << ccap_string(need) << " want " << ccap_string(want) << dendl;
    }
    
    wait_on_list(in->waitfor_caps);
  }
}


void Client::cap_delay_requeue(Inode *in)
{
  ldout(cct, 10) << "cap_delay_requeue on " << *in << dendl;
  in->hold_caps_until = ceph_clock_now(cct);
  in->hold_caps_until += cct->_conf->client_caps_release_delay;
  delayed_caps.push_back(&in->cap_item);
}

void Client::send_cap(Inode *in, MetaSession *session, Cap *cap,
		      int used, int want, int retain, int flush)
{
  int held = cap->issued | cap->implemented;
  int revoking = cap->implemented & ~cap->issued;
  retain &= ~revoking;
  int dropping = cap->issued & ~retain;
  int op = CEPH_CAP_OP_UPDATE;

  ldout(cct, 10) << "send_cap " << *in
	   << " mds." << session->mds_num << " seq " << cap->seq
	   << " used " << ccap_string(used)
	   << " want " << ccap_string(want)
	   << " flush " << ccap_string(flush)
	   << " retain " << ccap_string(retain)
	   << " held "<< ccap_string(held)
	   << " revoking " << ccap_string(revoking)
	   << " dropping " << ccap_string(dropping)
	   << dendl;

  cap->issued &= retain;
  cap->implemented &= cap->issued | used;

  uint64_t flush_tid = 0;
  snapid_t follows = 0;

  if (flush) {
    flush_tid = ++in->last_flush_tid;
    for (int i = 0; i < CEPH_CAP_BITS; ++i) {
      if (flush & (1<<i))
	in->flushing_cap_tid[i] = flush_tid;
    }
    follows = in->snaprealm->get_snap_context().seq;
  }
  
  MClientCaps *m = new MClientCaps(op,
				   in->ino,
				   0,
				   cap->cap_id, cap->seq,
				   cap->issued,
				   want,
				   flush,
				   cap->mseq);
  m->head.issue_seq = cap->issue_seq;
  m->set_tid(flush_tid);

  m->head.uid = in->uid;
  m->head.gid = in->gid;
  m->head.mode = in->mode;
  
  m->head.nlink = in->nlink;
  
  if (flush & CEPH_CAP_XATTR_EXCL) {
    ::encode(in->xattrs, m->xattrbl);
    m->head.xattr_version = in->xattr_version;
  }
  
  m->head.layout = in->layout;
  m->head.size = in->size;
  m->head.max_size = in->max_size;
  m->head.truncate_seq = in->truncate_seq;
  m->head.truncate_size = in->truncate_size;
  in->mtime.encode_timeval(&m->head.mtime);
  in->atime.encode_timeval(&m->head.atime);
  in->ctime.encode_timeval(&m->head.ctime);
  m->head.time_warp_seq = in->time_warp_seq;
    
  in->reported_size = in->size;
  m->set_snap_follows(follows);
  cap->wanted = want;
  if (cap == in->auth_cap) {
    m->set_max_size(in->wanted_max_size);
    in->requested_max_size = in->wanted_max_size;
    ldout(cct, 15) << "auth cap, setting max_size = " << in->requested_max_size << dendl;
  }
  messenger->send_message(m, session->con);
}


void Client::check_caps(Inode *in, bool is_delayed)
{
  unsigned wanted = in->caps_wanted();
  unsigned used = in->caps_used();

  int retain = wanted | CEPH_CAP_PIN;
  if (!unmounting) {
    if (wanted)
      retain |= CEPH_CAP_ANY;
    else
      retain |= CEPH_CAP_ANY_SHARED;
  }
  
  ldout(cct, 10) << "check_caps on " << *in
	   << " wanted " << ccap_string(wanted)
	   << " used " << ccap_string(used)
	   << " is_delayed=" << is_delayed
	   << dendl;

  if (in->snapid != CEPH_NOSNAP)
    return; //snap caps last forever, can't write
  
  if (in->caps.empty())
    return;   // guard if at end of func

  if (in->cap_snaps.size())
    flush_snaps(in);
  
  if (!is_delayed)
    cap_delay_requeue(in);
  else
    in->hold_caps_until = utime_t();

  utime_t now = ceph_clock_now(cct);

  map<int,Cap*>::iterator it = in->caps.begin();
  while (it != in->caps.end()) {
    int mds = it->first;
    Cap *cap = it->second;
    ++it;

    MetaSession *session = mds_sessions[mds];
    assert(session);

    int revoking = cap->implemented & ~cap->issued;
    
    ldout(cct, 10) << " cap mds." << mds
	     << " issued " << ccap_string(cap->issued)
	     << " implemented " << ccap_string(cap->implemented)
	     << " revoking " << ccap_string(revoking) << dendl;

    if (in->wanted_max_size > in->max_size &&
	in->wanted_max_size > in->requested_max_size &&
	cap == in->auth_cap)
      goto ack;

    /* approaching file_max? */
    if ((cap->issued & CEPH_CAP_FILE_WR) &&
	(in->size << 1) >= in->max_size &&
	(in->reported_size << 1) < in->max_size &&
	cap == in->auth_cap) {
      ldout(cct, 10) << "size " << in->size << " approaching max_size " << in->max_size
	       << ", reported " << in->reported_size << dendl;
      goto ack;
    }

    /* completed revocation? */
    if (revoking && (revoking & used) == 0) {
      ldout(cct, 10) << "completed revocation of " << ccap_string(cap->implemented & ~cap->issued) << dendl;
      goto ack;
    }

    if (!revoking && unmounting && (used == 0))
      goto ack;

    if (wanted == cap->wanted &&         // mds knows what we want.
	((cap->issued & ~retain) == 0) &&// and we don't have anything we wouldn't like
	!in->dirty_caps)                 // and we have no dirty caps
      continue;

    if (now < in->hold_caps_until) {
      ldout(cct, 10) << "delaying cap release" << dendl;
      continue;
    }

  ack:
    int flushing;
    if (in->auth_cap == cap && in->dirty_caps)
      flushing = mark_caps_flushing(in);
    else
      flushing = 0;

    send_cap(in, session, cap, used, wanted, retain, flushing);
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
  int dirty = in->caps_dirty();
  ldout(cct, 10) << "queue_cap_snap " << *in << " seq " << seq << " used " << ccap_string(used) << dendl;

  if (in->cap_snaps.size() &&
      in->cap_snaps.rbegin()->second->writing) {
    ldout(cct, 10) << "queue_cap_snap already have pending cap_snap on " << *in << dendl;
    return;
  } else if (in->caps_dirty() ||
            (used & CEPH_CAP_FILE_WR) ||
	     (dirty & CEPH_CAP_ANY_WR)) {
    in->get();
    CapSnap *capsnap = new CapSnap(in);
    in->cap_snaps[seq] = capsnap;
    capsnap->context = in->snaprealm->get_snap_context();
    capsnap->issued = in->caps_issued();
    capsnap->dirty = in->caps_dirty();  // a bit conservative?
    
    capsnap->dirty_data = (used & CEPH_CAP_FILE_BUFFER);
    
    capsnap->uid = in->uid;
    capsnap->gid = in->gid;
    capsnap->mode = in->mode;
    capsnap->xattrs = in->xattrs;
    capsnap->xattr_version = in->xattr_version;
 
    if (used & CEPH_CAP_FILE_WR) {
      ldout(cct, 10) << "queue_cap_snap WR used on " << *in << dendl;
      capsnap->writing = 1;
    } else {
      finish_cap_snap(in, capsnap, used);
    }
  } else {
    ldout(cct, 10) << "queue_cap_snap not dirty|writing on " << *in << dendl;
  }
}

void Client::finish_cap_snap(Inode *in, CapSnap *capsnap, int used)
{
  ldout(cct, 10) << "finish_cap_snap " << *in << " capsnap " << (void*)capsnap << " used " << ccap_string(used) << dendl;
  capsnap->size = in->size;
  capsnap->mtime = in->mtime;
  capsnap->atime = in->atime;
  capsnap->ctime = in->ctime;
  capsnap->time_warp_seq = in->time_warp_seq;

  if (used & CEPH_CAP_FILE_BUFFER) {
    ldout(cct, 10) << "finish_cap_snap " << *in << " cap_snap " << capsnap << " used " << used
	     << " WRBUFFER, delaying" << dendl;
  } else {
    capsnap->dirty_data = 0;
    flush_snaps(in);
  }
}

void Client::_flushed_cap_snap(Inode *in, snapid_t seq)
{
  ldout(cct, 10) << "_flushed_cap_snap seq " << seq << " on " << *in << dendl;
  assert(in->cap_snaps.count(seq));
  in->cap_snaps[seq]->dirty_data = 0;
  flush_snaps(in);
}

void Client::flush_snaps(Inode *in, bool all_again, CapSnap *again)
{
  ldout(cct, 10) << "flush_snaps on " << *in
		 << " all_again " << all_again
		 << " again " << again << dendl;
  assert(in->cap_snaps.size());

  // pick auth mds
  assert(in->auth_cap);
  MetaSession *session = in->auth_cap->session;
  int mseq = in->auth_cap->mseq;

  for (map<snapid_t,CapSnap*>::iterator p = in->cap_snaps.begin(); p != in->cap_snaps.end(); ++p) {
    CapSnap *capsnap = p->second;
    if (again) {
      // only one capsnap
      if (again != capsnap)
	continue;
    } else if (!all_again) {
      // only flush once per session
      if (capsnap->flushing_item.is_on_list())
	continue;
    }

    ldout(cct, 10) << "flush_snaps mds." << session->mds_num
	     << " follows " << p->first
	     << " size " << capsnap->size
	     << " mtime " << capsnap->mtime
	     << " dirty_data=" << capsnap->dirty_data
	     << " writing=" << capsnap->writing
	     << " on " << *in << dendl;
    if (capsnap->dirty_data || capsnap->writing)
      continue;
    
    in->auth_cap->session->flushing_capsnaps.push_back(&capsnap->flushing_item);

    capsnap->flush_tid = ++in->last_flush_tid;
    MClientCaps *m = new MClientCaps(CEPH_CAP_OP_FLUSHSNAP, in->ino, in->snaprealm->ino, 0, mseq);
    m->set_client_tid(capsnap->flush_tid);
    m->head.snap_follows = p->first;

    m->head.caps = capsnap->issued;
    m->head.dirty = capsnap->dirty;

    m->head.uid = capsnap->uid;
    m->head.gid = capsnap->gid;
    m->head.mode = capsnap->mode;

    m->head.size = capsnap->size;

    m->head.xattr_version = capsnap->xattr_version;
    ::encode(capsnap->xattrs, m->xattrbl);

    capsnap->ctime.encode_timeval(&m->head.ctime);
    capsnap->mtime.encode_timeval(&m->head.mtime);
    capsnap->atime.encode_timeval(&m->head.atime);
    m->head.time_warp_seq = capsnap->time_warp_seq;

    messenger->send_message(m, session->con);
  }
}



void Client::wait_on_list(list<Cond*>& ls)
{
  Cond cond;
  ls.push_back(&cond);
  cond.Wait(client_lock);
  ls.remove(&cond);
}

void Client::signal_cond_list(list<Cond*>& ls)
{
  for (list<Cond*>::iterator it = ls.begin(); it != ls.end(); ++it)
    (*it)->Signal();
}

void Client::wait_on_context_list(list<Context*>& ls)
{
  Cond cond;
  bool done = false;
  int r;
  ls.push_back(new C_Cond(&cond, &done, &r));
  while (!done)
    cond.Wait(client_lock);
}

void Client::signal_context_list(list<Context*>& ls)
{
  while (!ls.empty()) {
    ls.front()->complete(0);
    ls.pop_front();
  }
}

void Client::wake_inode_waiters(MetaSession *s)
{
  xlist<Cap*>::iterator iter = s->caps.begin();
  while (!iter.end()){
    signal_cond_list((*iter)->inode->waitfor_caps);
    ++iter;
  }
}


// flush dirty data (from objectcache)

class C_Client_CacheInvalidate : public Context  {
private:
  Client *client;
  Inode *inode;
  int64_t offset, length;
  bool keep_caps;
public:
  C_Client_CacheInvalidate(Client *c, Inode *in, int64_t off, int64_t len, bool keep) :
			   client(c), inode(in), offset(off), length(len), keep_caps(keep) {
    inode->get();
  }
  void finish(int r) {
    client->_async_invalidate(inode, offset, length, keep_caps);
  }
};

void Client::_async_invalidate(Inode *in, int64_t off, int64_t len, bool keep_caps)
{
  ldout(cct, 10) << "_async_invalidate " << off << "~" << len << (keep_caps ? " keep_caps" : "") << dendl;
  ino_invalidate_cb(ino_invalidate_cb_handle, in->vino(), off, len);

  client_lock.Lock();
  if (!keep_caps) {
    put_cap_ref(in, CEPH_CAP_FILE_CACHE);
  }
  put_inode(in);
  client_lock.Unlock();
  ldout(cct, 10) << "_async_invalidate " << off << "~" << len << (keep_caps ? " keep_caps" : "") << " done" << dendl;
}

void Client::_schedule_invalidate_callback(Inode *in, int64_t off, int64_t len, bool keep_caps) {

  if (ino_invalidate_cb)
    // we queue the invalidate, which calls the callback and decrements the ref
    async_ino_invalidator.queue(new C_Client_CacheInvalidate(this, in, off, len, keep_caps));
  else if (!keep_caps)
    // if not set, we just decrement the cap ref here
    in->put_cap_ref(CEPH_CAP_FILE_CACHE);
}

void Client::_invalidate_inode_cache(Inode *in, bool keep_caps)
{
  ldout(cct, 10) << "_invalidate_inode_cache " << *in << dendl;

  // invalidate our userspace inode cache
  if (cct->_conf->client_oc)
    objectcacher->release_set(&in->oset);

  _schedule_invalidate_callback(in, 0, 0, keep_caps);
}

void Client::_invalidate_inode_cache(Inode *in, int64_t off, int64_t len, bool keep_caps)
{
  ldout(cct, 10) << "_invalidate_inode_cache " << *in << " " << off << "~" << len << dendl;

  // invalidate our userspace inode cache
  if (cct->_conf->client_oc) {
    vector<ObjectExtent> ls;
    Striper::file_to_extents(cct, in->ino, &in->layout, off, len, in->truncate_size, ls);
    objectcacher->discard_set(&in->oset, ls);
  }

  _schedule_invalidate_callback(in, off, len, keep_caps);
}

void Client::_release(Inode *in)
{
  ldout(cct, 20) << "_release " << *in << dendl;
  if (in->cap_refs[CEPH_CAP_FILE_CACHE]) {
    _invalidate_inode_cache(in, false);
  }
}


class C_Client_PutInode : public Context {
  Client *client;
  Inode *in;
public:
  C_Client_PutInode(Client *c, Inode *i) : client(c), in(i) {
    in->get();
  }
  void finish(int) {
    client->put_inode(in);
  }
};

bool Client::_flush(Inode *in, Context *onfinish)
{
  ldout(cct, 10) << "_flush " << *in << dendl;

  if (!in->oset.dirty_or_tx) {
    ldout(cct, 10) << " nothing to flush" << dendl;
    if (onfinish)
      onfinish->complete(0);
    return true;
  }

  if (!onfinish) {
    onfinish = new C_Client_PutInode(this, in);
  }
  return objectcacher->flush_set(&in->oset, onfinish);
}

void Client::_flush_range(Inode *in, int64_t offset, uint64_t size)
{
  assert(client_lock.is_locked());
  if (!in->oset.dirty_or_tx) {
    ldout(cct, 10) << " nothing to flush" << dendl;
    return;
  }

  Mutex flock("Client::_flush_range flock");
  Cond cond;
  bool safe = false;
  Context *onflush = new C_SafeCond(&flock, &cond, &safe);
  bool ret = objectcacher->file_flush(&in->oset, &in->layout, in->snaprealm->get_snap_context(),
				      offset, size, onflush);
  if (!ret) {
    // wait for flush
    client_lock.Unlock();
    flock.Lock();
    while (!safe)
      cond.Wait(flock);
    flock.Unlock();
    client_lock.Lock();
  }
}

void Client::flush_set_callback(ObjectCacher::ObjectSet *oset)
{
  //  Mutex::Locker l(client_lock);
  assert(client_lock.is_locked());   // will be called via dispatch() -> objecter -> ...
  Inode *in = static_cast<Inode *>(oset->parent);
  assert(in);
  _flushed(in);
}

void Client::_flushed(Inode *in)
{
  ldout(cct, 10) << "_flushed " << *in << dendl;

  // release clean pages too, if we dont hold RDCACHE reference
  if (in->cap_refs[CEPH_CAP_FILE_CACHE] == 0) {
    _invalidate_inode_cache(in, true);
  }

  put_cap_ref(in, CEPH_CAP_FILE_BUFFER);
}



// checks common to add_update_cap, handle_cap_grant
void Client::check_cap_issue(Inode *in, Cap *cap, unsigned issued)
{
  unsigned had = in->caps_issued();

  if ((issued & CEPH_CAP_FILE_CACHE) &&
      !(had & CEPH_CAP_FILE_CACHE))
    in->cache_gen++;

  if ((issued & CEPH_CAP_FILE_SHARED) &&
      !(had & CEPH_CAP_FILE_SHARED)) {
    in->shared_gen++;

    if (in->is_dir() && (in->flags & I_COMPLETE)) {
      ldout(cct, 10) << " clearing I_COMPLETE on " << *in << dendl;
      in->flags &= ~I_COMPLETE;
    }
  }
}

void Client::add_update_cap(Inode *in, MetaSession *mds_session, uint64_t cap_id,
			    unsigned issued, unsigned seq, unsigned mseq, inodeno_t realm,
			    int flags)
{
  Cap *cap = 0;
  int mds = mds_session->mds_num;
  if (in->caps.count(mds)) {
    cap = in->caps[mds];
  } else {
    mds_session->num_caps++;
    if (!in->is_any_caps()) {
      assert(in->snaprealm == 0);
      in->snaprealm = get_snap_realm(realm);
      in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
      ldout(cct, 15) << "add_update_cap first one, opened snaprealm " << in->snaprealm << dendl;
    }
    if (in->exporting_mds == mds) {
      ldout(cct, 10) << "add_update_cap clearing exporting_caps on " << mds << dendl;
      in->exporting_mds = -1;
      in->exporting_issued = 0;
      in->exporting_mseq = 0;
    }
    in->caps[mds] = cap = new Cap;
    mds_session->caps.push_back(&cap->cap_item);
    cap->session = mds_session;
    cap->inode = in;
    cap->gen = mds_session->cap_gen;
    cap_list.push_back(&in->cap_item);
  }

  check_cap_issue(in, cap, issued);

  if (flags & CEPH_CAP_FLAG_AUTH) {
    if (in->auth_cap != cap &&
        (!in->auth_cap || in->auth_cap->mseq < mseq)) {
      if (in->auth_cap && in->flushing_cap_item.is_on_list()) {
	ldout(cct, 10) << "add_update_cap changing auth cap: removing myself from flush_caps list" << dendl;
	in->flushing_cap_item.remove_myself();
      }
      in->auth_cap = cap;
    }
  }

  unsigned old_caps = cap->issued;
  cap->cap_id = cap_id;
  cap->issued |= issued;
  cap->implemented |= issued;
  cap->seq = seq;
  cap->issue_seq = seq;
  cap->mseq = mseq;
  ldout(cct, 10) << "add_update_cap issued " << ccap_string(old_caps) << " -> " << ccap_string(cap->issued)
	   << " from mds." << mds
	   << " on " << *in
	   << dendl;

  if (issued & ~old_caps)
    signal_cond_list(in->waitfor_caps);
}

void Client::remove_cap(Cap *cap)
{
  Inode *in = cap->inode;
  MetaSession *session = cap->session;
  int mds = cap->session->mds_num;

  ldout(cct, 10) << "remove_cap mds." << mds << " on " << *in << dendl;
  
  if (!session->release)
    session->release = new MClientCapRelease;
  ceph_mds_cap_item i;
  i.ino = in->ino;
  i.cap_id = cap->cap_id;
  i.seq = cap->issue_seq;
  i.migrate_seq = cap->mseq;
  session->release->caps.push_back(i);
  
  cap->cap_item.remove_myself();

  if (in->auth_cap == cap) {
    if (in->flushing_cap_item.is_on_list()) {
      ldout(cct, 10) << " removing myself from flushing_cap list" << dendl;
      in->flushing_cap_item.remove_myself();
    }
    in->auth_cap = NULL;
  }
  assert(in->caps.count(mds));
  in->caps.erase(mds);
  delete cap;

  if (!in->is_any_caps()) {
    ldout(cct, 15) << "remove_cap last one, closing snaprealm " << in->snaprealm << dendl;
    in->snaprealm_item.remove_myself();
    put_snap_realm(in->snaprealm);
    in->snaprealm = 0;
  }
}

void Client::remove_all_caps(Inode *in)
{
  while (in->caps.size())
    remove_cap(in->caps.begin()->second);
}

void Client::remove_session_caps(MetaSession *mds) 
{
  while (mds->caps.size()) {
    Cap *cap = *mds->caps.begin();
    remove_cap(cap);
  }
}

void Client::trim_caps(MetaSession *s, int max)
{
  int mds = s->mds_num;
  ldout(cct, 10) << "trim_caps mds." << mds << " max " << max << dendl;

  int trimmed = 0;
  xlist<Cap*>::iterator p = s->caps.begin();
  while (s->caps.size() > max && !p.end()) {
    Cap *cap = *p;
    ++p;
    Inode *in = cap->inode;
    if (in->caps.size() > 1 && cap != in->auth_cap) {
      // disposable non-auth cap
      if (in->caps_used() || in->caps_dirty()) {
	ldout(cct, 20) << " keeping cap on " << *in << " used " << ccap_string(in->caps_used())
		       << " dirty " << ccap_string(in->caps_dirty()) << dendl;
	continue;
      }
      ldout(cct, 20) << " removing unused, unneeded non-auth cap on " << *in << dendl;
      remove_cap(cap);
      trimmed++;
    } else {
      ldout(cct, 20) << " trying to trim dentries for " << *in << dendl;
      bool all = true;
      set<Dentry*>::iterator q = in->dn_set.begin();
      while (q != in->dn_set.end()) {
	Dentry *dn = *q++;
	if (dn->lru_is_expireable())
	  trim_dentry(dn);
	else
	  all = false;
      }
      if (all)
	trimmed++;
    }
  }
}

void Client::mark_caps_dirty(Inode *in, int caps)
{
  ldout(cct, 10) << "mark_caps_dirty " << *in << " " << ccap_string(in->dirty_caps) << " -> "
	   << ccap_string(in->dirty_caps | caps) << dendl;
  if (caps && !in->caps_dirty())
    in->get();
  in->dirty_caps |= caps;
}

int Client::mark_caps_flushing(Inode *in)
{
  MetaSession *session = in->auth_cap->session;

  int flushing = in->dirty_caps;
  assert(flushing);

  if (flushing && !in->flushing_caps) {
    ldout(cct, 10) << "mark_caps_flushing " << ccap_string(flushing) << " " << *in << dendl;
    num_flushing_caps++;
  } else {
    ldout(cct, 10) << "mark_caps_flushing (more) " << ccap_string(flushing) << " " << *in << dendl;
  }

  in->flushing_caps |= flushing;
  in->dirty_caps = 0;
 
  in->flushing_cap_seq = ++last_flush_seq;

  session->flushing_caps.push_back(&in->flushing_cap_item);

  return flushing;
}

void Client::flush_caps()
{
  ldout(cct, 10) << "flush_caps" << dendl;
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
}

void Client::flush_caps(Inode *in, MetaSession *session)
{
  ldout(cct, 10) << "flush_caps " << in << " mds." << session->mds_num << dendl;
  Cap *cap = in->auth_cap;
  assert(cap->session == session);

  int wanted = in->caps_wanted();
  int retain = wanted | CEPH_CAP_PIN;

  send_cap(in, session, cap, in->caps_used(), wanted, retain, in->flushing_caps);
}

void Client::wait_sync_caps(uint64_t want)
{
 retry:
  ldout(cct, 10) << "wait_sync_caps want " << want << " (last is " << last_flush_seq << ", "
	   << num_flushing_caps << " total flushing)" << dendl;
  for (map<int,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    if (p->second->flushing_caps.empty())
	continue;
    Inode *in = p->second->flushing_caps.front();
    if (in->flushing_cap_seq <= want) {
      ldout(cct, 10) << " waiting on mds." << p->first << " tid " << in->flushing_cap_seq
	       << " (want " << want << ")" << dendl;
      sync_cond.Wait(client_lock);
      goto retry;
    }
  }
}

void Client::kick_flushing_caps(MetaSession *session)
{
  int mds = session->mds_num;
  ldout(cct, 10) << "kick_flushing_caps mds." << mds << dendl;

  for (xlist<CapSnap*>::iterator p = session->flushing_capsnaps.begin(); !p.end(); ++p) {
    CapSnap *capsnap = *p;
    Inode *in = capsnap->in;
    ldout(cct, 20) << " reflushing capsnap " << capsnap
		   << " on " << *in << " to mds." << mds << dendl;
    flush_snaps(in, false, capsnap);
  }
  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
    ldout(cct, 20) << " reflushing caps on " << *in << " to mds." << mds << dendl;
    if (in->flushing_caps)
      flush_caps(in, session);
  }
}

void Client::kick_maxsize_requests(MetaSession *session)
{
  xlist<Cap*>::iterator iter = session->caps.begin();
  while (!iter.end()){
    (*iter)->inode->requested_max_size = 0;
    (*iter)->inode->wanted_max_size = 0;
    signal_cond_list((*iter)->inode->waitfor_caps);
    ++iter;
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
  for (set<snapid_t>::reverse_iterator p = snaps.rbegin(); p != snaps.rend(); ++p)
    cached_snap_context.snaps.push_back(*p);
}

void Client::invalidate_snaprealm_and_children(SnapRealm *realm)
{
  list<SnapRealm*> q;
  q.push_back(realm);

  while (!q.empty()) {
    realm = q.front();
    q.pop_front();

    ldout(cct, 10) << "invalidate_snaprealm_and_children " << *realm << dendl;
    realm->invalidate_cache();

    for (set<SnapRealm*>::iterator p = realm->pchildren.begin();
	 p != realm->pchildren.end(); 
	 ++p)
      q.push_back(*p);
  }
}

SnapRealm *Client::get_snap_realm(inodeno_t r)
{
  SnapRealm *realm = snap_realms[r];
  if (!realm)
    snap_realms[r] = realm = new SnapRealm(r);
  ldout(cct, 20) << "get_snap_realm " << r << " " << realm << " " << realm->nref << " -> " << (realm->nref + 1) << dendl;
  realm->nref++;
  return realm;
}

SnapRealm *Client::get_snap_realm_maybe(inodeno_t r)
{
  if (snap_realms.count(r) == 0) {
    ldout(cct, 20) << "get_snap_realm_maybe " << r << " fail" << dendl;
    return NULL;
  }
  SnapRealm *realm = snap_realms[r];
  ldout(cct, 20) << "get_snap_realm_maybe " << r << " " << realm << " " << realm->nref << " -> " << (realm->nref + 1) << dendl;
  realm->nref++;
  return realm;
}

void Client::put_snap_realm(SnapRealm *realm)
{
  ldout(cct, 20) << "put_snap_realm " << realm->ino << " " << realm
		 << " " << realm->nref << " -> " << (realm->nref - 1) << dendl;
  if (--realm->nref == 0) {
    snap_realms.erase(realm->ino);
    delete realm;
  }
}

bool Client::adjust_realm_parent(SnapRealm *realm, inodeno_t parent)
{
  if (realm->parent != parent) {
    ldout(cct, 10) << "adjust_realm_parent " << *realm
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
  ldout(cct, 10) << "update_snap_trace len " << bl.length() << dendl;

  bufferlist::iterator p = bl.begin();
  while (!p.end()) {
    SnapRealmInfo info;
    ::decode(info, p);
    if (first_realm == 0)
      first_realm = info.ino();
    SnapRealm *realm = get_snap_realm(info.ino());

    if (info.seq() > realm->seq) {
      ldout(cct, 10) << "update_snap_trace " << *realm << " seq " << info.seq() << " > " << realm->seq
	       << dendl;

      if (flush) {
	// writeback any dirty caps _before_ updating snap list (i.e. with old snap info)
	//  flush me + children
	list<SnapRealm*> q;
	q.push_back(realm);
	while (!q.empty()) {
	  SnapRealm *realm = q.front();
	  q.pop_front();
	  ldout(cct, 10) << " flushing caps on " << *realm << dendl;
	  
	  xlist<Inode*>::iterator p = realm->inodes_with_caps.begin();
	  while (!p.end()) {
	    Inode *in = *p;
	    ++p;
	    queue_cap_snap(in, realm->get_snap_context().seq);
	  }
	  
	  for (set<SnapRealm*>::iterator p = realm->pchildren.begin(); 
	       p != realm->pchildren.end(); 
	       ++p)
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
      ldout(cct, 15) << "update_snap_trace " << *realm << " self|parent updated" << dendl;
      ldout(cct, 15) << "  snapc " << realm->get_snap_context() << dendl;
    } else {
      ldout(cct, 10) << "update_snap_trace " << *realm << " seq " << info.seq()
	       << " <= " << realm->seq << " and same parent, SKIPPING" << dendl;
    }
        
    put_snap_realm(realm);
  }

  return first_realm;
}

void Client::handle_snap(MClientSnap *m)
{
  ldout(cct, 10) << "handle_snap " << *m << dendl;
  int mds = m->get_source().num();
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
    m->put();
    return;
  }

  got_mds_push(session);

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
    ldout(cct, 10) << " splitting off " << *realm << dendl;
    for (vector<inodeno_t>::iterator p = m->split_inos.begin();
	 p != m->split_inos.end();
	 ++p) {
      vinodeno_t vino(*p, CEPH_NOSNAP);
      if (inode_map.count(vino)) {
	Inode *in = inode_map[vino];
	if (!in->snaprealm || in->snaprealm == realm)
	  continue;
	if (in->snaprealm->created > info.created()) {
	  ldout(cct, 10) << " NOT moving " << *in << " from _newer_ realm " 
		   << *in->snaprealm << dendl;
	  continue;
	}
	ldout(cct, 10) << " moving " << *in << " from " << *in->snaprealm << dendl;

	// queue for snap writeback
	queue_cap_snap(in, in->snaprealm->get_snap_context().seq);

	in->snaprealm_item.remove_myself();
	put_snap_realm(in->snaprealm);
	to_move.push_back(in);
      }
    }

    // move child snaprealms, too
    for (vector<inodeno_t>::iterator p = m->split_realms.begin();
	 p != m->split_realms.end();
	 ++p) {
      ldout(cct, 10) << "adjusting snaprealm " << *p << " parent" << dendl;
      SnapRealm *child = get_snap_realm_maybe(*p);
      if (!child)
	continue;
      adjust_realm_parent(child, realm->ino);
      put_snap_realm(child);
    }
  }

  update_snap_trace(m->bl, m->head.op != CEPH_SNAP_OP_DESTROY);

  if (realm) {
    for (list<Inode*>::iterator p = to_move.begin(); p != to_move.end(); ++p) {
      Inode *in = *p;
      in->snaprealm = realm;
      realm->inodes_with_caps.push_back(&in->snaprealm_item);
      realm->nref++;
    }
    put_snap_realm(realm);
  }

  m->put();
}

void Client::handle_caps(MClientCaps *m)
{
  int mds = m->get_source().num();
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
    m->put();
    return;
  }
  got_mds_push(session);

  m->clear_payload();  // for if/when we send back to MDS

  Inode *in = 0;
  vinodeno_t vino(m->get_ino(), CEPH_NOSNAP);
  if (inode_map.count(vino))
    in = inode_map[vino];
  if (!in) {
    if (m->get_op() == CEPH_CAP_OP_IMPORT) {
      ldout(cct, 5) << "handle_caps don't have vino " << vino << " on IMPORT, immediately releasing" << dendl;
      if (!session->release)
	session->release = new MClientCapRelease;
      ceph_mds_cap_item i;
      i.ino = m->get_ino();
      i.cap_id = m->get_cap_id();
      i.seq = m->get_seq();
      i.migrate_seq = m->get_mseq();
      session->release->caps.push_back(i);
    } else {
      ldout(cct, 5) << "handle_caps don't have vino " << vino << ", dropping" << dendl;
    }
    m->put();

    // in case the mds is waiting on e.g. a revocation
    flush_cap_releases();
    return;
  }

  switch (m->get_op()) {
  case CEPH_CAP_OP_IMPORT: return handle_cap_import(session, in, m);
  case CEPH_CAP_OP_EXPORT: return handle_cap_export(session, in, m);
  case CEPH_CAP_OP_FLUSHSNAP_ACK: return handle_cap_flushsnap_ack(session, in, m);
  }

  if (in->caps.count(mds) == 0) {
    ldout(cct, 5) << "handle_caps don't have " << *in << " cap on mds." << mds << dendl;
    m->put();
    return;
  }

  Cap *cap = in->caps[mds];

  switch (m->get_op()) {
  case CEPH_CAP_OP_TRUNC: return handle_cap_trunc(session, in, m);
  case CEPH_CAP_OP_REVOKE:
  case CEPH_CAP_OP_GRANT: return handle_cap_grant(session, in, cap, m);
  case CEPH_CAP_OP_FLUSH_ACK: return handle_cap_flush_ack(session, in, cap, m);
  default:
    m->put();
  }
}

void Client::handle_cap_import(MetaSession *session, Inode *in, MClientCaps *m)
{
  int mds = session->mds_num;

  // add/update it
  update_snap_trace(m->snapbl);
  add_update_cap(in, session, m->get_cap_id(),
		 m->get_caps(), m->get_seq(), m->get_mseq(), m->get_realm(),
		 CEPH_CAP_FLAG_AUTH);
  
  if (in->auth_cap && in->auth_cap->session->mds_num == mds) {
    // reflush any/all caps (if we are now the auth_cap)
    if (in->cap_snaps.size())
      flush_snaps(in, true);
    if (in->flushing_caps)
      flush_caps(in, session);
  }

  if (m->get_mseq() > in->exporting_mseq) {
    ldout(cct, 5) << "handle_cap_import ino " << m->get_ino() << " mseq " << m->get_mseq()
	    << " IMPORT from mds." << mds
	    << ", clearing exporting_issued " << ccap_string(in->exporting_issued) 
	    << " mseq " << in->exporting_mseq << dendl;
    in->exporting_issued = 0;
    in->exporting_mseq = 0;
    in->exporting_mds = -1;
  } else {
    ldout(cct, 5) << "handle_cap_import ino " << m->get_ino() << " mseq " << m->get_mseq()
	    << " IMPORT from mds." << mds 
	    << ", keeping exporting_issued " << ccap_string(in->exporting_issued) 
	    << " mseq " << in->exporting_mseq << " by mds." << in->exporting_mds << dendl;
  }
  m->put();
}

void Client::handle_cap_export(MetaSession *session, Inode *in, MClientCaps *m)
{
  int mds = session->mds_num;
  Cap *cap = NULL;

  // note?
  bool found_higher_mseq = false;
  for (map<int,Cap*>::iterator p = in->caps.begin();
       p != in->caps.end();
       ++p) {
    if (p->first == mds)
      cap = p->second;
    if (p->second->mseq > m->get_mseq()) {
      found_higher_mseq = true;
      ldout(cct, 5) << "handle_cap_export ino " << m->get_ino() << " mseq " << m->get_mseq() 
	      << " EXPORT from mds." << mds
	      << ", but mds." << p->first << " has higher mseq " << p->second->mseq << dendl;
    }
  }

  if (cap) {
    if (!found_higher_mseq) {
      ldout(cct, 5) << "handle_cap_export ino " << m->get_ino() << " mseq " << m->get_mseq() 
	      << " EXPORT from mds." << mds
	      << ", setting exporting_issued " << ccap_string(cap->issued) << dendl;
      in->exporting_issued = cap->issued;
      in->exporting_mseq = m->get_mseq();
      in->exporting_mds = mds;
    } else 
      ldout(cct, 5) << "handle_cap_export ino " << m->get_ino() << " mseq " << m->get_mseq() 
	      << " EXPORT from mds." << mds
	      << ", just removing old cap" << dendl;

    remove_cap(cap);
  }
  // else we already released it

  // open export targets, so we'll get the matching IMPORT, even if we
  // have seen a newer import (or have released the cap entirely), as there
  // may be an intervening revocation that will otherwise get blocked up.
  connect_mds_targets(mds);

  m->put();
}

void Client::handle_cap_trunc(MetaSession *session, Inode *in, MClientCaps *m)
{
  int mds = session->mds_num;
  assert(in->caps[mds]);

  ldout(cct, 10) << "handle_cap_trunc on ino " << *in
	   << " size " << in->size << " -> " << m->get_size()
	   << dendl;
  
  int implemented = 0;
  int issued = in->caps_issued(&implemented) | in->caps_dirty();
  issued |= implemented;
  update_inode_file_bits(in, m->get_truncate_seq(), m->get_truncate_size(),
                         m->get_size(), m->get_time_warp_seq(), m->get_ctime(),
                         m->get_mtime(), m->get_atime(), issued);
  m->put();
}

void Client::handle_cap_flush_ack(MetaSession *session, Inode *in, Cap *cap, MClientCaps *m)
{
  int mds = session->mds_num;
  int dirty = m->get_dirty();
  int cleaned = 0;
  for (int i = 0; i < CEPH_CAP_BITS; ++i) {
    if ((dirty & (1 << i)) &&
	(m->get_client_tid() == in->flushing_cap_tid[i]))
      cleaned |= 1 << i;
  }

  ldout(cct, 5) << "handle_cap_flush_ack mds." << mds
	  << " cleaned " << ccap_string(cleaned) << " on " << *in
	  << " with " << ccap_string(dirty) << dendl;


  if (!cleaned) {
    ldout(cct, 10) << " tid " << m->get_client_tid() << " != any cap bit tids" << dendl;
  } else {
    if (in->flushing_caps) {
      ldout(cct, 5) << "  flushing_caps " << ccap_string(in->flushing_caps)
	      << " -> " << ccap_string(in->flushing_caps & ~cleaned) << dendl;
      in->flushing_caps &= ~cleaned;
      if (in->flushing_caps == 0) {
	ldout(cct, 10) << " " << *in << " !flushing" << dendl;
	in->flushing_cap_item.remove_myself();
	num_flushing_caps--;
	sync_cond.Signal();
      }
      if (!in->caps_dirty())
	put_inode(in);
    }
  }
  
  m->put();
}


void Client::handle_cap_flushsnap_ack(MetaSession *session, Inode *in, MClientCaps *m)
{
  int mds = session->mds_num;
  assert(in->caps[mds]);
  snapid_t follows = m->get_snap_follows();

  if (in->cap_snaps.count(follows)) {
    CapSnap *capsnap = in->cap_snaps[follows];
    if (m->get_client_tid() != capsnap->flush_tid) {
      ldout(cct, 10) << " tid " << m->get_client_tid() << " != " << capsnap->flush_tid << dendl;
    } else {
      ldout(cct, 5) << "handle_cap_flushedsnap mds." << mds << " flushed snap follows " << follows
	      << " on " << *in << dendl;
      capsnap->flushing_item.remove_myself();
      delete capsnap;
      in->cap_snaps.erase(follows);
      put_inode(in);
    }
  } else {
    ldout(cct, 5) << "handle_cap_flushedsnap DUP(?) mds." << mds << " flushed snap follows " << follows
	    << " on " << *in << dendl;
    // we may not have it if we send multiple FLUSHSNAP requests and (got multiple FLUSHEDSNAPs back)
  }
    
  m->put();
}


void Client::handle_cap_grant(MetaSession *session, Inode *in, Cap *cap, MClientCaps *m)
{
  int mds = session->mds_num;
  int used = in->caps_used();

  const int old_caps = cap->issued;
  const int new_caps = m->get_caps();
  ldout(cct, 5) << "handle_cap_grant on in " << m->get_ino() 
          << " mds." << mds << " seq " << m->get_seq() 
          << " caps now " << ccap_string(new_caps) 
          << " was " << ccap_string(old_caps) << dendl;

  cap->seq = m->get_seq();

  in->layout = m->get_layout();
  
  // update inode
  int implemented = 0;
  int issued = in->caps_issued(&implemented) | in->caps_dirty();
  issued |= implemented;

  if ((issued & CEPH_CAP_AUTH_EXCL) == 0) {
    in->mode = m->head.mode;
    in->uid = m->head.uid;
    in->gid = m->head.gid;
  }
  if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
    in->nlink = m->head.nlink;
  }
  if ((issued & CEPH_CAP_XATTR_EXCL) == 0 &&
      m->xattrbl.length() &&
      m->head.xattr_version > in->xattr_version) {
    bufferlist::iterator p = m->xattrbl.begin();
    ::decode(in->xattrs, p);
    in->xattr_version = m->head.xattr_version;
  }
  update_inode_file_bits(in, m->get_truncate_seq(), m->get_truncate_size(), m->get_size(),
			 m->get_time_warp_seq(), m->get_ctime(), m->get_mtime(), m->get_atime(), issued);

  // max_size
  if (cap == in->auth_cap &&
      m->get_max_size() != in->max_size) {
    ldout(cct, 10) << "max_size " << in->max_size << " -> " << m->get_max_size() << dendl;
    in->max_size = m->get_max_size();
    if (in->max_size > in->wanted_max_size) {
      in->wanted_max_size = 0;
      in->requested_max_size = 0;
    }
  }

  check_cap_issue(in, cap, issued);

  // update caps
  if (old_caps & ~new_caps) { 
    ldout(cct, 10) << "  revocation of " << ccap_string(~new_caps & old_caps) << dendl;
    cap->issued = new_caps;
    cap->implemented |= new_caps;

    if ((~cap->issued & old_caps) & CEPH_CAP_FILE_CACHE)
      _release(in);
    
    if (((used & ~new_caps) & CEPH_CAP_FILE_BUFFER) &&
	!_flush(in)) {
      // waitin' for flush
    } else {
      cap->wanted = 0; // don't let check_caps skip sending a response to MDS
      check_caps(in, true);
    }

  } else if (old_caps == new_caps) {
    ldout(cct, 10) << "  caps unchanged at " << ccap_string(old_caps) << dendl;
  } else {
    ldout(cct, 10) << "  grant, new caps are " << ccap_string(new_caps & ~old_caps) << dendl;
    cap->issued = new_caps;
    cap->implemented |= new_caps;
  }

  // wake up waiters
  if (new_caps)
    signal_cond_list(in->waitfor_caps);

  m->put();
}

int Client::check_permissions(Inode *in, int flags, int uid, int gid)
{
  gid_t *sgids = NULL;
  int sgid_count = 0;
  if (getgroups_cb) {
    sgid_count = getgroups_cb(getgroups_cb_handle, uid, &sgids);
    if (sgid_count < 0) {
      ldout(cct, 3) << "getgroups failed!" << dendl;
      return sgid_count;
    }
  }
  // check permissions before doing anything else
  if (uid != 0 && !in->check_mode(uid, gid, sgids, sgid_count, flags)) {
    return -EACCES;
  }
  return 0;
}


// -------------------
// MOUNT

int Client::mount(const std::string &mount_root)
{
  Mutex::Locker lock(client_lock);

  if (mounted) {
    ldout(cct, 5) << "already mounted" << dendl;;
    return 0;
  }

  client_lock.Unlock();
  int r = monclient->authenticate(cct->_conf->client_mount_timeout);
  client_lock.Lock();
  if (r < 0)
    return r;
  
  whoami = monclient->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  mounted = true;

  tick(); // start tick
  
  ldout(cct, 2) << "mounted: have osdmap " << osdmap->get_epoch() 
	  << " and mdsmap " << mdsmap->get_epoch() 
	  << dendl;

  
  // hack: get+pin root inode.
  //  fuse assumes it's always there.
  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_GETATTR);
  filepath fp(CEPH_INO_ROOT);
  if (!mount_root.empty())
    fp = filepath(mount_root.c_str());
  req->set_filepath(fp);
  req->head.args.getattr.mask = CEPH_STAT_CAP_INODE_ALL;
  int res = make_request(req, -1, -1);
  ldout(cct, 10) << "root getattr result=" << res << dendl;
  if (res < 0)
    return res;

  assert(root);
  _ll_get(root);

  // trace?
  if (!cct->_conf->client_trace.empty()) {
    traceout.open(cct->_conf->client_trace.c_str());
    if (traceout.is_open()) {
      ldout(cct, 1) << "opened trace file '" << cct->_conf->client_trace << "'" << dendl;
    } else {
      ldout(cct, 1) << "FAILED to open trace file '" << cct->_conf->client_trace << "'" << dendl;
    }
  }

  /*
  ldout(cct, 3) << "op: // client trace data structs" << dendl;
  ldout(cct, 3) << "op: struct stat st;" << dendl;
  ldout(cct, 3) << "op: struct utimbuf utim;" << dendl;
  ldout(cct, 3) << "op: int readlinkbuf_len = 1000;" << dendl;
  ldout(cct, 3) << "op: char readlinkbuf[readlinkbuf_len];" << dendl;
  ldout(cct, 3) << "op: map<string, inode_t*> dir_contents;" << dendl;
  ldout(cct, 3) << "op: map<int, int> open_files;" << dendl;
  ldout(cct, 3) << "op: int fd;" << dendl;
  */
  return 0;
}

// UNMOUNT

void Client::unmount()
{
  Mutex::Locker lock(client_lock);

  assert(mounted);  // caller is confused?

  ldout(cct, 2) << "unmounting" << dendl;
  unmounting = true;

  while (!mds_requests.empty()) {
    ldout(cct, 10) << "waiting on " << mds_requests.size() << " requests" << dendl;
    mount_cond.Wait(client_lock);
  }

  if (tick_event)
    timer.cancel_event(tick_event);
  tick_event = 0;

  if (cwd)
    put_inode(cwd);
  cwd = NULL;

  // clean up any unclosed files
  while (!fd_map.empty()) {
    Fh *fh = fd_map.begin()->second;
    fd_map.erase(fd_map.begin());
    ldout(cct, 0) << " destroying lost open file " << fh << " on " << *fh->inode << dendl;
    _release_fh(fh);
  }

  _ll_drop_pins();

  while (unsafe_sync_write > 0) {
    ldout(cct, 0) << unsafe_sync_write << " unsafe_sync_writes, waiting"  << dendl;
    mount_cond.Wait(client_lock);
  }

  if (cct->_conf->client_oc) {
    // flush/release all buffered data
    hash_map<vinodeno_t, Inode*>::iterator next;
    for (hash_map<vinodeno_t, Inode*>::iterator p = inode_map.begin();
         p != inode_map.end(); 
         p = next) {
      next = p;
      ++next;
      Inode *in = p->second;
      if (!in) {
	ldout(cct, 0) << "null inode_map entry ino " << p->first << dendl;
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

  flush_caps();
  wait_sync_caps(last_flush_seq);

  // empty lru cache
  lru.lru_set_max(0);
  trim_cache();

  while (lru.lru_get_size() > 0 || 
         !inode_map.empty()) {
    ldout(cct, 2) << "cache still has " << lru.lru_get_size() 
            << "+" << inode_map.size() << " items" 
	    << ", waiting (for caps to release?)"
            << dendl;
    utime_t until = ceph_clock_now(cct) + utime_t(5, 0);
    int r = mount_cond.WaitUntil(client_lock, until);
    if (r == ETIMEDOUT) {
      dump_cache(NULL);
    }
  }
  assert(lru.lru_get_size() == 0);
  assert(inode_map.empty());

  // stop tracing
  if (!cct->_conf->client_trace.empty()) {
    ldout(cct, 1) << "closing trace file '" << cct->_conf->client_trace << "'" << dendl;
    traceout.close();
  }

  
  while (!mds_sessions.empty()) {
    // send session closes!
    for (map<int,MetaSession*>::iterator p = mds_sessions.begin();
	p != mds_sessions.end();
	++p) {
      if (p->second->state != MetaSession::STATE_CLOSING) {
	_close_mds_session(p->second);
      }
    }

    // wait for sessions to close
    ldout(cct, 2) << "waiting for " << mds_sessions.size() << " mds sessions to close" << dendl;
    mount_cond.Wait(client_lock);
  }

  mounted = false;

  ldout(cct, 2) << "unmounted." << dendl;
}



class C_C_Tick : public Context {
  Client *client;
public:
  C_C_Tick(Client *c) : client(c) {}
  void finish(int r) {
    client->tick();
  }
};

void Client::flush_cap_releases()
{
  // send any cap releases
  for (map<int,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    if (p->second->release && mdsmap->is_clientreplay_or_active_or_stopping(p->first)) {
      messenger->send_message(p->second->release, p->second->con);
      p->second->release = 0;
    }
  }
}

void Client::tick()
{
  if (cct->_conf->client_debug_inject_tick_delay > 0) {
    sleep(cct->_conf->client_debug_inject_tick_delay);
    assert(0 == cct->_conf->set_val("client_debug_inject_tick_delay", "0"));
    cct->_conf->apply_changes(NULL);
  }

  ldout(cct, 21) << "tick" << dendl;
  tick_event = new C_C_Tick(this);
  timer.add_event_after(cct->_conf->client_tick_interval, tick_event);

  utime_t now = ceph_clock_now(cct);

  if (mdsmap->get_epoch()) {
    // renew caps?
    utime_t el = now - last_cap_renew;
    if (el > mdsmap->get_session_timeout() / 3.0)
      renew_caps();

    flush_cap_releases();
  }

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
  ldout(cct, 10) << "renew_caps()" << dendl;
  last_cap_renew = ceph_clock_now(cct);
  
  for (map<int,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    ldout(cct, 15) << "renew_caps requesting from mds." << p->first << dendl;
    if (mdsmap->get_state(p->first) >= MDSMap::STATE_REJOIN)
      renew_caps(p->second);
  }
}

void Client::renew_caps(MetaSession *session)
{
  ldout(cct, 10) << "renew_caps mds." << session->mds_num << dendl;
  session->last_cap_renew_request = ceph_clock_now(cct);
  uint64_t seq = ++session->cap_renew_seq;
  messenger->send_message(new MClientSession(CEPH_SESSION_REQUEST_RENEWCAPS, seq),
			  session->con);
}


// ===============================================================
// high level (POSIXy) interface

int Client::_do_lookup(Inode *dir, const string& name, Inode **target)
{
  int op = dir->snapid == CEPH_SNAPDIR ? CEPH_MDS_OP_LOOKUPSNAP : CEPH_MDS_OP_LOOKUP;
  MetaRequest *req = new MetaRequest(op);
  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);
  req->set_inode(dir);
  req->head.args.getattr.mask = 0;
  ldout(cct, 10) << "_do_lookup on " << path << dendl;

  int r = make_request(req, 0, 0, target);
  ldout(cct, 10) << "_do_lookup res is " << r << dendl;
  return r;
}

int Client::_lookup(Inode *dir, const string& dname, Inode **target)
{
  int r = 0;

  if (!dir->is_dir()) {
    r = -ENOTDIR;
    goto done;
  }

  if (dname == "..") {
    if (dir->dn_set.empty())
      r = -ENOENT;
    else
      *target = dir->get_first_parent()->dir->parent_inode; //dirs can't be hard-linked
    goto done;
  }

  if (dname == ".") {
    *target = dir;
    goto done;
  }

  if (dname.length() > NAME_MAX) {
    r = -ENAMETOOLONG;
    goto done;
  }
  
  if (dname == cct->_conf->client_snapdir &&
      dir->snapid == CEPH_NOSNAP) {
    *target = open_snapdir(dir);
    goto done;
  }

  if (dir->dir &&
      dir->dir->dentries.count(dname)) {
    Dentry *dn = dir->dir->dentries[dname];

    ldout(cct, 20) << "_lookup have dn " << dname << " mds." << dn->lease_mds << " ttl " << dn->lease_ttl
	     << " seq " << dn->lease_seq
	     << dendl;

    // is dn lease valid?
    utime_t now = ceph_clock_now(cct);
    if (dn->lease_mds >= 0 && 
	dn->lease_ttl > now &&
	mds_sessions.count(dn->lease_mds)) {
      MetaSession *s = mds_sessions[dn->lease_mds];
      if (s->cap_ttl > now &&
	  s->cap_gen == dn->lease_gen) {
	*target = dn->inode;
	// touch this mds's dir cap too, even though we don't _explicitly_ use it here, to
	// make trim_caps() behave.
	dir->try_touch_cap(dn->lease_mds);
	touch_dn(dn);
	goto done;
      }
      ldout(cct, 20) << " bad lease, cap_ttl " << s->cap_ttl << ", cap_gen " << s->cap_gen
	       << " vs lease_gen " << dn->lease_gen << dendl;
    }
    // dir lease?
    if (dir->caps_issued_mask(CEPH_CAP_FILE_SHARED) &&
	dn->cap_shared_gen == dir->shared_gen) {
      *target = dn->inode;
      touch_dn(dn);
      goto done;
    }
  } else {
    // can we conclude ENOENT locally?
    if (dir->caps_issued_mask(CEPH_CAP_FILE_SHARED) &&
	(dir->flags & I_COMPLETE)) {
      ldout(cct, 10) << "_lookup concluded ENOENT locally for " << *dir << " dn '" << dname << "'" << dendl;
      return -ENOENT;
    }
  }

  r = _do_lookup(dir, dname, target);

 done:
  if (r < 0)
    ldout(cct, 10) << "_lookup " << *dir << " " << dname << " = " << r << dendl;
  else
    ldout(cct, 10) << "_lookup " << *dir << " " << dname << " = " << **target << dendl;
  return r;
}

int Client::get_or_create(Inode *dir, const char* name,
			  Dentry **pdn, bool expect_null)
{
  // lookup
  ldout(cct, 20) << "get_or_create " << *dir << " name " << name << dendl;
  dir->open_dir();
  if (dir->dir->dentries.count(name)) {
    Dentry *dn = dir->dir->dentries[name];
    
    // is dn lease valid?
    utime_t now = ceph_clock_now(cct);
    if (dn->inode &&
	dn->lease_mds >= 0 && 
	dn->lease_ttl > now &&
	mds_sessions.count(dn->lease_mds)) {
      MetaSession *s = mds_sessions[dn->lease_mds];
      if (s->cap_ttl > now &&
	  s->cap_gen == dn->lease_gen) {
	if (expect_null)
	  return -EEXIST;
      }
    }
    *pdn = dn;
  } else {
    // otherwise link up a new one
    *pdn = link(dir->dir, name, NULL, NULL);
  }

  // success
  return 0;
}

int Client::path_walk(const filepath& origpath, Inode **final, bool followsym)
{
  filepath path = origpath;
  Inode *cur;
  if (origpath.absolute())
    cur = root;
  else
    cur = cwd;
  assert(cur);

  ldout(cct, 10) << "path_walk " << path << dendl;

  int symlinks = 0;

  unsigned i=0;
  while (i < path.depth() && cur) {
    const string &dname = path[i];
    ldout(cct, 10) << " " << i << " " << *cur << " " << dname << dendl;
    ldout(cct, 20) << "  (path is " << path << ")" << dendl;
    Inode *next;
    int r = _lookup(cur, dname, &next);
    if (r < 0)
      return r;
    // only follow trailing symlink if followsym.  always follow
    // 'directory' symlinks.
    if (next && next->is_symlink()) {
      symlinks++;
      ldout(cct, 20) << " symlink count " << symlinks << ", value is '" << next->symlink << "'" << dendl;
      if (symlinks > MAXSYMLINKS) {
	return -ELOOP;
      }

      if (i < path.depth() - 1) {
	// dir symlink
	// replace consumed components of path with symlink dir target
	filepath resolved(next->symlink.c_str());
	resolved.append(path.postfixpath(i + 1));
	path = resolved;
	i = 0;
	if (next->symlink[0] == '/') {
	  cur = root;
	}
	continue;
      } else if (followsym) {
	if (next->symlink[0] == '/') {
	  path = next->symlink.c_str();
	  i = 0;
	  // reset position
	  cur = root;
	} else {
	  filepath more(next->symlink.c_str());
	  // we need to remove the symlink component from off of the path
	  // before adding the target that the symlink points to.  remain
	  // at the same position in the path.
	  path.pop_dentry();
	  path.append(more);
	}
	continue;
      }
    }
    cur = next;
    i++;
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
  tout(cct) << "link" << std::endl;
  tout(cct) << relexisting << std::endl;
  tout(cct) << relpath << std::endl;

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
 out_unlock:
  put_inode(in);
 out:
  return r;
}

int Client::unlink(const char *relpath)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "unlink" << std::endl;
  tout(cct) << relpath << std::endl;

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
  tout(cct) << "rename" << std::endl;
  tout(cct) << relfrom << std::endl;
  tout(cct) << relto << std::endl;

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
  todir->get();
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
  tout(cct) << "mkdir" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;
  ldout(cct, 10) << "mkdir: " << relpath << dendl;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  Inode *dir;
  int r = path_walk(path, &dir);
  if (r < 0) {
    return r;
  }
  return _mkdir(dir, name.c_str(), mode);
}

int Client::mkdirs(const char *relpath, mode_t mode)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 10) << "Client::mkdirs " << relpath << dendl;
  tout(cct) << "mkdirs" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;

  //get through existing parts of path
  filepath path(relpath);
  unsigned int i;
  int r=0;
  Inode *cur = cwd;
  Inode *next;
  for (i=0; i<path.depth(); ++i) {
    r=_lookup(cur, path[i].c_str(), &next);
    if (r < 0) break;
    cur = next;
  }
  //check that we have work left to do
  if (i==path.depth()) return -EEXIST;
  if (r!=-ENOENT) return r;
  ldout(cct, 20) << "mkdirs got through " << i << " directories on path " << relpath << dendl;
  //make new directory at each level
  for (; i<path.depth(); ++i) {
    //make new dir
    r = _mkdir(cur, path[i].c_str(), mode);
    //check proper creation/existence
    if (r < 0) return r;
    r = _lookup(cur, path[i], &next);
    if(r < 0) {
      ldout(cct, 0) << "mkdirs: successfully created new directory " << path[i]
	      << " but can't _lookup it!" << dendl;
      return r;
    }
    //move to new dir and continue
    cur = next;
    ldout(cct, 20) << "mkdirs: successfully created directory "
	     << filepath(cur->ino).get_path() << dendl;
  }
  return 0;
}

int Client::rmdir(const char *relpath)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "rmdir" << std::endl;
  tout(cct) << relpath << std::endl;
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
  tout(cct) << "mknod" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << rdev << std::endl;
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
  tout(cct) << "symlink" << std::endl;
  tout(cct) << target << std::endl;
  tout(cct) << relpath << std::endl;

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
  tout(cct) << "readlink" << std::endl;
  tout(cct) << relpath << std::endl;

  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in, false);
  if (r < 0)
    return r;
  
  if (!in->is_symlink())
    return -EINVAL;

  // copy into buf (at most size bytes)
  r = in->symlink.length();
  if (r > size)
    r = size;
  memcpy(buf, in->symlink.c_str(), r);
  return r;
}


// inode stuff

int Client::_getattr(Inode *in, int mask, int uid, int gid, bool force)
{
  bool yes = in->caps_issued_mask(mask);

  ldout(cct, 10) << "_getattr mask " << ccap_string(mask) << " issued=" << yes << dendl;
  if (yes && !force)
    return 0;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_GETATTR);
  filepath path;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_inode(in);
  req->head.args.getattr.mask = mask;
  
  int res = make_request(req, uid, gid);
  ldout(cct, 10) << "_getattr result=" << res << dendl;
  return res;
}

int Client::_setattr(Inode *in, struct stat *attr, int mask, int uid, int gid, Inode **inp)
{
  int issued = in->caps_issued();

  ldout(cct, 10) << "_setattr mask " << mask << " issued " << ccap_string(issued) << dendl;

  if (in->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  // make the change locally?

  if (!mask) {
    // caller just needs us to bump the ctime
    in->ctime = ceph_clock_now(cct);
    if (issued & CEPH_CAP_AUTH_EXCL)
      mark_caps_dirty(in, CEPH_CAP_AUTH_EXCL);
    else if (issued & CEPH_CAP_FILE_EXCL)
      mark_caps_dirty(in, CEPH_CAP_FILE_EXCL);
    else if (issued & CEPH_CAP_XATTR_EXCL)
      mark_caps_dirty(in, CEPH_CAP_XATTR_EXCL);
    else
      mask |= CEPH_SETATTR_CTIME;
  }

  if (in->caps_issued_mask(CEPH_CAP_AUTH_EXCL)) {
    if (mask & CEPH_SETATTR_MODE) {
      in->ctime = ceph_clock_now(cct);
      in->mode = (in->mode & ~07777) | (attr->st_mode & 07777);
      mark_caps_dirty(in, CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_MODE;
    }
    if (mask & CEPH_SETATTR_UID) {
      in->ctime = ceph_clock_now(cct);
      in->uid = attr->st_uid;
      mark_caps_dirty(in, CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_UID;
    }
    if (mask & CEPH_SETATTR_GID) {
      in->ctime = ceph_clock_now(cct);
      in->gid = attr->st_gid;
      mark_caps_dirty(in, CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_GID;
    }
  }
  if (in->caps_issued_mask(CEPH_CAP_FILE_EXCL)) {
    if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME)) {
      if (mask & CEPH_SETATTR_MTIME)
	in->mtime = utime_t(attr->st_mtim.tv_sec, attr->st_mtim.tv_nsec);
      if (mask & CEPH_SETATTR_ATIME)
	in->atime = utime_t(attr->st_atim.tv_sec, attr->st_atim.tv_nsec);
      in->ctime = ceph_clock_now(cct);
      in->time_warp_seq++;
      mark_caps_dirty(in, CEPH_CAP_FILE_EXCL);
      mask &= ~(CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME);
    }
  }
  if (!mask)
    return 0;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_SETATTR);

  filepath path;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_inode(in);

  if (mask & CEPH_SETATTR_MODE) {
    req->head.args.setattr.mode = attr->st_mode;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
  }
  if (mask & CEPH_SETATTR_UID) {
    req->head.args.setattr.uid = attr->st_uid;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
  }
  if (mask & CEPH_SETATTR_GID) {
    req->head.args.setattr.gid = attr->st_gid;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
  }
  if (mask & CEPH_SETATTR_MTIME) {
    req->head.args.setattr.mtime =
      utime_t(attr->st_mtim.tv_sec, attr->st_mtim.tv_nsec);
    req->inode_drop |= CEPH_CAP_AUTH_SHARED | CEPH_CAP_FILE_RD |
      CEPH_CAP_FILE_WR;
  }
  if (mask & CEPH_SETATTR_ATIME) {
    req->head.args.setattr.atime =
      utime_t(attr->st_atim.tv_sec, attr->st_atim.tv_nsec);
    req->inode_drop |= CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_RD |
      CEPH_CAP_FILE_WR;
  }
  if (mask & CEPH_SETATTR_SIZE) {
    if ((unsigned long)attr->st_size < mdsmap->get_max_filesize())
      req->head.args.setattr.size = attr->st_size;
    else { //too big!
      put_request(req);
      return -EFBIG;
    }
    req->inode_drop |= CEPH_CAP_AUTH_SHARED | CEPH_CAP_FILE_RD |
      CEPH_CAP_FILE_WR;
  }
  req->head.args.setattr.mask = mask;

  req->regetattr_mask = mask;

  int res = make_request(req, uid, gid, inp);
  ldout(cct, 10) << "_setattr result=" << res << dendl;
  return res;
}

int Client::setattr(const char *relpath, struct stat *attr, int mask)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "setattr" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mask  << std::endl;

  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  return _setattr(in, attr, mask); 
}

int Client::fsetattr(int fd, struct stat *attr, int mask)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "fsetattr" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << mask  << std::endl;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return _setattr(f->inode, attr, mask); 
}

int Client::stat(const char *relpath, struct stat *stbuf,
			  frag_info_t *dirstat, int mask)
{
  ldout(cct, 3) << "stat enter (relpath" << relpath << " mask " << mask << ")" << dendl;
  Mutex::Locker lock(client_lock);
  tout(cct) << "stat" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  r = _getattr(in, mask);
  if (r < 0) {
    ldout(cct, 3) << "stat exit on error!" << dendl;
    return r;
  }
  fill_stat(in, stbuf, dirstat);
  ldout(cct, 3) << "stat exit (relpath" << relpath << " mask " << mask << ")" << dendl;
  return r;
}

int Client::lstat(const char *relpath, struct stat *stbuf,
			  frag_info_t *dirstat, int mask)
{
  ldout(cct, 3) << "lstat enter (relpath" << relpath << " mask " << mask << ")" << dendl;
  Mutex::Locker lock(client_lock);
  tout(cct) << "lstat" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  Inode *in;
  // don't follow symlinks
  int r = path_walk(path, &in, false);
  if (r < 0)
    return r;
  r = _getattr(in, mask);
  if (r < 0) {
    ldout(cct, 3) << "lstat exit on error!" << dendl;
    return r;
  }
  fill_stat(in, stbuf, dirstat);
  ldout(cct, 3) << "lstat exit (relpath" << relpath << " mask " << mask << ")" << dendl;
  return r;
}

int Client::fill_stat(Inode *in, struct stat *st, frag_info_t *dirstat, nest_info_t *rstat)
{
  ldout(cct, 10) << "fill_stat on " << in->ino << " snap/dev" << in->snapid
	   << " mode 0" << oct << in->mode << dec
	   << " mtime " << in->mtime << " ctime " << in->ctime << dendl;
  memset(st, 0, sizeof(struct stat));
  st->st_ino = in->ino;
  st->st_dev = in->snapid;
  st->st_mode = in->mode;
  st->st_rdev = in->rdev;
  st->st_nlink = in->nlink;
  st->st_uid = in->uid;
  st->st_gid = in->gid;
  if (in->ctime.sec() > in->mtime.sec()) {
    st->st_ctim.tv_sec = in->ctime.sec();
    st->st_ctim.tv_nsec = in->ctime.nsec();
  } else {
    st->st_ctim.tv_sec = in->mtime.sec();
    st->st_ctim.tv_nsec = in->mtime.nsec();
  }
  st->st_atim.tv_sec = in->atime.sec();
  st->st_atim.tv_nsec = in->atime.nsec();
  st->st_mtim.tv_sec = in->mtime.sec();
  st->st_mtim.tv_nsec = in->mtime.nsec();
  if (in->is_dir()) {
    //st->st_size = in->dirstat.size();
    st->st_size = in->rstat.rbytes;
    st->st_blocks = 1;
  } else {
    st->st_size = in->size;
    st->st_blocks = (in->size + 511) >> 9;
  }
  st->st_blksize = MAX(in->layout.fl_stripe_unit, 4096);

  if (dirstat)
    *dirstat = in->dirstat;
  if (rstat)
    *rstat = in->rstat;

  return in->caps_issued();
}

void Client::touch_dn(Dentry *dn)
{
  lru.lru_touch(dn);
}

int Client::chmod(const char *relpath, mode_t mode)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "chmod" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_mode = mode;
  return _setattr(in, &attr, CEPH_SETATTR_MODE);
}

int Client::fchmod(int fd, mode_t mode)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "fchmod" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << mode << std::endl;
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  struct stat attr;
  attr.st_mode = mode;
  return _setattr(f->inode, &attr, CEPH_SETATTR_MODE);
}

int Client::lchmod(const char *relpath, mode_t mode)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "lchmod" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;
  filepath path(relpath);
  Inode *in;
  // don't follow symlinks
  int r = path_walk(path, &in, false);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_mode = mode;
  return _setattr(in, &attr, CEPH_SETATTR_MODE);
}

int Client::chown(const char *relpath, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "chown" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << uid << std::endl;
  tout(cct) << gid << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_uid = uid;
  attr.st_gid = gid;
  int mask = 0;
  if (uid != -1) mask |= CEPH_SETATTR_UID;
  if (gid != -1) mask |= CEPH_SETATTR_GID;
  return _setattr(in, &attr, mask);
}

int Client::fchown(int fd, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "fchown" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << uid << std::endl;
  tout(cct) << gid << std::endl;
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  struct stat attr;
  attr.st_uid = uid;
  attr.st_gid = gid;
  int mask = 0;
  if (uid != -1) mask |= CEPH_SETATTR_UID;
  if (gid != -1) mask |= CEPH_SETATTR_GID;
  return _setattr(f->inode, &attr, mask);
}

int Client::lchown(const char *relpath, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "lchown" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << uid << std::endl;
  tout(cct) << gid << std::endl;
  filepath path(relpath);
  Inode *in;
  // don't follow symlinks
  int r = path_walk(path, &in, false);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_uid = uid;
  attr.st_gid = gid;
  int mask = 0;
  if (uid != -1) mask |= CEPH_SETATTR_UID;
  if (gid != -1) mask |= CEPH_SETATTR_GID;
  return _setattr(in, &attr, mask);
}

int Client::utime(const char *relpath, struct utimbuf *buf)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "utime" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << buf->modtime << std::endl;
  tout(cct) << buf->actime << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_mtim.tv_sec = buf->modtime;
  attr.st_mtim.tv_nsec = 0;
  attr.st_atim.tv_sec = buf->actime;
  attr.st_atim.tv_nsec = 0;
  return _setattr(in, &attr, CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME);
}

int Client::lutime(const char *relpath, struct utimbuf *buf)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "lutime" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << buf->modtime << std::endl;
  tout(cct) << buf->actime << std::endl;
  filepath path(relpath);
  Inode *in;
  // don't follow symlinks
  int r = path_walk(path, &in, false);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_mtim.tv_sec = buf->modtime;
  attr.st_mtim.tv_nsec = 0;
  attr.st_atim.tv_sec = buf->actime;
  attr.st_atim.tv_nsec = 0;
  return _setattr(in, &attr, CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME);
}

int Client::opendir(const char *relpath, dir_result_t **dirpp) 
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "opendir" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  r = _opendir(in, dirpp);
  tout(cct) << (unsigned long)*dirpp << std::endl;
  return r;
}

int Client::_opendir(Inode *in, dir_result_t **dirpp, int uid, int gid) 
{
  if (!in->is_dir())
    return -ENOTDIR;
  *dirpp = new dir_result_t(in);
  (*dirpp)->set_frag(in->dirfragtree[0]);
  if (in->dir)
    (*dirpp)->release_count = in->dir->release_count;
  (*dirpp)->start_shared_gen = in->shared_gen;
  ldout(cct, 10) << "_opendir " << in->ino << ", our cache says the first dirfrag is " << (*dirpp)->frag() << dendl;
  ldout(cct, 3) << "_opendir(" << in->ino << ") = " << 0 << " (" << *dirpp << ")" << dendl;
  return 0;
}


int Client::closedir(dir_result_t *dir) 
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "closedir" << std::endl;
  tout(cct) << (unsigned long)dir << std::endl;

  ldout(cct, 3) << "closedir(" << dir << ") = 0" << dendl;
  _closedir(dir);
  return 0;
}

void Client::_closedir(dir_result_t *dirp)
{
  ldout(cct, 10) << "_closedir(" << dirp << ")" << dendl;
  if (dirp->inode) {
    ldout(cct, 10) << "_closedir detaching inode " << dirp->inode << dendl;
    put_inode(dirp->inode);
    dirp->inode = 0;
  }
  _readdir_drop_dirp_buffer(dirp);
  delete dirp;
}

void Client::rewinddir(dir_result_t *dirp)
{
  Mutex::Locker lock(client_lock);

  ldout(cct, 3) << "rewinddir(" << dirp << ")" << dendl;
  dir_result_t *d = static_cast<dir_result_t*>(dirp);
  _readdir_drop_dirp_buffer(d);
  d->reset();
}
 
loff_t Client::telldir(dir_result_t *dirp)
{
  dir_result_t *d = static_cast<dir_result_t*>(dirp);
  ldout(cct, 3) << "telldir(" << dirp << ") = " << d->offset << dendl;
  return d->offset;
}

void Client::seekdir(dir_result_t *dirp, loff_t offset)
{
  Mutex::Locker lock(client_lock);

  ldout(cct, 3) << "seekdir(" << dirp << ", " << offset << ")" << dendl;
  dir_result_t *d = static_cast<dir_result_t*>(dirp);

  if (offset == 0 ||
      dir_result_t::fpos_frag(offset) != d->frag() ||
      dir_result_t::fpos_off(offset) < d->fragpos()) {
    _readdir_drop_dirp_buffer(d);
    d->reset();
  }

  if (offset > d->offset)
    d->release_count--;   // bump if we do a forward seek

  d->offset = offset;
  if (!d->frag().is_leftmost() && d->next_offset == 2)
    d->next_offset = 0;  // not 2 on non-leftmost frags!
}





//struct dirent {
//  ino_t          d_ino;       /* inode number */
//  off_t          d_off;       /* offset to the next dirent */
//  unsigned short d_reclen;    /* length of this record */
//  unsigned char  d_type;      /* type of file */
//  char           d_name[256]; /* filename */
//};
void Client::fill_dirent(struct dirent *de, const char *name, int type, uint64_t ino, loff_t next_off)
{
  strncpy(de->d_name, name, 255);
  de->d_name[255] = '\0';
#ifndef __CYGWIN__
  de->d_ino = ino;
#if !defined(DARWIN) && !defined(__FreeBSD__)
  de->d_off = next_off;
#endif
  de->d_reclen = 1;
  de->d_type = IFTODT(type);
  ldout(cct, 10) << "fill_dirent '" << de->d_name << "' -> " << inodeno_t(de->d_ino)
	   << " type " << (int)de->d_type << " w/ next_off " << hex << next_off << dec << dendl;
#endif
}

void Client::_readdir_next_frag(dir_result_t *dirp)
{
  frag_t fg = dirp->frag();

  // advance
  dirp->next_frag();
  if (dirp->at_end()) {
    ldout(cct, 10) << "_readdir_next_frag advance from " << fg << " to END" << dendl;
  } else {
    ldout(cct, 10) << "_readdir_next_frag advance from " << fg << " to " << dirp->frag() << dendl;
    _readdir_rechoose_frag(dirp);
  }
}

void Client::_readdir_rechoose_frag(dir_result_t *dirp)
{
  assert(dirp->inode);
  frag_t cur = dirp->frag();
  frag_t f = dirp->inode->dirfragtree[cur.value()];
  if (f != cur) {
    ldout(cct, 10) << "_readdir_rechoose_frag frag " << cur << " maps to " << f << dendl;
    dirp->set_frag(f);
  }
}

void Client::_readdir_drop_dirp_buffer(dir_result_t *dirp)
{
  ldout(cct, 10) << "_readdir_drop_dirp_buffer " << dirp << dendl;
  if (dirp->buffer) {
    for (unsigned i = 0; i < dirp->buffer->size(); i++)
      put_inode((*dirp->buffer)[i].second);
    delete dirp->buffer;
    dirp->buffer = NULL;
  }
}

int Client::_readdir_get_frag(dir_result_t *dirp)
{
  assert(dirp);
  assert(dirp->inode);

  // get the current frag.
  frag_t fg = dirp->frag();
  
  ldout(cct, 10) << "_readdir_get_frag " << dirp << " on " << dirp->inode->ino << " fg " << fg
	   << " next_offset " << dirp->next_offset
	   << dendl;

  int op = CEPH_MDS_OP_READDIR;
  if (dirp->inode && dirp->inode->snapid == CEPH_SNAPDIR)
    op = CEPH_MDS_OP_LSSNAP;

  Inode *diri = dirp->inode;

  MetaRequest *req = new MetaRequest(op);
  filepath path;
  diri->make_nosnap_relative_path(path);
  req->set_filepath(path); 
  req->set_inode(diri);
  req->head.args.readdir.frag = fg;
  if (dirp->last_name.length()) {
    req->path2.set_path(dirp->last_name.c_str());
    req->readdir_start = dirp->last_name;
  }
  req->readdir_offset = dirp->next_offset;
  req->readdir_frag = fg;
  
  
  bufferlist dirbl;
  int res = make_request(req, -1, -1, NULL, NULL, -1, &dirbl);
  
  if (res == -EAGAIN) {
    ldout(cct, 10) << "_readdir_get_frag got EAGAIN, retrying" << dendl;
    _readdir_rechoose_frag(dirp);
    return _readdir_get_frag(dirp);
  }

  if (res == 0) {
    // stuff dir contents to cache, dir_result_t
    assert(diri);

    _readdir_drop_dirp_buffer(dirp);

    dirp->buffer = new vector<pair<string,Inode*> >;
    dirp->buffer->swap(req->readdir_result);

    if (fg != req->readdir_reply_frag) {
      fg = req->readdir_reply_frag;
      if (fg.is_leftmost())
	dirp->next_offset = 2;
      else
	dirp->next_offset = 0;
      dirp->offset = dir_result_t::make_fpos(fg, dirp->next_offset);
    }
    dirp->buffer_frag = fg;
    dirp->this_offset = dirp->next_offset;
    ldout(cct, 10) << "_readdir_get_frag " << dirp << " got frag " << dirp->buffer_frag
	     << " this_offset " << dirp->this_offset
	     << " size " << dirp->buffer->size() << dendl;

    if (req->readdir_end) {
      dirp->last_name.clear();
      if (fg.is_rightmost())
	dirp->next_offset = 2;
      else
	dirp->next_offset = 0;
    } else {
      dirp->last_name = req->readdir_last_name;
      dirp->next_offset += req->readdir_num;
    }
  } else {
    ldout(cct, 10) << "_readdir_get_frag got error " << res << ", setting end flag" << dendl;
    dirp->set_end();
  }

  return res;
}

int Client::_readdir_cache_cb(dir_result_t *dirp, add_dirent_cb_t cb, void *p)
{
  assert(client_lock.is_locked());
  ldout(cct, 10) << "_readdir_cache_cb " << dirp << " on " << dirp->inode->ino
	   << " at_cache_name " << dirp->at_cache_name << " offset " << hex << dirp->offset << dec
	   << dendl;
  Dir *dir = dirp->inode->dir;

  if (!dir) {
    ldout(cct, 10) << " dir is empty" << dendl;
    dirp->set_end();
    return 0;
  }

  map<string,Dentry*>::iterator pd;
  if (dirp->at_cache_name.length()) {
    pd = dir->dentry_map.find(dirp->at_cache_name);
    if (pd == dir->dentry_map.end())
      return -EAGAIN;  // weird, i give up
    ++pd;
  } else {
    pd = dir->dentry_map.begin();
  }

  string prev_name;
  while (pd != dir->dentry_map.end()) {
    Dentry *dn = pd->second;
    if (dn->inode == NULL) {
      ldout(cct, 15) << " skipping null '" << pd->first << "'" << dendl;
      ++pd;
      continue;
    }

    struct stat st;
    struct dirent de;
    int stmask = fill_stat(dn->inode, &st);  
    fill_dirent(&de, pd->first.c_str(), st.st_mode, st.st_ino, dirp->offset + 1);
      
    uint64_t next_off = dn->offset + 1;
    ++pd;
    if (pd == dir->dentry_map.end())
      next_off = dir_result_t::END;

    client_lock.Unlock();
    int r = cb(p, &de, &st, stmask, next_off);  // _next_ offset
    client_lock.Lock();
    ldout(cct, 15) << " de " << de.d_name << " off " << hex << dn->offset << dec
	     << " = " << r
	     << dendl;
    if (r < 0) {
      dirp->next_offset = dn->offset;
      dirp->at_cache_name = prev_name;
      return r;
    }

    prev_name = dn->name;
    dirp->offset = next_off;
  }

  ldout(cct, 10) << "_readdir_cache_cb " << dirp << " on " << dirp->inode->ino << " at end" << dendl;
  dirp->set_end();
  return 0;
}

int Client::readdir_r_cb(dir_result_t *d, add_dirent_cb_t cb, void *p)
{
  Mutex::Locker lock(client_lock);

  dir_result_t *dirp = static_cast<dir_result_t*>(d);

  ldout(cct, 10) << "readdir_r_cb " << *dirp->inode << " offset " << hex << dirp->offset << dec
	   << " frag " << dirp->frag() << " fragpos " << hex << dirp->fragpos() << dec
	   << " at_end=" << dirp->at_end()
	   << dendl;

  struct dirent de;
  struct stat st;
  memset(&de, 0, sizeof(de));
  memset(&st, 0, sizeof(st));

  frag_t fg = dirp->frag();
  uint32_t off = dirp->fragpos();

  Inode *diri = dirp->inode;

  if (dirp->at_end())
    return 0;

  if (dirp->offset == 0) {
    ldout(cct, 15) << " including ." << dendl;
    assert(diri->dn_set.size() < 2); // can't have multiple hard-links to a dir
    uint64_t next_off = 1;

    fill_dirent(&de, ".", S_IFDIR, diri->ino, next_off);

    fill_stat(diri, &st);

    client_lock.Unlock();
    int r = cb(p, &de, &st, -1, next_off);
    client_lock.Lock();
    if (r < 0)
      return r;

    dirp->offset = next_off;
    off = next_off;
  }
  if (dirp->offset == 1) {
    ldout(cct, 15) << " including .." << dendl;
    if (!diri->dn_set.empty()) {
      Inode* in = diri->get_first_parent()->inode;
      fill_dirent(&de, "..", S_IFDIR, in->ino, 2);
      fill_stat(in, &st);
    } else {
      /* must be at the root (no parent),
       * so we add the dotdot with a special inode (3) */
      fill_dirent(&de, "..", S_IFDIR, CEPH_INO_DOTDOT, 2);
    }


    client_lock.Unlock();
    int r = cb(p, &de, &st, -1, 2);
    client_lock.Lock();
    if (r < 0)
      return r;

    dirp->offset = 2;
    off = 2;
  }

  // can we read from our cache?
  ldout(cct, 10) << "offset " << hex << dirp->offset << dec << " at_cache_name " << dirp->at_cache_name
	   << " snapid " << dirp->inode->snapid << " complete " << (bool)(dirp->inode->flags & I_COMPLETE)
	   << " issued " << ccap_string(dirp->inode->caps_issued())
	   << dendl;
  if ((dirp->offset == 2 || dirp->at_cache_name.length()) &&
      dirp->inode->snapid != CEPH_SNAPDIR &&
      (dirp->inode->flags & I_COMPLETE) &&
      dirp->inode->caps_issued_mask(CEPH_CAP_FILE_SHARED)) {
    int err = _readdir_cache_cb(dirp, cb, p);
    if (err != -EAGAIN)
      return err;
  }					    
  if (dirp->at_cache_name.length()) {
    dirp->last_name = dirp->at_cache_name;
    dirp->at_cache_name.clear();
  }

  while (1) {
    if (dirp->at_end())
      return 0;

    if (dirp->buffer_frag != dirp->frag() || dirp->buffer == NULL) {
      int r = _readdir_get_frag(dirp);
      if (r)
	return r;
      // _readdir_get_frag () may updates dirp->offset if the replied dirfrag is
      // different than the requested one. (our dirfragtree was outdated)
      fg = dirp->buffer_frag;
      off = dirp->fragpos();
    }

    ldout(cct, 10) << "off " << off << " this_offset " << hex << dirp->this_offset << dec << " size " << dirp->buffer->size()
	     << " frag " << fg << dendl;

    dirp->offset = dir_result_t::make_fpos(fg, off);
    while (off >= dirp->this_offset &&
	   off - dirp->this_offset < dirp->buffer->size()) {
      pair<string,Inode*>& ent = (*dirp->buffer)[off - dirp->this_offset];

      int stmask = fill_stat(ent.second, &st);  
      fill_dirent(&de, ent.first.c_str(), st.st_mode, st.st_ino, dirp->offset + 1);
      
      client_lock.Unlock();
      int r = cb(p, &de, &st, stmask, dirp->offset + 1);  // _next_ offset
      client_lock.Lock();
      ldout(cct, 15) << " de " << de.d_name << " off " << hex << dirp->offset << dec
	       << " = " << r
	       << dendl;
      if (r < 0)
	return r;
      
      off++;
      dirp->offset++;
    }

    if (dirp->last_name.length()) {
      ldout(cct, 10) << " fetching next chunk of this frag" << dendl;
      _readdir_drop_dirp_buffer(dirp);
      continue;  // more!
    }

    if (!fg.is_rightmost()) {
      // next frag!
      _readdir_next_frag(dirp);
      ldout(cct, 10) << " advancing to next frag: " << fg << " -> " << dirp->frag() << dendl;
      fg = dirp->frag();
      off = 0;
      continue;
    }

    if (diri->dir &&
	diri->dir->release_count == dirp->release_count &&
	diri->shared_gen == dirp->start_shared_gen) {
      ldout(cct, 10) << " marking I_COMPLETE on " << *diri << dendl;
      diri->flags |= I_COMPLETE;
      if (diri->dir)
	diri->dir->max_offset = dirp->offset;
    }

    dirp->set_end();
    return 0;
  }
  assert(0);
  return 0;
}


int Client::readdir_r(dir_result_t *d, struct dirent *de)
{  
  return readdirplus_r(d, de, 0, 0);
}

/*
 * readdirplus_r
 *
 * returns
 *  1 if we got a dirent
 *  0 for end of directory
 * <0 on error
 */

struct single_readdir {
  struct dirent *de;
  struct stat *st;
  int *stmask;
  bool full;
};

static int _readdir_single_dirent_cb(void *p, struct dirent *de, struct stat *st,
				     int stmask, off_t off)
{
  single_readdir *c = static_cast<single_readdir *>(p);

  if (c->full)
    return -1;  // already filled this dirent

  *c->de = *de;
  if (c->st)
    *c->st = *st;
  if (c->stmask)
    *c->stmask = stmask;
  c->full = true;
  return 0;  
}

struct dirent *Client::readdir(dir_result_t *d)
{
  int ret;
  static int stmask;
  static struct dirent de;
  static struct stat st;
  single_readdir sr;
  sr.de = &de;
  sr.st = &st;
  sr.stmask = &stmask;
  sr.full = false;

  // our callback fills the dirent and sets sr.full=true on first
  // call, and returns -1 the second time around.
  ret = readdir_r_cb(d, _readdir_single_dirent_cb, (void *)&sr);
  if (ret < -1) {
    errno = -ret;  // this sucks.
    return (dirent *) NULL;
  }
  if (sr.full) {
    return &de;
  }
  return (dirent *) NULL;
}

int Client::readdirplus_r(dir_result_t *d, struct dirent *de, struct stat *st, int *stmask)
{  
  single_readdir sr;
  sr.de = de;
  sr.st = st;
  sr.stmask = stmask;
  sr.full = false;

  // our callback fills the dirent and sets sr.full=true on first
  // call, and returns -1 the second time around.
  int r = readdir_r_cb(d, _readdir_single_dirent_cb, (void *)&sr);
  if (r < -1)
    return r;
  if (sr.full)
    return 1;
  return 0;
}


/* getdents */
struct getdents_result {
  char *buf;
  int buflen;
  int pos;
  bool fullent;
};

static int _readdir_getdent_cb(void *p, struct dirent *de, struct stat *st, int stmask, off_t off)
{
  struct getdents_result *c = static_cast<getdents_result *>(p);

  int dlen;
  if (c->fullent)
    dlen = sizeof(*de);
  else
    dlen = strlen(de->d_name) + 1;

  if (c->pos + dlen > c->buflen)
    return -1;  // doesn't fit

  if (c->fullent) {
    memcpy(c->buf + c->pos, de, sizeof(*de));
  } else {
    memcpy(c->buf + c->pos, de->d_name, dlen);
  }
  c->pos += dlen;
  return 0;
}

int Client::_getdents(dir_result_t *dir, char *buf, int buflen, bool fullent)
{
  getdents_result gr;
  gr.buf = buf;
  gr.buflen = buflen;
  gr.fullent = fullent;
  gr.pos = 0;

  int r = readdir_r_cb(dir, _readdir_getdent_cb, (void *)&gr);

  if (r < 0) { // some error
    if (r == -1) { // buffer ran out of space
      if (gr.pos) { // but we got some entries already!
        return gr.pos;
      } // or we need a larger buffer
      return -ERANGE;
    } else { // actual error, return it
      return r;
    }
  }
  return gr.pos;
}


/* getdir */
struct getdir_result {
  list<string> *contents;
  int num;
};

static int _getdir_cb(void *p, struct dirent *de, struct stat *st, int stmask, off_t off)
{
  getdir_result *r = static_cast<getdir_result *>(p);

  r->contents->push_back(de->d_name);
  r->num++;
  return 0;
}

int Client::getdir(const char *relpath, list<string>& contents)
{
  ldout(cct, 3) << "getdir(" << relpath << ")" << dendl;
  {
    Mutex::Locker lock(client_lock);
    tout(cct) << "getdir" << std::endl;
    tout(cct) << relpath << std::endl;
  }

  dir_result_t *d;
  int r = opendir(relpath, &d);
  if (r < 0)
    return r;

  getdir_result gr;
  gr.contents = &contents;
  gr.num = 0;
  r = readdir_r_cb(d, _getdir_cb, (void *)&gr);

  closedir(d);

  if (r < 0)
    return r;
  return gr.num;
}


/****** file i/o **********/
int Client::open(const char *relpath, int flags, mode_t mode, int stripe_unit,
    int stripe_count, int object_size, const char *data_pool)
{
  ldout(cct, 3) << "open enter(" << relpath << ", " << flags << "," << mode << ") = " << dendl;
  Mutex::Locker lock(client_lock);
  tout(cct) << "open" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << flags << std::endl;

  Fh *fh = NULL;

  filepath path(relpath);
  Inode *in;
  bool created = false;
  int r = path_walk(path, &in);
  if (r == 0 && (flags & O_CREAT) && (flags & O_EXCL))
    return -EEXIST;
  if (r == -ENOENT && (flags & O_CREAT)) {
    filepath dirpath = path;
    string dname = dirpath.last_dentry();
    dirpath.pop_dentry();
    Inode *dir;
    r = path_walk(dirpath, &dir);
    if (r < 0)
      return r;
    r = _create(dir, dname.c_str(), flags, mode, &in, &fh, stripe_unit,
                stripe_count, object_size, data_pool, &created);
  }
  if (r < 0)
    goto out;

  if (!created) {
    // posix says we can only check permissions of existing files
    uid_t uid = geteuid();
    gid_t gid = getegid();
    r = check_permissions(in, flags, uid, gid);
    if (r < 0)
      goto out;
  }

  if (!fh)
    r = _open(in, flags, mode, &fh);
  if (r >= 0) {
    // allocate a integer file descriptor
    assert(fh);
    assert(in);
    r = get_fd();
    assert(fd_map.count(r) == 0);
    fd_map[r] = fh;
  }
  
 out:
  tout(cct) << r << std::endl;
  ldout(cct, 3) << "open exit(" << path << ", " << flags << ") = " << r << dendl;
  return r;
}

int Client::open(const char *relpath, int flags, mode_t mode)
{
  /* Use default file striping parameters */
  return open(relpath, flags, mode, 0, 0, 0, NULL);
}

int Client::lookup_hash(inodeno_t ino, inodeno_t dirino, const char *name)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "lookup_hash enter(" << ino << ", #" << dirino << "/" << name << ") = " << dendl;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPHASH);
  filepath path(ino);
  req->set_filepath(path);

  uint32_t h = ceph_str_hash(CEPH_STR_HASH_RJENKINS, name, strlen(name));
  char f[30];
  sprintf(f, "%u", h);
  filepath path2(dirino);
  path2.push_dentry(string(f));
  req->set_filepath2(path2);

  int r = make_request(req, -1, -1, NULL, NULL, rand() % mdsmap->get_num_in_mds());
  ldout(cct, 3) << "lookup_hash exit(" << ino << ", #" << dirino << "/" << name << ") = " << r << dendl;
  return r;
}

int Client::lookup_ino(inodeno_t ino)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "lookup_ino enter(" << ino << ") = " << dendl;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPINO);
  filepath path(ino);
  req->set_filepath(path);

  int r = make_request(req, -1, -1, NULL, NULL, rand() % mdsmap->get_num_in_mds());
  ldout(cct, 3) << "lookup_ino exit(" << ino << ") = " << r << dendl;
  return r;
}

Fh *Client::_create_fh(Inode *in, int flags, int cmode)
{
  // yay
  Fh *f = new Fh;
  f->mode = cmode;
  f->flags = flags;

  // inode
  assert(in);
  f->inode = in;
  f->inode->get();

  ldout(cct, 10) << "_create_fh " << in->ino << " mode " << cmode << dendl;

  if (in->snapid != CEPH_NOSNAP) {
    in->snap_cap_refs++;
    ldout(cct, 5) << "open success, fh is " << f << " combined IMMUTABLE SNAP caps " 
	    << ccap_string(in->caps_issued()) << dendl;
  }

  return f;
}

int Client::_release_fh(Fh *f)
{
  //ldout(cct, 3) << "op: client->close(open_files[ " << fh << " ]);" << dendl;
  //ldout(cct, 3) << "op: open_files.erase( " << fh << " );" << dendl;
  Inode *in = f->inode;
  ldout(cct, 5) << "_release_fh " << f << " mode " << f->mode << " on " << *in << dendl;

  if (in->snapid == CEPH_NOSNAP) {
    if (in->put_open_ref(f->mode)) {
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

int Client::_open(Inode *in, int flags, mode_t mode, Fh **fhp, int uid, int gid)
{
  int cmode = ceph_flags_to_mode(flags);
  if (cmode < 0)
    return -EINVAL;
  int want = ceph_caps_for_mode(cmode);
  int result = 0;

  if (in->snapid != CEPH_NOSNAP &&
      (flags & (O_WRONLY | O_RDWR | O_CREAT | O_TRUNC | O_APPEND))) {
    return -EROFS;
  }

  in->get_open_ref(cmode);  // make note of pending open, since it effects _wanted_ caps.

  if ((flags & O_TRUNC) == 0 &&
      in->caps_issued_mask(want)) {
    // update wanted?
    check_caps(in, true);
  } else {
    MetaRequest *req = new MetaRequest(CEPH_MDS_OP_OPEN);
    filepath path;
    in->make_nosnap_relative_path(path);
    req->set_filepath(path); 
    req->head.args.open.flags = flags & ~O_CREAT;
    req->head.args.open.mode = mode;
    req->head.args.open.pool = -1;
    req->head.args.open.old_size = in->size;   // for O_TRUNC
    req->set_inode(in);
    result = make_request(req, uid, gid);
  }

  // success?
  if (result >= 0) {
    *fhp = _create_fh(in, flags, cmode);
  } else {
    in->put_open_ref(cmode);
  }

  trim_cache();

  return result;
}

int Client::close(int fd)
{
  ldout(cct, 3) << "close enter(" << fd << ")" << dendl;
  Mutex::Locker lock(client_lock);
  tout(cct) << "close" << std::endl;
  tout(cct) << fd << std::endl;

  Fh *fh = get_filehandle(fd);
  if (!fh)
    return -EBADF;
  _release_fh(fh);
  fd_map.erase(fd);
  ldout(cct, 3) << "close exit(" << fd << ")" << dendl;
  return 0;
}


// ------------
// read, write

loff_t Client::lseek(int fd, loff_t offset, int whence)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "lseek" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << offset << std::endl;
  tout(cct) << whence << std::endl;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return _lseek(f, offset, whence);
}

loff_t Client::_lseek(Fh *f, loff_t offset, int whence)
{
  Inode *in = f->inode;
  int r;

  switch (whence) {
  case SEEK_SET: 
    f->pos = offset;
    break;

  case SEEK_CUR:
    f->pos += offset;
    break;

  case SEEK_END:
    r = _getattr(in, CEPH_STAT_CAP_SIZE);
    if (r < 0)
      return r;
    f->pos = in->size + offset;
    break;

  default:
    assert(0);
  }
  
  ldout(cct, 3) << "_lseek(" << f << ", " << offset << ", " << whence << ") = " << f->pos << dendl;
  return f->pos;
}


void Client::lock_fh_pos(Fh *f)
{
  ldout(cct, 10) << "lock_fh_pos " << f << dendl;

  if (f->pos_locked || !f->pos_waiters.empty()) {
    Cond cond;
    f->pos_waiters.push_back(&cond);
    ldout(cct, 10) << "lock_fh_pos BLOCKING on " << f << dendl;
    while (f->pos_locked || f->pos_waiters.front() != &cond)
      cond.Wait(client_lock);
    ldout(cct, 10) << "lock_fh_pos UNBLOCKING on " << f << dendl;
    assert(f->pos_waiters.front() == &cond);
    f->pos_waiters.pop_front();
  }
  
  f->pos_locked = true;
}

void Client::unlock_fh_pos(Fh *f)
{
  ldout(cct, 10) << "unlock_fh_pos " << f << dendl;
  f->pos_locked = false;
}


// 

// blocking osd interface

int Client::read(int fd, char *buf, loff_t size, loff_t offset) 
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "read" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << size << std::endl;
  tout(cct) << offset << std::endl;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  bufferlist bl;
  int r = _read(f, offset, size, &bl);
  ldout(cct, 3) << "read(" << fd << ", " << (void*)buf << ", " << size << ", " << offset << ") = " << r << dendl;
  if (r >= 0) {
    bl.copy(0, bl.length(), buf);
    r = bl.length();
  }
  return r;
}

int Client::_read(Fh *f, int64_t offset, uint64_t size, bufferlist *bl)
{
  const md_config_t *conf = cct->_conf;
  Inode *in = f->inode;

  //bool lazy = f->mode == CEPH_FILE_MODE_LAZY;

  int have;
  int r = get_caps(in, CEPH_CAP_FILE_RD, CEPH_CAP_FILE_CACHE, &have, -1);
  if (r < 0)
    return r;

  bool movepos = false;
  if (offset < 0) {
    lock_fh_pos(f);
    offset = f->pos;
    movepos = true;
  }

  if (!conf->client_debug_force_sync_read &&
      (cct->_conf->client_oc && (have & CEPH_CAP_FILE_CACHE))) {

    if (f->flags & O_RSYNC) {
      _flush_range(in, offset, size);
    }
    r = _read_async(f, offset, size, bl);
  } else {
    r = _read_sync(f, offset, size, bl);
  }

  // don't move pointer if the read failed
  if (r < 0) {
    goto done;
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
  ldout(cct, 10) << "readahead nr_consec_read " << f->nr_consec_read
	   << " for " << f->consec_read_bytes << " bytes" 
	   << " .. last_pos " << f->last_pos << " .. offset " << offset
	   << dendl;
  f->last_pos = offset+bl->length();

done:
  // done!
  put_cap_ref(in, CEPH_CAP_FILE_RD);
  return r;
}

int Client::_read_async(Fh *f, uint64_t off, uint64_t len, bufferlist *bl)
{
  const md_config_t *conf = cct->_conf;
  Inode *in = f->inode;
  bool readahead = true;

  ldout(cct, 10) << "_read_async " << *in << " " << off << "~" << len << dendl;

  // trim read based on file size?
  if (off >= in->size)
    return 0;
  if (off + len > in->size) {
    len = in->size - off;    
    readahead = false;
  }

  // we will populate the cache here
  if (in->cap_refs[CEPH_CAP_FILE_CACHE] == 0)
    in->get_cap_ref(CEPH_CAP_FILE_CACHE);
  
  ldout(cct, 10) << "readahead=" << readahead << " nr_consec=" << f->nr_consec_read
	   << " max_byes=" << conf->client_readahead_max_bytes
	   << " max_periods=" << conf->client_readahead_max_periods << dendl;

  // readahead?
  if (readahead &&
      f->nr_consec_read &&
      (conf->client_readahead_max_bytes ||
       conf->client_readahead_max_periods)) {
    loff_t l = f->consec_read_bytes * 2;
    if (conf->client_readahead_min)
      l = MAX(l, conf->client_readahead_min);
    if (conf->client_readahead_max_bytes)
      l = MIN(l, conf->client_readahead_max_bytes);
    loff_t p = in->layout.fl_stripe_count * in->layout.fl_object_size;
    if (conf->client_readahead_max_periods)
      l = MIN(l, conf->client_readahead_max_periods * p);

    if (l >= 2*p)
      // align large readahead with period
      l -= (off+l) % p;
    else {
      // align readahead with stripe unit if we cross su boundary
      int su = in->layout.fl_stripe_unit;
      if ((off+l)/su != off/su) l -= (off+l) % su;
    }
    
    // don't read past end of file
    if (off+l > in->size)
      l = in->size - off;
    
    loff_t min = MIN((loff_t)len, l/2);

    ldout(cct, 20) << "readahead " << f->nr_consec_read << " reads " 
	     << f->consec_read_bytes << " bytes ... readahead " << off << "~" << l
	     << " min " << min
	     << " (caller wants " << off << "~" << len << ")" << dendl;
    if (l > (loff_t)len) {
      if (objectcacher->file_is_cached(&in->oset, &in->layout, in->snapid, off, min))
	ldout(cct, 20) << "readahead already have min" << dendl;
      else {
	objectcacher->file_read(&in->oset, &in->layout, in->snapid, off, l, NULL, 0, 0);
	ldout(cct, 20) << "readahead initiated" << dendl;
      }
    }
  }
  
  // read (and possibly block)
  int r, rvalue = 0;
  Mutex flock("Client::_read_async flock");
  Cond cond;
  bool done = false;
  Context *onfinish = new C_SafeCond(&flock, &cond, &done, &rvalue);
  r = objectcacher->file_read(&in->oset, &in->layout, in->snapid,
                              off, len, bl, 0, onfinish);
  if (r == 0) {
    client_lock.Unlock();
    flock.Lock();
    while (!done) 
      cond.Wait(flock);
    flock.Unlock();
    client_lock.Lock();
    r = rvalue;
  } else {
    // it was cached.
    delete onfinish;
  }
  return r;
}

int Client::_read_sync(Fh *f, uint64_t off, uint64_t len, bufferlist *bl)
{
  Inode *in = f->inode;
  uint64_t pos = off;
  int left = len;
  int read = 0;

  ldout(cct, 10) << "_read_sync " << *in << " " << off << "~" << len << dendl;

  Mutex flock("Client::_read_sync flock");
  Cond cond;
  while (left > 0) {
    int r = 0;
    bool done = false;
    Context *onfinish = new C_SafeCond(&flock, &cond, &done, &r);
    bufferlist tbl;
    
    int wanted = left;
    filer->read_trunc(in->ino, &in->layout, in->snapid,
		      pos, left, &tbl, 0,
		      in->truncate_size, in->truncate_seq,
		      onfinish);
    client_lock.Unlock();
    flock.Lock();
    while (!done)
      cond.Wait(flock);
    flock.Unlock();
    client_lock.Lock();

    // if we get ENOENT from OSD, assume 0 bytes returned
    if (r == -ENOENT)
      r = 0;
    if (r < 0)
      return r;
    if (tbl.length()) {
      r = tbl.length();

      read += r;
      pos += r;
      left -= r;
      bl->claim_append(tbl);
    }
    // short read?
    if (r >= 0 && r < wanted) {
      if (pos < in->size) {
	// zero up to known EOF
	int some = in->size - pos;
	if (some > left)
	  some = left;
	bufferptr z(some);
	z.zero();
	bl->push_back(z);
	read += some;
	pos += some;
	left -= some;
	if (left == 0)
	  return read;
      }

      // reverify size
      r = _getattr(in, CEPH_STAT_CAP_SIZE);
      if (r < 0)
	return r;

      // eof?  short read.
      if (pos >= in->size)
	return read;
    }
  }
  return read;
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
  assert(unsafe_sync_write > 0);
  unsafe_sync_write--;

  put_cap_ref(in, CEPH_CAP_FILE_BUFFER);

  ldout(cct, 15) << "sync_write_commit unsafe_sync_write = " << unsafe_sync_write << dendl;
  if (unsafe_sync_write == 0 && unmounting) {
    ldout(cct, 10) << "sync_write_comit -- no more unsafe writes, unmount can proceed" << dendl;
    mount_cond.Signal();
  }

  put_inode(in);
}

int Client::write(int fd, const char *buf, loff_t size, loff_t offset) 
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "write" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << size << std::endl;
  tout(cct) << offset << std::endl;

  Fh *fh = get_filehandle(fd);
  if (!fh)
    return -EBADF;
  int r = _write(fh, offset, size, buf);
  ldout(cct, 3) << "write(" << fd << ", \"...\", " << size << ", " << offset << ") = " << r << dendl;
  return r;
}


int Client::_write(Fh *f, int64_t offset, uint64_t size, const char *buf)
{
  if ((uint64_t)(offset+size) > mdsmap->get_max_filesize()) //too large!
    return -EFBIG;

  if (osdmap->test_flag(CEPH_OSDMAP_FULL))
    return -ENOSPC;

  //ldout(cct, 7) << "write fh " << fh << " size " << size << " offset " << offset << dendl;
  Inode *in = f->inode;

  assert(in->snapid == CEPH_NOSNAP);

  // was Fh opened as writeable?
  if ((f->mode & CEPH_FILE_MODE_WR) == 0)
    return -EBADF;

  // use/adjust fd pos?
  if (offset < 0) {
    lock_fh_pos(f);
    /* 
     * FIXME: this is racy in that we may block _after_ this point waiting for caps, and size may
     * change out from under us.
     */
    if (f->flags & O_APPEND)
      _lseek(f, 0, SEEK_END);
    offset = f->pos;
    f->pos = offset+size;
    unlock_fh_pos(f);
  }

  //bool lazy = f->mode == CEPH_FILE_MODE_LAZY;

  ldout(cct, 10) << "cur file size is " << in->size << dendl;

  // time it.
  utime_t start = ceph_clock_now(cct);

  // copy into fresh buffer (since our write may be resub, async)
  bufferptr bp;
  if (size > 0) bp = buffer::copy(buf, size);
  bufferlist bl;
  bl.push_back( bp );

  utime_t lat;
  uint64_t totalwritten;
  uint64_t endoff = offset + size;
  int have;
  int r = get_caps(in, CEPH_CAP_FILE_WR, CEPH_CAP_FILE_BUFFER, &have, endoff);
  if (r < 0)
    return r;

  ldout(cct, 10) << " snaprealm " << *in->snaprealm << dendl;

  if (cct->_conf->client_oc && (have & CEPH_CAP_FILE_BUFFER)) {
    // do buffered write
    if (!in->oset.dirty_or_tx)
      get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

    get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

    // async, caching, non-blocking.
    r = objectcacher->file_write(&in->oset, &in->layout, in->snaprealm->get_snap_context(),
			         offset, size, bl, ceph_clock_now(cct), 0,
			         client_lock);

    put_cap_ref(in, CEPH_CAP_FILE_BUFFER);

    if (r < 0)
      goto done;

    // flush cached write if O_SYNC is set on file fh
    // O_DSYNC == O_SYNC on linux < 2.6.33
    // O_SYNC = __O_SYNC | O_DSYNC on linux >= 2.6.33
    if ((f->flags & O_SYNC) || (f->flags & O_DSYNC)) {
      _flush_range(in, offset, size);
    }
  } else {
    // simple, non-atomic sync write
    Mutex flock("Client::_write flock");
    Cond cond;
    bool done = false;
    Context *onfinish = new C_SafeCond(&flock, &cond, &done);
    Context *onsafe = new C_Client_SyncCommit(this, in);

    unsafe_sync_write++;
    get_cap_ref(in, CEPH_CAP_FILE_BUFFER);  // released by onsafe callback

    r = filer->write_trunc(in->ino, &in->layout, in->snaprealm->get_snap_context(),
			   offset, size, bl, ceph_clock_now(cct), 0,
			   in->truncate_size, in->truncate_seq,
			   onfinish, onsafe);
    if (r < 0)
      goto done;

    client_lock.Unlock();
    flock.Lock();
    while (!done)
      cond.Wait(flock);
    flock.Unlock();
    client_lock.Lock();
  }

  // if we get here, write was successful, update client metadata

  // time
  lat = ceph_clock_now(cct);
  lat -= start;
  logger->tinc(l_c_wrlat, lat);

  totalwritten = size;
  r = (int)totalwritten;

  // extend file?
  if (totalwritten + offset > in->size) {
    in->size = totalwritten + offset;
    mark_caps_dirty(in, CEPH_CAP_FILE_WR);

    if ((in->size << 1) >= in->max_size &&
	(in->reported_size << 1) < in->max_size)
      check_caps(in, false);

    ldout(cct, 7) << "wrote to " << totalwritten+offset << ", extending file size" << dendl;
  } else {
    ldout(cct, 7) << "wrote to " << totalwritten+offset << ", leaving file size at " << in->size << dendl;
  }

  // mtime
  in->mtime = ceph_clock_now(cct);
  mark_caps_dirty(in, CEPH_CAP_FILE_WR);

done:
  put_cap_ref(in, CEPH_CAP_FILE_WR);
  return r;
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
  tout(cct) << "ftruncate" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << length << std::endl;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  struct stat attr;
  attr.st_size = length;
  return _setattr(f->inode, &attr, CEPH_SETATTR_SIZE);
}

int Client::fsync(int fd, bool syncdataonly) 
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "fsync" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << syncdataonly << std::endl;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  int r = _fsync(f, syncdataonly);
  ldout(cct, 3) << "fsync(" << fd << ", " << syncdataonly << ") = " << r << dendl;
  return r;
}

int Client::_fsync(Fh *f, bool syncdataonly)
{
  int r = 0;

  Inode *in = f->inode;
  tid_t wait_on_flush = 0;
  bool flushed_metadata = false;
  Mutex lock("Client::_fsync::lock");
  Cond cond;
  bool done = false;
  C_SafeCond *object_cacher_completion = NULL;

  ldout(cct, 3) << "_fsync(" << f << ", " << (syncdataonly ? "dataonly)":"data+metadata)") << dendl;
  
  if (cct->_conf->client_oc) {
    object_cacher_completion = new C_SafeCond(&lock, &cond, &done, &r);
    in->get(); // take a reference; C_SafeCond doesn't and _flush won't either
    _flush(in, object_cacher_completion);
    ldout(cct, 15) << "using return-valued form of _fsync" << dendl;
  }
  
  if (!syncdataonly && (in->dirty_caps & ~CEPH_CAP_ANY_FILE_WR)) {
    for (map<int, Cap*>::iterator iter = in->caps.begin(); iter != in->caps.end(); ++iter) {
      if (iter->second->implemented & ~CEPH_CAP_ANY_FILE_WR) {
	MetaSession *session = mds_sessions[iter->first];
	assert(session);
        flush_caps(in, session);
      }
    }
    wait_on_flush = in->last_flush_tid;
    flushed_metadata = true;
  } else ldout(cct, 10) << "no metadata needs to commit" << dendl;

  if (object_cacher_completion) { // wait on a real reply instead of guessing
    client_lock.Unlock();
    lock.Lock();
    ldout(cct, 15) << "waiting on data to flush" << dendl;
    while (!done)
      cond.Wait(lock);
    lock.Unlock();
    client_lock.Lock();
    put_inode(in);
    ldout(cct, 15) << "got " << r << " from flush writeback" << dendl;
  } else {
    // FIXME: this can starve
    while (in->cap_refs[CEPH_CAP_FILE_BUFFER] > 0) {
      ldout(cct, 10) << "ino " << in->ino << " has " << in->cap_refs[CEPH_CAP_FILE_BUFFER]
		     << " uncommitted, waiting" << dendl;
      wait_on_list(in->waitfor_commit);
    }
  }

  if (!r) {
    if (flushed_metadata) wait_sync_caps(wait_on_flush);
    // this could wait longer than strictly necessary,
    // but on a sync the user can put up with it

    ldout(cct, 10) << "ino " << in->ino << " has no uncommitted writes" << dendl;
  } else {
    ldout(cct, 1) << "ino " << in->ino << " failed to commit to disk! "
		  << cpp_strerror(-r) << dendl;
  }
  return r;
}

int Client::fstat(int fd, struct stat *stbuf) 
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "fstat" << std::endl;
  tout(cct) << fd << std::endl;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  int r = _getattr(f->inode, -1);
  if (r < 0)
    return r;
  fill_stat(f->inode, stbuf, NULL);
  ldout(cct, 3) << "fstat(" << fd << ", " << stbuf << ") = " << r << dendl;
  return r;
}


// not written yet, but i want to link!

int Client::chdir(const char *relpath)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "chdir" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  if (cwd != in) {
    in->get();
    put_inode(cwd);
    cwd = in;
  }
  ldout(cct, 3) << "chdir(" << relpath << ")  cwd now " << cwd->ino << dendl;
  return 0;
}

void Client::getcwd(string& dir)
{
  filepath path;
  ldout(cct, 10) << "getcwd " << *cwd << dendl;

  Inode *in = cwd;
  while (in != root) {
    assert(in->dn_set.size() < 2); // dirs can't be hard-linked
    Dentry *dn = in->get_first_parent();
    if (!dn) {
      // look it up
      ldout(cct, 10) << "getcwd looking up parent for " << *in << dendl;
      MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPPARENT);
      filepath path(in->ino);
      req->set_filepath(path);
      req->set_inode(in);
      int res = make_request(req, -1, -1);
      if (res < 0)
	break;

      // start over
      path = filepath();
      in = cwd;
      continue;
    }
    path.push_front_dentry(dn->name);
    in = dn->dir->parent_inode;
  }
  dir = "/";
  dir += path.get_path();
}

int Client::statfs(const char *path, struct statvfs *stbuf)
{
  Mutex::Locker l(client_lock);
  tout(cct) << "statfs" << std::endl;

  ceph_statfs stats;

  Mutex lock("Client::statfs::lock");
  Cond cond;
  bool done;
  int rval;

  objecter->get_fs_stats(stats, new C_SafeCond(&lock, &cond, &done, &rval));

  client_lock.Unlock();
  lock.Lock();
  while (!done) 
    cond.Wait(lock);
  lock.Unlock();
  client_lock.Lock();

  memset(stbuf, 0, sizeof(*stbuf));

  /*
   * we're going to set a block size of 4MB so we can represent larger
   * FSes without overflowing. Additionally convert the space
   * measurements from KB to bytes while making them in terms of
   * blocks.  We use 4MB only because it is big enough, and because it
   * actually *is* the (ceph) default block size.
   */
  const int CEPH_BLOCK_SHIFT = 22;
  stbuf->f_frsize = 1 << CEPH_BLOCK_SHIFT;
  stbuf->f_bsize = 1 << CEPH_BLOCK_SHIFT;
  stbuf->f_blocks = stats.kb >> (CEPH_BLOCK_SHIFT - 10);
  stbuf->f_bfree = stats.kb_avail >> (CEPH_BLOCK_SHIFT - 10);
  stbuf->f_bavail = stats.kb_avail >> (CEPH_BLOCK_SHIFT - 10);
  stbuf->f_files = stats.num_objects;
  stbuf->f_ffree = -1;
  stbuf->f_favail = -1;
  stbuf->f_fsid = -1;       // ??
  stbuf->f_flag = 0;        // ??
  stbuf->f_namemax = NAME_MAX;

  return rval;
}

int Client::ll_statfs(vinodeno_t vino, struct statvfs *stbuf)
{
  tout(cct) << "ll_statfs" << std::endl;
  return statfs(0, stbuf);
}

void Client::ll_register_ino_invalidate_cb(client_ino_callback_t cb, void *handle)
{
  Mutex::Locker l(client_lock);
  ldout(cct, 10) << "ll_register_ino_invalidate_cb cb " << (void*)cb << " p " << (void*)handle << dendl;
  if (cb == NULL)
    return;
  ino_invalidate_cb = cb;
  ino_invalidate_cb_handle = handle;
  async_ino_invalidator.start();
}

void Client::ll_register_getgroups_cb(client_getgroups_callback_t cb, void *handle)
{
  Mutex::Locker l(client_lock);
  getgroups_cb = cb;
  getgroups_cb_handle = handle;
}

int Client::_sync_fs()
{
  ldout(cct, 10) << "_sync_fs" << dendl;

  // wait for unsafe mds requests
  // FIXME
  
  // flush caps
  flush_caps();
  wait_sync_caps(last_flush_seq);

  // flush file data
  // FIXME

  return 0;
}

int Client::sync_fs()
{
  Mutex::Locker l(client_lock);
  return _sync_fs();
}

int64_t Client::drop_caches()
{
  Mutex::Locker l(client_lock);
  return objectcacher->release_all();
}


int Client::lazyio_propogate(int fd, loff_t offset, size_t count)
{
  Mutex::Locker l(client_lock);
  ldout(cct, 3) << "op: client->lazyio_propogate(" << fd
          << ", " << offset << ", " << count << ")" << dendl;
  
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;

  // for now
  _fsync(f, true);

  return 0;
}

int Client::lazyio_synchronize(int fd, loff_t offset, size_t count)
{
  Mutex::Locker l(client_lock);
  ldout(cct, 3) << "op: client->lazyio_synchronize(" << fd
          << ", " << offset << ", " << count << ")" << dendl;
  
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode;
  
  _fsync(f, true);
  _release(in);
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
  Inode *snapdir = open_snapdir(in);
  return _mkdir(snapdir, name, 0);
}
int Client::rmsnap(const char *relpath, const char *name)
{
  Mutex::Locker l(client_lock);
  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  Inode *snapdir = open_snapdir(in);
  return _rmdir(snapdir, name);
}

// =============================
// expose caps

int Client::get_caps_issued(int fd) {

  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;

  return f->inode->caps_issued();
}

int Client::get_caps_issued(const char *path) {

  Mutex::Locker lock(client_lock);
  filepath p(path);
  Inode *in;
  int r = path_walk(p, &in, true);
  if (r < 0)
    return r;
  return in->caps_issued();
}

// =========================================
// low level

Inode *Client::open_snapdir(Inode *diri)
{
  Inode *in;
  vinodeno_t vino(diri->ino, CEPH_SNAPDIR);
  if (!inode_map.count(vino)) {
    in = new Inode(cct, vino, &diri->layout);

    in->ino = diri->ino;
    in->snapid = CEPH_SNAPDIR;
    in->mode = diri->mode;
    in->uid = diri->uid;
    in->gid = diri->gid;
    in->mtime = diri->mtime;
    in->ctime = diri->ctime;
    in->size = diri->size;

    in->dirfragtree.clear();
    inode_map[vino] = in;
    in->snapdir_parent = diri;
    diri->get();
    ldout(cct, 10) << "open_snapdir created snapshot inode " << *in << dendl;
  } else {
    in = inode_map[vino];
    ldout(cct, 10) << "open_snapdir had snapshot inode " << *in << dendl;
  }
  return in;
}

int Client::ll_lookup(vinodeno_t parent, const char *name, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_lookup " << parent << " " << name << dendl;
  tout(cct) << "ll_lookup" << std::endl;
  tout(cct) << parent.ino.val << std::endl;
  tout(cct) << name << std::endl;

  string dname = name;
  Inode *diri = 0;
  Inode *in = 0;
  int r = 0;

  if (inode_map.count(parent) == 0) {
    ldout(cct, 1) << "ll_lookup " << parent << " " << name << " -> ENOENT (parent DNE... WTF)" << dendl;
    r = -ENOENT;
    attr->st_ino = 0;
    goto out;
  }
  diri = inode_map[parent];
  if (!diri->is_dir()) {
    ldout(cct, 1) << "ll_lookup " << parent << " " << name << " -> ENOTDIR (parent not a dir... WTF)" << dendl;
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
  ldout(cct, 3) << "ll_lookup " << parent << " " << name
	  << " -> " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  tout(cct) << attr->st_ino << std::endl;
  return r;
}

void Client::_ll_get(Inode *in)
{
  if (in->ll_ref == 0) 
    in->get();
  in->ll_get();
  ldout(cct, 20) << "_ll_get " << in << " " << in->ino << " -> " << in->ll_ref << dendl;
}

int Client::_ll_put(Inode *in, int num)
{
  in->ll_put(num);
  ldout(cct, 20) << "_ll_put " << in << " " << in->ino << " " << num << " -> " << in->ll_ref << dendl;
  if (in->ll_ref == 0) {
    put_inode(in);
    return 0;
  } else {
    return in->ll_ref;
  }
}

void Client::_ll_drop_pins()
{
  ldout(cct, 10) << "_ll_drop_pins" << dendl;
  hash_map<vinodeno_t, Inode*>::iterator next;
  for (hash_map<vinodeno_t, Inode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it = next) {
    Inode *in = it->second;
    next = it;
    ++next;
    if (in->ll_ref)
      _ll_put(in, in->ll_ref);
  }
}

bool Client::ll_forget(vinodeno_t vino, int num)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_forget " << vino << " " << num << dendl;
  tout(cct) << "ll_forget" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << num << std::endl;

  if (vino.ino == 1) return true;  // ignore forget on root.

  bool last = false;
  if (inode_map.count(vino) == 0) {
    ldout(cct, 1) << "WARNING: ll_forget on " << vino << " " << num 
	    << ", which I don't have" << dendl;
  } else {
    Inode *in = inode_map[vino];
    assert(in);
    if (in->ll_ref < num) {
      ldout(cct, 1) << "WARNING: ll_forget on " << vino << " " << num << ", which only has ll_ref=" << in->ll_ref << dendl;
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
  ldout(cct, 3) << "ll_getattr " << vino << dendl;
  tout(cct) << "ll_getattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;

  /* special case for dotdot (..) */
  if (vino.ino.val == CEPH_INO_DOTDOT) {
    attr->st_mode = S_IFDIR | 0755;
    attr->st_nlink = 2;
    return 0;
  }

  Inode *in = _ll_get_inode(vino);
  int res;
  if (vino.snapid < CEPH_NOSNAP)
    res = 0;
  else
    res = _getattr(in, CEPH_STAT_CAP_INODE_ALL, uid, gid);
  if (res == 0)
    fill_stat(in, attr);
  ldout(cct, 3) << "ll_getattr " << vino << " = " << res << dendl;
  return res;
}

int Client::ll_setattr(vinodeno_t vino, struct stat *attr, int mask, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_setattr " << vino << " mask " << hex << mask << dec << dendl;
  tout(cct) << "ll_setattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << attr->st_mode << std::endl;
  tout(cct) << attr->st_uid << std::endl;
  tout(cct) << attr->st_gid << std::endl;
  tout(cct) << attr->st_size << std::endl;
  tout(cct) << attr->st_mtime << std::endl;
  tout(cct) << attr->st_atime << std::endl;
  tout(cct) << mask << std::endl;

  Inode *in = _ll_get_inode(vino);
  Inode *target = in;
  int res = _setattr(in, attr, mask, uid, gid, &target);
  if (res == 0) {
    assert(in == target);
    fill_stat(in, attr);
  }
  ldout(cct, 3) << "ll_setattr " << vino << " = " << res << dendl;
  return res;
}


// ----------
// xattrs

int Client::getxattr(const char *path, const char *name, void *value, size_t size)
{
  Mutex::Locker lock(client_lock);
  Inode *ceph_inode;
  int r = Client::path_walk(path, &ceph_inode, true);
  if (r < 0)
    return r;
  return Client::_getxattr(ceph_inode, name, value, size, getuid(), getgid());
}

int Client::lgetxattr(const char *path, const char *name, void *value, size_t size)
{
  Mutex::Locker lock(client_lock);
  Inode *ceph_inode;
  int r = Client::path_walk(path, &ceph_inode, false);
  if (r < 0)
    return r;
  return Client::_getxattr(ceph_inode, name, value, size, getuid(), getgid());
}

int Client::listxattr(const char *path, char *list, size_t size)
{
  Mutex::Locker lock(client_lock);
  Inode *ceph_inode;
  int r = Client::path_walk(path, &ceph_inode, true);
  if (r < 0)
    return r;
  return Client::_listxattr(ceph_inode, list, size, getuid(), getgid());
}

int Client::llistxattr(const char *path, char *list, size_t size)
{
  Mutex::Locker lock(client_lock);
  Inode *ceph_inode;
  int r = Client::path_walk(path, &ceph_inode, false);
  if (r < 0)
    return r;
  return Client::_listxattr(ceph_inode, list, size, getuid(), getgid());
}

int Client::removexattr(const char *path, const char *name)
{
  Mutex::Locker lock(client_lock);
  Inode *ceph_inode;
  int r = Client::path_walk(path, &ceph_inode, true);
  if (r < 0)
    return r;
  return Client::_removexattr(ceph_inode, name, getuid(), getgid());
}

int Client::lremovexattr(const char *path, const char *name)
{
  Mutex::Locker lock(client_lock);
  Inode *ceph_inode;
  int r = Client::path_walk(path, &ceph_inode, false);
  if (r < 0)
    return r;
  return Client::_removexattr(ceph_inode, name, getuid(), getgid());
}

int Client::setxattr(const char *path, const char *name, const void *value, size_t size, int flags)
{
  Mutex::Locker lock(client_lock);
  Inode *ceph_inode;
  int r = Client::path_walk(path, &ceph_inode, true);
  if (r < 0)
    return r;
  return Client::_setxattr(ceph_inode, name, value, size, flags, getuid(), getgid());
}

int Client::lsetxattr(const char *path, const char *name, const void *value, size_t size, int flags)
{
  Mutex::Locker lock(client_lock);
  Inode *ceph_inode;
  int r = Client::path_walk(path, &ceph_inode, false);
  if (r < 0)
    return r;
  return Client::_setxattr(ceph_inode, name, value, size, flags, getuid(), getgid());
}

int Client::_getxattr(Inode *in, const char *name, void *value, size_t size,
		      int uid, int gid)
{
  int r;

  if (strncmp(name, "ceph.", 5) == 0) {
    string n(name);
    char buf[256];

    r = -ENODATA;
    if ((in->is_file() && n.find("ceph.file.layout") == 0) ||
	(in->is_dir() && in->has_dir_layout() && n.find("ceph.dir.layout") == 0)) {
      string rest = n.substr(n.find("layout"));
      if (rest == "layout") {
	r = snprintf(buf, sizeof(buf),
		     "stripe_unit=%lu stripe_count=%lu object_size=%lu pool=",
		     (long unsigned)in->layout.fl_stripe_unit,
		     (long unsigned)in->layout.fl_stripe_count,
		     (long unsigned)in->layout.fl_object_size);
	if (osdmap->have_pg_pool(in->layout.fl_pg_pool))
	  r += snprintf(buf + r, sizeof(buf) - r, "%s",
			osdmap->get_pool_name(in->layout.fl_pg_pool));
	else
	  r += snprintf(buf + r, sizeof(buf) - r, "%lu",
			(long unsigned)in->layout.fl_pg_pool);
      } else if (rest == "layout.stripe_unit") {
	r = snprintf(buf, sizeof(buf), "%lu", (long unsigned)in->layout.fl_stripe_unit);
      } else if (rest == "layout.stripe_count") {
	r = snprintf(buf, sizeof(buf), "%lu", (long unsigned)in->layout.fl_stripe_count);
      } else if (rest == "layout.object_size") {
	r = snprintf(buf, sizeof(buf), "%lu", (long unsigned)in->layout.fl_object_size);
      } else if (rest == "layout.pool") {
	if (osdmap->have_pg_pool(in->layout.fl_pg_pool))
	  r = snprintf(buf, sizeof(buf), "%s",
		       osdmap->get_pool_name(in->layout.fl_pg_pool));
	else
	  r = snprintf(buf, sizeof(buf), "%lu",
		       (long unsigned)in->layout.fl_pg_pool);
      }
    }
    if (size != 0) {
      if (r > (int)size) {
	r = -ERANGE;
      } else if (r > 0) {
	memcpy(value, buf, r);
      }
    }
    goto out;
  }

  r = _getattr(in, CEPH_STAT_CAP_XATTR, uid, gid);
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
 out:
  ldout(cct, 3) << "_getxattr(" << in->ino << ", \"" << name << "\", " << size << ") = " << r << dendl;
  return r;
}

int Client::ll_getxattr(vinodeno_t vino, const char *name, void *value, size_t size, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_getxattr " << vino << " " << name << " size " << size << dendl;
  tout(cct) << "ll_getxattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  Inode *in = _ll_get_inode(vino);
  return _getxattr(in, name, value, size, uid, gid);
}

int Client::_listxattr(Inode *in, char *name, size_t size, int uid, int gid)
{
  int r = _getattr(in, CEPH_STAT_CAP_XATTR, uid, gid);
  if (r == 0) {
    const char file_vxattrs[] = "ceph.file.layout";
    const char dir_vxattrs[] = "ceph.dir.layout";
    for (map<string,bufferptr>::iterator p = in->xattrs.begin();
	 p != in->xattrs.end();
	 ++p)
      r += p->first.length() + 1;
    if (in->is_file())
      r += sizeof(file_vxattrs);
    else if (in->is_dir() && in->has_dir_layout())
      r += sizeof(dir_vxattrs);

    if (size != 0) {
      if (size >= (unsigned)r) {
	for (map<string,bufferptr>::iterator p = in->xattrs.begin();
	     p != in->xattrs.end();
	     ++p) {
	  memcpy(name, p->first.c_str(), p->first.length());
	  name += p->first.length();
	  *name = '\0';
	  name++;
	}
	if (in->is_file()) {
	  memcpy(name, file_vxattrs, sizeof(file_vxattrs));
	  name += sizeof(file_vxattrs);
	} else if (in->is_dir() && in->has_dir_layout()) {
	  memcpy(name, dir_vxattrs, sizeof(dir_vxattrs));
	  name += sizeof(dir_vxattrs);
	}
      } else
	r = -ERANGE;
    }
  }
  ldout(cct, 3) << "_listxattr(" << in->ino << ", " << size << ") = " << r << dendl;
  return r;
}

int Client::ll_listxattr(vinodeno_t vino, char *names, size_t size, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_listxattr " << vino << " size " << size << dendl;
  tout(cct) << "ll_listxattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << size << std::endl;

  Inode *in = _ll_get_inode(vino);
  return _listxattr(in, names, size, uid, gid);
}

int Client::_setxattr(Inode *in, const char *name, const void *value, size_t size, int flags,
		      int uid, int gid)
{
  if (in->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  // same xattrs supported by kernel client
  if (strncmp(name, "user.", 5) &&
      strncmp(name, "security.", 9) &&
      strncmp(name, "trusted.", 8) &&
      strncmp(name, "ceph.", 5))
    return -EOPNOTSUPP;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_SETXATTR);
  filepath path;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_string2(name);
  req->set_inode(in);
  req->head.args.setxattr.flags = flags;

  bufferlist bl;
  bl.append((const char*)value, size);
  req->set_data(bl);
  
  int res = make_request(req, uid, gid);

  trim_cache();
  ldout(cct, 3) << "_setxattr(" << in->ino << ", \"" << name << "\") = " << res << dendl;
  return res;
}

int Client::ll_setxattr(vinodeno_t vino, const char *name, const void *value, size_t size, int flags,
			int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_setxattr " << vino << " " << name << " size " << size << dendl;
  tout(cct) << "ll_setxattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  Inode *in = _ll_get_inode(vino);
  return _setxattr(in, name, value, size, flags, uid, gid);
}

int Client::_removexattr(Inode *in, const char *name, int uid, int gid)
{
  if (in->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  // same xattrs supported by kernel client
  if (strncmp(name, "user.", 5) &&
      strncmp(name, "security.", 9) &&
      strncmp(name, "trusted.", 8) &&
      strncmp(name, "ceph.", 5))
    return -EOPNOTSUPP;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_RMXATTR);
  filepath path;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_filepath2(name);
  req->set_inode(in);
 
  int res = make_request(req, uid, gid);

  trim_cache();
  ldout(cct, 3) << "_removexattr(" << in->ino << ", \"" << name << "\") = " << res << dendl;
  return res;
}


int Client::ll_removexattr(vinodeno_t vino, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_removexattr " << vino << " " << name << dendl;
  tout(cct) << "ll_removexattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  Inode *in = _ll_get_inode(vino);
  return _removexattr(in, name, uid, gid);
}


int Client::ll_readlink(vinodeno_t vino, const char **value, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_readlink " << vino << dendl;
  tout(cct) << "ll_readlink" << std::endl;
  tout(cct) << vino.ino.val << std::endl;

  Inode *in = _ll_get_inode(vino);
  set<Dentry*>::iterator dn = in->dn_set.begin();
  while (dn != in->dn_set.end()) {
    touch_dn(*dn);
    ++dn;
  }

  int r = 0;
  if (in->is_symlink()) {
    *value = in->symlink.c_str();
  } else {
    *value = "";
    r = -EINVAL;
  }
  ldout(cct, 3) << "ll_readlink " << vino << " = " << r << " (" << *value << ")" << dendl;
  return r;
}

int Client::_mknod(Inode *dir, const char *name, mode_t mode, dev_t rdev, int uid, int gid, Inode **inp)
{ 
  ldout(cct, 3) << "_mknod(" << dir->ino << " " << name << ", 0" << oct << mode << dec << ", " << rdev
	  << ", uid " << uid << ", gid " << gid << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_MKNOD);

  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path); 
  req->set_inode(dir);
  req->head.args.mknod.mode = mode;
  req->head.args.mknod.rdev = rdev;
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);

  res = make_request(req, uid, gid, inp);

  trim_cache();

  ldout(cct, 3) << "mknod(" << path << ", 0" << oct << mode << dec << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_mknod(vinodeno_t parent, const char *name, mode_t mode, dev_t rdev, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_mknod " << parent << " " << name << dendl;
  tout(cct) << "ll_mknod" << std::endl;
  tout(cct) << parent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << rdev << std::endl;

  Inode *diri = _ll_get_inode(parent);
  Inode *in = 0;
  int r = _mknod(diri, name, mode, rdev, uid, gid, &in);
  if (r == 0) {
    fill_stat(in, attr);
    _ll_get(in);
  }
  tout(cct) << attr->st_ino << std::endl;
  ldout(cct, 3) << "ll_mknod " << parent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  return r;
}

int Client::_create(Inode *dir, const char *name, int flags, mode_t mode, Inode **inp, Fh **fhp,
    int stripe_unit, int stripe_count, int object_size, const char *data_pool, bool *created, int uid, int gid)
{
  ldout(cct, 3) << "_create(" << dir->ino << " " << name << ", 0" << oct << mode << dec << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;
  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  int cmode = ceph_flags_to_mode(flags);
  if (cmode < 0)
    return -EINVAL;

  int64_t pool_id = -1;
  if (data_pool && *data_pool) {
    pool_id = osdmap->lookup_pg_pool_name(data_pool);
    if (pool_id < 0)
      return -EINVAL;
    if (pool_id > 0xffffffffll)
      return -ERANGE;  // bummer!
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_CREATE);

  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);
  req->set_inode(dir);
  req->head.args.open.flags = flags | O_CREAT;
  req->head.args.open.mode = mode;

  req->head.args.open.stripe_unit = stripe_unit;
  req->head.args.open.stripe_count = stripe_count;
  req->head.args.open.object_size = object_size;
  req->head.args.open.pool = pool_id;
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  bufferlist extra_bl;
  inodeno_t created_ino;

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);

  res = make_request(req, uid, gid, inp, created);
  if (res < 0) {
    goto reply_error;
  }

  (*inp)->get_open_ref(cmode);
  *fhp = _create_fh(*inp, flags, cmode);

 reply_error:
  trim_cache();

  ldout(cct, 3) << "create(" << path << ", 0" << oct << mode << dec 
		<< " layout " << stripe_unit
		<< ' ' << stripe_count
		<< ' ' << object_size
		<<") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}


int Client::_mkdir(Inode *dir, const char *name, mode_t mode, int uid, int gid,
		   Inode **inp)
{
  ldout(cct, 3) << "_mkdir(" << dir->ino << " " << name << ", 0" << oct << mode << dec
	  << ", uid " << uid << ", gid " << gid << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP && dir->snapid != CEPH_SNAPDIR) {
    return -EROFS;
  }
  MetaRequest *req = new MetaRequest(dir->snapid == CEPH_SNAPDIR ? CEPH_MDS_OP_MKSNAP:CEPH_MDS_OP_MKDIR);

  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);
  req->set_inode(dir);
  req->head.args.mkdir.mode = mode;
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL; 

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);
  
  ldout(cct, 10) << "_mkdir: making request" << dendl;
  res = make_request(req, uid, gid, inp);
  ldout(cct, 10) << "_mkdir result is " << res << dendl;

  trim_cache();

  ldout(cct, 3) << "_mkdir(" << path << ", 0" << oct << mode << dec << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_mkdir(vinodeno_t parent, const char *name, mode_t mode, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_mkdir " << parent << " " << name << dendl;
  tout(cct) << "ll_mkdir" << std::endl;
  tout(cct) << parent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;

  Inode *diri = _ll_get_inode(parent);

  Inode *in = 0;
  int r = _mkdir(diri, name, mode, uid, gid, &in);
  if (r == 0) {
    fill_stat(in, attr);
    _ll_get(in);
  }
  tout(cct) << attr->st_ino << std::endl;
  ldout(cct, 3) << "ll_mkdir " << parent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  return r;
}

int Client::_symlink(Inode *dir, const char *name, const char *target, int uid, int gid,
		     Inode **inp)
{
  ldout(cct, 3) << "_symlink(" << dir->ino << " " << name << ", " << target
	  << ", uid " << uid << ", gid " << gid << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_SYMLINK);

  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);
  req->set_inode(dir);
  req->set_string2(target); 
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);

  res = make_request(req, uid, gid, inp);

  trim_cache();
  ldout(cct, 3) << "_symlink(\"" << path << "\", \"" << target << "\") = " << res << dendl;
  return res;


 fail:
  put_request(req);
  return res;
}

int Client::ll_symlink(vinodeno_t parent, const char *name, const char *value, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_symlink " << parent << " " << name << " -> " << value << dendl;
  tout(cct) << "ll_symlink" << std::endl;
  tout(cct) << parent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << value << std::endl;

  Inode *diri = _ll_get_inode(parent);
  Inode *in = 0;
  int r = _symlink(diri, name, value, uid, gid, &in);
  if (r == 0) {
    fill_stat(in, attr);
    _ll_get(in);
  }
  tout(cct) << attr->st_ino << std::endl;
  ldout(cct, 3) << "ll_symlink " << parent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  return r;
}

int Client::_unlink(Inode *dir, const char *name, int uid, int gid)
{
  ldout(cct, 3) << "_unlink(" << dir->ino << " " << name << " uid " << uid << " gid " << gid << ")" << dendl;

  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_UNLINK);

  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  Inode *otherin;
  res = _lookup(dir, name, &otherin);
  if (res < 0)
    goto fail;
  req->set_other_inode(otherin);
  req->other_inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;

  req->set_inode(dir);

  res = make_request(req, uid, gid);
  if (res == 0) {
    if (dir->dir && dir->dir->dentries.count(name)) {
      Dentry *dn = dir->dir->dentries[name];
      unlink(dn, false);
    }
  }
  ldout(cct, 10) << "unlink result is " << res << dendl;

  trim_cache();
  ldout(cct, 3) << "unlink(" << path << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_unlink(vinodeno_t vino, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_unlink " << vino << " " << name << dendl;
  tout(cct) << "ll_unlink" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  Inode *diri = _ll_get_inode(vino);
  return _unlink(diri, name, uid, gid);
}

int Client::_rmdir(Inode *dir, const char *name, int uid, int gid)
{
  ldout(cct, 3) << "_rmdir(" << dir->ino << " " << name << " uid " << uid << " gid " << gid << ")" << dendl;

  if (dir->snapid != CEPH_NOSNAP && dir->snapid != CEPH_SNAPDIR) {
    return -EROFS;
  }

  MetaRequest *req = new MetaRequest(dir->snapid == CEPH_SNAPDIR ? CEPH_MDS_OP_RMSNAP:CEPH_MDS_OP_RMDIR);
  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);

  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;
  req->inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);
  Inode *in;
  res = _lookup(dir, name, &in);
  if (res < 0)
    goto fail;
  req->set_inode(in);

  res = make_request(req, uid, gid);
  if (res == 0) {
    if (dir->dir && dir->dir->dentries.count(name) ) {
      Dentry *dn = dir->dir->dentries[name];
      if (dn->inode->dir && dn->inode->dir->is_empty() &&
          (dn->inode->dn_set.size() == 1))
	close_dir(dn->inode->dir);  // FIXME: maybe i shoudl proactively hose the whole subtree from cache?
      unlink(dn, false);
    }
  }

  trim_cache();
  ldout(cct, 3) << "rmdir(" << path << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_rmdir(vinodeno_t vino, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_rmdir " << vino << " " << name << dendl;
  tout(cct) << "ll_rmdir" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  Inode *diri = _ll_get_inode(vino);
  return _rmdir(diri, name, uid, gid);
}

int Client::_rename(Inode *fromdir, const char *fromname, Inode *todir, const char *toname, int uid, int gid)
{
  ldout(cct, 3) << "_rename(" << fromdir->ino << " " << fromname << " to " << todir->ino << " " << toname
	  << " uid " << uid << " gid " << gid << ")" << dendl;

  if (fromdir->snapid != CEPH_NOSNAP ||
      todir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_RENAME);

  filepath from;
  fromdir->make_nosnap_relative_path(from);
  from.push_dentry(fromname);
  filepath to;
  todir->make_nosnap_relative_path(to);
  to.push_dentry(toname);
  req->set_filepath(to);
  req->set_filepath2(from);

  Dentry *oldde;
  int res = get_or_create(fromdir, fromname, &oldde);
  if (res < 0)
    goto fail;
  req->set_old_dentry(oldde);
  req->old_dentry_drop = CEPH_CAP_FILE_SHARED;
  req->old_dentry_unless = CEPH_CAP_FILE_EXCL;

  Dentry *de;
  res = get_or_create(todir, toname, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  Inode *oldin;
  res = _lookup(fromdir, fromname, &oldin);
  if (res < 0)
    goto fail;
  req->set_old_inode(oldin);
  req->old_inode_drop = CEPH_CAP_LINK_SHARED;

  Inode *otherin;
  res = _lookup(todir, toname, &otherin);
  if (res != 0 && res != -ENOENT) {
    goto fail;
  } else if (res == 0) {
    req->set_other_inode(otherin);
    req->other_inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;
  }

  req->set_inode(todir);

  Inode *target;
  res = make_request(req, uid, gid, &target);

  ldout(cct, 10) << "rename result is " << res << dendl;

  // renamed item from our cache

  trim_cache();
  ldout(cct, 3) << "_rename(" << from << ", " << to << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_rename(vinodeno_t parent, const char *name, vinodeno_t newparent, const char *newname, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_rename " << parent << " " << name << " to "
	  << newparent << " " << newname << dendl;
  tout(cct) << "ll_rename" << std::endl;
  tout(cct) << parent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << newparent.ino.val << std::endl;
  tout(cct) << newname << std::endl;

  Inode *fromdiri = _ll_get_inode(parent);
  Inode *todiri = _ll_get_inode(newparent);
  return _rename(fromdiri, name, todiri, newname, uid, gid);
}

int Client::_link(Inode *in, Inode *dir, const char *newname, int uid, int gid, Inode **inp)
{
  ldout(cct, 3) << "_link(" << in->ino << " to " << dir->ino << " " << newname
	  << " uid " << uid << " gid " << gid << ")" << dendl;

  if (strlen(newname) > NAME_MAX)
    return -ENAMETOOLONG;

  if (in->snapid != CEPH_NOSNAP || dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LINK);

  filepath path(newname, dir->ino);
  req->set_filepath(path);
  filepath existing(in->ino);
  req->set_filepath2(existing);

  req->set_inode(dir);
  req->inode_drop = CEPH_CAP_FILE_SHARED;
  req->inode_unless = CEPH_CAP_FILE_EXCL;

  Dentry *de;
  int res = get_or_create(dir, newname, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);
  
  res = make_request(req, uid, gid, inp);
  ldout(cct, 10) << "link result is " << res << dendl;

  trim_cache();
  ldout(cct, 3) << "link(" << existing << ", " << path << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_link(vinodeno_t vino, vinodeno_t newparent, const char *newname, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_link " << vino << " to " << newparent << " " << newname << dendl;
  tout(cct) << "ll_link" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << newparent << std::endl;
  tout(cct) << newname << std::endl;

  Inode *old = _ll_get_inode(vino);
  Inode *diri = _ll_get_inode(newparent);

  int r = _link(old, diri, newname, uid, gid, &old);
  if (r == 0) {
    Inode *in = _ll_get_inode(vino);
    fill_stat(in, attr);
    _ll_get(in);
  }
  return r;
}

int Client::ll_describe_layout(Fh *fh, ceph_file_layout* lp)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_describe_layout " << fh << " " << fh->inode->ino << dendl;
  tout(cct) << "ll_describe_layout" << std::endl;

  Inode *in = fh->inode;
  *lp = in->layout;

  return 0;
}

int Client::ll_opendir(vinodeno_t vino, void **dirpp, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_opendir " << vino << dendl;
  tout(cct) << "ll_opendir" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  
  Inode *diri = inode_map[vino];
  assert(diri);

  int r = 0;
  if (vino.snapid == CEPH_SNAPDIR) {
    *dirpp = new dir_result_t(diri);
  } else {
    r = _opendir(diri, (dir_result_t**)dirpp);
  }

  tout(cct) << (unsigned long)*dirpp << std::endl;

  ldout(cct, 3) << "ll_opendir " << vino << " = " << r << " (" << *dirpp << ")" << dendl;
  return r;
}

void Client::ll_releasedir(void *dirp)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_releasedir " << dirp << dendl;
  tout(cct) << "ll_releasedir" << std::endl;
  tout(cct) << (unsigned long)dirp << std::endl;
  _closedir(static_cast<dir_result_t*>(dirp));
}

int Client::ll_open(vinodeno_t vino, int flags, Fh **fhp, int uid, int gid)
{
  assert(!(flags & O_CREAT));

  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_open " << vino << " " << flags << dendl;
  tout(cct) << "ll_open" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << flags << std::endl;

  Inode *in = _ll_get_inode(vino);

  int r;
  if (uid < 0) {
    uid = geteuid();
    gid = getegid();
  }
  r = check_permissions(in, flags, uid, gid);
  if (r < 0)
    goto out;

  r = _open(in, flags, 0, fhp, uid, gid);

 out:
  tout(cct) << (unsigned long)*fhp << std::endl;
  ldout(cct, 3) << "ll_open " << vino << " " << flags << " = " << r << " (" << *fhp << ")" << dendl;
  return r;
}

int Client::ll_create(vinodeno_t parent, const char *name, mode_t mode, int flags,
		      struct stat *attr, Fh **fhp, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_create " << parent << " " << name << " 0" << oct << mode << dec << " " << flags << ", uid " << uid << ", gid " << gid << dendl;
  tout(cct) << "ll_create" << std::endl;
  tout(cct) << parent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << flags << std::endl;

  *fhp = NULL;

  bool created = false;
  Inode *in = NULL;
  Inode *dir = _ll_get_inode(parent);
  int r = _lookup(dir, name, &in);
  if (r == 0 && (flags & O_CREAT) && (flags & O_EXCL))
    return -EEXIST;
  if (r == -ENOENT && (flags & O_CREAT)) {
    r = _create(dir, name, flags, mode, &in, fhp,
	        0, 0, 0,
		NULL, &created, uid, gid);
    if (r < 0)
      goto out;

    in = (*fhp)->inode;
  }

  if (r < 0)
    goto out;

  assert(in);
  fill_stat(in, attr);
  _ll_get(in);

  ldout(cct, 20) << "ll_create created = " << created << dendl;
  if (!created) {
    r = check_permissions(in, flags, uid, gid);
    if (r < 0) {
      if (*fhp) {
	_release_fh(*fhp);
      }
      goto out;
    }
    if (*fhp == NULL) {
      r = _open(in, flags, mode, fhp);
      if (r < 0)
	goto out;
    }
  }

out:
  if (r < 0)
    attr->st_ino = 0;

  tout(cct) << (unsigned long)*fhp << std::endl;
  tout(cct) << attr->st_ino << std::endl;
  ldout(cct, 3) << "ll_create " << parent << " " << name << " 0" << oct << mode << dec << " " << flags
	  << " = " << r << " (" << *fhp << " " << hex << attr->st_ino << dec << ")" << dendl;
  return r;
}

int Client::ll_read(Fh *fh, loff_t off, loff_t len, bufferlist *bl)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_read " << fh << " " << fh->inode->ino << " " << " " << off << "~" << len << dendl;
  tout(cct) << "ll_read" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;
  tout(cct) << off << std::endl;
  tout(cct) << len << std::endl;

  return _read(fh, off, len, bl);
}

int Client::ll_write(Fh *fh, loff_t off, loff_t len, const char *data)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_write " << fh << " " << fh->inode->ino << " " << off << "~" << len << dendl;
  tout(cct) << "ll_write" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;
  tout(cct) << off << std::endl;
  tout(cct) << len << std::endl;

  int r = _write(fh, off, len, data);
  ldout(cct, 3) << "ll_write " << fh << " " << off << "~" << len << " = " << r << dendl;
  return r;
}

int Client::ll_flush(Fh *fh)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_flush " << fh << " " << fh->inode->ino << " " << dendl;
  tout(cct) << "ll_flush" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  return _flush(fh);
}

int Client::ll_fsync(Fh *fh, bool syncdataonly) 
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_fsync " << fh << " " << fh->inode->ino << " " << dendl;
  tout(cct) << "ll_fsync" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  return _fsync(fh, syncdataonly);
}

#ifdef FALLOC_FL_PUNCH_HOLE

int Client::_fallocate(Fh *fh, int mode, int64_t offset, int64_t length)
{
  if (offset < 0 || length <= 0)
    return -EINVAL;

  if (mode & ~(FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE))
    return -EOPNOTSUPP;

  if ((mode & FALLOC_FL_PUNCH_HOLE) && !(mode & FALLOC_FL_KEEP_SIZE))
    return -EOPNOTSUPP;

  if (osdmap->test_flag(CEPH_OSDMAP_FULL) && !(mode & FALLOC_FL_PUNCH_HOLE))
    return -ENOSPC;

  Inode *in = fh->inode;

  if (in->snapid != CEPH_NOSNAP)
    return -EROFS;

  if ((fh->mode & CEPH_FILE_MODE_WR) == 0)
    return -EBADF;

  int have;
  int r = get_caps(in, CEPH_CAP_FILE_WR, CEPH_CAP_FILE_BUFFER, &have, -1);
  if (r < 0)
    return r;

  if (mode & FALLOC_FL_PUNCH_HOLE) {
    Mutex flock("Client::_punch_hole flock");
    Cond cond;
    bool done = false;
    Context *onfinish = new C_SafeCond(&flock, &cond, &done);
    Context *onsafe = new C_Client_SyncCommit(this, in);

    unsafe_sync_write++;
    get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

    _invalidate_inode_cache(in, offset, length, true);
    r = filer->zero(in->ino, &in->layout,
                    in->snaprealm->get_snap_context(),
                    offset, length,
                    ceph_clock_now(cct),
                    0, true, onfinish, onsafe);
    if (r < 0)
      goto done;

    in->mtime = ceph_clock_now(cct);
    mark_caps_dirty(in, CEPH_CAP_FILE_WR);

    client_lock.Unlock();
    flock.Lock();
    while (!done)
      cond.Wait(flock);
    flock.Unlock();
    client_lock.Lock();
  } else if (!(mode & FALLOC_FL_KEEP_SIZE)) {
    uint64_t size = offset + length;
    if (size > in->size) {
      in->size = size;
      in->mtime = ceph_clock_now(cct);
      mark_caps_dirty(in, CEPH_CAP_FILE_WR);

      if ((in->size << 1) >= in->max_size &&
          (in->reported_size << 1) < in->max_size)
        check_caps(in, false);
    }
  }

done:
  put_cap_ref(in, CEPH_CAP_FILE_WR);
  return r;
}
#else

int Client::_fallocate(Fh *fh, int mode, int64_t offset, int64_t length)
{
  return -EOPNOTSUPP;
}

#endif


int Client::ll_fallocate(Fh *fh, int mode, loff_t offset, loff_t length)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_fallocate " << fh << " " << fh->inode->ino << " " << dendl;
  tout(cct) << "ll_fallocate " << mode << " " << offset << " " << length << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  return _fallocate(fh, mode, offset, length);
}

int Client::fallocate(int fd, int mode, loff_t offset, loff_t length)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "fallocate " << " " << fd << mode << " " << offset << " " << length << std::endl;

  Fh *fh = get_filehandle(fd);
  if (!fh)
    return -EBADF;
  return _fallocate(fh, mode, offset, length);
}

int Client::ll_release(Fh *fh)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_release " << fh << " " << fh->inode->ino << " " << dendl;
  tout(cct) << "ll_release" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  _release_fh(fh);
  return 0;
}






// =========================================
// layout

// expose file layouts

int Client::describe_layout(const char *relpath, ceph_file_layout *lp)
{
  Mutex::Locker lock(client_lock);

  filepath path(relpath);
  Inode *in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;

  *lp = in->layout;

  ldout(cct, 3) << "describe_layout(" << relpath << ") = 0" << dendl;
  return 0;
}

int Client::fdescribe_layout(int fd, ceph_file_layout *lp)
{
  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode;

  *lp = in->layout;

  ldout(cct, 3) << "fdescribe_layout(" << fd << ") = 0" << dendl;
  return 0;
}


// expose osdmap

int64_t Client::get_pool_id(const char *pool_name)
{
  Mutex::Locker lock(client_lock);
  return osdmap->lookup_pg_pool_name(pool_name);
}

string Client::get_pool_name(int64_t pool)
{
  Mutex::Locker lock(client_lock);
  if (!osdmap->have_pg_pool(pool))
    return string();
  return osdmap->get_pool_name(pool);
}

int Client::get_pool_replication(int64_t pool)
{
  Mutex::Locker lock(client_lock);
  if (!osdmap->have_pg_pool(pool))
    return -ENOENT;
  return osdmap->get_pg_pool(pool)->get_size();
}

int Client::get_file_extent_osds(int fd, loff_t off, loff_t *len, vector<int>& osds)
{
  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode;

  vector<ObjectExtent> extents;
  Striper::file_to_extents(cct, in->ino, &in->layout, off, 1, in->truncate_size, extents);
  assert(extents.size() == 1);

  pg_t pg = osdmap->object_locator_to_pg(extents[0].oid, extents[0].oloc);
  osdmap->pg_to_acting_osds(pg, osds);
  if (osds.empty())
    return -EINVAL;

  /*
   * Return the remainder of the extent (stripe unit)
   *
   * If length = 1 is passed to Striper::file_to_extents we get a single
   * extent back, but its length is one so we still need to compute the length
   * to the end of the stripe unit.
   *
   * If length = su then we may get 1 or 2 objects back in the extents vector
   * which would have to be examined. Even then, the offsets are local to the
   * object, so matching up to the file offset is extra work.
   *
   * It seems simpler to stick with length = 1 and manually compute the
   * remainder.
   */
  if (len) {
    uint64_t su = in->layout.fl_stripe_unit;
    *len = su - (off % su);
  }

  return 0;
}

int Client::get_osd_crush_location(int id, vector<pair<string, string> >& path)
{
  Mutex::Locker lock(client_lock);
  return osdmap->crush->get_full_location_ordered(id, path);
}

int Client::get_file_stripe_address(int fd, loff_t offset, vector<entity_addr_t>& address)
{
  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode;

  // which object?
  vector<ObjectExtent> extents;
  Striper::file_to_extents(cct, in->ino, &in->layout, offset, 1, in->truncate_size, extents);
  assert(extents.size() == 1);

  // now we have the object and its 'layout'
  pg_t pg = osdmap->object_locator_to_pg(extents[0].oid, extents[0].oloc);
  vector<int> osds;
  osdmap->pg_to_acting_osds(pg, osds);
  if (osds.empty())
    return -EINVAL;

  for (unsigned i = 0; i < osds.size(); i++) {
    entity_addr_t addr = osdmap->get_addr(osds[i]);
    address.push_back(addr);
  }

  return 0;
}

int Client::get_osd_addr(int osd, entity_addr_t& addr)
{
  Mutex::Locker lock(client_lock);

  if (!osdmap->exists(osd))
    return -ENOENT;

  addr = osdmap->get_addr(osd);

  return 0;
}

int Client::enumerate_layout(int fd, vector<ObjectExtent>& result,
			     loff_t length, loff_t offset)
{
  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode;

  // map to a list of extents
  Striper::file_to_extents(cct, in->ino, &in->layout, offset, length, in->truncate_size, result);

  ldout(cct, 3) << "enumerate_layout(" << fd << ", " << length << ", " << offset << ") = 0" << dendl;
  return 0;
}


/*
 * find an osd with the same ip.  -1 if none.
 */
int Client::get_local_osd()
{
  Mutex::Locker lock(client_lock);

  if (osdmap->get_epoch() != local_osd_epoch) {
    local_osd = osdmap->find_osd_on_ip(messenger->get_myaddr());
    local_osd_epoch = osdmap->get_epoch();
  }
  return local_osd;
}






// ===============================

void Client::ms_handle_connect(Connection *con)
{
  ldout(cct, 10) << "ms_handle_connect on " << con->get_peer_addr() << dendl;
  Mutex::Locker l(client_lock);
  objecter->ms_handle_connect(con);
}

bool Client::ms_handle_reset(Connection *con) 
{
  ldout(cct, 0) << "ms_handle_reset on " << con->get_peer_addr() << dendl;
  Mutex::Locker l(client_lock);
  objecter->ms_handle_reset(con);
  return false;
}

void Client::ms_handle_remote_reset(Connection *con) 
{
  ldout(cct, 0) << "ms_handle_remote_reset on " << con->get_peer_addr() << dendl;
  Mutex::Locker l(client_lock);
  switch (con->get_peer_type()) {
  case CEPH_ENTITY_TYPE_OSD:
    objecter->ms_handle_remote_reset(con);
    break;

  case CEPH_ENTITY_TYPE_MDS:
    {
      // kludge to figure out which mds this is; fixme with a Connection* state
      int mds = -1;
      MetaSession *s = NULL;
      for (map<int,MetaSession*>::iterator p = mds_sessions.begin();
	   p != mds_sessions.end();
	   ++p) {
	if (mdsmap->get_addr(p->first) == con->get_peer_addr()) {
	  mds = p->first;
	  s = p->second;
	}
      }
      if (mds >= 0) {
	switch (s->state) {
	case MetaSession::STATE_CLOSING:
	  ldout(cct, 1) << "reset from mds we were closing; we'll call that closed" << dendl;
	  _closed_mds_session(s);
	  break;

	case MetaSession::STATE_OPENING:
	  {
	    ldout(cct, 1) << "reset from mds we were opening; retrying" << dendl;
	    list<Context*> waiters;
	    waiters.swap(s->waiting_for_open);
	    _closed_mds_session(s);
	    MetaSession *news = _get_or_open_mds_session(mds);
	    news->waiting_for_open.swap(waiters);
	  }
	  break;

	case MetaSession::STATE_OPEN:
	case MetaSession::STATE_NEW:
	case MetaSession::STATE_CLOSED:
	default:
	  break;
	}
      }
    }
    break;
  }
}

bool Client::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new)
{
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;
  *authorizer = monclient->auth->build_authorizer(dest_type);
  return true;
}

void Client::set_filer_flags(int flags)
{
  Mutex::Locker l(client_lock);
  assert(flags == 0 ||
	 flags == CEPH_OSD_FLAG_LOCALIZE_READS);
  objecter->add_global_op_flags(flags);
}

void Client::clear_filer_flags(int flags)
{
  Mutex::Locker l(client_lock);
  assert(flags == CEPH_OSD_FLAG_LOCALIZE_READS);
  objecter->clear_global_op_flag(flags);
}

