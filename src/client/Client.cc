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
#include <sys/file.h>
#include <sys/utsname.h>
#include <sys/uio.h>

#include <boost/lexical_cast.hpp>
#include <boost/fusion/include/std_pair.hpp>

#if defined(__FreeBSD__)
#define XATTR_CREATE    0x1
#define XATTR_REPLACE   0x2
#else
#include <sys/xattr.h>
#endif

#if defined(__linux__)
#include <linux/falloc.h>
#endif

#include <sys/statvfs.h>

#include "common/config.h"
#include "common/version.h"

// ceph stuff
#include "messages/MClientSession.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientReply.h"
#include "messages/MClientCaps.h"
#include "messages/MClientLease.h"
#include "messages/MClientSnap.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDMap.h"
#include "messages/MClientQuota.h"
#include "messages/MClientCapRelease.h"
#include "messages/MMDSMap.h"
#include "messages/MFSMap.h"
#include "messages/MFSMapUser.h"

#include "mon/MonClient.h"

#include "mds/flock.h"
#include "osd/OSDMap.h"
#include "osdc/Filer.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/perf_counters.h"
#include "common/admin_socket.h"
#include "common/errno.h"
#include "include/str_list.h"

#define dout_subsys ceph_subsys_client

#include "include/lru.h"
#include "include/compat.h"
#include "include/stringify.h"

#include "Client.h"
#include "Inode.h"
#include "Dentry.h"
#include "Dir.h"
#include "ClientSnapRealm.h"
#include "Fh.h"
#include "MetaSession.h"
#include "MetaRequest.h"
#include "ObjecterWriteback.h"
#include "posix_acl.h"

#include "include/assert.h"
#include "include/stat.h"

#include "include/cephfs/ceph_statx.h"

#if HAVE_GETGROUPLIST
#include <grp.h>
#include <pwd.h>
#include <unistd.h>
#endif

#undef dout_prefix
#define dout_prefix *_dout << "client." << whoami << " "

#define  tout(cct)       if (!cct->_conf->client_trace.empty()) traceout

// FreeBSD fails to define this
#ifndef O_DSYNC
#define O_DSYNC 0x0
#endif
// Darwin fails to define this
#ifndef O_RSYNC
#define O_RSYNC 0x0
#endif

#ifndef O_DIRECT
#define O_DIRECT 0x0
#endif

#define DEBUG_GETATTR_CAPS (CEPH_CAP_XATTR_SHARED)

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
  Formatter *f = Formatter::create(format);
  f->open_object_section("result");
  m_client->client_lock.Lock();
  if (command == "mds_requests")
    m_client->dump_mds_requests(f);
  else if (command == "mds_sessions")
    m_client->dump_mds_sessions(f);
  else if (command == "dump_cache")
    m_client->dump_cache(f);
  else if (command == "kick_stale_sessions")
    m_client->_kick_stale_sessions();
  else if (command == "status")
    m_client->dump_status(f);
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
  : inode(in), owner_uid(-1), owner_gid(-1), offset(0), next_offset(2),
    release_count(0), ordered_count(0), cache_index(0), start_shared_gen(0)
  { }

void Client::_reset_faked_inos()
{
  ino_t start = 1024;
  free_faked_inos.clear();
  free_faked_inos.insert(start, (uint32_t)-1 - start + 1);
  last_used_faked_ino = 0;
  _use_faked_inos = sizeof(ino_t) < 8 || cct->_conf->client_use_faked_inos;
}

void Client::_assign_faked_ino(Inode *in)
{
  interval_set<ino_t>::const_iterator it = free_faked_inos.lower_bound(last_used_faked_ino + 1);
  if (it == free_faked_inos.end() && last_used_faked_ino > 0) {
    last_used_faked_ino = 0;
    it = free_faked_inos.lower_bound(last_used_faked_ino + 1);
  }
  assert(it != free_faked_inos.end());
  if (last_used_faked_ino < it.get_start()) {
    assert(it.get_len() > 0);
    last_used_faked_ino = it.get_start();
  } else {
    ++last_used_faked_ino;
    assert(it.get_start() + it.get_len() > last_used_faked_ino);
  }
  in->faked_ino = last_used_faked_ino;
  free_faked_inos.erase(in->faked_ino);
  faked_ino_map[in->faked_ino] = in->vino();
}

void Client::_release_faked_ino(Inode *in)
{
  free_faked_inos.insert(in->faked_ino);
  faked_ino_map.erase(in->faked_ino);
}

vinodeno_t Client::_map_faked_ino(ino_t ino)
{
  vinodeno_t vino;
  if (ino == 1)
    vino = root->vino();
  else if (faked_ino_map.count(ino))
    vino = faked_ino_map[ino];
  else
    vino = vinodeno_t(0, CEPH_NOSNAP);
  ldout(cct, 10) << "map_faked_ino " << ino << " -> " << vino << dendl;
  return vino;
}

vinodeno_t Client::map_faked_ino(ino_t ino)
{
  Mutex::Locker lock(client_lock);
  return _map_faked_ino(ino);
}

// cons/des

Client::Client(Messenger *m, MonClient *mc)
  : Dispatcher(m->cct),
    logger(NULL),
    m_command_hook(this),
    timer(m->cct, client_lock),
    callback_handle(NULL),
    switch_interrupt_cb(NULL),
    remount_cb(NULL),
    ino_invalidate_cb(NULL),
    dentry_invalidate_cb(NULL),
    getgroups_cb(NULL),
    umask_cb(NULL),
    can_invalidate_dentries(false),
    require_remount(false),
    async_ino_invalidator(m->cct),
    async_dentry_invalidator(m->cct),
    interrupt_finisher(m->cct),
    remount_finisher(m->cct),
    objecter_finisher(m->cct),
    tick_event(NULL),
    monclient(mc), messenger(m), whoami(m->get_myname().num()),
    cap_epoch_barrier(0), fsmap(nullptr), fsmap_user(nullptr),
    last_tid(0), oldest_tid(0), last_flush_tid(1),
    initialized(false), authenticated(false),
    mounted(false), unmounting(false),
    local_osd(-1), local_osd_epoch(0),
    unsafe_sync_write(0),
    client_lock("Client::client_lock")
{
  monclient->set_messenger(m);

  _reset_faked_inos();
  //
  root = 0;

  num_flushing_caps = 0;

  _dir_vxattrs_name_size = _vxattrs_calcu_name_size(_dir_vxattrs);
  _file_vxattrs_name_size = _vxattrs_calcu_name_size(_file_vxattrs);

  user_id = cct->_conf->client_mount_uid;
  group_id = cct->_conf->client_mount_gid;

  acl_type = NO_ACL;
  if (cct->_conf->client_acl_type == "posix_acl")
    acl_type = POSIX_ACL;

  lru.lru_set_max(cct->_conf->client_cache_size);
  lru.lru_set_midpoint(cct->_conf->client_cache_mid);

  // file handles
  free_fd_set.insert(10, 1<<30);

  // set up messengers
  messenger = m;

  // osd interfaces
  mdsmap = new MDSMap;
  objecter = new Objecter(cct, messenger, monclient, NULL,
			  0, 0);
  objecter->set_client_incarnation(0);  // client always 0, for now.
  writeback_handler = new ObjecterWriteback(objecter, &objecter_finisher,
					    &client_lock);
  objectcacher = new ObjectCacher(cct, "libcephfs", *writeback_handler, client_lock,
				  client_flush_set_callback,    // all commit callback
				  (void*)this,
				  cct->_conf->client_oc_size,
				  cct->_conf->client_oc_max_objects,
				  cct->_conf->client_oc_max_dirty,
				  cct->_conf->client_oc_target_dirty,
				  cct->_conf->client_oc_max_dirty_age,
				  true);
  objecter_finisher.start();
  filer = new Filer(objecter, &objecter_finisher);
}


Client::~Client()
{
  assert(!client_lock.is_locked());

  tear_down_cache();

  delete objectcacher;
  delete writeback_handler;

  delete filer;
  delete objecter;
  delete mdsmap;
  delete fsmap;

  delete logger;
}

void Client::tear_down_cache()
{
  // fd's
  for (ceph::unordered_map<int, Fh*>::iterator it = fd_map.begin();
       it != fd_map.end();
       ++it) {
    Fh *fh = it->second;
    ldout(cct, 1) << "tear_down_cache forcing close of fh " << it->first << " ino " << fh->inode->ino << dendl;
    _release_fh(fh);
  }
  fd_map.clear();

  while (!opened_dirs.empty()) {
    dir_result_t *dirp = *opened_dirs.begin();
    ldout(cct, 1) << "tear_down_cache forcing close of dir " << dirp << " ino " << dirp->inode->ino << dendl;
    _closedir(dirp);
  }

  // caps!
  // *** FIXME ***

  // empty lru
  lru.lru_set_max(0);
  trim_cache();
  assert(lru.lru_get_size() == 0);

  // close root ino
  assert(inode_map.size() <= 1 + root_parents.size());
  if (root && inode_map.size() == 1 + root_parents.size()) {
    delete root;
    root = 0;
    root_ancestor = 0;
    while (!root_parents.empty())
      root_parents.erase(root_parents.begin());
    inode_map.clear();
    _reset_faked_inos();
  }

  assert(inode_map.empty());
}

inodeno_t Client::get_root_ino()
{
  Mutex::Locker l(client_lock);
  if (use_faked_inos())
    return root->faked_ino;
  else
    return root->ino;
}

Inode *Client::get_root()
{
  Mutex::Locker l(client_lock);
  root->ll_get();
  return root;
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
    for (ceph::unordered_map<string, Dentry*>::iterator it = in->dir->dentries.begin();
         it != in->dir->dentries.end();
         ++it) {
      ldout(cct, 1) << "   " << in->ino << " dn " << it->first << " " << it->second << " ref " << it->second->ref << dendl;
      if (f) {
	f->open_object_section("dentry");
	it->second->dump(f);
	f->close_section();
      }	
      if (it->second->inode)
	dump_inode(f, it->second->inode.get(), did, false);
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
  for (ceph::unordered_map<vinodeno_t, Inode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       ++it) {
    if (did.count(it->second))
      continue;
    dump_inode(f, it->second, did, true);
  }

  if (f)
    f->close_section();
}

void Client::dump_status(Formatter *f)
{
  assert(client_lock.is_locked_by_me());

  ldout(cct, 1) << __func__ << dendl;

  const epoch_t osd_epoch
    = objecter->with_osdmap(std::mem_fn(&OSDMap::get_epoch));

  if (f) {
    f->open_object_section("metadata");
    for (const auto& kv : metadata)
      f->dump_string(kv.first.c_str(), kv.second);
    f->close_section();

    f->dump_int("dentry_count", lru.lru_get_size());
    f->dump_int("dentry_pinned_count", lru.lru_get_num_pinned());
    f->dump_int("inode_count", inode_map.size());
    f->dump_int("mds_epoch", mdsmap->get_epoch());
    f->dump_int("osd_epoch", osd_epoch);
    f->dump_int("osd_epoch_barrier", cap_epoch_barrier);
  }
}

int Client::init()
{
  timer.init();
  objectcacher->start();
  objecter->init();

  client_lock.Lock();
  assert(!initialized);

  // ok!
  messenger->add_dispatcher_tail(objecter);
  messenger->add_dispatcher_tail(this);

  int r = monclient->init();
  if (r < 0) {
    // need to do cleanup because we're in an intermediate init state
    timer.shutdown();
    client_lock.Unlock();
    objecter->shutdown();
    objectcacher->stop();
    monclient->shutdown();
    return r;
  }
  objecter->start();

  monclient->set_want_keys(CEPH_ENTITY_TYPE_MDS | CEPH_ENTITY_TYPE_OSD);

  // logger
  PerfCountersBuilder plb(cct, "client", l_c_first, l_c_last);
  plb.add_time_avg(l_c_reply, "reply", "Latency of receiving a reply on metadata request");
  plb.add_time_avg(l_c_lat, "lat", "Latency of processing a metadata request");
  plb.add_time_avg(l_c_wrlat, "wrlat", "Latency of a file data write operation");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);

  client_lock.Unlock();

  cct->_conf->add_observer(this);

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
  ret = admin_socket->register_command("kick_stale_sessions",
				       "kick_stale_sessions",
				       &m_command_hook,
				       "kick sessions that were remote reset");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }
  ret = admin_socket->register_command("status",
				       "status",
				       &m_command_hook,
				       "show overall client status");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }

  populate_metadata();

  client_lock.Lock();
  initialized = true;
  client_lock.Unlock();
  return r;
}

void Client::shutdown() 
{
  ldout(cct, 1) << "shutdown" << dendl;

  // If we were not mounted, but were being used for sending
  // MDS commands, we may have sessions that need closing.
  client_lock.Lock();
  _close_sessions();
  client_lock.Unlock();

  cct->_conf->remove_observer(this);

  AdminSocket* admin_socket = cct->get_admin_socket();
  admin_socket->unregister_command("mds_requests");
  admin_socket->unregister_command("mds_sessions");
  admin_socket->unregister_command("dump_cache");
  admin_socket->unregister_command("kick_stale_sessions");
  admin_socket->unregister_command("status");

  if (ino_invalidate_cb) {
    ldout(cct, 10) << "shutdown stopping cache invalidator finisher" << dendl;
    async_ino_invalidator.wait_for_empty();
    async_ino_invalidator.stop();
  }

  if (dentry_invalidate_cb) {
    ldout(cct, 10) << "shutdown stopping dentry invalidator finisher" << dendl;
    async_dentry_invalidator.wait_for_empty();
    async_dentry_invalidator.stop();
  }

  if (switch_interrupt_cb) {
    ldout(cct, 10) << "shutdown stopping interrupt finisher" << dendl;
    interrupt_finisher.wait_for_empty();
    interrupt_finisher.stop();
  }

  if (remount_cb) {
    ldout(cct, 10) << "shutdown stopping remount finisher" << dendl;
    remount_finisher.wait_for_empty();
    remount_finisher.stop();
  }

  objectcacher->stop();  // outside of client_lock! this does a join.

  client_lock.Lock();
  assert(initialized);
  initialized = false;
  timer.shutdown();
  client_lock.Unlock();

  objecter->shutdown();
  objecter_finisher.wait_for_empty();
  objecter_finisher.stop();

  monclient->shutdown();

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = NULL;
  }
}




// ===================
// metadata cache stuff

void Client::trim_cache(bool trim_kernel_dcache)
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

  if (trim_kernel_dcache && lru.lru_get_size() > lru.lru_get_max())
    _invalidate_kernel_dcache();

  // hose root?
  if (lru.lru_get_size() == 0 && root && root->get_num_ref() == 0 && inode_map.size() == 1 + root_parents.size()) {
    ldout(cct, 15) << "trim_cache trimmed root " << root << dendl;
    delete root;
    root = 0;
    root_ancestor = 0;
    while (!root_parents.empty())
      root_parents.erase(root_parents.begin());
    inode_map.clear();
    _reset_faked_inos();
  }
}

void Client::trim_cache_for_reconnect(MetaSession *s)
{
  mds_rank_t mds = s->mds_num;
  ldout(cct, 20) << "trim_cache_for_reconnect mds." << mds << dendl;

  int trimmed = 0;
  list<Dentry*> skipped;
  while (lru.lru_get_size() > 0) {
    Dentry *dn = static_cast<Dentry*>(lru.lru_expire());
    if (!dn)
      break;

    if ((dn->inode && dn->inode->caps.count(mds)) ||
	dn->dir->parent_inode->caps.count(mds)) {
      trim_dentry(dn);
      trimmed++;
    } else
      skipped.push_back(dn);
  }

  for(list<Dentry*>::iterator p = skipped.begin(); p != skipped.end(); ++p)
    lru.lru_insert_mid(*p);

  ldout(cct, 20) << "trim_cache_for_reconnect mds." << mds
		 << " trimmed " << trimmed << " dentries" << dendl;

  if (s->caps.size() > 0)
    _invalidate_kernel_dcache();
}

void Client::trim_dentry(Dentry *dn)
{
  ldout(cct, 15) << "trim_dentry unlinking dn " << dn->name 
		 << " in dir " << hex << dn->dir->parent_inode->ino 
		 << dendl;
  if (dn->inode) {
    Inode *diri = dn->dir->parent_inode;
    diri->dir_release_count++;
    clear_dir_complete_and_ordered(diri, true);
  }
  unlink(dn, false, false);  // drop dir, drop dentry
}


void Client::update_inode_file_bits(Inode *in,
				    uint64_t truncate_seq, uint64_t truncate_size,
				    uint64_t size, uint64_t change_attr,
				    uint64_t time_warp_seq, utime_t ctime,
				    utime_t mtime,
				    utime_t atime,
				    version_t inline_version,
				    bufferlist& inline_data,
				    int issued)
{
  bool warn = false;
  ldout(cct, 10) << "update_inode_file_bits " << *in << " " << ccap_string(issued)
	   << " mtime " << mtime << dendl;
  ldout(cct, 25) << "truncate_seq: mds " << truncate_seq <<  " local "
	   << in->truncate_seq << " time_warp_seq: mds " << time_warp_seq
	   << " local " << in->time_warp_seq << dendl;
  uint64_t prior_size = in->size;

  if (inline_version > in->inline_version) {
    in->inline_data = inline_data;
    in->inline_version = inline_version;
  }

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
	_invalidate_inode_cache(in, truncate_size, prior_size - truncate_size);
      }
    }

    // truncate inline data
    if (in->inline_version < CEPH_INLINE_NONE) {
      uint32_t len = in->inline_data.length();
      if (size < len)
        in->inline_data.splice(size, len - size);
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
    if (change_attr > in->change_attr)
      in->change_attr = change_attr;
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

void Client::_fragmap_remove_non_leaves(Inode *in)
{
  for (map<frag_t,int>::iterator p = in->fragmap.begin(); p != in->fragmap.end(); )
    if (!in->dirfragtree.is_leaf(p->first))
      in->fragmap.erase(p++);
    else
      ++p;
}

Inode * Client::add_update_inode(InodeStat *st, utime_t from,
				 MetaSession *session)
{
  Inode *in;
  bool was_new = false;
  if (inode_map.count(st->vino)) {
    in = inode_map[st->vino];
    ldout(cct, 12) << "add_update_inode had " << *in << " caps " << ccap_string(st->cap.caps) << dendl;
  } else {
    in = new Inode(this, st->vino, &st->layout);
    inode_map[st->vino] = in;

    if (use_faked_inos())
      _assign_faked_ino(in);

    if (!root) {
      root = in;
      root_ancestor = in;
      cwd = root;
    } else if (!mounted) {
      root_parents[root_ancestor] = in;
      root_ancestor = in;
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
      in->btime = st->btime;
    }

    if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
      in->nlink = st->nlink;
    }

    in->dirstat = st->dirstat;
    in->rstat = st->rstat;
    in->quota = st->quota;
    in->layout = st->layout;

    if (in->is_dir()) {
      in->dir_layout = st->dir_layout;
      ldout(cct, 20) << " dir hash is " << (int)in->dir_layout.dl_dir_hash << dendl;
    }

    update_inode_file_bits(in, st->truncate_seq, st->truncate_size, st->size,
			   st->change_attr, st->time_warp_seq, st->ctime,
			   st->mtime, st->atime, st->inline_version,
			   st->inline_data, issued);
  } else if (st->inline_version > in->inline_version) {
    in->inline_data = st->inline_data;
    in->inline_version = st->inline_version;
  }

  if ((in->xattr_version  == 0 || !(issued & CEPH_CAP_XATTR_EXCL)) &&
      st->xattrbl.length() &&
      st->xattr_version > in->xattr_version) {
    bufferlist::iterator p = st->xattrbl.begin();
    ::decode(in->xattrs, p);
    in->xattr_version = st->xattr_version;
  }

  // move me if/when version reflects fragtree changes.
  if (in->dirfragtree != st->dirfragtree) {
    in->dirfragtree = st->dirfragtree;
    _fragmap_remove_non_leaves(in);
  }

  if (in->snapid == CEPH_NOSNAP) {
    add_update_cap(in, session, st->cap.cap_id, st->cap.caps, st->cap.seq, st->cap.mseq, inodeno_t(st->cap.realm), st->cap.flags);
    if (in->auth_cap && in->auth_cap->session == session)
      in->max_size = st->max_size;
  } else
    in->snap_caps |= st->cap.caps;

  // setting I_COMPLETE needs to happen after adding the cap
  if (updating_inode &&
      in->is_dir() &&
      (st->cap.caps & CEPH_CAP_FILE_SHARED) &&
      (issued & CEPH_CAP_FILE_EXCL) == 0 &&
      in->dirstat.nfiles == 0 &&
      in->dirstat.nsubdirs == 0) {
    ldout(cct, 10) << " marking (I_COMPLETE|I_DIR_ORDERED) on empty dir " << *in << dendl;
    in->flags |= I_COMPLETE | I_DIR_ORDERED;
    if (in->dir) {
      ldout(cct, 10) << " dir is open on empty dir " << in->ino << " with "
		     << in->dir->dentries.size() << " entries, marking all dentries null" << dendl;
      in->dir->readdir_cache.clear();
      for (auto p = in->dir->dentries.begin();
	   p != in->dir->dentries.end();
	   ++p) {
	unlink(p->second, true, true);  // keep dir, keep dentry
      }
      if (in->dir->dentries.empty())
	close_dir(in->dir);
    }
  }

  return in;
}


/*
 * insert_dentry_inode - insert + link a single dentry + inode into the metadata cache.
 */
Dentry *Client::insert_dentry_inode(Dir *dir, const string& dname, LeaseStat *dlease, 
				    Inode *in, utime_t from, MetaSession *session,
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
      unlink(dn, true, true);  // keep dir, keep dentry
    }
  }
  
  if (!dn || !dn->inode) {
    InodeRef tmp_ref(in);
    if (old_dentry) {
      if (old_dentry->dir != dir) {
	Inode *old_diri = old_dentry->dir->parent_inode;
	old_diri->dir_ordered_count++;
	clear_dir_complete_and_ordered(old_diri, false);
      }
      unlink(old_dentry, dir == old_dentry->dir, false);  // drop dentry, keep dir open if its the same dir
    }
    Inode *diri = dir->parent_inode;
    diri->dir_ordered_count++;
    clear_dir_complete_and_ordered(diri, false);
    dn = link(dir, dname, in, dn);
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
  if (!in->dirfragtree.is_leaf(dst->frag)) {
    in->dirfragtree.force_to_leaf(cct, dst->frag);
    _fragmap_remove_non_leaves(in);
  }

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

void Client::clear_dir_complete_and_ordered(Inode *diri, bool complete)
{
  if (diri->flags & I_COMPLETE) {
    if (complete) {
      ldout(cct, 10) << " clearing (I_COMPLETE|I_DIR_ORDERED) on " << *diri << dendl;
      diri->flags &= ~(I_COMPLETE | I_DIR_ORDERED);
    } else {
      if (diri->flags & I_DIR_ORDERED) {
	ldout(cct, 10) << " clearing I_DIR_ORDERED on " << *diri << dendl;
	diri->flags &= ~I_DIR_ORDERED;
      }
    }
    if (diri->dir)
      diri->dir->readdir_cache.clear();
  }
}

/*
 * insert results from readdir or lssnap into the metadata cache.
 */
void Client::insert_readdir_results(MetaRequest *request, MetaSession *session, Inode *diri) {

  MClientReply *reply = request->reply;
  ConnectionRef con = request->reply->get_connection();
  uint64_t features = con->get_features();

  dir_result_t *dirp = request->dirp;
  assert(dirp);

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
    __u16 flags;
    ::decode(numdn, p);
    ::decode(flags, p);

    bool end = ((unsigned)flags & CEPH_READDIR_FRAG_END);
    bool hash_order = ((unsigned)flags & CEPH_READDIR_HASH_ORDER);

    frag_t fg = (unsigned)request->head.args.readdir.frag;
    unsigned readdir_offset = dirp->next_offset;
    string readdir_start = dirp->last_name;

    unsigned last_hash = 0;
    if (!readdir_start.empty())
      last_hash = ceph_frag_value(diri->hash_dentry_name(readdir_start));

    if (fg != dst.frag) {
      ldout(cct, 10) << "insert_trace got new frag " << fg << " -> " << dst.frag << dendl;
      fg = dst.frag;
      if (!hash_order) {
	readdir_offset = 2;
	readdir_start.clear();
	dirp->offset = dir_result_t::make_fpos(fg, readdir_offset, false);
      }
    }

    ldout(cct, 10) << __func__ << " " << numdn << " readdir items, end=" << end
		   << ", hash_order=" << hash_order << ", offset " << readdir_offset
		   << ", readdir_start " << readdir_start << dendl;

    if (diri->snapid != CEPH_SNAPDIR &&
	fg.is_leftmost() && readdir_offset == 2) {
      dirp->release_count = diri->dir_release_count;
      dirp->ordered_count = diri->dir_ordered_count;
      dirp->start_shared_gen = diri->shared_gen;
      dirp->cache_index = 0;
    }

    dirp->buffer_frag = fg;

    _readdir_drop_dirp_buffer(dirp);
    dirp->buffer.reserve(numdn);

    string dname;
    LeaseStat dlease;
    for (unsigned i=0; i<numdn; i++) {
      ::decode(dname, p);
      ::decode(dlease, p);
      InodeStat ist(p, features);

      ldout(cct, 15) << "" << i << ": '" << dname << "'" << dendl;

      Inode *in = add_update_inode(&ist, request->sent_stamp, session);
      Dentry *dn;
      if (diri->dir->dentries.count(dname)) {
	Dentry *olddn = diri->dir->dentries[dname];
	if (olddn->inode != in) {
	  // replace incorrect dentry
	  unlink(olddn, true, true);  // keep dir, dentry
	  dn = link(dir, dname, in, olddn);
	  assert(dn == olddn);
	} else {
	  // keep existing dn
	  dn = olddn;
	  touch_dn(dn);
	}
      } else {
	// new dn
	dn = link(dir, dname, in, NULL);
      }

      update_dentry_lease(dn, &dlease, request->sent_stamp, session);
      if (hash_order) {
	unsigned hash = ceph_frag_value(diri->hash_dentry_name(dname));
	if (hash != last_hash)
	  readdir_offset = 2;
	last_hash = hash;
	dn->offset = dir_result_t::make_fpos(hash, readdir_offset++, true);
      } else {
	dn->offset = dir_result_t::make_fpos(fg, readdir_offset++, false);
      }
      // add to readdir cache
      if (dirp->release_count == diri->dir_release_count &&
	  dirp->ordered_count == diri->dir_ordered_count &&
	  dirp->start_shared_gen == diri->shared_gen) {
	if (dirp->cache_index == dir->readdir_cache.size()) {
	  if (i == 0) {
	    assert(!dirp->inode->is_complete_and_ordered());
	    dir->readdir_cache.reserve(dirp->cache_index + numdn);
	  }
	  dir->readdir_cache.push_back(dn);
	} else if (dirp->cache_index < dir->readdir_cache.size()) {
	  if (dirp->inode->is_complete_and_ordered())
	    assert(dir->readdir_cache[dirp->cache_index] == dn);
	  else
	    dir->readdir_cache[dirp->cache_index] = dn;
	} else {
	  assert(0 == "unexpected readdir buffer idx");
	}
	dirp->cache_index++;
      }
      // add to cached result list
      dirp->buffer.push_back(dir_result_t::dentry(dn->offset, dname, in));
      ldout(cct, 15) << __func__ << "  " << hex << dn->offset << dec << ": '" << dname << "' -> " << in->ino << dendl;
    }

    if (numdn > 0)
      dirp->last_name = dname;
    if (end)
      dirp->next_offset = 2;
    else
      dirp->next_offset = readdir_offset;

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
  int op = request->get_op();

  ldout(cct, 10) << "insert_trace from " << request->sent_stamp << " mds." << session->mds_num
	   << " is_target=" << (int)reply->head.is_target
	   << " is_dentry=" << (int)reply->head.is_dentry
	   << dendl;

  bufferlist::iterator p = reply->get_trace_bl().begin();
  if (request->got_unsafe) {
    ldout(cct, 10) << "insert_trace -- already got unsafe; ignoring" << dendl;
    assert(p.end());
    return NULL;
  }

  if (p.end()) {
    ldout(cct, 10) << "insert_trace -- no trace" << dendl;

    Dentry *d = request->dentry();
    if (d) {
      Inode *diri = d->dir->parent_inode;
      diri->dir_release_count++;
      clear_dir_complete_and_ordered(diri, true);
    }

    if (d && reply->get_result() == 0) {
      if (op == CEPH_MDS_OP_RENAME) {
	// rename
	Dentry *od = request->old_dentry();
	ldout(cct, 10) << " unlinking rename src dn " << od << " for traceless reply" << dendl;
	assert(od);
	unlink(od, true, true);  // keep dir, dentry
      } else if (op == CEPH_MDS_OP_RMDIR ||
		 op == CEPH_MDS_OP_UNLINK) {
	// unlink, rmdir
	ldout(cct, 10) << " unlinking unlink/rmdir dn " << d << " for traceless reply" << dendl;
	unlink(d, true, true);  // keep dir, dentry
      }
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
    if (cct->_conf->client_debug_getattr_caps) {
      unsigned wanted = 0;
      if (op == CEPH_MDS_OP_GETATTR || op == CEPH_MDS_OP_LOOKUP)
	wanted = request->head.args.getattr.mask;
      else if (op == CEPH_MDS_OP_OPEN || op == CEPH_MDS_OP_OPEN)
	wanted = request->head.args.open.mask;

      if ((wanted & CEPH_CAP_XATTR_SHARED) &&
	  !(ist.xattr_version > 0 && ist.xattrbl.length() > 0))
	  assert(0 == "MDS reply does not contain xattrs");
    }

    in = add_update_inode(&ist, request->sent_stamp, session);
  }

  Inode *diri = NULL;
  if (reply->head.is_dentry) {
    diri = add_update_inode(&dirst, request->sent_stamp, session);
    update_dir_dist(diri, &dst);  // dir stat info is attached to ..

    if (in) {
      Dir *dir = diri->open_dir();
      insert_dentry_inode(dir, dname, &dlease, in, request->sent_stamp, session,
                          (op == CEPH_MDS_OP_RENAME) ? request->old_dentry() : NULL);
    } else {
      if (diri->dir && diri->dir->dentries.count(dname)) {
	Dentry *dn = diri->dir->dentries[dname];
	if (dn->inode) {
	  diri->dir_ordered_count++;
	  clear_dir_complete_and_ordered(diri, false);
	  unlink(dn, true, true);  // keep dir, dentry
	}
      }
    }
  } else if (op == CEPH_MDS_OP_LOOKUPSNAP ||
	     op == CEPH_MDS_OP_MKSNAP) {
    ldout(cct, 10) << " faking snap lookup weirdness" << dendl;
    // fake it for snap lookup
    vinodeno_t vino = ist.vino;
    vino.snapid = CEPH_SNAPDIR;
    assert(inode_map.count(vino));
    diri = inode_map[vino];
    
    string dname = request->path.last_dentry();
    
    LeaseStat dlease;
    dlease.duration_ms = 0;

    if (in) {
      Dir *dir = diri->open_dir();
      insert_dentry_inode(dir, dname, &dlease, in, request->sent_stamp, session);
    } else {
      if (diri->dir && diri->dir->dentries.count(dname)) {
	Dentry *dn = diri->dir->dentries[dname];
	if (dn->inode)
	  unlink(dn, true, true);  // keep dir, dentry
      }
    }
  }

  if (in) {
    if (op == CEPH_MDS_OP_READDIR ||
	op == CEPH_MDS_OP_LSSNAP) {
      insert_readdir_results(request, session, in);
    } else if (op == CEPH_MDS_OP_LOOKUPNAME) {
      // hack: return parent inode instead
      in = diri;
    }

    if (request->dentry() == NULL && in != request->inode()) {
      // pin the target inode if its parent dentry is not pinned
      request->set_other_inode(in);
    }
  }

  request->target = in;
  return in;
}

// -------

mds_rank_t Client::choose_target_mds(MetaRequest *req) 
{
  mds_rank_t mds = MDS_RANK_NONE;
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
      hash = in->hash_dentry_name(req->path[0]);
      ldout(cct, 20) << "choose_target_mds inode dir hash is " << (int)in->dir_layout.dl_dir_hash
	       << " on " << req->path[0]
	       << " => " << hash << dendl;
      is_hash = true;
    }
  } else if (de) {
    if (de->inode) {
      in = de->inode.get();
      ldout(cct, 20) << "choose_target_mds starting with req->dentry inode " << *in << dendl;
    } else {
      in = de->dir->parent_inode;
      hash = in->hash_dentry_name(de->name);
      ldout(cct, 20) << "choose_target_mds dentry dir hash is " << (int)in->dir_layout.dl_dir_hash
	       << " on " << de->name
	       << " => " << hash << dendl;
      is_hash = true;
    }
  }
  if (in) {
    if (in->snapid != CEPH_NOSNAP) {
      ldout(cct, 10) << "choose_target_mds " << *in << " is snapped, using nonsnap parent" << dendl;
      while (in->snapid != CEPH_NOSNAP) {
        if (in->snapid == CEPH_SNAPDIR)
	  in = in->snapdir_parent.get();
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
  }

random_mds:
  if (mds < 0) {
    mds = _get_random_up_mds();
    ldout(cct, 10) << "did not get mds through better means, so chose random mds " << mds << dendl;
  }

out:
  ldout(cct, 20) << "mds is " << mds << dendl;
  return mds;
}


void Client::connect_mds_targets(mds_rank_t mds)
{
  ldout(cct, 10) << "connect_mds_targets for mds." << mds << dendl;
  assert(mds_sessions.count(mds));
  const MDSMap::mds_info_t& info = mdsmap->get_mds_info(mds);
  for (set<mds_rank_t>::const_iterator q = info.export_targets.begin();
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
  f->dump_int("id", get_nodeid().v);
  f->open_array_section("sessions");
  for (map<mds_rank_t,MetaSession*>::const_iterator p = mds_sessions.begin(); p != mds_sessions.end(); ++p) {
    f->open_object_section("session");
    p->second->dump(f);
    f->close_section();
  }
  f->close_section();
  f->dump_int("mdsmap_epoch", mdsmap->get_epoch());
}
void Client::dump_mds_requests(Formatter *f)
{
  for (map<ceph_tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end();
       ++p) {
    f->open_object_section("request");
    p->second->dump(f);
    f->close_section();
  }
}

int Client::verify_reply_trace(int r,
			       MetaRequest *request, MClientReply *reply,
			       InodeRef *ptarget, bool *pcreated,
			       int uid, int gid)
{
  // check whether this request actually did the create, and set created flag
  bufferlist extra_bl;
  inodeno_t created_ino;
  bool got_created_ino = false;
  ceph::unordered_map<vinodeno_t, Inode*>::iterator p;

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
    ptarget->swap(request->target);
    ldout(cct, 20) << "make_request target is " << *ptarget->get() << dendl;
  } else {
    if (got_created_ino && (p = inode_map.find(vinodeno_t(created_ino, CEPH_NOSNAP))) != inode_map.end()) {
      (*ptarget) = p->second;
      ldout(cct, 20) << "make_request created, target is " << *ptarget->get() << dendl;
    } else {
      // we got a traceless reply, and need to look up what we just
      // created.  for now, do this by name.  someday, do this by the
      // ino... which we know!  FIXME.
      InodeRef target;
      Dentry *d = request->dentry();
      if (d) {
	if (d->dir) {
	  ldout(cct, 10) << "make_request got traceless reply, looking up #"
			 << d->dir->parent_inode->ino << "/" << d->name
			 << " got_ino " << got_created_ino
			 << " ino " << created_ino
			 << dendl;
	  r = _do_lookup(d->dir->parent_inode, d->name, request->regetattr_mask, &target, uid, gid);
	} else {
	  // if the dentry is not linked, just do our best. see #5021.
	  assert(0 == "how did this happen?  i want logs!");
	}
      } else {
	Inode *in = request->inode();
	ldout(cct, 10) << "make_request got traceless reply, forcing getattr on #"
		       << in->ino << dendl;
	r = _getattr(in, request->regetattr_mask, uid, gid, true);
	target = in;
      }
      if (r >= 0) {
	// verify ino returned in reply and trace_dist are the same
	if (got_created_ino &&
	    created_ino.val != target->ino.val) {
	  ldout(cct, 5) << "create got ino " << created_ino << " but then failed on lookup; EINTR?" << dendl;
	  r = -EINTR;
	}
	if (ptarget)
	  ptarget->swap(target);
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
			 InodeRef *ptarget, bool *pcreated,
			 int use_mds,
			 bufferlist *pdirbl)
{
  int r = 0;

  // assign a unique tid
  ceph_tid_t tid = ++last_tid;
  request->set_tid(tid);

  // and timestamp
  request->op_stamp = ceph_clock_now(NULL);

  // make note
  mds_requests[tid] = request->get();
  if (oldest_tid == 0 && request->get_op() != CEPH_MDS_OP_SETFILELOCK)
    oldest_tid = tid;

  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();

  request->set_caller_uid(uid);
  request->set_caller_gid(gid);

  if (cct->_conf->client_inject_fixed_oldest_tid) {
    ldout(cct, 20) << __func__ << " injecting fixed oldest_client_tid(1)" << dendl;
    request->set_oldest_client_tid(1);
  } else {
    request->set_oldest_client_tid(oldest_tid);
  }

  // hack target mds?
  if (use_mds >= 0)
    request->resend_mds = use_mds;

  while (1) {
    if (request->aborted())
      break;

    // set up wait cond
    Cond caller_cond;
    request->caller_cond = &caller_cond;

    // choose mds
    mds_rank_t mds = choose_target_mds(request);
    if (mds == MDS_RANK_NONE || !mdsmap->is_active_or_stopping(mds)) {
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
	  request->resend_mds = _get_random_up_mds();
	  continue;
	}
      }
      
      session = _get_or_open_mds_session(mds);

      // wait
      if (session->state == MetaSession::STATE_OPENING) {
	ldout(cct, 10) << "waiting for session to mds." << mds << " to open" << dendl;
	wait_on_context_list(session->waiting_for_open);
        // Abort requests on REJECT from MDS
        if (rejected_by_mds.count(mds)) {
          request->abort(-EPERM);
          break;
        }
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

  if (!request->reply) {
    assert(request->aborted());
    assert(!request->got_unsafe);
    r = request->get_abort_code();
    request->item.remove_myself();
    unregister_request(request);
    put_request(request); // ours
    return r;
  }

  // got it!
  MClientReply *reply = request->reply;
  request->reply = NULL;
  r = reply->get_result();
  if (r >= 0)
    request->success = true;

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

void Client::unregister_request(MetaRequest *req)
{
  mds_requests.erase(req->tid);
  if (req->tid == oldest_tid) {
    map<ceph_tid_t, MetaRequest*>::iterator p = mds_requests.upper_bound(oldest_tid);
    while (true) {
      if (p == mds_requests.end()) {
	oldest_tid = 0;
	break;
      }
      if (p->second->get_op() != CEPH_MDS_OP_SETFILELOCK) {
	oldest_tid = p->first;
	break;
      }
      ++p;
    }
  }
  put_request(req);
}

void Client::put_request(MetaRequest *request)
{
  if (request->_put()) {
    int op = -1;
    if (request->success)
      op = request->get_op();
    InodeRef other_in;
    request->take_other_inode(&other_in);
    delete request;

    if (other_in &&
	(op == CEPH_MDS_OP_RMDIR ||
	 op == CEPH_MDS_OP_RENAME ||
	 op == CEPH_MDS_OP_RMSNAP)) {
      _try_to_trim_inode(other_in.get(), false);
    }
  }
}

int Client::encode_inode_release(Inode *in, MetaRequest *req,
			 mds_rank_t mds, int drop,
			 int unless, int force)
{
  ldout(cct, 20) << "encode_inode_release enter(in:" << *in << ", req:" << req
	   << " mds:" << mds << ", drop:" << drop << ", unless:" << unless
	   << ", have:" << ", force:" << force << ")" << dendl;
  int released = 0;
  if (in->caps.count(mds)) {
    Cap *caps = in->caps[mds];
    drop &= ~(in->dirty_caps | get_caps_used(in));
    if ((drop & caps->issued) &&
	!(unless & caps->issued)) {
      ldout(cct, 25) << "Dropping caps. Initial " << ccap_string(caps->issued) << dendl;
      caps->issued &= ~drop;
      caps->implemented &= ~drop;
      released = 1;
      force = 1;
      ldout(cct, 25) << "Now have: " << ccap_string(caps->issued) << dendl;
    }
    if (force) {
      ceph_mds_request_release rel;
      rel.ino = in->ino;
      rel.cap_id = caps->cap_id;
      rel.seq = caps->seq;
      rel.issue_seq = caps->issue_seq;
      rel.mseq = caps->mseq;
      rel.caps = caps->implemented;
      rel.wanted = caps->wanted;
      rel.dname_len = 0;
      rel.dname_seq = 0;
      req->cap_releases.push_back(MClientRequest::Release(rel,""));
    }
  }
  ldout(cct, 25) << "encode_inode_release exit(in:" << *in << ") released:"
	   << released << dendl;
  return released;
}

void Client::encode_dentry_release(Dentry *dn, MetaRequest *req,
			   mds_rank_t mds, int drop, int unless)
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
void Client::encode_cap_releases(MetaRequest *req, mds_rank_t mds)
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

bool Client::have_open_session(mds_rank_t mds)
{
  return
    mds_sessions.count(mds) &&
    (mds_sessions[mds]->state == MetaSession::STATE_OPEN ||
     mds_sessions[mds]->state == MetaSession::STATE_STALE);
}

MetaSession *Client::_get_mds_session(mds_rank_t mds, Connection *con)
{
  if (mds_sessions.count(mds) == 0)
    return NULL;
  MetaSession *s = mds_sessions[mds];
  if (s->con != con)
    return NULL;
  return s;
}

MetaSession *Client::_get_or_open_mds_session(mds_rank_t mds)
{
  if (mds_sessions.count(mds))
    return mds_sessions[mds];
  return _open_mds_session(mds);
}

/**
 * Populate a map of strings with client-identifying metadata,
 * such as the hostname.  Call this once at initialization.
 */
void Client::populate_metadata()
{
  // Hostname
  struct utsname u;
  int r = uname(&u);
  if (r >= 0) {
    metadata["hostname"] = u.nodename;
    ldout(cct, 20) << __func__ << " read hostname '" << u.nodename << "'" << dendl;
  } else {
    ldout(cct, 1) << __func__ << " failed to read hostname (" << cpp_strerror(r) << ")" << dendl;
  }

  // Ceph entity id (the '0' in "client.0")
  metadata["entity_id"] = cct->_conf->name.get_id();

  // Our mount position
  metadata["root"] = cct->_conf->client_mountpoint;

  // Ceph version
  metadata["ceph_version"] = pretty_version_to_str();
  metadata["ceph_sha1"] = git_version_to_str();

  // Apply any metadata from the user's configured overrides
  std::vector<std::string> tokens;
  get_str_vec(cct->_conf->client_metadata, ",", tokens);
  for (const auto &i : tokens) {
    auto eqpos = i.find("=");
    // Throw out anything that isn't of the form "<str>=<str>"
    if (eqpos == 0 || eqpos == std::string::npos || eqpos == i.size()) {
      lderr(cct) << "Invalid metadata keyval pair: '" << i << "'" << dendl;
      continue;
    }
    metadata[i.substr(0, eqpos)] = i.substr(eqpos + 1);
  }
}

/**
 * Optionally add or override client metadata fields.
 */
void Client::update_metadata(std::string const &k, std::string const &v)
{
  Mutex::Locker l(client_lock);
  assert(initialized);

  if (metadata.count(k)) {
    ldout(cct, 1) << __func__ << " warning, overriding metadata field '" << k
      << "' from '" << metadata[k] << "' to '" << v << "'" << dendl;
  }

  metadata[k] = v;
}

MetaSession *Client::_open_mds_session(mds_rank_t mds)
{
  ldout(cct, 10) << "_open_mds_session mds." << mds << dendl;
  assert(mds_sessions.count(mds) == 0);
  MetaSession *session = new MetaSession;
  session->mds_num = mds;
  session->seq = 0;
  session->inst = mdsmap->get_inst(mds);
  session->con = messenger->get_connection(session->inst);
  session->state = MetaSession::STATE_OPENING;
  session->mds_state = MDSMap::STATE_NULL;
  mds_sessions[mds] = session;

  // Maybe skip sending a request to open if this MDS daemon
  // has previously sent us a REJECT.
  if (rejected_by_mds.count(mds)) {
    if (rejected_by_mds[mds] == session->inst) {
      ldout(cct, 4) << "_open_mds_session mds." << mds << " skipping "
                       "because we were rejected" << dendl;
      return session;
    } else {
      ldout(cct, 4) << "_open_mds_session mds." << mds << " old inst "
                       "rejected us, trying with new inst" << dendl;
      rejected_by_mds.erase(mds);
    }
  }

  MClientSession *m = new MClientSession(CEPH_SESSION_REQUEST_OPEN);
  m->client_meta = metadata;
  session->con->send_message(m);
  return session;
}

void Client::_close_mds_session(MetaSession *s)
{
  ldout(cct, 2) << "_close_mds_session mds." << s->mds_num << " seq " << s->seq << dendl;
  s->state = MetaSession::STATE_CLOSING;
  s->con->send_message(new MClientSession(CEPH_SESSION_REQUEST_CLOSE, s->seq));
}

void Client::_closed_mds_session(MetaSession *s)
{
  s->state = MetaSession::STATE_CLOSED;
  s->con->mark_down();
  signal_context_list(s->waiting_for_open);
  mount_cond.Signal();
  remove_session_caps(s);
  kick_requests_closed(s);
  mds_sessions.erase(s->mds_num);
  delete s;
}

void Client::handle_client_session(MClientSession *m) 
{
  mds_rank_t from = mds_rank_t(m->get_source().num());
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
    if (unmounting)
      mount_cond.Signal();
    else
      connect_mds_targets(from);
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

  case CEPH_SESSION_FLUSHMSG:
    session->con->send_message(new MClientSession(CEPH_SESSION_FLUSHMSG_ACK, m->get_seq()));
    break;

  case CEPH_SESSION_FORCE_RO:
    force_session_readonly(session);
    break;

  case CEPH_SESSION_REJECT:
    rejected_by_mds[session->mds_num] = session->inst;
    _closed_mds_session(session);

    break;

  default:
    assert(0);
  }

  m->put();
}

void Client::_kick_stale_sessions()
{
  ldout(cct, 1) << "kick_stale_sessions" << dendl;

  for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end(); ) {
    MetaSession *s = p->second;
    ++p;
    if (s->state == MetaSession::STATE_STALE)
      _closed_mds_session(s);
  }
}

void Client::send_request(MetaRequest *request, MetaSession *session,
			  bool drop_cap_releases)
{
  // make the request
  mds_rank_t mds = session->mds_num;
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
    if (drop_cap_releases) // we haven't send cap reconnect yet, drop cap releases
      request->cap_releases.clear();
    else
      r->releases.swap(request->cap_releases);
  }
  r->set_mdsmap_epoch(mdsmap->get_epoch());
  if (r->head.op == CEPH_MDS_OP_SETXATTR) {
    objecter->with_osdmap([r](const OSDMap& o) {
	r->set_osdmap_epoch(o.get_epoch());
      });
  }

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
  session->con->send_message(r);
}

MClientRequest* Client::build_client_request(MetaRequest *request)
{
  MClientRequest *req = new MClientRequest(request->get_op());
  req->set_tid(request->tid);
  req->set_stamp(request->op_stamp);
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
  req->set_retry_attempt(request->retry_attempt++);
  req->head.num_fwd = request->num_fwd;
  return req;
}



void Client::handle_client_request_forward(MClientRequestForward *fwd)
{
  mds_rank_t mds = mds_rank_t(fwd->get_source().num());
  MetaSession *session = _get_mds_session(mds, fwd->get_connection().get());
  if (!session) {
    fwd->put();
    return;
  }
  ceph_tid_t tid = fwd->get_tid();

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

bool Client::is_dir_operation(MetaRequest *req)
{
  int op = req->get_op();
  if (op == CEPH_MDS_OP_MKNOD || op == CEPH_MDS_OP_LINK ||
      op == CEPH_MDS_OP_UNLINK || op == CEPH_MDS_OP_RENAME ||
      op == CEPH_MDS_OP_MKDIR || op == CEPH_MDS_OP_RMDIR ||
      op == CEPH_MDS_OP_SYMLINK || op == CEPH_MDS_OP_CREATE)
    return true;
  return false;
}

void Client::handle_client_reply(MClientReply *reply)
{
  mds_rank_t mds_num = mds_rank_t(reply->get_source().num());
  MetaSession *session = _get_mds_session(mds_num, reply->get_connection().get());
  if (!session) {
    reply->put();
    return;
  }

  ceph_tid_t tid = reply->get_tid();
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
      reply->put();
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
    if (is_dir_operation(request)) {
      Inode *dir = request->inode();
      assert(dir);
      dir->unsafe_ops.push_back(&request->unsafe_dir_item);
    }
    if (request->target) {
      InodeRef &in = request->target;
      in->unsafe_ops.push_back(&request->unsafe_target_item);
    }
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
      request->unsafe_dir_item.remove_myself();
      request->unsafe_target_item.remove_myself();
      signal_cond_list(request->waitfor_safe);
    }
    request->item.remove_myself();
    unregister_request(request);
  }
  if (unmounting)
    mount_cond.Signal();
}

void Client::_handle_full_flag(int64_t pool)
{
  ldout(cct, 1) << __func__ << ": FULL: cancelling outstanding operations "
    << "on " << pool << dendl;
  // Cancel all outstanding ops in this pool with -ENOSPC: it is necessary
  // to do this rather than blocking, because otherwise when we fill up we
  // potentially lock caps forever on files with dirty pages, and we need
  // to be able to release those caps to the MDS so that it can delete files
  // and free up space.
  epoch_t cancelled_epoch = objecter->op_cancel_writes(-ENOSPC, pool);

  // For all inodes with layouts in this pool and a pending flush write op
  // (i.e. one of the ones we will cancel), we've got to purge_set their data
  // from ObjectCacher so that it doesn't re-issue the write in response to
  // the ENOSPC error.
  // Fortunately since we're cancelling everything in a given pool, we don't
  // need to know which ops belong to which ObjectSet, we can just blow all
  // the un-flushed cached data away and mark any dirty inodes' async_err
  // field with -ENOSPC as long as we're sure all the ops we cancelled were
  // affecting this pool, and all the objectsets we're purging were also
  // in this pool.
  for (unordered_map<vinodeno_t,Inode*>::iterator i = inode_map.begin();
       i != inode_map.end(); ++i)
  {
    Inode *inode = i->second;
    if (inode->oset.dirty_or_tx
        && (pool == -1 || inode->layout.pool_id == pool)) {
      ldout(cct, 4) << __func__ << ": FULL: inode 0x" << std::hex << i->first << std::dec
        << " has dirty objects, purging and setting ENOSPC" << dendl;
      objectcacher->purge_set(&inode->oset);
      inode->async_err = -ENOSPC;
    }
  }

  if (cancelled_epoch != (epoch_t)-1) {
    set_cap_epoch_barrier(cancelled_epoch);
  }
}

void Client::handle_osd_map(MOSDMap *m)
{
  if (objecter->osdmap_full_flag()) {
    _handle_full_flag(-1);
  } else {
    // Accumulate local list of full pools so that I can drop
    // the objecter lock before re-entering objecter in
    // cancel_writes
    std::vector<int64_t> full_pools;

    objecter->with_osdmap([&full_pools](const OSDMap &o) {
	for (const auto& kv : o.get_pools()) {
	  if (kv.second.has_flag(pg_pool_t::FLAG_FULL)) {
	    full_pools.push_back(kv.first);
	  }
	}
      });

    for (auto p : full_pools)
      _handle_full_flag(p);

    // Subscribe to subsequent maps to watch for the full flag going
    // away.  For the global full flag objecter does this for us, but
    // it pays no attention to the per-pool full flag so in this branch
    // we do it ourselves.
    if (!full_pools.empty()) {
      objecter->maybe_request_map();
    }
  }

  m->put();
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
    // mounting and mds sessions
  case CEPH_MSG_MDS_MAP:
    handle_mds_map(static_cast<MMDSMap*>(m));
    break;
  case CEPH_MSG_FS_MAP:
    handle_fs_map(static_cast<MFSMap*>(m));
    break;
  case CEPH_MSG_FS_MAP_USER:
    handle_fs_map_user(static_cast<MFSMapUser*>(m));
    break;
  case CEPH_MSG_CLIENT_SESSION:
    handle_client_session(static_cast<MClientSession*>(m));
    break;

  case CEPH_MSG_OSD_MAP:
    handle_osd_map(static_cast<MOSDMap*>(m));
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
  case MSG_COMMAND_REPLY:
    if (m->get_source().type() == CEPH_ENTITY_TYPE_MDS) {
      handle_command_reply(static_cast<MCommandReply*>(m));
    } else {
      return false;
    }
    break;
  case CEPH_MSG_CLIENT_QUOTA:
    handle_quota(static_cast<MClientQuota*>(m));
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

void Client::handle_fs_map(MFSMap *m)
{
  delete fsmap;
  fsmap = new FSMap;
  *fsmap = m->get_fsmap();
  m->put();

  signal_cond_list(waiting_for_fsmap);

  monclient->sub_got("fsmap", fsmap->get_epoch());
}

void Client::handle_fs_map_user(MFSMapUser *m)
{
  delete fsmap_user;
  fsmap_user = new FSMapUser;
  *fsmap_user = m->get_fsmap();
  m->put();

  monclient->sub_got("fsmap.user", fsmap_user->get_epoch());
  signal_cond_list(waiting_for_fsmap);
}

void Client::handle_mds_map(MMDSMap* m)
{
  if (m->get_epoch() <= mdsmap->get_epoch()) {
    ldout(cct, 1) << "handle_mds_map epoch " << m->get_epoch()
                  << " is identical to or older than our "
                  << mdsmap->get_epoch() << dendl;
    m->put();
    return;
  }  

  ldout(cct, 1) << "handle_mds_map epoch " << m->get_epoch() << dendl;

  MDSMap *oldmap = mdsmap;
  mdsmap = new MDSMap;
  mdsmap->decode(m->get_encoded());

  // Cancel any commands for missing or laggy GIDs
  std::list<ceph_tid_t> cancel_ops;
  for (std::map<ceph_tid_t, CommandOp>::iterator i = commands.begin();
       i != commands.end(); ++i) {
    const mds_gid_t op_mds_gid = i->second.mds_gid;
    if (mdsmap->is_dne_gid(op_mds_gid) || mdsmap->is_laggy_gid(op_mds_gid)) {
      ldout(cct, 1) << __func__ << ": cancelling command op " << i->first << dendl;
      cancel_ops.push_back(i->first);
      if (i->second.outs) {
        std::ostringstream ss;
        ss << "MDS " << op_mds_gid << " went away";
        *(i->second.outs) = ss.str();
      }
      i->second.con->mark_down();
      if (i->second.on_finish) {
        i->second.on_finish->complete(-ETIMEDOUT);
      }
    }
  }

  for (std::list<ceph_tid_t>::iterator i = cancel_ops.begin();
       i != cancel_ops.end(); ++i) {
    commands.erase(*i);
  }

  // reset session
  for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    int oldstate = oldmap->get_state(p->first);
    int newstate = mdsmap->get_state(p->first);
    if (!mdsmap->is_up(p->first) ||
	mdsmap->get_inst(p->first) != p->second->inst) {
      p->second->con->mark_down();
      if (mdsmap->is_up(p->first)) {
	p->second->inst = mdsmap->get_inst(p->first);
	// When new MDS starts to take over, notify kernel to trim unused entries
	// in its dcache/icache. Hopefully, the kernel will release some unused
	// inodes before the new MDS enters reconnect state.
	trim_cache_for_reconnect(p->second);
      }
    } else if (oldstate == newstate)
      continue;  // no change
    
    MetaSession *session = p->second;
    session->mds_state = newstate;
    if (newstate == MDSMap::STATE_RECONNECT) {
      session->inst = mdsmap->get_inst(p->first);
      session->con = messenger->get_connection(session->inst);
      send_reconnect(session);
    }

    if (newstate >= MDSMap::STATE_ACTIVE) {
      if (oldstate < MDSMap::STATE_ACTIVE) {
	// kick new requests
	kick_requests(session);
	kick_flushing_caps(session);
	signal_context_list(session->waiting_for_open);
	kick_maxsize_requests(session);
	wake_inode_waiters(session);
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
  mds_rank_t mds = session->mds_num;
  ldout(cct, 10) << "send_reconnect to mds." << mds << dendl;

  // trim unused caps to reduce MDS's cache rejoin time
  trim_cache_for_reconnect(session);

  session->readonly = false;

  if (session->release) {
    session->release->put();
    session->release = NULL;
  }

  // reset my cap seq number
  session->seq = 0;
  //connect to the mds' offload targets
  connect_mds_targets(mds);
  //make sure unsafe requests get saved
  resend_unsafe_requests(session);

  MClientReconnect *m = new MClientReconnect;

  // i have an open session.
  ceph::unordered_set<inodeno_t> did_snaprealm;
  for (ceph::unordered_map<vinodeno_t, Inode*>::iterator p = inode_map.begin();
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

      bufferlist flockbl;
      _encode_filelocks(in, flockbl);

      Cap *cap = in->caps[mds];
      cap->seq = 0;  // reset seq.
      cap->issue_seq = 0;  // reset seq.
      cap->mseq = 0;  // reset seq.
      cap->issued = cap->implemented;

      snapid_t snap_follows = 0;
      if (!in->cap_snaps.empty())
	snap_follows = in->cap_snaps.begin()->first;

      m->add_cap(p->first.ino, 
		 cap->cap_id,
		 path.get_ino(), path.get_path(),   // ino
		 in->caps_wanted(), // wanted
		 cap->issued,     // issued
		 in->snaprealm->ino,
		 snap_follows,
		 flockbl);

      if (did_snaprealm.count(in->snaprealm->ino) == 0) {
	ldout(cct, 10) << " snaprealm " << *in->snaprealm << dendl;
	m->add_snaprealm(in->snaprealm->ino, in->snaprealm->seq, in->snaprealm->parent);
	did_snaprealm.insert(in->snaprealm->ino);
      }
    }
  }

  early_kick_flushing_caps(session);

  session->con->send_message(m);

  mount_cond.Signal();
}


void Client::kick_requests(MetaSession *session)
{
  ldout(cct, 10) << "kick_requests for mds." << session->mds_num << dendl;
  for (map<ceph_tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end();
       ++p) {
    if (p->second->got_unsafe)
      continue;
    if (p->second->retry_attempt > 0)
      continue; // new requests only
    if (p->second->mds == session->mds_num) {
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

  // also re-send old requests when MDS enters reconnect stage. So that MDS can
  // process completed requests in clientreplay stage.
  for (map<ceph_tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end();
       ++p) {
    MetaRequest *req = p->second;
    if (req->got_unsafe)
      continue;
    if (req->retry_attempt == 0)
      continue; // old requests only
    if (req->mds == session->mds_num)
      send_request(req, session, true);
  }
}

void Client::wait_unsafe_requests()
{
  list<MetaRequest*> last_unsafe_reqs;
  for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    MetaSession *s = p->second;
    if (!s->unsafe_requests.empty()) {
      MetaRequest *req = s->unsafe_requests.back();
      req->get();
      last_unsafe_reqs.push_back(req);
    }
  }

  for (list<MetaRequest*>::iterator p = last_unsafe_reqs.begin();
       p != last_unsafe_reqs.end();
       ++p) {
    MetaRequest *req = *p;
    if (req->unsafe_item.is_on_list())
      wait_on_list(req->waitfor_safe);
    put_request(req);
  }
}

void Client::kick_requests_closed(MetaSession *session)
{
  ldout(cct, 10) << "kick_requests_closed for mds." << session->mds_num << dendl;
  for (map<ceph_tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end(); ) {
    MetaRequest *req = p->second;
    ++p;
    if (req->mds == session->mds_num) {
      if (req->caller_cond) {
	req->kick = true;
	req->caller_cond->Signal();
      }
      req->item.remove_myself();
      if (req->got_unsafe) {
	lderr(cct) << "kick_requests_closed removing unsafe request " << req->get_tid() << dendl;
	req->unsafe_item.remove_myself();
	req->unsafe_dir_item.remove_myself();
	req->unsafe_target_item.remove_myself();
	signal_cond_list(req->waitfor_safe);
	unregister_request(req);
      }
    }
  }
  assert(session->requests.empty());
  assert(session->unsafe_requests.empty());
}




/************
 * leases
 */

void Client::got_mds_push(MetaSession *s)
{
  s->seq++;
  ldout(cct, 10) << " mds." << s->mds_num << " seq now " << s->seq << dendl;
  if (s->state == MetaSession::STATE_CLOSING) {
    s->con->send_message(new MClientSession(CEPH_SESSION_REQUEST_CLOSE, s->seq));
  }
}

void Client::handle_lease(MClientLease *m)
{
  ldout(cct, 10) << "handle_lease " << *m << dendl;

  assert(m->get_action() == CEPH_MDS_LEASE_REVOKE);

  mds_rank_t mds = mds_rank_t(m->get_source().num());
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
  m->get_connection()->send_message(
    new MClientLease(
      CEPH_MDS_LEASE_RELEASE, seq,
      m->get_mask(), m->get_ino(), m->get_first(), m->get_last(), m->dname));
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
    inode_map.erase(in->vino());
    if (use_faked_inos())
      _release_faked_ino(in);

    if (in == root) {
      root = 0;
      root_ancestor = 0;
      while (!root_parents.empty())
        root_parents.erase(root_parents.begin());
    }

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
    lru.lru_insert_mid(dn);    // mid or top?

    ldout(cct, 15) << "link dir " << dir->parent_inode << " '" << name << "' to inode " << in
		   << " dn " << dn << " (new dn)" << dendl;
  } else {
    ldout(cct, 15) << "link dir " << dir->parent_inode << " '" << name << "' to inode " << in
		   << " dn " << dn << " (old dn)" << dendl;
  }

  if (in) {    // link to inode
    dn->inode = in;
    if (in->is_dir()) {
      if (in->dir)
	dn->get(); // dir -> dn pin
      if (in->ll_ref)
	dn->get(); // ll_ref -> dn pin
    }

    assert(in->dn_set.count(dn) == 0);

    // only one parent for directories!
    if (in->is_dir() && !in->dn_set.empty()) {
      Dentry *olddn = in->get_first_parent();
      assert(olddn->dir != dir || olddn->name != name);
      Inode *old_diri = olddn->dir->parent_inode;
      old_diri->dir_release_count++;
      clear_dir_complete_and_ordered(old_diri, true);
      unlink(olddn, true, true);  // keep dir, dentry
    }

    in->dn_set.insert(dn);

    ldout(cct, 20) << "link  inode " << in << " parents now " << in->dn_set << dendl; 
  }
  
  return dn;
}

void Client::unlink(Dentry *dn, bool keepdir, bool keepdentry)
{
  InodeRef in;
  in.swap(dn->inode);
  ldout(cct, 15) << "unlink dir " << dn->dir->parent_inode << " '" << dn->name << "' dn " << dn
		 << " inode " << dn->inode << dendl;

  // unlink from inode
  if (in) {
    if (in->is_dir()) {
      if (in->dir)
	dn->put(); // dir -> dn pin
      if (in->ll_ref)
	dn->put(); // ll_ref -> dn pin
    }
    dn->inode = 0;
    assert(in->dn_set.count(dn));
    in->dn_set.erase(dn);
    ldout(cct, 20) << "unlink  inode " << in << " parents now " << in->dn_set << dendl; 
  }

  if (keepdentry) {
    dn->lease_mds = -1;
  } else {
    ldout(cct, 15) << "unlink  removing '" << dn->name << "' dn " << dn << dendl;

    // unlink from dir
    dn->dir->dentries.erase(dn->name);
    if (dn->dir->is_empty() && !keepdir)
      close_dir(dn->dir);
    dn->dir = 0;

    // delete den
    lru.lru_remove(dn);
    dn->put();
  }
}

/**
 * For asynchronous flushes, check for errors from the IO and
 * update the inode if necessary
 */
class C_Client_FlushComplete : public Context {
private:
  Client *client;
  InodeRef inode;
public:
  C_Client_FlushComplete(Client *c, Inode *in) : client(c), inode(in) { }
  void finish(int r) {
    assert(client->client_lock.is_locked_by_me());
    if (r != 0) {
      client_t const whoami = client->whoami;  // For the benefit of ldout prefix
      ldout(client->cct, 1) << "I/O error from flush on inode " << inode
        << " 0x" << std::hex << inode->ino << std::dec
        << ": " << r << "(" << cpp_strerror(r) << ")" << dendl;
      inode->async_err = r;
    }
  }
};


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
  if ((cap & CEPH_CAP_FILE_CACHE) &&
      in->cap_refs[CEPH_CAP_FILE_CACHE] == 0) {
    ldout(cct, 5) << "get_cap_ref got first FILE_CACHE ref on " << *in << dendl;
    in->get();
  }
  in->get_cap_ref(cap);
}

void Client::put_cap_ref(Inode *in, int cap)
{
  int last = in->put_cap_ref(cap);
  if (last) {
    int put_nref = 0;
    int drop = last & ~in->caps_issued();
    if (in->snapid == CEPH_NOSNAP) {
      if ((last & CEPH_CAP_FILE_WR) &&
	  !in->cap_snaps.empty() &&
	  in->cap_snaps.rbegin()->second->writing) {
	ldout(cct, 10) << "put_cap_ref finishing pending cap_snap on " << *in << dendl;
	in->cap_snaps.rbegin()->second->writing = 0;
	finish_cap_snap(in, in->cap_snaps.rbegin()->second, get_caps_used(in));
	signal_cond_list(in->waitfor_caps);  // wake up blocked sync writers
      }
      if (last & CEPH_CAP_FILE_BUFFER) {
	for (map<snapid_t,CapSnap*>::iterator p = in->cap_snaps.begin();
	    p != in->cap_snaps.end();
	    ++p)
	  p->second->dirty_data = 0;
	signal_cond_list(in->waitfor_commit);
	ldout(cct, 5) << "put_cap_ref dropped last FILE_BUFFER ref on " << *in << dendl;
	++put_nref;
      }
    }
    if (last & CEPH_CAP_FILE_CACHE) {
      ldout(cct, 5) << "put_cap_ref dropped last FILE_CACHE ref on " << *in << dendl;
      ++put_nref;
    }
    if (drop)
      check_caps(in, false);
    if (put_nref)
      put_inode(in, put_nref);
  }
}

int Client::get_caps(Inode *in, int need, int want, int *phave, loff_t endoff)
{
  int r = check_pool_perm(in, need);
  if (r < 0)
    return r;

  while (1) {
    int file_wanted = in->caps_file_wanted();
    if ((file_wanted & need) != need) {
      ldout(cct, 10) << "get_caps " << *in << " need " << ccap_string(need)
		     << " file_wanted " << ccap_string(file_wanted) << ", EBADF "
		     << dendl;
      return -EBADF;
    }

    int implemented;
    int have = in->caps_issued(&implemented);

    bool waitfor_caps = false;
    bool waitfor_commit = false;

    if (have & need & CEPH_CAP_FILE_WR) {
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
	waitfor_caps = true;
      }
      if (!in->cap_snaps.empty()) {
	if (in->cap_snaps.rbegin()->second->writing) {
	  ldout(cct, 10) << "waiting on cap_snap write to complete" << dendl;
	  waitfor_caps = true;
	}
	for (map<snapid_t,CapSnap*>::iterator p = in->cap_snaps.begin();
	    p != in->cap_snaps.end();
	    ++p)
	  if (p->second->dirty_data) {
	    waitfor_commit = true;
	    break;
	  }
	if (waitfor_commit) {
	  _flush(in, new C_Client_FlushComplete(this, in));
	  ldout(cct, 10) << "waiting for WRBUFFER to get dropped" << dendl;
	}
      }
    }

    if (!waitfor_caps && !waitfor_commit) {
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
      waitfor_caps = true;
    }

    if ((need & CEPH_CAP_FILE_WR) && in->auth_cap &&
	in->auth_cap->session->readonly)
      return -EROFS;

    if (in->flags & I_CAP_DROPPED) {
      int mds_wanted = in->caps_mds_wanted();
      if ((mds_wanted & need) != need) {
	int ret = _renew_caps(in);
	if (ret < 0)
	  return ret;
	continue;
      }
      if ((mds_wanted & file_wanted) ==
	  (file_wanted & (CEPH_CAP_FILE_RD | CEPH_CAP_FILE_WR))) {
	in->flags &= ~I_CAP_DROPPED;
      }
    }

    if (waitfor_caps)
      wait_on_list(in->waitfor_caps);
    else if (waitfor_commit)
      wait_on_list(in->waitfor_commit);
  }
}

int Client::get_caps_used(Inode *in)
{
  unsigned used = in->caps_used();
  if (!(used & CEPH_CAP_FILE_CACHE) &&
      !objectcacher->set_is_empty(&in->oset))
    used |= CEPH_CAP_FILE_CACHE;
  return used;
}

void Client::cap_delay_requeue(Inode *in)
{
  ldout(cct, 10) << "cap_delay_requeue on " << *in << dendl;
  in->hold_caps_until = ceph_clock_now(cct);
  in->hold_caps_until += cct->_conf->client_caps_release_delay;
  delayed_caps.push_back(&in->cap_item);
}

void Client::send_cap(Inode *in, MetaSession *session, Cap *cap,
		      int used, int want, int retain, int flush,
		      ceph_tid_t flush_tid)
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

  if (cct->_conf->client_inject_release_failure && revoking) {
    const int would_have_issued = cap->issued & retain;
    const int would_have_implemented = cap->implemented & (cap->issued | used);
    // Simulated bug:
    //  - tell the server we think issued is whatever they issued plus whatever we implemented
    //  - leave what we have implemented in place
    ldout(cct, 20) << __func__ << " injecting failure to release caps" << dendl;
    cap->issued = cap->issued | cap->implemented;

    // Make an exception for revoking xattr caps: we are injecting
    // failure to release other caps, but allow xattr because client
    // will block on xattr ops if it can't release these to MDS (#9800)
    const int xattr_mask = CEPH_CAP_XATTR_SHARED | CEPH_CAP_XATTR_EXCL;
    cap->issued ^= xattr_mask & revoking;
    cap->implemented ^= xattr_mask & revoking;

    ldout(cct, 20) << __func__ << " issued " << ccap_string(cap->issued) << " vs " << ccap_string(would_have_issued) << dendl;
    ldout(cct, 20) << __func__ << " implemented " << ccap_string(cap->implemented) << " vs " << ccap_string(would_have_implemented) << dendl;
  } else {
    // Normal behaviour
    cap->issued &= retain;
    cap->implemented &= cap->issued | used;
  }

  snapid_t follows = 0;

  if (flush)
    follows = in->snaprealm->get_snap_context().seq;
  
  MClientCaps *m = new MClientCaps(op,
				   in->ino,
				   0,
				   cap->cap_id, cap->seq,
				   cap->implemented,
				   want,
				   flush,
				   cap->mseq,
                                   cap_epoch_barrier);
  m->caller_uid = in->cap_dirtier_uid;
  m->caller_gid = in->cap_dirtier_gid;

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
  
  m->layout = in->layout;
  m->size = in->size;
  m->max_size = in->max_size;
  m->truncate_seq = in->truncate_seq;
  m->truncate_size = in->truncate_size;
  m->mtime = in->mtime;
  m->atime = in->atime;
  m->ctime = in->ctime;
  m->btime = in->btime;
  m->time_warp_seq = in->time_warp_seq;
  m->change_attr = in->change_attr;
    
  if (flush & CEPH_CAP_FILE_WR) {
    m->inline_version = in->inline_version;
    m->inline_data = in->inline_data;
  }

  in->reported_size = in->size;
  m->set_snap_follows(follows);
  cap->wanted = want;
  if (cap == in->auth_cap) {
    m->set_max_size(in->wanted_max_size);
    in->requested_max_size = in->wanted_max_size;
    ldout(cct, 15) << "auth cap, setting max_size = " << in->requested_max_size << dendl;
  }

  if (!session->flushing_caps_tids.empty())
    m->set_oldest_flush_tid(*session->flushing_caps_tids.begin());

  session->con->send_message(m);
}


void Client::check_caps(Inode *in, bool is_delayed)
{
  unsigned wanted = in->caps_wanted();
  unsigned used = get_caps_used(in);
  unsigned cap_used;

  if (in->is_dir() && (in->flags & I_COMPLETE)) {
    // we do this here because we don't want to drop to Fs (and then
    // drop the Fs if we do a create!) if that alone makes us send lookups
    // to the MDS. Doing it in in->caps_wanted() has knock-on effects elsewhere
    wanted |= CEPH_CAP_FILE_EXCL;
  }

  int implemented;
  int issued = in->caps_issued(&implemented);
  int revoking = implemented & ~issued;

  int retain = wanted | used | CEPH_CAP_PIN;
  if (!unmounting) {
    if (wanted)
      retain |= CEPH_CAP_ANY;
    else
      retain |= CEPH_CAP_ANY_SHARED;
  }

  ldout(cct, 10) << "check_caps on " << *in
	   << " wanted " << ccap_string(wanted)
	   << " used " << ccap_string(used)
	   << " issued " << ccap_string(issued)
	   << " revoking " << ccap_string(revoking)
	   << " is_delayed=" << is_delayed
	   << dendl;

  if (in->snapid != CEPH_NOSNAP)
    return; //snap caps last forever, can't write

  if (in->caps.empty())
    return;   // guard if at end of func

  if ((revoking & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO)) &&
      (used & CEPH_CAP_FILE_CACHE) && !(used & CEPH_CAP_FILE_BUFFER))
    _release(in);

  if (!in->cap_snaps.empty())
    flush_snaps(in);

  if (!is_delayed)
    cap_delay_requeue(in);
  else
    in->hold_caps_until = utime_t();

  utime_t now = ceph_clock_now(cct);

  map<mds_rank_t, Cap*>::iterator it = in->caps.begin();
  while (it != in->caps.end()) {
    mds_rank_t mds = it->first;
    Cap *cap = it->second;
    ++it;

    MetaSession *session = mds_sessions[mds];
    assert(session);

    cap_used = used;
    if (in->auth_cap && cap != in->auth_cap)
      cap_used &= ~in->auth_cap->issued;

    revoking = cap->implemented & ~cap->issued;
    
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
    if (revoking && (revoking & cap_used) == 0) {
      ldout(cct, 10) << "completed revocation of " << ccap_string(cap->implemented & ~cap->issued) << dendl;
      goto ack;
    }

    /* want more caps from mds? */
    if (wanted & ~(cap->wanted | cap->issued))
      goto ack;

    if (!revoking && unmounting && (cap_used == 0))
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
    // re-send old cap/snapcap flushes first.
    if (session->mds_state >= MDSMap::STATE_RECONNECT &&
	session->mds_state < MDSMap::STATE_ACTIVE &&
	session->early_flushing_caps.count(in) == 0) {
      ldout(cct, 20) << " reflushing caps (check_caps) on " << *in
		     << " to mds." << session->mds_num << dendl;
      session->early_flushing_caps.insert(in);
      if (in->cap_snaps.size())
	flush_snaps(in, true);
      if (in->flushing_caps)
	flush_caps(in, session);
    }

    int flushing;
    ceph_tid_t flush_tid;
    if (in->auth_cap == cap && in->dirty_caps) {
      flushing = mark_caps_flushing(in, &flush_tid);
    } else {
      flushing = 0;
      flush_tid = 0;
    }

    send_cap(in, session, cap, cap_used, wanted, retain, flushing, flush_tid);
  }
}


void Client::queue_cap_snap(Inode *in, SnapContext& old_snapc)
{
  int used = get_caps_used(in);
  int dirty = in->caps_dirty();
  ldout(cct, 10) << "queue_cap_snap " << *in << " snapc " << old_snapc << " used " << ccap_string(used) << dendl;

  if (in->cap_snaps.size() &&
      in->cap_snaps.rbegin()->second->writing) {
    ldout(cct, 10) << "queue_cap_snap already have pending cap_snap on " << *in << dendl;
    return;
  } else if (in->caps_dirty() ||
            (used & CEPH_CAP_FILE_WR) ||
	     (dirty & CEPH_CAP_ANY_WR)) {
    CapSnap *capsnap = new CapSnap(in);
    in->cap_snaps[old_snapc.seq] = capsnap;
    capsnap->context = old_snapc;
    capsnap->issued = in->caps_issued();
    capsnap->dirty = in->caps_dirty();
    
    capsnap->dirty_data = (used & CEPH_CAP_FILE_BUFFER);
    
    capsnap->uid = in->uid;
    capsnap->gid = in->gid;
    capsnap->mode = in->mode;
    capsnap->btime = in->btime;
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
  capsnap->change_attr = in->change_attr;

  capsnap->dirty |= in->caps_dirty();

  if (capsnap->dirty & CEPH_CAP_FILE_WR) {
    capsnap->inline_data = in->inline_data;
    capsnap->inline_version = in->inline_version;
  }

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

void Client::flush_snaps(Inode *in, bool all_again)
{
  ldout(cct, 10) << "flush_snaps on " << *in << " all_again " << all_again << dendl;
  assert(in->cap_snaps.size());

  // pick auth mds
  assert(in->auth_cap);
  MetaSession *session = in->auth_cap->session;
  int mseq = in->auth_cap->mseq;

  for (map<snapid_t,CapSnap*>::iterator p = in->cap_snaps.begin(); p != in->cap_snaps.end(); ++p) {
    CapSnap *capsnap = p->second;
    if (!all_again) {
      // only flush once per session
      if (capsnap->flush_tid > 0)
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
    
    if (capsnap->flush_tid == 0) {
      capsnap->flush_tid = ++last_flush_tid;
      if (!in->flushing_cap_item.is_on_list())
	session->flushing_caps.push_back(&in->flushing_cap_item);
      session->flushing_caps_tids.insert(capsnap->flush_tid);
    }

    MClientCaps *m = new MClientCaps(CEPH_CAP_OP_FLUSHSNAP, in->ino, in->snaprealm->ino, 0, mseq,
				     cap_epoch_barrier);
    if (user_id >= 0)
      m->caller_uid = user_id;
    if (group_id >= 0)
      m->caller_gid = group_id;

    m->set_client_tid(capsnap->flush_tid);
    m->head.snap_follows = p->first;

    m->head.caps = capsnap->issued;
    m->head.dirty = capsnap->dirty;

    m->head.uid = capsnap->uid;
    m->head.gid = capsnap->gid;
    m->head.mode = capsnap->mode;
    m->btime = capsnap->btime;

    m->size = capsnap->size;

    m->head.xattr_version = capsnap->xattr_version;
    ::encode(capsnap->xattrs, m->xattrbl);

    m->ctime = capsnap->ctime;
    m->btime = capsnap->btime;
    m->mtime = capsnap->mtime;
    m->atime = capsnap->atime;
    m->time_warp_seq = capsnap->time_warp_seq;
    m->change_attr = capsnap->change_attr;

    if (capsnap->dirty & CEPH_CAP_FILE_WR) {
      m->inline_version = in->inline_version;
      m->inline_data = in->inline_data;
    }

    assert(!session->flushing_caps_tids.empty());
    m->set_oldest_flush_tid(*session->flushing_caps_tids.begin());

    session->con->send_message(m);
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
  vinodeno_t ino;
  int64_t offset, length;
public:
  C_Client_CacheInvalidate(Client *c, Inode *in, int64_t off, int64_t len) :
    client(c), offset(off), length(len) {
    if (client->use_faked_inos())
      ino = vinodeno_t(in->faked_ino, CEPH_NOSNAP);
    else
      ino = in->vino();
  }
  void finish(int r) {
    // _async_invalidate takes the lock when it needs to, call this back from outside of lock.
    assert(!client->client_lock.is_locked_by_me());
    client->_async_invalidate(ino, offset, length);
  }
};

void Client::_async_invalidate(vinodeno_t ino, int64_t off, int64_t len)
{
  if (unmounting)
    return;
  ldout(cct, 10) << "_async_invalidate " << ino << " " << off << "~" << len << dendl;
  ino_invalidate_cb(callback_handle, ino, off, len);
}

void Client::_schedule_invalidate_callback(Inode *in, int64_t off, int64_t len) {

  if (ino_invalidate_cb)
    // we queue the invalidate, which calls the callback and decrements the ref
    async_ino_invalidator.queue(new C_Client_CacheInvalidate(this, in, off, len));
}

void Client::_invalidate_inode_cache(Inode *in)
{
  ldout(cct, 10) << "_invalidate_inode_cache " << *in << dendl;

  // invalidate our userspace inode cache
  if (cct->_conf->client_oc)
    objectcacher->release_set(&in->oset);

  _schedule_invalidate_callback(in, 0, 0);
}

void Client::_invalidate_inode_cache(Inode *in, int64_t off, int64_t len)
{
  ldout(cct, 10) << "_invalidate_inode_cache " << *in << " " << off << "~" << len << dendl;

  // invalidate our userspace inode cache
  if (cct->_conf->client_oc) {
    vector<ObjectExtent> ls;
    Striper::file_to_extents(cct, in->ino, &in->layout, off, len, in->truncate_size, ls);
    objectcacher->discard_set(&in->oset, ls);
  }

  _schedule_invalidate_callback(in, off, len);
}

bool Client::_release(Inode *in)
{
  ldout(cct, 20) << "_release " << *in << dendl;
  if (in->cap_refs[CEPH_CAP_FILE_CACHE] == 0) {
    _invalidate_inode_cache(in);
    return true;
  }
  return false;
}

bool Client::_flush(Inode *in, Context *onfinish)
{
  ldout(cct, 10) << "_flush " << *in << dendl;

  if (!in->oset.dirty_or_tx) {
    ldout(cct, 10) << " nothing to flush" << dendl;
    onfinish->complete(0);
    return true;
  }

  if (objecter->osdmap_pool_full(in->layout.pool_id)) {
    ldout(cct, 1) << __func__ << ": FULL, purging for ENOSPC" << dendl;
    objectcacher->purge_set(&in->oset);
    if (onfinish) {
      onfinish->complete(-ENOSPC);
    }
    return true;
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

  put_cap_ref(in, CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER);
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

    if (in->is_dir())
      clear_dir_complete_and_ordered(in, true);
  }
}

void Client::add_update_cap(Inode *in, MetaSession *mds_session, uint64_t cap_id,
			    unsigned issued, unsigned seq, unsigned mseq, inodeno_t realm,
			    int flags)
{
  Cap *cap = 0;
  mds_rank_t mds = mds_session->mds_num;
  if (in->caps.count(mds)) {
    cap = in->caps[mds];

    /*
     * auth mds of the inode changed. we received the cap export
     * message, but still haven't received the cap import message.
     * handle_cap_export() updated the new auth MDS' cap.
     *
     * "ceph_seq_cmp(seq, cap->seq) <= 0" means we are processing
     * a message that was send before the cap import message. So
     * don't remove caps.
     */
    if (ceph_seq_cmp(seq, cap->seq) <= 0) {
      assert(cap == in->auth_cap);
      assert(cap->cap_id == cap_id);
      seq = cap->seq;
      mseq = cap->mseq;
      issued |= cap->issued;
      flags |= CEPH_CAP_FLAG_AUTH;
    }
  } else {
    mds_session->num_caps++;
    if (!in->is_any_caps()) {
      assert(in->snaprealm == 0);
      in->snaprealm = get_snap_realm(realm);
      in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
      ldout(cct, 15) << "add_update_cap first one, opened snaprealm " << in->snaprealm << dendl;
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
        (!in->auth_cap || ceph_seq_cmp(in->auth_cap->mseq, mseq) < 0)) {
      if (in->auth_cap && in->flushing_cap_item.is_on_list()) {
	ldout(cct, 10) << "add_update_cap changing auth cap: "
		       << "add myself to new auth MDS' flushing caps list" << dendl;
	adjust_session_flushing_caps(in, in->auth_cap->session, mds_session);
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

  if ((issued & ~old_caps) && in->auth_cap == cap) {
    // non-auth MDS is revoking the newly grant caps ?
    for (map<mds_rank_t,Cap*>::iterator it = in->caps.begin(); it != in->caps.end(); ++it) {
      if (it->second == cap)
	continue;
      if (it->second->implemented & ~it->second->issued & issued) {
	check_caps(in, true);
	break;
      }
    }
  }

  if (issued & ~old_caps)
    signal_cond_list(in->waitfor_caps);
}

void Client::remove_cap(Cap *cap, bool queue_release)
{
  Inode *in = cap->inode;
  MetaSession *session = cap->session;
  mds_rank_t mds = cap->session->mds_num;

  ldout(cct, 10) << "remove_cap mds." << mds << " on " << *in << dendl;
  
  if (queue_release) {
    session->enqueue_cap_release(
      in->ino,
      cap->cap_id,
      cap->issue_seq,
      cap->mseq,
      cap_epoch_barrier);
  }

  if (in->auth_cap == cap) {
    if (in->flushing_cap_item.is_on_list()) {
      ldout(cct, 10) << " removing myself from flushing_cap list" << dendl;
      in->flushing_cap_item.remove_myself();
    }
    in->auth_cap = NULL;
  }
  assert(in->caps.count(mds));
  in->caps.erase(mds);

  if (cap == session->s_cap_iterator) {
    cap->inode = NULL;
  } else {
    cap->cap_item.remove_myself();
    delete cap;
  }

  if (!in->is_any_caps()) {
    ldout(cct, 15) << "remove_cap last one, closing snaprealm " << in->snaprealm << dendl;
    in->snaprealm_item.remove_myself();
    put_snap_realm(in->snaprealm);
    in->snaprealm = 0;
  }
}

void Client::remove_all_caps(Inode *in)
{
  while (!in->caps.empty())
    remove_cap(in->caps.begin()->second, true);
}

void Client::remove_session_caps(MetaSession *s)
{
  ldout(cct, 10) << "remove_session_caps mds." << s->mds_num << dendl;

  while (s->caps.size()) {
    Cap *cap = *s->caps.begin();
    Inode *in = cap->inode;
    int dirty_caps = 0;
    if (in->auth_cap == cap) {
      dirty_caps = in->dirty_caps | in->flushing_caps;
      in->wanted_max_size = 0;
      in->requested_max_size = 0;
      in->flags |= I_CAP_DROPPED;
    }
    remove_cap(cap, false);
    signal_cond_list(in->waitfor_caps);
    if (dirty_caps) {
      lderr(cct) << "remove_session_caps still has dirty|flushing caps on " << *in << dendl;
      if (in->flushing_caps) {
	num_flushing_caps--;
	in->flushing_cap_tids.clear();
      }
      in->flushing_caps = 0;
      in->dirty_caps = 0;
      put_inode(in);
    }
  }
  s->flushing_caps_tids.clear();
  sync_cond.Signal();
}

class C_Client_Remount : public Context  {
private:
  Client *client;
public:
  explicit C_Client_Remount(Client *c) : client(c) {}
  void finish(int r) {
    assert (r == 0);
    r = client->remount_cb(client->callback_handle);
    if (r != 0) {
      client_t whoami = client->get_nodeid();
      lderr(client->cct) << "tried to remount (to trim kernel dentries) and got error "
			 << r << dendl;
      if (client->require_remount && !client->unmounting) {
	assert(0 == "failed to remount for kernel dentry trimming");
      }
    }
  }
};

void Client::_invalidate_kernel_dcache()
{
  if (can_invalidate_dentries && dentry_invalidate_cb && root->dir) {
    for (ceph::unordered_map<string, Dentry*>::iterator p = root->dir->dentries.begin();
	 p != root->dir->dentries.end();
	 ++p) {
      if (p->second->inode)
	_schedule_invalidate_dentry_callback(p->second, false);
    }
  } else if (remount_cb) {
    // Hacky:
    // when remounting a file system, linux kernel trims all unused dentries in the fs
    remount_finisher.queue(new C_Client_Remount(this));
  }
}

void Client::trim_caps(MetaSession *s, int max)
{
  mds_rank_t mds = s->mds_num;
  int caps_size = s->caps.size();
  ldout(cct, 10) << "trim_caps mds." << mds << " max " << max 
    << " caps " << caps_size << dendl;

  int trimmed = 0;
  xlist<Cap*>::iterator p = s->caps.begin();
  while ((caps_size - trimmed) > max && !p.end()) {
    Cap *cap = *p;
    s->s_cap_iterator = cap;
    Inode *in = cap->inode;

    if (in->caps.size() > 1 && cap != in->auth_cap) {
      int mine = cap->issued | cap->implemented;
      int oissued = in->auth_cap ? in->auth_cap->issued : 0;
      // disposable non-auth cap
      if (!(get_caps_used(in) & ~oissued & mine)) {
	ldout(cct, 20) << " removing unused, unneeded non-auth cap on " << *in << dendl;
	remove_cap(cap, true);
	trimmed++;
      }
    } else {
      ldout(cct, 20) << " trying to trim dentries for " << *in << dendl;
      bool all = true;
      set<Dentry*>::iterator q = in->dn_set.begin();
      InodeRef tmp_ref(in);
      while (q != in->dn_set.end()) {
	Dentry *dn = *q++;
	if (dn->lru_is_expireable()) {
	  if (can_invalidate_dentries &&
	      dn->dir->parent_inode->ino == MDS_INO_ROOT) {
	    // Only issue one of these per DN for inodes in root: handle
	    // others more efficiently by calling for root-child DNs at
	    // the end of this function.
	    _schedule_invalidate_dentry_callback(dn, true);
	  }
	  trim_dentry(dn);
        } else {
          ldout(cct, 20) << "  not expirable: " << dn->name << dendl;
	  all = false;
        }
      }
      if (all && in->ino != MDS_INO_ROOT) {
        ldout(cct, 20) << __func__ << " counting as trimmed: " << *in << dendl;
	trimmed++;
      }
    }

    ++p;
    if (!cap->inode) {
      cap->cap_item.remove_myself();
      delete cap;
    }
  }
  s->s_cap_iterator = NULL;

  if (s->caps.size() > max)
    _invalidate_kernel_dcache();
}

void Client::force_session_readonly(MetaSession *s)
{
  s->readonly = true;
  for (xlist<Cap*>::iterator p = s->caps.begin(); !p.end(); ++p) {
    Inode *in = (*p)->inode;
    if (in->caps_wanted() & CEPH_CAP_FILE_WR)
      signal_cond_list(in->waitfor_caps);
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

int Client::mark_caps_flushing(Inode *in, ceph_tid_t* ptid)
{
  MetaSession *session = in->auth_cap->session;

  int flushing = in->dirty_caps;
  assert(flushing);

  ceph_tid_t flush_tid = ++last_flush_tid;
  in->flushing_cap_tids[flush_tid] = flushing;

  if (!in->flushing_caps) {
    ldout(cct, 10) << "mark_caps_flushing " << ccap_string(flushing) << " " << *in << dendl;
    num_flushing_caps++;
  } else {
    ldout(cct, 10) << "mark_caps_flushing (more) " << ccap_string(flushing) << " " << *in << dendl;
  }

  in->flushing_caps |= flushing;
  in->dirty_caps = 0;
 
  if (!in->flushing_cap_item.is_on_list())
    session->flushing_caps.push_back(&in->flushing_cap_item);
  session->flushing_caps_tids.insert(flush_tid);

  *ptid = flush_tid;
  return flushing;
}

void Client::adjust_session_flushing_caps(Inode *in, MetaSession *old_s,  MetaSession *new_s)
{
  for (auto p = in->cap_snaps.begin(); p != in->cap_snaps.end(); ++p) {
    CapSnap *capsnap = p->second;
    if (capsnap->flush_tid > 0) {
      old_s->flushing_caps_tids.erase(capsnap->flush_tid);
      new_s->flushing_caps_tids.insert(capsnap->flush_tid);
    }
  }
  for (map<ceph_tid_t, int>::iterator it = in->flushing_cap_tids.begin();
       it != in->flushing_cap_tids.end();
       ++it) {
    old_s->flushing_caps_tids.erase(it->first);
    new_s->flushing_caps_tids.insert(it->first);
  }
  new_s->flushing_caps.push_back(&in->flushing_cap_item);
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

  for (map<ceph_tid_t,int>::iterator p = in->flushing_cap_tids.begin();
       p != in->flushing_cap_tids.end();
       ++p) {
    send_cap(in, session, cap, (get_caps_used(in) | in->caps_dirty()),
	     in->caps_wanted(), (cap->issued | cap->implemented),
	     p->second, p->first);
  }
}

void Client::wait_sync_caps(Inode *in, ceph_tid_t want)
{
  while (in->flushing_caps) {
    map<ceph_tid_t, int>::iterator it = in->flushing_cap_tids.begin();
    assert(it != in->flushing_cap_tids.end());
    if (it->first > want)
      break;
    ldout(cct, 10) << "wait_sync_caps on " << *in << " flushing "
		   << ccap_string(it->second) << " want " << want
		   << " last " << it->first << dendl;
    wait_on_list(in->waitfor_caps);
  }
}

void Client::wait_sync_caps(ceph_tid_t want)
{
 retry:
  ldout(cct, 10) << "wait_sync_caps want " << want  << " (last is " << last_flush_tid << ", "
	   << num_flushing_caps << " total flushing)" << dendl;
  for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    MetaSession *s = p->second;
    if (s->flushing_caps_tids.empty())
	continue;
    ceph_tid_t oldest_tid = *s->flushing_caps_tids.begin();
    if (oldest_tid <= want) {
      ldout(cct, 10) << " waiting on mds." << p->first << " tid " << oldest_tid
		     << " (want " << want << ")" << dendl;
      sync_cond.Wait(client_lock);
      goto retry;
    }
  }
}

void Client::kick_flushing_caps(MetaSession *session)
{
  mds_rank_t mds = session->mds_num;
  ldout(cct, 10) << "kick_flushing_caps mds." << mds << dendl;

  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
    if (session->early_flushing_caps.count(in))
      continue;
    ldout(cct, 20) << " reflushing caps on " << *in << " to mds." << mds << dendl;
    if (in->cap_snaps.size())
      flush_snaps(in, true);
    if (in->flushing_caps)
      flush_caps(in, session);
  }

  session->early_flushing_caps.clear();
}

void Client::early_kick_flushing_caps(MetaSession *session)
{
  session->early_flushing_caps.clear();

  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
    assert(in->auth_cap);

    // if flushing caps were revoked, we re-send the cap flush in client reconnect
    // stage. This guarantees that MDS processes the cap flush message before issuing
    // the flushing caps to other client.
    if ((in->flushing_caps & in->auth_cap->issued) == in->flushing_caps)
      continue;

    ldout(cct, 20) << " reflushing caps (early_kick) on " << *in
		   << " to mds." << session->mds_num << dendl;

    session->early_flushing_caps.insert(in);

    if (in->cap_snaps.size())
      flush_snaps(in, true);
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
    if (realm->pparent) {
      realm->pparent->pchildren.erase(realm);
      put_snap_realm(realm->pparent);
    }
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

static bool has_new_snaps(const SnapContext& old_snapc,
			  const SnapContext& new_snapc)
{
  return !new_snapc.snaps.empty() && new_snapc.snaps[0] > old_snapc.seq;
}


inodeno_t Client::update_snap_trace(bufferlist& bl, bool flush)
{
  inodeno_t first_realm = 0;
  ldout(cct, 10) << "update_snap_trace len " << bl.length() << dendl;

  map<SnapRealm*, SnapContext> dirty_realms;

  bufferlist::iterator p = bl.begin();
  while (!p.end()) {
    SnapRealmInfo info;
    ::decode(info, p);
    if (first_realm == 0)
      first_realm = info.ino();
    SnapRealm *realm = get_snap_realm(info.ino());

    bool invalidate = false;

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

	  for (set<SnapRealm*>::iterator p = realm->pchildren.begin(); 
	       p != realm->pchildren.end();
	       ++p)
	    q.push_back(*p);

	  if (dirty_realms.count(realm) == 0) {
	    realm->nref++;
	    dirty_realms[realm] = realm->get_snap_context();
	  }
	}
      }

      // update
      realm->seq = info.seq();
      realm->created = info.created();
      realm->parent_since = info.parent_since();
      realm->prior_parent_snaps = info.prior_parent_snaps;
      realm->my_snaps = info.my_snaps;
      invalidate = true;
    }

    // _always_ verify parent
    if (adjust_realm_parent(realm, info.parent()))
      invalidate = true;

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

  for (map<SnapRealm*, SnapContext>::iterator q = dirty_realms.begin();
       q != dirty_realms.end();
       ++q) {
    SnapRealm *realm = q->first;
    // if there are new snaps ?
    if (has_new_snaps(q->second, realm->get_snap_context())) { 
      ldout(cct, 10) << " flushing caps on " << *realm << dendl;
      xlist<Inode*>::iterator r = realm->inodes_with_caps.begin();
      while (!r.end()) {
	Inode *in = *r;
	++r;
	queue_cap_snap(in, q->second);
      }
    } else {
      ldout(cct, 10) << " no new snap on " << *realm << dendl;
    }
    put_snap_realm(realm);
  }

  return first_realm;
}

void Client::handle_snap(MClientSnap *m)
{
  ldout(cct, 10) << "handle_snap " << *m << dendl;
  mds_rank_t mds = mds_rank_t(m->get_source().num());
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
    m->put();
    return;
  }

  got_mds_push(session);

  map<Inode*, SnapContext> to_move;
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


	in->snaprealm_item.remove_myself();
	to_move[in] = in->snaprealm->get_snap_context();
	put_snap_realm(in->snaprealm);
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
    for (auto p = to_move.begin(); p != to_move.end(); ++p) {
      Inode *in = p->first;
      in->snaprealm = realm;
      realm->inodes_with_caps.push_back(&in->snaprealm_item);
      realm->nref++;
      // queue for snap writeback
      if (has_new_snaps(p->second, realm->get_snap_context()))
	queue_cap_snap(in, p->second);
    }
    put_snap_realm(realm);
  }

  m->put();
}

void Client::handle_quota(MClientQuota *m)
{
  mds_rank_t mds = mds_rank_t(m->get_source().num());
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
    m->put();
    return;
  }

  got_mds_push(session);

  ldout(cct, 10) << "handle_quota " << *m << " from mds." << mds << dendl;

  vinodeno_t vino(m->ino, CEPH_NOSNAP);
  if (inode_map.count(vino)) {
    Inode *in = NULL;
    in = inode_map[vino];

    if (in) {
      in->quota = m->quota;
      in->rstat = m->rstat;
    }
  }

  m->put();
}

void Client::handle_caps(MClientCaps *m)
{
  mds_rank_t mds = mds_rank_t(m->get_source().num());
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
    m->put();
    return;
  }

  if (m->osd_epoch_barrier && !objecter->have_map(m->osd_epoch_barrier)) {
    // Pause RADOS operations until we see the required epoch
    objecter->set_epoch_barrier(m->osd_epoch_barrier);
  }

  if (m->osd_epoch_barrier > cap_epoch_barrier) {
    // Record the barrier so that we will transmit it to MDS when releasing
    set_cap_epoch_barrier(m->osd_epoch_barrier);
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
      session->enqueue_cap_release(
        m->get_ino(),
        m->get_cap_id(),
        m->get_seq(),
        m->get_mseq(),
        cap_epoch_barrier);
    } else {
      ldout(cct, 5) << "handle_caps don't have vino " << vino << ", dropping" << dendl;
    }
    m->put();

    // in case the mds is waiting on e.g. a revocation
    flush_cap_releases();
    return;
  }

  switch (m->get_op()) {
  case CEPH_CAP_OP_EXPORT:
    return handle_cap_export(session, in, m);
  case CEPH_CAP_OP_FLUSHSNAP_ACK:
    return handle_cap_flushsnap_ack(session, in, m);
  case CEPH_CAP_OP_IMPORT:
    handle_cap_import(session, in, m);
  }

  if (in->caps.count(mds) == 0) {
    ldout(cct, 5) << "handle_caps don't have " << *in << " cap on mds." << mds << dendl;
    m->put();
    return;
  }

  Cap *cap = in->caps[mds];

  switch (m->get_op()) {
  case CEPH_CAP_OP_TRUNC: return handle_cap_trunc(session, in, m);
  case CEPH_CAP_OP_IMPORT:
  case CEPH_CAP_OP_REVOKE:
  case CEPH_CAP_OP_GRANT: return handle_cap_grant(session, in, cap, m);
  case CEPH_CAP_OP_FLUSH_ACK: return handle_cap_flush_ack(session, in, cap, m);
  default:
    m->put();
  }
}

void Client::handle_cap_import(MetaSession *session, Inode *in, MClientCaps *m)
{
  mds_rank_t mds = session->mds_num;

  ldout(cct, 5) << "handle_cap_import ino " << m->get_ino() << " mseq " << m->get_mseq()
		<< " IMPORT from mds." << mds << dendl;

  // add/update it
  update_snap_trace(m->snapbl);
  add_update_cap(in, session, m->get_cap_id(),
		 m->get_caps(), m->get_seq(), m->get_mseq(), m->get_realm(),
		 CEPH_CAP_FLAG_AUTH);

  const mds_rank_t peer_mds = mds_rank_t(m->peer.mds);

  if (m->peer.cap_id && in->caps.count(peer_mds)) {
    Cap *cap = in->caps[peer_mds];
    if (cap && cap->cap_id == m->peer.cap_id)
      remove_cap(cap, (m->peer.flags & CEPH_CAP_FLAG_RELEASE));
  }
  
  if (in->auth_cap && in->auth_cap->session->mds_num == mds) {
    // reflush any/all caps (if we are now the auth_cap)
    if (in->cap_snaps.size())
      flush_snaps(in, true);
    if (in->flushing_caps)
      flush_caps(in, session);
  }
}

void Client::handle_cap_export(MetaSession *session, Inode *in, MClientCaps *m)
{
  mds_rank_t mds = session->mds_num;

  ldout(cct, 5) << "handle_cap_export ino " << m->get_ino() << " mseq " << m->get_mseq()
		<< " EXPORT from mds." << mds << dendl;

  Cap *cap = NULL;
  if (in->caps.count(mds))
    cap = in->caps[mds];

  const mds_rank_t peer_mds = mds_rank_t(m->peer.mds);

  if (cap && cap->cap_id == m->get_cap_id()) {
    if (m->peer.cap_id) {
      MetaSession *tsession = _get_or_open_mds_session(peer_mds);
      if (in->caps.count(peer_mds)) {
	Cap *tcap = in->caps[peer_mds];
	if (tcap->cap_id != m->peer.cap_id ||
	    ceph_seq_cmp(tcap->seq, m->peer.seq) < 0) {
	  tcap->cap_id = m->peer.cap_id;
	  tcap->seq = m->peer.seq - 1;
	  tcap->issue_seq = tcap->seq;
	  tcap->mseq = m->peer.mseq;
	  tcap->issued |= cap->issued;
	  tcap->implemented |= cap->issued;
	  if (cap == in->auth_cap)
	    in->auth_cap = tcap;
	  if (in->auth_cap == tcap && in->flushing_cap_item.is_on_list())
	    adjust_session_flushing_caps(in, session, tsession);
	}
      } else {
	add_update_cap(in, tsession, m->peer.cap_id, cap->issued,
		       m->peer.seq - 1, m->peer.mseq, (uint64_t)-1,
		       cap == in->auth_cap ? CEPH_CAP_FLAG_AUTH : 0);
      }
    } else {
      if (cap == in->auth_cap)
	in->flags |= I_CAP_DROPPED;
    }

    remove_cap(cap, false);
  }

  m->put();
}

void Client::handle_cap_trunc(MetaSession *session, Inode *in, MClientCaps *m)
{
  mds_rank_t mds = session->mds_num;
  assert(in->caps[mds]);

  ldout(cct, 10) << "handle_cap_trunc on ino " << *in
	   << " size " << in->size << " -> " << m->get_size()
	   << dendl;
  
  int implemented = 0;
  int issued = in->caps_issued(&implemented) | in->caps_dirty();
  issued |= implemented;
  update_inode_file_bits(in, m->get_truncate_seq(), m->get_truncate_size(),
			 m->get_size(), m->get_change_attr(), m->get_time_warp_seq(),
			 m->get_ctime(), m->get_mtime(), m->get_atime(),
                         m->inline_version, m->inline_data, issued);
  m->put();
}

void Client::handle_cap_flush_ack(MetaSession *session, Inode *in, Cap *cap, MClientCaps *m)
{
  ceph_tid_t flush_ack_tid = m->get_client_tid();
  int dirty = m->get_dirty();
  int cleaned = 0;
  int flushed = 0;

  for (map<ceph_tid_t, int>::iterator it = in->flushing_cap_tids.begin();
       it != in->flushing_cap_tids.end(); ) {
    if (it->first == flush_ack_tid)
      cleaned = it->second;
    if (it->first <= flush_ack_tid) {
      session->flushing_caps_tids.erase(it->first);
      in->flushing_cap_tids.erase(it++);
      ++flushed;
      continue;
    }
    cleaned &= ~it->second;
    if (!cleaned)
      break;
    ++it;
  }

  ldout(cct, 5) << "handle_cap_flush_ack mds." << session->mds_num
	  << " cleaned " << ccap_string(cleaned) << " on " << *in
	  << " with " << ccap_string(dirty) << dendl;

  if (flushed) {
    signal_cond_list(in->waitfor_caps);
    if (session->flushing_caps_tids.empty() ||
	*session->flushing_caps_tids.begin() > flush_ack_tid)
      sync_cond.Signal();
  }

  if (!dirty) {
    in->cap_dirtier_uid = -1;
    in->cap_dirtier_gid = -1;
  }

  if (!cleaned) {
    ldout(cct, 10) << " tid " << m->get_client_tid() << " != any cap bit tids" << dendl;
  } else {
    if (in->flushing_caps) {
      ldout(cct, 5) << "  flushing_caps " << ccap_string(in->flushing_caps)
	      << " -> " << ccap_string(in->flushing_caps & ~cleaned) << dendl;
      in->flushing_caps &= ~cleaned;
      if (in->flushing_caps == 0) {
	ldout(cct, 10) << " " << *in << " !flushing" << dendl;
	num_flushing_caps--;
	if (in->cap_snaps.empty())
	  in->flushing_cap_item.remove_myself();
      }
      if (!in->caps_dirty())
	put_inode(in);
    }
  }
  
  m->put();
}


void Client::handle_cap_flushsnap_ack(MetaSession *session, Inode *in, MClientCaps *m)
{
  mds_rank_t mds = session->mds_num;
  assert(in->caps[mds]);
  snapid_t follows = m->get_snap_follows();

  if (in->cap_snaps.count(follows)) {
    CapSnap *capsnap = in->cap_snaps[follows];
    if (m->get_client_tid() != capsnap->flush_tid) {
      ldout(cct, 10) << " tid " << m->get_client_tid() << " != " << capsnap->flush_tid << dendl;
    } else {
      ldout(cct, 5) << "handle_cap_flushedsnap mds." << mds << " flushed snap follows " << follows
	      << " on " << *in << dendl;
      in->cap_snaps.erase(follows);
      if (in->flushing_caps == 0 && in->cap_snaps.empty())
	in->flushing_cap_item.remove_myself();
      session->flushing_caps_tids.erase(capsnap->flush_tid);

      delete capsnap;
    }
  } else {
    ldout(cct, 5) << "handle_cap_flushedsnap DUP(?) mds." << mds << " flushed snap follows " << follows
	    << " on " << *in << dendl;
    // we may not have it if we send multiple FLUSHSNAP requests and (got multiple FLUSHEDSNAPs back)
  }

  m->put();
}

class C_Client_DentryInvalidate : public Context  {
private:
  Client *client;
  vinodeno_t dirino;
  vinodeno_t ino;
  string name;
public:
  C_Client_DentryInvalidate(Client *c, Dentry *dn, bool del) :
    client(c), name(dn->name) {
      if (client->use_faked_inos()) {
	dirino.ino = dn->dir->parent_inode->faked_ino;
	if (del)
	  ino.ino = dn->inode->faked_ino;
      } else {
	dirino = dn->dir->parent_inode->vino();
	if (del)
	  ino = dn->inode->vino();
      }
      if (!del)
	ino.ino = inodeno_t();
  }
  void finish(int r) {
    // _async_dentry_invalidate is responsible for its own locking
    assert(!client->client_lock.is_locked_by_me());
    client->_async_dentry_invalidate(dirino, ino, name);
  }
};

void Client::_async_dentry_invalidate(vinodeno_t dirino, vinodeno_t ino, string& name)
{
  if (unmounting)
    return;
  ldout(cct, 10) << "_async_dentry_invalidate '" << name << "' ino " << ino
		 << " in dir " << dirino << dendl;
  dentry_invalidate_cb(callback_handle, dirino, ino, name);
}

void Client::_schedule_invalidate_dentry_callback(Dentry *dn, bool del)
{
  if (dentry_invalidate_cb && dn->inode->ll_ref > 0)
    async_dentry_invalidator.queue(new C_Client_DentryInvalidate(this, dn, del));
}

void Client::_try_to_trim_inode(Inode *in, bool sched_inval)
{
  int ref = in->get_num_ref();

  if (in->dir && !in->dir->dentries.empty()) {
    for (auto p = in->dir->dentries.begin();
	 p != in->dir->dentries.end(); ) {
      Dentry *dn = p->second;
      ++p;
      /* rmsnap removes whole subtree, need trim inodes recursively.
       * we don't need to invalidate dentries recursively. because
       * invalidating a directory dentry effectively invalidate
       * whole subtree */
      if (in->snapid != CEPH_NOSNAP && dn->inode && dn->inode->is_dir())
	_try_to_trim_inode(dn->inode.get(), false);

      if (dn->lru_is_expireable())
	unlink(dn, true, false);  // keep dir, drop dentry
    }
    if (in->dir->dentries.empty()) {
      close_dir(in->dir);
      --ref;
    }
  }

  if (ref > 0 && (in->flags & I_SNAPDIR_OPEN)) {
    InodeRef snapdir = open_snapdir(in);
    _try_to_trim_inode(snapdir.get(), false);
    --ref;
  }

  if (ref > 0 && in->ll_ref > 0 && sched_inval) {
    set<Dentry*>::iterator q = in->dn_set.begin();
    while (q != in->dn_set.end()) {
      Dentry *dn = *q++;
      // FIXME: we play lots of unlink/link tricks when handling MDS replies,
      //        so in->dn_set doesn't always reflect the state of kernel's dcache.
      _schedule_invalidate_dentry_callback(dn, true);
      unlink(dn, true, true);
    }
  }
}

void Client::handle_cap_grant(MetaSession *session, Inode *in, Cap *cap, MClientCaps *m)
{
  mds_rank_t mds = session->mds_num;
  int used = get_caps_used(in);
  int wanted = in->caps_wanted();

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
    in->btime = m->btime;
  }
  bool deleted_inode = false;
  if ((issued & CEPH_CAP_LINK_EXCL) == 0) {
    in->nlink = m->head.nlink;
    if (in->nlink == 0 &&
	(new_caps & (CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL)))
      deleted_inode = true;
  }
  if ((issued & CEPH_CAP_XATTR_EXCL) == 0 &&
      m->xattrbl.length() &&
      m->head.xattr_version > in->xattr_version) {
    bufferlist::iterator p = m->xattrbl.begin();
    ::decode(in->xattrs, p);
    in->xattr_version = m->head.xattr_version;
  }
  update_inode_file_bits(in, m->get_truncate_seq(), m->get_truncate_size(), m->get_size(),
			 m->get_change_attr(), m->get_time_warp_seq(), m->get_ctime(),
			 m->get_mtime(), m->get_atime(),
			 m->inline_version, m->inline_data, issued);

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

  bool check = false;
  if (m->get_op() == CEPH_CAP_OP_IMPORT && m->get_wanted() != wanted)
    check = true;

  check_cap_issue(in, cap, new_caps);

  // update caps
  if (old_caps & ~new_caps) { 
    ldout(cct, 10) << "  revocation of " << ccap_string(~new_caps & old_caps) << dendl;
    cap->issued = new_caps;
    cap->implemented |= new_caps;

    if (((used & ~new_caps) & CEPH_CAP_FILE_BUFFER)
        && !_flush(in, new C_Client_FlushComplete(this, in))) {
      // waitin' for flush
    } else if ((old_caps & ~new_caps) & CEPH_CAP_FILE_CACHE) {
      if (_release(in))
	check = true;
    } else {
      cap->wanted = 0; // don't let check_caps skip sending a response to MDS
      check = true;
    }

  } else if (old_caps == new_caps) {
    ldout(cct, 10) << "  caps unchanged at " << ccap_string(old_caps) << dendl;
  } else {
    ldout(cct, 10) << "  grant, new caps are " << ccap_string(new_caps & ~old_caps) << dendl;
    cap->issued = new_caps;
    cap->implemented |= new_caps;

    if (cap == in->auth_cap) {
      // non-auth MDS is revoking the newly grant caps ?
      for (map<mds_rank_t, Cap*>::iterator it = in->caps.begin(); it != in->caps.end(); ++it) {
	if (it->second == cap)
	  continue;
	if (it->second->implemented & ~it->second->issued & new_caps) {
	  check = true;
	  break;
	}
      }
    }
  }

  if (check)
    check_caps(in, false);

  // wake up waiters
  if (new_caps)
    signal_cond_list(in->waitfor_caps);

  // may drop inode's last ref
  if (deleted_inode)
    _try_to_trim_inode(in, true);

  m->put();
}

int Client::_getgrouplist(gid_t** sgids, int uid, int gid)
{
  // cppcheck-suppress variableScope
  int sgid_count;
  gid_t *sgid_buf;

  if (getgroups_cb) {
    sgid_count = getgroups_cb(callback_handle, &sgid_buf);
    if (sgid_count > 0) {
      *sgids = sgid_buf;
      return sgid_count;
    }
  }

#if HAVE_GETGROUPLIST
  struct passwd *pw;
  pw = getpwuid(uid);
  if (pw == NULL) {
    ldout(cct, 3) << "getting user entry failed" << dendl;
    return -errno;
  }
  //use PAM to get the group list
  // initial number of group entries, defaults to posix standard of 16
  // PAM implementations may provide more than 16 groups....
  sgid_count = 16;
  sgid_buf = (gid_t*)malloc(sgid_count * sizeof(gid_t));
  if (sgid_buf == NULL) {
    ldout(cct, 3) << "allocating group memory failed" << dendl;
    return -ENOMEM;
  }

  while (1) {
#if defined(__APPLE__)
    if (getgrouplist(pw->pw_name, gid, (int*)sgid_buf, &sgid_count) == -1) {
#else
    if (getgrouplist(pw->pw_name, gid, sgid_buf, &sgid_count) == -1) {
#endif
      // we need to resize the group list and try again
      void *_realloc = NULL;
      if ((_realloc = realloc(sgid_buf, sgid_count * sizeof(gid_t))) == NULL) {
	ldout(cct, 3) << "allocating group memory failed" << dendl;
	free(sgid_buf);
	return -ENOMEM;
      }
      sgid_buf = (gid_t*)_realloc;
      continue;
    }
    // list was successfully retrieved
    break;
  }
  *sgids = sgid_buf;
  return sgid_count;
#else
  return 0;
#endif
}

int Client::inode_permission(Inode *in, uid_t uid, UserGroups& groups, unsigned want)
{
  if (uid == 0)
    return 0;

  if (uid != in->uid && (in->mode & S_IRWXG)) {
    int ret = _posix_acl_permission(in, uid, groups, want);
    if (ret != -EAGAIN)
      return ret;
  }

  // check permissions before doing anything else
  if (!in->check_mode(uid, groups, want))
    return -EACCES;
  return 0;
}

int Client::xattr_permission(Inode *in, const char *name, unsigned want, int uid, int gid)
{
  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();
  RequestUserGroups groups(this, uid, gid);

  int r = _getattr_for_perm(in, uid, gid);
  if (r < 0)
    goto out;

  r = 0;
  if (strncmp(name, "system.", 7) == 0) {
    if ((want & MAY_WRITE) && (uid != 0 && (uid_t)uid != in->uid))
      r = -EPERM;
  } else {
    r = inode_permission(in, uid, groups, want);
  }
out:
  ldout(cct, 3) << __func__ << " " << in << " = " << r <<  dendl;
  return r;
}

int Client::may_setattr(Inode *in, struct stat *st, int mask, int uid, int gid)
{
  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();
  RequestUserGroups groups(this, uid, gid);

  int r = _getattr_for_perm(in, uid, gid);
  if (r < 0)
    goto out;

  if (mask & CEPH_SETATTR_SIZE) {
    r = inode_permission(in, uid, groups, MAY_WRITE);
    if (r < 0)
      goto out;
  }

  r = -EPERM;
  if (mask & CEPH_SETATTR_UID) {
    if (uid != 0 && ((uid_t)uid != in->uid || st->st_uid != in->uid))
      goto out;
  }
  if (mask & CEPH_SETATTR_GID) {
    if (uid != 0 && ((uid_t)uid != in->uid ||
      	       (!groups.is_in(st->st_gid) && st->st_gid != in->gid)))
      goto out;
  }

  if (mask & CEPH_SETATTR_MODE) {
    if (uid != 0 && (uid_t)uid != in->uid)
      goto out;

    gid_t i_gid = (mask & CEPH_SETATTR_GID) ? st->st_gid : in->gid;
    if (uid != 0 && !groups.is_in(i_gid))
      st->st_mode &= ~S_ISGID;
  }

  if (mask & (CEPH_SETATTR_CTIME | CEPH_SETATTR_MTIME | CEPH_SETATTR_ATIME)) {
    if (uid != 0 && (uid_t)uid != in->uid) {
      int check_mask = CEPH_SETATTR_CTIME;
      if (!(mask & CEPH_SETATTR_MTIME_NOW))
	check_mask |= CEPH_SETATTR_MTIME;
      if (!(mask & CEPH_SETATTR_ATIME_NOW))
	check_mask |= CEPH_SETATTR_ATIME;
      if (check_mask & mask) {
	goto out;
      } else {
	r = inode_permission(in, uid, groups, MAY_WRITE);
	if (r < 0)
	  goto out;
      }
    }
  }
  r = 0;
out:
  ldout(cct, 3) << __func__ << " " << in << " = " << r <<  dendl;
  return r;
}

int Client::may_open(Inode *in, int flags, int uid, int gid)
{
  unsigned want = 0;

  if ((flags & O_ACCMODE) == O_WRONLY)
    want = MAY_WRITE;
  else if ((flags & O_ACCMODE) == O_RDWR)
    want = MAY_READ | MAY_WRITE;
  else if ((flags & O_ACCMODE) == O_RDONLY)
    want = MAY_READ;
  if (flags & O_TRUNC)
    want |= MAY_WRITE;

  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();
  RequestUserGroups groups(this, uid, gid);

  int r = 0;
  switch (in->mode & S_IFMT) {
    case S_IFLNK:
      r = -ELOOP;
      goto out;
    case S_IFDIR:
      if (want & MAY_WRITE) {
	r = -EISDIR;
	goto out;
      }
      break;
  }

  r = _getattr_for_perm(in, uid, gid);
  if (r < 0)
    goto out;

  r = inode_permission(in, uid, groups, want);
out:
  ldout(cct, 3) << __func__ << " " << in << " = " << r <<  dendl;
  return r;
}

int Client::may_lookup(Inode *dir, int uid, int gid)
{
  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();
  RequestUserGroups groups(this, uid, gid);

  int r = _getattr_for_perm(dir, uid, gid);
  if (r < 0)
    goto out;

  r = inode_permission(dir, uid, groups, MAY_EXEC);
out:
  ldout(cct, 3) << __func__ << " " << dir << " = " << r <<  dendl;
  return r;
}

int Client::may_create(Inode *dir, int uid, int gid)
{
  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();
  RequestUserGroups groups(this, uid, gid);

  int r = _getattr_for_perm(dir, uid, gid);
  if (r < 0)
    goto out;

  r = inode_permission(dir, uid, groups, MAY_EXEC | MAY_WRITE);
out:
  ldout(cct, 3) << __func__ << " " << dir << " = " << r <<  dendl;
  return r;
}

int Client::may_delete(Inode *dir, const char *name, int uid, int gid)
{
  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();
  RequestUserGroups groups(this, uid, gid);

  int r = _getattr_for_perm(dir, uid, gid);
  if (r < 0)
    goto out;

  r = inode_permission(dir, uid, groups, MAY_EXEC | MAY_WRITE);
  if (r < 0)
    goto out;

  /* 'name == NULL' means rmsnap */
  if (uid != 0 && name && (dir->mode & S_ISVTX)) {
    InodeRef otherin;
    r = _lookup(dir, name, CEPH_CAP_AUTH_SHARED, &otherin, uid, gid);
    if (r < 0)
      goto out;
    if (dir->uid != (uid_t)uid && otherin->uid != (uid_t)uid)
      r = -EPERM;
  }
out:
  ldout(cct, 3) << __func__ << " " << dir << " = " << r <<  dendl;
  return r;
}

int Client::may_hardlink(Inode *in, int uid, int gid)
{
  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();
  RequestUserGroups groups(this, uid, gid);

  int r = _getattr_for_perm(in, uid, gid);
  if (r < 0)
    goto out;

  if (uid == 0 || (uid_t)uid == in->uid) {
    r = 0;
    goto out;
  }

  r = -EPERM;
  if (!S_ISREG(in->mode))
    goto out;

  if (in->mode & S_ISUID)
    goto out;

  if ((in->mode & (S_ISGID | S_IXGRP)) == (S_ISGID | S_IXGRP))
    goto out;

  r = inode_permission(in, uid, groups, MAY_READ | MAY_WRITE);
out:
  ldout(cct, 3) << __func__ << " " << in << " = " << r <<  dendl;
  return r;
}

int Client::_getattr_for_perm(Inode *in, int uid, int gid)
{
  int mask = CEPH_STAT_CAP_MODE;
  bool force = false;
  if (acl_type != NO_ACL) {
    mask |= CEPH_STAT_CAP_XATTR;
    force = in->xattr_version == 0;
  }
  return _getattr(in, mask, uid, gid, force);
}

vinodeno_t Client::_get_vino(Inode *in)
{
  /* The caller must hold the client lock */
  return vinodeno_t(in->ino, in->snapid);
}

inodeno_t Client::_get_inodeno(Inode *in)
{
  /* The caller must hold the client lock */
  return in->ino;
}


/**
 * Resolve an MDS spec to a list of MDS daemon GIDs.
 *
 * The spec is a string representing a GID, rank, filesystem:rank, or name/id.
 * It may be '*' in which case it matches all GIDs.
 *
 * If no error is returned, the `targets` vector will be populated with at least
 * one MDS.
 */
int Client::resolve_mds(
    const std::string &mds_spec,
    std::vector<mds_gid_t> *targets)
{
  assert(fsmap != nullptr);
  assert(targets != nullptr);

  mds_role_t role;
  std::stringstream ss;
  int role_r = fsmap->parse_role(mds_spec, &role, ss);
  if (role_r == 0) {
    // We got a role, resolve it to a GID
    ldout(cct, 10) << __func__ << ": resolved '" << mds_spec << "' to role '"
      << role << "'" << dendl;
    targets->push_back(
        fsmap->get_filesystem(role.fscid)->mds_map.get_info(role.rank).global_id);
    return 0;
  }

  std::string strtol_err;
  long long rank_or_gid = strict_strtoll(mds_spec.c_str(), 10, &strtol_err);
  if (strtol_err.empty()) {
    // It is a possible GID
    const mds_gid_t mds_gid = mds_gid_t(rank_or_gid);
    if (fsmap->gid_exists(mds_gid)) {
      ldout(cct, 10) << __func__ << ": validated GID " << mds_gid << dendl;
      targets->push_back(mds_gid);
    } else {
      lderr(cct) << __func__ << ": GID " << mds_gid << " not in MDS map"
                 << dendl;
      return -ENOENT;
    }
  } else if (mds_spec == "*") {
    // It is a wildcard: use all MDSs
    const auto mds_info = fsmap->get_mds_info();

    if (mds_info.empty()) {
      lderr(cct) << __func__ << ": * passed but no MDS daemons found" << dendl;
      return -ENOENT;
    }

    for (const auto i : mds_info) {
      targets->push_back(i.first);
    }
  } else {
    // It did not parse as an integer, it is not a wildcard, it must be a name
    const mds_gid_t mds_gid = fsmap->find_mds_gid_by_name(mds_spec);
    if (mds_gid == 0) {
      lderr(cct) << "MDS ID '" << mds_spec << "' not found" << dendl;

      lderr(cct) << "FSMap: " << *fsmap << dendl;

      return -ENOENT;
    } else {
      ldout(cct, 10) << __func__ << ": resolved ID '" << mds_spec
                     << "' to GID " << mds_gid << dendl;
      targets->push_back(mds_gid);
    }
  }

  return 0;
}


/**
 * Authenticate with mon and establish global ID
 */
int Client::authenticate()
{
  assert(client_lock.is_locked_by_me());

  if (authenticated) {
    return 0;
  }

  client_lock.Unlock();
  int r = monclient->authenticate(cct->_conf->client_mount_timeout);
  client_lock.Lock();
  if (r < 0) {
    return r;
  }

  whoami = monclient->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));
  authenticated = true;

  return 0;
}

int Client::fetch_fsmap(bool user)
{
  int r;
  // Retrieve FSMap to enable looking up daemon addresses.  We need FSMap
  // rather than MDSMap because no one MDSMap contains all the daemons, and
  // a `tell` can address any daemon.
  version_t fsmap_latest;
  do {
    C_SaferCond cond;
    monclient->get_version("fsmap", &fsmap_latest, NULL, &cond);
    client_lock.Unlock();
    r = cond.wait();
    client_lock.Lock();
  } while (r == -EAGAIN);

  if (r < 0) {
    lderr(cct) << "Failed to learn FSMap version: " << cpp_strerror(r) << dendl;
    return r;
  }

  ldout(cct, 10) << __func__ << " learned FSMap version " << fsmap_latest << dendl;

  if (user) {
    if (fsmap_user == nullptr || fsmap_user->get_epoch() < fsmap_latest) {
      monclient->sub_want("fsmap.user", fsmap_latest, CEPH_SUBSCRIBE_ONETIME);
      monclient->renew_subs();
      wait_on_list(waiting_for_fsmap);
    }
    assert(fsmap_user != nullptr);
    assert(fsmap_user->get_epoch() >= fsmap_latest);
  } else {
    if (fsmap == nullptr || fsmap->get_epoch() < fsmap_latest) {
      monclient->sub_want("fsmap", fsmap_latest, CEPH_SUBSCRIBE_ONETIME);
      monclient->renew_subs();
      wait_on_list(waiting_for_fsmap);
    }
    assert(fsmap != nullptr);
    assert(fsmap->get_epoch() >= fsmap_latest);
  }
  ldout(cct, 10) << __func__ << " finished waiting for FSMap version "
		 << fsmap_latest << dendl;
  return 0;
}

/**
 *
 * @mds_spec one of ID, rank, GID, "*"
 *
 */
int Client::mds_command(
    const std::string &mds_spec,
    const vector<string>& cmd,
    const bufferlist& inbl,
    bufferlist *outbl,
    string *outs,
    Context *onfinish)
{
  Mutex::Locker lock(client_lock);

  assert(initialized);

  int r;
  r = authenticate();
  if (r < 0) {
    return r;
  }

  r = fetch_fsmap(false);
  if (r < 0) {
    return r;
  }

  // Look up MDS target(s) of the command
  std::vector<mds_gid_t> targets;
  r = resolve_mds(mds_spec, &targets);
  if (r < 0) {
    return r;
  }

  // If daemons are laggy, we won't send them commands.  If all
  // are laggy then we fail.
  std::vector<mds_gid_t> non_laggy;
  for (const auto gid : targets) {
    const auto info = fsmap->get_info_gid(gid);
    if (!info.laggy()) {
      non_laggy.push_back(gid);
    }
  }
  if (non_laggy.size() == 0) {
    *outs = "All targeted MDS daemons are laggy";
    return -ENOENT;
  }

  // Send commands to targets
  C_GatherBuilder gather(cct, onfinish);
  for (const auto target_gid : non_laggy) {
    ceph_tid_t tid = ++last_tid;
    const auto info = fsmap->get_info_gid(target_gid);

    // Open a connection to the target MDS
    entity_inst_t inst = info.get_inst();
    ConnectionRef conn = messenger->get_connection(inst);

    // Generate CommandOp state
    CommandOp op;
    op.tid = tid;
    op.on_finish = gather.new_sub();
    op.outbl = outbl;
    op.outs = outs;
    op.mds_gid = target_gid;
    op.con = conn;
    commands[op.tid] = op;

    ldout(cct, 4) << __func__ << ": new command op to " << target_gid
      << " tid=" << op.tid << cmd << dendl;

    // Construct and send MCommand
    MCommand *m = new MCommand(monclient->get_fsid());
    m->cmd = cmd;
    m->set_data(inbl);
    m->set_tid(tid);
    conn->send_message(m);
  }
  gather.activate();

  return 0;
}

void Client::handle_command_reply(MCommandReply *m)
{
  ceph_tid_t const tid = m->get_tid();

  ldout(cct, 10) << __func__ << ": tid=" << m->get_tid() << dendl;

  map<ceph_tid_t, CommandOp>::iterator opiter = commands.find(tid);
  if (opiter == commands.end()) {
    ldout(cct, 1) << __func__ << ": unknown tid " << tid << ", dropping" << dendl;
    m->put();
    return;
  }

  CommandOp const &op = opiter->second;
  if (op.outbl) {
    op.outbl->claim(m->get_data());
  }
  if (op.outs) {
    *op.outs = m->rs;
  }

  if (op.on_finish) {
    op.on_finish->complete(m->r);
  }

  m->put();
}

// -------------------
// MOUNT

int Client::mount(const std::string &mount_root, bool require_mds)
{
  Mutex::Locker lock(client_lock);

  if (mounted) {
    ldout(cct, 5) << "already mounted" << dendl;
    return 0;
  }

  int r = authenticate();
  if (r < 0) {
    return r;
  }

  std::string want = "mdsmap";
  const auto &mds_ns = cct->_conf->client_mds_namespace;
  if (!mds_ns.empty()) {
    r = fetch_fsmap(true);
    if (r < 0)
      return r;
    fs_cluster_id_t cid = fsmap_user->get_fs_cid(mds_ns);
    if (cid == FS_CLUSTER_ID_NONE)
      return -ENOENT;

    std::ostringstream oss;
    oss << want << "." << cid;
    want = oss.str();
  }
  ldout(cct, 10) << "Subscribing to map '" << want << "'" << dendl;

  monclient->sub_want(want, 0, 0);
  monclient->renew_subs();

  tick(); // start tick
  
  if (require_mds) {
    while (1) {
      auto availability = mdsmap->is_cluster_available();
      if (availability == MDSMap::STUCK_UNAVAILABLE) {
        // Error out
        ldout(cct, 10) << "mds cluster unavailable: epoch=" << mdsmap->get_epoch() << dendl;
        return CEPH_FUSE_NO_MDS_UP;
      } else if (availability == MDSMap::AVAILABLE) {
        // Continue to mount
        break;
      } else if (availability == MDSMap::TRANSIENT_UNAVAILABLE) {
        // Else, wait.  MDSMonitor will update the map to bring
        // us to a conclusion eventually.
        wait_on_list(waiting_for_mdsmap);
      } else {
        // Unexpected value!
        assert(0);
      }
    }
  }

  filepath fp(CEPH_INO_ROOT);
  if (!mount_root.empty())
    fp = filepath(mount_root.c_str());
  while (true) {
    MetaRequest *req = new MetaRequest(CEPH_MDS_OP_GETATTR);
    req->set_filepath(fp);
    req->head.args.getattr.mask = CEPH_STAT_CAP_INODE_ALL;
    int res = make_request(req, -1, -1);
    if (res < 0) {
      if (res == -EACCES && root) {
	ldout(cct, 1) << __func__ << " EACCES on parent of mount point; quotas may not work" << dendl;
	break;
      }
      return res;
    }

    if (fp.depth())
      fp.pop_dentry();
    else
      break;
  }

  assert(root);
  _ll_get(root);

  mounted = true;

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

void Client::_close_sessions()
{
  while (!mds_sessions.empty()) {
    // send session closes!
    for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
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
}

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

  cwd.reset();

  // clean up any unclosed files
  while (!fd_map.empty()) {
    Fh *fh = fd_map.begin()->second;
    fd_map.erase(fd_map.begin());
    ldout(cct, 0) << " destroyed lost open file " << fh << " on " << *fh->inode << dendl;
    _release_fh(fh);
  }
  
  while (!ll_unclosed_fh_set.empty()) {
    set<Fh*>::iterator it = ll_unclosed_fh_set.begin();
    Fh *fh = *it;
    ll_unclosed_fh_set.erase(fh);
    ldout(cct, 0) << " destroyed lost open file " << fh << " on " << *(fh->inode) << dendl;
    _release_fh(fh);
  }

  while (!opened_dirs.empty()) {
    dir_result_t *dirp = *opened_dirs.begin();
    ldout(cct, 0) << " destroyed lost open dir " << dirp << " on " << *dirp->inode << dendl;
    _closedir(dirp);
  }

  _ll_drop_pins();

  while (unsafe_sync_write > 0) {
    ldout(cct, 0) << unsafe_sync_write << " unsafe_sync_writes, waiting"  << dendl;
    mount_cond.Wait(client_lock);
  }

  if (cct->_conf->client_oc) {
    // flush/release all buffered data
    ceph::unordered_map<vinodeno_t, Inode*>::iterator next;
    for (ceph::unordered_map<vinodeno_t, Inode*>::iterator p = inode_map.begin();
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
	InodeRef tmp_ref(in);
	_release(in);
	_flush(in, new C_Client_FlushComplete(this, in));
      }
    }
  }

  flush_caps();
  wait_sync_caps(last_flush_tid);

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

  _close_sessions();

  mounted = false;

  ldout(cct, 2) << "unmounted." << dendl;
}



class C_C_Tick : public Context {
  Client *client;
public:
  explicit C_C_Tick(Client *c) : client(c) {}
  void finish(int r) {
    // Called back via Timer, which takes client_lock for us
    assert(client->client_lock.is_locked_by_me());
    client->tick();
  }
};

void Client::flush_cap_releases()
{
  // send any cap releases
  for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
       p != mds_sessions.end();
       ++p) {
    if (p->second->release && mdsmap->is_clientreplay_or_active_or_stopping(
          p->first)) {
      if (cct->_conf->client_inject_release_failure) {
        ldout(cct, 20) << __func__ << " injecting failure to send cap release message" << dendl;
        p->second->release->put();
      } else {
        p->second->con->send_message(p->second->release);
      }
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

  if (!mounted && !mds_requests.empty()) {
    MetaRequest *req = mds_requests.begin()->second;
    if (req->op_stamp + cct->_conf->client_mount_timeout < now) {
      req->abort(-ETIMEDOUT);
      if (req->caller_cond) {
	req->kick = true;
	req->caller_cond->Signal();
      }
      signal_cond_list(waiting_for_mdsmap);
      for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
	   p != mds_sessions.end();
	  ++p)
	signal_context_list(p->second->waiting_for_open);
    }
  }

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

  trim_cache(true);
}

void Client::renew_caps()
{
  ldout(cct, 10) << "renew_caps()" << dendl;
  last_cap_renew = ceph_clock_now(cct);
  
  for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
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
  session->con->send_message(new MClientSession(CEPH_SESSION_REQUEST_RENEWCAPS, seq));
}


// ===============================================================
// high level (POSIXy) interface

int Client::_do_lookup(Inode *dir, const string& name, int mask, InodeRef *target,
		       int uid, int gid)
{
  int op = dir->snapid == CEPH_SNAPDIR ? CEPH_MDS_OP_LOOKUPSNAP : CEPH_MDS_OP_LOOKUP;
  MetaRequest *req = new MetaRequest(op);
  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);
  req->set_inode(dir);
  if (cct->_conf->client_debug_getattr_caps && op == CEPH_MDS_OP_LOOKUP)
      mask |= DEBUG_GETATTR_CAPS;
  req->head.args.getattr.mask = mask;

  ldout(cct, 10) << "_do_lookup on " << path << dendl;

  int r = make_request(req, uid, gid, target);
  ldout(cct, 10) << "_do_lookup res is " << r << dendl;
  return r;
}

int Client::_lookup(Inode *dir, const string& dname, int mask,
		    InodeRef *target, int uid, int gid)
{
  int r = 0;
  Dentry *dn = NULL;

  if (!dir->is_dir()) {
    r = -ENOTDIR;
    goto done;
  }

  if (dname == "..") {
    if (dir->dn_set.empty())
      *target = dir;
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
    dn = dir->dir->dentries[dname];

    ldout(cct, 20) << "_lookup have dn " << dname << " mds." << dn->lease_mds << " ttl " << dn->lease_ttl
	     << " seq " << dn->lease_seq
	     << dendl;

    if (!dn->inode || dn->inode->caps_issued_mask(mask)) {
      // is dn lease valid?
      utime_t now = ceph_clock_now(cct);
      if (dn->lease_mds >= 0 &&
	  dn->lease_ttl > now &&
	  mds_sessions.count(dn->lease_mds)) {
	MetaSession *s = mds_sessions[dn->lease_mds];
	if (s->cap_ttl > now &&
	    s->cap_gen == dn->lease_gen) {
	  // touch this mds's dir cap too, even though we don't _explicitly_ use it here, to
	  // make trim_caps() behave.
	  dir->try_touch_cap(dn->lease_mds);
	  goto hit_dn;
	}
	ldout(cct, 20) << " bad lease, cap_ttl " << s->cap_ttl << ", cap_gen " << s->cap_gen
		       << " vs lease_gen " << dn->lease_gen << dendl;
      }
      // dir lease?
      if (dir->caps_issued_mask(CEPH_CAP_FILE_SHARED)) {
	if (dn->cap_shared_gen == dir->shared_gen &&
	    (!dn->inode || dn->inode->caps_issued_mask(mask)))
	      goto hit_dn;
	if (!dn->inode && (dir->flags & I_COMPLETE)) {
	  ldout(cct, 10) << "_lookup concluded ENOENT locally for "
			 << *dir << " dn '" << dname << "'" << dendl;
	  return -ENOENT;
	}
      }
    } else {
      ldout(cct, 20) << " no cap on " << dn->inode->vino() << dendl;
    }
  } else {
    // can we conclude ENOENT locally?
    if (dir->caps_issued_mask(CEPH_CAP_FILE_SHARED) &&
	(dir->flags & I_COMPLETE)) {
      ldout(cct, 10) << "_lookup concluded ENOENT locally for " << *dir << " dn '" << dname << "'" << dendl;
      return -ENOENT;
    }
  }

  r = _do_lookup(dir, dname, mask, target, uid, gid);
  goto done;

 hit_dn:
  if (dn->inode) {
    *target = dn->inode;
  } else {
    r = -ENOENT;
  }
  touch_dn(dn);

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

int Client::path_walk(const filepath& origpath, InodeRef *end, bool followsym,
		      int mask, int uid, int gid)
{
  filepath path = origpath;
  InodeRef cur;
  if (origpath.absolute())
    cur = root;
  else
    cur = cwd;
  assert(cur);

  if (uid < 0)
    uid = get_uid();
  if (gid < 0)
    gid = get_gid();

  ldout(cct, 10) << "path_walk " << path << dendl;

  int symlinks = 0;

  unsigned i=0;
  while (i < path.depth() && cur) {
    int caps = 0;
    const string &dname = path[i];
    ldout(cct, 10) << " " << i << " " << *cur << " " << dname << dendl;
    ldout(cct, 20) << "  (path is " << path << ")" << dendl;
    InodeRef next;
    if (cct->_conf->client_permissions) {
      int r = may_lookup(cur.get(), uid, gid);
      if (r < 0)
	return r;
      caps = CEPH_CAP_AUTH_SHARED;
    }

    /* Get extra requested caps on the last component */
    if (i == (path.depth() - 1))
      caps |= mask;

    int r = _lookup(cur.get(), dname, caps, &next, uid, gid);
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
    cur.swap(next);
    i++;
  }
  if (!cur)
    return -ENOENT;
  if (end)
    end->swap(cur);
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

  InodeRef in, dir;
  int r = path_walk(existing, &in);
  if (r < 0)
    goto out;
  r = path_walk(path, &dir);
  if (r < 0)
    goto out;
  if (cct->_conf->client_permissions) {
    if (S_ISDIR(in->mode)) {
      r = -EPERM;
      goto out;
    }
    r = may_hardlink(in.get());
    if (r < 0)
      goto out;
    r = may_create(dir.get());
    if (r < 0)
      goto out;
  }
  r = _link(in.get(), dir.get(), name.c_str());
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
  InodeRef dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    r = may_delete(dir.get(), name.c_str());
    if (r < 0)
      return r;
  }
  return _unlink(dir.get(), name.c_str());
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

  InodeRef fromdir, todir;
  int r = path_walk(from, &fromdir);
  if (r < 0)
    goto out;
  r = path_walk(to, &todir);
  if (r < 0)
    goto out;

  if (cct->_conf->client_permissions) {
    int r = may_delete(fromdir.get(), fromname.c_str());
    if (r < 0)
      return r;
    r = may_delete(todir.get(), toname.c_str());
    if (r < 0 && r != -ENOENT)
      return r;
  }
  r = _rename(fromdir.get(), fromname.c_str(), todir.get(), toname.c_str());
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
  InodeRef dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    r = may_create(dir.get());
    if (r < 0)
      return r;
  }
  return _mkdir(dir.get(), name.c_str(), mode);
}

int Client::mkdirs(const char *relpath, mode_t mode)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 10) << "Client::mkdirs " << relpath << dendl;
  tout(cct) << "mkdirs" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;

  uid_t uid = get_uid();
  gid_t gid = get_gid();

  //get through existing parts of path
  filepath path(relpath);
  unsigned int i;
  int r = 0, caps = 0;
  InodeRef cur, next;
  cur = cwd;
  for (i=0; i<path.depth(); ++i) {
    if (cct->_conf->client_permissions) {
      r = may_lookup(cur.get(), uid, gid);
      if (r < 0)
	break;
      caps = CEPH_CAP_AUTH_SHARED;
    }
    r = _lookup(cur.get(), path[i].c_str(), caps, &next, uid, gid);
    if (r < 0)
      break;
    cur.swap(next);
  }
  //check that we have work left to do
  if (i==path.depth()) return -EEXIST;
  if (r!=-ENOENT) return r;
  ldout(cct, 20) << "mkdirs got through " << i << " directories on path " << relpath << dendl;
  //make new directory at each level
  for (; i<path.depth(); ++i) {
    if (cct->_conf->client_permissions) {
      r = may_create(cur.get(), uid, gid);
      if (r < 0)
	return r;
    }
    //make new dir
    r = _mkdir(cur.get(), path[i].c_str(), mode, uid, gid, &next);
    //check proper creation/existence
    if (r < 0) return r;
    //move to new dir and continue
    cur.swap(next);
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
  InodeRef dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    int r = may_delete(dir.get(), name.c_str());
    if (r < 0)
      return r;
  }
  return _rmdir(dir.get(), name.c_str());
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
  InodeRef dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    int r = may_create(dir.get());
    if (r < 0)
      return r;
  }
  return _mknod(dir.get(), name.c_str(), mode, rdev);
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
  InodeRef dir;
  int r = path_walk(path, &dir);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    int r = may_create(dir.get());
    if (r < 0)
      return r;
  }
  return _symlink(dir.get(), name.c_str(), target);
}

int Client::readlink(const char *relpath, char *buf, loff_t size) 
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "readlink" << std::endl;
  tout(cct) << relpath << std::endl;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, false);
  if (r < 0)
    return r;

  return _readlink(in.get(), buf, size);
}

int Client::_readlink(Inode *in, char *buf, size_t size)
{
  if (!in->is_symlink())
    return -EINVAL;

  // copy into buf (at most size bytes)
  int r = in->symlink.length();
  if (r > (int)size)
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

int Client::_do_setattr(Inode *in, struct stat *attr, int mask, int uid, int gid,
			InodeRef *inp)
{
  int issued = in->caps_issued();

  ldout(cct, 10) << "_setattr mask " << mask << " issued " <<
    ccap_string(issued) << dendl;

  if (in->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if ((mask & CEPH_SETATTR_SIZE) &&
      (unsigned long)attr->st_size > in->size &&
      is_quota_bytes_exceeded(in, (unsigned long)attr->st_size - in->size)) {
    return -EDQUOT;
  }

  if (uid < 0) {
    uid = get_uid();
    gid = get_gid();
  }

  // make the change locally?
  if ((in->cap_dirtier_uid >= 0 && uid != in->cap_dirtier_uid) ||
      (in->cap_dirtier_gid >= 0 && gid != in->cap_dirtier_gid)) {
    ldout(cct, 10) << __func__ << " caller " << uid << ":" << gid
		   << " != cap dirtier " << in->cap_dirtier_uid << ":"
		   << in->cap_dirtier_gid << ", forcing sync setattr"
		   << dendl;
    /*
     * This works because we implicitly flush the caps as part of the
     * request, so the cap update check will happen with the writeback
     * cap context, and then the setattr check will happen with the
     * caller's context.
     *
     * In reality this pattern is likely pretty rare (different users
     * setattr'ing the same file).  If that turns out not to be the
     * case later, we can build a more complex pipelined cap writeback
     * infrastructure...
     */
    if (!mask)
      mask |= CEPH_SETATTR_CTIME;
    goto force_request;
  }

  if (!mask) {
    // caller just needs us to bump the ctime
    in->ctime = ceph_clock_now(cct);
    in->cap_dirtier_uid = uid;
    in->cap_dirtier_gid = gid;
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
      in->cap_dirtier_uid = uid;
      in->cap_dirtier_gid = gid;
      in->mode = (in->mode & ~07777) | (attr->st_mode & 07777);
      mark_caps_dirty(in, CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_MODE;
      ldout(cct,10) << "changing mode to " << attr->st_mode << dendl;
    }
    if (mask & CEPH_SETATTR_UID) {
      in->ctime = ceph_clock_now(cct);
      in->cap_dirtier_uid = uid;
      in->cap_dirtier_gid = gid;
      in->uid = attr->st_uid;
      mark_caps_dirty(in, CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_UID;
      ldout(cct,10) << "changing uid to " << attr->st_uid << dendl;
    }
    if (mask & CEPH_SETATTR_GID) {
      in->ctime = ceph_clock_now(cct);
      in->cap_dirtier_uid = uid;
      in->cap_dirtier_gid = gid;
      in->gid = attr->st_gid;
      mark_caps_dirty(in, CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_GID;
      ldout(cct,10) << "changing gid to " << attr->st_gid << dendl;
    }
  }
  if (in->caps_issued_mask(CEPH_CAP_FILE_EXCL)) {
    if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME)) {
      if (mask & CEPH_SETATTR_MTIME)
        in->mtime = utime_t(stat_get_mtime_sec(attr), stat_get_mtime_nsec(attr));
      if (mask & CEPH_SETATTR_ATIME)
        in->atime = utime_t(stat_get_atime_sec(attr), stat_get_atime_nsec(attr));
      in->ctime = ceph_clock_now(cct);
      in->cap_dirtier_uid = uid;
      in->cap_dirtier_gid = gid;
      in->time_warp_seq++;
      mark_caps_dirty(in, CEPH_CAP_FILE_EXCL);
      mask &= ~(CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME);
    }
  }
  if (!mask) {
    in->change_attr++;
    return 0;
  }

force_request:
  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_SETATTR);

  filepath path;

  in->change_attr++;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_inode(in);

  if (mask & CEPH_SETATTR_MODE) {
    req->head.args.setattr.mode = attr->st_mode;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
    ldout(cct,10) << "changing mode to " << attr->st_mode << dendl;
  }
  if (mask & CEPH_SETATTR_UID) {
    req->head.args.setattr.uid = attr->st_uid;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
    ldout(cct,10) << "changing uid to " << attr->st_uid << dendl;
  }
  if (mask & CEPH_SETATTR_GID) {
    req->head.args.setattr.gid = attr->st_gid;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
    ldout(cct,10) << "changing gid to " << attr->st_gid << dendl;
  }
  if (mask & CEPH_SETATTR_MTIME) {
    utime_t mtime = utime_t(stat_get_mtime_sec(attr), stat_get_mtime_nsec(attr));
    req->head.args.setattr.mtime = mtime;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED | CEPH_CAP_FILE_RD |
      CEPH_CAP_FILE_WR;
  }
  if (mask & CEPH_SETATTR_ATIME) {
    utime_t atime = utime_t(stat_get_atime_sec(attr), stat_get_atime_nsec(attr));
    req->head.args.setattr.atime = atime;
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

int Client::_setattr(Inode *in, struct stat *attr, int mask, int uid, int gid,
		     InodeRef *inp)
{
  int ret = _do_setattr(in, attr, mask, uid, gid, inp);
  if (ret < 0)
   return ret;
  if (mask & CEPH_SETATTR_MODE)
    ret = _posix_acl_chmod(in, attr->st_mode, uid, gid);
  return ret;
}

int Client::_setattr(InodeRef &in, struct stat *attr, int mask)
{
  mask &= (CEPH_SETATTR_MODE | CEPH_SETATTR_UID |
	   CEPH_SETATTR_GID | CEPH_SETATTR_MTIME |
	   CEPH_SETATTR_ATIME | CEPH_SETATTR_SIZE |
	   CEPH_SETATTR_CTIME);
  if (cct->_conf->client_permissions) {
    int r = may_setattr(in.get(), attr, mask);
    if (r < 0)
      return r;
  }
  return _setattr(in.get(), attr, mask);
}

int Client::setattr(const char *relpath, struct stat *attr, int mask)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "setattr" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mask  << std::endl;

  filepath path(relpath);
  InodeRef in;
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
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  return _setattr(f->inode, attr, mask);
}

int Client::stat(const char *relpath, struct stat *stbuf,
			  frag_info_t *dirstat, int mask)
{
  ldout(cct, 3) << "stat enter (relpath " << relpath << " mask " << mask << ")" << dendl;
  Mutex::Locker lock(client_lock);
  tout(cct) << "stat" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, true, mask);
  if (r < 0)
    return r;
  r = _getattr(in, mask);
  if (r < 0) {
    ldout(cct, 3) << "stat exit on error!" << dendl;
    return r;
  }
  fill_stat(in, stbuf, dirstat);
  ldout(cct, 3) << "stat exit (relpath " << relpath << " mask " << mask << ")" << dendl;
  return r;
}

unsigned Client::statx_to_mask(unsigned int flags, unsigned int want)
{
  unsigned mask = 0;

  /* if NO_ATTR_SYNC is set, then we don't need any -- just use what's in cache */
  if (flags & AT_NO_ATTR_SYNC)
    goto out;

  /* Always set PIN to distinguish from AT_NO_ATTR_SYNC case */
  mask |= CEPH_CAP_PIN;
  if (want & (CEPH_STATX_MODE|CEPH_STATX_UID|CEPH_STATX_GID|CEPH_STATX_RDEV|CEPH_STATX_BTIME))
    mask |= CEPH_CAP_AUTH_SHARED;
  if (want & CEPH_STATX_NLINK)
    mask |= CEPH_CAP_LINK_SHARED;
  if (want & (CEPH_STATX_ATIME|CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_SIZE|CEPH_STATX_BLOCKS|CEPH_STATX_VERSION))
    mask |= CEPH_CAP_FILE_SHARED;

out:
  return mask;
}

int Client::statx(const char *relpath, struct ceph_statx *stx,
		  unsigned int want, unsigned int flags)
{
  ldout(cct, 3) << "statx enter (relpath " << relpath << " want " << want << ")" << dendl;
  Mutex::Locker lock(client_lock);
  tout(cct) << "statx" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  InodeRef in;

  unsigned mask = statx_to_mask(flags, want);

  int r = path_walk(path, &in, flags & AT_SYMLINK_NOFOLLOW, mask);
  if (r < 0)
    return r;

  if (mask && !in->caps_issued_mask(mask)) {
    r = _getattr(in, mask);
    if (r < 0) {
      ldout(cct, 3) << "statx exit on error!" << dendl;
      return r;
    }
  }

  fill_statx(in, mask, stx);
  ldout(cct, 3) << "statx exit (relpath " << relpath << " mask " << stx->stx_mask << ")" << dendl;
  return r;
}

int Client::lstat(const char *relpath, struct stat *stbuf,
			  frag_info_t *dirstat, int mask)
{
  ldout(cct, 3) << "lstat enter (relpath " << relpath << " mask " << mask << ")" << dendl;
  Mutex::Locker lock(client_lock);
  tout(cct) << "lstat" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  InodeRef in;
  // don't follow symlinks
  int r = path_walk(path, &in, false, mask);
  if (r < 0)
    return r;
  r = _getattr(in, mask);
  if (r < 0) {
    ldout(cct, 3) << "lstat exit on error!" << dendl;
    return r;
  }
  fill_stat(in, stbuf, dirstat);
  ldout(cct, 3) << "lstat exit (relpath " << relpath << " mask " << mask << ")" << dendl;
  return r;
}

int Client::fill_stat(Inode *in, struct stat *st, frag_info_t *dirstat, nest_info_t *rstat)
{
  ldout(cct, 10) << "fill_stat on " << in->ino << " snap/dev" << in->snapid
	   << " mode 0" << oct << in->mode << dec
	   << " mtime " << in->mtime << " ctime " << in->ctime << dendl;
  memset(st, 0, sizeof(struct stat));
  if (use_faked_inos())
    st->st_ino = in->faked_ino;
  else
    st->st_ino = in->ino;
  st->st_dev = in->snapid;
  st->st_mode = in->mode;
  st->st_rdev = in->rdev;
  st->st_nlink = in->nlink;
  st->st_uid = in->uid;
  st->st_gid = in->gid;
  if (in->ctime > in->mtime) {
    stat_set_ctime_sec(st, in->ctime.sec());
    stat_set_ctime_nsec(st, in->ctime.nsec());
  } else {
    stat_set_ctime_sec(st, in->mtime.sec());
    stat_set_ctime_nsec(st, in->mtime.nsec());
  }
  stat_set_atime_sec(st, in->atime.sec());
  stat_set_atime_nsec(st, in->atime.nsec());
  stat_set_mtime_sec(st, in->mtime.sec());
  stat_set_mtime_nsec(st, in->mtime.nsec());
  if (in->is_dir()) {
    if (cct->_conf->client_dirsize_rbytes)
      st->st_size = in->rstat.rbytes;
    else
      st->st_size = in->dirstat.size();
    st->st_blocks = 1;
  } else {
    st->st_size = in->size;
    st->st_blocks = (in->size + 511) >> 9;
  }
  st->st_blksize = MAX(in->layout.stripe_unit, 4096);

  if (dirstat)
    *dirstat = in->dirstat;
  if (rstat)
    *rstat = in->rstat;

  return in->caps_issued();
}

void Client::fill_statx(Inode *in, unsigned int mask, struct ceph_statx *stx)
{
  ldout(cct, 10) << "fill_statx on " << in->ino << " snap/dev" << in->snapid
	   << " mode 0" << oct << in->mode << dec
	   << " mtime " << in->mtime << " ctime " << in->ctime << dendl;
  memset(stx, 0, sizeof(struct ceph_statx));

  /*
   * If mask is 0, then the caller set AT_NO_ATTR_SYNC. Reset the mask
   * so that all bits are set.
   */
  if (!mask)
    mask = ~0;

  /* These are always considered to be available */
  stx->stx_dev_major = in->snapid >> 32;
  stx->stx_dev_minor = (uint32_t)in->snapid;
  stx->stx_blksize = MAX(in->layout.stripe_unit, 4096);

  if (use_faked_inos())
   stx->stx_ino = in->faked_ino;
  else
    stx->stx_ino = in->ino;
  stx->stx_rdev_minor = MINOR(in->rdev);
  stx->stx_rdev_major = MAJOR(in->rdev);
  stx->stx_mask |= (CEPH_STATX_INO|CEPH_STATX_RDEV);

  if (mask & CEPH_CAP_AUTH_SHARED) {
    stx->stx_uid = in->uid;
    stx->stx_gid = in->gid;
    stx->stx_mode = in->mode;
    stx->stx_btime = in->btime.sec();
    stx->stx_btime_ns = in->btime.nsec();
    stx->stx_mask |= (CEPH_STATX_MODE|CEPH_STATX_UID|CEPH_STATX_GID|CEPH_STATX_BTIME);
  }

  if (mask & CEPH_CAP_LINK_SHARED) {
    stx->stx_nlink = in->nlink;
    stx->stx_mask |= CEPH_STATX_NLINK;
  }

  if (mask & CEPH_CAP_FILE_SHARED) {
    if (in->ctime > in->mtime) {
      stx->stx_ctime = in->ctime.sec();
      stx->stx_ctime_ns = in->ctime.nsec();
    } else {
      stx->stx_ctime = in->mtime.sec();
      stx->stx_ctime_ns = in->mtime.nsec();
    }
    stx->stx_atime = in->atime.sec();
    stx->stx_atime_ns = in->atime.nsec();
    stx->stx_mtime = in->mtime.sec();
    stx->stx_mtime_ns = in->mtime.nsec();

    if (in->is_dir()) {
      if (cct->_conf->client_dirsize_rbytes)
	stx->stx_size = in->rstat.rbytes;
      else
	stx->stx_size = in->dirstat.size();
      stx->stx_blocks = 1;
    } else {
      stx->stx_size = in->size;
      stx->stx_blocks = (in->size + 511) >> 9;
    }
    stx->stx_mask |= (CEPH_STATX_ATIME|CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_SIZE|CEPH_STATX_BLOCKS);
  }
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
  InodeRef in;
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
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
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
  InodeRef in;
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
  InodeRef in;
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
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
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
  InodeRef in;
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
  InodeRef in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  struct stat attr;
  stat_set_mtime_sec(&attr, buf->modtime);
  stat_set_mtime_nsec(&attr, 0);
  stat_set_atime_sec(&attr, buf->actime);
  stat_set_atime_nsec(&attr, 0);
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
  InodeRef in;
  // don't follow symlinks
  int r = path_walk(path, &in, false);
  if (r < 0)
    return r;
  struct stat attr;
  stat_set_mtime_sec(&attr, buf->modtime);
  stat_set_mtime_nsec(&attr, 0);
  stat_set_atime_sec(&attr, buf->actime);
  stat_set_atime_nsec(&attr, 0);
  return _setattr(in, &attr, CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME);
}

int Client::flock(int fd, int operation, uint64_t owner)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "flock" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << operation << std::endl;
  tout(cct) << owner << std::endl;
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;

  return _flock(f, operation, owner);
}

int Client::opendir(const char *relpath, dir_result_t **dirpp) 
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "opendir" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    int r = may_open(in.get(), O_RDONLY);
    if (r < 0)
      return r;
  }
  r = _opendir(in.get(), dirpp);
  tout(cct) << (unsigned long)*dirpp << std::endl;
  return r;
}

int Client::_opendir(Inode *in, dir_result_t **dirpp, int uid, int gid) 
{
  if (!in->is_dir())
    return -ENOTDIR;
  *dirpp = new dir_result_t(in);
  opened_dirs.insert(*dirpp);
  (*dirpp)->owner_uid = uid;
  (*dirpp)->owner_gid = gid;
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
    dirp->inode.reset();
  }
  _readdir_drop_dirp_buffer(dirp);
  opened_dirs.erase(dirp);
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

  if (offset == dirp->offset)
    return;

  if (offset > dirp->offset)
    dirp->release_count = 0;   // bump if we do a forward seek
  else
    dirp->ordered_count = 0;   // disable filling readdir cache

  if (dirp->hash_order()) {
    if (dirp->offset > offset) {
      _readdir_drop_dirp_buffer(dirp);
      dirp->reset();
    }
  } else {
    if (offset == 0 ||
	dirp->buffer_frag != frag_t(dir_result_t::fpos_high(offset)) ||
	dirp->offset_low() > dir_result_t::fpos_low(offset))  {
      _readdir_drop_dirp_buffer(dirp);
      dirp->reset();
    }
  }

  dirp->offset = offset;
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
  frag_t fg = dirp->buffer_frag;

  if (fg.is_rightmost()) {
    ldout(cct, 10) << "_readdir_next_frag advance from " << fg << " to END" << dendl;
    dirp->set_end();
    return;
  }

  // advance
  fg = fg.next();
  ldout(cct, 10) << "_readdir_next_frag advance from " << dirp->buffer_frag << " to " << fg << dendl;

  if (dirp->hash_order()) {
    // keep last_name
    int64_t new_offset = dir_result_t::make_fpos(fg.value(), 2, true);
    if (dirp->offset < new_offset) // don't decrease offset
      dirp->offset = new_offset;
  } else {
    dirp->last_name.clear();
    dirp->offset = dir_result_t::make_fpos(fg, 2, false);
    _readdir_rechoose_frag(dirp);
  }
}

void Client::_readdir_rechoose_frag(dir_result_t *dirp)
{
  assert(dirp->inode);

  if (dirp->hash_order())
    return;

  frag_t cur = frag_t(dirp->offset_high());
  frag_t fg = dirp->inode->dirfragtree[cur.value()];
  if (fg != cur) {
    ldout(cct, 10) << "_readdir_rechoose_frag frag " << cur << " maps to " << fg << dendl;
    dirp->offset = dir_result_t::make_fpos(fg, 2, false);
    dirp->last_name.clear();
    dirp->next_offset = 2;
  }
}

void Client::_readdir_drop_dirp_buffer(dir_result_t *dirp)
{
  ldout(cct, 10) << "_readdir_drop_dirp_buffer " << dirp << dendl;
  dirp->buffer.clear();
}

int Client::_readdir_get_frag(dir_result_t *dirp)
{
  assert(dirp);
  assert(dirp->inode);

  // get the current frag.
  frag_t fg;
  if (dirp->hash_order())
    fg = dirp->inode->dirfragtree[dirp->offset_high()];
  else
    fg = frag_t(dirp->offset_high());
  
  ldout(cct, 10) << "_readdir_get_frag " << dirp << " on " << dirp->inode->ino << " fg " << fg
		 << " offset " << hex << dirp->offset << dec << dendl;

  int op = CEPH_MDS_OP_READDIR;
  if (dirp->inode && dirp->inode->snapid == CEPH_SNAPDIR)
    op = CEPH_MDS_OP_LSSNAP;

  InodeRef& diri = dirp->inode;

  MetaRequest *req = new MetaRequest(op);
  filepath path;
  diri->make_nosnap_relative_path(path);
  req->set_filepath(path); 
  req->set_inode(diri.get());
  req->head.args.readdir.frag = fg;
  req->head.args.readdir.flags = CEPH_READDIR_REPLY_BITFLAGS;
  if (dirp->last_name.length()) {
    req->path2.set_path(dirp->last_name.c_str());
  }
  req->dirp = dirp;
  
  bufferlist dirbl;
  int res = make_request(req, dirp->owner_uid, dirp->owner_gid, NULL, NULL, -1, &dirbl);
  
  if (res == -EAGAIN) {
    ldout(cct, 10) << "_readdir_get_frag got EAGAIN, retrying" << dendl;
    _readdir_rechoose_frag(dirp);
    return _readdir_get_frag(dirp);
  }

  if (res == 0) {
    ldout(cct, 10) << "_readdir_get_frag " << dirp << " got frag " << dirp->buffer_frag
		   << " size " << dirp->buffer.size() << dendl;
  } else {
    ldout(cct, 10) << "_readdir_get_frag got error " << res << ", setting end flag" << dendl;
    dirp->set_end();
  }

  return res;
}

struct dentry_off_lt {
  bool operator()(const Dentry* dn, int64_t off) const {
    return dir_result_t::fpos_cmp(dn->offset, off) < 0;
  }
};

int Client::_readdir_cache_cb(dir_result_t *dirp, add_dirent_cb_t cb, void *p)
{
  assert(client_lock.is_locked());
  ldout(cct, 10) << "_readdir_cache_cb " << dirp << " on " << dirp->inode->ino
	   << " last_name " << dirp->last_name << " offset " << hex << dirp->offset << dec
	   << dendl;
  Dir *dir = dirp->inode->dir;

  if (!dir) {
    ldout(cct, 10) << " dir is empty" << dendl;
    dirp->set_end();
    return 0;
  }

  vector<Dentry*>::iterator pd = std::lower_bound(dir->readdir_cache.begin(),
						  dir->readdir_cache.end(),
						  dirp->offset, dentry_off_lt());

  string dn_name;
  while (true) {
    if (!dirp->inode->is_complete_and_ordered())
      return -EAGAIN;
    if (pd == dir->readdir_cache.end())
      break;
    Dentry *dn = *pd;
    if (dn->inode == NULL) {
      ldout(cct, 15) << " skipping null '" << dn->name << "'" << dendl;
      ++pd;
      continue;
    }
    if (dn->cap_shared_gen != dir->parent_inode->shared_gen) {
      ldout(cct, 15) << " skipping mismatch shared gen '" << dn->name << "'" << dendl;
      ++pd;
      continue;
    }

    struct stat st;
    struct dirent de;
    int stmask = fill_stat(dn->inode, &st);

    uint64_t next_off = dn->offset + 1;
    ++pd;
    if (pd == dir->readdir_cache.end())
      next_off = dir_result_t::END;

    fill_dirent(&de, dn->name.c_str(), st.st_mode, st.st_ino, next_off);

    dn_name = dn->name; // fill in name while we have lock

    client_lock.Unlock();
    int r = cb(p, &de, &st, stmask, next_off);  // _next_ offset
    client_lock.Lock();
    ldout(cct, 15) << " de " << de.d_name << " off " << hex << dn->offset << dec
		   << " = " << r << dendl;
    if (r < 0) {
      return r;
    }

    dirp->offset = next_off;
    if (dirp->at_end())
      dirp->next_offset = 2;
    else
      dirp->next_offset = dirp->offset_low();
    dirp->last_name = dn_name; // we successfully returned this one; update!
    if (r > 0)
      return r;
  }

  ldout(cct, 10) << "_readdir_cache_cb " << dirp << " on " << dirp->inode->ino << " at end" << dendl;
  dirp->set_end();
  return 0;
}

int Client::readdir_r_cb(dir_result_t *d, add_dirent_cb_t cb, void *p)
{
  Mutex::Locker lock(client_lock);

  dir_result_t *dirp = static_cast<dir_result_t*>(d);

  ldout(cct, 10) << "readdir_r_cb " << *dirp->inode << " offset " << hex << dirp->offset
		 << dec << " at_end=" << dirp->at_end()
		 << " hash_order=" << dirp->hash_order() << dendl;

  struct dirent de;
  struct stat st;
  memset(&de, 0, sizeof(de));
  memset(&st, 0, sizeof(st));

  InodeRef& diri = dirp->inode;

  if (dirp->at_end())
    return 0;

  if (dirp->offset == 0) {
    ldout(cct, 15) << " including ." << dendl;
    assert(diri->dn_set.size() < 2); // can't have multiple hard-links to a dir
    uint64_t next_off = 1;

    fill_stat(diri, &st);
    fill_dirent(&de, ".", S_IFDIR, st.st_ino, next_off);

    client_lock.Unlock();
    int r = cb(p, &de, &st, -1, next_off);
    client_lock.Lock();
    if (r < 0)
      return r;

    dirp->offset = next_off;
    if (r > 0)
      return r;
  }
  if (dirp->offset == 1) {
    ldout(cct, 15) << " including .." << dendl;
    uint64_t next_off = 2;
    InodeRef in;
    if (diri->dn_set.empty())
      in = diri;
    else
      in = diri->get_first_parent()->inode;

    fill_stat(in, &st);
    fill_dirent(&de, "..", S_IFDIR, st.st_ino, next_off);

    client_lock.Unlock();
    int r = cb(p, &de, &st, -1, next_off);
    client_lock.Lock();
    if (r < 0)
      return r;

    dirp->offset = next_off;
    if (r > 0)
      return r;
  }

  // can we read from our cache?
  ldout(cct, 10) << "offset " << hex << dirp->offset << dec
	   << " snapid " << dirp->inode->snapid << " (complete && ordered) "
	   << dirp->inode->is_complete_and_ordered()
	   << " issued " << ccap_string(dirp->inode->caps_issued())
	   << dendl;
  if (dirp->inode->snapid != CEPH_SNAPDIR &&
      dirp->inode->is_complete_and_ordered() &&
      dirp->inode->caps_issued_mask(CEPH_CAP_FILE_SHARED)) {
    int err = _readdir_cache_cb(dirp, cb, p);
    if (err != -EAGAIN)
      return err;
  }

  while (1) {
    if (dirp->at_end())
      return 0;

    if (!dirp->is_cached()) {
      int r = _readdir_get_frag(dirp);
      if (r)
	return r;
      // _readdir_get_frag () may updates dirp->offset if the replied dirfrag is
      // different than the requested one. (our dirfragtree was outdated)
    }
    frag_t fg = dirp->buffer_frag;

    ldout(cct, 10) << "frag " << fg << " buffer size " << dirp->buffer.size()
		   << " offset " << hex << dirp->offset << dendl;

    for (auto it = std::lower_bound(dirp->buffer.begin(), dirp->buffer.end(),
				    dirp->offset, dir_result_t::dentry_off_lt());
	 it != dirp->buffer.end();
	 ++it) {
      dir_result_t::dentry &entry = *it;

      uint64_t next_off = entry.offset + 1;
      int stmask = fill_stat(entry.inode, &st);
      fill_dirent(&de, entry.name.c_str(), st.st_mode, st.st_ino, next_off);

      client_lock.Unlock();
      int r = cb(p, &de, &st, stmask, next_off);  // _next_ offset
      client_lock.Lock();

      ldout(cct, 15) << " de " << de.d_name << " off " << hex << next_off - 1 << dec
		     << " = " << r << dendl;
      if (r < 0)
	return r;

      dirp->offset = next_off;
      if (r > 0)
	return r;
    }

    if (dirp->next_offset > 2) {
      ldout(cct, 10) << " fetching next chunk of this frag" << dendl;
      _readdir_drop_dirp_buffer(dirp);
      continue;  // more!
    }

    if (!fg.is_rightmost()) {
      // next frag!
      _readdir_next_frag(dirp);
      continue;
    }

    if (diri->shared_gen == dirp->start_shared_gen &&
	diri->dir_release_count == dirp->release_count) {
      if (diri->dir_ordered_count == dirp->ordered_count) {
	ldout(cct, 10) << " marking (I_COMPLETE|I_DIR_ORDERED) on " << *diri << dendl;
	if (diri->dir) {
	  assert(diri->dir->readdir_cache.size() >= dirp->cache_index);
	  diri->dir->readdir_cache.resize(dirp->cache_index);
	}
	diri->flags |= I_COMPLETE | I_DIR_ORDERED;
      } else {
	ldout(cct, 10) << " marking I_COMPLETE on " << *diri << dendl;
	diri->flags |= I_COMPLETE;
      }
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
  return 1;
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

  uid_t uid = get_uid();
  gid_t gid = get_gid();

  Fh *fh = NULL;

#if defined(__linux__) && defined(O_PATH)
  /* When the O_PATH is being specified, others flags than O_DIRECTORY
   * and O_NOFOLLOW are ignored. Please refer do_entry_open() function
   * in kernel (fs/open.c). */
  if (flags & O_PATH)
    flags &= O_DIRECTORY | O_NOFOLLOW | O_PATH;
#endif

  filepath path(relpath);
  InodeRef in;
  bool created = false;
  /* O_CREATE with O_EXCL enforces O_NOFOLLOW. */
  bool followsym = !((flags & O_NOFOLLOW) || ((flags & O_CREAT) && (flags & O_EXCL)));
  int r = path_walk(path, &in, followsym, ceph_caps_for_mode(mode), uid, gid);

  if (r == 0 && (flags & O_CREAT) && (flags & O_EXCL))
    return -EEXIST;

#if defined(__linux__) && defined(O_PATH)
  if (r == 0 && in->is_symlink() && (flags & O_NOFOLLOW) && !(flags & O_PATH))
#else
  if (r == 0 && in->is_symlink() && (flags & O_NOFOLLOW))
#endif
    return -ELOOP;

  if (r == -ENOENT && (flags & O_CREAT)) {
    filepath dirpath = path;
    string dname = dirpath.last_dentry();
    dirpath.pop_dentry();
    InodeRef dir;
    r = path_walk(dirpath, &dir, true, 0, uid, gid);
    if (r < 0)
      goto out;
    if (cct->_conf->client_permissions) {
      r = may_create(dir.get(), uid, gid);
      if (r < 0)
	goto out;
    }
    r = _create(dir.get(), dname.c_str(), flags, mode, &in, &fh, stripe_unit,
                stripe_count, object_size, data_pool, &created, uid, gid);
  }
  if (r < 0)
    goto out;

  if (!created) {
    // posix says we can only check permissions of existing files
    if (cct->_conf->client_permissions) {
      r = may_open(in.get(), flags, uid, gid);
      if (r < 0)
	goto out;
    }
  }

  if (!fh)
    r = _open(in.get(), flags, mode, &fh, uid, gid);
  if (r >= 0) {
    // allocate a integer file descriptor
    assert(fh);
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


/**
 * Load inode into local cache.
 *
 * If inode pointer is non-NULL, and take a reference on
 * the resulting Inode object in one operation, so that caller
 * can safely assume inode will still be there after return.
 */
int Client::lookup_ino(inodeno_t ino, Inode **inode)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "lookup_ino enter(" << ino << ") = " << dendl;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPINO);
  filepath path(ino);
  req->set_filepath(path);

  int r = make_request(req, -1, -1, NULL, NULL, rand() % mdsmap->get_num_in_mds());
  if (r == 0 && inode != NULL) {
    vinodeno_t vino(ino, CEPH_NOSNAP);
    unordered_map<vinodeno_t,Inode*>::iterator p = inode_map.find(vino);
    assert(p != inode_map.end());
    *inode = p->second;
    _ll_get(*inode);
  }
  ldout(cct, 3) << "lookup_ino exit(" << ino << ") = " << r << dendl;
  return r;
}



/**
 * Find the parent inode of `ino` and insert it into
 * our cache.  Conditionally also set `parent` to a referenced
 * Inode* if caller provides non-NULL value.
 */
int Client::lookup_parent(Inode *ino, Inode **parent)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "lookup_parent enter(" << ino->ino << ") = " << dendl;

  if (!ino->dn_set.empty()) {
    ldout(cct, 3) << "lookup_parent dentry already present" << dendl;
    return 0;
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPPARENT);
  filepath path(ino->ino);
  req->set_filepath(path);
  req->set_inode(ino);

  InodeRef target;
  int r = make_request(req, -1, -1, &target, NULL, rand() % mdsmap->get_num_in_mds());
  // Give caller a reference to the parent ino if they provided a pointer.
  if (parent != NULL) {
    if (r == 0) {
      *parent = target.get();
      _ll_get(*parent);
      ldout(cct, 3) << "lookup_parent found parent " << (*parent)->ino << dendl;
    } else {
      *parent = NULL;
    }
  }
  ldout(cct, 3) << "lookup_parent exit(" << ino->ino << ") = " << r << dendl;
  return r;
}


/**
 * Populate the parent dentry for `ino`, provided it is
 * a child of `parent`.
 */
int Client::lookup_name(Inode *ino, Inode *parent)
{
  assert(parent->is_dir());

  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "lookup_name enter(" << ino->ino << ") = " << dendl;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPNAME);
  req->set_filepath2(filepath(parent->ino));
  req->set_filepath(filepath(ino->ino));
  req->set_inode(ino);

  int r = make_request(req, -1, -1, NULL, NULL, rand() % mdsmap->get_num_in_mds());
  ldout(cct, 3) << "lookup_name exit(" << ino->ino << ") = " << r << dendl;
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

  ldout(cct, 10) << "_create_fh " << in->ino << " mode " << cmode << dendl;

  if (in->snapid != CEPH_NOSNAP) {
    in->snap_cap_refs++;
    ldout(cct, 5) << "open success, fh is " << f << " combined IMMUTABLE SNAP caps " 
	    << ccap_string(in->caps_issued()) << dendl;
  }

  const md_config_t *conf = cct->_conf;
  f->readahead.set_trigger_requests(1);
  f->readahead.set_min_readahead_size(conf->client_readahead_min);
  uint64_t max_readahead = Readahead::NO_LIMIT;
  if (conf->client_readahead_max_bytes) {
    max_readahead = MIN(max_readahead, (uint64_t)conf->client_readahead_max_bytes);
  }
  if (conf->client_readahead_max_periods) {
    max_readahead = MIN(max_readahead, in->layout.get_period()*(uint64_t)conf->client_readahead_max_periods);
  }
  f->readahead.set_max_readahead_size(max_readahead);
  vector<uint64_t> alignments;
  alignments.push_back(in->layout.get_period());
  alignments.push_back(in->layout.stripe_unit);
  f->readahead.set_alignments(alignments);

  return f;
}

int Client::_release_fh(Fh *f)
{
  //ldout(cct, 3) << "op: client->close(open_files[ " << fh << " ]);" << dendl;
  //ldout(cct, 3) << "op: open_files.erase( " << fh << " );" << dendl;
  Inode *in = f->inode.get();
  ldout(cct, 5) << "_release_fh " << f << " mode " << f->mode << " on " << *in << dendl;

  if (in->snapid == CEPH_NOSNAP) {
    if (in->put_open_ref(f->mode)) {
      _flush(in, new C_Client_FlushComplete(this, in));
      check_caps(in, false);
    }
  } else {
    assert(in->snap_cap_refs > 0);
    in->snap_cap_refs--;
  }

  _release_filelocks(f);

  // Finally, read any async err (i.e. from flushes) from the inode
  int err = in->async_err;
  if (err != 0) {
    ldout(cct, 1) << "_release_fh " << f << " on inode " << *in << " caught async_err = "
                  << cpp_strerror(err) << dendl;
  } else {
    ldout(cct, 10) << "_release_fh " << f << " on inode " << *in << " no async_err state" << dendl;
  }

  _put_fh(f);

  return err;
}

void Client::_put_fh(Fh *f)
{
  int left = f->put();
  if (!left)
    delete f;
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
    if (cct->_conf->client_debug_getattr_caps)
      req->head.args.open.mask = DEBUG_GETATTR_CAPS;
    else
      req->head.args.open.mask = 0;
    req->head.args.open.old_size = in->size;   // for O_TRUNC
    req->set_inode(in);
    result = make_request(req, uid, gid);
  }

  // success?
  if (result >= 0) {
    if (fhp)
      *fhp = _create_fh(in, flags, cmode);
  } else {
    in->put_open_ref(cmode);
  }

  trim_cache();

  return result;
}

int Client::_renew_caps(Inode *in)
{
  int wanted = in->caps_file_wanted();
  if (in->is_any_caps() &&
      ((wanted & CEPH_CAP_ANY_WR) == 0 || in->auth_cap)) {
    check_caps(in, true);
    return 0;
  }

  int flags = 0;
  if ((wanted & CEPH_CAP_FILE_RD) && (wanted & CEPH_CAP_FILE_WR))
    flags = O_RDWR;
  else if (wanted & CEPH_CAP_FILE_RD)
    flags = O_RDONLY;
  else if (wanted & CEPH_CAP_FILE_WR)
    flags = O_WRONLY;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_OPEN);
  filepath path;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->head.args.open.flags = flags;
  req->head.args.open.pool = -1;
  if (cct->_conf->client_debug_getattr_caps)
    req->head.args.open.mask = DEBUG_GETATTR_CAPS;
  else
    req->head.args.open.mask = 0;
  req->set_inode(in);

  int ret = make_request(req, -1, -1);
  return ret;
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
  int err = _release_fh(fh);
  fd_map.erase(fd);
  put_fd(fd);
  ldout(cct, 3) << "close exit(" << fd << ")" << dendl;
  return err;
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
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  return _lseek(f, offset, whence);
}

loff_t Client::_lseek(Fh *f, loff_t offset, int whence)
{
  Inode *in = f->inode.get();
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

int Client::uninline_data(Inode *in, Context *onfinish)
{
  if (!in->inline_data.length()) {
    onfinish->complete(0);
    return 0;
  }

  char oid_buf[32];
  snprintf(oid_buf, sizeof(oid_buf), "%llx.00000000", (long long unsigned)in->ino);
  object_t oid = oid_buf;

  ObjectOperation create_ops;
  create_ops.create(false);

  objecter->mutate(oid,
		   OSDMap::file_to_object_locator(in->layout),
		   create_ops,
		   in->snaprealm->get_snap_context(),
		   ceph::real_clock::now(cct),
		   0,
		   NULL,
		   NULL);

  bufferlist inline_version_bl;
  ::encode(in->inline_version, inline_version_bl);

  ObjectOperation uninline_ops;
  uninline_ops.cmpxattr("inline_version",
                        CEPH_OSD_CMPXATTR_OP_GT,
                        CEPH_OSD_CMPXATTR_MODE_U64,
                        inline_version_bl);
  bufferlist inline_data = in->inline_data;
  uninline_ops.write(0, inline_data, in->truncate_size, in->truncate_seq);
  uninline_ops.setxattr("inline_version", stringify(in->inline_version));

  objecter->mutate(oid,
		   OSDMap::file_to_object_locator(in->layout),
		   uninline_ops,
		   in->snaprealm->get_snap_context(),
		   ceph::real_clock::now(cct),
		   0,
		   NULL,
		   onfinish);

  return 0;
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
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  bufferlist bl;
  int r = _read(f, offset, size, &bl);
  ldout(cct, 3) << "read(" << fd << ", " << (void*)buf << ", " << size << ", " << offset << ") = " << r << dendl;
  if (r >= 0) {
    bl.copy(0, bl.length(), buf);
    r = bl.length();
  }
  return r;
}

int Client::preadv(int fd, const struct iovec *iov, int iovcnt, loff_t offset)
{
  if (iovcnt < 0)
    return -EINVAL;
  return _preadv_pwritev(fd, iov, iovcnt, offset, false);
}

int Client::_read(Fh *f, int64_t offset, uint64_t size, bufferlist *bl)
{
  const md_config_t *conf = cct->_conf;
  Inode *in = f->inode.get();

  if ((f->mode & CEPH_FILE_MODE_RD) == 0)
    return -EBADF;
  //bool lazy = f->mode == CEPH_FILE_MODE_LAZY;

  bool movepos = false;
  if (offset < 0) {
    lock_fh_pos(f);
    offset = f->pos;
    movepos = true;
  }
  loff_t start_pos = offset;

  if (in->inline_version == 0) {
    int r = _getattr(in, CEPH_STAT_CAP_INLINE_DATA, -1, -1, true);
    if (r < 0)
      return r;
    assert(in->inline_version > 0);
  }

retry:
  int have;
  int r = get_caps(in, CEPH_CAP_FILE_RD, CEPH_CAP_FILE_CACHE, &have, -1);
  if (r < 0)
    return r;

  if (f->flags & O_DIRECT)
    have &= ~CEPH_CAP_FILE_CACHE;

  Mutex uninline_flock("Clinet::_read_uninline_data flock");
  Cond uninline_cond;
  bool uninline_done = false;
  int uninline_ret = 0;
  Context *onuninline = NULL;

  if (in->inline_version < CEPH_INLINE_NONE) {
    if (!(have & CEPH_CAP_FILE_CACHE)) {
      onuninline = new C_SafeCond(&uninline_flock,
                                  &uninline_cond,
                                  &uninline_done,
                                  &uninline_ret);
      uninline_data(in, onuninline);
    } else {
      uint32_t len = in->inline_data.length();

      uint64_t endoff = offset + size;
      if (endoff > in->size)
        endoff = in->size;

      if (offset < len) {
        if (endoff <= len) {
          bl->substr_of(in->inline_data, offset, endoff - offset);
        } else {
          bl->substr_of(in->inline_data, offset, len - offset);
          bl->append_zero(endoff - len);
        }
      } else if ((uint64_t)offset < endoff) {
        bl->append_zero(endoff - offset);
      }

      goto success;
    }
  }

  if (!conf->client_debug_force_sync_read &&
      (conf->client_oc && (have & CEPH_CAP_FILE_CACHE))) {

    if (f->flags & O_RSYNC) {
      _flush_range(in, offset, size);
    }
    r = _read_async(f, offset, size, bl);
    if (r < 0)
      goto done;
  } else {
    if (f->flags & O_DIRECT)
      _flush_range(in, offset, size);

    bool checkeof = false;
    r = _read_sync(f, offset, size, bl, &checkeof);
    if (r < 0)
      goto done;
    if (checkeof) {
      offset += r;
      size -= r;

      put_cap_ref(in, CEPH_CAP_FILE_RD);
      have = 0;
      // reverify size
      r = _getattr(in, CEPH_STAT_CAP_SIZE);
      if (r < 0)
	goto done;

      // eof?  short read.
      if ((uint64_t)offset < in->size)
	goto retry;
    }
  }

success:
  if (movepos) {
    // adjust fd pos
    f->pos = start_pos + bl->length();
    unlock_fh_pos(f);
  }

done:
  // done!

  if (onuninline) {
    client_lock.Unlock();
    uninline_flock.Lock();
    while (!uninline_done)
      uninline_cond.Wait(uninline_flock);
    uninline_flock.Unlock();
    client_lock.Lock();

    if (uninline_ret >= 0 || uninline_ret == -ECANCELED) {
      in->inline_data.clear();
      in->inline_version = CEPH_INLINE_NONE;
      mark_caps_dirty(in, CEPH_CAP_FILE_WR);
      check_caps(in, false);
    } else
      r = uninline_ret;
  }

  if (have)
    put_cap_ref(in, CEPH_CAP_FILE_RD);
  return r < 0 ? r : bl->length();
}

Client::C_Readahead::C_Readahead(Client *c, Fh *f) :
    client(c), f(f) {
  f->get();
  f->readahead.inc_pending();
}

Client::C_Readahead::~C_Readahead() {
  f->readahead.dec_pending();
  client->_put_fh(f);
}

void Client::C_Readahead::finish(int r) {
  lgeneric_subdout(client->cct, client, 20) << "client." << client->get_nodeid() << " " << "C_Readahead on " << f->inode << dendl;
  client->put_cap_ref(f->inode.get(), CEPH_CAP_FILE_RD | CEPH_CAP_FILE_CACHE);
}

int Client::_read_async(Fh *f, uint64_t off, uint64_t len, bufferlist *bl)
{
  const md_config_t *conf = cct->_conf;
  Inode *in = f->inode.get();

  ldout(cct, 10) << "_read_async " << *in << " " << off << "~" << len << dendl;

  // trim read based on file size?
  if (off >= in->size)
    return 0;
  if (len == 0)
    return 0;
  if (off + len > in->size) {
    len = in->size - off;    
  }

  ldout(cct, 10) << " min_bytes=" << f->readahead.get_min_readahead_size()
                 << " max_bytes=" << f->readahead.get_max_readahead_size()
                 << " max_periods=" << conf->client_readahead_max_periods << dendl;

  // read (and possibly block)
  int r, rvalue = 0;
  Mutex flock("Client::_read_async flock");
  Cond cond;
  bool done = false;
  Context *onfinish = new C_SafeCond(&flock, &cond, &done, &rvalue);
  r = objectcacher->file_read(&in->oset, &in->layout, in->snapid,
			      off, len, bl, 0, onfinish);
  if (r == 0) {
    get_cap_ref(in, CEPH_CAP_FILE_CACHE);
    client_lock.Unlock();
    flock.Lock();
    while (!done)
      cond.Wait(flock);
    flock.Unlock();
    client_lock.Lock();
    put_cap_ref(in, CEPH_CAP_FILE_CACHE);
    r = rvalue;
  } else {
    // it was cached.
    delete onfinish;
  }

  if(f->readahead.get_min_readahead_size() > 0) {
    pair<uint64_t, uint64_t> readahead_extent = f->readahead.update(off, len, in->size);
    if (readahead_extent.second > 0) {
      ldout(cct, 20) << "readahead " << readahead_extent.first << "~" << readahead_extent.second
		     << " (caller wants " << off << "~" << len << ")" << dendl;
      Context *onfinish2 = new C_Readahead(this, f);
      int r2 = objectcacher->file_read(&in->oset, &in->layout, in->snapid,
				       readahead_extent.first, readahead_extent.second,
				       NULL, 0, onfinish2);
      if (r2 == 0) {
	ldout(cct, 20) << "readahead initiated, c " << onfinish2 << dendl;
	get_cap_ref(in, CEPH_CAP_FILE_RD | CEPH_CAP_FILE_CACHE);
      } else {
	ldout(cct, 20) << "readahead was no-op, already cached" << dendl;
	delete onfinish2;
      }
    }
  }

  return r;
}

int Client::_read_sync(Fh *f, uint64_t off, uint64_t len, bufferlist *bl,
		       bool *checkeof)
{
  Inode *in = f->inode.get();
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
	int64_t some = in->size - pos;
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

      *checkeof = true;
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
  InodeRef in;
public:
  C_Client_SyncCommit(Client *c, Inode *i) : cl(c), in(i) {}
  void finish(int) {
    // Called back by Filter, then Client is responsible for taking its own lock
    assert(!cl->client_lock.is_locked_by_me()); 
    cl->sync_write_commit(in);
  }
};

void Client::sync_write_commit(InodeRef& in)
{
  Mutex::Locker l(client_lock);

  assert(unsafe_sync_write > 0);
  unsafe_sync_write--;

  put_cap_ref(in.get(), CEPH_CAP_FILE_BUFFER);

  ldout(cct, 15) << "sync_write_commit unsafe_sync_write = " << unsafe_sync_write << dendl;
  if (unsafe_sync_write == 0 && unmounting) {
    ldout(cct, 10) << "sync_write_commit -- no more unsafe writes, unmount can proceed" << dendl;
    mount_cond.Signal();
  }

  in.reset(); // put inode inside client_lock
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
#if defined(__linux__) && defined(O_PATH)
  if (fh->flags & O_PATH)
    return -EBADF;
#endif
  int r = _write(fh, offset, size, buf, NULL, 0);
  ldout(cct, 3) << "write(" << fd << ", \"...\", " << size << ", " << offset << ") = " << r << dendl;
  return r;
}

int Client::pwritev(int fd, const struct iovec *iov, int iovcnt, int64_t offset)
{
  if (iovcnt < 0)
    return -EINVAL;
  return _preadv_pwritev(fd, iov, iovcnt, offset, true);
}

int Client::_preadv_pwritev(int fd, const struct iovec *iov, unsigned iovcnt, int64_t offset, bool write)
{
    Mutex::Locker lock(client_lock);
    tout(cct) << fd << std::endl;
    tout(cct) << offset << std::endl;

    Fh *fh = get_filehandle(fd);
    if (!fh)
        return -EBADF;
#if defined(__linux__) && defined(O_PATH)
    if (fh->flags & O_PATH)
        return -EBADF;
#endif
    loff_t totallen = 0;
    for (unsigned i = 0; i < iovcnt; i++) {
        totallen += iov[i].iov_len;
    }
    if (write) {
        int w = _write(fh, offset, totallen, NULL, iov, iovcnt);
        ldout(cct, 3) << "pwritev(" << fd << ", \"...\", " << totallen << ", " << offset << ") = " << w << dendl;
        return w;
    } else {
        bufferlist bl;
        int r = _read(fh, offset, totallen, &bl);
        ldout(cct, 3) << "preadv(" << fd << ", " <<  offset << ") = " << r << dendl;
        if (r <= 0)
          return r;

        int bufoff = 0;
        for (unsigned j = 0, resid = r; j < iovcnt && resid > 0; j++) {
               /*
                * This piece of code aims to handle the case that bufferlist does not have enough data 
                * to fill in the iov 
                */
               if (resid < iov[j].iov_len) {
                    bl.copy(bufoff, resid, (char *)iov[j].iov_base);
                    break;
               } else {
                    bl.copy(bufoff, iov[j].iov_len, (char *)iov[j].iov_base);
               }
               resid -= iov[j].iov_len;
               bufoff += iov[j].iov_len;
        }
        return r;  
    }
}

int Client::_write(Fh *f, int64_t offset, uint64_t size, const char *buf,
                  const struct iovec *iov, int iovcnt)
{
  if ((uint64_t)(offset+size) > mdsmap->get_max_filesize()) //too large!
    return -EFBIG;

  //ldout(cct, 7) << "write fh " << fh << " size " << size << " offset " << offset << dendl;
  Inode *in = f->inode.get();

  if (objecter->osdmap_pool_full(in->layout.pool_id)) {
    return -ENOSPC;
  }

  assert(in->snapid == CEPH_NOSNAP);

  // was Fh opened as writeable?
  if ((f->mode & CEPH_FILE_MODE_WR) == 0)
    return -EBADF;

  // check quota
  uint64_t endoff = offset + size;
  if (endoff > in->size && is_quota_bytes_exceeded(in, endoff - in->size))
    return -EDQUOT;

  // use/adjust fd pos?
  if (offset < 0) {
    lock_fh_pos(f);
    /*
     * FIXME: this is racy in that we may block _after_ this point waiting for caps, and size may
     * change out from under us.
     */
    if (f->flags & O_APPEND) {
      int r = _lseek(f, 0, SEEK_END);
      if (r < 0) {
        unlock_fh_pos(f);
        return r;
      }
    }
    offset = f->pos;
    f->pos = offset+size;
    unlock_fh_pos(f);
  }

  //bool lazy = f->mode == CEPH_FILE_MODE_LAZY;

  ldout(cct, 10) << "cur file size is " << in->size << dendl;

  // time it.
  utime_t start = ceph_clock_now(cct);

  if (in->inline_version == 0) {
    int r = _getattr(in, CEPH_STAT_CAP_INLINE_DATA, -1, -1, true);
    if (r < 0)
      return r;
    assert(in->inline_version > 0);
  }

  // copy into fresh buffer (since our write may be resub, async)
  bufferlist bl;
  bufferptr *bparr = NULL;
  if (buf) {
      bufferptr bp;
      if (size > 0) bp = buffer::copy(buf, size);
      bl.push_back( bp );
  } else if (iov){
      //iov case 
      bparr = new bufferptr[iovcnt];
      for (int i = 0; i < iovcnt; i++) {
        if (iov[i].iov_len > 0) {
            bparr[i] = buffer::copy((char*)iov[i].iov_base, iov[i].iov_len);
        }
        bl.push_back( bparr[i] );
      }
  }

  utime_t lat;
  uint64_t totalwritten;
  int have;
  int r = get_caps(in, CEPH_CAP_FILE_WR, CEPH_CAP_FILE_BUFFER, &have, endoff);
  if (r < 0) {
    delete[] bparr;
    return r;
  }

  if (f->flags & O_DIRECT)
    have &= ~CEPH_CAP_FILE_BUFFER;

  ldout(cct, 10) << " snaprealm " << *in->snaprealm << dendl;

  Mutex uninline_flock("Clinet::_write_uninline_data flock");
  Cond uninline_cond;
  bool uninline_done = false;
  int uninline_ret = 0;
  Context *onuninline = NULL;

  if (in->inline_version < CEPH_INLINE_NONE) {
    if (endoff > cct->_conf->client_max_inline_size ||
        endoff > CEPH_INLINE_MAX_SIZE ||
        !(have & CEPH_CAP_FILE_BUFFER)) {
      onuninline = new C_SafeCond(&uninline_flock,
                                  &uninline_cond,
                                  &uninline_done,
                                  &uninline_ret);
      uninline_data(in, onuninline);
    } else {
      get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

      uint32_t len = in->inline_data.length();

      if (endoff < len)
        in->inline_data.copy(endoff, len - endoff, bl);

      if (offset < len)
        in->inline_data.splice(offset, len - offset);
      else if (offset > len)
        in->inline_data.append_zero(offset - len);

      in->inline_data.append(bl);
      in->inline_version++;

      put_cap_ref(in, CEPH_CAP_FILE_BUFFER);

      goto success;
    }
  }

  if (cct->_conf->client_oc && (have & CEPH_CAP_FILE_BUFFER)) {
    // do buffered write
    if (!in->oset.dirty_or_tx)
      get_cap_ref(in, CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER);

    get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

    // async, caching, non-blocking.
    r = objectcacher->file_write(&in->oset, &in->layout,
				 in->snaprealm->get_snap_context(),
				 offset, size, bl, ceph::real_clock::now(cct),
				 0);
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
    if (f->flags & O_DIRECT)
      _flush_range(in, offset, size);

    // simple, non-atomic sync write
    Mutex flock("Client::_write flock");
    Cond cond;
    bool done = false;
    Context *onfinish = new C_SafeCond(&flock, &cond, &done);
    Context *onsafe = new C_Client_SyncCommit(this, in);

    unsafe_sync_write++;
    get_cap_ref(in, CEPH_CAP_FILE_BUFFER);  // released by onsafe callback

    filer->write_trunc(in->ino, &in->layout, in->snaprealm->get_snap_context(),
			   offset, size, bl, ceph::real_clock::now(cct), 0,
			   in->truncate_size, in->truncate_seq,
			   onfinish, new C_OnFinisher(onsafe, &objecter_finisher));
    client_lock.Unlock();
    flock.Lock();

    while (!done)
      cond.Wait(flock);
    flock.Unlock();
    client_lock.Lock();
  }

  // if we get here, write was successful, update client metadata
success:
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

    if (is_quota_bytes_approaching(in)) {
      check_caps(in, true);
    } else {
      if ((in->size << 1) >= in->max_size &&
          (in->reported_size << 1) < in->max_size)
        check_caps(in, false);
    }

    ldout(cct, 7) << "wrote to " << totalwritten+offset << ", extending file size" << dendl;
  } else {
    ldout(cct, 7) << "wrote to " << totalwritten+offset << ", leaving file size at " << in->size << dendl;
  }

  // mtime
  in->mtime = ceph_clock_now(cct);
  in->change_attr++;
  mark_caps_dirty(in, CEPH_CAP_FILE_WR);

done:

  if (onuninline) {
    client_lock.Unlock();
    uninline_flock.Lock();
    while (!uninline_done)
      uninline_cond.Wait(uninline_flock);
    uninline_flock.Unlock();
    client_lock.Lock();

    if (uninline_ret >= 0 || uninline_ret == -ECANCELED) {
      in->inline_data.clear();
      in->inline_version = CEPH_INLINE_NONE;
      mark_caps_dirty(in, CEPH_CAP_FILE_WR);
      check_caps(in, false);
    } else
      r = uninline_ret;
  }

  put_cap_ref(in, CEPH_CAP_FILE_WR);
  delete[] bparr; 
  return r;
}

int Client::_flush(Fh *f)
{
  Inode *in = f->inode.get();
  int err = in->async_err;
  if (err != 0) {
    ldout(cct, 1) << __func__ << ": " << f << " on inode " << *in << " caught async_err = "
                  << cpp_strerror(err) << dendl;
  } else {
    ldout(cct, 10) << __func__ << ": " << f << " on inode " << *in << " no async_err state" << dendl;
  }

  return err;
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
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
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
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  int r = _fsync(f, syncdataonly);
  ldout(cct, 3) << "fsync(" << fd << ", " << syncdataonly << ") = " << r << dendl;
  return r;
}

int Client::_fsync(Inode *in, bool syncdataonly)
{
  int r = 0;
  Mutex lock("Client::_fsync::lock");
  Cond cond;
  bool done = false;
  C_SafeCond *object_cacher_completion = NULL;
  ceph_tid_t flush_tid = 0;
  InodeRef tmp_ref;

  ldout(cct, 3) << "_fsync on " << *in << " " << (syncdataonly ? "(dataonly)":"(data+metadata)") << dendl;
  
  if (cct->_conf->client_oc) {
    object_cacher_completion = new C_SafeCond(&lock, &cond, &done, &r);
    tmp_ref = in; // take a reference; C_SafeCond doesn't and _flush won't either
    _flush(in, object_cacher_completion);
    ldout(cct, 15) << "using return-valued form of _fsync" << dendl;
  }
  
  if (!syncdataonly && (in->dirty_caps & ~CEPH_CAP_ANY_FILE_WR)) {
    check_caps(in, true);
    if (in->flushing_caps)
      flush_tid = last_flush_tid;
  } else ldout(cct, 10) << "no metadata needs to commit" << dendl;

  if (!syncdataonly && !in->unsafe_ops.empty()) {
    MetaRequest *req = in->unsafe_ops.back();
    ldout(cct, 15) << "waiting on unsafe requests, last tid " << req->get_tid() <<  dendl;

    req->get();
    wait_on_list(req->waitfor_safe);
    put_request(req);
  }

  if (object_cacher_completion) { // wait on a real reply instead of guessing
    client_lock.Unlock();
    lock.Lock();
    ldout(cct, 15) << "waiting on data to flush" << dendl;
    while (!done)
      cond.Wait(lock);
    lock.Unlock();
    client_lock.Lock();
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
    if (flush_tid > 0)
      wait_sync_caps(in, flush_tid);

    ldout(cct, 10) << "ino " << in->ino << " has no uncommitted writes" << dendl;
  } else {
    ldout(cct, 1) << "ino " << in->ino << " failed to commit to disk! "
		  << cpp_strerror(-r) << dendl;
  }

  if (in->async_err) {
    ldout(cct, 1) << "ino " << in->ino << " marked with error from background flush! "
		  << cpp_strerror(in->async_err) << dendl;
    r = in->async_err;
  }

  return r;
}

int Client::_fsync(Fh *f, bool syncdataonly)
{
  ldout(cct, 3) << "_fsync(" << f << ", " << (syncdataonly ? "dataonly)":"data+metadata)") << dendl;
  return _fsync(f->inode.get(), syncdataonly);
}

int Client::fstat(int fd, struct stat *stbuf, int mask)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "fstat mask " << hex << mask << dec << std::endl;
  tout(cct) << fd << std::endl;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  int r = _getattr(f->inode, mask);
  if (r < 0)
    return r;
  fill_stat(f->inode, stbuf, NULL);
  ldout(cct, 3) << "fstat(" << fd << ", " << stbuf << ") = " << r << dendl;
  return r;
}


// not written yet, but i want to link!

int Client::chdir(const char *relpath, std::string &new_cwd)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "chdir" << std::endl;
  tout(cct) << relpath << std::endl;
  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  if (cwd != in)
    cwd.swap(in);
  ldout(cct, 3) << "chdir(" << relpath << ")  cwd now " << cwd->ino << dendl;

  getcwd(new_cwd);
  return 0;
}

void Client::getcwd(string& dir)
{
  filepath path;
  ldout(cct, 10) << "getcwd " << *cwd << dendl;

  Inode *in = cwd.get();
  while (in != root) {
    assert(in->dn_set.size() < 2); // dirs can't be hard-linked

    // A cwd or ancester is unlinked
    if (in->dn_set.empty()) {
      return;
    }

    Dentry *dn = in->get_first_parent();


    if (!dn) {
      // look it up
      ldout(cct, 10) << "getcwd looking up parent for " << *in << dendl;
      MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPNAME);
      filepath path(in->ino);
      req->set_filepath(path);
      req->set_inode(in);
      int res = make_request(req, -1, -1);
      if (res < 0)
	break;

      // start over
      path = filepath();
      in = cwd.get();
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
  C_SaferCond cond;
  objecter->get_fs_stats(stats, &cond);

  client_lock.Unlock();
  int rval = cond.wait();
  client_lock.Lock();

  if (rval < 0) {
    ldout(cct, 1) << "underlying call to statfs returned error: "
                  << cpp_strerror(rval)
                  << dendl;
    return rval;
  }

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
  stbuf->f_files = stats.num_objects;
  stbuf->f_ffree = -1;
  stbuf->f_favail = -1;
  stbuf->f_fsid = -1;       // ??
  stbuf->f_flag = 0;        // ??
  stbuf->f_namemax = NAME_MAX;

  // Usually quota_root will == root_ancestor, but if the mount root has no
  // quota but we can see a parent of it that does have a quota, we'll
  // respect that one instead.
  assert(root != nullptr);
  Inode *quota_root = root->quota.is_enable() ? root : get_quota_root(root);

  // get_quota_root should always give us something if client quotas are
  // enabled
  assert(cct->_conf->client_quota == false || quota_root != nullptr);

  if (quota_root && cct->_conf->client_quota_df && quota_root->quota.max_bytes) {
    // Special case: if there is a size quota set on the Inode acting
    // as the root for this client mount, then report the quota status
    // as the filesystem statistics.
    const fsblkcnt_t total = quota_root->quota.max_bytes >> CEPH_BLOCK_SHIFT;
    const fsblkcnt_t used = quota_root->rstat.rbytes >> CEPH_BLOCK_SHIFT;
    const fsblkcnt_t free = total - used;


    stbuf->f_blocks = total;
    stbuf->f_bfree = free;
    stbuf->f_bavail = free;
  } else {
    // General case: report the overall RADOS cluster's statistics.  Because
    // multiple pools may be used without one filesystem namespace via
    // layouts, this is the most correct thing we can do.
    stbuf->f_blocks = stats.kb >> (CEPH_BLOCK_SHIFT - 10);
    stbuf->f_bfree = stats.kb_avail >> (CEPH_BLOCK_SHIFT - 10);
    stbuf->f_bavail = stats.kb_avail >> (CEPH_BLOCK_SHIFT - 10);
  }

  return rval;
}

int Client::_do_filelock(Inode *in, Fh *fh, int lock_type, int op, int sleep,
			 struct flock *fl, uint64_t owner)
{
  ldout(cct, 10) << "_do_filelock ino " << in->ino
		 << (lock_type == CEPH_LOCK_FCNTL ? " fcntl" : " flock")
		 << " type " << fl->l_type << " owner " << owner
		 << " " << fl->l_start << "~" << fl->l_len << dendl;

  int lock_cmd;
  if (F_RDLCK == fl->l_type)
    lock_cmd = CEPH_LOCK_SHARED;
  else if (F_WRLCK == fl->l_type)
    lock_cmd = CEPH_LOCK_EXCL;
  else if (F_UNLCK == fl->l_type)
    lock_cmd = CEPH_LOCK_UNLOCK;
  else
    return -EIO;

  if (op != CEPH_MDS_OP_SETFILELOCK || lock_cmd == CEPH_LOCK_UNLOCK)
    sleep = 0;

  /*
   * Set the most significant bit, so that MDS knows the 'owner'
   * is sufficient to identify the owner of lock. (old code uses
   * both 'owner' and 'pid')
   */
  owner |= (1ULL << 63);

  MetaRequest *req = new MetaRequest(op);
  filepath path;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_inode(in);

  req->head.args.filelock_change.rule = lock_type;
  req->head.args.filelock_change.type = lock_cmd;
  req->head.args.filelock_change.owner = owner;
  req->head.args.filelock_change.pid = fl->l_pid;
  req->head.args.filelock_change.start = fl->l_start;
  req->head.args.filelock_change.length = fl->l_len;
  req->head.args.filelock_change.wait = sleep;

  int ret;
  bufferlist bl;

  if (sleep && switch_interrupt_cb) {
    // enable interrupt
    switch_interrupt_cb(callback_handle, req->get());

    ret = make_request(req, -1, -1, NULL, NULL, -1, &bl);

    // disable interrupt
    switch_interrupt_cb(callback_handle, NULL);
    put_request(req);
  } else {
    ret = make_request(req, -1, -1, NULL, NULL, -1, &bl);
  }

  if (ret == 0) {
    if (op == CEPH_MDS_OP_GETFILELOCK) {
      ceph_filelock filelock;
      bufferlist::iterator p = bl.begin();
      ::decode(filelock, p);

      if (CEPH_LOCK_SHARED == filelock.type)
	fl->l_type = F_RDLCK;
      else if (CEPH_LOCK_EXCL == filelock.type)
	fl->l_type = F_WRLCK;
      else
	fl->l_type = F_UNLCK;

      fl->l_whence = SEEK_SET;
      fl->l_start = filelock.start;
      fl->l_len = filelock.length;
      fl->l_pid = filelock.pid;
    } else if (op == CEPH_MDS_OP_SETFILELOCK) {
      ceph_lock_state_t *lock_state;
      if (lock_type == CEPH_LOCK_FCNTL) {
	if (!in->fcntl_locks)
	  in->fcntl_locks = new ceph_lock_state_t(cct, CEPH_LOCK_FCNTL);
	lock_state = in->fcntl_locks;
      } else if (lock_type == CEPH_LOCK_FLOCK) {
	if (!in->flock_locks)
	  in->flock_locks = new ceph_lock_state_t(cct, CEPH_LOCK_FLOCK);
	lock_state = in->flock_locks;
      } else {
	assert(0);
	return -EINVAL;
      }
      _update_lock_state(fl, owner, lock_state);

      if (fh) {
	if (lock_type == CEPH_LOCK_FCNTL) {
	  if (!fh->fcntl_locks)
	    fh->fcntl_locks = new ceph_lock_state_t(cct, CEPH_LOCK_FCNTL);
	  lock_state = fh->fcntl_locks;
	} else {
	  if (!fh->flock_locks)
	    fh->flock_locks = new ceph_lock_state_t(cct, CEPH_LOCK_FLOCK);
	  lock_state = fh->flock_locks;
	}
	_update_lock_state(fl, owner, lock_state);
      }
    } else
      assert(0);
  }
  return ret;
}

int Client::_interrupt_filelock(MetaRequest *req)
{
  Inode *in = req->inode();

  int lock_type;
  if (req->head.args.filelock_change.rule == CEPH_LOCK_FLOCK)
    lock_type = CEPH_LOCK_FLOCK_INTR;
  else if (req->head.args.filelock_change.rule == CEPH_LOCK_FCNTL)
    lock_type = CEPH_LOCK_FCNTL_INTR;
  else {
    assert(0);
    return -EINVAL;
  }

  MetaRequest *intr_req = new MetaRequest(CEPH_MDS_OP_SETFILELOCK);
  filepath path;
  in->make_nosnap_relative_path(path);
  intr_req->set_filepath(path);
  intr_req->set_inode(in);
  intr_req->head.args.filelock_change = req->head.args.filelock_change;
  intr_req->head.args.filelock_change.rule = lock_type;
  intr_req->head.args.filelock_change.type = CEPH_LOCK_UNLOCK;

  return make_request(intr_req, -1, -1, NULL, NULL, -1);
}

void Client::_encode_filelocks(Inode *in, bufferlist& bl)
{
  if (!in->fcntl_locks && !in->flock_locks)
    return;

  unsigned nr_fcntl_locks = in->fcntl_locks ? in->fcntl_locks->held_locks.size() : 0;
  ::encode(nr_fcntl_locks, bl);
  if (nr_fcntl_locks) {
    ceph_lock_state_t* lock_state = in->fcntl_locks;
    for(multimap<uint64_t, ceph_filelock>::iterator p = lock_state->held_locks.begin();
	p != lock_state->held_locks.end();
	++p)
      ::encode(p->second, bl);
  }

  unsigned nr_flock_locks = in->flock_locks ? in->flock_locks->held_locks.size() : 0;
  ::encode(nr_flock_locks, bl);
  if (nr_flock_locks) {
    ceph_lock_state_t* lock_state = in->flock_locks;
    for(multimap<uint64_t, ceph_filelock>::iterator p = lock_state->held_locks.begin();
	p != lock_state->held_locks.end();
	++p)
      ::encode(p->second, bl);
  }

  ldout(cct, 10) << "_encode_filelocks ino " << in->ino << ", " << nr_fcntl_locks
		 << " fcntl locks, " << nr_flock_locks << " flock locks" <<  dendl;
}

void Client::_release_filelocks(Fh *fh)
{
  if (!fh->fcntl_locks && !fh->flock_locks)
    return;

  Inode *in = fh->inode.get();
  ldout(cct, 10) << "_release_filelocks " << fh << " ino " << in->ino << dendl;

  list<pair<int, ceph_filelock> > to_release;

  if (fh->fcntl_locks) {
    ceph_lock_state_t* lock_state = fh->fcntl_locks;
    for(multimap<uint64_t, ceph_filelock>::iterator p = lock_state->held_locks.begin();
	p != lock_state->held_locks.end();
	++p)
      to_release.push_back(pair<int, ceph_filelock>(CEPH_LOCK_FCNTL, p->second));
    delete fh->fcntl_locks;
  }
  if (fh->flock_locks) {
    ceph_lock_state_t* lock_state = fh->flock_locks;
    for(multimap<uint64_t, ceph_filelock>::iterator p = lock_state->held_locks.begin();
	p != lock_state->held_locks.end();
	++p)
      to_release.push_back(pair<int, ceph_filelock>(CEPH_LOCK_FLOCK, p->second));
    delete fh->flock_locks;
  }

  if (to_release.empty())
    return;

  struct flock fl;
  memset(&fl, 0, sizeof(fl));
  fl.l_whence = SEEK_SET;
  fl.l_type = F_UNLCK;

  for (list<pair<int, ceph_filelock> >::iterator p = to_release.begin();
       p != to_release.end();
       ++p) {
    fl.l_start = p->second.start;
    fl.l_len = p->second.length;
    fl.l_pid = p->second.pid;
    _do_filelock(in, NULL, p->first, CEPH_MDS_OP_SETFILELOCK, 0, &fl, p->second.owner);
  }
}

void Client::_update_lock_state(struct flock *fl, uint64_t owner,
				ceph_lock_state_t *lock_state)
{
  int lock_cmd;
  if (F_RDLCK == fl->l_type)
    lock_cmd = CEPH_LOCK_SHARED;
  else if (F_WRLCK == fl->l_type)
    lock_cmd = CEPH_LOCK_EXCL;
  else
    lock_cmd = CEPH_LOCK_UNLOCK;;

  ceph_filelock filelock;
  filelock.start = fl->l_start;
  filelock.length = fl->l_len;
  filelock.client = 0;
  // see comment in _do_filelock()
  filelock.owner = owner | (1ULL << 63);
  filelock.pid = fl->l_pid;
  filelock.type = lock_cmd;

  if (filelock.type == CEPH_LOCK_UNLOCK) {
    list<ceph_filelock> activated_locks;
    lock_state->remove_lock(filelock, activated_locks);
  } else {
    bool r = lock_state->add_lock(filelock, false, false, NULL);
    assert(r);
  }
}

int Client::_getlk(Fh *fh, struct flock *fl, uint64_t owner)
{
  Inode *in = fh->inode.get();
  ldout(cct, 10) << "_getlk " << fh << " ino " << in->ino << dendl;
  int ret = _do_filelock(in, fh, CEPH_LOCK_FCNTL, CEPH_MDS_OP_GETFILELOCK, 0, fl, owner);
  return ret;
}

int Client::_setlk(Fh *fh, struct flock *fl, uint64_t owner, int sleep)
{
  Inode *in = fh->inode.get();
  ldout(cct, 10) << "_setlk " << fh << " ino " << in->ino << dendl;
  int ret =  _do_filelock(in, fh, CEPH_LOCK_FCNTL, CEPH_MDS_OP_SETFILELOCK, sleep, fl, owner);
  ldout(cct, 10) << "_setlk " << fh << " ino " << in->ino << " result=" << ret << dendl;
  return ret;
}

int Client::_flock(Fh *fh, int cmd, uint64_t owner)
{
  Inode *in = fh->inode.get();
  ldout(cct, 10) << "_flock " << fh << " ino " << in->ino << dendl;

  int sleep = !(cmd & LOCK_NB);
  cmd &= ~LOCK_NB;

  int type;
  switch (cmd) {
    case LOCK_SH:
      type = F_RDLCK;
      break;
    case LOCK_EX:
      type = F_WRLCK;
      break;
    case LOCK_UN:
      type = F_UNLCK;
      break;
    default:
      return -EINVAL;
  }

  struct flock fl;
  memset(&fl, 0, sizeof(fl));
  fl.l_type = type;
  fl.l_whence = SEEK_SET;

  int ret =  _do_filelock(in, fh, CEPH_LOCK_FLOCK, CEPH_MDS_OP_SETFILELOCK, sleep, &fl, owner);
  ldout(cct, 10) << "_flock " << fh << " ino " << in->ino << " result=" << ret << dendl;
  return ret;
}

int Client::ll_statfs(Inode *in, struct statvfs *stbuf)
{
  /* Since the only thing this does is wrap a call to statfs, and
     statfs takes a lock, it doesn't seem we have a need to split it
     out. */
  return statfs(0, stbuf);
}

void Client::ll_register_callbacks(struct client_callback_args *args)
{
  if (!args)
    return;
  Mutex::Locker l(client_lock);
  ldout(cct, 10) << "ll_register_callbacks cb " << args->handle
		 << " invalidate_ino_cb " << args->ino_cb
		 << " invalidate_dentry_cb " << args->dentry_cb
		 << " getgroups_cb" << args->getgroups_cb
		 << " switch_interrupt_cb " << args->switch_intr_cb
		 << " remount_cb " << args->remount_cb
		 << dendl;
  callback_handle = args->handle;
  if (args->ino_cb) {
    ino_invalidate_cb = args->ino_cb;
    async_ino_invalidator.start();
  }
  if (args->dentry_cb) {
    dentry_invalidate_cb = args->dentry_cb;
    async_dentry_invalidator.start();
  }
  if (args->switch_intr_cb) {
    switch_interrupt_cb = args->switch_intr_cb;
    interrupt_finisher.start();
  }
  if (args->remount_cb) {
    remount_cb = args->remount_cb;
    remount_finisher.start();
  }
  getgroups_cb = args->getgroups_cb;
  umask_cb = args->umask_cb;
}

int Client::test_dentry_handling(bool can_invalidate)
{
  int r = 0;

  can_invalidate_dentries = can_invalidate;

  if (can_invalidate_dentries) {
    assert(dentry_invalidate_cb);
    ldout(cct, 1) << "using dentry_invalidate_cb" << dendl;
  } else if (remount_cb) {
    ldout(cct, 1) << "using remount_cb" << dendl;
    int s = remount_cb(callback_handle);
    if (s) {
      lderr(cct) << "Failed to invoke remount, needed to ensure kernel dcache consistency"
		 << dendl;
    }
    if (cct->_conf->client_die_on_failed_remount) {
      require_remount = true;
      r = s;
    }
  } else {
    lderr(cct) << "no method to invalidate kernel dentry cache; expect issues!" << dendl;
    if (cct->_conf->client_die_on_failed_remount)
      assert(0);
  }
  return r;
}

int Client::_sync_fs()
{
  ldout(cct, 10) << "_sync_fs" << dendl;

  // flush file data
  Mutex lock("Client::_fsync::lock");
  Cond cond;
  bool flush_done = false;
  if (cct->_conf->client_oc)
    objectcacher->flush_all(new C_SafeCond(&lock, &cond, &flush_done));
  else
    flush_done = true;

  // flush caps
  flush_caps();
  ceph_tid_t flush_tid = last_flush_tid;

  // wait for unsafe mds requests
  wait_unsafe_requests();

  wait_sync_caps(flush_tid);

  if (!flush_done) {
    client_lock.Unlock();
    lock.Lock();
    ldout(cct, 15) << "waiting on data to flush" << dendl;
    while (!flush_done)
      cond.Wait(lock);
    lock.Unlock();
    client_lock.Lock();
  }

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
  Inode *in = f->inode.get();
  
  _fsync(f, true);
  if (_release(in))
    check_caps(in, false);
  return 0;
}


// =============================
// snaps

int Client::mksnap(const char *relpath, const char *name)
{
  Mutex::Locker l(client_lock);
  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    r = may_create(in.get());
    if (r < 0)
      return r;
  }
  Inode *snapdir = open_snapdir(in.get());
  return _mkdir(snapdir, name, 0);
}
int Client::rmsnap(const char *relpath, const char *name)
{
  Mutex::Locker l(client_lock);
  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    r = may_delete(in.get(), NULL);
    if (r < 0)
      return r;
  }
  Inode *snapdir = open_snapdir(in.get());
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
  InodeRef in;
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
    in = new Inode(this, vino, &diri->layout);

    in->ino = diri->ino;
    in->snapid = CEPH_SNAPDIR;
    in->mode = diri->mode;
    in->uid = diri->uid;
    in->gid = diri->gid;
    in->mtime = diri->mtime;
    in->ctime = diri->ctime;
    in->btime = diri->btime;
    in->size = diri->size;
    in->change_attr = diri->change_attr;

    in->dirfragtree.clear();
    in->snapdir_parent = diri;
    diri->flags |= I_SNAPDIR_OPEN;
    inode_map[vino] = in;
    if (use_faked_inos())
      _assign_faked_ino(in);
    ldout(cct, 10) << "open_snapdir created snapshot inode " << *in << dendl;
  } else {
    in = inode_map[vino];
    ldout(cct, 10) << "open_snapdir had snapshot inode " << *in << dendl;
  }
  return in;
}

int Client::ll_lookup(Inode *parent, const char *name, struct stat *attr,
		      Inode **out, int uid, int gid)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_lookup " << parent << " " << name << dendl;
  tout(cct) << "ll_lookup" << std::endl;
  tout(cct) << name << std::endl;

  int r = 0;
  if (!cct->_conf->fuse_default_permissions) {
    r = may_lookup(parent, uid, gid);
    if (r < 0)
      return r;
  }

  string dname(name);
  InodeRef in;

  r = _lookup(parent, dname, CEPH_STAT_CAP_INODE_ALL, &in, uid, gid);
  if (r < 0) {
    attr->st_ino = 0;
    goto out;
  }

  assert(in);
  fill_stat(in, attr);
  _ll_get(in.get());

 out:
  ldout(cct, 3) << "ll_lookup " << parent << " " << name
	  << " -> " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  tout(cct) << attr->st_ino << std::endl;
  *out = in.get();
  return r;
}

int Client::ll_walk(const char* name, Inode **out, struct stat *attr)
{
  Mutex::Locker lock(client_lock);
  filepath fp(name, 0);
  InodeRef in;
  int rc;

  ldout(cct, 3) << "ll_walk" << name << dendl;
  tout(cct) << "ll_walk" << std::endl;
  tout(cct) << name << std::endl;

  rc = path_walk(fp, &in, false, CEPH_STAT_CAP_INODE_ALL);
  if (rc < 0) {
    attr->st_ino = 0;
    *out = NULL;
    return rc;
  } else {
    assert(in);
    fill_stat(in, attr);
    *out = in.get();
    return 0;
  }
}


void Client::_ll_get(Inode *in)
{
  if (in->ll_ref == 0) {
    in->get();
    if (in->is_dir() && !in->dn_set.empty()) {
      assert(in->dn_set.size() == 1); // dirs can't be hard-linked
      in->get_first_parent()->get(); // pin dentry
    }
  }
  in->ll_get();
  ldout(cct, 20) << "_ll_get " << in << " " << in->ino << " -> " << in->ll_ref << dendl;
}

int Client::_ll_put(Inode *in, int num)
{
  in->ll_put(num);
  ldout(cct, 20) << "_ll_put " << in << " " << in->ino << " " << num << " -> " << in->ll_ref << dendl;
  if (in->ll_ref == 0) {
    if (in->is_dir() && !in->dn_set.empty()) {
      assert(in->dn_set.size() == 1); // dirs can't be hard-linked
      in->get_first_parent()->put(); // unpin dentry
    }
    put_inode(in);
    return 0;
  } else {
    return in->ll_ref;
  }
}

void Client::_ll_drop_pins()
{
  ldout(cct, 10) << "_ll_drop_pins" << dendl;
  ceph::unordered_map<vinodeno_t, Inode*>::iterator next;
  for (ceph::unordered_map<vinodeno_t, Inode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it = next) {
    Inode *in = it->second;
    next = it;
    ++next;
    if (in->ll_ref)
      _ll_put(in, in->ll_ref);
  }
}

bool Client::ll_forget(Inode *in, int count)
{
  Mutex::Locker lock(client_lock);
  inodeno_t ino = _get_inodeno(in);

  ldout(cct, 3) << "ll_forget " << ino << " " << count << dendl;
  tout(cct) << "ll_forget" << std::endl;
  tout(cct) << ino.val << std::endl;
  tout(cct) << count << std::endl;

  if (ino == 1) return true;  // ignore forget on root.

  bool last = false;
  if (in->ll_ref < count) {
    ldout(cct, 1) << "WARNING: ll_forget on " << ino << " " << count
		  << ", which only has ll_ref=" << in->ll_ref << dendl;
    _ll_put(in, in->ll_ref);
    last = true;
  } else {
    if (_ll_put(in, count) == 0)
      last = true;
  }

  return last;
}

bool Client::ll_put(Inode *in)
{
  /* ll_forget already takes the lock */
  return ll_forget(in, 1);
}

snapid_t Client::ll_get_snapid(Inode *in)
{
  Mutex::Locker lock(client_lock);
  return in->snapid;
}

Inode *Client::ll_get_inode(ino_t ino)
{
  Mutex::Locker lock(client_lock);
  vinodeno_t vino = _map_faked_ino(ino);
  unordered_map<vinodeno_t,Inode*>::iterator p = inode_map.find(vino);
  if (p == inode_map.end())
    return NULL;
  Inode *in = p->second;
  _ll_get(in);
  return in;
}

Inode *Client::ll_get_inode(vinodeno_t vino)
{
  Mutex::Locker lock(client_lock);
  unordered_map<vinodeno_t,Inode*>::iterator p = inode_map.find(vino);
  if (p == inode_map.end())
    return NULL;
  Inode *in = p->second;
  _ll_get(in);
  return in;
}

int Client::_ll_getattr(Inode *in, int uid, int gid)
{
  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_getattr " << vino << dendl;
  tout(cct) << "ll_getattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;

  if (vino.snapid < CEPH_NOSNAP)
    return 0;
  else
    return _getattr(in, CEPH_STAT_CAP_INODE_ALL, uid, gid);
}

int Client::ll_getattr(Inode *in, struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  int res = _ll_getattr(in, uid, gid);

  if (res == 0)
    fill_stat(in, attr);
  ldout(cct, 3) << "ll_getattr " << _get_vino(in) << " = " << res << dendl;
  return res;
}

int Client::ll_getattrx(Inode *in, struct ceph_statx *stx, unsigned int want,
			unsigned int flags, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  int res = 0;
  unsigned mask = statx_to_mask(flags, want);

  if (mask && !in->caps_issued_mask(mask))
    res = _ll_getattr(in, uid, gid);

  if (res == 0)
    fill_statx(in, mask, stx);
  ldout(cct, 3) << "ll_getattrx " << _get_vino(in) << " = " << res << dendl;
  return res;
}

int Client::ll_setattr(Inode *in, struct stat *attr, int mask, int uid,
		       int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_setattr " << vino << " mask " << hex << mask << dec
		<< dendl;
  tout(cct) << "ll_setattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << attr->st_mode << std::endl;
  tout(cct) << attr->st_uid << std::endl;
  tout(cct) << attr->st_gid << std::endl;
  tout(cct) << attr->st_size << std::endl;
  tout(cct) << attr->st_mtime << std::endl;
  tout(cct) << attr->st_atime << std::endl;
  tout(cct) << mask << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int res = may_setattr(in, attr, mask, uid, gid);
    if (res < 0)
      return res;
  }

  mask &= ~(CEPH_SETATTR_MTIME_NOW | CEPH_SETATTR_ATIME_NOW);

  InodeRef target(in);
  int res = _setattr(in, attr, mask, uid, gid, &target);
  if (res == 0) {
    assert(in == target.get());
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
  InodeRef in;
  int r = Client::path_walk(path, &in, true, CEPH_STAT_CAP_XATTR);
  if (r < 0)
    return r;
  return _getxattr(in, name, value, size);
}

int Client::lgetxattr(const char *path, const char *name, void *value, size_t size)
{
  Mutex::Locker lock(client_lock);
  InodeRef in;
  int r = Client::path_walk(path, &in, false, CEPH_STAT_CAP_XATTR);
  if (r < 0)
    return r;
  return _getxattr(in, name, value, size);
}

int Client::fgetxattr(int fd, const char *name, void *value, size_t size)
{
  Mutex::Locker lock(client_lock);
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return _getxattr(f->inode, name, value, size);
}

int Client::listxattr(const char *path, char *list, size_t size)
{
  Mutex::Locker lock(client_lock);
  InodeRef in;
  int r = Client::path_walk(path, &in, true, CEPH_STAT_CAP_XATTR);
  if (r < 0)
    return r;
  return Client::_listxattr(in.get(), list, size);
}

int Client::llistxattr(const char *path, char *list, size_t size)
{
  Mutex::Locker lock(client_lock);
  InodeRef in;
  int r = Client::path_walk(path, &in, false, CEPH_STAT_CAP_XATTR);
  if (r < 0)
    return r;
  return Client::_listxattr(in.get(), list, size);
}

int Client::flistxattr(int fd, char *list, size_t size)
{
  Mutex::Locker lock(client_lock);
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return Client::_listxattr(f->inode.get(), list, size);
}

int Client::removexattr(const char *path, const char *name)
{
  Mutex::Locker lock(client_lock);
  InodeRef in;
  int r = Client::path_walk(path, &in, true);
  if (r < 0)
    return r;
  return _removexattr(in, name);
}

int Client::lremovexattr(const char *path, const char *name)
{
  Mutex::Locker lock(client_lock);
  InodeRef in;
  int r = Client::path_walk(path, &in, false);
  if (r < 0)
    return r;
  return _removexattr(in, name);
}

int Client::fremovexattr(int fd, const char *name)
{
  Mutex::Locker lock(client_lock);
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return _removexattr(f->inode, name);
}

int Client::setxattr(const char *path, const char *name, const void *value, size_t size, int flags)
{
  Mutex::Locker lock(client_lock);
  InodeRef in;
  int r = Client::path_walk(path, &in, true);
  if (r < 0)
    return r;
  return _setxattr(in, name, value, size, flags);
}

int Client::lsetxattr(const char *path, const char *name, const void *value, size_t size, int flags)
{
  Mutex::Locker lock(client_lock);
  InodeRef in;
  int r = Client::path_walk(path, &in, false);
  if (r < 0)
    return r;
  return _setxattr(in, name, value, size, flags);
}

int Client::fsetxattr(int fd, const char *name, const void *value, size_t size, int flags)
{
  Mutex::Locker lock(client_lock);
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return _setxattr(f->inode, name, value, size, flags);
}

int Client::_getxattr(Inode *in, const char *name, void *value, size_t size,
		      int uid, int gid)
{
  int r;

  const VXattr *vxattr = _match_vxattr(in, name);
  if (vxattr) {
    r = -ENODATA;

    char buf[256];
    // call pointer-to-member function
    if (!(vxattr->exists_cb && !(this->*(vxattr->exists_cb))(in)))
      r = (this->*(vxattr->getxattr_cb))(in, buf, sizeof(buf));

    if (size != 0) {
      if (r > (int)size) {
	r = -ERANGE;
      } else if (r > 0) {
	memcpy(value, buf, r);
      }
    }
    goto out;
  }

  if (acl_type == NO_ACL && !strncmp(name, "system.", 7)) {
    r = -EOPNOTSUPP;
    goto out;
  }

  r = _getattr(in, CEPH_STAT_CAP_XATTR, uid, gid, in->xattr_version == 0);
  if (r == 0) {
    string n(name);
    r = -ENODATA;
    if (in->xattrs.count(n)) {
      r = in->xattrs[n].length();
      if (r > 0 && size != 0) {
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

int Client::_getxattr(InodeRef &in, const char *name, void *value, size_t size)
{
  if (cct->_conf->client_permissions) {
    int r = xattr_permission(in.get(), name, MAY_READ);
    if (r < 0)
      return r;
  }
  return _getxattr(in.get(), name, value, size);
}

int Client::ll_getxattr(Inode *in, const char *name, void *value,
			size_t size, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_getxattr " << vino << " " << name << " size " << size << dendl;
  tout(cct) << "ll_getxattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = xattr_permission(in, name, MAY_READ, uid, gid);
    if (r < 0)
      return r;
  }

  return _getxattr(in, name, value, size, uid, gid);
}

int Client::_listxattr(Inode *in, char *name, size_t size, int uid, int gid)
{
  int r = _getattr(in, CEPH_STAT_CAP_XATTR, uid, gid, in->xattr_version == 0);
  if (r == 0) {
    for (map<string,bufferptr>::iterator p = in->xattrs.begin();
	 p != in->xattrs.end();
	 ++p)
      r += p->first.length() + 1;

    const VXattr *vxattrs = _get_vxattrs(in);
    r += _vxattrs_name_size(vxattrs);

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
	if (vxattrs) {
	  for (int i = 0; !vxattrs[i].name.empty(); i++) {
	    const VXattr& vxattr = vxattrs[i];
	    if (vxattr.hidden)
	      continue;
	    // call pointer-to-member function
	    if(vxattr.exists_cb && !(this->*(vxattr.exists_cb))(in))
	      continue;
	    memcpy(name, vxattr.name.c_str(), vxattr.name.length());
	    name += vxattr.name.length();
	    *name = '\0';
	    name++;
	  }
	}
      } else
	r = -ERANGE;
    }
  }
  ldout(cct, 3) << "_listxattr(" << in->ino << ", " << size << ") = " << r << dendl;
  return r;
}

int Client::ll_listxattr(Inode *in, char *names, size_t size, int uid,
			 int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_listxattr " << vino << " size " << size << dendl;
  tout(cct) << "ll_listxattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << size << std::endl;

  return _listxattr(in, names, size, uid, gid);
}

int Client::_do_setxattr(Inode *in, const char *name, const void *value,
			 size_t size, int flags, int uid, int gid)
{

  int xattr_flags = 0;
  if (!value)
    xattr_flags |= CEPH_XATTR_REMOVE;
  if (flags & XATTR_CREATE)
    xattr_flags |= CEPH_XATTR_CREATE;
  if (flags & XATTR_REPLACE)
    xattr_flags |= CEPH_XATTR_REPLACE;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_SETXATTR);
  filepath path;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_string2(name);
  req->set_inode(in);
  req->head.args.setxattr.flags = xattr_flags;

  bufferlist bl;
  bl.append((const char*)value, size);
  req->set_data(bl);

  int res = make_request(req, uid, gid);

  trim_cache();
  ldout(cct, 3) << "_setxattr(" << in->ino << ", \"" << name << "\") = " <<
    res << dendl;
  return res;
}

int Client::_setxattr(Inode *in, const char *name, const void *value,
		      size_t size, int flags, int uid, int gid)
{
  if (in->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  bool posix_acl_xattr = false;
  if (acl_type == POSIX_ACL)
    posix_acl_xattr = !strncmp(name, "system.", 7);

  if (strncmp(name, "user.", 5) &&
      strncmp(name, "security.", 9) &&
      strncmp(name, "trusted.", 8) &&
      strncmp(name, "ceph.", 5) &&
      !posix_acl_xattr)
    return -EOPNOTSUPP;

  if (posix_acl_xattr) {
    if (!strcmp(name, ACL_EA_ACCESS)) {
      mode_t new_mode = in->mode;
      if (value) {
	int ret = posix_acl_equiv_mode(value, size, &new_mode);
	if (ret < 0)
	  return ret;
	if (ret == 0) {
	  value = NULL;
	  size = 0;
	}
	if (new_mode != in->mode) {
	  struct stat attr;
	  attr.st_mode = new_mode;
	  ret = _do_setattr(in, &attr, CEPH_SETATTR_MODE, uid, gid, NULL);
	  if (ret < 0)
	    return ret;
	}
      }
    } else if (!strcmp(name, ACL_EA_DEFAULT)) {
      if (value) {
	if (!S_ISDIR(in->mode))
	  return -EACCES;
	int ret = posix_acl_check(value, size);
	if (ret < 0)
	  return -EINVAL;
	if (ret == 0) {
	  value = NULL;
	  size = 0;
	}
      }
    } else {
      return -EOPNOTSUPP;
    }
  } else {
    const VXattr *vxattr = _match_vxattr(in, name);
    if (vxattr && vxattr->readonly)
      return -EOPNOTSUPP;
  }

  return _do_setxattr(in, name, value, size, flags, uid, gid);
}

int Client::_setxattr(InodeRef &in, const char *name, const void *value,
		      size_t size, int flags)
{
  if (cct->_conf->client_permissions) {
    int r = xattr_permission(in.get(), name, MAY_WRITE);
    if (r < 0)
      return r;
  }
  return _setxattr(in.get(), name, value, size, flags);
}

int Client::check_data_pool_exist(string name, string value, const OSDMap *osdmap)
{
  string tmp;
  if (name == "layout") {
    string::iterator begin = value.begin();
    string::iterator end = value.end();
    keys_and_values<string::iterator> p;    // create instance of parser
    std::map<string, string> m;             // map to receive results
    if (!qi::parse(begin, end, p, m)) {     // returns true if successful
      return -EINVAL;
    }
    if (begin != end)
      return -EINVAL;
    for (map<string,string>::iterator q = m.begin(); q != m.end(); ++q) {
      if (q->first == "pool") {
	tmp = q->second;
	break;
      }
    }
  } else if (name == "layout.pool") {
    tmp = value;
  }

  if (tmp.length()) {
    int64_t pool;
    try {
      pool = boost::lexical_cast<unsigned>(tmp);
      if (!osdmap->have_pg_pool(pool))
	return -ENOENT;
    } catch (boost::bad_lexical_cast const&) {
      pool = osdmap->lookup_pg_pool_name(tmp);
      if (pool < 0) {
	return -ENOENT;
      }
    }
  }

  return 0;
}

int Client::ll_setxattr(Inode *in, const char *name, const void *value,
			size_t size, int flags, int uid, int gid)
{
  // For setting pool of layout, MetaRequest need osdmap epoch.
  // There is a race which create a new data pool but client and mds both don't have.
  // Make client got the latest osdmap which make mds quickly judge whether get newer osdmap.
  if (strcmp(name, "ceph.file.layout.pool") == 0 ||  strcmp(name, "ceph.dir.layout.pool") == 0 ||
      strcmp(name, "ceph.file.layout") == 0 || strcmp(name, "ceph.dir.layout") == 0) {
    string rest(strstr(name, "layout"));
    string v((const char*)value);
    int r = objecter->with_osdmap([&](const OSDMap& o) {
      return check_data_pool_exist(rest, v, &o);
    });

    if (r == -ENOENT) {
      C_SaferCond ctx;
      objecter->wait_for_latest_osdmap(&ctx);
      ctx.wait();
    }
  }

  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_setxattr " << vino << " " << name << " size " << size << dendl;
  tout(cct) << "ll_setxattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = xattr_permission(in, name, MAY_WRITE, uid, gid);
    if (r < 0)
      return r;
  }

  return _setxattr(in, name, value, size, flags, uid, gid);
}

int Client::_removexattr(Inode *in, const char *name, int uid, int gid)
{
  if (in->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  // same xattrs supported by kernel client
  if (strncmp(name, "user.", 5) &&
      strncmp(name, "system.", 7) &&
      strncmp(name, "security.", 9) &&
      strncmp(name, "trusted.", 8) &&
      strncmp(name, "ceph.", 5))
    return -EOPNOTSUPP;

  const VXattr *vxattr = _match_vxattr(in, name);
  if (vxattr && vxattr->readonly)
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

int Client::_removexattr(InodeRef &in, const char *name)
{
  if (cct->_conf->client_permissions) {
    int r = xattr_permission(in.get(), name, MAY_WRITE);
    if (r < 0)
      return r;
  }
  return _removexattr(in.get(), name);
}

int Client::ll_removexattr(Inode *in, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_removexattr " << vino << " " << name << dendl;
  tout(cct) << "ll_removexattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = xattr_permission(in, name, MAY_WRITE, uid, gid);
    if (r < 0)
      return r;
  }

  return _removexattr(in, name, uid, gid);
}

bool Client::_vxattrcb_quota_exists(Inode *in)
{
  return in->quota.is_enable();
}
size_t Client::_vxattrcb_quota(Inode *in, char *val, size_t size)
{
  return snprintf(val, size,
                  "max_bytes=%lld max_files=%lld",
                  (long long int)in->quota.max_bytes,
                  (long long int)in->quota.max_files);
}
size_t Client::_vxattrcb_quota_max_bytes(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (long long int)in->quota.max_bytes);
}
size_t Client::_vxattrcb_quota_max_files(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (long long int)in->quota.max_files);
}

bool Client::_vxattrcb_layout_exists(Inode *in)
{
  return in->layout != file_layout_t();
}
size_t Client::_vxattrcb_layout(Inode *in, char *val, size_t size)
{
  int r = snprintf(val, size,
      "stripe_unit=%lld stripe_count=%lld object_size=%lld pool=",
      (unsigned long long)in->layout.stripe_unit,
      (unsigned long long)in->layout.stripe_count,
      (unsigned long long)in->layout.object_size);
  objecter->with_osdmap([&](const OSDMap& o) {
      if (o.have_pg_pool(in->layout.pool_id))
	r += snprintf(val + r, size - r, "%s",
		      o.get_pool_name(in->layout.pool_id).c_str());
      else
	r += snprintf(val + r, size - r, "%" PRIu64,
		      (uint64_t)in->layout.pool_id);
    });
  if (in->layout.pool_ns.length())
    r += snprintf(val + r, size - r, " pool_namespace=%s",
		  in->layout.pool_ns.c_str());
  return r;
}
size_t Client::_vxattrcb_layout_stripe_unit(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)in->layout.stripe_unit);
}
size_t Client::_vxattrcb_layout_stripe_count(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)in->layout.stripe_count);
}
size_t Client::_vxattrcb_layout_object_size(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)in->layout.object_size);
}
size_t Client::_vxattrcb_layout_pool(Inode *in, char *val, size_t size)
{
  size_t r;
  objecter->with_osdmap([&](const OSDMap& o) {
      if (o.have_pg_pool(in->layout.pool_id))
	r = snprintf(val, size, "%s", o.get_pool_name(
		       in->layout.pool_id).c_str());
      else
	r = snprintf(val, size, "%" PRIu64, (uint64_t)in->layout.pool_id);
    });
  return r;
}
size_t Client::_vxattrcb_layout_pool_namespace(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%s", in->layout.pool_ns.c_str());
}
size_t Client::_vxattrcb_dir_entries(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)(in->dirstat.nfiles + in->dirstat.nsubdirs));
}
size_t Client::_vxattrcb_dir_files(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)in->dirstat.nfiles);
}
size_t Client::_vxattrcb_dir_subdirs(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)in->dirstat.nsubdirs);
}
size_t Client::_vxattrcb_dir_rentries(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)(in->rstat.rfiles + in->rstat.rsubdirs));
}
size_t Client::_vxattrcb_dir_rfiles(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)in->rstat.rfiles);
}
size_t Client::_vxattrcb_dir_rsubdirs(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)in->rstat.rsubdirs);
}
size_t Client::_vxattrcb_dir_rbytes(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%lld", (unsigned long long)in->rstat.rbytes);
}
size_t Client::_vxattrcb_dir_rctime(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%ld.09%ld", (long)in->rstat.rctime.sec(),
      (long)in->rstat.rctime.nsec());
}

#define CEPH_XATTR_NAME(_type, _name) "ceph." #_type "." #_name
#define CEPH_XATTR_NAME2(_type, _name, _name2) "ceph." #_type "." #_name "." #_name2

#define XATTR_NAME_CEPH(_type, _name)				\
{								\
  name: CEPH_XATTR_NAME(_type, _name),				\
  getxattr_cb: &Client::_vxattrcb_ ## _type ## _ ## _name,	\
  readonly: true,						\
  hidden: false,						\
  exists_cb: NULL,						\
}
#define XATTR_LAYOUT_FIELD(_type, _name, _field)		\
{								\
  name: CEPH_XATTR_NAME2(_type, _name, _field),			\
  getxattr_cb: &Client::_vxattrcb_ ## _name ## _ ## _field,	\
  readonly: false,						\
  hidden: true,							\
  exists_cb: &Client::_vxattrcb_layout_exists,			\
}
#define XATTR_QUOTA_FIELD(_type, _name)		                \
{								\
  name: CEPH_XATTR_NAME(_type, _name),			        \
  getxattr_cb: &Client::_vxattrcb_ ## _type ## _ ## _name,	\
  readonly: false,						\
  hidden: true,							\
  exists_cb: &Client::_vxattrcb_quota_exists,			\
}

const Client::VXattr Client::_dir_vxattrs[] = {
  {
    name: "ceph.dir.layout",
    getxattr_cb: &Client::_vxattrcb_layout,
    readonly: false,
    hidden: true,
    exists_cb: &Client::_vxattrcb_layout_exists,
  },
  XATTR_LAYOUT_FIELD(dir, layout, stripe_unit),
  XATTR_LAYOUT_FIELD(dir, layout, stripe_count),
  XATTR_LAYOUT_FIELD(dir, layout, object_size),
  XATTR_LAYOUT_FIELD(dir, layout, pool),
  XATTR_LAYOUT_FIELD(dir, layout, pool_namespace),
  XATTR_NAME_CEPH(dir, entries),
  XATTR_NAME_CEPH(dir, files),
  XATTR_NAME_CEPH(dir, subdirs),
  XATTR_NAME_CEPH(dir, rentries),
  XATTR_NAME_CEPH(dir, rfiles),
  XATTR_NAME_CEPH(dir, rsubdirs),
  XATTR_NAME_CEPH(dir, rbytes),
  XATTR_NAME_CEPH(dir, rctime),
  {
    name: "ceph.quota",
    getxattr_cb: &Client::_vxattrcb_quota,
    readonly: false,
    hidden: true,
    exists_cb: &Client::_vxattrcb_quota_exists,
  },
  XATTR_QUOTA_FIELD(quota, max_bytes),
  XATTR_QUOTA_FIELD(quota, max_files),
  { name: "" }     /* Required table terminator */
};

const Client::VXattr Client::_file_vxattrs[] = {
  {
    name: "ceph.file.layout",
    getxattr_cb: &Client::_vxattrcb_layout,
    readonly: false,
    hidden: true,
    exists_cb: &Client::_vxattrcb_layout_exists,
  },
  XATTR_LAYOUT_FIELD(file, layout, stripe_unit),
  XATTR_LAYOUT_FIELD(file, layout, stripe_count),
  XATTR_LAYOUT_FIELD(file, layout, object_size),
  XATTR_LAYOUT_FIELD(file, layout, pool),
  XATTR_LAYOUT_FIELD(file, layout, pool_namespace),
  { name: "" }     /* Required table terminator */
};

const Client::VXattr *Client::_get_vxattrs(Inode *in)
{
  if (in->is_dir())
    return _dir_vxattrs;
  else if (in->is_file())
    return _file_vxattrs;
  return NULL;
}

const Client::VXattr *Client::_match_vxattr(Inode *in, const char *name)
{
  if (strncmp(name, "ceph.", 5) == 0) {
    const VXattr *vxattr = _get_vxattrs(in);
    if (vxattr) {
      while (!vxattr->name.empty()) {
	if (vxattr->name == name)
	  return vxattr;
	vxattr++;
      }
    }
  }
  return NULL;
}

size_t Client::_vxattrs_calcu_name_size(const VXattr *vxattr)
{
  size_t len = 0;
  while (!vxattr->name.empty()) {
    if (!vxattr->hidden)
      len += vxattr->name.length() + 1;
    vxattr++;
  }
  return len;
}

int Client::ll_readlink(Inode *in, char *buf, size_t buflen, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_readlink " << vino << dendl;
  tout(cct) << "ll_readlink" << std::endl;
  tout(cct) << vino.ino.val << std::endl;

  set<Dentry*>::iterator dn = in->dn_set.begin();
  while (dn != in->dn_set.end()) {
    touch_dn(*dn);
    ++dn;
  }

  int r = _readlink(in, buf, buflen);
  ldout(cct, 3) << "ll_readlink " << vino << " = " << r << dendl;
  return r;
}

int Client::_mknod(Inode *dir, const char *name, mode_t mode, dev_t rdev,
		   int uid, int gid, InodeRef *inp)
{
  ldout(cct, 3) << "_mknod(" << dir->ino << " " << name << ", 0" << oct
		<< mode << dec << ", " << rdev << ", uid " << uid << ", gid "
		<< gid << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir)) {
    return -EDQUOT;
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_MKNOD);

  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path); 
  req->set_inode(dir);
  req->head.args.mknod.rdev = rdev;
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  bufferlist xattrs_bl;
  int res = _posix_acl_create(dir, &mode, xattrs_bl, uid, gid);
  if (res < 0)
    goto fail;
  req->head.args.mknod.mode = mode;
  if (xattrs_bl.length() > 0)
    req->set_data(xattrs_bl);

  Dentry *de;
  res = get_or_create(dir, name, &de);
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

int Client::ll_mknod(Inode *parent, const char *name, mode_t mode,
		     dev_t rdev, struct stat *attr, Inode **out,
		     int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_mknod " << vparent << " " << name << dendl;
  tout(cct) << "ll_mknod" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << rdev << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = may_create(parent, uid, gid);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _mknod(parent, name, mode, rdev, uid, gid, &in);
  if (r == 0) {
    fill_stat(in, attr);
    _ll_get(in.get());
  }
  tout(cct) << attr->st_ino << std::endl;
  ldout(cct, 3) << "ll_mknod " << vparent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  *out = in.get();
  return r;
}

int Client::_create(Inode *dir, const char *name, int flags, mode_t mode,
		    InodeRef *inp, Fh **fhp, int stripe_unit, int stripe_count,
		    int object_size, const char *data_pool, bool *created,
		    int uid, int gid)
{
  ldout(cct, 3) << "_create(" << dir->ino << " " << name << ", 0" << oct <<
    mode << dec << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;
  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir)) {
    return -EDQUOT;
  }

  int cmode = ceph_flags_to_mode(flags);
  if (cmode < 0)
    return -EINVAL;

  int64_t pool_id = -1;
  if (data_pool && *data_pool) {
    pool_id = objecter->with_osdmap(
      std::mem_fn(&OSDMap::lookup_pg_pool_name), data_pool);
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

  req->head.args.open.stripe_unit = stripe_unit;
  req->head.args.open.stripe_count = stripe_count;
  req->head.args.open.object_size = object_size;
  if (cct->_conf->client_debug_getattr_caps)
    req->head.args.open.mask = DEBUG_GETATTR_CAPS;
  else
    req->head.args.open.mask = 0;
  req->head.args.open.pool = pool_id;
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  mode |= S_IFREG;
  bufferlist xattrs_bl;
  int res = _posix_acl_create(dir, &mode, xattrs_bl, uid, gid);
  if (res < 0)
    goto fail;
  req->head.args.open.mode = mode;
  if (xattrs_bl.length() > 0)
    req->set_data(xattrs_bl);

  Dentry *de;
  res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);

  res = make_request(req, uid, gid, inp, created);
  if (res < 0) {
    goto reply_error;
  }

  /* If the caller passed a value in fhp, do the open */
  if(fhp) {
    (*inp)->get_open_ref(cmode);
    *fhp = _create_fh(inp->get(), flags, cmode);
  }

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
		   InodeRef *inp)
{
  ldout(cct, 3) << "_mkdir(" << dir->ino << " " << name << ", 0" << oct
		<< mode << dec << ", uid " << uid << ", gid " << gid << ")"
		<< dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP && dir->snapid != CEPH_SNAPDIR) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir)) {
    return -EDQUOT;
  }
  MetaRequest *req = new MetaRequest(dir->snapid == CEPH_SNAPDIR ?
				     CEPH_MDS_OP_MKSNAP : CEPH_MDS_OP_MKDIR);

  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);
  req->set_inode(dir);
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  mode |= S_IFDIR;
  bufferlist xattrs_bl;
  int res = _posix_acl_create(dir, &mode, xattrs_bl, uid, gid);
  if (res < 0)
    goto fail;
  req->head.args.mkdir.mode = mode;
  if (xattrs_bl.length() > 0)
    req->set_data(xattrs_bl);

  Dentry *de;
  res = get_or_create(dir, name, &de);
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

int Client::ll_mkdir(Inode *parent, const char *name, mode_t mode,
		     struct stat *attr, Inode **out, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_mkdir " << vparent << " " << name << dendl;
  tout(cct) << "ll_mkdir" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = may_create(parent, uid, gid);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _mkdir(parent, name, mode, uid, gid, &in);
  if (r == 0) {
    fill_stat(in, attr);
    _ll_get(in.get());
  }
  tout(cct) << attr->st_ino << std::endl;
  ldout(cct, 3) << "ll_mkdir " << vparent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  *out = in.get();
  return r;
}

int Client::_symlink(Inode *dir, const char *name, const char *target, int uid,
		     int gid, InodeRef *inp)
{
  ldout(cct, 3) << "_symlink(" << dir->ino << " " << name << ", " << target
	  << ", uid " << uid << ", gid " << gid << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir)) {
    return -EDQUOT;
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
  ldout(cct, 3) << "_symlink(\"" << path << "\", \"" << target << "\") = " <<
    res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_symlink(Inode *parent, const char *name, const char *value,
		       struct stat *attr, Inode **out, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_symlink " << vparent << " " << name << " -> " << value
		<< dendl;
  tout(cct) << "ll_symlink" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << value << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = may_create(parent, uid, gid);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _symlink(parent, name, value, uid, gid, &in);
  if (r == 0) {
    fill_stat(in, attr);
    _ll_get(in.get());
  }
  tout(cct) << attr->st_ino << std::endl;
  ldout(cct, 3) << "ll_symlink " << vparent << " " << name
	  << " = " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  *out = in.get();
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

  InodeRef otherin;

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  res = _lookup(dir, name, 0, &otherin, uid, gid);
  if (res < 0)
    goto fail;
  req->set_other_inode(otherin.get());
  req->other_inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;

  req->set_inode(dir);

  res = make_request(req, uid, gid);

  trim_cache();
  ldout(cct, 3) << "unlink(" << path << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_unlink(Inode *in, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_unlink " << vino << " " << name << dendl;
  tout(cct) << "ll_unlink" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = may_delete(in, name, uid, gid);
    if (r < 0)
      return r;
  }
  return _unlink(in, name, uid, gid);
}

int Client::_rmdir(Inode *dir, const char *name, int uid, int gid)
{
  ldout(cct, 3) << "_rmdir(" << dir->ino << " " << name << " uid " << uid <<
    " gid " << gid << ")" << dendl;

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
  req->other_inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;

  InodeRef in;

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  res = _lookup(dir, name, 0, &in, uid, gid);
  if (res < 0)
    goto fail;
  if (req->get_op() == CEPH_MDS_OP_RMDIR) {
    req->set_inode(dir);
    req->set_dentry(de);
    req->set_other_inode(in.get());
  } else {
    unlink(de, true, true);
    req->set_other_inode(in.get());
  }

  res = make_request(req, uid, gid);

  trim_cache();
  ldout(cct, 3) << "rmdir(" << path << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_rmdir(Inode *in, const char *name, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_rmdir " << vino << " " << name << dendl;
  tout(cct) << "ll_rmdir" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = may_delete(in, name, uid, gid);
    if (r < 0)
      return r;
  }

  return _rmdir(in, name, uid, gid);
}

int Client::_rename(Inode *fromdir, const char *fromname, Inode *todir, const char *toname, int uid, int gid)
{
  ldout(cct, 3) << "_rename(" << fromdir->ino << " " << fromname << " to " << todir->ino << " " << toname
	  << " uid " << uid << " gid " << gid << ")" << dendl;

  if (fromdir->snapid != todir->snapid)
    return -EXDEV;

  int op = CEPH_MDS_OP_RENAME;
  if (fromdir->snapid != CEPH_NOSNAP) {
    if (fromdir == todir && fromdir->snapid == CEPH_SNAPDIR)
      op = CEPH_MDS_OP_RENAMESNAP;
    else
      return -EROFS;
  }
  if (cct->_conf->client_quota &&
      fromdir != todir &&
      (fromdir->quota.is_enable() ||
       todir->quota.is_enable() ||
       get_quota_root(fromdir) != get_quota_root(todir))) {
    return -EXDEV;
  }

  InodeRef target;
  MetaRequest *req = new MetaRequest(op);

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
  Dentry *de;
  res = get_or_create(todir, toname, &de);
  if (res < 0)
    goto fail;

  if (op == CEPH_MDS_OP_RENAME) {
    req->set_old_dentry(oldde);
    req->old_dentry_drop = CEPH_CAP_FILE_SHARED;
    req->old_dentry_unless = CEPH_CAP_FILE_EXCL;

    req->set_dentry(de);
    req->dentry_drop = CEPH_CAP_FILE_SHARED;
    req->dentry_unless = CEPH_CAP_FILE_EXCL;

    InodeRef oldin, otherin;
    res = _lookup(fromdir, fromname, 0, &oldin, uid, gid);
    if (res < 0)
      goto fail;
    req->set_old_inode(oldin.get());
    req->old_inode_drop = CEPH_CAP_LINK_SHARED;

    res = _lookup(todir, toname, 0, &otherin, uid, gid);
    if (res != 0 && res != -ENOENT) {
      goto fail;
    } else if (res == 0) {
      req->set_other_inode(otherin.get());
      req->other_inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;
    }

    req->set_inode(todir);
  } else {
    // renamesnap reply contains no tracedn, so we need to invalidate
    // dentry manually
    unlink(oldde, true, true);
    unlink(de, true, true);
  }

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

int Client::ll_rename(Inode *parent, const char *name, Inode *newparent,
		      const char *newname, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vparent = _get_vino(parent);
  vinodeno_t vnewparent = _get_vino(newparent);

  ldout(cct, 3) << "ll_rename " << vparent << " " << name << " to "
	  << vnewparent << " " << newname << dendl;
  tout(cct) << "ll_rename" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << vnewparent.ino.val << std::endl;
  tout(cct) << newname << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = may_delete(parent, name, uid, gid);
    if (r < 0)
      return r;
    r = may_delete(newparent, newname, uid, gid);
    if (r < 0 && r != -ENOENT)
      return r;
  }

  return _rename(parent, name, newparent, newname, uid, gid);
}

int Client::_link(Inode *in, Inode *dir, const char *newname, int uid, int gid, InodeRef *inp)
{
  ldout(cct, 3) << "_link(" << in->ino << " to " << dir->ino << " " << newname
	  << " uid " << uid << " gid " << gid << ")" << dendl;

  if (strlen(newname) > NAME_MAX)
    return -ENAMETOOLONG;

  if (in->snapid != CEPH_NOSNAP || dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir)) {
    return -EDQUOT;
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

int Client::ll_link(Inode *in, Inode *newparent, const char *newname,
		    struct stat *attr, int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);
  vinodeno_t vnewparent = _get_vino(newparent);

  ldout(cct, 3) << "ll_link " << in << " to " << vnewparent << " " <<
    newname << dendl;
  tout(cct) << "ll_link" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << vnewparent << std::endl;
  tout(cct) << newname << std::endl;

  int r = 0;
  InodeRef target;

  if (!cct->_conf->fuse_default_permissions) {
    if (S_ISDIR(in->mode)) {
      r = -EPERM;
      goto out;
    }
    r = may_hardlink(in, uid, gid);
    if (r < 0)
      goto out;
    r = may_create(newparent, uid, gid);
    if (r < 0)
      goto out;
  }

  r = _link(in, newparent, newname, uid, gid, &target);
  if (r == 0) {
    assert(target);
    fill_stat(target, attr);
    _ll_get(target.get());
  }
out:
  return r;
}

int Client::ll_num_osds(void)
{
  Mutex::Locker lock(client_lock);
  return objecter->with_osdmap(std::mem_fn(&OSDMap::get_num_osds));
}

int Client::ll_osdaddr(int osd, uint32_t *addr)
{
  Mutex::Locker lock(client_lock);
  entity_addr_t g;
  bool exists = objecter->with_osdmap([&](const OSDMap& o) {
      if (!o.exists(osd))
	return false;
      g = o.get_addr(osd);
      return true;
    });
  if (!exists)
    return -1;
  uint32_t nb_addr = (g.in4_addr()).sin_addr.s_addr;
  *addr = ntohl(nb_addr);
  return 0;
}
uint32_t Client::ll_stripe_unit(Inode *in)
{
  Mutex::Locker lock(client_lock);
  return in->layout.stripe_unit;
}

uint64_t Client::ll_snap_seq(Inode *in)
{
  Mutex::Locker lock(client_lock);
  return in->snaprealm->seq;
}

int Client::ll_file_layout(Inode *in, file_layout_t *layout)
{
  Mutex::Locker lock(client_lock);
  *layout = in->layout;
  return 0;
}

int Client::ll_file_layout(Fh *fh, file_layout_t *layout)
{
  return ll_file_layout(fh->inode.get(), layout);
}

/* Currently we cannot take advantage of redundancy in reads, since we
   would have to go through all possible placement groups (a
   potentially quite large number determined by a hash), and use CRUSH
   to calculate the appropriate set of OSDs for each placement group,
   then index into that.  An array with one entry per OSD is much more
   tractable and works for demonstration purposes. */

int Client::ll_get_stripe_osd(Inode *in, uint64_t blockno,
			      file_layout_t* layout)
{
  Mutex::Locker lock(client_lock);
  inodeno_t ino = ll_get_inodeno(in);
  uint32_t object_size = layout->object_size;
  uint32_t su = layout->stripe_unit;
  uint32_t stripe_count = layout->stripe_count;
  uint64_t stripes_per_object = object_size / su;

  uint64_t stripeno = blockno / stripe_count;    // which horizontal stripe        (Y)
  uint64_t stripepos = blockno % stripe_count;   // which object in the object set (X)
  uint64_t objectsetno = stripeno / stripes_per_object;       // which object set
  uint64_t objectno = objectsetno * stripe_count + stripepos;  // object id

  object_t oid = file_object_t(ino, objectno);
  return objecter->with_osdmap([&](const OSDMap& o) {
      ceph_object_layout olayout =
	o.file_to_object_layout(oid, *layout);
      pg_t pg = (pg_t)olayout.ol_pgid;
      vector<int> osds;
      int primary;
      o.pg_to_acting_osds(pg, &osds, &primary);
      return primary;
    });
}

/* Return the offset of the block, internal to the object */

uint64_t Client::ll_get_internal_offset(Inode *in, uint64_t blockno)
{
  Mutex::Locker lock(client_lock);
  file_layout_t *layout=&(in->layout);
  uint32_t object_size = layout->object_size;
  uint32_t su = layout->stripe_unit;
  uint64_t stripes_per_object = object_size / su;

  return (blockno % stripes_per_object) * su;
}

int Client::ll_opendir(Inode *in, int flags, dir_result_t** dirpp,
		       int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_opendir " << vino << dendl;
  tout(cct) << "ll_opendir" << std::endl;
  tout(cct) << vino.ino.val << std::endl;

  if (!cct->_conf->fuse_default_permissions) {
    int r = may_open(in, flags, uid, gid);
    if (r < 0)
      return r;
  }

  int r = _opendir(in, dirpp, uid, gid);
  tout(cct) << (unsigned long)*dirpp << std::endl;

  ldout(cct, 3) << "ll_opendir " << vino << " = " << r << " (" << *dirpp << ")"
		<< dendl;
  return r;
}

int Client::ll_releasedir(dir_result_t *dirp)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_releasedir " << dirp << dendl;
  tout(cct) << "ll_releasedir" << std::endl;
  tout(cct) << (unsigned long)dirp << std::endl;
  _closedir(dirp);
  return 0;
}

int Client::ll_fsyncdir(dir_result_t *dirp)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_fsyncdir " << dirp << dendl;
  tout(cct) << "ll_fsyncdir" << std::endl;
  tout(cct) << (unsigned long)dirp << std::endl;

  return _fsync(dirp->inode.get(), false);
}

int Client::ll_open(Inode *in, int flags, Fh **fhp, int uid, int gid)
{
  assert(!(flags & O_CREAT));

  Mutex::Locker lock(client_lock);

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_open " << vino << " " << flags << dendl;
  tout(cct) << "ll_open" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << flags << std::endl;

  int r;
  if (uid < 0) {
    uid = get_uid();
    gid = get_gid();
  }
  if (!cct->_conf->fuse_default_permissions) {
    r = may_open(in, flags, uid, gid);
    if (r < 0)
      goto out;
  }

  r = _open(in, flags, 0, fhp /* may be NULL */, uid, gid);

 out:
  Fh *fhptr = fhp ? *fhp : NULL;
  if (fhptr) {
    ll_unclosed_fh_set.insert(fhptr);
  }
  tout(cct) << (unsigned long)fhptr << std::endl;
  ldout(cct, 3) << "ll_open " << vino << " " << flags << " = " << r << " (" <<
    fhptr << ")" << dendl;
  return r;
}

int Client::ll_create(Inode *parent, const char *name, mode_t mode,
		      int flags, struct stat *attr, Inode **outp, Fh **fhp,
		      int uid, int gid)
{
  Mutex::Locker lock(client_lock);

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_create " << vparent << " " << name << " 0" << oct <<
    mode << dec << " " << flags << ", uid " << uid << ", gid " << gid << dendl;
  tout(cct) << "ll_create" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << flags << std::endl;

  bool created = false;
  InodeRef in;
  int r = _lookup(parent, name, CEPH_STAT_CAP_INODE_ALL, &in, uid, gid);

  if (r == 0 && (flags & O_CREAT) && (flags & O_EXCL))
    return -EEXIST;

  if (r == -ENOENT && (flags & O_CREAT)) {
    if (!cct->_conf->fuse_default_permissions) {
      r = may_create(parent, uid, gid);
      if (r < 0)
	goto out;
    }
    r = _create(parent, name, flags, mode, &in, fhp /* may be NULL */,
	        0, 0, 0, NULL, &created, uid, gid);
    if (r < 0)
      goto out;
  }

  if (r < 0)
    goto out;

  assert(in);
  fill_stat(in, attr);

  ldout(cct, 20) << "ll_create created = " << created << dendl;
  if (!created) {
    if (!cct->_conf->fuse_default_permissions) {
      r = may_open(in.get(), flags, uid, gid);
      if (r < 0) {
	if (fhp && *fhp) {
	  int release_r = _release_fh(*fhp);
	  assert(release_r == 0);  // during create, no async data ops should have happened
	}
	goto out;
      }
    }
    if (fhp && (*fhp == NULL)) {
      r = _open(in.get(), flags, mode, fhp, uid, gid);
      if (r < 0)
	goto out;
    }
  }

out:
  if (r < 0)
    attr->st_ino = 0;

  Fh *fhptr = fhp ? *fhp : NULL;
  if (fhptr) {
    ll_unclosed_fh_set.insert(fhptr);
  }
  tout(cct) << (unsigned long)fhptr << std::endl;
  tout(cct) << attr->st_ino << std::endl;
  ldout(cct, 3) << "ll_create " << parent << " " << name << " 0" << oct <<
    mode << dec << " " << flags << " = " << r << " (" << fhptr << " " <<
    hex << attr->st_ino << dec << ")" << dendl;

  // passing an Inode in outp requires an additional ref
  if (outp) {
    if (in)
      _ll_get(in.get());
    *outp = in.get();
  }

  return r;
}

loff_t Client::ll_lseek(Fh *fh, loff_t offset, int whence)
{
  Mutex::Locker lock(client_lock);
  tout(cct) << "ll_lseek" << std::endl;
  tout(cct) << offset << std::endl;
  tout(cct) << whence << std::endl;

  return _lseek(fh, offset, whence);
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

int Client::ll_read_block(Inode *in, uint64_t blockid,
			  char *buf,
			  uint64_t offset,
			  uint64_t length,
			  file_layout_t* layout)
{
  Mutex::Locker lock(client_lock);
  vinodeno_t vino = ll_get_vino(in);
  object_t oid = file_object_t(vino.ino, blockid);
  int r = 0;
  C_SaferCond cond;
  bufferlist bl;

  objecter->read(oid,
		 object_locator_t(layout->pool_id),
		 offset,
		 length,
		 vino.snapid,
		 &bl,
		 CEPH_OSD_FLAG_READ,
		 &cond);

  client_lock.Unlock();
  r = cond.wait();
  client_lock.Lock(); // lock is going to unlock on exit.

  if (r >= 0) {
      bl.copy(0, bl.length(), buf);
      r = bl.length();
  }

  return r;
}

/* It appears that the OSD doesn't return success unless the entire
   buffer was written, return the write length on success. */

int Client::ll_write_block(Inode *in, uint64_t blockid,
			   char* buf, uint64_t offset,
			   uint64_t length, file_layout_t* layout,
			   uint64_t snapseq, uint32_t sync)
{
  Mutex flock("Client::ll_write_block flock");
  vinodeno_t vino = ll_get_vino(in);
  Cond cond;
  bool done;
  int r = 0;
  Context *onack;
  Context *onsafe;

  if (length == 0) {
    return -EINVAL;
  }
  if (true || sync) {
    /* if write is stable, the epilogue is waiting on
     * flock */
    onack = new C_NoopContext;
    onsafe = new C_SafeCond(&flock, &cond, &done, &r);
    done = false;
  } else {
    /* if write is unstable, we just place a barrier for
     * future commits to wait on */
    onack = new C_NoopContext;
    /*onsafe = new C_Block_Sync(this, vino.ino,
			      barrier_interval(offset, offset + length), &r);
    */
    done = true;
  }
  object_t oid = file_object_t(vino.ino, blockid);
  SnapContext fakesnap;
  bufferptr bp;
  if (length > 0) bp = buffer::copy(buf, length);
  bufferlist bl;
  bl.push_back(bp);

  ldout(cct, 1) << "ll_block_write for " << vino.ino << "." << blockid
		<< dendl;

  fakesnap.seq = snapseq;

  /* lock just in time */
  client_lock.Lock();

  objecter->write(oid,
		  object_locator_t(layout->pool_id),
		  offset,
		  length,
		  fakesnap,
		  bl,
		  ceph::real_clock::now(cct),
		  0,
		  onack,
		  onsafe);

  client_lock.Unlock();
  if (!done /* also !sync */) {
    flock.Lock();
    while (! done)
      cond.Wait(flock);
    flock.Unlock();
  }

  if (r < 0) {
      return r;
  } else {
      return length;
  }
}

int Client::ll_commit_blocks(Inode *in,
			     uint64_t offset,
			     uint64_t length)
{
    Mutex::Locker lock(client_lock);
    /*
    BarrierContext *bctx;
    vinodeno_t vino = ll_get_vino(in);
    uint64_t ino = vino.ino;

    ldout(cct, 1) << "ll_commit_blocks for " << vino.ino << " from "
		  << offset << " to " << length << dendl;

    if (length == 0) {
      return -EINVAL;
    }

    map<uint64_t, BarrierContext*>::iterator p = barriers.find(ino);
    if (p != barriers.end()) {
      barrier_interval civ(offset, offset + length);
      p->second->commit_barrier(civ);
    }
    */
    return 0;
}

int Client::ll_write(Fh *fh, loff_t off, loff_t len, const char *data)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_write " << fh << " " << fh->inode->ino << " " << off <<
    "~" << len << dendl;
  tout(cct) << "ll_write" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;
  tout(cct) << off << std::endl;
  tout(cct) << len << std::endl;

  int r = _write(fh, off, len, data, NULL, 0);
  ldout(cct, 3) << "ll_write " << fh << " " << off << "~" << len << " = " << r
		<< dendl;
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

  Inode *in = fh->inode.get();

  if (objecter->osdmap_pool_full(in->layout.pool_id) &&
      !(mode & FALLOC_FL_PUNCH_HOLE)) {
    return -ENOSPC;
  }

  if (in->snapid != CEPH_NOSNAP)
    return -EROFS;

  if ((fh->mode & CEPH_FILE_MODE_WR) == 0)
    return -EBADF;

  uint64_t size = offset + length;
  if (!(mode & (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)) &&
      size > in->size &&
      is_quota_bytes_exceeded(in, size - in->size)) {
    return -EDQUOT;
  }

  int have;
  int r = get_caps(in, CEPH_CAP_FILE_WR, CEPH_CAP_FILE_BUFFER, &have, -1);
  if (r < 0)
    return r;

  Mutex uninline_flock("Clinet::_fallocate_uninline_data flock");
  Cond uninline_cond;
  bool uninline_done = false;
  int uninline_ret = 0;
  Context *onuninline = NULL;

  if (mode & FALLOC_FL_PUNCH_HOLE) {
    if (in->inline_version < CEPH_INLINE_NONE &&
        (have & CEPH_CAP_FILE_BUFFER)) {
      bufferlist bl;
      int len = in->inline_data.length();
      if (offset < len) {
        if (offset > 0)
          in->inline_data.copy(0, offset, bl);
        int size = length;
        if (offset + size > len)
          size = len - offset;
        if (size > 0)
          bl.append_zero(size);
        if (offset + size < len)
          in->inline_data.copy(offset + size, len - offset - size, bl);
        in->inline_data = bl;
        in->inline_version++;
      }
      in->mtime = ceph_clock_now(cct);
      in->change_attr++;
      mark_caps_dirty(in, CEPH_CAP_FILE_WR);
    } else {
      if (in->inline_version < CEPH_INLINE_NONE) {
        onuninline = new C_SafeCond(&uninline_flock,
                                    &uninline_cond,
                                    &uninline_done,
                                    &uninline_ret);
        uninline_data(in, onuninline);
      }

      Mutex flock("Client::_punch_hole flock");
      Cond cond;
      bool done = false;
      Context *onfinish = new C_SafeCond(&flock, &cond, &done);
      Context *onsafe = new C_Client_SyncCommit(this, in);

      unsafe_sync_write++;
      get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

      _invalidate_inode_cache(in, offset, length);
      filer->zero(in->ino, &in->layout,
		      in->snaprealm->get_snap_context(),
		      offset, length,
		      ceph::real_clock::now(cct),
		      0, true, onfinish,
		      new C_OnFinisher(onsafe, &objecter_finisher));
      in->mtime = ceph_clock_now(cct);
      in->change_attr++;
      mark_caps_dirty(in, CEPH_CAP_FILE_WR);

      client_lock.Unlock();
      flock.Lock();
      while (!done)
        cond.Wait(flock);
      flock.Unlock();
      client_lock.Lock();
    }
  } else if (!(mode & FALLOC_FL_KEEP_SIZE)) {
    uint64_t size = offset + length;
    if (size > in->size) {
      in->size = size;
      in->mtime = ceph_clock_now(cct);
      in->change_attr++;
      mark_caps_dirty(in, CEPH_CAP_FILE_WR);

      if (is_quota_bytes_approaching(in)) {
        check_caps(in, true);
      } else {
        if ((in->size << 1) >= in->max_size &&
            (in->reported_size << 1) < in->max_size)
          check_caps(in, false);
      }
    }
  }

  if (onuninline) {
    client_lock.Unlock();
    uninline_flock.Lock();
    while (!uninline_done)
      uninline_cond.Wait(uninline_flock);
    uninline_flock.Unlock();
    client_lock.Lock();

    if (uninline_ret >= 0 || uninline_ret == -ECANCELED) {
      in->inline_data.clear();
      in->inline_version = CEPH_INLINE_NONE;
      mark_caps_dirty(in, CEPH_CAP_FILE_WR);
      check_caps(in, false);
    } else
      r = uninline_ret;
  }

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
#if defined(__linux__) && defined(O_PATH)
  if (fh->flags & O_PATH)
    return -EBADF;
#endif
  return _fallocate(fh, mode, offset, length);
}

int Client::ll_release(Fh *fh)
{
  Mutex::Locker lock(client_lock);
  ldout(cct, 3) << "ll_release (fh)" << fh << " " << fh->inode->ino << " " <<
    dendl;
  tout(cct) << "ll_release (fh)" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  if (ll_unclosed_fh_set.count(fh))
    ll_unclosed_fh_set.erase(fh);
  return _release_fh(fh);
}

int Client::ll_getlk(Fh *fh, struct flock *fl, uint64_t owner)
{
  Mutex::Locker lock(client_lock);

  ldout(cct, 3) << "ll_getlk (fh)" << fh << " " << fh->inode->ino << dendl;
  tout(cct) << "ll_getk (fh)" << (unsigned long)fh << std::endl;

  return _getlk(fh, fl, owner);
}

int Client::ll_setlk(Fh *fh, struct flock *fl, uint64_t owner, int sleep)
{
  Mutex::Locker lock(client_lock);

  ldout(cct, 3) << "ll_setlk  (fh) " << fh << " " << fh->inode->ino << dendl;
  tout(cct) << "ll_setk (fh)" << (unsigned long)fh << std::endl;

  return _setlk(fh, fl, owner, sleep);
}

int Client::ll_flock(Fh *fh, int cmd, uint64_t owner)
{
  Mutex::Locker lock(client_lock);

  ldout(cct, 3) << "ll_flock  (fh) " << fh << " " << fh->inode->ino << dendl;
  tout(cct) << "ll_flock (fh)" << (unsigned long)fh << std::endl;

  return _flock(fh, cmd, owner);
}

class C_Client_RequestInterrupt : public Context  {
private:
  Client *client;
  MetaRequest *req;
public:
  C_Client_RequestInterrupt(Client *c, MetaRequest *r) : client(c), req(r) {
    req->get();
  }
  void finish(int r) {
    Mutex::Locker l(client->client_lock);
    assert(req->head.op == CEPH_MDS_OP_SETFILELOCK);
    client->_interrupt_filelock(req);
    client->put_request(req);
  }
};

void Client::ll_interrupt(void *d)
{
  MetaRequest *req = static_cast<MetaRequest*>(d);
  ldout(cct, 3) << "ll_interrupt tid " << req->get_tid() << dendl;
  tout(cct) << "ll_interrupt tid " << req->get_tid() << std::endl;
  interrupt_finisher.queue(new C_Client_RequestInterrupt(this, req));
}

// =========================================
// layout

// expose file layouts

int Client::describe_layout(const char *relpath, file_layout_t *lp)
{
  Mutex::Locker lock(client_lock);

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in);
  if (r < 0)
    return r;

  *lp = in->layout;

  ldout(cct, 3) << "describe_layout(" << relpath << ") = 0" << dendl;
  return 0;
}

int Client::fdescribe_layout(int fd, file_layout_t *lp)
{
  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();

  *lp = in->layout;

  ldout(cct, 3) << "fdescribe_layout(" << fd << ") = 0" << dendl;
  return 0;
}


// expose osdmap

int64_t Client::get_pool_id(const char *pool_name)
{
  Mutex::Locker lock(client_lock);
  return objecter->with_osdmap(std::mem_fn(&OSDMap::lookup_pg_pool_name),
			       pool_name);
}

string Client::get_pool_name(int64_t pool)
{
  Mutex::Locker lock(client_lock);
  return objecter->with_osdmap([pool](const OSDMap& o) {
      return o.have_pg_pool(pool) ? o.get_pool_name(pool) : string();
    });
}

int Client::get_pool_replication(int64_t pool)
{
  Mutex::Locker lock(client_lock);
  return objecter->with_osdmap([pool](const OSDMap& o) {
      return o.have_pg_pool(pool) ? o.get_pg_pool(pool)->get_size() : -ENOENT;
    });
}

int Client::get_file_extent_osds(int fd, loff_t off, loff_t *len, vector<int>& osds)
{
  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();

  vector<ObjectExtent> extents;
  Striper::file_to_extents(cct, in->ino, &in->layout, off, 1, in->truncate_size, extents);
  assert(extents.size() == 1);

  objecter->with_osdmap([&](const OSDMap& o) {
      pg_t pg = o.object_locator_to_pg(extents[0].oid, extents[0].oloc);
      o.pg_to_acting_osds(pg, osds);
    });

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
    uint64_t su = in->layout.stripe_unit;
    *len = su - (off % su);
  }

  return 0;
}

int Client::get_osd_crush_location(int id, vector<pair<string, string> >& path)
{
  Mutex::Locker lock(client_lock);
  if (id < 0)
    return -EINVAL;
  return objecter->with_osdmap([&](const OSDMap& o) {
      return o.crush->get_full_location_ordered(id, path);
    });
}

int Client::get_file_stripe_address(int fd, loff_t offset,
				    vector<entity_addr_t>& address)
{
  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();

  // which object?
  vector<ObjectExtent> extents;
  Striper::file_to_extents(cct, in->ino, &in->layout, offset, 1,
			   in->truncate_size, extents);
  assert(extents.size() == 1);

  // now we have the object and its 'layout'
  return objecter->with_osdmap([&](const OSDMap& o) {
      pg_t pg = o.object_locator_to_pg(extents[0].oid, extents[0].oloc);
      vector<int> osds;
      o.pg_to_acting_osds(pg, osds);
      if (osds.empty())
	return -EINVAL;
      for (unsigned i = 0; i < osds.size(); i++) {
	entity_addr_t addr = o.get_addr(osds[i]);
	address.push_back(addr);
      }
      return 0;
    });
}

int Client::get_osd_addr(int osd, entity_addr_t& addr)
{
  Mutex::Locker lock(client_lock);
  return objecter->with_osdmap([&](const OSDMap& o) {
      if (!o.exists(osd))
	return -ENOENT;

      addr = o.get_addr(osd);
      return 0;
    });
}

int Client::enumerate_layout(int fd, vector<ObjectExtent>& result,
			     loff_t length, loff_t offset)
{
  Mutex::Locker lock(client_lock);

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();

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
  objecter->with_osdmap([this](const OSDMap& o) {
      if (o.get_epoch() != local_osd_epoch) {
	local_osd = o.find_osd_on_ip(messenger->get_myaddr());
	local_osd_epoch = o.get_epoch();
      }
    });
  return local_osd;
}






// ===============================

void Client::ms_handle_connect(Connection *con)
{
  ldout(cct, 10) << "ms_handle_connect on " << con->get_peer_addr() << dendl;
}

bool Client::ms_handle_reset(Connection *con)
{
  ldout(cct, 0) << "ms_handle_reset on " << con->get_peer_addr() << dendl;
  return false;
}

void Client::ms_handle_remote_reset(Connection *con)
{
  ldout(cct, 0) << "ms_handle_remote_reset on " << con->get_peer_addr() << dendl;
  Mutex::Locker l(client_lock);
  switch (con->get_peer_type()) {
  case CEPH_ENTITY_TYPE_MDS:
    {
      // kludge to figure out which mds this is; fixme with a Connection* state
      mds_rank_t mds = MDS_RANK_NONE;
      MetaSession *s = NULL;
      for (map<mds_rank_t,MetaSession*>::iterator p = mds_sessions.begin();
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
	  ldout(cct, 1) << "reset from mds we were open; mark session as stale" << dendl;
	  s->state = MetaSession::STATE_STALE;
	  break;

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

Inode *Client::get_quota_root(Inode *in)
{
  if (!cct->_conf->client_quota)
    return NULL;

  Inode *cur = in;
  utime_t now = ceph_clock_now(cct);

  while (cur) {
    if (cur != in && cur->quota.is_enable())
      break;

    Inode *parent_in = NULL;
    if (!cur->dn_set.empty()) {
      for (auto p = cur->dn_set.begin(); p != cur->dn_set.end(); ++p) {
	Dentry *dn = *p;
	if (dn->lease_mds >= 0 &&
	    dn->lease_ttl > now &&
	    mds_sessions.count(dn->lease_mds)) {
	  parent_in = dn->dir->parent_inode;
	} else {
	  Inode *diri = dn->dir->parent_inode;
	  if (diri->caps_issued_mask(CEPH_CAP_FILE_SHARED) &&
	      diri->shared_gen == dn->cap_shared_gen) {
	    parent_in = dn->dir->parent_inode;
	  }
	}
	if (parent_in)
	  break;
      }
    } else if (root_parents.count(cur)) {
      parent_in = root_parents[cur].get();
    }

    if (parent_in) {
      cur = parent_in;
      continue;
    }

    if (cur == root_ancestor)
      break;

    MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPNAME);
    filepath path(cur->ino);
    req->set_filepath(path);
    req->set_inode(cur);

    InodeRef parent_ref;
    int ret = make_request(req, -1, -1, &parent_ref);
    if (ret < 0) {
      ldout(cct, 1) << __func__ << " " << in->vino()
		    << " failed to find parent of " << cur->vino()
		    << " err " << ret <<  dendl;
      // FIXME: what to do?
      cur = root_ancestor;
      break;
    }

    now = ceph_clock_now(cct);
    if (cur == in)
      cur = parent_ref.get();
    else
      cur = in; // start over
  }

  ldout(cct, 10) << __func__ << " " << in->vino() << " -> " << cur->vino() << dendl;
  return cur;
}

/**
 * Traverse quota ancestors of the Inode, return true
 * if any of them passes the passed function
 */
bool Client::check_quota_condition(
    Inode *in, std::function<bool (const Inode &in)> test)
{
  if (!cct->_conf->client_quota)
    return false;

  while (true) {
    assert(in != NULL);
    if (test(*in)) {
      return true;
    }

    if (in == root_ancestor) {
      // We're done traversing, drop out
      return false;
    } else {
      // Continue up the tree
      in = get_quota_root(in);
    }
  }

  return false;
}

bool Client::is_quota_files_exceeded(Inode *in)
{
  return check_quota_condition(in, 
      [](const Inode &in) {
        return in.quota.max_files && in.rstat.rsize() >= in.quota.max_files;
      });
}

bool Client::is_quota_bytes_exceeded(Inode *in, int64_t new_bytes)
{
  return check_quota_condition(in, 
      [&new_bytes](const Inode &in) {
        return in.quota.max_bytes && (in.rstat.rbytes + new_bytes)
               > in.quota.max_bytes;
      });
}

bool Client::is_quota_bytes_approaching(Inode *in)
{
  return check_quota_condition(in, 
      [](const Inode &in) {
        if (in.quota.max_bytes) {
          if (in.rstat.rbytes >= in.quota.max_bytes) {
            return true;
          }

          assert(in.size >= in.reported_size);
          const uint64_t space = in.quota.max_bytes - in.rstat.rbytes;
          const uint64_t size = in.size - in.reported_size;
          return (space >> 4) < size;
        } else {
          return false;
        }
      });
}

enum {
  POOL_CHECKED = 1,
  POOL_CHECKING = 2,
  POOL_READ = 4,
  POOL_WRITE = 8,
};

int Client::check_pool_perm(Inode *in, int need)
{
  if (!cct->_conf->client_check_pool_perm)
    return 0;

  int64_t pool_id = in->layout.pool_id;
  std::string pool_ns = in->layout.pool_ns;
  std::pair<int64_t, std::string> perm_key(pool_id, pool_ns);
  int have = 0;
  while (true) {
    auto it = pool_perms.find(perm_key);
    if (it == pool_perms.end())
      break;
    if (it->second == POOL_CHECKING) {
      // avoid concurrent checkings
      wait_on_list(waiting_for_pool_perm);
    } else {
      have = it->second;
      assert(have & POOL_CHECKED);
      break;
    }
  }

  if (!have) {
    if (in->snapid != CEPH_NOSNAP) {
      // pool permission check needs to write to the first object. But for snapshot,
      // head of the first object may have alread been deleted. To avoid creating
      // orphan object, skip the check for now.
      return 0;
    }

    pool_perms[perm_key] = POOL_CHECKING;

    char oid_buf[32];
    snprintf(oid_buf, sizeof(oid_buf), "%llx.00000000", (unsigned long long)in->ino);
    object_t oid = oid_buf;

    SnapContext nullsnapc;

    C_SaferCond rd_cond;
    ObjectOperation rd_op;
    rd_op.stat(NULL, (ceph::real_time*)nullptr, NULL);

    objecter->mutate(oid, OSDMap::file_to_object_locator(in->layout), rd_op,
		     nullsnapc, ceph::real_clock::now(cct), 0, &rd_cond, NULL);

    C_SaferCond wr_cond;
    ObjectOperation wr_op;
    wr_op.create(true);

    objecter->mutate(oid, OSDMap::file_to_object_locator(in->layout), wr_op,
		     nullsnapc, ceph::real_clock::now(cct), 0, &wr_cond, NULL);

    client_lock.Unlock();
    int rd_ret = rd_cond.wait();
    int wr_ret = wr_cond.wait();
    client_lock.Lock();

    bool errored = false;

    if (rd_ret == 0 || rd_ret == -ENOENT)
      have |= POOL_READ;
    else if (rd_ret != -EPERM) {
      ldout(cct, 10) << "check_pool_perm on pool " << pool_id << " ns " << pool_ns
		     << " rd_err = " << rd_ret << " wr_err = " << wr_ret << dendl;
      errored = true;
    }

    if (wr_ret == 0 || wr_ret == -EEXIST)
      have |= POOL_WRITE;
    else if (wr_ret != -EPERM) {
      ldout(cct, 10) << "check_pool_perm on pool " << pool_id << " ns " << pool_ns
		     << " rd_err = " << rd_ret << " wr_err = " << wr_ret << dendl;
      errored = true;
    }

    if (errored) {
      // Indeterminate: erase CHECKING state so that subsequent calls re-check.
      // Raise EIO because actual error code might be misleading for
      // userspace filesystem user.
      pool_perms.erase(perm_key);
      signal_cond_list(waiting_for_pool_perm);
      return -EIO;
    }

    pool_perms[perm_key] = have | POOL_CHECKED;
    signal_cond_list(waiting_for_pool_perm);
  }

  if ((need & CEPH_CAP_FILE_RD) && !(have & POOL_READ)) {
    ldout(cct, 10) << "check_pool_perm on pool " << pool_id << " ns " << pool_ns
		   << " need " << ccap_string(need) << ", but no read perm" << dendl;
    return -EPERM;
  }
  if ((need & CEPH_CAP_FILE_WR) && !(have & POOL_WRITE)) {
    ldout(cct, 10) << "check_pool_perm on pool " << pool_id << " ns " << pool_ns
		   << " need " << ccap_string(need) << ", but no write perm" << dendl;
    return -EPERM;
  }

  return 0;
}

int Client::_posix_acl_permission(Inode *in, uid_t uid, UserGroups& groups, unsigned want)
{
  if (acl_type == POSIX_ACL) {
    if (in->xattrs.count(ACL_EA_ACCESS)) {
      const bufferptr& access_acl = in->xattrs[ACL_EA_ACCESS];
      return posix_acl_permits(access_acl, in->uid, in->gid, uid, groups, want);
    }
  }
  return -EAGAIN;
}

int Client::_posix_acl_chmod(Inode *in, mode_t mode, int uid, int gid)
{
  if (acl_type == NO_ACL)
    return 0;

  int r = _getattr(in, CEPH_STAT_CAP_XATTR, uid, gid, in->xattr_version == 0);
  if (r < 0)
    goto out;

  if (acl_type == POSIX_ACL) {
    if (in->xattrs.count(ACL_EA_ACCESS)) {
      const bufferptr& access_acl = in->xattrs[ACL_EA_ACCESS];
      bufferptr acl(access_acl.c_str(), access_acl.length());
      r = posix_acl_access_chmod(acl, mode);
      if (r < 0)
	goto out;
      r = _do_setxattr(in, ACL_EA_ACCESS, acl.c_str(), acl.length(), 0, uid, gid);
    } else {
      r = 0;
    }
  }
out:
  ldout(cct, 10) << __func__ << " ino " << in->ino << " result=" << r << dendl;
  return r;
}

int Client::_posix_acl_create(Inode *dir, mode_t *mode, bufferlist& xattrs_bl,
			      int uid, int gid)
{
  if (acl_type == NO_ACL)
    return 0;

  if (S_ISLNK(*mode))
    return 0;

  int r = _getattr(dir, CEPH_STAT_CAP_XATTR, uid, gid, dir->xattr_version == 0);
  if (r < 0)
    goto out;

  if (acl_type == POSIX_ACL) {
    if (dir->xattrs.count(ACL_EA_DEFAULT)) {
      map<string, bufferptr> xattrs;

      const bufferptr& default_acl = dir->xattrs[ACL_EA_DEFAULT];
      bufferptr acl(default_acl.c_str(), default_acl.length());
      r = posix_acl_inherit_mode(acl, mode);
      if (r < 0)
	goto out;

      if (r > 0) {
	r = posix_acl_equiv_mode(acl.c_str(), acl.length(), mode);
	if (r < 0)
	  goto out;
	if (r > 0)
	  xattrs[ACL_EA_ACCESS] = acl;
      }

      if (S_ISDIR(*mode))
	xattrs[ACL_EA_DEFAULT] = dir->xattrs[ACL_EA_DEFAULT];

      r = xattrs.size();
      if (r > 0)
	::encode(xattrs, xattrs_bl);
    } else {
      if (umask_cb)
	*mode &= ~umask_cb(callback_handle);
      r = 0;
    }
  }
out:
  ldout(cct, 10) << __func__ << " dir ino " << dir->ino << " result=" << r << dendl;
  return r;
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

/**
 * This is included in cap release messages, to cause
 * the MDS to wait until this OSD map epoch.  It is necessary
 * in corner cases where we cancel RADOS ops, so that
 * nobody else tries to do IO to the same objects in
 * the same epoch as the cancelled ops.
 */
void Client::set_cap_epoch_barrier(epoch_t e)
{
  ldout(cct, 5) << __func__ << " epoch = " << e << dendl;
  cap_epoch_barrier = e;
}

const char** Client::get_tracked_conf_keys() const
{
  static const char* keys[] = {
    "client_cache_size",
    "client_cache_mid",
    "client_acl_type",
    NULL
  };
  return keys;
}

void Client::handle_conf_change(const struct md_config_t *conf,
				const std::set <std::string> &changed)
{
  Mutex::Locker lock(client_lock);

  if (changed.count("client_cache_size") ||
      changed.count("client_cache_mid")) {
    lru.lru_set_max(cct->_conf->client_cache_size);
    lru.lru_set_midpoint(cct->_conf->client_cache_mid);
  }
  if (changed.count("client_acl_type")) {
    acl_type = NO_ACL;
    if (cct->_conf->client_acl_type == "posix_acl")
      acl_type = POSIX_ACL;
  }
}

bool Client::RequestUserGroups::is_in(gid_t id)
{
  if (id == gid)
    return true;
  if (sgid_count < 0)
    init();
  for (int i = 0; i < sgid_count; ++i) {
    if (id == sgids[i])
      return true;
  }
  return false;
}

int Client::RequestUserGroups::get_gids(const gid_t **out)
{
  if (sgid_count < 0)
    init();
  if (sgid_count > 0) {
    *out = sgids;
    return sgid_count;
  } else {
    *out = &gid;
    return 1;
  }
}

void intrusive_ptr_add_ref(Inode *in)
{
  in->get();
}
		
void intrusive_ptr_release(Inode *in)
{
  in->client->put_inode(in);
}

mds_rank_t Client::_get_random_up_mds() const
{
  assert(client_lock.is_locked_by_me());

  std::set<mds_rank_t> up;
  mdsmap->get_up_mds_set(up);

  if (up.empty())
    return MDS_RANK_NONE;
  std::set<mds_rank_t>::const_iterator p = up.begin();
  for (int n = rand() % up.size(); n; n--)
    ++p;
  return *p;
}

