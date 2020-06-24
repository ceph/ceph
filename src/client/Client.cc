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
#include <string.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/utsname.h>
#include <sys/uio.h>

#include <boost/lexical_cast.hpp>
#include <boost/fusion/include/std_pair.hpp>

#include "common/async/waiter.h"

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
#include "common/async/blocked_completion.h"

#include "mon/MonClient.h"

#include "messages/MClientCaps.h"
#include "messages/MClientLease.h"
#include "messages/MClientQuota.h"
#include "messages/MClientReclaim.h"
#include "messages/MClientReclaimReply.h"
#include "messages/MClientReconnect.h"
#include "messages/MClientReply.h"
#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"
#include "messages/MClientSession.h"
#include "messages/MClientSnap.h"
#include "messages/MCommandReply.h"
#include "messages/MFSMap.h"
#include "messages/MFSMapUser.h"
#include "messages/MMDSMap.h"
#include "messages/MOSDMap.h"

#include "mds/flock.h"
#include "mds/cephfs_features.h"
#include "osd/OSDMap.h"
#include "osdc/Filer.h"

#include "common/Cond.h"
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
#include "Delegation.h"
#include "Dir.h"
#include "ClientSnapRealm.h"
#include "Fh.h"
#include "MetaSession.h"
#include "MetaRequest.h"
#include "ObjecterWriteback.h"
#include "posix_acl.h"

#include "include/ceph_assert.h"
#include "include/stat.h"

#include "include/cephfs/ceph_ll_client.h"

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

namespace bs = boost::system;
namespace ca = ceph::async;

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

int Client::CommandHook::call(
  std::string_view command,
  const cmdmap_t& cmdmap,
  Formatter *f,
  std::ostream& errss,
  bufferlist& out)
{
  f->open_object_section("result");
  {
    std::lock_guard l{m_client->client_lock};
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
      ceph_abort_msg("bad command registered");
  }
  f->close_section();
  return 0;
}


// -------------

dir_result_t::dir_result_t(Inode *in, const UserPerm& perms)
  : inode(in), offset(0), next_offset(2),
    release_count(0), ordered_count(0), cache_index(0), start_shared_gen(0),
    perms(perms)
  { }

void Client::_reset_faked_inos()
{
  ino_t start = 1024;
  free_faked_inos.clear();
  free_faked_inos.insert(start, (uint32_t)-1 - start + 1);
  last_used_faked_ino = 0;
  last_used_faked_root = 0;
  _use_faked_inos = sizeof(ino_t) < 8 || cct->_conf->client_use_faked_inos;
}

void Client::_assign_faked_ino(Inode *in)
{
  if (0 == last_used_faked_ino)
    last_used_faked_ino = last_used_faked_ino + 2048; // start(1024)~2048 reserved for _assign_faked_root
  interval_set<ino_t>::const_iterator it = free_faked_inos.lower_bound(last_used_faked_ino + 1);
  if (it == free_faked_inos.end() && last_used_faked_ino > 0) {
    last_used_faked_ino = 2048;
    it = free_faked_inos.lower_bound(last_used_faked_ino + 1);
  }
  ceph_assert(it != free_faked_inos.end());
  if (last_used_faked_ino < it.get_start()) {
    ceph_assert(it.get_len() > 0);
    last_used_faked_ino = it.get_start();
  } else {
    ++last_used_faked_ino;
    ceph_assert(it.get_start() + it.get_len() > last_used_faked_ino);
  }
  in->faked_ino = last_used_faked_ino;
  free_faked_inos.erase(in->faked_ino);
  faked_ino_map[in->faked_ino] = in->vino();
}

/*
 * In the faked mode, if you export multiple subdirectories,
 * you will see that the inode numbers of the exported subdirectories
 * are the same. so we distinguish the mount point by reserving 
 * the "fake ids" between "1024~2048" and combining the last 
 * 10bits(0x3ff) of the "root inodes".
*/
void Client::_assign_faked_root(Inode *in)
{
  interval_set<ino_t>::const_iterator it = free_faked_inos.lower_bound(last_used_faked_root + 1);
  if (it == free_faked_inos.end() && last_used_faked_root > 0) {
    last_used_faked_root = 0;
    it = free_faked_inos.lower_bound(last_used_faked_root + 1);
  }
  assert(it != free_faked_inos.end());
  vinodeno_t inode_info = in->vino();
  uint64_t inode_num = (uint64_t)inode_info.ino;
  ldout(cct, 10) << "inode_num " << inode_num << "inode_num & 0x3ff=" << (inode_num & 0x3ff)<< dendl;
  last_used_faked_root = it.get_start()  + (inode_num & 0x3ff); // 0x3ff mask and get_start will not exceed 2048
  assert(it.get_start() + it.get_len() > last_used_faked_root);

  in->faked_ino = last_used_faked_root;
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
  ldout(cct, 10) << __func__ << " " << ino << " -> " << vino << dendl;
  return vino;
}

vinodeno_t Client::map_faked_ino(ino_t ino)
{
  std::lock_guard lock(client_lock);
  return _map_faked_ino(ino);
}

// cons/des

Client::Client(Messenger *m, MonClient *mc, Objecter *objecter_)
  : Dispatcher(m->cct),
    timer(m->cct, client_lock),
    messenger(m),
    monclient(mc),
    objecter(objecter_),
    whoami(mc->get_global_id()),
    async_ino_invalidator(m->cct),
    async_dentry_invalidator(m->cct),
    interrupt_finisher(m->cct),
    remount_finisher(m->cct),
    async_ino_releasor(m->cct),
    objecter_finisher(m->cct),
    m_command_hook(this),
    fscid(0)
{
  _reset_faked_inos();

  user_id = cct->_conf->client_mount_uid;
  group_id = cct->_conf->client_mount_gid;
  fuse_default_permissions = cct->_conf.get_val<bool>(
    "fuse_default_permissions");

  if (cct->_conf->client_acl_type == "posix_acl")
    acl_type = POSIX_ACL;

  lru.lru_set_midpoint(cct->_conf->client_cache_mid);

  // file handles
  free_fd_set.insert(10, 1<<30);

  mdsmap.reset(new MDSMap);

  // osd interfaces
  writeback_handler.reset(new ObjecterWriteback(objecter, &objecter_finisher,
					    &client_lock));
  objectcacher.reset(new ObjectCacher(cct, "libcephfs", *writeback_handler, client_lock,
				  client_flush_set_callback,    // all commit callback
				  (void*)this,
				  cct->_conf->client_oc_size,
				  cct->_conf->client_oc_max_objects,
				  cct->_conf->client_oc_max_dirty,
				  cct->_conf->client_oc_target_dirty,
				  cct->_conf->client_oc_max_dirty_age,
				  true));
}


Client::~Client()
{
  ceph_assert(ceph_mutex_is_not_locked(client_lock));

  // It is necessary to hold client_lock, because any inode destruction
  // may call into ObjectCacher, which asserts that it's lock (which is
  // client_lock) is held.
  std::lock_guard l{client_lock};
  tear_down_cache();
}

void Client::tear_down_cache()
{
  // fd's
  for (ceph::unordered_map<int, Fh*>::iterator it = fd_map.begin();
       it != fd_map.end();
       ++it) {
    Fh *fh = it->second;
    ldout(cct, 1) << __func__ << " forcing close of fh " << it->first << " ino " << fh->inode->ino << dendl;
    _release_fh(fh);
  }
  fd_map.clear();

  while (!opened_dirs.empty()) {
    dir_result_t *dirp = *opened_dirs.begin();
    ldout(cct, 1) << __func__ << " forcing close of dir " << dirp << " ino " << dirp->inode->ino << dendl;
    _closedir(dirp);
  }

  // caps!
  // *** FIXME ***

  // empty lru
  trim_cache();
  ceph_assert(lru.lru_get_size() == 0);

  // close root ino
  ceph_assert(inode_map.size() <= 1 + root_parents.size());
  if (root && inode_map.size() == 1 + root_parents.size()) {
    delete root;
    root = 0;
    root_ancestor = 0;
    while (!root_parents.empty())
      root_parents.erase(root_parents.begin());
    inode_map.clear();
    _reset_faked_inos();
  }

  ceph_assert(inode_map.empty());
}

inodeno_t Client::get_root_ino()
{
  std::lock_guard l(client_lock);
  if (use_faked_inos())
    return root->faked_ino;
  else
    return root->ino;
}

Inode *Client::get_root()
{
  std::lock_guard l(client_lock);
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

  ldout(cct, 1) << __func__ << dendl;

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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));

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
    f->dump_int("id", get_nodeid().v);
    entity_inst_t inst(messenger->get_myname(), messenger->get_myaddr_legacy());
    f->dump_object("inst", inst);
    f->dump_object("addr", inst.addr);
    f->dump_stream("inst_str") << inst.name << " " << inst.addr.get_legacy_str();
    f->dump_string("addr_str", inst.addr.get_legacy_str());
    f->dump_int("inode_count", inode_map.size());
    f->dump_int("mds_epoch", mdsmap->get_epoch());
    f->dump_int("osd_epoch", osd_epoch);
    f->dump_int("osd_epoch_barrier", cap_epoch_barrier);
    f->dump_bool("blacklisted", blacklisted);
  }
}

void Client::_pre_init()
{
  timer.init();

  objecter_finisher.start();
  filer.reset(new Filer(objecter, &objecter_finisher));
  objecter->enable_blacklist_events();

  objectcacher->start();
}

int Client::init()
{
  _pre_init();
  {
    std::lock_guard l{client_lock};
    ceph_assert(!initialized);
    messenger->add_dispatcher_tail(this);
  }
  _finish_init();
  return 0;
}

void Client::_finish_init()
{
  {
    std::lock_guard l{client_lock};
    // logger
    PerfCountersBuilder plb(cct, "client", l_c_first, l_c_last);
    plb.add_time_avg(l_c_reply, "reply", "Latency of receiving a reply on metadata request");
    plb.add_time_avg(l_c_lat, "lat", "Latency of processing a metadata request");
    plb.add_time_avg(l_c_wrlat, "wrlat", "Latency of a file data write operation");
    plb.add_time_avg(l_c_read, "rdlat", "Latency of a file data read operation");
    plb.add_time_avg(l_c_fsync, "fsync", "Latency of a file sync operation");
    logger.reset(plb.create_perf_counters());
    cct->get_perfcounters_collection()->add(logger.get());
  }

  cct->_conf.add_observer(this);

  AdminSocket* admin_socket = cct->get_admin_socket();
  int ret = admin_socket->register_command("mds_requests",
					   &m_command_hook,
					   "show in-progress mds requests");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }
  ret = admin_socket->register_command("mds_sessions",
				       &m_command_hook,
				       "show mds session state");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }
  ret = admin_socket->register_command("dump_cache",
				       &m_command_hook,
				       "show in-memory metadata cache contents");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }
  ret = admin_socket->register_command("kick_stale_sessions",
				       &m_command_hook,
				       "kick sessions that were remote reset");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }
  ret = admin_socket->register_command("status",
				       &m_command_hook,
				       "show overall client status");
  if (ret < 0) {
    lderr(cct) << "error registering admin socket command: "
	       << cpp_strerror(-ret) << dendl;
  }

  std::lock_guard l{client_lock};
  initialized = true;
}

void Client::shutdown() 
{
  ldout(cct, 1) << __func__ << dendl;

  // If we were not mounted, but were being used for sending
  // MDS commands, we may have sessions that need closing.
  {
    std::lock_guard l{client_lock};
    _close_sessions();
  }
  cct->_conf.remove_observer(this);

  cct->get_admin_socket()->unregister_commands(&m_command_hook);

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

  if (ino_release_cb) {
    ldout(cct, 10) << "shutdown stopping inode release finisher" << dendl;
    async_ino_releasor.wait_for_empty();
    async_ino_releasor.stop();
  }

  objectcacher->stop();  // outside of client_lock! this does a join.
  {
    std::lock_guard l{client_lock};
    ceph_assert(initialized);
    initialized = false;
    timer.shutdown();
  }
  objecter_finisher.wait_for_empty();
  objecter_finisher.stop();

  if (logger) {
    cct->get_perfcounters_collection()->remove(logger.get());
    logger.reset();
  }
}


// ===================
// metadata cache stuff

void Client::trim_cache(bool trim_kernel_dcache)
{
  uint64_t max = cct->_conf->client_cache_size;
  ldout(cct, 20) << "trim_cache size " << lru.lru_get_size() << " max " << max << dendl;
  unsigned last = 0;
  while (lru.lru_get_size() != last) {
    last = lru.lru_get_size();

    if (!unmounting && lru.lru_get_size() <= max)  break;

    // trim!
    Dentry *dn = static_cast<Dentry*>(lru.lru_get_next_expire());
    if (!dn)
      break;  // done
    
    trim_dentry(dn);
  }

  if (trim_kernel_dcache && lru.lru_get_size() > max)
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
  ldout(cct, 20) << __func__ << " mds." << mds << dendl;

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

  ldout(cct, 20) << __func__ << " mds." << mds
		 << " trimmed " << trimmed << " dentries" << dendl;

  if (s->caps.size() > 0)
    _invalidate_kernel_dcache();
}

void Client::trim_dentry(Dentry *dn)
{
  ldout(cct, 15) << "trim_dentry unlinking dn " << dn->name 
		 << " in dir "
		 << std::hex << dn->dir->parent_inode->ino << std::dec
		 << dendl;
  if (dn->inode) {
    Inode *diri = dn->dir->parent_inode;
    diri->dir_release_count++;
    clear_dir_complete_and_ordered(diri, true);
  }
  unlink(dn, false, false);  // drop dir, drop dentry
}


void Client::update_inode_file_size(Inode *in, int issued, uint64_t size,
				    uint64_t truncate_seq, uint64_t truncate_size)
{
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
}

void Client::update_inode_file_time(Inode *in, int issued, uint64_t time_warp_seq,
				    utime_t ctime, utime_t mtime, utime_t atime)
{
  ldout(cct, 10) << __func__ << " " << *in << " " << ccap_string(issued)
		 << " ctime " << ctime << " mtime " << mtime << dendl;

  if (time_warp_seq > in->time_warp_seq)
    ldout(cct, 10) << " mds time_warp_seq " << time_warp_seq
		   << " is higher than local time_warp_seq "
		   << in->time_warp_seq << dendl;

  int warn = false;
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

void Client::_fragmap_remove_stopped_mds(Inode *in, mds_rank_t mds)
{
  for (auto p = in->fragmap.begin(); p != in->fragmap.end(); )
    if (p->second == mds)
      in->fragmap.erase(p++);
    else
      ++p;
}

Inode * Client::add_update_inode(InodeStat *st, utime_t from,
				 MetaSession *session,
				 const UserPerm& request_perms)
{
  Inode *in;
  bool was_new = false;
  if (inode_map.count(st->vino)) {
    in = inode_map[st->vino];
    ldout(cct, 12) << __func__ << " had " << *in << " caps " << ccap_string(st->cap.caps) << dendl;
  } else {
    in = new Inode(this, st->vino, &st->layout);
    inode_map[st->vino] = in;

    if (use_faked_inos())
      _assign_faked_ino(in);

    if (!root) {
      root = in;
      if (use_faked_inos())
        _assign_faked_root(root);
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

  // only update inode if mds info is strictly newer, or it is the same and projected (odd).
  bool new_version = false;
  if (in->version == 0 ||
      ((st->cap.flags & CEPH_CAP_FLAG_AUTH) &&
       (in->version & ~1) < st->version))
    new_version = true;

  int issued;
  in->caps_issued(&issued);
  issued |= in->caps_dirty();
  int new_issued = ~issued & (int)st->cap.caps;

  if ((new_version || (new_issued & CEPH_CAP_AUTH_SHARED)) &&
      !(issued & CEPH_CAP_AUTH_EXCL)) {
    in->mode = st->mode;
    in->uid = st->uid;
    in->gid = st->gid;
    in->btime = st->btime;
    in->snap_btime = st->snap_btime;
  }

  if ((new_version || (new_issued & CEPH_CAP_LINK_SHARED)) &&
      !(issued & CEPH_CAP_LINK_EXCL)) {
    in->nlink = st->nlink;
  }

  if (new_version || (new_issued & CEPH_CAP_ANY_RD)) {
    update_inode_file_time(in, issued, st->time_warp_seq,
			   st->ctime, st->mtime, st->atime);
  }

  if (new_version ||
      (new_issued & (CEPH_CAP_ANY_FILE_RD | CEPH_CAP_ANY_FILE_WR))) {
    in->layout = st->layout;
    update_inode_file_size(in, issued, st->size, st->truncate_seq, st->truncate_size);
  }

  if (in->is_dir()) {
    if (new_version || (new_issued & CEPH_CAP_FILE_SHARED)) {
      in->dirstat = st->dirstat;
    }
    // dir_layout/rstat/quota are not tracked by capability, update them only if
    // the inode stat is from auth mds
    if (new_version || (st->cap.flags & CEPH_CAP_FLAG_AUTH)) {
      in->dir_layout = st->dir_layout;
      ldout(cct, 20) << " dir hash is " << (int)in->dir_layout.dl_dir_hash << dendl;
      in->rstat = st->rstat;
      in->quota = st->quota;
      in->dir_pin = st->dir_pin;
    }
    // move me if/when version reflects fragtree changes.
    if (in->dirfragtree != st->dirfragtree) {
      in->dirfragtree = st->dirfragtree;
      _fragmap_remove_non_leaves(in);
    }
  }

  if ((in->xattr_version  == 0 || !(issued & CEPH_CAP_XATTR_EXCL)) &&
      st->xattrbl.length() &&
      st->xattr_version > in->xattr_version) {
    auto p = st->xattrbl.cbegin();
    decode(in->xattrs, p);
    in->xattr_version = st->xattr_version;
  }

  if (st->inline_version > in->inline_version) {
    in->inline_data = st->inline_data;
    in->inline_version = st->inline_version;
  }

  /* always take a newer change attr */
  if (st->change_attr > in->change_attr)
    in->change_attr = st->change_attr;

  if (st->version > in->version)
    in->version = st->version;

  if (was_new)
    ldout(cct, 12) << __func__ << " adding " << *in << " caps " << ccap_string(st->cap.caps) << dendl;

  if (!st->cap.caps)
    return in;   // as with readdir returning indoes in different snaprealms (no caps!)

  if (in->snapid == CEPH_NOSNAP) {
    add_update_cap(in, session, st->cap.cap_id, st->cap.caps, st->cap.wanted,
		   st->cap.seq, st->cap.mseq, inodeno_t(st->cap.realm),
		   st->cap.flags, request_perms);
    if (in->auth_cap && in->auth_cap->session == session) {
      in->max_size = st->max_size;
      in->rstat = st->rstat;
    }

    // setting I_COMPLETE needs to happen after adding the cap
    if (in->is_dir() &&
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
	for (const auto& p : in->dir->dentries) {
	  unlink(p.second, true, true);  // keep dir, keep dentry
	}
	if (in->dir->dentries.empty())
	  close_dir(in->dir);
      }
    }
  } else {
    in->snap_caps |= st->cap.caps;
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

  ldout(cct, 12) << __func__ << " '" << dname << "' vino " << in->vino()
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
  
  ceph_assert(dn);

  if (dlease->mask & CEPH_LEASE_VALID) {
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

  auto& reply = request->reply;
  ConnectionRef con = request->reply->get_connection();
  uint64_t features;
  if(session->mds_features.test(CEPHFS_FEATURE_REPLY_ENCODING)) {
    features = (uint64_t)-1;
  }
  else {
    features = con->get_features();
  }

  dir_result_t *dirp = request->dirp;
  ceph_assert(dirp);

  // the extra buffer list is only set for readdir and lssnap replies
  auto p = reply->get_extra_bl().cbegin();
  if (!p.end()) {
    // snapdir?
    if (request->head.op == CEPH_MDS_OP_LSSNAP) {
      ceph_assert(diri);
      diri = open_snapdir(diri);
    }

    // only open dir if we're actually adding stuff to it!
    Dir *dir = diri->open_dir();
    ceph_assert(dir);

    // dirstat
    DirStat dst(p, features);
    __u32 numdn;
    __u16 flags;
    decode(numdn, p);
    decode(flags, p);

    bool end = ((unsigned)flags & CEPH_READDIR_FRAG_END);
    bool hash_order = ((unsigned)flags & CEPH_READDIR_HASH_ORDER);

    frag_t fg = (unsigned)request->head.args.readdir.frag;
    unsigned readdir_offset = dirp->next_offset;
    string readdir_start = dirp->last_name;
    ceph_assert(!readdir_start.empty() || readdir_offset == 2);

    unsigned last_hash = 0;
    if (hash_order) {
      if (!readdir_start.empty()) {
	last_hash = ceph_frag_value(diri->hash_dentry_name(readdir_start));
      } else if (flags & CEPH_READDIR_OFFSET_HASH) {
	/* mds understands offset_hash */
	last_hash = (unsigned)request->head.args.readdir.offset_hash;
      }
    }

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
		   << ", hash_order=" << hash_order
		   << ", readdir_start " << readdir_start
		   << ", last_hash " << last_hash
		   << ", next_offset " << readdir_offset << dendl;

    if (diri->snapid != CEPH_SNAPDIR &&
	fg.is_leftmost() && readdir_offset == 2 &&
	!(hash_order && last_hash)) {
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
      decode(dname, p);
      dlease.decode(p, features);
      InodeStat ist(p, features);

      ldout(cct, 15) << "" << i << ": '" << dname << "'" << dendl;

      Inode *in = add_update_inode(&ist, request->sent_stamp, session,
				   request->perms);
      Dentry *dn;
      if (diri->dir->dentries.count(dname)) {
	Dentry *olddn = diri->dir->dentries[dname];
	if (olddn->inode != in) {
	  // replace incorrect dentry
	  unlink(olddn, true, true);  // keep dir, dentry
	  dn = link(dir, dname, in, olddn);
	  ceph_assert(dn == olddn);
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
	    ceph_assert(!dirp->inode->is_complete_and_ordered());
	    dir->readdir_cache.reserve(dirp->cache_index + numdn);
	  }
	  dir->readdir_cache.push_back(dn);
	} else if (dirp->cache_index < dir->readdir_cache.size()) {
	  if (dirp->inode->is_complete_and_ordered())
	    ceph_assert(dir->readdir_cache[dirp->cache_index] == dn);
	  else
	    dir->readdir_cache[dirp->cache_index] = dn;
	} else {
	  ceph_abort_msg("unexpected readdir buffer idx");
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
  auto& reply = request->reply;
  int op = request->get_op();

  ldout(cct, 10) << "insert_trace from " << request->sent_stamp << " mds." << session->mds_num
	   << " is_target=" << (int)reply->head.is_target
	   << " is_dentry=" << (int)reply->head.is_dentry
	   << dendl;

  auto p = reply->get_trace_bl().cbegin();
  if (request->got_unsafe) {
    ldout(cct, 10) << "insert_trace -- already got unsafe; ignoring" << dendl;
    ceph_assert(p.end());
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
	ceph_assert(od);
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
  uint64_t features;
  if (session->mds_features.test(CEPHFS_FEATURE_REPLY_ENCODING)) {
    features = (uint64_t)-1;
  }
  else {
    features = con->get_features();
  }
  ldout(cct, 10) << " features 0x" << hex << features << dec << dendl;

  // snap trace
  SnapRealm *realm = NULL;
  if (reply->snapbl.length())
    update_snap_trace(reply->snapbl, &realm);

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
    dst.decode(p, features);
    decode(dname, p);
    dlease.decode(p, features);
  }

  Inode *in = 0;
  if (reply->head.is_target) {
    ist.decode(p, features);
    if (cct->_conf->client_debug_getattr_caps) {
      unsigned wanted = 0;
      if (op == CEPH_MDS_OP_GETATTR || op == CEPH_MDS_OP_LOOKUP)
	wanted = request->head.args.getattr.mask;
      else if (op == CEPH_MDS_OP_OPEN || op == CEPH_MDS_OP_CREATE)
	wanted = request->head.args.open.mask;

      if ((wanted & CEPH_CAP_XATTR_SHARED) &&
	  !(ist.xattr_version > 0 && ist.xattrbl.length() > 0))
	ceph_abort_msg("MDS reply does not contain xattrs");
    }

    in = add_update_inode(&ist, request->sent_stamp, session,
			  request->perms);
  }

  Inode *diri = NULL;
  if (reply->head.is_dentry) {
    diri = add_update_inode(&dirst, request->sent_stamp, session,
			    request->perms);
    update_dir_dist(diri, &dst);  // dir stat info is attached to ..

    if (in) {
      Dir *dir = diri->open_dir();
      insert_dentry_inode(dir, dname, &dlease, in, request->sent_stamp, session,
                          (op == CEPH_MDS_OP_RENAME) ? request->old_dentry() : NULL);
    } else {
      Dentry *dn = NULL;
      if (diri->dir && diri->dir->dentries.count(dname)) {
	dn = diri->dir->dentries[dname];
	if (dn->inode) {
	  diri->dir_ordered_count++;
	  clear_dir_complete_and_ordered(diri, false);
	  unlink(dn, true, true);  // keep dir, dentry
	}
      }
      if (dlease.duration_ms > 0) {
	if (!dn) {
	  Dir *dir = diri->open_dir();
	  dn = link(dir, dname, NULL, NULL);
	}
	update_dentry_lease(dn, &dlease, request->sent_stamp, session);
      }
    }
  } else if (op == CEPH_MDS_OP_LOOKUPSNAP ||
	     op == CEPH_MDS_OP_MKSNAP) {
    ldout(cct, 10) << " faking snap lookup weirdness" << dendl;
    // fake it for snap lookup
    vinodeno_t vino = ist.vino;
    vino.snapid = CEPH_SNAPDIR;
    ceph_assert(inode_map.count(vino));
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

  if (realm)
    put_snap_realm(realm);

  request->target = in;
  return in;
}

// -------

mds_rank_t Client::choose_target_mds(MetaRequest *req, Inode** phash_diri)
{
  mds_rank_t mds = MDS_RANK_NONE;
  __u32 hash = 0;
  bool is_hash = false;

  Inode *in = NULL;
  Dentry *de = NULL;

  if (req->resend_mds >= 0) {
    mds = req->resend_mds;
    req->resend_mds = -1;
    ldout(cct, 10) << __func__ << " resend_mds specified as mds." << mds << dendl;
    goto out;
  }

  if (cct->_conf->client_use_random_mds)
    goto random_mds;

  in = req->inode();
  de = req->dentry();
  if (in) {
    ldout(cct, 20) << __func__ << " starting with req->inode " << *in << dendl;
    if (req->path.depth()) {
      hash = in->hash_dentry_name(req->path[0]);
      ldout(cct, 20) << __func__ << " inode dir hash is " << (int)in->dir_layout.dl_dir_hash
	       << " on " << req->path[0]
	       << " => " << hash << dendl;
      is_hash = true;
    }
  } else if (de) {
    if (de->inode) {
      in = de->inode.get();
      ldout(cct, 20) << __func__ << " starting with req->dentry inode " << *in << dendl;
    } else {
      in = de->dir->parent_inode;
      hash = in->hash_dentry_name(de->name);
      ldout(cct, 20) << __func__ << " dentry dir hash is " << (int)in->dir_layout.dl_dir_hash
	       << " on " << de->name
	       << " => " << hash << dendl;
      is_hash = true;
    }
  }
  if (in) {
    if (in->snapid != CEPH_NOSNAP) {
      ldout(cct, 10) << __func__ << " " << *in << " is snapped, using nonsnap parent" << dendl;
      while (in->snapid != CEPH_NOSNAP) {
        if (in->snapid == CEPH_SNAPDIR)
	  in = in->snapdir_parent.get();
        else if (!in->dentries.empty())
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
  
    ldout(cct, 20) << __func__ << " " << *in << " is_hash=" << is_hash
             << " hash=" << hash << dendl;
  
    if (is_hash && S_ISDIR(in->mode) && !in->fragmap.empty()) {
      frag_t fg = in->dirfragtree[hash];
      if (in->fragmap.count(fg)) {
	mds = in->fragmap[fg];
	if (phash_diri)
	  *phash_diri = in;
      } else if (in->auth_cap) {
	mds = in->auth_cap->session->mds_num;
      }
      if (mds >= 0) {
	ldout(cct, 10) << __func__ << " from dirfragtree hash" << dendl;
	goto out;
      }
    }
  
    if (in->auth_cap && req->auth_is_best()) {
      mds = in->auth_cap->session->mds_num;
    } else if (!in->caps.empty()) {
      mds = in->caps.begin()->second.session->mds_num;
    } else {
      goto random_mds;
    }
    ldout(cct, 10) << __func__ << " from caps on inode " << *in << dendl;
  
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
  ldout(cct, 10) << __func__ << " for mds." << mds << dendl;
  ceph_assert(mds_sessions.count(mds));
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
  entity_inst_t inst(messenger->get_myname(), messenger->get_myaddr_legacy());
  f->dump_object("inst", inst);
  f->dump_stream("inst_str") << inst;
  f->dump_stream("addr_str") << inst.addr;
  f->open_array_section("sessions");
  for (const auto &p : mds_sessions) {
    f->open_object_section("session");
    p.second.dump(f);
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

int Client::verify_reply_trace(int r, MetaSession *session,
			       MetaRequest *request, const MConstRef<MClientReply>& reply,
			       InodeRef *ptarget, bool *pcreated,
			       const UserPerm& perms)
{
  // check whether this request actually did the create, and set created flag
  bufferlist extra_bl;
  inodeno_t created_ino;
  bool got_created_ino = false;
  ceph::unordered_map<vinodeno_t, Inode*>::iterator p;

  extra_bl = reply->get_extra_bl();
  if (extra_bl.length() >= 8) {
    if (session->mds_features.test(CEPHFS_FEATURE_DELEG_INO)) {
     struct openc_response_t	ocres;

     decode(ocres, extra_bl);
     created_ino = ocres.created_ino;
     /*
      * The userland cephfs client doesn't have a way to do an async create
      * (yet), so just discard delegated_inos for now. Eventually we should
      * store them and use them in create calls, even if they are synchronous,
      * if only for testing purposes.
      */
     ldout(cct, 10) << "delegated_inos: " << ocres.delegated_inos << dendl;
    } else {
     // u64 containing number of created ino
     decode(created_ino, extra_bl);
    }
    ldout(cct, 10) << "make_request created ino " << created_ino << dendl;
    got_created_ino = true;
  }

  if (pcreated)
    *pcreated = got_created_ino;

  if (request->target) {
    *ptarget = request->target;
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
	  r = _do_lookup(d->dir->parent_inode, d->name, request->regetattr_mask,
			 &target, perms);
	} else {
	  // if the dentry is not linked, just do our best. see #5021.
	  ceph_abort_msg("how did this happen?  i want logs!");
	}
      } else {
	Inode *in = request->inode();
	ldout(cct, 10) << "make_request got traceless reply, forcing getattr on #"
		       << in->ino << dendl;
	r = _getattr(in, request->regetattr_mask, perms, true);
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
 * @param perms The user uid/gid to execute as (eventually, full group lists?)
 * @param ptarget [optional] address to store a pointer to the target inode we want to create or operate on
 * @param pcreated [optional; required if ptarget] where to store a bool of whether our create atomically created a file
 * @param use_mds [optional] prefer a specific mds (-1 for default)
 * @param pdirbl [optional; disallowed if ptarget] where to pass extra reply payload to the caller
 */
int Client::make_request(MetaRequest *request,
			 const UserPerm& perms,
			 InodeRef *ptarget, bool *pcreated,
			 mds_rank_t use_mds,
			 bufferlist *pdirbl)
{
  int r = 0;

  // assign a unique tid
  ceph_tid_t tid = ++last_tid;
  request->set_tid(tid);

  // and timestamp
  request->op_stamp = ceph_clock_now();

  // make note
  mds_requests[tid] = request->get();
  if (oldest_tid == 0 && request->get_op() != CEPH_MDS_OP_SETFILELOCK)
    oldest_tid = tid;

  request->set_caller_perms(perms);

  if (cct->_conf->client_inject_fixed_oldest_tid) {
    ldout(cct, 20) << __func__ << " injecting fixed oldest_client_tid(1)" << dendl;
    request->set_oldest_client_tid(1);
  } else {
    request->set_oldest_client_tid(oldest_tid);
  }

  // hack target mds?
  if (use_mds >= 0)
    request->resend_mds = use_mds;

  MetaSession *session = NULL;
  while (1) {
    if (request->aborted())
      break;

    if (blacklisted) {
      request->abort(-EBLACKLISTED);
      break;
    }

    // set up wait cond
    ceph::condition_variable caller_cond;
    request->caller_cond = &caller_cond;

    // choose mds
    Inode *hash_diri = NULL;
    mds_rank_t mds = choose_target_mds(request, &hash_diri);
    int mds_state = (mds == MDS_RANK_NONE) ? MDSMap::STATE_NULL : mdsmap->get_state(mds);
    if (mds_state != MDSMap::STATE_ACTIVE && mds_state != MDSMap::STATE_STOPPING) {
      if (mds_state == MDSMap::STATE_NULL && mds >= mdsmap->get_max_mds()) {
	if (hash_diri) {
	  ldout(cct, 10) << " target mds." << mds << " has stopped, remove it from fragmap" << dendl;
	  _fragmap_remove_stopped_mds(hash_diri, mds);
	} else {
	  ldout(cct, 10) << " target mds." << mds << " has stopped, trying a random mds" << dendl;
	  request->resend_mds = _get_random_up_mds();
	}
      } else {
	ldout(cct, 10) << " target mds." << mds << " not active, waiting for new mdsmap" << dendl;
	wait_on_list(waiting_for_mdsmap);
      }
      continue;
    }

    // open a session?
    if (!have_open_session(mds)) {
      session = _get_or_open_mds_session(mds);
      if (session->state == MetaSession::STATE_REJECTED) {
	request->abort(-EPERM);
	break;
      }
      // wait
      if (session->state == MetaSession::STATE_OPENING) {
	ldout(cct, 10) << "waiting for session to mds." << mds << " to open" << dendl;
	wait_on_context_list(session->waiting_for_open);
	continue;
      }

      if (!have_open_session(mds))
	continue;
    } else {
      session = &mds_sessions.at(mds);
    }

    // send request.
    send_request(request, session);

    // wait for signal
    ldout(cct, 20) << "awaiting reply|forward|kick on " << &caller_cond << dendl;
    request->kick = false;
    std::unique_lock l{client_lock, std::adopt_lock};
    caller_cond.wait(l, [request] {
      return (request->reply ||	          // reply
	      request->resend_mds >= 0 || // forward
	      request->kick);
    });
    l.release();
    request->caller_cond = nullptr;

    // did we get a reply?
    if (request->reply) 
      break;
  }

  if (!request->reply) {
    ceph_assert(request->aborted());
    ceph_assert(!request->got_unsafe);
    r = request->get_abort_code();
    request->item.remove_myself();
    unregister_request(request);
    put_request(request);
    return r;
  }

  // got it!
  auto reply = std::move(request->reply);
  r = reply->get_result();
  if (r >= 0)
    request->success = true;

  // kick dispatcher (we've got it!)
  ceph_assert(request->dispatch_cond);
  request->dispatch_cond->notify_all();
  ldout(cct, 20) << "sendrecv kickback on tid " << tid << " " << request->dispatch_cond << dendl;
  request->dispatch_cond = 0;
  
  if (r >= 0 && ptarget)
    r = verify_reply_trace(r, session, request, reply, ptarget, pcreated, perms);

  if (pdirbl)
    *pdirbl = reply->get_extra_bl();

  // -- log times --
  utime_t lat = ceph_clock_now();
  lat -= request->sent_stamp;
  ldout(cct, 20) << "lat " << lat << dendl;
  logger->tinc(l_c_lat, lat);
  logger->tinc(l_c_reply, lat);

  put_request(request);
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
  ldout(cct, 20) << __func__ << " enter(in:" << *in << ", req:" << req
	   << " mds:" << mds << ", drop:" << drop << ", unless:" << unless
	   << ", force:" << force << ")" << dendl;
  int released = 0;
  auto it = in->caps.find(mds);
  if (it != in->caps.end()) {
    Cap &cap = it->second;
    drop &= ~(in->dirty_caps | get_caps_used(in));
    if ((drop & cap.issued) &&
	!(unless & cap.issued)) {
      ldout(cct, 25) << "dropping caps " << ccap_string(drop) << dendl;
      cap.issued &= ~drop;
      cap.implemented &= ~drop;
      released = 1;
    } else {
      released = force;
    }
    if (released) {
      cap.wanted = in->caps_wanted();
      if (&cap == in->auth_cap &&
	  !(cap.wanted & CEPH_CAP_ANY_FILE_WR)) {
	in->requested_max_size = 0;
	ldout(cct, 25) << "reset requested_max_size due to not wanting any file write cap" << dendl;
      }
      ceph_mds_request_release rel;
      rel.ino = in->ino;
      rel.cap_id = cap.cap_id;
      rel.seq = cap.seq;
      rel.issue_seq = cap.issue_seq;
      rel.mseq = cap.mseq;
      rel.caps = cap.implemented;
      rel.wanted = cap.wanted;
      rel.dname_len = 0;
      rel.dname_seq = 0;
      req->cap_releases.push_back(MClientRequest::Release(rel,""));
    }
  }
  ldout(cct, 25) << __func__ << " exit(in:" << *in << ") released:"
	   << released << dendl;
  return released;
}

void Client::encode_dentry_release(Dentry *dn, MetaRequest *req,
			   mds_rank_t mds, int drop, int unless)
{
  ldout(cct, 20) << __func__ << " enter(dn:"
	   << dn << ")" << dendl;
  int released = 0;
  if (dn->dir)
    released = encode_inode_release(dn->dir->parent_inode, req,
				    mds, drop, unless, 1);
  if (released && dn->lease_mds == mds) {
    ldout(cct, 25) << "preemptively releasing dn to mds" << dendl;
    auto& rel = req->cap_releases.back();
    rel.item.dname_len = dn->name.length();
    rel.item.dname_seq = dn->lease_seq;
    rel.dname = dn->name;
  }
  ldout(cct, 25) << __func__ << " exit(dn:"
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
  ldout(cct, 20) << __func__ << " enter (req: "
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
  ldout(cct, 25) << __func__ << " exit (req: "
	   << req << ", mds " << mds <<dendl;
}

bool Client::have_open_session(mds_rank_t mds)
{
  const auto &it = mds_sessions.find(mds);
  return it != mds_sessions.end() &&
    (it->second.state == MetaSession::STATE_OPEN ||
     it->second.state == MetaSession::STATE_STALE);
}

MetaSession *Client::_get_mds_session(mds_rank_t mds, Connection *con)
{
  const auto &it = mds_sessions.find(mds);
  if (it == mds_sessions.end() || it->second.con != con) {
    return NULL;
  } else {
    return &it->second;
  }
}

MetaSession *Client::_get_or_open_mds_session(mds_rank_t mds)
{
  auto it = mds_sessions.find(mds);
  return it == mds_sessions.end() ? _open_mds_session(mds) : &it->second;
}

/**
 * Populate a map of strings with client-identifying metadata,
 * such as the hostname.  Call this once at initialization.
 */
void Client::populate_metadata(const std::string &mount_root)
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

  metadata["pid"] = stringify(getpid());

  // Ceph entity id (the '0' in "client.0")
  metadata["entity_id"] = cct->_conf->name.get_id();

  // Our mount position
  if (!mount_root.empty()) {
    metadata["root"] = mount_root;
  }

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
  std::lock_guard l(client_lock);
  ceph_assert(initialized);

  auto it = metadata.find(k);
  if (it != metadata.end()) {
    ldout(cct, 1) << __func__ << " warning, overriding metadata field '" << k
		  << "' from '" << it->second << "' to '" << v << "'" << dendl;
  }

  metadata[k] = v;
}

MetaSession *Client::_open_mds_session(mds_rank_t mds)
{
  ldout(cct, 10) << __func__ << " mds." << mds << dendl;
  auto addrs = mdsmap->get_addrs(mds);
  auto em = mds_sessions.emplace(std::piecewise_construct,
      std::forward_as_tuple(mds),
      std::forward_as_tuple(mds, messenger->connect_to_mds(addrs), addrs));
  ceph_assert(em.second); /* not already present */
  MetaSession *session = &em.first->second;

  auto m = make_message<MClientSession>(CEPH_SESSION_REQUEST_OPEN);
  m->metadata = metadata;
  m->supported_features = feature_bitset_t(CEPHFS_FEATURES_CLIENT_SUPPORTED);
  session->con->send_message2(std::move(m));
  return session;
}

void Client::_close_mds_session(MetaSession *s)
{
  ldout(cct, 2) << __func__ << " mds." << s->mds_num << " seq " << s->seq << dendl;
  s->state = MetaSession::STATE_CLOSING;
  s->con->send_message2(make_message<MClientSession>(CEPH_SESSION_REQUEST_CLOSE, s->seq));
}

void Client::_closed_mds_session(MetaSession *s, int err, bool rejected)
{
  ldout(cct, 5) << __func__ << " mds." << s->mds_num << " seq " << s->seq << dendl;
  if (rejected && s->state != MetaSession::STATE_CLOSING)
    s->state = MetaSession::STATE_REJECTED;
  else
    s->state = MetaSession::STATE_CLOSED;
  s->con->mark_down();
  signal_context_list(s->waiting_for_open);
  mount_cond.notify_all();
  remove_session_caps(s, err);
  kick_requests_closed(s);
  if (s->state == MetaSession::STATE_CLOSED)
    mds_sessions.erase(s->mds_num);
}

void Client::handle_client_session(const MConstRef<MClientSession>& m)
{
  mds_rank_t from = mds_rank_t(m->get_source().num());
  ldout(cct, 10) << __func__ << " " << *m << " from mds." << from << dendl;

  MetaSession *session = _get_mds_session(from, m->get_connection().get());
  if (!session) {
    ldout(cct, 10) << " discarding session message from sessionless mds " << m->get_source_inst() << dendl;
    return;
  }

  switch (m->get_op()) {
  case CEPH_SESSION_OPEN:
    {
      feature_bitset_t missing_features(CEPHFS_FEATURES_CLIENT_REQUIRED);
      missing_features -= m->supported_features;
      if (!missing_features.empty()) {
	lderr(cct) << "mds." << from << " lacks required features '"
		   << missing_features << "', closing session " << dendl;
	_close_mds_session(session);
	_closed_mds_session(session, -EPERM, true);
	break;
      }
      session->mds_features = std::move(m->supported_features);

      renew_caps(session);
      session->state = MetaSession::STATE_OPEN;
      if (unmounting)
	mount_cond.notify_all();
      else
	connect_mds_targets(from);
      signal_context_list(session->waiting_for_open);
      break;
    }

  case CEPH_SESSION_CLOSE:
    _closed_mds_session(session);
    break;

  case CEPH_SESSION_RENEWCAPS:
    if (session->cap_renew_seq == m->get_seq()) {
      bool was_stale = ceph_clock_now() >= session->cap_ttl;
      session->cap_ttl =
	session->last_cap_renew_request + mdsmap->get_session_timeout();
      if (was_stale)
	wake_up_session_caps(session, false);
    }
    break;

  case CEPH_SESSION_STALE:
    // invalidate session caps/leases
    session->cap_gen++;
    session->cap_ttl = ceph_clock_now();
    session->cap_ttl -= 1;
    renew_caps(session);
    break;

  case CEPH_SESSION_RECALL_STATE:
    trim_caps(session, m->get_max_caps());
    break;

  case CEPH_SESSION_FLUSHMSG:
    /* flush cap release */
    if (auto& m = session->release; m) {
      session->con->send_message2(std::move(m));
    }
    session->con->send_message2(make_message<MClientSession>(CEPH_SESSION_FLUSHMSG_ACK, m->get_seq()));
    break;

  case CEPH_SESSION_FORCE_RO:
    force_session_readonly(session);
    break;

  case CEPH_SESSION_REJECT:
    {
      std::string_view error_str;
      auto it = m->metadata.find("error_string");
      if (it != m->metadata.end())
	error_str = it->second;
      else
	error_str = "unknown error";
      lderr(cct) << "mds." << from << " rejected us (" << error_str << ")" << dendl;

      _closed_mds_session(session, -EPERM, true);
    }
    break;

  default:
    ceph_abort();
  }
}

bool Client::_any_stale_sessions() const
{
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));

  for (const auto &p : mds_sessions) {
    if (p.second.state == MetaSession::STATE_STALE) {
      return true;
    }
  }

  return false;
}

void Client::_kick_stale_sessions()
{
  ldout(cct, 1) << __func__ << dendl;

  for (auto it = mds_sessions.begin(); it != mds_sessions.end(); ) {
    MetaSession &s = it->second;
    if (s.state == MetaSession::STATE_REJECTED) {
      mds_sessions.erase(it++);
      continue;
    }
    ++it;
    if (s.state == MetaSession::STATE_STALE)
      _closed_mds_session(&s);
  }
}

void Client::send_request(MetaRequest *request, MetaSession *session,
			  bool drop_cap_releases)
{
  // make the request
  mds_rank_t mds = session->mds_num;
  ldout(cct, 10) << __func__ << " rebuilding request " << request->get_tid()
		 << " for mds." << mds << dendl;
  auto r = build_client_request(request);
  if (request->dentry()) {
    r->set_dentry_wanted();
  }
  if (request->got_unsafe) {
    r->set_replayed_op();
    if (request->target)
      r->head.ino = request->target->ino;
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
    request->sent_stamp = ceph_clock_now();
    ldout(cct, 20) << __func__ << " set sent_stamp to " << request->sent_stamp << dendl;
  }
  request->mds = mds;

  Inode *in = request->inode();
  if (in) {
    auto it = in->caps.find(mds);
    if (it != in->caps.end()) {
      request->sent_on_mseq = it->second.mseq;
    }
  }

  session->requests.push_back(&request->item);

  ldout(cct, 10) << __func__ << " " << *r << " to mds." << mds << dendl;
  session->con->send_message2(std::move(r));
}

ref_t<MClientRequest> Client::build_client_request(MetaRequest *request)
{
  auto req = make_message<MClientRequest>(request->get_op());
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
  const gid_t *_gids;
  int gid_count = request->perms.get_gids(&_gids);
  req->set_gid_list(gid_count, _gids);
  return req;
}



void Client::handle_client_request_forward(const MConstRef<MClientRequestForward>& fwd)
{
  mds_rank_t mds = mds_rank_t(fwd->get_source().num());
  MetaSession *session = _get_mds_session(mds, fwd->get_connection().get());
  if (!session) {
    return;
  }
  ceph_tid_t tid = fwd->get_tid();

  if (mds_requests.count(tid) == 0) {
    ldout(cct, 10) << __func__ << " no pending request on tid " << tid << dendl;
    return;
  }

  MetaRequest *request = mds_requests[tid];
  ceph_assert(request);

  // reset retry counter
  request->retry_attempt = 0;

  // request not forwarded, or dest mds has no session.
  // resend.
  ldout(cct, 10) << __func__ << " tid " << tid
	   << " fwd " << fwd->get_num_fwd() 
	   << " to mds." << fwd->get_dest_mds() 
	   << ", resending to " << fwd->get_dest_mds()
	   << dendl;
  
  request->mds = -1;
  request->item.remove_myself();
  request->num_fwd = fwd->get_num_fwd();
  request->resend_mds = fwd->get_dest_mds();
  request->caller_cond->notify_all();
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

void Client::handle_client_reply(const MConstRef<MClientReply>& reply)
{
  mds_rank_t mds_num = mds_rank_t(reply->get_source().num());
  MetaSession *session = _get_mds_session(mds_num, reply->get_connection().get());
  if (!session) {
    return;
  }

  ceph_tid_t tid = reply->get_tid();
  bool is_safe = reply->is_safe();

  if (mds_requests.count(tid) == 0) {
    lderr(cct) << __func__ << " no pending request on tid " << tid
	       << " safe is:" << is_safe << dendl;
    return;
  }
  MetaRequest *request = mds_requests.at(tid);

  ldout(cct, 20) << __func__ << " got a reply. Safe:" << is_safe
		 << " tid " << tid << dendl;

  if (request->got_unsafe && !is_safe) {
    //duplicate response
    ldout(cct, 0) << "got a duplicate reply on tid " << tid << " from mds "
	    << mds_num << " safe:" << is_safe << dendl;
    return;
  }

  if (-ESTALE == reply->get_result()) { // see if we can get to proper MDS
    ldout(cct, 20) << "got ESTALE on tid " << request->tid
		   << " from mds." << request->mds << dendl;
    request->send_to_auth = true;
    request->resend_mds = choose_target_mds(request);
    Inode *in = request->inode();
    std::map<mds_rank_t, Cap>::const_iterator it;
    if (request->resend_mds >= 0 &&
	request->resend_mds == request->mds &&
	(in == NULL ||
         (it = in->caps.find(request->resend_mds)) != in->caps.end() ||
         request->sent_on_mseq == it->second.mseq)) {
      ldout(cct, 20) << "have to return ESTALE" << dendl;
    } else {
      request->caller_cond->notify_all();
      return;
    }
  }
  
  ceph_assert(!request->reply);
  request->reply = reply;
  insert_trace(request, session);

  // Handle unsafe reply
  if (!is_safe) {
    request->got_unsafe = true;
    session->unsafe_requests.push_back(&request->unsafe_item);
    if (is_dir_operation(request)) {
      Inode *dir = request->inode();
      ceph_assert(dir);
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
    ceph::condition_variable cond;
    request->dispatch_cond = &cond;

    // wake up waiter
    ldout(cct, 20) << __func__ << " signalling caller " << (void*)request->caller_cond << dendl;
    request->caller_cond->notify_all();

    // wake for kick back
    std::unique_lock l{client_lock, std::adopt_lock};
    cond.wait(l, [tid, request, &cond, this] {
      if (request->dispatch_cond) {
        ldout(cct, 20) << "handle_client_reply awaiting kickback on tid "
		       << tid << " " << &cond << dendl;
      }
      return !request->dispatch_cond;
    });
    l.release();
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
    mount_cond.notify_all();
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
      inode->set_async_err(-ENOSPC);
    }
  }

  if (cancelled_epoch != (epoch_t)-1) {
    set_cap_epoch_barrier(cancelled_epoch);
  }
}

void Client::handle_osd_map(const MConstRef<MOSDMap>& m)
{
  std::set<entity_addr_t> new_blacklists;
  objecter->consume_blacklist_events(&new_blacklists);

  const auto myaddrs = messenger->get_myaddrs();
  bool new_blacklist = false;
  bool prenautilus = objecter->with_osdmap(
    [&](const OSDMap& o) {
      return o.require_osd_release < ceph_release_t::nautilus;
    });
  if (!blacklisted) {
    for (auto a : myaddrs.v) {
      // blacklist entries are always TYPE_ANY for nautilus+
      a.set_type(entity_addr_t::TYPE_ANY);
      if (new_blacklists.count(a)) {
	new_blacklist = true;
	break;
      }
      if (prenautilus) {
	// ...except pre-nautilus, they were TYPE_LEGACY
	a.set_type(entity_addr_t::TYPE_LEGACY);
	if (new_blacklists.count(a)) {
	  new_blacklist = true;
	  break;
	}
      }
    }
  }
  if (new_blacklist) {
    auto epoch = objecter->with_osdmap([](const OSDMap &o){
        return o.get_epoch();
        });
    lderr(cct) << "I was blacklisted at osd epoch " << epoch << dendl;
    blacklisted = true;

    _abort_mds_sessions(-EBLACKLISTED);

    // Since we know all our OSD ops will fail, cancel them all preemtively,
    // so that on an unhealthy cluster we can umount promptly even if e.g.
    // some PGs were inaccessible.
    objecter->op_cancel_writes(-EBLACKLISTED);

  } 

  if (blacklisted) {
    // Handle case where we were blacklisted but no longer are
    blacklisted = objecter->with_osdmap([myaddrs](const OSDMap &o){
        return o.is_blacklisted(myaddrs);});
  }

  // Always subscribe to next osdmap for blacklisted client
  // until this client is not blacklisted.
  if (blacklisted) {
    objecter->maybe_request_map();
  }

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
}


// ------------------------
// incoming messages


bool Client::ms_dispatch2(const MessageRef &m)
{
  std::lock_guard l(client_lock);
  if (!initialized) {
    ldout(cct, 10) << "inactive, discarding " << *m << dendl;
    return true;
  }

  switch (m->get_type()) {
    // mounting and mds sessions
  case CEPH_MSG_MDS_MAP:
    handle_mds_map(ref_cast<MMDSMap>(m));
    break;
  case CEPH_MSG_FS_MAP:
    handle_fs_map(ref_cast<MFSMap>(m));
    break;
  case CEPH_MSG_FS_MAP_USER:
    handle_fs_map_user(ref_cast<MFSMapUser>(m));
    break;
  case CEPH_MSG_CLIENT_SESSION:
    handle_client_session(ref_cast<MClientSession>(m));
    break;

  case CEPH_MSG_OSD_MAP:
    handle_osd_map(ref_cast<MOSDMap>(m));
    break;

    // requests
  case CEPH_MSG_CLIENT_REQUEST_FORWARD:
    handle_client_request_forward(ref_cast<MClientRequestForward>(m));
    break;
  case CEPH_MSG_CLIENT_REPLY:
    handle_client_reply(ref_cast<MClientReply>(m));
    break;

  // reclaim reply
  case CEPH_MSG_CLIENT_RECLAIM_REPLY:
    handle_client_reclaim_reply(ref_cast<MClientReclaimReply>(m));
    break;

  case CEPH_MSG_CLIENT_SNAP:
    handle_snap(ref_cast<MClientSnap>(m));
    break;
  case CEPH_MSG_CLIENT_CAPS:
    handle_caps(ref_cast<MClientCaps>(m));
    break;
  case CEPH_MSG_CLIENT_LEASE:
    handle_lease(ref_cast<MClientLease>(m));
    break;
  case MSG_COMMAND_REPLY:
    if (m->get_source().type() == CEPH_ENTITY_TYPE_MDS) {
      handle_command_reply(ref_cast<MCommandReply>(m));
    } else {
      return false;
    }
    break;
  case CEPH_MSG_CLIENT_QUOTA:
    handle_quota(ref_cast<MClientQuota>(m));
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
      mount_cond.notify_all();
    } else {
      ldout(cct, 10) << "unmounting: trim pass, size still " << lru.lru_get_size() 
               << "+" << inode_map.size() << dendl;
    }
  }

  return true;
}

void Client::handle_fs_map(const MConstRef<MFSMap>& m)
{
  fsmap.reset(new FSMap(m->get_fsmap()));

  signal_cond_list(waiting_for_fsmap);

  monclient->sub_got("fsmap", fsmap->get_epoch());
}

void Client::handle_fs_map_user(const MConstRef<MFSMapUser>& m)
{
  fsmap_user.reset(new FSMapUser);
  *fsmap_user = m->get_fsmap();

  monclient->sub_got("fsmap.user", fsmap_user->get_epoch());
  signal_cond_list(waiting_for_fsmap);
}

void Client::handle_mds_map(const MConstRef<MMDSMap>& m)
{
  mds_gid_t old_inc, new_inc;
  if (m->get_epoch() <= mdsmap->get_epoch()) {
    ldout(cct, 1) << __func__ << " epoch " << m->get_epoch()
                  << " is identical to or older than our "
                  << mdsmap->get_epoch() << dendl;
    return;
  }

  ldout(cct, 1) << __func__ << " epoch " << m->get_epoch() << dendl;

  std::unique_ptr<MDSMap> oldmap(new MDSMap);
  oldmap.swap(mdsmap);

  mdsmap->decode(m->get_encoded());

  // Cancel any commands for missing or laggy GIDs
  std::list<ceph_tid_t> cancel_ops;
  auto &commands = command_table.get_commands();
  for (const auto &i : commands) {
    auto &op = i.second;
    const mds_gid_t op_mds_gid = op.mds_gid;
    if (mdsmap->is_dne_gid(op_mds_gid) || mdsmap->is_laggy_gid(op_mds_gid)) {
      ldout(cct, 1) << __func__ << ": cancelling command op " << i.first << dendl;
      cancel_ops.push_back(i.first);
      if (op.outs) {
        std::ostringstream ss;
        ss << "MDS " << op_mds_gid << " went away";
        *(op.outs) = ss.str();
      }
      op.con->mark_down();
      if (op.on_finish) {
        op.on_finish->complete(-ETIMEDOUT);
      }
    }
  }

  for (std::list<ceph_tid_t>::iterator i = cancel_ops.begin();
       i != cancel_ops.end(); ++i) {
    command_table.erase(*i);
  }

  // reset session
  for (auto p = mds_sessions.begin(); p != mds_sessions.end(); ) {
    mds_rank_t mds = p->first;
    MetaSession *session = &p->second;
    ++p;

    int oldstate = oldmap->get_state(mds);
    int newstate = mdsmap->get_state(mds);
    if (!mdsmap->is_up(mds)) {
      session->con->mark_down();
    } else if (mdsmap->get_addrs(mds) != session->addrs) {
      old_inc = oldmap->get_incarnation(mds);
      new_inc = mdsmap->get_incarnation(mds);
      if (old_inc != new_inc) {
        ldout(cct, 1) << "mds incarnation changed from "
		      << old_inc << " to " << new_inc << dendl;
        oldstate = MDSMap::STATE_NULL;
      }
      session->con->mark_down();
      session->addrs = mdsmap->get_addrs(mds);
      // When new MDS starts to take over, notify kernel to trim unused entries
      // in its dcache/icache. Hopefully, the kernel will release some unused
      // inodes before the new MDS enters reconnect state.
      trim_cache_for_reconnect(session);
    } else if (oldstate == newstate)
      continue;  // no change
    
    session->mds_state = newstate;
    if (newstate == MDSMap::STATE_RECONNECT) {
      session->con = messenger->connect_to_mds(session->addrs);
      send_reconnect(session);
    } else if (newstate > MDSMap::STATE_RECONNECT) {
      if (oldstate < MDSMap::STATE_RECONNECT) {
	ldout(cct, 1) << "we may miss the MDSMap::RECONNECT, close mds session ... " << dendl;
	_closed_mds_session(session);
	continue;
      }
      if (newstate >= MDSMap::STATE_ACTIVE) {
	if (oldstate < MDSMap::STATE_ACTIVE) {
	  // kick new requests
	  kick_requests(session);
	  kick_flushing_caps(session);
	  signal_context_list(session->waiting_for_open);
	  wake_up_session_caps(session, true);
	}
	connect_mds_targets(mds);
      }
    } else if (newstate == MDSMap::STATE_NULL &&
	       mds >= mdsmap->get_max_mds()) {
      _closed_mds_session(session);
    }
  }

  // kick any waiting threads
  signal_cond_list(waiting_for_mdsmap);

  monclient->sub_got("mdsmap", mdsmap->get_epoch());
}

void Client::send_reconnect(MetaSession *session)
{
  mds_rank_t mds = session->mds_num;
  ldout(cct, 10) << __func__ << " to mds." << mds << dendl;

  // trim unused caps to reduce MDS's cache rejoin time
  trim_cache_for_reconnect(session);

  session->readonly = false;

  session->release.reset();

  // reset my cap seq number
  session->seq = 0;
  //connect to the mds' offload targets
  connect_mds_targets(mds);
  //make sure unsafe requests get saved
  resend_unsafe_requests(session);

  early_kick_flushing_caps(session);

  auto m = make_message<MClientReconnect>();
  bool allow_multi = session->mds_features.test(CEPHFS_FEATURE_MULTI_RECONNECT);

  // i have an open session.
  ceph::unordered_set<inodeno_t> did_snaprealm;
  for (ceph::unordered_map<vinodeno_t, Inode*>::iterator p = inode_map.begin();
       p != inode_map.end();
       ++p) {
    Inode *in = p->second;
    auto it = in->caps.find(mds);
    if (it != in->caps.end()) {
      if (allow_multi &&
	  m->get_approx_size() >=
	  static_cast<size_t>((std::numeric_limits<int>::max() >> 1))) {
	m->mark_more();
	session->con->send_message2(std::move(m));

	m = make_message<MClientReconnect>();
      }

      Cap &cap = it->second;
      ldout(cct, 10) << " caps on " << p->first
	       << " " << ccap_string(cap.issued)
	       << " wants " << ccap_string(in->caps_wanted())
	       << dendl;
      filepath path;
      in->make_long_path(path);
      ldout(cct, 10) << "    path " << path << dendl;

      bufferlist flockbl;
      _encode_filelocks(in, flockbl);

      cap.seq = 0;  // reset seq.
      cap.issue_seq = 0;  // reset seq.
      cap.mseq = 0;  // reset seq.
      // cap gen should catch up with session cap_gen
      if (cap.gen < session->cap_gen) {
	cap.gen = session->cap_gen;
	cap.issued = cap.implemented = CEPH_CAP_PIN;
      } else {
	cap.issued = cap.implemented;
      }
      snapid_t snap_follows = 0;
      if (!in->cap_snaps.empty())
	snap_follows = in->cap_snaps.begin()->first;

      m->add_cap(p->first.ino, 
		 cap.cap_id,
		 path.get_ino(), path.get_path(),   // ino
		 in->caps_wanted(), // wanted
		 cap.issued,     // issued
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

  if (!allow_multi)
    m->set_encoding_version(0); // use connection features to choose encoding
  session->con->send_message2(std::move(m));

  mount_cond.notify_all();

  if (session->reclaim_state == MetaSession::RECLAIMING)
    signal_cond_list(waiting_for_reclaim);
}


void Client::kick_requests(MetaSession *session)
{
  ldout(cct, 10) << __func__ << " for mds." << session->mds_num << dendl;
  for (map<ceph_tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end();
       ++p) {
    MetaRequest *req = p->second;
    if (req->got_unsafe)
      continue;
    if (req->aborted()) {
      if (req->caller_cond) {
	req->kick = true;
	req->caller_cond->notify_all();
      }
      continue;
    }
    if (req->retry_attempt > 0)
      continue; // new requests only
    if (req->mds == session->mds_num) {
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
    if (req->aborted())
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
  for (const auto &p : mds_sessions) {
    const MetaSession &s = p.second;
    if (!s.unsafe_requests.empty()) {
      MetaRequest *req = s.unsafe_requests.back();
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
  ldout(cct, 10) << __func__ << " for mds." << session->mds_num << dendl;
  for (map<ceph_tid_t, MetaRequest*>::iterator p = mds_requests.begin();
       p != mds_requests.end(); ) {
    MetaRequest *req = p->second;
    ++p;
    if (req->mds == session->mds_num) {
      if (req->caller_cond) {
	req->kick = true;
	req->caller_cond->notify_all();
      }
      req->item.remove_myself();
      if (req->got_unsafe) {
	lderr(cct) << __func__ << " removing unsafe request " << req->get_tid() << dendl;
	req->unsafe_item.remove_myself();
	if (is_dir_operation(req)) {
	  Inode *dir = req->inode();
	  assert(dir);
	  dir->set_async_err(-EIO);
	  lderr(cct) << "kick_requests_closed drop req of inode(dir) : "
		     <<  dir->ino  << " " << req->get_tid() << dendl;
	  req->unsafe_dir_item.remove_myself();
	}
	if (req->target) {
	  InodeRef &in = req->target;
	  in->set_async_err(-EIO);
	  lderr(cct) << "kick_requests_closed drop req of inode : "
		     <<  in->ino  << " " << req->get_tid() << dendl;
	  req->unsafe_target_item.remove_myself();
	}
	signal_cond_list(req->waitfor_safe);
	unregister_request(req);
      }
    }
  }
  ceph_assert(session->requests.empty());
  ceph_assert(session->unsafe_requests.empty());
}




/************
 * leases
 */

void Client::got_mds_push(MetaSession *s)
{
  s->seq++;
  ldout(cct, 10) << " mds." << s->mds_num << " seq now " << s->seq << dendl;
  if (s->state == MetaSession::STATE_CLOSING) {
    s->con->send_message2(make_message<MClientSession>(CEPH_SESSION_REQUEST_CLOSE, s->seq));
  }
}

void Client::handle_lease(const MConstRef<MClientLease>& m)
{
  ldout(cct, 10) << __func__ << " " << *m << dendl;

  ceph_assert(m->get_action() == CEPH_MDS_LEASE_REVOKE);

  mds_rank_t mds = mds_rank_t(m->get_source().num());
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
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

  if (m->get_mask() & CEPH_LEASE_VALID) {
    if (!in->dir || in->dir->dentries.count(m->dname) == 0) {
      ldout(cct, 10) << " don't have dir|dentry " << m->get_ino() << "/" << m->dname <<dendl;
      goto revoke;
    }
    Dentry *dn = in->dir->dentries[m->dname];
    ldout(cct, 10) << " revoked DN lease on " << dn << dendl;
    dn->lease_mds = -1;
  }

 revoke:
  {
    auto reply = make_message<MClientLease>(CEPH_MDS_LEASE_RELEASE, seq,
					    m->get_mask(), m->get_ino(),
					    m->get_first(), m->get_last(), m->dname);
    m->get_connection()->send_message2(std::move(reply));
  }
}

void Client::put_inode(Inode *in, int n)
{
  ldout(cct, 10) << __func__ << " on " << *in << dendl;
  int left = in->_put(n);
  if (left == 0) {
    // release any caps
    remove_all_caps(in);

    ldout(cct, 10) << __func__ << " deleting " << *in << dendl;
    bool unclean = objectcacher->release_set(&in->oset);
    ceph_assert(!unclean);
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
  ldout(cct, 15) << __func__ << " dir " << dir << " on " << in << dendl;
  ceph_assert(dir->is_empty());
  ceph_assert(in->dir == dir);
  ceph_assert(in->dentries.size() < 2);     // dirs can't be hard-linked
  if (!in->dentries.empty())
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
    dn = new Dentry(dir, name);

    lru.lru_insert_mid(dn);    // mid or top?

    ldout(cct, 15) << "link dir " << dir->parent_inode << " '" << name << "' to inode " << in
		   << " dn " << dn << " (new dn)" << dendl;
  } else {
    ceph_assert(!dn->inode);
    ldout(cct, 15) << "link dir " << dir->parent_inode << " '" << name << "' to inode " << in
		   << " dn " << dn << " (old dn)" << dendl;
  }

  if (in) {    // link to inode
    InodeRef tmp_ref;
    // only one parent for directories!
    if (in->is_dir() && !in->dentries.empty()) {
      tmp_ref = in; // prevent unlink below from freeing the inode.
      Dentry *olddn = in->get_first_parent();
      ceph_assert(olddn->dir != dir || olddn->name != name);
      Inode *old_diri = olddn->dir->parent_inode;
      old_diri->dir_release_count++;
      clear_dir_complete_and_ordered(old_diri, true);
      unlink(olddn, true, true);  // keep dir, dentry
    }

    dn->link(in);
    ldout(cct, 20) << "link  inode " << in << " parents now " << in->dentries << dendl;
  }
  
  return dn;
}

void Client::unlink(Dentry *dn, bool keepdir, bool keepdentry)
{
  InodeRef in(dn->inode);
  ldout(cct, 15) << "unlink dir " << dn->dir->parent_inode << " '" << dn->name << "' dn " << dn
		 << " inode " << dn->inode << dendl;

  // unlink from inode
  if (dn->inode) {
    dn->unlink();
    ldout(cct, 20) << "unlink  inode " << in << " parents now " << in->dentries << dendl;
  }

  if (keepdentry) {
    dn->lease_mds = -1;
  } else {
    ldout(cct, 15) << "unlink  removing '" << dn->name << "' dn " << dn << dendl;

    // unlink from dir
    Dir *dir = dn->dir;
    dn->detach();

    // delete den
    lru.lru_remove(dn);
    dn->put();

    if (dir->is_empty() && !keepdir)
      close_dir(dir);
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
  void finish(int r) override {
    ceph_assert(ceph_mutex_is_locked_by_me(client->client_lock));
    if (r != 0) {
      client_t const whoami = client->whoami;  // For the benefit of ldout prefix
      ldout(client->cct, 1) << "I/O error from flush on inode " << inode
        << " 0x" << std::hex << inode->ino << std::dec
        << ": " << r << "(" << cpp_strerror(r) << ")" << dendl;
      inode->set_async_err(r);
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
    ldout(cct, 5) << __func__ << " got first FILE_BUFFER ref on " << *in << dendl;
    in->get();
  }
  if ((cap & CEPH_CAP_FILE_CACHE) &&
      in->cap_refs[CEPH_CAP_FILE_CACHE] == 0) {
    ldout(cct, 5) << __func__ << " got first FILE_CACHE ref on " << *in << dendl;
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
	  in->cap_snaps.rbegin()->second.writing) {
	ldout(cct, 10) << __func__ << " finishing pending cap_snap on " << *in << dendl;
	in->cap_snaps.rbegin()->second.writing = 0;
	finish_cap_snap(in, in->cap_snaps.rbegin()->second, get_caps_used(in));
	signal_cond_list(in->waitfor_caps);  // wake up blocked sync writers
      }
      if (last & CEPH_CAP_FILE_BUFFER) {
	for (auto &p : in->cap_snaps)
	  p.second.dirty_data = 0;
	signal_cond_list(in->waitfor_commit);
	ldout(cct, 5) << __func__ << " dropped last FILE_BUFFER ref on " << *in << dendl;
	++put_nref;
      }
    }
    if (last & CEPH_CAP_FILE_CACHE) {
      ldout(cct, 5) << __func__ << " dropped last FILE_CACHE ref on " << *in << dendl;
      ++put_nref;
    }
    if (drop)
      check_caps(in, 0);
    if (put_nref)
      put_inode(in, put_nref);
  }
}

int Client::get_caps(Fh *fh, int need, int want, int *phave, loff_t endoff)
{
  Inode *in = fh->inode.get();

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

    if ((fh->mode & CEPH_FILE_MODE_WR) && fh->gen != fd_gen)
      return -EBADF;

    if ((in->flags & I_ERROR_FILELOCK) && fh->has_any_filelocks())
      return -EIO;

    int implemented;
    int have = in->caps_issued(&implemented);

    bool waitfor_caps = false;
    bool waitfor_commit = false;

    if (have & need & CEPH_CAP_FILE_WR) {
      if (endoff > 0) {
	 if ((endoff >= (loff_t)in->max_size ||
	      endoff > (loff_t)(in->size << 1)) &&
	     endoff > (loff_t)in->wanted_max_size) {
	   ldout(cct, 10) << "wanted_max_size " << in->wanted_max_size << " -> " << endoff << dendl;
	   in->wanted_max_size = endoff;
	 }
	 if (in->wanted_max_size > in->max_size &&
	     in->wanted_max_size > in->requested_max_size)
	   check_caps(in, 0);
      }

      if (endoff >= 0 && endoff > (loff_t)in->max_size) {
	ldout(cct, 10) << "waiting on max_size, endoff " << endoff << " max_size " << in->max_size << " on " << *in << dendl;
	waitfor_caps = true;
      }
      if (!in->cap_snaps.empty()) {
	if (in->cap_snaps.rbegin()->second.writing) {
	  ldout(cct, 10) << "waiting on cap_snap write to complete" << dendl;
	  waitfor_caps = true;
	}
	for (auto &p : in->cap_snaps) {
	  if (p.second.dirty_data) {
	    waitfor_commit = true;
	    break;
	  }
        }
	if (waitfor_commit) {
	  _flush(in, new C_Client_FlushComplete(this, in));
	  ldout(cct, 10) << "waiting for WRBUFFER to get dropped" << dendl;
	}
      }
    }

    if (!waitfor_caps && !waitfor_commit) {
      if ((have & need) == need) {
	int revoking = implemented & ~have;
	ldout(cct, 10) << "get_caps " << *in << " have " << ccap_string(have)
		 << " need " << ccap_string(need) << " want " << ccap_string(want)
		 << " revoking " << ccap_string(revoking)
		 << dendl;
	if ((revoking & want) == 0) {
	  *phave = need | (have & want);
	  in->get_cap_ref(need);
	  return 0;
	}
      }
      ldout(cct, 10) << "waiting for caps " << *in << " need " << ccap_string(need) << " want " << ccap_string(want) << dendl;
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
      if (!(file_wanted & ~mds_wanted))
	in->flags &= ~I_CAP_DROPPED;
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
  ldout(cct, 10) << __func__ << " on " << *in << dendl;
  in->hold_caps_until = ceph_clock_now();
  in->hold_caps_until += cct->_conf->client_caps_release_delay;
  delayed_list.push_back(&in->delay_cap_item);
}

void Client::send_cap(Inode *in, MetaSession *session, Cap *cap,
		      int flags, int used, int want, int retain,
		      int flush, ceph_tid_t flush_tid)
{
  int held = cap->issued | cap->implemented;
  int revoking = cap->implemented & ~cap->issued;
  retain &= ~revoking;
  int dropping = cap->issued & ~retain;
  int op = CEPH_CAP_OP_UPDATE;

  ldout(cct, 10) << __func__ << " " << *in
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
  
  auto m = make_message<MClientCaps>(op,
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
    encode(in->xattrs, m->xattrbl);
    m->head.xattr_version = in->xattr_version;
  }
  
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

  if (!(flags & MClientCaps::FLAG_PENDING_CAPSNAP) &&
      !in->cap_snaps.empty() &&
      in->cap_snaps.rbegin()->second.flush_tid == 0)
    flags |= MClientCaps::FLAG_PENDING_CAPSNAP;
  m->flags = flags;

  if (flush & CEPH_CAP_FILE_WR) {
    m->inline_version = in->inline_version;
    m->inline_data = in->inline_data;
  }

  in->reported_size = in->size;
  m->set_snap_follows(follows);
  cap->wanted = want;
  if (cap == in->auth_cap) {
    if (want & CEPH_CAP_ANY_FILE_WR) {
      m->set_max_size(in->wanted_max_size);
      in->requested_max_size = in->wanted_max_size;
      ldout(cct, 15) << "auth cap, requesting max_size " << in->requested_max_size << dendl;
    } else {
      in->requested_max_size = 0;
      ldout(cct, 15) << "auth cap, reset requested_max_size due to not wanting any file write cap" << dendl;
    }
  }

  if (!session->flushing_caps_tids.empty())
    m->set_oldest_flush_tid(*session->flushing_caps_tids.begin());

  session->con->send_message2(std::move(m));
}

static bool is_max_size_approaching(Inode *in)
{
  /* mds will adjust max size according to the reported size */
  if (in->flushing_caps & CEPH_CAP_FILE_WR)
    return false;
  if (in->size >= in->max_size)
    return true;
  /* half of previous max_size increment has been used */
  if (in->max_size > in->reported_size &&
      (in->size << 1) >= in->max_size + in->reported_size)
    return true;
  return false;
}

static int adjust_caps_used_for_lazyio(int used, int issued, int implemented)
{
  if (!(used & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER)))
    return used;
  if (!(implemented & CEPH_CAP_FILE_LAZYIO))
    return used;

  if (issued & CEPH_CAP_FILE_LAZYIO) {
    if (!(issued & CEPH_CAP_FILE_CACHE)) {
      used &= ~CEPH_CAP_FILE_CACHE;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
    if (!(issued & CEPH_CAP_FILE_BUFFER)) {
      used &= ~CEPH_CAP_FILE_BUFFER;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
  } else {
    if (!(implemented & CEPH_CAP_FILE_CACHE)) {
      used &= ~CEPH_CAP_FILE_CACHE;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
    if (!(implemented & CEPH_CAP_FILE_BUFFER)) {
      used &= ~CEPH_CAP_FILE_BUFFER;
      used |= CEPH_CAP_FILE_LAZYIO;
    }
  }
  return used;
}

/**
 * check_caps
 *
 * Examine currently used and wanted versus held caps. Release, flush or ack
 * revoked caps to the MDS as appropriate.
 *
 * @param in the inode to check
 * @param flags flags to apply to cap check
 */
void Client::check_caps(Inode *in, unsigned flags)
{
  unsigned wanted = in->caps_wanted();
  unsigned used = get_caps_used(in);
  unsigned cap_used;

  int implemented;
  int issued = in->caps_issued(&implemented);
  int revoking = implemented & ~issued;

  int orig_used = used;
  used = adjust_caps_used_for_lazyio(used, issued, implemented);

  int retain = wanted | used | CEPH_CAP_PIN;
  if (!unmounting && in->nlink > 0) {
    if (wanted) {
      retain |= CEPH_CAP_ANY;
    } else if (in->is_dir() &&
	       (issued & CEPH_CAP_FILE_SHARED) &&
	       (in->flags & I_COMPLETE)) {
      // we do this here because we don't want to drop to Fs (and then
      // drop the Fs if we do a create!) if that alone makes us send lookups
      // to the MDS. Doing it in in->caps_wanted() has knock-on effects elsewhere
      wanted = CEPH_CAP_ANY_SHARED | CEPH_CAP_FILE_EXCL;
      retain |= wanted;
    } else {
      retain |= CEPH_CAP_ANY_SHARED;
      // keep RD only if we didn't have the file open RW,
      // because then the mds would revoke it anyway to
      // journal max_size=0.
      if (in->max_size == 0)
	retain |= CEPH_CAP_ANY_RD;
    }
  }

  ldout(cct, 10) << __func__ << " on " << *in
	   << " wanted " << ccap_string(wanted)
	   << " used " << ccap_string(used)
	   << " issued " << ccap_string(issued)
	   << " revoking " << ccap_string(revoking)
	   << " flags=" << flags
	   << dendl;

  if (in->snapid != CEPH_NOSNAP)
    return; //snap caps last forever, can't write

  if (in->caps.empty())
    return;   // guard if at end of func

  if (!(orig_used & CEPH_CAP_FILE_BUFFER) &&
      (revoking & used & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO))) {
    if (_release(in))
      used &= ~(CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO);
  }


  for (auto &p : in->caps) {
    mds_rank_t mds = p.first;
    Cap &cap = p.second;

    MetaSession *session = &mds_sessions.at(mds);

    cap_used = used;
    if (in->auth_cap && &cap != in->auth_cap)
      cap_used &= ~in->auth_cap->issued;

    revoking = cap.implemented & ~cap.issued;
    
    ldout(cct, 10) << " cap mds." << mds
	     << " issued " << ccap_string(cap.issued)
	     << " implemented " << ccap_string(cap.implemented)
	     << " revoking " << ccap_string(revoking) << dendl;

    if (in->wanted_max_size > in->max_size &&
	in->wanted_max_size > in->requested_max_size &&
	&cap == in->auth_cap)
      goto ack;

    /* approaching file_max? */
    if ((cap.issued & CEPH_CAP_FILE_WR) &&
	&cap == in->auth_cap &&
	is_max_size_approaching(in)) {
      ldout(cct, 10) << "size " << in->size << " approaching max_size " << in->max_size
		     << ", reported " << in->reported_size << dendl;
      goto ack;
    }

    /* completed revocation? */
    if (revoking && (revoking & cap_used) == 0) {
      ldout(cct, 10) << "completed revocation of " << ccap_string(cap.implemented & ~cap.issued) << dendl;
      goto ack;
    }

    /* want more caps from mds? */
    if (wanted & ~(cap.wanted | cap.issued))
      goto ack;

    if (!revoking && unmounting && (cap_used == 0))
      goto ack;

    if ((cap.issued & ~retain) == 0 && // and we don't have anything we wouldn't like
	!in->dirty_caps)               // and we have no dirty caps
      continue;

    if (!(flags & CHECK_CAPS_NODELAY)) {
      ldout(cct, 10) << "delaying cap release" << dendl;
      cap_delay_requeue(in);
      continue;
    }

  ack:
    if (&cap == in->auth_cap) {
      if (in->flags & I_KICK_FLUSH) {
	ldout(cct, 20) << " reflushing caps (check_caps) on " << *in
		       << " to mds." << mds << dendl;
	kick_flushing_caps(in, session);
      }
      if (!in->cap_snaps.empty() &&
	  in->cap_snaps.rbegin()->second.flush_tid == 0)
	flush_snaps(in);
    }

    int flushing;
    int msg_flags = 0;
    ceph_tid_t flush_tid;
    if (in->auth_cap == &cap && in->dirty_caps) {
      flushing = mark_caps_flushing(in, &flush_tid);
      if (flags & CHECK_CAPS_SYNCHRONOUS)
	msg_flags |= MClientCaps::FLAG_SYNC;
    } else {
      flushing = 0;
      flush_tid = 0;
    }

    send_cap(in, session, &cap, msg_flags, cap_used, wanted, retain,
	     flushing, flush_tid);
  }
}


void Client::queue_cap_snap(Inode *in, SnapContext& old_snapc)
{
  int used = get_caps_used(in);
  int dirty = in->caps_dirty();
  ldout(cct, 10) << __func__ << " " << *in << " snapc " << old_snapc << " used " << ccap_string(used) << dendl;

  if (in->cap_snaps.size() &&
      in->cap_snaps.rbegin()->second.writing) {
    ldout(cct, 10) << __func__ << " already have pending cap_snap on " << *in << dendl;
    return;
  } else if (in->caps_dirty() ||
            (used & CEPH_CAP_FILE_WR) ||
	     (dirty & CEPH_CAP_ANY_WR)) {
    const auto &capsnapem = in->cap_snaps.emplace(std::piecewise_construct, std::make_tuple(old_snapc.seq), std::make_tuple(in));
    ceph_assert(capsnapem.second); /* element inserted */
    CapSnap &capsnap = capsnapem.first->second;
    capsnap.context = old_snapc;
    capsnap.issued = in->caps_issued();
    capsnap.dirty = in->caps_dirty();
    
    capsnap.dirty_data = (used & CEPH_CAP_FILE_BUFFER);
    
    capsnap.uid = in->uid;
    capsnap.gid = in->gid;
    capsnap.mode = in->mode;
    capsnap.btime = in->btime;
    capsnap.xattrs = in->xattrs;
    capsnap.xattr_version = in->xattr_version;
    capsnap.cap_dirtier_uid = in->cap_dirtier_uid;
    capsnap.cap_dirtier_gid = in->cap_dirtier_gid;
 
    if (used & CEPH_CAP_FILE_WR) {
      ldout(cct, 10) << __func__ << " WR used on " << *in << dendl;
      capsnap.writing = 1;
    } else {
      finish_cap_snap(in, capsnap, used);
    }
  } else {
    ldout(cct, 10) << __func__ << " not dirty|writing on " << *in << dendl;
  }
}

void Client::finish_cap_snap(Inode *in, CapSnap &capsnap, int used)
{
  ldout(cct, 10) << __func__ << " " << *in << " capsnap " << (void *)&capsnap << " used " << ccap_string(used) << dendl;
  capsnap.size = in->size;
  capsnap.mtime = in->mtime;
  capsnap.atime = in->atime;
  capsnap.ctime = in->ctime;
  capsnap.time_warp_seq = in->time_warp_seq;
  capsnap.change_attr = in->change_attr;
  capsnap.dirty |= in->caps_dirty();

  /* Only reset it if it wasn't set before */
  if (capsnap.cap_dirtier_uid == -1) {
    capsnap.cap_dirtier_uid = in->cap_dirtier_uid;
    capsnap.cap_dirtier_gid = in->cap_dirtier_gid;
  }

  if (capsnap.dirty & CEPH_CAP_FILE_WR) {
    capsnap.inline_data = in->inline_data;
    capsnap.inline_version = in->inline_version;
  }

  if (used & CEPH_CAP_FILE_BUFFER) {
    ldout(cct, 10) << __func__ << " " << *in << " cap_snap " << &capsnap << " used " << used
	     << " WRBUFFER, delaying" << dendl;
  } else {
    capsnap.dirty_data = 0;
    flush_snaps(in);
  }
}

void Client::_flushed_cap_snap(Inode *in, snapid_t seq)
{
  ldout(cct, 10) << __func__ << " seq " << seq << " on " << *in << dendl;
  in->cap_snaps.at(seq).dirty_data = 0;
  flush_snaps(in);
}

void Client::send_flush_snap(Inode *in, MetaSession *session,
			     snapid_t follows, CapSnap& capsnap)
{
  auto m = make_message<MClientCaps>(CEPH_CAP_OP_FLUSHSNAP,
				     in->ino, in->snaprealm->ino, 0,
				     in->auth_cap->mseq, cap_epoch_barrier);
  m->caller_uid = capsnap.cap_dirtier_uid;
  m->caller_gid = capsnap.cap_dirtier_gid;

  m->set_client_tid(capsnap.flush_tid);
  m->head.snap_follows = follows;

  m->head.caps = capsnap.issued;
  m->head.dirty = capsnap.dirty;

  m->head.uid = capsnap.uid;
  m->head.gid = capsnap.gid;
  m->head.mode = capsnap.mode;
  m->btime = capsnap.btime;

  m->size = capsnap.size;

  m->head.xattr_version = capsnap.xattr_version;
  encode(capsnap.xattrs, m->xattrbl);

  m->ctime = capsnap.ctime;
  m->btime = capsnap.btime;
  m->mtime = capsnap.mtime;
  m->atime = capsnap.atime;
  m->time_warp_seq = capsnap.time_warp_seq;
  m->change_attr = capsnap.change_attr;

  if (capsnap.dirty & CEPH_CAP_FILE_WR) {
    m->inline_version = in->inline_version;
    m->inline_data = in->inline_data;
  }

  ceph_assert(!session->flushing_caps_tids.empty());
  m->set_oldest_flush_tid(*session->flushing_caps_tids.begin());

  session->con->send_message2(std::move(m));
}

void Client::flush_snaps(Inode *in)
{
  ldout(cct, 10) << "flush_snaps on " << *in << dendl;
  ceph_assert(in->cap_snaps.size());

  // pick auth mds
  ceph_assert(in->auth_cap);
  MetaSession *session = in->auth_cap->session;

  for (auto &p : in->cap_snaps) {
    CapSnap &capsnap = p.second;
    // only do new flush
    if (capsnap.flush_tid > 0)
      continue;

    ldout(cct, 10) << "flush_snaps mds." << session->mds_num
	     << " follows " << p.first
	     << " size " << capsnap.size
	     << " mtime " << capsnap.mtime
	     << " dirty_data=" << capsnap.dirty_data
	     << " writing=" << capsnap.writing
	     << " on " << *in << dendl;
    if (capsnap.dirty_data || capsnap.writing)
      break;
    
    capsnap.flush_tid = ++last_flush_tid;
    session->flushing_caps_tids.insert(capsnap.flush_tid);
    in->flushing_cap_tids[capsnap.flush_tid] = 0;
    if (!in->flushing_cap_item.is_on_list())
      session->flushing_caps.push_back(&in->flushing_cap_item);

    send_flush_snap(in, session, p.first, capsnap);
  }
}

void Client::wait_on_list(list<ceph::condition_variable*>& ls)
{
  ceph::condition_variable cond;
  ls.push_back(&cond);
  std::unique_lock l{client_lock, std::adopt_lock};
  cond.wait(l);
  l.release();
  ls.remove(&cond);
}

void Client::signal_cond_list(list<ceph::condition_variable*>& ls)
{
  for (auto cond : ls) {
    cond->notify_all();
  }
}

void Client::wait_on_context_list(list<Context*>& ls)
{
  ceph::condition_variable cond;
  bool done = false;
  int r;
  ls.push_back(new C_Cond(cond, &done, &r));
  std::unique_lock l{client_lock, std::adopt_lock};
  cond.wait(l, [&done] { return done;});
  l.release();
}

void Client::signal_context_list(list<Context*>& ls)
{
  while (!ls.empty()) {
    ls.front()->complete(0);
    ls.pop_front();
  }
}

void Client::wake_up_session_caps(MetaSession *s, bool reconnect)
{
  for (const auto &cap : s->caps) {
    auto &in = cap->inode;
    if (reconnect) {
      in.requested_max_size = 0;
      in.wanted_max_size = 0;
    } else {
      if (cap->gen < s->cap_gen) {
	// mds did not re-issue stale cap.
	cap->issued = cap->implemented = CEPH_CAP_PIN;
	// make sure mds knows what we want.
	if (in.caps_file_wanted() & ~cap->wanted)
	  in.flags |= I_CAP_DROPPED;
      }
    }
    signal_cond_list(in.waitfor_caps);
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
  void finish(int r) override {
    // _async_invalidate takes the lock when it needs to, call this back from outside of lock.
    ceph_assert(ceph_mutex_is_not_locked_by_me(client->client_lock));
    client->_async_invalidate(ino, offset, length);
  }
};

void Client::_async_invalidate(vinodeno_t ino, int64_t off, int64_t len)
{
  if (unmounting)
    return;
  ldout(cct, 10) << __func__ << " " << ino << " " << off << "~" << len << dendl;
  ino_invalidate_cb(callback_handle, ino, off, len);
}

void Client::_schedule_invalidate_callback(Inode *in, int64_t off, int64_t len) {

  if (ino_invalidate_cb)
    // we queue the invalidate, which calls the callback and decrements the ref
    async_ino_invalidator.queue(new C_Client_CacheInvalidate(this, in, off, len));
}

void Client::_invalidate_inode_cache(Inode *in)
{
  ldout(cct, 10) << __func__ << " " << *in << dendl;

  // invalidate our userspace inode cache
  if (cct->_conf->client_oc) {
    objectcacher->release_set(&in->oset);
    if (!objectcacher->set_is_empty(&in->oset))
      lderr(cct) << "failed to invalidate cache for " << *in << dendl;
  }

  _schedule_invalidate_callback(in, 0, 0);
}

void Client::_invalidate_inode_cache(Inode *in, int64_t off, int64_t len)
{
  ldout(cct, 10) << __func__ << " " << *in << " " << off << "~" << len << dendl;

  // invalidate our userspace inode cache
  if (cct->_conf->client_oc) {
    vector<ObjectExtent> ls;
    Striper::file_to_extents(cct, in->ino, &in->layout, off, len, in->truncate_size, ls);
    objectcacher->discard_writeback(&in->oset, ls, nullptr);
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
    ldout(cct, 8) << __func__ << ": FULL, purging for ENOSPC" << dendl;
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
  ceph_assert(ceph_mutex_is_locked(client_lock));
  if (!in->oset.dirty_or_tx) {
    ldout(cct, 10) << " nothing to flush" << dendl;
    return;
  }

  C_SaferCond onflush("Client::_flush_range flock");
  bool ret = objectcacher->file_flush(&in->oset, &in->layout, in->snaprealm->get_snap_context(),
				      offset, size, &onflush);
  if (!ret) {
    // wait for flush
    client_lock.unlock();
    onflush.wait();
    client_lock.lock();
  }
}

void Client::flush_set_callback(ObjectCacher::ObjectSet *oset)
{
  //  std::lock_guard l(client_lock);
  ceph_assert(ceph_mutex_is_locked(client_lock));   // will be called via dispatch() -> objecter -> ...
  Inode *in = static_cast<Inode *>(oset->parent);
  ceph_assert(in);
  _flushed(in);
}

void Client::_flushed(Inode *in)
{
  ldout(cct, 10) << "_flushed " << *in << dendl;

  put_cap_ref(in, CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER);
}



// checks common to add_update_cap, handle_cap_grant
void Client::check_cap_issue(Inode *in, unsigned issued)
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
			    unsigned issued, unsigned wanted, unsigned seq, unsigned mseq,
			    inodeno_t realm, int flags, const UserPerm& cap_perms)
{
  if (!in->is_any_caps()) {
    ceph_assert(in->snaprealm == 0);
    in->snaprealm = get_snap_realm(realm);
    in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
    ldout(cct, 15) << __func__ << " first one, opened snaprealm " << in->snaprealm << dendl;
  } else {
    ceph_assert(in->snaprealm);
    if ((flags & CEPH_CAP_FLAG_AUTH) &&
	realm != inodeno_t(-1) && in->snaprealm->ino != realm) {
      in->snaprealm_item.remove_myself();
      auto oldrealm = in->snaprealm;
      in->snaprealm = get_snap_realm(realm);
      in->snaprealm->inodes_with_caps.push_back(&in->snaprealm_item);
      put_snap_realm(oldrealm);
    }
  }

  mds_rank_t mds = mds_session->mds_num;
  const auto &capem = in->caps.emplace(std::piecewise_construct, std::forward_as_tuple(mds), std::forward_as_tuple(*in, mds_session));
  Cap &cap = capem.first->second;
  if (!capem.second) {
    if (cap.gen < mds_session->cap_gen)
      cap.issued = cap.implemented = CEPH_CAP_PIN;

    /*
     * auth mds of the inode changed. we received the cap export
     * message, but still haven't received the cap import message.
     * handle_cap_export() updated the new auth MDS' cap.
     *
     * "ceph_seq_cmp(seq, cap->seq) <= 0" means we are processing
     * a message that was send before the cap import message. So
     * don't remove caps.
     */
    if (ceph_seq_cmp(seq, cap.seq) <= 0) {
      if (&cap != in->auth_cap)
         ldout(cct, 0) << "WARNING: " <<  "inode " << *in << " caps on mds." << mds << " != auth_cap." << dendl;

      ceph_assert(cap.cap_id == cap_id);
      seq = cap.seq;
      mseq = cap.mseq;
      issued |= cap.issued;
      flags |= CEPH_CAP_FLAG_AUTH;
    }
  }

  check_cap_issue(in, issued);

  if (flags & CEPH_CAP_FLAG_AUTH) {
    if (in->auth_cap != &cap &&
        (!in->auth_cap || ceph_seq_cmp(in->auth_cap->mseq, mseq) < 0)) {
      if (in->auth_cap && in->flushing_cap_item.is_on_list()) {
	ldout(cct, 10) << __func__ << " changing auth cap: "
		       << "add myself to new auth MDS' flushing caps list" << dendl;
	adjust_session_flushing_caps(in, in->auth_cap->session, mds_session);
      }
      in->auth_cap = &cap;
    }
  }

  unsigned old_caps = cap.issued;
  cap.cap_id = cap_id;
  cap.issued = issued;
  cap.implemented |= issued;
  if (ceph_seq_cmp(mseq, cap.mseq) > 0)
    cap.wanted = wanted;
  else
    cap.wanted |= wanted;
  cap.seq = seq;
  cap.issue_seq = seq;
  cap.mseq = mseq;
  cap.gen = mds_session->cap_gen;
  cap.latest_perms = cap_perms;
  ldout(cct, 10) << __func__ << " issued " << ccap_string(old_caps) << " -> " << ccap_string(cap.issued)
	   << " from mds." << mds
	   << " on " << *in
	   << dendl;

  if ((issued & ~old_caps) && in->auth_cap == &cap) {
    // non-auth MDS is revoking the newly grant caps ?
    for (auto &p : in->caps) {
      if (&p.second == &cap)
	continue;
      if (p.second.implemented & ~p.second.issued & issued) {
	check_caps(in, CHECK_CAPS_NODELAY);
	break;
      }
    }
  }

  if (issued & ~old_caps)
    signal_cond_list(in->waitfor_caps);
}

void Client::remove_cap(Cap *cap, bool queue_release)
{
  auto &in = cap->inode;
  MetaSession *session = cap->session;
  mds_rank_t mds = cap->session->mds_num;

  ldout(cct, 10) << __func__ << " mds." << mds << " on " << in << dendl;
  
  if (queue_release) {
    session->enqueue_cap_release(
      in.ino,
      cap->cap_id,
      cap->issue_seq,
      cap->mseq,
      cap_epoch_barrier);
  }

  if (in.auth_cap == cap) {
    if (in.flushing_cap_item.is_on_list()) {
      ldout(cct, 10) << " removing myself from flushing_cap list" << dendl;
      in.flushing_cap_item.remove_myself();
    }
    in.auth_cap = NULL;
  }
  size_t n = in.caps.erase(mds);
  ceph_assert(n == 1);
  cap = nullptr;

  if (!in.is_any_caps()) {
    ldout(cct, 15) << __func__ << " last one, closing snaprealm " << in.snaprealm << dendl;
    in.snaprealm_item.remove_myself();
    put_snap_realm(in.snaprealm);
    in.snaprealm = 0;
  }
}

void Client::remove_all_caps(Inode *in)
{
  while (!in->caps.empty())
    remove_cap(&in->caps.begin()->second, true);
}

void Client::remove_session_caps(MetaSession *s, int err)
{
  ldout(cct, 10) << __func__ << " mds." << s->mds_num << dendl;

  while (s->caps.size()) {
    Cap *cap = *s->caps.begin();
    InodeRef in(&cap->inode);
    bool dirty_caps = false;
    if (in->auth_cap == cap) {
      dirty_caps = in->dirty_caps | in->flushing_caps;
      in->wanted_max_size = 0;
      in->requested_max_size = 0;
      if (in->has_any_filelocks())
	in->flags |= I_ERROR_FILELOCK;
    }
    auto caps = cap->implemented;
    if (cap->wanted | cap->issued)
      in->flags |= I_CAP_DROPPED;
    remove_cap(cap, false);
    in->cap_snaps.clear();
    if (dirty_caps) {
      lderr(cct) << __func__ << " still has dirty|flushing caps on " << *in << dendl;
      if (in->flushing_caps) {
	num_flushing_caps--;
	in->flushing_cap_tids.clear();
      }
      in->flushing_caps = 0;
      in->mark_caps_clean();
      put_inode(in.get());
    }
    caps &= CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER;
    if (caps && !in->caps_issued_mask(caps, true)) {
      if (err == -EBLACKLISTED) {
	if (in->oset.dirty_or_tx) {
	  lderr(cct) << __func__ << " still has dirty data on " << *in << dendl;
	  in->set_async_err(err);
	}
	objectcacher->purge_set(&in->oset);
      } else {
	objectcacher->release_set(&in->oset);
      }
      _schedule_invalidate_callback(in.get(), 0, 0);
    }

    signal_cond_list(in->waitfor_caps);
  }
  s->flushing_caps_tids.clear();
  sync_cond.notify_all();
}

int Client::_do_remount(bool retry_on_error)
{
  uint64_t max_retries = g_conf().get_val<uint64_t>("mds_max_retries_on_remount_failure");

  errno = 0;
  int r = remount_cb(callback_handle);
  if (r == 0) {
    retries_on_invalidate = 0;
  } else {
    int e = errno;
    client_t whoami = get_nodeid();
    if (r == -1) {
      lderr(cct) <<
          "failed to remount (to trim kernel dentries): "
          "errno = " << e << " (" << strerror(e) << ")" << dendl;
    } else {
      lderr(cct) <<
          "failed to remount (to trim kernel dentries): "
          "return code = " << r << dendl;
    }
    bool should_abort =
      (cct->_conf.get_val<bool>("client_die_on_failed_remount") ||
       cct->_conf.get_val<bool>("client_die_on_failed_dentry_invalidate")) &&
      !(retry_on_error && (++retries_on_invalidate < max_retries));
    if (should_abort && !unmounting) {
      lderr(cct) << "failed to remount for kernel dentry trimming; quitting!" << dendl;
      ceph_abort();
    }
  }
  return r;
}

class C_Client_Remount : public Context  {
private:
  Client *client;
public:
  explicit C_Client_Remount(Client *c) : client(c) {}
  void finish(int r) override {
    ceph_assert(r == 0);
    client->_do_remount(true);
  }
};

void Client::_invalidate_kernel_dcache()
{
  if (unmounting)
    return;
  if (can_invalidate_dentries) {
    if (dentry_invalidate_cb && root->dir) {
      for (ceph::unordered_map<string, Dentry*>::iterator p = root->dir->dentries.begin();
         p != root->dir->dentries.end();
         ++p) {
       if (p->second->inode)
        _schedule_invalidate_dentry_callback(p->second, false);
      }
    }
  } else if (remount_cb) {
    // Hacky:
    // when remounting a file system, linux kernel trims all unused dentries in the fs
    remount_finisher.queue(new C_Client_Remount(this));
  }
}

void Client::_trim_negative_child_dentries(InodeRef& in)
{
  if (!in->is_dir())
    return;

  Dir* dir = in->dir;
  if (dir && dir->dentries.size() == dir->num_null_dentries) {
    for (auto p = dir->dentries.begin(); p != dir->dentries.end(); ) {
      Dentry *dn = p->second;
      ++p;
      ceph_assert(!dn->inode);
      if (dn->lru_is_expireable())
	unlink(dn, true, false);  // keep dir, drop dentry
    }
    if (dir->dentries.empty()) {
      close_dir(dir);
    }
  }

  if (in->flags & I_SNAPDIR_OPEN) {
    InodeRef snapdir = open_snapdir(in.get());
    _trim_negative_child_dentries(snapdir);
  }
}

class C_Client_CacheRelease : public Context  {
private:
  Client *client;
  vinodeno_t ino;
public:
  C_Client_CacheRelease(Client *c, Inode *in) :
    client(c) {
    if (client->use_faked_inos())
      ino = vinodeno_t(in->faked_ino, CEPH_NOSNAP);
    else
      ino = in->vino();
  }
  void finish(int r) override {
    ceph_assert(ceph_mutex_is_not_locked_by_me(client->client_lock));
    client->_async_inode_release(ino);
  }
};

void Client::_async_inode_release(vinodeno_t ino)
{
  if (unmounting)
    return;
  ldout(cct, 10) << __func__ << " " << ino << dendl;
  ino_release_cb(callback_handle, ino);
}

void Client::_schedule_ino_release_callback(Inode *in) {

  if (ino_release_cb)
    // we queue the invalidate, which calls the callback and decrements the ref
    async_ino_releasor.queue(new C_Client_CacheRelease(this, in));
}

void Client::trim_caps(MetaSession *s, uint64_t max)
{
  mds_rank_t mds = s->mds_num;
  size_t caps_size = s->caps.size();
  ldout(cct, 10) << __func__ << " mds." << mds << " max " << max 
    << " caps " << caps_size << dendl;

  uint64_t trimmed = 0;
  auto p = s->caps.begin();
  std::set<Dentry *> to_trim; /* this avoids caps other than the one we're
                               * looking at from getting deleted during traversal. */
  while ((caps_size - trimmed) > max && !p.end()) {
    Cap *cap = *p;
    InodeRef in(&cap->inode);

    // Increment p early because it will be invalidated if cap
    // is deleted inside remove_cap
    ++p;

    if (in->caps.size() > 1 && cap != in->auth_cap) {
      int mine = cap->issued | cap->implemented;
      int oissued = in->auth_cap ? in->auth_cap->issued : 0;
      // disposable non-auth cap
      if (!(get_caps_used(in.get()) & ~oissued & mine)) {
	ldout(cct, 20) << " removing unused, unneeded non-auth cap on " << *in << dendl;
	cap = (remove_cap(cap, true), nullptr);
	trimmed++;
      }
    } else {
      ldout(cct, 20) << " trying to trim dentries for " << *in << dendl;
      _trim_negative_child_dentries(in);
      bool all = true;
      auto q = in->dentries.begin();
      while (q != in->dentries.end()) {
        Dentry *dn = *q;
        ++q;
	if (dn->lru_is_expireable()) {
	  if (can_invalidate_dentries &&
	      dn->dir->parent_inode->ino == MDS_INO_ROOT) {
	    // Only issue one of these per DN for inodes in root: handle
	    // others more efficiently by calling for root-child DNs at
	    // the end of this function.
	    _schedule_invalidate_dentry_callback(dn, true);
	  }
          ldout(cct, 20) << " queueing dentry for trimming: " << dn->name << dendl;
          to_trim.insert(dn);
        } else {
          ldout(cct, 20) << "  not expirable: " << dn->name << dendl;
	  all = false;
        }
      }
      if (all && in->ino != MDS_INO_ROOT) {
        ldout(cct, 20) << __func__ << " counting as trimmed: " << *in << dendl;
	trimmed++;
	_schedule_ino_release_callback(in.get());
      }
    }
  }
  ldout(cct, 20) << " trimming queued dentries: " << dendl;
  for (const auto &dn : to_trim) {
    trim_dentry(dn);
  }
  to_trim.clear();

  caps_size = s->caps.size();
  if (caps_size > (size_t)max)
    _invalidate_kernel_dcache();
}

void Client::force_session_readonly(MetaSession *s)
{
  s->readonly = true;
  for (xlist<Cap*>::iterator p = s->caps.begin(); !p.end(); ++p) {
    auto &in = (*p)->inode;
    if (in.caps_wanted() & CEPH_CAP_FILE_WR)
      signal_cond_list(in.waitfor_caps);
  }
}

int Client::mark_caps_flushing(Inode *in, ceph_tid_t* ptid)
{
  MetaSession *session = in->auth_cap->session;

  int flushing = in->dirty_caps;
  ceph_assert(flushing);

  ceph_tid_t flush_tid = ++last_flush_tid;
  in->flushing_cap_tids[flush_tid] = flushing;

  if (!in->flushing_caps) {
    ldout(cct, 10) << __func__ << " " << ccap_string(flushing) << " " << *in << dendl;
    num_flushing_caps++;
  } else {
    ldout(cct, 10) << __func__ << " (more) " << ccap_string(flushing) << " " << *in << dendl;
  }

  in->flushing_caps |= flushing;
  in->mark_caps_clean();
 
  if (!in->flushing_cap_item.is_on_list())
    session->flushing_caps.push_back(&in->flushing_cap_item);
  session->flushing_caps_tids.insert(flush_tid);

  *ptid = flush_tid;
  return flushing;
}

void Client::adjust_session_flushing_caps(Inode *in, MetaSession *old_s,  MetaSession *new_s)
{
  for (auto &p : in->cap_snaps) {
    CapSnap &capsnap = p.second;
    if (capsnap.flush_tid > 0) {
      old_s->flushing_caps_tids.erase(capsnap.flush_tid);
      new_s->flushing_caps_tids.insert(capsnap.flush_tid);
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

/*
 * Flush all caps back to the MDS. Because the callers generally wait on the
 * result of this function (syncfs and umount cases), we set
 * CHECK_CAPS_SYNCHRONOUS on the last check_caps call.
 */
void Client::flush_caps_sync()
{
  ldout(cct, 10) << __func__ << dendl;
  xlist<Inode*>::iterator p = delayed_list.begin();
  while (!p.end()) {
    unsigned flags = CHECK_CAPS_NODELAY;
    Inode *in = *p;

    ++p;
    delayed_list.pop_front();
    if (p.end() && dirty_list.empty())
      flags |= CHECK_CAPS_SYNCHRONOUS;
    check_caps(in, flags);
  }

  // other caps, too
  p = dirty_list.begin();
  while (!p.end()) {
    unsigned flags = CHECK_CAPS_NODELAY;
    Inode *in = *p;

    ++p;
    if (p.end())
      flags |= CHECK_CAPS_SYNCHRONOUS;
    check_caps(in, flags);
  }
}

void Client::wait_sync_caps(Inode *in, ceph_tid_t want)
{
  while (in->flushing_caps) {
    map<ceph_tid_t, int>::iterator it = in->flushing_cap_tids.begin();
    ceph_assert(it != in->flushing_cap_tids.end());
    if (it->first > want)
      break;
    ldout(cct, 10) << __func__ << " on " << *in << " flushing "
		   << ccap_string(it->second) << " want " << want
		   << " last " << it->first << dendl;
    wait_on_list(in->waitfor_caps);
  }
}

void Client::wait_sync_caps(ceph_tid_t want)
{
 retry:
  ldout(cct, 10) << __func__ << " want " << want  << " (last is " << last_flush_tid << ", "
	   << num_flushing_caps << " total flushing)" << dendl;
  for (auto &p : mds_sessions) {
    MetaSession *s = &p.second;
    if (s->flushing_caps_tids.empty())
	continue;
    ceph_tid_t oldest_tid = *s->flushing_caps_tids.begin();
    if (oldest_tid <= want) {
      ldout(cct, 10) << " waiting on mds." << p.first << " tid " << oldest_tid
		     << " (want " << want << ")" << dendl;
      std::unique_lock l{client_lock, std::adopt_lock};
      sync_cond.wait(l);
      l.release();
      goto retry;
    }
  }
}

void Client::kick_flushing_caps(Inode *in, MetaSession *session)
{
  in->flags &= ~I_KICK_FLUSH;

  Cap *cap = in->auth_cap;
  ceph_assert(cap->session == session);

  ceph_tid_t last_snap_flush = 0;
  for (auto p = in->flushing_cap_tids.rbegin();
       p != in->flushing_cap_tids.rend();
       ++p) {
    if (!p->second) {
      last_snap_flush = p->first;
      break;
    }
  }

  int wanted = in->caps_wanted();
  int used = get_caps_used(in) | in->caps_dirty();
  auto it = in->cap_snaps.begin();
  for (auto& p : in->flushing_cap_tids) {
    if (p.second) {
      int msg_flags = p.first < last_snap_flush ? MClientCaps::FLAG_PENDING_CAPSNAP : 0;
      send_cap(in, session, cap, msg_flags, used, wanted, (cap->issued | cap->implemented),
	       p.second, p.first);
    } else {
      ceph_assert(it != in->cap_snaps.end());
      ceph_assert(it->second.flush_tid == p.first);
      send_flush_snap(in, session, it->first, it->second);
      ++it;
    }
  }
}

void Client::kick_flushing_caps(MetaSession *session)
{
  mds_rank_t mds = session->mds_num;
  ldout(cct, 10) << __func__ << " mds." << mds << dendl;

  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
    if (in->flags & I_KICK_FLUSH) {
      ldout(cct, 20) << " reflushing caps on " << *in << " to mds." << mds << dendl;
      kick_flushing_caps(in, session);
    }
  }
}

void Client::early_kick_flushing_caps(MetaSession *session)
{
  for (xlist<Inode*>::iterator p = session->flushing_caps.begin(); !p.end(); ++p) {
    Inode *in = *p;
    Cap *cap = in->auth_cap;
    ceph_assert(cap);

    // if flushing caps were revoked, we re-send the cap flush in client reconnect
    // stage. This guarantees that MDS processes the cap flush message before issuing
    // the flushing caps to other client.
    if ((in->flushing_caps & in->auth_cap->issued) == in->flushing_caps) {
      in->flags |= I_KICK_FLUSH;
      continue;
    }

    ldout(cct, 20) << " reflushing caps (early_kick) on " << *in
		   << " to mds." << session->mds_num << dendl;
    // send_reconnect() also will reset these sequence numbers. make sure
    // sequence numbers in cap flush message match later reconnect message.
    cap->seq = 0;
    cap->issue_seq = 0;
    cap->mseq = 0;
    cap->issued = cap->implemented;

    kick_flushing_caps(in, session);
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

    ldout(cct, 10) << __func__ << " " << *realm << dendl;
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
  ldout(cct, 20) << __func__ << " " << r << " " << realm << " " << realm->nref << " -> " << (realm->nref + 1) << dendl;
  realm->nref++;
  return realm;
}

SnapRealm *Client::get_snap_realm_maybe(inodeno_t r)
{
  if (snap_realms.count(r) == 0) {
    ldout(cct, 20) << __func__ << " " << r << " fail" << dendl;
    return NULL;
  }
  SnapRealm *realm = snap_realms[r];
  ldout(cct, 20) << __func__ << " " << r << " " << realm << " " << realm->nref << " -> " << (realm->nref + 1) << dendl;
  realm->nref++;
  return realm;
}

void Client::put_snap_realm(SnapRealm *realm)
{
  ldout(cct, 20) << __func__ << " " << realm->ino << " " << realm
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
    ldout(cct, 10) << __func__ << " " << *realm
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


void Client::update_snap_trace(const bufferlist& bl, SnapRealm **realm_ret, bool flush)
{
  SnapRealm *first_realm = NULL;
  ldout(cct, 10) << __func__ << " len " << bl.length() << dendl;

  map<SnapRealm*, SnapContext> dirty_realms;

  auto p = bl.cbegin();
  while (!p.end()) {
    SnapRealmInfo info;
    decode(info, p);
    SnapRealm *realm = get_snap_realm(info.ino());

    bool invalidate = false;

    if (info.seq() > realm->seq) {
      ldout(cct, 10) << __func__ << " " << *realm << " seq " << info.seq() << " > " << realm->seq
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
      ldout(cct, 15) << __func__ << " " << *realm << " self|parent updated" << dendl;
      ldout(cct, 15) << "  snapc " << realm->get_snap_context() << dendl;
    } else {
      ldout(cct, 10) << __func__ << " " << *realm << " seq " << info.seq()
	       << " <= " << realm->seq << " and same parent, SKIPPING" << dendl;
    }
        
    if (!first_realm)
      first_realm = realm;
    else
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

  if (realm_ret)
    *realm_ret = first_realm;
  else
    put_snap_realm(first_realm);
}

void Client::handle_snap(const MConstRef<MClientSnap>& m)
{
  ldout(cct, 10) << __func__ << " " << *m << dendl;
  mds_rank_t mds = mds_rank_t(m->get_source().num());
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
    return;
  }

  got_mds_push(session);

  map<Inode*, SnapContext> to_move;
  SnapRealm *realm = 0;

  if (m->head.op == CEPH_SNAP_OP_SPLIT) {
    ceph_assert(m->head.split);
    SnapRealmInfo info;
    auto p = m->bl.cbegin();
    decode(info, p);
    ceph_assert(info.ino() == m->head.split);
    
    // flush, then move, ino's.
    realm = get_snap_realm(info.ino());
    ldout(cct, 10) << " splitting off " << *realm << dendl;
    for (auto& ino : m->split_inos) {
      vinodeno_t vino(ino, CEPH_NOSNAP);
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
    for (auto& child_realm : m->split_realms) {
      ldout(cct, 10) << "adjusting snaprealm " << child_realm << " parent" << dendl;
      SnapRealm *child = get_snap_realm_maybe(child_realm);
      if (!child)
	continue;
      adjust_realm_parent(child, realm->ino);
      put_snap_realm(child);
    }
  }

  update_snap_trace(m->bl, NULL, m->head.op != CEPH_SNAP_OP_DESTROY);

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
}

void Client::handle_quota(const MConstRef<MClientQuota>& m)
{
  mds_rank_t mds = mds_rank_t(m->get_source().num());
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
    return;
  }

  got_mds_push(session);

  ldout(cct, 10) << __func__ << " " << *m << " from mds." << mds << dendl;

  vinodeno_t vino(m->ino, CEPH_NOSNAP);
  if (inode_map.count(vino)) {
    Inode *in = NULL;
    in = inode_map[vino];

    if (in) {
      in->quota = m->quota;
      in->rstat = m->rstat;
    }
  }
}

void Client::handle_caps(const MConstRef<MClientCaps>& m)
{
  mds_rank_t mds = mds_rank_t(m->get_source().num());
  MetaSession *session = _get_mds_session(mds, m->get_connection().get());
  if (!session) {
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

  Inode *in;
  vinodeno_t vino(m->get_ino(), CEPH_NOSNAP);
  if (auto it = inode_map.find(vino); it != inode_map.end()) {
    in = it->second;
  } else {
    if (m->get_op() == CEPH_CAP_OP_IMPORT) {
      ldout(cct, 5) << __func__ << " don't have vino " << vino << " on IMPORT, immediately releasing" << dendl;
      session->enqueue_cap_release(
        m->get_ino(),
        m->get_cap_id(),
        m->get_seq(),
        m->get_mseq(),
        cap_epoch_barrier);
    } else {
      ldout(cct, 5) << __func__ << " don't have vino " << vino << ", dropping" << dendl;
    }

    // in case the mds is waiting on e.g. a revocation
    flush_cap_releases();
    return;
  }

  switch (m->get_op()) {
    case CEPH_CAP_OP_EXPORT: return handle_cap_export(session, in, m);
    case CEPH_CAP_OP_FLUSHSNAP_ACK: return handle_cap_flushsnap_ack(session, in, m);
    case CEPH_CAP_OP_IMPORT: /* no return */ handle_cap_import(session, in, m);
  }

  if (auto it = in->caps.find(mds); it != in->caps.end()) {
    Cap &cap = in->caps.at(mds);

    switch (m->get_op()) {
      case CEPH_CAP_OP_TRUNC: return handle_cap_trunc(session, in, m);
      case CEPH_CAP_OP_IMPORT:
      case CEPH_CAP_OP_REVOKE:
      case CEPH_CAP_OP_GRANT: return handle_cap_grant(session, in, &cap, m);
      case CEPH_CAP_OP_FLUSH_ACK: return handle_cap_flush_ack(session, in, &cap, m);
    }
  } else {
    ldout(cct, 5) << __func__ << " don't have " << *in << " cap on mds." << mds << dendl;
    return;
  }
}

void Client::handle_cap_import(MetaSession *session, Inode *in, const MConstRef<MClientCaps>& m)
{
  mds_rank_t mds = session->mds_num;

  ldout(cct, 5) << __func__ << " ino " << m->get_ino() << " mseq " << m->get_mseq()
		<< " IMPORT from mds." << mds << dendl;

  const mds_rank_t peer_mds = mds_rank_t(m->peer.mds);
  Cap *cap = NULL;
  UserPerm cap_perms;
  if (auto it = in->caps.find(peer_mds); m->peer.cap_id && it != in->caps.end()) {
    cap = &it->second;
    cap_perms = cap->latest_perms;
  }

  // add/update it
  SnapRealm *realm = NULL;
  update_snap_trace(m->snapbl, &realm);

  int issued = m->get_caps();
  int wanted = m->get_wanted();
  add_update_cap(in, session, m->get_cap_id(),
		 issued, wanted, m->get_seq(), m->get_mseq(),
		 m->get_realm(), CEPH_CAP_FLAG_AUTH, cap_perms);
  
  if (cap && cap->cap_id == m->peer.cap_id) {
      remove_cap(cap, (m->peer.flags & CEPH_CAP_FLAG_RELEASE));
  }

  if (realm)
    put_snap_realm(realm);
  
  if (in->auth_cap && in->auth_cap->session == session) {
    if (!(wanted & CEPH_CAP_ANY_FILE_WR) ||
	in->requested_max_size > m->get_max_size()) {
      in->requested_max_size = 0;
      ldout(cct, 15) << "reset requested_max_size after cap import" << dendl;
    }
    // reflush any/all caps (if we are now the auth_cap)
    kick_flushing_caps(in, session);
  }
}

void Client::handle_cap_export(MetaSession *session, Inode *in, const MConstRef<MClientCaps>& m)
{
  mds_rank_t mds = session->mds_num;

  ldout(cct, 5) << __func__ << " ino " << m->get_ino() << " mseq " << m->get_mseq()
		<< " EXPORT from mds." << mds << dendl;

  auto it = in->caps.find(mds);
  if (it != in->caps.end()) {
    Cap &cap = it->second;
    if (cap.cap_id == m->get_cap_id()) {
      if (m->peer.cap_id) {
	const auto peer_mds = mds_rank_t(m->peer.mds);
        MetaSession *tsession = _get_or_open_mds_session(peer_mds);
        auto it = in->caps.find(peer_mds);
        if (it != in->caps.end()) {
	  Cap &tcap = it->second;
	  if (tcap.cap_id == m->peer.cap_id &&
	      ceph_seq_cmp(tcap.seq, m->peer.seq) < 0) {
	    tcap.cap_id = m->peer.cap_id;
	    tcap.seq = m->peer.seq - 1;
	    tcap.issue_seq = tcap.seq;
	    tcap.issued |= cap.issued;
	    tcap.implemented |= cap.issued;
	    if (&cap == in->auth_cap)
	      in->auth_cap = &tcap;
	    if (in->auth_cap == &tcap && in->flushing_cap_item.is_on_list())
	      adjust_session_flushing_caps(in, session, tsession);
	  }
        } else {
	  add_update_cap(in, tsession, m->peer.cap_id, cap.issued, 0,
		         m->peer.seq - 1, m->peer.mseq, (uint64_t)-1,
		         &cap == in->auth_cap ? CEPH_CAP_FLAG_AUTH : 0,
		         cap.latest_perms);
        }
      } else {
	if (cap.wanted | cap.issued)
	  in->flags |= I_CAP_DROPPED;
      }

      remove_cap(&cap, false);
    }
  }
}

void Client::handle_cap_trunc(MetaSession *session, Inode *in, const MConstRef<MClientCaps>& m)
{
  mds_rank_t mds = session->mds_num;
  ceph_assert(in->caps.count(mds));

  ldout(cct, 10) << __func__ << " on ino " << *in
	   << " size " << in->size << " -> " << m->get_size()
	   << dendl;
  
  int issued;
  in->caps_issued(&issued);
  issued |= in->caps_dirty();
  update_inode_file_size(in, issued, m->get_size(),
			 m->get_truncate_seq(), m->get_truncate_size());
}

void Client::handle_cap_flush_ack(MetaSession *session, Inode *in, Cap *cap, const MConstRef<MClientCaps>& m)
{
  ceph_tid_t flush_ack_tid = m->get_client_tid();
  int dirty = m->get_dirty();
  int cleaned = 0;
  int flushed = 0;

  auto it = in->flushing_cap_tids.begin();
  if (it->first < flush_ack_tid) {
       ldout(cct, 0) << __func__ << " mds." << session->mds_num
                   << " got unexpected flush ack tid " << flush_ack_tid
                   << " expected is " << it->first << dendl;
  }
  for (; it != in->flushing_cap_tids.end(); ) {
    if (!it->second) {
      // cap snap
      ++it;
      continue;
    }
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

  ldout(cct, 5) << __func__ << " mds." << session->mds_num
	  << " cleaned " << ccap_string(cleaned) << " on " << *in
	  << " with " << ccap_string(dirty) << dendl;

  if (flushed) {
    signal_cond_list(in->waitfor_caps);
    if (session->flushing_caps_tids.empty() ||
	*session->flushing_caps_tids.begin() > flush_ack_tid)
      sync_cond.notify_all();
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
       if (in->flushing_cap_tids.empty())
	  in->flushing_cap_item.remove_myself();
      }
      if (!in->caps_dirty())
	put_inode(in);
    }
  }
}


void Client::handle_cap_flushsnap_ack(MetaSession *session, Inode *in, const MConstRef<MClientCaps>& m)
{
  ceph_tid_t flush_ack_tid = m->get_client_tid();
  mds_rank_t mds = session->mds_num;
  ceph_assert(in->caps.count(mds));
  snapid_t follows = m->get_snap_follows();

  if (auto it = in->cap_snaps.find(follows); it != in->cap_snaps.end()) {
    auto& capsnap = it->second;
    if (flush_ack_tid != capsnap.flush_tid) {
      ldout(cct, 10) << " tid " << flush_ack_tid << " != " << capsnap.flush_tid << dendl;
    } else {
      InodeRef tmp_ref(in);
      ldout(cct, 5) << __func__ << " mds." << mds << " flushed snap follows " << follows
	      << " on " << *in << dendl;
      session->flushing_caps_tids.erase(capsnap.flush_tid);
      in->flushing_cap_tids.erase(capsnap.flush_tid);
      if (in->flushing_caps == 0 && in->flushing_cap_tids.empty())
	in->flushing_cap_item.remove_myself();
      in->cap_snaps.erase(it);

      signal_cond_list(in->waitfor_caps);
      if (session->flushing_caps_tids.empty() ||
	  *session->flushing_caps_tids.begin() > flush_ack_tid)
	sync_cond.notify_all();
    }
  } else {
    ldout(cct, 5) << __func__ << " DUP(?) mds." << mds << " flushed snap follows " << follows
	    << " on " << *in << dendl;
    // we may not have it if we send multiple FLUSHSNAP requests and (got multiple FLUSHEDSNAPs back)
  }
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
  void finish(int r) override {
    // _async_dentry_invalidate is responsible for its own locking
    ceph_assert(ceph_mutex_is_not_locked_by_me(client->client_lock));
    client->_async_dentry_invalidate(dirino, ino, name);
  }
};

void Client::_async_dentry_invalidate(vinodeno_t dirino, vinodeno_t ino, string& name)
{
  if (unmounting)
    return;
  ldout(cct, 10) << __func__ << " '" << name << "' ino " << ino
		 << " in dir " << dirino << dendl;
  dentry_invalidate_cb(callback_handle, dirino, ino, name.c_str(), name.length());
}

void Client::_schedule_invalidate_dentry_callback(Dentry *dn, bool del)
{
  if (dentry_invalidate_cb && dn->inode->ll_ref > 0)
    async_dentry_invalidator.queue(new C_Client_DentryInvalidate(this, dn, del));
}

void Client::_try_to_trim_inode(Inode *in, bool sched_inval)
{
  int ref = in->get_num_ref();
  ldout(cct, 5) << __func__ << " in " << *in <<dendl;

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

  if (ref > 0) {
    auto q = in->dentries.begin();
    while (q != in->dentries.end()) {
      Dentry *dn = *q;
      ++q;
      if( in->ll_ref > 0 && sched_inval) {
        // FIXME: we play lots of unlink/link tricks when handling MDS replies,
        //        so in->dentries doesn't always reflect the state of kernel's dcache.
        _schedule_invalidate_dentry_callback(dn, true);
      }
      unlink(dn, true, true);
    }
  }
}

void Client::handle_cap_grant(MetaSession *session, Inode *in, Cap *cap, const MConstRef<MClientCaps>& m)
{
  mds_rank_t mds = session->mds_num;
  int used = get_caps_used(in);
  int wanted = in->caps_wanted();

  const unsigned new_caps = m->get_caps();
  const bool was_stale = session->cap_gen > cap->gen;
  ldout(cct, 5) << __func__ << " on in " << m->get_ino() 
		<< " mds." << mds << " seq " << m->get_seq()
		<< " caps now " << ccap_string(new_caps)
		<< " was " << ccap_string(cap->issued)
		<< (was_stale ? " (stale)" : "") << dendl;

  if (was_stale)
      cap->issued = cap->implemented = CEPH_CAP_PIN;
  cap->seq = m->get_seq();
  cap->gen = session->cap_gen;

  check_cap_issue(in, new_caps);

  // update inode
  int issued;
  in->caps_issued(&issued);
  issued |= in->caps_dirty();

  if ((new_caps & CEPH_CAP_AUTH_SHARED) &&
      !(issued & CEPH_CAP_AUTH_EXCL)) {
    in->mode = m->head.mode;
    in->uid = m->head.uid;
    in->gid = m->head.gid;
    in->btime = m->btime;
  }
  bool deleted_inode = false;
  if ((new_caps & CEPH_CAP_LINK_SHARED) &&
      !(issued & CEPH_CAP_LINK_EXCL)) {
    in->nlink = m->head.nlink;
    if (in->nlink == 0 &&
	(new_caps & (CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL)))
      deleted_inode = true;
  }
  if (!(issued & CEPH_CAP_XATTR_EXCL) &&
      m->xattrbl.length() &&
      m->head.xattr_version > in->xattr_version) {
    auto p = m->xattrbl.cbegin();
    decode(in->xattrs, p);
    in->xattr_version = m->head.xattr_version;
  }

  if ((new_caps & CEPH_CAP_FILE_SHARED) && m->dirstat_is_valid()) {
    in->dirstat.nfiles = m->get_nfiles();
    in->dirstat.nsubdirs = m->get_nsubdirs();
  }

  if (new_caps & CEPH_CAP_ANY_RD) {
    update_inode_file_time(in, issued, m->get_time_warp_seq(),
			   m->get_ctime(), m->get_mtime(), m->get_atime());
  }

  if (new_caps & (CEPH_CAP_ANY_FILE_RD | CEPH_CAP_ANY_FILE_WR)) {
    in->layout = m->get_layout();
    update_inode_file_size(in, issued, m->get_size(),
			   m->get_truncate_seq(), m->get_truncate_size());
  }

  if (m->inline_version > in->inline_version) {
    in->inline_data = m->inline_data;
    in->inline_version = m->inline_version;
  }

  /* always take a newer change attr */
  if (m->get_change_attr() > in->change_attr)
    in->change_attr = m->get_change_attr();

  // max_size
  if (cap == in->auth_cap &&
      (new_caps & CEPH_CAP_ANY_FILE_WR) &&
      (m->get_max_size() != in->max_size)) {
    ldout(cct, 10) << "max_size " << in->max_size << " -> " << m->get_max_size() << dendl;
    in->max_size = m->get_max_size();
    if (in->max_size > in->wanted_max_size) {
      in->wanted_max_size = 0;
      in->requested_max_size = 0;
    }
  }

  bool check = false;
  if ((was_stale || m->get_op() == CEPH_CAP_OP_IMPORT) &&
      (wanted & ~(cap->wanted | new_caps))) {
    // If mds is importing cap, prior cap messages that update 'wanted'
    // may get dropped by mds (migrate seq mismatch).
    //
    // We don't send cap message to update 'wanted' if what we want are
    // already issued. If mds revokes caps, cap message that releases caps
    // also tells mds what we want. But if caps got revoked by mds forcedly
    // (session stale). We may haven't told mds what we want.
    check = true;
  }


  // update caps
  auto revoked = cap->issued & ~new_caps;
  if (revoked) {
    ldout(cct, 10) << "  revocation of " << ccap_string(revoked) << dendl;
    cap->issued = new_caps;
    cap->implemented |= new_caps;

    // recall delegations if we're losing caps necessary for them
    if (revoked & ceph_deleg_caps_for_type(CEPH_DELEGATION_RD))
      in->recall_deleg(false);
    else if (revoked & ceph_deleg_caps_for_type(CEPH_DELEGATION_WR))
      in->recall_deleg(true);

    used = adjust_caps_used_for_lazyio(used, cap->issued, cap->implemented);
    if ((used & revoked & (CEPH_CAP_FILE_BUFFER | CEPH_CAP_FILE_LAZYIO)) &&
	!_flush(in, new C_Client_FlushComplete(this, in))) {
      // waitin' for flush
    } else if (used & revoked & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO)) {
      if (_release(in))
	check = true;
    } else {
      cap->wanted = 0; // don't let check_caps skip sending a response to MDS
      check = true;
    }
  } else if (cap->issued == new_caps) {
    ldout(cct, 10) << "  caps unchanged at " << ccap_string(cap->issued) << dendl;
  } else {
    ldout(cct, 10) << "  grant, new caps are " << ccap_string(new_caps & ~cap->issued) << dendl;
    cap->issued = new_caps;
    cap->implemented |= new_caps;

    if (cap == in->auth_cap) {
      // non-auth MDS is revoking the newly grant caps ?
      for (const auto &p : in->caps) {
	if (&p.second == cap)
	  continue;
	if (p.second.implemented & ~p.second.issued & new_caps) {
	  check = true;
	  break;
	}
      }
    }
  }

  if (check)
    check_caps(in, 0);

  // wake up waiters
  if (new_caps)
    signal_cond_list(in->waitfor_caps);

  // may drop inode's last ref
  if (deleted_inode)
    _try_to_trim_inode(in, true);
}

int Client::inode_permission(Inode *in, const UserPerm& perms, unsigned want)
{
  if (perms.uid() == 0)
    return 0;
  
  if (perms.uid() != in->uid && (in->mode & S_IRWXG)) {
    int ret = _posix_acl_permission(in, perms, want);
    if (ret != -EAGAIN)
      return ret;
  }

  // check permissions before doing anything else
  if (!in->check_mode(perms, want))
    return -EACCES;
  return 0;
}

int Client::xattr_permission(Inode *in, const char *name, unsigned want,
			     const UserPerm& perms)
{
  int r = _getattr_for_perm(in, perms);
  if (r < 0)
    goto out;

  r = 0;
  if (strncmp(name, "system.", 7) == 0) {
    if ((want & MAY_WRITE) && (perms.uid() != 0 && perms.uid() != in->uid))
      r = -EPERM;
  } else {
    r = inode_permission(in, perms, want);
  }
out:
  ldout(cct, 5) << __func__ << " " << in << " = " << r <<  dendl;
  return r;
}

ostream& operator<<(ostream &out, const UserPerm& perm) {
  out << "UserPerm(uid: " << perm.uid() << ", gid: " << perm.gid() << ")";
  return out;
}

int Client::may_setattr(Inode *in, struct ceph_statx *stx, int mask,
			const UserPerm& perms)
{
  ldout(cct, 20) << __func__ << " " << *in << "; " << perms << dendl;
  int r = _getattr_for_perm(in, perms);
  if (r < 0)
    goto out;

  if (mask & CEPH_SETATTR_SIZE) {
    r = inode_permission(in, perms, MAY_WRITE);
    if (r < 0)
      goto out;
  }

  r = -EPERM;
  if (mask & CEPH_SETATTR_UID) {
    if (perms.uid() != 0 && (perms.uid() != in->uid || stx->stx_uid != in->uid))
      goto out;
  }
  if (mask & CEPH_SETATTR_GID) {
    if (perms.uid() != 0 && (perms.uid() != in->uid ||
      	       (!perms.gid_in_groups(stx->stx_gid) && stx->stx_gid != in->gid)))
      goto out;
  }

  if (mask & CEPH_SETATTR_MODE) {
    if (perms.uid() != 0 && perms.uid() != in->uid)
      goto out;

    gid_t i_gid = (mask & CEPH_SETATTR_GID) ? stx->stx_gid : in->gid;
    if (perms.uid() != 0 && !perms.gid_in_groups(i_gid))
      stx->stx_mode &= ~S_ISGID;
  }

  if (mask & (CEPH_SETATTR_CTIME | CEPH_SETATTR_BTIME |
	      CEPH_SETATTR_MTIME | CEPH_SETATTR_ATIME)) {
    if (perms.uid() != 0 && perms.uid() != in->uid) {
      int check_mask = CEPH_SETATTR_CTIME | CEPH_SETATTR_BTIME;
      if (!(mask & CEPH_SETATTR_MTIME_NOW))
	check_mask |= CEPH_SETATTR_MTIME;
      if (!(mask & CEPH_SETATTR_ATIME_NOW))
	check_mask |= CEPH_SETATTR_ATIME;
      if (check_mask & mask) {
	goto out;
      } else {
	r = inode_permission(in, perms, MAY_WRITE);
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

int Client::may_open(Inode *in, int flags, const UserPerm& perms)
{
  ldout(cct, 20) << __func__ << " " << *in << "; " << perms << dendl;
  unsigned want = 0;

  if ((flags & O_ACCMODE) == O_WRONLY)
    want = MAY_WRITE;
  else if ((flags & O_ACCMODE) == O_RDWR)
    want = MAY_READ | MAY_WRITE;
  else if ((flags & O_ACCMODE) == O_RDONLY)
    want = MAY_READ;
  if (flags & O_TRUNC)
    want |= MAY_WRITE;

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

  r = _getattr_for_perm(in, perms);
  if (r < 0)
    goto out;

  r = inode_permission(in, perms, want);
out:
  ldout(cct, 3) << __func__ << " " << in << " = " << r <<  dendl;
  return r;
}

int Client::may_lookup(Inode *dir, const UserPerm& perms)
{
  ldout(cct, 20) << __func__ << " " << *dir << "; " << perms << dendl;
  int r = _getattr_for_perm(dir, perms);
  if (r < 0)
    goto out;

  r = inode_permission(dir, perms, MAY_EXEC);
out:
  ldout(cct, 3) << __func__ << " " << dir << " = " << r <<  dendl;
  return r;
}

int Client::may_create(Inode *dir, const UserPerm& perms)
{
  ldout(cct, 20) << __func__ << " " << *dir << "; " << perms << dendl;
  int r = _getattr_for_perm(dir, perms);
  if (r < 0)
    goto out;

  r = inode_permission(dir, perms, MAY_EXEC | MAY_WRITE);
out:
  ldout(cct, 3) << __func__ << " " << dir << " = " << r <<  dendl;
  return r;
}

int Client::may_delete(Inode *dir, const char *name, const UserPerm& perms)
{
  ldout(cct, 20) << __func__ << " " << *dir << "; " << "; name " << name << "; " << perms << dendl;
  int r = _getattr_for_perm(dir, perms);
  if (r < 0)
    goto out;

  r = inode_permission(dir, perms, MAY_EXEC | MAY_WRITE);
  if (r < 0)
    goto out;

  /* 'name == NULL' means rmsnap */
  if (perms.uid() != 0 && name && (dir->mode & S_ISVTX)) {
    InodeRef otherin;
    r = _lookup(dir, name, CEPH_CAP_AUTH_SHARED, &otherin, perms);
    if (r < 0)
      goto out;
    if (dir->uid != perms.uid() && otherin->uid != perms.uid())
      r = -EPERM;
  }
out:
  ldout(cct, 3) << __func__ << " " << dir << " = " << r <<  dendl;
  return r;
}

int Client::may_hardlink(Inode *in, const UserPerm& perms)
{
  ldout(cct, 20) << __func__ << " " << *in << "; " << perms << dendl;
  int r = _getattr_for_perm(in, perms);
  if (r < 0)
    goto out;

  if (perms.uid() == 0 || perms.uid() == in->uid) {
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

  r = inode_permission(in, perms, MAY_READ | MAY_WRITE);
out:
  ldout(cct, 3) << __func__ << " " << in << " = " << r <<  dendl;
  return r;
}

int Client::_getattr_for_perm(Inode *in, const UserPerm& perms)
{
  int mask = CEPH_STAT_CAP_MODE;
  bool force = false;
  if (acl_type != NO_ACL) {
    mask |= CEPH_STAT_CAP_XATTR;
    force = in->xattr_version == 0;
  }
  return _getattr(in, mask, perms, force);
}

vinodeno_t Client::_get_vino(Inode *in)
{
  /* The caller must hold the client lock */
  return vinodeno_t(in->ino, in->snapid);
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
  ceph_assert(fsmap);
  ceph_assert(targets != nullptr);

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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));

  if (monclient->is_authenticated()) {
    return 0;
  }

  client_lock.unlock();
  int r = monclient->authenticate(cct->_conf->client_mount_timeout);
  client_lock.lock();
  if (r < 0) {
    return r;
  }

  whoami = monclient->get_global_id();
  messenger->set_myname(entity_name_t::CLIENT(whoami.v));

  return 0;
}

int Client::fetch_fsmap(bool user)
{
  // Retrieve FSMap to enable looking up daemon addresses.  We need FSMap
  // rather than MDSMap because no one MDSMap contains all the daemons, and
  // a `tell` can address any daemon.
  version_t fsmap_latest;
  bs::error_code ec;
  do {
    client_lock.unlock();
    std::tie(fsmap_latest, std::ignore) =
      monclient->get_version("fsmap", ca::use_blocked[ec]);
    client_lock.lock();
  } while (ec == bs::errc::resource_unavailable_try_again);

  if (ec) {
    lderr(cct) << "Failed to learn FSMap version: " << ec << dendl;
    return ceph::from_error_code(ec);
  }

  ldout(cct, 10) << __func__ << " learned FSMap version " << fsmap_latest << dendl;

  if (user) {
    if (!fsmap_user || fsmap_user->get_epoch() < fsmap_latest) {
      monclient->sub_want("fsmap.user", fsmap_latest, CEPH_SUBSCRIBE_ONETIME);
      monclient->renew_subs();
      wait_on_list(waiting_for_fsmap);
    }
    ceph_assert(fsmap_user);
    ceph_assert(fsmap_user->get_epoch() >= fsmap_latest);
  } else {
    if (!fsmap || fsmap->get_epoch() < fsmap_latest) {
      monclient->sub_want("fsmap", fsmap_latest, CEPH_SUBSCRIBE_ONETIME);
      monclient->renew_subs();
      wait_on_list(waiting_for_fsmap);
    }
    ceph_assert(fsmap);
    ceph_assert(fsmap->get_epoch() >= fsmap_latest);
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
  std::lock_guard lock(client_lock);

  if (!initialized)
    return -ENOTCONN;

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

  if (metadata.empty()) {
    // We are called on an unmounted client, so metadata
    // won't be initialized yet.
    populate_metadata("");
  }

  // Send commands to targets
  C_GatherBuilder gather(cct, onfinish);
  for (const auto target_gid : non_laggy) {
    const auto info = fsmap->get_info_gid(target_gid);

    // Open a connection to the target MDS
    ConnectionRef conn = messenger->connect_to_mds(info.get_addrs());

    // Generate MDSCommandOp state
    auto &op = command_table.start_command();

    op.on_finish = gather.new_sub();
    op.cmd = cmd;
    op.outbl = outbl;
    op.outs = outs;
    op.inbl = inbl;
    op.mds_gid = target_gid;
    op.con = conn;

    ldout(cct, 4) << __func__ << ": new command op to " << target_gid
      << " tid=" << op.tid << cmd << dendl;

    // Construct and send MCommand
    auto m = op.get_message(monclient->get_fsid());
    conn->send_message2(std::move(m));
  }
  gather.activate();

  return 0;
}

void Client::handle_command_reply(const MConstRef<MCommandReply>& m)
{
  ceph_tid_t const tid = m->get_tid();

  ldout(cct, 10) << __func__ << ": tid=" << m->get_tid() << dendl;

  if (!command_table.exists(tid)) {
    ldout(cct, 1) << __func__ << ": unknown tid " << tid << ", dropping" << dendl;
    return;
  }

  auto &op = command_table.get_command(tid);
  if (op.outbl) {
    *op.outbl = m->get_data();
  }
  if (op.outs) {
    *op.outs = m->rs;
  }

  if (op.on_finish) {
    op.on_finish->complete(m->r);
  }

  command_table.erase(tid);
}

// -------------------
// MOUNT

int Client::subscribe_mdsmap(const std::string &fs_name)
{
  int r = authenticate();
  if (r < 0) {
    lderr(cct) << "authentication failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  std::string resolved_fs_name;
  if (fs_name.empty()) {
    resolved_fs_name = cct->_conf.get_val<std::string>("client_fs");
    if (resolved_fs_name.empty())
	    // Try the backwards compatibility fs name option
	    resolved_fs_name = cct->_conf.get_val<std::string>("client_mds_namespace");
  } else {
    resolved_fs_name = fs_name;
  }

  std::string want = "mdsmap";
  if (!resolved_fs_name.empty()) {
    r = fetch_fsmap(true);
    if (r < 0)
      return r;
    fscid = fsmap_user->get_fs_cid(resolved_fs_name);
    if (fscid == FS_CLUSTER_ID_NONE) {
      return -ENOENT;
    }

    std::ostringstream oss;
    oss << want << "." << fscid;
    want = oss.str();
  }
  ldout(cct, 10) << "Subscribing to map '" << want << "'" << dendl;

  monclient->sub_want(want, 0, 0);
  monclient->renew_subs();

  return 0;
}

int Client::mount(const std::string &mount_root, const UserPerm& perms,
		  bool require_mds, const std::string &fs_name)
{
  std::lock_guard lock(client_lock);

  if (mounted) {
    ldout(cct, 5) << "already mounted" << dendl;
    return 0;
  }

  unmounting = false;

  int r = subscribe_mdsmap(fs_name);
  if (r < 0) {
    lderr(cct) << "mdsmap subscription failed: " << cpp_strerror(r) << dendl;
    return r;
  }

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
        ceph_abort();
      }
    }
  }

  populate_metadata(mount_root.empty() ? "/" : mount_root);

  filepath fp(CEPH_INO_ROOT);
  if (!mount_root.empty()) {
    fp = filepath(mount_root.c_str());
  }
  while (true) {
    MetaRequest *req = new MetaRequest(CEPH_MDS_OP_GETATTR);
    req->set_filepath(fp);
    req->head.args.getattr.mask = CEPH_STAT_CAP_INODE_ALL;
    int res = make_request(req, perms);
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

  ceph_assert(root);
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
  for (auto it = mds_sessions.begin(); it != mds_sessions.end(); ) {
    if (it->second.state == MetaSession::STATE_REJECTED)
      mds_sessions.erase(it++);
    else
      ++it;
  }

  while (!mds_sessions.empty()) {
    // send session closes!
    for (auto &p : mds_sessions) {
      if (p.second.state != MetaSession::STATE_CLOSING) {
	_close_mds_session(&p.second);
      }
    }

    // wait for sessions to close
    ldout(cct, 2) << "waiting for " << mds_sessions.size() << " mds sessions to close" << dendl;
    std::unique_lock l{client_lock, std::adopt_lock};
    mount_cond.wait(l);
    l.release();
  }
}

void Client::flush_mdlog_sync()
{
  if (mds_requests.empty()) 
    return;
  for (auto &p : mds_sessions) {
    flush_mdlog(&p.second);
  }
}

void Client::flush_mdlog(MetaSession *session)
{
  // Only send this to Luminous or newer MDS daemons, older daemons
  // will crash if they see an unknown CEPH_SESSION_* value in this msg.
  const uint64_t features = session->con->get_features();
  if (HAVE_FEATURE(features, SERVER_LUMINOUS)) {
    auto m = make_message<MClientSession>(CEPH_SESSION_REQUEST_FLUSH_MDLOG);
    session->con->send_message2(std::move(m));
  }
}


void Client::_abort_mds_sessions(int err)
{
  for (auto p = mds_requests.begin(); p != mds_requests.end(); ) {
    auto req = p->second;
    ++p;
    // unsafe requests will be removed during close session below.
    if (req->got_unsafe)
      continue;

    req->abort(err);
    if (req->caller_cond) {
      req->kick = true;
      req->caller_cond->notify_all();
    }
  }

  // Process aborts on any requests that were on this waitlist.
  // Any requests that were on a waiting_for_open session waitlist
  // will get kicked during close session below.
  signal_cond_list(waiting_for_mdsmap);

  // Force-close all sessions
  while(!mds_sessions.empty()) {
    auto& session = mds_sessions.begin()->second;
    _closed_mds_session(&session, err);
  }
}

void Client::_unmount(bool abort)
{
  std::unique_lock lock{client_lock, std::adopt_lock};
  if (unmounting)
    return;

  if (abort || blacklisted) {
    ldout(cct, 2) << "unmounting (" << (abort ? "abort)" : "blacklisted)") << dendl;
  } else {
    ldout(cct, 2) << "unmounting" << dendl;
  }
  unmounting = true;

  deleg_timeout = 0;

  if (abort) {
    // Abort all mds sessions
    _abort_mds_sessions(-ENOTCONN);

    objecter->op_cancel_writes(-ENOTCONN);
  } else {
    // flush the mdlog for pending requests, if any
    flush_mdlog_sync();
  }

  mount_cond.wait(lock, [this] {
    if (!mds_requests.empty()) {
      ldout(cct, 10) << "waiting on " << mds_requests.size() << " requests"
		     << dendl;
    }
    return mds_requests.empty();
  });
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

  mount_cond.wait(lock, [this] {
    if (unsafe_sync_write > 0) {
      ldout(cct, 0) << unsafe_sync_write << " unsafe_sync_writes, waiting"
		    << dendl;
    }
    return unsafe_sync_write <= 0;
  });

  if (cct->_conf->client_oc) {
    // flush/release all buffered data
    std::list<InodeRef> anchor;
    for (auto& p : inode_map) {
      Inode *in = p.second;
      if (!in) {
	ldout(cct, 0) << "null inode_map entry ino " << p.first << dendl;
	ceph_assert(in);
      }

      // prevent inode from getting freed
      anchor.emplace_back(in);

      if (abort || blacklisted) {
        objectcacher->purge_set(&in->oset);
      } else if (!in->caps.empty()) {
	_release(in);
	_flush(in, new C_Client_FlushComplete(this, in));
      }
    }
  }

  if (abort || blacklisted) {
    for (auto p = dirty_list.begin(); !p.end(); ) {
      Inode *in = *p;
      ++p;
      if (in->dirty_caps) {
	ldout(cct, 0) << " drop dirty caps on " << *in << dendl;
	in->mark_caps_clean();
	put_inode(in);
      }
    }
  } else {
    flush_caps_sync();
    wait_sync_caps(last_flush_tid);
  }

  // empty lru cache
  trim_cache();

  while (lru.lru_get_size() > 0 ||
         !inode_map.empty()) {
    ldout(cct, 2) << "cache still has " << lru.lru_get_size()
            << "+" << inode_map.size() << " items"
	    << ", waiting (for caps to release?)"
            << dendl;
    if (auto r = mount_cond.wait_for(lock, ceph::make_timespan(5));
	r == std::cv_status::timeout) {
      dump_cache(NULL);
    }
  }
  ceph_assert(lru.lru_get_size() == 0);
  ceph_assert(inode_map.empty());

  // stop tracing
  if (!cct->_conf->client_trace.empty()) {
    ldout(cct, 1) << "closing trace file '" << cct->_conf->client_trace << "'" << dendl;
    traceout.close();
  }

  _close_sessions();

  mounted = false;

  lock.release();
  ldout(cct, 2) << "unmounted." << dendl;
}

void Client::unmount()
{
  std::lock_guard lock(client_lock);
  _unmount(false);
}

void Client::abort_conn()
{
  std::lock_guard lock(client_lock);
  _unmount(true);
}

void Client::flush_cap_releases()
{
  // send any cap releases
  for (auto &p : mds_sessions) {
    auto &session = p.second;
    if (session.release && mdsmap->is_clientreplay_or_active_or_stopping(
          p.first)) {
      if (cct->_conf->client_inject_release_failure) {
        ldout(cct, 20) << __func__ << " injecting failure to send cap release message" << dendl;
      } else {
        session.con->send_message2(std::move(session.release));
      }
      session.release.reset();
    }
  }
}

void Client::tick()
{
  if (cct->_conf->client_debug_inject_tick_delay > 0) {
    sleep(cct->_conf->client_debug_inject_tick_delay);
    ceph_assert(0 == cct->_conf.set_val("client_debug_inject_tick_delay", "0"));
    cct->_conf.apply_changes(nullptr);
  }

  ldout(cct, 21) << "tick" << dendl;
  tick_event = timer.add_event_after(
    cct->_conf->client_tick_interval,
    new LambdaContext([this](int) {
	// Called back via Timer, which takes client_lock for us
	ceph_assert(ceph_mutex_is_locked_by_me(client_lock));
	tick();
      }));
  utime_t now = ceph_clock_now();

  if (!mounted && !mds_requests.empty()) {
    MetaRequest *req = mds_requests.begin()->second;
    if (req->op_stamp + cct->_conf->client_mount_timeout < now) {
      req->abort(-ETIMEDOUT);
      if (req->caller_cond) {
	req->kick = true;
	req->caller_cond->notify_all();
      }
      signal_cond_list(waiting_for_mdsmap);
      for (auto &p : mds_sessions) {
	signal_context_list(p.second.waiting_for_open);
      }
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
  xlist<Inode*>::iterator p = delayed_list.begin();
  while (!p.end()) {
    Inode *in = *p;
    ++p;
    if (in->hold_caps_until > now)
      break;
    delayed_list.pop_front();
    check_caps(in, CHECK_CAPS_NODELAY);
  }

  trim_cache(true);

  if (blacklisted && mounted &&
      last_auto_reconnect + 30 * 60 < now &&
      cct->_conf.get_val<bool>("client_reconnect_stale")) {
    messenger->client_reset();
    fd_gen++; // invalidate open files
    blacklisted = false;
    _kick_stale_sessions();
    last_auto_reconnect = now;
  }
}

void Client::renew_caps()
{
  ldout(cct, 10) << "renew_caps()" << dendl;
  last_cap_renew = ceph_clock_now();

  for (auto &p : mds_sessions) {
    ldout(cct, 15) << "renew_caps requesting from mds." << p.first << dendl;
    if (mdsmap->get_state(p.first) >= MDSMap::STATE_REJOIN)
      renew_caps(&p.second);
  }
}

void Client::renew_caps(MetaSession *session)
{
  ldout(cct, 10) << "renew_caps mds." << session->mds_num << dendl;
  session->last_cap_renew_request = ceph_clock_now();
  uint64_t seq = ++session->cap_renew_seq;
  session->con->send_message2(make_message<MClientSession>(CEPH_SESSION_REQUEST_RENEWCAPS, seq));
}


// ===============================================================
// high level (POSIXy) interface

int Client::_do_lookup(Inode *dir, const string& name, int mask,
		       InodeRef *target, const UserPerm& perms)
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

  ldout(cct, 10) << __func__ << " on " << path << dendl;

  int r = make_request(req, perms, target);
  ldout(cct, 10) << __func__ << " res is " << r << dendl;
  return r;
}

int Client::_lookup(Inode *dir, const string& dname, int mask, InodeRef *target,
		    const UserPerm& perms)
{
  int r = 0;
  Dentry *dn = NULL;

  if (dname == "..") {
    if (dir->dentries.empty()) {
      MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPPARENT);
      filepath path(dir->ino);
      req->set_filepath(path);

      InodeRef tmptarget;
      int r = make_request(req, perms, &tmptarget, NULL, rand() % mdsmap->get_num_in_mds());

      if (r == 0) {
	Inode *tempino = tmptarget.get();
	_ll_get(tempino);
	*target = tempino;
	ldout(cct, 8) << __func__ << " found target " << (*target)->ino << dendl;
      } else {
	*target = dir;
      }
    }
    else
      *target = dir->get_first_parent()->dir->parent_inode; //dirs can't be hard-linked
    goto done;
  }

  if (dname == ".") {
    *target = dir;
    goto done;
  }

  if (!dir->is_dir()) {
    r = -ENOTDIR;
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

    ldout(cct, 20) << __func__ << " have dn " << dname << " mds." << dn->lease_mds << " ttl " << dn->lease_ttl
	     << " seq " << dn->lease_seq
	     << dendl;

    if (!dn->inode || dn->inode->caps_issued_mask(mask, true)) {
      // is dn lease valid?
      utime_t now = ceph_clock_now();
      if (dn->lease_mds >= 0 &&
	  dn->lease_ttl > now &&
	  mds_sessions.count(dn->lease_mds)) {
	MetaSession &s = mds_sessions.at(dn->lease_mds);
	if (s.cap_ttl > now &&
	    s.cap_gen == dn->lease_gen) {
	  // touch this mds's dir cap too, even though we don't _explicitly_ use it here, to
	  // make trim_caps() behave.
	  dir->try_touch_cap(dn->lease_mds);
	  goto hit_dn;
	}
	ldout(cct, 20) << " bad lease, cap_ttl " << s.cap_ttl << ", cap_gen " << s.cap_gen
		       << " vs lease_gen " << dn->lease_gen << dendl;
      }
      // dir shared caps?
      if (dir->caps_issued_mask(CEPH_CAP_FILE_SHARED, true)) {
	if (dn->cap_shared_gen == dir->shared_gen &&
	    (!dn->inode || dn->inode->caps_issued_mask(mask, true)))
	      goto hit_dn;
	if (!dn->inode && (dir->flags & I_COMPLETE)) {
	  ldout(cct, 10) << __func__ << " concluded ENOENT locally for "
			 << *dir << " dn '" << dname << "'" << dendl;
	  return -ENOENT;
	}
      }
    } else {
      ldout(cct, 20) << " no cap on " << dn->inode->vino() << dendl;
    }
  } else {
    // can we conclude ENOENT locally?
    if (dir->caps_issued_mask(CEPH_CAP_FILE_SHARED, true) &&
	(dir->flags & I_COMPLETE)) {
      ldout(cct, 10) << __func__ << " concluded ENOENT locally for " << *dir << " dn '" << dname << "'" << dendl;
      return -ENOENT;
    }
  }

  r = _do_lookup(dir, dname, mask, target, perms);
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
    ldout(cct, 10) << __func__ << " " << *dir << " " << dname << " = " << r << dendl;
  else
    ldout(cct, 10) << __func__ << " " << *dir << " " << dname << " = " << **target << dendl;
  return r;
}

int Client::get_or_create(Inode *dir, const char* name,
			  Dentry **pdn, bool expect_null)
{
  // lookup
  ldout(cct, 20) << __func__ << " " << *dir << " name " << name << dendl;
  dir->open_dir();
  if (dir->dir->dentries.count(name)) {
    Dentry *dn = dir->dir->dentries[name];
    
    // is dn lease valid?
    utime_t now = ceph_clock_now();
    if (dn->inode &&
	dn->lease_mds >= 0 &&
	dn->lease_ttl > now &&
	mds_sessions.count(dn->lease_mds)) {
      MetaSession &s = mds_sessions.at(dn->lease_mds);
      if (s.cap_ttl > now &&
	  s.cap_gen == dn->lease_gen) {
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

int Client::path_walk(const filepath& origpath, InodeRef *end,
		      const UserPerm& perms, bool followsym, int mask)
{
  filepath path = origpath;
  InodeRef cur;
  if (origpath.absolute())
    cur = root;
  else
    cur = cwd;
  ceph_assert(cur);

  ldout(cct, 10) << __func__ << " " << path << dendl;

  int symlinks = 0;

  unsigned i=0;
  while (i < path.depth() && cur) {
    int caps = 0;
    const string &dname = path[i];
    ldout(cct, 10) << " " << i << " " << *cur << " " << dname << dendl;
    ldout(cct, 20) << "  (path is " << path << ")" << dendl;
    InodeRef next;
    if (cct->_conf->client_permissions) {
      int r = may_lookup(cur.get(), perms);
      if (r < 0)
	return r;
      caps = CEPH_CAP_AUTH_SHARED;
    }

    /* Get extra requested caps on the last component */
    if (i == (path.depth() - 1))
      caps |= mask;
    int r = _lookup(cur.get(), dname, caps, &next, perms);
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

int Client::link(const char *relexisting, const char *relpath, const UserPerm& perm)
{
  std::lock_guard lock(client_lock);
  tout(cct) << "link" << std::endl;
  tout(cct) << relexisting << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath existing(relexisting);

  InodeRef in, dir;
  int r = path_walk(existing, &in, perm, true);
  if (r < 0)
    return r;
  if (std::string(relpath) == "/") {
    r = -EEXIST;
    return r;
  }
  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();

  r = path_walk(path, &dir, perm, true);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    if (S_ISDIR(in->mode)) {
      r = -EPERM;
      return r;
    }
    r = may_hardlink(in.get(), perm);
    if (r < 0)
      return r;
    r = may_create(dir.get(), perm);
    if (r < 0)
      return r;
  }
  r = _link(in.get(), dir.get(), name.c_str(), perm);
  return r;
}

int Client::unlink(const char *relpath, const UserPerm& perm)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  if (std::string(relpath) == "/")
    return -EISDIR;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  InodeRef dir;
  int r = path_walk(path, &dir, perm);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    r = may_delete(dir.get(), name.c_str(), perm);
    if (r < 0)
      return r;
  }
  return _unlink(dir.get(), name.c_str(), perm);
}

int Client::rename(const char *relfrom, const char *relto, const UserPerm& perm)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relfrom << std::endl;
  tout(cct) << relto << std::endl;

  if (unmounting)
    return -ENOTCONN;

  if (std::string(relfrom) == "/" || std::string(relto) == "/")
    return -EBUSY;

  filepath from(relfrom);
  filepath to(relto);
  string fromname = from.last_dentry();
  from.pop_dentry();
  string toname = to.last_dentry();
  to.pop_dentry();

  InodeRef fromdir, todir;
  int r = path_walk(from, &fromdir, perm);
  if (r < 0)
    goto out;
  r = path_walk(to, &todir, perm);
  if (r < 0)
    goto out;

  if (cct->_conf->client_permissions) {
    int r = may_delete(fromdir.get(), fromname.c_str(), perm);
    if (r < 0)
      return r;
    r = may_delete(todir.get(), toname.c_str(), perm);
    if (r < 0 && r != -ENOENT)
      return r;
  }
  r = _rename(fromdir.get(), fromname.c_str(), todir.get(), toname.c_str(), perm);
out:
  return r;
}

// dirs

int Client::mkdir(const char *relpath, mode_t mode, const UserPerm& perm)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;
  ldout(cct, 10) << __func__ << ": " << relpath << dendl;

  if (unmounting)
    return -ENOTCONN;

  if (std::string(relpath) == "/")
    return -EEXIST;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  InodeRef dir;
  int r = path_walk(path, &dir, perm);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    r = may_create(dir.get(), perm);
    if (r < 0)
      return r;
  }
  return _mkdir(dir.get(), name.c_str(), mode, perm);
}

int Client::mkdirs(const char *relpath, mode_t mode, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 10) << "Client::mkdirs " << relpath << dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;

  if (unmounting)
    return -ENOTCONN;

  //get through existing parts of path
  filepath path(relpath);
  unsigned int i;
  int r = 0, caps = 0;
  InodeRef cur, next;
  cur = cwd;
  for (i=0; i<path.depth(); ++i) {
    if (cct->_conf->client_permissions) {
      r = may_lookup(cur.get(), perms);
      if (r < 0)
	break;
      caps = CEPH_CAP_AUTH_SHARED;
    }
    r = _lookup(cur.get(), path[i].c_str(), caps, &next, perms);
    if (r < 0)
      break;
    cur.swap(next);
  }
  if (r!=-ENOENT) return r;
  ldout(cct, 20) << __func__ << " got through " << i << " directories on path " << relpath << dendl;
  //make new directory at each level
  for (; i<path.depth(); ++i) {
    if (cct->_conf->client_permissions) {
      r = may_create(cur.get(), perms);
      if (r < 0)
	return r;
    }
    //make new dir
    r = _mkdir(cur.get(), path[i].c_str(), mode, perms, &next);
    
    //check proper creation/existence
    if(-EEXIST == r && i < path.depth() - 1) {
      r = _lookup(cur.get(), path[i].c_str(), CEPH_CAP_AUTH_SHARED, &next, perms);
    }	
    if (r < 0) 
      return r;
    //move to new dir and continue
    cur.swap(next);
    ldout(cct, 20) << __func__ << ": successfully created directory "
		   << filepath(cur->ino).get_path() << dendl;
  }
  return 0;
}

int Client::rmdir(const char *relpath, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  if (std::string(relpath) == "/")
    return -EBUSY;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  InodeRef dir;
  int r = path_walk(path, &dir, perms);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    int r = may_delete(dir.get(), name.c_str(), perms);
    if (r < 0)
      return r;
  }
  return _rmdir(dir.get(), name.c_str(), perms);
}

int Client::mknod(const char *relpath, mode_t mode, const UserPerm& perms, dev_t rdev) 
{ 
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << rdev << std::endl;

  if (unmounting)
    return -ENOTCONN;

  if (std::string(relpath) == "/")
    return -EEXIST;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  InodeRef dir;
  int r = path_walk(path, &dir, perms);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    int r = may_create(dir.get(), perms);
    if (r < 0)
      return r;
  }
  return _mknod(dir.get(), name.c_str(), mode, rdev, perms);
}

// symlinks
  
int Client::symlink(const char *target, const char *relpath, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << target << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  if (std::string(relpath) == "/")
    return -EEXIST;

  filepath path(relpath);
  string name = path.last_dentry();
  path.pop_dentry();
  InodeRef dir;
  int r = path_walk(path, &dir, perms);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    int r = may_create(dir.get(), perms);
    if (r < 0)
      return r;
  }
  return _symlink(dir.get(), name.c_str(), target, perms);
}

int Client::readlink(const char *relpath, char *buf, loff_t size, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms, false);
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

int Client::_getattr(Inode *in, int mask, const UserPerm& perms, bool force)
{
  bool yes = in->caps_issued_mask(mask, true);

  ldout(cct, 10) << __func__ << " mask " << ccap_string(mask) << " issued=" << yes << dendl;
  if (yes && !force)
    return 0;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_GETATTR);
  filepath path;
  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_inode(in);
  req->head.args.getattr.mask = mask;
  
  int res = make_request(req, perms);
  ldout(cct, 10) << __func__ << " result=" << res << dendl;
  return res;
}

int Client::_do_setattr(Inode *in, struct ceph_statx *stx, int mask,
			const UserPerm& perms, InodeRef *inp)
{
  int issued = in->caps_issued();

  ldout(cct, 10) << __func__ << " mask " << mask << " issued " <<
    ccap_string(issued) << dendl;

  if (in->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if ((mask & CEPH_SETATTR_SIZE) &&
      (unsigned long)stx->stx_size > in->size &&
      is_quota_bytes_exceeded(in, (unsigned long)stx->stx_size - in->size,
			      perms)) {
    return -EDQUOT;
  }

  // make the change locally?
  if ((in->cap_dirtier_uid >= 0 && perms.uid() != in->cap_dirtier_uid) ||
      (in->cap_dirtier_gid >= 0 && perms.gid() != in->cap_dirtier_gid)) {
    ldout(cct, 10) << __func__ << " caller " << perms.uid() << ":" << perms.gid()
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
    in->ctime = ceph_clock_now();
    in->cap_dirtier_uid = perms.uid();
    in->cap_dirtier_gid = perms.gid();
    if (issued & CEPH_CAP_AUTH_EXCL)
      in->mark_caps_dirty(CEPH_CAP_AUTH_EXCL);
    else if (issued & CEPH_CAP_FILE_EXCL)
      in->mark_caps_dirty(CEPH_CAP_FILE_EXCL);
    else if (issued & CEPH_CAP_XATTR_EXCL)
      in->mark_caps_dirty(CEPH_CAP_XATTR_EXCL);
    else
      mask |= CEPH_SETATTR_CTIME;
  }

  if (in->caps_issued_mask(CEPH_CAP_AUTH_EXCL)) {
    bool kill_sguid = mask & (CEPH_SETATTR_SIZE|CEPH_SETATTR_KILL_SGUID);

    mask &= ~CEPH_SETATTR_KILL_SGUID;

    if (mask & CEPH_SETATTR_UID) {
      in->ctime = ceph_clock_now();
      in->cap_dirtier_uid = perms.uid();
      in->cap_dirtier_gid = perms.gid();
      in->uid = stx->stx_uid;
      in->mark_caps_dirty(CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_UID;
      kill_sguid = true;
      ldout(cct,10) << "changing uid to " << stx->stx_uid << dendl;
    }
    if (mask & CEPH_SETATTR_GID) {
      in->ctime = ceph_clock_now();
      in->cap_dirtier_uid = perms.uid();
      in->cap_dirtier_gid = perms.gid();
      in->gid = stx->stx_gid;
      in->mark_caps_dirty(CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_GID;
      kill_sguid = true;
      ldout(cct,10) << "changing gid to " << stx->stx_gid << dendl;
    }

    if (mask & CEPH_SETATTR_MODE) {
      in->ctime = ceph_clock_now();
      in->cap_dirtier_uid = perms.uid();
      in->cap_dirtier_gid = perms.gid();
      in->mode = (in->mode & ~07777) | (stx->stx_mode & 07777);
      in->mark_caps_dirty(CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_MODE;
      ldout(cct,10) << "changing mode to " << stx->stx_mode << dendl;
    } else if (kill_sguid && S_ISREG(in->mode) && (in->mode & (S_IXUSR|S_IXGRP|S_IXOTH))) {
      /* Must squash the any setuid/setgid bits with an ownership change */
      in->mode &= ~(S_ISUID|S_ISGID);
      in->mark_caps_dirty(CEPH_CAP_AUTH_EXCL);
    }

    if (mask & CEPH_SETATTR_BTIME) {
      in->ctime = ceph_clock_now();
      in->cap_dirtier_uid = perms.uid();
      in->cap_dirtier_gid = perms.gid();
      in->btime = utime_t(stx->stx_btime);
      in->mark_caps_dirty(CEPH_CAP_AUTH_EXCL);
      mask &= ~CEPH_SETATTR_BTIME;
      ldout(cct,10) << "changing btime to " << in->btime << dendl;
    }
  } else if (mask & CEPH_SETATTR_SIZE) {
    /* If we don't have Ax, then we must ask the server to clear them on truncate */
    mask |= CEPH_SETATTR_KILL_SGUID;
  }

  if (in->caps_issued_mask(CEPH_CAP_FILE_EXCL)) {
    if (mask & (CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME)) {
      if (mask & CEPH_SETATTR_MTIME)
        in->mtime = utime_t(stx->stx_mtime);
      if (mask & CEPH_SETATTR_ATIME)
        in->atime = utime_t(stx->stx_atime);
      in->ctime = ceph_clock_now();
      in->cap_dirtier_uid = perms.uid();
      in->cap_dirtier_gid = perms.gid();
      in->time_warp_seq++;
      in->mark_caps_dirty(CEPH_CAP_FILE_EXCL);
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

  in->make_nosnap_relative_path(path);
  req->set_filepath(path);
  req->set_inode(in);

  if (mask & CEPH_SETATTR_KILL_SGUID) {
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
  }
  if (mask & CEPH_SETATTR_MODE) {
    req->head.args.setattr.mode = stx->stx_mode;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
    ldout(cct,10) << "changing mode to " << stx->stx_mode << dendl;
  }
  if (mask & CEPH_SETATTR_UID) {
    req->head.args.setattr.uid = stx->stx_uid;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
    ldout(cct,10) << "changing uid to " << stx->stx_uid << dendl;
  }
  if (mask & CEPH_SETATTR_GID) {
    req->head.args.setattr.gid = stx->stx_gid;
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
    ldout(cct,10) << "changing gid to " << stx->stx_gid << dendl;
  }
  if (mask & CEPH_SETATTR_BTIME) {
    req->head.args.setattr.btime = utime_t(stx->stx_btime);
    req->inode_drop |= CEPH_CAP_AUTH_SHARED;
  }
  if (mask & CEPH_SETATTR_MTIME) {
    req->head.args.setattr.mtime = utime_t(stx->stx_mtime);
    req->inode_drop |= CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_RD |
      CEPH_CAP_FILE_WR;
  }
  if (mask & CEPH_SETATTR_ATIME) {
    req->head.args.setattr.atime = utime_t(stx->stx_atime);
    req->inode_drop |= CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_RD |
      CEPH_CAP_FILE_WR;
  }
  if (mask & CEPH_SETATTR_SIZE) {
    if ((unsigned long)stx->stx_size < mdsmap->get_max_filesize()) {
      req->head.args.setattr.size = stx->stx_size;
      ldout(cct,10) << "changing size to " << stx->stx_size << dendl;
    } else { //too big!
      put_request(req);
      ldout(cct,10) << "unable to set size to " << stx->stx_size << ". Too large!" << dendl;
      return -EFBIG;
    }
    req->inode_drop |= CEPH_CAP_FILE_SHARED | CEPH_CAP_FILE_RD |
      CEPH_CAP_FILE_WR;
  }
  req->head.args.setattr.mask = mask;

  req->regetattr_mask = mask;

  int res = make_request(req, perms, inp);
  ldout(cct, 10) << "_setattr result=" << res << dendl;
  return res;
}

/* Note that we only care about attrs that setattr cares about */
void Client::stat_to_statx(struct stat *st, struct ceph_statx *stx)
{
  stx->stx_size = st->st_size;
  stx->stx_mode = st->st_mode;
  stx->stx_uid = st->st_uid;
  stx->stx_gid = st->st_gid;
#ifdef __APPLE__
  stx->stx_mtime = st->st_mtimespec;
  stx->stx_atime = st->st_atimespec;
#else
  stx->stx_mtime = st->st_mtim;
  stx->stx_atime = st->st_atim;
#endif
}

int Client::__setattrx(Inode *in, struct ceph_statx *stx, int mask,
		       const UserPerm& perms, InodeRef *inp)
{
  int ret = _do_setattr(in, stx, mask, perms, inp);
  if (ret < 0)
   return ret;
  if (mask & CEPH_SETATTR_MODE)
    ret = _posix_acl_chmod(in, stx->stx_mode, perms);
  return ret;
}

int Client::_setattrx(InodeRef &in, struct ceph_statx *stx, int mask,
		      const UserPerm& perms)
{
  mask &= (CEPH_SETATTR_MODE | CEPH_SETATTR_UID |
	   CEPH_SETATTR_GID | CEPH_SETATTR_MTIME |
	   CEPH_SETATTR_ATIME | CEPH_SETATTR_SIZE |
	   CEPH_SETATTR_CTIME | CEPH_SETATTR_BTIME);
  if (cct->_conf->client_permissions) {
    int r = may_setattr(in.get(), stx, mask, perms);
    if (r < 0)
      return r;
  }
  return __setattrx(in.get(), stx, mask, perms);
}

int Client::_setattr(InodeRef &in, struct stat *attr, int mask,
		     const UserPerm& perms)
{
  struct ceph_statx stx;

  stat_to_statx(attr, &stx);
  mask &= ~CEPH_SETATTR_BTIME;

  if ((mask & CEPH_SETATTR_UID) && attr->st_uid == static_cast<uid_t>(-1)) {
    mask &= ~CEPH_SETATTR_UID;
  }
  if ((mask & CEPH_SETATTR_GID) && attr->st_gid == static_cast<uid_t>(-1)) {
    mask &= ~CEPH_SETATTR_GID;
  }

  return _setattrx(in, &stx, mask, perms);
}

int Client::setattr(const char *relpath, struct stat *attr, int mask,
		    const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mask  << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms);
  if (r < 0)
    return r;
  return _setattr(in, attr, mask, perms);
}

int Client::setattrx(const char *relpath, struct ceph_statx *stx, int mask,
		     const UserPerm& perms, int flags)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mask  << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms, !(flags & AT_SYMLINK_NOFOLLOW));
  if (r < 0)
    return r;
  return _setattrx(in, stx, mask, perms);
}

int Client::fsetattr(int fd, struct stat *attr, int mask, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << mask  << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  return _setattr(f->inode, attr, mask, perms);
}

int Client::fsetattrx(int fd, struct ceph_statx *stx, int mask, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << mask  << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  return _setattrx(f->inode, stx, mask, perms);
}

int Client::stat(const char *relpath, struct stat *stbuf, const UserPerm& perms,
		 frag_info_t *dirstat, int mask)
{
  ldout(cct, 3) << __func__ << " enter (relpath " << relpath << " mask " << mask << ")" << dendl;
  std::lock_guard lock(client_lock);
  tout(cct) << "stat" << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms, true, mask);
  if (r < 0)
    return r;
  r = _getattr(in, mask, perms);
  if (r < 0) {
    ldout(cct, 3) << __func__ << " exit on error!" << dendl;
    return r;
  }
  fill_stat(in, stbuf, dirstat);
  ldout(cct, 3) << __func__ << " exit (relpath " << relpath << " mask " << mask << ")" << dendl;
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
  if (want & (CEPH_STATX_MODE|CEPH_STATX_UID|CEPH_STATX_GID|CEPH_STATX_BTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION))
    mask |= CEPH_CAP_AUTH_SHARED;
  if (want & (CEPH_STATX_NLINK|CEPH_STATX_CTIME|CEPH_STATX_VERSION))
    mask |= CEPH_CAP_LINK_SHARED;
  if (want & (CEPH_STATX_ATIME|CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_SIZE|CEPH_STATX_BLOCKS|CEPH_STATX_VERSION))
    mask |= CEPH_CAP_FILE_SHARED;
  if (want & (CEPH_STATX_VERSION|CEPH_STATX_CTIME))
    mask |= CEPH_CAP_XATTR_SHARED;
out:
  return mask;
}

int Client::statx(const char *relpath, struct ceph_statx *stx,
		  const UserPerm& perms,
		  unsigned int want, unsigned int flags)
{
  ldout(cct, 3) << __func__ << " enter (relpath " << relpath << " want " << want << ")" << dendl;
  std::lock_guard lock(client_lock);
  tout(cct) << "statx" << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;

  unsigned mask = statx_to_mask(flags, want);

  int r = path_walk(path, &in, perms, !(flags & AT_SYMLINK_NOFOLLOW), mask);
  if (r < 0)
    return r;

  r = _getattr(in, mask, perms);
  if (r < 0) {
    ldout(cct, 3) << __func__ << " exit on error!" << dendl;
    return r;
  }

  fill_statx(in, mask, stx);
  ldout(cct, 3) << __func__ << " exit (relpath " << relpath << " mask " << stx->stx_mask << ")" << dendl;
  return r;
}

int Client::lstat(const char *relpath, struct stat *stbuf,
		  const UserPerm& perms, frag_info_t *dirstat, int mask)
{
  ldout(cct, 3) << __func__ << " enter (relpath " << relpath << " mask " << mask << ")" << dendl;
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  // don't follow symlinks
  int r = path_walk(path, &in, perms, false, mask);
  if (r < 0)
    return r;
  r = _getattr(in, mask, perms);
  if (r < 0) {
    ldout(cct, 3) << __func__ << " exit on error!" << dendl;
    return r;
  }
  fill_stat(in, stbuf, dirstat);
  ldout(cct, 3) << __func__ << " exit (relpath " << relpath << " mask " << mask << ")" << dendl;
  return r;
}

int Client::fill_stat(Inode *in, struct stat *st, frag_info_t *dirstat, nest_info_t *rstat)
{
  ldout(cct, 10) << __func__ << " on " << in->ino << " snap/dev" << in->snapid
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
  if (in->is_dir()) {
    switch (in->nlink) {
      case 0:
        st->st_nlink = 0; /* dir is unlinked */
        break;
      case 1:
        st->st_nlink = 1 /* parent dentry */
                       + 1 /* <dir>/. */
                       + in->dirstat.nsubdirs; /* include <dir>/. self-reference */
        break;
      default:
        ceph_abort();
    }
  } else {
    st->st_nlink = in->nlink;
  }
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
  st->st_blksize = std::max<uint32_t>(in->layout.stripe_unit, 4096);

  if (dirstat)
    *dirstat = in->dirstat;
  if (rstat)
    *rstat = in->rstat;

  return in->caps_issued();
}

void Client::fill_statx(Inode *in, unsigned int mask, struct ceph_statx *stx)
{
  ldout(cct, 10) << __func__ << " on " << in->ino << " snap/dev" << in->snapid
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
  stx->stx_dev = in->snapid;
  stx->stx_blksize = std::max<uint32_t>(in->layout.stripe_unit, 4096);

  /* Type bits are always set, even when CEPH_STATX_MODE is not */
  stx->stx_mode = S_IFMT & in->mode;
  stx->stx_ino = use_faked_inos() ? in->faked_ino : (ino_t)in->ino;
  stx->stx_rdev = in->rdev;
  stx->stx_mask |= (CEPH_STATX_INO|CEPH_STATX_RDEV);

  if (mask & CEPH_CAP_AUTH_SHARED) {
    stx->stx_uid = in->uid;
    stx->stx_gid = in->gid;
    stx->stx_mode = in->mode;
    in->btime.to_timespec(&stx->stx_btime);
    stx->stx_mask |= (CEPH_STATX_MODE|CEPH_STATX_UID|CEPH_STATX_GID|CEPH_STATX_BTIME);
  }

  if (mask & CEPH_CAP_LINK_SHARED) {
    if (in->is_dir()) {
      switch (in->nlink) {
        case 0:
          stx->stx_nlink = 0; /* dir is unlinked */
          break;
        case 1:
          stx->stx_nlink = 1 /* parent dentry */
                           + 1 /* <dir>/. */
                           + in->dirstat.nsubdirs; /* include <dir>/. self-reference */
          break;
        default:
          ceph_abort();
      }
    } else {
      stx->stx_nlink = in->nlink;
    }
    stx->stx_mask |= CEPH_STATX_NLINK;
  }

  if (mask & CEPH_CAP_FILE_SHARED) {

    in->atime.to_timespec(&stx->stx_atime);
    in->mtime.to_timespec(&stx->stx_mtime);

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
    stx->stx_mask |= (CEPH_STATX_ATIME|CEPH_STATX_MTIME|
		      CEPH_STATX_SIZE|CEPH_STATX_BLOCKS);
  }

  /* Change time and change_attr both require all shared caps to view */
  if ((mask & CEPH_STAT_CAP_INODE_ALL) == CEPH_STAT_CAP_INODE_ALL) {
    stx->stx_version = in->change_attr;
    if (in->ctime > in->mtime)
      in->ctime.to_timespec(&stx->stx_ctime);
    else
      in->mtime.to_timespec(&stx->stx_ctime);
    stx->stx_mask |= (CEPH_STATX_CTIME|CEPH_STATX_VERSION);
  }

}

void Client::touch_dn(Dentry *dn)
{
  lru.lru_touch(dn);
}

int Client::chmod(const char *relpath, mode_t mode, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_mode = mode;
  return _setattr(in, &attr, CEPH_SETATTR_MODE, perms);
}

int Client::fchmod(int fd, mode_t mode, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << mode << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  struct stat attr;
  attr.st_mode = mode;
  return _setattr(f->inode, &attr, CEPH_SETATTR_MODE, perms);
}

int Client::lchmod(const char *relpath, mode_t mode, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << mode << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  // don't follow symlinks
  int r = path_walk(path, &in, perms, false);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_mode = mode;
  return _setattr(in, &attr, CEPH_SETATTR_MODE, perms);
}

int Client::chown(const char *relpath, uid_t new_uid, gid_t new_gid,
		  const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << new_uid << std::endl;
  tout(cct) << new_gid << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_uid = new_uid;
  attr.st_gid = new_gid;
  return _setattr(in, &attr, CEPH_SETATTR_UID|CEPH_SETATTR_GID, perms);
}

int Client::fchown(int fd, uid_t new_uid, gid_t new_gid, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << new_uid << std::endl;
  tout(cct) << new_gid << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  struct stat attr;
  attr.st_uid = new_uid;
  attr.st_gid = new_gid;
  int mask = 0;
  if (new_uid != static_cast<uid_t>(-1)) mask |= CEPH_SETATTR_UID;
  if (new_gid != static_cast<gid_t>(-1)) mask |= CEPH_SETATTR_GID;
  return _setattr(f->inode, &attr, mask, perms);
}

int Client::lchown(const char *relpath, uid_t new_uid, gid_t new_gid,
		   const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << new_uid << std::endl;
  tout(cct) << new_gid << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  // don't follow symlinks
  int r = path_walk(path, &in, perms, false);
  if (r < 0)
    return r;
  struct stat attr;
  attr.st_uid = new_uid;
  attr.st_gid = new_gid;
  int mask = 0;
  if (new_uid != static_cast<uid_t>(-1)) mask |= CEPH_SETATTR_UID;
  if (new_gid != static_cast<gid_t>(-1)) mask |= CEPH_SETATTR_GID;
  return _setattr(in, &attr, mask, perms);
}

static void attr_set_atime_and_mtime(struct stat *attr,
                                     const utime_t &atime,
                                     const utime_t &mtime)
{
  stat_set_atime_sec(attr, atime.tv.tv_sec);
  stat_set_atime_nsec(attr, atime.tv.tv_nsec);
  stat_set_mtime_sec(attr, mtime.tv.tv_sec);
  stat_set_mtime_nsec(attr, mtime.tv.tv_nsec);
}

// for [l]utime() invoke the timeval variant as the timespec
// variant are not yet implemented. for futime[s](), invoke
// the timespec variant.
int Client::utime(const char *relpath, struct utimbuf *buf,
		  const UserPerm& perms)
{
  struct timeval tv[2];
  tv[0].tv_sec  = buf->actime;
  tv[0].tv_usec = 0;
  tv[1].tv_sec  = buf->modtime;
  tv[1].tv_usec = 0;

  return utimes(relpath, tv, perms);
}

int Client::lutime(const char *relpath, struct utimbuf *buf,
		   const UserPerm& perms)
{
  struct timeval tv[2];
  tv[0].tv_sec  = buf->actime;
  tv[0].tv_usec = 0;
  tv[1].tv_sec  = buf->modtime;
  tv[1].tv_usec = 0;

  return lutimes(relpath, tv, perms);
}

int Client::futime(int fd, struct utimbuf *buf, const UserPerm& perms)
{
  struct timespec ts[2];
  ts[0].tv_sec  = buf->actime;
  ts[0].tv_nsec = 0;
  ts[1].tv_sec  = buf->modtime;
  ts[1].tv_nsec = 0;

  return futimens(fd, ts, perms);
}

int Client::utimes(const char *relpath, struct timeval times[2],
                   const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << "atime: " << times[0].tv_sec << "." << times[0].tv_usec
            << std::endl;
  tout(cct) << "mtime: " << times[1].tv_sec << "." << times[1].tv_usec
            << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms);
  if (r < 0)
    return r;
  struct stat attr;
  utime_t atime(times[0]);
  utime_t mtime(times[1]);

  attr_set_atime_and_mtime(&attr, atime, mtime);
  return _setattr(in, &attr, CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME, perms);
}

int Client::lutimes(const char *relpath, struct timeval times[2],
                    const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << "atime: " << times[0].tv_sec << "." << times[0].tv_usec
            << std::endl;
  tout(cct) << "mtime: " << times[1].tv_sec << "." << times[1].tv_usec
            << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms, false);
  if (r < 0)
    return r;
  struct stat attr;
  utime_t atime(times[0]);
  utime_t mtime(times[1]);

  attr_set_atime_and_mtime(&attr, atime, mtime);
  return _setattr(in, &attr, CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME, perms);
}

int Client::futimes(int fd, struct timeval times[2], const UserPerm& perms)
{
  struct timespec ts[2];
  ts[0].tv_sec  = times[0].tv_sec;
  ts[0].tv_nsec = times[0].tv_usec * 1000;
  ts[1].tv_sec  = times[1].tv_sec;
  ts[1].tv_nsec = times[1].tv_usec * 1000;

  return futimens(fd, ts, perms);
}

int Client::futimens(int fd, struct timespec times[2], const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << "atime: " << times[0].tv_sec << "." << times[0].tv_nsec
            << std::endl;
  tout(cct) << "mtime: " << times[1].tv_sec << "." << times[1].tv_nsec
            << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  struct stat attr;
  utime_t atime(times[0]);
  utime_t mtime(times[1]);

  attr_set_atime_and_mtime(&attr, atime, mtime);
  return _setattr(f->inode, &attr, CEPH_SETATTR_MTIME|CEPH_SETATTR_ATIME, perms);
}

int Client::flock(int fd, int operation, uint64_t owner)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << operation << std::endl;
  tout(cct) << owner << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;

  return _flock(f, operation, owner);
}

int Client::opendir(const char *relpath, dir_result_t **dirpp, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms, true);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    int r = may_open(in.get(), O_RDONLY, perms);
    if (r < 0)
      return r;
  }
  r = _opendir(in.get(), dirpp, perms);
  /* if ENOTDIR, dirpp will be an uninitialized point and it's very dangerous to access its value */
  if (r != -ENOTDIR)
      tout(cct) << (unsigned long)*dirpp << std::endl;
  return r;
}

int Client::_opendir(Inode *in, dir_result_t **dirpp, const UserPerm& perms)
{
  if (!in->is_dir())
    return -ENOTDIR;
  *dirpp = new dir_result_t(in, perms);
  opened_dirs.insert(*dirpp);
  ldout(cct, 8) << __func__ << "(" << in->ino << ") = " << 0 << " (" << *dirpp << ")" << dendl;
  return 0;
}


int Client::closedir(dir_result_t *dir) 
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << (unsigned long)dir << std::endl;

  ldout(cct, 3) << __func__ << "(" << dir << ") = 0" << dendl;
  _closedir(dir);
  return 0;
}

void Client::_closedir(dir_result_t *dirp)
{
  ldout(cct, 10) << __func__ << "(" << dirp << ")" << dendl;
  if (dirp->inode) {
    ldout(cct, 10) << __func__ << " detaching inode " << dirp->inode << dendl;
    dirp->inode.reset();
  }
  _readdir_drop_dirp_buffer(dirp);
  opened_dirs.erase(dirp);
  delete dirp;
}

void Client::rewinddir(dir_result_t *dirp)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << __func__ << "(" << dirp << ")" << dendl;

  if (unmounting)
    return;

  dir_result_t *d = static_cast<dir_result_t*>(dirp);
  _readdir_drop_dirp_buffer(d);
  d->reset();
}
 
loff_t Client::telldir(dir_result_t *dirp)
{
  dir_result_t *d = static_cast<dir_result_t*>(dirp);
  ldout(cct, 3) << __func__ << "(" << dirp << ") = " << d->offset << dendl;
  return d->offset;
}

void Client::seekdir(dir_result_t *dirp, loff_t offset)
{
  std::lock_guard lock(client_lock);

  ldout(cct, 3) << __func__ << "(" << dirp << ", " << offset << ")" << dendl;

  if (unmounting)
    return;

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
#if !defined(__APPLE__) && !defined(__FreeBSD__)
  de->d_off = next_off;
#endif
  de->d_reclen = 1;
  de->d_type = IFTODT(type);
  ldout(cct, 10) << __func__ << " '" << de->d_name << "' -> " << inodeno_t(de->d_ino)
	   << " type " << (int)de->d_type << " w/ next_off " << hex << next_off << dec << dendl;
#endif
}

void Client::_readdir_next_frag(dir_result_t *dirp)
{
  frag_t fg = dirp->buffer_frag;

  if (fg.is_rightmost()) {
    ldout(cct, 10) << __func__ << " advance from " << fg << " to END" << dendl;
    dirp->set_end();
    return;
  }

  // advance
  fg = fg.next();
  ldout(cct, 10) << __func__ << " advance from " << dirp->buffer_frag << " to " << fg << dendl;

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
  ceph_assert(dirp->inode);

  if (dirp->hash_order())
    return;

  frag_t cur = frag_t(dirp->offset_high());
  frag_t fg = dirp->inode->dirfragtree[cur.value()];
  if (fg != cur) {
    ldout(cct, 10) << __func__ << " frag " << cur << " maps to " << fg << dendl;
    dirp->offset = dir_result_t::make_fpos(fg, 2, false);
    dirp->last_name.clear();
    dirp->next_offset = 2;
  }
}

void Client::_readdir_drop_dirp_buffer(dir_result_t *dirp)
{
  ldout(cct, 10) << __func__ << " " << dirp << dendl;
  dirp->buffer.clear();
}

int Client::_readdir_get_frag(dir_result_t *dirp)
{
  ceph_assert(dirp);
  ceph_assert(dirp->inode);

  // get the current frag.
  frag_t fg;
  if (dirp->hash_order())
    fg = dirp->inode->dirfragtree[dirp->offset_high()];
  else
    fg = frag_t(dirp->offset_high());
  
  ldout(cct, 10) << __func__ << " " << dirp << " on " << dirp->inode->ino << " fg " << fg
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
    req->path2.set_path(dirp->last_name);
  } else if (dirp->hash_order()) {
    req->head.args.readdir.offset_hash = dirp->offset_high();
  }
  req->dirp = dirp;
  
  bufferlist dirbl;
  int res = make_request(req, dirp->perms, NULL, NULL, -1, &dirbl);
  
  if (res == -EAGAIN) {
    ldout(cct, 10) << __func__ << " got EAGAIN, retrying" << dendl;
    _readdir_rechoose_frag(dirp);
    return _readdir_get_frag(dirp);
  }

  if (res == 0) {
    ldout(cct, 10) << __func__ << " " << dirp << " got frag " << dirp->buffer_frag
		   << " size " << dirp->buffer.size() << dendl;
  } else {
    ldout(cct, 10) << __func__ << " got error " << res << ", setting end flag" << dendl;
    dirp->set_end();
  }

  return res;
}

struct dentry_off_lt {
  bool operator()(const Dentry* dn, int64_t off) const {
    return dir_result_t::fpos_cmp(dn->offset, off) < 0;
  }
};

int Client::_readdir_cache_cb(dir_result_t *dirp, add_dirent_cb_t cb, void *p,
			      int caps, bool getref)
{
  ceph_assert(ceph_mutex_is_locked(client_lock));
  ldout(cct, 10) << __func__ << " " << dirp << " on " << dirp->inode->ino
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

    int idx = pd - dir->readdir_cache.begin();
    int r = _getattr(dn->inode, caps, dirp->perms);
    if (r < 0)
      return r;
    
    // the content of readdir_cache may change after _getattr(), so pd may be invalid iterator    
    pd = dir->readdir_cache.begin() + idx;
    if (pd >= dir->readdir_cache.end() || *pd != dn)
      return -EAGAIN;

    struct ceph_statx stx;
    struct dirent de;
    fill_statx(dn->inode, caps, &stx);

    uint64_t next_off = dn->offset + 1;
    fill_dirent(&de, dn->name.c_str(), stx.stx_mode, stx.stx_ino, next_off);
    ++pd;
    if (pd == dir->readdir_cache.end())
      next_off = dir_result_t::END;

    Inode *in = NULL;
    if (getref) {
      in = dn->inode.get();
      _ll_get(in);
    }

    dn_name = dn->name; // fill in name while we have lock

    client_lock.unlock();
    r = cb(p, &de, &stx, next_off, in);  // _next_ offset
    client_lock.lock();
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
    dirp->release_count = 0; // last_name no longer match cache index
    if (r > 0)
      return r;
  }

  ldout(cct, 10) << __func__ << " " << dirp << " on " << dirp->inode->ino << " at end" << dendl;
  dirp->set_end();
  return 0;
}

int Client::readdir_r_cb(dir_result_t *d, add_dirent_cb_t cb, void *p,
			 unsigned want, unsigned flags, bool getref)
{
  int caps = statx_to_mask(flags, want);

  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  dir_result_t *dirp = static_cast<dir_result_t*>(d);

  ldout(cct, 10) << __func__ << " " << *dirp->inode << " offset " << hex << dirp->offset
		 << dec << " at_end=" << dirp->at_end()
		 << " hash_order=" << dirp->hash_order() << dendl;

  struct dirent de;
  struct ceph_statx stx;
  memset(&de, 0, sizeof(de));
  memset(&stx, 0, sizeof(stx));

  InodeRef& diri = dirp->inode;

  if (dirp->at_end())
    return 0;

  if (dirp->offset == 0) {
    ldout(cct, 15) << " including ." << dendl;
    ceph_assert(diri->dentries.size() < 2); // can't have multiple hard-links to a dir
    uint64_t next_off = 1;

    int r;
    r = _getattr(diri, caps, dirp->perms);
    if (r < 0)
      return r;

    fill_statx(diri, caps, &stx);
    fill_dirent(&de, ".", S_IFDIR, stx.stx_ino, next_off);

    Inode *inode = NULL;
    if (getref) {
      inode = diri.get();
      _ll_get(inode);
    }

    client_lock.unlock();
    r = cb(p, &de, &stx, next_off, inode);
    client_lock.lock();
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
    if (diri->dentries.empty())
      in = diri;
    else
      in = diri->get_first_parent()->dir->parent_inode;

    int r;
    r = _getattr(in, caps, dirp->perms);
    if (r < 0)
      return r;

    fill_statx(in, caps, &stx);
    fill_dirent(&de, "..", S_IFDIR, stx.stx_ino, next_off);

    Inode *inode = NULL;
    if (getref) {
      inode = in.get();
      _ll_get(inode);
    }

    client_lock.unlock();
    r = cb(p, &de, &stx, next_off, inode);
    client_lock.lock();
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
      dirp->inode->caps_issued_mask(CEPH_CAP_FILE_SHARED, true)) {
    int err = _readdir_cache_cb(dirp, cb, p, caps, getref);
    if (err != -EAGAIN)
      return err;
  }

  while (1) {
    if (dirp->at_end())
      return 0;

    bool check_caps = true;
    if (!dirp->is_cached()) {
      int r = _readdir_get_frag(dirp);
      if (r)
	return r;
      // _readdir_get_frag () may updates dirp->offset if the replied dirfrag is
      // different than the requested one. (our dirfragtree was outdated)
      check_caps = false;
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

      int r;
      if (check_caps) {
	r = _getattr(entry.inode, caps, dirp->perms);
	if (r < 0)
	  return r;
      }

      fill_statx(entry.inode, caps, &stx);
      fill_dirent(&de, entry.name.c_str(), stx.stx_mode, stx.stx_ino, next_off);

      Inode *inode = NULL;
      if (getref) {
	inode = entry.inode.get();
	_ll_get(inode);
      }

      client_lock.unlock();
      r = cb(p, &de, &stx, next_off, inode);  // _next_ offset
      client_lock.lock();

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
	  ceph_assert(diri->dir->readdir_cache.size() >= dirp->cache_index);
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
  ceph_abort();
  return 0;
}


int Client::readdir_r(dir_result_t *d, struct dirent *de)
{  
  return readdirplus_r(d, de, 0, 0, 0, NULL);
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
  struct ceph_statx *stx;
  Inode *inode;
  bool full;
};

static int _readdir_single_dirent_cb(void *p, struct dirent *de,
				     struct ceph_statx *stx, off_t off,
				     Inode *in)
{
  single_readdir *c = static_cast<single_readdir *>(p);

  if (c->full)
    return -1;  // already filled this dirent

  *c->de = *de;
  if (c->stx)
    *c->stx = *stx;
  c->inode = in;
  c->full = true;
  return 1;
}

struct dirent *Client::readdir(dir_result_t *d)
{
  int ret;
  static struct dirent de;
  single_readdir sr;
  sr.de = &de;
  sr.stx = NULL;
  sr.inode = NULL;
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

int Client::readdirplus_r(dir_result_t *d, struct dirent *de,
			  struct ceph_statx *stx, unsigned want,
			  unsigned flags, Inode **out)
{  
  single_readdir sr;
  sr.de = de;
  sr.stx = stx;
  sr.inode = NULL;
  sr.full = false;

  // our callback fills the dirent and sets sr.full=true on first
  // call, and returns -1 the second time around.
  int r = readdir_r_cb(d, _readdir_single_dirent_cb, (void *)&sr, want, flags, out);
  if (r < -1)
    return r;
  if (out)
    *out = sr.inode;
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

static int _readdir_getdent_cb(void *p, struct dirent *de,
			       struct ceph_statx *stx, off_t off, Inode *in)
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

static int _getdir_cb(void *p, struct dirent *de, struct ceph_statx *stx, off_t off, Inode *in)
{
  getdir_result *r = static_cast<getdir_result *>(p);

  r->contents->push_back(de->d_name);
  r->num++;
  return 0;
}

int Client::getdir(const char *relpath, list<string>& contents,
		   const UserPerm& perms)
{
  ldout(cct, 3) << "getdir(" << relpath << ")" << dendl;
  {
    std::lock_guard lock(client_lock);
    tout(cct) << "getdir" << std::endl;
    tout(cct) << relpath << std::endl;
  }

  dir_result_t *d;
  int r = opendir(relpath, &d, perms);
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
int Client::open(const char *relpath, int flags, const UserPerm& perms,
		 mode_t mode, int stripe_unit, int stripe_count,
		 int object_size, const char *data_pool)
{
  ldout(cct, 3) << "open enter(" << relpath << ", " << ceph_flags_sys2wire(flags) << "," << mode << ")" << dendl;
  std::lock_guard lock(client_lock);
  tout(cct) << "open" << std::endl;
  tout(cct) << relpath << std::endl;
  tout(cct) << ceph_flags_sys2wire(flags) << std::endl;

  if (unmounting)
    return -ENOTCONN;

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
  int r = path_walk(path, &in, perms, followsym, ceph_caps_for_mode(mode));

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
    r = path_walk(dirpath, &dir, perms, true,
		  cct->_conf->client_permissions ? CEPH_CAP_AUTH_SHARED : 0);
    if (r < 0)
      goto out;
    if (cct->_conf->client_permissions) {
      r = may_create(dir.get(), perms);
      if (r < 0)
	goto out;
    }
    r = _create(dir.get(), dname.c_str(), flags, mode, &in, &fh, stripe_unit,
                stripe_count, object_size, data_pool, &created, perms);
  }
  if (r < 0)
    goto out;

  if (!created) {
    // posix says we can only check permissions of existing files
    if (cct->_conf->client_permissions) {
      r = may_open(in.get(), flags, perms);
      if (r < 0)
	goto out;
    }
  }

  if (!fh)
    r = _open(in.get(), flags, mode, &fh, perms);
  if (r >= 0) {
    // allocate a integer file descriptor
    ceph_assert(fh);
    r = get_fd();
    ceph_assert(fd_map.count(r) == 0);
    fd_map[r] = fh;
  }
  
 out:
  tout(cct) << r << std::endl;
  ldout(cct, 3) << "open exit(" << path << ", " << ceph_flags_sys2wire(flags) << ") = " << r << dendl;
  return r;
}

int Client::open(const char *relpath, int flags, const UserPerm& perms, mode_t mode)
{
  /* Use default file striping parameters */
  return open(relpath, flags, perms, mode, 0, 0, 0, NULL);
}

int Client::lookup_hash(inodeno_t ino, inodeno_t dirino, const char *name,
			const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << __func__ << " enter(" << ino << ", #" << dirino << "/" << name << ")" << dendl;

  if (unmounting)
    return -ENOTCONN;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPHASH);
  filepath path(ino);
  req->set_filepath(path);

  uint32_t h = ceph_str_hash(CEPH_STR_HASH_RJENKINS, name, strlen(name));
  char f[30];
  sprintf(f, "%u", h);
  filepath path2(dirino);
  path2.push_dentry(string(f));
  req->set_filepath2(path2);

  int r = make_request(req, perms, NULL, NULL,
		       rand() % mdsmap->get_num_in_mds());
  ldout(cct, 3) << __func__ << " exit(" << ino << ", #" << dirino << "/" << name << ") = " << r << dendl;
  return r;
}


/**
 * Load inode into local cache.
 *
 * If inode pointer is non-NULL, and take a reference on
 * the resulting Inode object in one operation, so that caller
 * can safely assume inode will still be there after return.
 */
int Client::_lookup_ino(inodeno_t ino, const UserPerm& perms, Inode **inode)
{
  ldout(cct, 8) << __func__ << " enter(" << ino << ")" << dendl;

  if (unmounting)
    return -ENOTCONN;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPINO);
  filepath path(ino);
  req->set_filepath(path);

  int r = make_request(req, perms, NULL, NULL, rand() % mdsmap->get_num_in_mds());
  if (r == 0 && inode != NULL) {
    vinodeno_t vino(ino, CEPH_NOSNAP);
    unordered_map<vinodeno_t,Inode*>::iterator p = inode_map.find(vino);
    ceph_assert(p != inode_map.end());
    *inode = p->second;
    _ll_get(*inode);
  }
  ldout(cct, 8) << __func__ << " exit(" << ino << ") = " << r << dendl;
  return r;
}

int Client::lookup_ino(inodeno_t ino, const UserPerm& perms, Inode **inode)
{
  std::lock_guard lock(client_lock);
  return _lookup_ino(ino, perms, inode);
}

/**
 * Find the parent inode of `ino` and insert it into
 * our cache.  Conditionally also set `parent` to a referenced
 * Inode* if caller provides non-NULL value.
 */
int Client::_lookup_parent(Inode *ino, const UserPerm& perms, Inode **parent)
{
  ldout(cct, 8) << __func__ << " enter(" << ino->ino << ")" << dendl;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPPARENT);
  filepath path(ino->ino);
  req->set_filepath(path);

  InodeRef target;
  int r = make_request(req, perms, &target, NULL, rand() % mdsmap->get_num_in_mds());
  // Give caller a reference to the parent ino if they provided a pointer.
  if (parent != NULL) {
    if (r == 0) {
      *parent = target.get();
      _ll_get(*parent);
      ldout(cct, 8) << __func__ << " found parent " << (*parent)->ino << dendl;
    } else {
      *parent = NULL;
    }
  }
  ldout(cct, 8) << __func__ << " exit(" << ino->ino << ") = " << r << dendl;
  return r;
}

/**
 * Populate the parent dentry for `ino`, provided it is
 * a child of `parent`.
 */
int Client::_lookup_name(Inode *ino, Inode *parent, const UserPerm& perms)
{
  ceph_assert(parent->is_dir());
  ldout(cct, 3) << __func__ << " enter(" << ino->ino << ")" << dendl;

  if (unmounting)
    return -ENOTCONN;

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPNAME);
  req->set_filepath2(filepath(parent->ino));
  req->set_filepath(filepath(ino->ino));
  req->set_inode(ino);

  int r = make_request(req, perms, NULL, NULL, rand() % mdsmap->get_num_in_mds());
  ldout(cct, 3) << __func__ << " exit(" << ino->ino << ") = " << r << dendl;
  return r;
}

int Client::lookup_name(Inode *ino, Inode *parent, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  return _lookup_name(ino, parent, perms);
}

Fh *Client::_create_fh(Inode *in, int flags, int cmode, const UserPerm& perms)
{
  ceph_assert(in);
  Fh *f = new Fh(in, flags, cmode, fd_gen, perms);

  ldout(cct, 10) << __func__ << " " << in->ino << " mode " << cmode << dendl;

  if (in->snapid != CEPH_NOSNAP) {
    in->snap_cap_refs++;
    ldout(cct, 5) << "open success, fh is " << f << " combined IMMUTABLE SNAP caps " 
	    << ccap_string(in->caps_issued()) << dendl;
  }

  const auto& conf = cct->_conf;
  f->readahead.set_trigger_requests(1);
  f->readahead.set_min_readahead_size(conf->client_readahead_min);
  uint64_t max_readahead = Readahead::NO_LIMIT;
  if (conf->client_readahead_max_bytes) {
    max_readahead = std::min(max_readahead, (uint64_t)conf->client_readahead_max_bytes);
  }
  if (conf->client_readahead_max_periods) {
    max_readahead = std::min(max_readahead, in->layout.get_period()*(uint64_t)conf->client_readahead_max_periods);
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
  ldout(cct, 8) << __func__ << " " << f << " mode " << f->mode << " on " << *in << dendl;

  in->unset_deleg(f);

  if (in->snapid == CEPH_NOSNAP) {
    if (in->put_open_ref(f->mode)) {
      _flush(in, new C_Client_FlushComplete(this, in));
      check_caps(in, 0);
    }
  } else {
    ceph_assert(in->snap_cap_refs > 0);
    in->snap_cap_refs--;
  }

  _release_filelocks(f);

  // Finally, read any async err (i.e. from flushes)
  int err = f->take_async_err();
  if (err != 0) {
    ldout(cct, 1) << __func__ << " " << f << " on inode " << *in << " caught async_err = "
                  << cpp_strerror(err) << dendl;
  } else {
    ldout(cct, 10) << __func__ << " " << f << " on inode " << *in << " no async_err state" << dendl;
  }

  _put_fh(f);

  return err;
}

void Client::_put_fh(Fh *f)
{
  int left = f->put();
  if (!left) {
    delete f;
  }
}

int Client::_open(Inode *in, int flags, mode_t mode, Fh **fhp,
		  const UserPerm& perms)
{
  if (in->snapid != CEPH_NOSNAP &&
      (flags & (O_WRONLY | O_RDWR | O_CREAT | O_TRUNC | O_APPEND))) {
    return -EROFS;
  }

  // use normalized flags to generate cmode
  int cflags = ceph_flags_sys2wire(flags);
  if (cct->_conf.get_val<bool>("client_force_lazyio"))
    cflags |= CEPH_O_LAZY;

  int cmode = ceph_flags_to_mode(cflags);
  int want = ceph_caps_for_mode(cmode);
  int result = 0;

  in->get_open_ref(cmode);  // make note of pending open, since it effects _wanted_ caps.

  if ((flags & O_TRUNC) == 0 && in->caps_issued_mask(want)) {
    // update wanted?
    check_caps(in, CHECK_CAPS_NODELAY);
  } else {

    MetaRequest *req = new MetaRequest(CEPH_MDS_OP_OPEN);
    filepath path;
    in->make_nosnap_relative_path(path);
    req->set_filepath(path);
    req->head.args.open.flags = cflags & ~CEPH_O_CREAT;
    req->head.args.open.mode = mode;
    req->head.args.open.pool = -1;
    if (cct->_conf->client_debug_getattr_caps)
      req->head.args.open.mask = DEBUG_GETATTR_CAPS;
    else
      req->head.args.open.mask = 0;
    req->head.args.open.old_size = in->size;   // for O_TRUNC
    req->set_inode(in);
    result = make_request(req, perms);

    /*
     * NFS expects that delegations will be broken on a conflicting open,
     * not just when there is actual conflicting access to the file. SMB leases
     * and oplocks also have similar semantics.
     *
     * Ensure that clients that have delegations enabled will wait on minimal
     * caps during open, just to ensure that other clients holding delegations
     * return theirs first.
     */
    if (deleg_timeout && result == 0) {
      int need = 0, have;

      if (cmode & CEPH_FILE_MODE_WR)
        need |= CEPH_CAP_FILE_WR;
      if (cmode & CEPH_FILE_MODE_RD)
        need |= CEPH_CAP_FILE_RD;

      Fh fh(in, flags, cmode, fd_gen, perms);
      result = get_caps(&fh, need, want, &have, -1);
      if (result < 0) {
	ldout(cct, 8) << "Unable to get caps after open of inode " << *in <<
			  " . Denying open: " <<
			  cpp_strerror(result) << dendl;
	in->put_open_ref(cmode);
      } else {
	put_cap_ref(in, need);
      }
    }
  }

  // success?
  if (result >= 0) {
    if (fhp)
      *fhp = _create_fh(in, flags, cmode, perms);
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
    check_caps(in, CHECK_CAPS_NODELAY);
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

  // duplicate in case Cap goes away; not sure if that race is a concern?
  const UserPerm *pperm = in->get_best_perms();
  UserPerm perms;
  if (pperm != NULL)
    perms = *pperm;
  int ret = make_request(req, perms);
  return ret;
}

int Client::close(int fd)
{
  ldout(cct, 3) << "close enter(" << fd << ")" << dendl;
  std::lock_guard lock(client_lock);
  tout(cct) << "close" << std::endl;
  tout(cct) << fd << std::endl;

  if (unmounting)
    return -ENOTCONN;

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
  std::lock_guard lock(client_lock);
  tout(cct) << "lseek" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << offset << std::endl;
  tout(cct) << whence << std::endl;

  if (unmounting)
    return -ENOTCONN;

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
  bool whence_check = false;
  loff_t pos = -1;

  switch (whence) {
  case SEEK_END:
    whence_check = true;
  break;

#ifdef SEEK_DATA
  case SEEK_DATA:
    whence_check = true;
  break;
#endif

#ifdef SEEK_HOLE
  case SEEK_HOLE:
    whence_check = true;
  break;
#endif
  }

  if (whence_check) {
    int r = _getattr(in, CEPH_STAT_CAP_SIZE, f->actor_perms);
    if (r < 0)
      return r;
  }

  switch (whence) {
  case SEEK_SET:
    pos = offset;
    break;

  case SEEK_CUR:
    pos = f->pos + offset;
    break;

  case SEEK_END:
    pos = in->size + offset;
    break;

#ifdef SEEK_DATA
  case SEEK_DATA:
    if (offset < 0 || static_cast<uint64_t>(offset) >= in->size)
      return -ENXIO;
    pos = offset;
    break;
#endif

#ifdef SEEK_HOLE
  case SEEK_HOLE:
    if (offset < 0 || static_cast<uint64_t>(offset) >= in->size)
      return -ENXIO;
    pos = in->size;
    break;
#endif

  default:
    ldout(cct, 1) << __func__ << ": invalid whence value " << whence << dendl;
    return -EINVAL;
  }

  if (pos < 0) {
    return -EINVAL;
  } else {
    f->pos = pos;
  }

  ldout(cct, 8) << "_lseek(" << f << ", " << offset << ", " << whence << ") = " << f->pos << dendl;
  return f->pos;
}


void Client::lock_fh_pos(Fh *f)
{
  ldout(cct, 10) << __func__ << " " << f << dendl;

  if (f->pos_locked || !f->pos_waiters.empty()) {
    ceph::condition_variable cond;
    f->pos_waiters.push_back(&cond);
    ldout(cct, 10) << __func__ << " BLOCKING on " << f << dendl;
    std::unique_lock l{client_lock, std::adopt_lock};
    cond.wait(l, [f, me=&cond] {
      return !f->pos_locked && f->pos_waiters.front() == me;
    });
    l.release();
    ldout(cct, 10) << __func__ << " UNBLOCKING on " << f << dendl;
    ceph_assert(f->pos_waiters.front() == &cond);
    f->pos_waiters.pop_front();
  }

  f->pos_locked = true;
}

void Client::unlock_fh_pos(Fh *f)
{
  ldout(cct, 10) << __func__ << " " << f << dendl;
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
		   ceph::real_clock::now(),
		   0,
		   NULL);

  bufferlist inline_version_bl;
  encode(in->inline_version, inline_version_bl);

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
		   ceph::real_clock::now(),
		   0,
		   onfinish);

  return 0;
}

//

// blocking osd interface

int Client::read(int fd, char *buf, loff_t size, loff_t offset)
{
  std::lock_guard lock(client_lock);
  tout(cct) << "read" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << size << std::endl;
  tout(cct) << offset << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  bufferlist bl;
  /* We can't return bytes written larger than INT_MAX, clamp size to that */
  size = std::min(size, (loff_t)INT_MAX);
  int r = _read(f, offset, size, &bl);
  ldout(cct, 3) << "read(" << fd << ", " << (void*)buf << ", " << size << ", " << offset << ") = " << r << dendl;
  if (r >= 0) {
    bl.begin().copy(bl.length(), buf);
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

int64_t Client::_read(Fh *f, int64_t offset, uint64_t size, bufferlist *bl)
{
  int want, have = 0;
  bool movepos = false;
  std::unique_ptr<C_SaferCond> onuninline;
  int64_t r = 0;
  const auto& conf = cct->_conf;
  Inode *in = f->inode.get();
  utime_t lat;
  utime_t start = ceph_clock_now(); 

  if ((f->mode & CEPH_FILE_MODE_RD) == 0)
    return -EBADF;
  //bool lazy = f->mode == CEPH_FILE_MODE_LAZY;

  if (offset < 0) {
    lock_fh_pos(f);
    offset = f->pos;
    movepos = true;
  }
  loff_t start_pos = offset;

  if (in->inline_version == 0) {
    r = _getattr(in, CEPH_STAT_CAP_INLINE_DATA, f->actor_perms, true);
    if (r < 0) {
      goto done;
    }
    ceph_assert(in->inline_version > 0);
  }

retry:
  if (f->mode & CEPH_FILE_MODE_LAZY)
    want = CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO;
  else
    want = CEPH_CAP_FILE_CACHE;
  r = get_caps(f, CEPH_CAP_FILE_RD, want, &have, -1);
  if (r < 0) {
    goto done;
  }
  if (f->flags & O_DIRECT)
    have &= ~(CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO);

  if (in->inline_version < CEPH_INLINE_NONE) {
    if (!(have & CEPH_CAP_FILE_CACHE)) {
      onuninline.reset(new C_SaferCond("Client::_read_uninline_data flock"));
      uninline_data(in, onuninline.get());
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
        r = endoff - offset;
      } else if ((uint64_t)offset < endoff) {
        bl->append_zero(endoff - offset);
        r = endoff - offset;
      } else {
        r = 0;
      }
      goto success;
    }
  }

  if (!conf->client_debug_force_sync_read &&
      conf->client_oc &&
      (have & (CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_LAZYIO))) {

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
      r = _getattr(in, CEPH_STAT_CAP_SIZE, f->actor_perms);
      if (r < 0)
	goto done;

      // eof?  short read.
      if ((uint64_t)offset < in->size)
	goto retry;
    }
  }

success:
  ceph_assert(r >= 0);
  if (movepos) {
    // adjust fd pos
    f->pos = start_pos + r;
  }
  
  lat = ceph_clock_now();
  lat -= start;
  logger->tinc(l_c_read, lat);

done:
  // done!
  
  if (onuninline) {
    client_lock.unlock();
    int ret = onuninline->wait();
    client_lock.lock();
    if (ret >= 0 || ret == -ECANCELED) {
      in->inline_data.clear();
      in->inline_version = CEPH_INLINE_NONE;
      in->mark_caps_dirty(CEPH_CAP_FILE_WR);
      check_caps(in, 0);
    } else
      r = ret;
  }
  if (have) {
    put_cap_ref(in, CEPH_CAP_FILE_RD);
  }
  if (movepos) {
    unlock_fh_pos(f);
  }
  return r;
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
  const auto& conf = cct->_conf;
  Inode *in = f->inode.get();

  ldout(cct, 10) << __func__ << " " << *in << " " << off << "~" << len << dendl;

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
  int r = 0;
  C_SaferCond onfinish("Client::_read_async flock");
  r = objectcacher->file_read(&in->oset, &in->layout, in->snapid,
			      off, len, bl, 0, &onfinish);
  if (r == 0) {
    get_cap_ref(in, CEPH_CAP_FILE_CACHE);
    client_lock.unlock();
    r = onfinish.wait();
    client_lock.lock();
    put_cap_ref(in, CEPH_CAP_FILE_CACHE);
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

  ldout(cct, 10) << __func__ << " " << *in << " " << off << "~" << len << dendl;

  while (left > 0) {
    C_SaferCond onfinish("Client::_read_sync flock");
    bufferlist tbl;

    int wanted = left;
    filer->read_trunc(in->ino, &in->layout, in->snapid,
		      pos, left, &tbl, 0,
		      in->truncate_size, in->truncate_seq,
		      &onfinish);
    client_lock.unlock();
    int r = onfinish.wait();
    client_lock.lock();

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
	auto z = buffer::ptr_node::create(some);
	z->zero();
	bl->push_back(std::move(z));
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
void Client::_sync_write_commit(Inode *in)
{
  ceph_assert(unsafe_sync_write > 0);
  unsafe_sync_write--;

  put_cap_ref(in, CEPH_CAP_FILE_BUFFER);

  ldout(cct, 15) << __func__ << " unsafe_sync_write = " << unsafe_sync_write << dendl;
  if (unsafe_sync_write == 0 && unmounting) {
    ldout(cct, 10) << __func__ << " -- no more unsafe writes, unmount can proceed" << dendl;
    mount_cond.notify_all();
  }
}

int Client::write(int fd, const char *buf, loff_t size, loff_t offset) 
{
  std::lock_guard lock(client_lock);
  tout(cct) << "write" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << size << std::endl;
  tout(cct) << offset << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *fh = get_filehandle(fd);
  if (!fh)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (fh->flags & O_PATH)
    return -EBADF;
#endif
  /* We can't return bytes written larger than INT_MAX, clamp size to that */
  size = std::min(size, (loff_t)INT_MAX);
  int r = _write(fh, offset, size, buf, NULL, false);
  ldout(cct, 3) << "write(" << fd << ", \"...\", " << size << ", " << offset << ") = " << r << dendl;
  return r;
}

int Client::pwritev(int fd, const struct iovec *iov, int iovcnt, int64_t offset)
{
  if (iovcnt < 0)
    return -EINVAL;
  return _preadv_pwritev(fd, iov, iovcnt, offset, true);
}

int64_t Client::_preadv_pwritev_locked(Fh *fh, const struct iovec *iov,
				   unsigned iovcnt, int64_t offset, bool write,
				   bool clamp_to_int)
{
#if defined(__linux__) && defined(O_PATH)
    if (fh->flags & O_PATH)
        return -EBADF;
#endif
    loff_t totallen = 0;
    for (unsigned i = 0; i < iovcnt; i++) {
        totallen += iov[i].iov_len;
    }

    /*
     * Some of the API functions take 64-bit size values, but only return
     * 32-bit signed integers. Clamp the I/O sizes in those functions so that
     * we don't do I/Os larger than the values we can return.
     */
    if (clamp_to_int) {
      totallen = std::min(totallen, (loff_t)INT_MAX);
    }
    if (write) {
        int64_t w = _write(fh, offset, totallen, NULL, iov, iovcnt);
        ldout(cct, 3) << "pwritev(" << fh << ", \"...\", " << totallen << ", " << offset << ") = " << w << dendl;
        return w;
    } else {
        bufferlist bl;
        int64_t r = _read(fh, offset, totallen, &bl);
        ldout(cct, 3) << "preadv(" << fh << ", " <<  offset << ") = " << r << dendl;
        if (r <= 0)
          return r;

        auto iter = bl.cbegin();
        for (unsigned j = 0, resid = r; j < iovcnt && resid > 0; j++) {
               /*
                * This piece of code aims to handle the case that bufferlist does not have enough data 
                * to fill in the iov 
                */
               const auto round_size = std::min<unsigned>(resid, iov[j].iov_len);
               iter.copy(round_size, reinterpret_cast<char*>(iov[j].iov_base));
               resid -= round_size;
               /* iter is self-updating */
        }
        return r;  
    }
}

int Client::_preadv_pwritev(int fd, const struct iovec *iov, unsigned iovcnt, int64_t offset, bool write)
{
    std::lock_guard lock(client_lock);
    tout(cct) << fd << std::endl;
    tout(cct) << offset << std::endl;

    if (unmounting)
     return -ENOTCONN;

    Fh *fh = get_filehandle(fd);
    if (!fh)
        return -EBADF;
    return _preadv_pwritev_locked(fh, iov, iovcnt, offset, write, true);
}

int64_t Client::_write(Fh *f, int64_t offset, uint64_t size, const char *buf,
	                const struct iovec *iov, int iovcnt)
{
  uint64_t fpos = 0;

  if ((uint64_t)(offset+size) > mdsmap->get_max_filesize()) //too large!
    return -EFBIG;

  //ldout(cct, 7) << "write fh " << fh << " size " << size << " offset " << offset << dendl;
  Inode *in = f->inode.get();

  if (objecter->osdmap_pool_full(in->layout.pool_id)) {
    return -ENOSPC;
  }

  ceph_assert(in->snapid == CEPH_NOSNAP);

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
    if (f->flags & O_APPEND) {
      auto r = _lseek(f, 0, SEEK_END);
      if (r < 0) {
        unlock_fh_pos(f);
        return r;
      }
    }
    offset = f->pos;
    fpos = offset+size;
    unlock_fh_pos(f);
  }

  // check quota
  uint64_t endoff = offset + size;
  if (endoff > in->size && is_quota_bytes_exceeded(in, endoff - in->size,
						   f->actor_perms)) {
    return -EDQUOT;
  }

  //bool lazy = f->mode == CEPH_FILE_MODE_LAZY;

  ldout(cct, 10) << "cur file size is " << in->size << dendl;

  // time it.
  utime_t start = ceph_clock_now();

  if (in->inline_version == 0) {
    int r = _getattr(in, CEPH_STAT_CAP_INLINE_DATA, f->actor_perms, true);
    if (r < 0)
      return r;
    ceph_assert(in->inline_version > 0);
  }

  // copy into fresh buffer (since our write may be resub, async)
  bufferlist bl;
  if (buf) {
    if (size > 0)
      bl.append(buf, size);
  } else if (iov){
    for (int i = 0; i < iovcnt; i++) {
      if (iov[i].iov_len > 0) {
        bl.append((const char *)iov[i].iov_base, iov[i].iov_len);
      }
    }
  }

  utime_t lat;
  uint64_t totalwritten;
  int want, have;
  if (f->mode & CEPH_FILE_MODE_LAZY)
    want = CEPH_CAP_FILE_BUFFER | CEPH_CAP_FILE_LAZYIO;
  else
    want = CEPH_CAP_FILE_BUFFER;
  int r = get_caps(f, CEPH_CAP_FILE_WR|CEPH_CAP_AUTH_SHARED, want, &have, endoff);
  if (r < 0)
    return r;

  /* clear the setuid/setgid bits, if any */
  if (unlikely(in->mode & (S_ISUID|S_ISGID)) && size > 0) {
    struct ceph_statx stx = { 0 };

    put_cap_ref(in, CEPH_CAP_AUTH_SHARED);
    r = __setattrx(in, &stx, CEPH_SETATTR_KILL_SGUID, f->actor_perms);
    if (r < 0)
      return r;
  } else {
    put_cap_ref(in, CEPH_CAP_AUTH_SHARED);
  }

  if (f->flags & O_DIRECT)
    have &= ~(CEPH_CAP_FILE_BUFFER | CEPH_CAP_FILE_LAZYIO);

  ldout(cct, 10) << " snaprealm " << *in->snaprealm << dendl;

  std::unique_ptr<C_SaferCond> onuninline = nullptr;
  
  if (in->inline_version < CEPH_INLINE_NONE) {
    if (endoff > cct->_conf->client_max_inline_size ||
        endoff > CEPH_INLINE_MAX_SIZE ||
        !(have & CEPH_CAP_FILE_BUFFER)) {
      onuninline.reset(new C_SaferCond("Client::_write_uninline_data flock"));
      uninline_data(in, onuninline.get());
    } else {
      get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

      uint32_t len = in->inline_data.length();

      if (endoff < len)
        in->inline_data.begin(endoff).copy(len - endoff, bl); // XXX

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

  if (cct->_conf->client_oc &&
      (have & (CEPH_CAP_FILE_BUFFER | CEPH_CAP_FILE_LAZYIO))) {
    // do buffered write
    if (!in->oset.dirty_or_tx)
      get_cap_ref(in, CEPH_CAP_FILE_CACHE | CEPH_CAP_FILE_BUFFER);

    get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

    // async, caching, non-blocking.
    r = objectcacher->file_write(&in->oset, &in->layout,
				 in->snaprealm->get_snap_context(),
				 offset, size, bl, ceph::real_clock::now(),
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
    C_SaferCond onfinish("Client::_write flock");
    unsafe_sync_write++;
    get_cap_ref(in, CEPH_CAP_FILE_BUFFER);  // released by onsafe callback

    filer->write_trunc(in->ino, &in->layout, in->snaprealm->get_snap_context(),
		       offset, size, bl, ceph::real_clock::now(), 0,
		       in->truncate_size, in->truncate_seq,
		       &onfinish);
    client_lock.unlock();
    r = onfinish.wait();
    client_lock.lock();
    _sync_write_commit(in);
    if (r < 0)
      goto done;
  }

  // if we get here, write was successful, update client metadata
success:
  // time
  lat = ceph_clock_now();
  lat -= start;
  logger->tinc(l_c_wrlat, lat);

  if (fpos) {
    lock_fh_pos(f);
    f->pos = fpos;
    unlock_fh_pos(f);
  }
  totalwritten = size;
  r = (int64_t)totalwritten;

  // extend file?
  if (totalwritten + offset > in->size) {
    in->size = totalwritten + offset;
    in->mark_caps_dirty(CEPH_CAP_FILE_WR);

    if (is_quota_bytes_approaching(in, f->actor_perms)) {
      check_caps(in, CHECK_CAPS_NODELAY);
    } else if (is_max_size_approaching(in)) {
      check_caps(in, 0);
    }

    ldout(cct, 7) << "wrote to " << totalwritten+offset << ", extending file size" << dendl;
  } else {
    ldout(cct, 7) << "wrote to " << totalwritten+offset << ", leaving file size at " << in->size << dendl;
  }

  // mtime
  in->mtime = in->ctime = ceph_clock_now();
  in->change_attr++;
  in->mark_caps_dirty(CEPH_CAP_FILE_WR);

done:

  if (nullptr != onuninline) {
    client_lock.unlock();
    int uninline_ret = onuninline->wait();
    client_lock.lock();

    if (uninline_ret >= 0 || uninline_ret == -ECANCELED) {
      in->inline_data.clear();
      in->inline_version = CEPH_INLINE_NONE;
      in->mark_caps_dirty(CEPH_CAP_FILE_WR);
      check_caps(in, 0);
    } else
      r = uninline_ret;
  }

  put_cap_ref(in, CEPH_CAP_FILE_WR);
  return r;
}

int Client::_flush(Fh *f)
{
  Inode *in = f->inode.get();
  int err = f->take_async_err();
  if (err != 0) {
    ldout(cct, 1) << __func__ << ": " << f << " on inode " << *in << " caught async_err = "
                  << cpp_strerror(err) << dendl;
  } else {
    ldout(cct, 10) << __func__ << ": " << f << " on inode " << *in << " no async_err state" << dendl;
  }

  return err;
}

int Client::truncate(const char *relpath, loff_t length, const UserPerm& perms) 
{
  struct ceph_statx stx;
  stx.stx_size = length;
  return setattrx(relpath, &stx, CEPH_SETATTR_SIZE, perms);
}

int Client::ftruncate(int fd, loff_t length, const UserPerm& perms) 
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << length << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  struct stat attr;
  attr.st_size = length;
  return _setattr(f->inode, &attr, CEPH_SETATTR_SIZE, perms);
}

int Client::fsync(int fd, bool syncdataonly) 
{
  std::lock_guard lock(client_lock);
  tout(cct) << "fsync" << std::endl;
  tout(cct) << fd << std::endl;
  tout(cct) << syncdataonly << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
#if defined(__linux__) && defined(O_PATH)
  if (f->flags & O_PATH)
    return -EBADF;
#endif
  int r = _fsync(f, syncdataonly);
  if (r == 0) {
    // The IOs in this fsync were okay, but maybe something happened
    // in the background that we shoudl be reporting?
    r = f->take_async_err();
    ldout(cct, 5) << "fsync(" << fd << ", " << syncdataonly
                  << ") = 0, async_err = " << r << dendl;
  } else {
    // Assume that an error we encountered during fsync, even reported
    // synchronously, would also have applied the error to the Fh, and we
    // should clear it here to avoid returning the same error again on next
    // call.
    ldout(cct, 5) << "fsync(" << fd << ", " << syncdataonly << ") = "
                  << r << dendl;
    f->take_async_err();
  }
  return r;
}

int Client::_fsync(Inode *in, bool syncdataonly)
{
  int r = 0;
  std::unique_ptr<C_SaferCond> object_cacher_completion = nullptr;
  ceph_tid_t flush_tid = 0;
  InodeRef tmp_ref;
  utime_t lat;
  utime_t start = ceph_clock_now(); 

  ldout(cct, 8) << "_fsync on " << *in << " " << (syncdataonly ? "(dataonly)":"(data+metadata)") << dendl;
  
  if (cct->_conf->client_oc) {
    object_cacher_completion.reset(new C_SaferCond("Client::_fsync::lock"));
    tmp_ref = in; // take a reference; C_SaferCond doesn't and _flush won't either
    _flush(in, object_cacher_completion.get());
    ldout(cct, 15) << "using return-valued form of _fsync" << dendl;
  }
  
  if (!syncdataonly && in->dirty_caps) {
    check_caps(in, CHECK_CAPS_NODELAY|CHECK_CAPS_SYNCHRONOUS);
    if (in->flushing_caps)
      flush_tid = last_flush_tid;
  } else ldout(cct, 10) << "no metadata needs to commit" << dendl;

  if (!syncdataonly && !in->unsafe_ops.empty()) {
    flush_mdlog_sync();

    MetaRequest *req = in->unsafe_ops.back();
    ldout(cct, 15) << "waiting on unsafe requests, last tid " << req->get_tid() <<  dendl;

    req->get();
    wait_on_list(req->waitfor_safe);
    put_request(req);
  }

  if (nullptr != object_cacher_completion) { // wait on a real reply instead of guessing
    client_lock.unlock();
    ldout(cct, 15) << "waiting on data to flush" << dendl;
    r = object_cacher_completion->wait();
    client_lock.lock();
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
    ldout(cct, 8) << "ino " << in->ino << " failed to commit to disk! "
		  << cpp_strerror(-r) << dendl;
  }
   
  lat = ceph_clock_now();
  lat -= start;
  logger->tinc(l_c_fsync, lat);

  return r;
}

int Client::_fsync(Fh *f, bool syncdataonly)
{
  ldout(cct, 8) << "_fsync(" << f << ", " << (syncdataonly ? "dataonly)":"data+metadata)") << dendl;
  return _fsync(f->inode.get(), syncdataonly);
}

int Client::fstat(int fd, struct stat *stbuf, const UserPerm& perms, int mask)
{
  std::lock_guard lock(client_lock);
  tout(cct) << "fstat mask " << hex << mask << dec << std::endl;
  tout(cct) << fd << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  int r = _getattr(f->inode, mask, perms);
  if (r < 0)
    return r;
  fill_stat(f->inode, stbuf, NULL);
  ldout(cct, 5) << "fstat(" << fd << ", " << stbuf << ") = " << r << dendl;
  return r;
}

int Client::fstatx(int fd, struct ceph_statx *stx, const UserPerm& perms,
		   unsigned int want, unsigned int flags)
{
  std::lock_guard lock(client_lock);
  tout(cct) << "fstatx flags " << hex << flags << " want " << want << dec << std::endl;
  tout(cct) << fd << std::endl;

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;

  unsigned mask = statx_to_mask(flags, want);

  int r = 0;
  if (mask && !f->inode->caps_issued_mask(mask, true)) {
    r = _getattr(f->inode, mask, perms);
    if (r < 0) {
      ldout(cct, 3) << "fstatx exit on error!" << dendl;
      return r;
    }
  }

  fill_statx(f->inode, mask, stx);
  ldout(cct, 3) << "fstatx(" << fd << ", " << stx << ") = " << r << dendl;
  return r;
}

// not written yet, but i want to link!

int Client::chdir(const char *relpath, std::string &new_cwd,
		  const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  tout(cct) << "chdir" << std::endl;
  tout(cct) << relpath << std::endl;

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms);
  if (r < 0)
    return r;

  if (!(in.get()->is_dir()))
    return -ENOTDIR;

  if (cwd != in)
    cwd.swap(in);
  ldout(cct, 3) << "chdir(" << relpath << ")  cwd now " << cwd->ino << dendl;

  _getcwd(new_cwd, perms);
  return 0;
}

void Client::_getcwd(string& dir, const UserPerm& perms)
{
  filepath path;
  ldout(cct, 10) << __func__ << " " << *cwd << dendl;

  Inode *in = cwd.get();
  while (in != root) {
    ceph_assert(in->dentries.size() < 2); // dirs can't be hard-linked

    // A cwd or ancester is unlinked
    if (in->dentries.empty()) {
      return;
    }

    Dentry *dn = in->get_first_parent();


    if (!dn) {
      // look it up
      ldout(cct, 10) << __func__ << " looking up parent for " << *in << dendl;
      MetaRequest *req = new MetaRequest(CEPH_MDS_OP_LOOKUPNAME);
      filepath path(in->ino);
      req->set_filepath(path);
      req->set_inode(in);
      int res = make_request(req, perms);
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

void Client::getcwd(string& dir, const UserPerm& perms)
{
  std::lock_guard l(client_lock);
  if (!unmounting)
    _getcwd(dir, perms);
}

int Client::statfs(const char *path, struct statvfs *stbuf,
		   const UserPerm& perms)
{
  std::lock_guard l(client_lock);
  tout(cct) << __func__ << std::endl;
  unsigned long int total_files_on_fs;

  if (unmounting)
    return -ENOTCONN;

  ceph_statfs stats;
  C_SaferCond cond;

  const vector<int64_t> &data_pools = mdsmap->get_data_pools();
  if (data_pools.size() == 1) {
    objecter->get_fs_stats(stats, data_pools[0], &cond);
  } else {
    objecter->get_fs_stats(stats, boost::optional<int64_t>(), &cond);
  }

  client_lock.unlock();
  int rval = cond.wait();
  assert(root);
  total_files_on_fs = root->rstat.rfiles + root->rstat.rsubdirs;
  client_lock.lock();

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
  stbuf->f_files = total_files_on_fs;
  stbuf->f_ffree = 0;
  stbuf->f_favail = -1;
  stbuf->f_fsid = -1;       // ??
  stbuf->f_flag = 0;        // ??
  stbuf->f_namemax = NAME_MAX;

  // Usually quota_root will == root_ancestor, but if the mount root has no
  // quota but we can see a parent of it that does have a quota, we'll
  // respect that one instead.
  ceph_assert(root != nullptr);
  Inode *quota_root = root->quota.is_enable() ? root : get_quota_root(root, perms);

  // get_quota_root should always give us something
  // because client quotas are always enabled
  ceph_assert(quota_root != nullptr);

  if (quota_root && cct->_conf->client_quota_df && quota_root->quota.max_bytes) {

    // Skip the getattr if any sessions are stale, as we don't want to
    // block `df` if this client has e.g. been evicted, or if the MDS cluster
    // is unhealthy.
    if (!_any_stale_sessions()) {
      int r = _getattr(quota_root, 0, perms, true);
      if (r != 0) {
        // Ignore return value: error getting latest inode metadata is not a good
        // reason to break "df".
        lderr(cct) << "Error in getattr on quota root 0x"
                   << std::hex << quota_root->ino << std::dec
                   << " statfs result may be outdated" << dendl;
      }
    }

    // Special case: if there is a size quota set on the Inode acting
    // as the root for this client mount, then report the quota status
    // as the filesystem statistics.
    const fsblkcnt_t total = quota_root->quota.max_bytes >> CEPH_BLOCK_SHIFT;
    const fsblkcnt_t used = quota_root->rstat.rbytes >> CEPH_BLOCK_SHIFT;
    // It is possible for a quota to be exceeded: arithmetic here must
    // handle case where used > total.
    const fsblkcnt_t free = total > used ? total - used : 0;

    stbuf->f_blocks = total;
    stbuf->f_bfree = free;
    stbuf->f_bavail = free;
  } else {
    // General case: report the cluster statistics returned from RADOS. Because
    // multiple pools may be used without one filesystem namespace via
    // layouts, this is the most correct thing we can do.
    stbuf->f_blocks = stats.kb >> (CEPH_BLOCK_SHIFT - 10);
    stbuf->f_bfree = stats.kb_avail >> (CEPH_BLOCK_SHIFT - 10);
    stbuf->f_bavail = stats.kb_avail >> (CEPH_BLOCK_SHIFT - 10);
  }

  return rval;
}

int Client::_do_filelock(Inode *in, Fh *fh, int lock_type, int op, int sleep,
			 struct flock *fl, uint64_t owner, bool removing)
{
  ldout(cct, 10) << __func__ << " ino " << in->ino
		 << (lock_type == CEPH_LOCK_FCNTL ? " fcntl" : " flock")
		 << " type " << fl->l_type << " owner " << owner
		 << " " << fl->l_start << "~" << fl->l_len << dendl;

  if (in->flags & I_ERROR_FILELOCK)
    return -EIO;

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
    ret = make_request(req, fh->actor_perms, NULL, NULL, -1, &bl);
    // disable interrupt
    switch_interrupt_cb(callback_handle, NULL);
    if (ret == 0 && req->aborted()) {
      // effect of this lock request has been revoked by the 'lock intr' request
      ret = req->get_abort_code();
    }
    put_request(req);
  } else {
    ret = make_request(req, fh->actor_perms, NULL, NULL, -1, &bl);
  }

  if (ret == 0) {
    if (op == CEPH_MDS_OP_GETFILELOCK) {
      ceph_filelock filelock;
      auto p = bl.cbegin();
      decode(filelock, p);

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
	  in->fcntl_locks.reset(new ceph_lock_state_t(cct, CEPH_LOCK_FCNTL));
	lock_state = in->fcntl_locks.get();
      } else if (lock_type == CEPH_LOCK_FLOCK) {
	if (!in->flock_locks)
	  in->flock_locks.reset(new ceph_lock_state_t(cct, CEPH_LOCK_FLOCK));
	lock_state = in->flock_locks.get();
      } else {
	ceph_abort();
	return -EINVAL;
      }
      _update_lock_state(fl, owner, lock_state);

      if (!removing) {
	if (lock_type == CEPH_LOCK_FCNTL) {
	  if (!fh->fcntl_locks)
	    fh->fcntl_locks.reset(new ceph_lock_state_t(cct, CEPH_LOCK_FCNTL));
	  lock_state = fh->fcntl_locks.get();
	} else {
	  if (!fh->flock_locks)
	    fh->flock_locks.reset(new ceph_lock_state_t(cct, CEPH_LOCK_FLOCK));
	  lock_state = fh->flock_locks.get();
	}
	_update_lock_state(fl, owner, lock_state);
      }
    } else
      ceph_abort();
  }
  return ret;
}

int Client::_interrupt_filelock(MetaRequest *req)
{
  // Set abort code, but do not kick. The abort code prevents the request
  // from being re-sent.
  req->abort(-EINTR);
  if (req->mds < 0)
    return 0; // haven't sent the request

  Inode *in = req->inode();

  int lock_type;
  if (req->head.args.filelock_change.rule == CEPH_LOCK_FLOCK)
    lock_type = CEPH_LOCK_FLOCK_INTR;
  else if (req->head.args.filelock_change.rule == CEPH_LOCK_FCNTL)
    lock_type = CEPH_LOCK_FCNTL_INTR;
  else {
    ceph_abort();
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

  UserPerm perms(req->get_uid(), req->get_gid());
  return make_request(intr_req, perms, NULL, NULL, -1);
}

void Client::_encode_filelocks(Inode *in, bufferlist& bl)
{
  if (!in->fcntl_locks && !in->flock_locks)
    return;

  unsigned nr_fcntl_locks = in->fcntl_locks ? in->fcntl_locks->held_locks.size() : 0;
  encode(nr_fcntl_locks, bl);
  if (nr_fcntl_locks) {
    auto &lock_state = in->fcntl_locks;
    for(multimap<uint64_t, ceph_filelock>::iterator p = lock_state->held_locks.begin();
	p != lock_state->held_locks.end();
	++p)
      encode(p->second, bl);
  }

  unsigned nr_flock_locks = in->flock_locks ? in->flock_locks->held_locks.size() : 0;
  encode(nr_flock_locks, bl);
  if (nr_flock_locks) {
    auto &lock_state = in->flock_locks;
    for(multimap<uint64_t, ceph_filelock>::iterator p = lock_state->held_locks.begin();
	p != lock_state->held_locks.end();
	++p)
      encode(p->second, bl);
  }

  ldout(cct, 10) << __func__ << " ino " << in->ino << ", " << nr_fcntl_locks
		 << " fcntl locks, " << nr_flock_locks << " flock locks" <<  dendl;
}

void Client::_release_filelocks(Fh *fh)
{
  if (!fh->fcntl_locks && !fh->flock_locks)
    return;

  Inode *in = fh->inode.get();
  ldout(cct, 10) << __func__ << " " << fh << " ino " << in->ino << dendl;

  list<ceph_filelock> activated_locks;

  list<pair<int, ceph_filelock> > to_release;

  if (fh->fcntl_locks) {
    auto &lock_state = fh->fcntl_locks;
    for(auto p = lock_state->held_locks.begin(); p != lock_state->held_locks.end(); ) {
      auto q = p++;
      if (in->flags & I_ERROR_FILELOCK) {
	lock_state->remove_lock(q->second, activated_locks);
      } else {
	to_release.push_back(pair<int, ceph_filelock>(CEPH_LOCK_FCNTL, q->second));
      }
    }
    lock_state.reset();
  }
  if (fh->flock_locks) {
    auto &lock_state = fh->flock_locks;
    for(auto p = lock_state->held_locks.begin(); p != lock_state->held_locks.end(); ) {
      auto q = p++;
      if (in->flags & I_ERROR_FILELOCK) {
	lock_state->remove_lock(q->second, activated_locks);
      } else {
	to_release.push_back(pair<int, ceph_filelock>(CEPH_LOCK_FLOCK, q->second));
      }
    }
    lock_state.reset();
  }

  if ((in->flags & I_ERROR_FILELOCK) && !in->has_any_filelocks())
    in->flags &= ~I_ERROR_FILELOCK;

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
    _do_filelock(in, fh, p->first, CEPH_MDS_OP_SETFILELOCK, 0, &fl,
		 p->second.owner, true);
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
    ceph_assert(r);
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

int Client::ll_statfs(Inode *in, struct statvfs *stbuf, const UserPerm& perms)
{
  /* Since the only thing this does is wrap a call to statfs, and
     statfs takes a lock, it doesn't seem we have a need to split it
     out. */
  return statfs(0, stbuf, perms);
}

void Client::ll_register_callbacks(struct ceph_client_callback_args *args)
{
  if (!args)
    return;
  std::lock_guard l(client_lock);
  ldout(cct, 10) << __func__ << " cb " << args->handle
		 << " invalidate_ino_cb " << args->ino_cb
		 << " invalidate_dentry_cb " << args->dentry_cb
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
  if (args->ino_release_cb) {
    ino_release_cb = args->ino_release_cb;
    async_ino_releasor.start();
  }
  if (args->umask_cb)
    umask_cb = args->umask_cb;
}

int Client::test_dentry_handling(bool can_invalidate)
{
  int r = 0;

  can_invalidate_dentries = can_invalidate;

  if (can_invalidate_dentries) {
    ceph_assert(dentry_invalidate_cb);
    ldout(cct, 1) << "using dentry_invalidate_cb" << dendl;
    r = 0;
  } else {
    ceph_assert(remount_cb);
    ldout(cct, 1) << "using remount_cb" << dendl;
    r = _do_remount(false);
  }

  return r;
}

int Client::_sync_fs()
{
  ldout(cct, 10) << __func__ << dendl;

  // flush file data
  std::unique_ptr<C_SaferCond> cond = nullptr; 
  if (cct->_conf->client_oc) {
    cond.reset(new C_SaferCond("Client::_sync_fs:lock"));
    objectcacher->flush_all(cond.get());
  }

  // flush caps
  flush_caps_sync();
  ceph_tid_t flush_tid = last_flush_tid;

  // wait for unsafe mds requests
  wait_unsafe_requests();

  wait_sync_caps(flush_tid);

  if (nullptr != cond) {
    client_lock.unlock();
    ldout(cct, 15) << __func__ << " waiting on data to flush" << dendl;
    cond->wait();
    ldout(cct, 15) << __func__ << " flush finished" << dendl;
    client_lock.lock();
  }

  return 0;
}

int Client::sync_fs()
{
  std::lock_guard l(client_lock);

  if (unmounting)
    return -ENOTCONN;

  return _sync_fs();
}

int64_t Client::drop_caches()
{
  std::lock_guard l(client_lock);
  return objectcacher->release_all();
}

int Client::_lazyio(Fh *fh, int enable)
{
  Inode *in = fh->inode.get();
  ldout(cct, 20) << __func__ << " " << *in << " " << !!enable << dendl;

  if (!!(fh->mode & CEPH_FILE_MODE_LAZY) == !!enable)
    return 0;

  int orig_mode = fh->mode;
  if (enable) {
    fh->mode |= CEPH_FILE_MODE_LAZY;
    in->get_open_ref(fh->mode);
    in->put_open_ref(orig_mode);
    check_caps(in, CHECK_CAPS_NODELAY);
  } else {
    fh->mode &= ~CEPH_FILE_MODE_LAZY;
    in->get_open_ref(fh->mode);
    in->put_open_ref(orig_mode);
    check_caps(in, 0);
  }

  return 0;
}

int Client::lazyio(int fd, int enable)
{
  std::lock_guard l(client_lock);
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;

  return _lazyio(f, enable);
}

int Client::ll_lazyio(Fh *fh, int enable)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << __func__ << " " << fh << " " << fh->inode->ino << " " << !!enable << dendl;
  tout(cct) << __func__ << std::endl;

  return _lazyio(fh, enable);
}

int Client::lazyio_propagate(int fd, loff_t offset, size_t count)
{
  std::lock_guard l(client_lock);
  ldout(cct, 3) << "op: client->lazyio_propagate(" << fd
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
  std::lock_guard l(client_lock);
  ldout(cct, 3) << "op: client->lazyio_synchronize(" << fd
          << ", " << offset << ", " << count << ")" << dendl;
  
  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();
  
  _fsync(f, true);
  if (_release(in)) {
    int r =_getattr(in, CEPH_STAT_CAP_SIZE, f->actor_perms);
    if (r < 0) 
      return r;
  }
  return 0;
}


// =============================
// snaps

int Client::mksnap(const char *relpath, const char *name, const UserPerm& perm)
{
  std::lock_guard l(client_lock);

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perm);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    r = may_create(in.get(), perm);
    if (r < 0)
      return r;
  }
  Inode *snapdir = open_snapdir(in.get());
  return _mkdir(snapdir, name, 0, perm);
}

int Client::rmsnap(const char *relpath, const char *name, const UserPerm& perms)
{
  std::lock_guard l(client_lock);

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms);
  if (r < 0)
    return r;
  if (cct->_conf->client_permissions) {
    r = may_delete(in.get(), NULL, perms);
    if (r < 0)
      return r;
  }
  Inode *snapdir = open_snapdir(in.get());
  return _rmdir(snapdir, name, perms);
}

// =============================
// expose caps

int Client::get_caps_issued(int fd) {

  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;

  return f->inode->caps_issued();
}

int Client::get_caps_issued(const char *path, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  filepath p(path);
  InodeRef in;
  int r = path_walk(p, &in, perms, true);
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
    in->nlink = 1;
    in->mtime = diri->mtime;
    in->ctime = diri->ctime;
    in->btime = diri->btime;
    in->atime = diri->atime;
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
		      Inode **out, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  vinodeno_t vparent = _get_vino(parent);
  ldout(cct, 3) << __func__ << " " << vparent << " " << name << dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << name << std::endl;

  if (unmounting)
    return -ENOTCONN;

  int r = 0;
  if (!fuse_default_permissions) {
    if (strcmp(name, ".") && strcmp(name, "..")) {
      r = may_lookup(parent, perms);
      if (r < 0)
	return r;
    }
  }

  string dname(name);
  InodeRef in;

  r = _lookup(parent, dname, CEPH_STAT_CAP_INODE_ALL, &in, perms);
  if (r < 0) {
    attr->st_ino = 0;
    goto out;
  }

  ceph_assert(in);
  fill_stat(in, attr);
  _ll_get(in.get());

 out:
  ldout(cct, 3) << __func__ << " " << vparent << " " << name
	  << " -> " << r << " (" << hex << attr->st_ino << dec << ")" << dendl;
  tout(cct) << attr->st_ino << std::endl;
  *out = in.get();
  return r;
}

int Client::ll_lookup_inode(
    struct inodeno_t ino,
    const UserPerm& perms,
    Inode **inode)
{
  ceph_assert(inode != NULL);
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << "ll_lookup_inode " << ino  << dendl;
   
  if (unmounting)
    return -ENOTCONN;

  // Num1: get inode and *inode
  int r = _lookup_ino(ino, perms, inode);
  if (r)
    return r;

  ceph_assert(*inode != NULL);

  if (!(*inode)->dentries.empty()) {
    ldout(cct, 8) << __func__ << " dentry already present" << dendl;
    return 0;
  }

  if ((*inode)->is_root()) {
    ldout(cct, 8) << "ino is root, no parent" << dendl;
    return 0;
  }

  // Num2: Request the parent inode, so that we can look up the name
  Inode *parent;
  r = _lookup_parent(*inode, perms, &parent);
  if (r) {
    _ll_forget(*inode, 1);  
    return r;
  }

  ceph_assert(parent != NULL);

  // Num3: Finally, get the name (dentry) of the requested inode
  r = _lookup_name(*inode, parent, perms);
  if (r) {
    // Unexpected error
    _ll_forget(parent, 1);
    _ll_forget(*inode, 1);
    return r;
  }

  _ll_forget(parent, 1);
  return 0;
}

int Client::ll_lookupx(Inode *parent, const char *name, Inode **out,
		       struct ceph_statx *stx, unsigned want, unsigned flags,
		       const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  vinodeno_t vparent = _get_vino(parent);
  ldout(cct, 3) << __func__ << " " << vparent << " " << name << dendl;
  tout(cct) << "ll_lookupx" << std::endl;
  tout(cct) << name << std::endl;

  if (unmounting)
    return -ENOTCONN;

  int r = 0;
  if (!fuse_default_permissions) {
    r = may_lookup(parent, perms);
    if (r < 0)
      return r;
  }

  string dname(name);
  InodeRef in;

  unsigned mask = statx_to_mask(flags, want);
  r = _lookup(parent, dname, mask, &in, perms);
  if (r < 0) {
    stx->stx_ino = 0;
    stx->stx_mask = 0;
  } else {
    ceph_assert(in);
    fill_statx(in, mask, stx);
    _ll_get(in.get());
  }

  ldout(cct, 3) << __func__ << " " << vparent << " " << name
	  << " -> " << r << " (" << hex << stx->stx_ino << dec << ")" << dendl;
  tout(cct) << stx->stx_ino << std::endl;
  *out = in.get();
  return r;
}

int Client::ll_walk(const char* name, Inode **out, struct ceph_statx *stx,
		    unsigned int want, unsigned int flags, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  filepath fp(name, 0);
  InodeRef in;
  int rc;
  unsigned mask = statx_to_mask(flags, want);

  ldout(cct, 3) << __func__ << " " << name << dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << name << std::endl;

  rc = path_walk(fp, &in, perms, !(flags & AT_SYMLINK_NOFOLLOW), mask);
  if (rc < 0) {
    /* zero out mask, just in case... */
    stx->stx_mask = 0;
    stx->stx_ino = 0;
    *out = NULL;
    return rc;
  } else {
    ceph_assert(in);
    fill_statx(in, mask, stx);
    _ll_get(in.get());
    *out = in.get();
    return 0;
  }
}

void Client::_ll_get(Inode *in)
{
  if (in->ll_ref == 0) {
    in->get();
    if (in->is_dir() && !in->dentries.empty()) {
      ceph_assert(in->dentries.size() == 1); // dirs can't be hard-linked
      in->get_first_parent()->get(); // pin dentry
    }
    if (in->snapid != CEPH_NOSNAP)
      ll_snap_ref[in->snapid]++;
  }
  in->ll_get();
  ldout(cct, 20) << __func__ << " " << in << " " << in->ino << " -> " << in->ll_ref << dendl;
}

int Client::_ll_put(Inode *in, uint64_t num)
{
  in->ll_put(num);
  ldout(cct, 20) << __func__ << " " << in << " " << in->ino << " " << num << " -> " << in->ll_ref << dendl;
  if (in->ll_ref == 0) {
    if (in->is_dir() && !in->dentries.empty()) {
      ceph_assert(in->dentries.size() == 1); // dirs can't be hard-linked
      in->get_first_parent()->put(); // unpin dentry
    }
    if (in->snapid != CEPH_NOSNAP) {
      auto p = ll_snap_ref.find(in->snapid);
      ceph_assert(p != ll_snap_ref.end());
      ceph_assert(p->second > 0);
      if (--p->second == 0)
	ll_snap_ref.erase(p);
    }
    put_inode(in);
    return 0;
  } else {
    return in->ll_ref;
  }
}

void Client::_ll_drop_pins()
{
  ldout(cct, 10) << __func__ << dendl;
  std::set<InodeRef> to_be_put; //this set will be deconstructed item by item when exit
  ceph::unordered_map<vinodeno_t, Inode*>::iterator next;
  for (ceph::unordered_map<vinodeno_t, Inode*>::iterator it = inode_map.begin();
       it != inode_map.end();
       it = next) {
    Inode *in = it->second;
    next = it;
    ++next;
    if (in->ll_ref){
      to_be_put.insert(in);
      _ll_put(in, in->ll_ref);
    }
  }
}

bool Client::_ll_forget(Inode *in, uint64_t count)
{
  inodeno_t ino = in->ino;

  ldout(cct, 8) << __func__ << " " << ino << " " << count << dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << ino.val << std::endl;
  tout(cct) << count << std::endl;

  // Ignore forget if we're no longer mounted
  if (unmounting)
    return true;

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

bool Client::ll_forget(Inode *in, uint64_t count)
{
  std::lock_guard lock(client_lock);
  return _ll_forget(in, count);
}

bool Client::ll_put(Inode *in)
{
  /* ll_forget already takes the lock */
  return ll_forget(in, 1);
}

int Client::ll_get_snap_ref(snapid_t snap)
{
  std::lock_guard lock(client_lock);
  auto p = ll_snap_ref.find(snap);
  if (p != ll_snap_ref.end())
    return p->second;
  return 0;
}

snapid_t Client::ll_get_snapid(Inode *in)
{
  std::lock_guard lock(client_lock);
  return in->snapid;
}

Inode *Client::ll_get_inode(ino_t ino)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return NULL;

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
  std::lock_guard lock(client_lock);

  if (unmounting)
    return NULL;

  unordered_map<vinodeno_t,Inode*>::iterator p = inode_map.find(vino);
  if (p == inode_map.end())
    return NULL;
  Inode *in = p->second;
  _ll_get(in);
  return in;
}

int Client::_ll_getattr(Inode *in, int caps, const UserPerm& perms)
{
  vinodeno_t vino = _get_vino(in);

  ldout(cct, 8) << __func__ << " " << vino << dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << vino.ino.val << std::endl;

  if (vino.snapid < CEPH_NOSNAP)
    return 0;
  else
    return _getattr(in, caps, perms);
}

int Client::ll_getattr(Inode *in, struct stat *attr, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  int res = _ll_getattr(in, CEPH_STAT_CAP_INODE_ALL, perms);

  if (res == 0)
    fill_stat(in, attr);
  ldout(cct, 3) << __func__ << " " << _get_vino(in) << " = " << res << dendl;
  return res;
}

int Client::ll_getattrx(Inode *in, struct ceph_statx *stx, unsigned int want,
			unsigned int flags, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  int res = 0;
  unsigned mask = statx_to_mask(flags, want);

  if (mask && !in->caps_issued_mask(mask, true))
    res = _ll_getattr(in, mask, perms);

  if (res == 0)
    fill_statx(in, mask, stx);
  ldout(cct, 3) << __func__ << " " << _get_vino(in) << " = " << res << dendl;
  return res;
}

int Client::_ll_setattrx(Inode *in, struct ceph_statx *stx, int mask,
			 const UserPerm& perms, InodeRef *inp)
{
  vinodeno_t vino = _get_vino(in);

  ldout(cct, 8) << __func__ << " " << vino << " mask " << hex << mask << dec
		<< dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << stx->stx_mode << std::endl;
  tout(cct) << stx->stx_uid << std::endl;
  tout(cct) << stx->stx_gid << std::endl;
  tout(cct) << stx->stx_size << std::endl;
  tout(cct) << stx->stx_mtime << std::endl;
  tout(cct) << stx->stx_atime << std::endl;
  tout(cct) << stx->stx_btime << std::endl;
  tout(cct) << mask << std::endl;

  if (!fuse_default_permissions) {
    int res = may_setattr(in, stx, mask, perms);
    if (res < 0)
      return res;
  }

  mask &= ~(CEPH_SETATTR_MTIME_NOW | CEPH_SETATTR_ATIME_NOW);

  return __setattrx(in, stx, mask, perms, inp);
}

int Client::ll_setattrx(Inode *in, struct ceph_statx *stx, int mask,
			const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef target(in);
  int res = _ll_setattrx(in, stx, mask, perms, &target);
  if (res == 0) {
    ceph_assert(in == target.get());
    fill_statx(in, in->caps_issued(), stx);
  }

  ldout(cct, 3) << __func__ << " " << _get_vino(in) << " = " << res << dendl;
  return res;
}

int Client::ll_setattr(Inode *in, struct stat *attr, int mask,
		       const UserPerm& perms)
{
  struct ceph_statx stx;
  stat_to_statx(attr, &stx);

  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef target(in);
  int res = _ll_setattrx(in, &stx, mask, perms, &target);
  if (res == 0) {
    ceph_assert(in == target.get());
    fill_stat(in, attr);
  }

  ldout(cct, 3) << __func__ << " " << _get_vino(in) << " = " << res << dendl;
  return res;
}


// ----------
// xattrs

int Client::getxattr(const char *path, const char *name, void *value, size_t size,
		     const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef in;
  int r = Client::path_walk(path, &in, perms, true, CEPH_STAT_CAP_XATTR);
  if (r < 0)
    return r;
  return _getxattr(in, name, value, size, perms);
}

int Client::lgetxattr(const char *path, const char *name, void *value, size_t size,
		      const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef in;
  int r = Client::path_walk(path, &in, perms, false, CEPH_STAT_CAP_XATTR);
  if (r < 0)
    return r;
  return _getxattr(in, name, value, size, perms);
}

int Client::fgetxattr(int fd, const char *name, void *value, size_t size,
		      const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return _getxattr(f->inode, name, value, size, perms);
}

int Client::listxattr(const char *path, char *list, size_t size,
		      const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef in;
  int r = Client::path_walk(path, &in, perms, true, CEPH_STAT_CAP_XATTR);
  if (r < 0)
    return r;
  return Client::_listxattr(in.get(), list, size, perms);
}

int Client::llistxattr(const char *path, char *list, size_t size,
		       const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef in;
  int r = Client::path_walk(path, &in, perms, false, CEPH_STAT_CAP_XATTR);
  if (r < 0)
    return r;
  return Client::_listxattr(in.get(), list, size, perms);
}

int Client::flistxattr(int fd, char *list, size_t size, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return Client::_listxattr(f->inode.get(), list, size, perms);
}

int Client::removexattr(const char *path, const char *name,
			const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef in;
  int r = Client::path_walk(path, &in, perms, true);
  if (r < 0)
    return r;
  return _removexattr(in, name, perms);
}

int Client::lremovexattr(const char *path, const char *name,
			 const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef in;
  int r = Client::path_walk(path, &in, perms, false);
  if (r < 0)
    return r;
  return _removexattr(in, name, perms);
}

int Client::fremovexattr(int fd, const char *name, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return _removexattr(f->inode, name, perms);
}

int Client::setxattr(const char *path, const char *name, const void *value,
		     size_t size, int flags, const UserPerm& perms)
{
  _setxattr_maybe_wait_for_osdmap(name, value, size);

  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef in;
  int r = Client::path_walk(path, &in, perms, true);
  if (r < 0)
    return r;
  return _setxattr(in, name, value, size, flags, perms);
}

int Client::lsetxattr(const char *path, const char *name, const void *value,
		      size_t size, int flags, const UserPerm& perms)
{
  _setxattr_maybe_wait_for_osdmap(name, value, size);

  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  InodeRef in;
  int r = Client::path_walk(path, &in, perms, false);
  if (r < 0)
    return r;
  return _setxattr(in, name, value, size, flags, perms);
}

int Client::fsetxattr(int fd, const char *name, const void *value, size_t size,
		      int flags, const UserPerm& perms)
{
  _setxattr_maybe_wait_for_osdmap(name, value, size);

  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  return _setxattr(f->inode, name, value, size, flags, perms);
}

int Client::_getxattr(Inode *in, const char *name, void *value, size_t size,
		      const UserPerm& perms)
{
  int r;

  const VXattr *vxattr = _match_vxattr(in, name);
  if (vxattr) {
    r = -ENODATA;

    // Do a force getattr to get the latest quota before returning
    // a value to userspace.
    int flags = 0;
    if (vxattr->flags & VXATTR_RSTAT) {
      flags |= CEPH_STAT_RSTAT;
    }
    r = _getattr(in, flags, perms, true);
    if (r != 0) {
      // Error from getattr!
      return r;
    }

    // call pointer-to-member function
    char buf[256];
    if (!(vxattr->exists_cb && !(this->*(vxattr->exists_cb))(in))) {
      r = (this->*(vxattr->getxattr_cb))(in, buf, sizeof(buf));
    } else {
      r = -ENODATA;
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

  if (acl_type == NO_ACL && !strncmp(name, "system.", 7)) {
    r = -EOPNOTSUPP;
    goto out;
  }

  r = _getattr(in, CEPH_STAT_CAP_XATTR, perms, in->xattr_version == 0);
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
  ldout(cct, 8) << "_getxattr(" << in->ino << ", \"" << name << "\", " << size << ") = " << r << dendl;
  return r;
}

int Client::_getxattr(InodeRef &in, const char *name, void *value, size_t size,
		      const UserPerm& perms)
{
  if (cct->_conf->client_permissions) {
    int r = xattr_permission(in.get(), name, MAY_READ, perms);
    if (r < 0)
      return r;
  }
  return _getxattr(in.get(), name, value, size, perms);
}

int Client::ll_getxattr(Inode *in, const char *name, void *value,
			size_t size, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << __func__ << " " << vino << " " << name << " size " << size << dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!fuse_default_permissions) {
    int r = xattr_permission(in, name, MAY_READ, perms);
    if (r < 0)
      return r;
  }

  return _getxattr(in, name, value, size, perms);
}

int Client::_listxattr(Inode *in, char *name, size_t size,
		       const UserPerm& perms)
{
  bool len_only = (size == 0);
  int r = _getattr(in, CEPH_STAT_CAP_XATTR, perms, in->xattr_version == 0);
  if (r != 0) {
    goto out;
  }

  r = 0;
  for (const auto& p : in->xattrs) {
    size_t this_len = p.first.length() + 1;
    r += this_len;
    if (len_only)
      continue;

    if (this_len > size) {
      r = -ERANGE;
      goto out;
    }

    memcpy(name, p.first.c_str(), this_len);
    name += this_len;
    size -= this_len;
  }
out:
  ldout(cct, 8) << __func__ << "(" << in->ino << ", " << size << ") = " << r << dendl;
  return r;
}

int Client::ll_listxattr(Inode *in, char *names, size_t size,
			 const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << __func__ << " " << vino << " size " << size << dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << size << std::endl;

  return _listxattr(in, names, size, perms);
}

int Client::_do_setxattr(Inode *in, const char *name, const void *value,
			 size_t size, int flags, const UserPerm& perms)
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
  assert (value || size == 0);
  bl.append((const char*)value, size);
  req->set_data(bl);

  int res = make_request(req, perms);

  trim_cache();
  ldout(cct, 3) << __func__ << "(" << in->ino << ", \"" << name << "\") = " <<
    res << dendl;
  return res;
}

int Client::_setxattr(Inode *in, const char *name, const void *value,
		      size_t size, int flags, const UserPerm& perms)
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

  bool check_realm = false;

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
	  struct ceph_statx stx;
	  stx.stx_mode = new_mode;
	  ret = _do_setattr(in, &stx, CEPH_SETATTR_MODE, perms, NULL);
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
    if (vxattr) {
      if (vxattr->readonly)
	return -EOPNOTSUPP;
      if (vxattr->name.compare(0, 10, "ceph.quota") == 0 && value)
	check_realm = true;
    }
  }

  int ret = _do_setxattr(in, name, value, size, flags, perms);
  if (ret >= 0 && check_realm) {
    // check if snaprealm was created for quota inode
    if (in->quota.is_enable() &&
	!(in->snaprealm && in->snaprealm->ino == in->ino))
      ret = -EOPNOTSUPP;
  }

  return ret;
}

int Client::_setxattr(InodeRef &in, const char *name, const void *value,
		      size_t size, int flags, const UserPerm& perms)
{
  if (cct->_conf->client_permissions) {
    int r = xattr_permission(in.get(), name, MAY_WRITE, perms);
    if (r < 0)
      return r;
  }
  return _setxattr(in.get(), name, value, size, flags, perms);
}

int Client::_setxattr_check_data_pool(string& name, string& value, const OSDMap *osdmap)
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

void Client::_setxattr_maybe_wait_for_osdmap(const char *name, const void *value, size_t size)
{
  // For setting pool of layout, MetaRequest need osdmap epoch.
  // There is a race which create a new data pool but client and mds both don't have.
  // Make client got the latest osdmap which make mds quickly judge whether get newer osdmap.
  if (strcmp(name, "ceph.file.layout.pool") == 0 || strcmp(name, "ceph.dir.layout.pool") == 0 ||
      strcmp(name, "ceph.file.layout") == 0 || strcmp(name, "ceph.dir.layout") == 0) {
    string rest(strstr(name, "layout"));
    string v((const char*)value, size);
    int r = objecter->with_osdmap([&](const OSDMap& o) {
      return _setxattr_check_data_pool(rest, v, &o);
    });

    if (r == -ENOENT) {
      bs::error_code ec;
      objecter->wait_for_latest_osdmap(ca::use_blocked[ec]);
    }
  }
}

int Client::ll_setxattr(Inode *in, const char *name, const void *value,
			size_t size, int flags, const UserPerm& perms)
{
  _setxattr_maybe_wait_for_osdmap(name, value, size);

  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << __func__ << " " << vino << " " << name << " size " << size << dendl;
  tout(cct) << __func__ << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!fuse_default_permissions) {
    int r = xattr_permission(in, name, MAY_WRITE, perms);
    if (r < 0)
      return r;
  }
  return _setxattr(in, name, value, size, flags, perms);
}

int Client::_removexattr(Inode *in, const char *name, const UserPerm& perms)
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
 
  int res = make_request(req, perms);

  trim_cache();
  ldout(cct, 8) << "_removexattr(" << in->ino << ", \"" << name << "\") = " << res << dendl;
  return res;
}

int Client::_removexattr(InodeRef &in, const char *name, const UserPerm& perms)
{
  if (cct->_conf->client_permissions) {
    int r = xattr_permission(in.get(), name, MAY_WRITE, perms);
    if (r < 0)
      return r;
  }
  return _removexattr(in.get(), name, perms);
}

int Client::ll_removexattr(Inode *in, const char *name, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_removexattr " << vino << " " << name << dendl;
  tout(cct) << "ll_removexattr" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!fuse_default_permissions) {
    int r = xattr_permission(in, name, MAY_WRITE, perms);
    if (r < 0)
      return r;
  }

  return _removexattr(in, name, perms);
}

bool Client::_vxattrcb_quota_exists(Inode *in)
{
  return in->quota.is_enable() &&
	 in->snaprealm && in->snaprealm->ino == in->ino;
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
      "stripe_unit=%llu stripe_count=%llu object_size=%llu pool=",
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
  return snprintf(val, size, "%llu", (unsigned long long)in->layout.stripe_unit);
}
size_t Client::_vxattrcb_layout_stripe_count(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu", (unsigned long long)in->layout.stripe_count);
}
size_t Client::_vxattrcb_layout_object_size(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu", (unsigned long long)in->layout.object_size);
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
  return snprintf(val, size, "%llu", (unsigned long long)(in->dirstat.nfiles + in->dirstat.nsubdirs));
}
size_t Client::_vxattrcb_dir_files(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu", (unsigned long long)in->dirstat.nfiles);
}
size_t Client::_vxattrcb_dir_subdirs(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu", (unsigned long long)in->dirstat.nsubdirs);
}
size_t Client::_vxattrcb_dir_rentries(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu", (unsigned long long)(in->rstat.rfiles + in->rstat.rsubdirs));
}
size_t Client::_vxattrcb_dir_rfiles(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu", (unsigned long long)in->rstat.rfiles);
}
size_t Client::_vxattrcb_dir_rsubdirs(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu", (unsigned long long)in->rstat.rsubdirs);
}
size_t Client::_vxattrcb_dir_rbytes(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu", (unsigned long long)in->rstat.rbytes);
}
size_t Client::_vxattrcb_dir_rctime(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%ld.%09ld", (long)in->rstat.rctime.sec(),
      (long)in->rstat.rctime.nsec());
}
bool Client::_vxattrcb_dir_pin_exists(Inode *in)
{
  return in->dir_pin != -ENODATA;
}
size_t Client::_vxattrcb_dir_pin(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%ld", (long)in->dir_pin);
}

bool Client::_vxattrcb_snap_btime_exists(Inode *in)
{
  return !in->snap_btime.is_zero();
}

size_t Client::_vxattrcb_snap_btime(Inode *in, char *val, size_t size)
{
  return snprintf(val, size, "%llu.%09lu",
      (long long unsigned)in->snap_btime.sec(),
      (long unsigned)in->snap_btime.nsec());
}

#define CEPH_XATTR_NAME(_type, _name) "ceph." #_type "." #_name
#define CEPH_XATTR_NAME2(_type, _name, _name2) "ceph." #_type "." #_name "." #_name2

#define XATTR_NAME_CEPH(_type, _name)				\
{								\
  name: CEPH_XATTR_NAME(_type, _name),				\
  getxattr_cb: &Client::_vxattrcb_ ## _type ## _ ## _name,	\
  readonly: true,						\
  exists_cb: NULL,						\
  flags: 0,                                                     \
}
#define XATTR_NAME_CEPH2(_type, _name, _flags)                 \
{                                                              \
  name: CEPH_XATTR_NAME(_type, _name),                         \
  getxattr_cb: &Client::_vxattrcb_ ## _type ## _ ## _name,     \
  readonly: true,                                              \
  exists_cb: NULL,                                             \
  flags: _flags,                                               \
}
#define XATTR_LAYOUT_FIELD(_type, _name, _field)		\
{								\
  name: CEPH_XATTR_NAME2(_type, _name, _field),			\
  getxattr_cb: &Client::_vxattrcb_ ## _name ## _ ## _field,	\
  readonly: false,						\
  exists_cb: &Client::_vxattrcb_layout_exists,			\
  flags: 0,                                                     \
}
#define XATTR_QUOTA_FIELD(_type, _name)		                \
{								\
  name: CEPH_XATTR_NAME(_type, _name),			        \
  getxattr_cb: &Client::_vxattrcb_ ## _type ## _ ## _name,	\
  readonly: false,						\
  exists_cb: &Client::_vxattrcb_quota_exists,			\
  flags: 0,                                                     \
}

const Client::VXattr Client::_dir_vxattrs[] = {
  {
    name: "ceph.dir.layout",
    getxattr_cb: &Client::_vxattrcb_layout,
    readonly: false,
    exists_cb: &Client::_vxattrcb_layout_exists,
    flags: 0,
  },
  XATTR_LAYOUT_FIELD(dir, layout, stripe_unit),
  XATTR_LAYOUT_FIELD(dir, layout, stripe_count),
  XATTR_LAYOUT_FIELD(dir, layout, object_size),
  XATTR_LAYOUT_FIELD(dir, layout, pool),
  XATTR_LAYOUT_FIELD(dir, layout, pool_namespace),
  XATTR_NAME_CEPH(dir, entries),
  XATTR_NAME_CEPH(dir, files),
  XATTR_NAME_CEPH(dir, subdirs),
  XATTR_NAME_CEPH2(dir, rentries, VXATTR_RSTAT),
  XATTR_NAME_CEPH2(dir, rfiles, VXATTR_RSTAT),
  XATTR_NAME_CEPH2(dir, rsubdirs, VXATTR_RSTAT),
  XATTR_NAME_CEPH2(dir, rbytes, VXATTR_RSTAT),
  XATTR_NAME_CEPH2(dir, rctime, VXATTR_RSTAT),
  {
    name: "ceph.quota",
    getxattr_cb: &Client::_vxattrcb_quota,
    readonly: false,
    exists_cb: &Client::_vxattrcb_quota_exists,
    flags: 0,
  },
  XATTR_QUOTA_FIELD(quota, max_bytes),
  XATTR_QUOTA_FIELD(quota, max_files),
  {
    name: "ceph.dir.pin",
    getxattr_cb: &Client::_vxattrcb_dir_pin,
    readonly: false,
    exists_cb: &Client::_vxattrcb_dir_pin_exists,
    flags: 0,
  },
  {
    name: "ceph.snap.btime",
    getxattr_cb: &Client::_vxattrcb_snap_btime,
    readonly: true,
    exists_cb: &Client::_vxattrcb_snap_btime_exists,
    flags: 0,
  },
  { name: "" }     /* Required table terminator */
};

const Client::VXattr Client::_file_vxattrs[] = {
  {
    name: "ceph.file.layout",
    getxattr_cb: &Client::_vxattrcb_layout,
    readonly: false,
    exists_cb: &Client::_vxattrcb_layout_exists,
    flags: 0,
  },
  XATTR_LAYOUT_FIELD(file, layout, stripe_unit),
  XATTR_LAYOUT_FIELD(file, layout, stripe_count),
  XATTR_LAYOUT_FIELD(file, layout, object_size),
  XATTR_LAYOUT_FIELD(file, layout, pool),
  XATTR_LAYOUT_FIELD(file, layout, pool_namespace),
  {
    name: "ceph.snap.btime",
    getxattr_cb: &Client::_vxattrcb_snap_btime,
    readonly: true,
    exists_cb: &Client::_vxattrcb_snap_btime_exists,
    flags: 0,
  },
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

int Client::ll_readlink(Inode *in, char *buf, size_t buflen, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_readlink " << vino << dendl;
  tout(cct) << "ll_readlink" << std::endl;
  tout(cct) << vino.ino.val << std::endl;

  for (auto dn : in->dentries) {
    touch_dn(dn);
  }

  int r = _readlink(in, buf, buflen); // FIXME: no permission checking!
  ldout(cct, 3) << "ll_readlink " << vino << " = " << r << dendl;
  return r;
}

int Client::_mknod(Inode *dir, const char *name, mode_t mode, dev_t rdev,
		   const UserPerm& perms, InodeRef *inp)
{
  ldout(cct, 8) << "_mknod(" << dir->ino << " " << name << ", 0" << oct
		<< mode << dec << ", " << rdev << ", uid " << perms.uid()
		<< ", gid " << perms.gid() << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir, perms)) {
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
  int res = _posix_acl_create(dir, &mode, xattrs_bl, perms);
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

  res = make_request(req, perms, inp);

  trim_cache();

  ldout(cct, 8) << "mknod(" << path << ", 0" << oct << mode << dec << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_mknod(Inode *parent, const char *name, mode_t mode,
		     dev_t rdev, struct stat *attr, Inode **out,
		     const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_mknod " << vparent << " " << name << dendl;
  tout(cct) << "ll_mknod" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << rdev << std::endl;

  if (!fuse_default_permissions) {
    int r = may_create(parent, perms);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _mknod(parent, name, mode, rdev, perms, &in);
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

int Client::ll_mknodx(Inode *parent, const char *name, mode_t mode,
		      dev_t rdev, Inode **out,
		      struct ceph_statx *stx, unsigned want, unsigned flags,
		      const UserPerm& perms)
{
  unsigned caps = statx_to_mask(flags, want);
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_mknodx " << vparent << " " << name << dendl;
  tout(cct) << "ll_mknodx" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << rdev << std::endl;

  if (!fuse_default_permissions) {
    int r = may_create(parent, perms);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _mknod(parent, name, mode, rdev, perms, &in);
  if (r == 0) {
    fill_statx(in, caps, stx);
    _ll_get(in.get());
  }
  tout(cct) << stx->stx_ino << std::endl;
  ldout(cct, 3) << "ll_mknodx " << vparent << " " << name
	  << " = " << r << " (" << hex << stx->stx_ino << dec << ")" << dendl;
  *out = in.get();
  return r;
}

int Client::_create(Inode *dir, const char *name, int flags, mode_t mode,
		    InodeRef *inp, Fh **fhp, int stripe_unit, int stripe_count,
		    int object_size, const char *data_pool, bool *created,
		    const UserPerm& perms)
{
  ldout(cct, 8) << "_create(" << dir->ino << " " << name << ", 0" << oct <<
    mode << dec << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;
  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir, perms)) {
    return -EDQUOT;
  }

  // use normalized flags to generate cmode
  int cflags = ceph_flags_sys2wire(flags);
  if (cct->_conf.get_val<bool>("client_force_lazyio"))
    cflags |= CEPH_O_LAZY;

  int cmode = ceph_flags_to_mode(cflags);

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
  req->head.args.open.flags = cflags | CEPH_O_CREAT;

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
  int res = _posix_acl_create(dir, &mode, xattrs_bl, perms);
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

  res = make_request(req, perms, inp, created);
  if (res < 0) {
    goto reply_error;
  }

  /* If the caller passed a value in fhp, do the open */
  if(fhp) {
    (*inp)->get_open_ref(cmode);
    *fhp = _create_fh(inp->get(), flags, cmode, perms);
  }

 reply_error:
  trim_cache();

  ldout(cct, 8) << "create(" << path << ", 0" << oct << mode << dec
		<< " layout " << stripe_unit
		<< ' ' << stripe_count
		<< ' ' << object_size
		<<") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}


int Client::_mkdir(Inode *dir, const char *name, mode_t mode, const UserPerm& perm,
		   InodeRef *inp)
{
  ldout(cct, 8) << "_mkdir(" << dir->ino << " " << name << ", 0" << oct
		<< mode << dec << ", uid " << perm.uid()
		<< ", gid " << perm.gid() << ")" << dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP && dir->snapid != CEPH_SNAPDIR) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir, perm)) {
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
  int res = _posix_acl_create(dir, &mode, xattrs_bl, perm);
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
  res = make_request(req, perm, inp);
  ldout(cct, 10) << "_mkdir result is " << res << dendl;

  trim_cache();

  ldout(cct, 8) << "_mkdir(" << path << ", 0" << oct << mode << dec << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_mkdir(Inode *parent, const char *name, mode_t mode,
		     struct stat *attr, Inode **out, const UserPerm& perm)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_mkdir " << vparent << " " << name << dendl;
  tout(cct) << "ll_mkdir" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;

  if (!fuse_default_permissions) {
    int r = may_create(parent, perm);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _mkdir(parent, name, mode, perm, &in);
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

int Client::ll_mkdirx(Inode *parent, const char *name, mode_t mode, Inode **out,
		      struct ceph_statx *stx, unsigned want, unsigned flags,
		      const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_mkdirx " << vparent << " " << name << dendl;
  tout(cct) << "ll_mkdirx" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;

  if (!fuse_default_permissions) {
    int r = may_create(parent, perms);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _mkdir(parent, name, mode, perms, &in);
  if (r == 0) {
    fill_statx(in, statx_to_mask(flags, want), stx);
    _ll_get(in.get());
  } else {
    stx->stx_ino = 0;
    stx->stx_mask = 0;
  }
  tout(cct) << stx->stx_ino << std::endl;
  ldout(cct, 3) << "ll_mkdirx " << vparent << " " << name
	  << " = " << r << " (" << hex << stx->stx_ino << dec << ")" << dendl;
  *out = in.get();
  return r;
}

int Client::_symlink(Inode *dir, const char *name, const char *target,
		     const UserPerm& perms, InodeRef *inp)
{
  ldout(cct, 8) << "_symlink(" << dir->ino << " " << name << ", " << target
		<< ", uid " << perms.uid() << ", gid " << perms.gid() << ")"
		<< dendl;

  if (strlen(name) > NAME_MAX)
    return -ENAMETOOLONG;

  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir, perms)) {
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

  res = make_request(req, perms, inp);

  trim_cache();
  ldout(cct, 8) << "_symlink(\"" << path << "\", \"" << target << "\") = " <<
    res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_symlink(Inode *parent, const char *name, const char *value,
		       struct stat *attr, Inode **out, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_symlink " << vparent << " " << name << " -> " << value
		<< dendl;
  tout(cct) << "ll_symlink" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << value << std::endl;

  if (!fuse_default_permissions) {
    int r = may_create(parent, perms);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _symlink(parent, name, value, perms, &in);
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

int Client::ll_symlinkx(Inode *parent, const char *name, const char *value,
			Inode **out, struct ceph_statx *stx, unsigned want,
			unsigned flags, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 3) << "ll_symlinkx " << vparent << " " << name << " -> " << value
		<< dendl;
  tout(cct) << "ll_symlinkx" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << value << std::endl;

  if (!fuse_default_permissions) {
    int r = may_create(parent, perms);
    if (r < 0)
      return r;
  }

  InodeRef in;
  int r = _symlink(parent, name, value, perms, &in);
  if (r == 0) {
    fill_statx(in, statx_to_mask(flags, want), stx);
    _ll_get(in.get());
  }
  tout(cct) << stx->stx_ino << std::endl;
  ldout(cct, 3) << "ll_symlinkx " << vparent << " " << name
	  << " = " << r << " (" << hex << stx->stx_ino << dec << ")" << dendl;
  *out = in.get();
  return r;
}

int Client::_unlink(Inode *dir, const char *name, const UserPerm& perm)
{
  ldout(cct, 8) << "_unlink(" << dir->ino << " " << name
		<< " uid " << perm.uid() << " gid " << perm.gid()
		<< ")" << dendl;

  if (dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }

  MetaRequest *req = new MetaRequest(CEPH_MDS_OP_UNLINK);

  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);

  InodeRef otherin;
  Inode *in;
  Dentry *de;

  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  req->set_dentry(de);
  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;

  res = _lookup(dir, name, 0, &otherin, perm);
  if (res < 0)
    goto fail;

  in = otherin.get();
  req->set_other_inode(in);
  in->break_all_delegs();
  req->other_inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;

  req->set_inode(dir);

  res = make_request(req, perm);

  trim_cache();
  ldout(cct, 8) << "unlink(" << path << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_unlink(Inode *in, const char *name, const UserPerm& perm)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_unlink " << vino << " " << name << dendl;
  tout(cct) << "ll_unlink" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!fuse_default_permissions) {
    int r = may_delete(in, name, perm);
    if (r < 0)
      return r;
  }
  return _unlink(in, name, perm);
}

int Client::_rmdir(Inode *dir, const char *name, const UserPerm& perms)
{
  ldout(cct, 8) << "_rmdir(" << dir->ino << " " << name << " uid "
		<< perms.uid() << " gid " << perms.gid() << ")" << dendl;

  if (dir->snapid != CEPH_NOSNAP && dir->snapid != CEPH_SNAPDIR) {
    return -EROFS;
  }
  
  int op = dir->snapid == CEPH_SNAPDIR ? CEPH_MDS_OP_RMSNAP : CEPH_MDS_OP_RMDIR;
  MetaRequest *req = new MetaRequest(op);
  filepath path;
  dir->make_nosnap_relative_path(path);
  path.push_dentry(name);
  req->set_filepath(path);
  req->set_inode(dir);

  req->dentry_drop = CEPH_CAP_FILE_SHARED;
  req->dentry_unless = CEPH_CAP_FILE_EXCL;
  req->other_inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;

  InodeRef in;

  Dentry *de;
  int res = get_or_create(dir, name, &de);
  if (res < 0)
    goto fail;
  if (op == CEPH_MDS_OP_RMDIR) 
    req->set_dentry(de);
  else
    de->get();

  res = _lookup(dir, name, 0, &in, perms);
  if (res < 0)
    goto fail;

  if (op == CEPH_MDS_OP_RMSNAP) {
    unlink(de, true, true);
    de->put();
  }
  req->set_other_inode(in.get());

  res = make_request(req, perms);

  trim_cache();
  ldout(cct, 8) << "rmdir(" << path << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_rmdir(Inode *in, const char *name, const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_rmdir " << vino << " " << name << dendl;
  tout(cct) << "ll_rmdir" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << name << std::endl;

  if (!fuse_default_permissions) {
    int r = may_delete(in, name, perms);
    if (r < 0)
      return r;
  }

  return _rmdir(in, name, perms);
}

int Client::_rename(Inode *fromdir, const char *fromname, Inode *todir, const char *toname, const UserPerm& perm)
{
  ldout(cct, 8) << "_rename(" << fromdir->ino << " " << fromname << " to "
		<< todir->ino << " " << toname
		<< " uid " << perm.uid() << " gid " << perm.gid() << ")"
		<< dendl;

  if (fromdir->snapid != todir->snapid)
    return -EXDEV;

  int op = CEPH_MDS_OP_RENAME;
  if (fromdir->snapid != CEPH_NOSNAP) {
    if (fromdir == todir && fromdir->snapid == CEPH_SNAPDIR)
      op = CEPH_MDS_OP_RENAMESNAP;
    else
      return -EROFS;
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
    Inode *fromdir_root = nullptr;
    Inode *todir_root = nullptr;
    int mask = 0;
    bool quota_check = false;
    if (fromdir != todir) {
      fromdir_root =
        fromdir->quota.is_enable() ? fromdir : get_quota_root(fromdir, perm);
      todir_root =
        todir->quota.is_enable() ? todir : get_quota_root(todir, perm);

      if (todir_root->quota.is_enable() && fromdir_root != todir_root) {
        // use CEPH_STAT_RSTAT mask to force send getattr or lookup request 
        // to auth MDS to get latest rstat for todir_root and source dir 
        // even if their dentry caches and inode caps are satisfied.
        res = _getattr(todir_root, CEPH_STAT_RSTAT, perm, true);
        if (res < 0)
          goto fail;

        quota_check = true;
        if (oldde->inode && oldde->inode->is_dir()) {
          mask |= CEPH_STAT_RSTAT;
        }
      }
    }

    res = _lookup(fromdir, fromname, mask, &oldin, perm);
    if (res < 0)
      goto fail;

    Inode *oldinode = oldin.get();
    oldinode->break_all_delegs();
    req->set_old_inode(oldinode);
    req->old_inode_drop = CEPH_CAP_LINK_SHARED;

    if (quota_check) {
      int64_t old_bytes, old_files;
      if (oldinode->is_dir()) {
        old_bytes = oldinode->rstat.rbytes;
        old_files = oldinode->rstat.rsize();
      } else {
        old_bytes = oldinode->size;
        old_files = 1;
      }

      bool quota_exceed = false;
      if (todir_root && todir_root->quota.max_bytes &&
          (old_bytes + todir_root->rstat.rbytes) >= todir_root->quota.max_bytes) {
        ldout(cct, 10) << "_rename (" << oldinode->ino << " bytes="
                       << old_bytes << ") to (" << todir->ino 
		       << ") will exceed quota on " << *todir_root << dendl;
        quota_exceed = true;
      }

      if (todir_root && todir_root->quota.max_files &&
          (old_files + todir_root->rstat.rsize()) >= todir_root->quota.max_files) {
        ldout(cct, 10) << "_rename (" << oldinode->ino << " files="
                       << old_files << ") to (" << todir->ino 
                       << ") will exceed quota on " << *todir_root << dendl;
        quota_exceed = true;
      }

      if (quota_exceed) {
        res = (oldinode->is_dir()) ? -EXDEV : -EDQUOT;
        goto fail;
      }
    }

    res = _lookup(todir, toname, 0, &otherin, perm);
    switch (res) {
    case 0:
      {
	Inode *in = otherin.get();
	req->set_other_inode(in);
	in->break_all_delegs();
      }
      req->other_inode_drop = CEPH_CAP_LINK_SHARED | CEPH_CAP_LINK_EXCL;
      break;
    case -ENOENT:
      break;
    default:
      goto fail;
    }

    req->set_inode(todir);
  } else {
    // renamesnap reply contains no tracedn, so we need to invalidate
    // dentry manually
    unlink(oldde, true, true);
    unlink(de, true, true);

    req->set_inode(todir);
  }

  res = make_request(req, perm, &target);
  ldout(cct, 10) << "rename result is " << res << dendl;

  // renamed item from our cache

  trim_cache();
  ldout(cct, 8) << "_rename(" << from << ", " << to << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_rename(Inode *parent, const char *name, Inode *newparent,
		      const char *newname, const UserPerm& perm)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vparent = _get_vino(parent);
  vinodeno_t vnewparent = _get_vino(newparent);

  ldout(cct, 3) << "ll_rename " << vparent << " " << name << " to "
	  << vnewparent << " " << newname << dendl;
  tout(cct) << "ll_rename" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << vnewparent.ino.val << std::endl;
  tout(cct) << newname << std::endl;

  if (!fuse_default_permissions) {
    int r = may_delete(parent, name, perm);
    if (r < 0)
      return r;
    r = may_delete(newparent, newname, perm);
    if (r < 0 && r != -ENOENT)
      return r;
  }

  return _rename(parent, name, newparent, newname, perm);
}

int Client::_link(Inode *in, Inode *dir, const char *newname, const UserPerm& perm, InodeRef *inp)
{
  ldout(cct, 8) << "_link(" << in->ino << " to " << dir->ino << " " << newname
		<< " uid " << perm.uid() << " gid " << perm.gid() << ")" << dendl;

  if (strlen(newname) > NAME_MAX)
    return -ENAMETOOLONG;

  if (in->snapid != CEPH_NOSNAP || dir->snapid != CEPH_NOSNAP) {
    return -EROFS;
  }
  if (is_quota_files_exceeded(dir, perm)) {
    return -EDQUOT;
  }

  in->break_all_delegs();
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
  
  res = make_request(req, perm, inp);
  ldout(cct, 10) << "link result is " << res << dendl;

  trim_cache();
  ldout(cct, 8) << "link(" << existing << ", " << path << ") = " << res << dendl;
  return res;

 fail:
  put_request(req);
  return res;
}

int Client::ll_link(Inode *in, Inode *newparent, const char *newname,
		    const UserPerm& perm)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);
  vinodeno_t vnewparent = _get_vino(newparent);

  ldout(cct, 3) << "ll_link " << vino << " to " << vnewparent << " " <<
    newname << dendl;
  tout(cct) << "ll_link" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << vnewparent << std::endl;
  tout(cct) << newname << std::endl;

  InodeRef target;

  if (!fuse_default_permissions) {
    if (S_ISDIR(in->mode))
      return -EPERM;

    int r = may_hardlink(in, perm);
    if (r < 0)
      return r;

    r = may_create(newparent, perm);
    if (r < 0)
      return r;
  }

  return _link(in, newparent, newname, perm, &target);
}

int Client::ll_num_osds(void)
{
  std::lock_guard lock(client_lock);
  return objecter->with_osdmap(std::mem_fn(&OSDMap::get_num_osds));
}

int Client::ll_osdaddr(int osd, uint32_t *addr)
{
  std::lock_guard lock(client_lock);

  entity_addr_t g;
  bool exists = objecter->with_osdmap([&](const OSDMap& o) {
      if (!o.exists(osd))
	return false;
      g = o.get_addrs(osd).front();
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
  std::lock_guard lock(client_lock);
  return in->layout.stripe_unit;
}

uint64_t Client::ll_snap_seq(Inode *in)
{
  std::lock_guard lock(client_lock);
  return in->snaprealm->seq;
}

int Client::ll_file_layout(Inode *in, file_layout_t *layout)
{
  std::lock_guard lock(client_lock);
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
  std::lock_guard lock(client_lock);

  inodeno_t ino = in->ino;
  uint32_t object_size = layout->object_size;
  uint32_t su = layout->stripe_unit;
  uint32_t stripe_count = layout->stripe_count;
  uint64_t stripes_per_object = object_size / su;
  uint64_t stripeno = 0, stripepos = 0;

  if(stripe_count) {
      stripeno = blockno / stripe_count;    // which horizontal stripe        (Y)
      stripepos = blockno % stripe_count;   // which object in the object set (X)
  }
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
  std::lock_guard lock(client_lock);
  file_layout_t *layout=&(in->layout);
  uint32_t object_size = layout->object_size;
  uint32_t su = layout->stripe_unit;
  uint64_t stripes_per_object = object_size / su;

  return (blockno % stripes_per_object) * su;
}

int Client::ll_opendir(Inode *in, int flags, dir_result_t** dirpp,
		       const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_opendir " << vino << dendl;
  tout(cct) << "ll_opendir" << std::endl;
  tout(cct) << vino.ino.val << std::endl;

  if (!fuse_default_permissions) {
    int r = may_open(in, flags, perms);
    if (r < 0)
      return r;
  }

  int r = _opendir(in, dirpp, perms);
  tout(cct) << (unsigned long)*dirpp << std::endl;

  ldout(cct, 3) << "ll_opendir " << vino << " = " << r << " (" << *dirpp << ")"
		<< dendl;
  return r;
}

int Client::ll_releasedir(dir_result_t *dirp)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << "ll_releasedir " << dirp << dendl;
  tout(cct) << "ll_releasedir" << std::endl;
  tout(cct) << (unsigned long)dirp << std::endl;

  if (unmounting)
    return -ENOTCONN;

  _closedir(dirp);
  return 0;
}

int Client::ll_fsyncdir(dir_result_t *dirp)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << "ll_fsyncdir " << dirp << dendl;
  tout(cct) << "ll_fsyncdir" << std::endl;
  tout(cct) << (unsigned long)dirp << std::endl;

  if (unmounting)
    return -ENOTCONN;

  return _fsync(dirp->inode.get(), false);
}

int Client::ll_open(Inode *in, int flags, Fh **fhp, const UserPerm& perms)
{
  ceph_assert(!(flags & O_CREAT));

  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);

  ldout(cct, 3) << "ll_open " << vino << " " << ceph_flags_sys2wire(flags) << dendl;
  tout(cct) << "ll_open" << std::endl;
  tout(cct) << vino.ino.val << std::endl;
  tout(cct) << ceph_flags_sys2wire(flags) << std::endl;

  int r;
  if (!fuse_default_permissions) {
    r = may_open(in, flags, perms);
    if (r < 0)
      goto out;
  }

  r = _open(in, flags, 0, fhp /* may be NULL */, perms);

 out:
  Fh *fhptr = fhp ? *fhp : NULL;
  if (fhptr) {
    ll_unclosed_fh_set.insert(fhptr);
  }
  tout(cct) << (unsigned long)fhptr << std::endl;
  ldout(cct, 3) << "ll_open " << vino << " " << ceph_flags_sys2wire(flags) <<
      " = " << r << " (" << fhptr << ")" << dendl;
  return r;
}

int Client::_ll_create(Inode *parent, const char *name, mode_t mode,
		      int flags, InodeRef *in, int caps, Fh **fhp,
		      const UserPerm& perms)
{
  *fhp = NULL;

  vinodeno_t vparent = _get_vino(parent);

  ldout(cct, 8) << "_ll_create " << vparent << " " << name << " 0" << oct <<
    mode << dec << " " << ceph_flags_sys2wire(flags) << ", uid " << perms.uid()
		<< ", gid " << perms.gid() << dendl;
  tout(cct) << "ll_create" << std::endl;
  tout(cct) << vparent.ino.val << std::endl;
  tout(cct) << name << std::endl;
  tout(cct) << mode << std::endl;
  tout(cct) << ceph_flags_sys2wire(flags) << std::endl;

  bool created = false;
  int r = _lookup(parent, name, caps, in, perms);

  if (r == 0 && (flags & O_CREAT) && (flags & O_EXCL))
    return -EEXIST;

  if (r == -ENOENT && (flags & O_CREAT)) {
    if (!fuse_default_permissions) {
      r = may_create(parent, perms);
      if (r < 0)
	goto out;
    }
    r = _create(parent, name, flags, mode, in, fhp, 0, 0, 0, NULL, &created,
		perms);
    if (r < 0)
      goto out;
  }

  if (r < 0)
    goto out;

  ceph_assert(*in);

  ldout(cct, 20) << "_ll_create created = " << created << dendl;
  if (!created) {
    if (!fuse_default_permissions) {
      r = may_open(in->get(), flags, perms);
      if (r < 0) {
	if (*fhp) {
	  int release_r = _release_fh(*fhp);
	  ceph_assert(release_r == 0);  // during create, no async data ops should have happened
	}
	goto out;
      }
    }
    if (*fhp == NULL) {
      r = _open(in->get(), flags, mode, fhp, perms);
      if (r < 0)
	goto out;
    }
  }

out:
  if (*fhp) {
    ll_unclosed_fh_set.insert(*fhp);
  }

  ino_t ino = 0;
  if (r >= 0) {
    Inode *inode = in->get();
    if (use_faked_inos())
      ino = inode->faked_ino;
    else
      ino = inode->ino;
  }

  tout(cct) << (unsigned long)*fhp << std::endl;
  tout(cct) << ino << std::endl;
  ldout(cct, 8) << "_ll_create " << vparent << " " << name << " 0" << oct <<
    mode << dec << " " << ceph_flags_sys2wire(flags) << " = " << r << " (" <<
    *fhp << " " << hex << ino << dec << ")" << dendl;

  return r;
}

int Client::ll_create(Inode *parent, const char *name, mode_t mode,
		      int flags, struct stat *attr, Inode **outp, Fh **fhp,
		      const UserPerm& perms)
{
  std::lock_guard lock(client_lock);
  InodeRef in;

  if (unmounting)
    return -ENOTCONN;

  int r = _ll_create(parent, name, mode, flags, &in, CEPH_STAT_CAP_INODE_ALL,
		      fhp, perms);
  if (r >= 0) {
    ceph_assert(in);

    // passing an Inode in outp requires an additional ref
    if (outp) {
      _ll_get(in.get());
      *outp = in.get();
    }
    fill_stat(in, attr);
  } else {
    attr->st_ino = 0;
  }

  return r;
}

int Client::ll_createx(Inode *parent, const char *name, mode_t mode,
			int oflags, Inode **outp, Fh **fhp,
			struct ceph_statx *stx, unsigned want, unsigned lflags,
			const UserPerm& perms)
{
  unsigned caps = statx_to_mask(lflags, want);
  std::lock_guard lock(client_lock);
  InodeRef in;

  if (unmounting)
    return -ENOTCONN;

  int r = _ll_create(parent, name, mode, oflags, &in, caps, fhp, perms);
  if (r >= 0) {
    ceph_assert(in);

    // passing an Inode in outp requires an additional ref
    if (outp) {
      _ll_get(in.get());
      *outp = in.get();
    }
    fill_statx(in, caps, stx);
  } else {
    stx->stx_ino = 0;
    stx->stx_mask = 0;
  }

  return r;
}

loff_t Client::ll_lseek(Fh *fh, loff_t offset, int whence)
{
  std::lock_guard lock(client_lock);
  tout(cct) << "ll_lseek" << std::endl;
  tout(cct) << offset << std::endl;
  tout(cct) << whence << std::endl;

  if (unmounting)
    return -ENOTCONN;

  return _lseek(fh, offset, whence);
}

int Client::ll_read(Fh *fh, loff_t off, loff_t len, bufferlist *bl)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << "ll_read " << fh << " " << fh->inode->ino << " " << " " << off << "~" << len << dendl;
  tout(cct) << "ll_read" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;
  tout(cct) << off << std::endl;
  tout(cct) << len << std::endl;

  if (unmounting)
    return -ENOTCONN;

  /* We can't return bytes written larger than INT_MAX, clamp len to that */
  len = std::min(len, (loff_t)INT_MAX);
  int r = _read(fh, off, len, bl);
  ldout(cct, 3) << "ll_read " << fh << " " << off << "~" << len << " = " << r
		<< dendl;
  return r;
}

int Client::ll_read_block(Inode *in, uint64_t blockid,
			  char *buf,
			  uint64_t offset,
			  uint64_t length,
			  file_layout_t* layout)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  vinodeno_t vino = _get_vino(in);
  object_t oid = file_object_t(vino.ino, blockid);
  C_SaferCond onfinish;
  bufferlist bl;

  objecter->read(oid,
		 object_locator_t(layout->pool_id),
		 offset,
		 length,
		 vino.snapid,
		 &bl,
		 CEPH_OSD_FLAG_READ,
                 &onfinish);

  client_lock.unlock();
  int r = onfinish.wait();
  client_lock.lock();

  if (r >= 0) {
      bl.begin().copy(bl.length(), buf);
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
  vinodeno_t vino = ll_get_vino(in);
  int r = 0;
  std::unique_ptr<C_SaferCond> onsafe = nullptr;
  
  if (length == 0) {
    return -EINVAL;
  }
  if (true || sync) {
    /* if write is stable, the epilogue is waiting on
     * flock */
    onsafe.reset(new C_SaferCond("Client::ll_write_block flock"));
  }
  object_t oid = file_object_t(vino.ino, blockid);
  SnapContext fakesnap;
  ceph::bufferlist bl;
  if (length > 0) {
    bl.push_back(buffer::copy(buf, length));
  }

  ldout(cct, 1) << "ll_block_write for " << vino.ino << "." << blockid
		<< dendl;

  fakesnap.seq = snapseq;

  /* lock just in time */
  client_lock.lock();
  if (unmounting) {
    client_lock.unlock();
    return -ENOTCONN;
  }

  objecter->write(oid,
		  object_locator_t(layout->pool_id),
		  offset,
		  length,
		  fakesnap,
		  bl,
		  ceph::real_clock::now(),
		  0,
		  onsafe.get());

  client_lock.unlock();
  if (nullptr != onsafe) {
    r = onsafe->wait();
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
    std::lock_guard lock(client_lock);
    /*
    BarrierContext *bctx;
    vinodeno_t vino = _get_vino(in);
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
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << "ll_write " << fh << " " << fh->inode->ino << " " << off <<
    "~" << len << dendl;
  tout(cct) << "ll_write" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;
  tout(cct) << off << std::endl;
  tout(cct) << len << std::endl;

  if (unmounting)
    return -ENOTCONN;

  /* We can't return bytes written larger than INT_MAX, clamp len to that */
  len = std::min(len, (loff_t)INT_MAX);
  int r = _write(fh, off, len, data, NULL, 0);
  ldout(cct, 3) << "ll_write " << fh << " " << off << "~" << len << " = " << r
		<< dendl;
  return r;
}

int64_t Client::ll_writev(struct Fh *fh, const struct iovec *iov, int iovcnt, int64_t off)
{
  std::lock_guard lock(client_lock);
  if (unmounting)
   return -ENOTCONN;
  return _preadv_pwritev_locked(fh, iov, iovcnt, off, true, false);
}

int64_t Client::ll_readv(struct Fh *fh, const struct iovec *iov, int iovcnt, int64_t off)
{
  std::lock_guard lock(client_lock);
  if (unmounting)
   return -ENOTCONN;
  return _preadv_pwritev_locked(fh, iov, iovcnt, off, false, false);
}

int Client::ll_flush(Fh *fh)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << "ll_flush " << fh << " " << fh->inode->ino << " " << dendl;
  tout(cct) << "ll_flush" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  if (unmounting)
    return -ENOTCONN;

  return _flush(fh);
}

int Client::ll_fsync(Fh *fh, bool syncdataonly)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << "ll_fsync " << fh << " " << fh->inode->ino << " " << dendl;
  tout(cct) << "ll_fsync" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  if (unmounting)
    return -ENOTCONN;

  int r = _fsync(fh, syncdataonly);
  if (r) {
    // If we're returning an error, clear it from the FH
    fh->take_async_err();
  }
  return r;
}

int Client::ll_sync_inode(Inode *in, bool syncdataonly)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << "ll_sync_inode " << *in << " " << dendl;
  tout(cct) << "ll_sync_inode" << std::endl;
  tout(cct) << (unsigned long)in << std::endl;

  if (unmounting)
    return -ENOTCONN;

  return _fsync(in, syncdataonly);
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
      is_quota_bytes_exceeded(in, size - in->size, fh->actor_perms)) {
    return -EDQUOT;
  }

  int have;
  int r = get_caps(fh, CEPH_CAP_FILE_WR, CEPH_CAP_FILE_BUFFER, &have, -1);
  if (r < 0)
    return r;

  std::unique_ptr<C_SaferCond> onuninline = nullptr;
  if (mode & FALLOC_FL_PUNCH_HOLE) {
    if (in->inline_version < CEPH_INLINE_NONE &&
        (have & CEPH_CAP_FILE_BUFFER)) {
      bufferlist bl;
      auto inline_iter = in->inline_data.cbegin();
      int len = in->inline_data.length();
      if (offset < len) {
        if (offset > 0)
          inline_iter.copy(offset, bl);
        int size = length;
        if (offset + size > len)
          size = len - offset;
        if (size > 0)
          bl.append_zero(size);
        if (offset + size < len) {
          inline_iter += size;
          inline_iter.copy(len - offset - size, bl);
        }
        in->inline_data = bl;
        in->inline_version++;
      }
      in->mtime = in->ctime = ceph_clock_now();
      in->change_attr++;
      in->mark_caps_dirty(CEPH_CAP_FILE_WR);
    } else {
      if (in->inline_version < CEPH_INLINE_NONE) {
        onuninline.reset(new C_SaferCond("Client::_fallocate_uninline_data flock"));
        uninline_data(in, onuninline.get());
      }

      C_SaferCond onfinish("Client::_punch_hole flock");

      unsafe_sync_write++;
      get_cap_ref(in, CEPH_CAP_FILE_BUFFER);

      _invalidate_inode_cache(in, offset, length);
      filer->zero(in->ino, &in->layout,
		  in->snaprealm->get_snap_context(),
		  offset, length,
		  ceph::real_clock::now(),
		  0, true, &onfinish);
      in->mtime = in->ctime = ceph_clock_now();
      in->change_attr++;
      in->mark_caps_dirty(CEPH_CAP_FILE_WR);

      client_lock.unlock();
      onfinish.wait();
      client_lock.lock();
      _sync_write_commit(in);
    }
  } else if (!(mode & FALLOC_FL_KEEP_SIZE)) {
    uint64_t size = offset + length;
    if (size > in->size) {
      in->size = size;
      in->mtime = in->ctime = ceph_clock_now();
      in->change_attr++;
      in->mark_caps_dirty(CEPH_CAP_FILE_WR);

      if (is_quota_bytes_approaching(in, fh->actor_perms)) {
        check_caps(in, CHECK_CAPS_NODELAY);
      } else if (is_max_size_approaching(in)) {
	check_caps(in, 0);
      }
    }
  }

  if (nullptr != onuninline) {
    client_lock.unlock();
    int ret = onuninline->wait();
    client_lock.lock();

    if (ret >= 0 || ret == -ECANCELED) {
      in->inline_data.clear();
      in->inline_version = CEPH_INLINE_NONE;
      in->mark_caps_dirty(CEPH_CAP_FILE_WR);
      check_caps(in, 0);
    } else
      r = ret;
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


int Client::ll_fallocate(Fh *fh, int mode, int64_t offset, int64_t length)
{
  std::lock_guard lock(client_lock);
  ldout(cct, 3) << __func__ << " " << fh << " " << fh->inode->ino << " " << dendl;
  tout(cct) << __func__ << " " << mode << " " << offset << " " << length << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  if (unmounting)
    return -ENOTCONN;

  return _fallocate(fh, mode, offset, length);
}

int Client::fallocate(int fd, int mode, loff_t offset, loff_t length)
{
  std::lock_guard lock(client_lock);
  tout(cct) << __func__ << " " << " " << fd << mode << " " << offset << " " << length << std::endl;

  if (unmounting)
    return -ENOTCONN;

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
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  ldout(cct, 3) << __func__ << " (fh)" << fh << " " << fh->inode->ino << " " <<
    dendl;
  tout(cct) << __func__ << " (fh)" << std::endl;
  tout(cct) << (unsigned long)fh << std::endl;

  if (ll_unclosed_fh_set.count(fh))
    ll_unclosed_fh_set.erase(fh);
  return _release_fh(fh);
}

int Client::ll_getlk(Fh *fh, struct flock *fl, uint64_t owner)
{
  std::lock_guard lock(client_lock);

  ldout(cct, 3) << "ll_getlk (fh)" << fh << " " << fh->inode->ino << dendl;
  tout(cct) << "ll_getk (fh)" << (unsigned long)fh << std::endl;

  if (unmounting)
    return -ENOTCONN;

  return _getlk(fh, fl, owner);
}

int Client::ll_setlk(Fh *fh, struct flock *fl, uint64_t owner, int sleep)
{
  std::lock_guard lock(client_lock);

  ldout(cct, 3) << __func__ << "  (fh) " << fh << " " << fh->inode->ino << dendl;
  tout(cct) << __func__ << " (fh)" << (unsigned long)fh << std::endl;

  if (unmounting)
    return -ENOTCONN;

  return _setlk(fh, fl, owner, sleep);
}

int Client::ll_flock(Fh *fh, int cmd, uint64_t owner)
{
  std::lock_guard lock(client_lock);

  ldout(cct, 3) << __func__ << "  (fh) " << fh << " " << fh->inode->ino << dendl;
  tout(cct) << __func__ << " (fh)" << (unsigned long)fh << std::endl;

  if (unmounting)
    return -ENOTCONN;

  return _flock(fh, cmd, owner);
}

int Client::set_deleg_timeout(uint32_t timeout)
{
  std::lock_guard lock(client_lock);

  /*
   * The whole point is to prevent blacklisting so we must time out the
   * delegation before the session autoclose timeout kicks in.
   */
  if (timeout >= mdsmap->get_session_autoclose())
    return -EINVAL;

  deleg_timeout = timeout;
  return 0;
}

int Client::ll_delegation(Fh *fh, unsigned cmd, ceph_deleg_cb_t cb, void *priv)
{
  int ret = -EINVAL;

  std::lock_guard lock(client_lock);

  if (!mounted)
    return -ENOTCONN;

  Inode *inode = fh->inode.get();

  switch(cmd) {
  case CEPH_DELEGATION_NONE:
    inode->unset_deleg(fh);
    ret = 0;
    break;
  default:
    try {
      ret = inode->set_deleg(fh, cmd, cb, priv);
    } catch (std::bad_alloc&) {
      ret = -ENOMEM;
    }
    break;
  }
  return ret;
}

class C_Client_RequestInterrupt : public Context  {
private:
  Client *client;
  MetaRequest *req;
public:
  C_Client_RequestInterrupt(Client *c, MetaRequest *r) : client(c), req(r) {
    req->get();
  }
  void finish(int r) override {
    std::lock_guard l(client->client_lock);
    ceph_assert(req->head.op == CEPH_MDS_OP_SETFILELOCK);
    client->_interrupt_filelock(req);
    client->put_request(req);
  }
};

void Client::ll_interrupt(void *d)
{
  MetaRequest *req = static_cast<MetaRequest*>(d);
  ldout(cct, 3) << __func__ << " tid " << req->get_tid() << dendl;
  tout(cct) << __func__ << " tid " << req->get_tid() << std::endl;
  interrupt_finisher.queue(new C_Client_RequestInterrupt(this, req));
}

// =========================================
// layout

// expose file layouts

int Client::describe_layout(const char *relpath, file_layout_t *lp,
			    const UserPerm& perms)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  filepath path(relpath);
  InodeRef in;
  int r = path_walk(path, &in, perms);
  if (r < 0)
    return r;

  *lp = in->layout;

  ldout(cct, 3) << __func__ << "(" << relpath << ") = 0" << dendl;
  return 0;
}

int Client::fdescribe_layout(int fd, file_layout_t *lp)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();

  *lp = in->layout;

  ldout(cct, 3) << __func__ << "(" << fd << ") = 0" << dendl;
  return 0;
}

int64_t Client::get_default_pool_id()
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  /* first data pool is the default */ 
  return mdsmap->get_first_data_pool(); 
}

// expose osdmap

int64_t Client::get_pool_id(const char *pool_name)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  return objecter->with_osdmap(std::mem_fn(&OSDMap::lookup_pg_pool_name),
			       pool_name);
}

string Client::get_pool_name(int64_t pool)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return string();

  return objecter->with_osdmap([pool](const OSDMap& o) {
      return o.have_pg_pool(pool) ? o.get_pool_name(pool) : string();
    });
}

int Client::get_pool_replication(int64_t pool)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  return objecter->with_osdmap([pool](const OSDMap& o) {
      return o.have_pg_pool(pool) ? o.get_pg_pool(pool)->get_size() : -ENOENT;
    });
}

int Client::get_file_extent_osds(int fd, loff_t off, loff_t *len, vector<int>& osds)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();

  vector<ObjectExtent> extents;
  Striper::file_to_extents(cct, in->ino, &in->layout, off, 1, in->truncate_size, extents);
  ceph_assert(extents.size() == 1);

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
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  if (id < 0)
    return -EINVAL;
  return objecter->with_osdmap([&](const OSDMap& o) {
      return o.crush->get_full_location_ordered(id, path);
    });
}

int Client::get_file_stripe_address(int fd, loff_t offset,
				    vector<entity_addr_t>& address)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();

  // which object?
  vector<ObjectExtent> extents;
  Striper::file_to_extents(cct, in->ino, &in->layout, offset, 1,
			   in->truncate_size, extents);
  ceph_assert(extents.size() == 1);

  // now we have the object and its 'layout'
  return objecter->with_osdmap([&](const OSDMap& o) {
      pg_t pg = o.object_locator_to_pg(extents[0].oid, extents[0].oloc);
      vector<int> osds;
      o.pg_to_acting_osds(pg, osds);
      if (osds.empty())
	return -EINVAL;
      for (unsigned i = 0; i < osds.size(); i++) {
	entity_addr_t addr = o.get_addrs(osds[i]).front();
	address.push_back(addr);
      }
      return 0;
    });
}

int Client::get_osd_addr(int osd, entity_addr_t& addr)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  return objecter->with_osdmap([&](const OSDMap& o) {
      if (!o.exists(osd))
	return -ENOENT;

      addr = o.get_addrs(osd).front();
      return 0;
    });
}

int Client::enumerate_layout(int fd, vector<ObjectExtent>& result,
			     loff_t length, loff_t offset)
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  Fh *f = get_filehandle(fd);
  if (!f)
    return -EBADF;
  Inode *in = f->inode.get();

  // map to a list of extents
  Striper::file_to_extents(cct, in->ino, &in->layout, offset, length, in->truncate_size, result);

  ldout(cct, 3) << __func__ << "(" << fd << ", " << length << ", " << offset << ") = 0" << dendl;
  return 0;
}


/* find an osd with the same ip.  -ENXIO if none. */
int Client::get_local_osd()
{
  std::lock_guard lock(client_lock);

  if (unmounting)
    return -ENOTCONN;

  objecter->with_osdmap([this](const OSDMap& o) {
      if (o.get_epoch() != local_osd_epoch) {
	local_osd = o.find_osd_on_ip(messenger->get_myaddrs().front());
	local_osd_epoch = o.get_epoch();
      }
    });
  return local_osd;
}






// ===============================

void Client::ms_handle_connect(Connection *con)
{
  ldout(cct, 10) << __func__ << " on " << con->get_peer_addr() << dendl;
}

bool Client::ms_handle_reset(Connection *con)
{
  ldout(cct, 0) << __func__ << " on " << con->get_peer_addr() << dendl;
  return false;
}

void Client::ms_handle_remote_reset(Connection *con)
{
  ldout(cct, 0) << __func__ << " on " << con->get_peer_addr() << dendl;
  std::lock_guard l(client_lock);
  switch (con->get_peer_type()) {
  case CEPH_ENTITY_TYPE_MDS:
    {
      // kludge to figure out which mds this is; fixme with a Connection* state
      mds_rank_t mds = MDS_RANK_NONE;
      MetaSession *s = NULL;
      for (auto &p : mds_sessions) {
	if (mdsmap->get_addrs(p.first) == con->get_peer_addrs()) {
	  mds = p.first;
	  s = &p.second;
	}
      }
      if (mds >= 0) {
	assert (s != NULL);
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
	  {
	    objecter->maybe_request_map(); /* to check if we are blacklisted */
	    if (cct->_conf.get_val<bool>("client_reconnect_stale")) {
	      ldout(cct, 1) << "reset from mds we were open; close mds session for reconnect" << dendl;
	      _closed_mds_session(s);
	    } else {
	      ldout(cct, 1) << "reset from mds we were open; mark session as stale" << dendl;
	      s->state = MetaSession::STATE_STALE;
	    }
	  }
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

bool Client::ms_handle_refused(Connection *con)
{
  ldout(cct, 1) << __func__ << " on " << con->get_peer_addr() << dendl;
  return false;
}

Inode *Client::get_quota_root(Inode *in, const UserPerm& perms)
{
  Inode *quota_in = root_ancestor;
  SnapRealm *realm = in->snaprealm;
  while (realm) {
    ldout(cct, 10) << __func__ << " realm " << realm->ino << dendl;
    if (realm->ino != in->ino) {
      auto p = inode_map.find(vinodeno_t(realm->ino, CEPH_NOSNAP));
      if (p == inode_map.end())
	break;

      if (p->second->quota.is_enable()) {
	quota_in = p->second;
	break;
      }
    }
    realm = realm->pparent;
  }
  ldout(cct, 10) << __func__ << " " << in->vino() << " -> " << quota_in->vino() << dendl;
  return quota_in;
}

/**
 * Traverse quota ancestors of the Inode, return true
 * if any of them passes the passed function
 */
bool Client::check_quota_condition(Inode *in, const UserPerm& perms,
				   std::function<bool (const Inode &in)> test)
{
  while (true) {
    ceph_assert(in != NULL);
    if (test(*in)) {
      return true;
    }

    if (in == root_ancestor) {
      // We're done traversing, drop out
      return false;
    } else {
      // Continue up the tree
      in = get_quota_root(in, perms);
    }
  }

  return false;
}

bool Client::is_quota_files_exceeded(Inode *in, const UserPerm& perms)
{
  return check_quota_condition(in, perms,
      [](const Inode &in) {
        return in.quota.max_files && in.rstat.rsize() >= in.quota.max_files;
      });
}

bool Client::is_quota_bytes_exceeded(Inode *in, int64_t new_bytes,
				     const UserPerm& perms)
{
  return check_quota_condition(in, perms,
      [&new_bytes](const Inode &in) {
        return in.quota.max_bytes && (in.rstat.rbytes + new_bytes)
               > in.quota.max_bytes;
      });
}

bool Client::is_quota_bytes_approaching(Inode *in, const UserPerm& perms)
{
  ceph_assert(in->size >= in->reported_size);
  const uint64_t size = in->size - in->reported_size;
  return check_quota_condition(in, perms,
      [&size](const Inode &in) {
        if (in.quota.max_bytes) {
          if (in.rstat.rbytes >= in.quota.max_bytes) {
            return true;
          }

          const uint64_t space = in.quota.max_bytes - in.rstat.rbytes;
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
      ceph_assert(have & POOL_CHECKED);
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
    rd_op.stat(nullptr, nullptr, nullptr);

    objecter->mutate(oid, OSDMap::file_to_object_locator(in->layout), rd_op,
		     nullsnapc, ceph::real_clock::now(), 0, &rd_cond);

    C_SaferCond wr_cond;
    ObjectOperation wr_op;
    wr_op.create(true);

    objecter->mutate(oid, OSDMap::file_to_object_locator(in->layout), wr_op,
		     nullsnapc, ceph::real_clock::now(), 0, &wr_cond);

    client_lock.unlock();
    int rd_ret = rd_cond.wait();
    int wr_ret = wr_cond.wait();
    client_lock.lock();

    bool errored = false;

    if (rd_ret == 0 || rd_ret == -ENOENT)
      have |= POOL_READ;
    else if (rd_ret != -EPERM) {
      ldout(cct, 10) << __func__ << " on pool " << pool_id << " ns " << pool_ns
		     << " rd_err = " << rd_ret << " wr_err = " << wr_ret << dendl;
      errored = true;
    }

    if (wr_ret == 0 || wr_ret == -EEXIST)
      have |= POOL_WRITE;
    else if (wr_ret != -EPERM) {
      ldout(cct, 10) << __func__ << " on pool " << pool_id << " ns " << pool_ns
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
    ldout(cct, 10) << __func__ << " on pool " << pool_id << " ns " << pool_ns
		   << " need " << ccap_string(need) << ", but no read perm" << dendl;
    return -EPERM;
  }
  if ((need & CEPH_CAP_FILE_WR) && !(have & POOL_WRITE)) {
    ldout(cct, 10) << __func__ << " on pool " << pool_id << " ns " << pool_ns
		   << " need " << ccap_string(need) << ", but no write perm" << dendl;
    return -EPERM;
  }

  return 0;
}

int Client::_posix_acl_permission(Inode *in, const UserPerm& perms, unsigned want)
{
  if (acl_type == POSIX_ACL) {
    if (in->xattrs.count(ACL_EA_ACCESS)) {
      const bufferptr& access_acl = in->xattrs[ACL_EA_ACCESS];

      return posix_acl_permits(access_acl, in->uid, in->gid, perms, want);
    }
  }
  return -EAGAIN;
}

int Client::_posix_acl_chmod(Inode *in, mode_t mode, const UserPerm& perms)
{
  if (acl_type == NO_ACL)
    return 0;

  int r = _getattr(in, CEPH_STAT_CAP_XATTR, perms, in->xattr_version == 0);
  if (r < 0)
    goto out;

  if (acl_type == POSIX_ACL) {
    if (in->xattrs.count(ACL_EA_ACCESS)) {
      const bufferptr& access_acl = in->xattrs[ACL_EA_ACCESS];
      bufferptr acl(access_acl.c_str(), access_acl.length());
      r = posix_acl_access_chmod(acl, mode);
      if (r < 0)
	goto out;
      r = _do_setxattr(in, ACL_EA_ACCESS, acl.c_str(), acl.length(), 0, perms);
    } else {
      r = 0;
    }
  }
out:
  ldout(cct, 10) << __func__ << " ino " << in->ino << " result=" << r << dendl;
  return r;
}

int Client::_posix_acl_create(Inode *dir, mode_t *mode, bufferlist& xattrs_bl,
			      const UserPerm& perms)
{
  if (acl_type == NO_ACL)
    return 0;

  if (S_ISLNK(*mode))
    return 0;

  int r = _getattr(dir, CEPH_STAT_CAP_XATTR, perms, dir->xattr_version == 0);
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
	encode(xattrs, xattrs_bl);
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
  std::lock_guard l(client_lock);
  ceph_assert(flags == 0 ||
	 flags == CEPH_OSD_FLAG_LOCALIZE_READS);
  objecter->add_global_op_flags(flags);
}

void Client::clear_filer_flags(int flags)
{
  std::lock_guard l(client_lock);
  ceph_assert(flags == CEPH_OSD_FLAG_LOCALIZE_READS);
  objecter->clear_global_op_flag(flags);
}

// called before mount
void Client::set_uuid(const std::string& uuid)
{
  std::lock_guard l(client_lock);
  assert(initialized);
  assert(!uuid.empty());

  metadata["uuid"] = uuid;
  _close_sessions();
}

// called before mount. 0 means infinite
void Client::set_session_timeout(unsigned timeout)
{
  std::lock_guard l(client_lock);
  assert(initialized);

  metadata["timeout"] = stringify(timeout);
}

// called before mount
int Client::start_reclaim(const std::string& uuid, unsigned flags,
			  const std::string& fs_name)
{
  std::unique_lock l(client_lock);
  if (!initialized)
    return -ENOTCONN;

  if (uuid.empty())
    return -EINVAL;

  {
    auto it = metadata.find("uuid");
    if (it != metadata.end() && it->second == uuid)
      return -EINVAL;
  }

  int r = subscribe_mdsmap(fs_name);
  if (r < 0) {
    lderr(cct) << "mdsmap subscription failed: " << cpp_strerror(r) << dendl;
    return r;
  }

  if (metadata.empty())
    populate_metadata("");

  while (mdsmap->get_epoch() == 0)
    wait_on_list(waiting_for_mdsmap);

  reclaim_errno = 0;
  for (unsigned mds = 0; mds < mdsmap->get_num_in_mds(); ) {
    if (!mdsmap->is_up(mds)) {
      ldout(cct, 10) << "mds." << mds << " not active, waiting for new mdsmap" << dendl;
      wait_on_list(waiting_for_mdsmap);
      continue;
    }

    MetaSession *session;
    if (!have_open_session(mds)) {
      session = _get_or_open_mds_session(mds);
      if (session->state == MetaSession::STATE_REJECTED)
	return -EPERM;
      if (session->state != MetaSession::STATE_OPENING) {
	// umounting?
	return -EINVAL;
      }
      ldout(cct, 10) << "waiting for session to mds." << mds << " to open" << dendl;
      wait_on_context_list(session->waiting_for_open);
      continue;
    }

    session = &mds_sessions.at(mds);
    if (!session->mds_features.test(CEPHFS_FEATURE_RECLAIM_CLIENT))
      return -EOPNOTSUPP;

    if (session->reclaim_state == MetaSession::RECLAIM_NULL ||
	session->reclaim_state == MetaSession::RECLAIMING) {
      session->reclaim_state = MetaSession::RECLAIMING;
      auto m = make_message<MClientReclaim>(uuid, flags);
      session->con->send_message2(std::move(m));
      wait_on_list(waiting_for_reclaim);
    } else if (session->reclaim_state == MetaSession::RECLAIM_FAIL) {
      return reclaim_errno ? : -ENOTRECOVERABLE;
    } else {
      mds++;
    }
  }

  // didn't find target session in any mds
  if (reclaim_target_addrs.empty()) {
    if (flags & CEPH_RECLAIM_RESET)
      return -ENOENT;
    return -ENOTRECOVERABLE;
  }

  if (flags & CEPH_RECLAIM_RESET)
    return 0;

  // use blacklist to check if target session was killed
  // (config option mds_session_blacklist_on_evict needs to be true)
  ldout(cct, 10) << __func__ << ": waiting for OSD epoch " << reclaim_osd_epoch << dendl;
  bs::error_code ec;
  l.unlock();
  objecter->wait_for_map(reclaim_osd_epoch, ca::use_blocked[ec]);
  l.lock();

  if (ec)
    return ceph::from_error_code(ec);

  bool blacklisted = objecter->with_osdmap(
      [this](const OSDMap &osd_map) -> bool {
	return osd_map.is_blacklisted(reclaim_target_addrs);
      });
  if (blacklisted)
    return -ENOTRECOVERABLE;

  metadata["reclaiming_uuid"] = uuid;
  return 0;
}

void Client::finish_reclaim()
{
  auto it = metadata.find("reclaiming_uuid");
  if (it == metadata.end()) {
    for (auto &p : mds_sessions)
      p.second.reclaim_state = MetaSession::RECLAIM_NULL;
    return;
  }

  for (auto &p : mds_sessions) {
    p.second.reclaim_state = MetaSession::RECLAIM_NULL;
    auto m = make_message<MClientReclaim>("", MClientReclaim::FLAG_FINISH);
    p.second.con->send_message2(std::move(m));
  }

  metadata["uuid"] = it->second;
  metadata.erase(it);
}

void Client::handle_client_reclaim_reply(const MConstRef<MClientReclaimReply>& reply)
{
  mds_rank_t from = mds_rank_t(reply->get_source().num());
  ldout(cct, 10) << __func__ << " " << *reply << " from mds." << from << dendl;

  MetaSession *session = _get_mds_session(from, reply->get_connection().get());
  if (!session) {
    ldout(cct, 10) << " discarding reclaim reply from sessionless mds." <<  from << dendl;
    return;
  }

  if (reply->get_result() >= 0) {
    session->reclaim_state = MetaSession::RECLAIM_OK;
    if (reply->get_epoch() > reclaim_osd_epoch)
      reclaim_osd_epoch = reply->get_epoch();
    if (!reply->get_addrs().empty())
      reclaim_target_addrs = reply->get_addrs();
  } else {
    session->reclaim_state = MetaSession::RECLAIM_FAIL;
    reclaim_errno = reply->get_result();
  }

  signal_cond_list(waiting_for_reclaim);
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
    "client_deleg_timeout",
    "client_deleg_break_on_open",
    NULL
  };
  return keys;
}

void Client::handle_conf_change(const ConfigProxy& conf,
				const std::set <std::string> &changed)
{
  std::lock_guard lock(client_lock);

  if (changed.count("client_cache_mid")) {
    lru.lru_set_midpoint(cct->_conf->client_cache_mid);
  }
  if (changed.count("client_acl_type")) {
    acl_type = NO_ACL;
    if (cct->_conf->client_acl_type == "posix_acl")
      acl_type = POSIX_ACL;
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
  ceph_assert(ceph_mutex_is_locked_by_me(client_lock));

  std::set<mds_rank_t> up;
  mdsmap->get_up_mds_set(up);

  if (up.empty())
    return MDS_RANK_NONE;
  std::set<mds_rank_t>::const_iterator p = up.begin();
  for (int n = rand() % up.size(); n; n--)
    ++p;
  return *p;
}


StandaloneClient::StandaloneClient(Messenger *m, MonClient *mc,
				   boost::asio::io_context& ictx)
  : Client(m, mc, new Objecter(m->cct, m, mc, ictx, 0, 0))
{
  monclient->set_messenger(m);
  objecter->set_client_incarnation(0);
}

StandaloneClient::~StandaloneClient()
{
  delete objecter;
  objecter = nullptr;
}

int StandaloneClient::init()
{
  _pre_init();
  objecter->init();

  client_lock.lock();
  ceph_assert(!is_initialized());

  messenger->add_dispatcher_tail(objecter);
  messenger->add_dispatcher_tail(this);

  monclient->set_want_keys(CEPH_ENTITY_TYPE_MDS | CEPH_ENTITY_TYPE_OSD);
  int r = monclient->init();
  if (r < 0) {
    // need to do cleanup because we're in an intermediate init state
    timer.shutdown();
    client_lock.unlock();
    objecter->shutdown();
    objectcacher->stop();
    monclient->shutdown();
    return r;
  }
  objecter->start();

  client_lock.unlock();
  _finish_init();

  return 0;
}

void StandaloneClient::shutdown()
{
  Client::shutdown();
  objecter->shutdown();
  monclient->shutdown();
}
