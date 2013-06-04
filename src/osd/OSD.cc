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

#include <fstream>
#include <iostream>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>
#include <ctype.h>
#include <boost/scoped_ptr.hpp>

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN || __FreeBSD__

#include "osd/PG.h"

#include "include/types.h"
#include "include/compat.h"

#include "OSD.h"
#include "OSDMap.h"
#include "Watch.h"

#include "common/ceph_argparse.h"
#include "common/version.h"
#include "os/FileStore.h"
#include "os/FileJournal.h"

#include "ReplicatedPG.h"

#include "Ager.h"


#include "msg/Messenger.h"
#include "msg/Message.h"

#include "mon/MonClient.h"

#include "messages/MLog.h"

#include "messages/MGenericMessage.h"
#include "messages/MPing.h"
#include "messages/MOSDPing.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDPGTemp.h"

#include "messages/MOSDMap.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGQuery.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGMissing.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"

#include "messages/MOSDAlive.h"

#include "messages/MOSDScrub.h"
#include "messages/MOSDRepScrub.h"

#include "messages/MMonCommand.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"

#include "messages/MWatchNotify.h"

#include "common/perf_counters.h"
#include "common/Timer.h"
#include "common/LogClient.h"
#include "common/safe_io.h"
#include "common/HeartbeatMap.h"
#include "common/admin_socket.h"

#include "global/signal_handler.h"
#include "global/pidfile.h"

#include "include/color.h"
#include "perfglue/cpu_profiler.h"
#include "perfglue/heap_profiler.h"

#include "osd/ClassHandler.h"
#include "osd/OpRequest.h"

#include "auth/AuthAuthorizeHandler.h"

#include "common/errno.h"

#include "objclass/objclass.h"

#include "common/cmdparse.h"
#include "include/str_list.h"

#include "include/assert.h"
#include "common/config.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, get_osdmap())

static ostream& _prefix(std::ostream* _dout, int whoami, OSDMapRef osdmap) {
  return *_dout << "osd." << whoami << " "
		<< (osdmap ? osdmap->get_epoch():0)
		<< " ";
}

const coll_t coll_t::META_COLL("meta");

static CompatSet get_osd_compat_set() {
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_PGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_OLOC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_HOBJECTPOOL);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BIGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEVELDBLOG);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_SNAPMAPPER);
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
}

OSDService::OSDService(OSD *osd) :
  osd(osd),
  whoami(osd->whoami), store(osd->store), clog(osd->clog),
  pg_recovery_stats(osd->pg_recovery_stats),
  infos_oid(OSD::make_infos_oid()),
  cluster_messenger(osd->cluster_messenger),
  client_messenger(osd->client_messenger),
  logger(osd->logger),
  monc(osd->monc),
  op_wq(osd->op_wq),
  peering_wq(osd->peering_wq),
  recovery_wq(osd->recovery_wq),
  snap_trim_wq(osd->snap_trim_wq),
  scrub_wq(osd->scrub_wq),
  scrub_finalize_wq(osd->scrub_finalize_wq),
  rep_scrub_wq(osd->rep_scrub_wq),
  class_handler(osd->class_handler),
  publish_lock("OSDService::publish_lock"),
  pre_publish_lock("OSDService::pre_publish_lock"),
  sched_scrub_lock("OSDService::sched_scrub_lock"), scrubs_pending(0),
  scrubs_active(0),
  watch_lock("OSD::watch_lock"),
  watch_timer(osd->client_messenger->cct, watch_lock),
  next_notif_id(0),
  backfill_request_lock("OSD::backfill_request_lock"),
  backfill_request_timer(g_ceph_context, backfill_request_lock, false),
  last_tid(0),
  tid_lock("OSDService::tid_lock"),
  reserver_finisher(g_ceph_context),
  local_reserver(&reserver_finisher, g_conf->osd_max_backfills),
  remote_reserver(&reserver_finisher, g_conf->osd_max_backfills),
  pg_temp_lock("OSDService::pg_temp_lock"),
  map_cache_lock("OSDService::map_lock"),
  map_cache(g_conf->osd_map_cache_size),
  map_bl_cache(g_conf->osd_map_cache_size),
  map_bl_inc_cache(g_conf->osd_map_cache_size),
  in_progress_split_lock("OSDService::in_progress_split_lock"),
  full_status_lock("OSDService::full_status_lock"),
  cur_state(NONE),
  last_msg(0),
  cur_ratio(0),
  is_stopping_lock("OSDService::is_stopping_lock"),
  state(NOT_STOPPING)
#ifdef PG_DEBUG_REFS
  , pgid_lock("OSDService::pgid_lock")
#endif
{}

void OSDService::_start_split(pg_t parent, const set<pg_t> &children)
{
  for (set<pg_t>::const_iterator i = children.begin();
       i != children.end();
       ++i) {
    dout(10) << __func__ << ": Starting split on pg " << *i
	     << ", parent=" << parent << dendl;
    assert(!pending_splits.count(*i));
    assert(!in_progress_splits.count(*i));
    pending_splits.insert(make_pair(*i, parent));

    assert(!rev_pending_splits[parent].count(*i));
    rev_pending_splits[parent].insert(*i);
  }
}

void OSDService::mark_split_in_progress(pg_t parent, const set<pg_t> &children)
{
  Mutex::Locker l(in_progress_split_lock);
  map<pg_t, set<pg_t> >::iterator piter = rev_pending_splits.find(parent);
  assert(piter != rev_pending_splits.end());
  for (set<pg_t>::const_iterator i = children.begin();
       i != children.end();
       ++i) {
    assert(piter->second.count(*i));
    assert(pending_splits.count(*i));
    assert(!in_progress_splits.count(*i));
    assert(pending_splits[*i] == parent);

    pending_splits.erase(*i);
    piter->second.erase(*i);
    in_progress_splits.insert(*i);
  }
  if (piter->second.empty())
    rev_pending_splits.erase(piter);
}

void OSDService::cancel_pending_splits_for_parent(pg_t parent)
{
  Mutex::Locker l(in_progress_split_lock);
  return _cancel_pending_splits_for_parent(parent);
}

void OSDService::_cancel_pending_splits_for_parent(pg_t parent)
{
  map<pg_t, set<pg_t> >::iterator piter = rev_pending_splits.find(parent);
  if (piter == rev_pending_splits.end())
    return;

  for (set<pg_t>::iterator i = piter->second.begin();
       i != piter->second.end();
       ++i) {
    assert(pending_splits.count(*i));
    assert(!in_progress_splits.count(*i));
    pending_splits.erase(*i);
    dout(10) << __func__ << ": Completing split on pg " << *i
	     << " for parent: " << parent << dendl;
    _cancel_pending_splits_for_parent(*i);
  }
  rev_pending_splits.erase(piter);
}

void OSDService::_maybe_split_pgid(OSDMapRef old_map,
				  OSDMapRef new_map,
				  pg_t pgid)
{
  assert(old_map->have_pg_pool(pgid.pool()));
  if (pgid.ps() < static_cast<unsigned>(old_map->get_pg_num(pgid.pool()))) {
    set<pg_t> children;
    pgid.is_split(old_map->get_pg_num(pgid.pool()),
		  new_map->get_pg_num(pgid.pool()), &children);
    _start_split(pgid, children);
  } else {
    assert(pgid.ps() < static_cast<unsigned>(new_map->get_pg_num(pgid.pool())));
  }
}

void OSDService::init_splits_between(pg_t pgid,
				     OSDMapRef frommap,
				     OSDMapRef tomap)
{
  // First, check whether we can avoid this potentially expensive check
  if (tomap->have_pg_pool(pgid.pool()) &&
      pgid.is_split(
	frommap->get_pg_num(pgid.pool()),
	tomap->get_pg_num(pgid.pool()),
	NULL)) {
    // Ok, a split happened, so we need to walk the osdmaps
    set<pg_t> new_pgs; // pgs to scan on each map
    new_pgs.insert(pgid);
    for (epoch_t e = frommap->get_epoch() + 1;
	 e <= tomap->get_epoch();
	 ++e) {
      OSDMapRef curmap(get_map(e-1));
      OSDMapRef nextmap(get_map(e));
      set<pg_t> even_newer_pgs; // pgs added in this loop
      for (set<pg_t>::iterator i = new_pgs.begin(); i != new_pgs.end(); ++i) {
	set<pg_t> split_pgs;
	if (i->is_split(curmap->get_pg_num(i->pool()),
			nextmap->get_pg_num(i->pool()),
			&split_pgs)) {
	  start_split(*i, split_pgs);
	  even_newer_pgs.insert(split_pgs.begin(), split_pgs.end());
	}
      }
      new_pgs.insert(even_newer_pgs.begin(), even_newer_pgs.end());
    }
  }
}

void OSDService::expand_pg_num(OSDMapRef old_map,
			       OSDMapRef new_map)
{
  Mutex::Locker l(in_progress_split_lock);
  for (set<pg_t>::iterator i = in_progress_splits.begin();
       i != in_progress_splits.end();
    ) {
    if (!new_map->have_pg_pool(i->pool())) {
      in_progress_splits.erase(i++);
    } else {
      _maybe_split_pgid(old_map, new_map, *i);
      ++i;
    }
  }
  for (map<pg_t, pg_t>::iterator i = pending_splits.begin();
       i != pending_splits.end();
    ) {
    if (!new_map->have_pg_pool(i->first.pool())) {
      rev_pending_splits.erase(i->second);
      pending_splits.erase(i++);
    } else {
      _maybe_split_pgid(old_map, new_map, i->first);
      ++i;
    }
  }
}

bool OSDService::splitting(pg_t pgid)
{
  Mutex::Locker l(in_progress_split_lock);
  return in_progress_splits.count(pgid) ||
    pending_splits.count(pgid);
}

void OSDService::complete_split(const set<pg_t> &pgs)
{
  Mutex::Locker l(in_progress_split_lock);
  for (set<pg_t>::const_iterator i = pgs.begin();
       i != pgs.end();
       ++i) {
    dout(10) << __func__ << ": Completing split on pg " << *i << dendl;
    assert(!pending_splits.count(*i));
    assert(in_progress_splits.count(*i));
    in_progress_splits.erase(*i);
  }
}

void OSDService::need_heartbeat_peer_update()
{
  osd->need_heartbeat_peer_update();
}

void OSDService::pg_stat_queue_enqueue(PG *pg)
{
  osd->pg_stat_queue_enqueue(pg);
}

void OSDService::pg_stat_queue_dequeue(PG *pg)
{
  osd->pg_stat_queue_dequeue(pg);
}

void OSDService::shutdown()
{
  reserver_finisher.stop();
  {
    Mutex::Locker l(watch_lock);
    watch_timer.shutdown();
  }
  {
    Mutex::Locker l(backfill_request_lock);
    backfill_request_timer.shutdown();
  }
  osdmap = OSDMapRef();
  next_osdmap = OSDMapRef();
}

void OSDService::init()
{
  reserver_finisher.start();
  watch_timer.init();
}

ObjectStore *OSD::create_object_store(const std::string &dev, const std::string &jdev)
{
  struct stat st;
  if (::stat(dev.c_str(), &st) != 0)
    return 0;

  if (g_conf->filestore)
    return new FileStore(dev, jdev);

  if (S_ISDIR(st.st_mode))
    return new FileStore(dev, jdev);
  else
    return 0;
}

#undef dout_prefix
#define dout_prefix *_dout

int OSD::convert_collection(ObjectStore *store, coll_t cid)
{
  coll_t tmp0("convertfs_temp");
  coll_t tmp1("convertfs_temp1");
  vector<hobject_t> objects;

  map<string, bufferptr> aset;
  int r = store->collection_getattrs(cid, aset);
  if (r < 0)
    return r;

  {
    ObjectStore::Transaction t;
    t.create_collection(tmp0);
    for (map<string, bufferptr>::iterator i = aset.begin();
	 i != aset.end();
	 ++i) {
      bufferlist val;
      val.push_back(i->second);
      t.collection_setattr(tmp0, i->first, val);
    }
    store->apply_transaction(t);
  }

  hobject_t next;
  while (!next.is_max()) {
    objects.clear();
    hobject_t start = next;
    r = store->collection_list_partial(cid, start,
				       200, 300, 0,
				       &objects, &next);
    if (r < 0)
      return r;

    ObjectStore::Transaction t;
    for (vector<hobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      t.collection_add(tmp0, cid, *i);
    }
    store->apply_transaction(t);
  }

  {
    ObjectStore::Transaction t;
    t.collection_rename(cid, tmp1);
    t.collection_rename(tmp0, cid);
    store->apply_transaction(t);
  }

  recursive_remove_collection(store, tmp1);
  store->sync_and_flush();
  store->sync();
  return 0;
}

int OSD::do_convertfs(ObjectStore *store)
{
  int r = store->mount();
  if (r < 0)
    return r;

  uint32_t version;
  r = store->version_stamp_is_valid(&version);
  if (r < 0)
    return r;
  if (r == 1)
    return store->umount();

  derr << "FileStore is old at version " << version << ".  Updating..."  << dendl;

  derr << "Removing tmp pgs" << dendl;
  vector<coll_t> collections;
  r = store->list_collections(collections);
  if (r < 0)
    return r;
  for (vector<coll_t>::iterator i = collections.begin();
       i != collections.end();
       ++i) {
    pg_t pgid;
    if (i->is_temp(pgid))
      recursive_remove_collection(store, *i);
    else if (i->to_str() == "convertfs_temp" ||
	     i->to_str() == "convertfs_temp1")
      recursive_remove_collection(store, *i);
  }
  store->flush();


  derr << "Getting collections" << dendl;

  derr << collections.size() << " to process." << dendl;
  collections.clear();
  r = store->list_collections(collections);
  if (r < 0)
    return r;
  int processed = 0;
  for (vector<coll_t>::iterator i = collections.begin();
       i != collections.end();
       ++i, ++processed) {
    derr << processed << "/" << collections.size() << " processed" << dendl;
    uint32_t collection_version;
    r = store->collection_version_current(*i, &collection_version);
    if (r < 0) {
      return r;
    } else if (r == 1) {
      derr << "Collection " << *i << " is up to date" << dendl;
    } else {
      derr << "Updating collection " << *i << " current version is " 
	   << collection_version << dendl;
      r = convert_collection(store, *i);
      if (r < 0)
	return r;
      derr << "collection " << *i << " updated" << dendl;
    }
  }
  derr << "All collections up to date, updating version stamp..." << dendl;
  r = store->update_version_stamp();
  if (r < 0)
    return r;
  store->sync_and_flush();
  store->sync();
  derr << "Version stamp updated, done with upgrade!" << dendl;
  return store->umount();
}

int OSD::convertfs(const std::string &dev, const std::string &jdev)
{
  boost::scoped_ptr<ObjectStore> store(
    new FileStore(dev, jdev, "filestore", 
		  true));
  int r = do_convertfs(store.get());
  return r;
}

int OSD::mkfs(const std::string &dev, const std::string &jdev, uuid_d fsid, int whoami)
{
  int ret;
  ObjectStore *store = NULL;

  try {
    store = create_object_store(dev, jdev);
    if (!store) {
      ret = -ENOENT;
      goto out;
    }

    // if we are fed a uuid for this osd, use it.
    store->set_fsid(g_conf->osd_uuid);

    ret = store->mkfs();
    if (ret) {
      derr << "OSD::mkfs: FileStore::mkfs failed with error " << ret << dendl;
      goto free_store;
    }

    ret = store->mount();
    if (ret) {
      derr << "OSD::mkfs: couldn't mount FileStore: error " << ret << dendl;
      goto free_store;
    }

    // age?
    if (g_conf->osd_age_time != 0) {
      if (g_conf->osd_age_time >= 0) {
	dout(0) << "aging..." << dendl;
	Ager ager(store);
	ager.age(g_conf->osd_age_time,
		 g_conf->osd_age,
		 g_conf->osd_age - .05,
		 50000,
		 g_conf->osd_age - .05);
      }
    }

    OSDSuperblock sb;
    bufferlist sbbl;
    ret = store->read(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, 0, sbbl);
    if (ret >= 0) {
      dout(0) << " have superblock" << dendl;
      if (whoami != sb.whoami) {
	derr << "provided osd id " << whoami << " != superblock's " << sb.whoami << dendl;
	ret = -EINVAL;
	goto umount_store;
      }
      if (fsid != sb.cluster_fsid) {
	derr << "provided cluster fsid " << fsid << " != superblock's " << sb.cluster_fsid << dendl;
	ret = -EINVAL;
	goto umount_store;
      }
    } else {
      // create superblock
      if (fsid.is_zero()) {
	derr << "must specify cluster fsid" << dendl;
	ret = -EINVAL;
	goto umount_store;
      }

      sb.cluster_fsid = fsid;
      sb.osd_fsid = store->get_fsid();
      sb.whoami = whoami;
      sb.compat_features = get_osd_compat_set();

      // benchmark?
      if (g_conf->osd_auto_weight) {
	bufferlist bl;
	bufferptr bp(1048576);
	bp.zero();
	bl.push_back(bp);
	dout(0) << "testing disk bandwidth..." << dendl;
	utime_t start = ceph_clock_now(g_ceph_context);
	object_t oid("disk_bw_test");
	for (int i=0; i<1000; i++) {
	  ObjectStore::Transaction *t = new ObjectStore::Transaction;
	  t->write(coll_t::META_COLL, hobject_t(sobject_t(oid, 0)), i*bl.length(), bl.length(), bl);
	  store->queue_transaction(NULL, t);
	}
	store->sync();
	utime_t end = ceph_clock_now(g_ceph_context);
	end -= start;
	dout(0) << "measured " << (1000.0 / (double)end) << " mb/sec" << dendl;
	ObjectStore::Transaction tr;
	tr.remove(coll_t::META_COLL, hobject_t(sobject_t(oid, 0)));
	ret = store->apply_transaction(tr);
	if (ret) {
	  derr << "OSD::mkfs: error while benchmarking: apply_transaction returned "
	       << ret << dendl;
	  goto umount_store;
	}
	
	// set osd weight
	sb.weight = (1000.0 / (double)end);
      }

      bufferlist bl;
      ::encode(sb, bl);

      ObjectStore::Transaction t;
      t.create_collection(coll_t::META_COLL);
      t.write(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
      ret = store->apply_transaction(t);
      if (ret) {
	derr << "OSD::mkfs: error while writing OSD_SUPERBLOCK_POBJECT: "
	     << "apply_transaction returned " << ret << dendl;
	goto umount_store;
      }
    }

    store->sync_and_flush();

    ret = write_meta(dev, sb.cluster_fsid, sb.osd_fsid, whoami);
    if (ret) {
      derr << "OSD::mkfs: failed to write fsid file: error " << ret << dendl;
      goto umount_store;
    }

    ret = write_meta(dev, "ready", "ready\n", 6);
    if (ret) {
      derr << "OSD::mkfs: failed to write ready file: error " << ret << dendl;
      goto umount_store;
    }

  }
  catch (const std::exception &se) {
    derr << "OSD::mkfs: caught exception " << se.what() << dendl;
    ret = 1000;
  }
  catch (...) {
    derr << "OSD::mkfs: caught unknown exception." << dendl;
    ret = 1000;
  }

umount_store:
  store->umount();
free_store:
  delete store;
out:
  return ret;
}

int OSD::mkjournal(const std::string &dev, const std::string &jdev)
{
  ObjectStore *store = create_object_store(dev, jdev);
  if (!store)
    return -ENOENT;
  return store->mkjournal();
}

int OSD::flushjournal(const std::string &dev, const std::string &jdev)
{
  ObjectStore *store = create_object_store(dev, jdev);
  if (!store)
    return -ENOENT;
  int err = store->mount();
  if (!err) {
    store->sync_and_flush();
    store->umount();
  }
  delete store;
  return err;
}

int OSD::dump_journal(const std::string &dev, const std::string &jdev, ostream& out)
{
  ObjectStore *store = create_object_store(dev, jdev);
  if (!store)
    return -ENOENT;
  int err = store->dump_journal(out);
  delete store;
  return err;
}

int OSD::write_meta(const std::string &base, const std::string &file,
		    const char *val, size_t vallen)
{
  int ret;
  char fn[PATH_MAX];
  char tmp[PATH_MAX];
  int fd;

  // does the file already have correct content?
  char oldval[80];
  ret = read_meta(base, file, oldval, sizeof(oldval));
  if (ret == (int)vallen && memcmp(oldval, val, vallen) == 0)
    return 0;  // yes.

  snprintf(fn, sizeof(fn), "%s/%s", base.c_str(), file.c_str());
  snprintf(tmp, sizeof(tmp), "%s/%s.tmp", base.c_str(), file.c_str());
  fd = ::open(tmp, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd < 0) {
    ret = errno;
    derr << "write_meta: error opening '" << tmp << "': "
	 << cpp_strerror(ret) << dendl;
    return -ret;
  }
  ret = safe_write(fd, val, vallen);
  if (ret) {
    derr << "write_meta: failed to write to '" << tmp << "': "
	 << cpp_strerror(ret) << dendl;
    TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }

  ret = ::fsync(fd);
  TEMP_FAILURE_RETRY(::close(fd));
  if (ret) {
    ::unlink(tmp);
    derr << "write_meta: failed to fsync to '" << tmp << "': "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }
  ret = ::rename(tmp, fn);
  if (ret) {
    ::unlink(tmp);
    derr << "write_meta: failed to rename '" << tmp << "' to '" << fn << "': "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }

  fd = ::open(base.c_str(), O_RDONLY);
  if (fd < 0) {
    ret = errno;
    derr << "write_meta: failed to open dir '" << base << "': "
	 << cpp_strerror(ret) << dendl;
    return -ret;
  }
  ::fsync(fd);
  TEMP_FAILURE_RETRY(::close(fd));

  return 0;
}

int OSD::read_meta(const  std::string &base, const std::string &file,
		   char *val, size_t vallen)
{
  char fn[PATH_MAX];
  int fd, len;

  snprintf(fn, sizeof(fn), "%s/%s", base.c_str(), file.c_str());
  fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    int err = errno;
    return -err;
  }
  len = safe_read(fd, val, vallen);
  if (len < 0) {
    TEMP_FAILURE_RETRY(::close(fd));
    return len;
  }
  // close sometimes returns errors, but only after write()
  TEMP_FAILURE_RETRY(::close(fd));

  val[len] = 0;
  return len;
}

int OSD::write_meta(const std::string &base, uuid_d& cluster_fsid, uuid_d& osd_fsid, int whoami)
{
  char val[80];
  
  snprintf(val, sizeof(val), "%s\n", CEPH_OSD_ONDISK_MAGIC);
  write_meta(base, "magic", val, strlen(val));

  snprintf(val, sizeof(val), "%d\n", whoami);
  write_meta(base, "whoami", val, strlen(val));

  cluster_fsid.print(val);
  strcat(val, "\n");
  write_meta(base, "ceph_fsid", val, strlen(val));

  return 0;
}

int OSD::peek_meta(const std::string &dev, std::string& magic,
		   uuid_d& cluster_fsid, uuid_d& osd_fsid, int& whoami)
{
  char val[80] = { 0 };

  if (read_meta(dev, "magic", val, sizeof(val)) < 0)
    return -errno;
  int l = strlen(val);
  if (l && val[l-1] == '\n')
    val[l-1] = 0;
  magic = val;

  if (read_meta(dev, "whoami", val, sizeof(val)) < 0)
    return -errno;
  whoami = atoi(val);

  if (read_meta(dev, "ceph_fsid", val, sizeof(val)) < 0)
    return -errno;
  if (strlen(val) > 36)
    val[36] = 0;
  cluster_fsid.parse(val);

  if (read_meta(dev, "fsid", val, sizeof(val)) < 0)
    osd_fsid = uuid_d();
  else {
    if (strlen(val) > 36)
      val[36] = 0;
    osd_fsid.parse(val);
  }

  return 0;
}

int OSD::peek_journal_fsid(string path, uuid_d& fsid)
{
  // make sure we don't try to use aio or direct_io (and get annoying
  // error messages from failing to do so); performance implications
  // should be irrelevant for this use
  FileJournal j(fsid, 0, 0, path.c_str(), false, false);
  return j.peek_fsid(fsid);
}


#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, osdmap)

// cons/des

OSD::OSD(int id, Messenger *internal_messenger, Messenger *external_messenger,
	 Messenger *hb_clientm,
	 Messenger *hb_front_serverm,
	 Messenger *hb_back_serverm,
	 MonClient *mc,
	 const std::string &dev, const std::string &jdev) :
  Dispatcher(external_messenger->cct),
  osd_lock("OSD::osd_lock"),
  tick_timer(external_messenger->cct, osd_lock),
  authorize_handler_cluster_registry(new AuthAuthorizeHandlerRegistry(external_messenger->cct,
								      cct->_conf->auth_supported.length() ?
								      cct->_conf->auth_supported :
								      cct->_conf->auth_cluster_required)),
  authorize_handler_service_registry(new AuthAuthorizeHandlerRegistry(external_messenger->cct,
								      cct->_conf->auth_supported.length() ?
								      cct->_conf->auth_supported :
								      cct->_conf->auth_service_required)),
  cluster_messenger(internal_messenger),
  client_messenger(external_messenger),
  monc(mc),
  logger(NULL),
  store(NULL),
  clog(external_messenger->cct, client_messenger, &mc->monmap, LogClient::NO_FLAGS),
  whoami(id),
  dev_path(dev), journal_path(jdev),
  dispatch_running(false),
  asok_hook(NULL),
  osd_compat(get_osd_compat_set()),
  state(STATE_INITIALIZING), boot_epoch(0), up_epoch(0), bind_epoch(0),
  op_tp(external_messenger->cct, "OSD::op_tp", g_conf->osd_op_threads, "osd_op_threads"),
  recovery_tp(external_messenger->cct, "OSD::recovery_tp", g_conf->osd_recovery_threads, "osd_recovery_threads"),
  disk_tp(external_messenger->cct, "OSD::disk_tp", g_conf->osd_disk_threads, "osd_disk_threads"),
  command_tp(external_messenger->cct, "OSD::command_tp", 1),
  paused_recovery(false),
  heartbeat_lock("OSD::heartbeat_lock"),
  heartbeat_stop(false), heartbeat_need_update(true), heartbeat_epoch(0),
  hbclient_messenger(hb_clientm),
  hb_front_server_messenger(hb_front_serverm),
  hb_back_server_messenger(hb_back_serverm),
  heartbeat_thread(this),
  heartbeat_dispatcher(this),
  stat_lock("OSD::stat_lock"),
  finished_lock("OSD::finished_lock"),
  test_ops_hook(NULL),
  op_wq(this, g_conf->osd_op_thread_timeout, &op_tp),
  peering_wq(this, g_conf->osd_op_thread_timeout, &op_tp, 200),
  map_lock("OSD::map_lock"),
  peer_map_epoch_lock("OSD::peer_map_epoch_lock"),
  debug_drop_pg_create_probability(g_conf->osd_debug_drop_pg_create_probability),
  debug_drop_pg_create_duration(g_conf->osd_debug_drop_pg_create_duration),
  debug_drop_pg_create_left(-1),
  outstanding_pg_stats(false),
  up_thru_wanted(0), up_thru_pending(0),
  pg_stat_queue_lock("OSD::pg_stat_queue_lock"),
  osd_stat_updated(false),
  pg_stat_tid(0), pg_stat_tid_flushed(0),
  command_wq(this, g_conf->osd_command_thread_timeout, &command_tp),
  recovery_ops_active(0),
  recovery_wq(this, g_conf->osd_recovery_thread_timeout, &recovery_tp),
  replay_queue_lock("OSD::replay_queue_lock"),
  snap_trim_wq(this, g_conf->osd_snap_trim_thread_timeout, &disk_tp),
  scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  scrub_finalize_wq(this, g_conf->osd_scrub_finalize_thread_timeout, &op_tp),
  rep_scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  remove_wq(store, g_conf->osd_remove_thread_timeout, &disk_tp),
  next_removal_seq(0),
  service(this)
{
  monc->set_messenger(client_messenger);
}

OSD::~OSD()
{
  delete authorize_handler_cluster_registry;
  delete authorize_handler_service_registry;
  delete class_handler;
  g_ceph_context->get_perfcounters_collection()->remove(logger);
  delete logger;
  delete store;
}

void cls_initialize(ClassHandler *ch);

void OSD::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** Got signal " << sys_siglist[signum] << " ***" << dendl;
  //suicide(128 + signum);
  shutdown();
}

int OSD::pre_init()
{
  Mutex::Locker lock(osd_lock);
  if (is_stopping())
    return 0;
  
  assert(!store);
  store = create_object_store(dev_path, journal_path);
  if (!store) {
    derr << "OSD::pre_init: unable to create object store" << dendl;
    return -ENODEV;
  }

  if (store->test_mount_in_use()) {
    derr << "OSD::pre_init: object store '" << dev_path << "' is "
         << "currently in use. (Is ceph-osd already running?)" << dendl;
    return -EBUSY;
  }

  g_conf->add_observer(this);
  return 0;
}

// asok

class OSDSocketHook : public AdminSocketHook {
  OSD *osd;
public:
  OSDSocketHook(OSD *o) : osd(o) {}
  bool call(std::string command, std::string args, bufferlist& out) {
    stringstream ss;
    bool r = osd->asok_command(command, args, ss);
    out.append(ss);
    return r;
  }
};

bool OSD::asok_command(string command, string args, ostream& ss)
{
  if (command == "dump_ops_in_flight") {
    op_tracker.dump_ops_in_flight(ss);
  } else if (command == "dump_historic_ops") {
    op_tracker.dump_historic_ops(ss);
  } else if (command == "dump_op_pq_state") {
    JSONFormatter f(true);
    f.open_object_section("pq");
    op_wq.dump(&f);
    f.close_section();
    f.flush(ss);
  } else {
    assert(0 == "broken asok registration");
  }
  return true;
}

class TestOpsSocketHook : public AdminSocketHook {
  OSDService *service;
  ObjectStore *store;
public:
  TestOpsSocketHook(OSDService *s, ObjectStore *st) : service(s), store(st) {}
  bool call(std::string command, std::string args, bufferlist& out) {
    stringstream ss;
    test_ops(service, store, command, args, ss);
    out.append(ss);
    return true;
  }
  void test_ops(OSDService *service, ObjectStore *store, std::string command,
     std::string args, ostream &ss);

};

int OSD::init()
{
  Mutex::Locker lock(osd_lock);
  if (is_stopping())
    return 0;

  tick_timer.init();
  service.backfill_request_timer.init();

  // mount.
  dout(2) << "mounting " << dev_path << " "
	  << (journal_path.empty() ? "(no journal)" : journal_path) << dendl;
  assert(store);  // call pre_init() first!

  int r = store->mount();
  if (r < 0) {
    derr << "OSD:init: unable to mount object store" << dendl;
    return r;
  }

  dout(2) << "boot" << dendl;

  // read superblock
  r = read_superblock();
  if (r < 0) {
    derr << "OSD::init() : unable to read osd superblock" << dendl;
    store->umount();
    delete store;
    return -EINVAL;
  }

  // make sure info object exists
  if (!store->exists(coll_t::META_COLL, service.infos_oid)) {
    dout(10) << "init creating/touching snapmapper object" << dendl;
    ObjectStore::Transaction t;
    t.touch(coll_t::META_COLL, service.infos_oid);
    r = store->apply_transaction(t);
    if (r < 0)
      return r;
  }

  // make sure snap mapper object exists
  if (!store->exists(coll_t::META_COLL, OSD::make_snapmapper_oid())) {
    dout(10) << "init creating/touching infos object" << dendl;
    ObjectStore::Transaction t;
    t.touch(coll_t::META_COLL, OSD::make_snapmapper_oid());
    r = store->apply_transaction(t);
    if (r < 0)
      return r;
  }

  if (osd_compat.compare(superblock.compat_features) != 0) {
    // We need to persist the new compat_set before we
    // do anything else
    dout(5) << "Upgrading superblock compat_set" << dendl;
    superblock.compat_features = osd_compat;
    ObjectStore::Transaction t;
    write_superblock(t);
    r = store->apply_transaction(t);
    if (r < 0)
      return r;
  }

  class_handler = new ClassHandler();
  cls_initialize(class_handler);

  // load up "current" osdmap
  assert_warn(!osdmap);
  if (osdmap) {
    derr << "OSD::init: unable to read current osdmap" << dendl;
    return -EINVAL;
  }
  osdmap = get_map(superblock.current_epoch);
  check_osdmap_features();

  bind_epoch = osdmap->get_epoch();

  // load up pgs (as they previously existed)
  load_pgs();

  dout(2) << "superblock: i am osd." << superblock.whoami << dendl;
  assert_warn(whoami == superblock.whoami);
  if (whoami != superblock.whoami) {
    derr << "OSD::init: logic error: superblock says osd"
	 << superblock.whoami << " but i am osd." << whoami << dendl;
    return -EINVAL;
  }

  create_logger();
    
  // i'm ready!
  client_messenger->add_dispatcher_head(this);
  cluster_messenger->add_dispatcher_head(this);

  hbclient_messenger->add_dispatcher_head(&heartbeat_dispatcher);
  hb_front_server_messenger->add_dispatcher_head(&heartbeat_dispatcher);
  hb_back_server_messenger->add_dispatcher_head(&heartbeat_dispatcher);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  r = monc->init();
  if (r < 0)
    return r;

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&clog);

  op_tp.start();
  recovery_tp.start();
  disk_tp.start();
  command_tp.start();

  // start the heartbeat
  heartbeat_thread.create();

  // tick
  tick_timer.add_event_after(g_conf->osd_heartbeat_interval, new C_Tick(this));

  AdminSocket *admin_socket = cct->get_admin_socket();
  asok_hook = new OSDSocketHook(this);
  r = admin_socket->register_command("dump_ops_in_flight",
				     "dump_ops_in_flight", asok_hook,
				     "show the ops currently in flight");
  assert(r == 0);
  r = admin_socket->register_command("dump_historic_ops", "dump_historic_ops",
				     asok_hook,
				     "show slowest recent ops");
  assert(r == 0);
  r = admin_socket->register_command("dump_op_pq_state", "dump_op_pq_state",
				     asok_hook,
				     "dump op priority queue state");
  assert(r == 0);
  test_ops_hook = new TestOpsSocketHook(&(this->service), this->store);
  r = admin_socket->register_command(
   "setomapval",
   "setomapval " \
   "name=pool,type=CephPoolname " \
   "name=objname,type=CephObjectname " \
   "name=key,type=CephString "\
   "name=val,type=CephString",
   test_ops_hook,
   "set omap key");
  assert(r == 0);
  r = admin_socket->register_command(
    "rmomapkey",
    "rmomapkey " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname " \
    "name=key,type=CephString",
    test_ops_hook,
    "remove omap key");
  assert(r == 0);
  r = admin_socket->register_command(
    "setomapheader",
    "setomapheader " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname " \
    "name=header,type=CephString",
    test_ops_hook,
    "set omap header");
  assert(r == 0);

  r = admin_socket->register_command(
    "getomap",
    "getomap " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname",
    test_ops_hook,
    "output entire object map");
  assert(r == 0);

  r = admin_socket->register_command(
    "truncobj",
    "truncobj " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname " \
    "name=len,type=CephInt",
    test_ops_hook,
    "truncate object to length");
  assert(r == 0);

  r = admin_socket->register_command(
    "injectdataerr",
    "injectdataerr " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname",
    test_ops_hook,
    "inject data error into omap");
  assert(r == 0);

  r = admin_socket->register_command(
    "injectmdataerr",
    "injectmdataerr " \
    "name=pool,type=CephPoolname " \
    "name=objname,type=CephObjectname",
    test_ops_hook,
    "inject metadata error");
  assert(r == 0);

  service.init();
  service.publish_map(osdmap);
  service.publish_superblock(superblock);

  osd_lock.Unlock();

  r = monc->authenticate();
  if (r < 0) {
    monc->shutdown();
    store->umount();
    osd_lock.Lock(); // locker is going to unlock this on function exit
    if (is_stopping())
      return 0;
    return r;
  }

  while (monc->wait_auth_rotating(30.0) < 0) {
    derr << "unable to obtain rotating service keys; retrying" << dendl;
  }

  osd_lock.Lock();
  if (is_stopping())
    return 0;

  dout(10) << "ensuring pgs have consumed prior maps" << dendl;
  consume_map();
  peering_wq.drain();

  dout(10) << "done with init, starting boot process" << dendl;
  state = STATE_BOOTING;
  start_boot();

  return 0;
}

void OSD::create_logger()
{
  dout(10) << "create_logger" << dendl;

  PerfCountersBuilder osd_plb(g_ceph_context, "osd", l_osd_first, l_osd_last);

  osd_plb.add_u64(l_osd_opq, "opq");       // op queue length (waiting to be processed yet)
  osd_plb.add_u64(l_osd_op_wip, "op_wip");   // rep ops currently being processed (primary)

  osd_plb.add_u64_counter(l_osd_op,       "op");           // client ops
  osd_plb.add_u64_counter(l_osd_op_inb,   "op_in_bytes");       // client op in bytes (writes)
  osd_plb.add_u64_counter(l_osd_op_outb,  "op_out_bytes");      // client op out bytes (reads)
  osd_plb.add_time_avg(l_osd_op_lat,   "op_latency");       // client op latency

  osd_plb.add_u64_counter(l_osd_op_r,      "op_r");        // client reads
  osd_plb.add_u64_counter(l_osd_op_r_outb, "op_r_out_bytes");   // client read out bytes
  osd_plb.add_time_avg(l_osd_op_r_lat,  "op_r_latency");    // client read latency
  osd_plb.add_u64_counter(l_osd_op_w,      "op_w");        // client writes
  osd_plb.add_u64_counter(l_osd_op_w_inb,  "op_w_in_bytes");    // client write in bytes
  osd_plb.add_time_avg(l_osd_op_w_rlat, "op_w_rlat");   // client write readable/applied latency
  osd_plb.add_time_avg(l_osd_op_w_lat,  "op_w_latency");    // client write latency
  osd_plb.add_u64_counter(l_osd_op_rw,     "op_rw");       // client rmw
  osd_plb.add_u64_counter(l_osd_op_rw_inb, "op_rw_in_bytes");   // client rmw in bytes
  osd_plb.add_u64_counter(l_osd_op_rw_outb,"op_rw_out_bytes");  // client rmw out bytes
  osd_plb.add_time_avg(l_osd_op_rw_rlat,"op_rw_rlat");  // client rmw readable/applied latency
  osd_plb.add_time_avg(l_osd_op_rw_lat, "op_rw_latency");   // client rmw latency

  osd_plb.add_u64_counter(l_osd_sop,       "subop");         // subops
  osd_plb.add_u64_counter(l_osd_sop_inb,   "subop_in_bytes");     // subop in bytes
  osd_plb.add_time_avg(l_osd_sop_lat,   "subop_latency");     // subop latency

  osd_plb.add_u64_counter(l_osd_sop_w,     "subop_w");          // replicated (client) writes
  osd_plb.add_u64_counter(l_osd_sop_w_inb, "subop_w_in_bytes");      // replicated write in bytes
  osd_plb.add_time_avg(l_osd_sop_w_lat, "subop_w_latency");      // replicated write latency
  osd_plb.add_u64_counter(l_osd_sop_pull,     "subop_pull");       // pull request
  osd_plb.add_time_avg(l_osd_sop_pull_lat, "subop_pull_latency");
  osd_plb.add_u64_counter(l_osd_sop_push,     "subop_push");       // push (write)
  osd_plb.add_u64_counter(l_osd_sop_push_inb, "subop_push_in_bytes");
  osd_plb.add_time_avg(l_osd_sop_push_lat, "subop_push_latency");

  osd_plb.add_u64_counter(l_osd_pull,      "pull");       // pull requests sent
  osd_plb.add_u64_counter(l_osd_push,      "push");       // push messages
  osd_plb.add_u64_counter(l_osd_push_outb, "push_out_bytes");  // pushed bytes

  osd_plb.add_u64_counter(l_osd_push_in,    "push_in");        // inbound push messages
  osd_plb.add_u64_counter(l_osd_push_inb,   "push_in_bytes");  // inbound pushed bytes

  osd_plb.add_u64_counter(l_osd_rop, "recovery_ops");       // recovery ops (started)

  osd_plb.add_u64(l_osd_loadavg, "loadavg");
  osd_plb.add_u64(l_osd_buf, "buffer_bytes");       // total ceph::buffer bytes

  osd_plb.add_u64(l_osd_pg, "numpg");   // num pgs
  osd_plb.add_u64(l_osd_pg_primary, "numpg_primary"); // num primary pgs
  osd_plb.add_u64(l_osd_pg_replica, "numpg_replica"); // num replica pgs
  osd_plb.add_u64(l_osd_pg_stray, "numpg_stray");   // num stray pgs
  osd_plb.add_u64(l_osd_hb_to, "heartbeat_to_peers");     // heartbeat peers we send to
  osd_plb.add_u64(l_osd_hb_from, "heartbeat_from_peers"); // heartbeat peers we recv from
  osd_plb.add_u64_counter(l_osd_map, "map_messages");           // osdmap messages
  osd_plb.add_u64_counter(l_osd_mape, "map_message_epochs");         // osdmap epochs
  osd_plb.add_u64_counter(l_osd_mape_dup, "map_message_epoch_dups"); // dup osdmap epochs

  logger = osd_plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(logger);
}

void OSD::suicide(int exitcode)
{
  if (g_conf->filestore_blackhole) {
    derr << " filestore_blackhole=true, doing abbreviated shutdown" << dendl;
    _exit(exitcode);
  }

  // turn off lockdep; the surviving threads tend to fight with exit() below
  g_lockdep = 0;

  derr << " pausing thread pools" << dendl;
  op_tp.pause();
  disk_tp.pause();
  recovery_tp.pause();
  command_tp.pause();

  derr << " flushing io" << dendl;
  store->sync_and_flush();

  derr << " removing pid file" << dendl;
  pidfile_remove();

  derr << " exit" << dendl;
  exit(exitcode);
}

int OSD::shutdown()
{
  if (!service.prepare_to_stop())
    return 0; // already shutting down
  osd_lock.Lock();
  if (is_stopping()) {
    osd_lock.Unlock();
    return 0;
  }
  derr << "shutdown" << dendl;

  heartbeat_lock.Lock();
  state = STATE_STOPPING;
  heartbeat_lock.Unlock();

  // Debugging
  g_ceph_context->_conf->set_val("debug_osd", "100");
  g_ceph_context->_conf->set_val("debug_journal", "100");
  g_ceph_context->_conf->set_val("debug_filestore", "100");
  g_ceph_context->_conf->set_val("debug_ms", "100");
  g_ceph_context->_conf->apply_changes(NULL);
  
  // Shutdown PGs
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       ++p) {
    dout(20) << " kicking pg " << p->first << dendl;
    p->second->lock();
    p->second->on_shutdown();
    p->second->kick();
    p->second->unlock();
    p->second->osr->flush();
  }
  
  // finish ops
  op_wq.drain(); // should already be empty except for lagard PGs
  {
    Mutex::Locker l(finished_lock);
    finished.clear(); // zap waiters (bleh, this is messy)
  }

  // unregister commands
  cct->get_admin_socket()->unregister_command("dump_ops_in_flight");
  cct->get_admin_socket()->unregister_command("dump_historic_ops");
  cct->get_admin_socket()->unregister_command("dump_op_pq_state");
  delete asok_hook;
  asok_hook = NULL;

  cct->get_admin_socket()->unregister_command("setomapval");
  cct->get_admin_socket()->unregister_command("rmomapkey");
  cct->get_admin_socket()->unregister_command("setomapheader");
  cct->get_admin_socket()->unregister_command("getomap");
  cct->get_admin_socket()->unregister_command("truncobj");
  cct->get_admin_socket()->unregister_command("injectdataerr");
  cct->get_admin_socket()->unregister_command("injectmdataerr");
  delete test_ops_hook;
  test_ops_hook = NULL;

  osd_lock.Unlock();

  heartbeat_lock.Lock();
  heartbeat_stop = true;
  heartbeat_cond.Signal();
  heartbeat_lock.Unlock();
  heartbeat_thread.join();

  recovery_tp.drain();
  recovery_tp.stop();
  dout(10) << "recovery tp stopped" << dendl;

  op_tp.drain();
  op_tp.stop();
  dout(10) << "op tp stopped" << dendl;

  command_tp.drain();
  command_tp.stop();
  dout(10) << "command tp stopped" << dendl;

  disk_tp.drain();
  disk_tp.stop();
  dout(10) << "disk tp paused (new), kicking all pgs" << dendl;

  osd_lock.Lock();

  reset_heartbeat_peers();

  tick_timer.shutdown();

  // note unmount epoch
  dout(10) << "noting clean unmount in epoch " << osdmap->get_epoch() << dendl;
  superblock.mounted = boot_epoch;
  superblock.clean_thru = osdmap->get_epoch();
  ObjectStore::Transaction t;
  write_superblock(t);
  int r = store->apply_transaction(t);
  if (r) {
    derr << "OSD::shutdown: error writing superblock: "
	 << cpp_strerror(r) << dendl;
  }

  dout(10) << "syncing store" << dendl;
  store->flush();
  store->sync();
  store->umount();
  delete store;
  store = 0;
  dout(10) << "Store synced" << dendl;

  {
    Mutex::Locker l(pg_stat_queue_lock);
    assert(pg_stat_queue.empty());
  }

  peering_wq.clear();
  // Remove PGs
#ifdef PG_DEBUG_REFS
  service.dump_live_pgids();
#endif
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       ++p) {
    dout(20) << " kicking pg " << p->first << dendl;
    p->second->lock();
    if (p->second->ref.read() != 1) {
      derr << "pgid " << p->first << " has ref count of "
	   << p->second->ref.read() << dendl;
      assert(0);
    }
    p->second->unlock();
    p->second->put("PGMap");
  }
  pg_map.clear();
#ifdef PG_DEBUG_REFS
  service.dump_live_pgids();
#endif
  g_conf->remove_observer(this);

  monc->shutdown();
  osd_lock.Unlock();

  osdmap = OSDMapRef();
  service.shutdown();
  op_tracker.on_shutdown();

  client_messenger->shutdown();
  cluster_messenger->shutdown();
  hbclient_messenger->shutdown();
  hb_front_server_messenger->shutdown();
  hb_back_server_messenger->shutdown();
  peering_wq.clear();
  return r;
}

void OSD::write_superblock(ObjectStore::Transaction& t)
{
  dout(10) << "write_superblock " << superblock << dendl;

  //hack: at minimum it's using the baseline feature set
  if (!superblock.compat_features.incompat.mask |
      CEPH_OSD_FEATURE_INCOMPAT_BASE.id)
    superblock.compat_features.incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);

  bufferlist bl;
  ::encode(superblock, bl);
  t.write(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
}

int OSD::read_superblock()
{
  bufferlist bl;
  int r = store->read(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, 0, bl);
  if (r < 0)
    return r;

  bufferlist::iterator p = bl.begin();
  ::decode(superblock, p);

  dout(10) << "read_superblock " << superblock << dendl;
  if (osd_compat.compare(superblock.compat_features) < 0) {
    derr << "The disk uses features unsupported by the executable." << dendl;
    derr << " ondisk features " << superblock.compat_features << dendl;
    derr << " daemon features " << osd_compat << dendl;

    if (osd_compat.writeable(superblock.compat_features)) {
      derr << "it is still writeable, though. Missing features:" << dendl;
      CompatSet diff = osd_compat.unsupported(superblock.compat_features);
      return -EOPNOTSUPP;
    }
    else {
      derr << "Cannot write to disk! Missing features:" << dendl;
      CompatSet diff = osd_compat.unsupported(superblock.compat_features);
      return -EOPNOTSUPP;
    }
  }

  if (whoami != superblock.whoami) {
    derr << "read_superblock superblock says osd." << superblock.whoami
         << ", but i (think i) am osd." << whoami << dendl;
    return -1;
  }
  
  return 0;
}



void OSD::recursive_remove_collection(ObjectStore *store, coll_t tmp)
{
  OSDriver driver(
    store,
    coll_t(),
    make_snapmapper_oid());
  SnapMapper mapper(&driver, 0, 0, 0);

  vector<hobject_t> objects;
  store->collection_list(tmp, objects);

  // delete them.
  ObjectStore::Transaction t;
  unsigned removed = 0;
  for (vector<hobject_t>::iterator p = objects.begin();
       p != objects.end();
       ++p, removed++) {
    OSDriver::OSTransaction _t(driver.get_transaction(&t));
    int r = mapper.remove_oid(*p, &_t);
    if (r != 0 && r != -ENOENT)
      assert(0);
    t.collection_remove(tmp, *p);
    if (removed > 300) {
      int r = store->apply_transaction(t);
      assert(r == 0);
      t = ObjectStore::Transaction();
      removed = 0;
    }
  }
  t.remove_collection(tmp);
  int r = store->apply_transaction(t);
  assert(r == 0);
  store->sync_and_flush();
}


// ======================================================
// PG's

PGPool OSD::_get_pool(int id, OSDMapRef createmap)
{
  if (!createmap->have_pg_pool(id)) {
    dout(5) << __func__ << ": the OSDmap does not contain a PG pool with id = "
	    << id << dendl;
    assert(0);
  }

  PGPool p = PGPool(id, createmap->get_pool_name(id),
		    createmap->get_pg_pool(id)->auid);
    
  const pg_pool_t *pi = createmap->get_pg_pool(id);
  p.info = *pi;
  p.snapc = pi->get_snap_context();

  pi->build_removed_snaps(p.cached_removed_snaps);
  dout(10) << "_get_pool " << p.id << dendl;
  return p;
}

PG *OSD::_open_lock_pg(
  OSDMapRef createmap,
  pg_t pgid, bool no_lockdep_check, bool hold_map_lock)
{
  assert(osd_lock.is_locked());

  PG* pg = _make_pg(createmap, pgid);

  pg_map[pgid] = pg;

  pg->lock(no_lockdep_check);
  pg->get("PGMap");  // because it's in pg_map
  return pg;
}

PG* OSD::_make_pg(
  OSDMapRef createmap,
  pg_t pgid)
{
  dout(10) << "_open_lock_pg " << pgid << dendl;
  PGPool pool = _get_pool(pgid.pool(), createmap);

  // create
  PG *pg;
  hobject_t logoid = make_pg_log_oid(pgid);
  hobject_t infooid = make_pg_biginfo_oid(pgid);
  if (osdmap->get_pg_type(pgid) == pg_pool_t::TYPE_REP)
    pg = new ReplicatedPG(&service, createmap, pool, pgid, logoid, infooid);
  else 
    assert(0);

  return pg;
}


void OSD::add_newly_split_pg(PG *pg, PG::RecoveryCtx *rctx)
{
  epoch_t e(service.get_osdmap()->get_epoch());
  pg->get("PGMap");  // For pg_map
  pg_map[pg->info.pgid] = pg;
  dout(10) << "Adding newly split pg " << *pg << dendl;
  vector<int> up, acting;
  pg->get_osdmap()->pg_to_up_acting_osds(pg->info.pgid, up, acting);
  int role = pg->get_osdmap()->calc_pg_role(service.whoami, acting);
  pg->set_role(role);
  pg->reg_next_scrub();
  pg->handle_loaded(rctx);
  pg->write_if_dirty(*(rctx->transaction));
  pg->queue_null(e, e);
  map<pg_t, list<PG::CephPeeringEvtRef> >::iterator to_wake =
    peering_wait_for_split.find(pg->info.pgid);
  if (to_wake != peering_wait_for_split.end()) {
    for (list<PG::CephPeeringEvtRef>::iterator i =
	   to_wake->second.begin();
	 i != to_wake->second.end();
	 ++i) {
      pg->queue_peering_event(*i);
    }
    peering_wait_for_split.erase(to_wake);
  }
  wake_pg_waiters(pg->info.pgid);
  if (!service.get_osdmap()->have_pg_pool(pg->info.pgid.pool()))
    _remove_pg(pg);
}

PG *OSD::_create_lock_pg(
  OSDMapRef createmap,
  pg_t pgid, bool newly_created, bool hold_map_lock,
  int role, vector<int>& up, vector<int>& acting, pg_history_t history,
  pg_interval_map_t& pi,
  ObjectStore::Transaction& t)
{
  assert(osd_lock.is_locked());
  dout(20) << "_create_lock_pg pgid " << pgid << dendl;

  PG *pg = _open_lock_pg(createmap, pgid, true, hold_map_lock);

  DeletingStateRef df = service.deleting_pgs.lookup(pgid);
  bool backfill = false;

  if (df && df->try_stop_deletion()) {
    dout(10) << __func__ << ": halted deletion on pg " << pgid << dendl;
    backfill = true;
    service.deleting_pgs.remove(pgid); // PG is no longer being removed!
  } else {
    if (df) {
      // raced, ensure we don't see DeletingStateRef when we try to
      // delete this pg
      service.deleting_pgs.remove(pgid);
    }
    // either it's not deleting, or we failed to get to it in time
    t.create_collection(coll_t(pgid));
  }

  pg->init(role, up, acting, history, pi, backfill, &t);

  dout(7) << "_create_lock_pg " << *pg << dendl;
  return pg;
}


bool OSD::_have_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  return pg_map.count(pgid);
}

PG *OSD::_lookup_lock_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  if (!pg_map.count(pgid))
    return NULL;
  PG *pg = pg_map[pgid];
  pg->lock();
  return pg;
}


PG *OSD::_lookup_pg(pg_t pgid)
{
  assert(osd_lock.is_locked());
  if (!pg_map.count(pgid))
    return NULL;
  PG *pg = pg_map[pgid];
  return pg;
}

PG *OSD::_lookup_lock_pg_with_map_lock_held(pg_t pgid)
{
  assert(osd_lock.is_locked());
  assert(pg_map.count(pgid));
  PG *pg = pg_map[pgid];
  pg->lock();
  return pg;
}

void OSD::load_pgs()
{
  assert(osd_lock.is_locked());
  dout(10) << "load_pgs" << dendl;
  assert(pg_map.empty());

  vector<coll_t> ls;
  int r = store->list_collections(ls);
  if (r < 0) {
    derr << "failed to list pgs: " << cpp_strerror(-r) << dendl;
  }

  set<pg_t> head_pgs;
  map<pg_t, interval_set<snapid_t> > pgs;
  for (vector<coll_t>::iterator it = ls.begin();
       it != ls.end();
       ++it) {
    pg_t pgid;
    snapid_t snap;
    uint64_t seq;

    if (it->is_temp(pgid) ||
	it->is_removal(&seq, &pgid)) {
      dout(10) << "load_pgs " << *it << " clearing temp" << dendl;
      recursive_remove_collection(store, *it);
      continue;
    }

    if (it->is_pg(pgid, snap)) {
      if (snap != CEPH_NOSNAP) {
	dout(10) << "load_pgs skipping snapped dir " << *it
		 << " (pg " << pgid << " snap " << snap << ")" << dendl;
	pgs[pgid].insert(snap);
      } else {
	pgs[pgid];
	head_pgs.insert(pgid);
      }
      continue;
    }

    dout(10) << "load_pgs ignoring unrecognized " << *it << dendl;
  }

  bool has_upgraded = false;
  for (map<pg_t, interval_set<snapid_t> >::iterator i = pgs.begin();
       i != pgs.end();
       ++i) {
    pg_t pgid(i->first);

    if (!head_pgs.count(pgid)) {
      dout(10) << __func__ << ": " << pgid << " has orphan snap collections " << i->second
	       << " with no head" << dendl;
      continue;
    }

    if (!osdmap->have_pg_pool(pgid.pool())) {
      dout(10) << __func__ << ": skipping PG " << pgid << " because we don't have pool "
	       << pgid.pool() << dendl;
      continue;
    }

    if (pgid.preferred() >= 0) {
      dout(10) << __func__ << ": skipping localized PG " << pgid << dendl;
      // FIXME: delete it too, eventually
      continue;
    }

    dout(10) << "pgid " << pgid << " coll " << coll_t(pgid) << dendl;
    bufferlist bl;
    epoch_t map_epoch = PG::peek_map_epoch(store, coll_t(pgid), service.infos_oid, &bl);

    PG *pg = _open_lock_pg(map_epoch == 0 ? osdmap : service.get_map(map_epoch), pgid);

    // read pg state, log
    pg->read_state(store, bl);

    if (pg->must_upgrade()) {
      if (!has_upgraded) {
	derr << "PGs are upgrading" << dendl;
	has_upgraded = true;
      }
      dout(10) << "PG " << pg->info.pgid
	       << " must upgrade..." << dendl;
      pg->upgrade(store, i->second);
    } else if (!i->second.empty()) {
      // handle upgrade bug
      for (interval_set<snapid_t>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	for (snapid_t k = j.get_start();
	     k != j.get_start() + j.get_len();
	     ++k) {
	  assert(store->collection_empty(coll_t(pgid, k)));
	  ObjectStore::Transaction t;
	  t.remove_collection(coll_t(pgid, k));
	  store->apply_transaction(t);
	}
      }
    }

    if (!pg->snap_collections.empty()) {
      pg->snap_collections.clear();
      pg->dirty_big_info = true;
      pg->dirty_info = true;
      ObjectStore::Transaction t;
      pg->write_if_dirty(t);
      store->apply_transaction(t);
    }

    service.init_splits_between(pg->info.pgid, pg->get_osdmap(), osdmap);

    pg->reg_next_scrub();

    // generate state for PG's current mapping
    pg->get_osdmap()->pg_to_up_acting_osds(pgid, pg->up, pg->acting);
    int role = pg->get_osdmap()->calc_pg_role(whoami, pg->acting);
    pg->set_role(role);

    PG::RecoveryCtx rctx(0, 0, 0, 0, 0, 0);
    pg->handle_loaded(&rctx);

    dout(10) << "load_pgs loaded " << *pg << " " << pg->pg_log.get_log() << dendl;
    pg->unlock();
  }
  dout(10) << "load_pgs done" << dendl;
  
  build_past_intervals_parallel();
}


/*
 * build past_intervals efficiently on old, degraded, and buried
 * clusters.  this is important for efficiently catching up osds that
 * are way behind on maps to the current cluster state.
 *
 * this is a parallel version of PG::generate_past_intervals().
 * follow the same logic, but do all pgs at the same time so that we
 * can make a single pass across the osdmap history.
 */
struct pistate {
  epoch_t start, end;
  vector<int> old_acting, old_up;
  epoch_t same_interval_since;
};

void OSD::build_past_intervals_parallel()
{
  map<PG*,pistate> pis;

  // calculate untion of map range
  epoch_t end_epoch = superblock.oldest_map;
  epoch_t cur_epoch = superblock.newest_map;
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       ++i) {
    PG *pg = i->second;

    epoch_t start, end;
    if (!pg->_calc_past_interval_range(&start, &end))
      continue;

    dout(10) << pg->info.pgid << " needs " << start << "-" << end << dendl;
    pistate& p = pis[pg];
    p.start = start;
    p.end = end;
    p.same_interval_since = 0;

    if (start < cur_epoch)
      cur_epoch = start;
    if (end > end_epoch)
      end_epoch = end;
  }
  if (pis.empty()) {
    dout(10) << __func__ << " nothing to build" << dendl;
    return;
  }

  dout(1) << __func__ << " over " << cur_epoch << "-" << end_epoch << dendl;
  assert(cur_epoch <= end_epoch);

  OSDMapRef cur_map, last_map;
  for ( ; cur_epoch <= end_epoch; cur_epoch++) {
    dout(10) << __func__ << " epoch " << cur_epoch << dendl;
    last_map = cur_map;
    cur_map = get_map(cur_epoch);

    for (map<PG*,pistate>::iterator i = pis.begin(); i != pis.end(); ++i) {
      PG *pg = i->first;
      pistate& p = i->second;

      if (cur_epoch < p.start || cur_epoch > p.end)
	continue;

      vector<int> acting, up;
      cur_map->pg_to_up_acting_osds(pg->info.pgid, up, acting);

      if (p.same_interval_since == 0) {
	dout(10) << __func__ << " epoch " << cur_epoch << " pg " << pg->info.pgid
		 << " first map, acting " << acting
		 << " up " << up << ", same_interval_since = " << cur_epoch << dendl;
	p.same_interval_since = cur_epoch;
	p.old_up = up;
	p.old_acting = acting;
	continue;
      }
      assert(last_map);

      std::stringstream debug;
      bool new_interval = pg_interval_t::check_new_interval(p.old_acting, acting,
							    p.old_up, up,
							    p.same_interval_since,
							    pg->info.history.last_epoch_clean,
							    cur_map, last_map,
							    pg->info.pgid.pool(),
	                                                    pg->info.pgid,
							    &pg->past_intervals,
							    &debug);
      if (new_interval) {
	dout(10) << __func__ << " epoch " << cur_epoch << " pg " << pg->info.pgid
		 << " " << debug.str() << dendl;
	p.old_up = up;
	p.old_acting = acting;
	p.same_interval_since = cur_epoch;
      }
    }
  }

  // write info only at the end.  this is necessary because we check
  // whether the past_intervals go far enough back or forward in time,
  // but we don't check for holes.  we could avoid it by discarding
  // the previous past_intervals and rebuilding from scratch, or we
  // can just do this and commit all our work at the end.
  ObjectStore::Transaction t;
  int num = 0;
  for (map<PG*,pistate>::iterator i = pis.begin(); i != pis.end(); ++i) {
    PG *pg = i->first;
    pg->lock();
    pg->dirty_big_info = true;
    pg->dirty_info = true;
    pg->write_if_dirty(t);
    pg->unlock();

    // don't let the transaction get too big
    if (++num >= g_conf->osd_target_transaction_size) {
      store->apply_transaction(t);
      t = ObjectStore::Transaction();
      num = 0;
    }
  }
  if (!t.empty())
    store->apply_transaction(t);
}

/*
 * look up a pg.  if we have it, great.  if not, consider creating it IF the pg mapping
 * hasn't changed since the given epoch and we are the primary.
 */
PG *OSD::get_or_create_pg(
  const pg_info_t& info, pg_interval_map_t& pi,
  epoch_t epoch, int from, int& created, bool primary)
{
  PG *pg;

  if (!_have_pg(info.pgid)) {
    // same primary?
    if (!osdmap->have_pg_pool(info.pgid.pool()))
      return 0;
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(info.pgid, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    pg_history_t history = info.history;
    project_pg_history(info.pgid, history, epoch, up, acting);

    if (epoch < history.same_interval_since) {
      dout(10) << "get_or_create_pg " << info.pgid << " acting changed in "
	       << history.same_interval_since << " (msg from " << epoch << ")" << dendl;
      return NULL;
    }

    if (service.splitting(info.pgid)) {
      assert(0);
    }

    bool create = false;
    if (primary) {
      assert(role == 0);  // otherwise, probably bug in project_pg_history.

      // DNE on source?
      if (info.dne()) {
	// is there a creation pending on this pg?
	if (creating_pgs.count(info.pgid)) {
	  creating_pgs[info.pgid].prior.erase(from);
	  if (!can_create_pg(info.pgid))
	    return NULL;
	  history = creating_pgs[info.pgid].history;
	  create = true;
	} else {
	  dout(10) << "get_or_create_pg " << info.pgid
		   << " DNE on source, but creation probe, ignoring" << dendl;
	  return NULL;
	}
      }
      creating_pgs.erase(info.pgid);
    } else {
      assert(role != 0);    // i should be replica
      assert(!info.dne());  // and pg exists if we are hearing about it
    }

    // ok, create PG locally using provided Info and History
    PG::RecoveryCtx rctx = create_context();
    pg = _create_lock_pg(
      get_map(epoch),
      info.pgid, create, false, role, up, acting, history, pi,
      *rctx.transaction);
    pg->handle_create(&rctx);
    pg->write_if_dirty(*rctx.transaction);
    dispatch_context(rctx, pg, osdmap);
      
    created++;
    dout(10) << *pg << " is new" << dendl;

    // kick any waiters
    wake_pg_waiters(pg->info.pgid);

  } else {
    // already had it.  did the mapping change?
    pg = _lookup_lock_pg(info.pgid);
    if (epoch < pg->info.history.same_interval_since) {
      dout(10) << *pg << " get_or_create_pg acting changed in "
	       << pg->info.history.same_interval_since
	       << " (msg from " << epoch << ")" << dendl;
      pg->unlock();
      return NULL;
    }
  }
  return pg;
}


/*
 * calculate prior pg members during an epoch interval [start,end)
 *  - from each epoch, include all osds up then AND now
 *  - if no osds from then are up now, include them all, even tho they're not reachable now
 */
void OSD::calc_priors_during(pg_t pgid, epoch_t start, epoch_t end, set<int>& pset)
{
  dout(15) << "calc_priors_during " << pgid << " [" << start << "," << end << ")" << dendl;
  
  for (epoch_t e = start; e < end; e++) {
    OSDMapRef oldmap = get_map(e);
    vector<int> acting;
    oldmap->pg_to_acting_osds(pgid, acting);
    dout(20) << "  " << pgid << " in epoch " << e << " was " << acting << dendl;
    int up = 0;
    for (unsigned i=0; i<acting.size(); i++)
      if (osdmap->is_up(acting[i])) {
	if (acting[i] != whoami)
	  pset.insert(acting[i]);
	up++;
      }
    if (!up && !acting.empty()) {
      // sucky.  add down osds, even tho we can't reach them right now.
      for (unsigned i=0; i<acting.size(); i++)
	if (acting[i] != whoami)
	  pset.insert(acting[i]);
    }
  }
  dout(10) << "calc_priors_during " << pgid
	   << " [" << start << "," << end 
	   << ") = " << pset << dendl;
}


/**
 * Fill in the passed history so you know same_interval_since, same_up_since,
 * and same_primary_since.
 */
void OSD::project_pg_history(pg_t pgid, pg_history_t& h, epoch_t from,
			     const vector<int>& currentup,
			     const vector<int>& currentacting)
{
  dout(15) << "project_pg_history " << pgid
           << " from " << from << " to " << osdmap->get_epoch()
           << ", start " << h
           << dendl;

  epoch_t e;
  for (e = osdmap->get_epoch();
       e > from;
       e--) {
    // verify during intermediate epoch (e-1)
    OSDMapRef oldmap = get_map(e-1);
    assert(oldmap->have_pg_pool(pgid.pool()));

    vector<int> up, acting;
    oldmap->pg_to_up_acting_osds(pgid, up, acting);

    // acting set change?
    if ((acting != currentacting || up != currentup) && e > h.same_interval_since) {
      dout(15) << "project_pg_history " << pgid << " acting|up changed in " << e 
	       << " from " << acting << "/" << up
	       << " -> " << currentacting << "/" << currentup
	       << dendl;
      h.same_interval_since = e;
    }
    // split?
    if (pgid.is_split(oldmap->get_pg_num(pgid.pool()),
		      osdmap->get_pg_num(pgid.pool()),
		      0)) {
      h.same_interval_since = e;
    }
    // up set change?
    if (up != currentup && e > h.same_up_since) {
      dout(15) << "project_pg_history " << pgid << " up changed in " << e 
                << " from " << up << " -> " << currentup << dendl;
      h.same_up_since = e;
    }

    // primary change?
    if (!(!acting.empty() && !currentacting.empty() && acting[0] == currentacting[0]) &&
        e > h.same_primary_since) {
      dout(15) << "project_pg_history " << pgid << " primary changed in " << e << dendl;
      h.same_primary_since = e;
    }

    if (h.same_interval_since >= e && h.same_up_since >= e && h.same_primary_since >= e)
      break;
  }

  // base case: these floors should be the creation epoch if we didn't
  // find any changes.
  if (e == h.epoch_created) {
    if (!h.same_interval_since)
      h.same_interval_since = e;
    if (!h.same_up_since)
      h.same_up_since = e;
    if (!h.same_primary_since)
      h.same_primary_since = e;
  }

  dout(15) << "project_pg_history end " << h << dendl;
}

// -------------------------------------

float OSDService::get_full_ratio()
{
  float full_ratio = g_conf->osd_failsafe_full_ratio;
  if (full_ratio > 1.0) full_ratio /= 100.0;
  return full_ratio;
}

float OSDService::get_nearfull_ratio()
{
  float nearfull_ratio = g_conf->osd_failsafe_nearfull_ratio;
  if (nearfull_ratio > 1.0) nearfull_ratio /= 100.0;
  return nearfull_ratio;
}

void OSDService::check_nearfull_warning(const osd_stat_t &osd_stat)
{
  Mutex::Locker l(full_status_lock);
  enum s_names new_state;

  time_t now = ceph_clock_gettime(NULL);

  float ratio = ((float)osd_stat.kb_used) / ((float)osd_stat.kb);
  float nearfull_ratio = get_nearfull_ratio();
  float full_ratio = get_full_ratio();
  cur_ratio = ratio;

  if (full_ratio > 0 && ratio > full_ratio) {
    new_state = FULL;
  } else if (nearfull_ratio > 0 && ratio > nearfull_ratio) {
    new_state = NEAR;
  } else {
    cur_state = NONE;
    return;
  }

  if (cur_state != new_state) {
    cur_state = new_state;
  } else if (now - last_msg < g_conf->osd_op_complaint_time) {
    return;
  }
  last_msg = now;
  if (cur_state == FULL)
    clog.error() << "OSD full dropping all updates " << (int)(ratio * 100) << "% full";
  else
    clog.warn() << "OSD near full (" << (int)(ratio * 100) << "%)";
}

bool OSDService::check_failsafe_full()
{
  Mutex::Locker l(full_status_lock);
  if (cur_state == FULL)
    return true;
  return false;
}

bool OSDService::too_full_for_backfill(double *_ratio, double *_max_ratio)
{
  Mutex::Locker l(full_status_lock);
  double max_ratio;
  max_ratio = g_conf->osd_backfill_full_ratio;
  if (_ratio)
    *_ratio = cur_ratio;
  if (_max_ratio)
    *_max_ratio = max_ratio;
  return cur_ratio >= max_ratio;
}


void OSD::update_osd_stat()
{
  // fill in osd stats too
  struct statfs stbuf;
  store->statfs(&stbuf);

  osd_stat.kb = stbuf.f_blocks * stbuf.f_bsize / 1024;
  osd_stat.kb_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize / 1024;
  osd_stat.kb_avail = stbuf.f_bavail * stbuf.f_bsize / 1024;

  osd_stat.hb_in.clear();
  for (map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin(); p != heartbeat_peers.end(); ++p)
    osd_stat.hb_in.push_back(p->first);
  osd_stat.hb_out.clear();

  service.check_nearfull_warning(osd_stat);

  dout(20) << "update_osd_stat " << osd_stat << dendl;
}

void OSD::_add_heartbeat_peer(int p)
{
  if (p == whoami)
    return;
  HeartbeatInfo *hi;

  map<int,HeartbeatInfo>::iterator i = heartbeat_peers.find(p);
  if (i == heartbeat_peers.end()) {
    pair<ConnectionRef,ConnectionRef> cons = service.get_con_osd_hb(p, osdmap->get_epoch());
    if (!cons.first)
      return;
    hi = &heartbeat_peers[p];
    hi->peer = p;
    HeartbeatSession *s = new HeartbeatSession(p);
    hi->con_back = cons.first.get();
    hi->con_back->get();
    hi->con_back->set_priv(s);
    if (cons.second) {
      hi->con_front = cons.second.get();
      hi->con_front->get();
      hi->con_front->set_priv(s->get());
    }
    dout(10) << "_add_heartbeat_peer: new peer osd." << p
	     << " " << hi->con_back->get_peer_addr()
	     << " " << (hi->con_front ? hi->con_front->get_peer_addr() : entity_addr_t())
	     << dendl;
  } else {
    hi = &i->second;
  }
  hi->epoch = osdmap->get_epoch();
}

void OSD::_remove_heartbeat_peer(int n)
{
  map<int,HeartbeatInfo>::iterator q = heartbeat_peers.find(n);
  assert(q != heartbeat_peers.end());
  dout(20) << " removing heartbeat peer osd." << n
	   << " " << q->second.con_back->get_peer_addr()
	   << " " << (q->second.con_front ? q->second.con_front->get_peer_addr() : entity_addr_t())
	   << dendl;
  hbclient_messenger->mark_down(q->second.con_back);
  q->second.con_back->put();
  if (q->second.con_front) {
    hbclient_messenger->mark_down(q->second.con_front);
    q->second.con_front->put();
  }
  heartbeat_peers.erase(q);
}

void OSD::need_heartbeat_peer_update()
{
  Mutex::Locker l(heartbeat_lock);
  if (is_stopping())
    return;
  dout(20) << "need_heartbeat_peer_update" << dendl;
  heartbeat_need_update = true;
}

void OSD::maybe_update_heartbeat_peers()
{
  assert(osd_lock.is_locked());

  if (is_waiting_for_healthy()) {
    utime_t now = ceph_clock_now(g_ceph_context);
    if (last_heartbeat_resample == utime_t()) {
      last_heartbeat_resample = now;
      heartbeat_need_update = true;
    } else if (!heartbeat_need_update) {
      utime_t dur = now - last_heartbeat_resample;
      if (dur > g_conf->osd_heartbeat_grace) {
	dout(10) << "maybe_update_heartbeat_peers forcing update after " << dur << " seconds" << dendl;
	heartbeat_need_update = true;
	last_heartbeat_resample = now;
	reset_heartbeat_peers();   // we want *new* peers!
      }
    }
  }

  Mutex::Locker l(heartbeat_lock);
  if (!heartbeat_need_update)
    return;
  heartbeat_need_update = false;

  dout(10) << "maybe_update_heartbeat_peers updating" << dendl;

  heartbeat_epoch = osdmap->get_epoch();

  // build heartbeat from set
  if (is_active()) {
    for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
	 i != pg_map.end();
	 ++i) {
      PG *pg = i->second;
      pg->heartbeat_peer_lock.Lock();
      dout(20) << i->first << " heartbeat_peers " << pg->heartbeat_peers << dendl;
      for (set<int>::iterator p = pg->heartbeat_peers.begin();
	   p != pg->heartbeat_peers.end();
	   ++p)
	if (osdmap->is_up(*p))
	  _add_heartbeat_peer(*p);
      for (set<int>::iterator p = pg->probe_targets.begin();
	   p != pg->probe_targets.end();
	   ++p)
	if (osdmap->is_up(*p))
	  _add_heartbeat_peer(*p);
      pg->heartbeat_peer_lock.Unlock();
    }
  }

  // include next and previous up osds to ensure we have a fully-connected set
  set<int> want, extras;
  int next = osdmap->get_next_up_osd_after(whoami);
  if (next >= 0)
    want.insert(next);
  int prev = osdmap->get_previous_up_osd_before(whoami);
  if (prev >= 0)
    want.insert(prev);

  for (set<int>::iterator p = want.begin(); p != want.end(); ++p) {
    dout(10) << " adding neighbor peer osd." << *p << dendl;
    extras.insert(*p);
    _add_heartbeat_peer(*p);
  }

  // remove down peers; enumerate extras
  map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin();
  while (p != heartbeat_peers.end()) {
    if (!osdmap->is_up(p->first)) {
      int o = p->first;
      ++p;
      _remove_heartbeat_peer(o);
      continue;
    }
    if (p->second.epoch < osdmap->get_epoch()) {
      extras.insert(p->first);
    }
    ++p;
  }

  // too few?
  int start = osdmap->get_next_up_osd_after(whoami);
  for (int n = start; n >= 0; ) {
    if ((int)heartbeat_peers.size() >= g_conf->osd_heartbeat_min_peers)
      break;
    if (!extras.count(n) && !want.count(n) && n != whoami) {
      dout(10) << " adding random peer osd." << n << dendl;
      extras.insert(n);
      _add_heartbeat_peer(n);
    }
    n = osdmap->get_next_up_osd_after(n);
    if (n == start)
      break;  // came full circle; stop
  }

  // too many?
  for (set<int>::iterator p = extras.begin();
       (int)heartbeat_peers.size() > g_conf->osd_heartbeat_min_peers && p != extras.end();
       ++p) {
    if (want.count(*p))
      continue;
    _remove_heartbeat_peer(*p);
  }

  dout(10) << "maybe_update_heartbeat_peers " << heartbeat_peers.size() << " peers, extras " << extras << dendl;
}

void OSD::reset_heartbeat_peers()
{
  assert(osd_lock.is_locked());
  dout(10) << "reset_heartbeat_peers" << dendl;
  Mutex::Locker l(heartbeat_lock);
  while (!heartbeat_peers.empty()) {
    HeartbeatInfo& hi = heartbeat_peers.begin()->second;
    hbclient_messenger->mark_down(hi.con_back);
    hi.con_back->put();
    if (hi.con_front) {
      hbclient_messenger->mark_down(hi.con_front);
      hi.con_front->put();
    }
    heartbeat_peers.erase(heartbeat_peers.begin());
  }
  failure_queue.clear();
}

void OSD::handle_osd_ping(MOSDPing *m)
{
  if (superblock.cluster_fsid != m->fsid) {
    dout(20) << "handle_osd_ping from " << m->get_source_inst()
	     << " bad fsid " << m->fsid << " != " << superblock.cluster_fsid << dendl;
    m->put();
    return;
  }

  int from = m->get_source().num();

  heartbeat_lock.Lock();
  if (is_stopping()) {
    heartbeat_lock.Unlock();
    return;
  }

  OSDMapRef curmap = service.get_osdmap();
  
  switch (m->op) {

  case MOSDPing::PING:
    {
      if (g_conf->osd_debug_drop_ping_probability > 0) {
	if (debug_heartbeat_drops_remaining.count(from)) {
	  if (debug_heartbeat_drops_remaining[from] == 0) {
	    debug_heartbeat_drops_remaining.erase(from);
	  } else {
	    debug_heartbeat_drops_remaining[from]--;
	    dout(5) << "Dropping heartbeat from " << from
		    << ", " << debug_heartbeat_drops_remaining[from]
		    << " remaining to drop" << dendl;
	    break;
	  }
	} else if (g_conf->osd_debug_drop_ping_probability >
	           ((((double)(rand()%100))/100.0))) {
	  debug_heartbeat_drops_remaining[from] =
	    g_conf->osd_debug_drop_ping_duration;
	  dout(5) << "Dropping heartbeat from " << from
		  << ", " << debug_heartbeat_drops_remaining[from]
		  << " remaining to drop" << dendl;
	  break;
	}
      }

      if (!g_ceph_context->get_heartbeat_map()->is_healthy()) {
	dout(10) << "internal heartbeat not healthy, dropping ping request" << dendl;
	break;
      }

      Message *r = new MOSDPing(monc->get_fsid(),
				curmap->get_epoch(),
				MOSDPing::PING_REPLY,
				m->stamp);
      m->get_connection()->get_messenger()->send_message(r, m->get_connection());

      if (curmap->is_up(from)) {
	note_peer_epoch(from, m->map_epoch);
	if (is_active()) {
	  ConnectionRef con = service.get_con_osd_cluster(from, curmap->get_epoch());
	  if (con) {
	    _share_map_outgoing(from, con.get());
	  }
	}
      } else if (!curmap->exists(from) ||
		 curmap->get_down_at(from) > m->map_epoch) {
	// tell them they have died
	Message *r = new MOSDPing(monc->get_fsid(),
				  curmap->get_epoch(),
				  MOSDPing::YOU_DIED,
				  m->stamp);
	m->get_connection()->get_messenger()->send_message(r, m->get_connection());
      }
    }
    break;

  case MOSDPing::PING_REPLY:
    {
      map<int,HeartbeatInfo>::iterator i = heartbeat_peers.find(from);
      if (i != heartbeat_peers.end()) {
	if (m->get_connection() == i->second.con_back) {
	  dout(25) << "handle_osd_ping got reply from osd." << from
		   << " first_rx " << i->second.first_tx
		   << " last_tx " << i->second.last_tx
		   << " last_rx_back " << i->second.last_rx_back << " -> " << m->stamp
		   << " last_rx_front " << i->second.last_rx_front
		   << dendl;
	  i->second.last_rx_back = m->stamp;
	  // if there is no front con, set both stamps.
	  if (i->second.con_front == NULL)
	    i->second.last_rx_front = m->stamp;
	} else if (m->get_connection() == i->second.con_front) {
	  dout(25) << "handle_osd_ping got reply from osd." << from
		   << " first_rx " << i->second.first_tx
		   << " last_tx " << i->second.last_tx
		   << " last_rx_back " << i->second.last_rx_back
		   << " last_rx_front " << i->second.last_rx_front << " -> " << m->stamp
		   << dendl;
	  i->second.last_rx_front = m->stamp;
	}
      }

      if (m->map_epoch &&
	  curmap->is_up(from)) {
	note_peer_epoch(from, m->map_epoch);
	if (is_active()) {
	  ConnectionRef con = service.get_con_osd_cluster(from, curmap->get_epoch());
	  if (con) {
	    _share_map_outgoing(from, con.get());
	  }
	}
      }

      utime_t cutoff = ceph_clock_now(g_ceph_context);
      cutoff -= g_conf->osd_heartbeat_grace;
      if (i->second.is_healthy(cutoff)) {
	// Cancel false reports
	if (failure_queue.count(from)) {
	  dout(10) << "handle_osd_ping canceling queued failure report for osd." << from<< dendl;
	  failure_queue.erase(from);
	}
	if (failure_pending.count(from)) {
	  dout(10) << "handle_osd_ping canceling in-flight failure report for osd." << from<< dendl;
	  send_still_alive(curmap->get_epoch(), failure_pending[from]);
	  failure_pending.erase(from);
	}
      }
    }
    break;

  case MOSDPing::YOU_DIED:
    dout(10) << "handle_osd_ping " << m->get_source_inst() << " says i am down in " << m->map_epoch
	     << dendl;
    if (monc->sub_want("osdmap", m->map_epoch, CEPH_SUBSCRIBE_ONETIME))
      monc->renew_subs();
    break;
  }

  heartbeat_lock.Unlock();
  m->put();
}

void OSD::heartbeat_entry()
{
  Mutex::Locker l(heartbeat_lock);
  if (is_stopping())
    return;
  while (!heartbeat_stop) {
    heartbeat();

    double wait = .5 + ((float)(rand() % 10)/10.0) * (float)g_conf->osd_heartbeat_interval;
    utime_t w;
    w.set_from_double(wait);
    dout(30) << "heartbeat_entry sleeping for " << wait << dendl;
    heartbeat_cond.WaitInterval(g_ceph_context, heartbeat_lock, w);
    if (is_stopping())
      return;
    dout(30) << "heartbeat_entry woke up" << dendl;
  }
}

void OSD::heartbeat_check()
{
  assert(heartbeat_lock.is_locked());
  utime_t now = ceph_clock_now(g_ceph_context);
  double age = hbclient_messenger->get_dispatch_queue_max_age(now);
  if (age > (g_conf->osd_heartbeat_grace / 2)) {
    derr << "skipping heartbeat_check, hbqueue max age: " << age << dendl;
    return; // hb dispatch is too backed up for our hb status to be meaningful
  }

  // check for incoming heartbeats (move me elsewhere?)
  utime_t cutoff = now;
  cutoff -= g_conf->osd_heartbeat_grace;
  for (map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin();
       p != heartbeat_peers.end();
       ++p) {
    dout(25) << "heartbeat_check osd." << p->first
	     << " first_tx " << p->second.first_tx
	     << " last_tx " << p->second.last_tx
	     << " last_rx_back " << p->second.last_rx_back
	     << " last_rx_front " << p->second.last_rx_front
	     << dendl;
    if (p->second.is_unhealthy(cutoff)) {
      if (p->second.last_rx_back == utime_t() ||
	  p->second.last_rx_front == utime_t()) {
	derr << "heartbeat_check: no reply from osd." << p->first
	     << " ever on either front or back, first ping sent " << p->second.first_tx
	     << " (cutoff " << cutoff << ")" << dendl;
	// fail
	failure_queue[p->first] = p->second.last_tx;
      } else {
	derr << "heartbeat_check: no reply from osd." << p->first
	     << " since back " << p->second.last_rx_back
	     << " front " << p->second.last_rx_front
	     << " (cutoff " << cutoff << ")" << dendl;
	// fail
	failure_queue[p->first] = MIN(p->second.last_rx_back, p->second.last_rx_front);
      }
    }
  }
}

void OSD::heartbeat()
{
  dout(30) << "heartbeat" << dendl;

  // get CPU load avg
  double loadavgs[1];
  if (getloadavg(loadavgs, 1) == 1)
    logger->set(l_osd_loadavg, 100 * loadavgs[0]);

  dout(30) << "heartbeat checking stats" << dendl;

  // refresh stats?
  {
    Mutex::Locker lock(stat_lock);
    update_osd_stat();
  }

  dout(5) << "heartbeat: " << osd_stat << dendl;

  utime_t now = ceph_clock_now(g_ceph_context);

  // send heartbeats
  for (map<int,HeartbeatInfo>::iterator i = heartbeat_peers.begin();
       i != heartbeat_peers.end();
       ++i) {
    int peer = i->first;
    i->second.last_tx = now;
    if (i->second.first_tx == utime_t())
      i->second.first_tx = now;
    dout(30) << "heartbeat sending ping to osd." << peer << dendl;
    hbclient_messenger->send_message(new MOSDPing(monc->get_fsid(),
						  service.get_osdmap()->get_epoch(),
						  MOSDPing::PING,
						  now),
				     i->second.con_back);
    if (i->second.con_front)
      hbclient_messenger->send_message(new MOSDPing(monc->get_fsid(),
						    service.get_osdmap()->get_epoch(),
						    MOSDPing::PING,
						    now),
				       i->second.con_front);
  }

  dout(30) << "heartbeat check" << dendl;
  heartbeat_check();

  logger->set(l_osd_hb_to, heartbeat_peers.size());
  logger->set(l_osd_hb_from, 0);
  
  // hmm.. am i all alone?
  dout(30) << "heartbeat lonely?" << dendl;
  if (heartbeat_peers.empty()) {
    if (now - last_mon_heartbeat > g_conf->osd_mon_heartbeat_interval && is_active()) {
      last_mon_heartbeat = now;
      dout(10) << "i have no heartbeat peers; checking mon for new map" << dendl;
      monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
      monc->renew_subs();
    }
  }

  dout(30) << "heartbeat done" << dendl;
}

bool OSD::heartbeat_reset(Connection *con)
{
  HeartbeatSession *s = static_cast<HeartbeatSession*>(con->get_priv());
  if (s) {
    heartbeat_lock.Lock();
    if (is_stopping()) {
      heartbeat_lock.Unlock();
      return true;
    }
    map<int,HeartbeatInfo>::iterator p = heartbeat_peers.find(s->peer);
    if (p != heartbeat_peers.end() &&
	(p->second.con_back == con ||
	 p->second.con_front == con)) {
      dout(10) << "heartbeat_reset failed hb con " << con << " for osd." << p->second.peer
	       << ", reopening" << dendl;
      if (con != p->second.con_back) {
	hbclient_messenger->mark_down(p->second.con_back);
	p->second.con_back->put();
      }
      p->second.con_back = NULL;
      if (p->second.con_front && con != p->second.con_front) {
	hbclient_messenger->mark_down(p->second.con_front);
	p->second.con_front->put();
      }
      p->second.con_front = NULL;
      pair<ConnectionRef,ConnectionRef> newcon = service.get_con_osd_hb(p->second.peer, p->second.epoch);
      if (newcon.first) {
	p->second.con_back = newcon.first.get();
	p->second.con_back->get();
	p->second.con_back->set_priv(s);
	if (newcon.second) {
	  p->second.con_front = newcon.second.get();
	  p->second.con_front->get();
	  p->second.con_front->set_priv(s->get());
	}
      } else {
	dout(10) << "heartbeat_reset failed hb con " << con << " for osd." << p->second.peer
		 << ", raced with osdmap update, closing out peer" << dendl;
	heartbeat_peers.erase(p);
      }
    } else {
      dout(10) << "heartbeat_reset closing (old) failed hb con " << con << dendl;
    }
    heartbeat_lock.Unlock();
    s->put();
  }
  return true;
}



// =========================================

void OSD::tick()
{
  assert(osd_lock.is_locked());
  dout(5) << "tick" << dendl;

  logger->set(l_osd_buf, buffer::get_total_alloc());

  if (is_active() || is_waiting_for_healthy()) {
    map_lock.get_read();

    maybe_update_heartbeat_peers();

    heartbeat_lock.Lock();
    heartbeat_check();
    heartbeat_lock.Unlock();

    // mon report?
    utime_t now = ceph_clock_now(g_ceph_context);
    if (outstanding_pg_stats &&
	(now - g_conf->osd_mon_ack_timeout) > last_pg_stats_ack) {
      dout(1) << "mon hasn't acked PGStats in " << now - last_pg_stats_ack
	      << " seconds, reconnecting elsewhere" << dendl;
      monc->reopen_session();
      last_pg_stats_ack = ceph_clock_now(g_ceph_context);  // reset clock
      last_pg_stats_sent = utime_t();
    }
    if (now - last_pg_stats_sent > g_conf->osd_mon_report_interval_max) {
      osd_stat_updated = true;
      do_mon_report();
    } else if (now - last_mon_report > g_conf->osd_mon_report_interval_min) {
      do_mon_report();
    }

    map_lock.put_read();
  }

  if (is_waiting_for_healthy()) {
    if (_is_healthy()) {
      dout(1) << "healthy again, booting" << dendl;
      state = STATE_BOOTING;
      start_boot();
    }
  }

  if (is_active()) {
    // periodically kick recovery work queue
    recovery_tp.wake();

    if (!scrub_random_backoff()) {
      sched_scrub();
    }

    check_replay_queue();
  }

  // only do waiters if dispatch() isn't currently running.  (if it is,
  // it'll do the waiters, and doing them here may screw up ordering
  // of op_queue vs handle_osd_map.)
  if (!dispatch_running) {
    dispatch_running = true;
    do_waiters();
    dispatch_running = false;
    dispatch_cond.Signal();
  }

  check_ops_in_flight();

  tick_timer.add_event_after(1.0, new C_Tick(this));
}

void OSD::check_ops_in_flight()
{
  vector<string> warnings;
  if (op_tracker.check_ops_in_flight(warnings)) {
    for (vector<string>::iterator i = warnings.begin();
        i != warnings.end();
        ++i) {
      clog.warn() << *i;
    }
  }
  return;
}

// Usage:
//   setomapval <pool-id> <obj-name> <key> <val>
//   rmomapkey <pool-id> <obj-name> <key>
//   setomapheader <pool-id> <obj-name> <header>
//   truncobj <pool-id> <obj-name> <newlen>
void TestOpsSocketHook::test_ops(OSDService *service, ObjectStore *store,
     std::string command, std::string args, ostream &ss)
{
  //Test support
  //Support changing the omap on a single osd by using the Admin Socket to
  //directly request the osd make a change.
  if (command == "setomapval" || command == "rmomapkey" ||
      command == "setomapheader" || command == "getomap" ||
      command == "truncobj" || command == "injectmdataerr" ||
      command == "injectdataerr"
    ) {
    std::vector<std::string> argv;
    pg_t rawpg, pgid;
    int64_t pool;
    OSDMapRef curmap = service->get_osdmap();
    int r;

    argv.push_back(command);
    string_to_vec(argv, args);
    int argc = argv.size();

    if (argc < 3) {
      ss << "Illegal request";
      return;
    }
 
    pool = curmap->const_lookup_pg_pool_name(argv[1].c_str());
    //If we can't find it my name then maybe id specified
    if (pool < 0 && isdigit(argv[1].c_str()[0]))
      pool = atoll(argv[1].c_str());
    r = -1;
    if (pool >= 0)
        r = curmap->object_locator_to_pg(object_t(argv[2]),
          object_locator_t(pool), rawpg);
    if (r < 0) {
        ss << "Invalid pool " << argv[1];
        return;
    }
    pgid = curmap->raw_pg_to_pg(rawpg);

    hobject_t obj(object_t(argv[2]), string(""), CEPH_NOSNAP, rawpg.ps(), pool);
    ObjectStore::Transaction t;

    if (command == "setomapval") {
      if (argc != 5) {
        ss << "usage: setomapval <pool> <obj-name> <key> <val>";
        return;
      }
      map<string, bufferlist> newattrs;
      bufferlist val;
      string key(argv[3]);
 
      val.append(argv[4]);
      newattrs[key] = val;
      t.omap_setkeys(coll_t(pgid), obj, newattrs);
      r = store->apply_transaction(t);
      if (r < 0)
        ss << "error=" << r;
      else
        ss << "ok";
    } else if (command == "rmomapkey") {
      if (argc != 4) {
        ss << "usage: rmomapkey <pool> <obj-name> <key>";
        return;
      }
      set<string> keys;

      keys.insert(string(argv[3]));
      t.omap_rmkeys(coll_t(pgid), obj, keys);
      r = store->apply_transaction(t);
      if (r < 0)
        ss << "error=" << r;
      else
        ss << "ok";
    } else if (command == "setomapheader") {
      if (argc != 4) {
        ss << "usage: setomapheader <pool> <obj-name> <header>";
        return;
      }
      bufferlist newheader;

      newheader.append(argv[3]);
      t.omap_setheader(coll_t(pgid), obj, newheader);
      r = store->apply_transaction(t);
      if (r < 0)
        ss << "error=" << r;
      else
        ss << "ok";
    } else if (command == "getomap") {
      if (argc != 3) {
        ss << "usage: getomap <pool> <obj-name>";
        return;
      }
      //Debug: Output entire omap
      bufferlist hdrbl;
      map<string, bufferlist> keyvals;
      r = store->omap_get(coll_t(pgid), obj, &hdrbl, &keyvals);
      if (r >= 0) {
          ss << "header=" << string(hdrbl.c_str(), hdrbl.length());
          for (map<string, bufferlist>::iterator it = keyvals.begin();
              it != keyvals.end(); ++it)
            ss << " key=" << (*it).first << " val="
               << string((*it).second.c_str(), (*it).second.length());
      } else {
          ss << "error=" << r;
      }
    } else if (command == "truncobj") {
      if (argc != 4) {
	ss << "usage: truncobj <pool> <obj-name> <val>";
	return;
      }
      t.truncate(coll_t(pgid), obj, atoi(argv[3].c_str()));
      r = store->apply_transaction(t);
      if (r < 0)
	ss << "error=" << r;
      else
	ss << "ok";
    } else if (command == "injectdataerr") {
      store->inject_data_error(obj);
      ss << "ok";
    } else if (command == "injectmdataerr") {
      store->inject_mdata_error(obj);
      ss << "ok";
    }
    return;
  }
  ss << "Internal error - command=" << command;
  return;
}

// =========================================
bool remove_dir(
  ObjectStore *store, SnapMapper *mapper,
  OSDriver *osdriver,
  ObjectStore::Sequencer *osr,
  coll_t coll, DeletingStateRef dstate) {
  vector<hobject_t> olist;
  int64_t num = 0;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  hobject_t next;
  while (!next.is_max()) {
    store->collection_list_partial(
      coll,
      next,
      store->get_ideal_list_min(),
      store->get_ideal_list_max(),
      0,
      &olist,
      &next);
    for (vector<hobject_t>::iterator i = olist.begin();
	 i != olist.end();
	 ++i, ++num) {
      OSDriver::OSTransaction _t(osdriver->get_transaction(t));
      int r = mapper->remove_oid(*i, &_t);
      if (r != 0 && r != -ENOENT) {
	assert(0);
      }
      t->remove(coll, *i);
      if (num >= g_conf->osd_target_transaction_size) {
	store->apply_transaction(osr, *t);
	delete t;
	if (!dstate->check_canceled()) {
	  // canceled!
	  return false;
	}
	t = new ObjectStore::Transaction;
	num = 0;
      }
    }
    olist.clear();
  }
  store->apply_transaction(osr, *t);
  delete t;
  return true;
}

void OSD::RemoveWQ::_process(pair<PGRef, DeletingStateRef> item)
{
  PGRef pg(item.first);
  SnapMapper &mapper = pg->snap_mapper;
  OSDriver &driver = pg->osdriver;
  coll_t coll = coll_t(pg->info.pgid);
  pg->osr->flush();

  if (!item.second->start_clearing())
    return;

  if (pg->have_temp_coll()) {
    bool cont = remove_dir(
      store, &mapper, &driver, pg->osr.get(), pg->get_temp_coll(), item.second);
    if (!cont)
      return;
  }
  bool cont = remove_dir(
    store, &mapper, &driver, pg->osr.get(), coll, item.second);
  if (!cont)
    return;

  if (!item.second->start_deleting())
    return;

  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  PGLog::clear_info_log(
    pg->info.pgid,
    OSD::make_infos_oid(),
    pg->log_oid,
    t);
  if (pg->have_temp_coll())
    t->remove_collection(pg->get_temp_coll());
  t->remove_collection(coll);

  // We need the sequencer to stick around until the op is complete
  store->queue_transaction(
    pg->osr.get(),
    t,
    0, // onapplied
    0, // oncommit
    0, // onreadable sync
    new ObjectStore::C_DeleteTransactionHolder<PGRef>(
      t, pg), // oncomplete
    TrackedOpRef());

  item.second->finish_deleting();
}
// =========================================

void OSD::do_mon_report()
{
  dout(7) << "do_mon_report" << dendl;

  utime_t now(ceph_clock_now(g_ceph_context));
  last_mon_report = now;

  // do any pending reports
  send_alive();
  service.send_pg_temp();
  send_failures();
  send_pg_stats(now);
}

void OSD::ms_handle_connect(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    Mutex::Locker l(osd_lock);
    if (is_stopping())
      return;
    dout(10) << "ms_handle_connect on mon" << dendl;
    if (is_booting()) {
      start_boot();
    } else {
      send_alive();
      service.send_pg_temp();
      send_failures();
      send_pg_stats(ceph_clock_now(g_ceph_context));

      monc->sub_want("osd_pg_creates", 0, CEPH_SUBSCRIBE_ONETIME);
      monc->renew_subs();
    }
  }
}

bool OSD::ms_handle_reset(Connection *con)
{
  dout(1) << "OSD::ms_handle_reset()" << dendl;
  OSD::Session *session = (OSD::Session *)con->get_priv();
  if (!session)
    return false;
  session->wstate.reset();
  session->put();
  return true;
}

struct C_OSD_GetVersion : public Context {
  OSD *osd;
  uint64_t oldest, newest;
  C_OSD_GetVersion(OSD *o) : osd(o), oldest(0), newest(0) {}
  void finish(int r) {
    if (r >= 0)
      osd->_maybe_boot(oldest, newest);
  }
};

void OSD::start_boot()
{
  dout(10) << "start_boot - have maps " << superblock.oldest_map
	   << ".." << superblock.newest_map << dendl;
  C_OSD_GetVersion *c = new C_OSD_GetVersion(this);
  monc->get_version("osdmap", &c->newest, &c->oldest, c);
}

void OSD::_maybe_boot(epoch_t oldest, epoch_t newest)
{
  Mutex::Locker l(osd_lock);
  if (is_stopping())
    return;
  dout(10) << "_maybe_boot mon has osdmaps " << oldest << ".." << newest << dendl;

  if (is_initializing()) {
    dout(10) << "still initializing" << dendl;
    return;
  }

  // if our map within recent history, try to add ourselves to the osdmap.
  if (osdmap->test_flag(CEPH_OSDMAP_NOUP)) {
    dout(5) << "osdmap NOUP flag is set, waiting for it to clear" << dendl;
  } else if (is_waiting_for_healthy() || !_is_healthy()) {
    // if we are not healthy, do not mark ourselves up (yet)
    dout(1) << "not healthy; waiting to boot" << dendl;
    if (!is_waiting_for_healthy())
      start_waiting_for_healthy();
    // send pings sooner rather than later
    heartbeat_kick();
  } else if (osdmap->get_epoch() >= oldest - 1 &&
	     osdmap->get_epoch() + g_conf->osd_map_message_max > newest) {
    _send_boot();
    return;
  }
  
  // get all the latest maps
  if (osdmap->get_epoch() > oldest)
    monc->sub_want("osdmap", osdmap->get_epoch(), CEPH_SUBSCRIBE_ONETIME);
  else
    monc->sub_want("osdmap", oldest - 1, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();
}

void OSD::start_waiting_for_healthy()
{
  dout(1) << "start_waiting_for_healthy" << dendl;
  state = STATE_WAITING_FOR_HEALTHY;
  last_heartbeat_resample = utime_t();
}

bool OSD::_is_healthy()
{
  if (!g_ceph_context->get_heartbeat_map()->is_healthy()) {
    dout(1) << "is_healthy false -- internal heartbeat failed" << dendl;
    return false;
  }

  if (is_waiting_for_healthy()) {
    Mutex::Locker l(heartbeat_lock);
    utime_t cutoff = ceph_clock_now(g_ceph_context);
    cutoff -= g_conf->osd_heartbeat_grace;
    int num = 0, up = 0;
    for (map<int,HeartbeatInfo>::iterator p = heartbeat_peers.begin();
	 p != heartbeat_peers.end();
	 ++p) {
      if (p->second.is_healthy(cutoff))
	++up;
      ++num;
    }
    if (up < num / 3) {
      dout(1) << "is_healthy false -- only " << up << "/" << num << " up peers (less than 1/3)" << dendl;
      return false;
    }
  }

  return true;
}

void OSD::_send_boot()
{
  dout(10) << "_send_boot" << dendl;
  entity_addr_t cluster_addr = cluster_messenger->get_myaddr();
  if (cluster_addr.is_blank_ip()) {
    int port = cluster_addr.get_port();
    cluster_addr = client_messenger->get_myaddr();
    cluster_addr.set_port(port);
    cluster_messenger->set_addr_unknowns(cluster_addr);
    dout(10) << " assuming cluster_addr ip matches client_addr" << dendl;
  }
  entity_addr_t hb_back_addr = hb_back_server_messenger->get_myaddr();
  if (hb_back_addr.is_blank_ip()) {
    int port = hb_back_addr.get_port();
    hb_back_addr = cluster_addr;
    hb_back_addr.set_port(port);
    hb_back_server_messenger->set_addr_unknowns(hb_back_addr);
    dout(10) << " assuming hb_back_addr ip matches cluster_addr" << dendl;
  }
  entity_addr_t hb_front_addr = hb_front_server_messenger->get_myaddr();
  if (hb_front_addr.is_blank_ip()) {
    int port = hb_front_addr.get_port();
    hb_front_addr = client_messenger->get_myaddr();
    hb_front_addr.set_port(port);
    hb_front_server_messenger->set_addr_unknowns(hb_front_addr);
    dout(10) << " assuming hb_front_addr ip matches client_addr" << dendl;
  }

  MOSDBoot *mboot = new MOSDBoot(superblock, boot_epoch, hb_back_addr, hb_front_addr, cluster_addr);
  dout(10) << " client_addr " << client_messenger->get_myaddr()
	   << ", cluster_addr " << cluster_addr
	   << ", hb_back_addr " << hb_back_addr
	   << ", hb_front_addr " << hb_front_addr
	   << dendl;
  monc->send_mon_message(mboot);
}

void OSD::queue_want_up_thru(epoch_t want)
{
  map_lock.get_read();
  epoch_t cur = osdmap->get_up_thru(whoami);
  if (want > up_thru_wanted) {
    dout(10) << "queue_want_up_thru now " << want << " (was " << up_thru_wanted << ")" 
	     << ", currently " << cur
	     << dendl;
    up_thru_wanted = want;

    // expedite, a bit.  WARNING this will somewhat delay other mon queries.
    last_mon_report = ceph_clock_now(g_ceph_context);
    send_alive();
  } else {
    dout(10) << "queue_want_up_thru want " << want << " <= queued " << up_thru_wanted 
	     << ", currently " << cur
	     << dendl;
  }
  map_lock.put_read();
}

void OSD::send_alive()
{
  if (!osdmap->exists(whoami))
    return;
  epoch_t up_thru = osdmap->get_up_thru(whoami);
  dout(10) << "send_alive up_thru currently " << up_thru << " want " << up_thru_wanted << dendl;
  if (up_thru_wanted > up_thru) {
    up_thru_pending = up_thru_wanted;
    dout(10) << "send_alive want " << up_thru_wanted << dendl;
    monc->send_mon_message(new MOSDAlive(osdmap->get_epoch(), up_thru_wanted));
  }
}

void OSDService::send_message_osd_cluster(int peer, Message *m, epoch_t from_epoch)
{
  Mutex::Locker l(pre_publish_lock);

  // service map is always newer/newest
  assert(from_epoch <= next_osdmap->get_epoch());

  if (next_osdmap->is_down(peer) ||
      next_osdmap->get_info(peer).up_from > from_epoch) {
    m->put();
    return;
  }
  osd->cluster_messenger->send_message(m, next_osdmap->get_cluster_inst(peer));
}

ConnectionRef OSDService::get_con_osd_cluster(int peer, epoch_t from_epoch)
{
  Mutex::Locker l(pre_publish_lock);

  // service map is always newer/newest
  assert(from_epoch <= next_osdmap->get_epoch());

  if (next_osdmap->is_down(peer) ||
      next_osdmap->get_info(peer).up_from > from_epoch) {
    return NULL;
  }
  ConnectionRef ret(
    osd->cluster_messenger->get_connection(next_osdmap->get_cluster_inst(peer)));
  ret->put(); // Ref from get_connection
  return ret;
}

pair<ConnectionRef,ConnectionRef> OSDService::get_con_osd_hb(int peer, epoch_t from_epoch)
{
  Mutex::Locker l(pre_publish_lock);

  // service map is always newer/newest
  assert(from_epoch <= next_osdmap->get_epoch());

  pair<ConnectionRef,ConnectionRef> ret;
  if (next_osdmap->is_down(peer) ||
      next_osdmap->get_info(peer).up_from > from_epoch) {
    return ret;
  }
  ret.first = osd->hbclient_messenger->get_connection(next_osdmap->get_hb_back_inst(peer));
  ret.first->put(); // Ref from get_connection
  ret.second = osd->hbclient_messenger->get_connection(next_osdmap->get_hb_front_inst(peer));
  if (ret.second)
    ret.second->put(); // Ref from get_connection
  return ret;
}

void OSDService::queue_want_pg_temp(pg_t pgid, vector<int>& want)
{
  Mutex::Locker l(pg_temp_lock);
  pg_temp_wanted[pgid] = want;
}

void OSDService::send_pg_temp()
{
  Mutex::Locker l(pg_temp_lock);
  if (pg_temp_wanted.empty())
    return;
  dout(10) << "send_pg_temp " << pg_temp_wanted << dendl;
  MOSDPGTemp *m = new MOSDPGTemp(osdmap->get_epoch());
  m->pg_temp = pg_temp_wanted;
  monc->send_mon_message(m);
}

void OSD::send_failures()
{
  assert(osd_lock.is_locked());
  bool locked = false;
  if (!failure_queue.empty()) {
    heartbeat_lock.Lock();
    locked = true;
  }
  utime_t now = ceph_clock_now(g_ceph_context);
  while (!failure_queue.empty()) {
    int osd = failure_queue.begin()->first;
    int failed_for = (int)(double)(now - failure_queue.begin()->second);
    entity_inst_t i = osdmap->get_inst(osd);
    monc->send_mon_message(new MOSDFailure(monc->get_fsid(), i, failed_for, osdmap->get_epoch()));
    failure_pending[osd] = i;
    failure_queue.erase(osd);
  }
  if (locked) heartbeat_lock.Unlock();
}

void OSD::send_still_alive(epoch_t epoch, const entity_inst_t &i)
{
  MOSDFailure *m = new MOSDFailure(monc->get_fsid(), i, 0, epoch);
  m->is_failed = false;
  monc->send_mon_message(m);
}

void OSD::send_pg_stats(const utime_t &now)
{
  assert(osd_lock.is_locked());

  dout(20) << "send_pg_stats" << dendl;

  stat_lock.Lock();
  osd_stat_t cur_stat = osd_stat;
  stat_lock.Unlock();
   
  pg_stat_queue_lock.Lock();

  if (osd_stat_updated || !pg_stat_queue.empty()) {
    last_pg_stats_sent = now;
    osd_stat_updated = false;

    dout(10) << "send_pg_stats - " << pg_stat_queue.size() << " pgs updated" << dendl;

    utime_t had_for(now);
    had_for -= had_map_since;

    MPGStats *m = new MPGStats(monc->get_fsid(), osdmap->get_epoch(), had_for);
    m->set_tid(++pg_stat_tid);
    m->osd_stat = cur_stat;

    xlist<PG*>::iterator p = pg_stat_queue.begin();
    while (!p.end()) {
      PG *pg = *p;
      ++p;
      if (!pg->is_primary()) {  // we hold map_lock; role is stable.
	pg->stat_queue_item.remove_myself();
	pg->put("pg_stat_queue");
	continue;
      }
      pg->pg_stats_publish_lock.Lock();
      if (pg->pg_stats_publish_valid) {
	m->pg_stat[pg->info.pgid] = pg->pg_stats_publish;
	dout(25) << " sending " << pg->info.pgid << " " << pg->pg_stats_publish.reported << dendl;
      } else {
	dout(25) << " NOT sending " << pg->info.pgid << " " << pg->pg_stats_publish.reported << ", not valid" << dendl;
      }
      pg->pg_stats_publish_lock.Unlock();
    }

    if (!outstanding_pg_stats) {
      outstanding_pg_stats = true;
      last_pg_stats_ack = ceph_clock_now(g_ceph_context);
    }
    monc->send_mon_message(m);
  }

  pg_stat_queue_lock.Unlock();
}

void OSD::handle_pg_stats_ack(MPGStatsAck *ack)
{
  dout(10) << "handle_pg_stats_ack " << dendl;

  if (!require_mon_peer(ack)) {
    ack->put();
    return;
  }

  last_pg_stats_ack = ceph_clock_now(g_ceph_context);

  pg_stat_queue_lock.Lock();

  if (ack->get_tid() > pg_stat_tid_flushed) {
    pg_stat_tid_flushed = ack->get_tid();
    pg_stat_queue_cond.Signal();
  }

  xlist<PG*>::iterator p = pg_stat_queue.begin();
  while (!p.end()) {
    PG *pg = *p;
    PGRef _pg(pg);
    ++p;

    if (ack->pg_stat.count(pg->info.pgid)) {
      eversion_t acked = ack->pg_stat[pg->info.pgid];
      pg->pg_stats_publish_lock.Lock();
      if (acked == pg->pg_stats_publish.reported) {
	dout(25) << " ack on " << pg->info.pgid << " " << pg->pg_stats_publish.reported << dendl;
	pg->stat_queue_item.remove_myself();
	pg->put("pg_stat_queue");
      } else {
	dout(25) << " still pending " << pg->info.pgid << " " << pg->pg_stats_publish.reported
		 << " > acked " << acked << dendl;
      }
      pg->pg_stats_publish_lock.Unlock();
    } else {
      dout(30) << " still pending " << pg->info.pgid << " " << pg->pg_stats_publish.reported << dendl;
    }
  }
  
  if (!pg_stat_queue.size()) {
    outstanding_pg_stats = false;
  }

  pg_stat_queue_lock.Unlock();

  ack->put();
}

void OSD::flush_pg_stats()
{
  dout(10) << "flush_pg_stats" << dendl;
  utime_t now = ceph_clock_now(cct);
  send_pg_stats(now);

  osd_lock.Unlock();

  pg_stat_queue_lock.Lock();
  uint64_t tid = pg_stat_tid;
  dout(10) << "flush_pg_stats waiting for stats tid " << tid << " to flush" << dendl;
  while (tid > pg_stat_tid_flushed)
    pg_stat_queue_cond.Wait(pg_stat_queue_lock);
  dout(10) << "flush_pg_stats finished waiting for stats tid " << tid << " to flush" << dendl;
  pg_stat_queue_lock.Unlock();

  osd_lock.Lock();
}


void OSD::handle_command(MMonCommand *m)
{
  if (!require_mon_peer(m))
    return;

  Command *c = new Command(m->cmd, m->get_tid(), m->get_data(), NULL);
  command_wq.queue(c);
  m->put();
}

void OSD::handle_command(MCommand *m)
{
  Connection *con = m->get_connection();
  Session *session = static_cast<Session *>(con->get_priv());
  if (!session) {
    client_messenger->send_message(new MCommandReply(m, -EPERM), con);
    m->put();
    return;
  }

  OSDCap& caps = session->caps;
  session->put();

  if (!caps.allow_all() || m->get_source().is_mon()) {
    client_messenger->send_message(new MCommandReply(m, -EPERM), con);
    m->put();
    return;
  }

  Command *c = new Command(m->cmd, m->get_tid(), m->get_data(), con);
  command_wq.queue(c);

  m->put();
}

struct OSDCommand {
  string cmdstring;
  string helpstring;
} osd_commands[] = {

#define COMMAND(parsesig, helptext) \
  {parsesig, helptext},

// yes, these are really pg commands, but there's a limit to how
// much work it's worth.  The OSD returns all of them.

COMMAND("pg " \
	"name=pgid,type=CephPgid " \
	"name=cmd,type=CephChoices,strings=query", \
	"show details of a specific pg")
COMMAND("pg " \
	"name=pgid,type=CephPgid " \
	"name=cmd,type=CephChoices,strings=mark_unfound_lost " \
	"name=mulcmd,type=CephChoices,strings=revert", \
	"mark all unfound objects in this pg as lost, either removing or reverting to a prior version if one is available")
COMMAND("pg " \
	"name=pgid,type=CephPgid " \
	"name=cmd,type=CephChoices,strings=list_missing " \
	"name=offset,type=CephString,req=false",
	"list missing objects on this pg, perhaps starting at an offset given in JSON")

COMMAND("version", "report version of OSD")
COMMAND("injectargs " \
	"name=injected_args,type=CephString,n=N",
	"inject configuration arguments into running OSD")
COMMAND("bench " \
	"name=count,type=CephInt,req=false " \
	"name=size,type=CephInt,req=false ", \
	"OSD benchmark: write <count> <size>-byte objects, " \
	"(default 1G size 4MB). Results in log.")
COMMAND("flush_pg_stats", "flush pg stats")
COMMAND("debug dump_missing " \
	"name=filename,type=CephFilepath",
	"dump missing objects to a named file")
COMMAND("debug kick_recovery_wq " \
	"name=delay,type=CephInt,range=0",
	"set osd_recovery_delay_start to <val>")
COMMAND("cpu_profiler " \
	"name=arg,type=CephChoices,strings=status|flush",
	"run cpu profiling on daemon")
COMMAND("dump_pg_recovery_stats", "dump pg recovery statistics")
COMMAND("reset_pg_recovery_stats", "reset pg recovery statistics")

};

void OSD::do_command(Connection *con, tid_t tid, vector<string>& cmd, bufferlist& data)
{
  int r = 0;
  stringstream ss, ds;
  string rs;
  bufferlist odata;

  dout(20) << "do_command tid " << tid << " " << cmd << dendl;

  map<string, cmd_vartype> cmdmap;
  string prefix;

  if (cmd.empty()) {
    ss << "no command given";
    goto out;
  }

  if (!cmdmap_from_json(cmd, &cmdmap, ss)) {
    r = -EINVAL;
    goto out;
  }

  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  if (prefix == "get_command_descriptions") {
    int cmdnum = 0;
    JSONFormatter *f = new JSONFormatter();
    f->open_object_section("command_descriptions");
    for (OSDCommand *cp = osd_commands;
	 cp < &osd_commands[ARRAY_SIZE(osd_commands)]; cp++) {

      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(f, secname.str(),
				cp->cmdstring, cp->helpstring);
      cmdnum++;
    }
    f->close_section();	// command_descriptions

    f->flush(ds);
    odata.append(ds);
    delete f;
    goto out;
  }

  if (prefix == "version") {
    ds << pretty_version_to_str();
    goto out;
  }
  else if (prefix == "injectargs") {
    vector<string> argsvec;
    cmd_getval(g_ceph_context, cmdmap, "injected_args", argsvec);

    if (argsvec.empty()) {
      r = -EINVAL;
      ss << "ignoring empty injectargs";
      goto out;
    }
    string args = argsvec.front();
    for (vector<string>::iterator a = ++argsvec.begin(); a != argsvec.end(); a++)
      args += " " + *a;
    osd_lock.Unlock();
    g_conf->injectargs(args, &ss);
    osd_lock.Lock();
  }

  else if (prefix == "pg") {
    pg_t pgid;
    string pgidstr;

    if (!cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr)) {
      ss << "no pgid specified";
      r = -EINVAL;
    } else if (!pgid.parse(pgidstr.c_str())) {
      ss << "couldn't parse pgid '" << pgidstr << "'";
      r = -EINVAL;
    } else {
      vector<string> args;
      cmd_getval(g_ceph_context, cmdmap, "args", args);
      PG *pg = _lookup_lock_pg(pgid);
      if (!pg) {
	ss << "i don't have pgid " << pgid;
	r = -ENOENT;
      } else {
	r = pg->do_command(cmd, ss, data, odata);
	pg->unlock();
      }
    }
  }

  else if (prefix == "bench") {
    int64_t count;
    int64_t bsize;
    // default count 1G, size 4MB
    cmd_getval(g_ceph_context, cmdmap, "count", count, (int64_t)1 << 30);
    cmd_getval(g_ceph_context, cmdmap, "bsize", bsize, (int64_t)4 << 20);

    bufferlist bl;
    bufferptr bp(bsize);
    bp.zero();
    bl.push_back(bp);

    ObjectStore::Transaction *cleanupt = new ObjectStore::Transaction;

    store->sync_and_flush();
    utime_t start = ceph_clock_now(g_ceph_context);
    for (int64_t pos = 0; pos < count; pos += bsize) {
      char nm[30];
      snprintf(nm, sizeof(nm), "disk_bw_test_%lld", (long long)pos);
      object_t oid(nm);
      hobject_t soid(sobject_t(oid, 0));
      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      t->write(coll_t::META_COLL, soid, 0, bsize, bl);
      store->queue_transaction(NULL, t);
      cleanupt->remove(coll_t::META_COLL, soid);
    }
    store->sync_and_flush();
    utime_t end = ceph_clock_now(g_ceph_context);

    // clean up
    store->queue_transaction(NULL, cleanupt);

    uint64_t rate = (double)count / (end - start);
    ss << "bench: wrote " << prettybyte_t(count)
       << " in blocks of " << prettybyte_t(bsize) << " in "
       << (end-start) << " sec at " << prettybyte_t(rate) << "/sec";
  }

  else if (prefix == "flush_pg_stats") {
    flush_pg_stats();
  }
  
  else if (prefix == "heap") {
    if (!ceph_using_tcmalloc()) {
      r = -EOPNOTSUPP;
      ss << "could not issue heap profiler command -- not using tcmalloc!";
    } else {
      string heapcmd;
      cmd_getval(g_ceph_context, cmdmap, "heapcmd", heapcmd);
      // XXX 1-element vector, change at callee or make vector here?
      vector<string> heapcmd_vec;
      get_str_vec(heapcmd, heapcmd_vec);
      ceph_heap_profiler_handle_command(heapcmd_vec, ds);
    }
  }

  else if (prefix == "debug dump_missing") {
    string file_name;
    cmd_getval(g_ceph_context, cmdmap, "filename", file_name);
    std::ofstream fout(file_name.c_str());
    if (!fout.is_open()) {
	ss << "failed to open file '" << file_name << "'";
	r = -EINVAL;
	goto out;
    }

    std::set <pg_t> keys;
    for (hash_map<pg_t, PG*>::const_iterator pg_map_e = pg_map.begin();
	 pg_map_e != pg_map.end(); ++pg_map_e) {
      keys.insert(pg_map_e->first);
    }

    fout << "*** osd " << whoami << ": dump_missing ***" << std::endl;
    for (std::set <pg_t>::iterator p = keys.begin();
	 p != keys.end(); ++p) {
      hash_map<pg_t, PG*>::iterator q = pg_map.find(*p);
      assert(q != pg_map.end());
      PG *pg = q->second;
      pg->lock();

      fout << *pg << std::endl;
      std::map<hobject_t, pg_missing_t::item>::const_iterator mend =
	pg->pg_log.get_missing().missing.end();
      std::map<hobject_t, pg_missing_t::item>::const_iterator mi =
	pg->pg_log.get_missing().missing.begin();
      for (; mi != mend; ++mi) {
	fout << mi->first << " -> " << mi->second << std::endl;
	map<hobject_t, set<int> >::const_iterator mli =
	  pg->missing_loc.find(mi->first);
	if (mli == pg->missing_loc.end())
	  continue;
	const set<int> &mls(mli->second);
	if (mls.empty())
	  continue;
	fout << "missing_loc: " << mls << std::endl;
      }
      pg->unlock();
      fout << std::endl;
    }

    fout.close();
  }
  else if (prefix == "debug kick_recovery_wq") {
    int64_t delay;
    cmd_getval(g_ceph_context, cmdmap, "delay", delay);
    ostringstream oss;
    oss << delay;
    r = g_conf->set_val("osd_recovery_delay_start", oss.str().c_str());
    if (r != 0) {
      ss << "kick_recovery_wq: error setting "
	 << "osd_recovery_delay_start to '" << delay << "': error "
	 << r;
      goto out;
    }
    g_conf->apply_changes(NULL);
    ss << "kicking recovery queue. set osd_recovery_delay_start "
       << "to " << g_conf->osd_recovery_delay_start;
    defer_recovery_until = ceph_clock_now(g_ceph_context);
    defer_recovery_until += g_conf->osd_recovery_delay_start;
    recovery_wq.wake();
  }

  else if (prefix == "cpu_profiler") {
    string arg;
    cmd_getval(g_ceph_context, cmdmap, "arg", arg);
    vector<string> argvec;
    get_str_vec(arg, argvec);
    cpu_profiler_handle_command(argvec, ds);
  }

  else if (prefix == "dump_pg_recovery_stats") {
    stringstream s;
    pg_recovery_stats.dump(s);
    ds << "dump pg recovery stats: " << s.str();
  }

  else if (prefix == "reset_pg_recovery_stats") {
    ss << "reset pg recovery stats";
    pg_recovery_stats.reset();
  }

  else {
    ss << "unrecognized command! " << cmd;
    r = -EINVAL;
  }

 out:
  rs = ss.str();
  odata.append(ds);
  dout(0) << "do_command r=" << r << " " << rs << dendl;
  clog.info() << rs << "\n";
  if (con) {
    MCommandReply *reply = new MCommandReply(r, rs);
    reply->set_tid(tid);
    reply->set_data(odata);
    client_messenger->send_message(reply, con);
  }
  return;
}



// --------------------------------------
// dispatch

epoch_t OSD::get_peer_epoch(int peer)
{
  Mutex::Locker l(peer_map_epoch_lock);
  map<int,epoch_t>::iterator p = peer_map_epoch.find(peer);
  if (p == peer_map_epoch.end())
    return 0;
  return p->second;
}

epoch_t OSD::note_peer_epoch(int peer, epoch_t e)
{
  Mutex::Locker l(peer_map_epoch_lock);
  map<int,epoch_t>::iterator p = peer_map_epoch.find(peer);
  if (p != peer_map_epoch.end()) {
    if (p->second < e) {
      dout(10) << "note_peer_epoch osd." << peer << " has " << e << dendl;
      p->second = e;
    } else {
      dout(30) << "note_peer_epoch osd." << peer << " has " << p->second << " >= " << e << dendl;
    }
    return p->second;
  } else {
    dout(10) << "note_peer_epoch osd." << peer << " now has " << e << dendl;
    peer_map_epoch[peer] = e;
    return e;
  }
}
 
void OSD::forget_peer_epoch(int peer, epoch_t as_of) 
{
  Mutex::Locker l(peer_map_epoch_lock);
  map<int,epoch_t>::iterator p = peer_map_epoch.find(peer);
  if (p != peer_map_epoch.end()) {
    if (p->second <= as_of) {
      dout(10) << "forget_peer_epoch osd." << peer << " as_of " << as_of
	       << " had " << p->second << dendl;
      peer_map_epoch.erase(p);
    } else {
      dout(10) << "forget_peer_epoch osd." << peer << " as_of " << as_of
	       << " has " << p->second << " - not forgetting" << dendl;
    }
  }
}


bool OSD::_share_map_incoming(entity_name_t name, Connection *con, epoch_t epoch, Session* session)
{
  bool shared = false;
  dout(20) << "_share_map_incoming " << name << " " << con->get_peer_addr() << " " << epoch << dendl;
  //assert(osd_lock.is_locked());

  assert(is_active());

  // does client have old map?
  if (name.is_client()) {
    bool sendmap = epoch < osdmap->get_epoch();
    if (sendmap && session) {
      if (session->last_sent_epoch < osdmap->get_epoch()) {
	session->last_sent_epoch = osdmap->get_epoch();
      } else {
	sendmap = false; //we don't need to send it out again
	dout(15) << name << " already sent incremental to update from epoch "<< epoch << dendl;
      }
    }
    if (sendmap) {
      dout(10) << name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, con);
      shared = true;
    }
  }

  // does peer have old map?
  if (name.is_osd() &&
      osdmap->is_up(name.num()) &&
      (osdmap->get_cluster_addr(name.num()) == con->get_peer_addr() ||
       osdmap->get_hb_back_addr(name.num()) == con->get_peer_addr())) {
    // remember
    epoch_t has = note_peer_epoch(name.num(), epoch);

    // share?
    if (has < osdmap->get_epoch()) {
      dout(10) << name << " " << con->get_peer_addr() << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      note_peer_epoch(name.num(), osdmap->get_epoch());
      send_incremental_map(epoch, con);
      shared = true;
    }
  }

  if (session)
    session->put();
  return shared;
}


void OSD::_share_map_outgoing(int peer, Connection *con, OSDMapRef map)
{
  if (!map)
    map = service.get_osdmap();

  // send map?
  epoch_t pe = get_peer_epoch(peer);
  if (pe) {
    if (pe < map->get_epoch()) {
      send_incremental_map(pe, con);
      note_peer_epoch(peer, map->get_epoch());
    } else
      dout(20) << "_share_map_outgoing " << con << " already has epoch " << pe << dendl;
  } else {
    dout(20) << "_share_map_outgoing " << con << " don't know epoch, doing nothing" << dendl;
    // no idea about peer's epoch.
    // ??? send recent ???
    // do nothing.
  }
}


bool OSD::heartbeat_dispatch(Message *m)
{
  dout(30) << "heartbeat_dispatch " << m << dendl;
  switch (m->get_type()) {
    
  case CEPH_MSG_PING:
    dout(10) << "ping from " << m->get_source_inst() << dendl;
    m->put();
    break;

  case MSG_OSD_PING:
    handle_osd_ping(static_cast<MOSDPing*>(m));
    break;

  case CEPH_MSG_OSD_MAP:
    {
      Connection *self = cluster_messenger->get_loopback_connection();
      cluster_messenger->send_message(m, self);
      self->put();
    }
    break;

  default:
    dout(0) << "dropping unexpected message " << *m << " from " << m->get_source_inst() << dendl;
    m->put();
  }

  return true;
}

bool OSD::ms_dispatch(Message *m)
{
  if (m->get_type() == MSG_OSD_MARK_ME_DOWN) {
    service.got_stop_ack();
    m->put();
    return true;
  }

  // lock!

  osd_lock.Lock();
  if (is_stopping()) {
    osd_lock.Unlock();
    m->put();
    return true;
  }

  while (dispatch_running) {
    dout(10) << "ms_dispatch waiting for other dispatch thread to complete" << dendl;
    dispatch_cond.Wait(osd_lock);
  }
  dispatch_running = true;

  do_waiters();
  _dispatch(m);
  do_waiters();

  dispatch_running = false;
  dispatch_cond.Signal();

  osd_lock.Unlock();

  return true;
}

bool OSD::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new)
{
  dout(10) << "OSD::ms_get_authorizer type=" << ceph_entity_type_name(dest_type) << dendl;

  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    /* the MonClient checks keys every tick(), so we should just wait for that cycle
       to get through */
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}


bool OSD::ms_verify_authorizer(Connection *con, int peer_type,
			       int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			       bool& isvalid, CryptoKey& session_key)
{
  AuthAuthorizeHandler *authorize_handler = 0;
  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MDS:
    /*
     * note: mds is technically a client from our perspective, but
     * this makes the 'cluster' consistent w/ monitor's usage.
     */
  case CEPH_ENTITY_TYPE_OSD:
    authorize_handler = authorize_handler_cluster_registry->get_handler(protocol);
    break;
  default:
    authorize_handler = authorize_handler_service_registry->get_handler(protocol);
  }
  if (!authorize_handler) {
    dout(0) << "No AuthAuthorizeHandler found for protocol " << protocol << dendl;
    isvalid = false;
    return true;
  }

  AuthCapsInfo caps_info;
  EntityName name;
  uint64_t global_id;
  uint64_t auid = CEPH_AUTH_UID_DEFAULT;

  isvalid = authorize_handler->verify_authorizer(g_ceph_context, monc->rotating_secrets,
						 authorizer_data, authorizer_reply, name, global_id, caps_info, session_key, &auid);

  if (isvalid) {
    Session *s = static_cast<Session *>(con->get_priv());
    if (!s) {
      s = new Session;
      con->set_priv(s->get());
      s->con = con;
      dout(10) << " new session " << s << " con=" << s->con << " addr=" << s->con->get_peer_addr() << dendl;
    }

    s->entity_name = name;
    if (caps_info.allow_all)
      s->caps.set_allow_all();
    s->auid = auid;
 
    if (caps_info.caps.length() > 0) {
      bufferlist::iterator p = caps_info.caps.begin();
      string str;
      try {
	::decode(str, p);
      }
      catch (buffer::error& e) {
      }
      bool success = s->caps.parse(str);
      if (success)
	dout(10) << " session " << s << " " << s->entity_name << " has caps " << s->caps << " '" << str << "'" << dendl;
      else
	dout(10) << " session " << s << " " << s->entity_name << " failed to parse caps '" << str << "'" << dendl;
    }
    
    s->put();
  }
  return true;
};


void OSD::do_waiters()
{
  assert(osd_lock.is_locked());
  
  dout(10) << "do_waiters -- start" << dendl;
  finished_lock.Lock();
  while (!finished.empty()) {
    OpRequestRef next = finished.front();
    finished.pop_front();
    finished_lock.Unlock();
    dispatch_op(next);
    finished_lock.Lock();
  }
  finished_lock.Unlock();
  dout(10) << "do_waiters -- finish" << dendl;
}

void OSD::dispatch_op(OpRequestRef op)
{
  switch (op->request->get_type()) {

  case MSG_OSD_PG_CREATE:
    handle_pg_create(op);
    break;

  case MSG_OSD_PG_NOTIFY:
    handle_pg_notify(op);
    break;
  case MSG_OSD_PG_QUERY:
    handle_pg_query(op);
    break;
  case MSG_OSD_PG_LOG:
    handle_pg_log(op);
    break;
  case MSG_OSD_PG_REMOVE:
    handle_pg_remove(op);
    break;
  case MSG_OSD_PG_INFO:
    handle_pg_info(op);
    break;
  case MSG_OSD_PG_TRIM:
    handle_pg_trim(op);
    break;
  case MSG_OSD_PG_MISSING:
    assert(0 ==
	   "received MOSDPGMissing; this message is supposed to be unused!?!");
    break;
  case MSG_OSD_PG_SCAN:
    handle_pg_scan(op);
    break;
  case MSG_OSD_PG_BACKFILL:
    handle_pg_backfill(op);
    break;

  case MSG_OSD_BACKFILL_RESERVE:
    handle_pg_backfill_reserve(op);
    break;
  case MSG_OSD_RECOVERY_RESERVE:
    handle_pg_recovery_reserve(op);
    break;

    // client ops
  case CEPH_MSG_OSD_OP:
    handle_op(op);
    break;

    // for replication etc.
  case MSG_OSD_SUBOP:
    handle_sub_op(op);
    break;
  case MSG_OSD_SUBOPREPLY:
    handle_sub_op_reply(op);
    break;
  }
}

void OSD::_dispatch(Message *m)
{
  assert(osd_lock.is_locked());
  dout(20) << "_dispatch " << m << " " << *m << dendl;
  Session *session = NULL;

  logger->set(l_osd_buf, buffer::get_total_alloc());

  switch (m->get_type()) {

    // -- don't need lock -- 
  case CEPH_MSG_PING:
    dout(10) << "ping from " << m->get_source() << dendl;
    m->put();
    break;

    // -- don't need OSDMap --

    // map and replication
  case CEPH_MSG_OSD_MAP:
    handle_osd_map(static_cast<MOSDMap*>(m));
    break;

    // osd
  case CEPH_MSG_SHUTDOWN:
    session = static_cast<Session *>(m->get_connection()->get_priv());
    if (!session ||
	session->entity_name.is_mon() ||
	session->entity_name.is_osd())
      shutdown();
    else dout(0) << "shutdown message from connection with insufficient privs!"
		 << m->get_connection() << dendl;
    m->put();
    if (session)
      session->put();
    break;

  case MSG_PGSTATSACK:
    handle_pg_stats_ack(static_cast<MPGStatsAck*>(m));
    break;

  case MSG_MON_COMMAND:
    handle_command(static_cast<MMonCommand*>(m));
    break;
  case MSG_COMMAND:
    handle_command(static_cast<MCommand*>(m));
    break;

  case MSG_OSD_SCRUB:
    handle_scrub(static_cast<MOSDScrub*>(m));
    break;    

  case MSG_OSD_REP_SCRUB:
    handle_rep_scrub(static_cast<MOSDRepScrub*>(m));
    break;    

    // -- need OSDMap --

  default:
    {
      OpRequestRef op = op_tracker.create_request(m);
      op->mark_event("waiting_for_osdmap");
      // no map?  starting up?
      if (!osdmap) {
        dout(7) << "no OSDMap, not booted" << dendl;
        waiting_for_osdmap.push_back(op);
        break;
      }
      
      // need OSDMap
      dispatch_op(op);
    }
  }

  logger->set(l_osd_buf, buffer::get_total_alloc());

}

void OSD::handle_rep_scrub(MOSDRepScrub *m)
{
  dout(10) << "queueing MOSDRepScrub " << *m << dendl;
  rep_scrub_wq.queue(m);
}

void OSD::handle_scrub(MOSDScrub *m)
{
  dout(10) << "handle_scrub " << *m << dendl;
  if (!require_mon_peer(m))
    return;
  if (m->fsid != monc->get_fsid()) {
    dout(0) << "handle_scrub fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  if (m->scrub_pgs.empty()) {
    for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
	 p != pg_map.end();
	 ++p) {
      PG *pg = p->second;
      pg->lock();
      if (pg->is_primary()) {
	pg->unreg_next_scrub();
	pg->scrubber.must_scrub = true;
	pg->scrubber.must_deep_scrub = m->deep || m->repair;
	pg->scrubber.must_repair = m->repair;
	pg->reg_next_scrub();
	dout(10) << "marking " << *pg << " for scrub" << dendl;
      }
      pg->unlock();
    }
  } else {
    for (vector<pg_t>::iterator p = m->scrub_pgs.begin();
	 p != m->scrub_pgs.end();
	 ++p)
      if (pg_map.count(*p)) {
	PG *pg = pg_map[*p];
	pg->lock();
	if (pg->is_primary()) {
	  pg->unreg_next_scrub();
	  pg->scrubber.must_scrub = true;
	  pg->scrubber.must_deep_scrub = m->deep || m->repair;
	  pg->scrubber.must_repair = m->repair;
	  pg->reg_next_scrub();
	  dout(10) << "marking " << *pg << " for scrub" << dendl;
	}
	pg->unlock();
      }
  }
  
  m->put();
}

bool OSD::scrub_random_backoff()
{
  bool coin_flip = (rand() % 3) == whoami % 3;
  if (!coin_flip) {
    dout(20) << "scrub_random_backoff lost coin flip, randomly backing off" << dendl;
    return true;
  }
  return false;
}

bool OSD::scrub_should_schedule()
{
  double loadavgs[1];
  if (getloadavg(loadavgs, 1) != 1) {
    dout(10) << "scrub_should_schedule couldn't read loadavgs\n" << dendl;
    return false;
  }

  if (loadavgs[0] >= g_conf->osd_scrub_load_threshold) {
    dout(20) << "scrub_should_schedule loadavg " << loadavgs[0]
	     << " >= max " << g_conf->osd_scrub_load_threshold
	     << " = no, load too high" << dendl;
    return false;
  }

  dout(20) << "scrub_should_schedule loadavg " << loadavgs[0]
	   << " < max " << g_conf->osd_scrub_load_threshold
	   << " = yes" << dendl;
  return loadavgs[0] < g_conf->osd_scrub_load_threshold;
}

void OSD::sched_scrub()
{
  assert(osd_lock.is_locked());

  bool load_is_low = scrub_should_schedule();

  dout(20) << "sched_scrub load_is_low=" << (int)load_is_low << dendl;

  utime_t now = ceph_clock_now(g_ceph_context);
  
  //dout(20) << " " << last_scrub_pg << dendl;

  pair<utime_t, pg_t> pos;
  if (service.first_scrub_stamp(&pos)) {
    do {
      utime_t t = pos.first;
      pg_t pgid = pos.second;
      dout(30) << "sched_scrub examine " << pgid << " at " << t << dendl;

      utime_t diff = now - t;
      if ((double)diff < g_conf->osd_scrub_min_interval) {
	dout(10) << "sched_scrub " << pgid << " at " << t
		 << ": " << (double)diff << " < min (" << g_conf->osd_scrub_min_interval << " seconds)" << dendl;
	break;
      }
      if ((double)diff < g_conf->osd_scrub_max_interval && !load_is_low) {
	// save ourselves some effort
	dout(10) << "sched_scrub " << pgid << " high load at " << t
		 << ": " << (double)diff << " < max (" << g_conf->osd_scrub_max_interval << " seconds)" << dendl;
	break;
      }

      PG *pg = _lookup_lock_pg(pgid);
      if (pg) {
	if (pg->is_active() &&
	    (load_is_low ||
	     (double)diff >= g_conf->osd_scrub_max_interval ||
	     pg->scrubber.must_scrub)) {
	  dout(10) << "sched_scrub scrubbing " << pgid << " at " << t
		   << (pg->scrubber.must_scrub ? ", explicitly requested" :
		   ( (double)diff >= g_conf->osd_scrub_max_interval ? ", diff >= max" : ""))
		   << dendl;
	  if (pg->sched_scrub()) {
	    pg->unlock();
	    break;
	  }
	}
	pg->unlock();
      }
    } while  (service.next_scrub_stamp(pos, &pos));
  }    
  dout(20) << "sched_scrub done" << dendl;
}

bool OSDService::inc_scrubs_pending()
{
  bool result = false;

  sched_scrub_lock.Lock();
  if (scrubs_pending + scrubs_active < g_conf->osd_max_scrubs) {
    dout(20) << "inc_scrubs_pending " << scrubs_pending << " -> " << (scrubs_pending+1)
	     << " (max " << g_conf->osd_max_scrubs << ", active " << scrubs_active << ")" << dendl;
    result = true;
    ++scrubs_pending;
  } else {
    dout(20) << "inc_scrubs_pending " << scrubs_pending << " + " << scrubs_active << " active >= max " << g_conf->osd_max_scrubs << dendl;
  }
  sched_scrub_lock.Unlock();

  return result;
}

void OSDService::dec_scrubs_pending()
{
  sched_scrub_lock.Lock();
  dout(20) << "dec_scrubs_pending " << scrubs_pending << " -> " << (scrubs_pending-1)
	   << " (max " << g_conf->osd_max_scrubs << ", active " << scrubs_active << ")" << dendl;
  --scrubs_pending;
  assert(scrubs_pending >= 0);
  sched_scrub_lock.Unlock();
}

void OSDService::inc_scrubs_active(bool reserved)
{
  sched_scrub_lock.Lock();
  ++(scrubs_active);
  if (reserved) {
    --(scrubs_pending);
    dout(20) << "inc_scrubs_active " << (scrubs_active-1) << " -> " << scrubs_active
	     << " (max " << g_conf->osd_max_scrubs
	     << ", pending " << (scrubs_pending+1) << " -> " << scrubs_pending << ")" << dendl;
    assert(scrubs_pending >= 0);
  } else {
    dout(20) << "inc_scrubs_active " << (scrubs_active-1) << " -> " << scrubs_active
	     << " (max " << g_conf->osd_max_scrubs
	     << ", pending " << scrubs_pending << ")" << dendl;
  }
  sched_scrub_lock.Unlock();
}

void OSDService::dec_scrubs_active()
{
  sched_scrub_lock.Lock();
  dout(20) << "dec_scrubs_active " << scrubs_active << " -> " << (scrubs_active-1)
	   << " (max " << g_conf->osd_max_scrubs << ", pending " << scrubs_pending << ")" << dendl;
  --scrubs_active;
  sched_scrub_lock.Unlock();
}

bool OSDService::prepare_to_stop()
{
  Mutex::Locker l(is_stopping_lock);
  if (state != NOT_STOPPING)
    return false;

  if (get_osdmap()->is_up(whoami)) {
    state = PREPARING_TO_STOP;
    monc->send_mon_message(new MOSDMarkMeDown(monc->get_fsid(),
					      get_osdmap()->get_inst(whoami),
					      get_osdmap()->get_epoch(),
					      false
					      ));
    utime_t now = ceph_clock_now(g_ceph_context);
    utime_t timeout;
    timeout.set_from_double(now + g_conf->osd_mon_shutdown_timeout);
    while ((ceph_clock_now(g_ceph_context) < timeout) &&
	   (state != STOPPING)) {
      is_stopping_cond.WaitUntil(is_stopping_lock, timeout);
    }
  }
  state = STOPPING;
  return true;
}

void OSDService::got_stop_ack()
{
  Mutex::Locker l(is_stopping_lock);
  dout(10) << "Got stop ack" << dendl;
  state = STOPPING;
  is_stopping_cond.Signal();
}


// =====================================================
// MAP

void OSD::wait_for_new_map(OpRequestRef op)
{
  // ask?
  if (waiting_for_osdmap.empty()) {
    monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }
  
  waiting_for_osdmap.push_back(op);
  op->mark_delayed("wait for new map");
}


/** update_map
 * assimilate new OSDMap(s).  scan pgs, etc.
 */

void OSD::note_down_osd(int peer)
{
  assert(osd_lock.is_locked());
  cluster_messenger->mark_down(osdmap->get_cluster_addr(peer));

  heartbeat_lock.Lock();
  failure_queue.erase(peer);
  failure_pending.erase(peer);
  map<int,HeartbeatInfo>::iterator p = heartbeat_peers.find(peer);
  if (p != heartbeat_peers.end()) {
    hbclient_messenger->mark_down(p->second.con_back);
    p->second.con_back->put();
    if (p->second.con_front) {
      hbclient_messenger->mark_down(p->second.con_front);
      p->second.con_front->put();
    }
    heartbeat_peers.erase(p);
  }
  heartbeat_lock.Unlock();
}

void OSD::note_up_osd(int peer)
{
  forget_peer_epoch(peer, osdmap->get_epoch() - 1);
}

struct C_OnMapApply : public Context {
  OSDService *service;
  boost::scoped_ptr<ObjectStore::Transaction> t;
  list<OSDMapRef> pinned_maps;
  epoch_t e;
  C_OnMapApply(OSDService *service,
	       ObjectStore::Transaction *t,
	       const list<OSDMapRef> &pinned_maps,
	       epoch_t e)
    : service(service), t(t), pinned_maps(pinned_maps), e(e) {}
  void finish(int r) {
    service->clear_map_bl_cache_pins(e);
  }
};

void OSD::handle_osd_map(MOSDMap *m)
{
  assert(osd_lock.is_locked());
  list<OSDMapRef> pinned_maps;
  if (m->fsid != monc->get_fsid()) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  if (session && !(session->entity_name.is_mon() || session->entity_name.is_osd())) {
    //not enough perms!
    m->put();
    session->put();
    return;
  }
  if (session)
    session->put();

  epoch_t first = m->get_first();
  epoch_t last = m->get_last();
  dout(3) << "handle_osd_map epochs [" << first << "," << last << "], i have "
	  << osdmap->get_epoch()
	  << ", src has [" << m->oldest_map << "," << m->newest_map << "]"
	  << dendl;

  logger->inc(l_osd_map);
  logger->inc(l_osd_mape, last - first + 1);
  if (first <= osdmap->get_epoch())
    logger->inc(l_osd_mape_dup, osdmap->get_epoch() - first + 1);

  // make sure there is something new, here, before we bother flushing the queues and such
  if (last <= osdmap->get_epoch()) {
    dout(10) << " no new maps here, dropping" << dendl;
    m->put();
    return;
  }

  // missing some?
  bool skip_maps = false;
  if (first > osdmap->get_epoch() + 1) {
    dout(10) << "handle_osd_map message skips epochs " << osdmap->get_epoch() + 1
	     << ".." << (first-1) << dendl;
    if ((m->oldest_map < first && osdmap->get_epoch() == 0) ||
	m->oldest_map <= osdmap->get_epoch()) {
      monc->sub_want("osdmap", osdmap->get_epoch()+1, CEPH_SUBSCRIBE_ONETIME);
      monc->renew_subs();
      m->put();
      return;
    }
    skip_maps = true;
  }

  ObjectStore::Transaction *_t = new ObjectStore::Transaction;
  ObjectStore::Transaction &t = *_t;

  // store new maps: queue for disk and put in the osdmap cache
  epoch_t start = MAX(osdmap->get_epoch() + 1, first);
  for (epoch_t e = start; e <= last; e++) {
    map<epoch_t,bufferlist>::iterator p;
    p = m->maps.find(e);
    if (p != m->maps.end()) {
      dout(10) << "handle_osd_map  got full map for epoch " << e << dendl;
      OSDMap *o = new OSDMap;
      bufferlist& bl = p->second;
      
      o->decode(bl);
      pinned_maps.push_back(add_map(o));

      hobject_t fulloid = get_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, fulloid, 0, bl.length(), bl);
      pin_map_bl(e, bl);
      continue;
    }

    p = m->incremental_maps.find(e);
    if (p != m->incremental_maps.end()) {
      dout(10) << "handle_osd_map  got inc map for epoch " << e << dendl;
      bufferlist& bl = p->second;
      hobject_t oid = get_inc_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, oid, 0, bl.length(), bl);
      pin_map_inc_bl(e, bl);

      OSDMap *o = new OSDMap;
      if (e > 1) {
	bufferlist obl;
	OSDMapRef prev = get_map(e - 1);
	prev->encode(obl);
	o->decode(obl);
      }

      OSDMap::Incremental inc;
      bufferlist::iterator p = bl.begin();
      inc.decode(p);
      if (o->apply_incremental(inc) < 0) {
	derr << "ERROR: bad fsid?  i have " << osdmap->get_fsid() << " and inc has " << inc.fsid << dendl;
	assert(0 == "bad fsid");
      }

      pinned_maps.push_back(add_map(o));

      bufferlist fbl;
      o->encode(fbl);

      hobject_t fulloid = get_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, fulloid, 0, fbl.length(), fbl);
      pin_map_bl(e, fbl);
      continue;
    }

    assert(0 == "MOSDMap lied about what maps it had?");
  }

  if (superblock.oldest_map) {
    int num = 0;
    epoch_t min(
      MIN(m->oldest_map,
	  service.map_cache.cached_key_lower_bound()));
    for (epoch_t e = superblock.oldest_map; e < min; ++e) {
      dout(20) << " removing old osdmap epoch " << e << dendl;
      t.remove(coll_t::META_COLL, get_osdmap_pobject_name(e));
      t.remove(coll_t::META_COLL, get_inc_osdmap_pobject_name(e));
      superblock.oldest_map = e+1;
      num++;
      if (num >= g_conf->osd_target_transaction_size &&
	  (uint64_t)num > (last - first))  // make sure we at least keep pace with incoming maps
	break;
    }
  }

  if (!superblock.oldest_map || skip_maps)
    superblock.oldest_map = first;
  superblock.newest_map = last;

 
  map_lock.get_write();

  C_Contexts *fin = new C_Contexts(g_ceph_context);

  // advance through the new maps
  for (epoch_t cur = start; cur <= superblock.newest_map; cur++) {
    dout(10) << " advance to epoch " << cur << " (<= newest " << superblock.newest_map << ")" << dendl;

    OSDMapRef newmap = get_map(cur);
    assert(newmap);  // we just cached it above!

    // start blacklisting messages sent to peers that go down.
    service.pre_publish_map(newmap);

    // kill connections to newly down osds
    set<int> old;
    osdmap->get_all_osds(old);
    for (set<int>::iterator p = old.begin(); p != old.end(); ++p) {
      if (*p != whoami &&
	  osdmap->have_inst(*p) &&                        // in old map
	  (!newmap->exists(*p) || !newmap->is_up(*p))) {  // but not the new one
	note_down_osd(*p);
      }
    }
    
    osdmap = newmap;

    superblock.current_epoch = cur;
    advance_map(t, fin);
    had_map_since = ceph_clock_now(g_ceph_context);
  }

  if (osdmap->is_up(whoami) &&
      osdmap->get_addr(whoami) == client_messenger->get_myaddr() &&
      bind_epoch < osdmap->get_up_from(whoami)) {

    if (is_booting()) {
      dout(1) << "state: booting -> active" << dendl;
      state = STATE_ACTIVE;
    }
  }

  bool do_shutdown = false;
  bool do_restart = false;
  if (osdmap->get_epoch() > 0 &&
      state == STATE_ACTIVE) {
    if (!osdmap->exists(whoami)) {
      dout(0) << "map says i do not exist.  shutting down." << dendl;
      do_shutdown = true;   // don't call shutdown() while we have everything paused
    } else if (!osdmap->is_up(whoami) ||
	       !osdmap->get_addr(whoami).probably_equals(client_messenger->get_myaddr()) ||
	       !osdmap->get_cluster_addr(whoami).probably_equals(cluster_messenger->get_myaddr()) ||
	       !osdmap->get_hb_back_addr(whoami).probably_equals(hb_back_server_messenger->get_myaddr()) ||
	       !osdmap->get_hb_front_addr(whoami).probably_equals(hb_front_server_messenger->get_myaddr())) {
      if (!osdmap->is_up(whoami)) {
	if (service.is_preparing_to_stop()) {
	  service.got_stop_ack();
	} else {
	  clog.warn() << "map e" << osdmap->get_epoch()
		      << " wrongly marked me down";
	}
      }
      else if (!osdmap->get_addr(whoami).probably_equals(client_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong client addr (" << osdmap->get_addr(whoami)
		     << " != my " << client_messenger->get_myaddr() << ")";
      else if (!osdmap->get_cluster_addr(whoami).probably_equals(cluster_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong cluster addr (" << osdmap->get_cluster_addr(whoami)
		     << " != my " << cluster_messenger->get_myaddr() << ")";
      else if (!osdmap->get_hb_back_addr(whoami).probably_equals(hb_back_server_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong hb back addr (" << osdmap->get_hb_back_addr(whoami)
		     << " != my " << hb_back_server_messenger->get_myaddr() << ")";
      else if (!osdmap->get_hb_front_addr(whoami).probably_equals(hb_front_server_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong hb front addr (" << osdmap->get_hb_front_addr(whoami)
		     << " != my " << hb_front_server_messenger->get_myaddr() << ")";
      
      if (!service.is_stopping()) {
	up_epoch = 0;
	do_restart = true;
	bind_epoch = osdmap->get_epoch();

	start_waiting_for_healthy();

	set<int> avoid_ports;
	avoid_ports.insert(cluster_messenger->get_myaddr().get_port());
	avoid_ports.insert(hb_back_server_messenger->get_myaddr().get_port());
	avoid_ports.insert(hb_front_server_messenger->get_myaddr().get_port());

	int r = cluster_messenger->rebind(avoid_ports);
	if (r != 0)
	  do_shutdown = true;  // FIXME: do_restart?

	r = hb_back_server_messenger->rebind(avoid_ports);
	if (r != 0)
	  do_shutdown = true;  // FIXME: do_restart?

	r = hb_front_server_messenger->rebind(avoid_ports);
	if (r != 0)
	  do_shutdown = true;  // FIXME: do_restart?

	hbclient_messenger->mark_down_all();

	reset_heartbeat_peers();
      }
    }
  }


  // note in the superblock that we were clean thru the prior epoch
  if (boot_epoch && boot_epoch >= superblock.mounted) {
    superblock.mounted = boot_epoch;
    superblock.clean_thru = osdmap->get_epoch();
  }

  // superblock and commit
  write_superblock(t);
  store->queue_transaction(
    0,
    _t,
    new C_OnMapApply(&service, _t, pinned_maps, osdmap->get_epoch()),
    0, fin);
  service.publish_superblock(superblock);

  map_lock.put_write();

  check_osdmap_features();

  // yay!
  consume_map();

  if (is_active() || is_waiting_for_healthy())
    maybe_update_heartbeat_peers();

  if (!is_active()) {
    dout(10) << " not yet active; waiting for peering wq to drain" << dendl;
    peering_wq.drain();
  } else {
    activate_map();
  }

  if (m->newest_map && m->newest_map > last) {
    dout(10) << " msg say newest map is " << m->newest_map << ", requesting more" << dendl;
    monc->sub_want("osdmap", osdmap->get_epoch()+1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }
  else if (is_booting()) {
    start_boot();  // retry
  }
  else if (do_restart)
    start_boot();

  if (do_shutdown)
    shutdown();

  m->put();
}

void OSD::check_osdmap_features()
{
  // adjust required feature bits?

  // we have to be a bit careful here, because we are accessing the
  // Policy structures without taking any lock.  in particular, only
  // modify integer values that can safely be read by a racing CPU.
  // since we are only accessing existing Policy structures a their
  // current memory location, and setting or clearing bits in integer
  // fields, and we are the only writer, this is not a problem.

  uint64_t mask;
  uint64_t features = osdmap->get_features(&mask);

  {
    Messenger::Policy p = client_messenger->get_default_policy();
    if ((p.features_required & mask) != features) {
      dout(0) << "crush map has features " << features
	      << ", adjusting msgr requires for clients" << dendl;
      p.features_required = (p.features_required & ~mask) | features;
      client_messenger->set_default_policy(p);
    }
  }
  {
    Messenger::Policy p = cluster_messenger->get_policy(entity_name_t::TYPE_OSD);
    if ((p.features_required & mask) != features) {
      dout(0) << "crush map has features " << features
	      << ", adjusting msgr requires for osds" << dendl;
      p.features_required = (p.features_required & ~mask) | features;
      cluster_messenger->set_policy(entity_name_t::TYPE_OSD, p);
    }
  }
}

void OSD::advance_pg(
  epoch_t osd_epoch, PG *pg,
  ThreadPool::TPHandle &handle,
  PG::RecoveryCtx *rctx,
  set<boost::intrusive_ptr<PG> > *new_pgs)
{
  assert(pg->is_locked());
  epoch_t next_epoch = pg->get_osdmap()->get_epoch() + 1;
  OSDMapRef lastmap = pg->get_osdmap();

  if (lastmap->get_epoch() == osd_epoch)
    return;
  assert(lastmap->get_epoch() < osd_epoch);

  for (;
       next_epoch <= osd_epoch;
       ++next_epoch) {
    OSDMapRef nextmap = get_map(next_epoch);

    vector<int> newup, newacting;
    nextmap->pg_to_up_acting_osds(pg->info.pgid, newup, newacting);
    pg->handle_advance_map(nextmap, lastmap, newup, newacting, rctx);

    // Check for split!
    set<pg_t> children;
    if (pg->info.pgid.is_split(
	lastmap->get_pg_num(pg->pool.id),
	nextmap->get_pg_num(pg->pool.id),
	&children)) {
      service.mark_split_in_progress(pg->info.pgid, children);
      split_pgs(
	pg, children, new_pgs, lastmap, nextmap,
	rctx);
    }

    lastmap = nextmap;
    handle.reset_tp_timeout();
  }
  pg->handle_activate_map(rctx);
}

/** 
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(ObjectStore::Transaction& t, C_Contexts *tfin)
{
  assert(osd_lock.is_locked());

  dout(7) << "advance_map epoch " << osdmap->get_epoch()
          << "  " << pg_map.size() << " pgs"
          << dendl;

  if (!up_epoch &&
      osdmap->is_up(whoami) &&
      osdmap->get_inst(whoami) == client_messenger->get_myinst()) {
    up_epoch = osdmap->get_epoch();
    dout(10) << "up_epoch is " << up_epoch << dendl;
    if (!boot_epoch) {
      boot_epoch = osdmap->get_epoch();
      dout(10) << "boot_epoch is " << boot_epoch << dendl;
    }
  }

  // scan pg creations
  hash_map<pg_t, create_pg_info>::iterator n = creating_pgs.begin();
  while (n != creating_pgs.end()) {
    hash_map<pg_t, create_pg_info>::iterator p = n++;
    pg_t pgid = p->first;

    // am i still primary?
    vector<int> acting;
    int nrep = osdmap->pg_to_acting_osds(pgid, acting);
    int role = osdmap->calc_pg_role(whoami, acting, nrep);
    if (role != 0) {
      dout(10) << " no longer primary for " << pgid << ", stopping creation" << dendl;
      creating_pgs.erase(p);
    } else {
      /*
       * adding new ppl to our pg has no effect, since we're still primary,
       * and obviously haven't given the new nodes any data.
       */
      p->second.acting.swap(acting);  // keep the latest
    }
  }

  // scan pgs with waiters
  map<pg_t, list<OpRequestRef> >::iterator p = waiting_for_pg.begin();
  while (p != waiting_for_pg.end()) {
    pg_t pgid = p->first;

    // am i still primary?
    vector<int> acting;
    int nrep = osdmap->pg_to_acting_osds(pgid, acting);
    int role = osdmap->calc_pg_role(whoami, acting, nrep);
    if (role >= 0) {
      ++p;  // still me
    } else {
      dout(10) << " discarding waiting ops for " << pgid << dendl;
      while (!p->second.empty()) {
	p->second.pop_front();
      }
      waiting_for_pg.erase(p++);
    }
  }
  map<pg_t, list<PG::CephPeeringEvtRef> >::iterator q =
    peering_wait_for_split.begin();
  while (q != peering_wait_for_split.end()) {
    pg_t pgid = q->first;

    // am i still primary?
    vector<int> acting;
    int nrep = osdmap->pg_to_acting_osds(pgid, acting);
    int role = osdmap->calc_pg_role(whoami, acting, nrep);
    if (role >= 0) {
      ++q;  // still me
    } else {
      dout(10) << " discarding waiting ops for " << pgid << dendl;
      peering_wait_for_split.erase(q++);
    }
  }
}

void OSD::consume_map()
{
  assert(osd_lock.is_locked());
  dout(7) << "consume_map version " << osdmap->get_epoch() << dendl;

  int num_pg_primary = 0, num_pg_replica = 0, num_pg_stray = 0;
  list<PGRef> to_remove;

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       ++it) {
    PG *pg = it->second;
    pg->lock();
    if (pg->is_primary())
      num_pg_primary++;
    else if (pg->is_replica())
      num_pg_replica++;
    else
      num_pg_stray++;

    if (!osdmap->have_pg_pool(pg->info.pgid.pool())) {
      //pool is deleted!
      to_remove.push_back(PGRef(pg));
    } else {
      service.init_splits_between(it->first, service.get_osdmap(), osdmap);
    }

    pg->unlock();
  }

  for (list<PGRef>::iterator i = to_remove.begin();
       i != to_remove.end();
       to_remove.erase(i++)) {
    (*i)->lock();
    _remove_pg(&**i);
    (*i)->unlock();
  }
  to_remove.clear();

  service.expand_pg_num(service.get_osdmap(), osdmap);

  service.pre_publish_map(osdmap);
  service.publish_map(osdmap);

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       ++it) {
    PG *pg = it->second;
    pg->lock();
    pg->queue_null(osdmap->get_epoch(), osdmap->get_epoch());
    pg->unlock();
  }
  
  logger->set(l_osd_pg, pg_map.size());
  logger->set(l_osd_pg_primary, num_pg_primary);
  logger->set(l_osd_pg_replica, num_pg_replica);
  logger->set(l_osd_pg_stray, num_pg_stray);
}

void OSD::activate_map()
{
  assert(osd_lock.is_locked());

  dout(7) << "activate_map version " << osdmap->get_epoch() << dendl;

  wake_all_pg_waiters();   // the pg mapping may have shifted

  if (osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    dout(10) << " osdmap flagged full, doing onetime osdmap subscribe" << dendl;
    monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }

  // norecover?
  if (osdmap->test_flag(CEPH_OSDMAP_NORECOVER)) {
    if (!paused_recovery) {
      dout(1) << "pausing recovery (NORECOVER flag set)" << dendl;
      paused_recovery = true;
      recovery_tp.pause_new();
    }
  } else {
    if (paused_recovery) {
      dout(1) << "resuming recovery (NORECOVER flag cleared)" << dendl;
      paused_recovery = false;
      recovery_tp.unpause();
    }
  }

  // process waiters
  take_waiters(waiting_for_osdmap);
}


MOSDMap *OSD::build_incremental_map_msg(epoch_t since, epoch_t to)
{
  MOSDMap *m = new MOSDMap(monc->get_fsid());
  m->oldest_map = superblock.oldest_map;
  m->newest_map = superblock.newest_map;
  
  for (epoch_t e = to; e > since; e--) {
    bufferlist bl;
    if (e > m->oldest_map && get_inc_map_bl(e, bl)) {
      m->incremental_maps[e].claim(bl);
    } else if (get_map_bl(e, bl)) {
      m->maps[e].claim(bl);
      break;
    } else {
      derr << "since " << since << " to " << to
	   << " oldest " << m->oldest_map << " newest " << m->newest_map
	   << dendl;
      assert(0 == "missing an osdmap on disk");  // we should have all maps.
    }
  }
  return m;
}

void OSD::send_map(MOSDMap *m, Connection *con)
{
  Messenger *msgr = client_messenger;
  if (entity_name_t::TYPE_OSD == con->get_peer_type())
    msgr = cluster_messenger;
  msgr->send_message(m, con);
}

void OSD::send_incremental_map(epoch_t since, Connection *con)
{
  dout(10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
           << " to " << con << " " << con->get_peer_addr() << dendl;

  if (since < superblock.oldest_map) {
    // just send latest full map
    MOSDMap *m = new MOSDMap(monc->get_fsid());
    m->oldest_map = superblock.oldest_map;
    m->newest_map = superblock.newest_map;
    epoch_t e = osdmap->get_epoch();
    get_map_bl(e, m->maps[e]);
    send_map(m, con);
    return;
  }

  while (since < osdmap->get_epoch()) {
    epoch_t to = osdmap->get_epoch();
    if (to - since > (epoch_t)g_conf->osd_map_message_max)
      to = since + g_conf->osd_map_message_max;
    MOSDMap *m = build_incremental_map_msg(since, to);
    send_map(m, con);
    since = to;
  }
}

bool OSDService::_get_map_bl(epoch_t e, bufferlist& bl)
{
  bool found = map_bl_cache.lookup(e, &bl);
  if (found)
    return true;
  found = store->read(
    coll_t::META_COLL, OSD::get_osdmap_pobject_name(e), 0, 0, bl) >= 0;
  if (found)
    _add_map_bl(e, bl);
  return found;
}

bool OSDService::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  Mutex::Locker l(map_cache_lock);
  bool found = map_bl_inc_cache.lookup(e, &bl);
  if (found)
    return true;
  found = store->read(
    coll_t::META_COLL, OSD::get_inc_osdmap_pobject_name(e), 0, 0, bl) >= 0;
  if (found)
    _add_map_inc_bl(e, bl);
  return found;
}

void OSDService::_add_map_bl(epoch_t e, bufferlist& bl)
{
  dout(10) << "add_map_bl " << e << " " << bl.length() << " bytes" << dendl;
  map_bl_cache.add(e, bl);
}

void OSDService::_add_map_inc_bl(epoch_t e, bufferlist& bl)
{
  dout(10) << "add_map_inc_bl " << e << " " << bl.length() << " bytes" << dendl;
  map_bl_inc_cache.add(e, bl);
}

void OSDService::pin_map_inc_bl(epoch_t e, bufferlist &bl)
{
  Mutex::Locker l(map_cache_lock);
  map_bl_inc_cache.pin(e, bl);
}

void OSDService::pin_map_bl(epoch_t e, bufferlist &bl)
{
  Mutex::Locker l(map_cache_lock);
  map_bl_cache.pin(e, bl);
}

void OSDService::clear_map_bl_cache_pins(epoch_t e)
{
  Mutex::Locker l(map_cache_lock);
  map_bl_inc_cache.clear_pinned(e);
  map_bl_cache.clear_pinned(e);
}

OSDMapRef OSDService::_add_map(OSDMap *o)
{
  epoch_t e = o->get_epoch();

  if (g_conf->osd_map_dedup) {
    // Dedup against an existing map at a nearby epoch
    OSDMapRef for_dedup = map_cache.lower_bound(e);
    if (for_dedup) {
      OSDMap::dedup(for_dedup.get(), o);
    }
  }
  OSDMapRef l = map_cache.add(e, o);
  return l;
}

OSDMapRef OSDService::get_map(epoch_t epoch)
{
  Mutex::Locker l(map_cache_lock);
  OSDMapRef retval = map_cache.lookup(epoch);
  if (retval) {
    dout(30) << "get_map " << epoch << " -cached" << dendl;
    return retval;
  }

  OSDMap *map = new OSDMap;
  if (epoch > 0) {
    dout(20) << "get_map " << epoch << " - loading and decoding " << map << dendl;
    bufferlist bl;
    assert(_get_map_bl(epoch, bl));
    map->decode(bl);
  } else {
    dout(20) << "get_map " << epoch << " - return initial " << map << dendl;
  }
  return _add_map(map);
}

bool OSD::require_mon_peer(Message *m)
{
  if (!m->get_connection()->peer_is_mon()) {
    dout(0) << "require_mon_peer received from non-mon " << m->get_connection()->get_peer_addr()
	    << " " << *m << dendl;
    m->put();
    return false;
  }
  return true;
}

bool OSD::require_osd_peer(OpRequestRef op)
{
  if (!op->request->get_connection()->peer_is_osd()) {
    dout(0) << "require_osd_peer received from non-osd " << op->request->get_connection()->get_peer_addr()
	    << " " << *op->request << dendl;
    return false;
  }
  return true;
}

/*
 * require that we have same (or newer) map, and that
 * the source is the pg primary.
 */
bool OSD::require_same_or_newer_map(OpRequestRef op, epoch_t epoch)
{
  Message *m = op->request;
  dout(15) << "require_same_or_newer_map " << epoch << " (i am " << osdmap->get_epoch() << ") " << m << dendl;

  assert(osd_lock.is_locked());

  // do they have a newer map?
  if (epoch > osdmap->get_epoch()) {
    dout(7) << "waiting for newer map epoch " << epoch << " > my " << osdmap->get_epoch() << " with " << m << dendl;
    wait_for_new_map(op);
    return false;
  }

  if (epoch < up_epoch) {
    dout(7) << "from pre-up epoch " << epoch << " < " << up_epoch << dendl;
    return false;
  }

  // ok, our map is same or newer.. do they still exist?
  if (m->get_source().is_osd()) {
    int from = m->get_source().num();
    if (!osdmap->have_inst(from) ||
	osdmap->get_cluster_addr(from) != m->get_source_inst().addr) {
      dout(10) << "from dead osd." << from << ", marking down" << dendl;
      cluster_messenger->mark_down(m->get_connection());
      return false;
    }
  }

  // ok, we have at least as new a map as they do.  are we (re)booting?
  if (!is_active()) {
    dout(7) << "still in boot state, dropping message " << *m << dendl;
    return false;
  }

  return true;
}





// ----------------------------------------
// pg creation


bool OSD::can_create_pg(pg_t pgid)
{
  assert(creating_pgs.count(pgid));

  // priors empty?
  if (!creating_pgs[pgid].prior.empty()) {
    dout(10) << "can_create_pg " << pgid
	     << " - waiting for priors " << creating_pgs[pgid].prior << dendl;
    return false;
  }

  dout(10) << "can_create_pg " << pgid << " - can create now" << dendl;
  return true;
}

void OSD::split_pgs(
  PG *parent,
  const set<pg_t> &childpgids, set<boost::intrusive_ptr<PG> > *out_pgs,
  OSDMapRef curmap,
  OSDMapRef nextmap,
  PG::RecoveryCtx *rctx)
{
  unsigned pg_num = nextmap->get_pg_num(
    parent->pool.id);
  parent->update_snap_mapper_bits(
    parent->info.pgid.get_split_bits(pg_num)
    );
  for (set<pg_t>::const_iterator i = childpgids.begin();
       i != childpgids.end();
       ++i) {
    dout(10) << "Splitting " << *parent << " into " << *i << dendl;
    assert(service.splitting(*i));
    PG* child = _make_pg(nextmap, *i);
    child->lock(true);
    out_pgs->insert(child);

    unsigned split_bits = i->get_split_bits(pg_num);
    dout(10) << "pg_num is " << pg_num << dendl;
    dout(10) << "m_seed " << i->ps() << dendl;
    dout(10) << "split_bits is " << split_bits << dendl;

    rctx->transaction->create_collection(
      coll_t(*i));
    rctx->transaction->split_collection(
      coll_t(parent->info.pgid),
      split_bits,
      i->m_seed,
      coll_t(*i));
    if (parent->have_temp_coll()) {
      rctx->transaction->create_collection(
	coll_t::make_temp_coll(*i));
      rctx->transaction->split_collection(
	coll_t::make_temp_coll(parent->info.pgid),
	split_bits,
	i->m_seed,
	coll_t::make_temp_coll(*i));
    }
    parent->split_into(
      *i,
      child,
      split_bits);

    child->write_if_dirty(*(rctx->transaction));
    child->unlock();
  }
  parent->write_if_dirty(*(rctx->transaction));
}
  
/*
 * holding osd_lock
 */
void OSD::handle_pg_create(OpRequestRef op)
{
  MOSDPGCreate *m = (MOSDPGCreate*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_CREATE);

  dout(10) << "handle_pg_create " << *m << dendl;

  // drop the next N pg_creates in a row?
  if (debug_drop_pg_create_left < 0 &&
      g_conf->osd_debug_drop_pg_create_probability >
      ((((double)(rand()%100))/100.0))) {
    debug_drop_pg_create_left = debug_drop_pg_create_duration;
  }
  if (debug_drop_pg_create_left >= 0) {
    --debug_drop_pg_create_left;
    if (debug_drop_pg_create_left >= 0) {
      dout(0) << "DEBUG dropping/ignoring pg_create, will drop the next "
	      << debug_drop_pg_create_left << " too" << dendl;
      return;
    }
  }

  if (!require_mon_peer(op->request)) {
    // we have to hack around require_mon_peer's interface limits
    op->request = NULL;
    return;
  }

  if (!require_same_or_newer_map(op, m->epoch)) return;

  op->mark_started();

  int num_created = 0;

  for (map<pg_t,pg_create_t>::iterator p = m->mkpg.begin();
       p != m->mkpg.end();
       ++p) {
    pg_t pgid = p->first;
    epoch_t created = p->second.created;
    pg_t parent = p->second.parent;
    if (p->second.split_bits) // Skip split pgs
      continue;
    pg_t on = pgid;

    if (pgid.preferred() >= 0) {
      dout(20) << "ignoring localized pg " << pgid << dendl;
      continue;
    }

    dout(20) << "mkpg " << pgid << " e" << created << dendl;
   
    // is it still ours?
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(on, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    if (role != 0) {
      dout(10) << "mkpg " << pgid << "  not primary (role=" << role << "), skipping" << dendl;
      continue;
    }
    if (up != acting) {
      dout(10) << "mkpg " << pgid << "  up " << up << " != acting " << acting << ", ignoring" << dendl;
      // we'll get a query soon anyway, since we know the pg
      // must exist. we can ignore this.
      continue;
    }

    // does it already exist?
    if (_have_pg(pgid)) {
      dout(10) << "mkpg " << pgid << "  already exists, skipping" << dendl;
      continue;
    }

    // figure history
    pg_history_t history;
    history.epoch_created = created;
    history.last_epoch_clean = created;
    // Newly created PGs don't need to scrub immediately, so mark them
    // as scrubbed at creation time.
    utime_t now = ceph_clock_now(NULL);
    history.last_scrub_stamp = now;
    history.last_deep_scrub_stamp = now;
    project_pg_history(pgid, history, created, up, acting);
    
    // register.
    creating_pgs[pgid].history = history;
    creating_pgs[pgid].parent = parent;
    creating_pgs[pgid].acting.swap(acting);
    calc_priors_during(pgid, created, history.same_interval_since, 
		       creating_pgs[pgid].prior);

    PG::RecoveryCtx rctx = create_context();
    // poll priors
    set<int>& pset = creating_pgs[pgid].prior;
    dout(10) << "mkpg " << pgid << " e" << created
	     << " h " << history
	     << " : querying priors " << pset << dendl;
    for (set<int>::iterator p = pset.begin(); p != pset.end(); ++p) 
      if (osdmap->is_up(*p))
	(*rctx.query_map)[*p][pgid] = pg_query_t(pg_query_t::INFO, history,
						 osdmap->get_epoch());

    PG *pg = NULL;
    if (can_create_pg(pgid)) {
      pg_interval_map_t pi;
      pg = _create_lock_pg(
	osdmap, pgid, true, false,
	0, creating_pgs[pgid].acting, creating_pgs[pgid].acting,
	history, pi,
	*rctx.transaction);
      pg->info.last_epoch_started = pg->info.history.last_epoch_started;
      creating_pgs.erase(pgid);
      wake_pg_waiters(pg->info.pgid);
      pg->handle_create(&rctx);
      pg->write_if_dirty(*rctx.transaction);
      pg->publish_stats_to_osd();
      pg->unlock();
      num_created++;
    }
    dispatch_context(rctx, pg, osdmap);
  }

  maybe_update_heartbeat_peers();
}


// ----------------------------------------
// peering and recovery

PG::RecoveryCtx OSD::create_context()
{
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  C_Contexts *on_applied = new C_Contexts(g_ceph_context);
  C_Contexts *on_safe = new C_Contexts(g_ceph_context);
  map< int, map<pg_t,pg_query_t> > *query_map =
    new map<int, map<pg_t, pg_query_t> >;
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > *notify_list =
    new map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >;
  map<int,vector<pair<pg_notify_t, pg_interval_map_t> > > *info_map =
    new map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >;
  PG::RecoveryCtx rctx(query_map, info_map, notify_list,
		       on_applied, on_safe, t);
  return rctx;
}

void OSD::dispatch_context_transaction(PG::RecoveryCtx &ctx, PG *pg)
{
  if (!ctx.transaction->empty()) {
    ctx.on_applied->add(new ObjectStore::C_DeleteTransaction(ctx.transaction));
    int tr = store->queue_transaction(
      pg->osr.get(),
      ctx.transaction, ctx.on_applied, ctx.on_safe);
    assert(tr == 0);
    ctx.transaction = new ObjectStore::Transaction;
    ctx.on_applied = new C_Contexts(g_ceph_context);
    ctx.on_safe = new C_Contexts(g_ceph_context);
  }
}

bool OSD::compat_must_dispatch_immediately(PG *pg)
{
  assert(pg->is_locked());
  for (vector<int>::iterator i = pg->acting.begin();
       i != pg->acting.end();
       ++i) {
    if (*i == whoami)
      continue;
    ConnectionRef conn =
      service.get_con_osd_cluster(*i, pg->get_osdmap()->get_epoch());
    if (conn && !(conn->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      return true;
    }
  }
  return false;
}

void OSD::dispatch_context(PG::RecoveryCtx &ctx, PG *pg, OSDMapRef curmap)
{
  if (service.get_osdmap()->is_up(whoami)) {
    do_notifies(*ctx.notify_list, curmap);
    do_queries(*ctx.query_map, curmap);
    do_infos(*ctx.info_map, curmap);
  }
  delete ctx.notify_list;
  delete ctx.query_map;
  delete ctx.info_map;
  if ((ctx.on_applied->empty() &&
       ctx.on_safe->empty() &&
       ctx.transaction->empty()) || !pg) {
    delete ctx.transaction;
    delete ctx.on_applied;
    delete ctx.on_safe;
  } else {
    ctx.on_applied->add(new ObjectStore::C_DeleteTransaction(ctx.transaction));
    int tr = store->queue_transaction(
      pg->osr.get(),
      ctx.transaction, ctx.on_applied, ctx.on_safe);
    assert(tr == 0);
  }
}

/** do_notifies
 * Send an MOSDPGNotify to a primary, with a list of PGs that I have
 * content for, and they are primary for.
 */

void OSD::do_notifies(
  map< int,vector<pair<pg_notify_t,pg_interval_map_t> > >& notify_list,
  OSDMapRef curmap)
{
  for (map< int, vector<pair<pg_notify_t,pg_interval_map_t> > >::iterator it = notify_list.begin();
       it != notify_list.end();
       ++it) {
    if (it->first == whoami) {
      dout(7) << "do_notify osd." << it->first << " is self, skipping" << dendl;
      continue;
    }
    if (!curmap->is_up(it->first))
      continue;
    ConnectionRef con = service.get_con_osd_cluster(it->first, curmap->get_epoch());
    if (!con)
      continue;
    _share_map_outgoing(it->first, con.get(), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      dout(7) << "do_notify osd." << it->first
	      << " on " << it->second.size() << " PGs" << dendl;
      MOSDPGNotify *m = new MOSDPGNotify(curmap->get_epoch(),
					 it->second);
      cluster_messenger->send_message(m, con.get());
    } else {
      dout(7) << "do_notify osd." << it->first
	      << " sending seperate messages" << dendl;
      for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator i =
	     it->second.begin();
	   i != it->second.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > list(1);
	list[0] = *i;
	MOSDPGNotify *m = new MOSDPGNotify(i->first.epoch_sent,
					   list);
	cluster_messenger->send_message(m, con.get());
      }
    }
  }
}


/** do_queries
 * send out pending queries for info | summaries
 */
void OSD::do_queries(map< int, map<pg_t,pg_query_t> >& query_map,
		     OSDMapRef curmap)
{
  for (map< int, map<pg_t,pg_query_t> >::iterator pit = query_map.begin();
       pit != query_map.end();
       ++pit) {
    if (!curmap->is_up(pit->first))
      continue;
    int who = pit->first;
    ConnectionRef con = service.get_con_osd_cluster(who, curmap->get_epoch());
    if (!con)
      continue;
    _share_map_outgoing(who, con.get(), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      dout(7) << "do_queries querying osd." << who
	      << " on " << pit->second.size() << " PGs" << dendl;
      MOSDPGQuery *m = new MOSDPGQuery(curmap->get_epoch(), pit->second);
      cluster_messenger->send_message(m, con.get());
    } else {
      dout(7) << "do_queries querying osd." << who
	      << " sending seperate messages "
	      << " on " << pit->second.size() << " PGs" << dendl;
      for (map<pg_t, pg_query_t>::iterator i = pit->second.begin();
	   i != pit->second.end();
	   ++i) {
	map<pg_t, pg_query_t> to_send;
	to_send.insert(*i);
	MOSDPGQuery *m = new MOSDPGQuery(i->second.epoch_sent, to_send);
	cluster_messenger->send_message(m, con.get());
      }
    }
  }
}


void OSD::do_infos(map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >& info_map,
		   OSDMapRef curmap)
{
  for (map<int,vector<pair<pg_notify_t, pg_interval_map_t> > >::iterator p = info_map.begin();
       p != info_map.end();
       ++p) { 
    if (!curmap->is_up(p->first))
      continue;
    for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator i = p->second.begin();
	 i != p->second.end();
	 ++i) {
      dout(20) << "Sending info " << i->first.info << " to osd." << p->first << dendl;
    }
    ConnectionRef con = service.get_con_osd_cluster(p->first, curmap->get_epoch());
    if (!con)
      continue;
    _share_map_outgoing(p->first, con.get(), curmap);
    if ((con->features & CEPH_FEATURE_INDEP_PG_MAP)) {
      MOSDPGInfo *m = new MOSDPGInfo(curmap->get_epoch());
      m->pg_list = p->second;
      cluster_messenger->send_message(m, con.get());
    } else {
      for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator i =
	     p->second.begin();
	   i != p->second.end();
	   ++i) {
	vector<pair<pg_notify_t, pg_interval_map_t> > to_send(1);
	to_send[0] = *i;
	MOSDPGInfo *m = new MOSDPGInfo(i->first.epoch_sent);
	m->pg_list = to_send;
	cluster_messenger->send_message(m, con.get());
      }
    }
  }
  info_map.clear();
}


/** PGNotify
 * from non-primary to primary
 * includes pg_info_t.
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_notify(OpRequestRef op)
{
  MOSDPGNotify *m = (MOSDPGNotify*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_NOTIFY);

  dout(7) << "handle_pg_notify from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (!require_osd_peer(op))
    return;

  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  for (vector<pair<pg_notify_t, pg_interval_map_t> >::iterator it = m->get_pg_list().begin();
       it != m->get_pg_list().end();
       ++it) {
    PG *pg = 0;

    if (it->first.info.pgid.preferred() >= 0) {
      dout(20) << "ignoring localized pg " << it->first.info.pgid << dendl;
      continue;
    }

    int created = 0;
    if (service.splitting(it->first.info.pgid)) {
      peering_wait_for_split[it->first.info.pgid].push_back(
	PG::CephPeeringEvtRef(
	  new PG::CephPeeringEvt(
	    it->first.epoch_sent, it->first.query_epoch,
	    PG::MNotifyRec(from, it->first))));
      continue;
    }

    pg = get_or_create_pg(it->first.info, it->second,
                          it->first.query_epoch, from, created, true);
    if (!pg)
      continue;
    pg->queue_notify(it->first.epoch_sent, it->first.query_epoch, from, it->first);
    pg->unlock();
  }
}

void OSD::handle_pg_log(OpRequestRef op)
{
  MOSDPGLog *m = (MOSDPGLog*) op->request;
  assert(m->get_header().type == MSG_OSD_PG_LOG);
  dout(7) << "handle_pg_log " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  if (m->info.pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->info.pgid << dendl;
    return;
  }

  if (service.splitting(m->info.pgid)) {
    peering_wait_for_split[m->info.pgid].push_back(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->get_epoch(), m->get_query_epoch(),
	  PG::MLogRec(from, m))));
    return;
  }

  int created = 0;
  PG *pg = get_or_create_pg(m->info, m->past_intervals, m->get_epoch(), 
                            from, created, false);
  if (!pg)
    return;
  op->mark_started();
  pg->queue_log(m->get_epoch(), m->get_query_epoch(), from, m);
  pg->unlock();
}

void OSD::handle_pg_info(OpRequestRef op)
{
  MOSDPGInfo *m = static_cast<MOSDPGInfo *>(op->request);
  assert(m->get_header().type == MSG_OSD_PG_INFO);
  dout(7) << "handle_pg_info " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  int created = 0;

  for (vector<pair<pg_notify_t,pg_interval_map_t> >::iterator p = m->pg_list.begin();
       p != m->pg_list.end();
       ++p) {
    if (p->first.info.pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << p->first.info.pgid << dendl;
      continue;
    }

    if (service.splitting(p->first.info.pgid)) {
      peering_wait_for_split[p->first.info.pgid].push_back(
	PG::CephPeeringEvtRef(
	  new PG::CephPeeringEvt(
	    p->first.epoch_sent, p->first.query_epoch,
	    PG::MInfoRec(from, p->first.info, p->first.epoch_sent))));
      continue;
    }
    PG *pg = get_or_create_pg(p->first.info, p->second, p->first.epoch_sent,
                              from, created, false);
    if (!pg)
      continue;
    pg->queue_info(p->first.epoch_sent, p->first.query_epoch, from,
		   p->first.info);
    pg->unlock();
  }
}

void OSD::handle_pg_trim(OpRequestRef op)
{
  MOSDPGTrim *m = (MOSDPGTrim *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_TRIM);

  dout(7) << "handle_pg_trim " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(op))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(op, m->epoch)) return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  op->mark_started();

  if (!_have_pg(m->pgid)) {
    dout(10) << " don't have pg " << m->pgid << dendl;
  } else {
    PG *pg = _lookup_lock_pg(m->pgid);
    if (m->epoch < pg->info.history.same_interval_since) {
      dout(10) << *pg << " got old trim to " << m->trim_to << ", ignoring" << dendl;
      pg->unlock();
      return;
    }
    assert(pg);

    if (pg->is_primary()) {
      // peer is informing us of their last_complete_ondisk
      dout(10) << *pg << " replica osd." << from << " lcod " << m->trim_to << dendl;
      pg->peer_last_complete_ondisk[from] = m->trim_to;
      if (pg->calc_min_last_complete_ondisk()) {
	dout(10) << *pg << " min lcod now " << pg->min_last_complete_ondisk << dendl;
	pg->trim_peers();
      }
    } else {
      // primary is instructing us to trim
      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      pg->pg_log.trim(*t, m->trim_to, pg->info, pg->log_oid);
      pg->dirty_info = true;
      pg->write_if_dirty(*t);
      int tr = store->queue_transaction(pg->osr.get(), t,
					new ObjectStore::C_DeleteTransaction(t));
      assert(tr == 0);
    }
    pg->unlock();
  }
}

void OSD::handle_pg_scan(OpRequestRef op)
{
  MOSDPGScan *m = static_cast<MOSDPGScan*>(op->request);
  assert(m->get_header().type == MSG_OSD_PG_SCAN);
  dout(10) << "handle_pg_scan " << *m << " from " << m->get_source() << dendl;
  
  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  PG *pg;
  
  if (!_have_pg(m->pgid)) {
    return;
  }

  pg = _lookup_pg(m->pgid);
  assert(pg);

  enqueue_op(pg, op);
}

void OSD::handle_pg_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = static_cast<MOSDPGBackfill*>(op->request);
  assert(m->get_header().type == MSG_OSD_PG_BACKFILL);
  dout(10) << "handle_pg_backfill " << *m << " from " << m->get_source() << dendl;
  
  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  if (m->pgid.preferred() >= 0) {
    dout(10) << "ignoring localized pg " << m->pgid << dendl;
    return;
  }

  PG *pg;
  
  if (!_have_pg(m->pgid)) {
    return;
  }

  pg = _lookup_pg(m->pgid);
  assert(pg);

  enqueue_op(pg, op);
}

void OSD::handle_pg_backfill_reserve(OpRequestRef op)
{
  MBackfillReserve *m = static_cast<MBackfillReserve*>(op->request);
  assert(m->get_header().type == MSG_OSD_BACKFILL_RESERVE);

  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  PG *pg = 0;
  if (!_have_pg(m->pgid))
    return;

  pg = _lookup_lock_pg(m->pgid);
  assert(pg);

  if (m->type == MBackfillReserve::REQUEST) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RequestBackfillPrio(m->priority))));
  } else if (m->type == MBackfillReserve::GRANT) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RemoteBackfillReserved())));
  } else if (m->type == MBackfillReserve::REJECT) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RemoteReservationRejected())));
  } else {
    assert(0);
  }
  pg->unlock();
}

void OSD::handle_pg_recovery_reserve(OpRequestRef op)
{
  MRecoveryReserve *m = static_cast<MRecoveryReserve*>(op->request);
  assert(m->get_header().type == MSG_OSD_RECOVERY_RESERVE);

  if (!require_osd_peer(op))
    return;
  if (!require_same_or_newer_map(op, m->query_epoch))
    return;

  PG *pg = 0;
  if (!_have_pg(m->pgid))
    return;

  pg = _lookup_lock_pg(m->pgid);
  if (!pg)
    return;

  if (m->type == MRecoveryReserve::REQUEST) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RequestRecovery())));
  } else if (m->type == MRecoveryReserve::GRANT) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RemoteRecoveryReserved())));
  } else if (m->type == MRecoveryReserve::RELEASE) {
    pg->queue_peering_event(
      PG::CephPeeringEvtRef(
	new PG::CephPeeringEvt(
	  m->query_epoch,
	  m->query_epoch,
	  PG::RecoveryDone())));
  } else {
    assert(0);
  }
  pg->unlock();
}


/** PGQuery
 * from primary to replica | stray
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_query(OpRequestRef op)
{
  assert(osd_lock.is_locked());

  MOSDPGQuery *m = (MOSDPGQuery*)op->request;
  assert(m->get_header().type == MSG_OSD_PG_QUERY);

  if (!require_osd_peer(op))
    return;

  dout(7) << "handle_pg_query from " << m->get_source() << " epoch " << m->get_epoch() << dendl;
  int from = m->get_source().num();
  
  if (!require_same_or_newer_map(op, m->get_epoch())) return;

  op->mark_started();

  map< int, vector<pair<pg_notify_t, pg_interval_map_t> > > notify_list;
  
  for (map<pg_t,pg_query_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       ++it) {
    pg_t pgid = it->first;

    if (pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << pgid << dendl;
      continue;
    }

    if (service.splitting(pgid)) {
      peering_wait_for_split[pgid].push_back(
	PG::CephPeeringEvtRef(
	  new PG::CephPeeringEvt(
	    it->second.epoch_sent, it->second.epoch_sent,
	    PG::MQuery(from, it->second, it->second.epoch_sent))));
      continue;
    }

    if (pg_map.count(pgid)) {
      PG *pg = 0;
      pg = _lookup_lock_pg(pgid);
      pg->queue_query(it->second.epoch_sent, it->second.epoch_sent,
		      from, it->second);
      pg->unlock();
      continue;
    }

    if (!osdmap->have_pg_pool(pgid.pool()))
      continue;

    // get active crush mapping
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(pgid, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    // same primary?
    pg_history_t history = it->second.history;
    project_pg_history(pgid, history, it->second.epoch_sent, up, acting);

    if (it->second.epoch_sent < history.same_interval_since) {
      dout(10) << " pg " << pgid << " dne, and pg has changed in "
	       << history.same_interval_since
	       << " (msg from " << it->second.epoch_sent << ")" << dendl;
      continue;
    }

    assert(role != 0);
    dout(10) << " pg " << pgid << " dne" << dendl;
    pg_info_t empty(pgid);
    if (it->second.type == pg_query_t::LOG ||
	it->second.type == pg_query_t::FULLLOG) {
      ConnectionRef con = service.get_con_osd_cluster(from, osdmap->get_epoch());
      if (con) {
	MOSDPGLog *mlog = new MOSDPGLog(osdmap->get_epoch(), empty,
					it->second.epoch_sent);
	_share_map_outgoing(from, con.get(), osdmap);
	cluster_messenger->send_message(mlog, con.get());
      }
    } else {
      notify_list[from].push_back(make_pair(pg_notify_t(it->second.epoch_sent,
							osdmap->get_epoch(),
							empty),
					    pg_interval_map_t()));
    }
  }
  do_notifies(notify_list, osdmap);
}


void OSD::handle_pg_remove(OpRequestRef op)
{
  MOSDPGRemove *m = (MOSDPGRemove *)op->request;
  assert(m->get_header().type == MSG_OSD_PG_REMOVE);
  assert(osd_lock.is_locked());

  if (!require_osd_peer(op))
    return;

  dout(7) << "handle_pg_remove from " << m->get_source() << " on "
	  << m->pg_list.size() << " pgs" << dendl;
  
  if (!require_same_or_newer_map(op, m->get_epoch())) return;
  
  op->mark_started();

  for (vector<pg_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       ++it) {
    pg_t pgid = *it;
    if (pgid.preferred() >= 0) {
      dout(10) << "ignoring localized pg " << pgid << dendl;
      continue;
    }
    
    if (pg_map.count(pgid) == 0) {
      dout(10) << " don't have pg " << pgid << dendl;
      continue;
    }
    dout(5) << "queue_pg_for_deletion: " << pgid << dendl;
    PG *pg = _lookup_lock_pg(pgid);
    pg_history_t history = pg->info.history;
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(pgid, up, acting);
    project_pg_history(pg->info.pgid, history, pg->get_osdmap()->get_epoch(),
		       up, acting);
    if (history.same_interval_since <= m->get_epoch()) {
      assert(pg->get_primary() == m->get_source().num());
      PGRef _pg(pg);
      _remove_pg(pg);
      pg->unlock();
    } else {
      dout(10) << *pg << " ignoring remove request, pg changed in epoch "
	       << history.same_interval_since
	       << " > " << m->get_epoch() << dendl;
      pg->unlock();
    }
  }
}

void OSD::_remove_pg(PG *pg)
{
  ObjectStore::Transaction *rmt = new ObjectStore::Transaction;

  // on_removal, which calls remove_watchers_and_notifies, and the erasure from
  // the pg_map must be done together without unlocking the pg lock,
  // to avoid racing with watcher cleanup in ms_handle_reset
  // and handle_notify_timeout
  pg->on_removal(rmt);

  service.cancel_pending_splits_for_parent(pg->info.pgid);

  DeletingStateRef deleting = service.deleting_pgs.lookup_or_create(pg->info.pgid);
  remove_wq.queue(make_pair(PGRef(pg), deleting));

  store->queue_transaction(
    pg->osr.get(), rmt,
    new ObjectStore::C_DeleteTransactionHolder<
      SequencerRef>(rmt, pg->osr),
    new ContainerContext<
      SequencerRef>(pg->osr));

  // remove from map
  pg_map.erase(pg->info.pgid);
  pg->put("PGMap"); // since we've taken it out of map
}


// =========================================================
// RECOVERY

/*
 * caller holds osd_lock
 */
void OSD::check_replay_queue()
{
  assert(osd_lock.is_locked());

  utime_t now = ceph_clock_now(g_ceph_context);
  list< pair<pg_t,utime_t> > pgids;
  replay_queue_lock.Lock();
  while (!replay_queue.empty() &&
	 replay_queue.front().second <= now) {
    pgids.push_back(replay_queue.front());
    replay_queue.pop_front();
  }
  replay_queue_lock.Unlock();

  for (list< pair<pg_t,utime_t> >::iterator p = pgids.begin(); p != pgids.end(); ++p) {
    pg_t pgid = p->first;
    if (pg_map.count(pgid)) {
      PG *pg = _lookup_lock_pg_with_map_lock_held(pgid);
      dout(10) << "check_replay_queue " << *pg << dendl;
      if (pg->is_active() &&
	  pg->is_replay() &&
	  pg->get_role() == 0 &&
	  pg->replay_until == p->second) {
	pg->replay_queued_ops();
      }
      pg->unlock();
    } else {
      dout(10) << "check_replay_queue pgid " << pgid << " (not found)" << dendl;
    }
  }
  
  // wake up _all_ pg waiters; raw pg -> actual pg mapping may have shifted
  wake_all_pg_waiters();
}


bool OSDService::queue_for_recovery(PG *pg)
{
  bool b = recovery_wq.queue(pg);
  if (b)
    dout(10) << "queue_for_recovery queued " << *pg << dendl;
  else
    dout(10) << "queue_for_recovery already queued " << *pg << dendl;
  return b;
}

bool OSD::_recover_now()
{
  if (recovery_ops_active >= g_conf->osd_recovery_max_active) {
    dout(15) << "_recover_now active " << recovery_ops_active
	     << " >= max " << g_conf->osd_recovery_max_active << dendl;
    return false;
  }
  if (ceph_clock_now(g_ceph_context) < defer_recovery_until) {
    dout(15) << "_recover_now defer until " << defer_recovery_until << dendl;
    return false;
  }

  return true;
}

void OSD::do_recovery(PG *pg)
{
  // see how many we should try to start.  note that this is a bit racy.
  recovery_wq.lock();
  int max = g_conf->osd_recovery_max_active - recovery_ops_active;
  recovery_wq.unlock();
  if (max == 0) {
    dout(10) << "do_recovery raced and failed to start anything; requeuing " << *pg << dendl;
    recovery_wq.queue(pg);
  } else {
    pg->lock();
    if (pg->deleting || !(pg->is_active() && pg->is_primary())) {
      pg->unlock();
      return;
    }
    
    dout(10) << "do_recovery starting " << max
	     << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops) on "
	     << *pg << dendl;
#ifdef DEBUG_RECOVERY_OIDS
    dout(20) << "  active was " << recovery_oids[pg->info.pgid] << dendl;
#endif
    
    PG::RecoveryCtx rctx = create_context();
    int started = pg->start_recovery_ops(max, &rctx);
    dout(10) << "do_recovery started " << started
	     << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops) on "
	     << *pg << dendl;

    /*
     * if we couldn't start any recovery ops and things are still
     * unfound, see if we can discover more missing object locations.
     * It may be that our initial locations were bad and we errored
     * out while trying to pull.
     */
    if (!started && pg->have_unfound()) {
      pg->discover_all_missing(*rctx.query_map);
      if (rctx.query_map->empty()) {
	dout(10) << "do_recovery  no luck, giving up on this pg for now" << dendl;
	recovery_wq.lock();
	recovery_wq._dequeue(pg);
	recovery_wq.unlock();
      }
    }

    pg->write_if_dirty(*rctx.transaction);
    OSDMapRef curmap = pg->get_osdmap();
    pg->unlock();
    dispatch_context(rctx, pg, curmap);
  }
}

void OSD::start_recovery_op(PG *pg, const hobject_t& soid)
{
  recovery_wq.lock();
  dout(10) << "start_recovery_op " << *pg << " " << soid
	   << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops)"
	   << dendl;
  assert(recovery_ops_active >= 0);
  recovery_ops_active++;

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active was " << recovery_oids[pg->info.pgid] << dendl;
  assert(recovery_oids[pg->info.pgid].count(soid) == 0);
  recovery_oids[pg->info.pgid].insert(soid);
#endif

  recovery_wq.unlock();
}

void OSD::finish_recovery_op(PG *pg, const hobject_t& soid, bool dequeue)
{
  recovery_wq.lock();
  dout(10) << "finish_recovery_op " << *pg << " " << soid
	   << " dequeue=" << dequeue
	   << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops)"
	   << dendl;

  // adjust count
  recovery_ops_active--;
  assert(recovery_ops_active >= 0);

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active oids was " << recovery_oids[pg->info.pgid] << dendl;
  assert(recovery_oids[pg->info.pgid].count(soid));
  recovery_oids[pg->info.pgid].erase(soid);
#endif

  if (dequeue)
    recovery_wq._dequeue(pg);
  else {
    recovery_wq._queue_front(pg);
  }

  recovery_wq._wake();
  recovery_wq.unlock();
}

// =========================================================
// OPS

void OSDService::reply_op_error(OpRequestRef op, int err)
{
  reply_op_error(op, err, eversion_t());
}

void OSDService::reply_op_error(OpRequestRef op, int err, eversion_t v)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  int flags;
  flags = m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK);

  MOSDOpReply *reply = new MOSDOpReply(m, err, osdmap->get_epoch(), flags);
  Messenger *msgr = client_messenger;
  reply->set_version(v);
  if (m->get_source().is_osd())
    msgr = cluster_messenger;
  msgr->send_message(reply, m->get_connection());
}

void OSDService::handle_misdirected_op(PG *pg, OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  assert(m->get_header().type == CEPH_MSG_OSD_OP);

  if (m->get_map_epoch() < pg->info.history.same_primary_since) {
    dout(7) << *pg << " changed after " << m->get_map_epoch() << ", dropping" << dendl;
    return;
  }

  dout(7) << *pg << " misdirected op in " << m->get_map_epoch() << dendl;
  clog.warn() << m->get_source_inst() << " misdirected " << m->get_reqid()
	      << " pg " << m->get_pg()
	      << " to osd." << whoami
	      << " not " << pg->acting
	      << " in e" << m->get_map_epoch() << "/" << osdmap->get_epoch() << "\n";
  reply_op_error(op, -ENXIO);
}

void OSD::handle_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  assert(m->get_header().type == CEPH_MSG_OSD_OP);
  if (op_is_discardable(m)) {
    dout(10) << " discardable " << *m << dendl;
    return;
  }

  // we don't need encoded payload anymore
  m->clear_payload();

  // require same or newer map
  if (!require_same_or_newer_map(op, m->get_map_epoch()))
    return;

  // object name too long?
  if (m->get_oid().name.size() > MAX_CEPH_OBJECT_NAME_LEN) {
    dout(4) << "handle_op '" << m->get_oid().name << "' is longer than "
	    << MAX_CEPH_OBJECT_NAME_LEN << " bytes!" << dendl;
    service.reply_op_error(op, -ENAMETOOLONG);
    return;
  }

  // blacklisted?
  if (osdmap->is_blacklisted(m->get_source_addr())) {
    dout(4) << "handle_op " << m->get_source_addr() << " is blacklisted" << dendl;
    service.reply_op_error(op, -EBLACKLISTED);
    return;
  }
  // share our map with sender, if they're old
  _share_map_incoming(m->get_source(), m->get_connection(), m->get_map_epoch(),
		      static_cast<Session *>(m->get_connection()->get_priv()));

  if (op->rmw_flags == 0) {
    int r = init_op_flags(op);
    if (r) {
      service.reply_op_error(op, r);
      return;
    }
  }

  if (g_conf->osd_debug_drop_op_probability > 0 &&
      !m->get_source().is_mds()) {
    if ((double)rand() / (double)RAND_MAX < g_conf->osd_debug_drop_op_probability) {
      dout(0) << "handle_op DEBUG artificially dropping op " << *m << dendl;
      return;
    }
  }

  if (op->may_write()) {
    // full?
    if ((service.check_failsafe_full() ||
		  osdmap->test_flag(CEPH_OSDMAP_FULL)) &&
	!m->get_source().is_mds()) {  // FIXME: we'll exclude mds writes for now.
      service.reply_op_error(op, -ENOSPC);
      return;
    }

    // invalid?
    if (m->get_snapid() != CEPH_NOSNAP) {
      service.reply_op_error(op, -EINVAL);
      return;
    }

    // too big?
    if (g_conf->osd_max_write_size &&
	m->get_data_len() > g_conf->osd_max_write_size << 20) {
      // journal can't hold commit!
      derr << "handle_op msg data len " << m->get_data_len()
	   << " > osd_max_write_size " << (g_conf->osd_max_write_size << 20)
	   << " on " << *m << dendl;
      service.reply_op_error(op, -OSD_WRITETOOBIG);
      return;
    }
  }
  // calc actual pgid
  pg_t pgid = m->get_pg();
  int64_t pool = pgid.pool();
  if ((m->get_flags() & CEPH_OSD_FLAG_PGOP) == 0 &&
      osdmap->have_pg_pool(pool))
    pgid = osdmap->raw_pg_to_pg(pgid);

  // get and lock *pg.
  PG *pg = _have_pg(pgid) ? _lookup_pg(pgid) : NULL;
  if (!pg) {
    dout(7) << "hit non-existent pg " << pgid << dendl;

    if (osdmap->get_pg_acting_role(pgid, whoami) >= 0) {
      dout(7) << "we are valid target for op, waiting" << dendl;
      waiting_for_pg[pgid].push_back(op);
      op->mark_delayed("waiting for pg to exist locally");
      return;
    }

    // okay, we aren't valid now; check send epoch
    if (m->get_map_epoch() < superblock.oldest_map) {
      dout(7) << "don't have sender's osdmap; assuming it was valid and that client will resend" << dendl;
      return;
    }
    OSDMapRef send_map = get_map(m->get_map_epoch());

    if (send_map->get_pg_acting_role(pgid, whoami) >= 0) {
      dout(7) << "dropping request; client will resend when they get new map" << dendl;
    } else if (!send_map->have_pg_pool(pgid.pool())) {
      dout(7) << "dropping request; pool did not exist" << dendl;
      clog.warn() << m->get_source_inst() << " invalid " << m->get_reqid()
		  << " pg " << m->get_pg()
		  << " to osd." << whoami
		  << " in e" << osdmap->get_epoch()
		  << ", client e" << m->get_map_epoch()
		  << " when pool " << m->get_pg().pool() << " did not exist"
		  << "\n";
    } else {
      dout(7) << "we are invalid target" << dendl;
      pgid = m->get_pg();
      if ((m->get_flags() & CEPH_OSD_FLAG_PGOP) == 0)
	pgid = send_map->raw_pg_to_pg(pgid);
      clog.warn() << m->get_source_inst() << " misdirected " << m->get_reqid()
		  << " pg " << m->get_pg()
		  << " to osd." << whoami
		  << " in e" << osdmap->get_epoch()
		  << ", client e" << m->get_map_epoch()
		  << " pg " << pgid
		  << " features " << m->get_connection()->get_features()
		  << "\n";
      service.reply_op_error(op, -ENXIO);
    }
    return;
  }

  enqueue_op(pg, op);
}

void OSD::handle_sub_op(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOP);

  dout(10) << "handle_sub_op " << *m << " epoch " << m->map_epoch << dendl;
  if (m->map_epoch < up_epoch) {
    dout(3) << "replica op from before up" << dendl;
    return;
  }

  if (!require_osd_peer(op))
    return;

  // must be a rep op.
  assert(m->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = m->pgid;

  // require same or newer map
  if (!require_same_or_newer_map(op, m->map_epoch))
    return;

  // share our map with sender, if they're old
  _share_map_incoming(m->get_source(), m->get_connection(), m->map_epoch,
		      static_cast<Session*>(m->get_connection()->get_priv()));

  if (service.splitting(pgid)) {
    waiting_for_pg[pgid].push_back(op);
    return;
  }

  PG *pg = _have_pg(pgid) ? _lookup_pg(pgid) : NULL;
  if (!pg) {
    return;
  }
  enqueue_op(pg, op);
}

void OSD::handle_sub_op_reply(OpRequestRef op)
{
  MOSDSubOpReply *m = static_cast<MOSDSubOpReply*>(op->request);
  assert(m->get_header().type == MSG_OSD_SUBOPREPLY);
  if (m->get_map_epoch() < up_epoch) {
    dout(3) << "replica op reply from before up" << dendl;
    return;
  }

  if (!require_osd_peer(op))
    return;

  // must be a rep op.
  assert(m->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = m->get_pg();

  // require same or newer map
  if (!require_same_or_newer_map(op, m->get_map_epoch())) return;

  // share our map with sender, if they're old
  _share_map_incoming(m->get_source(), m->get_connection(), m->get_map_epoch(),
		      static_cast<Session*>(m->get_connection()->get_priv()));

  PG *pg = _have_pg(pgid) ? _lookup_pg(pgid) : NULL;
  if (!pg) {
    return;
  }
  enqueue_op(pg, op);
}

bool OSD::op_is_discardable(MOSDOp *op)
{
  // drop client request if they are not connected and can't get the
  // reply anyway.  unless this is a replayed op, in which case we
  // want to do what we can to apply it.
  if (!op->get_connection()->is_connected() &&
      op->get_version().version == 0) {
    return true;
  }
  return false;
}

/*
 * enqueue called with osd_lock held
 */
void OSD::enqueue_op(PG *pg, OpRequestRef op)
{
  utime_t latency = ceph_clock_now(g_ceph_context) - op->request->get_recv_stamp();
  dout(15) << "enqueue_op " << op << " prio " << op->request->get_priority()
	   << " cost " << op->request->get_cost()
	   << " latency " << latency
	   << " " << *(op->request) << dendl;
  pg->queue_op(op);
}

void OSD::OpWQ::_enqueue(pair<PGRef, OpRequestRef> item)
{
  unsigned priority = item.second->request->get_priority();
  unsigned cost = item.second->request->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict(
      item.second->request->get_source_inst(),
      priority, item);
  else
    pqueue.enqueue(item.second->request->get_source_inst(),
      priority, cost, item);
  osd->logger->set(l_osd_opq, pqueue.length());
}

void OSD::OpWQ::_enqueue_front(pair<PGRef, OpRequestRef> item)
{
  {
    Mutex::Locker l(qlock);
    if (pg_for_processing.count(&*(item.first))) {
      pg_for_processing[&*(item.first)].push_front(item.second);
      item.second = pg_for_processing[&*(item.first)].back();
      pg_for_processing[&*(item.first)].pop_back();
    }
  }
  unsigned priority = item.second->request->get_priority();
  unsigned cost = item.second->request->get_cost();
  if (priority >= CEPH_MSG_PRIO_LOW)
    pqueue.enqueue_strict_front(
      item.second->request->get_source_inst(),
      priority, item);
  else
    pqueue.enqueue_front(item.second->request->get_source_inst(),
      priority, cost, item);
  osd->logger->set(l_osd_opq, pqueue.length());
}

PGRef OSD::OpWQ::_dequeue()
{
  assert(!pqueue.empty());
  PGRef pg;
  {
    Mutex::Locker l(qlock);
    pair<PGRef, OpRequestRef> ret = pqueue.dequeue();
    pg = ret.first;
    pg_for_processing[&*pg].push_back(ret.second);
  }
  osd->logger->set(l_osd_opq, pqueue.length());
  return pg;
}

void OSD::OpWQ::_process(PGRef pg)
{
  pg->lock();
  OpRequestRef op;
  {
    Mutex::Locker l(qlock);
    if (!pg_for_processing.count(&*pg)) {
      pg->unlock();
      return;
    }
    assert(pg_for_processing[&*pg].size());
    op = pg_for_processing[&*pg].front();
    pg_for_processing[&*pg].pop_front();
    if (!(pg_for_processing[&*pg].size()))
      pg_for_processing.erase(&*pg);
  }
  osd->dequeue_op(pg, op);
  pg->unlock();
}


void OSDService::dequeue_pg(PG *pg, list<OpRequestRef> *dequeued)
{
  osd->op_wq.dequeue(pg, dequeued);
}

/*
 * NOTE: dequeue called in worker thread, with pg lock
 */
void OSD::dequeue_op(PGRef pg, OpRequestRef op)
{
  utime_t latency = ceph_clock_now(g_ceph_context) - op->request->get_recv_stamp();
  dout(10) << "dequeue_op " << op << " prio " << op->request->get_priority()
	   << " cost " << op->request->get_cost()
	   << " latency " << latency
	   << " " << *(op->request)
	   << " pg " << *pg << dendl;
  if (pg->deleting)
    return;

  op->mark_reached_pg();

  pg->do_request(op);

  // finish
  dout(10) << "dequeue_op " << op << " finish" << dendl;
}


void OSDService::queue_for_peering(PG *pg)
{
  peering_wq.queue(pg);
}

struct C_CompleteSplits : public Context {
  OSD *osd;
  set<boost::intrusive_ptr<PG> > pgs;
  C_CompleteSplits(OSD *osd, const set<boost::intrusive_ptr<PG> > &in)
    : osd(osd), pgs(in) {}
  void finish(int r) {
    Mutex::Locker l(osd->osd_lock);
    if (osd->is_stopping())
      return;
    PG::RecoveryCtx rctx = osd->create_context();
    set<pg_t> to_complete;
    for (set<boost::intrusive_ptr<PG> >::iterator i = pgs.begin();
	 i != pgs.end();
	 ++i) {
      (*i)->lock();
      osd->add_newly_split_pg(&**i, &rctx);
      osd->dispatch_context_transaction(rctx, &**i);
      if (!((*i)->deleting))
	to_complete.insert((*i)->info.pgid);
      (*i)->unlock();
    }
    osd->service.complete_split(to_complete);
    osd->dispatch_context(rctx, 0, osd->service.get_osdmap());
  }
};

void OSD::process_peering_events(
  const list<PG*> &pgs,
  ThreadPool::TPHandle &handle
  )
{
  bool need_up_thru = false;
  epoch_t same_interval_since = 0;
  OSDMapRef curmap = service.get_osdmap();
  PG::RecoveryCtx rctx = create_context();
  for (list<PG*>::const_iterator i = pgs.begin();
       i != pgs.end();
       ++i) {
    set<boost::intrusive_ptr<PG> > split_pgs;
    PG *pg = *i;
    pg->lock();
    curmap = service.get_osdmap();
    if (pg->deleting) {
      pg->unlock();
      continue;
    }
    advance_pg(curmap->get_epoch(), pg, handle, &rctx, &split_pgs);
    if (!pg->peering_queue.empty()) {
      PG::CephPeeringEvtRef evt = pg->peering_queue.front();
      pg->peering_queue.pop_front();
      pg->handle_peering_event(evt, &rctx);
    }
    need_up_thru = pg->need_up_thru || need_up_thru;
    same_interval_since = MAX(pg->info.history.same_interval_since,
			      same_interval_since);
    pg->write_if_dirty(*rctx.transaction);
    if (!split_pgs.empty()) {
      rctx.on_applied->add(new C_CompleteSplits(this, split_pgs));
      split_pgs.clear();
    }
    if (compat_must_dispatch_immediately(pg)) {
      dispatch_context(rctx, pg, curmap);
      rctx = create_context();
    } else {
      dispatch_context_transaction(rctx, pg);
    }
    pg->unlock();
    handle.reset_tp_timeout();
  }
  if (need_up_thru)
    queue_want_up_thru(same_interval_since);
  dispatch_context(rctx, 0, curmap);

  service.send_pg_temp();
}

// --------------------------------

const char** OSD::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "osd_max_backfills",
    NULL
  };
  return KEYS;
}

void OSD::handle_conf_change(const struct md_config_t *conf,
			     const std::set <std::string> &changed)
{
  if (changed.count("osd_max_backfills")) {
    service.local_reserver.set_max(g_conf->osd_max_backfills);
    service.remote_reserver.set_max(g_conf->osd_max_backfills);
  }
}

// --------------------------------

int OSD::init_op_flags(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->request);
  vector<OSDOp>::iterator iter;

  // client flags have no bearing on whether an op is a read, write, etc.
  op->rmw_flags = 0;

  // set bits based on op codes, called methods.
  for (iter = m->ops.begin(); iter != m->ops.end(); ++iter) {
    if (ceph_osd_op_mode_modify(iter->op.op))
      op->set_write();
    if (ceph_osd_op_mode_read(iter->op.op))
      op->set_read();

    // set READ flag if there are src_oids
    if (iter->soid.oid.name.length())
      op->set_read();

    // set PGOP flag if there are PG ops
    if (ceph_osd_op_type_pg(iter->op.op))
      op->set_pg_op();

    switch (iter->op.op) {
    case CEPH_OSD_OP_CALL:
      {
	bufferlist::iterator bp = iter->indata.begin();
	int is_write, is_read;
	string cname, mname;
	bp.copy(iter->op.cls.class_len, cname);
	bp.copy(iter->op.cls.method_len, mname);

	ClassHandler::ClassData *cls;
	int r = class_handler->open_class(cname, &cls);
	if (r) {
	  derr << "class " << cname << " open got " << cpp_strerror(r) << dendl;
	  if (r == -ENOENT)
	    r = -EOPNOTSUPP;
	  else
	    r = -EIO;
	  return r;
	}
	int flags = cls->get_method_flags(mname.c_str());
	if (flags < 0) {
	  if (flags == -ENOENT)
	    r = -EOPNOTSUPP;
	  else
	    r = flags;
	  return r;
	}
	is_read = flags & CLS_METHOD_RD;
	is_write = flags & CLS_METHOD_WR;

	dout(10) << "class " << cname << " method " << mname
		<< " flags=" << (is_read ? "r" : "") << (is_write ? "w" : "") << dendl;
	if (is_read)
	  op->set_class_read();
	if (is_write)
	  op->set_class_write();
	break;
      }
    default:
      break;
    }
  }

  if (op->rmw_flags == 0)
    return -EINVAL;

  return 0;
}
