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
#include "os/FileStore.h"

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
#include "messages/MOSDPGMissing.h"

#include "messages/MOSDAlive.h"

#include "messages/MOSDScrub.h"
#include "messages/MOSDRepScrub.h"

#include "messages/MMonCommand.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"

#include "messages/MWatchNotify.h"

#include "common/DoutStreambuf.h"
#include "common/perf_counters.h"
#include "common/Timer.h"
#include "common/LogClient.h"
#include "common/safe_io.h"
#include "common/HeartbeatMap.h"

#include "include/color.h"
#include "perfglue/cpu_profiler.h"
#include "perfglue/heap_profiler.h"

#include "osd/ClassHandler.h"

#include "auth/AuthAuthorizeHandler.h"

#include "common/errno.h"

#include "objclass/objclass.h"

#include "include/assert.h"
#include "common/config.h"

#define DOUT_SUBSYS osd
#undef dout_prefix
#define dout_prefix _prefix(*_dout, whoami, osdmap)

static ostream& _prefix(std::ostream* _dout, int whoami, OSDMapRef osdmap) {
  return *_dout << "osd." << whoami << " " << (osdmap ? osdmap->get_epoch():0) << " ";
}

const coll_t coll_t::META_COLL("meta");
const coll_t coll_t::TEMP_COLL("temp");

static CompatSet get_osd_compat_set() {
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_BASE);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_PGINFO);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_OLOC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_LEC);
  ceph_osd_feature_incompat.insert(CEPH_OSD_FEATURE_INCOMPAT_CATEGORIES);
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
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

static int convert_collection(ObjectStore *store, coll_t cid)
{
  vector<hobject_t> objects;
  int r = store->collection_list(cid, objects);
  if (r < 0)
    return r;
  string temp_name("temp");
  map<string, bufferptr> aset;
  r = store->collection_getattrs(cid, aset);
  if (r < 0)
    return r;
  while (store->collection_exists(coll_t(temp_name)))
    temp_name = "_" + temp_name;
  coll_t temp(temp_name);
  
  ObjectStore::Transaction t;
  t.collection_rename(cid, temp);
  t.create_collection(cid);
  for (vector<hobject_t>::iterator obj = objects.begin();
       obj != objects.end();
       ++obj) {
    t.collection_add(cid, temp, *obj);
    t.collection_remove(temp, *obj);
  }
  for (map<string,bufferptr>::iterator i = aset.begin();
       i != aset.end();
       ++i) {
    bufferlist bl;
    bl.append(i->second);
    t.collection_setattr(cid, i->first, bl);
  }
  t.remove_collection(temp);
  r = store->apply_transaction(t);
  if (r < 0)
    return r;
  store->sync_and_flush();
  store->sync();
  return 0;
}

static int do_convertfs(ObjectStore *store)
{
  int r = store->mount();
  if (r < 0)
    return r;

  uint32_t version;
  r = store->version_stamp_is_valid(&version);
  if (r < 0)
    return r;
  if (r == 1) {
    derr << "FileStore is up to date." << dendl;
    return store->umount();
  } else {
    derr << "FileStore is old at version " << version << ".  Updating..." 
	 << dendl;
  }

  derr << "Getting collections" << dendl;
  vector<coll_t> collections;
  r = store->list_collections(collections);
  if (r < 0)
    return r;

  derr << collections.size() << " to process." << dendl;
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
  cerr << "All collections up to date, updating version stamp..." << std::endl;
  r = store->update_version_stamp();
  if (r < 0)
    return r;
  store->sync_and_flush();
  store->sync();
  cerr << "Version stamp updated, done!" << std::endl;
  return store->umount();
}

int OSD::convertfs(const std::string &dev, const std::string &jdev)
{
  // N.B. at some point we should rewrite this to avoid playing games with
  // g_conf here
  char buf[16] = { 0 };
  char *b = buf;
  g_ceph_context->_conf->get_val("filestore_update_collections", &b, sizeof(buf));
  g_ceph_context->_conf->set_val_or_die("filestore_update_collections", "true");
  g_ceph_context->_conf->apply_changes(NULL);
  boost::scoped_ptr<ObjectStore> store(new FileStore(dev, jdev));
  int r = do_convertfs(store.get());
  g_ceph_context->_conf->set_val_or_die("filestore_update_collections", buf);
  g_ceph_context->_conf->apply_changes(NULL);
  return r;
}

int OSD::mkfs(const std::string &dev, const std::string &jdev, uuid_d fsid, int whoami)
{
  int ret;
  ObjectStore *store = NULL;
  OSDSuperblock sb;
  sb.fsid = fsid;
  sb.whoami = whoami;

  try {
    store = create_object_store(dev, jdev);
    if (!store) {
      ret = -ENOENT;
      goto out;
    }
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
    store->sync_and_flush();
    ret = write_meta(dev, fsid, whoami);
    if (ret) {
      derr << "OSD::mkfs: failed to write fsid file: error " << ret << dendl;
      goto umount_store;
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

    {
      bufferlist bl;
      ::encode(sb, bl);

      ObjectStore::Transaction t;
      t.create_collection(coll_t::META_COLL);
      t.write(coll_t::META_COLL, OSD_SUPERBLOCK_POBJECT, 0, bl.length(), bl);
      t.create_collection(coll_t::TEMP_COLL);
      ret = store->apply_transaction(t);
      if (ret) {
	derr << "OSD::mkfs: error while writing OSD_SUPERBLOCK_POBJECT: "
	     << "apply_transaction returned " << ret << dendl;
	goto umount_store;
      }
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

int OSD::write_meta(const std::string &base, const std::string &file,
		    const char *val, size_t vallen)
{
  int ret;
  char fn[PATH_MAX];
  int fd;

  snprintf(fn, sizeof(fn), "%s/%s", base.c_str(), file.c_str());
  fd = ::open(fn, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd < 0) {
    ret = errno;
    derr << "OSD::write_meta: error opening '" << fn << "': "
	 << cpp_strerror(ret) << dendl;
    return -ret;
  }
  ret = safe_write(fd, val, vallen);
  if (ret) {
    derr << "OSD::write_meta: failed to write to '" << fn << "': "
	 << cpp_strerror(ret) << dendl;
    TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }
  if (TEMP_FAILURE_RETRY(::close(fd)) < 0) {
    ret = errno;
    derr << "OSD::write_meta: close error writing to '" << fn << "': "
	 << cpp_strerror(ret) << dendl;
    return ret;
  }
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

#define FSID_FORMAT "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-" \
	"%02x%02x%02x%02x%02x%02x"
#define PR_FSID(f) (f)->fsid[0], (f)->fsid[1], (f)->fsid[2], (f)->fsid[3], \
		(f)->fsid[4], (f)->fsid[5], (f)->fsid[6], (f)->fsid[7],    \
		(f)->fsid[8], (f)->fsid[9], (f)->fsid[10], (f)->fsid[11],  \
		(f)->fsid[12], (f)->fsid[13], (f)->fsid[14], (f)->fsid[15]

int OSD::write_meta(const std::string &base, uuid_d& fsid, int whoami)
{
  char val[80];
  
  snprintf(val, sizeof(val), "%s\n", CEPH_OSD_ONDISK_MAGIC);
  write_meta(base, "magic", val, strlen(val));

  snprintf(val, sizeof(val), "%d\n", whoami);
  write_meta(base, "whoami", val, strlen(val));

  fsid.print(val);
  strcat(val, "\n");
  write_meta(base, "ceph_fsid", val, strlen(val));
  
  return 0;
}

int OSD::peek_meta(const std::string &dev, std::string& magic,
		   uuid_d& fsid, int& whoami)
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
  
  memset(&fsid, 0, sizeof(fsid));
  fsid.parse(val);
  return 0;
}

#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, osdmap)

// cons/des

OSD::OSD(int id, Messenger *internal_messenger, Messenger *external_messenger,
	 Messenger *hbinm, Messenger *hboutm, MonClient *mc,
	 const std::string &dev, const std::string &jdev) :
  Dispatcher(external_messenger->cct),
  osd_lock("OSD::osd_lock"),
  timer(external_messenger->cct, osd_lock),
  authorize_handler_registry(new AuthAuthorizeHandlerRegistry(external_messenger->cct)),
  cluster_messenger(internal_messenger),
  client_messenger(external_messenger),
  monc(mc),
  logger(NULL),
  store(NULL),
  map_in_progress(false),
  clog(external_messenger->cct, client_messenger, &mc->monmap, mc, LogClient::NO_FLAGS),
  whoami(id),
  dev_path(dev), journal_path(jdev),
  dispatch_running(false),
  osd_compat(get_osd_compat_set()),
  state(STATE_BOOTING), boot_epoch(0), up_epoch(0), bind_epoch(0),
  op_tp(external_messenger->cct, "OSD::op_tp", g_conf->osd_op_threads),
  recovery_tp(external_messenger->cct, "OSD::recovery_tp", g_conf->osd_recovery_threads),
  disk_tp(external_messenger->cct, "OSD::disk_tp", g_conf->osd_disk_threads),
  command_tp(external_messenger->cct, "OSD::command_tp", 1),
  heartbeat_lock("OSD::heartbeat_lock"),
  heartbeat_stop(false), heartbeat_epoch(0),
  hbin_messenger(hbinm),
  hbout_messenger(hboutm),
  heartbeat_thread(this),
  heartbeat_dispatcher(this),
  stat_lock("OSD::stat_lock"),
  finished_lock("OSD::finished_lock"),
  op_queue_len(0),
  op_wq(this, g_conf->osd_op_thread_timeout, &op_tp),
  map_lock("OSD::map_lock"),
  peer_map_epoch_lock("OSD::peer_map_epoch_lock"),
  map_cache_lock("OSD::map_cache_lock"),
  up_thru_wanted(0), up_thru_pending(0),
  pg_stat_queue_lock("OSD::pg_stat_queue_lock"),
  osd_stat_updated(false),
  pg_stat_tid(0), pg_stat_tid_flushed(0),
  last_tid(0),
  tid_lock("OSD::tid_lock"),
  backlog_wq(this, g_conf->osd_backlog_thread_timeout, &disk_tp),
  command_wq(this, g_conf->osd_command_thread_timeout, &command_tp),
  recovery_ops_active(0),
  recovery_wq(this, g_conf->osd_recovery_thread_timeout, &recovery_tp),
  remove_list_lock("OSD::remove_list_lock"),
  replay_queue_lock("OSD::replay_queue_lock"),
  snap_trim_wq(this, g_conf->osd_snap_trim_thread_timeout, &disk_tp),
  sched_scrub_lock("OSD::sched_scrub_lock"),
  scrubs_pending(0),
  scrubs_active(0),
  scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  scrub_finalize_wq(this, g_conf->osd_scrub_finalize_thread_timeout, &op_tp),
  rep_scrub_wq(this, g_conf->osd_scrub_thread_timeout, &disk_tp),
  remove_wq(this, g_conf->osd_remove_thread_timeout, &disk_tp),
  watch_lock("OSD::watch_lock"),
  watch_timer(external_messenger->cct, watch_lock)
{
  monc->set_messenger(client_messenger);

  map_in_progress_cond = new Cond();
  
}

OSD::~OSD()
{
  delete authorize_handler_registry;
  delete map_in_progress_cond;
  delete class_handler;
  g_ceph_context->get_perfcounters_collection()->remove(logger);
  delete logger;
  delete store;
}

bool got_sigterm = false;

void handle_signal(int signal)
{
  switch (signal) {
  case SIGTERM:
  case SIGINT:
#ifdef ENABLE_COVERAGE
    exit(0);
#else
    got_sigterm = true;
#endif
    break;
  }
}

void cls_initialize(ClassHandler *ch);


int OSD::pre_init()
{
  Mutex::Locker lock(osd_lock);
  
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
  return 0;
}

int OSD::init()
{
  Mutex::Locker lock(osd_lock);

  timer.init();
  watch_timer.init();
  watch = new Watch();

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
    return -1;
  }

  class_handler = new ClassHandler();
  if (!class_handler)
    return -ENOMEM;
  cls_initialize(class_handler);

  // load up "current" osdmap
  assert_warn(!osdmap);
  if (osdmap) {
    derr << "OSD::init: unable to read current osdmap" << dendl;
    return -1;
  }
  osdmap = get_map(superblock.current_epoch);

  bind_epoch = osdmap->get_epoch();

  clear_temp();

  // make sure (newish) temp dir exists
  if (!store->collection_exists(coll_t::TEMP_COLL)) {
    dout(10) << "creating temp pg dir" << dendl;
    ObjectStore::Transaction t;
    t.create_collection(coll_t::TEMP_COLL);
    store->apply_transaction(t);
  }

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
  client_messenger->add_dispatcher_head(&clog);
  cluster_messenger->add_dispatcher_head(this);

  hbin_messenger->add_dispatcher_head(&heartbeat_dispatcher);
  hbout_messenger->add_dispatcher_head(&heartbeat_dispatcher);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
  r = monc->init();
  if (r < 0)
    return r;

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&clog);

  osd_lock.Unlock();

  monc->authenticate();
  monc->wait_auth_rotating(30.0);

  osd_lock.Lock();

  op_tp.start();
  recovery_tp.start();
  disk_tp.start();
  command_tp.start();

  // start the heartbeat
  heartbeat_thread.create();

  // tick
  timer.add_event_after(g_conf->osd_heartbeat_interval, new C_Tick(this));

#ifdef ENABLE_COVERAGE
  signal(SIGTERM, handle_signal);
#endif
#if 0
  int ret = monc->start_auth_rotating(ename, KEY_ROTATE_TIME);
  if (ret < 0) {
    dout(0) << "could not start rotating keys, err=" << ret << dendl;
    return ret;
  }
  dout(0) << "started rotating keys" << dendl;
#endif

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
  osd_plb.add_fl_avg(l_osd_op_lat,   "op_latency");       // client op latency

  osd_plb.add_u64_counter(l_osd_op_r,      "op_r");        // client reads
  osd_plb.add_u64_counter(l_osd_op_r_outb, "op_r_out_bytes");   // client read out bytes
  osd_plb.add_fl_avg(l_osd_op_r_lat,  "op_r_latency");    // client read latency
  osd_plb.add_u64_counter(l_osd_op_w,      "op_w");        // client writes
  osd_plb.add_u64_counter(l_osd_op_w_inb,  "op_w_in_bytes");    // client write in bytes
  osd_plb.add_fl_avg(l_osd_op_w_rlat, "op_w_rlat");   // client write readable/applied latency
  osd_plb.add_fl_avg(l_osd_op_w_lat,  "op_w_latency");    // client write latency
  osd_plb.add_u64_counter(l_osd_op_rw,     "op_rw");       // client rmw
  osd_plb.add_u64_counter(l_osd_op_rw_inb, "op_rw_in_bytes");   // client rmw in bytes
  osd_plb.add_u64_counter(l_osd_op_rw_outb,"op_rw_out_bytes");  // client rmw out bytes
  osd_plb.add_fl_avg(l_osd_op_rw_rlat,"op_rw_rlat");  // client rmw readable/applied latency
  osd_plb.add_fl_avg(l_osd_op_rw_lat, "op_rw_latency");   // client rmw latency

  osd_plb.add_u64_counter(l_osd_sop,       "subop");         // subops
  osd_plb.add_u64_counter(l_osd_sop_inb,   "subop_in_bytes");     // subop in bytes
  osd_plb.add_fl_avg(l_osd_sop_lat,   "subop_latency");     // subop latency

  osd_plb.add_u64_counter(l_osd_sop_w,     "subop_w");          // replicated (client) writes
  osd_plb.add_u64_counter(l_osd_sop_w_inb, "subop_w_in_bytes");      // replicated write in bytes
  osd_plb.add_fl_avg(l_osd_sop_w_lat, "subop_w_latency");      // replicated write latency
  osd_plb.add_u64_counter(l_osd_sop_pull,     "subop_pull");       // pull request
  osd_plb.add_fl_avg(l_osd_sop_pull_lat, "subop_pull_latency");
  osd_plb.add_u64_counter(l_osd_sop_push,     "subop_push");       // push (write)
  osd_plb.add_u64_counter(l_osd_sop_push_inb, "subop_push_in_bytes");
  osd_plb.add_fl_avg(l_osd_sop_push_lat, "subop_push_latency");

  osd_plb.add_u64_counter(l_osd_pull,      "pull");       // pull requests sent
  osd_plb.add_u64_counter(l_osd_push,      "push");       // push messages
  osd_plb.add_u64_counter(l_osd_push_outb, "push_out_bytes");  // pushed bytes

  osd_plb.add_u64_counter(l_osd_rop, "recovery_ops");       // recovery ops (started)

  osd_plb.add_fl(l_osd_loadavg, "loadavg");
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

int OSD::shutdown()
{
  g_ceph_context->_conf->set_val("debug_osd", "100");
  g_ceph_context->_conf->set_val("debug_journal", "100");
  g_ceph_context->_conf->set_val("debug_filestore", "100");
  g_ceph_context->_conf->set_val("debug_ms", "100");
  g_ceph_context->_conf->apply_changes(NULL);
  
  derr << "OSD::shutdown" << dendl;

  state = STATE_STOPPING;

  timer.shutdown();

  watch_lock.Lock();
  watch_timer.shutdown();
  watch_lock.Unlock();

  heartbeat_lock.Lock();
  heartbeat_stop = true;
  heartbeat_cond.Signal();
  heartbeat_lock.Unlock();
  heartbeat_thread.join();

  command_tp.stop();

  // finish ops
  op_wq.drain();
  dout(10) << "no ops" << dendl;

  recovery_tp.stop();
  dout(10) << "recovery tp stopped" << dendl;
  op_tp.stop();
  dout(10) << "op tp stopped" << dendl;

  // pause _new_ disk work first (to avoid racing with thread pool),
  disk_tp.pause_new();
  dout(10) << "disk tp paused (new), kicking all pgs" << dendl;

  // then kick all pgs,
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    dout(20) << " kicking pg " << p->first << dendl;
    p->second->lock();
    p->second->kick();
    p->second->unlock();
  }
  dout(20) << " kicked all pgs" << dendl;

  // then stop thread.
  disk_tp.stop();
  dout(10) << "disk tp stopped" << dendl;

  // tell pgs we're shutting down
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    p->second->lock();
    p->second->on_shutdown();
    p->second->unlock();
  }

  osd_lock.Unlock();
  store->sync();
  store->flush();
  osd_lock.Lock();

  // zap waiters (bleh, this is messy)
  finished_lock.Lock();
  finished.clear();
  finished_lock.Unlock();

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

  // flush data to disk
  osd_lock.Unlock();
  dout(10) << "sync" << dendl;
  store->sync();
  r = store->umount();
  delete store;
  store = 0;
  dout(10) << "sync done" << dendl;
  osd_lock.Lock();

  clear_pg_stat_queue();

  // close pgs
  for (hash_map<pg_t, PG*>::iterator p = pg_map.begin();
       p != pg_map.end();
       p++) {
    PG *pg = p->second;
    pg->put();
  }
  pg_map.clear();

  client_messenger->shutdown();
  cluster_messenger->shutdown();
  hbin_messenger->shutdown();
  hbout_messenger->shutdown();

  monc->shutdown();

  delete watch;

  clear_map_cache();
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



void OSD::clear_temp()
{
  dout(10) << "clear_temp" << dendl;

  vector<hobject_t> objects;
  store->collection_list(coll_t::TEMP_COLL, objects);

  dout(10) << objects.size() << " objects" << dendl;
  if (objects.empty())
    return;

  // delete them.
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  for (vector<hobject_t>::iterator p = objects.begin();
       p != objects.end();
       p++)
    t->collection_remove(coll_t::TEMP_COLL, *p);
  int r = store->queue_transaction(NULL, t);
  assert(r == 0);
  store->sync_and_flush();
  dout(10) << "done" << dendl;
}


// ======================================================
// PG's

PGPool* OSD::_get_pool(int id)
{
  map<int, PGPool*>::iterator pm = pool_map.find(id);
  PGPool *p = (pm == pool_map.end()) ? NULL : pm->second;

  if (!p) {
    if (!osdmap->have_pg_pool(id)) {
      dout(5) << __func__ << ": the OSDmap does not contain a PG pool with id = "
	      << id << dendl;
      return NULL;
    }

    p = new PGPool(id, osdmap->get_pool_name(id),
		   osdmap->get_pg_pool(id)->auid);
    pool_map[id] = p;
    p->get();
    
    const pg_pool_t *pi = osdmap->get_pg_pool(id);
    p->info = *pi;
    p->snapc = pi->get_snap_context();

    pi->build_removed_snaps(p->cached_removed_snaps);
  }
  dout(10) << "_get_pool " << p->id << " " << p->num_pg << " -> " << (p->num_pg+1) << dendl;
  p->num_pg++;
  return p;
}

void OSD::_put_pool(PGPool *p)
{
  dout(10) << "_put_pool " << p->id << " " << p->num_pg
	   << " -> " << (p->num_pg-1) << dendl;
  assert(p->num_pg > 0);
  p->num_pg--;
  if (!p->num_pg) {
    pool_map.erase(p->id);
    p->put();
  }
}

PG *OSD::_open_lock_pg(pg_t pgid, bool no_lockdep_check)
{
  assert(osd_lock.is_locked());

  dout(10) << "_open_lock_pg " << pgid << dendl;
  PGPool *pool = _get_pool(pgid.pool());
  assert(pool);

  // create
  PG *pg;
  hobject_t logoid = make_pg_log_oid(pgid);
  hobject_t infooid = make_pg_biginfo_oid(pgid);
  if (osdmap->get_pg_type(pgid) == pg_pool_t::TYPE_REP)
    pg = new ReplicatedPG(this, pool, pgid, logoid, infooid);
  else 
    assert(0);

  assert(pg_map.count(pgid) == 0);
  pg_map[pgid] = pg;

  pg->lock(no_lockdep_check); // always lock.
  pg->get();  // because it's in pg_map
  return pg;
}


PG *OSD::_create_lock_pg(pg_t pgid, ObjectStore::Transaction& t)
{
  assert(osd_lock.is_locked());
  dout(10) << "_create_lock_pg " << pgid << dendl;

  if (pg_map.count(pgid)) 
    dout(0) << "_create_lock_pg on " << pgid << ", already have " << *pg_map[pgid] << dendl;

  // open
  PG *pg = _open_lock_pg(pgid);

  // create collection
  assert(!store->collection_exists(coll_t(pgid)));
  t.create_collection(coll_t(pgid));

  pg->write_info(t);
  pg->write_log(t);

  return pg;
}

PG *OSD::_create_lock_new_pg(pg_t pgid, vector<int>& acting, ObjectStore::Transaction& t,
                             PG::Info::History history)
{
  assert(osd_lock.is_locked());
  dout(20) << "_create_lock_new_pg pgid " << pgid << " -> " << acting << dendl;
  assert(whoami == acting[0]);
  assert(pg_map.count(pgid) == 0);

  PG *pg = _open_lock_pg(pgid, true);

  assert(!store->collection_exists(coll_t(pgid)));
  t.create_collection(coll_t(pgid));

  pg->set_role(0);
  pg->acting.swap(acting);
  pg->up = pg->acting;
  pg->info.history = history;
  /* This is weird, but all the peering code needs last_epoch_start
   * to be less than same_interval_since. Make it so!
   * This is easier to deal with if you remember that the PG, while
   * now created in memory, still hasn't peered and started -- and
   * the map epoch could change before that happens! */
  pg->info.history.last_epoch_started = history.epoch_created - 1;

  pg->write_info(t);
  pg->write_log(t);
  
  reg_last_pg_scrub(pg->info.pgid, pg->info.history.last_scrub_stamp);

  dout(7) << "_create_lock_new_pg " << *pg << dendl;
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
  assert(pg_map.count(pgid));
  PG *pg = pg_map[pgid];
  pg->lock();
  return pg;
}

PG *OSD::lookup_lock_raw_pg(pg_t pgid)
{
  Mutex::Locker l(osd_lock);
  if (osdmap->have_pg_pool(pgid.pool())) {
    pgid = osdmap->raw_pg_to_pg(pgid);
  }
  if (!_have_pg(pgid)) {
    return NULL;
  }
  PG *pg = _lookup_lock_pg(pgid);
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

  for (vector<coll_t>::iterator it = ls.begin();
       it != ls.end();
       it++) {
    pg_t pgid;
    snapid_t snap;
    if (!it->is_pg(pgid, snap)) {
      dout(10) << "load_pgs skipping non-pg " << *it << dendl;
      continue;
    }
    if (snap != CEPH_NOSNAP) {
      dout(10) << "load_pgs skipping snapped dir " << *it
	       << " (pg " << pgid << " snap " << snap << ")" << dendl;
      continue;
    }

    if (!osdmap->have_pg_pool(pgid.pool())) {
      dout(10) << __func__ << ": skipping PG " << pgid << " because we don't have pool "
	       << pgid.pool() << dendl;
      continue;
    }

    PG *pg = _open_lock_pg(pgid);

    // read pg state, log
    pg->read_state(store);

    reg_last_pg_scrub(pg->info.pgid, pg->info.history.last_scrub_stamp);

    // generate state for current mapping
    osdmap->pg_to_up_acting_osds(pgid, pg->up, pg->acting);
    int role = osdmap->calc_pg_role(whoami, pg->acting);
    pg->set_role(role);

    PG::RecoveryCtx rctx(0, 0, 0, 0, 0);
    pg->handle_loaded(&rctx);

    dout(10) << "load_pgs loaded " << *pg << " " << pg->log << dendl;
    pg->unlock();
  }
  dout(10) << "load_pgs done" << dendl;
}
 

/*
 * look up a pg.  if we have it, great.  if not, consider creating it IF the pg mapping
 * hasn't changed since the given epoch and we are the primary.
 */
PG *OSD::get_or_create_pg(const PG::Info& info, epoch_t epoch, int from, int& created,
			  bool primary,
			  ObjectStore::Transaction **pt,
			  C_Contexts **pfin)
{
  PG *pg;

  if (!_have_pg(info.pgid)) {
    // same primary?
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(info.pgid, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    PG::Info::History history = info.history;
    project_pg_history(info.pgid, history, epoch, up, acting);

    if (epoch < history.same_interval_since) {
      dout(10) << "get_or_create_pg " << info.pgid << " acting changed in "
	       << history.same_interval_since << " (msg from " << epoch << ")" << dendl;
      return NULL;
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
    *pt = new ObjectStore::Transaction;
    *pfin = new C_Contexts(g_ceph_context);
    if (create) {
      pg = _create_lock_new_pg(info.pgid, acting, **pt, history);
    } else {
      pg = _create_lock_pg(info.pgid, **pt);
      pg->acting.swap(acting);
      pg->up.swap(up);
      pg->set_role(role);
      pg->info.history = history;
      reg_last_pg_scrub(pg->info.pgid, pg->info.history.last_scrub_stamp);
      pg->clear_primary_state();  // yep, notably, set hml=false
      pg->write_info(**pt);
      pg->write_log(**pt);
    }
      
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
    *pt = new ObjectStore::Transaction;
    *pfin = new C_Contexts(g_ceph_context);
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
    int added = 0;
    for (unsigned i=0; i<acting.size(); i++)
      if (acting[i] != whoami && osdmap->is_up(acting[i])) {
	pset.insert(acting[i]);
	added++;
      }
    if (!added && acting.size()) {
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
void OSD::project_pg_history(pg_t pgid, PG::Info::History& h, epoch_t from,
			     vector<int>& currentup, vector<int>& currentacting)
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

void OSD::update_osd_stat()
{
  // fill in osd stats too
  struct statfs stbuf;
  store->statfs(&stbuf);

  osd_stat.kb = stbuf.f_blocks * stbuf.f_bsize / 1024;
  osd_stat.kb_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize / 1024;
  osd_stat.kb_avail = stbuf.f_bavail * stbuf.f_bsize / 1024;

  osd_stat.hb_in.clear();
  for (map<int,epoch_t>::iterator p = heartbeat_from.begin(); p != heartbeat_from.end(); p++)
    osd_stat.hb_in.push_back(p->first);
  osd_stat.hb_out.clear();
  for (map<int,epoch_t>::iterator p = heartbeat_to.begin(); p != heartbeat_to.end(); p++)
    osd_stat.hb_out.push_back(p->first);

  dout(20) << "update_osd_stat " << osd_stat << dendl;
}

void OSD::_add_heartbeat_source(int p, map<int, epoch_t>& old_from, map<int, utime_t>& old_from_stamp,
				map<int,Connection*>& old_con)
{
  if (p == whoami)
    return;
  if (heartbeat_from.count(p))
    return;

  heartbeat_from[p] = osdmap->get_epoch();
  Connection *con = hbin_messenger->get_connection(osdmap->get_hb_inst(p));
  heartbeat_from_con[p] = con;
  if (old_from_stamp.count(p) && old_from.count(p) && old_con[p] == con) {
    // have a stamp _AND_ i'm not new to the set
    heartbeat_from_stamp[p] = old_from_stamp[p];
  } else {
    dout(10) << "update_heartbeat_peers: new _from osd." << p
	     << " " << con->get_peer_addr() << dendl;
    heartbeat_from_stamp[p] = ceph_clock_now(g_ceph_context);  
    MOSDPing *m = new MOSDPing(osdmap->get_fsid(), 0, heartbeat_epoch,
			       MOSDPing::START_HEARTBEAT);
    hbin_messenger->send_message(m, con);
  }
}

void OSD::update_heartbeat_peers()
{
  assert(osd_lock.is_locked());
  heartbeat_lock.Lock();

  // filter heartbeat_from_stamp to only include osds that remain in
  // heartbeat_from.
  map<int, utime_t> old_from_stamp;
  old_from_stamp.swap(heartbeat_from_stamp);

  map<int, epoch_t> old_from;
  map<int, Connection*> old_con;
  old_from.swap(heartbeat_from);
  old_con.swap(heartbeat_from_con);

  heartbeat_epoch = osdmap->get_epoch();

  // build heartbeat from set
  for (hash_map<pg_t, PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    PG *pg = i->second;

    // replicas (new and old) ping primary.
    if (pg->get_role() == 0) {
      assert(pg->acting[0] == whoami);
      for (unsigned i=0; i<pg->acting.size(); i++)
	_add_heartbeat_source(pg->acting[i], old_from, old_from_stamp, old_con);
      for (unsigned i=0; i<pg->up.size(); i++)
	_add_heartbeat_source(pg->up[i], old_from, old_from_stamp, old_con);
      for (map<int,PG::Info>::iterator p = pg->peer_info.begin(); p != pg->peer_info.end(); ++p)
	if (osdmap->is_up(p->first))
	  _add_heartbeat_source(p->first, old_from, old_from_stamp, old_con);
    }
  }

  for (map<int,epoch_t>::iterator p = old_from.begin();
       p != old_from.end();
       p++) {
    assert(old_con.count(p->first));
    Connection *con = old_con[p->first];

    if (heartbeat_from.count(p->first) && heartbeat_from_con[p->first] == con) {
      con->put();
      continue;
    }

    // share latest map with this peer, just to be nice.
    if (osdmap->is_up(p->first) && is_active()) {
      dout(10) << "update_heartbeat_peers: sharing map with old _from peer osd." << p->first << dendl;
      _share_map_outgoing(osdmap->get_cluster_inst(p->first));
    }

    dout(10) << "update_heartbeat_peers: will mark down old _from peer osd." << p->first
	     << " " << con->get_peer_addr()
	     << " as of " << p->second << dendl;
    
    if (!osdmap->is_up(p->first) && is_active()) {
      dout(10) << "update_heartbeat_peers: telling old peer osd." << p->first
	       << " " << old_con[p->first]->get_peer_addr()
	       << " they are down" << dendl;
      hbin_messenger->send_message(new MOSDPing(osdmap->get_fsid(), heartbeat_epoch,
						heartbeat_epoch,
						MOSDPing::YOU_DIED), con);
      hbin_messenger->mark_disposable(con);
      hbin_messenger->mark_down_on_empty(con);
    } else {
      // tell them to stop sending heartbeats
      hbin_messenger->send_message(new MOSDPing(osdmap->get_fsid(), 0, heartbeat_epoch,
						MOSDPing::STOP_HEARTBEAT), con);
    }
    if (!osdmap->is_up(p->first)) {
      forget_peer_epoch(p->first, osdmap->get_epoch());
    }
    con->put();
  }

  dout(10) << "update_heartbeat_peers: hb   to: " << heartbeat_to << dendl;
  dout(10) << "update_heartbeat_peers: hb from: " << heartbeat_from << dendl;

  heartbeat_lock.Unlock();
}

void OSD::reset_heartbeat_peers()
{
  dout(10) << "reset_heartbeat_peers" << dendl;
  heartbeat_lock.Lock();
  heartbeat_to.clear();
  heartbeat_from.clear();
  heartbeat_from_stamp.clear();
  while (!heartbeat_to_con.empty()) {
    heartbeat_to_con.begin()->second->put();
    heartbeat_to_con.erase(heartbeat_to_con.begin());
  }
  while (!heartbeat_from_con.empty()) {
    heartbeat_from_con.begin()->second->put();
    heartbeat_from_con.erase(heartbeat_from_con.begin());
  }
  failure_queue.clear();
  heartbeat_lock.Unlock();

}

void OSD::handle_osd_ping(MOSDPing *m)
{
  if (superblock.fsid != m->fsid) {
    dout(20) << "handle_osd_ping from " << m->get_source_inst()
	     << " bad fsid " << m->fsid << " != " << superblock.fsid << dendl;
    m->put();
    return;
  }

  heartbeat_lock.Lock();
  int from = m->get_source().num();
  bool locked = map_lock.try_get_read();
  
  switch (m->op) {
  case MOSDPing::START_HEARTBEAT:
    if (heartbeat_to.count(from)) {
      if (heartbeat_to_con[from] != m->get_connection()) {
	// different connection
	if (heartbeat_to[from] > m->peer_as_of_epoch) {
	  dout(5) << "handle_osd_ping marking down peer " << m->get_source_inst()
		  << " after old start message from epoch " << m->peer_as_of_epoch
		  << " <= current " << heartbeat_to[from] << dendl;
	  hbout_messenger->mark_down(m->get_connection());
	} else {
	  dout(5) << "handle_osd_ping replacing old peer "
		  << heartbeat_to_con[from]->get_peer_addr()
		  << " epoch " << heartbeat_to[from]
		  << " with new peer " << m->get_source_inst()
		  << " epoch " << m->peer_as_of_epoch
		  << dendl;
	  hbout_messenger->mark_down(heartbeat_to_con[from]);
	  heartbeat_to_con[from]->put();
	  
	  // remember new connection
	  heartbeat_to[from] = m->peer_as_of_epoch;
	  heartbeat_to_con[from] = m->get_connection();
	  heartbeat_to_con[from]->get();
	}
      } else {
	// same connection
	dout(5) << "handle_osd_ping dup peer " << m->get_source_inst()
		<< " request for heartbeats as_of " << m->peer_as_of_epoch
		<< ", same connection" << dendl;
	heartbeat_to[from] = m->peer_as_of_epoch;
      }
    } else {
      // new
      dout(5) << "handle_osd_ping peer " << m->get_source_inst()
	      << " requesting heartbeats as_of " << m->peer_as_of_epoch << dendl;
      heartbeat_to[from] = m->peer_as_of_epoch;
      heartbeat_to_con[from] = m->get_connection();
      heartbeat_to_con[from]->get();
      
      if (locked && m->map_epoch && is_active()) {
	_share_map_incoming(m->get_source_inst(), m->map_epoch,
			    (Session*) m->get_connection()->get_priv());
      }
    }
    break;

  case MOSDPing::STOP_HEARTBEAT:
    {
      if (heartbeat_to.count(from) && heartbeat_to_con[from] == m->get_connection()) {
	dout(5) << "handle_osd_ping peer " << m->get_source_inst()
		<< " stopping heartbeats as_of " << m->peer_as_of_epoch << dendl;
	heartbeat_to.erase(from);
	//hbout_messenger->mark_down(heartbeat_to_con[from]);
	heartbeat_to_con[from]->put();
	heartbeat_to_con.erase(from);
      } else {
	dout(5) << "handle_osd_ping peer " << m->get_source_inst()
		<< " ignoring stop request as_of " << m->peer_as_of_epoch << dendl;
      }
    }
    break;

  case MOSDPing::HEARTBEAT:
    if (heartbeat_from.count(from) && heartbeat_from_con[from] == m->get_connection()) {
      dout(20) << "handle_osd_ping " << m->get_source_inst() << dendl;

      note_peer_epoch(from, m->map_epoch);
      if (locked && is_active())
	_share_map_outgoing(osdmap->get_cluster_inst(from));
      
      heartbeat_from_stamp[from] = ceph_clock_now(g_ceph_context);  // don't let _my_ lag interfere.
      
      // remove from failure lists if needed
      if (failure_pending.count(from)) {
	send_still_alive(failure_pending[from]);
	failure_pending.erase(from);
      }
      failure_queue.erase(from);
    } else {
      dout(10) << "handle_osd_ping ignoring " << m->get_source_inst() << dendl;
    }
    break;

  case MOSDPing::YOU_DIED:
    dout(10) << "handle_osd_ping " << m->get_source_inst() << " says i am down in " << m->map_epoch
	     << dendl;
    monc->sub_want("osdmap", m->map_epoch, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
    break;
  }

  if (locked) 
    map_lock.put_read();

  heartbeat_lock.Unlock();
  m->put();

}

void OSD::heartbeat_entry()
{
  heartbeat_lock.Lock();
  while (!heartbeat_stop) {
    heartbeat();

    double wait = .5 + ((float)(rand() % 10)/10.0) * (float)g_conf->osd_heartbeat_interval;
    utime_t w;
    w.set_from_double(wait);
    dout(30) << "heartbeat_entry sleeping for " << wait << dendl;
    heartbeat_cond.WaitInterval(g_ceph_context, heartbeat_lock, w);
    dout(30) << "heartbeat_entry woke up" << dendl;
  }
  heartbeat_lock.Unlock();
}

void OSD::heartbeat_check()
{
  assert(heartbeat_lock.is_locked());
  // we should also have map_lock rdlocked.

  // check for incoming heartbeats (move me elsewhere?)
  utime_t grace = ceph_clock_now(g_ceph_context);
  grace -= g_conf->osd_heartbeat_grace;
  for (map<int, epoch_t>::iterator p = heartbeat_from.begin();
       p != heartbeat_from.end();
       p++) {
    if (heartbeat_from_stamp.count(p->first) &&
	heartbeat_from_stamp[p->first] < grace) {
      derr << "heartbeat_check: no heartbeat from osd." << p->first
	   << " since " << heartbeat_from_stamp[p->first]
	   << " (cutoff " << grace << ")" << dendl;
      queue_failure(p->first);

    }
  }
}

void OSD::heartbeat()
{
  utime_t now = ceph_clock_now(g_ceph_context);

  dout(30) << "heartbeat" << dendl;

  if (got_sigterm) {
    derr << "got SIGTERM, shutting down" << dendl;
    Message *m = new MGenericMessage(CEPH_MSG_SHUTDOWN);
    m->set_priority(CEPH_MSG_PRIO_HIGHEST);
    cluster_messenger->send_message(m, cluster_messenger->get_myinst());
    return;
  }

  // get CPU load avg
  double loadavgs[1];
  if (getloadavg(loadavgs, 1) == 1)
    logger->fset(l_osd_loadavg, loadavgs[0]);

  dout(30) << "heartbeat checking stats" << dendl;

  // refresh stats?
  {
    Mutex::Locker lock(stat_lock);
    update_osd_stat();
  }

  dout(5) << "heartbeat: " << osd_stat << dendl;

  bool map_locked = map_lock.try_get_read();
  dout(30) << "heartbeat map_locked=" << map_locked << dendl;

  // send heartbeats
  for (map<int, epoch_t>::iterator i = heartbeat_to.begin();
       i != heartbeat_to.end();
       i++) {
    int peer = i->first;
    dout(30) << "heartbeat allocating ping for osd." << peer << dendl;
    Message *m = new MOSDPing(osdmap->get_fsid(),
			      map_locked ? osdmap->get_epoch():0, 
			      i->second, MOSDPing::HEARTBEAT);
    m->set_priority(CEPH_MSG_PRIO_HIGH);
    dout(30) << "heartbeat sending ping to osd." << peer << dendl;
    hbout_messenger->send_message(m, heartbeat_to_con[peer]);
  }

  if (map_locked) {
    dout(30) << "heartbeat check" << dendl;
    heartbeat_check();
  } else {
    dout(30) << "heartbeat no map_lock, no check" << dendl;
  }

  if (logger) {
    logger->set(l_osd_hb_to, heartbeat_to.size());
    logger->set(l_osd_hb_from, heartbeat_from.size());
  }
  
  // hmm.. am i all alone?
  dout(30) << "heartbeat lonely?" << dendl;
  if (heartbeat_from.empty() || heartbeat_to.empty()) {
    if (now - last_mon_heartbeat > g_conf->osd_mon_heartbeat_interval && is_active()) {
      last_mon_heartbeat = now;
      dout(10) << "i have no heartbeat peers; checking mon for new map" << dendl;
      monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
      monc->renew_subs();
    }
  }

  if (map_locked) {
    dout(30) << "heartbeat put map_lock" << dendl;
    map_lock.put_read();
  }
  dout(30) << "heartbeat done" << dendl;
}


// =========================================

void OSD::tick()
{
  assert(osd_lock.is_locked());
  dout(5) << "tick" << dendl;

  logger->set(l_osd_buf, buffer::get_total_alloc());

  if (got_sigterm) {
    derr << "got SIGTERM, shutting down" << dendl;
    cluster_messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN),
			    cluster_messenger->get_myinst());
    return;
  }

  // periodically kick recovery work queue
  recovery_tp.kick();
  
  map_lock.get_read();

  if (scrub_should_schedule()) {
    sched_scrub();
  }

  heartbeat_lock.Lock();
  heartbeat_check();
  heartbeat_lock.Unlock();

  check_replay_queue();

  // mon report?
  utime_t now = ceph_clock_now(g_ceph_context);
  if (now - last_pg_stats_sent > g_conf->osd_mon_report_interval_max) {
    osd_stat_updated = true;
    do_mon_report();
  }
  else if (now - last_mon_report > g_conf->osd_mon_report_interval_min) {
    do_mon_report();
  }

  // remove stray pgs?
  remove_list_lock.Lock();
  for (map<epoch_t, map<int, vector<pg_t> > >::iterator p = remove_list.begin();
       p != remove_list.end();
       p++)
    for (map<int, vector<pg_t> >::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      if (osdmap->is_up(q->first)) {
	MOSDPGRemove *m = new MOSDPGRemove(p->first, q->second);
	cluster_messenger->send_message(m, osdmap->get_cluster_inst(q->first));
      }
    }
  remove_list.clear();
  remove_list_lock.Unlock();

  map_lock.put_read();

  timer.add_event_after(1.0, new C_Tick(this));

  // only do waiters if dispatch() isn't currently running.  (if it is,
  // it'll do the waiters, and doing them here may screw up ordering
  // of op_queue vs handle_osd_map.)
  if (!dispatch_running) {
    dispatch_running = true;
    do_waiters();
    dispatch_running = false;
    dispatch_cond.Signal();
  }
}

// =========================================

void OSD::do_mon_report()
{
  dout(7) << "do_mon_report" << dendl;

  utime_t now(ceph_clock_now(g_ceph_context));
  last_mon_report = now;

  // do any pending reports
  send_alive();
  send_pg_temp();
  send_failures();
  send_pg_stats(now);
}

void OSD::ms_handle_connect(Connection *con)
{
  if (con->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
    Mutex::Locker l(osd_lock);
    dout(10) << "ms_handle_connect on mon" << dendl;
    if (is_booting()) {
      start_boot();
    } else {
      send_alive();
      send_pg_temp();
      send_failures();
      send_pg_stats(ceph_clock_now(g_ceph_context));
    }
  }
}

void OSD::put_object_context(void *_obc, pg_t pgid)
{
  ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)_obc;
  ReplicatedPG *pg = (ReplicatedPG *)lookup_lock_raw_pg(pgid);
  // If pg is being deleted, (which is the only case in which
  // it will be NULL) it will clean up its object contexts itself
  if (pg) {
    pg->put_object_context(obc);
    pg->unlock();
  }
}

void OSD::complete_notify(void *_notif, void *_obc)
{
  ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)_obc;
  Watch::Notification *notif = (Watch::Notification *)_notif;
  dout(0) << "got the last reply from pending watchers, can send response now" << dendl;
  MWatchNotify *reply = notif->reply;
  client_messenger->send_message(reply, notif->session->con);
  notif->session->put();
  notif->session->con->put();
  watch->remove_notification(notif);
  if (notif->timeout)
    watch_timer.cancel_event(notif->timeout);
  map<Watch::Notification *, bool>::iterator iter = obc->notifs.find(notif);
  if (iter != obc->notifs.end())
    obc->notifs.erase(iter);
  delete notif;
}

void OSD::ack_notification(entity_name_t& name, void *_notif, void *_obc, ReplicatedPG *pg)
{
  assert(watch_lock.is_locked());
  pg->assert_locked();
  Watch::Notification *notif = (Watch::Notification *)_notif;
  if (watch->ack_notification(name, notif)) {
    complete_notify(notif, _obc);
    pg->put_object_context(static_cast<ReplicatedPG::ObjectContext *>(_obc));
  }
}

void OSD::handle_watch_timeout(void *obc,
			       ReplicatedPG *pg,
			       entity_name_t entity,
			       utime_t expire)
{
  pg->lock();
  pg->handle_watch_timeout(obc, entity, expire);
  pg->unlock();
  pg->put();
}

void OSD::disconnect_session_watches(Session *session)
{
  // get any watched obc's
  map<ReplicatedPG::ObjectContext *, pg_t> obcs;
  watch_lock.Lock();
  for (map<void *, pg_t>::iterator iter = session->watches.begin(); iter != session->watches.end(); ++iter) {
    ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)iter->first;
    obcs[obc] = iter->second;
  }
  watch_lock.Unlock();

  for (map<ReplicatedPG::ObjectContext *, pg_t>::iterator oiter = obcs.begin(); oiter != obcs.end(); ++oiter) {
    ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)oiter->first;
    dout(0) << "obc=" << (void *)obc << dendl;

    ReplicatedPG *pg = static_cast<ReplicatedPG *>(lookup_lock_raw_pg(oiter->second));
    assert(pg);
    obc->lock.Lock();
    watch_lock.Lock();
    /* NOTE! fix this one, should be able to just lookup entity name,
       however, we currently only keep EntityName on the session and not
       entity_name_t. */
    map<entity_name_t, Session *>::iterator witer = obc->watchers.begin();
    while (1) {
      while (witer != obc->watchers.end() && witer->second == session) {
        dout(0) << "removing watching session entity_name=" << session->entity_name
		<< " from " << obc->obs.oi << dendl;
	entity_name_t entity = witer->first;
	watch_info_t& w = obc->obs.oi.watchers[entity];
	utime_t expire = ceph_clock_now(g_ceph_context);
	expire += w.timeout_seconds;
	pg->register_unconnected_watcher(obc, entity, expire);
	dout(10) << " disconnected watch " << w << " by " << entity << " session " << session
		 << ", expires " << expire << dendl;
        obc->watchers.erase(witer++);
	session->put();
      }
      if (witer == obc->watchers.end())
        break;
      ++witer;
    }
    watch_lock.Unlock();
    obc->lock.Unlock();
    pg->put_object_context(obc);
    /* now drop a reference to that obc */
    pg->unlock();
  }
}

bool OSD::ms_handle_reset(Connection *con)
{
  dout(0) << "OSD::ms_handle_reset()" << dendl;
  OSD::Session *session = (OSD::Session *)con->get_priv();
  if (!session)
    return false;
  dout(0) << "OSD::ms_handle_reset() s=" << (void *)session << dendl;
  disconnect_session_watches(session);
  session->put();
  return true;
}

void OSD::handle_notify_timeout(void *_notif)
{
  assert(watch_lock.is_locked());
  Watch::Notification *notif = (Watch::Notification *)_notif;
  dout(0) << "OSD::handle_notify_timeout notif " << notif->id << dendl;

  ReplicatedPG::ObjectContext *obc = (ReplicatedPG::ObjectContext *)notif->obc;

  complete_notify(_notif, obc);
  watch_lock.Unlock(); /* drop lock to change locking order */

  put_object_context(obc, notif->pgid);
  watch_lock.Lock();
  /* exiting with watch_lock held */
}

struct C_OSD_GetVersion : public Context {
  OSD *osd;
  uint64_t oldest, newest;
  C_OSD_GetVersion(OSD *o) : osd(o), oldest(0), newest(0) {}
  void finish(int r) {
    if (r >= 0)
      osd->_got_boot_version(oldest, newest);
  }
};

void OSD::start_boot()
{
  dout(10) << "start_boot - have maps " << superblock.oldest_map << ".." << superblock.newest_map << dendl;
  C_OSD_GetVersion *c = new C_OSD_GetVersion(this);
  monc->get_version("osdmap", &c->newest, &c->oldest, c);
}

void OSD::_got_boot_version(epoch_t oldest, epoch_t newest)
{
  Mutex::Locker l(osd_lock);
  dout(10) << "_got_boot_version mon has osdmaps " << oldest << ".." << newest << dendl;

  // if our map within recent history, try to add ourselves to the osdmap.
  if (osdmap->get_epoch() >= oldest - 1 &&
      osdmap->get_epoch() < newest + g_conf->osd_map_message_max) {
    send_boot();
    return;
  }
  
  // get all the latest maps
  if (osdmap->get_epoch() > oldest)
    monc->sub_want("osdmap", osdmap->get_epoch(), CEPH_SUBSCRIBE_ONETIME);
  else
    monc->sub_want("osdmap", oldest - 1, CEPH_SUBSCRIBE_ONETIME);
  monc->renew_subs();
}

void OSD::send_boot()
{
  dout(10) << "send_boot" << dendl;
  entity_addr_t cluster_addr = cluster_messenger->get_myaddr();
  if (cluster_addr.is_blank_ip()) {
    int port = cluster_addr.get_port();
    cluster_addr = client_messenger->get_myaddr();
    cluster_addr.set_port(port);
    cluster_messenger->set_ip(cluster_addr);
    dout(10) << " assuming cluster_addr ip matches client_addr" << dendl;
  }
  entity_addr_t hb_addr = hbout_messenger->get_myaddr();
  if (hb_addr.is_blank_ip()) {
    int port = hb_addr.get_port();
    hb_addr = cluster_addr;
    hb_addr.set_port(port);
    hbout_messenger->set_ip(hb_addr);
    dout(10) << " assuming hb_addr ip matches cluster_addr" << dendl;
  }
  MOSDBoot *mboot = new MOSDBoot(superblock, hb_addr, cluster_addr);
  dout(10) << " client_addr " << client_messenger->get_myaddr()
	   << ", cluster_addr " << cluster_addr
	   << ", hb addr " << hb_addr
	   << dendl;
  monc->send_mon_message(mboot);
}

void OSD::queue_want_up_thru(epoch_t want)
{
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

void OSD::queue_want_pg_temp(pg_t pgid, vector<int>& want)
{
  pg_temp_wanted[pgid] = want;
}

void OSD::send_pg_temp()
{
  if (pg_temp_wanted.empty())
    return;
  dout(10) << "send_pg_temp " << pg_temp_wanted << dendl;
  MOSDPGTemp *m = new MOSDPGTemp(osdmap->get_epoch());
  m->pg_temp = pg_temp_wanted;
  monc->send_mon_message(m);
}

void OSD::send_failures()
{
  bool locked = false;
  if (!failure_queue.empty()) {
    heartbeat_lock.Lock();
    locked = true;
  }
  while (!failure_queue.empty()) {
    int osd = *failure_queue.begin();
    entity_inst_t i = osdmap->get_inst(osd);
    monc->send_mon_message(new MOSDFailure(monc->get_fsid(), i, osdmap->get_epoch()));
    failure_pending[osd] = i;
    failure_queue.erase(osd);
  }
  if (locked) heartbeat_lock.Unlock();
}

void OSD::send_still_alive(entity_inst_t i)
{
  MOSDFailure *m = new MOSDFailure(monc->get_fsid(), i, osdmap->get_epoch());
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

    MPGStats *m = new MPGStats(osdmap->get_fsid(), osdmap->get_epoch(), had_for);
    m->set_tid(++pg_stat_tid);
    m->osd_stat = cur_stat;

    xlist<PG*>::iterator p = pg_stat_queue.begin();
    while (!p.end()) {
      PG *pg = *p;
      ++p;
      if (!pg->is_primary()) {  // we hold map_lock; role is stable.
	pg->stat_queue_item.remove_myself();
	pg->put();
	continue;
      }
      pg->pg_stats_lock.Lock();
      if (pg->pg_stats_valid) {
	m->pg_stat[pg->info.pgid] = pg->pg_stats_stable;
	dout(25) << " sending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
      } else {
	dout(25) << " NOT sending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << ", not valid" << dendl;
      }
      pg->pg_stats_lock.Unlock();
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

  pg_stat_queue_lock.Lock();

  if (ack->get_tid() > pg_stat_tid_flushed) {
    pg_stat_tid_flushed = ack->get_tid();
    pg_stat_queue_cond.Signal();
  }

  xlist<PG*>::iterator p = pg_stat_queue.begin();
  while (!p.end()) {
    PG *pg = *p;
    ++p;

    if (ack->pg_stat.count(pg->info.pgid)) {
      eversion_t acked = ack->pg_stat[pg->info.pgid];
      pg->pg_stats_lock.Lock();
      if (acked == pg->pg_stats_stable.reported) {
	dout(25) << " ack on " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
	pg->stat_queue_item.remove_myself();
	pg->put();
      } else {
	dout(25) << " still pending " << pg->info.pgid << " " << pg->pg_stats_stable.reported
		 << " > acked " << acked << dendl;
      }
      pg->pg_stats_lock.Unlock();
    } else
      dout(30) << " still pending " << pg->info.pgid << " " << pg->pg_stats_stable.reported << dendl;
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

  // special case shutdown, since shutdown() stops the command_tp
  if (m->cmd[0] == "stop") {
    shutdown();
    m->put();
    return;
  }

  Command *c = new Command(m->cmd, m->get_tid(), m->get_data(), NULL);
  command_wq.queue(c);
  m->put();
}

void OSD::handle_command(MCommand *m)
{
  Connection *con = m->get_connection();
  Session *session = (Session *)con->get_priv();
  if (!session) {
    client_messenger->send_message(new MCommandReply(m, -EPERM), con);
    m->put();
    return;
  }

  OSDCaps& caps = session->caps;
  session->put();

  if (!caps.allow_all || m->get_source().is_mon()) {
    client_messenger->send_message(new MCommandReply(m, -EPERM), con);
    m->put();
    return;
  }

  // special case shutdown, since shutdown() stops the command_tp
  if (m->cmd[0] == "stop") {
    shutdown();
    m->put();
    return;
  }

  Command *c = new Command(m->cmd, m->get_tid(), m->get_data(), con);
  command_wq.queue(c);

  m->put();
}

void OSD::do_command(Connection *con, tid_t tid, vector<string>& cmd, bufferlist& data)
{
  int r = 0;
  ostringstream ss;
  bufferlist odata;

  dout(20) << "do_command tid " << tid << " " << cmd << dendl;

  if (cmd[0] == "injectargs") {
    if (cmd.size() < 2) {
      r = -EINVAL;
      ss << "ignoring empty injectargs";
      goto out;
    }
    osd_lock.Unlock();
    g_conf->injectargs(cmd[1], &ss);
    osd_lock.Lock();
  }

  else if (cmd[0] == "stop") {
    ss << "got shutdown";
    shutdown();
  }

  else if (cmd[0] == "bench") {
    uint64_t count = 1 << 30;  // 1gb
    uint64_t bsize = 4 << 20;
    if (cmd.size() > 1)
      bsize = atoll(cmd[1].c_str());
    if (cmd.size() > 2)
      count = atoll(cmd[2].c_str());
    
    bufferlist bl;
    bufferptr bp(bsize);
    bp.zero();
    bl.push_back(bp);

    ObjectStore::Transaction *cleanupt = new ObjectStore::Transaction;

    store->sync_and_flush();
    utime_t start = ceph_clock_now(g_ceph_context);
    for (uint64_t pos = 0; pos < count; pos += bsize) {
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

  else if (cmd.size() >= 1 && cmd[0] == "flush_pg_stats") {
    flush_pg_stats();
  }
  
  else if (cmd.size() == 3 && cmd[0] == "mark_unfound_lost") {
    pg_t pgid;
    if (!pgid.parse(cmd[1].c_str())) {
      ss << "can't parse pgid '" << cmd[1] << "'";
      r = -EINVAL;
      goto out;
    }
    int mode;
    if (cmd[2] == "revert")
      mode = PG::Log::Entry::LOST_REVERT;
    /*
    else if (cmd[2] == "mark")
      mode = PG::Log::Entry::LOST_MARK;
    else if (cmd[2] == "delete" || cmd[2] == "remove")
      mode = PG::Log::Entry::LOST_DELETE;
    */
    else {
      //ss << "mode must be mark|revert|delete";
      ss << "mode must be revert (mark|delete not yet implemented)";
      r = -EINVAL;
      goto out;
    }
    PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid) : NULL;
    if (!pg) {
      ss << "pg " << pgid << " not found";
      r = -ENOENT;
      goto out;
    }
    if (!pg->is_primary()) {
      ss << "pg " << pgid << " not primary";
      pg->unlock();
      r = -EINVAL;
      goto out;
    }
    int unfound = pg->missing.num_missing() - pg->missing_loc.size();
    if (!unfound) {
      ss << "pg " << pgid << " has no unfound objects";
      pg->unlock();
      r = -ENOENT;
      goto out;
    }
    if (!pg->all_unfound_are_queried_or_lost(pg->osd->osdmap)) {
      pg->unlock();
      ss << "pg " << pgid << " has " << unfound
	 << " objects but we haven't probed all sources, not marking lost despite command "
	 << cmd;
      r = -EINVAL;
      goto out;
    }
    ss << pgid << " has " << unfound
       << " objects unfound and apparently lost, marking";
    pg->mark_all_unfound_lost(mode);
    pg->unlock();
  }

  else if (cmd[0] == "heap") {
    if (ceph_using_tcmalloc()) {
      ceph_heap_profiler_handle_command(cmd, clog);
    } else {
      r = -EOPNOTSUPP;
      ss << "could not issue heap profiler command -- not using tcmalloc!";
    }
  }

  else if (cmd.size() > 1 && cmd[0] == "debug") {
    if (cmd.size() == 3 && cmd[1] == "dump_missing") {
      const string &file_name(cmd[2]);
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
	std::map<hobject_t, PG::Missing::item>::iterator mend = pg->missing.missing.end();
	std::map<hobject_t, PG::Missing::item>::iterator mi = pg->missing.missing.begin();
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
    else if (cmd.size() == 3 && cmd[1] == "kick_recovery_wq") {
      r = g_conf->set_val("osd_recovery_delay_start", cmd[2].c_str());
      if (r != 0) {
	ss << "kick_recovery_wq: error setting "
	   << "osd_recovery_delay_start to '" << cmd[2] << "': error "
	   << r;
	goto out;
      }
      g_conf->apply_changes(NULL);
      ss << "kicking recovery queue. set osd_recovery_delay_start "
	 << "to " << g_conf->osd_recovery_delay_start;
      defer_recovery_until = ceph_clock_now(g_ceph_context);
      defer_recovery_until += g_conf->osd_recovery_delay_start;
      recovery_wq.kick();
    }
  }

  else if (cmd[0] == "cpu_profiler") {
    cpu_profiler_handle_command(cmd, clog);
  }

  else if (cmd[0] == "dump_pg_recovery_stats") {
    stringstream s;
    pg_recovery_stats.dump(s);
    ss << "dump pg recovery stats: " << s.str();
  }

  else if (cmd[0] == "reset_pg_recovery_stats") {
    ss << "reset pg recovery stats";
    pg_recovery_stats.reset();
  }

  else {
    ss << "unrecognized command! " << cmd;
    r = -EINVAL;
  }

 out:
  string rs = ss.str();
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


bool OSD::_share_map_incoming(const entity_inst_t& inst, epoch_t epoch,
			      Session* session)
{
  bool shared = false;
  dout(20) << "_share_map_incoming " << inst << " " << epoch << dendl;
  //assert(osd_lock.is_locked());

  assert(is_active());

  // does client have old map?
  if (inst.name.is_client()) {
    bool sendmap = epoch < osdmap->get_epoch();
    if (sendmap && session) {
      if ( session->last_sent_epoch < osdmap->get_epoch() ) {
	session->last_sent_epoch = osdmap->get_epoch();
      }
      else {
	sendmap = false; //we don't need to send it out again
	dout(15) << inst.name << " already sent incremental to update from epoch "<< epoch << dendl;
      }
    }
    if (sendmap) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      send_incremental_map(epoch, inst);
      shared = true;
    }
  }

  // does peer have old map?
  if (inst.name.is_osd() &&
      osdmap->is_up(inst.name.num()) &&
      (osdmap->get_cluster_inst(inst.name.num()) == inst ||
       osdmap->get_hb_inst(inst.name.num()) == inst)) {
    // remember
    epoch_t has = note_peer_epoch(inst.name.num(), epoch);

    // share?
    if (has < osdmap->get_epoch()) {
      dout(10) << inst.name << " has old map " << epoch << " < " << osdmap->get_epoch() << dendl;
      note_peer_epoch(inst.name.num(), osdmap->get_epoch());
      send_incremental_map(epoch, osdmap->get_cluster_inst(inst.name.num()));
      shared = true;
    }
  }

  if (session)
    session->put();
  return shared;
}


void OSD::_share_map_outgoing(const entity_inst_t& inst) 
{
  assert(inst.name.is_osd());

  int peer = inst.name.num();

  assert(is_active());

  // send map?
  epoch_t pe = get_peer_epoch(peer);
  if (pe) {
    if (pe < osdmap->get_epoch()) {
      send_incremental_map(pe, inst);
      note_peer_epoch(peer, osdmap->get_epoch());
    } else
      dout(20) << "_share_map_outgoing " << inst << " already has epoch " << pe << dendl;
  } else {
    dout(20) << "_share_map_outgoing " << inst << " don't know epoch, doing nothing" << dendl;
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
    handle_osd_ping((MOSDPing*)m);
    break;

  default:
    return false;
  }

  return true;
}

bool OSD::ms_dispatch(Message *m)
{
  // lock!
  osd_lock.Lock();
  while (dispatch_running) {
    dout(10) << "ms_dispatch waiting for other dispatch thread to complete" << dendl;
    dispatch_cond.Wait(osd_lock);
  }
  dispatch_running = true;

  do_waiters();
  _dispatch(m);
  do_waiters();

  dispatch_running = false;
  
  // no need to signal here, since tick() doesn't wait.
  //dispatch_cond.Signal();

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
			       bool& isvalid)
{
  AuthAuthorizeHandler *authorize_handler = authorize_handler_registry->get_handler(protocol);
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
						 authorizer_data, authorizer_reply, name, global_id, caps_info, &auid);

  dout(10) << "OSD::ms_verify_authorizer name=" << name << " auid=" << auid << dendl;

  if (isvalid) {
    Session *s = (Session *)con->get_priv();
    if (!s) {
      s = new Session;
      con->set_priv(s->get());
      s->con = con;
      dout(10) << " new session " << s << " con=" << s->con << " addr=" << s->con->get_peer_addr() << dendl;
    }

    s->caps.set_allow_all(caps_info.allow_all);
    s->caps.set_auid(auid);
    s->caps.set_peer_type(peer_type);
 
    if (caps_info.caps.length() > 0) {
      bufferlist::iterator iter = caps_info.caps.begin();
      s->caps.parse(iter);
      dout(10) << " session " << s << " has caps " << s->caps << dendl;
    }
    
    s->put();
  }
  return true;
};


void OSD::do_waiters()
{
  assert(osd_lock.is_locked());
  
  finished_lock.Lock();
  if (finished.empty()) {
    finished_lock.Unlock();
  } else {
    list<Message*> waiting;
    waiting.splice(waiting.begin(), finished);

    finished_lock.Unlock();
    
    dout(2) << "do_waiters -- start" << dendl;
    for (list<Message*>::iterator it = waiting.begin();
         it != waiting.end();
         it++)
      _dispatch(*it);
    dout(2) << "do_waiters -- finish" << dendl;
  }
}


void OSD::_dispatch(Message *m)
{
  assert(osd_lock.is_locked());
  dout(20) << "_dispatch " << m << " " << *m << dendl;
  Session *session = NULL;

  if (map_in_progress_cond) { //can't dispatch while map is being updated!
    if (map_in_progress) {
      dout(25) << "waiting for handle_osd_map to complete before dispatching" << dendl;
      while (map_in_progress)
        map_in_progress_cond->Wait(osd_lock);
    }
  }

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
    handle_osd_map((MOSDMap*)m);
    break;

    // osd
  case CEPH_MSG_SHUTDOWN:
    session = (Session *)m->get_connection()->get_priv();
    if (!session ||
	session->caps.is_mon() ||
	session->caps.is_osd()) shutdown();
    else dout(0) << "shutdown message from connection with insufficient privs!"
		 << m->get_connection() << dendl;
    m->put();
    if (session)
      session->put();
    break;

  case MSG_PGSTATSACK:
    handle_pg_stats_ack((MPGStatsAck*)m);
    break;

  case MSG_MON_COMMAND:
    handle_command((MMonCommand*) m);
    break;
  case MSG_COMMAND:
    handle_command((MCommand*) m);
    break;

  case MSG_OSD_SCRUB:
    handle_scrub((MOSDScrub*)m);
    break;    

  case MSG_OSD_REP_SCRUB:
    handle_rep_scrub((MOSDRepScrub*)m);
    break;    

    // -- need OSDMap --

  default:
    {
      // no map?  starting up?
      if (!osdmap) {
        dout(7) << "no OSDMap, not booted" << dendl;
        waiting_for_osdmap.push_back(m);
        break;
      }
      
      // need OSDMap
      switch (m->get_type()) {

      case MSG_OSD_PG_CREATE:
	handle_pg_create((MOSDPGCreate*)m);
	break;
        
      case MSG_OSD_PG_NOTIFY:
        handle_pg_notify((MOSDPGNotify*)m);
        break;
      case MSG_OSD_PG_QUERY:
        handle_pg_query((MOSDPGQuery*)m);
        break;
      case MSG_OSD_PG_LOG:
        handle_pg_log((MOSDPGLog*)m);
        break;
      case MSG_OSD_PG_REMOVE:
        handle_pg_remove((MOSDPGRemove*)m);
        break;
      case MSG_OSD_PG_INFO:
        handle_pg_info((MOSDPGInfo*)m);
        break;
      case MSG_OSD_PG_TRIM:
        handle_pg_trim((MOSDPGTrim*)m);
        break;
      case MSG_OSD_PG_MISSING:
	handle_pg_missing((MOSDPGMissing*)m);
	break;
      case MSG_OSD_PG_SCAN:
	handle_pg_scan((MOSDPGScan*)m);
	break;

	// client ops
      case CEPH_MSG_OSD_OP:
        handle_op((MOSDOp*)m);
        break;
        
        // for replication etc.
      case MSG_OSD_SUBOP:
	handle_sub_op((MOSDSubOp*)m);
	break;
      case MSG_OSD_SUBOPREPLY:
        handle_sub_op_reply((MOSDSubOpReply*)m);
        break;
      }
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
	 p++) {
      PG *pg = p->second;
      pg->lock();
      if (pg->is_primary()) {
	if (m->repair)
	  pg->state_set(PG_STATE_REPAIR);
	if (pg->queue_scrub()) {
	  dout(10) << "queueing " << *pg << " for scrub" << dendl;
	}
      }
      pg->unlock();
    }
  } else {
    for (vector<pg_t>::iterator p = m->scrub_pgs.begin();
	 p != m->scrub_pgs.end();
	 p++)
      if (pg_map.count(*p)) {
	PG *pg = pg_map[*p];
	pg->lock();
	if (pg->is_primary()) {
	  if (m->repair)
	    pg->state_set(PG_STATE_REPAIR);
	  if (pg->queue_scrub()) {
	    dout(10) << "queueing " << *pg << " for scrub" << dendl;
	  }
	}
	pg->unlock();
      }
  }
  
  m->put();
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

  bool coin_flip = (rand() % 3) == whoami % 3;
  if (!coin_flip) {
    dout(20) << "scrub_should_schedule loadavg " << loadavgs[0]
	     << " < max " << g_conf->osd_scrub_load_threshold
	     << " = no, randomly backing off"
	     << dendl;
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

  dout(20) << "sched_scrub" << dendl;

  pair<utime_t,pg_t> pos;
  utime_t max = ceph_clock_now(g_ceph_context);
  max -= g_conf->osd_scrub_max_interval;
  
  sched_scrub_lock.Lock();

  //dout(20) << " " << last_scrub_pg << dendl;

  set< pair<utime_t,pg_t> >::iterator p = last_scrub_pg.begin();
  while (p != last_scrub_pg.end()) {
    //dout(10) << "pos is " << *p << dendl;
    pos = *p;
    utime_t t = pos.first;
    pg_t pgid = pos.second;

    if (t > max) {
      dout(10) << " " << pgid << " at " << t
	       << " > " << max << " (" << g_conf->osd_scrub_max_interval << " seconds ago)" << dendl;
      break;
    }

    dout(10) << " on " << t << " " << pgid << dendl;
    sched_scrub_lock.Unlock();
    PG *pg = _lookup_lock_pg(pgid);
    if (pg) {
      if (pg->is_active() && !pg->sched_scrub()) {
	pg->unlock();
	sched_scrub_lock.Lock();
	break;
      }
      pg->unlock();
    }
    sched_scrub_lock.Lock();

    // next!
    p = last_scrub_pg.lower_bound(pos);
    //dout(10) << "lb is " << *p << dendl;
    if (p != last_scrub_pg.end())
      p++;
  }    
  sched_scrub_lock.Unlock();

  dout(20) << "sched_scrub done" << dendl;
}

bool OSD::inc_scrubs_pending()
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

void OSD::dec_scrubs_pending()
{
  sched_scrub_lock.Lock();
  dout(20) << "dec_scrubs_pending " << scrubs_pending << " -> " << (scrubs_pending-1)
	   << " (max " << g_conf->osd_max_scrubs << ", active " << scrubs_active << ")" << dendl;
  --scrubs_pending;
  assert(scrubs_pending >= 0);
  sched_scrub_lock.Unlock();
}

void OSD::dec_scrubs_active()
{
  sched_scrub_lock.Lock();
  dout(20) << "dec_scrubs_active " << scrubs_active << " -> " << (scrubs_active-1)
	   << " (max " << g_conf->osd_max_scrubs << ", pending " << scrubs_pending << ")" << dendl;
  --scrubs_active;
  sched_scrub_lock.Unlock();
}

// =====================================================
// MAP

void OSD::wait_for_new_map(Message *m)
{
  // ask?
  if (waiting_for_osdmap.empty()) {
    monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }
  
  waiting_for_osdmap.push_back(m);
}


/** update_map
 * assimilate new OSDMap(s).  scan pgs, etc.
 */

void OSD::note_down_osd(int peer)
{
  cluster_messenger->mark_down(osdmap->get_cluster_addr(peer));

  heartbeat_lock.Lock();

  // clean out down osds i am sending heartbeats _to_.
  // update_heartbeat_peers() will clean out peers i expect heartbeast _from_.
  if (heartbeat_to.count(peer) &&
      heartbeat_to[peer] < osdmap->get_epoch()) {
    dout(10) << "note_down_osd osd." << peer << " marking down hbout connection "
	     << heartbeat_to_con[peer]->get_peer_addr() << dendl;
    hbout_messenger->mark_down(heartbeat_to_con[peer]);
    heartbeat_to_con[peer]->put();
    heartbeat_to_con.erase(peer);
    heartbeat_to.erase(peer);
  }

  failure_queue.erase(peer);
  failure_pending.erase(peer);

  heartbeat_lock.Unlock();
}

void OSD::note_up_osd(int peer)
{
  forget_peer_epoch(peer, osdmap->get_epoch() - 1);
}

void OSD::handle_osd_map(MOSDMap *m)
{
  assert(osd_lock.is_locked());
  if (m->fsid != monc->get_fsid()) {
    dout(0) << "handle_osd_map fsid " << m->fsid << " != " << monc->get_fsid() << dendl;
    m->put();
    return;
  }

  Session *session = (Session *)m->get_connection()->get_priv();
  if (session && !(session->caps.is_mon() || session->caps.is_osd())) {
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

  if (logger) {
    logger->inc(l_osd_map);
    logger->inc(l_osd_mape, last - first + 1);
    if (first <= osdmap->get_epoch())
      logger->inc(l_osd_mape_dup, osdmap->get_epoch() - first + 1);
  }

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

  if (map_in_progress_cond) {
    if (map_in_progress) {
      dout(15) << "waiting for prior handle_osd_map to complete" << dendl;
      while (map_in_progress) {
        map_in_progress_cond->Wait(osd_lock);
      }
    }
    dout(10) << "locking handle_osd_map permissions" << dendl;
    map_in_progress = true;
  }

  osd_lock.Unlock();

  op_tp.pause();

  // requeue under osd_lock to preserve ordering of _dispatch() wrt incoming messages
  osd_lock.Lock();  

  op_wq.lock();

  list<Message*> rq;
  while (true) {
    PG *pg = op_wq._dequeue();
    if (!pg)
      break;
    pg->lock();
    Message *mess = pg->op_queue.front();
    pg->op_queue.pop_front();
    pg->unlock();
    pg->put();
    dout(15) << " will requeue " << *mess << dendl;
    rq.push_back(mess);
  }
  push_waiters(rq);  // requeue under osd_lock!
  op_wq.unlock();

  recovery_tp.pause();
  disk_tp.pause_new();   // _process() may be waiting for a replica message

  ObjectStore::Transaction t;

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
      add_map(o);

      hobject_t fulloid = get_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, fulloid, 0, bl.length(), bl);
      add_map_bl(e, bl);
      continue;
    }

    p = m->incremental_maps.find(e);
    if (p != m->incremental_maps.end()) {
      dout(10) << "handle_osd_map  got inc map for epoch " << e << dendl;
      bufferlist& bl = p->second;
      hobject_t oid = get_inc_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, oid, 0, bl.length(), bl);
      add_map_inc_bl(e, bl);

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

      add_map(o);

      bufferlist fbl;
      o->encode(fbl);

      hobject_t fulloid = get_osdmap_pobject_name(e);
      t.write(coll_t::META_COLL, fulloid, 0, fbl.length(), fbl);
      add_map_bl(e, fbl);
      continue;
    }

    assert(0 == "MOSDMap lied about what maps it had?");
  }

  // check for cluster snapshot
  string cluster_snap;
  for (epoch_t cur = start; cur <= last && cluster_snap.length() == 0; cur++) {
    OSDMapRef newmap = get_map(cur);
    cluster_snap = newmap->get_cluster_snapshot();
  }

  // flush here so that the peering code can re-read any pg data off
  // disk that it needs to... say for backlog generation.  (hmm, is
  // this really needed?)
  osd_lock.Unlock();
  if (cluster_snap.length()) {
    dout(0) << "creating cluster snapshot '" << cluster_snap << "'" << dendl;
    int r = store->snapshot(cluster_snap);
    if (r)
      dout(0) << "failed to create cluster snapshot: " << cpp_strerror(r) << dendl;
  } else {
    store->flush();
  }
  osd_lock.Lock();

  assert(osd_lock.is_locked());

  if (superblock.oldest_map) {
    for (epoch_t e = superblock.oldest_map; e < m->oldest_map; ++e) {
      dout(20) << " removing old osdmap epoch " << e << dendl;
      t.remove(coll_t::META_COLL, get_osdmap_pobject_name(e));
      t.remove(coll_t::META_COLL, get_inc_osdmap_pobject_name(e));
      superblock.oldest_map = e+1;
    }
  }

  if (!superblock.oldest_map || skip_maps)
    superblock.oldest_map = first;
  superblock.newest_map = last;

 
  // finally, take map_lock _after_ we do this flush, to avoid deadlock
  map_lock.get_write();

  // advance through the new maps
  for (epoch_t cur = start; cur <= superblock.newest_map; cur++) {
    dout(10) << " advance to epoch " << cur << " (<= newest " << superblock.newest_map << ")" << dendl;

    OSDMapRef newmap = get_map(cur);
    assert(newmap);  // we just cached it above!

    // kill connections to newly down osds
    set<int> old;
    osdmap->get_all_osds(old);
    for (set<int>::iterator p = old.begin(); p != old.end(); p++)
      if (*p != whoami &&
	  osdmap->have_inst(*p) &&                        // in old map
	  (!newmap->exists(*p) || !newmap->is_up(*p)))    // but not the new one
	note_down_osd(*p);
    
    osdmap = newmap;

    superblock.current_epoch = cur;
    advance_map(t);
    had_map_since = ceph_clock_now(g_ceph_context);
  }

  C_Contexts *fin = new C_Contexts(g_ceph_context);
  if (osdmap->is_up(whoami) &&
      osdmap->get_addr(whoami) == client_messenger->get_myaddr() &&
      bind_epoch < osdmap->get_up_from(whoami)) {

    if (is_booting()) {
      dout(1) << "state: booting -> active" << dendl;
      state = STATE_ACTIVE;
    }
      
    // yay!
    activate_map(t, fin->contexts);
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
	       !osdmap->get_hb_addr(whoami).probably_equals(hbout_messenger->get_myaddr())) {
      if (!osdmap->is_up(whoami))
	clog.warn() << "map e" << osdmap->get_epoch()
		    << " wrongly marked me down or wrong addr";
      else if (!osdmap->get_addr(whoami).probably_equals(client_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong client addr (" << osdmap->get_addr(whoami)
		    << " != my " << client_messenger->get_myaddr();
      else if (!osdmap->get_cluster_addr(whoami).probably_equals(cluster_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong cluster addr (" << osdmap->get_cluster_addr(whoami)
		    << " != my " << cluster_messenger->get_myaddr();
      else if (!osdmap->get_hb_addr(whoami).probably_equals(hbout_messenger->get_myaddr()))
	clog.error() << "map e" << osdmap->get_epoch()
		    << " had wrong hb addr (" << osdmap->get_hb_addr(whoami)
		    << " != my " << hbout_messenger->get_myaddr();
      
      state = STATE_BOOTING;
      up_epoch = 0;
      do_restart = true;
      bind_epoch = osdmap->get_epoch();

      int cport = cluster_messenger->get_myaddr().get_port();
      int hbport = hbout_messenger->get_myaddr().get_port();

      int r = cluster_messenger->rebind(hbport);
      if (r != 0)
	do_shutdown = true;  // FIXME: do_restart?

      r = hbout_messenger->rebind(cport);
      if (r != 0)
	do_shutdown = true;  // FIXME: do_restart?

      hbin_messenger->mark_down_all();

      reset_heartbeat_peers();
    }
  }

  // process waiters
  take_waiters(waiting_for_osdmap);

  // write updated pg state to store
  for (hash_map<pg_t,PG*>::iterator i = pg_map.begin();
       i != pg_map.end();
       i++) {
    PG *pg = i->second;
    if (pg->dirty_info)
      pg->write_info(t);
  }

  // note in the superblock that we were clean thru the prior epoch
  if (boot_epoch && boot_epoch >= superblock.mounted) {
    superblock.mounted = boot_epoch;
    superblock.clean_thru = osdmap->get_epoch();
  }

  // completion
  Mutex ulock("OSD::handle_osd_map() ulock");
  Cond ucond;
  bool udone;
  fin->add(new C_SafeCond(&ulock, &ucond, &udone));

  // superblock and commit
  write_superblock(t);
  int r = store->apply_transaction(t, fin);
  if (r) {
    map_lock.put_write();
    derr << "error writing map: " << cpp_strerror(-r) << dendl;
    m->put();
    shutdown();
    return;
  }

  map_lock.put_write();

  /*
   * wait for this to be stable.
   *
   * NOTE: This is almost certainly overkill.  The important things
   * that need to be stable to make the peering/recovery stuff correct
   * include:
   *
   *   - last_epoch_started, since that bounds how far back in time we
   *     check old peers
   *   - ???
   */
  osd_lock.Unlock();
  ulock.Lock();
  while (!udone)
    ucond.Wait(ulock);
  ulock.Unlock();
  osd_lock.Lock();

  // everything through current epoch now on disk; keep anything after
  // that in cache
  trim_map_bl_cache(osdmap->get_epoch()+1);
  trim_map_cache(0);

  op_tp.unpause();
  recovery_tp.unpause();
  disk_tp.unpause();

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

  if (map_in_progress_cond) {
    map_in_progress = false;
    dout(15) << "unlocking map_in_progress" << dendl;
    map_in_progress_cond->Signal();
  }
}


/** 
 * scan placement groups, initiate any replication
 * activities.
 */
void OSD::advance_map(ObjectStore::Transaction& t)
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

  // update pools
  for (map<int, PGPool*>::iterator p = pool_map.begin();
       p != pool_map.end();
       p++) {
    const pg_pool_t *pi = osdmap->get_pg_pool(p->first);
    if (pi == NULL) {
      dout(10) << " pool " << p->first << " appears to have been deleted" << dendl;
      continue;
    }
    PGPool *pool = p->second;
    
    // make sure auid stays up to date
    pool->auid = pi->auid;
    
    if (pi->get_snap_epoch() == osdmap->get_epoch()) {
      pi->build_removed_snaps(pool->newly_removed_snaps);
      pool->newly_removed_snaps.subtract(pool->cached_removed_snaps);
      pool->cached_removed_snaps.union_of(pool->newly_removed_snaps);
      dout(10) << " pool " << p->first << " removed_snaps " << pool->cached_removed_snaps
	       << ", newly so are " << pool->newly_removed_snaps << ")"
	       << dendl;
      pool->info = *pi;
      pool->snapc = pi->get_snap_context();
    } else {
      dout(10) << " pool " << p->first << " removed snaps " << pool->cached_removed_snaps
	       << ", unchanged (snap_epoch = " << pi->get_snap_epoch() << ")" << dendl;
      pool->newly_removed_snaps.clear();
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

  // if we skipped a discontinuity and are the first epoch, we won't have a previous map.
  OSDMapRef lastmap;
  if (osdmap->get_epoch() > superblock.oldest_map)
    lastmap = get_map(osdmap->get_epoch() - 1);

  // scan existing pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    PG *pg = it->second;

    vector<int> newup, newacting;
    osdmap->pg_to_up_acting_osds(pg->info.pgid, newup, newacting);

    pg->lock_with_map_lock_held();
    dout(10) << "Scanning pg " << *pg << dendl;
    pg->handle_advance_map(osdmap, lastmap, newup, newacting, 0);
    pg->unlock();
  }
}

void OSD::activate_map(ObjectStore::Transaction& t, list<Context*>& tfin)
{
  assert(osd_lock.is_locked());

  dout(7) << "activate_map version " << osdmap->get_epoch() << dendl;

  map< int, vector<PG::Info> >  notify_list;  // primary -> list
  map< int, map<pg_t,PG::Query> > query_map;    // peer -> PG -> get_summary_since
  map<int,MOSDPGInfo*> info_map;  // peer -> message

  int num_pg_primary = 0, num_pg_replica = 0, num_pg_stray = 0;

  epoch_t oldest_last_clean = osdmap->get_epoch();

  // scan pg's
  for (hash_map<pg_t,PG*>::iterator it = pg_map.begin();
       it != pg_map.end();
       it++) {
    PG *pg = it->second;
    pg->lock_with_map_lock_held();

    if (pg->is_primary())
      num_pg_primary++;
    else if (pg->is_replica())
      num_pg_replica++;
    else
      num_pg_stray++;

    if (pg->is_primary() && pg->info.history.last_epoch_clean < oldest_last_clean)
      oldest_last_clean = pg->info.history.last_epoch_clean;
    
    if (!osdmap->have_pg_pool(pg->info.pgid.pool())) {
      //pool is deleted!
      queue_pg_for_deletion(pg);
      pg->unlock();
      continue;
    }

    PG::RecoveryCtx rctx(&query_map, &info_map, &notify_list, &tfin, &t);
    pg->handle_activate_map(&rctx);
    
    pg->unlock();
  }  

  do_notifies(notify_list, osdmap->get_epoch());  // notify? (residual|replica)
  do_queries(query_map);
  do_infos(info_map);

  logger->set(l_osd_pg, pg_map.size());
  logger->set(l_osd_pg_primary, num_pg_primary);
  logger->set(l_osd_pg_replica, num_pg_replica);
  logger->set(l_osd_pg_stray, num_pg_stray);

  wake_all_pg_waiters();   // the pg mapping may have shifted
  trim_map_cache(oldest_last_clean);
  update_heartbeat_peers();

  send_pg_temp();

  if (osdmap->test_flag(CEPH_OSDMAP_FULL)) {
    dout(10) << " osdmap flagged full, doing onetime osdmap subscribe" << dendl;
    monc->sub_want("osdmap", osdmap->get_epoch() + 1, CEPH_SUBSCRIBE_ONETIME);
    monc->renew_subs();
  }
}


MOSDMap *OSD::build_incremental_map_msg(epoch_t since, epoch_t to)
{
  MOSDMap *m = new MOSDMap(monc->get_fsid());
  m->oldest_map = superblock.oldest_map;
  m->newest_map = superblock.newest_map;
  
  for (epoch_t e = to;
       e > since;
       e--) {
    bufferlist bl;
    if (get_inc_map_bl(e,bl)) {
      m->incremental_maps[e].claim(bl);
    } else if (get_map_bl(e,bl)) {
      m->maps[e].claim(bl);
      break;
    }
    else {
      assert(0);  // we should have all maps.
    }
  }
  return m;
}

void OSD::send_map(MOSDMap *m, const entity_inst_t& inst, bool lazy)
{
  Messenger *msgr = client_messenger;
  if (entity_name_t::TYPE_OSD == inst.name._type)
    msgr = cluster_messenger;
  if (lazy)
    msgr->lazy_send_message(m, inst);  // only if we already have an open connection
  else
    msgr->send_message(m, inst);
}

void OSD::send_incremental_map(epoch_t since, const entity_inst_t& inst, bool lazy)
{
  dout(10) << "send_incremental_map " << since << " -> " << osdmap->get_epoch()
           << " to " << inst << dendl;
  
  if (since < superblock.oldest_map) {
    // just send latest full map
    MOSDMap *m = new MOSDMap(monc->get_fsid());
    m->oldest_map = superblock.oldest_map;
    m->newest_map = superblock.newest_map;
    epoch_t e = osdmap->get_epoch();
    get_map_bl(e, m->maps[e]);
    send_map(m, inst, lazy);
    return;
  }

  while (since < osdmap->get_epoch()) {
    epoch_t to = osdmap->get_epoch();
    if (to - since > (epoch_t)g_conf->osd_map_message_max)
      to = since + g_conf->osd_map_message_max;
    MOSDMap *m = build_incremental_map_msg(since, to);
    send_map(m, inst, lazy);
    since = to;
  }
}

bool OSD::get_map_bl(epoch_t e, bufferlist& bl)
{
  {
    Mutex::Locker l(map_cache_lock);
    map<epoch_t,bufferlist>::iterator p = map_bl.find(e);
    if (p != map_bl.end()) {
      bl = p->second;
      return true;
    }
  }
  return store->read(coll_t::META_COLL, get_osdmap_pobject_name(e), 0, 0, bl) >= 0;
}

bool OSD::get_inc_map_bl(epoch_t e, bufferlist& bl)
{
  {
    Mutex::Locker l(map_cache_lock);
    map<epoch_t,bufferlist>::iterator p = map_inc_bl.find(e);
    if (p != map_inc_bl.end()) {
      bl = p->second;
      return true;
    }
  }
  return store->read(coll_t::META_COLL, get_inc_osdmap_pobject_name(e), 0, 0, bl) >= 0;
}

OSDMapRef OSD::add_map(OSDMap *o)
{
  Mutex::Locker l(map_cache_lock);
  epoch_t e = o->get_epoch();
  if (map_cache.count(e) == 0) {
    dout(10) << "add_map " << e << " " << o << dendl;
    map_cache.insert(make_pair(e, OSDMapRef(o)));
  } else {
    dout(10) << "add_map " << e << " already have it" << dendl;
  }
  return map_cache[e];
}

void OSD::add_map_bl(epoch_t e, bufferlist& bl)
{
  Mutex::Locker l(map_cache_lock);
  dout(10) << "add_map_bl " << e << " " << bl.length() << " bytes" << dendl;
  map_bl[e] = bl;
}

void OSD::add_map_inc_bl(epoch_t e, bufferlist& bl)
{
  Mutex::Locker l(map_cache_lock);
  dout(10) << "add_map_inc_bl " << e << " " << bl.length() << " bytes" << dendl;
  map_inc_bl[e] = bl;
}

OSDMapRef OSD::get_map(epoch_t epoch)
{
  {
    Mutex::Locker l(map_cache_lock);
    map<epoch_t,OSDMapRef>::iterator p = map_cache.find(epoch);
    if (p != map_cache.end()) {
      dout(30) << "get_map " << epoch << " - cached " << p->second << dendl;
      return p->second;
    }
  }

  OSDMap *map = new OSDMap;
  if (epoch > 0) {
    dout(20) << "get_map " << epoch << " - loading and decoding " << map << dendl;
    bufferlist bl;
    get_map_bl(epoch, bl);
    map->decode(bl);
  } else {
    dout(20) << "get_map " << epoch << " - return initial " << map << dendl;
  }
  return add_map(map);
}

void OSD::trim_map_bl_cache(epoch_t oldest)
{
  Mutex::Locker l(map_cache_lock);
  dout(10) << "trim_map_bl_cache up to " << oldest << dendl;
  while (!map_inc_bl.empty() && map_inc_bl.begin()->first < oldest)
    map_inc_bl.erase(map_inc_bl.begin());
  while (!map_bl.empty() && map_bl.begin()->first < oldest)
    map_bl.erase(map_bl.begin());
}

void OSD::trim_map_cache(epoch_t oldest)
{
  Mutex::Locker l(map_cache_lock);
  dout(10) << "trim_map_cache prior to " << oldest << dendl;
  while (!map_cache.empty() &&
	 (map_cache.begin()->first < oldest ||
	  (int)map_cache.size() > g_conf->osd_map_cache_max)) {
    epoch_t e = map_cache.begin()->first;
    OSDMapRef o = map_cache.begin()->second;
    dout(10) << "trim_map_cache " << e << " " << o << dendl;
    map_cache.erase(map_cache.begin());
  }
}

void OSD::clear_map_cache()
{
  while (!map_cache.empty()) {
    map_cache.erase(map_cache.begin());
  }
}

bool OSD::get_inc_map(epoch_t e, OSDMap::Incremental &inc)
{
  bufferlist bl;
  if (!get_inc_map_bl(e, bl)) 
    return false;
  bufferlist::iterator p = bl.begin();
  inc.decode(p);
  return true;
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

bool OSD::require_osd_peer(Message *m)
{
  if (!m->get_connection()->peer_is_osd()) {
    dout(0) << "require_osd_peer received from non-osd " << m->get_connection()->get_peer_addr()
	    << " " << *m << dendl;
    m->put();
    return false;
  }
  return true;
}

bool OSD::require_current_map(Message *m, epoch_t ep) 
{
  // older map?
  if (ep < osdmap->get_epoch()) {
    dout(7) << "require_current_map epoch " << ep << " < " << osdmap->get_epoch() << dendl;
    m->put();   // discard and ignore.
    return false;
  }

  // newer map?
  if (ep > osdmap->get_epoch()) {
    dout(7) << "require_current_map epoch " << ep << " > " << osdmap->get_epoch() << dendl;
    wait_for_new_map(m);
    return false;
  }

  assert(ep == osdmap->get_epoch());
  return true;
}


/*
 * require that we have same (or newer) map, and that
 * the source is the pg primary.
 */
bool OSD::require_same_or_newer_map(Message *m, epoch_t epoch)
{
  dout(15) << "require_same_or_newer_map " << epoch << " (i am " << osdmap->get_epoch() << ") " << m << dendl;

  // do they have a newer map?
  if (epoch > osdmap->get_epoch()) {
    dout(7) << "waiting for newer map epoch " << epoch << " > my " << osdmap->get_epoch() << " with " << m << dendl;
    wait_for_new_map(m);
    return false;
  }

  if (epoch < up_epoch) {
    dout(7) << "from pre-up epoch " << epoch << " < " << up_epoch << dendl;
    m->put();
    return false;
  }

  // ok, our map is same or newer.. do they still exist?
  if (m->get_source().is_osd()) {
    int from = m->get_source().num();
    if (!osdmap->have_inst(from) ||
	osdmap->get_cluster_addr(from) != m->get_source_inst().addr) {
      dout(0) << "from dead osd." << from << ", dropping, sharing map" << dendl;
      send_incremental_map(epoch, m->get_source_inst(), true);

      // close after we send the map; don't reconnect
      Connection *con = m->get_connection();
      cluster_messenger->mark_down_on_empty(con);
      cluster_messenger->mark_disposable(con);

      m->put();
      return false;
    }
  }

  // ok, we have at least as new a map as they do.  are we (re)booting?
  if (!is_active()) {
    dout(7) << "still in boot state, dropping message " << *m << dendl;
    m->put();
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

  if (creating_pgs[pgid].split_bits) {
    dout(10) << "can_create_pg " << pgid << " - queueing for split" << dendl;
    pg_split_ready[creating_pgs[pgid].parent].insert(pgid); 
    return false;
  }

  dout(10) << "can_create_pg " << pgid << " - can create now" << dendl;
  return true;
}


void OSD::kick_pg_split_queue()
{
  map< int, map<pg_t,PG::Query> > query_map;
  map<int, MOSDPGInfo*> info_map;
  int created = 0;

  dout(10) << "kick_pg_split_queue" << dendl;

  map<pg_t, set<pg_t> >::iterator n = pg_split_ready.begin();
  while (n != pg_split_ready.end()) {
    map<pg_t, set<pg_t> >::iterator p = n++;
    // how many children should this parent have?
    unsigned nchildren = (1 << (creating_pgs[*p->second.begin()].split_bits - 1)) - 1;
    if (p->second.size() < nchildren) {
      dout(15) << " parent " << p->first << " children " << p->second 
	       << " ... waiting for " << nchildren << " children" << dendl;
      continue;
    }

    PG *parent = _lookup_lock_pg(p->first);
    assert(parent);
    if (!parent->is_clean()) {
      dout(10) << "kick_pg_split_queue parent " << p->first << " not clean" << dendl;
      parent->unlock();
      continue;
    }

    dout(15) << " parent " << p->first << " children " << p->second 
	     << " ready" << dendl;
    
    // FIXME: this should be done in a separate thread, eventually

    // create and lock children
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    C_Contexts *fin = new C_Contexts(g_ceph_context);
    map<pg_t,PG*> children;
    for (set<pg_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
      PG::Info::History history;
      history.epoch_created = history.same_up_since =
          history.same_interval_since = history.same_primary_since =
          osdmap->get_epoch();
      PG *pg = _create_lock_new_pg(*q, creating_pgs[*q].acting, *t, history);
      children[*q] = pg;
    }

    // split
    split_pg(parent, children, *t); 

    parent->update_stats();
    parent->write_info(*t);

    // unlock parent, children
    parent->unlock();
    for (map<pg_t,PG*>::iterator q = children.begin(); q != children.end(); q++) {
      PG *pg = q->second;
      // fix up pg metadata
      pg->info.last_complete = pg->info.last_update;

      pg->write_info(*t);
      pg->write_log(*t);

      wake_pg_waiters(pg->info.pgid);

      PG::RecoveryCtx rctx(&query_map, &info_map, 0, &fin->contexts, t);
      pg->handle_create(&rctx);

      pg->update_stats();
      pg->unlock();
      created++;
    }

    int tr = store->queue_transaction(NULL, t, new ObjectStore::C_DeleteTransaction(t), fin);
    assert(tr == 0);

    // remove from queue
    pg_split_ready.erase(p);
  }

  do_queries(query_map);
  do_infos(info_map);
  if (created)
    update_heartbeat_peers();

}

void OSD::split_pg(PG *parent, map<pg_t,PG*>& children, ObjectStore::Transaction &t)
{
  dout(10) << "split_pg " << *parent << dendl;
  pg_t parentid = parent->info.pgid;

  // split objects
  vector<hobject_t> olist;
  store->collection_list(coll_t(parent->info.pgid), olist);

  for (vector<hobject_t>::iterator p = olist.begin(); p != olist.end(); p++) {
    hobject_t poid = *p;
    ceph_object_layout l = osdmap->make_object_layout(poid.oid, parentid.pool(), parentid.preferred());
    pg_t pgid = osdmap->raw_pg_to_pg(pg_t(l.ol_pgid));
    if (pgid != parentid) {
      dout(20) << "  moving " << poid << " from " << parentid << " -> " << pgid << dendl;
      PG *child = children[pgid];
      assert(child);
      bufferlist bv;

      struct stat st;
      store->stat(coll_t(parentid), poid, &st);
      store->getattr(coll_t(parentid), poid, OI_ATTR, bv);
      object_info_t oi(bv);

      t.collection_add(coll_t(pgid), coll_t(parentid), poid);
      t.collection_remove(coll_t(parentid), poid);
      if (oi.snaps.size()) {
	snapid_t first = oi.snaps[0];
	t.collection_add(coll_t(pgid, first), coll_t(parentid), poid);
	t.collection_remove(coll_t(parentid, first), poid);
	if (oi.snaps.size() > 1) {
	  snapid_t last = oi.snaps[oi.snaps.size()-1];
	  t.collection_add(coll_t(pgid, last), coll_t(parentid), poid);
	  t.collection_remove(coll_t(parentid, last), poid);
	}
      }

      // add to child stats
      child->info.stats.stats.sum.num_bytes += st.st_size;
      child->info.stats.stats.sum.num_kb += SHIFT_ROUND_UP(st.st_size, 10);
      child->info.stats.stats.sum.num_objects++;
      if (poid.snap && poid.snap != CEPH_NOSNAP)
	child->info.stats.stats.sum.num_object_clones++;
    } else {
      dout(20) << " leaving " << poid << "   in " << parentid << dendl;
    }
  }

  // split log
  parent->log.index();
  dout(20) << " parent " << parent->info.pgid << " log was ";
  parent->log.print(*_dout);
  *_dout << dendl;
  parent->log.unindex();

  list<PG::Log::Entry>::iterator p = parent->log.log.begin();
  while (p != parent->log.log.end()) {
    list<PG::Log::Entry>::iterator cur = p;
    p++;
    hobject_t& poid = cur->soid;
    ceph_object_layout l = osdmap->make_object_layout(poid.oid, parentid.pool(), parentid.preferred());
    pg_t pgid = osdmap->raw_pg_to_pg(pg_t(l.ol_pgid));
    if (pgid != parentid) {
      dout(20) << "  moving " << *cur << " from " << parentid << " -> " << pgid << dendl;
      PG *child = children[pgid];

      child->log.log.splice(child->log.log.end(), parent->log.log, cur);
    }
  }

  parent->log.index();
  dout(20) << " parent " << parent->info.pgid << " log now ";
  parent->log.print(*_dout);
  *_dout << dendl;

  for (map<pg_t,PG*>::iterator p = children.begin();
       p != children.end();
       p++) {
    PG *child = p->second;

    // fix log bounds
    if (!child->log.empty()) {
      child->log.head = child->log.log.rbegin()->version;
      child->log.tail =  parent->log.tail;
      child->log.backlog = parent->log.backlog;
      child->log.index();
    }
    child->info.last_update = child->log.head;
    child->info.last_complete = parent->info.last_complete;
    child->info.log_tail =  parent->log.tail;
    child->info.log_backlog = parent->log.backlog;
    child->info.history.last_epoch_split = osdmap->get_epoch();

    child->snap_trimq = parent->snap_trimq;

    dout(20) << " child " << p->first << " log now ";
    child->log.print(*_dout);
    *_dout << dendl;

    // sub off child stats
    parent->info.stats.sub(child->info.stats);
  }
}  


/*
 * holding osd_lock
 */
void OSD::handle_pg_create(MOSDPGCreate *m)
{
  dout(10) << "handle_pg_create " << *m << dendl;
  if (!require_mon_peer(m))
    return;

  if (!require_same_or_newer_map(m, m->epoch)) return;

  map< int, map<pg_t,PG::Query> > query_map;
  map<int, MOSDPGInfo*> info_map;

  int num_created = 0;

  for (map<pg_t,MOSDPGCreate::create_rec>::iterator p = m->mkpg.begin();
       p != m->mkpg.end();
       p++) {
    pg_t pgid = p->first;
    epoch_t created = p->second.created;
    pg_t parent = p->second.parent;
    int split_bits = p->second.split_bits;
    pg_t on = pgid;

    if (split_bits) {
      on = parent;
      dout(20) << "mkpg " << pgid << " e" << created << " from parent " << parent
	       << " split by " << split_bits << " bits" << dendl;
    } else {
      dout(20) << "mkpg " << pgid << " e" << created << dendl;
    }
   
    // is it still ours?
    vector<int> up, acting;
    osdmap->pg_to_up_acting_osds(on, up, acting);
    int role = osdmap->calc_pg_role(whoami, acting, acting.size());

    if (role != 0) {
      dout(10) << "mkpg " << pgid << "  not primary (role=" << role << "), skipping" << dendl;
      continue;
    }
    if (up != acting) {
      dout(10) << "mkpg " << pgid << "  up " << up << " != acting " << acting << dendl;
      clog.error() << "mkpg " << pgid << " up " << up << " != acting "
	    << acting << "\n";
      continue;
    }

    // does it already exist?
    if (_have_pg(pgid)) {
      dout(10) << "mkpg " << pgid << "  already exists, skipping" << dendl;
      continue;
    }

    // does parent exist?
    if (split_bits && !_have_pg(parent)) {
      dout(10) << "mkpg " << pgid << "  missing parent " << parent << ", skipping" << dendl;
      continue;
    }

    // figure history
    PG::Info::History history;
    history.epoch_created = created;
    history.last_epoch_clean = created;
    project_pg_history(pgid, history, created, up, acting);
    
    // register.
    creating_pgs[pgid].history = history;
    creating_pgs[pgid].parent = parent;
    creating_pgs[pgid].split_bits = split_bits;
    creating_pgs[pgid].acting.swap(acting);
    calc_priors_during(pgid, created, history.same_interval_since, 
		       creating_pgs[pgid].prior);

    // poll priors
    set<int>& pset = creating_pgs[pgid].prior;
    dout(10) << "mkpg " << pgid << " e" << created
	     << " h " << history
	     << " : querying priors " << pset << dendl;
    for (set<int>::iterator p = pset.begin(); p != pset.end(); p++) 
      if (osdmap->is_up(*p))
	query_map[*p][pgid] = PG::Query(PG::Query::INFO, history);
    
    if (can_create_pg(pgid)) {
      ObjectStore::Transaction *t = new ObjectStore::Transaction;
      C_Contexts *fin = new C_Contexts(g_ceph_context);

      PG *pg = _create_lock_new_pg(pgid, creating_pgs[pgid].acting, *t, history);
      creating_pgs.erase(pgid);

      wake_pg_waiters(pg->info.pgid);
      PG::RecoveryCtx rctx(&query_map, &info_map, 0, &fin->contexts, t);
      pg->handle_create(&rctx);
      pg->update_stats();

      int tr = store->queue_transaction(&pg->osr, t, new ObjectStore::C_DeleteTransaction(t), fin);
      assert(tr == 0);

      pg->unlock();
      num_created++;
    }
  }

  do_queries(query_map);
  do_infos(info_map);

  kick_pg_split_queue();
  if (num_created)
    update_heartbeat_peers();
  m->put();
}


// ----------------------------------------
// peering and recovery

/** do_notifies
 * Send an MOSDPGNotify to a primary, with a list of PGs that I have
 * content for, and they are primary for.
 */

void OSD::do_notifies(map< int, vector<PG::Info> >& notify_list,
		      epoch_t query_epoch)
{
  for (map< int, vector<PG::Info> >::iterator it = notify_list.begin();
       it != notify_list.end();
       it++) {
    if (it->first == whoami) {
      dout(7) << "do_notify osd." << it->first << " is self, skipping" << dendl;
      continue;
    }
    dout(7) << "do_notify osd." << it->first
	    << " on " << it->second.size() << " PGs" << dendl;
    MOSDPGNotify *m = new MOSDPGNotify(osdmap->get_epoch(),
				       it->second,
				       query_epoch);
    _share_map_outgoing(osdmap->get_cluster_inst(it->first));
    cluster_messenger->send_message(m, osdmap->get_cluster_inst(it->first));
  }
}


/** do_queries
 * send out pending queries for info | summaries
 */
void OSD::do_queries(map< int, map<pg_t,PG::Query> >& query_map)
{
  for (map< int, map<pg_t,PG::Query> >::iterator pit = query_map.begin();
       pit != query_map.end();
       pit++) {
    int who = pit->first;
    dout(7) << "do_queries querying osd." << who
            << " on " << pit->second.size() << " PGs" << dendl;
    MOSDPGQuery *m = new MOSDPGQuery(osdmap->get_epoch(), pit->second);
    _share_map_outgoing(osdmap->get_cluster_inst(who));
    cluster_messenger->send_message(m, osdmap->get_cluster_inst(who));
  }
}


void OSD::do_infos(map<int,MOSDPGInfo*>& info_map)
{
  for (map<int,MOSDPGInfo*>::iterator p = info_map.begin();
       p != info_map.end();
       ++p) { 
    for (vector<PG::Info>::iterator i = p->second->pg_info.begin();
	 i != p->second->pg_info.end();
	 ++i) {
      dout(20) << "Sending info " << *i << " to osd." << p->first << dendl;
    }
    cluster_messenger->send_message(p->second, osdmap->get_cluster_inst(p->first));
  }
  info_map.clear();
}


/** PGNotify
 * from non-primary to primary
 * includes PG::Info.
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_notify(MOSDPGNotify *m)
{
  dout(7) << "handle_pg_notify from " << m->get_source() << dendl;
  int from = m->get_source().num();

  if (!require_osd_peer(m))
    return;

  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  // look for unknown PGs i'm primary for
  map< int, map<pg_t,PG::Query> > query_map;
  map<int, MOSDPGInfo*> info_map;
  int created = 0;

  for (vector<PG::Info>::iterator it = m->get_pg_list().begin();
       it != m->get_pg_list().end();
       it++) {
    PG *pg = 0;

    ObjectStore::Transaction *t;
    C_Contexts *fin;
    pg = get_or_create_pg(*it, m->get_epoch(), from, created, true, &t, &fin);
    if (!pg)
      continue;

    if (pg->old_peering_msg(m->get_epoch(), m->get_query_epoch())) {
      dout(10) << "ignoring old peering message " << *m << dendl;
      pg->unlock();
      delete t;
      delete fin;
      continue;
    }

    PG::RecoveryCtx rctx(&query_map, &info_map, 0, &fin->contexts, t);
    pg->handle_notify(from, *it, &rctx);

    int tr = store->queue_transaction(&pg->osr, t, new ObjectStore::C_DeleteTransaction(t), fin);
    assert(tr == 0);
    pg->unlock();
  }
  
  do_queries(query_map);
  do_infos(info_map);
  
  kick_pg_split_queue();

  if (created)
    update_heartbeat_peers();

  m->put();
}

void OSD::handle_pg_log(MOSDPGLog *m) 
{
  dout(7) << "handle_pg_log " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(m))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  int created = 0;
  ObjectStore::Transaction *t;
  C_Contexts *fin;  
  PG *pg = get_or_create_pg(m->info, m->get_epoch(), 
			    from, created, false, &t, &fin);
  if (!pg) {
    m->put();
    return;
  }

  if (pg->old_peering_msg(m->get_epoch(), m->get_query_epoch())) {
    dout(10) << "ignoring old peering message " << *m << dendl;
    pg->unlock();
    delete t;
    delete fin;
    return;
  }

  map< int, map<pg_t,PG::Query> > query_map;
  map< int, MOSDPGInfo* > info_map;
  PG::RecoveryCtx rctx(&query_map, &info_map, 0, &fin->contexts, t);
  pg->handle_log(from, m, &rctx);
  pg->unlock();
  do_queries(query_map);
  do_infos(info_map);

  int tr = store->queue_transaction(&pg->osr, t, new ObjectStore::C_DeleteTransaction(t), fin);
  assert(!tr);

  if (created)
    update_heartbeat_peers();
  m->put();
}

void OSD::handle_pg_info(MOSDPGInfo *m)
{
  dout(7) << "handle_pg_info " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(m))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(m, m->get_epoch())) return;
  map< int, MOSDPGInfo* > info_map;

  int created = 0;

  for (vector<PG::Info>::iterator p = m->pg_info.begin();
       p != m->pg_info.end();
       ++p) {
    ObjectStore::Transaction *t = 0;
    C_Contexts *fin = 0;
    PG *pg = get_or_create_pg(*p, m->get_epoch(), 
			      from, created, false, &t, &fin);
    if (!pg)
      continue;

    if (pg->old_peering_msg(m->get_epoch(), m->get_epoch())) {
      dout(10) << "ignoring old peering message " << *m << dendl;
      pg->unlock();
      delete t;
      delete fin;
      continue;
    }

    PG::RecoveryCtx rctx(0, &info_map, 0, &fin->contexts, t);

    pg->handle_info(from, *p, &rctx);

    int tr = store->queue_transaction(&pg->osr, t, new ObjectStore::C_DeleteTransaction(t), fin);
    assert(!tr);

    pg->unlock();
  }

  do_infos(info_map);
  if (created)
    update_heartbeat_peers();

  m->put();
}

void OSD::handle_pg_trim(MOSDPGTrim *m)
{
  dout(7) << "handle_pg_trim " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(m))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(m, m->epoch)) return;

  if (!_have_pg(m->pgid)) {
    dout(10) << " don't have pg " << m->pgid << dendl;
  } else {
    PG *pg = _lookup_lock_pg(m->pgid);
    if (m->epoch < pg->info.history.same_interval_since) {
      dout(10) << *pg << " got old trim to " << m->trim_to << ", ignoring" << dendl;
      pg->unlock();
      goto out;
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
      pg->trim(*t, m->trim_to);
      pg->write_info(*t);
      int tr = store->queue_transaction(&pg->osr, t, new ObjectStore::C_DeleteTransaction(t));
      assert(tr == 0);
    }
    pg->unlock();
  }

 out:
  m->put();
}

void OSD::handle_pg_scan(MOSDPGScan *m)
{
  dout(10) << "handle_pg_scan " << *m << " from " << m->get_source() << dendl;
  
  if (!require_osd_peer(m))
    return;
  if (!require_same_or_newer_map(m, m->query_epoch))
    return;

  PG *pg;
  
  if (!_have_pg(m->pgid)) {
    m->put();
    return;
  }

  pg = _lookup_lock_pg(m->pgid);
  assert(pg);

  pg->get();
  enqueue_op(pg, m);
  pg->unlock();
  pg->put();
}

bool OSD::scan_is_queueable(PG *pg, MOSDPGScan *m)
{
  assert(pg->is_locked());

  if (m->query_epoch < pg->info.history.same_interval_since) {
    dout(10) << *pg << " got old scan, ignoring" << dendl;
    m->put();
    return false;
  }

  return true;
}

void OSD::handle_pg_missing(MOSDPGMissing *m)
{
  assert(0); // MOSDPGMissing is fantastical
#if 0
  dout(7) << __func__  << " " << *m << " from " << m->get_source() << dendl;

  if (!require_osd_peer(m))
    return;

  int from = m->get_source().num();
  if (!require_same_or_newer_map(m, m->get_epoch()))
    return;

  map< int, map<pg_t,PG::Query> > query_map;
  PG::Log empty_log;
  int created = 0;
  _pro-cess_pg_info(m->get_epoch(), from, m->info, //misspelling added to prevent erroneous finds
		   empty_log, &m->missing, query_map, NULL, created);
  do_queries(query_map);
  if (created)
    update_heartbeat_peers();

  m->put();
#endif
}

/** PGQuery
 * from primary to replica | stray
 * NOTE: called with opqueue active.
 */
void OSD::handle_pg_query(MOSDPGQuery *m) 
{
  assert(osd_lock.is_locked());

  if (!require_osd_peer(m))
    return;

  dout(7) << "handle_pg_query from " << m->get_source() << " epoch " << m->get_epoch() << dendl;
  int from = m->get_source().num();
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;

  map< int, vector<PG::Info> > notify_list;
  
  for (map<pg_t,PG::Query>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = it->first;
    PG *pg = 0;

    if (pg_map.count(pgid) == 0) {
      // get active crush mapping
      vector<int> up, acting;
      osdmap->pg_to_up_acting_osds(pgid, up, acting);
      int role = osdmap->calc_pg_role(whoami, acting, acting.size());

      // same primary?
      PG::Info::History history = it->second.history;
      project_pg_history(pgid, history, m->get_epoch(), up, acting);

      if (m->get_epoch() < history.same_interval_since) {
        dout(10) << " pg " << pgid << " dne, and pg has changed in "
                 << history.same_interval_since << " (msg from " << m->get_epoch() << ")" << dendl;
        continue;
      }

      assert(role != 0);
      dout(10) << " pg " << pgid << " dne" << dendl;
      PG::Info empty(pgid);
      if (it->second.type == PG::Query::LOG ||
	  it->second.type == PG::Query::BACKLOG ||
	  it->second.type == PG::Query::FULLLOG) {
	MOSDPGLog *mlog = new MOSDPGLog(osdmap->get_epoch(), empty,
					m->get_epoch());
	_share_map_outgoing(osdmap->get_cluster_inst(from));
	cluster_messenger->send_message(mlog,
					osdmap->get_cluster_inst(from));
      } else {
	notify_list[from].push_back(empty);
      }
      continue;
    }

    pg = _lookup_lock_pg(pgid);
    if (m->get_epoch() < pg->info.history.same_interval_since) {
      dout(10) << *pg << " handle_pg_query changed in "
	       << pg->info.history.same_interval_since
	       << " (msg from " << m->get_epoch() << ")" << dendl;
      pg->unlock();
      continue;
    }

    if (pg->old_peering_msg(m->get_epoch(), m->get_epoch())) {
      dout(10) << "ignoring old peering message " << *m << dendl;
      pg->unlock();
      continue;
    }

    if (pg->deleting) {
      /*
       * We cancel deletion on pg change.  And the primary will never
       * query anything it already asked us to delete.  So the only
       * reason we would ever get a query on a deleting pg is when we
       * get an old query from an old primary.. which we can safely
       * ignore.
       */
      dout(0) << *pg << " query on deleting pg" << dendl;
      assert(0 == "this should not happen");
      pg->unlock();
      continue;
    }

    unreg_last_pg_scrub(pg->info.pgid, pg->info.history.last_scrub_stamp);
    pg->info.history.merge(it->second.history);
    reg_last_pg_scrub(pg->info.pgid, pg->info.history.last_scrub_stamp);

    // ok, process query!
    PG::RecoveryCtx rctx(0, 0, &notify_list, 0, 0);
    pg->handle_query(from, it->second, m->get_epoch(), &rctx);
    pg->unlock();
  }
  
  do_notifies(notify_list, m->get_epoch());

  m->put();
}


void OSD::handle_pg_remove(MOSDPGRemove *m)
{
  assert(osd_lock.is_locked());

  if (!require_osd_peer(m))
    return;

  dout(7) << "handle_pg_remove from " << m->get_source() << " on "
	  << m->pg_list.size() << " pgs" << dendl;
  
  if (!require_same_or_newer_map(m, m->get_epoch())) return;
  
  for (vector<pg_t>::iterator it = m->pg_list.begin();
       it != m->pg_list.end();
       it++) {
    pg_t pgid = *it;
    
    if (pg_map.count(pgid) == 0) {
      dout(10) << " don't have pg " << pgid << dendl;
      continue;
    }
    dout(5) << "queue_pg_for_deletion: " << pgid << dendl;
    PG *pg = _lookup_lock_pg(pgid);
    if (pg->info.history.same_interval_since <= m->get_epoch()) {
      if (pg->deleting) {
	dout(10) << *pg << " already removing." << dendl;
      } else {
	assert(pg->get_primary() == m->get_source().num());
	queue_pg_for_deletion(pg);
      }
    } else {
      dout(10) << *pg << " ignoring remove request, pg changed in epoch "
	       << pg->info.history.same_interval_since
	       << " > " << m->get_epoch() << dendl;
    }
    pg->unlock();
  }
  m->put();
}


void OSD::queue_pg_for_deletion(PG *pg)
{
  dout(10) << *pg << " removing." << dendl;
  pg->assert_locked();
  assert(pg->get_role() == -1);
  if (!pg->deleting) {
    pg->deleting = true;
    remove_wq.queue(pg);
  }
}

void OSD::_remove_pg(PG *pg)
{
  pg_t pgid = pg->info.pgid;
  dout(10) << "_remove_pg " << pgid << dendl;
  
  pg->lock();
  if (!pg->deleting) {
    pg->unlock();
    return;
  }
  
  // reset log, last_complete, in case deletion gets canceled
  pg->info.last_complete = eversion_t();
  pg->info.last_update = eversion_t();
  pg->info.log_tail = eversion_t();
  pg->log.zero();
  pg->ondisklog.zero();

  {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    pg->write_info(*t);
    pg->write_log(*t);
    int tr = store->queue_transaction(&pg->osr, t);
    assert(tr == 0);
  }
  
  // flush all pg operations to the fs, so we can rely on
  // collection_list below.
  pg->osr.flush();

  int n = 0;

  ObjectStore::Transaction *rmt = new ObjectStore::Transaction;

  // snap collections
  for (interval_set<snapid_t>::iterator p = pg->snap_collections.begin();
       p != pg->snap_collections.end();
       p++) {
    for (snapid_t cur = p.get_start();
	 cur < p.get_start() + p.get_len();
	 ++cur) {
      vector<hobject_t> olist;      
      store->collection_list(coll_t(pgid, cur), olist);
      dout(10) << "_remove_pg " << pgid << " snap " << cur << " " << olist.size() << " objects" << dendl;
      for (vector<hobject_t>::iterator q = olist.begin();
	   q != olist.end();
	   q++) {
	ObjectStore::Transaction *t = new ObjectStore::Transaction;
	t->remove(coll_t(pgid, cur), *q);
	t->remove(coll_t(pgid), *q);          // we may hit this twice, but it's harmless
	int tr = store->queue_transaction(&pg->osr, t);
	assert(tr == 0);
	
	if ((++n & 0xff) == 0) {
	  pg->unlock();
	  pg->lock();
	  if (!pg->deleting) {
	    dout(10) << "_remove_pg aborted on " << *pg << dendl;
	    pg->unlock();
	    return;
	  }
	}
      }
      rmt->remove_collection(coll_t(pgid, cur));
    }
  }

  // (what remains of the) main collection
  vector<hobject_t> olist;
  store->collection_list(coll_t(pgid), olist);
  dout(10) << "_remove_pg " << pgid << " " << olist.size() << " objects" << dendl;
  for (vector<hobject_t>::iterator p = olist.begin();
       p != olist.end();
       p++) {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->remove(coll_t(pgid), *p);
    int tr = store->queue_transaction(&pg->osr, t);
    assert(tr == 0);

    if ((++n & 0xff) == 0) {
      pg->unlock();
      pg->lock();
      if (!pg->deleting) {
	dout(10) << "_remove_pg aborted on " << *pg << dendl;
	pg->unlock();
	return;
      }
    }
  }

  pg->unlock();

  dout(10) << "_remove_pg " << pgid << " flushing store" << dendl;
  store->flush();
  
  dout(10) << "_remove_pg " << pgid << " taking osd_lock" << dendl;
  osd_lock.Lock();
  pg->lock();
  
  if (!pg->deleting) {
    osd_lock.Unlock();
    pg->unlock();
    return;
  }

  dout(10) << "_remove_pg " << pgid << " removing final" << dendl;

  {
    rmt->remove(coll_t::META_COLL, pg->log_oid);
    rmt->remove(coll_t::META_COLL, pg->biginfo_oid);
    rmt->remove_collection(coll_t(pgid));
    int tr = store->queue_transaction(NULL, rmt);
    assert(tr == 0);
  }


  // on_removal, which calls remove_watchers_and_notifies, and the erasure from 
  // the pg_map must be done together without unlocking the pg lock,
  // to avoid racing with watcher cleanup in ms_handle_reset
  // and handle_notify_timeout
  pg->on_removal();

  // remove from map
  pg_map.erase(pgid);
  pg->put(); // since we've taken it out of map
  unreg_last_pg_scrub(pg->info.pgid, pg->info.history.last_scrub_stamp);

  _put_pool(pg->pool);

  // unlock, and probably delete
  pg->unlock();
  pg->put();  // will delete, if last reference
  osd_lock.Unlock();
  dout(10) << "_remove_pg " << pgid << " all done" << dendl;
}


// =========================================================
// RECOVERY


/*

  there are a few places we need to build a backlog.

  on a primary:
    - during peering 
      - if osd with newest update has log.bottom > our log.top
      - if other peers have log.tops below our log.bottom
        (most common case is they are a fresh osd with no pg info at all)

  on a replica or stray:
    - when queried by the primary (handle_pg_query)

  on a replica:
    - when activated by the primary (handle_pg_log -> merge_log)
    
*/
	
void OSD::queue_generate_backlog(PG *pg)
{
  if (pg->generate_backlog_epoch) {
    dout(10) << *pg << " queue_generate_backlog - already queued epoch " 
	     << pg->generate_backlog_epoch << dendl;
  } else {
    pg->generate_backlog_epoch = osdmap->get_epoch();
    dout(10) << *pg << " queue_generate_backlog epoch " << pg->generate_backlog_epoch << dendl;
    backlog_wq.queue(pg);
  }
}

void OSD::cancel_generate_backlog(PG *pg)
{
  dout(10) << *pg << " cancel_generate_backlog" << dendl;
  pg->generate_backlog_epoch = 0;
  backlog_wq.dequeue(pg);
}

void OSD::generate_backlog(PG *pg)
{
  map<eversion_t,PG::Log::Entry> omap;
  pg->lock();
  dout(10) << *pg << " generate_backlog" << dendl;

  int tr;

  if (!pg->generate_backlog_epoch) {
    dout(10) << *pg << " generate_backlog was canceled" << dendl;
    goto out;
  }

  if (!pg->build_backlog_map(omap))
    goto out;

  {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    C_Contexts *fin = new C_Contexts(g_ceph_context);
    pg->assemble_backlog(omap);
    pg->write_info(*t);
    pg->write_log(*t);
    tr = store->queue_transaction(&pg->osr, t,
				  new ObjectStore::C_DeleteTransaction(t), fin);
    assert(!tr);
  }
  
  // take osd_lock, map_log (read)
  pg->unlock();
  map_lock.get_read();
  pg->lock();

  if (!pg->generate_backlog_epoch) {
    dout(10) << *pg << " generate_backlog aborting" << dendl;
  } else {
    map< int, map<pg_t,PG::Query> > query_map;
    map< int, MOSDPGInfo* > info_map;
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    C_Contexts *fin = new C_Contexts(g_ceph_context);
    PG::RecoveryCtx rctx(&query_map, &info_map, 0, &fin->contexts, t);
    pg->handle_backlog_generated(&rctx);
    do_queries(query_map);
    do_infos(info_map);
    tr = store->queue_transaction(&pg->osr, t,
				  new ObjectStore::C_DeleteTransaction(t), fin);
    assert(!tr);
  }
  map_lock.put_read();

 out:
  pg->generate_backlog_epoch = 0;
  pg->unlock();
  pg->put();
}



void OSD::check_replay_queue()
{
  utime_t now = ceph_clock_now(g_ceph_context);
  list< pair<pg_t,utime_t> > pgids;
  replay_queue_lock.Lock();
  while (!replay_queue.empty() &&
	 replay_queue.front().second <= now) {
    pgids.push_back(replay_queue.front());
    replay_queue.pop_front();
  }
  replay_queue_lock.Unlock();

  for (list< pair<pg_t,utime_t> >::iterator p = pgids.begin(); p != pgids.end(); p++)
    activate_pg(p->first, p->second);
}

/*
 * NOTE: this is called from SafeTimer, so caller holds osd_lock
 */
void OSD::activate_pg(pg_t pgid, utime_t activate_at)
{
  assert(osd_lock.is_locked());

  if (pg_map.count(pgid)) {
    PG *pg = _lookup_lock_pg(pgid);
    dout(10) << "activate_pg " << *pg << dendl;
    if (pg->is_active() &&
	pg->is_replay() &&
	pg->get_role() == 0 &&
	pg->replay_until == activate_at) {
      pg->replay_queued_ops();
    }
    pg->unlock();
  } else {
    dout(10) << "activate_pg pgid " << pgid << " (not found)" << dendl;
  }
  
  // wake up _all_ pg waiters; raw pg -> actual pg mapping may have shifted
  wake_all_pg_waiters();
}


bool OSD::queue_for_recovery(PG *pg)
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
    
    dout(10) << "do_recovery starting " << max
	     << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops) on "
	     << *pg << dendl;
#ifdef DEBUG_RECOVERY_OIDS
    dout(20) << "  active was " << recovery_oids[pg->info.pgid] << dendl;
#endif
    
    int started = pg->start_recovery_ops(max);
    
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
      map< int, map<pg_t,PG::Query> > query_map;
      pg->discover_all_missing(query_map);
      if (query_map.size())
	do_queries(query_map);
      else {
	dout(10) << "do_recovery  no luck, giving up on this pg for now" << dendl;
	recovery_wq.lock();
	pg->recovery_item.remove_myself();	// sigh...
	recovery_wq.unlock();

      }
    }
    else if (started < max) {
      recovery_wq.lock();
      pg->recovery_item.remove_myself();
      recovery_wq.unlock();
    }
    
    pg->unlock();
  }
  pg->put();
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
  dout(10) << "finish_recovery_op " << *pg << " " << soid
	   << " dequeue=" << dequeue
	   << " (" << recovery_ops_active << "/" << g_conf->osd_recovery_max_active << " rops)"
	   << dendl;
  recovery_wq.lock();

  // adjust count
  recovery_ops_active--;
  assert(recovery_ops_active >= 0);

#ifdef DEBUG_RECOVERY_OIDS
  dout(20) << "  active oids was " << recovery_oids[pg->info.pgid] << dendl;
  assert(recovery_oids[pg->info.pgid].count(soid));
  recovery_oids[pg->info.pgid].erase(soid);
#endif

  if (dequeue)
    pg->recovery_item.remove_myself();
  else {
    pg->get();
    recovery_queue.push_front(&pg->recovery_item);  // requeue
  }

  recovery_wq.kick();
  recovery_wq.unlock();
}

void OSD::defer_recovery(PG *pg)
{
  dout(10) << "defer_recovery " << *pg << dendl;

  // move pg to the end of the queue...
  recovery_wq.lock();
  pg->get();
  recovery_queue.push_back(&pg->recovery_item);
  recovery_wq.kick();
  recovery_wq.unlock();
}


// =========================================================
// OPS

void OSD::reply_op_error(MOSDOp *op, int err)
{
  reply_op_error(op, err, eversion_t());
}

void OSD::reply_op_error(MOSDOp *op, int err, eversion_t v)
{
  int flags;
  flags = op->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK);

  MOSDOpReply *reply = new MOSDOpReply(op, err, osdmap->get_epoch(), flags);
  Messenger *msgr = client_messenger;
  reply->set_version(v);
  if (op->get_source().is_osd())
    msgr = cluster_messenger;
  msgr->send_message(reply, op->get_connection());
  op->put();
}

void OSD::handle_misdirected_op(PG *pg, MOSDOp *op)
{
  if (op->get_map_epoch() < pg->info.history.same_primary_since) {
    dout(7) << *pg << " changed after " << op->get_map_epoch() << ", dropping" << dendl;
    op->put();
  } else {
    dout(7) << *pg << " misdirected op in " << op->get_map_epoch() << dendl;
    clog.warn() << op->get_source_inst() << " misdirected "
		<< op->get_reqid() << " " << pg->info.pgid << " to osd." << whoami
		<< " not " << pg->acting
		<< " in e" << op->get_map_epoch() << "/" << osdmap->get_epoch()
		<< "\n";
    reply_op_error(op, -ENXIO);
  }
}

void OSD::handle_op(MOSDOp *op)
{
  if (op_is_discardable(op)) {
    op->put();
    return;
  }

  // we don't need encoded payload anymore
  op->clear_payload();

  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch()))
    return;

  // object name too long?
  if (op->get_oid().name.size() > MAX_CEPH_OBJECT_NAME_LEN) {
    dout(4) << "handle_op '" << op->get_oid().name << "' is longer than "
	    << MAX_CEPH_OBJECT_NAME_LEN << " bytes!" << dendl;
    reply_op_error(op, -ENAMETOOLONG);
    return;
  }

  // blacklisted?
  if (osdmap->is_blacklisted(op->get_source_addr())) {
    dout(4) << "handle_op " << op->get_source_addr() << " is blacklisted" << dendl;
    reply_op_error(op, -EBLACKLISTED);
    return;
  }

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->get_map_epoch(),
		      (Session *)op->get_connection()->get_priv());

  int r = init_op_flags(op);
  if (r) {
    reply_op_error(op, r);
    return;
  }

  if (op->may_write()) {
    // full?
    if (osdmap->test_flag(CEPH_OSDMAP_FULL) &&
	!op->get_source().is_mds()) {  // FIXME: we'll exclude mds writes for now.
      reply_op_error(op, -ENOSPC);
      return;
    }

    // invalid?
    if (op->get_snapid() != CEPH_NOSNAP) {
      reply_op_error(op, -EINVAL);
      return;
    }

    // too big?
    if (g_conf->osd_max_write_size &&
	op->get_data_len() > g_conf->osd_max_write_size << 20) {
      // journal can't hold commit!
      reply_op_error(op, -OSD_WRITETOOBIG);
      return;
    }
  }

  // calc actual pgid
  pg_t pgid = op->get_pg();
  int64_t pool = pgid.pool();
  if ((op->get_flags() & CEPH_OSD_FLAG_PGOP) == 0 &&
      osdmap->have_pg_pool(pool))
    pgid = osdmap->raw_pg_to_pg(pgid);

  // get and lock *pg.
  PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid) : NULL;
  if (!pg) {
    dout(7) << "hit non-existent pg " << pgid 
	    << ", waiting" << dendl;
    waiting_for_pg[pgid].push_back(op);
    return;
  }

  pg->get();
  enqueue_op(pg, op);
  pg->unlock();
  pg->put();
}

bool OSD::op_has_sufficient_caps(PG *pg, MOSDOp *op)
{
  Session *session = (Session *)op->get_connection()->get_priv();
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *op << dendl;
    return false;
  }
  OSDCaps& caps = session->caps;
  session->put();

  int perm = caps.get_pool_cap(pg->pool->name, pg->pool->auid);
  dout(20) << "op_has_sufficient_caps pool=" << pg->pool->id << " (" << pg->pool->name
	   << ") owner=" << pg->pool->auid << " perm=" << perm
	   << " may_read=" << op->may_read()
	   << " may_write=" << op->may_write()
	   << " may_exec=" << op->may_exec()
           << " require_exec_caps=" << op->require_exec_caps() << dendl;

  if (op->may_read() && !(perm & OSD_POOL_CAP_R)) {
    dout(10) << " no READ permission to access pool " << pg->pool->name << dendl;
    return false;
  } else if (op->may_write() && !(perm & OSD_POOL_CAP_W)) {
    dout(10) << " no WRITE permission to access pool " << pg->pool->name << dendl;
    return false;
  } else if (op->require_exec_caps() && !(perm & OSD_POOL_CAP_X)) {
    dout(10) << " no EXEC permission to access pool " << pg->pool->name << dendl;
    return false;
  }
  return true;
}

void OSD::handle_sub_op(MOSDSubOp *op)
{
  dout(10) << "handle_sub_op " << *op << " epoch " << op->map_epoch << dendl;
  if (op->map_epoch < up_epoch) {
    dout(3) << "replica op from before up" << dendl;
    op->put();
    return;
  }

  if (!require_osd_peer(op))
    return;

  // must be a rep op.
  assert(op->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = op->pgid;

  // require same or newer map
  if (!require_same_or_newer_map(op, op->map_epoch))
    return;

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->map_epoch,
		      (Session*)op->get_connection()->get_priv());

  PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid) : NULL;
  if (!pg) {
    op->put();
    return;
  }
  pg->get();
  enqueue_op(pg, op);
  pg->unlock();
  pg->put();
}

void OSD::handle_sub_op_reply(MOSDSubOpReply *op)
{
  if (op->get_map_epoch() < up_epoch) {
    dout(3) << "replica op reply from before up" << dendl;
    op->put();
    return;
  }

  if (!require_osd_peer(op))
    return;

  // must be a rep op.
  assert(op->get_source().is_osd());
  
  // make sure we have the pg
  const pg_t pgid = op->get_pg();

  // require same or newer map
  if (!require_same_or_newer_map(op, op->get_map_epoch())) return;

  // share our map with sender, if they're old
  _share_map_incoming(op->get_source_inst(), op->get_map_epoch(),
		      (Session*)op->get_connection()->get_priv());

  PG *pg = _have_pg(pgid) ? _lookup_lock_pg(pgid) : NULL;
  if (!pg) {
    op->put();
    return;
  }
  pg->get();
  enqueue_op(pg, op);
  pg->unlock();
  pg->put();
}

bool OSD::op_is_discardable(MOSDOp *op)
{
  // drop client request if they are not connected and can't get the
  // reply anyway.  unless this is a replayed op, in which case we
  // want to do what we can to apply it.
  if (!op->get_connection()->is_connected() &&
      op->get_version().version == 0) {
    dout(10) << " sender " << op->get_connection()->get_peer_addr()
	     << " not connected, dropping " << *op << dendl;
    return true;
  }
  return false;
}

/*
 * discard operation, or return true.  no side-effects.
 */
bool OSD::op_is_queueable(PG *pg, MOSDOp *op)
{
  assert(pg->is_locked());

  if (!op_has_sufficient_caps(pg, op)) {
    reply_op_error(op, -EPERM);
    return false;
  }

  if (op_is_discardable(op)) {
    op->put();
    return false;
  }

  // misdirected?
  if (op->may_write()) {
    if (!pg->is_primary() ||
	!pg->same_for_modify_since(op->get_map_epoch())) {
      handle_misdirected_op(pg, op);
      return false;
    }
  } else {
    if (!pg->same_for_read_since(op->get_map_epoch())) {
      handle_misdirected_op(pg, op);
      return false;
    }
  }

  if (!pg->is_active()) {
    dout(7) << *pg << " not active (yet)" << dendl;
    pg->waiting_for_active.push_back(op);
    return false;
  }

  if (pg->is_replay()) {
    if (op->get_version().version > 0) {
      dout(7) << *pg << " queueing replay at " << op->get_version()
	      << " for " << *op << dendl;
      pg->replay_queue[op->get_version()] = op;
      return false;
    }
  }

  return true;
}

/*
 * discard operation, or return true.  no side-effects.
 */
bool OSD::subop_is_queueable(PG *pg, MOSDSubOp *op)
{
  assert(pg->is_locked());

  // same pg?
  //  if pg changes _at all_, we reset and repeer!
  if (op->map_epoch < pg->info.history.same_interval_since) {
    dout(10) << "handle_sub_op pg changed " << pg->info.history
	     << " after " << op->map_epoch 
	     << ", dropping" << dendl;
    op->put();
    return false;
  }

  return true;
}

/*
 * enqueue called with osd_lock held
 */
void OSD::enqueue_op(PG *pg, Message *op)
{
  dout(15) << *pg << " enqueue_op " << op << " " << *op << dendl;
  assert(pg->is_locked());

  switch (op->get_type()) {
  case CEPH_MSG_OSD_OP:
    if (!op_is_queueable(pg, (MOSDOp*)op))
      return;
    break;

  case MSG_OSD_SUBOP:
    if (!subop_is_queueable(pg, (MOSDSubOp*)op))
      return;
    break;

  case MSG_OSD_SUBOPREPLY:
    // don't care.
    break;

  case MSG_OSD_PG_SCAN:
    if (!scan_is_queueable(pg, (MOSDPGScan*)op))
      return;
    break;

  default:
    assert(0 == "enqueued an illegal message type");
  }

  // add to pg's op_queue
  pg->op_queue.push_back(op);
  
  op_wq.queue(pg);
}

bool OSD::OpWQ::_enqueue(PG *pg)
{
  pg->get();
  osd->op_queue.push_back(pg);
  osd->op_queue_len++;
  osd->logger->set(l_osd_opq, osd->op_queue_len);
  return true;
}

PG *OSD::OpWQ::_dequeue()
{
  if (osd->op_queue.empty())
    return NULL;
  PG *pg = osd->op_queue.front();
  osd->op_queue.pop_front();
  osd->op_queue_len--;
  osd->logger->set(l_osd_opq, osd->op_queue_len);
  return pg;
}

/*
 * requeue ops at _front_ of queue.  these are previously queued
 * operations that need to get requeued ahead of anything the dispatch
 * thread is currently chewing on so as not to violate ordering from
 * the clients' perspective.
 */
void OSD::requeue_ops(PG *pg, list<Message*>& ls)
{
  dout(15) << *pg << " requeue_ops " << ls << dendl;
  assert(pg->is_locked());

  // you can't call this on pg->op_queue!
  assert(&ls != &pg->op_queue);

  // set current queue contents aside..
  list<Message*> orig_queue;
  orig_queue.swap(pg->op_queue);

  // grab whole list at once, in case methods we call below start adding things
  // back on the list reference we were passed!
  list<Message*> q;
  q.swap(ls);

  // requeue old items, now at front.
  while (!q.empty()) {
    Message *op = q.front();
    q.pop_front();
    enqueue_op(pg, op);
  }

  // put orig queue contents back in line, after the stuff we requeued.
  pg->op_queue.splice(pg->op_queue.end(), orig_queue);
}

/*
 * NOTE: dequeue called in worker thread, without osd_lock
 */
void OSD::dequeue_op(PG *pg)
{
  Message *op = 0;

  osd_lock.Lock();
  {
    // lock pg and get pending op
    pg->lock();

    assert(!pg->op_queue.empty());
    op = pg->op_queue.front();
    pg->op_queue.pop_front();
    
    dout(10) << "dequeue_op " << *op << " pg " << *pg << dendl;

    // share map?
    //  do this preemptively while we hold osd_lock and pg->lock
    //  to avoid lock ordering issues later.
    for (unsigned i=1; i<pg->acting.size(); i++) 
      _share_map_outgoing( osdmap->get_cluster_inst(pg->acting[i]) );
  }
  osd_lock.Unlock();

  switch (op->get_type()) {
  case CEPH_MSG_OSD_OP:
    if (op_is_discardable((MOSDOp*)op))
      op->put();
    else
      pg->do_op((MOSDOp*)op); // do it now
    break;

  case MSG_OSD_SUBOP:
    pg->do_sub_op((MOSDSubOp*)op);
    break;
    
  case MSG_OSD_SUBOPREPLY:
    pg->do_sub_op_reply((MOSDSubOpReply*)op);
    break;

  case MSG_OSD_PG_SCAN:
    pg->do_scan((MOSDPGScan*)op);
    break;

  default:
    assert(0 == "bad message type in dequeue_op");
  }

  // unlock and put pg
  pg->unlock();
  pg->put();
  
  //#warning foo
  //scrub_wq.queue(pg);

  // finish
  dout(10) << "dequeue_op " << op << " finish" << dendl;
}


// --------------------------------

int OSD::init_op_flags(MOSDOp *op)
{
  vector<OSDOp>::iterator iter;

  // did client explicitly set either bit?
  op->rmw_flags = op->get_flags() & (CEPH_OSD_FLAG_READ|CEPH_OSD_FLAG_WRITE|CEPH_OSD_FLAG_EXEC|CEPH_OSD_FLAG_EXEC_PUBLIC);

  // implicitly set bits based on op codes, called methods.
  for (iter = op->ops.begin(); iter != op->ops.end(); ++iter) {
    if (iter->op.op & CEPH_OSD_OP_MODE_WR)
      op->rmw_flags |= CEPH_OSD_FLAG_WRITE;
    if (iter->op.op & CEPH_OSD_OP_MODE_RD)
      op->rmw_flags |= CEPH_OSD_FLAG_READ;

    // set READ flag if there are src_oids
    if (iter->soid.oid.name.length())
      op->rmw_flags |= CEPH_OSD_FLAG_READ;

    // set PGOP flag if there are PG ops
    if (ceph_osd_op_type_pg(iter->op.op))
      op->rmw_flags |= CEPH_OSD_FLAG_PGOP;

    switch (iter->op.op) {
    case CEPH_OSD_OP_CALL:
      {
	bufferlist::iterator bp = iter->data.begin();
	int is_write, is_read, is_public;
	string cname, mname;
	bp.copy(iter->op.cls.class_len, cname);
	bp.copy(iter->op.cls.method_len, mname);

	ClassHandler::ClassData *cls;
	int r = class_handler->open_class(cname, &cls);
	if (r)
	  return r;
	int flags = cls->get_method_flags(mname.c_str());
	is_read = flags & CLS_METHOD_RD;
	is_write = flags & CLS_METHOD_WR;
        is_public = flags & CLS_METHOD_PUBLIC;

	dout(10) << "class " << cname << " method " << mname
		<< " flags=" << (is_read ? "r" : "") << (is_write ? "w" : "") << dendl;
	if (is_read)
	  op->rmw_flags |= CEPH_OSD_FLAG_READ;
	if (is_write)
	  op->rmw_flags |= CEPH_OSD_FLAG_WRITE;
        if (is_public)
	  op->rmw_flags |= CEPH_OSD_FLAG_EXEC_PUBLIC;
        else
	  op->rmw_flags |= CEPH_OSD_FLAG_EXEC;
	break;
      }
      
    default:
      break;
    }
  }

  return 0;
}
