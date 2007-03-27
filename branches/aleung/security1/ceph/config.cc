// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#include "config.h"
#include "include/types.h"

//#define MDS_CACHE_SIZE        4*10000   -> <20mb
//#define MDS_CACHE_SIZE        80000         62mb

#define AVG_PER_INODE_SIZE    450
#define MDS_CACHE_MB_TO_INODES(x) ((x)*1000000/AVG_PER_INODE_SIZE)

//#define MDS_CACHE_SIZE       MDS_CACHE_MB_TO_INODES( 50 )
//#define MDS_CACHE_SIZE 1500000
#define MDS_CACHE_SIZE 150000


// hack hack hack ugly FIXME
#include "common/Mutex.h"
long buffer_total_alloc = 0;
Mutex bufferlock;



FileLayout g_OSD_FileLayout( 1<<20, 1, 1<<20, 2 );  // stripe over 1M objects, 2x replication
//FileLayout g_OSD_FileLayout( 1<<17, 4, 1<<20 );   // 128k stripes over sets of 4

// ??
//FileLayout g_OSD_MDDirLayout( 1<<8, 1<<2, 1<<19, 3 );  // this is stupid, but can bring out an ebofs table bug?
FileLayout g_OSD_MDDirLayout( 1<<20, 1, 1<<20, 2 );  // 1M objects, 2x replication

// stripe mds log over 128 byte bits (see mds_log_pad_entry below to match!)
FileLayout g_OSD_MDLogLayout( 1<<20, 1, 1<<20, 2 );  // 1M objects
//FileLayout g_OSD_MDLogLayout( 1<<8, 1<<2, 1<<19, 3 );  // 256 byte bits
//FileLayout g_OSD_MDLogLayout( 1<<7, 32, 1<<20, 3 );  // 128 byte stripes over 32 1M objects
//FileLayout g_OSD_MDLogLayout( 57, 32, 1<<20 );  // pathological case to test striping buffer mapping
//FileLayout g_OSD_MDLogLayout( 1<<20, 1, 1<<20 );  // old way

// fake osd failures: osd -> time
std::map<int,float> g_fake_osd_down;
std::map<int,float> g_fake_osd_out;

entity_addr_t g_my_addr;

md_config_t g_debug_after_conf;

md_config_t g_conf = {
  num_mon: 1,
  num_mds: 1,
  num_osd: 4,
  num_client: 1,

  mkfs: false,

  // profiling and debugging
  log: true,
  log_interval: 1,
  log_name: (char*)0,

  log_messages: true,
  log_pins: true,

  fake_clock: false,
  fakemessenger_serialize: true,

  fake_osdmap_expand: 0,
  fake_osdmap_updates: 0,
  fake_osd_mttf: 0,
  fake_osd_mttr: 0,

  osd_remount_at: 0,

  kill_after: 0,

  tick: 0,

  debug: 0,
  debug_mds: 1,
  debug_mds_balancer: 1,
  debug_mds_log: 1,
  debug_buffer: 0,
  debug_filer: 0,
  debug_objecter: 0,
  debug_objectcacher: 0,
  debug_client: 0,
  debug_osd: 0,
  debug_ebofs: 1,
  debug_bdev: 1,         // block device
  debug_ns: 0,
  debug_ms: 0,
  debug_mon: 0,
  
  debug_after: 0,
  
  // -- misc --
  use_abspaths: false,      // make monitorstore et al use absolute path (to workaround FUSE chdir("/"))

  // --- clock ---
  clock_lock: false,
  
  // --- messenger ---
  ms_single_dispatch: false,
  ms_requeue_on_sender_fail: false,

  ms_stripe_osds: false,
  ms_skip_rank0: false,
  ms_overlay_clients: false,

  ms_die_on_failure: false,
  ms_tcp_nodelay: true,

  /*tcp_skip_rank0: false,
  tcp_overlay_clients: false,  // over osds!
  tcp_log: false,
  tcp_serial_marshall: true,
  tcp_serial_out: false,
  tcp_multi_out: true,
  tcp_multi_dispatch: false,  // not fully implemented yet
  */

  // --- mon ---
  mon_tick_interval: 5,
  mon_osd_down_out_interval: 5,  // seconds
  mon_lease: 2.000,  // seconds
  mon_stop_with_last_mds: true,

  // --- client ---
  client_cache_size: 300,
  client_cache_mid: .5,
  client_cache_stat_ttl: 0, // seconds until cached stat results become invalid
  client_cache_readdir_ttl: 1,  // 1 second only
  client_use_random_mds:  false,

  client_sync_writes: 0,

  client_oc: true,
  client_oc_size:      1024*1024* 5,    // MB * n
  client_oc_max_dirty: 1024*1024* 5,    // MB * n
  client_oc_max_sync_write: 128*1024,   // writes >= this use wrlock

  client_trace: 0,
  fuse_direct_io: 0,
  
  // --- objecter ---
  objecter_buffer_uncommitted: true,

  // --- journaler ---
  journaler_allow_split_entries: true,

  // --- mds ---
  mds_cache_size: MDS_CACHE_SIZE,
  mds_cache_mid: .7,

  mds_decay_halflife: 30,

  mds_beacon_interval: 5.0,
  mds_beacon_grace: 10.0,

  mds_log: true,
  mds_log_max_len:  MDS_CACHE_SIZE / 3,
  mds_log_max_trimming: 10000,
  mds_log_read_inc: 1<<20,
  mds_log_pad_entry: 128,//256,//64,
  mds_log_before_reply: true,
  mds_log_flush_on_shutdown: true,
  mds_log_import_map_interval: 1024*1024,  // frequency (in bytes) of EImportMap in log
  mds_bal_replicate_threshold: 2000,
  mds_bal_unreplicate_threshold: 0,//500,
  mds_bal_hash_rd: 10000,
  mds_bal_unhash_rd: 1000,
  mds_bal_hash_wr: 10000,
  mds_bal_unhash_wr: 1000,
  mds_bal_interval: 30,           // seconds
  mds_bal_hash_interval: 5,      // seconds
  mds_bal_idle_threshold: .1,
  mds_bal_max: -1,
  mds_bal_max_until: -1,

  mds_bal_mode: 0,
  mds_bal_min_start: .2,      // if we need less than this, we don't do anything
  mds_bal_need_min: .8,       // take within this range of what we need
  mds_bal_need_max: 1.2,
  mds_bal_midchunk: .3,       // any sub bigger than this taken in full
  mds_bal_minchunk: .001,     // never take anything smaller than this

  mds_commit_on_shutdown: true,
  mds_shutdown_check: 0, //30,
  mds_shutdown_on_last_unmount: true,

  mds_verify_export_dirauth: true,

  mds_local_osd: false,


  // --- osd ---
  osd_rep: OSD_REP_PRIMARY,
  osd_balance_reads: false,
  osd_pg_bits: 0,  // 0 == let osdmonitor decide
  osd_object_layout: OBJECT_LAYOUT_HASHINO,
  osd_pg_layout: PG_LAYOUT_CRUSH,
  osd_max_rep: 4,
  osd_maxthreads: 2,    // 0 == no threading
  osd_max_opq: 10,
  osd_mkfs: false,
  osd_age: .8,
  osd_age_time: 0,
  osd_heartbeat_interval: 5,   // shut up while i'm debugging
  osd_replay_window: 5,
  osd_max_pull: 2,
  osd_pad_pg_log: false,
  
  // --- fakestore ---
  fakestore_fake_sync: 2,    // 2 seconds
  fakestore_fsync: false,//true,
  fakestore_writesync: false,
  fakestore_syncthreads: 4,
  fakestore_fake_attrs: false,
  fakestore_fake_collections: false,   
  fakestore_dev: 0,

  // --- ebofs ---
  ebofs: 1,
  ebofs_cloneable: false,
  ebofs_verify: false,
  ebofs_commit_ms:      2000,       // 0 = no forced commit timeout (for debugging/tracing)
  ebofs_idle_commit_ms: 100,        // 0 = no idle detection.  use this -or- bdev_idle_kick_after_ms
  ebofs_oc_size:        10000,      // onode cache
  ebofs_cc_size:        10000,      // cnode cache
  ebofs_bc_size:        (80 *256), // 4k blocks, *256 for MB
  ebofs_bc_max_dirty:   (60 *256), // before write() will block
  ebofs_max_prefetch: 1000, // 4k blocks
  ebofs_realloc: true,
  
  ebofs_abp_zero: false,          // zero newly allocated buffers (may shut up valgrind)
  ebofs_abp_max_alloc: 4096*16,   // max size of new buffers (larger -> more memory fragmentation)

  // --- obfs ---
  uofs: 0,
  uofs_fake_sync: 2,      // 2 seconds
  uofs_cache_size:             1 << 28,        //256MB
  uofs_onode_size:             (int)1024,
  uofs_small_block_size:       (int)4096,      //4KB
  uofs_large_block_size:       (int)524288,    //512KB
  uofs_segment_size:           (int)268435456, //256MB
  uofs_block_meta_ratio:       (int)10,
  uofs_sync_write:             (int)0,
  uofs_nr_hash_buckets:        (int)1023,
  uofs_flush_interval:         (int)5,         //seconds
  uofs_min_flush_pages:        (int)1024,      //4096 4k-pages
  uofs_delay_allocation:       (int)1,         //true

  // --- block device ---
  bdev_lock: true,
  bdev_iothreads:    1,         // number of ios to queue with kernel
  bdev_idle_kick_after_ms: 0,//100, // ms   ** FIXME ** this seems to break things, not sure why yet **
  bdev_el_fw_max_ms: 10000,      // restart elevator at least once every 1000 ms
  bdev_el_bw_max_ms: 3000,       // restart elevator at least once every 300 ms
  bdev_el_bidir: true,          // bidirectional elevator?
  bdev_iov_max: 512,            // max # iov's to collect into a single readv()/writev() call
  bdev_debug_check_io_overlap: true,   // [DEBUG] check for any pending io overlaps
  bdev_fake_mb: 0,
  bdev_fake_max_mb:  0,

  // --- fakeclient (mds regression testing) (ancient history) ---
  num_fakeclient: 100,
  fakeclient_requests: 100,
  fakeclient_deterministic: false,

  fakeclient_op_statfs:     false,

  // loosely based on Roselli workload paper numbers
  fakeclient_op_stat:     610,
  fakeclient_op_lstat:      false,
  fakeclient_op_utime:    0,
  fakeclient_op_chmod:    1,
  fakeclient_op_chown:    1,

  fakeclient_op_readdir:  2,
  fakeclient_op_mknod:    30,
  fakeclient_op_link:     false,
  fakeclient_op_unlink:   20,
  fakeclient_op_rename:   0,//40,

  fakeclient_op_mkdir:    10,
  fakeclient_op_rmdir:    20,
  fakeclient_op_symlink:  20,

  fakeclient_op_openrd:   200,
  fakeclient_op_openwr:   0,
  fakeclient_op_openwrc:  0,
  fakeclient_op_read:       false,  // osd!
  fakeclient_op_write:      false,  // osd!
  fakeclient_op_truncate:   false,
  fakeclient_op_fsync:      false,
  fakeclient_op_close:    200,

  //security (all principals)
  secure_io:              1, /* 0=off, 1=on */
  mds_group:              0, /* 0=none, 1=unix, 2=batch, 3=define, 4=predict */
  mds_collection:         0, /* 0=none, 1=unix, 3=def */
  unix_group_file:        0, /* 0=no file, non-zero = filename ptr */
  fix_client_id:          0, /* 0=off, 1=on */
  renewal:                0, /* 0=off, 1=on */
  renewal_period:         240, /* renew every 4 minutes */
  config_predict:         0, /* 0=off, non-zero = filename ptr */
  collect_predictions:    0, /* 0=off, 1=on */
  preload_unix_groups:    0, /* 0=off, 1=on */
  client_aux:             0, /* 0=off, 1=on */
  sign_scheme:            0, /* 0=esign, 1=RSA */
  hash_scheme:            0, /* 0=sha-1, 1=sha-256,
				2=sha-512, 3 = md5 */
  crypt_scheme:           0  /* 0=rijndael, 1=RC5 */

#ifdef USE_OSBDB
  ,
  bdbstore: false,
  debug_bdbstore: 1,
  bdbstore_btree: false,
  bdbstore_ffactor: 0,
  bdbstore_nelem: 0,
  bdbstore_pagesize: 0,
  bdbstore_cachesize: 0,
  bdbstore_transactional: false
#endif // USE_OSBDB
};


#include <stdlib.h>
#include <string.h>


void env_to_vec(std::vector<char*>& args) 
{
  const char *p = getenv("CEPH_ARGS");
  if (!p) return;
  
  static char buf[1000];  
  int len = strlen(p);
  memcpy(buf, p, len);
  buf[len] = 0;
  //cout << "CEPH_ARGS " << buf << endl;

  int l = 0;
  for (int i=0; i<len; i++) {
    if (buf[i] == ' ') {
      buf[i] = 0;
      args.push_back(buf+l);
      //cout << "arg " << (buf+l) << endl;
      l = i+1;
    }
  }
  args.push_back(buf+l);
  //cout << "arg " << (buf+l) << endl;
}


void argv_to_vec(int argc, char **argv,
                 std::vector<char*>& args)
{
  for (int i=1; i<argc; i++)
    args.push_back(argv[i]);
}

void vec_to_argv(std::vector<char*>& args,
                 int& argc, char **&argv)
{
  argv = (char**)malloc(sizeof(char*) * argc);
  argc = 1;
  argv[0] = "asdf";

  for (unsigned i=0; i<args.size(); i++) 
    argv[argc++] = args[i];
}

bool parse_ip_port(const char *s, entity_addr_t& a)
{
  int count = 0; // digit count
  int off = 0;

  while (1) {
    // parse the #.
    int val = 0;
    int numdigits = 0;
    
    while (*s >= '0' && *s <= '9') {
      int digit = *s - '0';
      //cout << "digit " << digit << endl;
      val *= 10;
      val += digit;
      numdigits++;
      s++; off++;
    }
    //cout << "val " << val << endl;
    
    if (numdigits == 0) {
      cerr << "no digits at off " << off << endl;
      return false;           // no digits
    }
    if (count < 3 && *s != '.') {
      cerr << "should period at " << off << endl;
      return false;   // should have 3 periods
    }
    if (count == 3 && *s != ':') {
      cerr << "expected : at " << off << endl;
      return false;  // then a colon
    }
    s++; off++;

    if (count <= 3)
      a.ipq[count] = val;
    else
      a.port = val;
    
    count++;
    if (count == 5) break;  
  }
  
  return true;
}



void parse_config_options(std::vector<char*>& args)
{
  std::vector<char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i],"--bind") == 0) 
      assert(parse_ip_port(args[++i], g_my_addr));
    else if (strcmp(args[i], "--nummon") == 0) 
      g_conf.num_mon = atoi(args[++i]);
    else if (strcmp(args[i], "--nummds") == 0) 
      g_conf.num_mds = atoi(args[++i]);
    else if (strcmp(args[i], "--numclient") == 0) 
      g_conf.num_client = atoi(args[++i]);
    else if (strcmp(args[i], "--numosd") == 0) 
      g_conf.num_osd = atoi(args[++i]);

    else if (strcmp(args[i], "--ms_single_dispatch") == 0) 
      g_conf.ms_single_dispatch = atoi(args[++i]);
    else if (strcmp(args[i], "--ms_stripe_osds") == 0)
      g_conf.ms_stripe_osds = true;
    else if (strcmp(args[i], "--ms_skip_rank0") == 0)
      g_conf.ms_skip_rank0 = true;
    else if (strcmp(args[i], "--ms_overlay_clients") == 0)
      g_conf.ms_overlay_clients = true;
    else if (strcmp(args[i], "--ms_die_on_failure") == 0)
      g_conf.ms_die_on_failure = true;

    /*else if (strcmp(args[i], "--tcp_log") == 0)
      g_conf.tcp_log = true;
    else if (strcmp(args[i], "--tcp_multi_out") == 0)
      g_conf.tcp_multi_out = atoi(args[++i]);
    */

    else if (strcmp(args[i], "--mkfs") == 0) 
      g_conf.osd_mkfs = g_conf.mkfs = 1; //atoi(args[++i]);

    else if (strcmp(args[i], "--fake_osdmap_expand") == 0) 
      g_conf.fake_osdmap_expand = atoi(args[++i]);
    else if (strcmp(args[i], "--fake_osdmap_updates") == 0) 
      g_conf.fake_osdmap_updates = atoi(args[++i]);
    else if (strcmp(args[i], "--fake_osd_mttf") == 0) 
      g_conf.fake_osd_mttf = atoi(args[++i]);
    else if (strcmp(args[i], "--fake_osd_mttr") == 0) 
      g_conf.fake_osd_mttr = atoi(args[++i]);
    else if (strcmp(args[i], "--fake_osd_down") == 0) {
      int osd = atoi(args[++i]);
      float when = atof(args[++i]);
      g_fake_osd_down[osd] = when;
    }
    else if (strcmp(args[i], "--fake_osd_out") == 0) {
      int osd = atoi(args[++i]);
      float when = atof(args[++i]);
      g_fake_osd_out[osd] = when;
    }
    else if (strcmp(args[i], "--osd_remount_at") == 0) 
      g_conf.osd_remount_at = atoi(args[++i]);
    //else if (strcmp(args[i], "--fake_osd_sync") == 0) 
    //g_conf.fake_osd_sync = atoi(args[++i]);

    else if (strcmp(args[i], "--debug") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug = atoi(args[++i]);
      else 
        g_debug_after_conf.debug = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_mds") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_mds = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_mds = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_mds_balancer") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_mds_balancer = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_mds_balancer = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_mds_log") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_mds_log = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_mds_log = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_buffer") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_buffer = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_buffer = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_filer") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_filer = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_filer = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_objecter") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_objecter = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_objecter = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_objectcacher") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_objectcacher = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_objectcacher = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_client") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_client = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_client = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_osd") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_osd = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_osd = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_ebofs") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_ebofs = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_ebofs = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_bdev") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_bdev = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_bdev = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_ms") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_ms = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_ms = atoi(args[++i]);
    else if (strcmp(args[i], "--debug_mon") == 0) 
      if (!g_conf.debug_after) 
        g_conf.debug_mon = atoi(args[++i]);
      else 
        g_debug_after_conf.debug_mon = atoi(args[++i]);

    else if (strcmp(args[i], "--debug_after") == 0) {
      g_conf.debug_after = atoi(args[++i]);
      g_debug_after_conf = g_conf;
    }

    else if (strcmp(args[i], "--log") == 0) 
      g_conf.log = atoi(args[++i]);
    else if (strcmp(args[i], "--log_name") == 0) 
      g_conf.log_name = args[++i];

    else if (strcmp(args[i], "--fakemessenger_serialize") == 0) 
      g_conf.fakemessenger_serialize = atoi(args[++i]);


    else if (strcmp(args[i], "--clock_lock") == 0) 
      g_conf.clock_lock = atoi(args[++i]);

    else if (strcmp(args[i], "--objecter_buffer_uncommitted") == 0) 
      g_conf.objecter_buffer_uncommitted = atoi(args[++i]);

    else if (strcmp(args[i], "--mds_cache_size") == 0) 
      g_conf.mds_cache_size = atoi(args[++i]);

    else if (strcmp(args[i], "--mds_beacon_interval") == 0) 
      g_conf.mds_beacon_interval = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_beacon_grace") == 0) 
      g_conf.mds_beacon_grace = atoi(args[++i]);

    else if (strcmp(args[i], "--mds_log") == 0) 
      g_conf.mds_log = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_log_before_reply") == 0) 
      g_conf.mds_log_before_reply = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_log_max_len") == 0) 
      g_conf.mds_log_max_len = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_log_read_inc") == 0) 
      g_conf.mds_log_read_inc = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_log_max_trimming") == 0) 
      g_conf.mds_log_max_trimming = atoi(args[++i]);

    else if (strcmp(args[i], "--mds_commit_on_shutdown") == 0) 
      g_conf.mds_commit_on_shutdown = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_shutdown_check") == 0) 
      g_conf.mds_shutdown_check = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_shutdown_on_last_unmount") == 0) 
      g_conf.mds_shutdown_on_last_unmount = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_log_flush_on_shutdown") == 0) 
      g_conf.mds_log_flush_on_shutdown = atoi(args[++i]);

    else if (strcmp(args[i], "--mds_decay_halflife") == 0) 
      g_conf.mds_decay_halflife = atoi(args[++i]);

    else if (strcmp(args[i], "--mds_bal_interval") == 0) 
      g_conf.mds_bal_interval = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_rep") == 0) 
      g_conf.mds_bal_replicate_threshold = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_unrep") == 0) 
      g_conf.mds_bal_unreplicate_threshold = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_max") == 0) 
      g_conf.mds_bal_max = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_max_until") == 0) 
      g_conf.mds_bal_max_until = atoi(args[++i]);

    else if (strcmp(args[i], "--mds_bal_hash_rd") == 0) 
      g_conf.mds_bal_hash_rd = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_hash_wr") == 0) 
      g_conf.mds_bal_hash_wr = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_unhash_rd") == 0) 
      g_conf.mds_bal_unhash_rd = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_unhash_wr") == 0) 
      g_conf.mds_bal_unhash_wr = atoi(args[++i]);

    else if (strcmp(args[i], "--mds_bal_mode") == 0) 
      g_conf.mds_bal_mode = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_min_start") == 0) 
      g_conf.mds_bal_min_start = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_need_min") == 0) 
      g_conf.mds_bal_need_min = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_need_max") == 0) 
      g_conf.mds_bal_need_max = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_midchunk") == 0) 
      g_conf.mds_bal_midchunk = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_bal_minchunk") == 0) 
      g_conf.mds_bal_minchunk = atoi(args[++i]);
    
    else if (strcmp(args[i], "--mds_local_osd") == 0) 
      g_conf.mds_local_osd = atoi(args[++i]);
    
    else if (strcmp(args[i], "--client_use_random_mds") == 0)
      g_conf.client_use_random_mds = true;
    else if (strcmp(args[i], "--client_cache_size") == 0)
      g_conf.client_cache_size = atoi(args[++i]);
    else if (strcmp(args[i], "--client_cache_stat_ttl") == 0)
      g_conf.client_cache_stat_ttl = atoi(args[++i]);
    else if (strcmp(args[i], "--client_cache_readdir_ttl") == 0)
      g_conf.client_cache_readdir_ttl = atoi(args[++i]);
    else if (strcmp(args[i], "--client_trace") == 0)
      g_conf.client_trace = atoi(args[++i]);
    else if (strcmp(args[i], "--fuse_direct_io") == 0)
      g_conf.fuse_direct_io = atoi(args[++i]);

    else if (strcmp(args[i], "--mon_osd_down_out_interval") == 0)
      g_conf.mon_osd_down_out_interval = atoi(args[++i]);
    else if (strcmp(args[i], "--mon_stop_with_last_mds") == 0)
      g_conf.mon_stop_with_last_mds = atoi(args[++i]);

    else if (strcmp(args[i], "--client_sync_writes") == 0)
      g_conf.client_sync_writes = atoi(args[++i]);
    else if (strcmp(args[i], "--client_oc") == 0)
      g_conf.client_oc = atoi(args[++i]);
    else if (strcmp(args[i], "--client_oc_size") == 0)
      g_conf.client_oc_size = atoi(args[++i]);
    else if (strcmp(args[i], "--client_oc_max_dirty") == 0)
      g_conf.client_oc_max_dirty = atoi(args[++i]);


    else if (strcmp(args[i], "--ebofs") == 0) 
      g_conf.ebofs = 1;
    else if (strcmp(args[i], "--ebofs_cloneable") == 0)
      g_conf.ebofs_cloneable = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_verify") == 0)
      g_conf.ebofs_verify = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_commit_ms") == 0)
      g_conf.ebofs_commit_ms = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_idle_commit_ms") == 0)
      g_conf.ebofs_idle_commit_ms = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_oc_size") == 0)
      g_conf.ebofs_oc_size = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_cc_size") == 0)
      g_conf.ebofs_cc_size = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_bc_size") == 0)
      g_conf.ebofs_bc_size = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_bc_max_dirty") == 0)
      g_conf.ebofs_bc_max_dirty = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_abp_max_alloc") == 0)
      g_conf.ebofs_abp_max_alloc = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_max_prefetch") == 0)
      g_conf.ebofs_max_prefetch = atoi(args[++i]);
    else if (strcmp(args[i], "--ebofs_realloc") == 0)
      g_conf.ebofs_realloc = atoi(args[++i]);


    else if (strcmp(args[i], "--fakestore") == 0) {
      g_conf.ebofs = 0;
      //g_conf.osd_pg_bits = 5;
      //g_conf.osd_maxthreads = 1;   // fucking hell
    }
    else if (strcmp(args[i], "--fakestore_fsync") == 0) 
      g_conf.fakestore_fsync = atoi(args[++i]);
    else if (strcmp(args[i], "--fakestore_writesync") == 0) 
      g_conf.fakestore_writesync = atoi(args[++i]);
    else if (strcmp(args[i], "--fakestore_dev") == 0) 
      g_conf.fakestore_dev = args[++i];
    else if (strcmp(args[i], "--fakestore_fake_attrs") == 0) 
      g_conf.fakestore_fake_attrs = true;//atoi(args[++i]);
    else if (strcmp(args[i], "--fakestore_fake_collections") == 0) 
      g_conf.fakestore_fake_collections = true;//atoi(args[++i]);

    else if (strcmp(args[i], "--obfs") == 0) {
      g_conf.uofs = 1;
      g_conf.osd_maxthreads = 1;   // until feng merges joel's fixes
    }


    else if (strcmp(args[i], "--osd_balance_reads") == 0) 
      g_conf.osd_balance_reads = atoi(args[++i]);
    else if (strcmp(args[i], "--osd_rep") == 0) 
      g_conf.osd_rep = atoi(args[++i]);
    else if (strcmp(args[i], "--osd_rep_chain") == 0) 
      g_conf.osd_rep = OSD_REP_CHAIN;
    else if (strcmp(args[i], "--osd_rep_splay") == 0) 
      g_conf.osd_rep = OSD_REP_SPLAY;
    else if (strcmp(args[i], "--osd_rep_primary") == 0) 
      g_conf.osd_rep = OSD_REP_PRIMARY;
    else if (strcmp(args[i], "--osd_mkfs") == 0) 
      g_conf.osd_mkfs = atoi(args[++i]);
    else if (strcmp(args[i], "--osd_age") == 0) 
      g_conf.osd_age = atof(args[++i]);
    else if (strcmp(args[i], "--osd_age_time") == 0) 
      g_conf.osd_age_time = atoi(args[++i]);
    else if (strcmp(args[i], "--osd_pg_bits") == 0) 
      g_conf.osd_pg_bits = atoi(args[++i]);
    else if (strcmp(args[i], "--osd_max_rep") == 0) 
      g_conf.osd_max_rep = atoi(args[++i]);
    else if (strcmp(args[i], "--osd_maxthreads") == 0) 
      g_conf.osd_maxthreads = atoi(args[++i]);
    else if (strcmp(args[i], "--osd_max_pull") == 0) 
      g_conf.osd_max_pull = atoi(args[++i]);
    else if (strcmp(args[i], "--osd_pad_pg_log") == 0) 
      g_conf.osd_pad_pg_log = atoi(args[++i]);


    else if (strcmp(args[i], "--bdev_lock") == 0) 
      g_conf.bdev_lock = atoi(args[++i]);
    else if (strcmp(args[i], "--bdev_el_bidir") == 0) 
      g_conf.bdev_el_bidir = atoi(args[++i]);
    else if (strcmp(args[i], "--bdev_iothreads") == 0) 
      g_conf.bdev_iothreads = atoi(args[++i]);
    else if (strcmp(args[i], "--bdev_idle_kick_after_ms") == 0) 
      g_conf.bdev_idle_kick_after_ms = atoi(args[++i]);
    else if (strcmp(args[i], "--bdev_fake_mb") == 0) 
      g_conf.bdev_fake_mb = atoi(args[++i]);
    else if (strcmp(args[i], "--bdev_fake_max_mb") == 0) 
      g_conf.bdev_fake_max_mb = atoi(args[++i]);

    else if (strcmp(args[i], "--osd_object_layout") == 0) {
      i++;
      if (strcmp(args[i], "linear") == 0) g_conf.osd_object_layout = OBJECT_LAYOUT_LINEAR;
      else if (strcmp(args[i], "hashino") == 0) g_conf.osd_object_layout = OBJECT_LAYOUT_HASHINO;
      else if (strcmp(args[i], "hash") == 0) g_conf.osd_object_layout = OBJECT_LAYOUT_HASH;
      else assert(0);
    }
    
    else if (strcmp(args[i], "--osd_pg_layout") == 0) {
      i++;
      if (strcmp(args[i], "linear") == 0) g_conf.osd_pg_layout = PG_LAYOUT_LINEAR;
      else if (strcmp(args[i], "hash") == 0) g_conf.osd_pg_layout = PG_LAYOUT_HASH;
      else if (strcmp(args[i], "hybrid") == 0) g_conf.osd_pg_layout = PG_LAYOUT_HYBRID;
      else if (strcmp(args[i], "crush") == 0) g_conf.osd_pg_layout = PG_LAYOUT_CRUSH;
      else assert(0);
    }
    
    else if (strcmp(args[i], "--kill_after") == 0) 
      g_conf.kill_after = atoi(args[++i]);
    else if (strcmp(args[i], "--tick") == 0) 
      g_conf.tick = atoi(args[++i]);

    // security flag to turn off security
    //else if (strcmp(args[i], "--no_sec") == 0)
    else if(strcmp(args[i], "--secure_io") == 0)
      g_conf.secure_io = atoi(args[++i]);
      //g_conf.secure_io = false;
    else if (strcmp(args[i], "--mds_group") == 0)
      g_conf.mds_group = atoi(args[++i]);
    else if (strcmp(args[i], "--mds_collection") == 0)
      g_conf.mds_collection = atoi(args[++i]);
    else if (strcmp(args[i], "--client_aux") == 0)
      g_conf.client_aux = atoi(args[++i]);
    else if (strcmp(args[i], "--unix_group_file") == 0)
      g_conf.unix_group_file = args[++i];
    else if (strcmp(args[i], "--fix_client_id") == 0)
      g_conf.fix_client_id = atoi(args[++i]);
    else if (strcmp(args[i], "--renewal") == 0)
      g_conf.renewal = atoi(args[++i]);
    else if (strcmp(args[i], "--renewal_period") == 0)
      g_conf.renewal_period = atoi(args[++i]);
    else if (strcmp(args[i], "--config_predict") == 0)
      g_conf.config_predict = args[++i];
    else if (strcmp(args[i], "--collect_predictions") == 0)
      g_conf.collect_predictions = atoi(args[++i]);
    else if (strcmp(args[i], "--preload_unix_groups") == 0)
      g_conf.preload_unix_groups = atoi(args[++i]);

    else if (strcmp(args[i], "--file_layout_ssize") == 0) 
      g_OSD_FileLayout.stripe_size = atoi(args[++i]);
    else if (strcmp(args[i], "--file_layout_scount") == 0) 
      g_OSD_FileLayout.stripe_count = atoi(args[++i]);
    else if (strcmp(args[i], "--file_layout_osize") == 0) 
      g_OSD_FileLayout.object_size = atoi(args[++i]);
    else if (strcmp(args[i], "--file_layout_num_rep") == 0) 
      g_OSD_FileLayout.num_rep = atoi(args[++i]);
    else if (strcmp(args[i], "--meta_dir_layout_ssize") == 0) 
      g_OSD_MDDirLayout.stripe_size = atoi(args[++i]);
    else if (strcmp(args[i], "--meta_dir_layout_scount") == 0) 
      g_OSD_MDDirLayout.stripe_count = atoi(args[++i]);
    else if (strcmp(args[i], "--meta_dir_layout_osize") == 0) 
      g_OSD_MDDirLayout.object_size = atoi(args[++i]);
    else if (strcmp(args[i], "--meta_dir_layout_num_rep") == 0) 
      g_OSD_MDDirLayout.num_rep = atoi(args[++i]);
    else if (strcmp(args[i], "--meta_log_layout_ssize") == 0) 
      g_OSD_MDLogLayout.stripe_size = atoi(args[++i]);
    else if (strcmp(args[i], "--meta_log_layout_scount") == 0) 
      g_OSD_MDLogLayout.stripe_count = atoi(args[++i]);
    else if (strcmp(args[i], "--meta_log_layout_osize") == 0) 
      g_OSD_MDLogLayout.object_size = atoi(args[++i]);
    else if (strcmp(args[i], "--meta_log_layout_num_rep") == 0) {
      g_OSD_MDLogLayout.num_rep = atoi(args[++i]);
      if (!g_OSD_MDLogLayout.num_rep)
        g_conf.mds_log = false;
    }

#ifdef USE_OSBDB
    else if (strcmp(args[i], "--bdbstore") == 0) {
      g_conf.bdbstore = true;
      g_conf.ebofs = 0;
    }
    else if (strcmp(args[i], "--bdbstore-btree") == 0) {
      g_conf.bdbstore_btree = true;
    }
    else if (strcmp(args[i], "--bdbstore-hash-ffactor") == 0) {
      g_conf.bdbstore_ffactor = atoi(args[++i]);
    }
    else if (strcmp(args[i], "--bdbstore-hash-nelem") == 0) {
      g_conf.bdbstore_nelem = atoi(args[++i]);
    }
    else if (strcmp(args[i], "--bdbstore-hash-pagesize") == 0) {
      g_conf.bdbstore_pagesize = atoi(args[++i]);
    }
    else if (strcmp(args[i], "--bdbstore-cachesize") == 0) {
      g_conf.bdbstore_cachesize = atoi(args[++i]);
    }
    else if (strcmp(args[i], "--bdbstore-transactional") == 0) {
      g_conf.bdbstore_transactional = true;
    }
    else if (strcmp(args[i], "--debug-bdbstore") == 0) {
      g_conf.debug_bdbstore = atoi(args[++i]);
    }
#endif // USE_OSBDB

    else {
      nargs.push_back(args[i]);
    }
  }

  args = nargs;
}
