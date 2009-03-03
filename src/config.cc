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


#include "config.h"
#include "include/types.h"

#include "common/Clock.h"

#include <fstream>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>

// for tstring stringtable
#include "include/tstring.h"
stringtable g_stab;

// hack hack hack ugly FIXME
#include "include/atomic.h"
atomic_t buffer_total_alloc;

#include "osd/osd_types.h"

#include "common/ConfUtils.h"

/*
struct foobar {
  foobar() { cerr << "config.cc init" << std::endl; }
  ~foobar() { cerr << "config.cc shutdown" << std::endl; }
} asdf;
*/

int buffer::list::read_file(const char *fn)
{
  struct stat st;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    cerr << "can't open " << fn << ": " << strerror(errno) << std::endl;
    return -errno;
  }
  ::fstat(fd, &st);
  int s = ROUND_UP_TO(st.st_size, PAGE_SIZE);
  bufferptr bp = buffer::create_page_aligned(s);
  bp.set_length(st.st_size);
  append(bp);
  ::read(fd, (void*)c_str(), length());
  ::close(fd);
  return 0;
}

int buffer::list::write_file(const char *fn)
{
  int fd = ::open(fn, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd < 0) {
    cerr << "can't write " << fn << ": " << strerror(errno) << std::endl;
    return -errno;
  }
  for (std::list<ptr>::const_iterator it = _buffers.begin(); 
       it != _buffers.end(); 
       it++) {
    const char *c = it->c_str();
    int left = it->length();
    while (left > 0) {
      int r = ::write(fd, c, left);
      if (r < 0) {
	::close(fd);
	return -errno;
      }
      c += r;
      left -= r;
    }
  }
  ::close(fd);
  return 0;
}



// page size crap, see page.h
int _get_bits_of(int v) {
  int n = 0;
  while (v) {
    n++;
    v = v >> 1;
  }
  return n;
}
unsigned _page_size = sysconf(_SC_PAGESIZE);
unsigned long _page_mask = ~(unsigned long)(_page_size - 1);
unsigned _page_shift = _get_bits_of(_page_size);

atomic_t _num_threads(0);

// file layouts
struct ceph_file_layout g_default_file_layout = {
 fl_stripe_unit: init_le32(1<<22),
 fl_stripe_count: init_le32(1),
 fl_object_size: init_le32(1<<22),
 fl_cas_hash: init_le32(0),
 fl_object_stripe_unit: init_le32(0),
 fl_pg_preferred: init_le32(-1),
 fl_pg_type: CEPH_PG_TYPE_REP,
 fl_pg_size: 2,
 fl_pg_pool: 1
};

struct ceph_file_layout g_default_casdata_layout = {
 fl_stripe_unit: init_le32(1<<22),
 fl_stripe_count: init_le32(1),
 fl_object_size: init_le32(1<<22),
 fl_cas_hash: init_le32(0),
 fl_object_stripe_unit: init_le32(0),
 fl_pg_preferred: init_le32(-1),
 fl_pg_type: CEPH_PG_TYPE_REP,
 fl_pg_size: 2,
 fl_pg_pool: 2
};

struct ceph_file_layout g_default_mds_dir_layout = {
 fl_stripe_unit: init_le32(1<<22),
 fl_stripe_count: init_le32(1),
 fl_object_size: init_le32(1<<22),
 fl_cas_hash: init_le32(0),
 fl_object_stripe_unit: init_le32(0),
 fl_pg_preferred: init_le32(-1),
 fl_pg_type: CEPH_PG_TYPE_REP,
 fl_pg_size: 2,
 fl_pg_pool: 0
};

struct ceph_file_layout g_default_mds_log_layout = {
 fl_stripe_unit: init_le32(1<<20),
 fl_stripe_count: init_le32(1),
 fl_object_size: init_le32(1<<20),
 fl_cas_hash: init_le32(0),
 fl_object_stripe_unit: init_le32(0),
 fl_pg_preferred: init_le32(-1),
 fl_pg_type: CEPH_PG_TYPE_REP,
 fl_pg_size: 2,
 fl_pg_pool: 0
};

struct ceph_file_layout g_default_mds_anchortable_layout = {
 fl_stripe_unit: init_le32(1<<20),
 fl_stripe_count: init_le32(1),
 fl_object_size: init_le32(1<<20),
 fl_cas_hash: init_le32(0),
 fl_object_stripe_unit: init_le32(0),
 fl_pg_preferred: init_le32(-1),
 fl_pg_type: CEPH_PG_TYPE_REP,
 fl_pg_size: 2,
 fl_pg_pool: 0
};

const char *get_pool_name(int pool) 
{
  switch (pool) {
  case 0: return "metadata";
  case 1: return "data";
  case 2: return "casdata";
  default: return "";
  }
}

#include <msg/msg_types.h>

// fake osd failures: osd -> time
std::map<entity_name_t,float> g_fake_kill_after;

entity_addr_t g_my_addr;

md_config_t g_debug_after_conf;
md_config_t g_conf;
#if 0
md_config_t g_conf = {
  num_mon: 1,
  num_mds: 1,
  num_osd: 4,
  num_client: 1,

  monmap_file: ".ceph_monmap",
  mon_host: 0,
  daemonize: false,

  // profiling and debugging
  logger: true,
  logger_interval: 1,
  logger_calc_variance: true,
  logger_subdir: 0,
  logger_dir: INSTALL_PREFIX "/var/log/ceph/stat",

  log_dir: INSTALL_PREFIX "/var/log/ceph",        // if daemonize == true
  log_sym_dir: INSTALL_PREFIX "/var/log/ceph",    // if daemonize == true
  log_to_stdout: true,

  pid_file: 0,

  conf_file: INSTALL_PREFIX "/etc/ceph/ceph.conf",

  dump_conf: false,

  chdir_root: true,  // chdir("/") after daemonizing. if true, we generate absolute paths as needed.
  
  fake_clock: false,
  fakemessenger_serialize: true,

  kill_after: 0,

  debug: 0,
  debug_lockdep: 0,
  debug_mds: 1,
  debug_mds_balancer: 1,
  debug_mds_log: 1,
  debug_mds_log_expire: 1,
  debug_mds_migrator: 1,
  debug_buffer: 0,
  debug_timer: 0,
  debug_filer: 0,
  debug_objecter: 0,
  debug_journaler: 0,
  debug_objectcacher: 0,
  debug_client: 0,
  debug_osd: 0,
  debug_ebofs: 1,
  debug_filestore: 1,
  debug_journal: 1,
  debug_bdev: 1,         // block device
  debug_ns: 0,
  debug_ms: 0,
  debug_mon: 1,
  debug_paxos: 0,
  debug_tp: 0,
  
  debug_after: 0,
  
  // --- clock ---
  clock_lock: false,
  clock_tare: false,
  
  // --- messenger ---
  ms_tcp_nodelay: true,
  ms_retry_interval: 2.0,  // how often to attempt reconnect 
  ms_fail_interval: 15.0,  // fail after this long
  ms_die_on_failure: false,
  ms_nocrc: false,


  // --- mon ---
  mon_tick_interval: 5,
  mon_osd_down_out_interval: 5,  // seconds
  mon_lease: 5,  // seconds    // lease interval
  mon_lease_renew_interval: 3, // on leader, to renew the lease
  mon_lease_ack_timeout: 10.0, // on leader, if lease isn't acked by all peons
  mon_lease_timeout: 10.0,     // on peon, if lease isn't extended
  mon_accept_timeout: 10.0,    // on leader, if paxos update isn't accepted
  mon_stop_on_last_unmount: false,
  mon_stop_with_last_mds: false,
  mon_allow_mds_bully: false,   // allow a booting mds to (forcibly) claim an mds # .. FIXME
  mon_pg_create_interval: 30.0, // no more than every 30s

  paxos_propose_interval: 1.0,  // gather updates for this long before proposing a map update
  paxos_observer_timeout: 5*60,  // gather updates for this long before proposing a map update

  // --- client ---
  client_cache_size: 1000,
  client_cache_mid: .5,
  client_cache_stat_ttl: 0, // seconds until cached stat results become invalid
  client_cache_readdir_ttl: 1,  // 1 second only
  client_use_random_mds:  false,
  client_mount_timeout: 10.0,  // retry every N seconds
  client_tick_interval: 1.0,
  client_hack_balance_reads: false,
  client_trace: 0,
  client_readahead_min: 128*1024,  // readahead at _least_ this much.
  client_readahead_max_bytes: 0,//8 * 1024*1024,
  client_readahead_max_periods: 4,  // as multiple of file layout period (object size * num stripes)
  client_snapdir: ".snap",
  fuse_direct_io: 0,
  fuse_ll: true,
  
  // --- objectcacher ---
  client_oc: true,
  client_oc_size:      1024*1024* 64,    // MB * n
  client_oc_max_dirty: 1024*1024* 48,    // MB * n  (dirty OR tx.. bigish)
  client_oc_target_dirty:  1024*1024* 8, // target dirty (keep this smallish)
  // note: the max amount of "in flight" dirty data is roughly (max - target)
  client_oc_max_sync_write: 128*1024,   // sync writes >= this use wrlock

  // --- objecter ---
  objecter_buffer_uncommitted: true,  // this must be true for proper failure handling
  objecter_map_request_interval: 15.0, // request a new map every N seconds, if we have pending io
  objecter_tick_interval: 5.0,
  objecter_timeout: 10.0,    // before we ask for a map

  // --- journaler ---
  journaler_allow_split_entries: true,
  journaler_safe: true,  // wait for COMMIT on journal writes
  journaler_write_head_interval: 15,
  journaler_cache: false, // cache writes for later readback
  journaler_prefetch_periods: 50,   // * journal object size (1~MB? see above)
  journaler_batch_interval: .001,   // seconds.. max add'l latency we artificially incur
  //journaler_batch_max: 16384,        // max bytes we'll delay flushing
  journaler_batch_max: 0,  // disable, for now....

  // --- mds ---
  mds_cache_size: 300000,
  mds_cache_mid: .7,

  mds_decay_halflife: 5,

  mds_beacon_interval: 4, //30.0,
  mds_beacon_grace: 15, //60*60.0,
  mds_blacklist_interval: 24.0*60.0,  // how long to blacklist failed nodes

  mds_session_timeout: 60,    // cap bits and leases time out if client idle
  mds_session_autoclose: 300, // autoclose idle session 
  mds_client_lease: 120,      // (assuming session stays alive)
  mds_reconnect_timeout: 30,  // seconds to wait for clients during mds restart
                              //  make it (mds_session_timeout - mds_beacon_grace)

  mds_tick_interval: 5,
  mds_scatter_nudge_interval: 5,  // how quickly dirstat changes propagate up the hierarchy

  mds_client_prealloc_inos: 1000,
  mds_early_reply: true,

  mds_rdcap_ttl_ms: 60*1000,

  mds_log: true,
  mds_log_unsafe: false,      // only wait for log sync, when it's mostly safe to do so
  mds_log_max_events: -1,
  mds_log_max_segments: 100,  // segment size defined by FileLayout, above
  mds_log_max_expiring: 20,
  mds_log_pad_entry: 128,//256,//64,
  mds_log_eopen_size: 100,   // # open inodes per log entry

  mds_bal_sample_interval: 3.0,  // every 5 seconds
  mds_bal_replicate_threshold: 8000,
  mds_bal_unreplicate_threshold: 0,//500,
  mds_bal_frag: true,
  mds_bal_split_size: 10000,
  mds_bal_split_rd: 25000,
  mds_bal_split_wr: 10000,
  mds_bal_merge_size: 50,
  mds_bal_merge_rd: 1000,
  mds_bal_merge_wr: 1000,
  mds_bal_interval: 10,           // seconds
  mds_bal_fragment_interval: -1,      // seconds
  mds_bal_idle_threshold: 0, //.1,
  mds_bal_max: -1,
  mds_bal_max_until: -1,

  mds_bal_mode: 0,
  mds_bal_min_rebalance: .1,  // must be this much above average before we export anything
  mds_bal_min_start: .2,      // if we need less than this, we don't do anything
  mds_bal_need_min: .8,       // take within this range of what we need
  mds_bal_need_max: 1.2,
  mds_bal_midchunk: .3,       // any sub bigger than this taken in full
  mds_bal_minchunk: .001,     // never take anything smaller than this

  mds_trim_on_rejoin: true,
  mds_shutdown_check: 0, //30,

  mds_verify_export_dirauth: true,

  mds_local_osd: false,

  mds_thrash_exports: 0,
  mds_thrash_fragments: 0,
  mds_dump_cache_on_map: false,
  mds_dump_cache_after_rejoin: true,

  mds_hack_log_expire_for_better_stats: false,

  // --- osd ---
  osd_balance_reads: false,  // send from client to replica
  osd_flash_crowd_iat_threshold: 0,//100,
  osd_flash_crowd_iat_alpha: 0.125,
  osd_balance_reads_temp: 100,
  
  osd_shed_reads: false,     // forward from primary to replica
  osd_shed_reads_min_latency: .01,       // min local latency
  osd_shed_reads_min_latency_diff: .01,  // min latency difference
  osd_shed_reads_min_latency_ratio: 1.5,  // 1.2 == 20% higher than peer

  osd_immediate_read_from_cache: false,//true, // osds to read from the cache immediately?
  osd_exclusive_caching: true,         // replicas evict replicated writes

  osd_stat_refresh_interval: .5,

  osd_min_pg_size_without_alive: 2,  // smallest pg we allow to activate without telling the monitor

  osd_pg_bits: 6,  // bits per osd
  osd_lpg_bits: 1,  // bits per osd
  osd_object_layout: CEPH_OBJECT_LAYOUT_HASHINO,//LINEAR,//HASHINO,
  osd_pg_layout: CEPH_PG_LAYOUT_CRUSH,//LINEAR,//CRUSH,
  osd_min_rep: 2,
  osd_max_rep: 3,
  osd_min_raid_width: 3,
  osd_max_raid_width: 2, //6, 

  osd_maxthreads: 2,    // 0 == no threading
  osd_max_opq: 10,
  osd_age: .8,
  osd_age_time: 0,
  osd_heartbeat_interval: 1,
  osd_mon_heartbeat_interval: 30,  // if no peers, ping monitor
  osd_heartbeat_grace: 20,
  osd_mon_report_interval:  5,  // pg stats, failures, up_thru, boot.
  osd_replay_window: 45,
  osd_max_pull: 2,
  osd_preserve_trimmed_log: true,

  osd_recovery_delay_start: 15,
  osd_recovery_max_active: 5,

  osd_auto_weight: false,

  
  // --- filestore ---
  filestore: false,
  filestore_sync_interval: .2,    // seconds
  filestore_fake_attrs: false,
  filestore_fake_collections: false,   
  filestore_dev: 0,
  filestore_btrfs_trans: true,

  // --- ebofs ---
  ebofs: false,
  ebofs_cloneable: true,
  ebofs_verify: false,
  ebofs_commit_ms:      200,       // 0 = no forced commit timeout (for debugging/tracing)
  ebofs_oc_size:        10000,      // onode cache
  ebofs_cc_size:        10000,      // cnode cache
  ebofs_bc_size:        (50 *256), // 4k blocks, *256 for MB
  ebofs_bc_max_dirty:   (30 *256), // before write() will block
  ebofs_max_prefetch: 1000, // 4k blocks
  ebofs_realloc: false,    // hrm, this can cause bad fragmentation, don't use!
  ebofs_verify_csum_on_read: true,

  // journal
  journal_dio: false,
  journal_max_write_bytes: 0,
  journal_max_write_entries: 100,

  // --- block device ---
  bdev_lock: true,
  bdev_iothreads:   1,         // number of ios to queue with kernel
  bdev_idle_kick_after_ms: 100,  // ms
  bdev_el_fw_max_ms: 10000,      // restart elevator at least once every 1000 ms
  bdev_el_bw_max_ms: 3000,       // restart elevator at least once every 300 ms
  bdev_el_bidir: false,          // bidirectional elevator?
  bdev_iov_max: 512,            // max # iov's to collect into a single readv()/writev() call
  bdev_debug_check_io_overlap: true,   // [DEBUG] check for any pending io overlaps
  bdev_fake_mb: 0,
  bdev_fake_max_mb:  0

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
#endif

#include <stdlib.h>
#include <string.h>


void env_to_vec(std::vector<const char*>& args) 
{
  char *p = getenv("CEPH_ARGS");
  if (!p) return;
  
  int len = MIN(strlen(p), 1000);  // bleh.
  static char buf[1000];  
  memcpy(buf, p, len);
  buf[len] = 0;
  //cout << "CEPH_ARGS='" << p << ";" << endl;

  p = buf;
  while (*p && p < buf + len) {
    char *e = p;
    while (*e && *e != ' ')
      e++;
    *e = 0;
    args.push_back(p);
    //cout << "arg " << p << std::endl;
    p = e+1;
  }
}


void argv_to_vec(int argc, const char **argv,
                 std::vector<const char*>& args)
{
  for (int i=1; i<argc; i++)
    args.push_back(argv[i]);
}

void vec_to_argv(std::vector<const char*>& args,
                 int& argc, const char **&argv)
{
  argv = (const char**)malloc(sizeof(char*) * argc);
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

    if (numdigits == 0) {
      cerr << "no digits at off " << off << std::endl;
      return false;           // no digits
    }
    if (count < 3 && *s != '.') {
      cerr << "should period at " << off << std::endl;
      return false;   // should have 3 periods
    }
    s++; off++;

    if (count <= 3)
      a.set_ipquad(count, val);
    else 
      a.set_port(val);
    
    count++;
    if (count == 4 && *(s-1) != ':') break;
    if (count == 5) break;  
  }
  
  return true;
}



void parse_config_option_string(string& s)
{
  char b[s.length()+1];
  strcpy(b, s.c_str());
  vector<const char*> nargs;
  char *p = b;
  while (*p) {
    nargs.push_back(p);
    while (*p && *p != ' ') p++;
    if (!*p)
      break;
    *p++ = 0;
    while (*p && *p == ' ') p++;
  }
  preparse_config_options(nargs, false);
  parse_config_options(nargs, false);
}

void sighup_handler(int signum)
{
  _dout_need_open = true;
}

#define STRINGIFY(x) #x

typedef enum {
	NONE, INT, LONGLONG, STR, DOUBLE, FLOAT, BOOL
} opt_type_t;



struct config_option {
	const char *section;
	const char *conf_name;
	const char *name;
	void *val_ptr;
       
	const char *def_val;
	opt_type_t type;
	char char_option;  // if any
};

#define OPTION_DEF(section, name, schar, type, def_val) \
       { STRINGIFY(section), NULL, STRINGIFY(name), \
         &g_conf.name, STRINGIFY(def_val), type, schar }

#define OPTION_STR(section, name, schar, type, def_val) \
       { STRINGIFY(section), NULL, STRINGIFY(name), \
         &g_conf.name, def_val, type, schar }

#define OPTION_BOOL OPTION_DEF
#define OPTION_INT OPTION_DEF
#define OPTION_LONGLONG OPTION_DEF
#define OPTION_FLOAT OPTION_DEF
#define OPTION_DOUBLE OPTION_DEF

#define OPTION(section, name, schar, type, def_val) OPTION_##type(section, name, schar, type, def_val)

static struct config_option config_optionsp[] = {
	OPTION(global, num_mon, 0, INT, 1),
	OPTION(global, num_mds, 0, INT, 1),
	OPTION(global, num_osd, 0, INT, 4),
	OPTION(global, num_client, 0, INT, 1),
	OPTION(mon, monmap_file, 0, STR, ".ceph_monmap"),
	OPTION(mon, mon_host, 'm', STR, 0),
	OPTION(global, daemonize, 'd', BOOL, false),
	OPTION(global, logger, 0, BOOL, true),
	OPTION(global, logger_interval, 0, INT, 1),
	OPTION(global, logger_calc_variance, 0, BOOL, true),
	OPTION(global, logger_subdir, 0, STR, 0),
	OPTION(global, logger_dir, 0, STR, INSTALL_PREFIX "/var/log/ceph/stat"),
	OPTION(global, log_dir, 0, STR, INSTALL_PREFIX "/var/log/ceph"),
	OPTION(global, log_sym_dir, 0, STR, INSTALL_PREFIX "/var/log/ceph"),
	OPTION(global, log_to_stdout, 0, BOOL, true),
	OPTION(global, pid_file, 'p', STR, 0),
	OPTION(global, conf_file, 'c', STR, INSTALL_PREFIX "etc/ceph/ceph.conf"),
	OPTION(global, dump_conf, 0, BOOL, false),
	OPTION(global, chdir_root, 0, BOOL, true),
	OPTION(global, fake_clock, 0, BOOL, false),
	OPTION(global, fakemessenger_serialize, 0, BOOL, true),
	OPTION(global, kill_after, 0, INT, 0),
	OPTION(debug, debug, 0, INT, 0),
	OPTION(debug, debug_lockdep, 0, INT, 0),
	OPTION(debug, debug_mds, 0, INT, 1),
	OPTION(debug, debug_mds_balancer, 0, INT, 1),
	OPTION(debug, debug_mds_log, 0, INT, 1),
	OPTION(debug, debug_mds_log_expire, 0, INT, 1),
	OPTION(debug, debug_mds_migrator, 0, INT, 1),
	OPTION(debug, debug_buffer, 0, INT, 0),
	OPTION(debug, debug_timer, 0, INT, 0),
	OPTION(debug, debug_filer, 0, INT, 0),
	OPTION(debug, debug_objecter, 0, INT, 0),
	OPTION(debug, debug_journaler, 0, INT, 0),
	OPTION(debug, debug_objectcacher, 0, INT, 0),
	OPTION(debug, debug_client, 0, INT, 0),
	OPTION(debug, debug_osd, 0, INT, 0),
	OPTION(debug, debug_ebofs, 0, INT, 1),
	OPTION(debug, debug_filestore, 0, INT, 1),
	OPTION(debug, debug_journal, 0, INT, 1),
	OPTION(debug, debug_bdev, 0, INT, 1),
	OPTION(debug, debug_ns, 0, INT, 0),
	OPTION(debug, debug_ms, 0, INT, 0),
	OPTION(debug, debug_mon, 0, INT, 1),
	OPTION(debug, debug_paxos, 0, INT, 0),
	OPTION(debug, debug_tp, 0, INT, 0),
	OPTION(debug, debug_after, 0, INT, 0),
	OPTION(clock, clock_lock, 0, BOOL, false),
	OPTION(clock, clock_tare, 0, BOOL, false),
	OPTION(global, ms_tcp_nodelay, 0, BOOL, true),
	OPTION(global, ms_retry_interval, 0, DOUBLE, 2.0),
	OPTION(global, ms_fail_interval, 0, DOUBLE, 15.0),
	OPTION(global, ms_die_on_failure, 0, BOOL, false),
	OPTION(global, ms_nocrc, 0, BOOL, false),
	OPTION(mon, mon_tick_interval, 0, INT, 5),
	OPTION(mon, mon_osd_down_out_interval, 0, INT, 5),
	OPTION(mon, mon_lease, 0, FLOAT, 5),
	OPTION(mon, mon_lease_renew_interval, 0, FLOAT, 3),
	OPTION(mon, mon_lease_ack_timeout, 0, FLOAT, 10.0),
	OPTION(mon, mon_lease_timeout, 0, FLOAT, 10.0),
	OPTION(mon, mon_accept_timeout, 0, FLOAT, 10.0),
	OPTION(mon, mon_stop_on_last_unmount, 0, BOOL, false),
	OPTION(mon, mon_stop_with_last_mds, 0, BOOL, false),
	OPTION(mon, mon_allow_mds_bully, 0, BOOL, false),
	OPTION(mon, mon_pg_create_interval, 0, FLOAT, 30.0),
	OPTION(paxos, paxos_propose_interval, 0, DOUBLE, 1.0),
	OPTION(paxos, paxos_observer_timeout, 0, DOUBLE, 5*60),
	OPTION(client, client_cache_size, 0, INT, 1000),
	OPTION(client, client_cache_mid, 0, FLOAT, .5),
	OPTION(client, client_cache_stat_ttl, 0, INT, 0),
	OPTION(client, client_cache_readdir_ttl, 0, INT, 1),
	OPTION(client, client_use_random_mds, 0, BOOL, false),
	OPTION(client, client_mount_timeout, 0, DOUBLE, 10.0),
	OPTION(client, client_tick_interval, 0, DOUBLE, 1.0),
	OPTION(client, client_hack_balance_reads, 0, BOOL, false),
	OPTION(client, client_trace, 0, STR, 0),
	OPTION(client, client_readahead_min, 0, LONGLONG, 128*1024),
	OPTION(client, client_readahead_max_bytes, 0, LONGLONG, 0),
	OPTION(client, client_readahead_max_periods, 0, LONGLONG, 4),
	OPTION(client, client_snapdir, 0, STR, ".snap"),
	OPTION(global, fuse_direct_io, 0, INT, 0),
	OPTION(global, fuse_ll, 0, BOOL, true),
	OPTION(client_oc, client_oc, 0, BOOL, true),
	OPTION(client_oc, client_oc_size, 0, INT, 1024*1024* 64),
	OPTION(client_oc, client_oc_max_dirty, 0, INT, 1024*1024* 48),
	OPTION(client_oc, client_oc_target_dirty, 0, INT, 1024*1024* 8),
	OPTION(client_oc, client_oc_max_sync_write, 0, LONGLONG, 128*1024),
	OPTION(objecter, objecter_buffer_uncommitted, 0, BOOL, true),
	OPTION(objecter, objecter_map_request_interval, 0, DOUBLE, 15.0),
	OPTION(objecter, objecter_tick_interval, 0, DOUBLE, 5.0),
	OPTION(objecter, objecter_timeout, 0, DOUBLE, 10.0),
	OPTION(journaler, journaler_allow_split_entries, 0, BOOL, true),
	OPTION(journaler, journaler_safe, 0, BOOL, true),
	OPTION(journaler, journaler_write_head_interval, 0, INT, 15),
	OPTION(journaler, journaler_cache, 0, BOOL, false),
	OPTION(journaler, journaler_prefetch_periods, 0, INT, 50),
	OPTION(journaler, journaler_batch_interval, 0, DOUBLE, .001),
	OPTION(journaler, journaler_batch_max, 0, LONGLONG, 0),
	OPTION(mds, mds_cache_size, 0, INT, 300000),
	OPTION(mds, mds_cache_mid, 0, FLOAT, .7),
	OPTION(mds, mds_decay_halflife, 0, FLOAT, 5),
	OPTION(mds, mds_beacon_interval, 0, FLOAT, 4),
	OPTION(mds, mds_beacon_grace, 0, FLOAT, 15),
	OPTION(mds, mds_blacklist_interval, 0, FLOAT, 24.0*60.0),
	OPTION(mds, mds_session_timeout, 0, FLOAT, 60),
	OPTION(mds, mds_session_autoclose, 0, FLOAT, 300),
	OPTION(mds, mds_client_lease, 0, FLOAT, 120),
	OPTION(mds, mds_reconnect_timeout, 0, FLOAT, 30),
	OPTION(mds, mds_tick_interval, 0, FLOAT, 5),
	OPTION(mds, mds_scatter_nudge_interval, 0, FLOAT, 5),
	OPTION(mds, mds_client_prealloc_inos, 0, INT, 1000),
	OPTION(mds, mds_early_reply, 0, BOOL, true),
	OPTION(mds, mds_rdcap_ttl_ms, 0, INT, 60*1000),
	OPTION(mds, mds_log, 0, BOOL, true),
	OPTION(mds, mds_log_unsafe, 0, BOOL, false),
	OPTION(mds, mds_log_max_events, 0, INT, -1),
	OPTION(mds, mds_log_max_segments, 0, INT, 100),
	OPTION(mds, mds_log_max_expiring, 0, INT, 20),
	OPTION(mds, mds_log_pad_entry, 0, INT, 128),
	OPTION(mds, mds_log_eopen_size, 0, INT, 100),
	OPTION(mds, mds_bal_sample_interval, 0, FLOAT, 3.0),
	OPTION(mds, mds_bal_replicate_threshold, 0, FLOAT, 8000),
	OPTION(mds, mds_bal_unreplicate_threshold, 0, FLOAT, 0),
	OPTION(mds, mds_bal_frag, 0, BOOL, true),
	OPTION(mds, mds_bal_split_size, 0, INT, 10000),
	OPTION(mds, mds_bal_split_rd, 0, FLOAT, 25000),
	OPTION(mds, mds_bal_split_wr, 0, FLOAT, 10000),
	OPTION(mds, mds_bal_merge_size, 0, INT, 50),
	OPTION(mds, mds_bal_merge_rd, 0, FLOAT, 1000),
	OPTION(mds, mds_bal_merge_wr, 0, FLOAT, 1000),
	OPTION(mds, mds_bal_interval, 0, INT, 10),
	OPTION(mds, mds_bal_fragment_interval, 0, INT, -1),
	OPTION(mds, mds_bal_idle_threshold, 0, FLOAT, 0),
	OPTION(mds, mds_bal_max, 0, INT, -1),
	OPTION(mds, mds_bal_max_until, 0, INT, -1),
	OPTION(mds, mds_bal_mode, 0, INT, 0),
	OPTION(mds, mds_bal_min_rebalance, 0, FLOAT, .1),
	OPTION(mds, mds_bal_min_start, 0, FLOAT, .2),
	OPTION(mds, mds_bal_need_min, 0, FLOAT, .8),
	OPTION(mds, mds_bal_need_max, 0, FLOAT, 1.2),
	OPTION(mds, mds_bal_midchunk, 0, FLOAT, .3),
	OPTION(mds, mds_bal_minchunk, 0, FLOAT, .001),
	OPTION(mds, mds_trim_on_rejoin, 0, BOOL, true),
	OPTION(mds, mds_shutdown_check, 0, INT, 0),
	OPTION(mds, mds_verify_export_dirauth, 0, BOOL, true),
	OPTION(mds, mds_local_osd, 0, BOOL, false),
	OPTION(mds, mds_thrash_exports, 0, INT, 0),
	OPTION(mds, mds_thrash_fragments, 0, INT, 0),
	OPTION(mds, mds_dump_cache_on_map, 0, BOOL, false),
	OPTION(mds, mds_dump_cache_after_rejoin, 0, BOOL, true),
	OPTION(mds, mds_hack_log_expire_for_better_stats, 0, BOOL, false),
	OPTION(osd, osd_balance_reads, 0, BOOL, false),
	OPTION(osd, osd_flash_crowd_iat_threshold, 0, INT, 0),
	OPTION(osd, osd_flash_crowd_iat_alpha, 0, DOUBLE, 0.125),
	OPTION(osd, osd_balance_reads_temp, 0, DOUBLE, 100),
	OPTION(osd, osd_shed_reads, 0, INT, false),
	OPTION(osd, osd_shed_reads_min_latency, 0, DOUBLE, .01),
	OPTION(osd, osd_shed_reads_min_latency_diff, 0, DOUBLE, .01),
	OPTION(osd, osd_shed_reads_min_latency_ratio, 0, DOUBLE, 1.5),
	OPTION(osd, osd_immediate_read_from_cache, 0, BOOL, false),
	OPTION(osd, osd_exclusive_caching, 0, BOOL, true),
	OPTION(osd, osd_stat_refresh_interval, 0, DOUBLE, .5),
	OPTION(osd, osd_min_pg_size_without_alive, 0, INT, 2),
	OPTION(osd, osd_pg_bits, 0, INT, 6),
	OPTION(osd, osd_lpg_bits, 0, INT, 1),
	OPTION(osd, osd_object_layout, 0, INT, CEPH_OBJECT_LAYOUT_HASHINO),
	OPTION(osd, osd_pg_layout, 0, INT, CEPH_PG_LAYOUT_CRUSH),
	OPTION(osd, osd_min_rep, 0, INT, 2),
	OPTION(osd, osd_max_rep, 0, INT, 3),
	OPTION(osd, osd_min_raid_width, 0, INT, 3),
	OPTION(osd, osd_max_raid_width, 0, INT, 2),
	OPTION(osd, osd_maxthreads, 0, INT, 2),
	OPTION(osd, osd_max_opq, 0, INT, 10),
	OPTION(osd, osd_age, 0, FLOAT, .8),
	OPTION(osd, osd_age_time, 0, INT, 0),
	OPTION(osd, osd_heartbeat_interval, 0, INT, 1),
	OPTION(osd, osd_mon_heartbeat_interval, 0, INT, 30),
	OPTION(osd, osd_heartbeat_grace, 0, INT, 20),
	OPTION(osd, osd_mon_report_interval, 0, INT, 5),
	OPTION(osd, osd_replay_window, 0, INT, 45),
	OPTION(osd, osd_max_pull, 0, INT, 2),
	OPTION(osd, osd_preserve_trimmed_log, 0, BOOL, true),
	OPTION(osd, osd_recovery_delay_start, 0, FLOAT, 15),
	OPTION(osd, osd_recovery_max_active, 0, INT, 5),
	OPTION(osd, osd_auto_weight, 0, BOOL, false),
	OPTION(global, filestore, 0, BOOL, false),
	OPTION(global, filestore_sync_interval, 0, DOUBLE, .2),
	OPTION(global, filestore_fake_attrs, 0, BOOL, false),
	OPTION(global, filestore_fake_collections, 0, BOOL, false),
	OPTION(global, filestore_dev, 0, STR, 0),
	OPTION(global, filestore_btrfs_trans, 0, BOOL, true),
	OPTION(ebofs, ebofs, 0, BOOL, false),
	OPTION(ebofs, ebofs_cloneable, 0, BOOL, true),
	OPTION(ebofs, ebofs_verify, 0, BOOL, false),
	OPTION(ebofs, ebofs_commit_ms, 0, INT, 200),
	OPTION(ebofs, ebofs_oc_size, 0, INT, 10000),
	OPTION(ebofs, ebofs_cc_size, 0, INT, 10000),
	OPTION(ebofs, ebofs_bc_size, 0, LONGLONG, 50*256),
	OPTION(ebofs, ebofs_bc_max_dirty, 0, LONGLONG, 30*256),
	OPTION(ebofs, ebofs_max_prefetch, 0, INT, 1000),
	OPTION(ebofs, ebofs_realloc, 0, BOOL, false),
	OPTION(ebofs, ebofs_verify_csum_on_read, 0, BOOL, true),
	OPTION(journal, journal_dio, 0, BOOL, false),
	OPTION(journal, journal_max_write_bytes, 0, INT, 0),
	OPTION(journal, journal_max_write_entries, 0, INT, 100),
	OPTION(bdev, bdev_lock, 0, BOOL, true),
	OPTION(bdev, bdev_iothreads, 0, INT, 1),
	OPTION(bdev, bdev_idle_kick_after_ms, 0, INT, 100),
	OPTION(bdev, bdev_el_fw_max_ms, 0, INT, 10000),
	OPTION(bdev, bdev_el_bw_max_ms, 0, INT, 3000),
	OPTION(bdev, bdev_el_bidir, 0, BOOL, false),
	OPTION(bdev, bdev_iov_max, 0, INT, 512),
	OPTION(bdev, bdev_debug_check_io_overlap, 0, BOOL, true),
	OPTION(bdev, bdev_fake_mb, 0, INT, 0),
	OPTION(bdev, bdev_fake_max_mb, 0, INT, 0),
};

static bool set_conf_val(void *field, opt_type_t type, const char *val)
{
	switch (type) {
	case BOOL:
		if (strcasecmp(val, "false") == 0)
			*(bool *)field = false;
		else if (strcasecmp(val, "true") == 0)
			*(bool *)field = true;
		else
			*(bool *)field = (bool)atoi(val);
		break;
	case INT:
		*(int *)field = atoi(val);
		break;
	case LONGLONG:
		*(long long *)field = atoll(val);
		break;
	case STR:
		if (val)
		  *(char **)field = strdup(val);
	        else
		  *(char **)field = NULL;
		break;
	case FLOAT:
		*(float *)field = atof(val);
		break;
	case DOUBLE:
		*(double *)field = strtod(val, NULL);
		break;
	default:
		return false;
	}

	return true;
}

static void set_conf_name(config_option *opt)
{
  char *newsection = (char *)opt->section;
  char *newconf = (char *)opt->name;
  int i;

  if (opt->section[0] == 0) {
    newsection = strdup("global");
  }

  if (strncmp(newsection, opt->name, strlen(newsection)) == 0) {
    /* if key starts with the name of the section, remove name of the section
       unless key equals to it */

    if (strcmp(newsection, opt->name) == 0)
      goto done;

    newconf = strdup(&opt->name[strlen(newsection)+1]);
  } else {
    newconf = strdup(opt->name);
  }

  i = 0;
  while (newconf[i]) {
    if (newconf[i] == '_')
      newconf[i] = ' ';

    ++i;
  }

  done:
    opt->section = newsection;
    opt->conf_name = (const char *)newconf;
}

static bool init_g_conf()
{
  int len = sizeof(config_optionsp)/sizeof(config_option);
  int i;
  config_option *opt;

  for (i = 0; i<len; i++) {
    opt = &config_optionsp[i];
    if (!set_conf_val(opt->val_ptr,
		      opt->type,
		      opt->def_val)) {
      cerr << "error initializing g_conf value num " << i << std::endl;
      return false;
    }

    set_conf_name(opt);
  }

  return true;
}

static bool g_conf_initialized = init_g_conf();

static bool cmd_is_char(const char *cmd)
{
	return ((cmd[0] == '-') &&
		cmd[1] && !cmd[2]);
}

static bool cmd_equals(const char *cmd, const char *opt, char char_opt, unsigned int *val_pos)
{
	unsigned int i;
	unsigned int len = strlen(opt);

	*val_pos = 0;

	if (!*cmd)
		return false;

	if (char_opt && cmd_is_char(cmd))
		return (char_opt == cmd[1]);

	if ((cmd[0] != '-') || (cmd[1] != '-'))
		return false;

	for (i=0; i<len; i++) {
		if ((opt[i] == '_') || (opt[i] == '-')) {
			switch (cmd[i+2]) {
			case '-':
			case '_':
				continue;
			default:
				break;
			}
		}

		if (cmd[i+2] != opt[i])
			return false;
	}

	if (cmd[i+2] == '=')
		*val_pos = i+3;
	else if (cmd[i+2])
		return false;

	return true;
}

#define OPT_READ_TYPE(section, var, type, inout) \
  cf->read(section, var, (type *)inout, *(type *)inout)

void parse_config_file(ConfFile *cf, bool auto_update)
{
  int opt_len = sizeof(config_optionsp)/sizeof(config_option);

  cf->set_auto_update(true);
  cf->parse();

  for (int i=0; i<opt_len; i++) {
    config_option *opt = &config_optionsp[i];

    switch (opt->type) {
    case STR:
      OPT_READ_TYPE(opt->section, opt->conf_name, char *, opt->val_ptr);
      break;
    case BOOL:
      OPT_READ_TYPE(opt->section, opt->conf_name, bool, opt->val_ptr);
      break;
    case INT:
      OPT_READ_TYPE(opt->section, opt->conf_name, int, opt->val_ptr);
      break;
    case FLOAT:
      OPT_READ_TYPE(opt->section, opt->conf_name, float, opt->val_ptr);
      break;
    case DOUBLE:
      OPT_READ_TYPE(opt->section, opt->conf_name, double, opt->val_ptr);
      break;
    default:
      break;
    }
  }
  
}

void preparse_config_options(std::vector<const char*>& args, bool open)
{
  unsigned int val_pos;

  std::vector<const char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
#define NEXT_VAL (val_pos ? &args[i][val_pos] : args[++i])
#define SET_ARG_VAL(dest, type) \
	set_conf_val(dest, type, NEXT_VAL)
#define SET_BOOL_ARG_VAL(dest) \
	set_conf_val(dest, BOOL, (val_pos ? &args[i][val_pos] : "false"))
#define CMD_EQ(str_cmd, char_cmd) \
	cmd_equals(args[i], str_cmd, char_cmd, &val_pos)

    if (CMD_EQ("conf_file", 'c'))
	SET_ARG_VAL(&g_conf.conf_file, STR);
    else if (CMD_EQ("dump_conf", 0))
	SET_BOOL_ARG_VAL(&g_conf.dump_conf);
    else
      nargs.push_back(args[i]);
  }
  args.swap(nargs);
  nargs.clear();

  ConfFile cf(g_conf.conf_file);

  parse_config_file(&cf, true);
  if (g_conf.dump_conf)
    cf.dump();
}

void parse_config_options(std::vector<const char*>& args, bool open)
{
  int opt_len = sizeof(config_optionsp)/sizeof(config_option);
  unsigned int val_pos;

  std::vector<const char*> nargs;
  for (unsigned i=0; i<args.size(); i++) {
    bool isarg = i+1 < args.size();  // is more?

    if (CMD_EQ("bind", 0)) {
      assert_warn(parse_ip_port(args[++i], g_my_addr));
    } else if (CMD_EQ("daemonize", 'd')) {
      g_conf.daemonize = true;
      g_conf.log_to_stdout = false;
    } else if (CMD_EQ("foreground", 'f')) {
      g_conf.daemonize = false;
      g_conf.log_to_stdout = false;
    } else  {
      int optn;

      for (optn = 0; optn < opt_len; optn++) {
	if (cmd_equals(args[i], 
		  config_optionsp[optn].name,
		  config_optionsp[optn].char_option,
		  &val_pos)) {
	  if (isarg || val_pos || config_optionsp[optn].type == BOOL)
	    SET_ARG_VAL(config_optionsp[optn].val_ptr, config_optionsp[optn].type);
	  else
	    continue;
	  break;
	}
      }

      if (optn == opt_len)
        nargs.push_back(args[i]);
    }
  }

 // open log file?

  if (open)
    _dout_open_log();
  
  signal(SIGHUP, sighup_handler);

  args = nargs;
}
