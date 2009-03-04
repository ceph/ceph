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

md_config_t g_conf;

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

bool parse_ip_port(const char *s, entity_addr_t& a, const char **end)
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
      cerr << "should be period at " << off << std::endl;
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
  if (end)
    *end = s;
  
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
  parse_config_options(nargs);
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

#define OPTION_ALT(section, conf_name, name, schar, type, def_val) \
       { STRINGIFY(section), NULL, STRINGIFY(conf_name), \
         &g_conf.name, STRINGIFY(def_val), type, schar }

static struct config_option config_optionsp[] = {
	OPTION(global, num_mon, 0, INT, 1),
	OPTION(global, num_mds, 0, INT, 1),
	OPTION(global, num_osd, 0, INT, 4),
	OPTION(global, num_client, 0, INT, 1),
	OPTION(mon, monmap_file, 'M', STR, 0),
	OPTION(mon, mon_host, 'm', STR, 0),
	OPTION(global, daemonize, 'd', BOOL, false),
	OPTION(global, logger, 0, BOOL, true),
	OPTION(global, logger_interval, 0, INT, 1),
	OPTION(global, logger_calc_variance, 0, BOOL, true),
	OPTION(global, logger_subdir, 0, STR, 0),
	OPTION(global, logger_dir, 0, STR, INSTALL_PREFIX "/var/log/ceph/stat"),
	OPTION(global, log_dir, 0, STR, INSTALL_PREFIX "/var/log/ceph"),		// if daemonize == true
	OPTION(global, log_sym_dir, 0, STR, INSTALL_PREFIX "/var/log/ceph"),		// if daemonize == true
	OPTION(global, log_to_stdout, 0, BOOL, true),
	OPTION(global, pid_file, 'p', STR, 0),
	OPTION(global, conf_file, 'c', STR, INSTALL_PREFIX "/etc/ceph/ceph.conf"),
	OPTION(global, cluster_conf_file, 'C', STR, INSTALL_PREFIX "/etc/ceph/cluster.conf"),
	OPTION(global, dump_conf, 0, BOOL, false),
	OPTION(global, chdir_root, 0, BOOL, true),	// chdir("/") after daemonizing. if true, we generate absolute paths as needed.
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
	OPTION(debug, debug_bdev, 0, INT, 1),         // block device
	OPTION(debug, debug_ns, 0, INT, 0),
	OPTION(debug, debug_ms, 0, INT, 0),
	OPTION(debug, debug_mon, 0, INT, 1),
	OPTION(debug, debug_paxos, 0, INT, 0),
	OPTION(debug, debug_tp, 0, INT, 0),
	OPTION(clock, clock_lock, 0, BOOL, false),
	OPTION(clock, clock_tare, 0, BOOL, false),
	OPTION_ALT(messenger, tcp_nodelay, ms_tcp_nodelay, 0, BOOL, true),
	OPTION_ALT(messenger, retry_interval, ms_retry_interval, 0, DOUBLE, 2.0),  // how often to attempt reconnect
	OPTION_ALT(messenger, fail_interval, ms_fail_interval, 0, DOUBLE, 15.0),  // fail after this long
	OPTION_ALT(messenger, die_on_failure, ms_die_on_failure, 0, BOOL, false),
	OPTION_ALT(messenger, no_crc, ms_nocrc, 0, BOOL, false),
	OPTION(mon, mon_tick_interval, 0, INT, 5),
	OPTION(mon, mon_osd_down_out_interval, 0, INT, 5),  // seconds
	OPTION(mon, mon_lease, 0, FLOAT, 5),  		    // lease interval
	OPTION(mon, mon_lease_renew_interval, 0, FLOAT, 3), // on leader, to renew the lease
	OPTION(mon, mon_lease_ack_timeout, 0, FLOAT, 10.0), // on leader, if lease isn't acked by all peons
	OPTION(mon, mon_lease_timeout, 0, FLOAT, 10.0),     // on peon, if lease isn't extended
	OPTION(mon, mon_accept_timeout, 0, FLOAT, 10.0),    // on leader, if paxos update isn't accepted
	OPTION(mon, mon_stop_on_last_unmount, 0, BOOL, false),
	OPTION(mon, mon_stop_with_last_mds, 0, BOOL, false),
	OPTION(mon, mon_allow_mds_bully, 0, BOOL, false),   // allow a booting mds to (forcibly) claim an mds # .. FIXME
	OPTION(mon, mon_pg_create_interval, 0, FLOAT, 30.0), // no more than every 30s
	OPTION(paxos, paxos_propose_interval, 0, DOUBLE, 1.0),  // gather updates for this long before proposing a map update
	OPTION(paxos, paxos_observer_timeout, 0, DOUBLE, 5*60), // gather updates for this long before proposing a map update
	OPTION(client, client_cache_size, 0, INT, 1000),
	OPTION(client, client_cache_mid, 0, FLOAT, .5),
	OPTION(client, client_cache_stat_ttl, 0, INT, 0), // seconds until cached stat results become invalid
	OPTION(client, client_cache_readdir_ttl, 0, INT, 1),  // 1 second only
	OPTION(client, client_use_random_mds, 0, BOOL, false),
	OPTION(client, client_mount_timeout, 0, DOUBLE, 10.0),  // retry every N seconds
	OPTION(client, client_tick_interval, 0, DOUBLE, 1.0),
	OPTION(client, client_hack_balance_reads, 0, BOOL, false),
	OPTION(client, client_trace, 0, STR, 0),
	OPTION(client, client_readahead_min, 0, LONGLONG, 128*1024),  // readahead at _least_ this much.
	OPTION(client, client_readahead_max_bytes, 0, LONGLONG, 0),  //8 * 1024*1024,
	OPTION(client, client_readahead_max_periods, 0, LONGLONG, 4),  // as multiple of file layout period (object size * num stripes)
	OPTION(client, client_snapdir, 0, STR, ".snap"),
	OPTION(fuse, fuse_direct_io, 0, INT, 0),
	OPTION(fuse, fuse_ll, 0, BOOL, true),
	OPTION(client_oc, client_oc, 0, BOOL, true),
	OPTION(client_oc, client_oc_size, 0, INT, 1024*1024* 64),    // MB * n
	OPTION(client_oc, client_oc_max_dirty, 0, INT, 1024*1024* 48),    // MB * n  (dirty OR tx.. bigish)
	OPTION(client_oc, client_oc_target_dirty, 0, INT, 1024*1024* 8), // target dirty (keep this smallish)
	// note: the max amount of "in flight" dirty data is roughly (max - target)
	OPTION(client_oc, client_oc_max_sync_write, 0, LONGLONG, 128*1024),   // sync writes >= this use wrlock
	OPTION(objecter, objecter_buffer_uncommitted, 0, BOOL, true),  // this must be true for proper failure handling
	OPTION(objecter, objecter_map_request_interval, 0, DOUBLE, 15.0), // request a new map every N seconds, if we have pending io
	OPTION(objecter, objecter_tick_interval, 0, DOUBLE, 5.0),
	OPTION(objecter, objecter_timeout, 0, DOUBLE, 10.0),    // before we ask for a map
	OPTION(journaler, journaler_allow_split_entries, 0, BOOL, true),
	OPTION(journaler, journaler_safe, 0, BOOL, true),  // wait for COMMIT on journal writes
	OPTION(journaler, journaler_write_head_interval, 0, INT, 15),
	OPTION(journaler, journaler_cache, 0, BOOL, false), // cache writes for later readback
	OPTION(journaler, journaler_prefetch_periods, 0, INT, 50),   // * journal object size (1~MB? see above)
	OPTION(journaler, journaler_batch_interval, 0, DOUBLE, .001),   // seconds.. max add'l latency we artificially incur
	OPTION(journaler, journaler_batch_max, 0, LONGLONG, 0),  // max bytes we'll delay flushing; disable, for now....
	OPTION(mds, mds_cache_size, 0, INT, 300000),
	OPTION(mds, mds_cache_mid, 0, FLOAT, .7),
	OPTION(mds, mds_decay_halflife, 0, FLOAT, 5),
	OPTION(mds, mds_beacon_interval, 0, FLOAT, 4),
	OPTION(mds, mds_beacon_grace, 0, FLOAT, 15),
	OPTION(mds, mds_blacklist_interval, 0, FLOAT, 24.0*60.0),  // how long to blacklist failed nodes
	OPTION(mds, mds_session_timeout, 0, FLOAT, 60),    // cap bits and leases time out if client idle
	OPTION(mds, mds_session_autoclose, 0, FLOAT, 300), // autoclose idle session 
	OPTION(mds, mds_client_lease, 0, FLOAT, 120),      // (assuming session stays alive)
	OPTION(mds, mds_reconnect_timeout, 0, FLOAT, 30),  // seconds to wait for clients during mds restart
							   //  make it (mds_session_timeout - mds_beacon_grace)
	OPTION(mds, mds_tick_interval, 0, FLOAT, 5),
	OPTION(mds, mds_scatter_nudge_interval, 0, FLOAT, 5),  // how quickly dirstat changes propagate up the hierarchy
	OPTION(mds, mds_client_prealloc_inos, 0, INT, 1000),
	OPTION(mds, mds_early_reply, 0, BOOL, true),
	OPTION(mds, mds_rdcap_ttl_ms, 0, INT, 60*1000),
	OPTION(mds, mds_log, 0, BOOL, true),
	OPTION(mds, mds_log_unsafe, 0, BOOL, false),      // only wait for log sync, when it's mostly safe to do so
	OPTION(mds, mds_log_max_events, 0, INT, -1),
	OPTION(mds, mds_log_max_segments, 0, INT, 100),  // segment size defined by FileLayout, above
	OPTION(mds, mds_log_max_expiring, 0, INT, 20),
	OPTION(mds, mds_log_pad_entry, 0, INT, 128),
	OPTION(mds, mds_log_eopen_size, 0, INT, 100),   // # open inodes per log entry
	OPTION(mds, mds_bal_sample_interval, 0, FLOAT, 3.0),  // every 5 seconds
	OPTION(mds, mds_bal_replicate_threshold, 0, FLOAT, 8000),
	OPTION(mds, mds_bal_unreplicate_threshold, 0, FLOAT, 0),
	OPTION(mds, mds_bal_frag, 0, BOOL, true),
	OPTION(mds, mds_bal_split_size, 0, INT, 10000),
	OPTION(mds, mds_bal_split_rd, 0, FLOAT, 25000),
	OPTION(mds, mds_bal_split_wr, 0, FLOAT, 10000),
	OPTION(mds, mds_bal_merge_size, 0, INT, 50),
	OPTION(mds, mds_bal_merge_rd, 0, FLOAT, 1000),
	OPTION(mds, mds_bal_merge_wr, 0, FLOAT, 1000),
	OPTION(mds, mds_bal_interval, 0, INT, 10),           // seconds
	OPTION(mds, mds_bal_fragment_interval, 0, INT, -1),      // seconds
	OPTION(mds, mds_bal_idle_threshold, 0, FLOAT, 0),
	OPTION(mds, mds_bal_max, 0, INT, -1),
	OPTION(mds, mds_bal_max_until, 0, INT, -1),
	OPTION(mds, mds_bal_mode, 0, INT, 0),
	OPTION(mds, mds_bal_min_rebalance, 0, FLOAT, .1),  // must be this much above average before we export anything
	OPTION(mds, mds_bal_min_start, 0, FLOAT, .2),      // if we need less than this, we don't do anything
	OPTION(mds, mds_bal_need_min, 0, FLOAT, .8),       // take within this range of what we need
	OPTION(mds, mds_bal_need_max, 0, FLOAT, 1.2),
	OPTION(mds, mds_bal_midchunk, 0, FLOAT, .3),       // any sub bigger than this taken in full
	OPTION(mds, mds_bal_minchunk, 0, FLOAT, .001),     // never take anything smaller than this
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
	OPTION(osd, osd_balance_reads_temp, 0, DOUBLE, 100),  // send from client to replica
	OPTION(osd, osd_shed_reads, 0, INT, false),     // forward from primary to replica
	OPTION(osd, osd_shed_reads_min_latency, 0, DOUBLE, .01),       // min local latency
	OPTION(osd, osd_shed_reads_min_latency_diff, 0, DOUBLE, .01),  // min latency difference
	OPTION(osd, osd_shed_reads_min_latency_ratio, 0, DOUBLE, 1.5),  // 1.2 == 20% higher than peer
	OPTION(osd, osd_immediate_read_from_cache, 0, BOOL, false), // osds to read from the cache immediately?
	OPTION(osd, osd_exclusive_caching, 0, BOOL, true),         // replicas evict replicated writes
	OPTION(osd, osd_stat_refresh_interval, 0, DOUBLE, .5),
	OPTION(osd, osd_min_pg_size_without_alive, 0, INT, 2),  // smallest pg we allow to activate without telling the monitor
	OPTION(osd, osd_pg_bits, 0, INT, 6),  // bits per osd
	OPTION(osd, osd_lpg_bits, 0, INT, 1),  // bits per osd
	OPTION(osd, osd_object_layout, 0, INT, CEPH_OBJECT_LAYOUT_HASHINO),
	OPTION(osd, osd_pg_layout, 0, INT, CEPH_PG_LAYOUT_CRUSH),
	OPTION(osd, osd_min_rep, 0, INT, 2),
	OPTION(osd, osd_max_rep, 0, INT, 3),
	OPTION(osd, osd_min_raid_width, 0, INT, 3),
	OPTION(osd, osd_max_raid_width, 0, INT, 2),
	OPTION(osd, osd_maxthreads, 0, INT, 2),    // 0 == no threading
	OPTION(osd, osd_max_opq, 0, INT, 10),
	OPTION(osd, osd_age, 0, FLOAT, .8),
	OPTION(osd, osd_age_time, 0, INT, 0),
	OPTION(osd, osd_heartbeat_interval, 0, INT, 1),
	OPTION(osd, osd_mon_heartbeat_interval, 0, INT, 30),  // if no peers, ping monitor
	OPTION(osd, osd_heartbeat_grace, 0, INT, 20),
	OPTION(osd, osd_mon_report_interval, 0, INT, 5),  // pg stats, failures, up_thru, boot.
	OPTION(osd, osd_replay_window, 0, INT, 45),
	OPTION(osd, osd_max_pull, 0, INT, 2),
	OPTION(osd, osd_preserve_trimmed_log, 0, BOOL, true),
	OPTION(osd, osd_recovery_delay_start, 0, FLOAT, 15),
	OPTION(osd, osd_recovery_max_active, 0, INT, 5),
	OPTION(osd, osd_auto_weight, 0, BOOL, false),
	OPTION(filestore, filestore, 0, BOOL, false),
	OPTION(filestore, filestore_sync_interval, 0, DOUBLE, .2),    // seconds
	OPTION(filestore, filestore_fake_attrs, 0, BOOL, false),
	OPTION(filestore, filestore_fake_collections, 0, BOOL, false),
	OPTION(filestore, filestore_dev, 0, STR, 0),
	OPTION(filestore, filestore_btrfs_trans, 0, BOOL, true),
	OPTION(ebofs, ebofs, 0, BOOL, false),
	OPTION(ebofs, ebofs_cloneable, 0, BOOL, true),
	OPTION(ebofs, ebofs_verify, 0, BOOL, false),
	OPTION(ebofs, ebofs_commit_ms, 0, INT, 200),       // 0 = no forced commit timeout (for debugging/tracing)
	OPTION(ebofs, ebofs_oc_size, 0, INT, 10000),      // onode cache
	OPTION(ebofs, ebofs_cc_size, 0, INT, 10000),      // cnode cache
	OPTION(ebofs, ebofs_bc_size, 0, LONGLONG, 50*256), // 4k blocks, *256 for MB
	OPTION(ebofs, ebofs_bc_max_dirty, 0, LONGLONG, 30*256), // before write() will block
	OPTION(ebofs, ebofs_max_prefetch, 0, INT, 1000), // 4k blocks
	OPTION(ebofs, ebofs_realloc, 0, BOOL, false),    // hrm, this can cause bad fragmentation, don't use!
	OPTION(ebofs, ebofs_verify_csum_on_read, 0, BOOL, true),
	OPTION(journal, journal_dio, 0, BOOL, false),
	OPTION(journal, journal_max_write_bytes, 0, INT, 0),
	OPTION(journal, journal_max_write_entries, 0, INT, 100),
	OPTION(bdev, bdev_lock, 0, BOOL, true),
	OPTION(bdev, bdev_iothreads, 0, INT, 1),         // number of ios to queue with kernel
	OPTION(bdev, bdev_idle_kick_after_ms, 0, INT, 100),  // ms
	OPTION(bdev, bdev_el_fw_max_ms, 0, INT, 10000),      // restart elevator at least once every 1000 ms
	OPTION(bdev, bdev_el_bw_max_ms, 0, INT, 3000),       // restart elevator at least once every 300 ms
	OPTION(bdev, bdev_el_bidir, 0, BOOL, false),          // bidirectional elevator?
	OPTION(bdev, bdev_iov_max, 0, INT, 512),            // max # iov's to collect into a single readv()/writev() call
	OPTION(bdev, bdev_debug_check_io_overlap, 0, BOOL, true),  // [DEBUG] check for any pending io overlaps
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

void parse_startup_config_options(std::vector<const char*>& args)
{
  unsigned int val_pos;

  std::vector<const char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
    bool isarg = i+1 < args.size();  // is more?
#define NEXT_VAL (val_pos ? &args[i][val_pos] : args[++i])
#define SET_ARG_VAL(dest, type) \
	set_conf_val(dest, type, NEXT_VAL)
#define SAFE_SET_ARG_VAL(dest, type) \
	do { \
          if (isarg || val_pos) \
		SET_ARG_VAL(dest, type); \
	} while (0)
#define SET_BOOL_ARG_VAL(dest) \
	set_conf_val(dest, BOOL, (val_pos ? &args[i][val_pos] : "true"))
#define CMD_EQ(str_cmd, char_cmd) \
	cmd_equals(args[i], str_cmd, char_cmd, &val_pos)

    if (CMD_EQ("conf_file", 'c')) {
	SAFE_SET_ARG_VAL(&g_conf.conf_file, STR);
    } else if (CMD_EQ("cluster_conf_file", 'C')) {
	SAFE_SET_ARG_VAL(&g_conf.cluster_conf_file, STR);
    } else if (CMD_EQ("monmap_file", 'M')) {
	SAFE_SET_ARG_VAL(&g_conf.monmap_file, STR);
    } else if (CMD_EQ("dump_conf", 0)) {
	SET_BOOL_ARG_VAL(&g_conf.dump_conf);
    } else if (CMD_EQ("bind", 0)) {
      assert_warn(parse_ip_port(args[++i], g_my_addr));
    } else if (CMD_EQ("daemonize", 'd')) {
      g_conf.daemonize = true;
      g_conf.log_to_stdout = false;
    } else if (CMD_EQ("foreground", 'f')) {
      g_conf.daemonize = false;
      g_conf.log_to_stdout = false;
    } else {
      nargs.push_back(args[i]);
    }
  }
  args.swap(nargs);
  nargs.clear();

  ConfFile cf(g_conf.conf_file);

  parse_config_file(&cf, true);
  if (g_conf.dump_conf)
    cf.dump();
}

void parse_config_options(std::vector<const char*>& args)
{
  int opt_len = sizeof(config_optionsp)/sizeof(config_option);
  unsigned int val_pos;

  std::vector<const char*> nargs;
  for (unsigned i=0; i<args.size(); i++) {
    bool isarg = i+1 < args.size();  // is more?
    int optn;

    for (optn = 0; optn < opt_len; optn++) {
      if (CMD_EQ("lockdep", '\0')) {
	SAFE_SET_ARG_VAL(&g_lockdep, INT);
      } else if (cmd_equals(args[i],
	    config_optionsp[optn].name,
	    config_optionsp[optn].char_option,
	    &val_pos)) {
        if (isarg || val_pos || config_optionsp[optn].type == BOOL)
	    SET_ARG_VAL(config_optionsp[optn].val_ptr, config_optionsp[optn].type);
        else
          continue;
      } else {
        continue;
      }
      break;
    }

    if (optn == opt_len)
        nargs.push_back(args[i]);
  }

  signal(SIGHUP, sighup_handler);

  args = nargs;
}
