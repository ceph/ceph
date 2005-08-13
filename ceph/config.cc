
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



FileLayout g_OSD_FileLayout( 1<<20, 1, 1<<20 );   // stripe files over whole objects
//FileLayout g_OSD_FileLayout( 1<<17, 4, 1<<20 );   // 128k stripes over sets of 4

// ??
FileLayout g_OSD_MDDirLayout( 1<<14, 1<<2, 1<<19 );

// stripe mds log over 128 byte bits (see mds_log_pad_entry below to match!)
FileLayout g_OSD_MDLogLayout( 1<<7, 32, 1<<20 );  // new (good?) way
//FileLayout g_OSD_MDLogLayout( 57, 32, 1<<20 );  // pathological case to test striping buffer mapping
//FileLayout g_OSD_MDLogLayout( 1<<20, 1, 1<<20 );  // old way


md_config_t g_conf = {
  num_mds: 1,
  num_osd: 4,
  num_client: 1,

  // profiling and debugging
  log: true,
  log_interval: 1,
  log_name: (char*)0,

  log_messages: true,
  log_pins: true,

  fake_clock: false,
  fakemessenger_serialize: true,

  debug: 1,
  debug_mds_balancer: 1,
  debug_mds_log: 1,
  debug_buffer: 0,
  debug_filer: 0,
  debug_client: 0,
  debug_osd: 0,
  
  // --- client ---
  client_cache_size: 300,
  client_cache_mid: .5,
  client_cache_stat_ttl: 10, // seconds until cached stat results become invalid
  client_use_random_mds:  false,

  client_sync_writes: 0,

  client_bcache: 1,
  client_bcache_alloc_minsize: 1<<10, // 1KB
  client_bcache_alloc_maxsize: 1<<18, // 256KB
  client_bcache_ttl: 30, // seconds until dirty buffers are written to disk
  client_bcache_size: 2<<30, // 2GB
  //client_bcache_size: 5<<20, // 5MB
  client_bcache_lowater: 60, // % of size
  client_bcache_hiwater: 80, // % of size
  client_bcache_splice: 1<<10, // min size of spliced buffers

  client_trace: 0,
  fuse_direct_io: 0,
  
  // --- mds ---
  mds_cache_size: MDS_CACHE_SIZE,
  mds_cache_mid: .7,

  mds_log: true,
  mds_log_max_len:  MDS_CACHE_SIZE / 3,
  mds_log_max_trimming: 256,
  mds_log_read_inc: 1<<20,
  mds_log_pad_entry: 64,
  mds_log_before_reply: true,
  mds_log_flush_on_shutdown: true,

  mds_bal_replicate_threshold: 8000,
  mds_bal_unreplicate_threshold: 1000,
  mds_bal_interval: 30,           // seconds
  mds_bal_idle_threshold: .1,
  mds_bal_max: -1,
  mds_bal_max_until: -1,

  mds_commit_on_shutdown: true,

  mds_verify_export_dirauth: true,


  // --- osd ---
  osd_num_rg: 10000,
  osd_fsync: true,
  osd_writesync: false,
  osd_maxthreads: 10,
  


  // --- fakeclient (mds regression testing) ---
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
  fakeclient_op_close:    200
};


#include <stdlib.h>
#include <string.h>
#include <iostream>
using namespace std;


void argv_to_vec(int argc, char **argv,
				 vector<char*>& args)
{
  for (int i=1; i<argc; i++)
	args.push_back(argv[i]);
}

void vec_to_argv(vector<char*>& args,
				 int& argc, char **&argv)
{
  argv = (char**)malloc(sizeof(char*) * argc);
  argc = 1;
  argv[0] = "asdf";

  for (unsigned i=0; i<args.size(); i++) 
	argv[argc++] = args[i];
}

void parse_config_options(vector<char*>& args)
{
  vector<char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
	if (strcmp(args[i], "--nummds") == 0) 
	  g_conf.num_mds = atoi(args[++i]);
	else if (strcmp(args[i], "--numclient") == 0) 
	  g_conf.num_client = atoi(args[++i]);
	else if (strcmp(args[i], "--numosd") == 0) 
	  g_conf.num_osd = atoi(args[++i]);

	else if (strcmp(args[i], "--debug") == 0) 
	  g_conf.debug = atoi(args[++i]);
	else if (strcmp(args[i], "--debug_mds_balancer") == 0) 
	  g_conf.debug_mds_balancer = atoi(args[++i]);
	else if (strcmp(args[i], "--debug_mds_log") == 0) 
	  g_conf.debug_mds_log = atoi(args[++i]);
	else if (strcmp(args[i], "--debug_buffer") == 0) 
	  g_conf.debug_buffer = atoi(args[++i]);
	else if (strcmp(args[i], "--debug_filer") == 0) 
	  g_conf.debug_filer = atoi(args[++i]);
	else if (strcmp(args[i], "--debug_client") == 0) 
	  g_conf.debug_client = atoi(args[++i]);
	else if (strcmp(args[i], "--debug_osd") == 0) 
	  g_conf.debug_osd = atoi(args[++i]);

	else if (strcmp(args[i], "--log") == 0) 
	  g_conf.log = atoi(args[++i]);
	else if (strcmp(args[i], "--log_name") == 0) 
	  g_conf.log_name = args[++i];

	else if (strcmp(args[i], "--fakemessenger_serialize") == 0) 
	  g_conf.fakemessenger_serialize = atoi(args[++i]);

	else if (strcmp(args[i], "--mds_cache_size") == 0) 
	  g_conf.mds_cache_size = atoi(args[++i]);

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
	else if (strcmp(args[i], "--mds_log_flush_on_shutdown") == 0) 
	  g_conf.mds_log_flush_on_shutdown = atoi(args[++i]);

	else if (strcmp(args[i], "--mds_bal_interval") == 0) 
	  g_conf.mds_bal_interval = atoi(args[++i]);
	else if (strcmp(args[i], "--mds_bal_replicate_threshold") == 0) 
	  g_conf.mds_bal_replicate_threshold = atoi(args[++i]);
	else if (strcmp(args[i], "--mds_bal_unreplicate_threshold") == 0) 
	  g_conf.mds_bal_unreplicate_threshold = atoi(args[++i]);
	else if (strcmp(args[i], "--mds_bal_max") == 0) 
	  g_conf.mds_bal_max = atoi(args[++i]);
	else if (strcmp(args[i], "--mds_bal_max_until") == 0) 
	  g_conf.mds_bal_max_until = atoi(args[++i]);

	else if (strcmp(args[i], "--client_cache_size") == 0)
	  g_conf.client_cache_size = atoi(args[++i]);
	else if (strcmp(args[i], "--client_cache_stat_ttl") == 0)
	  g_conf.client_cache_stat_ttl = atoi(args[++i]);
	else if (strcmp(args[i], "--client_trace") == 0)
	  g_conf.client_trace = atoi(args[++i]);
	else if (strcmp(args[i], "--fuse_direct_io") == 0)
	  g_conf.fuse_direct_io = atoi(args[++i]);

	else if (strcmp(args[i], "--client_sync_writes") == 0)
	  g_conf.client_sync_writes = atoi(args[++i]);
	else if (strcmp(args[i], "--client_bcache") == 0)
	  g_conf.client_bcache = atoi(args[++i]);
	else if (strcmp(args[i], "--client_bcache_ttl") == 0)
	  g_conf.client_bcache_ttl = atoi(args[++i]);


	else if (strcmp(args[i], "--osd_num_rg") == 0) 
	  g_conf.osd_num_rg = atoi(args[++i]);
	else if (strcmp(args[i], "--osd_fsync") == 0) 
	  g_conf.osd_fsync = atoi(args[++i]);
	else if (strcmp(args[i], "--osd_writesync") == 0) 
	  g_conf.osd_writesync = atoi(args[++i]);
	else if (strcmp(args[i], "--osd_maxthreads") == 0) 
	  g_conf.osd_maxthreads = atoi(args[++i]);

	else {
	  nargs.push_back(args[i]);
	}
  }

  args = nargs;
}
