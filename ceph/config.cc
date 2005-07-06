
#include "include/config.h"
#include "osd/OSDCluster.h"


//#define MDS_CACHE_SIZE        4*10000   -> <20mb
//#define MDS_CACHE_SIZE        80000         62mb

#define AVG_PER_INODE_SIZE    450
#define MDS_CACHE_MB_TO_INODES(x) ((x)*1000000/AVG_PER_INODE_SIZE)

//#define MDS_CACHE_SIZE       MDS_CACHE_MB_TO_INODES( 50 )
#define MDS_CACHE_SIZE 1500000


// hack hack hack ugly FIXME
long buffer_total_alloc = 0;
Mutex bufferlock;



OSDFileLayout g_OSD_FileLayout( 1<<20, 1, 1<<20 );   // stripe files over whole objects
//OSDFileLayout g_OSD_FileLayout( 1<<17, 4, 1<<20 );   // 128k stripes over sets of 4

// ??
OSDFileLayout g_OSD_MDDirLayout( 1<<14, 1<<2, 1<<19 );

// stripe mds log over 128 byte bits (see mds_log_pad_entry below to match!)
OSDFileLayout g_OSD_MDLogLayout( 1<<7, 32, 1<<20 );  // new (good?) way
//OSDFileLayout g_OSD_MDLogLayout( 57, 32, 1<<20 );  // pathological case to test striping buffer mapping
//OSDFileLayout g_OSD_MDLogLayout( 1<<20, 1, 1<<20 );  // old way


md_config_t g_conf = {
  num_mds: 2,
  num_osd: 5,
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
  
  // --- client ---
  client_cache_size: 400,
  client_cache_mid: .5,
  client_cache_stat_ttl: 10, // seconds until cached stat results become invalid
  client_use_random_mds:  false,
  client_bcache_alloc_minsize: 1024,
  client_bcache_alloc_maxsize: 262144,
  client_bcache_ttl: 30, // seconds until dirty buffers are written to disk
  
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

  mds_bal_replicate_threshold: 500,
  mds_bal_unreplicate_threshold: 200,
  mds_bal_interval: 30,           // seconds
  mds_bal_idle_threshold: .1,
  mds_bal_max: -1,
  mds_bal_max_until: -1,

  mds_commit_on_shutdown: true,

  mds_verify_export_dirauth: true,


  // --- osd ---
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



void parse_config_options(int argc, char **argv,
						  int& nargc, char**&nargv)
{
  // alloc new argc
  nargv = (char**)malloc(sizeof(char*) * argc);
  nargc = 0;
  nargv[nargc++] = argv[0];

  for (int i=1; i<argc; i++) {
	if (strcmp(argv[i], "--nummds") == 0) 
	  g_conf.num_mds = atoi(argv[++i]);
	else if (strcmp(argv[i], "--numclient") == 0) 
	  g_conf.num_client = atoi(argv[++i]);
	else if (strcmp(argv[i], "--numosd") == 0) 
	  g_conf.num_osd = atoi(argv[++i]);

	else if (strcmp(argv[i], "--debug") == 0) 
	  g_conf.debug = atoi(argv[++i]);
	else if (strcmp(argv[i], "--debug_mds_balancer") == 0) 
	  g_conf.debug_mds_balancer = atoi(argv[++i]);
	else if (strcmp(argv[i], "--debug_mds_log") == 0) 
	  g_conf.debug_mds_log = atoi(argv[++i]);
	else if (strcmp(argv[i], "--debug_buffer") == 0) 
	  g_conf.debug_buffer = atoi(argv[++i]);
	else if (strcmp(argv[i], "--debug_filer") == 0) 
	  g_conf.debug_filer = atoi(argv[++i]);
	else if (strcmp(argv[i], "--debug_client") == 0) 
	  g_conf.debug_client = atoi(argv[++i]);

	else if (strcmp(argv[i], "--log") == 0) 
	  g_conf.log = atoi(argv[++i]);
	else if (strcmp(argv[i], "--log_name") == 0) 
	  g_conf.log_name = argv[++i];

	else if (strcmp(argv[i], "--fakemessenger_serialize") == 0) 
	  g_conf.fakemessenger_serialize = atoi(argv[++i]);

	else if (strcmp(argv[i], "--mds_cache_size") == 0) 
	  g_conf.mds_cache_size = atoi(argv[++i]);

	else if (strcmp(argv[i], "--mds_log") == 0) 
	  g_conf.mds_log = atoi(argv[++i]);
	else if (strcmp(argv[i], "--mds_log_before_reply") == 0) 
	  g_conf.mds_log_before_reply = atoi(argv[++i]);
	else if (strcmp(argv[i], "--mds_log_max_len") == 0) 
	  g_conf.mds_log_max_len = atoi(argv[++i]);
	else if (strcmp(argv[i], "--mds_log_read_inc") == 0) 
	  g_conf.mds_log_read_inc = atoi(argv[++i]);
	else if (strcmp(argv[i], "--mds_log_max_trimming") == 0) 
	  g_conf.mds_log_max_trimming = atoi(argv[++i]);

	else if (strcmp(argv[i], "--mds_commit_on_shutdown") == 0) 
	  g_conf.mds_commit_on_shutdown = atoi(argv[++i]);
	else if (strcmp(argv[i], "--mds_log_flush_on_shutdown") == 0) 
	  g_conf.mds_log_flush_on_shutdown = atoi(argv[++i]);

	else if (strcmp(argv[i], "--mds_bal_interval") == 0) 
	  g_conf.mds_bal_interval = atoi(argv[++i]);
	else if (strcmp(argv[i], "--mds_bal_max") == 0) 
	  g_conf.mds_bal_max = atoi(argv[++i]);
	else if (strcmp(argv[i], "--mds_bal_max_until") == 0) 
	  g_conf.mds_bal_max_until = atoi(argv[++i]);

	else if (strcmp(argv[i], "--client_cache_size") == 0)
	  g_conf.client_cache_size = atoi(argv[++i]);
	else if (strcmp(argv[i], "--client_cache_stat_ttl") == 0)
	  g_conf.client_cache_stat_ttl = atoi(argv[++i]);

	else if (strcmp(argv[i], "--osd_fsync") == 0) 
	  g_conf.osd_fsync = atoi(argv[++i]);
	else if (strcmp(argv[i], "--osd_writesync") == 0) 
	  g_conf.osd_writesync = atoi(argv[++i]);
	else if (strcmp(argv[i], "--osd_maxthreads") == 0) 
	  g_conf.osd_maxthreads = atoi(argv[++i]);

	else {
	  //cout << "passing arg " << argv[i] << endl;
	  nargv[nargc++] = argv[i];
	}
  }
}
