
#include "include/config.h"

//#define MDS_CACHE_SIZE        4*10000   -> <20mb
//#define MDS_CACHE_SIZE        80000         62mb

#define AVG_PER_INODE_SIZE    450
#define MDS_CACHE_MB_TO_INODES(x) ((x)*1000000/AVG_PER_INODE_SIZE)

#define MDS_CACHE_SIZE       MDS_CACHE_MB_TO_INODES( 100 )
//#define MDS_CACHE_SIZE 25000  // 


md_config_t g_conf = {
  num_mds: 2,
  num_osd: 5,
  num_client: 1,

  // profiling and debugging
  log_interval: 1,
  log_name: (char*)0,

  log_messages: true,
  log_pins: true,

  fake_clock: false,
  fakemessenger_serialize: true,

  debug: 5,
  debug_mds_balancer: 1,
  debug_mds_log: 1,
  
  // --- client ---
  client_cache_size: 400,
  client_cache_mid: .5,
  client_cache_stat_ttl: 10, // seconds until cached stat results become invalid
  client_use_random_mds:  false,
  
  // --- mds ---
  mds_cache_size: MDS_CACHE_SIZE,
  mds_cache_mid: .7,

  mds_log: true,
  mds_log_max_len:  10000,//MDS_CACHE_SIZE / 3,
  mds_log_max_trimming: 32,
  mds_log_read_inc: 65536,
  mds_log_before_reply: true,
  mds_log_flush_on_shutdown: true,

  mds_bal_replicate_threshold: 500,
  mds_bal_unreplicate_threshold: 200,
  mds_bal_interval: 60,           // seconds

  mds_commit_on_shutdown: true,

  mds_verify_export_dirauth: true,


  // --- osd ---
  osd_fsync: true,



  // --- fakeclient (mds regression testing) ---
  num_fakeclient: 100,
  fakeclient_requests: 100,
  fakeclient_deterministic: false,

  fakeclient_op_statfs:     false,

  fakeclient_op_stat:     10,
  fakeclient_op_lstat:      false,
  fakeclient_op_utime:    10,   // untested
  fakeclient_op_chmod:    10,
  fakeclient_op_chown:    10,   // untested

  fakeclient_op_readdir:  10,
  fakeclient_op_mknod:    10,
  fakeclient_op_link:     false,
  fakeclient_op_unlink:   10,
  fakeclient_op_rename:   100,

  fakeclient_op_mkdir:    50,
  fakeclient_op_rmdir:    0,  // there's a bug...10,
  fakeclient_op_symlink:  10,

  fakeclient_op_openrd:   100,
  fakeclient_op_openwr:   100,
  fakeclient_op_openwrc:  100,
  fakeclient_op_read:       false,  // osd!
  fakeclient_op_write:      false,  // osd!
  fakeclient_op_truncate:   false,
  fakeclient_op_fsync:      false,
  fakeclient_op_close:    20
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
	else if (strcmp(argv[i], "--log") == 0) 
	  g_conf.log_name = argv[++i];

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


	else if (strcmp(argv[i], "--osd_fsync") == 0) 
	  g_conf.osd_fsync = atoi(argv[++i]);

	else {
	  //cout << "passing arg " << argv[i] << endl;
	  nargv[nargc++] = argv[i];
	}
  }
}
