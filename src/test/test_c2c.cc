#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/config.h"
#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/mempool.h"

#include <iostream>
#include <string>

using std::cerr;
using std::string;

static void usage(void)
{
  cerr << "--threads       number of threads (default 1)" << std::endl;
  cerr << "--sharding      activate sharding optimization" << std::endl;
}


mempool::shard_t shards[mempool::num_shards] = {0};

void sigterm_handler(int signum)
{
  size_t total = 0;
  for (auto& shard : shards) {
    total += shard.bytes;
  }
  std::cout << total << std::endl;
  exit(0);
}

int main(int argc, const char **argv)
{
  int ret = 0;
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  int threads = 1;
  bool sharding = false;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    }
    else if (ceph_argparse_witharg(args, i, &threads, cerr, "--threads", "-t", (char*)NULL)) {
    }
    else if (ceph_argparse_flag(args, i, "--sharding", "-s", (char*)NULL)) {
      sharding = true;
    }
    else {
      cerr << "unknown command line option: " << *i << std::endl;
      cerr << std::endl;
      usage();
      return 2;
    }
  }

  init_async_signal_handler();
  register_async_signal_handler(SIGTERM, sigterm_handler);


  std::vector<std::thread> workers;
  for (int i = 0; i < threads; i++) {
    workers.push_back(
      std::thread([&](){
	  while(1) {
	    size_t i;
	    if (sharding) {
	      i = mempool::pick_a_shard_int();
	    } else {
	      i = 0;
	    }
	    shards[i].bytes++;
	  }
	}));
  }

  for (auto& t:workers) {
    t.join();
  }
  workers.clear();

  return ret;
}
