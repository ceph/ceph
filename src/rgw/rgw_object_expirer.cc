// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

using namespace std;

#include "auth/Crypto.h"

#include "common/armor.h"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "global/global_init.h"

#include "include/utime.h"
#include "include/str_list.h"

#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_replica_log.h"
#include "rgw_object_expirer_core.h"

#define dout_subsys ceph_subsys_rgw

static RGWRados *store = NULL;

class StoreDestructor {
  RGWRados *store;

public:
  StoreDestructor(RGWRados *_s) : store(_s) {}
  ~StoreDestructor() {
    if (store) {
      RGWStoreManager::close_storage(store);
    }
  }
};

static void usage()
{
  generic_server_usage();
}

static inline utime_t get_last_run_time(void)
{
  return utime_t();
}

int main(const int argc, const char **argv)
{
  vector<const char *> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  for (std::vector<const char *>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return 0;
    }
  }

  if (g_conf->daemonize) {
    global_init_daemonize(g_ceph_context, 0);
  }

  common_init_finish(g_ceph_context);

  store = RGWStoreManager::get_storage(g_ceph_context, false, false);
  if (!store) {
    std::cerr << "couldn't init storage provider" << std::endl;
    return EIO;
  }

  rgw_user_init(store);
  rgw_bucket_init(store->meta_mgr);

  /* Guard to not forget about closing the rados store. */
  StoreDestructor store_dtor(store);

  utime_t last_run = get_last_run_time();
  ObjectExpirer objexp(store);
  while (true) {
    const utime_t round_start = ceph_clock_now(g_ceph_context);
    objexp.inspect_all_shards(last_run, round_start);

    last_run = round_start;

    /* End of the real work for now. Prepare for sleep. */
    const utime_t round_time = ceph_clock_now(g_ceph_context) - round_start;
    const utime_t interval(g_ceph_context->_conf->rgw_objexp_gc_interval, 0);

    if (round_time < interval) {
      /* This should be the main path of execution. All currently expired
       * objects have been removed and we need go sleep waiting for the next
       * turn. If the check isn't true, it means we have to much hints
       * in relation to interval time. */
      const utime_t sleep_period = interval - round_time;
      dout(20) << "sleeping for " << sleep_period << dendl;
      sleep_period.sleep();
    }
  }

  return EXIT_SUCCESS;
}
