// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>


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
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_object_expirer_core.h"

#define dout_subsys ceph_subsys_rgw

static rgw::sal::RGWRadosStore *store = NULL;

class StoreDestructor {
  rgw::sal::RGWRadosStore *store;

public:
  explicit StoreDestructor(rgw::sal::RGWRadosStore *_s) : store(_s) {}
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

int main(const int argc, const char **argv)
{
  vector<const char *> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_DAEMON,
			 CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS, "rgw_data");

  for (std::vector<const char *>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    }
  }

  if (g_conf()->daemonize) {
    global_init_daemonize(g_ceph_context);
  }

  common_init_finish(g_ceph_context);

  store = RGWStoreManager::get_storage(g_ceph_context, false, false, false, false, false);
  if (!store) {
    std::cerr << "couldn't init storage provider" << std::endl;
    return EIO;
  }

  /* Guard to not forget about closing the rados store. */
  StoreDestructor store_dtor(store);

  RGWObjectExpirer objexp(store);
  objexp.start_processor();

  const utime_t interval(g_ceph_context->_conf->rgw_objexp_gc_interval, 0);
  while (true) {
    interval.sleep();
  }

  /* unreachable */

  return EXIT_SUCCESS;
}
