// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * create rgw admin user
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/compat.h"
#include <sys/types.h>
#include <string.h>
#include <chrono>
#include <errno.h>
#include <thread>
#include <string>
#include <mutex>

#include "include/types.h"
#include "include/rgw/librgw_admin_user.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"

#include "rgw_admin_user.h"
#include "rgw_rados.h"
#include "rgw_os_lib.h"
#include "rgw_auth.h"
#include "rgw_auth_s3.h"

#define dout_subsys ceph_subsys_rgw

bool global_stop = false;

static void handle_sigterm(int signum)
{
  dout(20) << __func__ << " SIGUSR1 ignored" << dendl;
}

namespace rgw {

  using std::string;

  static std::mutex librgw_admin_user_mtx;

  RGWLibAdmin rgw_lib_admin;

  class C_InitTimeout : public Context {
  public:
    C_InitTimeout() {}
    void finish(int r) override {
      derr << "Initialization timeout, failed to initialize" << dendl;
      exit(1);
    }
  };

  int RGWLibAdmin::init()
  {
    vector<const char*> args;
    return init(args);
  }

  int RGWLibAdmin::init(vector<const char*>& args)
  {
    /* alternative default for module */
    map<string,string> defaults = {
      { "debug_rgw", "1/5" },
      { "keyring", "$rgw_data/keyring" },
      { "log_file", "/var/log/radosgw/$cluster-$name.log" }
    };

    cct = global_init(&defaults, args,
		      CEPH_ENTITY_TYPE_CLIENT,
		      CODE_ENVIRONMENT_UTILITY, 0);

    Mutex mutex("main");
    SafeTimer init_timer(g_ceph_context, mutex);
    init_timer.init();
    mutex.Lock();
    init_timer.add_event_after(g_conf()->rgw_init_timeout, new C_InitTimeout);
    mutex.Unlock();

    common_init_finish(g_ceph_context);

    store = RGWStoreManager::get_storage(g_ceph_context, false, false, false, false, false);

    if (!store) {
      mutex.Lock();
      init_timer.cancel_all_events();
      init_timer.shutdown();
      mutex.Unlock();

      derr << "Couldn't init storage provider (RADOS)" << dendl;
      return -EIO;
    }

    mutex.Lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.Unlock();

    rgw_user_init(store);

    init_async_signal_handler();
    register_async_signal_handler(SIGUSR1, handle_sigterm);

    return 0;
  } /* RGWLibAdmin::init() */

  int RGWLibAdmin::stop()
  {
    derr << "shutting down" << dendl;

    unregister_async_signal_handler(SIGUSR1, handle_sigterm);
    shutdown_async_signal_handler();

    RGWStoreManager::close_storage(store);

    dout(1) << "final shutdown" << dendl;
    cct.reset();

    return 0;
  } /* RGWLibAdmin::stop() */

} /* namespace rgw */

extern "C" {

int librgw_admin_user_create(librgw_admin_user_t* rgw_admin_user, int argc, char **argv)
{
  using namespace rgw;

  int rc = -EINVAL;

  if (! g_ceph_context) {
    std::lock_guard<std::mutex> lg(librgw_admin_user_mtx);
    if (! g_ceph_context) {
      vector<const char*> args;
      std::vector<std::string> spl_args;
      // last non-0 argument will be split and consumed
      if (argc > 1) {
	const std::string spl_arg{argv[(--argc)]};
	get_str_vec(spl_arg, " \t", spl_args);
      }
      argv_to_vec(argc, const_cast<const char**>(argv), args);
      // append split args, if any
      for (const auto& elt : spl_args) {
	args.push_back(elt.c_str());
      }
      rc = rgw_lib_admin.init(args);
    }
  }

  *rgw_admin_user = g_ceph_context->get();

  return rc;
}

void librgw_admin_user_shutdown(librgw_admin_user_t rgw_admin_user)
{
  using namespace rgw;

  CephContext* cct = static_cast<CephContext*>(rgw_admin_user);
  rgw_lib_admin.stop();
  cct->put();
}

} /* extern "C" */


