// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/types.h>
#include <string.h>
#include <chrono>

#include "include/rados/librgw.h"

#include "include/str_list.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/dout.h"

#include "rgw_lib.h"

#include <errno.h>
#include <thread>
#include <string>
#include <mutex>

#define dout_subsys ceph_subsys_rgw

namespace rgw {

bool global_stop = false;

static std::mutex librgw_mtx;
static RGWLib rgwlib;

} // namespace rgw

extern "C" {

int librgw_create(librgw_t* rgw, int argc, char **argv)
{
  using namespace rgw;

  int rc = -EINVAL;

  g_rgwlib = &rgwlib;

  if (! g_ceph_context) {
    std::lock_guard<std::mutex> lg(librgw_mtx);
    if (! g_ceph_context) {
      std::vector<std::string> spl_args;
      // last non-0 argument will be split and consumed
      if (argc > 1) {
	const std::string spl_arg{argv[(--argc)]};
	get_str_vec(spl_arg, " \t", spl_args);
      }
      auto args = argv_to_vec(argc, argv);
      // append split args, if any
      for (const auto& elt : spl_args) {
	args.push_back(elt.c_str());
      }
      rc = rgwlib.init(args);
    }
  }

  *rgw = g_ceph_context->get();

  return rc;
}

void librgw_shutdown(librgw_t rgw)
{
  using namespace rgw;

  CephContext* cct = static_cast<CephContext*>(rgw);
  rgwlib.stop();

  dout(1) << "final shutdown" << dendl;

  cct->put();
}

} /* extern "C" */
