// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "auth/AuthSupported.h"
#include "auth/KeyRing.h"
#include "config.h"
#include "include/color.h"
#include "tls.h"

void common_set_defaults(bool daemon)
{
  if (daemon) {
    cout << TEXT_YELLOW << " ** WARNING: Ceph is still under heavy development, and is only suitable for **" << TEXT_NORMAL << std::endl;
    cout << TEXT_YELLOW <<  " **          testing and review.  Do not trust it with important data.       **" << TEXT_NORMAL << std::endl;

    g_conf.daemonize = true;
    g_conf.logger = true;
  } else {
    g_conf.pid_file = 0;
  }
}

void common_init(std::vector<const char*>& args, const char *module_type, bool init_keys)
{
  tls_init();
  tls_get_val()->disable_assert = 0;

  parse_startup_config_options(args, module_type);
  parse_config_options(args);

#ifdef HAVE_LIBTCMALLOC
  if (g_conf.tcmalloc_profiler_run && g_conf.tcmalloc_have) {
    char *profile_name = new char[PATH_MAX];
    sprintf(profile_name, "%s/%s", g_conf.log_dir, g_conf.name);
    char *val = new char[sizeof(int)*8+1];
    sprintf(val, "%i", g_conf.profiler_allocation_interval);
    setenv("HEAP_PROFILE_ALLOCATION_INTERVAL", val, g_conf.profiler_allocation_interval);
    sprintf(val, "%i", g_conf.profiler_highwater_interval);
    setenv("HEAP_PROFILE_INUSE_INTERVAL", val, g_conf.profiler_highwater_interval);
    generic_dout(0) << "turning on heap profiler with prefix " << profile_name << dendl;
    g_conf.profiler_start(profile_name);
    delete profile_name;
  }
#endif //HAVE_LIBTCMALLOC

  if (init_keys && is_supported_auth(CEPH_AUTH_CEPHX)) {
    g_keyring.load(g_conf.keyring);

    if (strlen(g_conf.key)) {
      string k = g_conf.key;
      EntityAuth ea;
      ea.key.decode_base64(k);
      g_keyring.add(*g_conf.entity_name, ea);
    } else if (strlen(g_conf.keyfile)) {
      char buf[100];
      int fd = ::open(g_conf.keyfile, O_RDONLY);
      if (fd < 0) {
	cerr << "unable to open " << g_conf.keyfile << ": " << strerror(errno) << std::endl;
	exit(1);
      }
      int len = ::read(fd, buf, sizeof(buf));
      if (len < 0) {
	cerr << "unable to read key from " << g_conf.keyfile << ": " << strerror(errno) << std::endl;
	exit(1);
      }
      ::close(fd);

      buf[len] = 0;
      string k = buf;
      EntityAuth ea;
      ea.key.decode_base64(k);
      g_keyring.add(*g_conf.entity_name, ea);
    }
  }
}
