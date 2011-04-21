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

#include "libceph.h"
#include <iostream>

using std::cout;
using std::cerr;

int main(int argc, const char **argv)
{
  ceph_mount_t *cmount;
  int ret = ceph_create(&cmount, NULL);
  if (ret) {
    cerr << "ceph_create failed with error: " << ret << std::endl;
    return 1;
  }

  ceph_conf_read_file(cmount, NULL);
  ceph_conf_parse_argv(cmount, argc, argv);

  char buf[128];
  ret = ceph_conf_get(cmount, "log file", buf, sizeof(buf));
  if (ret) {
    cerr << "ceph_conf_get(\"log file\") failed with error " << ret << std::endl;
  }
  else {
    cout << "log_file = \"" << buf << "\"" << std::endl;
  }

  ret = ceph_mount(cmount, NULL);
  if (ret) {
    cerr << "ceph_mount error: " << ret << std::endl;
    return 1;
  }
  cout << "Successfully mounted Ceph!" << std::endl;

  ceph_shutdown(cmount);

  return 0;
}
