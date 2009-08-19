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

using namespace std;

int main(int argc, const char **argv)
{
  if (ceph_initialize(argc, argv) < 0) {
    cerr << "error initializing\n" << std::endl;
    return(1);
  }
  cout << "Successfully initialized Ceph!" << std::endl;

  if(ceph_mount() < 0) {
    cerr << "error mounting\n" << std::endl;
    return(1);
  }
  cout << "Successfully mounted Ceph!" << std::endl;

  ceph_deinitialize();
  cout << "Successfully deinitialized Ceph!" << std::endl;

  return 0;
}
