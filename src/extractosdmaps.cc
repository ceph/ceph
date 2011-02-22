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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"

#include "mon/MonMap.h"

#include "ebofs/Ebofs.h"

#include "osd/OSD.h"
#include "mon/MonitorStore.h"

int main(int argc, char **argv)
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);

  Ebofs eb("dev/osd0");
  eb.mount();
  MonitorStore ms("mondata/mon0");
  ms.mount();
  
  epoch_t e = 1;
  while (1) {
    bufferlist bl;
    object_t oid = OSD::get_osdmap_object_name(e);
    eb.read(oid, 0, 0, bl);
    if (bl.length() == 0) break;
    cout << "saving epoch " << e << std::endl;

    bufferlist ibl;
    oid = OSD::get_inc_osdmap_object_name(e);
    eb.read(oid, 0, 0, ibl);

    ms.put_bl_sn(ibl, "osdmap", e);
    ms.put_bl_sn(bl, "osdmap_full", e);
    e++;
  }

  eb.umount();
  //ms.umount();
  
  return 0;
}
