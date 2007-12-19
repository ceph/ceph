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

#include "config.h"

#include "mon/MonMap.h"





int main(int argc, char **argv)
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  
  MonMap monmap;

  char *outfn = ".ceph_monmap";

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--out") == 0) 
      outfn = args[++i];
    else {
      // parse ip:port
      entity_inst_t inst;
      if (!parse_ip_port(args[i], inst.addr)) {
	cerr << "mkmonmap: invalid ip:port '" << args[i] << "'" << std::endl;
	return -1;
      }
      inst.name = entity_name_t::MON(monmap.size());
      cout << "mkmonmap: adding " << inst << std::endl;
      monmap.add_mon(inst);
    }
  }

  if (monmap.size() == 0) {
    cerr << "usage: mkmonmap ip:port [...]" << std::endl;
    return -1;
  }

  // write it out
  cout << "mkmonmap: writing monmap epoch " << monmap.epoch << " to " << outfn << " (" << monmap.size() << " monitors)" << std::endl;
  int r = monmap.write(outfn);
  assert(r >= 0);
  
  return 0;
}
