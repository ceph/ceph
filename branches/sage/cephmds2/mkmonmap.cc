// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


bool parse_ip_port(const char *s, tcpaddr_t& tcpaddr)
{
  unsigned char addr[4];
  int port = 0;

  int count = 0; // digit count

  while (1) {
    // parse the #.
    int val = 0;
    int numdigits = 0;
    
    while (*s >= '0' && *s <= '9') {
      int digit = *s - '0';
      //cout << "digit " << digit << endl;
      val *= 10;
      val += digit;
      numdigits++;
      s++;
    }
    //cout << "val " << val << endl;
    
    if (numdigits == 0) return false;           // no digits
    if (count < 3 && *s != '.') return false;   // should have 3 periods
    if (count == 3 && *s != ':') return false;  // then a colon
    s++;

    if (count <= 3)
      addr[count] = val;
    else
      port = val;
    
    count++;
    if (count == 5) break;  
  }
  
  // copy into inst
  memcpy((char*)&tcpaddr.sin_addr.s_addr, (char*)addr, 4);
  tcpaddr.sin_port = port;

  return true;
}


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
      tcpaddr_t addr;
      if (!parse_ip_port(args[i], addr)) {
	cerr << "mkmonmap: invalid ip:port '" << args[i] << "'" << endl;
	return -1;
      }
      entity_inst_t inst;
      inst.addr.set_addr(addr);
      inst.name = MSG_ADDR_MON(monmap.num_mon);
      cout << "mkmonmap: adding " << inst << endl;
      monmap.add_mon(inst);
    }
  }

  if (monmap.num_mon == 0) {
    cerr << "usage: mkmonmap ip:port [...]" << endl;
    return -1;
  }

  // write it out
  cout << "mkmonmap: writing monmap to " << outfn << " (" << monmap.num_mon << " monitors)" << endl;
  int r = monmap.write(outfn);
  assert(r >= 0);
  
  return 0;
}
