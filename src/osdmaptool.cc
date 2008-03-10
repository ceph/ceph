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
#include <errno.h>

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "osd/OSDMap.h"
#include "mon/MonMap.h"

void usage(const char *me)
{
  cout << me << " usage: [--print] [--createsimple <monmapfile> <numosd> [--clobber] [--pgbits <bitsperosd>]] <mapfilename>" << std::endl;
  exit(1);
}

void printmap(const char *me, OSDMap *m)
{
  cout << me << ": osdmap: epoch " << m->get_epoch() << std::endl
       << me << ": osdmap: fsid " << m->get_fsid() << std::endl;
  /*for (unsigned i=0; i<m->mon_inst.size(); i++)
    cout << me << ": osdmap:  " //<< "mon" << i << " " 
	 << m->mon_inst[i] << std::endl;
  */
}

int read_file(const char *fn, bufferlist &bl)
{
  struct stat st;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    cerr << "can't open " << fn << ": " << strerror(errno) << std::endl;
    return -errno;
  }
  ::fstat(fd, &st);
  bufferptr bp(st.st_size);
  bl.append(bp);
  ::read(fd, (void*)bl.c_str(), bl.length());
  ::close(fd);
  return 0;
}

int write_file(const char *fn, bufferlist &bl)
{
  int fd = ::open(fn, O_WRONLY|O_CREAT|O_TRUNC, 0644);
  if (fd < 0) {
    cerr << "can't write " << fn << ": " << strerror(errno) << std::endl;
    return -errno;
  }
  ::write(fd, (void*)bl.c_str(), bl.length());
  ::close(fd);
  return 0;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  const char *me = argv[0];

  const char *fn = 0;
  bool print = false;
  bool createsimple = false;
  const char *monmapfn;
  int num_osd;
  int pg_bits = g_conf.osd_pg_bits;
  bool clobber = false;
  bool modified = false;
  list<entity_addr_t> add, rm;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--print") == 0)
      print = true;
    else if (strcmp(args[i], "--createsimple") == 0) {
      createsimple = true;
      monmapfn = args[++i];
      num_osd = atoi(args[++i]);
    } else if (strcmp(args[i], "--clobber") == 0) 
      clobber = true;
    else if (strcmp(args[i], "--pgbits") == 0)
      pg_bits = atoi(args[++i]);
    else if (!fn)
      fn = args[i];
    else 
      usage(me);
  }
  if (!fn)
    usage(me);
  
  OSDMap osdmap;
  bufferlist bl;

  cout << me << ": osdmap file '" << fn << "'" << std::endl;
  
  int r = 0;
  if (!(createsimple && clobber))
    r = read_file(fn, bl);
  if (!createsimple && r < 0) {
    cerr << me << ": couldn't open " << fn << ": " << strerror(errno) << std::endl;
    return -1;
  }    
  else if (createsimple && !clobber && r == 0) {
    cerr << me << ": " << fn << " exists, --clobber to overwrite" << std::endl;
    return -1;
  }

  if (!print && !modified)
    usage(me);

  if (createsimple) {
    MonMap monmap;
    int r = monmap.read(monmapfn);
    if (r < 0) {
      cerr << me << ": can't read monmap from " << monmapfn << ": " << strerror(r) << std::endl;
      exit(1);
    }
    osdmap.build_simple(0, monmap.fsid, num_osd, pg_bits, 0);
    modified = true;
  }

  if (modified)
    osdmap.inc_epoch();
  
  if (print) 
    printmap(me, &osdmap);

  if (modified) {
    osdmap.encode(bl);

    // write it out
    cout << me << ": writing epoch " << osdmap.get_epoch()
	 << " to " << fn
	 << std::endl;
    int r = write_file(fn, bl);
    assert(r >= 0);
  }
  

  return 0;
}
