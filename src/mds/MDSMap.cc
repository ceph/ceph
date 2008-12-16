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


#include "MDSMap.h"

#include <sstream>
using std::stringstream;

void MDSMap::print(ostream& out) 
{
  out << "epoch " << epoch << std::endl;
  out << "\nclient_epoch " << client_epoch << std::endl;
  out << "created " << created << std::endl;
  out << "tableserver " << tableserver << std::endl;
  out << "root " << root << std::endl;
  out << "session_timeout " << session_timeout << "\n"
      << "session_autoclose " << session_autoclose << "\n";

  out << "\nmax_mds " << max_mds << std::endl;

  set<int> upset;
  get_up_mds_set(upset);
  out << "in " << in << "\n"
      << "up " << upset << "\n";

  map< pair<unsigned,unsigned>, entity_addr_t > foo;
  for (map<entity_addr_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       p++)
    foo[pair<unsigned,unsigned>(p->second.mds, p->second.inc-1)] = p->first;

  for (map< pair<unsigned,unsigned>, entity_addr_t >::iterator p = foo.begin();
       p != foo.end();
       p++) {
    mds_info_t& info = mds_info[p->second];
    
    out << info.addr
	<< " mds" << info.mds
	<< "." << info.inc
	<< " " << get_state_name(info.state)
	<< " seq " << info.state_seq;
    if (info.laggy())
      out << " laggy since " << info.laggy_since;
    out << "\n";    
  }

  if (failed.size())
    out << "failed " << failed << "\n";
  if (stopped.size())
    out << "stopped " << failed << "\n";
}



void MDSMap::print_summary(ostream& out) 
{
  map<int,int> by_state;
  for (map<entity_addr_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       p++)
    by_state[p->second.state]++;

  out << "e" << get_epoch() << ": " << up.size() << "/" << in.size() << " up";

  for (map<int,int>::iterator p = by_state.begin(); p != by_state.end(); p++)
    out << ", " << p->second << " " << get_state_name(p->first);
  
  if (failed.size())
    out << ", " << failed.size() << " failed";
}
