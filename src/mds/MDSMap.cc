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
  out << "epoch " << get_epoch() << std::endl;
  out << "max_mds " << max_mds << std::endl;

  entity_inst_t blank;
  set<int> all;
  get_mds_set(all);

  for (set<int>::iterator p = all.begin();
       p != all.end();
       ++p) {
    if (standby_for.count(*p) && !standby_for[*p].empty()) {
      out << " mds" << *p << "." << mds_inc[*p]
	  << " : " << get_state_name(get_state(*p))
	  << " : " << (have_inst(*p) ? get_inst(*p) : blank)
	  << " : +" << standby_for[*p].size()
	  << " standby " << standby_for[*p]
	  << std::endl;
    } else {
      out << " mds" << *p << "." << mds_inc[*p]
	  << " : " << get_state_name(get_state(*p))
	  << " : " << (have_inst(*p) ? get_inst(*p) : blank)
	  << std::endl;
    }
  }
  if (!standby_any.empty()) {
    out << " +" << standby_any.size() << " shared standby " << standby_any << std::endl;
  }
}



void MDSMap::print_summary(ostream& out) 
{
  stringstream ss;

  set<int> all;
  get_mds_set(all);

  int standby_spec = 0;
  map<int,int> by_state;
  for (set<int>::iterator p = all.begin();
       p != all.end();
       ++p) {
    by_state[get_state(*p)]++;
    standby_spec += get_num_standby_for(*p);
  }
  
  for (map<int,int>::iterator p = by_state.begin(); p != by_state.end(); p++) {
    if (p != by_state.begin())
      ss << ", ";
    ss << p->second << " " << MDSMap::get_state_name(p->first);
  }
  if (get_num_standby_any())
    ss << ", " << get_num_standby_any() << " standby (any)";
  if (standby_spec)
    ss << ", " << standby_spec << " standby (specific)";
  
  string states = ss.str();  
  out << "e" << get_epoch() << ": "
      << all.size() << " nodes: " 
      << states;
}
