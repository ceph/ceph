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

  set<int> all;
  get_mds_set(all);

  for (set<int>::iterator p = all.begin();
       p != all.end();
       ++p) {
    out << " mds" << *p << "." << mds_inc[*p]
	<< " : " << get_state_name(get_state(*p));
    if (have_inst(*p))
      out << " : " << get_inst(*p).addr
	  << (is_laggy(get_inst(*p).addr) ? " LAGGY" : "");
    out << "\n";
    if (standby_for.count(*p) && !standby_for[*p].empty()) {
      //out << " : +" << standby_for[*p].size() << std::endl;
      for (set<entity_addr_t>::iterator q = standby_for[*p].begin();
	   q != standby_for[*p].end();
	   q++)
	out << " mds" << *p << ".? : " << get_state_name(standby[*q].state)
	    << " : " << *q << std::endl;
    }
  }
  if (!standby_any.empty()) {
    for (set<entity_addr_t>::iterator q = standby_any.begin();
	 q != standby_any.end();
	 q++)
      out << " mds?.? : " << get_state_name(standby[*q].state) << " : " << *q << std::endl;
  }
}



void MDSMap::print_summary(ostream& out) 
{
  stringstream ss;

  set<int> all;
  get_mds_set(all);

  int standby_spec = 0;
  map<string,int> by_state;
  for (set<int>::iterator p = all.begin();
       p != all.end();
       ++p) {
    string s = get_state_name(get_state(*p));
    if (laggy.count(get_inst(*p).addr))
      s += "(laggy)";
    by_state[s]++;
    standby_spec += get_num_standby_for(*p);
  }
  
  for (map<string,int>::iterator p = by_state.begin(); p != by_state.end(); p++) {
    if (p != by_state.begin())
      ss << ", ";
    ss << p->second << " " << p->first;
  }
  if (laggy.size())
    ss << ", " << laggy.size() << " laggy";
  if (get_num_standby_any())
    ss << ", " << get_num_standby_any() << " standby (any)";
  if (standby_spec)
    ss << ", " << standby_spec << " standby (specific)";
  
  string states = ss.str();  
  out << "e" << get_epoch() << ": "
      << all.size() << " nodes: " 
      << states;
}
