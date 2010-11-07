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


// features

const struct CompatSet::Feature feature_compat[] = {
  END_FEATURE
};
const struct CompatSet::Feature feature_incompat[] = {
  MDS_FEATURE_INCOMPAT_BASE,
  MDS_FEATURE_INCOMPAT_CLIENTRANGES,
  MDS_FEATURE_INCOMPAT_FILELAYOUT,
  MDS_FEATURE_INCOMPAT_DIRINODE,
  END_FEATURE
};
const struct CompatSet::Feature feature_ro_compat[] = {
  END_FEATURE
};

CompatSet mdsmap_compat(feature_compat,
			feature_ro_compat,
			feature_incompat);

// base (pre v0.20)
const struct CompatSet::Feature feature_compat_base[] = {
  END_FEATURE
};
const struct CompatSet::Feature feature_incompat_base[] = {
  MDS_FEATURE_INCOMPAT_BASE,
  END_FEATURE
};
const struct CompatSet::Feature feature_ro_compat_base[] = {
  END_FEATURE
};

CompatSet mdsmap_compat_base(feature_compat_base,
			     feature_ro_compat_base,
			     feature_incompat_base);


// ----

void MDSMap::print(ostream& out) 
{
  out << "epoch " << epoch << std::endl;
  out << "\nclient_epoch " << client_epoch << std::endl;
  out << "created " << created << std::endl;
  out << "modified " << modified << std::endl;
  out << "tableserver " << tableserver << std::endl;
  out << "root " << root << std::endl;
  out << "session_timeout " << session_timeout << "\n"
      << "session_autoclose " << session_autoclose << "\n";
  out << "last_failure " << last_failure << "\n"
      << "last_failure_osd_epoch" << last_failure_osd_epoch << "\n";

  out << "\ncompat\t" << compat << std::endl;
  out << "\nmax_mds\t" << max_mds << std::endl;
  out << "in\t" << in << "\n"
      << "up\t" << up << "\n"
      << "failed\t" << failed << "\n"
      << "stopped\t" << stopped << "\n";

  multimap< pair<unsigned,unsigned>, uint64_t > foo;
  for (map<uint64_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       p++)
    foo.insert(pair<pair<unsigned,unsigned>,uint64_t>(pair<unsigned,unsigned>(p->second.rank, p->second.inc-1), p->first));

  for (multimap< pair<unsigned,unsigned>, uint64_t >::iterator p = foo.begin();
       p != foo.end();
       p++) {
    mds_info_t& info = mds_info[p->second];
    
    out << p->second << ":\t"
	<< info.addr
	<< " '" << info.name << "'"
	<< " mds" << info.rank
	<< "." << info.inc
	<< " " << ceph_mds_state_name(info.state)
	<< " seq " << info.state_seq;
    if (info.laggy())
      out << " laggy since " << info.laggy_since;
    if (info.standby_for_rank >= 0 ||
	info.standby_for_rank >= 0) {
      out << " (standby for";
      if (info.standby_for_rank >= 0) 
	out << " rank " << info.standby_for_rank;
      if (info.standby_for_name.length())
	out << " '" << info.standby_for_name << "'";
      out << ")";
    }
    if (info.export_targets.size())
      out << " export_targets=" << info.export_targets;
    out << "\n";    
  }

}



void MDSMap::print_summary(ostream& out) 
{
  map<int,string> by_rank;
  map<string,int> by_state;

  for (map<uint64_t,mds_info_t>::iterator p = mds_info.begin();
       p != mds_info.end();
       p++) {
    string s = ceph_mds_state_name(p->second.state);
    if (p->second.laggy())
      s += "(laggy or crashed)";

    if (p->second.rank >= 0)
      by_rank[p->second.rank] = s;
    else
      by_state[s]++;
  }

  out << "e" << get_epoch() << ": " << up.size() << "/" << in.size() << "/" << max_mds << " up";

  if (by_rank.size())
    out << " " << by_rank;

  for (map<string,int>::reverse_iterator p = by_state.rbegin(); p != by_state.rend(); p++)
    out << ", " << p->second << " " << p->first;
  
  if (failed.size())
    out << ", " << failed.size() << " failed";
  if (stopped.size())
    out << ", " << stopped.size() << " stopped";
}
