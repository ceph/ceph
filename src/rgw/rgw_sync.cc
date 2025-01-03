// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_sync.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

std::ostream&  RGWMetaSyncStatusManager::gen_prefix(std::ostream& out) const
{
  return out << "meta sync: ";
}

unsigned RGWMetaSyncStatusManager::get_subsys() const
{
  return dout_subsys;
}

void RGWRemoteMetaLog::finish()
{
  going_down = true;
  stop();
}