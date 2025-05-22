// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_realm_watcher.h"

RGWRealmWatcher::~RGWRealmWatcher()
{
}

void RGWRealmWatcher::add_watcher(RGWRealmNotify type, Watcher& watcher)
{
  watchers.emplace(type, watcher);
}
