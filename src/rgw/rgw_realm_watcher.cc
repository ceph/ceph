// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_realm_watcher.h"

RGWRealmWatcher::~RGWRealmWatcher()
{
}

void RGWRealmWatcher::add_watcher(RGWRealmNotify type, Watcher& watcher)
{
  watchers.emplace(type, watcher);
}
