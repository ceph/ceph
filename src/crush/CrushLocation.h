// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CRUSH_LOCATION_H
#define CEPH_CRUSH_LOCATION_H

#include <map>
#include <mutex>
#include <string>

class CephContext;

class CrushLocation {
  CephContext *cct;
  std::multimap<std::string,std::string> loc;
  std::mutex lock;

  int _parse(const std::string& s);

public:
  CrushLocation(CephContext *c) : cct(c) {
    update_from_conf();
  }

  int update_from_conf();  ///< refresh from config
  int update_from_hook();  ///< call hook, if present
  int init_on_startup();

  std::multimap<std::string,std::string> get_location() {
    std::lock_guard<std::mutex> l(lock);
    return loc;
  }
};

#endif
