// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ActivePyModule.h"

#include "mgr/Injector.h"

#include "PyFormatter.h"

#include "PyUtil.h"

#include "osd/OSDMap.h"

using namespace std;

int64_t Injector::get_num_osds() {
  return g_conf().get_val<int64_t>("inject_num_osds");
}

PyObject* Injector::get_python(const std::string& what) {
  PyFormatter f;
  OSDMap* osdmap = new OSDMap;
  int64_t num_osds = Injector::get_num_osds();

  osdmap->set_max_osd(num_osds);
  for (int i = 0; i < num_osds; i++) {
    osdmap->set_state(i, CEPH_OSD_EXISTS);
  }
  osdmap->set_epoch(40);
  if (what == "osd_map") {
    osdmap->dump(&f);
  } else {
    return nullptr;
  }
  return f.get();
}
