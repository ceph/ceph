// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr/Injector.h"

#include "osd/OSDMap.h"

#include "PyFormatter.h"

#include "PyUtil.h"

using namespace std;

int num_osds = 0;
OSDMap *osd_map_global = nullptr;
int64_t Injector::get_num_osds() {
  return g_conf().get_val<int64_t>("mgr_inject_num_osds");
}

// OSDMap::dump_osds filters not existent osds so we need
// to set the state so we dump non existent ones too
void Injector::mark_exists_osds(OSDMap *osdmap) {
  for (int osd = 0; osd < Injector::get_num_osds(); osd++) {
    osdmap->set_state(osd, CEPH_OSD_EXISTS);
  }
}

PyObject *Injector::get_python(const std::string &what) {
  PyJSONFormatter f;

  f.open_object_section("");
  if (what == "osd_map") {
    int64_t new_num_osds = Injector::get_num_osds();
    if (osd_map_global == nullptr || new_num_osds != num_osds) {
      osd_map_global = new OSDMap;
      uuid_d id = uuid_d();
      osd_map_global->build_simple(g_ceph_context, 1, id, new_num_osds);
      Injector::mark_exists_osds(osd_map_global);
      num_osds = new_num_osds;
    }
    osd_map_global->dump(&f);
    f.close_section();
  } else {
    Py_RETURN_NONE;
  }
  return f.get();
}

OSDMap *Injector::get_osdmap() {
  int64_t num_osds = Injector::get_num_osds();
  OSDMap *osdmap = new OSDMap;
  uuid_d id = uuid_d();
  osdmap->build_simple(g_ceph_context, 1, id, num_osds);
  Injector::mark_exists_osds(osdmap);
  return osdmap;
}
