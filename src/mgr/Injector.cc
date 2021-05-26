// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "mgr/Injector.h"

#include "osd/OSDMap.h"

#include "PyFormatter.h"

#include "PyUtil.h"


using namespace std;

int64_t Injector::get_num_osds() {
  return g_conf().get_val<int64_t>("mgr_inject_num_osds");
}

void Injector::mark_exists_osds(OSDMap* osdmap) {
  for(int osd = 0; osd < Injector::get_num_osds(); osd++) {
    osdmap->set_state(osd, CEPH_OSD_EXISTS);
  }
}

PyObject* Injector::get_python(const std::string& what) {
  PyFormatter f;

  if (what == "osd_map") {
    int64_t num_osds = Injector::get_num_osds();
    OSDMap* osdmap = new OSDMap;
    uuid_d id = uuid_d();
    osdmap->build_simple(g_ceph_context, 1, id, num_osds);
    // OSDMap::dump_osds filters not existent osds so we need
    // to set the state so we dump non existent ones too
    Injector::mark_exists_osds(osdmap);
    osdmap->dump(&f);
  } else {
    Py_RETURN_NONE
  }
  PyObject *obj = f.get();
  Py_INCREF(obj);
  return obj;
}
