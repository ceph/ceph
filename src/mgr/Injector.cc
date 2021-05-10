#include "PyFormatter.h"
#include "PyUtil.h"
#include "osd/OSDMap.h"
#include "mgr/inject.h"

PyObject* Injector::get_python(const std::string &what) {
  PyFormatter f;
  OSDMap *osdmap = new OSDMap;
  int num_osds = 1000;
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
