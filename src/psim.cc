
#include <iostream>

#include "crush/CrushWrapper.h"
#include "osd/OSDMap.h"
#include "config.h"
#include "include/buffer.h"

int main(int argc, char **argv)
{
  /*
   * you need to create a suitable osdmap first.  e.g., for 40 osds, 
   * $ ./osdmaptool --createsimple .ceph_monmap 40 --clobber .ceph_osdmap 
   */
  bufferlist bl;
  bl.read_file(".ceph_osdmap");
  OSDMap osdmap;
  osdmap.decode(bl);

  int n = osdmap.get_max_osd();
  int count[n];
  for (int i=0; i<n; i++) {
    osdmap.set_state(i, osdmap.get_state(i) | CEPH_OSD_UP);
    osdmap.set_offload(i, CEPH_OSD_IN);
    count[i] = 0;
  }

  ceph_file_layout layout = g_default_file_layout;
  layout.fl_pg_preferred = 2;
  for (int f = 1; f < 10000; f++) {  // files
    for (int b = 0; b < 4; b++) {   // blocks
      object_t oid(f, b);
      ceph_object_layout l = osdmap.file_to_object_layout(oid, layout);
      vector<int> osds;
      pg_t pgid = pg_t(l.ol_pgid);
      //cout << "oid " << oid << " pgid " << pgid << std::endl;
      osdmap.pg_to_osds(pgid, osds);
      for (unsigned i=0; i<osds.size(); i++) {
	//cout << " rep " << i << " on " << osds[i] << std::endl;
	count[osds[i]]++;
      }
    }
  }

  for (int i=0; i<n; i++) {
    cout << "osd" << i << "\t" << count[i] << std::endl;
  }
  
  return 0;
}
