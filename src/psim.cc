
#include <iostream>

#include "crush/CrushWrapper.h"
#include "osd/OSDMap.h"
#include "config.h"
#include "include/buffer.h"

int main()
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

  for (int f = 1; f < 10000; f++) {  // files
    for (int b = 0; b < 4; b++) {   // blocks
      object_t oid(f, b);
      //cout << "oid " << oid << std::endl;
      ceph_object_layout l = osdmap.file_to_object_layout(oid, g_default_file_layout);
      vector<int> osds;
      osdmap.pg_to_osds(pg_t(l.ol_pgid), osds);
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
