
#include <iostream>

#include "crush/CrushWrapper.h"
#include "osd/OSDMap.h"
#include "common/config.h"
#include "include/buffer.h"

int main(int argc, char **argv)
{
  /*
   * you need to create a suitable osdmap first.  e.g., for 40 osds, 
   * $ ./osdmaptool --createsimple .ceph_monmap 40 --clobber .ceph_osdmap 
   */
  bufferlist bl;
  std::string error;
  if (bl.read_file(".ceph_osdmap", &error)) {
    cout << argv[0] << ": error reading .ceph_osdmap: " << error << std::endl;
    return 1;
  }
  OSDMap osdmap;
  osdmap.decode(bl);

  int n = osdmap.get_max_osd();
  int count[n];
  for (int i=0; i<n; i++) {
    osdmap.set_state(i, osdmap.get_state(i) | CEPH_OSD_UP);
    //if (i<8)
      osdmap.set_weight(i, CEPH_OSD_IN);
    count[i] = 0;
  }

  int size[4];
  for (int i=0; i<4; i++)
    size[i] = 0;

  for (int f = 0; f < 50000; f++) {  // files
    for (int b = 0; b < 4; b++) {   // blocks
      char foo[20];
      snprintf(foo, sizeof(foo), "%d.%d", f, b);
      object_t oid(foo);
      ceph_object_layout l = osdmap.make_object_layout(oid, 0);
	//osdmap.file_to_object_layout(oid, g_default_file_layout);
      vector<int> osds;
      pg_t pgid = pg_t(l.ol_pgid);
      //pgid.u.ps = f * 4 + b;
      osdmap.pg_to_osds(pgid, osds);
      size[osds.size()]++;
#if 0
      if (0) {
	hash<object_t> H;
	int x = H(oid);
	x = ceph_stable_mod(x, 1023, 1023);
	int s = crush_hash32(x) % 15;
	//cout << "ceph_psim: x = " << x << " s = " << s << std::endl;
	//osds[0] = s;
      }
#endif
      //osds[0] = crush_hash32(f) % n;
      //cout << "oid " << oid << " pgid " << pgid << " on " << osds << std::endl;
      for (unsigned i=0; i<osds.size(); i++) {
	//cout << " rep " << i << " on " << osds[i] << std::endl;
	count[osds[i]]++;
      }
    }
  }

  uint64_t avg = 0;
  for (int i=0; i<n; i++) {
    cout << "osd." << i << "\t" << count[i] << std::endl;
    avg += count[i];
  }
  avg /= n;
  double dev = 0;
  for (int i=0; i<n; i++)
    dev += (avg - count[i]) * (avg - count[i]);
  dev /= n;
  dev = sqrt(dev);

  double pgavg = (double)osdmap.get_pg_pool(0)->get_pg_num() / (double)n;
  double edev = sqrt(pgavg) * (double)avg / pgavg;
  cout << " avg " << avg
       << " stddev " << dev
       << " (expected " << edev << ")"
       << " (indep object placement would be " << sqrt(avg) << ")" << std::endl;

  for (int i=0; i<4; i++) {
    cout << "size" << i << "\t" << size[i] << std::endl;
  }
  
  return 0;
}
