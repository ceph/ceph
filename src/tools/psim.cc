// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd/OSDMap.h"
#include "include/buffer.h"

int main(int argc, char **argv)
{
  /*
   * you need to create a suitable osdmap first.  e.g., for 40 osds, 
   * $ ./osdmaptool --createsimple 40 --clobber .ceph_osdmap
   */
  bufferlist bl;
  std::string error;
  if (bl.read_file(".ceph_osdmap", &error)) {
    cout << argv[0] << ": error reading .ceph_osdmap: " << error << std::endl;
    return 1;
  }
  OSDMap osdmap;

  try {
    osdmap.decode(bl);
  } catch (ceph::buffer::end_of_buffer &eob) {
    cout << "Exception (end_of_buffer) in decode(), exit." << std::endl;
    exit(1);
  }

  //osdmap.set_primary_affinity(0, 0x8000);
  //osdmap.set_primary_affinity(3, 0);

  int n = osdmap.get_max_osd();
  int count[n];
  int first_count[n];
  int primary_count[n];
  int size[4];

  memset(count, 0, sizeof(count));
  memset(first_count, 0, sizeof(first_count));
  memset(primary_count, 0, sizeof(primary_count));
  memset(size, 0, sizeof(size));

  for (int i=0; i<n; i++) {
    osdmap.set_state(i, osdmap.get_state(i) | CEPH_OSD_UP);
    //if (i<12)
      osdmap.set_weight(i, CEPH_OSD_IN);
  }

  //pg_pool_t *p = (pg_pool_t *)osdmap.get_pg_pool(0);
  //p->type = pg_pool_t::TYPE_ERASURE;

  for (int n = 0; n < 10; n++) {   // namespaces
    char nspace[20];
    snprintf(nspace, sizeof(nspace), "n%d", n);
  for (int f = 0; f < 5000; f++) {  // files
    for (int b = 0; b < 4; b++) {   // blocks
      char foo[20];
      snprintf(foo, sizeof(foo), "%d.%d", f, b);
      object_t oid(foo);
      ceph_object_layout l = osdmap.make_object_layout(oid, 0, nspace);
      vector<int> osds;
      pg_t pgid = pg_t(l.ol_pgid);
      //pgid.u.ps = f * 4 + b;
      int primary;
      osdmap.pg_to_acting_osds(pgid, &osds, &primary);
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
      if (osds.size())
	first_count[osds[0]]++;
      if (primary >= 0)
	primary_count[primary]++;
    }
  }
  }

  uint64_t avg = 0;
  for (int i=0; i<n; i++) {
    cout << "osd." << i << "\t" << count[i]
	 << "\t" << first_count[i]
	 << "\t" << primary_count[i]
	 << std::endl;
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
