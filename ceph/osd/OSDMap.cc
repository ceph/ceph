
#include "OSDMap.h"




// serialize/unserialize

void OSDMap::encode(bufferlist& blist)
{
  blist.append((char*)&version, sizeof(version));

  int ngroups = osd_groups.size();
  blist.append((char*)&ngroups, sizeof(ngroups));
  for (int i=0; i<ngroups; i++) {
	osd_groups[i]._encode(blist);
  }

  _encode(down_osds, blist);
  _encode(failed_osds, blist);
}

void OSDMap::decode(bufferlist& blist)
{
  int off = 0;
  blist.copy(off, sizeof(version), (char*)&version);
  off += sizeof(version);

  int ngroups;
  blist.copy(off, sizeof(ngroups), (char*)&ngroups);
  off += sizeof(ngroups);

  osd_groups = vector<OSDGroup>(ngroups);
  for (int i=0; i<ngroups; i++) {
	osd_groups[i]._decode(blist, off);
  }

  _decode(down_osds, blist, off);
  _decode(failed_osds, blist, off);

  init_rush();
}
 
