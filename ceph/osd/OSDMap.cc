
#include "OSDCluster.h"


// serialize/unserialize

void OSDCluster::_rope(crope& r)
{
  r.append((char*)&version, sizeof(version));

  int ngroups = osd_groups.size();
  r.append((char*)&ngroups, sizeof(ngroups));
  for (int i=0; i<ngroups; i++) {
	r.append((char*)&osd_groups[i], sizeof(OSDGroup));
  }

  // failed
}

void OSDCluster::_unrope(crope& r, int& off)
{
  r.copy(off, sizeof(version), (char*)&version);
  off += sizeof(version);

  int ngroups;
  r.copy(off, sizeof(ngroups), (char*)&ngroups);
  off += sizeof(ngroups);

  osd_groups = vector<OSDGroup>(ngroups);
  for (int i=0; i<ngroups; i++) {
	r.copy(off, sizeof(OSDGroup), (char*)&osd_groups[i]);
	off += sizeof(OSDGroup);
  }

  // failed
}
