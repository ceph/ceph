
#include "MDCluster.h"
#include "CDir.h"
#include "CInode.h"

#include <iostream>
using namespace std;

#include <sys/types.h>
#include <unistd.h>

MDCluster::MDCluster(int num_mds, int num_osd)
{
  this->num_mds = num_mds;
  this->num_osd = num_osd;

  map_osds();
}


void MDCluster::map_osds()
{
  // logs on ~10% of osd
  osd_log_begin = 0;
  osd_log_end = num_osd / 10;
  if (osd_log_end > num_mds)
	osd_log_end = num_mds;

  // metadata on the rest
  osd_meta_begin = osd_log_end;
  osd_meta_end = num_osd;

  cout << "mdcluster: " << num_mds << " mds, " << num_osd << " osd" << endl;
  cout << "mdcluster:  logs on " << (osd_log_end-osd_log_begin) << " osd [" << osd_log_begin << ", " << osd_log_end << ")" << endl;
  cout << "mdcluster:  metadata on " << (osd_meta_end-osd_meta_begin) << " osd [" << osd_meta_begin << ", " << osd_meta_end << ")" << endl;
}



int MDCluster::hash_dentry( CDir *dir, string& dn )
{
  unsigned r = dir->get_inode()->inode.ino;
  
  for (unsigned i=0; i<dn.length(); i++)
	r += (dn[r] ^ i);
  
  return osd_meta_begin + (r % (osd_meta_end - osd_meta_begin));
}


int MDCluster::get_meta_osd(inodeno_t ino)
{
  return osd_meta_begin + (ino % (osd_meta_end - osd_meta_begin));
}

object_t MDCluster::get_meta_oid(inodeno_t ino)
{
  return ino;
}


int MDCluster::get_log_osd(int mds)
{
  return osd_log_begin + (mds % (osd_log_end - osd_log_begin));
}

object_t MDCluster::get_log_oid(int mds)
{
  return ((object_t)1000*(object_t)getpid()) + (object_t)mds;
}
