
#include "MDCluster.h"
#include "CDir.h"
#include "CInode.h"

#include <iostream>
using namespace std;

#include <sys/types.h>
#include <unistd.h>

#include "include/config.h"


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

  dout(10) << "mdcluster: " << num_mds << " mds, " << num_osd << " osd" << endl;
  dout(10) << "mdcluster:  logs on " << (osd_log_end-osd_log_begin) << " osd [" << osd_log_begin << ", " << osd_log_end << ")" << endl;
  dout(10) << "mdcluster:  metadata on " << (osd_meta_end-osd_meta_begin) << " osd [" << osd_meta_begin << ", " << osd_meta_end << ")" << endl;
}



/* hash a directory inode, dentry to a mds server
 */
int MDCluster::hash_dentry( inodeno_t dirino, string& dn )
{
  unsigned r = dirino;
  
  for (unsigned i=0; i<dn.length(); i++)
	r += (dn[r] ^ i);
  
  r %= num_mds;

  dout(12) << "hash_dentry(" << dirino << ", " << dn << ") -> " << r;
  return r;
}


/* map an inode to an osd
 */
int MDCluster::get_meta_osd(inodeno_t ino)
{
  return osd_meta_begin + (ino % (osd_meta_end - osd_meta_begin));
}

object_t MDCluster::get_meta_oid(inodeno_t ino)
{
  return ino;
}


/* map an mds log to an osd
 */
int MDCluster::get_log_osd(int mds)
{
  return osd_log_begin + (mds % (osd_log_end - osd_log_begin));
}

object_t MDCluster::get_log_oid(int mds)
{
  return ((object_t)1000*(object_t)getpid()) + (object_t)mds;
}
