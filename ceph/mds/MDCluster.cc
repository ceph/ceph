
#include "MDCluster.h"
#include "CDir.h"
#include "CInode.h"

MDCluster::MDCluster()
{
}


MDCluster::~MDCluster()
{
}

int MDCluster::add_mds(MDS *m)
{
  mds.push_back(m);
  return mds.size() - 1;
}

int MDCluster::hash_dentry( CDir *dir, string& dn )
{
  unsigned r = dir->get_inode()->inode.ino;

  for (unsigned i=0; i<dn.length(); i++)
	r += (dn[r] ^ i);
  
  return r % get_size();
}
