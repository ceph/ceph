
#include "include/CInode.h"
#include "include/CDir.h"
#include "include/MDS.h"
#include <string>

// ====== CInode =======

CInode::~CInode() {
  if (dir) { delete dir; dir = 0; }
}

void CInode::add_parent(CDentry *p) {
  nparents++;
  if (nparents == 1)         // first
	parent = p;
  else if (nparents == 2) {  // second, switch to the vector
	parents.push_back(parent);
	parents.push_back(p);
  } else                     // additional
	parents.push_back(p);
}

bit_vector CInode::get_dist_spec(MDS *mds)
{
  bit_vector ds;

  // FIXME make me smarter

  // just us.
  if (ds.size() <= mds->get_nodeid())
	ds.resize( mds->get_nodeid()+1 );
  ds[ mds->get_nodeid() ] = true;

  return ds;
}


void CInode::dump(int dep)
{
  string ind(dep, '\t');
  //cout << ind << "[inode " << this << "]" << endl;
  
  if (dir)
	dir->dump(dep);
}

void CInode::dump_to_disk(MDS *mds) 
{
  if (dir)
	dir->dump_to_disk(mds);
}


