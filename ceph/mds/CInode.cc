
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include <string>

// ====== CInode =======
CInode::CInode() : LRUObject() {
  ref = 0;
  
  parent = NULL;
  nparents = 0;
  lru_next = lru_prev = NULL;
  
  dir = NULL;

  mid_fetch = false;	
}

CInode::~CInode() {
  if (dir) { delete dir; dir = 0; }
}


void CInode::make_path(string& s)
{
  if (parent) {
	parent->dir->inode->make_path(s);
	s += "/";
	s += parent->name;
  } else 
	s = "";  // root
}

// waiting
void CInode::add_inode_waiter(Context *c) {
  waiting_on_inode.push_back(c);
}


void CInode::take_inode_waiting(list<Context*>& ls)
{
  ls.splice(ls.end(), waiting_on_inode);

  if (dir) 
	dir->take_waiting(ls);
}



// authority

int CInode::authority(MDCluster *cl) {
  if (parent == NULL)
	return 0;  // i am root
  return parent->dir->dentry_authority( parent->name, cl );
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


