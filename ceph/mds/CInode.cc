
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include "include/Message.h"

#include "messages/MInodeSyncStart.h"

#include <string>

// ====== CInode =======
CInode::CInode() : LRUObject() {
  ref = 0;
  
  parent = NULL;
  nparents = 0;
  lru_next = lru_prev = NULL;
  
  dir = NULL;
  state = 0;

  mid_fetch = false;	
}

CInode::~CInode() {
  if (dir) { delete dir; dir = 0; }
}

CInode *CInode::get_parent_inode() 
{
  if (parent) 
	return parent->dir->inode;
  return NULL;
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

void CInode::add_write_waiter(Context *c) {
  waiting_for_write.push_back(c);
}
void CInode::take_write_waiting(list<Context*>& ls)
{
  ls.splice(ls.end(), waiting_for_write);
}

void CInode::add_read_waiter(Context *c) {
  waiting_for_read.push_back(c);
}
void CInode::take_read_waiting(list<Context*>& ls)
{
  ls.splice(ls.end(), waiting_for_read);
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


