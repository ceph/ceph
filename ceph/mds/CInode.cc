
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

  hard_pinned = 0;
  nested_hard_pinned = 0;

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


// locking 
int CInode::adjust_nested_hard_pinned(int a) {
  nested_hard_pinned += a;
  if (parent) 
	parent->dir->adjust_nested_hard_pinned(a);
}

bool CInode::can_hard_pin() {
  if (parent)
	return parent->dir->can_hard_pin();
  return true;
}

void CInode::hard_pin() {
  get();
  hard_pinned++;
  if (parent)
	parent->dir->adjust_nested_hard_pinned( 1 );
}

void CInode::hard_unpin() {
  put();
  hard_pinned--;
  if (parent)
	parent->dir->adjust_nested_hard_pinned( -1 );
}

void CInode::add_hard_pin_waiter(Context *c) {
  parent->dir->add_hard_pin_waiter(c);
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


