
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include "include/Message.h"

#include "messages/MInodeSyncStart.h"

#include <string>


ostream& operator<<(ostream& out, set<int>& iset)
{
  bool first = true;
  for (set<int>::iterator it = iset.begin();
	   it != iset.end();
	   it++) {
	if (!first) out << ",";
	first = false;
	out << *it;
  }
  return out;
}


// ====== CInode =======
CInode::CInode() : LRUObject() {
  ref = 0;
  
  parent = NULL;
  nparents = 0;
  lru_next = lru_prev = NULL;
  
  dir_auth = CDIR_AUTH_PARENT;
  dir = NULL;  // create CDir as needed

  hard_pinned = 0;
  nested_hard_pinned = 0;
  //  state = 0;

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

ostream& operator<<(ostream& out, CInode& in)
{
  string path;
  in.make_path(path);
  return out << "[" << in.inode.ino << "]" << path << " " << &in;
}


void CInode::hit()
{
  popularity.hit();

  // hit my containing directory, too
  if (parent)
	parent->dir->hit();
}


// waiting

void CInode::add_write_waiter(Context *c) {
  if (waiting_for_write.size() == 0)
	get(CINODE_PIN_WWAIT);
  waiting_for_write.push_back(c);
}
void CInode::take_write_waiting(list<Context*>& ls)
{
  if (waiting_for_write.size())
	put(CINODE_PIN_WWAIT);
  ls.splice(ls.end(), waiting_for_write);  
}

void CInode::add_read_waiter(Context *c) {
  if (waiting_for_read.size() == 0)
	get(CINODE_PIN_RWAIT);
  waiting_for_read.push_back(c);
}
void CInode::take_read_waiting(list<Context*>& ls)
{
  if (waiting_for_write.size())
	put(CINODE_PIN_RWAIT);
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
  get(CINODE_PIN_IHARDPIN + hard_pinned);
  hard_pinned++;
  if (parent)
	parent->dir->adjust_nested_hard_pinned( 1 );
}

void CInode::hard_unpin() {
  hard_pinned--;
  put(CINODE_PIN_IHARDPIN + hard_pinned);
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


int CInode::dir_authority(MDCluster *mdc) 
{
  // explicit
  if (dir_auth >= 0)
	return dir_auth;

  // parent
  if (dir_auth == CDIR_AUTH_PARENT)
	return authority(mdc);

  // hashed
  assert(0);  //  throw "hashed not implemented";
  return CDIR_AUTH_HASH;
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


