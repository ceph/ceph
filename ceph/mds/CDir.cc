
#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"
#include "MDStore.h"
#include "MDS.h"
#include "MDCluster.h"

#include "include/Context.h"


// CDir

void CDir::hit() 
{
  popularity.hit();

  // hit parent inodes
  CInode *in = inode;
  while (in) {
	in->popularity.hit();
	if (in->parent)
	  in = in->parent->dir->inode;
	else
	  break;
  }
}


void CDir::add_child(CDentry *d) 
{
  //cout << "adding " << d->name << " to " << this << endl;
  items[d->name] = d;
  d->dir = this;
  
  nitems++;
  namesize += d->name.length();
  
  if (nitems == 1)
	inode->get();       // pin parent
}

void CDir::remove_child(CDentry *d) {
  map<string, CDentry*>::iterator iter = items.find(d->name);
  items.erase(iter);

  nitems--;
  namesize -= d->name.length();

  if (nitems == 0)
	inode->put();       // release parent.
}


CDentry* CDir::lookup(string n) {
  //cout << " lookup " << n << " in " << this << endl;
  map<string,CDentry*>::iterator iter = items.find(n);
  if (iter == items.end()) return NULL;

  //cout << "  lookup got " << iter->second << endl;
  return iter->second;
}


int CDir::dentry_authority(string& dn, MDCluster *mdc)
{
  if (dir_auth == CDIR_AUTH_PARENT) {
	return inode->authority( mdc );       // same as my inode
  }
  if (dir_auth == CDIR_AUTH_HASH) {
	return mdc->hash_dentry( this, dn );  // hashed
  }

  // explicit for this whole dir
  return dir_auth;
}

int CDir::dir_authority(MDCluster *mdc) 
{
  // explicit
  if (dir_auth >= 0)
	return dir_auth;

  // parent
  if (dir_auth == CDIR_AUTH_PARENT)
	return inode->authority(mdc);

  // hashed
  throw "hashed not implemented";
  return CDIR_AUTH_HASH;
}


// wiating

void CDir::add_waiter(string& dentry,
								Context *c) {
  waiting_on_dentry[ dentry ].push_back(c);
}

void CDir::add_waiter(Context *c) {
  waiting_on_all.push_back(c);
}


void CDir::take_waiting(string& dentry,
						list<Context*>& ls)
{
  if (waiting_on_dentry.count(dentry) > 0)
	ls.splice(ls.end(), waiting_on_dentry[ dentry ]);
}

void CDir::take_waiting(list<Context*>& ls)
{
  ls.splice(ls.end(), waiting_on_all);

  // all dentry waiters
  hash_map<string, list<Context*> >::iterator it;
  for (it = waiting_on_dentry.begin(); it != waiting_on_dentry.end(); it++) 
	ls.splice(ls.end(), it->second);
}


// locking and freezing


void CDir::add_hard_pin_waiter(Context *c) {
  if (state & CDIR_MASK_FROZEN) 
	add_waiter(c);
  else
	inode->parent->dir->add_hard_pin_waiter(c);
}
	
  
void CDir::hard_pin() {
  inode->get();
  hard_pinned++;
  inode->adjust_nested_hard_pinned( 1 );
}

void CDir::hard_unpin() {
  inode->put();
  hard_pinned--;
  inode->adjust_nested_hard_pinned( -1 );

  // pending freeze?
  if (waiting_to_freeze.size() &&
	  hard_pinned + nested_hard_pinned == 0)
	freeze_finish();
}

int CDir::adjust_nested_hard_pinned(int a) {
  nested_hard_pinned += a;
  inode->adjust_nested_hard_pinned(a);

  // pending freeze?
  if (waiting_to_freeze.size() &&
	  hard_pinned + nested_hard_pinned == 0)
	freeze_finish();
}



bool CDir::is_frozen() 
{
  if (is_freeze_root())
	return true;
  if (inode->parent)
	return inode->parent->dir->is_freeze_root();
  return false;
}

void CDir::add_freeze_waiter(Context *c)
{
  // wait on freeze root
  CDir *t = this;
  while (!t->is_freeze_root()) {
	t = t->inode->parent->dir;
  }
  t->add_waiter(c);
}

void CDir::freeze(Context *c)
{
  assert((state & (CDIR_MASK_FROZEN|CDIR_MASK_FREEZING)) == 0);

  if (nested_hard_pinned == 0) {
	cout << "freeze " << *inode << endl;

	state_set(CDIR_MASK_FROZEN);
	inode->hard_pin();  // hard_pin for duration of freeze
  
	// easy, we're frozen
	c->finish(0);
	delete c;

  } else {
	state_set(CDIR_MASK_FREEZING);
	cout << "freeze + wait " << *inode << endl;
	// need to wait for pins to expire
	waiting_to_freeze.push_back(c);
  }
}

void CDir::freeze_finish()
{
  cout << "freeze_finish " << *inode << endl;

  inode->hard_pin();  // hard_pin for duration of freeze

  Context *c = waiting_to_freeze.front();
  waiting_to_freeze.pop_front();
  state_clear(CDIR_MASK_FREEZING);
  state_set(CDIR_MASK_FROZEN);

  if (c) {
	c->finish(0);
	delete c;
  }
}

void CDir::unfreeze()  // thaw?
{
  cout << "unfreeze " << *inode << endl;
  state_clear(CDIR_MASK_FROZEN);
  inode->hard_unpin();
  
  list<Context*> finished;
  take_waiting(finished);

  list<Context*>::iterator it;
  for (it = finished.begin(); it != finished.end(); it++) {
	Context *c = *it;
	c->finish(0);
	delete c;
  }
}



// debug shite


void CDir::dump(int depth) {
  string ind(depth, '\t');

  map<string,CDentry*>::iterator iter = items.begin();
  while (iter != items.end()) {
	CDentry* d = iter->second;
	char isdir = ' ';
	if (d->inode->dir != NULL) isdir = '/';
	cout << ind << d->inode->inode.ino << " " << d->name << isdir << endl;
	d->inode->dump(depth+1);
	iter++;
  }

  if (!(state & CDIR_MASK_COMPLETE))
	cout << ind << "..." << endl;
  if (state & CDIR_MASK_DIRTY)
	cout << ind << "[dirty]" << endl;

}


void CDir::dump_to_disk(MDS *mds)
{
  map<string,CDentry*>::iterator iter = items.begin();
  while (iter != items.end()) {
	CDentry* d = iter->second;
	if (d->inode->dir != NULL) {
	  cout << "dump2disk: " << d->inode->inode.ino << " " << d->name << '/' << endl;
	  d->inode->dump_to_disk(mds);
	}
	iter++;
  }

  cout << "dump2disk: writing dir " << inode->inode.ino << endl;
  mds->mdstore->commit_dir(inode, NULL);
}
