
#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"
#include "MDStore.h"
#include "MDS.h"
#include "MDCluster.h"

#include "include/Context.h"

#include <cassert>

#include "include/config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "cdir:"


// CDir

void CDir::hit() 
{
  popularity.hit();

  if (0) {
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
}

int CDir::get_rep_count(MDCluster *mdc) 
{
  if (dir_rep == CDIR_REP_NONE) 
	return 1;
  if (dir_rep == CDIR_REP_LIST) 
	return 1 + dir_rep_by.size();
  if (dir_rep == CDIR_REP_ALL) 
	return mdc->get_num_mds();
  assert(2+2==5);
}

void CDir::add_child(CDentry *d) 
{
  assert(nitems == items.size());
  assert(items.count(d->name) == 0);

  //cout << "adding " << d->name << " to " << this << endl;
  items[d->name] = d;
  d->dir = this;
  
  nitems++;
  namesize += d->name.length();
  
  if (nitems == 1)
	inode->get(CINODE_PIN_CHILD);       // pin parent
}

void CDir::remove_child(CDentry *d) {
  map<string, CDentry*>::iterator iter = items.find(d->name);
  items.erase(iter);

  nitems--;
  namesize -= d->name.length();

  if (nitems == 0)
	inode->put(CINODE_PIN_CHILD);       // release parent.
}


CDentry* CDir::lookup(string& n) {
  //cout << " lookup " << n << " in " << this << endl;
  map<string,CDentry*>::iterator iter = items.find(n);
  if (iter == items.end()) return NULL;

  //cout << "  lookup got " << iter->second << endl;
  return iter->second;
}


int CDir::dentry_authority(string& dn, MDCluster *mdc)
{
  if (inode->dir_auth == CDIR_AUTH_PARENT) {
	dout(11) << "dir_auth parent at " << *inode << endl;
	return inode->authority( mdc );       // same as my inode
  }
  if (inode->dir_auth == CDIR_AUTH_HASH) {
	assert(0);
	return mdc->hash_dentry( this, dn );  // hashed
  }

  // it's explicit for this whole dir
  dout(11) << "dir_auth explicit " << inode->dir_auth << " at " << *inode << endl;
  return inode->dir_auth;
}



// state



crope CDir::encode_basic_state()
{
  crope r;

  // dir rep
  r.append((char*)&dir_rep, sizeof(int));
  
  // dir_rep_by
  int n = dir_rep_by.size();
  r.append((char*)&n, sizeof(int));
  for (set<int>::iterator it = dir_rep_by.begin(); 
	   it != dir_rep_by.end();
	   it++) {
	int j = *it;
	r.append((char*)&j, sizeof(j));
  }
  
  return r;
}
 
int CDir::decode_basic_state(crope r, int off)
{
  // dir_rep
  r.copy(off, sizeof(int), (char*)&dir_rep);
  off += sizeof(int);

  // dir_rep_by
  int n;
  r.copy(off, sizeof(int), (char*)&n);
  off += sizeof(int);
  for (int i=0; i<n; i++) {
	int j;
	r.copy(off, sizeof(int), (char*)&j);
	dir_rep_by.insert(j);
	off += sizeof(int);
  }

  return off;
}



// wiating

void CDir::add_waiter(int tag,
					  string& dentry,
					  Context *c) {
  if (waiting_on_dentry.size() == 0)
	inode->get(CINODE_PIN_DIRWAITDN);
  waiting_on_dentry[ dentry ].insert(pair<int,Context*>(tag,c));
  dout(10) << "add_waiter dentry " << dentry << " tag " << tag << " " << c << " on " << *inode << endl;
}

void CDir::add_waiter(int tag, Context *c) {
  // hierarchical?
  if (tag & CDIR_WAIT_ATFREEZEROOT && (is_freezing() || is_frozen())) {  
	if (state_test(CDIR_STATE_FROZEN|CDIR_STATE_FREEZING)) {
	  // it's us, pin here.  (fall thru)
	} else {
	  // pin parent!
	  inode->parent->dir->add_waiter(tag, c);
	  return;
	}
  }

  // this dir.
  if (waiting.empty())
	inode->get(CINODE_PIN_DIRWAIT);
  waiting.insert(pair<int,Context*>(tag,c));
  dout(10) << "add_waiter " << tag << " " << c << " on dir " << *inode << endl;
}


void CDir::take_waiting(int mask, 
						const string& dentry,
						list<Context*>& ls)
{
  if (waiting_on_dentry.empty()) return;
  
  multimap<int,Context*>::iterator it = waiting_on_dentry[dentry].begin();
  while (it != waiting_on_dentry[dentry].end()) {
	if (it->first & mask) {
	  ls.push_back(it->second);
	  dout(10) << "take_waiting dentry " << dentry << " mask " << mask << " took " << it->second << " tag " << it->first << " on dir " << *inode << endl;
	  waiting_on_dentry[dentry].erase(it++);
	} else {
	  dout(10) << "take_waiting dentry " << dentry << " SKIPPING mask " << mask << " took " << it->second << " tag " << it->first << " on dir " << *inode << endl;
	  it++;
	}
  }

  // did we clear dentry?
  if (waiting_on_dentry[dentry].empty())
	waiting_on_dentry.erase(dentry);
  
  // ...whole map?
  if (waiting_on_dentry.size() == 0) 
	inode->put(CINODE_PIN_DIRWAITDN);
}

/* NOTE: this checks dentry waiters too */
void CDir::take_waiting(int mask,
						list<Context*>& ls)
{
  if (waiting_on_dentry.size()) {
	// try each dentry
	hash_map<string, multimap<int,Context*> >::iterator it = 
	  it = waiting_on_dentry.begin(); 
	while (it != waiting_on_dentry.end()) {
	  take_waiting(mask, it->first, ls);
	}
  }
  
  // waiting
  if (!waiting.empty()) {
	multimap<int,Context*>::iterator it = waiting.begin();
	while (it != waiting.end()) {
	  if (it->first & mask) {
		ls.push_back(it->second);
		dout(10) << "take_waiting mask " << mask << " took " << it->second << " tag " << it->first << " on dir " << *inode << endl;
		waiting.erase(it++);
	  } else {
		dout(10) << "take_waiting mask " << mask << " SKIPPING " << it->second << " tag " << it->first << " on dir " << *inode<< endl;
		it++;
	  }
	}
	
	if (waiting.empty())
	  inode->put(CINODE_PIN_DIRWAIT);
  }
}


// locking and freezing


void CDir::auth_pin() {
  inode->get(CINODE_PIN_DAUTHPIN + auth_pins);
  auth_pins++;
  dout(7) << "auth_unpin on " << *inode << " count now " << auth_pins << endl;
  inode->adjust_nested_auth_pins( 1 );
}

void CDir::auth_unpin() {
  auth_pins--;
  inode->put(CINODE_PIN_DAUTHPIN + auth_pins);
  assert(auth_pins >= 0);
  dout(7) << "auth_unpin on " << *inode << " count now " << auth_pins << endl;

  // pending freeze?
  if (auth_pins + nested_auth_pins == 0) {
	list<Context*> waiting_to_freeze;
	take_waiting(CDIR_WAIT_FREEZEABLE, waiting_to_freeze);
	if (waiting_to_freeze.size())
	  freeze_finish(waiting_to_freeze);
  }
  
  inode->adjust_nested_auth_pins( -1 );
}

int CDir::adjust_nested_auth_pins(int a) {
  nested_auth_pins += a;

  // pending freeze?
  if (auth_pins + nested_auth_pins == 0) {
	list<Context*> waiting_to_freeze;
	take_waiting(CDIR_WAIT_FREEZEABLE, waiting_to_freeze);
	if (waiting_to_freeze.size())
	  freeze_finish(waiting_to_freeze);
  }
  
  inode->adjust_nested_auth_pins(a);
}



bool CDir::is_frozen() 
{
  if (is_freeze_root())
	return true;
  if (inode->parent)
	return inode->parent->dir->is_freeze_root();
  return false;
}

bool CDir::is_freezing() 
{
  if (state_test(CDIR_STATE_FREEZING))
	return true;
  if (inode->parent)
	return inode->parent->dir->is_freezing();
  return false;
}

/*
void CDir::add_freeze_waiter(Context *c)
{
  // wait on freeze root
  CDir *t = this;
  while (!t->is_freeze_root()) {
	t = t->inode->parent->dir;
  }
  t->add_waiter(CDIR_WAIT_UNFREEZE, c);
}
*/

void CDir::freeze(Context *c)
{
  assert((state_test(CDIR_STATE_FROZEN|CDIR_STATE_FREEZING)) == 0);

  if (auth_pins + nested_auth_pins == 0) {
	dout(10) << "freeze " << *inode << endl;

	state_set(CDIR_STATE_FROZEN);
	inode->auth_pin();  // auth_pin for duration of freeze
  
	// easy, we're frozen
	c->finish(0);
	delete c;

  } else {
	state_set(CDIR_STATE_FREEZING);
	dout(10) << "freeze + wait " << *inode << endl;

	// need to wait for auth pins to expire
	add_waiter(CDIR_WAIT_FREEZEABLE, c);
  }
}

void CDir::freeze_finish(list<Context*>& waiters)
{
  dout(10) << "freeze_finish " << *inode << endl;

  inode->auth_pin();  // auth_pin for duration of freeze

  // froze!
  Context *c = waiters.front();
  waiters.pop_front();
  state_set(CDIR_STATE_FROZEN);

  // others trying?
  if (waiters.empty()) {
	state_clear(CDIR_STATE_FREEZING);  // that was the only freezer!
  } else {
	// make the others wait again
	while (!waiters.empty()) {
	  add_waiter(CDIR_WAIT_FREEZEABLE, waiters.front());
	  waiters.pop_front();
	}
  }

  // continue to frozen land
  if (c) {
	c->finish(0);
	delete c;
  }
}

void CDir::unfreeze()  // thaw?
{
  dout(10) << "unfreeze " << *inode << endl;
  state_clear(CDIR_STATE_FROZEN);

  // unpin  (may => FREEZEABLE)   FIXME: is this order good?
  inode->auth_unpin();
  
  // waiters
  list<Context*> finished;
  take_waiting(CDIR_WAIT_UNFREEZE, finished);

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
	dout(10) << ind << d->inode->inode.ino << " " << d->name << isdir << endl;
	d->inode->dump(depth+1);
	iter++;
  }

  if (!(state_test(CDIR_STATE_COMPLETE)))
	dout(10) << ind << "..." << endl;
  if (state_test(CDIR_STATE_DIRTY))
	dout(10) << ind << "[dirty]" << endl;

}


void CDir::dump_to_disk(MDS *mds)
{
  map<string,CDentry*>::iterator iter = items.begin();
  while (iter != items.end()) {
	CDentry* d = iter->second;
	if (d->inode->dir != NULL) {
	  dout(10) << "dump2disk: " << d->inode->inode.ino << " " << d->name << '/' << endl;
	  d->inode->dump_to_disk(mds);
	}
	iter++;
  }

  dout(10) << "dump2disk: writing dir " << inode->inode.ino << endl;
  mds->mdstore->commit_dir(inode, NULL);
}
