
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


ostream& operator<<(ostream& out, CDir& dir)
{
  string path;
  dir.get_inode()->make_path(path);
  out << "[dir " << dir.ino() << " " << path << "/";
  if (dir.is_import()) out << " import";
  if (dir.is_export()) out << " export";
  if (dir.is_auth()) {
	out << " auth";
	if (dir.is_open_by_anyone())
	  out << "+" << dir.get_open_by();
  } else {
	out << " rep a=" << dir.authority() << " n=" << dir.get_replica_nonce();
  }
  return out << "]";
}


// CDir

CDir::CDir(CInode *in, MDS *mds, bool auth)
{
  inode = in;
  this->mds = mds;
  
  nitems = 0;
  nauthitems = 0;
  state = CDIR_STATE_INITIAL;
  version = 0;
  committing_version = 0;

  // auth
  assert(in->is_dir());
  if (auth) 
	state |= CDIR_STATE_AUTH;
  if (in->dir_is_hashed()) 
	state |= CDIR_STATE_HASHED;
  
  auth_pins = 0;
  nested_auth_pins = 0;
  
  dir_rep = CDIR_REP_NONE;
}


inodeno_t CDir::ino() {
  return inode->ino();
}


CDir *CDir::get_parent_dir()
{
  return inode->get_parent_dir();
}


void CDir::hit(int type) 
{
  assert(type >= 0 && type < MDS_NPOP);
  popularity[type].hit();
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
  if (d->inode->is_auth())
	nauthitems++;
  //namesize += d->name.length();
  
  if (nitems == 1)
	inode->get(CINODE_PIN_CHILD);       // pin parent
}

void CDir::remove_child(CDentry *d) {
  map<string, CDentry*>::iterator iter = items.find(d->name);
  items.erase(iter);

  nitems--;
  if (d->inode->is_auth())
	nauthitems--;
  //namesize -= d->name.length();

  if (nitems == 0)
	inode->put(CINODE_PIN_CHILD);       // release parent.
}


CDentry* CDir::lookup(const string& n) {
  //cout << " lookup " << n << " in " << this << endl;
  map<string,CDentry*>::iterator iter = items.find(n);
  if (iter == items.end()) return NULL;

  //cout << "  lookup got " << iter->second << endl;
  return iter->second;
}









// state encoding

/*

what DIR state is encoded when

- dir open / discover
 nonce
 dir_auth
 dir_rep/by

- dir update
 dir_rep/by

- export
 dir_rep/by
 nitems
 version
 state
 popularity


*/

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
					  const string& dentry,
					  Context *c) {
  if (waiting_on_dentry.size() == 0)
	inode->get(CINODE_PIN_DIRWAITDN);
  waiting_on_dentry[ dentry ].insert(pair<int,Context*>(tag,c));
  dout(10) << "add_waiter dentry " << dentry << " tag " << tag << " " << c << " on " << *this << endl;
}

void CDir::add_waiter(int tag, Context *c) {
  // hierarchical?
  if (tag & CDIR_WAIT_ATFREEZEROOT && (is_freezing() || is_frozen())) {  
	if (is_freezing_tree_root() || is_frozen_tree_root()) {
	  // it's us, pin here.  (fall thru)
	} else {
	  // pin parent!
	  dout(10) << "add_waiter " << tag << " " << c << " should be ATFREEZEROOT, " << *this << " is not root, trying parent" << endl;
	  inode->parent->dir->add_waiter(tag, c);
	  return;
	}
  }

  // this dir.
  if (waiting.empty())
	inode->get(CINODE_PIN_DIRWAIT);
  waiting.insert(pair<int,Context*>(tag,c));
  dout(10) << "add_waiter " << tag << " " << c << " on " << *this << endl;
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
	  dout(10) << "take_waiting dentry " << dentry << " mask " << mask << " took " << it->second << " tag " << it->first << " on " << *this << endl;
	  waiting_on_dentry[dentry].erase(it++);
	} else {
	  dout(10) << "take_waiting dentry " << dentry << " mask " << mask << " SKIPPING " << it->second << " tag " << it->first << " on " << *this << endl;
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
	  take_waiting(mask, (it++)->first, ls);   // not post-inc
	}
  }
  
  // waiting
  if (!waiting.empty()) {
	multimap<int,Context*>::iterator it = waiting.begin();
	while (it != waiting.end()) {
	  if (it->first & mask) {
		ls.push_back(it->second);
		dout(10) << "take_waiting mask " << mask << " took " << it->second << " tag " << it->first << " on " << *this << endl;
		waiting.erase(it++);
	  } else {
		dout(10) << "take_waiting mask " << mask << " SKIPPING " << it->second << " tag " << it->first << " on" << *this<< endl;
		it++;
	  }
	}
	
	if (waiting.empty())
	  inode->put(CINODE_PIN_DIRWAIT);
  }
}


void CDir::finish_waiting(int mask, int result) 
{
  dout(11) << "finish_waiting mask " << mask << " result " << result << " on " << *this << endl;

  list<Context*> finished;
  take_waiting(mask, finished);
  for (list<Context*>::iterator it = finished.begin();
	   it != finished.end();
	   it++) {
	Context *c = *it;
	dout(11) << "finishing " << c << endl;
	c->finish(result);
	delete c;
  }
}


// dirty/clean

void CDir::mark_dirty()
{
  if (!state_test(CDIR_STATE_DIRTY)) {
	version++;
	state_set(CDIR_STATE_DIRTY);
	dout(10) << "mark_dirty " << *this << " new version " << version << endl;
  } 
  else if (state_test(CDIR_STATE_COMMITTING) &&
		   committing_version == version) {
	version++;  // now dirtier than committing version!
	dout(10) << "mark_dirty (committing) " << *this << " new version " << version << "/" << committing_version <<  endl;
  } else {
	dout(10) << "mark_dirty (dirty) " << *this << " version " << version << endl;
  }
}

void CDir::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << version << endl;
  state_clear(CDIR_STATE_DIRTY);
}



// ref counts

void CDir::get(int by) {
  // bad?
  if (ref == 0 || ref_set.count(by) != 1) {
	dout(7) << *this << " bad put by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	assert(ref_set.count(by) == 1);
	assert(ref > 0);
  }

  ref--;
  ref_set.erase(by);

  // inode
  if (ref == 0)
	inode->put(CINODE_PIN_DIR);

  dout(7) << *this << " put by " << by << " now " << ref << " (" << ref_set << ")" << endl;
}

void CDir::get(int by) {
  // inode
  if (ref == 0)
	inode->get(CINODE_PIN_DIR);

  // bad?
  if (ref_set.count(by)) {
	dout(7) << *this << " bad get by " << by << " was " << ref << " (" << ref_set << ")" << endl;
	assert(ref_set.count(by) == 0);
  }
  
  ref++;
  ref_set.insert(by);
  
  dout(7) << *this " get by " << by << " now " << ref << " (" << ref_set << ")" << endl;
}



// authority

/*
void CDir::update_auth(int whoami) {
  if (inode->dir_is_auth(whoami) && !is_auth())
	state_set(CDIR_STATE_AUTH);
  if (!inode->dir_is_auth(whoami) && is_auth())
	state_clear(CDIR_STATE_AUTH);
}
*/

int CDir::authority() 
{
  if (dir_auth >= 0)
	return dir_auth;

  CDir *parent = inode->get_parent_dir();
  if (parent)
	return parent->authority();

  // root on 0.
  assert(inode->is_root());
  return 0;
}

int CDir::dentry_authority(const string& dn )
{
  if (is_hashed()) {
	return mds->get_cluster()->hash_dentry( inode->ino(), dn );  // hashed
  }
  
  if (dir_auth == CDIR_AUTH_PARENT) {
	dout(11) << "dir_auth = parent at " << *this << endl;
	return inode->authority();       // same as my inode
  }

  // it's explicit for this whole dir
  dout(11) << "dir_auth explicit " << dir_auth << " at " << *this << endl;
  return dir_auth;
}



// auth pins

void CDir::auth_pin() {
  if (auth_pins == 0)
    get(CDIR_PIN_AUTHPIN);
  auth_pins++;

  dout(7) << "auth_pin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;
  
  inode->nested_auth_pins++;
  if (inode->parent)
	inode->parent->dir->adjust_nested_auth_pins( 1 );
}

void CDir::auth_unpin() {
  auth_pins--;
  if (auth_pins == 0)
    put(CINODE_PIN_DAUTHPIN);
  assert(auth_pins >= 0);

  dout(7) << "auth_unpin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;
  
  // pending freeze?
  if (auth_pins + nested_auth_pins == 0)
	on_freezeable();
  
  inode->nested_auth_pins--;
  if (inode->parent)
	inode->parent->dir->adjust_nested_auth_pins( -1 );
}

void CDir::adjust_nested_auth_pins(int inc) 
{
  CDir *dir = this;
  
  while (1) {
	// dir
	dir->nested_auth_pins += inc;
	
	dout(10) << "adjust_nested_auth_pins on " << *dir << " count now " << dir->auth_pins << " + " << dir->nested_auth_pins << endl;
	
	// pending freeze?
	if (dir->auth_pins + dir->nested_auth_pins == 0) 
	  dir->on_freezeable();
	
	// it's inode
	dir->inode->nested_auth_pins += inc;
	
	if (dir->inode->parent)
	  dir = dir->inode->parent->dir;
	else
	  break;
  }
}

void CDir::on_freezeable()
{
  // check for anything pending freezeable

  /* NOTE: the first of these will likely freeze the dir, and unmark
	 FREEZING.  additional ones will re-flag FREEZING.  this isn't
	 particularly graceful, and might cause problems if the first one
	 needs to know about other waiters.... FIXME? */
  
  finish_waiting(CDIR_WAIT_FREEZEABLE);
}


// FREEZE

class C_MDS_FreezeTree : public Context {
  CDir *dir;
  Context *con;
public:
  C_MDS_FreezeTree(CDir *dir, Context *c) {
	this->dir = dir;
	this->con = c;
  }
  virtual void finish(int r) {
	dir->freeze_tree_finish(con);
  }
};

void CDir::freeze_tree(Context *c)
{
  assert(!is_frozen());
  assert(!is_freezing());
  
  if (is_freezeable()) {
	dout(10) << "freeze_tree " << *this << endl;
	
	state_set(CDIR_STATE_FROZENTREE);
	inode->auth_pin();  // auth_pin for duration of freeze
	
	// easy, we're frozen
	c->finish(0);
	delete c;
	
  } else {
	state_set(CDIR_STATE_FREEZINGTREE);
	dout(10) << "freeze_tree + wait " << *this << endl;
	
	// need to wait for auth pins to expire
	add_waiter(CDIR_WAIT_FREEZEABLE, new C_MDS_FreezeTree(this, c));
  } 
}

void CDir::freeze_tree_finish(Context *c)
{
  // freezeable now?
  if (!is_freezeable()) {
	// wait again!
	dout(10) << "freeze_tree_finish still waiting " << *this << endl;
	state_set(CDIR_STATE_FREEZINGTREE);
	add_waiter(CDIR_WAIT_FREEZEABLE, new C_MDS_FreezeTree(this, c));
	return;
  }

  dout(10) << "freeze_tree_finish " << *this << endl;
  state_set(CDIR_STATE_FROZENTREE);
  state_clear(CDIR_STATE_FREEZINGTREE);   // actually, this may get set again by next context?

  inode->auth_pin();  // auth_pin for duration of freeze
  
  // continue to frozen land
  if (c) {
	c->finish(0);
	delete c;
  }
}

void CDir::unfreeze_tree()
{
  dout(10) << "unfreeze_tree " << *this << endl;
  state_clear(CDIR_STATE_FROZENTREE);
  
  // unpin  (may => FREEZEABLE)   FIXME: is this order good?
  inode->auth_unpin();

  // waiters?
  finish_waiting(CDIR_WAIT_UNFREEZE);
}

bool CDir::is_freezing_tree()
{
  CDir *dir = this;
  while (1) {
	if (dir->is_freezing_tree_root()) return true;
	if (dir->is_import()) return false;
	if (dir->inode->parent)
	  dir = dir->inode->parent->dir;
	else
	  return false; // root on replica
  }
}

bool CDir::is_frozen_tree()
{
  CDir *dir = this;
  while (1) {
	if (dir->is_frozen_tree_root()) return true;
	if (dir->is_import()) return false;
	if (dir->inode->parent)
	  dir = dir->inode->parent->dir;
	else
	  return false;  // root on replica
  }
}



// FREEZE DIR

class C_MDS_FreezeDir : public Context {
  CDir *dir;
  Context *con;
public:
  C_MDS_FreezeDir(CDir *dir, Context *c) {
	this->dir = dir;
	this->con = c;
  }
  virtual void finish(int r) {
	dir->freeze_dir_finish(con);
  }
};

void CDir::freeze_dir(Context *c)
{
  assert(!is_frozen());
  assert(!is_freezing());
  
  if (is_freezeable_dir()) {
	dout(10) << "freeze_dir " << *this << endl;
	
	state_set(CDIR_STATE_FROZENDIR);
	inode->auth_pin();  // auth_pin for duration of freeze
	
	// easy, we're frozen
	c->finish(0);
	delete c;
	
  } else {
	state_set(CDIR_STATE_FREEZINGDIR);
	dout(10) << "freeze_dir + wait " << *this << endl;
	
	// need to wait for auth pins to expire
	add_waiter(CDIR_WAIT_FREEZEABLE, new C_MDS_FreezeDir(this, c));
  } 
}

void CDir::freeze_dir_finish(Context *c)
{
  // freezeable now?
  if (!is_freezeable_dir()) {
	// wait again!
	dout(10) << "freeze_dir_finish still waiting " << *this << endl;
	state_set(CDIR_STATE_FREEZINGDIR);
	add_waiter(CDIR_WAIT_FREEZEABLE, new C_MDS_FreezeDir(this, c));
	return;
  }

  dout(10) << "freeze_dir_finish " << *this << endl;
  state_set(CDIR_STATE_FROZENDIR);
  state_clear(CDIR_STATE_FREEZINGDIR);   // actually, this may get set again by next context?
  
  inode->auth_pin();  // auth_pin for duration of freeze
  
  // continue to frozen land
  if (c) {
	c->finish(0);
	delete c;
  }
}

void CDir::unfreeze_dir()
{
  dout(10) << "unfreeze_dir " << *this << endl;
  state_clear(CDIR_STATE_FROZENDIR);
  
  // unpin  (may => FREEZEABLE)   FIXME: is this order good?
  inode->auth_unpin();

  // waiters?
  finish_waiting(CDIR_WAIT_UNFREEZE);
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
