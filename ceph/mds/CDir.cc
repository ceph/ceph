
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
#define dout(x)  if (x <= g_conf.debug) cout << "mds" << mds->get_nodeid() << "        cdir: "


ostream& operator<<(ostream& out, CDir& dir)
{
  string path;
  dir.get_inode()->make_path(path);
  out << "[dir " << dir.ino() << " " << path << "/";
  if (dir.is_dirty()) out << " dirty";
  if (dir.is_import()) out << " import";
  if (dir.is_export()) out << " export";
  if (dir.is_auth()) {
	out << " auth";
	if (dir.is_open_by_anyone())
	  out << "+" << dir.get_open_by();
  } else {
	out << " rep@" << dir.authority();
	if (dir.get_replica_nonce() > 1)
	  out << "." << dir.get_replica_nonce();
  }

  if (dir.is_pinned()) {
    out << " |";
    for(set<int>::iterator it = dir.get_ref_set().begin();
        it != dir.get_ref_set().end();
        it++)
      if (*it < CDIR_NUM_PINS)
        out << " " << cdir_pin_names[*it];
      else
        out << " " << *it;
  }

  if (dir.get_dir_auth() != CDIR_AUTH_PARENT)
	out << " dir_auth=" << dir.get_dir_auth();

  out << " state=" << dir.get_state();
  out << " sz=" << dir.get_nitems() << "+" << dir.get_nnull();
  out << " " << &dir;
  return out << "]";
}


// CDir

CDir::CDir(CInode *in, MDS *mds, bool auth)
{
  inode = in;
  this->mds = mds;
  
  nitems = 0;
  nnull = 0;
  state = CDIR_STATE_INITIAL;

  version = 0;
  committing_version = 0;
  last_committed_version = 0;

  ref = 0;

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





CDentry* CDir::lookup(const string& n) {
  //cout << " lookup " << n << " in " << this << endl;
  
  map<string,CDentry*>::iterator iter = items.find(n);
  if (iter != items.end()) return iter->second;

  return NULL;
}






CDentry* CDir::add_dentry( const string& dname, CInode *in ) 
{
  assert(lookup(dname) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, in);
  dn->dir = this;
  dn->parent_dir_version = version;
  
  // add to dir
  assert(items.count(dn->name) == 0);
  assert(null_items.count(dn->name) == 0);

  items[dn->name] = dn;
  
  if (in) {
	link_inode_work( dn, in );
  } else {
	assert(dn->inode == 0);
	null_items[dn->name] = dn;
	nnull++;
  }

  dout(10) << "add_dentry " << *dn << endl;

  // pin?
  if (nnull + nitems == 1) get(CDIR_PIN_CHILD);
  
  assert(nnull + nitems == items.size());
  assert(nnull == null_items.size());		 
  return dn;
}



void CDir::remove_dentry(CDentry *dn) 
{
  dout(10) << "remove_dentry " << *dn << endl;

  if (dn->inode) {
	// detach inode and dentry
	unlink_inode_work(dn);
  } else {
	// remove from null list
 	assert(null_items.count(dn->name) == 1);
	null_items.erase(dn->name);
	nnull--;
  }
  
  // remove from list
  assert(items.count(dn->name) == 1);
  items.erase(dn->name);

  delete dn;

  // unpin?
  if (nnull + nitems == 0) put(CDIR_PIN_CHILD);

  assert(nnull + nitems == items.size());
  assert(nnull == null_items.size());		 
}


void CDir::link_inode( CDentry *dn, CInode *in )
{
  link_inode_work(dn,in);
  dout(10) << "link_inode " << *dn << " " << *in << endl;
  
  // remove from null list
  assert(null_items.count(dn->name) == 1);
  null_items.erase(dn->name);
  nnull--;

  assert(nnull + nitems == items.size());
  assert(nnull == null_items.size());		 
}

void CDir::link_inode_work( CDentry *dn, CInode *in )
{
  dn->inode = in;
  in->add_parent(dn);

  nitems++;  // adjust dir size
  
  // set dir version
  in->parent_dir_version = get_version();
  
  // clear dangling
  in->state_clear(CINODE_STATE_DANGLING);

  // dn dirty?
  if (dn->is_dirty()) in->get(CINODE_PIN_DNDIRTY);

  // adjust auth pin count
  if (in->auth_pins + in->nested_auth_pins)
	adjust_nested_auth_pins( in->auth_pins + in->nested_auth_pins );
}

void CDir::unlink_inode( CDentry *dn )
{
  dout(10) << "unlink_inode " << *dn << " " << *dn->inode << endl;

  unlink_inode_work(dn);

  // add to null list
  assert(null_items.count(dn->name) == 0);
  null_items[dn->name] = dn;
  nnull++;

  assert(nnull + nitems == items.size());
  assert(nnull == null_items.size());		 
}

void CDir::unlink_inode_work( CDentry *dn )
{
  CInode *in = dn->inode;
  
  // explicitly define auth
  in->dangling_auth = in->authority();
  //dout(10) << "unlink_inode " << *in << " dangling_auth now " << in->dangling_auth << endl;
  
  // unlink auth_pin count
  if (in->auth_pins + in->nested_auth_pins)
	adjust_nested_auth_pins( 0 - (in->auth_pins + in->nested_auth_pins) );
  
  // set dangling flag
  in->state_set(CINODE_STATE_DANGLING);

  // dn dirty?
  if (dn->is_dirty()) in->put(CINODE_PIN_DNDIRTY);
  
  // detach inode
  in->remove_parent(dn);
  assert(in->nparents == 0);
  assert(in->parent == 0);
  // FIXME... might need to migrate inode to another dir?   XXX
  
  if (in->is_dirty() && in->nparents == 0) 
	in->mark_clean();
  
  dn->inode = 0;

  nitems--;   // adjust dir size
}

void CDir::remove_null_dentries() {
  dout(10) << "remove_null_dentries " << *this << endl;

  list<CDentry*> dns;
  for (CDir_map_t::iterator it = null_items.begin();
	   it != null_items.end(); 
	   it++) {
	dns.push_back(it->second);
  }

  for (list<CDentry*>::iterator it = dns.begin();
	   it != dns.end();
	   it++) {
	CDentry *dn = *it;
	assert(dn->is_sync());
	assert(dn->is_null());
	remove_dentry(dn);
  }
  assert(nnull == 0);
  assert(nnull + nitems == items.size());
  assert(nnull == null_items.size());		 
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

bool CDir::waiting_for(int tag)
{
  return waiting.count(tag) > 0;
}

bool CDir::waiting_for(int tag, const string& dn)
{
  if (!waiting_on_dentry.count(dn)) 
	return false;
  return waiting_on_dentry[dn].count(tag) > 0;
}

void CDir::add_waiter(int tag,
					  const string& dentry,
					  Context *c) {
  if (waiting.empty() && waiting_on_dentry.size() == 0)
	get(CDIR_PIN_WAITER);
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
  if (waiting.empty() && waiting_on_dentry.size() == 0)
	get(CDIR_PIN_WAITER);
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
  if (waiting_on_dentry.size() == 0 && waiting.empty())
	put(CDIR_PIN_WAITER);
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
		dout(10) << "take_waiting mask " << mask << " SKIPPING " << it->second << " tag " << it->first << " on " << *this<< endl;
		it++;
	  }
	}
	
	if (waiting_on_dentry.size() == 0 && waiting.empty())
	  put(CDIR_PIN_WAITER);
  }
}


void CDir::finish_waiting(int mask, int result) 
{
  dout(11) << "finish_waiting mask " << mask << " result " << result << " on " << *this << endl;

  list<Context*> finished;
  take_waiting(mask, finished);
  finish_contexts(finished, result);
}

void CDir::finish_waiting(int mask, const string& dn, int result) 
{
  dout(11) << "finish_waiting mask " << mask << " dn " << dn << " result " << result << " on " << *this << endl;

  list<Context*> finished;
  take_waiting(mask, dn, finished);
  finish_contexts(finished, result);
}


// dirty/clean

void CDir::mark_dirty()
{
  if (!state_test(CDIR_STATE_DIRTY)) {
	version++;
	state_set(CDIR_STATE_DIRTY);
	dout(10) << "mark_dirty (was clean) " << *this << " new version " << version << endl;
  } 
  else if (state_test(CDIR_STATE_COMMITTING) &&
		   committing_version == version) {
	version++;  // now dirtier than committing version!
	dout(10) << "mark_dirty (committing) " << *this << " new version " << version << "/" << committing_version <<  endl;
  } else {
	dout(10) << "mark_dirty (already dirty) " << *this << " version " << version << endl;
  }
}

void CDir::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << version << endl;
  state_clear(CDIR_STATE_DIRTY);
  //assert(nnull == 0);   // what about requests in progres... null dentries but dir isn't marked dirty (yet)
}



// ref counts

void CDir::put(int by) {
  // bad?
  if (ref == 0 || ref_set.count(by) != 1) {
	dout(7) << *this << " bad put by " << by << " " << cdir_pin_names[by] << " was " << ref << " (" << ref_set << ")" << endl;
	assert(ref_set.count(by) == 1);
	assert(ref > 0);
  }

  ref--;
  ref_set.erase(by);

  // inode
  if (ref == 0)
	inode->put(CINODE_PIN_DIR);

  dout(7) << *this << " put by " << by << " " << cdir_pin_names[by] << " now " << ref << " (" << ref_set << ")" << endl;
}

void CDir::get(int by) {
  // inode
  if (ref == 0)
	inode->get(CINODE_PIN_DIR);

  // bad?
  if (ref_set.count(by)) {
	dout(7) << *this << " bad get by " << by << " " << cdir_pin_names[by] << " was " << ref << " (" << ref_set << ")" << endl;
	assert(ref_set.count(by) == 0);
  }
  
  ref++;
  ref_set.insert(by);
  
  dout(7) << *this << " get by " << by << " " << cdir_pin_names[by] << " now " << ref << " (" << ref_set << ")" << endl;
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

  // root, or dangling
  assert(inode->is_root() || inode->is_dangling());  
  return inode->authority();
}

int CDir::dentry_authority(const string& dn )
{
  if (is_hashed()) {
	return mds->get_cluster()->hash_dentry( inode->ino(), dn );  // hashed
  }
  
  if (dir_auth == CDIR_AUTH_PARENT) {
	dout(15) << "dir_auth = parent at " << *this << endl;
	return inode->authority();       // same as my inode
  }

  // it's explicit for this whole dir
  dout(15) << "dir_auth explicit " << dir_auth << " at " << *this << endl;
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
    put(CDIR_PIN_AUTHPIN);

  dout(7) << "auth_unpin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;
  assert(auth_pins >= 0);
  
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
	assert(dir->nested_auth_pins >= 0);
	
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

  dout(10) << "dump:" << ind << *this << endl;

  map<string,CDentry*>::iterator iter = items.begin();
  while (iter != items.end()) {
	CDentry* d = iter->second;
	if (d->inode) {
	  char isdir = ' ';
	  if (d->inode->dir != NULL) isdir = '/';
	  dout(10) << "dump: " << ind << *d << " = " << *d->inode << endl;
	  d->inode->dump(depth+1);
	} else {
	  dout(10) << "dump: " << ind << *d << " = [null]" << endl;
	}
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
  mds->mdstore->commit_dir(inode->dir, NULL);
}
