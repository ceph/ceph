
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"


#include <string>

#include "include/config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "cinode:"



ostream& operator<<(ostream& out, CInode& in)
{
  string path;
  in.make_path(path);
  out << "[inode " << in.inode.ino << " ~" << path << " ";
  if (in.is_auth()) {
	out << "auth";
	if (in.is_cached_by_anyone()) {
	  //out << "+" << in.get_cached_by();
	  for (set<int>::iterator it = in.cached_by_begin();
		   it != in.cached_by_end();
		   it++) {
		out << "+" << *it << "." << in.get_cached_by_nonce(*it);
	  }
	}
  } else {
	out << "rep@" << in.authority();
	//if (in.get_replica_nonce() > 1)
	  out << "." << in.get_replica_nonce();
	assert(in.get_replica_nonce() >= 0);
  }

  if (in.is_symlink()) out << " symlink";

  out << " hard=" << in.hardlock;
  out << " soft=" << in.softlock;

  if (in.is_pinned()) {
    out << " |";
    for(set<int>::iterator it = in.get_ref_set().begin();
        it != in.get_ref_set().end();
        it++)
      if (*it < CINODE_NUM_PINS)
        out << " " << cinode_pin_names[*it];
      else
        out << " " << *it;
  }
  out << "]";
  return out;
}


// ====== CInode =======
CInode::CInode(bool auth) : LRUObject(),
							hardlock(LOCK_TYPE_BASIC),
							softlock(LOCK_TYPE_ASYNC) {
  ref = 0;
  
  parent = NULL;
  nparents = 0;
  lru_next = lru_prev = NULL;
  
  dir = NULL;     // CDir opened separately

  auth_pins = 0;
  nested_auth_pins = 0;

  state = 0;
  dist_state = 0;
  
  version = 0;

  if (auth) state_set(CINODE_STATE_AUTH);
}

CInode::~CInode() {
  if (dir) { delete dir; dir = 0; }
}

CDir *CInode::get_parent_dir()
{
  if (parent)
	return parent->dir;
  return NULL;
}
CInode *CInode::get_parent_inode() 
{
  if (parent) 
	return parent->dir->inode;
  return NULL;
}

bool CInode::dir_is_auth() {
  if (dir)
	return dir->is_auth();
  else
	return is_auth();
}

CDir *CInode::get_or_open_dir(MDS *mds)
{
  assert(is_dir());

  if (dir) return dir;

  // only auth can open dir alone.
  assert(is_auth());
  set_dir( new CDir(this, mds, true) );
  dir->dir_auth = -1;
  return dir;
}

CDir *CInode::set_dir(CDir *newdir)
{
  assert(dir == 0);
  dir = newdir;
  return dir;
}

void CInode::set_auth(bool a) 
{
  if (!is_dangling() && !is_root() && 
	  is_auth() != a) {
	CDir *dir = get_parent_dir();
	if (is_auth() && !a) 
	  dir->nauthitems--;
	else
	  dir->nauthitems++;
  }
  
  if (a) state_set(CINODE_STATE_AUTH);
  else state_clear(CINODE_STATE_AUTH);
}



void CInode::make_path(string& s)
{
  if (parent) {
	parent->dir->inode->make_path(s);
	s += "/";
	s += parent->name;
  } 
  else if (is_root()) {
	s = "";  // root
  } 
  else {
	s = "(dangling)";  // dangling
  }
}



void CInode::hit(int type)
{
  assert(type >= 0 && type < MDS_NPOP);
  popularity[type].hit();

  // hit my containing directory, too
  //if (parent) parent->dir->hit();
}


void CInode::mark_dirty() {
  
  dout(10) << "mark_dirty " << *this << endl;

  /*
	NOTE: I may already be dirty, but this fn _still_ needs to be called so that
	the directory is (perhaps newly) dirtied, and so that parent_dir_version is 
	updated below.
  */
  
  // only auth can get dirty.  "dirty" async data in replicas is relative to (say) softlock state, not dirty flag.
  assert(is_auth());

  // touch my private version
  version++;
  if (!(state & CINODE_STATE_DIRTY)) {
	state |= CINODE_STATE_DIRTY;
	get(CINODE_PIN_DIRTY);
  }
  
  // relative to parent dir:
  if (parent) {
	// dir is now dirty (if it wasn't already)
	parent->dir->mark_dirty();
	
	// i now live in that (potentially newly dirty) version
	parent_dir_version = parent->dir->get_version();
  }
}


// state 

/*

let's talk about what INODE state is encoded when.

when:
- after sync, lock
   .inode
- inode updates 
   .inode
/   cached_by?
- discover
   .inode
/   cached_by?
   nonce
   sync/etc. state
- export
   .inode
   version
   pop
   cached_by + cached_by_nonce
   sync/etc state
   dirty
   


*/

/*
crope CInode::encode_export_state()
{
  crope r;
  Inode_Export_State_t istate;

  istate.inode = inode;
  istate.version = version;
  istate.popularity = popularity[0]; // FIXME all pop values?
  //istate.ref = in->ref;
  istate.ncached_by = cached_by.size();
  
  istate.is_softasync = is_softasync();
  assert(!is_syncbyme());
  assert(!is_lockbyme());
  
  if (is_dirty())
	istate.dirty = true;
  else istate.dirty = false;

  //if (is_dir()) 
  //istate.dir_auth = dir_auth;
  //else
  //	istate.dir_auth = -1;

  // append to rope
  r.append( (char*)&istate, sizeof(istate) );
  
  // cached_by
  for (set<int>::iterator it = cached_by.begin();
	   it != cached_by.end();
	   it++) {
    // mds
	int i = *it;
	r.append( (char*)&i, sizeof(int) );
    // nonce
    int j = get_cached_by_nonce(i);
    r.append( (char*)&j, sizeof(int) );
  }

  return r;
}
*/


// new state encoders

void CInode::encode_soft_state(crope& r) 
{
  r.append((char*)&inode.size, sizeof(inode.size));
  r.append((char*)&inode.mtime, sizeof(inode.mtime));
  r.append((char*)&inode.atime, sizeof(inode.atime));  // ??
}

void CInode::decode_soft_state(crope& r, int& off)
{
  r.copy(off, sizeof(inode.size), (char*)&inode.size);
  off += sizeof(inode.size);
  r.copy(off, sizeof(inode.mtime), (char*)&inode.mtime);
  off += sizeof(inode.mtime);
  r.copy(off, sizeof(inode.atime), (char*)&inode.atime);
  off += sizeof(inode.atime);
}

void CInode::decode_merge_soft_state(crope& r, int& off)
{
  __uint64_t size;
  r.copy(off, sizeof(size), (char*)&size);
  off += sizeof(size);
  if (size > inode.size) inode.size = size;

  time_t t;
  r.copy(off, sizeof(t), (char*)&t);
  off += sizeof(t);
  if (t > inode.mtime) inode.mtime = t;

  r.copy(off, sizeof(t), (char*)&t);
  off += sizeof(t);
  if (t > inode.atime) inode.atime = t;
}

void CInode::encode_hard_state(crope& r)
{
  r.append((char*)&inode.mode, sizeof(inode.mode));
  r.append((char*)&inode.uid, sizeof(inode.uid));
  r.append((char*)&inode.gid, sizeof(inode.gid));
  r.append((char*)&inode.ctime, sizeof(inode.ctime));
}

void CInode::decode_hard_state(crope& r, int& off)
{
  r.copy(off, sizeof(inode.mode), (char*)&inode.mode);
  off += sizeof(inode.mode);
  r.copy(off, sizeof(inode.uid), (char*)&inode.uid);
  off += sizeof(inode.uid);
  r.copy(off, sizeof(inode.gid), (char*)&inode.gid);
  off += sizeof(inode.gid);
  r.copy(off, sizeof(inode.ctime), (char*)&inode.ctime);
  off += sizeof(inode.ctime);
}


// old state encoders

crope CInode::encode_basic_state()
{
  crope r;

  // inode
  r.append((char*)&inode, sizeof(inode));
  
  // cached_by
  int n = cached_by.size();
  r.append((char*)&n, sizeof(int));
  for (set<int>::iterator it = cached_by.begin(); 
	   it != cached_by.end();
	   it++) {
	int mds = *it;
	r.append((char*)&mds, sizeof(mds));
    int nonce = get_cached_by_nonce(mds);
    r.append((char*)&nonce, sizeof(nonce));
  }

  return r;
}
 
int CInode::decode_basic_state(crope r, int off)
{
  // inode
  r.copy(0,sizeof(inode_t), (char*)&inode);
  off += sizeof(inode_t);
	
  // cached_by --- although really this is rep_by,
  //               since we're non-authoritative  (?????)
  int n;
  r.copy(off, sizeof(int), (char*)&n);
  off += sizeof(int);
  cached_by.clear();
  for (int i=0; i<n; i++) {
    // mds
	int mds;
	r.copy(off, sizeof(int), (char*)&mds);
	off += sizeof(int);
    int nonce;
    r.copy(off, sizeof(int), (char*)&nonce);
    off += sizeof(int);
    cached_by_add(mds, nonce);
  }

  return off;
}



// waiting

bool CInode::is_frozen()
{
  if (parent && parent->dir->is_frozen())
	return true;
  return false;
}

bool CInode::is_freezing()
{
  if (parent && parent->dir->is_freezing())
	return true;
  return false;
}

bool CInode::waiting_for(int tag) 
{
  return waiting.count(tag) > 0;
}

void CInode::add_waiter(int tag, Context *c) {
  // waiting on hierarchy?
  if (tag & CDIR_WAIT_ATFREEZEROOT && (is_freezing() || is_frozen())) {  
	parent->dir->add_waiter(tag, c);
	return;
  }
  
  // this inode.
  if (waiting.size() == 0)
	get(CINODE_PIN_WAITER);
  waiting.insert(pair<int,Context*>(tag,c));
  dout(10) << "add_waiter " << tag << " " << c << " on " << *this << endl;
  
}

void CInode::take_waiting(int mask, list<Context*>& ls)
{
  if (waiting.empty()) return;
  
  multimap<int,Context*>::iterator it = waiting.begin();
  while (it != waiting.end()) {
	if (it->first & mask) {
	  ls.push_back(it->second);
	  dout(10) << "take_waiting mask " << mask << " took " << it->second << " tag " << it->first << " on " << *this << endl;

	  waiting.erase(it++);
	} else {
	  dout(10) << "take_waiting mask " << mask << " SKIPPING " << it->second << " tag " << it->first << " on " << *this << endl;
	  it++;
	}
  }

  if (waiting.empty())
	put(CINODE_PIN_WAITER);
}

void CInode::finish_waiting(int mask, int result) 
{
  dout(11) << "finish_waiting mask " << mask << " result " << result << " on " << *this << endl;
  
  list<Context*> finished;
  take_waiting(mask, finished);
  finish_contexts(finished);
}


// auth_pins
bool CInode::can_auth_pin() {
  if (parent)
	return parent->dir->can_auth_pin();
  return true;
}

void CInode::auth_pin() {
  if (auth_pins == 0)
    get(CINODE_PIN_AUTHPIN);
  auth_pins++;

  dout(7) << "auth_pin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;

  if (parent)
	parent->dir->adjust_nested_auth_pins( 1 );
}

void CInode::auth_unpin() {
  auth_pins--;
  if (auth_pins == 0)
    put(CINODE_PIN_AUTHPIN);

  dout(7) << "auth_unpin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;

  assert(auth_pins >= 0);

  if (parent)
	parent->dir->adjust_nested_auth_pins( -1 );
}



// authority

int CInode::authority() {
  if (is_dangling()) 
	return dangling_auth;   // explicit
  if (is_root())
	return 0;  // i am root
  assert(parent);
  return parent->dir->dentry_authority( parent->name );
}


/*
int CInode::dir_authority(MDCluster *mdc) 
{
  // explicit
  if (dir_auth >= 0) {
	dout(11) << "dir_auth explicit " << dir_auth << " at " << *this << endl;
	return dir_auth;
  }

  // parent
  if (dir_auth == CDIR_AUTH_PARENT) {
	dout(11) << "dir_auth parent at " << *this << endl;
	return authority(mdc);
  }
}
*/



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

void CInode::remove_parent(CDentry *p) {
  nparents--;
  if (nparents == 0) {         // first
	assert(parent == p);
	parent = 0;
  }
  else if (nparents == 1) {  // second, switch back from the vector
	parent = parents.front();
	if (parent == p)
	  parent = parents.back();
	assert(parent != p);
	parents.clear();
  } else {
	assert(0); // implement me
  }
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


