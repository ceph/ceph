
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include "AnchorTable.h"

#include <string>

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug) cout << "cinode: "


int cinode_pins[CINODE_NUM_PINS];  // counts


ostream& operator<<(ostream& out, CInode& in)
{
  string path;
  in.make_path(path);
  out << "[inode " << in.inode.ino << " " << path << (in.is_dir() ? "/ ":" ");
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
  out << " " << &in;
  out << "]";
  return out;
}


// ====== CInode =======
CInode::CInode(bool auth) : LRUObject(),
							hardlock(LOCK_TYPE_BASIC),
							softlock(LOCK_TYPE_ASYNC) {
  ref = 0;
  
  parent = NULL;
  
  dir = NULL;     // CDir opened separately

  auth_pins = 0;
  nested_auth_pins = 0;
  num_request_pins = 0;

  state = 0;  
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

  // can't open a dir if we're frozen_dir, bc of hashing stuff.
  assert(!is_frozen_dir());

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
	/*
	CDir *dir = get_parent_dir();
	if (is_auth() && !a) 
	  dir->nauthitems--;
	else
	  dir->nauthitems++;
	*/
  }
  
  if (a) state_set(CINODE_STATE_AUTH);
  else state_clear(CINODE_STATE_AUTH);
}



void CInode::make_path(string& s)
{
  if (parent) {
	parent->make_path(s);
  } 
  else if (is_root()) {
	s = "";  // root
  } 
  else {
	s = "(dangling)";  // dangling
  }
}

void CInode::make_anchor_trace(vector<Anchor*>& trace)
{
  if (parent) {
	parent->dir->inode->make_anchor_trace(trace);
	
	dout(7) << "make_anchor_trace adding " << ino() << " dirino " << parent->dir->inode->ino() << " dn " << parent->name << endl;
	trace.push_back( new Anchor(ino(), 
								parent->dir->inode->ino(),
								parent->name) );
  }
  else if (state_test(CINODE_STATE_DANGLING)) {
	dout(7) << "make_anchor_trace dangling " << ino() << " on mds " << dangling_auth << endl;
	string ref_dn;
	trace.push_back( new Anchor(ino(),
								MDS_INO_INODEFILE_OFFSET+dangling_auth,
								ref_dn) );
  }
  else 
	assert(is_root());
}




void CInode::mark_dirty() {
  
  dout(10) << "mark_dirty " << *this << endl;

  if (!parent) {
	dout(10) << " dangling, not marking dirty!" << endl;
	return;
  }

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

bool CInode::is_frozen_dir()
{
  if (parent && parent->dir->is_frozen_dir())
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
  finish_contexts(finished, result);
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


CInodeDiscover* CInode::replicate_to( int rep )
{
  // relax locks?
  if (!is_cached_by_anyone())
	replicate_relax_locks();
  
  // return the thinger
  int nonce = cached_by_add( rep );
  return new CInodeDiscover( this, nonce );
}


// debug crap -----------------------------

void CInode::dump(int dep)
{
  string ind(dep, '\t');
  //cout << ind << "[inode " << this << "]" << endl;
  
  if (dir)
	dir->dump(dep);
}

