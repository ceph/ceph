// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "MDS.h"
#include "MDCache.h"
#include "AnchorTable.h"

#include "common/Clock.h"

#include <string>

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) cout << g_clock.now() << " mds" << mdcache->mds->get_nodeid() << ".cache.ino(" << inode.ino << ") "


//int cinode_pins[CINODE_NUM_PINS];  // counts


ostream& operator<<(ostream& out, CInode& in)
{
  string path;
  in.make_path(path);
  out << "[inode " << in.inode.ino << " " << path << (in.is_dir() ? "/ ":" ");
  if (in.is_auth()) {
    out << "auth";
    if (in.is_replicated()) 
      out << in.get_replicas();
  } else {
    out << "rep@" << in.authority();
    out << "." << in.get_replica_nonce();
    assert(in.get_replica_nonce() >= 0);
  }

  if (in.is_symlink()) out << " symlink";

  out << " v" << in.get_version();

  out << " hard=" << in.hardlock;
  out << " file=" << in.filelock;

  if (in.get_num_ref()) {
    out << " |";
    for(set<int>::iterator it = in.get_ref_set().begin();
        it != in.get_ref_set().end();
        it++)
      out << " " << CInode::pin_name(*it);
  }

  // hack: spit out crap on which clients have caps
  if (!in.get_client_caps().empty()) {
    out << " caps={";
    for (map<int,Capability>::iterator it = in.get_client_caps().begin();
         it != in.get_client_caps().end();
         it++) {
      if (it != in.get_client_caps().begin()) out << ",";
      out << it->first;
    }
    out << "}";
  }
  out << " " << &in;
  out << "]";
  return out;
}


// ====== CInode =======
CInode::CInode(MDCache *c, bool auth) {
  mdcache = c;

  ref = 0;
  
  num_parents = 0;
  parent = NULL;
  
  dir = NULL;     // CDir opened separately

  // init unix group caps
  user_cap_set = false;
  group_cap_set = false;
  world_cap_set = false;

  if (g_conf.mds_group == 2) {
    thread_init = false;
    batching = false;
    buffer_stop = false;
    buffer_thread = BufferThread(this);
    buffer_thread.create();
  }

  auth_pins = 0;
  nested_auth_pins = 0;
  num_request_pins = 0;

  state = 0;  

  if (auth) state_set(STATE_AUTH);
}

CInode::~CInode() {
  if (dir) { delete dir; dir = 0; }
  if (g_conf.mds_group == 2) {
    buffer_lock.Lock();
    buffer_stop = true;
    buffer_cond.Signal();
    buffer_lock.Unlock();
    buffer_thread.join();
  }
}
void CInode::buffer_entry()
{
  cout << "buffer thread start------> for " << inode.ino << endl;
  buffer_lock.Lock();

  // init myself and signal anyone waiting for me to init
  thread_init = true;
  buffer_cond.Signal();

  while(!buffer_stop) {

    // ifwe're not buffering, then,
    // were gonna get signaled when we start buffering
    // plus i need to release the lock for anyone
    // waiting for me to init
    while (!batching)
      buffer_cond.Wait(buffer_lock);
    
    // the sleep releases the lock and allows the dispatch
    // to insert requests into the buffer
    // sleep first, then serve cap
    buffer_cond.WaitInterval(buffer_lock, utime_t(5,0));

    // now i've slept, make cap for users
    list<uid_t> user_set;
    CapGroup users_hash;
    for (set<MClientRequest *>::iterator si = buffered_reqs.begin();
	 si != buffered_reqs.end();
	 si++) {
      user_set.push_back((*si)->get_caller_uid());
      users_hash.add_user((*si)->get_caller_uid());
    }
    users_hash.sign_list(mds->getPrvKey());
    mds->unix_groups_byhash[users_hash.get_root_hash()]= users_hash;
      
    ExtCap *ext_cap = new ExtCap(FILE_MODE_RW,
				 inode.uid,
				 inode.gid,
				 users_hash.get_root_hash(),
				 inode.ino);
    ext_cap->set_type(BATCH);
    ext_cap->set_id(batch_id);
    ext_cap->sign_extcap(mds->getPrvKey());

    // put the cap in everyones cache
    for (list<uid_t>::iterator usi = user_set.begin();
	 usi != user_set.end();
	 usi++) {
      ext_caps[(*usi)] = (*ext_cap);
    }
    
    // let requests loose
    for (set<MClientRequest *>::iterator ri = buffered_reqs.begin();
	 ri != buffered_reqs.end();
	 ri++) {
      server->handle_client_open(*ri, this);
    }

    buffered_reqs.clear();
    
    //turn batching off
    batching = false;
  }

  buffer_lock.Unlock();
  cout << "<------buffer finish" << endl;
}

void CInode::add_to_buffer(MClientRequest *req, Server *serve, MDS *metads) {
  dout(1) << "Buffering the request for uid:"
       << req->get_caller_uid() << " on client:"
       << req->get_client() << " for file:"
       << inode.ino << " with client inst:" << req->get_client_inst() << endl;

  buffer_lock.Lock();

  // wait until the thread has initialized
  while (! thread_init)
    buffer_cond.Wait(buffer_lock);

  // was batching thread already on?
  if (batching) {
    buffered_reqs.insert(req);
  }
  else {
    
    // set external helper classes
    server = serve;
    mds = metads;
    
    batch_id.cid = mds->cap_id_count;
    batch_id.mds_id = mds->get_nodeid();
    mds->cap_id_count++;

    batching = true;
    batch_id_set = true;

    buffered_reqs.insert(req);

    // start the buffering now
    buffer_cond.Signal();
  }

  buffer_lock.Unlock();
  return;
}


// pins

void CInode::first_get()
{
  // pin my dentry?
  if (parent) 
    parent->get(CDentry::PIN_INODEPIN);
}

void CInode::last_put() 
{
  // unpin my dentry?
  if (parent) {
    parent->put(CDentry::PIN_INODEPIN);
  } 
  if (num_parents == 0 && get_num_ref() == 0)
    mdcache->inode_expire_queue.push_back(this);  // queue myself for garbage collection
}

void CInode::get_parent()
{
  num_parents++;
}
void CInode::put_parent()
{
  num_parents--;
  if (num_parents == 0 && get_num_ref() == 0)
    mdcache->inode_expire_queue.push_back(this);    // queue myself for garbage collection
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

CDir *CInode::get_or_open_dir(MDCache *mdcache)
{
  assert(is_dir());

  if (dir) return dir;

  // can't open a dir if we're frozen_dir, bc of hashing stuff.
  assert(!is_frozen_dir());

  // only auth can open dir alone.
  assert(is_auth());
  set_dir( new CDir(this, mdcache, true) );
  dir->dir_auth = -1;
  return dir;
}

CDir *CInode::set_dir(CDir *newdir)
{
  assert(dir == 0);
  dir = newdir;
  return dir;
}

void CInode::close_dir()
{
  assert(dir);
  assert(dir->get_num_ref() == 0);
  delete dir;
  dir = 0;
}


void CInode::set_auth(bool a) 
{
  if (!is_dangling() && !is_root() && 
      is_auth() != a) {
  }
  
  if (a) state_set(STATE_AUTH);
  else state_clear(STATE_AUTH);
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
  else if (state_test(STATE_DANGLING)) {
    dout(7) << "make_anchor_trace dangling " << ino() << " on mds " << dangling_auth << endl;
    string ref_dn;
    trace.push_back( new Anchor(ino(),
                                MDS_INO_INODEFILE_OFFSET+dangling_auth,
                                ref_dn) );
  }
  else 
    assert(is_root());
}




version_t CInode::pre_dirty()
{    
  assert(parent);
  return parent->pre_dirty();
}

void CInode::_mark_dirty()
{
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
    get(PIN_DIRTY);
  }
}

void CInode::mark_dirty(version_t pv) {
  
  dout(10) << "mark_dirty " << *this << endl;

  assert(parent);

  /*
    NOTE: I may already be dirty, but this fn _still_ needs to be called so that
    the directory is (perhaps newly) dirtied, and so that parent_dir_version is 
    updated below.
  */
  
  // only auth can get dirty.  "dirty" async data in replicas is relative to
  // filelock state, not the dirty flag.
  assert(is_auth());
  
  // touch my private version
  assert(inode.version < pv);
  inode.version = pv;
  _mark_dirty();

  // mark dentry too
  parent->mark_dirty(pv);
}

void CInode::mark_clean()
{
  dout(10) << " mark_clean " << *this << endl;
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
    put(PIN_DIRTY);
  }
}    

// state 





// new state encoders

void CInode::encode_file_state(bufferlist& bl) 
{
  bl.append((char*)&inode.size, sizeof(inode.size));
  bl.append((char*)&inode.mtime, sizeof(inode.mtime));
  bl.append((char*)&inode.atime, sizeof(inode.atime));  // ??
}

void CInode::decode_file_state(bufferlist& r, int& off)
{
  r.copy(off, sizeof(inode.size), (char*)&inode.size);
  off += sizeof(inode.size);
  r.copy(off, sizeof(inode.mtime), (char*)&inode.mtime);
  off += sizeof(inode.mtime);
  r.copy(off, sizeof(inode.atime), (char*)&inode.atime);
  off += sizeof(inode.atime);
}

/* not used currently
void CInode::decode_merge_file_state(crope& r, int& off)
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
*/

void CInode::encode_hard_state(bufferlist& r)
{
  r.append((char*)&inode.mode, sizeof(inode.mode));
  r.append((char*)&inode.uid, sizeof(inode.uid));
  r.append((char*)&inode.gid, sizeof(inode.gid));
  r.append((char*)&inode.ctime, sizeof(inode.ctime));
}

void CInode::decode_hard_state(bufferlist& r, int& off)
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
    get(PIN_WAITER);
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
    put(PIN_WAITER);
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
    get(PIN_AUTHPIN);
  auth_pins++;

  dout(7) << "auth_pin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;

  if (parent)
    parent->dir->adjust_nested_auth_pins( 1 );
}

void CInode::auth_unpin() {
  auth_pins--;
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(7) << "auth_unpin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;

  assert(auth_pins >= 0);

  if (parent)
    parent->dir->adjust_nested_auth_pins( -1 );
}



// authority

int CInode::authority() {
  if (is_dangling()) 
    return dangling_auth;      // explicit

  if (is_root()) {             // i am root
    if (dir)
      return dir->get_dir_auth();  // bit of a chicken/egg issue here!
    else
      return CDIR_AUTH_UNKNOWN;
  }

  if (parent)
    return parent->dir->dentry_authority( parent->name );

  return -1;  // undefined (inode must not be linked yet!)
}


CInodeDiscover* CInode::replicate_to( int rep )
{
  assert(is_auth());

  // relax locks?
  if (!is_replicated())
    replicate_relax_locks();
  
  // return the thinger
  int nonce = add_replica( rep );
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

