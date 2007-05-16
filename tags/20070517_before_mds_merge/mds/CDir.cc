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



#include "CDir.h"
#include "CDentry.h"
#include "CInode.h"

#include "MDS.h"
#include "MDCache.h"
#include "MDSMap.h"

#include "include/Context.h"
#include "common/Clock.h"

#include <cassert>

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) cout << g_clock.now() << " mds" << cache->mds->get_nodeid() << ".cache.dir(" << inode->inode.ino << ") "


// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };



ostream& operator<<(ostream& out, CDir& dir)
{
  string path;
  dir.get_inode()->make_path(path);
  out << "[dir " << dir.ino() << " " << path << "/";
  if (dir.is_auth()) {
    out << " auth";
    if (dir.is_replicated())
      out << dir.get_replicas();

    out << " v=" << dir.get_version();
    out << " pv=" << dir.get_projected_version();
    out << " cv=" << dir.get_committing_version();
    out << " lastcv=" << dir.get_last_committed_version();
  } else {
    out << " rep@" << dir.authority();
    if (dir.get_replica_nonce() > 1)
      out << "." << dir.get_replica_nonce();
  }

  if (dir.get_dir_auth() != CDIR_AUTH_PARENT)
    out << " dir_auth=" << dir.get_dir_auth();
  
  out << " state=" << dir.get_state();
  if (dir.state_test(CDIR_STATE_PROXY)) out << "|proxy";
  if (dir.state_test(CDIR_STATE_COMPLETE)) out << "|complete";
  if (dir.state_test(CDIR_STATE_FREEZINGTREE)) out << "|freezingtree";
  if (dir.state_test(CDIR_STATE_FROZENTREE)) out << "|frozentree";
  if (dir.state_test(CDIR_STATE_FROZENTREELEAF)) out << "|frozentreeleaf";
  if (dir.state_test(CDIR_STATE_FROZENDIR)) out << "|frozendir";
  if (dir.state_test(CDIR_STATE_FREEZINGDIR)) out << "|freezingdir";

  out << " sz=" << dir.get_nitems() << "+" << dir.get_nnull();
  
  if (dir.get_num_ref()) {
    out << " |";
    for(set<int>::iterator it = dir.get_ref_set().begin();
        it != dir.get_ref_set().end();
        it++)
      out << " " << CDir::pin_name(*it);
  }

  out << " " << &dir;
  return out << "]";
}


// -------------------------------------------------------------------
// CDir

CDir::CDir(CInode *in, MDCache *mdcache, bool auth)
{
  inode = in;
  this->cache = mdcache;
  
  nitems = 0;
  nnull = 0;
  state = CDIR_STATE_INITIAL;

  projected_version = version = 0;
  committing_version = 0;
  last_committed_version = 0;

  ref = 0;

  // auth
  dir_auth = -1;
  assert(in->is_dir());
  if (auth) 
    state |= CDIR_STATE_AUTH;
  /*
  if (in->dir_is_hashed()) {
    assert(0);                      // when does this happen?  
    state |= CDIR_STATE_HASHED;
  }
  */
 
  auth_pins = 0;
  nested_auth_pins = 0;
  request_pins = 0;
  
  dir_rep = CDIR_REP_NONE;
}




/***
 * linking fun
 */

CDentry* CDir::add_dentry( const string& dname, inodeno_t ino, bool auth) 
{
  // foreign
  assert(lookup(dname) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, ino);
  if (auth) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = projected_version;
  
  // add to dir
  assert(items.count(dn->name) == 0);
  assert(null_items.count(dn->name) == 0);

  items[dn->name] = dn;
  nitems++;

  dout(12) << "add_dentry " << *dn << endl;

  // pin?
  if (nnull + nitems == 1) get(PIN_CHILD);
  
  assert(nnull + nitems == items.size());
  assert(nnull == null_items.size());         
  return dn;
}


CDentry* CDir::add_dentry( const string& dname, CInode *in, bool auth ) 
{
  // primary
  assert(lookup(dname) == 0);
  
  // create dentry
  CDentry* dn = new CDentry(dname, in);
  if (auth) 
    dn->state_set(CDentry::STATE_AUTH);
  cache->lru.lru_insert_mid(dn);

  dn->dir = this;
  dn->version = projected_version;
  
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

  dout(12) << "add_dentry " << *dn << endl;

  // pin?
  if (nnull + nitems == 1) get(PIN_CHILD);
  
  assert(nnull + nitems == items.size());
  assert(nnull == null_items.size());         
  return dn;
}



void CDir::remove_dentry(CDentry *dn) 
{
  dout(12) << "remove_dentry " << *dn << endl;

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

  cache->lru.lru_remove(dn);
  delete dn;

  // unpin?
  if (nnull + nitems == 0) put(PIN_CHILD);

  assert(nnull + nitems == items.size());
  assert(nnull == null_items.size());         
}

void CDir::link_inode( CDentry *dn, inodeno_t ino)
{
  dout(12) << "link_inode " << *dn << " remote " << ino << endl;

  assert(dn->is_null());
  dn->set_remote_ino(ino);
  nitems++;

  assert(null_items.count(dn->name) == 1);
  null_items.erase(dn->name);
  nnull--;
}

void CDir::link_inode( CDentry *dn, CInode *in )
{
  dout(12) << "link_inode " << *dn << " " << *in << endl;
  assert(!dn->is_remote());

  link_inode_work(dn,in);
  
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
  in->set_primary_parent(dn);

  nitems++;  // adjust dir size
  
  // set dir version
  in->inode.version = dn->get_version();
  
  // clear dangling
  in->state_clear(CInode::STATE_DANGLING);

  // pin dentry?
  if (in->get_num_ref())
    dn->get(CDentry::PIN_INODEPIN);
  
  // adjust auth pin count
  if (in->auth_pins + in->nested_auth_pins)
    adjust_nested_auth_pins( in->auth_pins + in->nested_auth_pins );
}

void CDir::unlink_inode( CDentry *dn )
{
  dout(12) << "unlink_inode " << *dn << " " << *dn->inode << endl;

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
 
  if (dn->is_remote()) {
    // remote
    if (in) 
      dn->unlink_remote();

    dn->set_remote_ino(0);
  } else {
    // primary
    assert(dn->is_primary());
 
    // explicitly define auth
    in->dangling_auth = in->authority();
    //dout(10) << "unlink_inode " << *in << " dangling_auth now " << in->dangling_auth << endl;

    // unpin dentry?
    if (in->get_num_ref())
      dn->put(CDentry::PIN_INODEPIN);
    
    // unlink auth_pin count
    if (in->auth_pins + in->nested_auth_pins)
      adjust_nested_auth_pins( 0 - (in->auth_pins + in->nested_auth_pins) );
    
    // set dangling flag
    in->state_set(CInode::STATE_DANGLING);

    // detach inode
    in->remove_primary_parent(dn);
    dn->inode = 0;
  }

  nitems--;   // adjust dir size
}

void CDir::remove_null_dentries() {
  dout(12) << "remove_null_dentries " << *this << endl;

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
    remove_dentry(dn);
  }
  assert(null_items.empty());         
  assert(nnull == 0);
  assert(nnull + nitems == items.size());
}



/****************************************
 * WAITING
 */

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
    get(PIN_WAITER);
  waiting_on_dentry[ dentry ].insert(pair<int,Context*>(tag,c));
  dout(10) << "add_waiter dentry " << dentry << " tag " << tag << " " << c << " on " << *this << endl;
}

void CDir::add_waiter(int tag, Context *c) {
  // hierarchical?
  if (tag & CDIR_WAIT_ATFREEZEROOT && (is_freezing() || is_frozen())) {  
    if (is_freezing_tree_root() || is_frozen_tree_root() ||
        is_freezing_dir() || is_frozen_dir()) {
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
    get(PIN_WAITER);
  waiting.insert(pair<int,Context*>(tag,c));
  dout(10) << "add_waiter " << tag << " " << c << " on " << *this << endl;
}


void CDir::take_waiting(int mask, 
                        const string& dentry,
                        list<Context*>& ls,
                        int num)
{
  if (waiting_on_dentry.empty()) return;
  
  multimap<int,Context*>::iterator it = waiting_on_dentry[dentry].begin();
  while (it != waiting_on_dentry[dentry].end()) {
    if (it->first & mask) {
      ls.push_back(it->second);
      dout(10) << "take_waiting dentry " << dentry << " mask " << mask << " took " << it->second << " tag " << it->first << " on " << *this << endl;
      waiting_on_dentry[dentry].erase(it++);

      if (num) {
        if (num == 1) break;
        num--;
      }
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
    put(PIN_WAITER);
}

/* NOTE: this checks dentry waiters too */
void CDir::take_waiting(int mask,
                        list<Context*>& ls)
{
  if (waiting_on_dentry.size()) {
    // try each dentry
    hash_map<string, multimap<int,Context*> >::iterator it = 
      waiting_on_dentry.begin(); 
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
      put(PIN_WAITER);
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

version_t CDir::pre_dirty()
{
  ++projected_version;
  dout(10) << "pre_dirty " << projected_version << endl;
  return projected_version;
}

void CDir::_mark_dirty()
{
  if (!state_test(CDIR_STATE_DIRTY)) {
    state_set(CDIR_STATE_DIRTY);
    dout(10) << "mark_dirty (was clean) " << *this << " version " << version << endl;
    get(PIN_DIRTY);
  } else {
    dout(10) << "mark_dirty (already dirty) " << *this << " version " << version << endl;
  }
}

void CDir::mark_dirty(version_t pv)
{
  ++version;
  assert(pv == version);
  _mark_dirty();
}

void CDir::mark_clean()
{
  dout(10) << "mark_clean " << *this << " version " << version << endl;
  if (state_test(CDIR_STATE_DIRTY)) {
    state_clear(CDIR_STATE_DIRTY);
    put(PIN_DIRTY);
  }
}




void CDir::first_get()
{
  inode->get(CInode::PIN_DIR);
}

void CDir::last_put()
{
  inode->put(CInode::PIN_DIR);
}



/********************************
 * AUTHORITY
 */

/*
 * simple rule: if dir_auth isn't explicit, auth is the same as the inode.
 */
int CDir::authority() 
{
  if (dir_auth == CDIR_AUTH_PARENT)
    return inode->authority();
  return dir_auth;
}

int CDir::dentry_authority(const string& dn )
{
  // hashing -- subset of nodes have hashed the contents
  if (is_hashing() && !hashed_subset.empty()) {
    int hashauth = cache->hash_dentry( inode->ino(), dn );  // hashed
    if (hashed_subset.count(hashauth))
      return hashauth;
  }

  // hashed
  if (is_hashed()) {
    return cache->hash_dentry( inode->ino(), dn );  // hashed
  }
  
  if (get_dir_auth() == CDIR_AUTH_PARENT) {
    //dout(15) << "dir_auth = parent at " << *this << endl;
    return inode->authority();       // same as my inode
  }

  // it's explicit for this whole dir
  //dout(15) << "dir_auth explicit " << dir_auth << " at " << *this << endl;
  return get_dir_auth();
}

void CDir::set_dir_auth(int d) 
{ 
  dout(10) << "setting dir_auth=" << d << " from " << dir_auth << " on " << *this << endl;
  dir_auth = d; 
}


/*****************************************
 * AUTH PINS
 */

void CDir::auth_pin() {
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

  dout(7) << "auth_pin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;
  
  inode->nested_auth_pins++;
  if (inode->parent)
    inode->parent->dir->adjust_nested_auth_pins( 1 );
}

void CDir::auth_unpin() {
  auth_pins--;
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

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



/*****************************************************************************
 * FREEZING
 */

void CDir::on_freezeable()
{
  // check for anything pending freezeable

  /* NOTE: the first of these will likely freeze the dir, and unmark
     FREEZING.  additional ones will re-flag FREEZING.  this isn't
     particularly graceful, and might cause problems if the first one
     needs to know about other waiters.... FIXME? */
  
  finish_waiting(CDIR_WAIT_FREEZEABLE);
}

// FREEZE TREE

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

  if (state_test(CDIR_STATE_FROZENTREE)) {
    // frozen.  unfreeze.
    state_clear(CDIR_STATE_FROZENTREE);

    // unpin  (may => FREEZEABLE)   FIXME: is this order good?
    inode->auth_unpin();

    // waiters?
    finish_waiting(CDIR_WAIT_UNFREEZE);
  } else {
    // freezing.  stop it.
    assert(state_test(CDIR_STATE_FREEZINGTREE));
    state_clear(CDIR_STATE_FREEZINGTREE);
    
    // cancel freeze waiters
    finish_waiting(CDIR_WAIT_FREEZEABLE, -1);
  }
}

bool CDir::is_freezing_tree()
{
  CDir *dir = this;
  while (1) {
    if (dir->is_freezing_tree_root()) return true;
    if (dir->is_import()) return false;
    if (dir->is_hashed()) return false;
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
    if (dir->is_hashed()) return false;
    if (dir->is_frozen_tree_leaf()) return false;
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









// -----------------------------------------------------------------
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

