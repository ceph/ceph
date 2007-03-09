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

#include "osdc/Objecter.h"

#include <cassert>


// PINS
//int cdir_pins[CDIR_NUM_PINS] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0 };



ostream& operator<<(ostream& out, CDir& dir)
{
  string path;
  dir.get_inode()->make_path(path);
  out << "[dir " << dir.ino();
  if (!dir.frag.is_root()) out << "%" << dir.frag;
  out << " " << path << "/";
  if (dir.is_auth()) {
    out << " auth";
    if (dir.is_replicated())
      out << dir.get_replicas();

    out << " pv=" << dir.get_projected_version();
    out << " v=" << dir.get_version();
    out << " cv=" << dir.get_committing_version();
    out << "/" << dir.get_committed_version();
  } else {
    out << " rep@" << dir.authority();
    if (dir.get_replica_nonce() > 1)
      out << "." << dir.get_replica_nonce();
  }

  if (dir.get_dir_auth() != CDIR_AUTH_DEFAULT) 
    out << " dir_auth=" << dir.get_dir_auth();
  
  if (dir.get_cum_auth_pins())
    out << " ap=" << dir.get_auth_pins() << "+" << dir.get_nested_auth_pins();

  out << " state=" << dir.get_state();
  if (dir.state_test(CDir::STATE_PROXY)) out << "|proxy";
  if (dir.state_test(CDir::STATE_COMPLETE)) out << "|complete";
  if (dir.state_test(CDir::STATE_FREEZINGTREE)) out << "|freezingtree";
  if (dir.state_test(CDir::STATE_FROZENTREE)) out << "|frozentree";
  //if (dir.state_test(CDir::STATE_FROZENTREELEAF)) out << "|frozentreeleaf";
  if (dir.state_test(CDir::STATE_FROZENDIR)) out << "|frozendir";
  if (dir.state_test(CDir::STATE_FREEZINGDIR)) out << "|freezingdir";
  if (dir.state_test(CDir::STATE_EXPORTBOUND)) out << "|exportbound";
  if (dir.state_test(CDir::STATE_IMPORTBOUND)) out << "|importbound";

  out << " sz=" << dir.get_nitems() << "+" << dir.get_nnull();
  
  if (dir.get_num_ref()) {
    out << " |";
    dir.print_pin_set(out);
  }

  out << " " << &dir;
  return out << "]";
}


#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) cout << g_clock.now() << " mds" << cache->mds->get_nodeid() << ".cache.dir(" << get_inode()->inode.ino << ") "
//#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_mds) cout << g_clock.now() << " mds" << cache->mds->get_nodeid() << ".cache." << *this << " "


// -------------------------------------------------------------------
// CDir

CDir::CDir(CInode *in, frag_t fg, MDCache *mdcache, bool auth)
{
  inode = in;
  frag = fg;
  this->cache = mdcache;
  
  nitems = 0;
  nnull = 0;
  state = STATE_INITIAL;

  projected_version = version = 0;
  committing_version = 0;
  committed_version = 0;

  // dir_auth
  dir_auth = CDIR_AUTH_DEFAULT;

  // auth
  assert(in->is_dir());
  if (auth) 
    state |= STATE_AUTH;
 
  auth_pins = 0;
  nested_auth_pins = 0;
  request_pins = 0;
  
  //dir_rep = REP_NONE;
  dir_rep = REP_ALL;      // hack: to wring out some bugs! FIXME FIXME
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
  //assert(null_items.count(dn->name) == 0);

  items[dn->name] = dn;
  nitems++;

  dout(12) << "add_dentry " << *dn << endl;

  // pin?
  if (nnull + nitems == 1) get(PIN_CHILD);
  
  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
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
  //assert(null_items.count(dn->name) == 0);

  items[dn->name] = dn;
  
  if (in) {
    link_inode_work( dn, in );
  } else {
    assert(dn->inode == 0);
    //null_items[dn->name] = dn;
    nnull++;
  }

  dout(12) << "add_dentry " << *dn << endl;

  // pin?
  if (nnull + nitems == 1) get(PIN_CHILD);
  
  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
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
    //assert(null_items.count(dn->name) == 1);
    //null_items.erase(dn->name);
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
  //assert(nnull == null_items.size());         
}

void CDir::link_inode( CDentry *dn, inodeno_t ino)
{
  dout(12) << "link_inode " << *dn << " remote " << ino << endl;

  assert(dn->is_null());
  dn->set_remote_ino(ino);
  nitems++;

  //assert(null_items.count(dn->name) == 1);
  //null_items.erase(dn->name);
  nnull--;
}

void CDir::link_inode( CDentry *dn, CInode *in )
{
  dout(12) << "link_inode " << *dn << " " << *in << endl;
  assert(!dn->is_remote());

  link_inode_work(dn,in);
  
  // remove from null list
  //assert(null_items.count(dn->name) == 1);
  //null_items.erase(dn->name);
  nnull--;

  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
}

void CDir::link_inode_work( CDentry *dn, CInode *in )
{
  dn->inode = in;
  in->set_primary_parent(dn);

  nitems++;  // adjust dir size
  
  // set inode version
  //in->inode.version = dn->get_version();
  
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
  //assert(null_items.count(dn->name) == 0);
  //null_items[dn->name] = dn;
  nnull++;

  assert(nnull + nitems == items.size());
  //assert(nnull == null_items.size());         
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
  for (CDir_map_t::iterator it = items.begin();
       it != items.end(); 
       it++) {
    if (it->second->is_null())
      dns.push_back(it->second);
  }
  
  for (list<CDentry*>::iterator it = dns.begin();
       it != dns.end();
       it++) {
    CDentry *dn = *it;
    assert(dn->is_sync());
    remove_dentry(dn);
  }
  //assert(null_items.empty());         
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
  if (tag & WAIT_ATFREEZEROOT && (is_freezing() || is_frozen())) {  
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
  //finish_contexts(finished, result);
  cache->mds->queue_finished(finished);
}

void CDir::finish_waiting(int mask, const string& dn, int result) 
{
  dout(11) << "finish_waiting mask " << mask << " dn " << dn << " result " << result << " on " << *this << endl;

  list<Context*> finished;
  take_waiting(mask, dn, finished);
  //finish_contexts(finished, result);
  cache->mds->queue_finished(finished);
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
  if (!state_test(STATE_DIRTY)) {
    state_set(STATE_DIRTY);
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
  if (state_test(STATE_DIRTY)) {
    state_clear(STATE_DIRTY);
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



/******************************************************************************
 * FETCH and COMMIT
 */

// -----------------------
// FETCH

class C_Dir_Fetch : public Context {
 protected:
  CDir *dir;
 public:
  bufferlist bl;

  C_Dir_Fetch(CDir *d) : dir(d) { }
  void finish(int result) {
    dir->_fetched(bl);
  }
};

void CDir::fetch(Context *c)
{
  dout(10) << "fetch on " << *this << endl;
  
  if (c) add_waiter(WAIT_COMPLETE, c);
  
  // alrady fetching?
  if (state_test(CDir::STATE_FETCHING)) {
    dout(7) << "already fetching; waiting" << endl;
    return;
  }

  state_set(CDir::STATE_FETCHING);

  if (cache->mds->logger) cache->mds->logger->inc("fdir");

  // start by reading the first hunk of it
  C_Dir_Fetch *fin = new C_Dir_Fetch(this);
  cache->mds->objecter->read( get_ondisk_object(), 
			      0, 0,   // whole object
			      &fin->bl,
			      fin );
}

void CDir::_fetched(bufferlist &bl)
{
  dout(10) << "_fetched " << 0 << "~" << bl.length() 
	   << " on " << *this
	   << endl;
  
  // give up?
  if (!is_auth() || is_frozen()) {
    dout(10) << "_fetched canceling (!auth or frozen)" << endl;
    //ondisk_bl.clear();
    //ondisk_size = 0;
    
    // kick waiters?
    finish_waiting(WAIT_COMPLETE, -1);
    return;
  }

  // add to our buffer
  size_t ondisk_size;
  assert(bl.length() > sizeof(ondisk_size));
  bl.copy(0, sizeof(ondisk_size), (char*)&ondisk_size);
  off_t have = bl.length() - sizeof(ondisk_size);
  dout(10) << "ondisk_size " << ondisk_size << ", have " << have << endl;
  assert(have == ondisk_size);

  // decode.
  int off = sizeof(ondisk_size);

  __uint32_t num_dn;
  version_t  got_version;
  
  bl.copy(off, sizeof(num_dn), (char*)&num_dn);
  off += sizeof(num_dn);
  bl.copy(off, sizeof(got_version), (char*)&got_version);
  off += sizeof(got_version);

  dout(10) << "_fetched " << num_dn << " dn, got_version " << got_version
	   << ", " << ondisk_size << " bytes"
	   << endl;
  
  while (num_dn--) {
    // dentry
    string dname;
    ::_decode(dname, bl, off);
    dout(24) << "parse filename '" << dname << "'" << endl;
    
    CDentry *dn = lookup(dname);  // existing dentry?
    
    char type = bl[off];
    ++off;
    if (type == 'L') {
      // hard link
      inodeno_t ino;
      bl.copy(off, sizeof(ino), (char*)&ino);
      off += sizeof(ino);
      
      if (dn) {
        if (dn->get_inode() == 0) {
          dout(12) << "readdir had NEG dentry " << *dn << endl;
        } else {
          dout(12) << "readdir had dentry " << *dn << endl;
        }
      } else {
	// (remote) link
	CDentry *dn = add_dentry( dname, ino );
	
	// link to inode?
	CInode *in = cache->get_inode(ino);   // we may or may not have it.
	if (in) {
	  dn->link_remote(in);
	  dout(12) << "readdir got remote link " << ino << " which we have " << *in << endl;
	} else {
	  dout(12) << "readdir got remote link " << ino << " (dont' have it)" << endl;
	}
      }
    } 
    else if (type == 'I') {
      // inode
      
      // parse out inode
      inode_t inode;
      bl.copy(off, sizeof(inode), (char*)&inode);
      off += sizeof(inode);

      string symlink;
      if (inode.is_symlink())
        ::_decode(symlink, bl, off);
      
      if (dn) {
        if (dn->get_inode() == 0) {
          dout(12) << "readdir had NEG dentry " << *dn << endl;
        } else {
          dout(12) << "readdir had dentry " << *dn << endl;
        }
      } else {
	// add inode
	CInode *in = 0;
	if (cache->have_inode(inode.ino)) {
	  in = cache->get_inode(inode.ino);
	  dout(12) << "readdir got (but i already had) " << *in 
		   << " mode " << in->inode.mode 
		   << " mtime " << in->inode.mtime << endl;
	  assert(0);  // this shouldn't happen!! 
	} else {
	  // inode
	  in = new CInode(cache);
	  in->inode = inode;
	  
	  // symlink?
	  if (in->is_symlink()) 
	    in->symlink = symlink;
	  
	  // add 
	  cache->add_inode( in );
	
	  // link
	  add_dentry( dname, in );
	  dout(12) << "readdir got " << *in << " mode " << in->inode.mode << " mtime " << in->inode.mtime << endl;
	}
      }
    } else {
      dout(1) << "corrupt directory, i got tag char '" << type << "' val " << (int)(type) 
	      << " at pos " << off << endl;
      assert(0);
    }

    /** clean underwater item?
     * Underwater item is something that is dirty in our cache from
     * journal replay, but was previously flushed to disk before the
     * mds failed.
     *
     * We only do this is committed_version == 0. that implies either
     * - this is a fetch after from a clean/empty CDir is created
     *   (and has no effect, since the dn won't exist); or
     * - this is a fetch after _recovery_, which is what we're worried 
     *   about.  Items that are marked dirty from the journal should be
     *   marked clean if they appear on disk.
     */
    if (committed_version == 0 &&     
	dn &&
	dn->get_version() <= got_version &&
	dn->is_dirty()) {
      dout(10) << "readdir had underwater dentry " << *dn << ", marking clean" << endl;
      dn->mark_clean();

      if (dn->get_inode()) {
	assert(dn->get_inode()->get_version() <= got_version);
	dout(10) << "readdir had underwater inode " << *dn->get_inode() << ", marking clean" << endl;
	dn->get_inode()->mark_clean();
      }
    }
  }

  // take the loaded version?
  // only if we are a fresh CDir* with no prior state.
  if (version == 0) {
    assert(projected_version == 0);
    assert(!state_test(STATE_COMMITTING));
    projected_version = version = committing_version = committed_version = got_version;
  }

  // mark complete, !fetching
  state_set(STATE_COMPLETE);
  state_clear(STATE_FETCHING);

  // kick waiters
  finish_waiting(WAIT_COMPLETE, 0);
  /*
  list<Context*> waiters;
  take_waiting(WAIT_COMPLETE, waiters);
  cache->mds->queue_finished(waiters);
  */
}



// -----------------------
// COMMIT

/**
 * commit
 *
 * @param want min version i want committed
 * @param c callback for completion
 */
void CDir::commit(version_t want, Context *c)
{
  dout(10) << "commit want " << want << " on " << *this << endl;
  if (want == 0) want = version;

  // preconditions
  assert(want <= version);          // can't commit the future
  assert(committed_version < want); // the caller is stupid
  assert(is_auth());
  assert(can_auth_pin());

  // note: queue up a noop if necessary, so that we always
  // get an auth_pin.
  if (!c)
    c = new C_NoopContext;

  // auth_pin on first waiter
  if (waiting_for_commit.empty())
    auth_pin();
  waiting_for_commit[want].push_back(c);
  
  // ok.
  _commit(want);
}


class C_Dir_RetryCommit : public Context {
  CDir *dir;
  version_t want;
public:
  C_Dir_RetryCommit(CDir *d, version_t v) : 
    dir(d), want(v) { }
  void finish(int r) {
    dir->_commit(want);
  }
};

class C_Dir_Committed : public Context {
  CDir *dir;
  version_t version;
public:
  C_Dir_Committed(CDir *d, version_t v) : dir(d), version(v) { }
  void finish(int r) {
    dir->_committed(version);
  }
};

void CDir::_commit(version_t want)
{
  dout(10) << "_commit want " << want << " on " << *this << endl;

  // we can't commit things in the future.
  // (even the projected future.)
  assert(want <= version);

  // check pre+postconditions.
  assert(is_auth());

  // already committed?
  if (committed_version >= want) {
    dout(10) << "already committed " << committed_version << " >= " << want << endl;
    return;
  }
  // already committing >= want?
  if (committing_version >= want) {
    dout(10) << "already committing " << committing_version << " >= " << want << endl;
    assert(state_test(STATE_COMMITTING));
    return;
  }
  
  // complete?
  if (!is_complete()) {
    dout(7) << "commit not complete, fetching first" << endl;
    fetch(new C_Dir_RetryCommit(this, want));
    return;
  }
  
  // commit.
  committing_version = version;

  // mark committing (if not already)
  if (!state_test(STATE_COMMITTING)) {
    dout(10) << "marking committing" << endl;
    state_set(STATE_COMMITTING);
  }
  
  if (cache->mds->logger) cache->mds->logger->inc("cdir");

  // encode dentries
  bufferlist dnbl;
  __uint32_t num_dn = 0;

  for (CDir_map_t::iterator it = items.begin();
       it != items.end();
       it++) {
    CDentry *dn = it->second;
    
    if (dn->is_null()) 
      continue;  // skip negative entries
    
    // primary or remote?
    if (dn->is_remote()) {
      inodeno_t ino = dn->get_remote_ino();
      dout(14) << " pos " << dnbl.length() << " dn '" << it->first << "' remote ino " << ino << endl;
      
      // name, marker, ino
      dnbl.append( it->first.c_str(), it->first.length() + 1);
      dnbl.append( "L", 1 );         // remote link
      dnbl.append((char*)&ino, sizeof(ino));
    } else {
      // primary link
      CInode *in = dn->get_inode();
      assert(in);

      dout(14) << " pos " << dnbl.length() << " dn '" << it->first << "' inode " << *in << endl;
  
      // name, marker, inode, [symlink string]
      dnbl.append( it->first.c_str(), it->first.length() + 1);
      dnbl.append( "I", 1 );         // inode
      dnbl.append( (char*) &in->inode, sizeof(inode_t));
      
      if (in->is_symlink()) {
        // include symlink destination!
        dout(18) << "    inlcuding symlink ptr " << in->symlink << endl;
        dnbl.append( (char*) in->symlink.c_str(), in->symlink.length() + 1);
      }
    }
    num_dn++;
  }

  // wrap it up
  bufferlist bl;
  size_t size;
  size = dnbl.length() + sizeof(num_dn) + sizeof(version);
  bl.append((char*)&size, sizeof(size));
  bl.append((char*)&num_dn, sizeof(num_dn));
  bl.append((char*)&version, sizeof(version));
  bl.claim_append(dnbl);
  assert(size == bl.length() - sizeof(size));

  // write it.
  cache->mds->objecter->write( get_ondisk_object(),
			       0, bl.length(),
			       bl,
			       NULL, new C_Dir_Committed(this, version) );
}


/**
 * _committed
 *
 * @param v version i just committed
 */
void CDir::_committed(version_t v)
{
  dout(10) << "_committed v " << v << " on " << *this << endl;
  assert(is_auth());
  
  // take note.
  assert(v > committed_version);
  assert(v <= committing_version);
  committed_version = v;

  // _all_ commits done?
  if (committing_version == committed_version) 
    state_clear(CDir::STATE_COMMITTING);
  
  // dir clean?
  if (committed_version == version) 
    mark_clean();

  // dentries clean?
  for (CDir_map_t::iterator it = items.begin();
       it != items.end(); ) {
    CDentry *dn = it->second;
    it++;
    
    // dentry
    if (committed_version >= dn->get_version()) {
      if (dn->is_dirty()) {
	dout(15) << " dir " << committed_version << " >= dn " << dn->get_version() << " now clean " << *dn << endl;
	dn->mark_clean();
      } 
    } else {
      dout(15) << " dir " << committed_version << " < dn " << dn->get_version() << " still dirty " << *dn << endl;
    }

    // inode?
    if (dn->is_primary()) {
      CInode *in = dn->get_inode();
      assert(in);
      assert(in->is_auth());
      
      if (committed_version >= in->get_version()) {
	if (in->is_dirty()) {
	  dout(15) << " dir " << committed_version << " >= inode " << in->get_version() << " now clean " << *in << endl;
	  in->mark_clean();
	}
      } else {
	dout(15) << " dir " << committed_version << " < inode " << in->get_version() << " still dirty " << *in << endl;
	assert(in->is_dirty());
      }
    }
  }

  // finishers?
  bool were_waiters = !waiting_for_commit.empty();
  
  map<version_t, list<Context*> >::iterator p = waiting_for_commit.begin();
  while (p != waiting_for_commit.end()) {
    map<version_t, list<Context*> >::iterator n = p;
    n++;
    if (p->first > committed_version) break; // haven't committed this far yet.
    cache->mds->queue_finished(p->second);
    waiting_for_commit.erase(p);
    p = n;
  } 

  // unpin if we kicked the last waiter.
  if (were_waiters &&
      waiting_for_commit.empty())
    auth_unpin();
}






/********************************
 * AUTHORITY
 */

/*
 * if dir_auth.first == parent, auth is same as inode.
 * unless .second != unknown, in which case that sticks.
 */
pair<int,int> CDir::authority() 
{
  if (is_subtree_root()) 
    return dir_auth;
  else
    return inode->authority();
}

/** is_subtree_root()
 * true if this is an auth delegation point.  
 * that is, dir_auth != default (parent,unknown)
 *
 * some key observations:
 *  if i am auth:
 *    - any region bound will be an export, or frozen.
 *
 * note that this DOES heed dir_auth.pending
 */
bool CDir::is_subtree_root()
{
  if (dir_auth == CDIR_AUTH_DEFAULT) {
    //dout(10) << "is_subtree_root false " << dir_auth << " != " << CDIR_AUTH_DEFAULT
    //<< " on " << ino() << endl;
    return false;
  } else {
    //dout(10) << "is_subtree_root true " << dir_auth << " != " << CDIR_AUTH_DEFAULT
    //<< " on " << ino() << endl;
    return true;
  }
}



pair<int,int> CDir::dentry_authority(const string& dn)
{
  // forget hashing for now.
  return authority();

  /*
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
  
  if (dir_auth == CDIR_AUTH_PARENT) {
    //dout(15) << "dir_auth = parent at " << *this << endl;
    return inode->authority(a2);       // same as my inode
  }

  // it's explicit for this whole dir
  //dout(15) << "dir_auth explicit " << dir_auth << " at " << *this << endl;
  return get_dir_auth(a2);
  */
}


/** set_dir_auth
 *
 * always list ourselves first.
 *
 * accept 'iamauth' param so that i can intelligently adjust freeze auth_pins
 * even when the auth bit isn't correct.  
 * as when calling MDCache::import_subtree(...).
 */
void CDir::set_dir_auth(pair<int,int> a, bool iamauth) 
{ 
  dout(10) << "setting dir_auth=" << a
	   << " from " << dir_auth
	   << " on " << *this << endl;
  
  bool was_subtree = is_subtree_root();

  // set it.
  dir_auth = a;

  // new subtree root?
  if (!was_subtree && is_subtree_root()) {
    dout(10) << " new subtree root, adjusting auth_pins" << endl;
    
    // adjust nested auth pins
    inode->adjust_nested_auth_pins(-get_cum_auth_pins());
    
    // unpin parent of frozen dir/tree?
    if (inode->is_auth() && (is_frozen_tree_root() || is_frozen_dir()))
      inode->auth_unpin();
  } 
  if (was_subtree && !is_subtree_root()) {
    dout(10) << " old subtree root, adjusting auth_pins" << endl;
    
    // adjust nested auth pins
    inode->adjust_nested_auth_pins(get_cum_auth_pins());

    // pin parent of frozen dir/tree?
    if (inode->is_auth() && (is_frozen_tree_root() || is_frozen_dir()))
      inode->auth_pin();
  }
}


/*****************************************
 * AUTH PINS and FREEZING
 *
 * the basic plan is that auth_pins only exist in auth regions, and they
 * prevent a freeze (and subsequent auth change).  
 *
 * however, we also need to prevent a parent from freezing if a child is frozen.
 * for that reason, the parent inode of a frozen directory is auth_pinned.
 *
 * the oddity is when the frozen directory is a subtree root.  if that's the case,
 * the parent inode isn't frozen.  which means that when subtree authority is adjusted
 * at the bounds, inodes for any frozen bound directories need to get auth_pins at that
 * time.
 *
 */

void CDir::auth_pin() 
{
  if (auth_pins == 0)
    get(PIN_AUTHPIN);
  auth_pins++;

  dout(7) << "auth_pin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;

  // nest pins?
  if (is_subtree_root()) return;  // no.
  //assert(!is_import());

  inode->nested_auth_pins++;
  if (inode->parent)
    inode->parent->dir->adjust_nested_auth_pins( 1 );
}

void CDir::auth_unpin() 
{
  auth_pins--;
  if (auth_pins == 0)
    put(PIN_AUTHPIN);

  dout(7) << "auth_unpin on " << *this << " count now " << auth_pins << " + " << nested_auth_pins << endl;
  assert(auth_pins >= 0);
  
  // pending freeze?
  if (auth_pins + nested_auth_pins == 0) 
    on_freezeable();
  
  // nest?
  if (is_subtree_root()) return;  // no.
  //assert(!is_import());

  inode->nested_auth_pins--;
  if (inode->parent)
    inode->parent->dir->adjust_nested_auth_pins( -1 );
}

void CDir::adjust_nested_auth_pins(int inc) 
{
  CDir *dir = this;
  
  // dir
  dir->nested_auth_pins += inc;
  
  dout(10) << "adjust_nested_auth_pins " << inc << " on " << *dir << " count now " << dir->auth_pins << " + " << dir->nested_auth_pins << endl;
  assert(dir->nested_auth_pins >= 0);
  
  // pending freeze?
  if (is_freezeable())
    dir->on_freezeable();
  // on freezeable_dir too?  FIXME
  
  // adjust my inode?
  if (dir->is_subtree_root()) 
    return; // no, stop.

  // yes.
  dir->inode->adjust_nested_auth_pins(inc);
}



/*****************************************************************************
 * FREEZING
 */

void CDir::on_freezeable()
{
  // check for anything pending freezeable

  /* NOTE: this will be called on deeper dirs first, walking up toward
     the root, meaning that deeper freeze attempts will succeed first.
  */
  /* NOTE: the first of these will likely freeze the dir, and unmark
     FREEZING.  additional ones will re-flag FREEZING.  this isn't
     particularly graceful, and might cause problems if the first one
     needs to know about other waiters.... FIXME? */
  
  finish_waiting(WAIT_FREEZEABLE);
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
    _freeze_tree(c);
  } else {
    state_set(STATE_FREEZINGTREE);
    dout(10) << "freeze_tree + wait " << *this << endl;
    
    // need to wait for auth pins to expire
    add_waiter(WAIT_FREEZEABLE, new C_MDS_FreezeTree(this, c));
  } 
}

void CDir::freeze_tree_finish(Context *c)
{
  // freezeable now?
  if (!is_freezeable()) {
    // wait again!
    dout(10) << "freeze_tree_finish still waiting " << *this << endl;
    state_set(STATE_FREEZINGTREE);
    add_waiter(WAIT_FREEZEABLE, new C_MDS_FreezeTree(this, c));
    return;
  }

  dout(10) << "freeze_tree_finish " << *this << endl;
  _freeze_tree(c);
}

void CDir::_freeze_tree(Context *c)
{
  dout(10) << "_freeze_tree " << *this << endl;

  // there shouldn't be any conflicting auth_pins.
  assert(is_freezeable_dir());

  // twiddle state
  state_clear(STATE_FREEZINGTREE);   // actually, this may get set again by next context?
  state_set(STATE_FROZENTREE);

  // auth_pin inode for duration of freeze, if we are not a subtree root.
  if (is_auth() && !is_subtree_root())
    inode->auth_pin();  
  
  // continue to frozen land
  if (c) {
    c->finish(0);
    delete c;
  }
}

void CDir::unfreeze_tree()
{
  dout(10) << "unfreeze_tree " << *this << endl;

  if (state_test(STATE_FROZENTREE)) {
    // frozen.  unfreeze.
    state_clear(STATE_FROZENTREE);

    // unpin  (may => FREEZEABLE)   FIXME: is this order good?
    if (is_auth() && !is_subtree_root())
      inode->auth_unpin();

    // waiters?
    finish_waiting(WAIT_UNFREEZE);
  } else {
    // freezing.  stop it.
    assert(state_test(STATE_FREEZINGTREE));
    state_clear(STATE_FREEZINGTREE);
    
    // cancel freeze waiters
    finish_waiting(WAIT_FREEZEABLE, -1);
  }
}

bool CDir::is_freezing_tree()
{
  CDir *dir = this;
  while (1) {
    if (dir->is_freezing_tree_root()) return true;
    if (dir->is_subtree_root()) return false;
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
    if (dir->is_subtree_root()) return false;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      return false;  // root on replica
  }
}

CDir *CDir::get_frozen_tree_root() 
{
  assert(is_frozen());
  CDir *dir = this;
  while (1) {
    if (dir->is_frozen_tree_root()) 
      return dir;
    if (dir->inode->parent)
      dir = dir->inode->parent->dir;
    else
      assert(0);
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
    _freeze_dir(c);
  } else {
    state_set(STATE_FREEZINGDIR);
    dout(10) << "freeze_dir + wait " << *this << endl;
    
    // need to wait for auth pins to expire
    add_waiter(WAIT_FREEZEABLE, new C_MDS_FreezeDir(this, c));
  } 
}

void CDir::_freeze_dir(Context *c)
{  
  dout(10) << "_freeze_dir " << *this << endl;

  state_set(STATE_FROZENDIR);

  if (is_auth() && !is_subtree_root())
    inode->auth_pin();  // auth_pin for duration of freeze
  
  if (c) {
    c->finish(0);
    delete c;
  }
}

void CDir::freeze_dir_finish(Context *c)
{
  // freezeable now?
  if (is_freezeable_dir()) {
    // freeze now
    _freeze_dir(c);
  } else {
    // wait again!
    dout(10) << "freeze_dir_finish still waiting " << *this << endl;
    state_set(STATE_FREEZINGDIR);
    add_waiter(WAIT_FREEZEABLE, new C_MDS_FreezeDir(this, c));
  }
}

void CDir::unfreeze_dir()
{
  dout(10) << "unfreeze_dir " << *this << endl;
  state_clear(STATE_FROZENDIR);
  
  // unpin  (may => FREEZEABLE)   FIXME: is this order good?
  if (is_auth() && !is_subtree_root())
    inode->auth_unpin();

  // waiters?
  finish_waiting(WAIT_UNFREEZE);
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

  if (!(state_test(STATE_COMPLETE)))
    dout(10) << ind << "..." << endl;
  if (state_test(STATE_DIRTY))
    dout(10) << ind << "[dirty]" << endl;

}

