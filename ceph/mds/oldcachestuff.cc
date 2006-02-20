// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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



/*


OLD LOCK CRAP:
 (old):
  sync -  soft metadata.. no reads/writes can proceed.  (eg no stat)
  lock -  hard(+soft) metadata.. path traversals stop etc.  (??)


 replication consistency modes:
  hard+soft - hard and soft are defined on all replicas.
              all reads proceed (in absense of sync lock)
              writes require sync lock, fw to auth
   -> normal behavior.

  hard      - hard only, soft is undefined
              reads require a sync
              writes proceed if field updates are monotonic (e.g. size, m/c/atime)
   -> 'softasync'

 types of access by cache users:

   hard   soft
    R      -    read_hard_try       path traversal
    R  <=  R    read_soft_start     stat
    R  <=  W    write_soft_start    touch
    W  =>  W    write_hard_start    chmod

   note on those implications:
     read_soft_start() calls read_hard_try()
     write_soft_start() calls read_hard_try()
     a hard lock implies/subsumes a soft sync  (read_soft_start() returns true if a 
                      lock is held)


 relationship with frozen directories:

   read_hard_try - can proceed, because any hard changes require a lock, which 
      requires an active authority, which implies things are unfrozen.
   write_hard_start - waits (has to; only auth can initiate)
   read_soft_start  - ???? waits for now.  (FIXME: if !softasync & !syncbyauth)
   write_soft_start - ???? waits for now.  (FIXME: if (softasync & !syncbyauth))

   if sticky is on, an export_dir will drop any sync or lock so that the freeze will 
   proceed (otherwise, deadlock!).  likewise, a sync will not stick if is_freezing().
   


NAMESPACE:

 none right now.

 
*/


/* soft sync locks: mtime, size, etc. 
 */

bool MDCache::read_soft_start(CInode *in, Message *m)
{
  //  if (!read_hard_try(in, m))
  //	return false;

  // if frozen: i can't proceed (for now, see above)
  if (in->is_frozen()) {
	dout(7) << "read_soft_start " << *in << " is frozen, waiting" << endl;
	in->add_waiter(CDIR_WAIT_UNFREEZE,
				   new C_MDS_RetryMessage(mds, m));
	return false;
  }


  dout(5) << "read_soft_start " << *in << endl;

  // what soft sync mode?

  if (in->is_softasync()) {
	// softasync: hard consistency only

	if (in->is_auth()) {
	  // i am auth: i need sync
	  if (in->is_syncbyme()) goto yes;
	  if (in->is_lockbyme()) goto yes;   // lock => sync
	  if (!in->is_cached_by_anyone() &&
		  !in->is_open_write()) goto yes;  // i'm alone
	} else {
	  // i am replica: fw to auth
	  int auth = in->authority();
	  dout(5) << "read_soft_start " << *in << " is softasync, fw to auth " << auth << endl;
	  assert(auth != mds->get_nodeid());
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(auth), m->get_dest_port(),
								   MDS_PORT_CACHE);
	  return false;	  
	}
  } else {
	// normal: soft+hard consistency

	if (in->is_syncbyauth()) {
	  // wait for sync
	} else {
	  // i'm consistent 
	  goto yes;
	}
  }

  // we need sync
  if (in->is_syncbyauth() && !in->is_softasync()) {
    dout(5) << "read_soft_start " << *in << " is normal+replica+syncbyauth" << endl;
  } else if (in->is_softasync() && in->is_auth()) {
    dout(5) << "read_soft_start " << *in << " is softasync+auth, waiting on sync" << endl;
  } else 
	assert(2+2==5);

  if (!in->can_auth_pin()) {
	dout(5) << "read_soft_start " << *in << " waiting to auth_pin" << endl;
	in->add_waiter(CINODE_WAIT_AUTHPINNABLE,
				   new C_MDS_RetryMessage(mds,m));
	return false;
  }

  if (in->is_auth()) {
	// wait for sync
	in->add_waiter(CINODE_WAIT_SYNC,
				   new C_MDS_RetryMessage(mds, m));

	if (!in->is_presync())
	  inode_sync_start(in);
  } else {
	// wait for unsync
	in->add_waiter(CINODE_WAIT_UNSYNC,
				   new C_MDS_RetryMessage(mds, m));

	assert(in->is_syncbyauth());

	if (!in->is_waitonunsync())
	  inode_sync_wait(in);
  }
  
  return false;

 yes:
  mds->balancer->hit_inode(in, MDS_POP_SOFTRD);
  mds->balancer->hit_inode(in, MDS_POP_ANY);
  return true;
}


int MDCache::read_soft_finish(CInode *in)
{
  dout(5) << "read_soft_finish " << *in << endl;   // " soft_sync_count " << in->soft_sync_count << endl;
  return 0;  // do nothing, actually..
}


bool MDCache::write_soft_start(CInode *in, Message *m)
{
  //  if (!read_hard_try(in, m))
  //return false;

  // if frozen: i can't proceed (for now, see above)
  if (in->is_frozen()) {
	dout(7) << "read_soft_start " << *in << " is frozen, waiting" << endl;
	in->add_waiter(CDIR_WAIT_UNFREEZE,
				   new C_MDS_RetryMessage(mds, m));
	return false;
  }

  dout(5) << "write_soft_start " << *in << endl;
  // what soft sync mode?

  if (in->is_softasync()) {
	// softasync: hard consistency only

	if (in->is_syncbyauth()) {
	  // wait for sync release
	} else {
	  // i'm inconsistent; write away!
	  goto yes;
	}

  } else {
	// normal: soft+hard consistency
	
	if (in->is_auth()) {
	  // i am auth: i need sync
	  if (in->is_syncbyme()) goto yes;
	  if (in->is_lockbyme()) goto yes;   // lock => sync
	  if (!in->is_cached_by_anyone() &&
		  !in->is_open_write()) goto yes;  // i'm alone
	} else {
	  // i am replica: fw to auth
	  int auth = in->authority();
	  dout(5) << "write_soft_start " << *in << " is !softasync, fw to auth " << auth << endl;
	  assert(auth != mds->get_nodeid());
	  mds->messenger->send_message(m,
								   MSG_ADDR_MDS(auth), m->get_dest_port(),
								   MDS_PORT_CACHE);
	  return false;	  
	}
  }

  // we need sync
  if (in->is_syncbyauth() && in->is_softasync() && !in->is_auth()) {
    dout(5) << "write_soft_start " << *in << " is softasync+replica+syncbyauth" << endl;
  } else if (!in->is_softasync() && in->is_auth()) {
    dout(5) << "write_soft_start " << *in << " is normal+auth, waiting on sync" << endl;
  } else 
	assert(2+2==5);

  if (!in->can_auth_pin()) {
	dout(5) << "write_soft_start " << *in << " waiting to auth_pin" << endl;
	in->add_waiter(CINODE_WAIT_AUTHPINNABLE,
				   new C_MDS_RetryMessage(mds,m));
	return false;
  }

  if (in->is_auth()) {
	// wait for sync
	in->add_waiter(CINODE_WAIT_SYNC, 
				   new C_MDS_RetryMessage(mds, m));

	if (!in->is_presync())
	  inode_sync_start(in);
  } else {
	// wait for unsync
	in->add_waiter(CINODE_WAIT_UNSYNC, 
				   new C_MDS_RetryMessage(mds, m));

	assert(in->is_syncbyauth());
	assert(in->is_softasync());
	
	if (!in->is_waitonunsync())
	  inode_sync_wait(in);
  }
  
  return false;

 yes:
  mds->balancer->hit_inode(in, MDS_POP_SOFTWR);
  mds->balancer->hit_inode(in, MDS_POP_ANY);
  return true;
}


int MDCache::write_soft_finish(CInode *in)
{
  dout(5) << "write_soft_finish " << *in << endl;  //" soft_sync_count " << in->soft_sync_count << endl;
  return 0;  // do nothing, actually..
}








/* hard locks: owner, mode 
 */

/*
bool MDCache::read_hard_try(CInode *in,
							Message *m)
{
  //dout(5) << "read_hard_try " << *in << endl;
  
  if (in->is_auth()) {
	// auth
	goto yes;      // fine
  } else {
	// replica
	if (in->is_lockbyauth()) {
	  // locked by auth; wait!
	  dout(7) << "read_hard_try waiting on " << *in << endl;
	  in->add_waiter(CINODE_WAIT_UNLOCK, new C_MDS_RetryMessage(mds, m));
	  if (!in->is_waitonunlock())
		inode_lock_wait(in);
	  return false;
	} else {
	  // not locked.
	  goto yes;
	}
  }

 yes:
  mds->balancer->hit_inode(in, MDS_POP_HARDRD);
  mds->balancer->hit_inode(in, MDS_POP_ANY);
  return true;
}


bool MDCache::write_hard_start(CInode *in, 
							   Message *m)
{
  // if frozen: i can't proceed; only auth can initiate lock
  if (in->is_frozen()) {
	dout(7) << "write_hard_start " << *in << " is frozen, waiting" << endl;
	in->add_waiter(CDIR_WAIT_UNFREEZE,
				   new C_MDS_RetryMessage(mds, m));
	return false;
  }

  // NOTE: if freezing, and locked, we must proceed, to avoid deadlock (where
  // the freeze is waiting for our lock to be released)


  if (in->is_auth()) {
	// auth
	if (in->is_lockbyme()) goto success;
	if (!in->is_cached_by_anyone()) goto success;
	
	// need lock
	if (!in->can_auth_pin()) {
	  dout(5) << "write_hard_start " << *in << " waiting to auth_pin" << endl;
	  in->add_waiter(CINODE_WAIT_AUTHPINNABLE, new C_MDS_RetryMessage(mds, m));
	  return false;
	}
	
	in->add_waiter(CINODE_WAIT_LOCK, new C_MDS_RetryMessage(mds, m));
	
	if (!in->is_prelock())
	  inode_lock_start(in);
	
	return false;
  } else {
	// replica
	// fw to auth
	int auth = in->authority();
	dout(5) << "write_hard_start " << *in << " on replica, fw to auth " << auth << endl;
	assert(auth != mds->get_nodeid());
	mds->messenger->send_message(m,
								 MSG_ADDR_MDS(auth), m->get_dest_port(),
								 MDS_PORT_CACHE);
	return false;
  }

 success:
  in->lock_active_count++;
  dout(5) << "write_hard_start " << *in << " count now " << in->lock_active_count << endl;
  assert(in->lock_active_count > 0);

  mds->balancer->hit_inode(in, MDS_POP_HARDWR);
  mds->balancer->hit_inode(in, MDS_POP_ANY);
  return true;
}

void MDCache::write_hard_finish(CInode *in)
{
  in->lock_active_count--;
  dout(5) << "write_hard_finish " << *in << " count now " << in->lock_active_count << endl;
  assert(in->lock_active_count >= 0);

  // release lock?
  if (in->lock_active_count == 0 &&
	  in->is_lockbyme() &&
	  !g_conf.mdcache_sticky_lock) {
	dout(7) << "write_hard_finish " << *in << " !sticky, releasing lock immediately" << endl;
	inode_lock_release(in);
  }
}


void MDCache::inode_lock_start(CInode *in)
{
  dout(5) << "lock_start on " << *in << ", waiting for " << in->cached_by << endl;

  assert(in->is_auth());
  assert(!in->is_prelock());
  assert(!in->is_lockbyme());
  assert(!in->is_lockbyauth());

  in->lock_waiting_for_ack = in->cached_by;
  in->dist_state |= CINODE_DIST_PRELOCK;
  in->get(CINODE_PIN_PRELOCK);
  in->auth_pin();

  // send messages
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	mds->messenger->send_message(new MInodeLockStart(in->inode.ino, mds->get_nodeid()),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
}


void MDCache::inode_lock_release(CInode *in)
{
  dout(5) << "lock_release on " << *in << ", messages to " << in->get_cached_by() << endl;
  
  assert(in->is_lockbyme());
  assert(in->is_auth());

  in->dist_state &= ~CINODE_DIST_LOCKBYME;

  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	mds->messenger->send_message(new MInodeLockRelease(in),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  in->auth_unpin();
}

void MDCache::inode_lock_wait(CInode *in)
{
  dout(5) << "lock_wait on " << *in << endl;
  assert(!in->is_auth());
  assert(in->is_lockbyauth());
  
  in->dist_state |= CINODE_DIST_WAITONUNLOCK;
  in->get(CINODE_PIN_WAITONUNLOCK);
}


void MDCache::handle_inode_lock_start(MInodeLockStart *m)
{
  // authority is requesting a lock
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	// don't have it anymore!
	dout(7) << "handle_lock_start " << m->get_ino() << ": don't have it anymore, nak" << endl;
	mds->messenger->send_message(new MInodeLockAck(m->get_ino(), false),
								 MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	delete m; // done
	return;
  }
  
  // we shouldn't be authoritative...
  assert(!in->is_auth());
  
  dout(7) << "handle_lock_start " << *in << ", sending ack" << endl;
  
  // lock it
  in->dist_state |= CINODE_DIST_LOCKBYAUTH;

  // sanity check: make sure we know who _is_ authoritative! 
  assert(m->get_asker() == in->authority());
  
  // send ack
  mds->messenger->send_message(new MInodeLockAck(in->ino()),
							   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  delete m;  // done
}


void MDCache::handle_inode_lock_ack(MInodeLockAck *m)
{
  CInode *in = get_inode(m->get_ino());
  int from = m->get_source();
  dout(7) << "handle_lock_ack from " << from << " on " << *in << endl;

  assert(in);
  assert(in->is_auth());
  assert(in->dist_state & CINODE_DIST_PRELOCK);

  // remove it from waiting list
  in->lock_waiting_for_ack.erase(from);
  
  if (!m->did_have()) {
	// erase from cached_by too!
	in->cached_by_remove(from);
  }

  if (in->lock_waiting_for_ack.size()) {

	// more coming
	dout(7) << "handle_lock_ack " << *in << " from " << from << ", still waiting for " << in->lock_waiting_for_ack << endl;
	
  } else {
	
	// yay!
	dout(7) << "handle_lock_ack " << *in << " from " << from << ", last one" << endl;

	in->dist_state &= ~CINODE_DIST_PRELOCK;
	in->dist_state |= CINODE_DIST_LOCKBYME;
	in->put(CINODE_PIN_PRELOCK);

	// do waiters!
	in->finish_waiting(CINODE_WAIT_LOCK);
  }

  delete m; // done
}


void MDCache::handle_inode_lock_release(MInodeLockRelease *m)
{
  CInode *in = get_inode(m->get_ino());

  if (!in) {
	dout(7) << "handle_lock_release " << m->get_ino() << ", don't have it, dropping" << endl;
	delete m;  // done
	return;
  }
  
  if (!in->is_lockbyauth()) {
	dout(7) << "handle_lock_release " << m->get_ino() << ", not flagged as locked, wtf" << endl;
	assert(0);   // i should have it, locked, or not have it at all!
	delete m;  // done
	return;
  }
  
  dout(7) << "handle_lock_release " << *in << endl;
  assert(!in->is_auth());
  
  // release state
  in->dist_state &= ~CINODE_DIST_LOCKBYAUTH;

  // waiters?
  if (in->is_waitonunlock()) {
	in->put(CINODE_PIN_WAITONUNLOCK);
	in->dist_state &= ~CINODE_DIST_WAITONUNLOCK;
	
	// finish
	in->finish_waiting(CINODE_WAIT_UNLOCK);
  }
  
  // done
  delete m;
}
*/









// sync interface

void MDCache::inode_sync_wait(CInode *in)
{
  assert(!in->is_auth());
  
  int auth = in->authority();
  dout(5) << "inode_sync_wait on " << *in << ", auth " << auth << endl;
  
  assert(in->is_syncbyauth());
  assert(!in->is_waitonunsync());
  
  in->dist_state |= CINODE_DIST_WAITONUNSYNC;
  in->get(CINODE_PIN_WAITONUNSYNC);
  
  if ((in->is_softasync() && g_conf.mdcache_sticky_sync_softasync) ||
	  (!in->is_softasync() && g_conf.mdcache_sticky_sync_normal)) {
	// actually recall; if !sticky, auth will immediately release.
	dout(5) << "inode_sync_wait on " << *in << " sticky, recalling from auth" << endl;
	mds->messenger->send_message(new MInodeSyncRecall(in->inode.ino),
								 MSG_ADDR_MDS(auth), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
}


void MDCache::inode_sync_start(CInode *in)
{
  // wait for all replicas
  dout(5) << "inode_sync_start on " << *in << ", waiting for " << in->cached_by << " " << in->get_open_write()<< endl;

  assert(in->is_auth());
  assert(!in->is_presync());
  assert(!in->is_sync());

  in->sync_waiting_for_ack.clear();
  in->dist_state |= CINODE_DIST_PRESYNC;
  in->get(CINODE_PIN_PRESYNC);
  in->auth_pin();
  
  in->sync_replicawantback = false;

  // send messages
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	in->sync_waiting_for_ack.insert(MSG_ADDR_MDS(*it));
	mds->messenger->send_message(new MInodeSyncStart(in->inode.ino, mds->get_nodeid()),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }

  // sync clients
  int last = -1;
  for (multiset<int>::iterator it = in->get_open_write().begin();
	   it != in->get_open_write().end();
	   it++) {
	if (*it == last) continue;  last = *it;   // only 1 per client (even if open multiple times)
	in->sync_waiting_for_ack.insert(MSG_ADDR_CLIENT(*it));
	mds->messenger->send_message(new MInodeSyncStart(in->ino(), mds->get_nodeid()),
								 MSG_ADDR_CLIENT(*it), 0,
								 MDS_PORT_CACHE);
  }

}

void MDCache::inode_sync_release(CInode *in)
{
  dout(5) << "inode_sync_release on " << *in << ", messages to " << in->get_cached_by() << " " << in->get_open_write() << endl;
  
  assert(in->is_syncbyme());
  assert(in->is_auth());

  in->dist_state &= ~CINODE_DIST_SYNCBYME;

  // release replicas
  for (set<int>::iterator it = in->cached_by_begin(); 
	   it != in->cached_by_end(); 
	   it++) {
	mds->messenger->send_message(new MInodeSyncRelease(in),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
  
  // release writers
  for (multiset<int>::iterator it = in->get_open_write().begin();
	   it != in->get_open_write().end();
	   it++) {
	mds->messenger->send_message(new MInodeSyncRelease(in),
								 MSG_ADDR_CLIENT(*it), 0,
								 MDS_PORT_CACHE);
  }

  in->auth_unpin();
}




// messages
void MDCache::handle_inode_sync_start(MInodeSyncStart *m)
{
  // assume asker == authority for now.
  
  // authority is requesting a lock
  CInode *in = get_inode(m->get_ino());
  if (!in) {
	// don't have it anymore!
	dout(7) << "handle_sync_start " << m->get_ino() << ": don't have it anymore, nak" << endl;
	mds->messenger->send_message(new MInodeSyncAck(m->get_ino(), false),
								 MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
	delete m; // done
	return;
  }
  
  dout(10) << "handle_sync_start " << *in << endl;

  // we shouldn't be authoritative...
  assert(!in->is_auth());
  
  // sanity check: make sure we know who _is_ authoritative! 
  assert(m->get_asker() == in->authority());

  // lock it
  in->dist_state |= CINODE_DIST_SYNCBYAUTH;

  // open for write by clients?
  if (in->is_open_write()) {
	dout(7) << "handle_sync_start " << *in << " syncing write clients " << in->get_open_write() << endl;
	
	// sync clients
	in->sync_waiting_for_ack.clear();
	for (multiset<int>::iterator it = in->get_open_write().begin();
		 it != in->get_open_write().end();
		 it++) {
	  in->sync_waiting_for_ack.insert(MSG_ADDR_CLIENT(*it));
	  mds->messenger->send_message(new MInodeSyncStart(in->ino(), mds->get_nodeid()),
								   MSG_ADDR_CLIENT(*it), 0,
								   MDS_PORT_CACHE);
	}

	in->pending_sync_request = m;	
  } else {
	// no writers, ack.
	dout(7) << "handle_sync_start " << *in << ", sending ack" << endl;
  
	inode_sync_ack(in, m);
  }
}

void MDCache::inode_sync_ack(CInode *in, MInodeSyncStart *m, bool wantback)
{
  dout(7) << "sending inode_sync_ack " << *in << endl;
    
  // send ack
  mds->messenger->send_message(new MInodeSyncAck(in->ino(), true, wantback),
							   MSG_ADDR_MDS(m->get_asker()), MDS_PORT_CACHE,
							   MDS_PORT_CACHE);

  delete m;
}

void MDCache::handle_inode_sync_ack(MInodeSyncAck *m)
{
  CInode *in = get_inode(m->get_ino());
  assert(in);

  dout(7) << "handle_sync_ack " << *in << " from " << m->get_source() << endl;

  if (in->is_auth()) {
	assert(in->is_presync());
  } else {
	assert(in->is_syncbyauth());
	assert(in->pending_sync_request);
  }

  // remove it from waiting list
  in->sync_waiting_for_ack.erase(m->get_source());
  
  if (MSG_ADDR_ISCLIENT(m->get_source()) && !m->did_have()) {
	// erase from cached_by too!
	in->cached_by_remove(m->get_source());
  }

  if (m->replica_wantsback())
	in->sync_replicawantback = true;

  if (in->sync_waiting_for_ack.size()) {

	// more coming
	dout(7) << "handle_sync_ack " << *in << " from " << m->get_source() << ", still waiting for " << in->sync_waiting_for_ack << endl;
	
  } else {
	
	// yay!
	dout(7) << "handle_sync_ack " << *in << " from " << m->get_source() << ", last one" << endl;

	if (!in->is_auth()) {
	  // replica, sync ack back to auth
	  assert(in->pending_sync_request);
	  inode_sync_ack(in, in->pending_sync_request, true);
	  in->pending_sync_request = 0;
	  delete m;
	  return;
	}

	in->dist_state &= ~CINODE_DIST_PRESYNC;
	in->dist_state |= CINODE_DIST_SYNCBYME;
	in->put(CINODE_PIN_PRESYNC);

	// do waiters!
	in->finish_waiting(CINODE_WAIT_SYNC);


	// release sync right away?
	if (in->is_syncbyme()) {
	  if (in->is_freezing()) {
		dout(7) << "handle_sync_ack freezing " << *in << ", dropping sync immediately" << endl;
		inode_sync_release(in);
	  } 
	  else if (in->sync_replicawantback) {
		dout(7) << "handle_sync_ack replica wantback, releasing sync immediately" << endl;
		inode_sync_release(in);
	  }
	  else if ((in->is_softasync() && !g_conf.mdcache_sticky_sync_softasync) ||
			   (!in->is_softasync() && !g_conf.mdcache_sticky_sync_normal)) {
		dout(7) << "handle_sync_ack !sticky, releasing sync immediately" << endl;
		inode_sync_release(in);
	  } 
	  else {
		dout(7) << "handle_sync_ack sticky sync is on, keeping sync for now" << endl;
	  }
	} else {
	  dout(7) << "handle_sync_ack don't have sync anymore, something must have just released it?" << endl;
	}
  }

  delete m; // done
}


void MDCache::handle_inode_sync_release(MInodeSyncRelease *m)
{
  CInode *in = get_inode(m->get_ino());

  if (!in) {
	dout(7) << "handle_sync_release " << m->get_ino() << ", don't have it, dropping" << endl;
	delete m;  // done
	return;
  }
  
  if (!in->is_syncbyauth()) {
	dout(7) << "handle_sync_release " << *in << ", not flagged as sync" << endl;
	assert(0);  // this shouldn't happen.
	delete m;  // done
	return;
  }
  
  dout(7) << "handle_sync_release " << *in << endl;
  assert(!in->is_auth());
  
  // release state
  in->dist_state &= ~CINODE_DIST_SYNCBYAUTH;

  // waiters?
  if (in->is_waitonunsync()) {
	in->put(CINODE_PIN_WAITONUNSYNC);
	in->dist_state &= ~CINODE_DIST_WAITONUNSYNC;

	// finish
	in->finish_waiting(CINODE_WAIT_UNSYNC);
  }

  // client readers?
  if (in->is_open_write()) {
	dout(7) << "handle_sync_release releasing clients " << in->get_open_write() << endl;
	for (multiset<int>::iterator it = in->get_open_write().begin();
		 it != in->get_open_write().end();
		 it++) {
	  mds->messenger->send_message(new MInodeSyncRelease(in),
								   MSG_ADDR_CLIENT(*it), 0,
								   MDS_PORT_CACHE);
	}
  }

  
  // done
  delete m;
}


void MDCache::handle_inode_sync_recall(MInodeSyncRecall *m)
{
  CInode *in = get_inode(m->get_ino());

  if (!in) {
	dout(7) << "handle_sync_recall " << m->get_ino() << ", don't have it, wtf" << endl;
	assert(0); // shouldn't happen
	delete m;  // done
	return;
  }
  if(!in->is_auth()) {
	do_ino_proxy(in, m);
	return;
  }
  
  if (in->is_syncbyme()) {
	dout(7) << "handle_sync_recall " << *in << ", releasing" << endl;
	inode_sync_release(in);
  }
  else if (in->is_presync()) {
	dout(7) << "handle_sync_recall " << *in << " is presync, flagging" << endl;
	in->sync_replicawantback = true;
  }
  else {
	dout(7) << "handle_sync_recall " << m->get_ino() << ", not flagged as sync or presync, dropping" << endl;
  }
  
  // done
  delete m;
}










// DIR SYNC

/*

 dir sync

 - this are used when a directory is HASHED only.  namely,
   - to stat the dir inode we need an accurate directory size  (????)
   - for a readdir 

*/

void MDCache::dir_sync_start(CDir *dir)
{
  // wait for all replicas
  dout(5) << "sync_start on " << *dir << endl;

  assert(dir->is_hashed());
  assert(dir->is_auth());
  assert(!dir->is_presync());
  assert(!dir->is_sync());

  dir->sync_waiting_for_ack = mds->get_cluster()->get_mds_set();
  dir->state_set(CDIR_STATE_PRESYNC);
  dir->auth_pin();
  
  //dir->sync_replicawantback = false;

  // send messages
  for (set<int>::iterator it = dir->sync_waiting_for_ack.begin();
	   it != dir->sync_waiting_for_ack.end();
	   it++) {
	mds->messenger->send_message(new MDirSyncStart(dir->ino(), mds->get_nodeid()),
								 MSG_ADDR_MDS(*it), MDS_PORT_CACHE,
								 MDS_PORT_CACHE);
  }
}


void MDCache::dir_sync_release(CDir *dir)
{


}

void MDCache::dir_sync_wait(CDir *dir)
{

}


void handle_dir_sync_start(MDirSyncStart *m)
{
}




