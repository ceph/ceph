// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#include "Mutation.h"
#include "CInode.h"
#include "CDir.h"
#include "CDentry.h"

#include "messages/MClientRequest.h"

#define dout_subsys ceph_subsys_mds

// MutationImpl

void MutationImpl::pin(CObject *o)
{
  pins.insert(o);
}

void MutationImpl::unpin(CObject *o)
{
  pins.erase(o);
}

void MutationImpl::drop_pins()
{
  pins.clear();
}

void MutationImpl::add_projected_inode(CInode *in, bool early)
{
  if (early)
    projected_nodes[0].push_back(in);
  else
    projected_nodes[1].push_back(in);
}

void MutationImpl::add_projected_fnode(CDir *dir, bool early)
{
  if (early)
    projected_nodes[0].push_back(dir);
  else
    projected_nodes[1].push_back(dir);
}

void MutationImpl::add_updated_lock(CInode *in, int mask)
{
  updated_locks[in] |= mask;
}

CObject* MutationImpl::pop_early_projected_node()
{
  assert(!projected_nodes[0].empty());
  CObject *o = projected_nodes[0].front();
  projected_nodes[0].pop_front();
  return o;
}

void MutationImpl::pop_and_dirty_early_projected_nodes()
{
  for (auto p : projected_nodes[0]) {
    CInode *in = NULL;
    CDir *dir = NULL;
    if ((in = dynamic_cast<CInode*>(p))) {
      assert(is_object_locked(in));
      if (!in->is_base()) {
	CDentry *dn = in->get_parent_dn();
	assert(is_object_locked(dn->get_dir_inode()));
      }
    } else if ((dir = dynamic_cast<CDir*>(p))) {
      assert(is_object_locked(dir->get_inode()));
    }  else {
      assert(0);
    }
    if (in)
      in->pop_and_dirty_projected_inode(ls);
    else if (dir)
      dir->pop_and_dirty_projected_fnode(ls);

    auto q = updated_locks.find(in ? : dir->get_inode());
    if (q != updated_locks.end()) {
      q->first->mark_dirty_scattered(ls, q->second);
      updated_locks.erase(q);
    }
  }
  projected_nodes[0].clear();
}

void MutationImpl::pop_and_dirty_projected_nodes()
{
  for (auto p : projected_nodes[1]) {
    CInode *in = NULL;
    CDir *dir = NULL;
    if ((in = dynamic_cast<CInode*>(p))) {
      if (in->is_base()) {
	if (!is_object_locked(in)) {
	  unlock_all_objects();
	  lock_object(in);
	}
      } else {
	bool done = false;
	if (is_object_locked(in)) {
	  CDentry* dn = in->get_parent_dn();
	  if (is_object_locked(dn->get_dir_inode())) {
	    done = true;
	  } else if (dn->get_dir_inode()->mutex_trylock()) {
	    add_locked_object(dn->get_dir_inode());
	    done = true;
	  }
	}
	if (!done) {
	  unlock_all_objects();
	  CDentryRef dn = in->get_lock_parent_dn();
	  add_locked_object(dn->get_dir_inode());
	  lock_object(in);
	}
      }
    } else if ((dir = dynamic_cast<CDir*>(p))) {
      if (!is_object_locked(dir->get_inode())) {
	unlock_all_objects();
	dir->get_inode()->mutex_lock();
	add_locked_object(dir->get_inode());
      }
    }  else {
      assert(0);
    }

    if (in)
      in->pop_and_dirty_projected_inode(ls);
    else if (dir)
      dir->pop_and_dirty_projected_fnode(ls);

    auto q = updated_locks.find(in ? : dir->get_inode());
    if (q != updated_locks.end()) {
      q->first->mark_dirty_scattered(ls, q->second);
      updated_locks.erase(q);
    }
  }
  projected_nodes[1].clear();

  assert(updated_locks.empty());
  
  unlock_all_objects();
}

void MutationImpl::add_locked_object(CObject *o)
{
//  dout(10) << "add locked " << o << this << dendl;
  assert(!is_object_locked(o));
  locked_objects.insert(o);
}
void MutationImpl::remove_locked_object(CObject *o)
{
  auto p = locked_objects.find(o);
  assert(p != locked_objects.end());
  locked_objects.erase(p);
}

void MutationImpl::lock_object(CObject *o)
{
//  dout(10) << "lock " << o << " " <<this  <<dendl;
  assert(!is_object_locked(o));
  o->mutex_lock();
  locked_objects.insert(o);
}

void MutationImpl::unlock_object(CObject *o)
{
//  dout(10) << "unlock " << o << " " << this<< dendl;
  auto it = locked_objects.find(o);
  assert(it != locked_objects.end());
  o->mutex_unlock();
  locked_objects.erase(it);
}

void MutationImpl::unlock_all_objects()
{
  while (!locked_objects.empty()) {
    auto it = locked_objects.begin();
//  dout(10) << "unlock all " << *it << " " << this << dendl;
    (*it)->mutex_unlock();
    locked_objects.erase(it);
  }
}

void MutationImpl::start_locking(SimpleLock *lock, bool xlock)
{
  assert(locking == NULL);
  pin(lock->get_parent());
  locking = lock;
  locking_xlock = xlock;
}

void MutationImpl::finish_locking(SimpleLock *lock)
{
  assert(locking == lock);
  unpin(lock->get_parent());
  locking = NULL;
}

void MutationImpl::apply()
{
  pop_and_dirty_projected_nodes();
}

void MutationImpl::early_apply()
{
  pop_and_dirty_early_projected_nodes();
}

void MutationImpl::cleanup()
{
  unlock_all_objects();
  drop_pins();
}

const filepath& MDRequestImpl::get_filepath()
{
  return client_request->get_filepath();
}

const filepath& MDRequestImpl::get_filepath2()
{
  return client_request->get_filepath2();
}

// MDRequestImpl
MDRequestImpl::~MDRequestImpl()
{
  client_request->put();
}
