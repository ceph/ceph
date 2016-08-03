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
  if (pins.count(o) == 0) {
    o->get(CObject::PIN_REQUEST);
    pins.insert(o);
  }      
}

void MutationImpl::unpin(CObject *o)
{
  assert(pins.count(o));
  o->put(CObject::PIN_REQUEST);
  pins.erase(o);
}

void MutationImpl::drop_pins()
{
  for (set<CObject*>::iterator it = pins.begin();
       it != pins.end();
       ++it) 
    (*it)->put(CObject::PIN_REQUEST);
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
  }
  projected_nodes[1].clear();
  
  unlock_all_objects();
}

void MutationImpl::add_locked_object(CObject *o)
{
//  dout(10) << "add locked " << o << dendl;
  assert(!is_object_locked(o));
  locked_objects.insert(o);
}

void MutationImpl::lock_object(CObject *o)
{
//  dout(10) << "lock " << o << dendl;
  assert(!is_object_locked(o));
  o->mutex_lock();
  locked_objects.insert(o);
}

void MutationImpl::unlock_object(CObject *o)
{
//  dout(10) << "unlock " << o << dendl;
  auto it = locked_objects.find(o);
  assert(it != locked_objects.end());
  o->mutex_unlock();
  locked_objects.erase(it);
}

void MutationImpl::unlock_all_objects()
{
//  dout(10) << "unlock all " << dendl;
  while (!locked_objects.empty()) {
    auto it = locked_objects.begin();
    (*it)->mutex_unlock();
    locked_objects.erase(it);
  }
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
