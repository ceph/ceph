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
#include "ScatterLock.h"
#include "CDir.h"

#include "messages/MClientRequest.h"
#include "messages/MMDSSlaveRequest.h"


// MutationImpl

void MutationImpl::pin(MDSCacheObject *o)
{
  if (pins.count(o) == 0) {
    o->get(MDSCacheObject::PIN_REQUEST);
    pins.insert(o);
  }      
}

void MutationImpl::unpin(MDSCacheObject *o)
{
  assert(pins.count(o));
  o->put(MDSCacheObject::PIN_REQUEST);
  pins.erase(o);
}

void MutationImpl::set_stickydirs(CInode *in)
{
  if (stickydirs.count(in) == 0) {
    in->get_stickydirs();
    stickydirs.insert(in);
  }
}

void MutationImpl::drop_pins()
{
  for (set<MDSCacheObject*>::iterator it = pins.begin();
       it != pins.end();
       ++it) 
    (*it)->put(MDSCacheObject::PIN_REQUEST);
  pins.clear();
}

void MutationImpl::start_locking(SimpleLock *lock, int target)
{
  assert(locking == NULL);
  pin(lock->get_parent());
  locking = lock;
  locking_target_mds = target;
}

void MutationImpl::finish_locking(SimpleLock *lock)
{
  assert(locking == lock);
  locking = NULL;
  locking_target_mds = -1;
}


// auth pins
bool MutationImpl::is_auth_pinned(MDSCacheObject *object)
{ 
  return auth_pins.count(object) || remote_auth_pins.count(object); 
}

void MutationImpl::auth_pin(MDSCacheObject *object)
{
  if (!is_auth_pinned(object)) {
    object->auth_pin(this);
    auth_pins.insert(object);
  }
}

void MutationImpl::auth_unpin(MDSCacheObject *object)
{
  assert(auth_pins.count(object));
  object->auth_unpin(this);
  auth_pins.erase(object);
}

void MutationImpl::drop_local_auth_pins()
{
  for (set<MDSCacheObject*>::iterator it = auth_pins.begin();
       it != auth_pins.end();
       ++it) {
    assert((*it)->is_auth());
    (*it)->auth_unpin(this);
  }
  auth_pins.clear();
}

void MutationImpl::add_projected_inode(CInode *in)
{
  projected_inodes.push_back(in);
}

void MutationImpl::pop_and_dirty_projected_inodes()
{
  while (!projected_inodes.empty()) {
    CInode *in = projected_inodes.front();
    projected_inodes.pop_front();
    in->pop_and_dirty_projected_inode(ls);
  }
}

void MutationImpl::add_projected_fnode(CDir *dir)
{
  projected_fnodes.push_back(dir);
}

void MutationImpl::pop_and_dirty_projected_fnodes()
{
  while (!projected_fnodes.empty()) {
    CDir *dir = projected_fnodes.front();
    projected_fnodes.pop_front();
    dir->pop_and_dirty_projected_fnode(ls);
  }
}

void MutationImpl::add_updated_lock(ScatterLock *lock)
{
  updated_locks.push_back(lock);
}

void MutationImpl::add_cow_inode(CInode *in)
{
  pin(in);
  dirty_cow_inodes.push_back(in);
}

void MutationImpl::add_cow_dentry(CDentry *dn)
{
  pin(dn);
  dirty_cow_dentries.push_back(pair<CDentry*,version_t>(dn, dn->get_projected_version()));
}

void MutationImpl::apply()
{
  pop_and_dirty_projected_inodes();
  pop_and_dirty_projected_fnodes();
  
  for (list<CInode*>::iterator p = dirty_cow_inodes.begin();
       p != dirty_cow_inodes.end();
       ++p) 
    (*p)->_mark_dirty(ls);
  for (list<pair<CDentry*,version_t> >::iterator p = dirty_cow_dentries.begin();
       p != dirty_cow_dentries.end();
       ++p)
    p->first->mark_dirty(p->second, ls);
  
  for (list<ScatterLock*>::iterator p = updated_locks.begin();
       p != updated_locks.end();
       ++p)
    (*p)->mark_dirty();
}

void MutationImpl::cleanup()
{
  drop_local_auth_pins();
  drop_pins();
}


// MDRequestImpl

MDRequestImpl::~MDRequestImpl()
{
  if (client_request)
    client_request->put();
  if (slave_request)
    slave_request->put();
  delete _more;
}

MDRequestImpl::More* MDRequestImpl::more()
{ 
  if (!_more)
    _more = new More();
  return _more;
}

bool MDRequestImpl::has_more()
{
  return _more;
}

bool MDRequestImpl::are_slaves()
{
  return _more && !_more->slaves.empty();
}

bool MDRequestImpl::slave_did_prepare()
{
  return more()->slave_commit;
}

bool MDRequestImpl::did_ino_allocation()
{
  return alloc_ino || used_prealloc_ino || prealloc_inos.size();
}      

bool MDRequestImpl::freeze_auth_pin(CInode *inode)
{
  assert(!more()->rename_inode || more()->rename_inode == inode);
  more()->rename_inode = inode;
  more()->is_freeze_authpin = true;
  auth_pin(inode);
  if (!inode->freeze_inode(1)) {
    return false;
  }
  inode->freeze_auth_pin();
  inode->unfreeze_inode();
  return true;
}

void MDRequestImpl::unfreeze_auth_pin(bool clear_inode)
{
  assert(more()->is_freeze_authpin);
  CInode *inode = more()->rename_inode;
  if (inode->is_frozen_auth_pin())
    inode->unfreeze_auth_pin();
  else
    inode->unfreeze_inode();
  more()->is_freeze_authpin = false;
  if (clear_inode)
    more()->rename_inode = NULL;
}

void MDRequestImpl::set_remote_frozen_auth_pin(CInode *inode)
{
  assert(!more()->rename_inode || more()->rename_inode == inode);
  more()->rename_inode = inode;
  more()->is_remote_frozen_authpin = true;
}

void MDRequestImpl::set_ambiguous_auth(CInode *inode)
{
  assert(!more()->rename_inode || more()->rename_inode == inode);
  assert(!more()->is_ambiguous_auth);

  inode->set_ambiguous_auth();
  more()->rename_inode = inode;
  more()->is_ambiguous_auth = true;
}

void MDRequestImpl::clear_ambiguous_auth()
{
  CInode *inode = more()->rename_inode;
  assert(inode && more()->is_ambiguous_auth);
  inode->clear_ambiguous_auth();
  more()->is_ambiguous_auth = false;
}

bool MDRequestImpl::can_auth_pin(MDSCacheObject *object)
{
  return object->can_auth_pin() ||
         (is_auth_pinned(object) && has_more() &&
	  more()->is_freeze_authpin &&
	  more()->rename_inode == object);
}

void MDRequestImpl::drop_local_auth_pins()
{
  if (has_more() && more()->is_freeze_authpin)
    unfreeze_auth_pin(true);
  MutationImpl::drop_local_auth_pins();
}

void MDRequestImpl::print(ostream &out)
{
  out << "request(" << reqid;
  //if (request) out << " " << *request;
  if (is_slave()) out << " slave_to mds." << slave_to_mds;
  if (client_request) out << " cr=" << client_request;
  if (slave_request) out << " sr=" << slave_request;
  out << ")";
}

