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

bool MDRequestImpl::has_witnesses()
{
  return _more && !_more->witnessed.empty();
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

const filepath& MDRequestImpl::get_filepath()
{
  if (client_request)
    return client_request->get_filepath();
  return more()->filepath1;
}

const filepath& MDRequestImpl::get_filepath2()
{
  if (client_request)
    return client_request->get_filepath2();
  return more()->filepath2;
}

void MDRequestImpl::set_filepath(const filepath& fp)
{
  assert(!client_request);
  more()->filepath1 = fp;
}
void MDRequestImpl::set_filepath2(const filepath& fp)
{
  assert(!client_request);
  more()->filepath2 = fp;
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

void MDRequestImpl::dump(Formatter *f) const
{
  _dump(ceph_clock_now(g_ceph_context), f);
}

void MDRequestImpl::_dump(utime_t now, Formatter *f) const
{
  f->dump_string("flag_point", state_string());
  f->dump_stream("reqid") << reqid;
  {
    if (client_request) {
      f->dump_string("op_type", "client_request");
      f->open_object_section("client_info");
      f->dump_stream("client") << client_request->get_orig_source();
      f->dump_int("tid", client_request->get_tid());
      f->close_section(); // client_info
    } else if (is_slave() && slave_request) { // replies go to an existing mdr
      f->dump_string("op_type", "slave_request");
      f->open_object_section("master_info");
      f->dump_stream("master") << slave_request->get_orig_source();
      f->close_section(); // master_info

      f->open_object_section("request_info");
      f->dump_int("attempt", slave_request->get_attempt());
      f->dump_string("op_type",
                     slave_request->get_opname(slave_request->get_op()));
      f->dump_int("lock_type", slave_request->get_lock_type());
      f->dump_stream("object_info") << slave_request->get_object_info();
      f->dump_stream("srcdnpath") << slave_request->srcdnpath;
      f->dump_stream("destdnpath") << slave_request->destdnpath;
      f->dump_stream("witnesses") << slave_request->witnesses;
      f->dump_bool("has_inode_export",
                   slave_request->inode_export.length() != 0);
      f->dump_int("inode_export_v", slave_request->inode_export_v);
      f->dump_bool("has_srci_replica",
                   slave_request->srci_replica.length() != 0);
      f->dump_stream("op_stamp") << slave_request->op_stamp;
      f->close_section(); // request_info
    }
    else if (internal_op != -1) { // internal request
      f->dump_string("op_type", "internal_op");
      f->dump_int("internal_op", internal_op);
      f->dump_string("op_name", ceph_mds_op_name(internal_op));
    }
    else {
      f->dump_string("op_type", "no_available_op_found");
    }
  }
  {
    f->open_array_section("events");
    Mutex::Locker l(lock);
    for (list<pair<utime_t, string> >::const_iterator i = events.begin();
         i != events.end();
         ++i) {
      f->open_object_section("event");
      f->dump_stream("time") << i->first;
      f->dump_string("event", i->second);
      f->close_section();
    }
    f->close_section(); // events
  }
}

void MDRequestImpl::_dump_op_descriptor_unlocked(ostream& stream) const
{
  if (client_request) {
    client_request->print(stream);
  } else if (slave_request) {
    slave_request->print(stream);
  } else if (internal_op >= 0) {
    stream << "internal op " << ceph_mds_op_name(internal_op) << ":" << reqid;
  } else {
    // drat, it's triggered by a slave request, but we don't have a message
    // FIXME
    stream << "rejoin:" << reqid;
  }
}
