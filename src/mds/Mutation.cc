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
#include "CInode.h"
#include "CDir.h"

using namespace std;

// MutationImpl

void MutationImpl::pin(MDSCacheObject *o)
{
  auto& stat = object_states[o];
  if (!stat.pinned) {
    o->get(MDSCacheObject::PIN_REQUEST);
    stat.pinned = true;
    ++num_pins;
  }      
}

void MutationImpl::unpin(MDSCacheObject *o)
{
  auto& stat = object_states[o];
  ceph_assert(stat.pinned);
  o->put(MDSCacheObject::PIN_REQUEST);
  stat.pinned = false;
  --num_pins;
}

void MutationImpl::set_stickydirs(CInode *in)
{
  if (!stickydiri || stickydiri != in) {
    in->get_stickydirs();
    if (stickydiri)
      stickydiri->put_stickydirs();
    stickydiri = in;
  }
}

void MutationImpl::put_stickydirs()
{
  if (stickydiri) {
    stickydiri->put_stickydirs();
    stickydiri = nullptr;

  }
}

void MutationImpl::drop_pins()
{
  for (auto& p : object_states) {
    if (p.second.pinned) {
      p.first->put(MDSCacheObject::PIN_REQUEST);
      p.second.pinned = false;
      --num_pins;
    }
  }
}

void MutationImpl::start_locking(SimpleLock *lock, int target)
{
  ceph_assert(locking == NULL);
  pin(lock->get_parent());
  locking = lock;
  locking_target_mds = target;
}

void MutationImpl::finish_locking(SimpleLock *lock)
{
  ceph_assert(locking == lock);
  locking = NULL;
  locking_target_mds = -1;
}

bool MutationImpl::is_rdlocked(SimpleLock *lock) const {
  auto it = locks.find(lock);
  if (it != locks.end() && it->is_rdlock())
    return true;
  if (lock_cache)
    return static_cast<const MutationImpl*>(lock_cache)->is_rdlocked(lock);
  return false;
}

bool MutationImpl::is_wrlocked(SimpleLock *lock) const {
  auto it = locks.find(lock);
  if (it != locks.end() && it->is_wrlock())
    return true;
  if (lock_cache)
    return static_cast<const MutationImpl*>(lock_cache)->is_wrlocked(lock);
  return false;
}

void MutationImpl::LockOpVec::erase_rdlock(SimpleLock* lock)
{
  for (int i = size() - 1; i >= 0; --i) {
    auto& op = (*this)[i];
    if (op.lock == lock && op.is_rdlock()) {
      erase(begin() + i);
      return;
    }
  }
}
void MutationImpl::LockOpVec::sort_and_merge()
{
  // sort locks on the same object
  auto cmp = [](const LockOp &l, const LockOp &r) {
    ceph_assert(l.lock->get_parent() == r.lock->get_parent());
    return l.lock->type->type < r.lock->type->type;
  };
  for (auto i = begin(), j = i; ; ++i) {
    if (i == end()) {
      std::sort(j, i, cmp);
      break;
    }
    if (j->lock->get_parent() != i->lock->get_parent()) {
      std::sort(j, i, cmp);
      j = i;
    }
  }
  // merge ops on the same lock
  for (auto i = end() - 1; i > begin(); ) {
    auto j = i;
    while (--j >= begin()) {
      if (i->lock != j->lock)
	break;
    }
    if (i - j == 1) {
      i = j;
      continue;
    }
    // merge
    ++j;
    for (auto k = i; k > j; --k) {
      if (k->is_remote_wrlock()) {
	ceph_assert(!j->is_remote_wrlock());
	j->wrlock_target = k->wrlock_target;
      }
      j->flags |= k->flags;
    }
    if (j->is_xlock()) {
      // xlock overwrites other types
      ceph_assert(!j->is_remote_wrlock());
      j->flags = LockOp::XLOCK;
    }
    erase(j + 1, i + 1);
    i = j - 1;
  }
}

// auth pins
bool MutationImpl::is_auth_pinned(MDSCacheObject *object) const
{ 
  auto stat_p = find_object_state(object);
  if (!stat_p)
    return false;
  return stat_p->auth_pinned || stat_p->remote_auth_pinned != MDS_RANK_NONE;
}

void MutationImpl::auth_pin(MDSCacheObject *object)
{
  auto &stat = object_states[object];
  if (!stat.auth_pinned) {
    object->auth_pin(this);
    stat.auth_pinned = true;
    ++num_auth_pins;
  }
}

void MutationImpl::auth_unpin(MDSCacheObject *object)
{
  auto &stat = object_states[object];
  ceph_assert(stat.auth_pinned);
  object->auth_unpin(this);
  stat.auth_pinned = false;
  --num_auth_pins;
}

void MutationImpl::drop_local_auth_pins()
{
  for (auto& p : object_states) {
    if (p.second.auth_pinned) {
      ceph_assert(p.first->is_auth());
      p.first->auth_unpin(this);
      p.second.auth_pinned = false;
      --num_auth_pins;
    }
  }
}

void MutationImpl::set_remote_auth_pinned(MDSCacheObject *object, mds_rank_t from)
{
  auto &stat = object_states[object];
  if (stat.remote_auth_pinned == MDS_RANK_NONE) {
    stat.remote_auth_pinned = from;
    ++num_remote_auth_pins;
  } else {
    ceph_assert(stat.remote_auth_pinned == from);
  }
}

void MutationImpl::_clear_remote_auth_pinned(ObjectState &stat)
{
  ceph_assert(stat.remote_auth_pinned != MDS_RANK_NONE);
  stat.remote_auth_pinned = MDS_RANK_NONE;
  --num_remote_auth_pins;
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
  dirty_cow_dentries.emplace_back(dn, dn->get_projected_version());
}

void MutationImpl::apply()
{
  for (auto& obj : projected_nodes) {
    if (CInode *in = dynamic_cast<CInode*>(obj))
      in->pop_and_dirty_projected_inode(ls, nullptr);
  }

  for (const auto& in : dirty_cow_inodes) {
    in->_mark_dirty(ls);
  }

  for (const auto& [dn, v] : dirty_cow_dentries) {
    dn->mark_dirty(v, ls);
  }

  for (auto& obj : projected_nodes) {
    if (CDir *dir = dynamic_cast<CDir*>(obj))
      dir->pop_and_dirty_projected_fnode(ls, nullptr);
  }

  for (const auto& lock : updated_locks) {
    lock->mark_dirty();
  }

  projected_nodes.clear();
}

void MutationImpl::cleanup()
{
  drop_local_auth_pins();
  drop_pins();
}

void MutationImpl::_dump_op_descriptor(ostream& stream) const
{
  stream << "Mutation";
}

// MDRequestImpl

MDRequestImpl::~MDRequestImpl()
{
  delete _more;
}

MDRequestImpl::More* MDRequestImpl::more()
{ 
  if (!_more)
    _more = new More();
  return _more;
}

MDRequestImpl::More const* MDRequestImpl::more() const
{
  ceph_assert(_more);
  return _more;
}

bool MDRequestImpl::has_more() const
{
  return _more != nullptr;
}

bool MDRequestImpl::has_witnesses()
{
  return (_more != nullptr) && (!_more->witnessed.empty());
}

bool MDRequestImpl::peer_did_prepare()
{
  return has_more() && more()->peer_commit;
}

bool MDRequestImpl::peer_rolling_back()
{
  return has_more() && more()->peer_rolling_back;
}

bool MDRequestImpl::freeze_auth_pin(CInode *inode)
{
  ceph_assert(!more()->rename_inode || more()->rename_inode == inode);
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
  ceph_assert(more()->is_freeze_authpin);
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
  ceph_assert(!more()->rename_inode || more()->rename_inode == inode);
  ceph_assert(!more()->is_ambiguous_auth);

  inode->set_ambiguous_auth();
  more()->rename_inode = inode;
  more()->is_ambiguous_auth = true;
}

void MDRequestImpl::clear_ambiguous_auth()
{
  CInode *inode = more()->rename_inode;
  ceph_assert(inode && more()->is_ambiguous_auth);
  inode->clear_ambiguous_auth();
  more()->is_ambiguous_auth = false;
}

bool MDRequestImpl::can_auth_pin(MDSCacheObject *object, bool bypassfreezing)
{
  return object->can_auth_pin(nullptr, bypassfreezing) ||
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

const filepath& MDRequestImpl::get_filepath() const
{
  if (client_request)
    return client_request->get_filepath();
  return more()->filepath1;
}

const filepath& MDRequestImpl::get_filepath2() const
{
  if (client_request)
    return client_request->get_filepath2();
  return more()->filepath2;
}

void MDRequestImpl::set_filepath(const filepath& fp)
{
  ceph_assert(!client_request);
  more()->filepath1 = fp;
}

void MDRequestImpl::set_filepath2(const filepath& fp)
{
  ceph_assert(!client_request);
  more()->filepath2 = fp;
}

bool MDRequestImpl::is_queued_for_replay() const
{
  return client_request ? client_request->is_queued_for_replay() : false;
}

bool MDRequestImpl::can_batch()
{
  if (num_auth_pins || num_remote_auth_pins || lock_cache || !locks.empty())
    return false;

  auto op = client_request->get_op();
  auto& path = client_request->get_filepath();
  if (op == CEPH_MDS_OP_GETATTR) {
    if (path.depth() == 0)
      return true;
  } else if (op == CEPH_MDS_OP_LOOKUP) {
    if (path.depth() == 1 && !path.is_last_snap())
      return true;
  }

  return false;
}

std::unique_ptr<BatchOp> MDRequestImpl::release_batch_op()
{
  int mask = client_request->head.args.getattr.mask;
  auto it = batch_op_map->find(mask);
  std::unique_ptr<BatchOp> bop = std::move(it->second);
  batch_op_map->erase(it);
  return bop;
}

int MDRequestImpl::compare_paths()
{
  if (dir_root[0] < dir_root[1])
    return -1;
  if (dir_root[0] > dir_root[1])
    return 1;
  if (dir_depth[0] < dir_depth[1])
    return -1;
  if (dir_depth[0] > dir_depth[1])
    return 1;
  return 0;
}

cref_t<MClientRequest> MDRequestImpl::release_client_request()
{
  std::lock_guard l(lock);
  cref_t<MClientRequest> req;
  req.swap(client_request);
  client_request = req;
  return req;
}

void MDRequestImpl::reset_peer_request(const cref_t<MMDSPeerRequest>& req)
{
  std::lock_guard l(lock);
  peer_request = req;
}

void MDRequestImpl::print(ostream &out) const
{
  out << "request(" << reqid << " nref=" << nref;
  //if (request) out << " " << *request;
  if (is_peer()) out << " peer_to mds." << peer_to_mds;
  if (client_request) out << " cr=" << client_request;
  if (peer_request) out << " sr=" << peer_request;
  out << ")";
}

void MDRequestImpl::_dump(Formatter *f, bool has_mds_lock) const
{
  std::lock_guard l(lock);
  f->dump_int("result", result);
  f->dump_string("flag_point", _get_state_string());
  f->dump_object("reqid", reqid);
  if (client_request) {
    f->dump_string("op_type", "client_request");
  } else if (is_peer()) { // replies go to an existing mdr
    f->dump_string("op_type", "peer_request");
    f->open_object_section("leader_info");
    f->dump_stream("leader") << peer_to_mds;
    f->close_section(); // leader_info

    if (peer_request) {
      f->open_object_section("request_info");
      f->dump_int("attempt", peer_request->get_attempt());
      f->dump_string("op_type",
         MMDSPeerRequest::get_opname(peer_request->get_op()));
      f->dump_int("lock_type", peer_request->get_lock_type());
      f->dump_stream("object_info") << peer_request->get_object_info();
      f->dump_stream("srcdnpath") << peer_request->srcdnpath;
      f->dump_stream("destdnpath") << peer_request->destdnpath;
      f->dump_stream("witnesses") << peer_request->witnesses;
      f->dump_bool("has_inode_export",
         peer_request->inode_export_v != 0);
      f->dump_int("inode_export_v", peer_request->inode_export_v);
      f->dump_stream("op_stamp") << peer_request->op_stamp;
      f->close_section(); // request_info
    }
  }
  else if (internal_op != -1) { // internal request
    f->dump_string("op_type", "internal_op");
    f->dump_int("internal_op", internal_op);
    f->dump_string("op_name", ceph_mds_op_name(internal_op));
  }
  else {
    f->dump_string("op_type", "no_available_op_found");
  }

  {
    f->open_array_section("events");
    for (auto& i : events) {
      f->dump_object("event", i);
    }
    f->close_section(); // events
  }

  if (has_mds_lock) {
    f->open_array_section("locks");
    for (auto& l : locks) {
      f->open_object_section("lock");
      {
        auto* mdsco = l.lock->get_parent();
        f->dump_object("object", *mdsco);
        CachedStackStringStream css;
        *css << *mdsco;
        f->dump_string("object_string", css->strv());
        f->dump_object("lock", *l.lock);
        f->dump_int("flags", l.flags);
        f->dump_int("wrlock_target", l.wrlock_target);
      }
      f->close_section();
    }
    f->close_section();
  } else {
    f->dump_null("locks");
  }
}

void MDRequestImpl::_dump_op_descriptor(ostream& os) const
{
  if (client_request) {
    client_request->print(os);
  } else if (peer_request) {
    peer_request->print(os);
  } else if (is_peer()) {
    os << "peer_request:" << reqid;
  } else if (internal_op >= 0) {
    os << "internal op " << ceph_mds_op_name(internal_op) << ":" << reqid;
    if (has_more()) {
      auto& fp = get_filepath();
      if (!fp.empty()) {
        os << " fp=" << fp;
      }
      auto& fp2 = get_filepath2();
      if (!fp2.empty()) {
        os << " fp2=" << fp2;
      }
    }
  } else {
    // drat, it's triggered by a peer request, but we don't have a message
    // FIXME
    os << "rejoin:" << reqid;
  }
}

void MDLockCache::attach_locks()
{
  ceph_assert(!items_lock);
  items_lock.reset(new LockItem[locks.size()]);
  int i = 0;
  for (auto& p : locks) {
    items_lock[i].parent = this;
    p.lock->add_cache(items_lock[i]);
    ++i;
  }
}

void MDLockCache::attach_dirfrags(std::vector<CDir*>&& dfv)
{
  std::sort(dfv.begin(), dfv.end());
  auto last = std::unique(dfv.begin(), dfv.end());
  dfv.erase(last, dfv.end());
  auth_pinned_dirfrags = std::move(dfv);

  ceph_assert(!items_dir);
  items_dir.reset(new DirItem[auth_pinned_dirfrags.size()]);
  int i = 0;
  for (auto dir : auth_pinned_dirfrags) {
    items_dir[i].parent = this;
    dir->lock_caches_with_auth_pins.push_back(&items_dir[i].item_dir);
    ++i;
  }
}

void MDLockCache::detach_locks()
{
  ceph_assert(items_lock);
  int i = 0;
  for (auto& p : locks) {
    auto& item = items_lock[i];
    p.lock->remove_cache(item);
    ++i;
  }
  items_lock.reset();
}

void MDLockCache::detach_dirfrags()
{
  ceph_assert(items_dir);
  int i = 0;
  for (auto dir : auth_pinned_dirfrags) {
    (void)dir;
    items_dir[i].item_dir.remove_myself();
    ++i;
  }
  items_dir.reset();
}
