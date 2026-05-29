// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>
#include <string_view>

#include <limits.h>
#include <string.h>

#include "include/ceph_assert.h"
#include "include/rados/librados.hpp"

#include "cls/lock/cls_lock_client.h"

#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/strtol.h" // for strict_strtoll()
#include "common/version.h"

#include "SimpleRADOSStriper.h"

using ceph::bufferlist;

#define dout_subsys ceph_subsys_cephsqlite
#undef dout_prefix
#define dout_prefix *_dout << "client." << ioctx.get_instance_id() << ": SimpleRADOSStriper(" << cookie_dstr << "): " << __func__ << ": " << oid << ": "
#define d(lvl) ldout((CephContext*)ioctx.cct(), (lvl))

enum {
  P_FIRST = 0xe0000,
  P_UPDATE_METADATA,
  P_UPDATE_ALLOCATED,
  P_UPDATE_SIZE,
  P_UPDATE_VERSION,
  P_SHRINK,
  P_SHRINK_BYTES,
  P_LOCK,
  P_UNLOCK,
  P_LAST,
};

int SimpleRADOSStriper::check_reservation(bool* is_reserved)
{
  bufferlist bl_state;
  auto ext = get_first_extent();
  if (int rc = ioctx.getxattr(ext.soid, XATTR_STATE, bl_state); rc < 0) {
    if (rc == -ENOENT || rc == -ENODATA) {
      *is_reserved = false;
      return 0;
    }
    d(-1) << " getxattr failed: " << cpp_strerror(rc) << dendl;
    return rc;
  }
  auto state = bl_state.to_str();
  *is_reserved = (state >= state2bl(LockLevel::Reserved).to_str());
  return 0;
}

int SimpleRADOSStriper::config_logger(CephContext* cct, std::string_view name, std::shared_ptr<PerfCounters>* l)
{
  PerfCountersBuilder plb(cct, name.data(), P_FIRST, P_LAST);
  plb.add_u64_counter(P_UPDATE_METADATA, "update_metadata", "Number of metadata updates");
  plb.add_u64_counter(P_UPDATE_ALLOCATED, "update_allocated", "Number of allocated updates");
  plb.add_u64_counter(P_UPDATE_SIZE, "update_size", "Number of size updates");
  plb.add_u64_counter(P_UPDATE_VERSION, "update_version", "Number of version updates");
  plb.add_u64_counter(P_SHRINK, "shrink", "Number of allocation shrinks");
  plb.add_u64_counter(P_SHRINK_BYTES, "shrink_bytes", "Bytes shrunk");
  plb.add_u64_counter(P_LOCK, "lock", "Number of locks");
  plb.add_u64_counter(P_UNLOCK, "unlock", "Number of unlocks");
  l->reset(plb.create_perf_counters());
  return 0;
}

SimpleRADOSStriper::~SimpleRADOSStriper()
{
  wait_for_aios(true);

  if (lock_keeper.joinable()) {
    {
      std::scoped_lock l(lock_keeper_mutex);
      shutdown = true;
    }
    lock_keeper_cvar.notify_all();
    lock_keeper.join();
  }

  if (ioctx.is_valid()) {
    d(5) << dendl;

    if (is_locked()) {
      unlock(LockLevel::None);
    }
  }
}

SimpleRADOSStriper::extent SimpleRADOSStriper::get_next_extent(uint64_t off, size_t len) const
{
  extent e;
  {
    uint64_t stripe = (off>>object_size);
    CachedStackStringStream css;
    *css << oid;
    *css << ".";
    *css << std::setw(16) << std::setfill('0') << std::hex << stripe;
    e.soid = css->str();
  }
  e.off = off & ((1<<object_size)-1);
  e.len = std::min<size_t>(len, (1<<object_size)-e.off);
  return e;
}

int SimpleRADOSStriper::remove()
{
  d(5) << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  if (int rc = wait_for_aios(true); rc < 0) {
    aios_failure = 0;
    return rc;
  }

  if (int rc = set_metadata(0, true); rc < 0) {
    return rc;
  }

  auto ext = get_first_extent();
  if (int rc = ioctx.remove(ext.soid); rc < 0) {
    d(1) << " remove failed: " << cpp_strerror(rc) << dendl;
    return rc;
  }

  locked = LockLevel::None;

  return 0;
}

int SimpleRADOSStriper::truncate(uint64_t size)
{
  d(5) << size << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  /* TODO: (not currently used by SQLite) handle growth + sparse */
  if (int rc = set_metadata(size, true); rc < 0) {
    return rc;
  }

  return 0;
}

int SimpleRADOSStriper::wait_for_aios(bool block)
{
  while (!aios.empty()) {
    auto& aiocp = aios.front();
    int rc;
    if (block) {
      rc = aiocp->wait_for_complete();
    } else {
      if (aiocp->is_complete()) {
        rc = aiocp->get_return_value();
      } else {
        return 0;
      }
    }
    if (rc) {
      d(1) << " aio failed: " << cpp_strerror(rc) << dendl;
      if (aios_failure == 0) {
        aios_failure = rc;
      }
    }
    aios.pop();
  }
  return aios_failure;
}

int SimpleRADOSStriper::flush()
{
  d(5) << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  if (size_dirty) {
    if (int rc = set_metadata(size, true); rc < 0) {
      return rc;
    }
  }

  if (int rc = wait_for_aios(true); rc < 0) {
    aios_failure = 0;
    return rc;
  }

  return 0;
}

int SimpleRADOSStriper::stat(uint64_t* s)
{
  d(5) << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  *s = size;

  d(5) << " = " << size << dendl;
  return 0;
}

int SimpleRADOSStriper::create()
{
  d(5) << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  auto ext = get_first_extent();
  /* create and init if not exists: */
  {
    auto op = librados::ObjectWriteOperation();
    /* exclusive create ensures we do none of these setxattrs happen if it fails */
    op.create(1);
    op.setxattr(XATTR_VERSION, uint2bl(0));
    op.setxattr(XATTR_EXCL, bufferlist());
    op.setxattr(XATTR_STATE, state2bl(LockLevel::Shared));
    op.setxattr(XATTR_WRITE_EPOCH, uint2bl(0));
    op.setxattr(XATTR_SIZE, uint2bl(0));
    op.setxattr(XATTR_ALLOCATED, uint2bl(0));
    op.setxattr(XATTR_LAYOUT_STRIPE_UNIT, uint2bl(1));
    op.setxattr(XATTR_LAYOUT_STRIPE_COUNT, uint2bl(1));
    op.setxattr(XATTR_LAYOUT_OBJECT_SIZE, uint2bl(1<<object_size));
    if (int rc = ioctx.operate(ext.soid, &op); rc != -EEXIST) {
      return rc;
    }
  }

  // EEXIST

  while (true) {
    // Batch check all xattrs to optimize connection latency for existing databases
    librados::ObjectReadOperation batch_op;
    int rval_version = 0, rval_excl = 0, rval_state = 0, rval_epoch = 0;
    int rval_size = 0, rval_alloc = 0, rval_su = 0, rval_sc = 0, rval_os = 0;

    batch_op.getxattr(XATTR_VERSION, nullptr, &rval_version);
    batch_op.set_op_flags2(librados::OP_FAILOK);
    batch_op.getxattr(XATTR_EXCL, nullptr, &rval_excl);
    batch_op.set_op_flags2(librados::OP_FAILOK);
    batch_op.getxattr(XATTR_STATE, nullptr, &rval_state);
    batch_op.set_op_flags2(librados::OP_FAILOK);
    batch_op.getxattr(XATTR_WRITE_EPOCH, nullptr, &rval_epoch);
    batch_op.set_op_flags2(librados::OP_FAILOK);
    batch_op.getxattr(XATTR_SIZE, nullptr, &rval_size);
    batch_op.set_op_flags2(librados::OP_FAILOK);
    batch_op.getxattr(XATTR_ALLOCATED, nullptr, &rval_alloc);
    batch_op.set_op_flags2(librados::OP_FAILOK);
    batch_op.getxattr(XATTR_LAYOUT_STRIPE_UNIT, nullptr, &rval_su);
    batch_op.set_op_flags2(librados::OP_FAILOK);
    batch_op.getxattr(XATTR_LAYOUT_STRIPE_COUNT, nullptr, &rval_sc);
    batch_op.set_op_flags2(librados::OP_FAILOK);
    batch_op.getxattr(XATTR_LAYOUT_OBJECT_SIZE, nullptr, &rval_os);
    batch_op.set_op_flags2(librados::OP_FAILOK);

    bufferlist out_bl;
    int batch_rc = ioctx.operate(ext.soid, &batch_op, &out_bl);
    if (batch_rc < 0 && batch_rc != -ENODATA) {
      return batch_rc;
    }

    // Fallback: at least one xattr is missing (upgrade path).
    // Initialize missing ones individually.
    if (rval_version == -ENODATA) {
      d(-1) << "backing database exists but there is no " << XATTR_VERSION << dendl;
      return -EINVAL;
    }

    librados::ObjectWriteOperation write_op;
    bool need_write = false;

    if (rval_excl == -ENODATA) { write_op.setxattr(XATTR_EXCL, bufferlist()); need_write = true; }
    if (rval_state == -ENODATA) { write_op.setxattr(XATTR_STATE, state2bl(LockLevel::Shared)); need_write = true; }
    if (rval_epoch == -ENODATA) { write_op.setxattr(XATTR_WRITE_EPOCH, uint2bl(0)); need_write = true; }
    if (rval_size == -ENODATA) { write_op.setxattr(XATTR_SIZE, uint2bl(0)); need_write = true; }
    if (rval_alloc == -ENODATA) { write_op.setxattr(XATTR_ALLOCATED, uint2bl(0)); need_write = true; }
    if (rval_su == -ENODATA) { write_op.setxattr(XATTR_LAYOUT_STRIPE_UNIT, uint2bl(1)); need_write = true; }
    if (rval_sc == -ENODATA) { write_op.setxattr(XATTR_LAYOUT_STRIPE_COUNT, uint2bl(1)); need_write = true; }
    if (rval_os == -ENODATA) { write_op.setxattr(XATTR_LAYOUT_OBJECT_SIZE, uint2bl(1<<object_size)); need_write = true; }

    if (need_write) {
      uint64_t obj_ver = ioctx.get_last_version();
      write_op.assert_version(obj_ver);
      if (int rc = ioctx.operate(ext.soid, &write_op); rc < 0) {
        if (rc == -ERANGE || rc == -EOVERFLOW) {
          continue;
        }
        return rc;
      }
    }
    
    return 0;
  }
}

void SimpleRADOSStriper::print(std::ostream& os) const
{
  os << "SimpleRADOSStriper"
     << "(oid: " << oid
     << " self_state: " << std::to_underlying(locked)
     << " disk_state: " << std::to_underlying(disk_state)
     << " size: " << size
     << " allocated: " << allocated
     << " version: " << version
     << " write_epoch: " << write_epoch
     << " cookie: " << cookie_dstr << "..."
     << ")";
}

int SimpleRADOSStriper::get_metadata()
{
  d(5) << oid << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  auto ext = get_first_extent();
  auto op = librados::ObjectReadOperation();
  bufferlist bl_excl, bl_epoch, bl_size, bl_alloc, bl_version, bl_state, pbl;
  bufferlist bl_layout_su, bl_layout_sc, bl_layout_os;
  int prval_excl, prval_epoch, prval_size, prval_alloc, prval_version, prval_state;
  int prval_layout_su, prval_layout_sc, prval_layout_os;

  op.getxattr(XATTR_ALLOCATED, &bl_alloc, &prval_alloc);
  op.getxattr(XATTR_EXCL, &bl_excl, &prval_excl);
  op.getxattr(XATTR_LAYOUT_OBJECT_SIZE, &bl_layout_os, &prval_layout_os);
  op.getxattr(XATTR_LAYOUT_STRIPE_COUNT, &bl_layout_sc, &prval_layout_sc);
  op.getxattr(XATTR_LAYOUT_STRIPE_UNIT, &bl_layout_su, &prval_layout_su);
  op.getxattr(XATTR_SIZE, &bl_size, &prval_size);
  op.getxattr(XATTR_STATE, &bl_state, &prval_state);
  op.getxattr(XATTR_VERSION, &bl_version, &prval_version);
  op.getxattr(XATTR_WRITE_EPOCH, &bl_epoch, &prval_epoch);

  if (int rc = ioctx.operate(ext.soid, &op, &pbl); rc < 0) {
    d(1) << " getxattr failed: " << cpp_strerror(rc) << dendl;
    return rc;
  }

  if (prval_alloc >= 0 && bl_alloc.length() > 0) {
    auto sstr = bl_alloc.to_str();
    std::string err;
    allocated = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
    d(20) << XATTR_ALLOCATED << "=" << sstr << " (rval=" << prval_alloc << ")" << dendl;
  } else {
    d(20) << XATTR_ALLOCATED << " is missing or empty (rval=" << prval_alloc << ")" << dendl;
  }
  if (prval_excl >= 0) {
    exclusive_holder = bl_excl.to_str();
    d(20) << XATTR_EXCL << "=" << exclusive_holder << " (rval=" << prval_excl << ")" << dendl;
  } else {
    exclusive_holder.clear();
    d(20) << XATTR_EXCL << " is missing (rval=" << prval_excl << ")" << dendl;
  }
  if (prval_layout_os >= 0 && bl_layout_os.length() > 0) {
    auto sstr = bl_layout_os.to_str();
    std::string err;
    layout_object_size = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
    d(20) << XATTR_LAYOUT_OBJECT_SIZE << "=" << sstr << " (rval=" << prval_layout_os << ")" << dendl;
  } else {
    d(20) << XATTR_LAYOUT_OBJECT_SIZE << " is missing or empty (rval=" << prval_layout_os << ")" << dendl;
  }
  if (prval_layout_sc >= 0 && bl_layout_sc.length() > 0) {
    auto sstr = bl_layout_sc.to_str();
    std::string err;
    layout_stripe_count = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
    d(20) << XATTR_LAYOUT_STRIPE_COUNT << "=" << sstr << " (rval=" << prval_layout_sc << ")" << dendl;
  } else {
    d(20) << XATTR_LAYOUT_STRIPE_COUNT << " is missing or empty (rval=" << prval_layout_sc << ")" << dendl;
  }
  if (prval_layout_su >= 0 && bl_layout_su.length() > 0) {
    auto sstr = bl_layout_su.to_str();
    std::string err;
    layout_stripe_unit = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
    d(20) << XATTR_LAYOUT_STRIPE_UNIT << "=" << sstr << " (rval=" << prval_layout_su << ")" << dendl;
  } else {
    d(20) << XATTR_LAYOUT_STRIPE_UNIT << " is missing or empty (rval=" << prval_layout_su << ")" << dendl;
  }
  if (prval_size >= 0 && bl_size.length() > 0) {
    auto sstr = bl_size.to_str();
    std::string err;
    size = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
    d(20) << XATTR_SIZE << "=" << sstr << " (rval=" << prval_size << ")" << dendl;
  } else {
    d(20) << XATTR_SIZE << " is missing or empty (rval=" << prval_size << ")" << dendl;
  }
  if (prval_state >= 0 && bl_state.length() > 0) {
    auto sstr = bl_state.to_str();
    std::string err;
    disk_state = static_cast<LockLevel>(strict_strtoll(sstr.c_str(), 10, &err));
    ceph_assert(err.empty());
    d(20) << XATTR_STATE << "=" << sstr << " (rval=" << prval_state << ")" << dendl;
  } else {
    d(20) << XATTR_STATE << " is missing or empty (rval=" << prval_state << ")" << dendl;
  }
  if (prval_version >= 0 && bl_version.length() > 0) {
    auto sstr = bl_version.to_str();
    std::string err;
    version = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
    d(20) << XATTR_VERSION << "=" << sstr << " (rval=" << prval_version << ")" << dendl;
  } else {
    d(20) << XATTR_VERSION << " is missing or empty (rval=" << prval_version << ")" << dendl;
  }
  if (prval_epoch >= 0 && bl_epoch.length() > 0) {
    auto sstr = bl_epoch.to_str();
    std::string err;
    write_epoch = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
    d(20) << XATTR_WRITE_EPOCH << "=" << sstr << " (rval=" << prval_epoch << ")" << dendl;
  } else {
    d(20) << XATTR_WRITE_EPOCH << " is missing or empty (rval=" << prval_epoch << ")" << dendl;
  }

  d(10) << *this << dendl;
  return 0;
}

int SimpleRADOSStriper::open()
{
  d(5) << oid << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  return get_metadata();
}

int SimpleRADOSStriper::shrink_alloc(uint64_t a)
{
  d(5) << dendl;
  std::vector<aiocompletionptr> removes;

  ceph_assert(a <= allocated);
  uint64_t prune = std::max<uint64_t>(a, (1u << object_size)); /* never delete first extent here */
  uint64_t len = allocated - prune;
  const uint64_t bytes_removed = len;
  uint64_t offset = prune;
  while (len > 0) {
    auto ext = get_next_extent(offset, len);
    auto aiocp = aiocompletionptr(librados::Rados::aio_create_completion());
    if (int rc = ioctx.aio_remove(ext.soid, aiocp.get()); rc < 0) {
      d(1) << " aio_remove failed: " << cpp_strerror(rc) << dendl;
      return rc;
    }
    removes.emplace_back(std::move(aiocp));
    len -= ext.len;
    offset += ext.len;
  }

  for (auto& aiocp : removes) {
    if (int rc = aiocp->wait_for_complete(); rc < 0 && rc != -ENOENT) {
      d(1) << " aio_remove failed: " << cpp_strerror(rc) << dendl;
      return rc;
    }
  }

  auto ext = get_first_extent();
  auto op = librados::ObjectWriteOperation();
  auto aiocp = aiocompletionptr(librados::Rados::aio_create_completion());
  op.setxattr(XATTR_ALLOCATED, uint2bl(a));
  d(15) << " updating allocated to " << a << dendl;
  op.setxattr(XATTR_VERSION, uint2bl(version+1));
  d(15) << " updating version to " << (version+1) << dendl;
  if (int rc = ioctx.aio_operate(ext.soid, aiocp.get(), &op); rc < 0) {
    d(1) << " update failed: " << cpp_strerror(rc) << dendl;
    return rc;
  }
  /* we need to wait so we don't have dangling extents */
  d(10) << " waiting for allocated update" << dendl;
  if (int rc = aiocp->wait_for_complete(); rc < 0) {
    d(1) << " update failure: " << cpp_strerror(rc) << dendl;
    return rc;
  }
  if (logger) {
    logger->inc(P_UPDATE_METADATA);
    logger->inc(P_UPDATE_ALLOCATED);
    logger->inc(P_UPDATE_VERSION);
    logger->inc(P_SHRINK);
    logger->inc(P_SHRINK_BYTES, bytes_removed);
  }

  version += 1;
  allocated = a;
  return 0;
}

int SimpleRADOSStriper::maybe_shrink_alloc()
{
  d(15) << dendl;

  if (size == 0) {
    if (allocated > 0) {
      d(10) << "allocation shrink to 0" << dendl;
      return shrink_alloc(0);
    } else {
      return 0;
    }
  }

  uint64_t mask = (1<<object_size)-1;
  uint64_t new_allocated = min_growth + ((size + mask) & ~mask); /* round up base 2 */
  if (allocated > new_allocated && ((allocated-new_allocated) > min_growth)) {
    d(10) << "allocation shrink to " << new_allocated << dendl;
    return shrink_alloc(new_allocated);
  }

  return 0;
}

bufferlist SimpleRADOSStriper::str2bl(std::string_view sv)
{
  bufferlist bl;
  bl.append(sv);
  return bl;
}

bufferlist SimpleRADOSStriper::uint2bl(uint64_t v)
{
  CachedStackStringStream css;
  *css << std::dec << std::setw(16) << std::setfill('0') << v;
  bufferlist bl;
  bl.append(css->strv());
  return bl;
}

bufferlist SimpleRADOSStriper::state2bl(LockLevel l)
{
  CachedStackStringStream css;
  *css << std::dec << std::setw(8) << std::setfill('0') << std::to_underlying(l);
  bufferlist bl;
  bl.append(css->strv());
  return bl;
}

int SimpleRADOSStriper::set_metadata(uint64_t new_size, bool update_size)
{
  d(10) << " new_size: " << new_size
        << " update_size: " << update_size
        << " allocated: " << allocated
        << " size: " << size
        << " version: " << version
        << dendl;

  bool do_op = false;
  auto new_allocated = allocated;
  auto ext = get_first_extent();
  auto op = librados::ObjectWriteOperation();
  if (new_size > allocated) {
    uint64_t mask = (1<<object_size)-1;
    new_allocated = min_growth + ((size + mask) & ~mask); /* round up base 2 */
    op.setxattr(XATTR_ALLOCATED, uint2bl(new_allocated));
    do_op = true;
    if (logger) logger->inc(P_UPDATE_ALLOCATED);
    d(15) << " updating allocated to " << new_allocated << dendl;
  }
  if (update_size) {
    op.setxattr(XATTR_SIZE, uint2bl(new_size));
    do_op = true;
    if (logger) logger->inc(P_UPDATE_SIZE);
    d(15) << " updating size to " << new_size << dendl;
  }
  if (do_op) {
    if (logger) logger->inc(P_UPDATE_METADATA);
    if (logger) logger->inc(P_UPDATE_VERSION);
    op.setxattr(XATTR_VERSION, uint2bl(version+1));
    d(15) << " updating version to " << (version+1) << dendl;
    auto aiocp = aiocompletionptr(librados::Rados::aio_create_completion());
    if (int rc = ioctx.aio_operate(ext.soid, aiocp.get(), &op); rc < 0) {
      d(1) << " update failure: " << cpp_strerror(rc) << dendl;
      return rc;
    }
    version += 1;
    if (allocated != new_allocated) {
      /* we need to wait so we don't have dangling extents */
      d(10) << "waiting for allocated update" << dendl;
      if (int rc = aiocp->wait_for_complete(); rc < 0) {
        d(1) << " update failure: " << cpp_strerror(rc) << dendl;
        return rc;
      }
      aiocp.reset();
      allocated = new_allocated;
    }
    if (aiocp) {
      aios.emplace(std::move(aiocp));
    }
    if (update_size) {
      size = new_size;
      size_dirty = false;
      return maybe_shrink_alloc();
    }
  }
  return 0;
}

ssize_t SimpleRADOSStriper::write(const void* data, size_t len, uint64_t off)
{
  d(5) << off << "~" << len << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  if (allocated < (len+off)) {
    if (int rc = set_metadata(len+off, false); rc < 0) {
      return rc;
    }
  }

  size_t w = 0;
  while ((len-w) > 0) {
    auto ext = get_next_extent(off+w, len-w);
    auto aiocp = aiocompletionptr(librados::Rados::aio_create_completion());
    bufferlist bl;
    bl.append((const char*)data+w, ext.len);
    if (int rc = ioctx.aio_write(ext.soid, aiocp.get(), bl, ext.len, ext.off); rc < 0) {
      break;
    }
    aios.emplace(std::move(aiocp));
    w += ext.len;
  }

  wait_for_aios(false); // clean up finished completions

  if (size < (len+off)) {
    size = len+off;
    size_dirty = true;
    d(10) << " dirty size: " << size << dendl;
  }

  return (ssize_t)w;
}

ssize_t SimpleRADOSStriper::read(void* data, size_t len, uint64_t off)
{
  d(5) << off << "~" << len << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  size_t r = 0;
  // Don't use std::vector to store bufferlists (e.g for parallelizing aio_reads),
  // as they are being moved whenever the vector resizes
  // and will cause invalidated references.
  std::deque<std::pair<bufferlist, aiocompletionptr>> reads;
  while ((len-r) > 0) {
    auto ext = get_next_extent(off+r, len-r);
    auto& [bl, aiocp] = reads.emplace_back();
    aiocp = aiocompletionptr(librados::Rados::aio_create_completion());
    if (int rc = ioctx.aio_read(ext.soid, aiocp.get(), &bl, ext.len, ext.off); rc < 0) {
      d(1) << " read failure: " << cpp_strerror(rc) << dendl;
      return rc;
    }
    r += ext.len;
  }

  r = 0;
  for (auto& [bl, aiocp] : reads) {
    if (int rc = aiocp->wait_for_complete(); rc < 0) {
      d(1) << " read failure: " << cpp_strerror(rc) << dendl;
      return rc;
    }
    bl.begin().copy(bl.length(), ((char*)data)+r);
    r += bl.length();
  }
  ceph_assert(r <= len);

  return r;
}

int SimpleRADOSStriper::print_lockers(std::ostream& out)
{
  int exclusive;
  std::string tag;
  std::list<librados::locker_t> lockers;
  auto ext = get_first_extent();
  if (int rc = ioctx.list_lockers(ext.soid, primary, &exclusive, &tag, &lockers); rc < 0) {
    d(1) << " list_lockers failure: " << cpp_strerror(rc) << dendl;
    return rc;
  }
  if (lockers.empty()) {
    out << " primary: none";
  } else {
    out << " primary: exclusive=" << exclusive  << " tag=" << tag << " lockers=[";
    bool first = true;
    for (const auto& l : lockers) {
      if (!first) out << ",";
      out << l.client << ":" << l.cookie << ":" << l.address;
    }
    out << "]";
  }

  if (int rc = get_metadata(); rc < 0) {
    return rc;
  }

  out << " " << *this;

  return 0;
}

/* Do lock renewal in a separate thread: while it's unlikely sqlite chews on
 * something for multiple seconds without calling into the VFS (where we could
 * initiate a lock renewal), it's not impossible with complex queries. Also, we
 * want to allow "PRAGMA locking_mode = exclusive" where the application may
 * not use the sqlite3 database connection for an indeterminate amount of time.
 */
void SimpleRADOSStriper::lock_keeper_main(void)
{
  d(20) << dendl;
  const auto ext = get_first_extent();
  while (!shutdown) {
    d(20) << "tick" << dendl;
    std::unique_lock lock(lock_keeper_mutex);
    auto now = clock::now();
    auto since = now-last_renewal;

    if (since >= lock_keeper_interval && locked > LockLevel::None) {
      d(10) << "renewing lock" << dendl;
      auto tv = ceph::to_timeval(lock_keeper_timeout);
      int rc = 0;
      if (locked >= LockLevel::Exclusive) {
        rc = ioctx.lock_exclusive(ext.soid, primary, cookie.to_string(), lockdesc, &tv, LIBRADOS_LOCK_FLAG_MUST_RENEW);
      } else if (locked >= LockLevel::Shared) {
        rc = ioctx.lock_shared(ext.soid, primary, cookie.to_string(), "", lockdesc, &tv, LIBRADOS_LOCK_FLAG_MUST_RENEW);
      }
      if (rc) {
        /* If lock renewal fails, we cannot continue the application. Return
         * -EBLOCKLISTED for all calls into the striper for this instance, even
         * if we're not actually blocklisted.
         */
        d(-1) << "lock renewal failed: " << cpp_strerror(rc) << dendl;
        blocklisted = true;
        break;
      }
      last_renewal = clock::now();
    }

    lock_keeper_cvar.wait_for(lock, lock_keeper_interval);
  }
}

int SimpleRADOSStriper::recover_lock()
{
  d(5) << "attempting to recover lock" << dendl;

  const auto ext = get_first_extent();

  if (int rc = get_metadata(); rc < 0) {
    if (rc == -ENOENT || rc == -ENODATA) {
      d(20) << "someone else cleaned up?" << dendl;
      return -EBUSY; /* someone else cleaned up, wait our turn */
    } else {
      d(-1) << "could not recover exclusive locker" << dendl;
      return -EIO;
    }
  }

  if (exclusive_holder.empty()) {
    d(5) << "someone else cleaned up" << dendl;
    return -EBUSY;
  }

  int exclusive;
  std::string tag;
  std::list<librados::locker_t> lockers;
  if (int rc = ioctx.list_lockers(ext.soid, primary, &exclusive, &tag, &lockers); rc < 0) {
    return rc;
  }

  bool active = false;
  for (const auto& l : lockers) {
    d(20) << "locker: " << l.address << dendl;
    if (exclusive_holder.find(l.address) != std::string::npos || l.address.find(exclusive_holder) != std::string::npos) {
      d(15) << "  found locker: " << l.address << dendl;
      active = true;
      break;
    }
  }

  if (active) {
    d(5) << "exclusive lock holder " << exclusive_holder << " is still active" << dendl;
    return -EBUSY;
  } else {
    d(5) << "exclusive lock holder " << exclusive_holder << " is dead, recovering" << dendl;
  }

  if (blocklist_the_dead) {
    entity_addrvec_t addrv;
    addrv.parse(exclusive_holder.c_str());
    auto R = librados::Rados(ioctx);
    std::string_view b = "blocklist";
retry:
    for (auto& a : addrv.v) {
      CachedStackStringStream css;
      *css << "{\"prefix\":\"osd " << b << "\", \"" << b << "op\":\"add\",";
      *css << "\"addr\":\"";
      *css << a;
      *css << "\"}";
      std::vector<std::string> cmd = {css->str()};
      d(5) << "sending blocklist command: " << cmd << dendl;
      std::string out;
      if (int rc = R.mon_command(css->str(), bufferlist(), nullptr, &out); rc < 0) {
        if (rc == -EINVAL && b == "blocklist") {
          b = "blacklist";
          goto retry;
        }
        d(-1) << "Cannot proceed with recovery because I have failed to blocklist the old client: " << cpp_strerror(rc) << ", out = " << out << dendl;
        return -EIO;
      }
    }
    /* Ensure our osd_op requests have the latest epoch. */
    R.wait_for_latest_osdmap();
  }

  {
    auto tv = ceph::to_timeval(lock_keeper_timeout);
    if (int rc = ioctx.lock_shared(ext.soid, primary, cookie.to_string(), "", lockdesc, &tv, LIBRADOS_LOCK_FLAG_MAY_RENEW); rc < 0) {
      return rc;
    }
    last_renewal = clock::now();
    d(5) << "acquired Shared lock" << dendl;
  }

  d(5) << "cleaning up the lock state" << dendl;
  {
    auto op = librados::ObjectWriteOperation();
    op.cmpxattr(XATTR_EXCL, LIBRADOS_CMPXATTR_OP_EQ, str2bl(exclusive_holder));
    op.setxattr(XATTR_EXCL, bufferlist());
    op.setxattr(XATTR_STATE, state2bl(LockLevel::Shared));
    if (int rc = ioctx.operate(ext.soid, &op); rc < 0) {
      if (rc == -ECANCELED) {
        d(5) << "another client recovered the lock concurrently" << dendl;
        return -ECANCELED;
      } else {
        d(-1) << "could not set lock owner: " << cpp_strerror(rc) << dendl;
        return -EIO;
      }
    }
  }

  return 0;
}

int SimpleRADOSStriper::lock(LockLevel ilock)
{
  using enum LockLevel;

  d(5) << "locked=" << std::to_underlying(locked) << " ilock=" << std::to_underlying(ilock) << dendl;

  LockLevel initial_lock = locked;

  auto start_time = std::chrono::steady_clock::now();
  auto last_print_time = start_time;
  bool first_print = true;
  auto ext = get_first_extent();
  auto tv = ceph::to_timeval(lock_keeper_timeout);
  utime_t duration; duration.set_from_timeval(&tv);

  if (locked == None) {
    get_metadata();
  }

  while (locked < ilock) {
    if (blocklisted.load()) {
      return -EBLOCKLISTED;
    }

    std::unique_lock l(lock_keeper_mutex);

    auto op = librados::ObjectWriteOperation();
    int flags = LOCK_FLAG_MAY_RENEW;

    /* XXX N.B.: CMPXATTR is weird. It is read as:
     *    <arg> OP <ondisk>
     */

    if (ilock == Shared) {
      d(10) << "escalating to Shared" << dendl;
      /* allow Shared lock if lock state < Pending */
      rados::cls::lock::lock(&op, primary, ClsLockType::SHARED, cookie.to_string(), "", lockdesc, duration, flags);
      op.cmpxattr(XATTR_STATE, LIBRADOS_CMPXATTR_OP_GT, state2bl(LockLevel::Pending));
    } else if (ilock == Reserved) {
      d(10) << "escalating to Reserved" << dendl;
      /* allow Reserve lock if lock state < Reserved */
      rados::cls::lock::lock(&op, primary, ClsLockType::SHARED, cookie.to_string(), "", lockdesc, duration, flags);
      op.cmpxattr(XATTR_STATE, LIBRADOS_CMPXATTR_OP_GT, state2bl(LockLevel::Reserved));
      op.setxattr(XATTR_STATE, state2bl(LockLevel::Reserved));
    } else if (ilock == Pending) {
      d(10) << "escalating to Pending" << dendl;
      /* allow Pending lock if lock state == Reserved */
      rados::cls::lock::lock(&op, primary, ClsLockType::SHARED, cookie.to_string(), "", lockdesc, duration, flags);
      op.cmpxattr(XATTR_STATE, LIBRADOS_CMPXATTR_OP_EQ, state2bl(LockLevel::Reserved));
      op.setxattr(XATTR_STATE, state2bl(LockLevel::Pending));
    } else if (ilock == Exclusive) {
      d(10) << "escalating to Exclusive" << dendl;
      /* allow Exclusive lock if Shared <= lock state <= Pending */
      rados::cls::lock::lock(&op, primary, ClsLockType::EXCLUSIVE, cookie.to_string(), "", lockdesc, duration, flags);
      op.cmpxattr(XATTR_STATE, LIBRADOS_CMPXATTR_OP_LTE, state2bl(LockLevel::Shared));
      op.cmpxattr(XATTR_STATE, LIBRADOS_CMPXATTR_OP_GTE, state2bl(LockLevel::Pending));
      op.cmpxattr(XATTR_WRITE_EPOCH, LIBRADOS_CMPXATTR_OP_EQ, uint2bl(write_epoch));
      op.setxattr(XATTR_STATE, state2bl(LockLevel::Exclusive));
      op.setxattr(XATTR_WRITE_EPOCH, uint2bl(write_epoch + 1));
    }

    if (locked < Reserved && ilock >= Reserved) {
      op.cmpxattr(XATTR_EXCL, LIBRADOS_CMPXATTR_OP_EQ, bufferlist());
      op.setxattr(XATTR_EXCL, str2bl(myaddrs));
    } else if (locked >= Reserved) {
      op.cmpxattr(XATTR_EXCL, LIBRADOS_CMPXATTR_OP_EQ, str2bl(myaddrs));
    }

    if (int rc = ioctx.operate(ext.soid, &op); rc == 0) {
      locked = ilock;
      last_renewal = clock::now();
      if (ilock >= Exclusive) {
        write_epoch++;
      }
    } else if (rc == -ECANCELED) {
      if (locked >= Reserved) {
        blocklisted = true;
        return -EBLOCKLISTED; /* We lost our reservation */
      }
      if (int rc = recover_lock(); rc < 0 && rc != -ECANCELED) {
        return rc;
      }
    } else if (rc == -EBUSY) {
      auto now = std::chrono::steady_clock::now();
      auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_time);

      if (elapsed >= acquire_timeout) {
        d(10) << "acquire_timeout exceeded" << dendl;
        return -EBUSY;
      }

      if (first_print || std::chrono::duration_cast<std::chrono::milliseconds>(now - last_print_time).count() >= 500) {
        d(10) << "waiting for locks: ";
        print_lockers(*_dout);
        *_dout << dendl;
        last_print_time = now;
        first_print = false;
      }

      l.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(5));

      if (int rc = get_metadata(); rc < 0) {
        return rc;
      }
    } else {
      d(-1) << " lock failed: " << cpp_strerror(rc) << dendl;
      if (locked > initial_lock) {
        _unlock(initial_lock);
      }
      return rc;
    }
  }

  if (int rc = get_metadata(); rc < 0) {
    return rc;
  }

  if (!lock_keeper.joinable() && locked > None) {
    lock_keeper = std::thread(&SimpleRADOSStriper::lock_keeper_main, this);
  }

  d(5) << " = 0" << dendl;
  if (logger) logger->inc(P_LOCK);
  return 0;
}

int SimpleRADOSStriper::unlock(LockLevel ilock)
{
  using enum LockLevel;

  d(5) << "ilock=" << std::to_underlying(ilock) << dendl;
  if (blocklisted.load()) return -EBLOCKLISTED;

  std::scoped_lock l(lock_keeper_mutex);
  return _unlock(ilock);
}

int SimpleRADOSStriper::_unlock(LockLevel ilock)
{
  using enum LockLevel;

  if (locked >= Exclusive && ilock < Exclusive) {
    if (int rc = flush(); rc < 0) {
      return rc;
    }
  }

  auto ext = get_first_extent();
  auto tv = ceph::to_timeval(lock_keeper_timeout);
  utime_t duration; duration.set_from_timeval(&tv);

  auto op = librados::ObjectWriteOperation();

  if (locked >= Reserved) {
    op.cmpxattr(XATTR_EXCL, LIBRADOS_CMPXATTR_OP_EQ, str2bl(myaddrs));
    if (ilock < Reserved) {
      op.setxattr(XATTR_EXCL, bufferlist());
      op.setxattr(XATTR_STATE, state2bl(LockLevel::Shared));
    } else if (ilock == Reserved) {
      op.setxattr(XATTR_STATE, state2bl(LockLevel::Reserved));
    } else if (ilock == Pending) {
      op.setxattr(XATTR_STATE, state2bl(LockLevel::Pending));
    }
  }

  if (ilock >= Shared) {
    rados::cls::lock::lock(&op, primary, ClsLockType::SHARED, cookie.to_string(), "", lockdesc, duration, LOCK_FLAG_MAY_RENEW);
  } else {
    rados::cls::lock::unlock(&op, primary, cookie.to_string());
  }

  if (int rc = ioctx.operate(ext.soid, &op); rc < 0) {
    if (rc == -ENOENT) {
      blocklisted = true;
      return -EBLOCKLISTED;
    } else {
      return rc;
    }
  }

  locked = ilock;
  d(5) << " = 0" << dendl;
  if (logger) logger->inc(P_UNLOCK);
  return 0;
}
