// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
#include "common/version.h"

#include "SimpleRADOSStriper.h"

using ceph::bufferlist;

#define dout_subsys ceph_subsys_cephsqlite
#undef dout_prefix
#define dout_prefix *_dout << "client." << ioctx.get_instance_id() << ": SimpleRADOSStriper: " << __func__ << ": " << oid << ": "
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
  if (lock_keeper.joinable()) {
    shutdown = true;
    lock_keeper_cvar.notify_all();
    lock_keeper.join();
  }

  if (ioctx.is_valid()) {
    d(5) << dendl;

    if (is_locked()) {
      unlock();
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

  locked = false;

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
  return 0;
}

int SimpleRADOSStriper::create()
{
  d(5) << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  auto ext = get_first_extent();
  auto op = librados::ObjectWriteOperation();
  /* exclusive create ensures we do none of these setxattrs happen if it fails */
  op.create(1);
  op.setxattr(XATTR_VERSION, uint2bl(0));
  op.setxattr(XATTR_EXCL, bufferlist());
  op.setxattr(XATTR_SIZE, uint2bl(0));
  op.setxattr(XATTR_ALLOCATED, uint2bl(0));
  op.setxattr(XATTR_LAYOUT_STRIPE_UNIT, uint2bl(1));
  op.setxattr(XATTR_LAYOUT_STRIPE_COUNT, uint2bl(1));
  op.setxattr(XATTR_LAYOUT_OBJECT_SIZE, uint2bl(1<<object_size));
  if (int rc = ioctx.operate(ext.soid, &op); rc < 0) {
    return rc; /* including EEXIST */
  }
  return 0;
}

int SimpleRADOSStriper::open()
{
  d(5) << oid << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  auto ext = get_first_extent();
  auto op = librados::ObjectReadOperation();
  bufferlist bl_excl, bl_size, bl_alloc, bl_version, pbl;
  int prval_excl, prval_size, prval_alloc, prval_version;
  op.getxattr(XATTR_EXCL, &bl_excl, &prval_excl);
  op.getxattr(XATTR_SIZE, &bl_size, &prval_size);
  op.getxattr(XATTR_ALLOCATED, &bl_alloc, &prval_alloc);
  op.getxattr(XATTR_VERSION, &bl_version, &prval_version);
  if (int rc = ioctx.operate(ext.soid, &op, &pbl); rc < 0) {
    d(1) << " getxattr failed: " << cpp_strerror(rc) << dendl;
    return rc;
  }
  exclusive_holder = bl_excl.to_str();
  {
    auto sstr = bl_size.to_str();
    std::string err;
    size = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
  }
  {
    auto sstr = bl_alloc.to_str();
    std::string err;
    allocated = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
  }
  {
    auto sstr = bl_version.to_str();
    std::string err;
    version = strict_strtoll(sstr.c_str(), 10, &err);
    ceph_assert(err.empty());
  }
  d(15) << " size: " << size << " allocated: " << allocated << " version: " << version << dendl;
  return 0;
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
  if (int rc = ioctx.list_lockers(ext.soid, biglock, &exclusive, &tag, &lockers); rc < 0) {
    d(1) << " list_lockers failure: " << cpp_strerror(rc) << dendl;
    return rc;
  }
  if (lockers.empty()) {
    out << " lockers none";
  } else {
    out << " lockers exclusive=" << exclusive  << " tag=" << tag << " lockers=[";
    bool first = true;
    for (const auto& l : lockers) {
      if (!first) out << ",";
      out << l.client << ":" << l.cookie << ":" << l.address;
    }
    out << "]";
  }
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

    if (since >= lock_keeper_interval && locked) {
      d(10) << "renewing lock" << dendl;
      auto tv = ceph::to_timeval(lock_keeper_timeout);
      int rc = ioctx.lock_exclusive(ext.soid, biglock, cookie.to_string(), lockdesc, &tv, LIBRADOS_LOCK_FLAG_MUST_RENEW);
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

  std::string addrs;
  const auto ext = get_first_extent();

  {
    auto tv = ceph::to_timeval(lock_keeper_timeout);
    if (int rc = ioctx.lock_exclusive(ext.soid, biglock, cookie.to_string(), lockdesc, &tv, 0); rc < 0) {
      return rc;
    }
    locked = true;
    last_renewal = clock::now();
  }

  d(5) << "acquired lock, fetching last owner" << dendl;

  {
    bufferlist bl_excl;
    if (int rc = ioctx.getxattr(ext.soid, XATTR_EXCL, bl_excl); rc < 0) {
      if (rc == -ENOENT) {
        /* someone removed it? ok... */
        goto setowner;
      } else {
        d(-1) << "could not recover exclusive locker" << dendl;
        locked = false; /* it will drop eventually */
        return -EIO;
      }
    }
    addrs = bl_excl.to_str();
  }

  if (addrs.empty()) {
    d(5) << "someone else cleaned up" << dendl;
    goto setowner;
  } else {
    d(5) << "exclusive lock holder was " << addrs << dendl;
  }

  if (blocklist_the_dead) {
    entity_addrvec_t addrv;
    addrv.parse(addrs.c_str());
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
        locked = false; /* it will drop eventually */
        return -EIO;
      }
    }
    /* Ensure our osd_op requests have the latest epoch. */
    R.wait_for_latest_osdmap();
  }

setowner:
  d(5) << "setting new owner to myself, " << myaddrs << dendl;
  {
    auto myaddrbl = str2bl(myaddrs);
    if (int rc = ioctx.setxattr(ext.soid, XATTR_EXCL, myaddrbl); rc < 0) {
      d(-1) << "could not set lock owner" << dendl;
      locked = false; /* it will drop eventually */
      return -EIO;
    }
  }
  return 0;
}

int SimpleRADOSStriper::lock(uint64_t timeoutms)
{
  /* XXX: timeoutms is unused */
  d(5) << "timeout=" << timeoutms << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  std::scoped_lock lock(lock_keeper_mutex);

  ceph_assert(!is_locked());

  /* We're going to be very lazy here in implementation: only exclusive locks
   * are allowed. That even ensures a single reader.
   */
  uint64_t slept = 0;

  auto ext = get_first_extent();
  while (true) {
    /* The general fast path in one compound operation: obtain the lock,
     * confirm the past locker cleaned up after themselves (set XATTR_EXCL to
     * ""), then finally set XATTR_EXCL to our address vector as the new
     * exclusive locker.
     */

    auto op = librados::ObjectWriteOperation();
    auto tv = ceph::to_timeval(lock_keeper_timeout);
    utime_t duration;
    duration.set_from_timeval(&tv);
    rados::cls::lock::lock(&op, biglock, ClsLockType::EXCLUSIVE, cookie.to_string(), "", lockdesc, duration, 0);
    op.cmpxattr(XATTR_EXCL, LIBRADOS_CMPXATTR_OP_EQ, bufferlist());
    op.setxattr(XATTR_EXCL, str2bl(myaddrs));
    int rc = ioctx.operate(ext.soid, &op);
    if (rc == 0) {
      locked = true;
      last_renewal = clock::now();
      break;
    } else if (rc == -EBUSY) {
      if ((slept % 500000) == 0) {
        d(-1) << "waiting for locks: ";
        print_lockers(*_dout);
        *_dout << dendl;
      }
      usleep(5000);
      slept += 5000;
      continue;
    } else if (rc == -ECANCELED) {
      /* CMPXATTR failed, a locker didn't cleanup. Try to recover! */
      if (rc = recover_lock(); rc < 0) {
        if (rc == -EBUSY) {
          continue; /* try again */
        }
        return rc;
      }
      break;
    } else {
      d(-1) << " lock failed: " << cpp_strerror(rc) << dendl;
      return rc;
    }
  }

  if (!lock_keeper.joinable()) {
    lock_keeper = std::thread(&SimpleRADOSStriper::lock_keeper_main, this);
  }

  if (int rc = open(); rc < 0) {
    d(1) << " open failed: " << cpp_strerror(rc) << dendl;
    return rc;
  }

  d(5) << " = 0" << dendl;
  if (logger) {
    logger->inc(P_LOCK);
  }

  return 0;
}

int SimpleRADOSStriper::unlock()
{
  d(5) << dendl;

  if (blocklisted.load()) {
    return -EBLOCKLISTED;
  }

  std::scoped_lock lock(lock_keeper_mutex);

  ceph_assert(is_locked());

  /* wait for flush of metadata */
  if (int rc = flush(); rc < 0) {
    return rc;
  }

  const auto ext = get_first_extent();
  auto op = librados::ObjectWriteOperation();
  op.cmpxattr(XATTR_EXCL, LIBRADOS_CMPXATTR_OP_EQ, str2bl(myaddrs));
  op.setxattr(XATTR_EXCL, bufferlist());
  rados::cls::lock::unlock(&op, biglock, cookie.to_string());
  if (int rc = ioctx.operate(ext.soid, &op); rc < 0) {
    d(-1) << " unlock failed: " << cpp_strerror(rc) << dendl;
    return rc;
  }
  locked = false;

  d(5) << " = 0" << dendl;
  if (logger) {
    logger->inc(P_UNLOCK);
  }

  return 0;
}
