// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file
 *
 * This is an OSD class that implements methods for object
 * advisory locking.
 *
 */

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <errno.h>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"

#include "common/errno.h"
#include "common/Clock.h"

#include "cls/lock/cls_lock_types.h"
#include "cls/lock/cls_lock_ops.h"

#include "global/global_context.h"

#include "include/compat.h"


using namespace rados::cls::lock;


CLS_VER(1,0)
CLS_NAME(lock)

cls_handle_t h_class;
cls_method_handle_t h_lock_op;
cls_method_handle_t h_unlock_op;
cls_method_handle_t h_break_lock;
cls_method_handle_t h_get_info;
cls_method_handle_t h_list_locks;
cls_method_handle_t h_assert_locked;

#define LOCK_PREFIX    "lock."

typedef struct lock_info_s {
  map<locker_id_t, locker_info_t> lockers; // map of lockers
  ClsLockType lock_type;                              // lock type (exclusive / shared)
  string tag;                                         // tag: operations on lock can only succeed with this tag
                                                      //      as long as set of non expired lockers
                                                      //      is bigger than 0.

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(lockers, bl);
    uint8_t t = (uint8_t)lock_type;
    ::encode(t, bl);
    ::encode(tag, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    ::decode(lockers, bl);
    uint8_t t;
    ::decode(t, bl);
    lock_type = (ClsLockType)t; 
    ::decode(tag, bl);
    DECODE_FINISH(bl);
  }
  lock_info_s() : lock_type(LOCK_NONE) {}
} lock_info_t;
WRITE_CLASS_ENCODER(lock_info_t)


static int read_lock(cls_method_context_t hctx, const string& name, lock_info_t *lock)
{
  bufferlist bl;
  string key = LOCK_PREFIX;
  key.append(name);
 
  int r = cls_cxx_getxattr(hctx, key.c_str(), &bl);
  if (r < 0) {
    if (r ==  -ENODATA) {
      *lock = lock_info_t();
      return 0;
    }
    if (r != -ENOENT) {
      CLS_ERR("error reading xattr %s: %d", key.c_str(), r);
    }
    return r;
  }

  try {
    bufferlist::iterator it = bl.begin();
    ::decode(*lock, it);
  } catch (const buffer::error &err) {
    CLS_ERR("error decoding %s", key.c_str());
    return -EIO;
  }

  /* now trim expired locks */

  utime_t now = ceph_clock_now(g_ceph_context);

  map<locker_id_t, locker_info_t>::iterator iter = lock->lockers.begin();

  while (iter != lock->lockers.end()) {
    map<locker_id_t, locker_info_t>::iterator next = iter;
    ++next;

    struct locker_info_t& info = iter->second;
    if (!info.expiration.is_zero() && info.expiration < now) {
      CLS_LOG(20, "expiring locker");
      lock->lockers.erase(iter);
    }

    iter = next;
  }

  return 0;
}

static int write_lock(cls_method_context_t hctx, const string& name, const lock_info_t& lock)
{
  string key = LOCK_PREFIX;
  key.append(name);

  bufferlist lock_bl;
  ::encode(lock, lock_bl);

  int r = cls_cxx_setxattr(hctx, key.c_str(), &lock_bl);
  if (r < 0)
    return r;

  return 0;
}

/**
 * helper function to add a lock and update disk state.
 *
 * Input:
 * @param name Lock name
 * @param lock_type Type of lock (exclusive / shared)
 * @param duration Duration of lock (in seconds). Zero means it doesn't expire.
 * @param flags lock flags
 * @param cookie The cookie to set in the lock
 * @param tag The tag to match with the lock (can only lock with matching tags)
 * @param lock_description The lock description to set (if not empty)
 * @param locker_description The locker description
 *
 * @return 0 on success, or -errno on failure
 */
static int lock_obj(cls_method_context_t hctx,
                    const string& name,
                    ClsLockType lock_type,
                    utime_t duration,
                    const string& description,
                    uint8_t flags,
                    const string& cookie,
                    const string& tag)
{
  bool exclusive = lock_type == LOCK_EXCLUSIVE;
  lock_info_t linfo;
  bool fail_if_exists = (flags & LOCK_FLAG_RENEW) == 0;

  CLS_LOG(20, "requested lock_type=%s fail_if_exists=%d", cls_lock_type_str(lock_type), fail_if_exists);
  if (lock_type != LOCK_EXCLUSIVE &&
      lock_type != LOCK_SHARED)
    return -EINVAL;

  if (name.empty())
    return -EINVAL;

  // see if there's already a locker
  int r = read_lock(hctx, name, &linfo);
  if (r < 0 && r != -ENOENT) {
    CLS_ERR("Could not read lock info: %s", cpp_strerror(r).c_str());
    return r;
  }
  map<locker_id_t, locker_info_t>& lockers = linfo.lockers;
  map<locker_id_t, locker_info_t>::iterator iter;

  locker_id_t id;
  id.cookie = cookie;
  entity_inst_t inst;
  r = cls_get_request_origin(hctx, &inst);
  id.locker = inst.name;
  assert(r == 0);

  /* check this early, before we check fail_if_exists, otherwise we might
   * remove the locker entry and not check it later */
  if (lockers.size() && tag != linfo.tag) {
    CLS_LOG(20, "cannot take lock on object, conflicting tag");
    return -EBUSY;
  }

  ClsLockType existing_lock_type = linfo.lock_type;
  CLS_LOG(20, "existing_lock_type=%s", cls_lock_type_str(existing_lock_type));
  iter = lockers.find(id);
  if (iter != lockers.end()) {
    if (fail_if_exists) {
      return -EEXIST;
    } else {
      lockers.erase(iter); // remove old entry
    }
  }

  if (!lockers.empty()) {
    if (exclusive) {
      CLS_LOG(20, "could not exclusive-lock object, already locked");
      return -EBUSY;
    }

    if (existing_lock_type != lock_type) {
      CLS_LOG(20, "cannot take lock on object, conflicting lock type");
      return -EBUSY;
    }
  }

  linfo.lock_type = lock_type;
  linfo.tag = tag;
  utime_t expiration;
  if (!duration.is_zero()) {
    expiration = ceph_clock_now(g_ceph_context);
    expiration += duration;

  }
  struct locker_info_t info(expiration, inst.addr, description);

  linfo.lockers[id] = info;

  r = write_lock(hctx, name, linfo);
  if (r < 0)
    return r;

  return 0;
}

/**
 * Set an exclusive lock on an object for the activating client, if possible.
 *
 * Input:
 * @param cls_lock_lock_op request input
 *
 * @returns 0 on success, -EINVAL if it can't decode the lock_cookie,
 * -EBUSY if the object is already locked, or -errno on (unexpected) failure.
 */
static int lock_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "lock_op");
  cls_lock_lock_op op;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(op, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return lock_obj(hctx,
                  op.name, op.type, op.duration, op.description,
                  op.flags, op.cookie, op.tag);
}

/**
 *  helper function to remove a lock from on disk and clean up state.
 *
 *  @param name The lock name
 *  @param locker The locker entity name
 *  @param cookie The user-defined cookie associated with the lock.
 *
 *  @return 0 on success, -ENOENT if there is no such lock (either
 *  entity or cookie is wrong), or -errno on other error.
 */
static int remove_lock(cls_method_context_t hctx,
                const string& name,
                entity_name_t& locker,
                const string& cookie)
{
  // get current lockers
  lock_info_t linfo;
  int r = read_lock(hctx, name, &linfo);
  if (r < 0) {
    CLS_ERR("Could not read list of current lockers off disk: %s", cpp_strerror(r).c_str());
    return r;
  }

  map<locker_id_t, locker_info_t>& lockers = linfo.lockers;
  struct locker_id_t id(locker, cookie);

  // remove named locker from set
  map<locker_id_t, locker_info_t>::iterator iter = lockers.find(id);
  if (iter == lockers.end()) { // no such key
    return -ENOENT;
  }
  lockers.erase(iter);

  r = write_lock(hctx, name, linfo);

  return r;
}

/**
 * Unlock an object which the activating client currently has locked.
 *
 * Input:
 * @param cls_lock_unlock_op request input
 *
 * @return 0 on success, -EINVAL if it can't decode the cookie, -ENOENT
 * if there is no such lock (either entity or cookie is wrong), or
 * -errno on other (unexpected) error.
 */
static int unlock_op(cls_method_context_t hctx,
                     bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "unlock_op");
  cls_lock_unlock_op op;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(op, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  entity_inst_t inst;
  int r = cls_get_request_origin(hctx, &inst);
  assert(r == 0);
  return remove_lock(hctx, op.name, inst.name, op.cookie);
}

/**
 * Break the lock on an object held by any client.
 *
 * Input:
 * @param cls_lock_break_op request input
 *
 * @return 0 on success, -EINVAL if it can't decode the locker and
 * cookie, -ENOENT if there is no such lock (either entity or cookie
 * is wrong), or -errno on other (unexpected) error.
 */
static int break_lock(cls_method_context_t hctx,
               bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "break_lock");
  cls_lock_break_op op;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(op, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  return remove_lock(hctx, op.name, op.locker, op.cookie);
}


/**
 * Retrieve lock info: lockers, tag, exclusive
 *
 * Input:
 * @param cls_lock_list_lockers_op request input
 *
 * Output:
 * @param cls_lock_list_lockers_reply result
 *
 * @return 0 on success, -errno on failure.
 */
static int get_info(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_info");
  cls_lock_get_info_op op;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(op, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  // get current lockers
  lock_info_t linfo;
  int r = read_lock(hctx, op.name, &linfo);
  if (r < 0) {
    CLS_ERR("Could not read lock info: %s", cpp_strerror(r).c_str());
    return r;
  }

  struct cls_lock_get_info_reply ret;

  map<locker_id_t, locker_info_t>::iterator iter;
  for (iter = linfo.lockers.begin(); iter != linfo.lockers.end(); ++iter) {
    ret.lockers[iter->first] = iter->second;
  }
  ret.lock_type = linfo.lock_type;
  ret.tag = linfo.tag;

  ::encode(ret, *out);

  return 0;
}


/**
 * Retrieve a list of locks for this object
 *
 * Input:
 * @param in is ignored.
 *
 * Output:
 * @param out contains encoded cls_list_locks_reply
 *
 * @return 0 on success, -errno on failure.
 */
static int list_locks(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "list_locks");

  map<string, bufferlist> attrs;

  int r = cls_cxx_getxattrs(hctx, &attrs);
  if (r < 0)
    return r;

  cls_lock_list_locks_reply ret;

  map<string, bufferlist>::iterator iter;
  size_t pos = sizeof(LOCK_PREFIX) - 1;
  for (iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& attr = iter->first;
    if (attr.substr(0, pos).compare(LOCK_PREFIX) == 0) {
      ret.locks.push_back(attr.substr(pos));
    }
  }

  ::encode(ret, *out);

  return 0;
}

/**
 * Assert that the object is currently locked
 *
 * Input:
 * @param cls_lock_assert_op request input
 *
 * Output:
 * @param none
 *
 * @return 0 on success, -errno on failure.
 */
int assert_locked(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "assert_locked");

  cls_lock_assert_op op;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(op, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  if (op.type != LOCK_EXCLUSIVE && op.type != LOCK_SHARED) {
    return -EINVAL;
  }

  if (op.name.empty()) {
    return -EINVAL;
  }

  // see if there's already a locker
  lock_info_t linfo;
  int r = read_lock(hctx, op.name, &linfo);
  if (r < 0) {
    CLS_ERR("Could not read lock info: %s", cpp_strerror(r).c_str());
    return r;
  }

  if (linfo.lockers.empty()) {
    CLS_LOG(20, "object not locked");
    return -EBUSY;
  }

  if (linfo.lock_type != op.type) {
    CLS_LOG(20, "lock type mismatch: current=%s, assert=%s",
            cls_lock_type_str(linfo.lock_type), cls_lock_type_str(op.type));
    return -EBUSY;
  }

  if (linfo.tag != op.tag) {
    CLS_LOG(20, "lock tag mismatch: current=%s, assert=%s", linfo.tag.c_str(),
            op.tag.c_str());
    return -EBUSY;
  }

  entity_inst_t inst;
  r = cls_get_request_origin(hctx, &inst);
  assert(r == 0);

  locker_id_t id;
  id.cookie = op.cookie;
  id.locker = inst.name;

  map<locker_id_t, locker_info_t>::iterator iter = linfo.lockers.find(id);
  if (iter == linfo.lockers.end()) {
    CLS_LOG(20, "not locked by assert client");
    return -EBUSY;
  }
  return 0;
}

void __cls_init()
{
  CLS_LOG(20, "Loaded lock class!");

  cls_register("lock", &h_class);
  cls_register_cxx_method(h_class, "lock",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PROMOTE,
                          lock_op, &h_lock_op);
  cls_register_cxx_method(h_class, "unlock",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PROMOTE,
                          unlock_op, &h_unlock_op);
  cls_register_cxx_method(h_class, "break_lock",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          break_lock, &h_break_lock);
  cls_register_cxx_method(h_class, "get_info",
                          CLS_METHOD_RD,
                          get_info, &h_get_info);
  cls_register_cxx_method(h_class, "list_locks",
                          CLS_METHOD_RD,
                          list_locks, &h_list_locks);
  cls_register_cxx_method(h_class, "assert_locked",
                          CLS_METHOD_RD | CLS_METHOD_PROMOTE,
                          assert_locked, &h_assert_locked);

  return;
}
