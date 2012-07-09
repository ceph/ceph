// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/** \file
 *
 * This is an OSD class that implements methods for
 * use with rbd.
 *
 * Most of these deal with the rbd header object. Methods prefixed
 * with old_ deal with the original rbd design, in which clients read
 * and interpreted the header object directly.
 *
 * The new format is meant to be opaque to clients - all their
 * interactions with non-data objects should go through this
 * class. The OSD class interface leaves the class to implement its
 * own argument and payload serialization/deserialization, so for ease
 * of implementation we use the existing ceph encoding/decoding
 * methods. Something like json might be preferable, but the rbd
 * kernel module has to be able understand format as well. The
 * datatypes exposed to the clients are strings, unsigned integers,
 * and vectors of those types. The on-wire format can be found in
 * src/include/encoding.h.
 *
 * The methods for interacting with the new format document their
 * parameters as the client sees them - it would be silly to mention
 * in each one that they take an input and an output bufferlist.
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
#include "objclass/objclass.h"
#include "include/rbd_types.h"

#include "librbd/cls_rbd.h"


CLS_VER(2,0)
CLS_NAME(rbd)

cls_handle_t h_class;
cls_method_handle_t h_create;
cls_method_handle_t h_get_features;
cls_method_handle_t h_get_size;
cls_method_handle_t h_set_size;
cls_method_handle_t h_get_parent;
cls_method_handle_t h_set_parent;
cls_method_handle_t h_remove_parent;
cls_method_handle_t h_get_snapcontext;
cls_method_handle_t h_get_object_prefix;
cls_method_handle_t h_get_snapshot_name;
cls_method_handle_t h_snapshot_add;
cls_method_handle_t h_snapshot_remove;
cls_method_handle_t h_get_all_features;
cls_method_handle_t h_lock_image_exclusive;
cls_method_handle_t h_lock_image_shared;
cls_method_handle_t h_unlock_image;
cls_method_handle_t h_break_lock;
cls_method_handle_t h_list_locks;
cls_method_handle_t h_get_id;
cls_method_handle_t h_set_id;
cls_method_handle_t h_dir_get_id;
cls_method_handle_t h_dir_get_name;
cls_method_handle_t h_dir_list;
cls_method_handle_t h_dir_add_image;
cls_method_handle_t h_dir_remove_image;
cls_method_handle_t h_dir_rename_image;
cls_method_handle_t h_old_snapshots_list;
cls_method_handle_t h_old_snapshot_add;
cls_method_handle_t h_old_snapshot_remove;
cls_method_handle_t h_assign_bid;

#define RBD_MAX_KEYS_READ 64
#define RBD_SNAP_KEY_PREFIX "snapshot_"
#define RBD_LOCK_PREFIX "lock_"
#define RBD_LOCK_TYPE_KEY RBD_LOCK_PREFIX "type"
#define RBD_LOCKS_KEY RBD_LOCK_PREFIX "lockers"
#define RBD_LOCK_EXCLUSIVE "exclusive"
#define RBD_LOCK_SHARED "shared"
#define RBD_DIR_ID_KEY_PREFIX "id_"
#define RBD_DIR_NAME_KEY_PREFIX "name_"

static int snap_read_header(cls_method_context_t hctx, bufferlist& bl)
{
  unsigned snap_count = 0;
  uint64_t snap_names_len = 0;
  int rc;
  struct rbd_obj_header_ondisk *header;

  CLS_LOG(20, "snapshots_list");

  while (1) {
    int len = sizeof(*header) +
      snap_count * sizeof(struct rbd_obj_snap_ondisk) +
      snap_names_len;

    rc = cls_cxx_read(hctx, 0, len, &bl);
    if (rc < 0)
      return rc;

    header = (struct rbd_obj_header_ondisk *)bl.c_str();

    if ((snap_count != header->snap_count) ||
        (snap_names_len != header->snap_names_len)) {
      snap_count = header->snap_count;
      snap_names_len = header->snap_names_len;
      bl.clear();
      continue;
    }
    break;
  }

  return 0;
}

static void key_from_snap_id(snapid_t snap_id, string *out)
{
  ostringstream oss;
  oss << RBD_SNAP_KEY_PREFIX
      << std::setw(16) << std::setfill('0') << std::hex << snap_id;
  *out = oss.str();
}

static snapid_t snap_id_from_key(const string &key)
{
  istringstream iss(key);
  uint64_t id;
  iss.ignore(strlen(RBD_SNAP_KEY_PREFIX)) >> std::hex >> id;
  return id;
}

template<typename T>
static int read_key(cls_method_context_t hctx, const string &key, T *out)
{
  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r < 0) {
    if (r != -ENOENT) {
      CLS_ERR("error reading omap key %s: %d", key.c_str(), r);
    }
    return r;
  }

  try {
    bufferlist::iterator it = bl.begin();
    ::decode(*out, it);
  } catch (const buffer::error &err) {
    CLS_ERR("error decoding %s", key.c_str());
    return -EIO;
  }

  return 0;
}

static bool is_valid_id(const string &id) {
  if (!id.size())
    return false;
  for (size_t i = 0; i < id.size(); ++i) {
    if (!isalnum(id[i])) {
      return false;
    }
  }
  return true;
}

/**
 * Initialize the header with basic metadata.
 * Extra features may initialize more fields in the future.
 * Everything is stored as key/value pairs as omaps in the header object.
 *
 * If features the OSD does not understand are requested, -ENOSYS is
 * returned.
 *
 * Input:
 * @param size number of bytes in the image (uint64_t)
 * @param order bits to shift to determine the size of data objects (uint8_t)
 * @param features what optional things this image will use (uint64_t)
 * @param object_prefix a prefix for all the data objects
 *
 * Output:
 * @return 0 on success, negative error code on failure
 */
int create(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string object_prefix;
  uint64_t features, size;
  uint8_t order;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(size, iter);
    ::decode(order, iter);
    ::decode(features, iter);
    ::decode(object_prefix, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "create object_prefix=%s size=%llu order=%u features=%llu",
	  object_prefix.c_str(), size, order, features);

  if (features & ~RBD_FEATURES_ALL) {
    return -ENOSYS;
  }

  if (!object_prefix.size()) {
    return -EINVAL;
  }

  bufferlist stored_prefixbl;
  int r = cls_cxx_map_get_val(hctx, "object_prefix", &stored_prefixbl);
  if (r != -ENOENT) {
    CLS_ERR("reading object_prefix returned %d", r);
    return -EEXIST;
  }

  bufferlist sizebl;
  ::encode(size, sizebl);
  r = cls_cxx_map_set_val(hctx, "size", &sizebl);
  if (r < 0)
    return r;

  bufferlist orderbl;
  ::encode(order, orderbl);
  r = cls_cxx_map_set_val(hctx, "order", &orderbl);
  if (r < 0)
    return r;

  bufferlist featuresbl;
  ::encode(features, featuresbl);
  r = cls_cxx_map_set_val(hctx, "features", &featuresbl);
  if (r < 0)
    return r;

  bufferlist object_prefixbl;
  ::encode(object_prefix, object_prefixbl);
  r = cls_cxx_map_set_val(hctx, "object_prefix", &object_prefixbl);
  if (r < 0)
    return r;

  bufferlist snap_seqbl;
  uint64_t snap_seq = 0;
  ::encode(snap_seq, snap_seqbl);
  r = cls_cxx_map_set_val(hctx, "snap_seq", &snap_seqbl);
  if (r < 0)
    return r;

  return 0;
}

/**
 * Input:
 * @param snap_id which snapshot to query, or CEPH_NOSNAP (uint64_t)
 *
 * Output:
 * @param features list of enabled features for the given snapshot (uint64_t)
 * @returns 0 on success, negative error code on failure
 */
int get_features(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t features, snap_id;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "get_features snap_id=%llu", snap_id);

  if (snap_id == CEPH_NOSNAP) {
    int r = read_key(hctx, "features", &features);
    if (r < 0) {
      CLS_ERR("failed to read features off disk: %s", strerror(r));
      return r;
    }
  } else {
    cls_rbd_snap snap;
    string snapshot_key;
    key_from_snap_id(snap_id, &snapshot_key);
    int r = read_key(hctx, snapshot_key, &snap);
    if (r < 0)
      return r;

    features = snap.features;
  }

  uint64_t incompatible = features & RBD_FEATURES_INCOMPATIBLE;
  ::encode(features, *out);
  ::encode(incompatible, *out);

  return 0;
}

/**
 * check that given feature(s) are set
 *
 * @param hctx context
 * @param need features needed
 * @return 0 if features are set, negative error (like ENOEXEC) otherwise
 */
int require_feature(cls_method_context_t hctx, uint64_t need)
{
  uint64_t features;
  int r = read_key(hctx, "features", &features);
  if (r == -ENOENT)   // this implies it's an old-style image with no features
    return -ENOEXEC;
  if (r < 0)
    return r;
  if ((features & need) != need) {
    CLS_LOG(10, "require_feature missing feature %llx, have %llx", need, features);
    return -ENOEXEC;
  }
  return 0;
}

/**
 * Input:
 * @param snap_id which snapshot to query, or CEPH_NOSNAP (uint64_t)
 *
 * Output:
 * @param order bits to shift to get the size of data objects (uint8_t)
 * @param size size of the image in bytes for the given snapshot (uint64_t)
 * @returns 0 on success, negative error code on failure
 */
int get_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id, size;
  uint8_t order;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "get_size snap_id=%llu", snap_id);

  int r = read_key(hctx, "order", &order);
  if (r < 0) {
    CLS_ERR("failed to read the order off of disk: %s", strerror(r));
    return r;
  }

  if (snap_id == CEPH_NOSNAP) {
    r = read_key(hctx, "size", &size);
    if (r < 0) {
      CLS_ERR("failed to read the image's size off of disk: %s", strerror(r));
      return r;
    }
  } else {
    cls_rbd_snap snap;
    string snapshot_key;
    key_from_snap_id(snap_id, &snapshot_key);
    int r = read_key(hctx, snapshot_key, &snap);
    if (r < 0)
      return r;

    size = snap.image_size;
  }

  ::encode(order, *out);
  ::encode(size, *out);

  return 0;
}

/**
 * Input:
 * @param size new capacity of the image in bytes (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int set_size(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t size;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(size, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  // check that size exists to make sure this is a header object
  // that was created correctly
  uint64_t orig_size;
  int r = read_key(hctx, "size", &orig_size);
  if (r < 0) {
    CLS_ERR("Could not read image's size off disk: %s", strerror(r));
    return r;
  }

  CLS_LOG(20, "set_size size=%llu orig_size=%llu", size);

  bufferlist sizebl;
  ::encode(size, sizebl);
  r = cls_cxx_map_set_val(hctx, "size", &sizebl);
  if (r < 0) {
    CLS_ERR("error writing snapshot metadata: %d", r);
    return r;
  }

  // if we are shrinking, and have a parent, shrink our overlap with
  // the parent, too.
  if (size < orig_size) {
    cls_rbd_parent parent;
    r = read_key(hctx, "parent", &parent);
    if (r == -ENOENT)
      r = 0;
    if (r < 0)
      return r;
    if (parent.exists() && parent.overlap > size) {
      bufferlist parentbl;
      parent.overlap = size;
      ::encode(parent, parentbl);
      r = cls_cxx_map_set_val(hctx, "parent", &parentbl);
      if (r < 0) {
	CLS_ERR("error writing parent: %d", r);
	return r;
      }
    }
  }

  return 0;
}

/**
 * helper function to add a lock and update disk state.
 *
 * Input:
 * @param lock_type The type of lock, either RBD_LOCK_EXCLUSIVE or RBD_LOCK_SHARED
 * @param cookie The cookie to set in the lock
 *
 * @return 0 on success, or -errno on failure
 */
int lock_image(cls_method_context_t hctx, string lock_type,
               const string &cookie)
{
  bool exclusive = lock_type == RBD_LOCK_EXCLUSIVE;

  // see if there's already a locker
  set<pair<string, string> > lockers;
  string existing_lock_type;
  int r = read_key(hctx, RBD_LOCKS_KEY, &lockers);
  if (r != 0 && r != -ENOENT) {
    CLS_ERR("Could not read list of current lockers: %s", strerror(r));
    return r;
  }
  if (exclusive && r != -ENOENT && lockers.size()) {
    CLS_LOG(20, "could not exclusive-lock image, already locked");
    return -EBUSY;
  }
  if (lockers.size() && !exclusive) {
    // make sure existing lock is a shared lock
    r = read_key(hctx, RBD_LOCK_TYPE_KEY, &existing_lock_type);
    if (r != 0) {
      CLS_ERR("Could not read type of current locks off disk: %s", strerror(r));
      return r;
    }
    if (existing_lock_type != lock_type) {
      CLS_LOG(20, "cannot take shared lock on image, existing exclusive lock");
      return -EBUSY;
    }
  }

  // lock the image
  entity_inst_t locker;
  r = cls_get_request_origin(hctx, &locker);
  assert(r == 0);
  stringstream locker_stringstream;
  locker_stringstream << locker;
  pair<set<pair<string, string> >::iterator, bool> result;
  result = lockers.insert(make_pair(locker_stringstream.str(), cookie));
  if (!result.second) { // we didn't insert, because it already existed
    CLS_LOG(20, "could not insert locker -- already present");
    return -EEXIST;
  }

  map<string, bufferlist> lock_keys;
  ::encode(lockers, lock_keys[RBD_LOCKS_KEY]);
  ::encode(lock_type, lock_keys[RBD_LOCK_TYPE_KEY]);

  r = cls_cxx_map_set_vals(hctx, &lock_keys);
  if (r != 0) {
    CLS_ERR("error writing new lock state");
  }
  return r;
}

/**
 * Set an exclusive lock on an image for the activating client, if possible.
 *
 * Input:
 * @param lock_cookie A string cookie, defined by the locker.
 *
 * @returns 0 on success, -EINVAL if it can't decode the lock_cookie,
 * -EBUSY if the image is already locked, or -errno on (unexpected) failure.
 */
int lock_image_exclusive(cls_method_context_t hctx,
                         bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "lock_image_exclusive");
  string lock_cookie;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(lock_cookie, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return lock_image(hctx, RBD_LOCK_EXCLUSIVE, lock_cookie);
}

/**
 * Set an exclusive lock on an image, if possible.
 *
 * Input:
 * @param lock_cookie A string cookie, defined by the locker.
 *
 * @returns 0 on success, -EINVAL if it can't decode the lock_cookie,
 * -EBUSY if the image is exclusive locked, or -errno on (unexpected) failure.
 */
int lock_image_shared(cls_method_context_t hctx,
                      bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "lock_image_shared");
  string lock_cookie;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(lock_cookie, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return lock_image(hctx, RBD_LOCK_SHARED, lock_cookie);
}

/**
 *  helper function to remove a lock from on disk and clean up state.
 *
 *  @param inst The string representation of the locker's entity.
 *  @param cookie The user-defined cookie associated with the lock.
 *
 *  @return 0 on success, -ENOENT if there is no such lock (either
 *  entity or cookie is wrong), or -errno on other error.
 */
int remove_lock(cls_method_context_t hctx, const string& inst,
                const string& cookie)
{
  // get current lockers
  set<pair<string, string> > lockers;
  string location = RBD_LOCKS_KEY;
  int r = read_key(hctx, location, &lockers);
  if (r != 0) {
    CLS_ERR("Could not read list of current lockers off disk: %s", strerror(r));
    return r;
  }

  // remove named locker from set
  pair<string, string> locker(inst, cookie);
  set<pair<string, string> >::iterator iter = lockers.find(locker);
  if (iter == lockers.end()) { // no such key
    return -ENOENT;
  }
  lockers.erase(iter);

  // encode and write new set to disk
  bufferlist locker_bufferlist;
  ::encode(lockers, locker_bufferlist);
  cls_cxx_map_set_val(hctx, location, &locker_bufferlist);

  return 0;
}

/**
 * Unlock an image which the activating client currently has locked.
 *
 * Input:
 * @param lock_cookie The user-defined cookie associated with the lock.
 *
 * @return 0 on success, -EINVAL if it can't decode the cookie, -ENOENT
 * if there is no such lock (either entity or cookie is wrong), or
 * -errno on other (unexpected) error.
 */
int unlock_image(cls_method_context_t hctx,
                 bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "unlock_image");
  string lock_cookie;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(lock_cookie, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  entity_inst_t inst;
  int r = cls_get_request_origin(hctx, &inst);
  assert(r == 0);
  stringstream inst_stringstream;
  inst_stringstream << inst;
  return remove_lock(hctx, inst_stringstream.str(), lock_cookie);
}

/**
 * Break the lock on an image held by any client.
 *
 * Input:
 * @param locker The string representation of the locking client's entity.
 * @param lock_cookie The user-defined cookie associated with the lock.
 *
 * @return 0 on success, -EINVAL if it can't decode the locker and
 * cookie, -ENOENT if there is no such lock (either entity or cookie
 * is wrong), or -errno on other (unexpected) error.
 */
int break_lock(cls_method_context_t hctx,
               bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "break_lock");
  string locker;
  string lock_cookie;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(locker, iter);
    ::decode(lock_cookie, iter);
  } catch (const buffer::error& err) {
    return -EINVAL;
  }

  return remove_lock(hctx, locker, lock_cookie);
}

 /**
 * Retrieve a list of clients locking this object (presumably an rbd header),
 * as well as whether the lock is shared or exclusive.
 *
 * Input:
 * @param in is ignored.
 *
 * Output:
 * @param set<pair<string, string> > lockers The set of clients holding locks,
 * as <client, cookie> pairs.
 * @param exclusive_lock A bool, true if the lock is exclusive. If there are no
 * lockers, this is meaningless.
 *
 * @return 0 on success, -errno on failure.
 */
int list_locks(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "list_locks");
  string key = RBD_LOCKS_KEY;
  string exclusive_string;
  bool have_locks = true;
  int r = cls_cxx_map_get_val(hctx, key, out);
  if (r != 0 && r != -ENOENT) {
    CLS_ERR("Failure in reading list of current lockers: %s", strerror(r));
    return r;
  }
  if (r == -ENOENT) { // none listed
    set<pair<string, string> > empty_lockers;
    ::encode(empty_lockers, *out);
    have_locks = false;
    r = 0;
  }
  if (have_locks) {
    key = RBD_LOCK_TYPE_KEY;
    r = read_key(hctx, key, &exclusive_string);
    if (r < 0) {
      CLS_ERR("Failed to read lock type off disk: %s", strerror(r));
    }
  }
  ::encode((exclusive_string == RBD_LOCK_EXCLUSIVE), *out);
  return r;
}


/**
 * verify that the header object exists
 *
 * @return 0 if the object exists, -ENOENT if it does not, or other error
 */
int check_exists(cls_method_context_t hctx)
{
  uint64_t size;
  time_t mtime;
  return cls_cxx_stat(hctx, &size, &mtime);
}

/**
 * get the current parent, if any
 *
 * Input:
 * @param snap_id which snapshot to query, or CEPH_NOSNAP (uint64_t)
 *
 * Output:
 * @param pool parent pool id (-1 if parent does not exist)
 * @param image parent image id
 * @param snapid parent snapid
 * @param size portion of parent mapped under the child
 *
 * @returns 0 on success or parent does not exist, negative error code on failure
 */
int get_parent(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = check_exists(hctx);
  if (r < 0)
    return r;

  CLS_LOG(20, "get_parent snap_id=%llu", snap_id);

  cls_rbd_parent parent;
  r = require_feature(hctx, RBD_FEATURE_LAYERING);
  if (r < 0) {
    ::encode(parent.pool, *out);
    ::encode(parent.id, *out);
    ::encode(parent.snapid, *out);
    ::encode(parent.overlap, *out);
    return 0;
  }

  if (snap_id == CEPH_NOSNAP) {
    r = read_key(hctx, "parent", &parent);
    if (r < 0 && r != -ENOENT)
      return r;
  } else {
    cls_rbd_snap snap;
    string snapshot_key;
    key_from_snap_id(snap_id, &snapshot_key);
    r = read_key(hctx, snapshot_key, &snap);
    if (r < 0 && r != -ENOENT)
      return r;
    parent = snap.parent;
  }

  ::encode(parent.pool, *out);
  ::encode(parent.id, *out);
  ::encode(parent.snapid, *out);
  ::encode(parent.overlap, *out);
  return 0;
}

/**
 * set the image parent
 *
 * Input:
 * @param pool parent pool
 * @param id parent image id
 * @param snapid parent snapid
 * @param size parent size
 *
 * @returns 0 on success, or negative error code
 */
int set_parent(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int64_t pool;
  string id;
  snapid_t snapid;
  uint64_t size;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(pool, iter);
    ::decode(id, iter);
    ::decode(snapid, iter);
    ::decode(size, iter);
  } catch (const buffer::error &err) {
    CLS_LOG(20, "cls_rbd::set_parent: invalid decode");
    return -EINVAL;
  }

  int r = check_exists(hctx);
  if (r < 0) {
    CLS_LOG(20, "cls_rbd::set_parent: child already exists");
    return r;
  }

  r = require_feature(hctx, RBD_FEATURE_LAYERING);
  if (r < 0) {
    CLS_LOG(20, "cls_rbd::set_parent: child does not support layering");
    return r;
  }

  CLS_LOG(20, "set_parent pool=%lld id=%s snapid=%llu size=%llu",
	  pool, id.c_str(), snapid.val, size);

  if (pool < 0 || id.length() == 0 || snapid == CEPH_NOSNAP || size == 0) {
    return -EINVAL;
  }

  // make sure there isn't already a parent
  cls_rbd_parent parent;
  r = read_key(hctx, "parent", &parent);
  if (r == 0) {
    CLS_LOG(20, "set_parent existing parent pool=%lld id=%s snapid=%llu overlap=%llu",
	    parent.pool, parent.id.c_str(), parent.snapid.val,
	    parent.overlap);
    return -EEXIST;
  }

  // our overlap is the min of our size and the parent's size.
  uint64_t our_size;
  r = read_key(hctx, "size", &our_size);
  if (r < 0)
    return r;

  bufferlist parentbl;
  parent.pool = pool;
  parent.id = id;
  parent.snapid = snapid;
  parent.overlap = MIN(our_size, size);
  ::encode(parent, parentbl);
  r = cls_cxx_map_set_val(hctx, "parent", &parentbl);
  if (r < 0) {
    CLS_ERR("error writing parent: %d", r);
    return r;
  }

  return 0;
}


/**
 * remove the parent pointer
 *
 * This can only happen on the head, not on a snapshot.  No arguments.
 *
 * @returns 0 on success, negative error code on failure.
 */
int remove_parent(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = check_exists(hctx);
  if (r < 0)
    return r;

  r = require_feature(hctx, RBD_FEATURE_LAYERING);
  if (r < 0)
    return r;

  cls_rbd_parent parent;
  r = read_key(hctx, "parent", &parent);
  if (r < 0)
    return r;

  r = cls_cxx_map_remove_key(hctx, "parent");
  if (r < 0) {
    CLS_ERR("error removing parent: %d", r);
    return r;
  }

  return 0;
}


/**
 * Get the information needed to create a rados snap context for doing
 * I/O to the data objects. This must include all snapshots.
 *
 * Output:
 * @param snap_seq the highest snapshot id ever associated with the image (uint64_t)
 * @param snap_ids existing snapshot ids in descending order (vector<uint64_t>)
 * @returns 0 on success, negative error code on failure
 */
int get_snapcontext(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_snapcontext");

  int r;
  int max_read = RBD_MAX_KEYS_READ;
  vector<snapid_t> snap_ids;
  string last_read = RBD_SNAP_KEY_PREFIX;

  do {
    set<string> keys;
    r = cls_cxx_map_get_keys(hctx, last_read, max_read, &keys);
    if (r < 0)
      return r;

    for (set<string>::const_iterator it = keys.begin();
	 it != keys.end(); ++it) {
      snapid_t snap_id = snap_id_from_key(*it);
      snap_ids.push_back(snap_id);
    }
    if (keys.size() > 0)
      last_read = *(keys.rbegin());
  } while (r == max_read);

  uint64_t snap_seq;
  r = read_key(hctx, "snap_seq", &snap_seq);
  if (r < 0) {
    CLS_ERR("could not read the image's snap_seq off disk: %s", strerror(r));
    return r;
  }

  // snap_ids must be descending in a snap context
  std::reverse(snap_ids.begin(), snap_ids.end());

  ::encode(snap_seq, *out);
  ::encode(snap_ids, *out);

  return 0;
}

/**
 * Output:
 * @param object_prefix prefix for data object names (string)
 * @returns 0 on success, negative error code on failure
 */
int get_object_prefix(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(20, "get_object_prefix");

  string object_prefix;
  int r = read_key(hctx, "object_prefix", &object_prefix);
  if (r < 0) {
    CLS_ERR("failed to read the image's object prefix off of disk: %s",
            strerror(r));
    return r;
  }

  ::encode(object_prefix, *out);

  return 0;
}

int get_snapshot_name(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t snap_id;

  bufferlist::iterator iter = in->begin();
  try {
    ::decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "get_snapshot_name snap_id=%llu", snap_id);

  if (snap_id == CEPH_NOSNAP)
    return -EINVAL;

  cls_rbd_snap snap;
  string snapshot_key;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = read_key(hctx, snapshot_key, &snap);
  if (r < 0)
    return r;

  ::encode(snap.name, *out);

  return 0;
}

/**
 * Adds a snapshot to an rbd header. Ensures the id and name are unique.
 *
 * Input:
 * @param snap_name name of the snapshot (string)
 * @param snap_id id of the snapshot (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure.
 * @returns -ESTALE if the input snap_id is less than the image's snap_seq
 * @returns -EEXIST if the id or name are already used by another snapshot
 */
int snapshot_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist snap_namebl, snap_idbl;
  cls_rbd_snap snap_meta;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(snap_meta.name, iter);
    ::decode(snap_meta.id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "snapshot_add name=%s id=%llu", snap_meta.name.c_str(), snap_meta.id.val);

  if (snap_meta.id > CEPH_MAXSNAP)
    return -EINVAL;

  uint64_t cur_snap_seq;
  int r = read_key(hctx, "snap_seq", &cur_snap_seq);
  if (r < 0) {
    CLS_ERR("Could not read image's snap_seq off disk: %s", strerror(r));
    return r;
  }

  // client lost a race with another snapshot creation.
  // snap_seq must be monotonically increasing.
  if (snap_meta.id < cur_snap_seq)
    return -ESTALE;

  r = read_key(hctx, "size", &snap_meta.image_size);
  if (r < 0) {
    CLS_ERR("Could not read image's size off disk: %s", strerror(r));
    return r;
  }
  r = read_key(hctx, "features", &snap_meta.features);
  if (r < 0) {
    CLS_ERR("Could not read image's features off disk: %s", strerror(r));
    return r;
  }

  int max_read = RBD_MAX_KEYS_READ;
  string last_read = RBD_SNAP_KEY_PREFIX;
  do {
    map<string, bufferlist> vals;
    r = cls_cxx_map_get_vals(hctx, last_read, RBD_SNAP_KEY_PREFIX,
			     max_read, &vals);
    if (r < 0)
      return r;

    for (map<string, bufferlist>::iterator it = vals.begin();
	 it != vals.end(); ++it) {
      cls_rbd_snap old_meta;
      bufferlist::iterator iter = it->second.begin();
      try {
	::decode(old_meta, iter);
      } catch (const buffer::error &err) {
	snapid_t snap_id = snap_id_from_key(it->first);
	CLS_ERR("error decoding snapshot metadata for snap_id: %llu", snap_id.val);
	return -EIO;
      }
      if (snap_meta.name == old_meta.name || snap_meta.id == old_meta.id) {
	CLS_LOG(20, "snap_name %s or snap_id %llu matches existing snap %s %llu",
		snap_meta.name.c_str(), snap_meta.id.val,
		old_meta.name.c_str(), old_meta.id.val);
	return -EEXIST;
      }
    }

    if (vals.size() > 0)
      last_read = vals.rbegin()->first;
  } while (r == RBD_MAX_KEYS_READ);

  // snapshot inherits parent, if any
  cls_rbd_parent parent;
  r = read_key(hctx, "parent", &parent);
  if (r < 0 && r != -ENOENT)
    return r;
  if (r == 0) {
    snap_meta.parent = parent;
  }

  bufferlist snap_metabl, snap_seqbl;
  ::encode(snap_meta, snap_metabl);
  ::encode(snap_meta.id, snap_seqbl);

  string snapshot_key;
  key_from_snap_id(snap_meta.id, &snapshot_key);
  map<string, bufferlist> vals;
  vals["snap_seq"] = snap_seqbl;
  vals[snapshot_key] = snap_metabl;
  r = cls_cxx_map_set_vals(hctx, &vals);
  if (r < 0) {
    CLS_ERR("error writing snapshot metadata: %d", r);
    return r;
  }

  return 0;
}

/**
 * Removes a snapshot from an rbd header.
 *
 * Input:
 * @param snap_id the id of the snapshot to remove (uint64_t)
 *
 * Output:
 * @returns 0 on success, negative error code on failure
 */
int snapshot_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  snapid_t snap_id;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "snapshot_remove id=%llu", snap_id.val);

  // check if the key exists. we can rely on remove_key doing this for
  // us, since OMAPRMKEYS returns success if the key is not there.
  // bug or feature? sounds like a bug, since tmap did not have this
  // behavior, but cls_rgw may rely on it...
  string snapshot_key;
  bufferlist snapbl;
  key_from_snap_id(snap_id, &snapshot_key);
  int r = cls_cxx_map_get_val(hctx, snapshot_key, &snapbl);
  if (r == -ENOENT)
    return -ENOENT;

  r = cls_cxx_map_remove_key(hctx, snapshot_key);
  if (r < 0) {
    CLS_ERR("error writing snapshot metadata: %d", r);
    return r;
  }

  return 0;
}

/**
 * Returns a uint64_t of all the features supported by this class.
 */
int get_all_features(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t all_features = RBD_FEATURES_ALL;
  ::encode(all_features, *out);
  return 0;
}

/************************ rbd_id object methods **************************/

/**
 * Input:
 * @param in ignored
 *
 * Output:
 * @param id the id stored in the object
 * @returns 0 on success, negative error code on failure
 */
int get_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  uint64_t size;
  int r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;

  if (size == 0)
    return -ENOENT;

  bufferlist read_bl;
  r = cls_cxx_read(hctx, 0, size, &read_bl);
  if (r < 0) {
    CLS_ERR("get_id: could not read id: %d", r);
    return r;
  }

  string id;
  try {
    bufferlist::iterator iter = read_bl.begin();
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EIO;
  }

  ::encode(id, *out);
  return 0;
};

/**
 * Set the id of an image. The object must already exist.
 *
 * Input:
 * @param id the id of the image, as an alpha-numeric string
 *
 * Output:
 * @returns 0 on success, -EEXIST if the atomic create fails,
 *          negative error code on other error
 */
int set_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = check_exists(hctx);
  if (r < 0)
    return r;

  string id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  if (!is_valid_id(id)) {
    CLS_ERR("set_id: invalid id '%s'", id.c_str());
    return -EINVAL;
  }

  uint64_t size;
  r = cls_cxx_stat(hctx, &size, NULL);
  if (r < 0)
    return r;
  if (size != 0)
    return -EEXIST;

  CLS_LOG(20, "set_id: id=%s", id.c_str());

  bufferlist write_bl;
  ::encode(id, write_bl);
  return cls_cxx_write(hctx, 0, write_bl.length(), &write_bl);
}

/*********************** methods for rbd_directory ***********************/

static const string dir_key_for_id(const string &id)
{
  return RBD_DIR_ID_KEY_PREFIX + id;
}

static const string dir_key_for_name(const string &name)
{
  return RBD_DIR_NAME_KEY_PREFIX + name;
}

static const string dir_name_from_key(const string &key)
{
  return key.substr(strlen(RBD_DIR_NAME_KEY_PREFIX));
}

static int dir_add_image_helper(cls_method_context_t hctx,
				const string &name, const string &id,
				bool check_for_unique_id)
{
  if (!name.size() || !is_valid_id(id)) {
    CLS_ERR("dir_add_image_helper: invalid name '%s' or id '%s'",
	    name.c_str(), id.c_str());
    return -EINVAL;
  }

  CLS_LOG(20, "dir_add_image_helper name=%s id=%s", name.c_str(), id.c_str());

  string tmp;
  string name_key = dir_key_for_name(name);
  string id_key = dir_key_for_id(id);
  int r = read_key(hctx, name_key, &tmp);
  if (r != -ENOENT) {
    CLS_LOG(10, "name already exists");
    return -EEXIST;
  }
  r = read_key(hctx, id_key, &tmp);
  if (r != -ENOENT && check_for_unique_id) {
    CLS_LOG(10, "id already exists");
    return -EBADF;
  }
  bufferlist id_bl, name_bl;
  ::encode(id, id_bl);
  ::encode(name, name_bl);
  map<string, bufferlist> omap_vals;
  omap_vals[name_key] = id_bl;
  omap_vals[id_key] = name_bl;
  return cls_cxx_map_set_vals(hctx, &omap_vals);
}

static int dir_remove_image_helper(cls_method_context_t hctx,
				   const string &name, const string &id)
{
  CLS_LOG(20, "dir_remove_image_helper name=%s id=%s",
	  name.c_str(), id.c_str());

  string stored_name, stored_id;
  string name_key = dir_key_for_name(name);
  string id_key = dir_key_for_id(id);
  int r = read_key(hctx, name_key, &stored_id);
  if (r < 0) {
    CLS_ERR("error reading name to id mapping: %d", r);
    return r;
  }
  r = read_key(hctx, id_key, &stored_name);
  if (r < 0) {
    CLS_ERR("error reading id to name mapping: %d", r);
    return r;
  }

  // check if this op raced with a rename
  if (stored_name != name || stored_id != id) {
    CLS_ERR("stored name '%s' and id '%s' do not match args '%s' and '%s'",
	    stored_name.c_str(), stored_id.c_str(), name.c_str(), id.c_str());
    return -ESTALE;
  }

  r = cls_cxx_map_remove_key(hctx, name_key);
  if (r < 0) {
    CLS_ERR("error removing name: %d", r);
    return r;
  }

  r = cls_cxx_map_remove_key(hctx, id_key);
  if (r < 0) {
    CLS_ERR("error removing id: %d", r);
    return r;
  }

  return 0;
}

/**
 * Rename an image in the directory, updating both indexes
 * atomically. This can't be done from the client calling
 * dir_add_image and dir_remove_image in one transaction because the
 * results of the first method are not visibale to later steps.
 *
 * Input:
 * @param src original name of the image
 * @param dest new name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -ESTALE if src and id do not map to each other
 * @returns -ENOENT if src or id are not in the directory
 * @returns -EEXIST if dest already exists
 * @returns 0 on success, negative error code on failure
 */
int dir_rename_image(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string src, dest, id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(src, iter);
    ::decode(dest, iter);
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int r = dir_remove_image_helper(hctx, src, id);
  if (r < 0)
    return r;
  // ignore duplicate id because the result of
  // remove_image_helper is not visible yet
  return dir_add_image_helper(hctx, dest, id, false);
}

/**
 * Get the id of an image given its name.
 *
 * Input:
 * @param name the name of the image
 *
 * Output:
 * @param id the id of the image
 * @returns 0 on success, negative error code on failure
 */
int dir_get_id(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string name;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(name, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "dir_get_id: name=%s", name.c_str());

  string id;
  int r = read_key(hctx, dir_key_for_name(name), &id);
  if (r < 0) {
    CLS_ERR("error reading id for name '%s': %d", name.c_str(), r);
    return r;
  }
  ::encode(id, *out);
  return 0;
}

/**
 * Get the name of an image given its id.
 *
 * Input:
 * @param id the id of the image
 *
 * Output:
 * @param name the name of the image
 * @returns 0 on success, negative error code on failure
 */
int dir_get_name(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string id;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  CLS_LOG(20, "dir_get_name: id=%s", id.c_str());

  string name;
  int r = read_key(hctx, dir_key_for_id(id), &name);
  if (r < 0) {
    CLS_ERR("error reading name for id '%s': %d", id.c_str(), r);
    return r;
  }
  ::encode(name, *out);
  return 0;
}

/**
 * List the names and ids of the images in the directory, sorted by
 * name.
 *
 * Input:
 * @param start_after which name to begin listing after
 *        (use the empty string to start at the beginning)
 * @param max_return the maximum number of names to list
 *
 * Output:
 * @param images map from name to id of up to max_return images
 * @returns 0 on success, negative error code on failure
 */
int dir_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string start_after;
  uint64_t max_return;

  try {
    bufferlist::iterator iter = in->begin();
    ::decode(start_after, iter);
    ::decode(max_return, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  int max_read = RBD_MAX_KEYS_READ;
  int r = max_read;
  map<string, string> images;
  string last_read = dir_key_for_name(start_after);

  while (r == max_read && images.size() < max_return) {
    map<string, bufferlist> vals;
    CLS_LOG(20, "last_read = '%s'", last_read.c_str());
    r = cls_cxx_map_get_vals(hctx, last_read, RBD_DIR_NAME_KEY_PREFIX,
			     max_read, &vals);
    if (r < 0) {
      CLS_ERR("error reading directory by name: %d", r);
      return r;
    }

    for (map<string, bufferlist>::iterator it = vals.begin();
	 it != vals.end(); ++it) {
      string id;
      bufferlist::iterator iter = it->second.begin();
      try {
	::decode(id, iter);
      } catch (const buffer::error &err) {
	CLS_ERR("could not decode id of image '%s'", it->first.c_str());
	return -EIO;
      }
      CLS_LOG(20, "adding '%s' -> '%s'", dir_name_from_key(it->first).c_str(), id.c_str());
      images[dir_name_from_key(it->first)] = id;
      if (images.size() >= max_return)
	break;
    }
    if (vals.size() > 0) {
      last_read = images.rbegin()->first;
    }
  }

  ::encode(images, *out);

  return 0;
}

/**
 * Add an image to the rbd directory. Creates the directory object if
 * needed, and updates the index from id to name and name to id.
 *
 * Input:
 * @param name the name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -EEXIST if the image name is already in the directory
 * @returns -EBADF if the image id is already in the directory
 * @returns 0 on success, negative error code on failure
 */
int dir_add_image(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  int r = cls_cxx_create(hctx, false);
  if (r < 0) {
    CLS_ERR("could not create directory: error %d", r);
    return r;
  }

  string name, id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(name, iter);
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return dir_add_image_helper(hctx, name, id, true);
}

/**
 * Remove an image from the rbd directory.
 *
 * Input:
 * @param name the name of the image
 * @param id the id of the image
 *
 * Output:
 * @returns -ESTALE if the name and id do not map to each other
 * @returns 0 on success, negative error code on failure
 */
int dir_remove_image(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string name, id;
  try {
    bufferlist::iterator iter = in->begin();
    ::decode(name, iter);
    ::decode(id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }

  return dir_remove_image_helper(hctx, name, id);
}

/****************************** Old format *******************************/

int old_snapshots_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rbd_obj_header_ondisk *header;
  int rc = snap_read_header(hctx, bl);
  if (rc < 0)
    return rc;

  header = (struct rbd_obj_header_ondisk *)bl.c_str();
  bufferptr p(header->snap_names_len);
  char *buf = (char *)header;
  char *name = buf + sizeof(*header) + header->snap_count * sizeof(struct rbd_obj_snap_ondisk);
  char *end = name + header->snap_names_len;
  memcpy(p.c_str(),
         buf + sizeof(*header) + header->snap_count * sizeof(struct rbd_obj_snap_ondisk),
         header->snap_names_len);

  ::encode(header->snap_seq, *out);
  ::encode(header->snap_count, *out);

  for (unsigned i = 0; i < header->snap_count; i++) {
    string s = name;
    ::encode(header->snaps[i].id, *out);
    ::encode(header->snaps[i].image_size, *out);
    ::encode(s, *out);

    name += strlen(name) + 1;
    if (name > end)
      return -EIO;
  }

  return 0;
}

int old_snapshot_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rbd_obj_header_ondisk *header;
  bufferlist newbl;
  bufferptr header_bp(sizeof(*header));
  struct rbd_obj_snap_ondisk *new_snaps;

  int rc = snap_read_header(hctx, bl);
  if (rc < 0)
    return rc;

  header = (struct rbd_obj_header_ondisk *)bl.c_str();

  int snaps_id_ofs = sizeof(*header);
  int len = snaps_id_ofs;
  int names_ofs = snaps_id_ofs + sizeof(*new_snaps) * header->snap_count;
  const char *snap_name;
  const char *snap_names = ((char *)header) + names_ofs;
  const char *end = snap_names + header->snap_names_len;
  bufferlist::iterator iter = in->begin();
  string s;
  uint64_t snap_id;

  try {
    ::decode(s, iter);
    ::decode(snap_id, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }
  snap_name = s.c_str();

  const char *cur_snap_name;
  for (cur_snap_name = snap_names; cur_snap_name < end; cur_snap_name += strlen(cur_snap_name) + 1) {
    if (strncmp(cur_snap_name, snap_name, end - cur_snap_name) == 0)
      return -EEXIST;
  }
  if (cur_snap_name > end)
    return -EIO;

  int snap_name_len = strlen(snap_name);

  bufferptr new_names_bp(header->snap_names_len + snap_name_len + 1);
  bufferptr new_snaps_bp(sizeof(*new_snaps) * (header->snap_count + 1));

  /* copy snap names and append to new snap name */
  char *new_snap_names = new_names_bp.c_str();
  strcpy(new_snap_names, snap_name);
  memcpy(new_snap_names + snap_name_len + 1, snap_names, header->snap_names_len);

  /* append new snap id */
  new_snaps = (struct rbd_obj_snap_ondisk *)new_snaps_bp.c_str();
  memcpy(new_snaps + 1, header->snaps, sizeof(*new_snaps) * header->snap_count);

  header->snap_count = header->snap_count + 1;
  header->snap_names_len = header->snap_names_len + snap_name_len + 1;
  header->snap_seq = snap_id;

  new_snaps[0].id = snap_id;
  new_snaps[0].image_size = header->image_size;

  len += sizeof(*new_snaps) * header->snap_count + header->snap_names_len;

  memcpy(header_bp.c_str(), header, sizeof(*header));

  newbl.push_back(header_bp);
  newbl.push_back(new_snaps_bp);
  newbl.push_back(new_names_bp);

  rc = cls_cxx_write_full(hctx, &newbl);
  if (rc < 0)
    return rc;

  return 0;
}

int old_snapshot_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  struct rbd_obj_header_ondisk *header;
  bufferlist newbl;
  bufferptr header_bp(sizeof(*header));
  struct rbd_obj_snap_ondisk *new_snaps;

  int rc = snap_read_header(hctx, bl);
  if (rc < 0)
    return rc;

  header = (struct rbd_obj_header_ondisk *)bl.c_str();

  int snaps_id_ofs = sizeof(*header);
  int names_ofs = snaps_id_ofs + sizeof(*new_snaps) * header->snap_count;
  const char *snap_name;
  const char *snap_names = ((char *)header) + names_ofs;
  const char *orig_names = snap_names;
  const char *end = snap_names + header->snap_names_len;
  bufferlist::iterator iter = in->begin();
  string s;
  unsigned i;
  bool found = false;
  struct rbd_obj_snap_ondisk snap;

  try {
    ::decode(s, iter);
  } catch (const buffer::error &err) {
    return -EINVAL;
  }
  snap_name = s.c_str();

  for (i = 0; snap_names < end; i++) {
    if (strcmp(snap_names, snap_name) == 0) {
      snap = header->snaps[i];
      found = true;
      break;
    }
    snap_names += strlen(snap_names) + 1;
  }
  if (!found) {
    CLS_ERR("couldn't find snap %s\n", snap_name);
    return -ENOENT;
  }

  header->snap_names_len  = header->snap_names_len - (s.length() + 1);
  header->snap_count = header->snap_count - 1;

  bufferptr new_names_bp(header->snap_names_len);
  bufferptr new_snaps_bp(sizeof(header->snaps[0]) * header->snap_count);

  memcpy(header_bp.c_str(), header, sizeof(*header));
  newbl.push_back(header_bp);

  if (header->snap_count) {
    int snaps_len = 0;
    int names_len = 0;
    CLS_LOG(20, "i=%d\n", i);
    if (i > 0) {
      snaps_len = sizeof(header->snaps[0]) * i;
      names_len =  snap_names - orig_names;
      memcpy(new_snaps_bp.c_str(), header->snaps, snaps_len);
      memcpy(new_names_bp.c_str(), orig_names, names_len);
    }
    snap_names += s.length() + 1;

    if (i < header->snap_count) {
      memcpy(new_snaps_bp.c_str() + snaps_len,
             header->snaps + i + 1,
             sizeof(header->snaps[0]) * (header->snap_count - i));
      memcpy(new_names_bp.c_str() + names_len, snap_names , end - snap_names);
    }
    newbl.push_back(new_snaps_bp);
    newbl.push_back(new_names_bp);
  }

  rc = cls_cxx_write_full(hctx, &newbl);
  if (rc < 0)
    return rc;

  return 0;

}


/* assign block id. This method should be called on the rbd_info object */
int rbd_assign_bid(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  struct rbd_info info;
  int rc;
  bufferlist bl;

  rc = cls_cxx_read(hctx, 0, sizeof(info), &bl);
  if (rc < 0 && rc != -EEXIST)
    return rc;

  if (rc && rc < (int)sizeof(info)) {
    CLS_ERR("bad rbd_info object, read %d bytes, expected %d", rc, sizeof(info));
    return -EIO;
  }

  uint64_t max_id;
  if (rc) {
    memcpy(&info, bl.c_str(), sizeof(info));
    max_id = info.max_id + 1;
    info.max_id = max_id;
  } else {
    memset(&info, 0, sizeof(info));
    max_id = 0;
  }

  bufferlist newbl;
  bufferptr bp(sizeof(info));
  memcpy(bp.c_str(), &info, sizeof(info));
  newbl.push_back(bp);
  rc = cls_cxx_write_full(hctx, &newbl);
  if (rc < 0) {
    CLS_ERR("error writing rbd_info, got rc=%d", rc);
    return rc;
  }

  ::encode(max_id, *out);

  return out->length();
}

void __cls_init()
{
  CLS_LOG(20, "Loaded rbd class!");

  cls_register("rbd", &h_class);
  cls_register_cxx_method(h_class, "create",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  create, &h_create);
  cls_register_cxx_method(h_class, "get_features",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  get_features, &h_get_features);
  cls_register_cxx_method(h_class, "get_size",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  get_size, &h_get_size);
  cls_register_cxx_method(h_class, "set_size",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  set_size, &h_set_size);
  cls_register_cxx_method(h_class, "get_snapcontext",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  get_snapcontext, &h_get_snapcontext);
  cls_register_cxx_method(h_class, "get_object_prefix",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  get_object_prefix, &h_get_object_prefix);
  cls_register_cxx_method(h_class, "get_snapshot_name",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  get_snapshot_name, &h_get_snapshot_name);
  cls_register_cxx_method(h_class, "snapshot_add",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  snapshot_add, &h_snapshot_add);
  cls_register_cxx_method(h_class, "snapshot_remove",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  snapshot_remove, &h_snapshot_remove);
  cls_register_cxx_method(h_class, "get_all_features",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  get_all_features, &h_get_all_features);
  cls_register_cxx_method(h_class, "lock_exclusive",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
                          lock_image_exclusive, &h_lock_image_exclusive);
  cls_register_cxx_method(h_class, "lock_shared",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
                          lock_image_shared, &h_lock_image_shared);
  cls_register_cxx_method(h_class, "unlock_image",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
                          unlock_image, &h_unlock_image);
  cls_register_cxx_method(h_class, "break_lock",
                          CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
                          break_lock, &h_break_lock);
  cls_register_cxx_method(h_class, "list_locks",
                          CLS_METHOD_RD | CLS_METHOD_PUBLIC,
                          list_locks, &h_list_locks);
  cls_register_cxx_method(h_class, "get_parent",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  get_parent, &h_get_parent);
  cls_register_cxx_method(h_class, "set_parent",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  set_parent, &h_set_parent);
  cls_register_cxx_method(h_class, "remove_parent",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  remove_parent, &h_remove_parent);

  /* methods for the rbd_id.$image_name objects */
  cls_register_cxx_method(h_class, "get_id",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  get_id, &h_get_id);
  cls_register_cxx_method(h_class, "set_id",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  set_id, &h_set_id);

  /* methods for the rbd_directory object */
  cls_register_cxx_method(h_class, "dir_get_id",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  dir_get_id, &h_dir_get_id);
  cls_register_cxx_method(h_class, "dir_get_name",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  dir_get_name, &h_dir_get_name);
  cls_register_cxx_method(h_class, "dir_list",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  dir_list, &h_dir_list);
  cls_register_cxx_method(h_class, "dir_add_image",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  dir_add_image, &h_dir_add_image);
  cls_register_cxx_method(h_class, "dir_remove_image",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  dir_remove_image, &h_dir_remove_image);
  cls_register_cxx_method(h_class, "dir_rename_image",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  dir_rename_image, &h_dir_rename_image);

  /* methods for the old format */
  cls_register_cxx_method(h_class, "snap_list",
			  CLS_METHOD_RD | CLS_METHOD_PUBLIC,
			  old_snapshots_list, &h_old_snapshots_list);
  cls_register_cxx_method(h_class, "snap_add",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  old_snapshot_add, &h_old_snapshot_add);
  cls_register_cxx_method(h_class, "snap_remove",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  old_snapshot_remove, &h_old_snapshot_remove);

  /* assign a unique block id for rbd blocks */
  cls_register_cxx_method(h_class, "assign_bid",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PUBLIC,
			  rbd_assign_bid, &h_assign_bid);

  return;
}
