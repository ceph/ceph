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

#include "include/types.h"
#include "objclass/objclass.h"

#include "include/rbd_types.h"

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <errno.h>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>

CLS_VER(2,0)
CLS_NAME(rbd)

cls_handle_t h_class;
cls_method_handle_t h_create;
cls_method_handle_t h_get_features;
cls_method_handle_t h_get_size;
cls_method_handle_t h_set_size;
cls_method_handle_t h_get_snapcontext;
cls_method_handle_t h_get_object_prefix;
cls_method_handle_t h_get_snapshot_name;
cls_method_handle_t h_snapshot_add;
cls_method_handle_t h_snapshot_remove;
cls_method_handle_t h_get_all_features;
cls_method_handle_t h_old_snapshots_list;
cls_method_handle_t h_old_snapshot_add;
cls_method_handle_t h_old_snapshot_remove;
cls_method_handle_t h_assign_bid;

#define RBD_MAX_KEYS_READ 64
#define RBD_SNAP_KEY_PREFIX "snapshot_"

struct cls_rbd_snap {
  snapid_t id;
  string name;
  uint64_t image_size;
  uint64_t features;

  cls_rbd_snap() : id(CEPH_NOSNAP), image_size(0), features(0) {}
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(id, bl);
    ::encode(name, bl);
    ::encode(image_size, bl);
    ::encode(features, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& p) {
    DECODE_START(1, p);
    ::decode(id, p);
    ::decode(name, p);
    ::decode(image_size, p);
    ::decode(features, p);
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(cls_rbd_snap)

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
    CLS_ERR("error reading omap key %s: %d", key.c_str(), r);
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
    if (r < 0)
      return r;
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
  if (r < 0)
    return r;

  if (snap_id == CEPH_NOSNAP) {
    r = read_key(hctx, "size", &size);
    if (r < 0)
      return r;
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

  CLS_LOG(20, "set_size size=%llu", size);

  // check that size exists to make sure this is a header object
  // that was created correctly
  uint64_t orig_size;
  int r = read_key(hctx, "size", &orig_size);
  if (r < 0)
    return r;

  bufferlist sizebl;
  ::encode(size, sizebl);

  r = cls_cxx_map_set_val(hctx, "size", &sizebl);
  if (r < 0) {
    CLS_ERR("error writing snapshot metadata: %d", r);
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
  uint64_t max_read = RBD_MAX_KEYS_READ;
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
  } while (r == RBD_MAX_KEYS_READ);

  uint64_t snap_seq;
  r = read_key(hctx, "snap_seq", &snap_seq);
  if (r < 0)
    return r;

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
  if (r < 0)
    return r;

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
  if (r < 0)
    return r;

  // client lost a race with another snapshot creation.
  // snap_seq must be monotonically increasing.
  if (snap_meta.id < cur_snap_seq)
    return -ESTALE;

  r = read_key(hctx, "size", &snap_meta.image_size);
  if (r < 0)
    return r;
  r = read_key(hctx, "features", &snap_meta.features);
  if (r < 0)
    return r;

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

  if (header->snap_seq > snap_id)
    return -ESTALE;

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
