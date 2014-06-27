/*
 * OSD classes for the key value store
 *
 *  Created on: Aug 10, 2012
 *      Author: Eleanor Cawthon
 */

#include "objclass/objclass.h"
#include <errno.h>
#include "key_value_store/kvs_arg_types.h"
#include "include/types.h"
#include <iostream>
#include <climits>


cls_handle_t h_class;
cls_method_handle_t h_get_idata_from_key;
cls_method_handle_t h_get_next_idata;
cls_method_handle_t h_get_prev_idata;
cls_method_handle_t h_read_many;
cls_method_handle_t h_check_writable;
cls_method_handle_t h_assert_size_in_bound;
cls_method_handle_t h_omap_insert;
cls_method_handle_t h_create_with_omap;
cls_method_handle_t h_omap_remove;
cls_method_handle_t h_maybe_read_for_balance;


/**
 * finds the index_data where a key belongs.
 *
 * @param key: the key to search for
 * @param idata: the index_data for the first index value such that idata.key
 * is greater than key.
 * @param next_idata: the index_data for the next index entry after idata
 * @pre: key is not encoded
 * @post: idata contains complete information
 * stored
 */
static int get_idata_from_key(cls_method_context_t hctx, const string &key,
    index_data &idata, index_data &next_idata) {
  bufferlist raw_val;
  int r = 0;
  std::map<std::string, bufferlist> kvmap;

  r = cls_cxx_map_get_vals(hctx, key_data(key).encoded(), "", 2, &kvmap);
  if (r < 0) {
    CLS_LOG(20, "error reading index for range %s: %d", key.c_str(), r);
    return r;
  }

  r = cls_cxx_map_get_val(hctx, key_data(key).encoded(), &raw_val);
  if (r == 0){
    CLS_LOG(20, "%s is already in the index: %d", key.c_str(), r);
    bufferlist::iterator b = raw_val.begin();
    idata.decode(b);
    if (!kvmap.empty()) {
      bufferlist::iterator b = kvmap.begin()->second.begin();
      next_idata.decode(b);
    }
    return r;
  } else if (r == -ENOENT || r == -ENODATA) {
    bufferlist::iterator b = kvmap.begin()->second.begin();
    idata.decode(b);
    if (idata.kdata.prefix != "1") {
      bufferlist::iterator nb = (++kvmap.begin())->second.begin();
      next_idata.decode(nb);
    }
    r = 0;
  } else if (r < 0) {
    CLS_LOG(20, "error reading index for duplicates %s: %d", key.c_str(), r);
    return r;
  }

  CLS_LOG(20, "idata is %s", idata.str().c_str());
  return r;
}


static int get_idata_from_key_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "get_idata_from_key_op");
  idata_from_key_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    CLS_LOG(20, "error decoding idata_from_key_args.");
    return -EINVAL;
  }
  int r = get_idata_from_key(hctx, op.key, op.idata, op.next_idata);
  if (r < 0) {
    return r;
  } else {
    ::encode(op, *out);
    return 0;
  }
}

/**
 * finds the object in the index with the lowest key value that is greater
 * than idata.key. If idata.key is the max key, returns -EOVERFLOW. If
 * idata has a prefix and has timed out, cleans up.
 *
 * @param idata: idata for the object to search for.
 * @param out_data: the idata for the next object.
 *
 * @pre: idata must contain a key.
 * @post: out_data contains complete information
 */
static int get_next_idata(cls_method_context_t hctx, const index_data &idata,
    index_data &out_data) {
  int r = 0;
  std::map<std::string, bufferlist> kvs;
  r = cls_cxx_map_get_vals(hctx, idata.kdata.encoded(), "", 1, &kvs);
  if (r < 0){
    CLS_LOG(20, "getting kvs failed with error %d", r);
    return r;
  }

  if (!kvs.empty()) {
    out_data.kdata.parse(kvs.begin()->first);
    bufferlist::iterator b = kvs.begin()->second.begin();
    out_data.decode(b);
  } else {
    r = -EOVERFLOW;
  }

  return r;
}

static int get_next_idata_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "get_next_idata_op");
  idata_from_idata_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  int r = get_next_idata(hctx, op.idata, op.next_idata);
  if (r < 0) {
    return r;
  } else {
    op.encode(*out);
    return 0;
  }
}

/**
 * finds the object in the index with the highest key value that is less
 * than idata.key. If idata.key is the lowest key, returns -ERANGE If
 * idata has a prefix and has timed out, cleans up.
 *
 * @param idata: idata for the object to search for.
 * @param out_data: the idata for the next object.
 *
 * @pre: idata must contain a key.
 * @ost: out_data contains complete information
 */
static int get_prev_idata(cls_method_context_t hctx, const index_data &idata,
    index_data &out_data) {
  int r = 0;
  std::map<std::string, bufferlist> kvs;
  r = cls_cxx_map_get_vals(hctx, "", "", LONG_MAX, &kvs);
  if (r < 0){
    CLS_LOG(20, "getting kvs failed with error %d", r);
    return r;
  }

  std::map<std::string, bufferlist>::iterator it =
      kvs.lower_bound(idata.kdata.encoded());
  if (it->first != idata.kdata.encoded()) {
    CLS_LOG(20, "object %s not found in the index (expected %s, found %s)",
	idata.str().c_str(), idata.kdata.encoded().c_str(),
	it->first.c_str());
    return -ENODATA;
  }
  if (it == kvs.begin()) {
    //it is the first object, there is no previous.
    return -ERANGE;
  } else {
    --it;
  }
  out_data.kdata.parse(it->first);
  bufferlist::iterator b = it->second.begin();
  out_data.decode(b);

  return 0;
}

static int get_prev_idata_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "get_next_idata_op");
  idata_from_idata_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  int r = get_prev_idata(hctx, op.idata, op.next_idata);
  if (r < 0) {
    return r;
  } else {
    op.encode(*out);
    return 0;
  }
}

/**
 * Read all of the index entries where any keys in the map go
 */
static int read_many(cls_method_context_t hctx, const set<string> &keys,
    map<string, bufferlist> * out) {
  int r = 0;
  CLS_ERR("reading from a map of size %d, first key encoded is %s",
      (int)keys.size(), key_data(*keys.begin()).encoded().c_str());
  r = cls_cxx_map_get_vals(hctx, key_data(*keys.begin()).encoded().c_str(),
      "", LONG_MAX, out);
  if (r < 0) {
    CLS_ERR("getting omap vals failed with error %d", r);
  }

  CLS_ERR("got map of size %d ", (int)out->size());
  if (out->size() > 1) {
    out->erase(out->upper_bound(key_data(*keys.rbegin()).encoded().c_str()),
      out->end());
  }
  CLS_ERR("returning map of size %d", (int)out->size());
  return r;
}

static int read_many_op(cls_method_context_t hctx, bufferlist *in,
    bufferlist *out) {
  CLS_LOG(20, "read_many_op");
  set<string> op;
  map<string, bufferlist> outmap;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error & err) {
    return -EINVAL;
  }
  int r = read_many(hctx, op, &outmap);
  if (r < 0) {
    return r;
  } else {
    encode(outmap, *out);
    return 0;
  }
}

/**
 * Checks the unwritable xattr. If it is "1" (i.e., it is unwritable), returns
 * -EACCES. otherwise, returns 0.
 */
static int check_writable(cls_method_context_t hctx) {
  bufferlist bl;
  int r = cls_cxx_getxattr(hctx, "unwritable", &bl);
  if (r < 0) {
    CLS_LOG(20, "error reading xattr %s: %d", "unwritable", r);
    return r;
  }
  if (string(bl.c_str(), bl.length()) == "1") {
    return -EACCES;
  } else{
    return 0;
  }
}

static int check_writable_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "check_writable_op");
  return check_writable(hctx);
}

/**
 * returns -EKEYREJECTED if size is outside of bound, according to comparator.
 *
 * @bound: the limit to test
 * @comparator: should be CEPH_OSD_CMPXATTR_OP_[EQ|GT|LT]
 */
static int assert_size_in_bound(cls_method_context_t hctx, int bound,
    int comparator) {
  //determine size
  bufferlist size_bl;
  int r = cls_cxx_getxattr(hctx, "size", &size_bl);
  if (r < 0) {
    CLS_LOG(20, "error reading xattr %s: %d", "size", r);
    return r;
  }

  int size = atoi(string(size_bl.c_str(), size_bl.length()).c_str());
  CLS_LOG(20, "size is %d, bound is %d", size, bound);

  //compare size to comparator
  switch (comparator) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    if (size != bound) {
      return -EKEYREJECTED;
    }
    break;
  case CEPH_OSD_CMPXATTR_OP_LT:
    if (size >= bound) {
      return -EKEYREJECTED;
    }
    break;
  case CEPH_OSD_CMPXATTR_OP_GT:
    if (size <= bound) {
      return -EKEYREJECTED;
    }
    break;
  default:
    CLS_LOG(20, "invalid argument passed to assert_size_in_bound: %d",
	    comparator);
    return -EINVAL;
  }
  return 0;
}

static int assert_size_in_bound_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "assert_size_in_bound_op");
  assert_size_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  return assert_size_in_bound(hctx, op.bound, op.comparator);
}

/**
 * Attempts to insert omap into this object's omap.
 *
 * @return:
 * if unwritable, returns -EACCES.
 * if size > bound and key doesn't already exist in the omap, returns -EBALANCE.
 * if exclusive is true, returns -EEXIST if any keys already exist.
 *
 * @post: object has omap entries inserted, and size xattr is updated
 */
static int omap_insert(cls_method_context_t hctx,
    const map<string, bufferlist> &omap, int bound, bool exclusive) {

  uint64_t size;
  time_t time;
  int r = cls_cxx_stat(hctx, &size, &time);
  if (r < 0) {
    return r;
  }
  CLS_LOG(20, "inserting %s", omap.begin()->first.c_str());
  r = check_writable(hctx);
  if (r < 0) {
    CLS_LOG(20, "omap_insert: this object is unwritable: %d", r);
    return r;
  }

  int assert_bound = bound;

  //if this is an exclusive insert, make sure the key doesn't already exist.
  for (map<string, bufferlist>::const_iterator it = omap.begin();
      it != omap.end(); ++it) {
    bufferlist bl;
    r = cls_cxx_map_get_val(hctx, it->first, &bl);
    if (r == 0 && string(bl.c_str(), bl.length()) != ""){
      if (exclusive) {
	CLS_LOG(20, "error: this is an exclusive insert and %s exists.",
	    it->first.c_str());
	return -EEXIST;
      }
      assert_bound++;
      CLS_LOG(20, "increased assert_bound to %d", assert_bound);
    } else if (r != -ENODATA && r != -ENOENT) {
      CLS_LOG(20, "error reading omap val for %s: %d", it->first.c_str(), r);
      return r;
    }
  }

  bufferlist old_size;
  r = cls_cxx_getxattr(hctx, "size", &old_size);
  if (r < 0) {
    CLS_LOG(20, "error reading xattr %s: %d", "size", r);
    return r;
  }

  int old_size_int = atoi(string(old_size.c_str(), old_size.length()).c_str());

  CLS_LOG(20, "asserting size is less than %d (bound is %d)", assert_bound, bound);
  if (old_size_int >= assert_bound) {
    return -EKEYREJECTED;
  }

  int new_size_int = old_size_int + omap.size() - (assert_bound - bound);
  CLS_LOG(20, "old size is %d, new size is %d", old_size_int, new_size_int);
  bufferlist new_size;
  stringstream s;
  s << new_size_int;
  new_size.append(s.str());

  r = cls_cxx_map_set_vals(hctx, &omap);
  if (r < 0) {
    CLS_LOG(20, "error setting omap: %d", r);
    return r;
  }

  r = cls_cxx_setxattr(hctx, "size", &new_size);
  if (r < 0) {
    CLS_LOG(20, "error setting xattr %s: %d", "size", r);
    return r;
  }
  CLS_LOG(20, "successfully inserted %s", omap.begin()->first.c_str());
  return 0;
}

static int omap_insert_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "omap_insert");
  omap_set_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  return omap_insert(hctx, op.omap, op.bound, op.exclusive);
}

static int create_with_omap(cls_method_context_t hctx,
    const map<string, bufferlist> &omap) {
  CLS_LOG(20, "creating with omap: %s", omap.begin()->first.c_str());
  //first make sure the object is writable
  int r = cls_cxx_create(hctx, true);
  if (r < 0) {
    CLS_LOG(20, "omap create: creating failed: %d", r);
    return r;
  }

  int new_size_int = omap.size();
  CLS_LOG(20, "omap insert: new size is %d", new_size_int);
  bufferlist new_size;
  stringstream s;
  s << new_size_int;
  new_size.append(s.str());

  r = cls_cxx_map_set_vals(hctx, &omap);
  if (r < 0) {
    CLS_LOG(20, "omap create: error setting omap: %d", r);
    return r;
  }

  r = cls_cxx_setxattr(hctx, "size", &new_size);
  if (r < 0) {
    CLS_LOG(20, "omap create: error setting xattr %s: %d", "size", r);
    return r;
  }

  bufferlist u;
  u.append("0");
  r = cls_cxx_setxattr(hctx, "unwritable", &u);
  if (r < 0) {
    CLS_LOG(20, "omap create: error setting xattr %s: %d", "unwritable", r);
    return r;
  }

  CLS_LOG(20, "successfully created %s", omap.begin()->first.c_str());
  return 0;
}

static int create_with_omap_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "omap_insert");
  map<string, bufferlist> omap;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(omap, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  return create_with_omap(hctx, omap);
}

/**
 * Attempts to remove omap from this object's omap.
 *
 * @return:
 * if unwritable, returns -EACCES.
 * if size < bound and key doesn't already exist in the omap, returns -EBALANCE.
 * if any of the keys are not in this object, returns -ENODATA.
 *
 * @post: object has omap entries removed, and size xattr is updated
 */
static int omap_remove(cls_method_context_t hctx,
    const std::set<string> &omap, int bound) {
  int r;
  uint64_t size;
  time_t time;
  r = cls_cxx_stat(hctx, &size, &time);
  if (r < 0) {
    return r;
  }

  //first make sure the object is writable
  r = check_writable(hctx);
  if (r < 0) {
    return r;
  }

  //check for existance of the key first
  for (set<string>::const_iterator it = omap.begin();
      it != omap.end(); ++it) {
    bufferlist bl;
    r = cls_cxx_map_get_val(hctx, *it, &bl);
    if (r == -ENOENT || r == -ENODATA
	|| string(bl.c_str(), bl.length()) == ""){
      return -ENODATA;
    } else if (r < 0) {
      CLS_LOG(20, "error reading omap val for %s: %d", it->c_str(), r);
      return r;
    }
  }

  //fail if removing from an object with only bound entries.
  bufferlist old_size;
  r = cls_cxx_getxattr(hctx, "size", &old_size);
  if (r < 0) {
    CLS_LOG(20, "error reading xattr %s: %d", "size", r);
    return r;
  }
  int old_size_int = atoi(string(old_size.c_str(), old_size.length()).c_str());

  CLS_LOG(20, "asserting size is greater than %d", bound);
  if (old_size_int <= bound) {
    return -EKEYREJECTED;
  }

  int new_size_int = old_size_int - omap.size();
  CLS_LOG(20, "old size is %d, new size is %d", old_size_int, new_size_int);
  bufferlist new_size;
  stringstream s;
  s << new_size_int;
  new_size.append(s.str());

  r = cls_cxx_setxattr(hctx, "size", &new_size);
  if (r < 0) {
    CLS_LOG(20, "error setting xattr %s: %d", "unwritable", r);
    return r;
  }

  for (std::set<string>::const_iterator it = omap.begin();
      it != omap.end(); ++it) {
    r = cls_cxx_map_remove_key(hctx, *it);
    if (r < 0) {
      CLS_LOG(20, "error removing omap: %d", r);
      return r;
    }
  }
  return 0;
}

static int omap_remove_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "omap_remove");
  omap_rm_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  return omap_remove(hctx, op.omap, op.bound);
}

/**
 * checks to see if this object needs to be split or rebalanced. if so, reads
 * information about it.
 *
 * @post: if assert_size_in_bound(hctx, bound, comparator) succeeds,
 * odata contains the size, omap, and unwritable attributes for this object.
 * Otherwise, odata contains the size and unwritable attribute.
 */
static int maybe_read_for_balance(cls_method_context_t hctx,
    object_data &odata, int bound, int comparator) {
  CLS_LOG(20, "rebalance reading");
  //if unwritable, return
  int r = check_writable(hctx);
  if (r < 0) {
    odata.unwritable = true;
    CLS_LOG(20, "rebalance read: error getting xattr %s: %d", "unwritable", r);
    return r;
  } else {
    odata.unwritable = false;
  }

  //get the size attribute
  bufferlist size;
  r = cls_cxx_getxattr(hctx, "size", &size);
  if (r < 0) {
    CLS_LOG(20, "rebalance read: error getting xattr %s: %d", "size", r);
    return r;
  }
  odata.size = atoi(string(size.c_str(), size.length()).c_str());

  //check if it needs to be balanced
  r = assert_size_in_bound(hctx, bound, comparator);
  if (r < 0) {
    CLS_LOG(20, "rebalance read: error on asserting size: %d", r);
    return -EBALANCE;
  }

  //if the assert succeeded, it needs to be balanced
  r = cls_cxx_map_get_vals(hctx, "", "", LONG_MAX, &odata.omap);
  if (r < 0){
    CLS_LOG(20, "rebalance read: getting kvs failed with error %d", r);
    return r;
  }

  CLS_LOG(20, "rebalance read: size xattr is %llu, omap size is %llu",
	  (unsigned long long)odata.size,
	  (unsigned long long)odata.omap.size());
  return 0;
}

static int maybe_read_for_balance_op(cls_method_context_t hctx,
                   bufferlist *in, bufferlist *out) {
  CLS_LOG(20, "maybe_read_for_balance");
  rebalance_args op;
  bufferlist::iterator it = in->begin();
  try {
    ::decode(op, it);
  } catch (buffer::error& err) {
    return -EINVAL;
  }
  int r = maybe_read_for_balance(hctx, op.odata, op.bound, op.comparator);
  if (r < 0) {
    return r;
  } else {
    op.encode(*out);
    return 0;
  }
}


void __cls_init()
{
  CLS_LOG(20, "Loaded assert condition class!");

  cls_register("kvs", &h_class);
  cls_register_cxx_method(h_class, "get_idata_from_key",
                          CLS_METHOD_RD,
                          get_idata_from_key_op, &h_get_idata_from_key);
  cls_register_cxx_method(h_class, "get_next_idata",
                          CLS_METHOD_RD,
                          get_next_idata_op, &h_get_next_idata);
  cls_register_cxx_method(h_class, "get_prev_idata",
                          CLS_METHOD_RD,
                          get_prev_idata_op, &h_get_prev_idata);
  cls_register_cxx_method(h_class, "read_many",
                          CLS_METHOD_RD,
                          read_many_op, &h_read_many);
  cls_register_cxx_method(h_class, "check_writable",
                          CLS_METHOD_RD | CLS_METHOD_WR,
                          check_writable_op, &h_check_writable);
  cls_register_cxx_method(h_class, "assert_size_in_bound",
                          CLS_METHOD_WR,
                          assert_size_in_bound_op, &h_assert_size_in_bound);
  cls_register_cxx_method(h_class, "omap_insert",
                          CLS_METHOD_WR,
                          omap_insert_op, &h_omap_insert);
  cls_register_cxx_method(h_class, "create_with_omap",
			  CLS_METHOD_WR,
			  create_with_omap_op, &h_create_with_omap);
  cls_register_cxx_method(h_class, "omap_remove",
                          CLS_METHOD_WR,
                          omap_remove_op, &h_omap_remove);
  cls_register_cxx_method(h_class, "maybe_read_for_balance",
                          CLS_METHOD_RD,
                          maybe_read_for_balance_op, &h_maybe_read_for_balance);

  return;
}
