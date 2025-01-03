// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "objclass/objclass.h"

#include "cls_timeindex_ops.h"

#include "include/compat.h"

using std::map;
using std::string;

using ceph::bufferlist;

CLS_VER(1,0)
CLS_NAME(timeindex)

static const size_t MAX_LIST_ENTRIES = 1000;
static const size_t MAX_TRIM_ENTRIES = 1000;

static const string TIMEINDEX_PREFIX = "1_";

static void get_index_time_prefix(const utime_t& ts,
                                  string& index)
{
  char buf[32];

  snprintf(buf, sizeof(buf), "%s%010ld.%06ld_", TIMEINDEX_PREFIX.c_str(),
          (long)ts.sec(), (long)ts.usec());
  buf[sizeof(buf) - 1] = '\0';

  index = buf;
}

static void get_index(cls_method_context_t hctx,
                      const utime_t& key_ts,
                      const string& key_ext,
                      string& index)
{
  get_index_time_prefix(key_ts, index);
  index.append(key_ext);
}

static int parse_index(const string& index,
                       utime_t& key_ts,
                       string& key_ext)
{
  int sec, usec;
  char keyext[256];

  int ret = sscanf(index.c_str(), "1_%d.%d_%255s", &sec, &usec, keyext);

  key_ts  = utime_t(sec, usec);
  key_ext = string(keyext);
  return ret;
}

static int cls_timeindex_add(cls_method_context_t hctx,
                             bufferlist * const in,
                             bufferlist * const out)
{
  auto in_iter = in->cbegin();

  cls_timeindex_add_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_timeindex_add_op(): failed to decode op");
    return -EINVAL;
  }

  for (auto iter = op.entries.begin();
       iter != op.entries.end();
       ++iter) {
    cls_timeindex_entry& entry = *iter;

    string index;
    get_index(hctx, entry.key_ts, entry.key_ext, index);

    CLS_LOG(20, "storing entry at %s", index.c_str());

    int ret = cls_cxx_map_set_val(hctx, index, &entry.value);
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

static int cls_timeindex_list(cls_method_context_t hctx,
                              bufferlist * const in,
                              bufferlist * const out)
{
  auto in_iter = in->cbegin();

  cls_timeindex_list_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_timeindex_list_op(): failed to decode op");
    return -EINVAL;
  }

  map<string, bufferlist> keys;

  string from_index;
  string to_index;

  if (op.marker.empty()) {
    get_index_time_prefix(op.from_time, from_index);
  } else {
    from_index = op.marker;
  }
  const bool use_time_boundary = (op.to_time >= op.from_time);

  if (use_time_boundary) {
    get_index_time_prefix(op.to_time, to_index);
  }

  size_t max_entries = op.max_entries;
  if (max_entries > MAX_LIST_ENTRIES) {
    max_entries = MAX_LIST_ENTRIES;
  }

  cls_timeindex_list_ret ret;

  int rc = cls_cxx_map_get_vals(hctx, from_index, TIMEINDEX_PREFIX,
          max_entries, &keys, &ret.truncated);
  if (rc < 0) {
    return rc;
  }

  auto& entries = ret.entries;
  auto iter = keys.begin();

  string marker;

  for (; iter != keys.end(); ++iter) {
    const string& index = iter->first;
    bufferlist& bl = iter->second;

    if (use_time_boundary && index.compare(0, to_index.size(), to_index) >= 0) {
      CLS_LOG(20, "DEBUG: cls_timeindex_list: finishing on to_index=%s",
              to_index.c_str());
      ret.truncated = false;
      break;
    }

    cls_timeindex_entry e;

    if (parse_index(index, e.key_ts, e.key_ext) < 0) {
      CLS_LOG(0, "ERROR: cls_timeindex_list: could not parse index=%s",
              index.c_str());
    } else {
      CLS_LOG(20, "DEBUG: cls_timeindex_list: index=%s, key_ext=%s, bl.len = %d",
              index.c_str(), e.key_ext.c_str(), bl.length());
      e.value = bl;
      entries.push_back(e);
    }
    marker = index;
  }

  ret.marker = marker;

  encode(ret, *out);

  return 0;
}


static int cls_timeindex_trim(cls_method_context_t hctx,
                              bufferlist * const in,
                              bufferlist * const out)
{
  auto in_iter = in->cbegin();

  cls_timeindex_trim_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_timeindex_trim: failed to decode entry");
    return -EINVAL;
  }

  map<string, bufferlist> keys;

  string from_index;
  string to_index;

  if (op.from_marker.empty()) {
    get_index_time_prefix(op.from_time, from_index);
  } else {
    from_index = op.from_marker;
  }

  if (op.to_marker.empty()) {
    get_index_time_prefix(op.to_time, to_index);
  } else {
    to_index = op.to_marker;
  }

  bool more;

  int rc = cls_cxx_map_get_vals(hctx, from_index, TIMEINDEX_PREFIX,
          MAX_TRIM_ENTRIES, &keys, &more);
  if (rc < 0) {
    return rc;
  }

  auto iter = keys.begin();

  bool removed = false;
  for (; iter != keys.end(); ++iter) {
    const string& index = iter->first;

    CLS_LOG(20, "index=%s to_index=%s", index.c_str(), to_index.c_str());

    if (index.compare(0, to_index.size(), to_index) > 0) {
      CLS_LOG(20, "DEBUG: cls_timeindex_trim: finishing on to_index=%s",
              to_index.c_str());
      break;
    }

    CLS_LOG(20, "removing key: index=%s", index.c_str());

    int rc = cls_cxx_map_remove_key(hctx, index);
    if (rc < 0) {
      CLS_LOG(1, "ERROR: cls_cxx_map_remove_key failed rc=%d", rc);
      return rc;
    }

    removed = true;
  }

  if (!removed) {
    return -ENODATA;
  }

  return 0;
}

CLS_INIT(timeindex)
{
  CLS_LOG(1, "Loaded timeindex class!");

  cls_handle_t h_class;
  cls_method_handle_t h_timeindex_add;
  cls_method_handle_t h_timeindex_list;
  cls_method_handle_t h_timeindex_trim;

  cls_register("timeindex", &h_class);

  /* timeindex */
  cls_register_cxx_method(h_class, "add", CLS_METHOD_RD | CLS_METHOD_WR,
          cls_timeindex_add, &h_timeindex_add);
  cls_register_cxx_method(h_class, "list", CLS_METHOD_RD,
          cls_timeindex_list, &h_timeindex_list);
  cls_register_cxx_method(h_class, "trim", CLS_METHOD_RD | CLS_METHOD_WR,
          cls_timeindex_trim, &h_timeindex_trim);

  return;
}

