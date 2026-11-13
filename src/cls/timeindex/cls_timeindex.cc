// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
static const string EXTINDEX_PREFIX  = "2_";

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

  int ret = sscanf(index.c_str(), "1_%d.%d_", &sec, &usec);
  if (ret != 2) {
    return -EINVAL;
  }

  /* key_ext may be arbitrary long and contain any characters, so take
   * everything after the time prefix as is */
  auto pos = index.find('_', TIMEINDEX_PREFIX.size());
  if (pos == string::npos) {
    return -EINVAL;
  }

  key_ts  = utime_t(sec, usec);
  key_ext = index.substr(pos + 1);
  return 0;
}

static void get_ext_index_key(const string& key_ext,
                              string *ext_index)
{
  *ext_index = EXTINDEX_PREFIX;
  ext_index->append(key_ext);
}

static int get_ext_index(cls_method_context_t hctx,
                         const string& ext_index,
                         string *index)
{
  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, ext_index, &bl);
  if (ret < 0) {
    if (ret != -ENOENT) {
      CLS_LOG(1, "ERROR: cls_cxx_map_get_val failed ret=%d", ret);
    }
    return ret;
  }

  try {
    auto bl_iter = bl.cbegin();
    decode(*index, bl_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: failed to decode ext index %s", ext_index.c_str());
    return -EIO;
  }

  return 0;
}

/* Point the ext index for key_ext to the new index. If the ext index
 * was pointing to another (old) index, return it in old_index.
 * The omap updates are not visible to reads within the same op, so
 * the ext indexes already updated by this op are tracked in
 * added_ext_indexes. */
static int add_ext_index(cls_method_context_t hctx,
                         const string& key_ext,
                         const string& index,
                         map<string, string> *added_ext_indexes,
                         string *old_index)
{
  old_index->clear();

  if (key_ext.empty()) {
    return 0;
  }

  string ext_index;
  get_ext_index_key(key_ext, &ext_index);

  auto it = added_ext_indexes->find(ext_index);
  if (it != added_ext_indexes->end()) {
    *old_index = it->second;
  } else {
    int ret = get_ext_index(hctx, ext_index, old_index);
    if (ret == -EIO) {
      /* overwrite the corrupted ext index */
      old_index->clear();
    } else if (ret < 0 && ret != -ENOENT) {
      return ret;
    }
  }
  (*added_ext_indexes)[ext_index] = index;

  bufferlist bl;
  encode(index, bl);
  int ret = cls_cxx_map_set_val(hctx, ext_index, &bl);
  if (ret < 0) {
    CLS_LOG(1, "ERROR: cls_cxx_map_set_val failed ret=%d", ret);
    return ret;
  }

  return 0;
}

/* Remove the ext index for the index being trimmed, but only if it
 * points to this index. It may point to a newer index if the trimmed
 * entry was added before the ext index was introduced (e.g. before
 * upgrade), and so was not replaced by the newer one. */
static int remove_ext_index(cls_method_context_t hctx,
                            const string& index)
{
  utime_t key_ts;
  string key_ext;

  int ret = parse_index(index, key_ts, key_ext);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: could not parse index=%s", index.c_str());
    return 0;
  }
  if (key_ext.empty()) {
    return 0;
  }

  string ext_index;
  get_ext_index_key(key_ext, &ext_index);

  string cur_index;
  ret = get_ext_index(hctx, ext_index, &cur_index);
  if (ret == -ENOENT) {
    return 0;
  }
  if (ret < 0 && ret != -EIO) {
    return ret;
  }
  if (ret == 0 && cur_index != index) {
    CLS_LOG(20, "not removing ext index %s pointing to %s",
            ext_index.c_str(), cur_index.c_str());
    return 0;
  }

  /* remove also if the ext index is corrupted */
  ret = cls_cxx_map_remove_key(hctx, ext_index);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(1, "ERROR: cls_cxx_map_remove_key failed ret=%d", ret);
    return ret;
  }

  return 0;
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

  map<string, string> added_ext_indexes;

  for (auto iter = op.entries.begin();
       iter != op.entries.end();
       ++iter) {
    cls_timeindex_entry& entry = *iter;

    string index;
    get_index(hctx, entry.key_ts, entry.key_ext, index);

    string old_index;
    int ret = add_ext_index(hctx, entry.key_ext, index, &added_ext_indexes,
                            &old_index);
    if (ret < 0) {
      CLS_LOG(1, "ERROR: cls_timeindex_add_op(): failed to add ext index");
      return ret;
    }

    if (!old_index.empty() && old_index != index) {
      CLS_LOG(20, "removing old entry at %s", old_index.c_str());

      ret = cls_cxx_map_remove_key(hctx, old_index);
      if (ret < 0 && ret != -ENOENT) {
        CLS_LOG(1, "ERROR: cls_cxx_map_remove_key failed ret=%d", ret);
        return ret;
      }
    }

    CLS_LOG(20, "storing entry at %s", index.c_str());

    ret = cls_cxx_map_set_val(hctx, index, &entry.value);
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

    rc = remove_ext_index(hctx, index);
    if (rc < 0) {
      CLS_LOG(1, "ERROR: cls_timeindex_trim: failed to remove ext index");
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

  using namespace cls::timeindex;
  cls_register(ClassId::name, &h_class);
  ClassRegistrar<ClassId> cls(h_class);

  cls.register_cxx_method(method::add,  cls_timeindex_add,  &h_timeindex_add);
  cls.register_cxx_method(method::list, cls_timeindex_list, &h_timeindex_list);
  cls.register_cxx_method(method::trim, cls_timeindex_trim, &h_timeindex_trim);

  return;
}

