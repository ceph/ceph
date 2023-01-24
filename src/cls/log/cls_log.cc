// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include "common/ceph_time.h"

#include "objclass/objclass.h"

#include "cls_log_types.h"
#include "cls_log_ops.h"

#include "global/global_context.h"
#include "include/compat.h"

using std::map;
using std::string;

using ceph::bufferlist;
using namespace std::literals;

CLS_VER(1,0)
CLS_NAME(log)

static string log_index_prefix = "1_";


static int write_log_entry(cls_method_context_t hctx, string& index, cls_log_entry& entry)
{
  bufferlist bl;
  encode(entry, bl);

  int ret = cls_cxx_map_set_val(hctx, index, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static void get_index_time_prefix(ceph::real_time ts, string& index)
{
  auto tv = ceph::real_clock::to_timeval(ts);
  char buf[32];
  snprintf(buf, sizeof(buf), "%010ld.%06ld_", (long)tv.tv_sec, (long)tv.tv_usec);

  index = log_index_prefix + buf;
}

static int read_header(cls_method_context_t hctx, cls_log_header& header)
{
  bufferlist header_bl;

  int ret = cls_cxx_map_read_header(hctx, &header_bl);
  if (ret < 0)
    return ret;

  if (header_bl.length() == 0) {
    header = cls_log_header();
    return 0;
  }

  auto iter = header_bl.cbegin();
  try {
    decode(header, iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: read_header(): failed to decode header");
  }

  return 0;
}

static int write_header(cls_method_context_t hctx, cls_log_header& header)
{
  bufferlist header_bl;
  encode(header, header_bl);

  int ret = cls_cxx_map_write_header(hctx, &header_bl);
  if (ret < 0)
    return ret;

  return 0;
}

static void get_index(cls_method_context_t hctx, ceph::real_time ts, string& index)
{
  get_index_time_prefix(ts, index);

  string unique_id;

  cls_cxx_subop_version(hctx, &unique_id);

  index.append(unique_id);
}

static int cls_log_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_log_add_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_log_add_op(): failed to decode op");
    return -EINVAL;
  }

  cls_log_header header;

  int ret = read_header(hctx, header);
  if (ret < 0)
    return ret;

  for (auto iter = op.entries.begin(); iter != op.entries.end(); ++iter) {
    cls_log_entry& entry = *iter;

    string index;

    auto timestamp = entry.timestamp;
    if (op.monotonic_inc && timestamp < header.max_time)
      timestamp = header.max_time;
    else if (timestamp > header.max_time)
      header.max_time = timestamp;

    if (entry.id.empty()) {
      get_index(hctx, timestamp, index);
      entry.id = index;
    } else {
      index = entry.id;
    }

    CLS_LOG(20, "storing entry at %s", index.c_str());


    if (index > header.max_marker)
      header.max_marker = index;

    ret = write_log_entry(hctx, index, entry);
    if (ret < 0)
      return ret;
  }

  ret = write_header(hctx, header);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_log_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_log_list_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_log_list_op(): failed to decode op");
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
  bool use_time_boundary = (!ceph::real_clock::is_zero(op.from_time) && (op.to_time >= op.from_time));

  if (use_time_boundary)
    get_index_time_prefix(op.to_time, to_index);

  static constexpr auto MAX_ENTRIES = 1000u;
  size_t max_entries = op.max_entries;
  if (!max_entries || max_entries > MAX_ENTRIES)
    max_entries = MAX_ENTRIES;

  cls_log_list_ret ret;

  int rc = cls_cxx_map_get_vals(hctx, from_index, log_index_prefix, max_entries, &keys, &ret.truncated);
  if (rc < 0)
    return rc;

  auto& entries = ret.entries;
  auto iter = keys.begin();

  string marker;

  for (; iter != keys.end(); ++iter) {
    const string& index = iter->first;
    marker = index;
    if (use_time_boundary && index.compare(0, to_index.size(), to_index) >= 0) {
      ret.truncated = false;
      break;
    }

    bufferlist& bl = iter->second;
    auto biter = bl.cbegin();
    try {
      cls_log_entry e;
      decode(e, biter);
      entries.push_back(e);
    } catch (ceph::buffer::error& err) {
      CLS_LOG(0, "ERROR: cls_log_list: could not decode entry, index=%s", index.c_str());
    }
  }

  ret.marker = marker;

  encode(ret, *out);

  return 0;
}


static int cls_log_trim(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_log_trim_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: cls_log_trim(): failed to decode entry");
    return -EINVAL;
  }

  string from_index;
  string to_index;

  if (op.from_marker.empty()) {
    get_index_time_prefix(op.from_time, from_index);
  } else {
    from_index = op.from_marker;
  }

  // cls_cxx_map_remove_range() expects one-past-end
  if (op.to_marker.empty()) {
    auto t = op.to_time;
    t += 1000us; // equivalent to usec() += 1
    get_index_time_prefix(t, to_index);
  } else {
    to_index = op.to_marker;
    to_index.append(1, '\0');
  }

  // list a single key to detect whether the range is empty
  const size_t max_entries = 1;
  std::set<std::string> keys;
  bool more = false;

  int rc = cls_cxx_map_get_keys(hctx, from_index, max_entries, &keys, &more);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: cls_cxx_map_get_keys failed rc=%d", rc);
    return rc;
  }

  if (keys.empty()) {
    CLS_LOG(20, "range is empty from_index=%s", from_index.c_str());
    return -ENODATA;
  }

  const std::string& first_key = *keys.begin();
  if (to_index < first_key) {
    CLS_LOG(20, "listed key %s past to_index=%s", first_key.c_str(), to_index.c_str());
    return -ENODATA;
  }

  CLS_LOG(20, "listed key %s, removing through %s", first_key.c_str(), to_index.c_str());

  rc = cls_cxx_map_remove_range(hctx, first_key, to_index);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: cls_cxx_map_remove_range failed rc=%d", rc);
    return rc;
  }

  return 0;
}

static int cls_log_info(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_log_info_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_log_add_op(): failed to decode op");
    return -EINVAL;
  }

  cls_log_info_ret ret;

  int rc = read_header(hctx, ret.header);
  if (rc < 0)
    return rc;

  encode(ret, *out);

  return 0;
}

CLS_INIT(log)
{
  CLS_LOG(1, "Loaded log class!");

  cls_handle_t h_class;
  cls_method_handle_t h_log_add;
  cls_method_handle_t h_log_list;
  cls_method_handle_t h_log_trim;
  cls_method_handle_t h_log_info;

  cls_register("log", &h_class);

  /* log */
  cls_register_cxx_method(h_class, "add", CLS_METHOD_RD | CLS_METHOD_WR, cls_log_add, &h_log_add);
  cls_register_cxx_method(h_class, "list", CLS_METHOD_RD, cls_log_list, &h_log_list);
  cls_register_cxx_method(h_class, "trim", CLS_METHOD_RD | CLS_METHOD_WR, cls_log_trim, &h_log_trim);
  cls_register_cxx_method(h_class, "info", CLS_METHOD_RD, cls_log_info, &h_log_info);

  return;
}

