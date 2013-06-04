// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "common/Clock.h"

#include "global/global_context.h"

CLS_VER(1,0)
CLS_NAME(rgw)

cls_handle_t h_class;
cls_method_handle_t h_rgw_bucket_init_index;
cls_method_handle_t h_rgw_bucket_set_tag_timeout;
cls_method_handle_t h_rgw_bucket_list;
cls_method_handle_t h_rgw_bucket_check_index;
cls_method_handle_t h_rgw_bucket_rebuild_index;
cls_method_handle_t h_rgw_bucket_prepare_op;
cls_method_handle_t h_rgw_bucket_complete_op;
cls_method_handle_t h_rgw_dir_suggest_changes;
cls_method_handle_t h_rgw_user_usage_log_add;
cls_method_handle_t h_rgw_user_usage_log_read;
cls_method_handle_t h_rgw_user_usage_log_trim;
cls_method_handle_t h_rgw_gc_set_entry;
cls_method_handle_t h_rgw_gc_list;
cls_method_handle_t h_rgw_gc_remove;


#define ROUND_BLOCK_SIZE 4096

static uint64_t get_rounded_size(uint64_t size)
{
  return (size + ROUND_BLOCK_SIZE - 1) & ~(ROUND_BLOCK_SIZE - 1);
}

int rgw_bucket_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator iter = in->begin();

  struct rgw_cls_list_op op;
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to decode request\n");
    return -EINVAL;
  }

  struct rgw_cls_list_ret ret;
  struct rgw_bucket_dir& new_dir = ret.dir;
  bufferlist header_bl;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0)
    return rc;
  bufferlist::iterator header_iter = header_bl.begin();
  try {
    ::decode(new_dir.header, header_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to decode header\n");
    return -EINVAL;
  }

  bufferlist bl;

  map<string, bufferlist> keys;
  rc = cls_cxx_map_get_vals(hctx, op.start_obj, op.filter_prefix, op.num_entries + 1, &keys);
  if (rc < 0)
    return rc;

  std::map<string, struct rgw_bucket_dir_entry>& m = new_dir.m;
  std::map<string, bufferlist>::iterator kiter = keys.begin();
  uint32_t i;

  for (i = 0; i < op.num_entries && kiter != keys.end(); ++i, ++kiter) {
    struct rgw_bucket_dir_entry entry;
    bufferlist& entrybl = kiter->second;
    bufferlist::iterator eiter = entrybl.begin();
    try {
      ::decode(entry, eiter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to decode entry, key=%s\n", kiter->first.c_str());
      return -EINVAL;
    }
    
    m[kiter->first] = entry;
  }

  ret.is_truncated = (kiter != keys.end());

  ::encode(ret, *out);
  return 0;
}

static int check_index(cls_method_context_t hctx, struct rgw_bucket_dir_header *existing_header, struct rgw_bucket_dir_header *calc_header)
{
  bufferlist header_bl;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0)
    return rc;
  bufferlist::iterator header_iter = header_bl.begin();
  try {
    ::decode(*existing_header, header_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to decode header\n");
    return -EINVAL;
  }

  calc_header->tag_timeout = existing_header->tag_timeout;

  bufferlist bl;

  map<string, bufferlist> keys;
  string start_obj;
  string filter_prefix;

#define CHECK_CHUNK_SIZE 1000
  do {
    rc = cls_cxx_map_get_vals(hctx, start_obj, filter_prefix, CHECK_CHUNK_SIZE, &keys);
    if (rc < 0)
      return rc;

    std::map<string, bufferlist>::iterator kiter = keys.begin();
    for (; kiter != keys.end(); ++kiter) {
      struct rgw_bucket_dir_entry entry;
      bufferlist::iterator eiter = kiter->second.begin();
      try {
        ::decode(entry, eiter);
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to decode entry, key=%s\n", kiter->first.c_str());
        return -EIO;
      }
      struct rgw_bucket_category_stats& stats = calc_header->stats[entry.meta.category];
      stats.num_entries++;
      stats.total_size += entry.meta.size;
      stats.total_size_rounded += get_rounded_size(entry.meta.size);

      start_obj = kiter->first;
    }
  } while (keys.size() == CHECK_CHUNK_SIZE);

  return 0;
}

int rgw_bucket_check_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  struct rgw_cls_check_index_ret ret;

  int rc = check_index(hctx, &ret.existing_header, &ret.calculated_header);
  if (rc < 0)
    return rc;

  ::encode(ret, *out);

  return 0;
}

int rgw_bucket_rebuild_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  struct rgw_bucket_dir_header existing_header;
  struct rgw_bucket_dir_header calc_header;
  int rc = check_index(hctx, &existing_header, &calc_header);
  if (rc < 0)
    return rc;

  bufferlist header_bl;
  ::encode(calc_header, header_bl);
  rc = cls_cxx_map_write_header(hctx, &header_bl);
  return rc;
}


int rgw_bucket_init_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist bl;
  bufferlist::iterator iter;

  bufferlist header_bl;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0) {
    switch (rc) {
    case -ENODATA:
    case -ENOENT:
      break;
    default:
      return rc;
    }
  }

  if (header_bl.length() != 0) {
    CLS_LOG(1, "ERROR: index already initialized\n");
    return -EINVAL;
  }

  rgw_bucket_dir dir;
  ::encode(dir.header, header_bl);
  rc = cls_cxx_map_write_header(hctx, &header_bl);
  return rc;
}

int rgw_bucket_set_tag_timeout(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_tag_timeout_op op;
  bufferlist::iterator iter = in->begin();
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_set_tag_timeout(): failed to decode request\n");
    return -EINVAL;
  }

  bufferlist header_bl;
  struct rgw_bucket_dir_header header;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0)
    return rc;
  bufferlist::iterator header_iter = header_bl.begin();
  try {
    ::decode(header, header_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_complete_op(): failed to decode header\n");
    return -EINVAL;
  }

  header.tag_timeout = op.tag_timeout;

  header_bl.clear();
  ::encode(header, header_bl);
  rc = cls_cxx_map_write_header(hctx, &header_bl);
  return rc;
}

int rgw_bucket_prepare_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_prepare_op op;
  bufferlist::iterator iter = in->begin();
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_prepare_op(): failed to decode request\n");
    return -EINVAL;
  }

  if (op.tag.empty()) {
    CLS_LOG(1, "ERROR: tag is empty\n");
    return -EINVAL;
  }

  CLS_LOG(1, "rgw_bucket_prepare_op(): request: op=%d name=%s tag=%s\n", op.op, op.name.c_str(), op.tag.c_str());

  // get on-disk state
  bufferlist cur_value;
  int rc = cls_cxx_map_get_val(hctx, op.name, &cur_value);
  if (rc < 0 && rc != -ENOENT)
    return rc;

  struct rgw_bucket_dir_entry entry;

  bool noent = (rc == -ENOENT);

  rc = 0;

  if (!noent) {
    try {
      bufferlist::iterator biter = cur_value.begin();
      ::decode(entry, biter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: rgw_bucket_prepare_op(): failed to decode entry\n");
      /* ignoring error */

      noent = true;
    }
  }

  if (noent) { // no entry, initialize fields
    entry.name = op.name;
    entry.epoch = 0;
    entry.exists = false;
    entry.locator = op.locator;
  }

  // fill in proper state
  struct rgw_bucket_pending_info& info = entry.pending_map[op.tag];
  info.timestamp = ceph_clock_now(g_ceph_context);
  info.state = CLS_RGW_STATE_PENDING_MODIFY;
  info.op = op.op;

  // write out new key to disk
  bufferlist info_bl;
  ::encode(entry, info_bl);
  rc = cls_cxx_map_set_val(hctx, op.name, &info_bl);
  return rc;
}

static void unaccount_entry(struct rgw_bucket_dir_header& header, struct rgw_bucket_dir_entry& entry)
{
  struct rgw_bucket_category_stats& stats = header.stats[entry.meta.category];
  stats.num_entries--;
  stats.total_size -= entry.meta.size;
  stats.total_size_rounded -= get_rounded_size(entry.meta.size);
}

static int read_index_entry(cls_method_context_t hctx, string& name, struct rgw_bucket_dir_entry *entry)
{
  bufferlist current_entry;
  int rc = cls_cxx_map_get_val(hctx, name, &current_entry);
  if (rc < 0) {
    return rc;
  }

  bufferlist::iterator cur_iter = current_entry.begin();
  try {
    ::decode(*entry, cur_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: read_index_entry(): failed to decode entry\n");
    return -EIO;
  }

  CLS_LOG(1, "read_index_entry(): existing entry: epoch=%llu name=%s locator=%s\n", (unsigned long long)entry->epoch, entry->name.c_str(), entry->locator.c_str());
  return 0;
}

int rgw_bucket_complete_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_complete_op op;
  bufferlist::iterator iter = in->begin();
  try {
    ::decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_complete_op(): failed to decode request\n");
    return -EINVAL;
  }
  CLS_LOG(1, "rgw_bucket_complete_op(): request: op=%d name=%s epoch=%llu tag=%s\n", op.op, op.name.c_str(), (unsigned long long)op.epoch, op.tag.c_str());

  bufferlist header_bl;
  struct rgw_bucket_dir_header header;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0)
    return rc;
  bufferlist::iterator header_iter = header_bl.begin();
  try {
    ::decode(header, header_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_complete_op(): failed to decode header\n");
    return -EINVAL;
  }

  struct rgw_bucket_dir_entry entry;
  bool ondisk = true;

  rc = read_index_entry(hctx, op.name, &entry);
  if (rc == -ENOENT) {
    entry.name = op.name;
    entry.epoch = op.epoch;
    entry.meta = op.meta;
    entry.locator = op.locator;
    ondisk = false;
  } else if (rc < 0) {
    return rc;
  }

  if (op.tag.size()) {
    map<string, struct rgw_bucket_pending_info>::iterator pinter = entry.pending_map.find(op.tag);
    if (pinter == entry.pending_map.end()) {
      CLS_LOG(1, "ERROR: couldn't find tag for pending operation\n");
      return -EINVAL;
    }
    entry.pending_map.erase(pinter);
  }

  bool cancel = false;
  bufferlist update_bl;

  if (op.tag.size() && op.op == CLS_RGW_OP_CANCEL) {
    CLS_LOG(1, "rgw_bucket_complete_op(): cancel requested\n");
    cancel = true;
  } else if (op.epoch && op.epoch <= entry.epoch) {
    CLS_LOG(1, "rgw_bucket_complete_op(): skipping request, old epoch\n");
    cancel = true;
  }

  bufferlist op_bl;
  if (cancel) {
    if (op.tag.size()) {
      bufferlist new_key_bl;
      ::encode(entry, new_key_bl);
      return cls_cxx_map_set_val(hctx, op.name, &new_key_bl);
    } else {
      return 0;
    }
  }

  if (entry.exists) {
    unaccount_entry(header, entry);
  }

  switch (op.op) {
  case CLS_RGW_OP_DEL:
    if (ondisk) {
      if (!entry.pending_map.size()) {
	int ret = cls_cxx_map_remove_key(hctx, op.name);
	if (ret < 0)
	  return ret;
      } else {
        entry.exists = false;
        bufferlist new_key_bl;
        ::encode(entry, new_key_bl);
	int ret = cls_cxx_map_set_val(hctx, op.name, &new_key_bl);
	if (ret < 0)
	  return ret;
      }
    } else {
      return -ENOENT;
    }
    break;
  case CLS_RGW_OP_ADD:
    {
      struct rgw_bucket_dir_entry_meta& meta = op.meta;
      struct rgw_bucket_category_stats& stats = header.stats[meta.category];
      entry.meta = meta;
      entry.name = op.name;
      entry.epoch = op.epoch;
      entry.exists = true;
      stats.num_entries++;
      stats.total_size += meta.size;
      stats.total_size_rounded += get_rounded_size(meta.size);
      bufferlist new_key_bl;
      ::encode(entry, new_key_bl);
      int ret = cls_cxx_map_set_val(hctx, op.name, &new_key_bl);
      if (ret < 0)
	return ret;
    }
    break;
  }

  list<string>::iterator remove_iter;
  CLS_LOG(0, "rgw_bucket_complete_op(): remove_objs.size()=%d\n", (int)op.remove_objs.size());
  for (remove_iter = op.remove_objs.begin(); remove_iter != op.remove_objs.end(); ++remove_iter) {
    string& remove_oid_name = *remove_iter;
    CLS_LOG(1, "rgw_bucket_complete_op(): removing entries, read_index_entry name=%s\n", remove_oid_name.c_str());
    struct rgw_bucket_dir_entry remove_entry;
    int ret = read_index_entry(hctx, remove_oid_name, &remove_entry);
    if (ret < 0) {
      CLS_LOG(1, "rgw_bucket_complete_op(): removing entries, read_index_entry name=%s ret=%d\n", remove_oid_name.c_str(), rc);
      continue;
    }
    CLS_LOG(0, "rgw_bucket_complete_op(): entry.name=%s entry.meta.category=%d\n", remove_entry.name.c_str(), remove_entry.meta.category);
    unaccount_entry(header, remove_entry);

    ret = cls_cxx_map_remove_key(hctx, remove_oid_name);
    if (ret < 0) {
      CLS_LOG(1, "rgw_bucket_complete_op(): cls_cxx_map_remove_key, failed to remove entry, name=%s read_index_entry ret=%d\n", remove_oid_name.c_str(), rc);
      continue;
    }
  }

  bufferlist new_header_bl;
  ::encode(header, new_header_bl);
  return cls_cxx_map_write_header(hctx, &new_header_bl);
}

int rgw_dir_suggest_changes(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "rgw_dir_suggest_changes()");

  bufferlist header_bl;
  struct rgw_bucket_dir_header header;
  bool header_changed = false;
  int rc = cls_cxx_map_read_header(hctx, &header_bl);
  if (rc < 0)
    return rc;

  uint64_t tag_timeout;

  try {
    bufferlist::iterator header_iter = header_bl.begin();
    ::decode(header, header_iter);
  } catch (buffer::error& error) {
    CLS_LOG(1, "ERROR: rgw_dir_suggest_changes(): failed to decode header\n");
    return -EINVAL;
  }

  tag_timeout = (header.tag_timeout ? header.tag_timeout : CEPH_RGW_TAG_TIMEOUT);

  bufferlist::iterator in_iter = in->begin();

  while (!in_iter.end()) {
    __u8 op;
    rgw_bucket_dir_entry cur_change;
    rgw_bucket_dir_entry cur_disk;
    try {
      ::decode(op, in_iter);
      ::decode(cur_change, in_iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: rgw_dir_suggest_changes(): failed to decode request\n");
      return -EINVAL;
    }

    bufferlist cur_disk_bl;
    int ret = cls_cxx_map_get_val(hctx, cur_change.name, &cur_disk_bl);
    if (ret < 0 && ret != -ENOENT)
      return -EINVAL;

    if (cur_disk_bl.length()) {
      bufferlist::iterator cur_disk_iter = cur_disk_bl.begin();
      try {
        ::decode(cur_disk, cur_disk_iter);
      } catch (buffer::error& error) {
        CLS_LOG(1, "ERROR: rgw_dir_suggest_changes(): failed to decode cur_disk\n");
        return -EINVAL;
      }

      utime_t cur_time = ceph_clock_now(g_ceph_context);
      map<string, struct rgw_bucket_pending_info>::iterator iter =
                cur_disk.pending_map.begin();
      while(iter != cur_disk.pending_map.end()) {
        map<string, struct rgw_bucket_pending_info>::iterator cur_iter=iter++;
        if (cur_time > (cur_iter->second.timestamp + tag_timeout)) {
          cur_disk.pending_map.erase(cur_iter);
        }
      }
    }

    CLS_LOG(20, "cur_disk.pending_map.empty()=%d op=%d cur_disk.exists=%d cur_change.pending_map.size()=%d cur_change.exists=%d\n",
	    cur_disk.pending_map.empty(), (int)op, cur_disk.exists,
	    (int)cur_change.pending_map.size(), cur_change.exists);

    if (cur_disk.pending_map.empty()) {
      if (cur_disk.exists) {
        struct rgw_bucket_category_stats& old_stats = header.stats[cur_disk.meta.category];
        CLS_LOG(10, "total_entries: %"PRId64" -> %"PRId64"\n", old_stats.num_entries, old_stats.num_entries - 1);
        old_stats.num_entries--;
        old_stats.total_size -= cur_disk.meta.size;
        old_stats.total_size_rounded -= get_rounded_size(cur_disk.meta.size);
        header_changed = true;
      }
      struct rgw_bucket_category_stats& stats =
          header.stats[cur_change.meta.category];
      switch(op) {
      case CEPH_RGW_REMOVE:
        CLS_LOG(10, "CEPH_RGW_REMOVE name=%s\n", cur_change.name.c_str());
	ret = cls_cxx_map_remove_key(hctx, cur_change.name);
	if (ret < 0)
	  return ret;
        break;
      case CEPH_RGW_UPDATE:
        CLS_LOG(10, "CEPH_RGW_UPDATE name=%s total_entries: %"PRId64" -> %"PRId64"\n", cur_change.name.c_str(), stats.num_entries, stats.num_entries + 1);
        stats.num_entries++;
        stats.total_size += cur_change.meta.size;
        stats.total_size_rounded += get_rounded_size(cur_change.meta.size);
        header_changed = true;
        bufferlist cur_state_bl;
        ::encode(cur_change, cur_state_bl);
        ret = cls_cxx_map_set_val(hctx, cur_change.name, &cur_state_bl);
        if (ret < 0)
	  return ret;
        break;
      }
    }
  }

  bufferlist update_bl;
  if (header_changed) {
    bufferlist new_header_bl;
    ::encode(header, new_header_bl);
    return cls_cxx_map_write_header(hctx, &new_header_bl);
  }
  return 0;
}

static void usage_record_prefix_by_time(uint64_t epoch, string& key)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%011llu", (long long unsigned)epoch);
  key = buf;
}

static void usage_record_prefix_by_user(string& user, uint64_t epoch, string& key)
{
  char buf[user.size() + 32];
  snprintf(buf, sizeof(buf), "%s_%011llu_", user.c_str(), (long long unsigned)epoch);
  key = buf;
}

static void usage_record_name_by_time(uint64_t epoch, string& user, string& bucket, string& key)
{
  char buf[32 + user.size() + bucket.size()];
  snprintf(buf, sizeof(buf), "%011llu_%s_%s", (long long unsigned)epoch, user.c_str(), bucket.c_str());
  key = buf;
}

static void usage_record_name_by_user(string& user, uint64_t epoch, string& bucket, string& key)
{
  char buf[32 + user.size() + bucket.size()];
  snprintf(buf, sizeof(buf), "%s_%011llu_%s", user.c_str(), (long long unsigned)epoch, bucket.c_str());
  key = buf;
}

static int usage_record_decode(bufferlist& record_bl, rgw_usage_log_entry& e)
{
  bufferlist::iterator kiter = record_bl.begin();
  try {
    ::decode(e, kiter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: usage_record_decode(): failed to decode record_bl\n");
    return -EINVAL;
  }

  return 0;
}

int rgw_user_usage_log_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "rgw_user_usage_log_add()");

  bufferlist::iterator in_iter = in->begin();
  rgw_cls_usage_log_add_op op;

  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_user_usage_log_add(): failed to decode request\n");
    return -EINVAL;
  }

  rgw_usage_log_info& info = op.info;
  vector<rgw_usage_log_entry>::iterator iter;

  for (iter = info.entries.begin(); iter != info.entries.end(); ++iter) {
    rgw_usage_log_entry& entry = *iter;
    string key_by_time;
    usage_record_name_by_time(entry.epoch, entry.owner, entry.bucket, key_by_time);

    CLS_LOG(10, "rgw_user_usage_log_add user=%s bucket=%s\n", entry.owner.c_str(), entry.bucket.c_str());

    bufferlist record_bl;
    int ret = cls_cxx_map_get_val(hctx, key_by_time, &record_bl);
    if (ret < 0 && ret != -ENOENT) {
      CLS_LOG(1, "ERROR: rgw_user_usage_log_add(): cls_cxx_map_read_key returned %d\n", ret);
      return -EINVAL;
    }
    if (ret >= 0) {
      rgw_usage_log_entry e;
      ret = usage_record_decode(record_bl, e);
      if (ret < 0)
        return ret;
      CLS_LOG(10, "rgw_user_usage_log_add aggregating existing bucket\n");
      entry.aggregate(e);
    }

    bufferlist new_record_bl;
    ::encode(entry, new_record_bl);
    ret = cls_cxx_map_set_val(hctx, key_by_time, &new_record_bl);
    if (ret < 0)
      return ret;

    string key_by_user;
    usage_record_name_by_user(entry.owner, entry.epoch, entry.bucket, key_by_user);
    ret = cls_cxx_map_set_val(hctx, key_by_user, &new_record_bl);
    if (ret < 0)
      return ret;
  }

  return 0;
}

static int usage_iterate_range(cls_method_context_t hctx, uint64_t start, uint64_t end,
                            string& user, string& key_iter, uint32_t max_entries, bool *truncated,
                            int (*cb)(cls_method_context_t, const string&, rgw_usage_log_entry&, void *),
                            void *param)
{
  CLS_LOG(10, "usage_iterate_range");

  map<string, bufferlist> keys;
#define NUM_KEYS 32
  string filter_prefix;
  string start_key, end_key;
  bool by_user = !user.empty();
  uint32_t i = 0;
  string user_key;

  if (truncated)
    *truncated = false;

  if (!by_user) {
    usage_record_prefix_by_time(end, end_key);
  } else {
    user_key = user;
    user_key.append("_");
  }

  if (key_iter.empty()) {
    if (by_user) {
      usage_record_prefix_by_user(user, start, start_key);
    } else {
      usage_record_prefix_by_time(start, start_key);
    }
  } else {
    start_key = key_iter;
  }

  do {
    CLS_LOG(20, "usage_iterate_range start_key=%s", start_key.c_str());
    int ret = cls_cxx_map_get_vals(hctx, start_key, filter_prefix, NUM_KEYS, &keys);
    if (ret < 0)
      return ret;


    map<string, bufferlist>::iterator iter = keys.begin();
    if (iter == keys.end())
      break;

    for (; iter != keys.end(); ++iter) {
      const string& key = iter->first;
      rgw_usage_log_entry e;

      if (!by_user && key.compare(end_key) >= 0) {
        CLS_LOG(20, "usage_iterate_range reached key=%s, done", key.c_str());
        return 0;
      }

      if (by_user && key.compare(0, user_key.size(), user_key) != 0) {
        CLS_LOG(20, "usage_iterate_range reached key=%s, done", key.c_str());
        return 0;
      }

      ret = usage_record_decode(iter->second, e);
      if (ret < 0)
        return ret;

      if (e.epoch < start)
	continue;

      /* keys are sorted by epoch, so once we're past end we're done */
      if (e.epoch >= end)
        return 0;

      ret = cb(hctx, key, e, param);
      if (ret < 0)
        return ret;


      i++;
      if (max_entries && (i > max_entries)) {
        CLS_LOG(20, "usage_iterate_range reached max_entries (%d), done", max_entries);
        *truncated = true;
        key_iter = key;
        return 0;
      }
    }
    --iter;
    start_key = iter->first;
  } while (true);
  return 0;
}

static int usage_log_read_cb(cls_method_context_t hctx, const string& key, rgw_usage_log_entry& entry, void *param)
{
  map<rgw_user_bucket, rgw_usage_log_entry> *usage = (map<rgw_user_bucket, rgw_usage_log_entry> *)param;
  rgw_user_bucket ub(entry.owner, entry.bucket);
  rgw_usage_log_entry& le = (*usage)[ub];
  le.aggregate(entry);
 
  return 0;
}

int rgw_user_usage_log_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "rgw_user_usage_log_read()");

  bufferlist::iterator in_iter = in->begin();
  rgw_cls_usage_log_read_op op;

  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_user_usage_log_read(): failed to decode request\n");
    return -EINVAL;
  }

  rgw_cls_usage_log_read_ret ret_info;
  map<rgw_user_bucket, rgw_usage_log_entry> *usage = &ret_info.usage;
  string iter = op.iter;
#define MAX_ENTRIES 1000
  uint32_t max_entries = (op.max_entries ? op.max_entries : MAX_ENTRIES);
  int ret = usage_iterate_range(hctx, op.start_epoch, op.end_epoch, op.owner, iter, max_entries, &ret_info.truncated, usage_log_read_cb, (void *)usage);
  if (ret < 0)
    return ret;

  if (ret_info.truncated)
    ret_info.next_iter = iter;

  ::encode(ret_info, *out);
  return 0;
}

static int usage_log_trim_cb(cls_method_context_t hctx, const string& key, rgw_usage_log_entry& entry, void *param)
{
  string key_by_time;
  string key_by_user;

  usage_record_name_by_time(entry.epoch, entry.owner, entry.bucket, key_by_time);
  usage_record_name_by_user(entry.owner, entry.epoch, entry.bucket, key_by_user);

  int ret = cls_cxx_map_remove_key(hctx, key_by_time);
  if (ret < 0)
    return ret;

  return cls_cxx_map_remove_key(hctx, key_by_user);
}

int rgw_user_usage_log_trim(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "rgw_user_usage_log_trim()");

  /* only continue if object exists! */
  int ret = cls_cxx_stat(hctx, NULL, NULL);
  if (ret < 0)
    return ret;

  bufferlist::iterator in_iter = in->begin();
  rgw_cls_usage_log_trim_op op;

  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_user_log_usage_log_trim(): failed to decode request\n");
    return -EINVAL;
  }

  string iter;
  ret = usage_iterate_range(hctx, op.start_epoch, op.end_epoch, op.user, iter, 0, NULL, usage_log_trim_cb, NULL);
  if (ret < 0)
    return ret;

  return 0;
}

/*
 * We hold the garbage collection chain data under two different indexes: the first 'name' index
 * keeps them under a unique tag that represents the chains, and a second 'time' index keeps
 * them by their expiration timestamp
 */
#define GC_OBJ_NAME_INDEX 0
#define GC_OBJ_TIME_INDEX 1

static string gc_index_prefixes[] = { "0_",
                                      "1_" };

static void prepend_index_prefix(const string& src, int index, string *dest)
{
  *dest = gc_index_prefixes[index];
  dest->append(src);
}

static int gc_omap_get(cls_method_context_t hctx, int type, const string& key, cls_rgw_gc_obj_info *info)
{
  string index;
  prepend_index_prefix(key, type, &index);

  bufferlist bl;
  int ret = cls_cxx_map_get_val(hctx, index, &bl);
  if (ret < 0)
    return ret;

  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(*info, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: rgw_cls_gc_omap_get(): failed to decode index=%s\n", index.c_str());
  }

  return 0;
}

static int gc_omap_set(cls_method_context_t hctx, int type, const string& key, const cls_rgw_gc_obj_info *info)
{
  bufferlist bl;
  ::encode(*info, bl);

  string index = gc_index_prefixes[type];
  index.append(key);

  int ret = cls_cxx_map_set_val(hctx, index, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int gc_omap_remove(cls_method_context_t hctx, int type, const string& key)
{
  string index = gc_index_prefixes[type];
  index.append(key);

  bufferlist bl;
  int ret = cls_cxx_map_remove_key(hctx, index);
  if (ret < 0)
    return ret;

  return 0;
}

static void get_time_key(utime_t& ut, string *key)
{
  char buf[32];
  snprintf(buf, 32, "%011llu.%09u", (unsigned long long)ut.sec(), ut.nsec());
  *key = buf;
}

static bool key_in_index(const string& key, int index_type)
{
  const string& prefix = gc_index_prefixes[index_type]; 
  return (key.compare(0, prefix.size(), prefix) == 0);
}


static int gc_update_entry(cls_method_context_t hctx, uint32_t expiration_secs,
                           cls_rgw_gc_obj_info& info)
{
  cls_rgw_gc_obj_info old_info;
  int ret = gc_omap_get(hctx, GC_OBJ_NAME_INDEX, info.tag, &old_info);
  if (ret == 0) {
    string key;
    get_time_key(old_info.time, &key);
    ret = gc_omap_remove(hctx, GC_OBJ_TIME_INDEX, key);
    if (ret < 0 && ret != -ENOENT) {
      CLS_LOG(0, "ERROR: failed to remove key=%s\n", key.c_str());
      return ret;
    }
  }
  info.time = ceph_clock_now(g_ceph_context);
  info.time += expiration_secs;
  ret = gc_omap_set(hctx, GC_OBJ_NAME_INDEX, info.tag, &info);
  if (ret < 0)
    return ret;

  string key;
  get_time_key(info.time, &key);
  ret = gc_omap_set(hctx, GC_OBJ_TIME_INDEX, key, &info);
  if (ret < 0)
    goto done_err;

  return 0;

done_err:
  CLS_LOG(0, "ERROR: gc_set_entry error info.tag=%s, ret=%d\n", info.tag.c_str(), ret);
  gc_omap_remove(hctx, GC_OBJ_NAME_INDEX, info.tag);
  return ret;
}

static int gc_defer_entry(cls_method_context_t hctx, const string& tag, uint32_t expiration_secs)
{
  cls_rgw_gc_obj_info info;
  int ret = gc_omap_get(hctx, GC_OBJ_NAME_INDEX, tag, &info);
  if (ret == -ENOENT)
    return 0;
  if (ret < 0)
    return ret;
  return gc_update_entry(hctx, expiration_secs, info);
}

int gc_record_decode(bufferlist& bl, cls_rgw_gc_obj_info& e)
{
  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(e, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode cls_rgw_gc_obj_info");
    return -EIO;
  }
  return 0;
}

static int rgw_cls_gc_set_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_rgw_gc_set_entry_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_gc_set_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  return gc_update_entry(hctx, op.expiration_secs, op.info);
}

static int rgw_cls_gc_defer_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_rgw_gc_defer_entry_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_gc_defer_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  return gc_defer_entry(hctx, op.tag, op.expiration_secs);
}

static int gc_iterate_entries(cls_method_context_t hctx, const string& marker,
                              string& key_iter, uint32_t max_entries, bool *truncated,
                              int (*cb)(cls_method_context_t, const string&, cls_rgw_gc_obj_info&, void *),
                              void *param)
{
  CLS_LOG(10, "gc_iterate_range");

  map<string, bufferlist> keys;
  string filter_prefix, end_key;
  uint32_t i = 0;
  string key;

  if (truncated)
    *truncated = false;

  string start_key;
  if (key_iter.empty()) {
    prepend_index_prefix(marker, GC_OBJ_TIME_INDEX, &start_key);
  } else {
    start_key = key_iter;
  }

  utime_t now = ceph_clock_now(g_ceph_context);
  string now_str;
  get_time_key(now, &now_str);
  prepend_index_prefix(now_str, GC_OBJ_TIME_INDEX, &end_key);

  CLS_LOG(0, "gc_iterate_entries end_key=%s\n", end_key.c_str());

  string filter;

  do {
#define GC_NUM_KEYS 32
    int ret = cls_cxx_map_get_vals(hctx, start_key, filter, GC_NUM_KEYS, &keys);
    if (ret < 0)
      return ret;


    map<string, bufferlist>::iterator iter = keys.begin();
    if (iter == keys.end())
      break;

    for (; iter != keys.end(); ++iter) {
      const string& key = iter->first;
      cls_rgw_gc_obj_info e;

      CLS_LOG(10, "gc_iterate_entries key=%s\n", key.c_str());

      if (key.compare(end_key) >= 0)
        return 0;

      if (!key_in_index(key, GC_OBJ_TIME_INDEX))
	return 0;

      ret = gc_record_decode(iter->second, e);
      if (ret < 0)
        return ret;

      if (max_entries && (i >= max_entries)) {
        if (truncated)
          *truncated = true;
        key_iter = key;
        return 0;
      }

      ret = cb(hctx, key, e, param);
      if (ret < 0)
        return ret;
      i++;

    }
    --iter;
    start_key = iter->first;
  } while (true);
  return 0;
}

static int gc_list_cb(cls_method_context_t hctx, const string& key, cls_rgw_gc_obj_info& info, void *param)
{
  list<cls_rgw_gc_obj_info> *l = (list<cls_rgw_gc_obj_info> *)param;
  l->push_back(info);
  return 0;
}

static int gc_list_entries(cls_method_context_t hctx, const string& marker,
			   uint32_t max, list<cls_rgw_gc_obj_info>& entries, bool *truncated)
{
  string key_iter;
  int ret = gc_iterate_entries(hctx, marker,
                              key_iter, max, truncated,
                              gc_list_cb, &entries);
  return ret;
}

static int rgw_cls_gc_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_rgw_gc_list_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_gc_list(): failed to decode entry\n");
    return -EINVAL;
  }

  cls_rgw_gc_list_ret op_ret;
  int ret = gc_list_entries(hctx, op.marker, op.max, op_ret.entries, &op_ret.truncated);
  if (ret < 0)
    return ret;

  ::encode(op_ret, *out);

  return 0;
}

static int gc_remove(cls_method_context_t hctx, list<string>& tags)
{
  list<string>::iterator iter;

  for (iter = tags.begin(); iter != tags.end(); ++iter) {
    string& tag = *iter;
    cls_rgw_gc_obj_info info;
    int ret = gc_omap_get(hctx, GC_OBJ_NAME_INDEX, tag, &info);
    if (ret == -ENOENT) {
      CLS_LOG(0, "couldn't find tag in name index tag=%s\n", tag.c_str());
      continue;
    }

    if (ret < 0)
      return ret;

    string time_key;
    get_time_key(info.time, &time_key);
    ret = gc_omap_remove(hctx, GC_OBJ_TIME_INDEX, time_key);
    if (ret < 0 && ret != -ENOENT)
      return ret;
    if (ret == -ENOENT) {
      CLS_LOG(0, "couldn't find key in time index key=%s\n", time_key.c_str());
    }

    ret = gc_omap_remove(hctx, GC_OBJ_NAME_INDEX, tag);
    if (ret < 0 && ret != -ENOENT)
      return ret;
  }

  return 0;
}

static int rgw_cls_gc_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_rgw_gc_remove_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_gc_remove(): failed to decode entry\n");
    return -EINVAL;
  }

  return gc_remove(hctx, op.tags);
}

void __cls_init()
{
  CLS_LOG(1, "Loaded rgw class!");

  cls_register("rgw", &h_class);

  /* bucket index */
  cls_register_cxx_method(h_class, "bucket_init_index", CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_init_index, &h_rgw_bucket_init_index);
  cls_register_cxx_method(h_class, "bucket_set_tag_timeout", CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_set_tag_timeout, &h_rgw_bucket_set_tag_timeout);
  cls_register_cxx_method(h_class, "bucket_list", CLS_METHOD_RD, rgw_bucket_list, &h_rgw_bucket_list);
  cls_register_cxx_method(h_class, "bucket_check_index", CLS_METHOD_RD, rgw_bucket_check_index, &h_rgw_bucket_check_index);
  cls_register_cxx_method(h_class, "bucket_rebuild_index", CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_rebuild_index, &h_rgw_bucket_rebuild_index);
  cls_register_cxx_method(h_class, "bucket_prepare_op", CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_prepare_op, &h_rgw_bucket_prepare_op);
  cls_register_cxx_method(h_class, "bucket_complete_op", CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_complete_op, &h_rgw_bucket_complete_op);
  cls_register_cxx_method(h_class, "dir_suggest_changes", CLS_METHOD_RD | CLS_METHOD_WR, rgw_dir_suggest_changes, &h_rgw_dir_suggest_changes);

  /* usage logging */
  cls_register_cxx_method(h_class, "user_usage_log_add", CLS_METHOD_RD | CLS_METHOD_WR, rgw_user_usage_log_add, &h_rgw_user_usage_log_add);
  cls_register_cxx_method(h_class, "user_usage_log_read", CLS_METHOD_RD, rgw_user_usage_log_read, &h_rgw_user_usage_log_read);
  cls_register_cxx_method(h_class, "user_usage_log_trim", CLS_METHOD_RD | CLS_METHOD_WR, rgw_user_usage_log_trim, &h_rgw_user_usage_log_trim);

  /* garbage collection */
  cls_register_cxx_method(h_class, "gc_set_entry", CLS_METHOD_RD | CLS_METHOD_WR, rgw_cls_gc_set_entry, &h_rgw_gc_set_entry);
  cls_register_cxx_method(h_class, "gc_defer_entry", CLS_METHOD_RD | CLS_METHOD_WR, rgw_cls_gc_defer_entry, &h_rgw_gc_set_entry);
  cls_register_cxx_method(h_class, "gc_list", CLS_METHOD_RD, rgw_cls_gc_list, &h_rgw_gc_list);
  cls_register_cxx_method(h_class, "gc_remove", CLS_METHOD_RD | CLS_METHOD_WR, rgw_cls_gc_remove, &h_rgw_gc_remove);

  return;
}

