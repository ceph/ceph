// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "include/types.h"
#include "include/utime.h"
#include "objclass/objclass.h"

#include "cls_user_types.h"
#include "cls_user_ops.h"

CLS_VER(1,0)
CLS_NAME(user)

cls_handle_t h_class;
cls_method_handle_t h_user_set_buckets_info;
cls_method_handle_t h_user_complete_stats_sync;
cls_method_handle_t h_user_remove_bucket;
cls_method_handle_t h_user_list_buckets;
cls_method_handle_t h_user_get_header;

static int write_entry(cls_method_context_t hctx, const string& key, const cls_user_bucket_entry& entry)
{
  bufferlist bl;
  ::encode(entry, bl);

  int ret = cls_cxx_map_set_val(hctx, key, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int remove_entry(cls_method_context_t hctx, const string& key)
{
  int ret = cls_cxx_map_remove_key(hctx, key);
  if (ret < 0)
    return ret;

  return 0;
}

static void get_key_by_bucket_name(const string& bucket_name, string *key)
{
  *key = bucket_name;
}

static int get_existing_bucket_entry(cls_method_context_t hctx, const string& bucket_name,
                                     cls_user_bucket_entry& entry)
{
  if (bucket_name.empty()) {
    return -EINVAL;
  }

  string key;
  get_key_by_bucket_name(bucket_name, &key);

  bufferlist bl;
  int rc = cls_cxx_map_get_val(hctx, key, &bl);
  if (rc < 0) {
    CLS_LOG(10, "could not read entry %s", key.c_str());
    return rc;
  }
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(entry, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode entry %s", key.c_str());
    return -EIO;
  }

  return 0;
}

static int read_header(cls_method_context_t hctx, cls_user_header *header)
{
  bufferlist bl;

  int ret = cls_cxx_map_read_header(hctx, &bl);
  if (ret < 0)
    return ret;

  if (bl.length() == 0) {
    *header = cls_user_header();
    return 0;
  }

  try {
    ::decode(*header, bl);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode user header");
    return -EIO;
  }

  return 0;
}

static void add_header_stats(cls_user_stats *stats, cls_user_bucket_entry& entry)
{
  stats->total_entries += entry.count;
  stats->total_bytes += entry.size;
  stats->total_bytes_rounded += entry.size_rounded;
}

static void dec_header_stats(cls_user_stats *stats, cls_user_bucket_entry& entry)
{
  stats->total_bytes -= entry.size;
  stats->total_bytes_rounded -= entry.size_rounded;
  stats->total_entries -= entry.count;
}

static void apply_entry_stats(const cls_user_bucket_entry& src_entry, cls_user_bucket_entry *target_entry)
{
  target_entry->size = src_entry.size;
  target_entry->size_rounded = src_entry.size_rounded;
  target_entry->count = src_entry.count;
}

static int cls_user_set_buckets_info(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_user_set_buckets_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_user_add_op(): failed to decode op");
    return -EINVAL;
  }

  cls_user_header header;
  int ret = read_header(hctx, &header);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to read user info header ret=%d", ret);
    return ret;
  }

  for (list<cls_user_bucket_entry>::iterator iter = op.entries.begin();
       iter != op.entries.end(); ++iter) {
    cls_user_bucket_entry& update_entry = *iter;

    string key;

    get_key_by_bucket_name(update_entry.bucket.name, &key);

    cls_user_bucket_entry entry;
    ret = get_existing_bucket_entry(hctx, key, entry);

    if (ret == -ENOENT) {
     if (!op.add)
      continue; /* racing bucket removal */

     entry = update_entry;

     ret = 0;
    }

    if (ret < 0) {
      CLS_LOG(0, "ERROR: get_existing_bucket_entry() key=%s returned %d", key.c_str(), ret);
      return ret;
    } else if (ret >= 0 && entry.user_stats_sync) {
      dec_header_stats(&header.stats, entry);
    }

    CLS_LOG(20, "storing entry for key=%s size=%lld count=%lld",
            key.c_str(), (long long)update_entry.size, (long long)update_entry.count);

    apply_entry_stats(update_entry, &entry);
    entry.user_stats_sync = true;

    ret = write_entry(hctx, key, entry);
    if (ret < 0)
      return ret;

    add_header_stats(&header.stats, entry);
  }

  bufferlist bl;

  CLS_LOG(20, "header: total bytes=%lld entries=%lld", (long long)header.stats.total_bytes, (long long)header.stats.total_entries);

  if (header.last_stats_update < op.time)
    header.last_stats_update = op.time;

  ::encode(header, bl);
  
  ret = cls_cxx_map_write_header(hctx, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_user_complete_stats_sync(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_user_complete_stats_sync_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_user_add_op(): failed to decode op");
    return -EINVAL;
  }

  cls_user_header header;
  int ret = read_header(hctx, &header);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to read user info header ret=%d", ret);
    return ret;
  }

  if (header.last_stats_sync < op.time)
    header.last_stats_sync = op.time;

  bufferlist bl;

  ::encode(header, bl);

  ret = cls_cxx_map_write_header(hctx, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_user_remove_bucket(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_user_remove_bucket_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_user_add_op(): failed to decode op");
    return -EINVAL;
  }

  cls_user_header header;
  int ret = read_header(hctx, &header);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to read user info header ret=%d", ret);
    return ret;
  }

  string key;

  get_key_by_bucket_name(op.bucket.name, &key);

  cls_user_bucket_entry entry;
  ret = get_existing_bucket_entry(hctx, key, entry);
  if (ret == -ENOENT) {
    return 0; /* idempotent removal */
  }
  if (ret < 0) {
    CLS_LOG(0, "ERROR: get existing bucket entry, key=%s ret=%d", key.c_str(), ret);
    return ret;
  }

  if (entry.user_stats_sync) {
    dec_header_stats(&header.stats, entry);
  }

  CLS_LOG(20, "removing entry at %s", key.c_str());

  ret = remove_entry(hctx, key);
  if (ret < 0)
    return ret;
  
  return 0;
}

static int cls_user_list_buckets(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_user_list_buckets_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_user_list_op(): failed to decode op");
    return -EINVAL;
  }

  map<string, bufferlist> keys;

  const string& from_index = op.marker;
  const string& to_index = op.end_marker;
  const bool to_index_valid = !to_index.empty();

#define MAX_ENTRIES 1000
  size_t max_entries = op.max_entries;
  if (max_entries > MAX_ENTRIES)
    max_entries = MAX_ENTRIES;

  string match_prefix;

  int rc = cls_cxx_map_get_vals(hctx, from_index, match_prefix, max_entries + 1, &keys);
  if (rc < 0)
    return rc;

  CLS_LOG(20, "from_index=%s to_index=%s match_prefix=%s",
          from_index.c_str(),
          to_index.c_str(),
          match_prefix.c_str());
  cls_user_list_buckets_ret ret;

  list<cls_user_bucket_entry>& entries = ret.entries;
  map<string, bufferlist>::iterator iter = keys.begin();

  bool done = false;
  string marker;

  size_t i;
  for (i = 0; i < max_entries && iter != keys.end(); ++i, ++iter) {
    const string& index = iter->first;
    marker = index;

    if (to_index_valid && to_index.compare(index) <= 0)
      break;

    bufferlist& bl = iter->second;
    bufferlist::iterator biter = bl.begin();
    try {
      cls_user_bucket_entry e;
      ::decode(e, biter);
      entries.push_back(e);
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR: cls_user_list: could not decode entry, index=%s", index.c_str());
    }
  }

  if (iter == keys.end())
    done = true;
  else
    ret.marker = marker;

  ret.truncated = !done;

  ::encode(ret, *out);

  return 0;
}

static int cls_user_get_header(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  bufferlist::iterator in_iter = in->begin();

  cls_user_get_header_op op;
  try {
    ::decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_user_get_header_op(): failed to decode op");
    return -EINVAL;
  }

  cls_user_get_header_ret op_ret;

  int ret = read_header(hctx, &op_ret.header);
  if (ret < 0)
    return ret;

  ::encode(op_ret, *out);

  return 0;
}

void __cls_init()
{
  CLS_LOG(1, "Loaded user class!");

  cls_register("user", &h_class);

  /* log */
  cls_register_cxx_method(h_class, "set_buckets_info", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_user_set_buckets_info, &h_user_set_buckets_info);
  cls_register_cxx_method(h_class, "complete_stats_sync", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_user_complete_stats_sync, &h_user_set_buckets_info);
  cls_register_cxx_method(h_class, "remove_bucket", CLS_METHOD_RD | CLS_METHOD_WR, cls_user_remove_bucket, &h_user_remove_bucket);
  cls_register_cxx_method(h_class, "list_buckets", CLS_METHOD_RD, cls_user_list_buckets, &h_user_list_buckets);
  cls_register_cxx_method(h_class, "get_header", CLS_METHOD_RD, cls_user_get_header, &h_user_get_header);

  return;
}

