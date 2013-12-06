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
cls_method_handle_t h_user_remove_bucket;
cls_method_handle_t h_user_list_buckets;

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

  for (list<cls_user_bucket_entry>::iterator iter = op.entries.begin();
       iter != op.entries.end(); ++iter) {
    cls_user_bucket_entry& entry = *iter;

    string key;

    get_key_by_bucket_name(entry.bucket.name, &key);

    CLS_LOG(0, "storing entry by client/op at %s", key.c_str());

    int ret = write_entry(hctx, key, entry);
    if (ret < 0)
      return ret;
  }
  
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

  string key;

  get_key_by_bucket_name(op.bucket.name, &key);

  CLS_LOG(20, "removing entry at %s", key.c_str());

  int ret = remove_entry(hctx, key);
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

  string from_index = op.marker;

#define MAX_ENTRIES 1000
  size_t max_entries = op.max_entries;
  if (!max_entries || max_entries > MAX_ENTRIES)
    max_entries = MAX_ENTRIES;

  string match_prefix;

  int rc = cls_cxx_map_get_vals(hctx, from_index, match_prefix, max_entries + 1, &keys);
  if (rc < 0)
    return rc;

  CLS_LOG(20, "from_index=%s match_prefix=%s", from_index.c_str(), match_prefix.c_str());
  cls_user_list_buckets_ret ret;

  list<cls_user_bucket_entry>& entries = ret.entries;
  map<string, bufferlist>::iterator iter = keys.begin();

  bool done = false;
  string marker;

  size_t i;
  for (i = 0; i < max_entries && iter != keys.end(); ++i, ++iter) {
    const string& index = iter->first;
    marker = index;

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

void __cls_init()
{
  CLS_LOG(1, "Loaded user class!");

  cls_register("user", &h_class);

  /* log */
  cls_register_cxx_method(h_class, "set_buckets_info", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_user_set_buckets_info, &h_user_set_buckets_info);
  cls_register_cxx_method(h_class, "remove_bucket", CLS_METHOD_RD | CLS_METHOD_WR, cls_user_remove_bucket, &h_user_remove_bucket);
  cls_register_cxx_method(h_class, "list_buckets", CLS_METHOD_RD, cls_user_list_buckets, &h_user_list_buckets);

  return;
}

