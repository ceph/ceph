// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <algorithm>
#include <cctype>

#include "include/utime.h"
#include "objclass/objclass.h"

#include "cls_user_ops.h"
#include "rgw/rgw_string.h"

using std::map;
using std::string;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

CLS_VER(1,0)
CLS_NAME(user)

static int write_entry(cls_method_context_t hctx, const string& key, const cls_user_bucket_entry& entry)
{
  bufferlist bl;
  encode(entry, bl);

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
    auto iter = bl.cbegin();
    decode(entry, iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode entry %s", key.c_str());
    return -EIO;
  }

  return 0;
}

template <typename T>
static int read_header(cls_method_context_t hctx, T *header)
{
  bufferlist bl;

  int ret = cls_cxx_map_read_header(hctx, &bl);
  if (ret < 0)
    return ret;

  if (bl.length() == 0) {
    *header = T();
    return 0;
  }

  try {
    decode(*header, bl);
  } catch (ceph::buffer::error& err) {
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
  auto in_iter = in->cbegin();

  cls_user_set_buckets_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_user_add_op(): failed to decode op");
    return -EINVAL;
  }

  cls_user_header header;
  int ret = read_header(hctx, &header);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to read user info header ret=%d", ret);
    return ret;
  }

  for (auto iter = op.entries.begin(); iter != op.entries.end(); ++iter) {
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
    } else if (op.add) {
      // bucket id may have changed (ie reshard)
      entry.bucket.bucket_id = update_entry.bucket.bucket_id;
      // creation date may have changed (ie delete/recreate bucket)
      entry.creation_time = update_entry.creation_time;
    }

    if (ret < 0) {
      CLS_LOG(0, "ERROR: get_existing_bucket_entry() key=%s returned %d", key.c_str(), ret);
      return ret;
    } else if (ret >= 0 && entry.user_stats_sync) {
      dec_header_stats(&header.stats, entry);
    }

    CLS_LOG(20, "storing entry for key=%s size=%lld count=%lld",
            key.c_str(), (long long)update_entry.size, (long long)update_entry.count);

    // sync entry stats when not an op.add, as when the case is op.add if its a
    // new entry we already have copied update_entry earlier, OTOH, for an existing entry
    // we end up clobbering the existing stats for the bucket
    if (!op.add){
      apply_entry_stats(update_entry, &entry);
    }
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

  encode(header, bl);
  
  ret = cls_cxx_map_write_header(hctx, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_user_complete_stats_sync(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_user_complete_stats_sync_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
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

  encode(header, bl);

  ret = cls_cxx_map_write_header(hctx, &bl);
  if (ret < 0)
    return ret;

  return 0;
}

static int cls_user_remove_bucket(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_user_remove_bucket_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
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

  CLS_LOG(20, "removing entry at %s", key.c_str());

  ret = remove_entry(hctx, key);
  if (ret < 0)
    return ret;

  if (!entry.user_stats_sync) {
    return 0;
  }

  dec_header_stats(&header.stats, entry);

  CLS_LOG(20, "header: total bytes=%lld entries=%lld", (long long)header.stats.total_bytes, (long long)header.stats.total_entries);

  bufferlist bl;
  encode(header, bl);
  return cls_cxx_map_write_header(hctx, &bl);
}

static int cls_user_list_buckets(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_user_list_buckets_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
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
  cls_user_list_buckets_ret ret;

  int rc = cls_cxx_map_get_vals(hctx, from_index, match_prefix, max_entries, &keys, &ret.truncated);
  if (rc < 0)
    return rc;

  CLS_LOG(20, "from_index=%s to_index=%s match_prefix=%s",
          from_index.c_str(),
          to_index.c_str(),
          match_prefix.c_str());

  auto& entries = ret.entries;
  auto iter = keys.begin();

  string marker;

  for (; iter != keys.end(); ++iter) {
    const string& index = iter->first;
    marker = index;

    if (to_index_valid && to_index.compare(index) <= 0) {
      ret.truncated = false;
      break;
    }

    bufferlist& bl = iter->second;
    auto biter = bl.cbegin();
    try {
      cls_user_bucket_entry e;
      decode(e, biter);
      entries.push_back(e);
    } catch (ceph::buffer::error& err) {
      CLS_LOG(0, "ERROR: cls_user_list: could not decode entry, index=%s", index.c_str());
    }
  }

  if (ret.truncated) {
    ret.marker = marker;
  }

  encode(ret, *out);

  return 0;
}

static int cls_user_get_header(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_user_get_header_op op;
  try {
    decode(op, in_iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_user_get_header_op(): failed to decode op");
    return -EINVAL;
  }

  cls_user_get_header_ret op_ret;

  int ret = read_header(hctx, &op_ret.header);
  if (ret < 0)
    return ret;

  encode(op_ret, *out);

  return 0;
}

/// A method to reset the user.buckets header stats in accordance to
/// the values seen in the user.buckets omap keys. This is not be
/// equivalent to --sync-stats which also re-calculates the stats for
/// each bucket.
static int cls_user_reset_stats(cls_method_context_t hctx,
				bufferlist *in, bufferlist *out /*ignore*/)
{
  cls_user_reset_stats_op op;

  try {
    auto bliter = in->cbegin();
    decode(op, bliter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: %s failed to decode op", __func__);
    return -EINVAL;
  }

  cls_user_header header;
  bool truncated = false;
  string from_index, prefix;
  do {
    map<string, bufferlist> keys;
    int rc = cls_cxx_map_get_vals(hctx, from_index, prefix, MAX_ENTRIES,
				  &keys, &truncated);
    if (rc < 0) {
      CLS_LOG(0, "ERROR: %s failed to retrieve omap key-values", __func__);
      return rc;
    }
    CLS_LOG(20, "%s: read %lu key-values, truncated=%d",
	    __func__, keys.size(), truncated);

    for (const auto& kv : keys) {
      cls_user_bucket_entry e;
      try {
	auto bl = kv.second;
	auto bliter = bl.cbegin();
	decode(e, bliter);
      } catch (ceph::buffer::error& err) {
	CLS_LOG(0, "ERROR: %s failed to decode bucket entry for %s",
		__func__, kv.first.c_str());
	return -EIO;
      }
      add_header_stats(&header.stats, e);
    }
    if (!keys.empty()) {
      from_index = keys.rbegin()->first;
    }
  } while (truncated);

  bufferlist bl;
  header.last_stats_update = op.time;
  encode(header, bl);

  CLS_LOG(20, "%s: updating header", __func__);
  return cls_cxx_map_write_header(hctx, &bl);
} /* legacy cls_user_reset_stats */

/// A method to reset the user.buckets header stats in accordance to
/// the values seen in the user.buckets omap keys. This is not be
/// equivalent to --sync-stats which also re-calculates the stats for
/// each bucket.
static int cls_user_reset_stats2(cls_method_context_t hctx,
				 buffer::list *in, buffer::list *out)
{
  cls_user_reset_stats2_op op;

  try {
    auto bliter = in->cbegin();
    decode(op, bliter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: %s failed to decode op", __func__);
    return -EINVAL;
  }

  cls_user_header header;
  string from_index{op.marker}, prefix;
  cls_user_reset_stats2_ret ret;

  map<string, buffer::list> keys;
  int rc = cls_cxx_map_get_vals(hctx, from_index, prefix, MAX_ENTRIES,
				&keys, &ret.truncated);
  if (rc < 0) {
    CLS_LOG(0, "ERROR: %s failed to retrieve omap key-values", __func__);
    return rc;
  }
  CLS_LOG(20, "%s: read %lu key-values, truncated=%d",
	  __func__, keys.size(), ret.truncated);

  for (const auto& kv : keys) {
    cls_user_bucket_entry e;
    try {
      auto& bl = kv.second;
      auto bliter = bl.cbegin();
      decode(e, bliter);
    } catch (ceph::buffer::error& err) {
      CLS_LOG(0, "ERROR: %s failed to decode bucket entry for %s",
	      __func__, kv.first.c_str());
      return -EIO;
    }
    add_header_stats(&ret.acc_stats, e);
  }

  if (! ret.truncated) {
    buffer::list bl;
    header.last_stats_update = op.time;
    header.stats = ret.acc_stats;
    encode(header, bl);

    CLS_LOG(20, "%s: updating header", __func__);
    rc = cls_cxx_map_write_header(hctx, &bl);

    /* return final result */
    encode(ret, *out);
    return rc;
  }

  /* try-update marker */
  if(!keys.empty())
    ret.marker = (--keys.cend())->first;

  /* return partial result */
  encode(ret, *out);
  return 0;
} /* cls_user_reset_stats2 */


// account resource names must be unique and aren't distinguished by case, so
// convert all keys to lowercase
static std::string resource_key(std::string_view name)
{
  std::string key;
  key.resize(name.size());
  std::transform(name.begin(), name.end(), key.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return key;
}

static int cls_account_resource_add(cls_method_context_t hctx,
                                    buffer::list *in, buffer::list *out)
{
  cls_user_account_resource_add_op op;
  try {
    auto bliter = in->cbegin();
    decode(op, bliter);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: %s failed to decode op", __func__);
    return -EINVAL;
  }

  CLS_LOG(20, "adding account resource name=%s path=%s",
          op.entry.name.c_str(), op.entry.path.c_str());

  const std::string key = resource_key(op.entry.name);

  // does this resource entry exist?
  bufferlist readbl; // unused
  int ret = cls_cxx_map_get_val(hctx, key, &readbl);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  const bool exists = (ret == 0);

  std::optional<cls_user_account_header> header;
  if (!exists) {
    // if this is a new entry, update the resource count in the account header
    ret = read_header(hctx, &header.emplace());
    if (ret < 0) {
      CLS_LOG(0, "ERROR: failed to read account header ret=%d", ret);
      return ret;
    }
    if (header->count >= op.limit) {
      CLS_LOG(4, "account resource limit exceeded, %u >= %u",
              header->count, op.limit);
      return -EUSERS; // too many users
    }
    header->count++;
  } else if (op.exclusive) {
    return -EEXIST;
  }

  // write/overwrite the entry
  bufferlist writebl;
  encode(op.entry, writebl);
  ret = cls_cxx_map_set_val(hctx, key, &writebl);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to write account resource: %d", ret);
    return ret;
  }

  // write the updated account header
  if (header) {
    bufferlist headerbl;
    encode(*header, headerbl);
    return cls_cxx_map_write_header(hctx, &headerbl);
  }
  return 0;
} // cls_account_resource_add

static int cls_account_resource_get(cls_method_context_t hctx,
                                    bufferlist *in, bufferlist *out)
{
  cls_user_account_resource_get_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: %s failed to decode op", __func__);
    return -EINVAL;
  }

  CLS_LOG(20, "reading account resource name=%s", op.name.c_str());

  const std::string key = resource_key(op.name);

  bufferlist bl;
  int r = cls_cxx_map_get_val(hctx, key, &bl);
  if (r < 0) {
    return r;
  }

  cls_user_account_resource_get_ret ret;
  try {
    auto iter = bl.cbegin();
    decode(ret.entry, iter);
  } catch (ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode entry %s", key.c_str());
    return -EIO;
  }

  encode(ret, *out);
  return 0;
} // cls_account_resource_get

static int cls_account_resource_rm(cls_method_context_t hctx,
                                   buffer::list *in, buffer::list *out)
{
  cls_user_account_resource_rm_op op;
  try {
    auto bliter = in->cbegin();
    decode(op, bliter);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: %s failed to decode op", __func__);
    return -EINVAL;
  }

  CLS_LOG(20, "removing account resource name=%s", op.name.c_str());

  const std::string key = resource_key(op.name);

  // verify that the resource entry exists, so we can return ENOENT otherwise.
  // remove_key() alone would return success either way
  bufferlist readbl; // unused
  int ret = cls_cxx_map_get_val(hctx, key, &readbl);
  if (ret < 0) {
    return ret;
  }

  // remove the resource entry
  ret = cls_cxx_map_remove_key(hctx, key);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to remove account resource: %d", ret);
    return ret;
  }

  // update resource count in the account header
  cls_user_account_header header;
  ret = read_header(hctx, &header);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to read account header ret=%d", ret);
    return ret;
  }
  if (header.count) { // guard underflow
    header.count--;
  }

  bufferlist headerbl;
  encode(header, headerbl);
  return cls_cxx_map_write_header(hctx, &headerbl);
} // cls_account_resource_rm

static int cls_account_resource_list(cls_method_context_t hctx,
                                     bufferlist *in, bufferlist *out)
{
  cls_user_account_resource_list_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const ceph::buffer::error& err) {
    CLS_LOG(0, "ERROR: %s failed to decode op", __func__);
    return -EINVAL;
  }
  CLS_LOG(20, "listing account resources from marker=%s path_prefix=%s max_entries=%d",
          op.marker.c_str(), op.path_prefix.c_str(), (int)op.max_entries);

  const std::string prefix; // empty
  const uint32_t max_entries = std::min(op.max_entries, 1000u);
  std::map<std::string, bufferlist> entries;
  bool truncated = false;

  int rc = cls_cxx_map_get_vals(hctx, op.marker, prefix, max_entries,
                                &entries, &truncated);
  if (rc < 0) {
    return rc;
  }

  cls_user_account_resource_list_ret ret;

  // copy matching decoded omap values into a vector
  for (auto& [key, bl] : entries) {
    // decode as cls_user_account_resource
    cls_user_account_resource entry;
    try {
      auto p = bl.cbegin();
      decode(entry, p);
    } catch (const ceph::buffer::error& e) {
      CLS_LOG(1, "ERROR: %s failed to decode resource entry at key=%s",
              __func__, key.c_str());
      return -EIO;
    }

    // filter entries by path prefix
    if (entry.path.starts_with(op.path_prefix)) {
      CLS_LOG(20, "included resource path=%s name=%s",
              entry.path.c_str(), entry.name.c_str());
      ret.entries.push_back(std::move(entry));
    }
  }

  ret.truncated = truncated;
  if (!entries.empty()) {
    ret.marker = entries.rbegin()->first;
  }
  CLS_LOG(20, "entries=%d next_marker=%s truncated=%d",
          (int)ret.entries.size(), ret.marker.c_str(), (int)ret.truncated);

  encode(ret, *out);
  return 0;
} // cls_account_resource_list


CLS_INIT(user)
{
  CLS_LOG(1, "Loaded user class!");

  cls_handle_t h_class;
  cls_method_handle_t h_user_set_buckets_info;
  cls_method_handle_t h_user_complete_stats_sync;
  cls_method_handle_t h_user_remove_bucket;
  cls_method_handle_t h_user_list_buckets;
  cls_method_handle_t h_user_get_header;
  cls_method_handle_t h_user_reset_stats;
  cls_method_handle_t h_user_reset_stats2;

  cls_register("user", &h_class);

  /* log */
  cls_register_cxx_method(h_class, "set_buckets_info", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_user_set_buckets_info, &h_user_set_buckets_info);
  cls_register_cxx_method(h_class, "complete_stats_sync", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_user_complete_stats_sync, &h_user_complete_stats_sync);
  cls_register_cxx_method(h_class, "remove_bucket", CLS_METHOD_RD | CLS_METHOD_WR, cls_user_remove_bucket, &h_user_remove_bucket);
  cls_register_cxx_method(h_class, "list_buckets", CLS_METHOD_RD, cls_user_list_buckets, &h_user_list_buckets);
  cls_register_cxx_method(h_class, "get_header", CLS_METHOD_RD, cls_user_get_header, &h_user_get_header);
  cls_register_cxx_method(h_class, "reset_user_stats", CLS_METHOD_RD | CLS_METHOD_WR, cls_user_reset_stats, &h_user_reset_stats);
  cls_register_cxx_method(h_class, "reset_user_stats2", CLS_METHOD_RD | CLS_METHOD_WR, cls_user_reset_stats2, &h_user_reset_stats2);

  // account
  cls_method_handle_t h_account_resource_add;
  cls_method_handle_t h_account_resource_get;
  cls_method_handle_t h_account_resource_rm;
  cls_method_handle_t h_account_resource_list;

  cls_register_cxx_method(h_class, "account_resource_add", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_account_resource_add, &h_account_resource_add);
  cls_register_cxx_method(h_class, "account_resource_get", CLS_METHOD_RD,
                          cls_account_resource_get, &h_account_resource_get);
  cls_register_cxx_method(h_class, "account_resource_rm", CLS_METHOD_RD | CLS_METHOD_WR,
                          cls_account_resource_rm, &h_account_resource_rm);
  cls_register_cxx_method(h_class, "account_resource_list", CLS_METHOD_RD,
                          cls_account_resource_list, &h_account_resource_list);
}
