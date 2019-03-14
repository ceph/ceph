// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include <errno.h>

#include "objclass/objclass.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw/cls_rgw_const.h"
#include "common/Clock.h"
#include "common/strtol.h"
#include "common/escape.h"

#include "include/compat.h"

CLS_VER(1,0)
CLS_NAME(rgw)


#define BI_PREFIX_CHAR 0x80

#define BI_BUCKET_OBJS_INDEX          0
#define BI_BUCKET_LOG_INDEX           1
#define BI_BUCKET_OBJ_INSTANCE_INDEX  2
#define BI_BUCKET_OLH_DATA_INDEX      3

#define BI_BUCKET_LAST_INDEX          4

static std::string bucket_index_prefixes[] = { "", /* special handling for the objs list index */
                                          "0_",     /* bucket log index */
                                          "1000_",  /* obj instance index */
                                          "1001_",  /* olh data index */

                                          /* this must be the last index */
                                          "9999_",};

static bool bi_is_objs_index(const string& s) {
  return ((unsigned char)s[0] != BI_PREFIX_CHAR);
}

int bi_entry_type(const string& s)
{
  if (bi_is_objs_index(s)) {
    return BI_BUCKET_OBJS_INDEX;
  }

  for (size_t i = 1;
       i < sizeof(bucket_index_prefixes) / sizeof(bucket_index_prefixes[0]);
       ++i) {
    const string& t = bucket_index_prefixes[i];

    if (s.compare(1, t.size(), t) == 0) {
      return i;
    }
  }

  return -EINVAL;
}

static bool bi_entry_gt(const string& first, const string& second)
{
  int fi = bi_entry_type(first);
  int si = bi_entry_type(second);

  if (fi > si) {
    return true;
  } else if (fi < si) {
    return false;
  }

  return first > second;
}

static void get_time_key(real_time& ut, string *key)
{
  char buf[32];
  ceph_timespec ts = ceph::real_clock::to_ceph_timespec(ut);
  snprintf(buf, 32, "%011llu.%09u", (unsigned long long)ts.tv_sec, (unsigned int)ts.tv_nsec);
  *key = buf;
}

static void get_index_ver_key(cls_method_context_t hctx, uint64_t index_ver, string *key)
{
  char buf[48];
  snprintf(buf, sizeof(buf), "%011llu.%llu.%d", (unsigned long long)index_ver,
           (unsigned long long)cls_current_version(hctx),
           cls_current_subop_num(hctx));
  *key = buf;
}

static void bi_log_prefix(string& key)
{
  key = BI_PREFIX_CHAR;
  key.append(bucket_index_prefixes[BI_BUCKET_LOG_INDEX]);
}

static void bi_log_index_key(cls_method_context_t hctx, string& key, string& id, uint64_t index_ver)
{
  bi_log_prefix(key);
  get_index_ver_key(hctx, index_ver, &id);
  key.append(id);
}

static int log_index_operation(cls_method_context_t hctx, cls_rgw_obj_key& obj_key, RGWModifyOp op,
                               string& tag, real_time& timestamp,
                               rgw_bucket_entry_ver& ver, RGWPendingState state, uint64_t index_ver,
                               string& max_marker, uint16_t bilog_flags, string *owner, string *owner_display_name, rgw_zone_set *zones_trace)
{
  bufferlist bl;

  rgw_bi_log_entry entry;

  entry.object = obj_key.name;
  entry.instance = obj_key.instance;
  entry.timestamp = timestamp;
  entry.op = op;
  entry.ver = ver;
  entry.state = state;
  entry.index_ver = index_ver;
  entry.tag = tag;
  entry.bilog_flags = bilog_flags;
  if (owner) {
    entry.owner = *owner;
  }
  if (owner_display_name) {
    entry.owner_display_name = *owner_display_name;
  }
  if (zones_trace) {
    entry.zones_trace = std::move(*zones_trace);
  }

  string key;
  bi_log_index_key(hctx, key, entry.id, index_ver);

  encode(entry, bl);

  if (entry.id > max_marker)
    max_marker = entry.id;

  return cls_cxx_map_set_val(hctx, key, &bl);
}

/*
 * read list of objects, skips objects in the ugly namespace
 */
static int get_obj_vals(cls_method_context_t hctx, const string& start, const string& filter_prefix,
                        int num_entries, map<string, bufferlist> *pkeys, bool *pmore)
{
  int ret = cls_cxx_map_get_vals(hctx, start, filter_prefix, num_entries, pkeys, pmore);
  if (ret < 0)
    return ret;

  if (pkeys->empty())
    return 0;

  auto last_element = pkeys->rbegin();
  if ((unsigned char)last_element->first[0] < BI_PREFIX_CHAR) {
    /* nothing to see here, move along */
    return 0;
  }

  auto first_element = pkeys->begin();
  if ((unsigned char)first_element->first[0] > BI_PREFIX_CHAR) {
    return 0;
  }

  /* let's rebuild the list, only keep entries we're interested in */
  auto comp = [](const pair<string, bufferlist>& l, const string &r) { return l.first < r; };
  string new_start = {static_cast<char>(BI_PREFIX_CHAR + 1)};

  auto lower = pkeys->lower_bound(string{static_cast<char>(BI_PREFIX_CHAR)});
  auto upper = std::lower_bound(lower, pkeys->end(), new_start, comp);
  pkeys->erase(lower, upper);

  if (num_entries == (int)pkeys->size())
    return 0;

  map<string, bufferlist> new_keys;

  /* now get some more keys */
  ret = cls_cxx_map_get_vals(hctx, new_start, filter_prefix, num_entries - pkeys->size(), &new_keys, pmore);
  if (ret < 0)
    return ret;

  pkeys->insert(std::make_move_iterator(new_keys.begin()),
                std::make_move_iterator(new_keys.end()));
  return 0;
}

/*
 * get a monotonically decreasing string representation.
 * For num = x, num = y, where x > y, str(x) < str(y)
 * Another property is that string size starts short and grows as num increases
 */
static void decreasing_str(uint64_t num, string *str)
{
  char buf[32];
  if (num < 0x10) { /* 16 */
    snprintf(buf, sizeof(buf), "9%02lld", 15 - (long long)num);
  } else if (num < 0x100) { /* 256 */
    snprintf(buf, sizeof(buf), "8%03lld", 255 - (long long)num);
  } else if (num < 0x1000) /* 4096 */ {
    snprintf(buf, sizeof(buf), "7%04lld", 4095 - (long long)num);
  } else if (num < 0x10000) /* 65536 */ {
    snprintf(buf, sizeof(buf), "6%05lld", 65535 - (long long)num);
  } else if (num < 0x100000000) /* 4G */ {
    snprintf(buf, sizeof(buf), "5%010lld", 0xFFFFFFFF - (long long)num);
  } else {
    snprintf(buf, sizeof(buf), "4%020lld",  (long long)-num);
  }

  *str = buf;
}

/*
 * we now hold two different indexes for objects. The first one holds the list of objects in the
 * order that we want them to be listed. The second one only holds the objects instances (for
 * versioned objects), and they're not arranged in any particular order.
 * When listing objects we'll use the first index, when doing operations on the objects themselves
 * we'll use the second index. Note that regular objects only map to the first index anyway
 */

static void get_list_index_key(rgw_bucket_dir_entry& entry, string *index_key)
{
  *index_key = entry.key.name;

  string ver_str;
  decreasing_str(entry.versioned_epoch, &ver_str);
  string instance_delim("\0i", 2);
  string ver_delim("\0v", 2);

  index_key->append(ver_delim);
  index_key->append(ver_str);
  index_key->append(instance_delim);
  index_key->append(entry.key.instance);
}

static void encode_obj_versioned_data_key(const cls_rgw_obj_key& key, string *index_key, bool append_delete_marker_suffix = false)
{
  *index_key = BI_PREFIX_CHAR;
  index_key->append(bucket_index_prefixes[BI_BUCKET_OBJ_INSTANCE_INDEX]);
  index_key->append(key.name);
  string delim("\0i", 2);
  index_key->append(delim);
  index_key->append(key.instance);
  if (append_delete_marker_suffix) {
    string dm("\0d", 2);
    index_key->append(dm);
  }
}

static void encode_obj_index_key(const cls_rgw_obj_key& key, string *index_key)
{
  if (key.instance.empty()) {
    *index_key = key.name;
  } else {
    encode_obj_versioned_data_key(key, index_key);
  }
}

static void encode_olh_data_key(const cls_rgw_obj_key& key, string *index_key)
{
  *index_key = BI_PREFIX_CHAR;
  index_key->append(bucket_index_prefixes[BI_BUCKET_OLH_DATA_INDEX]);
  index_key->append(key.name);
}

template <class T>
static int read_index_entry(cls_method_context_t hctx, string& name, T *entry);

static int encode_list_index_key(cls_method_context_t hctx, const cls_rgw_obj_key& key, string *index_key)
{
  if (key.instance.empty()) {
    *index_key = key.name;
    return 0;
  }

  string obj_index_key;
  encode_obj_index_key(key, &obj_index_key);

  rgw_bucket_dir_entry entry;

  int ret = read_index_entry(hctx, obj_index_key, &entry);
  if (ret == -ENOENT) {
   /* couldn't find the entry, set key value after the current object */
    char buf[2] = { 0x1, 0 };
    string s(buf);
    *index_key  = key.name + s;
    return 0;
  }
  if (ret < 0) {
    CLS_LOG(1, "ERROR: encode_list_index_key(): cls_cxx_map_get_val returned %d\n", ret);
    return ret;
  }

  get_list_index_key(entry, index_key);

  return 0;
}

static void split_key(const string& key, list<string>& vals)
{
  size_t pos = 0;
  const char *p = key.c_str();
  while (pos < key.size()) {
    size_t len = strlen(p);
    vals.push_back(p);
    pos += len + 1;
    p += len + 1;
  }
}

static string escape_str(const string& s)
{
  int len = escape_json_attr_len(s.c_str(), s.size());
  std::string escaped(len, 0);
  escape_json_attr(s.c_str(), s.size(), escaped.data());
  return escaped;
}

/*
 * list index key structure:
 *
 * <obj name>\0[v<ver>\0i<instance id>]
 */
static int decode_list_index_key(const string& index_key, cls_rgw_obj_key *key, uint64_t *ver)
{
  size_t len = strlen(index_key.c_str());

  key->instance.clear();
  *ver = 0;

  if (len == index_key.size()) {
    key->name = index_key;
    return 0;
  }

  list<string> vals;
  split_key(index_key, vals);

  if (vals.empty()) {
    CLS_LOG(0, "ERROR: %s(): bad index_key (%s): split_key() returned empty vals", __func__, escape_str(index_key).c_str());
    return -EIO;
  }

  list<string>::iterator iter = vals.begin();
  key->name = *iter;
  ++iter;

  if (iter == vals.end()) {
    CLS_LOG(0, "ERROR: %s(): bad index_key (%s): no vals", __func__, escape_str(index_key).c_str());
    return -EIO;
  }

  for (; iter != vals.end(); ++iter) {
    string& val = *iter;
    if (val[0] == 'i') {
      key->instance = val.substr(1);
    } else if (val[0] == 'v') {
      string err;
      const char *s = val.c_str() + 1;
      *ver = strict_strtoll(s, 10, &err);
      if (!err.empty()) {
        CLS_LOG(0, "ERROR: %s(): bad index_key (%s): could not parse val (v=%s)", __func__, escape_str(index_key).c_str(), s);
        return -EIO;
      }
    }
  }

  return 0;
}

static int read_bucket_header(cls_method_context_t hctx,
			      rgw_bucket_dir_header *header)
{
  bufferlist bl;
  int rc = cls_cxx_map_read_header(hctx, &bl);
  if (rc < 0)
    return rc;

  if (bl.length() == 0) {
      *header = rgw_bucket_dir_header();
      return 0;
  }
  auto iter = bl.cbegin();
  try {
    decode(*header, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: read_bucket_header(): failed to decode header\n");
    return -EIO;
  }

  return 0;
}

int rgw_bucket_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto iter = in->cbegin();

  rgw_cls_list_op op;
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to decode request\n");
    return -EINVAL;
  }

  rgw_cls_list_ret ret;
  rgw_bucket_dir& new_dir = ret.dir;
  int rc = read_bucket_header(hctx, &new_dir.header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to read header\n");
    return rc;
  }

  map<string, bufferlist> keys;
  std::map<string, bufferlist>::iterator kiter;
  string start_key;
  encode_list_index_key(hctx, op.start_obj, &start_key);
  bool done = false;
  uint32_t left_to_read = op.num_entries;
  bool more;

  do {
    rc = get_obj_vals(hctx, start_key, op.filter_prefix, left_to_read, &keys, &more);
    if (rc < 0)
      return rc;

    std::map<string, rgw_bucket_dir_entry>& m = new_dir.m;

    done = keys.empty();

    for (kiter = keys.begin(); kiter != keys.end(); ++kiter) {
      rgw_bucket_dir_entry entry;

      if (!bi_is_objs_index(kiter->first)) {
        done = true;
        break;
      }

      bufferlist& entrybl = kiter->second;
      auto eiter = entrybl.cbegin();
      try {
        decode(entry, eiter);
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to decode entry, key=%s\n", kiter->first.c_str());
        return -EINVAL;
      }

      cls_rgw_obj_key key;
      uint64_t ver;

      start_key = kiter->first;
      CLS_LOG(20, "start_key=%s len=%zu", start_key.c_str(), start_key.size());

      int ret = decode_list_index_key(kiter->first, &key, &ver);
      if (ret < 0) {
        CLS_LOG(0, "ERROR: failed to decode list index key (%s)\n", escape_str(kiter->first).c_str());
        continue;
      }

      if (!entry.is_valid()) {
        CLS_LOG(20, "entry %s[%s] is not valid\n", key.name.c_str(), key.instance.c_str());
        continue;
      }
      
      // filter out noncurrent versions, delete markers, and initial marker
      if (!op.list_versions && (!entry.is_visible() || op.start_obj.name == key.name)) {
        CLS_LOG(20, "entry %s[%s] is not visible\n", key.name.c_str(), key.instance.c_str());
        continue;
      }
      if (m.size() < op.num_entries) {
        m[kiter->first] = entry;
      }
      left_to_read--;

      CLS_LOG(20, "got entry %s[%s] m.size()=%d\n", key.name.c_str(), key.instance.c_str(), (int)m.size());
    }
  } while (left_to_read > 0 && !done);

  ret.is_truncated = more && !done;

  encode(ret, *out);
  return 0;
}

static int check_index(cls_method_context_t hctx,
		       rgw_bucket_dir_header *existing_header,
		       rgw_bucket_dir_header *calc_header)
{
  int rc = read_bucket_header(hctx, existing_header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: check_index(): failed to read header\n");
    return rc;
  }

  calc_header->tag_timeout = existing_header->tag_timeout;
  calc_header->ver = existing_header->ver;

  map<string, bufferlist> keys;
  string start_obj;
  string filter_prefix;

#define CHECK_CHUNK_SIZE 1000
  bool done = false;
  bool more;

  do {
    rc = get_obj_vals(hctx, start_obj, filter_prefix, CHECK_CHUNK_SIZE, &keys, &more);
    if (rc < 0)
      return rc;

    std::map<string, bufferlist>::iterator kiter = keys.begin();
    for (; kiter != keys.end(); ++kiter) {
      if (!bi_is_objs_index(kiter->first)) {
        done = true;
        break;
      }

      rgw_bucket_dir_entry entry;
      auto eiter = kiter->second.cbegin();
      try {
        decode(entry, eiter);
      } catch (buffer::error& err) {
        CLS_LOG(1, "ERROR: rgw_bucket_list(): failed to decode entry, key=%s\n", kiter->first.c_str());
        return -EIO;
      }
      rgw_bucket_category_stats& stats = calc_header->stats[entry.meta.category];
      stats.num_entries++;
      stats.total_size += entry.meta.accounted_size;
      stats.total_size_rounded += cls_rgw_get_rounded_size(entry.meta.accounted_size);
      stats.actual_size += entry.meta.size;

      start_obj = kiter->first;
    }
  } while (keys.size() == CHECK_CHUNK_SIZE && !done);

  return 0;
}

int rgw_bucket_check_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  rgw_cls_check_index_ret ret;

  int rc = check_index(hctx, &ret.existing_header, &ret.calculated_header);
  if (rc < 0)
    return rc;

  encode(ret, *out);

  return 0;
}

static int write_bucket_header(cls_method_context_t hctx, rgw_bucket_dir_header *header)
{
  header->ver++;

  bufferlist header_bl;
  encode(*header, header_bl);
  return cls_cxx_map_write_header(hctx, &header_bl);
}


int rgw_bucket_rebuild_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  rgw_bucket_dir_header existing_header;
  rgw_bucket_dir_header calc_header;
  int rc = check_index(hctx, &existing_header, &calc_header);
  if (rc < 0)
    return rc;

  return write_bucket_header(hctx, &calc_header);
}

int rgw_bucket_update_stats(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_bucket_update_stats_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: %s(): failed to decode request\n", __func__);
    return -EINVAL;
  }

  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: %s(): failed to read header\n", __func__);
    return rc;
  }

  for (auto& s : op.stats) {
    auto& dest = header.stats[s.first];
    if (op.absolute) {
      dest = s.second;
    } else {
      dest.total_size += s.second.total_size;
      dest.total_size_rounded += s.second.total_size_rounded;
      dest.num_entries += s.second.num_entries;
      dest.actual_size += s.second.actual_size;
    }
  }

  return write_bucket_header(hctx, &header);
}

int rgw_bucket_init_index(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
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

  return write_bucket_header(hctx, &dir.header);
}

int rgw_bucket_set_tag_timeout(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_tag_timeout_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_set_tag_timeout(): failed to decode request\n");
    return -EINVAL;
  }

  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: rgw_bucket_set_tag_timeout(): failed to read header\n");
    return rc;
  }

  header.tag_timeout = op.tag_timeout;

  return write_bucket_header(hctx, &header);
}

static int read_key_entry(cls_method_context_t hctx, cls_rgw_obj_key& key,
			  string *idx, rgw_bucket_dir_entry *entry,
                          bool special_delete_marker_name = false);

int rgw_bucket_prepare_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_prepare_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_prepare_op(): failed to decode request\n");
    return -EINVAL;
  }

  if (op.tag.empty()) {
    CLS_LOG(1, "ERROR: tag is empty\n");
    return -EINVAL;
  }

  CLS_LOG(1, "rgw_bucket_prepare_op(): request: op=%d name=%s instance=%s tag=%s\n",
          op.op, op.key.name.c_str(), op.key.instance.c_str(), op.tag.c_str());

  // get on-disk state
  string idx;

  rgw_bucket_dir_entry entry;
  int rc = read_key_entry(hctx, op.key, &idx, &entry);
  if (rc < 0 && rc != -ENOENT)
    return rc;

  bool noent = (rc == -ENOENT);

  rc = 0;

  if (noent) { // no entry, initialize fields
    entry.key = op.key;
    entry.ver = rgw_bucket_entry_ver();
    entry.exists = false;
    entry.locator = op.locator;
  }

  // fill in proper state
  rgw_bucket_pending_info info;
  info.timestamp = real_clock::now();
  info.state = CLS_RGW_STATE_PENDING_MODIFY;
  info.op = op.op;
  entry.pending_map.insert(pair<string, rgw_bucket_pending_info>(op.tag, info));

  rgw_bucket_dir_header header;
  rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: rgw_bucket_prepare_op(): failed to read header\n");
    return rc;
  }

  if (op.log_op && !header.syncstopped) {
    rc = log_index_operation(hctx, op.key, op.op, op.tag, entry.meta.mtime,
                             entry.ver, info.state, header.ver, header.max_marker, op.bilog_flags, NULL, NULL, &op.zones_trace);
    if (rc < 0)
      return rc;
  }

  // write out new key to disk
  bufferlist info_bl;
  encode(entry, info_bl);
  rc = cls_cxx_map_set_val(hctx, idx, &info_bl);
  if (rc < 0)
    return rc;

  if (op.log_op && !header.syncstopped)
    return write_bucket_header(hctx, &header);
  return 0;
}

static void unaccount_entry(rgw_bucket_dir_header& header,
			    rgw_bucket_dir_entry& entry)
{
  rgw_bucket_category_stats& stats = header.stats[entry.meta.category];
  stats.num_entries--;
  stats.total_size -= entry.meta.accounted_size;
  stats.total_size_rounded -= cls_rgw_get_rounded_size(entry.meta.accounted_size);
  stats.actual_size -= entry.meta.size;
}

static void log_entry(const char *func, const char *str, rgw_bucket_dir_entry *entry)
{
  CLS_LOG(1, "%s(): %s: ver=%ld:%llu name=%s instance=%s locator=%s\n", func, str,
          (long)entry->ver.pool, (unsigned long long)entry->ver.epoch,
          entry->key.name.c_str(), entry->key.instance.c_str(), entry->locator.c_str());
}

static void log_entry(const char *func, const char *str, rgw_bucket_olh_entry *entry)
{
  CLS_LOG(1, "%s(): %s: epoch=%llu name=%s instance=%s tag=%s\n", func, str,
          (unsigned long long)entry->epoch, entry->key.name.c_str(), entry->key.instance.c_str(),
          entry->tag.c_str());
}

template <class T>
static int read_omap_entry(cls_method_context_t hctx, const std::string& name,
                           T* entry)
{
  bufferlist current_entry;
  int rc = cls_cxx_map_get_val(hctx, name, &current_entry);
  if (rc < 0) {
    return rc;
  }

  auto cur_iter = current_entry.cbegin();
  try {
    decode(*entry, cur_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: %s(): failed to decode entry\n", __func__);
    return -EIO;
  }
  return 0;
}

template <class T>
static int read_index_entry(cls_method_context_t hctx, string& name, T* entry)
{
  int ret = read_omap_entry(hctx, name, entry);
  if (ret < 0) {
    return ret;
  }

  log_entry(__func__, "existing entry", entry);
  return 0;
}

static int read_key_entry(cls_method_context_t hctx, cls_rgw_obj_key& key,
			  string *idx, rgw_bucket_dir_entry *entry,
                          bool special_delete_marker_name)
{
  encode_obj_index_key(key, idx);
  int rc = read_index_entry(hctx, *idx, entry);
  if (rc < 0) {
    return rc;
  }

  if (key.instance.empty() &&
      entry->flags & RGW_BUCKET_DIRENT_FLAG_VER_MARKER) {
    /* we only do it where key.instance is empty. In this case the delete marker will have a
     * separate entry in the index to avoid collisions with the actual object, as it's mutable
     */
    if (special_delete_marker_name) {
      encode_obj_versioned_data_key(key, idx, true);
      rc = read_index_entry(hctx, *idx, entry);
      if (rc == 0) {
        return 0;
      }
    }
    encode_obj_versioned_data_key(key, idx);
    rc = read_index_entry(hctx, *idx, entry);
    if (rc < 0) {
      *entry = rgw_bucket_dir_entry(); /* need to reset entry because we initialized it earlier */
      return rc;
    }
  }

  return 0;
}

int rgw_bucket_complete_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_complete_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bucket_complete_op(): failed to decode request\n");
    return -EINVAL;
  }
  CLS_LOG(1, "rgw_bucket_complete_op(): request: op=%d name=%s instance=%s ver=%lu:%llu tag=%s\n",
          op.op, op.key.name.c_str(), op.key.instance.c_str(),
          (unsigned long)op.ver.pool, (unsigned long long)op.ver.epoch,
          op.tag.c_str());

  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: rgw_bucket_complete_op(): failed to read header\n");
    return -EINVAL;
  }

  rgw_bucket_dir_entry entry;
  bool ondisk = true;

  string idx;
  rc = read_key_entry(hctx, op.key, &idx, &entry);
  if (rc == -ENOENT) {
    entry.key = op.key;
    entry.ver = op.ver;
    entry.meta = op.meta;
    entry.locator = op.locator;
    ondisk = false;
  } else if (rc < 0) {
    return rc;
  }

  entry.index_ver = header.ver;
  entry.flags = (entry.key.instance.empty() ? 0 : RGW_BUCKET_DIRENT_FLAG_VER); /* resetting entry flags, entry might have been previously a delete marker */

  if (op.tag.size()) {
    map<string, rgw_bucket_pending_info>::iterator pinter = entry.pending_map.find(op.tag);
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
  } else if (op.ver.pool == entry.ver.pool &&
             op.ver.epoch && op.ver.epoch <= entry.ver.epoch) {
    CLS_LOG(1, "rgw_bucket_complete_op(): skipping request, old epoch\n");
    cancel = true;
  }

  bufferlist op_bl;
  if (cancel) {
    if (op.log_op && !header.syncstopped) {
      rc = log_index_operation(hctx, op.key, op.op, op.tag, entry.meta.mtime, entry.ver,
                               CLS_RGW_STATE_COMPLETE, header.ver, header.max_marker, op.bilog_flags, NULL, NULL, &op.zones_trace);
      if (rc < 0)
        return rc;
    }

    if (op.tag.size()) {
      bufferlist new_key_bl;
      encode(entry, new_key_bl);
      rc = cls_cxx_map_set_val(hctx, idx, &new_key_bl);
      if (rc < 0)
        return rc;
    }

    if (op.log_op && !header.syncstopped) {
      return write_bucket_header(hctx, &header);
    }
    return 0;
  }

  if (entry.exists) {
    unaccount_entry(header, entry);
  }

  entry.ver = op.ver;
  switch ((int)op.op) {
  case CLS_RGW_OP_DEL:
    entry.meta = op.meta;
    if (ondisk) {
      if (!entry.pending_map.size()) {
	int ret = cls_cxx_map_remove_key(hctx, idx);
	if (ret < 0)
	  return ret;
      } else {
        entry.exists = false;
        bufferlist new_key_bl;
        encode(entry, new_key_bl);
	int ret = cls_cxx_map_set_val(hctx, idx, &new_key_bl);
	if (ret < 0)
	  return ret;
      }
    } else {
      return -ENOENT;
    }
    break;
  case CLS_RGW_OP_ADD:
    {
      rgw_bucket_dir_entry_meta& meta = op.meta;
      rgw_bucket_category_stats& stats = header.stats[meta.category];
      entry.meta = meta;
      entry.key = op.key;
      entry.exists = true;
      entry.tag = op.tag;
      stats.num_entries++;
      stats.total_size += meta.accounted_size;
      stats.total_size_rounded += cls_rgw_get_rounded_size(meta.accounted_size);
      stats.actual_size += meta.size;
      bufferlist new_key_bl;
      encode(entry, new_key_bl);
      int ret = cls_cxx_map_set_val(hctx, idx, &new_key_bl);
      if (ret < 0)
	return ret;
    }
    break;
  }

  if (op.log_op && !header.syncstopped) {
    rc = log_index_operation(hctx, op.key, op.op, op.tag, entry.meta.mtime, entry.ver,
                             CLS_RGW_STATE_COMPLETE, header.ver, header.max_marker, op.bilog_flags, NULL, NULL, &op.zones_trace);
    if (rc < 0)
      return rc;
  }

  list<cls_rgw_obj_key>::iterator remove_iter;
  CLS_LOG(20, "rgw_bucket_complete_op(): remove_objs.size()=%d\n", (int)op.remove_objs.size());
  for (remove_iter = op.remove_objs.begin(); remove_iter != op.remove_objs.end(); ++remove_iter) {
    cls_rgw_obj_key& remove_key = *remove_iter;
    CLS_LOG(1, "rgw_bucket_complete_op(): removing entries, read_index_entry name=%s instance=%s\n",
            remove_key.name.c_str(), remove_key.instance.c_str());
    rgw_bucket_dir_entry remove_entry;
    string k;
    int ret = read_key_entry(hctx, remove_key, &k, &remove_entry);
    if (ret < 0) {
      CLS_LOG(1, "rgw_bucket_complete_op(): removing entries, read_index_entry name=%s instance=%s ret=%d\n",
            remove_key.name.c_str(), remove_key.instance.c_str(), ret);
      continue;
    }
    CLS_LOG(0,
	    "rgw_bucket_complete_op(): entry.name=%s entry.instance=%s entry.meta.category=%d\n",
            remove_entry.key.name.c_str(),
	    remove_entry.key.instance.c_str(),
	    int(remove_entry.meta.category));
    unaccount_entry(header, remove_entry);

    if (op.log_op && !header.syncstopped) {
      ++header.ver; // increment index version, or we'll overwrite keys previously written
      rc = log_index_operation(hctx, remove_key, CLS_RGW_OP_DEL, op.tag, remove_entry.meta.mtime,
                               remove_entry.ver, CLS_RGW_STATE_COMPLETE, header.ver, header.max_marker, op.bilog_flags, NULL, NULL, &op.zones_trace);
      if (rc < 0)
        continue;
    }

    ret = cls_cxx_map_remove_key(hctx, k);
    if (ret < 0) {
      CLS_LOG(1, "rgw_bucket_complete_op(): cls_cxx_map_remove_key, failed to remove entry, name=%s instance=%s read_index_entry ret=%d\n", remove_key.name.c_str(), remove_key.instance.c_str(), rc);
      continue;
    }
  }

  return write_bucket_header(hctx, &header);
}

template <class T>
static int write_entry(cls_method_context_t hctx, T& entry, const string& key)
{
  bufferlist bl;
  encode(entry, bl);
  return cls_cxx_map_set_val(hctx, key, &bl);
}

static int read_olh(cls_method_context_t hctx,cls_rgw_obj_key& obj_key, rgw_bucket_olh_entry *olh_data_entry, string *index_key, bool *found)
{
  cls_rgw_obj_key olh_key;
  olh_key.name = obj_key.name;

  encode_olh_data_key(olh_key, index_key);
  int ret = read_index_entry(hctx, *index_key, olh_data_entry);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: read_index_entry() olh_key=%s ret=%d", olh_key.name.c_str(), ret);
    return ret;
  }
  if (found) {
    *found = (ret != -ENOENT);
  }
  return 0;
}

static void update_olh_log(rgw_bucket_olh_entry& olh_data_entry, OLHLogOp op, const string& op_tag,
                           cls_rgw_obj_key& key, bool delete_marker, uint64_t epoch)
{
  vector<rgw_bucket_olh_log_entry>& log = olh_data_entry.pending_log[olh_data_entry.epoch];
  rgw_bucket_olh_log_entry log_entry;
  log_entry.epoch = epoch;
  log_entry.op = op;
  log_entry.op_tag = op_tag;
  log_entry.key = key;
  log_entry.delete_marker = delete_marker;
  log.push_back(log_entry);
}

static int write_obj_instance_entry(cls_method_context_t hctx, rgw_bucket_dir_entry& instance_entry, const string& instance_idx)
{
  CLS_LOG(20, "write_entry() instance=%s idx=%s flags=%d", escape_str(instance_entry.key.instance).c_str(), instance_idx.c_str(), instance_entry.flags);
  /* write the instance entry */
  int ret = write_entry(hctx, instance_entry, instance_idx);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: write_entry() instance_key=%s ret=%d", escape_str(instance_idx).c_str(), ret);
    return ret;
  }
  return 0;
}

/*
 * write object instance entry, and if needed also the list entry
 */
static int write_obj_entries(cls_method_context_t hctx, rgw_bucket_dir_entry& instance_entry, const string& instance_idx)
{
  int ret = write_obj_instance_entry(hctx, instance_entry, instance_idx);
  if (ret < 0) {
    return ret;
  }
  string instance_list_idx;
  get_list_index_key(instance_entry, &instance_list_idx);

  if (instance_idx != instance_list_idx) {
    CLS_LOG(20, "write_entry() idx=%s flags=%d", escape_str(instance_list_idx).c_str(), instance_entry.flags);
    /* write a new list entry for the object instance */
    ret = write_entry(hctx, instance_entry, instance_list_idx);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: write_entry() instance=%s instance_list_idx=%s ret=%d", instance_entry.key.instance.c_str(), instance_list_idx.c_str(), ret);
      return ret;
    }
  }
  return 0;
}


class BIVerObjEntry {
  cls_method_context_t hctx;
  cls_rgw_obj_key key;
  string instance_idx;

  rgw_bucket_dir_entry instance_entry;

  bool initialized;

public:
  BIVerObjEntry(cls_method_context_t& _hctx, const cls_rgw_obj_key& _key) : hctx(_hctx), key(_key), initialized(false) {
  }

  int init(bool check_delete_marker = true) {
    int ret = read_key_entry(hctx, key, &instance_idx, &instance_entry,
                             check_delete_marker && key.instance.empty()); /* this is potentially a delete marker, for null objects we
                                                                              keep separate instance entry for the delete markers */

    if (ret < 0) {
      CLS_LOG(0, "ERROR: read_key_entry() idx=%s ret=%d", instance_idx.c_str(), ret);
      return ret;
    }
    initialized = true;
    CLS_LOG(20, "read instance_entry key.name=%s key.instance=%s flags=%d", instance_entry.key.name.c_str(), instance_entry.key.instance.c_str(), instance_entry.flags);
    return 0;
  }

  rgw_bucket_dir_entry& get_dir_entry() {
    return instance_entry;
  }

  void init_as_delete_marker(rgw_bucket_dir_entry_meta& meta) {
    /* a deletion marker, need to initialize it, there's no instance entry for it yet */
    instance_entry.key = key;
    instance_entry.flags = RGW_BUCKET_DIRENT_FLAG_DELETE_MARKER;
    instance_entry.meta = meta;
    instance_entry.tag = "delete-marker";

    initialized = true;
  }

  void set_epoch(uint64_t epoch) {
    instance_entry.versioned_epoch = epoch;
  }

  int unlink_list_entry() {
    string list_idx;
    /* this instance has a previous list entry, remove that entry */
    get_list_index_key(instance_entry, &list_idx);
    CLS_LOG(20, "unlink_list_entry() list_idx=%s", escape_str(list_idx).c_str());
    int ret = cls_cxx_map_remove_key(hctx, list_idx);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: cls_cxx_map_remove_key() list_idx=%s ret=%d", list_idx.c_str(), ret);
      return ret;
    }
    return 0;
  }

  int unlink() {
    /* remove the instance entry */
    CLS_LOG(20, "unlink() idx=%s", escape_str(instance_idx).c_str());
    int ret = cls_cxx_map_remove_key(hctx, instance_idx);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: cls_cxx_map_remove_key() instance_idx=%s ret=%d", instance_idx.c_str(), ret);
      return ret;
    }
    return 0;
  }

  int write_entries(uint64_t flags_set, uint64_t flags_reset) {
    if (!initialized) {
      int ret = init();
      if (ret < 0) {
        return ret;
      }
    }
    instance_entry.flags &= ~flags_reset;
    instance_entry.flags |= flags_set;

    /* write the instance and list entries */
    bool special_delete_marker_key = (instance_entry.is_delete_marker() && instance_entry.key.instance.empty());
    encode_obj_versioned_data_key(key, &instance_idx, special_delete_marker_key);
    int ret = write_obj_entries(hctx, instance_entry, instance_idx);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: write_obj_entries() instance_idx=%s ret=%d", instance_idx.c_str(), ret);
      return ret;
    }

    return 0;
  }

  int write(uint64_t epoch, bool current) {
    if (instance_entry.versioned_epoch > 0) {
      CLS_LOG(20, "%s(): instance_entry.versioned_epoch=%d epoch=%d", __func__, (int)instance_entry.versioned_epoch, (int)epoch);
      /* this instance has a previous list entry, remove that entry */
      int ret = unlink_list_entry();
      if (ret < 0) {
        return ret;
      }
    }

    uint64_t flags = RGW_BUCKET_DIRENT_FLAG_VER;
    if (current) {
      flags |= RGW_BUCKET_DIRENT_FLAG_CURRENT;
    }

    instance_entry.versioned_epoch = epoch;
    return write_entries(flags, 0);
  }

  int demote_current() {
    return write_entries(0, RGW_BUCKET_DIRENT_FLAG_CURRENT);
  }

  bool is_delete_marker() {
    return instance_entry.is_delete_marker();
  }

  int find_next_key(cls_rgw_obj_key *next_key, bool *found) {
    string list_idx;
    /* this instance has a previous list entry, remove that entry */
    get_list_index_key(instance_entry, &list_idx);
    /* this is the current head, need to update! */
    map<string, bufferlist> keys;
    bool more;
    string filter = key.name; /* list key starts with key name, filter it to avoid a case where we cross to
                                 different namespace */
    int ret = cls_cxx_map_get_vals(hctx, list_idx, filter, 1, &keys, &more);
    if (ret < 0) {
      return ret;
    }

    if (keys.size() < 1) {
      *found = false;
      return 0;
    }

    rgw_bucket_dir_entry next_entry;

    map<string, bufferlist>::reverse_iterator last = keys.rbegin();
    try {
      auto iter = last->second.cbegin();
      decode(next_entry, iter);
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR; failed to decode entry: %s", last->first.c_str());
      return -EIO;
    }

    *found = (key.name == next_entry.key.name);
    if (*found) {
      *next_key = next_entry.key;
    }

    return 0;
  }

  real_time mtime() {
    return instance_entry.meta.mtime;
  }
};


class BIOLHEntry {
  cls_method_context_t hctx;
  cls_rgw_obj_key key;

  string olh_data_idx;
  rgw_bucket_olh_entry olh_data_entry;

  bool initialized;
public:
  BIOLHEntry(cls_method_context_t& _hctx, const cls_rgw_obj_key& _key) : hctx(_hctx), key(_key), initialized(false) { }

  int init(bool *exists) {
    /* read olh */
    int ret = read_olh(hctx, key, &olh_data_entry, &olh_data_idx, exists);
    if (ret < 0) {
      return ret;
    }

    initialized = true;
    return 0;
  }

  bool start_modify(uint64_t candidate_epoch) {
    if (candidate_epoch) {
      if (candidate_epoch < olh_data_entry.epoch) {
        return false; /* olh cannot be modified, old epoch */
      }
      olh_data_entry.epoch = candidate_epoch;
    } else {
      if (olh_data_entry.epoch == 0) {
        olh_data_entry.epoch = 2; /* versioned epoch should start with 2, 1 is reserved to converted plain entries */
      } else {
        olh_data_entry.epoch++;
      }
    }
    return true;
  }

  uint64_t get_epoch() {
    return olh_data_entry.epoch;
  }

  rgw_bucket_olh_entry& get_entry() {
    return olh_data_entry;
  }

  void update(cls_rgw_obj_key& key, bool delete_marker) {
    olh_data_entry.delete_marker = delete_marker;
    olh_data_entry.key = key;
  }

  int write() {
    /* write the olh data entry */
    int ret = write_entry(hctx, olh_data_entry, olh_data_idx);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: write_entry() olh_key=%s ret=%d", olh_data_idx.c_str(), ret);
      return ret;
    }

    return 0;
  }

  void update_log(OLHLogOp op, const string& op_tag, cls_rgw_obj_key& key, bool delete_marker, uint64_t epoch = 0) {
    if (epoch == 0) {
      epoch = olh_data_entry.epoch;
    }
    update_olh_log(olh_data_entry, op, op_tag, key, delete_marker, epoch);
  }

  bool exists() { return olh_data_entry.exists; }

  void set_exists(bool exists) {
    olh_data_entry.exists = exists;
  }

  bool pending_removal() { return olh_data_entry.pending_removal; }

  void set_pending_removal(bool pending_removal) {
    olh_data_entry.pending_removal = pending_removal;
  }

  const string& get_tag() { return olh_data_entry.tag; }
  void set_tag(const string& tag) {
    olh_data_entry.tag = tag;
  }
};

static int write_version_marker(cls_method_context_t hctx, cls_rgw_obj_key& key)
{
  rgw_bucket_dir_entry entry;
  entry.key = key;
  entry.flags = RGW_BUCKET_DIRENT_FLAG_VER_MARKER;
  int ret = write_entry(hctx, entry, key.name);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: write_entry returned ret=%d", ret);
    return ret;
  }
  return 0;
}

/*
 * plain entries are the ones who were created when bucket was not versioned,
 * if we override these objects, we need to convert these to versioned entries -- ones that have
 * both data entry, and listing key. Their version is going to be empty though
 */
static int convert_plain_entry_to_versioned(cls_method_context_t hctx, cls_rgw_obj_key& key, bool demote_current, bool instance_only)
{
  if (!key.instance.empty()) {
    return -EINVAL;
  }

  rgw_bucket_dir_entry entry;

  string orig_idx;
  int ret = read_key_entry(hctx, key, &orig_idx, &entry);
  if (ret != -ENOENT) {
    if (ret < 0) {
      CLS_LOG(0, "ERROR: read_key_entry() returned ret=%d", ret);
      return ret;
    }

    entry.versioned_epoch = 1; /* converted entries are always 1 */
    entry.flags |= RGW_BUCKET_DIRENT_FLAG_VER;

    if (demote_current) {
      entry.flags &= ~RGW_BUCKET_DIRENT_FLAG_CURRENT;
    }

    string new_idx;
    encode_obj_versioned_data_key(key, &new_idx);

    if (instance_only) {
      ret = write_obj_instance_entry(hctx, entry, new_idx);
    } else {
      ret = write_obj_entries(hctx, entry, new_idx);
    }
    if (ret < 0) {
      CLS_LOG(0, "ERROR: write_obj_entries new_idx=%s returned %d", new_idx.c_str(), ret);
      return ret;
    }
  }

  ret = write_version_marker(hctx, key);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

/*
 * link an object version to an olh, update the relevant index entries. It will also handle the
 * deletion marker case. We have a few entries that we need to take care of. For object 'foo',
 * instance BAR, we'd update the following (not actual encoding):
 *  - olh data: [BI_BUCKET_OLH_DATA_INDEX]foo
 *  - object instance data: [BI_BUCKET_OBJ_INSTANCE_INDEX]foo,BAR
 *  - object instance list entry: foo,123,BAR
 *
 *  The instance list entry needs to be ordered by newer to older, so we generate an appropriate
 *  number string that follows the name.
 *  The top instance for each object is marked appropriately.
 *  We generate instance entry for deletion markers here, as they are not created prior.
 */
static int rgw_bucket_link_olh(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string olh_data_idx;
  string instance_idx;

  // decode request
  rgw_cls_link_olh_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: rgw_bucket_link_olh_op(): failed to decode request\n");
    return -EINVAL;
  }

  BIVerObjEntry obj(hctx, op.key);
  BIOLHEntry olh(hctx, op.key);

  /* read instance entry */
  int ret = obj.init(op.delete_marker);
  bool existed = (ret == 0);
  if (ret == -ENOENT && op.delete_marker) {
    ret = 0;
  }
  if (ret < 0) {
    return ret;
  }

  if (existed && !real_clock::is_zero(op.unmod_since)) {
    timespec mtime = ceph::real_clock::to_timespec(obj.mtime());
    timespec unmod = ceph::real_clock::to_timespec(op.unmod_since);
    if (!op.high_precision_time) {
      mtime.tv_nsec = 0;
      unmod.tv_nsec = 0;
    }
    if (mtime >= unmod) {
      return 0; /* no need to set error, we just return 0 and avoid writing to the bi log */
    }
  }

  bool removing;

  /*
   * Special handling for null instance object / delete-marker. For these objects we're going to
   * have separate instances for a data object vs. delete-marker to avoid collisions. We now check
   * if we got to overwrite a previous entry, and in that case we'll remove its list entry.
   */
  if (op.key.instance.empty()) {
    BIVerObjEntry other_obj(hctx, op.key);
    ret = other_obj.init(!op.delete_marker); /* try reading the other null versioned entry */
    existed = (ret >= 0 && !other_obj.is_delete_marker());
    if (ret >= 0 && other_obj.is_delete_marker() != op.delete_marker) {
      ret = other_obj.unlink_list_entry();
      if (ret < 0) {
        return ret;
      }
    }

    removing = existed && op.delete_marker;
    if (!removing) {
      ret = other_obj.unlink();
      if (ret < 0) {
        return ret;
      }
    }
  } else {
    removing = (existed && !obj.is_delete_marker() && op.delete_marker);
  }

  if (op.delete_marker) {
    /* a deletion marker, need to initialize entry as such */
    obj.init_as_delete_marker(op.meta);
  }

  /* read olh */
  bool olh_found;
  ret = olh.init(&olh_found);
  if (ret < 0) {
    return ret;
  }

  if (!olh.start_modify(op.olh_epoch)) {
    ret = obj.write(op.olh_epoch, false);
    if (ret < 0) {
      return ret;
    }
    if (removing) {
      olh.update_log(CLS_RGW_OLH_OP_REMOVE_INSTANCE, op.op_tag, op.key, false, op.olh_epoch);
    }
    return 0;
  }

  if (olh_found) {
    const string& olh_tag = olh.get_tag();
    if (op.olh_tag != olh_tag) {
      if (!olh.pending_removal()) {
        CLS_LOG(5, "NOTICE: op.olh_tag (%s) != olh.tag (%s)", op.olh_tag.c_str(), olh_tag.c_str());
        return -ECANCELED;
      }
      /* if pending removal, this is a new olh instance */
      olh.set_tag(op.olh_tag);
    }
    if (olh.exists()) {
      rgw_bucket_olh_entry& olh_entry = olh.get_entry();
      /* found olh, previous instance is no longer the latest, need to update */
      if (!(olh_entry.key == op.key)) {
        BIVerObjEntry old_obj(hctx, olh_entry.key);

        ret = old_obj.demote_current();
        if (ret < 0) {
          CLS_LOG(0, "ERROR: could not demote current on previous key ret=%d", ret);
          return ret;
        }
      }
    }
    olh.set_pending_removal(false);
  } else {
    bool instance_only = (op.key.instance.empty() && op.delete_marker);
    cls_rgw_obj_key key(op.key.name);
    ret = convert_plain_entry_to_versioned(hctx, key, true, instance_only);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: convert_plain_entry_to_versioned ret=%d", ret);
      return ret;
    }
    olh.set_tag(op.olh_tag);
  }

  /* update the olh log */
  olh.update_log(CLS_RGW_OLH_OP_LINK_OLH, op.op_tag, op.key, op.delete_marker);
  if (removing) {
    olh.update_log(CLS_RGW_OLH_OP_REMOVE_INSTANCE, op.op_tag, op.key, false);
  }

  olh.update(op.key, op.delete_marker);

  olh.set_exists(true);

  ret = olh.write();
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to update olh ret=%d", ret);
    return ret;
  }

  /* write the instance and list entries */
  ret = obj.write(olh.get_epoch(), true);
  if (ret < 0) {
    return ret;
  }

  rgw_bucket_dir_header header;
  ret = read_bucket_header(hctx, &header);
  if (ret < 0) {
    CLS_LOG(1, "ERROR: rgw_bucket_link_olh(): failed to read header\n");
    return ret;
  }

  if (op.log_op && !header.syncstopped) {
    rgw_bucket_dir_entry& entry = obj.get_dir_entry();

    rgw_bucket_entry_ver ver;
    ver.epoch = (op.olh_epoch ? op.olh_epoch : olh.get_epoch());

    string *powner = NULL;
    string *powner_display_name = NULL;

    if (op.delete_marker) {
      powner = &entry.meta.owner;
      powner_display_name = &entry.meta.owner_display_name;
    }

    RGWModifyOp operation = (op.delete_marker ? CLS_RGW_OP_LINK_OLH_DM : CLS_RGW_OP_LINK_OLH);
    ret = log_index_operation(hctx, op.key, operation, op.op_tag,
                              entry.meta.mtime, ver,
                              CLS_RGW_STATE_COMPLETE, header.ver, header.max_marker, op.bilog_flags | RGW_BILOG_FLAG_VERSIONED_OP,
                              powner, powner_display_name, &op.zones_trace);
    if (ret < 0)
      return ret;

    return write_bucket_header(hctx, &header); /* updates header version */
  }

  return 0;
}

static int rgw_bucket_unlink_instance(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  string olh_data_idx;
  string instance_idx;

  // decode request
  rgw_cls_unlink_instance_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: rgw_bucket_rm_obj_instance_op(): failed to decode request\n");
    return -EINVAL;
  }

  cls_rgw_obj_key dest_key = op.key;
  if (dest_key.instance == "null") {
    dest_key.instance.clear();
  }

  BIVerObjEntry obj(hctx, dest_key);
  BIOLHEntry olh(hctx, dest_key);

  int ret = obj.init();
  if (ret == -ENOENT) {
    return 0; /* already removed */
  }
  if (ret < 0) {
    CLS_LOG(0, "ERROR: obj.init() returned ret=%d", ret);
    return ret;
  }

  bool olh_found;
  ret = olh.init(&olh_found);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: olh.init() returned ret=%d", ret);
    return ret;
  }

  if (!olh_found) {
    bool instance_only = false;
    cls_rgw_obj_key key(dest_key.name);
    ret = convert_plain_entry_to_versioned(hctx, key, true, instance_only);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: convert_plain_entry_to_versioned ret=%d", ret);
      return ret;
    }
    olh.update(dest_key, false);
    olh.set_tag(op.olh_tag);

    obj.set_epoch(1);
  }

  if (!olh.start_modify(op.olh_epoch)) {
    ret = obj.unlink_list_entry();
    if (ret < 0) {
      return ret;
    }

    if (!obj.is_delete_marker()) {
      olh.update_log(CLS_RGW_OLH_OP_REMOVE_INSTANCE, op.op_tag, op.key, false, op.olh_epoch);
    }

    return 0;
  }

  rgw_bucket_olh_entry& olh_entry = olh.get_entry();
  cls_rgw_obj_key& olh_key = olh_entry.key;
  CLS_LOG(20, "%s(): updating olh log: existing olh entry: %s[%s] (delete_marker=%d)", __func__,
             olh_key.name.c_str(), olh_key.instance.c_str(), olh_entry.delete_marker);

  if (olh_key == dest_key) {
    /* this is the current head, need to update! */
    cls_rgw_obj_key next_key;
    bool found = false;
    ret = obj.find_next_key(&next_key, &found);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: obj.find_next_key() returned ret=%d", ret);
      return ret;
    }

    if (found) {
      BIVerObjEntry next(hctx, next_key);
      ret = next.write(olh.get_epoch(), true);
      if (ret < 0) {
        CLS_LOG(0, "ERROR: next.write() returned ret=%d", ret);
        return ret;
      }

      CLS_LOG(20, "%s(): updating olh log: link olh -> %s[%s] (is_delete=%d)", __func__,
              next_key.name.c_str(), next_key.instance.c_str(), (int)next.is_delete_marker());

      olh.update(next_key, next.is_delete_marker());
      olh.update_log(CLS_RGW_OLH_OP_LINK_OLH, op.op_tag, next_key, next.is_delete_marker());
    } else {
      /* next_key is empty */
      olh.update(next_key, false);
      olh.update_log(CLS_RGW_OLH_OP_UNLINK_OLH, op.op_tag, next_key, false);
      olh.set_exists(false);
      olh.set_pending_removal(true);
    }
  }

  if (!obj.is_delete_marker()) {
    olh.update_log(CLS_RGW_OLH_OP_REMOVE_INSTANCE, op.op_tag, op.key, false);
  } else {
    /* this is a delete marker, it's our responsibility to remove its instance entry */
    ret = obj.unlink();
    if (ret < 0) {
      return ret;
    }
  }

  ret = obj.unlink_list_entry();
  if (ret < 0) {
    return ret;
  }

  ret = olh.write();
  if (ret < 0) {
    return ret;
  }

  rgw_bucket_dir_header header;
  ret = read_bucket_header(hctx, &header);
  if (ret < 0) {
    CLS_LOG(1, "ERROR: rgw_bucket_unlink_instance(): failed to read header\n");
    return ret;
  }

  if (op.log_op && !header.syncstopped) {
    rgw_bucket_entry_ver ver;
    ver.epoch = (op.olh_epoch ? op.olh_epoch : olh.get_epoch());

    real_time mtime = obj.mtime(); /* mtime has no real meaning in instance removal context */
    ret = log_index_operation(hctx, op.key, CLS_RGW_OP_UNLINK_INSTANCE, op.op_tag,
                              mtime, ver,
                              CLS_RGW_STATE_COMPLETE, header.ver, header.max_marker,
                              op.bilog_flags | RGW_BILOG_FLAG_VERSIONED_OP, NULL, NULL, &op.zones_trace);
    if (ret < 0)
      return ret;

    return write_bucket_header(hctx, &header); /* updates header version */
  }

  return 0;
}

static int rgw_bucket_read_olh_log(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_read_olh_log_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: rgw_bucket_read_olh_log(): failed to decode request\n");
    return -EINVAL;
  }

  if (!op.olh.instance.empty()) {
    CLS_LOG(1, "bad key passed in (non empty instance)");
    return -EINVAL;
  }

  rgw_bucket_olh_entry olh_data_entry;
  string olh_data_key;
  encode_olh_data_key(op.olh, &olh_data_key);
  int ret = read_index_entry(hctx, olh_data_key, &olh_data_entry);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: read_index_entry() olh_key=%s ret=%d", olh_data_key.c_str(), ret);
    return ret;
  }

  if (olh_data_entry.tag != op.olh_tag) {
    CLS_LOG(1, "NOTICE: %s(): olh_tag_mismatch olh_data_entry.tag=%s op.olh_tag=%s", __func__, olh_data_entry.tag.c_str(), op.olh_tag.c_str());
    return -ECANCELED;
  }

  rgw_cls_read_olh_log_ret op_ret;

#define MAX_OLH_LOG_ENTRIES 1000
  map<uint64_t, vector<rgw_bucket_olh_log_entry> >& log = olh_data_entry.pending_log;

  if (log.begin()->first > op.ver_marker && log.size() <= MAX_OLH_LOG_ENTRIES) {
    op_ret.log = log;
    op_ret.is_truncated = false;
  } else {
    map<uint64_t, vector<rgw_bucket_olh_log_entry> >::iterator iter = log.upper_bound(op.ver_marker);

    for (int i = 0; i < MAX_OLH_LOG_ENTRIES && iter != log.end(); ++i, ++iter) {
      op_ret.log[iter->first] = iter->second;
    }
    op_ret.is_truncated = (iter != log.end());
  }

  encode(op_ret, *out);

  return 0;
}

static int rgw_bucket_trim_olh_log(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_trim_olh_log_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: rgw_bucket_trim_olh_log(): failed to decode request\n");
    return -EINVAL;
  }

  if (!op.olh.instance.empty()) {
    CLS_LOG(1, "bad key passed in (non empty instance)");
    return -EINVAL;
  }

  /* read olh entry */
  rgw_bucket_olh_entry olh_data_entry;
  string olh_data_key;
  encode_olh_data_key(op.olh, &olh_data_key);
  int ret = read_index_entry(hctx, olh_data_key, &olh_data_entry);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: read_index_entry() olh_key=%s ret=%d", olh_data_key.c_str(), ret);
    return ret;
  }

  if (olh_data_entry.tag != op.olh_tag) {
    CLS_LOG(1, "NOTICE: %s(): olh_tag_mismatch olh_data_entry.tag=%s op.olh_tag=%s", __func__, olh_data_entry.tag.c_str(), op.olh_tag.c_str());
    return -ECANCELED;
  }

  /* remove all versions up to and including ver from the pending map */
  map<uint64_t, vector<rgw_bucket_olh_log_entry> >& log = olh_data_entry.pending_log;
  map<uint64_t, vector<rgw_bucket_olh_log_entry> >::iterator liter = log.begin();
  while (liter != log.end() && liter->first <= op.ver) {
    map<uint64_t, vector<rgw_bucket_olh_log_entry> >::iterator rm_iter = liter;
    ++liter;
    log.erase(rm_iter);
  }

  /* write the olh data entry */
  ret = write_entry(hctx, olh_data_entry, olh_data_key);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: write_entry() olh_key=%s ret=%d", olh_data_key.c_str(), ret);
    return ret;
  }

  return 0;
}

static int rgw_bucket_clear_olh(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_bucket_clear_olh_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: rgw_bucket_clear_olh(): failed to decode request\n");
    return -EINVAL;
  }

  if (!op.key.instance.empty()) {
    CLS_LOG(1, "bad key passed in (non empty instance)");
    return -EINVAL;
  }

  /* read olh entry */
  rgw_bucket_olh_entry olh_data_entry;
  string olh_data_key;
  encode_olh_data_key(op.key, &olh_data_key);
  int ret = read_index_entry(hctx, olh_data_key, &olh_data_entry);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: read_index_entry() olh_key=%s ret=%d", olh_data_key.c_str(), ret);
    return ret;
  }

  if (olh_data_entry.tag != op.olh_tag) {
    CLS_LOG(1, "NOTICE: %s(): olh_tag_mismatch olh_data_entry.tag=%s op.olh_tag=%s", __func__, olh_data_entry.tag.c_str(), op.olh_tag.c_str());
    return -ECANCELED;
  }

  ret = cls_cxx_map_remove_key(hctx, olh_data_key);
  if (ret < 0) {
    CLS_LOG(1, "NOTICE: %s(): can't remove key %s ret=%d", __func__, olh_data_key.c_str(), ret);
    return ret;
  }

  rgw_bucket_dir_entry plain_entry;

  /* read plain entry, make sure it's a versioned place holder */
  ret = read_index_entry(hctx, op.key.name, &plain_entry);
  if (ret == -ENOENT) {
    /* we're done, no entry existing */
    return 0;
  }
  if (ret < 0) {
    CLS_LOG(0, "ERROR: read_index_entry key=%s ret=%d", op.key.name.c_str(), ret);
    return ret;
  }

  if ((plain_entry.flags & RGW_BUCKET_DIRENT_FLAG_VER_MARKER) == 0) {
    /* it's not a version marker, don't remove it */
    return 0;
  }

  ret = cls_cxx_map_remove_key(hctx, op.key.name);
  if (ret < 0) {
    CLS_LOG(1, "NOTICE: %s(): can't remove key %s ret=%d", __func__, op.key.name.c_str(), ret);
    return ret;
  }

  return 0;
}

int rgw_dir_suggest_changes(cls_method_context_t hctx,
			    bufferlist *in, bufferlist *out)
{
  CLS_LOG(1, "rgw_dir_suggest_changes()");

  bufferlist header_bl;
  rgw_bucket_dir_header header;
  bool header_changed = false;

  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: rgw_dir_suggest_changes(): failed to read header\n");
    return rc;
  }

  timespan tag_timeout(
    std::chrono::seconds(
      header.tag_timeout ? header.tag_timeout : CEPH_RGW_TAG_TIMEOUT));

  auto in_iter = in->cbegin();

  while (!in_iter.end()) {
    __u8 op;
    rgw_bucket_dir_entry cur_change;
    rgw_bucket_dir_entry cur_disk;
    try {
      decode(op, in_iter);
      decode(cur_change, in_iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: rgw_dir_suggest_changes(): failed to decode request\n");
      return -EINVAL;
    }

    bufferlist cur_disk_bl;
    string cur_change_key;
    encode_obj_index_key(cur_change.key, &cur_change_key);
    int ret = cls_cxx_map_get_val(hctx, cur_change_key, &cur_disk_bl);
    if (ret < 0 && ret != -ENOENT)
      return -EINVAL;

    if (ret == -ENOENT) {
      continue;
    }

    if (cur_disk_bl.length()) {
      auto cur_disk_iter = cur_disk_bl.cbegin();
      try {
        decode(cur_disk, cur_disk_iter);
      } catch (buffer::error& error) {
        CLS_LOG(1, "ERROR: rgw_dir_suggest_changes(): failed to decode cur_disk\n");
        return -EINVAL;
      }

      real_time cur_time = real_clock::now();
      map<string, rgw_bucket_pending_info>::iterator iter =
                cur_disk.pending_map.begin();
      while(iter != cur_disk.pending_map.end()) {
        map<string, rgw_bucket_pending_info>::iterator cur_iter=iter++;
        if (cur_time > (cur_iter->second.timestamp + timespan(tag_timeout))) {
          cur_disk.pending_map.erase(cur_iter);
        }
      }
    }

    CLS_LOG(20, "cur_disk.pending_map.empty()=%d op=%d cur_disk.exists=%d cur_change.pending_map.size()=%d cur_change.exists=%d\n",
	    cur_disk.pending_map.empty(), (int)op, cur_disk.exists,
	    (int)cur_change.pending_map.size(), cur_change.exists);

    if (cur_disk.pending_map.empty()) {
      if (cur_disk.exists) {
        rgw_bucket_category_stats& old_stats = header.stats[cur_disk.meta.category];
        CLS_LOG(10, "total_entries: %" PRId64 " -> %" PRId64 "\n", old_stats.num_entries, old_stats.num_entries - 1);
        old_stats.num_entries--;
        old_stats.total_size -= cur_disk.meta.accounted_size;
        old_stats.total_size_rounded -= cls_rgw_get_rounded_size(cur_disk.meta.accounted_size);
        old_stats.actual_size -= cur_disk.meta.size;
        header_changed = true;
      }
      rgw_bucket_category_stats& stats = header.stats[cur_change.meta.category];
      bool log_op = (op & CEPH_RGW_DIR_SUGGEST_LOG_OP) != 0;
      op &= CEPH_RGW_DIR_SUGGEST_OP_MASK;
      switch(op) {
      case CEPH_RGW_REMOVE:
        CLS_LOG(10, "CEPH_RGW_REMOVE name=%s instance=%s\n", cur_change.key.name.c_str(), cur_change.key.instance.c_str());
	ret = cls_cxx_map_remove_key(hctx, cur_change_key);
	if (ret < 0)
	  return ret;
        if (log_op && cur_disk.exists && !header.syncstopped) {
          ret = log_index_operation(hctx, cur_disk.key, CLS_RGW_OP_DEL, cur_disk.tag, cur_disk.meta.mtime,
                                    cur_disk.ver, CLS_RGW_STATE_COMPLETE, header.ver, header.max_marker, 0, NULL, NULL, NULL);
          if (ret < 0) {
            CLS_LOG(0, "ERROR: %s(): failed to log operation ret=%d", __func__, ret);
            return ret;
          }
        }
        break;
      case CEPH_RGW_UPDATE:
        CLS_LOG(10, "CEPH_RGW_UPDATE name=%s instance=%s total_entries: %" PRId64 " -> %" PRId64 "\n",
                cur_change.key.name.c_str(), cur_change.key.instance.c_str(), stats.num_entries, stats.num_entries + 1);

        stats.num_entries++;
        stats.total_size += cur_change.meta.accounted_size;
        stats.total_size_rounded += cls_rgw_get_rounded_size(cur_change.meta.accounted_size);
        stats.actual_size += cur_change.meta.size;
        header_changed = true;
        cur_change.index_ver = header.ver;
        bufferlist cur_state_bl;
        encode(cur_change, cur_state_bl);
        ret = cls_cxx_map_set_val(hctx, cur_change_key, &cur_state_bl);
        if (ret < 0)
	  return ret;
        if (log_op && !header.syncstopped) {
          ret = log_index_operation(hctx, cur_change.key, CLS_RGW_OP_ADD, cur_change.tag, cur_change.meta.mtime,
                                    cur_change.ver, CLS_RGW_STATE_COMPLETE, header.ver, header.max_marker, 0, NULL, NULL, NULL);
          if (ret < 0) {
            CLS_LOG(0, "ERROR: %s(): failed to log operation ret=%d", __func__, ret);
            return ret;
          }
        }
        break;
      } // switch(op)
    } // if (cur_disk.pending_map.empty())
  } // while (!in_iter.end())

  if (header_changed) {
    return write_bucket_header(hctx, &header);
  }
  return 0;
}

static int rgw_obj_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_remove_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  if (op.keep_attr_prefixes.empty()) {
    return cls_cxx_remove(hctx);
  }

  map<string, bufferlist> attrset;
  int ret = cls_cxx_getxattrs(hctx, &attrset);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_getxattrs() returned %d", __func__, ret);
    return ret;
  }

  map<string, bufferlist> new_attrs;
  for (list<string>::iterator iter = op.keep_attr_prefixes.begin();
       iter != op.keep_attr_prefixes.end(); ++iter) {
    string& check_prefix = *iter;

    for (map<string, bufferlist>::iterator aiter = attrset.lower_bound(check_prefix);
         aiter != attrset.end(); ++aiter) {
      const string& attr = aiter->first;

      if (attr.substr(0, check_prefix.size()) > check_prefix) {
        break;
      }

      new_attrs[attr] = aiter->second;
    }
  }

  CLS_LOG(20, "%s(): removing object", __func__);
  ret = cls_cxx_remove(hctx);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_remove returned %d", __func__, ret);
    return ret;
  }

  if (new_attrs.empty()) {
    /* no data to keep */
    return 0;
  }

  ret = cls_cxx_create(hctx, false);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_create returned %d", __func__, ret);
    return ret;
  }

  for (map<string, bufferlist>::iterator aiter = new_attrs.begin();
       aiter != new_attrs.end(); ++aiter) {
    const string& attr = aiter->first;

    ret = cls_cxx_setxattr(hctx, attr.c_str(), &aiter->second);
    CLS_LOG(20, "%s(): setting attr: %s", __func__, attr.c_str());
    if (ret < 0) {
      CLS_LOG(0, "ERROR: %s(): cls_cxx_setxattr (attr=%s) returned %d", __func__, attr.c_str(), ret);
      return ret;
    }
  }

  return 0;
}

static int rgw_obj_store_pg_ver(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_store_pg_ver_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  bufferlist bl;
  uint64_t ver = cls_current_version(hctx);
  encode(ver, bl);
  int ret = cls_cxx_setxattr(hctx, op.attr.c_str(), &bl);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_setxattr (attr=%s) returned %d", __func__, op.attr.c_str(), ret);
    return ret;
  }

  return 0;
}

static int rgw_obj_check_attrs_prefix(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_check_attrs_prefix op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  if (op.check_prefix.empty()) {
    return -EINVAL;
  }

  map<string, bufferlist> attrset;
  int ret = cls_cxx_getxattrs(hctx, &attrset);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_getxattrs() returned %d", __func__, ret);
    return ret;
  }

  bool exist = false;

  for (map<string, bufferlist>::iterator aiter = attrset.lower_bound(op.check_prefix);
       aiter != attrset.end(); ++aiter) {
    const string& attr = aiter->first;

    if (attr.substr(0, op.check_prefix.size()) > op.check_prefix) {
      break;
    }

    exist = true;
  }

  if (exist == op.fail_if_exist) {
    return -ECANCELED;
  }

  return 0;
}

static int rgw_obj_check_mtime(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_obj_check_mtime op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  real_time obj_ut;
  int ret = cls_cxx_stat2(hctx, NULL, &obj_ut);
  if (ret < 0 && ret != -ENOENT) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_stat() returned %d", __func__, ret);
    return ret;
  }
  if (ret == -ENOENT) {
    CLS_LOG(10, "object does not exist, skipping check");
  }

  ceph_timespec obj_ts = ceph::real_clock::to_ceph_timespec(obj_ut);
  ceph_timespec op_ts = ceph::real_clock::to_ceph_timespec(op.mtime);

  if (!op.high_precision_time) {
    obj_ts.tv_nsec = 0;
    op_ts.tv_nsec = 0;
  }

  CLS_LOG(10, "%s: obj_ut=%lld.%06lld op.mtime=%lld.%06lld", __func__,
          (long long)obj_ts.tv_sec, (long long)obj_ts.tv_nsec,
          (long long)op_ts.tv_sec, (long long)op_ts.tv_nsec);

  bool check;

  switch (op.type) {
  case CLS_RGW_CHECK_TIME_MTIME_EQ:
    check = (obj_ts == op_ts);
    break;
  case CLS_RGW_CHECK_TIME_MTIME_LT:
    check = (obj_ts < op_ts);
    break;
  case CLS_RGW_CHECK_TIME_MTIME_LE:
    check = (obj_ts <= op_ts);
    break;
  case CLS_RGW_CHECK_TIME_MTIME_GT:
    check = (obj_ts > op_ts);
    break;
  case CLS_RGW_CHECK_TIME_MTIME_GE:
    check = (obj_ts >= op_ts);
    break;
  default:
    return -EINVAL;
  };

  if (!check) {
    return -ECANCELED;
  }

  return 0;
}

static int rgw_bi_get_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_bi_get_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  string idx;

  switch (op.type) {
    case BIIndexType::Plain:
      idx = op.key.name;
      break;
    case BIIndexType::Instance:
      encode_obj_index_key(op.key, &idx);
      break;
    case BIIndexType::OLH:
      encode_olh_data_key(op.key, &idx);
      break;
    default:
      CLS_LOG(10, "%s(): invalid key type encoding: %d",
	      __func__, int(op.type));
      return -EINVAL;
  }

  rgw_cls_bi_get_ret op_ret;

  rgw_cls_bi_entry& entry = op_ret.entry;

  entry.type = op.type;
  entry.idx = idx;

  int r = cls_cxx_map_get_val(hctx, idx, &entry.data);
  if (r < 0) {
      CLS_LOG(10, "%s(): cls_cxx_map_get_val() returned %d", __func__, r);
      return r;
  }

  encode(op_ret, *out);

  return 0;
}

static int rgw_bi_put_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_bi_put_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  rgw_cls_bi_entry& entry = op.entry;

  int r = cls_cxx_map_set_val(hctx, entry.idx, &entry.data);
  if (r < 0) {
    CLS_LOG(0, "ERROR: %s(): cls_cxx_map_set_val() returned r=%d", __func__, r);
  }

  return 0;
}

static int list_plain_entries(cls_method_context_t hctx, const string& name, const string& marker, uint32_t max,
                              list<rgw_cls_bi_entry> *entries, bool *pmore)
{
  string filter = name;
  string start_key = marker;

  string end_key; // stop listing at bi_log_prefix
  bi_log_prefix(end_key);

  int count = 0;
  map<string, bufferlist> keys;
  int ret = cls_cxx_map_get_vals(hctx, start_key, filter, max, &keys, pmore);
  if (ret < 0) {
    return ret;
  }

  map<string, bufferlist>::iterator iter;
  for (iter = keys.begin(); iter != keys.end(); ++iter) {
    if (iter->first >= end_key) {
      /* past the end of plain namespace */
      if (pmore) {
	*pmore = false;
      }
      return count;
    }

    rgw_cls_bi_entry entry;
    entry.type = BIIndexType::Plain;
    entry.idx = iter->first;
    entry.data = iter->second;

    auto biter = entry.data.cbegin();

    rgw_bucket_dir_entry e;
    try {
      decode(e, biter);
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR: %s(): failed to decode buffer", __func__);
      return -EIO;
    }

    CLS_LOG(20, "%s(): entry.idx=%s e.key.name=%s", __func__, escape_str(entry.idx).c_str(), escape_str(e.key.name).c_str());

    if (!name.empty() && e.key.name != name) {
      /* we are skipping the rest of the entries */
      if (pmore) {
	*pmore = false;
      }
      return count;
    }

    entries->push_back(entry);
    count++;
    if (count >= (int)max) {
      return count;
    }
    start_key = entry.idx;
  }

  return count;
}

static int list_instance_entries(cls_method_context_t hctx, const string& name, const string& marker, uint32_t max,
                                 list<rgw_cls_bi_entry> *entries, bool *pmore)
{
  cls_rgw_obj_key key(name);
  string first_instance_idx;
  encode_obj_versioned_data_key(key, &first_instance_idx);
  string start_key;

  if (!name.empty()) {
    start_key = first_instance_idx;
  } else {
    start_key = BI_PREFIX_CHAR;
    start_key.append(bucket_index_prefixes[BI_BUCKET_OBJ_INSTANCE_INDEX]);
  }
  string filter = start_key;
  if (bi_entry_gt(marker, start_key)) {
    start_key = marker;
  }
  int count = 0;
  map<string, bufferlist> keys;
  bufferlist k;
  int ret = cls_cxx_map_get_val(hctx, start_key, &k);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  bool found_first = (ret == 0);
  if (found_first) {
    --max;
  }
  if (max > 0) {
    ret = cls_cxx_map_get_vals(hctx, start_key, string(), max, &keys, pmore);
    CLS_LOG(20, "%s(): start_key=%s first_instance_idx=%s keys.size()=%d", __func__, escape_str(start_key).c_str(), escape_str(first_instance_idx).c_str(), (int)keys.size());
    if (ret < 0) {
      return ret;
    }
  }
  if (found_first) {
    keys[start_key].claim(k);
  }

  map<string, bufferlist>::iterator iter;
  for (iter = keys.begin(); iter != keys.end(); ++iter) {
    rgw_cls_bi_entry entry;
    entry.type = BIIndexType::Instance;
    entry.idx = iter->first;
    entry.data = iter->second;

    if (!filter.empty() && entry.idx.compare(0, filter.size(), filter) != 0) {
      /* we are skipping the rest of the entries */
      if (pmore) {
	*pmore = false;
      }
      return count;
    }

    CLS_LOG(20, "%s(): entry.idx=%s", __func__, escape_str(entry.idx).c_str());

    auto biter = entry.data.cbegin();

    rgw_bucket_dir_entry e;
    try {
      decode(e, biter);
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR: %s(): failed to decode buffer (size=%d)", __func__, entry.data.length());
      return -EIO;
    }

    if (!name.empty() && e.key.name != name) {
      /* we are skipping the rest of the entries */
      if (pmore) {
	*pmore = false;
      }
      return count;
    }

    entries->push_back(entry);
    count++;
    start_key = entry.idx;
  }

  return count;
}

static int list_olh_entries(cls_method_context_t hctx, const string& name, const string& marker, uint32_t max,
                            list<rgw_cls_bi_entry> *entries, bool *pmore)
{
  cls_rgw_obj_key key(name);
  string first_instance_idx;
  encode_olh_data_key(key, &first_instance_idx);
  string start_key;

  if (!name.empty()) {
    start_key = first_instance_idx;
  } else {
    start_key = BI_PREFIX_CHAR;
    start_key.append(bucket_index_prefixes[BI_BUCKET_OLH_DATA_INDEX]);
  }
  string filter = start_key;
  if (bi_entry_gt(marker, start_key)) {
    start_key = marker;
  }
  int count = 0;
  map<string, bufferlist> keys;
  int ret;
  bufferlist k;
  ret = cls_cxx_map_get_val(hctx, start_key, &k);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  bool found_first = (ret == 0);
  if (found_first) {
    --max;
  }
  if (max > 0) {
    ret = cls_cxx_map_get_vals(hctx, start_key, string(), max, &keys, pmore);
    CLS_LOG(20, "%s(): start_key=%s first_instance_idx=%s keys.size()=%d", __func__, escape_str(start_key).c_str(), escape_str(first_instance_idx).c_str(), (int)keys.size());
    if (ret < 0) {
      return ret;
    }
  }

  if (found_first) {
    keys[start_key].claim(k);
  }

  map<string, bufferlist>::iterator iter;
  for (iter = keys.begin(); iter != keys.end(); ++iter) {
    rgw_cls_bi_entry entry;
    entry.type = BIIndexType::OLH;
    entry.idx = iter->first;
    entry.data = iter->second;

    if (!filter.empty() && entry.idx.compare(0, filter.size(), filter) != 0) {
      /* we are skipping the rest of the entries */
      if (pmore) {
	*pmore = false;
      }
      return count;
    }

    CLS_LOG(20, "%s(): entry.idx=%s", __func__, escape_str(entry.idx).c_str());

    auto biter = entry.data.cbegin();

    rgw_bucket_olh_entry e;
    try {
      decode(e, biter);
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR: %s(): failed to decode buffer (size=%d)", __func__, entry.data.length());
      return -EIO;
    }

    if (!name.empty() && e.key.name != name) {
      /* we are skipping the rest of the entries */
      if (pmore) {
	*pmore = false;
      }
      return count;
    }

    entries->push_back(entry);
    count++;
    start_key = entry.idx;
  }

  return count;
}

static int rgw_bi_list_op(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // decode request
  rgw_cls_bi_list_op op;
  auto iter = in->cbegin();
  try {
    decode(op, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: %s(): failed to decode request", __func__);
    return -EINVAL;
  }

  rgw_cls_bi_list_ret op_ret;

  string filter = op.name;
#define MAX_BI_LIST_ENTRIES 1000
  int32_t max = (op.max < MAX_BI_LIST_ENTRIES ? op.max : MAX_BI_LIST_ENTRIES);
  string start_key = op.marker;
  bool more;
  int ret = list_plain_entries(hctx, op.name, op.marker, max, &op_ret.entries, &more);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: %s(): list_plain_entries returned ret=%d", __func__, ret);
    return ret;
  }
  int count = ret;

  CLS_LOG(20, "found %d plain entries", count);

  if (!more) {
    ret = list_instance_entries(hctx, op.name, op.marker, max - count, &op_ret.entries, &more);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: %s(): list_instance_entries returned ret=%d", __func__, ret);
      return ret;
    }

    count += ret;
  }

  if (!more) {
    ret = list_olh_entries(hctx, op.name, op.marker, max - count, &op_ret.entries, &more);
    if (ret < 0) {
      CLS_LOG(0, "ERROR: %s(): list_olh_entries returned ret=%d", __func__, ret);
      return ret;
    }

    count += ret;
  }

  op_ret.is_truncated = (count >= max) || more;
  while (count > max) {
    op_ret.entries.pop_back();
    count--;
  }

  encode(op_ret, *out);

  return 0;
}

int bi_log_record_decode(bufferlist& bl, rgw_bi_log_entry& e)
{
  auto iter = bl.cbegin();
  try {
    decode(e, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode rgw_bi_log_entry");
    return -EIO;
  }
  return 0;
}

static int bi_log_iterate_entries(cls_method_context_t hctx, const string& marker, const string& end_marker,
                              string& key_iter, uint32_t max_entries, bool *truncated,
                              int (*cb)(cls_method_context_t, const string&, rgw_bi_log_entry&, void *),
                              void *param)
{
  CLS_LOG(10, "bi_log_iterate_range");

  map<string, bufferlist> keys;
  string filter_prefix, end_key;
  uint32_t i = 0;
  string key;

  if (truncated)
    *truncated = false;

  string start_key;
  if (key_iter.empty()) {
    key = BI_PREFIX_CHAR;
    key.append(bucket_index_prefixes[BI_BUCKET_LOG_INDEX]);
    key.append(marker);

    start_key = key;
  } else {
    start_key = key_iter;
  }

  if (end_marker.empty()) {
    end_key = BI_PREFIX_CHAR;
    end_key.append(bucket_index_prefixes[BI_BUCKET_LOG_INDEX + 1]);
  } else {
    end_key = BI_PREFIX_CHAR;
    end_key.append(bucket_index_prefixes[BI_BUCKET_LOG_INDEX]);
    end_key.append(end_marker);
  }

  CLS_LOG(10, "bi_log_iterate_entries start_key=%s end_key=%s\n", start_key.c_str(), end_key.c_str());

  string filter;

  int ret = cls_cxx_map_get_vals(hctx, start_key, filter, max_entries, &keys, truncated);
  if (ret < 0)
    return ret;

  map<string, bufferlist>::iterator iter = keys.begin();
  if (iter == keys.end())
    return 0;

  uint32_t num_keys = keys.size();

  for (; iter != keys.end(); ++iter,++i) {
    const string& key = iter->first;
    rgw_bi_log_entry e;

    CLS_LOG(10, "bi_log_iterate_entries key=%s bl.length=%d\n", key.c_str(), (int)iter->second.length());

    if (key.compare(end_key) > 0) {
      key_iter = key;
      if (truncated) {
        *truncated = false;
      }
      return 0;
    }

    ret = bi_log_record_decode(iter->second, e);
    if (ret < 0)
      return ret;

    ret = cb(hctx, key, e, param);
    if (ret < 0)
      return ret;

    if (i == num_keys - 1) {
      key_iter = key;
    }
  }

  return 0;
}

static int bi_log_list_cb(cls_method_context_t hctx, const string& key, rgw_bi_log_entry& info, void *param)
{
  list<rgw_bi_log_entry> *l = (list<rgw_bi_log_entry> *)param;
  l->push_back(info);
  return 0;
}

static int bi_log_list_entries(cls_method_context_t hctx, const string& marker,
			   uint32_t max, list<rgw_bi_log_entry>& entries, bool *truncated)
{
  string key_iter;
  string end_marker;
  int ret = bi_log_iterate_entries(hctx, marker, end_marker,
                              key_iter, max, truncated,
                              bi_log_list_cb, &entries);
  return ret;
}

static int rgw_bi_log_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_bi_log_list_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bi_log_list(): failed to decode entry\n");
    return -EINVAL;
  }

  cls_rgw_bi_log_list_ret op_ret;
  int ret = bi_log_list_entries(hctx, op.marker, op.max, op_ret.entries, &op_ret.truncated);
  if (ret < 0)
    return ret;

  encode(op_ret, *out);

  return 0;
}

static int bi_log_list_trim_cb(cls_method_context_t hctx, const string& key, rgw_bi_log_entry& info, void *param)
{
  list<rgw_bi_log_entry> *entries = (list<rgw_bi_log_entry> *)param;

  entries->push_back(info);
  return 0;
}

static int bi_log_remove_entry(cls_method_context_t hctx, rgw_bi_log_entry& entry)
{
  string key;
  key = BI_PREFIX_CHAR;
  key.append(bucket_index_prefixes[BI_BUCKET_LOG_INDEX]);
  key.append(entry.id);
  return cls_cxx_map_remove_key(hctx, key);
}

static int bi_log_list_trim_entries(cls_method_context_t hctx,
                                    const string& start_marker, const string& end_marker,
			            list<rgw_bi_log_entry>& entries, bool *truncated)
{
  string key_iter;
#define MAX_TRIM_ENTRIES 1000 /* max entries to trim in a single operation */
  int ret = bi_log_iterate_entries(hctx, start_marker, end_marker,
                              key_iter, MAX_TRIM_ENTRIES, truncated,
                              bi_log_list_trim_cb, &entries);
  return ret;
}

static int rgw_bi_log_trim(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_bi_log_trim_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_bi_log_list(): failed to decode entry\n");
    return -EINVAL;
  }

  cls_rgw_bi_log_list_ret op_ret;
  list<rgw_bi_log_entry> entries;
#define MAX_TRIM_ENTRIES 1000 /* don't do more than that in a single operation */
  bool truncated;
  int ret = bi_log_list_trim_entries(hctx, op.start_marker, op.end_marker, entries, &truncated);
  if (ret < 0)
    return ret;

  if (entries.empty())
    return -ENODATA;

  list<rgw_bi_log_entry>::iterator iter;
  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    rgw_bi_log_entry& entry = *iter;

    ret = bi_log_remove_entry(hctx, entry);
    if (ret < 0)
      return ret;
  }

  return 0;
}

static int rgw_bi_log_resync(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: rgw_bucket_complete_op(): failed to read header\n");
    return rc;
  }

  bufferlist bl;

  rgw_bi_log_entry entry;

  entry.timestamp = real_clock::now();
  entry.op = RGWModifyOp::CLS_RGW_OP_RESYNC;
  entry.state = RGWPendingState::CLS_RGW_STATE_COMPLETE;

  string key;
  bi_log_index_key(hctx, key, entry.id, header.ver);

  encode(entry, bl);

  if (entry.id > header.max_marker)
    header.max_marker = entry.id;

  header.syncstopped = false;

  rc = cls_cxx_map_set_val(hctx, key, &bl);
  if (rc < 0)
    return rc;

  return write_bucket_header(hctx, &header);
}

static int rgw_bi_log_stop(cls_method_context_t hctx, bufferlist *in, bufferlist *out) 
{
  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: rgw_bucket_complete_op(): failed to read header\n");
    return rc;
  }

  bufferlist bl;

  rgw_bi_log_entry entry;

  entry.timestamp = real_clock::now();
  entry.op = RGWModifyOp::CLS_RGW_OP_SYNCSTOP;
  entry.state = RGWPendingState::CLS_RGW_STATE_COMPLETE;

  string key;
  bi_log_index_key(hctx, key, entry.id, header.ver);

  encode(entry, bl);

  if (entry.id > header.max_marker)
    header.max_marker = entry.id;
  header.syncstopped = true;

  rc = cls_cxx_map_set_val(hctx, key, &bl);
  if (rc < 0)
    return rc;

  return write_bucket_header(hctx, &header);
}


static void usage_record_prefix_by_time(uint64_t epoch, string& key)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%011llu", (long long unsigned)epoch);
  key = buf;
}

static void usage_record_prefix_by_user(const string& user, uint64_t epoch, string& key)
{
  char buf[user.size() + 32];
  snprintf(buf, sizeof(buf), "%s_%011llu_", user.c_str(), (long long unsigned)epoch);
  key = buf;
}

static void usage_record_name_by_time(uint64_t epoch, const string& user, const string& bucket, string& key)
{
  char buf[32 + user.size() + bucket.size()];
  snprintf(buf, sizeof(buf), "%011llu_%s_%s", (long long unsigned)epoch, user.c_str(), bucket.c_str());
  key = buf;
}

static void usage_record_name_by_user(const string& user, uint64_t epoch, const string& bucket, string& key)
{
  char buf[32 + user.size() + bucket.size()];
  snprintf(buf, sizeof(buf), "%s_%011llu_%s", user.c_str(), (long long unsigned)epoch, bucket.c_str());
  key = buf;
}

static int usage_record_decode(bufferlist& record_bl, rgw_usage_log_entry& e)
{
  auto kiter = record_bl.cbegin();
  try {
    decode(e, kiter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: usage_record_decode(): failed to decode record_bl\n");
    return -EINVAL;
  }

  return 0;
}

int rgw_user_usage_log_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "rgw_user_usage_log_add()");

  auto in_iter = in->cbegin();
  rgw_cls_usage_log_add_op op;

  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_user_usage_log_add(): failed to decode request\n");
    return -EINVAL;
  }

  rgw_usage_log_info& info = op.info;
  vector<rgw_usage_log_entry>::iterator iter;

  for (iter = info.entries.begin(); iter != info.entries.end(); ++iter) {
    rgw_usage_log_entry& entry = *iter;
    string key_by_time;

    rgw_user *puser = (entry.payer.empty() ? &entry.owner : &entry.payer);

    usage_record_name_by_time(entry.epoch, puser->to_str(), entry.bucket, key_by_time);

    CLS_LOG(10, "rgw_user_usage_log_add user=%s bucket=%s\n", puser->to_str().c_str(), entry.bucket.c_str());

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
    encode(entry, new_record_bl);
    ret = cls_cxx_map_set_val(hctx, key_by_time, &new_record_bl);
    if (ret < 0)
      return ret;

    string key_by_user;
    usage_record_name_by_user(puser->to_str(), entry.epoch, entry.bucket, key_by_user);
    ret = cls_cxx_map_set_val(hctx, key_by_user, &new_record_bl);
    if (ret < 0)
      return ret;
  }

  return 0;
}

static int usage_iterate_range(cls_method_context_t hctx, uint64_t start, uint64_t end, const string& user,
                            const string& bucket, string& key_iter, uint32_t max_entries, bool *truncated,
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
  bool truncated_status = false;

  ceph_assert(truncated != nullptr);

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

  CLS_LOG(20, "usage_iterate_range start_key=%s", start_key.c_str());
  int ret = cls_cxx_map_get_vals(hctx, start_key, filter_prefix, max_entries, &keys, &truncated_status);
  if (ret < 0)
    return ret;

  *truncated = truncated_status;
      
  map<string, bufferlist>::iterator iter = keys.begin();
  if (iter == keys.end())
    return 0;

  uint32_t num_keys = keys.size();

  for (; iter != keys.end(); ++iter,++i) {
    const string& key = iter->first;
    rgw_usage_log_entry e;

    if (!by_user && key.compare(end_key) >= 0) {
      CLS_LOG(20, "usage_iterate_range reached key=%s, done", key.c_str());
      *truncated = false;
      key_iter = key;
      return 0;
    }

    if (by_user && key.compare(0, user_key.size(), user_key) != 0) {
      CLS_LOG(20, "usage_iterate_range reached key=%s, done", key.c_str());
      *truncated = false;
      key_iter = key;
      return 0;
    }

    ret = usage_record_decode(iter->second, e);
    if (ret < 0)
      return ret;

    if (!bucket.empty() && bucket.compare(e.bucket))
      continue;

    if (e.epoch < start)
      continue;

    /* keys are sorted by epoch, so once we're past end we're done */
    if (e.epoch >= end) {
      *truncated = false;
      return 0;
    }

    ret = cb(hctx, key, e, param);
    if (ret < 0)
      return ret;


    if (i == num_keys - 1) {
      key_iter = key;
      return 0;
    }
  }
  return 0;
}

static int usage_log_read_cb(cls_method_context_t hctx, const string& key, rgw_usage_log_entry& entry, void *param)
{
  map<rgw_user_bucket, rgw_usage_log_entry> *usage = (map<rgw_user_bucket, rgw_usage_log_entry> *)param;
  rgw_user *puser;
  if (!entry.payer.empty()) {
    puser = &entry.payer;
  } else {
    puser = &entry.owner;
  }
  rgw_user_bucket ub(puser->to_str(), entry.bucket);
  rgw_usage_log_entry& le = (*usage)[ub];
  le.aggregate(entry);

  return 0;
}

int rgw_user_usage_log_read(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10, "rgw_user_usage_log_read()");

  auto in_iter = in->cbegin();
  rgw_cls_usage_log_read_op op;

  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_user_usage_log_read(): failed to decode request\n");
    return -EINVAL;
  }

  rgw_cls_usage_log_read_ret ret_info;
  map<rgw_user_bucket, rgw_usage_log_entry> *usage = &ret_info.usage;
  string iter = op.iter;
#define MAX_ENTRIES 1000
  uint32_t max_entries = (op.max_entries ? op.max_entries : MAX_ENTRIES);
  int ret = usage_iterate_range(hctx, op.start_epoch, op.end_epoch, op.owner, op.bucket, iter, max_entries, &ret_info.truncated, usage_log_read_cb, (void *)usage);
  if (ret < 0)
    return ret;

  if (ret_info.truncated)
    ret_info.next_iter = iter;

  encode(ret_info, *out);
  return 0;
}

static int usage_log_trim_cb(cls_method_context_t hctx, const string& key, rgw_usage_log_entry& entry, void *param)
{
  bool *found = (bool *)param;
  if (found) {
    *found = true;
  }
  string key_by_time;
  string key_by_user;

  string o = entry.owner.to_str();
  usage_record_name_by_time(entry.epoch, o, entry.bucket, key_by_time);
  usage_record_name_by_user(o, entry.epoch, entry.bucket, key_by_user);

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

  auto in_iter = in->cbegin();
  rgw_cls_usage_log_trim_op op;

  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_user_log_usage_log_trim(): failed to decode request\n");
    return -EINVAL;
  }

  string iter;
  bool more;
  bool found = false;
#define MAX_USAGE_TRIM_ENTRIES 128
  ret = usage_iterate_range(hctx, op.start_epoch, op.end_epoch, op.user, op.bucket, iter, MAX_USAGE_TRIM_ENTRIES, &more, usage_log_trim_cb, (void *)&found);
  if (ret < 0)
    return ret;

  if (!more && !found)
    return -ENODATA;

  return 0;
}

int rgw_usage_log_clear(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(10,"%s", __func__);

  int ret = cls_cxx_map_clear(hctx);
  /* if object doesn't exist all the logs are cleared anyway */
  if (ret == -ENOENT)
    ret = 0;

  return ret;
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

  int ret = read_omap_entry(hctx, index, info);
  if (ret < 0)
    return ret;

  return 0;
}

static int gc_omap_set(cls_method_context_t hctx, int type, const string& key, const cls_rgw_gc_obj_info *info)
{
  bufferlist bl;
  encode(*info, bl);

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

  int ret = cls_cxx_map_remove_key(hctx, index);
  if (ret < 0)
    return ret;

  return 0;
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

  // calculate time and time key
  info.time = ceph::real_clock::now();
  info.time += make_timespan(expiration_secs);
  string time_key;
  get_time_key(info.time, &time_key);

  if (info.chain.objs.empty()) {
    CLS_LOG(0,
	    "WARNING: %s setting GC log entry with zero-length chain, "
	    "tag='%s', timekey='%s'",
	    __func__, info.tag.c_str(), time_key.c_str());
  }

  ret = gc_omap_set(hctx, GC_OBJ_NAME_INDEX, info.tag, &info);
  if (ret < 0)
    return ret;

  ret = gc_omap_set(hctx, GC_OBJ_TIME_INDEX, time_key, &info);
  if (ret < 0)
    goto done_err;

  return 0;

done_err:

  CLS_LOG(0, "ERROR: gc_set_entry error info.tag=%s, ret=%d\n",
	  info.tag.c_str(), ret);
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
  auto iter = bl.cbegin();
  try {
    decode(e, iter);
  } catch (buffer::error& err) {
    CLS_LOG(0, "ERROR: failed to decode cls_rgw_gc_obj_info");
    return -EIO;
  }
  return 0;
}

static int rgw_cls_gc_set_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_gc_set_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_gc_set_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  return gc_update_entry(hctx, op.expiration_secs, op.info);
}

static int rgw_cls_gc_defer_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_gc_defer_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_gc_defer_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  return gc_defer_entry(hctx, op.tag, op.expiration_secs);
}

static int gc_iterate_entries(cls_method_context_t hctx,
			      const string& marker,
			      bool expired_only,
                              string& out_marker,
			      uint32_t max_entries,
			      bool *truncated,
                              int (*cb)(cls_method_context_t,
					const string&,
					cls_rgw_gc_obj_info&,
					void *),
                              void *param)
{
  CLS_LOG(10, "gc_iterate_entries");

  map<string, bufferlist> keys;
  string filter_prefix, end_key;
  string key;

  if (truncated)
    *truncated = false;

  string start_key;
  if (marker.empty()) {
    prepend_index_prefix(marker, GC_OBJ_TIME_INDEX, &start_key);
  } else {
    start_key = marker;
  }

  if (expired_only) {
    real_time now = ceph::real_clock::now();
    string now_str;
    get_time_key(now, &now_str);
    prepend_index_prefix(now_str, GC_OBJ_TIME_INDEX, &end_key);

    CLS_LOG(10, "gc_iterate_entries end_key=%s\n", end_key.c_str());
  }

  string filter;

  int ret = cls_cxx_map_get_vals(hctx, start_key, filter, max_entries,
				 &keys, truncated);
  if (ret < 0)
    return ret;

  map<string, bufferlist>::iterator iter = keys.begin();
  if (iter == keys.end()) {
    // if keys empty must not come back as truncated
    ceph_assert(!truncated || !(*truncated));
    return 0;
  }

  const string* last_key = nullptr; // last key processed, for end-marker
  for (; iter != keys.end(); ++iter) {
    const string& key = iter->first;
    cls_rgw_gc_obj_info e;

    CLS_LOG(10, "gc_iterate_entries key=%s\n", key.c_str());

    if (!end_key.empty() && key.compare(end_key) >= 0) {
      if (truncated)
        *truncated = false;
      return 0;
    }

    if (!key_in_index(key, GC_OBJ_TIME_INDEX)) {
      if (truncated)
	*truncated = false;
      return 0;
    }

    ret = gc_record_decode(iter->second, e);
    if (ret < 0)
      return ret;

    ret = cb(hctx, key, e, param);
    if (ret < 0)
      return ret;
    last_key = &(iter->first); // update when callback successful
  }

  // set the out marker if either caller does not capture truncated or
  // if they do capture and we are truncated
  if (!truncated || *truncated) {
    assert(last_key);
    out_marker = *last_key;
  }

  return 0;
}

static int gc_list_cb(cls_method_context_t hctx, const string& key, cls_rgw_gc_obj_info& info, void *param)
{
  list<cls_rgw_gc_obj_info> *l = (list<cls_rgw_gc_obj_info> *)param;
  l->push_back(info);
  return 0;
}

static int gc_list_entries(cls_method_context_t hctx, const string& marker,
			   uint32_t max, bool expired_only,
                           list<cls_rgw_gc_obj_info>& entries, bool *truncated, string& next_marker)
{
  int ret = gc_iterate_entries(hctx, marker, expired_only,
                              next_marker, max, truncated,
                              gc_list_cb, &entries);
  return ret;
}

static int rgw_cls_gc_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_gc_list_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_gc_list(): failed to decode entry\n");
    return -EINVAL;
  }

  cls_rgw_gc_list_ret op_ret;
#define GC_LIST_ENTRIES_DEFAULT 128
  int ret = gc_list_entries(hctx, op.marker, (op.max ? op.max : GC_LIST_ENTRIES_DEFAULT), op.expired_only, 
   op_ret.entries, &op_ret.truncated, op_ret.next_marker);
  if (ret < 0)
    return ret;

  encode(op_ret, *out);

  return 0;
}

static int gc_remove(cls_method_context_t hctx, vector<string>& tags)
{
  for (auto iter = tags.begin(); iter != tags.end(); ++iter) {
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
  auto in_iter = in->cbegin();

  cls_rgw_gc_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_gc_remove(): failed to decode entry\n");
    return -EINVAL;
  }

  return gc_remove(hctx, op.tags);
}

static int rgw_cls_lc_get_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_lc_get_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_lc_set_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  rgw_lc_entry_t lc_entry;
  int ret = read_omap_entry(hctx, op.marker, &lc_entry);
  if (ret < 0)
    return ret;

  cls_rgw_lc_get_entry_ret op_ret(std::move(lc_entry));
  encode(op_ret, *out);
  return 0;
}


static int rgw_cls_lc_set_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_lc_set_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_lc_set_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  bufferlist bl;
  encode(op.entry, bl);

  int ret = cls_cxx_map_set_val(hctx, op.entry.first, &bl);
  return ret;
}

static int rgw_cls_lc_rm_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_lc_rm_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_lc_rm_entry(): failed to decode entry\n");
    return -EINVAL;
  }

  int ret = cls_cxx_map_remove_key(hctx, op.entry.first);
  return ret;
}

static int rgw_cls_lc_get_next_entry(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();
  cls_rgw_lc_get_next_entry_ret op_ret;
  cls_rgw_lc_get_next_entry_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_lc_get_next_entry: failed to decode op\n");
    return -EINVAL;
  }

  map<string, bufferlist> vals;
  string filter_prefix;
  bool more;
  int ret = cls_cxx_map_get_vals(hctx, op.marker, filter_prefix, 1, &vals, &more);
  if (ret < 0)
    return ret;
  map<string, bufferlist>::iterator it;
  pair<string, int> entry;
  if (!vals.empty()) {
    it=vals.begin();
    in_iter = it->second.begin();
    try {
      decode(entry, in_iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: rgw_cls_lc_get_next_entry(): failed to decode entry\n");
      return -EIO;
    }
  }
  op_ret.entry = entry;
  encode(op_ret, *out);
  return 0;
}

static int rgw_cls_lc_list_entries(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_rgw_lc_list_entries_op op;
  auto in_iter = in->cbegin();
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_lc_list_entries(): failed to decode op\n");
    return -EINVAL;
  }

  cls_rgw_lc_list_entries_ret op_ret;
  bufferlist::const_iterator iter;
  map<string, bufferlist> vals;
  string filter_prefix;
  int ret = cls_cxx_map_get_vals(hctx, op.marker, filter_prefix, op.max_entries, &vals, &op_ret.is_truncated);
  if (ret < 0)
    return ret;
  map<string, bufferlist>::iterator it;
  pair<string, int> entry;
  for (it = vals.begin(); it != vals.end(); ++it) {
    iter = it->second.cbegin();
    try {
    decode(entry, iter);
    } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_lc_list_entries(): failed to decode entry\n");
    return -EIO;
   }
   op_ret.entries.insert(entry);
  }
  encode(op_ret, *out);
  return 0;
}

static int rgw_cls_lc_put_head(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_lc_put_head_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_lc_put_head(): failed to decode entry\n");
    return -EINVAL;
  }

  bufferlist bl;
  encode(op.head, bl);
  int ret = cls_cxx_map_write_header(hctx,&bl);
  return ret;
}

static int rgw_cls_lc_get_head(cls_method_context_t hctx, bufferlist *in,  bufferlist *out)
{
  bufferlist bl;
  int ret = cls_cxx_map_read_header(hctx, &bl);
  if (ret < 0)
    return ret;
  cls_rgw_lc_obj_head head;
  if (bl.length() != 0) {
    auto iter = bl.cbegin();
    try {
      decode(head, iter);
    } catch (buffer::error& err) {
      CLS_LOG(0, "ERROR: rgw_cls_lc_get_head(): failed to decode entry %s\n",err.what());
      return -EINVAL;
    }
  } else {
    head.start_date = 0;
    head.marker.clear();
  }
  cls_rgw_lc_get_head_ret op_ret;
  op_ret.head = head;
  encode(op_ret, *out);
  return 0;
}

static int rgw_reshard_add(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_reshard_add_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_reshard_add: failed to decode entry\n");
    return -EINVAL;
  }


  string key;
  op.entry.get_key(&key);

  bufferlist bl;
  encode(op.entry, bl);
  int ret = cls_cxx_map_set_val(hctx, key, &bl);
  if (ret < 0) {
    CLS_ERR("error adding reshard job for bucket %s with key %s",op.entry.bucket_name.c_str(), key.c_str());
    return ret;
  }

  return ret;
}

static int rgw_reshard_list(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  cls_rgw_reshard_list_op op;
  auto in_iter = in->cbegin();
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_rehard_list(): failed to decode entry\n");
    return -EINVAL;
  }
  cls_rgw_reshard_list_ret op_ret;
  bufferlist::const_iterator iter;
  map<string, bufferlist> vals;
  string filter_prefix;
#define MAX_RESHARD_LIST_ENTRIES 1000
  /* one extra entry for identifying truncation */
  int32_t max = (op.max && (op.max < MAX_RESHARD_LIST_ENTRIES) ? op.max : MAX_RESHARD_LIST_ENTRIES);
  int ret = cls_cxx_map_get_vals(hctx, op.marker, filter_prefix, max, &vals, &op_ret.is_truncated);
  if (ret < 0)
    return ret;
  map<string, bufferlist>::iterator it;
  cls_rgw_reshard_entry entry;
  int i = 0;
  for (it = vals.begin(); i < (int)op.max && it != vals.end(); ++it, ++i) {
    iter = it->second.cbegin();
    try {
      decode(entry, iter);
    } catch (buffer::error& err) {
      CLS_LOG(1, "ERROR: rgw_cls_rehard_list(): failed to decode entry\n");
      return -EIO;
   }
    op_ret.entries.push_back(entry);
  }
  encode(op_ret, *out);
  return 0;
}

static int rgw_reshard_get(cls_method_context_t hctx, bufferlist *in,  bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_reshard_get_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_reshard_get: failed to decode entry\n");
    return -EINVAL;
  }

  string key;
  cls_rgw_reshard_entry  entry;
  op.entry.get_key(&key);
  int ret = read_omap_entry(hctx, key, &entry);
  if (ret < 0) {
    return ret;
  }

  cls_rgw_reshard_get_ret op_ret;
  op_ret.entry = entry;
  encode(op_ret, *out);
  return 0;
}

static int rgw_reshard_remove(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  auto in_iter = in->cbegin();

  cls_rgw_reshard_remove_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: rgw_cls_rehard_remove: failed to decode entry\n");
    return -EINVAL;
  }

  string key;
  cls_rgw_reshard_entry  entry;
  cls_rgw_reshard_entry::generate_key(op.tenant, op.bucket_name, &key);
  int ret = read_omap_entry(hctx, key, &entry);
  if (ret < 0) {
    return ret;
  }

  if (!op.bucket_id.empty() &&
      entry.bucket_id != op.bucket_id) {
    return 0;
  }

  ret = cls_cxx_map_remove_key(hctx, key);
  if (ret < 0) {
    CLS_LOG(0, "ERROR: failed to remove key: key=%s ret=%d", key.c_str(), ret);
    return 0;
  }
  return ret;
}

static int rgw_set_bucket_resharding(cls_method_context_t hctx, bufferlist *in,  bufferlist *out)
{
  cls_rgw_set_bucket_resharding_op op;

  auto in_iter = in->cbegin();
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rgw_set_bucket_resharding: failed to decode entry\n");
    return -EINVAL;
  }

  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: %s(): failed to read header\n", __func__);
    return rc;
  }

  header.new_instance.set_status(op.entry.new_bucket_instance_id, op.entry.num_shards, op.entry.reshard_status);

  return write_bucket_header(hctx, &header);
}

static int rgw_clear_bucket_resharding(cls_method_context_t hctx, bufferlist *in,  bufferlist *out)
{
  cls_rgw_clear_bucket_resharding_op op;

  auto in_iter = in->cbegin();
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rgw_clear_bucket_resharding: failed to decode entry\n");
    return -EINVAL;
  }

  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: %s(): failed to read header\n", __func__);
    return rc;
  }
  header.new_instance.clear();

  return write_bucket_header(hctx, &header);
}

static int rgw_guard_bucket_resharding(cls_method_context_t hctx, bufferlist *in,  bufferlist *out)
{
  cls_rgw_guard_bucket_resharding_op op;

  auto in_iter = in->cbegin();
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rgw_clear_bucket_resharding: failed to decode entry\n");
    return -EINVAL;
  }

  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: %s(): failed to read header\n", __func__);
    return rc;
  }

  if (header.resharding()) {
    return op.ret_err;
  }

  return 0;
}

static int rgw_get_bucket_resharding(cls_method_context_t hctx,
				     bufferlist *in, bufferlist *out)
{
  cls_rgw_get_bucket_resharding_op op;

  auto in_iter = in->cbegin();
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_rgw_clear_bucket_resharding: failed to decode entry\n");
    return -EINVAL;
  }

  rgw_bucket_dir_header header;
  int rc = read_bucket_header(hctx, &header);
  if (rc < 0) {
    CLS_LOG(1, "ERROR: %s(): failed to read header\n", __func__);
    return rc;
  }

  cls_rgw_get_bucket_resharding_ret op_ret;
  op_ret.new_instance = header.new_instance;

  encode(op_ret, *out);

  return 0;
}

CLS_INIT(rgw)
{
  CLS_LOG(1, "Loaded rgw class!");

  cls_handle_t h_class;
  cls_method_handle_t h_rgw_bucket_init_index;
  cls_method_handle_t h_rgw_bucket_set_tag_timeout;
  cls_method_handle_t h_rgw_bucket_list;
  cls_method_handle_t h_rgw_bucket_check_index;
  cls_method_handle_t h_rgw_bucket_rebuild_index;
  cls_method_handle_t h_rgw_bucket_update_stats;
  cls_method_handle_t h_rgw_bucket_prepare_op;
  cls_method_handle_t h_rgw_bucket_complete_op;
  cls_method_handle_t h_rgw_bucket_link_olh;
  cls_method_handle_t h_rgw_bucket_unlink_instance_op;
  cls_method_handle_t h_rgw_bucket_read_olh_log;
  cls_method_handle_t h_rgw_bucket_trim_olh_log;
  cls_method_handle_t h_rgw_bucket_clear_olh;
  cls_method_handle_t h_rgw_obj_remove;
  cls_method_handle_t h_rgw_obj_store_pg_ver;
  cls_method_handle_t h_rgw_obj_check_attrs_prefix;
  cls_method_handle_t h_rgw_obj_check_mtime;
  cls_method_handle_t h_rgw_bi_get_op;
  cls_method_handle_t h_rgw_bi_put_op;
  cls_method_handle_t h_rgw_bi_list_op;
  cls_method_handle_t h_rgw_bi_log_list_op;
  cls_method_handle_t h_rgw_bi_log_resync_op;
  cls_method_handle_t h_rgw_bi_log_stop_op;
  cls_method_handle_t h_rgw_dir_suggest_changes;
  cls_method_handle_t h_rgw_user_usage_log_add;
  cls_method_handle_t h_rgw_user_usage_log_read;
  cls_method_handle_t h_rgw_user_usage_log_trim;
  cls_method_handle_t h_rgw_usage_log_clear;
  cls_method_handle_t h_rgw_gc_set_entry;
  cls_method_handle_t h_rgw_gc_list;
  cls_method_handle_t h_rgw_gc_remove;
  cls_method_handle_t h_rgw_lc_get_entry;
  cls_method_handle_t h_rgw_lc_set_entry;
  cls_method_handle_t h_rgw_lc_rm_entry;
  cls_method_handle_t h_rgw_lc_get_next_entry;
  cls_method_handle_t h_rgw_lc_put_head;
  cls_method_handle_t h_rgw_lc_get_head;
  cls_method_handle_t h_rgw_lc_list_entries;
  cls_method_handle_t h_rgw_reshard_add;
  cls_method_handle_t h_rgw_reshard_list;
  cls_method_handle_t h_rgw_reshard_get;
  cls_method_handle_t h_rgw_reshard_remove;
  cls_method_handle_t h_rgw_set_bucket_resharding;
  cls_method_handle_t h_rgw_clear_bucket_resharding;
  cls_method_handle_t h_rgw_guard_bucket_resharding;
  cls_method_handle_t h_rgw_get_bucket_resharding;


  cls_register(RGW_CLASS, &h_class);

  /* bucket index */
  cls_register_cxx_method(h_class, RGW_BUCKET_INIT_INDEX, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_init_index, &h_rgw_bucket_init_index);
  cls_register_cxx_method(h_class, RGW_BUCKET_SET_TAG_TIMEOUT, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_set_tag_timeout, &h_rgw_bucket_set_tag_timeout);
  cls_register_cxx_method(h_class, RGW_BUCKET_LIST, CLS_METHOD_RD, rgw_bucket_list, &h_rgw_bucket_list);
  cls_register_cxx_method(h_class, RGW_BUCKET_CHECK_INDEX, CLS_METHOD_RD, rgw_bucket_check_index, &h_rgw_bucket_check_index);
  cls_register_cxx_method(h_class, RGW_BUCKET_REBUILD_INDEX, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_rebuild_index, &h_rgw_bucket_rebuild_index);
  cls_register_cxx_method(h_class, RGW_BUCKET_UPDATE_STATS, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_update_stats, &h_rgw_bucket_update_stats);
  cls_register_cxx_method(h_class, RGW_BUCKET_PREPARE_OP, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_prepare_op, &h_rgw_bucket_prepare_op);
  cls_register_cxx_method(h_class, RGW_BUCKET_COMPLETE_OP, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_complete_op, &h_rgw_bucket_complete_op);
  cls_register_cxx_method(h_class, RGW_BUCKET_LINK_OLH, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_link_olh, &h_rgw_bucket_link_olh);
  cls_register_cxx_method(h_class, RGW_BUCKET_UNLINK_INSTANCE, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_unlink_instance, &h_rgw_bucket_unlink_instance_op);
  cls_register_cxx_method(h_class, RGW_BUCKET_READ_OLH_LOG, CLS_METHOD_RD, rgw_bucket_read_olh_log, &h_rgw_bucket_read_olh_log);
  cls_register_cxx_method(h_class, RGW_BUCKET_TRIM_OLH_LOG, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_trim_olh_log, &h_rgw_bucket_trim_olh_log);
  cls_register_cxx_method(h_class, RGW_BUCKET_CLEAR_OLH, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bucket_clear_olh, &h_rgw_bucket_clear_olh);

  cls_register_cxx_method(h_class, RGW_OBJ_REMOVE, CLS_METHOD_RD | CLS_METHOD_WR, rgw_obj_remove, &h_rgw_obj_remove);
  cls_register_cxx_method(h_class, RGW_OBJ_STORE_PG_VER, CLS_METHOD_WR, rgw_obj_store_pg_ver, &h_rgw_obj_store_pg_ver);
  cls_register_cxx_method(h_class, RGW_OBJ_CHECK_ATTRS_PREFIX, CLS_METHOD_RD, rgw_obj_check_attrs_prefix, &h_rgw_obj_check_attrs_prefix);
  cls_register_cxx_method(h_class, RGW_OBJ_CHECK_MTIME, CLS_METHOD_RD, rgw_obj_check_mtime, &h_rgw_obj_check_mtime);

  cls_register_cxx_method(h_class, RGW_BI_GET, CLS_METHOD_RD, rgw_bi_get_op, &h_rgw_bi_get_op);
  cls_register_cxx_method(h_class, RGW_BI_PUT, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bi_put_op, &h_rgw_bi_put_op);
  cls_register_cxx_method(h_class, RGW_BI_LIST, CLS_METHOD_RD, rgw_bi_list_op, &h_rgw_bi_list_op);

  cls_register_cxx_method(h_class, RGW_BI_LOG_LIST, CLS_METHOD_RD, rgw_bi_log_list, &h_rgw_bi_log_list_op);
  cls_register_cxx_method(h_class, RGW_BI_LOG_TRIM, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bi_log_trim, &h_rgw_bi_log_list_op);
  cls_register_cxx_method(h_class, RGW_DIR_SUGGEST_CHANGES, CLS_METHOD_RD | CLS_METHOD_WR, rgw_dir_suggest_changes, &h_rgw_dir_suggest_changes);

  cls_register_cxx_method(h_class, RGW_BI_LOG_RESYNC, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bi_log_resync, &h_rgw_bi_log_resync_op);
  cls_register_cxx_method(h_class, RGW_BI_LOG_STOP, CLS_METHOD_RD | CLS_METHOD_WR, rgw_bi_log_stop, &h_rgw_bi_log_stop_op);

  /* usage logging */
  cls_register_cxx_method(h_class, RGW_USER_USAGE_LOG_ADD, CLS_METHOD_RD | CLS_METHOD_WR, rgw_user_usage_log_add, &h_rgw_user_usage_log_add);
  cls_register_cxx_method(h_class, RGW_USER_USAGE_LOG_READ, CLS_METHOD_RD, rgw_user_usage_log_read, &h_rgw_user_usage_log_read);
  cls_register_cxx_method(h_class, RGW_USER_USAGE_LOG_TRIM, CLS_METHOD_RD | CLS_METHOD_WR, rgw_user_usage_log_trim, &h_rgw_user_usage_log_trim);
  cls_register_cxx_method(h_class, RGW_USAGE_LOG_CLEAR, CLS_METHOD_WR, rgw_usage_log_clear, &h_rgw_usage_log_clear);

  /* garbage collection */
  cls_register_cxx_method(h_class, RGW_GC_SET_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, rgw_cls_gc_set_entry, &h_rgw_gc_set_entry);
  cls_register_cxx_method(h_class, RGW_GC_DEFER_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, rgw_cls_gc_defer_entry, &h_rgw_gc_set_entry);
  cls_register_cxx_method(h_class, RGW_GC_LIST, CLS_METHOD_RD, rgw_cls_gc_list, &h_rgw_gc_list);
  cls_register_cxx_method(h_class, RGW_GC_REMOVE, CLS_METHOD_RD | CLS_METHOD_WR, rgw_cls_gc_remove, &h_rgw_gc_remove);

  /* lifecycle bucket list */
  cls_register_cxx_method(h_class, RGW_LC_GET_ENTRY, CLS_METHOD_RD, rgw_cls_lc_get_entry, &h_rgw_lc_get_entry);
  cls_register_cxx_method(h_class, RGW_LC_SET_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, rgw_cls_lc_set_entry, &h_rgw_lc_set_entry);
  cls_register_cxx_method(h_class, RGW_LC_RM_ENTRY, CLS_METHOD_RD | CLS_METHOD_WR, rgw_cls_lc_rm_entry, &h_rgw_lc_rm_entry);
  cls_register_cxx_method(h_class, RGW_LC_GET_NEXT_ENTRY, CLS_METHOD_RD, rgw_cls_lc_get_next_entry, &h_rgw_lc_get_next_entry);
  cls_register_cxx_method(h_class, RGW_LC_PUT_HEAD, CLS_METHOD_RD| CLS_METHOD_WR, rgw_cls_lc_put_head, &h_rgw_lc_put_head);
  cls_register_cxx_method(h_class, RGW_LC_GET_HEAD, CLS_METHOD_RD, rgw_cls_lc_get_head, &h_rgw_lc_get_head);
  cls_register_cxx_method(h_class, RGW_LC_LIST_ENTRIES, CLS_METHOD_RD, rgw_cls_lc_list_entries, &h_rgw_lc_list_entries);

  /* resharding */
  cls_register_cxx_method(h_class, RGW_RESHARD_ADD, CLS_METHOD_RD | CLS_METHOD_WR, rgw_reshard_add, &h_rgw_reshard_add);
  cls_register_cxx_method(h_class, RGW_RESHARD_LIST, CLS_METHOD_RD, rgw_reshard_list, &h_rgw_reshard_list);
  cls_register_cxx_method(h_class, RGW_RESHARD_GET, CLS_METHOD_RD,rgw_reshard_get, &h_rgw_reshard_get);
  cls_register_cxx_method(h_class, RGW_RESHARD_REMOVE, CLS_METHOD_RD | CLS_METHOD_WR, rgw_reshard_remove, &h_rgw_reshard_remove);

  /* resharding attribute  */
  cls_register_cxx_method(h_class, RGW_SET_BUCKET_RESHARDING, CLS_METHOD_RD | CLS_METHOD_WR,
			  rgw_set_bucket_resharding, &h_rgw_set_bucket_resharding);
  cls_register_cxx_method(h_class, RGW_CLEAR_BUCKET_RESHARDING, CLS_METHOD_RD | CLS_METHOD_WR,
			  rgw_clear_bucket_resharding, &h_rgw_clear_bucket_resharding);
  cls_register_cxx_method(h_class, RGW_GUARD_BUCKET_RESHARDING, CLS_METHOD_RD ,
			  rgw_guard_bucket_resharding, &h_rgw_guard_bucket_resharding);
  cls_register_cxx_method(h_class, RGW_GET_BUCKET_RESHARDING, CLS_METHOD_RD ,
			  rgw_get_bucket_resharding, &h_rgw_get_bucket_resharding);

  return;
}

