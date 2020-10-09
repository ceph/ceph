
#include "common/debug.h"
#include "common/Formatter.h"

#include "services/svc_bilog_rados.h"

#include "rgw_sip_bucket.h"
#include "rgw_bucket.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"


#define dout_subsys ceph_subsys_rgw

void siprovider_bucket_entry_info::dump(Formatter *f) const
{
  entry.dump(f);
}

void siprovider_bucket_entry_info::decode_json(JSONObj *obj)
{
  decode_json_obj(entry, obj);
}

static inline string escape_str(const string& s)
{
  return rgw_escape_str(s, '\\', ':');
}


std::string SIProvider_BucketFull::to_marker(const cls_rgw_obj_key& k) const
{
  return escape_str(k.name) + ":" + escape_str(k.instance);
}

SIProvider::Entry SIProvider_BucketFull::create_entry(rgw_bucket_dir_entry& be) const
{
  SIProvider::Entry e;
  e.key = to_marker(be.key);

  siprovider_bucket_entry_info be_info;

  auto& log_entry = be_info.entry;

  log_entry.id = e.key;
  log_entry.object = be.key.name;
  log_entry.instance = be.key.instance;
  log_entry.timestamp = be.meta.mtime;

#warning FIXME link olh, olh_dm
  log_entry.op = CLS_RGW_OP_ADD;
  log_entry.owner = be.meta.owner;
  log_entry.owner_display_name = be.meta.owner_display_name;
  log_entry.tag = be.tag;

#warning zones_trace

#if 0
  uint64_t index_ver;
  uint16_t bilog_flags;
  rgw_zone_set zones_trace;
#endif

  be_info.encode(e.data);
  return e;
}

int SIProvider_BucketFull::do_fetch(int shard_id, std::string marker, int max, fetch_result *result)
{
  int num_shards = (bucket_info.layout.current_index.layout.normal.num_shards ? : 1);

  if (shard_id >= num_shards) {
    return -ERANGE;
  }

  vector<rgw_bucket_dir_entry> objs;

  result->done = false;

  RGWRados::Bucket target(store->getRados(), bucket_info);

  target.set_shard_id(shard_id);

  RGWRados::Bucket::List list_op(&target);

  list_op.params.marker = marker;
  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true; /* we'll be reading from a single shard */

  bool is_truncated;

  int ret = list_op.list_objects(max, &objs, nullptr, &is_truncated, null_yield);
  if (ret < 0) {
    ldout(cct, 5) << __func__ << "(): list_op.list_objects() returned ret=" << ret << dendl;
    return ret;
  }

  for (auto& o : objs) {
    auto e = create_entry(o);
    result->entries.push_back(e);
  }

  result->more = is_truncated;

  return 0;
}

SIProviderRef RGWSIPGen_BucketFull::get(std::optional<std::string> instance)
{
  if (!instance) {
    return nullptr;
  }

  rgw_bucket bucket;

  int r = rgw_bucket_parse_bucket_key(cct, *instance, &bucket, nullptr);
  if (r < 0) {
    ldout(cct, 20) << __func__ << ": failed to parse bucket key (instance=" << *instance << ") r=" << r << dendl;
    return nullptr;
  }

  RGWBucketInfo bucket_info;
  r = ctl.bucket->read_bucket_info(bucket, &bucket_info, null_yield);
  if (r < 0) {
    ldout(cct, 20) << "failed to read bucket info (bucket=" << bucket << ") r=" << r << dendl;
    return nullptr;
  }

  SIProviderRef result;

  result.reset(new SIProvider_BucketFull(cct, store, bucket_info));

  return result;
}

SIProvider::Entry SIProvider_BucketInc::create_entry(rgw_bi_log_entry& be) const
{
  SIProvider::Entry e;
  e.key = be.id;

  siprovider_bucket_entry_info bei;
  bei.entry = be;
  bei.encode(e.data);
  return e;
}

int SIProvider_BucketInc::do_fetch(int shard_id, std::string marker, int max, fetch_result *result)
{
  int num_shards = (bucket_info.layout.current_index.layout.normal.num_shards ? : 1);

  if (shard_id >= num_shards) {
    return -ERANGE;
  }

  int sid = (bucket_info.layout.current_index.layout.normal.num_shards > 0  ? shard_id : -1);

  bool truncated = true;
  int count = 0;

#define DEFAULT_MAX_ENTRIES 1000
  if (max < 0) {
    max = DEFAULT_MAX_ENTRIES;
  }

  while (max > 0 && truncated) {
    list<rgw_bi_log_entry> entries;

    int ret = store->svc()->bilog_rados->log_list(bucket_info, sid, marker, max - count, entries, &truncated);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: list_bi_log_entries(): ret=" << ret << dendl;
      return -ret;
    }

    count += entries.size();

    for (auto& be : entries) {
      auto e = create_entry(be);
      result->entries.push_back(e);
    }
    if (!entries.empty()) {
      marker = entries.back().id;
    }
  }

  result->done = false;
  result->more = !truncated;

  return 0;
}

int SIProvider_BucketInc::do_get_cur_state(int shard_id, std::string *marker, ceph::real_time *timestamp) const
{
  int sid = (bucket_info.layout.current_index.layout.normal.num_shards > 0  ? shard_id : -1);

  map<int, string> markers;
  int ret = store->svc()->bilog_rados->get_log_status(bucket_info, sid, &markers);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << ": get_log_status() bucket=" << bucket_info.bucket << " shard_id=" << shard_id << " returned ret=" << ret << dendl;
    return ret;
  }
  *marker = markers[shard_id];

  /* bucket index header doesn't keep the timestamp info for max marker,
   * we can add that, but at the moment it's not needed
   */
  *timestamp = ceph::real_time();

  return 0;
}

int SIProvider_BucketInc::do_trim( int shard_id, const std::string& marker)
{
  int sid = (bucket_info.layout.current_index.layout.normal.num_shards > 0  ? shard_id : -1);

  int ret = store->svc()->bilog_rados->log_trim(bucket_info, sid, string(), marker);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << ": log_trim() bucket=" << bucket_info.bucket << " shard_id=" << shard_id << " marker=" << marker << " returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

SIProviderRef RGWSIPGen_BucketInc::get(std::optional<std::string> instance)
{
  if (!instance) {
    return nullptr;
  }

  rgw_bucket bucket;

  int r = rgw_bucket_parse_bucket_key(cct, *instance, &bucket, nullptr);
  if (r < 0) {
    ldout(cct, 20) << __func__ << ": failed to parse bucket key (instance=" << *instance << ") r=" << r << dendl;
    return nullptr;
  }

  RGWBucketInfo bucket_info;
  r = ctl.bucket->read_bucket_info(bucket, &bucket_info, null_yield);
  if (r < 0) {
    ldout(cct, 20) << "failed to read bucket info (bucket=" << bucket << ") r=" << r << dendl;
    return nullptr;
  }

  SIProviderRef result;

  result.reset(new SIProvider_BucketInc(cct, store, bucket_info));

  return result;
}

