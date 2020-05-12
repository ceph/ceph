
#include "common/debug.h"

#include "rgw_sip_bucket.h"
#include "rgw_bucket.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw

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
