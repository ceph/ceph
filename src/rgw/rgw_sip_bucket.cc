
#include "common/debug.h"
#include "common/Formatter.h"

#include "services/svc_bilog_rados.h"

#include "rgw_sip_bucket.h"
#include "rgw_bucket.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"


#define dout_subsys ceph_subsys_rgw

void siprovider_bucket_entry_info::Info::dump(Formatter *f) const
{
  encode_json("object", object, f);
  encode_json("instance", instance, f);
  utime_t ut(timestamp);
  ut.gmtime_nsec(f->dump_stream("timestamp"));
  encode_json("versioned_epoch", versioned_epoch, f);
  encode_json("op", op, f);
  encode_json("owner", owner, f);
  encode_json("owner_display_name", owner_display_name, f);
  encode_json("instance_tag", instance_tag, f);
  encode_json("complete", complete, f);
  encode_json("sync_trace", sync_trace, f);
}

void siprovider_bucket_entry_info::Info::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("object", object, obj);
  JSONDecoder::decode_json("instance", instance, obj);
  JSONDecoder::decode_json("timestamp", timestamp, obj);
  JSONDecoder::decode_json("versioned_epoch", versioned_epoch, obj);
  JSONDecoder::decode_json("op", op, obj);
  JSONDecoder::decode_json("owner", owner, obj);
  JSONDecoder::decode_json("owner_display_name", owner_display_name, obj);
  JSONDecoder::decode_json("instance_tag", instance_tag, obj);
  JSONDecoder::decode_json("complete", complete, true, obj); /* default_val = true */
  JSONDecoder::decode_json("sync_trace", sync_trace, obj);
}

void siprovider_bucket_entry_info::dump(Formatter *f) const
{
  info.dump(f);
}

void siprovider_bucket_entry_info::decode_json(JSONObj *obj)
{
  info.decode_json(obj);
}

const std::string& siprovider_bucket_entry_info::Info::to_sip_op(RGWModifyOp rgw_op)
{
  switch (rgw_op) {
    case CLS_RGW_OP_ADD:
      return SIP_BUCKET_OP_CREATE_OBJ;
      break;
    case CLS_RGW_OP_DEL:
    case CLS_RGW_OP_UNLINK_INSTANCE:
      return SIP_BUCKET_OP_DELETE_OBJ;
      break;
    case CLS_RGW_OP_LINK_OLH:
      return SIP_BUCKET_OP_SET_CURRENT;
      break;
    case CLS_RGW_OP_LINK_OLH_DM:
      return SIP_BUCKET_OP_CREATE_DM;
      break;
    case CLS_RGW_OP_CANCEL:
      return SIP_BUCKET_OP_CANCEL;
      break;
    case CLS_RGW_OP_SYNCSTOP:
      return SIP_BUCKET_OP_SYNC_STOP;
      break;
    case CLS_RGW_OP_RESYNC:
      return SIP_BUCKET_OP_SYNC_START;
      break;
    default:
      break;
  }

  return SIP_BUCKET_OP_UNKNOWN;
}

RGWModifyOp siprovider_bucket_entry_info::Info::from_sip_op(const string& op, std::optional<uint64_t> versioned_epoch)
{
  if (op == SIP_BUCKET_OP_CREATE_OBJ) {
    if (versioned_epoch) {
      return CLS_RGW_OP_LINK_OLH;
    }
    return CLS_RGW_OP_ADD;
  }
  if (op == SIP_BUCKET_OP_DELETE_OBJ) {
    if (versioned_epoch) {
      return CLS_RGW_OP_UNLINK_INSTANCE;
    }
    return CLS_RGW_OP_DEL;
  }
  if (op == SIP_BUCKET_OP_SET_CURRENT) {
    return CLS_RGW_OP_LINK_OLH;
  }
  if (op == SIP_BUCKET_OP_CREATE_DM) {
    return CLS_RGW_OP_LINK_OLH_DM;
  }
  if (op == SIP_BUCKET_OP_CANCEL) {
    return CLS_RGW_OP_CANCEL;
  }
  if (op == SIP_BUCKET_OP_SYNC_STOP) {
    return CLS_RGW_OP_SYNCSTOP;
  }
  if (op == SIP_BUCKET_OP_SYNC_START) {
    return CLS_RGW_OP_RESYNC;
  }
  return CLS_RGW_OP_UNKNOWN;
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

  auto& log_entry = be_info.info;

  log_entry.object = be.key.name;
  log_entry.instance = be.key.instance;
  log_entry.timestamp = be.meta.mtime;

  if (be.versioned_epoch > 0) {
    log_entry.versioned_epoch = be.versioned_epoch;

    if (be.key.instance.empty()) {
      log_entry.instance = "null";
    }
  }

  if (!be.is_delete_marker()) {
    log_entry.op = SIP_BUCKET_OP_CREATE_OBJ;
  } else {
    log_entry.op = SIP_BUCKET_OP_CREATE_DM;
  }
  log_entry.owner = be.meta.owner;
  log_entry.owner_display_name = be.meta.owner_display_name;
  log_entry.instance_tag = be.tag;
  log_entry.complete = true;

#warning zones_trace

#if 0
  rgw_zone_set zones_trace;
#endif

  be_info.encode(e.data);
  return e;
}

int SIProvider_BucketFull::do_fetch(const DoutPrefixProvider *dpp, int shard_id, std::string marker, int max, fetch_result *result)
{
  int num_shards = (bucket_info.layout.current_index.layout.normal.num_shards ? : 1);

  if (shard_id >= num_shards) {
    return -ERANGE;
  }

  result->done = false;

  RGWRados::Bucket target(store->getRados(), bucket_info);

  target.set_shard_id(shard_id);

  RGWRados::Bucket::List list_op(&target);

  list_op.params.marker = marker;
  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true; /* we'll be reading from a single shard */

  while (max > 0) {
    vector<rgw_bucket_dir_entry> objs;

    bool is_truncated;

    int ret = list_op.list_objects(dpp, max, &objs, nullptr, &is_truncated, null_yield);
    if (ret < 0) {
      ldout(cct, 5) << __func__ << "(): list_op.list_objects() returned ret=" << ret << dendl;
      return ret;
    }

    if (objs.empty()) {
      break;
    }

    for (auto& o : objs) {
      auto e = create_entry(o);
      result->entries.push_back(e);
    }

    if (!is_truncated) {
      break;
    }

    max -= result->entries.size();
    result->more = is_truncated;
  }

  return 0;
}

SIProviderRef RGWSIPGen_BucketFull::get(const DoutPrefixProvider *dpp, std::optional<std::string> instance)
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
  r = ctl.bucket->read_bucket_info(bucket, &bucket_info, null_yield, dpp);
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

  auto& log_entry = bei.info;

  log_entry.object = be.object;
  log_entry.instance = be.instance;
  log_entry.timestamp = be.timestamp;
  if (be.ver.pool < 0) {
    log_entry.versioned_epoch = be.ver.epoch;
  }

  if (be.is_versioned() &&
      log_entry.instance.empty()) {
    log_entry.instance = "null";
  }

  log_entry.op = siprovider_bucket_entry_info::Info::to_sip_op(be.op);
  log_entry.owner = be.owner;
  log_entry.owner_display_name = be.owner_display_name;
  log_entry.instance_tag = be.tag;
  log_entry.complete = (be.state == CLS_RGW_STATE_COMPLETE);

  for (auto& z : be.zones_trace.entries) {
    log_entry.sync_trace.insert(z.to_str());
  }

  bei.encode(e.data);
  return e;
}

int SIProvider_BucketInc::do_fetch(const DoutPrefixProvider *dpp, int shard_id, std::string marker, int max, fetch_result *result)
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

    int ret = store->svc()->bilog_rados->log_list(dpp, bucket_info, sid, marker, max - count, entries, &truncated);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: list_bi_log_entries(): ret=" << ret << dendl;
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

int SIProvider_BucketInc::do_get_cur_state(const DoutPrefixProvider *dpp,
                                           int shard_id, std::string *marker, ceph::real_time *timestamp,
                                           bool *disabled,
                                           optional_yield y) const
{
  int sid = (bucket_info.layout.current_index.layout.normal.num_shards > 0  ? shard_id : -1);

  map<int, RGWSI_BILog_RADOS::Status> markers;
  int ret = store->svc()->bilog_rados->get_log_status(dpp, bucket_info, sid, &markers, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": get_log_status() bucket=" << bucket_info.bucket << " shard_id=" << shard_id << " returned ret=" << ret << dendl;
    return ret;
  }

  auto& entry = markers[shard_id];
  *marker = entry.marker;
  *disabled = entry.disabled;

  /* bucket index header doesn't keep the timestamp info for max marker,
   * we can add that, but at the moment it's not needed
   */
  *timestamp = ceph::real_time();

  return 0;
}

int SIProvider_BucketInc::do_trim(const DoutPrefixProvider *dpp, int shard_id, const std::string& marker)
{
  int sid = (bucket_info.layout.current_index.layout.normal.num_shards > 0  ? shard_id : -1);

  int ret = store->svc()->bilog_rados->log_trim(dpp, bucket_info, sid, string(), marker);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << ": log_trim() bucket=" << bucket_info.bucket << " shard_id=" << shard_id << " marker=" << marker << " returned ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

SIProviderRef RGWSIPGen_BucketInc::get(const DoutPrefixProvider *dpp,
                                       std::optional<std::string> instance)
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
  r = ctl.bucket->read_bucket_info(bucket, &bucket_info, null_yield, dpp);
  if (r < 0) {
    ldout(cct, 20) << "failed to read bucket info (bucket=" << bucket << ") r=" << r << dendl;
    return nullptr;
  }

  SIProviderRef result;

  result.reset(new SIProvider_BucketInc(cct, store, bucket_info));

  return result;
}

SIProviderRef RGWSIPGen_BucketContainer::get(const DoutPrefixProvider *dpp,
                                             std::optional<std::string> instance)
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
  r = ctl.bucket->read_bucket_info(bucket, &bucket_info, null_yield, dpp);
  if (r < 0) {
    ldout(cct, 20) << "failed to read bucket info (bucket=" << bucket << ") r=" << r << dendl;
    return nullptr;
  }

  SIProviderRef result;

  auto full = new SIProvider_BucketFull(cct, store, bucket_info);
  auto inc = new SIProvider_BucketInc(cct, store, bucket_info);

  result.reset(new SIProvider_Container(cct,
                                        "bucket",
                                        instance,
                                        { SIProviderRef(full),
                                          SIProviderRef(inc) } ));

  return result;
}

