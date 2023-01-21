// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_metadata.h"

#include "rgw_zone.h"
#include "rgw_mdlog.h"

#include "services/svc_zone.h"
#include "services/svc_cls.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

const std::string RGWMetadataLogHistory::oid = "meta.history";

struct obj_version;

void rgw_shard_name(const string& prefix, unsigned max_shards, const string& key, string& name, int *shard_id)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  char buf[16];
  if (shard_id) {
    *shard_id = val % max_shards;
  }
  snprintf(buf, sizeof(buf), "%u", (unsigned)(val % max_shards));
  name = prefix + buf;
}

void rgw_shard_name(const string& prefix, unsigned max_shards, const string& section, const string& key, string& name)
{
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  val ^= ceph_str_hash_linux(section.c_str(), section.size());
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", (unsigned)(val % max_shards));
  name = prefix + buf;
}

void rgw_shard_name(const string& prefix, unsigned shard_id, string& name)
{
  char buf[16];
  snprintf(buf, sizeof(buf), "%u", shard_id);
  name = prefix + buf;
}

int RGWMetadataLog::add_entry(const DoutPrefixProvider *dpp, const string& hash_key, const string& section, const string& key, bufferlist& bl, optional_yield y) {
  if (!svc.zone->need_to_log_metadata())
    return 0;

  string oid;
  int shard_id;

  rgw_shard_name(prefix, cct->_conf->rgw_md_log_max_shards, hash_key, oid, &shard_id);
  mark_modified(shard_id);
  real_time now = real_clock::now();
  return svc.cls->timelog.add(dpp, oid, now, section, key, bl, y);
}

int RGWMetadataLog::get_shard_id(const string& hash_key, int *shard_id)
{
  string oid;

  rgw_shard_name(prefix, cct->_conf->rgw_md_log_max_shards, hash_key, oid, shard_id);
  return 0;
}

int RGWMetadataLog::store_entries_in_shard(const DoutPrefixProvider *dpp, vector<cls_log_entry>& entries, int shard_id, librados::AioCompletion *completion)
{
  string oid;

  mark_modified(shard_id);
  rgw_shard_name(prefix, shard_id, oid);
  return svc.cls->timelog.add(dpp, oid, entries, completion, false, null_yield);
}

void RGWMetadataLog::init_list_entries(int shard_id, const real_time& from_time, const real_time& end_time, 
                                       const string& marker, void **handle)
{
  LogListCtx *ctx = new LogListCtx();

  ctx->cur_shard = shard_id;
  ctx->from_time = from_time;
  ctx->end_time  = end_time;
  ctx->marker    = marker;

  get_shard_oid(ctx->cur_shard, ctx->cur_oid);

  *handle = (void *)ctx;
}

void RGWMetadataLog::complete_list_entries(void *handle) {
  LogListCtx *ctx = static_cast<LogListCtx *>(handle);
  delete ctx;
}

int RGWMetadataLog::list_entries(const DoutPrefixProvider *dpp, void *handle,
				 int max_entries,
				 vector<cls_log_entry>& entries,
				 string *last_marker,
				 bool *truncated,
				 optional_yield y) {
  LogListCtx *ctx = static_cast<LogListCtx *>(handle);

  if (!max_entries) {
    *truncated = false;
    return 0;
  }

  std::string next_marker;
  int ret = svc.cls->timelog.list(dpp, ctx->cur_oid, ctx->from_time, ctx->end_time,
                                  max_entries, entries, ctx->marker,
                                  &next_marker, truncated, y);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  ctx->marker = std::move(next_marker);
  if (last_marker) {
    *last_marker = ctx->marker;
  }

  if (ret == -ENOENT)
    *truncated = false;

  return 0;
}

int RGWMetadataLog::get_info(const DoutPrefixProvider *dpp, int shard_id, RGWMetadataLogInfo *info, optional_yield y)
{
  string oid;
  get_shard_oid(shard_id, oid);

  cls_log_header header;

  int ret = svc.cls->timelog.info(dpp, oid, &header, y);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  info->marker = header.max_marker;
  info->last_update = header.max_time.to_real_time();

  return 0;
}

static void _mdlog_info_completion(librados::completion_t cb, void *arg)
{
  auto infoc = static_cast<RGWMetadataLogInfoCompletion *>(arg);
  infoc->finish(cb);
  infoc->put(); // drop the ref from get_info_async()
}

RGWMetadataLogInfoCompletion::RGWMetadataLogInfoCompletion(info_callback_t cb)
  : completion(librados::Rados::aio_create_completion((void *)this,
                                                      _mdlog_info_completion)),
    callback(cb)
{
}

RGWMetadataLogInfoCompletion::~RGWMetadataLogInfoCompletion()
{
  completion->release();
}

int RGWMetadataLog::get_info_async(const DoutPrefixProvider *dpp, int shard_id, RGWMetadataLogInfoCompletion *completion)
{
  string oid;
  get_shard_oid(shard_id, oid);

  completion->get(); // hold a ref until the completion fires

  return svc.cls->timelog.info_async(dpp, completion->get_io_obj(), oid,
                                     &completion->get_header(),
                                     completion->get_completion());
}

int RGWMetadataLog::trim(const DoutPrefixProvider *dpp, int shard_id, const real_time& from_time, const real_time& end_time,
                         const string& start_marker, const string& end_marker, optional_yield y)
{
  string oid;
  get_shard_oid(shard_id, oid);

  return svc.cls->timelog.trim(dpp, oid, from_time, end_time, start_marker,
                               end_marker, nullptr, y);
}
  
int RGWMetadataLog::lock_exclusive(const DoutPrefixProvider *dpp, int shard_id, timespan duration, string& zone_id, string& owner_id) {
  string oid;
  get_shard_oid(shard_id, oid);

  return svc.cls->lock.lock_exclusive(dpp, svc.zone->get_zone_params().log_pool, oid, duration, zone_id, owner_id);
}

int RGWMetadataLog::unlock(const DoutPrefixProvider *dpp, int shard_id, string& zone_id, string& owner_id) {
  string oid;
  get_shard_oid(shard_id, oid);

  return svc.cls->lock.unlock(dpp, svc.zone->get_zone_params().log_pool, oid, zone_id, owner_id);
}

void RGWMetadataLog::mark_modified(int shard_id)
{
  lock.get_read();
  if (modified_shards.find(shard_id) != modified_shards.end()) {
    lock.unlock();
    return;
  }
  lock.unlock();

  std::unique_lock wl{lock};
  modified_shards.insert(shard_id);
}

void RGWMetadataLog::read_clear_modified(set<int> &modified)
{
  std::unique_lock wl{lock};
  modified.swap(modified_shards);
  modified_shards.clear();
}

void RGWMetadataLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  utime_t ut(last_update);
  encode_json("last_update", ut, f);
}

void RGWMetadataLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  utime_t ut;
  JSONDecoder::decode_json("last_update", ut, obj);
  last_update = ut.to_real_time();
}

