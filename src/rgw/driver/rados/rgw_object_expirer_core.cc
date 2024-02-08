// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>


#include "auth/Crypto.h"

#include "common/armor.h"
#include "common/ceph_json.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "global/global_init.h"

#include "include/utime.h"
#include "include/str_list.h"

#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_object_expirer_core.h"
#include "rgw_zone.h"
#include "rgw_sal_rados.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_bi_rados.h"

#include "cls/lock/cls_lock_client.h"
#include "cls/timeindex/cls_timeindex_client.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

static string objexp_lock_name = "gc_process";

static string objexp_hint_get_shardname(int shard_num)
{
  char buf[64];
  snprintf(buf, sizeof(buf), "obj_delete_at_hint.%010u", (unsigned)shard_num);
  return buf;
}

static int objexp_key_shard(const rgw_obj_index_key& key, int num_shards)
{
  string obj_key = key.name + key.instance;
  return RGWSI_BucketIndex_RADOS::bucket_shard_index(obj_key, num_shards);
}

static string objexp_hint_get_keyext(const string& tenant_name,
                                     const string& bucket_name,
                                     const string& bucket_id,
                                     const rgw_obj_key& obj_key) {
  return tenant_name + (tenant_name.empty() ? "" : ":") + bucket_name + ":" + bucket_id +
    ":" + obj_key.name + ":" + obj_key.instance;
}

static void objexp_get_shard(int shard_num,
                             string *shard)
{
  *shard = objexp_hint_get_shardname(shard_num);
}

static int objexp_hint_parse(const DoutPrefixProvider *dpp, CephContext *cct, cls_timeindex_entry &ti_entry,
                             objexp_hint_entry *hint_entry)
{
  try {
    auto iter = ti_entry.value.cbegin();
    decode(*hint_entry, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: couldn't decode avail_pools" << dendl;
  }

  return 0;
}

int RGWObjExpStore::objexp_hint_add(const DoutPrefixProvider *dpp, 
                              const ceph::real_time& delete_at,
                              const string& tenant_name,
                              const string& bucket_name,
                              const string& bucket_id,
                              const rgw_obj_index_key& obj_key)
{
  const string keyext = objexp_hint_get_keyext(tenant_name, bucket_name,
          bucket_id, obj_key);
  objexp_hint_entry he = {
      .tenant = tenant_name,
      .bucket_name = bucket_name,
      .bucket_id = bucket_id,
      .obj_key = obj_key,
      .exp_time = delete_at };
  bufferlist hebl;
  encode(he, hebl);
  librados::ObjectWriteOperation op;
  cls_timeindex_add(op, utime_t(delete_at), keyext, hebl);

  string shard_name = objexp_hint_get_shardname(objexp_key_shard(obj_key, cct->_conf->rgw_objexp_hints_num_shards));
  rgw_rados_ref obj;
  int r = rgw_get_rados_ref(dpp, driver->getRados()->get_rados_handle(),
			    { driver->svc()->zone->get_zone_params().log_pool,
			      shard_name },
			    &obj);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): failed to open obj=" << obj << " (r=" << r << ")" << dendl;
    return r;
  }
  return obj.operate(dpp, &op, null_yield);
}

int RGWObjExpStore::objexp_hint_list(const DoutPrefixProvider *dpp, 
                               const string& oid,
                               const ceph::real_time& start_time,
                               const ceph::real_time& end_time,
                               const int max_entries,
                               const string& marker,
                               list<cls_timeindex_entry>& entries, /* out */
                               string *out_marker,                 /* out */
                               bool *truncated)                    /* out */
{
  librados::ObjectReadOperation op;
  cls_timeindex_list(op, utime_t(start_time), utime_t(end_time), marker, max_entries, entries,
        out_marker, truncated);

  rgw_rados_ref obj;
  int r = rgw_get_rados_ref(dpp, driver->getRados()->get_rados_handle(),
			    { driver->svc()->zone->get_zone_params().log_pool,
			      oid }, &obj);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): failed to open obj=" << obj << " (r=" << r << ")" << dendl;
    return r;
  }
  bufferlist obl;
  int ret = obj.operate(dpp, &op, &obl, null_yield);

  if ((ret < 0 ) && (ret != -ENOENT)) {
    return ret;
  }

  if ((ret == -ENOENT) && truncated) {
    *truncated = false;
  }

  return 0;
}

static int cls_timeindex_trim_repeat(const DoutPrefixProvider *dpp, 
                                rgw_rados_ref ref,
                                const string& oid,
                                const utime_t& from_time,
                                const utime_t& to_time,
                                const string& from_marker,
                                const string& to_marker, optional_yield y)
{
  bool done = false;
  do {
    librados::ObjectWriteOperation op;
    cls_timeindex_trim(op, from_time, to_time, from_marker, to_marker);
    int r = rgw_rados_operate(dpp, ref.ioctx, oid, &op, null_yield);
    if (r == -ENODATA)
      done = true;
    else if (r < 0)
      return r;
  } while (!done);

  return 0;
}

int RGWObjExpStore::objexp_hint_trim(const DoutPrefixProvider *dpp, 
                               const string& oid,
                               const ceph::real_time& start_time,
                               const ceph::real_time& end_time,
                               const string& from_marker,
                               const string& to_marker, optional_yield y)
{
  rgw_rados_ref ref;
  auto ret = rgw_get_rados_ref(dpp, driver->getRados()->get_rados_handle(),
			       {driver->svc()->zone->get_zone_params().log_pool, oid},
			       &ref);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): failed to open oid="
		      << oid << " (r=" << ret << ")" << dendl;
    return ret;
  }
  ret = cls_timeindex_trim_repeat(dpp, ref, oid, utime_t(start_time), utime_t(end_time),
				  from_marker, to_marker, y);
  if ((ret < 0 ) && (ret != -ENOENT)) {
    return ret;
  }

  return 0;
}

int RGWObjectExpirer::garbage_single_object(const DoutPrefixProvider *dpp, objexp_hint_entry& hint)
{
  RGWBucketInfo bucket_info;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  int ret = driver->load_bucket(dpp, rgw_bucket(hint.tenant, hint.bucket_name, hint.bucket_id), &bucket, null_yield);
  if (-ENOENT == ret) {
    ldpp_dout(dpp, 15) << "NOTICE: cannot find bucket = " \
        << hint.bucket_name << ". The object must be already removed" << dendl;
    return -ERR_PRECONDITION_FAILED;
  } else if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: could not init bucket = " \
        << hint.bucket_name << "due to ret = " << ret << dendl;
    return ret;
  }

  rgw_obj_key key = hint.obj_key;
  if (key.instance.empty()) {
    key.instance = "null";
  }

  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);
  obj->set_atomic();
  ret = obj->delete_object(dpp, null_yield, rgw::sal::FLAG_LOG_OP);

  return ret;
}

void RGWObjectExpirer::garbage_chunk(const DoutPrefixProvider *dpp, 
                                  list<cls_timeindex_entry>& entries,      /* in  */
                                  bool& need_trim)                         /* out */
{
  need_trim = false;

  for (list<cls_timeindex_entry>::iterator iter = entries.begin();
       iter != entries.end();
       ++iter)
  {
    objexp_hint_entry hint;
    ldpp_dout(dpp, 15) << "got removal hint for: " << iter->key_ts.sec() \
        << " - " << iter->key_ext << dendl;

    int ret = objexp_hint_parse(dpp, driver->ctx(), *iter, &hint);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "cannot parse removal hint for " << hint.obj_key << dendl;
      continue;
    }

    /* PRECOND_FAILED simply means that our hint is not valid.
     * We can silently ignore that and move forward. */
    ret = garbage_single_object(dpp, hint);
    if (ret == -ERR_PRECONDITION_FAILED) {
      ldpp_dout(dpp, 15) << "not actual hint for object: " << hint.obj_key << dendl;
    } else if (ret < 0) {
      ldpp_dout(dpp, 1) << "cannot remove expired object: " << hint.obj_key << dendl;
    }

    need_trim = true;
  }

  return;
}

void RGWObjectExpirer::trim_chunk(const DoutPrefixProvider *dpp, 
                                  const string& shard,
                                  const utime_t& from,
                                  const utime_t& to,
                                  const string& from_marker,
                                  const string& to_marker, optional_yield y)
{
  ldpp_dout(dpp, 20) << "trying to trim removal hints to=" << to
                          << ", to_marker=" << to_marker << dendl;

  real_time rt_from = from.to_real_time();
  real_time rt_to = to.to_real_time();

  int ret = exp_store.objexp_hint_trim(dpp, shard, rt_from, rt_to,
                                       from_marker, to_marker, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR during trim: " << ret << dendl;
  }

  return;
}

bool RGWObjectExpirer::process_single_shard(const DoutPrefixProvider *dpp, 
                                            const string& shard,
                                            const utime_t& last_run,
                                            const utime_t& round_start, optional_yield y)
{
  string marker;
  string out_marker;
  bool truncated = false;
  bool done = true;

  CephContext *cct = driver->ctx();
  int num_entries = cct->_conf->rgw_objexp_chunk_size;

  int max_secs = cct->_conf->rgw_objexp_gc_interval;
  utime_t end = ceph_clock_now();
  end += max_secs;

  rados::cls::lock::Lock l(objexp_lock_name);

  utime_t time(max_secs, 0);
  l.set_duration(time);

  int ret = l.lock_exclusive(&static_cast<rgw::sal::RadosStore*>(driver)->getRados()->objexp_pool_ctx, shard);
  if (ret == -EBUSY) { /* already locked by another processor */
    ldpp_dout(dpp, 5) << __func__ << "(): failed to acquire lock on " << shard << dendl;
    return false;
  }

  do {
    real_time rt_last = last_run.to_real_time();
    real_time rt_start = round_start.to_real_time();

    list<cls_timeindex_entry> entries;
    ret = exp_store.objexp_hint_list(dpp, shard, rt_last, rt_start,
                                     num_entries, marker, entries,
                                     &out_marker, &truncated);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "cannot get removal hints from shard: " << shard
                     << dendl;
      continue;
    }

    bool need_trim;
    garbage_chunk(dpp, entries, need_trim);

    if (need_trim) {
      trim_chunk(dpp, shard, last_run, round_start, marker, out_marker, y);
    }

    utime_t now = ceph_clock_now();
    if (now >= end) {
      done = false;
      break;
    }

    marker = out_marker;
  } while (truncated);

  l.unlock(&static_cast<rgw::sal::RadosStore*>(driver)->getRados()->objexp_pool_ctx, shard);
  return done;
}

/* Returns true if all shards have been processed successfully. */
bool RGWObjectExpirer::inspect_all_shards(const DoutPrefixProvider *dpp, 
                                          const utime_t& last_run,
                                          const utime_t& round_start, optional_yield y)
{
  CephContext * const cct = driver->ctx();
  int num_shards = cct->_conf->rgw_objexp_hints_num_shards;
  bool all_done = true;

  for (int i = 0; i < num_shards; i++) {
    string shard;
    objexp_get_shard(i, &shard);

    ldpp_dout(dpp, 20) << "processing shard = " << shard << dendl;

    if (! process_single_shard(dpp, shard, last_run, round_start, y)) {
      all_done = false;
    }
  }

  return all_done;
}

bool RGWObjectExpirer::going_down()
{
  return down_flag;
}

void RGWObjectExpirer::start_processor()
{
  worker = new OEWorker(driver->ctx(), this);
  worker->create("rgw_obj_expirer");
}

void RGWObjectExpirer::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}

void *RGWObjectExpirer::OEWorker::entry() {
  utime_t last_run;
  do {
    utime_t start = ceph_clock_now();
    ldpp_dout(this, 2) << "object expiration: start" << dendl;
    if (oe->inspect_all_shards(this, last_run, start, null_yield)) {
      /* All shards have been processed properly. Next time we can start
       * from this moment. */
      last_run = start;
    }
    ldpp_dout(this, 2) << "object expiration: stop" << dendl;


    if (oe->going_down())
      break;

    utime_t end = ceph_clock_now();
    end -= start;
    int secs = cct->_conf->rgw_objexp_gc_interval;

    if (secs <= end.sec())
      continue; // next round

    secs -= end.sec();

    std::unique_lock l{lock};
    cond.wait_for(l, std::chrono::seconds(secs));
  } while (!oe->going_down());

  return NULL;
}

void RGWObjectExpirer::OEWorker::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}

CephContext *RGWObjectExpirer::OEWorker::get_cct() const 
{ 
  return cct; 
}

unsigned RGWObjectExpirer::OEWorker::get_subsys() const 
{
    return dout_subsys;
}

std::ostream& RGWObjectExpirer::OEWorker::gen_prefix(std::ostream& out) const 
{ 
  return out << "rgw object expirer Worker thread: "; 
}
