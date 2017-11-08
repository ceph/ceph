// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rados.h"
#include "rgw_bucket.h"
#include "rgw_reshard.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "common/errno.h"
#include "common/ceph_json.h"

#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

const string reshard_oid_prefix = "reshard.";
const string reshard_lock_name = "reshard_process";
const string bucket_instance_lock_name = "bucket_instance_lock";

using namespace std;

#define RESHARD_SHARD_WINDOW 64
#define RESHARD_MAX_AIO 128

class BucketReshardShard {
  RGWRados *store;
  const RGWBucketInfo& bucket_info;
  int num_shard;
  RGWRados::BucketShard bs;
  vector<rgw_cls_bi_entry> entries;
  map<uint8_t, rgw_bucket_category_stats> stats;
  deque<librados::AioCompletion *>& aio_completions;

  int wait_next_completion() {
    librados::AioCompletion *c = aio_completions.front();
    aio_completions.pop_front();

    c->wait_for_safe();

    int ret = c->get_return_value();
    c->release();

    if (ret < 0) {
      derr << "ERROR: reshard rados operation failed: " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    return 0;
  }

  int get_completion(librados::AioCompletion **c) {
    if (aio_completions.size() >= RESHARD_MAX_AIO) {
      int ret = wait_next_completion();
      if (ret < 0) {
        return ret;
      }
    }

    *c = librados::Rados::aio_create_completion(nullptr, nullptr, nullptr);
    aio_completions.push_back(*c);

    return 0;
  }

public:
  BucketReshardShard(RGWRados *_store, const RGWBucketInfo& _bucket_info,
                     int _num_shard,
                     deque<librados::AioCompletion *>& _completions) : store(_store), bucket_info(_bucket_info), bs(store),
                                                                       aio_completions(_completions) {
    num_shard = (bucket_info.num_shards > 0 ? _num_shard : -1);
    bs.init(bucket_info.bucket, num_shard);
  }

  int get_num_shard() {
    return num_shard;
  }

  int add_entry(rgw_cls_bi_entry& entry, bool account, uint8_t category,
                const rgw_bucket_category_stats& entry_stats) {
    entries.push_back(entry);
    if (account) {
      rgw_bucket_category_stats& target = stats[category];
      target.num_entries += entry_stats.num_entries;
      target.total_size += entry_stats.total_size;
      target.total_size_rounded += entry_stats.total_size_rounded;
    }
    if (entries.size() >= RESHARD_SHARD_WINDOW) {
      int ret = flush();
      if (ret < 0) {
        return ret;
      }
    }
    return 0;
  }
  int flush() {
    if (entries.size() == 0) {
      return 0;
    }

    librados::ObjectWriteOperation op;
    for (auto& entry : entries) {
      store->bi_put(op, bs, entry);
    }
    cls_rgw_bucket_update_stats(op, false, stats);

    librados::AioCompletion *c;
    int ret = get_completion(&c);
    if (ret < 0) {
      return ret;
    }
    ret = bs.index_ctx.aio_operate(bs.bucket_obj, c, &op);
    if (ret < 0) {
      derr << "ERROR: failed to store entries in target bucket shard (bs=" << bs.bucket << "/" << bs.shard_id << ") error=" << cpp_strerror(-ret) << dendl;
      return ret;
    }
    entries.clear();
    stats.clear();
    return 0;
  }

  int wait_all_aio() {
    int ret = 0;
    while (!aio_completions.empty()) {
      int r = wait_next_completion();
      if (r < 0) {
        ret = r;
      }
    }
    return ret;
  }
};

class BucketReshardManager {
  RGWRados *store;
  const RGWBucketInfo& target_bucket_info;
  deque<librados::AioCompletion *> completions;
  int num_target_shards;
  vector<BucketReshardShard *> target_shards;

public:
  BucketReshardManager(RGWRados *_store, const RGWBucketInfo& _target_bucket_info, int _num_target_shards) : store(_store), target_bucket_info(_target_bucket_info),
                                                                                                       num_target_shards(_num_target_shards) {
    target_shards.resize(num_target_shards);
    for (int i = 0; i < num_target_shards; ++i) {
      target_shards[i] = new BucketReshardShard(store, target_bucket_info, i, completions);
    }
  }

  ~BucketReshardManager() {
    for (auto& shard : target_shards) {
      int ret = shard->wait_all_aio();
      if (ret < 0) {
        ldout(store->ctx(), 20) << __func__ << ": shard->wait_all_aio() returned ret=" << ret << dendl;
      }
    }
  }

  int add_entry(int shard_index,
                rgw_cls_bi_entry& entry, bool account, uint8_t category,
                const rgw_bucket_category_stats& entry_stats) {
    int ret = target_shards[shard_index]->add_entry(entry, account, category, entry_stats);
    if (ret < 0) {
      derr << "ERROR: target_shards.add_entry(" << entry.idx << ") returned error: " << cpp_strerror(-ret) << dendl;
      return ret;
    }
    return 0;
  }

  int finish() {
    int ret = 0;
    for (auto& shard : target_shards) {
      int r = shard->flush();
      if (r < 0) {
        derr << "ERROR: target_shards[" << shard->get_num_shard() << "].flush() returned error: " << cpp_strerror(-r) << dendl;
        ret = r;
      }
    }
    for (auto& shard : target_shards) {
      int r = shard->wait_all_aio();
      if (r < 0) {
        derr << "ERROR: target_shards[" << shard->get_num_shard() << "].wait_all_aio() returned error: " << cpp_strerror(-r) << dendl;
        ret = r;
      }
      delete shard;
    }
    target_shards.clear();
    return ret;
  }
};

RGWBucketReshard::RGWBucketReshard(RGWRados *_store, const RGWBucketInfo& _bucket_info, const map<string, bufferlist>& _bucket_attrs) :
                                                     store(_store), bucket_info(_bucket_info), bucket_attrs(_bucket_attrs),
                                                     reshard_lock(reshard_lock_name) {
  const rgw_bucket& b = bucket_info.bucket;
  reshard_oid = b.tenant + (b.tenant.empty() ? "" : ":") + b.name + ":" + b.bucket_id;

  utime_t lock_duration(store->ctx()->_conf->rgw_reshard_bucket_lock_duration, 0);
#define COOKIE_LEN 16
  char cookie_buf[COOKIE_LEN + 1];
  gen_rand_alphanumeric(store->ctx(), cookie_buf, sizeof(cookie_buf) - 1);
  cookie_buf[COOKIE_LEN] = '\0';

  reshard_lock.set_cookie(cookie_buf);
  reshard_lock.set_duration(lock_duration);
}

int RGWBucketReshard::lock_bucket()
{
  int ret = reshard_lock.lock_exclusive(&store->reshard_pool_ctx, reshard_oid);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "RGWReshard::add failed to acquire lock on " << reshard_oid << " ret=" << ret << dendl;
    return ret;
  }
  return 0;
}

void RGWBucketReshard::unlock_bucket()
{
  int ret = reshard_lock.unlock(&store->reshard_pool_ctx, reshard_oid);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "WARNING: RGWReshard::add failed to drop lock on " << reshard_oid << " ret=" << ret << dendl;
  }
}

int RGWBucketReshard::set_resharding_status(const string& new_instance_id, int32_t num_shards, cls_rgw_reshard_status status)
{
  if (new_instance_id.empty()) {
    ldout(store->ctx(), 0) << __func__ << " missing new bucket instance id" << dendl;
    return -EINVAL;
  }

  cls_rgw_bucket_instance_entry instance_entry;
  instance_entry.set_status(new_instance_id, num_shards, status);

  int ret = store->bucket_set_reshard(bucket_info, instance_entry);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "RGWReshard::" << __func__ << " ERROR: error setting bucket resharding flag on bucket index: "
		  << cpp_strerror(-ret) << dendl;
    return ret;
  }
  return 0;
}

int RGWBucketReshard::clear_resharding()
{
  cls_rgw_bucket_instance_entry instance_entry;

  int ret = store->bucket_set_reshard(bucket_info, instance_entry);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "RGWReshard::" << __func__ << " ERROR: error setting bucket resharding flag on bucket index: "
		  << cpp_strerror(-ret) << dendl;
    return ret;
  }
  return 0;
}

static int create_new_bucket_instance(RGWRados *store,
				      int new_num_shards,
				      const RGWBucketInfo& bucket_info,
				      map<string, bufferlist>& attrs,
				      RGWBucketInfo& new_bucket_info)
{
  new_bucket_info = bucket_info;

  store->create_bucket_id(&new_bucket_info.bucket.bucket_id);
  new_bucket_info.bucket.oid.clear();

  new_bucket_info.num_shards = new_num_shards;
  new_bucket_info.objv_tracker.clear();

  new_bucket_info.new_bucket_instance_id.clear();
  new_bucket_info.reshard_status = 0;

  int ret = store->init_bucket_index(new_bucket_info, new_bucket_info.num_shards);
  if (ret < 0) {
    cerr << "ERROR: failed to init new bucket indexes: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  ret = store->put_bucket_instance_info(new_bucket_info, true, real_time(), &attrs);
  if (ret < 0) {
    cerr << "ERROR: failed to store new bucket instance info: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  return 0;
}

int RGWBucketReshard::create_new_bucket_instance(int new_num_shards,
                                                 RGWBucketInfo& new_bucket_info)
{
  return ::create_new_bucket_instance(store, new_num_shards, bucket_info, bucket_attrs, new_bucket_info);
}

class BucketInfoReshardUpdate
{
  RGWRados *store;
  RGWBucketInfo bucket_info;
  std::map<string, bufferlist> bucket_attrs;

  bool in_progress{false};

  int set_status(cls_rgw_reshard_status s) {
    bucket_info.reshard_status = s;
    int ret = store->put_bucket_instance_info(bucket_info, false, real_time(), &bucket_attrs);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to write bucket info, ret=" << ret << dendl;
      return ret;
    }
    return 0;
  }

public:
  BucketInfoReshardUpdate(RGWRados *_store, RGWBucketInfo& _bucket_info,
                          map<string, bufferlist>& _bucket_attrs, const string& new_bucket_id) : store(_store), 
                                                                                                 bucket_info(_bucket_info),
                                                                                                 bucket_attrs(_bucket_attrs) {
    bucket_info.new_bucket_instance_id = new_bucket_id;
  }
  ~BucketInfoReshardUpdate() {
    if (in_progress) {
      bucket_info.new_bucket_instance_id.clear();
      set_status(CLS_RGW_RESHARD_NONE);
    }
  }

  int start() {
    int ret = set_status(CLS_RGW_RESHARD_IN_PROGRESS);
    if (ret < 0) {
      return ret;
    }
    in_progress = true;
    return 0;
  }

  int complete() {
    int ret = set_status(CLS_RGW_RESHARD_DONE);
    if (ret < 0) {
      return ret;
    }
    in_progress = false;
    return 0;
  }
};

int RGWBucketReshard::do_reshard(
		   int num_shards,
		   const RGWBucketInfo& new_bucket_info,
		   int max_entries,
                   bool verbose,
                   ostream *out,
		   Formatter *formatter)
{
  rgw_bucket& bucket = bucket_info.bucket;

  int ret = 0;

  if (out) {
    (*out) << "*** NOTICE: operation will not remove old bucket index objects ***" << std::endl;
    (*out) << "***         these will need to be removed manually             ***" << std::endl;
    (*out) << "tenant: " << bucket_info.bucket.tenant << std::endl;
    (*out) << "bucket name: " << bucket_info.bucket.name << std::endl;
    (*out) << "old bucket instance id: " << bucket_info.bucket.bucket_id << std::endl;
    (*out) << "new bucket instance id: " << new_bucket_info.bucket.bucket_id << std::endl;
  }

  /* update bucket info  -- in progress*/
  list<rgw_cls_bi_entry> entries;

  if (max_entries < 0) {
    ldout(store->ctx(), 0) << __func__ << ": can't reshard, negative max_entries" << dendl;
    return -EINVAL;
  }

  BucketInfoReshardUpdate bucket_info_updater(store, bucket_info, bucket_attrs, new_bucket_info.bucket.bucket_id);

  ret = bucket_info_updater.start();
  if (ret < 0) {
    ldout(store->ctx(), 0) << __func__ << ": failed to update bucket info ret=" << ret << dendl;
    return ret;
  }

  int num_target_shards = (new_bucket_info.num_shards > 0 ? new_bucket_info.num_shards : 1);

  BucketReshardManager target_shards_mgr(store, new_bucket_info, num_target_shards);

  verbose = verbose && (formatter != nullptr);

  if (verbose) {
    formatter->open_array_section("entries");
  }

  uint64_t total_entries = 0;

  if (!verbose) {
    cout << "total entries:";
  }

  int num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);
  string marker;
  for (int i = 0; i < num_source_shards; ++i) {
    bool is_truncated = true;
    marker.clear();
    while (is_truncated) {
      entries.clear();
      ret = store->bi_list(bucket, i, string(), marker, max_entries, &entries, &is_truncated);
      if (ret < 0 && ret != -ENOENT) {
	derr << "ERROR: bi_list(): " << cpp_strerror(-ret) << dendl;
	return -ret;
      }

      list<rgw_cls_bi_entry>::iterator iter;
      for (iter = entries.begin(); iter != entries.end(); ++iter) {
	rgw_cls_bi_entry& entry = *iter;
	if (verbose) {
	  formatter->open_object_section("entry");

	  encode_json("shard_id", i, formatter);
	  encode_json("num_entry", total_entries, formatter);
	  encode_json("entry", entry, formatter);
	}
	total_entries++;

	marker = entry.idx;

	int target_shard_id;
	cls_rgw_obj_key cls_key;
	uint8_t category;
	rgw_bucket_category_stats stats;
	bool account = entry.get_info(&cls_key, &category, &stats);
	rgw_obj_key key(cls_key);
	rgw_obj obj(new_bucket_info.bucket, key);
	int ret = store->get_target_shard_id(new_bucket_info, obj.get_hash_object(), &target_shard_id);
	if (ret < 0) {
	  lderr(store->ctx()) << "ERROR: get_target_shard_id() returned ret=" << ret << dendl;
	  return ret;
	}

	int shard_index = (target_shard_id > 0 ? target_shard_id : 0);

	ret = target_shards_mgr.add_entry(shard_index, entry, account, category, stats);
	if (ret < 0) {
	  return ret;
	}
	if (verbose) {
	  formatter->close_section();
	  if (out) {
	    formatter->flush(*out);
	    formatter->flush(*out);
	  }
	} else if (out && !(total_entries % 1000)) {
	  (*out) << " " << total_entries;
	}
      }
    }
  }
  if (verbose) {
    formatter->close_section();
    if (out) {
      formatter->flush(*out);
    }
  } else if (out) {
    (*out) << " " << total_entries << std::endl;
  }

  ret = target_shards_mgr.finish();
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to reshard" << dendl;
    return EIO;
  }

  RGWBucketAdminOpState bucket_op;

  bucket_op.set_bucket_name(new_bucket_info.bucket.name);
  bucket_op.set_bucket_id(new_bucket_info.bucket.bucket_id);
  bucket_op.set_user_id(new_bucket_info.owner);
  string err;
  int r = RGWBucketAdminOp::link(store, bucket_op, &err);
  if (r < 0) {
    lderr(store->ctx()) << "failed to link new bucket instance (bucket_id=" << new_bucket_info.bucket.bucket_id << ": " << err << "; " << cpp_strerror(-r) << ")" << dendl;
    return -r;
  }

  ret = bucket_info_updater.complete();
  if (ret < 0) {
    ldout(store->ctx(), 0) << __func__ << ": failed to update bucket info ret=" << ret << dendl;
    /* don't error out, reshard process succeeded */
  }
  return 0;
}

int RGWBucketReshard::get_status(list<cls_rgw_bucket_instance_entry> *status)
{
  librados::IoCtx index_ctx;
  map<int, string> bucket_objs;

  int r = store->open_bucket_index(bucket_info, index_ctx, bucket_objs);
  if (r < 0) {
    return r;
  }

  for (auto i : bucket_objs) {
    cls_rgw_bucket_instance_entry entry;

    int ret = cls_rgw_get_bucket_resharding(index_ctx, i.second, &entry);
    if (ret < 0 && ret != -ENOENT) {
      lderr(store->ctx()) << "ERROR: " << __func__ << ": cls_rgw_get_bucket_resharding() returned ret=" << ret << dendl;
      return ret;
    }

    status->push_back(entry);
  }

  return 0;
}

int RGWBucketReshard::execute(int num_shards, int max_op_entries,
                              bool verbose, ostream *out, Formatter *formatter, RGWReshard* reshard_log)

{
  int ret = lock_bucket();
  if (ret < 0) {
    return ret;
  }

  RGWBucketInfo new_bucket_info;
  ret = create_new_bucket_instance(num_shards, new_bucket_info);
  if (ret < 0) {
    unlock_bucket();
    return ret;
  }

  if (reshard_log) {
    ret = reshard_log->update(bucket_info, new_bucket_info);
    if (ret < 0) {
      unlock_bucket();
      return ret;
    }
  }

  ret = set_resharding_status(new_bucket_info.bucket.bucket_id, num_shards, CLS_RGW_RESHARD_IN_PROGRESS);
  if (ret < 0) {
    unlock_bucket();
    return ret;
  }

  ret = do_reshard(num_shards,
		   new_bucket_info,
		   max_op_entries,
                   verbose, out, formatter);

  if (ret < 0) {
    unlock_bucket();
    return ret;
  }

  ret = set_resharding_status(new_bucket_info.bucket.bucket_id, num_shards, CLS_RGW_RESHARD_DONE);
  if (ret < 0) {
    unlock_bucket();
    return ret;
  }

  unlock_bucket();

  return 0;
}


RGWReshard::RGWReshard(RGWRados* _store, bool _verbose, ostream *_out,
                       Formatter *_formatter) : store(_store), instance_lock(bucket_instance_lock_name),
                                                verbose(_verbose), out(_out), formatter(_formatter)
{
  num_logshards = store->ctx()->_conf->rgw_reshard_num_logs;
}

string RGWReshard::get_logshard_key(const string& tenant, const string& bucket_name)
{
  return tenant + ":" + bucket_name;
}

#define MAX_RESHARD_LOGSHARDS_PRIME 7877

void RGWReshard::get_bucket_logshard_oid(const string& tenant, const string& bucket_name, string *oid)
{
  string key = get_logshard_key(tenant, bucket_name);

  uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
  uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
  sid = sid2 % MAX_RESHARD_LOGSHARDS_PRIME % num_logshards;
  int logshard = sid % num_logshards;

  get_logshard_oid(logshard, oid);
}

int RGWReshard::add(cls_rgw_reshard_entry& entry)
{
  if (!store->can_reshard()) {
    ldout(store->ctx(), 20) << __func__ << " Resharding is disabled"  << dendl;
    return 0;
  }

  string logshard_oid;

  get_bucket_logshard_oid(entry.tenant, entry.bucket_name, &logshard_oid);

  librados::ObjectWriteOperation op;
  cls_rgw_reshard_add(op, entry);

  int ret = store->reshard_pool_ctx.operate(logshard_oid, &op);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to add entry to reshard log, oid=" << logshard_oid << " tenant=" << entry.tenant << " bucket=" << entry.bucket_name << dendl;
    return ret;
  }
  return 0;
}

int RGWReshard::update(const RGWBucketInfo& bucket_info, const RGWBucketInfo& new_bucket_info)
{
  cls_rgw_reshard_entry entry;
  entry.bucket_name = bucket_info.bucket.name;
  entry.bucket_id = bucket_info.bucket.bucket_id;

  int ret = get(entry);
  if (ret < 0) {
    return ret;
  }

  entry.new_instance_id = new_bucket_info.bucket.name + ":"  + new_bucket_info.bucket.bucket_id;

  ret = add(entry);
  if (ret < 0) {
    ldout(store->ctx(), 0) << __func__ << ":Error in updating entry bucket " << entry.bucket_name << ": " <<
      cpp_strerror(-ret) << dendl;
  }

  return ret;
}


int RGWReshard::list(int logshard_num, string& marker, uint32_t max, std::list<cls_rgw_reshard_entry>& entries, bool *is_truncated)
{
  string logshard_oid;

  get_logshard_oid(logshard_num, &logshard_oid);

  int ret = cls_rgw_reshard_list(store->reshard_pool_ctx, logshard_oid, marker, max, entries, is_truncated);

  if (ret < 0) {
    if (ret == -ENOENT) {
      *is_truncated = false;
      ret = 0;
    }
    lderr(store->ctx()) << "ERROR: failed to list reshard log entries, oid=" << logshard_oid << dendl;
    if (ret == -EACCES) {
      lderr(store->ctx()) << "access denied to pool " << store->get_zone_params().reshard_pool
                          << ". Fix the pool access permissions of your client" << dendl;
    }
  }

  return ret;
}

int RGWReshard::get(cls_rgw_reshard_entry& entry)
{
  string logshard_oid;

  get_bucket_logshard_oid(entry.tenant, entry.bucket_name, &logshard_oid);

  int ret = cls_rgw_reshard_get(store->reshard_pool_ctx, logshard_oid, entry);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to get entry from reshard log, oid=" << logshard_oid << " tenant=" << entry.tenant << " bucket=" << entry.bucket_name << dendl;
    return ret;
  }

  return 0;
}

int RGWReshard::remove(cls_rgw_reshard_entry& entry)
{
  string logshard_oid;

  get_bucket_logshard_oid(entry.tenant, entry.bucket_name, &logshard_oid);

  librados::ObjectWriteOperation op;
  cls_rgw_reshard_remove(op, entry);

  int ret = store->reshard_pool_ctx.operate(logshard_oid, &op);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to remove entry from reshard log, oid=" << logshard_oid << " tenant=" << entry.tenant << " bucket=" << entry.bucket_name << dendl;
    return ret;
  }

  return ret;
}

int RGWReshard::clear_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry)
{
  int ret = cls_rgw_clear_bucket_resharding(store->reshard_pool_ctx, bucket_instance_oid);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to clear bucket resharding, bucket_instance_oid=" << bucket_instance_oid << dendl;
    return ret;
  }

  return 0;
}

const int num_retries = 10;
const int default_reshard_sleep_duration = 5;

int RGWReshardWait::do_wait()
{
  Mutex::Locker l(lock);

  cond.WaitInterval(lock, utime_t(default_reshard_sleep_duration, 0));

  if (going_down) {
    return -ECANCELED;
  }

  return 0;
}

int RGWReshardWait::block_while_resharding(RGWRados::BucketShard *bs, string *new_bucket_id)
{
  int ret = 0;
  cls_rgw_bucket_instance_entry entry;

  for (int i=0; i < num_retries;i++) {
    ret = cls_rgw_get_bucket_resharding(bs->index_ctx, bs->bucket_obj, &entry);
    if (ret < 0) {
      ldout(store->ctx(), 0) << __func__ << " ERROR: failed to get bucket resharding :"  <<
	cpp_strerror(-ret)<< dendl;
      return ret;
    }
    if (!entry.resharding_in_progress()) {
      *new_bucket_id = entry.new_bucket_instance_id;
      return 0;
    }
    ldout(store->ctx(), 20) << "NOTICE: reshard still in progress; " << (i < num_retries - 1 ? "retrying" : "too many retries") << dendl;
    /* needed to unlock as clear resharding uses the same lock */

    if (i == num_retries - 1) {
      break;
    }

    ret = do_wait();
    if (ret < 0) {
      ldout(store->ctx(), 0) << __func__ << " ERROR: bucket is still resharding, please retry" << dendl;
      return ret;
    }
  }
  ldout(store->ctx(), 0) << __func__ << " ERROR: bucket is still resharding, please retry" << dendl;
  return -ERR_BUSY_RESHARDING;
}

int RGWReshard::process_single_logshard(int logshard_num)
{
  string marker;
  bool truncated = true;

  CephContext *cct = store->ctx();
  int max_entries = 1000;
  int max_secs = 60;

  rados::cls::lock::Lock l(reshard_lock_name);

  utime_t time(max_secs, 0);
  l.set_duration(time);

  char cookie_buf[COOKIE_LEN + 1];
  gen_rand_alphanumeric(store->ctx(), cookie_buf, sizeof(cookie_buf) - 1);
  cookie_buf[COOKIE_LEN] = '\0';

  l.set_cookie(cookie_buf);

  string logshard_oid;
  get_logshard_oid(logshard_num, &logshard_oid);

  int ret = l.lock_exclusive(&store->reshard_pool_ctx, logshard_oid);
  if (ret == -EBUSY) { /* already locked by another processor */
    ldout(store->ctx(), 5) << __func__ << "(): failed to acquire lock on " << logshard_oid << dendl;
    return ret;
  }

  utime_t lock_start_time = ceph_clock_now();

  do {
    std::list<cls_rgw_reshard_entry> entries;
    ret = list(logshard_num, marker, max_entries, entries, &truncated);
    if (ret < 0) {
      ldout(cct, 10) << "cannot list all reshards in logshard oid=" << logshard_oid << dendl;
      continue;
    }

    for(auto& entry: entries) {
      if(entry.new_instance_id.empty()) {

	ldout(store->ctx(), 20) << __func__ << " resharding " << entry.bucket_name  << dendl;

	RGWObjectCtx obj_ctx(store);
	rgw_bucket bucket;
	RGWBucketInfo bucket_info;
	map<string, bufferlist> attrs;

	ret = store->get_bucket_info(obj_ctx, entry.tenant, entry.bucket_name, bucket_info, nullptr,
                                   &attrs);
	if (ret < 0) {
	  ldout(cct, 0) <<  __func__ << ": Error in get_bucket_info: " << cpp_strerror(-ret) << dendl;
	  return -ret;
	}

	RGWBucketReshard br(store, bucket_info, attrs);

	Formatter* formatter = new JSONFormatter(false);
	auto formatter_ptr = std::unique_ptr<Formatter>(formatter);
	ret = br.execute(entry.new_num_shards, max_entries, true,nullptr, formatter, this);
	if (ret < 0) {
	  ldout (store->ctx(), 0) <<  __func__ << "ERROR in reshard_bucket " << entry.bucket_name << ":" <<
	    cpp_strerror(-ret)<< dendl;
	  return ret;
	}

	ldout (store->ctx(), 20) <<  " removing entry" << entry.bucket_name<< dendl;

      	ret = remove(entry);
	if (ret < 0) {
	  ldout(cct, 0)<< __func__ << ":Error removing bucket " << entry.bucket_name << " for resharding queue: "
		       << cpp_strerror(-ret) << dendl;
	  return ret;
	}
      }
      utime_t now = ceph_clock_now();

      if (now > lock_start_time + max_secs / 2) { /* do you need to renew lock? */
        l.set_renew(true);
        ret = l.lock_exclusive(&store->reshard_pool_ctx, logshard_oid);
        if (ret == -EBUSY) { /* already locked by another processor */
          ldout(store->ctx(), 5) << __func__ << "(): failed to acquire lock on " << logshard_oid << dendl;
          return ret;
        }
        lock_start_time = now;
      }
      entry.get_key(&marker);
    }
  } while (truncated);

  l.unlock(&store->reshard_pool_ctx, logshard_oid);
  return 0;
}


void  RGWReshard::get_logshard_oid(int shard_num, string *logshard)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%010u", (unsigned)shard_num);

  string objname(reshard_oid_prefix);
  *logshard =  objname + buf;
}

int RGWReshard::process_all_logshards()
{
  if (!store->can_reshard()) {
    ldout(store->ctx(), 20) << __func__ << " Resharding is disabled"  << dendl;
    return 0;
  }
  int ret = 0;

  for (int i = 0; i < num_logshards; i++) {
    string logshard;
    get_logshard_oid(i, &logshard);

    ldout(store->ctx(), 20) << "proceeding logshard = " << logshard << dendl;

    ret = process_single_logshard(i);
    if (ret <0) {
      return ret;
    }
  }

  return 0;
}

bool RGWReshard::going_down()
{
  return down_flag;
}

void RGWReshard::start_processor()
{
  worker = new ReshardWorker(store->ctx(), this);
  worker->create("rgw_reshard");
}

void RGWReshard::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = nullptr;
}

void *RGWReshard::ReshardWorker::entry() {
  utime_t last_run;
  do {
    utime_t start = ceph_clock_now();
    if (reshard->process_all_logshards()) {
      /* All shards have been processed properly. Next time we can start
       * from this moment. */
      last_run = start;
    }

    if (reshard->going_down())
      break;

    utime_t end = ceph_clock_now();
    end -= start;
    int secs = cct->_conf->rgw_reshard_thread_interval;

    if (secs <= end.sec())
      continue; // next round

    secs -= end.sec();

    lock.Lock();
    cond.WaitInterval(lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!reshard->going_down());

  return NULL;
}

void RGWReshard::ReshardWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}
