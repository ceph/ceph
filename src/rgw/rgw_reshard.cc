// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rados.h"
#include "rgw_bucket.h"
#include "rgw_reshard.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "common/errno.h"
#include "common/ceph_json.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

const string reshard_oid = "reshard";
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
      cerr << "ERROR: reshard rados operation failed: " << cpp_strerror(-ret) << std::endl;
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
      std::cerr << "ERROR: failed to store entries in target bucket shard (bs=" << bs.bucket << "/" << bs.shard_id << ") error=" << cpp_strerror(-ret) << std::endl;
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
      cerr << "ERROR: target_shards.add_entry(" << entry.idx << ") returned error: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    return 0;
  }

  int finish() {
    int ret = 0;
    for (auto& shard : target_shards) {
      int r = shard->flush();
      if (r < 0) {
        cerr << "ERROR: target_shards[" << shard->get_num_shard() << "].flush() returned error: " << cpp_strerror(-r) << std::endl;
        ret = r;
      }
    }
    for (auto& shard : target_shards) {
      int r = shard->wait_all_aio();
      if (r < 0) {
        cerr << "ERROR: target_shards[" << shard->get_num_shard() << "].wait_all_aio() returned error: " << cpp_strerror(-r) << std::endl;
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

int RGWBucketReshard::create_new_bucket_instance(int new_num_shards,
                                                 RGWBucketInfo& new_bucket_info)
{
  new_bucket_info = bucket_info;

  store->create_bucket_id(&new_bucket_info.bucket.bucket_id);
  new_bucket_info.bucket.oid.clear();

  new_bucket_info.num_shards = new_num_shards;
  new_bucket_info.objv_tracker.clear();

  int ret = store->init_bucket_index(new_bucket_info, new_bucket_info.num_shards);
  if (ret < 0) {
    cerr << "ERROR: failed to init new bucket indexes: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  ret = store->put_bucket_instance_info(new_bucket_info, true, real_time(), &bucket_attrs);
  if (ret < 0) {
    cerr << "ERROR: failed to store new bucket instance info: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  return 0;
}

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
    (*out) << "old bucket instance id: " << bucket_info.bucket.bucket_id << std::endl;
    (*out) << "new bucket instance id: " << new_bucket_info.bucket.bucket_id << std::endl;
  }

  list<rgw_cls_bi_entry> entries;

  if (max_entries < 0) {
    ldout(store->ctx(), 0) << __func__ << ": can't reshard, negative max_entries" << dendl;
    return -EINVAL;
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
      if (ret < 0) {
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
	  formatter->flush(*out);
	  formatter->flush(*out);
	} else if (out && !(total_entries % 1000)) {
	  (*out) << " " << total_entries;
	}
      }
    }
  }
  if (verbose) {
    formatter->close_section();
    formatter->flush(*out);
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
                              bool verbose, ostream *out, Formatter *formatter)

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

  ret = set_resharding_status(new_bucket_info.bucket.bucket_id, num_shards, CLS_RGW_RESHARD_IN_PROGRESS);
  if (ret < 0) {
    unlock_bucket();
    return ret;
  }

#warning remove me
sleep(10);

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


RGWReshard::RGWReshard(RGWRados* _store): store(_store), instance_lock(bucket_instance_lock_name)
{
  max_jobs = store->ctx()->_conf->rgw_reshard_max_jobs;
}


int RGWReshard::add(cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(reshard_lock_name);

  int ret = l.lock_exclusive(&store->reshard_pool_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(store->ctx(), 0) << "RGWReshard::add failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  librados::ObjectWriteOperation op;
  cls_rgw_reshard_add(op, entry);

  ret = store->reshard_pool_ctx.operate(reshard_oid, &op);

  l.unlock(&store->reshard_pool_ctx, reshard_oid);
  return ret;
}


int RGWReshard::list(string& marker, uint32_t max, std::list<cls_rgw_reshard_entry>& entries, bool& is_truncated)
{
  rados::cls::lock::Lock l(reshard_lock_name);

  int ret = l.lock_shared(&store->reshard_pool_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(store->ctx(), 0) << "RGWReshard::list failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  ret =  cls_rgw_reshard_list(store->reshard_pool_ctx, reshard_oid, marker, max, entries, &is_truncated);

  l.unlock(&store->reshard_pool_ctx, reshard_oid);
  return ret;
}

int RGWReshard::get(cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(reshard_lock_name);

  int ret = l.lock_shared(&store->reshard_pool_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(store->ctx(), 0) << "RGWReshardLog::get failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  ret = cls_rgw_reshard_get(store->reshard_pool_ctx, reshard_oid, entry);

  l.unlock(&store->reshard_pool_ctx, reshard_oid);
  return ret;
}

int RGWReshard::remove(cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(reshard_lock_name);

  int ret = l.lock_exclusive(&store->reshard_pool_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(store->ctx(), 0) << "RGWReshardLog::remove failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  librados::ObjectWriteOperation op;
  cls_rgw_reshard_remove(op, entry);

  ret =  store->reshard_pool_ctx.operate(reshard_oid, &op);

  l.unlock(&store->reshard_pool_ctx, reshard_oid);
  return ret;
}

std::string create_bucket_index_lock_name(const string& bucket_instance_id) {
  return bucket_instance_lock_name + "." + bucket_instance_id;
}

int RGWReshard::clear_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(create_bucket_index_lock_name(entry.old_instance_id));

  int ret = l.lock_exclusive(&store->reshard_pool_ctx, bucket_instance_oid);
  if (ret == -EBUSY) {
    ldout(store->ctx(), 0) << "RGWReshardLog::add failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  entry.new_instance_id.clear();

  ret = cls_rgw_clear_bucket_resharding(store->reshard_pool_ctx, bucket_instance_oid);

  l.unlock(&store->reshard_pool_ctx, bucket_instance_oid);
  return ret;
}

int RGWReshard::lock_bucket_index_shared(const string& oid)
{
  int ret = instance_lock.lock_shared(&store->reshard_pool_ctx, oid);
  if (ret == -EBUSY) {
    ldout(store->ctx(), 0) << "RGWReshardLog::add failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }

  return ret;
}

int RGWReshard::unlock_bucket_index(const string& oid)
{
  instance_lock.unlock(&store->reshard_pool_ctx, oid);
  return 0;
}

const int num_retries = 10;
const int default_reshard_sleep_duration = 5;

int RGWReshard::block_while_resharding(RGWRados::BucketShard *bs, string *new_bucket_id)
{
  int ret = 0;
  cls_rgw_bucket_instance_entry entry;

  for (int i=0; i< num_retries;i++) {
    ret = cls_rgw_get_bucket_resharding(bs->index_ctx, bs->bucket_obj, &entry);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "RGWReshard::" << __func__ << " ERROR: failed to get bucket resharding :"  <<
	cpp_strerror(-ret)<< dendl;
      return ret;
    }
    if (!entry.resharding_in_progress()) {
      *new_bucket_id = entry.new_bucket_instance_id;
      return 0;
    }
    ldout(store->ctx(), 20) << "NOTICE: reshard still in progress; " << (i < num_retries - 1 ? "retrying" : "too many retries") << dendl;
    /* needed to unlock as clear resharding uses the same lock */
#warning replace sleep with interruptible condition
    sleep(default_reshard_sleep_duration);
  }
  ldout(store->ctx(), 0) << "RGWReshard::" << __func__ << " ERROR: bucket is still resharding, please retry" << dendl;
  return -ERR_BUSY_RESHARDING;
}

BucketIndexLockGuard::BucketIndexLockGuard(CephContext *_cct, RGWRados* _store, const string& bucket_instance_id, const string& _oid, const librados::IoCtx& _io_ctx) :
  cct(_cct), store(_store),
  l(create_bucket_index_lock_name(bucket_instance_id)),
  oid(_oid), io_ctx(_io_ctx),locked(false)
{
}

int BucketIndexLockGuard::lock()
{
  if (!locked) {
    int ret = l.lock_shared(&store->reshard_pool_ctx, oid);
    if (ret == -EBUSY) {
      ldout(cct,0) << "RGWReshardLog::add failed to acquire lock on " << oid << dendl;
      return 0;
    }
    if (ret < 0) {
      return ret;
    }
    locked = true;
    return ret;
  } else {
    ldout(cct,0) << " % alread lock" << oid << dendl;
    return -EBUSY;
  }
}

int BucketIndexLockGuard::unlock()
{
  if (locked) {
    int ret = l.unlock(&io_ctx, oid);
    if (ret <0) {
      ldout(cct, 0) << "failed to unlock " << oid << dendl;
    } else {
      locked = false;
    }
    return ret;
  }
  return 0;
}

BucketIndexLockGuard::~BucketIndexLockGuard()
{
  unlock();
}
