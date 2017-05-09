// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_rados.h"
#include "rgw_reshard.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "common/errno.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

const string reshard_oid = "reshard";
const string reshard_lock_name = "reshard_process";
const string bucket_instance_lock_name = "bucket_instance_lock";

RGWBucketReshard::RGWBucketReshard(RGWRados *_store, const RGWBucketInfo& _bucket_info) :
                                                     store(_store), bucket_info(_bucket_info),
                                                     reshard_lock(reshard_lock_name) {
  const rgw_bucket& b = bucket_info.bucket;                                                       
  reshard_oid = b.tenant + (b.tenant.empty() ? "" : ":") + b.name + ":" + b.bucket_id;
}

int RGWBucketReshard::lock_bucket()
{
#warning set timeout for guard lock

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

int RGWBucketReshard::init_resharding(const cls_rgw_reshard_entry& entry)
{
  if (entry.new_instance_id.empty()) {
    ldout(store->ctx(), 0) << __func__ << " missing new bucket instance id" << dendl;
    return -EINVAL;
  }

  cls_rgw_bucket_instance_entry instance_entry;
  instance_entry.new_bucket_instance_id = entry.new_instance_id;

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

RGWReshard::RGWReshard(CephContext *_cct, RGWRados* _store):cct(_cct), store(_store),
							    instance_lock(bucket_instance_lock_name)
{
    max_jobs = cct->_conf->rgw_reshard_max_jobs;
}


int RGWReshard::add(cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(reshard_lock_name);

  int ret = l.lock_exclusive(&store->reshard_pool_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(cct, 0) << "RGWReshard::add failed to acquire lock on " << reshard_oid << dendl;
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
    ldout(cct, 0) << "RGWReshard::list failed to acquire lock on " << reshard_oid << dendl;
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
    ldout(cct, 0) << "RGWReshardLog::get failed to acquire lock on " << reshard_oid << dendl;
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
    ldout(cct, 0) << "RGWReshardLog::remove failed to acquire lock on " << reshard_oid << dendl;
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
    ldout(cct, 0) << "RGWReshardLog::add failed to acquire lock on " << reshard_oid << dendl;
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
    ldout(cct, 0) << "RGWReshardLog::add failed to acquire lock on " << reshard_oid << dendl;
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
const int default_reshard_sleep_duration = 30;

int RGWReshard::block_while_resharding(const string& bucket_instance_oid,
				       BucketIndexLockGuard& guard)
{
  int ret = 0;
  cls_rgw_bucket_instance_entry entry;

  for (int i=0; i< num_retries;i++) {
    ret = guard.lock();
    if (ret < 0) {
      return ret;
    }

    ret = cls_rgw_get_bucket_resharding(store->reshard_pool_ctx, bucket_instance_oid, &entry);
    if (ret < 0) {
      ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: failed to get bucket resharding :"  <<
	cpp_strerror(-ret)<< dendl;
      return ret;
    }

    ret = guard.unlock();
    if (ret < 0) {
      return ret;
    }
    if (!entry.resharding) {
      return 0;
    }
    /* needed to unlock as clear resharding uses the same lock */
    sleep(default_reshard_sleep_duration);
  }
  ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: bucket is still resharding, please retry" << dendl;
  return -EAGAIN;
}

BucketIndexLockGuard::BucketIndexLockGuard(CephContext* _cct, RGWRados* _store,
					   const string& bucket_instance_id, const string& _oid, const librados::IoCtx& _io_ctx) :
  cct(_cct),store(_store),
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
