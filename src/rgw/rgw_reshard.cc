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

RGWReshard::RGWReshard(CephContext *_cct, RGWRados* _store):cct(_cct), store(_store),
							    instance_lock(bucket_instance_lock_name)
{
    max_jobs = cct->_conf->rgw_reshard_max_jobs;
}

int RGWReshard::get_io_ctx(librados::IoCtx& io_ctx)
{
  string reshard_pool = store->get_zone_params().reshard_pool.name;
  librados::Rados *rad = store->get_rados_handle();
  int r = rad->ioctx_create(reshard_pool.c_str(), io_ctx);
  if (r == -ENOENT) {
    r = store->create_pool(store->get_zone_params().reshard_pool);
    if (r < 0)
      return r;

    // retry
    r = rad->ioctx_create(reshard_pool.c_str(), io_ctx);
  }
  if (r < 0)
    return r;

  return 0;
}

int RGWReshard::add(cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(reshard_lock_name);
  librados::IoCtx io_ctx;

  int ret = get_io_ctx(io_ctx);
  if (ret < 0)
    return ret;

  ret = l.lock_exclusive(&io_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(cct, 0) << "RGWReshard::add failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  librados::ObjectWriteOperation op;
  cls_rgw_reshard_add(op, entry);

  ret = io_ctx.operate(reshard_oid, &op);

  l.unlock(&io_ctx, reshard_oid);
  return ret;
}


int RGWReshard::list(string& marker, uint32_t max, std::list<cls_rgw_reshard_entry>& entries, bool& is_truncated)
{
  rados::cls::lock::Lock l(reshard_lock_name);
  librados::IoCtx io_ctx;

  int ret = get_io_ctx(io_ctx);
  if (ret < 0)
    return ret;

  ret = l.lock_shared(&io_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(cct, 0) << "RGWReshard::list failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  ret =  cls_rgw_reshard_list(io_ctx, reshard_oid, marker, max, entries, &is_truncated);

  l.unlock(&io_ctx, reshard_oid);
  return ret;
}

int RGWReshard::get(cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(reshard_lock_name);
  librados::IoCtx io_ctx;

  int ret = get_io_ctx(io_ctx);
  if (ret < 0)
    return ret;

  ret = l.lock_shared(&io_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(cct, 0) << "RGWReshardLog::get failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  ret = cls_rgw_reshard_get(io_ctx, reshard_oid, entry);

  l.unlock(&io_ctx, reshard_oid);
  return ret;
}

int RGWReshard::remove(cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(reshard_lock_name);
  librados::IoCtx io_ctx;

  int ret = get_io_ctx(io_ctx);
  if (ret < 0)
    return ret;

  ret = l.lock_exclusive(&io_ctx, reshard_oid);
  if (ret == -EBUSY) {
    ldout(cct, 0) << "RGWReshardLog::remove failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  librados::ObjectWriteOperation op;
  cls_rgw_reshard_remove(op, entry);

  ret =  io_ctx.operate(reshard_oid, &op);

  l.unlock(&io_ctx, reshard_oid);
  return ret;
}

int RGWReshard::set_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(reshard_lock_name);
  librados::IoCtx io_ctx;

  int ret = get_io_ctx(io_ctx);
  if (ret < 0)
    return ret;

  if (entry.new_instance_id.empty()) {
    ldout(cct, 0) << "RGWReshard::" << __func__ << " missing new bucket instance id" << dendl;
    return -EEXIST;
  }

  ret = l.lock_exclusive(&io_ctx, bucket_instance_oid);
  if (ret == -EBUSY) {
    ldout(cct, 0) << "RGWReshardLog::add failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  RGWBucketInfo new_bucket_info;
  map<string, bufferlist> attrs;
  RGWObjectCtx obj_ctx(store);
  cls_rgw_bucket_instance_entry instance_entry;

  ret = store->get_bucket_info(obj_ctx, entry.tenant, entry.bucket_name, new_bucket_info, nullptr, &attrs);
  if (ret < 0) {
    ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: could not init bucket: " << cpp_strerror(-ret)
		  << dendl;
    return ret;
  }

  new_bucket_info.new_bucket_instance_id = entry.new_instance_id;
  new_bucket_info.resharding = true;
  instance_entry.new_bucket_instance_id = entry.new_instance_id;

  try {
    ::encode(new_bucket_info, instance_entry.data);
  } catch(buffer::error& err) {
    ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: could not decode buffer info, caught buffer::error"
		  << dendl;
    ret = -EIO;
    goto done;
  }

  ret =   cls_rgw_set_bucket_resharding(io_ctx, bucket_instance_oid, instance_entry);
  if (ret < 0) {
    ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: cls_rgw_set_bucket_reshardind: "
		  << cpp_strerror(-ret) << dendl;
    goto done;
  }
done:
  l.unlock(&io_ctx, bucket_instance_oid);
  return ret;
}

int RGWReshard::clear_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry)
{
  rados::cls::lock::Lock l(bucket_instance_lock_name);
  librados::IoCtx io_ctx;

  int ret = get_io_ctx(io_ctx);
  if (ret < 0)
    return ret;

  ret = l.lock_exclusive(&io_ctx, bucket_instance_oid);
  if (ret == -EBUSY) {
    ldout(cct, 0) << "RGWReshardLog::add failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  entry.new_instance_id.clear();

  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
  RGWObjectCtx obj_ctx(store);

  ret = store->get_bucket_info(obj_ctx, entry.tenant, entry.bucket_name, bucket_info, nullptr, &attrs);
  if (ret < 0) {
    ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: could not init bucket: " << cpp_strerror(-ret)
		  << dendl;
    return ret;
  }

  bucket_info.resharding = false;
  bucket_info.new_bucket_instance_id.clear();
  cls_rgw_bucket_instance_entry instance_entry;

  try {
    ::encode(bucket_info, instance_entry.data);
  } catch(buffer::error& err) {
    ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: could not decode buffer info, caught buffer::error"
		  << dendl;
    l.unlock(&io_ctx, bucket_instance_oid);
    return -EIO;
  }

  ret =   cls_rgw_clear_bucket_resharding(io_ctx, bucket_instance_oid, instance_entry);

  l.unlock(&io_ctx, bucket_instance_oid);
  return ret;
}

int RGWReshard::lock_bucket_index_shared(const string& oid)
{
  int ret = get_io_ctx(io_ctx);
  if (ret < 0)
    return ret;

  ret = instance_lock.lock_shared(&io_ctx, oid);
  if (ret == -EBUSY) {
    ldout(cct, 0) << "RGWReshardLog::add failed to acquire lock on " << reshard_oid << dendl;
    return 0;
  }

  return ret;
}

int RGWReshard::unlock_bucket_index(const string& oid)
{
  int ret = get_io_ctx(io_ctx);
  if (ret < 0)
    return ret;
  instance_lock.unlock(&io_ctx, oid);
  return 0;
}


const int num_retries = 10;
const int default_reshard_sleep_duration = 30;

int RGWReshard::block_while_resharding(const string& bucket_instance_oid)
{
  int ret = 0;
  cls_rgw_bucket_instance_entry entry;
  bool resharding = false;

  for (int i=0; i< num_retries;i++) {
    ret = lock_bucket_index_shared(bucket_instance_oid);
    if (ret < 0) {
      return ret;
    }

    ret = cls_rgw_get_bucket_resharding(io_ctx, bucket_instance_oid,
					entry, resharding);
    if (ret < 0 && ret != -ENOENT && ret != -ENODATA) {
      ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: failed to get bucket resharding :"  <<
	cpp_strerror(-ret)<< dendl;
      return ret;
    }

    if (resharding) {
      ret = unlock_bucket_index(bucket_instance_oid);
      if (ret < 0) {
	return ret;
      }
      sleep(default_reshard_sleep_duration);
    } else {
      return 0;
    }
  }
  ldout(cct, 0) << "RGWReshard::" << __func__ << " ERROR: bucket is still resharding, please retry" << dendl;
  ret = unlock_bucket_index(bucket_instance_oid);
  if (ret < 0) {
    return ret;
  }
  return -EAGAIN;
}
