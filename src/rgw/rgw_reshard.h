// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_RESHARD_H
#define RGW_RESHARD_H

#include <vector>
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/lock/cls_lock_client.h"

class CephContext;
class RGWRados;


/* gets a locked lock , release it when exiting context */
class BucketIndexLockGuard
{
   CephContext *cct;
   RGWRados *store;
   rados::cls::lock::Lock l;
   string oid;
   librados::IoCtx io_ctx;
   bool locked;
public:
  BucketIndexLockGuard(CephContext* cct, RGWRados* store, const string& bucket_instance_id,
		                         const string& oid, const librados::IoCtx& io_ctx);
  /* unlocks the lock */
  ~BucketIndexLockGuard();
protected:
  friend class RGWReshard;
  int lock();
  int unlock();
};

class RGWReshard {
    CephContext *cct;
    RGWRados *store;
    string lock_name;
    int max_jobs;
    rados::cls::lock::Lock instance_lock;

    int lock_bucket_index_shared(const string& oid);
    int unlock_bucket_index(const string& oid);

  public:
    RGWReshard(CephContext* cct, RGWRados* _store);
    int add(cls_rgw_reshard_entry& entry);
    int get(cls_rgw_reshard_entry& entry);
    int remove(cls_rgw_reshard_entry& entry);
    int list(string& marker, uint32_t max, list<cls_rgw_reshard_entry>& entries, bool& is_truncated);
    int set_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry);
    int clear_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry);
    /*
      if succefull, keeps the bucket index locked. It will be unlocked
      in the guard dtor.
     */
    int block_while_resharding(const string& bucket_instance_oid, BucketIndexLockGuard& guard);
};

#endif
