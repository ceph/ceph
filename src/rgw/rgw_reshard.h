// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RGW_RESHARD_H
#define RGW_RESHARD_H

#include <vector>
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_types.h"
#include "cls/lock/cls_lock_client.h"
#include "rgw_bucket.h"

class CephContext;
class RGWRados;


#if 0
/* gets a locked lock , release it when exiting context */
class BucketIndexLockGuard
{
   RGWRados *store;
   rados::cls::lock::Lock l;
   string oid;
   librados::IoCtx io_ctx;
   bool locked;

public:
  BucketIndexLockGuard(RGWRados* store, const string& bucket_instance_id,
		                         const string& oid, const librados::IoCtx& io_ctx);
  /* unlocks the lock */
  ~BucketIndexLockGuard();
protected:
  friend class RGWReshard;
  int lock();
  int unlock();
};
#endif


class RGWBucketReshard {
  RGWRados *store;
  RGWBucketInfo bucket_info;
  std::map<string, bufferlist> bucket_attrs;

  string reshard_oid;
  rados::cls::lock::Lock reshard_lock;

  int lock_bucket();
  void unlock_bucket();
  int set_resharding_status(const string& new_instance_id, int32_t num_shards, cls_rgw_reshard_status status);
  int clear_resharding();

  int create_new_bucket_instance(int new_num_shards,
                                 RGWBucketInfo& new_bucket_info);
  int do_reshard(int num_shards,
		 const RGWBucketInfo& new_bucket_info,
		 int max_entries,
                 bool verbose,
                 ostream *os,
		 Formatter *formatter);
public:
  RGWBucketReshard(RGWRados *_store, const RGWBucketInfo& _bucket_info,
                   const std::map<string, bufferlist>& _bucket_attrs);

  int execute(int num_shards, int max_op_entries,
              bool verbose = false, ostream *out = nullptr,
              Formatter *formatter = nullptr);
  int abort();
  int get_status(std::list<cls_rgw_bucket_instance_entry> *status);
};

class RGWReshard {
    RGWRados *store;
    string lock_name;
    rados::cls::lock::Lock instance_lock;
    int num_logshards;

    void get_logshard_oid(int shard_num, string *shard);
protected:
  class ReshardWorker : public Thread {
    CephContext *cct;
    RGWReshard *reshard;
    Mutex lock;
    Cond cond;

  public:
    ReshardWorker(CephContext * const _cct,
		              RGWReshard * const _reshard)
      : cct(_cct),
        reshard(_reshard),
        lock("ReshardWorker") {
    }

    void *entry() override;
    void stop();
  };

  ReshardWorker *worker;
  std::atomic<bool> down_flag = { false };

  string get_logshard_key(const string& tenant, const string& bucket_name);
  void get_bucket_logshard_oid(const string& tenant, const string& bucket_name, string *oid);

public:
  RGWReshard(RGWRados* _store);
  int add(cls_rgw_reshard_entry& entry);
  int get(cls_rgw_reshard_entry& entry);
  int remove(cls_rgw_reshard_entry& entry);
  int list(int logshard_num, string& marker, uint32_t max, std::list<cls_rgw_reshard_entry>& entries, bool *is_truncated);
  int clear_bucket_resharding(const string& bucket_instance_oid, cls_rgw_reshard_entry& entry);

  int reshard_bucket(Formatter *formatter,
                     int num_shards,
                     rgw_bucket& bucket,
                     RGWBucketInfo& bucket_info,
                     RGWBucketInfo& new_bucket_info,
                     int max_entries,
                     RGWBucketAdminOpState& bucket_op,
                     bool verbose = false);

  /* reshard thread */
  int process_single_logshard(int logshard_num);
  int inspect_all_logshards();
  bool going_down();
  void start_processor();
  void stop_processor();
};


class RGWReshardWait {
  RGWRados *store;
  Mutex lock{"RGWReshardWait::lock"};
  Cond cond;

  bool going_down{false};

  int do_wait();
public:
  RGWReshardWait(RGWRados *_store) : store(_store) {}
  ~RGWReshardWait() {
    assert(going_down);
  }
  int block_while_resharding(RGWRados::BucketShard *bs, string *new_bucket_id);

  void stop() {
    Mutex::Locker l(lock);
    going_down = true;
    cond.SignalAll();
  }
};

#endif
