// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OBJEXP_H
#define CEPH_OBJEXP_H

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

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

#include "global/global_init.h"

#include "include/utime.h"
#include "include/str_list.h"

#include "rgw_user.h"
#include "rgw_bucket.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_log.h"
#include "rgw_formats.h"
#include "rgw_usage.h"
#include "rgw_replica_log.h"

class RGWObjectExpirer {
protected:
  RGWRados *store;

  int init_bucket_info(const std::string& tenant_name,
                       const std::string& bucket_name,
                       const std::string& bucket_id,
                       RGWBucketInfo& bucket_info);

  class OEWorker : public Thread {
    CephContext *cct;
    RGWObjectExpirer *oe;
    Mutex lock;
    Cond cond;

  public:
    OEWorker(CephContext * const cct,
             RGWObjectExpirer * const oe)
      : cct(cct),
        oe(oe),
        lock("OEWorker") {
    }

    void *entry();
    void stop();
  };

  OEWorker *worker;
  atomic_t down_flag;

public:
  explicit RGWObjectExpirer(RGWRados *_store)
    : store(_store) {
  }

  int garbage_single_object(objexp_hint_entry& hint);

  void garbage_chunk(std::list<cls_timeindex_entry>& entries, /* in  */
                     bool& need_trim);                        /* out */

  void trim_chunk(const std::string& shard,
                  const utime_t& from,
                  const utime_t& to,
                  const string& from_marker,
                  const string& to_marker);

  bool process_single_shard(const std::string& shard,
                            const utime_t& last_run,
                            const utime_t& round_start);

  bool inspect_all_shards(const utime_t& last_run,
                          const utime_t& round_start);

  bool going_down();
  void start_processor();
  void stop_processor();
};
#endif /* CEPH_OBJEXP_H */
