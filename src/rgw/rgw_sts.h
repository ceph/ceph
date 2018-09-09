#ifndef CEPH_RGW_STS_H
#define CEPH_RGW_STS_H

#include <map>
#include <string>
#include <iostream>

#include "common/debug.h"

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/iso_8601.h"
#include "common/Thread.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_multi.h"
#include "cls/rgw/cls_rgw_types.h"
#include "rgw_tag.h"

#include <atomic>

#define HASH_PRIME 7877
#define MAX_ID_LEN 255
static string sts_oid_prefix = "sts";
static string sts_index_lock_name = "sts_process";

class RGWSTS {
  CephContext *cct;
  RGWRados *store;
  int max_objs{0};
  string *obj_names{nullptr};
  std::atomic<bool> down_flag = { false };
  string cookie;

  class STSWorker : public Thread {
    CephContext *cct;
    RGWSTS *sts;
    Mutex lock;
    Cond cond;

  public:
    STSWorker(CephContext *_cct, RGWSTS *_sts) : cct(_cct), sts(_sts), lock("STSWorker") {}
    void *entry() override;
    void stop();
    bool should_work(utime_t& now);
    int schedule_next_start_time(utime_t& start, utime_t& now);
  };
  
  public:
  STSWorker *worker;
  RGWSTS() : cct(NULL), store(NULL), worker(NULL) {}
  ~RGWSTS() {
    stop_processor();
    finalize();
  }

  void initialize(CephContext *_cct, RGWRados *_store);
  void finalize();

  int process();
  int process(int index, int max_secs);
  bool if_already_run_today(time_t& start_date);
  int list_sts_progress(const string& marker, uint32_t max_entries, map<string, int> *progress_map);
  int user_sts_process(const string& access_key, int64_t expire);
  int user_sts_post(int index, int max_lock_sec, pair<string, int >& entry, int& result);
  bool going_down();
  void start_processor();
  void stop_processor();
};

#endif
