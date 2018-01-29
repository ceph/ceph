// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_GC_H
#define CEPH_RGW_GC_H


#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "common/WorkQueue.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "cls/rgw/cls_rgw_types.h"
#include <list>
#include <mutex>
#include <atomic>

class RGWGC {
  CephContext *cct;
  RGWRados *store;
  int max_objs;
  string *obj_names;
  std::atomic<bool> down_flag = { false };
  ThreadPool tp;

  int tag_index(const string& tag);

  class GCWorker : public Thread {
    CephContext *cct;
    RGWGC *gc;
    Mutex lock;
    Cond cond;

  public:
    GCWorker(CephContext *_cct, RGWGC *_gc) : cct(_cct), gc(_gc), lock("GCWorker") {}
    void *entry() override;
    void stop();
  };

  GCWorker *worker;

public:
  struct GCJob {
    cls_rgw_gc_obj_info *info;
    utime_t end;

    std::list<string> *remove_tags;
    std::mutex *lock;
    GCJob(cls_rgw_gc_obj_info *_info, utime_t _end, std::list<string> *_remove_tags, std::mutex *_lock)
      :info(_info), end(_end), remove_tags(_remove_tags), lock(_lock){}
  };

  class RGWGCWQ : public ThreadPool::WorkQueue<GCJob> {
    RGWGC *gc;
    std::list<GCJob*> job_queue;
  public:
    RGWGCWQ(const std::string &name, ThreadPool *tp, RGWGC *_gc)
    : ThreadPool::WorkQueue<GCJob>(name, 0, 0, tp), gc(_gc) {}
    ~RGWGCWQ(){ clear(); }

    bool _enqueue(GCJob *item) override{
      job_queue.push_back(item);
      return true;
    }
    void _dequeue(GCJob *item) override {
      if (job_queue.empty()) {
        item = nullptr;
        return;
      }
      item = job_queue.front();
      job_queue.pop_front();
    }
    GCJob *_dequeue() override {
      if (job_queue.empty())
        return nullptr;
      GCJob *item = job_queue.front();
      job_queue.pop_front();
      return item;
    }
    bool _empty() override {
      return job_queue.empty();
    }
    void _clear() override {
      while(!job_queue.empty()){
        GCJob *item = job_queue.front();
        job_queue.pop_front();
        if(item) delete item;
      }
    }
    void _process(GCJob *item, ThreadPool::TPHandle &tp_handle) override;
    void _process_finish(GCJob *item) override {
      if(item) delete item;
    }
  };

private:
  RGWGCWQ gc_wq;
  friend class RGWGCWQ;

public:
  RGWGC(CephContext *_cct, RGWRados *_store, const int num_threads)
    : cct(_cct), store(_store),
      max_objs(0), obj_names(NULL),
      tp(cct, "RGWGC::tp", "tp_gc_process", num_threads),
      worker(NULL), gc_wq("RGWGCWQ", &tp, this) {}
  ~RGWGC() {
    stop_processor();
    finalize();
  }

  void add_chain(librados::ObjectWriteOperation& op, cls_rgw_obj_chain& chain, const string& tag);
  int send_chain(cls_rgw_obj_chain& chain, const string& tag, bool sync);
  int defer_chain(const string& tag, bool sync);
  int remove(int index, const std::list<string>& tags);

  void initialize();
  void finalize();

  int list(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated);
  void list_init(int *index) { *index = 0; }
  int process(int index, int process_max_secs, bool expired_only);
  int process(bool expired_only);

  bool going_down();
  void start_processor();
  void stop_processor();
};


#endif
