

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include <atomic>

#include "common/Thread.h"
#include "common/Mutex.h"
#include "common/Cond.h"

class CephContext;
class RGWRados;

class RGWRadosThread {
  class Worker : public Thread {
    CephContext *cct;
    RGWRadosThread *processor;
    Mutex lock;
    Cond cond;

    void wait() {
      Mutex::Locker l(lock);
      cond.Wait(lock);
    };

    void wait_interval(const utime_t& wait_time) {
      Mutex::Locker l(lock);
      cond.WaitInterval(lock, wait_time);
    }

  public:
    Worker(CephContext *_cct, RGWRadosThread *_p) : cct(_cct), processor(_p), lock("RGWRadosThread::Worker") {}
    void *entry() override;
    void signal() {
      Mutex::Locker l(lock);
      cond.Signal();
    }
  };

  Worker *worker;

protected:
  CephContext *cct;
  RGWRados *store;

  std::atomic<bool> down_flag = { false };

  std::string thread_name;

  virtual uint64_t interval_msec() = 0;
  virtual void stop_process() {}
public:
  RGWRadosThread(RGWRados *_store, const std::string& thread_name = "radosgw") 
    : worker(NULL), cct(_store->ctx()), store(_store), thread_name(thread_name) {}
  virtual ~RGWRadosThread() {
    stop();
  }

  virtual int init() { return 0; }
  virtual int process() = 0;

  bool going_down() { return down_flag; }

  void start();
  void stop();

  void signal() {
    if (worker) {
      worker->signal();
    }
  }
};

