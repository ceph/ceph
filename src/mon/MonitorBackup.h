// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2021 B1-Systems GmbH
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/


#ifndef CEPH_MONITOR_BACKUP_H
#define CEPH_MONITOR_BACKUP_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>


#include "common/Thread.h"
#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "kv/KeyValueDB.h"
#include "mon/MonitorDBStore.h"

class Monitor;

class MonitorBackupManager : public Thread {
  CephContext *cct;
  Monitor *mon;
  ceph::mutex mutex;
  ceph::condition_variable work_cond;
  bool should_stop{false};
  // set by tick(); a sticky flag so a notification delivered before the
  // worker enters wait() is not lost. cleared each time the worker
  // re-evaluates timer triggers.
  bool wakeup_pending{false};

  bool should_backup{false};
  bool should_cleanup{false};
  uint64_t last_job_id{0};
  std::shared_ptr<KeyValueDB::BackupCleanupStats> last_cleanup;
  std::shared_ptr<KeyValueDB::BackupStats> last_backup;

  void do_backup();
  void do_cleanup();
  bool check_free_space();
  void record_last_backup(std::shared_ptr<KeyValueDB::BackupStats> stats);
protected:
  void *entry() override;

public:
  explicit MonitorBackupManager(CephContext *cct, Monitor *monitor) :
    cct(cct),
    mon(monitor),
    mutex(ceph::make_mutex("mon::BackupManager::mutex")) {
      create("mon::backups");
  }

  void tick() {
    std::lock_guard guard{mutex};
    if (should_stop) {
      return;
    }
    wakeup_pending = true;
    work_cond.notify_one();
  }

  /**
   * Stop the backup manager thread. Safe to call more than once.
   **/
  void stop();
  /**
   * Start a new backup.
   * @returns {uint64_t} new job id
   **/
  uint64_t backup() {
    std::lock_guard guard{mutex};
    should_backup = true;
    uint64_t rv = ++last_job_id;
    work_cond.notify_one();
    return rv;
  }

  /// Queue a cleanup pass.
  uint64_t cleanup() {
    std::lock_guard guard{mutex};
    should_cleanup = true;
    uint64_t rv = ++last_job_id;
    work_cond.notify_one();
    return rv;
  }

};

#endif

