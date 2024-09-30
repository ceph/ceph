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

#include <list>
#include <set>
#include <string>


#include "common/Semaphore.h"
#include "common/Thread.h"
#include "common/ceph_context.h"
#include "kv/KeyValueDB.h"
#include "mon/MonitorDBStore.h"

using std::shared_ptr;
class Monitor;

using ceph::common::CephContext;


class MonitorBackupManager : public Thread {
  CephContext *m_cct;
  //PerfCounters *logger;
  Monitor *mon;
  //MonitorDBStore *m_db;
  ceph::mutex m_lock;
  Semaphore m_wakeup{};
  bool m_manager_stop{false};

  bool m_do_backup;
  bool m_do_backup_full;
  bool m_do_cleanup;
  uint64_t m_last_job_id;
  shared_ptr<KeyValueDB::BackupCleanupStats> m_last_cleanup;
  shared_ptr<KeyValueDB::BackupStats> m_last_backup;

  void backup_loop();
  void do_backup(bool full);
  void do_cleanup();
  bool check_free_space();
protected:
  void *entry() override;

public:
  explicit MonitorBackupManager(CephContext *cct, Monitor *monitor) : 
    m_cct(cct),
    mon(monitor),
    m_lock(ceph::make_mutex("mon::BackupManager::m_lock")) {
      create("mon::backups");
  }

  void tick() {
    // wake up timers etc
    m_wakeup.Put();
  }

  /**
   * Stop the backup manager thread.
   * @returns {uint64_t} new job id
   **/
  void stop();
  /**
   * Start a new backup.
   * @returns {uint64_t} new job id
   **/
  uint64_t backup(bool full) {
    m_lock.lock();
    // requesting a full backup has higher priority
    m_do_backup = true;
    if (full) {
      m_do_backup_full = true;
    }
    uint64_t rv = ++m_last_job_id;
    m_lock.unlock();
    m_wakeup.Put();
    return rv;
  }

  /// Start a new backup
  uint64_t cleanup() {
    m_lock.lock();
    m_do_cleanup = true;
    uint64_t rv = ++m_last_job_id;
    m_lock.unlock();
    m_wakeup.Put();
    return rv;
  }

  /// Start a new backup
  shared_ptr<KeyValueDB::BackupStats> last_backup_info() {
    return m_last_backup;
  }
  
  shared_ptr<KeyValueDB::BackupCleanupStats> last_cleanup_info() {
    return m_last_cleanup;
  }
};

#endif