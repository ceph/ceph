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

#include <chrono>
#include <filesystem>

#include "include/util.h"
#include "mon/MonitorBackup.h"
#include "mon/Monitor.h"

#define dout_subsys ceph_subsys_mon
#undef dout_context
#define dout_context cct

namespace fs = std::filesystem;

/***
 * Thread which runs monitor backup operations
 */
void *MonitorBackupManager::entry() {
  std::unique_lock lock{mutex};
  auto wakeup_predicate = [this] {
    return should_stop || should_backup || should_cleanup || wakeup_pending;
  };
  while (true) {
    // Wait for any signal (tick, request, or stop) before doing
    // scheduled work. Predicate-based so notifications delivered
    // before the worker entered wait() are not lost, and so we don't
    // fire scheduled work while init() is still wiring the mon up.
    work_cond.wait(lock, wakeup_predicate);
    if (should_stop) {
      return nullptr;
    }
    wakeup_pending = false;

    auto now = ceph_clock_now();
    std::string backup_path = cct->_conf.get_val<std::string>("mon_backup_path");
    auto interval = cct->_conf.get_val<std::chrono::seconds>("mon_backup_interval");
    auto cleanup_interval = cct->_conf.get_val<std::chrono::seconds>("mon_backup_cleanup_interval");
    bool path_ok = !backup_path.empty();

    bool timer_backup = false;
    bool timer_cleanup = false;

    if (path_ok && interval.count() > 0 &&
        (mon->is_leader() || mon->is_peon())) {
      if (!last_backup) {
        dout(10) << "trigger first timed backup" << dendl;
        timer_backup = true;
      } else if ((now - last_backup->timestamp) >= utime_t(interval.count(), 0)) {
        dout(10) << "trigger timed backup" << dendl;
        timer_backup = true;
      }
    }
    if (path_ok && cleanup_interval.count() > 0) {
      if (!last_cleanup) {
        dout(10) << "trigger first timed backup cleanup" << dendl;
        timer_cleanup = true;
      } else if ((now - last_cleanup->timestamp) >= utime_t(cleanup_interval.count(), 0)) {
        dout(10) << "trigger timed backup cleanup" << dendl;
        timer_cleanup = true;
      }
    }

    bool run_cleanup = should_cleanup || timer_cleanup;
    bool run_backup = should_backup || timer_backup;

    should_backup = false;
    should_cleanup = false;

    if (!run_backup && !run_cleanup) {
      continue;
    }

    lock.unlock();
    if (run_cleanup) {
      do_cleanup();
    }
    if (run_backup) {
      do_backup();
    }
    lock.lock();
  }
}

void MonitorBackupManager::stop() {
  {
    std::lock_guard guard{mutex};
    if (should_stop) {
      return;
    }
    should_stop = true;
    work_cond.notify_one();
  }
  join();
}

void MonitorBackupManager::do_cleanup() {
  dout(5) << "start backup cleanup" << dendl;
  mon->logger->inc(l_mon_backup_cleanup_started);
  mon->logger->set(l_mon_backup_cleanup_running, 1);
  auto start = ceph_clock_now();
  KeyValueDB::BackupCleanupStats stats = mon->store->backup_cleanup();
  mon->logger->set(l_mon_backup_cleanup_size, stats.size);
  mon->logger->set(l_mon_backup_cleanup_kept, stats.kept);
  mon->logger->set(l_mon_backup_cleanup_freed, stats.freed);
  mon->logger->set(l_mon_backup_cleanup_deleted, stats.deleted);
  if (stats.error) {
    mon->logger->inc(l_mon_backup_cleanup_failed);
  } else {
    mon->logger->inc(l_mon_backup_cleanup_success);
  }
  auto ptr = std::make_shared<KeyValueDB::BackupCleanupStats>(stats);
  last_cleanup.swap(ptr);
  auto end = ceph_clock_now();
  utime_t duration = end - start;
  mon->logger->tinc(l_mon_backup_cleanup_duration, duration);
  mon->logger->set(l_mon_backup_cleanup_running, 0);
}

void MonitorBackupManager::record_last_backup(std::shared_ptr<KeyValueDB::BackupStats> stats) {
  if (stats->error && last_backup) {
    stats->id = last_backup->id;
  }
  last_backup.swap(stats);
}

// Returns true if there is enough free space on the backup volume.
bool MonitorBackupManager::check_free_space() {
  auto backup_path = cct->_conf.get_val<std::string>("mon_backup_path");

  std::error_code ec;
  if (!fs::exists(backup_path, ec)) {
    if (!fs::create_directories(backup_path, ec)) {
      dout(1) << "failed to create monitor backup directory '"
              << backup_path << "': " << ec.message() << dendl;
      return false;
    }
    fs::permissions(backup_path, fs::perms::owner_all,
                    fs::perm_options::replace, ec);
    if (ec) {
      dout(1) << "failed to set permissions on monitor backup directory '"
              << backup_path << "': " << ec.message() << dendl;
      return false;
    }
    dout(5) << "created monitor backup directory '" << backup_path
            << "'" << dendl;
  }

  ceph_data_stats_t stats;
  int err = get_fs_stats(stats, backup_path.c_str());
  if (err < 0) {
    dout(1) << "error checking monitor backup directory: " << cpp_strerror(err)
            << dendl;
    return false;
  }

  if (stats.avail_percent <= cct->_conf.get_val<int64_t>("mon_backup_min_avail")) {
    dout(1) << "ERROR: not enough disk space to start backup: " << "(available: "
            << stats.avail_percent << "% " << byte_u_t(stats.byte_avail) << ")\n"
            << "run backup_cleanup regularly or decrease mon_backup_min_avail" << dendl;
    return false;
  }
  return true;
}

void MonitorBackupManager::do_backup() {
  dout(1) << "start backup" << dendl;
  mon->logger->inc(l_mon_backup_started);
  mon->logger->set(l_mon_backup_running, 1);
  auto start = ceph_clock_now();

  std::shared_ptr<KeyValueDB::BackupStats> result;

  if (!check_free_space()) {
    mon->logger->inc(l_mon_backup_failed);
    mon->logger->tset(l_mon_backup_last_failed, start);
    result = std::make_shared<KeyValueDB::BackupStats>();
    result->error = true;
    result->timestamp = start;
    result->msg = "insufficient free space";
  } else {
    KeyValueDB::BackupStats stats = mon->store->backup();
    utime_t duration = ceph_clock_now() - start;
    mon->logger->tinc(l_mon_backup_duration, duration);
    mon->logger->set(l_mon_backup_last_size, stats.size);
    mon->logger->set(l_mon_backup_last_files, stats.number_files);
    if (stats.error) {
      mon->logger->inc(l_mon_backup_failed);
      mon->logger->tset(l_mon_backup_last_failed, stats.timestamp);
      dout(1) << "failed backup in " << utimespan_str(duration) << dendl;
    } else {
      mon->logger->inc(l_mon_backup_success);
      mon->logger->tset(l_mon_backup_last_success, stats.timestamp);
      mon->logger->set(l_mon_backup_last_success_id, stats.id);
      dout(1) << "finished backup in " << utimespan_str(duration) << dendl;
    }
    result = std::make_shared<KeyValueDB::BackupStats>(stats);
  }

  record_last_backup(result);
  mon->logger->set(l_mon_backup_running, 0);
}

