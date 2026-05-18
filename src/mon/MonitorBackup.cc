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

#include "mon/MonitorBackup.h"
#include "mon/Monitor.h"

#define dout_subsys ceph_subsys_mon

/***
 * Thread which runs monitor backup operations
 */
void *MonitorBackupManager::entry() {
    while (true) {
        m_wakeup.Get();

        if (m_manager_stop) {
            return nullptr;
        }

        KeyValueDB::BackupStats *stats = m_last_backup.get();
        auto now = ceph_clock_now();
        uint64_t interval = m_cct->_conf.get_val<uint64_t>("mon_backup_interval");
        bool start_backup = false;
        bool start_backup_full = false;

        if (!stats && interval > 0) {
            start_backup = true;
            start_backup_full = true;
        } else if (stats && interval > 0) {
            if ((now - stats->timestamp) > (interval * 60)) {
                dout(10) << " trigger timed backup " << dendl;
                start_backup = true;
            }
        }

        m_lock.lock();
        start_backup |= m_do_backup;
        start_backup_full |= m_do_backup_full;
        m_do_backup = false;
        m_do_backup_full = false;
        m_lock.unlock();

        if (start_backup) {
            do_backup(start_backup_full);
        }
    }
}

void MonitorBackupManager::stop() {
    m_manager_stop = true;
    m_wakeup.Put();
    join();
}

/***
 * Checks for enough free space to do backup.
 * Returns true if there is enough free space
*/
bool MonitorBackupManager::check_free_space() {
    ceph_data_stats_t stats;
    int err = get_fs_stats(stats, m_cct->_conf.get_val<std::string>("mon_backup_path").c_str());
    if (err < 0) {
      dout(1) << "error checking monitor backup directory: " << cpp_strerror(err)
           << dendl;
      return false;
    }

    if (stats.avail_percent <= m_cct->_conf.get_val<int64_t>("mon_backup_min_avail")) {
      dout(1) << "ERROR: not enough disk space to start backup: " << "(available: "
           << stats.avail_percent << "% " << byte_u_t(stats.byte_avail) << ")\n"
           << "run backup_cleanup regularly or decrease mon_backup_min_avail" << dendl;
      return false;
    }
    return true;
}

void MonitorBackupManager::do_backup(bool full) {
    dout(1) << "start backup" << dendl;
    if (!mon || !mon->store || !mon->logger) {
        return;
    }
    mon->logger->inc(l_mon_backup_started);
    auto start = ceph_clock_now();

    if (!check_free_space()) {
        mon->logger->inc(l_mon_backup_failed);
        mon->logger->tset(l_mon_backup_last_failed, start);
        return;
    }

    full |= m_cct->_conf.get_val<bool>("mon_backup_always_full");

    // we do full backups when it is the first of every 5th backup
    if (!full && (!m_last_backup || m_last_backup.get()->id % 5 == 0)) {
        full = true;
    }

    KeyValueDB::BackupStats stats = mon->store->backup(full);

    auto end = ceph_clock_now();
    utime_t duration = end - start;
    mon->logger->tinc(l_mon_backup_duration, duration);
    mon->logger->set(l_mon_backup_last_size, stats.size);
    mon->logger->set(l_mon_backup_last_files, stats.number_files);
    if (stats.error) {
        mon->logger->inc(l_mon_backup_failed);
        mon->logger->tset(l_mon_backup_last_failed, stats.timestamp);
        dout(1) << "failed backup in "
                  << utimespan_str(duration) << " seconds" << dendl;
    } else {
        mon->logger->inc(l_mon_backup_success);
        mon->logger->tset(l_mon_backup_last_success, stats.timestamp);
        mon->logger->set(l_mon_backup_last_success_id, stats.id);
        dout(1) << "finished backup in "
                << utimespan_str(duration) << " seconds" << dendl;
    }
    std::shared_ptr<KeyValueDB::BackupStats> ptr = std::make_shared<KeyValueDB::BackupStats>(stats);
    m_last_backup.swap(ptr);
}
