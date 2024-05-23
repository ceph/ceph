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
//#undef dout_prefix
//#define dout_prefix _prefix(_dout, this)
//#define dout_prefix *_dout << "asok(" << (void*)m_cct << ") "

/***
 * Thread which runs monitor backup operations
 */
void *MonitorBackupManager::entry() {
    while (true) {
        m_wakeup.Get();

        bool start_backup = false;
        KeyValueDB::BackupStats *stats = m_last_backup.get();
        uint64_t interval = m_cct->_conf.get_val<uint64_t>("mon_backup_interval");
        if (stats && interval > 0) {
            if ((stats->timestamp - ceph_clock_now()) > interval) {
                dout(20)  << " trigger timed backup " << dendl;
                start_backup = true;
            }
        }
        m_lock.lock();
        bool start_cleanup = m_do_cleanup;
        start_backup |= m_do_backup;
        m_do_cleanup = false;
        m_do_backup = false;
        m_lock.unlock();
        
        if (start_cleanup) {
            do_cleanup();
        }
        if (start_backup) {
            do_backup();
        }
    }
}

void MonitorBackupManager::do_cleanup() {
    if (!mon || !mon->store || !mon->logger) {
        return;
    }
    mon->logger->set(l_mon_backup_cleanup_running, 1);
    KeyValueDB::BackupCleanupStats stats = mon->store->backup_cleanup();
    mon->logger->set(l_mon_backup_cleanup_size, stats.size);
    mon->logger->set(l_mon_backup_cleanup_kept, stats.kept);
    mon->logger->set(l_mon_backup_cleanup_freed, stats.freed);
    shared_ptr<KeyValueDB::BackupCleanupStats> ptr = std::make_shared<KeyValueDB::BackupCleanupStats>(stats);
    m_last_cleanup.swap(ptr);
    mon->logger->set(l_mon_backup_cleanup_running, 0);

}

void MonitorBackupManager::do_backup() {
    dout(1) << "triggering backup" << dendl;
    if (!mon || !mon->store || !mon->logger) {
        return;
    }
    mon->logger->inc(l_mon_backup_started);
    auto start = ceph_clock_now();
    KeyValueDB::BackupStats stats = mon->store->backup();
    auto end = ceph_clock_now();
    utime_t duration = end - start;
    //auto duration = ceph::to_seconds<utime_t>(x);
    mon->logger->tinc(l_mon_backup_duration, duration);
    mon->logger->set(l_mon_backup_last_size, stats.size);
    mon->logger->set(l_mon_backup_last_files, stats.number_files);
    if (stats.error) {
        mon->logger->inc(l_mon_backup_failed);
        mon->logger->set(l_mon_backup_last_failed, stats.timestamp);
        dout(1) << "failed backup in "
                  << utimespan_str(duration) << " seconds" << dendl;
    } else {
        mon->logger->inc(l_mon_backup_success);
        mon->logger->set(l_mon_backup_last_success, stats.timestamp);
        dout(1) << "finished backup in "
                <<  utimespan_str(duration) << " seconds" << dendl;
    }
    std::shared_ptr<KeyValueDB::BackupStats> ptr = std::make_shared<KeyValueDB::BackupStats>(stats);
    m_last_backup.swap(ptr);
}