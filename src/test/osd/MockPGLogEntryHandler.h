// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "osd/PGLog.h"
#include "os/ObjectStore.h"
#include "MockPGBackend.h"

// MockPGLogEntryHandler
//
// This is a fully functional implementation of the PGLog::LogEntryHandler
// interface. It calls code in PGBackend to perform the requested operations
// although some of the stubs in MockPGBackend return questionable information
// about the object size so the generated ObjectStore::Transaction is probably
// not correct. The main purpose is to use partial_write to update the PWLC
// information when appending entries to the log
class MockPGLogEntryHandler : public PGLog::LogEntryHandler {
 public:
  MockPGBackend *backend;
  ObjectStore::Transaction *t;
  MockPGLogEntryHandler(MockPGBackend *backend, ObjectStore::Transaction *t) : backend(backend), t(t) {}

  // LogEntryHandler
  void remove(const hobject_t &hoid) override {
    lgeneric_dout(g_ceph_context, 0) << "MockPGLogEntryHandler::remove " << hoid << dendl;
    backend->remove(hoid, t);
  }
  void try_stash(const hobject_t &hoid, version_t v) override {
    lgeneric_dout(g_ceph_context, 0) << "MockPGLogEntryHandler::try_stash " << hoid << " " << v << dendl;
    backend->try_stash(hoid, v, t);
  }
  void rollback(const pg_log_entry_t &entry) override {
    lgeneric_dout(g_ceph_context, 0) << "MockPGLogEntryHandler::rollback " << entry << dendl;
    ceph_assert(entry.can_rollback());
    backend->rollback(entry, t);
  }
  void rollforward(const pg_log_entry_t &entry) override {
    lgeneric_dout(g_ceph_context, 0) << "MockPGLogEntryHandler::rollforward " << entry << dendl;
    backend->rollforward(entry, t);
  }
  void trim(const pg_log_entry_t &entry) override {
    lgeneric_dout(g_ceph_context, 0) << "MockPGLogEntryHandler::trim " << entry << dendl;
    backend->trim(entry, t);
  }
  void partial_write(pg_info_t *info, eversion_t previous_version,
                      const pg_log_entry_t &entry
    ) override {
    lgeneric_dout(g_ceph_context, 0) << "MockPGLogEntryHandler::partial_write " << entry << dendl;
    backend->partial_write(info, previous_version, entry);
  }
};

