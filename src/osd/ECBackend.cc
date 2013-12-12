// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ECBackend.h"

PGBackend::RecoveryHandle *open_recovery_op()
{
  return 0;
}

void ECBackend::run_recovery_op(
  RecoveryHandle *h,
  int priority)
{
}

void ECBackend::recover_object(
  const hobject_t &hoid,
  eversion_t v,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *h)
{
}

bool ECBackend::handle_message(
  OpRequestRef op)
{
  return false;
}

void ECBackend::check_recovery_sources(const OSDMapRef osdmap)
{
}

void ECBackend::_on_change(ObjectStore::Transaction *t)
{
}

void ECBackend::clear_state()
{
}

void ECBackend::on_flushed()
{
}


void ECBackend::dump_recovery_info(Formatter *f) const
{
}

PGBackend::PGTransaction *ECBackend::get_transaction()
{
  return new ECTransaction;
}

void ECBackend::submit_transaction(
  const hobject_t &hoid,
  const eversion_t &at_version,
  PGTransaction *t,
  const eversion_t &trim_to,
  vector<pg_log_entry_t> &log_entries,
  Context *on_local_applied_sync,
  Context *on_all_applied,
  Context *on_all_commit,
  tid_t tid,
  osd_reqid_t reqid,
  OpRequestRef op)
{
}

int ECBackend::objects_read_sync(
  const hobject_t &hoid,
  uint64_t off,
  uint64_t len,
  bufferlist *bl)
{
  return -EOPNOTSUPP;
}

void ECBackend::objects_read_async(
  const hobject_t &hoid,
  const list<pair<pair<uint64_t, uint64_t>,
		  pair<bufferlist*, Context*> > > &to_read,
  Context *on_complete)
{
  return;
}
