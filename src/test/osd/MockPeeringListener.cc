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

#include "test/osd/MockPeeringListener.h"
#include "test/osd/EventLoop.h"

void MockPeeringListener::request_local_background_io_reservation(
  unsigned priority,
  PGPeeringEventURef on_grant,
  PGPeeringEventURef on_preempt) {
  // Check inject_event_stall first: a grant delivered across a later interval
  // change would hit a PeeringState in Reset and abort.
  if (inject_event_stall) {
    stalled_events.push_back(std::move(on_grant));
  } else if (event_loop) {
    PGPeeringEventRef evt_ref = std::move(on_grant);
    int shard = pg_whoami.osd;
    event_loop->schedule_peering_event(shard, [this, evt_ref]() {
      if (!ps || !ctx) return;
      ps->handle_event(evt_ref, ctx);
    });
  } else {
    events.push_back(std::move(on_grant));
  }
  if (inject_keep_preempt) {
    stalled_events.push_back(std::move(on_preempt));
  }
  io_reservations_requested++;
}

void MockPeeringListener::request_remote_recovery_reservation(
  unsigned priority,
  PGPeeringEventURef on_grant,
  PGPeeringEventURef on_preempt) {
  if (inject_event_stall) {
    stalled_events.push_back(std::move(on_grant));
  } else if (event_loop) {
    PGPeeringEventRef evt_ref = std::move(on_grant);
    int shard = pg_whoami.osd;
    event_loop->schedule_peering_event(shard, [this, evt_ref]() {
      if (!ps || !ctx) return;
      ps->handle_event(evt_ref, ctx);
    });
  } else {
    events.push_back(std::move(on_grant));
  }
  if (inject_keep_preempt) {
    stalled_events.push_back(std::move(on_preempt));
  }
  remote_recovery_reservations_requested++;
}

void MockPeeringListener::schedule_event_on_commit(
  ObjectStore::Transaction &t,
  PGPeeringEventRef on_commit) {
  if (event_loop) {
    // Gate on pg_has_reset_since for both epoch fields, mirroring
    // PG::old_peering_evt -> old_peering_msg which discards an event when
    // last_peering_reset > epoch_sent OR last_peering_reset > epoch_requested.
    int shard = pg_whoami.osd;
    event_loop->schedule_peering_event(shard, [this, on_commit]() {
      if (!ps || !ctx) return;
      if (ps->pg_has_reset_since(on_commit->get_epoch_sent()) ||
          ps->pg_has_reset_since(on_commit->get_epoch_requested())) return;
      ps->handle_event(on_commit, ctx);
    });
  } else if (inject_event_stall) {
    stalled_events.push_back(std::move(on_commit));
  } else {
    events.push_back(std::move(on_commit));
  }
  events_on_commit_scheduled++;
}

void MockPeeringListener::on_activate_complete() {
  dout(0) << __func__ << dendl;

  auto schedule_event = [this](PGPeeringEventRef evt) {
    if (event_loop) {
      int shard = pg_whoami.osd;
      // Gate on both epoch fields — mirrors PG::old_peering_evt -> old_peering_msg.
      event_loop->schedule_peering_event(shard, [this, evt]() {
        if (!ps || !ctx) return;
        if (ps->pg_has_reset_since(evt->get_epoch_sent()) ||
            ps->pg_has_reset_since(evt->get_epoch_requested())) return;
        ps->handle_event(evt, ctx);
      });
    } else if (inject_event_stall) {
      stalled_events.push_back(evt);
    } else {
      events.push_back(evt);
    }
  };

  if (ps->needs_recovery()) {
    dout(10) << "activate not all replicas are up-to-date, queueing recovery" << dendl;
    schedule_event(std::make_shared<PGPeeringEvent>(
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::DoRecovery()));
  } else if (ps->needs_backfill()) {
    dout(10) << "activate queueing backfill" << dendl;
    schedule_event(std::make_shared<PGPeeringEvent>(
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::RequestBackfill()));
#if POOL_MIGRATION
  } else if (ps->needs_pool_migration()) {
    dout(10) << "activate queueing pool migration" << dendl;
    schedule_event(std::make_shared<PGPeeringEvent>(
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::DoPoolMigration()));
#endif
  } else {
    dout(10) << "activate all replicas clean, no recovery" << dendl;
    schedule_event(std::make_shared<PGPeeringEvent>(
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::AllReplicasRecovered()));
  }
  activate_complete_called = true;
}

