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
#include "test/osd/ECPeeringTestFixture.h"
#include "test/osd/EventLoop.h"

// Implementation of MockPeeringListener::request_local_background_io_reservation
// This must be defined after ECPeeringTestFixture is fully defined to avoid incomplete type errors
void MockPeeringListener::request_local_background_io_reservation(
  unsigned priority,
  PGPeeringEventURef on_grant,
  PGPeeringEventURef on_preempt) {
  // If the test has configured an event loop (i.e. ECPeeringTestFixture),
  // then use the event loop to run this event, rather than putting it on the queue.
  if (event_loop && fixture) {
    // Schedule the event through the event loop for deterministic execution
    PGPeeringEventRef evt_ref = std::move(on_grant);
    int shard = pg_whoami.osd;
    event_loop->schedule_peering_event(shard, [this, evt_ref, shard]() {
      fixture->get_peering_state(shard)->handle_event(evt_ref, fixture->get_peering_ctx(shard));
    });
  } else if (inject_event_stall) {
    stalled_events.push_back(std::move(on_grant));
  } else {
    events.push_back(std::move(on_grant));
  }
  if (inject_keep_preempt) {
    stalled_events.push_back(std::move(on_preempt));
  }
  io_reservations_requested++;
}

// Implementation of MockPeeringListener::request_remote_recovery_reservation
// This must be defined after ECPeeringTestFixture is fully defined to avoid incomplete type errors
void MockPeeringListener::request_remote_recovery_reservation(
  unsigned priority,
  PGPeeringEventURef on_grant,
  PGPeeringEventURef on_preempt) {
  // If the test has configured an event loop (i.e. ECPeeringTestFixture),
  // then use the event loop to run this event, rather than putting it on the queue.
  if (event_loop && fixture) {
    // Schedule the event through the event loop for deterministic execution
    PGPeeringEventRef evt_ref = std::move(on_grant);
    int shard = pg_whoami.osd;
    event_loop->schedule_peering_event(shard, [this, evt_ref, shard]() {
      fixture->get_peering_state(shard)->handle_event(evt_ref, fixture->get_peering_ctx(shard));
    });
  } else if (inject_event_stall) {
    stalled_events.push_back(std::move(on_grant));
  } else {
    events.push_back(std::move(on_grant));
  }
  if (inject_keep_preempt) {
    stalled_events.push_back(std::move(on_preempt));
  }
  remote_recovery_reservations_requested++;
}

// Implementation of MockPeeringListener::schedule_event_on_commit
// This must be defined after ECPeeringTestFixture is fully defined to avoid incomplete type errors
void MockPeeringListener::schedule_event_on_commit(
  ObjectStore::Transaction &t,
  PGPeeringEventRef on_commit) {
  // If the test has configured an event loop (i.e. ECPeeringTestFixture),
  // then use the event loop to run this event, rather than putting it on the queue.
  if (event_loop && fixture) {
    // Schedule the event through the event loop for deterministic execution
    int shard = pg_whoami.osd;
    event_loop->schedule_peering_event(shard, [this, on_commit, shard]() {
      fixture->get_peering_state(shard)->handle_event(on_commit, fixture->get_peering_ctx(shard));
    });
  } else if (inject_event_stall) {
    stalled_events.push_back(std::move(on_commit));
  } else {
    events.push_back(std::move(on_commit));
  }
  events_on_commit_scheduled++;
}

// Implementation of MockPeeringListener::on_activate_complete
// This must be defined after ECPeeringTestFixture is fully defined to avoid incomplete type errors
void MockPeeringListener::on_activate_complete() {
  dout(0) << __func__ << dendl;
  
  // Helper lambda to schedule an event
  auto schedule_event = [this](PGPeeringEventRef evt) {
    if (event_loop && fixture) {
      // Use event loop for deterministic execution
      int shard = pg_whoami.osd;
      event_loop->schedule_peering_event(shard, [this, evt, shard]() {
        fixture->get_peering_state(shard)->handle_event(evt, fixture->get_peering_ctx(shard));
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

