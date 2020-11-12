// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cassert>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "PG.h"
#include "ScrubStore.h"
#include "scrub_machine_lstnr.h"
#include "scrubber_common.h"

class Callback;

namespace Scrub {
class ScrubMachine;
struct BuildMap;

/**
 * Reserving/freeing scrub resources at the replicas.
 *
 *  When constructed - sends reservation requests to the acting_set.
 *  A rejection triggers a "couldn't acquire the replicas' scrub resources" event.
 *  All previous requests, whether already granted or not, are explicitly released.
 *
 *  A note re performance: I've measured a few container alternatives for
 *  m_reserved_peers, with its specific usage pattern. Std::set is extremely slow, as
 *  expected. flat_set is only slightly better. Surprisingly - std::vector (with no
 *  sorting) is better than boost::small_vec. And for std::vector: no need to pre-reserve.
 */
class ReplicaReservations {
  using OrigSet = decltype(std::declval<PG>().get_actingset());

  PG* m_pg;
  OrigSet m_acting_set;
  OSDService* m_osds;
  std::vector<pg_shard_t> m_waited_for_peers;
  std::vector<pg_shard_t> m_reserved_peers;
  bool m_had_rejections{false};
  int m_pending{-1};

  void release_replica(pg_shard_t peer, epoch_t epoch);

  void send_all_done();	 ///< all reservations are granted

  /// notify the scrubber that we have failed to reserve replicas' resources
  void send_reject();

  void release_all();

 public:
  ReplicaReservations(PG* pg, pg_shard_t whoami);

  ~ReplicaReservations();

  void handle_reserve_grant(OpRequestRef op, pg_shard_t from);

  void handle_reserve_reject(OpRequestRef op, pg_shard_t from);
};

/**
 *  wraps the local OSD scrub resource reservation in an RAII wrapper
 */
class LocalReservation {
  PG* m_pg;
  OSDService* m_osds;
  bool m_holding_local_reservation{false};

 public:
  LocalReservation(PG* pg, OSDService* osds);
  ~LocalReservation();
  bool is_reserved() const { return m_holding_local_reservation; }
  void early_release();
};

/**
 *  wraps the OSD resource we are using when reserved as a replica by a scrubbing master.
 */
class ReservedByRemotePrimary {
  PG* m_pg;
  OSDService* m_osds;
  bool m_reserved_by_remote_primary{false};

 public:
  ReservedByRemotePrimary(PG* pg, OSDService* osds);
  ~ReservedByRemotePrimary();
  [[nodiscard]] bool is_reserved() const { return m_reserved_by_remote_primary; }
  void early_release();
};

/**
 * Once all replicas' scrub maps are received, we go on to compare the maps. That is -
 * unless we we have not yet completed building our own scrub map. MapsCollectionStatus
 * combines the status of waiting for both the local map and the replicas, without
 * resorting to adding dummy entries into a list.
 */
class MapsCollectionStatus {

  bool m_local_map_ready{false};
  std::vector<pg_shard_t> m_maps_awaited_for;

 public:
  [[nodiscard]] bool are_all_maps_available() const
  {
    return m_local_map_ready && m_maps_awaited_for.empty();
  }

  void mark_local_map_ready() { m_local_map_ready = true; }

  void mark_replica_map_request(pg_shard_t from_whom)
  {
    m_maps_awaited_for.push_back(from_whom);
  }

  /// @returns true if indeed waiting for this one. Otherwise: an error string
  auto mark_arriving_map(pg_shard_t from) -> std::tuple<bool, std::string_view>;

  std::vector<pg_shard_t> get_awaited() const { return m_maps_awaited_for; }

  void reset();

  std::string dump() const;

  friend ostream& operator<<(ostream& out, const MapsCollectionStatus& sf);
};


}  // namespace Scrub
