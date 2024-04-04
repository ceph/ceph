// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/statechart/event.hpp>

#include "osd/osd_types.h"

class MOSDPGLog;

/// what we need to instantiate a pg
struct PGCreateInfo {
  spg_t pgid;
  epoch_t epoch = 0;
  pg_history_t history;
  PastIntervals past_intervals;
  bool by_mon;
  PGCreateInfo(spg_t p, epoch_t e,
	       const pg_history_t& h,
	       const PastIntervals& pi,
	       bool mon)
    : pgid(p), epoch(e), history(h), past_intervals(pi), by_mon(mon) {}
};

class PGPeeringEvent {
  epoch_t epoch_sent;
  epoch_t epoch_requested;
  std::string desc;
public:
  boost::intrusive_ptr< const boost::statechart::event_base > evt;
  bool requires_pg;
  std::unique_ptr<PGCreateInfo> create_info;
  MEMPOOL_CLASS_HELPERS();
  template <class T>
  PGPeeringEvent(
    epoch_t epoch_sent,
    epoch_t epoch_requested,
    const T &evt_,
    bool req = true,
    PGCreateInfo *ci = 0)
    : epoch_sent(epoch_sent),
      epoch_requested(epoch_requested),
      evt(evt_.intrusive_from_this()),
      requires_pg(req),
      create_info(ci) {
    std::stringstream out;
    out << "epoch_sent: " << epoch_sent
	<< " epoch_requested: " << epoch_requested << " ";
    evt_.print(&out);
    if (create_info) {
      out << " +create_info";
    }
    desc = out.str();
  }
  epoch_t get_epoch_sent() const {
    return epoch_sent;
  }
  epoch_t get_epoch_requested() const {
    return epoch_requested;
  }
  const boost::statechart::event_base &get_event() const {
    return *evt;
  }
  const std::string& get_desc() const {
    return desc;
  }
};
typedef std::shared_ptr<PGPeeringEvent> PGPeeringEventRef;
typedef std::unique_ptr<PGPeeringEvent> PGPeeringEventURef;

struct MInfoRec : boost::statechart::event< MInfoRec > {
  pg_shard_t from;
  pg_info_t info;
  epoch_t msg_epoch;
  std::optional<pg_lease_t> lease;
  std::optional<pg_lease_ack_t> lease_ack;
  MInfoRec(pg_shard_t from, const pg_info_t &info, epoch_t msg_epoch,
	   std::optional<pg_lease_t> l = {},
	   std::optional<pg_lease_ack_t> la = {})
    : from(from), info(info), msg_epoch(msg_epoch),
      lease(l), lease_ack(la) {}
  void print(std::ostream *out) const {
    *out << "MInfoRec from " << from << " info: " << info;
    if (lease) {
      *out << " " << *lease;
    }
    if (lease_ack) {
      *out << " " << *lease_ack;
    }
  }
};

struct MLogRec : boost::statechart::event< MLogRec > {
  pg_shard_t from;
  boost::intrusive_ptr<MOSDPGLog> msg;
  MLogRec(pg_shard_t from, MOSDPGLog *msg);
  void print(std::ostream *out) const;
};

struct MNotifyRec : boost::statechart::event< MNotifyRec > {
  spg_t pgid;
  pg_shard_t from;
  pg_notify_t notify;
  uint64_t features;
  MNotifyRec(spg_t p, pg_shard_t from, const pg_notify_t &notify, uint64_t f)
    : pgid(p), from(from), notify(notify), features(f) {}
  void print(std::ostream *out) const {
    *out << "MNotifyRec " << pgid << " from " << from << " notify: " << notify
	 << " features: 0x" << std::hex << features << std::dec;
  }
};

struct MQuery : boost::statechart::event< MQuery > {
  spg_t pgid;
  pg_shard_t from;
  pg_query_t query;
  epoch_t query_epoch;
  MQuery(spg_t p, pg_shard_t from, const pg_query_t &query, epoch_t query_epoch)
    : pgid(p), from(from), query(query), query_epoch(query_epoch) {}
  void print(std::ostream *out) const {
    *out << "MQuery " << pgid << " from " << from
	 << " query_epoch " << query_epoch
	 << " query: " << query;
  }
};

struct MTrim : boost::statechart::event<MTrim> {
  epoch_t epoch;
  int from;
  shard_id_t shard;
  eversion_t trim_to;
  MTrim(epoch_t epoch, int from, shard_id_t shard, eversion_t trim_to)
    : epoch(epoch), from(from), shard(shard), trim_to(trim_to) {}
  void print(std::ostream *out) const {
    *out << "MTrim epoch " << epoch << " from " << from << " shard " << shard
	 << " trim_to " << trim_to;
  }
};

struct MLease : boost::statechart::event<MLease> {
  epoch_t epoch;
  int from;
  pg_lease_t lease;
  MLease(epoch_t epoch, int from, pg_lease_t l)
    : epoch(epoch), from(from), lease(l) {}
  void print(std::ostream *out) const {
    *out << "MLease epoch " << epoch << " from osd." << from << " " << lease;
  }
};

struct MLeaseAck : boost::statechart::event<MLeaseAck> {
  epoch_t epoch;
  int from;
  pg_lease_ack_t lease_ack;
  MLeaseAck(epoch_t epoch, int from, pg_lease_ack_t l)
    : epoch(epoch), from(from), lease_ack(l) {}
  void print(std::ostream *out) const {
    *out << "MLeaseAck epoch " << epoch << " from osd." << from
	 << " " << lease_ack;
  }
};

struct RequestBackfillPrio : boost::statechart::event< RequestBackfillPrio > {
  unsigned priority;
  int64_t primary_num_bytes;
  int64_t local_num_bytes;
  explicit RequestBackfillPrio(unsigned prio, int64_t pbytes, int64_t lbytes) :
    boost::statechart::event< RequestBackfillPrio >(),
    priority(prio), primary_num_bytes(pbytes), local_num_bytes(lbytes) {}
  void print(std::ostream *out) const {
    *out << "RequestBackfillPrio: priority " << priority
         << " primary bytes " << primary_num_bytes
         << " local bytes " << local_num_bytes;
  }
};

struct RequestRecoveryPrio : boost::statechart::event< RequestRecoveryPrio > {
  unsigned priority;
  explicit RequestRecoveryPrio(unsigned prio) :
    boost::statechart::event< RequestRecoveryPrio >(),
    priority(prio) {}
  void print(std::ostream *out) const {
    *out << "RequestRecoveryPrio: priority " << priority;
  }
};

#define TrivialEvent(T) struct T : boost::statechart::event< T > { \
    T() : boost::statechart::event< T >() {}			   \
    void print(std::ostream *out) const {			   \
      *out << #T;						   \
    }								   \
  };

TrivialEvent(NullEvt)
TrivialEvent(PgCreateEvt)
TrivialEvent(RemoteBackfillReserved)
TrivialEvent(RemoteReservationRejectedTooFull)
TrivialEvent(RemoteReservationRevokedTooFull)
TrivialEvent(RemoteReservationRevoked)
TrivialEvent(RemoteReservationCanceled)
TrivialEvent(RemoteRecoveryReserved)
TrivialEvent(RecoveryDone)

struct DeferRecovery : boost::statechart::event<DeferRecovery> {
  float delay;
  explicit DeferRecovery(float delay) : delay(delay) {}
  void print(std::ostream *out) const {
    *out << "DeferRecovery: delay " << delay;
  }
};

struct DeferBackfill : boost::statechart::event<DeferBackfill> {
  float delay;
  explicit DeferBackfill(float delay) : delay(delay) {}
  void print(std::ostream *out) const {
    *out << "DeferBackfill: delay " << delay;
  }
};

TrivialEvent(RenewLease)
