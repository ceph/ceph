// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <ostream>

#include "include/types.h"
#include "include/utime_fmt.h"
#include "osd/osd_types_fmt.h"
#include "osd/OpRequest.h"
#include "osd/PG.h"
#include "osd/PGPeeringEvent.h"
#include "messages/MOSDOp.h"


class OSD;
struct OSDShard;

namespace ceph::osd::scheduler {

enum class op_scheduler_class : uint8_t {
  background_recovery = 0,
  background_best_effort,
  immediate,
  client,
};

std::ostream& operator<<(std::ostream& out, const op_scheduler_class& class_id);

class OpSchedulerItem {
public:
  // Abstraction for operations queueable in the op queue
  class OpQueueable {
  public:
    using Ref = std::unique_ptr<OpQueueable>;

    /// Items with the same queue token will end up in the same shard
    virtual uint32_t get_queue_token() const = 0;

    /* Items will be dequeued and locked atomically w.r.t. other items with the
       * same ordering token */
    virtual const spg_t& get_ordering_token() const = 0;

    virtual std::optional<OpRequestRef> maybe_get_op() const {
      return std::nullopt;
    }

    virtual uint64_t get_reserved_pushes() const {
      return 0;
    }

    virtual bool is_peering() const {
      return false;
    }
    virtual bool peering_requires_pg() const {
      ceph_abort();
    }
    virtual const PGCreateInfo *creates_pg() const {
      return nullptr;
    }

    virtual std::ostream &print(std::ostream &rhs) const = 0;
    /// and a version geared towards fmt::format use:
    virtual std::string print() const = 0;

    virtual void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) = 0;
    virtual op_scheduler_class get_scheduler_class() const = 0;

    virtual ~OpQueueable() {}
    friend std::ostream& operator<<(std::ostream& out, const OpQueueable& q) {
      return q.print(out);
    }

  };

private:
  OpQueueable::Ref qitem;
  int cost;
  unsigned priority;
  utime_t start_time;
  uint64_t owner;  ///< global id (e.g., client.XXX)
  epoch_t map_epoch;    ///< an epoch we expect the PG to exist in

  /**
   * qos_cost
   *
   * Set by mClockScheduler iff queued into mclock proper and not the
   * high/immediate queues.  Represents mClockScheduler's adjusted
   * cost value.
   */
  uint32_t qos_cost = 0;

  /// True iff queued via mclock proper, not the high/immediate queues
  bool was_queued_via_mclock() const {
    return qos_cost > 0;
  }

public:
  OpSchedulerItem(
    OpQueueable::Ref &&item,
    int cost,
    unsigned priority,
    utime_t start_time,
    uint64_t owner,
    epoch_t e)
    : qitem(std::move(item)),
      cost(cost),
      priority(priority),
      start_time(start_time),
      owner(owner),
      map_epoch(e) {}
  OpSchedulerItem(OpSchedulerItem &&) = default;
  OpSchedulerItem(const OpSchedulerItem &) = delete;
  OpSchedulerItem &operator=(OpSchedulerItem &&) = default;
  OpSchedulerItem &operator=(const OpSchedulerItem &) = delete;

  friend struct fmt::formatter<OpSchedulerItem>;

  uint32_t get_queue_token() const {
    return qitem->get_queue_token();
  }
  const spg_t& get_ordering_token() const {
    return qitem->get_ordering_token();
  }
  std::optional<OpRequestRef> maybe_get_op() const {
    return qitem->maybe_get_op();
  }
  uint64_t get_reserved_pushes() const {
    return qitem->get_reserved_pushes();
  }
  void run(OSD *osd, OSDShard *sdata,PGRef& pg, ThreadPool::TPHandle &handle) {
    qitem->run(osd, sdata, pg, handle);
  }
  unsigned get_priority() const { return priority; }
  int get_cost() const { return cost; }
  utime_t get_start_time() const { return start_time; }
  uint64_t get_owner() const { return owner; }
  epoch_t get_map_epoch() const { return map_epoch; }

  bool is_peering() const {
    return qitem->is_peering();
  }

  const PGCreateInfo *creates_pg() const {
    return qitem->creates_pg();
  }

  bool peering_requires_pg() const {
    return qitem->peering_requires_pg();
  }

  op_scheduler_class get_scheduler_class() const {
    return qitem->get_scheduler_class();
  }

  void set_qos_cost(uint32_t scaled_cost) {
    qos_cost = scaled_cost;
  }

  friend std::ostream& operator<<(std::ostream& out, const OpSchedulerItem& item) {
    out << "OpSchedulerItem("
        << item.get_ordering_token() << " " << *item.qitem;

    out << " class_id " << item.get_scheduler_class();

    out << " prio " << item.get_priority();

    if (item.was_queued_via_mclock()) {
      out << " qos_cost " << item.qos_cost;
    }

    out << " cost " << item.get_cost()
        << " e" << item.get_map_epoch();

    if (item.get_reserved_pushes()) {
      out << " reserved_pushes " << item.get_reserved_pushes();
    }

    return out << ")";
  }
}; // class OpSchedulerItem


/// Implements boilerplate for operations queued for the pg lock
class PGOpQueueable : public OpSchedulerItem::OpQueueable {
  spg_t pgid;
protected:
  const spg_t& get_pgid() const {
    return pgid;
  }

  static op_scheduler_class priority_to_scheduler_class(int priority) {
    if (priority >= CEPH_MSG_PRIO_HIGH) {
      return op_scheduler_class::immediate;
    } else if (priority >= PeeringState::recovery_msg_priority_t::DEGRADED) {
      return op_scheduler_class::background_recovery;
    } else {
      return op_scheduler_class::background_best_effort;
    }
  }

public:
  explicit PGOpQueueable(spg_t pg) : pgid(pg) {}
  uint32_t get_queue_token() const final {
    return get_pgid().ps();
  }

  const spg_t& get_ordering_token() const final {
    return get_pgid();
  }
};

class PGOpItem : public PGOpQueueable {
  OpRequestRef op;

public:
  PGOpItem(spg_t pg, OpRequestRef op) : PGOpQueueable(pg), op(std::move(op)) {}

  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGOpItem(op=" << *(op->get_req()) << ")";
  }

  std::string print() const override {
    return fmt::format("PGOpItem(op={})", *(op->get_req()));
  }

  std::optional<OpRequestRef> maybe_get_op() const final {
    return op;
  }

  op_scheduler_class get_scheduler_class() const final {
    auto type = op->get_req()->get_type();
    if (type == CEPH_MSG_OSD_OP ||
	type == CEPH_MSG_OSD_BACKOFF) {
      return op_scheduler_class::client;
    } else {
      return op_scheduler_class::immediate;
    }
  }

  void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
};

class PGPeeringItem : public PGOpQueueable {
  PGPeeringEventRef evt;
public:
  PGPeeringItem(spg_t pg, PGPeeringEventRef e) : PGOpQueueable(pg), evt(e) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGPeeringEvent(" << evt->get_desc() << ")";
  }
  std::string print() const final {
    return fmt::format("PGPeeringEvent({})", evt->get_desc());
  }
  void run(OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  bool is_peering() const override {
    return true;
  }
  bool peering_requires_pg() const override {
    return evt->requires_pg;
  }
  const PGCreateInfo *creates_pg() const override {
    return evt->create_info.get();
  }
  op_scheduler_class get_scheduler_class() const final {
    return op_scheduler_class::immediate;
  }
};

class PGSnapTrim : public PGOpQueueable {
  epoch_t epoch_queued;
public:
  PGSnapTrim(
    spg_t pg,
    epoch_t epoch_queued)
    : PGOpQueueable(pg), epoch_queued(epoch_queued) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGSnapTrim(pgid=" << get_pgid()
	       << " epoch_queued=" << epoch_queued
	       << ")";
  }
  std::string print() const final {
    return fmt::format(
	"PGSnapTrim(pgid={} epoch_queued={})", get_pgid(), epoch_queued);
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return op_scheduler_class::background_best_effort;
  }
};

class PGScrub : public PGOpQueueable {
  epoch_t epoch_queued;
public:
  PGScrub(
    spg_t pg,
    epoch_t epoch_queued)
    : PGOpQueueable(pg), epoch_queued(epoch_queued) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGScrub(pgid=" << get_pgid()
	       << "epoch_queued=" << epoch_queued
	       << ")";
  }
  std::string print() const final {
    return fmt::format(
	"PGScrub(pgid={} epoch_queued={})", get_pgid(), epoch_queued);
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return op_scheduler_class::background_best_effort;
  }
};

class PGScrubItem : public PGOpQueueable {
 protected:
  epoch_t epoch_queued;
  Scrub::act_token_t activation_index;
  std::string_view message_name;
  PGScrubItem(spg_t pg, epoch_t epoch_queued, std::string_view derivative_name)
      : PGOpQueueable{pg}
      , epoch_queued{epoch_queued}
      , activation_index{0}
      , message_name{derivative_name}
  {}
  PGScrubItem(spg_t pg,
	      epoch_t epoch_queued,
	      Scrub::act_token_t op_index,
	      std::string_view derivative_name)
      : PGOpQueueable{pg}
      , epoch_queued{epoch_queued}
      , activation_index{op_index}
      , message_name{derivative_name}
  {}
  std::ostream& print(std::ostream& rhs) const final
  {
    return rhs << message_name << "(pgid=" << get_pgid()
	       << "epoch_queued=" << epoch_queued
	       << " scrub-token=" << activation_index << ")";
  }
  std::string print() const override {
    return fmt::format(
	"{}(pgid={} epoch_queued={} scrub-token={})", message_name, get_pgid(),
	epoch_queued, activation_index);
  }
  void run(OSD* osd,
	   OSDShard* sdata,
	   PGRef& pg,
	   ThreadPool::TPHandle& handle) override = 0;
  op_scheduler_class get_scheduler_class() const final
  {
    return op_scheduler_class::background_best_effort;
  }
};

class PGScrubResched : public PGScrubItem {
 public:
  PGScrubResched(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubResched"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

/**
 *  called when a repair process completes, to initiate scrubbing. No local/remote
 *  resources are allocated.
 */
class PGScrubAfterRepair : public PGScrubItem {
 public:
  PGScrubAfterRepair(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubAfterRepair"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubPushesUpdate : public PGScrubItem {
 public:
  PGScrubPushesUpdate(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubPushesUpdate"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubAppliedUpdate : public PGScrubItem {
 public:
  PGScrubAppliedUpdate(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubAppliedUpdate"}
  {}
  void run(OSD* osd,
	   OSDShard* sdata,
	   PGRef& pg,
	   [[maybe_unused]] ThreadPool::TPHandle& handle) final;
};

class PGScrubUnblocked : public PGScrubItem {
 public:
  PGScrubUnblocked(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubUnblocked"}
  {}
  void run(OSD* osd,
	   OSDShard* sdata,
	   PGRef& pg,
	   [[maybe_unused]] ThreadPool::TPHandle& handle) final;
};

class PGScrubDigestUpdate : public PGScrubItem {
 public:
  PGScrubDigestUpdate(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubDigestUpdate"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubGotReplMaps : public PGScrubItem {
 public:
  PGScrubGotReplMaps(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubGotReplMaps"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGRepScrub : public PGScrubItem {
 public:
  PGRepScrub(spg_t pg, epoch_t epoch_queued, Scrub::act_token_t op_token)
      : PGScrubItem{pg, epoch_queued, op_token, "PGRepScrub"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGRepScrubResched : public PGScrubItem {
 public:
  PGRepScrubResched(spg_t pg, epoch_t epoch_queued, Scrub::act_token_t op_token)
      : PGScrubItem{pg, epoch_queued, op_token, "PGRepScrubResched"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubReplicaPushes : public PGScrubItem {
 public:
  PGScrubReplicaPushes(spg_t pg, epoch_t epoch_queued)
      : PGScrubItem{pg, epoch_queued, "PGScrubReplicaPushes"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubScrubFinished : public PGScrubItem {
 public:
  PGScrubScrubFinished(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubScrubFinished"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubGetNextChunk : public PGScrubItem {
 public:
  PGScrubGetNextChunk(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubGetNextChunk"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubChunkIsBusy : public PGScrubItem {
 public:
  PGScrubChunkIsBusy(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubChunkIsBusy"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGScrubChunkIsFree : public PGScrubItem {
 public:
  PGScrubChunkIsFree(spg_t pg, epoch_t epoch_queued)
    : PGScrubItem{pg, epoch_queued, "PGScrubChunkIsFree"}
  {}
  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

class PGRecovery : public PGOpQueueable {
  utime_t time_queued;
  epoch_t epoch_queued;
  uint64_t reserved_pushes;
  int priority;
public:
  PGRecovery(
    spg_t pg,
    epoch_t epoch_queued,
    uint64_t reserved_pushes,
    int priority)
    : PGOpQueueable(pg),
      time_queued(ceph_clock_now()),
      epoch_queued(epoch_queued),
      reserved_pushes(reserved_pushes),
      priority(priority) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGRecovery(pgid=" << get_pgid()
	       << " epoch_queued=" << epoch_queued
	       << " reserved_pushes=" << reserved_pushes
	       << ")";
  }
  std::string print() const final {
    return fmt::format(
	"PGRecovery(pgid={} epoch_queued={} reserved_pushes={})", get_pgid(),
	epoch_queued, reserved_pushes);
  }
  uint64_t get_reserved_pushes() const final {
    return reserved_pushes;
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return priority_to_scheduler_class(priority);
  }
};

class PGRecoveryContext : public PGOpQueueable {
  utime_t time_queued;
  std::unique_ptr<GenContext<ThreadPool::TPHandle&>> c;
  epoch_t epoch;
  int priority;
public:
  PGRecoveryContext(spg_t pgid,
		    GenContext<ThreadPool::TPHandle&> *c, epoch_t epoch,
		    int priority)
    : PGOpQueueable(pgid),
      time_queued(ceph_clock_now()),
      c(c), epoch(epoch), priority(priority) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGRecoveryContext(pgid=" << get_pgid()
	       << " c=" << c.get() << " epoch=" << epoch
	       << ")";
  }
  std::string print() const final {
    return fmt::format(
	"PGRecoveryContext(pgid={} c={} epoch={})", get_pgid(), (void*)c.get(), epoch);
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return priority_to_scheduler_class(priority);
  }
};

class PGDelete : public PGOpQueueable {
  epoch_t epoch_queued;
public:
  PGDelete(
    spg_t pg,
    epoch_t epoch_queued)
    : PGOpQueueable(pg),
      epoch_queued(epoch_queued) {}
  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGDelete(" << get_pgid()
	       << " e" << epoch_queued
	       << ")";
  }
  std::string print() const final {
    return fmt::format(
	"PGDelete(pgid={} epoch_queued={})", get_pgid(), epoch_queued);
  }
  void run(
    OSD *osd, OSDShard *sdata, PGRef& pg, ThreadPool::TPHandle &handle) final;
  op_scheduler_class get_scheduler_class() const final {
    return op_scheduler_class::background_best_effort;
  }
};

class PGRecoveryMsg : public PGOpQueueable {
  utime_t time_queued;
  OpRequestRef op;

public:
  PGRecoveryMsg(spg_t pg, OpRequestRef op)
    : PGOpQueueable(pg), time_queued(ceph_clock_now()), op(std::move(op)) {}

  static bool is_recovery_msg(OpRequestRef &op) {
    switch (op->get_req()->get_type()) {
    case MSG_OSD_PG_PUSH:
    case MSG_OSD_PG_PUSH_REPLY:
    case MSG_OSD_PG_PULL:
    case MSG_OSD_PG_BACKFILL:
    case MSG_OSD_PG_BACKFILL_REMOVE:
    case MSG_OSD_PG_SCAN:
      return true;
    default:
      return false;
    }
  }

  std::ostream &print(std::ostream &rhs) const final {
    return rhs << "PGRecoveryMsg(op=" << *(op->get_req()) << ")";
  }

  std::string print() const final {
    return fmt::format("PGRecoveryMsg(op={})", *(op->get_req()));
  }

  std::optional<OpRequestRef> maybe_get_op() const final {
    return op;
  }

  op_scheduler_class get_scheduler_class() const final {
    return priority_to_scheduler_class(op->get_req()->get_priority());
  }

  void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle)
      final;
};

}  // namespace ceph::osd::scheduler

template <>
struct fmt::formatter<ceph::osd::scheduler::OpSchedulerItem> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(
      const ceph::osd::scheduler::OpSchedulerItem& opsi,
      FormatContext& ctx) const
  {
    // matching existing op_scheduler_item_t::operator<<() format
    using class_t =
	std::underlying_type_t<ceph::osd::scheduler::op_scheduler_class>;
    const auto qos_cost = opsi.was_queued_via_mclock()
			      ? fmt::format(" qos_cost {}", opsi.qos_cost)
			      : "";
    const auto pushes =
	opsi.get_reserved_pushes()
	    ? fmt::format(" reserved_pushes {}", opsi.get_reserved_pushes())
	    : "";

    return fmt::format_to(
	ctx.out(), "OpSchedulerItem({} {} class_id {} prio {}{} cost {} e{}{})",
	opsi.get_ordering_token(), opsi.qitem->print(),
	static_cast<class_t>(opsi.get_scheduler_class()), opsi.get_priority(),
	qos_cost, opsi.get_cost(), opsi.get_map_epoch(), pushes);
  }
};
