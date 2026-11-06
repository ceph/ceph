// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "common/Formatter.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/scrub/pg_scrubber.h"
#include "osd/osd_types.h"
#include "osd/SnapMapReaderI.h"
#include "peering_event.h"

namespace crimson::osd {

class PG;

template <typename T>
class RemoteScrubEventBaseT :
    public PhasedOperationT<T>,
    public RemoteOperation {
  T* that() {
    return static_cast<T*>(this);
  }
  const T* that() const {
    return static_cast<const T*>(this);
  }

  PipelineHandle handle;

  spg_t pgid;

protected:
  using interruptor = InterruptibleOperation::interruptor;
  epoch_t epoch;

  template <typename U=void>
  using ifut = InterruptibleOperation::interruptible_future<U>;

  virtual ifut<> handle_event(PG &pg) = 0;
public:
  RemoteScrubEventBaseT(
    crimson::net::ConnectionRef conn, epoch_t epoch, spg_t pgid)
    : RemoteOperation(std::move(conn)), pgid(pgid), epoch(epoch) {}

  PGPeeringPipeline &get_peering_pipeline(PG &pg);

  ConnectionPipeline &get_connection_pipeline();

  PerShardPipeline &get_pershard_pipeline(ShardServices &);

  static constexpr bool can_create() { return false; }

  spg_t get_pgid() const {
    return pgid;
  }

  PipelineHandle &get_handle() { return handle; }
  epoch_t get_epoch() const { return epoch; }

  seastar::future<> with_pg(
    ShardServices &shard_services, Ref<PG> pg);

  std::tuple<
    class TrackableOperationT<T>::StartEvent,
    ConnectionPipeline::AwaitActive::BlockingEvent,
    ConnectionPipeline::AwaitMap::BlockingEvent,
    OSD_OSDMapGate::OSDMapBlocker::BlockingEvent,
    ConnectionPipeline::GetPGMapping::BlockingEvent,
    PerShardPipeline::CreateOrWaitPG::BlockingEvent,
    PGMap::PGCreationBlockingEvent,
    PGPeeringPipeline::AwaitMap::BlockingEvent,
    PG_OSDMapGate::OSDMapBlocker::BlockingEvent,
    PGPeeringPipeline::Process::BlockingEvent,
    class TrackableOperationT<T>::CompletionEvent
  > tracking_events;

  virtual ~RemoteScrubEventBaseT() = default;
};

class ScrubRequested final : public RemoteScrubEventBaseT<ScrubRequested> {
  bool deep = false;
  bool repair = false;
protected:
  ifut<> handle_event(PG &pg) final;

public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_requested;

  template <typename... Args>
  ScrubRequested(bool deep, bool repair, Args&&... base_args)
    : RemoteScrubEventBaseT<ScrubRequested>(std::forward<Args>(base_args)...),
      deep(deep), repair(repair) {}

  epoch_t get_epoch_sent_at() const {
    return epoch;
  }

  void print(std::ostream &out) const final {
    out << "(deep=" << deep << ", repair=" << repair << ")";
  }
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_bool("deep", deep);
    f->dump_bool("repair", repair);
  }

};

class ScrubMessage final : public RemoteScrubEventBaseT<ScrubMessage> {
  MessageRef m;
protected:
  ifut<> handle_event(PG &pg) final;

public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_message;

  template <typename... Args>
  ScrubMessage(MessageRef m, Args&&... base_args)
    : RemoteScrubEventBaseT<ScrubMessage>(std::forward<Args>(base_args)...),
      m(m) {
    ceph_assert(scrub::PGScrubber::is_scrub_message(*m));
  }

  epoch_t get_epoch_sent_at() const {
    return epoch;
  }

  void print(std::ostream &out) const final {
    out << "(m=" << *m << ")";
  }
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_stream("m") << *m;
  }

};

template <typename T>
class ScrubAsyncOpT : public TrackableOperationT<T> {
  Ref<PG> pg;
  const bool scheduled;

public:
  using interruptor = InterruptibleOperation::interruptor;
  template <typename U=void>
  using ifut = InterruptibleOperation::interruptible_future<U>;

  ScrubAsyncOpT(Ref<PG> pg, bool scheduled = true);

  ifut<> start();

  virtual ~ScrubAsyncOpT() = default;

protected:
  virtual ifut<> run(PG &pg) = 0;
};

class ScrubFindRange : public ScrubAsyncOpT<ScrubFindRange> {
  hobject_t begin;
public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_find_range;

  template <typename... Args>
  ScrubFindRange(const hobject_t &begin, Args&&... args)
    : ScrubAsyncOpT(std::forward<Args>(args)...), begin(begin) {}

  void print(std::ostream &out) const final {
    out << "(begin=" << begin << ")";
  }
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_stream("begin") << begin;
  }


protected:
  ifut<> run(PG &pg) final;
};

class ScrubReserveRange : public ScrubAsyncOpT<ScrubReserveRange> {
  hobject_t begin;
  hobject_t end;

  /// see run(), used to unlock background_io_mutex on interval change
  bool blocked_set = false;
public:
  static constexpr OperationTypeCode type =
    OperationTypeCode::scrub_reserve_range;

  template <typename... Args>
  ScrubReserveRange(const hobject_t &begin, const hobject_t &end, Args&&... args)
    : ScrubAsyncOpT(std::forward<Args>(args)...), begin(begin), end(end) {}

  void print(std::ostream &out) const final {
    out << "(begin=" << begin << ", end=" << end << ")";
  }
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_stream("begin") << begin;
    f->dump_stream("end") << end;
  }


protected:
  ifut<> run(PG &pg) final;
};

class ScrubScan : public ScrubAsyncOpT<ScrubScan> {
  /// deep or shallow scrub
  const bool deep;

  /// true: send event locally, false: send result to primary
  const bool local;

  /// object range to scan: [begin, end)
  const hobject_t begin;
  const hobject_t end;

  /// result, see local
  ScrubMap ret;

  ifut<> scan_object(PG &pg, const ghobject_t &obj);
  ifut<> deep_scan_object(PG &pg, const ghobject_t &obj);

public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_scan;

  void print(std::ostream &out) const final {
    out << "(deep=" << deep
	<< ", local=" << local
	<< ", begin=" << begin
	<< ", end=" << end
	<< ")";
  }
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_bool("deep", deep);
    f->dump_bool("local", local);
    f->dump_stream("begin") << begin;
    f->dump_stream("end") << end;
  }

  ScrubScan(
    Ref<PG> pg, bool deep, bool local,
    const hobject_t &begin, const hobject_t &end)
    : ScrubAsyncOpT(pg), deep(deep), local(local), begin(begin), end(end) {}

protected:
  ifut<> run(PG &pg) final;
};

class ScrubSleep : public ScrubAsyncOpT<ScrubSleep> {
public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_sleep;

  ScrubSleep(Ref<PG> pg)
    : ScrubAsyncOpT(pg, false) {}

  void print(std::ostream &out) const final {
    out << "()";
  }
  void dump_detail(ceph::Formatter *f) const final {
  }

protected:
  ifut<> run(PG &pg) final;
};

class ScrubDigestUpdate : public ScrubAsyncOpT<ScrubDigestUpdate> {
  hobject_t oid;
  std::optional<uint32_t> data_digest;
  std::optional<uint32_t> omap_digest;
  uint64_t generation;

public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_digest_update;

  ScrubDigestUpdate(
    Ref<PG> pg,
    const hobject_t &oid,
    std::optional<uint32_t> data_digest,
    std::optional<uint32_t> omap_digest,
    uint64_t generation)
    : ScrubAsyncOpT(pg), oid(oid),
      data_digest(data_digest), omap_digest(omap_digest),
      generation(generation) {}

  void print(std::ostream &out) const final {
    out << "(oid=" << oid << ")";
  }
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_stream("oid") << oid;
  }

protected:
  ifut<> run(PG &pg) final;
};

/**
 * ScrubSnapMapperRepair
 *
 * Background operation that validates and repairs a single clone's snap mapper
 * entry.  Spawned by PGScrubber::scan_snaps() for each clone found in the
 * scrub map.  All blocking KV reads and writes happen inside interruptor::async
 * so they never run on the Seastar reactor thread.
 */
class ScrubSnapMapperRepair : public ScrubAsyncOpT<ScrubSnapMapperRepair> {
  hobject_t hoid;
  std::set<snapid_t> expected_snaps;  ///< canonical snaps from snapset SS_ATTR

public:
  static constexpr OperationTypeCode type =
    OperationTypeCode::scrub_snap_mapper_repair;

  ScrubSnapMapperRepair(
    Ref<PG> pg,
    const hobject_t &hoid,
    const std::set<snapid_t> &expected_snaps)
    : ScrubAsyncOpT(pg), hoid(hoid), expected_snaps(expected_snaps) {}

  void print(std::ostream &out) const final {
    out << "(hoid=" << hoid << ")";
  }
  void dump_detail(ceph::Formatter *f) const final {
    f->dump_stream("hoid") << hoid;
  }

protected:
  ifut<> run(PG &pg) final;
};

struct obj_scrub_progress_t {
  // nullopt once complete
  std::optional<uint64_t> offset = 0;
  ceph::buffer::hash data_hash{std::numeric_limits<uint32_t>::max()};

  bool header_done = false;
  std::optional<std::string> next_key;
  bool keys_done = false;
  ceph::buffer::hash omap_hash{std::numeric_limits<uint32_t>::max()};
};

}

namespace crimson {

template <>
struct EventBackendRegistry<osd::ScrubRequested> {
  static std::tuple<> get_backends() {
    return {};
  }
};

template <>
struct EventBackendRegistry<osd::ScrubMessage> {
  static std::tuple<> get_backends() {
    return {};
  }
};

template <>
struct EventBackendRegistry<osd::ScrubDigestUpdate> {
  static std::tuple<> get_backends() {
    return {};
  }
};

template <>
struct EventBackendRegistry<osd::ScrubSnapMapperRepair> {
  static std::tuple<> get_backends() {
    return {};
  }
};

}

template <>
struct fmt::formatter<crimson::osd::obj_scrub_progress_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const crimson::osd::obj_scrub_progress_t &progress,
	      FormatContext& ctx) const
  {
    return fmt::format_to(
      ctx.out(),
      "obj_scrub_progress_t(offset: {}, "
      "header_done: {}, next_key: {}, keys_done: {})",
      progress.offset.has_value() ? *progress.offset : 0,
      progress.header_done,
      progress.next_key.has_value() ? *progress.next_key : "",
      progress.keys_done);
  }
};

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::osd::ScrubRequested>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubMessage>
  : fmt::ostream_formatter {};

template <typename T>
struct fmt::formatter<crimson::osd::ScrubAsyncOpT<T>>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubFindRange>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubReserveRange>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubScan>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubSleep>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubDigestUpdate>
  : fmt::ostream_formatter {};

template <> struct fmt::formatter<crimson::osd::ScrubSnapMapperRepair>
  : fmt::ostream_formatter {};

#endif
