// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/shared_mutex.hh>

#include "crimson/common/operation.h"
#include "crimson/osd/osd_operation.h"

namespace crimson::os::seastore {

struct WritePipeline {
  struct ReserveProjectedUsage : OrderedExclusivePhaseT<ReserveProjectedUsage> {
    constexpr static auto type_name = "WritePipeline::reserve_projected_usage";
  } reserve_projected_usage;
  struct OolWritesAndLBAUpdates : UnorderedStageT<OolWritesAndLBAUpdates> {
    constexpr static auto type_name = "UnorderedStage::ool_writes_and_update_lba_stage";
  } ool_writes_and_lba_updates;
  struct Prepare : OrderedExclusivePhaseT<Prepare> {
    constexpr static auto type_name = "WritePipeline::prepare_phase";
  } prepare;
  struct DeviceSubmission : OrderedConcurrentPhaseT<DeviceSubmission> {
    constexpr static auto type_name = "WritePipeline::device_submission_phase";
  } device_submission;
  struct Finalize : OrderedExclusivePhaseT<Finalize> {
    constexpr static auto type_name = "WritePipeline::finalize_phase";
  } finalize;

  using  BlockingEvents = std::tuple<
    ReserveProjectedUsage::BlockingEvent,
    OolWritesAndLBAUpdates::BlockingEvent,
    Prepare::BlockingEvent,
    DeviceSubmission::BlockingEvent,
    Finalize::BlockingEvent
  >;
};

/**
 * PlaceholderOperation
 *
 * Once seastore is more complete, I expect to update the externally
 * facing interfaces to permit passing the osd level operation through.
 * Until then (and for tests likely permanently) we'll use this unregistered
 * placeholder for the pipeline phases necessary for journal correctness.
 */
class PlaceholderOperation : public crimson::osd::PhasedOperationT<PlaceholderOperation> {
public:
  constexpr static auto type = 0U;
  constexpr static auto type_name =
    "crimson::os::seastore::PlaceholderOperation";

  static PlaceholderOperation::IRef create() {
    return IRef{new PlaceholderOperation()};
  }

  PipelineHandle handle;
  WritePipeline::BlockingEvents tracking_events;

  PipelineHandle& get_handle() {
    return handle;
  }
private:
  void dump_detail(ceph::Formatter *f) const final {}
  void print(std::ostream &) const final {}
};

struct OperationProxy {
  OperationRef op;
  OperationProxy(OperationRef op) : op(std::move(op)) {}

  virtual seastar::future<> enter(WritePipeline::ReserveProjectedUsage&) = 0;
  virtual seastar::future<> enter(WritePipeline::OolWritesAndLBAUpdates&) = 0;
  virtual seastar::future<> enter(WritePipeline::Prepare&) = 0;
  virtual seastar::future<> enter(WritePipeline::DeviceSubmission&) = 0;
  virtual seastar::future<> enter(WritePipeline::Finalize&) = 0;

  virtual void exit() = 0;
  virtual seastar::future<> complete() = 0;

  virtual ~OperationProxy() = default;
};

template <typename OpT>
struct OperationProxyT : OperationProxy {
  OperationProxyT(typename OpT::IRef op) : OperationProxy(op) {}

  OpT* that() {
    return static_cast<OpT*>(op.get());
  }
  const OpT* that() const {
    return static_cast<const OpT*>(op.get());
  }

  seastar::future<> enter(WritePipeline::ReserveProjectedUsage& s) final {
    return that()->enter_stage(s);
  }
  seastar::future<> enter(WritePipeline::OolWritesAndLBAUpdates& s) final {
    return that()->enter_stage(s);
  }
  seastar::future<> enter(WritePipeline::Prepare& s) final {
    return that()->enter_stage(s);
  }
  seastar::future<> enter(WritePipeline::DeviceSubmission& s) final {
    return that()->enter_stage(s);
  }
  seastar::future<> enter(WritePipeline::Finalize& s) final {
    return that()->enter_stage(s);
  }

  void exit() final {
    return that()->handle.exit();
  }
  seastar::future<> complete() final {
    return that()->handle.complete();
  }
};

struct OrderingHandle {
  // we can easily optimize this dynalloc out as all concretes are
  // supposed to have exactly the same size.
  std::unique_ptr<OperationProxy> op;
  seastar::shared_mutex *collection_ordering_lock = nullptr;

  // in the future we might add further constructors / template to type
  // erasure while extracting the location of tracking events.
  OrderingHandle(std::unique_ptr<OperationProxy> op) : op(std::move(op)) {}
  OrderingHandle(OrderingHandle &&other)
    : op(std::move(other.op)),
      collection_ordering_lock(other.collection_ordering_lock) {
    other.collection_ordering_lock = nullptr;
  }

  seastar::future<> take_collection_lock(seastar::shared_mutex &mutex) {
    ceph_assert(!collection_ordering_lock);
    collection_ordering_lock = &mutex;
    return collection_ordering_lock->lock();
  }

  void maybe_release_collection_lock() {
    if (collection_ordering_lock) {
      collection_ordering_lock->unlock();
      collection_ordering_lock = nullptr;
    }
  }

  template <typename T>
  seastar::future<> enter(T &t) {
    return op->enter(t);
  }

  void exit() {
    op->exit();
  }

  seastar::future<> complete() {
    return op->complete();
  }

  ~OrderingHandle() {
    maybe_release_collection_lock();
  }
};

inline OrderingHandle get_dummy_ordering_handle() {
  using PlaceholderOpProxy = OperationProxyT<PlaceholderOperation>;
  return OrderingHandle{
    std::make_unique<PlaceholderOpProxy>(PlaceholderOperation::create())};
}

} // namespace crimson::os::seastore

namespace crimson {
  template <>
  struct EventBackendRegistry<os::seastore::PlaceholderOperation> {
    static std::tuple<> get_backends() {
      return {};
    }
  };
} // namespace crimson

