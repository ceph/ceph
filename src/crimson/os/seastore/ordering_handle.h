// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/shared_mutex.hh>

#include "crimson/common/operation.h"

namespace crimson::os::seastore {

/**
 * PlaceholderOperation
 *
 * Once seastore is more complete, I expect to update the externally
 * facing interfaces to permit passing the osd level operation through.
 * Until then (and for tests likely permanently) we'll use this unregistered
 * placeholder for the pipeline phases necessary for journal correctness.
 */
class PlaceholderOperation : public Operation {
public:
  using IRef = boost::intrusive_ptr<PlaceholderOperation>;

  unsigned get_type() const final {
    return 0;
  }

  const char *get_type_name() const final {
    return "crimson::os::seastore::PlaceholderOperation";
  }

private:
  void dump_detail(ceph::Formatter *f) const final {}
  void print(std::ostream &) const final {}
};

struct OrderingHandle {
  OperationRef op;
  PipelineHandle phase_handle;
  seastar::shared_mutex *collection_ordering_lock = nullptr;

  OrderingHandle(OperationRef &&op) : op(std::move(op)) {}
  OrderingHandle(OrderingHandle &&other)
    : op(std::move(other.op)), phase_handle(std::move(other.phase_handle)),
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
    return op->with_blocking_future(phase_handle.enter(t));
  }

  void exit() {
    return phase_handle.exit();
  }

  seastar::future<> complete() {
    return phase_handle.complete();
  }

  ~OrderingHandle() {
    maybe_release_collection_lock();
  }
};

inline OrderingHandle get_dummy_ordering_handle() {
  return OrderingHandle{new PlaceholderOperation};
}

struct WritePipeline {
  OrderedExclusivePhase prepare{
    "WritePipeline::prepare_phase"
  };
  OrderedConcurrentPhase device_submission{
    "WritePipeline::device_submission_phase"
  };
  OrderedExclusivePhase finalize{
    "WritePipeline::finalize_phase"
  };
};

}
