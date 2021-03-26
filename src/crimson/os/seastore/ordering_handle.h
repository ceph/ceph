// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

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
};

inline OrderingHandle get_dummy_ordering_handle() {
  return OrderingHandle{new PlaceholderOperation, {}};
}

struct WritePipeline {
  OrderedExclusivePhase wait_throttle{
    "TransactionManager::wait_throttle"
  };
  OrderedExclusivePhase prepare{
    "TransactionManager::prepare_phase"
  };
  OrderedConcurrentPhase device_submission{
    "TransactionManager::journal_phase"
  };
  OrderedExclusivePhase finalize{
    "TransactionManager::finalize_phase"
  };
};

}
