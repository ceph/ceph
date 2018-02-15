// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ObjectDispatchSpec.h"
#include "include/Context.h"
#include "librbd/io/ObjectDispatcher.h"
#include <boost/variant.hpp>

namespace librbd {
namespace io {

void ObjectDispatchSpec::C_Dispatcher::complete(int r) {
  if (r < 0) {
    finish(r);
    return;
  }

  switch (object_dispatch_spec->dispatch_result) {
  case DISPATCH_RESULT_CONTINUE:
    object_dispatch_spec->send(0);
    break;
  case DISPATCH_RESULT_COMPLETE:
    finish(r);
    break;
  case DISPATCH_RESULT_INVALID:
    assert(false);
    break;
  }
}

void ObjectDispatchSpec::C_Dispatcher::finish(int r) {
  on_finish->complete(r);
  delete object_dispatch_spec;
}

struct ObjectDispatchSpec::SetJournalTid : public boost::static_visitor<void> {
  uint64_t journal_tid;

  SetJournalTid(uint64_t journal_tid) : journal_tid(journal_tid) {
  }

  template <typename T>
  void operator()(T& t) const {
  }

  void operator()(WriteRequestBase& write_request_base) const {
    write_request_base.journal_tid = journal_tid;
  }
};

void ObjectDispatchSpec::send(uint64_t journal_tid) {
  // TODO removed in future commit
  if (journal_tid != 0) {
    boost::apply_visitor(SetJournalTid{journal_tid}, request);
  }

  object_dispatcher->send(this);
}

void ObjectDispatchSpec::fail(int r) {
  assert(r < 0);
  dispatcher_ctx.complete(r);
}

} // namespace io
} // namespace librbd
