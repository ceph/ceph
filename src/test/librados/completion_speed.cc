// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/rados/librados.hpp"
#include "common/ceph_context.h"
#include "common/Finisher.h"
#include "librados/AioCompletionImpl.h"


constexpr int max_completions = 10'000'000;
int completed = 0;
auto cct = (new CephContext(CEPH_ENTITY_TYPE_CLIENT))->get();
Finisher f(cct);

void completion_cb(librados::completion_t cb, void* arg) {
  auto c = static_cast<librados::AioCompletion*>(arg);
  delete c;
  if (++completed < max_completions) {
    auto aio = librados::Rados::aio_create_completion();
    aio->set_complete_callback(static_cast<void*>(aio), &completion_cb);
    f.queue(new librados::C_AioComplete(aio->pc));
  }
}

int main(void) {
  auto aio = librados::Rados::aio_create_completion();
  aio->set_complete_callback(static_cast<void*>(aio), &completion_cb);
  f.queue(new librados::C_AioComplete(aio->pc));
  f.start();

  while (completed < max_completions)
    f.wait_for_empty();

  f.stop();

  assert(completed == max_completions);
  cct->put();
}
