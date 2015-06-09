// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Future.h"
#include "journal/FutureImpl.h"

namespace journal {

void Future::flush(Context *on_safe) {
  m_future_impl->flush(on_safe);
}

void Future::wait(Context *on_safe) {
  m_future_impl->wait(on_safe);
}

bool Future::is_complete() const {
  return m_future_impl->is_complete();
}

int Future::get_return_value() const {
  return m_future_impl->get_return_value();
}

void intrusive_ptr_add_ref(FutureImpl *p) {
  p->get();
}

void intrusive_ptr_release(FutureImpl *p) {
  p->put();
}

std::ostream &operator<<(std::ostream &os, const Future &future) {
  return os << *future.m_future_impl.get();
}

} // namespace journal

