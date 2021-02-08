// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/Future.h"
#include "journal/FutureImpl.h"
#include "include/ceph_assert.h"

namespace journal {

Future::Future() = default;
Future::Future(const Future& o) = default;
Future& Future::operator=(const Future& o) = default;
Future::Future(Future&& o) = default;
Future& Future::operator=(Future&& o) = default;
Future::Future(ceph::ref_t<FutureImpl> future_impl) : m_future_impl(std::move(future_impl)) {}
Future::~Future() = default;

void Future::flush(Context *on_safe) {
  m_future_impl->flush(on_safe);
}

void Future::wait(Context *on_safe) {
  ceph_assert(on_safe != NULL);
  m_future_impl->wait(on_safe);
}

bool Future::is_complete() const {
  return m_future_impl->is_complete();
}

int Future::get_return_value() const {
  return m_future_impl->get_return_value();
}

std::ostream &operator<<(std::ostream &os, const Future &future) {
  return os << *future.m_future_impl;
}

} // namespace journal

