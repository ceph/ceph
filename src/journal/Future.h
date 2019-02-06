// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_FUTURE_H
#define CEPH_JOURNAL_FUTURE_H

#include <iosfwd>
#include <string>

#include <boost/intrusive_ptr.hpp>

#include "include/ceph_assert.h"
#include "include/int_types.h"

class Context;

namespace journal {

class FutureImpl;

class Future {
public:
  using FutureImplPtr = boost::intrusive_ptr<class FutureImpl>;

  Future();
  Future(const Future&);
  Future& operator=(const Future&);
  Future(Future&&);
  Future& operator=(Future&&);
  Future(const FutureImplPtr& future_impl);
  ~Future();

  bool is_valid() const {
    return !!m_future_impl;
  }

  void flush(Context *on_safe);
  void wait(Context *on_safe);

  bool is_complete() const;
  int get_return_value() const;

private:
  friend class Journaler;
  friend std::ostream& operator<<(std::ostream&, const Future&);

  const auto& get_future_impl() const {
    return m_future_impl;
  }

  FutureImplPtr m_future_impl;
};

std::ostream &operator<<(std::ostream &os, const Future &future);

} // namespace journal

using journal::operator<<;

#endif // CEPH_JOURNAL_FUTURE_H
