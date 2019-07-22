// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COMMON_REF_H
#define COMMON_REF_H

#include <boost/intrusive_ptr.hpp>

namespace ceph {
template<typename T> using ref_t = boost::intrusive_ptr<T>;
template<typename T> using cref_t = boost::intrusive_ptr<const T>;
template<class T, class U>
boost::intrusive_ptr<T> ref_cast(const boost::intrusive_ptr<U>& r) noexcept {
  return static_cast<T*>(r.get());
}
template<class T, class U>
boost::intrusive_ptr<T> ref_cast(boost::intrusive_ptr<U>&& r) noexcept {
  return {static_cast<T*>(r.detach()), false};
}
template<class T, class U>
boost::intrusive_ptr<const T> ref_cast(const boost::intrusive_ptr<const U>& r) noexcept {
  return static_cast<const T*>(r.get());
}
}

#endif
