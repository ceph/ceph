// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef COMMON_REF_H
#define COMMON_REF_H

#include <boost/intrusive_ptr.hpp>

namespace ceph {
template<typename T> using ref_t = boost::intrusive_ptr<T>;
template<typename T> using cref_t = boost::intrusive_ptr<const T>;
template<class T, class U>
ref_t<T> ref_cast(const ref_t<U>& r) noexcept {
  return static_cast<T*>(r.get());
}
template<class T, class U>
ref_t<T> ref_cast(ref_t<U>&& r) noexcept {
  return {static_cast<T*>(r.detach()), false};
}
template<class T, class U>
cref_t<T> ref_cast(const cref_t<U>& r) noexcept {
  return static_cast<const T*>(r.get());
}
template<class T, typename... Args>
ceph::ref_t<T> make_ref(Args&&... args) {
  return {new T(std::forward<Args>(args)...), false};
}
}

// Friends cannot be partial specializations: https://en.cppreference.com/w/cpp/language/friend
#define FRIEND_MAKE_REF(C) \
template<class T, typename... Args> friend ceph::ref_t<T> ceph::make_ref(Args&&... args)

#endif
