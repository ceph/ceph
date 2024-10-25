#ifndef __CEPH_STRINGIFY_H
#define __CEPH_STRINGIFY_H

#include <string>
#include <sstream>

template<typename T>
inline std::string stringify(const T& a) {
#if defined(__GNUC__) && !(defined(__clang__) || defined(__INTEL_COMPILER))
  static __thread std::ostringstream ss;
  ss.str("");
#else
  std::ostringstream ss;
#endif
  ss << a;
  return ss.str();
}

template <class T, class A>
T joinify(const A &begin, const A &end, const T &t)
{
  T result;
  for (A it = begin; it != end; it++) {
    if (!result.empty())
      result.append(t);
    result.append(*it);
  }
  return result;
}

#endif
