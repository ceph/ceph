#ifndef __CEPH_STRINGIFY_H
#define __CEPH_STRINGIFY_H

#include <string>
#include <sstream>

template<typename T>
inline std::string stringify(T a) {
  std::stringstream ss;
  ss << a;
  return ss.str();
}

#endif
