// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_TYPES_H
#define CEPH_LIBRBD_MIGRATION_TYPES_H

#include <string>
#include <utility>

namespace librbd {
namespace migration {

enum UrlScheme {
  URL_SCHEME_HTTP,
  URL_SCHEME_HTTPS,
};

struct UrlSpec {
  UrlSpec() {}
  UrlSpec(UrlScheme scheme, const std::string& host, const std::string& port,
          const std::string& path)
    : scheme(scheme), host(host), port(port), path(path) {
  }

  UrlScheme scheme = URL_SCHEME_HTTP;
  std::string host;
  std::string port = "80";
  std::string path = "/";

};

inline bool operator==(const UrlSpec& lhs, const UrlSpec& rhs) {
  return (lhs.scheme == rhs.scheme &&
          lhs.host == rhs.host &&
          lhs.port == rhs.port &&
          lhs.path == rhs.path);
}

} // namespace migration
} // namespace librbd

#endif // CEPH_LIBRBD_MIGRATION_TYPES_H
