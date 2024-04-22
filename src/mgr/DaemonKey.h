// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <ostream>
#include <string>
#include <utility>

// Unique reference to a daemon within a cluster
struct DaemonKey
{
  std::string type; // service type, like "osd", "mon"
  std::string name; // service id / name, like "1", "a"
  static std::pair<DaemonKey, bool> parse(const std::string& s);
};

bool operator<(const DaemonKey& lhs, const DaemonKey& rhs);
std::ostream& operator<<(std::ostream& os, const DaemonKey& key);

namespace ceph {
  std::string to_string(const DaemonKey& key);
}

