// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LOG_SUBSYSTEMS
#define CEPH_LOG_SUBSYSTEMS

#include <string>
#include <vector>
#include <algorithm>

#include "common/subsys_types.h"

#include "include/assert.h"

namespace ceph {
namespace logging {

class SubsystemMap {
  // Access to the current gathering levels must be *FAST* as they are
  // read over and over from all places in the code (via should_gather()
  // by i.e. dout).
  std::array<uint8_t, ceph_subsys_get_num()> m_gather_levels;

  // The rest. Should be as small as possible to not unnecessarily
  // enlarge md_config_t and spread it other elements across cache
  // lines. Access can be slow.
  std::vector<ceph_subsys_item_t> m_subsys;

  friend class Log;

public:
  SubsystemMap() {
    constexpr auto s = ceph_subsys_get_as_array();
    m_subsys.reserve(s.size());

    std::size_t i = 0;
    for (const ceph_subsys_item_t& item : s) {
      m_subsys.emplace_back(item);
      m_gather_levels[i++] = std::max(item.log_level, item.gather_level);
    }
  }

  constexpr static std::size_t get_num() {
    return ceph_subsys_get_num();
  }

  constexpr static std::size_t get_max_subsys_len() {
    return ceph_subsys_max_name_length();
  }

  int get_log_level(unsigned subsys) const {
    if (subsys >= get_num())
      subsys = 0;
    return m_subsys[subsys].log_level;
  }

  int get_gather_level(unsigned subsys) const {
    if (subsys >= get_num())
      subsys = 0;
    return m_subsys[subsys].gather_level;
  }

  // TODO(rzarzynski): move to string_view?
  constexpr const char* get_name(unsigned subsys) const {
    if (subsys >= get_num())
      subsys = 0;
    return ceph_subsys_get_as_array()[subsys].name;
  }

  template <unsigned SubV, int LvlV>
  bool should_gather() {
    static_assert(SubV < get_num(), "wrong subsystem ID");
    static_assert(LvlV >= -1 && LvlV <= 200);

    if constexpr (LvlV <= 0) {
      // handle the -1 and 0 levels entirely at compile-time.
      // Such debugs are intended be gathered regardless even
      // of the user configuration.
      return true;
    } else {
      return LvlV <= static_cast<int>(m_gather_levels[SubV]);
    }
  }
  bool should_gather(const unsigned sub, int level) {
    assert(sub < m_subsys.size());
    return level <= static_cast<int>(m_gather_levels[sub]);
  }

  void set_log_level(unsigned subsys, uint8_t log)
  {
    assert(subsys < m_subsys.size());
    m_subsys[subsys].log_level = log;
    m_gather_levels[subsys] = \
      std::max(log, m_subsys[subsys].gather_level);
  }

  void set_gather_level(unsigned subsys, uint8_t gather)
  {
    assert(subsys < m_subsys.size());
    m_subsys[subsys].gather_level = gather;
    m_gather_levels[subsys] = \
      std::max(m_subsys[subsys].log_level, gather);
  }
};

}
}

#endif
