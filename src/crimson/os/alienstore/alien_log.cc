// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "alien_log.h"
#include "log/SubsystemMap.h"
#include <seastar/core/alien.hh>
#include "crimson/common/log.h"

namespace ceph::logging {
CnLog::CnLog(const SubsystemMap *s, seastar::alien::instance& inst, unsigned shard)
  :Log(s)
  ,inst(inst)
  ,shard(shard) {
}

CnLog::~CnLog() {
}

void CnLog::_flush(EntryVector& q, bool crash) {
  std::ignore = seastar::alien::submit_to(inst, shard, [&q] {
    for (auto& it : q) {
      crimson::get_logger(it.m_subsys).log(
        crimson::to_log_level(it.m_prio),
        "{}",
        it.strv());
    }
    return seastar::make_ready_future<>();
  });
  q.clear();
  return;
}

} //namespace ceph::logging
