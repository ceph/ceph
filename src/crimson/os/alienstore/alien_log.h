// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef ALIEN_LOG_H
#define ALIEN_LOG_H

#include "log/Log.h"

namespace ceph {
namespace logging {
class SubsystemMap;
}
}

namespace seastar::alien {
  class instance;
}
namespace ceph::logging
{
class CnLog : public ceph::logging::Log
{
  seastar::alien::instance& inst;
  unsigned shard;
  void _flush(EntryVector& q, bool crash) override;
public:
  CnLog(const SubsystemMap *s, seastar::alien::instance& inst, unsigned shard);
  ~CnLog() override;
};
}

#endif
