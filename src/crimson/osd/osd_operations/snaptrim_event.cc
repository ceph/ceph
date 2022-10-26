// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/osd_operations/snaptrim_event.h"
#include "crimson/osd/pg.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

void SnapTrimEvent::print(std::ostream &lhs) const
{
  lhs << "SnapTrimEvent("
      << "pgid=" << pg->get_pgid()
      << " snapid=" << snapid
      << ")";
}

void SnapTrimEvent::dump_detail(Formatter *f) const
{
  f->open_object_section("SnapTrimEvent");
  f->dump_stream("pgid") << pg->get_pgid();
  f->close_section();
}

seastar::future<> SnapTrimEvent::start()
{
  logger().debug("{}", __func__);
  return seastar::now();
}

} // namespace crimson::osd
