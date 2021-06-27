// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_interval_interrupt_condition.h"
#include "pg.h"

#include "crimson/common/log.h"

namespace crimson::osd {

IOInterruptCondition::IOInterruptCondition(Ref<PG>& pg)
  : pg(pg), e(pg->get_osdmap_epoch()) {}

IOInterruptCondition::~IOInterruptCondition() {
  // for the sake of forward declaring PG (which is a detivate of
  // intrusive_ref_counter<...>)
}

bool IOInterruptCondition::new_interval_created() {
  bool ret = e < pg->get_interval_start_epoch();
  if (ret)
    ::crimson::get_logger(ceph_subsys_osd).debug(
      "{} new interval, should interrupt, e{}", *pg, e);
  return ret;
}

bool IOInterruptCondition::is_stopping() {
  if (pg->stopping)
    ::crimson::get_logger(ceph_subsys_osd).debug(
      "{} shutting down, should interrupt", *pg);
  return pg->stopping;
}

bool IOInterruptCondition::is_primary() {
  return pg->is_primary();
}

} // namespace crimson::osd
