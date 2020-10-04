// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "io_interrupt_condition.h"
#include "pg.h"

namespace crimson::osd {

IOInterruptCondition::IOInterruptCondition(Ref<PG>& pg)
  : pg(pg), e(pg->get_osdmap_epoch()) {}

epoch_t IOInterruptCondition::get_current_osdmap_epoch() {
  return pg->get_osdmap_epoch();
}

bool IOInterruptCondition::is_stopping() {
  return pg->stopping;
}

bool IOInterruptCondition::is_primary() {
  return pg->is_primary();
}

} // namespace crimson::osd
