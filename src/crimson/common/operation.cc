// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "operation.h"
#include "common/Formatter.h"

namespace crimson {

void Operation::dump(ceph::Formatter* f) const
{
  f->open_object_section("operation");
  f->dump_string("type", get_type_name());
  f->dump_unsigned("id", id);
  {
    f->open_object_section("detail");
    dump_detail(f);
    f->close_section();
  }
  f->open_array_section("blockers");
  for (auto &blocker : blockers) {
    blocker->dump(f);
  }
  f->close_section();
  f->close_section();
}

void Operation::dump_brief(ceph::Formatter* f) const
{
  f->open_object_section("operation");
  f->dump_string("type", get_type_name());
  f->dump_unsigned("id", id);
  f->close_section();
}

std::ostream &operator<<(std::ostream &lhs, const Operation &rhs) {
  lhs << rhs.get_type_name() << "(id=" << rhs.get_id() << ", detail=";
  rhs.print(lhs);
  lhs << ")";
  return lhs;
}

void Blocker::dump(ceph::Formatter* f) const
{
  f->open_object_section("blocker");
  f->dump_string("op_type", get_type_name());
  {
    f->open_object_section("detail");
    dump_detail(f);
    f->close_section();
  }
  f->close_section();
}

void AggregateBlocker::dump_detail(ceph::Formatter *f) const
{
  f->open_array_section("parent_blockers");
  for (auto b : parent_blockers) {
    f->open_object_section("parent_blocker");
    b->dump(f);
    f->close_section();
  }
  f->close_section();
}

void OrderedExclusivePhase::dump_detail(ceph::Formatter* f) const
{
}

void OrderedConcurrentPhase::dump_detail(ceph::Formatter* f) const
{
}

}
