// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "DaemonHealthMetric.h"

#include <ostream>

#include "DaemonHealthMetric.h"
#include "common/Formatter.h"

void DaemonHealthMetric::dump(Formatter *f) const {
  f->dump_string("type", get_type_name());
  f->dump_int("n", get_n());
  f->dump_int("n1", get_n1());
  f->dump_int("n2", get_n2());
}

std::list<DaemonHealthMetric> DaemonHealthMetric::generate_test_instances() {
  std::list<DaemonHealthMetric> o;
  o.push_back(DaemonHealthMetric(daemon_metric::SLOW_OPS, 1));
  o.push_back(DaemonHealthMetric(daemon_metric::PENDING_CREATING_PGS, 1, 2));
  return o;
}

std::ostream& operator<<(std::ostream& out, const DaemonHealthMetric& m) {
  return out << daemon_metric_name(m.get_type()) << "("
	     << m.get_n() << "|(" << m.get_n1() << "," << m.get_n2() << "))";
}
