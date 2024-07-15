// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <ostream>
#include "common/Formatter.h"
#include "include/denc.h"

enum class daemon_metric : uint8_t {
  SLOW_OPS,
  PENDING_CREATING_PGS,
  NONE,
};

static inline const char *daemon_metric_name(daemon_metric t) {
  switch (t) {
  case daemon_metric::SLOW_OPS: return "SLOW_OPS";
  case daemon_metric::PENDING_CREATING_PGS: return "PENDING_CREATING_PGS";
  case daemon_metric::NONE: return "NONE";
  default: return "???";
  }
}

union daemon_metric_t {
  struct {
    uint32_t n1;
    uint32_t n2;
  };
  uint64_t n;
  daemon_metric_t(uint32_t x, uint32_t y)
    : n1(x), n2(y)
  {}
  daemon_metric_t(uint64_t x = 0)
    : n(x)
  {}
};

class DaemonHealthMetric
{
public:
  DaemonHealthMetric() = default;
  DaemonHealthMetric(daemon_metric type_, uint64_t n)
    : type(type_), value(n)
  {}
  DaemonHealthMetric(daemon_metric type_, uint32_t n1, uint32_t n2)
    : type(type_), value(n1, n2) 
  {}

  daemon_metric get_type() const {
    return type;
  }
  uint64_t get_n() const {
    return value.n;
  }
  uint32_t get_n1() const {
    return value.n1;
  }
  uint32_t get_n2() const {
    return value.n2;
  }

  DENC(DaemonHealthMetric, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.value.n, p);
    DENC_FINISH(p);
  }
  void dump(Formatter *f) const {
    f->dump_string("type", get_type_name());
    f->dump_int("n", get_n());
    f->dump_int("n1", get_n1());
    f->dump_int("n2", get_n2());
  }
  static void generate_test_instances(std::list<DaemonHealthMetric*>& o) {
    o.push_back(new DaemonHealthMetric(daemon_metric::SLOW_OPS, 1));
    o.push_back(new DaemonHealthMetric(daemon_metric::PENDING_CREATING_PGS, 1, 2));
  }
  std::string get_type_name() const {
    return daemon_metric_name(get_type());
  }

  friend std::ostream& operator<<(std::ostream& out, const DaemonHealthMetric& m) {
    return out << daemon_metric_name(m.get_type()) << "("
	       << m.get_n() << "|(" << m.get_n1() << "," << m.get_n2() << "))";
  }
private:
  daemon_metric type = daemon_metric::NONE;
  daemon_metric_t value;
};
WRITE_CLASS_DENC(DaemonHealthMetric)
