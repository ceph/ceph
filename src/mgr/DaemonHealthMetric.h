// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstdint>
#include <iosfwd>
#include "include/denc.h"

namespace ceph { class Formatter; }

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
  void dump(Formatter *f) const;
  static std::list<DaemonHealthMetric> generate_test_instances();
  std::string get_type_name() const {
    return daemon_metric_name(get_type());
  }

  friend std::ostream& operator<<(std::ostream& out, const DaemonHealthMetric& m);
private:
  daemon_metric type = daemon_metric::NONE;
  daemon_metric_t value;
};
WRITE_CLASS_DENC(DaemonHealthMetric)
