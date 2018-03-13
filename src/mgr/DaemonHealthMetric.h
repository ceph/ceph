// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include "include/denc.h"

enum class daemon_metric : uint8_t {
  SLOW_OPS,
  PENDING_CREATING_PGS,
  NONE,
};

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
private:
  daemon_metric type = daemon_metric::NONE;
  daemon_metric_t value;
};
WRITE_CLASS_DENC(DaemonHealthMetric)
