// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstddef>

#ifdef CEPH_SANITIZED  // set in top-level CMakeLists.txt for any sanitizer
inline constexpr bool bench_under_sanitizer = true;
#else
inline constexpr bool bench_under_sanitizer = false;
#endif

// For counts that only repeat the same work; the distinct cases (sizes,
// algorithms, ...) must stay in an outer loop. If the count also shapes the
// data, pass the smallest count that still covers the case as reduced.
inline constexpr size_t sanitized_bench_rounds(size_t full,
                                               size_t reduced = 2) {
  return (bench_under_sanitizer && full > reduced) ? reduced : full;
}
