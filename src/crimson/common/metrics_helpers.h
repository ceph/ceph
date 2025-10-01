// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab
#pragma once

#include <string>

#include <seastar/core/scollectd_api.hh>

#include "common/Formatter.h"

namespace crimson {
namespace metrics {

using registered_metric = seastar::metrics::impl::registered_metric;
using data_type = seastar::metrics::impl::data_type;
using value_map = seastar::metrics::impl::value_map;

void dump_metric_value(
  Formatter* f,
  std::string_view full_name,
  const registered_metric& metric,
  const seastar::metrics::impl::labels_type& labels)
{
  f->open_object_section(full_name);
  for (const auto& [key, value] : labels) {
    f->dump_string(key, value);
  }
  auto value_name = "value";
  switch (auto v = metric(); v.type()) {
  case data_type::GAUGE:
    f->dump_float(value_name, v.d());
    break;
  case data_type::REAL_COUNTER:
    f->dump_float(value_name, v.d());
    break;
  case data_type::COUNTER:
    double val;
    try {
      val = v.ui();
    } catch (std::range_error&) {
      // seastar's cpu steal time may be negative
      val = 0;
    }
    f->dump_unsigned(value_name, val);
    break;
  case data_type::HISTOGRAM: {
    f->open_object_section(value_name);
    auto&& h = v.get_histogram();
    f->dump_float("sum", h.sample_sum);
    f->dump_unsigned("count", h.sample_count);
    f->open_array_section("buckets");
    for (auto i : h.buckets) {
      f->open_object_section("bucket");
      f->dump_float("le", i.upper_bound);
      f->dump_unsigned("count", i.count);
      f->close_section(); // "bucket"
    }
    {
      f->open_object_section("bucket");
      f->dump_string("le", "+Inf");
      f->dump_unsigned("count", h.sample_count);
      f->close_section();
    }
    f->close_section(); // "buckets"
    f->close_section(); // value_name
  }
    break;
  default:
    std::abort();
    break;
  }
  f->close_section(); // full_name
}

template <typename F>
void dump_metric_value_map(
  const value_map &vmap,
  Formatter *f,
  F &&filter)
{
  assert(f);
  for (const auto& [full_name, metric_family]: seastar::scollectd::get_value_map()) {
    if (!std::invoke(filter, full_name)) {
      continue;
    }
    for (const auto& [labels, metric] : metric_family) {
      if (metric && metric->is_enabled()) {
	f->open_object_section(""); // enclosed by array
	dump_metric_value(
	  f, full_name, *metric, labels.labels());
	f->close_section();
      }
    }
  }
}

}
}
