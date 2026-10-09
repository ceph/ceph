// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>

class DoutPrefixProvider;
class RGWFormatterFlusher;
namespace rgw::sal { class Driver; }

struct rgw_admin_meta_list_options {
  std::string metadata_key;
  std::string marker;
  std::optional<int> max_entries;
};

int rgw_admin_meta_list_keys(const DoutPrefixProvider* dpp,
                             rgw::sal::Driver* driver,
                             RGWFormatterFlusher& stream_flusher,
                             const rgw_admin_meta_list_options& opts);
