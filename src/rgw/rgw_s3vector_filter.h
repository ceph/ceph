// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <optional>
#include <string>
#include <vector>
#include "rgw_s3vector.h"

struct LanceDBExpr;
class DoutPrefixProvider;

namespace rgw::s3vector {

struct FilterExprs {
  LanceDBExpr* column_expr = nullptr;
  LanceDBExpr* json_expr = nullptr;
};

std::optional<FilterExprs> build_filter_expr(
    JSONObj& filter_obj,
    const std::vector<filterable_metadata_key_t>& filterable_keys,
    const std::vector<std::string>& nonfilterable_keys,
    DoutPrefixProvider* dpp,
    std::vector<validation_error_t>& errors);

}
