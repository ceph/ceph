// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <stdexcept>

namespace rgw::store {

// generic database exception
struct database_exception : std::runtime_error {
  using std::runtime_error::runtime_error;
};

} // namespace rgw::store
