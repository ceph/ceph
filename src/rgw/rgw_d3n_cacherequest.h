// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <mutex>

#include <common/async/yield_context.h>
#include <common/dout.h>

#include "rgw_aio.h"


struct D3nGetObjData {
  std::mutex d3n_lock;
};

// TODO: move under src/rgw/driver/d3n/
namespace rgw::d3n {

// async cache read operation
Aio::OpFunc cache_read_op(const DoutPrefixProvider* dpp, optional_yield y,
                          off_t ofs, off_t len, const std::string& cache_dir);

} // namespace rgw::d3n
