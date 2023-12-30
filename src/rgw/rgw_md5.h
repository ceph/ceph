// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <memory>
#include <string>
#include "rgw_sal_fwd.h"

namespace rgw {

/// Create a DataProcessor filter that calculates the md5sum of processed data
auto create_md5_putobj_pipe(sal::DataProcessor* next,
                            std::string& output)
    -> std::unique_ptr<sal::DataProcessor>;

} // namespace rgw
