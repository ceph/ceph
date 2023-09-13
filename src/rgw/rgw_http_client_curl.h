// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 SUSE Linux GmBH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <map>
#include <boost/optional.hpp>
#include "rgw_frontend.h"

namespace rgw {
namespace curl {
using fe_map_t = std::multimap <std::string, RGWFrontendConfig *>;

void setup_curl(boost::optional<const fe_map_t&> m);
void cleanup_curl();
}
}
