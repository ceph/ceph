// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 */

#pragma once

#include <functional>
#include <string_view>

#include "include/buffer.h"

typedef std::function<void(int,std::string_view,ceph::buffer::list&)> asok_finisher;
