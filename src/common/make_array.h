// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

#pragma once

#include <type_traits>
#include <array>

template<typename CT, typename... Args>
constexpr auto make_array(Args&&... args)
{
  return std::array<CT, sizeof...(Args)>{std::forward<CT>(args)...};
}
