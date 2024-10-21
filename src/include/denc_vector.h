// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Allen Samuels <allen.samuels@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <vector>

#include "denc.h"

template<typename T, typename ...Ts>
struct denc_traits<
  std::vector<T, Ts...>,
  typename std::enable_if_t<denc_traits<T>::supported>>
  : public _denc::container_base<std::vector,
				 _denc::pushback_details<std::vector<T, Ts...>>,
				 T, Ts...> {};
