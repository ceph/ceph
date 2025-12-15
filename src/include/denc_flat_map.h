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

#include <boost/container/flat_map.hpp>

#include "denc.h"

template<typename A, typename B, typename ...Ts>
struct denc_traits<
  boost::container::flat_map<A, B, Ts...>,
  std::enable_if_t<denc_traits<A>::supported &&
		   denc_traits<B>::supported>>
  : public _denc::container_base<
  boost::container::flat_map,
  _denc::maplike_details<boost::container::flat_map<
			   A, B, Ts...>>,
  A, B, Ts...> {};
