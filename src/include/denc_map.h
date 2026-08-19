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

#include <map>

#include "denc.h"

template<typename A, typename B, typename ...Ts>
struct denc_traits<
  std::map<A, B, Ts...>,
  std::enable_if_t<denc_traits<A>::supported &&
		   denc_traits<B>::supported>>
  : public _denc::container_base<std::map,
				 _denc::maplike_details<std::map<A, B, Ts...>>,
				 A, B, Ts...> {};
