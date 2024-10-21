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

#include <set>

#include "denc.h"

template<typename T, typename ...Ts>
struct denc_traits<
  std::set<T, Ts...>,
  std::enable_if_t<denc_traits<T>::supported>>
  : public _denc::container_base<std::set,
				 _denc::setlike_details<std::set<T, Ts...>>,
				 T, Ts...> {};
