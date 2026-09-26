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

#include <boost/container/small_vector.hpp>

#include "denc.h"

template<typename T, std::size_t N, typename ...Ts>
struct denc_traits<
  boost::container::small_vector<T, N, Ts...>,
  typename std::enable_if_t<denc_traits<T>::supported>>
  : public _denc::container_base<
      boost::container::small_vector<T, N, Ts...>,
      _denc::pushback_details<
        boost::container::small_vector<T, N, Ts...>>> {};
