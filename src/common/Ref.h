// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_REF_H
#define CEPH_REF_H

#include <boost/smart_ptr/intrusive_ptr.hpp>

namespace ceph {

template<typename T>
using ref_t = boost::intrusive_ptr<T>;
template<typename T>
using cref_t = boost::intrusive_ptr<T const>;

}

#endif
