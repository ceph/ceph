// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <boost/asio/random_access_file.hpp>
#include "rgw_aio.h"

namespace rgw {

Aio::OpFunc file_read_op(boost::asio::random_access_file& file,
                         uint64_t offset, uint64_t len);

Aio::OpFunc file_write_op(boost::asio::random_access_file& file,
                          uint64_t offset, bufferlist bl);

} // namespace rgw
