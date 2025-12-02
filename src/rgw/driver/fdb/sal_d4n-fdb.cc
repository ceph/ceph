// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "sal_d4n-fdb.h"

#include "rgw_perf_counters.h"

extern "C" {

rgw::sal::Driver* newD4N_FDB_Filter(CephContext* cct, rgw::sal::Driver* /*next*/, boost::asio::io_context& /*io_context*/, bool admin)
{
 // auto *d = new my::type::D4NFilterDriver();
 return nullptr;
}

}
