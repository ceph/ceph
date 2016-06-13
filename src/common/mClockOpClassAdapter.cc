// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <memory>

#include "mClockOpClassAdapter.h"

namespace ceph {
    std::unique_ptr<mclock_op_tags_t> mclock_op_tags(nullptr);

    dmc::ClientInfo op_class_client_info_f(const osd_op_type_t& op_type) {
        static dmc::ClientInfo _default(1.0, 1.0, 1.0);
        return _default;
    }
}
