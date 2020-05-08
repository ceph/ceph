// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "mds/Server.h"
#include "MDSContinuation.h"

MDSContext *MDSContinuation::get_internal_callback(int stage) {
    return new MDSInternalContextWrapper(server->mds, get_callback(stage));
}

MDSIOContextBase *MDSContinuation::get_io_callback(int stage) {
    return new MDSIOContextWrapper(server->mds, get_callback(stage));
}
