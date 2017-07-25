// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_SECURITY_H
#define CEPH_SECURITY_H

namespace ceph { namespace security {

// Mask security sensitive output which would otherwise end up in the logs

const std::string mask(const std::string &candidate);
const std::vector<std::string> mask(const std::vector<std::string> &candidate);

}} // namespace ceph::security

#endif // CEPH_SECURITY_H
