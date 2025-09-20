// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CLIENT_DENTRYREF_H
#define CEPH_CLIENT_DENTRYREF_H

#include <boost/intrusive_ptr.hpp>
class Dentry;
void intrusive_ptr_add_ref(Dentry *in);
void intrusive_ptr_release(Dentry *in);
typedef boost::intrusive_ptr<Dentry> DentryRef;
#endif
