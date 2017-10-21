// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/intrusive_ptr.hpp>

#include "msg/msg_types.h"

class Message;
using MessageRef = boost::intrusive_ptr<Message>;

namespace ceph::net {

class Connection;
using ConnectionRef = boost::intrusive_ptr<Connection>;

class Dispatcher;

class Messenger;

} // namespace ceph::net

