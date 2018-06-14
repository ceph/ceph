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

#include "Errors.h"
#include "msg/msg_types.h"
#include "msg/Message.h"

using peer_type_t = int;
using auth_proto_t = int;

class Message;
using MessageRef = boost::intrusive_ptr<Message>;

namespace ceph::net {

using msgr_tag_t = uint8_t;

class Connection;
using ConnectionRef = boost::intrusive_ptr<Connection>;

class Dispatcher;

class Messenger;

} // namespace ceph::net

