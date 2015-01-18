// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef XIO_SUBMIT_H
#define XIO_SUBMIT_H

#include <boost/intrusive/list.hpp>
#include "msg/SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"
#include "msg/msg_types.h"
#include "XioPool.h"

namespace bi = boost::intrusive;

class XioConnection;

struct XioSubmit
{
public:
  enum submit_type
  {
    OUTGOING_MSG,
    INCOMING_MSG_RELEASE
  };
  enum submit_type type;
  bi::list_member_hook<> submit_list;
  XioConnection *xcon;

  XioSubmit(enum submit_type _type, XioConnection *_xcon) :
    type(_type), xcon(_xcon)
    {}

  typedef bi::list< XioSubmit,
		    bi::member_hook< XioSubmit,
				     bi::list_member_hook<>,
				     &XioSubmit::submit_list >
		    > Queue;
};

#endif /* XIO_SUBMIT_H */
