// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "AuthNoneAuthorizeHandler.h"
#include "common/debug.h"

bool AuthNoneAuthorizeHandler::verify_authorizer(KeyStore *keys,
						 bufferlist& authorizer_data, bufferlist& authorizer_reply,
						 EntityName& entity_name, uint64_t& global_id, AuthCapsInfo& caps_info,
uint64_t *auid)
{
  bufferlist::iterator iter = authorizer_data.begin();

  try {
    __u8 struct_v = 1;
    ::decode(struct_v, iter);
    ::decode(entity_name, iter);
    ::decode(global_id, iter);
  } catch (const buffer::error &err) {
    dout(0) << "AuthNoneAuthorizeHandle::verify_authorizer() failed to decode" << dendl;
    return false;
  }

  caps_info.allow_all = true;

  return true;
}

