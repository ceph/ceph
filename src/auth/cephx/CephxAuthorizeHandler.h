// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_CEPHXAUTHORIZEHANDLER_H
#define CEPH_CEPHXAUTHORIZEHANDLER_H

#include "../AuthAuthorizeHandler.h"

struct CephxAuthorizeHandler : public AuthAuthorizeHandler {
  bool verify_authorizer(KeyStore *keys,
			 bufferlist& authorizer_data, bufferlist& authorizer_reply,
                         EntityName& entity_name, uint64_t& global_id,
			 AuthCapsInfo& caps_info, uint64_t *auid = NULL);
};



#endif
