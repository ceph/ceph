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

#ifndef CEPH_CEPHXSERVICEHANDLER_H
#define CEPH_CEPHXSERVICEHANDLER_H

#include "../AuthServiceHandler.h"
#include "../Auth.h"

class KeyServer;

class CephxServiceHandler  : public AuthServiceHandler {
  KeyServer *key_server;
  uint64_t server_challenge;

public:
  CephxServiceHandler(CephContext *cct_, KeyServer *ks) 
    : AuthServiceHandler(cct_), key_server(ks), server_challenge(0) {}
  ~CephxServiceHandler() {}
  
  int start_session(EntityName& name, bufferlist::iterator& indata, bufferlist& result_bl, AuthCapsInfo& caps);
  int handle_request(bufferlist::iterator& indata, bufferlist& result_bl, uint64_t& global_id, AuthCapsInfo& caps, uint64_t *auid = NULL);
  void build_cephx_response_header(int request_type, int status, bufferlist& bl);
};

#endif
