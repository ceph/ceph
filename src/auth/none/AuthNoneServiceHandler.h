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

#ifndef __AUTHNONESERVICEHANDLER_H
#define __AUTHNONESERVICEHANDLER_H

#include "../AuthServiceHandler.h"
#include "../Auth.h"

class AuthNoneServiceHandler  : public AuthServiceHandler {
  EntityName entity_name;

public:
  AuthNoneServiceHandler()  {}
  ~AuthNoneServiceHandler() {}
  
  int start_session(bufferlist& result_bl) { return 0; }
  int handle_request(bufferlist::iterator& indata, bufferlist& result_bl, bufferlist& caps) { return 0; }
  void build_cephx_response_header(int request_type, int status, bufferlist& bl) { }
  EntityName& get_entity_name() { return entity_name; }
};

#endif
