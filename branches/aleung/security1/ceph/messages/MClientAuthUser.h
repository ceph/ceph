// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef __MCLIENTAUTHUSER_H
#define __MCLIENTAUTHUSER_H

#include "msg/Message.h"

class MClientAuthUser : public Message {

 public:
  MClientAuthUser() : Message(MSG_CLIENT_AUTH_USER) { 
  }

  char *get_type_name() { return "client_auth_user"; }

  void decode_payload() {  
  }
  void encode_payload() {  
  }
};

#endif
