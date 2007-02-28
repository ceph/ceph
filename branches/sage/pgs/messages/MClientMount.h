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


#ifndef __MCLIENTMOUNT_H
#define __MCLIENTMOUNT_H

#include "msg/Message.h"

class MClientMount : public Message {

 public:
  MClientMount() : Message(MSG_CLIENT_MOUNT) { 
  }

  char *get_type_name() { return "Cmnt"; }

  virtual void decode_payload(crope& s, int& off) {  
  }
  virtual void encode_payload(crope& s) {  
  }
};

#endif
