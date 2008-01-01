// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
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

#ifndef __MCLIENTUNMOUNT_H
#define __MCLIENTUNMOUNT_H

#include "msg/Message.h"

class MClientUnmount : public Message {
public:
  MClientUnmount() : Message(CEPH_MSG_CLIENT_UNMOUNT) { }
  /*
  MClientUnmount(entity_inst_t i) : 
    Message(CEPH_MSG_CLIENT_UNMOUNT),
    inst(i) { }
  */
  
  const char *get_type_name() { return "client_unmount"; }

  void decode_payload() { 
    //int off = 0;
    //::_decode(inst, payload, off);
  }
  void encode_payload() { 
    //::_encode(inst, payload);
  }
};

#endif
