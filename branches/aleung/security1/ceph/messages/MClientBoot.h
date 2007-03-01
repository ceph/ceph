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


#ifndef __MCLIENTBOOT_H
#define __MCLIENTBOOT_H

#include "msg/Message.h"

class MClientBoot : public Message {
  
 public:
  MClientBoot() : Message(MSG_CLIENT_BOOT) { }

  char *get_type_name() { return "client_boot"; }

  void encode_payload() { }
  void decode_payload() { }
};

#endif
