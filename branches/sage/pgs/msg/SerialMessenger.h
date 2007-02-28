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


#ifndef __SERIAL_MESSENGER_H
#define __SERIAL_MESSENGER_H

#include "Dispatcher.h"
#include "Message.h"

class SerialMessenger : public Dispatcher {
 public:
  virtual void dispatch(Message *m) = 0;      // i receive my messages here
  virtual void send(Message *m, entity_name_t dest, int port=0, int fromport=0) = 0;          // doesn't block
  virtual Message *sendrecv(Message *m, entity_name_t dest, int port=0, int fromport=0) = 0;  // blocks for matching reply
};

#endif
