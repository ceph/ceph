// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef __SERIAL_MESSENGER_H
#define __SERIAL_MESSENGER_H

#include "Dispatcher.h"
#include "Message.h"

class SerialMessenger : public Dispatcher {
 public:
  virtual void dispatch(Message *m) = 0;      // i receive my messages here
  virtual void send(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;          // doesn't block
  virtual Message *sendrecv(Message *m, msg_addr_t dest, int port=0, int fromport=0) = 0;  // blocks for matching reply
};

#endif
