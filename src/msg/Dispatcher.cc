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



#include "Dispatcher.h"
#include "Messenger.h"

#include "mds/MDS.h"

/*
int Dispatcher::send_message(Message *m, msg_addr_t dest, int dest_port)
{
  assert(0);
  //return dis_messenger->send_message(m, dest, dest_port, MDS_PORT_SERVER);  // on my port!
}
*/
