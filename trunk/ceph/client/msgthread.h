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


#include "msg/Message.h"

// send the message, expecting no response.  threads other than the
// MPI thread use this function; if the MPI thread uses this function
// it could deadlock: this function could wait for the out queue to be
// emptied, but only the MPI thread can empty it.
void obfsmpi_send(Message *m)

// send the message to a server and wait for the response.  threads
// other than the MPI thread use this function.
Message *obfsmpi_sendrecv(Message *m)
