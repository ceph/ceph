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

#ifndef __MTMESSENGER_H
#define __MTMESSENGER_H

#include "Message.h"
#include "SerialMessenger.h"

// Marshall and unmarshall OBFS messages, send and receive them over
// MPI.

class MTMessenger
{
public:
    // sets up the queues and internal thread; the MPI initialization
    // will scan argc/argv for MPI specific flags and remove them from
    // argc/argv.
    MTMessenger(int &argc, char **&argv);

    // tears it all down
    ~MTMessenger();

    // send a request to a server and wait (block) for the response;
    virtual Message *sendrecv(Message *m, msg_addr_t dest);

    // wait (block) for a request from anyone
    Message *recvreq();

    // forward request, masquerading as original source
    void fwdreq(Message *req, int dest);

    // send the response to the originator of the request
    virtual void sendresp(Message *req, Message *resp);


}; // class MTMessenger

#endif // __MTMESSENGER_H
