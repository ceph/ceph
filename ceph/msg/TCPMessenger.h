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

#ifndef __TCPMESSENGER_H
#define __TCPMESSENGER_H

#include "Messenger.h"
#include "Dispatcher.h"


# include <sys/socket.h>
# include <netinet/in.h>
# include <arpa/inet.h>

class Timer;

typedef struct sockaddr_in tcpaddr_t;


class TCPMessenger : public Messenger {
 protected:

  //class Logger *logger;  // for logging
  
 public:
  TCPMessenger(msg_addr_t myaddr);
  ~TCPMessenger();

  void ready();

  tcpaddr_t& get_tcpaddr();
  void map_entity_rank(msg_addr_t e, int r);
  void map_rank_addr(int r, tcpaddr_t a);

  // init, shutdown MPI and associated event loop thread.
  virtual int shutdown();

  // message interface
  virtual int send_message(Message *m, msg_addr_t dest, int port=0, int fromport=0);
};

/**
 * these are all ONE per process.
 */

extern int tcpmessenger_lookup(char *str, tcpaddr_t& ta);

extern int tcpmessenger_init();
extern int tcpmessenger_start();   // start thread
extern void tcpmessenger_wait();    // wait for thread to finish.
extern int tcpmessenger_shutdown();   // finalize MPI

extern void tcpmessenger_start_nameserver(tcpaddr_t& ta);  // on rank 0
extern void tcpmessenger_stop_nameserver();   // on rank 0
extern void tcpmessenger_start_rankserver(tcpaddr_t& ta);  // on all ranks
extern void tcpmessenger_stop_rankserver();   // on all ranks

extern int tcpmessenger_get_rank();

inline ostream& operator<<(ostream& out, struct sockaddr_in &a)
{
  unsigned char addr[4];
  memcpy((char*)addr, (char*)&a.sin_addr.s_addr, 4);
  out << (unsigned)addr[0] << "."
	  << (unsigned)addr[1] << "."
	  << (unsigned)addr[2] << "."
	  << (unsigned)addr[3] << ":"
	  << (int)a.sin_port;
  return out;
}


#endif
