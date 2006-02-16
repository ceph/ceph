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

#ifndef __TCPDIRECTORY_H
#define __TCPDIRECTORY_H

/*
 * rank   -- a process (listening on some host:port)
 * entity -- a logical entity (osd123, mds3, client3245, etc.)
 *
 * multiple entities can coexist on a single rank.
 */

#include "Dispatcher.h"
#include "TCPMessenger.h"

#include <map>
using namespace std;
#include <ext/hash_map>
using namespace __gnu_cxx;

class TCPDirectory : public Dispatcher {
 protected:
  // how i communicate
  TCPMessenger *messenger;

  // directory
  hash_map<msg_addr_t, int> dir;        // entity -> rank
  hash_map<int, tcpaddr_t>  rank_addr;  // rank -> ADDR (e.g. host:port)
  
  __uint64_t                version;
  map<__uint64_t, msg_addr_t>  update_log;
  
  int                       nrank;
  int                       nclient, nmds, nosd;

  set<msg_addr_t>           hold;
  map<msg_addr_t, list<Message*> > waiting;

  // messages
  void handle_connect(class MNSConnect*);
  void handle_register(class MNSRegister *m);
  void handle_started(Message *m);
  void handle_lookup(class MNSLookup *m);
  void handle_unregister(Message *m);

 public:
  TCPDirectory(TCPMessenger *m) : 
	messenger(m),
	version(0),
	nrank(0), nclient(0), nmds(0), nosd(0) { 
	messenger->set_dispatcher(this);
	// i am rank 0!
	dir[MSG_ADDR_DIRECTORY] = 0;
	rank_addr[0] = m->get_tcpaddr();
	cout << "export CEPH_NAMESERVER=" << m->get_tcpaddr() << endl;
	++nrank;
  }

  void dispatch(Message *m) {
	switch (m->get_type()) {
	case MSG_NS_CONNECT:
	  handle_connect((class MNSConnect*)m);
	  break;
	case MSG_NS_REGISTER:
	  handle_register((class MNSRegister*)m);
	  break;
	case MSG_NS_STARTED:
	  handle_started(m);
	  break;
	case MSG_NS_UNREGISTER:
	  handle_unregister(m);
	  break;
	case MSG_NS_LOOKUP:
	  handle_lookup((class MNSLookup*)m);
	  break;

	default:
	  assert(0);
	}
  }
};

#endif
