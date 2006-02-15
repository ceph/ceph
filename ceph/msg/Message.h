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


#ifndef __MESSAGE_H
#define __MESSAGE_H
 
#define MSG_NS_CONNECT     1
#define MSG_NS_CONNECTACK  2
#define MSG_NS_REGISTER    3
#define MSG_NS_REGISTERACK 4
#define MSG_NS_STARTED     5
#define MSG_NS_UNREGISTER  6
#define MSG_NS_LOOKUP      7
#define MSG_NS_LOOKUPREPLY 8


#define MSG_PING        10
#define MSG_PING_ACK    11

#define MSG_FAILURE     12
#define MSG_FAILURE_ACK 13

#define MSG_SHUTDOWN    99999


#define MSG_OSD_OP           14    // delete, etc.
#define MSG_OSD_OPREPLY      15    // delete, etc.
#define MSG_OSD_PING         16

#define MSG_OSD_GETMAP       17
#define MSG_OSD_MAP          18

#define MSG_OSD_MKFS_ACK     19

#define MSG_OSD_PG_NOTIFY      50
#define MSG_OSD_PG_PEER        51
#define MSG_OSD_PG_PEERACK     52

#define MSG_OSD_PG_QUERY       55
#define MSG_OSD_PG_QUERYREPLY  56
#define MSG_OSD_PG_UPDATE      57
#define MSG_OSD_PG_REMOVE      58

#define MSG_CLIENT_REQUEST         60
#define MSG_CLIENT_REPLY           61
//#define MSG_CLIENT_DONE            62
#define MSG_CLIENT_FILECAPS        63
#define MSG_CLIENT_INODEAUTHUPDATE 64

#define MSG_CLIENT_MOUNT           70
#define MSG_CLIENT_MOUNTACK        71
#define MSG_CLIENT_UNMOUNT         72

#define MSG_MDS_HEARTBEAT          100
#define MSG_MDS_DISCOVER           110
#define MSG_MDS_DISCOVERREPLY      111

#define MSG_MDS_INODEGETREPLICA    112
#define MSG_MDS_INODEGETREPLICAACK 113

#define MSG_MDS_INODEFILECAPS      115

#define MSG_MDS_INODEUPDATE  120
#define MSG_MDS_DIRUPDATE    121
#define MSG_MDS_INODEEXPIRE  122
#define MSG_MDS_DIREXPIRE    123

#define MSG_MDS_DIREXPIREREQ 124

#define MSG_MDS_CACHEEXPIRE  125

#define MSG_MDS_ANCHORREQUEST 130
#define MSG_MDS_ANCHORREPLY   131

#define MSG_MDS_INODELINK       140
#define MSG_MDS_INODELINKACK    141
#define MSG_MDS_INODEUNLINK     142
#define MSG_MDS_INODEUNLINKACK  143

#define MSG_MDS_EXPORTDIRDISCOVER      150
#define MSG_MDS_EXPORTDIRDISCOVERACK   151
#define MSG_MDS_EXPORTDIRPREP      152
#define MSG_MDS_EXPORTDIRPREPACK   153
#define MSG_MDS_EXPORTDIRWARNING   154
#define MSG_MDS_EXPORTDIR          155
#define MSG_MDS_EXPORTDIRNOTIFY    156
#define MSG_MDS_EXPORTDIRNOTIFYACK 157
#define MSG_MDS_EXPORTDIRFINISH    158


#define MSG_MDS_HASHDIRDISCOVER    160
#define MSG_MDS_HASHDIRDISCOVERACK 161
#define MSG_MDS_HASHDIRPREP        162
#define MSG_MDS_HASHDIRPREPACK     163
#define MSG_MDS_HASHDIR            164
#define MSG_MDS_HASHDIRACK         165
#define MSG_MDS_HASHDIRNOTIFY      166

#define MSG_MDS_HASHREADDIR        168
#define MSG_MDS_HASHREADDIRREPLY   169

#define MSG_MDS_UNHASHDIRPREP      170
#define MSG_MDS_UNHASHDIRPREPACK   171
#define MSG_MDS_UNHASHDIR          172
#define MSG_MDS_UNHASHDIRACK       173
#define MSG_MDS_UNHASHDIRNOTIFY    174
#define MSG_MDS_UNHASHDIRNOTIFYACK 175

#define MSG_MDS_DENTRYUNLINK      200

#define MSG_MDS_RENAMEWARNING    300   // sent from src to bystanders
#define MSG_MDS_RENAMENOTIFY     301   // sent from dest to bystanders
#define MSG_MDS_RENAMENOTIFYACK  302   // sent back to src
#define MSG_MDS_RENAMEACK        303   // sent from src to initiator, to xlock_finish

#define MSG_MDS_RENAMEPREP       304   // sent from initiator to dest auth (if dir)
#define MSG_MDS_RENAMEREQ        305   // sent from initiator (or dest if dir) to src auth
#define MSG_MDS_RENAME           306   // sent from src to dest, includes inode

#define MSG_MDS_LOCK             500

#define MSG_MDS_SHUTDOWNSTART  900
#define MSG_MDS_SHUTDOWNFINISH 901



#include "include/bufferlist.h"


// use fixed offsets and static entity -> logical addr mapping!
#define MSG_ADDR_RANK_BASE    0x10000000 // per-rank messenger services
#define MSG_ADDR_MDS_BASE     0x20000000
#define MSG_ADDR_OSD_BASE     0x30000000
#define MSG_ADDR_CLIENT_BASE  0x40000000
#define MSG_ADDR_TYPE_MASK    0xf0000000
#define MSG_ADDR_NUM_MASK     0x0fffffff
#define MSG_ADDR_NEW          0x0fffffff

#define MSG_ADDR_RANK(x)    (MSG_ADDR_RANK_BASE + (x))
#define MSG_ADDR_MDS(x)     (MSG_ADDR_MDS_BASE + (x))
#define MSG_ADDR_OSD(x)     (MSG_ADDR_OSD_BASE + (x))
#define MSG_ADDR_CLIENT(x)  (MSG_ADDR_CLIENT_BASE + (x))

#define MSG_ADDR_DIRECTORY   0
#define MSG_ADDR_RANK_NEW    MSG_ADDR_RANK(MSG_ADDR_NEW)
#define MSG_ADDR_MDS_NEW     MSG_ADDR_MDS(MSG_ADDR_NEW)
#define MSG_ADDR_OSD_NEW     MSG_ADDR_OSD(MSG_ADDR_NEW)
#define MSG_ADDR_CLIENT_NEW  MSG_ADDR_CLIENT(MSG_ADDR_NEW)

#define MSG_ADDR_ISCLIENT(x)  ((x) >= MSG_ADDR_CLIENT_BASE)
#define MSG_ADDR_TYPE(x)    (((x) & MSG_ADDR_TYPE_MASK) == MSG_ADDR_RANK_BASE ? "rank": \
							 (((x) & MSG_ADDR_TYPE_MASK) == MSG_ADDR_CLIENT_BASE ? "client": \
							  (((x) & MSG_ADDR_TYPE_MASK) == MSG_ADDR_OSD_BASE ? "osd": \
							   (((x) & MSG_ADDR_TYPE_MASK) == MSG_ADDR_MDS_BASE ? "mds": \
                                ((x) == MSG_ADDR_DIRECTORY ? "namer":"unknown")))))
#define MSG_ADDR_NUM(x)    ((x) & MSG_ADDR_NUM_MASK)
#define MSG_ADDR_NICE(x)   MSG_ADDR_TYPE(x) << MSG_ADDR_NUM(x)


#include <stdlib.h>
#include <cassert>

#include <iostream>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;



// abstract Message class
typedef int  msg_addr_t;

typedef struct {
  int type;
  msg_addr_t source, dest;
  int source_port, dest_port;
  int nchunks;
  __uint64_t lamport_stamp;
} msg_envelope_t;

#define MSG_ENVELOPE_LEN  sizeof(msg_envelope_t)


class Message {
 private:
  
 protected:
  msg_envelope_t  env;    // envelope
  bufferlist      payload;        // payload
  
  int tcp_sd;
  friend class Messenger;
public:
  int get_tcp_sd() { return tcp_sd; }
  void set_tcp_sd(int s) { tcp_sd = s; }

 public:
  Message() : tcp_sd(0) { 
	env.source_port = env.dest_port = -1;
	env.source = env.dest = -1;
	env.nchunks = 0;
	env.lamport_stamp = 0;
  };
  Message(int t) : tcp_sd(0) {
	env.source_port = env.dest_port = -1;
	env.source = env.dest = -1;
	env.nchunks = 0;
	env.type = t;
	env.lamport_stamp = 0;
  }
  virtual ~Message() {
  }

  void set_lamport_stamp(__uint64_t t) {
	env.lamport_stamp = t;
  }
  __uint64_t get_lamport_stamp() { return env.lamport_stamp; }


  // for rpc-type procedural messages (pcid = procedure call id)
  virtual long get_pcid() { return 0; }
  virtual void set_pcid(long t) { assert(0); }  // overload me

  bool empty_payload() { return payload.length() == 0; }
  bufferlist& get_payload() {
	return payload;
  }
  void set_payload(bufferlist& bl) {
	payload.claim(bl);
  }
  msg_envelope_t& get_envelope() {
	return env;
  }
  void set_envelope(msg_envelope_t& env) {
	this->env = env;
  }


  // ENVELOPE ----

  // type
  int get_type() { return env.type; }
  void set_type(int t) { env.type = t; }
  virtual char *get_type_name() = 0;

  // source/dest
  msg_addr_t get_dest() { return env.dest; }
  void set_dest(msg_addr_t a, int p) { env.dest = a; env.dest_port = p; }
  int get_dest_port() { return env.dest_port; }
  
  msg_addr_t get_source() { return env.source; }
  void set_source(msg_addr_t a, int p) { env.source = a; env.source_port = p; }
  int get_source_port() { return env.source_port; }


  // PAYLOAD ----
  void reset_payload() {
	payload.clear();
  }

  // overload either the rope version (easier!)
  virtual void encode_payload(crope& s)           { assert(0); }
  virtual void decode_payload(crope& s, int& off) { assert(0); }
 
  // of the bufferlist versions (faster!)
  virtual void decode_payload() {
	// use a crope for convenience, small messages, etc.  FIXME someday.
	crope ser;
	payload._rope(ser);
	
	int off = 0;
	decode_payload(ser, off);
	assert((unsigned)off == payload.length());
  }
  virtual void encode_payload() {
	assert(payload.length() == 0);  // caller should reset payload

	// use crope for convenience, small messages. FIXME someday.
	crope r;
	encode_payload(r);

	// copy payload
	payload.push_back( new buffer(r.c_str(), r.length()) );
  }
  
};


ostream& operator<<(ostream& out, Message& m);

#endif
