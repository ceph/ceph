
#ifndef __MESSAGE_H
#define __MESSAGE_H

#define MSG_PING       1

#define MSG_FINISH     0

#define MSG_OSD_READ         10
#define MSG_OSD_READREPLY    11
#define MSG_OSD_WRITE        12
#define MSG_OSD_WRITEREPLY   13

#define MSG_CLIENT_REQUEST   20
#define MSG_CLIENT_REPLY     21
#define MSG_CLIENT_DONE      22

#define MSG_MDS_HEARTBEAT          100
#define MSG_MDS_DISCOVER           110
#define MSG_MDS_DISCOVERREPLY      111

#define MSG_MDS_INODEGETREPLICA    112
#define MSG_MDS_INODEGETREPLICAACK 113

#define MSG_MDS_INODEUPDATE  120
#define MSG_MDS_DIRUPDATE    121
#define MSG_MDS_INODEEXPIRE  122
#define MSG_MDS_DIREXPIRE    123

#define MSG_MDS_DIREXPIREREQ 124

#define MSG_MDS_CACHEEXPIRE  125

#define MSG_MDS_EXPORTDIRDISCOVER      150
#define MSG_MDS_EXPORTDIRDISCOVERACK   151
#define MSG_MDS_EXPORTDIRPREP      152
#define MSG_MDS_EXPORTDIRPREPACK   153
#define MSG_MDS_EXPORTDIRWARNING   154
#define MSG_MDS_EXPORTDIR          155
#define MSG_MDS_EXPORTDIRNOTIFY    156
#define MSG_MDS_EXPORTDIRNOTIFYACK 157
#define MSG_MDS_EXPORTDIRFINISH    158


#define MSG_MDS_HASHDIR          160
#define MSG_MDS_HASHDIRACK       161
#define MSG_MDS_UNHASHDIR        162
#define MSG_MDS_UNHASHDIRACK     163

#define MSG_MDS_INODESYNCSTART   170
#define MSG_MDS_INODESYNCACK     171
#define MSG_MDS_INODESYNCRELEASE 172
#define MSG_MDS_INODESYNCRECALL  173

#define MSG_MDS_INODELOCKSTART   180
#define MSG_MDS_INODELOCKACK     181
#define MSG_MDS_INODELOCKRELEASE 182

#define MSG_MDS_DIRSYNCSTART     190
#define MSG_MDS_DIRSYNCACK       191
#define MSG_MDS_DIRSYNCRELEASE   192
#define MSG_MDS_DIRSYNCRECALL    193

#define MSG_MDS_INODEUNLINK      200
#define MSG_MDS_INODEUNLINKACK   201

#define MSG_MDS_RENAMELOCALFILE  300

#define MSG_MDS_LOCK             500

#define MSG_MDS_SHUTDOWNSTART  900
#define MSG_MDS_SHUTDOWNFINISH 901

//#include "config.h"


// address types
typedef int  msg_addr_t;

// mds's, client's share same (integer) namespace    ??????
// osd's could be separate.


/* sandwich mds's, then osd's, then clients */
#define MSG_ADDR_MDS(x)     (x)
#define MSG_ADDR_OSD(x)     (g_conf.num_mds+(x))
#define MSG_ADDR_CLIENT(x)  (g_conf.num_mds+g_conf.num_osd+(x))

#define MSG_ADDR_ISCLIENT(x)  ((x) >= g_conf.num_mds+g_conf.num_osd)

#define MSG_ADDR_TYPE(x)    ((x)<g_conf.num_mds ? "mds":((x)<(g_conf.num_mds+g_conf.num_osd) ? "osd":"client"))
#define MSG_ADDR_NUM(x)    ((x)<g_conf.num_mds ? (x) : \
							((x)<(g_conf.num_mds+g_conf.num_osd) ? ((x)-g_conf.num_mds) : \
							 ((x)-(g_conf.num_mds+g_conf.num_osd))))
#define MSG_ADDR_NICE(x)   MSG_ADDR_TYPE(x) << MSG_ADDR_NUM(x)

#include <stdlib.h>
#include <cassert>

#include <iostream>
using namespace std;

#include <ext/rope>
using namespace __gnu_cxx;


// abstract Message class

#define MSG_ENVELOPE_LEN  ((3*sizeof(int)+2*sizeof(long)))

class Message {
 private:
  char tname[20];
  
 protected:
  // envelope  (make sure you update MSG_ENVELOPE_LEN above if you change this)
  int type;
  msg_addr_t source, dest;
  int source_port, dest_port;
  
  // any payload is in an overloaded child class

  friend class Messenger;

 public:
  Message() { 
	source_port = dest_port = -1;
	source = dest = -1;
  };
  Message(int t) {
	source_port = dest_port = -1;
	source = dest = -1;
	type = t;
	sprintf(tname, "%d", type);
  }
  Message(crope& s) {
	decode_envelope(s);
	// no payload in default message
  }
  virtual ~Message() {}

  // ENVELOPE ----

  // type
  int get_type() { return type; }
  void set_type(int t) { type = t; }
  virtual char *get_type_name() { return tname; }

  // source/dest
  msg_addr_t get_dest() { return dest; }
  void set_dest(msg_addr_t a, int p) { dest = a; dest_port = p; }
  int get_dest_port() { return dest_port; }
  
  
  msg_addr_t get_source() { return source; }
  void set_source(msg_addr_t a, int p) { source = a; source_port = p; }
  int get_source_port() { return source_port; }

  void encode_envelope(crope& e) {
	e.append((char*)&type, sizeof(int));
	e.append((char*)&source, sizeof(msg_addr_t));
	e.append((char*)&source_port, sizeof(int));
	e.append((char*)&dest, sizeof(msg_addr_t));
	e.append((char*)&dest_port, sizeof(int));
  }
  void decode_envelope(crope s) {
	int off = 0;
	s.copy(off, sizeof(int), (char*)&type);
	off += sizeof(int);
	s.copy(off, sizeof(msg_addr_t), (char*)&source);
	off += sizeof(msg_addr_t);
	s.copy(off, sizeof(int), (char*)&source_port);
	off += sizeof(int);
	s.copy(off, sizeof(long), (char*)&dest);
	off += sizeof(long);
	s.copy(off, sizeof(int), (char*)&dest_port);
	off += sizeof(int);
  }
  
  // PAYLOAD ----
  virtual void encode_payload(crope& s) { 
	// no payload by default
  }
  virtual void decode_payload(crope& s) {
	// no default, nothing to decode
  }
 
  // BOTH ----
  void encode(crope& both) {
	encode_envelope(both);
	encode_payload(both);
  }
};


#endif
