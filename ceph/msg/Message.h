
#ifndef __MESSAGE_H
#define __MESSAGE_H

#define MSG_PING       1

#define MSG_OSD_READ         10
#define MSG_OSD_READREPLY    11
#define MSG_OSD_WRITE        12
#define MSG_OSD_WRITEREPLY   13

#define MSG_CLIENT_REQUEST   20
#define MSG_CLIENT_REPLY     21

#define MSG_MDS_HEARTBEAT    100
#define MSG_MDS_DISCOVER     110

#define MSG_MDS_INODEUPDATE  120
#define MSG_MDS_DIRUPDATE    121
#define MSG_MDS_INODEEXPIRE  122

#define MSG_MDS_EXPORTDIRPREP    150
#define MSG_MDS_EXPORTDIRPREPACK 151
#define MSG_MDS_EXPORTDIR        152
#define MSG_MDS_EXPORTDIRACK     153
#define MSG_MDS_EXPORTDIRNOTIFY  154

#define MSG_MDS_INODESYNCSTART   180
#define MSG_MDS_INODESYNCACK     181
#define MSG_MDS_INODESYNCRELEASE 182

#define MSG_MDS_SHUTDOWNSTART  900
#define MSG_MDS_SHUTDOWNFINISH 901

#include "config.h"

#define MSG_ADDR_MDS(x)     (x)
#define MSG_ADDR_OSD(x)     (NUMMDS+(x))
#define MSG_ADDR_CLIENT(x)  (NUMMDS+NUMOSD+(x))

#define MSG_ADDR_TYPE(x)    ((x)<NUMMDS ? "mds":((x)<(NUMMDS+NUMOSD) ? "osd":"client"))
#define MSG_ADDR_NUM(x)    ((x)<NUMMDS ? (x) : \ 
							((x)<(NUMMDS+NUMOSD) ? ((x)-NUMMDS) : \
							 ((x)-(NUMMDS+NUMOSD))))
#define MSG_ADDR_NICE(x)   MSG_ADDR_TYPE(x) << MSG_ADDR_NUM(x)

#include <iostream>
#include <stdlib.h>
#include <ext/rope>
#include <cassert>
using namespace std;


// abstract Message class

#define MSG_ENVELOPE_LEN  ((3*sizeof(int)+2*sizeof(long)))

class Message {
 private:
  char tname[20];
  
 protected:
  // envelope  (make sure you update MSG_ENVELOPE_LEN above if you change this)
  int type;
  long source, dest;
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
  long get_dest() { return dest; }
  void set_dest(long a, int p) { dest = a; dest_port = p; }
  int get_dest_port() { return dest_port; }
  

  long get_source() { return source; }
  void set_source(long a, int p) { source = a; source_port = p; }
  int get_source_port() { return source_port; }

  crope get_envelope() {
	crope e;
	e.append((char*)&type, sizeof(int));
	e.append((char*)&source, sizeof(long));
	e.append((char*)&source_port, sizeof(int));
	e.append((char*)&dest, sizeof(long));
	e.append((char*)&dest_port, sizeof(int));
	return e;
  }
  int decode_envelope(crope s) {
	s.copy(0, sizeof(int), (char*)&type);
	s.copy(sizeof(int), sizeof(long), (char*)&source);
	s.copy(sizeof(int)+sizeof(long), sizeof(int), (char*)&source_port);
	s.copy(2*sizeof(int)+sizeof(long), sizeof(long), (char*)&dest);
	s.copy(2*sizeof(int)+2*sizeof(long), sizeof(int), (char*)&dest_port);
	return 0;
  }
  
  // PAYLOAD ----
  virtual crope get_payload() { 
	return crope();   // blank message body, by default.
  }
  virtual int decode_payload(crope s) {
	return 0;       // no default, nothing to decode
  }
 
  // BOTH ----
  crope get_serialized() {
	crope both;
	both.append(get_envelope());
	assert(both.length() == MSG_ENVELOPE_LEN);
	both.append(get_payload());
	return both;
  }
};


#endif
