
#ifndef __MESSAGE_H
#define __MESSAGE_H

#define MSG_PING       1
#define MSG_FWD        2
#define MSG_DISCOVER   3

#define MSG_OSD_READ         10
#define MSG_OSD_READREPLY    11
#define MSG_OSD_WRITE        12
#define MSG_OSD_WRITEREPLY   13

#define MSG_CLIENT_REQUEST   20
#define MSG_CLIENT_REPLY     21

#define MSG_MDS_HEARTBEAT    30
#define MSG_MDS_DISCOVER     31

#define MSG_MDS_EXPORTDIR    35
#define MSG_MDS_EXPORTDIRACK 36


#define MSG_ADDR_MDS(x)     (x)
#define MSG_ADDR_OSD(x)     (0x800 + x)
#define MSG_ADDR_CLIENT(x)  (0x1000 + x)

#define MSG_ADDR_TYPE(x)    (x < 0x800 ? "mds":(x < 0x1000 ? "osd":"client"))
#define MSG_ADDR_NUM(x)    (x < 0x800 ? x:(x < 0x1000 ? (x-0x800):(x-0x1000)))
#define MSG_ADDR_NICE(x)   MSG_ADDR_TYPE(x) << MSG_ADDR_NUM(x)

#include <iostream>
using namespace std;


// abstract Message class

class Message {
 protected:
  char *serialized;
  unsigned long serial_len;

  int type;

  long source, dest;
  int source_port, dest_port;

 public:
  Message() { 
	serialized = 0;
	serial_len = 0;
	source_port = dest_port = -1;
	source = dest = -1;
  };
  Message(int t) {
	serialized = 0;
	serial_len = 0;
	source_port = dest_port = -1;
	source = dest = -1;
	type = t;
  }
  ~Message() {
	if (serialized) { delete serialized; serialized = 0; }
  };

  // type
  int get_type() { return type; }
  void set_type(int t) { type = t; }

  // source/dest
  long get_dest() { return dest; }
  void set_dest(long a, int p) { dest = a; dest_port = p; }
  int get_dest_port() { return dest_port; }
  

  long get_source() { return source; }
  void set_source(long a, int p) { source = a; source_port = p; }
  int get_source_port() { return source_port; }
  
  // serialization
  virtual unsigned long serialize() { }   // = 0;
  void *get_serialized() {
	return serialized;
  }
  unsigned long get_serialized_len() {
	return serial_len;
  }  
 
  friend class Messenger;
};


#endif
