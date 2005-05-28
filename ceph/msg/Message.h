
#ifndef __MESSAGE_H
#define __MESSAGE_H

#define MSG_PING       2

#define MSG_SHUTDOWN   3

#define MSG_OSD_READ         10
#define MSG_OSD_READREPLY    11
#define MSG_OSD_WRITE        12
#define MSG_OSD_WRITEREPLY   13
#define MSG_OSD_OP           14    // delete, etc.
#define MSG_OSD_OPREPLY      15    // delete, etc.
#define MSG_OSD_PING         16

#define MSG_CLIENT_REQUEST         20
#define MSG_CLIENT_REPLY           21
//#define MSG_CLIENT_DONE            22
#define MSG_CLIENT_FILECAPS        23
#define MSG_CLIENT_INODEAUTHUPDATE 24

#define MSG_CLIENT_MOUNT           30
#define MSG_CLIENT_MOUNTACK        31
#define MSG_CLIENT_UNMOUNT         32

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


#define MSG_MDS_HASHDIR          160
#define MSG_MDS_HASHDIRACK       161
#define MSG_MDS_UNHASHDIR        162
#define MSG_MDS_UNHASHDIRACK     163

#define MSG_MDS_INODEWRITERCLOSED 170

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
  
 protected:
  // envelope  (make sure you update MSG_ENVELOPE_LEN above if you change this)
  int type;
  msg_addr_t source, dest;
  int source_port, dest_port;

  char *raw_message;
  int raw_message_len;
  
  // any payload is in an overloaded child class

  friend class Messenger;

 public:
  Message() { 
	source_port = dest_port = -1;
	source = dest = -1;
	raw_message = 0;
	raw_message_len = 0;
  };
  Message(int t) {
	source_port = dest_port = -1;
	source = dest = -1;
	type = t;
	raw_message = 0;
	raw_message_len = 0;
  }
  virtual ~Message() {
	if (raw_message) delete raw_message;
  }


  // for rpc-type procedural messages (pcid = procedure call id)
  virtual long get_pcid() { return 0; }
  virtual void set_pcid(long t) { assert(0); }  // overload me

  
  void set_raw_message(char *raw, int len) {
	raw_message = raw;
	raw_message_len = len;
  }
  char *get_raw_message() { 
	return raw_message;
  }
  int get_raw_message_len() {
	return raw_message_len;
  }
  void clear_raw_message() {
	raw_message = 0;
  }
  

  // ENVELOPE ----

  // type
  int get_type() { return type; }
  void set_type(int t) { type = t; }
  virtual char *get_type_name() = 0;

  // source/dest
  msg_addr_t get_dest() { return dest; }
  void set_dest(msg_addr_t a, int p) { dest = a; dest_port = p; }
  int get_dest_port() { return dest_port; }
  
  
  msg_addr_t get_source() { return source; }
  void set_source(msg_addr_t a, int p) { source = a; source_port = p; }
  int get_source_port() { return source_port; }

  void encode_envelope() {
	assert(raw_message);
	int off = 0;
	*(int*)(raw_message + off) = type;
	off += sizeof(type);
	*(msg_addr_t*)(raw_message + off) = source;
	off += sizeof(source);
	*(int*)(raw_message + off) = source_port;
	off += sizeof(source_port);
	*(msg_addr_t*)(raw_message + off) = dest;
	off += sizeof(dest);
	*(int*)(raw_message + off) = dest_port;
	off += sizeof(dest_port);
  }
  void decode_envelope() {
	assert(raw_message);
	int off = 0;
	type = *(int*)(raw_message + off);
	off += sizeof(int);
	source = *(msg_addr_t*)(raw_message + off);
	off += sizeof(msg_addr_t);
	source_port = *(int*)(raw_message + off);
	off += sizeof(int);
	dest = *(msg_addr_t*)(raw_message + off);
	off += sizeof(msg_addr_t);
	dest_port = *(int*)(raw_message + off);
	off += sizeof(int);
  }
  
  // PAYLOAD ----
  // overload either the rope version (easier)
  virtual void encode_payload(crope& s)           { assert(0); }
  virtual void decode_payload(crope& s, int& off) { assert(0); }
 
  // of the buffer version (faster)
  virtual void decode_payload() {
	assert(raw_message);
	
	// use a crope for convenience, small messages, etc.  FIXME someday.
	crope ser;
	ser.append(raw_message + MSG_ENVELOPE_LEN, raw_message_len-MSG_ENVELOPE_LEN);
	
	int off = 0;
	decode_payload(ser, off);
	assert(off == raw_message_len-MSG_ENVELOPE_LEN);
	assert(off == ser.length());
  }
  virtual void encode() {
	// use crope for convenience, small messages. FIXME someday.
	crope r;
	
	// payload
	encode_payload(r);

	// alloc or reuse buf
	if (raw_message && 
		raw_message_len != r.length() + MSG_ENVELOPE_LEN) {
	  delete raw_message;
	  raw_message = 0;
	}

	raw_message_len = r.length() + MSG_ENVELOPE_LEN;
	if (!raw_message) raw_message = new char[raw_message_len];
	
	// envelope
	int off = 0;
	encode_envelope();

	// copy payload
	memcpy(raw_message + MSG_ENVELOPE_LEN, r.c_str(), r.length());
  }
  virtual void decode() {
	assert(raw_message);

	// decode envelope
	assert(raw_message_len >= MSG_ENVELOPE_LEN);
	decode_envelope();

	if (raw_message_len > MSG_ENVELOPE_LEN) 
	  decode_payload();
  }

  
};


ostream& operator<<(ostream& out, Message& m);

#endif
