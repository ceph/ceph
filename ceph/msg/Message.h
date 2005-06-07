
#ifndef __MESSAGE_H
#define __MESSAGE_H
 
#define MSG_PING        2
#define MSG_PING_ACK    3

#define MSG_FAILURE     4
#define MSG_FAILURE_ACK 5

#define MSG_SHUTDOWN    6

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

#include "include/bufferlist.h"


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
typedef int  msg_addr_t;

typedef struct {
  int type;
  msg_addr_t source, dest;
  int source_port, dest_port;
  int nchunks;
} msg_envelope_t;

#define MSG_ENVELOPE_LEN  sizeof(msg_envelope_t)


class Message {
 private:
  
 protected:
  msg_envelope_t  env;    // envelope
  bufferlist      payload;        // payload
  
  friend class Messenger;

 public:
  Message() { 
	env.source_port = env.dest_port = -1;
	env.source = env.dest = -1;
	env.nchunks = 0;
  };
  Message(int t) {
	env.source_port = env.dest_port = -1;
	env.source = env.dest = -1;
	env.nchunks = 0;
	env.type = t;
  }
  virtual ~Message() {
  }


  // for rpc-type procedural messages (pcid = procedure call id)
  virtual long get_pcid() { return 0; }
  virtual void set_pcid(long t) { assert(0); }  // overload me

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
	assert(off == payload.length());
  }
  virtual void encode_payload() {
	// use crope for convenience, small messages. FIXME someday.
	crope r;
	encode_payload(r);

	// copy payload
	payload.clear();
	payload.push_back( new buffer(r.c_str(), r.length()) );
  }
  
};


ostream& operator<<(ostream& out, Message& m);

#endif
