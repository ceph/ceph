#ifndef __MOSDOP_H
#define __MOSDOP_H

#include "msg/Message.h"

/*
 * OSD op
 *
 * oid - object id
 * op  - OSD_OP_DELETE, etc.
 *
 */

//#define OSD_OP_MKFS       20

// client ops
#define OSD_OP_READ       1
#define OSD_OP_WRITE      2
#define OSD_OP_STAT       10
#define OSD_OP_DELETE     11
#define OSD_OP_TRUNCATE   12
#define OSD_OP_ZERORANGE  13

#define OSD_OP_IS_REP(x)  ((x) >= 30)

// replication/recovery -- these ops are relative to a specific object version #
#define OSD_OP_REP_WRITE    (100+OSD_OP_WRITE)     // replicated (partial object) write
#define OSD_OP_REP_TRUNCATE (100+OSD_OP_TRUNCATE)  // replicated truncate
#define OSD_OP_REP_DELETE   (100+OSD_OP_DELETE)

#define OSD_OP_REP_PULL     30   // whole object read
#define OSD_OP_REP_PUSH     31   // whole object write
#define OSD_OP_REP_REMOVE   32   // delete replica

//#define OSD_OP_FLAG_TRUNCATE  1   // truncate object after end of write

typedef struct {
  long tid;
  long pcid;
  msg_addr_t asker;

  object_t oid;
  pg_t pg;
  int        pg_role;//, rg_nrep;
  version_t map_version;

  int op;
  size_t length, offset;
  version_t version;
  version_t old_version;

  bool   want_ack;
  bool   want_safe;

  size_t _data_len;
} MOSDOp_st;

class MOSDOp : public Message {
  MOSDOp_st st;
  bufferlist data;

  friend class MOSDOpReply;

 public:
  bool hack_blah;

  const long       get_tid() { return st.tid; }
  const msg_addr_t get_asker() { return st.asker; }

  const object_t   get_oid() { return st.oid; }
  const pg_t get_pg() { return st.pg; }
  const version_t  get_map_version() { return st.map_version; }

  const int        get_pg_role() { return st.pg_role; }  // who am i asking for?
  const version_t  get_version() { return st.version; }
  const version_t  get_old_version() { return st.old_version; }

  const int    get_op() { return st.op; }
  const size_t get_length() { return st.length; }
  const size_t get_offset() { return st.offset; }

  const bool wants_ack() { return st.want_ack; }
  const bool wants_safe() { return st.want_safe; }

  void set_data(bufferlist &d) {
	data.claim(d);
	st._data_len = data.length();
  }
  bufferlist& get_data() {
	return data;
  }
  size_t get_data_len() { return st._data_len; }

  // keep a pcid (procedure call id) to match up request+reply
  void set_pcid(long pcid) { this->st.pcid = pcid; }
  long get_pcid() { return st.pcid; }

  MOSDOp(long tid, msg_addr_t asker, 
		 object_t oid, pg_t pg, version_t mapversion, int op) :
	Message(MSG_OSD_OP) {
	memset(&st, 0, sizeof(st));
	this->st.tid = tid;
	this->st.asker = asker;

	this->st.oid = oid;
	this->st.pg = pg;
	this->st.pg_role = 0;
	this->st.map_version = mapversion;
	this->st.op = op;

	this->st.want_ack = true;
	this->st.want_safe = true;
  }
  MOSDOp() {}

  void set_pg_role(int r) { st.pg_role = r; }
  //void set_rg_nrep(int n) { st.rg_nrep = n; }

  void set_length(size_t l) { st.length = l; }
  void set_offset(size_t o) { st.offset = o; }
  void set_version(version_t v) { st.version = v; }
  void set_old_version(version_t ov) { st.old_version = ov; }
  
  void set_want_ack(bool b) { st.want_ack = b; }
  void set_want_safe(bool b) { st.want_safe = b; }

  // marshalling
  virtual void decode_payload() {
	payload.copy(0, sizeof(st), (char*)&st);
	payload.splice(0, sizeof(st));
	data.claim(payload);
  }
  virtual void encode_payload() {
	payload.push_back( new buffer((char*)&st, sizeof(st)) );
	payload.claim_append( data );
  }

  virtual char *get_type_name() { return "oop"; }
};

inline ostream& operator<<(ostream& out, MOSDOp& op)
{
  return out << "MOSDOp(" << MSG_ADDR_NICE(op.get_asker()) << "." << op.get_tid() << " oid " << hex << op.get_oid() << dec << " " << &op << ")";
}

#endif
