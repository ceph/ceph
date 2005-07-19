#ifndef __MCLIENTFILECAPS_H
#define __MCLIENTFILECAPS_H

class MClientFileCaps : public Message {
  long      seq;
  inode_t   inode;
  int       mds;
  int       caps;
  bool      need_ack;
  int       client;

 public:
  inodeno_t get_ino() { return inode.ino; }
  inode_t   get_inode() { return inode; }
  int       get_mds() { return mds; }
  int       get_caps() { return caps; }
  bool      needs_ack() { return need_ack; }
  long      get_seq() { return seq; }
  int       get_client() { return client; }

  void set_client(int c) { client = c; }

  MClientFileCaps() {}
  MClientFileCaps(CInode *in,
				  Capability& cap,
				  bool need_ack,
				  int new_mds = -1) :
	Message(MSG_CLIENT_FILECAPS) {

	this->seq = cap.get_last_seq();
	this->caps = cap.pending();

	this->inode = in->inode;
	this->need_ack = need_ack;
	this->mds = new_mds;
  }
  virtual char *get_type_name() { return "Cfcap";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(seq), (char*)&seq);
	off += sizeof(seq);
	s.copy(off, sizeof(inode), (char*)&inode);
	off += sizeof(inode);
	s.copy(off, sizeof(mds), (char*)&mds);
	off += sizeof(mds);
	s.copy(off, sizeof(caps), (char*)&caps);
	off += sizeof(caps);
	s.copy(off, sizeof(need_ack), (char*)&need_ack);
	off += sizeof(need_ack);
	s.copy(off, sizeof(client), (char*)&client);
	off += sizeof(client);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&seq, sizeof(seq));
	s.append((char*)&inode, sizeof(inode));
	s.append((char*)&mds,sizeof(mds));
	s.append((char*)&caps, sizeof(caps));
	s.append((char*)&need_ack, sizeof(need_ack));
	s.append((char*)&client, sizeof(client));
  }
};

#endif
