#ifndef __MCLIENTFILECAPS_H
#define __MCLIENTFILECAPS_H

class MClientFileCaps : public Message {
  long      seq;
  inode_t   inode;
  int       mds;
  int       caps;
  int       wanted;
  int       client;

 public:
  inodeno_t get_ino() { return inode.ino; }
  inode_t&  get_inode() { return inode; }
  int       get_mds() { return mds; }
  int       get_caps() { return caps; }
  int       get_wanted() { return wanted; }
  long      get_seq() { return seq; }
  int       get_client() { return client; }

  //void set_client(int c) { client = c; }
  void set_caps(int c) { caps = c; }
  void set_wanted(int w) { wanted = w; }

  MClientFileCaps() {}
  MClientFileCaps(inode_t& inode,
				  long seq,
				  int caps,
				  int wanted,
				  int client,
				  int new_mds = -1) :
	Message(MSG_CLIENT_FILECAPS) {

	this->seq = seq;
	this->caps = caps;
	this->wanted = wanted;

	this->client = client;

	this->inode = inode;
	this->mds = new_mds;
  }
  virtual char *get_type_name() { return "Cfcap";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(seq), (char*)&seq);
	off += sizeof(seq);
	s.copy(off, sizeof(inode), (char*)&inode);
	off += sizeof(inode);
	s.copy(off, sizeof(caps), (char*)&caps);
	off += sizeof(caps);
	s.copy(off, sizeof(wanted), (char*)&wanted);
	off += sizeof(wanted);
	s.copy(off, sizeof(mds), (char*)&mds);
	off += sizeof(mds);
	s.copy(off, sizeof(client), (char*)&client);
	off += sizeof(client);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&seq, sizeof(seq));
	s.append((char*)&inode, sizeof(inode));
	s.append((char*)&caps, sizeof(caps));
	s.append((char*)&wanted, sizeof(wanted));
	s.append((char*)&mds,sizeof(mds));
	s.append((char*)&client, sizeof(client));
  }
};

#endif
