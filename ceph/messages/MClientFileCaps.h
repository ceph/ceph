#ifndef __MCLIENTFILECAPS_H
#define __MCLIENTFILECAPS_H

class MClientFileCaps : public Message {
  long      seq;
  inode_t   inode;
  fileh_t   fh;
  int       mds;
  int       caps;
  bool      need_ack;

 public:
  inodeno_t get_ino() { return inode.ino; }
  inode_t   get_inode() { return inode; }
  fileh_t   get_fh() { return fh; }
  int       get_mds() { return mds; }
  int       get_caps() { return caps; }
  bool      needs_ack() { return need_ack; }
  long      get_seq() { return seq; }

  MClientFileCaps() {}
  MClientFileCaps(CInode *in,
				  CFile *f,
				  bool need_ack,
				  int new_mds = -1) :
	Message(MSG_CLIENT_FILECAPS) {
	f->last_sent++;
	this->seq = f->last_sent;
	this->inode = in->inode;
	this->fh = f->fh;
	this->caps = f->pending_caps;
	this->need_ack = need_ack;
	this->mds = new_mds;
  }
  virtual char *get_type_name() { return "Cfcap";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(seq), (char*)&seq);
	off += sizeof(seq);
	s.copy(off, sizeof(inode), (char*)&inode);
	off += sizeof(inode);
	s.copy(off, sizeof(fh), (char*)&fh);
	off += sizeof(fh);
	s.copy(off, sizeof(mds), (char*)&mds);
	off += sizeof(mds);
	s.copy(off, sizeof(caps), (char*)&caps);
	off += sizeof(caps);
	s.copy(off, sizeof(need_ack), (char*)&need_ack);
	off += sizeof(need_ack);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&seq, sizeof(seq));
	s.append((char*)&inode, sizeof(inode));
	s.append((char*)&fh,sizeof(fh));
	s.append((char*)&mds,sizeof(mds));
	s.append((char*)&caps, sizeof(caps));
	s.append((char*)&need_ack, sizeof(need_ack));
  }
};

#endif
