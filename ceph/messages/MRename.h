#ifndef __MRENAME_H
#define __MRENAME_H

class MRename : public Message {
  inodeno_t srcdirino;
  string srcname;
  inodeno_t destdirino;
  string destname;
  int initiator;

  crope inode_state;

 public:
  int get_initiator() { return initiator; }
  inodeno_t get_srcdirino() { return srcdirino; }
  string& get_srcname() { return srcname; }
  inodeno_t get_destdirino() { return destdirino; }
  string& get_destname() { return destname; }
  crope& get_inode_state() { return inode_state; }

  MRename() {}
  MRename(int initiator,
		  inodeno_t srcdirino,
		  const string& srcname,
		  inodeno_t destdirino,
		  const string& destname,
		  crope& inode_state) :
	Message(MSG_MDS_RENAME) {
	this->initiator = initiator;
	this->srcdirino = srcdirino;
	this->srcname = srcname;
	this->destdirino = destdirino;
	this->destname = destname;
	this->inode_state = inode_state;
  }
  virtual char *get_type_name() { return "Rn";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(initiator), (char*)&initiator);
	off += sizeof(initiator);
	s.copy(off, sizeof(srcdirino), (char*)&srcdirino);
	off += sizeof(srcdirino);
	s.copy(off, sizeof(destdirino), (char*)&destdirino);
	off += sizeof(destdirino);
	_unrope(srcname, s, off);
	_unrope(destname, s, off);
	size_t len;
	s.copy(off, sizeof(len), (char*)&len);
	off += sizeof(len);
	inode_state = s.substr(off, len);
	off += len;
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&initiator,sizeof(initiator));
	s.append((char*)&srcdirino,sizeof(srcdirino));
	s.append((char*)&destdirino,sizeof(destdirino));
	_rope(srcname, s);
	_rope(destname, s);
	size_t len = inode_state.length();
	s.append((char*)&len, sizeof(len));
	s.append(inode_state);
  }
};

#endif
