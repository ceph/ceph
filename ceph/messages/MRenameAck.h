#ifndef __MRENAMEACK_H
#define __MRENAMEACK_H

class MRename : public Message {
  inodeno_t srcdirino;
  string srcname;
  inodeno_t destdirino;
  string destname;

 public:
  inodeno_t get_srcdirino() { return srcdirino; }
  string& get_srcname() { return srcname; }

  MRenameAck() {}
  MRenameAck(inodeno_t srcdirino,
			 const string& srcname) :
	Message(MSG_MDS_RENAMEACK) {
	this->srcdirino = srcdirino;
	this->srcname = srcname;
  }
  virtual char *get_type_name() { return "RnotA";}

  virtual void decode_payload(crope& s) {
	int off = 0;
	s.copy(off, sizeof(srcdirino), (char*)&srcdirino);
	off += sizeof(srcdirino);
	srcname = s.c_str() + off;
	off += srcname.length() + 1;
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&srcdirino,sizeof(srcdirino));
	s.append((char*)srcname.c_str());
	s.append((char)0);
  }
};

#endif
