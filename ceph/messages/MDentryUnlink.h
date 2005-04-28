#ifndef __MDENTRYUNLINK_H
#define __MDENTRYUNLINK_H

class MDentryUnlink : public Message {
  inodeno_t dirino;
  string dn;

 public:
  inodeno_t get_dirino() { return dirino; }
  string& get_dn() { return dn; }

  MDentryUnlink() {}
  MDentryUnlink(inodeno_t dirino, string& dn) :
	Message(MSG_MDS_DENTRYUNLINK) {
	this->dirino = dirino;
	this->dn = dn;
  }
  virtual char *get_type_name() { return "Dun";}
  
  virtual void decode_payload(crope& s) {
	int off = 0;
	s.copy(off, sizeof(dirino), (char*)&dirino);
	off += sizeof(dirino);
	dn = s.c_str() + off;
	off += dn.length() + 1;
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&dirino,sizeof(dirino));
	s.append((char*)dn.c_str(), dn.length()+1);
  }
};

#endif
