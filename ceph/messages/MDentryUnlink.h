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
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(dirino), (char*)&dirino);
	off += sizeof(dirino);
	_unrope(dn, s, off);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&dirino,sizeof(dirino));
	_rope(dn, s);
  }
};

#endif
