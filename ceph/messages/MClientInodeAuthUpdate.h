#ifndef __MCLIENTINODEAUTHUPDATE_H
#define __MCLIENTINODEAUTHUPDATE_H

class MClientInodeAuthUpdate : public Message {
  //inodeno_t ino;
  fileh_t   fh;
  int       newauth;

 public:
  //inodeno_t get_ino() { return ino; }
  fileh_t   get_fh() { return fh; }
  int       get_auth() { return newauth; }

  MClientInodeAuthUpdate() {}
  MClientInodeAuthUpdate(fileh_t fh, int newauth) :
	Message(MSG_CLIENT_INODEAUTHUPDATE) {
	this->fh = fh;
	this->newauth = newauth;
  }
  virtual char *get_type_name() { return "Ciau";}
  
  virtual void decode_payload(crope& s, int& off) {
	s.copy(off, sizeof(fh), (char*)&fh);
	off += sizeof(fh);
	s.copy(off, sizeof(newauth), (char*)&newauth);
	off += sizeof(newauth);
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&fh,sizeof(fh));
	s.append((char*)&newauth,sizeof(newauth));
  }
};

#endif
