#ifndef __MEXPORTDIRDISCOVER_H
#define __MEXPORTDIRDISCOVER_H

#include "include/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirDiscover : public Message {
  inodeno_t ino;
  string path;

 public:
  inodeno_t get_ino() { return ino; }
  string& get_path() { return path; }

  MExportDirDiscover() {}
  MExportDirDiscover(CInode *in) : 
	Message(MSG_MDS_EXPORTDIRDISCOVER) {
	in->make_path(path);
	ino = in->ino();
  }
  virtual char *get_type_name() { return "ExDis"; }


  virtual int decode_payload(crope s) {
	s.copy(0, sizeof(ino), (char*)&ino);
	path = s.c_str() + sizeof(ino);
    int off = sizeof(ino) + path.length() + 1;
    return off;
  }

  virtual crope get_payload() {
	crope s;
	s.append((char*)&ino, sizeof(ino));
	s.append(path.c_str());
	s.append((char)0);
	return s;
  }
};

#endif
