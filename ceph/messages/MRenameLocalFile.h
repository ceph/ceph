#ifndef __MRENAMELOCALFILE_H
#define __MRENAMELOCALFILE_H

class MRenameLocalFile : public Message {
  inodeno_t fromino;
  // and (
  inodeno_t destdirino;
  string name;
  //  or
  inodeno_t oldino;
  // )

 public:
  inodeno_t get_fromino() { return fromino; }
  inodeno_t get_destdirino() { return destdirino; }
  inodeno_t get_oldino() { return oldino; }
  string& get_name() { return name; }

  MRenameLocalFile() {}
  MRenameLocalFile(inodeno_t fromino,
				   inodeno_t destdirino,
				   string name,
				   inodeno_t oldino) :
	Message(MSG_MDS_RENAMELOCALFILE) {
	this->fromino = fromino;
	this->destdirino = destdirino;
	this->name = name;
	this->oldino = oldino;
  }
  virtual char *get_type_name() { return "Rlf";}
  
  virtual int decode_payload(crope s) {
	int off = 0;
	s.copy(off, sizeof(fromino), (char*)&fromino);
	off += sizeof(fromino);
	s.copy(off, sizeof(destdirino), (char*)&destdirino);
	off += sizeof(destdirino);
	s.copy(off, sizeof(oldino), (char*)&oldino);
	off += sizeof(oldino);
	name = s.c_str() + off;
	off += name.length() + 1;
	return off;
  }
  virtual crope get_payload() {
	crope s;
	s.append((char*)&fromino,sizeof(fromino));
	s.append((char*)&destdirino,sizeof(destdirino));
	s.append((char*)&oldino,sizeof(oldino));
	s.append((char*)name.c_str());
	s.append((char)0);
	return s;
  }
};

#endif
