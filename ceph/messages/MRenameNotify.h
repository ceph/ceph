#ifndef __MRENAMENOTIFY_H
#define __MRENAMENOTIFY_H

class MRenameNotify : public Message {
  inodeno_t ino;
  inodeno_t srcdirino;
  string srcname;
  inodeno_t destdirino;
  string destname;
  string destdirpath;

 public:
  inodeno_t get_ino() { return ino; }
  inodeno_t get_srcdirino() { return srcdirino; }
  string& get_srcname() { return srcname; }
  inodeno_t get_destdirino() { return destdirino; }
  string& get_destname() { return destname; }
  string& get_destdirpath() { return destdirpath; }

  MRenameNotify() {}
  MRenameNotify(inodeno_t ino,
				inodeno_t srcdirino,
				const string& srcname,
				inodeno_t destdirino,
				const string& destdirpath,
				const string& destname) :
	Message(MSG_MDS_RENAMENOTIFY) {
	this->ino = ino;
	this->srcdirino = srcdirino;
	this->srcname = srcname;
	this->destdirino = destdirino;
	this->destname = destname;
	this->destdirpath = destdirpath;
  }
  virtual char *get_type_name() { return "Rnot";}
  
  virtual void decode_payload(crope& s) {
	int off = 0;
	s.copy(off, sizeof(ino), (char*)&ino);
	off += sizeof(ino);
	s.copy(off, sizeof(srcdirino), (char*)&srcdirino);
	off += sizeof(srcdirino);
	s.copy(off, sizeof(destdirino), (char*)&destdirino);
	off += sizeof(destdirino);
	srcname = s.c_str() + off;
	off += srcname.length() + 1;
	destname = s.c_str() + off;
	off += destname.length() + 1;
	destdirpath = s.c_str() + off;
	off += destdirpath.length() + 1;
  }
  virtual void encode_payload(crope& s) {
	s.append((char*)&ino,sizeof(ino));
	s.append((char*)&srcdirino,sizeof(srcdirino));
	s.append((char*)&destdirino,sizeof(destdirino));
	s.append((char*)srcname.c_str());
	s.append((char)0);
	s.append((char*)destname.c_str());
	s.append((char)0);
	s.append((char*)destdirpath.c_str());
	s.append((char)0);
  }
};

#endif
