#ifndef __MEXPORTDIR_H
#define __MEXPORTDIR_H

#include "include/Message.h"

#include <vector>
using namespace std;

//typedef struct {
//} MExportDirRec_t;

typedef struct {
  inode_t inode;
  //list<client_t> open_by;
} Inode_Export_State_t;

class MExportDir : public Message {
 public:
  string path;
  inodeno_t ino;

  // ...?

  MExportDir(CInode *in) : 
	Message(MSG_MDS_EXPORTDIR) {
	this->ino = in->inode.ino;
	in->make_path(path);
  }
  virtual char *get_type_name() { return "exp"; }
};

#endif
