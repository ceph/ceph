#ifndef __MEXPORTDIRPREP_H
#define __MEXPORTDIRPREP_H

#include "include/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirPrep : public Message {
 public:
  string path;
  inodeno_t ino;

  MExportDirPrep(CInode *in) : 
	Message(MSG_MDS_EXPORTDIRPREP) {
	in->make_path(path);
	ino = in->ino();
  }

  virtual char *get_type_name() { return "ExP"; }
};

#endif
