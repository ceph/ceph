#ifndef __MEXPORTDIRNOTIFY_H
#define __MEXPORTDIRNOTIFY_H

#include "include/Message.h"
#include <string>
using namespace std;

class MExportDirNotify : public Message {
 public:
  string    path;
  int       new_auth;

  MExportDirNotify(string& path, int new_auth) :
	Message(MSG_MDS_EXPORTDIRNOTIFY) {
	this->path = path;
	this->new_auth = new_auth;
  }
  virtual char *get_type_name() { return "exnot"; }
};

#endif
