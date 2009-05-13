#ifndef __CLASSHANDLER_H
#define __CLASHANDLER_H

#include "include/types.h"
#include "include/ClassEntry.h"

#include "common/Cond.h"


class OSD;


class ClassHandler
{
  OSD *osd;
  typedef enum { CLASS_UNKNOWN, CLASS_UNLOADED, CLASS_LOADED, CLASS_REQUESTED, CLASS_ERROR } ClassStatus;
  struct ClassData {
    ClassStatus status;
    version_t version;
    Cond *queue;
    ClassImpl impl;
    bool init_queue() {
      queue = new Cond();
      return (queue != NULL);
    }
    ClassData() : status(CLASS_UNKNOWN), version(-1), queue(NULL) {}
    ~ClassData() { delete queue; }
  };
  map<string, ClassData> objects;

  Mutex mutex;
public:

  ClassHandler(OSD *_osd) : osd(_osd), mutex("classhandler") {}

  bool load_class(string name);
  void handle_response(MClass *m);
};


#endif
