#ifndef __CLASSHANDLER_H
#define __CLASHANDLER_H

#include "include/types.h"
#include "include/ClassLibrary.h"

#include "common/Cond.h"


class OSD;


class ClassHandler
{
  OSD *osd;

  struct ClassMethod {
    int (*func)(struct ceph_osd_op *op, char **indata, int datalen, char **outdata, int *outdatalen);
  };

  struct ClassData {
    enum { 
      CLASS_UNKNOWN, 
      //CLASS_UNLOADED, 
      CLASS_LOADED, 
      CLASS_REQUESTED, 
      //CLASS_ERROR
    } status;
    version_t version;
    ClassImpl impl;
    void *handle;
    bool registered;
    map<string name, ClassMethod> methods_map;

    ClassData() : status(CLASS_UNKNOWN), version(-1), handle(NULL), registered(false) {}
    ~ClassData() { }
  };
  map<nstring, ClassData> classes;

  void load_class(const nstring& cname);

public:
  ClassHandler(OSD *_osd) : osd(_osd) {}

  bool get_class(const nstring& cname);
  void resend_class_requests();

  void handle_class(MClass *m);
};


#endif
