#ifndef __CLASSHANDLER_H
#define __CLASSHANDLER_H

#include "include/types.h"
#include "include/ClassLibrary.h"

#include "objclass/objclass.h"

#include "common/Cond.h"
#include "common/ClassVersion.h"


class OSD;
class MClass;


class ClassHandler
{
  OSD *osd;

public:
  class ClassData;

  struct ClassMethod {
    struct ClassHandler::ClassData *cls;
    string name;
    cls_method_call_t func;
    int exec(cls_method_context_t ctx, bufferlist& indata, bufferlist& outdata);

    void unregister();
  };

  struct ClassData {
    enum { 
      CLASS_UNKNOWN, 
      //CLASS_UNLOADED, 
      CLASS_LOADED, 
      CLASS_REQUESTED, 
      //CLASS_ERROR
    } status;
    ClassVersion version;
    ClassImpl impl;
    void *handle;
    bool registered;
    map<string, ClassMethod> methods_map;

    ClassData() : status(CLASS_UNKNOWN), version(), handle(NULL), registered(false) {}
    ~ClassData() { }

    ClassMethod *register_method(const char *mname,
                          cls_method_call_t func);
    ClassMethod *get_method(const char *mname);
    void unregister_method(ClassMethod *method);
  };
  map<nstring, ClassData> classes;

  void load_class(const nstring& cname);

  ClassHandler(OSD *_osd) : osd(_osd) {}

  ClassData *get_class(const nstring& cname, ClassVersion& version);
  void resend_class_requests();

  void handle_class(MClass *m);

  ClassData *register_class(const char *cname);
  void unregister_class(ClassData *cls);

};


#endif
