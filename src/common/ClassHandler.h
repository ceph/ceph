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
    bool add_dependency(cls_deps_t *dep);
    void add_dependent(ClassData& dependent);

    void satisfy_dependency(ClassData *cls);

    enum { 
      CLASS_UNKNOWN, 
      CLASS_INVALID, 
      CLASS_LOADED, 
      CLASS_REQUESTED, 
    } status;
    ClassVersion version;
    ClassImpl impl;
    nstring name;
    OSD *osd;
    ClassHandler *handler;
    void *handle;
    bool registered;
    map<nstring, ClassMethod> methods_map;

    map<nstring, ClassData *> dependencies; /* our dependencies */
    map<nstring, ClassData *> missing_dependencies; /* only missing dependencies */
    list<ClassData *> dependents;          /* classes that depend on us */

    bool has_missing_deps() { return (missing_dependencies.size() > 0); }

    ClassData() : status(CLASS_UNKNOWN), version(), handle(NULL), registered(false)  {}
    ~ClassData() { }

    ClassMethod *register_method(const char *mname,
                          cls_method_call_t func);
    ClassMethod *get_method(const char *mname);
    void unregister_method(ClassMethod *method);

    void load();
    void init();
  };
  map<nstring, ClassData> classes;

  ClassData& get_obj(const nstring& cname);

  void load_class(const nstring& cname);

  ClassHandler(OSD *_osd) : osd(_osd) {}

  ClassData *get_class(const nstring& cname, ClassVersion& version);
  void resend_class_requests();

  void handle_class(MClass *m);

  ClassData *register_class(const char *cname);
  void unregister_class(ClassData *cls);

};


#endif
