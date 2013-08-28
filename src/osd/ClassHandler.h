#ifndef CEPH_CLASSHANDLER_H
#define CEPH_CLASSHANDLER_H

#include "include/types.h"

#include "objclass/objclass.h"

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/ceph_context.h"


class ClassHandler
{
public:
  CephContext *cct;

  struct ClassData;

  struct ClassMethod {
    struct ClassHandler::ClassData *cls;
    string name;
    int flags;
    cls_method_call_t func;
    cls_method_cxx_call_t cxx_func;

    int exec(cls_method_context_t ctx, bufferlist& indata, bufferlist& outdata);
    void unregister();

    int get_flags() {
      Mutex::Locker l(cls->handler->mutex);
      return flags;
    }

    ClassMethod() : cls(0), flags(0), func(0), cxx_func(0) {}
  };

  struct ClassData {
    enum Status { 
      CLASS_UNKNOWN,
      CLASS_MISSING,         // missing
      CLASS_MISSING_DEPS,    // missing dependencies
      CLASS_INITIALIZING,    // calling init() right now
      CLASS_OPEN,            // initialized, usable
    } status;

    string name;
    ClassHandler *handler;
    void *handle;

    map<string, ClassMethod> methods_map;

    set<ClassData *> dependencies;         /* our dependencies */
    set<ClassData *> missing_dependencies; /* only missing dependencies */

    ClassMethod *_get_method(const char *mname);

    ClassData() : status(CLASS_UNKNOWN), 
		  handler(NULL),
		  handle(NULL) {}
    ~ClassData() { }

    ClassMethod *register_method(const char *mname, int flags, cls_method_call_t func);
    ClassMethod *register_cxx_method(const char *mname, int flags, cls_method_cxx_call_t func);
    void unregister_method(ClassMethod *method);

    ClassMethod *get_method(const char *mname) {
      Mutex::Locker l(handler->mutex);
      return _get_method(mname);
    }
    int get_method_flags(const char *mname);
  };

private:
  Mutex mutex;
  map<string, ClassData> classes;

  ClassData *_get_class(const string& cname);
  int _load_class(ClassData *cls);

public:
  ClassHandler(CephContext *cct_) : cct(cct_), mutex("ClassHandler") {}
  
  int open_all_classes();

  int open_class(const string& cname, ClassData **pcls);
  
  ClassData *register_class(const char *cname);
  void unregister_class(ClassData *cls);

  void shutdown();
};


#endif
