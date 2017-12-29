// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLASSHANDLER_H
#define CEPH_CLASSHANDLER_H

#include "include/types.h"
#include "objclass/objclass.h"
#include "common/Mutex.h"

//forward declaration
class CephContext;

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

  struct ClassFilter {
    struct ClassHandler::ClassData *cls = nullptr;
    std::string name;
    cls_cxx_filter_factory_t fn;

    void unregister();

    ClassFilter() : fn(0)
    {}
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

    bool whitelisted = false;

    map<string, ClassMethod> methods_map;
    map<string, ClassFilter> filters_map;

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

    ClassFilter *register_cxx_filter(
        const std::string &filter_name,
        cls_cxx_filter_factory_t fn);
    void unregister_filter(ClassFilter *method);

    ClassMethod *get_method(const char *mname) {
      Mutex::Locker l(handler->mutex);
      return _get_method(mname);
    }
    int get_method_flags(const char *mname);

    ClassFilter *get_filter(const std::string &filter_name)
    {
      Mutex::Locker l(handler->mutex);
      std::map<std::string, ClassFilter>::iterator i = filters_map.find(filter_name);
      if (i == filters_map.end()) {
        return NULL;
      } else {
        return &(i->second);
      }
    }
  };

private:
  map<string, ClassData> classes;

  ClassData *_get_class(const string& cname, bool check_allowed);
  int _load_class(ClassData *cls);

  static bool in_class_list(const std::string& cname,
      const std::string& list);

public:
  Mutex mutex;

  explicit ClassHandler(CephContext *cct_) : cct(cct_), mutex("ClassHandler") {}

  int open_all_classes();

  void add_embedded_class(const string& cname);
  int open_class(const string& cname, ClassData **pcls);
  
  ClassData *register_class(const char *cname);
  void unregister_class(ClassData *cls);

  void shutdown();
};


#endif
