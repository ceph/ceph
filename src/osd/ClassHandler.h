// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_CLASSHANDLER_H
#define CEPH_CLASSHANDLER_H

#include <variant>

#include "include/types.h"
#include "common/ceph_mutex.h"
#include "objclass/objclass.h"

//forward declaration
class CephContext;

class ClassHandler
{
public:
  CephContext *cct;

  struct ClassData;

  struct ClassMethod {
    const std::string name;
    using func_t = std::variant<cls_method_cxx_call_t, cls_method_call_t>;
    func_t func;
    int flags = 0;
    ClassData *cls = nullptr;

    int exec(cls_method_context_t ctx,
	     ceph::bufferlist& indata,
	     ceph::bufferlist& outdata);
    void unregister();

    int get_flags() {
      std::lock_guard l(cls->handler->mutex);
      return flags;
    }
    ClassMethod(const char* name, func_t call, int flags, ClassData* cls)
      : name{name}, func{call}, flags{flags}, cls{cls}
    {}
  };

  struct ClassFilter {
    ClassData *cls = nullptr;
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

    std::string name;
    ClassHandler *handler;
    void *handle;

    bool whitelisted = false;

    std::map<std::string, ClassMethod> methods_map;
    std::map<std::string, ClassFilter> filters_map;

    std::set<ClassData *> dependencies;         /* our dependencies */
    std::set<ClassData *> missing_dependencies; /* only missing dependencies */

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
      std::lock_guard l(handler->mutex);
      return _get_method(mname);
    }
    int get_method_flags(const char *mname);

    ClassFilter *get_filter(const std::string &filter_name)
    {
      std::lock_guard l(handler->mutex);
      std::map<std::string, ClassFilter>::iterator i = filters_map.find(filter_name);
      if (i == filters_map.end()) {
        return NULL;
      } else {
        return &(i->second);
      }
    }
  };

private:
  std::map<std::string, ClassData> classes;

  ClassData *_get_class(const std::string& cname, bool check_allowed);
  int _load_class(ClassData *cls);

  static bool in_class_list(const std::string& cname,
      const std::string& list);

public:
  ceph::mutex mutex = ceph::make_mutex("ClassHandler");

  explicit ClassHandler(CephContext *cct_) : cct(cct_) {}

  int open_all_classes();
  int open_class(const std::string& cname, ClassData **pcls);

  ClassData *register_class(const char *cname);
  void unregister_class(ClassData *cls);

  void shutdown();
};


#endif
