// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_CLASS_HANDLER_H
#define CEPH_TEST_CLASS_HANDLER_H

#include "objclass/objclass.h"
#include "common/snap_types.h"
#include <boost/shared_ptr.hpp>
#include <list>
#include <map>
#include <string>

namespace librados
{

class TestIoCtxImpl;

class TestClassHandler {
public:

  TestClassHandler();
  ~TestClassHandler();

  struct MethodContext {
    ~MethodContext();

    TestIoCtxImpl *io_ctx_impl;
    std::string oid;
    SnapContext snapc;
  };
  typedef boost::shared_ptr<MethodContext> SharedMethodContext;

  struct Method {
    cls_method_cxx_call_t class_call;
  };
  typedef boost::shared_ptr<Method> SharedMethod;
  typedef std::map<std::string, SharedMethod> Methods;

  struct Class {
    Methods methods;
  };
  typedef boost::shared_ptr<Class> SharedClass;

  void open_all_classes();

  int create(const std::string &name, cls_handle_t *handle);
  int create_method(cls_handle_t hclass, const char *method,
                    cls_method_cxx_call_t class_call,
                    cls_method_handle_t *handle);
  cls_method_cxx_call_t get_method(const std::string &cls,
                                   const std::string &method);
  SharedMethodContext get_method_context(TestIoCtxImpl *io_ctx_impl,
                                         const std::string &oid,
                                         const SnapContext &snapc);

private:

  typedef std::map<std::string, SharedClass> Classes;
  typedef std::list<void*> ClassHandles;

  Classes m_classes;
  ClassHandles m_class_handles;

  void open_class(const std::string& name, const std::string& path);

};

} // namespace librados

#endif // CEPH_TEST_CLASS_HANDLER_H
