// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestClassHandler.h"
#include "test/librados_test_stub/TestIoCtxImpl.h"
#include <boost/algorithm/string/predicate.hpp>
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "common/debug.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rados

namespace librados {

TestClassHandler::TestClassHandler() {
}

TestClassHandler::~TestClassHandler() {
  for (ClassHandles::iterator it = m_class_handles.begin();
      it != m_class_handles.end(); ++it) {
    dlclose(*it);
  }
}

void TestClassHandler::open_class(const std::string& name,
                                  const std::string& path) {
  void *handle = dlopen(path.c_str(), RTLD_NOW);
  if (handle == NULL) {
    derr << "Failed to load class: " << dlerror() << dendl;
    return;
  }
  m_class_handles.push_back(handle);

  // initialize
  void (*cls_init)() = reinterpret_cast<void (*)()>(
      dlsym(handle, "__cls_init"));
  if (cls_init) {
    cls_init();
  }
}

void TestClassHandler::open_all_classes() {
  assert(m_class_handles.empty());

  const char* env = getenv("CEPH_LIB");
  std::string CEPH_LIB(env ? env : "lib");
  DIR *dir = ::opendir(CEPH_LIB.c_str());
  if (dir == NULL) {
    assert(false);;
  }

  char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];
  struct dirent *pde;
  int r = 0;
  while ((r = ::readdir_r(dir, (dirent *)&buf, &pde)) == 0 && pde) {
    std::string name(pde->d_name);
    if (!boost::algorithm::starts_with(name, "libcls_") ||
        !boost::algorithm::ends_with(name, ".so")) {
      continue;
    }
    std::string class_name = name.substr(7, name.size() - 10);
    open_class(class_name, CEPH_LIB + "/" + name);
  }
  closedir(dir);
}

int TestClassHandler::create(const std::string &name, cls_handle_t *handle) {
  if (m_classes.find(name) != m_classes.end()) {
    return -EEXIST;
  }

  SharedClass cls(new Class());
  m_classes[name] = cls;
  *handle = reinterpret_cast<cls_handle_t>(cls.get());
  return 0;
}

int TestClassHandler::create_method(cls_handle_t hclass,
                                    const char *name,
                                    cls_method_cxx_call_t class_call,
                                    cls_method_handle_t *handle) {
  Class *cls = reinterpret_cast<Class*>(hclass);
  if (cls->methods.find(name) != cls->methods.end()) {
    return -EEXIST;
  }

  SharedMethod method(new Method());
  method->class_call = class_call;
  cls->methods[name] = method;
  return 0;
}

cls_method_cxx_call_t TestClassHandler::get_method(const std::string &cls,
                                                   const std::string &method) {
  Classes::iterator c_it = m_classes.find(cls);
  if (c_it == m_classes.end()) {
    return NULL;
  }

  SharedClass scls = c_it->second;
  Methods::iterator m_it = scls->methods.find(method);
  if (m_it == scls->methods.end()) {
    return NULL;
  }
  return m_it->second->class_call;
}

TestClassHandler::SharedMethodContext TestClassHandler::get_method_context(
    TestIoCtxImpl *io_ctx_impl, const std::string &oid,
    const SnapContext &snapc) {
  SharedMethodContext ctx(new MethodContext());

  // clone to ioctx to provide a firewall for gmock expectations
  ctx->io_ctx_impl = io_ctx_impl->clone();
  ctx->oid = oid;
  ctx->snapc = snapc;
  return ctx;
}

TestClassHandler::MethodContext::~MethodContext() {
  io_ctx_impl->put();
}

} // namespace librados
