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
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
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
    std::cerr << "Failed to load class: " << name << " (" << path << "): "
              << dlerror() << std::endl;
    return;
  }

  // clear any existing error
  dlerror();

  // initialize
  void (*cls_init)() = reinterpret_cast<void (*)()>(
    dlsym(handle, "__cls_init"));

  char* error = nullptr;
  if ((error = dlerror()) != nullptr) {
    std::cerr << "Error locating initializer: " << error << std::endl;
  } else if (cls_init) {
    m_class_handles.push_back(handle);
    cls_init();
    return;
  }

  std::cerr << "Class: " << name << " (" << path << ") missing initializer"
            << std::endl;
  dlclose(handle);
}

void TestClassHandler::open_all_classes() {
  ceph_assert(m_class_handles.empty());

  const char* env = getenv("CEPH_LIB");
  std::string CEPH_LIB(env ? env : "lib");
  DIR *dir = ::opendir(CEPH_LIB.c_str());
  if (dir == NULL) {
    ceph_abort();;
  }

  std::set<std::string> names;
  struct dirent *pde = nullptr;
  while ((pde = ::readdir(dir))) {
    std::string name(pde->d_name);
    if (!boost::algorithm::starts_with(name, "libcls_") ||
        !boost::algorithm::ends_with(name, ".so")) {
      continue;
    }
    names.insert(name);
  }

  for (auto& name : names) {
    std::string class_name = name.substr(7, name.size() - 10);
    open_class(class_name, CEPH_LIB + "/" + name);
  }
  closedir(dir);
}

int TestClassHandler::create(const std::string &name, cls_handle_t *handle) {
  if (m_classes.find(name) != m_classes.end()) {
    std::cerr << "Class " << name << " already exists" << std::endl;
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
    std::cerr << "Class method " << hclass << ":" << name << " already exists"
              << std::endl;
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
    std::cerr << "Failed to located class " << cls << std::endl;
    return NULL;
  }

  SharedClass scls = c_it->second;
  Methods::iterator m_it = scls->methods.find(method);
  if (m_it == scls->methods.end()) {
    std::cerr << "Failed to located class method" << cls << "." << method
              << std::endl;
    return NULL;
  }
  return m_it->second->class_call;
}

TestClassHandler::SharedMethodContext TestClassHandler::get_method_context(
    TestIoCtxImpl *io_ctx_impl, const std::string &oid, uint64_t snap_id,
    const SnapContext &snapc) {
  SharedMethodContext ctx(new MethodContext());

  // clone to ioctx to provide a firewall for gmock expectations
  ctx->io_ctx_impl = io_ctx_impl->clone();
  ctx->oid = oid;
  ctx->snap_id = snap_id;
  ctx->snapc = snapc;
  return ctx;
}

int TestClassHandler::create_filter(cls_handle_t hclass,
				    const std::string& name,
				    cls_cxx_filter_factory_t fn)
{
  Class *cls = reinterpret_cast<Class*>(hclass);
  if (cls->filters.find(name) != cls->filters.end()) {
    return -EEXIST;
  }
  cls->filters[name] = fn;
  return 0;
}

TestClassHandler::MethodContext::~MethodContext() {
  io_ctx_impl->put();
}

} // namespace librados
