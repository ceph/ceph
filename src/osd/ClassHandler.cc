// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "ClassHandler.h"
#include "common/errno.h"
#include "common/ceph_context.h"

#include <dlfcn.h>

#include <map>

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "common/config.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout


#define CLS_PREFIX "libcls_"
#define CLS_SUFFIX ".so"


void ClassHandler::add_embedded_class(const string& cname)
{
  assert(mutex.is_locked());
  ClassData *cls = _get_class(cname, false);
  assert(cls->status == ClassData::CLASS_UNKNOWN);
  cls->status = ClassData::CLASS_INITIALIZING;
}

int ClassHandler::open_class(const string& cname, ClassData **pcls)
{
  Mutex::Locker lock(mutex);
  ClassData *cls = _get_class(cname, true);
  if (!cls)
    return -EPERM;
  if (cls->status != ClassData::CLASS_OPEN) {
    int r = _load_class(cls);
    if (r)
      return r;
  }
  *pcls = cls;
  return 0;
}

int ClassHandler::open_all_classes()
{
  ldout(cct, 10) << __func__ << dendl;
  DIR *dir = ::opendir(cct->_conf->osd_class_dir.c_str());
  if (!dir)
    return -errno;

  struct dirent *pde = nullptr;
  int r = 0;
  while ((pde = ::readdir(dir))) {
    if (pde->d_name[0] == '.')
      continue;
    if (strlen(pde->d_name) > sizeof(CLS_PREFIX) - 1 + sizeof(CLS_SUFFIX) - 1 &&
	strncmp(pde->d_name, CLS_PREFIX, sizeof(CLS_PREFIX) - 1) == 0 &&
	strcmp(pde->d_name + strlen(pde->d_name) - (sizeof(CLS_SUFFIX) - 1), CLS_SUFFIX) == 0) {
      char cname[PATH_MAX + 1];
      strncpy(cname, pde->d_name + sizeof(CLS_PREFIX) - 1, sizeof(cname) -1);
      cname[strlen(cname) - (sizeof(CLS_SUFFIX) - 1)] = '\0';
      ldout(cct, 10) << __func__ << " found " << cname << dendl;
      ClassData *cls;
      // skip classes that aren't in 'osd class load list'
      r = open_class(cname, &cls);
      if (r < 0 && r != -EPERM)
	goto out;
    }
  }
 out:
  closedir(dir);
  return r;
}

void ClassHandler::shutdown()
{
  for (auto& cls : classes) {
    if (cls.second.handle) {
      dlclose(cls.second.handle);
    }
  }
  classes.clear();
}

/*
 * Check if @cname is in the whitespace delimited list @list, or the @list
 * contains the wildcard "*".
 *
 * This is expensive but doesn't consume memory for an index, and is performed
 * only once when a class is loaded.
 */
bool ClassHandler::in_class_list(const std::string& cname,
    const std::string& list)
{
  std::istringstream ss(list);
  std::istream_iterator<std::string> begin{ss};
  std::istream_iterator<std::string> end{};

  const std::vector<std::string> targets{cname, "*"};

  auto it = std::find_first_of(begin, end,
      targets.begin(), targets.end());

  return it != end;
}

ClassHandler::ClassData *ClassHandler::_get_class(const string& cname,
    bool check_allowed)
{
  ClassData *cls;
  map<string, ClassData>::iterator iter = classes.find(cname);

  if (iter != classes.end()) {
    cls = &iter->second;
  } else {
    if (check_allowed && !in_class_list(cname, cct->_conf->osd_class_load_list)) {
      ldout(cct, 0) << "_get_class not permitted to load " << cname << dendl;
      return NULL;
    }
    cls = &classes[cname];
    ldout(cct, 10) << "_get_class adding new class name " << cname << " " << cls << dendl;
    cls->name = cname;
    cls->handler = this;
    cls->whitelisted = in_class_list(cname, cct->_conf->osd_class_default_list);
  }
  return cls;
}

int ClassHandler::_load_class(ClassData *cls)
{
  // already open
  if (cls->status == ClassData::CLASS_OPEN)
    return 0;

  if (cls->status == ClassData::CLASS_UNKNOWN ||
      cls->status == ClassData::CLASS_MISSING) {
    char fname[PATH_MAX];
    snprintf(fname, sizeof(fname), "%s/" CLS_PREFIX "%s" CLS_SUFFIX,
	     cct->_conf->osd_class_dir.c_str(),
	     cls->name.c_str());
    ldout(cct, 10) << "_load_class " << cls->name << " from " << fname << dendl;

    cls->handle = dlopen(fname, RTLD_NOW);
    if (!cls->handle) {
      struct stat st;
      int r = ::stat(fname, &st);
      if (r < 0) {
        r = -errno;
	ldout(cct, 0) << __func__ << " could not stat class " << fname
		      << ": " << cpp_strerror(r) << dendl;
      } else {
	ldout(cct, 0) << "_load_class could not open class " << fname
		      << " (dlopen failed): " << dlerror() << dendl;
	r = -EIO;
      }
      cls->status = ClassData::CLASS_MISSING;
      return r;
    }

    cls_deps_t *(*cls_deps)();
    cls_deps = (cls_deps_t *(*)())dlsym(cls->handle, "class_deps");
    if (cls_deps) {
      cls_deps_t *deps = cls_deps();
      while (deps) {
	if (!deps->name)
	  break;
	ClassData *cls_dep = _get_class(deps->name, false);
	cls->dependencies.insert(cls_dep);
	if (cls_dep->status != ClassData::CLASS_OPEN)
	  cls->missing_dependencies.insert(cls_dep);
	deps++;
      }
    }
  }

  // resolve dependencies
  set<ClassData*>::iterator p = cls->missing_dependencies.begin();
  while (p != cls->missing_dependencies.end()) {
    ClassData *dc = *p;
    int r = _load_class(dc);
    if (r < 0) {
      cls->status = ClassData::CLASS_MISSING_DEPS;
      return r;
    }

    ldout(cct, 10) << "_load_class " << cls->name << " satisfied dependency " << dc->name << dendl;
    cls->missing_dependencies.erase(p++);
  }

  // initialize
  void (*cls_init)() = (void (*)())dlsym(cls->handle, "__cls_init");
  if (cls_init) {
    cls->status = ClassData::CLASS_INITIALIZING;
    cls_init();
  }

  ldout(cct, 10) << "_load_class " << cls->name << " success" << dendl;
  cls->status = ClassData::CLASS_OPEN;
  return 0;
}



ClassHandler::ClassData *ClassHandler::register_class(const char *cname)
{
  assert(mutex.is_locked());

  ClassData *cls = _get_class(cname, false);
  ldout(cct, 10) << "register_class " << cname << " status " << cls->status << dendl;

  if (cls->status != ClassData::CLASS_INITIALIZING) {
    ldout(cct, 0) << "class " << cname << " isn't loaded; is the class registering under the wrong name?" << dendl;
    return NULL;
  }
  return cls;
}

void ClassHandler::unregister_class(ClassHandler::ClassData *cls)
{
  /* FIXME: do we really need this one? */
}

ClassHandler::ClassMethod *ClassHandler::ClassData::register_method(const char *mname,
                                                                    int flags,
								    cls_method_call_t func)
{
  /* no need for locking, called under the class_init mutex */
  if (!flags) {
    lderr(handler->cct) << "register_method " << name << "." << mname
			<< " flags " << flags << " " << (void*)func
			<< " FAILED -- flags must be non-zero" << dendl;
    return NULL;
  }
  ldout(handler->cct, 10) << "register_method " << name << "." << mname << " flags " << flags << " " << (void*)func << dendl;
  ClassMethod& method = methods_map[mname];
  method.func = func;
  method.name = mname;
  method.flags = flags;
  method.cls = this;
  return &method;
}

ClassHandler::ClassMethod *ClassHandler::ClassData::register_cxx_method(const char *mname,
                                                                        int flags,
									cls_method_cxx_call_t func)
{
  /* no need for locking, called under the class_init mutex */
  ldout(handler->cct, 10) << "register_cxx_method " << name << "." << mname << " flags " << flags << " " << (void*)func << dendl;
  ClassMethod& method = methods_map[mname];
  method.cxx_func = func;
  method.name = mname;
  method.flags = flags;
  method.cls = this;
  return &method;
}

ClassHandler::ClassFilter *ClassHandler::ClassData::register_cxx_filter(
    const std::string &filter_name,
    cls_cxx_filter_factory_t fn)
{
  ClassFilter &filter = filters_map[filter_name];
  filter.fn = fn;
  filter.name = filter_name;
  filter.cls = this;
  return &filter;
}

ClassHandler::ClassMethod *ClassHandler::ClassData::_get_method(const char *mname)
{
  map<string, ClassHandler::ClassMethod>::iterator iter = methods_map.find(mname);
  if (iter == methods_map.end())
    return NULL;
  return &(iter->second);
}

int ClassHandler::ClassData::get_method_flags(const char *mname)
{
  Mutex::Locker l(handler->mutex);
  ClassMethod *method = _get_method(mname);
  if (!method)
    return -ENOENT;
  return method->flags;
}

void ClassHandler::ClassData::unregister_method(ClassHandler::ClassMethod *method)
{
  /* no need for locking, called under the class_init mutex */
   map<string, ClassMethod>::iterator iter = methods_map.find(method->name);
   if (iter == methods_map.end())
     return;
   methods_map.erase(iter);
}

void ClassHandler::ClassMethod::unregister()
{
  cls->unregister_method(this);
}

void ClassHandler::ClassData::unregister_filter(ClassHandler::ClassFilter *filter)
{
  /* no need for locking, called under the class_init mutex */
   map<string, ClassFilter>::iterator iter = filters_map.find(filter->name);
   if (iter == filters_map.end())
     return;
   filters_map.erase(iter);
}

void ClassHandler::ClassFilter::unregister()
{
  cls->unregister_filter(this);
}

int ClassHandler::ClassMethod::exec(cls_method_context_t ctx, bufferlist& indata, bufferlist& outdata)
{
  int ret;
  if (cxx_func) {
    // C++ call version
    ret = cxx_func(ctx, &indata, &outdata);
  } else {
    // C version
    char *out = NULL;
    int olen = 0;
    ret = func(ctx, indata.c_str(), indata.length(), &out, &olen);
    if (out) {
      // assume *out was allocated via cls_alloc (which calls malloc!)
      buffer::ptr bp = buffer::claim_malloc(olen, out);
      outdata.push_back(bp);
    }
  }
  return ret;
}

