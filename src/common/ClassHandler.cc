
#include "include/types.h"
#include "msg/Message.h"
#include "osd/OSD.h"
#include "messages/MClass.h"
#include "ClassHandler.h"

#include <dlfcn.h>

#include <map>

#include "config.h"

#define DOUT_SUBSYS osd
#undef dout_prefix
#define dout_prefix *_dout << dbeginl


void ClassHandler::load_class(const nstring& cname)
{
  dout(10) << "load_class " << cname << dendl;

  ClassData& data = classes[cname];
  char *fname=strdup("/tmp/class-XXXXXX");
  int fd = mkstemp(fname);

  for (list<bufferptr>::const_iterator it = data.impl.binary.buffers().begin();
       it != data.impl.binary.buffers().end(); it++)
    write(fd, it->c_str(), it->length());

  close(fd);

  data.handle = dlopen(fname, RTLD_LAZY);
  void (*cls_init)() = (void (*)())dlsym(data.handle, "class_init");
  if (cls_init)
    cls_init();

  unlink(fname);
  free(fname);
}


ClassHandler::ClassData *ClassHandler::get_class(const nstring& cname, ClassVersion& version)
{
  ClassData *class_data = &classes[cname];

  switch (class_data->status) {
  case ClassData::CLASS_LOADED:
    return class_data;
    
  case ClassData::CLASS_REQUESTED:
    return NULL;
    break;

  case ClassData::CLASS_UNKNOWN:
    class_data->status = ClassData::CLASS_REQUESTED;
    break;

  case ClassData::CLASS_INVALID:
    return class_data;

  default:
    assert(0);
  }

  class_data->version = version;

  osd->send_class_request(cname.c_str(), version);
  return NULL;
}

void ClassHandler::handle_class(MClass *m)
{
  deque<ClassInfo>::iterator info_iter;
  deque<ClassImpl>::iterator impl_iter;
  deque<bool>::iterator add_iter;
  
  for (info_iter = m->info.begin(), add_iter = m->add.begin(), impl_iter = m->impl.begin();
       info_iter != m->info.end();
       ++info_iter, ++add_iter) {
    ClassData& data = classes[info_iter->name];
    
    if (*add_iter) {
      
      if (data.status == ClassData::CLASS_REQUESTED) {
	dout(10) << "added class '" << info_iter->name << "'" << dendl;
	data.impl = *impl_iter;
	++impl_iter;
	data.status = ClassData::CLASS_LOADED;
	
	load_class(info_iter->name);
	osd->got_class(info_iter->name);
      }
    } else {
        data.status = ClassData::CLASS_INVALID;
        osd->got_class(info_iter->name);
    }
  }
}


void ClassHandler::resend_class_requests()
{
  for (map<nstring,ClassData>::iterator p = classes.begin(); p != classes.end(); p++) {
    dout(20) << "resending class request "<< p->first.c_str() << " v" << p->second.version << dendl;
    osd->send_class_request(p->first.c_str(), p->second.version);
  }
}

ClassHandler::ClassData *ClassHandler::register_class(const char *cname)
{
  ClassData& class_data = classes[cname];

  if (class_data.status != ClassData::CLASS_LOADED) {
    dout(0) << "class " << cname << " can't be loaded" << dendl;
    return NULL;
  }

  if (class_data.registered) {
    dout(0) << "class " << cname << " already registered" << dendl;
  }

  class_data.registered = true;

  return &class_data;
}

void ClassHandler::unregister_class(ClassHandler::ClassData *cls)
{
  /* FIXME: do we really need this one? */
}

ClassHandler::ClassMethod *ClassHandler::ClassData::register_method(const char *mname, cls_method_call_t func)
{
  ClassMethod& method = methods_map[mname];
  method.func = func;
  method.name = mname;
  method.cls = this;

  return &method;
}

ClassHandler::ClassMethod *ClassHandler::ClassData::get_method(const char *mname)
{
   map<string, ClassHandler::ClassMethod>::iterator iter = methods_map.find(mname);

  if (iter == methods_map.end())
    return NULL;

  return &(iter->second);
}

void ClassHandler::ClassData::unregister_method(ClassHandler::ClassMethod *method)
{
   map<string, ClassMethod>::iterator iter;

   iter = methods_map.find(method->name);
   if (iter == methods_map.end())
     return;

   methods_map.erase(iter);
}

void ClassHandler::ClassMethod::unregister()
{
  cls->unregister_method(this);
}

int ClassHandler::ClassMethod::exec(cls_method_context_t ctx, bufferlist& indata, bufferlist& outdata)
{
  char *out = NULL;
  int olen;
  int ret;
  ret = func(ctx, indata.c_str(), indata.length(), &out, &olen);
  if (out)
    outdata.append(out, olen);

  return ret;
}
