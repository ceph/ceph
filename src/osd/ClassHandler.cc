
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

  unlink(fname);
  free(fname);
}


bool ClassHandler::get_class(const nstring& cname)
{
  ClassData& class_data = classes[cname];

  switch (class_data.status) {
  case ClassData::CLASS_LOADED:
    return true;
    
  case ClassData::CLASS_REQUESTED:
    return false;
    break;

  case ClassData::CLASS_UNKNOWN:
    class_data.status = ClassData::CLASS_REQUESTED;
    break;

  default:
    assert(0);
  }

  osd->send_class_request(cname.c_str());
  return false;
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
	dout(0) << "added class '" << info_iter->name << "'" << dendl;
	data.impl = *impl_iter;
	++impl_iter;
	data.status = ClassData::CLASS_LOADED;
	
	load_class(info_iter->name);
	osd->got_class(info_iter->name);
      }
    } else {
      /* fixme: handle case in which we didn't get the class */
    }
  }
}


void ClassHandler::resend_class_requests()
{
  for (map<nstring,ClassData>::iterator p = classes.begin(); p != classes.end(); p++)
    osd->send_class_request(p->first.c_str());
}
