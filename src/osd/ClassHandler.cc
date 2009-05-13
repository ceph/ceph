
#include "include/types.h"
#include "msg/Message.h"
#include "osd/OSD.h"
#include "messages/MClass.h"
#include "ClassHandler.h"


#include <map>

#include "config.h"

#define DOUT_SUBSYS osd
#undef dout_prefix
#define dout_prefix *_dout << dbeginl


bool ClassHandler::load_class(string name)
{
  bool need_request = false;
  Mutex::Locker locker(mutex);

  ClassData& class_data = objects[name];

  switch (class_data.status) {
    case CLASS_LOADED:
      return true;
    case CLASS_ERROR:
      return false;
    case CLASS_UNKNOWN:
      class_data.status = CLASS_REQUESTED;
      need_request = true;
      break;
    case CLASS_REQUESTED:
      need_request = false;
      break;
    default:
      assert(1);
  }

  if(need_request) {
    osd->get_class(name.c_str());
  }

  if (!class_data.queue) {
    if (!class_data.init_queue())
      return false;
  }

  class_data.queue->Wait(mutex);

  if (objects[name].status != CLASS_UNLOADED)
    return false;

  ClassImpl& impl=objects[name].impl;
  dout(0) << "received class " << name << " size=" << impl.binary.length() << dendl;


  char *fname=strdup("/tmp/class-XXXXXX");
  int fd = mkstemp(fname);

  for (list<bufferptr>::const_iterator it = impl.binary.buffers().begin();
       it != impl.binary.buffers().end(); it++) {
    write(fd, it->c_str(), it->length());
  }

  close(fd);

  free(fname);

  return true; 
}

void ClassHandler::handle_response(MClass *m)
{
  Mutex::Locker locker(mutex);

  deque<ClassLibrary>::iterator info_iter;
  deque<ClassImpl>::iterator impl_iter;
  deque<bool>::iterator add_iter;
  

  for (info_iter = m->info.begin(), add_iter = m->add.begin(), impl_iter = m->impl.begin();
       info_iter != m->info.end();
       ++info_iter, ++add_iter) {
    dout(0) << "handle_response class name=" << (*info_iter).name << dendl;
    bool need_notify = false;
    ClassData& data = objects[(*info_iter).name];
    if (data.status == CLASS_REQUESTED) {
      need_notify = true; 
    }
    
    if (*add_iter) {
      data.impl = *impl_iter;
      ++impl_iter;
      data.status = CLASS_UNLOADED;
    } else {
       /* fixme: handle case in which we didn't get the class */
    }
    if (need_notify) {
      data.queue->Signal();
    }
  }
}
