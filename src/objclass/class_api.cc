
#include "config.h"

#include "objclass/objclass.h"
#include "osd/OSD.h"

#include "common/ClassHandler.h"

static OSD *osd;

void cls_initialize(OSD *_osd)
{
    osd = _osd;
}

void cls_finalize()
{
    osd = NULL;
}


void *cls_alloc(size_t size)
{
  return malloc(size);
}

void cls_free(void *p)
{
  free(p);
}

int cls_register(const char *name, cls_handle_t *handle)
{
  ClassHandler::ClassData *cd;

  cd = osd->class_handler->register_class(name);

  *handle = (cls_handle_t)cd;

  return (cd != NULL);
}

int cls_unregister(cls_handle_t handle)
{
  ClassHandler::ClassData *cd;
  cd = (ClassHandler::ClassData *)handle;

  osd->class_handler->unregister_class(cd);
  return 1;
}

int cls_register_method(cls_handle_t hclass, const char *method,
                        cls_method_call_t class_call, cls_method_handle_t *handle)
{
  ClassHandler::ClassData *cd;
  cls_method_handle_t hmethod;

  cd = (ClassHandler::ClassData *)hclass;

  hmethod  = (cls_method_handle_t)cd->register_method(method, class_call);

  if (handle)
    *handle = hmethod;

  return (hmethod != NULL);
}

int cls_unregister_method(cls_method_handle_t handle)
{
  ClassHandler::ClassMethod *method = (ClassHandler::ClassMethod *)handle;
  method->unregister();

  return 1;
}

