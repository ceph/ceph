// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <cstdarg>
#include "common/ceph_context.h"
#include "common/ceph_releases.h"
#include "common/config.h"
#include "common/debug.h"

#include "objclass/objclass.h"
#include "osd/ClassHandler.h"

#include "auth/Crypto.h"
#include "common/armor.h"

#define dout_context ClassHandler::get_instance().cct

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
  ClassHandler::ClassData *cls = \
    ClassHandler::get_instance().register_class(name);
  *handle = (cls_handle_t)cls;
  return (cls != NULL);
}

int cls_unregister(cls_handle_t handle)
{
  ClassHandler::ClassData *cls = (ClassHandler::ClassData *)handle;
  ClassHandler::get_instance().unregister_class(cls);
  return 1;
}

int cls_register_method(cls_handle_t hclass, const char *method,
                        int flags,
                        cls_method_call_t class_call, cls_method_handle_t *handle)
{
  if (!(flags & (CLS_METHOD_RD | CLS_METHOD_WR)))
    return -EINVAL;
  ClassHandler::ClassData *cls = (ClassHandler::ClassData *)hclass;
  cls_method_handle_t hmethod =(cls_method_handle_t)cls->register_method(method, flags, class_call);
  if (handle)
    *handle = hmethod;
  return (hmethod != NULL);
}

int cls_register_cxx_method(cls_handle_t hclass, const char *method,
                            int flags,
			    cls_method_cxx_call_t class_call, cls_method_handle_t *handle)
{
  ClassHandler::ClassData *cls = (ClassHandler::ClassData *)hclass;
  cls_method_handle_t hmethod = (cls_method_handle_t)cls->register_cxx_method(method, flags, class_call);
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

int cls_register_cxx_filter(cls_handle_t hclass,
                            const std::string &filter_name,
                            cls_cxx_filter_factory_t fn,
                            cls_filter_handle_t *handle)
{
  ClassHandler::ClassData *cls = (ClassHandler::ClassData *)hclass;
  cls_filter_handle_t hfilter = (cls_filter_handle_t)cls->register_cxx_filter(filter_name, fn);
  if (handle) {
    *handle = hfilter;
  }
  return (hfilter != NULL);
}

void cls_unregister_filter(cls_filter_handle_t handle)
{
  ClassHandler::ClassFilter *filter = (ClassHandler::ClassFilter *)handle;
  filter->unregister();
}

int cls_cxx_read(cls_method_context_t hctx, int ofs, int len,
		 ceph::buffer::list *outbl)
{
  return cls_cxx_read2(hctx, ofs, len, outbl, 0);
}

int cls_cxx_write(cls_method_context_t hctx, int ofs, int len,
		  ceph::buffer::list *inbl)
{
  return cls_cxx_write2(hctx, ofs, len, inbl, 0);
}

int cls_gen_random_bytes(char *buf, int size)
{
  ClassHandler::get_instance().cct->random()->get_bytes(buf, size);
  return 0;
}

int cls_gen_rand_base64(char *dest, int size) /* size should be the required string size + 1 */
{
  char buf[size];
  char tmp_dest[size + 4]; /* so that there's space for the extra '=' characters, and some */
  int ret;

  ret = cls_gen_random_bytes(buf, sizeof(buf));
  if (ret < 0) {
    derr << "cannot get random bytes: " << ret << dendl;
    return -1;
  }

  ret = ceph_armor(tmp_dest, &tmp_dest[sizeof(tmp_dest)],
		   (const char *)buf, ((const char *)buf) + ((size - 1) * 3 + 4 - 1) / 4);
  if (ret < 0) {
    derr << "ceph_armor failed" << dendl;
    return -1;
  }
  tmp_dest[ret] = '\0';
  memcpy(dest, tmp_dest, size);
  dest[size-1] = '\0';

  return 0;
}

void cls_cxx_subop_version(cls_method_context_t hctx, std::string *s)
{
  if (!s)
    return;

  char buf[32];
  uint64_t ver = cls_current_version(hctx);
  int subop_num = cls_current_subop_num(hctx);
  snprintf(buf, sizeof(buf), "%lld.%d", (long long)ver, subop_num);

  *s = buf;
}
