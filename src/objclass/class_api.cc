
#include "config.h"

#include "objclass/objclass.h"
#include "osd/OSD.h"
#include "osd/ReplicatedPG.h"

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

int cls_register_cxx_method(cls_handle_t hclass, const char *method,
			    cls_method_cxx_call_t class_call, cls_method_handle_t *handle)
{
  ClassHandler::ClassData *cd;
  cls_method_handle_t hmethod;

  cd = (ClassHandler::ClassData *)hclass;
  hmethod  = (cls_method_handle_t)cd->register_cxx_method(method, class_call);
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

int cls_rdcall(cls_method_handle_t hctx, const char *cls, const char *method,
                                 char *indata, int datalen,
                                 char **outdata, int *outdatalen)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  bufferlist odata;
  bufferlist idata;
  vector<ceph_osd_op> nops(1);
  ceph_osd_op& op = nops[0];
  int r;

  op.op = CEPH_OSD_OP_RDCALL;
  op.class_len = strlen(cls);
  op.method_len = strlen(method);
  op.indata_len = datalen;
  idata.append(cls, op.class_len);
  idata.append(method, op.method_len);
  idata.append(indata, datalen);
  bufferlist::iterator iter = idata.begin();
  r = (*pctx)->pg->do_osd_ops(*pctx, nops, iter, odata);

  *outdata = odata.c_str();
  *outdatalen = odata.length();
#warning use after free!

  return r;
}

int cls_read(cls_method_handle_t hctx, int ofs, int len,
                                 char **outdata, int *outdatalen)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<ceph_osd_op> ops(1);
  ops[0].op = CEPH_OSD_OP_READ;
  ops[0].offset = ofs;
  ops[0].length = len;
  bufferlist idata, odata;
  bufferlist::iterator iter = idata.begin();
  int r = (*pctx)->pg->do_osd_ops(*pctx, ops, iter, odata);

  *outdata = odata.c_str();
  *outdatalen = odata.length();
#warning use after free!

  return r;
}

int cls_cxx_read(cls_method_handle_t hctx, int ofs, int len, bufferlist *outbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<ceph_osd_op> ops(1);
  ops[0].op = CEPH_OSD_OP_READ;
  ops[0].offset = ofs;
  ops[0].length = len;
  bufferlist idata;
  bufferlist::iterator iter = idata.begin();
  return (*pctx)->pg->do_osd_ops(*pctx, ops, iter, *outbl);
}

int cls_cxx_write(cls_method_handle_t hctx, int ofs, int len, bufferlist *inbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<ceph_osd_op> ops(1);
  ops[0].op = CEPH_OSD_OP_WRITE;
  ops[0].offset = ofs;
  ops[0].length = len;
  bufferlist outbl;
  bufferlist::iterator iter = inbl->begin();
  return (*pctx)->pg->do_osd_ops(*pctx, ops, iter, outbl);
}

int cls_cxx_replace(cls_method_handle_t hctx, int ofs, int len, bufferlist *inbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<ceph_osd_op> ops(2);
  ops[0].op = CEPH_OSD_OP_TRUNCATE;
  ops[0].offset = 0;
  ops[0].length = 0;
  ops[1].op = CEPH_OSD_OP_WRITE;
  ops[1].offset = ofs;
  ops[1].length = len;
  bufferlist outbl;
  bufferlist::iterator iter = inbl->begin();
  return (*pctx)->pg->do_osd_ops(*pctx, ops, iter, outbl);
}

