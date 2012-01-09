
#include "common/config.h"

#include "objclass/objclass.h"
#include "osd/ReplicatedPG.h"

#include "osd/ClassHandler.h"

static ClassHandler *ch;

void cls_initialize(ClassHandler *h)
{
  ch = h;
}

void cls_finalize()
{
  ch = NULL;
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
  ClassHandler::ClassData *cls = ch->register_class(name);
  *handle = (cls_handle_t)cls;
  return (cls != NULL);
}

int cls_unregister(cls_handle_t handle)
{
  ClassHandler::ClassData *cls = (ClassHandler::ClassData *)handle;
  ch->unregister_class(cls);
  return 1;
}

int cls_register_method(cls_handle_t hclass, const char *method,
                        int flags,
                        cls_method_call_t class_call, cls_method_handle_t *handle)
{
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

int cls_call(cls_method_context_t hctx, const char *cls, const char *method,
                                 char *indata, int datalen,
                                 char **outdata, int *outdatalen)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  bufferlist idata;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_CALL;
  op.op.cls.class_len = strlen(cls);
  op.op.cls.method_len = strlen(method);
  op.op.cls.indata_len = datalen;
  op.indata.append(cls, op.op.cls.class_len);
  op.indata.append(method, op.op.cls.method_len);
  op.indata.append(indata, datalen);
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);

  *outdata = (char *)malloc(op.outdata.length());
  memcpy(*outdata, op.outdata.c_str(), op.outdata.length());
  *outdatalen = op.outdata.length();

  return r;
}

int cls_getxattr(cls_method_context_t hctx, const char *name,
                                 char **outdata, int *outdatalen)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  bufferlist name_data;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_GETXATTR;
  op.indata.append(name);
  op.op.xattr.name_len = strlen(name);
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);

  *outdata = (char *)malloc(op.outdata.length());
  memcpy(*outdata, op.outdata.c_str(), op.outdata.length());
  *outdatalen = op.outdata.length();

  return r;
}

int cls_setxattr(cls_method_context_t hctx, const char *name,
                                 const char *value, int val_len)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  bufferlist name_data;
  vector<OSDOp> nops(1);
  OSDOp& op = nops[0];
  int r;

  op.op.op = CEPH_OSD_OP_SETXATTR;
  op.indata.append(name);
  op.indata.append(value);
  op.op.xattr.name_len = strlen(name);
  op.op.xattr.value_len = val_len;
  r = (*pctx)->pg->do_osd_ops(*pctx, nops);

  return r;
}

int cls_read(cls_method_context_t hctx, int ofs, int len,
                                 char **outdata, int *outdatalen)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_READ;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = len;
  int r = (*pctx)->pg->do_osd_ops(*pctx, ops);

  *outdata = (char *)malloc(ops[0].outdata.length());
  memcpy(*outdata, ops[0].outdata.c_str(), ops[0].outdata.length());
  *outdatalen = ops[0].outdata.length();

  if (r < 0)
    return r;

  return *outdatalen;
}

int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  int ret;
  ops[0].op.op = CEPH_OSD_OP_STAT;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;
  bufferlist::iterator iter = ops[0].outdata.begin();
  utime_t ut;
  uint64_t s;
  try {
    ::decode(s, iter);
    ::decode(ut, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  if (size)
    *size = s;
  if (mtime)
    *mtime = ut.sec();
  return 0;
}

int cls_cxx_read(cls_method_context_t hctx, int ofs, int len, bufferlist *outbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  int ret;
  ops[0].op.op = CEPH_OSD_OP_READ;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = len;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;
  outbl->claim(ops[0].outdata);
  return outbl->length();
}

int cls_cxx_write(cls_method_context_t hctx, int ofs, int len, bufferlist *inbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_WRITE;
  ops[0].op.extent.offset = ofs;
  ops[0].op.extent.length = len;
  ops[0].indata = *inbl;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_write_full(cls_method_context_t hctx, bufferlist *inbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_WRITEFULL;
  ops[0].op.extent.offset = 0;
  ops[0].op.extent.length = inbl->length();
  ops[0].indata = *inbl;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_replace(cls_method_context_t hctx, int ofs, int len, bufferlist *inbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(2);
  ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
  ops[0].op.extent.offset = 0;
  ops[0].op.extent.length = 0;
  ops[1].op.op = CEPH_OSD_OP_WRITE;
  ops[1].op.extent.offset = ofs;
  ops[1].op.extent.length = len;
  ops[1].indata = *inbl;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_snap_revert(cls_method_context_t hctx, snapid_t snapid)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_ROLLBACK;
  ops[0].op.snap.snapid = snapid;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_read_full(cls_method_context_t hctx, bufferlist* outbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  int ret;
  ops[0].op.op = CEPH_OSD_OP_TMAPGET;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;
  outbl->claim(ops[0].outdata);
  return outbl->length();
}

int cls_cxx_map_read_header(cls_method_context_t hctx, bufferlist *outbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  int ret;
  ops[0].op.op = CEPH_OSD_OP_TMAPGET;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;

  // decode and return the header
  bufferlist::iterator map_iter = ops[0].outdata.begin();
  ::decode(*outbl, map_iter);
  return 0;
}
int cls_cxx_map_read_key(cls_method_context_t hctx, string key, bufferlist *outbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  int ret;
  ops[0].op.op = CEPH_OSD_OP_TMAPGET;
  ret = (*pctx)->pg->do_osd_ops(*pctx, ops);
  if (ret < 0)
    return ret;

  //find and return just the requested key!
  bufferlist header;
  string next_key;
  bufferlist next_val;
  __u32 nkeys;
  bufferlist::iterator map_iter = ops[0].outdata.begin();
  ::decode(header, map_iter);
  ::decode(nkeys, map_iter);
  while (nkeys) {
    ::decode(next_key, map_iter);
    ::decode(next_val, map_iter);
    if (next_key == key) {
      *outbl = next_val;
      return 0;
    }
    if (next_key > key)
      return -ENOENT;
    --nkeys;
  }
  return -ENOENT;
}

int cls_cxx_map_write_full(cls_method_context_t hctx, bufferlist* inbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_TMAPPUT;
  ops[0].indata = *inbl;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_write_key(cls_method_context_t hctx, string key, bufferlist *inbl)
{
  bufferlist update_bl;
  update_bl.append(CEPH_OSD_TMAP_SET);
  ::encode(key, update_bl);
  ::encode(*inbl, update_bl);
  return cls_cxx_map_update(hctx, &update_bl);
}

int cls_cxx_map_write_header(cls_method_context_t hctx, bufferlist *inbl)
{
  ReplicatedPG::OpContext **pctx = static_cast<ReplicatedPG::OpContext **>(hctx);
  vector<OSDOp> ops(1);
  bufferlist update_bl;
  update_bl.append(CEPH_OSD_TMAP_HDR);
  ::encode(*inbl, update_bl);

  ops[0].op.op = CEPH_OSD_OP_TMAPUP;
  ops[0].indata = update_bl;

  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}

int cls_cxx_map_remove_key(cls_method_context_t hctx, string key)
{
  bufferlist update_bl;
  update_bl.append(CEPH_OSD_TMAP_RM);
  ::encode(key, update_bl);
  return cls_cxx_map_update(hctx, &update_bl);
}

int cls_cxx_map_update(cls_method_context_t hctx, bufferlist* inbl)
{
  ReplicatedPG::OpContext **pctx = (ReplicatedPG::OpContext **)hctx;
  vector<OSDOp> ops(1);
  ops[0].op.op = CEPH_OSD_OP_TMAPUP;
  ops[0].indata = *inbl;
  return (*pctx)->pg->do_osd_ops(*pctx, ops);
}
