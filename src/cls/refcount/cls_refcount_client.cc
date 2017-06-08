#include <errno.h>

#include "include/types.h"
#include "cls/refcount/cls_refcount_ops.h"
#include "include/rados/librados.hpp"

using namespace librados;


void cls_refcount_get(librados::ObjectWriteOperation& op, const string& tag, bool implicit_ref)
{
  bufferlist in;
  cls_refcount_get_op call;
  call.tag = tag;
  call.implicit_ref = implicit_ref;
  ::encode(call, in);
  op.exec("refcount", "get", in);
}

void cls_refcount_put(librados::ObjectWriteOperation& op, const string& tag, bool implicit_ref)
{
  bufferlist in;
  cls_refcount_put_op call;
  call.tag = tag;
  call.implicit_ref = implicit_ref;
  ::encode(call, in);
  op.exec("refcount", "put", in);
}

void cls_refcount_set(librados::ObjectWriteOperation& op, list<string>& refs)
{
  bufferlist in;
  cls_refcount_set_op call;
  call.refs = refs;
  ::encode(call, in);
  op.exec("refcount", "set", in);
}

int cls_refcount_read(librados::IoCtx& io_ctx, string& oid, list<string> *refs, bool implicit_ref)
{
  bufferlist in, out;
  cls_refcount_read_op call;
  call.implicit_ref = implicit_ref;
  ::encode(call, in);
  int r = io_ctx.exec(oid, "refcount", "read", in, out);
  if (r < 0)
    return r;

  cls_refcount_read_ret ret;
  try {
    bufferlist::iterator iter = out.begin();
    ::decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  *refs = ret.refs;

  return r;
}
