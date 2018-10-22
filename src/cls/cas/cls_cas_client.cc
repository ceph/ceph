#include <errno.h>

#include "cls/cas/cls_cas_client.h"
#include "cls/cas/cls_cas_ops.h"
#include "include/rados/librados.hpp"

using namespace librados;

void cls_chunk_refcount_get(librados::ObjectWriteOperation& op, const hobject_t& soid)
{
  bufferlist in;
  cls_chunk_refcount_get_op call;
  call.source = soid;
  encode(call, in);
  op.exec("refcount", "chunk_get", in);
}

void cls_chunk_refcount_put(librados::ObjectWriteOperation& op, const hobject_t& soid)
{
  bufferlist in;
  cls_chunk_refcount_put_op call;
  call.source = soid;
  encode(call, in);
  op.exec("refcount", "chunk_put", in);
}

void cls_chunk_refcount_set(librados::ObjectWriteOperation& op, set<hobject_t>& refs)
{
  bufferlist in;
  cls_chunk_refcount_set_op call;
  call.refs = refs;
  encode(call, in);
  op.exec("refcount", "chunk_set", in);
}

int cls_chunk_refcount_read(librados::IoCtx& io_ctx, string& oid, set<hobject_t> *refs)
{
  bufferlist in, out;
  int r = io_ctx.exec(oid, "refcount", "chunk_read", in, out);
  if (r < 0)
    return r;

  cls_chunk_refcount_read_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  *refs = ret.refs;

  return r;
}
