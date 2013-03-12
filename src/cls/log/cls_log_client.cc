#include <errno.h>

#include "include/types.h"
#include "cls/log/cls_log_ops.h"
#include "include/rados/librados.hpp"


using namespace librados;




void cls_log_add(librados::ObjectWriteOperation& op, cls_log_entry& entry)
{
  bufferlist in;
  cls_log_add_op call;
  call.entry = entry;
  ::encode(call, in);
  op.exec("log", "add", in);
}

void cls_log_trim(librados::ObjectWriteOperation& op, utime_t& from, utime_t& to)
{
  bufferlist in;
  cls_log_trim_op call;
  call.from_time = from;
  call.to_time = to;
  ::encode(call, in);
  op.exec("log", "trim", in);
}

int cls_log_trim(librados::IoCtx& io_ctx, string& oid, utime_t& from, utime_t& to)
{
  bool done = false;

  do {
    ObjectWriteOperation op;

    cls_log_trim(op, from, to);

    int r = io_ctx.operate(oid, &op);
    if (r == -ENODATA)
      done = true;
    else if (r < 0)
      return r;

  } while (!done);


  return 0;
}

class LogListCtx : public ObjectOperationCompletion {
  list<cls_log_entry> *entries;
  bool *truncated;
public:
  LogListCtx(list<cls_log_entry> *_entries, bool *_truncated) :
                                      entries(_entries), truncated(_truncated) {}
  void handle_completion(int r, bufferlist& outbl) {
    if (r >= 0) {
      cls_log_list_ret ret;
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(ret, iter);
	*entries = ret.entries;
        *truncated = ret.truncated;
      } catch (buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_log_list(librados::ObjectReadOperation& op, utime_t& from, int max,
                  list<cls_log_entry>& entries, bool *truncated)
{
  bufferlist inbl;
  cls_log_list_op call;
  call.from_time = from;
  call.num_entries = max;
  op.exec("log", "list", inbl, new LogListCtx(&entries, truncated));
}

