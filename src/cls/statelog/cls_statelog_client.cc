#include <errno.h>

#include "include/types.h"
#include "cls/statelog/cls_statelog_ops.h"
#include "include/rados/librados.hpp"


using namespace librados;


void cls_statelog_add(librados::ObjectWriteOperation& op, list<cls_statelog_entry>& entries)
{
  bufferlist in;
  cls_statelog_add_op call;
  call.entries = entries;
  ::encode(call, in);
  op.exec("statelog", "add", in);
}

void cls_statelog_add(librados::ObjectWriteOperation& op, cls_statelog_entry& entry)
{
  bufferlist in;
  cls_statelog_add_op call;
  call.entries.push_back(entry);
  ::encode(call, in);
  op.exec("statelog", "add", in);
}

void cls_statelog_add_prepare_entry(cls_statelog_entry& entry, const string& client_id, const string& op_id,
                 const string& object, const utime_t& timestamp, uint32_t state, bufferlist& bl)
{
  entry.client_id = client_id;
  entry.op_id = op_id;
  entry.object = object;
  entry.timestamp = timestamp;
  entry.state = state;
  entry.data = bl;
}

void cls_statelog_add(librados::ObjectWriteOperation& op, const string& client_id, const string& op_id,
                 const string& object, const utime_t& timestamp, uint32_t state, bufferlist& bl)

{
  cls_statelog_entry entry;

  cls_statelog_add_prepare_entry(entry, client_id, op_id, object, timestamp, state, bl);
  cls_statelog_add(op, entry);
}

void cls_statelog_remove_by_client(librados::ObjectWriteOperation& op, const string& client_id, const string& op_id)
{
  bufferlist in;
  cls_statelog_remove_op call;
  call.client_id = client_id;
  call.op_id = op_id;
  ::encode(call, in);
  op.exec("statelog", "remove", in);
}

void cls_statelog_remove_by_object(librados::ObjectWriteOperation& op, const string& object, const string& op_id)
{
  bufferlist in;
  cls_statelog_remove_op call;
  call.object = object;
  call.op_id = op_id;
  ::encode(call, in);
  op.exec("statelog", "remove", in);
}

class StateLogListCtx : public ObjectOperationCompletion {
  list<cls_statelog_entry> *entries;
  string *marker;
  bool *truncated;
public:
  StateLogListCtx(list<cls_statelog_entry> *_entries, string *_marker, bool *_truncated) :
                                      entries(_entries), marker(_marker), truncated(_truncated) {}
  void handle_completion(int r, bufferlist& outbl) {
    if (r >= 0) {
      cls_statelog_list_ret ret;
      try {
        bufferlist::iterator iter = outbl.begin();
        ::decode(ret, iter);
        if (entries)
	  *entries = ret.entries;
        if (truncated)
          *truncated = ret.truncated;
        if (marker)
          *marker = ret.marker;
      } catch (buffer::error& err) {
        // nothing we can do about it atm
      }
    }
  }
};

void cls_statelog_list(librados::ObjectReadOperation& op,
                       const string& client_id, const string& op_id, const string& object, /* op_id may be empty, also one of client_id, object*/
                       const string& in_marker, int max_entries, list<cls_statelog_entry>& entries,
                       string *out_marker, bool *truncated)
{
  bufferlist inbl;
  cls_statelog_list_op call;
  call.client_id = client_id;
  call.op_id = op_id;
  call.object = object;
  call.marker = in_marker;
  call.max_entries = max_entries;

  ::encode(call, inbl);

  op.exec("statelog", "list", inbl, new StateLogListCtx(&entries, out_marker, truncated));
}

void cls_statelog_check_state(librados::ObjectOperation& op, const string& client_id, const string& op_id, const string& object, uint32_t state)
{
  bufferlist inbl;
  bufferlist outbl;
  cls_statelog_check_state_op call;
  call.client_id = client_id;
  call.op_id = op_id;
  call.object = object;
  call.state = state;

  ::encode(call, inbl);

  op.exec("statelog", "check_state", inbl, NULL);
}

