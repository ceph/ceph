// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * This is a simple example RADOS class, designed to be usable as a
 * template for implementing new methods.
 *
 * Our goal here is to illustrate the interface between the OSD and
 * the class and demonstrate what kinds of things a class can do.
 *
 * Note that any *real* class will probably have a much more
 * sophisticated protocol dealing with the in and out data buffers.
 * For an example of the model that we've settled on for handling that
 * in a clean way, please refer to cls_lock or cls_version for
 * relatively simple examples of how the parameter encoding can be
 * encoded in a way that allows for forward and backward compatibility
 * between client vs class revisions.
 */

/*
 * A quick note about bufferlists:
 *
 * The bufferlist class allows memory buffers to be concatenated,
 * truncated, spliced, "copied," encoded/embedded, and decoded.  For
 * most operations no actual data is ever copied, making bufferlists
 * very convenient for efficiently passing data around.
 *
 * bufferlist is actually a typedef of buffer::list, and is defined in
 * include/buffer.h (and implemented in common/buffer.cc).
 */

#include <algorithm>
#include <string>
#include <sstream>
#include <errno.h>

#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(hello)

cls_handle_t h_class;
cls_method_handle_t h_say_hello;
cls_method_handle_t h_record_hello;
cls_method_handle_t h_replay;
cls_method_handle_t h_writes_dont_return_data;
cls_method_handle_t h_turn_it_to_11;
cls_method_handle_t h_bad_reader;
cls_method_handle_t h_bad_writer;

/**
 * say hello - a "read" method that does not depend on the object
 *
 * This is an example of a method that does some computation and
 * returns data to the caller, without depending on the local object
 * content.
 */
static int say_hello(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // see if the input data from the client matches what this method
  // expects to receive.  your class can fill this buffer with what it
  // wants.
  if (in->length() > 100)
    return -EINVAL;

  // we generate our reply
  out->append("Hello, ");
  if (in->length() == 0)
    out->append("world");
  else
    out->append(*in);
  out->append("!");

  // this return value will be returned back to the librados caller
  return 0;
}

/**
 * record hello - a "write" method that creates an object
 *
 * This method modifies a local object (in this case, by creating it
 * if it doesn't exist).  We make multiple write calls (write,
 * setxattr) which are accumulated and applied as an atomic
 * transaction.
 */
static int record_hello(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // we can write arbitrary stuff to the ceph-osd debug log.  each log
  // message is accompanied by an integer log level.  smaller is
  // "louder".  how much of this makes it into the log is controlled
  // by the debug_cls option on the ceph-osd, similar to how other log
  // levels are controlled.  this message, at level 20, will generally
  // not be seen by anyone unless debug_cls is set at 20 or higher.
  CLS_LOG(20, "in record_hello");

  // see if the input data from the client matches what this method
  // expects to receive.  your class can fill this buffer with what it
  // wants.
  if (in->length() > 100)
    return -EINVAL;

  // only say hello to non-existent objects
  if (cls_cxx_stat(hctx, NULL, NULL) == 0)
    return -EEXIST;

  bufferlist content;
  content.append("Hello, ");
  if (in->length() == 0)
    content.append("world");
  else
    content.append(*in);
  content.append("!");

  // create/write the object
  int r = cls_cxx_write_full(hctx, &content);
  if (r < 0)
    return r;

  // also make note of who said it
  entity_inst_t origin;
  cls_get_request_origin(hctx, &origin);
  ostringstream ss;
  ss << origin;
  bufferlist attrbl;
  attrbl.append(ss.str());
  r = cls_cxx_setxattr(hctx, "said_by", &attrbl);
  if (r < 0)
    return r;

  // For write operations, there are two possible outcomes:
  //
  //  * For a failure, we return a negative error code.  The out
  //    buffer can contain any data that we want, and that data will
  //    be returned to the caller.  No change is made to the object.
  //
  //  * For a success, we must return 0 and *no* data in the out
  //    buffer.  This is becaues the OSD does not log write result
  //    codes or output buffers and we need a replayed/resent
  //    operation (e.g., after a TCP disconnect) to be idempotent.
  //
  //    If a class returns a positive value or puts data in the out
  //    buffer, the OSD code will ignore it and return 0 to the
  //    client.
  return 0;
}

static int writes_dont_return_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // make some change to the object
  bufferlist attrbl;
  attrbl.append("bar");
  int r = cls_cxx_setxattr(hctx, "foo", &attrbl);
  if (r < 0)
    return r;

  if (in->length() > 0) {
    // note that if we return anything < 0 (an error), this
    // operation/transaction will abort, and the setattr above will
    // never happen.  however, we *can* return data on error.
    out->append("too much input data!");
    return -EINVAL;
  }

  // try to return some data.  note that this *won't* reach the
  // client!  see the matching test case in test_cls_hello.cc.
  out->append("you will never see this");

  // if we try to return anything > 0 here the client will see 0.
  return 42;
}


/**
 * replay - a "read" method to get a previously recorded hello
 *
 * This is a read method that will retrieve a previously recorded
 * hello statement.
 */
static int replay(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // read contents out of the on-disk object.  our behavior can be a
  // function of either the request alone, or the request and the
  // on-disk state, depending on whether the RD flag is specified when
  // registering the method (see the __cls__init function below).
  int r = cls_cxx_read(hctx, 0, 1100, out);
  if (r < 0)
    return r;

  // note that our return value need not be the length of the returned
  // data; it can be whatever value we want: positive, zero or
  // negative (this is a read).
  return 0;
}

/**
 * turn_it_to_11 - a "write" method that mutates existing object data
 *
 * A write method can depend on previous object content (i.e., perform
 * a read/modify/write operation).  This atomically transitions the
 * object state from the old content to the new content.
 */
static int turn_it_to_11(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  // see if the input data from the client matches what this method
  // expects to receive.  your class can fill this buffer with what it
  // wants.
  if (in->length() != 0)
    return -EINVAL;

  bufferlist previous;
  int r = cls_cxx_read(hctx, 0, 1100, &previous);
  if (r < 0)
    return r;

  std::string str(previous.c_str(), previous.length());
  std::transform(str.begin(), str.end(), str.begin(), ::toupper);
  previous.clear();
  previous.append(str);

  // replace previous byte data content (write_full == truncate(0) + write)
  r = cls_cxx_write_full(hctx, &previous);
  if (r < 0)
    return r;

  // record who did it
  entity_inst_t origin;
  cls_get_request_origin(hctx, &origin);
  ostringstream ss;
  ss << origin;
  bufferlist attrbl;
  attrbl.append(ss.str());
  r = cls_cxx_setxattr(hctx, "amplified_by", &attrbl);
  if (r < 0)
    return r;

  // return value is 0 for success; out buffer is empty.
  return 0;
}

/**
 * example method that does not behave
 *
 * This method is registered as WR but tries to read
 */
static int bad_reader(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  return cls_cxx_read(hctx, 0, 100, out);
}

/**
 * example method that does not behave
 *
 * This method is registered as RD but tries to write
 */
static int bad_writer(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  return cls_cxx_write_full(hctx, in);
}


class PGLSHelloFilter : public PGLSFilter {
  string val;
public:
  int init(bufferlist::iterator& params) {
    try {
      ::decode(xattr, params);
      ::decode(val, params);
    } catch (buffer::error &e) {
      return -EINVAL;
    }
    return 0;
  }

  virtual ~PGLSHelloFilter() {}
  virtual bool filter(const hobject_t &obj, bufferlist& xattr_data,
                      bufferlist& outdata)
  {
    if (val.size() != xattr_data.length())
      return false;

    if (memcmp(val.c_str(), xattr_data.c_str(), val.size()))
      return false;

    return true;
  }
};


PGLSFilter *hello_filter()
{
  return new PGLSHelloFilter();
}


/**
 * initialize class
 *
 * We do two things here: we register the new class, and then register
 * all of the class's methods.
 */
void __cls_init()
{
  // this log message, at level 0, will always appear in the ceph-osd
  // log file.
  CLS_LOG(0, "loading cls_hello");

  cls_register("hello", &h_class);

  // There are two flags we specify for methods:
  //
  //    RD : whether this method (may) read prior object state
  //    WR : whether this method (may) write or update the object
  //
  // A method can be RD, WR, neither, or both.  If a method does
  // neither, the data it returns to the caller is a function of the
  // request and not the object contents.

  cls_register_cxx_method(h_class, "say_hello",
			  CLS_METHOD_RD,
			  say_hello, &h_say_hello);
  cls_register_cxx_method(h_class, "record_hello",
			  CLS_METHOD_WR | CLS_METHOD_PROMOTE,
			  record_hello, &h_record_hello);
  cls_register_cxx_method(h_class, "writes_dont_return_data",
			  CLS_METHOD_WR,
			  writes_dont_return_data, &h_writes_dont_return_data);
  cls_register_cxx_method(h_class, "replay",
			  CLS_METHOD_RD,
			  replay, &h_replay);

  // RD | WR is a read-modify-write method.
  cls_register_cxx_method(h_class, "turn_it_to_11",
			  CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PROMOTE,
			  turn_it_to_11, &h_turn_it_to_11);

  // counter-examples
  cls_register_cxx_method(h_class, "bad_reader", CLS_METHOD_WR,
			  bad_reader, &h_bad_reader);
  cls_register_cxx_method(h_class, "bad_writer", CLS_METHOD_RD,
			  bad_writer, &h_bad_writer);

  // A PGLS filter
  cls_register_cxx_filter(h_class, "hello", hello_filter);
}
