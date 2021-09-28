#ifndef _RPMA_OP_H_
#define _RPMA_OP_H_

#include <functional>
#include <memory>
#include <atomic>
#include "librpma.h"


namespace librbd::cache::pwl::rwl::replica {

class RpmaOp {
  std::function<void()> func;
public:
  RpmaOp(std::function<void()> f) : func(f) {}
  void do_callback() { if (func) func(); }
  virtual ~RpmaOp() {};
};

class RpmaRecv : public RpmaOp {
public:
  RpmaRecv(std::function<void()> f) : RpmaOp(f) {}
  virtual ~RpmaRecv() {}

  int operator() (struct rpma_conn *conn, struct rpma_mr_local *dst, size_t offset, size_t len, const void *op_context) {
    return rpma_recv(conn, dst, offset, len, op_context);
  }
};

class RpmaReqRecv : public RpmaOp {
public:
  RpmaReqRecv(std::function<void()> f) : RpmaOp(f) {}
  virtual ~RpmaReqRecv() {}

  int operator() (struct rpma_conn_req *req, struct rpma_mr_local *dst, size_t offset, size_t len, const void *op_context) {
    return rpma_conn_req_recv(req, dst, offset, len, op_context);
  }
};

class RpmaSend : public RpmaOp {
public:
  RpmaSend(std::function<void()> f) : RpmaOp(f) {}
  virtual ~RpmaSend() {}

  int operator() (struct rpma_conn *conn,
                  const struct rpma_mr_local *src,
                  size_t offset,
                  size_t len,
                  int flags,
                  const void *op_context) {
    return rpma_send(conn, src, offset, len, flags, op_context);
  }
};

class RpmaWrite : public RpmaOp {
public:
  RpmaWrite(std::function<void()> f = nullptr) : RpmaOp(f) {}
  virtual ~RpmaWrite() {}

  int operator() ( struct rpma_conn *conn,
                   struct rpma_mr_remote *dst,
                   size_t dst_offset,
                   const struct rpma_mr_local *src,
                   size_t src_offset,
                   size_t len,
                   int flags,
                   const void *op_context) {
    return rpma_write(conn, dst, dst_offset, src, src_offset, len, flags, op_context);
  }
};

class RpmaFlush : public RpmaOp {
public:
  RpmaFlush(std::function<void()> f = nullptr) : RpmaOp(f) {}
  virtual ~RpmaFlush() {}

  int operator() ( struct rpma_conn *conn,
                   struct rpma_mr_remote *dst,
                   size_t dst_offset,
                   size_t len,
                   enum rpma_flush_type type,
                   int flags,
                   const void *op_context) {
    return rpma_flush(conn, dst, dst_offset, len, type, flags, op_context);
  }
};

class RpmaRead : public RpmaOp {
public:
  RpmaRead(std::function<void()> f) : RpmaOp(f) {}
  virtual ~RpmaRead() {}

  int operator() (struct rpma_conn *conn,
                  struct rpma_mr_local *dst,
                  size_t dst_offset,
                  const struct rpma_mr_remote *src,
                  size_t src_offset,
                  size_t len,
                  int flags,
                  const void *op_context) {
    return rpma_read(conn, dst, dst_offset, src, src_offset, len, flags, op_context);
  }
};
} //namespace ceph::librbd::cache::pwl::rwl::replica
#endif //_RPMA_OP_H_
