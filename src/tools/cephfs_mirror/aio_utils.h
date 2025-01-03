// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_AIO_UTILS_H
#define CEPHFS_MIRROR_AIO_UTILS_H

#include "include/rados/librados.hpp"

namespace cephfs {
namespace mirror {

template <typename T, void(T::*MF)(int)>
void rados_callback(rados_completion_t c, void *arg) {
  T *obj = reinterpret_cast<T*>(arg);
  int r = rados_aio_get_return_value(c);
  (obj->*MF)(r);
}

template <typename T, void (T::*MF)(int)>
class C_CallbackAdapter : public Context {
  T *obj;
public:
  C_CallbackAdapter(T *obj)
    : obj(obj) {
  }

protected:
  void finish(int r) override {
    (obj->*MF)(r);
  }
};

template <typename WQ>
struct C_AsyncCallback : public Context {
  WQ *op_work_queue;
  Context *on_finish;

  C_AsyncCallback(WQ *op_work_queue, Context *on_finish)
    : op_work_queue(op_work_queue), on_finish(on_finish) {
  }
  ~C_AsyncCallback() override {
    delete on_finish;
  }
  void finish(int r) override {
    op_work_queue->queue(on_finish, r);
    on_finish = nullptr;
  }
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_AIO_UTILS_H
