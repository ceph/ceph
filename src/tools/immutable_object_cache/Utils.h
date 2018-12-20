// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_UTILS_H
#define CEPH_CACHE_UTILS_H

#include "include/rados/librados.hpp"
#include "include/Context.h"


namespace ceph {
namespace immutable_obj_cache {
namespace detail {

template <typename T>
void rados_callback(rados_completion_t c, void *arg) {
  reinterpret_cast<T*>(arg)->complete(rados_aio_get_return_value(c));
}

template <typename T, void(T::*MF)(int)>
void rados_callback(rados_completion_t c, void *arg) {
  T *obj = reinterpret_cast<T*>(arg);
  int r = rados_aio_get_return_value(c);
  (obj->*MF)(r);
}

template <typename T, Context*(T::*MF)(int*), bool destroy>
void rados_state_callback(rados_completion_t c, void *arg) {
  T *obj = reinterpret_cast<T*>(arg);
  int r = rados_aio_get_return_value(c);
  Context *on_finish = (obj->*MF)(&r);
  if (on_finish != nullptr) {
    on_finish->complete(r);
    if (destroy) {
      delete obj;
    }
  }
}

template <typename T, void (T::*MF)(int)>
class C_CallbackAdapter : public Context {
  T *obj;
public:
  C_CallbackAdapter(T *obj) : obj(obj) {
  }

protected:
  void finish(int r) override {
    (obj->*MF)(r);
  }
};

template <typename T, Context*(T::*MF)(int*), bool destroy>
class C_StateCallbackAdapter : public Context {
  T *obj;
public:
  C_StateCallbackAdapter(T *obj) : obj(obj){
  }

protected:
  void complete(int r) override {
    Context *on_finish = (obj->*MF)(&r);
    if (on_finish != nullptr) {
      on_finish->complete(r);
      if (destroy) {
        delete obj;
      }
    }
    Context::complete(r);
  }
  void finish(int r) override {
  }
};

template <typename WQ>
struct C_AsyncCallback : public Context {
  WQ *op_work_queue;
  Context *on_finish;

  C_AsyncCallback(WQ *op_work_queue, Context *on_finish)
    : op_work_queue(op_work_queue), on_finish(on_finish) {
  }
  void finish(int r) override {
    op_work_queue->queue(on_finish, r);
  }
};

} // namespace detail


librados::AioCompletion *create_rados_callback(Context *on_finish);

template <typename T>
librados::AioCompletion *create_rados_callback(T *obj) {
  return librados::Rados::aio_create_completion(
    obj, &detail::rados_callback<T>, nullptr);
}

template <typename T, void(T::*MF)(int)>
librados::AioCompletion *create_rados_callback(T *obj) {
  return librados::Rados::aio_create_completion(
    obj, &detail::rados_callback<T, MF>, nullptr);
}

template <typename T, Context*(T::*MF)(int*), bool destroy=true>
librados::AioCompletion *create_rados_callback(T *obj) {
  return librados::Rados::aio_create_completion(
    obj, &detail::rados_state_callback<T, MF, destroy>, nullptr);
}

} // namespace immutable_obj_cache
} // namespace ceph
#endif
