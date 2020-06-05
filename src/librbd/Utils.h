// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_UTILS_H
#define CEPH_LIBRBD_UTILS_H

#include "include/rados/librados.hpp"
#include "include/rbd_types.h"
#include "include/ceph_assert.h"
#include "include/Context.h"
#include "common/zipkin_trace.h"
#include "common/RefCountedObj.h"

#include <atomic>
#include <type_traits>
#include <stdio.h>

namespace librbd {

class ImageCtx;

namespace util {
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

template <typename T, void (T::*MF)(int)>
class C_RefCallbackAdapter : public Context {
  RefCountedPtr refptr;
  Context *on_finish;

public:
  C_RefCallbackAdapter(T *obj, RefCountedPtr refptr)
    : refptr(std::move(refptr)),
      on_finish(new C_CallbackAdapter<T, MF>(obj)) {
  }

protected:
  void finish(int r) override {
    on_finish->complete(r);
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

template <typename T, Context*(T::*MF)(int*)>
class C_RefStateCallbackAdapter : public Context {
  RefCountedPtr refptr;
  Context *on_finish;

public:
  C_RefStateCallbackAdapter(T *obj, RefCountedPtr refptr)
    : refptr(std::move(refptr)),
      on_finish(new C_StateCallbackAdapter<T, MF, true>(obj)) {
  }

protected:
  void finish(int r) override {
    on_finish->complete(r);
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

} // namespace detail

std::string generate_image_id(librados::IoCtx &ioctx);

template <typename T>
inline std::string generate_image_id(librados::IoCtx &ioctx) {
  return generate_image_id(ioctx);
}

const std::string group_header_name(const std::string &group_id);
const std::string id_obj_name(const std::string &name);
const std::string header_name(const std::string &image_id);
const std::string old_header_name(const std::string &image_name);
std::string unique_lock_name(const std::string &name, void *address);

template <typename I>
std::string data_object_name(I* image_ctx, uint64_t object_no) {
  char buf[RBD_MAX_OBJ_NAME_SIZE];
  size_t length = snprintf(buf, RBD_MAX_OBJ_NAME_SIZE,
                           image_ctx->format_string, object_no);
  ceph_assert(length < RBD_MAX_OBJ_NAME_SIZE);

  std::string oid;
  oid.reserve(RBD_MAX_OBJ_NAME_SIZE);
  oid.append(buf, length);
  return oid;
}

librados::AioCompletion *create_rados_callback(Context *on_finish);

template <typename T>
librados::AioCompletion *create_rados_callback(T *obj) {
  return librados::Rados::aio_create_completion(
    obj, &detail::rados_callback<T>);
}

template <typename T, void(T::*MF)(int)>
librados::AioCompletion *create_rados_callback(T *obj) {
  return librados::Rados::aio_create_completion(
    obj, &detail::rados_callback<T, MF>);
}

template <typename T, Context*(T::*MF)(int*), bool destroy=true>
librados::AioCompletion *create_rados_callback(T *obj) {
  return librados::Rados::aio_create_completion(
    obj, &detail::rados_state_callback<T, MF, destroy>);
}

template <typename T, void(T::*MF)(int) = &T::complete>
Context *create_context_callback(T *obj) {
  return new detail::C_CallbackAdapter<T, MF>(obj);
}

template <typename T, Context*(T::*MF)(int*), bool destroy=true>
Context *create_context_callback(T *obj) {
  return new detail::C_StateCallbackAdapter<T, MF, destroy>(obj);
}

//for reference counting objects
template <typename T, void(T::*MF)(int) = &T::complete>
Context *create_context_callback(T *obj, RefCountedPtr refptr) {
  return new detail::C_RefCallbackAdapter<T, MF>(obj, refptr);
}

template <typename T, Context*(T::*MF)(int*)>
Context *create_context_callback(T *obj, RefCountedPtr refptr) {
  return new detail::C_RefStateCallbackAdapter<T, MF>(obj, refptr);
}

//for objects that don't inherit from RefCountedObj, to handle unit tests
template <typename T, void(T::*MF)(int) = &T::complete, typename R>
typename std::enable_if<not std::is_base_of<RefCountedPtr, R>::value, Context*>::type
create_context_callback(T *obj, R *refptr) {
  return new detail::C_CallbackAdapter<T, MF>(obj);
}

template <typename T, Context*(T::*MF)(int*), typename R, bool destroy=true>
typename std::enable_if<not std::is_base_of<RefCountedPtr, R>::value, Context*>::type
create_context_callback(T *obj, R *refptr) {
  return new detail::C_StateCallbackAdapter<T, MF, destroy>(obj);
}

template <typename I>
Context *create_async_context_callback(I &image_ctx, Context *on_finish) {
  // use async callback to acquire a clean lock context
  return new detail::C_AsyncCallback<
    typename std::decay<decltype(*image_ctx.op_work_queue)>::type>(
      image_ctx.op_work_queue, on_finish);
}

template <typename WQ>
Context *create_async_context_callback(WQ *work_queue, Context *on_finish) {
  // use async callback to acquire a clean lock context
  return new detail::C_AsyncCallback<WQ>(work_queue, on_finish);
}

// TODO: temporary until AioCompletion supports templated ImageCtx
inline ImageCtx *get_image_ctx(ImageCtx *image_ctx) {
  return image_ctx;
}

uint64_t get_rbd_default_features(CephContext* cct);

bool calc_sparse_extent(const bufferptr &bp,
                        size_t sparse_size,
                        uint64_t length,
                        size_t *write_offset,
                        size_t *write_length,
                        size_t *offset);

template <typename I>
inline ZTracer::Trace create_trace(const I &image_ctx, const char *trace_name,
				   const ZTracer::Trace &parent_trace) {
  if (parent_trace.valid()) {
    return ZTracer::Trace(trace_name, &image_ctx.trace_endpoint, &parent_trace);
  }
  return ZTracer::Trace();
}

bool is_metadata_config_override(const std::string& metadata_key,
                                 std::string* config_key);

int create_ioctx(librados::IoCtx& src_io_ctx, const std::string& pool_desc,
                 int64_t pool_id,
                 const std::optional<std::string>& pool_namespace,
                 librados::IoCtx* dst_io_ctx);

int snap_create_flags_api_to_internal(CephContext *cct, uint32_t api_flags,
                                      uint64_t *internal_flags);

} // namespace util
} // namespace librbd

#endif // CEPH_LIBRBD_UTILS_H
