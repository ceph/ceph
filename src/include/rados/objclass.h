// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OBJCLASS_OBJCLASS_PUBLIC_H
#define CEPH_OBJCLASS_OBJCLASS_PUBLIC_H

#ifdef __cplusplus

#include "buffer.h"

extern "C" {
#endif

#ifndef BUILDING_FOR_EMBEDDED
#define CLS_VER(maj,min) \
int __cls_ver__## maj ## _ ##min = 0; \
int __cls_ver_maj = maj; \
int __cls_ver_min = min;

#define CLS_NAME(name) \
int __cls_name__## name = 0; \
const char *__cls_name = #name;
#define CLS_INIT(name) \
void CEPH_CLS_API __cls_init()
#else
#define CLS_VER(maj,min)
#define CLS_NAME(name)
#define CLS_INIT(name) \
void CEPH_CLS_API name##_cls_init()
#endif

#define CLS_METHOD_RD       0x1 /// method executes read operations
#define CLS_METHOD_WR       0x2 /// method executes write operations
#define CLS_METHOD_PROMOTE  0x8 /// method cannot be proxied to base tier

#define CLS_LOG(level, fmt, ...)                                        \
  cls_log(level, "<cls> %s:%d: " fmt, __FILE__, __LINE__, ##__VA_ARGS__)
#define CLS_ERR(fmt, ...) CLS_LOG(0, fmt, ##__VA_ARGS__)

/**
 * Initialize a class.
 */
void __cls_init();

/**
 * @typdef cls_handle_t
 *
 * A handle for interacting with the object class.
 */
typedef void *cls_handle_t;

/**
 * @typedef cls_method_handle_t
 *
 * A handle for interacting with the method of the object class.
 */
typedef void *cls_method_handle_t;

/**
 * @typedef cls_method_context_t
 *
 * A context for the method of the object class.
 */
typedef void* cls_method_context_t;

/*class utils*/
extern int cls_log(int level, const char *format, ...)
  __attribute__((__format__(printf, 2, 3)));

/* class registration api */
extern int cls_register(const char *name, cls_handle_t *handle);

#ifdef __cplusplus
}

/**
 * @typedef cls_method_cxx_call_t
 *
 */
typedef int (*cls_method_cxx_call_t)(cls_method_context_t ctx,
    class ceph::buffer::list *inbl, class ceph::buffer::list *outbl);

/**
 * Register a method.
 *
 * @param hclass
 * @param method
 * @param flags
 * @param class_call
 * @param handle
 */
extern int cls_register_cxx_method(cls_handle_t hclass, const char *method, int flags,
                                   cls_method_cxx_call_t class_call, cls_method_handle_t *handle);

/**
 * Create an object.
 *
 * @param hctx
 * @param exclusive
 */
extern int cls_cxx_create(cls_method_context_t hctx, bool exclusive);

/**
 * Remove an object.
 *
 * @param hctx
 */
extern int cls_cxx_remove(cls_method_context_t hctx);

/**
 * Check on the status of an object.
 *
 * @param hctx
 * @param size
 * @param mtime
 */
extern int cls_cxx_stat(cls_method_context_t hctx, uint64_t *size, time_t *mtime);

/**
 * Read contents of an object.
 *
 * @param hctx
 * @param ofs
 * @param len
 * @param bl
 */
extern int cls_cxx_read(cls_method_context_t hctx, int ofs, int len, ceph::bufferlist *bl);

/**
 * Write to the object.
 *
 * @param hctx
 * @param ofs
 * @param len
 * @param bl
 */
extern int cls_cxx_write(cls_method_context_t hctx, int ofs, int len, ceph::bufferlist *bl);

/**
 * Get xattr of the object.
 *
 * @param hctx
 * @param name
 * @param outbl
 */
extern int cls_cxx_getxattr(cls_method_context_t hctx, const char *name,
                            ceph::bufferlist *outbl);

/**
 * Set xattr of the object.
 *
 * @param hctx
 * @param name
 * @param inbl
 */
extern int cls_cxx_setxattr(cls_method_context_t hctx, const char *name,
                            ceph::bufferlist *inbl);

/**
 * Get value corresponding to a key from the map.
 *
 * @param hctx
 * @param key
 * @param outbl
 */
extern int cls_cxx_map_get_val(cls_method_context_t hctx,
                               const std::string &key, ceph::bufferlist *outbl);

/**
 * Set value corresponding to a key in the map.
 *
 * @param hctx
 * @param key
 * @param inbl
 */
extern int cls_cxx_map_set_val(cls_method_context_t hctx,
                               const std::string &key, ceph::bufferlist *inbl);

#endif

#endif
