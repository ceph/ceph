#ifndef __OBJCLASS_H
#define __OBJCLASS_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void *cls_method_handle_t;
typedef int (*cls_method_call_t)(struct ceph_osd_op *op,
				 char **indata, int datalen,
				 char **outdata, int *outdatalen);

/* class log */
extern int cls_log(const char *format, ...);

/* class registration api */
extern int cls_register(const char *name, const char *method,
                        cls_method_call_t class_call, cls_method_handle_t handle);
extern int cls_unregister(cls_method_handle_t handle);

/* triggers */
#define OBJ_READ    0x1
#define OBJ_WRITE   0x2

typedef int cls_trigger_t;

extern int cls_link(cls_method_handle_t handle, int priority, cls_trigger_t trigger);
extern int cls_unlink(cls_method_handle_t handle);

#ifdef __cplusplus
}
#endif

#endif
