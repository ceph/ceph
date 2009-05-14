#ifndef __OBJCLASS_H
#define __OBJCLASS_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void * ClsMethodHandle;


#define OBJ_READ    0x1
#define OBJ_WRITE   0x2

typedef int ClsTrigger;

/* class log */
extern int cls_log(const char *format, ...);

/* class registration api */
extern int cls_register(const char *name, const char *method,
                        int (*class_call)(struct ceph_osd_op *op, char **indata, int datalen, char **outdata, int *outdatalen),
                        ClsMethodHandle *handle);
extern int cls_unregister(ClsMethodHandle handle);

extern int cls_link(ClsMethodHandle handle, int priority, ClsTrigger trigger);
extern int cls_unlink(ClsMethodHandle handle);

#ifdef __cplusplus
}
#endif

#endif
