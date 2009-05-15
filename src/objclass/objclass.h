#ifndef __OBJCLASS_H
#define __OBJCLASS_H

#ifdef __cplusplus
extern "C" {
#endif

typedef void *cls_handle_t;
typedef void *cls_method_handle_t;
typedef int (*cls_method_call_t)(char **indata, int datalen,
				 char **outdata, int *outdatalen);

/* class utils */
extern int cls_log(const char *format, ...);
extern void *cls_alloc(size_t size);
extern void cls_free(void *p);

/* class registration api */
extern int cls_register(const char *name, cls_handle_t *handle);
extern int cls_unregister(cls_handle_t);

extern int cls_register_method(cls_handle_t hclass, const char *method,
                        cls_method_call_t class_call, cls_method_handle_t *handle);
extern int cls_unregister_method(cls_method_handle_t handle);



/* triggers */
#define OBJ_READ    0x1
#define OBJ_WRITE   0x2

typedef int cls_trigger_t;

extern int cls_link(cls_method_handle_t handle, int priority, cls_trigger_t trigger);
extern int cls_unlink(cls_method_handle_t handle);


/* should be defined by the class implementation
   defined here inorder to get it compiled without C++ mangling */
extern void class_init(void);
extern void class_fini(void);

#ifdef __cplusplus
}
#endif

#endif
