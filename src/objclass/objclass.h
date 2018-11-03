// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OBJCLASS_H
#define CEPH_OBJCLASS_H

#ifdef __cplusplus

#include "../include/types.h"
#include "msg/msg_types.h"
#include "common/hobject.h"
#include "common/ceph_time.h"
#include "include/rados/objclass.h"

struct obj_list_watch_response_t;

extern "C" {
#endif

#define CLS_METHOD_PUBLIC   0x4 /// unused

typedef void *cls_filter_handle_t;
typedef int (*cls_method_call_t)(cls_method_context_t ctx,
				 char *indata, int datalen,
				 char **outdata, int *outdatalen);
typedef struct {
	const char *name;
	const char *ver;
} cls_deps_t;

/* class utils */
extern void *cls_alloc(size_t size);
extern void cls_free(void *p);

extern int cls_read(cls_method_context_t hctx, int ofs, int len,
                                 char **outdata, int *outdatalen);
extern int cls_call(cls_method_context_t hctx, const char *cls, const char *method,
                                 char *indata, int datalen,
                                 char **outdata, int *outdatalen);
extern int cls_getxattr(cls_method_context_t hctx, const char *name,
                                 char **outdata, int *outdatalen);
extern int cls_setxattr(cls_method_context_t hctx, const char *name,
                                 const char *value, int val_len);
/** This will fill in the passed origin pointer with the origin of the
 * request which activated your class call. */
extern int cls_get_request_origin(cls_method_context_t hctx,
                                  entity_inst_t *origin);

/* class registration api */
extern int cls_unregister(cls_handle_t);

extern int cls_register_method(cls_handle_t hclass, const char *method, int flags,
                        cls_method_call_t class_call, cls_method_handle_t *handle);
extern int cls_unregister_method(cls_method_handle_t handle);
extern void cls_unregister_filter(cls_filter_handle_t handle);



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

class PGLSFilter {
  CephContext* cct;
protected:
  string xattr;
public:
  PGLSFilter();
  virtual ~PGLSFilter();
  virtual bool filter(const hobject_t &obj, bufferlist& xattr_data,
                      bufferlist& outdata) = 0;

  /**
   * Arguments passed from the RADOS client.  Implementations must
   * handle any encoding errors, and return an appropriate error code,
   * or 0 on valid input.
   */
  virtual int init(bufferlist::const_iterator &params) = 0;

  /**
   * xattr key, or empty string.  If non-empty, this xattr will be fetched
   * and the value passed into ::filter
   */
   virtual string& get_xattr() { return xattr; }

  /**
   * If true, objects without the named xattr (if xattr name is not empty)
   * will be rejected without calling ::filter
   */
  virtual bool reject_empty_xattr() { return true; }
};

// Classes expose a filter constructor that returns a subclass of PGLSFilter
typedef PGLSFilter* (*cls_cxx_filter_factory_t)();


extern int cls_register_cxx_filter(cls_handle_t hclass,
                                   const std::string &filter_name,
				   cls_cxx_filter_factory_t fn,
                                   cls_filter_handle_t *handle=NULL);

extern int cls_cxx_stat2(cls_method_context_t hctx, uint64_t *size, ceph::real_time *mtime);
extern int cls_cxx_read2(cls_method_context_t hctx, int ofs, int len,
                         bufferlist *bl, uint32_t op_flags);
extern int cls_cxx_write2(cls_method_context_t hctx, int ofs, int len,
                          bufferlist *bl, uint32_t op_flags);
extern int cls_cxx_write_full(cls_method_context_t hctx, bufferlist *bl);
extern int cls_cxx_getxattrs(cls_method_context_t hctx, map<string, bufferlist> *attrset);
extern int cls_cxx_replace(cls_method_context_t hctx, int ofs, int len, bufferlist *bl);
extern int cls_cxx_snap_revert(cls_method_context_t hctx, snapid_t snapid);
extern int cls_cxx_map_clear(cls_method_context_t hctx);
extern int cls_cxx_map_get_all_vals(cls_method_context_t hctx,
                                    std::map<string, bufferlist> *vals,
                                    bool *more);
extern int cls_cxx_map_get_keys(cls_method_context_t hctx,
                                const string &start_after,
                                uint64_t max_to_get,
                                std::set<string> *keys,
                                bool *more);
extern int cls_cxx_map_get_vals(cls_method_context_t hctx,
                                const string &start_after,
                                const string &filter_prefix,
                                uint64_t max_to_get,
                                std::map<string, bufferlist> *vals,
                                bool *more);
extern int cls_cxx_map_read_header(cls_method_context_t hctx, bufferlist *outbl);
extern int cls_cxx_map_set_vals(cls_method_context_t hctx,
                                const std::map<string, bufferlist> *map);
extern int cls_cxx_map_write_header(cls_method_context_t hctx, bufferlist *inbl);
extern int cls_cxx_map_remove_key(cls_method_context_t hctx, const string &key);
extern int cls_cxx_map_update(cls_method_context_t hctx, bufferlist *inbl);

extern int cls_cxx_list_watchers(cls_method_context_t hctx,
				 obj_list_watch_response_t *watchers);

/* utility functions */
extern int cls_gen_random_bytes(char *buf, int size);
extern int cls_gen_rand_base64(char *dest, int size); /* size should be the required string size + 1 */

/* environment */
extern uint64_t cls_current_version(cls_method_context_t hctx);
extern int cls_current_subop_num(cls_method_context_t hctx);
extern uint64_t cls_get_features(cls_method_context_t hctx);
extern uint64_t cls_get_client_features(cls_method_context_t hctx);
extern int8_t cls_get_required_osd_release(cls_method_context_t hctx);

/* helpers */
extern void cls_cxx_subop_version(cls_method_context_t hctx, string *s);

extern int cls_get_snapset_seq(cls_method_context_t hctx, uint64_t *snap_seq);

/* These are also defined in rados.h and librados.h. Keep them in sync! */
#define CEPH_OSD_TMAP_HDR 'h'
#define CEPH_OSD_TMAP_SET 's'
#define CEPH_OSD_TMAP_CREATE 'c'
#define CEPH_OSD_TMAP_RM 'r'

int cls_cxx_chunk_write_and_set(cls_method_context_t hctx, int ofs, int len,
                   bufferlist *write_inbl, uint32_t op_flags, bufferlist *set_inbl,
		   int set_len);
bool cls_has_chunk(cls_method_context_t hctx, string fp_oid);

#endif

#endif
