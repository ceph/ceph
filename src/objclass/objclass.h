// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OBJCLASS_H
#define CEPH_OBJCLASS_H

#ifdef __cplusplus

#include "../include/types.h"
#include "msg/msg_types.h"
#include "common/hobject.h"
#include "common/ceph_time.h"
#include "common/ceph_releases.h"
#include "include/rados/objclass.h"

struct obj_list_watch_response_t;
class PGLSFilter;
class object_info_t;

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
// Classes expose a filter constructor that returns a subclass of PGLSFilter
typedef PGLSFilter* (*cls_cxx_filter_factory_t)();


extern int cls_register_cxx_filter(cls_handle_t hclass,
                                   const std::string &filter_name,
				   cls_cxx_filter_factory_t fn,
                                   cls_filter_handle_t *handle=NULL);

extern int cls_cxx_stat2(cls_method_context_t hctx, uint64_t *size, ceph::real_time *mtime);
extern int cls_cxx_read2(cls_method_context_t hctx, int ofs, int len,
                         ceph::buffer::list *bl, uint32_t op_flags);
extern int cls_cxx_write2(cls_method_context_t hctx, int ofs, int len,
                          ceph::buffer::list *bl, uint32_t op_flags);
extern int cls_cxx_write_full(cls_method_context_t hctx, ceph::buffer::list *bl);
extern int cls_cxx_getxattrs(cls_method_context_t hctx, std::map<std::string,
			     ceph::buffer::list> *attrset);
extern int cls_cxx_replace(cls_method_context_t hctx, int ofs, int len,
			   ceph::buffer::list *bl);
extern int cls_cxx_truncate(cls_method_context_t hctx, int ofs);
extern int cls_cxx_write_zero(cls_method_context_t hctx, int ofs, int len);
extern int cls_cxx_snap_revert(cls_method_context_t hctx, snapid_t snapid);
extern int cls_cxx_map_clear(cls_method_context_t hctx);
extern int cls_cxx_map_get_all_vals(cls_method_context_t hctx,
                                    std::map<std::string, ceph::buffer::list> *vals,
                                    bool *more);
extern int cls_cxx_map_get_keys(cls_method_context_t hctx,
                                const std::string &start_after,
                                uint64_t max_to_get,
                                std::set<std::string> *keys,
                                bool *more);
extern int cls_cxx_map_get_vals(cls_method_context_t hctx,
                                const std::string& start_after,
                                const std::string& filter_prefix,
                                uint64_t max_to_get,
                                std::map<std::string, ceph::buffer::list> *vals,
                                bool *more);
extern int cls_cxx_map_get_val(cls_method_context_t hctx, const std::string &key,
                               bufferlist *outbl);
extern int cls_cxx_map_get_vals_by_keys(cls_method_context_t hctx,
                                        const std::set<std::string> &keys,
                                        std::map<std::string, bufferlist> *map);
extern int cls_cxx_map_read_header(cls_method_context_t hctx, ceph::buffer::list *outbl);
extern int cls_cxx_map_set_vals(cls_method_context_t hctx,
                                const std::map<std::string, ceph::buffer::list> *map);
extern int cls_cxx_map_write_header(cls_method_context_t hctx, ceph::buffer::list *inbl);
extern int cls_cxx_map_remove_key(cls_method_context_t hctx, const std::string &key);
/* remove keys in the range [key_begin, key_end) */
extern int cls_cxx_map_remove_range(cls_method_context_t hctx,
                                    const std::string& key_begin,
                                    const std::string& key_end);
extern int cls_cxx_map_update(cls_method_context_t hctx, ceph::buffer::list *inbl);

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
extern ceph_release_t cls_get_required_osd_release(cls_method_context_t hctx);
extern ceph_release_t cls_get_min_compatible_client(cls_method_context_t hctx);
extern const ConfigProxy& cls_get_config(cls_method_context_t hctx);
extern const object_info_t& cls_get_object_info(cls_method_context_t hctx);

/* helpers */
extern void cls_cxx_subop_version(cls_method_context_t hctx, std::string *s);

extern int cls_get_snapset_seq(cls_method_context_t hctx, uint64_t *snap_seq);

/* gather */
extern int cls_cxx_gather(cls_method_context_t hctx, const std::set<std::string> &src_objs, const std::string& pool,
			  const char *cls, const char *method, bufferlist& inbl) __attribute__ ((deprecated));

extern int cls_cxx_get_gathered_data(cls_method_context_t hctx, std::map<std::string, bufferlist> *results);

/* These are also defined in rados.h and librados.h. Keep them in sync! */
#define CEPH_OSD_TMAP_HDR 'h'
#define CEPH_OSD_TMAP_SET 's'
#define CEPH_OSD_TMAP_CREATE 'c'
#define CEPH_OSD_TMAP_RM 'r'

int cls_cxx_chunk_write_and_set(cls_method_context_t hctx, int ofs, int len,
                   ceph::buffer::list *write_inbl, uint32_t op_flags, ceph::buffer::list *set_inbl,
		   int set_len);
int cls_get_manifest_ref_count(cls_method_context_t hctx, std::string fp_oid);

extern uint64_t cls_get_osd_min_alloc_size(cls_method_context_t hctx);
extern uint64_t cls_get_pool_stripe_width(cls_method_context_t hctx);

#endif

#endif
