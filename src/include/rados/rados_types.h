#ifndef CEPH_RADOS_TYPES_H
#define CEPH_RADOS_TYPES_H

#include <stdint.h>

/**
 * @struct obj_watch_t
 * One item from list_watchers
 */
struct obj_watch_t {
  char addr[256];
  int64_t watcher_id;
  uint64_t cookie;
  uint32_t timeout_seconds;
}; 

/**
 * @defines
 *
 * Pass as nspace argument to rados_ioctx_set_namespace()
 * before calling rados_nobjects_list_open() to return
 * all objects in all namespaces.
 */
#define	LIBRADOS_ALL_NSPACES "\001"

#endif
