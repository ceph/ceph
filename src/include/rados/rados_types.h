#ifndef CEPH_RADOS_TYPES_H
#define CEPH_RADOS_TYPES_H

#include <stdint.h>

/**
 * @struct obj_watch_t
 * One item from list_watchers
 */
struct obj_watch_t {
  /// Address of the Watcher
  char addr[256];
  /// Watcher ID
  int64_t watcher_id;
  /// Cookie
  uint64_t cookie;
  /// Timeout in Seconds
  uint32_t timeout_seconds;
}; 

struct notify_ack_t {
  uint64_t notifier_id;
  uint64_t cookie;
  char *payload;
  uint64_t payload_len;
};

struct notify_timeout_t {
  uint64_t notifier_id;
  uint64_t cookie;
};

/**
 *
 * Pass as nspace argument to rados_ioctx_set_namespace()
 * before calling rados_nobjects_list_open() to return
 * all objects in all namespaces.
 */
#define	LIBRADOS_ALL_NSPACES "\001"

#endif
