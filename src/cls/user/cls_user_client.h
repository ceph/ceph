#ifndef CEPH_CLS_USER_CLIENT_H
#define CEPH_CLS_USER_CLIENT_H

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "cls_user_types.h"

/*
 * user objclass
 */

void cls_user_set_buckets(librados::ObjectWriteOperation& op, list<cls_user_bucket_entry>& entries);
void cls_user_remove_bucket(librados::ObjectWriteOperation& op,  const cls_user_bucket& bucket);
void cls_user_bucket_list(librados::ObjectReadOperation& op,
                       const string& in_marker, int max_entries,
                       list<cls_user_bucket_entry>& entries,
                       string *out_marker, bool *truncated);
void cls_user_get_header(librados::ObjectReadOperation& op, cls_user_header *header);

#endif
