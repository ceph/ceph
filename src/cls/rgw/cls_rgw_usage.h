// Private entry points for the rgw CLS module's usage domain.
#ifndef CEPH_CLS_RGW_USAGE_H
 #define CEPH_CLS_RGW_USAGE_H

#include "include/rados/objclass.h"

int rgw_user_usage_log_add(cls_method_context_t context,
                           ceph::buffer::list *input, ceph::buffer::list *output);
int rgw_user_usage_log_read(cls_method_context_t context,
                            ceph::buffer::list *input, ceph::buffer::list *output);
int rgw_user_usage_log_trim(cls_method_context_t context,
                            ceph::buffer::list *input, ceph::buffer::list *output);
int rgw_usage_log_clear(cls_method_context_t context,
                        ceph::buffer::list *input, ceph::buffer::list *output);

#endif // CEPH_CLS_RGW_USAGE_H
