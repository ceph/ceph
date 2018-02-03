#ifndef CEPH_RGW_ADMIN_OPT_QUOTA_H
#define CEPH_RGW_ADMIN_OPT_QUOTA_H

#include "rgw_user.h"
#include "rgw_admin_common.h"

int handle_opt_global_quota(string& realm_id, const string& realm_name, bool have_max_size, int64_t max_size,
                            bool have_max_objects, int64_t max_objects, RgwAdminCommand opt_cmd, const string& quota_scope,
                            RGWRados *store, Formatter *formatter);

int handle_opt_quota(const rgw_user& user_id, const string& bucket_name, const string& tenant, bool have_max_size,
                     int64_t max_size, bool have_max_objects, int64_t max_objects, RgwAdminCommand opt_cmd,
                     const string& quota_scope, RGWUser& user, RGWUserAdminOpState& user_op, RGWRados *store);


#endif //CEPH_RGW_ADMIN_OPT_QUOTA_H
