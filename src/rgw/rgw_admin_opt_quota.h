#ifndef CEPH_RGW_ADMIN_OPT_QUOTA_H
#define CEPH_RGW_ADMIN_OPT_QUOTA_H

#include "rgw_user.h"
#include "rgw_admin_common.h"

// This header and the corresponding source file contain handling of the following commads / groups of commands:
// Global quota, quota

int handle_opt_global_quota(std::string& realm_id, const std::string& realm_name, bool have_max_size, int64_t max_size,
                            bool have_max_objects, int64_t max_objects, RgwAdminCommand opt_cmd, const std::string& quota_scope,
                            RGWRados *store, Formatter *formatter);

int handle_opt_quota(const rgw_user& user_id, const std::string& bucket_name, const std::string& tenant, bool have_max_size,
                     int64_t max_size, bool have_max_objects, int64_t max_objects, RgwAdminCommand opt_cmd,
                     const std::string& quota_scope, RGWUser& user, RGWUserAdminOpState& user_op, RGWRados *store);


#endif //CEPH_RGW_ADMIN_OPT_QUOTA_H
