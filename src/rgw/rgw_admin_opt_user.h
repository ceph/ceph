#ifndef CEPH_RGW_ADMIN_OPT_USER_H
#define CEPH_RGW_ADMIN_OPT_USER_H

#include "rgw_rados.h"
#include "rgw_user.h"

// This header and the corresponding source file contain handling of the following commads / groups of commands:
// User

int handle_opt_user_create(const std::string& subuser, RGWUserAdminOpState& user_op, RGWUser& user);

int handle_opt_user_stats(bool sync_stats, const std::string& bucket_name, const std::string& tenant,
                          rgw_user& user_id, RGWRados *store, Formatter *formatter);

#endif //CEPH_RGW_ADMIN_OPT_USER_H
