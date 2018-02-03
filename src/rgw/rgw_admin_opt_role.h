#ifndef CEPH_RGW_ADMIN_OPT_ROLE_H
#define CEPH_RGW_ADMIN_OPT_ROLE_H


#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"

#include "common/ceph_json.h"
#include "rgw_role.h"
#include "rgw_admin_common.h"

int handle_opt_role_get(const std::string& role_name, const std::string& tenant, CephContext *context,
                        RGWRados *store, Formatter *formatter);

int handle_opt_role_create(const std::string& role_name, const std::string& assume_role_doc, const std::string& path,
                           const std::string& tenant, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_role_delete(const std::string& role_name, const std::string& tenant, CephContext *context, RGWRados *store);

int handle_opt_role_modify(const std::string& role_name, const std::string& assume_role_doc, const std::string& tenant,
                           CephContext *context, RGWRados *store);

int handle_opt_role_list(const std::string& path_prefix, const std::string& tenant, CephContext *context,
                         RGWRados *store, Formatter *formatter);

int handle_opt_role_policy_put(const std::string& role_name, const std::string& policy_name, const std::string& perm_policy_doc,
                               const std::string& tenant, CephContext *context, RGWRados *store);

int handle_opt_role_policy_list(const std::string& role_name, const std::string& tenant, CephContext *context,
                                RGWRados *store, Formatter *formatter);

int handle_opt_role_policy_get(const std::string& role_name, const std::string& policy_name, const std::string& tenant,
                               CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_role_policy_delete(const std::string& role_name, const std::string& policy_name, const std::string& tenant,
                                  CephContext *context, RGWRados *store);

#endif //CEPH_RGW_ADMIN_OPT_ROLE_H
