#ifndef CEPH_RGW_ADMIN_OPT_ROLE_H
#define CEPH_RGW_ADMIN_OPT_ROLE_H


#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"

#include "common/ceph_json.h"
#include "rgw_role.h"
#include "rgw_admin_common.h"

int handle_opt_role_get(const string& role_name, const string& tenant, CephContext *context,
                        RGWRados *store, Formatter *formatter);

int handle_opt_role_create(const string& role_name, const string& assume_role_doc, const string& path,
                           const string& tenant, CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_role_delete(const string& role_name, const string& tenant, CephContext *context, RGWRados *store);

int handle_opt_role_modify(const string& role_name, const string& assume_role_doc, const string& tenant,
                           CephContext *context, RGWRados *store);

int handle_opt_role_list(const string& path_prefix, const string& tenant, CephContext *context,
                         RGWRados *store, Formatter *formatter);

int handle_opt_role_policy_put(const string& role_name, const string& policy_name, const string& perm_policy_doc,
                               const string& tenant, CephContext *context, RGWRados *store);

int handle_opt_role_policy_list(const string& role_name, const string& tenant, CephContext *context,
                                RGWRados *store, Formatter *formatter);

int handle_opt_role_policy_get(const string& role_name, const string& policy_name, const string& tenant,
                               CephContext *context, RGWRados *store, Formatter *formatter);

int handle_opt_role_policy_delete(const string& role_name, const string& policy_name, const string& tenant,
                                  CephContext *context, RGWRados *store);

#endif //CEPH_RGW_ADMIN_OPT_ROLE_H
