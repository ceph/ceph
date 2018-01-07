//
// Created by cache-nez on 08.01.18.
//

#ifndef CEPH_RGW_ADMIN_OPT_POLICY_H
#define CEPH_RGW_ADMIN_OPT_POLICY_H

#include "rgw_rest_conn.h"

#include "rgw_rest_conn.h"
#include "rgw_realm_watcher.h"

#include "common/ceph_json.h"
#include "rgw_role.h"


void show_perm_policy(const string& perm_policy, Formatter* formatter);

void show_policy_names(const std::vector<string>& policy_names, Formatter* formatter);

void show_role_info(RGWRole& role, Formatter* formatter);

void show_roles_info(const vector<RGWRole>& roles, Formatter* formatter);

#endif //CEPH_RGW_ADMIN_OPT_POLICY_H
