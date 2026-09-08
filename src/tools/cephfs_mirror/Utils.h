// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPHFS_MIRROR_UTILS_H
#define CEPHFS_MIRROR_UTILS_H

#include <string>

#include "Types.h"
#include "json_spirit/json_spirit.h"

namespace cephfs {
namespace mirror {

std::string snapshot_dir_path(CephContext *cct, const std::string &dir_root);
std::string snapshot_path(const std::string &snap_dir, const std::string &snap_name);
std::string snapshot_path(CephContext *cct, const std::string &dir_root,
                          const std::string &snap_name);

int connect(std::string_view client_name, std::string_view cluster_name,
            RadosRef *cluster, std::string_view mon_host={}, std::string_view cephx_key={},
            std::vector<const char *> args={});

int mount(RadosRef cluster, const Filesystem &filesystem, bool cross_check_fscid,
          MountRef *mount);

// Typed JSON field getters. Use a local mValue so callers can keep a live
// copy/reference of a parent object without get_json_value() overwriting it.
bool get_json_value(const json_spirit::mObject& obj,
                    const std::string& key,
                    json_spirit::mValue *val);
bool get_json_string(const json_spirit::mObject& obj,
                     const std::string& key,
                     std::string *val);
bool get_json_uint64(const json_spirit::mObject& obj,
                     const std::string& key,
                     uint64_t *val);
bool get_json_real(const json_spirit::mObject& obj,
                   const std::string& key,
                   double *val);

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_UTILS_H
