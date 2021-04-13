// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "build_options.h"

std::vector<Option> get_global_options();
std::vector<Option> get_rgw_options();
std::vector<Option> get_rbd_options();
std::vector<Option> get_rbd_mirror_options();
std::vector<Option> get_immutable_object_cache_options();
std::vector<Option> get_mds_options();
std::vector<Option> get_mds_client_options();
std::vector<Option> get_cephfs_mirror_options();

std::vector<Option> build_options()
{
  std::vector<Option> result = get_global_options();

  auto ingest = [&result](std::vector<Option>&& options, const char* svc) {
    for (auto &o : options) {
      o.add_service(svc);
      result.push_back(std::move(o));
    }
  };

  ingest(get_rgw_options(), "rgw");
  ingest(get_rbd_options(), "rbd");
  ingest(get_rbd_mirror_options(), "rbd-mirror");
  ingest(get_immutable_object_cache_options(), "immutable-object-cache");
  ingest(get_mds_options(), "mds");
  ingest(get_mds_client_options(), "mds_client");
  ingest(get_cephfs_mirror_options(), "cephfs-mirror");

  return result;
}
