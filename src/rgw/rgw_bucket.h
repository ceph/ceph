// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>
#include <memory>
#include <variant>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_sal.h"

extern void init_bucket(rgw_bucket *b, const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id);

extern int rgw_bucket_parse_bucket_key(CephContext *cct, const std::string& key,
                                       rgw_bucket* bucket, int *shard_id);

extern std::string rgw_make_bucket_entry_name(const std::string& tenant_name,
                                              const std::string& bucket_name);

[[nodiscard]] int rgw_parse_url_bucket(const std::string& bucket,
                                       const std::string& auth_tenant,
                                       std::string &tenant_name,
                                       std::string &bucket_name);

extern int rgw_chown_bucket_and_objects(rgw::sal::Driver* driver,
					rgw::sal::Bucket* bucket,
					rgw::sal::User* new_user,
					const std::string& marker,
					std::string *err_msg,
					const DoutPrefixProvider *dpp,
					optional_yield y);
