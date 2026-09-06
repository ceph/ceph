// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/oidc.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "include/str_list.h"
#include "rgw_account.h"
#include "rgw_arn.h"
#include "rgw_common.h"
#include "rgw_oidc_provider.h"
#include "rgw_sal.h"

using namespace rgw_admin;

namespace {

std::pair<int, std::string> resolve_oidc_tenant(const rgw_admin_oidc_options& opts)
{
  if (!opts.tenant.empty()) {
    std::cerr << "ERROR: --tenant is not supported for OIDC providers. "
              << "Use --account-id for account-scoped providers, "
              << "or omit for global providers." << std::endl;
    return {-EINVAL, {}};
  }
  if (!opts.account_id.empty()) {
    std::string err_msg;
    if (!rgw::account::validate_id(opts.account_id, &err_msg)) {
      std::cerr << "ERROR: invalid --account-id '" << opts.account_id << "': "
                << err_msg << std::endl;
      return {-EINVAL, {}};
    }
    return {0, opts.account_id};
  }
  return {0, std::string(global_oidc_id)};
}

} // anonymous namespace

int rgw_admin_oidc(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   ceph::Formatter* formatter,
                   const rgw_admin_oidc_options& opts)
{
  int ret = 0;

  switch (opts.command) {
  case OPT::OIDC_PROVIDER_CREATE:
    {
      if (opts.provider_url.empty()) {
        std::cerr << "ERROR: --provider-url is required" << std::endl;
        return EINVAL;
      }

      const auto [oidc_ret, oidc_tenant] = resolve_oidc_tenant(opts);
      if (oidc_ret < 0) {
        return -oidc_ret;
      }

      RGWOIDCProviderInfo info;
      info.provider_url = opts.provider_url;
      info.tenant = oidc_tenant;
      info.creation_date = format_creation_date(ceph::real_clock::now());

      if (!opts.client_ids_str.empty()) {
        get_str_vec(opts.client_ids_str, ",", info.client_ids);
      }
      if (!opts.thumbprints_str.empty()) {
        get_str_vec(opts.thumbprints_str, ",", info.thumbprints);
      }

      if (!is_global_oidc_provider(info)) {
        info.arn = rgw::ARN(url_remove_prefix(info.provider_url),
                            "oidc-provider/", info.tenant, true).to_string();
      }

      ret = driver->store_oidc_provider(dpp, null_yield, info,
                                        /*exclusive=*/true, nullptr);
      if (ret < 0) {
        std::cerr << "ERROR: failed to create OIDC provider: "
                  << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      encode_json("oidc_provider", info, formatter);
      formatter->flush(std::cout);
      return 0;
    }

  case OPT::OIDC_PROVIDER_MODIFY:
    {
      if (opts.provider_url.empty()) {
        std::cerr << "ERROR: --provider-url is required" << std::endl;
        return EINVAL;
      }

      if (opts.client_ids_str.empty() && opts.thumbprints_str.empty()) {
        std::cerr << "ERROR: at least one of --client-ids or --thumbprints "
                  << "is required" << std::endl;
        return EINVAL;
      }

      const auto [oidc_ret, oidc_tenant] = resolve_oidc_tenant(opts);
      if (oidc_ret < 0) {
        return -oidc_ret;
      }

      RGWOIDCProviderInfo info;
      RGWObjVersionTracker objv_tracker;
      ret = driver->load_oidc_provider(dpp, null_yield, oidc_tenant,
                                       url_remove_prefix(opts.provider_url),
                                       info, &objv_tracker);
      if (ret < 0) {
        std::cerr << "ERROR: failed to load OIDC provider: "
                  << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      if (!opts.client_ids_str.empty()) {
        info.client_ids.clear();
        get_str_vec(opts.client_ids_str, ",", info.client_ids);
      }
      if (!opts.thumbprints_str.empty()) {
        info.thumbprints.clear();
        get_str_vec(opts.thumbprints_str, ",", info.thumbprints);
      }

      constexpr bool exclusive = false;
      ret = driver->store_oidc_provider(dpp, null_yield, info,
                                        exclusive, &objv_tracker);
      if (ret < 0) {
        std::cerr << "ERROR: failed to modify OIDC provider: "
                  << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      encode_json("oidc_provider", info, formatter);
      formatter->flush(std::cout);
      return 0;
    }

  case OPT::OIDC_PROVIDER_GET:
    {
      if (opts.provider_url.empty()) {
        std::cerr << "ERROR: --provider-url is required" << std::endl;
        return EINVAL;
      }

      const auto [oidc_ret, oidc_tenant] = resolve_oidc_tenant(opts);
      if (oidc_ret < 0) {
        return -oidc_ret;
      }

      RGWOIDCProviderInfo info;
      ret = driver->load_oidc_provider(dpp, null_yield, oidc_tenant,
                                       url_remove_prefix(opts.provider_url),
                                       info, nullptr);
      if (ret < 0) {
        std::cerr << "ERROR: failed to get OIDC provider: "
                  << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      encode_json("oidc_provider", info, formatter);
      formatter->flush(std::cout);
      return 0;
    }

  case OPT::OIDC_PROVIDER_DELETE:
    {
      if (opts.provider_url.empty()) {
        std::cerr << "ERROR: --provider-url is required" << std::endl;
        return EINVAL;
      }

      const auto [oidc_ret, oidc_tenant] = resolve_oidc_tenant(opts);
      if (oidc_ret < 0) {
        return -oidc_ret;
      }

      ret = driver->delete_oidc_provider(dpp, null_yield, oidc_tenant,
                                         url_remove_prefix(opts.provider_url));
      if (ret < 0) {
        std::cerr << "ERROR: failed to delete OIDC provider: "
                  << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      std::cout << "OIDC provider successfully deleted" << std::endl;
      return 0;
    }

  case OPT::OIDC_PROVIDER_LIST:
    {
      const auto [oidc_ret, oidc_tenant] = resolve_oidc_tenant(opts);
      if (oidc_ret < 0) {
        return -oidc_ret;
      }

      std::vector<RGWOIDCProviderInfo> providers;
      ret = driver->get_oidc_providers(dpp, null_yield, oidc_tenant, providers);
      if (ret < 0) {
        std::cerr << "ERROR: failed to list OIDC providers: "
                  << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      formatter->open_array_section("oidc_providers");
      for (const auto& p : providers) {
        encode_json("oidc_provider", p, formatter);
      }
      formatter->close_section();
      formatter->flush(std::cout);
      return 0;
    }

  default:
    return EINVAL;
  }
}
