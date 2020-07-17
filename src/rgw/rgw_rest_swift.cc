// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/algorithm/string/predicate.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>

#include "include/ceph_assert.h"
#include "ceph_ver.h"

#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"

#include "rgw_rest_swift.h"
#include "rgw_acl_swift.h"
#include "rgw_cors_swift.h"
#include "rgw_formats.h"
#include "rgw_client_io.h"
#include "rgw_compression.h"

#include "rgw_auth.h"
#include "rgw_swift_auth.h"

#include "rgw_request.h"
#include "rgw_process.h"

#include "rgw_zone.h"

#include "services/svc_zone.h"

#include <array>
#include <string_view>
#include <sstream>
#include <memory>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

int RGWListBuckets_ObjStore_SWIFT::get_params()
{
  prefix = s->info.args.get("prefix");
  marker = s->info.args.get("marker");
  end_marker = s->info.args.get("end_marker");
  wants_reversed = s->info.args.exists("reverse");

  if (wants_reversed) {
    std::swap(marker, end_marker);
  }

  std::string limit_str = s->info.args.get("limit");
  if (!limit_str.empty()) {
    std::string err;
    long l = strict_strtol(limit_str.c_str(), 10, &err);
    if (!err.empty()) {
      return -EINVAL;
    }

    if (l > (long)limit_max || l < 0) {
      return -ERR_PRECONDITION_FAILED;
    }

    limit = (uint64_t)l;
  }

  if (s->cct->_conf->rgw_swift_need_stats) {
    bool stats, exists;
    int r = s->info.args.get_bool("stats", &stats, &exists);

    if (r < 0) {
      return r;
    }

    if (exists) {
      need_stats = stats;
    }
  } else {
    need_stats = false;
  }

  return 0;
}

static void dump_account_metadata(struct req_state * const s,
                                  const RGWUsageStats& global_stats,
                                  const std::map<std::string, RGWUsageStats> &policies_stats,
                                  /* const */map<string, bufferlist>& attrs,
                                  const RGWQuotaInfo& quota,
                                  const RGWAccessControlPolicy_SWIFTAcct &policy)
{
  /* Adding X-Timestamp to keep align with Swift API */
  dump_header(s, "X-Timestamp", ceph_clock_now());

  dump_header(s, "X-Account-Container-Count", global_stats.buckets_count);
  dump_header(s, "X-Account-Object-Count", global_stats.objects_count);
  dump_header(s, "X-Account-Bytes-Used", global_stats.bytes_used);
  dump_header(s, "X-Account-Bytes-Used-Actual", global_stats.bytes_used_rounded);

  for (const auto& kv : policies_stats) {
    const auto& policy_name = camelcase_dash_http_attr(kv.first);
    const auto& policy_stats = kv.second;

    dump_header_infixed(s, "X-Account-Storage-Policy-", policy_name,
                        "-Container-Count", policy_stats.buckets_count);
    dump_header_infixed(s, "X-Account-Storage-Policy-", policy_name,
                        "-Object-Count", policy_stats.objects_count);
    dump_header_infixed(s, "X-Account-Storage-Policy-", policy_name,
                        "-Bytes-Used", policy_stats.bytes_used);
    dump_header_infixed(s, "X-Account-Storage-Policy-", policy_name,
                        "-Bytes-Used-Actual", policy_stats.bytes_used_rounded);
  }

  /* Dump TempURL-related stuff */
  if (s->perm_mask == RGW_PERM_FULL_CONTROL) {
    auto iter = s->user->get_info().temp_url_keys.find(0);
    if (iter != std::end(s->user->get_info().temp_url_keys) && ! iter->second.empty()) {
      dump_header(s, "X-Account-Meta-Temp-Url-Key", iter->second);
    }

    iter = s->user->get_info().temp_url_keys.find(1);
    if (iter != std::end(s->user->get_info().temp_url_keys) && ! iter->second.empty()) {
      dump_header(s, "X-Account-Meta-Temp-Url-Key-2", iter->second);
    }
  }

  /* Dump quota headers. */
  if (quota.enabled) {
    if (quota.max_size >= 0) {
      dump_header(s, "X-Account-Meta-Quota-Bytes", quota.max_size);
    }

    /* Limit on the number of objects in a given account is a RadosGW's
     * extension. Swift's account quota WSGI filter doesn't support it. */
    if (quota.max_objects >= 0) {
      dump_header(s, "X-Account-Meta-Quota-Count", quota.max_objects);
    }
  }

  /* Dump user-defined metadata items and generic attrs. */
  const size_t PREFIX_LEN = sizeof(RGW_ATTR_META_PREFIX) - 1;
  map<string, bufferlist>::iterator iter;
  for (iter = attrs.lower_bound(RGW_ATTR_PREFIX); iter != attrs.end(); ++iter) {
    const char *name = iter->first.c_str();
    map<string, string>::const_iterator geniter = rgw_to_http_attrs.find(name);

    if (geniter != rgw_to_http_attrs.end()) {
      dump_header(s, geniter->second, iter->second);
    } else if (strncmp(name, RGW_ATTR_META_PREFIX, PREFIX_LEN) == 0) {
      dump_header_prefixed(s, "X-Account-Meta-",
                           camelcase_dash_http_attr(name + PREFIX_LEN),
                           iter->second);
    }
  }

  /* Dump account ACLs */
  auto account_acls = policy.to_str();
  if (account_acls) {
    dump_header(s, "X-Account-Access-Control", std::move(*account_acls));
  }
}

void RGWListBuckets_ObjStore_SWIFT::send_response_begin(bool has_buckets)
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
  } else if (!has_buckets && s->format == RGW_FORMAT_PLAIN) {
    op_ret = STATUS_NO_CONTENT;
    set_req_state_err(s, op_ret);
  }

  if (! s->cct->_conf->rgw_swift_enforce_content_length) {
    /* Adding account stats in the header to keep align with Swift API */
    dump_account_metadata(s,
            global_stats,
            policies_stats,
            attrs,
            user_quota,
            static_cast<RGWAccessControlPolicy_SWIFTAcct&>(*s->user_acl));
    dump_errno(s);
    dump_header(s, "Accept-Ranges", "bytes");
    end_header(s, NULL, NULL, NO_CONTENT_LENGTH, true);
  }

  if (! op_ret) {
    dump_start(s);
    s->formatter->open_array_section_with_attrs("account",
            FormatterAttrs("name", s->user->get_display_name().c_str(), NULL));

    sent_data = true;
  }
}

void RGWListBuckets_ObjStore_SWIFT::handle_listing_chunk(rgw::sal::RGWBucketList&& buckets)
{
  if (wants_reversed) {
    /* Just store in the reversal buffer. Its content will be handled later,
     * in send_response_end(). */
    reverse_buffer.emplace(std::begin(reverse_buffer), std::move(buckets));
  } else {
    return send_response_data(buckets);
  }
}

void RGWListBuckets_ObjStore_SWIFT::send_response_data(rgw::sal::RGWBucketList& buckets)
{
  if (! sent_data) {
    return;
  }

  /* Take care of the prefix parameter of Swift API. There is no business
   * in applying the filter earlier as we really need to go through all
   * entries regardless of it (the headers like X-Account-Container-Count
   * aren't affected by specifying prefix). */
  const auto& m = buckets.get_buckets();
  for (auto iter = m.lower_bound(prefix);
       iter != m.end() && boost::algorithm::starts_with(iter->first, prefix);
       ++iter) {
    dump_bucket_entry(*iter->second);
  }
}

void RGWListBuckets_ObjStore_SWIFT::dump_bucket_entry(const rgw::sal::RGWBucket& obj)
{
  s->formatter->open_object_section("container");
  s->formatter->dump_string("name", obj.get_name());

  if (need_stats) {
    s->formatter->dump_int("count", obj.get_count());
    s->formatter->dump_int("bytes", obj.get_size());
  }

  s->formatter->close_section();

  if (! s->cct->_conf->rgw_swift_enforce_content_length) {
    rgw_flush_formatter(s, s->formatter);
  }
}

void RGWListBuckets_ObjStore_SWIFT::send_response_data_reversed(rgw::sal::RGWBucketList& buckets)
{
  if (! sent_data) {
    return;
  }

  /* Take care of the prefix parameter of Swift API. There is no business
   * in applying the filter earlier as we really need to go through all
   * entries regardless of it (the headers like X-Account-Container-Count
   * aren't affected by specifying prefix). */
  auto& m = buckets.get_buckets();

  auto iter = m.rbegin();
  for (/* initialized above */;
       iter != m.rend() && !boost::algorithm::starts_with(iter->first, prefix);
       ++iter) {
    /* NOP */;
  }

  for (/* iter carried */;
       iter != m.rend() && boost::algorithm::starts_with(iter->first, prefix);
       ++iter) {
    dump_bucket_entry(*iter->second);
  }
}

void RGWListBuckets_ObjStore_SWIFT::send_response_end()
{
  if (wants_reversed) {
    for (auto& buckets : reverse_buffer) {
      send_response_data_reversed(buckets);
    }
  }

  if (sent_data) {
    s->formatter->close_section();
  }

  if (s->cct->_conf->rgw_swift_enforce_content_length) {
    /* Adding account stats in the header to keep align with Swift API */
    dump_account_metadata(s,
            global_stats,
            policies_stats,
            attrs,
            user_quota,
            static_cast<RGWAccessControlPolicy_SWIFTAcct&>(*s->user_acl));
    dump_errno(s);
    end_header(s, nullptr, nullptr, s->formatter->get_len(), true);
  }

  if (sent_data || s->cct->_conf->rgw_swift_enforce_content_length) {
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWListBucket_ObjStore_SWIFT::get_params()
{
  prefix = s->info.args.get("prefix");
  marker = s->info.args.get("marker");
  end_marker = s->info.args.get("end_marker");
  max_keys = s->info.args.get("limit");

  // non-standard
  s->info.args.get_bool("allow_unordered", &allow_unordered, false);

  delimiter = s->info.args.get("delimiter");

  op_ret = parse_max_keys();
  if (op_ret < 0) {
    return op_ret;
  }
  // S3 behavior is to silently cap the max-keys.
  // Swift behavior is to abort.
  if (max > default_max)
    return -ERR_PRECONDITION_FAILED;

  string path_args;
  if (s->info.args.exists("path")) { // should handle empty path
    path_args = s->info.args.get("path");
    if (!delimiter.empty() || !prefix.empty()) {
      return -EINVAL;
    }
    prefix = path_args;
    delimiter="/";

    path = prefix;
    if (path.size() && path[path.size() - 1] != '/')
      path.append("/");

    int len = prefix.size();
    int delim_size = delimiter.size();

    if (len >= delim_size) {
      if (prefix.substr(len - delim_size).compare(delimiter) != 0)
        prefix.append(delimiter);
    }
  }

  return 0;
}

static void dump_container_metadata(struct req_state *,
                                    const rgw::sal::RGWBucket*,
                                    const RGWQuotaInfo&,
                                    const RGWBucketWebsiteConf&);

void RGWListBucket_ObjStore_SWIFT::send_response()
{
  vector<rgw_bucket_dir_entry>::iterator iter = objs.begin();
  map<string, bool>::iterator pref_iter = common_prefixes.begin();

  dump_start(s);
  dump_container_metadata(s, s->bucket.get(), bucket_quota,
                          s->bucket->get_info().website_conf);

  s->formatter->open_array_section_with_attrs("container",
					      FormatterAttrs("name",
							     s->bucket->get_name().c_str(),
							     NULL));

  while (iter != objs.end() || pref_iter != common_prefixes.end()) {
    bool do_pref = false;
    bool do_objs = false;
    rgw_obj_key key;
    if (iter != objs.end()) {
      key = iter->key;
    }
    if (pref_iter == common_prefixes.end())
      do_objs = true;
    else if (iter == objs.end())
      do_pref = true;
    else if (!key.empty() && key.name.compare(pref_iter->first) == 0) {
      do_objs = true;
      ++pref_iter;
    } else if (!key.empty() && key.name.compare(pref_iter->first) <= 0)
      do_objs = true;
    else
      do_pref = true;

    if (do_objs && (allow_unordered || marker.empty() || marker < key)) {
      if (key.name.compare(path) == 0)
        goto next;

      s->formatter->open_object_section("object");
      s->formatter->dump_string("name", key.name);
      s->formatter->dump_string("hash", iter->meta.etag);
      s->formatter->dump_int("bytes", iter->meta.accounted_size);
      if (!iter->meta.user_data.empty())
        s->formatter->dump_string("user_custom_data", iter->meta.user_data);
      string single_content_type = iter->meta.content_type;
      if (iter->meta.content_type.size()) {
        // content type might hold multiple values, just dump the last one
        ssize_t pos = iter->meta.content_type.rfind(',');
        if (pos > 0) {
          ++pos;
          while (single_content_type[pos] == ' ')
            ++pos;
          single_content_type = single_content_type.substr(pos);
        }
        s->formatter->dump_string("content_type", single_content_type);
      }
      dump_time(s, "last_modified", &iter->meta.mtime);
      s->formatter->close_section();
    }

    if (do_pref &&  (marker.empty() || pref_iter->first.compare(marker.name) > 0)) {
      const string& name = pref_iter->first;
      if (name.compare(delimiter) == 0)
        goto next;

      s->formatter->open_object_section_with_attrs("subdir", FormatterAttrs("name", name.c_str(), NULL));

      /* swift is a bit inconsistent here */
      switch (s->format) {
        case RGW_FORMAT_XML:
          s->formatter->dump_string("name", name);
          break;
        default:
          s->formatter->dump_string("subdir", name);
      }
      s->formatter->close_section();
    }
next:
    if (do_objs)
      ++iter;
    else
      ++pref_iter;
  }

  s->formatter->close_section();

  int64_t content_len = 0;
  if (! op_ret) {
    content_len = s->formatter->get_len();
    if (content_len == 0) {
      op_ret = STATUS_NO_CONTENT;
    }
  } else if (op_ret > 0) {
    op_ret = 0;
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, NULL, content_len);
  if (op_ret < 0) {
    return;
  }

  rgw_flush_formatter_and_reset(s, s->formatter);
} // RGWListBucket_ObjStore_SWIFT::send_response

static void dump_container_metadata(struct req_state *s,
                                    const rgw::sal::RGWBucket* bucket,
                                    const RGWQuotaInfo& quota,
                                    const RGWBucketWebsiteConf& ws_conf)
{
  /* Adding X-Timestamp to keep align with Swift API */
  dump_header(s, "X-Timestamp", utime_t(s->bucket->get_info().creation_time));

  dump_header(s, "X-Container-Object-Count", bucket->get_count());
  dump_header(s, "X-Container-Bytes-Used", bucket->get_size());
  dump_header(s, "X-Container-Bytes-Used-Actual", bucket->get_size_rounded());

  if (rgw::sal::RGWObject::empty(s->object.get())) {
    auto swift_policy = \
      static_cast<RGWAccessControlPolicy_SWIFT*>(s->bucket_acl.get());
    std::string read_acl, write_acl;
    swift_policy->to_str(read_acl, write_acl);

    if (read_acl.size()) {
      dump_header(s, "X-Container-Read", read_acl);
    }
    if (write_acl.size()) {
      dump_header(s, "X-Container-Write", write_acl);
    }
    if (!s->bucket->get_placement_rule().name.empty()) {
      dump_header(s, "X-Storage-Policy", s->bucket->get_placement_rule().name);
    }
    dump_header(s, "X-Storage-Class", s->bucket->get_placement_rule().get_storage_class());

    /* Dump user-defined metadata items and generic attrs. */
    const size_t PREFIX_LEN = sizeof(RGW_ATTR_META_PREFIX) - 1;
    map<string, bufferlist>::iterator iter;
    for (iter = s->bucket_attrs.lower_bound(RGW_ATTR_PREFIX);
         iter != s->bucket_attrs.end();
         ++iter) {
      const char *name = iter->first.c_str();
      map<string, string>::const_iterator geniter = rgw_to_http_attrs.find(name);

      if (geniter != rgw_to_http_attrs.end()) {
        dump_header(s, geniter->second, iter->second);
      } else if (strncmp(name, RGW_ATTR_META_PREFIX, PREFIX_LEN) == 0) {
        dump_header_prefixed(s, "X-Container-Meta-",
                             camelcase_dash_http_attr(name + PREFIX_LEN),
                             iter->second);
      }
    }
  }

  /* Dump container versioning info. */
  if (! s->bucket->get_info().swift_ver_location.empty()) {
    dump_header(s, "X-Versions-Location",
                url_encode(s->bucket->get_info().swift_ver_location));
  }

  /* Dump quota headers. */
  if (quota.enabled) {
    if (quota.max_size >= 0) {
      dump_header(s, "X-Container-Meta-Quota-Bytes", quota.max_size);
    }

    if (quota.max_objects >= 0) {
      dump_header(s, "X-Container-Meta-Quota-Count", quota.max_objects);
    }
  }

  /* Dump Static Website headers. */
  if (! ws_conf.index_doc_suffix.empty()) {
    dump_header(s, "X-Container-Meta-Web-Index", ws_conf.index_doc_suffix);
  }

  if (! ws_conf.error_doc.empty()) {
    dump_header(s, "X-Container-Meta-Web-Error", ws_conf.error_doc);
  }

  if (! ws_conf.subdir_marker.empty()) {
    dump_header(s, "X-Container-Meta-Web-Directory-Type",
                ws_conf.subdir_marker);
  }

  if (! ws_conf.listing_css_doc.empty()) {
    dump_header(s, "X-Container-Meta-Web-Listings-CSS",
                ws_conf.listing_css_doc);
  }

  if (ws_conf.listing_enabled) {
    dump_header(s, "X-Container-Meta-Web-Listings", "true");
  }

  /* Dump bucket's modification time. Compliance with the Swift API really
   * needs that. */
  dump_last_modified(s, s->bucket_mtime);
}

void RGWStatAccount_ObjStore_SWIFT::execute()
{
  RGWStatAccount_ObjStore::execute();
  op_ret = store->ctl()->user->get_attrs_by_uid(s->user->get_id(), &attrs, s->yield);
}

void RGWStatAccount_ObjStore_SWIFT::send_response()
{
  if (op_ret >= 0) {
    op_ret = STATUS_NO_CONTENT;
    dump_account_metadata(s,
            global_stats,
            policies_stats,
            attrs,
            user_quota,
            static_cast<RGWAccessControlPolicy_SWIFTAcct&>(*s->user_acl));
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);

  end_header(s, NULL, NULL, 0,  true);

  dump_start(s);
}

void RGWStatBucket_ObjStore_SWIFT::send_response()
{
  if (op_ret >= 0) {
    op_ret = STATUS_NO_CONTENT;
    dump_container_metadata(s, bucket.get(), bucket_quota,
                            s->bucket->get_info().website_conf);
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);

  end_header(s, this, NULL, 0, true);
  dump_start(s);
}

static int get_swift_container_settings(req_state * const s,
                                        rgw::sal::RGWRadosStore * const store,
                                        RGWAccessControlPolicy * const policy,
                                        bool * const has_policy,
                                        uint32_t * rw_mask,
                                        RGWCORSConfiguration * const cors_config,
                                        bool * const has_cors)
{
  const char * const read_list = s->info.env->get("HTTP_X_CONTAINER_READ");
  const char * const write_list = s->info.env->get("HTTP_X_CONTAINER_WRITE");

  *has_policy = false;

  if (read_list || write_list) {
    RGWAccessControlPolicy_SWIFT swift_policy(s->cct);
    const auto r = swift_policy.create(store->ctl()->user,
                                       s->user->get_id(),
                                       s->user->get_display_name(),
                                       read_list,
                                       write_list,
                                       *rw_mask);
    if (r < 0) {
      return r;
    }

    *policy = swift_policy;
    *has_policy = true;
  }

  *has_cors = false;

  /*Check and update CORS configuration*/
  const char *allow_origins = s->info.env->get("HTTP_X_CONTAINER_META_ACCESS_CONTROL_ALLOW_ORIGIN");
  const char *allow_headers = s->info.env->get("HTTP_X_CONTAINER_META_ACCESS_CONTROL_ALLOW_HEADERS");
  const char *expose_headers = s->info.env->get("HTTP_X_CONTAINER_META_ACCESS_CONTROL_EXPOSE_HEADERS");
  const char *max_age = s->info.env->get("HTTP_X_CONTAINER_META_ACCESS_CONTROL_MAX_AGE");
  if (allow_origins) {
    RGWCORSConfiguration_SWIFT *swift_cors = new RGWCORSConfiguration_SWIFT;
    int r = swift_cors->create_update(allow_origins, allow_headers, expose_headers, max_age);
    if (r < 0) {
      dout(0) << "Error creating/updating the cors configuration" << dendl;
      delete swift_cors;
      return r;
    }
    *has_cors = true;
    *cors_config = *swift_cors;
    cors_config->dump();
    delete swift_cors;
  }

  return 0;
}

#define ACCT_REMOVE_ATTR_PREFIX     "HTTP_X_REMOVE_ACCOUNT_META_"
#define ACCT_PUT_ATTR_PREFIX        "HTTP_X_ACCOUNT_META_"
#define CONT_REMOVE_ATTR_PREFIX     "HTTP_X_REMOVE_CONTAINER_META_"
#define CONT_PUT_ATTR_PREFIX        "HTTP_X_CONTAINER_META_"

static void get_rmattrs_from_headers(const req_state * const s,
				     const char * const put_prefix,
				     const char * const del_prefix,
				     set<string>& rmattr_names)
{
  const size_t put_prefix_len = strlen(put_prefix);
  const size_t del_prefix_len = strlen(del_prefix);

  for (const auto& kv : s->info.env->get_map()) {
    size_t prefix_len = 0;
    const char * const p = kv.first.c_str();

    if (strncasecmp(p, del_prefix, del_prefix_len) == 0) {
      /* Explicitly requested removal. */
      prefix_len = del_prefix_len;
    } else if ((strncasecmp(p, put_prefix, put_prefix_len) == 0)
	       && kv.second.empty()) {
      /* Removal requested by putting an empty value. */
      prefix_len = put_prefix_len;
    }

    if (prefix_len > 0) {
      string name(RGW_ATTR_META_PREFIX);
      name.append(lowercase_dash_http_attr(p + prefix_len));
      rmattr_names.insert(name);
    }
  }
}

static int get_swift_versioning_settings(
  req_state * const s,
  boost::optional<std::string>& swift_ver_location)
{
  /* Removing the Swift's versions location has lower priority than setting
   * a new one. That's the reason why we're handling it first. */
  const std::string vlocdel =
    s->info.env->get("HTTP_X_REMOVE_VERSIONS_LOCATION", "");
  if (vlocdel.size()) {
    swift_ver_location = boost::in_place(std::string());
  }

  if (s->info.env->exists("HTTP_X_VERSIONS_LOCATION")) {
    /* If the Swift's versioning is globally disabled but someone wants to
     * enable it for a given container, new version of Swift will generate
     * the precondition failed error. */
    if (! s->cct->_conf->rgw_swift_versioning_enabled) {
      return -ERR_PRECONDITION_FAILED;
    }

    swift_ver_location = s->info.env->get("HTTP_X_VERSIONS_LOCATION", "");
  }

  return 0;
}

int RGWCreateBucket_ObjStore_SWIFT::get_params()
{
  bool has_policy;
  uint32_t policy_rw_mask = 0;

  int r = get_swift_container_settings(s, store, &policy, &has_policy,
				       &policy_rw_mask, &cors_config, &has_cors);
  if (r < 0) {
    return r;
  }

  if (!has_policy) {
    policy.create_default(s->user->get_id(), s->user->get_display_name());
  }

  location_constraint = store->svc()->zone->get_zonegroup().api_name;
  get_rmattrs_from_headers(s, CONT_PUT_ATTR_PREFIX,
                           CONT_REMOVE_ATTR_PREFIX, rmattr_names);
  placement_rule.init(s->info.env->get("HTTP_X_STORAGE_POLICY", ""), s->info.storage_class);

  return get_swift_versioning_settings(s, swift_ver_location);
}

static inline int handle_metadata_errors(req_state* const s, const int op_ret)
{
  if (op_ret == -EFBIG) {
    /* Handle the custom error message of exceeding maximum custom attribute
     * (stored as xattr) size. */
    const auto error_message = boost::str(
      boost::format("Metadata value longer than %lld")
        % s->cct->_conf.get_val<Option::size_t>("rgw_max_attr_size"));
    set_req_state_err(s, EINVAL, error_message);
    return -EINVAL;
  } else if (op_ret == -E2BIG) {
    const auto error_message = boost::str(
      boost::format("Too many metadata items; max %lld")
        % s->cct->_conf.get_val<uint64_t>("rgw_max_attrs_num_in_req"));
    set_req_state_err(s, EINVAL, error_message);
    return -EINVAL;
  }

  return op_ret;
}

void RGWCreateBucket_ObjStore_SWIFT::send_response()
{
  const auto meta_ret = handle_metadata_errors(s, op_ret);
  if (meta_ret != op_ret) {
    op_ret = meta_ret;
  } else {
    if (!op_ret) {
      op_ret = STATUS_CREATED;
    } else if (op_ret == -ERR_BUCKET_EXISTS) {
      op_ret = STATUS_ACCEPTED;
    }
    set_req_state_err(s, op_ret);
  }

  dump_errno(s);
  /* Propose ending HTTP header with 0 Content-Length header. */
  end_header(s, NULL, NULL, 0);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWDeleteBucket_ObjStore_SWIFT::send_response()
{
  int r = op_ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s, this, NULL, 0);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

static int get_delete_at_param(req_state *s, boost::optional<real_time> &delete_at)
{
  /* Handle Swift object expiration. */
  real_time delat_proposal;
  string x_delete = s->info.env->get("HTTP_X_DELETE_AFTER", "");

  if (x_delete.empty()) {
    x_delete = s->info.env->get("HTTP_X_DELETE_AT", "");
  } else {
    /* X-Delete-After HTTP is present. It means we need add its value
     * to the current time. */
    delat_proposal = real_clock::now();
  }

  if (x_delete.empty()) {
    delete_at = boost::none;
    if (s->info.env->exists("HTTP_X_REMOVE_DELETE_AT")) {
      delete_at = boost::in_place(real_time());
    }
    return 0;
  }
  string err;
  long ts = strict_strtoll(x_delete.c_str(), 10, &err);

  if (!err.empty()) {
    return -EINVAL;
  }

  delat_proposal += make_timespan(ts);
  if (delat_proposal < real_clock::now()) {
    return -EINVAL;
  }

  delete_at = delat_proposal;

  return 0;
}

int RGWPutObj_ObjStore_SWIFT::verify_permission()
{
  op_ret = RGWPutObj_ObjStore::verify_permission();

  /* We have to differentiate error codes depending on whether user is
   * anonymous (401 Unauthorized) or he doesn't have necessary permissions
   * (403 Forbidden). */
  if (s->auth.identity->is_anonymous() && op_ret == -EACCES) {
    return -EPERM;
  } else {
    return op_ret;
  }
}

int RGWPutObj_ObjStore_SWIFT::update_slo_segment_size(rgw_slo_entry& entry) {

  int r = 0;
  const string& path = entry.path;

  /* If the path starts with slashes, strip them all. */
  const size_t pos_init = path.find_first_not_of('/');

  if (pos_init == string::npos) {
    return -EINVAL;
  }

  const size_t pos_sep = path.find('/', pos_init);
  if (pos_sep == string::npos) {
    return -EINVAL;
  }

  string bucket_name = path.substr(pos_init, pos_sep - pos_init);
  string obj_name = path.substr(pos_sep + 1);

  rgw_bucket bucket;

  if (bucket_name.compare(s->bucket->get_name()) != 0) {
    RGWBucketInfo bucket_info;
    map<string, bufferlist> bucket_attrs;
    r = store->getRados()->get_bucket_info(store->svc(), s->user->get_id().tenant,
			       bucket_name, bucket_info, nullptr,
			       s->yield, &bucket_attrs);
    if (r < 0) {
      ldpp_dout(this, 0) << "could not get bucket info for bucket="
			 << bucket_name << dendl;
      return r;
    }
    bucket = bucket_info.bucket;
  } else {
    bucket = s->bucket->get_bi();
  }

  /* fetch the stored size of the seg (or error if not valid) */
  rgw_obj_key slo_key(obj_name);
  rgw_obj slo_seg(bucket, slo_key);

  /* no prefetch */
  RGWObjectCtx obj_ctx(store);
  obj_ctx.set_atomic(slo_seg);

  RGWRados::Object op_target(store->getRados(), s->bucket->get_info(), obj_ctx, slo_seg);
  RGWRados::Object::Read read_op(&op_target);

  bool compressed;
  RGWCompressionInfo cs_info;
  map<std::string, buffer::list> attrs;
  uint64_t size_bytes{0};

  read_op.params.attrs = &attrs;
  read_op.params.obj_size = &size_bytes;

  r = read_op.prepare(s->yield);
  if (r < 0) {
    return r;
  }

  r = rgw_compression_info_from_attrset(attrs, compressed, cs_info);
  if (r < 0) {
    return -EIO;
  }

  if (compressed) {
    size_bytes = cs_info.orig_size;
  }

  /* "When the PUT operation sees the multipart-manifest=put query
   * parameter, it reads the request body and verifies that each
   * segment object exists and that the sizes and ETags match. If
   * there is a mismatch, the PUT operation fails."
   */
  if (entry.size_bytes &&
      (entry.size_bytes != size_bytes)) {
    return -EINVAL;
  }

  entry.size_bytes = size_bytes;

  return 0;
} /* RGWPutObj_ObjStore_SWIFT::update_slo_segment_sizes */

int RGWPutObj_ObjStore_SWIFT::get_params()
{
  if (s->has_bad_meta) {
    return -EINVAL;
  }

  if (!s->length) {
    const char *encoding = s->info.env->get("HTTP_TRANSFER_ENCODING");
    if (!encoding || strcmp(encoding, "chunked") != 0) {
      ldpp_dout(this, 20) << "neither length nor chunked encoding" << dendl;
      return -ERR_LENGTH_REQUIRED;
    }

    chunked_upload = true;
  }

  supplied_etag = s->info.env->get("HTTP_ETAG");

  if (!s->generic_attrs.count(RGW_ATTR_CONTENT_TYPE)) {
    ldpp_dout(this, 5) << "content type wasn't provided, trying to guess" << dendl;
    const char *suffix = strrchr(s->object->get_name().c_str(), '.');
    if (suffix) {
      suffix++;
      if (*suffix) {
	string suffix_str(suffix);
	const char *mime = rgw_find_mime_by_ext(suffix_str);
	if (mime) {
	  s->generic_attrs[RGW_ATTR_CONTENT_TYPE] = mime;
	}
      }
    }
  }

  policy.create_default(s->user->get_id(), s->user->get_display_name());

  int r = get_delete_at_param(s, delete_at);
  if (r < 0) {
    ldpp_dout(this, 5) << "ERROR: failed to get Delete-At param" << dendl;
    return r;
  }

  if (!s->cct->_conf->rgw_swift_custom_header.empty()) {
    string custom_header = s->cct->_conf->rgw_swift_custom_header;
    if (s->info.env->exists(custom_header.c_str())) {
      user_data = s->info.env->get(custom_header.c_str());
    }
  }

  dlo_manifest = s->info.env->get("HTTP_X_OBJECT_MANIFEST");
  bool exists;
  string multipart_manifest = s->info.args.get("multipart-manifest", &exists);
  if (exists) {
    if (multipart_manifest != "put") {
      ldpp_dout(this, 5) << "invalid multipart-manifest http param: " << multipart_manifest << dendl;
      return -EINVAL;
    }

#define MAX_SLO_ENTRY_SIZE (1024 + 128) // 1024 - max obj name, 128 - enough extra for other info
    uint64_t max_len = s->cct->_conf->rgw_max_slo_entries * MAX_SLO_ENTRY_SIZE;
    
    slo_info = new RGWSLOInfo;
    
    int r = 0;
    std::tie(r, slo_info->raw_data) = rgw_rest_get_json_input_keep_data(s->cct, s, slo_info->entries, max_len);
    if (r < 0) {
      ldpp_dout(this, 5) << "failed to read input for slo r=" << r << dendl;
      return r;
    }

    if ((int64_t)slo_info->entries.size() > s->cct->_conf->rgw_max_slo_entries) {
      ldpp_dout(this, 5) << "too many entries in slo request: " << slo_info->entries.size() << dendl;
      return -EINVAL;
    }

    MD5 etag_sum;
    uint64_t total_size = 0;
    for (auto& entry : slo_info->entries) {
      etag_sum.Update((const unsigned char *)entry.etag.c_str(),
                      entry.etag.length());

      /* if size_bytes == 0, it should be replaced with the
       * real segment size (which could be 0);  this follows from the
       * fact that Swift requires all segments to exist, but permits
       * the size_bytes element to be omitted from the SLO manifest, see
       * https://docs.openstack.org/swift/latest/api/large_objects.html
       */
      r = update_slo_segment_size(entry);
      if (r < 0) {
	return r;
      }

      total_size += entry.size_bytes;

      ldpp_dout(this, 20) << "slo_part: " << entry.path
                        << " size=" << entry.size_bytes
                        << " etag=" << entry.etag
                        << dendl;
    }
    complete_etag(etag_sum, &lo_etag);
    slo_info->total_size = total_size;

    ofs = slo_info->raw_data.length();
  }

  return RGWPutObj_ObjStore::get_params();
}

void RGWPutObj_ObjStore_SWIFT::send_response()
{
  const auto meta_ret = handle_metadata_errors(s, op_ret);
  if (meta_ret) {
    op_ret = meta_ret;
  } else {
    if (!op_ret) {
      op_ret = STATUS_CREATED;
    }
    set_req_state_err(s, op_ret);
  }

  if (! lo_etag.empty()) {
    /* Static Large Object of Swift API has two etags represented by
     * following members:
     *  - etag - for the manifest itself (it will be stored in xattrs),
     *  - lo_etag - for the content composited from SLO's segments.
     *    The value is calculated basing on segments' etags.
     * In response for PUT request we have to expose the second one.
     * The first one may be obtained by GET with "multipart-manifest=get"
     * in query string on a given SLO. */
    dump_etag(s, lo_etag, true /* quoted */);
  } else {
    dump_etag(s, etag);
  }

  dump_last_modified(s, mtime);
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

static int get_swift_account_settings(req_state * const s,
                                      rgw::sal::RGWRadosStore * const store,
                                      RGWAccessControlPolicy_SWIFTAcct * const policy,
                                      bool * const has_policy)
{
  *has_policy = false;

  const char * const acl_attr = s->info.env->get("HTTP_X_ACCOUNT_ACCESS_CONTROL");
  if (acl_attr) {
    RGWAccessControlPolicy_SWIFTAcct swift_acct_policy(s->cct);
    const bool r = swift_acct_policy.create(store->ctl()->user,
                                     s->user->get_id(),
                                     s->user->get_display_name(),
                                     string(acl_attr));
    if (r != true) {
      return -EINVAL;
    }

    *policy = swift_acct_policy;
    *has_policy = true;
  }

  return 0;
}

int RGWPutMetadataAccount_ObjStore_SWIFT::get_params()
{
  if (s->has_bad_meta) {
    return -EINVAL;
  }

  int ret = get_swift_account_settings(s,
                                       store,
                                       // FIXME: we need to carry unique_ptr in generic class
                                       // and allocate appropriate ACL class in the ctor
                                       static_cast<RGWAccessControlPolicy_SWIFTAcct *>(&policy),
                                       &has_policy);
  if (ret < 0) {
    return ret;
  }

  get_rmattrs_from_headers(s, ACCT_PUT_ATTR_PREFIX, ACCT_REMOVE_ATTR_PREFIX,
			   rmattr_names);
  return 0;
}

void RGWPutMetadataAccount_ObjStore_SWIFT::send_response()
{
  const auto meta_ret = handle_metadata_errors(s, op_ret);
  if (meta_ret != op_ret) {
    op_ret = meta_ret;
  } else {
    if (!op_ret) {
      op_ret = STATUS_NO_CONTENT;
    }
    set_req_state_err(s, op_ret);
  }

  dump_errno(s);
  end_header(s, this);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWPutMetadataBucket_ObjStore_SWIFT::get_params()
{
  if (s->has_bad_meta) {
    return -EINVAL;
  }

  int r = get_swift_container_settings(s, store, &policy, &has_policy,
				       &policy_rw_mask, &cors_config, &has_cors);
  if (r < 0) {
    return r;
  }

  get_rmattrs_from_headers(s, CONT_PUT_ATTR_PREFIX, CONT_REMOVE_ATTR_PREFIX,
			   rmattr_names);
  placement_rule.init(s->info.env->get("HTTP_X_STORAGE_POLICY", ""), s->info.storage_class);

  return get_swift_versioning_settings(s, swift_ver_location);
}

void RGWPutMetadataBucket_ObjStore_SWIFT::send_response()
{
  const auto meta_ret = handle_metadata_errors(s, op_ret);
  if (meta_ret != op_ret) {
    op_ret = meta_ret;
  } else {
    if (!op_ret && (op_ret != -EINVAL)) {
      op_ret = STATUS_NO_CONTENT;
    }
    set_req_state_err(s, op_ret);
  }

  dump_errno(s);
  end_header(s, this);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWPutMetadataObject_ObjStore_SWIFT::get_params()
{
  if (s->has_bad_meta) {
    return -EINVAL;
  }

  /* Handle Swift object expiration. */
  int r = get_delete_at_param(s, delete_at);
  if (r < 0) {
    ldpp_dout(this, 5) << "ERROR: failed to get Delete-At param" << dendl;
    return r;
  }

  dlo_manifest = s->info.env->get("HTTP_X_OBJECT_MANIFEST");

  return 0;
}

void RGWPutMetadataObject_ObjStore_SWIFT::send_response()
{
  const auto meta_ret = handle_metadata_errors(s, op_ret);
  if (meta_ret != op_ret) {
    op_ret = meta_ret;
  } else {
    if (!op_ret) {
      op_ret = STATUS_ACCEPTED;
    }
    set_req_state_err(s, op_ret);
  }

  if (!s->is_err()) {
    dump_content_length(s, 0);
  }

  dump_errno(s);
  end_header(s, this);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

static void bulkdelete_respond(const unsigned num_deleted,
                               const unsigned int num_unfound,
                               const std::list<RGWBulkDelete::fail_desc_t>& failures,
                               const int prot_flags,                  /* in  */
                               ceph::Formatter& formatter)            /* out */
{
  formatter.open_object_section("delete");

  string resp_status;
  string resp_body;

  if (!failures.empty()) {
    int reason = ERR_INVALID_REQUEST;
    for (const auto fail_desc : failures) {
      if (-ENOENT != fail_desc.err && -EACCES != fail_desc.err) {
        reason = fail_desc.err;
      }
    }
    rgw_err err;
    set_req_state_err(err, reason, prot_flags);
    dump_errno(err, resp_status);
  } else if (0 == num_deleted && 0 == num_unfound) {
    /* 400 Bad Request */
    dump_errno(400, resp_status);
    resp_body = "Invalid bulk delete.";
  } else {
    /* 200 OK */
    dump_errno(200, resp_status);
  }

  encode_json("Number Deleted", num_deleted, &formatter);
  encode_json("Number Not Found", num_unfound, &formatter);
  encode_json("Response Body", resp_body, &formatter);
  encode_json("Response Status", resp_status, &formatter);

  formatter.open_array_section("Errors");
  for (const auto fail_desc : failures) {
    formatter.open_array_section("object");

    stringstream ss_name;
    ss_name << fail_desc.path;
    encode_json("Name", ss_name.str(), &formatter);

    rgw_err err;
    set_req_state_err(err, fail_desc.err, prot_flags);
    string status;
    dump_errno(err, status);
    encode_json("Status", status, &formatter);
    formatter.close_section();
  }
  formatter.close_section();

  formatter.close_section();
}

int RGWDeleteObj_ObjStore_SWIFT::verify_permission()
{
  op_ret = RGWDeleteObj_ObjStore::verify_permission();

  /* We have to differentiate error codes depending on whether user is
   * anonymous (401 Unauthorized) or he doesn't have necessary permissions
   * (403 Forbidden). */
  if (s->auth.identity->is_anonymous() && op_ret == -EACCES) {
    return -EPERM;
  } else {
    return op_ret;
  }
}

int RGWDeleteObj_ObjStore_SWIFT::get_params()
{
  const string& mm = s->info.args.get("multipart-manifest");
  multipart_delete = (mm.compare("delete") == 0);

  return RGWDeleteObj_ObjStore::get_params();
}

void RGWDeleteObj_ObjStore_SWIFT::send_response()
{
  int r = op_ret;

  if (multipart_delete) {
    r = 0;
  } else if(!r) {
    r = STATUS_NO_CONTENT;
  }

  set_req_state_err(s, r);
  dump_errno(s);

  if (multipart_delete) {
    end_header(s, this /* RGWOp */, nullptr /* contype */,
               CHUNKED_TRANSFER_ENCODING);

    if (deleter) {
      bulkdelete_respond(deleter->get_num_deleted(),
                         deleter->get_num_unfound(),
                         deleter->get_failures(),
                         s->prot_flags,
                         *s->formatter);
    } else if (-ENOENT == op_ret) {
      bulkdelete_respond(0, 1, {}, s->prot_flags, *s->formatter);
    } else {
      RGWBulkDelete::acct_path_t path;
      path.bucket_name = s->bucket_name;
      path.obj_key = s->object->get_key();

      RGWBulkDelete::fail_desc_t fail_desc;
      fail_desc.err = op_ret;
      fail_desc.path = path;

      bulkdelete_respond(0, 0, { fail_desc }, s->prot_flags, *s->formatter);
    }
  } else {
    end_header(s, this);
  }

  rgw_flush_formatter_and_reset(s, s->formatter);

}

static void get_contype_from_attrs(map<string, bufferlist>& attrs,
				   string& content_type)
{
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_CONTENT_TYPE);
  if (iter != attrs.end()) {
    content_type = rgw_bl_str(iter->second);
  }
}

static void dump_object_metadata(const DoutPrefixProvider* dpp, struct req_state * const s,
				 const map<string, bufferlist>& attrs)
{
  map<string, string> response_attrs;

  for (auto kv : attrs) {
    const char * name = kv.first.c_str();
    const auto aiter = rgw_to_http_attrs.find(name);

    if (aiter != std::end(rgw_to_http_attrs)) {
      response_attrs[aiter->second] = rgw_bl_str(kv.second);
    } else if (strcmp(name, RGW_ATTR_SLO_UINDICATOR) == 0) {
      // this attr has an extra length prefix from encode() in prior versions
      dump_header(s, "X-Object-Meta-Static-Large-Object", "True");
    } else if (strncmp(name, RGW_ATTR_META_PREFIX,
		       sizeof(RGW_ATTR_META_PREFIX)-1) == 0) {
      name += sizeof(RGW_ATTR_META_PREFIX) - 1;
      dump_header_prefixed(s, "X-Object-Meta-",
                           camelcase_dash_http_attr(name), kv.second);
    }
  }

  /* Handle override and fallback for Content-Disposition HTTP header.
   * At the moment this will be used only by TempURL of the Swift API. */
  const auto cditer = rgw_to_http_attrs.find(RGW_ATTR_CONTENT_DISP);
  if (cditer != std::end(rgw_to_http_attrs)) {
    const auto& name = cditer->second;

    if (!s->content_disp.override.empty()) {
      response_attrs[name] = s->content_disp.override;
    } else if (!s->content_disp.fallback.empty()
        && response_attrs.find(name) == std::end(response_attrs)) {
      response_attrs[name] = s->content_disp.fallback;
    }
  }

  for (const auto kv : response_attrs) {
    dump_header(s, kv.first, kv.second);
  }

  const auto iter = attrs.find(RGW_ATTR_DELETE_AT);
  if (iter != std::end(attrs)) {
    utime_t delete_at;
    try {
      decode(delete_at, iter->second);
      if (!delete_at.is_zero()) {
        dump_header(s, "X-Delete-At", delete_at.sec());
      }
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: cannot decode object's " RGW_ATTR_DELETE_AT
                          " attr, ignoring"
                       << dendl;
    }
  }
}

int RGWCopyObj_ObjStore_SWIFT::init_dest_policy()
{
  dest_policy.create_default(s->user->get_id(), s->user->get_display_name());

  return 0;
}

int RGWCopyObj_ObjStore_SWIFT::get_params()
{
  if_mod = s->info.env->get("HTTP_IF_MODIFIED_SINCE");
  if_unmod = s->info.env->get("HTTP_IF_UNMODIFIED_SINCE");
  if_match = s->info.env->get("HTTP_COPY_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_COPY_IF_NONE_MATCH");

  src_tenant_name = s->src_tenant_name;
  src_bucket_name = s->src_bucket_name;
  src_object = s->src_object->clone();
  dest_tenant_name = s->bucket_tenant;
  dest_bucket_name = s->bucket_name;
  dest_obj_name = s->object->get_name();

  const char * const fresh_meta = s->info.env->get("HTTP_X_FRESH_METADATA");
  if (fresh_meta && strcasecmp(fresh_meta, "TRUE") == 0) {
    attrs_mod = RGWRados::ATTRSMOD_REPLACE;
  } else {
    attrs_mod = RGWRados::ATTRSMOD_MERGE;
  }

  int r = get_delete_at_param(s, delete_at);
  if (r < 0) {
    ldpp_dout(this, 5) << "ERROR: failed to get Delete-At param" << dendl;
    return r;
  }

  return 0;
}

void RGWCopyObj_ObjStore_SWIFT::send_partial_response(off_t ofs)
{
  if (! sent_header) {
    if (! op_ret)
      op_ret = STATUS_CREATED;
    set_req_state_err(s, op_ret);
    dump_errno(s);
    end_header(s, this);

    /* Send progress information. Note that this diverge from the original swift
     * spec. We do this in order to keep connection alive.
     */
    if (op_ret == 0) {
      s->formatter->open_array_section("progress");
    }
    sent_header = true;
  } else {
    s->formatter->dump_int("ofs", (uint64_t)ofs);
  }
  rgw_flush_formatter(s, s->formatter);
}

void RGWCopyObj_ObjStore_SWIFT::dump_copy_info()
{
  /* Dump X-Copied-From. */
  dump_header(s, "X-Copied-From", url_encode(src_bucket->get_name()) +
              "/" + url_encode(src_object->get_name()));

  /* Dump X-Copied-From-Account. */
  /* XXX tenant */
  dump_header(s, "X-Copied-From-Account", url_encode(s->user->get_id().id));

  /* Dump X-Copied-From-Last-Modified. */
  dump_time_header(s, "X-Copied-From-Last-Modified", src_mtime);
}

void RGWCopyObj_ObjStore_SWIFT::send_response()
{
  if (! sent_header) {
    string content_type;
    if (! op_ret)
      op_ret = STATUS_CREATED;
    set_req_state_err(s, op_ret);
    dump_errno(s);
    dump_etag(s, etag);
    dump_last_modified(s, mtime);
    dump_copy_info();
    get_contype_from_attrs(attrs, content_type);
    dump_object_metadata(this, s, attrs);
    end_header(s, this, !content_type.empty() ? content_type.c_str()
	       : "binary/octet-stream");
  } else {
    s->formatter->close_section();
    rgw_flush_formatter(s, s->formatter);
  }
}

int RGWGetObj_ObjStore_SWIFT::verify_permission()
{
  op_ret = RGWGetObj_ObjStore::verify_permission();

  /* We have to differentiate error codes depending on whether user is
   * anonymous (401 Unauthorized) or he doesn't have necessary permissions
   * (403 Forbidden). */
  if (s->auth.identity->is_anonymous() && op_ret == -EACCES) {
    return -EPERM;
  } else {
    return op_ret;
  }
}

int RGWGetObj_ObjStore_SWIFT::get_params()
{
  const string& mm = s->info.args.get("multipart-manifest");
  skip_manifest = (mm.compare("get") == 0);

  return RGWGetObj_ObjStore::get_params();
}

int RGWGetObj_ObjStore_SWIFT::send_response_data_error()
{
  std::string error_content;
  op_ret = error_handler(op_ret, &error_content);
  if (! op_ret) {
    /* The error handler has taken care of the error. */
    return 0;
  }

  bufferlist error_bl;
  error_bl.append(error_content);
  return send_response_data(error_bl, 0, error_bl.length());
}

int RGWGetObj_ObjStore_SWIFT::send_response_data(bufferlist& bl,
                                                 const off_t bl_ofs,
                                                 const off_t bl_len)
{
  string content_type;

  if (sent_header) {
    goto send_data;
  }

  if (custom_http_ret) {
    set_req_state_err(s, 0);
    dump_errno(s, custom_http_ret);
  } else {
    set_req_state_err(s, (partial_content && !op_ret) ? STATUS_PARTIAL_CONTENT
		    : op_ret);
    dump_errno(s);

    if (s->is_err()) {
      end_header(s, NULL);
      return 0;
    }
  }

  if (range_str) {
    dump_range(s, ofs, end, s->obj_size);
  }

  if (s->is_err()) {
    end_header(s, NULL);
    return 0;
  }

  dump_content_length(s, total_len);
  dump_last_modified(s, lastmod);
  dump_header(s, "X-Timestamp", utime_t(lastmod));
  if (is_slo) {
    dump_header(s, "X-Static-Large-Object", "True");
  }

  if (! op_ret) {
    if (! lo_etag.empty()) {
      dump_etag(s, lo_etag, true /* quoted */);
    } else {
      auto iter = attrs.find(RGW_ATTR_ETAG);
      if (iter != attrs.end()) {
        dump_etag(s, iter->second.to_str());
      }
    }

    get_contype_from_attrs(attrs, content_type);
    dump_object_metadata(this, s, attrs);
  }

  end_header(s, this, !content_type.empty() ? content_type.c_str()
	     : "binary/octet-stream");

  sent_header = true;

send_data:
  if (get_data && !op_ret) {
    const auto r = dump_body(s, bl.c_str() + bl_ofs, bl_len);
    if (r < 0) {
      return r;
    }
  }
  rgw_flush_formatter_and_reset(s, s->formatter);

  return 0;
}

void RGWOptionsCORS_ObjStore_SWIFT::send_response()
{
  string hdrs, exp_hdrs;
  uint32_t max_age = CORS_MAX_AGE_INVALID;
  /*EACCES means, there is no CORS registered yet for the bucket
   *ENOENT means, there is no match of the Origin in the list of CORSRule
   */
  if (op_ret == -ENOENT)
    op_ret = -EACCES;
  if (op_ret < 0) {
    set_req_state_err(s, op_ret);
    dump_errno(s);
    end_header(s, NULL);
    return;
  }
  get_response_params(hdrs, exp_hdrs, &max_age);
  dump_errno(s);
  dump_access_control(s, origin, req_meth, hdrs.c_str(), exp_hdrs.c_str(),
		      max_age);
  end_header(s, NULL);
}

int RGWBulkDelete_ObjStore_SWIFT::get_data(
  list<RGWBulkDelete::acct_path_t>& items, bool * const is_truncated)
{
  constexpr size_t MAX_LINE_SIZE = 2048;

  RGWClientIOStreamBuf ciosb(static_cast<RGWRestfulIO&>(*(s->cio)),
			     size_t(s->cct->_conf->rgw_max_chunk_size));
  istream cioin(&ciosb);

  char buf[MAX_LINE_SIZE];
  while (cioin.getline(buf, sizeof(buf))) {
    string path_str(buf);

    ldpp_dout(this, 20) << "extracted Bulk Delete entry: " << path_str << dendl;

    RGWBulkDelete::acct_path_t path;

    /* We need to skip all slashes at the beginning in order to preserve
     * compliance with Swift. */
    const size_t start_pos = path_str.find_first_not_of('/');

    if (string::npos != start_pos) {
      /* Seperator is the first slash after the leading ones. */
      const size_t sep_pos = path_str.find('/', start_pos);

      if (string::npos != sep_pos) {
        path.bucket_name = url_decode(path_str.substr(start_pos,
                                                      sep_pos - start_pos));
        path.obj_key = url_decode(path_str.substr(sep_pos + 1));
      } else {
        /* It's guaranteed here that bucket name is at least one character
         * long and is different than slash. */
        path.bucket_name = url_decode(path_str.substr(start_pos));
      }

      items.push_back(path);
    }

    if (items.size() == MAX_CHUNK_ENTRIES) {
      *is_truncated = true;
      return 0;
    }
  }

  *is_truncated = false;
  return 0;
}

void RGWBulkDelete_ObjStore_SWIFT::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this /* RGWOp */, nullptr /* contype */,
             CHUNKED_TRANSFER_ENCODING);

  bulkdelete_respond(deleter->get_num_deleted(),
                     deleter->get_num_unfound(),
                     deleter->get_failures(),
                     s->prot_flags,
                     *s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}


std::unique_ptr<RGWBulkUploadOp::StreamGetter>
RGWBulkUploadOp_ObjStore_SWIFT::create_stream()
{
  class SwiftStreamGetter : public StreamGetter {
    const DoutPrefixProvider* dpp;
    const size_t conlen;
    size_t curpos;
    req_state* const s;

  public:
    SwiftStreamGetter(const DoutPrefixProvider* dpp, req_state* const s, const size_t conlen)
      : dpp(dpp),
        conlen(conlen),
        curpos(0),
        s(s) {
    }

    ssize_t get_at_most(size_t want, ceph::bufferlist& dst) override {
      /* maximum requested by a caller */
      /* data provided by client */
      /* RadosGW's limit. */
      const size_t max_chunk_size = \
        static_cast<size_t>(s->cct->_conf->rgw_max_chunk_size);
      const size_t max_to_read = std::min({ want, conlen - curpos, max_chunk_size });

      ldpp_dout(dpp, 20) << "bulk_upload: get_at_most max_to_read="
                        << max_to_read
                        << ", dst.c_str()=" << reinterpret_cast<intptr_t>(dst.c_str()) << dendl;

      bufferptr bp(max_to_read);
      const auto read_len = recv_body(s, bp.c_str(), max_to_read);
      dst.append(bp, 0, read_len);
      //const auto read_len = recv_body(s, dst.c_str(), max_to_read);
      if (read_len < 0) {
        return read_len;
      }

      curpos += read_len;
      return curpos > s->cct->_conf->rgw_max_put_size ? -ERR_TOO_LARGE
                                                      : read_len;
    }

    ssize_t get_exactly(size_t want, ceph::bufferlist& dst) override {
      ldpp_dout(dpp, 20) << "bulk_upload: get_exactly want=" << want << dendl;

      /* FIXME: do this in a loop. */
      const auto ret = get_at_most(want, dst);
      ldpp_dout(dpp, 20) << "bulk_upload: get_exactly ret=" << ret << dendl;
      if (ret < 0) {
        return ret;
      } else if (static_cast<size_t>(ret) != want) {
        return -EINVAL;
      } else {
        return want;
      }
    }
  };

  if (! s->length) {
    op_ret = -EINVAL;
    return nullptr;
  } else {
    ldpp_dout(this, 20) << "bulk upload: create_stream for length="
                      << s->length << dendl;

    const size_t conlen = atoll(s->length);
    return std::unique_ptr<SwiftStreamGetter>(new SwiftStreamGetter(this, s, conlen));
  }
}

void RGWBulkUploadOp_ObjStore_SWIFT::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this /* RGWOp */, nullptr /* contype */,
             CHUNKED_TRANSFER_ENCODING);
  rgw_flush_formatter_and_reset(s, s->formatter);

  s->formatter->open_object_section("delete");

  std::string resp_status;
  std::string resp_body;

  if (! failures.empty()) {
    rgw_err err;

    const auto last_err = { failures.back().err };
    if (boost::algorithm::contains(last_err, terminal_errors)) {
      /* The terminal errors are affecting the status of the whole upload. */
      set_req_state_err(err, failures.back().err, s->prot_flags);
    } else {
      set_req_state_err(err, ERR_INVALID_REQUEST, s->prot_flags);
    }

    dump_errno(err, resp_status);
  } else if (0 == num_created && failures.empty()) {
    /* Nothing created, nothing failed. This means the archive contained no
     * entity we could understand (regular file or directory). We need to
     * send 400 Bad Request to an HTTP client in the internal status field. */
    dump_errno(400, resp_status);
    resp_body = "Invalid Tar File: No Valid Files";
  } else {
    /* 200 OK */
    dump_errno(201, resp_status);
  }

  encode_json("Number Files Created", num_created, s->formatter);
  encode_json("Response Body", resp_body, s->formatter);
  encode_json("Response Status", resp_status, s->formatter);

  s->formatter->open_array_section("Errors");
  for (const auto& fail_desc : failures) {
    s->formatter->open_array_section("object");

    encode_json("Name", fail_desc.path, s->formatter);

    rgw_err err;
    set_req_state_err(err, fail_desc.err, s->prot_flags);
    std::string status;
    dump_errno(err, status);
    encode_json("Status", status, s->formatter);

    s->formatter->close_section();
  }
  s->formatter->close_section();

  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}


void RGWGetCrossDomainPolicy_ObjStore_SWIFT::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");

  std::stringstream ss;

  ss << R"(<?xml version="1.0"?>)" << "\n"
     << R"(<!DOCTYPE cross-domain-policy SYSTEM )"
     << R"("http://www.adobe.com/xml/dtds/cross-domain-policy.dtd" >)" << "\n"
     << R"(<cross-domain-policy>)" << "\n"
     << g_conf()->rgw_cross_domain_policy << "\n"
     << R"(</cross-domain-policy>)";

  dump_body(s, ss.str());
}

void RGWGetHealthCheck_ObjStore_SWIFT::send_response()
{
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");

  if (op_ret) {
    static constexpr char DISABLED[] = "DISABLED BY FILE";
    dump_body(s, DISABLED, strlen(DISABLED));
  }
}

const vector<pair<string, RGWInfo_ObjStore_SWIFT::info>> RGWInfo_ObjStore_SWIFT::swift_info =
{
    {"bulk_delete", {false, nullptr}},
    {"container_quotas", {false, nullptr}},
    {"swift", {false, RGWInfo_ObjStore_SWIFT::list_swift_data}},
    {"tempurl", { false, RGWInfo_ObjStore_SWIFT::list_tempurl_data}},
    {"slo", {false, RGWInfo_ObjStore_SWIFT::list_slo_data}},
    {"account_quotas", {false, nullptr}},
    {"staticweb", {false, nullptr}},
    {"tempauth", {false, RGWInfo_ObjStore_SWIFT::list_tempauth_data}},
};

void RGWInfo_ObjStore_SWIFT::execute()
{
  bool is_admin_info_enabled = false;

  const string& swiftinfo_sig = s->info.args.get("swiftinfo_sig");
  const string& swiftinfo_expires = s->info.args.get("swiftinfo_expires");

  if (!swiftinfo_sig.empty() &&
      !swiftinfo_expires.empty() &&
      !is_expired(swiftinfo_expires, this)) {
    is_admin_info_enabled = true;
  }

  s->formatter->open_object_section("info");

  for (const auto& pair : swift_info) {
    if(!is_admin_info_enabled && pair.second.is_admin_info)
      continue;

    if (!pair.second.list_data) {
      s->formatter->open_object_section((pair.first).c_str());
      s->formatter->close_section();
    }
    else {
      pair.second.list_data(*(s->formatter), s->cct->_conf, *store->getRados());
    }
  }

  s->formatter->close_section();
}

void RGWInfo_ObjStore_SWIFT::send_response()
{
  if (op_ret <  0) {
    op_ret = STATUS_NO_CONTENT;
  }
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWInfo_ObjStore_SWIFT::list_swift_data(Formatter& formatter,
                                              const ConfigProxy& config,
                                              RGWRados& store)
{
  formatter.open_object_section("swift");
  formatter.dump_int("max_file_size", config->rgw_max_put_size);
  formatter.dump_int("container_listing_limit", RGW_LIST_BUCKETS_LIMIT_MAX);

  string ceph_version(CEPH_GIT_NICE_VER);
  formatter.dump_string("version", ceph_version);

  const size_t max_attr_name_len = \
    g_conf().get_val<Option::size_t>("rgw_max_attr_name_len");
  if (max_attr_name_len) {
    const size_t meta_name_limit = \
      max_attr_name_len - strlen(RGW_ATTR_PREFIX RGW_AMZ_META_PREFIX);
    formatter.dump_int("max_meta_name_length", meta_name_limit);
  }

  const size_t meta_value_limit = g_conf().get_val<Option::size_t>("rgw_max_attr_size");
  if (meta_value_limit) {
    formatter.dump_int("max_meta_value_length", meta_value_limit);
  }

  const size_t meta_num_limit = \
    g_conf().get_val<uint64_t>("rgw_max_attrs_num_in_req");
  if (meta_num_limit) {
    formatter.dump_int("max_meta_count", meta_num_limit);
  }

  formatter.open_array_section("policies");
  const RGWZoneGroup& zonegroup = store.svc.zone->get_zonegroup();

  for (const auto& placement_targets : zonegroup.placement_targets) {
    formatter.open_object_section("policy");
    if (placement_targets.second.name.compare(zonegroup.default_placement.name) == 0)
      formatter.dump_bool("default", true);
    formatter.dump_string("name", placement_targets.second.name.c_str());
    formatter.close_section();
  }
  formatter.close_section();

  formatter.dump_int("max_object_name_size", RGWHandler_REST::MAX_OBJ_NAME_LEN);
  formatter.dump_bool("strict_cors_mode", true);
  formatter.dump_int("max_container_name_length", RGWHandler_REST::MAX_BUCKET_NAME_LEN);
  formatter.close_section();
}

void RGWInfo_ObjStore_SWIFT::list_tempauth_data(Formatter& formatter,
                                                 const ConfigProxy& config,
                                                 RGWRados& store)
{
  formatter.open_object_section("tempauth");
  formatter.dump_bool("account_acls", true);
  formatter.close_section();
}
void RGWInfo_ObjStore_SWIFT::list_tempurl_data(Formatter& formatter,
                                                const ConfigProxy& config,
                                                RGWRados& store)
{
  formatter.open_object_section("tempurl");
  formatter.open_array_section("methods");
  formatter.dump_string("methodname", "GET");
  formatter.dump_string("methodname", "HEAD");
  formatter.dump_string("methodname", "PUT");
  formatter.dump_string("methodname", "POST");
  formatter.dump_string("methodname", "DELETE");
  formatter.close_section();
  formatter.close_section();
}

void RGWInfo_ObjStore_SWIFT::list_slo_data(Formatter& formatter,
                                            const ConfigProxy& config,
                                            RGWRados& store)
{
  formatter.open_object_section("slo");
  formatter.dump_int("max_manifest_segments", config->rgw_max_slo_entries);
  formatter.close_section();
}

bool RGWInfo_ObjStore_SWIFT::is_expired(const std::string& expires, const DoutPrefixProvider *dpp)
{
  string err;
  const utime_t now = ceph_clock_now();
  const uint64_t expiration = (uint64_t)strict_strtoll(expires.c_str(),
                                                       10, &err);
  if (!err.empty()) {
    ldpp_dout(dpp, 5) << "failed to parse siginfo_expires: " << err << dendl;
    return true;
  }

  if (expiration <= (uint64_t)now.sec()) {
    ldpp_dout(dpp, 5) << "siginfo expired: " << expiration << " <= " << now.sec() << dendl;
    return true;
  }

  return false;
}


void RGWFormPost::init(rgw::sal::RGWRadosStore* const store,
                       req_state* const s,
                       RGWHandler* const dialect_handler)
{
  prefix = std::move(s->object->get_name());
  s->object->set_key(rgw_obj_key());

  return RGWPostObj_ObjStore::init(store, s, dialect_handler);
}

std::size_t RGWFormPost::get_max_file_size() /*const*/
{
  std::string max_str = get_part_str(ctrl_parts, "max_file_size", "0");

  std::string err;
  const std::size_t max_file_size =
    static_cast<uint64_t>(strict_strtoll(max_str.c_str(), 10, &err));

  if (! err.empty()) {
    ldpp_dout(this, 5) << "failed to parse FormPost's max_file_size: " << err
                     << dendl;
    return 0;
  }

  return max_file_size;
}

bool RGWFormPost::is_non_expired()
{
  std::string expires = get_part_str(ctrl_parts, "expires", "0");

  std::string err;
  const uint64_t expires_timestamp =
    static_cast<uint64_t>(strict_strtoll(expires.c_str(), 10, &err));

  if (! err.empty()) {
    ldpp_dout(this, 5) << "failed to parse FormPost's expires: " << err << dendl;
    return false;
  }

  const utime_t now = ceph_clock_now();
  if (expires_timestamp <= static_cast<uint64_t>(now.sec())) {
    ldpp_dout(this, 5) << "FormPost form expired: "
            << expires_timestamp << " <= " << now.sec() << dendl;
    return false;
  }

  return true;
}

bool RGWFormPost::is_integral()
{
  const std::string form_signature = get_part_str(ctrl_parts, "signature");

  try {
    get_owner_info(s, s->user->get_info());
    s->auth.identity = rgw::auth::transform_old_authinfo(s);
  } catch (...) {
    ldpp_dout(this, 5) << "cannot get user_info of account's owner" << dendl;
    return false;
  }

  for (const auto& kv : s->user->get_info().temp_url_keys) {
    const int temp_url_key_num = kv.first;
    const string& temp_url_key = kv.second;

    if (temp_url_key.empty()) {
      continue;
    }

    SignatureHelper sig_helper;
    sig_helper.calc(temp_url_key,
                    s->info.request_uri,
                    get_part_str(ctrl_parts, "redirect"),
                    get_part_str(ctrl_parts, "max_file_size", "0"),
                    get_part_str(ctrl_parts, "max_file_count", "0"),
                    get_part_str(ctrl_parts, "expires", "0"));

    const auto local_sig = sig_helper.get_signature();

    ldpp_dout(this, 20) << "FormPost signature [" << temp_url_key_num << "]"
                      << " (calculated): " << local_sig << dendl;

    if (sig_helper.is_equal_to(form_signature)) {
      return true;
    } else {
      ldpp_dout(this, 5) << "FormPost's signature mismatch: "
                       << local_sig << " != " << form_signature << dendl;
    }
  }

  return false;
}

void RGWFormPost::get_owner_info(const req_state* const s,
                                   RGWUserInfo& owner_info) const
{
  /* We cannot use req_state::bucket_name because it isn't available
   * now. It will be initialized in RGWHandler_REST_SWIFT::postauth_init(). */
  const string& bucket_name = s->init_state.url_bucket;

  auto user_ctl = store->ctl()->user;

  /* TempURL in Formpost only requires that bucket name is specified. */
  if (bucket_name.empty()) {
    throw -EPERM;
  }

  string bucket_tenant;
  if (!s->account_name.empty()) {
    RGWUserInfo uinfo;
    bool found = false;

    const rgw_user uid(s->account_name);
    if (uid.tenant.empty()) {
      const rgw_user tenanted_uid(uid.id, uid.id);

      if (user_ctl->get_info_by_uid(tenanted_uid, &uinfo, s->yield) >= 0) {
        /* Succeeded. */
        bucket_tenant = uinfo.user_id.tenant;
        found = true;
      }
    }

    if (!found && user_ctl->get_info_by_uid(uid, &uinfo, s->yield) < 0) {
      throw -EPERM;
    } else {
      bucket_tenant = uinfo.user_id.tenant;
    }
  }

  /* Need to get user info of bucket owner. */
  RGWBucketInfo bucket_info;
  int ret = store->getRados()->get_bucket_info(store->svc(),
                                   bucket_tenant, bucket_name,
                                   bucket_info, nullptr, s->yield);
  if (ret < 0) {
    throw ret;
  }

  ldpp_dout(this, 20) << "temp url user (bucket owner): " << bucket_info.owner
                 << dendl;

  if (user_ctl->get_info_by_uid(bucket_info.owner, &owner_info, s->yield) < 0) {
    throw -EPERM;
  }
}

int RGWFormPost::get_params()
{
  /* The parentt class extracts boundary info from the Content-Type. */
  int ret = RGWPostObj_ObjStore::get_params();
  if (ret < 0) {
    return ret;
  }

  policy.create_default(s->user->get_id(), s->user->get_display_name());

  /* Let's start parsing the HTTP body by parsing each form part step-
   * by-step till encountering the first part with file data. */
  do {
    struct post_form_part part;
    ret = read_form_part_header(&part, stream_done);
    if (ret < 0) {
      return ret;
    }

    if (s->cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      ldpp_dout(this, 20) << "read part header -- part.name="
                        << part.name << dendl;

      for (const auto& pair : part.fields) {
        ldpp_dout(this, 20) << "field.name=" << pair.first << dendl;
        ldpp_dout(this, 20) << "field.val=" << pair.second.val << dendl;
        ldpp_dout(this, 20) << "field.params:" << dendl;

        for (const auto& param_pair : pair.second.params) {
          ldpp_dout(this, 20) << " " << param_pair.first
                            << " -> " << param_pair.second << dendl;
        }
      }
    }

    if (stream_done) {
      /* Unexpected here. */
      err_msg = "Malformed request";
      return -EINVAL;
    }

    const auto field_iter = part.fields.find("Content-Disposition");
    if (std::end(part.fields) != field_iter &&
        std::end(field_iter->second.params) != field_iter->second.params.find("filename")) {
      /* First data part ahead. */
      current_data_part = std::move(part);

      /* Stop the iteration. We can assume that all control parts have been
       * already parsed. The rest of HTTP body should contain data parts
       * only. They will be picked up by ::get_data(). */
      break;
    } else {
      /* Control part ahead. Receive, parse and store for later usage. */
      bool boundary;
      ret = read_data(part.data, s->cct->_conf->rgw_max_chunk_size,
                      boundary, stream_done);
      if (ret < 0) {
        return ret;
      } else if (! boundary) {
        err_msg = "Couldn't find boundary";
        return -EINVAL;
      }

      ctrl_parts[part.name] = std::move(part);
    }
  } while (! stream_done);

  min_len = 0;
  max_len = get_max_file_size();

  if (! current_data_part) {
    err_msg = "FormPost: no files to process";
    return -EINVAL;
  }

  if (! is_non_expired()) {
    err_msg = "FormPost: Form Expired";
    return -EPERM;
  }

  if (! is_integral()) {
    err_msg = "FormPost: Invalid Signature";
    return -EPERM;
  }

  return 0;
}

std::string RGWFormPost::get_current_filename() const
{
  try {
    const auto& field = current_data_part->fields.at("Content-Disposition");
    const auto iter = field.params.find("filename");

    if (std::end(field.params) != iter) {
      return prefix + iter->second;
    }
  } catch (std::out_of_range&) {
    /* NOP */;
  }

  return prefix;
}

std::string RGWFormPost::get_current_content_type() const
{
  try {
    const auto& field = current_data_part->fields.at("Content-Type");
    return field.val;
  } catch (std::out_of_range&) {
    /* NOP */;
  }

  return std::string();
}

bool RGWFormPost::is_next_file_to_upload()
{
  if (! stream_done) {
    /* We have at least one additional part in the body. */
    struct post_form_part part;
    int r = read_form_part_header(&part, stream_done);
    if (r < 0) {
      return false;
    }

    const auto field_iter = part.fields.find("Content-Disposition");
    if (std::end(part.fields) != field_iter) {
      const auto& params = field_iter->second.params;
      const auto& filename_iter = params.find("filename");

      if (std::end(params) != filename_iter && ! filename_iter->second.empty()) {
        current_data_part = std::move(part);
        return true;
      }
    }
  }

  return false;
}

int RGWFormPost::get_data(ceph::bufferlist& bl, bool& again)
{
  bool boundary;

  int r = read_data(bl, s->cct->_conf->rgw_max_chunk_size,
                    boundary, stream_done);
  if (r < 0) {
    return r;
  }

  /* Tell RGWPostObj::execute() that it has some data to put. */
  again = !boundary;

  return bl.length();
}

void RGWFormPost::send_response()
{
  std::string redirect = get_part_str(ctrl_parts, "redirect");
  if (! redirect.empty()) {
    op_ret = STATUS_REDIRECT;
  }

  set_req_state_err(s, op_ret);
  s->err.err_code = err_msg;
  dump_errno(s);
  if (! redirect.empty()) {
    dump_redirect(s, redirect);
  }
  end_header(s, this);
}

bool RGWFormPost::is_formpost_req(req_state* const s)
{
  std::string content_type;
  std::map<std::string, std::string> params;

  parse_boundary_params(s->info.env->get("CONTENT_TYPE", ""),
                        content_type, params);

  return boost::algorithm::iequals(content_type, "multipart/form-data") &&
         params.count("boundary") > 0;
}


RGWOp *RGWHandler_REST_Service_SWIFT::op_get()
{
  return new RGWListBuckets_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Service_SWIFT::op_head()
{
  return new RGWStatAccount_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Service_SWIFT::op_put()
{
  if (s->info.args.exists("extract-archive")) {
    return new RGWBulkUploadOp_ObjStore_SWIFT;
  }
  return nullptr;
}

RGWOp *RGWHandler_REST_Service_SWIFT::op_post()
{
  if (s->info.args.exists("bulk-delete")) {
    return new RGWBulkDelete_ObjStore_SWIFT;
  }
  return new RGWPutMetadataAccount_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Service_SWIFT::op_delete()
{
  if (s->info.args.exists("bulk-delete")) {
    return new RGWBulkDelete_ObjStore_SWIFT;
  }
  return NULL;
}

int RGWSwiftWebsiteHandler::serve_errordoc(const int http_ret,
                                           const std::string error_doc)
{
  /* Try to throw it all away. */
  s->formatter->reset();

  class RGWGetErrorPage : public RGWGetObj_ObjStore_SWIFT {
  public:
    RGWGetErrorPage(rgw::sal::RGWRadosStore* const store,
                    RGWHandler_REST* const handler,
                    req_state* const s,
                    const int http_ret) {
      /* Calling a virtual from the base class is safe as the subobject should
       * be properly initialized and we haven't overridden the init method. */
      init(store, s, handler);
      set_get_data(true);
      set_custom_http_response(http_ret);
    }

    int error_handler(const int err_no,
                      std::string* const error_content) override {
      /* Enforce that any error generated while getting the error page will
       * not be send to a client. This allows us to recover from the double
       * fault situation by sending the original message. */
      return 0;
    }
  } get_errpage_op(store, handler, s, http_ret);

  if (!rgw::sal::RGWBucket::empty(s->bucket.get())) {
    s->object = s->bucket->get_object(rgw_obj_key(std::to_string(http_ret) + error_doc));
  } else {
    s->object = store->get_object(rgw_obj_key(std::to_string(http_ret) + error_doc));
  }

  RGWOp* newop = &get_errpage_op;
  RGWRequest req(0);
  return rgw_process_authenticated(handler, newop, &req, s, true);
}

int RGWSwiftWebsiteHandler::error_handler(const int err_no,
                                          std::string* const error_content)
{
  const auto& ws_conf = s->bucket->get_info().website_conf;

  if (can_be_website_req() && ! ws_conf.error_doc.empty()) {
    set_req_state_err(s, err_no);
    return serve_errordoc(s->err.http_ret, ws_conf.error_doc);
  }

  /* Let's go to the default, no-op handler. */
  return err_no;
}

bool RGWSwiftWebsiteHandler::is_web_mode() const
{
  const std::string_view webmode = s->info.env->get("HTTP_X_WEB_MODE", "");
  return boost::algorithm::iequals(webmode, "true");
}

bool RGWSwiftWebsiteHandler::can_be_website_req() const
{
  /* Static website works only with the GET or HEAD method. Nothing more. */
  static const std::set<std::string_view> ws_methods = { "GET", "HEAD" };
  if (ws_methods.count(s->info.method) == 0) {
    return false;
  }

  /* We also need to handle early failures from the auth system. In such cases
   * req_state::auth.identity may be empty. Let's treat that the same way as
   * the anonymous access. */
  if (! s->auth.identity) {
    return true;
  }

  /* Swift serves websites only for anonymous requests unless client explicitly
   * requested this behaviour by supplying X-Web-Mode HTTP header set to true. */
  if (s->auth.identity->is_anonymous() || is_web_mode()) {
    return true;
  }

  return false;
}

RGWOp* RGWSwiftWebsiteHandler::get_ws_redirect_op()
{
  class RGWMovedPermanently: public RGWOp {
    const std::string location;
  public:
    explicit RGWMovedPermanently(const std::string& location)
      : location(location) {
    }

    int verify_permission() override {
      return 0;
    }

    void execute() override {
      op_ret = -ERR_PERMANENT_REDIRECT;
      return;
    }

    void send_response() override {
      set_req_state_err(s, op_ret);
      dump_errno(s);
      dump_content_length(s, 0);
      dump_redirect(s, location);
      end_header(s, this);
    }

    const char* name() const override {
      return "RGWMovedPermanently";
    }
  };

  return new RGWMovedPermanently(s->info.request_uri + '/');
}

RGWOp* RGWSwiftWebsiteHandler::get_ws_index_op()
{
  /* Retarget to get obj on requested index file. */
  if (! s->object->empty()) {
    s->object->set_name(s->object->get_name() +
                s->bucket->get_info().website_conf.get_index_doc());
  } else {
    s->object->set_name(s->bucket->get_info().website_conf.get_index_doc());
  }

  auto getop = new RGWGetObj_ObjStore_SWIFT;
  getop->set_get_data(boost::algorithm::equals("GET", s->info.method));

  return getop;
}

RGWOp* RGWSwiftWebsiteHandler::get_ws_listing_op()
{
  class RGWWebsiteListing : public RGWListBucket_ObjStore_SWIFT {
    const std::string prefix_override;

    int get_params() override {
      prefix = prefix_override;
      max = default_max;
      delimiter = "/";
      return 0;
    }

    void send_response() override {
      /* Generate the header now. */
      set_req_state_err(s, op_ret);
      dump_errno(s);
      dump_container_metadata(s, s->bucket.get(), bucket_quota,
                              s->bucket->get_info().website_conf);
      end_header(s, this, "text/html");
      if (op_ret < 0) {
        return;
      }

      /* Now it's the time to start generating HTML bucket listing.
       * All the crazy stuff with crafting tags will be delegated to
       * RGWSwiftWebsiteListingFormatter. */
      std::stringstream ss;
      RGWSwiftWebsiteListingFormatter htmler(ss, prefix);

      const auto& ws_conf = s->bucket->get_info().website_conf;
      htmler.generate_header(s->decoded_uri,
                             ws_conf.listing_css_doc);

      for (const auto& pair : common_prefixes) {
        std::string subdir_name = pair.first;
        if (! subdir_name.empty()) {
          /* To be compliant with Swift we need to remove the trailing
           * slash. */
          subdir_name.pop_back();
        }

        htmler.dump_subdir(subdir_name);
      }

      for (const rgw_bucket_dir_entry& obj : objs) {
        if (! common_prefixes.count(obj.key.name + '/')) {
          htmler.dump_object(obj);
        }
      }

      htmler.generate_footer();
      dump_body(s, ss.str());
    }
  public:
    /* Taking prefix_override by value to leverage std::string r-value ref
     * ctor and thus avoid extra memory copying/increasing ref counter. */
    explicit RGWWebsiteListing(std::string prefix_override)
      : prefix_override(std::move(prefix_override)) {
    }
  };

  std::string prefix = std::move(s->object->get_name());
  s->object->set_key(rgw_obj_key());

  return new RGWWebsiteListing(std::move(prefix));
}

bool RGWSwiftWebsiteHandler::is_web_dir() const
{
  std::string subdir_name = url_decode(s->object->get_name());

  /* Remove character from the subdir name if it is "/". */
  if (subdir_name.empty()) {
    return false;
  } else if (subdir_name.back() == '/') {
    subdir_name.pop_back();
  }

  rgw::sal::RGWRadosObject obj(store, rgw_obj_key(std::move(subdir_name)), s->bucket.get());

  /* First, get attrset of the object we'll try to retrieve. */
  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  obj.set_atomic(&obj_ctx);
  obj.set_prefetch_data(&obj_ctx);

  RGWObjState* state = nullptr;
  if (obj.get_obj_state(&obj_ctx, *s->bucket, &state, s->yield, false)) {
    return false;
  }

  /* A nonexistent object cannot be a considered as a marker representing
   * the emulation of catalog in FS hierarchy. */
  if (! state->exists) {
    return false;
  }

  /* Decode the content type. */
  std::string content_type;
  get_contype_from_attrs(state->attrset, content_type);

  const auto& ws_conf = s->bucket->get_info().website_conf;
  const std::string subdir_marker = ws_conf.subdir_marker.empty()
                                      ? "application/directory"
                                      : ws_conf.subdir_marker;
  return subdir_marker == content_type && state->size <= 1;
}

bool RGWSwiftWebsiteHandler::is_index_present(const std::string& index) const
{
  rgw::sal::RGWRadosObject obj(store, rgw_obj_key(index), s->bucket.get());

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  obj.set_atomic(&obj_ctx);
  obj.set_prefetch_data(&obj_ctx);

  RGWObjState* state = nullptr;
  if (obj.get_obj_state(&obj_ctx, *s->bucket, &state, s->yield, false)) {
    return false;
  }

  /* A nonexistent object cannot be a considered as a viable index. We will
   * try to list the bucket or - if this is impossible - return an error. */
  return state->exists;
}

int RGWSwiftWebsiteHandler::retarget_bucket(RGWOp* op, RGWOp** new_op)
{
  ldpp_dout(s, 10) << "Starting retarget" << dendl;
  RGWOp* op_override = nullptr;

  /* In Swift static web content is served if the request is anonymous or
   * has X-Web-Mode HTTP header specified to true. */
  if (can_be_website_req()) {
    const auto& ws_conf = s->bucket->get_info().website_conf;
    const auto& index = s->bucket->get_info().website_conf.get_index_doc();

    if (s->decoded_uri.back() != '/') {
      op_override = get_ws_redirect_op();
    } else if (! index.empty() && is_index_present(index)) {
      op_override = get_ws_index_op();
    } else if (ws_conf.listing_enabled) {
      op_override = get_ws_listing_op();
    }
  }

  if (op_override) {
    handler->put_op(op);
    op_override->init(store, s, handler);

    *new_op = op_override;
  } else {
    *new_op = op;
  }

  /* Return 404 Not Found is the request has web mode enforced but we static web
   * wasn't able to serve it accordingly. */
  return ! op_override && is_web_mode() ? -ENOENT : 0;
}

int RGWSwiftWebsiteHandler::retarget_object(RGWOp* op, RGWOp** new_op)
{
  ldpp_dout(s, 10) << "Starting object retarget" << dendl;
  RGWOp* op_override = nullptr;

  /* In Swift static web content is served if the request is anonymous or
   * has X-Web-Mode HTTP header specified to true. */
  if (can_be_website_req() && is_web_dir()) {
    const auto& ws_conf = s->bucket->get_info().website_conf;
    const auto& index = s->bucket->get_info().website_conf.get_index_doc();

    if (s->decoded_uri.back() != '/') {
      op_override = get_ws_redirect_op();
    } else if (! index.empty() && is_index_present(index)) {
      op_override = get_ws_index_op();
    } else if (ws_conf.listing_enabled) {
      op_override = get_ws_listing_op();
    }
  } else {
    /* A regular request or the specified object isn't a subdirectory marker.
     * We don't need any re-targeting. Error handling (like sending a custom
     * error page) will be performed by error_handler of the actual RGWOp. */
    return 0;
  }

  if (op_override) {
    handler->put_op(op);
    op_override->init(store, s, handler);

    *new_op = op_override;
  } else {
    *new_op = op;
  }

  /* Return 404 Not Found if we aren't able to re-target for subdir marker. */
  return ! op_override ? -ENOENT : 0;
}


RGWOp *RGWHandler_REST_Bucket_SWIFT::get_obj_op(bool get_data)
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_SWIFT;
  }

  if (get_data)
    return new RGWListBucket_ObjStore_SWIFT;
  else
    return new RGWStatBucket_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Bucket_SWIFT::op_get()
{
  return get_obj_op(true);
}

RGWOp *RGWHandler_REST_Bucket_SWIFT::op_head()
{
  return get_obj_op(false);
}

RGWOp *RGWHandler_REST_Bucket_SWIFT::op_put()
{
  if (is_acl_op()) {
    return new RGWPutACLs_ObjStore_SWIFT;
  }
  if(s->info.args.exists("extract-archive")) {
    return new RGWBulkUploadOp_ObjStore_SWIFT;
  }
  return new RGWCreateBucket_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Bucket_SWIFT::op_delete()
{
  return new RGWDeleteBucket_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Bucket_SWIFT::op_post()
{
  if (RGWFormPost::is_formpost_req(s)) {
    return new RGWFormPost;
  } else {
    return new RGWPutMetadataBucket_ObjStore_SWIFT;
  }
}

RGWOp *RGWHandler_REST_Bucket_SWIFT::op_options()
{
  return new RGWOptionsCORS_ObjStore_SWIFT;
}


RGWOp *RGWHandler_REST_Obj_SWIFT::get_obj_op(bool get_data)
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_SWIFT;
  }

  RGWGetObj_ObjStore_SWIFT *get_obj_op = new RGWGetObj_ObjStore_SWIFT;
  get_obj_op->set_get_data(get_data);
  return get_obj_op;
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_get()
{
  return get_obj_op(true);
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_head()
{
  return get_obj_op(false);
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_put()
{
  if (is_acl_op()) {
    return new RGWPutACLs_ObjStore_SWIFT;
  }
  if(s->info.args.exists("extract-archive")) {
    return new RGWBulkUploadOp_ObjStore_SWIFT;
  }
  if (s->init_state.src_bucket.empty())
    return new RGWPutObj_ObjStore_SWIFT;
  else
    return new RGWCopyObj_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_delete()
{
  return new RGWDeleteObj_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_post()
{
  if (RGWFormPost::is_formpost_req(s)) {
    return new RGWFormPost;
  } else {
    return new RGWPutMetadataObject_ObjStore_SWIFT;
  }
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_copy()
{
  return new RGWCopyObj_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_options()
{
  return new RGWOptionsCORS_ObjStore_SWIFT;
}


int RGWHandler_REST_SWIFT::authorize(const DoutPrefixProvider *dpp)
{
  return rgw::auth::Strategy::apply(dpp, auth_strategy, s);
}

int RGWHandler_REST_SWIFT::postauth_init()
{
  struct req_init_state* t = &s->init_state;

  /* XXX Stub this until Swift Auth sets account into URL. */
  s->bucket_tenant = s->user->get_tenant();
  s->bucket_name = t->url_bucket;

  if (!s->object) {
    /* Need an object, even an empty one */
    s->object = store->get_object(rgw_obj_key());
  }

  dout(10) << "s->object=" <<
    (!s->object->empty() ? s->object->get_key() : rgw_obj_key("<NULL>"))
           << " s->bucket="
	   << rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name)
	   << dendl;

  int ret;
  ret = rgw_validate_tenant_name(s->bucket_tenant);
  if (ret)
    return ret;
  ret = validate_bucket_name(s->bucket_name);
  if (ret)
    return ret;
  ret = validate_object_name(s->object->get_name());
  if (ret)
    return ret;

  if (!t->src_bucket.empty()) {
    /*
     * We don't allow cross-tenant copy at present. It requires account
     * names in the URL for Swift.
     */
    s->src_tenant_name = s->user->get_tenant();
    s->src_bucket_name = t->src_bucket;

    ret = validate_bucket_name(s->src_bucket_name);
    if (ret < 0) {
      return ret;
    }
    ret = validate_object_name(s->src_object->get_name());
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

int RGWHandler_REST_SWIFT::validate_bucket_name(const string& bucket)
{
  const size_t len = bucket.size();

  if (len > MAX_BUCKET_NAME_LEN) {
    /* Bucket Name too long. Generate custom error message and bind it
     * to an R-value reference. */
    const auto msg = boost::str(
      boost::format("Container name length of %lld longer than %lld")
        % len % int(MAX_BUCKET_NAME_LEN));
    set_req_state_err(s, ERR_INVALID_BUCKET_NAME, msg);
    return -ERR_INVALID_BUCKET_NAME;
  }

  const auto ret = RGWHandler_REST::validate_bucket_name(bucket);
  if (ret < 0) {
    return ret;
  }

  if (len == 0)
    return 0;

  if (bucket[0] == '.')
    return -ERR_INVALID_BUCKET_NAME;

  if (check_utf8(bucket.c_str(), len))
    return -ERR_INVALID_UTF8;

  const char *s = bucket.c_str();

  for (size_t i = 0; i < len; ++i, ++s) {
    if (*(unsigned char *)s == 0xff)
      return -ERR_INVALID_BUCKET_NAME;
    if (*(unsigned char *)s == '/')
      return -ERR_INVALID_BUCKET_NAME;
  }

  return 0;
}

static void next_tok(string& str, string& tok, char delim)
{
  if (str.size() == 0) {
    tok = "";
    return;
  }
  tok = str;
  int pos = str.find(delim);
  if (pos > 0) {
    tok = str.substr(0, pos);
    str = str.substr(pos + 1);
  } else {
    str = "";
  }
}

int RGWHandler_REST_SWIFT::init_from_header(rgw::sal::RGWRadosStore* store,
					    struct req_state* const s,
                                            const std::string& frontend_prefix)
{
  string req;
  string first;

  s->prot_flags |= RGW_REST_SWIFT;

  char reqbuf[frontend_prefix.length() + s->decoded_uri.length() + 1];
  sprintf(reqbuf, "%s%s", frontend_prefix.c_str(), s->decoded_uri.c_str());
  const char *req_name = reqbuf;

  const char *p;

  if (*req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse();

  /* Skip the leading slash of URL hierarchy. */
  if (req_name[0] != '/') {
    return 0;
  } else {
    req_name++;
  }

  if ('\0' == req_name[0]) {
    return g_conf()->rgw_swift_url_prefix == "/" ? -ERR_BAD_URL : 0;
  }

  req = req_name;

  size_t pos = req.find('/');
  if (std::string::npos != pos && g_conf()->rgw_swift_url_prefix != "/") {
    bool cut_url = g_conf()->rgw_swift_url_prefix.length();
    first = req.substr(0, pos);

    if (first.compare(g_conf()->rgw_swift_url_prefix) == 0) {
      if (cut_url) {
        /* Rewind to the "v1/..." part. */
        next_tok(req, first, '/');
      }
    }
  } else if (req.compare(g_conf()->rgw_swift_url_prefix) == 0) {
    s->formatter = new RGWFormatter_Plain;
    return -ERR_BAD_URL;
  } else {
    first = req;
  }

  std::string tenant_path;
  if (! g_conf()->rgw_swift_tenant_name.empty()) {
    tenant_path = "/AUTH_";
    tenant_path.append(g_conf()->rgw_swift_tenant_name);
  }

  /* verify that the request_uri conforms with what's expected */
  char buf[g_conf()->rgw_swift_url_prefix.length() + 16 + tenant_path.length()];
  int blen;
  if (g_conf()->rgw_swift_url_prefix == "/") {
    blen = sprintf(buf, "/v1%s", tenant_path.c_str());
  } else {
    blen = sprintf(buf, "/%s/v1%s",
                   g_conf()->rgw_swift_url_prefix.c_str(), tenant_path.c_str());
  }

  if (strncmp(reqbuf, buf, blen) != 0) {
    return -ENOENT;
  }

  int ret = allocate_formatter(s, RGW_FORMAT_PLAIN, true);
  if (ret < 0)
    return ret;

  string ver;

  next_tok(req, ver, '/');

  if (!tenant_path.empty() || g_conf()->rgw_swift_account_in_url) {
    string account_name;
    next_tok(req, account_name, '/');

    /* Erase all pre-defined prefixes like "AUTH_" or "KEY_". */
    const vector<string> skipped_prefixes = { "AUTH_", "KEY_" };

    for (const auto pfx : skipped_prefixes) {
      const size_t comp_len = min(account_name.length(), pfx.length());
      if (account_name.compare(0, comp_len, pfx) == 0) {
        /* Prefix is present. Drop it. */
        account_name = account_name.substr(comp_len);
        break;
      }
    }

    if (account_name.empty()) {
      return -ERR_PRECONDITION_FAILED;
    } else {
      s->account_name = account_name;
    }
  }

  next_tok(req, first, '/');

  dout(10) << "ver=" << ver << " first=" << first << " req=" << req << dendl;
  if (first.size() == 0)
    return 0;

  s->info.effective_uri = "/" + first;

  // Save bucket to tide us over until token is parsed.
  s->init_state.url_bucket = first;

  if (req.size()) {
    s->object = store->get_object(
      rgw_obj_key(req, s->info.env->get("HTTP_X_OBJECT_VERSION_ID", ""))); /* rgw swift extension */
    s->info.effective_uri.append("/" + s->object->get_name());
  }

  return 0;
}

int RGWHandler_REST_SWIFT::init(rgw::sal::RGWRadosStore* store, struct req_state* s,
				rgw::io::BasicClient *cio)
{
  struct req_init_state *t = &s->init_state;

  s->dialect = "swift";

  std::string copy_source = s->info.env->get("HTTP_X_COPY_FROM", "");
  if (! copy_source.empty()) {
    rgw_obj_key key;
    bool result = RGWCopyObj::parse_copy_location(copy_source, t->src_bucket, key);
    if (!result)
      return -ERR_BAD_URL;
    s->src_object = store->get_object(key);
    if (!s->src_object)
      return -ERR_BAD_URL;
  }

  if (s->op == OP_COPY) {
    std::string req_dest = s->info.env->get("HTTP_DESTINATION", "");
    if (req_dest.empty())
      return -ERR_BAD_URL;

    std::string dest_bucket_name;
    rgw_obj_key dest_obj_key;
    bool result =
      RGWCopyObj::parse_copy_location(req_dest, dest_bucket_name,
				      dest_obj_key);
    if (!result)
       return -ERR_BAD_URL;

    std::string dest_object_name = dest_obj_key.name;

    /* convert COPY operation into PUT */
    t->src_bucket = t->url_bucket;
    s->src_object = s->object->clone();
    t->url_bucket = dest_bucket_name;
    s->object->set_name(dest_object_name);
    s->op = OP_PUT;
  }

  s->info.storage_class = s->info.env->get("HTTP_X_OBJECT_STORAGE_CLASS", "");

  return RGWHandler_REST::init(store, s, cio);
}

RGWHandler_REST*
RGWRESTMgr_SWIFT::get_handler(rgw::sal::RGWRadosStore* store,
			      struct req_state* const s,
                              const rgw::auth::StrategyRegistry& auth_registry,
                              const std::string& frontend_prefix)
{
  int ret = RGWHandler_REST_SWIFT::init_from_header(store, s, frontend_prefix);
  if (ret < 0) {
    ldpp_dout(s, 10) << "init_from_header returned err=" << ret <<  dendl;
    return nullptr;
  }

  const auto& auth_strategy = auth_registry.get_swift();

  if (s->init_state.url_bucket.empty()) {
    return new RGWHandler_REST_Service_SWIFT(auth_strategy);
  }

  if (rgw::sal::RGWObject::empty(s->object.get())) {
    return new RGWHandler_REST_Bucket_SWIFT(auth_strategy);
  }

  return new RGWHandler_REST_Obj_SWIFT(auth_strategy);
}

RGWHandler_REST* RGWRESTMgr_SWIFT_Info::get_handler(
  rgw::sal::RGWRadosStore* store,
  struct req_state* const s,
  const rgw::auth::StrategyRegistry& auth_registry,
  const std::string& frontend_prefix)
{
  s->prot_flags |= RGW_REST_SWIFT;
  const auto& auth_strategy = auth_registry.get_swift();
  return new RGWHandler_REST_SWIFT_Info(auth_strategy);
}
