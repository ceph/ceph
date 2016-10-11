// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/algorithm/string/predicate.hpp>
#include <boost/optional.hpp>
#include <boost/utility/in_place_factory.hpp>

#include "include/assert.h"
#include "ceph_ver.h"

#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"

#include "rgw_rest_swift.h"
#include "rgw_acl_swift.h"
#include "rgw_cors_swift.h"
#include "rgw_formats.h"
#include "rgw_client_io.h"

#include "rgw_auth.h"
#include "rgw_auth_decoimpl.h"
#include "rgw_swift_auth.h"

#include "rgw_request.h"
#include "rgw_process.h"

#include <array>
#include <sstream>
#include <memory>

#include <boost/utility/string_ref.hpp>

#define dout_subsys ceph_subsys_rgw

int RGWListBuckets_ObjStore_SWIFT::get_params()
{
  marker = s->info.args.get("marker");
  end_marker = s->info.args.get("end_marker");

  string limit_str = s->info.args.get("limit");
  if (!limit_str.empty()) {
    string err;
    long l = strict_strtol(limit_str.c_str(), 10, &err);
    if (!err.empty()) {
      return -EINVAL;
    }

    if (l > (long)limit_max || l < 0) {
      return -ERR_PRECONDITION_FAILED;
    }

    limit = (uint64_t)l;
  }

  if (need_stats) {
    bool stats, exists;
    int r = s->info.args.get_bool("stats", &stats, &exists);

    if (r < 0) {
      return r;
    }

    if (exists) {
      need_stats = stats;
    }
  }

  return 0;
}

static void dump_account_metadata(struct req_state * const s,
                                  const uint32_t buckets_count,
                                  const uint64_t buckets_object_count,
                                  const uint64_t buckets_size,
                                  const uint64_t buckets_size_rounded,
                                  /* const */map<string, bufferlist>& attrs,
                                  const RGWQuotaInfo& quota,
                                  const RGWAccessControlPolicy_SWIFTAcct &policy)
{
  /* Adding X-Timestamp to keep align with Swift API */
  dump_header(s, "X-Timestamp", ceph_clock_now(g_ceph_context));

  dump_header(s, "X-Account-Container-Count", buckets_count);
  dump_header(s, "X-Account-Object-Count", buckets_object_count);
  dump_header(s, "X-Account-Bytes-Used", buckets_size);
  dump_header(s, "X-Account-Bytes-Used-Actual", buckets_size_rounded);

  /* Dump TempURL-related stuff */
  if (s->perm_mask == RGW_PERM_FULL_CONTROL) {
    auto iter = s->user->temp_url_keys.find(0);
    if (iter != std::end(s->user->temp_url_keys) && ! iter->second.empty()) {
      dump_header(s, "X-Account-Meta-Temp-Url-Key", iter->second);
    }

    iter = s->user->temp_url_keys.find(1);
    if (iter != std::end(s->user->temp_url_keys) && ! iter->second.empty()) {
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
  string acct_acl;
  policy.to_str(acct_acl);
  if (acct_acl.size()) {
    dump_header(s, "X-Account-Access-Control", std::move(acct_acl));
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

  if (!g_conf->rgw_swift_enforce_content_length) {
    /* Adding account stats in the header to keep align with Swift API */
    dump_account_metadata(s,
            buckets_count,
            buckets_objcount,
            buckets_size,
            buckets_size_rounded,
            attrs,
            user_quota,
            static_cast<RGWAccessControlPolicy_SWIFTAcct&>(*s->user_acl));
    dump_errno(s);
    end_header(s, NULL, NULL, NO_CONTENT_LENGTH, true);
  }

  if (! op_ret) {
    dump_start(s);
    s->formatter->open_array_section_with_attrs("account",
            FormatterAttrs("name", s->user->display_name.c_str(), NULL));

    sent_data = true;
  }
}

void RGWListBuckets_ObjStore_SWIFT::send_response_data(RGWUserBuckets& buckets)
{
  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  map<string, RGWBucketEnt>::iterator iter;

  if (!sent_data)
    return;

  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt obj = iter->second;
    s->formatter->open_object_section("container");
    s->formatter->dump_string("name", obj.bucket.name);
    if (need_stats) {
      s->formatter->dump_int("count", obj.count);
      s->formatter->dump_int("bytes", obj.size);
    }
    s->formatter->close_section();
    if (!g_conf->rgw_swift_enforce_content_length) {
      rgw_flush_formatter(s, s->formatter);
    }
  }
}

void RGWListBuckets_ObjStore_SWIFT::send_response_end()
{
  if (sent_data) {
    s->formatter->close_section();
  }

  if (g_conf->rgw_swift_enforce_content_length) {
    /* Adding account stats in the header to keep align with Swift API */
    dump_account_metadata(s,
            buckets_count,
            buckets_objcount,
            buckets_size,
            buckets_size_rounded,
            attrs,
            user_quota,
            static_cast<RGWAccessControlPolicy_SWIFTAcct&>(*s->user_acl));
    dump_errno(s);
    end_header(s, NULL, NULL, s->formatter->get_len(), true);
  }

  if (sent_data || g_conf->rgw_swift_enforce_content_length) {
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWListBucket_ObjStore_SWIFT::get_params()
{
  prefix = s->info.args.get("prefix");
  marker = s->info.args.get("marker");
  end_marker = s->info.args.get("end_marker");
  max_keys = s->info.args.get("limit");
  op_ret = parse_max_keys();
  if (op_ret < 0) {
    return op_ret;
  }
  if (max > default_max)
    return -ERR_PRECONDITION_FAILED;

  delimiter = s->info.args.get("delimiter");

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
                                    const RGWBucketEnt&,
                                    const RGWQuotaInfo&,
                                    const RGWBucketWebsiteConf&);

void RGWListBucket_ObjStore_SWIFT::send_response()
{
  vector<RGWObjEnt>::iterator iter = objs.begin();
  map<string, bool>::iterator pref_iter = common_prefixes.begin();

  dump_start(s);
  dump_container_metadata(s, bucket, bucket_quota,
                          s->bucket_info.website_conf);

  s->formatter->open_array_section_with_attrs("container", FormatterAttrs("name", s->bucket.name.c_str(), NULL));

  while (iter != objs.end() || pref_iter != common_prefixes.end()) {
    bool do_pref = false;
    bool do_objs = false;
    rgw_obj_key& key = iter->key;
    if (pref_iter == common_prefixes.end())
      do_objs = true;
    else if (iter == objs.end())
      do_pref = true;
    else if (key.name.compare(pref_iter->first) == 0) {
      do_objs = true;
      ++pref_iter;
    } else if (key.name.compare(pref_iter->first) <= 0)
      do_objs = true;
    else
      do_pref = true;

    if (do_objs && (marker.empty() || marker < key)) {
      if (key.name.compare(path) == 0)
        goto next;

      s->formatter->open_object_section("object");
      s->formatter->dump_string("name", key.name);
      s->formatter->dump_string("hash", iter->etag);
      s->formatter->dump_int("bytes", iter->size);
      string single_content_type = iter->content_type;
      if (iter->content_type.size()) {
        // content type might hold multiple values, just dump the last one
        ssize_t pos = iter->content_type.rfind(',');
        if (pos > 0) {
          ++pos;
          while (single_content_type[pos] == ' ')
            ++pos;
          single_content_type = single_content_type.substr(pos);
        }
        s->formatter->dump_string("content_type", single_content_type);
      }
      dump_time(s, "last_modified", &iter->mtime);
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
}

static void dump_container_metadata(struct req_state *s,
                                    const RGWBucketEnt& bucket,
                                    const RGWQuotaInfo& quota,
                                    const RGWBucketWebsiteConf& ws_conf)
{
  /* Adding X-Timestamp to keep align with Swift API */
  dump_header(s, "X-Timestamp", utime_t(s->bucket_info.creation_time));

  dump_header(s, "X-Container-Object-Count", bucket.count);
  dump_header(s, "X-Container-Bytes-Used", bucket.size);
  dump_header(s, "X-Container-Bytes-Used-Actual", bucket.size_rounded);

  if (s->object.empty()) {
    auto swift_policy = static_cast<RGWAccessControlPolicy_SWIFT*>(s->bucket_acl);
    std::string read_acl, write_acl;
    swift_policy->to_str(read_acl, write_acl);

    if (read_acl.size()) {
      dump_header(s, "X-Container-Read", read_acl);
    }
    if (write_acl.size()) {
      dump_header(s, "X-Container-Write", write_acl);
    }
    if (!s->bucket_info.placement_rule.empty()) {
      dump_header(s, "X-Storage-Policy", s->bucket_info.placement_rule);
    }

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
  if (! s->bucket_info.swift_ver_location.empty()) {
    dump_header(s, "X-Versions-Location",
                url_encode(s->bucket_info.swift_ver_location));
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
}

void RGWStatAccount_ObjStore_SWIFT::execute()
{
  RGWStatAccount_ObjStore::execute();
  op_ret = rgw_get_user_attrs_by_uid(store, s->user->user_id, attrs);
}

void RGWStatAccount_ObjStore_SWIFT::send_response()
{
  if (op_ret >= 0) {
    op_ret = STATUS_NO_CONTENT;
    dump_account_metadata(s,
            buckets_count,
            buckets_objcount,
            buckets_size,
            buckets_size_rounded,
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
    dump_container_metadata(s, bucket, bucket_quota,
                            s->bucket_info.website_conf);
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);

  end_header(s, this, NULL, 0, true);
  dump_start(s);
}

static int get_swift_container_settings(req_state * const s,
                                        RGWRados * const store,
                                        RGWAccessControlPolicy * const policy,
                                        bool * const has_policy,
                                        RGWCORSConfiguration * const cors_config,
                                        bool * const has_cors)
{
  string read_list, write_list;

  const char * const read_attr = s->info.env->get("HTTP_X_CONTAINER_READ");
  if (read_attr) {
    read_list = read_attr;
  }
  const char * const write_attr = s->info.env->get("HTTP_X_CONTAINER_WRITE");
  if (write_attr) {
    write_list = write_attr;
  }

  *has_policy = false;

  if (read_attr || write_attr) {
    RGWAccessControlPolicy_SWIFT swift_policy(s->cct);
    const bool r = swift_policy.create(store,
                                s->user->user_id,
                                s->user->display_name,
                                read_list,
                                write_list);
    if (r != true) {
      return -EINVAL;
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
  map<string, string, ltstr_nocase>& m = s->info.env->get_map();
  map<string, string, ltstr_nocase>::const_iterator iter;
  const size_t put_prefix_len = strlen(put_prefix);
  const size_t del_prefix_len = strlen(del_prefix);

  for (iter = m.begin(); iter != m.end(); ++iter) {
    size_t prefix_len = 0;
    const char * const p = iter->first.c_str();

    if (strncasecmp(p, del_prefix, del_prefix_len) == 0) {
      /* Explicitly requested removal. */
      prefix_len = del_prefix_len;
    } else if ((strncasecmp(p, put_prefix, put_prefix_len) == 0)
	       && iter->second.empty()) {
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

  std::string vloc = s->info.env->get("HTTP_X_VERSIONS_LOCATION", "");
  if (vloc.size()) {
    /* If the Swift's versioning is globally disabled but someone wants to
     * enable it for a given container, new version of Swift will generate
     * the precondition failed error. */
    if (! s->cct->_conf->rgw_swift_versioning_enabled) {
      return -ERR_PRECONDITION_FAILED;
    }

    swift_ver_location = std::move(vloc);
  }

  return 0;
}

int RGWCreateBucket_ObjStore_SWIFT::get_params()
{
  bool has_policy;

  int r = get_swift_container_settings(s, store, &policy, &has_policy, &cors_config, &has_cors);
  if (r < 0) {
    return r;
  }

  if (!has_policy) {
    policy.create_default(s->user->user_id, s->user->display_name);
  }

  location_constraint = store->get_zonegroup().api_name;
  get_rmattrs_from_headers(s, CONT_PUT_ATTR_PREFIX,
                           CONT_REMOVE_ATTR_PREFIX, rmattr_names);
  placement_rule = s->info.env->get("HTTP_X_STORAGE_POLICY", "");

  return get_swift_versioning_settings(s, swift_ver_location);
}

void RGWCreateBucket_ObjStore_SWIFT::send_response()
{
  if (! op_ret)
    op_ret = STATUS_CREATED;
  else if (op_ret == -ERR_BUCKET_EXISTS)
    op_ret = STATUS_ACCEPTED;
  set_req_state_err(s, op_ret);
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

static int get_delete_at_param(req_state *s, real_time *delete_at)
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

  *delete_at = delat_proposal;

  return 0;
}

int RGWPutObj_ObjStore_SWIFT::verify_permission()
{
  op_ret = RGWPutObj_ObjStore::verify_permission();

  /* We have to differentiate error codes depending on whether user is
   * anonymous (401 Unauthorized) or he doesn't have necessary permissions
   * (403 Forbidden). */
  if (s->auth_identity->is_anonymous() && op_ret == -EACCES) {
    return -EPERM;
  } else {
    return op_ret;
  }
}

int RGWPutObj_ObjStore_SWIFT::get_params()
{
  if (s->has_bad_meta) {
    return -EINVAL;
  }

  if (!s->length) {
    const char *encoding = s->info.env->get("HTTP_TRANSFER_ENCODING");
    if (!encoding || strcmp(encoding, "chunked") != 0) {
      ldout(s->cct, 20) << "neither length nor chunked encoding" << dendl;
      return -ERR_LENGTH_REQUIRED;
    }

    chunked_upload = true;
  }

  supplied_etag = s->info.env->get("HTTP_ETAG");

  if (!s->generic_attrs.count(RGW_ATTR_CONTENT_TYPE)) {
    ldout(s->cct, 5) << "content type wasn't provided, trying to guess" << dendl;
    const char *suffix = strrchr(s->object.name.c_str(), '.');
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

  policy.create_default(s->user->user_id, s->user->display_name);

  int r = get_delete_at_param(s, &delete_at);
  if (r < 0) {
    ldout(s->cct, 5) << "ERROR: failed to get Delete-At param" << dendl;
    return r;
  }

  dlo_manifest = s->info.env->get("HTTP_X_OBJECT_MANIFEST");
  bool exists;
  string multipart_manifest = s->info.args.get("multipart-manifest", &exists);
  if (exists) {
    if (multipart_manifest != "put") {
      ldout(s->cct, 5) << "invalid multipart-manifest http param: " << multipart_manifest << dendl;
      return -EINVAL;
    }

#define MAX_SLO_ENTRY_SIZE (1024 + 128) // 1024 - max obj name, 128 - enough extra for other info
    uint64_t max_len = s->cct->_conf->rgw_max_slo_entries * MAX_SLO_ENTRY_SIZE;
    
    slo_info = new RGWSLOInfo;
    
    int r = rgw_rest_get_json_input_keep_data(s->cct, s, slo_info->entries, max_len, &slo_info->raw_data, &slo_info->raw_data_len);
    if (r < 0) {
      ldout(s->cct, 5) << "failed to read input for slo r=" << r << dendl;
      return r;
    }

    if ((int64_t)slo_info->entries.size() > s->cct->_conf->rgw_max_slo_entries) {
      ldout(s->cct, 5) << "too many entries in slo request: " << slo_info->entries.size() << dendl;
      return -EINVAL;
    }

    MD5 etag_sum;
    uint64_t total_size = 0;
    for (const auto& entry : slo_info->entries) {
      etag_sum.Update((const byte *)entry.etag.c_str(),
                      entry.etag.length());
      total_size += entry.size_bytes;

      ldout(s->cct, 20) << "slo_part: " << entry.path
                        << " size=" << entry.size_bytes
                        << " etag=" << entry.etag
                        << dendl;
    }
    complete_etag(etag_sum, &lo_etag);
    slo_info->total_size = total_size;

    ofs = slo_info->raw_data_len;
  }

  return RGWPutObj_ObjStore::get_params();
}

void RGWPutObj_ObjStore_SWIFT::send_response()
{
  if (! op_ret) {
    op_ret = STATUS_CREATED;
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
                                      RGWRados * const store,
                                      RGWAccessControlPolicy_SWIFTAcct * const policy,
                                      bool * const has_policy)
{
  *has_policy = false;

  const char * const acl_attr = s->info.env->get("HTTP_X_ACCOUNT_ACCESS_CONTROL");
  if (acl_attr) {
    RGWAccessControlPolicy_SWIFTAcct swift_acct_policy(s->cct);
    const bool r = swift_acct_policy.create(store,
                                     s->user->user_id,
                                     s->user->display_name,
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
  if (! op_ret) {
    op_ret = STATUS_NO_CONTENT;
  }
  set_req_state_err(s, op_ret);
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
				       &cors_config, &has_cors);
  if (r < 0) {
    return r;
  }

  get_rmattrs_from_headers(s, CONT_PUT_ATTR_PREFIX, CONT_REMOVE_ATTR_PREFIX,
			   rmattr_names);
  placement_rule = s->info.env->get("HTTP_X_STORAGE_POLICY", "");

  return get_swift_versioning_settings(s, swift_ver_location);
}

void RGWPutMetadataBucket_ObjStore_SWIFT::send_response()
{
  if (!op_ret && (op_ret != -EINVAL)) {
    op_ret = STATUS_NO_CONTENT;
  }
  set_req_state_err(s, op_ret);
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
  int r = get_delete_at_param(s, &delete_at);
  if (r < 0) {
    ldout(s->cct, 5) << "ERROR: failed to get Delete-At param" << dendl;
    return r;
  }

  placement_rule = s->info.env->get("HTTP_X_STORAGE_POLICY", "");
  dlo_manifest = s->info.env->get("HTTP_X_OBJECT_MANIFEST");

  return 0;
}

void RGWPutMetadataObject_ObjStore_SWIFT::send_response()
{
  if (! op_ret) {
    op_ret = STATUS_ACCEPTED;
  }
  set_req_state_err(s, op_ret);
  if (!s->err.is_err()) {
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
  if (s->auth_identity->is_anonymous() && op_ret == -EACCES) {
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
      path.obj_key = s->object;

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
    content_type = iter->second.c_str();
  }
}

static void dump_object_metadata(struct req_state * const s,
				 map<string, bufferlist> attrs)
{
  map<string, string> response_attrs;

  for (auto kv : attrs) {
    const char * name = kv.first.c_str();
    const auto aiter = rgw_to_http_attrs.find(name);

    if (aiter != std::end(rgw_to_http_attrs)) {
      response_attrs[aiter->second] = kv.second.c_str();
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
      ::decode(delete_at, iter->second);
      dump_header(s, "X-Delete-At", delete_at.sec());
    } catch (buffer::error& err) {
      ldout(s->cct, 0) << "ERROR: cannot decode object's " RGW_ATTR_DELETE_AT
                          " attr, ignoring"
                       << dendl;
    }
  }
}

int RGWCopyObj_ObjStore_SWIFT::init_dest_policy()
{
  dest_policy.create_default(s->user->user_id, s->user->display_name);

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
  src_object = s->src_object;
  dest_tenant_name = s->bucket_tenant;
  dest_bucket_name = s->bucket_name;
  dest_object = s->object.name;

  const char * const fresh_meta = s->info.env->get("HTTP_X_FRESH_METADATA");
  if (fresh_meta && strcasecmp(fresh_meta, "TRUE") == 0) {
    attrs_mod = RGWRados::ATTRSMOD_REPLACE;
  } else {
    attrs_mod = RGWRados::ATTRSMOD_MERGE;
  }

  int r = get_delete_at_param(s, &delete_at);
  if (r < 0) {
    ldout(s->cct, 5) << "ERROR: failed to get Delete-At param" << dendl;
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
  dump_header(s, "X-Copied-From", url_encode(src_bucket.name) +
              "/" + url_encode(src_object.name));

  /* Dump X-Copied-From-Account. */
  /* XXX tenant */
  dump_header(s, "X-Copied-From-Account", url_encode(s->user->user_id.id));

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
    dump_object_metadata(s, attrs);
    end_header(s, this, !content_type.empty() ? content_type.c_str()
	       : "binary/octet-stream");
  } else {
    s->formatter->close_section();
    rgw_flush_formatter(s, s->formatter);
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

    if (s->err.is_err()) {
      end_header(s, NULL);
      return 0;
    }
  }

  if (range_str) {
    dump_range(s, ofs, end, s->obj_size);
  }

  if (s->err.is_err()) {
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
        dump_etag(s, iter->second);
      }
    }

    get_contype_from_attrs(attrs, content_type);
    dump_object_metadata(s, attrs);
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

    ldout(s->cct, 20) << "extracted Bulk Delete entry: " << path_str << dendl;

    RGWBulkDelete::acct_path_t path;

    /* We need to skip all slashes at the beginning in order to preserve
     * compliance with Swift. */
    const size_t start_pos = path_str.find_first_not_of('/');

    if (string::npos != start_pos) {
      /* Seperator is the first slash after the leading ones. */
      const size_t sep_pos = path_str.find('/', start_pos);

      if (string::npos != sep_pos) {
        string bucket_name;
        url_decode(path_str.substr(start_pos, sep_pos - start_pos), bucket_name);

        string obj_name;
        url_decode(path_str.substr(sep_pos + 1), obj_name);

        path.bucket_name = bucket_name;
        path.obj_key = obj_name;
      } else {
        /* It's guaranteed here that bucket name is at least one character
         * long and is different than slash. */
        url_decode(path_str.substr(start_pos), path.bucket_name);
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
     << g_conf->rgw_cross_domain_policy << "\n"
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
    {"tempauth", {false, nullptr}},
};

void RGWInfo_ObjStore_SWIFT::execute()
{
  bool is_admin_info_enabled = false;

  const string& swiftinfo_sig = s->info.args.get("swiftinfo_sig");
  const string& swiftinfo_expires = s->info.args.get("swiftinfo_expires");

  if (!swiftinfo_sig.empty() &&
      !swiftinfo_expires.empty() &&
      !is_expired(swiftinfo_expires, s->cct)) {
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
      pair.second.list_data(*(s->formatter), *(s->cct->_conf), *store);
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
                                              const md_config_t& config,
                                              RGWRados& store)
{
  formatter.open_object_section("swift");
  formatter.dump_int("max_file_size", config.rgw_max_put_size);
  formatter.dump_int("container_listing_limit", RGW_LIST_BUCKETS_LIMIT_MAX);

  string ceph_version(CEPH_GIT_NICE_VER);
  formatter.dump_string("version", ceph_version);
  formatter.dump_int("max_meta_name_length", 81);

  formatter.open_array_section("policies");
  RGWZoneGroup& zonegroup = store.get_zonegroup();

  for (const auto& placement_targets : zonegroup.placement_targets) {
    formatter.open_object_section("policy");
    if (placement_targets.second.name.compare(zonegroup.default_placement) == 0)
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

void RGWInfo_ObjStore_SWIFT::list_tempurl_data(Formatter& formatter,
                                                const md_config_t& config,
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
                                            const md_config_t& config,
                                            RGWRados& store)
{
  formatter.open_object_section("slo");
  formatter.dump_int("max_manifest_segments", config.rgw_max_slo_entries);
  formatter.close_section();
}

bool RGWInfo_ObjStore_SWIFT::is_expired(const std::string& expires, CephContext* cct)
{
  string err;
  const utime_t now = ceph_clock_now(cct);
  const uint64_t expiration = (uint64_t)strict_strtoll(expires.c_str(),
                                                       10, &err);
  if (!err.empty()) {
    ldout(cct, 5) << "failed to parse siginfo_expires: " << err << dendl;
    return true;
  }

  if (expiration <= (uint64_t)now.sec()) {
    ldout(cct, 5) << "siginfo expired: " << expiration << " <= " << now.sec() << dendl;
    return true;
  }

  return false;
}

RGWOp *RGWHandler_REST_Service_SWIFT::op_get()
{
  return new RGWListBuckets_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Service_SWIFT::op_head()
{
  return new RGWStatAccount_ObjStore_SWIFT;
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
    RGWGetErrorPage(RGWRados* const store,
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

  s->object = std::to_string(http_ret) + error_doc;

  RGWOp* newop = &get_errpage_op;
  RGWRequest req(0);
  return rgw_process_authenticated(handler, newop, &req, s, true);
}

int RGWSwiftWebsiteHandler::error_handler(const int err_no,
                                          std::string* const error_content)
{
  const auto& ws_conf = s->bucket_info.website_conf;

  if (can_be_website_req() && ! ws_conf.error_doc.empty()) {
    struct rgw_err err;
    set_req_state_err(err, err_no, s->prot_flags);
    return serve_errordoc(err.http_ret, ws_conf.error_doc);
  }

  /* Let's go to the default, no-op handler. */
  return err_no;
}

bool RGWSwiftWebsiteHandler::is_web_mode() const
{
  const boost::string_ref webmode = s->info.env->get("HTTP_X_WEB_MODE", "");
  return boost::algorithm::iequals(webmode, "true");
}

bool RGWSwiftWebsiteHandler::can_be_website_req() const
{
  /* Static website works only with the GET or HEAD method. Nothing more. */
  static const std::set<boost::string_ref> ws_methods = { "GET", "HEAD" };
  if (ws_methods.count(s->info.method) == 0) {
    return false;
  }

  /* We also need to handle early failures from the auth system. In such cases
   * req_state::auth_identity may be empty. Let's treat that the same way as
   * the anonymous access. */
  if (! s->auth_identity) {
    return true;
  }

  /* Swift serves websites only for anonymous requests unless client explicitly
   * requested this behaviour by supplying X-Web-Mode HTTP header set to true. */
  if (s->auth_identity->is_anonymous() || is_web_mode()) {
    return true;
  }

  return false;
}

RGWOp* RGWSwiftWebsiteHandler::get_ws_redirect_op()
{
  class RGWMovedPermanently: public RGWOp {
    const std::string location;
  public:
    RGWMovedPermanently(const std::string& location)
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

    const string name() override {
      return "RGWMovedPermanently";
    }
  };

  return new RGWMovedPermanently(s->info.request_uri + '/');
}

RGWOp* RGWSwiftWebsiteHandler::get_ws_index_op()
{
  /* Retarget to get obj on requested index file. */
  if (! s->object.empty()) {
    s->object = s->object.name +
                s->bucket_info.website_conf.get_index_doc();
  } else {
    s->object = s->bucket_info.website_conf.get_index_doc();
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
      dump_container_metadata(s, bucket, bucket_quota,
                              s->bucket_info.website_conf);
      end_header(s, this, "text/html");
      if (op_ret < 0) {
        return;
      }

      /* Now it's the time to start generating HTML bucket listing.
       * All the crazy stuff with crafting tags will be delegated to
       * RGWSwiftWebsiteListingFormatter. */
      std::stringstream ss;
      RGWSwiftWebsiteListingFormatter htmler(ss, prefix);

      const auto& ws_conf = s->bucket_info.website_conf;
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

      for (const RGWObjEnt& obj : objs) {
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
    RGWWebsiteListing(std::string prefix_override)
      : prefix_override(std::move(prefix_override)) {
    }
  };

  std::string prefix = std::move(s->object.name);
  s->object = rgw_obj_key();

  return new RGWWebsiteListing(std::move(prefix));
}

bool RGWSwiftWebsiteHandler::is_web_dir() const
{
  std::string subdir_name;
  url_decode(s->object.name, subdir_name);

  /* Remove character from the subdir name if it is "/". */
  if (subdir_name.empty()) {
    return false;
  } else if (subdir_name.back() == '/') {
    subdir_name.pop_back();
  }

  rgw_obj obj(s->bucket, subdir_name);

  /* First, get attrset of the object we'll try to retrieve. */
  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  obj_ctx.set_atomic(obj);

  RGWObjState* state = nullptr;
  if (store->get_obj_state(&obj_ctx, obj, &state, false) < 0) {
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

  const auto& ws_conf = s->bucket_info.website_conf;
  const std::string subdir_marker = ws_conf.subdir_marker.empty()
                                      ? "application/directory"
                                      : ws_conf.subdir_marker;
  return subdir_marker == content_type && state->size <= 1;
}

bool RGWSwiftWebsiteHandler::is_index_present(const std::string& index)
{
  rgw_obj obj(s->bucket, index);

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  obj_ctx.set_atomic(obj);

  RGWObjState* state = nullptr;
  if (store->get_obj_state(&obj_ctx, obj, &state, false) < 0) {
    return false;
  }

  /* A nonexistent object cannot be a considered as a viable index. We will
   * try to list the bucket or - if this is impossible - return an error. */
  return state->exists;
}

int RGWSwiftWebsiteHandler::retarget_bucket(RGWOp* op, RGWOp** new_op)
{
  ldout(s->cct, 10) << "Starting retarget" << dendl;
  RGWOp* op_override = nullptr;

  /* In Swift static web content is served if the request is anonymous or
   * has X-Web-Mode HTTP header specified to true. */
  if (can_be_website_req()) {
    const auto& ws_conf = s->bucket_info.website_conf;
    const auto& index = s->bucket_info.website_conf.get_index_doc();

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
  ldout(s->cct, 10) << "Starting object retarget" << dendl;
  RGWOp* op_override = nullptr;

  /* In Swift static web content is served if the request is anonymous or
   * has X-Web-Mode HTTP header specified to true. */
  if (can_be_website_req() && is_web_dir()) {
    const auto& ws_conf = s->bucket_info.website_conf;
    const auto& index = s->bucket_info.website_conf.get_index_doc();

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
  return new RGWCreateBucket_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Bucket_SWIFT::op_delete()
{
  return new RGWDeleteBucket_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Bucket_SWIFT::op_post()
{
  return new RGWPutMetadataBucket_ObjStore_SWIFT;
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
  return new RGWPutMetadataObject_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_copy()
{
  return new RGWCopyObj_ObjStore_SWIFT;
}

RGWOp *RGWHandler_REST_Obj_SWIFT::op_options()
{
  return new RGWOptionsCORS_ObjStore_SWIFT;
}

int RGWHandler_REST_SWIFT::authorize()
{
  /* Factories. */
  class SwiftAuthFactory : public RGWTempURLAuthApplier::Factory,
                           public RGWLocalAuthApplier::Factory,
                           public RGWRemoteAuthApplier::Factory {
    typedef RGWAuthApplier::aplptr_t aplptr_t;

    RGWRados * const store;
    const std::string acct_override;

  public:
    SwiftAuthFactory(RGWRados * const store,
                     const std::string& acct_override)
      : store(store),
        acct_override(acct_override) {
    }

    aplptr_t create_apl_turl(CephContext * const cct,
                             const RGWUserInfo& user_info) const override {
      /* TempURL doesn't need any user account override. It's a Swift-specific
       * mechanism that requires  account name internally, so there is no
       * business with delegating the responsibility outside. */
      return aplptr_t(new RGWTempURLAuthApplier(cct, user_info));
    }

    aplptr_t create_apl_local(CephContext * const cct,
                              const RGWUserInfo& user_info,
                              const std::string& subuser) const override {
      return aplptr_t(
        new RGWThirdPartyAccountAuthApplier<RGWLocalAuthApplier>(
          RGWLocalAuthApplier(cct, user_info, subuser),
          store, acct_override));
    }

    aplptr_t create_apl_remote(CephContext * const cct,
                               RGWRemoteAuthApplier::acl_strategy_t&& acl_alg,
                               const RGWRemoteAuthApplier::AuthInfo info
                              ) const override {
      return aplptr_t(
        new RGWThirdPartyAccountAuthApplier<RGWRemoteAuthApplier>(
          RGWRemoteAuthApplier(cct, store, std::move(acl_alg), info),
          store, acct_override));
    }
  } aplfact(store, s->account_name);

  /* Extractors. */
  RGWXAuthTokenExtractor token_extr(s);

  /* Auth engines. */
  RGWTempURLAuthEngine tempurl(s, store,                    &aplfact);
  RGWSignedTokenAuthEngine rgwtk(s->cct, store, token_extr, &aplfact);
  RGWKeystoneAuthEngine keystone(s->cct,        token_extr, &aplfact);
  RGWExternalTokenAuthEngine ext(s->cct, store, token_extr, &aplfact);
  RGWAnonymousAuthEngine anoneng(s->cct,        token_extr, &aplfact);

  /* Pipeline. */
  constexpr size_t ENGINES_NUM = 5;
  const std::array<const RGWAuthEngine *, ENGINES_NUM> engines = {
    &tempurl, &rgwtk, &keystone, &ext, &anoneng
  };

  for (const auto engine : engines) {
    if (! engine->is_applicable()) {
      /* Engine said it isn't suitable for handling this particular
       * request. Let's try a next one. */
      continue;
    }

    try {
      ldout(s->cct, 5) << "trying auth engine: " << engine->get_name() << dendl;

      auto applier = engine->authenticate();
      if (! applier) {
        /* Access denied is acknowledged by returning a std::unique_ptr with
         * nullptr inside. */
        ldout(s->cct, 5) << "auth engine refused to authenicate" << dendl;
        return -EPERM;
      }

      try {
        /* Account used by a given RGWOp is decoupled from identity employed
         * in the authorization phase (RGWOp::verify_permissions). */
        applier->load_acct_info(*s->user);
        s->perm_mask = applier->get_perm_mask();

        /* This is the signle place where we pass req_state as a pointer
         * to non-const and thus its modification is allowed. In the time
         * of writing only RGWTempURLEngine needed that feature. */
        applier->modify_request_state(s);

        s->auth_identity = std::move(applier);
      } catch (int err) {
        ldout(s->cct, 5) << "applier throwed err=" << err << dendl;
        return err;
      }
    } catch (int err) {
      ldout(s->cct, 5) << "auth engine throwed err=" << err << dendl;
      return err;
    }

    /* FIXME(rzarzynski): move into separated RGWAuthApplier decorator. */
    if (s->user->system) {
      s->system_request = true;
      ldout(s->cct, 20) << "system request over Swift API" << dendl;

      rgw_user euid(s->info.args.sys_get(RGW_SYS_PARAM_PREFIX "uid"));
      if (!euid.empty()) {
        RGWUserInfo einfo;

        const int ret = rgw_get_user_info_by_uid(store, euid, einfo);
        if (ret < 0) {
          ldout(s->cct, 0) << "User lookup failed, euid=" << euid
                           << " ret=" << ret << dendl;
          return ret;
        }

        *(s->user) = einfo;
      }
    }

    return 0;
  }

  /* All engines refused to handle this authentication request by
   * returning RGWAuthEngine::Status::UNKKOWN. Rather rare case. */
  return -EPERM;
}

int RGWHandler_REST_SWIFT::postauth_init()
{
  struct req_init_state* t = &s->init_state;

  /* XXX Stub this until Swift Auth sets account into URL. */
  s->bucket_tenant = s->user->user_id.tenant;
  s->bucket_name = t->url_bucket;

  dout(10) << "s->object=" <<
    (!s->object.empty() ? s->object : rgw_obj_key("<NULL>"))
           << " s->bucket="
	   << rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name)
	   << dendl;

  int ret;
  ret = validate_tenant_name(s->bucket_tenant);
  if (ret)
    return ret;
  ret = validate_bucket_name(s->bucket_name);
  if (ret)
    return ret;
  ret = validate_object_name(s->object.name);
  if (ret)
    return ret;

  if (!t->src_bucket.empty()) {
    /*
     * We don't allow cross-tenant copy at present. It requires account
     * names in the URL for Swift.
     */
    s->src_tenant_name = s->user->user_id.tenant;
    s->src_bucket_name = t->src_bucket;

    ret = validate_bucket_name(s->src_bucket_name);
    if (ret < 0) {
      return ret;
    }
    ret = validate_object_name(s->src_object.name);
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

int RGWHandler_REST_SWIFT::validate_bucket_name(const string& bucket)
{
  int ret = RGWHandler_REST::validate_bucket_name(bucket);
  if (ret < 0)
    return ret;

  int len = bucket.size();

  if (len == 0)
    return 0;

  if (bucket[0] == '.')
    return -ERR_INVALID_BUCKET_NAME;

  if (check_utf8(bucket.c_str(), len))
    return -ERR_INVALID_UTF8;

  const char *s = bucket.c_str();

  for (int i = 0; i < len; ++i, ++s) {
    if (*(unsigned char *)s == 0xff)
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

int RGWHandler_REST_SWIFT::init_from_header(struct req_state* const s,
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
    return g_conf->rgw_swift_url_prefix == "/" ? -ERR_BAD_URL : 0;
  }

  req = req_name;

  size_t pos = req.find('/');
  if (std::string::npos != pos && g_conf->rgw_swift_url_prefix != "/") {
    bool cut_url = g_conf->rgw_swift_url_prefix.length();
    first = req.substr(0, pos);

    if (first.compare(g_conf->rgw_swift_url_prefix) == 0) {
      if (cut_url) {
        /* Rewind to the "v1/..." part. */
        next_tok(req, first, '/');
      }
    }
  } else if (req.compare(g_conf->rgw_swift_url_prefix) == 0) {
    s->formatter = new RGWFormatter_Plain;
    return -ERR_BAD_URL;
  } else {
    first = req;
  }

  std::string tenant_path;
  if (! g_conf->rgw_swift_tenant_name.empty()) {
    tenant_path = "/AUTH_";
    tenant_path.append(g_conf->rgw_swift_tenant_name);
  }

  /* verify that the request_uri conforms with what's expected */
  char buf[g_conf->rgw_swift_url_prefix.length() + 16 + tenant_path.length()];
  int blen;
  if (g_conf->rgw_swift_url_prefix == "/") {
    blen = sprintf(buf, "/v1%s", tenant_path.c_str());
  } else {
    blen = sprintf(buf, "/%s/v1%s",
                   g_conf->rgw_swift_url_prefix.c_str(), tenant_path.c_str());
  }

  if (strncmp(reqbuf, buf, blen) != 0) {
    return -ENOENT;
  }

  int ret = allocate_formatter(s, RGW_FORMAT_PLAIN, true);
  if (ret < 0)
    return ret;

  string ver;

  next_tok(req, ver, '/');

  if (!tenant_path.empty() || g_conf->rgw_swift_account_in_url) {
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
    s->object =
      rgw_obj_key(req, s->info.env->get("HTTP_X_OBJECT_VERSION_ID", "")); /* rgw swift extension */
    s->info.effective_uri.append("/" + s->object.name);
  }

  return 0;
}

int RGWHandler_REST_SWIFT::init(RGWRados* store, struct req_state* s,
				rgw::io::BasicClient *cio)
{
  struct req_init_state *t = &s->init_state;

  s->dialect = "swift";

  const char *copy_source = s->info.env->get("HTTP_X_COPY_FROM");
  if (copy_source) {
    bool result = RGWCopyObj::parse_copy_location(copy_source, t->src_bucket,
						  s->src_object);
    if (!result)
      return -ERR_BAD_URL;
  }

  if (s->op == OP_COPY) {
    const char *req_dest = s->info.env->get("HTTP_DESTINATION");
    if (!req_dest)
      return -ERR_BAD_URL;

    string dest_bucket_name;
    rgw_obj_key dest_obj_key;
    bool result =
      RGWCopyObj::parse_copy_location(req_dest, dest_bucket_name,
				      dest_obj_key);
    if (!result)
       return -ERR_BAD_URL;

    string dest_object = dest_obj_key.name;

    /* convert COPY operation into PUT */
    t->src_bucket = t->url_bucket;
    s->src_object = s->object;
    t->url_bucket = dest_bucket_name;
    s->object = rgw_obj_key(dest_object);
    s->op = OP_PUT;
  }

  return RGWHandler_REST::init(store, s, cio);
}

RGWHandler_REST* RGWRESTMgr_SWIFT::get_handler(struct req_state* const s,
                                               const std::string& frontend_prefix)
{
  int ret = RGWHandler_REST_SWIFT::init_from_header(s, frontend_prefix);
  if (ret < 0) {
    ldout(s->cct, 10) << "init_from_header returned err=" << ret <<  dendl;
    return nullptr;
  }

  if (s->init_state.url_bucket.empty()) {
    return new RGWHandler_REST_Service_SWIFT;
  }

  if (s->object.empty()) {
    return new RGWHandler_REST_Bucket_SWIFT;
  }

  return new RGWHandler_REST_Obj_SWIFT;
}

RGWHandler_REST* RGWRESTMgr_SWIFT_Info::get_handler(
  struct req_state* const s,
  const std::string& frontend_prefix)
{
  s->prot_flags |= RGW_REST_SWIFT;
  return new RGWHandler_REST_SWIFT_Info;
}
