// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <array>
#include <string.h>

#include "common/ceph_crypto.h"
#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/safe_io.h"
#include "common/backport14.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/utility/string_view.hpp>

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_s3website.h"
#include "rgw_auth_s3.h"
#include "rgw_acl.h"
#include "rgw_policy_s3.h"
#include "rgw_user.h"
#include "rgw_cors.h"
#include "rgw_cors_s3.h"
#include "rgw_tag_s3.h"

#include "rgw_client_io.h"

#include "rgw_keystone.h"
#include "rgw_auth_keystone.h"
#include "rgw_auth_registry.h"

#include "rgw_es_query.h"

#include <typeinfo> // for 'typeid'

#include "rgw_ldap.h"
#include "rgw_token.h"
#include "rgw_rest_role.h"
#include "rgw_crypt.h"
#include "rgw_crypt_sanitize.h"

#include "include/assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace rgw;
using namespace ceph::crypto;

using std::get;

void list_all_buckets_start(struct req_state *s)
{
  s->formatter->open_array_section_in_ns("ListAllMyBucketsResult", XMLNS_AWS_S3);
}

void list_all_buckets_end(struct req_state *s)
{
  s->formatter->close_section();
}

void dump_bucket(struct req_state *s, RGWBucketEnt& obj)
{
  s->formatter->open_object_section("Bucket");
  s->formatter->dump_string("Name", obj.bucket.name);
  dump_time(s, "CreationDate", &obj.creation_time);
  s->formatter->close_section();
}

void rgw_get_errno_s3(rgw_http_error *e , int err_no)
{
  rgw_http_errors::const_iterator r = rgw_http_s3_errors.find(err_no);

  if (r != rgw_http_s3_errors.end()) {
    e->http_ret = r->second.first;
    e->s3_code = r->second.second;
  } else {
    e->http_ret = 500;
    e->s3_code = "UnknownError";
  }
}

struct response_attr_param {
  const char *param;
  const char *http_attr;
};

static struct response_attr_param resp_attr_params[] = {
  {"response-content-type", "Content-Type"},
  {"response-content-language", "Content-Language"},
  {"response-expires", "Expires"},
  {"response-cache-control", "Cache-Control"},
  {"response-content-disposition", "Content-Disposition"},
  {"response-content-encoding", "Content-Encoding"},
  {NULL, NULL},
};

int RGWGetObj_ObjStore_S3Website::send_response_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  map<string, bufferlist>::iterator iter;
  iter = attrs.find(RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION);
  if (iter != attrs.end()) {
    bufferlist &bl = iter->second;
    s->redirect = string(bl.c_str(), bl.length());
    s->err.http_ret = 301;
    ldout(s->cct, 20) << __CEPH_ASSERT_FUNCTION << " redirecting per x-amz-website-redirect-location=" << s->redirect << dendl;
    op_ret = -ERR_WEBSITE_REDIRECT;
    set_req_state_err(s, op_ret);
    dump_errno(s);
    dump_content_length(s, 0);
    dump_redirect(s, s->redirect);
    end_header(s, this);
    return op_ret;
  } else {
    return RGWGetObj_ObjStore_S3::send_response_data(bl, bl_ofs, bl_len);
  }
}

int RGWGetObj_ObjStore_S3Website::send_response_data_error()
{
  return RGWGetObj_ObjStore_S3::send_response_data_error();
}

int RGWGetObj_ObjStore_S3::get_params()
{
  // for multisite sync requests, only read the slo manifest itself, rather than
  // all of the data from its parts. the parts will sync as separate objects
  skip_manifest = s->info.args.exists(RGW_SYS_PARAM_PREFIX "sync-manifest");

  return RGWGetObj_ObjStore::get_params();
}

int RGWGetObj_ObjStore_S3::send_response_data_error()
{
  bufferlist bl;
  return send_response_data(bl, 0 , 0);
}

template <class T>
int decode_attr_bl_single_value(map<string, bufferlist>& attrs, const char *attr_name, T *result, T def_val)
{
  map<string, bufferlist>::iterator iter = attrs.find(attr_name);
  if (iter == attrs.end()) {
    *result = def_val;
    return 0;
  }
  bufferlist& bl = iter->second;
  if (bl.length() == 0) {
    *result = def_val;
    return 0;
  }
  bufferlist::iterator bliter = bl.begin();
  try {
    ::decode(*result, bliter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}

int RGWGetObj_ObjStore_S3::send_response_data(bufferlist& bl, off_t bl_ofs,
					      off_t bl_len)
{
  const char *content_type = NULL;
  string content_type_str;
  map<string, string> response_attrs;
  map<string, string>::iterator riter;
  bufferlist metadata_bl;

  if (sent_header)
    goto send_data;

  if (custom_http_ret) {
    set_req_state_err(s, 0);
    dump_errno(s, custom_http_ret);
  } else {
    set_req_state_err(s, (partial_content && !op_ret) ? STATUS_PARTIAL_CONTENT
                  : op_ret);
    dump_errno(s);
  }

  if (op_ret)
    goto done;

  if (range_str)
    dump_range(s, start, end, s->obj_size);

  if (s->system_request &&
      s->info.args.exists(RGW_SYS_PARAM_PREFIX "prepend-metadata")) {

    dump_header(s, "Rgwx-Object-Size", (long long)total_len);

    if (rgwx_stat) {
      /*
       * in this case, we're not returning the object's content, only the prepended
       * extra metadata
       */
      total_len = 0;
    }

    /* JSON encode object metadata */
    JSONFormatter jf;
    jf.open_object_section("obj_metadata");
    encode_json("attrs", attrs, &jf);
    utime_t ut(lastmod);
    encode_json("mtime", ut, &jf);
    jf.close_section();
    stringstream ss;
    jf.flush(ss);
    metadata_bl.append(ss.str());
    dump_header(s, "Rgwx-Embedded-Metadata-Len", metadata_bl.length());
    total_len += metadata_bl.length();
  }

  if (s->system_request && !real_clock::is_zero(lastmod)) {
    /* we end up dumping mtime in two different methods, a bit redundant */
    dump_epoch_header(s, "Rgwx-Mtime", lastmod);
    uint64_t pg_ver = 0;
    int r = decode_attr_bl_single_value(attrs, RGW_ATTR_PG_VER, &pg_ver, (uint64_t)0);
    if (r < 0) {
      ldout(s->cct, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
    }
    dump_header(s, "Rgwx-Obj-PG-Ver", pg_ver);

    uint32_t source_zone_short_id = 0;
    r = decode_attr_bl_single_value(attrs, RGW_ATTR_SOURCE_ZONE, &source_zone_short_id, (uint32_t)0);
    if (r < 0) {
      ldout(s->cct, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
    }
    if (source_zone_short_id != 0) {
      dump_header(s, "Rgwx-Source-Zone-Short-Id", source_zone_short_id);
    }
  }

  for (auto &it : crypt_http_responses)
    dump_header(s, it.first, it.second);

  dump_content_length(s, total_len);
  dump_last_modified(s, lastmod);
  if (!version_id.empty()) {
    dump_header(s, "x-amz-version-id", version_id);
  }
  

  if (! op_ret) {
    if (! lo_etag.empty()) {
      /* Handle etag of Swift API's large objects (DLO/SLO). It's entirerly
       * legit to perform GET on them through S3 API. In such situation,
       * a client should receive the composited content with corresponding
       * etag value. */
      dump_etag(s, lo_etag);
    } else {
      auto iter = attrs.find(RGW_ATTR_ETAG);
      if (iter != attrs.end()) {
        dump_etag(s, iter->second);
      }
    }

    for (struct response_attr_param *p = resp_attr_params; p->param; p++) {
      bool exists;
      string val = s->info.args.get(p->param, &exists);
      if (exists) {
	if (strcmp(p->param, "response-content-type") != 0) {
	  response_attrs[p->http_attr] = val;
	} else {
	  content_type_str = val;
	  content_type = content_type_str.c_str();
	}
      }
    }

    for (auto iter = attrs.begin(); iter != attrs.end(); ++iter) {
      const char *name = iter->first.c_str();
      map<string, string>::iterator aiter = rgw_to_http_attrs.find(name);
      if (aiter != rgw_to_http_attrs.end()) {
        if (response_attrs.count(aiter->second) == 0) {
          /* Was not already overridden by a response param. */
          response_attrs[aiter->second] = iter->second.c_str();
        }
      } else if (iter->first.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
        /* Special handling for content_type. */
        if (!content_type) {
          content_type = iter->second.c_str();
        }
      } else if (strcmp(name, RGW_ATTR_SLO_UINDICATOR) == 0) {
        // this attr has an extra length prefix from ::encode() in prior versions
        dump_header(s, "X-Object-Meta-Static-Large-Object", "True");
      } else if (strncmp(name, RGW_ATTR_META_PREFIX,
			 sizeof(RGW_ATTR_META_PREFIX)-1) == 0) {
        /* User custom metadata. */
        name += sizeof(RGW_ATTR_PREFIX) - 1;
        dump_header(s, name, iter->second);
      } else if (iter->first.compare(RGW_ATTR_TAGS) == 0) {
        RGWObjTags obj_tags;
        try{
          bufferlist::iterator it = iter->second.begin();
          obj_tags.decode(it);
        } catch (buffer::error &err) {
          ldout(s->cct,0) << "Error caught buffer::error couldn't decode TagSet " << dendl;
        }
        dump_header(s, RGW_AMZ_TAG_COUNT, obj_tags.count());
      }
    }
  }

done:
  for (riter = response_attrs.begin(); riter != response_attrs.end();
       ++riter) {
    dump_header(s, riter->first, riter->second);
  }

  if (op_ret == -ERR_NOT_MODIFIED) {
      end_header(s, this);
  } else {
      if (!content_type)
          content_type = "binary/octet-stream";

      end_header(s, this, content_type);
  }

  if (metadata_bl.length()) {
    dump_body(s, metadata_bl);
  }
  sent_header = true;

send_data:
  if (get_data && !op_ret) {
    int r = dump_body(s, bl.c_str() + bl_ofs, bl_len);
    if (r < 0)
      return r;
  }

  return 0;
}

int RGWGetObj_ObjStore_S3::get_decrypt_filter(std::unique_ptr<RGWGetDataCB> *filter, RGWGetDataCB* cb, bufferlist* manifest_bl)
{
  int res = 0;
  std::unique_ptr<BlockCrypt> block_crypt;
  res = rgw_s3_prepare_decrypt(s, attrs, &block_crypt, crypt_http_responses);
  if (res == 0) {
    if (block_crypt != nullptr) {
      auto f = std::unique_ptr<RGWGetObj_BlockDecrypt>(new RGWGetObj_BlockDecrypt(s->cct, cb, std::move(block_crypt)));
      //RGWGetObj_BlockDecrypt* f = new RGWGetObj_BlockDecrypt(s->cct, cb, std::move(block_crypt));
      if (f != nullptr) {
        if (manifest_bl != nullptr) {
          res = f->read_manifest(*manifest_bl);
          if (res == 0) {
            *filter = std::move(f);
          }
        }
      }
    }
  }
  return res;
}

void RGWGetObjTags_ObjStore_S3::send_response_data(bufferlist& bl)
{
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  s->formatter->open_object_section_in_ns("Tagging", XMLNS_AWS_S3);
  s->formatter->open_object_section("TagSet");
  if (has_tags){
    RGWObjTagSet_S3 tagset;
    bufferlist::iterator iter = bl.begin();
    try {
      tagset.decode(iter);
    } catch (buffer::error& err) {
      ldout(s->cct,0) << "ERROR: caught buffer::error, couldn't decode TagSet" << dendl;
      op_ret= -EIO;
      return;
    }
    tagset.dump_xml(s->formatter);
  }
  s->formatter->close_section();
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}


int RGWPutObjTags_ObjStore_S3::get_params()
{
  RGWObjTagsXMLParser parser;

  if (!parser.init()){
    return -EINVAL;
  }

  char *data=nullptr;
  int len=0;

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r = rgw_rest_read_all_input(s, &data, &len, max_size, false);

  if (r < 0)
    return r;

  auto data_deleter = std::unique_ptr<char, decltype(free)*>{data, free};

  if (!parser.parse(data, len, 1)) {
    return -ERR_MALFORMED_XML;
  }

  RGWObjTagSet_S3 *obj_tags_s3;
  RGWObjTagging_S3 *tagging;

  tagging = static_cast<RGWObjTagging_S3 *>(parser.find_first("Tagging"));
  obj_tags_s3 = static_cast<RGWObjTagSet_S3 *>(tagging->find_first("TagSet"));
  if(!obj_tags_s3){
    return -ERR_MALFORMED_XML;
  }

  RGWObjTags obj_tags;
  r = obj_tags_s3->rebuild(obj_tags);
  if (r < 0)
    return r;

  obj_tags.encode(tags_bl);
  ldout(s->cct, 20) << "Read " << obj_tags.count() << "tags" << dendl;

  return 0;
}

void RGWPutObjTags_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

}

void RGWDeleteObjTags_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (r == -ENOENT)
    r = 0;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s, this);
}

void RGWListBuckets_ObjStore_S3::send_response_begin(bool has_buckets)
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  dump_start(s);
  end_header(s, NULL, "application/xml");

  if (! op_ret) {
    list_all_buckets_start(s);
    dump_owner(s, s->user->user_id, s->user->display_name);
    s->formatter->open_array_section("Buckets");
    sent_data = true;
  }
}

void RGWListBuckets_ObjStore_S3::send_response_data(RGWUserBuckets& buckets)
{
  if (!sent_data)
    return;

  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  map<string, RGWBucketEnt>::iterator iter;

  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt obj = iter->second;
    dump_bucket(s, obj);
  }
  rgw_flush_formatter(s, s->formatter);
}

void RGWListBuckets_ObjStore_S3::send_response_end()
{
  if (sent_data) {
    s->formatter->close_section();
    list_all_buckets_end(s);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWGetUsage_ObjStore_S3::get_params()
{
  start_date = s->info.args.get("start-date");
  end_date = s->info.args.get("end-date"); 
  return 0;
}

static void dump_usage_categories_info(Formatter *formatter, const rgw_usage_log_entry& entry, map<string, bool> *categories)
{
  formatter->open_array_section("categories");
  map<string, rgw_usage_data>::const_iterator uiter;
  for (uiter = entry.usage_map.begin(); uiter != entry.usage_map.end(); ++uiter) {
    if (categories && !categories->empty() && !categories->count(uiter->first))
      continue;
    const rgw_usage_data& usage = uiter->second;
    formatter->open_object_section("Entry");
    formatter->dump_string("Category", uiter->first);
    formatter->dump_int("BytesSent", usage.bytes_sent);
    formatter->dump_int("BytesReceived", usage.bytes_received);
    formatter->dump_int("Ops", usage.ops);
    formatter->dump_int("SuccessfulOps", usage.successful_ops);
    formatter->close_section(); // Entry
  }
  formatter->close_section(); // Category
}

void RGWGetUsage_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    set_req_state_err(s, op_ret);
  dump_errno(s);

  end_header(s, this, "application/xml");
  dump_start(s);
  if (op_ret < 0)
    return;

  Formatter *formatter = s->formatter;
  string last_owner;
  bool user_section_open = false;
  
  formatter->open_object_section("Usage");
  if (show_log_entries) {
    formatter->open_array_section("Entries");
  }
  map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;
  for (iter = usage.begin(); iter != usage.end(); ++iter) {
    const rgw_user_bucket& ub = iter->first;
    const rgw_usage_log_entry& entry = iter->second;

    if (show_log_entries) {
      if (ub.user.compare(last_owner) != 0) {
        if (user_section_open) {
          formatter->close_section();
          formatter->close_section();
        }
        formatter->open_object_section("User");
        formatter->dump_string("Owner", ub.user);
        formatter->open_array_section("Buckets");
        user_section_open = true;
        last_owner = ub.user;
      }
      formatter->open_object_section("Bucket");
      formatter->dump_string("Bucket", ub.bucket);
      utime_t ut(entry.epoch, 0);
      ut.gmtime(formatter->dump_stream("Time"));
      formatter->dump_int("Epoch", entry.epoch);
      dump_usage_categories_info(formatter, entry, &categories);
      formatter->close_section(); // bucket
    }

    summary_map[ub.user].aggregate(entry, &categories);
  }

  if (show_log_entries) {
     if (user_section_open) {
       formatter->close_section(); // buckets
       formatter->close_section(); //user
     }
     formatter->close_section(); // entries
   }
  
   if (show_log_sum) {
     formatter->open_array_section("Summary");
     map<string, rgw_usage_log_entry>::iterator siter;
     for (siter = summary_map.begin(); siter != summary_map.end(); ++siter) {
       const rgw_usage_log_entry& entry = siter->second;
       formatter->open_object_section("User");
       formatter->dump_string("User", siter->first);
       dump_usage_categories_info(formatter, entry, &categories);
       rgw_usage_data total_usage;
       entry.sum(total_usage, categories);
       formatter->open_object_section("Total");
       formatter->dump_int("BytesSent", total_usage.bytes_sent);
       formatter->dump_int("BytesReceived", total_usage.bytes_received);
       formatter->dump_int("Ops", total_usage.ops);
       formatter->dump_int("SuccessfulOps", total_usage.successful_ops);
       formatter->close_section(); // total 
       formatter->close_section(); // user
     }

     if (s->cct->_conf->rgw_rest_getusage_op_compat) {
       formatter->open_object_section("Stats"); 
     }	
       
     formatter->dump_int("TotalBytes", header.stats.total_bytes);
     formatter->dump_int("TotalBytesRounded", header.stats.total_bytes_rounded);
     formatter->dump_int("TotalEntries", header.stats.total_entries);
     
     if (s->cct->_conf->rgw_rest_getusage_op_compat) { 
       formatter->close_section(); //Stats
     }

     formatter->close_section(); // summary
   }
   formatter->close_section(); // usage
   rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWListBucket_ObjStore_S3::get_params()
{
  list_versions = s->info.args.exists("versions");
  prefix = s->info.args.get("prefix");
  if (!list_versions) {
    marker = s->info.args.get("marker");
  } else {
    marker.name = s->info.args.get("key-marker");
    marker.instance = s->info.args.get("version-id-marker");
  }
  max_keys = s->info.args.get("max-keys");
  op_ret = parse_max_keys();
  if (op_ret < 0) {
    return op_ret;
  }
  delimiter = s->info.args.get("delimiter");
  encoding_type = s->info.args.get("encoding-type");
  if (s->system_request) {
    s->info.args.get_bool("objs-container", &objs_container, false);
    const char *shard_id_str = s->info.env->get("HTTP_RGWX_SHARD_ID");
    if (shard_id_str) {
      string err;
      shard_id = strict_strtol(shard_id_str, 10, &err);
      if (!err.empty()) {
        ldout(s->cct, 5) << "bad shard id specified: " << shard_id_str << dendl;
        return -EINVAL;
      }
    } else {
      shard_id = s->bucket_instance_shard_id;
    }
  }
  return 0;
}

void RGWListBucket_ObjStore_S3::send_versioned_response()
{
  s->formatter->open_object_section_in_ns("ListVersionsResult", XMLNS_AWS_S3);
  if (!s->bucket_tenant.empty())
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  s->formatter->dump_string("Name", s->bucket_name);
  s->formatter->dump_string("Prefix", prefix);
  s->formatter->dump_string("KeyMarker", marker.name);
  s->formatter->dump_string("VersionIdMarker", marker.instance);
  if (is_truncated && !next_marker.empty()) {
    s->formatter->dump_string("NextKeyMarker", next_marker.name);
    s->formatter->dump_string("NextVersionIdMarker", next_marker.instance);
  }
  s->formatter->dump_int("MaxKeys", max);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);

  s->formatter->dump_string("IsTruncated", (max && is_truncated ? "true"
					    : "false"));

  bool encode_key = false;
  if (strcasecmp(encoding_type.c_str(), "url") == 0) {
    s->formatter->dump_string("EncodingType", "url");
    encode_key = true;
  }

  if (op_ret >= 0) {
    if (objs_container) {
      s->formatter->open_array_section("Entries");
    }

    vector<rgw_bucket_dir_entry>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      const char *section_name = (iter->is_delete_marker() ? "DeleteMarker"
				  : "Version");
      s->formatter->open_object_section(section_name);
      if (objs_container) {
        s->formatter->dump_bool("IsDeleteMarker", iter->is_delete_marker());
      }
      rgw_obj_key key(iter->key);
      if (encode_key) {
	string key_name;
	url_encode(key.name, key_name);
	s->formatter->dump_string("Key", key_name);
      } else {
	s->formatter->dump_string("Key", key.name);
      }
      string version_id = key.instance;
      if (version_id.empty()) {
	version_id = "null";
      }
      if (s->system_request) {
        if (iter->versioned_epoch > 0) {
          s->formatter->dump_int("VersionedEpoch", iter->versioned_epoch);
        }
        s->formatter->dump_string("RgwxTag", iter->tag);
        utime_t ut(iter->meta.mtime);
        ut.gmtime_nsec(s->formatter->dump_stream("RgwxMtime"));
      }
      s->formatter->dump_string("VersionId", version_id);
      s->formatter->dump_bool("IsLatest", iter->is_current());
      dump_time(s, "LastModified", &iter->meta.mtime);
      if (!iter->is_delete_marker()) {
	s->formatter->dump_format("ETag", "\"%s\"", iter->meta.etag.c_str());
	s->formatter->dump_int("Size", iter->meta.accounted_size);
	s->formatter->dump_string("StorageClass", "STANDARD");
      }
      dump_owner(s, iter->meta.owner, iter->meta.owner_display_name);
      s->formatter->close_section();
    }
    if (objs_container) {
      s->formatter->close_section();
    }

    if (!common_prefixes.empty()) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
	   pref_iter != common_prefixes.end(); ++pref_iter) {
	s->formatter->open_array_section("CommonPrefixes");
	s->formatter->dump_string("Prefix", pref_iter->first);
	s->formatter->close_section();
      }
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWListBucket_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    set_req_state_err(s, op_ret);
  dump_errno(s);

  end_header(s, this, "application/xml");
  dump_start(s);
  if (op_ret < 0)
    return;

  if (list_versions) {
    send_versioned_response();
    return;
  }

  s->formatter->open_object_section_in_ns("ListBucketResult", XMLNS_AWS_S3);
  if (!s->bucket_tenant.empty())
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  s->formatter->dump_string("Name", s->bucket_name);
  s->formatter->dump_string("Prefix", prefix);
  s->formatter->dump_string("Marker", marker.name);
  if (is_truncated && !next_marker.empty())
    s->formatter->dump_string("NextMarker", next_marker.name);
  s->formatter->dump_int("MaxKeys", max);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);

  s->formatter->dump_string("IsTruncated", (max && is_truncated ? "true"
					    : "false"));

  bool encode_key = false;
  if (strcasecmp(encoding_type.c_str(), "url") == 0) {
    s->formatter->dump_string("EncodingType", "url");
    encode_key = true;
  }

  if (op_ret >= 0) {
    vector<rgw_bucket_dir_entry>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      rgw_obj_key key(iter->key);
      s->formatter->open_array_section("Contents");
      if (encode_key) {
	string key_name;
	url_encode(key.name, key_name);
	s->formatter->dump_string("Key", key_name);
      } else {
	s->formatter->dump_string("Key", key.name);
      }
      dump_time(s, "LastModified", &iter->meta.mtime);
      s->formatter->dump_format("ETag", "\"%s\"", iter->meta.etag.c_str());
      s->formatter->dump_int("Size", iter->meta.accounted_size);
      s->formatter->dump_string("StorageClass", "STANDARD");
      dump_owner(s, iter->meta.owner, iter->meta.owner_display_name);
      if (s->system_request) {
        s->formatter->dump_string("RgwxTag", iter->tag);
      }
      s->formatter->close_section();
    }
    if (!common_prefixes.empty()) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
	   pref_iter != common_prefixes.end(); ++pref_iter) {
	s->formatter->open_array_section("CommonPrefixes");
	s->formatter->dump_string("Prefix", pref_iter->first);
	s->formatter->close_section();
      }
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketLogging_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  s->formatter->open_object_section_in_ns("BucketLoggingStatus", XMLNS_AWS_S3);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketLocation_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this);
  dump_start(s);

  RGWZoneGroup zonegroup;
  string api_name;

  int ret = store->get_zonegroup(s->bucket_info.zonegroup, zonegroup);
  if (ret >= 0) {
    api_name = zonegroup.api_name;
  } else  {
    if (s->bucket_info.zonegroup != "default") {
      api_name = s->bucket_info.zonegroup;
    }
  }

  s->formatter->dump_format_ns("LocationConstraint", XMLNS_AWS_S3,
			       "%s", api_name.c_str());
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketVersioning_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  s->formatter->open_object_section_in_ns("VersioningConfiguration", XMLNS_AWS_S3);
  if (versioned) {
    const char *status = (versioning_enabled ? "Enabled" : "Suspended");
    s->formatter->dump_string("Status", status);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

class RGWSetBucketVersioningParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) override {
    return new XMLObj;
  }

public:
  RGWSetBucketVersioningParser() {}
  ~RGWSetBucketVersioningParser() override {}

  int get_versioning_status(bool *status) {
    XMLObj *config = find_first("VersioningConfiguration");
    if (!config)
      return -EINVAL;

    *status = false;

    XMLObj *field = config->find_first("Status");
    if (!field)
      return 0;

    string& s = field->get_data();

    if (stringcasecmp(s, "Enabled") == 0) {
      *status = true;
    } else if (stringcasecmp(s, "Suspended") != 0) {
      return -EINVAL;
    }

    return 0;
  }
};

int RGWSetBucketVersioning_ObjStore_S3::get_params()
{
  char *data = nullptr;
  int len = 0;
  int r =
    rgw_rest_read_all_input(s, &data, &len, s->cct->_conf->rgw_max_put_param_size, false);
  if (r < 0) {
    return r;
  }
  
  auto data_deleter = std::unique_ptr<char, decltype(free)*>{data, free};

  r = do_aws4_auth_completion();
  if (r < 0) {
    return r;
  }

  RGWSetBucketVersioningParser parser;

  if (!parser.init()) {
    ldout(s->cct, 0) << "ERROR: failed to initialize parser" << dendl;
    r = -EIO;
    return r;
  }

  if (!parser.parse(data, len, 1)) {
    ldout(s->cct, 10) << "failed to parse data: " << data << dendl;
    r = -EINVAL;
    return r;
  }

  if (!store->is_meta_master()) {
    /* only need to keep this data around if we're not meta master */
    in_data.append(data, len);
  }

  r = parser.get_versioning_status(&enable_versioning);
  
  return r;
}

void RGWSetBucketVersioning_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);
}

int RGWSetBucketWebsite_ObjStore_S3::get_params()
{
  char *data = nullptr;
  int len = 0;
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r = rgw_rest_read_all_input(s, &data, &len, max_size, false);

  if (r < 0) {
    return r;
  }

  auto data_deleter = std::unique_ptr<char, decltype(free)*>{data, free};

  r = do_aws4_auth_completion();
  if (r < 0) {
    return r;
  }

  bufferptr in_ptr(data, len);
  in_data.append(in_ptr);

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldout(s->cct, 0) << "ERROR: failed to initialize parser" << dendl;
    return -EIO;
  }

  if (!parser.parse(data, len, 1)) {
    string str(data, len);
    ldout(s->cct, 5) << "failed to parse xml: " << str << dendl;
    return -EINVAL;
  }

  try {
    RGWXMLDecoder::decode_xml("WebsiteConfiguration", website_conf, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    string str(data, len);
    ldout(s->cct, 5) << "unexpected xml: " << str << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWSetBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);
}

void RGWDeleteBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret == 0) {
    op_ret = STATUS_NO_CONTENT;
  }
  set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);
}

void RGWGetBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  if (op_ret < 0) {
    return;
  }

  RGWBucketWebsiteConf& conf = s->bucket_info.website_conf;

  s->formatter->open_object_section_in_ns("WebsiteConfiguration", XMLNS_AWS_S3);
  conf.dump_xml(s->formatter);
  s->formatter->close_section(); // WebsiteConfiguration
  rgw_flush_formatter_and_reset(s, s->formatter);
}

static void dump_bucket_metadata(struct req_state *s, RGWBucketEnt& bucket)
{
  dump_header(s, "X-RGW-Object-Count", static_cast<long long>(bucket.count));
  dump_header(s, "X-RGW-Bytes-Used", static_cast<long long>(bucket.size));
}

void RGWStatBucket_ObjStore_S3::send_response()
{
  if (op_ret >= 0) {
    dump_bucket_metadata(s, bucket);
  }

  set_req_state_err(s, op_ret);
  dump_errno(s);

  end_header(s, this);
  dump_start(s);
}

static int create_s3_policy(struct req_state *s, RGWRados *store,
			    RGWAccessControlPolicy_S3& s3policy,
			    ACLOwner& owner)
{
  if (s->has_acl_header) {
    if (!s->canned_acl.empty())
      return -ERR_INVALID_REQUEST;

    return s3policy.create_from_headers(store, s->info.env, owner);
  }

  return s3policy.create_canned(owner, s->bucket_owner, s->canned_acl);
}

class RGWLocationConstraint : public XMLObj
{
public:
  RGWLocationConstraint() {}
  ~RGWLocationConstraint() override {}
  bool xml_end(const char *el) override {
    if (!el)
      return false;

    location_constraint = get_data();

    return true;
  }

  string location_constraint;
};

class RGWCreateBucketConfig : public XMLObj
{
public:
  RGWCreateBucketConfig() {}
  ~RGWCreateBucketConfig() override {}
};

class RGWCreateBucketParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) override {
    return new XMLObj;
  }

public:
  RGWCreateBucketParser() {}
  ~RGWCreateBucketParser() override {}

  bool get_location_constraint(string& zone_group) {
    XMLObj *config = find_first("CreateBucketConfiguration");
    if (!config)
      return false;

    XMLObj *constraint = config->find_first("LocationConstraint");
    if (!constraint)
      return false;

    zone_group = constraint->get_data();

    return true;
  }
};

int RGWCreateBucket_ObjStore_S3::get_params()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);

  int r = create_s3_policy(s, store, s3policy, s->owner);
  if (r < 0)
    return r;

  policy = s3policy;

  int len = 0;
  char *data = nullptr;

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  op_ret = rgw_rest_read_all_input(s, &data, &len, max_size, false);

  if ((op_ret < 0) && (op_ret != -ERR_LENGTH_REQUIRED))
    return op_ret;

  auto data_deleter = std::unique_ptr<char, decltype(free)*>{data, free};

  const int auth_ret = do_aws4_auth_completion();
  if (auth_ret < 0) {
    return auth_ret;
  }
  
  bufferptr in_ptr(data, len);
  in_data.append(in_ptr);

  if (len) {
    RGWCreateBucketParser parser;

    if (!parser.init()) {
      ldout(s->cct, 0) << "ERROR: failed to initialize parser" << dendl;
      return -EIO;
    }

    bool success = parser.parse(data, len, 1);
    ldout(s->cct, 20) << "create bucket input data=" << data << dendl;

    if (!success) {
      ldout(s->cct, 0) << "failed to parse input: " << data << dendl;
      return -EINVAL;
    }

    if (!parser.get_location_constraint(location_constraint)) {
      ldout(s->cct, 0) << "provided input did not specify location constraint correctly" << dendl;
      return -EINVAL;
    }

    ldout(s->cct, 10) << "create bucket location constraint: "
		      << location_constraint << dendl;
  }

  size_t pos = location_constraint.find(':');
  if (pos != string::npos) {
    placement_rule = location_constraint.substr(pos + 1);
    location_constraint = location_constraint.substr(0, pos);
  }

  return 0;
}

void RGWCreateBucket_ObjStore_S3::send_response()
{
  if (op_ret == -ERR_BUCKET_EXISTS)
    op_ret = 0;
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);

  if (op_ret < 0)
    return;

  if (s->system_request) {
    JSONFormatter f; /* use json formatter for system requests output */

    f.open_object_section("info");
    encode_json("entry_point_object_ver", ep_objv, &f);
    encode_json("object_ver", info.objv_tracker.read_version, &f);
    encode_json("bucket_info", info, &f);
    f.close_section();
    rgw_flush_formatter_and_reset(s, &f);
  }
}

void RGWDeleteBucket_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s, this);

  if (s->system_request) {
    JSONFormatter f; /* use json formatter for system requests output */

    f.open_object_section("info");
    encode_json("object_ver", objv_tracker.read_version, &f);
    f.close_section();
    rgw_flush_formatter_and_reset(s, &f);
  }
}

int RGWPutObj_ObjStore_S3::get_params()
{
  if (!s->length)
    return -ERR_LENGTH_REQUIRED;

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  map<string, bufferlist> src_attrs;
  size_t pos;
  int ret;

  RGWAccessControlPolicy_S3 s3policy(s->cct);
  ret = create_s3_policy(s, store, s3policy, s->owner);
  if (ret < 0)
    return ret;

  policy = s3policy;

  if_match = s->info.env->get("HTTP_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_IF_NONE_MATCH");
  copy_source = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE");
  copy_source_range = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_RANGE");

  /* handle x-amz-copy-source */

  if (copy_source) {
    if (*copy_source == '/') ++copy_source;
    copy_source_bucket_name = copy_source;
    pos = copy_source_bucket_name.find("/");
    if (pos == std::string::npos) {
      ret = -EINVAL;
      ldout(s->cct, 5) << "x-amz-copy-source bad format" << dendl;
      return ret;
    }
    copy_source_object_name = copy_source_bucket_name.substr(pos + 1, copy_source_bucket_name.size());
    copy_source_bucket_name = copy_source_bucket_name.substr(0, pos);
#define VERSION_ID_STR "?versionId="
    pos = copy_source_object_name.find(VERSION_ID_STR);
    if (pos == std::string::npos) {
      copy_source_object_name = url_decode(copy_source_object_name);
    } else {
      copy_source_version_id = copy_source_object_name.substr(pos + sizeof(VERSION_ID_STR) - 1);
      copy_source_object_name = url_decode(copy_source_object_name.substr(0, pos));
    }
    pos = copy_source_bucket_name.find(":");
    if (pos == std::string::npos) {
       copy_source_tenant_name = s->src_tenant_name;
    } else {
       copy_source_tenant_name = copy_source_bucket_name.substr(0, pos);
       copy_source_bucket_name = copy_source_bucket_name.substr(pos + 1, copy_source_bucket_name.size());
       if (copy_source_bucket_name.empty()) {
         ret = -EINVAL;
         ldout(s->cct, 5) << "source bucket name is empty" << dendl;
         return ret;
       }
    }
    ret = store->get_bucket_info(obj_ctx,
                                 copy_source_tenant_name,
                                 copy_source_bucket_name,
                                 copy_source_bucket_info,
                                 NULL, &src_attrs);
    if (ret < 0) {
      ldout(s->cct, 5) << __func__ << "(): get_bucket_info() returned ret=" << ret << dendl;
      return ret;
    }

    /* handle x-amz-copy-source-range */

    if (copy_source_range) {
      string range = copy_source_range;
      pos = range.find("=");
      if (pos == std::string::npos) {
        ret = -EINVAL;
        ldout(s->cct, 5) << "x-amz-copy-source-range bad format" << dendl;
        return ret;
      }
      range = range.substr(pos + 1);
      pos = range.find("-");
      if (pos == std::string::npos) {
        ret = -EINVAL;
        ldout(s->cct, 5) << "x-amz-copy-source-range bad format" << dendl;
        return ret;
      }
      string first = range.substr(0, pos);
      string last = range.substr(pos + 1);
      copy_source_range_fst = strtoull(first.c_str(), NULL, 10);
      copy_source_range_lst = strtoull(last.c_str(), NULL, 10);
    }

  } /* copy_source */

  /* handle object tagging */
  auto tag_str = s->info.env->get("HTTP_X_AMZ_TAGGING");
  if (tag_str){
    obj_tags = ceph::make_unique<RGWObjTags>();
    ret = obj_tags->set_from_string(tag_str);
    if (ret < 0){
      ldout(s->cct,0) << "setting obj tags failed with " << ret << dendl;
      if (ret == -ERR_INVALID_TAG){
        ret = -EINVAL; //s3 returns only -EINVAL for PUT requests
      }

      return ret;
    }
  }

  return RGWPutObj_ObjStore::get_params();
}

int RGWPutObj_ObjStore_S3::get_data(bufferlist& bl)
{
  const int ret = RGWPutObj_ObjStore::get_data(bl);
  if (ret == 0) {
    const int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }

  return ret;
}

static int get_success_retcode(int code)
{
  switch (code) {
    case 201:
      return STATUS_CREATED;
    case 204:
      return STATUS_NO_CONTENT;
  }
  return 0;
}

void RGWPutObj_ObjStore_S3::send_response()
{
  if (op_ret) {
    set_req_state_err(s, op_ret);
    dump_errno(s);
  } else {
    if (s->cct->_conf->rgw_s3_success_create_obj_status) {
      op_ret = get_success_retcode(
	s->cct->_conf->rgw_s3_success_create_obj_status);
      set_req_state_err(s, op_ret);
    }
    if (!copy_source) {
      dump_errno(s);
      dump_etag(s, etag);
      dump_content_length(s, 0);
      for (auto &it : crypt_http_responses)
        dump_header(s, it.first, it.second);
    } else {
      dump_errno(s);
      end_header(s, this, "application/xml");
      dump_start(s);
      struct tm tmp;
      utime_t ut(mtime);
      time_t secs = (time_t)ut.sec();
      gmtime_r(&secs, &tmp);
      char buf[TIME_BUF_SIZE];
      s->formatter->open_object_section_in_ns("CopyPartResult",
          "http://s3.amazonaws.com/doc/2006-03-01/");
      if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", &tmp) > 0) {
        s->formatter->dump_string("LastModified", buf);
      }
      s->formatter->dump_string("ETag", etag);
      s->formatter->close_section();
      rgw_flush_formatter_and_reset(s, s->formatter);
      return;
    }
  }
  if (s->system_request && !real_clock::is_zero(mtime)) {
    dump_epoch_header(s, "Rgwx-Mtime", mtime);
  }
  end_header(s, this);
}

static inline int get_obj_attrs(RGWRados *store, struct req_state *s, rgw_obj& obj, map<string, bufferlist>& attrs)
{
  RGWRados::Object op_target(store, s->bucket_info, *static_cast<RGWObjectCtx *>(s->obj_ctx), obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;

  return read_op.prepare();
}

static inline void set_attr(map<string, bufferlist>& attrs, const char* key, const std::string& value)
{
  bufferlist bl;
  ::encode(value,bl);
  attrs.emplace(key, std::move(bl));
}

static inline void set_attr(map<string, bufferlist>& attrs, const char* key, const char* value)
{
  bufferlist bl;
  ::encode(value,bl);
  attrs.emplace(key, std::move(bl));
}

int RGWPutObj_ObjStore_S3::get_decrypt_filter(
    std::unique_ptr<RGWGetDataCB>* filter,
    RGWGetDataCB* cb,
    map<string, bufferlist>& attrs,
    bufferlist* manifest_bl)
{
  std::map<std::string, std::string> crypt_http_responses_unused;

  int res = 0;
  std::unique_ptr<BlockCrypt> block_crypt;
  res = rgw_s3_prepare_decrypt(s, attrs, &block_crypt, crypt_http_responses_unused);
  if (res == 0) {
    if (block_crypt != nullptr) {
      auto f = std::unique_ptr<RGWGetObj_BlockDecrypt>(new RGWGetObj_BlockDecrypt(s->cct, cb, std::move(block_crypt)));
      //RGWGetObj_BlockDecrypt* f = new RGWGetObj_BlockDecrypt(s->cct, cb, std::move(block_crypt));
      if (f != nullptr) {
        if (manifest_bl != nullptr) {
          res = f->read_manifest(*manifest_bl);
          if (res == 0) {
            *filter = std::move(f);
          }
        }
      }
    }
  }
  return res;
}

int RGWPutObj_ObjStore_S3::get_encrypt_filter(
    std::unique_ptr<RGWPutObjDataProcessor>* filter,
    RGWPutObjDataProcessor* cb)
{
  int res = 0;
  RGWPutObjProcessor_Multipart* multi_processor=dynamic_cast<RGWPutObjProcessor_Multipart*>(cb);
  if (multi_processor != nullptr) {
    RGWMPObj* mp = nullptr;
    multi_processor->get_mp(&mp);
    if (mp != nullptr) {
      map<string, bufferlist> xattrs;
      string meta_oid;
      meta_oid = mp->get_meta();

      rgw_obj obj;
      obj.init_ns(s->bucket, meta_oid, RGW_OBJ_NS_MULTIPART);
      obj.set_in_extra_data(true);
      res = get_obj_attrs(store, s, obj, xattrs);
      if (res == 0) {
        std::unique_ptr<BlockCrypt> block_crypt;
        /* We are adding to existing object.
         * We use crypto mode that configured as if we were decrypting. */
        res = rgw_s3_prepare_decrypt(s, xattrs, &block_crypt, crypt_http_responses);
        if (res == 0 && block_crypt != nullptr)
          *filter = std::unique_ptr<RGWPutObj_BlockEncrypt>(
              new RGWPutObj_BlockEncrypt(s->cct, cb, std::move(block_crypt)));
      }
    }
    /* it is ok, to not have encryption at all */
  }
  else
  {
    std::unique_ptr<BlockCrypt> block_crypt;
    res = rgw_s3_prepare_encrypt(s, attrs, nullptr, &block_crypt, crypt_http_responses);
    if (res == 0 && block_crypt != nullptr) {
      *filter = std::unique_ptr<RGWPutObj_BlockEncrypt>(
          new RGWPutObj_BlockEncrypt(s->cct, cb, std::move(block_crypt)));
    }
  }
  return res;
}

void RGWPostObj_ObjStore_S3::rebuild_key(string& key)
{
  static string var = "${filename}";
  int pos = key.find(var);
  if (pos < 0)
    return;

  string new_key = key.substr(0, pos);
  new_key.append(filename);
  new_key.append(key.substr(pos + var.size()));

  key = new_key;
}

std::string RGWPostObj_ObjStore_S3::get_current_filename() const
{
  return s->object.name;
}

std::string RGWPostObj_ObjStore_S3::get_current_content_type() const
{
  return content_type;
}

int RGWPostObj_ObjStore_S3::get_params()
{
  op_ret = RGWPostObj_ObjStore::get_params();
  if (op_ret < 0) {
    return op_ret;
  }

  ldout(s->cct, 20) << "adding bucket to policy env: " << s->bucket.name
		    << dendl;
  env.add_var("bucket", s->bucket.name);

  bool done;
  do {
    struct post_form_part part;
    int r = read_form_part_header(&part, done);
    if (r < 0)
      return r;

    if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
      ldout(s->cct, 20) << "read part header -- part.name="
                        << part.name << dendl;

      for (const auto& pair : part.fields) {
        ldout(s->cct, 20) << "field.name=" << pair.first << dendl;
        ldout(s->cct, 20) << "field.val=" << pair.second.val << dendl;
        ldout(s->cct, 20) << "field.params:" << dendl;

        for (const auto& param_pair : pair.second.params) {
          ldout(s->cct, 20) << " " << param_pair.first
                            << " -> " << param_pair.second << dendl;
        }
      }
    }

    if (done) { /* unexpected here */
      err_msg = "Malformed request";
      return -EINVAL;
    }

    if (stringcasecmp(part.name, "file") == 0) { /* beginning of data transfer */
      struct post_part_field& field = part.fields["Content-Disposition"];
      map<string, string>::iterator iter = field.params.find("filename");
      if (iter != field.params.end()) {
	filename = iter->second;
      }
      parts[part.name] = part;
      break;
    }

    bool boundary;
    uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
    r = read_data(part.data, chunk_size, boundary, done);
    if (r < 0 || !boundary) {
      err_msg = "Couldn't find boundary";
      return -EINVAL;
    }
    parts[part.name] = part;
    string part_str(part.data.c_str(), part.data.length());
    env.add_var(part.name, part_str);
  } while (!done);

  string object_str;
  if (!part_str(parts, "key", &object_str)) {
    err_msg = "Key not specified";
    return -EINVAL;
  }

  s->object = rgw_obj_key(object_str);

  rebuild_key(s->object.name);

  if (s->object.empty()) {
    err_msg = "Empty object name";
    return -EINVAL;
  }

  env.add_var("key", s->object.name);

  part_str(parts, "Content-Type", &content_type);
  env.add_var("Content-Type", content_type);

  map<string, struct post_form_part, ltstr_nocase>::iterator piter =
    parts.upper_bound(RGW_AMZ_META_PREFIX);
  for (; piter != parts.end(); ++piter) {
    string n = piter->first;
    if (strncasecmp(n.c_str(), RGW_AMZ_META_PREFIX,
		    sizeof(RGW_AMZ_META_PREFIX) - 1) != 0)
      break;

    string attr_name = RGW_ATTR_PREFIX;
    attr_name.append(n);

    /* need to null terminate it */
    bufferlist& data = piter->second.data;
    string str = string(data.c_str(), data.length());

    bufferlist attr_bl;
    attr_bl.append(str.c_str(), str.size() + 1);

    attrs[attr_name] = attr_bl;
  }
  // TODO: refactor this and the above loop to share code
  piter = parts.find(RGW_AMZ_WEBSITE_REDIRECT_LOCATION);
  if (piter != parts.end()) {
    string n = piter->first;
    string attr_name = RGW_ATTR_PREFIX;
    attr_name.append(n);
    /* need to null terminate it */
    bufferlist& data = piter->second.data;
    string str = string(data.c_str(), data.length());

    bufferlist attr_bl;
    attr_bl.append(str.c_str(), str.size() + 1);

    attrs[attr_name] = attr_bl;
  }

  int r = get_policy();
  if (r < 0)
    return r;

  r = get_tags();
  if (r < 0)
    return r;


  min_len = post_policy.min_length;
  max_len = post_policy.max_length;



  return 0;
}

int RGWPostObj_ObjStore_S3::get_tags()
{
  string tags_str;
  if (part_str(parts, "tagging", &tags_str)) {
    RGWObjTagsXMLParser parser;
    if (!parser.init()){
      ldout(s->cct, 0) << "Couldn't init RGWObjTags XML parser" << dendl;
      err_msg = "Server couldn't process the request";
      return -EINVAL; // TODO: This class of errors in rgw code should be a 5XX error
    }
    if (!parser.parse(tags_str.c_str(), tags_str.size(), 1)) {
      ldout(s->cct,0 ) << "Invalid Tagging XML" << dendl;
      err_msg = "Invalid Tagging XML";
      return -EINVAL;
    }

    RGWObjTagSet_S3 *obj_tags_s3;
    RGWObjTagging_S3 *tagging;

    tagging = static_cast<RGWObjTagging_S3 *>(parser.find_first("Tagging"));
    obj_tags_s3 = static_cast<RGWObjTagSet_S3 *>(tagging->find_first("TagSet"));
    if(!obj_tags_s3){
      return -ERR_MALFORMED_XML;
    }

    RGWObjTags obj_tags;
    int r = obj_tags_s3->rebuild(obj_tags);
    if (r < 0)
      return r;

    bufferlist tags_bl;
    obj_tags.encode(tags_bl);
    ldout(s->cct, 20) << "Read " << obj_tags.count() << "tags" << dendl;
    attrs[RGW_ATTR_TAGS] = tags_bl;
  }


  return 0;
}

int RGWPostObj_ObjStore_S3::get_policy()
{
  if (part_bl(parts, "policy", &s->auth.s3_postobj_creds.encoded_policy)) {
    bool aws4_auth = false;

    /* x-amz-algorithm handling */
    using rgw::auth::s3::AWS4_HMAC_SHA256_STR;
    if ((part_str(parts, "x-amz-algorithm", &s->auth.s3_postobj_creds.x_amz_algorithm)) &&
        (s->auth.s3_postobj_creds.x_amz_algorithm == AWS4_HMAC_SHA256_STR)) {
      ldout(s->cct, 0) << "Signature verification algorithm AWS v4 (AWS4-HMAC-SHA256)" << dendl;
      aws4_auth = true;
    } else {
      ldout(s->cct, 0) << "Signature verification algorithm AWS v2" << dendl;
    }

    // check that the signature matches the encoded policy
    if (aws4_auth) {
      /* AWS4 */

      /* x-amz-credential handling */
      if (!part_str(parts, "x-amz-credential",
                    &s->auth.s3_postobj_creds.x_amz_credential)) {
        ldout(s->cct, 0) << "No S3 aws4 credential found!" << dendl;
        err_msg = "Missing aws4 credential";
        return -EINVAL;
      }

      /* x-amz-signature handling */
      if (!part_str(parts, "x-amz-signature",
                    &s->auth.s3_postobj_creds.signature)) {
        ldout(s->cct, 0) << "No aws4 signature found!" << dendl;
        err_msg = "Missing aws4 signature";
        return -EINVAL;
      }

      /* x-amz-date handling */
      std::string received_date_str;
      if (!part_str(parts, "x-amz-date", &received_date_str)) {
        ldout(s->cct, 0) << "No aws4 date found!" << dendl;
        err_msg = "Missing aws4 date";
        return -EINVAL;
      }
    } else {
      /* AWS2 */

      // check that the signature matches the encoded policy
      if (!part_str(parts, "AWSAccessKeyId",
                    &s->auth.s3_postobj_creds.access_key)) {
        ldout(s->cct, 0) << "No S3 aws2 access key found!" << dendl;
        err_msg = "Missing aws2 access key";
        return -EINVAL;
      }

      if (!part_str(parts, "signature", &s->auth.s3_postobj_creds.signature)) {
        ldout(s->cct, 0) << "No aws2 signature found!" << dendl;
        err_msg = "Missing aws2 signature";
        return -EINVAL;
      }
    }

    /* FIXME: this is a makeshift solution. The browser upload authentication will be
     * handled by an instance of rgw::auth::Completer spawned in Handler's authorize()
     * method. */
    const int ret = rgw::auth::Strategy::apply(auth_registry_ptr->get_s3_post(), s);
    if (ret != 0) {
      return -EACCES;
    } else {
      /* Populate the owner info. */
      s->owner.set_id(s->user->user_id);
      s->owner.set_name(s->user->display_name);
      ldout(s->cct, 20) << "Successful Signature Verification!" << dendl;
    }

    ceph::bufferlist decoded_policy;
    try {
      decoded_policy.decode_base64(s->auth.s3_postobj_creds.encoded_policy);
    } catch (buffer::error& err) {
      ldout(s->cct, 0) << "failed to decode_base64 policy" << dendl;
      err_msg = "Could not decode policy";
      return -EINVAL;
    }

    decoded_policy.append('\0'); // NULL terminate
    ldout(s->cct, 20) << "POST policy: " << decoded_policy.c_str() << dendl;


    int r = post_policy.from_json(decoded_policy, err_msg);
    if (r < 0) {
      if (err_msg.empty()) {
	err_msg = "Failed to parse policy";
      }
      ldout(s->cct, 0) << "failed to parse policy" << dendl;
      return -EINVAL;
    }

    if (aws4_auth) {
      /* AWS4 */
      post_policy.set_var_checked("x-amz-signature");
    } else {
      /* AWS2 */
      post_policy.set_var_checked("AWSAccessKeyId");
      post_policy.set_var_checked("signature");
    }
    post_policy.set_var_checked("policy");

    r = post_policy.check(&env, err_msg);
    if (r < 0) {
      if (err_msg.empty()) {
	err_msg = "Policy check failed";
      }
      ldout(s->cct, 0) << "policy check failed" << dendl;
      return r;
    }

  } else {
    ldout(s->cct, 0) << "No attached policy found!" << dendl;
  }

  string canned_acl;
  part_str(parts, "acl", &canned_acl);

  RGWAccessControlPolicy_S3 s3policy(s->cct);
  ldout(s->cct, 20) << "canned_acl=" << canned_acl << dendl;
  if (s3policy.create_canned(s->owner, s->bucket_owner, canned_acl) < 0) {
    err_msg = "Bad canned ACLs";
    return -EINVAL;
  }

  policy = s3policy;

  return 0;
}

int RGWPostObj_ObjStore_S3::complete_get_params()
{
  bool done;
  do {
    struct post_form_part part;
    int r = read_form_part_header(&part, done);
    if (r < 0) {
      return r;
    }

    ceph::bufferlist part_data;
    bool boundary;
    uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
    r = read_data(part.data, chunk_size, boundary, done);
    if (r < 0 || !boundary) {
      return -EINVAL;
    }

    /* Just reading the data but not storing any results of that. */
  } while (!done);

  return 0;
}

int RGWPostObj_ObjStore_S3::get_data(ceph::bufferlist& bl, bool& again)
{
  bool boundary;
  bool done;

  const uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  int r = read_data(bl, chunk_size, boundary, done);
  if (r < 0) {
    return r;
  }

  if (boundary) {
    if (!done) {
      /* Reached end of data, let's drain the rest of the params */
      r = complete_get_params();
      if (r < 0) {
       return r;
      }
    }
  }

  again = !boundary;
  return bl.length();
}

void RGWPostObj_ObjStore_S3::send_response()
{
  if (op_ret == 0 && parts.count("success_action_redirect")) {
    string redirect;

    part_str(parts, "success_action_redirect", &redirect);

    string tenant;
    string bucket;
    string key;
    string etag_str = "\"";

    etag_str.append(etag);
    etag_str.append("\"");

    string etag_url;

    url_encode(s->bucket_tenant, tenant); /* surely overkill, but cheap */
    url_encode(s->bucket_name, bucket);
    url_encode(s->object.name, key);
    url_encode(etag_str, etag_url);

    if (!s->bucket_tenant.empty()) {
      /*
       * What we really would like is to quaily the bucket name, so
       * that the client could simply copy it and paste into next request.
       * Unfortunately, in S3 we cannot know if the client will decide
       * to come through DNS, with "bucket.tenant" sytanx, or through
       * URL with "tenant\bucket" syntax. Therefore, we provide the
       * tenant separately.
       */
      redirect.append("?tenant=");
      redirect.append(tenant);
      redirect.append("&bucket=");
      redirect.append(bucket);
    } else {
      redirect.append("?bucket=");
      redirect.append(bucket);
    }
    redirect.append("&key=");
    redirect.append(key);
    redirect.append("&etag=");
    redirect.append(etag_url);

    int r = check_utf8(redirect.c_str(), redirect.size());
    if (r < 0) {
      op_ret = r;
      goto done;
    }
    dump_redirect(s, redirect);
    op_ret = STATUS_REDIRECT;
  } else if (op_ret == 0 && parts.count("success_action_status")) {
    string status_string;
    uint32_t status_int;

    part_str(parts, "success_action_status", &status_string);

    int r = stringtoul(status_string, &status_int);
    if (r < 0) {
      op_ret = r;
      goto done;
    }

    switch (status_int) {
      case 200:
	break;
      case 201:
	op_ret = STATUS_CREATED;
	break;
      default:
	op_ret = STATUS_NO_CONTENT;
	break;
    }
  } else if (! op_ret) {
    op_ret = STATUS_NO_CONTENT;
  }

done:
  if (op_ret == STATUS_CREATED) {
    for (auto &it : crypt_http_responses)
      dump_header(s, it.first, it.second);
    s->formatter->open_object_section("PostResponse");
    if (g_conf->rgw_dns_name.length())
      s->formatter->dump_format("Location", "%s/%s",
				s->info.script_uri.c_str(),
				s->object.name.c_str());
    if (!s->bucket_tenant.empty())
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object.name);
    s->formatter->close_section();
  }
  s->err.message = err_msg;
  set_req_state_err(s, op_ret);
  dump_errno(s);
  if (op_ret >= 0) {
    dump_content_length(s, s->formatter->get_len());
  }
  end_header(s, this);
  if (op_ret != STATUS_CREATED)
    return;

  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWPostObj_ObjStore_S3::get_encrypt_filter(
    std::unique_ptr<RGWPutObjDataProcessor>* filter, RGWPutObjDataProcessor* cb)
{
  int res = 0;
  std::unique_ptr<BlockCrypt> block_crypt;
  res = rgw_s3_prepare_encrypt(s, attrs, &parts, &block_crypt, crypt_http_responses);
  if (res == 0 && block_crypt != nullptr) {
    *filter = std::unique_ptr<RGWPutObj_BlockEncrypt>(
        new RGWPutObj_BlockEncrypt(s->cct, cb, std::move(block_crypt)));
  }
  else
    *filter = nullptr;
  return res;
}

int RGWDeleteObj_ObjStore_S3::get_params()
{
  const char *if_unmod = s->info.env->get("HTTP_X_AMZ_DELETE_IF_UNMODIFIED_SINCE");

  if (s->system_request) {
    s->info.args.get_bool(RGW_SYS_PARAM_PREFIX "no-precondition-error", &no_precondition_error, false);
  }

  if (if_unmod) {
    std::string if_unmod_decoded = url_decode(if_unmod);
    uint64_t epoch;
    uint64_t nsec;
    if (utime_t::parse_date(if_unmod_decoded, &epoch, &nsec) < 0) {
      ldout(s->cct, 10) << "failed to parse time: " << if_unmod_decoded << dendl;
      return -EINVAL;
    }
    unmod_since = utime_t(epoch, nsec).to_real_time();
  }

  return 0;
}

void RGWDeleteObj_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (r == -ENOENT)
    r = 0;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  if (!version_id.empty()) {
    dump_header(s, "x-amz-version-id", version_id);
  }
  if (delete_marker) {
    dump_header(s, "x-amz-delete-marker", "true");
  }
  end_header(s, this);
}

int RGWCopyObj_ObjStore_S3::init_dest_policy()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);

  /* build a policy for the target object */
  int r = create_s3_policy(s, store, s3policy, s->owner);
  if (r < 0)
    return r;

  dest_policy = s3policy;

  return 0;
}

int RGWCopyObj_ObjStore_S3::get_params()
{
  if_mod = s->info.env->get("HTTP_X_AMZ_COPY_IF_MODIFIED_SINCE");
  if_unmod = s->info.env->get("HTTP_X_AMZ_COPY_IF_UNMODIFIED_SINCE");
  if_match = s->info.env->get("HTTP_X_AMZ_COPY_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_X_AMZ_COPY_IF_NONE_MATCH");

  src_tenant_name = s->src_tenant_name;
  src_bucket_name = s->src_bucket_name;
  src_object = s->src_object;
  dest_tenant_name = s->bucket.tenant;
  dest_bucket_name = s->bucket.name;
  dest_object = s->object.name;

  if (s->system_request) {
    source_zone = s->info.args.get(RGW_SYS_PARAM_PREFIX "source-zone");
    s->info.args.get_bool(RGW_SYS_PARAM_PREFIX "copy-if-newer", &copy_if_newer, false);
    if (!source_zone.empty()) {
      client_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "client-id");
      op_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "op-id");

      if (client_id.empty() || op_id.empty()) {
	ldout(s->cct, 0) <<
	  RGW_SYS_PARAM_PREFIX "client-id or "
	  RGW_SYS_PARAM_PREFIX "op-id were not provided, "
	  "required for intra-region copy"
			 << dendl;
	return -EINVAL;
      }
    }
  }

  const char *md_directive = s->info.env->get("HTTP_X_AMZ_METADATA_DIRECTIVE");
  if (md_directive) {
    if (strcasecmp(md_directive, "COPY") == 0) {
      attrs_mod = RGWRados::ATTRSMOD_NONE;
    } else if (strcasecmp(md_directive, "REPLACE") == 0) {
      attrs_mod = RGWRados::ATTRSMOD_REPLACE;
    } else if (!source_zone.empty()) {
      attrs_mod = RGWRados::ATTRSMOD_NONE; // default for intra-zone_group copy
    } else {
      ldout(s->cct, 0) << "invalid metadata directive" << dendl;
      return -EINVAL;
    }
  }

  if (source_zone.empty() &&
      (dest_tenant_name.compare(src_tenant_name) == 0) &&
      (dest_bucket_name.compare(src_bucket_name) == 0) &&
      (dest_object.compare(src_object.name) == 0) &&
      src_object.instance.empty() &&
      (attrs_mod != RGWRados::ATTRSMOD_REPLACE)) {
    /* can only copy object into itself if replacing attrs */
    ldout(s->cct, 0) << "can't copy object into itself if not replacing attrs"
		     << dendl;
    return -ERR_INVALID_REQUEST;
  }
  return 0;
}

void RGWCopyObj_ObjStore_S3::send_partial_response(off_t ofs)
{
  if (! sent_header) {
    if (op_ret)
    set_req_state_err(s, op_ret);
    dump_errno(s);

    end_header(s, this, "application/xml");
    if (op_ret == 0) {
      s->formatter->open_object_section_in_ns("CopyObjectResult", XMLNS_AWS_S3);
    }
    sent_header = true;
  } else {
    /* Send progress field. Note that this diverge from the original S3
     * spec. We do this in order to keep connection alive.
     */
    s->formatter->dump_int("Progress", (uint64_t)ofs);
  }
  rgw_flush_formatter(s, s->formatter);
}

void RGWCopyObj_ObjStore_S3::send_response()
{
  if (!sent_header)
    send_partial_response(0);

  if (op_ret == 0) {
    dump_time(s, "LastModified", &mtime);
    std::string etag_str = etag.to_str();
    if (! etag_str.empty()) {
      s->formatter->dump_string("ETag", std::move(etag_str));
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWGetACLs_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
  rgw_flush_formatter(s, s->formatter);
  dump_body(s, acls);
}

int RGWPutACLs_ObjStore_S3::get_params()
{
  int ret =  RGWPutACLs_ObjStore::get_params();
  if (ret >= 0) {
    const int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }
  return ret;
}

int RGWPutACLs_ObjStore_S3::get_policy_from_state(RGWRados *store,
						  struct req_state *s,
						  stringstream& ss)
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);

  // bucket-* canned acls do not apply to bucket
  if (s->object.empty()) {
    if (s->canned_acl.find("bucket") != string::npos)
      s->canned_acl.clear();
  }

  int r = create_s3_policy(s, store, s3policy, owner);
  if (r < 0)
    return r;

  s3policy.to_xml(ss);

  return 0;
}

void RGWPutACLs_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
}

void RGWGetLC_ObjStore_S3::execute()
{
  config.set_ctx(s->cct);

  map<string, bufferlist>::iterator aiter = s->bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == s->bucket_attrs.end()) {
    op_ret = -ENOENT;
    return;
  }

  bufferlist::iterator iter(&aiter->second);
  try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      ldout(s->cct, 0) << __func__ <<  "decode life cycle config failed" << dendl;
      op_ret = -EIO;
      return;
    }
}

void RGWGetLC_ObjStore_S3::send_response()
{
  if (op_ret) {
    if (op_ret == -ENOENT) {	
      set_req_state_err(s, ERR_NO_SUCH_LC);
    } else {
      set_req_state_err(s, op_ret);
    }
  }
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  if (op_ret < 0)
    return;

  config.dump_xml(s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWPutLC_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
}

void RGWDeleteLC_ObjStore_S3::send_response()
{
  if (op_ret == 0)
      op_ret = STATUS_NO_CONTENT;
  if (op_ret) {   
    set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
}

void RGWGetCORS_ObjStore_S3::send_response()
{
  if (op_ret) {
    if (op_ret == -ENOENT)
      set_req_state_err(s, ERR_NOT_FOUND);
    else
      set_req_state_err(s, op_ret);
  }
  dump_errno(s);
  end_header(s, NULL, "application/xml");
  dump_start(s);
  if (! op_ret) {
    string cors;
    RGWCORSConfiguration_S3 *s3cors =
      static_cast<RGWCORSConfiguration_S3 *>(&bucket_cors);
    stringstream ss;

    s3cors->to_xml(ss);
    cors = ss.str();
    dump_body(s, cors);
  }
}

int RGWPutCORS_ObjStore_S3::get_params()
{
  int r;
  char *data = nullptr;
  int len = 0;
  RGWCORSXMLParser_S3 parser(s->cct);
  RGWCORSConfiguration_S3 *cors_config;

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  r = rgw_rest_read_all_input(s, &data, &len, max_size, false);
  if (r < 0) {
    return r;
  }

  auto data_deleter = std::unique_ptr<char, decltype(free)*>{data, free};

  r = do_aws4_auth_completion();
  if (r < 0) {
    return r;
  }

  if (!parser.init()) {
    return -EINVAL;
  }

  if (!data || !parser.parse(data, len, 1)) {
    return -EINVAL;
  }
  cors_config =
    static_cast<RGWCORSConfiguration_S3 *>(parser.find_first(
					     "CORSConfiguration"));
  if (!cors_config) {
    return -EINVAL;
  }

  // forward bucket cors requests to meta master zone
  if (!store->is_meta_master()) {
    /* only need to keep this data around if we're not meta master */
    in_data.append(data, len);
  }

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    ldout(s->cct, 15) << "CORSConfiguration";
    cors_config->to_xml(*_dout);
    *_dout << dendl;
  }

  cors_config->encode(cors_bl);

  return 0;
}

void RGWPutCORS_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, NULL, "application/xml");
  dump_start(s);
}

void RGWDeleteCORS_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r || r == -ENOENT)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s, NULL);
}

void RGWOptionsCORS_ObjStore_S3::send_response()
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

void RGWGetRequestPayment_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  s->formatter->open_object_section_in_ns("RequestPaymentConfiguration", XMLNS_AWS_S3);
  const char *payer = requester_pays ? "Requester" :  "BucketOwner";
  s->formatter->dump_string("Payer", payer);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

class RGWSetRequestPaymentParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) override {
    return new XMLObj;
  }

public:
  RGWSetRequestPaymentParser() {}
  ~RGWSetRequestPaymentParser() override {}

  int get_request_payment_payer(bool *requester_pays) {
    XMLObj *config = find_first("RequestPaymentConfiguration");
    if (!config)
      return -EINVAL;

    *requester_pays = false;

    XMLObj *field = config->find_first("Payer");
    if (!field)
      return 0;

    string& s = field->get_data();

    if (stringcasecmp(s, "Requester") == 0) {
      *requester_pays = true;
    } else if (stringcasecmp(s, "BucketOwner") != 0) {
      return -EINVAL;
    }

    return 0;
  }
};

int RGWSetRequestPayment_ObjStore_S3::get_params()
{
  char *data;
  int len = 0;
  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r = rgw_rest_read_all_input(s, &data, &len, max_size, false);

  if (r < 0) {
    return r;
  }

  RGWSetRequestPaymentParser parser;

  if (!parser.init()) {
    ldout(s->cct, 0) << "ERROR: failed to initialize parser" << dendl;
    r = -EIO;
    goto done;
  }

  if (!parser.parse(data, len, 1)) {
    ldout(s->cct, 10) << "failed to parse data: " << data << dendl;
    r = -EINVAL;
    goto done;
  }

  r = parser.get_request_payment_payer(&requester_pays);

done:
  free(data);

  return r;
}

void RGWSetRequestPayment_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s);
}

int RGWInitMultipart_ObjStore_S3::get_params()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);
  op_ret = create_s3_policy(s, store, s3policy, s->owner);
  if (op_ret < 0)
    return op_ret;

  policy = s3policy;

  return 0;
}

void RGWInitMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  for (auto &it : crypt_http_responses)
     dump_header(s, it.first, it.second);
  end_header(s, this, "application/xml");
  if (op_ret == 0) {
    dump_start(s);
    s->formatter->open_object_section_in_ns("InitiateMultipartUploadResult", XMLNS_AWS_S3);
    if (!s->bucket_tenant.empty())
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object.name);
    s->formatter->dump_string("UploadId", upload_id);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWInitMultipart_ObjStore_S3::prepare_encryption(map<string, bufferlist>& attrs)
{
  int res = 0;
  res = rgw_s3_prepare_encrypt(s, attrs, nullptr, nullptr, crypt_http_responses);
  return res;
}

int RGWCompleteMultipart_ObjStore_S3::get_params()
{
  int ret = RGWCompleteMultipart_ObjStore::get_params();
  if (ret < 0) {
    return ret;
  }

  return do_aws4_auth_completion();
}

void RGWCompleteMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  if (op_ret == 0) { 
    dump_start(s);
    s->formatter->open_object_section_in_ns("CompleteMultipartUploadResult", XMLNS_AWS_S3);
    if (!s->bucket_tenant.empty()) {
      if (s->info.domain.length()) {
        s->formatter->dump_format("Location", "%s.%s.%s",
          s->bucket_name.c_str(),
          s->bucket_tenant.c_str(),
          s->info.domain.c_str());
      }
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    } else {
      if (s->info.domain.length()) {
        s->formatter->dump_format("Location", "%s.%s",
          s->bucket_name.c_str(),
          s->info.domain.c_str());
      }
    }
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object.name);
    s->formatter->dump_string("ETag", etag);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWAbortMultipart_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  set_req_state_err(s, r);
  dump_errno(s);
  end_header(s, this);
}

void RGWListMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");

  if (op_ret == 0) {
    dump_start(s);
    s->formatter->open_object_section_in_ns("ListPartsResult", XMLNS_AWS_S3);
    map<uint32_t, RGWUploadPartInfo>::iterator iter;
    map<uint32_t, RGWUploadPartInfo>::reverse_iterator test_iter;
    int cur_max = 0;

    iter = parts.begin();
    test_iter = parts.rbegin();
    if (test_iter != parts.rend()) {
      cur_max = test_iter->first;
    }
    if (!s->bucket_tenant.empty())
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object.name);
    s->formatter->dump_string("UploadId", upload_id);
    s->formatter->dump_string("StorageClass", "STANDARD");
    s->formatter->dump_int("PartNumberMarker", marker);
    s->formatter->dump_int("NextPartNumberMarker", cur_max);
    s->formatter->dump_int("MaxParts", max_parts);
    s->formatter->dump_string("IsTruncated", (truncated ? "true" : "false"));

    ACLOwner& owner = policy.get_owner();
    dump_owner(s, owner.get_id(), owner.get_display_name());

    for (; iter != parts.end(); ++iter) {
      RGWUploadPartInfo& info = iter->second;

      s->formatter->open_object_section("Part");

      dump_time(s, "LastModified", &info.modified);

      s->formatter->dump_unsigned("PartNumber", info.num);
      s->formatter->dump_format("ETag", "\"%s\"", info.etag.c_str());
      s->formatter->dump_unsigned("Size", info.accounted_size);
      s->formatter->close_section();
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWListBucketMultiparts_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    set_req_state_err(s, op_ret);
  dump_errno(s);

  end_header(s, this, "application/xml");
  dump_start(s);
  if (op_ret < 0)
    return;

  s->formatter->open_object_section_in_ns("ListMultipartUploadsResult", XMLNS_AWS_S3);
  if (!s->bucket_tenant.empty())
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  s->formatter->dump_string("Bucket", s->bucket_name);
  if (!prefix.empty())
    s->formatter->dump_string("ListMultipartUploadsResult.Prefix", prefix);
  string& key_marker = marker.get_key();
  if (!key_marker.empty())
    s->formatter->dump_string("KeyMarker", key_marker);
  string& upload_id_marker = marker.get_upload_id();
  if (!upload_id_marker.empty())
    s->formatter->dump_string("UploadIdMarker", upload_id_marker);
  string next_key = next_marker.mp.get_key();
  if (!next_key.empty())
    s->formatter->dump_string("NextKeyMarker", next_key);
  string next_upload_id = next_marker.mp.get_upload_id();
  if (!next_upload_id.empty())
    s->formatter->dump_string("NextUploadIdMarker", next_upload_id);
  s->formatter->dump_int("MaxUploads", max_uploads);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);
  s->formatter->dump_string("IsTruncated", (is_truncated ? "true" : "false"));

  if (op_ret >= 0) {
    vector<RGWMultipartUploadEntry>::iterator iter;
    for (iter = uploads.begin(); iter != uploads.end(); ++iter) {
      RGWMPObj& mp = iter->mp;
      s->formatter->open_array_section("Upload");
      s->formatter->dump_string("Key", mp.get_key());
      s->formatter->dump_string("UploadId", mp.get_upload_id());
      dump_owner(s, s->user->user_id, s->user->display_name, "Initiator");
      dump_owner(s, s->user->user_id, s->user->display_name);
      s->formatter->dump_string("StorageClass", "STANDARD");
      dump_time(s, "Initiated", &iter->obj.meta.mtime);
      s->formatter->close_section();
    }
    if (!common_prefixes.empty()) {
      s->formatter->open_array_section("CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
	   pref_iter != common_prefixes.end(); ++pref_iter) {
	s->formatter->dump_string("CommonPrefixes.Prefix", pref_iter->first);
      }
      s->formatter->close_section();
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWDeleteMultiObj_ObjStore_S3::get_params()
{
  int ret = RGWDeleteMultiObj_ObjStore::get_params();
  if (ret < 0) {
    return ret;
  }

  return do_aws4_auth_completion();
}

void RGWDeleteMultiObj_ObjStore_S3::send_status()
{
  if (! status_dumped) {
    if (op_ret < 0)
      set_req_state_err(s, op_ret);
    dump_errno(s);
    status_dumped = true;
  }
}

void RGWDeleteMultiObj_ObjStore_S3::begin_response()
{

  if (!status_dumped) {
    send_status();
  }

  dump_start(s);
  end_header(s, this, "application/xml");
  s->formatter->open_object_section_in_ns("DeleteResult", XMLNS_AWS_S3);

  rgw_flush_formatter(s, s->formatter);
}

void RGWDeleteMultiObj_ObjStore_S3::send_partial_response(rgw_obj_key& key,
							  bool delete_marker,
							  const string& marker_version_id, int ret)
{
  if (!key.empty()) {
    if (op_ret == 0 && !quiet) {
      s->formatter->open_object_section("Deleted");
      s->formatter->dump_string("Key", key.name);
      if (!key.instance.empty()) {
	s->formatter->dump_string("VersionId", key.instance);
      }
      if (delete_marker) {
	s->formatter->dump_bool("DeleteMarker", true);
	s->formatter->dump_string("DeleteMarkerVersionId", marker_version_id);
      }
      s->formatter->close_section();
    } else if (op_ret < 0) {
      struct rgw_http_error r;
      int err_no;

      s->formatter->open_object_section("Error");

      err_no = -op_ret;
      rgw_get_errno_s3(&r, err_no);

      s->formatter->dump_string("Key", key.name);
      s->formatter->dump_string("VersionId", key.instance);
      s->formatter->dump_int("Code", r.http_ret);
      s->formatter->dump_string("Message", r.s3_code);
      s->formatter->close_section();
    }

    rgw_flush_formatter(s, s->formatter);
  }
}

void RGWDeleteMultiObj_ObjStore_S3::end_response()
{

  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetObjLayout_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/json");

  JSONFormatter f;

  if (op_ret < 0) {
    return;
  }

  f.open_object_section("result");
  ::encode_json("head", head_obj, &f);
  ::encode_json("manifest", *manifest, &f);
  f.open_array_section("data_location");
  for (auto miter = manifest->obj_begin(); miter != manifest->obj_end(); ++miter) {
    f.open_object_section("obj");
    rgw_raw_obj raw_loc = miter.get_location().get_raw_obj(store);
    ::encode_json("ofs", miter.get_ofs(), &f);
    ::encode_json("loc", raw_loc, &f);
    ::encode_json("loc_ofs", miter.location_ofs(), &f);
    ::encode_json("loc_size", miter.get_stripe_size(), &f);
    f.close_section();
    rgw_flush_formatter(s, &f);
  }
  f.close_section();
  f.close_section();
  rgw_flush_formatter(s, &f);
}

int RGWConfigBucketMetaSearch_ObjStore_S3::get_params()
{
  auto iter = s->info.x_meta_map.find("x-amz-meta-search");
  if (iter == s->info.x_meta_map.end()) {
    s->err.message = "X-Rgw-Meta-Search header not provided";
    ldout(s->cct, 5) << s->err.message << dendl;
    return -EINVAL;
  }

  list<string> expressions;
  get_str_list(iter->second, ",", expressions);

  for (auto& expression : expressions) {
    vector<string> args;
    get_str_vec(expression, ";", args);

    if (args.empty()) {
      s->err.message = "invalid empty expression";
      ldout(s->cct, 5) << s->err.message << dendl;
      return -EINVAL;
    }
    if (args.size() > 2) {
      s->err.message = string("invalid expression: ") + expression;
      ldout(s->cct, 5) << s->err.message << dendl;
      return -EINVAL;
    }

    string key = boost::algorithm::to_lower_copy(rgw_trim_whitespace(args[0]));
    string val;
    if (args.size() > 1) {
      val = boost::algorithm::to_lower_copy(rgw_trim_whitespace(args[1]));
    }

    if (!boost::algorithm::starts_with(key, RGW_AMZ_META_PREFIX)) {
      s->err.message = string("invalid expression, key must start with '" RGW_AMZ_META_PREFIX "' : ") + expression;
      ldout(s->cct, 5) << s->err.message << dendl;
      return -EINVAL;
    }

    key = key.substr(sizeof(RGW_AMZ_META_PREFIX) - 1);

    ESEntityTypeMap::EntityType entity_type;

    if (val.empty() || val == "str" || val == "string") {
      entity_type = ESEntityTypeMap::ES_ENTITY_STR;
    } else if (val == "int" || val == "integer") {
      entity_type = ESEntityTypeMap::ES_ENTITY_INT;
    } else if (val == "date" || val == "datetime") {
      entity_type = ESEntityTypeMap::ES_ENTITY_DATE;
    } else {
      s->err.message = string("invalid entity type: ") + val;
      ldout(s->cct, 5) << s->err.message << dendl;
      return -EINVAL;
    }

    mdsearch_config[key] = entity_type;
  }

  return 0;
}

void RGWConfigBucketMetaSearch_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}

void RGWGetBucketMetaSearch_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, NULL, "application/xml");

  Formatter *f = s->formatter;
  f->open_array_section("GetBucketMetaSearchResult");
  for (auto& e : s->bucket_info.mdsearch_config) {
    f->open_object_section("Entry");
    string k = string("x-amz-meta-") + e.first;
    f->dump_string("Key", k.c_str());
    const char *type;
    switch (e.second) {
      case ESEntityTypeMap::ES_ENTITY_INT:
        type = "int";
        break;
      case ESEntityTypeMap::ES_ENTITY_DATE:
        type = "date";
        break;
      default:
        type = "str";
    }
    f->dump_string("Type", type);
    f->close_section();
  }
  f->close_section();
  rgw_flush_formatter(s, f);
}

void RGWDelBucketMetaSearch_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this);
}


RGWOp *RGWHandler_REST_Service_S3::op_get()
{
  if (is_usage_op()) {
    return new RGWGetUsage_ObjStore_S3;
  } else {
    return new RGWListBuckets_ObjStore_S3;
  }
}

RGWOp *RGWHandler_REST_Service_S3::op_head()
{
  return new RGWListBuckets_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Service_S3::op_post()
{
  if (s->info.args.exists("Action")) {
    string action = s->info.args.get("Action");
    if (action.compare("CreateRole") == 0)
      return new RGWCreateRole;
    if (action.compare("DeleteRole") == 0)
      return new RGWDeleteRole;
    if (action.compare("GetRole") == 0)
      return new RGWGetRole;
    if (action.compare("UpdateAssumeRolePolicy") == 0)
      return new RGWModifyRole;
    if (action.compare("ListRoles") == 0)
      return new RGWListRoles;
    if (action.compare("PutRolePolicy") == 0)
      return new RGWPutRolePolicy;
    if (action.compare("GetRolePolicy") == 0)
      return new RGWGetRolePolicy;
    if (action.compare("ListRolePolicies") == 0)
      return new RGWListRolePolicies;
    if (action.compare("DeleteRolePolicy") == 0)
      return new RGWDeleteRolePolicy;
  }
  return NULL;
}

RGWOp *RGWHandler_REST_Bucket_S3::get_obj_op(bool get_data)
{
  // Non-website mode
  if (get_data) {
    return new RGWListBucket_ObjStore_S3;
  } else {
    return new RGWStatBucket_ObjStore_S3;
  }
}

RGWOp *RGWHandler_REST_Bucket_S3::op_get()
{
  if (s->info.args.sub_resource_exists("logging"))
    return new RGWGetBucketLogging_ObjStore_S3;

  if (s->info.args.sub_resource_exists("location"))
    return new RGWGetBucketLocation_ObjStore_S3;

  if (s->info.args.sub_resource_exists("versioning"))
    return new RGWGetBucketVersioning_ObjStore_S3;

  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWGetBucketWebsite_ObjStore_S3;
  }

  if (s->info.args.exists("mdsearch")) {
    return new RGWGetBucketMetaSearch_ObjStore_S3;
  }

  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (is_cors_op()) {
    return new RGWGetCORS_ObjStore_S3;
  } else if (is_request_payment_op()) {
    return new RGWGetRequestPayment_ObjStore_S3;
  } else if (s->info.args.exists("uploads")) {
    return new RGWListBucketMultiparts_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWGetLC_ObjStore_S3;
  } else if(is_policy_op()) {
    return new RGWGetBucketPolicy;
  }
  return get_obj_op(true);
}

RGWOp *RGWHandler_REST_Bucket_S3::op_head()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploads")) {
    return new RGWListBucketMultiparts_ObjStore_S3;
  }
  return get_obj_op(false);
}

RGWOp *RGWHandler_REST_Bucket_S3::op_put()
{
  if (s->info.args.sub_resource_exists("logging"))
    return NULL;
  if (s->info.args.sub_resource_exists("versioning"))
    return new RGWSetBucketVersioning_ObjStore_S3;
  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWSetBucketWebsite_ObjStore_S3;
  }
  if (is_acl_op()) {
    return new RGWPutACLs_ObjStore_S3;
  } else if (is_cors_op()) {
    return new RGWPutCORS_ObjStore_S3;
  } else if (is_request_payment_op()) {
    return new RGWSetRequestPayment_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWPutLC_ObjStore_S3;
  } else if(is_policy_op()) {
    return new RGWPutBucketPolicy;
  }
  return new RGWCreateBucket_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_delete()
{
  if (is_cors_op()) {
    return new RGWDeleteCORS_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWDeleteLC_ObjStore_S3;
  } else if(is_policy_op()) {
    return new RGWDeleteBucketPolicy;
  }

  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWDeleteBucketWebsite_ObjStore_S3;
  }

  if (s->info.args.exists("mdsearch")) {
    return new RGWDelBucketMetaSearch_ObjStore_S3;
  }

  return new RGWDeleteBucket_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_post()
{
  if (s->info.args.exists("delete")) {
    return new RGWDeleteMultiObj_ObjStore_S3;
  }

  if (s->info.args.exists("mdsearch")) {
    return new RGWConfigBucketMetaSearch_ObjStore_S3;
  }

  return new RGWPostObj_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_options()
{
  return new RGWOptionsCORS_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::get_obj_op(bool get_data)
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  }
  RGWGetObj_ObjStore_S3 *get_obj_op = new RGWGetObj_ObjStore_S3;
  get_obj_op->set_get_data(get_data);
  return get_obj_op;
}

RGWOp *RGWHandler_REST_Obj_S3::op_get()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploadId")) {
    return new RGWListMultipart_ObjStore_S3;
  } else if (s->info.args.exists("layout")) {
    return new RGWGetObjLayout_ObjStore_S3;
  } else if (is_tagging_op()) {
    return new RGWGetObjTags_ObjStore_S3;
  }
  return get_obj_op(true);
}

RGWOp *RGWHandler_REST_Obj_S3::op_head()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploadId")) {
    return new RGWListMultipart_ObjStore_S3;
  }
  return get_obj_op(false);
}

RGWOp *RGWHandler_REST_Obj_S3::op_put()
{
  if (is_acl_op()) {
    return new RGWPutACLs_ObjStore_S3;
  } else if (is_tagging_op()) {
    return new RGWPutObjTags_ObjStore_S3;
  }

  if (s->init_state.src_bucket.empty())
    return new RGWPutObj_ObjStore_S3;
  else
    return new RGWCopyObj_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::op_delete()
{
  if (is_tagging_op()) {
    return new RGWDeleteObjTags_ObjStore_S3;
  }
  string upload_id = s->info.args.get("uploadId");

  if (upload_id.empty())
    return new RGWDeleteObj_ObjStore_S3;
  else
    return new RGWAbortMultipart_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::op_post()
{
  if (s->info.args.exists("uploadId"))
    return new RGWCompleteMultipart_ObjStore_S3;

  if (s->info.args.exists("uploads"))
    return new RGWInitMultipart_ObjStore_S3;

  return new RGWPostObj_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::op_options()
{
  return new RGWOptionsCORS_ObjStore_S3;
}

int RGWHandler_REST_S3::init_from_header(struct req_state* s,
					int default_formatter,
					bool configurable_format)
{
  string req;
  string first;

  const char *req_name = s->relative_uri.c_str();
  const char *p;

  if (*req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse();

  /* must be called after the args parsing */
  int ret = allocate_formatter(s, default_formatter, configurable_format);
  if (ret < 0)
    return ret;

  if (*req_name != '/')
    return 0;

  req_name++;

  if (!*req_name)
    return 0;

  req = req_name;
  int pos = req.find('/');
  if (pos >= 0) {
    first = req.substr(0, pos);
  } else {
    first = req;
  }

  /*
   * XXX The intent of the check for empty is apparently to let the bucket
   * name from DNS to be set ahead. However, we currently take the DNS
   * bucket and re-insert it into URL in rgw_rest.cc:RGWREST::preprocess().
   * So, this check is meaningless.
   *
   * Rather than dropping this, the code needs to be changed into putting
   * the bucket (and its tenant) from DNS and Host: header (HTTP_HOST)
   * into req_status.bucket_name directly.
   */
  if (s->init_state.url_bucket.empty()) {
    // Save bucket to tide us over until token is parsed.
    s->init_state.url_bucket = first;
    if (pos >= 0) {
      string encoded_obj_str = req.substr(pos+1);
      s->object = rgw_obj_key(encoded_obj_str, s->info.args.get("versionId"));
    }
  } else {
    s->object = rgw_obj_key(req_name, s->info.args.get("versionId"));
  }
  return 0;
}

int RGWHandler_REST_S3::postauth_init()
{
  struct req_init_state *t = &s->init_state;
  bool relaxed_names = s->cct->_conf->rgw_relaxed_s3_bucket_names;

  rgw_parse_url_bucket(t->url_bucket, s->user->user_id.tenant,
		      s->bucket_tenant, s->bucket_name);

  dout(10) << "s->object=" << (!s->object.empty() ? s->object : rgw_obj_key("<NULL>"))
           << " s->bucket=" << rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name) << dendl;

  int ret;
  ret = validate_tenant_name(s->bucket_tenant);
  if (ret)
    return ret;
  if (!s->bucket_name.empty()) {
    ret = valid_s3_bucket_name(s->bucket_name, relaxed_names);
    if (ret)
      return ret;
    ret = validate_object_name(s->object.name);
    if (ret)
      return ret;
  }

  if (!t->src_bucket.empty()) {
    rgw_parse_url_bucket(t->src_bucket, s->user->user_id.tenant,
			s->src_tenant_name, s->src_bucket_name);
    ret = validate_tenant_name(s->src_tenant_name);
    if (ret)
      return ret;
    ret = valid_s3_bucket_name(s->src_bucket_name, relaxed_names);
    if (ret)
      return ret;
  }
  return 0;
}

int RGWHandler_REST_S3::init(RGWRados *store, struct req_state *s,
                             rgw::io::BasicClient *cio)
{
  int ret;

  s->dialect = "s3";
  
  ret = validate_tenant_name(s->bucket_tenant);
  if (ret)
    return ret;
  bool relaxed_names = s->cct->_conf->rgw_relaxed_s3_bucket_names;
  if (!s->bucket_name.empty()) {
    ret = valid_s3_bucket_name(s->bucket_name, relaxed_names);
    if (ret)
      return ret;
    ret = validate_object_name(s->object.name);
    if (ret)
      return ret;
  }

  const char *cacl = s->info.env->get("HTTP_X_AMZ_ACL");
  if (cacl)
    s->canned_acl = cacl;

  s->has_acl_header = s->info.env->exists_prefix("HTTP_X_AMZ_GRANT");

  const char *copy_source = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE");

  if (copy_source && !s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_RANGE")) {
    ret = RGWCopyObj::parse_copy_location(copy_source,
                                          s->init_state.src_bucket,
                                          s->src_object);
    if (!ret) {
      ldout(s->cct, 0) << "failed to parse copy location" << dendl;
      return -EINVAL; // XXX why not -ERR_INVALID_BUCKET_NAME or -ERR_BAD_URL?
    }
  }

  return RGWHandler_REST::init(store, s, cio);
}

enum class AwsVersion {
  UNKOWN,
  V2,
  V4
};

enum class AwsRoute {
  UNKOWN,
  QUERY_STRING,
  HEADERS
};

static inline std::pair<AwsVersion, AwsRoute>
discover_aws_flavour(const req_info& info)
{
  using rgw::auth::s3::AWS4_HMAC_SHA256_STR;

  AwsVersion version = AwsVersion::UNKOWN;
  AwsRoute route = AwsRoute::UNKOWN;

  const char* http_auth = info.env->get("HTTP_AUTHORIZATION");
  if (http_auth && http_auth[0]) {
    /* Authorization in Header */
    route = AwsRoute::HEADERS;

    if (!strncmp(http_auth, AWS4_HMAC_SHA256_STR,
                 strlen(AWS4_HMAC_SHA256_STR))) {
      /* AWS v4 */
      version = AwsVersion::V4;
    } else if (!strncmp(http_auth, "AWS ", 4)) {
      /* AWS v2 */
      version = AwsVersion::V2;
    }
  } else {
    route = AwsRoute::QUERY_STRING;

    if (info.args.get("X-Amz-Algorithm") == AWS4_HMAC_SHA256_STR) {
      /* AWS v4 */
      version = AwsVersion::V4;
    } else if (!info.args.get("AWSAccessKeyId").empty()) {
      /* AWS v2 */
      version = AwsVersion::V2;
    }
  }

  return std::make_pair(version, route);
}

static void init_anon_user(struct req_state *s)
{
  rgw_get_anon_user(*(s->user));
  s->perm_mask = RGW_PERM_FULL_CONTROL;
}

/*
 * verify that a signed request comes from the keyholder
 * by checking the signature against our locally-computed version
 *
 * it tries AWS v4 before AWS v2
 */
int RGW_Auth_S3::authorize(RGWRados* const store,
                           const rgw::auth::StrategyRegistry& auth_registry,
                           struct req_state* const s)
{

  /* neither keystone and rados enabled; warn and exit! */
  if (!store->ctx()->_conf->rgw_s3_auth_use_rados &&
      !store->ctx()->_conf->rgw_s3_auth_use_keystone &&
      !store->ctx()->_conf->rgw_s3_auth_use_ldap) {
    dout(0) << "WARNING: no authorization backend enabled! Users will never authenticate." << dendl;
    return -EPERM;
  }

  if (s->op == OP_OPTIONS) {
    init_anon_user(s);
    return 0;
  }

  AwsVersion version;
  AwsRoute route;
  std::tie(version, route) = discover_aws_flavour(s->info);

  if (route == AwsRoute::QUERY_STRING && version == AwsVersion::UNKOWN) {
    /* FIXME(rzarzynski): handle anon user. */
    init_anon_user(const_cast<req_state*>(s));
    return 0;
  }

  return authorize_v2(store, auth_registry, s);
}


/*
 * handle v2 signatures
 */
int RGW_Auth_S3::authorize_v2(RGWRados* const store,
                              const rgw::auth::StrategyRegistry& auth_registry,
                              struct req_state* const s)
{
  const auto ret = rgw::auth::Strategy::apply(auth_registry.get_s3_main(), s);
  if (ret == 0) {
    /* Populate the owner info. */
    s->owner.set_id(s->user->user_id);
    s->owner.set_name(s->user->display_name);
  }
  return ret;
}

int RGWHandler_Auth_S3::init(RGWRados *store, struct req_state *state,
                             rgw::io::BasicClient *cio)
{
  int ret = RGWHandler_REST_S3::init_from_header(state, RGW_FORMAT_JSON,
						     true);
  if (ret < 0)
    return ret;

  return RGWHandler_REST::init(store, state, cio);
}

RGWHandler_REST* RGWRESTMgr_S3::get_handler(struct req_state* const s,
                                            const rgw::auth::StrategyRegistry& auth_registry,
                                            const std::string& frontend_prefix)
{
  bool is_s3website = enable_s3website && (s->prot_flags & RGW_REST_WEBSITE);
  int ret =
    RGWHandler_REST_S3::init_from_header(s,
					is_s3website ? RGW_FORMAT_HTML :
					RGW_FORMAT_XML, true);
  if (ret < 0)
    return NULL;

  RGWHandler_REST* handler;
  // TODO: Make this more readable
  if (is_s3website) {
    if (s->init_state.url_bucket.empty()) {
      handler = new RGWHandler_REST_Service_S3Website(auth_registry);
    } else if (s->object.empty()) {
      handler = new RGWHandler_REST_Bucket_S3Website(auth_registry);
    } else {
      handler = new RGWHandler_REST_Obj_S3Website(auth_registry);
    }
  } else {
    if (s->init_state.url_bucket.empty()) {
      handler = new RGWHandler_REST_Service_S3(auth_registry);
    } else if (s->object.empty()) {
      handler = new RGWHandler_REST_Bucket_S3(auth_registry);
    } else {
      handler = new RGWHandler_REST_Obj_S3(auth_registry);
    }
  }

  ldout(s->cct, 20) << __func__ << " handler=" << typeid(*handler).name()
		    << dendl;
  return handler;
}

bool RGWHandler_REST_S3Website::web_dir() const {
  std::string subdir_name = url_decode(s->object.name);

  if (subdir_name.empty()) {
    return false;
  } else if (subdir_name.back() == '/') {
    subdir_name.pop_back();
  }

  rgw_obj obj(s->bucket, subdir_name);

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  obj_ctx.obj.set_atomic(obj);
  obj_ctx.obj.set_prefetch_data(obj);

  RGWObjState* state = nullptr;
  if (store->get_obj_state(&obj_ctx, s->bucket_info, obj, &state, false) < 0) {
    return false;
  }
  if (! state->exists) {
    return false;
  }
  return state->exists;
}

int RGWHandler_REST_S3Website::retarget(RGWOp* op, RGWOp** new_op) {
  *new_op = op;
  ldout(s->cct, 10) << __func__ << "Starting retarget" << dendl;

  if (!(s->prot_flags & RGW_REST_WEBSITE))
    return 0;

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  int ret = store->get_bucket_info(obj_ctx, s->bucket_tenant,
				  s->bucket_name, s->bucket_info, NULL,
				  &s->bucket_attrs);
  if (ret < 0) {
      // TODO-FUTURE: if the bucket does not exist, maybe expose it here?
      return -ERR_NO_SUCH_BUCKET;
  }
  if (!s->bucket_info.has_website) {
      // TODO-FUTURE: if the bucket has no WebsiteConfig, expose it here
      return -ERR_NO_SUCH_WEBSITE_CONFIGURATION;
  }

  rgw_obj_key new_obj;
  s->bucket_info.website_conf.get_effective_key(s->object.name, &new_obj.name, web_dir());
  ldout(s->cct, 10) << "retarget get_effective_key " << s->object << " -> "
		    << new_obj << dendl;

  RGWBWRoutingRule rrule;
  bool should_redirect =
    s->bucket_info.website_conf.should_redirect(new_obj.name, 0, &rrule);

  if (should_redirect) {
    const string& hostname = s->info.env->get("HTTP_HOST", "");
    const string& protocol =
      (s->info.env->get("SERVER_PORT_SECURE") ? "https" : "http");
    int redirect_code = 0;
    rrule.apply_rule(protocol, hostname, s->object.name, &s->redirect,
		    &redirect_code);
    // APply a custom HTTP response code
    if (redirect_code > 0)
      s->err.http_ret = redirect_code; // Apply a custom HTTP response code
    ldout(s->cct, 10) << "retarget redirect code=" << redirect_code
		      << " proto+host:" << protocol << "://" << hostname
		      << " -> " << s->redirect << dendl;
    return -ERR_WEBSITE_REDIRECT;
  }

  /*
   * FIXME: if s->object != new_obj, drop op and create a new op to handle
   * operation. Or remove this comment if it's not applicable anymore
   */

  s->object = new_obj;

  return 0;
}

RGWOp* RGWHandler_REST_S3Website::op_get()
{
  return get_obj_op(true);
}

RGWOp* RGWHandler_REST_S3Website::op_head()
{
  return get_obj_op(false);
}

int RGWHandler_REST_S3Website::serve_errordoc(int http_ret, const string& errordoc_key) {
  int ret = 0;
  s->formatter->reset(); /* Try to throw it all away */

  std::shared_ptr<RGWGetObj_ObjStore_S3Website> getop( static_cast<RGWGetObj_ObjStore_S3Website*>(op_get()));
  if (getop.get() == NULL) {
    return -1; // Trigger double error handler
  }
  getop->init(store, s, this);
  getop->range_str = NULL;
  getop->if_mod = NULL;
  getop->if_unmod = NULL;
  getop->if_match = NULL;
  getop->if_nomatch = NULL;
  s->object = errordoc_key;

  ret = init_permissions(getop.get());
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, init_permissions ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = read_permissions(getop.get());
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, read_permissions ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  if (http_ret) {
     getop->set_custom_http_response(http_ret);
  }

  ret = getop->init_processing();
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, init_processing ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_op_mask();
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, verify_op_mask ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_permission();
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, verify_permission ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_params();
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, verify_params ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  // No going back now
  getop->pre_exec();
  /*
   * FIXME Missing headers:
   * With a working errordoc, the s3 error fields are rendered as HTTP headers,
   *   x-amz-error-code: NoSuchKey
   *   x-amz-error-message: The specified key does not exist.
   *   x-amz-error-detail-Key: foo
   */
  getop->execute();
  getop->complete();
  return 0;

}

int RGWHandler_REST_S3Website::error_handler(int err_no,
					    string* error_content) {
  int new_err_no = -1;
  rgw_http_errors::const_iterator r = rgw_http_s3_errors.find(err_no > 0 ? err_no : -err_no);
  int http_error_code = -1;

  if (r != rgw_http_s3_errors.end()) {
    http_error_code = r->second.first;
  }
  ldout(s->cct, 10) << "RGWHandler_REST_S3Website::error_handler err_no=" << err_no << " http_ret=" << http_error_code << dendl;

  RGWBWRoutingRule rrule;
  bool should_redirect =
    s->bucket_info.website_conf.should_redirect(s->object.name, http_error_code,
						&rrule);

  if (should_redirect) {
    const string& hostname = s->info.env->get("HTTP_HOST", "");
    const string& protocol =
      (s->info.env->get("SERVER_PORT_SECURE") ? "https" : "http");
    int redirect_code = 0;
    rrule.apply_rule(protocol, hostname, s->object.name, &s->redirect,
		    &redirect_code);
    // Apply a custom HTTP response code
    if (redirect_code > 0)
      s->err.http_ret = redirect_code; // Apply a custom HTTP response code
    ldout(s->cct, 10) << "error handler redirect code=" << redirect_code
		      << " proto+host:" << protocol << "://" << hostname
		      << " -> " << s->redirect << dendl;
    return -ERR_WEBSITE_REDIRECT;
  } else if (err_no == -ERR_WEBSITE_REDIRECT) {
    // Do nothing here, this redirect will be handled in abort_early's ERR_WEBSITE_REDIRECT block
    // Do NOT fire the ErrorDoc handler
  } else if (!s->bucket_info.website_conf.error_doc.empty()) {
    /* This serves an entire page!
       On success, it will return zero, and no further content should be sent to the socket
       On failure, we need the double-error handler
     */
    new_err_no = RGWHandler_REST_S3Website::serve_errordoc(http_error_code, s->bucket_info.website_conf.error_doc);
    if (new_err_no && new_err_no != -1) {
      err_no = new_err_no;
    }
  } else {
    ldout(s->cct, 20) << "No special error handling today!" << dendl;
  }

  return err_no;
}

RGWOp* RGWHandler_REST_Obj_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}

RGWOp* RGWHandler_REST_Bucket_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}

RGWOp* RGWHandler_REST_Service_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}


namespace rgw {
namespace auth {
namespace s3 {

bool AWSGeneralAbstractor::is_time_skew_ok(const utime_t& header_time,
                                           const bool qsr) const
{
  /* Check for time skew first. */
  const time_t req_sec = header_time.sec();
  time_t now;
  time(&now);

  if ((req_sec < now - RGW_AUTH_GRACE_MINS * 60 ||
       req_sec > now + RGW_AUTH_GRACE_MINS * 60) && !qsr) {
    ldout(cct, 10) << "req_sec=" << req_sec << " now=" << now
                   << "; now - RGW_AUTH_GRACE_MINS="
                   << now - RGW_AUTH_GRACE_MINS * 60
                   << "; now + RGW_AUTH_GRACE_MINS="
                   << now + RGW_AUTH_GRACE_MINS * 60
                   << dendl;

    ldout(cct, 0)  << "NOTICE: request time skew too big now="
                   << utime_t(now, 0)
                   << " req_time=" << header_time
                   << dendl;
    return false;
  } else {
    return true;
  }
}


static rgw::auth::Completer::cmplptr_t
null_completer_factory(const boost::optional<std::string>& secret_key)
{
  return nullptr;
}


AWSEngine::VersionAbstractor::auth_data_t
AWSGeneralAbstractor::get_auth_data(const req_state* const s) const
{
  AwsVersion version;
  AwsRoute route;
  std::tie(version, route) = discover_aws_flavour(s->info);

  if (version == AwsVersion::V2) {
    return get_auth_data_v2(s);
  } else if (version == AwsVersion::V4) {
    return get_auth_data_v4(s, route == AwsRoute::QUERY_STRING);
  } else {
    /* FIXME(rzarzynski): handle anon user. */
    throw -EINVAL;
  }
}

boost::optional<std::string>
AWSGeneralAbstractor::get_v4_canonical_headers(
  const req_info& info,
  const boost::string_view& signedheaders,
  const bool using_qs) const
{
  return rgw::auth::s3::get_v4_canonical_headers(info, signedheaders,
                                                 using_qs, false);
}

AWSEngine::VersionAbstractor::auth_data_t
AWSGeneralAbstractor::get_auth_data_v4(const req_state* const s,
                                       /* FIXME: const. */
                                       bool using_qs) const
{
  boost::string_view access_key_id;
  boost::string_view signed_hdrs;

  boost::string_view date;
  boost::string_view credential_scope;
  boost::string_view client_signature;

  int ret = rgw::auth::s3::parse_credentials(s->info,
                                             access_key_id,
                                             credential_scope,
                                             signed_hdrs,
                                             client_signature,
                                             date,
                                             using_qs);
  if (ret < 0) {
    throw ret;
  }

  /* craft canonical headers */
  boost::optional<std::string> canonical_headers = \
    get_v4_canonical_headers(s->info, signed_hdrs, using_qs);
  if (canonical_headers) {
    ldout(s->cct, 10) << "canonical headers format = " << *canonical_headers
                      << dendl;
  } else {
    throw -EPERM;
  }

  /* Get the expected hash. */
  auto exp_payload_hash = rgw::auth::s3::get_v4_exp_payload_hash(s->info);

  /* Craft canonical URI. Using std::move later so let it be non-const. */
  auto canonical_uri = rgw::auth::s3::get_v4_canonical_uri(s->info);

  /* Craft canonical query string. std::moving later so non-const here. */
  auto canonical_qs = rgw::auth::s3::get_v4_canonical_qs(s->info, using_qs);

  /* Craft canonical request. */
  auto canonical_req_hash = \
    rgw::auth::s3::get_v4_canon_req_hash(s->cct,
                                         s->info.method,
                                         std::move(canonical_uri),
                                         std::move(canonical_qs),
                                         std::move(*canonical_headers),
                                         signed_hdrs,
                                         exp_payload_hash);

  auto string_to_sign = \
    rgw::auth::s3::get_v4_string_to_sign(s->cct,
                                         AWS4_HMAC_SHA256_STR,
                                         date,
                                         credential_scope,
                                         std::move(canonical_req_hash));

  const auto sig_factory = std::bind(rgw::auth::s3::get_v4_signature,
                                     credential_scope,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3);

  /* Requests authenticated with the Query Parameters are treated as unsigned.
   * From "Authenticating Requests: Using Query Parameters (AWS Signature
   * Version 4)":
   *
   *   You don't include a payload hash in the Canonical Request, because
   *   when you create a presigned URL, you don't know the payload content
   *   because the URL is used to upload an arbitrary payload. Instead, you
   *   use a constant string UNSIGNED-PAYLOAD.
   *
   * This means we have absolutely no business in spawning completer. Both
   * aws4_auth_needs_complete and aws4_auth_streaming_mode are set to false
   * by default. We don't need to change that. */
  if (is_v4_payload_unsigned(exp_payload_hash) || is_v4_payload_empty(s)) {
    return {
      access_key_id,
      client_signature,
      std::move(string_to_sign),
      sig_factory,
      null_completer_factory
    };
  } else {
    /* We're going to handle a signed payload. Be aware that even empty HTTP
     * body (no payload) requires verification:
     *
     *   The x-amz-content-sha256 header is required for all AWS Signature
     *   Version 4 requests. It provides a hash of the request payload. If
     *   there is no payload, you must provide the hash of an empty string. */
    if (!is_v4_payload_streamed(exp_payload_hash)) {
      ldout(s->cct, 10) << "delaying v4 auth" << dendl;

      /* payload in a single chunk */
      switch (s->op_type)
      {
        case RGW_OP_CREATE_BUCKET:
        case RGW_OP_PUT_OBJ:
        case RGW_OP_PUT_ACLS:
        case RGW_OP_PUT_CORS:
        case RGW_OP_COMPLETE_MULTIPART:
        case RGW_OP_SET_BUCKET_VERSIONING:
        case RGW_OP_DELETE_MULTI_OBJ:
        case RGW_OP_ADMIN_SET_METADATA:
        case RGW_OP_SET_BUCKET_WEBSITE:
        case RGW_OP_PUT_BUCKET_POLICY:
        case RGW_OP_PUT_OBJ_TAGGING:
          break;
        default:
          dout(10) << "ERROR: AWS4 completion for this operation NOT IMPLEMENTED" << dendl;
          throw -ERR_NOT_IMPLEMENTED;
      }

      const auto cmpl_factory = std::bind(AWSv4ComplSingle::create,
                                          s,
                                          std::placeholders::_1);
      return {
        access_key_id,
        client_signature,
        std::move(string_to_sign),
        sig_factory,
        cmpl_factory
      };
    } else {
      /* IMHO "streamed" doesn't fit too good here. I would prefer to call
       * it "chunked" but let's be coherent with Amazon's terminology. */

      dout(10) << "body content detected in multiple chunks" << dendl;

      /* payload in multiple chunks */

      switch(s->op_type)
      {
        case RGW_OP_PUT_OBJ:
          break;
        default:
          dout(10) << "ERROR: AWS4 completion for this operation NOT IMPLEMENTED (streaming mode)" << dendl;
          throw -ERR_NOT_IMPLEMENTED;
      }

      dout(10) << "aws4 seed signature ok... delaying v4 auth" << dendl;

      /* In the case of streamed payload client sets the x-amz-content-sha256
       * to "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" but uses "UNSIGNED-PAYLOAD"
       * when constructing the Canonical Request. */

      /* In the case of single-chunk upload client set the header's value is
       * coherent with the one used for Canonical Request crafting. */

      /* In the case of query string-based authentication there should be no
       * x-amz-content-sha256 header and the value "UNSIGNED-PAYLOAD" is used
       * for CanonReq. */
      const auto cmpl_factory = std::bind(AWSv4ComplMulti::create,
                                          s,
                                          date,
                                          credential_scope,
                                          client_signature,
                                          std::placeholders::_1);
      return {
        access_key_id,
        client_signature,
        std::move(string_to_sign),
        sig_factory,
        cmpl_factory
      };
    }
  }
}


boost::optional<std::string>
AWSGeneralBoto2Abstractor::get_v4_canonical_headers(
  const req_info& info,
  const boost::string_view& signedheaders,
  const bool using_qs) const
{
  return rgw::auth::s3::get_v4_canonical_headers(info, signedheaders,
                                                 using_qs, true);
}


AWSEngine::VersionAbstractor::auth_data_t
AWSGeneralAbstractor::get_auth_data_v2(const req_state* const s) const
{
  boost::string_view access_key_id;
  boost::string_view signature;
  bool qsr = false;

  const char* http_auth = s->info.env->get("HTTP_AUTHORIZATION");
  if (! http_auth || http_auth[0] == '\0') {
    /* Credentials are provided in query string. We also need to verify
     * the "Expires" parameter now. */
    access_key_id = s->info.args.get("AWSAccessKeyId");
    signature = s->info.args.get("Signature");
    qsr = true;

    boost::string_view expires = s->info.args.get("Expires");
    if (! expires.empty()) {
      /* It looks we have the guarantee that expires is a null-terminated,
       * and thus string_view::data() can be safely used. */
      const time_t exp = atoll(expires.data());
      time_t now;
      time(&now);

      if (now >= exp) {
        throw -EPERM;
      }
    }
  } else {
    /* The "Authorization" HTTP header is being used. */
    const boost::string_view auth_str(http_auth + strlen("AWS "));
    const size_t pos = auth_str.rfind(':');
    if (pos != boost::string_view::npos) {
      access_key_id = auth_str.substr(0, pos);
      signature = auth_str.substr(pos + 1);
    }
  }

  /* Let's canonize the HTTP headers that are covered by the AWS auth v2. */
  std::string string_to_sign;
  utime_t header_time;
  if (! rgw_create_s3_canonical_header(s->info, &header_time, string_to_sign,
        qsr)) {
    ldout(cct, 10) << "failed to create the canonized auth header\n"
                   << rgw::crypt_sanitize::auth{s,string_to_sign} << dendl;
    throw -EPERM;
  }

  ldout(cct, 10) << "string_to_sign:\n"
                 << rgw::crypt_sanitize::auth{s,string_to_sign} << dendl;

  if (! is_time_skew_ok(header_time, qsr)) {
    throw -ERR_REQUEST_TIME_SKEWED;
  }

  return {
    std::move(access_key_id),
    std::move(signature),
    std::move(string_to_sign),
    rgw::auth::s3::get_v2_signature,
    null_completer_factory
  };
}


AWSEngine::VersionAbstractor::auth_data_t
AWSBrowserUploadAbstractor::get_auth_data_v2(const req_state* const s) const
{
  return {
    s->auth.s3_postobj_creds.access_key,
    s->auth.s3_postobj_creds.signature,
    s->auth.s3_postobj_creds.encoded_policy.to_str(),
    rgw::auth::s3::get_v2_signature,
    null_completer_factory
  };
}

AWSEngine::VersionAbstractor::auth_data_t
AWSBrowserUploadAbstractor::get_auth_data_v4(const req_state* const s) const
{
  const boost::string_view credential = s->auth.s3_postobj_creds.x_amz_credential;

  /* grab access key id */
  const size_t pos = credential.find("/");
  const boost::string_view access_key_id = credential.substr(0, pos);
  dout(10) << "access key id = " << access_key_id << dendl;

  /* grab credential scope */
  const boost::string_view credential_scope = credential.substr(pos + 1);
  dout(10) << "credential scope = " << credential_scope << dendl;

  const auto sig_factory = std::bind(rgw::auth::s3::get_v4_signature,
                                     credential_scope,
                                     std::placeholders::_1,
                                     std::placeholders::_2,
                                     std::placeholders::_3);

  return {
    access_key_id,
    s->auth.s3_postobj_creds.signature,
    s->auth.s3_postobj_creds.encoded_policy.to_str(),
    sig_factory,
    null_completer_factory
  };
}

AWSEngine::VersionAbstractor::auth_data_t
AWSBrowserUploadAbstractor::get_auth_data(const req_state* const s) const
{
  if (s->auth.s3_postobj_creds.x_amz_algorithm == AWS4_HMAC_SHA256_STR) {
    ldout(s->cct, 0) << "Signature verification algorithm AWS v4"
                     << " (AWS4-HMAC-SHA256)" << dendl;
    return get_auth_data_v4(s);
  } else {
    ldout(s->cct, 0) << "Signature verification algorithm AWS v2" << dendl;
    return get_auth_data_v2(s);
  }
}


AWSEngine::result_t
AWSEngine::authenticate(const req_state* const s) const
{
  /* Small reminder: an ver_abstractor is allowed to throw! */
  const auto auth_data = ver_abstractor.get_auth_data(s);

  if (auth_data.access_key_id.empty() || auth_data.client_signature.empty()) {
    return result_t::deny(-EINVAL);
  } else {
    return authenticate(auth_data.access_key_id,
		        auth_data.client_signature,
			auth_data.string_to_sign,
                        auth_data.signature_factory,
			auth_data.completer_factory,
			s);
  }
}

} /* namespace s3 */
} /* namespace auth */
} /* namespace rgw */

rgw::LDAPHelper* rgw::auth::s3::LDAPEngine::ldh = nullptr;
std::mutex rgw::auth::s3::LDAPEngine::mtx;

void rgw::auth::s3::LDAPEngine::init(CephContext* const cct)
{
  if (! ldh) {
    std::lock_guard<std::mutex> lck(mtx);
    if (! ldh) {
      const string& ldap_uri = cct->_conf->rgw_ldap_uri;
      const string& ldap_binddn = cct->_conf->rgw_ldap_binddn;
      const string& ldap_searchdn = cct->_conf->rgw_ldap_searchdn;
      const string& ldap_searchfilter = cct->_conf->rgw_ldap_searchfilter;
      const string& ldap_dnattr = cct->_conf->rgw_ldap_dnattr;
      std::string ldap_bindpw = parse_rgw_ldap_bindpw(cct);

      ldh = new rgw::LDAPHelper(ldap_uri, ldap_binddn, ldap_bindpw,
                                ldap_searchdn, ldap_searchfilter, ldap_dnattr);

      ldh->init();
      ldh->bind();
    }
  }
}

rgw::auth::RemoteApplier::acl_strategy_t
rgw::auth::s3::LDAPEngine::get_acl_strategy() const
{
  //This is based on the assumption that the default acl strategy in
  // get_perms_from_aclspec, will take care. Extra acl spec is not required.
  return nullptr;
}

rgw::auth::RemoteApplier::AuthInfo
rgw::auth::s3::LDAPEngine::get_creds_info(const rgw::RGWToken& token) const noexcept
{
  /* The short form of "using" can't be used here -- we're aliasing a class'
   * member. */
  using acct_privilege_t = \
    rgw::auth::RemoteApplier::AuthInfo::acct_privilege_t;

  return rgw::auth::RemoteApplier::AuthInfo {
    rgw_user(token.id),
    token.id,
    RGW_PERM_FULL_CONTROL,
    acct_privilege_t::IS_PLAIN_ACCT,
    TYPE_LDAP
  };
}

rgw::auth::Engine::result_t
rgw::auth::s3::LDAPEngine::authenticate(
  const boost::string_view& access_key_id,
  const boost::string_view& signature,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t&,
  const completer_factory_t& completer_factory,
  const req_state* const s) const
{
  /* boost filters and/or string_ref may throw on invalid input */
  rgw::RGWToken base64_token;
  try {
    base64_token = rgw::from_base64(access_key_id);
  } catch (...) {
    base64_token = std::string("");
  }

  if (! base64_token.valid()) {
    return result_t::deny();
  }

  //TODO: Uncomment, when we have a migration plan in place.
  //Check if a user of type other than 'ldap' is already present, if yes, then
  //return error.
  /*RGWUserInfo user_info;
  user_info.user_id = base64_token.id;
  if (rgw_get_user_info_by_uid(store, user_info.user_id, user_info) >= 0) {
    if (user_info.type != TYPE_LDAP) {
      ldout(cct, 10) << "ERROR: User id of type: " << user_info.type << " is already present" << dendl;
      return nullptr;
    }
  }*/

  if (ldh->auth(base64_token.id, base64_token.key) != 0) {
    return result_t::deny();
  }

  auto apl = apl_factory->create_apl_remote(cct, s, get_acl_strategy(),
                                            get_creds_info(base64_token));
  return result_t::grant(std::move(apl), completer_factory(boost::none));
}


/* LocalEndgine */
rgw::auth::Engine::result_t
rgw::auth::s3::LocalEngine::authenticate(
  const boost::string_view& _access_key_id,
  const boost::string_view& signature,
  const string_to_sign_t& string_to_sign,
  const signature_factory_t& signature_factory,
  const completer_factory_t& completer_factory,
  const req_state* const s) const
{
  /* get the user info */
  RGWUserInfo user_info;
  /* TODO(rzarzynski): we need to have string-view taking variant. */
  const std::string access_key_id = _access_key_id.to_string();
  if (rgw_get_user_info_by_access_key(store, access_key_id, user_info) < 0) {
      ldout(cct, 5) << "error reading user info, uid=" << access_key_id
              << " can't authenticate" << dendl;
      return result_t::deny(-ERR_INVALID_ACCESS_KEY);
  }
  //TODO: Uncomment, when we have a migration plan in place.
  /*else {
    if (s->user->type != TYPE_RGW) {
      ldout(cct, 10) << "ERROR: User id of type: " << s->user->type
                     << " is present" << dendl;
      throw -EPERM;
    }
  }*/

  const auto iter = user_info.access_keys.find(access_key_id);
  if (iter == std::end(user_info.access_keys)) {
    ldout(cct, 0) << "ERROR: access key not encoded in user info" << dendl;
    return result_t::deny(-EPERM);
  }
  const RGWAccessKey& k = iter->second;

  const VersionAbstractor::server_signature_t server_signature = \
    signature_factory(cct, k.key, string_to_sign);

  ldout(cct, 15) << "string_to_sign="
                 << rgw::crypt_sanitize::log_content{string_to_sign}
                 << dendl;
  ldout(cct, 15) << "server signature=" << server_signature << dendl;
  ldout(cct, 15) << "client signature=" << signature << dendl;
  ldout(cct, 15) << "compare=" << signature.compare(server_signature) << dendl;

  if (static_cast<boost::string_view>(server_signature) != signature) {
    return result_t::deny(-ERR_SIGNATURE_NO_MATCH);
  }

  auto apl = apl_factory->create_apl_local(cct, s, user_info, k.subuser);
  return result_t::grant(std::move(apl), completer_factory(k.key));
}
