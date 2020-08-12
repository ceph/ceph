// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_sync_module_es.h"
#include "rgw_sync_module_es_rest.h"
#include "rgw_es_query.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_s3.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

struct es_index_obj_response {
  string bucket;
  rgw_obj_key key;
  uint64_t versioned_epoch{0};
  ACLOwner owner;
  set<string> read_permissions;

  struct {
    uint64_t size{0};
    ceph::real_time mtime;
    string etag;
    string content_type;
    string storage_class;
    map<string, string> custom_str;
    map<string, int64_t> custom_int;
    map<string, string> custom_date;

    template <class T>
    struct _custom_entry {
      string name;
      T value;
      void decode_json(JSONObj *obj) {
        JSONDecoder::decode_json("name", name, obj);
        JSONDecoder::decode_json("value", value, obj);
      }
    };

    void decode_json(JSONObj *obj) {
      JSONDecoder::decode_json("size", size, obj);
      string mtime_str;
      JSONDecoder::decode_json("mtime", mtime_str, obj);
      parse_time(mtime_str.c_str(), &mtime);
      JSONDecoder::decode_json("etag", etag, obj);
      JSONDecoder::decode_json("content_type", content_type, obj);
      JSONDecoder::decode_json("storage_class", storage_class, obj);
      list<_custom_entry<string> > str_entries;
      JSONDecoder::decode_json("custom-string", str_entries, obj);
      for (auto& e : str_entries) {
        custom_str[e.name] = e.value;
      }
      list<_custom_entry<int64_t> > int_entries;
      JSONDecoder::decode_json("custom-int", int_entries, obj);
      for (auto& e : int_entries) {
        custom_int[e.name] = e.value;
      }
      list<_custom_entry<string> > date_entries;
      JSONDecoder::decode_json("custom-date", date_entries, obj);
      for (auto& e : date_entries) {
        custom_date[e.name] = e.value;
      }
    }
  } meta;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("bucket", bucket, obj);
    JSONDecoder::decode_json("name", key.name, obj);
    JSONDecoder::decode_json("instance", key.instance, obj);
    JSONDecoder::decode_json("versioned_epoch", versioned_epoch, obj);
    JSONDecoder::decode_json("permissions", read_permissions, obj);
    JSONDecoder::decode_json("owner", owner, obj);
    JSONDecoder::decode_json("meta", meta, obj);
  }
};

struct es_search_response {
  uint32_t took;
  bool timed_out;
  struct {
    uint32_t total;
    uint32_t successful;
    uint32_t failed;
    void decode_json(JSONObj *obj) {
      JSONDecoder::decode_json("total", total, obj);
      JSONDecoder::decode_json("successful", successful, obj);
      JSONDecoder::decode_json("failed", failed, obj);
    }
  } shards;
  struct obj_hit {
    string index;
    string type;
    string id;
    // double score
    es_index_obj_response source;
    void decode_json(JSONObj *obj) {
      JSONDecoder::decode_json("_index", index, obj);
      JSONDecoder::decode_json("_type", type, obj);
      JSONDecoder::decode_json("_id", id, obj);
      JSONDecoder::decode_json("_source", source, obj);
    }
  };
  struct {
    uint32_t total;
    // double max_score;
    list<obj_hit> hits;
    void decode_json(JSONObj *obj) {
      JSONDecoder::decode_json("total", total, obj);
      // JSONDecoder::decode_json("max_score", max_score, obj);
      JSONDecoder::decode_json("hits", hits, obj);
    }
  } hits;
  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("took", took, obj);
    JSONDecoder::decode_json("timed_out", timed_out, obj);
    JSONDecoder::decode_json("_shards", shards, obj);
    JSONDecoder::decode_json("hits", hits, obj);
  }
};

class RGWMetadataSearchOp : public RGWOp {
  RGWSyncModuleInstanceRef sync_module_ref;
  RGWElasticSyncModuleInstance *es_module;
protected:
  string expression;
  string custom_prefix;
#define MAX_KEYS_DEFAULT 100
  uint64_t max_keys{MAX_KEYS_DEFAULT};
  string marker_str;
  uint64_t marker{0};
  string next_marker;
  bool is_truncated{false};
  string err;

  es_search_response response;

public:
  RGWMetadataSearchOp(const RGWSyncModuleInstanceRef& sync_module) : sync_module_ref(sync_module) {
    es_module = static_cast<RGWElasticSyncModuleInstance *>(sync_module_ref.get());
  }

  int verify_permission(const Span& parent_span = nullptr) override {
    return 0;
  }
  virtual int get_params() = 0;
  void pre_exec(const Span& parent_span = nullptr) override;
  void execute(const Span& parent_span = nullptr) override;

  const char* name() const override { return "metadata_search"; }
  virtual RGWOpType get_type() override { return RGW_OP_METADATA_SEARCH; }
  virtual uint32_t op_mask() override { return RGW_OP_TYPE_READ; }
};

void RGWMetadataSearchOp::pre_exec(const Span& parent_span)
{
  rgw_bucket_object_pre_exec(s);
}

void RGWMetadataSearchOp::execute(const Span& parent_span)
{
  op_ret = get_params();
  if (op_ret < 0)
    return;

  list<pair<string, string> > conds;

  if (!s->user->get_info().system) {
    conds.push_back(make_pair("permissions", s->user->get_id().to_str()));
  }

  if (!s->bucket_name.empty()) {
    conds.push_back(make_pair("bucket", s->bucket_name));
  }

  ESQueryCompiler es_query(expression, &conds, custom_prefix);
  
  static map<string, string, ltstr_nocase> aliases = {
                                  { "bucket", "bucket" }, /* forces lowercase */
                                  { "name", "name" },
                                  { "key", "name" },
                                  { "instance", "instance" },
                                  { "etag", "meta.etag" },
                                  { "size", "meta.size" },
                                  { "mtime", "meta.mtime" },
                                  { "lastmodified", "meta.mtime" },
                                  { "last_modified", "meta.mtime" },
                                  { "contenttype", "meta.content_type" },
                                  { "content_type", "meta.content_type" },
                                  { "storageclass", "meta.storage_class" },
                                  { "storage_class", "meta.storage_class" },
  };
  es_query.set_field_aliases(&aliases);

  static map<string, ESEntityTypeMap::EntityType> generic_map = { {"bucket", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"name", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"instance", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"permissions", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.etag", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.content_type", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.mtime", ESEntityTypeMap::ES_ENTITY_DATE},
                                                           {"meta.size", ESEntityTypeMap::ES_ENTITY_INT},
                                                           {"meta.storage_class", ESEntityTypeMap::ES_ENTITY_STR} };
  ESEntityTypeMap gm(generic_map);
  es_query.set_generic_type_map(&gm);

  static set<string> restricted_fields = { {"permissions"} };
  es_query.set_restricted_fields(&restricted_fields);

  map<string, ESEntityTypeMap::EntityType> custom_map;
  for (auto& i : s->bucket->get_info().mdsearch_config) {
    custom_map[i.first] = (ESEntityTypeMap::EntityType)i.second;
  }

  ESEntityTypeMap em(custom_map);
  es_query.set_custom_type_map(&em);

  bool valid = es_query.compile(&err);
  if (!valid) {
    ldout(s->cct, 10) << "invalid query, failed generating request json" << dendl;
    op_ret = -EINVAL;
    return;
  }

  JSONFormatter f;
  encode_json("root", es_query, &f);

  RGWRESTConn *conn = es_module->get_rest_conn();

  bufferlist in;
  bufferlist out;

  stringstream ss;

  f.flush(ss);
  in.append(ss.str());

  string resource = es_module->get_index_path() + "/_search";
  param_vec_t params;
  static constexpr int BUFSIZE = 32;
  char buf[BUFSIZE];
  snprintf(buf, sizeof(buf), "%lld", (long long)max_keys);
  params.push_back(param_pair_t("size", buf));
  if (marker > 0) {
    params.push_back(param_pair_t("from", marker_str.c_str()));
  }
  ldout(s->cct, 20) << "sending request to elasticsearch, payload=" << string(in.c_str(), in.length()) << dendl;
  auto& extra_headers = es_module->get_request_headers();
  op_ret = conn->get_resource(resource, &params, &extra_headers, out, &in);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "ERROR: failed to fetch resource (r=" << resource << ", ret=" << op_ret << ")" << dendl;
    return;
  }

  ldout(s->cct, 20) << "response: " << string(out.c_str(), out.length()) << dendl;

  JSONParser jparser;
  if (!jparser.parse(out.c_str(), out.length())) {
    ldout(s->cct, 0) << "ERROR: failed to parse elasticsearch response" << dendl;
    op_ret = -EINVAL;
    return;
  }

  try {
    decode_json_obj(response, &jparser);
  } catch (const JSONDecoder::err& e) {
    ldout(s->cct, 0) << "ERROR: failed to decode JSON input: " << e.what() << dendl;
    op_ret = -EINVAL;
    return;
  }

}

class RGWMetadataSearch_ObjStore_S3 : public RGWMetadataSearchOp {
public:
  explicit RGWMetadataSearch_ObjStore_S3(const RGWSyncModuleInstanceRef& _sync_module) : RGWMetadataSearchOp(_sync_module) {
    custom_prefix = "x-amz-meta-";
  }

  int get_params() override {
    expression = s->info.args.get("query");
    bool exists;
    string max_keys_str = s->info.args.get("max-keys", &exists);
#define MAX_KEYS_MAX 10000
    if (exists) {
      string err;
      max_keys = strict_strtoll(max_keys_str.c_str(), 10, &err);
      if (!err.empty()) {
        return -EINVAL;
      }
      if (max_keys > MAX_KEYS_MAX) {
        max_keys = MAX_KEYS_MAX;
      }
    }
    marker_str = s->info.args.get("marker", &exists);
    if (exists) {
      string err;
      marker = strict_strtoll(marker_str.c_str(), 10, &err);
      if (!err.empty()) {
        return -EINVAL;
      }
    }
    uint64_t nm = marker + max_keys;
    static constexpr int BUFSIZE = 32;
    char buf[BUFSIZE];
    snprintf(buf, sizeof(buf), "%lld", (long long)nm);
    next_marker = buf;
    return 0;
  }
  void send_response(const Span& parent_span = nullptr) override {
    if (op_ret) {
      s->err.message = err;
      set_req_state_err(s, op_ret);
    }
    dump_errno(s);
    end_header(s, this, "application/xml");

    if (op_ret < 0) {
      return;
    }

    is_truncated = (response.hits.hits.size() >= max_keys);

    s->formatter->open_object_section("SearchMetadataResponse");
    s->formatter->dump_string("Marker", marker_str);
    s->formatter->dump_string("IsTruncated", (is_truncated ? "true" : "false"));
    if (is_truncated) {
      s->formatter->dump_string("NextMarker", next_marker);
    }
    if (s->format == RGW_FORMAT_JSON) {
      s->formatter->open_array_section("Objects");
    }
    for (auto& i : response.hits.hits) {
      s->formatter->open_object_section("Contents");
      es_index_obj_response& e = i.source;
      s->formatter->dump_string("Bucket", e.bucket);
      s->formatter->dump_string("Key", e.key.name);
      string instance = (!e.key.instance.empty() ? e.key.instance : "null");
      s->formatter->dump_string("Instance", instance.c_str());
      s->formatter->dump_int("VersionedEpoch", e.versioned_epoch);
      dump_time(s, "LastModified", &e.meta.mtime);
      s->formatter->dump_int("Size", e.meta.size);
      s->formatter->dump_format("ETag", "\"%s\"", e.meta.etag.c_str());
      s->formatter->dump_string("ContentType", e.meta.content_type.c_str());
      s->formatter->dump_string("StorageClass", e.meta.storage_class.c_str());
      dump_owner(s, e.owner.get_id(), e.owner.get_display_name());
      s->formatter->open_array_section("CustomMetadata");
      for (auto& m : e.meta.custom_str) {
        s->formatter->open_object_section("Entry");
        s->formatter->dump_string("Name", m.first.c_str());
        s->formatter->dump_string("Value", m.second);
        s->formatter->close_section();
      }
      for (auto& m : e.meta.custom_int) {
        s->formatter->open_object_section("Entry");
        s->formatter->dump_string("Name", m.first.c_str());
        s->formatter->dump_int("Value", m.second);
        s->formatter->close_section();
      }
      for (auto& m : e.meta.custom_date) {
        s->formatter->open_object_section("Entry");
        s->formatter->dump_string("Name", m.first.c_str());
        s->formatter->dump_string("Value", m.second);
        s->formatter->close_section();
      }
      s->formatter->close_section();
      rgw_flush_formatter(s, s->formatter);
      s->formatter->close_section();
    };
    if (s->format == RGW_FORMAT_JSON) {
      s->formatter->close_section();
    }
    s->formatter->close_section();
   rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

class RGWHandler_REST_MDSearch_S3 : public RGWHandler_REST_S3 {
protected:
  RGWOp *op_get() override {
    if (s->info.args.exists("query")) {
      return new RGWMetadataSearch_ObjStore_S3(store->getRados()->get_sync_module());
    }
    if (!s->init_state.url_bucket.empty() &&
        s->info.args.exists("mdsearch")) {
      return new RGWGetBucketMetaSearch_ObjStore_S3;
    }
    return nullptr;
  }
  RGWOp *op_head() override {
    return nullptr;
  }
  RGWOp *op_post() override {
    return nullptr;
  }
public:
  explicit RGWHandler_REST_MDSearch_S3(const rgw::auth::StrategyRegistry& auth_registry) : RGWHandler_REST_S3(auth_registry) {}
  virtual ~RGWHandler_REST_MDSearch_S3() {}
};


RGWHandler_REST* RGWRESTMgr_MDSearch_S3::get_handler(rgw::sal::RGWRadosStore *store,
						     struct req_state* const s,
                                                     const rgw::auth::StrategyRegistry& auth_registry,
                                                     const std::string& frontend_prefix)
{
  int ret =
    RGWHandler_REST_S3::init_from_header(store, s,
					RGW_FORMAT_XML, true);
  if (ret < 0) {
    return nullptr;
  }

  if (!s->object->empty()) {
    return nullptr;
  }

  RGWHandler_REST *handler = new RGWHandler_REST_MDSearch_S3(auth_registry);

  ldout(s->cct, 20) << __func__ << " handler=" << typeid(*handler).name()
		    << dendl;
  return handler;
}

