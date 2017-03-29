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
  ACLOwner owner;
  set<string> read_permissions;

  struct {
    uint64_t size;
    ceph::real_time mtime;
    string etag;
    map<string, string> custom_str;

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
      list<_custom_entry<string> > str_entries;
      JSONDecoder::decode_json("custom-string", str_entries, obj);
      for (auto& e : str_entries) {
        custom_str[e.name] = e.value;
      }
    }
  } meta;

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("bucket", bucket, obj);
    JSONDecoder::decode_json("name", key.name, obj);
    JSONDecoder::decode_json("instance", key.instance, obj);
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
  RGWMetadataSearchOp(RGWElasticSyncModuleInstance *_es_module) : es_module(_es_module) {}

  int verify_permission() {
    return 0;
  }
  virtual int get_params() = 0;
  void pre_exec();
  void execute();

  virtual void send_response() = 0;
  virtual const string name() { return "metadata_search"; }
  virtual RGWOpType get_type() { return RGW_OP_METADATA_SEARCH; }
  virtual uint32_t op_mask() { return RGW_OP_TYPE_READ; }
};

void RGWMetadataSearchOp::pre_exec()
{
  rgw_bucket_object_pre_exec(s);
}

void RGWMetadataSearchOp::execute()
{
  op_ret = get_params();
  if (op_ret < 0)
    return;

  list<pair<string, string> > conds;

  conds.push_back(make_pair("permissions", s->user->user_id.to_str()));

  if (!s->bucket_name.empty()) {
    conds.push_back(make_pair("bucket", s->bucket_name));
  }

  ESQueryCompiler es_query(expression, &conds, custom_prefix);
  
  static map<string, string> aliases = { { "key", "name" },
                                  { "etag", "meta.etag" },
                                  { "size", "meta.size" },
                                  { "mtime", "meta.mtime" },
                                  { "lastmodified", "meta.mtime" },
                                  { "contenttype", "meta.contenttype" },
  };
  es_query.set_field_aliases(&aliases);

  static map<string, ESEntityTypeMap::EntityType> generic_map = { {"bucket", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"name", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"instance", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"permissions", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.etag", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.contenttype", ESEntityTypeMap::ES_ENTITY_STR},
                                                           {"meta.mtime", ESEntityTypeMap::ES_ENTITY_DATE},
                                                           {"meta.size", ESEntityTypeMap::ES_ENTITY_INT} };
  ESEntityTypeMap gm(generic_map);
  es_query.set_generic_type_map(&gm);

  static set<string> restricted_fields = { {"permissions"} };
  es_query.set_restricted_fields(&restricted_fields);

  static map<string, ESEntityTypeMap::EntityType> custom_map = { };
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

  string resource = es_module->get_index_path(store->get_realm()) + "/_search";
  param_vec_t params;
#define BUFSIZE 32
  char buf[BUFSIZE];
  snprintf(buf, sizeof(buf), "%lld", (long long)max_keys);
  params.push_back(param_pair_t("size", buf));
  if (marker > 0) {
    params.push_back(param_pair_t("from", marker_str.c_str()));
  }
  ldout(s->cct, 20) << "sending request to elasticsearch, payload=" << string(in.c_str(), in.length()) << dendl;
  op_ret = conn->get_resource(resource, &params, nullptr, out, &in);
  if (op_ret < 0) {
    ldout(s->cct, 0) << "ERROR: failed to fetch resource (r=" << resource << ", ret=" << op_ret << ")" << dendl;
    return;
  }

  ldout(s->cct, 20) << "response: " << string(out.c_str(), out.length()) << dendl;

  JSONParser jparser;
  if (!jparser.parse(out.c_str(), out.length())) {
    ldout(s->cct, 0) << "ERROR: failed to parser elasticsearch response" << dendl;
    op_ret = -EINVAL;
    return;
  }

  try {
    decode_json_obj(response, &jparser);
  } catch (JSONDecoder::err& e) {
    ldout(s->cct, 0) << "ERROR: failed to decode JSON input: " << e.message << dendl;
    op_ret = -EINVAL;
    return;
  }

}

class RGWMetadataSearch_ObjStore_S3 : public RGWMetadataSearchOp {
public:
  RGWMetadataSearch_ObjStore_S3(RGWElasticSyncModuleInstance *_es_module) : RGWMetadataSearchOp(_es_module) {
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
    char buf[BUFSIZE];
    snprintf(buf, sizeof(buf), "%lld", (long long)nm);
    next_marker = buf;
    return 0;
  }
  void send_response() override {
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
    for (auto& i : response.hits.hits) {
      es_index_obj_response& e = i.source;
      s->formatter->open_object_section("Contents");
      s->formatter->dump_string("Bucket", e.bucket);
      s->formatter->dump_string("Key", e.key.name);
      string instance = (!e.key.instance.empty() ? e.key.instance : "null");
      s->formatter->dump_string("Instance", instance.c_str());
      dump_time(s, "LastModified", &e.meta.mtime);
      s->formatter->dump_format("ETag", "\"%s\"", e.meta.etag.c_str());
      dump_owner(s, e.owner.get_id(), e.owner.get_display_name());
      s->formatter->open_array_section("CustomMetadata");
      for (auto& m : e.meta.custom_str) {
        s->formatter->open_object_section("Entry");
        s->formatter->dump_string("Name", m.first.c_str());
        s->formatter->dump_string("Value", m.second);
        s->formatter->close_section();
      }
      s->formatter->close_section();
      s->formatter->close_section();
      rgw_flush_formatter(s, s->formatter);
    };
    s->formatter->close_section();
   rgw_flush_formatter_and_reset(s, s->formatter);
  }
};

class RGWHandler_REST_MDSearch_S3 : public RGWHandler_REST_S3 {
  RGWElasticSyncModuleInstance *es_module;
protected:
  RGWOp *op_get() {
    if (!s->info.args.exists("query")) {
      return nullptr;
    }
    return new RGWMetadataSearch_ObjStore_S3(es_module);
  }
  RGWOp *op_head() {
    return nullptr;
  }
  RGWOp *op_post() {
    return nullptr;
  }
public:
  RGWHandler_REST_MDSearch_S3(const rgw::auth::StrategyRegistry& auth_registry,
                              RGWElasticSyncModuleInstance *_es_module) : RGWHandler_REST_S3(auth_registry), es_module(_es_module) {}
  virtual ~RGWHandler_REST_MDSearch_S3() {}
};


RGWHandler_REST* RGWRESTMgr_MDSearch_S3::get_handler(struct req_state* const s,
                                                     const rgw::auth::StrategyRegistry& auth_registry,
                                                     const std::string& frontend_prefix)
{
  int ret =
    RGWHandler_REST_S3::init_from_header(s,
					RGW_FORMAT_XML, true);
  if (ret < 0) {
    return nullptr;
  }

  if (!s->object.empty()) {
    return nullptr;
  }

  RGWHandler_REST *handler = new RGWHandler_REST_MDSearch_S3(auth_registry, es_module);

  ldout(s->cct, 20) << __func__ << " handler=" << typeid(*handler).name()
		    << dendl;
  return handler;
}

