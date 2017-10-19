#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_aws.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"
#include "rgw_acl.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

// TODO: have various bucket naming schemes at a global/user and a bucket level

static string aws_bucket_name(const RGWBucketInfo& bucket_info, bool user_buckets=false){
  string bucket_name="rgwx" + bucket_info.zonegroup;
  if (user_buckets){
    bucket_name+=bucket_info.owner.tenant + bucket_info.owner.id;
  }
  bucket_name.erase(std::remove(bucket_name.begin(),bucket_name.end(),'-'));
  return bucket_name;
}

static string aws_object_name(const RGWBucketInfo& bucket_info, const rgw_obj_key&key, bool user_buckets=false){
  string bucket_name = aws_bucket_name(bucket_info, user_buckets);
  string object_name;
  object_name += bucket_info.owner.to_str() + "/";
  object_name += bucket_info.bucket.name + "/" + key.name;
  return object_name;
}

static string obj_to_aws_path(const rgw_obj& obj)
{
  return obj.bucket.name + "/" + obj.key.name;
}

struct AWSConfig {
  string id;
  std::unique_ptr<RGWRESTConn> conn;
};

class RGWRESTStreamGetCRF : public RGWStreamReadHTTPResourceCRF
{
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *conn;
  rgw_obj src_obj;
  RGWRESTConn::get_obj_params req_params;
public:
  RGWRESTStreamGetCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWDataSyncEnv *_sync_env,
                               RGWRESTConn *_conn,
                               rgw_obj& _src_obj) : RGWStreamReadHTTPResourceCRF(_cct, _env, _caller, _sync_env->http_manager),
                                                                                 sync_env(_sync_env), conn(_conn), src_obj(_src_obj) {
  }

  int init() override {
    /* init input connection */


    req_params.get_op = true;
    req_params.prepend_metadata = true;

    if (range.is_set) {
      req_params.range_is_set = true;
      req_params.range_start = range.ofs;
      req_params.range_end = range.ofs + range.size - 1;
    }

    RGWRESTStreamRWRequest *in_req;
    int ret = conn->get_obj(src_obj, req_params, false /* send */, &in_req);
    if (ret < 0) {
      ldout(sync_env->cct, 0) << "ERROR: " << __func__ << "(): conn->get_obj() returned ret=" << ret << dendl;
      return ret;
    }

    set_req(in_req);

    return RGWStreamReadHTTPResourceCRF::init();
  }

  int decode_rest_obj(map<string, string>& headers, bufferlist& extra_data, rgw_rest_obj *info) override {
    for (auto header : headers) {
      const string& val = header.second;
      if (header.first == "RGWX_OBJECT_SIZE") {
        rest_obj.content_len = atoi(val.c_str());
      } else {
        rest_obj.attrs[header.first] = val;
      }
    }

    ldout(sync_env->cct, 20) << __func__ << ":" << " headers=" << headers << " extra_data.length()=" << extra_data.length() << dendl;

    return 0;

  }

  bool need_extra_data() override {
    return true;
  }
};

class RGWAWSStreamPutCRF : public RGWStreamWriteHTTPResourceCRF
{
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *conn;
  rgw_obj dest_obj;
public:
  RGWAWSStreamPutCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWDataSyncEnv *_sync_env,
                               RGWRESTConn* _conn,
                               rgw_obj& _dest_obj) : RGWStreamWriteHTTPResourceCRF(_cct, _env, _caller, _sync_env->http_manager),
                                                     sync_env(_sync_env), conn(_conn), dest_obj(_dest_obj) {
  }

  int init() {
    /* init output connection */
    RGWRESTStreamS3PutObj *out_req{nullptr};

    if (multipart.is_multipart) {
      char buf[32];
      snprintf(buf, sizeof(buf), "%d", multipart.part_num);
      rgw_http_param_pair params[] = { { "uploadId", multipart.upload_id.c_str() },
                                       { "partNumber", buf },
                                       { nullptr, nullptr } };
      conn->put_obj_send_init(dest_obj, params, &out_req);
    } else {
      conn->put_obj_send_init(dest_obj, nullptr, &out_req);
    }

    set_req(out_req);

    return RGWStreamWriteHTTPResourceCRF::init();
  }

  void send_ready(const rgw_rest_obj& rest_obj) override {
    RGWRESTStreamS3PutObj *r = (RGWRESTStreamS3PutObj *)req;

    /* here we need to convert rest_obj.attrs to cloud specific representation */

    map<string, bufferlist> new_attrs;

    for (auto attr : rest_obj.attrs) {
      new_attrs[attr.first].append(attr.second);
    }

    RGWAccessControlPolicy policy;
    ::encode(policy, new_attrs[RGW_ATTR_ACL]);

    r->set_send_length(rest_obj.content_len);

    r->send_ready(conn->get_key(), new_attrs, false);
  }
};


class RGWAWSStreamObjToCloudPlainCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *source_conn;
  RGWRESTConn *dest_conn;
  rgw_obj src_obj;
  rgw_obj dest_obj;

  std::shared_ptr<RGWStreamReadHTTPResourceCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;

public:
  RGWAWSStreamObjToCloudPlainCR(RGWDataSyncEnv *_sync_env,
                                RGWRESTConn *_source_conn,
                                const rgw_obj& _src_obj,
                                RGWRESTConn *_dest_conn,
                                const rgw_obj& _dest_obj) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   source_conn(_source_conn),
                                                   dest_conn(_dest_conn),
                                                   src_obj(_src_obj),
                                                   dest_obj(_dest_obj) {}

  int operate() {
    reenter(this) {
      /* init input */
      in_crf.reset(new RGWRESTStreamGetCRF(cct, get_env(), this, sync_env, source_conn, src_obj));

      /* init output */
      out_crf.reset(new RGWAWSStreamPutCRF(cct, get_env(), this, sync_env, dest_conn,
                                           dest_obj));

      yield call(new RGWStreamSpliceCR(cct, sync_env->http_manager, in_crf, out_crf));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }

    return 0;
  }
};

class RGWAWSStreamObjToCloudMultipartPartCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *source_conn;
  RGWRESTConn *dest_conn;
  rgw_obj src_obj;
  rgw_obj dest_obj;

  string upload_id;
  uint64_t ofs;
  uint64_t size;
  int part_num;

  std::shared_ptr<RGWStreamReadHTTPResourceCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;

public:
  RGWAWSStreamObjToCloudMultipartPartCR(RGWDataSyncEnv *_sync_env,
                                RGWRESTConn *_source_conn,
                                const rgw_obj& _src_obj,
                                RGWRESTConn *_dest_conn,
                                const rgw_obj& _dest_obj,
                                const string& _upload_id,
                                uint64_t _ofs,
                                uint64_t _size,
                                int _part_num) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   source_conn(_source_conn),
                                                   dest_conn(_dest_conn),
                                                   src_obj(_src_obj),
                                                   dest_obj(_dest_obj),
                                                   upload_id(_upload_id),
                                                   ofs(_ofs), size(_size),
                                                   part_num(_part_num) {}

  int operate() {
    reenter(this) {
      /* init input */
      in_crf.reset(new RGWRESTStreamGetCRF(cct, get_env(), this, sync_env, source_conn, src_obj));

      in_crf->set_range(ofs, size);

      /* init output */
      out_crf.reset(new RGWAWSStreamPutCRF(cct, get_env(), this, sync_env, dest_conn,
                                           dest_obj));

      out_crf->set_multipart(upload_id, part_num, size);

      yield call(new RGWStreamSpliceCR(cct, sync_env->http_manager, in_crf, out_crf));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }

    return 0;
  }
};

class RGWAWSAbortMultipartCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  string upload_id;

public:
  RGWAWSAbortMultipartCR(RGWDataSyncEnv *_sync_env,
                        RGWRESTConn *_dest_conn,
                        const rgw_obj& _dest_obj,
                        const string& _upload_id) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   dest_conn(_dest_conn),
                                                   dest_obj(_dest_obj),
                                                   upload_id(_upload_id) {}

  int operate() {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };
        bufferlist bl;
        call(new RGWDeleteRESTResourceCR(sync_env->cct, dest_conn, sync_env->http_manager,
                                         obj_to_aws_path(dest_obj), params));
      }

      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to abort multipart upload for dest object=" << dest_obj << " (retcode=" << retcode << ")" << dendl;
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }

    return 0;
  }
};

class RGWAWSInitMultipartCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  uint64_t obj_size;
  ceph::real_time mtime;

  bufferlist out_bl;

  string *upload_id;

  struct InitMultipartResult {
    string bucket;
    string key;
    string upload_id;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("UploadId", upload_id, obj);
    }
  } result;

public:
  RGWAWSInitMultipartCR(RGWDataSyncEnv *_sync_env,
                        RGWRESTConn *_dest_conn,
                        const rgw_obj& _dest_obj,
                        uint64_t _obj_size,
                        const ceph::real_time& _mtime,
                        string *_upload_id) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   dest_conn(_dest_conn),
                                                   dest_obj(_dest_obj),
                                                   obj_size(_obj_size),
                                                   mtime(_mtime),
                                                   upload_id(_upload_id) {}

  int operate() {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploads", nullptr }, {nullptr, nullptr} };
        bufferlist bl;
        call(new RGWPostRawRESTResourceCR <bufferlist> (sync_env->cct, dest_conn, sync_env->http_manager,
                                                 obj_to_aws_path(dest_obj), params, bl, &out_bl));
      }

      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to initialize multipart upload for dest object=" << dest_obj << dendl;
        return set_cr_error(retcode);
      }
      {
        /*
         * If one of the following fails we cannot abort upload, as we cannot
         * extract the upload id. If one of these fail it's very likely that that's
         * the least of our problem.
         */
        RGWXMLDecoder::XMLParser parser;
        if (!parser.init()) {
          ldout(sync_env->cct, 0) << "ERROR: failed to initialize xml parser for parsing multipart init response from server" << dendl;
          return set_cr_error(-EIO);
        }

        if (!parser.parse(out_bl.c_str(), out_bl.length(), 1)) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(sync_env->cct, 5) << "ERROR: failed to parse xml: " << str << dendl;
          return set_cr_error(-EIO);
        }

        try {
          RGWXMLDecoder::decode_xml("InitiateMultipartUploadResult", result, &parser, true);
        } catch (RGWXMLDecoder::err& err) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(sync_env->cct, 5) << "ERROR: unexpected xml: " << str << dendl;
          return set_cr_error(-EIO);
        }
      }

      ldout(sync_env->cct, 20) << "init multipart result: bucket=" << result.bucket << " key=" << result.key << " upload_id=" << result.upload_id << dendl;

      *upload_id = result.upload_id;

      return set_cr_done();
    }

    return 0;
  }
};

class RGWAWSStreamObjToCloudMultipartCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *source_conn;
  RGWRESTConn *dest_conn;
  rgw_obj src_obj;
  rgw_obj dest_obj;

  uint64_t obj_size;
  ceph::real_time mtime;

  string upload_id;

  uint32_t part_size;
  int num_parts;

  struct part_info {
    int part_num;
    uint64_t ofs;
    uint64_t size;
    string etag;
  };

  int cur_part{0};
  uint64_t cur_ofs{0};

public:
  RGWAWSStreamObjToCloudMultipartCR(RGWDataSyncEnv *_sync_env,
                                RGWRESTConn *_source_conn,
                                const rgw_obj& _src_obj,
                                RGWRESTConn *_dest_conn,
                                const rgw_obj& _dest_obj,
                                uint64_t _obj_size,
                                const ceph::real_time& _mtime) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   source_conn(_source_conn),
                                                   dest_conn(_dest_conn),
                                                   src_obj(_src_obj),
                                                   dest_obj(_dest_obj),
                                                   obj_size(_obj_size),
                                                   mtime(_mtime) {
#warning flexible part size needed
    part_size = 5 * 1024 * 1024;

    num_parts = (obj_size + part_size - 1) / part_size;
  }


  int operate() {
    reenter(this) {
      yield call(new RGWAWSInitMultipartCR(sync_env, dest_conn, dest_obj, obj_size, mtime, &upload_id));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      for (cur_part = 1; cur_part <= num_parts; ++cur_part) {
        yield {
          part_info cur_part_info;
          cur_part_info.part_num = cur_part + 1;
          cur_part_info.ofs = cur_ofs;
          cur_part_info.size = std::min((uint64_t)part_size, obj_size - cur_ofs);

          cur_ofs += part_size;

          call(new RGWAWSStreamObjToCloudMultipartPartCR(sync_env,
                                                             source_conn, src_obj,
                                                             dest_conn, dest_obj,
                                                             upload_id,
                                                             cur_part_info.ofs, cur_part_info.size,
                                                             cur_part_info.part_num));
        }
      }

      return set_cr_done();
    }

    return 0;
  }
};

// maybe use Fetch Remote Obj instead?
class RGWAWSHandleRemoteObjCBCR: public RGWStatRemoteObjCBCR {
  const AWSConfig& conf;
  RGWRESTConn *source_conn;
  bufferlist res;
  unordered_map <string, bool> bucket_created;
  string target_bucket_name;
  rgw_rest_obj rest_obj;
  string obj_path;
  int ret{0};

  static constexpr uint32_t multipart_threshold = 8 * 1024 * 1024;

public:
  RGWAWSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                            RGWBucketInfo& _bucket_info,
                            rgw_obj_key& _key,
                            const AWSConfig& _conf) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                                         conf(_conf)
  {}

  ~RGWAWSHandleRemoteObjCBCR(){
  }

  int operate() override {
    reenter(this) {

      ldout(sync_env->cct, 0) << "AWS: download begin: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size
                              << " mtime=" << mtime << " attrs=" << attrs
                              << dendl;

      source_conn = sync_env->store->get_zone_conn_by_id(sync_env->source_zone);
      if (!source_conn) {
        ldout(sync_env->cct, 0) << "ERROR: cannot find http connection to zone " << sync_env->source_zone << dendl;
        return set_cr_error(-EINVAL);
      }

      obj_path = bucket_info.bucket.name + "/" + key.name;

      target_bucket_name = aws_bucket_name(bucket_info);
      if (bucket_created.find(target_bucket_name) == bucket_created.end()){
        yield {
          ldout(sync_env->cct,0) << "AWS: creating bucket" << target_bucket_name << dendl;
          bufferlist bl;
          call(new RGWPutRawRESTResourceCR <int> (sync_env->cct, conf.conn.get(),
                                                  sync_env->http_manager,
                                                  target_bucket_name, nullptr, bl, nullptr));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        bucket_created[target_bucket_name] = true;
      }

      yield {
        rgw_obj src_obj(bucket_info.bucket, key);

        /* init output */
        rgw_bucket target_bucket;
        target_bucket.name = target_bucket_name; /* this is only possible because we only use bucket name for
                                                    uri resolution */
        rgw_obj dest_obj(target_bucket, aws_object_name(bucket_info, key));

        if (size < multipart_threshold) {
          call(new RGWAWSStreamObjToCloudPlainCR(sync_env, source_conn, src_obj, conf.conn.get(), dest_obj));
        } else {
          call(new RGWAWSStreamObjToCloudMultipartCR(sync_env, source_conn, src_obj, conf.conn.get(),
                                                     dest_obj, size, mtime));
        }
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      return set_cr_done();
    }

    return 0;
  }
};

class RGWAWSHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  const AWSConfig& conf;
public:
  RGWAWSHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                              RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                              const AWSConfig& _conf) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                            conf(_conf) {
  }

  ~RGWAWSHandleRemoteObjCR() {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWAWSHandleRemoteObjCBCR(sync_env, bucket_info, key, conf);
  }
};

class RGWAWSRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  const AWSConfig& conf;
public:
  RGWAWSRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          const AWSConfig& _conf) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": remove remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
        string path = aws_bucket_name(bucket_info) + "/" + aws_object_name(bucket_info, key);
        ldout(sync_env->cct, 0) << "AWS: removing aws object at" << path << dendl;
        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf.conn.get(),
                                         sync_env->http_manager,
                                         path, nullptr /* params */));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};


class RGWAWSDataSyncModule: public RGWDataSyncModule {
  CephContext *cct;
  AWSConfig conf;
  string s3_endpoint;
  RGWAccessKey key;
public:
  RGWAWSDataSyncModule(CephContext *_cct, const string& _s3_endpoint, const string& access_key, const string& secret) :
                  cct(_cct),
                  s3_endpoint(_s3_endpoint),
                  key(access_key, secret) {
  }

  void init(RGWDataSyncEnv *sync_env, uint64_t instance_id) {
    conf.id = string("s3:") + s3_endpoint;
    conf.conn.reset(new RGWRESTConn(cct,
                                    sync_env->store,
                                    conf.id,
                                    { s3_endpoint },
                                    key));
  }

  ~RGWAWSDataSyncModule() {}

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch,
                            rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << conf.id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWAWSHandleRemoteObjCR(sync_env, bucket_info, key, conf);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch,
                              rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) <<"rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWAWSRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, conf);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch,
                                     rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) <<"AWS Not implemented: create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWAWSSyncModuleInstance : public RGWSyncModuleInstance {
  RGWAWSDataSyncModule data_handler;
public:
  RGWAWSSyncModuleInstance(CephContext *cct, const string& s3_endpoint, const string& access_key, const string& secret) : data_handler(cct, s3_endpoint, access_key, secret) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWAWSSyncModule::create_instance(CephContext *cct, map<string, string, ltstr_nocase>& config,  RGWSyncModuleInstanceRef *instance){
  string s3_endpoint, access_key, secret;
  auto i = config.find("s3_endpoint");
  if (i != config.end())
    s3_endpoint = i->second;

  i = config.find("access_key");
  if (i != config.end())
    access_key = i->second;

  i = config.find("secret");
  if (i != config.end())
    secret = i->second;

  instance->reset(new RGWAWSSyncModuleInstance(cct, s3_endpoint, access_key, secret));
  return 0;
}
