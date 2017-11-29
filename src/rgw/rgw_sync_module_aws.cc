#include "common/errno.h"

#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_aws.h"
#include "rgw_cr_rados.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"
#include "rgw_acl.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw


#define DEFAULT_MULTIPART_SYNC_PART_SIZE (32 * 1024 * 1024)

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
  string object_name;
  object_name += bucket_info.owner.to_str() + "/";
  object_name += bucket_info.bucket.name + "/" + key.name;
  return object_name;
}

static string obj_to_aws_path(const rgw_obj& obj)
{
  return obj.bucket.name + "/" + obj.key.name;
}

struct AWSSyncConfig {
  string s3_endpoint;
  RGWAccessKey key;
  HostStyle host_style;


  uint64_t multipart_sync_threshold{DEFAULT_MULTIPART_SYNC_PART_SIZE};
  uint64_t multipart_min_part_size{DEFAULT_MULTIPART_SYNC_PART_SIZE};
};

struct AWSSyncInstanceEnv {
  AWSSyncConfig conf;
  string id;
  std::unique_ptr<RGWRESTConn> conn;

  AWSSyncInstanceEnv(const AWSSyncConfig& _conf) : conf(_conf) {}
};

class RGWRESTStreamGetCRF : public RGWStreamReadHTTPResourceCRF
{
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *conn;
  rgw_obj src_obj;
  RGWRESTConn::get_obj_params req_params;

  rgw_sync_aws_src_obj_properties src_properties;
public:
  RGWRESTStreamGetCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWDataSyncEnv *_sync_env,
                               RGWRESTConn *_conn,
                               rgw_obj& _src_obj,
                               const rgw_sync_aws_src_obj_properties& _src_properties) : RGWStreamReadHTTPResourceCRF(_cct, _env, _caller, _sync_env->http_manager),
                                                                                 sync_env(_sync_env), conn(_conn), src_obj(_src_obj),
                                                                                 src_properties(_src_properties) {
  }

  int init() override {
    /* init input connection */


    req_params.get_op = true;
    req_params.prepend_metadata = true;

    req_params.unmod_ptr = &src_properties.mtime;
    req_params.etag = src_properties.etag;
    req_params.mod_zone_id = src_properties.zone_short_id;
    req_params.mod_pg_ver = src_properties.pg_ver;

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
  string etag;
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

  void handle_headers(const map<string, string>& headers) {
    for (auto h : headers) {
      if (h.first == "ETAG") {
        etag = h.second;
      }
    }
  }

  bool get_etag(string *petag) {
    if (etag.empty()) {
      return false;
    }
    *petag = etag;
    return true;
  }
};


class RGWAWSStreamObjToCloudPlainCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *source_conn;
  RGWRESTConn *dest_conn;
  rgw_obj src_obj;
  rgw_obj dest_obj;

  rgw_sync_aws_src_obj_properties src_properties;

  std::shared_ptr<RGWStreamReadHTTPResourceCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;

public:
  RGWAWSStreamObjToCloudPlainCR(RGWDataSyncEnv *_sync_env,
                                RGWRESTConn *_source_conn,
                                const rgw_obj& _src_obj,
                                const rgw_sync_aws_src_obj_properties& _src_properties,
                                RGWRESTConn *_dest_conn,
                                const rgw_obj& _dest_obj) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   source_conn(_source_conn),
                                                   dest_conn(_dest_conn),
                                                   src_obj(_src_obj),
                                                   dest_obj(_dest_obj),
                                                   src_properties(_src_properties) {}

  int operate() {
    reenter(this) {
      /* init input */
      in_crf.reset(new RGWRESTStreamGetCRF(cct, get_env(), this, sync_env,
                                           source_conn, src_obj,
                                           src_properties));

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

  rgw_sync_aws_src_obj_properties src_properties;

  string upload_id;

  rgw_sync_aws_multipart_part_info part_info;

  std::shared_ptr<RGWStreamReadHTTPResourceCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;

  string *petag;

public:
  RGWAWSStreamObjToCloudMultipartPartCR(RGWDataSyncEnv *_sync_env,
                                RGWRESTConn *_source_conn,
                                const rgw_obj& _src_obj,
                                RGWRESTConn *_dest_conn,
                                const rgw_obj& _dest_obj,
                                const rgw_sync_aws_src_obj_properties& _src_properties,
                                const string& _upload_id,
                                const rgw_sync_aws_multipart_part_info& _part_info,
                                string *_petag) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   source_conn(_source_conn),
                                                   dest_conn(_dest_conn),
                                                   src_obj(_src_obj),
                                                   dest_obj(_dest_obj),
                                                   src_properties(_src_properties),
                                                   upload_id(_upload_id),
                                                   part_info(_part_info),
                                                   petag(_petag) {}

  int operate() {
    reenter(this) {
      /* init input */
      in_crf.reset(new RGWRESTStreamGetCRF(cct, get_env(), this, sync_env,
                                           source_conn, src_obj,
                                           src_properties));

      in_crf->set_range(part_info.ofs, part_info.size);

      /* init output */
      out_crf.reset(new RGWAWSStreamPutCRF(cct, get_env(), this, sync_env, dest_conn,
                                           dest_obj));

      out_crf->set_multipart(upload_id, part_info.part_num, part_info.size);

      yield call(new RGWStreamSpliceCR(cct, sync_env->http_manager, in_crf, out_crf));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      if (!((RGWAWSStreamPutCRF *)out_crf.get())->get_etag(petag)) {
        ldout(sync_env->cct, 0) << "ERROR: failed to get etag from PUT request" << dendl;
        return set_cr_error(-EIO);
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
                        string *_upload_id) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   dest_conn(_dest_conn),
                                                   dest_obj(_dest_obj),
                                                   obj_size(_obj_size),
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

class RGWAWSCompleteMultipartCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *dest_conn;
  rgw_obj dest_obj;

  bufferlist out_bl;

  string upload_id;

  struct CompleteMultipartReq {
    map<int, rgw_sync_aws_multipart_part_info> parts;

    CompleteMultipartReq(const map<int, rgw_sync_aws_multipart_part_info>& _parts) : parts(_parts) {}

    void dump_xml(Formatter *f) const {
      for (auto p : parts) {
        f->open_object_section("Part");
        encode_xml("PartNumber", p.first, f);
        encode_xml("ETag", p.second.etag, f);
        f->close_section();
      };
    }
  } req_enc;

  struct CompleteMultipartResult {
    string location;
    string bucket;
    string key;
    string etag;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("Location", bucket, obj);
      RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
      RGWXMLDecoder::decode_xml("Key", key, obj);
      RGWXMLDecoder::decode_xml("ETag", etag, obj);
    }
  } result;

public:
  RGWAWSCompleteMultipartCR(RGWDataSyncEnv *_sync_env,
                        RGWRESTConn *_dest_conn,
                        const rgw_obj& _dest_obj,
                        string _upload_id,
                        const map<int, rgw_sync_aws_multipart_part_info>& _parts) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   dest_conn(_dest_conn),
                                                   dest_obj(_dest_obj),
                                                   upload_id(_upload_id),
                                                   req_enc(_parts) {}

  int operate() {
    reenter(this) {

      yield {
        rgw_http_param_pair params[] = { { "uploadId", upload_id.c_str() }, {nullptr, nullptr} };
        stringstream ss;
        XMLFormatter formatter;

        encode_xml("CompleteMultipartUpload", req_enc, &formatter);

        formatter.flush(ss);

        bufferlist bl;
        bl.append(ss.str());

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
          RGWXMLDecoder::decode_xml("CompleteMultipartUploadResult", result, &parser, true);
        } catch (RGWXMLDecoder::err& err) {
          string str(out_bl.c_str(), out_bl.length());
          ldout(sync_env->cct, 5) << "ERROR: unexpected xml: " << str << dendl;
          return set_cr_error(-EIO);
        }
      }

      ldout(sync_env->cct, 20) << "complete multipart result: location=" << result.location << " bucket=" << result.bucket << " key=" << result.key << " etag=" << result.etag << dendl;

      return set_cr_done();
    }

    return 0;
  }
};


class RGWAWSStreamAbortMultipartUploadCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *dest_conn;
  const rgw_obj dest_obj;
  const rgw_raw_obj status_obj;

  string upload_id;

public:

  RGWAWSStreamAbortMultipartUploadCR(RGWDataSyncEnv *_sync_env,
                                RGWRESTConn *_dest_conn,
                                const rgw_obj& _dest_obj,
                                const rgw_raw_obj& _status_obj,
                                const string& _upload_id) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                            dest_conn(_dest_conn),
                                                            dest_obj(_dest_obj),
                                                            status_obj(_status_obj),
                                                            upload_id(_upload_id) {}

  int operate() {
    reenter(this) {
      yield call(new RGWAWSAbortMultipartCR(sync_env, dest_conn, dest_obj, upload_id));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to abort multipart upload dest obj=" << dest_obj << " upload_id=" << upload_id << " retcode=" << retcode << dendl;
        /* ignore error, best effort */
      }
      yield call(new RGWRadosRemoveCR(sync_env->store, status_obj));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to remove sync status obj obj=" << status_obj << " retcode=" << retcode << dendl;
        /* ignore error, best effort */
      }
      return set_cr_done();
    }

    return 0;
  }
};

class RGWAWSStreamObjToCloudMultipartCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  const AWSSyncConfig& conf;
  RGWRESTConn *source_conn;
  RGWRESTConn *dest_conn;
  rgw_obj src_obj;
  rgw_obj dest_obj;

  uint64_t obj_size;
  string src_etag;
  rgw_sync_aws_src_obj_properties src_properties;

  rgw_sync_aws_multipart_upload_info status;

  rgw_sync_aws_multipart_part_info *pcur_part_info{nullptr};

  int ret_err{0};

  rgw_raw_obj status_obj;

public:
  RGWAWSStreamObjToCloudMultipartCR(RGWDataSyncEnv *_sync_env,
                                const AWSSyncConfig& _conf,
                                RGWRESTConn *_source_conn,
                                const rgw_obj& _src_obj,
                                RGWRESTConn *_dest_conn,
                                const rgw_obj& _dest_obj,
                                uint64_t _obj_size,
                                const rgw_sync_aws_src_obj_properties& _src_properties) : RGWCoroutine(_sync_env->cct),
                                                   sync_env(_sync_env),
                                                   conf(_conf),
                                                   source_conn(_source_conn),
                                                   dest_conn(_dest_conn),
                                                   src_obj(_src_obj),
                                                   dest_obj(_dest_obj),
                                                   obj_size(_obj_size),
                                                   src_properties(_src_properties),
                                                   status_obj(sync_env->store->get_zone_params().log_pool,
                                                              RGWBucketSyncStatusManager::obj_status_oid(sync_env->source_zone, src_obj)) {
  }


  int operate() {
    reenter(this) {
      yield call(new RGWSimpleRadosReadCR<rgw_sync_aws_multipart_upload_info>(sync_env->async_rados, sync_env->store,
                                                                 status_obj, &status, false));

      if (retcode < 0 && retcode != -ENOENT) {
        ldout(sync_env->cct, 0) << "ERROR: failed to read sync status of object " << src_obj << " retcode=" << retcode << dendl;
        return retcode;
      }

      if (retcode >= 0) {
        /* check here that mtime and size did not change */

        if (status.src_properties.mtime != src_properties.mtime || status.obj_size != obj_size ||
            status.src_properties.etag != src_properties.etag) {
          yield call(new RGWAWSStreamAbortMultipartUploadCR(sync_env, dest_conn, dest_obj, status_obj, status.upload_id));
          retcode = -ENOENT;
        }
      }

      if (retcode == -ENOENT) {
        yield call(new RGWAWSInitMultipartCR(sync_env, dest_conn, dest_obj, status.obj_size, &status.upload_id));
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        status.obj_size = obj_size;
        status.src_properties = src_properties;
#define MULTIPART_MAX_PARTS 10000
        uint64_t min_part_size = obj_size / MULTIPART_MAX_PARTS;
        status.part_size = std::max(conf.multipart_min_part_size, min_part_size);
        status.num_parts = (obj_size + status.part_size - 1) / status.part_size;
        status.cur_part = 1;
      }

      for (; (uint32_t)status.cur_part <= status.num_parts; ++status.cur_part) {
        yield {
          rgw_sync_aws_multipart_part_info& cur_part_info = status.parts[status.cur_part];
          cur_part_info.part_num = status.cur_part;
          cur_part_info.ofs = status.cur_ofs;
          cur_part_info.size = std::min((uint64_t)status.part_size, status.obj_size - status.cur_ofs);

          pcur_part_info = &cur_part_info;

          status.cur_ofs += status.part_size;

          call(new RGWAWSStreamObjToCloudMultipartPartCR(sync_env,
                                                             source_conn, src_obj,
                                                             dest_conn, dest_obj,
                                                             status.src_properties,
                                                             status.upload_id,
                                                             cur_part_info,
                                                             &cur_part_info.etag));
        }

        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to sync obj=" << src_obj << ", sync via multipart upload, upload_id=" << status.upload_id << " part number " << status.cur_part << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
          ret_err = retcode;
          yield call(new RGWAWSStreamAbortMultipartUploadCR(sync_env, dest_conn, dest_obj, status_obj, status.upload_id));
          return set_cr_error(ret_err);
        }

        yield call(new RGWSimpleRadosWriteCR<rgw_sync_aws_multipart_upload_info>(sync_env->async_rados, sync_env->store, status_obj, status));
        if (retcode < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to store multipart upload state, retcode=" << retcode << dendl;
          /* continue with upload anyway */
        }
        ldout(sync_env->cct, 20) << "sync of object=" << src_obj << " via multipart upload, finished sending part #" << status.cur_part << " etag=" << pcur_part_info->etag << dendl;
      }

      yield call(new RGWAWSCompleteMultipartCR(sync_env, dest_conn, dest_obj, status.upload_id, status.parts));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to complete multipart upload of obj=" << src_obj << " (error: " << cpp_strerror(-retcode) << ")" << dendl;
        ret_err = retcode;
        yield call(new RGWAWSStreamAbortMultipartUploadCR(sync_env, dest_conn, dest_obj, status_obj, status.upload_id));
        return set_cr_error(ret_err);
      }

      /* remove status obj */
      yield call(new RGWRadosRemoveCR(sync_env->store, status_obj));
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to abort multipart upload obj=" << src_obj << " upload_id=" << status.upload_id << " part number " << status.cur_part << " (" << cpp_strerror(-retcode) << ")" << dendl;
        /* ignore error, best effort */
      }
      return set_cr_done();
    }

    return 0;
  }
};
template <class T>
int decode_attr(map<string, bufferlist>& attrs, const char *attr_name, T *result, T def_val)
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
    decode(*result, bliter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}

// maybe use Fetch Remote Obj instead?
class RGWAWSHandleRemoteObjCBCR: public RGWStatRemoteObjCBCR {
  const AWSSyncInstanceEnv& instance;
  RGWRESTConn *source_conn;
  bufferlist res;
  unordered_map <string, bool> bucket_created;
  string target_bucket_name;
  rgw_rest_obj rest_obj;
  int ret{0};

  uint32_t src_zone_short_id{0};
  uint64_t src_pg_ver{0};

public:
  RGWAWSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                            RGWBucketInfo& _bucket_info,
                            rgw_obj_key& _key,
                            const AWSSyncInstanceEnv& _instance) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                                         instance(_instance)
  {}

  ~RGWAWSHandleRemoteObjCBCR(){
  }

  int operate() override {
    reenter(this) {
      ret = decode_attr(attrs, RGW_ATTR_PG_VER, &src_pg_ver, (uint64_t)0);
      if (ret < 0) {
        ldout(sync_env->cct, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
      } else {
        ret = decode_attr(attrs, RGW_ATTR_SOURCE_ZONE, &src_zone_short_id, (uint32_t)0);
        if (ret < 0) {
          ldout(sync_env->cct, 0) << "ERROR: failed to decode source zone short_id attr, ignoring" << dendl;
          src_pg_ver = 0; /* all or nothing */
        }
      }
      ldout(sync_env->cct, 0) << "AWS: download begin: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size
                              << " mtime=" << mtime << " etag=" << etag
                              << " zone_short_id=" << src_zone_short_id << " pg_ver=" << src_pg_ver
                              << " attrs=" << attrs
                              << dendl;

      source_conn = sync_env->store->get_zone_conn_by_id(sync_env->source_zone);
      if (!source_conn) {
        ldout(sync_env->cct, 0) << "ERROR: cannot find http connection to zone " << sync_env->source_zone << dendl;
        return set_cr_error(-EINVAL);
      }

      target_bucket_name = aws_bucket_name(bucket_info);
      if (bucket_created.find(target_bucket_name) == bucket_created.end()){
        yield {
          ldout(sync_env->cct,0) << "AWS: creating bucket" << target_bucket_name << dendl;
          bufferlist bl;
          call(new RGWPutRawRESTResourceCR <int> (sync_env->cct, instance.conn.get(),
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


        rgw_sync_aws_src_obj_properties src_properties;
        src_properties.mtime = mtime;
        src_properties.etag = etag;
        src_properties.zone_short_id = src_zone_short_id;
        src_properties.pg_ver = src_pg_ver;

        if (size < instance.conf.multipart_sync_threshold) {
          call(new RGWAWSStreamObjToCloudPlainCR(sync_env, source_conn, src_obj,
                                                 src_properties,
                                                 instance.conn.get(), dest_obj));
        } else {
          call(new RGWAWSStreamObjToCloudMultipartCR(sync_env, instance.conf, source_conn, src_obj, instance.conn.get(),
                                                     dest_obj, size, src_properties));
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
  const AWSSyncInstanceEnv& instance;
public:
  RGWAWSHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                              RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                              const AWSSyncInstanceEnv& _instance) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                          instance(_instance) {
  }

  ~RGWAWSHandleRemoteObjCR() {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWAWSHandleRemoteObjCBCR(sync_env, bucket_info, key, instance);
  }
};

class RGWAWSRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  const AWSSyncInstanceEnv& instance;
public:
  RGWAWSRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          const AWSSyncInstanceEnv& _instance) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), instance(_instance) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": remove remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
        string path = aws_bucket_name(bucket_info) + "/" + aws_object_name(bucket_info, key);
        ldout(sync_env->cct, 0) << "AWS: removing aws object at" << path << dendl;
        call(new RGWDeleteRESTResourceCR(sync_env->cct, instance.conn.get(),
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
  AWSSyncInstanceEnv instance;
public:
  RGWAWSDataSyncModule(CephContext *_cct, const AWSSyncConfig& _conf) :
                  cct(_cct),
                  instance(_conf) {
  }

  void init(RGWDataSyncEnv *sync_env, uint64_t instance_id) {
    instance.id = string("s3:") + instance.conf.s3_endpoint;
    instance.conn.reset(new S3RESTConn(cct,
                                    sync_env->store,
                                    instance.id,
                                    { instance.conf.s3_endpoint },
                                    instance.conf.key,
                                    instance.conf.host_style));
  }

  ~RGWAWSDataSyncModule() {}

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch,
                            rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << instance.id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWAWSHandleRemoteObjCR(sync_env, bucket_info, key, instance);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch,
                              rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) <<"rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWAWSRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, instance);
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
  RGWAWSSyncModuleInstance(CephContext *cct, const AWSSyncConfig& _conf) : data_handler(cct, _conf) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

static int conf_to_uint64(CephContext *cct, const JSONFormattable& config, const string& key, uint64_t *pval)
{
  string sval;
  if (config.find(key, &sval)) {
    string err;
    uint64_t val = strict_strtoll(sval.c_str(), 10, &err);
    if (!err.empty()) {
      ldout(cct, 0) << "ERROR: could not parse configurable value for cloud sync module: " << key << ": " << sval << dendl;
      return -EINVAL;
    }
    *pval = val;
  }
  return 0;
}

int RGWAWSSyncModule::create_instance(CephContext *cct, const JSONFormattable& config,  RGWSyncModuleInstanceRef *instance){
  AWSSyncConfig conf;

  conf.s3_endpoint = config["s3_endpoint"];

  i = config.find("host_style");
  string host_style_str = config["host_style"];

  if (host_style_str != "virtual") {
    conf.host_style = PathStyle;
  } else {
    conf.host_style = VirtualStyle;
  }

  conf.bucket_suffix = config["bucket_suffix"];

  string access_key = config["access_key"];
  string secret = config["secret"];

  conf.key = RGWAccessKey(access_key, secret);

  int r = conf_to_uint64(cct, config, "multipart_sync_threshold", &conf.multipart_sync_threshold);
  if (r < 0) {
    return r;
  }

  r = conf_to_uint64(cct, config, "multipart_min_part_size", &conf.multipart_min_part_size);
  if (r < 0) {
    return r;
  }
#define MULTIPART_MIN_POSSIBLE_PART_SIZE (5 * 1024 * 1024)
  if (conf.multipart_min_part_size < MULTIPART_MIN_POSSIBLE_PART_SIZE) {
    conf.multipart_min_part_size = MULTIPART_MIN_POSSIBLE_PART_SIZE;
  }

  instance->reset(new RGWAWSSyncModuleInstance(cct, conf));
  return 0;
}
