#include "rgw_cloud_oss.h"
#include "rgw_common.h"
#include "rgw_rest_client.h"
#include "rgw_rados.h"
#include "common/armor.h"
#include "common/ceph_crypto.h"
#include "common/ceph_json.h"
#include "rgw/rgw_xml.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

class RGWRESTOSSRequest : public RGWRESTSimpleRequest {
  Mutex lock;
  RGWCloudInfo& cloud_info;
  uint64_t bl_len;
  uint64_t total_send;
  uint64_t pos;
  std::list<bufferptr>::const_iterator cur_it;
  std::list<bufferptr>::const_iterator end_it;

protected:
  RGWGetDataCB *cb;
  RGWHTTPManager http_manager;
private:
  void create_oss_canonical_header(const std::string& method, const string& resource, 
                        const std::string& content_type,const std::string& gmt, std::string& dest_str);
  int get_oss_header_digest(const std::string& auth_hdr, const string& key, string& dest);
  int complete();
  int add_output_data(bufferlist& bl, uint64_t len);
  void reset();

  static std::string get_gmt_now() {
    time_t rawTime;
    time(&rawTime);
    struct tm* timeInfo = gmtime(&rawTime);
    char szTemp[30]={0};
    strftime(szTemp,sizeof(szTemp),"%a, %d %b %Y %H:%M:%S GMT",timeInfo);
    std::string date(szTemp, strlen(szTemp));
    return date;
  }

public:
  static int get_oss_retcode(const std::string& http_response, int& retcode);
  static int get_oss_resp_value(const std::string& http_response, const std::string& parent, 
                                const std::string& name, std::string& value);

public:
  int send_data(void *ptr, size_t len);
  
  RGWRESTOSSRequest(CephContext *_cct, const std::string& _url, param_vec_t *_headers,
                                 param_vec_t *_params, RGWCloudInfo& _cloud_info) 
                                   : RGWRESTSimpleRequest(_cct, _url, _headers, _params),
                                     lock("RGWRESTStreamWriteOSSRequest"), 
                                     cloud_info(_cloud_info), bl_len(0),total_send(0),pos(0),
                                     cb(NULL),
                                     http_manager(_cct)
  { }
  ~RGWRESTOSSRequest();
  
  RGWGetDataCB *get_out_cb() { return cb; }

  int put_obj(const std::string& bucket, const string& obj, bufferlist* bl, uint64_t obj_size);
  int rm_obj(const std::string& bucket, const string& obj);
  int create_bucket(const std::string& bucket);

  int init_upload_multipart(const std::string& bucket, const std::string& key, std::string& upload_id, int& retcode);
  int upload_multipart(const std::string& bucket, const std::string& key, const std::string& uplaod_id, 
                       uint64_t seq, bufferlist& bl, uint64_t obj_size, std::string& etag);
  int finish_upload_multipart(const std::string& bucket, const std::string& key, const std::string& upload_id,
                              std::map<uint64_t, std::string>& etags);
  int abort_upload_multipart(const std::string& bucket, const std::string& key, const std::string& upload_id); 
};

RGWRESTOSSRequest::~RGWRESTOSSRequest() {
  delete cb;
  cb = nullptr; 
}

void RGWRESTOSSRequest::reset() {
  pos = 0;
  total_send = 0;
  cur_it = end_it;
}

int RGWRESTOSSRequest::add_output_data(bufferlist& bl, uint64_t len) {
  if (status < 0) {
    int ret = status;
    return ret;
  }
  cur_it = bl.buffers().begin();
  end_it = bl.buffers().end();
  bl_len = len;
  bool done;
  return http_manager.process_requests(false, &done);
}

int RGWRESTOSSRequest::send_data(void *ptr, size_t len) {
  uint64_t sent = 0;
  if (bl_len == 0 || status < 0) {
    dout(20) << "RGWRESTStreamWriteRequest::send_data status=" <<status << " len:"<<bl_len<<dendl;
    reset();
    return status;
  }
  
  uint64 remain_len = 0;
  while(cur_it != end_it && len>0) {
    remain_len = cur_it->length()-pos;
    uint64_t send_len = min(len, (size_t)remain_len);
    memcpy(ptr, cur_it->c_str() + pos, send_len);
    ptr = (char*)ptr + send_len;
    len -= send_len;
    sent += send_len;
    pos += send_len;
    total_send += send_len;
     
    if ( pos == cur_it->length() ) {
      ++cur_it;
      pos = 0;
    }
  }
  
  return sent;
}

int RGWRESTOSSRequest::complete() {
  int ret = http_manager.complete_requests();
  if (ret < 0)
    return ret;
  return status;
}

int RGWRESTOSSRequest::get_oss_resp_value(const std::string& resp, const std::string& parent, const std::string& name, std::string& value) {
  RGWXMLParser parser;
  if (!parser.init()) {
    dout(0) << "ERROR:oss init multipart failed to initialize xml parser" << dendl;
    return -EIO;
  }

  if (!parser.parse(resp.c_str(), resp.length(), 1)) {
    dout(0) << "ERROR:oss init multipart failed to parse xml data" << dendl;
    return -EIO;
  }

  XMLObj * config = parser.find_first(parent.c_str());
  if (!config) {
    dout(0) << "ERROR:oss init multipart failed to find element:" << parent << dendl;
    return -EINVAL;
  }

  XMLObj *field = config->find_first(name.c_str());
  if (!field) {
    dout(0) << "ERROR:oss init multipart failed to find element:" << name << dendl;
    return -EINVAL;
  }

  value = std::string(field->get_data());
  return 0;
}

int RGWRESTOSSRequest::get_oss_retcode(const std::string& http_response, int& retcode) {
  std::string code_value;
  retcode = 0;
  int ret = get_oss_resp_value(http_response, "Error", "Code", code_value);
  if (ret < 0)
    return ret;
 
  if (code_value == "NoSuchBucket") {
    retcode = -1;
    return 0;
  }
  return -1;
}

void RGWRESTOSSRequest::create_oss_canonical_header(const std::string& method, const std::string& resource, 
                                                    const std::string& content_type,const std::string& gmt,
                                                    std::string& dest_str) {
 
  dest_str = method +"\n" + "\n" + content_type + "\n" + gmt + "\n" + resource;
}

int RGWRESTOSSRequest::get_oss_header_digest(const std::string& auth_hdr, const string& key, string& dest)
{
  if (key.empty())
    return -EINVAL;

  char hmac_sha1[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
  calc_hmac_sha1(key.c_str(), key.size(), auth_hdr.c_str(), auth_hdr.size(), hmac_sha1);

  char encode_buf_64[64]; /* 64 is really enough */
  int ret = ceph_armor(encode_buf_64, encode_buf_64 + 64, hmac_sha1,
           hmac_sha1 + CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
  if (ret < 0) {
    dout(10) << "ceph_armor failed:" << ret << dendl;
    return ret;
  }
  encode_buf_64[ret] = '\0';

  dest = encode_buf_64;

  return 0;
}

int RGWRESTOSSRequest::init_upload_multipart(const std::string& bucket, const std::string& obj, 
                                     std::string& upload_id, int& retcode) {
  std::string new_url = "http://" + bucket + "." + cloud_info.domain_name + "/" + obj + "?uploads";
  std::string string2sign;
  std::string sign;
  std::string auth;
  std::string content_type = "application/octet-stream";
  std::string gmt = get_gmt_now();
  std::string resource = "/" + bucket + "/" + obj+ "?uploads";
  create_oss_canonical_header("POST", resource,content_type, gmt, string2sign);
  get_oss_header_digest(string2sign ,cloud_info.private_key, sign);
  auth = "OSS ";
  auth.append(cloud_info.public_key);
  auth.append(":");
  auth.append(sign);

  headers.push_back(pair<std::string, std::string>("Authorization", auth));
  headers.push_back(pair<std::string, std::string>("Date", gmt));
  headers.push_back(pair<std::string, std::string>("Content-Type", content_type));
  retcode = 0;

  int r = http_manager.add_request(this, "POST", new_url.c_str());
  if (r < 0) {
    return r;
  }

  r = complete();
  auto& buflist = get_response();
  string resp(buflist.c_str(), buflist.length());
  if (r < 0) {
    get_oss_retcode(resp, retcode);
    return r;
  }

  r = get_oss_resp_value(resp, "InitiateMultipartUploadResult", "UploadId", upload_id);
  return r;
}

int RGWRESTOSSRequest::upload_multipart(const std::string& bucket, const std::string& obj, 
                                          const std::string& upload_id, uint64_t seq,
                                          bufferlist& bl, uint64_t obj_size, std::string& etag) {
  std::string new_url = "http://" + bucket + "." + cloud_info.domain_name + "/" + obj + 
                        "?uploadId=" + upload_id + "&partNumber="+ std::to_string(seq);
  std::string string2sign;
  std::string sign;
  std::string auth;
  std::string content_type = "application/octet-stream";
  std::string gmt = get_gmt_now();
  std::string resource = "/" + bucket + "/" + obj + 
                        "?partNumber=" + std::to_string(seq) +"&uploadId=" + upload_id;  
  create_oss_canonical_header("PUT", resource, content_type, gmt, string2sign);
  get_oss_header_digest(string2sign ,cloud_info.private_key, sign);
  auth = "OSS ";
  auth.append(cloud_info.public_key);
  auth.append(":");
  auth.append(sign);

  headers.push_back(pair<std::string, std::string>("Authorization", auth));
  headers.push_back(pair<std::string, std::string>("Date", gmt));
  headers.push_back(pair<std::string, std::string>("Content-Type", content_type));
  
  set_send_length(obj_size);
  int r = http_manager.add_request(this, "PUT", new_url.c_str());
  if (r < 0)
    return r;
  
  add_output_data(bl, obj_size);
 
  r = complete();
  if (r < 0)
    return r;

  auto& headers = get_out_headers();
  auto it = headers.find("ETAG");
  if (it != headers.end()) {
    etag = it->second;
  }else {
    r = -EINVAL;
  }

  return r;
}

int RGWRESTOSSRequest::finish_upload_multipart(const std::string& bucket, const std::string& obj, 
                       const std::string& upload_id, std::map<uint64_t,std::string>& etags) {
  std::string new_url = "http://" + bucket + "." + cloud_info.domain_name + "/" + obj +
                        "?uploadId=" + upload_id;
  std::string string2sign;
  std::string sign;
  std::string auth;
  std::string content_type = "application/octet-stream";
  std::string gmt = get_gmt_now();
  std::string resource = "/" + bucket + "/" + obj + "?uploadId=" + upload_id;
  create_oss_canonical_header("POST", resource, content_type, gmt, string2sign);
  get_oss_header_digest(string2sign ,cloud_info.private_key, sign);
  auth = "OSS ";
  auth.append(cloud_info.public_key);
  auth.append(":");
  auth.append(sign);

  headers.push_back(pair<std::string, std::string>("Authorization", auth));
  headers.push_back(pair<std::string, std::string>("Date", gmt));
  headers.push_back(pair<std::string, std::string>("Content-Type", content_type));

  int r = http_manager.add_request(this, "POST", new_url.c_str());
  if (r < 0)
    return r;

  bufferlist resp;
  Formatter *f = Formatter::create("xml");
  f->open_object_section("CompleteMultipartUpload");
  for(auto it=etags.begin(); it!=etags.end(); ++it) {
    f->open_object_section("Part");
    encode_xml("PartNumber", it->first, f);
    encode_xml("ETag", it->second, f);
    f->close_section(); 
  }
  f->close_section();
  f->flush(resp);
  delete f;
 
  add_output_data(resp, resp.length());

  r = complete();
  return r;
}

int RGWRESTOSSRequest::abort_upload_multipart(const std::string& bucket, const std::string& obj, const std::string& upload_id) {
  std::string new_url = "http://" + bucket + "." + cloud_info.domain_name + "/" + obj +
                        "?uploadId=" + upload_id;
  std::string string2sign;
  std::string sign;
  std::string auth;
  std::string content_type = "application/octet-stream";
  std::string gmt = get_gmt_now();
  std::string resource = "/" + bucket + "/" + obj + "?uploadId=" + upload_id;
  create_oss_canonical_header("DELETE", resource, content_type, gmt, string2sign);
  get_oss_header_digest(string2sign ,cloud_info.private_key, sign);
  auth = "OSS ";
  auth.append(cloud_info.public_key);
  auth.append(":");
  auth.append(sign);

  headers.push_back(pair<std::string, std::string>("Authorization", auth));
  headers.push_back(pair<std::string, std::string>("Date", gmt));
  headers.push_back(pair<std::string, std::string>("Content-Type", content_type));

  int r = http_manager.add_request(this, "DELETE", new_url.c_str());
  if (r < 0)
    return r;

  r = complete();
  return r;
}

int RGWRESTOSSRequest::put_obj(const std::string& bucket, const string& obj, bufferlist* bl, uint64_t obj_size) {
  
  std::string new_url = "http://" + bucket + "." + cloud_info.domain_name + "/" + obj;
  
  std::string string2sign;
  std::string sign;
  std::string auth;
  std::string content_type = "application/octet-stream";
  std::string gmt = get_gmt_now();
  std::string resource = "/" + bucket + "/" + obj;
  create_oss_canonical_header("PUT", resource, content_type, gmt, string2sign);
  get_oss_header_digest(string2sign ,cloud_info.private_key, sign);
  auth = "OSS ";
  auth.append(cloud_info.public_key);
  auth.append(":");
  auth.append(sign);

  headers.push_back(pair<std::string, std::string>("Authorization", auth));
  headers.push_back(pair<std::string, std::string>("Date", gmt)); 
  headers.push_back(pair<std::string, std::string>("Content-Type", content_type));

  set_send_length(obj_size);

  int r = http_manager.add_request(this, "PUT", new_url.c_str());
  if (r < 0) {
    return r;
  }

  add_output_data(*bl, obj_size);
  
  r = complete();
  reset();
  return r;
}
    
int RGWRESTOSSRequest::rm_obj(const std::string& bucket, const string& obj) {

  std::string new_url = "http://" + bucket+ "." + cloud_info.domain_name + "/" + obj;

  std::string string2sign;
  std::string sign;
  std::string auth;
  std::string content_type = "application/octet-stream";
  std::string gmt = get_gmt_now();
  std::string resource = "/" + bucket + "/" + obj;
  create_oss_canonical_header("DELETE", resource, content_type, gmt, string2sign);
  get_oss_header_digest(string2sign, cloud_info.private_key, sign);
  auth = "OSS ";
  auth.append(cloud_info.public_key);
  auth.append(":");
  auth.append(sign);

  headers.push_back(pair<std::string, std::string>("Authorization", auth));
  headers.push_back(pair<std::string, std::string>("Date", gmt));
  headers.push_back(pair<std::string, std::string>("Content-Type", content_type));

  int r = http_manager.add_request(this, "DELETE", new_url.c_str());
  if (r < 0)
    return r;
  
  r = complete();
  return r;
}

int RGWRESTOSSRequest::create_bucket(const std::string& bucket) {
  std::string new_url = "http://" + bucket + "." + cloud_info.domain_name;

  std::string string2sign;
  std::string sign;
  std::string auth;
  std::string content_type = "";
  std::string gmt = get_gmt_now();
  std::string resource = "/" + bucket + "/";
  create_oss_canonical_header("PUT", resource, content_type, gmt, string2sign);
  //string2sign = "PUT" + "\n" + "\n" + content_type + "\n" + gmt + "\n" + resource;
  get_oss_header_digest(string2sign ,cloud_info.private_key, sign);
  auth = "OSS ";
  auth.append(cloud_info.public_key);
  auth.append(":");
  auth.append(sign);

  headers.push_back(pair<std::string, std::string>("Authorization", auth));
  headers.push_back(pair<std::string, std::string>("Date", gmt));

  int r = http_manager.add_request(this, "PUT", new_url.c_str());
  if (r < 0) {
    return r;
  }

  bufferlist resp;
  Formatter *f = Formatter::create("xml");
  f->open_object_section("CreateBucketConfiguration");
  encode_xml("StorageClass", "Standard", f);
  f->close_section();
  f->flush(resp);
  delete f;

  add_output_data(resp, resp.length());

  r = complete();
  reset();
  return r;
}

int RGWCloudOSS::init_multipart(const std::string& bucket, const string& key) {
  param_vec_t _headers;
  param_vec_t _params;
  std::string url = "";
  std::string dest_bucket = cloud_info.dest_bucket.empty() ? bucket : cloud_info.dest_bucket;
  dest_bucket = cloud_info.bucket_prefix + dest_bucket;

  int32_t retcode = 0;  

  RGWRESTOSSRequest* req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
  int ret = req->init_upload_multipart(dest_bucket, key, upload_id, retcode);
  delete req;
  if (ret >= 0)
    return ret;
 

  // if bucket not exist, create it
  if(retcode == RGWCloudOSS::OSS_BUCKET_NOT_EXIST) {
    req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
    ret = req->create_bucket(dest_bucket);
    delete req;
    if(ret < 0) {
      dout(0) << "oss create_bucket:"<<bucket<<" failed:" << ret<< dendl;
      return ret;
    }

    req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
    ret = req->init_upload_multipart(dest_bucket, key, upload_id, retcode);
    delete req;
    if (ret < 0) {
      dout(0) <<"oss init upload multipart failed:"<<ret<<" bucket:"<<bucket<<" file:"<<key<<dendl;
    }
    return ret;
  }
  else {
    dout(0) << "oss init upload multipart failed:" << ret<<" bucket:"<<bucket<<" file:"<<key<<dendl;
    return ret;
  }
}

int RGWCloudOSS::finish_multipart(const std::string& bucket, const string& key) {
  param_vec_t _headers;
  param_vec_t _params;
  std::string url = "";
  std::string dest_bucket = cloud_info.dest_bucket.empty() ? bucket : cloud_info.dest_bucket;
  dest_bucket = cloud_info.bucket_prefix + dest_bucket;

  RGWRESTOSSRequest* req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);  
  int ret = req->finish_upload_multipart(dest_bucket, key, upload_id, etags);

  delete req;
  if (ret < 0) {
    dout(0) << "cloud error finish upload multipart ret:" << ret << " "
                          << dest_bucket << " file:" << key << dendl;
    req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
    int r = req->abort_upload_multipart(dest_bucket, key, upload_id);
    delete req;
    if (r < 0) {
      dout(0) << "cloud error abort upload multipart when finish ret:" << ret << " "
                            << dest_bucket << " file:" << key << dendl;
    }
    return ret;
  }
  return ret;
}

int RGWCloudOSS::abort_multipart(const std::string& bucket, const string& key) {
  param_vec_t _headers;
  param_vec_t _params;
  std::string url = "";
  std::string dest_bucket = cloud_info.dest_bucket.empty() ? bucket : cloud_info.dest_bucket;
  dest_bucket = cloud_info.bucket_prefix + dest_bucket;

  RGWRESTOSSRequest* req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
  int ret = req->abort_upload_multipart(dest_bucket, key, upload_id);
  delete req;
  if (ret < 0) {
    dout(0) << "cloud error abort upload multipart when finish ret:" << ret << " "
                            << dest_bucket << " file:" << key << dendl;
  }
  return ret;
}

int RGWCloudOSS::upload_multipart(const std::string& bucket, const string& key, bufferlist& buf, uint64_t size) {
  param_vec_t _headers;
  param_vec_t _params;
  std::string url = "";
  std::string dest_bucket = cloud_info.dest_bucket.empty() ? bucket : cloud_info.dest_bucket;
  dest_bucket = cloud_info.bucket_prefix + dest_bucket;
  
  ++part_number;
  RGWRESTOSSRequest* req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
  string etag_part;
  int ret = req->upload_multipart(dest_bucket, key, upload_id, part_number, buf, size, etag_part);
  etags.insert(std::make_pair(part_number,etag_part));
  
  delete req;
  if (ret < 0) {
    dout(0) << "cloud error upload multipart ret:" << ret << " "
                          << dest_bucket << " file:" << key << dendl;
  }
  return ret;
}

int RGWCloudOSS::put_obj(const std::string& bucket, const string& key, bufferlist *bl, off_t bl_len) {
  param_vec_t _headers;
  param_vec_t _params;
  std::string url = "";
  std::string dest_bucket = cloud_info.dest_bucket.empty() ? bucket : cloud_info.dest_bucket;
  dest_bucket = cloud_info.bucket_prefix + dest_bucket;
  
  RGWRESTOSSRequest* req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
  int ret = req->put_obj(dest_bucket, key, bl, bl_len);
  if (ret >= 0) {
    delete req;
    return ret;
  }
  
  int retcode = 0;
  auto& buflist = req->get_response();
  string resp(buflist.c_str(), buflist.length());
  RGWRESTOSSRequest::get_oss_retcode(resp, retcode);
  delete req;

  // if bucket not exist, create it
  if(retcode == RGWCloudOSS::OSS_BUCKET_NOT_EXIST) {
    req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
    ret = req->create_bucket(dest_bucket);
    delete req;
    if(ret < 0) {
      dout(0) << "oss create_bucket:"<<bucket<<" failed:" << ret<< dendl;
      return ret;
    }
    
    req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
    ret = req->put_obj(dest_bucket, key, bl, bl_len);
    delete req;
    if (ret < 0) {
      dout(0) <<"oss put_obj failed:"<<ret<<" bucket:"<<bucket<<" file:"<<key<<dendl;
    }
  }
  else {
    dout(0) << "oss put_obj failed:" << ret<<" bucket:"<<bucket<<" file:"<<key<<dendl;
  }
  return ret;
}

int RGWCloudOSS::remove_obj(const std::string& bucket, const string& key) {
  param_vec_t _headers;
  param_vec_t _params;
  std::string url = "";
  std::string dest_bucket = cloud_info.dest_bucket.empty() ? bucket : cloud_info.dest_bucket;
  dest_bucket = cloud_info.bucket_prefix + dest_bucket;
  RGWRESTOSSRequest* req = new RGWRESTOSSRequest(cct, url, &_headers, &_params, cloud_info);
  int ret = req->rm_obj(dest_bucket, key);
  if (ret == 0) {
    dout(0) << "oss rm_obj:" << key << " success." << dendl;
    delete req;
    return ret; 
  }
   
  auto& resp = req->get_response();
  int resp_len = resp.length();
  if (resp_len <= 0) {
    delete req;
    return ret;
  }
  std::string msg(resp.c_str(), resp_len);
  delete req;

  dout(0) << "oss rm_obj:"<< key << " failed:" << ret<< " error msg:" << msg << dendl;
  return ret;
}
