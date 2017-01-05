// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 eNovance SAS <licensing@enovance.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fstream>
#include <map>
#include <list>
extern "C"{
#include <curl/curl.h>
}
#include "common/ceph_crypto.h"
#include "include/str_list.h"
#include "common/ceph_json.h"
#include "common/code_environment.h"
#include "common/ceph_argparse.h"
#include "common/Finisher.h"
#include "global/global_init.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_bucket.h"
#include "rgw/rgw_rados.h"
#include "include/utime.h"
#include "include/object.h"
#define GTEST
#ifdef GTEST
#include <gtest/gtest.h>
#else
#define TEST(x, y) void y()
#define ASSERT_EQ(v, s) if(v != s)cout << "Error at " << __LINE__ << "(" << #v << "!= " << #s << "\n"; \
                                else cout << "(" << #v << "==" << #s << ") PASSED\n";
#define EXPECT_EQ(v, s) ASSERT_EQ(v, s)
#define ASSERT_TRUE(c) if(c)cout << "Error at " << __LINE__ << "(" << #c << ")" << "\n"; \
                          else cout << "(" << #c << ") PASSED\n";
#define EXPECT_TRUE(c) ASSERT_TRUE(c) 
#endif
using namespace std;

#define CURL_VERBOSE 0
#define HTTP_RESPONSE_STR "RespCode"
#define CEPH_CRYPTO_HMACSHA1_DIGESTSIZE 20
#define RGW_ADMIN_RESP_PATH "/tmp/.test_rgw_admin_resp"
#define TEST_BUCKET_NAME "test_bucket"
#define TEST_BUCKET_OBJECT "test_object"
#define TEST_BUCKET_OBJECT_1 "test_object1"
#define TEST_BUCKET_OBJECT_SIZE 1024

static string uid = "ceph";
static string display_name = "CEPH";

extern "C" int ceph_armor(char *dst, const char *dst_end, 
                          const char *src, const char *end);
static void print_usage(char *exec){
  cout << "Usage: " << exec << " <Options>\n";
  cout << "Options:\n"
          "-g <gw-ip> - The ip address of the gateway\n"
          "-p <gw-port> - The port number of the gateway\n"
          "-c <ceph.conf> - Absolute path of ceph config file\n"
          "-rgw-admin <path/to/radosgw-admin> - radosgw-admin absolute path\n";
}

namespace admin_log {
class test_helper {
  private:
    string host;
    string port;
    string creds;
    string rgw_admin_path;
    string conf_path;
    CURL *curl_inst;
    map<string, string> response;
    list<string> extra_hdrs;
    string *resp_data;
    unsigned resp_code;
  public:
    test_helper() : resp_data(NULL){
      curl_global_init(CURL_GLOBAL_ALL);
    }
    ~test_helper(){
      curl_global_cleanup();
    }
    int send_request(string method, string uri, 
                     size_t (*function)(void *,size_t,size_t,void *) = 0,
                     void *ud = 0, size_t length = 0);
    int extract_input(int argc, char *argv[]);
    string& get_response(string hdr){
      return response[hdr];
    }
    void set_extra_header(string hdr){
      extra_hdrs.push_back(hdr);
    }
    void set_response(char *val);
    void set_response_data(char *data, size_t len){
      if(resp_data) delete resp_data;
      resp_data = new string(data, len);
    }
    string& get_rgw_admin_path() {
      return rgw_admin_path;
    }
    string& get_ceph_conf_path() {
      return conf_path;
    }
    void set_creds(string& c) {
      creds = c;
    }
    const string *get_response_data(){return resp_data;}
    unsigned get_resp_code(){return resp_code;}
};

int test_helper::extract_input(int argc, char *argv[]){
#define ERR_CHECK_NEXT_PARAM(o) \
  if(((int)loop + 1) >= argc)return -1;		\
  else o = argv[loop+1];

  for(unsigned loop = 1;loop < (unsigned)argc; loop += 2){
    if(strcmp(argv[loop], "-g") == 0){
      ERR_CHECK_NEXT_PARAM(host);
    }else if(strcmp(argv[loop],"-p") == 0){
      ERR_CHECK_NEXT_PARAM(port);
    }else if(strcmp(argv[loop], "-c") == 0){
      ERR_CHECK_NEXT_PARAM(conf_path);
    }else if(strcmp(argv[loop], "-rgw-admin") == 0){
      ERR_CHECK_NEXT_PARAM(rgw_admin_path);
    }else return -1;
  }
  if(host.length() <= 0 ||
     rgw_admin_path.length() <= 0)
    return -1;
  return 0;
}

void test_helper::set_response(char *r){
  string sr(r), h, v;
  size_t off = sr.find(": ");
  if(off != string::npos){
    h.assign(sr, 0, off);
    v.assign(sr, off + 2, sr.find("\r\n") - (off+2));
  }else{
    /*Could be the status code*/
    if(sr.find("HTTP/") != string::npos){
      h.assign(HTTP_RESPONSE_STR);
      off = sr.find(" ");
      v.assign(sr, off + 1, sr.find("\r\n") - (off + 1));
      resp_code = atoi((v.substr(0, 3)).c_str());
    }
  }
  response[h] = v;
}

size_t write_header(void *ptr, size_t size, size_t nmemb, void *ud){
  test_helper *h = static_cast<test_helper *>(ud);
  h->set_response((char *)ptr);
  return size*nmemb;
}

size_t write_data(void *ptr, size_t size, size_t nmemb, void *ud){
  test_helper *h = static_cast<test_helper *>(ud);
  h->set_response_data((char *)ptr, size*nmemb);
  return size*nmemb;
}

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

static void calc_hmac_sha1(const char *key, int key_len,
                    const char *msg, int msg_len, char *dest)
/* destination should be CEPH_CRYPTO_HMACSHA1_DIGESTSIZE bytes long */
{
  ceph::crypto::HMACSHA1 hmac((const unsigned char *)key, key_len);
  hmac.Update((const unsigned char *)msg, msg_len);
  hmac.Final((unsigned char *)dest);
  
  char hex_str[(CEPH_CRYPTO_HMACSHA1_DIGESTSIZE * 2) + 1];
  admin_log::buf_to_hex((unsigned char *)dest, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE, hex_str);
}

static int get_s3_auth(string method, string creds, string date, string res, string& out){
  string aid, secret, auth_hdr;
  string tmp_res;
  size_t off = creds.find(":");
  out = "";
  if(off != string::npos){
    aid.assign(creds, 0, off);
    secret.assign(creds, off + 1, string::npos);

    /*sprintf(auth_hdr, "%s\n\n\n%s\n%s", req_type, date, res);*/
    char hmac_sha1[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];
    char b64[65]; /* 64 is really enough */
    size_t off = res.find("?");
    if(off == string::npos)
      tmp_res = res;
    else
      tmp_res.assign(res, 0, off);
    auth_hdr.append(method + string("\n\n\n") + date + string("\n") + tmp_res);
    admin_log::calc_hmac_sha1(secret.c_str(), secret.length(), 
                               auth_hdr.c_str(), auth_hdr.length(), hmac_sha1);
    int ret = ceph_armor(b64, b64 + 64, hmac_sha1,
                         hmac_sha1 + CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
    if (ret < 0) {
      cout << "ceph_armor failed\n";
      return -1;
    }
    b64[ret] = 0;
    out.append(aid + string(":") + b64);
  }else return -1;
  return 0;
}

void get_date(string& d){
  struct timeval tv;
  char date[64];
  struct tm tm;
  char *days[] = {(char *)"Sun", (char *)"Mon", (char *)"Tue",
                  (char *)"Wed", (char *)"Thu", (char *)"Fri", 
                  (char *)"Sat"};
  char *months[] = {(char *)"Jan", (char *)"Feb", (char *)"Mar", 
                    (char *)"Apr", (char *)"May", (char *)"Jun",
                    (char *)"Jul",(char *) "Aug", (char *)"Sep", 
                    (char *)"Oct", (char *)"Nov", (char *)"Dec"};
  gettimeofday(&tv, NULL);
  gmtime_r(&tv.tv_sec, &tm);
  sprintf(date, "%s, %d %s %d %d:%d:%d GMT", 
          days[tm.tm_wday], 
          tm.tm_mday, months[tm.tm_mon], 
          tm.tm_year + 1900,
          tm.tm_hour, tm.tm_min, tm.tm_sec);
  d = date;
}

int test_helper::send_request(string method, string res, 
                                   size_t (*read_function)( void *,size_t,size_t,void *),
                                   void *ud,
                                   size_t length){
  string url;
  string auth, date;
  url.append(string("http://") + host);
  if(port.length() > 0)url.append(string(":") + port);
  url.append(res);
  curl_inst = curl_easy_init();
  if(curl_inst){
    curl_easy_setopt(curl_inst, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_inst, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(curl_inst, CURLOPT_VERBOSE, CURL_VERBOSE);
    curl_easy_setopt(curl_inst, CURLOPT_HEADERFUNCTION, admin_log::write_header);
    curl_easy_setopt(curl_inst, CURLOPT_WRITEHEADER, (void *)this);
    curl_easy_setopt(curl_inst, CURLOPT_WRITEFUNCTION, admin_log::write_data);
    curl_easy_setopt(curl_inst, CURLOPT_WRITEDATA, (void *)this);
    if(read_function){
      curl_easy_setopt(curl_inst, CURLOPT_READFUNCTION, read_function);
      curl_easy_setopt(curl_inst, CURLOPT_READDATA, (void *)ud);
      curl_easy_setopt(curl_inst, CURLOPT_UPLOAD, 1L);
      curl_easy_setopt(curl_inst, CURLOPT_INFILESIZE_LARGE, (curl_off_t)length);
    }

    get_date(date);
    string http_date;
    http_date.append(string("Date: ") + date);

    string s3auth;
    if (admin_log::get_s3_auth(method, creds, date, res, s3auth) < 0)
      return -1;
    auth.append(string("Authorization: AWS ") + s3auth);

    struct curl_slist *slist = NULL;
    slist = curl_slist_append(slist, auth.c_str());
    slist = curl_slist_append(slist, http_date.c_str());
    for(list<string>::iterator it = extra_hdrs.begin();
        it != extra_hdrs.end(); ++it){
      slist = curl_slist_append(slist, (*it).c_str());
    }
    if(read_function)
      curl_slist_append(slist, "Expect:");
    curl_easy_setopt(curl_inst, CURLOPT_HTTPHEADER, slist); 

    response.erase(response.begin(), response.end());
    extra_hdrs.erase(extra_hdrs.begin(), extra_hdrs.end());
    CURLcode res = curl_easy_perform(curl_inst);
    if(res != CURLE_OK){
      cout << "Curl perform failed for " << url << ", res: " << 
        curl_easy_strerror(res) << "\n";
      return -1;
    }
    curl_slist_free_all(slist);
  }
  curl_easy_cleanup(curl_inst);
  return 0;
}
};

admin_log::test_helper *g_test;
Finisher *finisher;

int run_rgw_admin(string& cmd, string& resp) {
  pid_t pid;
  pid = fork();
  if (pid == 0) {
    /* child */
    list<string> l;
    get_str_list(cmd, " \t", l);
    char *argv[l.size()];
    unsigned loop = 1;

    argv[0] = (char *)"radosgw-admin";
    for (list<string>::iterator it = l.begin(); 
         it != l.end(); ++it) {
      argv[loop++] = (char *)(*it).c_str();
    }
    argv[loop] = NULL;
    if (!freopen(RGW_ADMIN_RESP_PATH, "w+", stdout)) {
      cout << "Unable to open stdout file" << std::endl;
    }
    execv((g_test->get_rgw_admin_path()).c_str(), argv); 
  } else if (pid > 0) {
    int status;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status)) {
      if(WEXITSTATUS(status) != 0) {
        cout << "Child exited with status " << WEXITSTATUS(status) << std::endl;
        return -1;
      }
    }
    ifstream in;
    struct stat st;

    if (stat(RGW_ADMIN_RESP_PATH, &st) < 0) {
      cout << "Error stating the admin response file, errno " << errno << std::endl;
      return -1;
    } else {
      char *data = (char *)malloc(st.st_size + 1);
      in.open(RGW_ADMIN_RESP_PATH);
      in.read(data, st.st_size);
      in.close();
      data[st.st_size] = 0;
      resp = data;
      free(data);
      unlink(RGW_ADMIN_RESP_PATH);
      /* cout << "radosgw-admin " << cmd << ": " << resp << std::endl; */
    }
  } else 
    return -1;
  return 0;
}

int get_creds(string& json, string& creds) {
  JSONParser parser;
  if(!parser.parse(json.c_str(), json.length())) {
    cout << "Error parsing create user response" << std::endl;
    return -1;
  }

  RGWUserInfo info;
  decode_json_obj(info, &parser);
  creds = "";
  for(map<string, RGWAccessKey>::iterator it = info.access_keys.begin();
      it != info.access_keys.end(); ++it) {
    RGWAccessKey _k = it->second;
    /*cout << "accesskeys [ " << it->first << " ] = " << 
      "{ " << _k.id << ", " << _k.key << ", " << _k.subuser << "}" << std::endl;*/
    creds.append(it->first + string(":") + _k.key);
    break;
  }
  return 0;
}

int user_create(string& uid, string& display_name, bool set_creds = true) {
  stringstream ss;
  string creds;
  ss << "-c " << g_test->get_ceph_conf_path() << " user create --uid=" << uid
    << " --display-name=" << display_name;

  string out;
  string cmd = ss.str();
  if(run_rgw_admin(cmd, out) != 0) {
    cout << "Error creating user" << std::endl;
    return -1;
  }
  get_creds(out, creds);
  if(set_creds)
    g_test->set_creds(creds);
  return 0;
}

int user_info(string& uid, string& display_name, RGWUserInfo& uinfo) {
  stringstream ss;
  ss << "-c " << g_test->get_ceph_conf_path() << " user info --uid=" << uid
    << " --display-name=" << display_name;

  string out;
  string cmd = ss.str();
  if(run_rgw_admin(cmd, out) != 0) {
    cout << "Error reading user information" << std::endl;
    return -1;
  }
  JSONParser parser;
  if(!parser.parse(out.c_str(), out.length())) {
    cout << "Error parsing create user response" << std::endl;
    return -1;
  }
  decode_json_obj(uinfo, &parser);
  return 0;
}

int user_rm(string& uid, string& display_name) {
  stringstream ss;
  ss << "-c " << g_test->get_ceph_conf_path() << 
    " metadata rm --metadata-key=user:" << uid;

  string out;
  string cmd = ss.str();
  if(run_rgw_admin(cmd, out) != 0) {
    cout << "Error removing user" << std::endl;
    return -1;
  }
  return 0;
}

int caps_add(const char * name, const char *perm) {
  stringstream ss;

  ss << "-c " << g_test->get_ceph_conf_path() << " caps add --caps=" <<
     name << "=" << perm << " --uid=" << uid;
  string out;
  string cmd = ss.str();
  if(run_rgw_admin(cmd, out) != 0) {
    cout << "Error creating user" << std::endl;
    return -1;
  }
  return 0;
}

int caps_rm(const char * name, const char *perm) {
  stringstream ss;

  ss << "-c " << g_test->get_ceph_conf_path() << " caps rm --caps=" <<
     name << "=" << perm << " --uid=" << uid;
  string out;
  string cmd = ss.str();
  if(run_rgw_admin(cmd, out) != 0) {
    cout << "Error creating user" << std::endl;
    return -1;
  }
  return 0;
}

static int create_bucket(void){
  g_test->send_request(string("PUT"), string("/" TEST_BUCKET_NAME));
  if(g_test->get_resp_code() != 200U){
    cout << "Error creating bucket, http code " << g_test->get_resp_code();
    return -1;
  }
  return 0;
}

static int delete_bucket(void){
  g_test->send_request(string("DELETE"), string("/" TEST_BUCKET_NAME));
  if(g_test->get_resp_code() != 204U){
    cout << "Error deleting bucket, http code " << g_test->get_resp_code();
    return -1;
  }
  return 0;
}

size_t read_dummy_post(void *ptr, size_t s, size_t n, void *ud) {
  int dummy = 0;
  memcpy(ptr, &dummy, sizeof(dummy));
  return sizeof(dummy);
}

size_t read_bucket_object(void *ptr, size_t s, size_t n, void *ud) {
  memcpy(ptr, ud, TEST_BUCKET_OBJECT_SIZE);
  return TEST_BUCKET_OBJECT_SIZE;
}

static int put_bucket_obj(const char *obj_name, char *data, unsigned len) {
  string req = "/" TEST_BUCKET_NAME"/";
  req.append(obj_name);
  g_test->send_request(string("PUT"), req,
                       read_bucket_object, (void *)data, (size_t)len);
  if (g_test->get_resp_code() != 200U) {
    cout << "Errror sending object to the bucket, http_code " << g_test->get_resp_code();
    return -1;
  }
  return 0;
}

static int read_bucket_obj(const char *obj_name) {
  string req = "/" TEST_BUCKET_NAME"/";
  req.append(obj_name);
  g_test->send_request(string("GET"), req);
  if (g_test->get_resp_code() != 200U) {
    cout << "Errror sending object to the bucket, http_code " << g_test->get_resp_code();
    return -1;
  }
  return 0;
}

static int delete_obj(const char *obj_name) {
  string req = "/" TEST_BUCKET_NAME"/";
  req.append(obj_name);
  g_test->send_request(string("DELETE"), req);
  if (g_test->get_resp_code() != 204U) {
    cout << "Errror deleting object from bucket, http_code " << g_test->get_resp_code();
    return -1;
  }
  return 0;
}

int get_formatted_time(string& ret) {
  struct tm *tm = NULL;
  char str_time[200];
  const char *format = "%Y-%m-%d%%20%H:%M:%S";
  time_t t;

  t = time(NULL);
  tm = gmtime(&t);
  if(!tm) {
    cerr << "Error returned by gmtime\n";
    return -1;
  }
  if (strftime(str_time, sizeof(str_time), format, tm) == 0) {
    cerr << "Error returned by strftime\n";
    return -1;
  }
  ret = str_time;
  return 0;
}

int parse_json_resp(JSONParser &parser) {
  string *resp;
  resp = (string *)g_test->get_response_data();
  if(!resp)
    return -1;
  if(!parser.parse(resp->c_str(), resp->length())) {
    cout << "Error parsing create user response" << std::endl;
    return -1;
  }
  return 0;
}

struct cls_log_entry_json {
  string section;
  string name;
  utime_t timestamp;
  RGWMetadataLogData log_data;
};

static int decode_json(JSONObj *obj, RGWMetadataLogData &data) {
  JSONObj *jo;

  jo = obj->find_obj("read_version");
  if (!jo)
    return -1;
  data.read_version.decode_json(obj);
  data.write_version.decode_json(obj);

  jo = obj->find_obj("status");
  if (!jo)
    return -1;
  JSONDecoder::decode_json("status", data, jo);
  return 0;
}

static int decode_json(JSONObj *obj, cls_log_entry_json& ret) {
  JSONDecoder::decode_json("section", ret.section, obj);
  JSONDecoder::decode_json("name", ret.name, obj);
  JSONObj *jo = obj->find_obj("data");
  if(!jo) 
    return 0;
  return decode_json(jo, ret.log_data);
}

static int get_log_list(list<cls_log_entry_json> &entries) {
  JSONParser parser;
  if (parse_json_resp(parser) != 0)
    return -1;
  if (!parser.is_array()) 
    return -1;

  vector<string> l;
  l = parser.get_array_elements();
  int loop = 0;
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it, loop++) {
    JSONParser jp;
    cls_log_entry_json entry;

    if(!jp.parse((*it).c_str(), (*it).length())) {
      cerr << "Error parsing log json object" << std::endl;
      return -1;
    }
    EXPECT_EQ(decode_json((JSONObj *)&jp, entry), 0);
    entries.push_back(entry);
  }
  return 0;
}

struct cls_bilog_entry {
  string op_id;
  string op_tag;
  string op;
  string object;
  string status;
  unsigned index_ver;
};

static int decode_json(JSONObj *obj, cls_bilog_entry& ret) {
  JSONDecoder::decode_json("op_id", ret.op_id, obj);
  JSONDecoder::decode_json("op_tag", ret.op_tag, obj);
  JSONDecoder::decode_json("op", ret.op, obj);
  JSONDecoder::decode_json("object", ret.object, obj);
  JSONDecoder::decode_json("state", ret.status, obj);
  JSONDecoder::decode_json("index_ver", ret.index_ver, obj);
  return 0;
}

static int get_bilog_list(list<cls_bilog_entry> &entries) {
  JSONParser parser;
  if (parse_json_resp(parser) != 0)
    return -1;
  if (!parser.is_array()) 
    return -1;

  vector<string> l;
  l = parser.get_array_elements();
  int loop = 0;
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it, loop++) {
    JSONParser jp;
    cls_bilog_entry entry;

    if(!jp.parse((*it).c_str(), (*it).length())) {
      cerr << "Error parsing log json object" << std::endl;
      return -1;
    }
    EXPECT_EQ(decode_json((JSONObj *)&jp, entry), 0);
    entries.push_back(entry);
  }
  return 0;
}

static int decode_json(JSONObj *obj, rgw_data_change& ret) {
  string entity;

  JSONDecoder::decode_json("entity_type", entity, obj);
  if (entity.compare("bucket") == 0)
    ret.entity_type = ENTITY_TYPE_BUCKET;
  JSONDecoder::decode_json("key", ret.key, obj);
  return 0;
}

static int get_datalog_list(list<rgw_data_change> &entries) {
  JSONParser parser;

  if (parse_json_resp(parser) != 0)
    return -1;
  if (!parser.is_array()) 
    return -1;

  vector<string> l;
  l = parser.get_array_elements();
  int loop = 0;
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it, loop++) {
    JSONParser jp;
    rgw_data_change entry;

    if(!jp.parse((*it).c_str(), (*it).length())) {
      cerr << "Error parsing log json object" << std::endl;
      return -1;
    }
    EXPECT_EQ(decode_json((JSONObj *)&jp, entry), 0);
    entries.push_back(entry);
  }
  return 0;
}

unsigned get_mdlog_shard_id(string& key, int max_shards) {
  string section = "user";
  uint32_t val = ceph_str_hash_linux(key.c_str(), key.size());
  val ^= ceph_str_hash_linux(section.c_str(), section.size());
  return (unsigned)(val % max_shards);
}

unsigned get_datalog_shard_id(const char *bucket_name, int max_shards) {
  uint32_t r = ceph_str_hash_linux(bucket_name, strlen(bucket_name)) % max_shards;
  return (int)r;
}

TEST(TestRGWAdmin, datalog_list) {
  string start_time, 
         end_time;
  const char *cname = "datalog",
             *perm = "*";
  string rest_req;
  unsigned shard_id = get_datalog_shard_id(TEST_BUCKET_NAME, g_ceph_context->_conf->rgw_data_log_num_shards);
  stringstream ss;
  list<rgw_data_change> entries;

  ASSERT_EQ(get_formatted_time(start_time), 0);
  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, caps_add(cname, perm));

  rest_req = "/admin/log?type=data";
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  JSONParser parser;
  int num_objects;
  EXPECT_EQ (parse_json_resp(parser), 0);
  JSONDecoder::decode_json("num_objects", num_objects, (JSONObj *)&parser);
  ASSERT_EQ(num_objects,g_ceph_context->_conf->rgw_data_log_num_shards);
 
  sleep(1);
  ASSERT_EQ(0, create_bucket());
  
  char *bucket_obj = (char *)calloc(1, TEST_BUCKET_OBJECT_SIZE);
  ASSERT_TRUE(bucket_obj != NULL);
  EXPECT_EQ(put_bucket_obj(TEST_BUCKET_OBJECT, bucket_obj, TEST_BUCKET_OBJECT_SIZE), 0);
  sleep(1); 
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_datalog_list(entries);
  EXPECT_EQ(1U, entries.size());
  if (entries.size() == 1) {
    rgw_data_change entry = *(entries.begin());
    EXPECT_EQ(entry.entity_type, ENTITY_TYPE_BUCKET);
    EXPECT_EQ(entry.key.compare(TEST_BUCKET_NAME), 0);
  }
  ASSERT_EQ(0, delete_obj(TEST_BUCKET_OBJECT));
  sleep(1);
  ASSERT_EQ(get_formatted_time(end_time), 0);
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_datalog_list(entries);
  EXPECT_EQ(1U, entries.size());
  if (entries.size() == 1) {
    list<rgw_data_change>::iterator it = (entries.begin());
    EXPECT_EQ((*it).entity_type, ENTITY_TYPE_BUCKET);
    EXPECT_EQ((*it).key.compare(TEST_BUCKET_NAME), 0);
  }

  sleep(1);
  EXPECT_EQ(put_bucket_obj(TEST_BUCKET_OBJECT, bucket_obj, TEST_BUCKET_OBJECT_SIZE), 0);
  free(bucket_obj);
  sleep(20);
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_datalog_list(entries);
  EXPECT_EQ(2U, entries.size());
  if (entries.size() == 2) {
    list<rgw_data_change>::iterator it = (entries.begin());
    EXPECT_EQ((*it).entity_type, ENTITY_TYPE_BUCKET);
    EXPECT_EQ((*it).key.compare(TEST_BUCKET_NAME), 0);
    ++it; 
    EXPECT_EQ((*it).entity_type, ENTITY_TYPE_BUCKET);
    EXPECT_EQ((*it).key.compare(TEST_BUCKET_NAME), 0);
  }

  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time
    << "&max-entries=1";
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_datalog_list(entries);
  EXPECT_EQ(1U, entries.size());
  
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time 
    << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_datalog_list(entries);
  EXPECT_EQ(1U, entries.size());

  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "read";
  ASSERT_EQ(0, caps_add(cname, perm));
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  ASSERT_EQ(0, caps_rm(cname, perm));
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();

  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(403U, g_test->get_resp_code());

  ASSERT_EQ(0, delete_obj(TEST_BUCKET_OBJECT));
  ASSERT_EQ(0, delete_bucket());
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, datalog_lock_unlock) {
  const char *cname = "datalog",
             *perm = "*";
  string rest_req;

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, caps_add(cname, perm));

  rest_req = "/admin/log?type=data&lock&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=data&lock&id=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=data&lock&length=3&id=1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=data&lock&length=3&id=1&locker-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/

  rest_req = "/admin/log?type=data&unlock&id=1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/

  rest_req = "/admin/log?type=data&unlock&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=data&unlock&locker-id=ceph&id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=data&unlock&id=1&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=data&unlock&id=1&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  utime_t sleep_time(3, 0);

  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(500U, g_test->get_resp_code()); 

  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph1&zone-id=2";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(500U, g_test->get_resp_code()); 

  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  sleep_time.sleep();

  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=data&unlock&id=1&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 

  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "read";
  ASSERT_EQ(0, caps_add(cname, perm));
  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=data&unlock&id=1&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "write";
  ASSERT_EQ(0, caps_add(cname, perm));
  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=data&unlock&id=1&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, caps_rm(cname, perm));
  rest_req = "/admin/log?type=data&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=data&unlock&id=1&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, datalog_trim) {
  string start_time, 
         end_time;
  const char *cname = "datalog",
             *perm = "*";
  string rest_req;
  unsigned shard_id = get_datalog_shard_id(TEST_BUCKET_NAME, g_ceph_context->_conf->rgw_data_log_num_shards);
  stringstream ss;
  list<rgw_data_change> entries;

  ASSERT_EQ(get_formatted_time(start_time), 0);
  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, caps_add(cname, perm));

  rest_req = "/admin/log?type=data";
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  ss.str("");
  ss << "/admin/log?type=data&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
 
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  ASSERT_EQ(0, create_bucket());

  char *bucket_obj = (char *)calloc(1, TEST_BUCKET_OBJECT_SIZE);
  ASSERT_TRUE(bucket_obj != NULL);
  EXPECT_EQ(put_bucket_obj(TEST_BUCKET_OBJECT, bucket_obj, TEST_BUCKET_OBJECT_SIZE), 0);
  ASSERT_EQ(0, delete_obj(TEST_BUCKET_OBJECT));
  sleep(1);
  EXPECT_EQ(put_bucket_obj(TEST_BUCKET_OBJECT, bucket_obj, TEST_BUCKET_OBJECT_SIZE), 0);
  ASSERT_EQ(0, delete_obj(TEST_BUCKET_OBJECT));
  sleep(20);
  free(bucket_obj);

  ASSERT_EQ(get_formatted_time(end_time), 0);
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time 
    << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_datalog_list(entries);
  EXPECT_TRUE(!entries.empty());

  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time 
    << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());

  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time 
    << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_datalog_list(entries);
  EXPECT_TRUE(entries.empty());

  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "write";
  ASSERT_EQ(0, caps_add(cname, perm));
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time 
    << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "";
  ASSERT_EQ(0, caps_add(cname, perm));
  ss.str("");
  ss << "/admin/log?type=data&id=" << shard_id << "&start-time=" << start_time 
    << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(403U, g_test->get_resp_code());

  ASSERT_EQ(0, delete_bucket());
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, mdlog_list) {
  string start_time, 
         end_time,
         start_time_2;
  const char *cname = "mdlog",
             *perm = "*";
  string rest_req;
  unsigned shard_id = get_mdlog_shard_id(uid, g_ceph_context->_conf->rgw_md_log_max_shards);
  stringstream ss;

  sleep(2);
  ASSERT_EQ(get_formatted_time(start_time), 0);
  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, caps_add(cname, perm));

  rest_req = "/admin/log?type=metadata";
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  JSONParser parser;
  int num_objects;
  EXPECT_EQ (parse_json_resp(parser), 0);
  JSONDecoder::decode_json("num_objects", num_objects, (JSONObj *)&parser);
  ASSERT_EQ(num_objects,g_ceph_context->_conf->rgw_md_log_max_shards);

  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  
  list<cls_log_entry_json> entries;
  EXPECT_EQ(get_log_list(entries), 0);
  EXPECT_EQ(entries.size(), 4U);

  if(entries.size() == 4) {
    list<cls_log_entry_json>::iterator it = entries.begin();
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_WRITE);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_COMPLETE);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_WRITE);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_COMPLETE);
  }

  sleep(1); /*To get a modified time*/
  ASSERT_EQ(get_formatted_time(start_time_2), 0);
  ASSERT_EQ(0, caps_rm(cname, perm));
  perm="read";
  ASSERT_EQ(0, caps_add(cname, perm));
  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time_2;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
 
  entries.clear();
  EXPECT_EQ(get_log_list(entries), 0);
  EXPECT_EQ(entries.size(), 4U);

  if(entries.size() == 4) {
    list<cls_log_entry_json>::iterator it = entries.begin();
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_WRITE);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_COMPLETE);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_WRITE);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_COMPLETE);
  }

  sleep(1);
  ASSERT_EQ(get_formatted_time(start_time_2), 0);
  ASSERT_EQ(0, user_rm(uid, display_name));
  
  ASSERT_EQ(0, user_create(uid, display_name));
  perm = "*";
  ASSERT_EQ(0, caps_add(cname, perm));

  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time_2;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  
  entries.clear();
  EXPECT_EQ(get_log_list(entries), 0);
  EXPECT_EQ(entries.size(), 6U);
  if(entries.size() == 6) {
    list<cls_log_entry_json>::iterator it = entries.begin();
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_REMOVE);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_WRITE);
    ++it;
    EXPECT_TRUE(it->section.compare("user") == 0);
    EXPECT_TRUE(it->name.compare(uid) == 0);
    EXPECT_TRUE(it->log_data.status == MDLOG_STATUS_COMPLETE);
  }

  sleep(1);
  ASSERT_EQ(get_formatted_time(end_time), 0);
  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time 
    << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  EXPECT_EQ(get_log_list(entries), 0);
  EXPECT_EQ(entries.size(), 14U);

  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time 
    << "&max-entries=" << 1;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  EXPECT_EQ(get_log_list(entries), 0);
  EXPECT_EQ(entries.size(), 1U);

  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time 
    << "&max-entries=" << 6;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  EXPECT_EQ(get_log_list(entries), 0);
  EXPECT_EQ(entries.size(), 6U);

  ASSERT_EQ(0, caps_rm(cname, perm));
  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(403U, g_test->get_resp_code());

  /*cleanup*/
  ASSERT_EQ(0, caps_add(cname, perm));
  sleep(1);
  ASSERT_EQ(get_formatted_time(end_time), 0);
  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time 
    << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, mdlog_trim) {
  string start_time, 
         end_time;
  const char *cname = "mdlog",
             *perm = "*";
  string rest_req;
  list<cls_log_entry_json> entries;
  unsigned shard_id = get_mdlog_shard_id(uid, g_ceph_context->_conf->rgw_md_log_max_shards);
  ostringstream ss;

  sleep(1);
  ASSERT_EQ(get_formatted_time(start_time), 0);
  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, caps_add(cname, perm));

  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/

  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  EXPECT_EQ(get_log_list(entries), 0);
  EXPECT_EQ(entries.size(), 4U);

  sleep(1);
  ASSERT_EQ(get_formatted_time(end_time), 0);
  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());

  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time;
  rest_req = ss.str();
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  EXPECT_EQ(get_log_list(entries), 0);
  EXPECT_EQ(entries.size(), 0U);

  ASSERT_EQ(0, caps_rm(cname, perm));
  perm="write";
  ASSERT_EQ(0, caps_add(cname, perm));
  ASSERT_EQ(get_formatted_time(end_time), 0);
  ss.str("");
  ss << "/admin/log?type=metadata&id=" << shard_id << "&start-time=" << start_time << "&end-time=" << end_time;
  rest_req = ss.str();
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_EQ(0, caps_rm(cname, perm));
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(403U, g_test->get_resp_code());
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, mdlog_lock_unlock) {
  const char *cname = "mdlog",
             *perm = "*";
  string rest_req;

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, caps_add(cname, perm));

  rest_req = "/admin/log?type=metadata&lock&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=metadata&lock&id=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=metadata&lock&length=3&id=1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/

  rest_req = "/admin/log?type=metadata&lock&id=3&locker-id=ceph&length=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=metadata&unlock&id=1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/

  rest_req = "/admin/log?type=metadata&unlock&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=metadata&unlock&locker-id=ceph&id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=metadata&unlock&id=1&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=metadata&unlock&id=1&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  utime_t sleep_time(3, 0);

  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(500U, g_test->get_resp_code()); 

  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph&zone-id=2";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(500U, g_test->get_resp_code()); 

  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  sleep_time.sleep();

  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=metadata&unlock&id=1&locker-id=ceph1&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 

  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "read";
  ASSERT_EQ(0, caps_add(cname, perm));
  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=metadata&unlock&id=1&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "write";
  ASSERT_EQ(0, caps_add(cname, perm));
  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=metadata&unlock&id=1&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, caps_rm(cname, perm));
  rest_req = "/admin/log?type=metadata&lock&id=1&length=3&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  rest_req = "/admin/log?type=metadata&unlock&id=1&locker-id=ceph&zone-id=1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, bilog_list) {
  const char *cname = "bilog",
             *perm = "*";
  string rest_req;

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, caps_add(cname, perm));

  ASSERT_EQ(0, create_bucket());

  char *bucket_obj = (char *)calloc(1, TEST_BUCKET_OBJECT_SIZE);
  ASSERT_TRUE(bucket_obj != NULL);
  EXPECT_EQ(put_bucket_obj(TEST_BUCKET_OBJECT, bucket_obj, TEST_BUCKET_OBJECT_SIZE), 0);
  free(bucket_obj);
  
  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  list<cls_bilog_entry> entries;
  get_bilog_list(entries);
  EXPECT_EQ(2U, entries.size());
  if (entries.size() == 2) {
    list<cls_bilog_entry>::iterator it = entries.begin();
    EXPECT_EQ(it->op.compare("write"), 0);
    EXPECT_EQ(it->object.compare(TEST_BUCKET_OBJECT), 0);
    EXPECT_EQ(it->status.compare("pending"), 0);
    EXPECT_EQ(it->index_ver, 1U);
    ++it;
    EXPECT_EQ(it->op.compare("write"), 0);
    EXPECT_EQ(it->object.compare(TEST_BUCKET_OBJECT), 0);
    EXPECT_EQ(it->status.compare("complete"), 0);
    EXPECT_EQ(it->index_ver, 2U);
  }
  EXPECT_EQ(read_bucket_obj(TEST_BUCKET_OBJECT), 0);
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_bilog_list(entries);
  EXPECT_EQ(2U, entries.size());

  bucket_obj = (char *)calloc(1, TEST_BUCKET_OBJECT_SIZE);
  ASSERT_TRUE(bucket_obj != NULL);
  EXPECT_EQ(put_bucket_obj(TEST_BUCKET_OBJECT_1, bucket_obj, TEST_BUCKET_OBJECT_SIZE), 0);
  free(bucket_obj);
  
  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_bilog_list(entries);
  EXPECT_EQ(4U, entries.size());
  if (entries.size() == 4) {
    list<cls_bilog_entry>::iterator it = entries.begin();

    ++it; ++it;
    EXPECT_EQ(it->op.compare("write"), 0);
    EXPECT_EQ(it->object.compare(TEST_BUCKET_OBJECT_1), 0);
    EXPECT_EQ(it->status.compare("pending"), 0);
    EXPECT_EQ(it->index_ver, 3U);
    ++it;
    EXPECT_EQ(it->op.compare("write"), 0);
    EXPECT_EQ(it->object.compare(TEST_BUCKET_OBJECT_1), 0);
    EXPECT_EQ(it->status.compare("complete"), 0);
    EXPECT_EQ(it->index_ver, 4U);
  }

  ASSERT_EQ(0, delete_obj(TEST_BUCKET_OBJECT));
  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_bilog_list(entries);

  EXPECT_EQ(6U, entries.size());
  string marker;
  if (entries.size() == 6) {
    list<cls_bilog_entry>::iterator it = entries.begin();
    
    ++it; ++it; ++it; ++it;
    marker = it->op_id;
    EXPECT_EQ(it->op.compare("del"), 0);
    EXPECT_EQ(it->object.compare(TEST_BUCKET_OBJECT), 0);
    EXPECT_EQ(it->status.compare("pending"), 0);
    EXPECT_EQ(it->index_ver, 5U);
    ++it;
    EXPECT_EQ(it->op.compare("del"), 0);
    EXPECT_EQ(it->object.compare(TEST_BUCKET_OBJECT), 0);
    EXPECT_EQ(it->status.compare("complete"), 0);
    EXPECT_EQ(it->index_ver, 6U);
  }

  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  rest_req.append("&marker=");
  rest_req.append(marker);
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_bilog_list(entries);
  EXPECT_EQ(2U, entries.size());
  if (entries.size() == 2U) {
    list<cls_bilog_entry>::iterator it = entries.begin();
    EXPECT_EQ(it->index_ver, 5U);
    ++it;
    EXPECT_EQ(it->index_ver, 6U);
    EXPECT_EQ(it->op.compare("del"), 0);
  }

  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  rest_req.append("&marker=");
  rest_req.append(marker);
  rest_req.append("&max-entries=1");
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_bilog_list(entries);
  EXPECT_EQ(1U, entries.size());
  EXPECT_EQ((entries.begin())->index_ver, 5U);

  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "read";
  ASSERT_EQ(0, caps_add(cname, perm));
  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_EQ(0, caps_rm(cname, perm));
  perm = "write";
  ASSERT_EQ(0, caps_add(cname, perm));
  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(403U, g_test->get_resp_code());

  ASSERT_EQ(0, delete_obj(TEST_BUCKET_OBJECT_1));
  ASSERT_EQ(0, delete_bucket());
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, bilog_trim) {
  const char *cname = "bilog",
             *perm = "*";
  string rest_req, start_marker, end_marker;

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, caps_add(cname, perm));

  ASSERT_EQ(0, create_bucket());

  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/

  char *bucket_obj = (char *)calloc(1, TEST_BUCKET_OBJECT_SIZE);
  ASSERT_TRUE(bucket_obj != NULL);
  EXPECT_EQ(put_bucket_obj(TEST_BUCKET_OBJECT, bucket_obj, TEST_BUCKET_OBJECT_SIZE), 0);
  free(bucket_obj);
  
  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  list<cls_bilog_entry> entries;
  get_bilog_list(entries);
  EXPECT_EQ(2U, entries.size());

  list<cls_bilog_entry>::iterator it = entries.begin();
  start_marker = it->op_id;
  ++it;
  end_marker = it->op_id;

  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  rest_req.append("&start-marker=");
  rest_req.append(start_marker);
  rest_req.append("&end-marker=");
  rest_req.append(end_marker);
  g_test->send_request(string("DELETE"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());

  rest_req = "/admin/log?type=bucket-index&bucket=" TEST_BUCKET_NAME;
  g_test->send_request(string("GET"), rest_req);
  EXPECT_EQ(200U, g_test->get_resp_code());
  entries.clear();
  get_bilog_list(entries);
  EXPECT_EQ(0U, entries.size());
  
  ASSERT_EQ(0, delete_obj(TEST_BUCKET_OBJECT));
  ASSERT_EQ(0, delete_bucket());
  ASSERT_EQ(0, user_rm(uid, display_name));
}

int main(int argc, char *argv[]){
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_test = new admin_log::test_helper();
  finisher = new Finisher(g_ceph_context);
#ifdef GTEST
  ::testing::InitGoogleTest(&argc, argv);
#endif
  finisher->start();

  if(g_test->extract_input(argc, argv) < 0){
    print_usage(argv[0]);
    return -1;
  }
#ifdef GTEST
  int r = RUN_ALL_TESTS();
  if (r >= 0) {
    cout << "There are no failures in the test case\n";
  } else {
    cout << "There are some failures\n";
  }
#endif
  finisher->stop();
  return 0;
}
