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
#include "rgw/rgw_rados.h"
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
#define CEPH_UID  "ceph"

static string uid = CEPH_UID;
static string display_name = "CEPH";
static string meta_caps = "metadata";

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

namespace admin_meta {
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
    test_helper() : curl_inst(0), resp_data(NULL), resp_code(0) {
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
  admin_meta::buf_to_hex((unsigned char *)dest, CEPH_CRYPTO_HMACSHA1_DIGESTSIZE, hex_str);
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
    admin_meta::calc_hmac_sha1(secret.c_str(), secret.length(), 
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
          tm.tm_hour, tm.tm_min, 0 /*tm.tm_sec*/);
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
    curl_easy_setopt(curl_inst, CURLOPT_HEADERFUNCTION, admin_meta::write_header);
    curl_easy_setopt(curl_inst, CURLOPT_WRITEHEADER, (void *)this);
    curl_easy_setopt(curl_inst, CURLOPT_WRITEFUNCTION, admin_meta::write_data);
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
    if (admin_meta::get_s3_auth(method, creds, date, res, s3auth) < 0)
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

admin_meta::test_helper *g_test;
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
      /* cout << "radosgw-admin " << cmd << ": " << resp << std::endl;*/
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
  ss << "-c " << g_test->get_ceph_conf_path() << " user rm --uid=" << uid
    << " --display-name=" << display_name;

  string out;
  string cmd = ss.str();
  if(run_rgw_admin(cmd, out) != 0) {
    cout << "Error removing user" << std::endl;
    return -1;
  }
  return 0;
}

int meta_caps_add(const char *perm) {
  stringstream ss;

  ss << "-c " << g_test->get_ceph_conf_path() << " caps add --caps=" <<
     meta_caps << "=" << perm << " --uid=" << uid;
  string out;
  string cmd = ss.str();
  if(run_rgw_admin(cmd, out) != 0) {
    cout << "Error creating user" << std::endl;
    return -1;
  }
  return 0;
}

int meta_caps_rm(const char *perm) {
  stringstream ss;

  ss << "-c " << g_test->get_ceph_conf_path() << " caps rm --caps=" <<
     meta_caps << "=" << perm << " --uid=" << uid;
  string out;
  string cmd = ss.str();
  if(run_rgw_admin(cmd, out) != 0) {
    cout << "Error creating user" << std::endl;
    return -1;
  }
  return 0;
}

int compare_access_keys(RGWAccessKey& k1, RGWAccessKey& k2) {
  if (k1.id.compare(k2.id) != 0)
    return -1;
  if (k1.key.compare(k2.key) != 0) 
    return -1;
  if (k1.subuser.compare(k2.subuser) != 0) 
    return -1;

  return 0;
}

int compare_user_info(RGWUserInfo& i1, RGWUserInfo& i2) {
  int rv;

  if ((rv = i1.user_id.compare(i2.user_id)) != 0)
    return rv;
  if ((rv = i1.display_name.compare(i2.display_name)) != 0)
    return rv;
  if ((rv = i1.user_email.compare(i2.user_email)) != 0)
    return rv;
  if (i1.access_keys.size() != i2.access_keys.size())
    return -1;
  for (map<string, RGWAccessKey>::iterator it = i1.access_keys.begin();
       it != i1.access_keys.end(); ++it) {
    RGWAccessKey k1, k2;
    k1 = it->second;
    if (i2.access_keys.count(it->first) == 0)
      return -1;
    k2 = i2.access_keys[it->first];
    if (compare_access_keys(k1, k2) != 0)
      return -1;
  }
  if (i1.swift_keys.size() != i2.swift_keys.size())
    return -1;
  for (map<string, RGWAccessKey>::iterator it = i1.swift_keys.begin();
       it != i1.swift_keys.end(); ++it) {
    RGWAccessKey k1, k2;
    k1 = it->second;
    if (i2.swift_keys.count(it->first) == 0)
      return -1;
    k2 = i2.swift_keys[it->first];
    if (compare_access_keys(k1, k2) != 0)
      return -1;
  }
  if (i1.subusers.size() != i2.subusers.size()) 
    return -1;
  for (map<string, RGWSubUser>::iterator it = i1.subusers.begin();
       it != i1.subusers.end(); ++it) {
    RGWSubUser k1, k2;
    k1 = it->second;
    if (!i2.subusers.count(it->first))
      return -1;
    k2 = i2.subusers[it->first];
    if (k1.name.compare(k2.name) != 0) 
      return -1;
    if (k1.perm_mask != k2.perm_mask)
      return -1;
  }
  if (i1.suspended != i2.suspended)
    return -1;
  if (i1.max_buckets != i2.max_buckets)
    return -1;
  uint32_t p1, p2;
  p1 = p2 = RGW_CAP_ALL;
  if (i1.caps.check_cap(meta_caps, p1) != 0)
    return -1;
  if (i2.caps.check_cap(meta_caps, p2) != 0)
    return -1;
  return 0;
}

size_t read_dummy_post(void *ptr, size_t s, size_t n, void *ud) {
  int dummy = 0;
  memcpy(ptr, &dummy, sizeof(dummy));
  return sizeof(dummy);
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

size_t meta_read_json(void *ptr, size_t s, size_t n, void *ud){
  stringstream *ss = (stringstream *)ud;
  size_t len = ss->str().length();
  if(s*n < len){
    cout << "Cannot copy json data, as len is not enough\n";
    return 0;
  }
  memcpy(ptr, (void *)ss->str().c_str(), len);
  return len;
}

TEST(TestRGWAdmin, meta_list){
  JSONParser parser;
  bool found = false;
  const char *perm = "*";

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, meta_caps_add(perm));

  /*Check the sections*/
  g_test->send_request(string("GET"), string("/admin/metadata/"));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(parse_json_resp(parser) == 0);
  EXPECT_TRUE(parser.is_array());
  
  vector<string> l;
  l = parser.get_array_elements();
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it) {
    if((*it).compare("\"user\"") == 0) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  /*Check with a wrong section*/
  g_test->send_request(string("GET"), string("/admin/metadata/users"));
  EXPECT_EQ(404U, g_test->get_resp_code());

  /*Check the list of keys*/
  g_test->send_request(string("GET"), string("/admin/metadata/user"));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(parse_json_resp(parser) == 0);
  EXPECT_TRUE(parser.is_array());
  
  l = parser.get_array_elements();
  EXPECT_EQ(1U, l.size());
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it) {
    if((*it).compare(string("\"") + uid + string("\"")) == 0) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found);

  /*Check with second user*/
  string uid2 = "ceph1", display_name2 = "CEPH1";
  ASSERT_EQ(0, user_create(uid2, display_name2, false));
  /*Check the list of keys*/
  g_test->send_request(string("GET"), string("/admin/metadata/user"));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(parse_json_resp(parser) == 0);
  EXPECT_TRUE(parser.is_array());
  
  l = parser.get_array_elements();
  EXPECT_EQ(2U, l.size());
  bool found2 = false;
  for(vector<string>::iterator it = l.begin();
      it != l.end(); ++it) {
    if((*it).compare(string("\"") + uid + string("\"")) == 0) {
      found = true;
    }
    if((*it).compare(string("\"") + uid2 + string("\"")) == 0) {
      found2 = true;
    }
  }
  EXPECT_TRUE(found && found2);
  ASSERT_EQ(0, user_rm(uid2, display_name2));

  /*Remove the metadata caps*/
  int rv = meta_caps_rm(perm);
  EXPECT_EQ(0, rv);
  
  if(rv == 0) {
    g_test->send_request(string("GET"), string("/admin/metadata/"));
    EXPECT_EQ(403U, g_test->get_resp_code());

    g_test->send_request(string("GET"), string("/admin/metadata/user"));
    EXPECT_EQ(403U, g_test->get_resp_code());
  }
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, meta_get){
  JSONParser parser;
  const char *perm = "*";
  RGWUserInfo info;

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, meta_caps_add(perm));

  ASSERT_EQ(0, user_info(uid, display_name, info));
 
  g_test->send_request(string("GET"), string("/admin/metadata/user?key=test"));
  EXPECT_EQ(404U, g_test->get_resp_code());

  g_test->send_request(string("GET"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(parse_json_resp(parser) == 0);
  RGWObjVersionTracker objv_tracker;
  string metadata_key;

  obj_version *objv = &objv_tracker.read_version;
     
  JSONDecoder::decode_json("key", metadata_key, &parser);
  JSONDecoder::decode_json("ver", *objv, &parser);
  JSONObj *jo = parser.find_obj("data");
  ASSERT_TRUE(jo);
  string exp_meta_key = "user:";
  exp_meta_key.append(uid);
  EXPECT_TRUE(metadata_key.compare(exp_meta_key) == 0);

  RGWUserInfo obt_info;
  decode_json_obj(obt_info, jo);

  EXPECT_TRUE(compare_user_info(info, obt_info) == 0);

  /*Make a modification and check if its reflected*/
  ASSERT_EQ(0, meta_caps_rm(perm));
  perm = "read";
  ASSERT_EQ(0, meta_caps_add(perm));
  
  JSONParser parser1;
  g_test->send_request(string("GET"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(parse_json_resp(parser1) == 0);
 
  RGWObjVersionTracker objv_tracker1;
  obj_version *objv1 = &objv_tracker1.read_version;

  JSONDecoder::decode_json("key", metadata_key, &parser1);
  JSONDecoder::decode_json("ver", *objv1, &parser1);
  jo = parser1.find_obj("data");
  ASSERT_TRUE(jo);

  decode_json_obj(obt_info, jo);
  uint32_t p1, p2;
  p1 = RGW_CAP_ALL;
  p2 = RGW_CAP_READ;
  EXPECT_TRUE (info.caps.check_cap(meta_caps, p1) == 0);
  EXPECT_TRUE (obt_info.caps.check_cap(meta_caps, p2) == 0);
  p2 = RGW_CAP_WRITE;
  EXPECT_TRUE (obt_info.caps.check_cap(meta_caps, p2) != 0);

  /*Version and tag infromation*/
  EXPECT_TRUE(objv1->ver > objv->ver);
  EXPECT_EQ(objv1->tag, objv->tag);
  
  int rv = meta_caps_rm(perm);
  EXPECT_EQ(0, rv);
  
  if(rv == 0) {
    g_test->send_request(string("GET"), (string("/admin/metadata/user?key=") + uid));
    EXPECT_EQ(403U, g_test->get_resp_code());
  }
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, meta_put){
  JSONParser parser;
  const char *perm = "*";
  RGWUserInfo info;

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, meta_caps_add(perm));
  
  g_test->send_request(string("GET"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(parse_json_resp(parser) == 0);
  RGWObjVersionTracker objv_tracker;
  string metadata_key;

  obj_version *objv = &objv_tracker.read_version;
     
  JSONDecoder::decode_json("key", metadata_key, &parser);
  JSONDecoder::decode_json("ver", *objv, &parser);
  JSONObj *jo = parser.find_obj("data");
  ASSERT_TRUE(jo);
  string exp_meta_key = "user:";
  exp_meta_key.append(uid);
  EXPECT_TRUE(metadata_key.compare(exp_meta_key) == 0);

  RGWUserInfo obt_info;
  decode_json_obj(obt_info, jo);

  /*Change the cap and PUT */
  RGWUserCaps caps;
  string new_cap;
  Formatter *f = new JSONFormatter();

  new_cap = meta_caps + string("=write");
  caps.add_from_string(new_cap);
  obt_info.caps = caps;
  f->open_object_section("metadata_info");
  ::encode_json("key", metadata_key, f);
  ::encode_json("ver", *objv, f);
  ::encode_json("data", obt_info, f);
  f->close_section();
  std::stringstream ss;
  f->flush(ss);

  g_test->send_request(string("PUT"), (string("/admin/metadata/user?key=") + uid), 
                       meta_read_json,
                       (void *)&ss, ss.str().length());
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_EQ(0, user_info(uid, display_name, obt_info));
  uint32_t cp;
  cp = RGW_CAP_WRITE;
  EXPECT_TRUE (obt_info.caps.check_cap(meta_caps, cp) == 0);
  cp = RGW_CAP_READ;
  EXPECT_TRUE (obt_info.caps.check_cap(meta_caps, cp) != 0);
  
  int rv = meta_caps_rm("write");
  EXPECT_EQ(0, rv);
  if(rv == 0) {
    g_test->send_request(string("PUT"), (string("/admin/metadata/user?key=") + uid));
    EXPECT_EQ(403U, g_test->get_resp_code());
  }
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, meta_lock_unlock) {
  const char *perm = "*";
  string rest_req;

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, meta_caps_add(perm));

  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/metadata/user?lock&length=3&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/

  rest_req = "/admin/metadata/user?key=" CEPH_UID "&unlock";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/

  rest_req = "/admin/metadata/user?unlock&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(400U, g_test->get_resp_code()); /*Bad request*/
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&unlock&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&unlock&lock_id=ceph1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  utime_t sleep_time(3, 0);

  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(500U, g_test->get_resp_code()); 

  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(409U, g_test->get_resp_code()); 
  sleep_time.sleep();

  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&unlock&lock_id=ceph1";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 

  ASSERT_EQ(0, meta_caps_rm(perm));
  perm = "read";
  ASSERT_EQ(0, meta_caps_add(perm));
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&unlock&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, meta_caps_rm(perm));
  perm = "write";
  ASSERT_EQ(0, meta_caps_add(perm));
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&unlock&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(200U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, meta_caps_rm(perm));
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&lock&length=3&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  rest_req = "/admin/metadata/user?key=" CEPH_UID "&unlock&lock_id=ceph";
  g_test->send_request(string("POST"), rest_req, read_dummy_post, NULL, sizeof(int));
  EXPECT_EQ(403U, g_test->get_resp_code()); 
  
  ASSERT_EQ(0, user_rm(uid, display_name));
}

TEST(TestRGWAdmin, meta_delete){
  JSONParser parser;
  const char *perm = "*";
  RGWUserInfo info;

  ASSERT_EQ(0, user_create(uid, display_name));
  ASSERT_EQ(0, meta_caps_add(perm));

  g_test->send_request(string("DELETE"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(200U, g_test->get_resp_code());

  ASSERT_TRUE(user_info(uid, display_name, info) != 0);

  ASSERT_EQ(0, user_create(uid, display_name));
  perm = "read";
  ASSERT_EQ(0, meta_caps_add(perm));
  
  g_test->send_request(string("DELETE"), (string("/admin/metadata/user?key=") + uid));
  EXPECT_EQ(403U, g_test->get_resp_code());
  ASSERT_EQ(0, user_rm(uid, display_name));
}

int main(int argc, char *argv[]){
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_test = new admin_meta::test_helper();
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
